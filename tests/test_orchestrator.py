"""One coordinator divides the work; PALINOR runs the pipeline (dispatch, hand-off, re-dispatch)."""
from datetime import timedelta

import pytest

from palinor import orchestrator
from palinor.config import Config
from palinor.core import Forbidden, Palinor, PalinorError, iso, now
from palinor.events import EventBus
from palinor.graph import MemoryGraph


@pytest.fixture
def team(tmp_path):
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "e.jsonl"), Config(data_dir=tmp_path, kafka_bootstrap=""))
    proj = svc.create_project("Pipeline", "ship a feature")
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    lead = svc.auth(svc.join(pid, jk, "Claude", "claude", role="coordinator", admin_key=ak)["token"])
    gpt = svc.auth(svc.join(pid, jk, "GPT", "chatgpt", ["research"])["token"])
    oc = svc.auth(svc.join(pid, jk, "OpenCode", "opencode", ["coding"])["token"])
    return svc, pid, lead, gpt, oc


def accept_and_complete(svc, who, note):
    cmds = svc.inbox(who)["commands"]
    assert cmds, f"{who['id']} has no command"
    c = cmds[0]
    svc.command_update(who, c["command_id"], "ACCEPTED")
    svc.command_update(who, c["command_id"], "COMPLETED", note=note, result_ref="R-1")
    return c


def test_plan_dispatch_handoff_and_completion(team):
    svc, pid, lead, gpt, oc = team
    plan = orchestrator.submit_plan(svc, lead, "Add caching", [
        {"key": "research", "title": "Survey caching options", "capability": "research",
         "acceptance": "3 options compared"},
        {"key": "build", "title": "Implement cache", "capability": "coding", "depends_on": ["research"]},
        {"key": "review", "title": "Review implementation", "depends_on": ["build"], "assignee": "GPT"},
    ])
    t = plan["tasks"]
    assert plan["dispatched"] == [{"task_id": t["research"], "participant_id": gpt["id"],
                                   "command_id": plan["dispatched"][0]["command_id"]}]
    assert svc.g.get(t["build"])["status"] == "BLOCKED" and not svc.inbox(oc)["commands"]
    c = svc.inbox(gpt)["commands"][0]
    assert "3 options compared" in c["instruction"]  # acceptance criteria travel with the command
    accept_and_complete(svc, gpt, "Redis wins: shared + TTL")
    # hand-off: build became READY and went straight to the coding specialist, carrying the upstream result
    build_cmd = svc.inbox(oc)["commands"][0]
    assert build_cmd["task_id"] == t["build"] and "Redis wins" in build_cmd["instruction"]
    progress = [m for m in svc.message_inbox(lead)["messages"] if m["sender"] == "orchestrator"]
    assert progress and "1/3" in progress[-1]["body"]
    accept_and_complete(svc, oc, "cache implemented in PR #4")
    review_cmd = svc.inbox(gpt)["commands"][0]  # explicit assignee honoured
    assert review_cmd["task_id"] == t["review"]
    accept_and_complete(svc, gpt, "LGTM")
    st = orchestrator.pipeline_status(svc, pid)["plans"][0]
    assert st["status"] == "completed" and st["percent"] == 100 and st["stages"] == 3
    assert st["workload"][gpt["id"]]["done"] == 2
    assert any("Plan complete" in m["body"] for m in svc.message_inbox(oc)["messages"])


def test_rejection_redispatches_and_alerts(team):
    svc, pid, lead, gpt, oc = team
    plan = orchestrator.submit_plan(svc, lead, "Fix bug", [{"key": "fix", "title": "Fix it", "capability": "coding"}])
    c = svc.inbox(oc)["commands"][0]
    svc.command_update(oc, c["command_id"], "REJECTED", note="no repo access")
    task = svc.g.get(plan["tasks"]["fix"])
    assert oc["id"] in task["rejected_by"]
    assert svc.inbox(gpt)["commands"][0]["task_id"] == task["id"]  # next best, never the rejector
    assert any("rejected" in m["body"] for m in svc.message_inbox(lead)["messages"])


def test_offline_worker_loses_its_task(team):
    svc, pid, lead, gpt, oc = team
    plan = orchestrator.submit_plan(svc, lead, "Docs", [{"key": "d", "title": "Write docs", "capability": "research"}])
    assert svc.inbox(gpt)["commands"]
    svc.g.upsert("Participant", gpt["id"], {"last_seen": iso(now() - timedelta(hours=2))})
    svc.sweep(force=True)
    tid = plan["tasks"]["d"]
    assert any(c["task_id"] == tid for c in svc.inbox(oc)["commands"])
    old = [c for c in svc.g.find("Command", task_id=tid) if c["target_participant_id"] == gpt["id"]][0]
    assert old["status"] == "CANCELLED"


def test_failure_alerts_coordinator(team):
    svc, pid, lead, gpt, oc = team
    orchestrator.submit_plan(svc, lead, "Train", [{"key": "t", "title": "Train model", "capability": "coding"}])
    c = svc.inbox(oc)["commands"][0]
    svc.command_update(oc, c["command_id"], "ACCEPTED")
    svc.command_update(oc, c["command_id"], "FAILED", note="OOM", retryable=False)
    alerts = [m for m in svc.message_inbox(lead)["messages"] if m["kind"] == "alert"]
    assert alerts and "OOM" in alerts[0]["body"]
    assert orchestrator.pipeline_status(svc, pid)["plans"][0]["status"] == "attention"


def test_plan_validation(team):
    svc, pid, lead, gpt, oc = team
    with pytest.raises(Forbidden):
        orchestrator.submit_plan(svc, gpt, "x", [{"key": "a", "title": "a"}])  # workers do not command
    with pytest.raises(PalinorError, match="cycle"):
        orchestrator.submit_plan(svc, lead, "x", [{"key": "a", "depends_on": ["b"]}, {"key": "b", "depends_on": ["a"]}])
    with pytest.raises(PalinorError, match="unique"):
        orchestrator.submit_plan(svc, lead, "x", [{"key": "a"}, {"key": "a"}])
    with pytest.raises(Exception):
        orchestrator.submit_plan(svc, lead, "x", [{"key": "a", "depends_on": ["T-missing"]}])


def test_manual_assignment_of_blocked_task_is_deferred(team):
    svc, pid, lead, gpt, oc = team
    t1 = svc.create_task(lead, pid, "first")
    t2 = svc.create_task(lead, pid, "second", depends_on=[t1["id"]])
    res = svc.assign_task(lead, pid, t2["id"], oc["id"], "do the second step")
    assert res["status"] == "DEFERRED" and not svc.inbox(oc)["commands"]  # no command for a blocked task
    svc.assign_task(lead, pid, t1["id"], gpt["id"])
    accept_and_complete(svc, gpt, "first done")
    cmd = svc.inbox(oc)["commands"][0]
    assert cmd["task_id"] == t2["id"] and cmd["instruction"] == "do the second step"


def test_capabilities_and_new_joiner_pick_up_waiting_work(team):
    svc, pid, lead, gpt, oc = team
    svc.g.upsert("Participant", gpt["id"], {"status": "offline"})
    svc.g.upsert("Participant", oc["id"], {"status": "offline"})
    svc.g.upsert("Participant", lead["id"], {"status": "offline"})
    plan = orchestrator.submit_plan(svc, svc.g.get(lead["id"]), "ML", [{"key": "m", "title": "Train", "capability": "ml"}])
    assert plan["dispatched"] == []  # nobody online yet
    svc.g.upsert("Participant", gpt["id"], {"status": "online"})
    out = orchestrator.set_capabilities(svc, svc.g.get(gpt["id"]), ["ML", "research"])
    assert out["capabilities"] == ["ml", "research"]
    assert svc.inbox(svc.g.get(gpt["id"]))["commands"][0]["task_id"] == plan["tasks"]["m"]


# ------------------------------------------------- EPLB-style placement (projected finish time, affinity)
@pytest.fixture
def pool(tmp_path):
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "e.jsonl"), Config(data_dir=tmp_path, kafka_bootstrap=""))
    proj = svc.create_project("Balance", "split work well")
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    lead = svc.auth(svc.join(pid, jk, "Lead", "claude", role="coordinator", admin_key=ak)["token"])

    def worker(name, caps, gpu):
        p = svc.auth(svc.join(pid, jk, name, "opencode", caps)["token"])
        svc.set_profile(p, gpu=gpu, can_run_code=True)
        return p
    return svc, pid, lead, worker


def _owner(svc, pid, key):
    t = next(t for t in svc.g.find("Task", project_id=pid) if t.get("plan_key") == key)
    return svc.g.out(t["id"], "ASSIGNED_TO")[0]["id"]


def test_placement_uses_estimated_minutes_not_task_count(pool):
    svc, pid, lead, worker = pool
    a, b = worker("A", ["coding"], "RTX 4050"), worker("B", ["coding"], "RTX 4050")
    # A holds one 3-hour task, B two 10-minute tasks: by count B is busier, by minutes A is
    orchestrator.submit_plan(svc, lead, "load", [
        {"key": "big", "title": "big", "capability": "coding", "assignee": "A", "est_minutes": 180},
        {"key": "s1", "title": "s1", "capability": "coding", "assignee": "B", "est_minutes": 10},
        {"key": "s2", "title": "s2", "capability": "coding", "assignee": "B", "est_minutes": 10},
    ])
    orchestrator.submit_plan(svc, lead, "more", [{"key": "next", "title": "next", "capability": "coding", "est_minutes": 30}])
    assert _owner(svc, pid, "next") == b["id"]  # B finishes earlier (20+30 min) than A (180+30 min)


def test_gpu_task_avoids_machine_without_gpu(pool):
    svc, pid, lead, worker = pool
    worker("CPU-box", ["gpu"], "none")
    gpu = worker("GPU-box", ["gpu"], "RTX 4050 6GB")
    orchestrator.submit_plan(svc, lead, "warm", [{"key": "warm", "title": "warm", "capability": "gpu", "assignee": "GPU-box", "est_minutes": 60}])
    orchestrator.submit_plan(svc, lead, "train", [{"key": "train", "title": "train", "capability": "gpu", "est_minutes": 30}])
    assert _owner(svc, pid, "train") == gpu["id"]  # busier, but the only real GPU


def test_heaviest_ready_task_is_dispatched_first(pool):
    svc, pid, lead, worker = pool
    worker("W", ["research"], "none")
    r = orchestrator.submit_plan(svc, lead, "lpt", [
        {"key": "light", "title": "light", "capability": "research", "est_minutes": 5},
        {"key": "heavy", "title": "heavy", "capability": "research", "est_minutes": 120},
    ])
    assert svc.g.get(r["dispatched"][0]["task_id"])["plan_key"] == "heavy"


def test_gpu_task_waits_rather_than_going_to_a_cpu_machine(pool):
    svc, pid, lead, worker = pool
    svc.set_profile(lead, gpu="none")  # the coordinator's own machine has no GPU either
    worker("CPU-box", ["gpu"], "none")
    r = orchestrator.submit_plan(svc, lead, "train", [{"key": "train", "title": "train", "capability": "gpu"}])
    assert r["dispatched"] == []  # stays READY until a machine with a GPU is online
    gpu = worker("GPU-box", ["gpu"], "RTX 4050")
    orchestrator.dispatch(svc, pid)
    assert _owner(svc, pid, "train") == gpu["id"]


def test_started_task_stays_with_owner_who_goes_quiet(pool):
    svc, pid, lead, worker = pool
    a, b = worker("A", ["coding"], "RTX 4050"), worker("B", ["coding"], "RTX 4050")
    orchestrator.submit_plan(svc, lead, "job", [{"key": "job", "title": "job", "capability": "coding", "assignee": "A"}])
    svc.sync(a)  # delivers the command
    cmd = next(c for c in svc.g.find("Command", target_participant_id=a["id"]))
    svc.command_update(a, cmd["id"], "ACCEPTED")
    orchestrator.on_participant_offline(svc, pid, a["id"])
    assert _owner(svc, pid, "job") == a["id"]  # not silently handed to B while A is mid-run
