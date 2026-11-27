"""End-to-end flow tests for the PALINOR core (in-memory graph, Kafka off)."""
import pytest

from palinor.config import Config
from palinor.core import Forbidden, Palinor, PalinorError
from palinor.events import EventBus
from palinor.graph import MemoryGraph


@pytest.fixture
def env(tmp_path):
    cfg = Config(data_dir=tmp_path, kafka_bootstrap="")
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "events.jsonl"), cfg)
    proj = svc.create_project("Demo", "Pick the best model", ["latency < 50ms"])
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    lead = svc.auth(svc.join(pid, jk, "Lead", "claude", role="coordinator", admin_key=ak)["token"])
    a = svc.auth(svc.join(pid, jk, "Laptop1", "claude", ["research"])["token"])
    b = svc.auth(svc.join(pid, jk, "Laptop2", "chatgpt", ["coding"])["token"])
    return svc, pid, lead, a, b


def test_join_rejects_bad_keys(env):
    svc, pid, *_ = env
    with pytest.raises(Forbidden):
        svc.join(pid, "wrong", "x", "claude")
    with pytest.raises(Forbidden):
        svc.auth("not-a-token")


def test_command_lifecycle_and_dependencies(env):
    svc, pid, lead, a, b = env
    t1 = svc.create_task(lead, pid, "Research", task_type="research")
    t2 = svc.create_task(lead, pid, "Implement", task_type="coding", depends_on=[t1["id"]])
    assert t2["status"] == "BLOCKED"
    cmd = svc.assign_task(lead, pid, t1["id"], a["id"])
    assert cmd["status"] == "QUEUED"
    # idempotent re-assign returns the same live command
    assert svc.assign_task(lead, pid, t1["id"], a["id"])["id"] == cmd["id"]
    inbox = svc.inbox(a)["commands"]
    assert inbox[0]["status"] == "DELIVERED"
    svc.command_update(a, cmd["id"], "ACCEPTED")
    svc.command_update(a, cmd["id"], "RUNNING")
    done = svc.command_update(a, cmd["id"], "COMPLETED", note="found 3 papers")
    assert done["status"] == "COMPLETED"
    assert svc.g.get(t1["id"])["status"] == "COMPLETED"
    assert svc.g.get(t2["id"])["status"] == "READY"  # unblocked
    statuses = [h["status"] for h in svc.command_status(pid, cmd["id"])["history"]]
    assert statuses == ["CREATED", "AUTHORIZED", "QUEUED", "DELIVERED", "ACCEPTED", "RUNNING", "COMPLETED"]


def test_retry_and_permissions(env):
    svc, pid, lead, a, b = env
    t = svc.create_task(lead, pid, "Flaky")
    cmd = svc.assign_task(lead, pid, t["id"], b["id"])
    svc.inbox(b)
    svc.command_update(b, cmd["id"], "ACCEPTED")
    again = svc.command_update(b, cmd["id"], "FAILED", "timeout", retryable=True)
    assert again["status"] == "QUEUED" and again["attempt"] == 2
    with pytest.raises(Forbidden):
        svc.command_update(a, cmd["id"], "ACCEPTED")  # not the target
    with pytest.raises(Forbidden):
        svc.send_command(a, pid, b["id"], "START_TASK", "do it")  # workers cannot command
    with pytest.raises(PalinorError):
        svc.command_update(b, cmd["id"], "COMPLETED")  # QUEUED -> COMPLETED illegal


def test_context_sharing_between_sessions(env):
    svc, pid, lead, a, b = env
    t1 = svc.create_task(lead, pid, "Research")
    t2 = svc.create_task(lead, pid, "Build", depends_on=[t1["id"]])
    svc.submit_finding(a, "Model X hits 42ms", task_id=t1["id"],
                       evidence=[{"source": "paper", "uri": "https://example.org", "reliability": 0.8}])
    ctx = svc.get_context(b, t2["id"])
    assert ctx["findings"][0]["claim"] == "Model X hits 42ms"
    assert ctx["findings"][0]["provenance"]["by"] == a["id"]
    feed = svc.notifications(b)["events"]
    assert any(e["event_type"] == "FINDING_ADDED" for e in feed)


def test_contradiction_verification_then_decision(env):
    svc, pid, lead, a, b = env
    q = "Which architecture?"
    pa = svc.create_proposal(a, q, "Approach A")
    pb = svc.create_proposal(b, q, "Approach B")
    assert svc.g.find("Contradiction", project_id=pid, status="open")
    svc.cast_vote(lead, pa["id"], "approve")
    svc.cast_vote(b, pa["id"], "approve")
    v = svc.request_decision(lead, pid, q)
    assert v["outcome"] == "VERIFY"  # majority alone is not enough
    assert v["verification_task"]
    # a failed run never counts
    bad = svc.submit_experiment(b, pb["id"], {}, True, status="failed")
    assert bad["counted_as_evidence"] is False
    # non-reproducible (no SHA) result is not enough either
    weak = svc.submit_experiment(a, pa["id"], {"acc": 0.95}, True, reproducible=True)
    assert weak["reproducible"] is False and weak["laya"]["outcome"] == "VERIFY"
    res = svc.submit_experiment(b, pb["id"], {"acc": 0.91}, True, commit_sha="abc123", reproducible=True)
    assert res["laya"]["outcome"] == "DECIDED"
    assert res["laya"]["selected"] == pb["id"]
    d = svc.decisions(pid)[0]
    assert d["policy_version"].startswith("laya")
    assert not svc.g.find("Contradiction", project_id=pid, status="open")


def test_requirement_gate_and_merge_rules(env):
    svc, pid, lead, a, b = env
    q = "Which db?"
    svc.create_proposal(a, q, "Fast but violates", violates=["latency < 50ms"])
    v = svc.request_decision(lead, pid, q)
    assert v["excluded"] and v["outcome"] == "NEED_PERSPECTIVES"
    t = svc.create_task(lead, pid, "Code")
    with pytest.raises(Forbidden):
        svc.link_github(a, t["id"], pr_url="https://github.com/x/y/pull/1", pr_state="merged", ci_status="passed")
    out = svc.link_github(lead, t["id"], branch="b", commit_sha="aaa", pr_url="https://github.com/x/y/pull/1",
                          pr_state="merged", ci_status="passed")
    assert out["pull_request"]
