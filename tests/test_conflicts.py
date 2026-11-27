"""The conflict protocol, exactly as described by the user:

Claude commands ChatGPT and OpenCode. ChatGPT sees a conflict in the knowledge graph and reacts to Claude.
Claude asks OpenCode's opinion, OpenCode replies, Claude chooses and solves it. Several Claudes are named
unambiguously, nobody else is disturbed, and invented references are rejected.
"""
import pytest

from palinor import conflicts, orchestrator
from palinor.config import Config
from palinor.core import Forbidden, NotFound, Palinor, PalinorError
from palinor.events import EventBus
from palinor.graph import MemoryGraph


@pytest.fixture
def team(tmp_path):
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "e.jsonl"), Config(data_dir=tmp_path, kafka_bootstrap=""))
    proj = svc.create_project("Imagery", "cloud-free satellite reconstruction")
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    j = lambda name, plat, **kw: svc.auth(svc.join(pid, jk, name, plat, **kw)["token"])  # noqa: E731
    lead = j("", "claude", role="coordinator", admin_key=ak)     # Claude-1, the commander
    claude2 = j("", "claude", capabilities=["research"])         # Claude-2
    claude3 = j("", "claude", capabilities=["research"])         # Claude-3
    gpt = j("", "chatgpt", capabilities=["research"])            # ChatGPT-1
    oc = j("", "opencode", capabilities=["coding"])              # OpenCode-1
    return svc, pid, lead, claude2, claude3, gpt, oc


def inbox(svc, p):
    return svc.message_inbox(p)["messages"]


def test_names_are_unambiguous(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    assert [lead["name"], c2["name"], c3["name"], gpt["name"], oc["name"]] == \
        ["Claude-1", "Claude-2", "Claude-3", "ChatGPT-1", "OpenCode-1"]
    with pytest.raises(PalinorError, match="ambiguous"):
        svc.send_message(gpt, pid, "Claude", "which one?")
    assert svc.send_message(gpt, pid, "claude-1", "hi")["to"] == [lead["id"]]


def test_conflict_raise_consult_opinion_resolve(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    # Claude-1 commands: research by ChatGPT, implementation by OpenCode
    f = svc.submit_finding(c2, "Diffusion beats GAN on SEN12MS-CR", evidence=[{"source": "paper A", "uri": "u",
                                                                              "reliability": 0.6}])
    plan = orchestrator.submit_plan(svc, lead, "Pick the model", [
        {"key": "verify", "title": "Verify model claims", "capability": "research", "assignee": "ChatGPT-1"},
        {"key": "build", "title": "Implement chosen model", "capability": "coding", "depends_on": ["verify"]}])
    cmd = svc.inbox(gpt)["commands"][0]
    svc.command_update(gpt, cmd["command_id"], "ACCEPTED")
    for p in (lead, c2, c3, gpt, oc):
        inbox(svc, p)  # clear the plan announcement
    # ChatGPT finds the conflict in the graph and reacts — to its commander only
    with pytest.raises(NotFound, match="do not invent"):
        conflicts.raise_conflict(svc, gpt, ["F-hallucinated"], "made-up reference")
    raised = conflicts.raise_conflict(svc, gpt, [f["id"]], "On cloudy tiles GAN wins: diffusion hallucinates texture",
                                      evidence=[{"source": "benchmark B", "uri": "u2", "reliability": 0.9}],
                                      task_id=plan["tasks"]["verify"])
    cid = raised["conflict_id"]
    assert raised["resolver"] == lead["id"]  # the plan owner, not "some Claude"
    assert conflicts.raise_conflict(svc, gpt, [f["id"]], "same again")["duplicate"] is True
    assert len(inbox(svc, lead)) == 1                      # exactly one alert reaches Claude-1
    assert inbox(svc, c2) == [] and inbox(svc, c3) == [] and inbox(svc, oc) == []  # nobody else disturbed
    assert any(a["action"] == "resolve_conflict" for a in svc._pending_actions(lead))
    # Claude-1 asks OpenCode's opinion
    with pytest.raises(Forbidden):
        conflicts.consult(svc, c2, cid, ["OpenCode-1"], "?")  # only the resolver consults
    conflicts.consult(svc, lead, cid, ["OpenCode-1"], "Which model handles cloudy tiles better?")
    q = inbox(svc, oc)
    assert len(q) == 1 and q[0]["context"]["challenged"][0]["id"] == f["id"]  # the facts travel with the question
    assert inbox(svc, gpt) == [] and inbox(svc, c3) == []
    with pytest.raises(Forbidden):
        conflicts.give_opinion(svc, c3, cid, "keep", "not asked", refs=[f["id"]])
    with pytest.raises(PalinorError, match="ground your opinion"):
        conflicts.give_opinion(svc, oc, cid, "accept_challenge", "trust me")  # no refs, no evidence
    with pytest.raises(PalinorError, match="waiting for opinions"):
        conflicts.resolve(svc, lead, cid, "accept_challenge", "too early")
    op = conflicts.give_opinion(svc, oc, cid, "accept_challenge", "our tile tests agree with benchmark B",
                                refs=[f["id"]], evidence=[{"source": "tile test", "uri": "u3", "reliability": 0.7}])
    assert op["pending"] == []
    answer = inbox(svc, lead)
    assert len(answer) == 1 and answer[0]["thread_id"] == raised["thread_id"]  # one thread per conflict
    # Claude-1 chooses and solves
    out = conflicts.resolve(svc, lead, cid, "accept_challenge", "two independent sources favour GAN on clouds")
    assert out["evidence_audit"]["against_evidence"] is False
    assert svc.g.get(f["id"])["status"] == "superseded"
    assert sorted(out["notified"]) == sorted([gpt["id"], oc["id"], c2["id"]])  # raiser, consultant, author
    assert inbox(svc, c3) == []  # Claude-3 had no part in it: not messaged
    d = svc.g.get(out["decision_id"])
    assert d["outcome"] == "RESOLVED_BY_COORDINATOR" and d["opinions"][0]["by"] == oc["id"]
    assert not [a for a in svc._pending_actions(lead) if a.get("conflict_id") == cid]


def test_resolver_overriding_evidence_is_flagged(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    f = svc.submit_finding(c2, "weak claim")  # no evidence at all
    assert svc.g.get(f["id"])["grounded"] is False
    cid = conflicts.raise_conflict(svc, gpt, [f["id"]], "strong counter-claim",
                                   evidence=[{"source": "s", "uri": "u", "reliability": 0.9}])["conflict_id"]
    out = conflicts.resolve(svc, lead, cid, "keep", "gut feeling")
    assert out["evidence_audit"]["against_evidence"] is True
    msg = [m for m in inbox(svc, gpt) if m["kind"] == "decision"][0]
    assert "weaker evidence" in msg["body"]


def test_challenging_a_laya_decision_reopens_a_debate(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    q = "Which loss?"
    svc.open_debate(lead, pid, q)
    a = svc.create_proposal(c2, q, "L1", evidence=[{"source": "s", "reliability": 0.7}])
    b = svc.create_proposal(gpt, q, "SSIM")
    for v, p_, val in ((lead, a, "approve"), (c3, a, "approve"), (gpt, a, "approve"), (oc, a, "approve"),
                       (lead, b, "reject"), (c2, b, "reject"), (c3, b, "reject")):
        svc.cast_vote(v, p_["id"], val)
    res = svc.cast_vote(oc, b["id"], "reject")
    assert res["laya"]["outcome"] == "DECIDED"
    did = res["laya"]["decision_id"]
    cid = conflicts.raise_conflict(svc, oc, [did], "L1 blurs edges; we need a perceptual term",
                                   evidence=[{"source": "exp", "uri": "u", "reliability": 0.8}])["conflict_id"]
    out = conflicts.resolve(svc, lead, cid, "accept_challenge", "valid concern")
    assert out["reopened_debate"]  # the coordinator cannot flip Laya's decision alone
    assert svc._open_debate_for(pid, q)["status"] == "open"


def test_consult_limits_and_withdraw(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    f = svc.submit_finding(c2, "x", evidence=[{"source": "s", "uri": "u"}])
    cid = conflicts.raise_conflict(svc, gpt, [f["id"]], "y")["conflict_id"]
    with pytest.raises(PalinorError, match="not everyone"):
        conflicts.consult(svc, lead, cid, "all", "?")
    with pytest.raises(PalinorError, match="at most"):
        conflicts.consult(svc, lead, cid, ["Claude-2", "Claude-3", "OpenCode-1", "ChatGPT-1"], "?")
    with pytest.raises(Forbidden):
        conflicts.withdraw(svc, oc, cid, "not mine")
    assert conflicts.withdraw(svc, gpt, cid, "I misread the finding")["status"] == "withdrawn"


def test_noise_controls(team):
    svc, pid, lead, c2, c3, gpt, oc = team
    first = svc.send_message(gpt, pid, "Claude-1", "status: verify task 50% done")
    again = svc.send_message(gpt, pid, "Claude-1", "status: verify task 50% done")
    assert again["duplicate"] is True and again["message_id"] == first["message_id"]
    assert len(inbox(svc, lead)) == 1
    for i in range(6):
        svc.send_message(c2, pid, "all", f"broadcast {i}")
    with pytest.raises(PalinorError, match="broadcast limit"):
        svc.send_message(c2, pid, "all", "one too many")
