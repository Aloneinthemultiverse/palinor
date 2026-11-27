"""Edge cases for messaging, debates (auto-decide, quorum, tie-break, synthesis), ingestion and export."""
import re
import threading
import time
from datetime import timedelta

import pytest

from palinor.config import Config
from palinor.core import MAX_BODY, MAX_FIELD, MAX_TRANSCRIPT, Forbidden, NotFound, Palinor, PalinorError, iso, now
from palinor.events import EventBus
from palinor.graph import MemoryGraph


@pytest.fixture
def env(tmp_path):
    cfg = Config(data_dir=tmp_path, kafka_bootstrap="")
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "events.jsonl"), cfg)
    proj = svc.create_project("Edge", "Stress the edges")
    pid, jk = proj["project_id"], proj["join_key"]
    claude = svc.auth(svc.join(pid, jk, "Claude", "claude")["token"])
    gpt = svc.auth(svc.join(pid, jk, "GPT", "chatgpt")["token"])
    oc = svc.auth(svc.join(pid, jk, "OpenCode", "opencode")["token"])
    return svc, pid, proj, claude, gpt, oc


@pytest.fixture
def other(env):
    """A second project with one participant, for cross-project isolation checks."""
    svc = env[0]
    proj = svc.create_project("Other", "Someone else's work")
    x = svc.auth(svc.join(proj["project_id"], proj["join_key"], "Intruder", "claude")["token"])
    return proj["project_id"], x


def laya_msgs(svc, pid, subject_prefix=""):
    return [m for m in svc.g.find("Message", project_id=pid)
            if m["sender"] == "laya" and m["subject"].startswith(subject_prefix)]


def events(svc, pid, etype):
    return [e for e in svc.log[pid] if e["event_type"] == etype]


def go_offline(svc, p):
    svc.g.upsert("Participant", p["id"], {"last_seen": iso(now() - timedelta(hours=1))})
    svc.sweep(force=True)
    assert svc.g.get(p["id"])["status"] == "offline"


# ------------------------------------------------------------------ messaging
def test_recipients_by_name_id_list_and_all(env):
    svc, pid, _, claude, gpt, oc = env
    assert svc.send_message(claude, pid, "gpt", "hi")["to"] == [gpt["id"]]  # names are case-insensitive
    assert svc.send_message(claude, pid, oc["id"], "hi")["to"] == [oc["id"]]
    both = sorted([gpt["id"], oc["id"]])
    assert svc.send_message(claude, pid, ["GPT", oc["id"], "gpt"], "hi")["to"] == both  # de-duplicated
    assert svc.send_message(claude, pid, "GPT, OpenCode", "hi")["to"] == both  # comma list
    assert svc.send_message(claude, pid, ["GPT", "ALL"], "hi")["to"] == ["all"]
    for bad in ("", None, []):
        with pytest.raises(PalinorError):
            svc.send_message(claude, pid, bad, "hi")
    with pytest.raises(NotFound):
        svc.send_message(claude, pid, ["GPT", "ghost"], "hi")


def test_duplicate_names_prefer_online_then_refuse_to_guess(env):
    svc, pid, proj, claude, gpt, oc = env
    old = claude
    assert old["name"] == "Claude-1"  # generic platform names are numbered per project
    # a second Claude never shares a name: it becomes Claude-2, and "Claude" is then ambiguous
    second = svc.auth(svc.join(pid, proj["join_key"], "claude", "claude")["token"])
    assert second["name"] == "Claude-2"
    with pytest.raises(PalinorError, match="ambiguous"):
        svc.send_message(gpt, pid, "Claude", "who gets this?")
    assert svc.send_message(gpt, pid, "Claude-2", "hi 2")["to"] == [second["id"]]
    custom = svc.auth(svc.join(pid, proj["join_key"], "Reviewer Bot", "claude")["token"])
    clash = svc.auth(svc.join(pid, proj["join_key"], "reviewer bot", "chatgpt")["token"])
    assert custom["name"] == "Reviewer Bot" and clash["name"] == "reviewer bot-2"  # custom names de-duplicated
    for reserved in ("Laya", "all", "You (dashboard)"):
        with pytest.raises(PalinorError, match="reserved"):
            svc.join(pid, proj["join_key"], reserved, "claude")
    go_offline(svc, second)
    assert svc.send_message(gpt, pid, "Claude", "hello")["to"] == [old["id"]]  # only one online Claude left


def test_exclude_list_hides_broadcast(env):
    svc, pid, _, claude, gpt, oc = env
    svc.send_message(None, pid, "all", "for everyone but OpenCode", sender_id="laya", exclude=[oc["id"]])
    assert svc.message_inbox(oc)["messages"] == []
    assert [m["body"] for m in svc.message_inbox(gpt)["messages"]] == ["for everyone but OpenCode"]


def test_reply_defaults_to_sender_or_all_for_laya_broadcast(env):
    svc, pid, _, claude, gpt, oc = env
    q = svc.send_message(claude, pid, "GPT", "ping", kind="question")
    r = svc.send_message(gpt, pid, None, "pong", kind="answer", reply_to=q["message_id"])
    assert r["to"] == [claude["id"]] and r["thread_id"] == q["message_id"] and r["reply_to"] == q["message_id"]
    lm = svc.send_message(None, pid, gpt["id"], "Laya needs your vote", kind="request", sender_id="laya")
    with pytest.raises(PalinorError, match="explicit 'to'"):  # a private Laya DM must not echo to everyone
        svc.send_message(gpt, pid, "", "I will vote", reply_to=lm["message_id"])
    r2 = svc.send_message(gpt, pid, "all", "I will vote", reply_to=lm["message_id"])
    assert r2["to"] == ["all"] and r2["thread_id"] == lm["thread_id"]
    with pytest.raises(NotFound):  # cannot thread onto someone else's private message
        svc.send_message(oc, pid, "all", "peek", reply_to=q["message_id"])
    with pytest.raises(NotFound):
        svc.send_message(gpt, pid, None, "reply into nowhere", reply_to="MSG-nope")


def test_body_limits_and_kinds(env):
    svc, pid, _, claude, gpt, oc = env
    assert svc.send_message(claude, pid, "GPT", "x" * MAX_BODY)["to"] == [gpt["id"]]
    with pytest.raises(PalinorError, match="exceeds"):
        svc.send_message(claude, pid, "GPT", "x" * (MAX_BODY + 1))
    for empty in ("", "   \n\t"):
        with pytest.raises(PalinorError, match="empty"):
            svc.send_message(claude, pid, "GPT", empty)
    with pytest.raises(PalinorError, match="kind"):
        svc.send_message(claude, pid, "GPT", "hi", kind="shout")
    assert svc.send_message(claude, pid, "GPT", "hi", kind="QUESTION")["kind"] == "question"
    long_subject = svc.send_message(claude, pid, "GPT", "y" * 500)
    assert len(long_subject["subject"]) == 140


def test_refs_must_exist_in_this_project(env, other):
    svc, pid, _, claude, gpt, oc = env
    _, x = other
    mine = svc.submit_finding(claude, "ours")
    theirs = svc.submit_finding(x, "theirs")
    # grounding: citing another project's node or an invented id is rejected, never silently accepted
    with pytest.raises(NotFound, match="refs not found"):
        svc.send_message(claude, pid, "all", "see refs", refs=[mine["id"], theirs["id"], "NOPE-1"])
    m = svc.send_message(claude, pid, "all", "see refs", refs=[mine["id"]])
    assert m["refs"] == [mine["id"]]
    assert [n["id"] for n in svc.g.out(m["message_id"], "REFERENCES")] == [mine["id"]]
    # system notices may still mention ids loosely (they are filtered, not fatal)
    sysm = svc.send_message(None, pid, "all", "fyi", refs=[mine["id"], "NOPE-2"], sender_id="laya")
    assert sysm["refs"] == [mine["id"]]


def test_cross_project_access_is_denied(env, other):
    svc, pid, _, claude, gpt, oc = env
    opid, x = other
    dm = svc.send_message(claude, pid, "GPT", "private to our project")
    conv = svc.ingest_conversation(claude, "Our chat", summary="internal notes")["conversation_id"]
    with pytest.raises(NotFound):
        svc.send_message(x, opid, claude["id"], "hello from outside")
    with pytest.raises(NotFound):
        svc.send_message(x, opid, None, "hijack thread", reply_to=dm["message_id"])
    assert svc.message_thread(x, dm["thread_id"]) == []
    with pytest.raises(NotFound):
        svc.conversation_get(x, conv)
    with pytest.raises(NotFound):
        svc.context_send(x, "all", node_ids=[conv])
    assert all(dm["message_id"] != m["message_id"] for m in svc.message_inbox(x, unread_only=False)["messages"])


def test_observer_can_read_but_not_send_or_vote(env):
    svc, pid, proj, claude, gpt, oc = env
    obs = svc.auth(svc.join(pid, proj["join_key"], "Watcher", "human", role="observer")["token"])
    with pytest.raises(Forbidden):
        svc.send_message(obs, pid, "all", "hello")
    with pytest.raises(Forbidden):
        svc.context_send(obs, "all")
    svc.send_message(claude, pid, "Watcher", "fyi")
    assert [m["body"] for m in svc.message_inbox(obs)["messages"]] == ["fyi"]
    q = "Observer-visible debate?"
    svc.open_debate(claude, pid, q)
    pr = svc.create_proposal(claude, q, "Option A")
    assert svc._pending_actions(obs) == []  # no perspective to submit and no vote it could cast
    with pytest.raises(Forbidden):
        svc.cast_vote(obs, pr["id"], "approve")


def test_read_receipts_and_sync_marks_read_once(env):
    svc, pid, _, claude, gpt, oc = env
    m = svc.send_message(gpt, pid, "Claude", "read me")
    assert svc.message_inbox(claude, mark_read=False)["messages"][0]["read_by"] == []  # peeking is not reading
    first = svc.sync(claude)
    assert [x["message_id"] for x in first["messages"]] == [m["message_id"]]
    second = svc.sync(claude, first["latest_seq"])
    assert second["messages"] == []
    reads = [e for e in events(svc, pid, "MESSAGE_READ") if e["payload"]["message_id"] == m["message_id"]]
    assert len(reads) == 1 and reads[0]["target_participant_id"] == gpt["id"]
    assert svc.message_thread(gpt, m["thread_id"])[0]["read_by"] == [claude["id"]]
    receipt = [e for e in svc.notifications(gpt)["events"] if e["event_type"] == "MESSAGE_READ"]
    assert receipt and receipt[0]["for_you"] is True


def test_notifications_for_you_and_no_dm_content_leak(env):
    svc, pid, _, claude, gpt, oc = env
    seq = len(svc.log[pid])
    svc.send_message(gpt, pid, "all", "standup at 10")
    svc.send_message(gpt, pid, "OpenCode", "the staging password is hunter2")
    oc_feed = [e for e in svc.notifications(oc, seq)["events"] if e["event_type"] == "MESSAGE_SENT"]
    claude_feed = [e for e in svc.notifications(claude, seq)["events"] if e["event_type"] == "MESSAGE_SENT"]
    assert [e["for_you"] for e in oc_feed] == [True, True]
    assert [e["for_you"] for e in claude_feed] == [True, False]
    assert claude_feed[0]["summary"]["subject"] == "standup at 10"
    assert "hunter2" not in str(claude_feed[1]) and "subject" not in claude_feed[1]["summary"]
    assert "hunter2" not in str(oc_feed[1])  # DM content never travels in events (Kafka, events.jsonl)
    assert any("hunter2" in m["body"] for m in svc.message_inbox(oc)["messages"])  # only via the inbox
    assert all(e["by"] != gpt["id"] for e in svc.notifications(gpt, seq)["events"])  # own events excluded


# -------------------------------------------------------------------- debates
def test_open_debate_is_idempotent_per_question(env):
    svc, pid, _, claude, gpt, oc = env
    d1 = svc.open_debate(claude, pid, "Which queue?")
    d2 = svc.open_debate(gpt, pid, "  which   QUEUE? ", mode="empirical")
    assert d1["id"] == d2["id"] and d2["mode"] == "deliberative"
    assert len(svc.g.find("Debate", project_id=pid)) == 1
    assert len([m for m in svc.g.find("Message", project_id=pid) if m["subject"].startswith("Debate:")]) == 1
    with pytest.raises(PalinorError):
        svc.open_debate(claude, pid, "   ")
    with pytest.raises(PalinorError):
        svc.open_debate(claude, pid, "Q?", mode="coin-flip")


def _decide_simple(svc, pid, q, claude, gpt, oc):
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "Kafka")
    b = svc.create_proposal(gpt, q, "RabbitMQ")
    c = svc.create_proposal(oc, q, "SQS")
    for voter, prop, val in ((gpt, a, "approve"), (oc, a, "approve"), (claude, b, "reject"),
                             (oc, b, "reject"), (claude, c, "reject")):
        svc.cast_vote(voter, prop["id"], val)
    return a, b, c, svc.cast_vote(gpt, c["id"], "reject")


def test_debate_status_for_decided_debate(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Which queue?"
    a, b, c, last = _decide_simple(svc, pid, q, claude, gpt, oc)
    assert last["laya"]["outcome"] == "DECIDED" and last["laya"]["selected"] == a["id"]
    st = svc.debate_status(pid, q)
    assert st["debate"] and st["debate"]["status"] == "decided"
    assert st["laya_preview"]["outcome"] == "DECIDED" and st["laya_preview"]["selected"] == a["id"]
    assert st["laya_preview"]["mode"] == "deliberative" and st["laya_preview"]["ranking"]
    assert {p["status"] for p in st["perspectives"]} == {"selected", "rejected"}
    by_id = svc.debate_status(pid, debate_id=st["debate"]["id"])
    assert by_id["laya_preview"]["outcome"] == "DECIDED" and by_id["debate"]["id"] == st["debate"]["id"]
    assert svc.list_debates(pid)[0]["laya"] == "DECIDED"
    assert len(laya_msgs(svc, pid, "Decided:")) == 1
    svc.cast_vote(oc, b["id"], "approve")  # late vote on a closed debate: no new decision, no message
    assert len(svc.decisions(pid)) == 1 and len(laya_msgs(svc, pid, "Decided:")) == 1


def test_auto_decide_never_repeats_the_same_laya_request(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Tabs or spaces?"
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "Tabs")
    b = svc.create_proposal(gpt, q, "Spaces")
    c = svc.create_proposal(oc, q, "Whatever the formatter says")
    for voter, prop, val in ((gpt, a, "approve"), (oc, a, "approve"), (claude, b, "approve"),
                             (oc, b, "approve"), (claude, c, "reject")):
        svc.cast_vote(voter, prop["id"], val)
    assert svc.cast_vote(gpt, c["id"], "reject")["laya"]["outcome"] == "SYNTHESIZE"
    assert len(laya_msgs(svc, pid, "Combine perspectives")) == 1
    svc.cast_vote(gpt, c["id"], "reject", confidence=0.9)  # unchanged verdict
    # a tiny reject on A swaps the two leaders but they stay within the margin: same synthesis request
    flipped = svc.cast_vote(oc, a["id"], "reject", confidence=0.05)
    assert flipped["laya"]["outcome"] == "SYNTHESIZE"
    assert len(laya_msgs(svc, pid, "Combine perspectives")) == 1
    assert len(events(svc, pid, "SYNTHESIS_REQUESTED")) == 1
    assert len(laya_msgs(svc, pid)) == 1


def test_need_evidence_auto_request_is_sent_once(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Rename the project?"
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "Yes", evidence=[{"source": "survey", "reliability": 0.4}])
    b = svc.create_proposal(gpt, q, "No")
    for voter, prop in ((gpt, a), (oc, a), (claude, b)):
        svc.cast_vote(voter, prop["id"], "reject")
    assert svc.cast_vote(oc, b["id"], "reject")["laya"]["outcome"] == "NEED_EVIDENCE"
    for _ in range(3):
        svc.cast_vote(oc, b["id"], "reject", confidence=0.7)
    assert len(laya_msgs(svc, pid, "Laya needs more input")) == 1


def test_experiment_results_do_not_repeat_an_unchanged_laya_request(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Which index structure?"
    svc.open_debate(claude, pid, q, mode="empirical")
    a = svc.create_proposal(claude, q, "B-tree", evidence=[{"source": "paper", "reliability": 0.6}])
    b = svc.create_proposal(gpt, q, "LSM", evidence=[{"source": "paper", "reliability": 0.95}])
    for _ in range(3):  # non-reproducible support only -> VERIFY (one verification task)
        assert svc.submit_experiment(oc, b["id"], {"ops": 1}, True)["laya"]["outcome"] == "VERIFY"
    assert len(svc.g.find("Task", project_id=pid, type="verification")) == 1
    r1 = svc.submit_experiment(oc, a["id"], {"ops": 2}, True, commit_sha="c0ffee", reproducible=True)
    assert r1["laya"]["outcome"] == "SYNTHESIZE"
    r2 = svc.submit_experiment(oc, a["id"], {"ops": 0}, False)  # still tied, same pair
    assert r2["laya"]["outcome"] == "SYNTHESIZE"
    assert len(laya_msgs(svc, pid, "Combine perspectives")) == 1


def test_offline_participant_is_excluded_from_quorum(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Which logger?"
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "structlog")
    b = svc.create_proposal(gpt, q, "loguru")
    svc.cast_vote(claude, b["id"], "reject")
    assert svc.cast_vote(gpt, a["id"], "approve").get("laya") is None  # OpenCode still owes votes
    assert svc.debate_status(pid, q)["laya_preview"]["pending_votes"] == {a["id"]: [oc["id"]], b["id"]: [oc["id"]]}
    go_offline(svc, oc)
    res = svc.request_decision(claude, pid, q)
    assert res["outcome"] == "DECIDED" and res["selected"] == a["id"]


def test_human_not_in_quorum_but_its_vote_counts(env):
    svc, pid, _, claude, gpt, oc = env
    human = svc.ensure_human(pid)
    q = "Ship on Friday?"
    svc.open_debate(human, pid, q)
    assert not any(a["action"] == "submit_perspective" for a in svc._pending_actions(human))
    a = svc.create_proposal(claude, q, "Yes")
    b = svc.create_proposal(gpt, q, "No")
    svc.cast_vote(gpt, a["id"], "approve")
    svc.cast_vote(oc, a["id"], "approve")
    svc.cast_vote(claude, b["id"], "approve")
    tied = svc.cast_vote(oc, b["id"], "approve")["laya"]
    assert tied["outcome"] == "SYNTHESIZE" and tied["pending_votes"] == {}  # quorum met without the human
    assert {x["action"] for x in svc._pending_actions(human)} == {"vote"}
    decided = svc.cast_vote(human, b["id"], "reject", rationale="never on Fridays")["laya"]
    assert decided["outcome"] == "DECIDED" and decided["selected"] == a["id"]  # the human's vote tipped it
    ranked_b = next(r for r in svc.decisions(pid)[0]["ranking"] if r["proposal_id"] == b["id"])
    assert ranked_b["votes"] == 3 and ranked_b["vote_score"] < 1.0


def test_tiebreak_only_after_escalate(env):
    svc, pid, proj, claude, gpt, oc = env
    human = svc.ensure_human(pid)
    q = "REST or gRPC?"
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "REST")
    b = svc.create_proposal(gpt, q, "gRPC")
    for voter, prop in ((gpt, a), (oc, a), (claude, b)):
        svc.cast_vote(voter, prop["id"], "approve")
    assert svc.cast_vote(oc, b["id"], "approve")["laya"]["outcome"] == "SYNTHESIZE"
    with pytest.raises(PalinorError, match="only allowed while Laya reports ESCALATE"):
        svc.tiebreak(human, pid, q, a["id"], "I prefer REST")
    s = svc.synthesis_create(oc, q, [a["id"], b["id"]], "REST outside, gRPC inside", "both")
    svc.cast_vote(claude, s["id"], "reject")
    assert svc.cast_vote(gpt, s["id"], "reject")["laya"]["outcome"] == "ESCALATE"
    assert svc.g.find("Debate", project_id=pid)[0]["status"] == "escalated"
    assert any(x["action"] == "tiebreak" for x in svc._pending_actions(human))
    with pytest.raises(PalinorError, match="rationale"):
        svc.tiebreak(human, pid, q, a["id"], "   ")
    with pytest.raises(NotFound):
        svc.tiebreak(human, pid, q, "PR-unknown", "x")
    out = svc.tiebreak(human, pid, q, b["id"], "internal services need streaming")
    assert out["outcome"] == "TIEBREAK" and out["selected"] == b["id"]
    assert svc.debate_status(pid, q)["laya_preview"]["outcome"] == "TIEBREAK"
    assert len(laya_msgs(svc, pid, "Tie-break needed")) == 1


def test_synthesis_validation(env, other):
    svc, pid, _, claude, gpt, oc = env
    _, x = other
    q = "Cache?"
    a = svc.create_proposal(claude, q, "Redis")
    b = svc.create_proposal(gpt, q, "LRU")
    elsewhere = svc.create_proposal(claude, "Database?", "Postgres")
    with pytest.raises(PalinorError, match="same question"):
        svc.synthesis_create(oc, q, [a["id"], elsewhere["id"]], "mix", "r")
    with pytest.raises(PalinorError, match="at least two"):
        svc.synthesis_create(oc, q, [a["id"]], "mix", "r")
    with pytest.raises(PalinorError, match="at least two"):
        svc.synthesis_create(oc, q, [a["id"], a["id"]], "mix", "r")
    foreign = svc.create_proposal(x, q, "Memcached")
    with pytest.raises(NotFound):
        svc.synthesis_create(oc, q, [a["id"], foreign["id"]], "mix", "r")
    before = len(svc.g.find("Proposal", project_id=pid))
    s = svc.synthesis_create(oc, q, [b["id"], a["id"]], "Redis + LRU front", "r")
    assert s["kind"] == "synthesis" and s["derived_from"] == sorted([a["id"], b["id"]])
    assert len(svc.g.find("Proposal", project_id=pid)) == before + 1


def test_long_participant_text_never_half_fails_internal_notices(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Big design?"
    svc.open_debate(claude, pid, q, context="c" * MAX_BODY)  # context is quoted in the invitation
    with pytest.raises(PalinorError, match="longer than"):
        svc.create_proposal(gpt, q, "a" * (MAX_FIELD + 1))  # fields are capped before anything is written
    huge = "a" * MAX_FIELD
    p = svc.create_proposal(gpt, q, huge)  # approach is quoted in the vote request broadcast
    assert len(svc.g.find("Proposal", project_id=pid)) == 1 and p["approach"] == huge
    notices = [m for m in svc.g.find("Message", project_id=pid) if m["sender"] in (claude["id"], gpt["id"])]
    assert len(notices) == 2 and all(len(m["body"]) <= MAX_BODY for m in notices)
    out = svc.ingest_conversation(claude, "t" * (MAX_BODY + 1), summary="short")
    assert out["conversation_id"]


def test_concurrent_final_votes_commit_one_decision(env):
    svc, pid, _, claude, gpt, oc = env
    q = "Race?"
    svc.open_debate(claude, pid, q)
    a = svc.create_proposal(claude, q, "A")
    b = svc.create_proposal(gpt, q, "B")
    svc.cast_vote(claude, b["id"], "reject")
    svc.cast_vote(oc, b["id"], "reject")
    real = svc._assess

    def slow(*args, **kw):  # widen the check-then-commit window
        time.sleep(0.05)
        return real(*args, **kw)
    svc._assess = slow
    errors, verdicts = [], []

    def vote(p):
        try:
            verdicts.append(svc.cast_vote(p, a["id"], "approve").get("laya"))
        except Exception as exc:  # noqa: BLE001 - surfaced by the assert below
            errors.append(exc)
    threads = [threading.Thread(target=vote, args=(p,)) for p in (gpt, oc)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(svc.decisions(pid)) == 1
    # the losing voter must not see (or broadcast) a bogus re-assessment of the just-closed debate
    assert {v["outcome"] for v in verdicts if v} == {"DECIDED"}
    assert [m["subject"].split(":")[0] for m in laya_msgs(svc, pid)] == ["Decided"]


# ------------------------------------------------------------- ingestion / export
def test_ingest_dict_items_with_evidence(env):
    svc, pid, _, claude, gpt, oc = env
    out = svc.ingest_conversation(
        claude, "Benchmark chat", summary="we benchmarked stores", source_platform="claude",
        findings=[{"claim": "HBase does 10k writes/s", "confidence": 0.8,
                   "evidence": [{"source": "bench", "uri": "https://x/y", "reliability": 0.9}]}, "plain finding"],
        agreements=[{"text": "use salted keys"}], open_questions=[{"text": "need indexes?"}],
        tasks=[{"title": "benchmark scans", "description": "p99 latency"}])
    made = out["created"]
    assert [len(made[k]) for k in ("findings", "agreements", "open_questions", "tasks")] == [2, 1, 1, 1]
    f = svc.g.get(made["findings"][0])
    assert f["claim"] == "HBase does 10k writes/s" and f["confidence"] == 0.8 and f["kind"] == "extracted"
    ev = svc.g.out(f["id"], "SUPPORTED_BY", "Evidence")
    assert len(ev) == 1 and ev[0]["reliability"] == 0.9 and ev[0]["uri"] == "https://x/y"
    assert svc.g.get(made["findings"][1])["confidence"] == 0.5
    assert svc.g.get(made["tasks"][0])["description"] == "p99 latency"
    assert svc.g.get(made["agreements"][0])["kind"] == "agreement"
    conv = svc.conversation_get(gpt, out["conversation_id"])
    assert {x["label"] for x in conv["extracted"]} == {"Finding", "Task", "OpenQuestion"}


def test_ingest_duplicates_and_empty_input(env):
    svc, pid, _, claude, gpt, oc = env
    for t, s in (("", ""), ("   ", ""), ("", " \n ")):
        with pytest.raises(PalinorError, match="transcript or a summary"):
            svc.ingest_conversation(claude, "empty", t, s)
    first = svc.ingest_conversation(claude, "Chat", "hello world transcript")
    again = svc.ingest_conversation(gpt, "Different title", "hello world transcript")
    assert again == {"conversation_id": first["conversation_id"], "already_ingested": True}
    assert len(svc.g.find("Conversation", project_id=pid)) == 1
    summary_only = svc.ingest_conversation(claude, "Summary", summary="just a summary")
    assert summary_only["created"] == {"findings": [], "agreements": [], "open_questions": [], "tasks": []}


def test_transcript_size_cap(env, tmp_path):
    svc, pid, _, claude, gpt, oc = env
    with pytest.raises(PalinorError, match="too large"):
        svc.ingest_conversation(claude, "huge", "x" * (MAX_TRANSCRIPT + 1))
    with pytest.raises(PalinorError, match="too large"):
        svc.ingest_conversation(claude, "huge", "x" * MAX_TRANSCRIPT, summary="y")
    assert svc.g.find("Conversation", project_id=pid) == []
    big = svc.ingest_conversation(claude, "big but fine", "z" * 300_000)
    conv = svc.conversation_get(claude, big["conversation_id"])
    assert conv["length"] == 300_000 and len(conv["content"]) == 100_000 and len(conv["excerpt"]) == 4000


def _unescaped_pipes(line):
    return len(re.findall(r"(?<!\\)\|", line))


def test_markdown_export_survives_pipes_and_newlines(env):
    svc, pid, _, claude, gpt, oc = env
    t = svc.create_task(claude, pid, "Split | merge\n## Injected heading\nthird line")
    svc.submit_finding(claude, "claim with | pipe\n# fake title")
    q = "Which | one?\nreally"
    svc.open_debate(claude, pid, q)
    svc.create_proposal(gpt, q, "approach | with\nnewline")
    svc.ingest_conversation(claude, "title | x\ny", summary="s")
    md = svc.export_markdown(pid, oc)
    lines = md.splitlines()
    row = next(line for line in lines if t["id"] in line and line.startswith("|"))
    assert _unescaped_pipes(row) == 6 and "## Injected heading" in row
    assert "## Injected heading" not in lines and "# fake title" not in lines
    for title in ("## Participants", "## Tasks"):
        i = lines.index(title)
        width = _unescaped_pipes(lines[i + 1])
        j = i + 1
        while j < len(lines) and lines[j].startswith("|"):
            assert _unescaped_pipes(lines[j]) == width, lines[j]
            j += 1
    assert not any(line.strip() in {"really", "newline", "third line", "y"} for line in lines)
    assert "### Which \\| one? really" in md
