"""All-way communication, debates with synthesis, chat ingestion and markdown export."""
import pytest

from palinor.config import Config
from palinor.core import Forbidden, NotFound, Palinor, PalinorError
from palinor.events import EventBus
from palinor.graph import MemoryGraph
from palinor.notes import parse_markdown


@pytest.fixture
def env(tmp_path):
    cfg = Config(data_dir=tmp_path, kafka_bootstrap="")
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "events.jsonl"), cfg)
    proj = svc.create_project("Collab", "Choose an architecture")
    pid, jk = proj["project_id"], proj["join_key"]
    claude = svc.auth(svc.join(pid, jk, "Claude", "claude")["token"])
    gpt = svc.auth(svc.join(pid, jk, "GPT", "chatgpt")["token"])
    oc = svc.auth(svc.join(pid, jk, "OpenCode", "opencode")["token"])
    return svc, pid, claude, gpt, oc


def test_messages_flow_every_direction(env):
    svc, pid, claude, gpt, oc = env
    svc.send_message(claude, pid, "GPT", "Can you check the dataset?", kind="question")  # by name
    svc.send_message(gpt, pid, oc["id"], "Please review my PR")
    svc.send_message(oc, pid, "all", "Pushed branch opencode/task-1")
    gpt_in = svc.message_inbox(gpt)["messages"]
    assert {m["sender"] for m in gpt_in} == {claude["id"], oc["id"]}
    q = next(m for m in gpt_in if m["kind"] == "question")
    reply = svc.send_message(gpt, pid, None, "Dataset looks clean", kind="answer", reply_to=q["message_id"])
    assert reply["to"] == [claude["id"]] and reply["thread_id"] == q["message_id"]
    claude_in = svc.message_inbox(claude)["messages"]
    assert any(m["body"] == "Dataset looks clean" for m in claude_in)
    assert all(m["untrusted"] for m in claude_in)
    # read receipts flow back to the sender
    thread = svc.message_thread(claude, q["message_id"])
    assert gpt["id"] in thread[0]["read_by"]
    assert svc.message_inbox(gpt)["messages"] == []  # all read now
    with pytest.raises(NotFound):
        svc.send_message(claude, pid, "nobody", "hi")


def test_context_send_and_sync(env):
    svc, pid, claude, gpt, oc = env
    t = svc.create_task(claude, pid, "Survey")
    f = svc.submit_finding(claude, "Approach X is 2x faster", task_id=t["id"])
    out = svc.context_send(claude, [gpt["id"], oc["id"]], task_id=t["id"], node_ids=[f["id"]], note="FYI")
    assert out["to"] == sorted([gpt["id"], oc["id"]])
    s = svc.sync(oc)
    ctx_msg = next(m for m in s["messages"] if m["kind"] == "context")
    assert ctx_msg["context"]["task_context"]["findings"][0]["claim"] == "Approach X is 2x faster"
    assert ctx_msg["context"]["nodes"][0]["node"]["id"] == f["id"]
    assert s["latest_seq"] > 0


def test_debate_vote_synthesize_decide(env):
    svc, pid, claude, gpt, oc = env
    q = "Which caching strategy?"
    svc.open_debate(claude, pid, q, mode="deliberative")
    pa = svc.create_proposal(claude, q, "Redis", rationale="fast",
                             evidence=[{"source": "bench", "reliability": 0.6}])
    pb = svc.create_proposal(gpt, q, "In-process LRU", rationale="simple",
                             evidence=[{"source": "doc", "reliability": 0.6}])
    actions = {a["action"] for a in svc.sync(oc)["your_actions"]}
    assert {"submit_perspective", "vote"} <= actions
    with pytest.raises(PalinorError):
        svc.cast_vote(claude, pa["id"], "approve")  # no self-votes
    pc = svc.create_proposal(oc, q, "CDN edge cache")
    # everyone votes on the others: A and B equally liked, C rejected
    svc.cast_vote(gpt, pa["id"], "approve"); svc.cast_vote(oc, pa["id"], "approve")
    svc.cast_vote(claude, pb["id"], "approve"); svc.cast_vote(oc, pb["id"], "approve")
    svc.cast_vote(claude, pc["id"], "reject")
    last = svc.cast_vote(gpt, pc["id"], "reject")
    assert last["laya"]["outcome"] == "SYNTHESIZE"  # auto-evaluated when quorum completed
    pair = set(last["laya"]["synthesize"])
    assert pair == {pa["id"], pb["id"]}
    owner = svc._open_debate_for(pid, q)["synthesis_requested"]["assigned_to"]
    assert owner == oc["id"]  # the participant who authored neither side combines them
    assert any(a["action"] == "synthesize" for a in svc.sync(oc)["your_actions"])
    s = svc.synthesis_create(oc, q, sorted(pair), "Redis for shared state + in-process LRU front cache",
                             rationale="keeps simplicity and scale")
    svc.cast_vote(claude, s["id"], "approve")
    final = svc.cast_vote(gpt, s["id"], "approve", rationale="best of both")
    assert final["laya"]["outcome"] == "DECIDED"
    assert final["laya"]["selected"] == s["id"]
    d = svc.decisions(pid)[-1]
    assert d["lineage"] == sorted(pair) and d["mode"] == "deliberative"
    assert any(m["kind"] == "decision" for m in svc.message_inbox(claude)["messages"])


def test_tie_after_synthesis_escalates_to_human(env):
    svc, pid, claude, gpt, oc = env
    q = "Monorepo or polyrepo?"
    svc.open_debate(claude, pid, q)
    pa = svc.create_proposal(claude, q, "Monorepo")
    pb = svc.create_proposal(gpt, q, "Polyrepo")
    s = svc.synthesis_create(oc, q, [pa["id"], pb["id"]], "Monorepo with independent packages", "mix")
    # the group rejects the synthesis but keeps liking both originals equally -> still tied
    for voter, prop in ((gpt, pa), (oc, pa), (claude, pb), (oc, pb), (claude, s)):
        svc.cast_vote(voter, prop["id"], "approve" if prop is not s else "reject")
    res = svc.cast_vote(gpt, s["id"], "reject")
    assert res["laya"]["outcome"] == "ESCALATE"
    with pytest.raises(Forbidden):
        svc.tiebreak(gpt, pid, q, pa["id"], "I like it")  # workers cannot break ties
    human = svc.ensure_human(pid)
    out = svc.tiebreak(human, pid, q, pa["id"], "team already uses a monorepo")
    assert out["outcome"] == "TIEBREAK" and out["selected"] == pa["id"]


def test_chat_ingest_and_markdown(env):
    svc, pid, claude, gpt, oc = env
    transcript = """# Design chat
We discussed storage.

## Findings
- HBase handles 10k writes/s on one node
## Agreements
- use salted row keys
## Questions
- do we need secondary indexes?
## Tasks
- benchmark HBase scan latency
"""
    out = svc.ingest_conversation(claude, "Design chat", transcript, source_platform="claude")
    assert len(out["created"]["findings"]) == 1 and len(out["created"]["tasks"]) == 1
    assert svc.ingest_conversation(claude, "again", transcript)["already_ingested"] is True
    conv = svc.conversation_get(gpt, out["conversation_id"])
    assert "salted row keys" in conv["content"]
    assert {x["label"] for x in conv["extracted"]} >= {"Finding", "Task", "OpenQuestion"}
    md = svc.export_markdown(pid, gpt)
    assert "HBase handles 10k writes/s" in md and "Do not edit" in md and "Waiting on you" in md
    ctx = svc.get_context(oc)
    assert ctx["conversations"][0]["title"] == "Design chat" and ctx["open_questions"]


def test_parse_markdown_template():
    p = parse_markdown("# T\nintro\n## TODO\n- [ ] a\n- [x] b\n## Random\ntext\n## Decisions\n1. c\n")
    assert p["title"] == "T" and p["tasks"] == ["a", "b"] and p["agreements"] == ["c"]
    assert "intro" in p["summary"] and "Random" in p["summary"]


def test_observer_cannot_message(env, tmp_path):
    svc, pid, claude, gpt, oc = env
    proj = svc.g.get(pid)
    # create an observer through the API with the real join key stored hashed -> rejoin via create_project data
    obs_id = "observer-x"
    svc.g.upsert("Participant", obs_id, {"participant_id": obs_id, "project_id": pid, "name": "Obs",
                                          "platform": "human", "role": "observer", "status": "online",
                                          "last_seen": proj["created_at"], "token_hash": "x"})
    with pytest.raises(Forbidden):
        svc.send_message(svc.g.get(obs_id), pid, "all", "hello")
