"""Neo4j compatibility: property encoding round-trips, plus an optional live run against a local Neo4j."""
import os
import uuid

import pytest

from palinor.config import Config
from palinor.core import Palinor
from palinor.events import EventBus
from palinor.graph import MemoryGraph, _dec, _enc

SHAPES = {
    "dict": {"a": 1, "b": "x"},
    "empty_dict": {},
    "list_of_dicts": [{"status": "CREATED", "at": "t", "by": None}, {"status": "QUEUED"}],
    "list_of_str": ["a", "b"],
    "empty_list": [],
    "nested": {"pair": ["P1", "P2"], "deep": {"x": [1, {"y": None}]}, "n": None},
    "list_of_lists": [["a"], []],
    "mixed_list": ["a", 1, True],
    "list_with_none": ["a", None],
    "int_float_list": [1, 2.5],
    "str": "plain",
    "json_prefixed_str": "json: this is just text",
    "json_prefixed_valid": 'json:{"a": 1}',
    "int": 3,
    "float": 0.25,
    "bool": False,
    "unicode": "naïve ✓ 👍",
}


def storable(v) -> bool:
    """What Neo4j accepts as a property value: a primitive or a flat, homogeneous, null-free list."""
    if isinstance(v, (str, int, float, bool)):
        return True
    if isinstance(v, list):
        return all(isinstance(x, (str, int, float, bool)) for x in v) and len({type(x) for x in v}) <= 1
    return False


@pytest.mark.parametrize("name", sorted(SHAPES))
def test_enc_dec_round_trip(name):
    value = SHAPES[name]
    enc = _enc(value)
    assert storable(enc), (name, enc)
    assert _dec({"k": enc})["k"] == value
    assert type(_dec({"k": enc})["k"]) is type(value)


def test_dec_tolerates_legacy_raw_json_prefixed_strings():
    assert _dec({"k": "json: not json", "n": 5}) == {"k": "json: not json", "n": 5}


def test_every_property_core_stores_round_trips(tmp_path):
    """Run a broad scenario on the memory graph, then check every stored property is Neo4j-safe."""
    svc = Palinor(MemoryGraph(), EventBus("", tmp_path / "e.jsonl"), Config(data_dir=tmp_path, kafka_bootstrap=""))
    proj = svc.create_project("Codec", "goal", ["r1"], ["c1"])
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    lead = svc.auth(svc.join(pid, jk, "Lead", "claude", role="coordinator", admin_key=ak)["token"])
    a = svc.auth(svc.join(pid, jk, "A", "chatgpt", ["x"])["token"])
    b = svc.auth(svc.join(pid, jk, "B", "opencode")["token"])
    _scenario(svc, pid, lead, a, b)
    bad = [(n["id"], k, v) for n in svc.g.nodes.values() for k, v in n["props"].items()
           if v is not None and (not storable(_enc(v)) or _dec({"k": _enc(v)})["k"] != v)]
    assert bad == []


def _scenario(svc, pid, lead, a, b, sha=None):
    """Debate -> synthesis -> decision, messaging, ingestion, experiment, commands. Returns key ids."""
    q = "Which serializer?"
    svc.open_debate(lead, pid, q, context="pick one")
    pa = svc.create_proposal(a, q, "JSON", rationale="simple", evidence=[{"source": "s", "reliability": 0.6}])
    pb = svc.create_proposal(b, q, "Avro", assumptions=["schemas"], evidence=[{"source": "t", "reliability": 0.6}])
    pc = svc.create_proposal(lead, q, "Protobuf")
    for voter, prop, val in ((b, pa, "approve"), (lead, pa, "approve"), (a, pb, "approve"), (lead, pb, "approve"),
                             (a, pc, "reject")):
        svc.cast_vote(voter, prop["id"], val)
    tied = svc.cast_vote(b, pc["id"], "reject", rationale="too heavy")["laya"]
    assert tied["outcome"] == "SYNTHESIZE" and set(tied["synthesize"]) == {pa["id"], pb["id"]}
    s = svc.synthesis_create(lead, q, [pa["id"], pb["id"]], "JSON at the edge, Avro inside", "both")
    svc.cast_vote(a, s["id"], "approve")
    done = svc.cast_vote(b, s["id"], "approve")["laya"]
    assert done["outcome"] == "DECIDED" and done["selected"] == s["id"]
    dm = svc.send_message(a, pid, "B", "json: looks like an encoded value", kind="question",
                          context={"k": [1, 2], "nested": {"z": None}})
    rep = svc.send_message(b, pid, None, "answer", kind="answer", reply_to=dm["message_id"])
    f = svc.submit_finding(a, 'json:{"not": "a dict"}', evidence=[{"source": "x", "reliability": 0.7}])
    t = svc.create_task(lead, pid, "Benchmark", depends_on=[])
    cmd = svc.assign_task(lead, pid, t["id"], b["id"])
    svc.inbox(b)
    svc.command_update(b, cmd["id"], "ACCEPTED")
    svc.submit_experiment(b, s["id"], {"p99_ms": 12.5, "hist": [1, 2]}, True, commit_sha=sha or "abc123",
                          config={"threads": 4}, reproducible=True, task_id=t["id"])
    conv = svc.ingest_conversation(a, "Chat | one", "# Chat\n## Findings\n- f1\n## Questions\n- q1\n## Tasks\n- t1\n")
    svc.context_send(lead, "all", node_ids=[f["id"]], note="see this")
    return {"q": q, "dm": dm, "reply": rep, "finding": f, "conv": conv["conversation_id"], "synthesis": s["id"]}


# --------------------------------------------------------------- live Neo4j (optional)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "palinor-dev-pass"))


@pytest.fixture
def neo4j_svc(tmp_path):
    neo4j = pytest.importorskip("neo4j")
    try:
        drv = neo4j.GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH, connection_timeout=3)
        drv.verify_connectivity()
    except Exception as exc:  # noqa: BLE001 - any connection problem means "skip"
        pytest.skip(f"Neo4j not reachable at {NEO4J_URI}: {exc}")
    from palinor.graph import Neo4jGraph
    svc = Palinor(Neo4jGraph(NEO4J_URI, *NEO4J_AUTH), EventBus("", tmp_path / "e.jsonl"),
                  Config(data_dir=tmp_path, kafka_bootstrap=""))
    svc.sweep = lambda force=False: None  # never expire/offline other (live) projects from a test
    pids = []
    yield svc, pids
    with drv.session() as s:
        for pid in pids:
            s.run("MATCH (n:Node) WHERE n.project_id = $pid OR n.id = $pid DETACH DELETE n", pid=pid).consume()
            left = s.run("MATCH (n:Node) WHERE n.project_id = $pid OR n.id = $pid RETURN count(n) AS c",
                         pid=pid).single()["c"]
            assert left == 0
    drv.close()


def test_live_neo4j_full_scenario(neo4j_svc):
    svc, pids = neo4j_svc
    proj = svc.create_project(f"edge-test-{uuid.uuid4().hex[:6]}", "throwaway", ["r1"])
    pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
    pids.append(pid)
    lead = svc.auth(svc.join(pid, jk, "Lead", "claude", role="coordinator", admin_key=ak)["token"])
    a = svc.auth(svc.join(pid, jk, "A", "chatgpt")["token"])
    b = svc.auth(svc.join(pid, jk, "B", "opencode")["token"])
    ids = _scenario(svc, pid, lead, a, b, sha=uuid.uuid4().hex)
    inbox = svc.message_inbox(b)["messages"]
    dm = next(m for m in inbox if m["message_id"] == ids["dm"]["message_id"])
    assert dm["body"] == "json: looks like an encoded value"
    assert dm["context"] == {"k": [1, 2], "nested": {"z": None}}
    assert svc.message_thread(a, ids["dm"]["thread_id"])[0]["read_by"] == [b["id"]]
    assert svc.g.get(ids["finding"]["id"])["claim"] == 'json:{"not": "a dict"}'
    st = svc.debate_status(pid, ids["q"])
    assert st["laya_preview"]["outcome"] == "DECIDED" and st["laya_preview"]["selected"] == ids["synthesis"]
    d = svc.decisions(pid)[0]
    assert d["lineage"] and isinstance(d["ranking"], list) and isinstance(d["criteria"], dict)
    conv = svc.conversation_get(lead, ids["conv"])
    assert {x["label"] for x in conv["extracted"]} == {"Finding", "Task", "OpenQuestion"}
    md = svc.export_markdown(pid, a)
    assert "Chat \\| one" in md and "JSON at the edge, Avro inside" in md
    graph = svc.project_graph(pid)
    assert len(graph["nodes"]) > 20 and graph["edges"]
    ov = svc.overview(pid)
    assert ov["debates"][0]["laya"] == "DECIDED" and ov["messages"]
