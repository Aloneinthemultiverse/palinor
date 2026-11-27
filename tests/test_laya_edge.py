"""Laya unit tests: every outcome and the synthesis / quorum / gate rules, on the pure scoring engine."""
import pytest

from palinor import laya
from palinor.laya import Policy, evaluate, policy_for

DELIB = policy_for("deliberative")
EMPIRICAL = policy_for("empirical")
EV_ONLY = Policy(mode="deliberative", w_evidence=1.0, w_results=0.0, w_votes=0.0, min_score=0.0,
                 min_margin=0.10, require_reproducible=False, quorum=0.0)  # score == evidence reliability


def P(pid, author, *, ev=(), results=(), votes=(), kind="perspective", derived=(), violates=()):
    return {"id": pid, "approach": f"approach {pid}", "author": author, "kind": kind,
            "derived_from": list(derived), "violates": list(violates), "evidence": list(ev),
            "results": list(results),
            "votes": [{"voter": v, "value": val, "confidence": c} for v, val, c in votes]}


def yes(*voters, c=1.0):
    return [(v, "approve", c) for v in voters]


def no(*voters, c=1.0):
    return [(v, "reject", c) for v in voters]


REPRO = {"supports": True, "reproducible": True, "commit_sha": "abc"}


def test_need_perspectives_when_empty_or_all_excluded():
    assert evaluate([], DELIB, ["a"])["outcome"] == "NEED_PERSPECTIVES"
    v = evaluate([P("P1", "a", violates=["latency"])], DELIB, ["a", "b"])
    assert v["outcome"] == "NEED_PERSPECTIVES" and v["selected"] is None
    assert v["excluded"] == [{"proposal_id": "P1", "violates": ["latency"]}] and v["ranking"] == []


def test_requirement_gate_excludes_even_the_strongest_option():
    strong = P("P1", "a", ev=[0.9], results=[REPRO, REPRO], violates=["budget"])
    weak = P("P2", "b", results=[REPRO])
    v = evaluate([strong, weak], EMPIRICAL)
    assert v["outcome"] == "DECIDED" and v["selected"] == "P2"
    assert [r["proposal_id"] for r in v["ranking"]] == ["P2"]
    assert any("P1 excluded" in r for r in v["reasons"])


def test_need_votes_lists_exactly_who_still_owes_a_vote():
    props = [P("P1", "a", votes=yes("b")), P("P2", "b")]
    v = evaluate(props, DELIB, ["a", "b", "c"])
    assert v["outcome"] == "NEED_VOTES" and v["selected"] is None
    assert v["pending_votes"] == {"P1": ["c"], "P2": ["a", "c"]}  # authors never owe a vote on their own
    assert len(v["missing"]) == 2


def test_votes_from_non_eligible_voters_do_not_fill_quorum_but_still_score():
    props = [P("P1", "a", votes=yes("human")), P("P2", "b", votes=yes("a"))]
    v = evaluate(props, DELIB, ["a", "b"])
    assert v["pending_votes"] == {"P1": ["b"]}
    assert next(r for r in v["ranking"] if r["proposal_id"] == "P1")["vote_score"] == 1.0


def test_verify_without_reproducible_result_and_with_net_zero_results():
    v = evaluate([P("P1", "a", ev=[0.9], votes=yes("b", "c"))], EMPIRICAL, ["a", "b", "c"])
    assert v["outcome"] == "VERIFY" and "reproducible" in v["missing"][0]
    refuted = {"supports": False, "reproducible": True, "commit_sha": "def"}
    v = evaluate([P("P1", "a", results=[REPRO, refuted])], EMPIRICAL)
    assert v["outcome"] == "VERIFY" and "below threshold" in v["missing"][0]  # repro exists, score too low


def test_need_evidence_when_deliberative_winner_is_weak():
    props = [P("P1", "a", ev=[0.5], votes=no("b")), P("P2", "b", votes=no("a"))]  # 0.175 vs 0: clear but weak
    v = evaluate(props, DELIB, ["a", "b"])
    assert v["outcome"] == "NEED_EVIDENCE" and "below threshold" in v["missing"][0]
    v = evaluate([P("P1", "a", votes=no("b"))], DELIB, ["a", "b"])
    assert v["outcome"] == "NEED_EVIDENCE"


def test_synthesize_pair_when_top_two_are_close():
    props = [P("P1", "a", votes=yes("b", "c")), P("P2", "b", votes=yes("a", "c")),
             P("P3", "c", votes=no("a", "b"))]
    v = evaluate(props, DELIB, ["a", "b", "c"])
    assert v["outcome"] == "SYNTHESIZE" and v["selected"] is None
    assert sorted(v["synthesize"]) == ["P1", "P2"]


def test_synthesis_supersedes_its_sources_and_wins():
    props = [P("P1", "a", votes=yes("b", "c")), P("P2", "b", votes=yes("a", "c")),
             P("S", "c", kind="synthesis", derived=["P1", "P2"], votes=yes("a", "b"))]
    v = evaluate(props, DELIB, ["a", "b", "c"])
    assert v["outcome"] == "DECIDED" and v["selected"] == "S"
    assert v["contenders"] == ["S"]
    ranked = {r["proposal_id"]: r for r in v["ranking"]}
    assert ranked["P1"].get("superseded_by") == "S" and ranked["P2"].get("superseded_by") == "S"


def test_original_clearly_preferred_over_its_synthesis_stays_a_contender():
    props = [P("P1", "a", votes=yes("b", "c")), P("P2", "b", votes=no("a", "c")),
             P("S", "c", kind="synthesis", derived=["P1", "P2"], votes=no("a", "b"))]
    v = evaluate(props, DELIB, ["a", "b", "c"])
    ranked = {r["proposal_id"]: r for r in v["ranking"]}
    assert "superseded_by" not in ranked["P1"] and ranked["P2"].get("superseded_by") == "S"
    assert v["outcome"] == "DECIDED" and v["selected"] == "P1"


def test_original_beating_synthesis_by_exactly_min_margin_is_not_superseded():
    # 0.3 - 0.2 == min_margin (0.1): "beats by at least min_margin" must hold despite float noise
    props = [P("A", "a", ev=[0.3]), P("B", "b", ev=[]),
             P("S", "c", kind="synthesis", derived=["A", "B"], ev=[0.2])]
    v = evaluate(props, EV_ONLY)
    ranked = {r["proposal_id"]: r for r in v["ranking"]}
    assert "superseded_by" not in ranked["A"]
    assert v["outcome"] == "DECIDED" and v["selected"] == "A"


def test_lead_of_exactly_min_margin_is_decisive_not_a_tie():
    v = evaluate([P("A", "a", ev=[0.6]), P("B", "b", ev=[0.5])], EV_ONLY)  # 0.6 - 0.5 == 0.1
    assert v["outcome"] == "DECIDED" and v["selected"] == "A"
    v = evaluate([P("A", "a", ev=[0.6]), P("B", "b", ev=[0.51])], EV_ONLY)
    assert v["outcome"] == "SYNTHESIZE"


def test_escalate_when_still_tied_after_synthesis():
    props = [P("P1", "a", votes=yes("b", "c")), P("P2", "b", votes=yes("a", "c")),
             P("S", "c", kind="synthesis", derived=["P1", "P2"], votes=no("a", "b"))]
    v = evaluate(props, DELIB, ["a", "b", "c"])
    assert v["outcome"] == "ESCALATE" and v["selected"] is None and v["synthesize"] is None
    assert "tie-break" in v["missing"][0]


def test_synthesis_rounds_are_bounded():
    def scenario(n_synth):
        tied = [P("C1", "a", ev=[0.6]), P("C2", "b", ev=[0.6])]
        synths = [P(f"S{i}", "c", kind="synthesis", derived=[f"X{i}", f"Y{i}"]) for i in range(n_synth)]
        return evaluate(tied + synths, EV_ONLY)
    assert scenario(2)["outcome"] == "SYNTHESIZE"  # unrelated syntheses: still worth one more try
    assert scenario(3)["outcome"] == "ESCALATE"    # third round reached: stop asking for syntheses


def test_self_votes_are_ignored_in_scoring_and_quorum():
    props = [P("P1", "a", votes=yes("a") + no("b")), P("P2", "b", votes=no("a"))]
    v = evaluate(props, DELIB, ["a", "b"])
    p1 = next(r for r in v["ranking"] if r["proposal_id"] == "P1")
    assert p1["votes"] == 1 and p1["vote_score"] == 0.0
    v = evaluate([P("P1", "a", votes=yes("a"))], DELIB, ["a", "b"])
    assert v["pending_votes"] == {"P1": ["b"]}  # own vote never satisfies quorum


def test_confidence_weighting_and_policy_validation():
    v = evaluate([P("P1", "a", votes=[("b", "approve", 0.9), ("c", "reject", 0.1)])], DELIB, ["a", "b", "c"])
    assert v["ranking"][0]["vote_score"] == 0.8
    with pytest.raises(ValueError):
        policy_for("vibes")
    assert v["policy"]["version"] == laya.POLICY_VERSION
