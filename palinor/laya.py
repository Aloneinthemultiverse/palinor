"""Laya — PALINOR's decision chooser / adjudicator.

Laya is NOT a participant. It is a deterministic, versioned rule + score engine that
evaluates competing perspectives (proposals) on one question.

Two decision modes:
  empirical     - technical claims that can be tested. A winner needs a reproducible
                  supporting result tied to a commit SHA. Votes carry little weight.
  deliberative  - design / strategy / idea questions where no experiment exists.
                  Every eligible participant must have voted on every perspective
                  (quorum), votes carry more weight, evidence still matters.

Scoring (per perspective):
  1. Requirement gate  - violating a project requirement excludes the perspective.
  2. Evidence          - noisy-OR over cited evidence reliability (synthesis inherits
                         the evidence of the perspectives it combines).
  3. Results           - reproducible results dominate (1.0), others count 0.15,
                         refuting results subtract.
  4. Votes             - confidence-weighted net approval of OTHER participants.

Outcomes:
  DECIDED            - clear winner, recorded with rationale and dissent
  NEED_PERSPECTIVES  - nothing admissible to decide between
  NEED_VOTES         - quorum not reached; lists who still has to vote on what
  VERIFY             - empirical mode lacks a reproducible result (→ verification task)
  NEED_EVIDENCE      - deliberative winner too weak
  SYNTHESIZE         - top two are too close: combine them into one perspective and vote again
  ESCALATE           - still tied after synthesis: human / coordinator tie-break
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass

POLICY_VERSION = "laya-rules-2.0"
MODES = ("empirical", "deliberative")


@dataclass
class Policy:
    mode: str = "empirical"
    w_evidence: float = 0.25
    w_results: float = 0.60
    w_votes: float = 0.15
    min_score: float = 0.35
    min_margin: float = 0.10
    require_reproducible: bool = True
    quorum: float = 0.0  # fraction of eligible voters that must vote on each perspective
    min_votes: int = 0   # deliberative: the winner needs at least this many votes from others
    version: str = POLICY_VERSION

    def as_dict(self) -> dict:
        return asdict(self)


def policy_for(mode: str) -> Policy:
    if mode == "deliberative":
        return Policy(mode="deliberative", w_evidence=0.35, w_results=0.20, w_votes=0.45,
                      min_score=0.30, min_margin=0.08, require_reproducible=False, quorum=1.0, min_votes=1)
    if mode != "empirical":
        raise ValueError(f"mode must be one of {MODES}")
    return Policy()


def _evidence_score(reliabilities: list[float]) -> float:
    miss = 1.0
    for r in reliabilities:
        r = float(r)
        if not math.isfinite(r):  # NaN/inf must never reach the ranking
            continue
        miss *= 1.0 - min(max(r, 0.0), 0.95)
    return 1.0 - miss


def _result_score(results: list[dict]) -> tuple[float, int]:
    total, reproducible_support = 0.0, 0
    for r in results:
        weight = 1.0 if r.get("reproducible") and r.get("commit_sha") else 0.15
        if r.get("supports"):
            total += weight
            reproducible_support += int(weight == 1.0)
        else:
            total -= weight
    pos = max(total, 0.0)
    return pos / (0.5 + pos), reproducible_support  # one reproducible result -> 0.67


def _vote_score(votes: list[dict]) -> float:
    if not votes:
        return 0.0
    votes = [v for v in votes if math.isfinite(float(v.get("confidence", 1.0)))]
    num = sum((1 if v["value"] == "approve" else -1) * float(v.get("confidence", 1.0)) for v in votes)
    den = sum(float(v.get("confidence", 1.0)) for v in votes) or 1.0
    return max(num / den, 0.0)


def evaluate(proposals: list[dict], policy: Policy | None = None,
             eligible_voters: list[str] | None = None) -> dict:
    """proposals: [{id, approach, author, kind, derived_from[], violates[], evidence[float],
                    results[{supports, reproducible, commit_sha}], votes[{voter, value, confidence}]}]"""
    policy = policy or Policy()
    eligible = set(eligible_voters or [])
    scored, excluded, reasons, missing = [], [], [], []
    pending_votes: dict[str, list[str]] = {}

    for p in proposals:
        if p.get("violates"):
            excluded.append({"proposal_id": p["id"], "violates": p["violates"]})
            reasons.append(f"{p['id']} excluded: violates requirement(s) {p['violates']}")
            continue
        votes = [v for v in p.get("votes", []) if v.get("voter") != p.get("author")]
        if policy.quorum > 0:
            need = eligible - {p.get("author")}
            have = {v.get("voter") for v in votes}
            if need and len(need & have) / len(need) < policy.quorum:
                pending_votes[p["id"]] = sorted(need - have)
        # supporting evidence, discounted by evidence that contradicts the perspective
        ev = _evidence_score(p.get("evidence", [])) * (1.0 - _evidence_score(p.get("counter_evidence", [])))
        res, repro = _result_score(p.get("results", []))
        vt = _vote_score(votes)
        score = policy.w_evidence * ev + policy.w_results * res + policy.w_votes * vt
        if not math.isfinite(score):
            score = 0.0
        scored.append({"proposal_id": p["id"], "approach": p.get("approach"), "author": p.get("author"),
                       "kind": p.get("kind", "perspective"), "derived_from": p.get("derived_from", []),
                       "score": round(score, 4), "evidence_score": round(ev, 4), "result_score": round(res, 4),
                       "vote_score": round(vt, 4), "votes": len(votes), "reproducible_support": repro})
    scored.sort(key=lambda s: (s["score"], s["kind"] == "synthesis"), reverse=True)

    # A synthesis supersedes the perspectives it combines unless the group clearly prefers an original
    # (original beats the synthesis by at least min_margin). Superseded options stay ranked but don't contend.
    by_id = {s["proposal_id"]: s for s in scored}
    for s in scored:
        if s["kind"] == "synthesis":
            for src in s["derived_from"]:
                x = by_id.get(src)
                # compare the rounded gap: 0.3 - 0.2 must count as a full 0.1 margin (float noise)
                if x and round(x["score"] - s["score"], 4) < policy.min_margin:
                    x["superseded_by"] = s["proposal_id"]
    contenders = [s for s in scored if not s.get("superseded_by")] or scored
    synthesis_rounds = sum(1 for s in scored if s["kind"] == "synthesis")

    outcome, selected, synthesize_pair = "DECIDED", None, None
    if not scored:
        outcome = "NEED_PERSPECTIVES"
        missing.append("no admissible perspective (all excluded or none submitted)")
    elif pending_votes:
        outcome = "NEED_VOTES"
        missing += [f"{pid}: waiting for votes from {', '.join(v)}" for pid, v in pending_votes.items()]
    else:
        top = contenders[0]
        margin = round(top["score"] - (contenders[1]["score"] if len(contenders) > 1 else 0.0), 4)
        if policy.require_reproducible and top["reproducible_support"] == 0:
            outcome = "VERIFY"
            missing.append(f"{top['proposal_id']} has no reproducible supporting result tied to a commit SHA")
        elif len(contenders) > 1 and margin < policy.min_margin:
            a, b = contenders[0], contenders[1]
            covered = any({a["proposal_id"], b["proposal_id"]} <= set(s["derived_from"]) for s in scored)
            if covered or synthesis_rounds >= 3:  # bounded: never loop on synthesis forever
                outcome = "ESCALATE"
                missing.append(f"still tied after synthesis ({a['proposal_id']} vs {b['proposal_id']}, "
                               f"margin {round(margin, 4)}): coordinator or human tie-break needed")
            else:
                outcome = "SYNTHESIZE"
                synthesize_pair = [a["proposal_id"], b["proposal_id"]]
                missing.append(f"{a['proposal_id']} and {b['proposal_id']} are too close (margin {round(margin, 4)} "
                               f"< {policy.min_margin}): combine them into one synthesis and vote on it")
        elif top["votes"] < policy.min_votes:
            # a lone author with self-rated evidence must never decide a deliberative question
            outcome = "NEED_VOTES"
            missing.append(f"{top['proposal_id']} needs at least {policy.min_votes} vote(s) from other participants")
        elif top["score"] < policy.min_score:
            outcome = "VERIFY" if policy.mode == "empirical" else "NEED_EVIDENCE"
            missing.append(f"top score {top['score']} below threshold {policy.min_score}")
        if outcome == "DECIDED":
            selected = top["proposal_id"]
            reasons.append(f"{selected} selected ({policy.mode}): score {top['score']}, margin {round(margin, 4)}, "
                           f"{top['votes']} vote(s), {top['reproducible_support']} reproducible result(s)")

    return {"outcome": outcome, "selected": selected, "synthesize": synthesize_pair, "ranking": scored,
            "contenders": [s["proposal_id"] for s in contenders],
            "excluded": excluded, "pending_votes": pending_votes, "reasons": reasons, "missing": missing,
            "policy": policy.as_dict()}
