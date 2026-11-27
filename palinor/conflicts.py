"""Conflict protocol — a worker challenges the knowledge graph, the coordinator consults and resolves.

    ChatGPT-1 (worker)  conflict_raise(about=[F-12], claim, evidence)     → ONE DM to the resolver
    Claude-1 (resolver) conflict_consult(C-7, ask=[OpenCode-1], question) → ONE DM per consultant, with context
    OpenCode-1          conflict_opinion(C-7, position, rationale, refs)  → ONE DM back to the resolver
    Claude-1            conflict_resolve(C-7, keep | accept_challenge | merge, rationale)
                        → graph updated, Laya audits the evidence, result sent ONLY to the people involved

Anti-hallucination: every reference must be a real node of this project (unknown ids are rejected, so an
invented id fails loudly); an opinion must cite nodes or evidence; Laya records whether the resolver chose
the side the evidence supports. Anti-noise: no broadcasts, one thread per conflict, duplicate raises reuse
the open conflict, consultation is limited to three participants.

A coordinator never overturns a Laya decision single-handedly: accepting a challenge against a Decision
re-opens the question as a new debate instead.
"""
from __future__ import annotations

from . import laya
from .core import ROLE_PERMS, Forbidden, NotFound, PalinorError, capped, iso, nid, unit

CHALLENGEABLE = {"Finding", "Proposal", "Decision", "Task", "Result", "Evidence", "Conversation", "Experiment"}
POSITIONS = {"keep", "accept_challenge", "merge", "unsure"}
RESOLUTIONS = {"keep", "accept_challenge", "merge"}
MAX_CONSULT = 3


def _grounded(svc, project_id: str, refs: list[str], allowed: set[str] | None = None) -> list[dict]:
    """Every id must exist in this project — invented references fail loudly."""
    nodes, missing = [], []
    for r in refs or []:
        n = svc.g.get(r)
        if not n or n.get("project_id") != project_id or (allowed and n["_label"] not in allowed):
            missing.append(r)
        else:
            nodes.append(n)
    if missing:
        raise NotFound(f"unknown or non-citable node id(s): {missing}. Cite ids exactly as returned by "
                       f"context_get / pipeline_status; do not invent them")
    return nodes


def _resolver_for(svc, project_id: str, raiser: dict, task_id: str = "") -> str:
    """The coordinator responsible for the raiser's work; never ambiguous with several Claudes."""
    if task_id:
        task = svc.g.get(task_id)
        if task and task.get("plan_id"):
            return svc.g.get(task["plan_id"])["owner"]
        for c in sorted(svc.g.find("Command", task_id=task_id), key=lambda c: c["created_at"], reverse=True):
            issuer = svc.g.get(c.get("issued_by") or "")
            if issuer and issuer.get("_label") == "Participant" and "command" in ROLE_PERMS.get(issuer["role"], set()):
                return issuer["id"]
    coords = sorted((p for p in svc.g.find("Participant", project_id=project_id)
                     if p.get("role") == "coordinator" and p["id"] != raiser["id"]),
                    key=lambda p: (p.get("status") != "online", p["joined_at"]))
    if coords:
        return coords[0]["id"]
    human = svc.g.get(f"human-{project_id}")
    if human:
        return human["id"]
    raise PalinorError("no coordinator in this project to resolve conflicts; ask the user to join one")


def _package(svc, conflict: dict) -> dict:
    """Exactly the knowledge the conflict is about — nothing more, nothing invented."""
    about = []
    for n_id in conflict["about"]:
        n = svc.g.get(n_id)
        if n:
            about.append({"id": n["id"], "label": n["_label"],
                          "text": n.get("claim") or n.get("approach") or n.get("selected_approach")
                          or n.get("title") or n.get("metrics"),
                          "by": n.get("submitted_by") or n.get("author") or n.get("created_by"),
                          "status": n.get("status"),
                          "evidence": [{"source": e["source"], "uri": e["uri"], "reliability": e["reliability"]}
                                       for e in svc.g.out(n["id"], "SUPPORTED_BY", "Evidence")]})
    return {"conflict_id": conflict["id"], "claim": conflict["claim"], "raised_by": conflict["raised_by"],
            "challenged": about,
            "challenge_evidence": [{"source": e["source"], "uri": e["uri"], "reliability": e["reliability"]}
                                   for e in svc.g.out(conflict["id"], "SUPPORTED_BY", "Evidence")]}


def raise_conflict(svc, p: dict, about: list[str], claim: str, evidence: list[dict] | None = None,
                   task_id: str = "", severity: str = "normal") -> dict:
    svc._require(p, "write")
    pid = p["project_id"]
    claim = capped(claim, "claim").strip()
    if not claim:
        raise PalinorError("state the conflict: what in the graph is wrong and why")
    if not about:
        raise PalinorError("'about' must list the node ids you challenge (from context_get)")
    if severity not in {"low", "normal", "high"}:
        raise PalinorError("severity must be low|normal|high")
    targets = _grounded(svc, pid, sorted(set(about)), CHALLENGEABLE)
    if task_id:
        svc._node(task_id, "Task", pid)
    for c in svc.g.find("Conflict", project_id=pid, raised_by=p["id"]):
        if c["status"] in {"open", "consulting"} and set(c["about"]) == {t["id"] for t in targets}:
            return {**svc._public(c), "duplicate": True, "note": "you already raised this; it is still open"}
    resolver = _resolver_for(svc, pid, p, task_id)
    cid = nid("CF")
    conflict = svc.g.upsert("Conflict", cid, {
        "conflict_id": cid, "project_id": pid, "claim": claim, "about": [t["id"] for t in targets],
        "raised_by": p["id"], "resolver": resolver, "severity": severity, "status": "open",
        "task_id": task_id or "", "consulted": [], "created_at": iso()})
    svc.g.link(p["id"], "RAISED", cid)
    for t in targets:
        svc.g.link(cid, "CHALLENGES", t["id"])
    for ev in evidence or []:
        svc._add_evidence(p, cid, ev)
    msg = svc.send_message(None, pid, resolver,
                           f"{p['name']} challenges {', '.join(t['id'] for t in targets)}:\n{claim}\n\n"
                           f"Next: consult someone (conflict_consult) or resolve it (conflict_resolve).",
                           kind="alert", subject=f"Conflict {cid} ({severity}) from {p['name']}",
                           refs=[cid] + [t["id"] for t in targets], sender_id=p["id"],
                           context=_package(svc, conflict))
    svc.g.upsert("Conflict", cid, {"thread_id": msg["thread_id"]})
    svc._emit(pid, "CONFLICT_RAISED", p["id"], task_id or None, target_participant_id=resolver,
              payload={"conflict_id": cid, "about": [t["id"] for t in targets], "severity": severity,
                       "evidence": len(evidence or [])})
    return {"conflict_id": cid, "resolver": resolver, "status": "open", "thread_id": msg["thread_id"]}


def _own(svc, p: dict, conflict_id: str) -> dict:
    c = svc._node(conflict_id, "Conflict", p["project_id"])
    if c["resolver"] != p["id"]:
        raise Forbidden(f"only the resolver ({c['resolver']}) can do this")
    if c["status"] not in {"open", "consulting"}:
        raise PalinorError(f"conflict is already {c['status']}")
    return c


def consult(svc, p: dict, conflict_id: str, ask: list[str], question: str) -> dict:
    c = _own(svc, p, conflict_id)
    pid = p["project_id"]
    who = svc._resolve_recipients(pid, ask)
    if who == ["all"] or not who:
        raise PalinorError("consult specific participants (at most 3), not everyone")
    who = [w for w in who if w != p["id"]]
    consulted = sorted(set(c.get("consulted") or []) | set(who))
    if len(consulted) > MAX_CONSULT:
        raise PalinorError(f"consult at most {MAX_CONSULT} participants per conflict")
    question = capped(question, "question").strip() or "Which side does the evidence support?"
    for w in who:
        svc.send_message(None, pid, w,
                         f"{p['name']} asks your opinion on conflict {conflict_id}: {question}\n"
                         f"Answer once with conflict_opinion(conflict_id, position=keep|accept_challenge|merge|"
                         f"unsure, rationale, refs=[node ids you rely on]). Cite only ids from the attached "
                         f"context or context_get. If you do not know, answer 'unsure'.",
                         kind="question", subject=f"Opinion needed on {conflict_id}", refs=[conflict_id],
                         sender_id=p["id"], context=_package(svc, c), thread_id=c.get("thread_id", ""))
    svc.g.upsert("Conflict", conflict_id, {"status": "consulting", "consulted": consulted,
                                           "consult_question": question})
    svc._emit(pid, "CONFLICT_CONSULTED", p["id"], payload={"conflict_id": conflict_id, "asked": who})
    return {"conflict_id": conflict_id, "consulted": consulted}


def give_opinion(svc, p: dict, conflict_id: str, position: str, rationale: str, refs: list[str] | None = None,
                 evidence: list[dict] | None = None, confidence: float = 0.7) -> dict:
    svc._require(p, "write")
    pid = p["project_id"]
    c = svc._node(conflict_id, "Conflict", pid)
    if p["id"] not in (c.get("consulted") or []):
        raise Forbidden("you were not asked for an opinion on this conflict")
    if c["status"] != "consulting":
        raise PalinorError(f"conflict is {c['status']}; opinions are closed")
    if position not in POSITIONS:
        raise PalinorError(f"position must be one of {sorted(POSITIONS)}")
    rationale = capped(rationale, "rationale").strip()
    if not rationale:
        raise PalinorError("give a rationale")
    cited = _grounded(svc, pid, sorted(set(refs or [])))
    if position != "unsure" and not cited and not evidence:
        raise PalinorError("ground your opinion: cite node ids (refs) or attach evidence, or answer 'unsure'")
    oid = f"OPN-{conflict_id}-{p['id']}"  # one opinion per participant; answering again replaces it
    op = svc.g.upsert("Opinion", oid, {"project_id": pid, "conflict_id": conflict_id, "position": position,
                                       "rationale": rationale, "refs": [n["id"] for n in cited],
                                       "confidence": unit(confidence, "confidence"), "by": p["id"],
                                       "created_at": iso()})
    svc.g.link(oid, "ON", conflict_id)
    for n in cited:
        svc.g.link(oid, "CITES", n["id"])
    for ev in evidence or []:
        svc._add_evidence(p, oid, ev)
    answered = {o["by"] for o in svc.g.inn(conflict_id, "ON", "Opinion")}
    pending = sorted(set(c["consulted"]) - answered)
    svc.send_message(None, pid, c["resolver"],
                     f"{p['name']} on {conflict_id}: {position} — {rationale}"
                     + (f"\nCites: {', '.join(n['id'] for n in cited)}" if cited else "")
                     + ("\nAll consulted participants have answered: you can resolve now." if not pending
                        else f"\nStill waiting for: {', '.join(pending)}"),
                     kind="answer", subject=f"Opinion on {conflict_id}: {position}", refs=[conflict_id, oid],
                     sender_id=p["id"], thread_id=c.get("thread_id", ""))
    svc._emit(pid, "CONFLICT_OPINION", p["id"], payload={"conflict_id": conflict_id, "position": position,
                                                         "cites": len(cited), "pending": len(pending)})
    return {**svc._public(op), "pending": pending}


def _side_scores(svc, c: dict) -> dict:
    """Laya's audit: how much evidence backs the existing knowledge vs the challenge."""
    existing, challenge = [], [e["reliability"] for e in svc.g.out(c["id"], "SUPPORTED_BY", "Evidence")]
    for n_id in c["about"]:
        existing += [e["reliability"] for e in svc.g.out(n_id, "SUPPORTED_BY", "Evidence")]
    for o in svc.g.inn(c["id"], "ON", "Opinion"):
        own = [e["reliability"] for e in svc.g.out(o["id"], "SUPPORTED_BY", "Evidence")]
        cited = [e["reliability"] for r in o.get("refs") or [] for e in svc.g.out(r, "SUPPORTED_BY", "Evidence")]
        if o["position"] == "keep":
            existing += own + cited
        elif o["position"] == "accept_challenge":
            challenge += own + cited
    return {"existing": round(laya._evidence_score(existing), 4), "challenge": round(laya._evidence_score(challenge), 4)}


def resolve(svc, p: dict, conflict_id: str, resolution: str, rationale: str, merged_statement: str = "",
            force: bool = False) -> dict:
    c = _own(svc, p, conflict_id)
    pid = p["project_id"]
    if resolution not in RESOLUTIONS:
        raise PalinorError(f"resolution must be one of {sorted(RESOLUTIONS)}")
    rationale = capped(rationale, "rationale").strip()
    if not rationale:
        raise PalinorError("explain the resolution (rationale)")
    opinions = svc.g.inn(conflict_id, "ON", "Opinion")
    pending = sorted(set(c.get("consulted") or []) - {o["by"] for o in opinions})
    if pending and not force:
        raise PalinorError(f"waiting for opinions from {pending}; pass force=true to resolve without them")
    if resolution == "merge" and not merged_statement.strip():
        raise PalinorError("a merge needs merged_statement: the corrected knowledge")
    scores = _side_scores(svc, c)
    chosen = {"keep": "existing", "accept_challenge": "challenge"}.get(resolution)
    other = {"existing": "challenge", "challenge": "existing"}.get(chosen or "")
    against = bool(chosen and scores[other] - scores[chosen] >= 0.2)

    effects, reopened = [], None
    for n_id in c["about"]:
        n = svc.g.get(n_id)
        if not n:
            continue
        if resolution == "keep":
            if n["_label"] == "Finding":
                svc.g.upsert("Finding", n_id, {"status": "confirmed"})
                effects.append(f"{n_id} confirmed")
        elif n["_label"] == "Decision":
            # a coordinator never flips a Laya decision alone: the question goes back to a debate
            reopened = svc.open_debate(p, pid, n["question"], n.get("mode") or "deliberative",
                                       f"Re-opened by conflict {conflict_id}: {c['claim']}")
            effects.append(f"{n_id} re-opened as debate {reopened['id']}")
        elif n["_label"] in {"Finding", "Proposal", "Result"}:
            svc.g.upsert(n["_label"], n_id, {"status": "superseded", "superseded_by": conflict_id})
            effects.append(f"{n_id} superseded")
    new_finding = None
    if resolution == "merge":
        new_finding = svc.submit_finding(p, merged_statement, f"merged resolution of {conflict_id}",
                                         confidence=0.8)
        svc.g.upsert("Finding", new_finding["id"], {"status": "confirmed", "kind": "resolution"})
        svc.g.link(new_finding["id"], "RESOLVES", conflict_id)
        effects.append(f"{new_finding['id']} created (merged)")
    elif resolution == "accept_challenge":
        accepted = svc.submit_finding(p, c["claim"], f"challenge accepted in {conflict_id}", confidence=0.8)
        svc.g.upsert("Finding", accepted["id"], {"status": "confirmed", "kind": "resolution"})
        svc.g.link(accepted["id"], "RESOLVES", conflict_id)
        effects.append(f"{accepted['id']} records the accepted challenge")

    did = nid("D")
    svc.g.upsert("Decision", did, {
        "decision_id": did, "project_id": pid, "question": f"Conflict {conflict_id}: {c['claim'][:300]}",
        "outcome": "RESOLVED_BY_COORDINATOR", "mode": "conflict", "selected_option": resolution,
        "selected_approach": merged_statement or resolution, "resolver": p["id"], "rationale": [rationale],
        "opinions": [{"by": o["by"], "position": o["position"], "rationale": o["rationale"], "refs": o.get("refs")}
                     for o in opinions],
        "evidence_audit": {**scores, "chosen": chosen or "merge", "against_evidence": against},
        "effects": effects, "dissent": [{"by": o["by"], "position": o["position"]} for o in opinions
                                        if o["position"] not in {resolution, "unsure"}],
        "policy_version": laya.POLICY_VERSION, "created_at": iso()})
    svc.g.link(did, "RESOLVES", conflict_id)
    svc.g.link(did, "MADE_BY", p["id"])
    svc.g.upsert("Conflict", conflict_id, {"status": "resolved", "resolution": resolution, "decision_id": did,
                                           "resolved_at": iso()})
    svc._emit(pid, "CONFLICT_RESOLVED", p["id"], payload={
        "conflict_id": conflict_id, "resolution": resolution, "opinions": len(opinions),
        "against_evidence": against, "forced": bool(pending)})

    involved = {c["raised_by"], *(c.get("consulted") or [])}
    for n_id in c["about"]:
        n = svc.g.get(n_id) or {}
        author = n.get("submitted_by") or n.get("author")
        if author and svc.g.get(author):
            involved.add(author)
    involved.discard(p["id"])
    warning = (f"\n⚠ Laya audit: the chosen side has weaker evidence (existing {scores['existing']} vs "
               f"challenge {scores['challenge']}); the resolver's rationale is recorded.") if against else ""
    if involved:
        svc.send_message(None, pid, sorted(involved),
                         f"{p['name']} resolved {conflict_id}: {resolution}. {rationale}\n"
                         f"Effects: {'; '.join(effects) or 'none'}{warning}",
                         kind="decision", subject=f"Resolved {conflict_id}: {resolution}",
                         refs=[conflict_id, did], sender_id=p["id"], thread_id=c.get("thread_id", ""))
    return {"conflict_id": conflict_id, "decision_id": did, "resolution": resolution, "effects": effects,
            "evidence_audit": {**scores, "against_evidence": against},
            "reopened_debate": reopened["id"] if reopened else None, "notified": sorted(involved)}


def withdraw(svc, p: dict, conflict_id: str, reason: str) -> dict:
    c = svc._node(conflict_id, "Conflict", p["project_id"])
    if c["raised_by"] != p["id"]:
        raise Forbidden("only the participant who raised a conflict can withdraw it")
    if c["status"] not in {"open", "consulting"}:
        raise PalinorError(f"conflict is already {c['status']}")
    svc.g.upsert("Conflict", conflict_id, {"status": "withdrawn", "withdraw_reason": capped(reason, "reason")})
    svc._emit(p["project_id"], "CONFLICT_WITHDRAWN", p["id"], payload={"conflict_id": conflict_id})
    to = sorted({c["resolver"], *(c.get("consulted") or [])} - {p["id"]})
    if to:
        svc.send_message(None, p["project_id"], to, f"{p['name']} withdrew {conflict_id}: {reason}",
                         kind="info", subject=f"Withdrawn {conflict_id}", refs=[conflict_id], sender_id=p["id"],
                         thread_id=c.get("thread_id", ""))
    return {"conflict_id": conflict_id, "status": "withdrawn"}


def conflict_status(svc, project_id: str, conflict_id: str = "") -> dict:
    items = [svc._node(conflict_id, "Conflict", project_id)] if conflict_id else \
        sorted(svc.g.find("Conflict", project_id=project_id), key=lambda c: c["created_at"])
    out = []
    for c in items:
        ops = svc.g.inn(c["id"], "ON", "Opinion")
        out.append({**svc._public(c), "package": _package(svc, c),
                    "opinions": [{"by": o["by"], "position": o["position"], "rationale": o["rationale"],
                                  "refs": o.get("refs"), "confidence": o.get("confidence")} for o in ops],
                    "pending": sorted(set(c.get("consulted") or []) - {o["by"] for o in ops}),
                    "evidence_audit": _side_scores(svc, c)})
    return {"conflicts": out}


def pending_actions(svc, p: dict) -> list[dict]:
    acts = []
    for c in svc.g.find("Conflict", project_id=p["project_id"]):
        if c["status"] not in {"open", "consulting"}:
            continue
        answered = {o["by"] for o in svc.g.inn(c["id"], "ON", "Opinion")}
        if c["resolver"] == p["id"]:
            pending = sorted(set(c.get("consulted") or []) - answered)
            acts.append({"action": "resolve_conflict" if not pending else "await_opinions",
                         "conflict_id": c["id"], "claim": c["claim"][:200], "waiting_for": pending})
        elif p["id"] in (c.get("consulted") or []) and p["id"] not in answered:
            acts.append({"action": "give_opinion", "conflict_id": c["id"], "claim": c["claim"][:200]})
    return acts
