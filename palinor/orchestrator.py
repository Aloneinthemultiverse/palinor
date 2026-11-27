"""Orchestrator — one coordinator (e.g. Claude) divides the work, PALINOR runs the pipeline.

    coordinator: plan_submit(goal, tasks=[{key, title, capability, depends_on, assignee}...])
        → tasks become a DAG (validated: unique keys, known deps, no cycles)
        → every READY task is dispatched to the best participant:
             explicit assignee if online, else capability match → least loaded → workers before coordinator
        → worker: command (START_TASK with inputs + acceptance criteria) → ACCEPTED → RUNNING → COMPLETED(summary)
        → hand-off: dependents whose inputs are all done become READY and are dispatched immediately,
          their command carries the upstream results; the coordinator gets a progress message
        → REJECTED / participant offline → automatic re-dispatch to someone else
        → FAILED (not retryable) → coordinator is alerted
        → all tasks done → PLAN_COMPLETED broadcast; coordinator synthesises the final answer

The coordinator commands; the system never waits on a human to move work between stages.
"""
from __future__ import annotations

from datetime import datetime

from .core import CMD_OPEN, ROLE_PERMS, NotFound, PalinorError, iso, nid

WORKER_ROLES = {"worker", "coordinator", "reviewer"}


DEFAULT_EST = {"research": 30, "coding": 45, "gpu": 60, "review": 15}  # minutes, when a plan gives none


def _est(task: dict) -> float:
    """Estimated minutes for a task: the plan's est_minutes, else a default for its capability."""
    try:
        return max(1.0, float(task.get("est_minutes") or 0)) if task.get("est_minutes") else             float(DEFAULT_EST.get((task.get("capability") or "").lower(), 30))
    except (TypeError, ValueError):
        return 30.0


def _when(c: dict, status: str) -> datetime | None:
    for h in c.get("history") or []:
        if h.get("status") == status and h.get("at"):
            return datetime.fromisoformat(h["at"])
    return None


def _speed(svc, participant_id: str) -> float:
    """Measured actual/estimated duration ratio over finished tasks (EPLB idea: place by observed load).
    1.0 until there is history; clamped so one outlier cannot dominate."""
    ratios = []
    for c in svc.g.find("Command", target_participant_id=participant_id):
        if c["status"] != "COMPLETED" or not c.get("task_id"):
            continue
        start, end = _when(c, "ACCEPTED") or _when(c, "DELIVERED"), _when(c, "COMPLETED")
        task = svc.g.get(c["task_id"]) or {}
        if start and end and end > start:
            ratios.append(((end - start).total_seconds() / 60) / _est(task))
    if not ratios:
        return 1.0
    ratios.sort()
    return min(4.0, max(0.25, ratios[len(ratios) // 2]))


def _load(svc, participant_id: str) -> float:
    """Projected busy minutes: estimated work still open for this participant, scaled by their measured speed."""
    open_minutes = sum(_est(svc.g.get(c["task_id"]) or {}) if c.get("task_id") else 15.0
                       for c in svc.g.find("Command", target_participant_id=participant_id) if c["status"] in CMD_OPEN)
    return open_minutes * _speed(svc, participant_id)


def _has_gpu(c: dict) -> bool:
    gpu = str((c.get("profile") or {}).get("gpu") or "").strip().lower()
    return bool(gpu) and gpu not in {"none", "no", "n/a", "cpu"}


def _candidates(svc, project_id: str, exclude: set[str]) -> list[dict]:
    return [p for p in svc.g.find("Participant", project_id=project_id)
            if p.get("status") == "online" and p.get("role") in WORKER_ROLES and p["id"] not in exclude]


def _pick(svc, task: dict, candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    pref = task.get("preferred_assignee")
    for c in candidates:
        if pref and c["id"] == pref:
            return c
    need = (task.get("capability") or "").lower()

    def rank(c):
        caps = [x.lower() for x in (c.get("capabilities") or [])]
        match = 0 if not need else (0 if need in caps else 1)
        if need == "gpu" and (c.get("profile") or {}).get("gpu") is not None and not _has_gpu(c):
            match = 2  # affinity: a declared no-GPU machine is the last resort for GPU work
        finish = _load(svc, c["id"]) + _est(task) * _speed(svc, c["id"])  # earliest projected finish wins
        return (match, finish, c.get("role") == "coordinator", c["id"])

    best = sorted(candidates, key=rank)[0]
    if rank(best)[0] == 2:
        return None  # GPU work and only declared no-GPU machines are available: wait for a GPU participant
    return best


def _instruction(svc, task: dict) -> str:
    plan = svc.g.get(task["plan_id"]) if task.get("plan_id") else None
    lines = [f"[{task['id']}] {task['title']}"]
    if plan:
        lines.append(f"Plan goal: {plan['goal']}")
    if task.get("description"):
        lines.append(f"What to do: {task['description']}")
    if task.get("acceptance"):
        lines.append(f"Done when: {task['acceptance']}")
    inputs = svc.g.out(task["id"], "DEPENDS_ON", "Task")
    if inputs:
        lines.append("Inputs from upstream tasks:")
        for d in inputs:
            lines.append(f"  - {d['id']} {d['title']} (by {d.get('completed_by', '?')}): "
                         f"{(d.get('result_summary') or 'no summary')[:600]}"
                         + (f" [ref {d['result_ref']}]" if d.get("result_ref") else ""))
    lines.append("Protocol: command_update(ACCEPTED) → context_get(task_id) → work (finding_submit / code on "
                 "github_branch_name) → command_update(COMPLETED, note=<summary for the next stage>, result_ref=...). "
                 "If you cannot do it: command_update(REJECTED, note=why) and it is re-dispatched.")
    return "\n".join(lines)


def submit_plan(svc, coordinator: dict, goal: str, tasks: list[dict], auto_dispatch: bool = True) -> dict:
    """Create a whole division of labour in one call."""
    svc._require(coordinator, "command")
    pid = coordinator["project_id"]
    if not goal.strip():
        raise PalinorError("a plan needs a goal")
    if not tasks:
        raise PalinorError("a plan needs at least one task")
    keys = [str(t.get("key") or t.get("title") or "").strip() for t in tasks]
    if any(not k for k in keys) or len(set(keys)) != len(keys):
        raise PalinorError("every task needs a unique 'key' (or unique title)")
    spec = dict(zip(keys, tasks))
    for k, t in spec.items():
        for d in t.get("depends_on") or []:
            if d not in spec:
                svc._node(d, "Task", pid)  # may also depend on an existing task id
    # topological order (Kahn) — rejects cycles
    indeg = {k: sum(1 for d in (spec[k].get("depends_on") or []) if d in spec) for k in keys}
    order, ready = [], [k for k in keys if indeg[k] == 0]
    while ready:
        k = ready.pop(0)
        order.append(k)
        for j in keys:
            if k in (spec[j].get("depends_on") or []):
                indeg[j] -= 1
                if indeg[j] == 0:
                    ready.append(j)
    if len(order) != len(keys):
        raise PalinorError(f"plan has a dependency cycle among: {sorted(set(keys) - set(order))}")

    plan_id = nid("PLAN")
    svc.g.upsert("Plan", plan_id, {"plan_id": plan_id, "project_id": pid, "goal": goal[:4000], "status": "running",
                                   "owner": coordinator["id"], "size": len(keys), "created_at": iso()})
    svc.g.link(pid, "HAS_PLAN", plan_id)
    ids: dict[str, str] = {}
    for k in order:
        t = spec[k]
        assignee = (t.get("assignee") or "auto").strip()
        preferred = None
        if assignee.lower() != "auto":
            preferred = svc._resolve_recipients(pid, assignee)[0]
        deps = [ids.get(d, d) for d in (t.get("depends_on") or [])]
        task = svc.create_task(coordinator, pid, t.get("title") or k, t.get("description", ""),
                               t.get("type", "general"), t.get("priority", "normal"), deps, t.get("question", ""))
        svc.g.upsert("Task", task["id"], {"plan_id": plan_id, "plan_key": k, "capability": t.get("capability", ""),
                                          "acceptance": t.get("acceptance", ""), "preferred_assignee": preferred,
                                          "est_minutes": t.get("est_minutes")})
        svc.g.link(plan_id, "INCLUDES", task["id"])
        ids[k] = task["id"]
    svc._emit(pid, "PLAN_SUBMITTED", coordinator["id"], payload={"plan_id": plan_id, "tasks": len(keys),
                                                                 "goal": goal[:200]})
    svc.send_message(None, pid, "all",
                     f"{coordinator['name']} published a plan ({len(keys)} tasks) for: {goal}\n"
                     + "\n".join(f"- {ids[k]} {spec[k].get('title') or k}"
                                 + (f" (after {', '.join(ids.get(d, d) for d in spec[k]['depends_on'])})"
                                    if spec[k].get("depends_on") else "") for k in order)
                     + "\nYou will receive a command when a task is yours and its inputs are ready.",
                     kind="info", subject=f"Plan: {goal[:100]}", refs=[plan_id], sender_id=coordinator["id"],
                     exclude=[coordinator["id"]])
    dispatched = dispatch(svc, pid) if auto_dispatch else []
    return {"plan_id": plan_id, "tasks": ids, "dispatched": dispatched}


def dispatch(svc, project_id: str, exclude: set[str] | None = None) -> list[dict]:
    """Assign every READY, unassigned plan task to the best available participant."""
    out = []
    exclude = exclude or set()
    order = {"high": 0, "normal": 1, "low": 2}
    tasks = [t for t in svc.g.find("Task", project_id=project_id, status="READY") if t.get("plan_id")]
    # heaviest first within a priority (longest-processing-time first, as EPLB places its heaviest experts first)
    tasks.sort(key=lambda t: (order.get(t.get("priority"), 1), -_est(t), t["created_at"]))
    for t in tasks:
        if svc.g.out(t["id"], "ASSIGNED_TO"):
            continue
        skip = exclude | set(t.get("rejected_by") or [])
        who = _pick(svc, t, _candidates(svc, project_id, skip))
        if not who:
            continue
        cmd = svc.assign_task(None, project_id, t["id"], who["id"], _instruction(svc, t))
        svc._emit(project_id, "TASK_DISPATCHED", "orchestrator", t["id"], target_participant_id=who["id"],
                  payload={"plan_id": t["plan_id"], "capability": t.get("capability", ""),
                           "load": _load(svc, who["id"])})
        out.append({"task_id": t["id"], "participant_id": who["id"], "command_id": cmd["id"]})
    return out


def _unassign(svc, project_id: str, task: dict, reason: str, rejected_by: str | None = None) -> None:
    svc.g.unlink(task["id"], "ASSIGNED_TO")
    props = {"status": "READY" if all(d["status"] == "COMPLETED" for d in svc.g.out(task["id"], "DEPENDS_ON", "Task"))
             else "BLOCKED"}
    if rejected_by:
        props["rejected_by"] = sorted(set((task.get("rejected_by") or []) + [rejected_by]))
    svc.g.upsert("Task", task["id"], props)
    svc._emit(project_id, "TASK_REASSIGNED", "orchestrator", task["id"], payload={"reason": reason})


def on_task_completed(svc, project_id: str, task_id: str, by: str) -> None:
    task = svc.g.get(task_id)
    if not task or not task.get("plan_id"):
        return
    svc.g.upsert("Task", task_id, {"completed_by": by})
    plan = svc.g.get(task["plan_id"])
    members = svc.g.out(plan["id"], "INCLUDES", "Task")
    done = sum(1 for m in members if m["status"] == "COMPLETED")
    dispatched = dispatch(svc, project_id)
    nxt = ", ".join(f"{d['task_id']}→{d['participant_id']}" for d in dispatched) or "none yet"
    if plan["owner"] != by:
        svc.send_message(None, project_id, plan["owner"],
                         f"✓ {task_id} '{task['title']}' completed by {by} ({done}/{len(members)}).\n"
                         f"Summary: {(task.get('result_summary') or '')[:800]}\nDispatched next: {nxt}",
                         kind="info", subject=f"Progress {done}/{len(members)}: {task['title'][:60]}",
                         refs=[task_id, plan["id"]], sender_id="orchestrator")
    if done == len(members):
        svc.g.upsert("Plan", plan["id"], {"status": "completed", "completed_at": iso()})
        svc._emit(project_id, "PLAN_COMPLETED", "orchestrator", payload={"plan_id": plan["id"], "tasks": done})
        svc.send_message(None, project_id, "all",
                         f"Plan complete: {plan['goal']}\nAll {done} tasks are done. "
                         f"{plan['owner']} will synthesise the final result (pipeline_status shows every output).",
                         kind="decision", subject=f"Plan complete: {plan['goal'][:80]}", refs=[plan["id"]],
                         sender_id="orchestrator")


def on_command_rejected(svc, project_id: str, cmd: dict, by: str, note: str) -> None:
    task = svc.g.get(cmd.get("task_id") or "")
    if not task or not task.get("plan_id") or cmd["type"] != "START_TASK":
        return
    _unassign(svc, project_id, task, f"rejected by {by}: {note}", rejected_by=by)
    redo = dispatch(svc, project_id)
    plan = svc.g.get(task["plan_id"])
    svc.send_message(None, project_id, plan["owner"],
                     f"{by} rejected {task['id']} '{task['title']}': {note or 'no reason'}. "
                     + (f"Re-dispatched to {redo[0]['participant_id']}." if redo else "Nobody else is available."),
                     kind="alert" if not redo else "info", subject=f"Rejected: {task['title'][:70]}",
                     refs=[task["id"]], sender_id="orchestrator")


def on_task_failed(svc, project_id: str, task_id: str, by: str, reason: str) -> None:
    task = svc.g.get(task_id)
    if not task or not task.get("plan_id"):
        return
    plan = svc.g.get(task["plan_id"])
    svc.g.upsert("Plan", plan["id"], {"status": "attention"})
    svc.send_message(None, project_id, plan["owner"],
                     f"✗ {task_id} '{task['title']}' FAILED at {by} after retries: {reason or 'no reason'}.\n"
                     f"Downstream tasks stay blocked. Options: task_assign it to someone else, or change the plan.",
                     kind="alert", subject=f"Failed: {task['title'][:70]}", refs=[task_id, plan["id"]],
                     sender_id="orchestrator")


def on_participant_offline(svc, project_id: str, participant_id: str) -> None:
    moved = []
    for task in svc.g.inn(participant_id, "ASSIGNED_TO", "Task"):
        if not task.get("plan_id") or task["status"] not in {"ASSIGNED", "RUNNING"}:
            continue
        cmds = svc.g.find("Command", task_id=task["id"], target_participant_id=participant_id)
        if any(c["status"] in {"ACCEPTED", "RUNNING"} for c in cmds):
            # work already started (e.g. a long GPU job with a quiet client): keep it with its owner;
            # sweep() flags reassign_suggested so the coordinator decides instead of silently duplicating work
            continue
        for c in cmds:
            if c["status"] in {"QUEUED", "DELIVERED"}:
                svc._transition(c, "CANCELLED", "orchestrator", "assignee went offline")
        _unassign(svc, project_id, task, f"{participant_id} went offline")
        moved.append(task["id"])
    if moved:
        dispatch(svc, project_id, exclude={participant_id})


def pipeline_status(svc, project_id: str, plan_id: str = "") -> dict:
    plans = [svc._node(plan_id, "Plan", project_id)] if plan_id else \
        sorted(svc.g.find("Plan", project_id=project_id), key=lambda p: p["created_at"])
    out = []
    for plan in plans:
        tasks = svc.g.out(plan["id"], "INCLUDES", "Task")
        by_id = {t["id"]: t for t in tasks}
        deps = {t["id"]: [d["id"] for d in svc.g.out(t["id"], "DEPENDS_ON", "Task")] for t in tasks}
        depth: dict[str, int] = {}

        def d_of(tid, seen=()):
            if tid in depth:
                return depth[tid]
            ds = [x for x in deps.get(tid, []) if x in by_id and x not in seen]
            depth[tid] = 0 if not ds else 1 + max(d_of(x, seen + (tid,)) for x in ds)
            return depth[tid]

        rows = []
        for t in tasks:
            assignee = next((a["id"] for a in svc.g.out(t["id"], "ASSIGNED_TO")), None)
            cmds = sorted(svc.g.find("Command", task_id=t["id"]), key=lambda c: c["created_at"])
            waiting = [x for x in deps[t["id"]] if svc.g.get(x) and svc.g.get(x)["status"] != "COMPLETED"]
            rows.append({"task_id": t["id"], "key": t.get("plan_key"), "title": t["title"], "stage": d_of(t["id"]),
                         "status": t["status"], "assignee": assignee, "capability": t.get("capability", ""),
                         "command": cmds[-1]["status"] if cmds else None,
                         "attempt": cmds[-1]["attempt"] if cmds else 0, "waiting_on": waiting,
                         "result_summary": (t.get("result_summary") or "")[:300],
                         "completed_by": t.get("completed_by")})
        rows.sort(key=lambda r: (r["stage"], r["task_id"]))
        workload: dict[str, dict] = {}
        for r in rows:
            who = r["assignee"] or r["completed_by"]
            if who:
                w = workload.setdefault(who, {"active": 0, "done": 0})
                w["done" if r["status"] == "COMPLETED" else "active"] += 1
        done = sum(1 for r in rows if r["status"] == "COMPLETED")
        out.append({"plan_id": plan["id"], "goal": plan["goal"], "status": plan["status"], "owner": plan["owner"],
                    "progress": f"{done}/{len(rows)}", "percent": round(100 * done / max(len(rows), 1)),
                    "stages": max((r["stage"] for r in rows), default=0) + 1, "tasks": rows, "workload": workload})
    return {"plans": out}


def set_capabilities(svc, p: dict, capabilities: list[str]) -> dict:
    caps = sorted({c.strip().lower() for c in capabilities if c and c.strip()})
    p = svc.g.upsert("Participant", p["id"], {"capabilities": caps})
    svc._emit(p["project_id"], "PARTICIPANT_UPDATED", p["id"], payload={"capabilities": caps})
    dispatch(svc, p["project_id"])  # new skills may unblock waiting work
    return svc._public(p)


__all__ = ["submit_plan", "dispatch", "pipeline_status", "set_capabilities", "on_task_completed",
           "on_command_rejected", "on_task_failed", "on_participant_offline", "NotFound", "ROLE_PERMS"]
