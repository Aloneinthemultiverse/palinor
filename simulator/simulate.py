"""PALINOR load simulator — produces realistic multi-agent collaboration event streams at scale.

Real sessions produce hundreds of events; to exercise Kafka/Spark/HBase at Big Data scale we simulate many
projects that emit EXACTLY the envelope and payloads of the live backend (palinor/core.py, orchestrator.py,
conflicts.py). Simulated events carry `"sim": true` in the payload.

Per project:
  * a team named the way the backend names it: Claude-1 (coordinator), Claude-2, Claude-3, ChatGPT-1, OpenCode-1,
    LocalLLM-1 ... with participant ids "<platform>-<name>-<4 hex>" (claude-claude-2-9f3a), roles and capabilities,
    plus the person at the dashboard (human-<project_id>, no PARTICIPANT_CONNECTED). With several Claudes a second
    one sometimes co-coordinates and owns its own plans.
  * orchestration (orchestrator.py): a coordinator submits plans (task DAGs) → every READY task is dispatched to the
    best online participant (capability → load → workers before the coordinator) → the worker sees the command at
    its next sync and accepts or REJECTS it (→ TASK_REASSIGNED + re-dispatch to someone else) → hand-off: a completed
    task unblocks its dependents, which are dispatched at once and the owner gets a progress message → a session
    that crashes is swept offline after the heartbeat timeout and its tasks are re-dispatched → a task that fails
    after its retries alerts the owner, who usually re-assigns it by hand → PLAN_COMPLETED + broadcast.
  * conflicts (conflicts.py): a worker that disagrees with an upstream finding, or with a committed decision, raises
    ONE alert to the coordinator responsible → the coordinator consults 1-3 participants, preferably on other
    platforms (question DMs in the conflict thread) → grounded opinions (they cite node ids, or answer 'unsure') →
    resolve keep | accept_challenge | merge with Laya's evidence audit; some resolutions go against the evidence,
    some are forced before every opinion is in, a few conflicts stay open, a few raisers withdraw. Accepting a
    challenge against a decision re-opens the question as a new debate.
  * all-way communication between the sessions and the human: direct, small-group and broadcast messages, question
    → answer threads (reply_to) with follow-ups, read receipts, context shares and chat ingestion. A session only
    sees messages when it syncs, so read and response latency follow each platform's rhythm (BEHAVIOUR); a crashed
    session sees nothing until it reconnects.
  * deliberative and empirical debates adjudicated by the REAL Laya engine (palinor.laya.evaluate).

Deliberately injects ~1% duplicate deliveries and ~0.5% malformed events so the ETL dedupe / quarantine stages
have something to do.

By default (--anchor end) the timeline is shifted so the LAST simulated event happens now: the simulated history
lies in the past and never pushes the streaming event-time watermark ahead of live backend events.

  python -m simulator.simulate --projects 300 --out kafka           # stream to Kafka
  python -m simulator.simulate --projects 300 --out data/sim.jsonl  # or to a file
"""
from __future__ import annotations

import argparse
import bisect
import heapq
import json
import math
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from palinor import laya
from palinor.core import PLATFORM_NAMES, slug
from palinor.events import topic_for

TASK_TYPES = {  # base minutes, base failure logit
    "research": (25, -2.2), "coding": (60, -1.6), "verification": (35, -2.0),
    "experiment": (120, -1.2), "review": (15, -2.6), "data_analysis": (45, -1.8)}
ENVIRONMENTS = {"local": (1.0, 0.0), "docker": (0.9, -0.3), "cloud": (0.7, 0.2), "gpu": (0.35, 0.4)}
TOPICS = ["vision", "nlp", "satellite", "fintech", "climate", "semiconductor", "health", "security"]

# Team make-up: how many sessions of each platform join a project (values, weights).
TEAM = {"claude": ([1, 2, 3], [0.35, 0.45, 0.20]), "chatgpt": ([0, 1, 2], [0.15, 0.65, 0.20]),
        "opencode": ([0, 1, 2], [0.20, 0.60, 0.20]), "local-llm": ([0, 1], [0.85, 0.15])}
CAPABILITIES = {"claude": ["research", "coding", "review", "verification", "data_analysis"],
                "chatgpt": ["research", "data_analysis", "review"],
                "opencode": ["coding", "verification", "experiment"],
                "local-llm": ["experiment", "data_analysis", "coding"]}
WORKER_ROLES = {"worker", "coordinator", "reviewer"}  # orchestrator.WORKER_ROLES
HEARTBEAT = timedelta(seconds=300)                   # Config.heartbeat_timeout_s: crash → swept offline
MAX_RETRIES = 2                                      # Config.max_retries
# P(a worker challenges an upstream input when it starts a task with dependencies)
CONFLICT_PROPENSITY = {"claude": 0.06, "chatgpt": 0.14, "opencode": 0.12, "local-llm": 0.09}
REJECT_NOTES = ["outside my capabilities", "busy with another task", "no access to the target environment",
                "needs a GPU this session does not have"]
FAIL_REASONS = ["timeout", "oom", "test failure", "dependency error"]
PLAN_GOALS = ["Build the {d} baseline", "Ship the {d} ingestion service", "Evaluate candidate {d} models",
              "Harden the {d} pipeline", "Prototype the {d} dashboard", "Reproduce the {d} paper results"]
TASK_TITLES = {"research": "Survey {d} approaches", "coding": "Implement the {d} module",
               "verification": "Verify the {d} results", "experiment": "Run the {d} benchmark",
               "review": "Review the {d} changes", "data_analysis": "Analyse the {d} dataset"}

# How each platform converses. A session sees messages only when it syncs (one call per turn); a ChatGPT
# session waits for its user to prompt it; the human looks at the dashboard every now and then.
#   (mean minutes between syncs, median minutes to write a reply, P(answers a question put to it))
BEHAVIOUR = {"claude": (4.0, 1.5, 0.95), "chatgpt": (9.0, 2.0, 0.88), "opencode": (5.0, 2.5, 0.90),
             "local-llm": (2.5, 3.5, 0.75), "human": (25.0, 4.0, 0.80)}
DM_KINDS = (["question", "info", "request", "review"], [0.40, 0.27, 0.17, 0.16])
MEDIAN_CHARS = {"question": 350, "answer": 900, "info": 300, "request": 450, "review": 700, "context": 1600,
                "decision": 700, "alert": 400}
DEBATE_QUESTIONS = {
    "deliberative": ["Which architecture should the {d} pipeline use?", "How should we split the {d} work?",
                     "Which evaluation protocol fits {d}?", "Monolith or services for the {d} backend?",
                     "Which data retention policy for {d}?"],
    "empirical": ["Which {d} model is more accurate?", "Does caching cut {d} inference latency?",
                  "Parquet or Delta for the {d} feature store?", "Which tokenizer gives better {d} recall?"]}
ACTIONABLE = {"DECIDED", "SYNTHESIZE", "ESCALATE", "NEED_EVIDENCE", "VERIFY"}
PRIORITY = {"high": 0, "normal": 1, "low": 2}


def sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def ms(a: datetime, b: datetime) -> int:
    return int((b - a).total_seconds() * 1000)


def other_side(position: str) -> str:
    return {"keep": "accept_challenge", "accept_challenge": "keep"}.get(position, position)


class Sim:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)
        self.heap: list = []
        self.seq: dict[str, int] = {}
        self.last: dict[str, datetime] = {}
        self.counter = 0
        self.shift = timedelta(0)
        self.outcomes: Counter = Counter()   # Laya outcomes, for the run summary
        self.stats: Counter = Counter()      # orchestration / conflict outcomes, for the run summary

    # ------------------------------------------------------------------ envelope
    def emit(self, t: datetime, etype: str, pid: str, actor=None, task=None, payload=None, **top):
        self.seq[pid] = self.seq.get(pid, 0) + 1
        self.last[pid] = max(self.last.get(pid, t), t)
        ev = {"event_id": f"EVT-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12]}", "event_type": etype,
              "project_id": pid, "task_id": task, "participant_id": actor, "occurred_at": t.isoformat(),
              "schema_version": 1, "seq": self.seq[pid], "payload": {**(payload or {}), "sim": True},
              "status": None, "duration_ms": None, "retry_count": None, "result_ref": None,
              "commit_sha": None, "target_participant_id": None, "command_id": None, **top}
        self.counter += 1
        heapq.heappush(self.heap, (t, self.counter, ev))

    def nid(self, prefix: str) -> str:
        return f"{prefix}-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:10]}"

    def tick(self, t: datetime) -> datetime:
        """Server-side processing time between two events of one backend call."""
        return t + timedelta(milliseconds=self.rng.uniform(5, 250))

    # ------------------------------------------------------------------ project
    def project(self, start: datetime) -> None:
        r = self.rng
        pid = f"P-sim{uuid.UUID(int=r.getrandbits(128)).hex[:6]}"
        domain = r.choice(TOPICS)
        self.emit(start, "PROJECT_CREATED", pid, payload={"name": f"{domain} challenge", "domain": domain,
                                                         "goal": f"Solve the {domain} challenge"})
        self.pid, self.domain = pid, domain
        t = self.join_team(start)
        self.schedule_crashes(start)
        for q in range(r.randint(1, 3)):  # debates → Laya
            self.debate(q, domain, t + timedelta(minutes=r.uniform(30, 600)))
        self.run_plans(t)                  # plans → dispatch → hand-offs (+ conflicts about upstream findings)
        self.decision_conflicts()          # workers challenging committed decisions
        self.communicate(t, self.last[pid], [x["id"] for x in self.tasks.values()], domain)

    def join_team(self, start: datetime) -> datetime:
        """PARTICIPANT_CONNECTED exactly as core.join emits it: Claude-1 first (it coordinates), then the others.
        Names are numbered per platform in join order; ids are '<platform>-<name>-<4 hex>'."""
        r = self.rng
        counts = {p: r.choices(*TEAM[p])[0] for p in TEAM}
        if counts["chatgpt"] + counts["opencode"] + counts["local-llm"] + counts["claude"] - 1 < 2:
            counts["chatgpt"] = counts["opencode"] = 1  # at least two sessions besides the coordinator
        later = [p for p, n in counts.items() for _ in range(n - (p == "claude"))]
        r.shuffle(later)
        self.plat, self.role, self.caps, self.name, self.skill = {}, {}, {}, {}, {}
        self.down: dict[str, list] = {}   # who → [(crash, swept offline, reconnect)]
        self.busy: dict[str, list] = {}   # who → [(command created, command closed)]
        self.findings, self.decisions, self.evidence, self.authors = [], [], {}, {}
        numbering: Counter = Counter()
        t = start + timedelta(seconds=5)
        joined = {}
        for i, plat in enumerate(["claude"] + later):
            numbering[plat] += 1
            name = f"{PLATFORM_NAMES[plat]}-{numbering[plat]}"
            part = f"{slug(plat)}-{slug(name)}-{uuid.UUID(int=r.getrandbits(128)).hex[:4]}"
            if i == 0:
                role = "coordinator"
            elif name == "Claude-2" and r.random() < 0.3:
                role = "coordinator"  # a second Claude co-coordinates (owns its own plans)
            elif plat == "claude" and r.random() < 0.2:
                role = "reviewer"
            else:
                role = "worker"
            pool = CAPABILITIES[plat]
            caps = [] if role == "coordinator" and r.random() < 0.6 else sorted(r.sample(pool, r.randint(1, len(pool))))
            self.plat[part], self.role[part], self.caps[part], self.name[part] = plat, role, caps, name
            self.skill[part] = r.uniform(0.7, 1.3)
            self.emit(t, "PARTICIPANT_CONNECTED", self.pid, part,
                      payload={"platform": plat, "role": role, "capabilities": caps})
            joined[part] = t
            t += timedelta(seconds=r.uniform(5, 240))
        self.coordinators = [p for p in joined if self.role[p] == "coordinator"]
        # the person at the dashboard (backend: ensure_human → "human-<project_id>", no PARTICIPANT_CONNECTED)
        self.human = f"human-{self.pid}" if r.random() < 0.75 else None
        if self.human:
            self.plat[self.human], self.role[self.human], self.caps[self.human] = "human", "human", ["review", "decide"]
            self.name[self.human] = "You (dashboard)"
            joined[self.human] = start
        self.members = list(joined)                     # everyone who receives broadcasts
        self.voters = [p for p in joined if p != self.human]  # Laya's quorum (the human never counts)
        self.syncs = {m: [joined[m]] for m in self.members}
        return t

    def schedule_crashes(self, start: datetime) -> None:
        """Some sessions die (closed tab, crashed CLI) and come back later. The sweep marks them offline after the
        heartbeat timeout; reconnecting with the same token re-emits PARTICIPANT_CONNECTED (reconnected)."""
        r = self.rng
        for m in self.voters:
            if r.random() < (0.05 if self.role[m] == "coordinator" else 0.12):
                crash = start + timedelta(hours=r.uniform(0.3, 9))
                back = crash + timedelta(minutes=max(7.0, r.lognormvariate(math.log(25), 0.9)))
                self.down[m] = [(crash, crash + HEARTBEAT, back)]
                self.emit(crash + HEARTBEAT, "PARTICIPANT_DISCONNECTED", self.pid, m)
                self.emit(back, "PARTICIPANT_CONNECTED", self.pid, m, payload={"reconnected": True})

    # ------------------------------------------------------------------ presence
    def next_sync(self, who: str, t: datetime) -> datetime:
        """First time after t at which `who` syncs (palinor_sync / message_inbox) and so sees new messages and
        commands. A crashed session syncs again only once it has reconnected."""
        s = self.syncs[who]
        gap = BEHAVIOUR[self.plat[who]][0]
        while s[-1] <= t:
            s.append(s[-1] + timedelta(minutes=self.rng.expovariate(1 / gap)))
        x = s[bisect.bisect_right(s, t)]
        for crash, _, back in self.down.get(who, ()):
            if crash <= x < back:
                return back + timedelta(seconds=self.rng.uniform(1, 30))
        return x

    def online(self, who: str, t: datetime) -> bool:
        """Participant.status == 'online' as the backend sees it (a dead session stays 'online' until swept)."""
        return not any(off <= t < back for _, off, back in self.down.get(who, ()))

    def outage(self, who: str, a: datetime, b: datetime):
        """The crash window overlapping [a, b), if any."""
        return next(((c, off, back) for c, off, back in self.down.get(who, ()) if c < b and back > a), None)

    def load(self, who: str, t: datetime) -> int:
        """orchestrator._load: open commands targeting `who` at time t."""
        return sum(1 for a, b in self.busy.get(who, ()) if a <= t < b)

    def compose(self, who: str, scale: float = 1.0) -> timedelta:
        return timedelta(minutes=self.rng.lognormvariate(math.log(BEHAVIOUR[self.plat[who]][1] * scale), 0.6))

    # ------------------------------------------------------------------ plans → orchestrator
    def push(self, t: datetime, kind: str, *args) -> None:
        self.counter += 1
        heapq.heappush(self.queue, (t, self.counter, kind, args))

    def run_plans(self, t_team: datetime) -> None:
        """Each coordinator call and each orchestrator reaction is processed in time order, so the load-based
        choice of the orchestrator always sees the commands that are open at that moment."""
        r = self.rng
        self.tasks: dict[str, dict] = {}
        self.queue: list = []
        t = t_team + timedelta(minutes=r.uniform(2, 15))
        for i in range(r.choices([1, 2, 3], [0.45, 0.38, 0.17])[0]):
            self.push(t, "submit", self.make_plan(i))
            t += timedelta(minutes=r.uniform(40, 300))
        while self.queue:
            t, _, kind, args = heapq.heappop(self.queue)
            getattr(self, f"on_{kind}")(t, *args)

    def make_plan(self, i: int) -> dict:
        r = self.rng
        owner = self.coordinators[i % len(self.coordinators)]
        plan = {"id": f"PLAN-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}", "owner": owner, "tasks": [],
                "goal": r.choice(PLAN_GOALS).format(d=self.domain) + f" (stage {i + 1})", "status": "running"}
        for k in range(r.randint(3, 9)):
            ttype = r.choice(list(TASK_TYPES))
            deps = r.sample(range(k), min(k, r.choices([0, 1, 2], [0.25, 0.5, 0.25])[0])) if k else []
            plan["tasks"].append({
                "id": f"T-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}", "key": f"step{k + 1}", "plan": plan,
                "title": TASK_TITLES[ttype].format(d=self.domain), "ttype": ttype,
                "env": r.choice(list(ENVIRONMENTS)), "complexity": r.randint(1, 10),
                "context_size": int(r.lognormvariate(8, 0.8)), "deps": deps,
                "priority": r.choices(["high", "normal", "low"], [0.15, 0.75, 0.10])[0],
                "status": "BLOCKED" if deps else "READY", "assignee": None, "rejected_by": set(), "failed_by": set(),
                "manual_pending": False, "seq": k})
        for task in plan["tasks"]:
            task["deps"] = [plan["tasks"][d] for d in task["deps"]]
        for task in plan["tasks"]:
            task["dependents"] = [x for x in plan["tasks"] if any(d is task for d in x["deps"])]
        return plan

    def on_submit(self, t: datetime, plan: dict) -> None:
        """orchestrator.submit_plan: tasks (topological order) → PLAN_SUBMITTED → plan broadcast → dispatch."""
        owner = plan["owner"]
        for task in plan["tasks"]:
            task["created"] = t
            self.tasks[task["id"]] = task
            self.emit(t, "TASK_CREATED", self.pid, owner, task["id"],
                      {"title": task["title"], "task_type": task["ttype"],
                       "depends_on": [d["id"] for d in task["deps"]]}, status=task["status"])
            t = self.tick(t)
        self.emit(t, "PLAN_SUBMITTED", self.pid, owner, payload={
            "plan_id": plan["id"], "tasks": len(plan["tasks"]), "goal": plan["goal"][:200]})
        self.message(t, owner, "all", "info", f"Plan: {plan['goal']}", exclude=[owner],
                     chars=180 + 70 * len(plan["tasks"]))
        self.stats["plans_submitted"] += 1
        self.dispatch(self.tick(t))

    def dispatch(self, t: datetime, exclude: frozenset = frozenset()) -> tuple[list, datetime]:
        """orchestrator.dispatch: every READY, unassigned plan task → best online participant."""
        out = []
        ready = sorted((x for x in self.tasks.values() if x["status"] == "READY" and not x["assignee"]),
                       key=lambda x: (PRIORITY[x["priority"]], x["created"], x["seq"]))
        for task in ready:
            skip = exclude | task["rejected_by"]
            cands = [m for m in self.voters if self.role[m] in WORKER_ROLES and m not in skip and self.online(m, t)]
            if not cands:
                self.need_manual(task, t)  # the owner sees an unassigned READY task in pipeline_status
                continue
            who = min(cands, key=lambda c: (0 if task["ttype"] in self.caps[c] else 1, self.load(c, t),
                                            self.role[c] == "coordinator", c))
            t = self.tick(t)
            self.start(task, who, t, issuer=None)
            out.append((task, who))
        return out, t

    def need_manual(self, task: dict, t: datetime) -> None:
        if not task["manual_pending"]:
            task["manual_pending"] = True
            owner = task["plan"]["owner"]
            self.push(self.next_sync(owner, t) + self.compose(owner, 1.5), "manual", task, 0)

    def cmd(self, t: datetime, status: str, by, task: dict, cmd: str, w: str, note: str = "",
            duration: int | None = None, attempt: int = 1, result_ref: str | None = None) -> None:
        """core._transition: COMMAND_<status> with the command's envelope fields."""
        self.emit(t, f"COMMAND_{status}", self.pid, by, task["id"], {"type": "START_TASK", "note": note},
                  command_id=cmd, target_participant_id=w, status=status, duration_ms=duration,
                  retry_count=attempt - 1, result_ref=result_ref)

    def close(self, w: str, opened: datetime, closed: datetime) -> None:
        self.busy.setdefault(w, []).append((opened, closed))

    def start(self, task: dict, w: str, t0: datetime, issuer: str | None) -> None:
        """core.assign_task (+ TASK_DISPATCHED when the orchestrator chose), then the worker's side of the
        command: delivered at its next sync → reject, or accept → run (retries) → complete / fail; a crash
        interrupts the command and the sweep hands the task on."""
        r, pid, tid = self.rng, self.pid, task["id"]
        task["assignee"], task["status"], task["manual_pending"] = w, "ASSIGNED", False
        self.emit(t0, "TASK_ASSIGNED", pid, issuer, tid, target_participant_id=w)
        cmd = f"CMD-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
        self.emit(t0, "COMMAND_CREATED", pid, issuer or "system", tid, {"type": "START_TASK"}, command_id=cmd,
                  target_participant_id=w, status="CREATED")
        self.cmd(t0, "AUTHORIZED", None, task, cmd, w)
        self.cmd(t0, "QUEUED", None, task, cmd, w)
        if issuer is None:
            self.emit(t0, "TASK_DISPATCHED", pid, "orchestrator", tid, target_participant_id=w, payload={
                "plan_id": task["plan"]["id"], "capability": task["ttype"], "load": self.load(w, t0) + 1})
            self.stats["dispatches"] += 1
        deliver = self.next_sync(w, t0)
        out = self.outage(w, t0, deliver)
        if out:  # dead before it ever saw the command
            stop = self.tick(max(out[1], t0))
            self.close(w, t0, stop)
            self.push(stop, "offline", task, w, cmd, "QUEUED", out[2])
            return
        self.cmd(deliver, "DELIVERED", w, task, cmd, w, duration=ms(t0, deliver))
        decide = deliver + timedelta(seconds=r.uniform(5, 90))
        p_reject = (0.03 if task["ttype"] in self.caps[w] else 0.15) + (0.06 if self.load(w, deliver) >= 3 else 0)
        if r.random() < p_reject:
            self.close(w, t0, decide)
            self.push(decide, "reject", task, w, cmd, r.choice(REJECT_NOTES))
            return
        self.cmd(decide, "ACCEPTED", w, task, cmd, w, duration=ms(t0, decide))
        self.cmd(decide, "RUNNING", w, task, cmd, w)
        self.emit(decide, "TASK_STARTED", pid, w, tid, {"task_type": task["ttype"]}, status="RUNNING")
        self.maybe_conflict(task, w, decide)
        plat, ttype = self.plat[w], task["ttype"]
        feats = {"environment": task["env"], "complexity": task["complexity"], "context_size": task["context_size"],
                 "platform": plat, "num_dependencies": len(task["deps"])}
        base_min, base_logit = TASK_TYPES[ttype]
        speed, env_risk = ENVIRONMENTS[task["env"]]
        skill = self.skill[w] * (1.0 if ttype in self.caps[w] else 0.85)
        p_fail = sigmoid(base_logit + 0.28 * (task["complexity"] - 5) + env_risk
                         + 0.00008 * (task["context_size"] - 3000) - 0.6 * (skill - 1)
                         + (0.3 if plat == "local-llm" else 0))
        cur = accepted = decide
        retries = 0
        while True:
            minutes = base_min * speed * (0.5 + task["complexity"] / 6) / skill * r.lognormvariate(0, 0.35)
            end = cur + timedelta(minutes=minutes)
            out = self.outage(w, cur, end)
            if out:  # crashed mid-run: the sweep cancels the command and the task moves on
                stop = self.tick(out[1])
                self.close(w, t0, stop)
                self.push(stop, "offline", task, w, cmd, "RUNNING", out[2])
                return
            if r.random() >= p_fail:
                break
            reason = r.choice(FAIL_REASONS)
            self.emit(end, "EXECUTION_FAILED", pid, w, tid, {"task_type": ttype, **feats, "reason": reason},
                      status="FAILED", duration_ms=int(minutes * 60000), retry_count=retries)
            if retries >= MAX_RETRIES:
                self.close(w, t0, end)
                self.push(end, "failed", task, w, cmd, retries, accepted, feats, reason)
                return
            self.cmd(end, "FAILED", w, task, cmd, w, reason, duration=ms(accepted, end), attempt=retries + 1)
            self.cmd(end, "RETRY_QUEUED", w, task, cmd, w, reason, attempt=retries + 1)
            retries += 1
            self.cmd(end, "QUEUED", None, task, cmd, w, "retry", attempt=retries + 1)
            again = self.next_sync(w, end)
            out = self.outage(w, end, again)
            if out:
                stop = self.tick(max(out[1], end))
                self.close(w, t0, stop)
                self.push(stop, "offline", task, w, cmd, "QUEUED", out[2])
                return
            self.cmd(again, "DELIVERED", w, task, cmd, w, duration=ms(t0, again), attempt=retries + 1)
            cur = accepted = again + timedelta(seconds=r.uniform(5, 60))
            self.cmd(cur, "ACCEPTED", w, task, cmd, w, duration=ms(t0, cur), attempt=retries + 1)
            self.cmd(cur, "RUNNING", w, task, cmd, w, attempt=retries + 1)
        for _ in range(r.randint(0, 3)):  # findings submitted while working
            self.finding(decide + (end - decide) * r.uniform(0.2, 1.0), w, tid, f"{self.domain} {ttype} insight")
        if ttype == "coding":
            sha = uuid.UUID(int=r.getrandbits(128)).hex[:7]
            self.emit(end, "GITHUB_COMMIT", pid, w, tid, {"branch": f"{slug(plat)}-{slug(self.name[w])}/task-{tid}"},
                      commit_sha=sha)
            self.emit(end + timedelta(minutes=2), "PULL_REQUEST_CREATED", pid, w, tid, {"ci_status": "pending"},
                      commit_sha=sha, status="OPEN")
            if r.random() < 0.85:
                mergers = [m for m in self.voters if self.role[m] in ("reviewer", "coordinator") and m != w]
                self.emit(end + timedelta(minutes=r.uniform(10, 240)), "PULL_REQUEST_MERGED", pid,
                          r.choice(mergers) if mergers else (self.human or w), tid, {"ci_status": "passed"},
                          commit_sha=sha, status="MERGED")
        if ttype == "experiment":
            self.emit(end, "EXPERIMENT_COMPLETED", pid, w, tid,
                      {"task_type": ttype, **feats, "metric": round(r.uniform(0.6, 0.97), 3),
                       "reproducible": r.random() < 0.8},
                      status="SUCCEEDED", duration_ms=ms(decide, end), commit_sha=uuid.UUID(int=r.getrandbits(128)).hex[:7])
        self.close(w, t0, end)
        self.push(end, "complete", task, w, cmd, retries, accepted, feats, decide)

    def on_complete(self, t, task, w, cmd, retries, accepted, feats, started) -> None:
        """core.command_update(COMPLETED) → _unblock_dependents → orchestrator.on_task_completed (hand-off)."""
        rref = f"R-{task['id'][-8:]}"
        self.cmd(t, "COMPLETED", w, task, cmd, w, "done", duration=ms(accepted, t), attempt=retries + 1,
                 result_ref=rref)
        self.emit(t, "TASK_COMPLETED", self.pid, w, task["id"],
                  {"task_type": task["ttype"], "summary": f"{task['title']}: done", **feats},
                  status="COMPLETED", duration_ms=ms(started, t), retry_count=retries, result_ref=rref)
        task["status"], task["assignee"], task["done_by"] = "COMPLETED", None, w
        t = self.tick(t)
        for d in task["dependents"]:
            if d["status"] == "BLOCKED" and all(x["status"] == "COMPLETED" for x in d["deps"]):
                d["status"] = "READY"
                self.emit(t, "TASK_READY", self.pid, None, d["id"], {"unblocked_by": task["id"]})
        _, t = self.dispatch(t)
        plan = plan_of(task)
        done = sum(1 for x in plan["tasks"] if x["status"] == "COMPLETED")
        if plan["owner"] != w:
            self.message(t, "orchestrator", [plan["owner"]], "info",
                         f"Progress {done}/{len(plan['tasks'])}: {task['title']}", chars=260)
        if done == len(plan["tasks"]):
            plan["status"] = "completed"
            self.stats["plans_completed"] += 1
            self.emit(t, "PLAN_COMPLETED", self.pid, "orchestrator", payload={"plan_id": plan["id"], "tasks": done})
            self.message(t, "orchestrator", "all", "decision", f"Plan complete: {plan['goal']}", chars=220)
            owner = plan["owner"]  # the coordinator synthesises the final answer
            self.finding(self.next_sync(owner, t) + self.compose(owner, 3), owner, None,
                         f"{self.domain} plan result: {plan['goal']}")

    def on_reject(self, t, task, w, cmd, note) -> None:
        """command_update(REJECTED) → orchestrator.on_command_rejected: unassign, re-dispatch, tell the owner."""
        self.cmd(t, "REJECTED", w, task, cmd, w, note)
        task["assignee"], task["status"] = None, "READY"
        task["rejected_by"] = task["rejected_by"] | {w}
        self.emit(t, "TASK_REASSIGNED", self.pid, "orchestrator", task["id"], {"reason": f"rejected by {w}: {note}"})
        self.stats["rejections"] += 1
        redo, t2 = self.dispatch(self.tick(t))
        owner = plan_of(task)["owner"]
        self.message(t2, "orchestrator", [owner], "info" if task["assignee"] else "alert",
                     f"Rejected: {task['title']}", chars=200)

    def on_offline(self, t, task, w, cmd, status, back) -> None:
        """sweep → orchestrator.on_participant_offline: cancel the command, unassign, re-dispatch elsewhere."""
        if status in ("QUEUED", "DELIVERED"):
            self.cmd(t, "CANCELLED", "orchestrator", task, cmd, w, "assignee went offline")
        else:
            self.cmd(t, "CANCEL_REQUESTED", "orchestrator", task, cmd, w, "assignee went offline")
            # the restarted session finds the cancel request in its inbox and confirms it
            self.cmd(back + timedelta(seconds=self.rng.uniform(5, 120)), "CANCELLED", w, task, cmd, w,
                     "session restarted")
        task["assignee"], task["status"] = None, "READY"
        self.emit(t, "TASK_REASSIGNED", self.pid, "orchestrator", task["id"], {"reason": f"{w} went offline"})
        self.stats["offline_handoffs"] += 1
        self.dispatch(self.tick(t), exclude=frozenset({w}))

    def on_failed(self, t, task, w, cmd, retries, accepted, feats, reason) -> None:
        """Not retryable any more → TASK_FAILED → orchestrator.on_task_failed alerts the owner."""
        self.cmd(t, "FAILED", w, task, cmd, w, reason, duration=ms(accepted, t), attempt=retries + 1)
        self.emit(t, "TASK_FAILED", self.pid, w, task["id"], {"task_type": task["ttype"], "reason": reason, **feats},
                  status="FAILED", retry_count=retries)
        task["status"], task["assignee"] = "FAILED", None
        task["failed_by"] = task["failed_by"] | {w}
        plan = plan_of(task)
        plan["status"] = "attention"
        self.stats["tasks_failed"] += 1
        self.message(t, "orchestrator", [plan["owner"]], "alert", f"Failed: {task['title']}", chars=330)
        if self.rng.random() < 0.65:  # the owner re-assigns it by hand (task_assign)
            task["manual_pending"] = True
            self.push(self.next_sync(plan["owner"], t) + self.compose(plan["owner"], 2), "manual", task, 0)

    def on_manual(self, t, task, tries) -> None:
        """The plan owner assigns a stuck task by hand (core.assign_task: no TASK_DISPATCHED)."""
        task["manual_pending"] = False
        if task["assignee"] or task["status"] not in ("READY", "FAILED"):
            return  # somebody picked it up in the meantime
        owner = plan_of(task)["owner"]
        cands = [m for m in self.voters if self.role[m] in WORKER_ROLES and self.online(m, t)]
        if not cands:
            if tries < 3:
                task["manual_pending"] = True
                self.push(t + timedelta(minutes=30), "manual", task, tries + 1)
            return
        who = min(cands, key=lambda c: (c in task["rejected_by"] or c in task["failed_by"],
                                        0 if task["ttype"] in self.caps[c] else 1, self.load(c, t), c == owner, c))
        self.stats["manual_assignments"] += 1
        self.start(task, who, t, issuer=owner)

    # ------------------------------------------------------------------ conflicts
    def finding(self, t: datetime, who: str, task_id, claim: str) -> str:
        """core.submit_finding: EVIDENCE_ADDED per evidence item, FINDING_ADDED, CONTEXT_UPDATED."""
        r = self.rng
        fid = f"F-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
        rels = [round(r.uniform(0.35, 0.9), 2) for _ in range(r.choices([0, 1, 2], [0.4, 0.4, 0.2])[0])]
        for _ in rels:
            self.emit(t, "EVIDENCE_ADDED", self.pid, who, task_id, {"target": fid})
        self.emit(t, "FINDING_ADDED", self.pid, who, task_id, {"finding_id": fid, "claim": claim[:200]})
        self.emit(t, "CONTEXT_UPDATED", self.pid, who, task_id,
                  {"node": fid, "hint": f"Query context for {task_id or 'project'}"})
        self.findings.append((t, fid, who, task_id))
        self.evidence[fid], self.authors[fid] = rels, who
        return fid

    def maybe_conflict(self, task: dict, w: str, t: datetime) -> None:
        """A worker reads its upstream inputs (context_get) and may challenge one of them."""
        r = self.rng
        plan = plan_of(task)
        if w == plan["owner"] or not task["deps"] or r.random() >= CONFLICT_PROPENSITY.get(self.plat[w], 0):
            return
        dep_ids = {d["id"] for d in task["deps"]}
        upstream = [f for f in self.findings if f[3] in dep_ids and f[2] != w and f[0] <= t]
        if not upstream:
            return
        about = [f[1] for f in r.sample(upstream, min(len(upstream), r.choices([1, 2], [0.8, 0.2])[0]))]
        t_raise = t + timedelta(minutes=r.uniform(2, 25))
        if not self.outage(w, t, t_raise):
            # conflicts._resolver_for: the owner of the plan the task belongs to
            self.conflict(t_raise, w, about, task["id"], plan["owner"])

    def decision_conflicts(self) -> None:
        """Workers that disagree with a committed Laya decision challenge it (no task → the coordinator)."""
        r = self.rng
        for t, did, question, mode in list(self.decisions):
            if r.random() >= 0.12:
                continue
            workers = [m for m in self.voters if self.role[m] != "coordinator"]
            if not workers:
                return
            raiser = r.choices(workers, [2.0 if self.plat[m] in ("chatgpt", "opencode") else 1.0 for m in workers])[0]
            t_raise = self.next_sync(raiser, t) + timedelta(minutes=r.lognormvariate(math.log(45), 0.8))
            resolver = next((c for c in self.coordinators if c != raiser and self.online(c, t_raise)),
                            next((c for c in self.coordinators if c != raiser), self.human))
            if resolver and not self.outage(raiser, t, t_raise):
                self.conflict(t_raise, raiser, [did], None, resolver)

    def conflict(self, t: datetime, raiser: str, about: list, task_id, resolver: str) -> None:
        """conflicts.py end to end. One thread per conflict; DMs only (no broadcast); consultants are named."""
        r, pid = self.rng, self.pid
        self.stats["conflicts_raised"] += 1
        cid = f"CF-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
        n_ev = r.choices([0, 1, 2, 3], [0.2, 0.4, 0.3, 0.1])[0]
        severity = r.choices(["low", "normal", "high"], [0.25, 0.55, 0.20])[0]
        existing = [x for a in about for x in self.evidence.get(a, [])]  # a Decision has no SUPPORTED_BY evidence
        challenge = [round(r.uniform(0.3, 0.9), 2) for _ in range(n_ev)]
        # latent truth (is the challenge right?), correlated with how much evidence each side brings
        valid = r.random() < sigmoid(-0.3 + 3.0 * (laya._evidence_score(challenge) - laya._evidence_score(existing)))
        thread, _, reads = self.message(t, raiser, [resolver], "alert", f"Conflict {cid} ({severity})",
                                        has_context=True)
        self.emit(t, "CONFLICT_RAISED", pid, raiser, task_id, target_participant_id=resolver, payload={
            "conflict_id": cid, "about": about, "severity": severity, "evidence": n_ev})
        withdraw_at = t + timedelta(minutes=r.uniform(10, 120)) if r.random() < 0.07 else None
        t_c = (reads.get(resolver) or self.next_sync(resolver, t)) + self.compose(resolver, 1.0)
        if withdraw_at and withdraw_at < t_c:
            return self.withdraw(withdraw_at, cid, raiser, [resolver], thread)
        pool = [m for m in self.members if m not in (raiser, resolver)]
        consulted, answers = [], []
        if pool and r.random() < 0.8:  # conflict_consult: ONE question DM per consultant, in the conflict thread
            weights = [0.25 if self.plat[m] == "human" else (1.0 if self.plat[m] != self.plat[raiser] else 0.45)
                       for m in pool]
            for _ in range(min(len(pool), r.choices([1, 2, 3], [0.6, 0.3, 0.1])[0])):
                c = r.choices(pool, weights)[0]
                weights[pool.index(c)] = 0.0
                consulted.append(c)
                if not any(weights):
                    break
            consulted = sorted(set(consulted))
            seen = {}
            for c in consulted:
                _, _, rd = self.message(t_c, resolver, [c], "question", f"Opinion needed on {cid}", thread=thread,
                                        has_context=True)
                seen.update(rd)
            self.emit(t_c, "CONFLICT_CONSULTED", pid, resolver, payload={"conflict_id": cid, "asked": consulted})
            self.stats["conflicts_consulted"] += 1
            answers = sorted((seen[c] + self.compose(c, 2.5), c) for c in consulted
                             if c in seen and r.random() < BEHAVIOUR[self.plat[c]][2])
        # when does the resolver act?
        if not consulted:
            t_res = t_c
        elif len(answers) == len(consulted):
            t_res = self.next_sync(resolver, answers[-1][0]) + self.compose(resolver, 1.5)
        elif r.random() < 0.12:
            t_res = None  # never resolved: stays 'consulting'
        else:  # waits a while, then resolves with force=true
            t_res = self.next_sync(resolver, t_c + timedelta(minutes=r.uniform(45, 240))) + self.compose(resolver, 1.5)
        stop = min(x for x in (t_res, withdraw_at) if x) if (t_res or withdraw_at) else None
        answers = [a for a in answers if stop is None or a[0] < stop]  # later answers: "opinions are closed"
        truth = "accept_challenge" if valid else "keep"
        opinions = []
        for i, (ta, c) in enumerate(answers):  # conflict_opinion: grounded, or 'unsure'
            x = r.random()
            position = "unsure" if x < 0.10 else "merge" if x < 0.22 else (truth if r.random() < 0.8 else other_side(truth))
            cites = r.randint(1, 3) if position != "unsure" or r.random() < 0.4 else 0
            rels = ([round(r.uniform(0.4, 0.85), 2)] if position != "unsure" and r.random() < 0.4 else []) + \
                   [round(r.uniform(0.3, 0.8), 2) for _ in range(cites) if r.random() < 0.5]
            opinions.append((c, position, rels))
            self.message(ta, c, [resolver], "answer", f"Opinion on {cid}: {position}", thread=thread)
            self.emit(ta, "CONFLICT_OPINION", pid, c, payload={
                "conflict_id": cid, "position": position, "cites": cites, "pending": len(consulted) - i - 1})
            self.stats["opinions"] += 1
        if withdraw_at and (t_res is None or withdraw_at < t_res):
            return self.withdraw(withdraw_at, cid, raiser, [resolver] + consulted, thread)
        if t_res is None:
            self.stats["conflicts_left_open"] += 1
            return
        # conflict_resolve with Laya's evidence audit (conflicts._side_scores)
        exist = existing + [x for _, pos, rels in opinions if pos == "keep" for x in rels]
        chall = challenge + [x for _, pos, rels in opinions if pos == "accept_challenge" for x in rels]
        scores = {"existing": laya._evidence_score(exist), "challenge": laya._evidence_score(chall)}
        votes = Counter(pos for _, pos, _ in opinions if pos != "unsure").most_common()
        if votes:
            resolution = votes[0][0] if len(votes) == 1 or votes[0][1] > votes[1][1] else "merge"
        else:
            favoured = "accept_challenge" if scores["challenge"] > scores["existing"] else "keep"
            resolution = favoured if r.random() < 0.8 else ("merge" if r.random() < 0.5 else other_side(favoured))
        if resolution != "merge" and r.random() < 0.10:
            resolution = other_side(resolution)  # the coordinator overrides the advice (its rationale is recorded)
        chosen = {"keep": "existing", "accept_challenge": "challenge"}.get(resolution)
        against = bool(chosen and scores[{"existing": "challenge", "challenge": "existing"}[chosen]]
                       - scores[chosen] >= 0.2)
        if resolution in ("accept_challenge", "merge"):
            self.finding(t_res, resolver, None, f"{'merged resolution' if resolution == 'merge' else 'accepted challenge'}"
                                                f" of {cid}")
            for t_d, did, question, mode in list(self.decisions):
                if did in about:  # never overturn a Laya decision alone: the question goes back to a debate
                    self.stats["debates_reopened"] += 1
                    self.debate(0, self.domain, t_res, question=question, mode=mode, opener=resolver,
                                context=f"Re-opened by conflict {cid}")
        forced = len(opinions) < len(consulted)
        self.emit(t_res, "CONFLICT_RESOLVED", pid, resolver, payload={
            "conflict_id": cid, "resolution": resolution, "opinions": len(opinions), "against_evidence": against,
            "forced": forced})
        self.stats[f"resolved_{resolution}"] += 1
        self.stats["resolved_against_evidence"] += against
        involved = {raiser, *consulted} | {self.authors[a] for a in about if self.authors.get(a) in self.plat}
        involved.discard(resolver)
        if involved:  # the result goes ONLY to the people involved
            self.message(t_res, resolver, sorted(involved), "decision", f"Resolved {cid}: {resolution}",
                         thread=thread)

    def withdraw(self, t: datetime, cid: str, raiser: str, to: list, thread: str) -> None:
        self.emit(t, "CONFLICT_WITHDRAWN", self.pid, raiser, payload={"conflict_id": cid})
        self.stats["conflicts_withdrawn"] += 1
        to = sorted(set(to) - {raiser})
        if to:
            self.message(t, raiser, to, "info", f"Withdrawn {cid}", thread=thread)

    # ------------------------------------------------------------------ communication
    def message(self, t: datetime, sender: str, to, kind: str, subject: str, thread: str | None = None,
                reply_to: str = "", has_context: bool = False, exclude=(), chars: int | None = None
                ) -> tuple[str, list, dict]:
        """MESSAGE_SENT exactly as core.send_message emits it (a subject only on broadcasts), then a MESSAGE_READ
        per recipient at its next sync (core.message_inbox). Returns (message_id, recipients, {reader: read_at})."""
        r = self.rng
        mid = f"MSG-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
        recipients = ["all"] if to == "all" else sorted(set(to))
        chars = chars or int(r.lognormvariate(math.log(MEDIAN_CHARS.get(kind, 400)), 0.7)) + 1
        payload = {"message_id": mid, "kind": kind, "to": recipients, "thread_id": thread or mid, "chars": chars,
                   "has_context": has_context, "reply_to": reply_to}
        if recipients == ["all"]:
            payload["subject"] = subject[:80]
        self.emit(t, "MESSAGE_SENT", self.pid, sender, payload=payload,
                  target_participant_id=recipients[0] if len(recipients) == 1 else "multi")
        audience = ([m for m in self.members if m != sender and m not in exclude] if recipients == ["all"]
                    else [m for m in recipients if m != sender and m in self.syncs])
        reads = {}
        for m in audience:
            if r.random() < 0.98:  # ~2% never opened (the session ended)
                at = self.next_sync(m, t)
                self.emit(at, "MESSAGE_READ", self.pid, m, payload={"message_id": mid}, target_participant_id=sender)
                reads[m] = at
        return mid, recipients, reads

    def answers(self, qmid: str, thread: str, asker: str, reads: dict, broadcast: bool = False,
                depth: int = 0) -> None:
        """Readers answer a question (reply_to = the question) after reading it; the asker sometimes follows up."""
        r = self.rng
        readers = sorted(reads.items(), key=lambda kv: kv[1])
        if broadcast:  # an open question to everyone: the first one or two readers pick it up
            readers = readers[:r.choice([1, 1, 2])]
        for m, seen in readers:
            if r.random() > BEHAVIOUR[self.plat[m]][2]:
                continue
            amid, _, back = self.message(seen + self.compose(m), m, [asker], "answer", "Re: question", thread=thread,
                                         reply_to=qmid)
            if depth < 2 and asker in back and r.random() < 0.25:
                fmid, _, fr = self.message(back[asker] + self.compose(asker), asker, [m], "question", "Follow-up",
                                           thread=thread, reply_to=amid)
                self.answers(fmid, thread, asker, fr, depth=depth + 1)

    def communicate(self, t0: datetime, t1: datetime, task_ids: list, domain: str) -> None:
        r = self.rng
        span = max((t1 - t0).total_seconds(), 3600.0)

        def at() -> datetime:
            return t0 + timedelta(seconds=r.uniform(0, span))

        weights = [0.4 if self.plat[m] == "human" else 1.0 for m in self.members]
        # direct and small-group messages
        for _ in range(r.randint(4, 24)):
            sender = r.choices(self.members, weights)[0]
            others = [m for m in self.members if m != sender]
            n_to = 1 if len(others) < 2 or r.random() < 0.85 else r.randint(2, min(3, len(others)))
            kind = r.choices(*DM_KINDS)[0]
            mid, _, reads = self.message(at(), sender, r.sample(others, n_to), kind, f"{kind}: {domain}")
            if kind == "question":
                self.answers(mid, mid, sender, reads)
            elif kind in ("request", "review") and reads and r.random() < 0.5:
                m, seen = min(reads.items(), key=lambda kv: kv[1])
                self.message(seen + self.compose(m), m, [sender], "info", "On it", thread=mid, reply_to=mid)
        # broadcasts: status updates, alerts and open questions to everyone
        for _ in range(r.randint(1, 6)):
            sender = r.choices(self.members, weights)[0]
            kind = r.choices(["info", "question", "alert"], [0.6, 0.3, 0.1])[0]
            mid, _, reads = self.message(at(), sender, "all", kind, f"{kind} for everyone: {domain}")
            if kind == "question":
                self.answers(mid, mid, sender, reads, broadcast=True)
        # context packages (context_send): MESSAGE_SENT kind=context, then CONTEXT_SHARED
        for _ in range(r.randint(0, 5)):
            sender = r.choice(self.voters)
            others = [m for m in self.members if m != sender]
            if not others:
                break
            to = "all" if r.random() < 0.3 else r.sample(others, min(len(others), r.choice([1, 1, 2])))
            task = r.choice(task_ids) if task_ids and r.random() < 0.7 else None
            t = at()
            mid, rec, _ = self.message(t, sender, to, "context",
                                       f"{self.name[sender]} shared context" + (f" for task {task}" if task else ""),
                                       has_context=True)
            self.emit(t, "CONTEXT_SHARED", self.pid, sender, task,
                      {"message_id": mid, "to": rec, "nodes": r.choice([0, 1, 2, 3, 5])})
        # chat ingestion (chat_ingest): findings / agreements become graph nodes, extracted tasks are created
        for _ in range(r.choices([0, 1, 2, 3], [0.3, 0.4, 0.2, 0.1])[0]):
            who, t, kind = r.choice(self.voters), at(), "chat" if r.random() < 0.85 else "note"
            made = {"findings": r.randint(0, 6), "agreements": r.randint(0, 3),
                    "open_questions": r.randint(0, 4), "tasks": r.randint(0, 3)}
            for i in range(made["findings"] + made["agreements"]):
                fid = f"F-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
                what = "agreement" if i >= made["findings"] else "chat insight"
                self.emit(t, "FINDING_ADDED", self.pid, who, payload={"finding_id": fid, "claim": f"{domain} {what}"})
                self.emit(t, "CONTEXT_UPDATED", self.pid, who,
                          payload={"node": fid, "hint": "Query context for project"})
            for _ in range(made["tasks"]):
                self.emit(t, "TASK_CREATED", self.pid, who, f"T-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}",
                          {"title": f"follow-up from {kind}", "task_type": "general", "depends_on": []},
                          status="READY")
            self.emit(t, "CONVERSATION_INGESTED", self.pid, who, payload={
                "conversation_id": self.nid("CONV"), "kind": kind, "chars": int(r.lognormvariate(8.6, 0.9)),
                **made})
            self.message(t, who, "all", "context", f"Ingested: {domain} {kind}", exclude=[who])

    # ------------------------------------------------------------------ debates → Laya
    def debate(self, q: int, domain: str, t0: datetime, question: str | None = None, mode: str | None = None,
               opener: str | None = None, context: str = "") -> None:
        r = self.rng
        mode = mode or ("deliberative" if r.random() < 0.65 else "empirical")
        question = question or r.choice(DEBATE_QUESTIONS[mode]).format(d=domain) + f" (#{q + 1})"
        st = {"q": question, "mode": mode, "t0": t0, "props": [], "quality": {}, "lean": {}, "nudged": set(),
              "contradiction": False, "verify": False, "advisory_votes": False, "evidence_rounds": 0}
        opener = opener or (self.human if self.human and r.random() < 0.25 else r.choice(self.voters))
        self.emit(t0, "DEBATE_OPENED", self.pid, opener,
                  payload={"debate_id": self.nid("DB"), "question": question[:200], "mode": mode})
        _, _, seen = self.message(t0, opener, "all", "request", f"Debate: {question}", exclude=[opener],
                                  chars=420 + len(context))
        seen.setdefault(opener, t0)
        if r.random() < 0.06:  # somebody asks Laya before any perspective exists
            ta = t0 + timedelta(minutes=r.uniform(1, 10))
            self.laya_event(st, ta, "NEED_PERSPECTIVES",
                            ["no admissible perspective (all excluded or none submitted)"])
            self.message(ta, "laya", "all", "request", f"Laya needs more input: {question[:60]}")
        # every participant submits a perspective after it has read the invitation
        subs = [((seen.get(v) or self.next_sync(v, t0)) + self.compose(v, 8), v) for v in self.voters
                if r.random() < (0.92 if mode == "deliberative" else 0.8)]
        if not subs:
            v = r.choice(self.voters)
            subs = [(self.next_sync(v, t0) + self.compose(v, 8), v)]
        for ts, v in sorted(subs):
            self.propose(st, v, ts)
        t = max(ts for ts, _ in subs)
        for _ in range(12):
            if mode == "deliberative":
                t, verdict = self.collect_votes(st, t)  # Laya auto-decides once every vote is in
                if verdict is None:  # nothing to auto-decide: someone asks Laya explicitly
                    t += timedelta(minutes=r.uniform(5, 60))
                    verdict = self.assess(st)
                    if verdict["outcome"] == "NEED_VOTES":
                        self.need_votes(st, t, verdict)
                        continue
            else:
                t, verdict = self.empirical_step(st, t)
            done, t = self.act(st, t, verdict)
            if done:
                return  # decided, or abandoned without enough evidence (the debate stays open)

    def prop(self, st: dict, prid: str) -> dict:
        return next(p for p in st["props"] if p["id"] == prid)

    def propose(self, st: dict, author: str, t: datetime, kind: str = "perspective", sources=()) -> str:
        r = self.rng
        prid = self.nid("PR")
        if kind == "synthesis":  # usually better than both sources, sometimes worse
            quality = max(st["quality"][s] for s in sources) + r.gauss(0.25, 0.6)
            evidence = [round(r.uniform(0.4, 0.9), 2) for _ in range(r.choice([0, 1]))]
            approach = "synthesis of " + " + ".join(sources)
        else:
            quality = r.gauss(0, 1)
            evidence = [round(r.uniform(0.35, 0.9), 2) for _ in range(r.choice([0, 1, 1, 2, 2, 3]))]
            approach = f"approach proposed by {self.name.get(author, author)}"
        self.emit(t, "SYNTHESIS_CREATED" if kind == "synthesis" else "PROPOSAL_CREATED", self.pid, author, payload={
            "proposal_id": prid, "question": st["q"][:200], "approach": approach[:200], "kind": kind,
            "derived_from": list(sources)})
        if st["props"] and not st["contradiction"]:  # first rival → one open contradiction for the question
            st["contradiction"] = True
            self.emit(t, "CONTRADICTION_CREATED", self.pid, author,
                      payload={"contradiction_id": self.nid("C"), "question": st["q"][:200]})
        _, _, seen = self.message(t, author, "all", "request", f"Vote: {st['q'][:80]}", exclude=[author])
        st["quality"][prid] = quality
        st["props"].append({"id": prid, "approach": approach, "author": author, "kind": kind,
                            "derived_from": list(sources), "violates": [], "evidence": evidence,
                            "results": [], "votes": [], "t": t, "seen": seen})
        self.evidence[prid], self.authors[prid] = evidence, author
        return prid

    def assess(self, st: dict) -> dict:
        """Score the open proposals with the real Laya engine (a synthesis inherits its sources' evidence)."""
        data = []
        for p in st["props"]:
            ev = list(p["evidence"]) + [e for s in p["derived_from"] for e in self.prop(st, s)["evidence"]]
            data.append({"id": p["id"], "approach": p["approach"], "author": p["author"], "kind": p["kind"],
                         "derived_from": p["derived_from"], "violates": p["violates"], "evidence": ev,
                         "results": p["results"], "votes": p["votes"]})
        return laya.evaluate(data, laya.policy_for(st["mode"]), self.voters)

    def vote(self, st: dict, voter: str, prid: str, t: datetime) -> None:
        r = self.rng
        lean = st["lean"].setdefault(voter, r.gauss(0, 0.5))
        value = "approve" if r.random() < sigmoid(1.6 * st["quality"][prid] + lean) else "reject"
        p = self.prop(st, prid)
        p["votes"] = [v for v in p["votes"] if v["voter"] != voter] + [
            {"voter": voter, "value": value, "confidence": round(r.uniform(0.5, 1.0), 2)}]
        self.emit(t, "VOTE_CAST", self.pid, voter,
                  payload={"proposal_id": prid, "value": value, "question": st["q"][:200]})

    def collect_votes(self, st: dict, t_from: datetime) -> tuple[datetime, dict | None]:
        """Each voter votes on every other perspective at its next sync after reading the vote request; Laya
        re-evaluates after every vote (core._maybe_auto_decide). Returns the first actionable verdict."""
        r = self.rng
        plan = []
        for v in self.voters + ([self.human] if self.human else []):
            for p in st["props"]:
                if p["author"] == v or any(x["voter"] == v for x in p["votes"]):
                    continue
                if v == self.human and r.random() > 0.3:  # the human votes now and then (never in quorum)
                    continue
                nudged = (v, p["id"]) in st["nudged"]
                if v != self.human and not nudged and r.random() < 0.03:  # forgets → Laya will have to nudge
                    continue
                base = max(p["seen"].get(v) or p["t"], t_from)
                plan.append((self.next_sync(v, base) + timedelta(minutes=r.uniform(0.5, 4)), v, p["id"]))
        plan.sort()
        ask_at = plan[0][0] + (plan[-1][0] - plan[0][0]) * r.random() if plan and r.random() < 0.3 else None
        for at, v, prid in plan:
            if ask_at and at >= ask_at:  # someone asks Laya before every vote is in
                verdict = self.assess(st)
                if verdict["outcome"] == "NEED_VOTES":
                    self.need_votes(st, ask_at, verdict)
                elif verdict["outcome"] in ACTIONABLE:
                    return ask_at, verdict
                ask_at = None
            self.vote(st, v, prid, at)
            verdict = self.assess(st)
            if verdict["outcome"] in ACTIONABLE:
                return at, verdict
        return (plan[-1][0] if plan else t_from), None

    def need_votes(self, st: dict, t: datetime, verdict: dict) -> None:
        self.laya_event(st, t, "NEED_VOTES", verdict["missing"])
        for voter in sorted({v for vs in verdict["pending_votes"].values() for v in vs}):
            owed = [prid for prid, vs in verdict["pending_votes"].items() if voter in vs]
            _, _, seen = self.message(t, "laya", [voter], "request", f"Your vote is needed: {st['q'][:70]}")
            for prid in owed:
                st["nudged"].add((voter, prid))
                p = self.prop(st, prid)
                p["seen"][voter] = max(p["seen"].get(voter, t), seen.get(voter, t))

    def empirical_step(self, st: dict, t: datetime) -> tuple[datetime, dict]:
        """Empirical mode: an explicit decision request; once Laya asked for verification, an independent
        participant runs a reproducible experiment and submit_experiment triggers Laya."""
        r = self.rng
        if not st["advisory_votes"]:  # a few votes; they carry little weight and no quorum applies
            st["advisory_votes"] = True
            for p in st["props"]:
                for v in self.voters:
                    if v != p["author"] and r.random() < 0.35:
                        self.vote(st, v, p["id"], self.next_sync(v, p["seen"].get(v) or p["t"]))
        if not st["verify"]:
            return t + timedelta(minutes=r.uniform(5, 90)), self.assess(st)
        for _ in range(3):  # an experiment run may fail; a failed run is never evidence
            ranking = self.assess(st)["ranking"]
            untested = [x["proposal_id"] for x in ranking if not self.prop(st, x["proposal_id"])["results"]]
            target = self.prop(st, untested[0] if untested else ranking[0]["proposal_id"])
            runner = next((v for v in self.voters if v != target["author"] and r.random() < 0.7), target["author"])
            env = r.choice(list(ENVIRONMENTS))
            minutes = r.lognormvariate(math.log(45), 0.6)
            end = self.next_sync(runner, t) + timedelta(minutes=minutes)
            xid = self.nid("X")
            if r.random() < 0.06:
                self.emit(end, "EXECUTION_FAILED", self.pid, runner,
                          payload={"experiment_id": xid, "environment": env}, status="FAILED",
                          duration_ms=int(minutes * 60000))
                t = end + timedelta(minutes=r.uniform(5, 30))
                continue
            supports = r.random() < sigmoid(1.5 * st["quality"][target["id"]])
            sha = uuid.UUID(int=r.getrandbits(128)).hex[:7] if r.random() < 0.85 else None
            reproducible = bool(sha) and r.random() < 0.85
            target["results"].append({"supports": supports, "reproducible": reproducible, "commit_sha": sha})
            self.emit(end, "EXPERIMENT_COMPLETED", self.pid, runner, payload={
                "experiment_id": xid, "proposal_id": target["id"],
                "metrics": {"score": round(r.uniform(0.6, 0.97), 3)}, "supports": supports,
                "reproducible": reproducible, "environment": env},
                status="SUCCEEDED", duration_ms=int(minutes * 60000), commit_sha=sha, result_ref=self.nid("R"))
            # with an open contradiction submit_experiment asks Laya itself; otherwise somebody asks a bit later
            return (end if st["contradiction"] else end + timedelta(minutes=r.uniform(5, 30))), self.assess(st)
        return t, self.assess(st)

    def act(self, st: dict, t: datetime, verdict: dict) -> tuple[bool, datetime]:
        """What core.request_decision does with each verdict. Returns (debate finished, time)."""
        r = self.rng
        outcome, q = verdict["outcome"], st["q"]
        if outcome == "DECIDED":
            self.commit(st, t, verdict["selected"])
            return True, t
        if outcome == "VERIFY":  # one verification task per question (no LAYA_ event)
            if not st["verify"]:
                st["verify"] = True
                self.outcomes["VERIFY"] += 1
                self.emit(t, "VERIFICATION_REQUESTED", self.pid, "laya", self.nid("T"),
                          payload={"question": q[:200], "missing": verdict["missing"]})
            return False, t
        if outcome == "NEED_VOTES":
            self.need_votes(st, t, verdict)
            return False, t
        self.laya_event(st, t, outcome, verdict["missing"])
        if outcome == "SYNTHESIZE":
            pair = verdict["synthesize"]
            authors = {self.prop(st, x)["author"] for x in pair}
            owner = next((m for m in sorted(self.voters) if m not in authors), self.prop(st, pair[1])["author"])
            self.emit(t, "SYNTHESIS_REQUESTED", self.pid, "laya", payload={"question": q[:200], "pair": pair},
                      target_participant_id=owner)
            _, _, seen = self.message(t, "laya", "all", "request", f"Combine perspectives: {q[:60]}")
            ts = (seen.get(owner) or self.next_sync(owner, t)) + self.compose(owner, 12)
            self.propose(st, owner, ts, "synthesis", pair)
            return False, ts
        if outcome == "ESCALATE":
            _, _, seen = self.message(t, "laya", "all", "alert", f"Tie-break needed: {q[:60]}")
            breaker = self.human if self.human in seen else self.voters[0]  # dashboard human, else coordinator
            tb = (seen.get(breaker) or self.next_sync(breaker, t)) + self.compose(breaker, 3)
            top = verdict["contenders"]
            self.commit(st, tb, top[0] if len(top) < 2 or r.random() < 0.7 else top[1], tiebreak=True)
            return True, tb
        # NEED_EVIDENCE / NEED_PERSPECTIVES: participants strengthen their perspectives, then ask again
        _, _, seen = self.message(t, "laya", "all", "request", f"Laya needs more input: {q[:60]}")
        st["evidence_rounds"] += 1
        if st["evidence_rounds"] > 3:
            return True, t
        last = t
        ranking = [x["proposal_id"] for x in verdict["ranking"]]
        for v in [v for v in self.voters if v in seen and r.random() < 0.6] or [r.choice(self.voters)]:
            own = [p["id"] for p in st["props"] if p["author"] == v]
            target = self.prop(st, own[0] if own else ranking[0])
            te = (seen.get(v) or self.next_sync(v, t)) + self.compose(v, 4)
            target["evidence"].append(round(r.uniform(0.5, 0.9), 2))
            self.emit(te, "EVIDENCE_ADDED", self.pid, v, payload={"target": target["id"], "stance": "supports"})
            last = max(last, te)
        return False, last

    def laya_event(self, st: dict, t: datetime, outcome: str, missing: list) -> None:
        self.outcomes[outcome] += 1
        self.emit(t, f"LAYA_{outcome}", self.pid, "laya", payload={"question": st["q"][:200], "missing": missing[:5]})

    def commit(self, st: dict, t: datetime, selected: str, tiebreak: bool = False) -> None:
        sel = self.prop(st, selected)
        self.outcomes["TIEBREAK" if tiebreak else "DECIDED"] += 1
        did = self.nid("D")
        # duration_ms (debate opened → decided) is a simulator convenience; the backend leaves it null
        self.emit(t, "DECISION_COMMITTED", self.pid, "laya", payload={
            "decision_id": did, "question": st["q"][:200], "selected": selected, "mode": st["mode"],
            "num_options": len(st["props"]), "synthesis": sel["kind"] == "synthesis", "tiebreak": tiebreak,
            "contradictions_resolved": int(st["contradiction"])},
            duration_ms=int((t - st["t0"]).total_seconds() * 1000))
        self.decisions.append((t, did, st["q"], st["mode"]))
        self.evidence[did] = []  # a Decision is BASED_ON evidence, it has no SUPPORTED_BY edges of its own
        self.message(t, "laya", "all", "decision", f"Decided: {st['q'][:70]}")

    def drain(self):
        while self.heap:
            t, _, ev = heapq.heappop(self.heap)
            if self.shift:
                ev["occurred_at"] = (t + self.shift).isoformat()
            yield ev
            if self.rng.random() < 0.01:  # at-least-once duplicate delivery
                yield ev
            if self.rng.random() < 0.005:  # malformed event for the quarantine path
                yield {"event_type": None, "project_id": ev["project_id"], "occurred_at": "not-a-date"}


def plan_of(task: dict) -> dict:
    return task["plan"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--projects", type=int, default=100)
    ap.add_argument("--hours", type=float, default=24, help="span of simulated project starts")
    ap.add_argument("--out", default="kafka", help="'kafka' or a .jsonl path")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--rate", type=float, default=0, help="max events/sec to Kafka (0 = unlimited)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--anchor", choices=["end", "start"], default="end",
                    help="'end' (default): shift the timeline so the last simulated event is now; "
                         "'start': projects start now and run into the future")
    a = ap.parse_args()

    sim = Sim(a.seed)
    origin = datetime.now(timezone.utc)
    for _ in range(a.projects):
        sim.project(origin + timedelta(hours=sim.rng.uniform(0, a.hours)))
    if a.anchor == "end" and sim.heap:
        sim.shift = datetime.now(timezone.utc) - max(t for t, _, _ in sim.heap)

    sent, t0, types = 0, time.time(), Counter()
    if a.out == "kafka":
        from kafka import KafkaProducer

        prod = KafkaProducer(bootstrap_servers=a.bootstrap, linger_ms=20, batch_size=262144,
                             key_serializer=lambda k: (k or "none").encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
        for ev in sim.drain():
            prod.send(topic_for(ev.get("event_type") or "ERROR"), key=ev.get("project_id"), value=ev)
            types[ev.get("event_type")] += 1
            sent += 1
            if a.rate and sent % 100 == 0:
                ahead = sent / a.rate - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
        prod.flush()
    else:
        with open(a.out, "w", encoding="utf-8") as fh:
            for ev in sim.drain():
                fh.write(json.dumps(ev) + "\n")
                types[ev.get("event_type")] += 1
                sent += 1
    dt = time.time() - t0
    print(f"sent {sent:,} events from {a.projects} projects in {dt:.1f}s ({sent / max(dt, 1e-9):,.0f} ev/s)")
    groups = {"orchestration": ("PLAN_", "TASK_DISPATCHED", "TASK_REASSIGNED", "COMMAND_REJECTED"),
              "conflicts": ("CONFLICT_",),
              "collaboration": ("MESSAGE_", "CONTEXT_SHARED", "CONVERSATION_", "DEBATE_", "PROPOSAL_", "SYNTHESIS_",
                                "VOTE_", "LAYA_", "DECISION_", "VERIFICATION_", "EVIDENCE_", "CONTRADICTION_")}
    for label, prefixes in groups.items():
        sel = {k: v for k, v in types.items() if k and k.startswith(prefixes)}
        print(f"{label} events: " + ", ".join(f"{k}={v:,}" for k, v in sorted(sel.items())))
    print("Laya outcomes: " + ", ".join(f"{k}={v:,}" for k, v in sim.outcomes.most_common()))
    print("simulated outcomes: " + ", ".join(f"{k}={v:,}" for k, v in sorted(sim.stats.items())))


if __name__ == "__main__":
    main()
