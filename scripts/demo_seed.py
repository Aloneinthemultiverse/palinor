"""Seed a rich PALINOR demo project through the real MCP endpoint.

Usage:
    python scripts/demo_seed.py --url http://localhost:8000/mcp --key <server key> [-v]

Every run creates a NEW project (safe to repeat). It drives the same MCP tools the AI sessions use, as four
independent participants whose names PALINOR assigns automatically (name=""):

    Claude-1   coordinator  - plans the work, resolves conflicts
    Claude-2   worker       - research / review
    ChatGPT-1  worker       - modelling / analysis
    OpenCode-1 worker       - coding / benchmarks

Story:
  1. naming guard: with two Claudes online, addressing plain "Claude" is rejected as ambiguous
  2. chat_ingest of the kickoff chat (findings, agreements, open questions, a task)
  3. findings with evidence (one of them will be challenged later)
  4. messages in every direction: a 3-message reply thread, a review request, a multi-recipient alert,
     a broadcast, and a context_send; an identical re-send is suppressed as a duplicate
  5. plan_submit by Claude-1 with 3 tasks (features -> scorer -> review); every hand-off is automatic
  6. ChatGPT-1, while working, finds the graph contradicts its measurements: conflict_raise (an invented
     node id is rejected first) -> only Claude-1 hears about it -> conflict_consult(OpenCode-1) ->
     grounded conflict_opinion -> Claude-1 conflict_resolve(accept_challenge) with Laya's evidence audit
  7. ChatGPT-1 completes the features task -> scorer dispatched to OpenCode-1 with the upstream summary ->
     completed -> review dispatched to Claude-2, which is left RUNNING (plan 2/3)
  8. debate 1 (deliberative): 3 perspectives, votes -> Laya SYNTHESIZE -> the assigned participant writes the
     synthesis -> votes -> Laya DECIDED (the synthesis supersedes its sources)
  9. debate 2: still tied after a synthesis -> Laya ESCALATE (the dashboard human can tie-break)
 10. debate 3: still collecting votes -> NEED_VOTES (the dashboard human can vote)
Prints project_id and admin_key for the dashboard.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import warnings
from contextlib import AsyncExitStack
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from mcp import ClientSession


class SeedError(RuntimeError):
    pass


class ToolError(SeedError):
    """A tool returned a structured domain error (expected in the guard checks)."""


def with_key(url: str, key: str) -> str:
    """Append ?key=<server key> (the KeyGate accepts it on every request)."""
    if not key:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query["key"] = key
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def unwrap(result) -> object:
    """Tool result -> python value (structuredContent, possibly wrapped as {'result': ...}, or JSON text)."""
    data = getattr(result, "structuredContent", None)
    if data is None:
        texts = [c.text for c in (result.content or []) if getattr(c, "type", "") == "text"]
        raw = "\n".join(texts)
        try:
            data = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            data = raw
    if isinstance(data, dict) and set(data) == {"result"}:
        data = data["result"]
    if getattr(result, "isError", False):
        raise ToolError(f"tool error: {data}")
    if isinstance(data, dict) and "error" in data and "message" in data:
        raise ToolError(f"{data['error']}: {data['message']}")
    return data


class Hub:
    """One MCP connection; every participant below is an independent identity (its own token)."""

    def __init__(self, session: ClientSession, verbose: bool):
        self.s, self.verbose, self.calls = session, verbose, 0

    async def call(self, tool: str, **args):
        self.calls += 1
        out = unwrap(await self.s.call_tool(tool, args))
        if self.verbose:
            print(f"    {tool:<20} {json.dumps(out, default=str)[:130]}")
        return out


class Actor:
    def __init__(self, hub: Hub, joined: dict):
        self.hub, self.token = hub, joined["token"]
        self.id, self.name = joined["participant_id"], joined["name"]
        self.seq = 0

    async def call(self, tool: str, **args):
        return await self.hub.call(tool, token=self.token, **args)

    async def sync(self) -> dict:
        """What a real session does at the start of every turn."""
        out = await self.call("palinor_sync", since_seq=self.seq)
        self.seq = out["latest_seq"]
        return out


def say(who: str, text: str) -> None:
    print(f"  {who:<10} {text}")


def expect(cond: bool, what: str) -> None:
    if not cond:
        raise SeedError(what)


async def open_transport(stack: AsyncExitStack, url: str, key: str):
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    try:  # mcp >= 1.2x: the non-deprecated client takes a configured httpx client
        import httpx
        from mcp.client.streamable_http import streamable_http_client

        http = await stack.enter_async_context(
            httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(30, read=300), follow_redirects=True))
        return await stack.enter_async_context(streamable_http_client(url, http_client=http))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return await stack.enter_async_context(streamablehttp_client(url, headers=headers))


async def vote(actor: Actor, proposal: dict, value: str, confidence: float, rationale: str) -> dict:
    return await actor.call("vote_cast", proposal_id=proposal["id"], value=value, confidence=confidence,
                            rationale=rationale)


async def seed(url: str, key: str, verbose: bool) -> dict:
    async with AsyncExitStack() as stack:
        read, write, _ = await open_transport(stack, with_key(url, key), key)
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        hub = Hub(session, verbose)

        # ------------------------------------------------------------ project + people
        proj = await hub.call(
            "palinor_create_project", name="Realtime Fraud Radar",
            goal="Detect card fraud on a 50k events/s stream with < 200 ms end-to-end latency",
            requirements=["p99 latency < 200 ms", "explainable alerts for analysts"],
            constraints=["open-source stack only", "runs on the existing Kafka + Spark + HBase cluster"])
        pid, join_key, admin_key = proj["project_id"], proj["join_key"], proj["admin_key"]
        print(f"project {pid} created")

        async def join(platform: str, caps: list[str], role: str = "worker") -> Actor:
            j = await hub.call("palinor_join", project_id=pid, join_key=join_key, name="", platform=platform,
                               role=role, capabilities=caps, admin_key=admin_key if role == "coordinator" else "")
            return Actor(hub, j)

        c1 = await join("claude", ["planning", "review"], "coordinator")
        c2 = await join("claude", ["research", "review"])
        gpt = await join("chatgpt", ["modeling", "analysis"])
        oc = await join("opencode", ["coding", "benchmarks"])
        names = [a.name for a in (c1, c2, gpt, oc)]
        expect(names == ["Claude-1", "Claude-2", "ChatGPT-1", "OpenCode-1"], f"unexpected auto names {names}")
        print("participants: " + ", ".join(f"{a.name} ({a.id})" for a in (c1, c2, gpt, oc)))

        print("\n1) naming guard")
        try:
            await gpt.call("message_send", to="Claude", body="Who owns the HBase schema?")
            raise SeedError("addressing 'Claude' with two Claudes online should be ambiguous")
        except ToolError as exc:
            say(gpt.name, f"to='Claude' rejected: {str(exc).split(':', 1)[1].strip()[:90]}...")

        # --------------------------------------------------------- chat ingestion
        print("\n2) chat ingestion")
        conv = await c1.call(
            "chat_ingest", title="Kickoff: fraud radar architecture", source_platform="claude",
            transcript=("User and Claude scoped the fraud radar. Card events arrive on Kafka at ~50k/s with "
                        "3x bursts. Analysts need a reason for every alert. Spark Structured Streaming is "
                        "already deployed; HBase stores per-card profiles keyed by salted card id."),
            summary="Scoping chat: throughput, latency budget, explainability, existing cluster.",
            findings=["Peak load is ~50k card events/s with 3x bursts on Black Friday"],
            agreements=["Every alert must carry the top-3 contributing features"],
            open_questions=["Do we need exactly-once semantics for alert emission?",
                            "How long should per-card behavioural windows be?"],
            tasks=["Collect a labelled week of transactions for offline evaluation"])
        say(c1.name, f"ingested {conv['conversation_id']}: "
                     + ", ".join(f"{len(v)} {k}" for k, v in conv["created"].items()))

        # ----------------------------------------------------- findings + evidence
        print("\n3) findings")
        f_salt = await c2.call(
            "finding_submit", claim="16 salt buckets on the card id remove HBase region hot-spotting at 50k events/s",
            summary="From the HBase sizing note", confidence=0.6,
            evidence=[{"source": "HBase sizing note", "uri": "notes://hbase-sizing", "reliability": 0.5}])
        f_bench = await oc.call(
            "finding_submit", claim="Replay at 150k events/s: with 16 salt buckets 2 regions exceed 90% CPU; "
                                    "64 buckets stay below 55%",
            summary="Load test bench://hbase-salt-02", confidence=0.75,
            evidence=[{"source": "load test", "uri": "bench://hbase-salt-02",
                       "excerpt": "16 buckets: r7, r12 > 90% CPU; 64 buckets: max 54%", "reliability": 0.7}])
        f_auc = await gpt.call(
            "finding_submit", claim="Gradient-boosted trees reach 0.94 AUC on last month's labelled fraud set",
            summary="Offline eval on 2.1M transactions", confidence=0.75,
            evidence=[{"source": "offline-eval notebook", "uri": "lake://eval/gbt-v1",
                       "excerpt": "AUC 0.941, recall@1%FPR 0.62", "reliability": 0.7}])
        say(c2.name, f"{f_salt['id']} (will be challenged)")
        say(oc.name, f"{f_bench['id']} load test")
        say(gpt.name, f"{f_auc['id']} offline eval")

        # ------------------------------------------------ messaging, every direction
        print("\n4) messages")
        q = await c1.call("message_send", to=gpt.name, kind="question", subject="Latency budget",
                          body="How much of the 200 ms budget does the feature lookup take?")
        await gpt.sync()  # read receipt for Claude-1
        a1 = await gpt.call("message_send", to="", kind="answer", reply_to=q["message_id"],
                            body="About 12 ms p99: one HBase multi-get of 6 features per event.")
        await c1.call("message_send", to=gpt.name, kind="answer", reply_to=a1["message_id"],
                      body="Good - that leaves ~150 ms for scoring and explanations.")
        say(c1.name, f"thread {q['thread_id']}: question -> answer -> follow-up")
        await oc.call("message_send", to=c1.name, kind="request", subject="Need labels",
                      body="I need the labelled week before the scorer can be evaluated - who owns the export?")
        await c2.call("message_send", to=oc.name, kind="review", subject="Review request",
                      body="Once the scorer lands, please share the latency histogram so I can review it.")
        alert = {"to": [c1.name, oc.name], "kind": "alert", "subject": "Burst traffic",
                 "body": "Replay shows 3x bursts; size the consumer group and HBase for 150k events/s."}
        first = await gpt.call("message_send", **alert)
        again = await gpt.call("message_send", **alert)  # identical re-send within 10 min
        expect(again.get("duplicate") and again["message_id"] == first["message_id"], "duplicate not suppressed")
        say(gpt.name, f"alert to {', '.join(alert['to'])}; identical re-send suppressed (duplicate=True)")
        await c2.call("message_send", to="all", kind="info", subject="Research notes",
                      body="Collected three papers on explainable fraud scoring; summaries go in as findings.")
        await c1.call("context_send", to=[gpt.name, oc.name], node_ids=[f_auc["id"], f_bench["id"]],
                      note="Context for the plan: offline AUC and the HBase load test.")
        say(c1.name, f"context_send -> {gpt.name}, {oc.name} ({f_auc['id']}, {f_bench['id']})")

        # ------------------------------------------------------------- plan (division of labour)
        print("\n5) plan")
        plan = await c1.call("plan_submit", goal="Ship the streaming fraud scorer", tasks=[
            {"key": "features", "title": "Build streaming feature pipeline (Kafka -> Spark -> HBase)",
             "capability": "analysis", "assignee": gpt.name, "priority": "high",
             "acceptance": "6 velocity features in HBase, lookup p99 < 15 ms"},
            {"key": "scorer", "title": "Implement the GBT scorer service", "capability": "coding",
             "depends_on": ["features"], "assignee": oc.name,
             "acceptance": "scores every event, p99 < 60 ms, top-3 SHAP features attached"},
            {"key": "review", "title": "Review scorer latency and alert explanations", "capability": "review",
             "depends_on": ["scorer"], "assignee": c2.name,
             "acceptance": "latency histogram reviewed; explanation examples signed off"}])
        t_feat, t_scorer, t_review = (plan["tasks"][k] for k in ("features", "scorer", "review"))
        say(c1.name, f"{plan['plan_id']}: {t_feat} -> {t_scorer} -> {t_review}; dispatched "
                     + ", ".join(f"{d['task_id']}->{d['participant_id']}" for d in plan["dispatched"]))

        s = await gpt.sync()
        cmd_feat = next(c for c in s["commands"] if c["task_id"] == t_feat)
        await gpt.call("command_update", command_id=cmd_feat["command_id"], status="ACCEPTED")
        await gpt.call("command_update", command_id=cmd_feat["command_id"], status="RUNNING",
                       note="consuming card-events")
        say(gpt.name, f"{cmd_feat['command_id']} ACCEPTED -> RUNNING")

        # ------------------------------------------------------------- conflict
        print("\n6) conflict")
        ctx = await gpt.call("context_get")
        target = next(f for f in ctx["findings"] if f["claim"].startswith("16 salt buckets"))
        try:
            await gpt.call("conflict_raise", about=["F-deadbeef"], claim="hallucinated reference")
            raise SeedError("an invented node id should be rejected")
        except ToolError:
            say(gpt.name, "conflict about an invented id F-deadbeef rejected (refs must be real)")
        cf = await gpt.call(
            "conflict_raise", about=[target["finding_id"]], task_id=t_feat, severity="high",
            claim="16 salt buckets are not enough: at 3x bursts two HBase regions run hot; use 64 buckets",
            evidence=[{"source": "feature pipeline replay", "uri": "bench://feature-replay-150k",
                       "excerpt": "regions r7/r12 at 93% CPU during bursts", "reliability": 0.8}])
        expect(cf["resolver"] == c1.id, f"resolver should be {c1.id}, got {cf['resolver']}")
        say(gpt.name, f"raised {cf['conflict_id']} about {target['finding_id']} -> resolver {c1.name}")

        s1 = await c1.sync()
        expect(any(m["kind"] == "alert" and cf["conflict_id"] in m["subject"] for m in s1["messages"]),
               "coordinator did not receive the conflict alert")
        for other in (c2, oc):
            got = [m for m in (await other.sync())["messages"] if cf["conflict_id"] in (m.get("subject") or "")]
            expect(not got, f"{other.name} should not hear about the conflict yet")
        await c1.call("conflict_consult", conflict_id=cf["conflict_id"], ask=[oc.name],
                      question="Does your load test support 16 or 64 salt buckets?")
        say(c1.name, f"consulted {oc.name} (only the coordinator and the consultant are involved)")

        so = await oc.sync()
        expect(any(m["kind"] == "question" and cf["conflict_id"] in m["subject"] for m in so["messages"]),
               "consultant did not receive the question")
        op = await oc.call("conflict_opinion", conflict_id=cf["conflict_id"], position="accept_challenge",
                           refs=[f_bench["id"]], confidence=0.8,
                           rationale="My replay at 150k/s reproduces the hot regions with 16 buckets; "
                                     "64 buckets keep every region below 55% CPU.")
        say(oc.name, f"opinion accept_challenge citing {f_bench['id']} (pending: {op['pending'] or 'none'})")

        res = await c1.call("conflict_resolve", conflict_id=cf["conflict_id"], resolution="accept_challenge",
                            rationale="Two independent measurements (feature replay + load test) beat the "
                                      "sizing note; switch the row key to 64 salt buckets.")
        audit = res["evidence_audit"]
        name_of = {a.id: a.name for a in (c1, c2, gpt, oc)}
        say(c1.name, f"resolved {res['resolution']} -> {res['decision_id']}; Laya audit existing "
                     f"{audit['existing']} vs challenge {audit['challenge']} (against evidence: "
                     f"{audit['against_evidence']}); notified "
                     + ", ".join(name_of.get(x, x) for x in res["notified"]))

        # ------------------------------------------------------------- hand-offs
        print("\n7) hand-offs")
        await gpt.call("command_update", command_id=cmd_feat["command_id"], status="COMPLETED",
                       result_ref="PR #12",
                       note="Pipeline live: 6 velocity features in HBase table card_profile, 64 salt buckets "
                            "(conflict " + cf["conflict_id"] + "), lookup p99 12 ms.")
        say(gpt.name, f"{t_feat} COMPLETED")
        so = await oc.sync()
        cmd_sc = next(c for c in so["commands"] if c["task_id"] == t_scorer)
        expect("Inputs from upstream tasks" in cmd_sc["instruction"] and t_feat in cmd_sc["instruction"],
               "scorer command does not carry the upstream result")
        say(oc.name, f"received {cmd_sc['command_id']} for {t_scorer} with the upstream summary of {t_feat}")
        await oc.call("command_update", command_id=cmd_sc["command_id"], status="ACCEPTED")
        await oc.call("command_update", command_id=cmd_sc["command_id"], status="RUNNING", note="training GBT")
        await oc.call("github_link", task_id=t_scorer, branch=f"opencode-opencode-1/task-{t_scorer}",
                      commit_sha="9f2c1ab", pr_url="https://github.com/example/fraud-radar/pull/14",
                      pr_state="open", ci_status="passed")
        await oc.call("command_update", command_id=cmd_sc["command_id"], status="COMPLETED", result_ref="PR #14",
                      note="Scorer service: GBT + top-3 SHAP per alert, p99 41 ms at 150k events/s.")
        say(oc.name, f"{t_scorer} COMPLETED")
        sc2 = await c2.sync()
        cmd_rv = next(c for c in sc2["commands"] if c["task_id"] == t_review)
        await c2.call("command_update", command_id=cmd_rv["command_id"], status="ACCEPTED")
        await c2.call("command_update", command_id=cmd_rv["command_id"], status="RUNNING",
                      note="reviewing latency histogram")
        st = await c1.call("pipeline_status")
        say(c1.name, f"pipeline {st['plans'][0]['progress']} ({st['plans'][0]['percent']}%), "
                     f"{t_review} RUNNING at {c2.name}")

        # ------------------------------------- debate 1: SYNTHESIZE -> synthesis -> DECIDED
        print("\n8) debate 1 (deliberative)")
        q1 = "Which scoring architecture should the fraud radar use?"
        await c1.call("debate_open", question=q1, mode="deliberative",
                      context="Must meet p99 < 200 ms and give explainable alerts.")
        pa = await c1.call("perspective_submit", question=q1,
                           approach="Gradient-boosted trees scored inside Spark micro-batches",
                           rationale="Best offline AUC and native SHAP explanations; fits the existing cluster.",
                           evidence=[{"source": "offline eval", "uri": "lake://eval/gbt-v1", "reliability": 0.6}])
        pb = await gpt.call("perspective_submit", question=q1,
                            approach="Rules engine first, model only for borderline events",
                            rationale="Rules are instantly explainable and cheap; the model handles the grey zone.",
                            evidence=[{"source": "analyst interviews", "uri": "notes://analysts", "reliability": 0.6}])
        pc = await oc.call("perspective_submit", question=q1,
                           approach="Deep sequence model (LSTM) on raw event streams",
                           rationale="Captures long behavioural patterns, but needs GPUs.")
        await vote(c2, pa, "approve", 0.9, "Strong AUC and SHAP for analysts.")
        await vote(gpt, pa, "approve", 0.8, "Accurate; fits the cluster.")
        await vote(oc, pa, "approve", 0.8, "Works on our cluster.")
        await vote(c1, pb, "approve", 0.9, "Analysts will trust rules.")
        await vote(c2, pb, "approve", 0.9, "Cheap at 50k/s.")
        await vote(oc, pb, "reject", 0.1, "Rules drift; slight concern.")
        await vote(c1, pc, "reject", 0.8, "No GPUs in the cluster.")
        await vote(c2, pc, "reject", 0.7, "Hard to explain to analysts.")
        last = await vote(gpt, pc, "reject", 0.7, "Too slow to train and serve.")
        laya1 = last.get("laya") or {}
        expect(laya1.get("outcome") == "SYNTHESIZE", f"expected Laya SYNTHESIZE, got {laya1}")
        say("Laya", f"SYNTHESIZE {laya1['synthesize'][0]} + {laya1['synthesize'][1]} (too close)")

        merger = None
        for actor in (c1, c2, gpt, oc):  # whoever Laya asked finds it in your_actions
            acts = (await actor.sync())["your_actions"]
            if any(a["action"] == "synthesize" for a in acts):
                merger = actor
        expect(merger is not None, "nobody was asked to synthesize")
        syn = await merger.call(
            "synthesis_create", question=q1, source_proposal_ids=laya1["synthesize"],
            approach="Rules gate for clear cases + GBT with SHAP for the borderline band",
            rationale="Keeps the rules' instant explanations and the model's accuracy; the model runs on only "
                      "~15% of events, well inside the latency budget.")
        say(merger.name, f"synthesis {syn['id']} of {' + '.join(laya1['synthesize'])}")
        final = None
        for actor in (c1, c2, gpt, oc):
            if actor is not merger:
                final = await vote(actor, syn, "approve", 0.9, "Best of both.")
        laya1 = final.get("laya") or {}
        expect(laya1.get("outcome") == "DECIDED" and laya1.get("selected") == syn["id"],
               f"expected Laya DECIDED on the synthesis, got {laya1}")
        say("Laya", f"DECIDED {syn['id']} -> {laya1['decision_id']}")

        # ----------------------------------- debate 2: tied after synthesis -> ESCALATE
        print("\n9) debate 2 (tie -> escalate)")
        q2 = "Exactly-once or at-least-once alert delivery?"
        await gpt.call("debate_open", question=q2, mode="deliberative",
                       context="Raised in the kickoff chat as an open question.")
        ea = await c1.call("perspective_submit", question=q2, approach="Exactly-once via Kafka transactions",
                           rationale="No duplicate alerts for analysts.")
        eb = await gpt.call("perspective_submit", question=q2, approach="At-least-once with idempotent alert ids",
                            rationale="Simpler and faster; dedupe downstream.")
        es = await oc.call("synthesis_create", question=q2, source_proposal_ids=[ea["id"], eb["id"]],
                           approach="Exactly-once for high-value cards, at-least-once elsewhere",
                           rationale="Pay the transaction cost only where it matters.")
        for actor in (c2, gpt, oc):
            await vote(actor, ea, "approve", 1.0, "No duplicates is worth it.")
        for actor in (c1, c2, oc):
            await vote(actor, eb, "approve", 1.0, "Idempotent ids are enough.")
        await vote(c1, es, "reject", 1.0, "Two delivery modes double the test matrix.")
        await vote(c2, es, "reject", 1.0, "Too complex to operate.")
        tie = await vote(gpt, es, "reject", 1.0, "Two code paths are hard to test.")
        outcome = (tie.get("laya") or {}).get("outcome")
        expect(outcome == "ESCALATE", f"expected Laya ESCALATE, got {tie.get('laya')}")
        say("Laya", "ESCALATE (still tied after synthesis; the dashboard can tie-break)")

        # ---------------------------------------- debate 3: still collecting votes
        print("\n10) debate 3 (collecting votes)")
        q3 = "How long should per-card behavioural windows be?"
        await oc.call("debate_open", question=q3, mode="deliberative")
        wa = await gpt.call("perspective_submit", question=q3, approach="Sliding 24 h window",
                            rationale="Catches bursts of small test transactions.",
                            evidence=[{"source": "fraud ops playbook", "uri": "notes://playbook", "reliability": 0.5}])
        await oc.call("perspective_submit", question=q3, approach="Tumbling 7-day windows",
                      rationale="Captures weekly spending rhythm; cheaper state.")
        await vote(c1, wa, "approve", 0.7, "Matches the burst finding.")
        say("Laya", "NEED_VOTES (the dashboard human can vote)")

        for actor in (c1, c2, gpt, oc):  # everyone syncs at the start of their next turn (read receipts)
            await actor.sync()

        return {"project_id": pid, "admin_key": admin_key, "join_key": join_key, "tool_calls": hub.calls,
                "participants": {a.name: a.id for a in (c1, c2, gpt, oc)}}


def main() -> int:
    ap = argparse.ArgumentParser(description="Create a rich PALINOR demo project over MCP.")
    ap.add_argument("--url", default="http://localhost:8000/mcp", help="MCP endpoint, e.g. http://localhost:8000/mcp")
    ap.add_argument("--key", default="", help="server key (PALINOR_SERVER_KEY), sent as ?key= and Bearer")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every tool call")
    args = ap.parse_args()
    try:
        out = asyncio.run(seed(args.url, args.key, args.verbose))
    except SeedError as exc:
        print(f"seed failed: {exc}", file=sys.stderr)
        return 1
    base = args.url.rsplit("/mcp", 1)[0]
    print()
    print(f"project_id : {out['project_id']}")
    print(f"admin_key  : {out['admin_key']}")
    print(f"join_key   : {out['join_key']}")
    print(f"dashboard  : {base}/?key=<server key>   ({out['tool_calls']} tool calls)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
