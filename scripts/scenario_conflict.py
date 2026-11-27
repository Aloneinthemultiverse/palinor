"""Live end-to-end scenario over real MCP/HTTP: one Claude commands, a worker raises a conflict,
the coordinator consults another worker, then resolves. Five independent sessions.

  python scripts/scenario_conflict.py --url https://<host>/mcp --key <SERVER_KEY>
"""
from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import AsyncExitStack

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


class Session:
    def __init__(self, s: ClientSession, label: str):
        self.s, self.label, self.token, self.name, self.seq = s, label, "", "", 0

    async def call(self, tool: str, **kw):
        if self.token and "token" not in kw:
            kw["token"] = self.token
        r = await self.s.call_tool(tool, kw)
        data = r.structuredContent if r.structuredContent is not None else json.loads(r.content[0].text)
        data = data.get("result", data) if isinstance(data, dict) else data
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"{self.label}.{tool}: {data['message']}")
        return data

    async def sync(self):
        out = await self.call("palinor_sync", since_seq=self.seq)
        self.seq = out["latest_seq"]
        return out


def say(who: str, text: str) -> None:
    print(f"  {who:<11} {text}")


async def main(url: str, key: str) -> None:
    endpoint = f"{url}?key={key}"
    async with AsyncExitStack() as stack:
        async def connect(label):
            r, w, _ = await stack.enter_async_context(streamablehttp_client(endpoint))
            s = await stack.enter_async_context(ClientSession(r, w))
            await s.initialize()
            return Session(s, label)

        c1, c2, c3, gpt, oc = [await connect(x) for x in ("claude-a", "claude-b", "claude-c", "chatgpt", "opencode")]
        proj = await c1.call("palinor_create_project", name="Scenario: commander + conflict",
                             goal="Choose and build a cloud-removal model", requirements=["runs on 1 GPU"])
        pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
        for sess, plat, role, caps in ((c1, "claude", "coordinator", []), (c2, "claude", "worker", ["research"]),
                                       (c3, "claude", "worker", ["research"]), (gpt, "chatgpt", "worker", ["research"]),
                                       (oc, "opencode", "worker", ["coding"])):
            j = await sess.call("palinor_join", project_id=pid, join_key=jk, name="", platform=plat, role=role,
                                capabilities=caps, admin_key=ak if role == "coordinator" else "")
            sess.token, sess.name = j["token"], j["name"]
        print(f"project {pid} · members: {', '.join(s.name for s in (c1, c2, c3, gpt, oc))}")

        print("\n1) Claude-2 records a finding; Claude-1 commands the work")
        f = await c2.call("finding_submit", claim="Diffusion beats GAN on SEN12MS-CR",
                          evidence=[{"source": "paper A", "uri": "https://example.org/a", "reliability": 0.6}])
        plan = await c1.call("plan_submit", goal="Pick and build the model", tasks=[
            {"key": "verify", "title": "Verify model claims on cloudy tiles", "capability": "research",
             "assignee": gpt.name, "acceptance": "a benchmark-backed recommendation"},
            {"key": "build", "title": "Implement the chosen model", "capability": "coding", "depends_on": ["verify"]}])
        say(c1.name, f"plan {plan['plan_id']}: dispatched {[d['participant_id'] for d in plan['dispatched']]}")

        print("\n2) ChatGPT-1 works, sees the conflict in the graph and reacts to its commander")
        s = await gpt.sync()
        cmd = s["commands"][0]
        await gpt.call("command_update", command_id=cmd["command_id"], status="ACCEPTED")
        ctx = await gpt.call("context_get")
        target = next(x for x in ctx["findings"] if x["claim"].startswith("Diffusion"))
        try:
            await gpt.call("conflict_raise", about=["F-made-up-id"], claim="hallucinated reference")
        except RuntimeError as exc:
            say(gpt.name, f"invented id rejected ✓ ({str(exc)[:70]}…)")
        cf = await gpt.call("conflict_raise", about=[target["finding_id"]], task_id=cmd["task_id"],
                            claim="On cloudy tiles GAN wins: diffusion hallucinates texture under thick cloud",
                            evidence=[{"source": "benchmark B", "uri": "https://example.org/b", "reliability": 0.9}])
        say(gpt.name, f"raised {cf['conflict_id']} → resolver {cf['resolver']}")

        print("\n3) Only Claude-1 hears about it; it consults OpenCode-1")
        for sess in (c2, c3, oc):
            n = len((await sess.sync())["messages"])
            say(sess.name, f"unrelated inbox messages: {n}")
        s1 = await c1.sync()
        alert = next(m for m in s1["messages"] if m["kind"] == "alert")
        say(c1.name, f"got: {alert['subject']} (untrusted={alert['untrusted']})")
        await c1.call("conflict_consult", conflict_id=cf["conflict_id"], ask=[oc.name],
                      question="Which model handles thick cloud better?")

        print("\n4) OpenCode-1 replies once, grounded in the graph")
        so = await oc.sync()
        q = next(m for m in so["messages"] if m["kind"] == "question")
        say(oc.name, f"asked: {q['subject']} · facts attached: {[c['id'] for c in q['context']['challenged']]}")
        op = await oc.call("conflict_opinion", conflict_id=cf["conflict_id"], position="accept_challenge",
                           rationale="Our tile tests reproduce benchmark B", refs=[target["finding_id"]],
                           evidence=[{"source": "tile test", "uri": "https://example.org/t", "reliability": 0.7}])
        say(oc.name, f"opinion recorded · still pending: {op['pending']}")

        print("\n5) Claude-1 chooses and solves it")
        res = await c1.call("conflict_resolve", conflict_id=cf["conflict_id"], resolution="accept_challenge",
                            rationale="two independent sources favour GAN on thick cloud")
        say(c1.name, f"{res['resolution']} · effects {res['effects']} · audit {res['evidence_audit']} · "
                     f"notified {res['notified']}")
        say(c3.name, f"messages after resolution: {len((await c3.sync())['messages'])} (not involved)")

        print("\n6) Pipeline continues: ChatGPT-1 completes, the build goes to OpenCode-1 with the result")
        await gpt.call("command_update", command_id=cmd["command_id"], status="COMPLETED",
                       note="Recommend GAN for thick cloud (conflict " + cf["conflict_id"] + " resolved)")
        so = await oc.sync()
        build = next(c for c in so["commands"] if "Implement" in c["instruction"])
        say(oc.name, "received build command carrying: " + build["instruction"].split("Inputs from upstream tasks:")[1]
            .strip().split("\n")[0][:110])
        st = await c1.call("pipeline_status")
        say(c1.name, f"pipeline {st['plans'][0]['progress']} done")
        dup1 = await gpt.call("message_send", to=c1.name, body="verify done, see summary")
        dup2 = await gpt.call("message_send", to=c1.name, body="verify done, see summary")
        say(gpt.name, f"duplicate message suppressed: {dup2.get('duplicate', False)} (same id: "
                      f"{dup1['message_id'] == dup2['message_id']})")
        print(f"\nDashboard: project_id={pid} admin_key={ak}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--key", required=True)
    a = ap.parse_args()
    asyncio.run(main(a.url, a.key))
