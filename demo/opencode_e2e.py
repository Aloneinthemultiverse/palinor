"""Real OpenCode in the PALINOR command pipeline + conflict protocol.

Driver (this script, via MCP) = Claude-1 (coordinator), Claude-2, Claude-3 and a SIMULATED ChatGPT-1.
OpenCode-1 = a real `opencode run` session in demo/opencode-workspace using a free model.

  python demo/opencode_e2e.py --key <SERVER_KEY> [--model opencode/big-pickle]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from contextlib import AsyncExitStack
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from scenario_conflict import Session  # noqa: E402  (same MCP helper)

from mcp import ClientSession  # noqa: E402
from mcp.client.streamable_http import streamablehttp_client  # noqa: E402

WS = Path(__file__).resolve().parent / "opencode-workspace"


def opencode(prompt: str, model: str, env: dict, label: str) -> str:
    print(f"\n--- OpenCode run: {label} ---")
    try:
        exe = shutil.which("opencode.cmd") or shutil.which("opencode")
        r = subprocess.run([exe, "run", "--auto", "-m", model, prompt], cwd=WS, env=env, capture_output=True,
                           text=True, encoding="utf-8", errors="replace", timeout=420, stdin=subprocess.DEVNULL)
        out = (r.stdout or "") + (r.stderr or "")[-400:]
    except subprocess.TimeoutExpired:
        out = "TIMEOUT"
    (WS.parent / "logs").mkdir(exist_ok=True)
    (WS.parent / "logs" / f"e2e_{label}.txt").write_text(out, encoding="utf-8")
    print(out.strip()[-700:])
    return out


async def main(base: str, key: str, model: str) -> None:
    env = {**os.environ, "PALINOR_MCP_URL": f"{base}/mcp?key={key}", "PYTHONIOENCODING": "utf-8",
           "PWD": str(WS)}  # OpenCode picks its project dir from PWD, not the process cwd
    (WS / ".palinor").mkdir(exist_ok=True)
    for f in (WS / ".palinor" / "session.json", WS / "hello_palinor.py"):
        f.unlink(missing_ok=True)
    async with AsyncExitStack() as stack:
        async def connect(label):
            r, w, _ = await stack.enter_async_context(streamablehttp_client(f"{base}/mcp?key={key}"))
            s = await stack.enter_async_context(ClientSession(r, w))
            await s.initialize()
            return Session(s, label)

        c1, c2, c3, gpt = [await connect(x) for x in ("claude-1", "claude-2", "claude-3", "chatgpt-sim")]
        proj = await c1.call("palinor_create_project", name="E2E: real OpenCode", goal="Ship a tiny feature safely")
        pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
        for s, plat, role, caps in ((c1, "claude", "coordinator", []), (c2, "claude", "worker", ["research"]),
                                    (c3, "claude", "worker", ["research"]), (gpt, "chatgpt", "worker", ["research"])):
            j = await s.call("palinor_join", project_id=pid, join_key=jk, name="", platform=plat, role=role,
                             capabilities=caps, admin_key=ak if role == "coordinator" else "")
            s.token, s.name = j["token"], j["name"]
        f = await c2.call("finding_submit", claim="A greeting script must print to stderr, not stdout",
                          evidence=[{"source": "style guide v1", "uri": "https://example.org/style", "reliability": 0.4}])
        print(f"project {pid} (admin_key {ak}) · finding {f['id']}")

        # 1) OpenCode joins
        opencode(f"Join the PALINOR project. Call the tool palinor_palinor_join with project_id='{pid}', join_key='{jk}', "
                 "name='', platform='opencode', role='worker', capabilities=['coding']. Then write the full JSON "
                 "result of palinor_palinor_join to the file .palinor/session.json (create it). Then call palinor_palinor_sync with "
                 "the token from that result. Finally reply with only your participant name.", model, env, "1_join")
        sess = json.loads((WS / ".palinor" / "session.json").read_text(encoding="utf-8"))
        oc_name, oc_id = sess["name"], sess["participant_id"]
        print(f"OpenCode joined as {oc_name} ({oc_id})")

        # 2) Claude-1 commands
        plan = await c1.call("plan_submit", goal="Add a greeting script", tasks=[
            {"key": "code", "title": "Create hello_palinor.py", "capability": "coding", "assignee": oc_name,
             "description": "Create hello_palinor.py that prints 'hello from PALINOR'",
             "acceptance": "file exists and prints the greeting"}])
        print(f"Claude-1 plan {plan['plan_id']} dispatched to {[d['participant_id'] for d in plan['dispatched']]}")
        opencode("Read .palinor/session.json to get your PALINOR token. Call palinor_palinor_sync with that token. You have a "
                 "START_TASK command. Call palinor_command_update with status ACCEPTED for it. Then create the file "
                 "hello_palinor.py containing: print('hello from PALINOR'). Then call palinor_command_update with status "
                 "COMPLETED, note='created hello_palinor.py printing the greeting', result_ref='hello_palinor.py'. "
                 "Do not use bash. Reply with one short line.", model, env, "2_command")
        st = (await c1.call("pipeline_status"))["plans"][0]
        print(f"pipeline: {st['progress']} · task status {st['tasks'][0]['status']} · file exists: "
              f"{(WS / 'hello_palinor.py').exists()}")

        # 3) ChatGPT-1 (simulated) raises a conflict; Claude-1 consults OpenCode
        for s in (c1, c2, c3, gpt):
            await s.sync()
        cf = await gpt.call("conflict_raise", about=[f["id"]], claim="Greetings are normal output: stdout is correct",
                            evidence=[{"source": "POSIX conventions", "uri": "https://example.org/posix", "reliability": 0.8}])
        await c1.call("conflict_consult", conflict_id=cf["conflict_id"], ask=[oc_name],
                      question="You wrote hello_palinor.py: should a greeting go to stdout or stderr?")
        opencode("Read .palinor/session.json to get your PALINOR token. Call palinor_palinor_sync with that token. A coordinator "
                 "asked your opinion on a conflict (see your_actions and the question message with its context). Look at "
                 "hello_palinor.py if useful. Answer ONCE with palinor_conflict_opinion: conflict_id from your_actions, position "
                 "one of keep|accept_challenge|merge|unsure (keep = the challenged finding stands, accept_challenge = the "
                 "challenger is right), a short rationale, and refs = a list containing the challenged finding id exactly "
                 "as it appears in the message context. Do not use bash. Reply with one short line.", model, env, "3_opinion")
        cs = (await c1.call("conflict_status", conflict_id=cf["conflict_id"]))["conflicts"][0]
        print(f"opinions: {cs['opinions']} · pending {cs['pending']}")
        if cs["opinions"]:
            pos = cs["opinions"][0]["position"]
            res = await c1.call("conflict_resolve", conflict_id=cf["conflict_id"],
                                resolution=pos if pos in ("keep", "accept_challenge") else "accept_challenge",
                                rationale=f"following {oc_name}'s grounded opinion")
            print(f"resolved: {res['resolution']} · audit {res['evidence_audit']} · notified {res['notified']}")
        print(f"Claude-3 (uninvolved) unread messages: {len((await c3.sync())['messages'])}")
        print(f"\nDashboard: project_id={pid} admin_key={ak}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--key", required=True)
    ap.add_argument("--model", default="opencode/big-pickle")
    a = ap.parse_args()
    asyncio.run(main(a.base, a.key, a.model))
