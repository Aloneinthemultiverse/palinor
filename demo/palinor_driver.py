"""Claude-side driver for the PALINOR x OpenCode demo.

Talks to a PALINOR server over MCP (streamable HTTP), exactly like any remote MCP client would.

  python demo/palinor_driver.py setup                     # create the demo project + join as Claude (coordinator)
  python demo/palinor_driver.py call <tool> '<json args>' # call any PALINOR tool; Claude's token is injected
  python demo/palinor_driver.py tools                     # list the server's MCP tools

Environment: PALINOR_URL (default http://localhost:8000), PALINOR_SERVER_KEY (default: data/server_key.txt).
State (project_id, join_key, admin_key, Claude's token) is kept in demo/.driver_state.json - keep it private.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

ROOT = Path(__file__).resolve().parents[1]
STATE = Path(__file__).resolve().parent / ".driver_state.json"
BASE = os.getenv("PALINOR_URL", "http://localhost:8000").rstrip("/")
KEY = os.getenv("PALINOR_SERVER_KEY") or (ROOT / "data" / "server_key.txt").read_text().strip()
MCP_URL = f"{BASE}/mcp?key={KEY}"


def unwrap(result) -> object:
    """Tool result -> python object (structuredContent, maybe wrapped as {'result': ...}, else JSON text)."""
    sc = getattr(result, "structuredContent", None)
    if sc is not None:
        return sc["result"] if isinstance(sc, dict) and set(sc) == {"result"} else sc
    texts = [c.text for c in result.content if getattr(c, "type", "") == "text"]
    if not texts:
        return None
    try:
        return json.loads(texts[0])
    except json.JSONDecodeError:
        return texts[0]


async def call_many(calls: list[tuple[str, dict]]) -> list[object]:
    out = []
    async with AsyncExitStack() as stack:
        read, write, _ = await stack.enter_async_context(streamablehttp_client(MCP_URL))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        for name, args in calls:
            res = await session.call_tool(name, args)
            out.append(unwrap(res))
    return out


async def list_tools() -> list[str]:
    async with AsyncExitStack() as stack:
        read, write, _ = await stack.enter_async_context(streamablehttp_client(MCP_URL))
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        return [t.name for t in (await session.list_tools()).tools]


def load_state() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def dump(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    cmd = sys.argv[1]
    if cmd == "tools":
        names = asyncio.run(list_tools())
        print(len(names), "tools:", ", ".join(names))
        return
    if cmd == "setup":
        if STATE.exists() and "--force" not in sys.argv:
            print(f"{STATE} exists; pass --force to create a new project")
            dump(load_state())
            return
        (proj,) = asyncio.run(call_many([("palinor_create_project", {
            "name": "PALINOR demo: Claude x OpenCode",
            "goal": "Prove two-way collaboration between an independent Claude session and a real OpenCode "
                    "session through the live PALINOR MCP server: messages, a deliberative debate decided by "
                    "Laya, and repo notes ingested by the bridge.",
            "requirements": ["Only free models (opencode/*) on the OpenCode side",
                             "Every step is verified through PALINOR tool outputs"]})]))
        dump(proj)
        (me,) = asyncio.run(call_many([("palinor_join", {
            "project_id": proj["project_id"], "join_key": proj["join_key"], "name": "Claude",
            "platform": "claude", "role": "coordinator", "admin_key": proj["admin_key"],
            "capabilities": ["coordination", "code-review", "python"]})]))
        dump(me)
        state = {**proj, "claude": me, "since_seq": 0}
        STATE.write_text(json.dumps(state, indent=2))
        print(f"state saved to {STATE}")
        return
    if cmd == "call":
        st = load_state()
        tool = sys.argv[2]
        args = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {}
        if tool not in {"palinor_create_project", "palinor_join"} and "token" not in args:
            args["token"] = st["claude"]["token"]
        (res,) = asyncio.run(call_many([(tool, args)]))
        if tool == "palinor_sync" and isinstance(res, dict) and "latest_seq" in res:
            st["since_seq"] = res["latest_seq"]
            STATE.write_text(json.dumps(st, indent=2))
        dump(res)
        return
    print(__doc__)
    sys.exit(2)


if __name__ == "__main__":
    main()
