"""Tiny PALINOR MCP client: python pal.py <tool> '<json args>'  (token auto-loaded from me.json)"""
import asyncio, json, sys, os
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
D = os.path.dirname(os.path.abspath(__file__)); ME = os.path.join(D, os.environ.get("PAL_ME", "me.json"))
async def main():
    tool, args = sys.argv[1], json.loads(sys.argv[2] if len(sys.argv) > 2 else "{}")
    key = open(r"C:\Users\Sujit Narrayan M\Big-Data\data\server_key.txt").read().strip()
    if os.path.exists(ME) and "token" not in args and tool != "palinor_join":
        args["token"] = json.load(open(ME))["token"]
    async with streamablehttp_client(f"http://localhost:{os.environ.get('PALINOR_PORT', '8010')}/mcp?key={key}") as (r, w, _):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            parts = [getattr(c, "text", "") for c in res.content]
            txt = parts[0] if len(parts) == 1 else "[" + ",".join(parts) + "]"  # list results arrive one item per block
            if tool in ("palinor_join", "palinor_create_project") and not res.isError and tool == "palinor_join":
                open(ME, "w").write(txt)
            print(txt)
asyncio.run(main())
