"""PALINOR x real OpenCode: command pipeline + conflict protocol + repo bridge, verified end to end.

Who is who (nothing is hidden):
  Claude-1   coordinator  - this driver script (scripted decisions, run from Claude Code)
  Claude-2   worker       - SIMULATED by this script (adds one finding with evidence)
  Claude-3   worker       - SIMULATED by this script (bystander: must never hear about the conflict)
  ChatGPT-1  worker       - SIMULATED by this script (research task, raises the conflict)
  OpenCode-1 worker       - a REAL `opencode run` session (free opencode/* model) in demo/opencode-workspace

Run (live server on :8000, key from data/server_key.txt):
  python demo/opencode_scenario.py
  python demo/opencode_scenario.py --url http://localhost:8765 --key <KEY> --models opencode/big-pickle

Every OpenCode turn is a separate `opencode run --format json` call with stdin closed (an open stdin pipe makes
`opencode run` wait for EOF forever). Raw OpenCode event logs go to demo/logs/oc_turn*.jsonl with secrets redacted.
Secrets (admin_key, join_key, tokens) are kept in demo/.oc_scenario_state.json - keep that file private.
"""
from __future__ import annotations

import argparse
import ast
import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from contextlib import AsyncExitStack
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "demo"
LOGS = DEMO / "logs"
PROMPTS = DEMO / "prompts"
STATE = DEMO / ".oc_scenario_state.json"
FREE_MODELS = ["opencode/big-pickle", "opencode/nemotron-3-ultra-free", "opencode/ling-3.1-flash-free"]
GENERATED = [".palinor", "cloud_cover.py", "test_cloud_cover.py", "PALINOR.md", ".palinor-bridge.json",
             "palinor-inbox", "__pycache__", ".pytest_cache"]

SECRETS: set[str] = set()
CHECKS: list[tuple[str, bool, str]] = []
REPORT: list[str] = []


def redact(text: str) -> str:
    for s in sorted(SECRETS, key=len, reverse=True):
        if s and len(s) >= 8:
            text = text.replace(s, "***")
    return text


def say(line: str = "") -> None:
    line = redact(line)
    REPORT.append(line)
    print(line, flush=True)


def check(name: str, ok: bool, detail: str = "") -> bool:
    CHECKS.append((name, bool(ok), redact(str(detail))))
    say(f"    [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))
    return bool(ok)


# --------------------------------------------------------------------------- PALINOR MCP client
class Mcp:
    """One streamable-HTTP MCP connection. PALINOR is stateless over HTTP: identity is the token in each call."""

    def __init__(self, url: str):
        self.url, self.stack, self.s = url, AsyncExitStack(), None

    async def __aenter__(self):
        r, w, _ = await self.stack.enter_async_context(streamablehttp_client(self.url))
        self.s = await self.stack.enter_async_context(ClientSession(r, w))
        await self.s.initialize()
        return self

    async def __aexit__(self, *exc):
        await self.stack.aclose()

    async def call(self, tool: str, **kw):
        r = await self.s.call_tool(tool, kw)
        data = r.structuredContent if r.structuredContent is not None else json.loads(r.content[0].text)
        return data.get("result", data) if isinstance(data, dict) and set(data) == {"result"} else data


class Member:
    def __init__(self, mcp: Mcp, label: str, simulated: bool):
        self.mcp, self.label, self.simulated = mcp, label, simulated
        self.token = self.name = self.id = ""
        self.seq, self.received = 0, []

    async def call(self, tool: str, **kw):
        if self.token and "token" not in kw:
            kw["token"] = self.token
        data = await self.mcp.call(tool, **kw)
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"{self.name or self.label}.{tool}: {data['error']}: {data['message']}")
        return data

    async def sync(self) -> dict:
        out = await self.call("palinor_sync", since_seq=self.seq)
        self.seq = out["latest_seq"]
        self.received += out["messages"]
        return out

    @property
    def tag(self) -> str:
        return f"{self.name}{' (simulated)' if self.simulated else ''}"


# --------------------------------------------------------------------------- OpenCode runner
def opencode_binary() -> str:
    """The real .exe: the npm .cmd shim goes through cmd.exe, which mangles multi-line prompts."""
    if os.getenv("OPENCODE_BIN"):
        return os.environ["OPENCODE_BIN"]
    found = shutil.which("opencode")
    if not found:
        raise SystemExit("opencode CLI not found on PATH")
    p = Path(found)
    if p.suffix.lower() in {".cmd", ".ps1", ""}:
        exe = p.parent / "node_modules" / "opencode-ai" / "bin" / "opencode.exe"
        if exe.exists():
            return str(exe)
    return found


def summarize_events(raw: str) -> dict:
    tools, texts, errors, session = [], [], [], None
    for line in raw.splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        session = session or e.get("sessionID")
        part = e.get("part") or {}
        if e.get("type") == "tool_use":
            st = part.get("state") or {}
            tools.append({"tool": part.get("tool"), "status": st.get("status"), "input": st.get("input"),
                          "output": str(st.get("output") or st.get("error") or "")[:400]})
        elif e.get("type") == "text" and part.get("text"):
            texts.append(part["text"])
        elif e.get("type") == "error":
            err = e.get("error") or {}
            errors.append(f"{err.get('name')}: {(err.get('data') or {}).get('message') or err}")
    return {"session": session, "tools": tools, "text": "\n".join(texts).strip(), "errors": errors}


async def run_opencode(turn: str, prompt: str, model: str, ws: Path, mcp_url: str, timeout_s: int,
                       session_id: str | None, keepalive) -> dict:
    args = [opencode_binary(), "run", "-m", model, "--format", "json", "--print-logs", "--log-level", "INFO",
            "--title", f"PALINOR {turn}"]
    if session_id:
        args += ["--session", session_id]
    args.append(prompt)
    env = {**os.environ, "PALINOR_MCP_URL": mcp_url}
    t0 = time.time()
    proc = await asyncio.create_subprocess_exec(*args, cwd=str(ws), env=env, stdin=subprocess.DEVNULL,
                                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ka = asyncio.create_task(keepalive())
    timed_out = False
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout_s)
    except (asyncio.TimeoutError, TimeoutError):
        timed_out = True
        proc.kill()
        out, err = await proc.communicate()
    finally:
        ka.cancel()
    raw, log = out.decode("utf-8", "replace"), err.decode("utf-8", "replace")
    stem = f"oc_{turn}_{model.split('/')[-1]}"
    (LOGS / f"{stem}.jsonl").write_text(redact(raw), encoding="utf-8")
    (LOGS / f"{stem}.err").write_text(redact(log), encoding="utf-8")
    res = summarize_events(raw)
    res.update(model=model, seconds=round(time.time() - t0, 1), exit=proc.returncode, timed_out=timed_out,
               log=f"demo/logs/{stem}.jsonl")
    return res


def show_run(res: dict) -> None:
    say(f"    opencode run -m {res['model']}: {res['seconds']}s, exit {res['exit']}"
        + (", TIMED OUT" if res["timed_out"] else "") + f", log {res['log']}")
    for t in res["tools"]:
        args = {k: v for k, v in (t["input"] or {}).items() if k not in {"token", "join_key", "content"}}
        say(f"      tool {t['tool']} [{t['status']}] {json.dumps(args, ensure_ascii=False)[:230]}")
    for e in res["errors"]:
        say(f"      ERROR {e}")
    if res["text"]:
        say("      OpenCode says: " + res["text"].replace("\n", "\n        ")[:1500])


async def opencode_turn(turn: str, prompt: str, a, ws: Path, mcp_url: str, session_id: str | None,
                        keepalive, done) -> tuple[dict | None, str | None]:
    """Run one OpenCode turn (continuing the same OpenCode session, like a user's next message); if the turn's
    server-side check `done()` fails, try the next free model."""
    for model in a.models:
        res = await run_opencode(turn, prompt, model, ws, mcp_url, a.turn_timeout, session_id, keepalive)
        session_id = session_id or res["session"]
        show_run(res)
        if await done():
            return res, session_id
        say(f"    turn '{turn}' not completed with {model}")
    return None, session_id


# --------------------------------------------------------------------------- helpers
def admin_get(base: str, key: str, admin_key: str, path: str) -> dict:
    req = urllib.request.Request(f"{base}{path}", headers={"Authorization": f"Bearer {key}",
                                                         "X-Palinor-Admin": admin_key})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def safe_python(path: Path, allowed_imports: set[str]) -> tuple[bool, str]:
    """Static gate before the driver executes model-written code: only whitelisted imports, no I/O builtins."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as exc:
        return False, f"cannot parse: {exc}"
    banned = {"exec", "eval", "open", "compile", "__import__", "input", "globals", "locals", "getattr",
              "setattr", "delattr", "breakpoint", "memoryview"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            bad = [n.name for n in node.names if n.name.split(".")[0] not in allowed_imports]
        elif isinstance(node, ast.ImportFrom):
            bad = [] if (node.module or "").split(".")[0] in allowed_imports else [node.module]
        elif isinstance(node, ast.Name) and node.id in banned:
            bad = [node.id]
        elif isinstance(node, ast.Attribute) and node.attr.startswith("__") and node.attr not in {"__name__"}:
            bad = [node.attr]
        else:
            continue
        if bad:
            return False, f"not allowed: {bad}"
    return True, "ok"


def reset_workspace(ws: Path) -> None:
    for name in GENERATED:
        p = ws / name
        if p.is_symlink() or p.is_file():
            p.unlink()
        elif p.is_dir():
            shutil.rmtree(p)


def read_session(ws: Path) -> dict:
    try:
        return json.loads((ws / ".palinor" / "session.json").read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}


# --------------------------------------------------------------------------- scenario
async def main(a) -> int:
    base = a.url.rstrip("/")
    key = a.key or (ROOT / "data" / "server_key.txt").read_text().strip()
    SECRETS.add(key)
    mcp_url = f"{base}/mcp?key={key}"
    ws = Path(a.workspace).resolve()
    LOGS.mkdir(exist_ok=True)
    reset_workspace(ws)
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    say(f"PALINOR x OpenCode scenario - {started} - server {base} - workspace {ws}")
    say(f"OpenCode {subprocess.run([opencode_binary(), '--version'], capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout.strip()}"
        f", models to try: {', '.join(a.models)}")

    async with Mcp(mcp_url) as mcp:
        c1, c2, c3, gpt = (Member(mcp, "claude-1", False), Member(mcp, "claude-2", True),
                           Member(mcp, "claude-3", True), Member(mcp, "chatgpt-1", True))
        sims = [c1, c2, c3, gpt]

        async def keepalive():
            """While OpenCode works, the scripted sessions stay online (PALINOR marks silent ones offline
            after the heartbeat timeout and re-dispatches their work)."""
            while True:
                await asyncio.sleep(45)
                for m in sims:
                    try:
                        await m.call("heartbeat")
                    except Exception as exc:  # keep going; a missed heartbeat is not fatal
                        say(f"    heartbeat {m.name} failed: {exc}")

        # ---------------------------------------------------------------- 1. project + sessions
        say("\n1) Claude-1 creates the project; Claude-2, Claude-3 and ChatGPT-1 join (simulated sessions)")
        proj = await c1.call("palinor_create_project", name="PALINOR x OpenCode: cloud-cover utility",
                             goal="Build a small cloud-cover utility for satellite tiles and settle disagreements "
                                  "about it through the PALINOR conflict protocol.",
                             requirements=["OpenCode uses only free opencode/* models",
                                           "every step is verified through PALINOR tool outputs"])
        pid, jk, ak = proj["project_id"], proj["join_key"], proj["admin_key"]
        SECRETS.update({jk, ak})
        for m, plat, role, caps in ((c1, "claude", "coordinator", ["coordination", "review"]),
                                    (c2, "claude", "worker", ["research"]),
                                    (c3, "claude", "worker", ["review"]),
                                    (gpt, "chatgpt", "worker", ["research"])):
            j = await m.call("palinor_join", project_id=pid, join_key=jk, name="", platform=plat, role=role,
                             capabilities=caps, admin_key=ak if role == "coordinator" else "")
            m.token, m.name, m.id = j["token"], j["name"], j["participant_id"]
            SECRETS.add(m.token)
            say(f"    {m.tag:<24} {m.id:<28} role={role} capabilities={caps}")
        STATE.write_text(json.dumps({"url": base, "project_id": pid, "join_key": jk, "admin_key": ak,
                                     "members": {m.name: {"id": m.id, "token": m.token} for m in sims}},
                                    indent=2), encoding="utf-8")
        check("several Claude sessions are numbered", [c1.name, c2.name, c3.name] == ["Claude-1", "Claude-2", "Claude-3"]
              and gpt.name == "ChatGPT-1", f"{c1.name}, {c2.name}, {c3.name}, {gpt.name}")
        finding = await c2.call("finding_submit",
                                claim="Cloud fraction of a tile = cloudy pixels / ALL pixels, including no-data (255) pixels",
                                summary="Simulated Claude-2 note: convention used by an early prototype.",
                                evidence=[{"source": "simulated Claude-2 prototype note", "uri": "",
                                           "excerpt": "prototype divided by width*height", "reliability": 0.4}],
                                confidence=0.5)
        fid = finding["id"]
        say(f"    {c2.tag} finding {fid}: {finding['claim']}")

        # ---------------------------------------------------------------- 2. OpenCode joins
        say("\n2) REAL OpenCode turn 1: join (name='' -> OpenCode-N), save .palinor/session.json, palinor_sync")
        prompt1 = (PROMPTS / "oc_turn1_join.md").read_text(encoding="utf-8").replace("{PROJECT_ID}", pid) \
            .replace("{JOIN_KEY}", jk)

        async def oc_member() -> dict | None:
            ps = await c1.call("participants_list")
            return next((p for p in ps if p.get("platform") == "opencode"), None)

        async def joined() -> bool:
            s = read_session(ws)
            if s.get("token"):
                SECRETS.add(s["token"])
            return bool(await oc_member()) and bool(s.get("participant_id")) and s.get("latest_seq", 0) > 0

        async def no_participant_yet() -> bool:
            return not await oc_member()

        res, oc_session = None, None
        for model in a.models:  # retry turn 1 only while nobody joined yet: a re-join would create OpenCode-2
            res = await run_opencode("turn1_join", prompt1, model, ws, mcp_url, a.turn_timeout, None, keepalive)
            show_run(res)
            oc_session = res["session"]
            if await joined():
                break
            if not await no_participant_yet():
                say("    OpenCode joined but did not finish saving its session; not re-joining (would create OpenCode-2)")
                break
            res = None
        ocp = await oc_member()
        sess = read_session(ws)
        if sess.get("token"):
            SECRETS.add(sess["token"])
        if not check("OpenCode joined as OpenCode-1 (platform opencode, capabilities [coding])",
                     bool(ocp) and ocp["name"] == "OpenCode-1" and ocp.get("capabilities") == ["coding"],
                     json.dumps({k: ocp.get(k) for k in ("participant_id", "name", "platform", "role", "capabilities")})
                     if ocp else "no OpenCode participant"):
            return finish(pid, ak, base)
        check(".palinor/session.json written by OpenCode", sess.get("participant_id") == ocp["participant_id"]
              and sess.get("project_id") == pid and bool(sess.get("token")) and sess.get("latest_seq", 0) > 0,
              f"participant_id={sess.get('participant_id')} latest_seq={sess.get('latest_seq')} token={'set' if sess.get('token') else 'missing'}")
        oc_id = ocp["participant_id"]
        idname = {m.id: m.name for m in sims} | {oc_id: ocp["name"]}

        def names_of(pid_: str | None) -> str:
            return idname.get(pid_ or "", pid_ or "")

        # ---------------------------------------------------------------- 3. plan
        say("\n3) Claude-1 divides the work: plan_submit (research -> ChatGPT-1, coding -> OpenCode-1)")
        plan = await c1.call("plan_submit", goal="Ship a verified cloud_fraction() utility", tasks=[
            {"key": "research", "title": "Check how cloud fraction must treat no-data pixels",
             "capability": "research", "assignee": gpt.name,
             "description": "Compare the conventions in the graph with the coding spec; raise a conflict if they disagree.",
             "acceptance": "a recommendation backed by evidence"},
            {"key": "code", "title": "Implement cloud_fraction() in cloud_cover.py", "capability": "coding",
             "assignee": "OpenCode-1",
             "description": "Create cloud_cover.py in the workspace root with a function cloud_fraction(mask) -> float. "
                            "mask is a list of rows, each a list of ints: 1 = cloud, 0 = clear, 255 = no-data. "
                            "Return cloudy pixels divided by valid pixels, where valid pixels are the 0s and 1s "
                            "(no-data pixels are excluded from the denominator). Return 0.0 when there is no valid "
                            "pixel. Raise ValueError for any other value. Use only the standard library. Also create "
                            "test_cloud_cover.py with pytest tests for these cases.",
             "acceptance": "cloud_fraction([[1, 0], [255, 1]]) == 2/3; cloud_fraction([[255, 255]]) == 0.0; "
                           "cloud_fraction([[7]]) raises ValueError"}])
        tasks = plan["tasks"]
        routed = {d["task_id"]: d["participant_id"] for d in plan["dispatched"]}
        say(f"    plan {plan['plan_id']}: research={tasks['research']} code={tasks['code']}")
        check("plan dispatched research -> ChatGPT-1 and coding -> OpenCode-1",
              routed.get(tasks["research"]) == gpt.id and routed.get(tasks["code"]) == oc_id,
              ", ".join(f"{t}->{p}" for t, p in routed.items()))
        g = await gpt.sync()
        gcmd = next(c for c in g["commands"] if c["task_id"] == tasks["research"])
        await gpt.call("command_update", command_id=gcmd["command_id"], status="ACCEPTED")
        say(f"    {gpt.tag} accepted {gcmd['command_id']} (research)")
        for m in (c1, c2, c3):
            await m.sync()

        # ---------------------------------------------------------------- 4. OpenCode codes
        say("\n4) REAL OpenCode turn 2: palinor_sync -> accept its command -> write code -> command_update COMPLETED")
        oc_cmd_id = next(d["command_id"] for d in plan["dispatched"] if d["task_id"] == tasks["code"])

        async def coded() -> bool:
            return (await c1.call("command_status", command_id=oc_cmd_id)).get("status") == "COMPLETED"

        prompt2 = (PROMPTS / "oc_turn2_command.md").read_text(encoding="utf-8")
        res2, oc_session = await opencode_turn("turn2_command", prompt2, a, ws, mcp_url, oc_session, keepalive, coded)
        cst = await c1.call("command_status", command_id=oc_cmd_id)
        hist = cst.get("history") or []
        say(f"    command {oc_cmd_id} lifecycle: " + " -> ".join(
            h["status"] + (f"({names_of(h.get('by'))})" if h.get("by") else "") for h in hist))
        by_oc = {h["status"] for h in hist if h.get("by") == oc_id}
        check("OpenCode-1 drove its command DELIVERED -> ACCEPTED -> RUNNING -> COMPLETED",
              cst.get("status") == "COMPLETED" and {"DELIVERED", "ACCEPTED", "RUNNING", "COMPLETED"} <= by_oc,
              f"status={cst.get('status')}; note: {(cst.get('last_note') or '')[:160]}; result_ref={cst.get('result_ref')}")
        ps = await c1.call("pipeline_status", plan_id=plan["plan_id"])
        row = next(r for r in ps["plans"][0]["tasks"] if r["task_id"] == tasks["code"])
        check("pipeline: coding task COMPLETED by OpenCode-1 with a summary",
              row["status"] == "COMPLETED" and row["completed_by"] == oc_id and bool(row["result_summary"]),
              f"{ps['plans'][0]['progress']} done; summary: {row['result_summary'][:160]}")
        code_file, test_file = ws / "cloud_cover.py", ws / "test_cloud_cover.py"
        check("cloud_cover.py exists in the workspace", code_file.exists(),
              f"{code_file.stat().st_size} bytes" if code_file.exists() else "missing")
        if code_file.exists():
            ok, why = safe_python(code_file, {"typing", "__future__", "math", "collections", "numbers"})
            check("static gate on model-written code (imports/builtins)", ok, why)
            if ok:
                probe = ("import sys; sys.path.insert(0, sys.argv[1]); from cloud_cover import cloud_fraction as f\n"
                         "assert abs(f([[1, 0], [255, 1]]) - 2/3) < 1e-9, f([[1, 0], [255, 1]])\n"
                         "assert f([[255, 255]]) == 0.0\nassert f([[1, 1], [255, 255]]) == 1.0\n"
                         "try:\n    f([[7]])\n    raise SystemExit('no ValueError')\nexcept ValueError:\n    pass\nprint('ok')")
                r = subprocess.run([sys.executable, "-I", "-B", "-c", probe, str(ws)], capture_output=True, text=True,
                                   timeout=60, stdin=subprocess.DEVNULL)
                check("driver re-runs the acceptance cases against OpenCode's code", r.stdout.strip() == "ok",
                      (r.stdout + r.stderr).strip()[-300:])
        if test_file.exists():
            ok, why = safe_python(test_file, {"pytest", "cloud_cover", "math", "typing", "__future__"})
            if ok:
                r = subprocess.run([sys.executable, "-B", "-m", "pytest", "-q", "-p", "no:cacheprovider",
                                    str(test_file)], cwd=str(ws), capture_output=True, text=True, timeout=120,
                                   stdin=subprocess.DEVNULL)
                check("OpenCode's own test_cloud_cover.py passes", r.returncode == 0,
                      (r.stdout.strip().splitlines() or ["no output"])[-1])
            else:
                check("static gate on test_cloud_cover.py", False, why)
        s1 = await c1.sync()
        prog = [m for m in s1["messages"] if m["sender"] == "orchestrator" and tasks["code"] in (m.get("refs") or [])]
        check("Claude-1 (plan owner) got the hand-off progress message", len(prog) == 1,
              prog[0]["subject"] if prog else "none")

        # ---------------------------------------------------------------- 5. conflict
        say("\n5) ChatGPT-1 (simulated) raises a conflict about Claude-2's finding; Claude-1 consults OpenCode-1")
        ctx = await gpt.call("context_get")
        target = next(f for f in ctx["findings"] if f["finding_id"] == fid)
        try:
            await gpt.call("conflict_raise", about=["F-invented-0000"], claim="hallucinated reference test")
            check("an invented node id is rejected", False, "accepted!")
        except RuntimeError as exc:
            check("an invented node id is rejected", "unknown or non-citable" in str(exc), str(exc)[:110])
        cf = await gpt.call("conflict_raise", about=[target["finding_id"]], task_id=tasks["research"], severity="normal",
                            claim="No-data (255) pixels must be excluded from the denominator: counting them understates "
                                  "cloud cover on partial tiles, and it contradicts the coding spec of "
                                  f"{tasks['code']}.",
                            evidence=[{"source": "worked example (simulated ChatGPT-1)", "uri": "",
                                       "excerpt": "2 cloudy + 2 no-data pixels: counting no-data gives 0.5, the "
                                                  "observed area is 100% cloud", "reliability": 0.7}])
        cid = cf["conflict_id"]
        say(f"    {gpt.tag} raised {cid} about {fid} -> resolver {cf['resolver']}")
        check("the conflict goes to the plan's coordinator only", cf["resolver"] == c1.id, cf["resolver"])
        s1 = await c1.sync()
        alerts = [m for m in s1["messages"] if cid in (m.get("refs") or [])]
        check("Claude-1 received exactly one alert", len(alerts) == 1 and alerts[0]["kind"] == "alert",
              alerts[0]["subject"] if alerts else "none")
        for m in (c2, c3):
            n = [x for x in (await m.sync())["messages"] if cid in (x.get("refs") or [])]
            check(f"{m.name} heard nothing about {cid} yet", not n, f"{len(n)} message(s)")
        cons = await c1.call("conflict_consult", conflict_id=cid, ask=["OpenCode-1"],
                             question="You implemented cloud_fraction (task " + tasks["code"] + "). How does your code "
                                      "treat no-data pixels, and which side does that support?")
        check("Claude-1 consulted exactly OpenCode-1", cons["consulted"] == [oc_id], str(cons["consulted"]))

        # ---------------------------------------------------------------- 6. OpenCode opinion
        say("\n6) REAL OpenCode turn 3: palinor_sync -> conflict_opinion grounded in real node ids")

        async def opined() -> bool:
            st = (await c1.call("conflict_status", conflict_id=cid))["conflicts"][0]
            return any(o["by"] == oc_id for o in st["opinions"])

        prompt3 = (PROMPTS / "oc_turn3_opinion.md").read_text(encoding="utf-8")
        res3, oc_session = await opencode_turn("turn3_opinion", prompt3, a, ws, mcp_url, oc_session, keepalive, opined)
        st = (await c1.call("conflict_status", conflict_id=cid))["conflicts"][0]
        op = next((o for o in st["opinions"] if o["by"] == oc_id), None)
        check("OpenCode-1 answered the consultation", bool(op),
              f"{op['position']} (confidence {op['confidence']}): {op['rationale'][:200]}" if op else "no opinion")
        if op:
            known = {cid, fid, *tasks.values(), plan["plan_id"], *[x["id"] for x in st["package"]["challenged"]]}
            check("every ref OpenCode-1 cited is a real node of this conflict/plan",
                  bool(op["refs"]) and set(op["refs"]) <= known, f"refs={op['refs']}")
        s1 = await c1.sync()
        answers = [m for m in s1["messages"] if m["kind"] == "answer" and cid in (m.get("refs") or [])]
        check("Claude-1 received OpenCode-1's opinion (one message)", len(answers) == 1 and answers[0]["sender"] == oc_id,
              answers[0]["subject"] if answers else "none")

        # ---------------------------------------------------------------- 6b. Claude-1 decides
        audit = st["evidence_audit"]
        positions = [o["position"] for o in st["opinions"]]
        if "accept_challenge" in positions and audit["challenge"] >= audit["existing"]:
            resolution = "accept_challenge"
        elif "keep" in positions and audit["existing"] >= audit["challenge"]:
            resolution = "keep"
        else:
            resolution = "accept_challenge" if audit["challenge"] > audit["existing"] else "keep"
        rationale = (f"OpenCode-1 (consulted, implemented {tasks['code']}) answered {positions[0] if positions else 'nothing'}; "
                     f"Laya's evidence audit: existing {audit['existing']} vs challenge {audit['challenge']}. "
                     f"Resolution follows the opinion where the evidence agrees.")
        say(f"    Claude-1 chooses '{resolution}' (rule: follow the consulted opinion when the evidence audit agrees)")
        resv = await c1.call("conflict_resolve", conflict_id=cid, resolution=resolution, rationale=rationale)
        say(f"    resolved -> decision {resv['decision_id']}; effects {resv['effects']}; audit {resv['evidence_audit']}")
        expected = sorted({gpt.id, oc_id, c2.id})
        check("resolution notified exactly the involved: raiser, consultant, finding author",
              resv["notified"] == expected, f"notified={resv['notified']}")
        thread = await c1.call("message_thread", thread_id=alerts[0]["thread_id"])
        say("    conflict thread (seen by Claude-1): " + " -> ".join(f"{m['sender_name']}:{m['kind']}" for m in thread))
        check("one thread: alert -> question -> answer -> decision",
              [m["kind"] for m in thread] == ["alert", "question", "answer", "decision"],
              str([m["kind"] for m in thread]))

        # ---------------------------------------------------------------- 7. finish the pipeline + verify
        say("\n7) ChatGPT-1 completes the research task; verification from the driver side")
        await gpt.sync()
        await gpt.call("command_update", command_id=gcmd["command_id"], status="COMPLETED",
                       note=f"No-data pixels are excluded from the denominator ({cid} resolved: {resolution}); "
                            f"matches OpenCode-1's cloud_cover.py.")
        ps = await c1.call("pipeline_status", plan_id=plan["plan_id"])
        check("plan completed", ps["plans"][0]["status"] == "completed", f"{ps['plans'][0]['progress']} done")
        await c3.sync()
        c3_conf = [m for m in c3.received if cid in (m.get("refs") or [])]
        check("Claude-3 (uninvolved) never received a conflict message", not c3_conf, f"{len(c3_conf)} message(s)")
        ov = admin_get(base, key, ak, f"/api/projects/{pid}/overview")
        msgs = ov["messages"]
        names = {p["participant_id"]: p["name"] for p in ov["participants"]}
        names.update({"orchestrator": "orchestrator", "system": "system", "laya": "Laya"})
        conflict_msgs = [m for m in msgs if cid in (m.get("refs") or [])]
        involved = {c1.id, gpt.id, oc_id, c2.id}
        stray = [m for m in conflict_msgs if m["to"] == ["all"] or set(m["to"]) - involved]
        check("no conflict message was broadcast or sent to anyone uninvolved", not stray and len(conflict_msgs) == 4,
              f"{len(conflict_msgs)} conflict messages, stray={len(stray)}")
        oc_sent = [m for m in msgs if m["sender"] == oc_id]
        check("OpenCode-1 sent no unnecessary message (only its opinion)", len(oc_sent) == 1 and oc_sent[0]["kind"] == "answer",
              ", ".join(f"{m['kind']}->{[names.get(t, t) for t in m['to']]}" for m in oc_sent))
        bodies = [(m["sender"], tuple(m["to"]), m["body"]) for m in msgs]
        check("no duplicate messages", len(bodies) == len(set(bodies)), f"{len(msgs)} messages in the project")
        say("    message ledger (sender -> recipients : kind : subject):")
        for m in msgs:
            to = "all" if m["to"] == ["all"] else ", ".join(names.get(t, t) for t in m["to"])
            say(f"      {names.get(m['sender'], m['sender']):<12} -> {to:<40} : {m['kind']:<8} : {m['subject'][:70]}")

    # -------------------------------------------------------------------- 8. bridge
    say("\n8) Repo bridge for the OpenCode workspace (token from .palinor/session.json, key via env)")
    sess = read_session(ws)
    env = {**os.environ, "PALINOR_TOKEN": sess.get("token", ""), "PALINOR_KEY": key, "PYTHONIOENCODING": "utf-8"}

    def bridge() -> str:
        r = subprocess.run([sys.executable, "-m", "palinor.bridge", "--server", base, "--repo", str(ws), "--once"],
                           cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=120,
                           stdin=subprocess.DEVNULL, encoding="utf-8")
        out = redact((r.stdout + r.stderr).strip())
        say("    " + out.replace("\n", "\n    "))
        return out

    out1 = bridge()
    md = (ws / "PALINOR.md").read_text(encoding="utf-8") if (ws / "PALINOR.md").exists() else ""
    check("bridge run 1 wrote PALINOR.md", "PALINOR.md updated" in out1 and "cloud-cover utility" in md
          and f"Waiting on you ({oc_id})" in md, f"{len(md)} chars")
    inbox = ws / "palinor-inbox"
    note = inbox / "opencode-cloud-cover-notes.md"
    note.write_text("# OpenCode-1 notes: cloud_cover.py\n\n"
                    "Written in the OpenCode workspace after the conflict was resolved.\n\n"
                    "## Findings\n"
                    "- cloud_cover.cloud_fraction excludes no-data (255) pixels from the denominator\n"
                    "- cloud_fraction returns 0.0 for a tile that is entirely no-data\n\n"
                    "## Questions\n"
                    "- Should cloud shadow get its own mask value?\n", encoding="utf-8")
    say(f"    user wrote {note.relative_to(ws)}")
    out2 = bridge()
    conv_id = next((w.split("-> ")[1].split()[0] for w in out2.splitlines() if "ingested note" in w and "-> " in w), "")
    check("bridge run 2 ingested the note", note.name in out2 and conv_id.startswith("CONV"), conv_id or out2[-200:])
    async with Mcp(mcp_url) as mcp:
        c1b = Member(mcp, "claude-1", False)
        c1b.token = c1.token
        if conv_id:
            conv = await c1b.call("conversation_get", conversation_id=conv_id)
            labels = sorted(x["label"] for x in conv["extracted"])
            check("the note is in the graph with its findings and question, ingested by OpenCode-1",
                  conv["ingested_by"] == oc_id and labels == ["Finding", "Finding", "OpenQuestion"],
                  f"{conv_id} by {conv['ingested_by']}: {labels}")
    out3 = bridge()
    md = (ws / "PALINOR.md").read_text(encoding="utf-8")
    check("bridge run 3 refreshed PALINOR.md with the ingested note", "PALINOR.md updated" in out3
          and conv_id in md and "excludes no-data" in md, "note listed under 'Ingested chats & notes'")
    return finish(pid, ak, base)


def finish(pid: str, ak: str, base: str) -> int:
    failed = [c for c in CHECKS if not c[1]]
    say(f"\nRESULT: {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    for name, _, detail in failed:
        say(f"  FAILED: {name} - {detail}")
    say(f"Dashboard: {base}/  project_id={pid}  (admin_key is in demo/.oc_scenario_state.json)")
    (LOGS / "oc_scenario_report.txt").write_text("\n".join(REPORT) + "\n", encoding="utf-8")
    (LOGS / "oc_scenario_checks.json").write_text(json.dumps(
        [{"check": n, "ok": ok, "detail": d} for n, ok, d in CHECKS], indent=2), encoding="utf-8")
    print(f"admin_key (private): {ak}")
    return 1 if failed else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.getenv("PALINOR_URL", "http://localhost:8000"))
    ap.add_argument("--key", default=os.getenv("PALINOR_SERVER_KEY", ""), help="server key (default data/server_key.txt)")
    ap.add_argument("--workspace", default=str(DEMO / "opencode-workspace"))
    ap.add_argument("--models", nargs="+", default=FREE_MODELS, help="free opencode/* models, tried in order")
    ap.add_argument("--turn-timeout", type=int, default=360, help="seconds per OpenCode turn")
    args = ap.parse_args()
    if any(not m.startswith("opencode/") for m in args.models):
        raise SystemExit("only free opencode/* models are allowed")
    sys.exit(asyncio.run(main(args)))
