"""PALINOR repo bridge — keeps a repository in sync with the knowledge graph (stdlib only).

Runs next to any coding agent (OpenCode, Claude Code, Cursor...) on any laptop:
  * pulls  PALINOR.md      - regenerated digest of the project, written to the repo root
  * pushes palinor-inbox/  - every new or edited *.md note is ingested into the graph
                             (template: ## Findings / ## Agreements / ## Questions / ## Tasks)
  * prints what is waiting for you (unread messages, votes owed, syntheses to write)

  python -m palinor.bridge --server https://<host> --key <SERVER_KEY> --token <PARTICIPANT_TOKEN> --repo .

The graph stays the single source of truth; PALINOR.md is a read-only view of it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

STATE_FILE = ".palinor-bridge.json"
INBOX_TEMPLATE = """# Title of your note

Short summary of what this note is about.

## Findings
- a fact you learned

## Agreements
- something the team agreed on

## Questions
- an open question

## Tasks
- a follow-up task
"""


class Bridge:
    def __init__(self, server: str, key: str, token: str, repo: Path, inbox: str = "palinor-inbox"):
        self.server, self.key, self.token = server.rstrip("/"), key, token
        self.repo = repo.resolve()
        self.inbox = self.repo / inbox
        self.state_path = self.repo / STATE_FILE
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {"notes": {}, "seq": 0}

    def _req(self, method: str, path: str, body: dict | None = None, params: dict | None = None):
        q = dict(params or {})
        url = f"{self.server}{path}" + (f"?{urllib.parse.urlencode(q)}" if q else "")
        data = json.dumps(body).encode() if body is not None else None
        headers = {"X-Palinor-Token": self.token, "Content-Type": "application/json", "User-Agent": "palinor-bridge/1"}
        if self.key:  # header, never the URL: query strings end up in proxy/tunnel/access logs
            headers["Authorization"] = f"Bearer {self.key}"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8")
            return raw if r.headers.get_content_type() == "text/plain" else json.loads(raw)

    def _safe_write(self, target: Path, text: str) -> None:
        """Never follow a symlink planted in the repo (it could point at ~/.bashrc or credentials)."""
        if target.is_symlink() or (target.exists() and not target.is_file()):
            raise RuntimeError(f"refusing to write {target}: not a regular file")
        if target.resolve().parent != self.repo and target.resolve().parent != self.inbox.resolve():
            raise RuntimeError(f"refusing to write outside the repo: {target}")
        tmp = target.with_name(f".{target.name}.tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, target)

    def _save(self) -> None:
        self._safe_write(self.state_path, json.dumps(self.state, indent=2))

    def pull_digest(self) -> bool:
        md = self._req("GET", "/api/me/export.md")
        target = self.repo / "PALINOR.md"
        # ignore the timestamp line when deciding whether anything really changed
        body = "\n".join(l for l in md.splitlines() if not l.startswith("> Auto-generated"))
        h = hashlib.sha256(body.encode()).hexdigest()
        if h == self.state.get("digest"):
            return False
        self._safe_write(target, md)
        self.state["digest"] = h
        return True

    def push_notes(self) -> list[str]:
        if self.inbox.is_symlink():
            raise RuntimeError(f"refusing to use {self.inbox}: the inbox is a symlink")
        self.inbox.mkdir(exist_ok=True)
        readme = self.inbox / "_TEMPLATE.md"
        if not readme.exists() and not readme.is_symlink():
            self._safe_write(readme, INBOX_TEMPLATE)
        sent = []
        inbox_real = self.inbox.resolve()
        for f in sorted(self.inbox.glob("*.md")):
            if f.name.startswith("_"):
                continue
            # a symlinked note could exfiltrate any local file (e.g. ../../.aws/credentials) to every participant
            if f.is_symlink() or not f.is_file() or f.resolve().parent != inbox_real:
                print(f"skipping {f.name}: symlinks and files outside the inbox are never uploaded", file=sys.stderr)
                continue
            if f.stat().st_size > 2_000_000:
                print(f"skipping {f.name}: larger than 2 MB", file=sys.stderr)
                continue
            text = f.read_text(encoding="utf-8")
            h = hashlib.sha256(text.encode()).hexdigest()
            if self.state["notes"].get(f.name) == h or not text.strip():
                continue
            res = self._req("POST", "/api/me/notes", {"filename": f.stem, "content": text})
            self.state["notes"][f.name] = h
            sent.append(f"{f.name} -> {res.get('conversation_id')}"
                        + (" (already ingested)" if res.get("already_ingested") else ""))
        return sent

    def status(self) -> dict:
        s = self._req("GET", "/api/me/sync", params={"since_seq": self.state.get("seq", 0)})
        self.state["seq"] = s["latest_seq"]
        return s

    def once(self) -> None:
        changed = self.pull_digest()
        sent = self.push_notes()
        st = self.status()
        self._save()
        stamp = time.strftime("%H:%M:%S")
        if changed:
            print(f"[{stamp}] PALINOR.md updated")
        for s in sent:
            print(f"[{stamp}] ingested note {s}")
        if st["unread_messages"] or st["your_actions"]:
            acts = ", ".join(f"{a['action']}{' ' + a['proposal_id'] if a.get('proposal_id') else ''}"
                             for a in st["your_actions"][:6])
            print(f"[{stamp}] waiting on you: {st['unread_messages']} unread message(s)"
                  + (f"; actions: {acts}" if acts else "") + " -> ask your agent to run palinor_sync")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True, help="base URL, e.g. https://xyz.trycloudflare.com")
    ap.add_argument("--key", default=os.getenv("PALINOR_KEY", ""),
                    help="PALINOR server key (prefer env PALINOR_KEY: arguments show up in process lists)")
    ap.add_argument("--token", default=os.getenv("PALINOR_TOKEN", ""),
                    help="participant token from palinor_join (prefer env PALINOR_TOKEN)")
    ap.add_argument("--repo", default=".", help="repository root to keep in sync")
    ap.add_argument("--interval", type=float, default=20, help="seconds between syncs")
    ap.add_argument("--once", action="store_true", help="sync once and exit")
    a = ap.parse_args()
    if not a.token:
        ap.error("a participant token is required: set PALINOR_TOKEN or pass --token")
    b = Bridge(a.server, a.key, a.token, Path(a.repo))
    print(f"PALINOR bridge: {b.repo} <-> {b.server} (inbox: {b.inbox.name}/)")
    while True:
        try:
            b.once()
        except urllib.error.HTTPError as exc:
            print(f"server said {exc.code}: {exc.read().decode()[:200]}", file=sys.stderr)
            if exc.code in (401, 403):
                sys.exit(1)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"server unreachable ({exc}); retrying", file=sys.stderr)
        if a.once:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
