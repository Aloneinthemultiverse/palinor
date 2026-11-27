"""Repo bridge against a real PALINOR server process (own port, temp data dir, in-memory graph, Kafka off)."""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from palinor.bridge import Bridge

ROOT = Path(__file__).resolve().parents[1]
KEY = "bridge-test-key"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _http(base, method, path, body=None, params=None, headers=None):
    q = urllib.parse.urlencode({**(params or {}), "key": KEY})
    req = urllib.request.Request(f"{base}{path}?{q}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.headers.get_content_type(), r.read().decode("utf-8")


def _mcp(base, tool, arguments):
    _, raw = _http(base, "POST", "/mcp", {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                          "params": {"name": tool, "arguments": arguments}},
                   headers={"Accept": "application/json, text/event-stream"})
    data = next(line[5:].strip() for line in raw.splitlines() if line.startswith("data:"))
    result = json.loads(data)["result"]
    assert not result.get("isError"), result
    return json.loads(result["content"][0]["text"])


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    data = tmp_path_factory.mktemp("palinor-srv")
    port = _free_port()
    env = {**os.environ, "PALINOR_DATA_DIR": str(data), "PALINOR_PORT": str(port), "PALINOR_HOST": "127.0.0.1",
           "PALINOR_SERVER_KEY": KEY, "PALINOR_GRAPH": "memory", "KAFKA_BOOTSTRAP": ""}
    out = open(data / "server.out", "wb")
    proc = subprocess.Popen([sys.executable, "-m", "palinor.server"], cwd=ROOT, env=env, stdout=out,
                            stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 40
    try:
        while True:
            if proc.poll() is not None:
                pytest.fail("server exited early:\n" + (data / "server.out").read_text(errors="replace")[-2000:])
            try:
                urllib.request.urlopen(base + "/api/health", timeout=1).read()
                break
            except OSError:
                if time.time() > deadline:
                    pytest.fail("server did not start in time")
                time.sleep(0.25)
        yield base
    finally:
        proc.terminate()
        try:
            proc.wait(15)
        except subprocess.TimeoutExpired:
            proc.kill()
        out.close()


@pytest.fixture
def project(server):
    _, raw = _http(server, "POST", "/api/projects", {"name": "Bridge | test", "goal": "sync a repo"})
    proj = json.loads(raw)
    joined = _mcp(server, "palinor_join", {"project_id": proj["project_id"], "join_key": proj["join_key"],
                                           "name": "RepoAgent", "platform": "opencode"})
    return proj, joined["token"]


def _conversations(server, proj):
    _, raw = _http(server, "GET", f"/api/projects/{proj['project_id']}/overview",
                   params={"admin_key": proj["admin_key"]})
    return json.loads(raw)["conversations"]


def test_server_rejects_missing_key_and_token(server):
    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(server + "/api/me/sync", timeout=5)
    assert e.value.code == 401
    with pytest.raises(urllib.error.HTTPError) as e:
        _http(server, "GET", "/api/me/sync")
    assert e.value.code == 401  # key ok, participant token missing
    with pytest.raises(urllib.error.HTTPError) as e:
        _http(server, "GET", "/api/me/sync", headers={"X-Palinor-Token": "forged"})
    assert e.value.code == 403


def test_bridge_pull_and_push_notes(server, project, tmp_path, capsys):
    proj, token = project
    repo = tmp_path / "repo"
    inbox = repo / "palinor-inbox"
    inbox.mkdir(parents=True)
    note = inbox / "design.md"
    note.write_text("# Storage design\nWe compared stores.\n\n## Findings\n- HBase scales writes\n"
                    "## Tasks\n- benchmark scans\n", encoding="utf-8")
    (inbox / "_draft.md").write_text("# Draft\n## Findings\n- not ready\n", encoding="utf-8")
    (inbox / "empty.md").write_text("  \n", encoding="utf-8")

    Bridge(server, KEY, token, repo).once()
    md = (repo / "PALINOR.md").read_text(encoding="utf-8")
    assert md.startswith("# PALINOR · Bridge \\| test") and "## Waiting on you" in md
    assert (inbox / "_TEMPLATE.md").exists()
    convs = _conversations(server, proj)
    assert [(c["title"], c["kind"]) for c in convs] == [("Storage design", "note")]
    first = capsys.readouterr().out
    assert "PALINOR.md updated" in first and "ingested note design.md -> CONV-" in first

    b = Bridge(server, KEY, token, repo)  # state persisted in .palinor-bridge.json
    b.once()  # digest now contains the note's finding; the note itself is not re-sent
    assert "HBase scales writes" in (repo / "PALINOR.md").read_text(encoding="utf-8")
    b.once()
    assert "ingested note" not in capsys.readouterr().out
    assert len(_conversations(server, proj)) == 1

    (repo / ".palinor-bridge.json").unlink()  # lost state: re-sent, but the server de-duplicates
    Bridge(server, KEY, token, repo).once()
    assert "(already ingested)" in capsys.readouterr().out
    assert len(_conversations(server, proj)) == 1

    note.write_text(note.read_text(encoding="utf-8") + "- HBase needs salted row keys\n", encoding="utf-8")
    (inbox / "bom.md").write_bytes("﻿# Windows note\n## Questions\n- BOM safe?\n".encode("utf-8"))
    Bridge(server, KEY, token, repo).once()
    titles = sorted(c["title"] for c in _conversations(server, proj))
    assert titles == ["Storage design", "Storage design", "Windows note"]
    assert not any(t in titles for t in ("Title of your note", "Draft", "empty"))
    state = json.loads((repo / ".palinor-bridge.json").read_text())
    assert set(state["notes"]) == {"design.md", "bom.md"} and state["seq"] > 0
