"""Code graph of a project's GitHub repo, built with Graphify (pip package `graphifyy`, AST only, no LLM).

Used to verify claims about the code ("I added X in file Y", "Y calls Z") against what is really in the repo.
The full graph stays on disk under data/codegraphs/<project>/<branch>; the knowledge graph gets one CodeGraph
summary node per build so everyone can see what was indexed and when.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

from .core import PalinorError, iso

ROOT = Path(__file__).resolve().parents[1] / "data" / "codegraphs"
BRANCH_RE = re.compile(r"[A-Za-z0-9._/-]{1,100}")


def _rmtree(path: Path) -> None:
    """shutil.rmtree that also removes git's read-only pack files on Windows."""
    import os
    import stat

    def unlock(func, target, _exc):
        os.chmod(target, stat.S_IWRITE)
        func(target)

    if path.exists():
        shutil.rmtree(path, onexc=unlock)


def _dirs(project_id: str, branch: str) -> tuple[Path, Path]:
    base = ROOT / project_id / branch.replace("/", "__")
    return base / "repo", base / "repo" / "graphify-out" / "graph.json"


def _run(cmd: list[str], cwd: Path | None = None, timeout: int = 600) -> str:
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                       timeout=timeout, env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8"})
    if r.returncode != 0:
        raise PalinorError(f"{cmd[0]} failed: {(r.stderr or r.stdout).strip()[-300:]}")
    return r.stdout


def build(svc, p: dict, branch: str = "main") -> dict:
    """Clone (or update) the project repo at `branch` and rebuild its Graphify code graph."""
    repo = svc._project(p["project_id"]).get("repo")
    if not repo:
        raise PalinorError("this project has no repo yet; the owner sets one with project_repo_set")
    if not BRANCH_RE.fullmatch(branch) or ".." in branch:
        raise PalinorError("invalid branch name")
    src, graph = _dirs(p["project_id"], branch)
    _rmtree(src)
    src.parent.mkdir(parents=True, exist_ok=True)
    _run(["gh", "repo", "clone", repo, str(src), "--", "--depth", "1", "--branch", branch])
    _run(["graphify", "update", "."], cwd=src)
    g = json.loads(graph.read_text(encoding="utf-8"))
    nodes, links = g.get("nodes", []), g.get("links", [])
    files = sorted({n.get("source_file") for n in nodes if n.get("source_file")})
    communities = sorted({n.get("community_name") for n in nodes if n.get("community_name")})
    head = _run(["git", "rev-parse", "HEAD"], cwd=src).strip()
    summary = {"project_id": p["project_id"], "repo": repo, "branch": branch, "commit": head, "nodes": len(nodes),
               "edges": len(links), "files": len(files), "modules": communities[:60], "built_by": p["id"],
               "built_at": iso()}
    svc.g.upsert("CodeGraph", f"CG-{p['project_id']}-{branch.replace('/', '__')}", summary)
    svc._emit(p["project_id"], "CODE_GRAPH_BUILT", p["id"],
              payload={"branch": branch, "commit": head[:12], "nodes": len(nodes), "edges": len(links)})
    return {**summary, "modules": communities[:20],
            "next": "code_verify(names=[...]) to check symbols/files exist and see what they connect to."}


def _load(project_id: str, branch: str) -> dict:
    _, graph = _dirs(project_id, branch)
    if not graph.exists():
        raise PalinorError(f"no code graph for branch {branch}; run code_graph_build first")
    return json.loads(graph.read_text(encoding="utf-8"))


def _norm(name) -> str:
    return str(name or "").strip().lower().removesuffix("()").lstrip(".")


def verify(svc, p: dict, names: list[str], branch: str = "main") -> dict:
    """For each name (function, class, file), say whether it exists in the code and what it links to."""
    g = _load(p["project_id"], branch)
    nodes, links = g.get("nodes", []), g.get("links", [])
    label = {n["id"]: n.get("label", n["id"]) for n in nodes}
    out = []
    for raw in (names or [])[:30]:
        q = _norm(raw)
        hits = [n for n in nodes if q and (q == _norm(n.get("norm_label")) or q == _norm(n.get("label"))
                                           or q == (n.get("source_file") or "").lower())]
        if not hits:
            out.append({"name": raw, "exists": False})
            continue
        n = hits[0]
        rel = [f"{l['relation']} -> {label.get(l['target'], l['target'])}" for l in links if l["source"] == n["id"]][:12]
        inc = [f"{label.get(l['source'], l['source'])} {l['relation']}" for l in links if l["target"] == n["id"]][:12]
        out.append({"name": raw, "exists": True, "matches": len(hits), "file": n.get("source_file"),
                    "location": n.get("source_location"), "module": n.get("community_name"),
                    "links_to": rel, "linked_from": inc})
    missing = [o["name"] for o in out if not o["exists"]]
    return {"branch": branch, "checked": len(out), "missing": missing, "results": out,
            "verdict": "all found" if not missing else f"{len(missing)} not in the code"}


def query(svc, p: dict, question: str, branch: str = "main") -> dict:
    """Graphify's BFS answer to a question about the code (no LLM)."""
    src, graph = _dirs(p["project_id"], branch)
    if not graph.exists():
        raise PalinorError(f"no code graph for branch {branch}; run code_graph_build first")
    text = _run(["graphify", "query", question[:300], "--graph", str(graph)], cwd=src, timeout=120)
    return {"branch": branch, "answer": text.strip()[-6000:]}


# ----------------------------------------------------------- read / submit for participants without git
MAX_READ_LINES = 400
MAX_FILE_BYTES = 200_000
BLOCKED = (".git/", ".github/")  # no git internals, no CI workflow changes through the server


def _safe_path(root: Path, rel: str) -> Path:
    rel = (rel or "").replace("\\", "/").strip().lstrip("/")
    if not rel or ".." in rel.split("/") or rel.startswith(BLOCKED) or rel in {".git", ".github"}:
        raise PalinorError(f"path not allowed: {rel!r}")
    target = (root / rel).resolve()
    if root.resolve() not in target.parents:
        raise PalinorError(f"path escapes the repo: {rel!r}")
    return target


def _checkout(svc, p: dict, branch: str) -> Path:
    """Fresh shallow clone of `branch` (used for reads; falls back to main if the branch does not exist yet)."""
    repo = svc._project(p["project_id"]).get("repo")
    if not repo:
        raise PalinorError("this project has no repo yet; the owner sets one with project_repo_set")
    if not BRANCH_RE.fullmatch(branch) or ".." in branch:
        raise PalinorError("invalid branch name")
    src, _ = _dirs(p["project_id"], branch)
    if not (src / ".git").exists():
        src.parent.mkdir(parents=True, exist_ok=True)
        _run(["gh", "repo", "clone", repo, str(src), "--", "--depth", "1", "--branch", branch])
    return src


def read(svc, p: dict, path: str = "", start: int = 1, end: int = 200, branch: str = "main") -> dict:
    """File contents (a line range) from the project repo, or a file listing when path is empty or a folder."""
    src = _checkout(svc, p, branch)
    # the cache is a read-only mirror: match the remote exactly (a fast-forward pull fails after squash merges)
    try:
        _run(["git", "fetch", "-q", "--depth", "1", "origin", branch], cwd=src)
        _run(["git", "reset", "-q", "--hard", "FETCH_HEAD"], cwd=src)
    except PalinorError as exc:
        _rmtree(src)
        raise PalinorError(f"branch {branch!r} is no longer on GitHub (merged or deleted?): {exc}") from exc
    if not path or (src / path).is_dir():
        base = src if not path else _safe_path(src, path)
        files = sorted(str(f.relative_to(src)).replace("\\", "/") for f in base.rglob("*")
                       if f.is_file() and ".git" not in f.parts and "graphify-out" not in f.parts)
        return {"branch": branch, "files": files[:500], "count": len(files)}
    f = _safe_path(src, path)
    if not f.is_file():
        raise PalinorError(f"no such file on {branch}: {path}")
    lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
    start = max(1, int(start))
    end = min(len(lines), max(start, int(end)), start + MAX_READ_LINES - 1)
    body = "\n".join(f"{i:>5}  {lines[i - 1]}" for i in range(start, end + 1))
    return {"branch": branch, "path": path, "lines": f"{start}-{end}", "total_lines": len(lines), "content": body,
            "more": end < len(lines)}


def submit(svc, p: dict, task_id: str, files: list[dict], message: str) -> dict:
    """Commit files to the participant's task branch (never main) and open or update its PR."""
    svc._require(p, "write")
    svc._node(task_id, "Task", p["project_id"])
    repo = svc._project(p["project_id"]).get("repo")
    if not repo:
        raise PalinorError("this project has no repo yet; the owner sets one with project_repo_set")
    branch = svc.branch_name(p, task_id)
    if branch in {"main", "master"} or not BRANCH_RE.fullmatch(branch):
        raise PalinorError("refusing to write to that branch")
    if not files or len(files) > 30:
        raise PalinorError("send 1-30 files: [{path, content}] (content '' deletes the file)")
    work = ROOT / p["project_id"] / "_work" / branch.replace("/", "__")
    _rmtree(work)
    work.parent.mkdir(parents=True, exist_ok=True)
    _run(["gh", "repo", "clone", repo, str(work), "--", "-q"])
    remote = _run(["git", "ls-remote", "--heads", "origin", branch], cwd=work).strip()
    _run(["git", "checkout", "-q", "-B", branch] + ([f"origin/{branch}"] if remote else []), cwd=work)
    changed = []
    for item in files:
        content = item.get("content", "")
        binary = (item.get("encoding") or "").lower() in {"base64", "b64"}  # images, PDFs, other binary files
        if binary:
            import base64
            try:
                data = base64.b64decode(content.split(",", 1)[-1] if content.startswith("data:") else content,
                                        validate=True)
            except ValueError as exc:
                raise PalinorError(f"{item.get('path')}: invalid base64 ({exc})") from exc
        else:
            data = content.encode("utf-8")
        if len(data) > MAX_FILE_BYTES:
            raise PalinorError(f"{item.get('path')}: file too large (max {MAX_FILE_BYTES // 1000} kB)")
        target = _safe_path(work, item.get("path", ""))
        if content == "":
            if target.exists():
                target.unlink()
        elif binary:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8", newline="\n")
        changed.append(item["path"])
    _run(["git", "add", "-A"], cwd=work)
    # files the participant sent explicitly are committed even if .gitignore matches them (git add -A skips those)
    for path in changed:
        if (work / path).exists():
            _run(["git", "add", "-f", "--", path], cwd=work)
    if not _run(["git", "status", "--porcelain"], cwd=work).strip():
        return {"branch": branch, "changed": [], "note": "no changes compared to the branch"}
    author = f"{p['name']} via PALINOR <palinor+{slug_id(p['id'])}@users.noreply.github.com>"
    _run(["git", "-c", f"user.name={p['name']} (PALINOR)", "-c", "user.email=palinor@users.noreply.github.com",
          "commit", "-q", "-m", (message or f"{task_id}: update")[:500], f"--author={author}"], cwd=work)
    sha = _run(["git", "rev-parse", "HEAD"], cwd=work).strip()
    _run(["git", "push", "-q", "origin", branch], cwd=work)
    pr = _run(["gh", "pr", "list", "--repo", repo, "--head", branch, "--json", "url", "--jq", ".[0].url"], cwd=work).strip()
    if not pr:
        title = svc.g.get(task_id).get("title", task_id)
        pr = _run(["gh", "pr", "create", "--repo", repo, "--head", branch, "--base", "main",
                   "--title", f"[{task_id}] {title}"[:200],
                   "--body", f"Submitted through PALINOR by {p['name']} ({p['id']}) for task {task_id}.\n\n"
                             f"Merge only after code_verify on this branch (project rule)."], cwd=work).strip()
    svc.link_github(p, task_id, branch=branch, commit_sha=sha, pr_url=pr, pr_state="open")
    _rmtree(work)
    return {"branch": branch, "commit": sha, "pr": pr, "changed": changed,
            "next": "Master-Claude verifies with code_graph_build(branch) + code_verify, then merges."}


def slug_id(s: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", s.lower())[:40]
