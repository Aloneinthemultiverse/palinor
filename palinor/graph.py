"""Knowledge Graph storage behind one small interface.

MemoryGraph  - in-process, JSON-snapshotted (dev / tests / single laptop)
Neo4jGraph   - production graph store

Every node has a label, an `id`, and a `project_id` property so the backend can
scope queries to a project.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Protocol

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"invalid graph identifier: {name!r}")
    return name


class Graph(Protocol):
    def upsert(self, label: str, node_id: str, props: dict) -> dict: ...
    def get(self, node_id: str) -> dict | None: ...
    def find(self, label: str, **eq: Any) -> list[dict]: ...
    def link(self, src: str, rel: str, dst: str) -> None: ...
    def unlink(self, src: str, rel: str, dst: str | None = None) -> None: ...
    def out(self, node_id: str, rel: str | None = None, label: str | None = None) -> list[dict]: ...
    def inn(self, node_id: str, rel: str | None = None, label: str | None = None) -> list[dict]: ...
    def stats(self) -> dict: ...
    def subgraph(self, project_id: str, limit: int = 400) -> tuple[list[dict], list[tuple[str, str, str]]]: ...


class MemoryGraph:
    def __init__(self, path: Path | None = None):
        self.path = path
        self.nodes: dict[str, dict] = {}  # id -> {"label": str, "props": dict}
        self.edges: set[tuple[str, str, str]] = set()
        self._lock = threading.RLock()
        if path and path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            self.nodes = data["nodes"]
            self.edges = {tuple(e) for e in data["edges"]}

    SAVE_EVERY_S = 1.0

    def _save(self) -> None:
        """Mark dirty; write at most once per SAVE_EVERY_S (flush() writes the rest)."""
        if not self.path:
            return
        self._dirty = True
        if time.time() - getattr(self, "_last_save", 0.0) >= self.SAVE_EVERY_S:
            self.flush()

    def flush(self) -> None:
        if not self.path or not getattr(self, "_dirty", False):
            return
        with self._lock:
            self._write()
            self._dirty, self._last_save = False, time.time()

    def _write(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"nodes": self.nodes, "edges": sorted(self.edges)}, default=str),
                       encoding="utf-8")
        os.replace(tmp, self.path)

    def _view(self, node_id: str) -> dict:
        n = self.nodes[node_id]
        return {**n["props"], "_label": n["label"]}

    def upsert(self, label: str, node_id: str, props: dict) -> dict:
        with self._lock:
            node = self.nodes.setdefault(node_id, {"label": _check_ident(label), "props": {}})
            node["props"].update(props)
            node["props"]["id"] = node_id
            self._save()
            return self._view(node_id)

    def get(self, node_id: str) -> dict | None:
        with self._lock:
            return self._view(node_id) if node_id in self.nodes else None

    def find(self, label: str, **eq: Any) -> list[dict]:
        with self._lock:
            return [self._view(i) for i, n in self.nodes.items()
                    if n["label"] == label and all(n["props"].get(k) == v for k, v in eq.items())]

    def link(self, src: str, rel: str, dst: str) -> None:
        with self._lock:
            if src not in self.nodes or dst not in self.nodes:
                raise KeyError(f"link endpoint missing: {src} -{rel}-> {dst}")
            self.edges.add((src, _check_ident(rel), dst))
            self._save()

    def unlink(self, src: str, rel: str, dst: str | None = None) -> None:
        with self._lock:
            self.edges = {e for e in self.edges
                          if not (e[0] == src and e[1] == rel and (dst is None or e[2] == dst))}
            self._save()

    def _walk(self, node_id, rel, label, outgoing: bool) -> list[dict]:
        with self._lock:
            res = []
            for s, r, d in self.edges:
                a, b = (s, d) if outgoing else (d, s)
                if a == node_id and (rel is None or r == rel) and b in self.nodes:
                    if label is None or self.nodes[b]["label"] == label:
                        res.append(self._view(b))
            return res

    def out(self, node_id, rel=None, label=None):
        return self._walk(node_id, rel, label, True)

    def inn(self, node_id, rel=None, label=None):
        return self._walk(node_id, rel, label, False)

    def subgraph(self, project_id: str, limit: int = 400):
        with self._lock:
            ids = [i for i, n in self.nodes.items() if n["props"].get("project_id") == project_id or i == project_id]
            ids = set(ids[-limit:])
            return ([self._view(i) for i in ids],
                    [e for e in self.edges if e[0] in ids and e[2] in ids])

    def stats(self) -> dict:
        with self._lock:
            labels: dict[str, int] = {}
            for n in self.nodes.values():
                labels[n["label"]] = labels.get(n["label"], 0) + 1
            return {"backend": "memory", "nodes": len(self.nodes), "edges": len(self.edges), "labels": labels}


def _enc(v: Any) -> Any:
    # Neo4j stores only flat, homogeneous, null-free lists; strings that already look encoded are escaped
    if isinstance(v, dict) or (isinstance(v, str) and v.startswith("json:")) or (isinstance(v, list) and (
            any(x is None or isinstance(x, (dict, list)) for x in v) or len({type(x) for x in v}) > 1)):
        return "json:" + json.dumps(v, default=str)
    return v


def _dec1(v: Any) -> Any:
    if isinstance(v, str) and v.startswith("json:"):
        try:
            return json.loads(v[5:])
        except ValueError:  # legacy raw string that merely starts with "json:"
            return v
    return v


def _dec(props: dict) -> dict:
    return {k: _dec1(v) for k, v in props.items()}


class Neo4jGraph:
    def __init__(self, uri: str, user: str, password: str):
        from neo4j import GraphDatabase

        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        with self.driver.session() as s:
            s.run("CREATE CONSTRAINT node_id IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE")
            s.run("CREATE INDEX node_project IF NOT EXISTS FOR (n:Node) ON (n.project_id)")

    def _q(self, cypher: str, **params) -> list:
        with self.driver.session() as s:
            return list(s.run(cypher, **params))

    @staticmethod
    def _node(rec, key="n") -> dict:
        n = rec[key]
        labels = [l for l in n.labels if l != "Node"]
        return {**_dec(dict(n)), "_label": labels[0] if labels else "Node"}

    def upsert(self, label, node_id, props):
        label = _check_ident(label)
        props = {k: _enc(v) for k, v in props.items() if v is not None}
        rec = self._q(f"MERGE (n:Node {{id:$id}}) SET n:{label} SET n += $props RETURN n",
                      id=node_id, props=props)
        return self._node(rec[0])

    def get(self, node_id):
        rec = self._q("MATCH (n:Node {id:$id}) RETURN n", id=node_id)
        return self._node(rec[0]) if rec else None

    def find(self, label, **eq):
        label = _check_ident(label)
        where = " AND ".join(f"n.{_check_ident(k)} = $p{i}" for i, k in enumerate(eq)) or "true"
        params = {f"p{i}": _enc(v) for i, v in enumerate(eq.values())}
        return [self._node(r) for r in self._q(f"MATCH (n:{label}) WHERE {where} RETURN n", **params)]

    def link(self, src, rel, dst):
        rel = _check_ident(rel)
        self._q(f"MATCH (a:Node {{id:$s}}),(b:Node {{id:$d}}) MERGE (a)-[:{rel}]->(b)", s=src, d=dst)

    def unlink(self, src, rel, dst=None):
        rel = _check_ident(rel)
        self._q(f"MATCH (a:Node {{id:$s}})-[r:{rel}]->(b) WHERE $d IS NULL OR b.id = $d DELETE r",
                s=src, d=dst)

    def _walk(self, node_id, rel, label, outgoing):
        arrow = "-[r]->" if outgoing else "<-[r]-"
        cy = (f"MATCH (a:Node {{id:$id}}){arrow}(n) WHERE ($rel IS NULL OR type(r)=$rel) "
              "AND ($label IS NULL OR $label IN labels(n)) RETURN n")
        return [self._node(r) for r in self._q(cy, id=node_id, rel=rel, label=label)]

    def out(self, node_id, rel=None, label=None):
        return self._walk(node_id, rel, label, True)

    def inn(self, node_id, rel=None, label=None):
        return self._walk(node_id, rel, label, False)

    def subgraph(self, project_id, limit=400):
        recs = self._q("MATCH (n:Node) WHERE n.project_id = $p OR n.id = $p RETURN n LIMIT $lim",
                       p=project_id, lim=limit)
        nodes = [self._node(r) for r in recs]
        ids = [n["id"] for n in nodes]
        rels = self._q("MATCH (a:Node)-[r]->(b:Node) WHERE a.id IN $ids AND b.id IN $ids "
                       "RETURN a.id AS s, type(r) AS t, b.id AS d", ids=ids)
        return nodes, [(r["s"], r["t"], r["d"]) for r in rels]

    def stats(self):
        nodes = self._q("MATCH (n:Node) RETURN count(n) AS c")[0]["c"]
        edges = self._q("MATCH ()-[r]->() RETURN count(r) AS c")[0]["c"]
        return {"backend": "neo4j", "nodes": nodes, "edges": edges}
