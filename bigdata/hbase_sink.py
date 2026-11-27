"""Kafka → HBase sink: wide-column persistence of events, snapshots and Spark metrics.

Row-key design (avoids hot-spotting on a monotonically increasing key):
    <salt:2>#PROJECT#<project_id>#<KIND>#<...>
    salt = crc32(project_id) % 16  → writes spread over 16 key ranges (regions as tables split),
    yet every row of one project stays contiguous for a prefix scan.
Writes are idempotent (row key derived from event_id / entity id), so Kafka
at-least-once redelivery cannot create duplicates.

Collaboration rows (all keys behind the <salt>#PROJECT#<project_id># prefix):
  context_history  MSG#<thread_id>#<ts>#<message_id>   one row per message; a prefix scan on
                                                       MSG#<thread_id> returns the thread in order
                   RCPT#<message_id>                   receipts: sender, sent_at and one dynamic
                                                       column meta:read:<reader> = read time per reader
                   CTX#<ts>#<event_id>                 context timeline (findings, context shares,
                                                       chat ingestions, proposals, ...)
                   CONV#<conversation_id>              ingested chat: kind, size, what was extracted
  decisions        DEBATE#<qid>                        debate header: mode, status, options, votes
                                                       (d:vote:<proposal>:<voter>), Laya outcomes
                   DEBATE#<qid>#<ts>#<event_id>        debate timeline (open → perspectives → votes →
                                                       Laya → synthesis → decision); prefix scan DEBATE#<qid>
                   DECISION#<decision_id>              committed decision
  qid = sha1 of the normalised question: Laya verdicts, votes and decisions carry the question, not
  the debate id. Header columns that change (d:status) are safe because the partition key is the
  project id, so a project's events are consumed in the order they were produced.

  python -m bigdata.hbase_sink --bootstrap localhost:9092 --hbase localhost
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
import zlib
from datetime import datetime

log = logging.getLogger("palinor.hbase")
SALTS = 16
TABLES = {
    "projects": ["metadata"],
    "participants": ["identity", "status"],
    "tasks": ["task", "state", "ts"],
    "commands": ["cmd", "state"],
    "events": ["e", "p"],
    "context_history": ["content", "meta"],
    "decisions": ["d"],
    "execution_history": ["execution", "result"],
    "analytics": ["m"],
}


def salt(project_id: str) -> str:
    return f"{zlib.crc32(project_id.encode()) % SALTS:02d}"


def key(pid: str, *parts: str) -> bytes:
    return "#".join([salt(pid), "PROJECT", pid, *parts]).encode()


def ts_ms(iso: str) -> int:
    try:
        return int(datetime.fromisoformat(iso).timestamp() * 1000)
    except (TypeError, ValueError):
        return 0


def b(v) -> bytes:
    return (v if isinstance(v, str) else json.dumps(v, default=str)).encode()


def qid(question: str) -> str:
    """Stable debate key: same normalisation as the backend's qkey()."""
    return hashlib.sha1(re.sub(r"\s+", " ", question.strip().lower()).encode()).hexdigest()[:12]


DEBATE_PREFIXES = ("DEBATE_", "PROPOSAL_", "SYNTHESIS_", "VOTE_", "LAYA_", "DECISION_", "VERIFICATION_",
                   "CONTRADICTION_")


def write_message(batches: dict, ev: dict, payload: dict, t: int) -> None:
    """MESSAGE_SENT / MESSAGE_READ / CONTEXT_SHARED / CONVERSATION_INGESTED → context_history."""
    pid, et, eid, at = ev["project_id"], ev["event_type"], ev["event_id"], ev["occurred_at"]
    actor = ev.get("participant_id") or ""
    ctx = batches["context_history"]
    if et == "MESSAGE_SENT" and payload.get("message_id"):
        mid = payload["message_id"]
        thread = payload.get("thread_id") or mid
        ctx.put(key(pid, "MSG", thread, f"{t:013d}", mid), {
            b"content:type": b(et), b"content:kind": b(payload.get("kind", "")),
            b"content:subject": b(payload.get("subject", "")), b"content:chars": b(payload.get("chars", 0)),
            b"content:to": b(payload.get("to", [])), b"content:has_context": b(payload.get("has_context", False)),
            b"meta:author": b(actor), b"meta:message_id": b(mid), b"meta:thread": b(thread),
            b"meta:sent_at": b(at)})
        ctx.put(key(pid, "RCPT", mid), {b"meta:sender": b(actor), b"meta:sent_at": b(at), b"meta:thread": b(thread),
                                        b"meta:kind": b(payload.get("kind", "")), b"meta:to": b(payload.get("to", []))})
        if actor and actor not in ("laya", "system", "user"):
            batches["participants"].put(key(pid, "PARTICIPANT", actor), {b"status:last_message_at": b(at)})
    elif et == "MESSAGE_READ" and payload.get("message_id") and actor:
        ctx.put(key(pid, "RCPT", payload["message_id"]), {f"meta:read:{actor}".encode(): b(at)})
    elif et in ("CONTEXT_SHARED", "CONVERSATION_INGESTED"):
        ctx.put(key(pid, "CTX", f"{t:013d}", eid), {
            b"content:type": b(et), b"content:data": b(payload), b"meta:author": b(actor),
            b"meta:task": b(ev.get("task_id") or ""), b"meta:to": b(payload.get("to", []))})
        if et == "CONVERSATION_INGESTED" and payload.get("conversation_id"):
            cols = {f"content:{k}".encode(): b(payload[k]) for k in
                    ("kind", "chars", "findings", "agreements", "open_questions", "tasks") if k in payload}
            ctx.put(key(pid, "CONV", payload["conversation_id"]),
                    cols | {b"meta:author": b(actor), b"meta:ingested_at": b(at)})


def write_debate(batches: dict, ev: dict, payload: dict, t: int) -> None:
    """Debate lifecycle → decisions: header row (state) + one timeline row per event."""
    pid, et, eid, at = ev["project_id"], ev["event_type"], ev["event_id"], ev["occurred_at"]
    actor = ev.get("participant_id") or ""
    q = qid(payload["question"])
    dec = batches["decisions"]
    dec.put(key(pid, "DEBATE", q, f"{t:013d}", eid), {
        b"d:type": b(et), b"d:actor": b(actor), b"d:at": b(at), b"d:payload": b(payload),
        b"d:target": b(ev.get("target_participant_id") or "")})
    head = {b"d:question": b(payload["question"])}
    if et == "DEBATE_OPENED":
        head |= {b"d:debate_id": b(payload.get("debate_id", "")), b"d:mode": b(payload.get("mode", "")),
                 b"d:opened_by": b(actor), b"d:opened_at": b(at), b"d:status": b"open"}
    elif et in ("PROPOSAL_CREATED", "SYNTHESIS_CREATED") and payload.get("proposal_id"):
        head[f"d:option:{payload['proposal_id']}".encode()] = b(
            {"author": actor, "kind": payload.get("kind", "perspective"),
             "derived_from": payload.get("derived_from", []), "at": at})
    elif et == "VOTE_CAST" and payload.get("proposal_id"):
        head[f"d:vote:{payload['proposal_id']}:{actor}".encode()] = b(payload.get("value", ""))
    elif et.startswith("LAYA_"):
        head |= {f"d:outcome:{t:013d}".encode(): b(et[5:]), b"d:last_outcome": b(et[5:])}
        if et == "LAYA_ESCALATE":
            head[b"d:status"] = b"escalated"
    elif et == "SYNTHESIS_REQUESTED":
        head |= {b"d:synthesis_pair": b(payload.get("pair", [])),
                 b"d:synthesis_owner": b(ev.get("target_participant_id") or "")}
    elif et == "VERIFICATION_REQUESTED":
        head |= {b"d:verification_task": b(ev.get("task_id") or ""), b"d:last_outcome": b"VERIFY"}
    elif et == "CONTRADICTION_CREATED":
        head[b"d:contradiction_id"] = b(payload.get("contradiction_id", ""))
    elif et == "DECISION_COMMITTED":
        head |= {b"d:status": b"decided", b"d:decision_id": b(payload.get("decision_id", "")),
                 b"d:selected": b(payload.get("selected", "")), b"d:decided_at": b(at),
                 b"d:synthesis": b(payload.get("synthesis", False)), b"d:tiebreak": b(payload.get("tiebreak", False)),
                 b"d:last_outcome": b"TIEBREAK" if payload.get("tiebreak") else b"DECIDED"}
    dec.put(key(pid, "DEBATE", q), head)


def ensure_tables(conn) -> None:
    existing = {t.decode() for t in conn.tables()}
    for name, families in TABLES.items():
        if name not in existing:
            conn.create_table(name, {f: dict(max_versions=3) for f in families})
            log.info("created HBase table %s", name)


def write_event(batches: dict, ev: dict) -> None:
    pid, et, eid = ev["project_id"], ev["event_type"], ev["event_id"]
    payload = ev.get("payload") or {}
    t = ts_ms(ev.get("occurred_at"))
    cols = {f"e:{k}".encode(): b(v) for k, v in ev.items() if k != "payload" and v is not None}
    cols.update({f"p:{k}".encode(): b(v) for k, v in payload.items() if v is not None})
    batches["events"].put(key(pid, "EVENT", f"{t:013d}", eid), cols)

    if et == "PROJECT_CREATED":
        batches["projects"].put(key(pid), {b"metadata:name": b(payload.get("name", "")),
                                           b"metadata:created_at": b(ev["occurred_at"])})
    elif et.startswith("PARTICIPANT_") and ev.get("participant_id"):
        batches["participants"].put(key(pid, "PARTICIPANT", ev["participant_id"]), {
            b"identity:platform": b(payload.get("platform", "")),
            b"status:state": b(et.split("_", 1)[1].lower()), b"status:last_seen": b(ev["occurred_at"])})
    if et.startswith("TASK_") and ev.get("task_id"):
        cols = {b"state:status": b(ev.get("status") or et[5:]), b"state:last_event": b(et),
                f"ts:{et.lower()}".encode(): b(ev["occurred_at"])}
        for k in ("task_type", "environment", "complexity", "title"):
            if k in payload:
                cols[f"task:{k}".encode()] = b(payload[k])
        if ev.get("duration_ms") is not None:
            cols[b"state:duration_ms"] = b(ev["duration_ms"])
        if ev.get("participant_id"):
            cols[b"state:participant"] = b(ev["participant_id"])
        batches["tasks"].put(key(pid, "TASK", ev["task_id"]), cols)
    elif et.startswith("COMMAND_") and ev.get("command_id"):
        batches["commands"].put(key(pid, "CMD", ev["command_id"]), {
            b"cmd:target": b(ev.get("target_participant_id") or ""), b"cmd:task": b(ev.get("task_id") or ""),
            b"state:status": b(ev.get("status") or et[8:]), f"state:{et[8:].lower()}_at".encode(): b(ev["occurred_at"]),
            b"state:retry_count": b(ev.get("retry_count") or 0)})
    elif et in ("FINDING_ADDED", "CONTEXT_UPDATED", "EVIDENCE_ADDED", "PROPOSAL_CREATED", "ARGUMENT_ADDED"):
        batches["context_history"].put(key(pid, "CTX", f"{t:013d}", eid), {
            b"content:type": b(et), b"content:data": b(payload),
            b"meta:author": b(ev.get("participant_id") or ""), b"meta:task": b(ev.get("task_id") or "")})
    elif et == "DECISION_COMMITTED":
        batches["decisions"].put(key(pid, "DECISION", payload.get("decision_id") or eid),
                                 {f"d:{k}".encode(): b(v) for k, v in payload.items()} |
                                 {b"d:committed_at": b(ev["occurred_at"])})
    elif et.startswith(("EXPERIMENT_", "EXECUTION_")):
        batches["execution_history"].put(key(pid, "EXEC", f"{t:013d}", eid), {
            b"execution:type": b(et), b"execution:status": b(ev.get("status") or ""),
            b"execution:runtime_ms": b(ev.get("duration_ms") or 0),
            b"execution:environment": b(payload.get("environment", "")),
            b"execution:commit_sha": b(ev.get("commit_sha") or ""), b"result:payload": b(payload)})
    if et.startswith(("MESSAGE_", "CONTEXT_SHARED", "CONVERSATION_")):
        write_message(batches, ev, payload, t)
    if et.startswith(DEBATE_PREFIXES) and payload.get("question"):
        write_debate(batches, ev, payload, t)


def write_metric(batches: dict, m: dict) -> None:
    pid = m["project_id"]
    batches["analytics"].put(key(pid, "WINDOW", m["window_start"], m["event_type"]),
                             {f"m:{k}".encode(): b(v) for k, v in m.items() if v is not None})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--hbase", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--group", default="palinor-hbase-sink")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import happybase
    from kafka import KafkaConsumer
    from thriftpy2.transport import TTransportException

    from palinor.events import ALL_TOPICS

    conn = happybase.Connection(a.hbase, port=a.port, autoconnect=True)
    ensure_tables(conn)
    consumer = KafkaConsumer(*ALL_TOPICS, "analytics-events", bootstrap_servers=a.bootstrap, group_id=a.group,
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             value_deserializer=lambda v: v)
    total = bad = 0
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=5000)
        if not polled:
            continue
        for attempt in range(5):  # HBase Thrift drops idle connections: reconnect and redo the batch
            try:
                batches = {t: conn.table(t).batch(batch_size=1000) for t in TABLES}
                n_ok = n_bad = 0
                for tp, records in polled.items():
                    for rec in records:
                        try:
                            msg = json.loads(rec.value)
                            if tp.topic == "analytics-events":
                                write_metric(batches, msg)
                            elif msg.get("event_id") and msg.get("event_type") and msg.get("project_id"):
                                write_event(batches, msg)
                            else:
                                n_bad += 1
                        except (ValueError, KeyError, TypeError):
                            n_bad += 1
                        n_ok += 1
                for bt in batches.values():
                    bt.send()
                break
            except Exception as exc:  # dropped connection, or regions still coming online after a restart
                if not isinstance(exc, (TTransportException, OSError)) and "NotServingRegion" not in str(exc)                         and "MasterNotRunning" not in str(exc) and "RegionOffline" not in str(exc):
                    raise
                log.warning("HBase connection lost (%s); reconnecting (attempt %d)", exc, attempt + 1)
                time.sleep(10 * (attempt + 1))
                conn = happybase.Connection(a.hbase, port=a.port, autoconnect=True)
        else:
            raise RuntimeError("HBase unreachable after 5 attempts; offsets not committed")
        total, bad = total + n_ok, bad + n_bad
        consumer.commit()  # commit only after HBase accepted the batch (idempotent puts make redo safe)
        log.info("persisted %s records (%s rejected as malformed)", f"{total:,}", f"{bad:,}")


if __name__ == "__main__":
    main()
