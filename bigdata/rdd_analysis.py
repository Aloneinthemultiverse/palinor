"""Historical analysis with the RDD API (map / filter / flatMap / reduceByKey / groupByKey / sortBy).

spark-submit bigdata/rdd_analysis.py
"""
from __future__ import annotations

import json
import re

from common import load_events, spark_session

STOP = {"the", "a", "of", "on", "and", "for", "to", "in", "is", "with"}
ID_PLATFORM = re.compile(r"^(local-llm|[a-z]+)")


def platform_of(lookup: dict, project_id: str, who: str) -> str:
    """PARTICIPANT_CONNECTED platform, else the id prefix (claude-…, local-llm-…, human-<project>, laya)."""
    who = (who or "").lower()
    if who in ("", "all"):
        return "broadcast" if who == "all" else "unknown"
    m = ID_PLATFORM.match(who)
    return lookup.get((project_id, who)) or (m.group(1) if m else who)


def main() -> None:
    spark = spark_session("rdd-analysis")
    spark.sparkContext.setLogLevel("WARN")
    rows = load_events(spark).select("event_type", "project_id", "participant_id", "task_id", "status",
                                     "duration_ms", "retry_count", "environment", "task_type", "platform",
                                     "payload").rdd
    rows.cache()
    print(f"\n=== PALINOR RDD analysis over {rows.count():,} events ===")

    # participant → platform lookup, shipped once to every executor as a broadcast variable
    connected = rows.filter(lambda r: r.event_type == "PARTICIPANT_CONNECTED" and r.platform and r.participant_id)
    plat = spark.sparkContext.broadcast(
        connected.map(lambda r: ((r.project_id, r.participant_id), r.platform)).collectAsMap())
    # project → members (connected sessions + anyone who reads messages, e.g. the dashboard human)
    roster = spark.sparkContext.broadcast(
        rows.filter(lambda r: r.event_type in ("PARTICIPANT_CONNECTED", "MESSAGE_READ") and r.participant_id)
        .map(lambda r: (r.project_id, r.participant_id)).distinct().groupByKey().mapValues(list).collectAsMap())

    # 1) event volume per type: map → reduceByKey → sortBy
    by_type = (rows.map(lambda r: (r.event_type, 1)).reduceByKey(lambda a, b: a + b)
               .sortBy(lambda kv: kv[1], ascending=False))
    print("\n[1] Event volume by type"); [print(f"  {k:<26}{v:>10,}") for k, v in by_type.take(12)]

    # 2) participant contribution: filter → map → reduceByKey → sortBy
    contrib = (rows.filter(lambda r: r.event_type in ("TASK_COMPLETED", "FINDING_ADDED", "PROPOSAL_CREATED",
                                                     "SYNTHESIS_CREATED") and r.participant_id)
               .map(lambda r: (platform_of(plat.value, r.project_id, r.participant_id), 1))
               .reduceByKey(lambda a, b: a + b).sortBy(lambda kv: -kv[1]))
    print("\n[2] Contributions by platform"); [print(f"  {k:<26}{v:>10,}") for k, v in contrib.collect()]

    # 3) failure distribution per environment: (env, (failed, total)) via reduceByKey
    terminal = rows.filter(lambda r: r.event_type in ("TASK_COMPLETED", "TASK_FAILED") and r.environment)
    fail = (terminal.map(lambda r: (r.environment, (int(r.event_type == "TASK_FAILED"), 1)))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .map(lambda kv: (kv[0], kv[1][0] / kv[1][1], kv[1][1])).sortBy(lambda x: -x[1]))
    print("\n[3] Failure rate by execution environment")
    for env, rate, n in fail.collect():
        print(f"  {env:<12} {rate:6.1%}  (n={n:,})")

    # 4) per-task retry history: groupByKey (bounded group size: retries per task)
    retries = (rows.filter(lambda r: r.event_type == "EXECUTION_FAILED" and r.task_id)
               .map(lambda r: (r.task_id, r.retry_count or 0)).groupByKey()
               .mapValues(lambda xs: max(xs) + 1).map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b)
               .sortBy(lambda kv: kv[0]))
    print("\n[4] Tasks by number of failed attempts"); [print(f"  {k} attempt(s): {v:,}") for k, v in retries.collect()]

    # 5) median-ish runtime per task type using groupByKey on a sample
    durations = (rows.filter(lambda r: r.event_type == "TASK_COMPLETED" and r.duration_ms and r.task_type)
                 .map(lambda r: (r.task_type, r.duration_ms / 60000)).groupByKey()
                 .mapValues(lambda xs: sorted(xs)).mapValues(lambda xs: (xs[len(xs) // 2], len(xs)))
                 .sortBy(lambda kv: -kv[1][0]))
    print("\n[5] Median task runtime (minutes)")
    for t, (med, n) in durations.collect():
        print(f"  {t:<15}{med:8.1f}  (n={n:,})")

    # 6) vocabulary of findings: flatMap over semi-structured payload text
    words = (rows.filter(lambda r: r.event_type == "FINDING_ADDED" and r.payload)
             .flatMap(lambda r: re.findall(r"[a-z_]{3,}", (json.loads(r.payload).get("claim") or "").lower()))
             .filter(lambda w: w not in STOP).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
             .sortBy(lambda kv: -kv[1]))
    print("\n[6] Top terms in findings"); print("  " + ", ".join(f"{w}({c})" for w, c in words.take(12)))

    # 7) platform → platform message counts: flatMap over each message's recipients → reduceByKey
    msgs = (rows.filter(lambda r: r.event_type == "MESSAGE_SENT" and r.payload)
            .map(lambda r: (r.project_id, r.participant_id, json.loads(r.payload).get("to") or []))
            .cache())
    edges = (msgs.flatMap(lambda m: [((platform_of(plat.value, m[0], m[1]), platform_of(plat.value, m[0], to)), 1)
                                     for to in m[2]])
             .reduceByKey(lambda a, b: a + b).sortBy(lambda kv: -kv[1]))
    print(f"\n[7] Messages platform → platform ({msgs.count():,} messages; 'broadcast' = to all)")
    for (src, dst), n in edges.take(15):
        print(f"  {src:>10} → {dst:<10}{n:>9,}")

    # 8) delivery fan-out: a broadcast reaches every other member → flatMap expands it per recipient
    def deliveries(m):
        pid, sender, to = m
        targets = [x for x in roster.value.get(pid, []) if x != sender] if "all" in to else to
        return [(platform_of(plat.value, pid, x), 1) for x in targets]

    fanout = msgs.flatMap(deliveries).reduceByKey(lambda a, b: a + b).sortBy(lambda kv: -kv[1])
    print("\n[8] Message deliveries by recipient platform (broadcasts fanned out to every project member)")
    for p_, n in fanout.collect():
        print(f"  {p_:<12}{n:>10,}")
    spark.stop()


if __name__ == "__main__":
    main()
