"""Spark Structured Streaming ETL: Kafka → validate → normalize → dedupe → lake + live metrics.

  Kafka (12 topics, incl. message-events)
    → parse JSON envelope (semi-structured → typed columns)
    → schema validation; malformed rows → quarantine (never silently dropped)
    → UTC timestamp normalization, field standardization, payload flattening
    → watermark + dropDuplicatesWithinWatermark(event_id)   (at-least-once → exactly-once view)
    → sink 1: Parquet data lake, partitioned by event_date      (batch RDD / SQL / ML)
    → sink 2: 1-minute sliding-window metrics → Kafka `analytics-events` (→ HBase, dashboard)
    → sink 3: console metrics (demo)

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 bigdata/stream_etl.py

Restarting with a new topic list (or any incompatible change) needs a fresh checkpoint:
  PALINOR_CHECKPOINT_VERSION=v2       → checkpoints in _checkpoints_v2, lake segment events_v2 (see common.py)
  PALINOR_STARTING_OFFSETS=latest     → skip what earlier runs already wrote (default: earliest)
  PALINOR_RESUME_FROM=_checkpoints    → better: continue exactly where that checkpoint's last committed
                                        lake batch ended; topics it never read start at earliest.
                                        No gap and no double fill.
"""
from __future__ import annotations

import json
import os

from pyspark.sql import functions as F

from common import (CHECKPOINTS, EVENT_SCHEMA, EVENTS_PATH, KAFKA_BOOTSTRAP, LAKE, QUARANTINE_PATH,
                    SOURCE_TOPICS, spark_session, with_payload_features)

TRIGGER = os.getenv("PALINOR_TRIGGER", "10 seconds")
NEW_TOPIC_PARTITIONS = int(os.getenv("PALINOR_TOPIC_PARTITIONS", "6"))


def _batch_ids(folder: str) -> list:
    return sorted(int(n) for n in os.listdir(folder) if n.isdigit()) if os.path.isdir(folder) else []


def resume_offsets(old_checkpoints: str) -> str:
    """Kafka startingOffsets JSON = end offsets of the last batch the old lake query committed.

    Offset log format (offsets/<batch>): line 0 'v1', line 1 batch metadata, line 2.. one JSON per source.
    Spark requires every partition of every subscribed topic in the JSON; topics the old run did not
    subscribe to get -2 (earliest) for each of their PALINOR_TOPIC_PARTITIONS partitions."""
    root = old_checkpoints if old_checkpoints.startswith("/") else f"{LAKE}/{old_checkpoints}"
    committed = _batch_ids(f"{root}/events/commits")
    if not committed:
        raise RuntimeError(f"no committed batch in {root}/events - nothing to resume from")
    with open(f"{root}/events/offsets/{committed[-1]}", encoding="utf-8") as fh:
        offsets = json.loads(fh.read().splitlines()[2])
    for topic in SOURCE_TOPICS.split(","):
        offsets.setdefault(topic, {str(p): -2 for p in range(NEW_TOPIC_PARTITIONS)})
    return json.dumps(offsets, sort_keys=True)


def starting_offsets() -> str:
    resume = os.getenv("PALINOR_RESUME_FROM", "").strip()
    if resume:
        offsets = resume_offsets(resume)
        print(f"resuming from {resume}: startingOffsets={offsets}")
        return offsets
    return os.getenv("PALINOR_STARTING_OFFSETS", "earliest")


def main() -> None:
    spark = spark_session("stream-etl")
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", SOURCE_TOPICS)
           .option("startingOffsets", starting_offsets())
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", os.getenv("PALINOR_MAX_OFFSETS", "200000"))
           .load())

    parsed = (raw.select(F.col("topic"), F.col("partition"), F.col("offset"),
                         F.col("timestamp").alias("received_at"),
                         F.col("value").cast("string").alias("json"))
              .withColumn("e", F.from_json("json", EVENT_SCHEMA))
              .select("topic", "partition", "offset", "received_at", "json", "e.*")
              .withColumn("payload", F.get_json_object("json", "$.payload"))
              .withColumn("event_time", F.to_timestamp("occurred_at")))

    valid = (F.col("event_id").isNotNull() & F.col("event_type").isNotNull() &
             F.col("project_id").isNotNull() & F.col("event_time").isNotNull())

    # --- quarantine: malformed records kept for inspection / replay
    quarantine_q = (parsed.filter(~valid)
                    .select("topic", "partition", "offset", "received_at", "json")
                    .writeStream.format("parquet").option("path", QUARANTINE_PATH)
                    .option("checkpointLocation", f"{CHECKPOINTS}/quarantine")
                    .trigger(processingTime=TRIGGER).start())

    clean = (parsed.filter(valid)
             .withColumn("event_type", F.upper(F.trim("event_type")))
             .withColumn("participant_id", F.lower(F.trim("participant_id")))
             .withColumn("status", F.upper("status"))
             .withColumn("retry_count", F.coalesce("retry_count", F.lit(0)))
             .withColumn("ingest_lag_ms",
                         (F.col("received_at").cast("double") - F.col("event_time").cast("double")) * 1000)
             .withColumn("event_date", F.to_date("event_time"))
             .transform(with_payload_features)
             .drop("json"))

    deduped = (clean.withWatermark("event_time", "30 minutes")
               .dropDuplicatesWithinWatermark(["event_id"]))

    lake_q = (deduped.writeStream.format("parquet").option("path", EVENTS_PATH)
              .option("checkpointLocation", f"{CHECKPOINTS}/events")
              .partitionBy("event_date").trigger(processingTime=TRIGGER).start())

    # --- real-time windowed metrics (1-min windows sliding every 30 s)
    metrics = (deduped
               .groupBy(F.window("event_time", "1 minute", "30 seconds"), "project_id", "event_type")
               .agg(F.count("*").alias("events"),
                    F.avg("duration_ms").alias("avg_duration_ms"),
                    F.max("duration_ms").alias("max_duration_ms"),
                    F.sum("retry_count").alias("retries"),
                    F.approx_count_distinct("participant_id").alias("active_participants")))

    to_kafka = metrics.select(
        F.col("project_id").alias("key"),
        F.to_json(F.struct(F.col("window.start").cast("string").alias("window_start"),
                           F.col("window.end").cast("string").alias("window_end"),
                           "project_id", "event_type", "events", "avg_duration_ms", "max_duration_ms",
                           "retries", "active_participants")).alias("value"))
    metrics_q = (to_kafka.writeStream.format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                 .option("topic", "analytics-events")
                 .option("checkpointLocation", f"{CHECKPOINTS}/metrics")
                 .outputMode("update").trigger(processingTime=TRIGGER).start())

    # --- platform-wide health for the console (tasks, failures, command ack latency, collaboration)
    def n(event_type: str):
        return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))

    health = (deduped.groupBy(F.window("event_time", "5 minutes"))
              .agg(n("TASK_COMPLETED").alias("tasks_done"), n("TASK_FAILED").alias("tasks_failed"),
                   F.avg(F.when(F.col("event_type") == "COMMAND_ACCEPTED", F.col("duration_ms"))).alias("ack_ms"),
                   n("MESSAGE_SENT").alias("msgs"), n("MESSAGE_READ").alias("reads"),
                   n("VOTE_CAST").alias("votes"), n("DECISION_COMMITTED").alias("decisions"),
                   n("CONTRADICTION_CREATED").alias("contradictions"),
                   F.count("*").alias("events")))
    console_q = (health.writeStream.format("console").outputMode("update")
                 .option("truncate", "false").option("numRows", "8")
                 .option("checkpointLocation", f"{CHECKPOINTS}/console")
                 .trigger(processingTime=TRIGGER).start())

    print(f"PALINOR stream ETL running: {SOURCE_TOPICS} → {EVENTS_PATH} (checkpoints {CHECKPOINTS})")
    spark.streams.awaitAnyTermination()
    for q in (quarantine_q, lake_q, metrics_q, console_q):
        q.stop()


if __name__ == "__main__":
    main()
