"""Shared Spark helpers: session, event schema, lake paths, payload feature extraction.

Lake segments
-------------
A Structured Streaming file sink records its committed batches in <path>/_spark_metadata. A query
started with a NEW checkpoint restarts at batch 0, and the sink silently skips every batch id that
the metadata log already holds ("already committed"). So each checkpoint version owns its own lake
segment:

    PALINOR_CHECKPOINT_VERSION unset  -> events/      quarantine/      _checkpoints/
    PALINOR_CHECKPOINT_VERSION=v2     -> events_v2/   quarantine_v2/   _checkpoints_v2/

Batch jobs read every segment (events, events_*) and de-duplicate on event_id, so segments that
overlap after a restart never double-count events.
"""
from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.utils import AnalysisException

LAKE = os.getenv("PALINOR_LAKE", "data/lake")
CHECKPOINT_VERSION = os.getenv("PALINOR_CHECKPOINT_VERSION", "").strip()
_SEGMENT = f"_{CHECKPOINT_VERSION}" if CHECKPOINT_VERSION else ""
EVENTS_PATH = f"{LAKE}/events{_SEGMENT}"            # where THIS streaming run writes
QUARANTINE_PATH = f"{LAKE}/quarantine{_SEGMENT}"
CHECKPOINTS = f"{LAKE}/_checkpoints{_SEGMENT}"
MODELS_PATH = os.getenv("PALINOR_MODELS", "data/models")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
SOURCE_TOPICS = ("project-events,participant-events,task-events,command-events,context-events,"
                 "finding-events,experiment-events,execution-events,github-events,decision-events,error-events,"
                 "message-events")

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("project_id", T.StringType()),
    T.StructField("task_id", T.StringType()),
    T.StructField("participant_id", T.StringType()),
    T.StructField("occurred_at", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("seq", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("duration_ms", T.LongType()),
    T.StructField("retry_count", T.IntegerType()),
    T.StructField("result_ref", T.StringType()),
    T.StructField("commit_sha", T.StringType()),
    T.StructField("target_participant_id", T.StringType()),
    T.StructField("command_id", T.StringType()),
])


def spark_session(app: str) -> SparkSession:
    return (SparkSession.builder.appName(f"palinor-{app}")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", os.getenv("PALINOR_SHUFFLE", "12"))
            .getOrCreate())


def with_payload_features(df: DataFrame, payload_col: str = "payload") -> DataFrame:
    """Flatten the semi-structured payload into typed analytical columns."""
    pj = lambda path: F.get_json_object(F.col(payload_col), f"$.{path}")  # noqa: E731
    return (df.withColumn("task_type", pj("task_type"))
              .withColumn("environment", pj("environment"))
              .withColumn("complexity", pj("complexity").cast("int"))
              .withColumn("context_size", pj("context_size").cast("int"))
              .withColumn("platform", pj("platform"))
              .withColumn("num_dependencies", pj("num_dependencies").cast("int"))
              .withColumn("question", pj("question"))
              .withColumn("is_sim", F.coalesce(pj("sim").cast("boolean"), F.lit(False))))


def with_collab_features(df: DataFrame, payload_col: str = "payload") -> DataFrame:
    """Typed columns for the collaboration events (messages, context shares, debates, Laya, decisions).

    Derived at read time from the raw payload so every lake segment - including ones written before
    these events existed - exposes the same schema."""
    pj = lambda path: F.get_json_object(F.col(payload_col), f"$.{path}")  # noqa: E731
    return (df.withColumn("message_id", pj("message_id"))
              .withColumn("kind", pj("kind"))  # message kind | proposal kind | conversation kind
              .withColumn("thread_id", pj("thread_id"))
              .withColumn("reply_to", F.when(pj("reply_to") != "", pj("reply_to")))
              .withColumn("recipients", F.from_json(pj("to"), "array<string>"))
              .withColumn("chars", pj("chars").cast("int"))
              .withColumn("has_context", pj("has_context").cast("boolean"))
              .withColumn("debate_id", pj("debate_id"))
              .withColumn("mode", pj("mode"))
              .withColumn("proposal_id", pj("proposal_id"))
              .withColumn("vote_value", pj("value"))
              .withColumn("derived_from", F.from_json(pj("derived_from"), "array<string>"))
              .withColumn("is_synthesis", pj("synthesis").cast("boolean"))
              .withColumn("is_tiebreak", pj("tiebreak").cast("boolean"))
              # same normalisation as the backend's qkey(): debates, votes, Laya verdicts and decisions
              # are only linked through the question text
              .withColumn("question_key", F.lower(F.regexp_replace(F.trim(pj("question")), r"\s+", " ")))
              # orchestration: PLAN_SUBMITTED / PLAN_COMPLETED / TASK_DISPATCHED / TASK_REASSIGNED
              .withColumn("plan_id", pj("plan_id"))
              .withColumn("plan_tasks", F.when(F.col("event_type").startswith("PLAN_"), pj("tasks").cast("int")))
              .withColumn("capability", pj("capability"))
              .withColumn("dispatch_load", pj("load").cast("int"))
              .withColumn("reassign_reason", pj("reason"))
              .withColumn("depends_on", F.from_json(pj("depends_on"), "array<string>"))
              # conflicts: CONFLICT_RAISED / CONSULTED / OPINION / RESOLVED / WITHDRAWN
              .withColumn("conflict_id", pj("conflict_id"))
              .withColumn("conflict_about", F.from_json(pj("about"), "array<string>"))
              .withColumn("severity", pj("severity"))
              .withColumn("consulted", F.from_json(pj("asked"), "array<string>"))
              .withColumn("position", pj("position"))
              .withColumn("cites", pj("cites").cast("int"))
              .withColumn("pending", pj("pending").cast("int"))
              .withColumn("resolution", pj("resolution"))
              .withColumn("n_opinions", pj("opinions").cast("int"))
              .withColumn("against_evidence", pj("against_evidence").cast("boolean"))
              .withColumn("forced", pj("forced").cast("boolean")))


def lake_segments(kind: str = "events") -> list:
    """events/, events_v2/, ... (one per streaming checkpoint version)."""
    try:
        names = sorted(os.listdir(LAKE))
    except FileNotFoundError:
        return []
    return [f"{LAKE}/{n}" for n in names
            if (n == kind or n.startswith(kind + "_")) and os.path.isdir(f"{LAKE}/{n}")]


def load_events(spark: SparkSession) -> DataFrame:
    """Union of all lake segments, de-duplicated on event_id, with collaboration columns."""
    frames = []
    for seg in lake_segments():
        try:
            frames.append(spark.read.parquet(seg))
        except AnalysisException:  # segment created but nothing committed yet
            continue
    if not frames:
        raise RuntimeError(f"no events in the lake at {LAKE} - run stream_etl first")
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    return with_collab_features(df.dropDuplicates(["event_id"]))
