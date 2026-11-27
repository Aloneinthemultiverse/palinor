"""Event bus: every state change becomes an envelope on Kafka (transport only).

Events are also appended to a local JSONL log so the backend can rebuild its
notification feed after a restart and so nothing is lost while Kafka is down.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Iterator

log = logging.getLogger("palinor.events")

TOPIC_BY_PREFIX = [
    ("PROJECT_", "project-events"),
    ("PARTICIPANT_", "participant-events"),
    ("TASK_", "task-events"),
    ("COMMAND_", "command-events"),
    ("CONTEXT_", "context-events"),
    ("CONVERSATION_", "context-events"),
    ("MESSAGE_", "message-events"),
    ("DEBATE_", "decision-events"),
    ("SYNTHESIS_", "decision-events"),
    ("LAYA_", "decision-events"),
    ("CONFLICT_", "decision-events"),
    ("PLAN_", "task-events"),
    ("FINDING_", "finding-events"),
    ("EVIDENCE_", "finding-events"),
    ("PROPOSAL_", "decision-events"),
    ("ARGUMENT_", "decision-events"),
    ("VOTE_", "decision-events"),
    ("CONTRADICTION_", "decision-events"),
    ("VERIFICATION_", "decision-events"),
    ("DECISION_", "decision-events"),
    ("EXPERIMENT_", "experiment-events"),
    ("EXECUTION_", "execution-events"),
    ("GITHUB_", "github-events"),
    ("PULL_REQUEST_", "github-events"),
    ("ERROR", "error-events"),
]
ALL_TOPICS = sorted({t for _, t in TOPIC_BY_PREFIX})


def topic_for(event_type: str) -> str:
    for prefix, topic in TOPIC_BY_PREFIX:
        if event_type.startswith(prefix):
            return topic
    return "project-events"


class EventBus:
    RECONNECT_AFTER_S = 30

    def __init__(self, bootstrap: str, log_path: Path):
        self.bootstrap = bootstrap
        self.log_path = log_path
        self._lock = threading.Lock()
        self._producer = None
        self._next_attempt = 0.0

    def _get_producer(self):
        if not self.bootstrap or self._producer is not None or time.time() < self._next_attempt:
            return self._producer
        try:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap.split(","),
                acks="all",
                retries=5,
                key_serializer=lambda k: k.encode(),
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
            )
            log.info("Kafka producer connected to %s", self.bootstrap)
        except Exception as exc:  # broker down: keep working, retry later
            log.warning("Kafka unavailable (%s); events kept in local log", exc)
            self._next_attempt = time.time() + self.RECONNECT_AFTER_S
        return self._producer

    def publish(self, event: dict) -> None:
        with self._lock:
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, default=str) + "\n")
            producer = self._get_producer()
            if producer is None:
                return
            try:
                # partition key = project_id → per-project ordering
                producer.send(topic_for(event["event_type"]), key=event["project_id"], value=event)
            except Exception as exc:
                log.warning("Kafka send failed (%s)", exc)
                self._producer = None
                self._next_attempt = time.time() + self.RECONNECT_AFTER_S

    def replay(self) -> Iterator[dict]:
        if not self.log_path.exists():
            return
        with self.log_path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=5)
