"""PALINOR runtime configuration (environment-driven)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


@dataclass
class Config:
    data_dir: Path = field(default_factory=lambda: Path(_env("PALINOR_DATA_DIR", "data")))
    graph_backend: str = field(default_factory=lambda: _env("PALINOR_GRAPH", "memory"))  # memory | neo4j
    neo4j_uri: str = field(default_factory=lambda: _env("NEO4J_URI", "bolt://localhost:7687"))
    neo4j_user: str = field(default_factory=lambda: _env("NEO4J_USER", "neo4j"))
    neo4j_password: str = field(default_factory=lambda: _env("NEO4J_PASSWORD", "palinor-dev-pass"))
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", ""))  # empty = Kafka off
    server_key: str = field(default_factory=lambda: _env("PALINOR_SERVER_KEY", ""))
    host: str = field(default_factory=lambda: _env("PALINOR_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(_env("PALINOR_PORT", "8000")))
    heartbeat_timeout_s: int = field(default_factory=lambda: int(_env("PALINOR_HEARTBEAT_TIMEOUT", "300")))
    command_ttl_s: int = field(default_factory=lambda: int(_env("PALINOR_COMMAND_TTL", "3600")))
    max_retries: int = field(default_factory=lambda: int(_env("PALINOR_MAX_RETRIES", "2")))
    human_approval_required: bool = field(
        default_factory=lambda: _env("PALINOR_HUMAN_APPROVAL", "false").lower() == "true")

    def __post_init__(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
