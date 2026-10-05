"""Runtime configuration, sourced entirely from environment variables.

Secrets (the Tapo account password and the PostgreSQL DSN) are never
hard-coded or persisted to disk by this project -- they must be supplied via
the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass
class Config:
    # Tapo account the plugs are registered to. Used for LOCAL login only
    # (TPAP/KLAP); both values are case-sensitive.
    tapo_username: str = ""
    tapo_password: str = ""
    # Per-plug ceiling on one poll (login + every read), seconds.
    plug_timeout: float = 60.0
    # After a credentials-type login failure, leave that plug alone for this
    # long (seconds). TPAP plugs lock themselves after repeated bad logins and
    # "retrying keeps the device locked", so a 15-min timer must not hammer them.
    auth_backoff: float = 3 * 3600.0

    # Plug list source: SELECT ... FROM tapo.plug. libpq DSN or URI. When
    # PostgreSQL is unreachable the last good list in state_dir/plugs.json is used.
    pg_dsn: str = ""
    pg_timeout: int = 10

    # Kafka / Avro (Confluent wire format) -- the only data sink. One topic per
    # table, "<prefix><table>". Connect JDBC sinks land them in PostgreSQL.
    kafka_bootstrap: str = "badger:9092"
    schema_registry_url: str = "http://badger:8081"
    kafka_topic_prefix: str = "tapo."
    kafka_client_id: str = "tapo-watcher"
    kafka_probe_timeout: float = 5.0

    # Every run's messages are written to buffer_dir FIRST (one file per topic,
    # "<topic>.<yyyymmdd_HHMMSS>"), then drained to Kafka if it is up. Sent files
    # are gzipped and kept for buffer_retention_days. drain_time_limit caps how
    # long one run spends replaying a backlog (seconds, checked between files).
    buffer_dir: str = "buffer"
    buffer_retention_days: float = 30.0
    drain_time_limit: float = 120.0

    # Local state: cached plug list + per-plug last-success / auth-failure times.
    state_dir: str = "state"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env
        d = cls()
        return cls(
            tapo_username=env.get("TAPO_USERNAME", d.tapo_username),
            tapo_password=env.get("TAPO_PASSWORD", d.tapo_password),
            plug_timeout=float(env.get("TAPO_PLUG_TIMEOUT", d.plug_timeout)),
            auth_backoff=float(env.get("TAPO_AUTH_BACKOFF", d.auth_backoff)),
            pg_dsn=env.get("TAPO_PG_DSN", d.pg_dsn),
            pg_timeout=int(env.get("TAPO_PG_TIMEOUT", d.pg_timeout)),
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", d.kafka_bootstrap),
            schema_registry_url=env.get("SCHEMA_REGISTRY_URL", d.schema_registry_url),
            kafka_topic_prefix=env.get("KAFKA_TOPIC_PREFIX", d.kafka_topic_prefix),
            kafka_client_id=env.get("KAFKA_CLIENT_ID", d.kafka_client_id),
            kafka_probe_timeout=float(
                env.get("TAPO_KAFKA_PROBE_TIMEOUT", d.kafka_probe_timeout)),
            buffer_dir=env.get("TAPO_BUFFER_DIR", d.buffer_dir),
            buffer_retention_days=float(
                env.get("TAPO_BUFFER_RETENTION_DAYS", d.buffer_retention_days)),
            drain_time_limit=float(
                env.get("TAPO_DRAIN_TIME_LIMIT", d.drain_time_limit)),
            state_dir=env.get("TAPO_STATE_DIR", d.state_dir),
        )
