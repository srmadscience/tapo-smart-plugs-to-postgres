"""Self-healing for the Connect JDBC sinks: restart any FAILED tapo sink.

Why: the Confluent JDBC sink (10.8.x) fails its task permanently when it
cannot open a database connection -- ``connection.attempts`` (5 x 30s here)
then a non-retriable ConnectException. ``max.retries`` only covers errors on
an already-open connection. So any PostgreSQL outage longer than ~2.5 minutes
leaves the sinks FAILED until someone restarts them (velop's sat FAILED for
thirteen days once). Each watcher run therefore checks the tapo sinks and
restarts failed ones, so data flows again within one timer tick of PostgreSQL
returning. A restarted task resumes from its last committed offset; the
deterministic-id upserts make the re-delivery harmless.

Best-effort: any REST failure is reported and ignored.
"""

from __future__ import annotations

from .schema import TABLES


def sink_names() -> list[str]:
    """Connector names, as in connect/tapo-sink-<table>-postgres.json."""
    return [f"postgres-jdbc-sink-tapo-{t.name.replace('_', '-')}" for t in TABLES]


def failed_parts(status: dict) -> list[str]:
    """Which parts of a ``GET /connectors/<name>/status`` body are FAILED."""
    parts = []
    if (status.get("connector") or {}).get("state") == "FAILED":
        parts.append("connector")
    parts += [f"task {t.get('id')}" for t in status.get("tasks") or []
              if t.get("state") == "FAILED"]
    return parts


def heal(connect_url: str, timeout: float = 10.0, log=print) -> int:
    """Restart FAILED tapo sinks (connector + failed tasks). Returns # restarted."""
    import requests

    base = connect_url.rstrip("/")
    restarted = 0
    for name in sink_names():
        try:
            resp = requests.get(f"{base}/connectors/{name}/status", timeout=timeout)
            if resp.status_code == 404:
                log(f"warn: sink {name} is not registered (run connect/install-sinks.sh)")
                continue
            resp.raise_for_status()
            parts = failed_parts(resp.json())
            if not parts:
                continue
            requests.post(f"{base}/connectors/{name}/restart",
                          params={"includeTasks": "true", "onlyFailed": "true"},
                          timeout=timeout).raise_for_status()
            restarted += 1
            log(f"Restarted FAILED sink {name} ({', '.join(parts)})")
        except Exception as exc:  # noqa: BLE001 -- best-effort
            log(f"note: could not check/restart sink {name}: {exc}")
    return restarted
