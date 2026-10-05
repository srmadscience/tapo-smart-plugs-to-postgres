"""Which plugs to poll: ``tapo.plug`` in PostgreSQL, with a local cache.

This is the watcher's ONLY direct database access. Every successful read
rewrites ``<state_dir>/plugs.json``; when PostgreSQL is unreachable the cached
list is used instead, so a database outage never stops collection.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

QUERY = ("SELECT plug_name, ip FROM tapo.plug "
         "WHERE enabled ORDER BY plug_name")


def fetch_from_postgres(dsn: str, timeout: int) -> list[dict]:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=timeout) as conn:
        rows = conn.execute(QUERY).fetchall()
    return [{"plug_name": name, "ip": ip} for name, ip in rows]


def _cache_path(state_dir: str) -> Path:
    return Path(state_dir) / "plugs.json"


def write_cache(state_dir: str, plugs: list[dict]) -> None:
    path = _cache_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_text(json.dumps(plugs, indent=2))
    tmp.replace(path)


def read_cache(state_dir: str) -> list[dict] | None:
    path = _cache_path(state_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def load_plugs(cfg, fetch=fetch_from_postgres) -> tuple[list[dict], str]:
    """Return ``(plugs, source)`` where source is ``"postgres"`` or ``"cache"``.

    Raises RuntimeError if PostgreSQL is unavailable and there is no cache.
    """
    if cfg.pg_dsn:
        try:
            plugs = fetch(cfg.pg_dsn, cfg.pg_timeout)
            write_cache(cfg.state_dir, plugs)
            return plugs, "postgres"
        except Exception as exc:  # noqa: BLE001 -- any PG failure -> cache
            print(f"note: plug list from PostgreSQL failed ({exc}); "
                  "using cached list", file=sys.stderr)
    cached = read_cache(cfg.state_dir)
    if cached is None:
        raise RuntimeError(
            "no plug list: PostgreSQL unavailable (or TAPO_PG_DSN unset) and no "
            f"cache at {_cache_path(cfg.state_dir)}")
    return cached, "cache"
