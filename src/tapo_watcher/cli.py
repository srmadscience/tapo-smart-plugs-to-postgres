"""One watcher run: plug list -> poll -> rows -> outbox file -> drain to Kafka.

Runs once and exits (a systemd timer fires it every 15 minutes). The order
is the store-and-forward contract:

1. Load the plug list (PostgreSQL ``tapo.plug``, else the local cache).
2. Poll every plug concurrently.
3. ALWAYS write this run's messages to the outbox (``buffer/``) first.
4. If Kafka + Schema Registry are up, drain every pending file oldest-first
   (gzipping each on success). If not, exit 0 -- the next run catches up.
5. Prune sent archives older than the retention period.

PostgreSQL outages are absorbed downstream: Kafka retains the messages and
the Connect JDBC sinks retry until the database is back.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone

from .config import Config
from .kafka_sink import messages_for
from .outbox import Outbox, drain, prune, write_run
from .plugs import load_plugs
from .poll import poll_all
from .records import build_rows, merge_rows
from .state import PollState


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def collect(cfg: Config, outbox: Outbox, fetched_at: datetime) -> None:
    """Steps 1-3. Any failure here is logged; the drain still runs."""
    plugs, source = load_plugs(cfg)
    _log(f"{len(plugs)} plug(s) from {source}")
    if not plugs:
        return
    state = PollState(cfg.state_dir)
    results = asyncio.run(poll_all(cfg, plugs, state, fetched_at))
    state.save()
    for r in results:
        line = f"  {r['plug_name']} ({r['ip']}): {r['status']}"
        if r.get("error"):
            line += f" -- {r['error']}"
        _log(line)

    rows = merge_rows([build_rows(r, fetched_at) for r in results])
    written = write_run(outbox, messages_for(rows, cfg.kafka_topic_prefix),
                        now=fetched_at)
    _log("Wrote " + (", ".join(f"{n} {t}" for t, n in written.items())
                     or "nothing") + f" to {cfg.buffer_dir}/")


def ship(cfg: Config, outbox: Outbox) -> None:
    """Step 4: drain the outbox to Kafka if it is reachable."""
    pending = outbox.pending_files()
    if not pending:
        return
    try:
        from .kafka_sink import KafkaSink

        sink = KafkaSink(cfg)
    except ImportError:
        _log("error: confluent-kafka is not installed. pip install -e .")
        return
    if not sink.kafka_up(cfg.kafka_probe_timeout):
        _log(f"Kafka {cfg.kafka_bootstrap} unreachable; {len(pending)} file(s) "
             f"left pending in {cfg.buffer_dir}/")
        return
    try:
        sent = drain(sink, outbox, time_limit=cfg.drain_time_limit)
    except Exception as exc:  # noqa: BLE001 -- e.g. registry rejects a schema
        _log(f"error: drain failed ({type(exc).__name__}: {exc}); "
             "files left pending")
        return
    left = len(outbox.pending_files())
    _log(f"Sent {len(sent)} file(s) ({sum(sent.values())} messages) to Kafka"
         + (f"; {left} still pending" if left else ""))


def main(argv: list[str] | None = None) -> int:
    cfg = Config.from_env()
    if not cfg.tapo_username or not cfg.tapo_password:
        _log("error: TAPO_USERNAME / TAPO_PASSWORD must be set")
        return 2
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
    outbox = Outbox(cfg.buffer_dir)

    rc = 0
    try:
        collect(cfg, outbox, fetched_at)
    except Exception as exc:  # noqa: BLE001 -- still drain what's buffered
        _log(f"error: collection failed: {type(exc).__name__}: {exc}")
        rc = 1
    ship(cfg, outbox)
    pruned = prune(outbox, cfg.buffer_retention_days)
    if pruned:
        _log(f"Pruned {pruned} archive(s) older than "
             f"{cfg.buffer_retention_days:g} days")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
