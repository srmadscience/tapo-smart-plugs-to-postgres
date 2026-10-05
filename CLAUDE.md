# CLAUDE.md

Guidance for Claude Code in this repo. README.md has the data flow, tables,
outage behaviour and setup; this file covers code layout and rules.

## Commands

```bash
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest                                   # all tests (pure logic only)
set -a; source .env; set +a
tapo-watcher | tapo-drain | tapo-discover <ip> [--dump DIR]
python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql   # after schema.py edits
```

## Layout (`src/tapo_watcher/`)

- `schema.py` — **single source of truth** for the data tables; generates both
  the Avro value schemas (`kafka_sink.value_schema`) and the PostgreSQL DDL.
  Unlike velop there is no separate TABLE_SPECS to keep in sync.
- `records.py` — pure: poll result (tapo `to_dict()` dicts) → table rows.
  Deterministic ids (MAC / `plug_name|fetched_at` / `mac|interval_start`) so
  re-sends upsert. History entries with `None` are skipped.
- `poll.py` — async, one `ApiClient` per plug, `asyncio.wait_for` per plug.
  Core reads decide `status`; history reads are best-effort and `last_ok`
  only advances when history landed (`history_ok`).
- `state.py` — `state/poll_state.json` (last_ok, auth_fail_at) + backfill
  window maths (hourly ≤ 8 days inclusive, 5-min ≤ 12 h).
- `plugs.py` — `tapo.plug` via psycopg, cached to `state/plugs.json`.
  The ONLY direct DB access; everything else goes through Kafka.
- `outbox.py` — copied from velop-watcher; `write_run` + `prune` added. Every
  run writes first, then drains (velop only buffered when Kafka was down).
- `kafka_sink.py` — copied from velop-watcher; key = MAC (or plug_name).
- `cli.py` — collect → write → ship → prune. Collection failures still drain.
- `discover.py` — discovery (no login) and `--dump` fixture capture.

`connect/` (4 JDBC sinks + install/status/restart scripts) and `systemd/` are
adapted from linksys-velop-watcher.

## Rules

- **Never retry a plug login in a loop** (in code, tests or by hand): plugs lock
  themselves after repeated failures. Keep the auth backoff.
- Credentials come only from the environment; never commit `.env`,
  `spike_output/`, `buffer/` or `state/` (MAC, SSID and location).
- `tapo` units: `energy_usage.current_power` is mW but `get_current_power` is W.
  History `start_date_time` values are UTC; `get_energy_data` takes plug-local dates.
- Unit tests cover pure logic only. `tests/conftest.py::ok_result` is
  SYNTHETIC (from the tapo stubs) until replaced with real `--dump` output.
