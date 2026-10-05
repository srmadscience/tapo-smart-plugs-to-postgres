# tapo-smart-plugs-to-postgres

Polls Tapo P110 / P110M smart plugs every 15 minutes and lands their power,
energy and device state in PostgreSQL — via a local file outbox, Kafka and
Kafka Connect, so that an outage of Kafka or PostgreSQL loses nothing.

```
systemd timer (:00/:15/:30/:45)
  → tapo-watcher (oneshot)
      1. plug list   ← PostgreSQL tapo.plug   (PG down → state/plugs.json cache)
      2. poll every plug concurrently over the LAN (tapo library, TPAP/KLAP)
      3. ALWAYS write the run to buffer/<topic>.<utc-ts>   (JSON Lines)
      4. Kafka + registry up?  → drain every pending file oldest-first, gzip on success
                         down? → exit 0; the next run catches up
  → Kafka badger:9092 (Confluent Avro, registry badger:8081), one topic per table
  → Kafka Connect JDBC sinks (upsert on id) → PostgreSQL endowment:5433/endowment_db, schema tapo
```

Structure and the outbox are lifted from
[linksys-velop-watcher](../linksys-velop-watcher).

## Tables (`sql/tapo_schema_postgres.sql`)

| table | one row per | id (upsert key) |
|---|---|---|
| `tapo.plug` | plug to poll — **edit by hand** | `plug_name` |
| `tapo.device` | plug (latest model/firmware/Wi-Fi/state) | MAC |
| `tapo.reading` | plug per run, *including failed polls* (`status`, `error`) | `plug_name\|fetched_at` |
| `tapo.energy_hourly` | plug per hour (Wh), from the plug's own history | `mac\|interval_start` |
| `tapo.power_5min` | plug per 5 minutes (average W), from the plug's own history | `mac\|interval_start` |

Units: `current_power_mw` is milliwatts; energies are Wh; runtimes minutes;
all timestamps UTC.

## Catching up

| outage | what happens |
|---|---|
| Kafka / registry | runs keep writing `buffer/`; the first run that sees Kafka up drains the backlog (≤120 s per run, rest next run) |
| PostgreSQL (data) | Kafka retains the messages; sinks retry every 60 s (`max.retries` 100000 ≈ 69 days). Survives as long as topic retention (default 7 days) |
| PostgreSQL (plug list) | cached `state/plugs.json` is used |
| the watcher host | the plugs keep history: hourly energy for 8 days, 5-minute power for 12 hours; each run fetches back to its last success, deterministic ids make re-sends upserts |
| one plug | a `reading` row with `status='error'`; others unaffected |

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"   # Python >= 3.11
cp .env.example .env    # fill in; then:
set -a; source .env; set +a

tapo-discover 10.13.1.255                       # find plugs (no login needed)
tapo-discover 10.13.1.230 --dump spike_output   # log in once, dump every API call

psql "$TAPO_PG_DSN" -f sql/tapo_schema_postgres.sql
psql "$TAPO_PG_DSN" -c "INSERT INTO tapo.plug (plug_name, ip) VALUES ('kettle', '10.13.1.230')"

tapo-watcher                    # one run; registers the Avro schemas
./connect/install-sinks.sh      # needs PG_USER / PG_PASSWORD, curl, jq
./connect/status-sinks.sh

sudo ./systemd/install-service.sh   # on the Pi: venv + 15-min timer
```

Other commands: `tapo-drain` (just flush the outbox), `pytest`,
`./connect/restart-sinks.sh [--all]`.

## Gotchas

- **Plugs lock themselves after repeated failed logins**, and retrying keeps
  them locked. The watcher logs in once per plug per run, never retries, and
  after a credentials-type failure leaves that plug alone for
  `TAPO_AUTH_BACKOFF` seconds (default 3 h) — its readings show
  `status='auth_backoff'`. Delete its entry from `state/poll_state.json` to
  retry sooner.
- **Third-Party Compatibility** must be on in the Tapo app
  (Me → Third-Party Services) or logins are refused.
- P110M firmware uses **TPAP** login; that needs `tapo >= 0.11`.
- The sinks' `errors.tolerance=all` silently drops records PostgreSQL rejects.
  Check `connect/status-sinks.sh` and the Connect logs if rows go missing.
- `sql/tapo_schema_postgres.sql` is generated:
  `python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql`
  (a test fails if it's stale). After changing a column, keep the Avro change
  BACKWARD-compatible (add nullable fields only) or the registry rejects it.
