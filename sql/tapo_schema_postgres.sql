-- tapo schema, PostgreSQL -- GENERATED from tapo_watcher/schema.py.
-- Regenerate with:
--   python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql
-- Apply once (idempotent):  psql -f sql/tapo_schema_postgres.sql
--
-- The data tables are filled by the Connect JDBC sinks in connect/
-- (auto.create=false, insert.mode=upsert, pk.fields=id).

CREATE SCHEMA IF NOT EXISTS tapo;

-- The plug list that drives the watcher. Maintained by hand; NOT fed by Kafka.
--   INSERT INTO tapo.plug (plug_name, ip) VALUES ('kettle', '10.13.1.230');
CREATE TABLE IF NOT EXISTS tapo.plug (
    plug_name  TEXT PRIMARY KEY,
    ip         TEXT NOT NULL UNIQUE,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    notes      TEXT,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest identity/state per plug. id = MAC, so one row per plug (upserted every run); fetched_at is when it was last seen.
CREATE TABLE IF NOT EXISTS tapo.device (
    id TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ,
    plug_name TEXT,
    mac TEXT,
    ip TEXT,
    device_id TEXT,
    model TEXT,
    type TEXT,
    hw_ver TEXT,
    fw_ver TEXT,
    hw_id TEXT,
    fw_id TEXT,
    oem_id TEXT,
    specs TEXT,
    region TEXT,
    lang TEXT,
    nickname TEXT,
    ssid TEXT,
    rssi INTEGER,
    signal_level INTEGER,
    device_on BOOLEAN,
    on_time BIGINT,
    overheat_status TEXT,
    overcurrent_status TEXT,
    power_protection_status TEXT,
    charging_status TEXT,
    time_diff INTEGER,
    info_json JSONB
);

-- One row per plug per run, including failed polls. id = plug_name|fetched_at.
CREATE TABLE IF NOT EXISTS tapo.reading (
    id TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ,
    plug_name TEXT,
    ip TEXT,
    mac TEXT,
    status TEXT,
    error TEXT,
    device_on BOOLEAN,
    on_time_s BIGINT,
    rssi INTEGER,
    signal_level INTEGER,
    current_power_mw BIGINT,
    today_energy_wh BIGINT,
    month_energy_wh BIGINT,
    today_runtime_min BIGINT,
    month_runtime_min BIGINT,
    time_usage_today_min BIGINT,
    time_usage_past7_min BIGINT,
    time_usage_past30_min BIGINT,
    power_usage_today_wh BIGINT,
    power_usage_past7_wh BIGINT,
    power_usage_past30_wh BIGINT,
    saved_power_today_wh BIGINT,
    saved_power_past7_wh BIGINT,
    saved_power_past30_wh BIGINT
);
CREATE INDEX IF NOT EXISTS reading_plug_name_fetched_at_idx ON tapo.reading (plug_name, fetched_at);
CREATE INDEX IF NOT EXISTS reading_mac_fetched_at_idx ON tapo.reading (mac, fetched_at);

-- Plug-side hourly energy history (backfills watcher outages up to 8 days). id = mac|interval_start; the current hour is re-upserted as it grows.
CREATE TABLE IF NOT EXISTS tapo.energy_hourly (
    id TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ,
    plug_name TEXT,
    mac TEXT,
    interval_start TIMESTAMPTZ,
    energy_wh BIGINT
);
CREATE INDEX IF NOT EXISTS energy_hourly_mac_interval_start_idx ON tapo.energy_hourly (mac, interval_start);

-- Plug-side 5-minute average power history (backfills watcher outages up to 12 hours). id = mac|interval_start.
CREATE TABLE IF NOT EXISTS tapo.power_5min (
    id TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ,
    plug_name TEXT,
    mac TEXT,
    interval_start TIMESTAMPTZ,
    power_w BIGINT
);
CREATE INDEX IF NOT EXISTS power_5min_mac_interval_start_idx ON tapo.power_5min (mac, interval_start);
