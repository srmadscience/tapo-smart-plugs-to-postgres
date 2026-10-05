"""Single source of truth for the data tables.

``TABLES`` drives both the Avro value schemas (``kafka_sink``) and the
PostgreSQL DDL (``sql/tapo_schema_postgres.sql``, regenerate with
``python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql``;
``tests/test_schema.py`` fails if the checked-in file is stale).

Every data row carries ``id`` (the PRIMARY KEY the Connect JDBC sinks upsert
on), ``fetched_at`` (UTC, when this run polled) and ``plug_name``. Ids are
*deterministic* -- derived from the row's natural key, not a uuid -- so the
same fact re-sent by a later run (history backfill, outbox replay, Kafka
re-delivery) overwrites itself instead of duplicating.
"""

from __future__ import annotations

import sys

# kind -> (Avro type of the non-null branch, PostgreSQL type)
KINDS = {
    "str": ("string", "TEXT"),
    "int": ("int", "INTEGER"),
    "long": ("long", "BIGINT"),
    "double": ("double", "DOUBLE PRECISION"),
    "bool": ("boolean", "BOOLEAN"),
    # Sent as a JSON string; lands in JSONB thanks to ?stringtype=unspecified
    # on the sink's JDBC URL (PostgreSQL won't implicitly cast TEXT -> JSONB).
    "json": ("string", "JSONB"),
    "ts": ({"type": "long", "logicalType": "timestamp-millis"}, "TIMESTAMPTZ"),
}

SCHEMA = "tapo"

# Columns common to every table, ahead of the table's own.
COMMON = [("id", "str"), ("fetched_at", "ts"), ("plug_name", "str")]


class Table:
    def __init__(self, name: str, columns: list[tuple[str, str]], *,
                 doc: str, indexes: list[tuple[str, ...]] = ()):
        self.name = name
        self.columns = columns
        self.doc = doc
        self.indexes = list(indexes)

    @property
    def all_columns(self) -> list[tuple[str, str]]:
        return COMMON + self.columns


TABLES: list[Table] = [
    Table("device", [
        ("mac", "str"), ("ip", "str"), ("device_id", "str"), ("model", "str"),
        ("type", "str"), ("hw_ver", "str"), ("fw_ver", "str"), ("hw_id", "str"),
        ("fw_id", "str"), ("oem_id", "str"), ("specs", "str"), ("region", "str"),
        ("lang", "str"), ("nickname", "str"), ("ssid", "str"),
        ("rssi", "int"), ("signal_level", "int"), ("device_on", "bool"),
        ("on_time", "long"), ("overheat_status", "str"),
        ("overcurrent_status", "str"), ("power_protection_status", "str"),
        ("charging_status", "str"), ("time_diff", "int"), ("info_json", "json"),
    ], doc="Latest identity/state per plug. id = MAC, so one row per plug "
           "(upserted every run); fetched_at is when it was last seen."),
    Table("reading", [
        ("ip", "str"), ("mac", "str"),
        # 'ok', 'error' (unreachable / failed) or 'auth_backoff' (skipped).
        ("status", "str"), ("error", "str"),
        ("device_on", "bool"), ("on_time_s", "long"),
        ("rssi", "int"), ("signal_level", "int"),
        ("current_power_mw", "long"),
        ("today_energy_wh", "long"), ("month_energy_wh", "long"),
        ("today_runtime_min", "long"), ("month_runtime_min", "long"),
        ("time_usage_today_min", "long"), ("time_usage_past7_min", "long"),
        ("time_usage_past30_min", "long"),
        ("power_usage_today_wh", "long"), ("power_usage_past7_wh", "long"),
        ("power_usage_past30_wh", "long"),
        ("saved_power_today_wh", "long"), ("saved_power_past7_wh", "long"),
        ("saved_power_past30_wh", "long"),
    ], doc="One row per plug per run, including failed polls. "
           "id = plug_name|fetched_at.",
       indexes=[("plug_name", "fetched_at"), ("mac", "fetched_at")]),
    Table("energy_hourly", [
        ("mac", "str"), ("interval_start", "ts"), ("energy_wh", "long"),
    ], doc="Plug-side hourly energy history (backfills watcher outages up to "
           "8 days). id = mac|interval_start; the current hour is re-upserted "
           "as it grows.",
       indexes=[("mac", "interval_start")]),
    Table("power_5min", [
        ("mac", "str"), ("interval_start", "ts"), ("power_w", "long"),
    ], doc="Plug-side 5-minute average power history (backfills watcher "
           "outages up to 12 hours). id = mac|interval_start.",
       indexes=[("mac", "interval_start")]),
]

TABLES_BY_NAME = {t.name: t for t in TABLES}

PLUG_DDL = f"""\
-- The plug list that drives the watcher. Maintained by hand; NOT fed by Kafka.
--   INSERT INTO {SCHEMA}.plug (plug_name, ip) VALUES ('kettle', '10.13.1.230');
CREATE TABLE IF NOT EXISTS {SCHEMA}.plug (
    plug_name  TEXT PRIMARY KEY,
    ip         TEXT NOT NULL UNIQUE,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    notes      TEXT,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def postgres_ddl() -> str:
    out = [
        "-- tapo schema, PostgreSQL -- GENERATED from tapo_watcher/schema.py.",
        "-- Regenerate with:",
        "--   python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql",
        "-- Apply once (idempotent):  psql -f sql/tapo_schema_postgres.sql",
        "--",
        "-- The data tables are filled by the Connect JDBC sinks in connect/",
        "-- (auto.create=false, insert.mode=upsert, pk.fields=id).",
        "",
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};",
        "",
        PLUG_DDL,
    ]
    for t in TABLES:
        out.append(f"-- {t.doc}")
        cols = []
        for name, kind in t.all_columns:
            pg = KINDS[kind][1]
            cols.append(f"    {name} {pg} PRIMARY KEY" if name == "id"
                        else f"    {name} {pg}")
        out.append(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{t.name} (\n"
                   + ",\n".join(cols) + "\n);")
        for idx in t.indexes:
            iname = f"{t.name}_{'_'.join(idx)}_idx"
            out.append(f"CREATE INDEX IF NOT EXISTS {iname} "
                       f"ON {SCHEMA}.{t.name} ({', '.join(idx)});")
        out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    sys.stdout.write(postgres_ddl())
