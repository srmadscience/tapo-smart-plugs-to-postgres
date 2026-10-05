import io
import json
from pathlib import Path

import fastavro

from conftest import FETCHED_AT
from tapo_watcher.kafka_sink import messages_for, value_schema
from tapo_watcher.records import build_rows
from tapo_watcher.schema import TABLES, TABLES_BY_NAME, postgres_ddl

ROOT = Path(__file__).resolve().parent.parent


def test_checked_in_ddl_is_current():
    checked_in = (ROOT / "sql" / "tapo_schema_postgres.sql").read_text()
    assert checked_in == postgres_ddl(), (
        "sql/tapo_schema_postgres.sql is stale: run "
        "python -m tapo_watcher.schema > sql/tapo_schema_postgres.sql")


def test_build_rows_keys_match_table_columns(ok_result):
    """A records.py field that isn't a schema column would silently be dropped."""
    for table, rows in build_rows(ok_result, FETCHED_AT).items():
        cols = {name for name, _ in TABLES_BY_NAME[table].all_columns}
        for row in rows:
            assert set(row) == cols, table


def test_avro_schemas_parse_and_every_message_serializes(ok_result):
    rows = build_rows(ok_result, FETCHED_AT)
    msgs = messages_for(rows, "tapo.")
    assert set(msgs) == {f"tapo.{t.name}" for t in TABLES}
    for table in TABLES:
        schema = fastavro.parse_schema(json.loads(value_schema(table)))
        for key, value in msgs[f"tapo.{table.name}"]:
            assert key == "18:69:45:C1:65:39"
            fastavro.schemaless_writer(io.BytesIO(), schema, value)


def test_json_column_is_sent_as_string(ok_result):
    msgs = messages_for(build_rows(ok_result, FETCHED_AT), "tapo.")
    (_key, device), = msgs["tapo.device"]
    assert json.loads(device["info_json"])["nickname"] == "S2V0dGxl"


def test_failed_poll_is_keyed_by_plug_name():
    rows = build_rows({"plug_name": "kettle", "ip": "x", "status": "error"}, FETCHED_AT)
    (key, _value), = messages_for(rows, "tapo.")["tapo.reading"]
    assert key == "kettle"
