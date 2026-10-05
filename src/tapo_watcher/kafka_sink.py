"""Produce table rows to Kafka as Confluent-Avro (copied from velop-watcher).

One topic per table, ``<kafka_topic_prefix><table>`` (e.g. ``tapo.reading``),
each with an Avro value schema generated from ``schema.TABLES`` and registered
in the Schema Registry as ``<topic>-value`` on first use. The key is the plug's
MAC (or plug_name when the poll failed and no MAC is known) so a plug's rows
stay ordered on one partition.

``confluent_kafka`` is imported lazily (only when a producer is built) so the
pure schema/message helpers -- and the outbox, which buffers *logical*
messages -- import without it.
"""

from __future__ import annotations

import json
from datetime import datetime

from .schema import KINDS, SCHEMA, TABLES, Table


def value_schema(table: Table) -> str:
    """Avro value schema (JSON string). ``id`` is required; all else nullable."""
    fields = []
    for name, kind in table.all_columns:
        avro = KINDS[kind][0]
        if name == "id":
            fields.append({"name": name, "type": avro})
        else:
            fields.append({"name": name, "type": ["null", avro], "default": None})
    return json.dumps({"type": "record", "name": table.name,
                       "namespace": SCHEMA, "fields": fields})


def record_value(table: Table, row: dict) -> dict:
    """Avro value dict for one row (pure). ``json`` columns are JSON-encoded;
    ``ts`` columns stay datetimes for the ``timestamp-millis`` logical type."""
    value = {}
    for name, kind in table.all_columns:
        raw = row.get(name)
        if kind == "json" and raw is not None and not isinstance(raw, str):
            raw = json.dumps(raw, sort_keys=True, default=str)
        value[name] = raw
    return value


def messages_for(rows: dict[str, list[dict]],
                 prefix: str) -> dict[str, list[tuple[str, dict]]]:
    """``{topic: [(key, value_dict), ...]}`` for this run's rows (pure)."""
    out: dict[str, list[tuple[str, dict]]] = {}
    for table in TABLES:
        recs = rows.get(table.name) or []
        if recs:
            out[f"{prefix}{table.name}"] = [
                (r.get("mac") or r["plug_name"], record_value(table, r)) for r in recs]
    return out


class KafkaSink:
    """Confluent producer + per-topic Avro serializers."""

    def __init__(self, cfg):
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        self._cfg = cfg
        self._producer = Producer({
            "bootstrap.servers": cfg.kafka_bootstrap,
            "client.id": cfg.kafka_client_id,
        })
        registry = SchemaRegistryClient({"url": cfg.schema_registry_url})
        self._key_serializer = StringSerializer("utf_8")
        # Keyed by topic: the outbox replays by topic (filename "<topic>.<ts>").
        self._serializers = {
            f"{cfg.kafka_topic_prefix}{t.name}": AvroSerializer(registry, value_schema(t))
            for t in TABLES
        }
        # produce() is async: a broker that accepts the enqueue can still fail
        # delivery, so the outbox checks this before gzipping a drained file.
        self._delivery_errors = 0

    def kafka_up(self, timeout: float = 5.0) -> bool:
        """Best-effort check that both the broker AND the registry are reachable
        (serializing needs the registry; both live on badger)."""
        from confluent_kafka import KafkaException

        try:
            self._producer.list_topics(timeout=timeout)
        except (KafkaException, RuntimeError):
            return False
        try:
            import requests

            base = self._cfg.schema_registry_url.rstrip("/")
            requests.get(f"{base}/subjects", timeout=timeout).raise_for_status()
        except Exception:
            return False
        return True

    def reset_delivery_errors(self) -> None:
        self._delivery_errors = 0

    @property
    def delivery_errors(self) -> int:
        return self._delivery_errors

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self._delivery_errors += 1

    def produce_one(self, topic: str, key: str, value: dict) -> None:
        """Serialize (Avro) and enqueue one message."""
        from confluent_kafka.serialization import MessageField, SerializationContext

        self._producer.produce(
            topic=topic,
            key=self._key_serializer(key, SerializationContext(topic, MessageField.KEY)),
            value=self._serializers[topic](
                value, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until queued messages are delivered; returns # still in queue."""
        return self._producer.flush(timeout)
