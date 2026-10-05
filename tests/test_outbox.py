"""Tests for the store-and-forward outbox (pure file I/O + drain/buffer logic).

Ported from velop-watcher. No broker: a FakeSink stands in for KafkaSink,
exposing the interface drain uses (produce_one / flush / delivery errors);
its messages_for just fabricates per-topic messages for the drain tests.
"""

import gzip
from datetime import datetime, timezone

from tapo_watcher.outbox import Outbox, drain, prune, write_run


class FakeSink:
    """Minimal KafkaSink stand-in. Records produced messages; can be told to fail."""

    def __init__(self, prefix="tapo.", *, flush_remaining=0, delivery_errors=0):
        self.prefix = prefix
        self.produced = []            # [(topic, key, value), ...]
        self._flush_remaining = flush_remaining
        self._delivery_errors_to_report = delivery_errors
        self.delivery_errors = 0

    # --- interface the outbox relies on ---
    def messages_for(self, parsed, snapshot_id, fetched_at):
        out = {}
        for table, recs in parsed.items():
            if not recs:
                continue
            topic = f"{self.prefix}{table}"
            out[topic] = [
                (snapshot_id, {"id": r["id"], "fetched_at": fetched_at, **r})
                for r in recs
            ]
        return out

    def produce_one(self, topic, key, value):
        self.produced.append((topic, key, value))

    def reset_delivery_errors(self):
        self.delivery_errors = 0

    def flush(self, timeout=30.0):
        # Surface configured failures on the first flush of a drain batch.
        self.delivery_errors = self._delivery_errors_to_report
        return self._flush_remaining


def buffer_snapshot(sink, outbox, parsed, snapshot_id, fetched_at):
    """Test helper: write a fake run (velop's old buffer_snapshot shape)."""
    return write_run(outbox, sink.messages_for(parsed, snapshot_id, fetched_at))


def _ts(s):
    return datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)


def test_write_and_read_roundtrip_restores_datetime(tmp_path):
    outbox = Outbox(tmp_path)
    fetched = datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc)
    msgs = [("snap-1", {"id": "a", "mac": "aa", "fetched_at": fetched}),
            ("snap-1", {"id": "b", "mac": None, "fetched_at": fetched})]
    path = outbox.write("tapo.device", msgs)

    assert path.name.startswith("tapo.device.")
    assert Outbox.topic_of(path) == "tapo.device"   # topic keeps its dot
    back = Outbox.read(path)
    assert back == msgs                               # incl. fetched_at as datetime
    assert isinstance(back[0][1]["fetched_at"], datetime)


def test_filename_format_is_topic_dot_timestamp(tmp_path):
    outbox = Outbox(tmp_path)
    now = datetime(2026, 6, 25, 9, 8, 7, tzinfo=timezone.utc)
    path = outbox.write("tapo.ping", [("k", {"id": "1"})], now=now)
    assert path.name == "tapo.ping.20260625_090807"


def test_same_second_collision_gets_unique_suffix(tmp_path):
    outbox = Outbox(tmp_path)
    now = datetime(2026, 6, 25, 9, 8, 7, tzinfo=timezone.utc)
    p1 = outbox.write("tapo.ping", [("k", {"id": "1"})], now=now)
    p2 = outbox.write("tapo.ping", [("k", {"id": "2"})], now=now)
    assert p1 != p2
    assert Outbox.topic_of(p2) == "tapo.ping"        # suffix doesn't break topic
    assert {Outbox.read(p1)[0][1]["id"], Outbox.read(p2)[0][1]["id"]} == {"1", "2"}


def test_pending_files_excludes_gz_and_part(tmp_path):
    outbox = Outbox(tmp_path)
    keep = outbox.write("tapo.ping", [("k", {"id": "1"})])
    (tmp_path / "tapo.ping.20000101_000000.gz").write_bytes(b"x")
    (tmp_path / "tapo.ping.20000101_000000.part").write_text("x")
    pending = outbox.pending_files()
    assert pending == [keep]


def test_pending_files_missing_dir_is_empty(tmp_path):
    assert Outbox(tmp_path / "nope").pending_files() == []


def test_gzip_file_replaces_original_with_readable_gz(tmp_path):
    outbox = Outbox(tmp_path)
    path = outbox.write("tapo.ping", [("k", {"id": "1"})])
    raw = path.read_bytes()
    gz = Outbox.gzip_file(path)
    assert not path.exists()
    assert gz.name == path.name + ".gz"
    assert gzip.decompress(gz.read_bytes()) == raw


def test_write_run_writes_one_file_per_nonempty_topic(tmp_path):
    outbox = Outbox(tmp_path)
    fetched = _ts(None)
    counts = write_run(outbox, {
        "tapo.reading": [("aa", {"id": "1", "fetched_at": fetched})] * 2,
        "tapo.device": [],
    }, now=fetched)
    assert counts == {"tapo.reading": 2}
    names = [p.name for p in outbox.pending_files()]
    assert names == ["tapo.reading.20260625_120000"]


def test_prune_deletes_only_old_archives(tmp_path):
    import os

    outbox = Outbox(tmp_path)
    old_gz = tmp_path / "tapo.reading.20260101_000000.gz"
    new_gz = tmp_path / "tapo.reading.20260601_000000.gz"
    old_pending = tmp_path / "tapo.reading.20260101_000001"
    for p in (old_gz, new_gz, old_pending):
        p.write_text("x")
    now = 1_800_000_000.0
    os.utime(old_gz, (now - 40 * 86400,) * 2)
    os.utime(old_pending, (now - 40 * 86400,) * 2)
    os.utime(new_gz, (now - 1 * 86400,) * 2)

    assert prune(outbox, 30, now=now) == 1
    assert not old_gz.exists()
    assert new_gz.exists()
    assert old_pending.exists()          # unsent data is never pruned
    assert prune(outbox, 0, now=now) == 0


def test_drain_sends_all_then_gzips_each(tmp_path):
    sink = FakeSink()
    outbox = Outbox(tmp_path)
    buffer_snapshot(sink, outbox, {"device": [{"id": "d1"}], "ping": [{"id": "p1"}]},
                    "snap-1", _ts(0))

    sent = drain(sink, outbox)
    assert sum(sent.values()) == 2
    assert outbox.pending_files() == []                      # all drained
    # everything was replayed to the right topic
    assert {t for t, _k, _v in sink.produced} == {"tapo.device", "tapo.ping"}
    # gzipped archives remain
    assert sorted(p.suffix for p in tmp_path.iterdir()) == [".gz", ".gz"]


def test_drain_keeps_file_and_stops_on_delivery_failure(tmp_path):
    sink = FakeSink(delivery_errors=1)
    outbox = Outbox(tmp_path)
    buffer_snapshot(sink, outbox, {"device": [{"id": "d1"}]}, "snap-1", _ts(0))

    sent = drain(sink, outbox)
    assert sent == {}                                        # nothing confirmed
    assert len(outbox.pending_files()) == 1                  # left for next run
    assert not list(tmp_path.glob("*.gz"))                   # not gzipped


def test_drain_keeps_file_when_flush_leaves_queue(tmp_path):
    sink = FakeSink(flush_remaining=3)
    outbox = Outbox(tmp_path)
    buffer_snapshot(sink, outbox, {"ping": [{"id": "p1"}]}, "snap-1", _ts(0))

    sent = drain(sink, outbox)
    assert sent == {}
    assert len(outbox.pending_files()) == 1


def test_drain_stops_at_time_limit_after_first_file(tmp_path, monkeypatch):
    # Deterministic clock: time advances past the deadline right after the first
    # file, so file #1 sends but the loop stops before #2 and #3.
    clock = iter([0.0, 0.0, 100.0, 100.0])  # set-deadline, then one tick per file
    monkeypatch.setattr("time.monotonic", lambda: next(clock))
    sink = FakeSink()
    outbox = Outbox(tmp_path)
    for i in range(3):
        outbox.write("tapo.ping", [("k", {"id": str(i)})])

    sent = drain(sink, outbox, time_limit=10.0)       # deadline = 0 + 10
    assert len(sent) == 1                              # only the first file
    assert len(outbox.pending_files()) == 2           # rest still waiting


def test_drain_zero_time_limit_disables_bound(tmp_path):
    sink = FakeSink()
    outbox = Outbox(tmp_path)
    for i in range(3):
        outbox.write("tapo.ping", [("k", {"id": str(i)})])

    sent = drain(sink, outbox, time_limit=0)          # 0 => no limit
    assert len(sent) == 3
    assert outbox.pending_files() == []
