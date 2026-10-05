import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

from tapo_watcher import poll
from tapo_watcher.config import Config
from tapo_watcher.plugs import load_plugs, read_cache
from tapo_watcher.state import PollState, hourly_start, power_5min_start

NOW = datetime(2026, 10, 5, 14, 15, tzinfo=timezone.utc)
TODAY = date(2026, 10, 5)


# --- backfill windows -------------------------------------------------------

def test_hourly_start_first_run_takes_full_8_days():
    assert hourly_start(TODAY, None) == date(2026, 9, 28)


def test_hourly_start_recent_success_still_covers_yesterday():
    assert hourly_start(TODAY, NOW - timedelta(minutes=15)) == date(2026, 10, 4)


def test_hourly_start_after_outage_goes_back_to_day_before_last_ok():
    assert hourly_start(TODAY, datetime(2026, 10, 1, 9, tzinfo=timezone.utc)) \
        == date(2026, 9, 30)
    # ...but never past the plug's 8-day window.
    assert hourly_start(TODAY, datetime(2026, 8, 1, tzinfo=timezone.utc)) \
        == date(2026, 9, 28)


def test_power_5min_start():
    assert power_5min_start(NOW, None) == NOW - timedelta(hours=12)
    assert power_5min_start(NOW, NOW - timedelta(minutes=15)) \
        == NOW - timedelta(minutes=45)
    assert power_5min_start(NOW, NOW - timedelta(days=2)) == NOW - timedelta(hours=12)


# --- poll state -------------------------------------------------------------

def test_poll_state_roundtrip_and_ok_clears_auth_fail(tmp_path):
    st = PollState(str(tmp_path))
    st.mark_auth_fail("kettle", NOW)
    st.save()
    st = PollState(str(tmp_path))
    assert st.auth_fail_at("kettle") == NOW
    st.mark_ok("kettle", NOW)
    assert st.auth_fail_at("kettle") is None
    assert st.last_ok("kettle") == NOW


def test_auth_backoff_skips_login(tmp_path, monkeypatch):
    cfg = Config(auth_backoff=3600)
    st = PollState(str(tmp_path))
    st.mark_auth_fail("kettle", NOW - timedelta(minutes=30))

    async def boom(*a, **k):
        raise AssertionError("must not log in during backoff")

    monkeypatch.setattr(poll, "_poll", boom)
    r = asyncio.run(poll.poll_plug(cfg, {"plug_name": "kettle", "ip": "x"}, st, NOW))
    assert r["status"] == "auth_backoff"


@pytest.mark.parametrize("msg,auth", [
    ("Tapo(ResponseError(InvalidCredentials))", True),
    ("TPAP_AUTH_ATTEMPTS_LIMIT", True),
    ("Please verify that your password is correct", True),
    ("Http(reqwest::Error { kind: Request, source: ConnectTimeout })", False),
])
def test_is_auth_error(msg, auth):
    assert poll.is_auth_error(Exception(msg)) is auth


def test_poll_all_updates_state(tmp_path, monkeypatch):
    st = PollState(str(tmp_path))
    st.mark_ok("partial", NOW - timedelta(hours=1))
    outcomes = {
        "good": {"status": "ok", "history_ok": True},
        "partial": {"status": "ok", "history_ok": False},
        "badpw": {"status": "error", "auth_error": True},
        "down": {"status": "error"},
    }

    async def fake(cfg, plug, last_ok, now):
        return {**plug, **outcomes[plug["plug_name"]]}

    monkeypatch.setattr(poll, "_poll", fake)
    plugs = [{"plug_name": n, "ip": "x"} for n in outcomes]
    asyncio.run(poll.poll_all(Config(), plugs, st, NOW))
    assert st.last_ok("good") == NOW
    assert st.last_ok("partial") == NOW - timedelta(hours=1)   # not advanced
    assert st.auth_fail_at("badpw") == NOW
    assert st.last_ok("down") is None and st.auth_fail_at("down") is None


# --- plug list --------------------------------------------------------------

PLUGS = [{"plug_name": "kettle", "ip": "10.13.1.230"}]


def test_load_plugs_from_postgres_writes_cache(tmp_path):
    cfg = Config(pg_dsn="x", state_dir=str(tmp_path))
    plugs, src = load_plugs(cfg, fetch=lambda dsn, t: PLUGS)
    assert (plugs, src) == (PLUGS, "postgres")
    assert read_cache(str(tmp_path)) == PLUGS


def test_load_plugs_falls_back_to_cache_when_postgres_down(tmp_path):
    cfg = Config(pg_dsn="x", state_dir=str(tmp_path))
    load_plugs(cfg, fetch=lambda dsn, t: PLUGS)

    def down(dsn, t):
        raise OSError("connection refused")

    assert load_plugs(cfg, fetch=down) == (PLUGS, "cache")


def test_load_plugs_without_postgres_or_cache_raises(tmp_path):
    with pytest.raises(RuntimeError, match="no plug list"):
        load_plugs(Config(state_dir=str(tmp_path)))


def test_config_from_env():
    cfg = Config.from_env({"TAPO_USERNAME": "u", "TAPO_PASSWORD": "p",
                           "TAPO_AUTH_BACKOFF": "60", "KAFKA_BOOTSTRAP": "k:1"})
    assert (cfg.tapo_username, cfg.auth_backoff, cfg.kafka_bootstrap) == ("u", 60.0, "k:1")
    assert cfg.kafka_topic_prefix == "tapo."
