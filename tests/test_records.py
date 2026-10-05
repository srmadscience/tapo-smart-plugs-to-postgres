from datetime import datetime, timezone

from conftest import FETCHED_AT
from tapo_watcher.records import build_rows, merge_rows, normalize_mac, utc

MAC = "18:69:45:C1:65:39"


def test_normalize_mac():
    assert normalize_mac("18-69-45-c1-65-39") == MAC
    assert normalize_mac(None) is None


def test_utc_accepts_iso_strings_and_naive():
    want = datetime(2026, 10, 5, 13, 0, tzinfo=timezone.utc)
    assert utc("2026-10-05T13:00:00Z") == want
    assert utc("2026-10-05T14:00:00+01:00") == want
    assert utc(datetime(2026, 10, 5, 13, 0)) == want


def test_ok_result_rows(ok_result):
    rows = build_rows(ok_result, FETCHED_AT)

    (device,) = rows["device"]
    assert device["id"] == MAC
    assert device["model"] == "P110M"
    assert device["info_json"] == {"device_on": True, "nickname": "S2V0dGxl"}

    (reading,) = rows["reading"]
    assert reading["id"] == "kettle|2026-10-05T14:15:00Z"
    assert reading["status"] == "ok"
    assert reading["mac"] == MAC
    assert reading["current_power_mw"] == 2_350_000
    assert reading["today_energy_wh"] == 310
    assert reading["time_usage_past7_min"] == 400
    assert reading["power_usage_past30_wh"] == 9000


def test_history_rows_skip_empty_slots_and_use_deterministic_ids(ok_result):
    rows = build_rows(ok_result, FETCHED_AT)

    hourly = rows["energy_hourly"]
    assert [r["energy_wh"] for r in hourly] == [120, 40]          # None skipped
    assert hourly[0]["id"] == f"{MAC}|2026-10-05T13:00:00Z"
    assert hourly[0]["interval_start"] == datetime(2026, 10, 5, 13, tzinfo=timezone.utc)

    power = rows["power_5min"]
    assert [r["power_w"] for r in power] == [2300, 0]             # 0 kept
    assert power[1]["id"] == f"{MAC}|2026-10-05T14:05:00Z"

    # Same facts on a later run -> same ids (so the sink upserts, not dupes).
    later = build_rows(ok_result, datetime(2026, 10, 5, 14, 30, tzinfo=timezone.utc))
    assert [r["id"] for r in later["energy_hourly"]] == [r["id"] for r in hourly]


def test_failed_poll_still_yields_a_reading_row():
    result = {"plug_name": "kettle", "ip": "10.13.1.230",
              "status": "error", "error": "timed out after 60s"}
    rows = build_rows(result, FETCHED_AT)
    assert rows["device"] == []
    assert rows["energy_hourly"] == [] and rows["power_5min"] == []
    (reading,) = rows["reading"]
    assert reading["status"] == "error"
    assert reading["error"] == "timed out after 60s"
    assert reading["mac"] is None and reading["current_power_mw"] is None


def test_merge_rows(ok_result):
    merged = merge_rows([build_rows(ok_result, FETCHED_AT),
                         build_rows({"plug_name": "b", "ip": "x", "status": "error"},
                                    FETCHED_AT)])
    assert len(merged["reading"]) == 2
    assert len(merged["device"]) == 1


# --- real P110M capture -------------------------------------------------------

from conftest import REAL_FETCHED_AT  # noqa: E402


def test_synthetic_fixture_has_the_real_shape(ok_result, real_result):
    for section in ("device_info", "energy_usage", "device_usage"):
        assert set(ok_result[section]) <= set(real_result[section]), section


def test_real_capture_rows(real_result):
    rows = build_rows(real_result, REAL_FETCHED_AT)
    (device,) = rows["device"]
    assert device["id"] == "AA:BB:CC:00:11:22"
    assert device["model"] == "P110M"
    assert device["fw_ver"].startswith("1.4.3")
    assert device["overheat_status"] is None          # null on this firmware
    assert device["overcurrent_status"] == "normal"
    (reading,) = rows["reading"]
    assert reading["month_energy_wh"] == 82
    assert reading["time_usage_past30_min"] == 702


def test_real_hourly_drops_future_zero_padding(real_result):
    # The plug returns 192 hourly slots (8 local days) padded with 0s out to
    # 22:00Z, the end of its local day; slots after fetched_at are not readings.
    assert len(real_result["energy_hourly"]["entries"]) == 192
    hourly = build_rows(real_result, REAL_FETCHED_AT)["energy_hourly"]
    assert max(r["interval_start"] for r in hourly) == REAL_FETCHED_AT.replace(minute=0)
    assert len(hourly) == 192 - 9                     # 14Z..22Z dropped
    assert len(build_rows(real_result, REAL_FETCHED_AT)["power_5min"]) == 144
