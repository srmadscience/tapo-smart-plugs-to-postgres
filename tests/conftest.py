import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

FETCHED_AT = datetime(2026, 10, 5, 14, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def ok_result():
    """A successful poll result shaped like the tapo responses' to_dict().

    SYNTHETIC (non-zero values, None slots, datetime objects) so the unit tests
    can assert exact numbers; its shape is checked against the real capture in
    ``real_result``.
    """
    return {
        "plug_name": "kettle",
        "ip": "10.13.1.230",
        "status": "ok",
        "error": None,
        "history_ok": True,
        "device_info": {
            "device_id": "9ae579e8cb14f84066d583d99c5c4db9",
            "mac": "18-69-45-C1-65-39",
            "ip": "10.13.1.230",
            "model": "P110M",
            "type": "SMART.TAPOPLUG",
            "hw_ver": "1.0", "fw_ver": "1.2.3 Build 240101",
            "hw_id": "HW", "fw_id": "FW", "oem_id": "OEM",
            "specs": "UK", "region": "Europe/London", "lang": "en_US",
            "nickname": "Kettle", "ssid": "home",
            "rssi": -52, "signal_level": 3,
            "device_on": True, "on_time": 3600,
            "overheat_status": "Normal",
            "overcurrent_status": "Normal",
            "power_protection_status": "Normal",
            "charging_status": "Normal",
            "time_diff": 0,
        },
        "device_info_raw": {"device_on": True, "nickname": "S2V0dGxl"},
        "energy_usage": {
            "current_power": 2_350_000, "local_time": "2026-10-05T15:15:00",
            "month_energy": 4200, "month_runtime": 900,
            "today_energy": 310, "today_runtime": 55,
        },
        "device_usage": {
            "time_usage": {"today": 55, "past7": 400, "past30": 1700},
            "power_usage": {"today": 310, "past7": 2100, "past30": 9000},
            "saved_power": {"today": 0, "past7": 0, "past30": 0},
        },
        "energy_hourly": {
            "local_time": "2026-10-05T15:15:00",
            "start_date_time": "2026-10-04T23:00:00Z",
            "interval_length": 60,
            "entries": [
                {"start_date_time": "2026-10-05T13:00:00Z", "energy": 120},
                {"start_date_time": "2026-10-05T14:00:00Z", "energy": 40},
                {"start_date_time": "2026-10-05T15:00:00Z", "energy": None},
            ],
        },
        "power_5min": {
            "start_date_time": "2026-10-05T14:00:00Z",
            "end_date_time": "2026-10-05T14:15:00Z",
            "interval_length": 5,
            "entries": [
                {"start_date_time": datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc),
                 "power": 2300},
                {"start_date_time": datetime(2026, 10, 5, 14, 5, tzinfo=timezone.utc),
                 "power": 0},
                {"start_date_time": datetime(2026, 10, 5, 14, 10, tzinfo=timezone.utc),
                 "power": None},
            ],
        },
    }


# Captured from a P110M(UK), fw 1.4.3, tapo 0.11.1 at 2026-10-05 13:45:04Z via
# `tapo-discover --dump`, with identifiers replaced (tests/fixtures/p110m/).
REAL_FETCHED_AT = datetime(2026, 10, 5, 13, 45, 0, tzinfo=timezone.utc)


@pytest.fixture
def real_result():
    result = {"plug_name": "real", "ip": "192.0.2.10", "status": "ok",
              "error": None, "history_ok": True}
    for f in (FIXTURES / "p110m").glob("*.json"):
        result[f.stem] = json.loads(f.read_text())
    return result
