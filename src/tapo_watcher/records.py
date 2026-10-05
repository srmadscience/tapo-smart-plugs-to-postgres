"""Turn one plug's poll result into table rows (pure; no network, no Kafka).

A poll result (built by ``poll.poll_plug``) is a dict::

    {"plug_name", "ip", "status", "error",
     "device_info", "device_info_raw", "energy_usage", "device_usage",
     "energy_hourly", "power_5min"}

where the API sections are the ``tapo`` responses' ``to_dict()`` output (or
None when that call was skipped/failed). Datetimes may arrive either as
``datetime`` objects or ISO strings, so both are accepted.

Units, per the tapo stubs: ``energy_usage.current_power`` is **milliwatts**,
energies are Wh, runtimes and time_usage are minutes, ``power_5min`` entries
are W, history ``start_date_time`` values are UTC.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone


def utc(value) -> datetime | None:
    """Coerce a datetime or ISO string to an aware UTC datetime (naive = UTC)."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso(ts: datetime) -> str:
    """Stable second-resolution UTC string used inside deterministic ids."""
    return utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_mac(mac: str | None) -> str | None:
    """``18-69-45-c1-65-39`` -> ``18:69:45:C1:65:39``."""
    if not mac:
        return None
    return mac.replace("-", ":").upper()


def _str(value) -> str | None:
    """Enums (OverheatStatus etc.) and anything else -> plain text, None kept."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _get(d: dict | None, *path):
    for key in path:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def device_row(result: dict, fetched_at: datetime) -> dict | None:
    info = result.get("device_info")
    if not info:
        return None
    mac = normalize_mac(info.get("mac"))
    return {
        "id": mac,
        "fetched_at": fetched_at,
        "plug_name": result["plug_name"],
        "mac": mac,
        "ip": info.get("ip") or result.get("ip"),
        "device_id": info.get("device_id"),
        "model": info.get("model"),
        "type": info.get("type"),
        "hw_ver": info.get("hw_ver"),
        "fw_ver": info.get("fw_ver"),
        "hw_id": info.get("hw_id"),
        "fw_id": info.get("fw_id"),
        "oem_id": info.get("oem_id"),
        "specs": info.get("specs"),
        "region": info.get("region"),
        "lang": info.get("lang"),
        "nickname": info.get("nickname"),
        "ssid": info.get("ssid"),
        "rssi": info.get("rssi"),
        "signal_level": info.get("signal_level"),
        "device_on": info.get("device_on"),
        "on_time": info.get("on_time"),
        "overheat_status": _str(info.get("overheat_status")),
        "overcurrent_status": _str(info.get("overcurrent_status")),
        "power_protection_status": _str(info.get("power_protection_status")),
        "charging_status": _str(info.get("charging_status")),
        "time_diff": info.get("time_diff"),
        "info_json": result.get("device_info_raw"),
    }


def reading_row(result: dict, fetched_at: datetime) -> dict:
    """Always produced -- a failed poll still yields a status/error row."""
    info = result.get("device_info") or {}
    energy = result.get("energy_usage") or {}
    usage = result.get("device_usage") or {}
    return {
        "id": f"{result['plug_name']}|{iso(fetched_at)}",
        "fetched_at": fetched_at,
        "plug_name": result["plug_name"],
        "ip": result.get("ip"),
        "mac": normalize_mac(info.get("mac")),
        "status": result.get("status"),
        "error": result.get("error"),
        "device_on": info.get("device_on"),
        "on_time_s": info.get("on_time"),
        "rssi": info.get("rssi"),
        "signal_level": info.get("signal_level"),
        "current_power_mw": energy.get("current_power"),
        "today_energy_wh": energy.get("today_energy"),
        "month_energy_wh": energy.get("month_energy"),
        "today_runtime_min": energy.get("today_runtime"),
        "month_runtime_min": energy.get("month_runtime"),
        "time_usage_today_min": _get(usage, "time_usage", "today"),
        "time_usage_past7_min": _get(usage, "time_usage", "past7"),
        "time_usage_past30_min": _get(usage, "time_usage", "past30"),
        "power_usage_today_wh": _get(usage, "power_usage", "today"),
        "power_usage_past7_wh": _get(usage, "power_usage", "past7"),
        "power_usage_past30_wh": _get(usage, "power_usage", "past30"),
        "saved_power_today_wh": _get(usage, "saved_power", "today"),
        "saved_power_past7_wh": _get(usage, "saved_power", "past7"),
        "saved_power_past30_wh": _get(usage, "saved_power", "past30"),
    }


def _history_rows(result: dict, section: str, value_key: str, out_key: str,
                  fetched_at: datetime, mac: str | None) -> list[dict]:
    """Rows for one history section. Skipped: entries with no data (None), so an
    empty slot never overwrites a value an earlier run already landed; and
    entries starting after ``fetched_at`` -- the plug pads hourly energy out to
    the end of its local day with 0s (seen on a P110M, fw 1.4.3), which are not
    real readings. The in-progress slot is kept; later runs overwrite it."""
    data = result.get(section)
    if not data or not mac:
        return []
    rows = []
    for entry in data.get("entries") or []:
        value = entry.get(value_key)
        start = utc(entry.get("start_date_time"))
        if value is None or start > fetched_at:
            continue
        rows.append({
            "id": f"{mac}|{iso(start)}",
            "fetched_at": fetched_at,
            "plug_name": result["plug_name"],
            "mac": mac,
            "interval_start": start,
            out_key: value,
        })
    return rows


def build_rows(result: dict, fetched_at: datetime) -> dict[str, list[dict]]:
    """``{table_name: [row, ...]}`` for one plug's poll result."""
    mac = normalize_mac(_get(result, "device_info", "mac"))
    device = device_row(result, fetched_at)
    return {
        "device": [device] if device else [],
        "reading": [reading_row(result, fetched_at)],
        "energy_hourly": _history_rows(
            result, "energy_hourly", "energy", "energy_wh", fetched_at, mac),
        "power_5min": _history_rows(
            result, "power_5min", "power", "power_w", fetched_at, mac),
    }


def merge_rows(per_plug: list[dict[str, list[dict]]]) -> dict[str, list[dict]]:
    merged: dict[str, list[dict]] = {}
    for rows in per_plug:
        for table, recs in rows.items():
            merged.setdefault(table, []).extend(recs)
    return merged
