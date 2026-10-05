"""Poll the plugs over the LAN with the ``tapo`` library (async, concurrent).

Each plug gets its own ``ApiClient`` and logs in ONCE per run. A failed login
is never retried within a run, and a credentials-type failure starts a
backoff (``cfg.auth_backoff``): TPAP plugs lock themselves after repeated bad
logins and retrying keeps them locked.

The core reads (device info, energy usage, device usage) decide the poll's
status; the history reads (hourly energy, 5-minute power) are best-effort and
a failure there only adds a note to ``error``.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta

from .state import PollState, hourly_start, power_5min_start

# Substrings of tapo's error text that mean "bad credentials / locked out"
# rather than "unreachable". Seen in the tapo 0.11 binary's error strings.
_AUTH_ERROR = re.compile(
    r"CREDENTIALS|HASH_MISMATCH|AUTH_ATTEMPTS_LIMIT|FORBIDDEN|"
    r"Unauthorized|password", re.IGNORECASE)


def is_auth_error(exc: BaseException) -> bool:
    return bool(_AUTH_ERROR.search(f"{type(exc).__name__}: {exc}"))


def _short(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= 500 else text[:497] + "..."


def _dict(obj):
    return obj.to_dict() if hasattr(obj, "to_dict") else obj


async def _poll(cfg, plug: dict, last_ok: datetime | None,
                now: datetime) -> dict:
    from tapo import ApiClient
    from tapo.requests import EnergyDataInterval, PowerDataInterval

    result = {**plug, "status": "error", "error": None}
    client = ApiClient(cfg.tapo_username, cfg.tapo_password,
                       timeout_s=max(1, int(cfg.plug_timeout // 3)))
    try:
        dev = await client.p110(plug["ip"])
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"login: {_short(exc)}"
        result["auth_error"] = is_auth_error(exc)
        return result

    try:
        result["device_info"] = _dict(await dev.get_device_info())
        result["device_info_raw"] = await dev.get_device_info_json()
        energy = _dict(await dev.get_energy_usage())
        if energy.get("current_power") is None:
            # energy_usage reports mW; get_current_power reports whole W.
            cp = _dict(await dev.get_current_power())
            if cp.get("current_power") is not None:
                energy["current_power"] = cp["current_power"] * 1000
        result["energy_usage"] = energy
        result["device_usage"] = _dict(await dev.get_device_usage())
    except Exception as exc:  # noqa: BLE001
        result["error"] = _short(exc)
        return result
    result["status"] = "ok"

    # History, sized from the last success. The plug's own local date anchors
    # the hourly request (that is the calendar the plug buckets by).
    notes = []
    local_now = energy.get("local_time")
    today = (datetime.fromisoformat(local_now) if isinstance(local_now, str)
             else local_now or now).date()
    try:
        result["energy_hourly"] = _dict(await dev.get_energy_data(
            EnergyDataInterval.Hourly, hourly_start(today, last_ok), today))
    except Exception as exc:  # noqa: BLE001
        notes.append(f"energy_hourly: {_short(exc)}")
    try:
        result["power_5min"] = _dict(await dev.get_power_data(
            PowerDataInterval.Every5Minutes, power_5min_start(now, last_ok), now))
    except Exception as exc:  # noqa: BLE001
        notes.append(f"power_5min: {_short(exc)}")
    if notes:
        result["error"] = "; ".join(notes)
    result["history_ok"] = not notes
    return result


async def poll_plug(cfg, plug: dict, state: PollState, now: datetime) -> dict:
    name = plug["plug_name"]
    failed = state.auth_fail_at(name)
    if failed and now - failed < timedelta(seconds=cfg.auth_backoff):
        until = failed + timedelta(seconds=cfg.auth_backoff)
        return {**plug, "status": "auth_backoff",
                "error": f"login failed at {failed.isoformat()}; not retrying "
                         f"until {until.isoformat()} (plugs lock on repeated "
                         "bad logins)"}
    try:
        return await asyncio.wait_for(
            _poll(cfg, plug, state.last_ok(name), now), cfg.plug_timeout)
    except asyncio.TimeoutError:
        return {**plug, "status": "error",
                "error": f"timed out after {cfg.plug_timeout:g}s"}


async def poll_all(cfg, plugs: list[dict], state: PollState,
                   now: datetime) -> list[dict]:
    """Poll every plug concurrently and update ``state`` (not saved here)."""
    results = await asyncio.gather(
        *(poll_plug(cfg, p, state, now) for p in plugs))
    for r in results:
        if r["status"] == "ok":
            # Only advance last_ok when the history landed too; otherwise the
            # next run re-fetches from the older last_ok and fills the gap.
            if r.get("history_ok"):
                state.mark_ok(r["plug_name"], now)
            else:
                state.clear_auth_fail(r["plug_name"])
        elif r.get("auth_error"):
            state.mark_auth_fail(r["plug_name"], now)
    return results
