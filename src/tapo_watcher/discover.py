"""``tapo-discover``: inspect plugs, to seed ``tapo.plug`` and capture fixtures.

    tapo-discover 10.13.1.230              # unauthenticated discovery only
    tapo-discover 10.13.1.255              # broadcast: find every plug on the LAN
    tapo-discover 10.13.1.230 --dump DIR   # + log in and dump every read call

Discovery needs no credentials and prints model, MAC and login protocol
(KLAP/TPAP). ``--dump`` logs in ONCE per plug (no retry -- plugs lock on
repeated bad logins) and writes ``DIR/<ip>/<call>.json``. Those files contain
MAC/SSID/location, so keep them out of git (``spike_output/`` is ignored).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .config import Config


def _dict(obj):
    return obj.to_dict() if hasattr(obj, "to_dict") else obj


async def discover(target: str, timeout: int) -> None:
    from tapo import ApiClient

    async for item in await ApiClient.discover_devices_raw(target, timeout):
        try:
            found = item.get()
        except Exception as exc:  # noqa: BLE001
            print(f"  error: {exc}")
            continue
        res = (_dict(found).get("message") or {}).get("result") or {}
        scheme = (res.get("mgt_encrypt_schm") or {}).get("encrypt_type")
        print(f"{res.get('ip')}\t{res.get('device_model')}\t{res.get('mac')}\t"
              f"protocol={scheme}")


async def dump(cfg: Config, ip: str, out_dir: Path) -> bool:
    from tapo import ApiClient
    from tapo.requests import EnergyDataInterval, PowerDataInterval

    out = out_dir / ip
    out.mkdir(parents=True, exist_ok=True)
    try:
        dev = await ApiClient(cfg.tapo_username, cfg.tapo_password,
                              timeout_s=15).p110(ip)
    except Exception as exc:  # noqa: BLE001 -- report, never retry
        print(f"{ip}: LOGIN FAILED (not retrying): {exc}", file=sys.stderr)
        return False

    today = date.today()
    now = datetime.now(timezone.utc)
    quarter = date(today.year, 3 * ((today.month - 1) // 3) + 1, 1)
    calls = {
        "device_info": dev.get_device_info,
        "device_info_raw": dev.get_device_info_json,
        "current_power": dev.get_current_power,
        "energy_usage": dev.get_energy_usage,
        "device_usage": dev.get_device_usage,
        "component_list": dev.get_component_list,
        "energy_hourly": lambda: dev.get_energy_data(
            EnergyDataInterval.Hourly, today - timedelta(days=7), today),
        "energy_daily": lambda: dev.get_energy_data(EnergyDataInterval.Daily, quarter),
        "energy_monthly": lambda: dev.get_energy_data(
            EnergyDataInterval.Monthly, date(today.year, 1, 1)),
        "power_5min": lambda: dev.get_power_data(
            PowerDataInterval.Every5Minutes, now - timedelta(hours=12), now),
        "power_hourly": lambda: dev.get_power_data(
            PowerDataInterval.Hourly, now - timedelta(days=6), now),
    }
    for name, call in calls.items():
        try:
            result = _dict(await call())
            # Record Python types too, so fixtures show what to_dict() returns.
            (out / f"{name}.json").write_text(json.dumps(
                result, indent=2, default=lambda o: f"<{type(o).__name__}> {o}"))
            print(f"  {ip} {name}: ok")
        except Exception as exc:  # noqa: BLE001
            print(f"  {ip} {name}: FAILED {type(exc).__name__}: {exc}")
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tapo-discover", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("targets", nargs="+", help="plug IP(s) or a broadcast address")
    ap.add_argument("--dump", metavar="DIR", type=Path,
                    help="log in and dump every read-only call to DIR/<ip>/")
    ap.add_argument("--timeout", type=int, default=5, help="discovery timeout (s)")
    args = ap.parse_args(argv)

    for target in args.targets:
        asyncio.run(discover(target, args.timeout))
    if not args.dump:
        return 0
    cfg = Config.from_env()
    if not cfg.tapo_username or not cfg.tapo_password:
        print("error: --dump needs TAPO_USERNAME / TAPO_PASSWORD", file=sys.stderr)
        return 2
    ok = all([asyncio.run(dump(cfg, ip, args.dump)) for ip in args.targets])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
