"""Per-plug poll state kept between oneshot runs (``<state_dir>/poll_state.json``).

    {"<plug_name>": {"last_ok": "<iso utc>", "auth_fail_at": "<iso utc>"}}

``last_ok`` sizes the history backfill (fetch from the last success instead of
the plug's full history every run); ``auth_fail_at`` enforces the login backoff.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from .records import utc

# Plug-side history limits (tapo stubs): hourly energy spans at most 8 days
# inclusive; 5-minute power returns at most 144 entries (12 hours).
HOURLY_MAX_DAYS = 7          # today - 7 .. today = 8 days inclusive
POWER_5MIN_MAX = timedelta(hours=12)
# Re-fetch a little before the last success so a slot that was still filling
# in (current hour / current 5 minutes) gets its final value.
POWER_5MIN_OVERLAP = timedelta(minutes=30)


class PollState:
    def __init__(self, state_dir: str):
        self.path = Path(state_dir) / "poll_state.json"
        self.data: dict[str, dict] = {}
        if self.path.exists():
            self.data = json.loads(self.path.read_text())

    def _get(self, plug: str, key: str) -> datetime | None:
        value = self.data.get(plug, {}).get(key)
        return utc(value) if value else None

    def last_ok(self, plug: str) -> datetime | None:
        return self._get(plug, "last_ok")

    def auth_fail_at(self, plug: str) -> datetime | None:
        return self._get(plug, "auth_fail_at")

    def mark_ok(self, plug: str, when: datetime) -> None:
        self.data[plug] = {"last_ok": utc(when).isoformat()}

    def clear_auth_fail(self, plug: str) -> None:
        self.data.get(plug, {}).pop("auth_fail_at", None)

    def mark_auth_fail(self, plug: str, when: datetime) -> None:
        self.data.setdefault(plug, {})["auth_fail_at"] = utc(when).isoformat()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".part")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        tmp.replace(self.path)


def hourly_start(today: date, last_ok: datetime | None) -> date:
    """First day of hourly energy to fetch: back to the day before the last
    success (covers a run that died mid-day), at least yesterday (so the last
    hours of yesterday land after midnight), at most the plug's 8-day window."""
    floor = today - timedelta(days=HOURLY_MAX_DAYS)
    if last_ok is None:
        return floor
    want = min(today - timedelta(days=1), last_ok.date() - timedelta(days=1))
    return max(floor, want)


def power_5min_start(now: datetime, last_ok: datetime | None) -> datetime:
    floor = now - POWER_5MIN_MAX
    if last_ok is None:
        return floor
    return max(floor, last_ok - POWER_5MIN_OVERLAP)
