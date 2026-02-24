from __future__ import annotations

import os
from zoneinfo import ZoneInfo


_TZ_ALIASES = {
  "Europe/Kiev": "Europe/Kyiv",
}


def canonical_tz_name(raw: str | None, fallback_to_env: bool = True) -> str:
  tz = str(raw or "").strip()
  if not tz or tz == "UTC":
    tz = ""
  if not tz and fallback_to_env:
    tz = str(os.getenv("TZ", "")).strip()
  if not tz:
    tz = "UTC"

  tz = _TZ_ALIASES.get(tz, tz)
  try:
    ZoneInfo(tz)
    return tz
  except Exception:
    if tz != "UTC":
      try:
        ZoneInfo("UTC")
        return "UTC"
      except Exception:
        pass
    return "UTC"


def is_valid_timezone(raw: str | None) -> bool:
  tz = str(raw or "").strip()
  if not tz:
    return False
  tz = _TZ_ALIASES.get(tz, tz)
  try:
    ZoneInfo(tz)
    return True
  except Exception:
    return False
