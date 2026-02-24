from __future__ import annotations

import datetime as dt
import hashlib
import re
from collections import defaultdict
from typing import Dict, List

from aiogram import Bot
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from . import db
from .i18n import t
from .text_utils import normalize_text

CATEGORY_KEYWORDS = {
  "drones": ["шахед", "shahed", "дрон", "бпла", "uav"],
  "missiles": ["ракет", "калибр", "искандер", "х-101", "x-101", "кинжал"],
  "launches": ["пуск", "вылет", "зліт", "takeoff", "launch"],
  "naval": ["корабл", "флот", "чф", "черномор", "мор", "носител", "ракетонос"],
  "aviation": ["авіа", "авиа", "ту-95", "миг", "су-", "стратег"],
}

CATEGORY_ORDER = ["launches", "drones", "missiles", "aviation", "naval"]
WHERE_RE = re.compile(
  r"\b(?:в|у|на|до|над|через)\s+([А-ЯA-ZІЇЄҐ][а-яa-zіїєґ'\-]{2,}(?:\s+[А-ЯA-ZІЇЄҐ][а-яa-zіїєґ'\-]{2,})?)",
  re.IGNORECASE,
)


def _parse_csv_words(csv: str | None) -> List[str]:
  if not csv:
    return []
  return [x.strip().lower() for x in str(csv).split(",") if x.strip()]


def _match_any(text: str, words: List[str]) -> bool:
  if not words:
    return False
  low = text.lower()
  return any(w in low for w in words)


def _selected_categories(settings: Dict[str, object]) -> List[str]:
  raw = str(settings.get("monitor_categories") or "all").strip().lower()
  if not raw or raw == "all":
    return list(CATEGORY_ORDER)
  out = []
  for c in [x.strip() for x in raw.split(",") if x.strip()]:
    if c in CATEGORY_KEYWORDS and c not in out:
      out.append(c)
  return out or list(CATEGORY_ORDER)


def _detect_where(text: str) -> str:
  m = WHERE_RE.search(text)
  return m.group(1) if m else "?"


def _detect_category(text: str, selected: List[str]) -> str | None:
  low = text.lower()
  for cat in selected:
    if any(k in low for k in CATEGORY_KEYWORDS.get(cat, [])):
      return cat
  return None


def _what_head(text: str) -> str:
  first = text.split("\n", 1)[0]
  first = " ".join(first.split())
  return first[:140]


def _event_sig(cat: str, where: str, what: str) -> str:
  # coarse signature for clustering similar notices
  base = f"{cat}|{where.lower()}|{' '.join(what.lower().split()[:8])}"
  return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _priority(cat: str, text: str, sources: int) -> str:
  low = text.lower()
  if cat == "missiles" and sources >= 2:
    return "critical"
  if cat == "naval" and ("ракетонос" in low or "калибр" in low):
    return "high"
  if cat == "launches" and ("шахед" in low or "ракет" in low) and sources >= 2:
    return "high"
  if cat == "drones" and sources >= 3:
    return "high"
  if cat in ("missiles", "launches"):
    return "high"
  return "medium"


def _priority_rank(p: str) -> int:
  return {"critical": 3, "high": 2, "medium": 1}.get(p, 0)


def _is_confirmed(sources: int) -> bool:
  return sources >= 2


def analyze_events(posts: List[Dict[str, object]], settings: Dict[str, object]) -> List[Dict[str, object]]:
  selected = _selected_categories(settings)
  inc_words = _parse_csv_words(settings.get("monitor_include_keywords"))
  exc_words = _parse_csv_words(settings.get("monitor_exclude_keywords"))

  grouped: Dict[str, Dict[str, object]] = {}
  for p in posts:
    txt = normalize_text(str(p.get("text") or ""))
    if not txt:
      continue
    if inc_words and not _match_any(txt, inc_words):
      continue
    if exc_words and _match_any(txt, exc_words):
      continue

    cat = _detect_category(txt, selected)
    if not cat:
      continue

    where = _detect_where(txt)
    what = _what_head(txt)
    sig = _event_sig(cat, where, what)

    cur = grouped.get(sig)
    if cur is None:
      grouped[sig] = {
        "sig": sig,
        "category": cat,
        "where": where,
        "what": what,
        "date_utc": str(p.get("date_utc") or ""),
        "source": str(p.get("channel_username") or ""),
        "link": str(p.get("link") or ""),
        "sources": {str(p.get("channel_username") or "")},
        "sample_text": txt,
      }
    else:
      cur["sources"].add(str(p.get("channel_username") or ""))
      d = str(p.get("date_utc") or "")
      if d > str(cur.get("date_utc") or ""):
        cur["date_utc"] = d
        cur["source"] = str(p.get("channel_username") or "")
        cur["link"] = str(p.get("link") or "")
        cur["what"] = what
        cur["sample_text"] = txt

  events = []
  for ev in grouped.values():
    sources_count = len([x for x in ev["sources"] if x])
    pr = _priority(str(ev["category"]), str(ev.get("sample_text") or ""), sources_count)
    events.append(
      {
        "sig": ev["sig"],
        "category": ev["category"],
        "where": ev["where"],
        "what": ev["what"],
        "date_utc": ev["date_utc"],
        "source": ev["source"],
        "link": ev["link"],
        "sources_count": sources_count,
        "priority": pr,
        "confirmed": _is_confirmed(sources_count),
      }
    )

  events.sort(key=lambda e: (_priority_rank(str(e["priority"])), str(e["date_utc"])), reverse=True)
  return events


def _cat_title(lang: str, cat: str) -> str:
  return t(lang, f"monitor_cat_{cat}")


def _prio_title(lang: str, p: str) -> str:
  return t(lang, f"monitor_prio_{p}")


def _format_period(period_min: int) -> str:
  if period_min % 60 == 0:
    return f"{period_min // 60}h"
  return f"{period_min}m"


def _short_time(iso_utc: str | None) -> str:
  if not iso_utc:
    return "--:--"
  try:
    d = dt.datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
    return d.strftime("%H:%M")
  except Exception:
    return "--:--"


def _parse_period_min(raw: str | None) -> int | None:
  if raw is None:
    return None
  s = raw.strip().lower()
  if not s:
    return None
  if re.fullmatch(r"\d+", s):
    return max(1, int(s))
  m = re.fullmatch(r"(\d+)\s*([mhd])", s)
  if not m:
    return None
  n = max(1, int(m.group(1)))
  unit = m.group(2)
  if unit == "m":
    return n
  if unit == "h":
    return n * 60
  return n * 1440


def build_monitor_text(lang: str, events: List[Dict[str, object]], period_min: int, compact: bool = True) -> str:
  header = f"{t(lang, 'monitor_title')} ({_format_period(period_min)})"
  if not events:
    return f"{header}\n\n{t(lang, 'monitor_empty')}"

  if compact:
    lines = [header, ""]
    for e in events[:8]:
      pr = _prio_title(lang, str(e["priority"]))
      cfm = f" {t(lang, 'monitor_confirmed')}" if bool(e.get("confirmed")) else ""
      tm = _short_time(str(e.get("date_utc") or ""))
      lines.append(f"• {tm} [{pr}] {_cat_title(lang, str(e['category']))} • {e['where']} • src:{e['sources_count']}{cfm}")
    if len(events) > 8:
      lines.append(f"... +{len(events)-8}")
    return "\n".join(lines).strip()

  by_cat: Dict[str, List[Dict[str, object]]] = defaultdict(list)
  for e in events:
    by_cat[str(e["category"])].append(e)

  lines = [header, ""]
  total = 0
  for cat in CATEGORY_ORDER:
    items = by_cat.get(cat, [])
    if not items:
      continue
    lines.append(f"{_cat_title(lang, cat)} ({len(items)}):")
    for e in items[:12]:
      total += 1
      pr = _prio_title(lang, str(e["priority"]))
      cfm = f" {t(lang, 'monitor_confirmed')}" if bool(e.get("confirmed")) else ""
      tm = _short_time(str(e.get("date_utc") or ""))
      lines.append(f"• {tm} [{pr}] {e['where']}: {e['what']} (src:{e['sources_count']}, @{e['source']}){cfm}")
    lines.append("")

  if total == 0:
    lines.append(t(lang, "monitor_empty"))
  return "\n".join(lines).strip()


def monitor_keyboard(lang: str, period_min: int = 2) -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
    inline_keyboard=[
      [
        InlineKeyboardButton(text=t(lang, "monitor_on"), callback_data="mon:on"),
        InlineKeyboardButton(text=t(lang, "monitor_off"), callback_data="mon:off"),
      ],
      [InlineKeyboardButton(text=t(lang, "monitor_pause_1h"), callback_data="mon:pause:60")],
      [
        InlineKeyboardButton(text=t(lang, "monitor_filter_all"), callback_data="mon:filter:all"),
        InlineKeyboardButton(text=t(lang, "monitor_filter_drones"), callback_data="mon:filter:drones"),
        InlineKeyboardButton(text=t(lang, "monitor_filter_missiles"), callback_data="mon:filter:missiles"),
      ],
      [
        InlineKeyboardButton(text=t(lang, "monitor_filter_launches"), callback_data="mon:filter:launches"),
        InlineKeyboardButton(text=t(lang, "monitor_filter_naval"), callback_data="mon:filter:naval"),
        InlineKeyboardButton(text=t(lang, "monitor_filter_aviation"), callback_data="mon:filter:aviation"),
      ],
      [
        InlineKeyboardButton(text=t(lang, "monitor_details"), callback_data=f"mon:details:{int(period_min)}"),
      ],
      [
        InlineKeyboardButton(text=t(lang, "monitor_report_1h"), callback_data="mon:report:60"),
        InlineKeyboardButton(text=t(lang, "monitor_report_2h"), callback_data="mon:report:120"),
        InlineKeyboardButton(text=t(lang, "monitor_report_24h"), callback_data="mon:report:1440"),
      ],
    ]
  )


def _antiflood_key(event: Dict[str, object]) -> str:
  return f"mon:{event.get('sig')}"


def _apply_antiflood(uid: int, events: List[Dict[str, object]], antiflood_min: int) -> List[Dict[str, object]]:
  out = []
  for e in events:
    key = _antiflood_key(e)
    if db.alert_recently_sent(uid, key, cooldown_min=antiflood_min):
      continue
    out.append(e)
  return out


def _mark_antiflood(uid: int, events: List[Dict[str, object]]):
  for e in events:
    db.mark_alert_sent(uid, _antiflood_key(e))


async def send_monitoring_summary(bot: Bot, user_id: int, period_min: int = 2, force: bool = False) -> bool:
  settings = db.get_user_settings(user_id)
  lang = db.get_lang(user_id)
  if not force and not bool(settings.get("monitor_enabled", 0)):
    return False

  end = dt.datetime.now(dt.timezone.utc)
  start = end - dt.timedelta(minutes=int(period_min))
  posts = db.get_monitor_posts_between(
    user_id,
    start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    end.strftime("%Y-%m-%dT%H:%M:%SZ"),
  )
  events = analyze_events(posts, settings)

  antiflood_min = max(1, int(settings.get("monitor_antiflood_min", 7)))
  send_events = events if force else _apply_antiflood(user_id, events, antiflood_min)

  if not force and not send_events:
    return False

  text = build_monitor_text(lang, send_events, period_min=period_min, compact=True)
  await bot.send_message(user_id, text, disable_web_page_preview=True, reply_markup=monitor_keyboard(lang, period_min=period_min))

  if not force:
    _mark_antiflood(user_id, send_events)
  db.incr_metric("monitor_sent", 1)
  return True


async def send_monitoring_report(bot: Bot, user_id: int, period_min: int) -> bool:
  settings = db.get_user_settings(user_id)
  lang = db.get_lang(user_id)
  end = dt.datetime.now(dt.timezone.utc)
  start = end - dt.timedelta(minutes=int(period_min))
  posts = db.get_monitor_posts_between(
    user_id,
    start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    end.strftime("%Y-%m-%dT%H:%M:%SZ"),
  )
  events = analyze_events(posts, settings)
  text = build_monitor_text(lang, events, period_min=period_min, compact=False)
  await bot.send_message(user_id, text, disable_web_page_preview=True, reply_markup=monitor_keyboard(lang, period_min=period_min))
  db.incr_metric("monitor_report_sent", 1)
  return True


async def handle_monitor_callback(q: CallbackQuery, bot: Bot) -> bool:
  data = q.data or ""
  if not data.startswith("mon:"):
    return False

  uid = q.from_user.id
  lang = db.get_lang(uid)

  if data == "mon:on":
    db.set_monitoring_enabled(uid, True)
    await q.message.answer(t(lang, "monitor_enabled"))
    await q.answer()
    return True

  if data == "mon:off":
    db.set_monitoring_enabled(uid, False)
    await q.message.answer(t(lang, "monitor_disabled"))
    await q.answer()
    return True

  if data.startswith("mon:pause:"):
    mins = int(data.split(":")[-1])
    until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=mins)
    db.set_monitoring_params(uid, pause_until_utc=until.strftime("%Y-%m-%dT%H:%M:%SZ"))
    await q.message.answer(t(lang, "monitor_paused").format(mins=mins))
    await q.answer()
    return True

  if data.startswith("mon:filter:"):
    cat = data.split(":")[-1].strip().lower()
    allowed = set(CATEGORY_ORDER + ["all"])
    if cat in allowed:
      db.set_monitoring_params(uid, categories=cat)
      await q.message.answer(t(lang, "monitor_filter_set").format(value=cat))
    await q.answer()
    return True

  if data.startswith("mon:details:"):
    mins = int(data.split(":")[-1])
    await send_monitoring_report(bot, uid, mins)
    await q.answer()
    return True

  if data.startswith("mon:report:"):
    mins = int(data.split(":")[-1])
    await send_monitoring_report(bot, uid, mins)
    await q.answer()
    return True

  await q.answer()
  return True


async def monitor_command(message: Message, args: str, parse_channel_ref):
  uid = message.from_user.id
  lang = db.get_lang(uid)
  db.ensure_user(uid)

  parts = (args or "").strip().split()
  if not parts:
    s = db.get_user_settings(uid)
    channels = db.list_monitor_channels(uid)
    txt = (
      f"{t(lang, 'monitor_title')}\n"
      f"• enabled={bool(s.get('monitor_enabled', 0))}\n"
      f"• interval={int(s.get('monitor_interval_min', 2))}m\n"
      f"• antiflood={int(s.get('monitor_antiflood_min', 7))}m\n"
      f"• categories={s.get('monitor_categories', 'all')}\n"
      f"• include={s.get('monitor_include_keywords') or '-'}\n"
      f"• exclude={s.get('monitor_exclude_keywords') or '-'}\n"
      f"• channels={', '.join(['@'+c for c in channels]) if channels else '-'}"
    )
    await message.answer(txt)
    return

  cmd = parts[0].lower()
  if cmd in ("on", "off"):
    db.set_monitoring_enabled(uid, cmd == "on")
    await message.answer(t(lang, "monitor_enabled") if cmd == "on" else t(lang, "monitor_disabled"))
    return

  if cmd == "add" and len(parts) >= 2:
    ch = parse_channel_ref(parts[1])
    if not ch:
      await message.answer(t(lang, "invalid_channel"))
      return
    db.add_monitor_channel(uid, ch)
    await message.answer(t(lang, "added_to_monitor").format(ch='@'+ch))
    return

  if cmd == "rm" and len(parts) >= 2:
    ch = parse_channel_ref(parts[1])
    if not ch:
      await message.answer(t(lang, "invalid_channel"))
      return
    n = db.remove_monitor_channel(uid, ch)
    await message.answer(t(lang, "removed_from_monitor").format(ch='@'+ch) if n else t(lang, "not_found_scope").format(ch='@'+ch, scope='monitor'))
    return

  if cmd == "interval" and len(parts) >= 2:
    try:
      mins = max(1, min(30, int(parts[1])))
    except Exception:
      await message.answer(t(lang, "monitor_format"))
      return
    db.set_monitoring_params(uid, interval_min=mins)
    await message.answer(t(lang, "monitor_interval_set").format(mins=mins))
    return

  if cmd == "antiflood" and len(parts) >= 2:
    try:
      mins = max(1, min(120, int(parts[1])))
    except Exception:
      await message.answer(t(lang, "monitor_format"))
      return
    db.set_monitoring_params(uid, antiflood_min=mins)
    await message.answer(t(lang, "monitor_antiflood_set").format(mins=mins))
    return

  if cmd == "include":
    val = " ".join(parts[1:]).strip()
    db.set_monitoring_params(uid, include_keywords=val)
    await message.answer(t(lang, "saved"))
    return

  if cmd == "exclude":
    val = " ".join(parts[1:]).strip()
    db.set_monitoring_params(uid, exclude_keywords=val)
    await message.answer(t(lang, "saved"))
    return

  if cmd == "categories" and len(parts) >= 2:
    cats = parts[1].lower()
    db.set_monitoring_params(uid, categories=cats)
    await message.answer(t(lang, "saved"))
    return

  if cmd == "pause" and len(parts) >= 2:
    try:
      mins = int(parts[1])
    except Exception:
      await message.answer(t(lang, "monitor_format"))
      return
    until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=max(1, mins))
    db.set_monitoring_params(uid, pause_until_utc=until.strftime("%Y-%m-%dT%H:%M:%SZ"))
    await message.answer(t(lang, "monitor_paused").format(mins=max(1, mins)))
    return

  if cmd == "now":
    mins = int(db.get_user_settings(uid).get("monitor_interval_min", 2))
    if len(parts) >= 2:
      parsed = _parse_period_min(parts[1])
      if parsed is None:
        await message.answer(t(lang, "monitor_report_format"))
        return
      mins = parsed
    await send_monitoring_summary(message.bot, uid, period_min=mins, force=True)
    return

  await message.answer(t(lang, "monitor_format"))
