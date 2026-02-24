from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
from zoneinfo import ZoneInfo

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from . import db
from .bot import send_digest
from .i18n import t
from . import monitoring
from .semantic import DEDUP_MODE, embed_texts, similarity

TZ = os.getenv("TZ", "UTC")
DB_RETENTION_DAYS = int(os.getenv("DB_RETENTION_DAYS", "60"))
DB_CLEANUP_EVERY_MIN = int(os.getenv("DB_CLEANUP_EVERY_MIN", "60"))
logger = logging.getLogger(__name__)


def _now_local() -> dt.datetime:
  return dt.datetime.now().astimezone()


def _user_local_now(timezone_name: str) -> dt.datetime:
  try:
    return dt.datetime.now(dt.timezone.utc).astimezone(ZoneInfo(timezone_name))
  except Exception:
    return dt.datetime.now(dt.timezone.utc)


def _effective_tz_name(raw: str | None) -> str:
  tz = str(raw or "").strip() or "UTC"
  if tz == "UTC":
    tz = TZ
  try:
    ZoneInfo(tz)
    return tz
  except Exception:
    return "UTC"


def _in_quiet_hours(s: dict, local_now: dt.datetime) -> bool:
  if not bool(s.get("quiet_hours_enabled", 0)):
    return False
  start = str(s.get("quiet_start", "23:00"))
  end = str(s.get("quiet_end", "07:00"))
  try:
    sh, sm = [int(x) for x in start.split(":", 1)]
    eh, em = [int(x) for x in end.split(":", 1)]
  except Exception:
    return False
  cur_m = local_now.hour * 60 + local_now.minute
  start_m = sh * 60 + sm
  end_m = eh * 60 + em
  if start_m == end_m:
    return True
  if start_m < end_m:
    return start_m <= cur_m < end_m
  return cur_m >= start_m or cur_m < end_m


def _alert_key(rep: dict) -> str:
  base = rep.get("norm_hash") or rep.get("link") or (rep.get("text", "")[:120])
  return hashlib.sha1(str(base).encode("utf-8", errors="ignore")).hexdigest()[:40]


def _daily_time_reached(local_now: dt.datetime, hhmm: str) -> bool:
  try:
    h, m = [int(x) for x in str(hhmm).split(":", 1)]
    target = h * 60 + m
    cur = local_now.hour * 60 + local_now.minute
    return cur >= target
  except Exception:
    return local_now.hour >= 9


def _parse_slot_utc(slot: str | None, timezone_name: str = "UTC") -> dt.datetime | None:
  if not slot:
    return None
  try:
    s = str(slot)
    if len(s) == 16:
      # Backward compatibility: old slot format had no timezone and was local user time.
      naive = dt.datetime.fromisoformat(s)
      try:
        local = naive.replace(tzinfo=ZoneInfo(timezone_name))
        return local.astimezone(dt.timezone.utc)
      except Exception:
        return naive.replace(tzinfo=dt.timezone.utc)
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
  except Exception:
    return None


async def _run_breaking(bot):
  uids = db.list_users_breaking_enabled()
  if not uids:
    return

  settings_by_uid = {uid: db.get_user_settings(uid) for uid in uids}
  max_window = max(int(s.get("breaking_window_min", 10)) for s in settings_by_uid.values())
  min_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=max_window)).strftime("%Y-%m-%dT%H:%M:%SZ")

  with db.connect() as conn:
    recent = conn.execute(
      "SELECT channel_username,text,link,norm_hash,date_utc,is_forward FROM posts "
      "WHERE date_utc >= ? ORDER BY date_utc DESC LIMIT 500",
      (min_utc,),
    ).fetchall()
  recent = [dict(r) for r in recent]
  if not recent:
    return

  clusters = []
  if DEDUP_MODE == "embeddings":
    texts = [r.get("text", "")[:500] for r in recent]
    embs = embed_texts(texts)
    if embs is None:
      by = {}
      for r in recent:
        by.setdefault(r.get("norm_hash"), []).append(r)
      clusters = list(by.values())
    else:
      used = [False] * len(recent)
      sim_threshold = float(os.getenv("BREAKING_SIM", "0.85"))
      for i, row in enumerate(recent):
        if used[i]:
          continue
        cl = [row]
        used[i] = True
        for j in range(i + 1, len(recent)):
          if used[j]:
            continue
          if similarity(row.get("text", ""), recent[j].get("text", ""), embs[i], embs[j]) >= sim_threshold:
            cl.append(recent[j])
            used[j] = True
        clusters.append(cl)
  else:
    by = {}
    for r in recent:
      by.setdefault(r.get("norm_hash"), []).append(r)
    clusters = list(by.values())

  now_utc = dt.datetime.now(dt.timezone.utc)
  cooldown = int(os.getenv("BREAKING_COOLDOWN_MIN", "30"))

  for uid in uids:
    s = settings_by_uid[uid]
    local_now = _user_local_now(str(s.get("timezone", "UTC")))
    if _in_quiet_hours(s, local_now):
      continue
    lang = db.get_lang(uid)
    min_sources = int(s.get("breaking_sources", 8))
    window_min = int(s.get("breaking_window_min", 10))
    originals_only = bool(s.get("originals_only", 0))
    window_start = now_utc - dt.timedelta(minutes=window_min)

    best = None
    for cl in clusters:
      cl2 = []
      for r in cl:
        if originals_only and int(r.get("is_forward", 0)) == 1:
          continue
        d = dt.datetime.fromisoformat(str(r["date_utc"]).replace("Z", "+00:00"))
        if d < window_start:
          continue
        cl2.append(r)
      if not cl2:
        continue
      sources = len({r["channel_username"] for r in cl2})
      if sources < min_sources:
        continue
      if best is None or sources > best["sources"]:
        best = {"items": cl2, "sources": sources}

    if not best:
      continue

    rep = best["items"][0]
    alert_key = _alert_key(rep)
    if db.is_alert_muted(uid, alert_key):
      continue
    if db.alert_recently_sent(uid, alert_key, cooldown_min=cooldown):
      continue

    chs = sorted({r["channel_username"] for r in best["items"]})[:15]
    chlist = ", ".join(["@" + c for c in chs])

    msg = (rep.get("text", "") or "").replace("\n", " ").strip()
    if len(msg) > 350:
      msg = msg[:350] + "…"

    text = (
      f"{t(lang, 'breaking_title')}\n"
      f"{msg}\n\n"
      f"🔗 {rep.get('link', '')}\n"
      f"{t(lang, 'breaking_sources')} ({len(chs)}): {chlist}"
    )
    kb = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text=t(lang, "breaking_mute"), callback_data=f"mute_brk:{alert_key}")]]
    )
    await bot.send_message(uid, text, disable_web_page_preview=True, reply_markup=kb)
    db.mark_alert_sent(uid, alert_key)
    db.incr_metric("alerts_sent", 1)


def setup_scheduler(bot):
  sched = AsyncIOScheduler(timezone=TZ)
  last_cleanup_at: dt.datetime | None = None

  async def tick():
    nonlocal last_cleanup_at
    now = _now_local()

    for uid in db.list_users_with_flag("hourly_enabled"):
      try:
        sch = db.get_schedule(uid)
        local_now = _user_local_now(_effective_tz_name(str(sch.get("timezone", "UTC"))))
        if _in_quiet_hours(sch, local_now):
          continue
        hour_key = local_now.strftime("%Y-%m-%dT%H")
        if sch.get("last_hourly_sent_hour") == hour_key:
          continue
        target_min = max(0, min(59, int(sch.get("hourly_minute", 2))))
        # Send once in this hour when configured minute is reached (robust to restarts/lag).
        if local_now.minute < target_min:
          continue
        lang = db.get_lang(uid)
        await send_digest(bot, uid, dt.timedelta(hours=1), top_k=8, title_prefix=t(lang, "hourly_digest"), scope="hourly")
        db.mark_hourly_sent(uid, hour_key)
        db.incr_metric("digests_hourly_sent", 1)
      except Exception:
        logger.exception("Hourly digest tick failed for uid=%s", uid)
        continue

    for uid in db.list_users_with_flag("daily_enabled"):
      try:
        sch = db.get_schedule(uid)
        local_now = _user_local_now(_effective_tz_name(str(sch.get("timezone", "UTC"))))
        if _in_quiet_hours(sch, local_now):
          continue
        today_key = local_now.strftime("%Y-%m-%d")
        if sch.get("last_daily_sent_date") == today_key:
          continue
        if not _daily_time_reached(local_now, str(sch.get("daily_time", "09:00"))):
          continue
        lang = db.get_lang(uid)
        await send_digest(bot, uid, dt.timedelta(hours=24), top_k=15, title_prefix=t(lang, "daily_digest"), scope="daily")
        db.mark_daily_sent(uid, today_key)
        db.incr_metric("digests_daily_sent", 1)
      except Exception:
        logger.exception("Daily digest tick failed for uid=%s", uid)
        continue

    for uid in db.list_users_monitoring_enabled():
      try:
        s = db.get_user_settings(uid)
        tzname = _effective_tz_name(str(s.get("timezone", "UTC")))
        local_now = _user_local_now(tzname)
        if _in_quiet_hours(s, local_now):
          continue
        pause_until = s.get("monitor_pause_until_utc")
        if pause_until:
          try:
            pu = dt.datetime.fromisoformat(str(pause_until).replace("Z", "+00:00"))
            if dt.datetime.now(dt.timezone.utc) < pu:
              continue
          except Exception:
            pass
        interval = max(1, min(30, int(s.get("monitor_interval_min", 2))))
        now_utc_min = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
        last_slot = _parse_slot_utc(s.get("monitor_last_slot"), timezone_name=tzname)
        if last_slot and (now_utc_min - last_slot).total_seconds() < interval * 60:
          continue
        await monitoring.send_monitoring_summary(bot, uid, period_min=interval, force=False)
        db.mark_monitor_slot(uid, now_utc_min.strftime("%Y-%m-%dT%H:%M:%SZ"))
      except Exception:
        logger.exception("Monitoring tick failed for uid=%s", uid)
        continue

    try:
      cleanup_every = max(5, int(DB_CLEANUP_EVERY_MIN))
      now_utc = dt.datetime.now(dt.timezone.utc)
      if last_cleanup_at is None or (now_utc - last_cleanup_at).total_seconds() >= cleanup_every * 60:
        db.cleanup_old_data(max(1, int(DB_RETENTION_DAYS)))
        last_cleanup_at = now_utc
    except Exception:
      logger.exception("DB cleanup tick failed")
      pass

    try:
      await _run_breaking(bot)
    except Exception:
      logger.exception("Breaking tick failed")
      pass

  sched.add_job(tick, "cron", second=10)
  return sched
