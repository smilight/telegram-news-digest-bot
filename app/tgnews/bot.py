from __future__ import annotations

import datetime as dt
import os
import re
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
  BotCommand,
  BotCommandScopeChat,
  CallbackQuery,
  InlineKeyboardButton,
  InlineKeyboardMarkup,
  KeyboardButton,
  MenuButtonCommands,
  Message,
  ReplyKeyboardMarkup,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from . import db
from . import monitoring
from .digest import cluster_posts, format_digest
from .i18n import norm_lang, t

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DIGEST_TOPK_HOURLY = int(os.getenv("DIGEST_TOPK_HOURLY", "8"))
DIGEST_TOPK_DAILY = int(os.getenv("DIGEST_TOPK_DAILY", "15"))

RE_CH = re.compile(r"@?([A-Za-z0-9_]{4,32})")
RE_CH_FULL = re.compile(r"^@?([A-Za-z0-9_]{4,32})$")
RE_TME = re.compile(r"^(?:https?://)?(?:t\.me|telegram\.me)/(?:(?:s|c)/)?([A-Za-z0-9_]{4,32})(?:[/?#].*)?$", re.IGNORECASE)


class AddRemoveFlow(StatesGroup):
  waiting_add_hourly = State()
  waiting_add_daily = State()
  waiting_rm_hourly = State()
  waiting_rm_daily = State()
  waiting_monitor_add = State()
  waiting_monitor_rm = State()
  waiting_daily_time = State()
  waiting_include_kw = State()
  waiting_exclude_kw = State()


def now_utc() -> dt.datetime:
  return dt.datetime.now(dt.timezone.utc)


def parse_period_scope(args: str | None) -> tuple[dt.timedelta, str]:
  if not args:
    return dt.timedelta(hours=1), "all"
  parts = args.strip().lower().split()
  period = None
  scope = "all"
  for p in parts:
    if re.fullmatch(r"\d+[hd]", p):
      n = int(p[:-1])
      unit = p[-1]
      period = dt.timedelta(hours=n) if unit == "h" else dt.timedelta(days=n)
    elif p in ("hourly", "daily", "all"):
      scope = p
  if period is None:
    period = dt.timedelta(hours=1)
  return period, scope


def parse_topic_arg(args: str | None) -> str | None:
  if not args:
    return None
  for part in args.strip().split():
    low = part.lower()
    if low.startswith("topic:") and len(part) > 6:
      return part.split(":", 1)[1].strip().lower()
  return None


def parse_channel_ref(raw: str | None) -> str | None:
  if not raw:
    return None
  s = raw.strip()
  if not s:
    return None
  # Trim common punctuation around pasted links/usernames.
  s = s.strip("()[]<>{},.;")

  m = RE_CH_FULL.fullmatch(s)
  if m:
    return m.group(1)

  m = RE_TME.fullmatch(s)
  if m:
    return m.group(1)

  return None


def _ensure_user_and_lang(user_id: int, tg_lang: str | None = None) -> str:
  created = db.ensure_user(user_id)
  if created and tg_lang:
    db.set_lang(user_id, norm_lang(tg_lang))
  return db.get_lang(user_id)


def _lang_kb() -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
    inline_keyboard=[
      [
        InlineKeyboardButton(text="English", callback_data="setlang:en"),
        InlineKeyboardButton(text="Українська", callback_data="setlang:uk"),
        InlineKeyboardButton(text="Русский", callback_data="setlang:ru"),
      ]
    ]
  )


def _menu_button_kb(lang: str) -> ReplyKeyboardMarkup:
  return ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=t(lang, "menu_open"))]],
    resize_keyboard=True,
    is_persistent=True,
  )


def _scope_title(lang: str, scope: str) -> str:
  if scope == "hourly":
    return t(lang, "scope_hourly")
  if scope == "daily":
    return t(lang, "scope_daily")
  return t(lang, "scope_all")


async def send_digest(
  bot: Bot,
  user_id: int,
  period: dt.timedelta,
  top_k: int,
  title_prefix: str,
  scope: str,
  topic_name: str | None = None,
):
  end = now_utc()
  start = end - period

  lang = db.get_lang(user_id)
  posts = db.get_posts_for_user_between(
    user_id=user_id,
    start_utc=start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    end_utc=end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    scope=scope,
  )

  settings = db.get_user_settings(user_id)
  if settings.get("originals_only"):
    posts = [p for p in posts if int(p.get("is_forward", 0)) == 0]

  inc = (settings.get("include_keywords") or "").strip()
  exc = (settings.get("exclude_keywords") or "").strip()

  def _match_any(text: str, csv: str) -> bool:
    kws = [k.strip().lower() for k in csv.split(",") if k.strip()]
    txt = (text or "").lower()
    return any(k in txt for k in kws)

  if inc:
    posts = [p for p in posts if _match_any(p.get("text", ""), inc)]
  if exc:
    posts = [p for p in posts if not _match_any(p.get("text", ""), exc)]

  if topic_name:
    topic = db.get_topic_profile(user_id, topic_name)
    if topic and bool(topic.get("enabled", 1)):
      topic_scope = str(topic.get("scope", "all"))
      if topic_scope != "all" and topic_scope != scope:
        posts = []
      else:
        inc_t = (topic.get("include_keywords") or "").strip()
        exc_t = (topic.get("exclude_keywords") or "").strip()
        if inc_t:
          posts = [p for p in posts if _match_any(p.get("text", ""), inc_t)]
        if exc_t:
          posts = [p for p in posts if not _match_any(p.get("text", ""), exc_t)]

  clusters = cluster_posts(posts)
  hours = max(1, int(period.total_seconds() // 3600))
  scope_title = _scope_title(lang, scope)
  try:
    tzname = str(settings.get("timezone", "UTC"))
    end_local = end.astimezone(ZoneInfo(tzname))
  except Exception:
    end_local = end.astimezone()
  title = f"{title_prefix} ({scope_title}) • {t(lang, 'for_hours').format(hours=hours)} • {end_local.strftime('%d.%m.%Y %H:%M')}"
  text = format_digest(title, clusters, top_k=top_k, lang=lang)
  db.incr_metric("digests_generated", 1)

  if len(text) <= 3800:
    await bot.send_message(user_id, text, disable_web_page_preview=True)
    return

  chunks, cur = [], ""
  for part in text.split("\n\n"):
    if len(cur) + len(part) + 2 > 3800:
      if cur.strip():
        chunks.append(cur.strip())
      cur = part + "\n\n"
    else:
      cur += part + "\n\n"
  if cur.strip():
    chunks.append(cur.strip())

  for chunk in chunks:
    await bot.send_message(user_id, chunk, disable_web_page_preview=True)


def menu_kb(hourly_on: bool, daily_on: bool, lang: str = "en", view: str = "main"):
  kb = InlineKeyboardBuilder()

  if view == "main":
    kb.button(
      text=f"{t(lang, 'hourly_state')}: {t(lang, 'toggle_on') if hourly_on else t(lang, 'toggle_off')}",
      callback_data="toggle:hourly",
    )
    kb.button(
      text=f"{t(lang, 'daily_state')}: {t(lang, 'toggle_on') if daily_on else t(lang, 'toggle_off')}",
      callback_data="toggle:daily",
    )
    kb.adjust(2)
    kb.button(text=t(lang, "menu_channels"), callback_data="nav:channels")
    kb.button(text=t(lang, "menu_digest"), callback_data="nav:digest")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_settings"), callback_data="nav:settings")
    kb.button(text=t(lang, "status"), callback_data="status")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_monitoring"), callback_data="nav:monitor")
    kb.adjust(1)
    kb.button(text=t(lang, "lang"), callback_data="lang")
    kb.adjust(1)
  elif view == "channels":
    kb.button(text=t(lang, "add_hourly"), callback_data="add:hourly")
    kb.button(text=t(lang, "add_daily"), callback_data="add:daily")
    kb.button(text=t(lang, "rm_hourly"), callback_data="rm:hourly")
    kb.button(text=t(lang, "rm_daily"), callback_data="rm:daily")
    kb.adjust(2, 2)
    kb.button(text=t(lang, "lists"), callback_data="lists")
    kb.button(text=t(lang, "sources_recent"), callback_data="sources:recent")
    kb.button(text=t(lang, "sources_top"), callback_data="sources:top")
    kb.adjust(1, 2)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:main")
    kb.adjust(1)
  elif view == "digest":
    kb.button(text="⚡ 1h (all)", callback_data="now:1h:all")
    kb.button(text="⚡ 1h (hourly)", callback_data="now:1h:hourly")
    kb.button(text="⚡ 24h (daily)", callback_data="now:24h:daily")
    kb.button(text="⚡ 24h (all)", callback_data="now:24h:all")
    kb.adjust(2, 2)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:main")
    kb.adjust(1)
  elif view == "settings":
    kb.button(text=t(lang, "menu_schedule"), callback_data="nav:schedule")
    kb.button(text=t(lang, "menu_keywords"), callback_data="nav:keywords")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_breaking"), callback_data="nav:breaking")
    kb.button(text=t(lang, "originals_toggle"), callback_data="toggle:originals")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:main")
    kb.adjust(1)
  elif view == "schedule":
    kb.button(text=t(lang, "hourly_min_minus"), callback_data="sched:hourly:-1")
    kb.button(text=t(lang, "hourly_min_plus"), callback_data="sched:hourly:+1")
    kb.adjust(2)
    kb.button(text=t(lang, "daily_custom"), callback_data="sched:daily:custom")
    kb.adjust(1)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:settings")
    kb.adjust(1)
  elif view == "keywords":
    kb.button(text=t(lang, "kw_include"), callback_data="kw:include")
    kb.button(text=t(lang, "kw_exclude"), callback_data="kw:exclude")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:settings")
    kb.adjust(1)
  elif view == "breaking":
    kb.button(text=t(lang, "breaking_toggle"), callback_data="toggle:breaking")
    kb.adjust(1)
    kb.button(text=t(lang, "breaking_sources_minus"), callback_data="brk:sources:-1")
    kb.button(text=t(lang, "breaking_sources_plus"), callback_data="brk:sources:+1")
    kb.adjust(2)
    kb.button(text=t(lang, "breaking_window_minus"), callback_data="brk:window:-1")
    kb.button(text=t(lang, "breaking_window_plus"), callback_data="brk:window:+1")
    kb.adjust(2)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:settings")
    kb.adjust(1)
  elif view == "monitor":
    kb.button(text=t(lang, "monitor_on"), callback_data="mon:on")
    kb.button(text=t(lang, "monitor_off"), callback_data="mon:off")
    kb.adjust(2)
    kb.button(text=t(lang, "monitor_add_source"), callback_data="mon:add")
    kb.button(text=t(lang, "monitor_rm_source"), callback_data="mon:rm")
    kb.adjust(2)
    kb.button(text=t(lang, "monitor_pause_1h"), callback_data="mon:pause:60")
    kb.adjust(1)
    kb.button(text=t(lang, "monitor_report_1h"), callback_data="mon:report:60")
    kb.button(text=t(lang, "monitor_report_2h"), callback_data="mon:report:120")
    kb.button(text=t(lang, "monitor_report_24h"), callback_data="mon:report:1440")
    kb.adjust(3)
    kb.button(text=t(lang, "menu_back"), callback_data="nav:main")
    kb.adjust(1)
  else:
    kb.button(text=t(lang, "menu_main"), callback_data="nav:main")
    kb.adjust(1)

  return kb.as_markup()


def build_help(lang: str) -> str:
  return t(lang, "help_commands")


def format_lists(user_id: int, lang: str) -> str:
  hc = db.list_channels_for_user(user_id, "hourly")
  dc = db.list_channels_for_user(user_id, "daily")
  out = [t(lang, "lists_title"), "", f"🕐 {t(lang, 'scope_hourly')}:" ]
  out.extend([f"• @{c}" for c in hc] or [t(lang, "empty")])
  out.append("")
  out.append(f"🗓 {t(lang, 'scope_daily')}:")
  out.extend([f"• @{c}" for c in dc] or [t(lang, "empty")])
  return "\n".join(out)


def _status_text(user_id: int, lang: str, hourly_on: bool, daily_on: bool) -> str:
  st = db.status_summary(user_id)
  sch = db.get_schedule(user_id)
  s = db.get_user_settings(user_id)
  last = st.get("last_channel")
  last_line = "—"
  if last:
    last_line = f"@{last['username']} (last_msg_id={last['last_msg_id']}, updated_at={last['updated_at']})"

  txt = (
    f"{t(lang, 'status_title')}\n"
    f"{t(lang, 'status_hourly')}: {hourly_on}\n"
    f"{t(lang, 'status_daily')}: {daily_on}\n"
    f"{t(lang, 'status_hourly_min')}: {int(sch.get('hourly_minute', 2)):02d}\n"
    f"{t(lang, 'status_daily_time')}: {sch.get('daily_time', '09:00')}\n"
    f"{t(lang, 'status_timezone')}: {sch.get('timezone', 'UTC')}\n"
    f"{t(lang, 'status_quiet')}: {bool(sch.get('quiet_hours_enabled', 0))} "
    f"({sch.get('quiet_start', '23:00')}–{sch.get('quiet_end', '07:00')})\n"
    f"{t(lang, 'status_dedup')}: {os.getenv('DEDUP_MODE', 'simhash')}\n"
    f"{t(lang, 'status_breaking')}: {bool(s.get('breaking_enabled', 0))}\n"
    f"{t(lang, 'status_breaking_sources')}: {int(s.get('breaking_sources', 8))}\n"
    f"{t(lang, 'status_breaking_window')}: {int(s.get('breaking_window_min', 10))}\n"
    f"{t(lang, 'status_originals')}: {bool(s.get('originals_only', 0))}\n"
    f"{t(lang, 'status_hourly_channels')}: {st['hourly_channels']}\n"
    f"{t(lang, 'status_daily_channels')}: {st['daily_channels']}\n"
    f"{t(lang, 'status_posts_24h')}: {st['posts_24h']}\n"
    f"{t(lang, 'status_last_update')}: {last_line}\n"
  )

  inc = (s.get("include_keywords") or "").strip()
  exc = (s.get("exclude_keywords") or "").strip()
  if inc:
    txt += f"{t(lang, 'status_include')}: {inc}\n"
  if exc:
    txt += f"{t(lang, 'status_exclude')}: {exc}\n"
  topics = db.list_topic_profiles(user_id)
  if topics:
    txt += f"{t(lang, 'status_topics')}: {len(topics)}\n"

  spam = db.list_top_spammy_channels(6)
  if spam:
    txt += f"\n{t(lang, 'status_lowuniq')}\n"
    for r in spam:
      txt += f"• @{r['username']}: uniq={r['uniqueness']:.2f}, total={r['total_posts']}, fwd={r['forwards']}\n"
  return txt


def _monitoring_text(user_id: int, lang: str) -> str:
  s = db.get_user_settings(user_id)
  channels = db.list_monitor_channels(user_id)
  return (
    f"{t(lang, 'monitor_title')}\n"
    f"• enabled={bool(s.get('monitor_enabled', 0))}\n"
    f"• interval={int(s.get('monitor_interval_min', 2))}m\n"
    f"• categories={s.get('monitor_categories', 'all')}\n"
    f"• include={s.get('monitor_include_keywords') or '-'}\n"
    f"• exclude={s.get('monitor_exclude_keywords') or '-'}\n"
    f"• channels={', '.join(['@'+c for c in channels]) if channels else '-'}"
  )


async def _send_main_menu(message: Message, lang: str):
  hourly_on, daily_on = db.get_user_flags(message.from_user.id)
  await message.answer(
    t(lang, "menu_hint"),
    reply_markup=menu_kb(hourly_on, daily_on, lang, view="main"),
  )


async def _configure_telegram_chat_ui(bot: Bot, user_id: int, lang: str):
  # Telegram left-side menu button with command list.
  await bot.set_chat_menu_button(chat_id=user_id, menu_button=MenuButtonCommands())
  commands = [
    BotCommand(command="menu", description=t(lang, "cmd_menu_desc")),
    BotCommand(command="monitor", description=t(lang, "cmd_monitor_desc")),
    BotCommand(command="mreport", description=t(lang, "cmd_mreport_desc")),
    BotCommand(command="now", description=t(lang, "cmd_now_desc")),
    BotCommand(command="status", description=t(lang, "cmd_status_desc")),
    BotCommand(command="sources", description=t(lang, "cmd_sources_desc")),
  ]
  await bot.set_my_commands(commands=commands, scope=BotCommandScopeChat(chat_id=user_id))


def make_bot_and_dp() -> tuple[Bot, Dispatcher]:
  bot = Bot(BOT_TOKEN)
  dp = Dispatcher()

  @dp.message(Command("start"))
  async def start(m: Message):
    db.init_db()
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    try:
      await _configure_telegram_chat_ui(bot, m.from_user.id, lang)
    except Exception:
      pass
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      t(lang, "help_start") + "\n\n" + build_help(lang),
      reply_markup=menu_kb(hourly_on, daily_on, lang),
      disable_web_page_preview=True,
    )
    await m.answer(t(lang, "menu_open_hint"), reply_markup=_menu_button_kb(lang))

  @dp.message(Command("menu"))
  async def menu_cmd(m: Message):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    await _send_main_menu(m, lang)

  @dp.message(Command("status"))
  async def status_cmd(m: Message):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      _status_text(m.from_user.id, lang, hourly_on, daily_on),
      reply_markup=menu_kb(hourly_on, daily_on, lang),
      disable_web_page_preview=True,
    )

  @dp.message(Command("add"))
  async def add(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    if not command.args:
      await m.answer(t(lang, "add_format"))
      return
    parts = command.args.split()
    ch = parse_channel_ref(parts[0])
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    scope = (parts[1].lower() if len(parts) > 1 else "daily")
    if scope not in ("hourly", "daily"):
      await m.answer(t(lang, "scope_format"))
      return
    db.add_channel_for_user(m.from_user.id, ch, scope)
    await m.answer(t(lang, "added_to_hourly" if scope == "hourly" else "added_to_daily").format(ch=f"@{ch}"))

  @dp.message(Command("rm"))
  async def rm(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    if not command.args:
      await m.answer(t(lang, "rm_format"))
      return
    parts = command.args.split()
    ch = parse_channel_ref(parts[0])
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    scope = (parts[1].lower() if len(parts) > 1 else "daily")
    if scope not in ("hourly", "daily"):
      await m.answer(t(lang, "scope_format"))
      return
    n = db.remove_channel_for_user(m.from_user.id, ch, scope)
    if n:
      await m.answer(t(lang, "removed_from_hourly" if scope == "hourly" else "removed_from_daily").format(ch=f"@{ch}"))
    else:
      await m.answer(t(lang, "not_found_scope").format(ch=f"@{ch}", scope=scope))

  @dp.message(Command("list"))
  async def list_cmd(m: Message):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(format_lists(m.from_user.id, lang), reply_markup=menu_kb(hourly_on, daily_on, lang))

  @dp.message(Command("hourly"))
  async def hourly_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip().lower()
    if arg not in ("on", "off"):
      await m.answer(t(lang, "hourly_format"))
      return
    db.set_user_flag(m.from_user.id, "hourly_enabled", arg == "on")
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      f"{t(lang, 'hourly_state')} {t(lang, 'toggle_on') if arg == 'on' else t(lang, 'toggle_off')}",
      reply_markup=menu_kb(hourly_on, daily_on, lang),
    )

  @dp.message(Command("daily"))
  async def daily_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip().lower()
    if arg not in ("on", "off"):
      await m.answer(t(lang, "daily_format"))
      return
    db.set_user_flag(m.from_user.id, "daily_enabled", arg == "on")
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      f"{t(lang, 'daily_state')} {t(lang, 'toggle_on') if arg == 'on' else t(lang, 'toggle_off')}",
      reply_markup=menu_kb(hourly_on, daily_on, lang),
    )

  @dp.message(Command("lang"))
  async def lang_cmd(m: Message, command: CommandObject):
    _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip().lower()
    if arg in ("en", "english", "uk", "ua", "ukrainian", "ru", "russian"):
      target = "uk" if arg in ("uk", "ua", "ukrainian") else ("ru" if arg in ("ru", "russian") else "en")
      db.set_lang(m.from_user.id, target)
    lang = db.get_lang(m.from_user.id)
    await m.answer(t(lang, "lang_prompt"), reply_markup=_lang_kb())

  @dp.message(Command("breaking"))
  async def breaking_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip().lower()
    if arg not in ("on", "off"):
      await m.answer(t(lang, "breaking_format"))
      return
    db.set_breaking(m.from_user.id, arg == "on")
    await m.answer(f"{t(lang, 'breaking_toggle')} {t(lang, 'toggle_on') if arg == 'on' else t(lang, 'toggle_off')}")

  @dp.message(Command("originals"))
  async def originals_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip().lower()
    if arg not in ("on", "off"):
      await m.answer(t(lang, "originals_format"))
      return
    db.set_originals_only(m.from_user.id, arg == "on")
    await m.answer(f"{t(lang, 'originals_toggle')} {t(lang, 'toggle_on') if arg == 'on' else t(lang, 'toggle_off')}")

  @dp.message(Command("kw"))
  async def kw_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    if not command.args:
      await m.answer(t(lang, "kw_format"))
      return
    parts = command.args.split(maxsplit=1)
    if len(parts) != 2 or parts[0].lower() not in ("include", "exclude"):
      await m.answer(t(lang, "kw_format"))
      return
    mode, value = parts[0].lower(), parts[1].strip()
    if mode == "include":
      db.set_keywords(m.from_user.id, include=value)
      await m.answer(t(lang, "include_set"))
    else:
      db.set_keywords(m.from_user.id, exclude=value)
      await m.answer(t(lang, "exclude_set"))

  @dp.message(Command("set"))
  async def set_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    if not command.args:
      await m.answer(t(lang, "set_format"))
      return
    parts = command.args.strip().split()
    if len(parts) != 2:
      await m.answer(t(lang, "set_format"))
      return
    key, val = parts[0].lower(), parts[1]
    try:
      if key == "daily_time":
        db.set_daily_time(m.from_user.id, val)
        await m.answer(f"🗓 {t(lang, 'status_daily_time')}: {val}")
      elif key == "hourly_minute":
        db.set_hourly_minute(m.from_user.id, int(val))
        await m.answer(f"⏱ {t(lang, 'status_hourly_min')}: {int(val):02d}")
      else:
        await m.answer(t(lang, "set_unknown"))
    except Exception as e:
      await m.answer(f"{t(lang, 'error')}: {e}")

  @dp.message(Command("tz"))
  async def tz_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "").strip()
    if not arg:
      sch = db.get_schedule(m.from_user.id)
      await m.answer(f"{t(lang, 'status_timezone')}: {sch.get('timezone', 'UTC')}")
      return
    try:
      ZoneInfo(arg)
    except Exception:
      await m.answer(t(lang, "tz_invalid"))
      return
    db.set_timezone(m.from_user.id, arg)
    await m.answer(f"{t(lang, 'status_timezone')}: {arg}")

  @dp.message(Command("quiet"))
  async def quiet_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    args = (command.args or "").strip().split()
    if not args:
      sch = db.get_schedule(m.from_user.id)
      await m.answer(
        f"{t(lang, 'status_quiet')}: {bool(sch.get('quiet_hours_enabled', 0))} "
        f"({sch.get('quiet_start', '23:00')}–{sch.get('quiet_end', '07:00')})"
      )
      return
    onoff = args[0].lower()
    if onoff not in ("on", "off"):
      await m.answer(t(lang, "quiet_format"))
      return
    start, end = None, None
    if len(args) >= 3:
      start, end = args[1], args[2]
      if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", start) or not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", end):
        await m.answer(t(lang, "quiet_format"))
        return
    db.set_quiet_hours(m.from_user.id, onoff == "on", start_hhmm=start, end_hhmm=end)
    sch = db.get_schedule(m.from_user.id)
    await m.answer(
      f"{t(lang, 'status_quiet')}: {bool(sch.get('quiet_hours_enabled', 0))} "
      f"({sch.get('quiet_start', '23:00')}–{sch.get('quiet_end', '07:00')})"
    )

  @dp.message(Command("topic"))
  async def topic_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    args = (command.args or "").strip()
    if not args:
      items = db.list_topic_profiles(m.from_user.id)
      if not items:
        await m.answer(t(lang, "topic_empty"))
        return
      lines = [t(lang, "topic_title")]
      for it in items:
        lines.append(
          f"• {it['name']} [{it['scope']}] enabled={bool(it['enabled'])} "
          f"+({(it.get('include_keywords') or '').strip() or '-'}) "
          f"-({(it.get('exclude_keywords') or '').strip() or '-'})"
        )
      await m.answer("\n".join(lines))
      return

    parts = args.split(maxsplit=2)
    action = parts[0].lower()
    if action == "del" and len(parts) >= 2:
      n = db.delete_topic_profile(m.from_user.id, parts[1])
      await m.answer(t(lang, "saved") if n else t(lang, "topic_not_found"))
      return
    if action in ("on", "off") and len(parts) >= 2:
      old = db.get_topic_profile(m.from_user.id, parts[1])
      if not old:
        await m.answer(t(lang, "topic_not_found"))
        return
      db.upsert_topic_profile(
        m.from_user.id,
        old["name"],
        include_keywords=old.get("include_keywords"),
        exclude_keywords=old.get("exclude_keywords"),
        scope=old.get("scope", "all"),
        enabled=(action == "on"),
      )
      await m.answer(t(lang, "saved"))
      return
    if action == "set" and len(parts) == 3:
      # /topic set geopolitics include=war,ukraine exclude=ads scope=all
      name = parts[1].strip().lower()
      tail = parts[2]
      inc = ""
      exc = ""
      scope = "all"
      for token in tail.split():
        low = token.lower()
        if low.startswith("include="):
          inc = token.split("=", 1)[1]
        elif low.startswith("exclude="):
          exc = token.split("=", 1)[1]
        elif low.startswith("scope="):
          scope = token.split("=", 1)[1].lower()
      if scope not in ("hourly", "daily", "all"):
        await m.answer(t(lang, "scope_format"))
        return
      db.upsert_topic_profile(m.from_user.id, name, include_keywords=inc, exclude_keywords=exc, scope=scope, enabled=True)
      await m.answer(t(lang, "saved"))
      return
    await m.answer(t(lang, "topic_format"))

  @dp.message(Command("health"))
  async def health_cmd(m: Message):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    metrics = db.get_metrics()
    st = db.status_summary(m.from_user.id)
    txt = (
      f"{t(lang, 'health_title')}\n"
      f"• posts_24h={st['posts_24h']}\n"
      f"• channels={st['hourly_channels'] + st['daily_channels']}\n"
      f"• digests_generated={metrics.get('digests_generated', 0)}\n"
      f"• digests_hourly_sent={metrics.get('digests_hourly_sent', 0)}\n"
      f"• digests_daily_sent={metrics.get('digests_daily_sent', 0)}\n"
      f"• alerts_sent={metrics.get('alerts_sent', 0)}\n"
      f"• posts_collected={metrics.get('posts_collected', 0)}"
    )
    await m.answer(txt)

  @dp.message(Command("monitor"))
  async def monitor_cmd(m: Message, command: CommandObject):
    _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    await monitoring.monitor_command(m, command.args or "", parse_channel_ref)

  @dp.message(Command("mreport"))
  async def mreport_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    arg = (command.args or "60").strip().lower()
    table = {"1h": 60, "2h": 120, "24h": 1440, "60": 60, "120": 120, "1440": 1440}
    mins = table.get(arg)
    if not mins:
      await m.answer(t(lang, "monitor_report_format"))
      return
    await monitoring.send_monitoring_report(bot, m.from_user.id, mins)

  @dp.message(Command("sources"))
  async def sources_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    mode = (command.args or "recent").strip().lower()
    if mode == "top":
      rows = db.list_top_channels_for_user(m.from_user.id, hours=24, limit=10)
      if not rows:
        await m.answer(t(lang, "empty"))
        return
      await m.answer(
        t(lang, "sources_top") + "\n" + "\n".join([f"• @{r['username']}: {r['posts']}" for r in rows])
      )
      return
    rows = db.list_recent_channels_for_user(m.from_user.id, limit=10)
    if not rows:
      await m.answer(t(lang, "empty"))
      return
    await m.answer(
      t(lang, "sources_recent") + "\n" + "\n".join([f"• @{r['username']} ({r['scope']})" for r in rows])
    )

  @dp.message(Command("now"))
  async def now_cmd(m: Message, command: CommandObject):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    period, scope = parse_period_scope(command.args)
    topic = parse_topic_arg(command.args)
    top_k = DIGEST_TOPK_HOURLY if period <= dt.timedelta(hours=2) else DIGEST_TOPK_DAILY
    await m.answer(t(lang, "collecting"))
    await send_digest(
      bot,
      m.from_user.id,
      period,
      top_k=top_k,
      title_prefix=t(lang, "digest_title"),
      scope=scope,
      topic_name=topic,
    )

  @dp.callback_query()
  async def callbacks(q: CallbackQuery, state: FSMContext):
    user_id = q.from_user.id
    lang = _ensure_user_and_lang(user_id, getattr(q.from_user, "language_code", None))
    data = q.data or ""
    hourly_on, daily_on = db.get_user_flags(user_id)

    if data.startswith("nav:"):
      view = data.split(":", 1)[1]
      if view == "monitor":
        await q.message.answer(_monitoring_text(user_id, lang), reply_markup=menu_kb(hourly_on, daily_on, lang, view="monitor"))
      else:
        await q.message.answer(t(lang, "menu_hint"), reply_markup=menu_kb(hourly_on, daily_on, lang, view=view))
      await q.answer()
      return

    if data == "mon:on":
      db.set_monitoring_enabled(user_id, True)
      await q.message.answer(_monitoring_text(user_id, lang), reply_markup=menu_kb(hourly_on, daily_on, lang, view="monitor"))
      await q.answer()
      return

    if data == "mon:add":
      await state.set_state(AddRemoveFlow.waiting_monitor_add)
      await q.message.answer(t(lang, "waiting_channel"))
      await q.answer()
      return

    if data == "mon:rm":
      await state.set_state(AddRemoveFlow.waiting_monitor_rm)
      await q.message.answer(t(lang, "waiting_channel"))
      await q.answer()
      return

    if data.startswith("mon:"):
      handled = await monitoring.handle_monitor_callback(q, bot)
      if handled:
        hourly_on, daily_on = db.get_user_flags(user_id)
        await q.message.answer(_monitoring_text(user_id, lang), reply_markup=menu_kb(hourly_on, daily_on, lang, view="monitor"))
        return

    if data == "lists":
      await q.message.answer(format_lists(user_id, lang), reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"))
      await q.answer()
      return

    if data == "sources:recent":
      rows = db.list_recent_channels_for_user(user_id, limit=10)
      if rows:
        txt = t(lang, "sources_recent") + "\n" + "\n".join([f"• @{r['username']} ({r['scope']})" for r in rows])
      else:
        txt = t(lang, "empty")
      await q.message.answer(txt, reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"))
      await q.answer()
      return

    if data == "sources:top":
      rows = db.list_top_channels_for_user(user_id, hours=24, limit=10)
      if rows:
        txt = t(lang, "sources_top") + "\n" + "\n".join([f"• @{r['username']}: {r['posts']}" for r in rows])
      else:
        txt = t(lang, "empty")
      await q.message.answer(txt, reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"))
      await q.answer()
      return

    if data.startswith("toggle:"):
      sc = data.split(":", 1)[1]
      if sc == "hourly":
        db.set_user_flag(user_id, "hourly_enabled", not hourly_on)
      elif sc == "daily":
        db.set_user_flag(user_id, "daily_enabled", not daily_on)
      elif sc == "breaking":
        s = db.get_user_settings(user_id)
        db.set_breaking(user_id, not bool(s.get("breaking_enabled", 0)))
      elif sc == "originals":
        s = db.get_user_settings(user_id)
        db.set_originals_only(user_id, not bool(s.get("originals_only", 0)))
      hourly_on, daily_on = db.get_user_flags(user_id)
      view = "main" if sc in ("hourly", "daily") else ("breaking" if sc == "breaking" else "settings")
      await q.message.answer(t(lang, "saved"), reply_markup=menu_kb(hourly_on, daily_on, lang, view=view))
      await q.answer()
      return

    if data.startswith("now:"):
      _, p, scope = data.split(":")
      period = dt.timedelta(hours=int(p[:-1])) if p.endswith("h") else dt.timedelta(days=int(p[:-1]))
      top_k = DIGEST_TOPK_HOURLY if period <= dt.timedelta(hours=2) else DIGEST_TOPK_DAILY
      await q.message.answer(t(lang, "collecting"))
      await send_digest(bot, user_id, period, top_k=top_k, title_prefix=t(lang, "digest_title"), scope=scope)
      hourly_on, daily_on = db.get_user_flags(user_id)
      await q.message.answer(t(lang, "menu_hint"), reply_markup=menu_kb(hourly_on, daily_on, lang, view="digest"))
      await q.answer()
      return

    if data.startswith("brk:sources:"):
      s = db.get_user_settings(user_id)
      cur = int(s.get("breaking_sources", 8))
      delta = data.split(":")[-1]
      cur = max(2, cur + (1 if delta == "+1" else -1))
      db.set_breaking_params(user_id, sources=cur)
      await q.message.answer(
        f"🚨 {t(lang, 'status_breaking_sources')}: {cur}",
        reply_markup=menu_kb(hourly_on, daily_on, lang, view="breaking"),
      )
      await q.answer()
      return

    if data.startswith("brk:window:"):
      s = db.get_user_settings(user_id)
      cur = int(s.get("breaking_window_min", 10))
      delta = data.split(":")[-1]
      cur = max(2, cur + (1 if delta == "+1" else -1))
      db.set_breaking_params(user_id, window_min=cur)
      await q.message.answer(
        f"⏲ {t(lang, 'status_breaking_window')}: {cur}",
        reply_markup=menu_kb(hourly_on, daily_on, lang, view="breaking"),
      )
      await q.answer()
      return

    if data.startswith("sched:hourly:"):
      delta = data.split(":")[-1]
      sch = db.get_schedule(user_id)
      cur = int(sch.get("hourly_minute", 2))
      cur = (cur + 1) % 60 if delta == "+1" else (cur - 1) % 60
      db.set_hourly_minute(user_id, cur)
      await q.message.answer(
        f"⏱ {t(lang, 'status_hourly_min')}: {cur:02d}",
        reply_markup=menu_kb(hourly_on, daily_on, lang, view="schedule"),
      )
      await q.answer()
      return

    if data == "sched:daily:custom":
      await state.set_state(AddRemoveFlow.waiting_daily_time)
      await q.message.answer(t(lang, "send_daily_time"))
      await q.answer()
      return

    if data == "kw:include":
      await state.set_state(AddRemoveFlow.waiting_include_kw)
      await q.message.answer(t(lang, "send_kw_include"))
      await q.answer()
      return

    if data == "kw:exclude":
      await state.set_state(AddRemoveFlow.waiting_exclude_kw)
      await q.message.answer(t(lang, "send_kw_exclude"))
      await q.answer()
      return

    if data.startswith("mute_brk:"):
      alert_key = data.split("mute_brk:", 1)[1]
      db.mute_alert(user_id, alert_key)
      await q.message.answer(t(lang, "mute_done"))
      await q.answer()
      return

    if data == "lang":
      await q.message.answer(t(lang, "lang_prompt"), reply_markup=_lang_kb())
      await q.answer()
      return

    if data.startswith("setlang:"):
      new_lang = data.split("setlang:", 1)[1]
      db.set_lang(user_id, new_lang)
      lang = db.get_lang(user_id)
      hourly_on, daily_on = db.get_user_flags(user_id)
      label = "English" if lang == "en" else ("Українська" if lang == "uk" else "Русский")
      await q.message.answer(f"✅ {t(lang, 'lang')}: {label}", reply_markup=menu_kb(hourly_on, daily_on, lang, view="main"))
      await q.answer()
      return

    if data == "status":
      hourly_on, daily_on = db.get_user_flags(user_id)
      await q.message.answer(
        _status_text(user_id, lang, hourly_on, daily_on),
        reply_markup=menu_kb(hourly_on, daily_on, lang, view="main"),
      )
      await q.answer()
      return

    if data.startswith("add:"):
      sc = data.split(":", 1)[1]
      if sc == "hourly":
        await state.set_state(AddRemoveFlow.waiting_add_hourly)
      else:
        await state.set_state(AddRemoveFlow.waiting_add_daily)
      await q.message.answer(t(lang, "waiting_channel"))
      await q.answer()
      return

    if data.startswith("rm:"):
      sc = data.split(":", 1)[1]
      if sc == "hourly":
        await state.set_state(AddRemoveFlow.waiting_rm_hourly)
      else:
        await state.set_state(AddRemoveFlow.waiting_rm_daily)
      await q.message.answer(t(lang, "waiting_channel"))
      await q.answer()
      return

    await q.answer()

  @dp.message(AddRemoveFlow.waiting_add_hourly)
  async def add_h(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    db.add_channel_for_user(m.from_user.id, ch, "hourly")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      t(lang, "added_to_hourly").format(ch=f"@{ch}"),
      reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"),
    )

  @dp.message(AddRemoveFlow.waiting_add_daily)
  async def add_d(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    db.add_channel_for_user(m.from_user.id, ch, "daily")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      t(lang, "added_to_daily").format(ch=f"@{ch}"),
      reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"),
    )

  @dp.message(AddRemoveFlow.waiting_rm_hourly)
  async def rm_h(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    n = db.remove_channel_for_user(m.from_user.id, ch, "hourly")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    if n:
      msg = t(lang, "removed_from_hourly").format(ch=f"@{ch}")
    else:
      msg = t(lang, "not_found_scope").format(ch=f"@{ch}", scope="hourly")
    await m.answer(msg, reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"))

  @dp.message(AddRemoveFlow.waiting_rm_daily)
  async def rm_d(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    n = db.remove_channel_for_user(m.from_user.id, ch, "daily")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    if n:
      msg = t(lang, "removed_from_daily").format(ch=f"@{ch}")
    else:
      msg = t(lang, "not_found_scope").format(ch=f"@{ch}", scope="daily")
    await m.answer(msg, reply_markup=menu_kb(hourly_on, daily_on, lang, view="channels"))

  @dp.message(AddRemoveFlow.waiting_monitor_add)
  async def mon_add(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    db.add_monitor_channel(m.from_user.id, ch)
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      t(lang, "added_to_monitor").format(ch=f"@{ch}"),
      reply_markup=menu_kb(hourly_on, daily_on, lang, view="monitor"),
    )

  @dp.message(AddRemoveFlow.waiting_monitor_rm)
  async def mon_rm(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    ch = parse_channel_ref(m.text)
    if not ch:
      await m.answer(t(lang, "invalid_channel"))
      return
    n = db.remove_monitor_channel(m.from_user.id, ch)
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    if n:
      msg = t(lang, "removed_from_monitor").format(ch=f"@{ch}")
    else:
      msg = t(lang, "not_found_scope").format(ch=f"@{ch}", scope="monitor")
    await m.answer(msg, reply_markup=menu_kb(hourly_on, daily_on, lang, view="monitor"))

  @dp.message(AddRemoveFlow.waiting_daily_time)
  async def set_daily_time_flow(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    txt = (m.text or "").strip()
    try:
      db.set_daily_time(m.from_user.id, txt)
    except Exception as e:
      await m.answer(f"{t(lang, 'error')}: {e}")
      return
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(
      f"🗓 {t(lang, 'status_daily_time')}: {txt}",
      reply_markup=menu_kb(hourly_on, daily_on, lang, view="schedule"),
    )

  @dp.message(AddRemoveFlow.waiting_include_kw)
  async def set_inc_kw_flow(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    txt = (m.text or "").strip()
    db.set_keywords(m.from_user.id, include=txt if txt else "")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(t(lang, "include_set"), reply_markup=menu_kb(hourly_on, daily_on, lang, view="keywords"))

  @dp.message(AddRemoveFlow.waiting_exclude_kw)
  async def set_exc_kw_flow(m: Message, state: FSMContext):
    lang = db.get_lang(m.from_user.id)
    txt = (m.text or "").strip()
    db.set_keywords(m.from_user.id, exclude=txt if txt else "")
    await state.clear()
    hourly_on, daily_on = db.get_user_flags(m.from_user.id)
    await m.answer(t(lang, "exclude_set"), reply_markup=menu_kb(hourly_on, daily_on, lang, view="keywords"))

  @dp.message()
  async def fallback(m: Message):
    lang = _ensure_user_and_lang(m.from_user.id, getattr(m.from_user, "language_code", None))
    if (m.text or "").strip() == t(lang, "menu_open"):
      await _send_main_menu(m, lang)
      return
    if m.text and m.text.startswith("/"):
      await m.answer(build_help(lang))

  return bot, dp
