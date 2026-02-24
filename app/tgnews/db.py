from __future__ import annotations
import os
import sqlite3
import datetime as dt
from contextlib import contextmanager
from typing import Optional, Tuple, List, Dict

DB_PATH = os.getenv("DB_PATH", "/data/app.db")

# user_version:
# 0 -> new / unknown
# 1 -> old schema (single list user_channels)
# 2 -> scopes (hourly/daily)

SCHEMA_V2 = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  hourly_enabled INTEGER NOT NULL DEFAULT 0,
  daily_enabled INTEGER NOT NULL DEFAULT 0,
  hourly_minute INTEGER NOT NULL DEFAULT 2,
  daily_time TEXT NOT NULL DEFAULT '09:00',
  last_hourly_sent_hour TEXT,
  last_daily_sent_date TEXT,
breaking_enabled INTEGER NOT NULL DEFAULT 0,
breaking_sources INTEGER NOT NULL DEFAULT 8,
breaking_window_min INTEGER NOT NULL DEFAULT 10,
originals_only INTEGER NOT NULL DEFAULT 0,
include_keywords TEXT,
exclude_keywords TEXT,
lang TEXT NOT NULL DEFAULT 'en',
timezone TEXT NOT NULL DEFAULT 'UTC',
quiet_hours_enabled INTEGER NOT NULL DEFAULT 0,
quiet_start TEXT NOT NULL DEFAULT '23:00',
quiet_end TEXT NOT NULL DEFAULT '07:00',
noise_keywords TEXT
);

CREATE TABLE IF NOT EXISTS user_channels (
  user_id INTEGER NOT NULL,
  username TEXT NOT NULL,
  scope TEXT NOT NULL CHECK(scope IN ('hourly','daily')),
  added_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, username, scope),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS channels (
  username TEXT PRIMARY KEY,
  tg_id INTEGER,
  title TEXT,
  last_msg_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS muted_alerts (
  user_id INTEGER NOT NULL,
  alert_key TEXT NOT NULL,
  muted_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, alert_key)
);

CREATE TABLE IF NOT EXISTS alerts (
  user_id INTEGER NOT NULL,
  alert_key TEXT NOT NULL,
  sent_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, alert_key)
);

CREATE TABLE IF NOT EXISTS channel_stats (
  username TEXT PRIMARY KEY,
  window_hours INTEGER NOT NULL DEFAULT 24,
  total_posts INTEGER NOT NULL DEFAULT 0,
  unique_posts INTEGER NOT NULL DEFAULT 0,
  forwards INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel_username TEXT NOT NULL,
  msg_id INTEGER NOT NULL,
  date_utc TEXT NOT NULL,
  text TEXT NOT NULL,
  link TEXT NOT NULL,
  norm_hash TEXT NOT NULL,
  simhash INTEGER NOT NULL, -- signed 64-bit
  is_forward INTEGER NOT NULL DEFAULT 0,
  fwd_from TEXT,
  url_canonical TEXT,
  url_hash TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(channel_username, msg_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(date_utc);
CREATE INDEX IF NOT EXISTS idx_posts_hash ON posts(norm_hash);
CREATE INDEX IF NOT EXISTS idx_posts_simhash ON posts(simhash);

CREATE TABLE IF NOT EXISTS user_topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  include_keywords TEXT,
  exclude_keywords TEXT,
  scope TEXT NOT NULL DEFAULT 'all' CHECK(scope IN ('hourly','daily','all')),
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(user_id, name)
);

CREATE TABLE IF NOT EXISTS app_metrics (
  key TEXT PRIMARY KEY,
  value INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

PRAGMA user_version=2;
"""

@contextmanager
def connect():
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  try:
    yield conn
    conn.commit()
  finally:
    conn.close()

def _get_user_version(conn) -> int:
  row = conn.execute("PRAGMA user_version").fetchone()
  return int(row[0]) if row else 0

def _table_exists(conn, name: str) -> bool:
  row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
  return bool(row)

def _migrate_v1_to_v2(conn):
  # v1 had user_channels(user_id, username, added_at) with PK (user_id, username)
  # We'll migrate by putting everything into scope='daily' (safe default).
  conn.execute("ALTER TABLE user_channels RENAME TO user_channels_v1")
  conn.execute("""
    CREATE TABLE user_channels (
      user_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      scope TEXT NOT NULL CHECK(scope IN ('hourly','daily')),
      added_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (user_id, username, scope),
      FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
    )
  """)
  conn.execute("""
    INSERT INTO user_channels(user_id, username, scope, added_at)
    SELECT user_id, username, 'daily', added_at FROM user_channels_v1
  """)
  conn.execute("DROP TABLE user_channels_v1")
  conn.execute("PRAGMA user_version=2")


def _ensure_user_columns(conn):
  cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
  wanted = {
    "hourly_minute": "INTEGER NOT NULL DEFAULT 2",
    "daily_time": "TEXT NOT NULL DEFAULT '09:00'",
    "last_hourly_sent_hour": "TEXT",
    "last_daily_sent_date": "TEXT",
    "breaking_enabled": "INTEGER NOT NULL DEFAULT 0",
    "breaking_sources": "INTEGER NOT NULL DEFAULT 8",
    "breaking_window_min": "INTEGER NOT NULL DEFAULT 10",
    "originals_only": "INTEGER NOT NULL DEFAULT 0",
    "include_keywords": "TEXT",
    "exclude_keywords": "TEXT",
    "lang": "TEXT NOT NULL DEFAULT 'en'",
    "timezone": "TEXT NOT NULL DEFAULT 'UTC'",
    "quiet_hours_enabled": "INTEGER NOT NULL DEFAULT 0",
    "quiet_start": "TEXT NOT NULL DEFAULT '23:00'",
    "quiet_end": "TEXT NOT NULL DEFAULT '07:00'",
    "noise_keywords": "TEXT",
    "monitor_enabled": "INTEGER NOT NULL DEFAULT 0",
    "monitor_interval_min": "INTEGER NOT NULL DEFAULT 2",
    "monitor_last_slot": "TEXT",
    "monitor_pause_until_utc": "TEXT",
    "monitor_include_keywords": "TEXT",
    "monitor_exclude_keywords": "TEXT",
    "monitor_categories": "TEXT NOT NULL DEFAULT 'all'",
    "monitor_antiflood_min": "INTEGER NOT NULL DEFAULT 7",
  }
  for name, ddl in wanted.items():
    if name not in cols:
      conn.execute(f"ALTER TABLE users ADD COLUMN {name} {ddl}")

def _ensure_posts_columns(conn):
  cols = [r["name"] for r in conn.execute("PRAGMA table_info(posts)").fetchall()] if _table_exists(conn, "posts") else []
  wanted = {
    "is_forward": "INTEGER NOT NULL DEFAULT 0",
    "fwd_from": "TEXT",
    "url_canonical": "TEXT",
    "url_hash": "TEXT",
  }
  for name, ddl in wanted.items():
    if name not in cols and cols:
      conn.execute(f"ALTER TABLE posts ADD COLUMN {name} {ddl}")
  cols_after = [r["name"] for r in conn.execute("PRAGMA table_info(posts)").fetchall()] if _table_exists(conn, "posts") else []
  if "url_hash" in cols_after:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_url_hash ON posts(url_hash)")

def _ensure_stats_table(conn):
  conn.executescript("""
    CREATE TABLE IF NOT EXISTS muted_alerts (
  user_id INTEGER NOT NULL,
  alert_key TEXT NOT NULL,
  muted_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, alert_key)
);

CREATE TABLE IF NOT EXISTS alerts (
  user_id INTEGER NOT NULL,
  alert_key TEXT NOT NULL,
  sent_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY(user_id, alert_key)
);

CREATE TABLE IF NOT EXISTS channel_stats (
      username TEXT PRIMARY KEY,
      window_hours INTEGER NOT NULL DEFAULT 24,
      total_posts INTEGER NOT NULL DEFAULT 0,
      unique_posts INTEGER NOT NULL DEFAULT 0,
      forwards INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
	  """)


def _ensure_extra_tables(conn):
  conn.executescript("""
    CREATE TABLE IF NOT EXISTS user_topics (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      include_keywords TEXT,
      exclude_keywords TEXT,
      scope TEXT NOT NULL DEFAULT 'all' CHECK(scope IN ('hourly','daily','all')),
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS app_metrics (
      key TEXT PRIMARY KEY,
      value INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS user_monitor_channels (
      user_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      added_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (user_id, username)
    );
  """)



def init_db():
  with connect() as conn:
    uv = _get_user_version(conn)

    # Fresh DB
    if uv == 0 and not _table_exists(conn, "users"):
      conn.executescript(SCHEMA_V2)
      _ensure_user_columns(conn)
      _ensure_posts_columns(conn)
      _ensure_stats_table(conn)
      _ensure_extra_tables(conn)
      return

    # v1 -> v2 migration
    if _table_exists(conn, "user_channels"):
      # Detect if scope column exists
      cols = [r["name"] for r in conn.execute("PRAGMA table_info(user_channels)").fetchall()]
      if "scope" not in cols:
        _migrate_v1_to_v2(conn)

    # Ensure all tables exist (idempotent)
    conn.executescript(SCHEMA_V2)
    _ensure_user_columns(conn)
    _ensure_posts_columns(conn)
    _ensure_stats_table(conn)
    _ensure_extra_tables(conn)

def ensure_user(user_id: int) -> bool:
  with connect() as conn:
    cur = conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,))
    return cur.rowcount == 1

def set_user_flag(user_id: int, flag: str, enabled: bool):
  if flag not in ("hourly_enabled", "daily_enabled"):
    raise ValueError("invalid flag")
  with connect() as conn:
    conn.execute(f"UPDATE users SET {flag}=? WHERE user_id=?", (1 if enabled else 0, user_id))

def get_user_flags(user_id: int) -> Tuple[bool, bool]:
  with connect() as conn:
    row = conn.execute("SELECT hourly_enabled, daily_enabled FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not row:
      return False, False
    return bool(row["hourly_enabled"]), bool(row["daily_enabled"])

def add_channel_for_user(user_id: int, username: str, scope: str):
  username = username.lstrip("@").strip()
  scope = scope.strip().lower()
  if scope not in ("hourly", "daily"):
    raise ValueError("scope must be hourly or daily")
  with connect() as conn:
    conn.execute("INSERT OR IGNORE INTO user_channels(user_id, username, scope) VALUES (?,?,?)", (user_id, username, scope))

def remove_channel_for_user(user_id: int, username: str, scope: str) -> int:
  username = username.lstrip("@").strip()
  scope = scope.strip().lower()
  if scope not in ("hourly", "daily"):
    raise ValueError("scope must be hourly or daily")
  with connect() as conn:
    cur = conn.execute("DELETE FROM user_channels WHERE user_id=? AND username=? AND scope=?", (user_id, username, scope))
    return cur.rowcount

def list_channels_for_user(user_id: int, scope: str) -> List[str]:
  scope = scope.strip().lower()
  if scope not in ("hourly", "daily"):
    raise ValueError("scope must be hourly or daily")
  with connect() as conn:
    rows = conn.execute(
      "SELECT username FROM user_channels WHERE user_id=? AND scope=? ORDER BY username",
      (user_id, scope)
    ).fetchall()
    return [r["username"] for r in rows]

def list_all_tracked_channels() -> List[str]:
  with connect() as conn:
    rows = conn.execute("SELECT DISTINCT username FROM user_channels ORDER BY username").fetchall()
    return [r["username"] for r in rows]


def list_all_collected_channels() -> List[str]:
  with connect() as conn:
    rows_main = conn.execute("SELECT DISTINCT username FROM user_channels").fetchall()
    rows_mon = []
    if _table_exists(conn, "user_monitor_channels"):
      rows_mon = conn.execute("SELECT DISTINCT username FROM user_monitor_channels").fetchall()
    merged = {str(r["username"]) for r in rows_main}
    merged.update({str(r["username"]) for r in rows_mon})
    return sorted([x for x in merged if x])

def upsert_channel_meta(username: str, tg_id: Optional[int], title: Optional[str]):
  with connect() as conn:
    conn.execute("""
      INSERT INTO channels(username, tg_id, title, updated_at) VALUES (?,?,?,datetime('now'))
      ON CONFLICT(username) DO UPDATE SET tg_id=COALESCE(excluded.tg_id, channels.tg_id),
                                       title=COALESCE(excluded.title, channels.title),
                                       updated_at=datetime('now')
    """, (username, tg_id, title))

def get_channel_last_msg_id(username: str) -> int:
  with connect() as conn:
    row = conn.execute("SELECT last_msg_id FROM channels WHERE username=?", (username,)).fetchone()
    return int(row["last_msg_id"]) if row else 0

def set_channel_last_msg_id(username: str, last_msg_id: int):
  with connect() as conn:
    conn.execute("""
      INSERT INTO channels(username, last_msg_id, updated_at) VALUES (?,?,datetime('now'))
      ON CONFLICT(username) DO UPDATE SET last_msg_id=excluded.last_msg_id, updated_at=datetime('now')
    """, (username, int(last_msg_id)))

def insert_post(
  channel_username: str,
  msg_id: int,
  date_utc: str,
  text: str,
  link: str,
  norm_hash: str,
  simhash: int,
  is_forward: int = 0,
  fwd_from: str | None = None,
  url_canonical: str | None = None,
  url_hash: str | None = None,
) -> bool:
  with connect() as conn:
    try:
      conn.execute("""
        INSERT INTO posts(channel_username, msg_id, date_utc, text, link, norm_hash, simhash, is_forward, fwd_from, url_canonical, url_hash)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
      """, (
        channel_username, int(msg_id), date_utc, text, link, norm_hash, int(simhash),
        int(is_forward), fwd_from, url_canonical, url_hash,
      ))
      return True
    except sqlite3.IntegrityError:
      return False

def get_posts_for_user_between(user_id: int, start_utc: str, end_utc: str, scope: str):
  scope = scope.strip().lower()
  if scope not in ("hourly", "daily", "all"):
    raise ValueError("scope must be hourly, daily or all")
  where_scope = ""
  params = [start_utc, end_utc, user_id]
  if scope in ("hourly", "daily"):
    where_scope = "AND uc.scope=?"
    params.append(scope)

  with connect() as conn:
    rows = conn.execute(f"""
      SELECT p.* FROM posts p
      WHERE p.date_utc>=? AND p.date_utc<?
        AND EXISTS (
          SELECT 1 FROM user_channels uc
          WHERE uc.user_id=? AND uc.username=p.channel_username {where_scope}
        )
      ORDER BY p.date_utc DESC
    """, tuple(params)).fetchall()
    return [dict(r) for r in rows]

def status_summary(user_id: int) -> Dict[str, object]:
  with connect() as conn:
    # counts
    hc = conn.execute("SELECT COUNT(*) AS n FROM user_channels WHERE user_id=? AND scope='hourly'", (user_id,)).fetchone()["n"]
    dc = conn.execute("SELECT COUNT(*) AS n FROM user_channels WHERE user_id=? AND scope='daily'", (user_id,)).fetchone()["n"]
    since_24h = conn.execute("SELECT strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-24 hours') AS ts").fetchone()["ts"]
    p24 = conn.execute(
      "SELECT COUNT(DISTINCT p.id) AS n FROM posts p JOIN user_channels uc ON uc.username=p.channel_username "
      "WHERE uc.user_id=? AND p.date_utc >= ?",
      (user_id, since_24h),
    ).fetchone()["n"]
    # last updated channel meta
    last = conn.execute("SELECT username, last_msg_id, updated_at FROM channels ORDER BY updated_at DESC LIMIT 1").fetchone()
    last_obj = dict(last) if last else None
  return {"hourly_channels": int(hc), "daily_channels": int(dc), "posts_24h": int(p24), "last_channel": last_obj}


def set_hourly_minute(user_id: int, minute: int):
  minute = int(minute)
  if minute < 0 or minute > 59:
    raise ValueError("minute must be 0..59")
  with connect() as conn:
    conn.execute("UPDATE users SET hourly_minute=? WHERE user_id=?", (minute, user_id))

def set_daily_time(user_id: int, hhmm: str):
  import re as _re
  hhmm = (hhmm or "").strip()
  if not _re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", hhmm):
    raise ValueError("time must be HH:MM (00:00..23:59)")
  with connect() as conn:
    conn.execute("UPDATE users SET daily_time=? WHERE user_id=?", (hhmm, user_id))

def get_schedule(user_id: int) -> Dict[str, object]:
  with connect() as conn:
    row = conn.execute(
      "SELECT hourly_minute, daily_time, last_hourly_sent_hour, last_daily_sent_date, timezone, "
      "quiet_hours_enabled, quiet_start, quiet_end, monitor_enabled, monitor_interval_min, "
      "monitor_last_slot, monitor_pause_until_utc "
      "FROM users WHERE user_id=?",
      (user_id,),
    ).fetchone()
    if not row:
      return {
        "hourly_minute": 2,
        "daily_time": "09:00",
        "last_hourly_sent_hour": None,
        "last_daily_sent_date": None,
        "timezone": "UTC",
        "quiet_hours_enabled": 0,
        "quiet_start": "23:00",
        "quiet_end": "07:00",
        "monitor_enabled": 0,
        "monitor_interval_min": 2,
        "monitor_last_slot": None,
        "monitor_pause_until_utc": None,
      }
    return dict(row)


def set_timezone(user_id: int, timezone_name: str):
  with connect() as conn:
    conn.execute("UPDATE users SET timezone=? WHERE user_id=?", (timezone_name, int(user_id)))


def set_quiet_hours(user_id: int, enabled: bool, start_hhmm: str | None = None, end_hhmm: str | None = None):
  with connect() as conn:
    conn.execute("UPDATE users SET quiet_hours_enabled=? WHERE user_id=?", (1 if enabled else 0, int(user_id)))
    if start_hhmm is not None:
      conn.execute("UPDATE users SET quiet_start=? WHERE user_id=?", (start_hhmm, int(user_id)))
    if end_hhmm is not None:
      conn.execute("UPDATE users SET quiet_end=? WHERE user_id=?", (end_hhmm, int(user_id)))

def mark_hourly_sent(user_id: int, hour_key: str):
  with connect() as conn:
    conn.execute("UPDATE users SET last_hourly_sent_hour=? WHERE user_id=?", (hour_key, user_id))

def mark_daily_sent(user_id: int, date_key: str):
  with connect() as conn:
    conn.execute("UPDATE users SET last_daily_sent_date=? WHERE user_id=?", (date_key, user_id))

def list_users_with_flag(flag: str) -> List[int]:
  if flag not in ("hourly_enabled", "daily_enabled"):
    raise ValueError("invalid flag")
  with connect() as conn:
    rows = conn.execute(f"SELECT user_id FROM users WHERE {flag}=1").fetchall()
    return [int(r["user_id"]) for r in rows]


def set_breaking(user_id: int, enabled: bool):
  with connect() as conn:
    conn.execute("UPDATE users SET breaking_enabled=? WHERE user_id=?", (1 if enabled else 0, user_id))

def set_breaking_params(user_id: int, sources: int | None = None, window_min: int | None = None):
  with connect() as conn:
    if sources is not None:
      conn.execute("UPDATE users SET breaking_sources=? WHERE user_id=?", (int(sources), user_id))
    if window_min is not None:
      conn.execute("UPDATE users SET breaking_window_min=? WHERE user_id=?", (int(window_min), user_id))

def set_originals_only(user_id: int, enabled: bool):
  with connect() as conn:
    conn.execute("UPDATE users SET originals_only=? WHERE user_id=?", (1 if enabled else 0, user_id))

def set_keywords(user_id: int, include: str | None = None, exclude: str | None = None, noise: str | None = None):
  with connect() as conn:
    if include is not None:
      conn.execute("UPDATE users SET include_keywords=? WHERE user_id=?", (include, int(user_id)))
    if exclude is not None:
      conn.execute("UPDATE users SET exclude_keywords=? WHERE user_id=?", (exclude, int(user_id)))
    if noise is not None:
      conn.execute("UPDATE users SET noise_keywords=? WHERE user_id=?", (noise, int(user_id)))

def get_user_settings(user_id: int) -> Dict[str, object]:
  with connect() as conn:
    row = conn.execute("""
      SELECT hourly_enabled,daily_enabled,hourly_minute,daily_time,
             breaking_enabled,breaking_sources,breaking_window_min,
             originals_only, include_keywords, exclude_keywords,
             last_hourly_sent_hour,last_daily_sent_date,
             timezone, quiet_hours_enabled, quiet_start, quiet_end,
             noise_keywords,
             monitor_enabled, monitor_interval_min, monitor_last_slot, monitor_pause_until_utc,
             monitor_include_keywords, monitor_exclude_keywords, monitor_categories,
             monitor_antiflood_min
      FROM users WHERE user_id=?
    """, (user_id,)).fetchone()
    return dict(row) if row else {}

def list_users_breaking_enabled() -> List[int]:
  with connect() as conn:
    rows = conn.execute("SELECT user_id FROM users WHERE breaking_enabled=1").fetchall()
    return [int(r["user_id"]) for r in rows]

def upsert_channel_stats(username: str, window_hours: int, total: int, unique: int, forwards: int):
  with connect() as conn:
    conn.execute("""
      INSERT INTO channel_stats(username, window_hours, total_posts, unique_posts, forwards, updated_at)
      VALUES (?,?,?,?,?,datetime('now'))
      ON CONFLICT(username) DO UPDATE SET
        window_hours=excluded.window_hours,
        total_posts=excluded.total_posts,
        unique_posts=excluded.unique_posts,
        forwards=excluded.forwards,
        updated_at=datetime('now')
    """, (username, int(window_hours), int(total), int(unique), int(forwards)))

def get_channel_stats(username: str) -> Optional[Dict[str, object]]:
  with connect() as conn:
    row = conn.execute("SELECT * FROM channel_stats WHERE username=?", (username,)).fetchone()
    return dict(row) if row else None

def list_top_spammy_channels(limit: int = 8) -> List[Dict[str, object]]:
  with connect() as conn:
    rows = conn.execute("""
      SELECT username, total_posts, unique_posts, forwards,
             CASE WHEN total_posts=0 THEN 0.0 ELSE (CAST(unique_posts AS REAL)/total_posts) END AS uniqueness
      FROM channel_stats
      ORDER BY uniqueness ASC, total_posts DESC
      LIMIT ?
    """, (int(limit),)).fetchall()
    return [dict(r) for r in rows]

def alert_recently_sent(user_id: int, alert_key: str, cooldown_min: int = 30) -> bool:
  with connect() as conn:
    row = conn.execute(
      "SELECT sent_at FROM alerts WHERE user_id=? AND alert_key=? AND sent_at >= datetime('now', ?)",
      (int(user_id), alert_key, f"-{int(cooldown_min)} minutes")
    ).fetchone()
    return bool(row)

def mark_alert_sent(user_id: int, alert_key: str):
  with connect() as conn:
    conn.execute(
      "INSERT INTO alerts(user_id, alert_key, sent_at) VALUES (?,?,datetime('now')) "
      "ON CONFLICT(user_id, alert_key) DO UPDATE SET sent_at=datetime('now')",
      (int(user_id), alert_key)
    )

def is_alert_muted(user_id: int, alert_key: str) -> bool:
  with connect() as conn:
    row = conn.execute("SELECT 1 FROM muted_alerts WHERE user_id=? AND alert_key=?",
                       (int(user_id), alert_key)).fetchone()
    return bool(row)

def mute_alert(user_id: int, alert_key: str):
  with connect() as conn:
    conn.execute(
      "INSERT INTO muted_alerts(user_id, alert_key, muted_at) VALUES (?,?,datetime('now')) "
      "ON CONFLICT(user_id, alert_key) DO UPDATE SET muted_at=datetime('now')",
      (int(user_id), alert_key)
    )

def set_lang(user_id: int, lang: str):
  from .i18n import norm_lang
  lang = norm_lang(lang)
  with connect() as conn:
    conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang, int(user_id)))

def get_lang(user_id: int) -> str:
  with connect() as conn:
    row = conn.execute("SELECT lang FROM users WHERE user_id=?", (int(user_id),)).fetchone()
    return (row["lang"] if row and row["lang"] else "en")


def upsert_topic_profile(
  user_id: int,
  name: str,
  include_keywords: str | None = None,
  exclude_keywords: str | None = None,
  scope: str = "all",
  enabled: bool = True,
):
  if scope not in ("hourly", "daily", "all"):
    raise ValueError("scope must be hourly, daily or all")
  with connect() as conn:
    conn.execute(
      "INSERT INTO user_topics(user_id, name, include_keywords, exclude_keywords, scope, enabled) "
      "VALUES (?,?,?,?,?,?) "
      "ON CONFLICT(user_id, name) DO UPDATE SET "
      "include_keywords=excluded.include_keywords, exclude_keywords=excluded.exclude_keywords, "
      "scope=excluded.scope, enabled=excluded.enabled",
      (int(user_id), name.strip().lower(), include_keywords, exclude_keywords, scope, 1 if enabled else 0),
    )


def delete_topic_profile(user_id: int, name: str) -> int:
  with connect() as conn:
    cur = conn.execute(
      "DELETE FROM user_topics WHERE user_id=? AND name=?",
      (int(user_id), name.strip().lower()),
    )
    return int(cur.rowcount)


def list_topic_profiles(user_id: int) -> List[Dict[str, object]]:
  with connect() as conn:
    rows = conn.execute(
      "SELECT name, include_keywords, exclude_keywords, scope, enabled "
      "FROM user_topics WHERE user_id=? ORDER BY name",
      (int(user_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_topic_profile(user_id: int, name: str) -> Optional[Dict[str, object]]:
  with connect() as conn:
    row = conn.execute(
      "SELECT name, include_keywords, exclude_keywords, scope, enabled "
      "FROM user_topics WHERE user_id=? AND name=?",
      (int(user_id), name.strip().lower()),
    ).fetchone()
    return dict(row) if row else None


def list_recent_channels_for_user(user_id: int, limit: int = 10) -> List[Dict[str, object]]:
  with connect() as conn:
    rows = conn.execute(
      "SELECT username, scope, added_at FROM user_channels "
      "WHERE user_id=? ORDER BY added_at DESC LIMIT ?",
      (int(user_id), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def list_top_channels_for_user(user_id: int, hours: int = 24, limit: int = 8) -> List[Dict[str, object]]:
  since_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=int(hours))
  since = since_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
  with connect() as conn:
    rows = conn.execute(
      "SELECT p.channel_username AS username, COUNT(*) AS posts "
      "FROM posts p "
      "WHERE p.date_utc >= ? "
      "AND EXISTS (SELECT 1 FROM user_channels uc WHERE uc.user_id=? AND uc.username=p.channel_username) "
      "GROUP BY p.channel_username ORDER BY posts DESC, username ASC LIMIT ?",
      (since, int(user_id), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def incr_metric(key: str, delta: int = 1):
  with connect() as conn:
    conn.execute(
      "INSERT INTO app_metrics(key, value, updated_at) VALUES (?, ?, datetime('now')) "
      "ON CONFLICT(key) DO UPDATE SET value=app_metrics.value+excluded.value, updated_at=datetime('now')",
      (key, int(delta)),
    )


def get_metrics() -> Dict[str, int]:
  with connect() as conn:
    rows = conn.execute("SELECT key, value FROM app_metrics ORDER BY key").fetchall()
    return {str(r["key"]): int(r["value"]) for r in rows}


def set_monitoring_enabled(user_id: int, enabled: bool):
  with connect() as conn:
    conn.execute("UPDATE users SET monitor_enabled=? WHERE user_id=?", (1 if enabled else 0, int(user_id)))


def set_monitoring_params(
  user_id: int,
  interval_min: int | None = None,
  antiflood_min: int | None = None,
  pause_until_utc: str | None = None,
  include_keywords: str | None = None,
  exclude_keywords: str | None = None,
  categories: str | None = None,
):
  with connect() as conn:
    if interval_min is not None:
      conn.execute("UPDATE users SET monitor_interval_min=? WHERE user_id=?", (int(interval_min), int(user_id)))
    if antiflood_min is not None:
      conn.execute("UPDATE users SET monitor_antiflood_min=? WHERE user_id=?", (int(antiflood_min), int(user_id)))
    if pause_until_utc is not None:
      conn.execute("UPDATE users SET monitor_pause_until_utc=? WHERE user_id=?", (pause_until_utc, int(user_id)))
    if include_keywords is not None:
      conn.execute("UPDATE users SET monitor_include_keywords=? WHERE user_id=?", (include_keywords, int(user_id)))
    if exclude_keywords is not None:
      conn.execute("UPDATE users SET monitor_exclude_keywords=? WHERE user_id=?", (exclude_keywords, int(user_id)))
    if categories is not None:
      conn.execute("UPDATE users SET monitor_categories=? WHERE user_id=?", (categories, int(user_id)))


def mark_monitor_slot(user_id: int, slot_key: str):
  with connect() as conn:
    conn.execute("UPDATE users SET monitor_last_slot=? WHERE user_id=?", (slot_key, int(user_id)))


def list_users_monitoring_enabled() -> List[int]:
  with connect() as conn:
    rows = conn.execute("SELECT user_id FROM users WHERE monitor_enabled=1").fetchall()
    return [int(r["user_id"]) for r in rows]


def add_monitor_channel(user_id: int, username: str):
  username = username.lstrip("@").strip()
  with connect() as conn:
    conn.execute(
      "INSERT OR IGNORE INTO user_monitor_channels(user_id, username) VALUES (?,?)",
      (int(user_id), username),
    )


def remove_monitor_channel(user_id: int, username: str) -> int:
  username = username.lstrip("@").strip()
  with connect() as conn:
    cur = conn.execute(
      "DELETE FROM user_monitor_channels WHERE user_id=? AND username=?",
      (int(user_id), username),
    )
    return int(cur.rowcount)


def list_monitor_channels(user_id: int) -> List[str]:
  with connect() as conn:
    rows = conn.execute(
      "SELECT username FROM user_monitor_channels WHERE user_id=? ORDER BY username",
      (int(user_id),),
    ).fetchall()
    return [str(r["username"]) for r in rows]


def get_monitor_posts_between(user_id: int, start_utc: str, end_utc: str) -> List[Dict[str, object]]:
  with connect() as conn:
    rows = conn.execute(
      "SELECT p.* FROM posts p "
      "WHERE p.date_utc>=? AND p.date_utc<? "
      "AND EXISTS (SELECT 1 FROM user_monitor_channels mc WHERE mc.user_id=? AND mc.username=p.channel_username) "
      "ORDER BY p.date_utc DESC",
      (start_utc, end_utc, int(user_id)),
    ).fetchall()
    return [dict(r) for r in rows]
def ensure_schema():
  """Lightweight migrations for existing db.sqlite."""
  with connect() as conn:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "lang" not in cols:
      conn.execute("ALTER TABLE users ADD COLUMN lang TEXT NOT NULL DEFAULT 'en'")
