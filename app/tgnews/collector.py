\
import os
import asyncio
import datetime as dt
from typing import List

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Message

from . import db
from .text_utils import normalize_text, norm_hash, first_url, canonical_url, url_hash
from .simhash import simhash64, to_sqlite_int

TELETHON_SESSION = os.getenv("TELETHON_SESSION", "/data/telethon.session")
TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")

POLL_SECONDS = int(os.getenv("COLLECTOR_POLL_SECONDS", "120"))

def msg_link(username: str, msg_id: int) -> str:
  return f"https://t.me/{username}/{msg_id}"

def to_iso_utc(d: dt.datetime) -> str:
  if d.tzinfo is None:
    d = d.replace(tzinfo=dt.timezone.utc)
  return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def media_placeholder(m: Message) -> str:
  if getattr(m, "photo", None) is not None:
    return "[photo]"
  if getattr(m, "video", None) is not None:
    return "[video]"
  if getattr(m, "voice", None) is not None:
    return "[voice]"
  if getattr(m, "audio", None) is not None:
    return "[audio]"
  if getattr(m, "sticker", None) is not None:
    return "[sticker]"
  if getattr(m, "document", None) is not None:
    return "[document]"
  if getattr(m, "media", None) is not None:
    return "[media]"
  return ""


def media_label(tag: str) -> str:
  return {
    "[photo]": "📷 photo",
    "[video]": "🎬 video",
    "[voice]": "🎤 voice",
    "[audio]": "🎵 audio",
    "[sticker]": "🧩 sticker",
    "[document]": "📄 document",
    "[media]": "📎 media",
  }.get(tag, tag)

class Collector:
  def __init__(self):
    self.client = TelegramClient(TELETHON_SESSION, TG_API_ID, TG_API_HASH)

  async def ensure_started(self):
    await self.client.connect()
    if not await self.client.is_user_authorized():
      raise RuntimeError(
        "Telethon session not authorized. Run: docker compose run --rm tgnews python -m tgnews.main --login"
      )

  async def ensure_login_interactive(self):
    await self.client.start()

  async def resolve_and_store_meta(self, username: str):
    try:
      ent = await self.client.get_entity(username)
      title = getattr(ent, "title", None)
      tg_id = getattr(ent, "id", None)
      db.upsert_channel_meta(username, tg_id, title)
    except Exception:
      db.upsert_channel_meta(username, None, None)

  async def fetch_new_for_channel(self, username: str):
    last_id = db.get_channel_last_msg_id(username)
    try:
      # Catch up all unseen messages since last_id (important for busy channels).
      msgs: List[Message] = []
      async for m in self.client.iter_messages(username, min_id=last_id, reverse=True):
        if not getattr(m, "id", None):
          continue
        if int(m.id) <= int(last_id):
          continue
        msgs.append(m)
        # Hard safety cap per channel tick to avoid endless backlog in one pass.
        if len(msgs) >= 1000:
          break
    except FloodWaitError as e:
      await asyncio.sleep(e.seconds + 1)
      return
    except Exception:
      return

    if not msgs:
      return

    msgs_sorted = sorted(msgs, key=lambda m: int(m.id))
    max_id = last_id
    for m in msgs_sorted:
      raw_text = (m.message or m.raw_text or "").strip()
      media_tag = media_placeholder(m)
      media_txt = media_label(media_tag) if media_tag else ""
      link = msg_link(username, m.id)

      first = first_url(raw_text) if raw_text else None
      canonical = canonical_url(first) if first else None
      canonical_h = url_hash(canonical)

      if raw_text:
        # Keep media signal in digest text, but dedup by original text.
        text = f"{raw_text}\n{media_txt}" if media_txt else raw_text
        norm_source = raw_text
      else:
        if not media_tag:
          continue
        text = media_txt
        # Avoid collapsing all media-only posts into one cluster.
        norm_source = f"{media_tag} {link}"

      if not text:
        continue
      # Link-aware dedup: include canonical URL for short/link-heavy posts.
      if canonical and len(norm_source) < 240:
        norm_source = f"{norm_source}\n{canonical}"

      norm = normalize_text(norm_source)
      if not norm and media_tag:
        norm = normalize_text(f"{media_tag} {link}")
      if not norm:
        continue
      h = norm_hash(norm)
      sh = to_sqlite_int(simhash64(norm))
      is_fwd = 1 if getattr(m, 'fwd_from', None) is not None else 0
      fwd_src = None
      try:
        if is_fwd and m.fwd_from and getattr(m.fwd_from, 'from_name', None):
          fwd_src = m.fwd_from.from_name
      except Exception:
        pass
      inserted = db.insert_post(
        channel_username=username,
        msg_id=m.id,
        date_utc=to_iso_utc(m.date),
        text=text,
        link=link,
        norm_hash=h,
        simhash=sh,
        is_forward=is_fwd,
        fwd_from=fwd_src,
        url_canonical=canonical,
        url_hash=canonical_h,
      )
      if inserted:
        db.incr_metric("posts_collected", 1)
        max_id = max(max_id, m.id)

    if max_id > last_id:
      db.set_channel_last_msg_id(username, max_id)

  async def loop(self, stop_event: asyncio.Event):
    await self.ensure_started()
    for ch in db.list_all_collected_channels():
      await self.resolve_and_store_meta(ch)

    while not stop_event.is_set():
      channels = db.list_all_collected_channels()
      for ch in channels:
        await self.fetch_new_for_channel(ch)
        # update basic stats (24h) occasionally
        try:
          await self._update_stats_24h(ch, 24)
        except Exception:
          pass
        await asyncio.sleep(0.6)
      try:
        await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
      except asyncio.TimeoutError:
        pass


  async def _update_stats_24h(self, username: str, window_hours: int = 24):
    # compute simple stats from stored posts (per tracked channel)
    from . import db as _db
    min_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=int(window_hours))).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _db.connect() as conn:
      rows = conn.execute("""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT norm_hash) AS uniq,
               SUM(CASE WHEN is_forward=1 THEN 1 ELSE 0 END) AS fwds
        FROM posts
        WHERE channel_username=? AND date_utc >= ?
      """, (username, min_utc)).fetchone()
      total = int(rows["total"] or 0)
      uniq = int(rows["uniq"] or 0)
      fwds = int(rows["fwds"] or 0)
    _db.upsert_channel_stats(username, window_hours, total, uniq, fwds)


  async def _maybe_breaking(self, window_min: int = 10, min_sources: int = 8):
    # Look for a rapidly spreading item in last window_min across tracked channels
    from . import db as _db
    min_utc = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=int(window_min))).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _db.connect() as conn:
      rows = conn.execute("""
        SELECT norm_hash, COUNT(*) AS c, COUNT(DISTINCT channel_username) AS sources,
               MAX(date_utc) AS last_seen
        FROM posts
        WHERE date_utc >= ?
        GROUP BY norm_hash
        HAVING sources >= ?
        ORDER BY sources DESC, last_seen DESC
        LIMIT 1
      """, (min_utc, int(min_sources))).fetchone()
      if not rows:
        return None
      norm_hash = rows["norm_hash"]
      # representative
      rep = conn.execute("""
        SELECT channel_username, text, link, date_utc FROM posts
        WHERE norm_hash=?
        ORDER BY date_utc DESC
        LIMIT 1
      """, (norm_hash,)).fetchone()
      # channels involved
      chans = conn.execute("""
        SELECT DISTINCT channel_username FROM posts
        WHERE norm_hash=? AND date_utc >= ?
        ORDER BY channel_username
        LIMIT 20
      """, (norm_hash, min_utc)).fetchall()
    return {"norm_hash": norm_hash, "rep": dict(rep) if rep else None, "channels": [c["channel_username"] for c in chans]}
