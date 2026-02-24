\
from __future__ import annotations
import os
import datetime as dt
from dataclasses import dataclass
from .semantic import embed_texts, match_clusters, DEDUP_MODE
from .i18n import t

from typing import List, Dict, Tuple

from .simhash import hamming
from .text_utils import normalize_text

DEDUP_MODE = os.getenv("DEDUP_MODE", "simhash").strip().lower()  # simhash | embeddings

MEDIA_TAGS = {
  "[photo]": "📷 photo",
  "[video]": "🎬 video",
  "[voice]": "🎤 voice",
  "[audio]": "🎵 audio",
  "[sticker]": "🧩 sticker",
  "[document]": "📄 document",
  "[media]": "📎 media",
}


def _pretty_media_tags(text: str) -> str:
  out = text or ""
  for k, v in MEDIA_TAGS.items():
    out = out.replace(k, v)
  return out

def first_sentence(text: str, limit: int = 220) -> str:
  t = text.strip().replace("\n", " ")
  t = " ".join(t.split())
  if len(t) <= limit:
    return t
  cut = t[:limit]
  m = max(cut.rfind("."), cut.rfind("!"), cut.rfind("?"))
  if m >= 80:
    return cut[:m+1]
  return cut.rstrip() + "…"

@dataclass
class Cluster:
  rep: Dict
  items: List[Dict]

def _cluster_simhash(posts: List[Dict], max_hamming: int = 6) -> List[Cluster]:
  clusters: List[Cluster] = []
  posts_sorted = sorted(posts, key=lambda p: p["date_utc"], reverse=True)
  for p in posts_sorted:
    placed = False
    for c in clusters:
      if p["norm_hash"] == c.rep["norm_hash"]:
        c.items.append(p); placed = True; break
      if hamming(int(p["simhash"]), int(c.rep["simhash"])) <= max_hamming:
        c.items.append(p); placed = True; break
    if not placed:
      clusters.append(Cluster(rep=p, items=[p]))
  return clusters

def _cluster_embeddings(posts: List[Dict], threshold: float = 0.90) -> List[Cluster]:
  # Lazy import: optional dependency
  try:
    from .embeddings import embed, cosine
  except Exception:
    # fallback
    return _cluster_simhash(posts)

  texts = [normalize_text(p["text"]) for p in posts]
  vecs = embed(texts)

  clusters: List[Cluster] = []
  # newest first
  order = sorted(range(len(posts)), key=lambda i: posts[i]["date_utc"], reverse=True)
  for i in order:
    p = posts[i]
    v = vecs[i]
    placed = False
    for c in clusters:
      # exact hash first
      if p["norm_hash"] == c.rep["norm_hash"]:
        c.items.append(p); placed = True; break
      # cosine similarity to representative
      j = c.rep["_emb_i"]
      if cosine(v, vecs[j]) >= threshold:
        c.items.append(p); placed = True; break
    if not placed:
      rep = dict(p)
      rep["_emb_i"] = i
      clusters.append(Cluster(rep=rep, items=[p]))
  # remove internal marker
  for c in clusters:
    c.rep.pop("_emb_i", None)
  return clusters

def cluster_posts(posts: List[Dict]) -> List[Cluster]:
  if not posts:
    return []
  if DEDUP_MODE == "embeddings":
    return _cluster_embeddings(posts)
  return _cluster_simhash(posts)

def rank_clusters(clusters: List[Cluster]) -> List[Cluster]:
  def score(c: Cluster) -> Tuple[float, str]:
    # Importance = cross-source spread + volume + mild recency boost.
    sources = len({it.get("channel_username") for it in c.items if it.get("channel_username")})
    volume = len(c.items)
    recency_bonus = 0.0
    try:
      d = dt.datetime.fromisoformat(str(c.rep.get("date_utc", "")).replace("Z", "+00:00"))
      age_h = max(0.0, (dt.datetime.now(dt.timezone.utc) - d).total_seconds() / 3600.0)
      recency_bonus = max(0.0, 3.0 - age_h / 2.0)
    except Exception:
      pass
    importance = sources * 3.0 + volume * 1.0 + recency_bonus
    c.rep["_importance"] = importance
    return (importance, c.rep["date_utc"])
  return sorted(clusters, key=score, reverse=True)


def _cluster_summary(c: Cluster, lang: str) -> str:
  texts = []
  rep = _pretty_media_tags(normalize_text(c.rep.get("text", "")))
  if rep:
    texts.append(first_sentence(rep, limit=180))
  for it in c.items[:4]:
    txt = _pretty_media_tags(normalize_text(it.get("text", "")))
    if not txt:
      continue
    sent = first_sentence(txt, limit=120)
    if sent and sent not in texts:
      texts.append(sent)
    if len(texts) >= 2:
      break
  if not texts:
    return t(lang, "digest_no_text")
  if len(texts) == 1:
    return texts[0]
  return f"{texts[0]} {t(lang, 'digest_then')} {texts[1]}"

def format_digest(title: str, clusters: List[Cluster], top_k: int = 12, lang: str = "en") -> str:
  clusters = rank_clusters(clusters)[:top_k]
  lines = [f"🗞️ {title}", ""]
  if not clusters:
    lines.append(t(lang, "digest_empty"))
    return "\n".join(lines)

  for idx, c in enumerate(clusters, 1):
    summary = _cluster_summary(c, lang)
    links, seen = [], set()
    for it in c.items:
      if it["link"] in seen:
        continue
      seen.add(it["link"])
      links.append(it["link"])
      if len(links) >= 3:
        break
    src = " | ".join(links)
    extra = f" (+{len(c.items)-len(links)} {t(lang, 'digest_more')})" if len(c.items) > len(links) else ""
    sources_cnt = len({it['channel_username'] for it in c.items})
    lines.append(f"{idx}) {summary}")
    rep_time = str(c.rep.get("date_utc", ""))
    if rep_time:
      lines.append(f"🕒 {rep_time}")
    importance = float(c.rep.get("_importance", 0.0))
    lines.append(
      f"{t(lang,'cluster_sources')}: {sources_cnt} • "
      f"{t(lang,'cluster_posts')}: {len(c.items)} • "
      f"{t(lang,'cluster_importance')}: {importance:.1f}"
    )
    first_ch, first_dt, first_link = cluster_first_source(c)
    if first_ch and first_dt:
      lines.append(f"{t(lang,'cluster_first')}: @{first_ch} • {first_dt}")
    spread = cluster_spread(c, limit=6)
    if len(spread) > 1:
      lines.append(t(lang,'cluster_spread') + ": " + ", ".join(["@"+x for x in spread]))
    lines.append(f"🔗 {src}{extra}")
    if len(c.items) > 1:
      chans = sorted({it['channel_username'] for it in c.items})
      if len(chans) <= 6:
        lines.append(f"📣 {t(lang, 'breaking_sources')}: {', '.join('@'+ch for ch in chans)}")
      else:
        lines.append(
          f"📣 {t(lang, 'breaking_sources')}: {', '.join('@'+ch for ch in chans[:6])} (+{len(chans)-6} {t(lang, 'digest_more')})"
        )
    lines.append("")
  return "\n".join(lines).strip()


def _centroid_text(cluster) -> str:
  # use summary if available else first post text
  summary = getattr(cluster, "summary", None)
  if summary:
    return summary
  if cluster.items:
    return cluster.items[0].get("text", "") or ""
  return ""

def cluster_centroids(clusters):
  texts = [_centroid_text(c) for c in clusters]
  embs = embed_texts(texts)
  if embs is None:
    return [(t, None) for t in texts]
  return list(zip(texts, embs))

def diff_clusters(current, previous, threshold: float = 0.82):
  """Return (new, continued_pairs, dropped). continued_pairs = list of (cur, prev, score)."""
  if not previous:
    return current, [], []
  cur_cent = cluster_centroids(current)
  prev_cent = cluster_centroids(previous)
  if DEDUP_MODE == "embeddings":
    pairs = match_clusters(cur_cent, prev_cent, threshold=threshold)
  else:
    # fallback: match by norm_hash of representative item
    prev_keys = []
    for c in previous:
      k = c.items[0].get("norm_hash") if c.items else None
      prev_keys.append(k)
    pairs = []
    used_prev = set()
    for i, c in enumerate(current):
      k = c.items[0].get("norm_hash") if c.items else None
      if k and k in prev_keys:
        j = prev_keys.index(k)
        if j not in used_prev:
          used_prev.add(j)
          pairs.append((i, j, 1.0))
  matched_cur = {i for i,_,_ in pairs}
  matched_prev = {j for _,j,_ in pairs}
  new = [c for i,c in enumerate(current) if i not in matched_cur]
  dropped = [c for j,c in enumerate(previous) if j not in matched_prev]
  continued = [(current[i], previous[j], sc) for i,j,sc in pairs]
  return new, continued, dropped


def cluster_first_source(cluster):
  """Return (first_channel, first_date_utc, first_link)."""
  if not cluster.items:
    return None, None, None
  # items may be newest-first; find earliest
  earliest = None
  for it in cluster.items:
    dt = it.get("date_utc")
    if dt is None:
      continue
    if earliest is None or dt < earliest.get("date_utc",""):
      earliest = it
  if not earliest:
    earliest = cluster.items[-1]
  return earliest.get("channel_username"), earliest.get("date_utc"), earliest.get("link")

def cluster_spread(cluster, limit=6):
  """Return list of channels in chronological order (approx)"""
  if not cluster.items:
    return []
  # sort by date_utc asc
  items = sorted(cluster.items, key=lambda x: x.get("date_utc",""))
  chans = []
  for it in items:
    ch = it.get("channel_username")
    if ch and ch not in chans:
      chans.append(ch)
    if len(chans) >= limit:
      break
  return chans
