from __future__ import annotations
import re
import hashlib
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

PROMO_PATTERNS = [
  r"подпис(ывай|уй)тес[ья].*$",
  r"підпис(уй|уйте)тесь.*$",
  r"наш (?:телеграм|tg).*$",
  r"our (?:telegram|tg).*$",
  r"реклама.*$",
  r"advertisement.*$",
]
URL_RE = re.compile(r"https?://\S+")

def strip_utm(url: str) -> str:
  try:
    u = urlparse(url)
    q = [(k,v) for (k,v) in parse_qsl(u.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    newq = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, newq, u.fragment))
  except Exception:
    return url


def first_url(text: str) -> str | None:
  if not text:
    return None
  m = URL_RE.search(text)
  return m.group(0) if m else None


def canonical_url(url: str) -> str:
  c = strip_utm(url)
  try:
    u = urlparse(c)
    netloc = (u.netloc or "").lower()
    if netloc.startswith("www."):
      netloc = netloc[4:]
    path = u.path.rstrip("/") or "/"
    return urlunparse((u.scheme.lower(), netloc, path, "", u.query, ""))
  except Exception:
    return c


def url_hash(url: str | None) -> str | None:
  if not url:
    return None
  return hashlib.sha256(url.encode("utf-8", errors="ignore")).hexdigest()

def normalize_text(text: str) -> str:
  t = text.strip()
  def _repl(m):
    return strip_utm(m.group(0))
  t = URL_RE.sub(_repl, t)
  lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
  if not lines:
    return ""
  joined = "\n".join(lines)
  joined = re.sub(r"[ \t]+", " ", joined)
  joined = re.sub(r"\n{3,}", "\n\n", joined)
  for pat in PROMO_PATTERNS:
    joined = re.sub(pat, "", joined, flags=re.IGNORECASE | re.MULTILINE).strip()
  return joined

def norm_hash(text: str) -> str:
  return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
