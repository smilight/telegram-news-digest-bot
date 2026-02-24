\
import re
from collections import Counter

WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁёІіЇїЄє0-9_]+")

MASK64 = (1 << 64) - 1

def tokenize(text: str) -> list[str]:
  return WORD_RE.findall(text.lower())

def fnv1a64(s: str) -> int:
  h = 1469598103934665603
  for b in s.encode("utf-8", errors="ignore"):
    h ^= b
    h = (h * 1099511628211) & MASK64
  return h

def simhash64(text: str) -> int:
  tokens = tokenize(text)
  if not tokens:
    return 0
  weights = Counter(tokens)
  v = [0] * 64
  for tok, w in weights.items():
    h = fnv1a64(tok)
    for i in range(64):
      bit = (h >> i) & 1
      v[i] += w if bit else -w
  out = 0
  for i in range(64):
    if v[i] > 0:
      out |= (1 << i)
  return out & MASK64

def to_sqlite_int(u: int) -> int:
  u = u & MASK64
  return u - (1 << 64) if u >= (1 << 63) else u

def to_uint64(x: int) -> int:
  return x & MASK64

def hamming(a: int, b: int) -> int:
  return (to_uint64(a) ^ to_uint64(b)).bit_count()
