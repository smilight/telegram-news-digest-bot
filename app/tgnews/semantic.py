import os
import math
from typing import List, Optional, Tuple

DEDUP_MODE = os.getenv("DEDUP_MODE", "simhash").lower()

def _cosine(a, b) -> float:
  # lists of floats
  dot = 0.0
  na = 0.0
  nb = 0.0
  for x, y in zip(a, b):
    dot += x * y
    na += x * x
    nb += y * y
  if na == 0.0 or nb == 0.0:
    return 0.0
  return dot / (math.sqrt(na) * math.sqrt(nb))

def _try_sentence_transformers():
  try:
    from sentence_transformers import SentenceTransformer  # type: ignore
    return SentenceTransformer
  except Exception:
    return None

_MODEL = None

def embed_texts(texts: List[str]) -> Optional[List[List[float]]]:
  """Return embeddings if available (and DEDUP_MODE=embeddings), else None."""
  if DEDUP_MODE != "embeddings":
    return None
  global _MODEL
  SentenceTransformer = _try_sentence_transformers()
  if SentenceTransformer is None:
    return None
  if _MODEL is None:
    # multilingual, good for uk/ru; downloads on first run (needs internet or cached model)
    model_name = os.getenv("EMBEDDING_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")
    _MODEL = SentenceTransformer(model_name)
  embs = _MODEL.encode(texts, show_progress_bar=False, normalize_embeddings=True)
  # convert numpy to list
  return [e.tolist() for e in embs]

def similarity(text_a: str, text_b: str, emb_a: Optional[List[float]] = None, emb_b: Optional[List[float]] = None) -> float:
  """Return similarity in [0..1]. In embeddings mode uses cosine (expects normalized)."""
  if DEDUP_MODE == "embeddings" and emb_a is not None and emb_b is not None:
    # embeddings from sentence-transformers already normalized if normalize_embeddings=True
    return max(0.0, min(1.0, _cosine(emb_a, emb_b)))
  # fallback: rough token overlap
  sa = set((text_a or "").lower().split())
  sb = set((text_b or "").lower().split())
  if not sa or not sb:
    return 0.0
  return len(sa & sb) / float(len(sa | sb))

def _hungarian_max(score):
  """Return assignment (rows->cols) maximizing total score. score is list[list[float]]."""
  n = len(score)
  m = len(score[0]) if n else 0
  N = max(n, m)
  # build cost matrix for minimization
  cost = [[0.0]*N for _ in range(N)]
  for i in range(N):
    for j in range(N):
      s = score[i][j] if i < n and j < m else 0.0
      cost[i][j] = 1.0 - s
  # Hungarian (potential-based) for rectangular via padding
  u = [0.0]*(N+1)
  v = [0.0]*(N+1)
  p = [0]*(N+1)
  way = [0]*(N+1)
  for i in range(1, N+1):
    p[0] = i
    j0 = 0
    minv = [float('inf')]*(N+1)
    used = [False]*(N+1)
    while True:
      used[j0] = True
      i0 = p[j0]
      delta = float('inf')
      j1 = 0
      for j in range(1, N+1):
        if not used[j]:
          cur = cost[i0-1][j-1] - u[i0] - v[j]
          if cur < minv[j]:
            minv[j] = cur
            way[j] = j0
          if minv[j] < delta:
            delta = minv[j]
            j1 = j
      for j in range(0, N+1):
        if used[j]:
          u[p[j]] += delta
          v[j] -= delta
        else:
          minv[j] -= delta
      j0 = j1
      if p[j0] == 0:
        break
    while True:
      j1 = way[j0]
      p[j0] = p[j1]
      j0 = j1
      if j0 == 0:
        break
  assignment = [-1]*N
  for j in range(1, N+1):
    if p[j] != 0:
      assignment[p[j]-1] = j-1
  return assignment[:n]


def match_clusters(centroids_a: List[Tuple[str, Optional[List[float]]]],
                   centroids_b: List[Tuple[str, Optional[List[float]]]],
                   threshold: float = 0.82) -> List[Tuple[int, int, float]]:
  """Match clusters A->B. Uses optimal assignment when possible."""
  if not centroids_a or not centroids_b:
    return []
  # Build similarity matrix
  score = []
  for ta, ea in centroids_a:
    row = []
    for tb, eb in centroids_b:
      row.append(similarity(ta, tb, ea, eb))
    score.append(row)
  # Optimal assignment (Hungarian) to reduce mismatches
  assign = _hungarian_max(score)
  matches: List[Tuple[int,int,float]] = []
  for i, j in enumerate(assign):
    if j is None or j < 0 or j >= len(centroids_b):
      continue
    sc = score[i][j]
    if sc >= threshold:
      matches.append((i, j, sc))
  return matches

