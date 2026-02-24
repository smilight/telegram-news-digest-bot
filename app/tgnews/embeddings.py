\
import os
from functools import lru_cache
from typing import List, Tuple

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

@lru_cache(maxsize=1)
def _load_model():
  from sentence_transformers import SentenceTransformer
  return SentenceTransformer(MODEL_NAME)

def embed(texts: List[str]) -> "list[list[float]]":
  model = _load_model()
  return model.encode(texts, normalize_embeddings=True).tolist()

def cosine(a: List[float], b: List[float]) -> float:
  # embeddings already normalized, so dot = cosine
  return sum(x*y for x,y in zip(a,b))
