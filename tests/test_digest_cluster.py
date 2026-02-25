import unittest
from typing import Optional

from tgnews.digest import cluster_posts


class TestDigestCluster(unittest.TestCase):
  def _post(self, idx: int, text: str, norm_hash: str, simhash: int, url_hash: Optional[str] = None):
    return {
      "id": idx,
      "channel_username": f"c{idx}",
      "msg_id": idx,
      "date_utc": f"2026-01-01T10:{idx:02d}:00Z",
      "text": text,
      "link": f"https://t.me/c{idx}/{idx}",
      "norm_hash": norm_hash,
      "simhash": simhash,
      "url_hash": url_hash,
    }

  def test_clusters_by_similar_text(self):
    p1 = self._post(
      1,
      "Air alert in region A due to drone activity near the city and surrounding area. Stay in shelters.",
      "h1",
      11,
      url_hash="u1",
    )
    p2 = self._post(
      2,
      "Air alert in region A because of drone activity near city and nearby area. Stay in shelters now.",
      "h2",
      99,
      url_hash="u2",
    )
    clusters = cluster_posts([p1, p2])
    self.assertEqual(len(clusters), 1)
    self.assertEqual(len(clusters[0].items), 2)

  def test_media_only_posts_not_fuzzy_merged(self):
    p1 = self._post(1, "📷 photo", "m1", 123, url_hash=None)
    p2 = self._post(2, "📷 photo", "m2", 124, url_hash=None)
    clusters = cluster_posts([p1, p2])
    self.assertEqual(len(clusters), 2)


if __name__ == "__main__":
  unittest.main()
