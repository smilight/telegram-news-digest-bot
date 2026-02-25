import unittest

from tgnews.text_utils import (
  canonical_url,
  first_url,
  keyword_items_to_csv,
  mutate_keyword_csv,
  parse_keyword_items,
  url_hash,
)


class TestTextUtils(unittest.TestCase):
  def test_first_url(self):
    txt = 'hello https://example.com/a?utm_source=x&b=1 world'
    self.assertEqual(first_url(txt), 'https://example.com/a?utm_source=x&b=1')

  def test_canonical_url(self):
    raw = 'https://www.Example.com/a/?utm_source=x&b=1'
    self.assertEqual(canonical_url(raw), 'https://example.com/a?b=1')

  def test_url_hash(self):
    h = url_hash('https://example.com')
    self.assertTrue(isinstance(h, str) and len(h) == 64)

  def test_parse_keyword_items(self):
    self.assertEqual(parse_keyword_items(" Drone, missile ; Drone\nlaunch "), ["drone", "missile", "launch"])
    self.assertEqual(parse_keyword_items("shahed alert"), ["shahed alert"])

  def test_mutate_keyword_csv(self):
    cur = "drone, missile"
    self.assertEqual(mutate_keyword_csv(cur, "add", "launch, drone"), "drone, missile, launch")
    self.assertEqual(mutate_keyword_csv(cur, "rm", "missile"), "drone")
    self.assertEqual(mutate_keyword_csv(cur, "set", "shahed"), "shahed")
    self.assertEqual(mutate_keyword_csv(cur, "clear", ""), "")
    self.assertEqual(mutate_keyword_csv(cur, "show", ""), "drone, missile")

  def test_keyword_items_to_csv(self):
    self.assertEqual(keyword_items_to_csv([" Drone ", "drone", "missile "]), "drone, missile")


if __name__ == '__main__':
  unittest.main()
