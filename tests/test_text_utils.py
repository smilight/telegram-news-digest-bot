import unittest

from tgnews.text_utils import canonical_url, first_url, url_hash


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


if __name__ == '__main__':
  unittest.main()
