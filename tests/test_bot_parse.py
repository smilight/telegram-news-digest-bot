import unittest

try:
  from tgnews.bot import parse_channel_ref
except Exception:
  parse_channel_ref = None


class TestChannelParse(unittest.TestCase):
  def setUp(self):
    if parse_channel_ref is None:
      self.skipTest("aiogram/bot module is unavailable in this environment")

  def test_username_formats(self):
    self.assertEqual(parse_channel_ref('@ukrpravda'), 'ukrpravda')
    self.assertEqual(parse_channel_ref('ukrpravda'), 'ukrpravda')

  def test_tme_formats(self):
    self.assertEqual(parse_channel_ref('https://t.me/ukrpravda'), 'ukrpravda')
    self.assertEqual(parse_channel_ref('http://t.me/s/ukrpravda?test=1'), 'ukrpravda')
    self.assertEqual(parse_channel_ref('https://telegram.me/ukrpravda'), 'ukrpravda')

  def test_invalid(self):
    self.assertIsNone(parse_channel_ref('https://example.com/x'))
    self.assertIsNone(parse_channel_ref(''))


if __name__ == '__main__':
  unittest.main()
