import sys
import unittest

from gooddataclient import text

from tests import logger


logger.set_log_level(debug=('-v' in sys.argv))


class TestText(unittest.TestCase):

    def test_date(self):
        self.assertEquals('test', text.to_identifier('Test'))

    def test_gd_repr(self):
        self.assertEquals('1', text.gd_repr(1))
        self.assertEquals('"a"', text.gd_repr('a'))
        self.assertEquals('"a\\"a"', text.gd_repr('a"a'))


if __name__ == '__main__':
    unittest.main()
