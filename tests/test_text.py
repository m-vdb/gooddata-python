import sys
import unittest

from gooddataclient import text

from tests import logger


logger.set_log_level(debug=('-v' in sys.argv))


class TestText(unittest.TestCase):

    def test_date(self):
        self.assertEquals('test', text.to_identifier('Test'))


if __name__ == '__main__':
    unittest.main()
