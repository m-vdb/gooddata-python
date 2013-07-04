import sys
import unittest
from datetime import datetime

from gooddataclient.formatter import format_dates

from tests import logger


logger.set_log_level(debug=('-v' in sys.argv))


class TestText(unittest.TestCase):

    def test_date(self):
        dates = ['date']
        line = {'date': datetime(2054, 12, 6, 1, 6, 19)}
        self.assertEquals('2049-12-06', format_dates(line, dates, [])['date'])



if __name__ == '__main__':
    unittest.main()
