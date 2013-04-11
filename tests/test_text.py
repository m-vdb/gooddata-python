import sys
import unittest

from gooddataclient import text

from tests import logger, get_parser


class TestText(unittest.TestCase):

    def test_date(self):
        self.assertEquals('test', text.to_identifier('Test'))


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
