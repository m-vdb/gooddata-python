import sys
import unittest


class TestMigration(unittest.TestCase):

    def test_add_column(self):
        pass

    def test_delete_column(self):
        pass

    def test_alter_column(self):
        pass

    def test_migration_one_dataset(self):
        pass

    def test_migration_several_dataset(self):
        pass


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
