from copy import copy
import sys
import unittest

from gooddataclient.columns import Attribute, Fact
from gooddataclient.connection import Connection
from gooddataclient.migration.actions import AddColumn, DeleteColumn
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.schema.maql import SYNCHRONIZE
from gooddataclient.text import to_identifier

from tests import logger, get_parser, examples
from tests.credentials import username, password, gd_token
from tests.test_project import TEST_PROJECT_NAME


class TestMigration(unittest.TestCase):

    def setUp(self):
        connection = Connection(username, password)
        delete_projects_by_name(connection, TEST_PROJECT_NAME)
        self.project = Project(connection).create(TEST_PROJECT_NAME, gd_token)
        department, Department = examples.examples[0]
        self.dataset = Department(self.project)
        self.dataset.create()

    def tearDown(self):
        self.project.delete()

    def test_simple_add_column(self):
        boss = Attribute(title='Boss', dataType='VARCHAR(50)', folder='Department')
        number_of_windows = Fact(title='Nb of Windows', dataType='INT')
        add1 = AddColumn(self.dataset.schema_name, 'boss', boss)
        add2 = AddColumn(self.dataset.schema_name, 'number_of_windows', number_of_windows)

        maql1 = add1.get_maql() + SYNCHRONIZE % {'schema_name': to_identifier(self.dataset.schema_name)}
        self.project.execute_maql(maql1)

        maql2 = add2.get_maql() + SYNCHRONIZE % {'schema_name': to_identifier(self.dataset.schema_name)}
        self.project.execute_maql(maql2)

        self.assertTrue(self.dataset.has_attribute('boss'))
        self.assertTrue(self.dataset.has_fact('number_of_windows'))

        self.dataset.boss = boss
        self.dataset.number_of_windows = number_of_windows
        self.dataset.data = self.dataset.added_data
        self.dataset.upload()

    def test_delete_column(self):
        column = copy(self.dataset.city)
        del1 = DeleteColumn(self.dataset.schema_name, 'city', column)

        maql1 = del1.get_maql() + SYNCHRONIZE % {'schema_name': to_identifier(self.dataset.schema_name)}
        self.project.execute_maql(maql1)

        self.assertFalse(self.dataset.has_attribute('city'))

        self.dataset.city = None
        self.dataset.data = self.dataset.deleted_data
        self.dataset.upload()

        add2 = AddColumn(self.dataset.schema_name, 'city', column)
        maql2 = add2.get_maql() + SYNCHRONIZE % {'schema_name': to_identifier(self.dataset.schema_name)}
        self.project.execute_maql(maql2)

        self.assertTrue(self.dataset.has_attribute('city'))

    # def test_alter_column(self):
    #     pass

    # def test_migration_one_dataset(self):
    #     pass

    # def test_migration_several_dataset(self):
    #     pass


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
