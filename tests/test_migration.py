from copy import copy
import sys
import unittest

from gooddataclient.columns import Attribute, Fact, Date, Label, Reference
from gooddataclient.connection import Connection
from gooddataclient.migration.actions import AddColumn, AddDate, DeleteColumn, DeleteRow
from gooddataclient.migration.chain import MigrationChain, DataMigrationChain
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.schema.maql import SYNCHRONIZE
from gooddataclient.text import to_identifier

from tests import logger, examples
from tests.credentials import username, password, gd_token
from tests.examples.country import Country
from tests.test_project import TEST_PROJECT_NAME


logger.set_log_level(debug=('-v' in sys.argv))


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

    def test_simple_delete_column(self):
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

    def test_migration_one_dataset(self):
        boss = Attribute(title='Boss', dataType='VARCHAR(50)', folder='Department')
        number_of_windows = Fact(title='Nb of Windows', dataType='INT')
        add1 = AddColumn(self.dataset.schema_name, 'boss', boss)
        add2 = AddColumn(self.dataset.schema_name, 'number_of_windows', number_of_windows)
        del1 = DeleteColumn(self.dataset.schema_name, 'city', self.dataset.city)

        class TestChain(MigrationChain):
            chain = [add1, del1, add2]

        test_chain = TestChain(self.project)
        test_chain.execute()

        self.assertTrue(self.dataset.has_attribute('boss'))
        self.assertTrue(self.dataset.has_fact('number_of_windows'))
        self.assertFalse(self.dataset.has_attribute('city'))

    def test_complex_add_column(self):
        # create a simple dataset to refrence
        country_dataset = Country(self.project)
        country_dataset.create()

        shortname = Label(title='Short Name', reference='department')
        created_at = Date(title='Created at', format='yyyy-MM-dd HH:mm:SS',
                          schemaReference='created_at', datetime=True)
        country = Reference(title='Country', reference='country', schemaReference='Country')

        add1 = AddColumn(self.dataset.schema_name, 'country', country)
        add2 = AddColumn(self.dataset.schema_name, 'shortname', shortname, label_references_cp=True)
        add3 = AddDate(self.dataset.schema_name, 'created_at', created_at)

        class ComplexMigration(MigrationChain):
            chain = [add1, add2, add3]

        complex_migration = ComplexMigration(self.project)
        complex_migration.execute()

        self.assertTrue(self.dataset.has_date('created_at'))
        self.assertTrue(self.dataset.has_label('shortname'))
        self.assertTrue(self.dataset.has_reference('country'))

    def test_complex_delete_column(self):
        worker, Worker = examples.examples[1]
        dataset = Worker(self.project)
        dataset.create()

        # reference drop
        del1 = DeleteColumn(dataset.schema_name, 'department', dataset.department)
        # label drop
        del2 = DeleteColumn(dataset.schema_name, 'lastname', dataset.lastname)

        class ComplexMigration(MigrationChain):
            chain = [del1, del2]

        complex_migration = ComplexMigration(self.project)
        complex_migration.execute()

        self.assertFalse(dataset.has_label('lastname'))
        self.assertFalse(dataset.has_reference('department'))

        salary, Salary = examples.examples[2]
        dataset = Salary(self.project)
        dataset.payday = Date(
            title='Pay Day', format='yyyy-MM-dd HH:mm:SS',
            schemaReference='payment', folder='Salary', datetime=True
        )
        dataset.create()

        # date drop
        del3 = DeleteColumn(dataset.schema_name, 'payday', dataset.payday)

        complex_migration.chain = [del3]
        complex_migration.execute()

        self.assertFalse(dataset.has_date('payday'))

    def test_attr_delete_label_cascade(self):
        work, Worker = examples.examples[1]
        dataset = Worker(self.project)
        dataset.hobby = Attribute(title='Hobby', folder='Worker', dataType='VARCHAR(40)')
        dataset.style = Label(title='Life style', reference='hobby', dataType='VARCHAR(30)')
        dataset.create()

        del1 =DeleteColumn(dataset.schema_name, 'hobby', dataset.hobby)

        class ComplexMigration(MigrationChain):
            chain = [del1, ]

        complex_migration = ComplexMigration(self.project)
        complex_migration.execute()

        self.assertFalse(dataset.has_attribute('hobby'))
        self.assertFalse(dataset.has_label('style'))

    def test_data_migration(self):
        self.dataset.upload()
        where = ' {label.department.department.name} IN ("HQ General Management", "HQ Information Systems")'
        del_row = DeleteRow(self.dataset, where)
        chain = DataMigrationChain(chain=[del_row], project=self.project)
        chain.execute()


if __name__ == '__main__':
    unittest.main()
