import sys
import unittest

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.connection import Connection
from gooddataclient.columns import Reference

from tests.credentials import password, username, gd_token
from tests.test_project import TEST_PROJECT_NAME
from tests import logger, examples


logger.set_log_level(debug=('-v' in sys.argv))


class TestState(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        delete_projects_by_name(self.connection, TEST_PROJECT_NAME)
        self.project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)

    def tearDown(self):
        self.project.delete()

    def test_has_properties(self):
        department = examples.examples[0][1](self.project)
        department.create()
        worker = examples.examples[1][1](self.project)
        worker.create()
        salary = examples.examples[2][1](self.project)
        salary.create()

        # attributes
        self.assertTrue(department.has_attribute('department'))
        self.assertTrue(department.has_attribute('city'))
        self.assertFalse(department.has_attribute('town'))
        # facts
        self.assertTrue(salary.has_fact('payment'))
        self.assertFalse(worker.has_fact('name'))
        self.assertFalse(worker.has_fact('age'))
        # labels
        self.assertTrue(department.has_label('name'))
        self.assertFalse(department.has_label('city'))
        self.assertFalse(department.has_label('building'))
        # references
        self.assertTrue(worker.has_reference('department'))
        self.assertFalse(worker.has_reference('jungle'))
        self.assertTrue(salary.has_reference('worker'))
        # dates
        self.assertTrue(salary.has_date('payday'))
        self.assertFalse(salary.has_date('expires_at'))
        self.assertFalse(department.has_date('birthday'))

    def test_remote_columns(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.create()

        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            columns = dataset.get_remote_columns()

            self.assertListEqual(sorted(columns), sorted(dict(dataset._columns).keys()))
            for col_name, col in columns.iteritems():
                dataset_col = getattr(dataset, col_name)
                self.assertIs(type(col), type(dataset_col))
                # references have no title on the API...
                if not isinstance(col, Reference):
                    self.assertEqual(col.title, dataset_col.title)
                self.assertEqual(col.dataType, dataset_col.dataType)
                self.assertEqual(col.reference, dataset_col.reference)
                self.assertEqual(col.schemaReference, dataset_col.schemaReference)
                self.assertEqual(col.datetime, dataset_col.datetime)


if __name__ == '__main__':
    unittest.main()
