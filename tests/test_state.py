import sys
import unittest

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.dataset import Dataset
from gooddataclient.connection import Connection
from gooddataclient.columns import Reference, HyperLink, Attribute, Fact, ConnectionPoint
from tests.credentials import password, username, gd_token, test_project_name
from tests import logger, examples


logger.set_log_level(debug=('-v' in sys.argv))


class TestState(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        delete_projects_by_name(self.connection, test_project_name)
        self.project = Project(self.connection).create(test_project_name, gd_token)

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

    def test_remote_diff(self):
        Department = examples.examples[0][1]
        Department(self.project).create()
        Worker = examples.examples[1][1]
        Worker(self.project).create()
        Salary = examples.examples[2][1]
        Salary(self.project).create()

        old_city = Department.city
        old_name = Department.name
        Department.name = HyperLink(title='Name', reference='department', folder='Department', dataType='VARCHAR(128)')
        Department.city = None
        Department.town = Attribute(title='Town', folder='Department', dataType='VARCHAR(20)')
        remote_diff = Department(self.project).get_remote_diff()

        self.assertIn('town', remote_diff['added'])
        self.assertIn('name', remote_diff['altered'])
        self.assertIn('city', remote_diff['deleted'])
        self.assertEqual(remote_diff['added']['town'], Department.town)
        self.assertEqual(remote_diff['altered']['name']['new'], Department.name)
        self.assertEqual(remote_diff['altered']['name']['old'], old_name)
        self.assertEqual(remote_diff['deleted']['city'], old_city)

        old_dpt = Worker.department
        Worker.department = None
        remote_diff = Worker(self.project).get_remote_diff()

        self.assertIn('department', remote_diff['deleted'])
        self.assertFalse(remote_diff['added'])
        self.assertFalse(remote_diff['altered'])
        self.assertEqual(remote_diff['deleted']['department'], old_dpt)

        Salary.payment = Fact(title='Payment', folder='Salary', dataType='BIGINT')
        remote_diff = Salary(self.project).get_remote_diff()

        self.assertIn('payment', remote_diff['altered'])
        self.assertFalse(remote_diff['added'])
        self.assertFalse(remote_diff['deleted'])

    def test_remote_diff_factsof(self):
        class Snapshot(Dataset):
            wrong_snapshot_id = Attribute(title='snapshot_id', dataType='VARCHAR(20)')
        Snapshot(self.project).create()
        remote_diff = Snapshot(self.project).get_remote_diff()
        self.assertTrue('factsof' not in remote_diff['deleted'])

        Snapshot.real_snapshot_id = ConnectionPoint(title='snapshot_id')
        remote_diff = Snapshot(self.project).get_remote_diff()
        self.assertTrue('factsof' in remote_diff['deleted'])

if __name__ == '__main__':
    unittest.main()
