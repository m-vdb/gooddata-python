import sys
import unittest
import os

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.connection import Connection
from gooddataclient.dataset import DateDimension, Dataset
from gooddataclient.exceptions import MaqlValidationFailed, DataSetNotFoundError

from tests.credentials import password, username, project_id, gd_token
from tests.test_project import TEST_PROJECT_NAME
from tests import logger, examples, get_parser


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)

    def tearDown(self):
        self.project.delete()

    def test_create_dataset(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.create()
            # TODO: verify the creation

    def test_upload_dataset(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.upload()
            dataset_metadata = dataset.get_metadata(name=dataset.schema_name)
            self.assertTrue(dataset_metadata['dataUploads'])
            self.assertEquals('OK', dataset_metadata['lastUpload']['dataUploadShort']['status'])
            # TODO: check different data for the upload

    def test_date_dimension(self):
        date_dimension = DateDimension(self.project)
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE"', date_dimension.get_maql())
        self.assertEquals('INCLUDE TEMPLATE "URN:GOODDATA:DATE" MODIFY (IDENTIFIER "test", TITLE "Test");\n\n',
                          date_dimension.get_maql('Test'))
        self.assertEquals(examples.forex.date_dimension_maql, date_dimension.get_maql('Forex', include_time=True))
        self.assertEquals(examples.forex.date_dimension_maql.replace('forex', 'xerof').replace('Forex', 'Xerof'),
                          date_dimension.get_maql('Xerof', include_time=True))

    def test_sli_manifest(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            sli_manifest = dataset.get_sli_manifest()
            self.assertIsInstance(sli_manifest, dict)

    def test_no_upload(self):
        csv_file = os.path.join(os.path.abspath('./'), 'tmp.csv')
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.upload(
                no_upload=True, keep_csv=True,
                csv_file=csv_file
            )
            os.remove(csv_file)

    def test_exceptions(self):

        class DummyDataset(Dataset):
            pass

        dataset = DummyDataset(self.project)
        self.assertRaises(DataSetNotFoundError, dataset.get_metadata, 'dummy_dataset')
        self.assertRaises(NotImplementedError, dataset.data)

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


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
