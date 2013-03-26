import unittest

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.connection import Connection
from gooddataclient.dataset import DateDimension
from gooddataclient.exceptions import MaqlValidationFailed

from tests.credentials import password, username, project_id
from tests.test_project import TEST_PROJECT_NAME
from tests import logger, examples


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)

        if not project_id:
            delete_projects_by_name(self.connection, TEST_PROJECT_NAME)
            self.project = Project(self.connection).create(TEST_PROJECT_NAME)
        else:
            self.project = Project(self.connection).load(id=project_id)

    def tearDown(self):
        if not project_id:
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

        date_dimension.create(name='testDateDimension')
        self.assertRaises(MaqlValidationFailed, date_dimension.create, 'testDateDimension')

    def test_sli_manifest(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            sli_manifest = dataset.get_sli_manifest()
            self.assertIsInstance(sli_manifest, dict)


if __name__ == '__main__':
    unittest.main()
