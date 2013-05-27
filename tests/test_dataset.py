import sys
import unittest
import os

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.connection import Connection
from gooddataclient.columns import Date
from gooddataclient.dataset import DateDimension, Dataset
from gooddataclient.exceptions import MaqlValidationFailed, DataSetNotFoundError

from tests.credentials import password, username, gd_token, test_project_name
from tests import logger, examples


logger.set_log_level(debug=('-v' in sys.argv))


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        delete_projects_by_name(self.connection, test_project_name)
        self.project = Project(self.connection).create(test_project_name, gd_token)

    def tearDown(self):
        self.project.delete()

    def test_create_dataset(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.create()
            dataset.get_metadata(dataset.schema_name)

    def test_upload_dataset(self):
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(self.project)
            dataset.upload()
            dataset_metadata = dataset.get_metadata(name=dataset.schema_name)
            self.assertTrue(dataset_metadata['dataUploads'])
            self.assertEquals('OK', dataset_metadata['lastUpload']['dataUploadShort']['status'])

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
            self.assertEquals('INCREMENTAL', sli_manifest['dataSetSLIManifest']['parts'][0]['mode'])
            sli_manifest = dataset.get_sli_manifest(full_upload=True)
            self.assertEquals('FULL', sli_manifest['dataSetSLIManifest']['parts'][0]['mode'])

    def test_dates_sli_manifest(self):
        _datetime = Date(title='Created at', schemaReference='created_at', datetime=True)
        _datetime.set_name_and_schema('_name', '_schema')
        self.assertEquals(
            'yyyy-MM-dd HH:mm:SS',
            _datetime.get_sli_manifest_part(full_upload=False)[0]['constraints']['date']
        )
        _date = Date(title='Created at', schemaReference='created_at', datetime=False)
        _date.set_name_and_schema('_name', '_schema')
        self.assertEquals(
            'yyyy-MM-dd',
            _date.get_sli_manifest_part(full_upload=False)[0]['constraints']['date']
        )

    def test_no_upload(self):
        '''
        test that no connection to GD API is made.
        Uses a mock connection that will raise an error if put is called
        '''
        csv_file = os.path.join(os.path.abspath('./'), 'tmp.csv')

        def mock_put(uri, data, headers):
            raise Exception('GD API should not be called')

        self.project.connection.webdav.put = mock_put
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


if __name__ == '__main__':
    unittest.main()
