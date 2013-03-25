import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.exceptions import DataSetNotFoundError, MaqlValidationFailed, \
                                      ProjectNotOpenedError, ProjectNotFoundError

from tests.credentials import password, username, project_id
from tests import examples, logger


TEST_PROJECT_NAME = 'gdc_unittest'


class TestProject(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        #drop all the test projects:
        if not project_id:
            delete_projects_by_name(self.connection, TEST_PROJECT_NAME)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            self.connection.delete_webdav_dir(dir_name)

    def test_create_and_delete_project(self):
        if not project_id:
            project = Project(self.connection).create(TEST_PROJECT_NAME)
        else:
            project = Project(self.connection).load(id=project_id)
        self.assert_(project is not None)
        self.assert_(project.id is not None)
        if not project_id:
            project.delete()
            self.assertRaises(ProjectNotOpenedError, project.delete)
            self.assertRaises(ProjectNotFoundError, project.load,
                              name=TEST_PROJECT_NAME)

    def test_create_structure(self):
        if not project_id:
            project = Project(self.connection).create(TEST_PROJECT_NAME)
        else:
            project = Project(self.connection).load(id=project_id)
        for (example, ExampleDataset) in examples.examples:
            dataset = ExampleDataset(project)
            self.assertRaises(DataSetNotFoundError, dataset.get_metadata,
                              name=dataset.schema_name)
            dataset.create()
            self.assert_(dataset.get_metadata(name=dataset.schema_name))

        if not project_id:
            project.delete()

    def test_validate_maql(self):
        if not project_id:
            project = Project(self.connection).create(TEST_PROJECT_NAME)
        else:
            project = Project(self.connection).load(id=project_id)

        self.assertRaises(MaqlValidationFailed, project.execute_maql, 'CREATE DATASET {dat')
        self.assertRaises(AttributeError, project.execute_maql, '')

        if not project_id:
            project.delete()

if __name__ == '__main__':
    unittest.main()
