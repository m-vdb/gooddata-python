import sys
import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.exceptions import (
    DataSetNotFoundError, MaqlValidationFailed, ProjectNotOpenedError,
    ProjectNotFoundError, DMLExecutionFailed
)

from tests.credentials import password, username, gd_token
from tests import examples, logger


TEST_PROJECT_NAME = 'gdc_unittest'
logger.set_log_level(debug=('-v' in sys.argv))


class TestProject(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        if not gd_token:
            raise ValueError('You should define either project_id or gd_token in credentials.py')
        delete_projects_by_name(self.connection, TEST_PROJECT_NAME)

    def test_create_and_delete_project(self):
        project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)
        self.assert_(project is not None)
        self.assert_(project.id is not None)

        project.delete()
        self.assertRaises(ProjectNotOpenedError, project.delete)
        self.assertRaises(ProjectNotFoundError, project.load,
                          name=TEST_PROJECT_NAME)

    def test_validate_maql(self):
        project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)

        self.assertRaises(MaqlValidationFailed, project.execute_maql, 'CREATE DATASET {dat')
        self.assertRaises(AttributeError, project.execute_maql, '')
        project.delete()

    def test_execute_dml(self):
        project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)

        self.assertRaises(DMLExecutionFailed, project.execute_dml, 'DELETE _%$;')
        self.assertRaises(DMLExecutionFailed, project.execute_dml, '')

        dataset = examples.examples[0][1](project)
        dataset.upload()

        try:
            project.execute_dml('DELETE FROM {attr.department.department};')
        except DMLExecutionFailed, e:
            self.fail('project.execute_dml: unexpected exception: %s' %e)


if __name__ == '__main__':
    unittest.main()
