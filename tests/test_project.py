import sys
import unittest
import requests

from requests.exceptions import ConnectionError
from gooddataclient.connection import Connection
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.exceptions import (
    DataSetNotFoundError, MaqlValidationFailed, ProjectNotOpenedError,
    ProjectNotFoundError, DMLExecutionFailed, GoodDataTotallyDown
)

from tests.credentials import password, username, gd_token
from tests import examples, logger, get_parser


TEST_PROJECT_NAME = 'gdc_unittest'


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
            self.fail('project.execute_dml: unexpected exception: %s' % e)

    def test_gooddata_totally_down_exception(self):
        self.connection.HOST = 'http://toto'
        self.assertRaises(GoodDataTotallyDown, Project(self.connection).create, TEST_PROJECT_NAME, gd_token)

        # SSLError check
        self.connection.HOST = 'https://kennethreitz.com'
        self.assertRaises(GoodDataTotallyDown, Project(self.connection).create, TEST_PROJECT_NAME, gd_token)
        try:
            Project(self.connection).create(TEST_PROJECT_NAME, gd_token)
        except GoodDataTotallyDown, err:
            try:
                err.__str__()
            except TypeError, e:
                self.fail('GoodDataTotallyDown.__str__(): unexpected exception: %s' % e)


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
