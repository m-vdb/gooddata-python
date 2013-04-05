import sys
import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.exceptions import DataSetNotFoundError, MaqlValidationFailed, \
                                      ProjectNotOpenedError, ProjectNotFoundError

from tests.credentials import password, username, project_id, gd_token
from tests import examples, logger, get_parser


TEST_PROJECT_NAME = 'gdc_unittest'


class TestProject(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        if not project_id and not gd_token:
            raise ValueError('You should define either project_id or gd_token in credentials.py')

    def test_create_and_delete_project(self):
        if gd_token:
            project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)
        else:
            project = Project(self.connection).load(id=project_id)
        self.assert_(project is not None)
        self.assert_(project.id is not None)

        if gd_token:
            project.delete()
            self.assertRaises(ProjectNotOpenedError, project.delete)
            self.assertRaises(ProjectNotFoundError, project.load,
                              name=TEST_PROJECT_NAME)

    def test_validate_maql(self):
        if gd_token:
            project = Project(self.connection).create(TEST_PROJECT_NAME, gd_token)
        else:
            project = Project(self.connection).load(id=project_id)

        self.assertRaises(MaqlValidationFailed, project.execute_maql, 'CREATE DATASET {dat')
        self.assertRaises(AttributeError, project.execute_maql, '')

        if gd_token:
            project.delete()


if __name__ == '__main__':
    args = get_parser().parse_args()
    logger.logger.setLevel(args.loglevel)
    del sys.argv[1:]
    unittest.main()
