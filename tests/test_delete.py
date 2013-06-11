import sys
import unittest

from gooddataclient.project import Project, delete_projects_by_name
from gooddataclient.connection import Connection
from gooddataclient.exceptions import RowDeletionError

from tests.credentials import password, username, gd_token, test_project_name
from tests import logger
from tests.examples.department import Department



logger.set_log_level(debug=('-v' in sys.argv))


class TestDelete(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        delete_projects_by_name(self.connection, test_project_name)
        self.project = Project(self.connection).create(test_project_name, gd_token)

    def tearDown(self):
        self.project.delete()

    def test_column_maql_delete(self):
        dataset = Department(self.project)
        dataset.create()

        expected_maql_delete_department_row = 'DELETE FROM {attr.department.department}'\
            + ' WHERE {label.department.city} IN ("Boston", "NYC");'
        self.assertEquals(
            expected_maql_delete_department_row,
            dataset.get_maql_delete(where_clause='{label.department.city} IN ("Boston", "NYC")')
        )

        expected_maql_delete_department_row_with_ids = 'DELETE FROM {attr.department.department}'\
            + ' WHERE {label.department.department} IN ("d1", "d2");'
        self.assertEquals(
            expected_maql_delete_department_row_with_ids,
            dataset.get_maql_delete(where_values=["d1", "d2"])
        )
        self.assertEquals(
            expected_maql_delete_department_row_with_ids,
            dataset.get_maql_delete(column=dataset.department, where_values=["d1", "d2"])
        )

        expected_maql_delete_city_row = 'DELETE FROM {label.department.city}'\
            + ' WHERE {label.department.city} IN ("Boston", "NYC");'
        self.assertEquals(
            expected_maql_delete_city_row,
            dataset.get_maql_delete(
                column=dataset.city,
                where_clause='{label.department.city} IN ("Boston", "NYC")'
            )
        )
        self.assertEquals(
            expected_maql_delete_city_row,
            dataset.get_maql_delete(
                column=dataset.city,
                where_values=["Boston", "NYC"]
            )
        )
        self.assertRaises(RowDeletionError, dataset.get_maql_delete)


if __name__ == '__main__':
    unittest.main()
