import os
import sys
import unittest

from gooddataclient.connection import Connection
from gooddataclient.exceptions import DashboardExportError
from gooddataclient.project import Project
from gooddataclient.dashboard import Dashboard


from tests.credentials import (
    password, username, user_id, test_dashboard_id,
    test_tab_id, test_dashboard_name,
    test_dashboard_project_id
)
from tests import logger

logger.set_log_level(debug=('-v' in sys.argv))


class TestDashboard(unittest.TestCase):
    common_filters = [{"object_id": 126, "constraint": {"type": "floating", "from": -3, "to": -1}}]
    wildcard_filter = {
        'attribute': 'label.page.page_name',
        'value': 'fake_page'
    }
    output_path = './fake_page.pdf'

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection)
        self.project.load(test_dashboard_project_id)
        self.dashboard = Dashboard(
            self.project, user_id, test_dashboard_id,
            test_tab_id, test_dashboard_name
        )

    def test_get_execution_context(self):
        expected_answer = '/gdc/projects/%(project_id)s/users/%(user_id)s/executioncontexts/' % {
            'project_id': test_dashboard_project_id,
            'user_id': user_id
        }
        self.dashboard._get_execution_context(self.common_filters)
        self.assertIn(expected_answer, self.dashboard.execution_context_response_uri)


        common_filters = [{"wrong_object_id": 126, "constraint": {"type": "floating", "from": -3, "to": -1}}]
        self.assertRaises(DashboardExportError, self.dashboard._get_execution_context, common_filters)

    def test_get_client_export(self):
        expected_answer = '/gdc/projects/%(project_id)s/clientexport/' % {
            'project_id': test_dashboard_project_id
        }
        self.dashboard._get_client_export(self.common_filters, self.wildcard_filter)
        self.assertIn(expected_answer, self.dashboard.client_export_response_uri)

    def test_poll_for_dashboard_data(self):
        self.dashboard._poll_for_dashboard_data(self.common_filters, self.wildcard_filter)
        self.assertIsNotNone(self.dashboard.pdf_data)

    def test_save_as_pdf(self):
        self.dashboard.save_as_pdf(self.common_filters, self.wildcard_filter, self.output_path)
        try:
            os.remove(self.output_path)
        except:
            self.fail('pdf should be found')

    def test_saved_dashboard_is_empty(self):
        self.dashboard.save_as_pdf(self.common_filters, self.wildcard_filter, self.output_path)
        self.dashboard.EMPTY_SIZE = 10  # fake pdf size
        self.assertFalse(self.dashboard.saved_dashboard_is_empty(self.output_path))
        self.dashboard.EMPTY_SIZE = 13109  # real pdf size
        self.assertTrue(self.dashboard.saved_dashboard_is_empty(self.output_path))
        os.remove(self.output_path)

if __name__ == '__main__':
    unittest.main()
