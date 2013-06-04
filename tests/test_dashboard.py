import os
import sys
import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project
from gooddataclient.dashboard import Dashboard


from tests.credentials import (
    password, username, user_id, test_dashboards_id,
    test_dashboard_id, test_dashboard_name,
    test_dashboard_project_id
)
from tests import logger

logger.set_log_level(debug=('-v' in sys.argv))


class TestDashboard(unittest.TestCase):
    date_filters = {"from": -3, "to": -1}
    page_name = 'fake_page'

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection)
        self.project.load(test_dashboard_project_id)
        self.dashboard = Dashboard(
            self.project, user_id, test_dashboards_id,
            test_dashboard_id, test_dashboard_name
        )

    def test_download(self):
        self.dashboard.download(self.date_filters, company=None, page=self.page_name, output_dir='./')
        try:
            os.remove('./'+self.page_name+'.pptx')
        except:
            self.fail('ppt should be found')

    def test_get_execution_context(self):
        expected_answer = '/gdc/projects/%(project_id)s/users/%(user_id)s/executioncontexts/' % {
            'project_id': test_dashboard_project_id,
            'user_id': user_id
        }
        self.dashboard._get_execution_context(self.date_filters)
        self.assertIn(expected_answer, self.dashboard.execution_context_response_uri)

    def test_get_client_export(self):
        self.dashboard._get_execution_context(self.date_filters)

        expected_answer = '/gdc/projects/%(project_id)s/clientexport/' % {
            'project_id': test_dashboard_project_id
        }
        self.dashboard._get_client_export(self.page_name, self.dashboard.PAGE_IDENTIFIER)
        self.assertIn(expected_answer, self.dashboard.client_export_response_uri)

    def test_poll_for_pdf(self):
        self.dashboard._get_execution_context(self.date_filters)
        self.dashboard._get_client_export(self.page_name, self.dashboard.PAGE_IDENTIFIER)

        self.dashboard._poll_for_pdf()
        self.assertIsNotNone(self.dashboard.pdf_data)

    def test_save_as_pdf(self):
        self.dashboard._get_execution_context(self.date_filters)
        self.dashboard._get_client_export(self.page_name, self.dashboard.PAGE_IDENTIFIER)
        self.dashboard._poll_for_pdf()

        self.dashboard._save_as_pdf('./', self.page_name)
        try:
            os.remove('./'+self.page_name+'.pdf')
        except:
            self.fail('pdf should be found')

    def test_pdf_to_ppt(self):
        self.dashboard._get_execution_context(self.date_filters)
        self.dashboard._get_client_export(self.page_name, self.dashboard.PAGE_IDENTIFIER)
        self.dashboard._poll_for_pdf()
        self.dashboard._save_as_pdf('./', self.page_name)

        self.dashboard._pdf_to_ppt('./', self.page_name, delete_pdf=True)
        try:
            os.remove('./'+self.page_name+'.pptx')
        except:
            self.fail('ppt should be found')

        self.assertRaises(
            IOError, self.dashboard._pdf_to_ppt,
            output_dir='./', pdf_name='wrong_name', delete_pdf=False
        )

if __name__ == '__main__':
    unittest.main()
