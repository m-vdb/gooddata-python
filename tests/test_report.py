import sys
import unittest

from gooddataclient.connection import Connection
from gooddataclient.project import Project
from gooddataclient.report import Report

from tests.credentials import password, username, project_id
from tests import logger


TEST_PROJECT_NAME = 'gdc_unittest'
TEST_REPORT_ID = '12003'
logger.set_log_level(debug=('-v' in sys.argv))


class TestReport(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection)
        self.project.load(project_id)

    def test_exec_report(self):
        report = Report(self.connection, self.project, TEST_REPORT_ID)
        report.execute_report()
        self.assertTrue(report.execResult)

    def test_export_report(self):
        report = Report(self.connection, self.project, TEST_REPORT_ID)
        report.export_report()
        self.assertTrue(report.export_download_URI)

    def test_get_report(self):
        report = Report(self.connection, self.project, TEST_REPORT_ID)
        report.get_report()
        self.assertTrue(report.report_content)
        self.assertTrue("Page Id" in report.report_content)



if __name__ == '__main__':
    unittest.main()
