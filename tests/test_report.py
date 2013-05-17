import sys
import unittest
import os

from gooddataclient.connection import Connection
from gooddataclient.project import Project
from gooddataclient.report import Report

from tests.credentials import password, username
from tests import logger


TEST_PROJECT_NAME = 'gdc_unittest'
TEST_REPORT_ID = '12003'
TEST_PROJECT_ID = 'co8r98zq367cwhw4ugeht7aw2honroep'
logger.set_log_level(debug=('-v' in sys.argv))


class TestReport(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection)
        self.project.load(TEST_PROJECT_ID)

    def test_exec_report(self):
        report = Report(self.project, TEST_REPORT_ID)
        report.execute_report()
        self.assertTrue(report.exec_result)

    def test_export_report(self):
        report = Report(self.project, TEST_REPORT_ID)
        report.export_report()
        self.assertTrue(report.export_download_uri)

    def test_get_report(self):
        report = Report(self.project, TEST_REPORT_ID)
        report.get_report()
        self.assertTrue(report.report_content)
        self.assertTrue("Page Id" in report.report_content)

    def test_save_report(self):
        report = Report(self.project, TEST_REPORT_ID)
        file_path = './test_report.txt'
        report.save_report(file_path)
        try:
            with open(file_path):
                pass
        except IOError:
                self.fail()
        os.remove(file_path)

if __name__ == '__main__':
    unittest.main()
