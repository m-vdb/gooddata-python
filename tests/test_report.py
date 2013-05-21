import sys
import unittest
import os

from gooddataclient.connection import Connection
from gooddataclient.project import Project
from gooddataclient.report import Report

from tests.credentials import password, username, test_report_id, report_project_id
from tests import logger

logger.set_log_level(debug=('-v' in sys.argv))


class TestReport(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)
        self.project = Project(self.connection)
        self.project.load(report_project_id)

    def test_exec_report(self):
        report = Report(self.project, test_report_id)
        report.execute_report()
        self.assertTrue(report.exec_result)

    def test_export_report(self):
        report = Report(self.project, test_report_id)
        report.export_report()
        self.assertTrue(report.export_download_uri)

    def test_get_report(self):
        report = Report(self.project, test_report_id)
        report.get_report()
        self.assertTrue(report.report_content)
        self.assertFalse(report.report_content[0] == '{')

    def test_save_report(self):
        report = Report(self.project, test_report_id)
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
