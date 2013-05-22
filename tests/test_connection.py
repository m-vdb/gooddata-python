import sys
import os
import unittest
from zipfile import ZipFile

from requests.exceptions import HTTPError

from gooddataclient.connection import Connection
from gooddataclient.exceptions import (
    AuthenticationError, GoodDataTotallyDown, ReportExportFailed
)
from gooddataclient.archiver import write_tmp_file
from gooddataclient.report import Report
from gooddataclient.project import Project

from tests.credentials import username, password, test_report_id, report_project_id
from tests import examples, logger


logger.set_log_level(debug=('-v' in sys.argv))


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(username, password)

    def test_login(self):
        if username == '' or password == '':
            raise AttributeError('Please provide your username and password to your GoodData account (into creadentials.py)')
        self.assertRaises(AuthenticationError, Connection, '', '')
        self.assertTrue(self.connection.get('/gdc/account/token').ok)
        self.assertTrue(self.connection.webdav.get('/uploads').ok)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            self.connection.webdav.delete(dir_name)

    def test_upload(self):
        example = examples.examples[0][0]
        dir_name = self.connection.webdav.upload(example.data_csv, example.sli_manifest)
        self.assertTrue(dir_name)
        self.assertTrue(self.connection.webdav.get('/uploads/%s' % dir_name).ok)
        uploaded_file = self.connection.webdav.get('/uploads/%s/upload.zip' % dir_name).content
        tmp_file = write_tmp_file(uploaded_file)
        zip_file = ZipFile(tmp_file, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(tmp_file)
        self.connection.webdav.delete(dir_name)

    def test_gooddata_totally_down_exception(self):
        self.connection.HOST = 'http://toto'
        self.assertRaises(GoodDataTotallyDown, self.connection.get, '')
        self.assertRaises(GoodDataTotallyDown, self.connection.post, '', [])

        # SSLError check
        self.connection.HOST = 'https://kennethreitz.com'
        self.assertRaises(GoodDataTotallyDown, self.connection.get, '')
        self.assertRaises(GoodDataTotallyDown, self.connection.post, '', [])
        try:
            self.connection.get('')
        except GoodDataTotallyDown, err:
            try:
                str(err)
            except TypeError, e:
                self.fail('str(GoodDataTotallyDown): unexpected exception: %s' % e)

    def test_raise_appropriate_exception(self):
        self.project = Project(self.connection)
        self.project.load(report_project_id)

        report = Report(self.project, test_report_id)
        report.exec_result = 'fake'
        self.assertRaises(ReportExportFailed, report.export_report)

        self.assertRaises(HTTPError, self.connection.get, '/wronguri')


if __name__ == '__main__':
    unittest.main()
