import os
import unittest
from zipfile import ZipFile

from gooddataclient.connection import Connection
from gooddataclient.exceptions import AuthenticationError
from gooddataclient.archiver import write_tmp_file

from tests.credentials import username, password
from tests import examples, logger


class TestConnection(unittest.TestCase):

    def test_login(self):
        if username == '' or password == '':
            raise AttributeError('Please provide your username and password to your GoodData account (into creadentials.py)')
        self.assertRaises(AuthenticationError, Connection, '', '')
        connection = Connection(username, password)
        self.assertTrue(connection.get('/gdc/account/token').ok)
        self.assertTrue(connection.webdav.get('/uploads').ok)
        # you can delete here multiple directories from webdav
        for dir_name in ():
            connection.webdav.delete(dir_name)

    def test_upload(self):
        example = examples.examples[0][0]
        connection = Connection(username, password)
        dir_name = connection.webdav.upload(example.data_csv, example.sli_manifest)
        self.assertTrue(dir_name)
        self.assertTrue(connection.webdav.get('/uploads/%s' % dir_name).ok)
        uploaded_file = connection.webdav.get('/uploads/%s/upload.zip' % dir_name).content
        tmp_file = write_tmp_file(uploaded_file)
        zip_file = ZipFile(tmp_file, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(tmp_file)
        connection.webdav.delete(dir_name)

if __name__ == '__main__':
    unittest.main()
