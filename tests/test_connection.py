import os
import unittest
from zipfile import ZipFile

from gooddataclient.connection import Connection
from gooddataclient.exceptions import AuthenticationError, ProjectNotOpenedError,\
    ProjectNotFoundError
from gooddataclient.archiver import write_tmp_file

from tests.credentials import username, password
from tests.test_project import TEST_PROJECT_NAME
from tests import logger, examples


class TestConnection(unittest.TestCase):

    def test_login(self):
        if username == '' or password == '':
            raise AttributeError, 'Please provide your username and password to your GoodData account (into creadentials.py)'
        self.assertRaises(AuthenticationError, Connection, '', '')
        connection = Connection(username, password)
        self.assertEquals('', connection.request('/gdc/account/token').read())
        self.assert_(connection.webdav.request('/uploads'))
        # you can delete here multiple directories from webdav
        for dir_name in ():
            connection.webdav.delete(dir_name)

    def test_create_and_delete_project(self):
        connection = Connection(username, password, debug=0)
        #drop all the test projects:
        connection.delete_projects_by_name(TEST_PROJECT_NAME)
        project = connection.create_project(TEST_PROJECT_NAME)
        self.assert_(project is not None)
        self.assert_(project.id is not None)
        project.delete()
        self.assertRaises(ProjectNotOpenedError, project.delete)
        self.assertRaises(ProjectNotFoundError, connection.get_project, 
                          name=TEST_PROJECT_NAME)
    
    def test_upload(self):
        example = examples.examples[0]
        connection = Connection(username, password, debug=0)
        dir_name = connection.webdav.upload(example.data_csv, example.sli_manifest)
        self.assert_(len(dir_name) > 0)
        self.assert_(connection.webdav.request('/uploads/%s' % dir_name))
        uploaded_file = connection.webdav.request('/uploads/%s/upload.zip' % dir_name)
        tmp_file = write_tmp_file(uploaded_file.read())
        zip_file = ZipFile(tmp_file, "r")
        self.assertEquals(None, zip_file.testzip())
        self.assertEquals(zip_file.namelist(), ['data.csv', 'upload_info.json'])
        zip_file.close()
        os.remove(tmp_file)
    

if __name__ == '__main__':
    unittest.main()
