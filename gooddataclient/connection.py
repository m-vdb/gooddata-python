import os
import logging

import simplejson as json
import requests
from requests.exceptions import HTTPError

from gooddataclient.exceptions import AuthenticationError
from gooddataclient.archiver import create_archive, DEFAULT_ARCHIVE_NAME, DLI_MANIFEST_FILENAME

logger = logging.getLogger("gooddataclient")

JSON_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}


class Connection(object):

    HOST = 'https://secure.gooddata.com'

    LOGIN_URI = '/gdc/account/login'
    TOKEN_URI = '/gdc/account/token'
    MD_URI = '/gdc/md/'

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.webdav = Webdav(username, password)
        self.login(username, password)

    def login(self, username, password):
        try:
            data = {
                'postUserLogin': {
                    'login': username,
                    'password': password,
                    'remember': 1,
                }
            }
            r1 = self.post(uri=self.LOGIN_URI, data=data, login=True)
            r1.raise_for_status()
            self.cookies = self.webdav.cookies = r1.cookies

            r2 = self.get(uri=self.TOKEN_URI)
            r2.raise_for_status()
        except HTTPError, err:
            raise AuthenticationError(str(err), err.response.content)

    def relogin(self):
        self.login(self.username, self.password)

    def get(self, uri):
        logger.debug('GET: %s' % uri)
        response = requests.get(self.HOST + uri, cookies=self.cookies,
                                headers=JSON_HEADERS, auth=(self.username, self.password))
        return response

    def post(self, uri, data, headers=JSON_HEADERS, login=False):
        logger.debug('POST: %s' % uri)
        kwargs = {
            'url': self.HOST + uri,
            'data': json.dumps(data),
            'headers': headers,
            'auth': (self.username, self.password)
        }
        if not login:
            kwargs['cookies'] = self.cookies

        return requests.post(**kwargs)

    def delete(self, uri):
        logger.debug('DELETE: %s' % uri)
        r = requests.delete(url=self.HOST + uri, auth=(self.username, self.password))
        r.raise_for_status()

    def get_metadata(self):
        return self.get(self.MD_URI)


class Webdav(Connection):

    HOST = 'https://secure-di.gooddata.com'
    UPLOADS_URI = '/uploads/%s/'

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def upload(self, data, sli_manifest):
        '''Create zip file with data in csv format and manifest file, then create
        directory in webdav and upload the zip file there.

        @param data: csv data to upload
        @param sli_manifest: dictionary with the columns definitions

        return the name of the temporary file, hence the name of the directory
        created in webdav uploads folder
        '''
        archive = create_archive(data, sli_manifest)
        dir_name = os.path.basename(archive)
        # create the folder on WebDav
        self.mkcol(uri=self.UPLOADS_URI % dir_name)
        # open the files to read them
        f_archive = open(archive, 'rb')

        # upload the files to WebDav
        archive_uri = ''.join((self.UPLOADS_URI % dir_name, DEFAULT_ARCHIVE_NAME))
        self.put(uri=archive_uri, data=f_archive.read(),
                 headers={'Content-Type': 'application/zip'})

        # close and remove the files
        f_archive.close()
        os.remove(archive)

        return dir_name

    def mkcol(self, uri):
        logger.debug('MKCOL: %s' % uri)
        r = requests.request(method='MKCOL', url=self.HOST + uri, auth=(self.username, self.password))
        r.raise_for_status()

    def put(self, uri, data, headers):
        logger.debug('PUT: %s' % uri)
        r = requests.put(url=self.HOST + uri, data=data, headers=headers, auth=(self.username, self.password))
        r.raise_for_status()

    def delete(self, dir_name):
        uri = self.UPLOADS_URI % dir_name
        super(Webdav, self).delete(uri=uri)
