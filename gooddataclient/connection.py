import os
import urllib2
import cookielib
import logging

import simplejson as json
import requests
from requests.exceptions import HTTPError

from gooddataclient.exceptions import AuthenticationError
from gooddataclient.archiver import create_archive, DEFAULT_ARCHIVE_NAME

logger = logging.getLogger("gooddataclient")

class RequestWithMethod(urllib2.Request):

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        if self._method:
            return self._method
        elif self.has_data():
            return 'POST'
        else:
            return 'GET'



class Connection(object):

    HOST = 'https://secure.gooddata.com'

    LOGIN_URI = '/gdc/account/login'
    TOKEN_URI = '/gdc/account/token'
    MD_URI = '/gdc/md/'

    JSON_HEADERS = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'gooddata-python/0.1'
    }

    def __init__(self, username, password, debug=0):
        self.webdav = Webdav(username, password)
        self.setup_urllib2(debug)
        self.login(username, password)

    def setup_urllib2(self, debug):
        handlers = [urllib2.HTTPCookieProcessor(cookielib.CookieJar()),
                    urllib2.HTTPHandler(debuglevel=debug),
                    urllib2.HTTPSHandler(debuglevel=debug),
                    ]
        handlers.append(self.webdav.get_handler())
        opener = urllib2.build_opener(*handlers)
        urllib2.install_opener(opener)

    def login(self, username, password):
        try:
            request_data = {'postUserLogin': {
                              'login': username,
                              'password': password,
                              'remember': 1,
                              }}
            response = self.request(self.LOGIN_URI, request_data)
            response['userLogin']['profile']
            self.request(self.TOKEN_URI)
        except urllib2.URLError:
            raise AuthenticationError('Please provide correct username and password.')
        except KeyError:
            raise AuthenticationError('No userLogin information in response to login.')

            data = {
                'postUserLogin': {
                    'login': username,
                    'password': password,
                    'remember': 1,
                }
            }
            r1 = self.post(uri=self.LOGIN_URI, data=json.dumps(data), login=True)
            r1.raise_for_status()
            self.cookies = self.webdav.cookies = r1.cookies

            r2 = self.get(uri=self.TOKEN_URI)
            r2.raise_for_status()
        except HTTPError, err:
            raise AuthenticationError(str(err))

    def get(self, uri):
        logger.debug('GET: %s' % uri)
        headers = {'content-type': 'application/json'}
        response = requests.get(self.HOST + uri, cookies=self.cookies,
                                headers=headers, auth=(self.username, self.password))
        if response.headers['content-type'] == 'application/json':
            return response.json()
        return response

    def post(self, uri, data, headers={'content-type': 'application/json'}, login=False):
        logger.debug('POST: %s' % uri)
        kwargs = {
            'url': self.HOST + uri,
            'data': data,
            'headers': headers
        }
        if not login:
            kwargs['cookies'] = self.cookies

        return requests.post(**kwargs)

    def get_metadata(self):
        return self.get(self.MD_URI)


class Webdav(Connection):

    HOST = 'https://secure-di.gooddata.com'
    UPLOADS_URI = '/uploads/%s/'

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_handler(self):
        passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
        passman.add_password(None, self.HOST, self.username, self.password)
        return urllib2.HTTPBasicAuthHandler(passman)

    def request(self, *args, **kwargs):
        try:
            return super(Webdav, self).request(*args, **kwargs)
        except urllib2.URLError, err:
            if hasattr(err, 'code') and err.code in (201, 204, 207):
                return
            raise

    def upload(self, data, sli_manifest):
        '''Create zip file with data in csv format and manifest file, then create
        directory in webdav and upload the zip file there. 
        
        @param data: csv data to upload
        @param sli_manifest: dictionary with the columns definitions
        @param wait_for_finish: check periodically for the integration result
        
        return the name of the temporary file, hence the name of the directory
        created in webdav uploads folder
        '''
        filename = create_archive(data, sli_manifest)
        dir_name = os.path.basename(filename)
        self.request(self.UPLOADS_URI % dir_name, method='MKCOL')
        f = open(filename, 'rb')
        # can it be streamed?
        self.request(''.join((self.UPLOADS_URI % dir_name, DEFAULT_ARCHIVE_NAME)),
                     data=f.read(), headers={'Content-Type': 'application/zip'},
                     method='PUT')
        f.close()
        os.remove(filename)
        return dir_name

    def delete(self, dir_name):
        self.request(self.UPLOADS_URI % dir_name, method='DELETE')

