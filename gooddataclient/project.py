import time
import urllib2
import logging

from requests.exceptions import HTTPError

from gooddataclient.exceptions import ProjectNotOpenedError, UploadFailed,\
    ProjectNotFoundError, MaqlExecutionFailed

logger = logging.getLogger("gooddataclient")


def delete_projects_by_name(connection, name):
    """Delete all GoodData projects by that name"""
    logger.debug('Dropping project by name %s' % name)
    try:
        while True:
            Project(connection).load(name=name).delete()
    except ProjectNotFoundError:
        pass


class Project(object):

    PROJECTS_URI = '/gdc/projects'
    MAQL_EXEC_URI = '/gdc/md/%s/ldm/manage'
    PULL_URI = '/gdc/md/%s/etl/pull'

    def __init__(self, connection):
        self.connection = connection

    def load(self, id=None, name=None):
        self.id = id or self.get_id_by_name(name)
        return self

    def get_id_by_name(self, name):
        """Retrieve the project identifier"""
        data = self.connection.get_metadata()
        for link in data['about']['links']:
            if link['title'] == name:
                logger.debug('Retrieved Project identifier for %s: %s' % (name, link['identifier']))
                return link['identifier']
        raise ProjectNotFoundError('Failed to retrieve Project identifier for %s' % (name))

    # TODO cannot test this....
    def create(self, name, desc=None, template_uri=None):
        """Create a new GoodData project"""
        request_data = {
            'project': {
                'meta': {
                    'title': name,
                    'summary': desc,
                },
                'content': {
                    'guidedNavigation': '1',
                },
            }
        }
        if template_uri:
            request_data['project']['meta']['projectTemplate'] = template_uri

        response = self.connection.post(self.PROJECTS_URI, request_data)
        id = response['uri'].split('/')[-1]
        logger.debug("Created project name=%s with id=%s" % (name, id))
        return self.load(id=id)

    # TODO cannot test this....
    def delete(self):
        """Delete a GoodData project"""
        try:
            uri = '/'.join((self.PROJECTS_URI, self.id))
            self.connection.delete(uri=uri)
        except (TypeError, urllib2.URLError):
            raise ProjectNotOpenedError()

    def execute_maql(self, maql):
        if not maql:
            raise AttributeError('MAQL missing, nothing to execute')
        data = {'manage': {'maql': maql}}
        try:
            response = self.connection.post(uri=self.MAQL_EXEC_URI % self.id, data=data)
            response.raise_for_status()

            if len(response.json()['uris']) == 0:
                raise MaqlExecutionFailed
        except HTTPError, err:
            err_json = err.response.json()['error']
            err_code = err.response.status_code
            self.log(error=err_json, error_code=err_code, maql=maql)

            if err_code != 504:
                raise MaqlExecutionFailed

    def integrate_uploaded_data(self, dir_name, wait_for_finish=True):
        try:
            response = self.connection.post(self.PULL_URI % self.id,
                                            {'pullIntegration': dir_name})
            response.raise_for_status()
        except HTTPError, err:
            err_code = err.response.status_code
            if err_code == 401:
                self.connection.relogin()
                response = self.connection.post(self.PULL_URI % self.id,
                                                {'pullIntegration': dir_name})
            else:
                err_json = err.response.json()['error']
                self.log(error=err_json, error_code=err_code)
                raise
        task_uri = response.json()['pullTask']['uri']
        # checkLoadingStatus in AbstractConnector.java
        if wait_for_finish:
            while True:
                status = self.connection.get(uri=task_uri).json()['taskStatus']
                logger.debug(status)
                if status == 'OK':
                    break
                if status in ('ERROR', 'WARNING'):
                    raise UploadFailed(status)
                time.sleep(0.5)

    def log(self, error, error_code=None, **kwargs):
        """
        A hook to log errors
        """
        logger.error(error['message'] % tuple(error['parameters']))
        logger.debug(''.join(kwargs.values()))
