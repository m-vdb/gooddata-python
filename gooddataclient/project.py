import time
import urllib2
import logging

from requests.exceptions import HTTPError

from gooddataclient.exceptions import ProjectNotOpenedError, UploadFailed,\
                                      ProjectNotFoundError, MaqlExecutionFailed, \
                                      get_api_msg, GetSLIManifestFailed

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
    SLI_URI = '/gdc/md/%s/ldm/singleloadinterface/{dataset.%s}/manifest'
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
        err_json = {
            'links': data['about']['links'],
            'project_name': name,
        }
        raise ProjectNotFoundError('Failed to retrieve Project identifier for %s' % (name), err_json)

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
        except HTTPError, err:
            err_json = {
                'project_id': self.id,
                'uri': uri,
                'status_code': err.response.status_code,
                'response': err.response.content
            }
            raise ProjectNotOpenedError('Project does not seem to be opened: %s' % self.id, err_json)
        except TypeError:
            err_json = {
                'project_id': self.id,
                'uri': uri
            }
            raise ProjectNotOpenedError('Project does not seem to be opened: %s' % self.id, err_json)

    def execute_maql(self, maql):
        if not maql:
            raise AttributeError('MAQL missing, nothing to execute')
        data = {'manage': {'maql': maql}}
        try:
            response = self.connection.post(uri=self.MAQL_EXEC_URI % self.id, data=data)
            response.raise_for_status()

            if len(response.json()['uris']) == 0:
                raise MaqlExecutionFailed('Length of `uris` array should not be 0', response.json())
        except HTTPError, err:
            err_json = err.response.json()['error']
            err_json.update({
                'status_code': err.response.status_code,
                'maql': maql,
            })

            # FIXME : this should be removed when using ldm/manage2
            if err.response.status_code != 504:
                raise MaqlExecutionFailed(get_api_msg(err_json),err_json)

    def integrate_uploaded_data(self, dir_name, wait_for_finish=True):
        try:
            response = self.connection.post(self.PULL_URI % self.id,
                                            {'pullIntegration': dir_name})
            response.raise_for_status()
        except HTTPError, err:
            status_code = err.response.status_code
            if status_code == 401:
                self.connection.relogin()
                response = self.connection.post(self.PULL_URI % self.id,
                                                {'pullIntegration': dir_name})
            else:
                err_json = err.response.json()['error']
                err_json['status_code'] = status_code
                err_json['dir_name'] = dir_name
                raise UploadFailed(get_api_msg(err_json), err_json)
        task_uri = response.json()['pullTask']['uri']
        # checkLoadingStatus in AbstractConnector.java
        if wait_for_finish:
            while True:
                status = self.connection.get(uri=task_uri).json()['taskStatus']
                logger.debug(status)
                if status == 'OK':
                    break
                if status in ('ERROR', 'WARNING'):
                    raise UploadFailed('Failed with status: %s' % status, {'dir_name': dir_name})
                time.sleep(0.5)

    def get_sli_manifest(self, dataset_name):
        """
        Get the SLI manifest from API entry point.
        """

        try:
            uri = self.SLI_URI % (self.id, dataset_name)
            response = self.connection.get(uri)
            response.raise_for_status()
        except HTTPError, err:
            err_json = {
                'error': err.response.json()['error'],
                'status_code': err.response.status_code,
            }
            raise GetSLIManifestFailed('Getting SLI manifest failed : %s' % err.response.status_code, err_json)
        else:
            return response.json()
