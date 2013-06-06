import urllib2
import logging

from gooddataclient.exceptions import DashboardExportError

logger = logging.getLogger("gooddataclient")


class Dashboard(object):
    '''
    This class represents a GD dashboard.
    Use it to programatically export a GD dashboard
    as a pdf.
    '''

    EXECUTION_CONTEXT_URI = '/gdc/projects/%(project_id)s/users/%(user_id)s/executioncontexts'
    CLIENT_EXPORT_URI = '/gdc/projects/%(project_id)s/clientexport'

    err_msg = 'An error occured while exporting dashboard %(id)s'

    ID = None
    NAME = None

    def __init__(self, project, user_id, dashboards_id, id=None, name=None):
        self.project = project
        self.connection = project.connection
        self.id = id or self.ID
        self.name = name or self.NAME
        self.user_id = user_id
        self.dashboards_id = dashboards_id
        self.execution_context_response_uri = None
        self.client_export_response_uri = None
        self.pdf_data = None

    def save_as_pdf(self, common_filters, wildcard_filter, output_path):
        '''
        Saves the exported dashboard as pdf.
        First retrieves an execution_context,
        then retrieves a client_export,
        then polls the url to wait GD response,
        and lastly saves the response as a pdf.

        :param common_filters:      a list of filters that will be
                                    used in the execution context.
                                    This filter uses objects ids.
        ex: common_filters = [{"object_id": 126, "constraint": {"type": "floating", "from": -3, "to": -1}}]

        :param wildcard_filter:     a list of filters that will be
                                    used in the client export.
                                    This filter uses objects names.
        ex: wildcard_filter = {'attribute': 'label.page.page_name', 'value': 'fake_page'}
        '''
        logger.debug(
            'Exporting dashboard %(dashboard_name)s with filters %(common_filters)s'
            + ' and %(wildcard_filter)s' % {
                'dashboard_name': self.name,
                'common_filters': common_filters,
                'wildcard_filter': wildcard_filter
            }
        )

        self._poll_for_dashboard_data(common_filters, wildcard_filter)

        with open(output_path + '.pdf', 'wb') as handle:
            for block in self.pdf_data.iter_content(1024):
                if not block:
                    break
                handle.write(block)

    def _poll_for_dashboard_data(self, common_filters, wildcard_filter):
        '''
        Poll and retrieve the dashboard data, third step of the dashboard download
        '''
        self._get_client_export(common_filters, wildcard_filter)

        self.pdf_data = self.connection.poll_server_response(
            self.client_export_response_uri,
            DashboardExportError, err_json={
                'id': self.id,
                'wildcard_filter': wildcard_filter
            }
        )

    def _get_client_export(self, common_filters, wildcard_filter):
        '''
        Retrieve the client export, second step of the dashboard download
        '''
        self._get_execution_context(common_filters)

        client_export_uri = self.CLIENT_EXPORT_URI % {
            'project_id': self.project.id
        }

        wildcard_filter = '?' + wildcard_filter['attribute'] + '=' \
            + urllib2.quote(wildcard_filter['value'].encode('utf8')) if wildcard_filter else ''

        client_export_data = {
            "clientExport": {
                "url": self.connection.HOST + "/dashboard.html" + wildcard_filter
                + "#project=/gdc/projects/" + self.project.id
                + "&dashboard=/gdc/md/" + self.project.id + "/obj/" + self.dashboards_id
                + "&tab=" + self.id + "&export=1&ctx=" + self.execution_context_response_uri,
                "name": self.name
            }
        }

        client_export_response = self.project.connection.post(
            client_export_uri,
            client_export_data,
            raise_cls=DashboardExportError,
            err_msg=self.err_msg % {'id': self.id}
        )
        self.client_export_response_uri = client_export_response.json()['asyncTask']['link']['poll']

    def _get_execution_context(self, common_filters):
        '''
        Retrieve the execution context, first step of the dashboard download.
        '''
        logger.debug('Retrieving the execution context')

        execution_context_uri = self.EXECUTION_CONTEXT_URI % {
            'project_id': self.project.id,
            'user_id': self.user_id
        }

        execution_context_data = {
            "executionContext": {
                "filters": []
            }
        }
        for common_filter in common_filters:
            try:
                execution_context_data['executionContext']['filters'].append(
                    {
                        "uri": "/gdc/md/" + self.project.id + "/obj/" + str(common_filter['object_id']),
                        "constraint": common_filter['constraint']
                    }
                )
            except KeyError:
                err_msg = 'common_filter %s is missing object_id or constraint key' % common_filter
                raise DashboardExportError(err_msg)

        execution_context_response = self.project.connection.post(
            execution_context_uri,
            execution_context_data,
            raise_cls=DashboardExportError,
            err_msg=self.err_msg % {'id': self.id}
        )
        self.execution_context_response_uri = execution_context_response.json()['uri']
