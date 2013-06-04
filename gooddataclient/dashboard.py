import json
import requests
import urllib2
import logging
import os

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
    PPT_UPLOAD_FILE = 'http://work4.alwaysdata.net/api'
    PPT_DOWNLOAD_FILE = 'http://work4.alwaysdata.net/output/report.pptx'

    EXECUTION_CONTEXT_DATA = '{"executionContext":{"filters":[{"uri":"/gdc/md/%(project_id)s/obj/126",'\
        + '"constraint":{"type":"floating","from":%(from)s,"to":%(to)s}}]}}'

    CLIENT_EXPORT_DATA = '{"clientExport":{"url":"%(connection_host)s/dashboard.html'\
        + '?%(entity_identifier)s=%(entity_name)s#project=/gdc/projects/%(project_id)s'\
        + '&dashboard=/gdc/md/%(project_id)s/obj/%(dashboards_id)s'\
        + '&tab=%(dashboard_id)s&export=1&ctx=%(execution_context_response_uri)s",'\
        + '"name":"%(dashboard_name)s"}}'

    COMPANY_IDENTIFIER = 'label.page.company_name_2'
    PAGE_IDENTIFIER = 'label.page.page_name'

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

    def download(self, date_filters, company=None, page=None, output_dir='./', delete_pdf=True):
        '''
        This method exports a dashboard as a pdf.
        It does it in 3 steps:
        - ask for the execution context (and apply date filters)
        - ask to generate the report with this execution context (and add company or page filters)
        - download and save the report
        :param date_filters:        json input to give date filters (like {"from": -3, "to": -1}).
                                    The function is set to use these values as months.
                                    -3 to -1 means from 3 full months ago to last full month
        :param company:             We want to generate reports for a company or for a page.
        :param page:                Use company to filter on a company, use page to filter on a page
        :param output_dir:          Where to save the file
        :param delete_pdf:          set to false to keep the pdf file
        '''
        if company:
            entity = company
            entity_identifier = self.COMPANY_IDENTIFIER
        elif page:
            entity = page
            entity_identifier = self.PAGE_IDENTIFIER
        else:
            raise ValueError("page or company should not be empty")

        logger.info('Exporting dashboard %(dashboard_name)s for entity %(entity)s' % {
            'dashboard_name': self.name,
            'entity': entity
        })
        self._get_execution_context(date_filters)
        self._get_client_export(entity, entity_identifier)
        self._poll_for_pdf()
        logger.debug('PDF downloaded')
        self._save_as_pdf(output_dir, entity)
        logger.debug('PDF saved')
        self._pdf_to_ppt(output_dir, entity, delete_pdf)
        logger.debug('PPT saved')

    def _get_execution_context(self, date_filters):
        '''
        Retrieve the execution context, first step of the pdf download.
        '''
        execution_context_uri = self.EXECUTION_CONTEXT_URI % {
            'project_id': self.project.id,
            'user_id': self.user_id
        }
        execution_context_data = self.EXECUTION_CONTEXT_DATA % {
            'project_id': self.project.id,
            'from': date_filters['from'],
            'to': date_filters['to']
        }

        execution_context_response = self.project.connection.post(
            execution_context_uri,
            json.loads(execution_context_data),
            raise_cls=DashboardExportError,
            err_msg=self.err_msg % {'id': self.id}
        )
        self.execution_context_response_uri = execution_context_response.json()['uri']

    def _get_client_export(self, entity, entity_identifier):
        '''
        Retrieve the client export, second step of the pdf download
        '''
        if not self.execution_context_response_uri:
            raise DashboardExportError('An execution context shoud be done'
                                       + 'before retrieving the client export')

        entity_name = urllib2.quote(entity.encode('utf8'))

        client_export_uri = self.CLIENT_EXPORT_URI % {
            'project_id': self.project.id
        }

        client_export_data = self.CLIENT_EXPORT_DATA % {
            'connection_host': self.project.connection.HOST,
            'entity_identifier': entity_identifier,
            'entity_name': entity_name,
            'project_id': self.project.id,
            'dashboards_id': self.dashboards_id,
            'dashboard_id': self.id,
            'execution_context_response_uri': self.execution_context_response_uri,
            'dashboard_name': self.name
        }

        client_export_response = self.project.connection.post(
            client_export_uri,
            json.loads(client_export_data),
            raise_cls=DashboardExportError,
            err_msg=self.err_msg % {'id': self.id}
        )
        self.client_export_response_uri = client_export_response.json()['asyncTask']['link']['poll']

    def _poll_for_pdf(self):
        '''
        Poll and retrieve the pdf data
        '''
        if not self.client_export_response_uri:
            raise DashboardExportError('A client export shoud be done'
                                       + 'before retrieving the pdf data')

        self.pdf_data = self.connection.poll_server_response(
            self.client_export_response_uri,
            DashboardExportError,
            {'id': self.id}
        )

    def _save_as_pdf(self, output_dir, entity):
        '''
        Saves the downloaded data as pdf.
        '''
        if not self.pdf_data:
            raise DashboardExportError('A poll shoud be done'
                                       + 'before saving the pdf data as pdf')

        with open(output_dir + entity + '.pdf', 'wb') as handle:
            for block in self.pdf_data.iter_content(1024):
                if not block:
                    break
                handle.write(block)

    def _pdf_to_ppt(self, output_dir, pdf_name, delete_pdf):
        '''
        Given a pdf report name and a path, this method will convert
        the pdf into ppt using work4.alwaysdata.net web service
        '''
        # upload the pdf file to the web service
        filename = pdf_name + '.pdf'
        filepath = output_dir + filename
        requests.post(
            url=self.PPT_UPLOAD_FILE,
            data={'data': 'data'},
            files={'file': open(filepath)}
        )

        # retrieve the ppt file
        r = requests.get(self.PPT_DOWNLOAD_FILE)
        with open(output_dir + pdf_name + '.pptx', 'wb') as handle:
            for block in r.iter_content(1024):
                if not block:
                    break
                handle.write(block)
        if delete_pdf:
            os.remove(filepath)
