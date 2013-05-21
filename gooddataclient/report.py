import time
import logging

from gooddataclient.exceptions import ReportExecutionFailed, ReportExportFailed, ReportRetrievalFailed


logger = logging.getLogger("gooddataclient")


class Report(object):
    '''
    This class represents a GD report.
    Use it to programatically export a GD report
    as a csv
    '''

    REPORT_DEFINITION_URI = '/gdc/md/%(project)s/obj/%(report)s'
    REPORT_EXEC_URI = '/gdc/xtab2/executor3'
    REPORT_EXPORT_URI = '/gdc/exporter/executor'

    def __init__(self, project, id):
        self.project = project
        self.connection = project.connection
        self.id = id
        self.exec_result = None
        self.export_download_uri = None
        self.report_content = None

    def execute_report(self):
        '''
        Use this method to retrieve a report's information
        given a report id.
        '''
        report_definition = self.REPORT_DEFINITION_URI % {
            'project': self.project.id,
            'report': self.id
        }
        request_data = {
            "report_req": {
                "report": report_definition
            }
        }
        response = self.connection.post(
            uri=self.REPORT_EXEC_URI, data=request_data,
            raise_cls=ReportExecutionFailed, report_id=self.id
        )
        self.exec_result = response.json()

    def export_report(self):
        '''
        Use this method to retrieve the report's data uri.
        Stores the uri in export_download_uri.
        '''
        if not self.exec_result:
            self.execute_report()

        request_data = {
            "result_req": {
                "format": "csv",
                "result": self.exec_result
            }
        }
        response = self.connection.post(
            uri=self.REPORT_EXPORT_URI, data=request_data,
            raise_cls=ReportExportFailed, report_id=self.id
        )

        self.export_download_uri = response.json()['uri']

    def get_report(self):
        '''
        Use this method to retrieve the report's data.
        Stores the data in report_content.
        '''
        if self.is_ready:
            return self.report_content

        if not self.export_download_uri:
            self.export_report()
        response = self.connection.get(
            uri=self.export_download_uri,
            raise_cls=ReportRetrievalFailed,
            report_id=self.id
        )
        self.report_content = response.text

        if not self.is_ready:
            time.sleep(0.5)
            self.get_report()

    def save_report(self, file_path):
        '''
        Use this method to save the report's data
        in a given file.
        '''
        if not self.is_ready:
            self.get_report()
        with open(file_path, 'w') as f:
            f.write(self.report_content)

    @property
    def is_ready(self):
        '''
        Calling GD to export a report can be long.
        During the call and before the final response,
        GD replies with the current URL.
        report_content contains the current URL, which
        starts with '{'
        The report is ready when the response does not start
        with '{'
        '''
        report_content = self.report_content or ''
        return report_content and report_content[0] != '{'
