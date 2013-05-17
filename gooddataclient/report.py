import time
import logging
import json

from requests.exceptions import HTTPError, ConnectionError

from gooddataclient.exceptions import (
    GoodDataTotallyDown, ReportExecutionFailed,
    get_api_msg, ReportExportFailed, ReportRetrievalFailed
)


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
        try:
            request_data = {
                "report_req": {
                    "report": report_definition
                }
            }
            response = self.connection.post(uri=self.REPORT_EXEC_URI, data=request_data)
            response.raise_for_status()
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportExecutionFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)
        self.exec_result = response.json()

    def export_report(self):
        '''
        Use this method to retrieve the report's data uri.
        Stores the uri in export_download_uri.
        '''
        if not self.exec_result:
            self.execute_report()
        try:
            request_data = {
                "result_req": {
                    "format": "csv",
                    "result": self.exec_result
                }
            }
            response = self.connection.post(uri=self.REPORT_EXPORT_URI, data=request_data)
            response.raise_for_status()
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportExportFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)
        self.export_download_uri = response.json()['uri']

    def get_report(self):
        '''
        Use this method to retrieve the report's data.
        Stores the data in report_content.
        '''
        if self.report_content and not self.report_content[0] == '{':
            return self.report_content

        if not self.export_download_uri:
            self.export_report()
        try:
            self.report_content = self.connection.get(self.export_download_uri).text
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportRetrievalFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)

        if self.report_content[0] == '{':
            time.sleep(0.5)
            self.get_report()
        print self.connection.get(self.export_download_uri).text[:100]

    def save_report(self, file_path):
        '''
        Use this method to save the report's data
        in a given file.
        '''
        if not self.report_content:
            self.get_report()
        with open(file_path, 'w') as f:
            f.write(self.report_content)
