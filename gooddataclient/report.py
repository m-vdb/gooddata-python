import logging

from requests.exceptions import (
    HTTPError, ConnectionError
)

from gooddataclient.exceptions import (
    GoodDataTotallyDown, ReportExecutionFailed,
    get_api_msg, ReportExportFailed, ReportRetrievalFailed
)


logger = logging.getLogger("gooddataclient")


class Report(object):

    REPORTS_URI = '/gdc/md/%s/query/reports'
    REPORT_DEFINITION_URI = '/gdc/md/%(project)s/obj/%(report)s'
    REPORT_EXEC_URI = '/gdc/xtab2/executor3'
    REPORT_EXPORT_URI = '/gdc/exporter/executor'
    PULL_URI = '/gdc/md/%s/etl/pull'

    def __init__(self, connection, project, id):
        self.connection = connection
        self.project = project
        self.id = id
        self.execResult = None
        self.export_download_URI = None
        self.report_content = None

    def execute_report(self):
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
            self.execResult = response.json()
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportExecutionFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)

    def export_report(self):
        if not self.execResult:
            self.execute_report()
        try:
            request_data = {
                "result_req": {
                    "format": "csv",
                    "result": self.execResult
                }
            }
            response = self.connection.post(uri=self.REPORT_EXPORT_URI, data=request_data)
            response.raise_for_status()
            self.export_download_URI = response.json()['uri']
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportExportFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)

    def get_report(self):
        if self.report_content:
            return self.report_content

        if not self.export_download_URI:
            self.export_report()
        try:
            self.report_content = self.connection.get(self.export_download_URI).text
        except HTTPError, err:
            err_json = err.response.json()['error']
            raise ReportRetrievalFailed(
                get_api_msg(err_json), gd_error=err_json,
                status_code=err.response.status_code, report_id=self.id
            )
        except ConnectionError, err:
            raise GoodDataTotallyDown(err.message)

    def save_report(self, file_path):
        if not self.report_content:
            self.get_report()
        with open(file_path, 'w') as f:
            f.write(self.report_content)
