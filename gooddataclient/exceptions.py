
class GoodDataClientError(Exception):

    def __init__(self, msg, error_info=None):
        self.msg = msg
        self.error_info = error_info or {}

    def __str__(self):
        return repr(self.msg)


class AuthenticationError(GoodDataClientError):
    pass


class ProjectNotOpenedError(GoodDataClientError):
    pass


class ProjectNotFoundError(GoodDataClientError):
    pass


class DataSetNotFoundError(GoodDataClientError):
    pass


class UploadFailed(GoodDataClientError):
    pass


class MaqlExecutionFailed(GoodDataClientError):
    pass


def get_api_msg(err_json):
    return err_json['message'] % tuple(err_json['parameters'])
