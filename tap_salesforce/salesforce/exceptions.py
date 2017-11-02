# pylint: disable=super-init-not-called

class TapSalesforceException(Exception):
    pass


class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass


class TapSalesforceHTTPException(TapSalesforceException):
    def __init__(self, requestException):
        self.requestException = requestException

    def __str__(self):
        return str(self.requestException) + \
            ", Response from Salesforce: {}".format(self.requestException.response.text)
