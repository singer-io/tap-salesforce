class TapSalesforceException(Exception):
    pass

class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass

class TapSalesforceHTTPException(TapSalesforceException):
    def __init__(self, requestException):
        self.requestException = requestException

    def __str__(self):
        str(self.requestException) + ", Response from Salesforce: {}".format(self.requestException.response.text)
