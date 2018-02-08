# pylint: disable=super-init-not-called

class TapSalesforceException(Exception):
    pass


class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass


class TapSalesforceHTTPException(TapSalesforceException):
    pass
