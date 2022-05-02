# pylint: disable=super-init-not-called


class TapSalesforceException(Exception):
    pass


class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass


class TapSalesforceOauthException(TapSalesforceException):
    pass


class SalesforceException(Exception):
    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code

