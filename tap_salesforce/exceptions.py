# pylint: disable=super-init-not-called


from typing import Optional
from requests import Response


class TapSalesforceException(Exception):
    pass


class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass


class TapSalesforceOauthException(TapSalesforceException):
    pass

class TapSalesforceInvalidCredentialsException(TapSalesforceException):
    pass

class SalesforceException(Exception):
    def __init__(self, message: str, code: Optional[str] = None) -> None:
        super().__init__(message)
        self.code = code


# build_salesforce_exception transforms a generic Response into a SalesforceException if the
# response body has a salesforce exception, returns None otherwise
# salesforce error body looks like:
# [
#   {
#       'message': 'Your query request was running for too long.',
#       'errorCode': 'QUERY_TIMEOUT',
#   }
# ]
def build_salesforce_exception(resp: Response) -> Optional[SalesforceException]:
    err_array = resp.json()
    if not isinstance(err_array, list):
        return None

    if len(err_array) < 1:
        return None

    err_dict = err_array[0]

    if not isinstance(err_dict, dict):
        return None

    msg = err_dict.get("message")
    if msg is None:
        return None

    code = err_dict.get("errorCode")

    return SalesforceException(msg, code)
