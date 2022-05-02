# pylint: disable=super-init-not-called


from typing import Optional
from requests import Response


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
    
    err_dict = err_array[0]

    if not isinstance(err_dict, dict):
        return None

    if not err_dict['message'] or not err_dict['errorCode']:
        return None
            
    return SalesforceException(err_dict['message'], err_dict['errorCode'])

