# pylint: disable=super-init-not-called

class TapSalesforceException(Exception):
    pass

class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass

class TapSalesforceBulkAPIDisabledException(TapSalesforceException):
    pass

class TapSalesforceBulkObjectNotSupportedException(TapSalesforceException):
    """Raised when Salesforce rejects Bulk API job creation for a specific
    object with 'is not supported by the Bulk API'. Callers catch this to
    fall back to querying the affected object via the REST API instead."""
    pass

class Client406Error(Exception):
    pass
