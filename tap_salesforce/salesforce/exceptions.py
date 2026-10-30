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

class TapSalesforceStreamUnavailableException(TapSalesforceException):
    """Raised when a Bulk API batch fails because of a condition tied to the
    org's own environment, licensing, or permissions -- not a tap bug or a
    true data/Bulk-API-specific failure -- that makes the target object
    entirely unqueryable for this org. Known cases:
      - A disabled/unlicensed API such as Data.com
        (`DATACLOUD_API_DISABLED_EXCEPTION`).
      - An external object whose backing service can't be reached, e.g.
        `ExternalEncryptionRootKey`'s BYOK key management service
        (`EXTERNAL_OBJECT_EXCEPTION: We can't connect to your key management
        service...`).
    Callers catch this to skip the affected stream instead of failing the
    whole sync, since the REST API would hit the identical restriction.
    """

class Client406Error(Exception):
    pass
