"""
This module defines custom exceptions used throughout the Salesforce tap.

These exceptions are used to handle specific error conditions, such as API quota
exceedances or when a required API is disabled.
"""

# pylint: disable=super-init-not-called

class TapSalesforceException(Exception):
    """Base exception for the Salesforce tap."""
    pass

class TapSalesforceQuotaExceededException(TapSalesforceException):
    """
    Exception raised when a Salesforce API quota has been exceeded.

    This can be either the total daily quota or the per-run quota configured
    for the tap.
    """
    pass

class TapSalesforceBulkAPIDisabledException(TapSalesforceException):
    """
    Exception raised when the Bulk API is used but is disabled for the org.
    """
    pass
