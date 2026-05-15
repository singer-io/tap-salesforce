"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
from tap_tester import connections

from test_salesforce_automatic_fields_rest import SalesforceAutomaticFields


class SalesforceAutomaticFieldsBulk(SalesforceAutomaticFields):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_salesforce_auto_bulk"

    def _verify_discovered_catalog(self, found_catalogs):
        """Assert that every expected stream survived the do_discover filters.

        The do_discover changes filter out objects with queryable=False or
        deprecatedAndHidden=True. Standard objects (Account, Contact, …) must
        never be removed by those filters.
        """
        expected_streams = self.expected_sync_streams()
        found_stream_names = {c.get('stream_name') for c in found_catalogs}
        missing_from_catalog = expected_streams - found_stream_names
        self.assertSetEqual(
            set(),
            missing_from_catalog,
            msg="The following expected streams were absent from the discovered "
                "catalog. They may have been incorrectly filtered by the "
                "queryable / deprecatedAndHidden discovery checks: {}".format(
                    missing_from_catalog))

    def test_run(self):
        self.automatic_fields_test()
