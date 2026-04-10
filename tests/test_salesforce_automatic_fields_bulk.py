"""
Test that with no fields selected for a stream automatic fields are still replicated
"""
import unittest
from datetime import datetime, timedelta

from tap_tester import runner, connections

from test_salesforce_automatic_fields_rest import SalesforceAutomaticFields


class SalesforceAutomaticFieldsBulk(SalesforceAutomaticFields):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_salesforce_auto_bulk"

    def automatic_fields_test(self):
        """
        Extends the base automatic_fields_test with BULK-specific assertions
        that validate the behaviour introduced by the unsupported-stream fixes:

        1. All expected streams must survive discovery (i.e. they were not
           incorrectly filtered by the new queryable / deprecatedAndHidden
           checks added to do_discover).
        2. The base null-guard on synced_records catches any stream that was
           silently skipped due to our 400-InvalidEntity handling in
           sync_stream (inherited from the parent).
        """
        expected_streams = self.expected_sync_streams()

        # instantiate connection
        conn_id = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # --- BULK-specific assertion ----------------------------------------
        # Verify that every expected stream is present in the catalog.
        # Our do_discover changes filter out objects with queryable=False or
        # deprecatedAndHidden=True.  Standard objects (Account, Contact, …)
        # must never be removed by those filters.
        found_stream_names = {c.get('stream_name') for c in found_catalogs}
        missing_from_catalog = expected_streams - found_stream_names
        self.assertSetEqual(
            set(),
            missing_from_catalog,
            msg="The following expected streams were absent from the discovered "
                "catalog. They may have been incorrectly filtered by the "
                "queryable / deprecatedAndHidden discovery checks: {}".format(
                    missing_from_catalog))
        # --------------------------------------------------------------------

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                data = synced_records.get(stream)

                # Guard: a None here means the stream produced no target output.
                # With the new 400-InvalidEntity skip in sync_stream this gives a
                # clear failure message instead of a cryptic TypeError.
                self.assertIsNotNone(
                    data,
                    msg="Stream '{}' was not found in synced records. "
                        "It may have been silently skipped due to a Bulk API "
                        "incompatibility (400 InvalidEntity).".format(stream))

                record_messages_keys = [set(row['data'].keys()) for row in data['messages']
                                        if row['action'] == 'upsert']

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

    def test_run(self):
        self.automatic_fields_test()
