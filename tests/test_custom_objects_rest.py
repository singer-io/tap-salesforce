"""
Test that all fields can be replicated for a stream that is a custom object (REST API)
"""
from datetime import datetime, timedelta

from tap_tester import connections, menagerie, runner

from sfbase import SFBaseTest


class SalesforceCustomObjects(SFBaseTest):
    """Test that all fields can be replicated for a stream that is a custom object"""

    # Note: custom record as seen Aug 9, 2023
    # TODO do we need to hard code start date to 08-04?
    # 'CreatedDate': '2023-08-04T20:13:50.000000Z'
    # 'SystemModstamp': '2023-08-04T20:13:50.000000Z',
    # 'LastModifiedDate': '2023-08-04T20:13:50.000000Z',
    # 'LastReferencedDate': '2023-08-09T20:19:45.000000Z',
    # 'LastViewedDate': '2023-08-09T20:19:45.000000Z',
    start_date = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%dT00:00:00Z")

    @staticmethod
    def expected_sync_streams():
        return {
            'TapTester__c',  # 1 created record present, spike on CRUD?
            # 'TapTester__Share',  # TODO no data, fix or omit?
        }

    def custom_objects_test(self):
        """
        Verify that all fields can be replicated for a custom  stream

        PREREQUISITE
        Create a custom object and at least one data record for that cusotm object
        """

        expected_streams = self.expected_sync_streams()

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # grab metadata after performing table-and-field selection to set expectations
        #   used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = stream_to_all_catalog_fields.get(stream)

                # collect actual values
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']
                                        if row['action'] == 'upsert']

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)


class SalesforceCustomObjectsRest(SalesforceCustomObjects):
    """Test that all fields can be replicated for a stream that is a custom object (REST API)"""

    salesforce_api = 'REST'

    @staticmethod
    def name():
        return "tt_salesforce_custom_obj_rest"

    @staticmethod
    def streams_to_selected_fields():
        """Note: if this is overridden you are not selecting all fields.
        Therefore this should rarely if ever be used for this test."""
        return {}

    def test_run(self):
        self.custom_objects_test()
