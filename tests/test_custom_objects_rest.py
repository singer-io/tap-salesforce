"""
Test that all fields can be replicated for a stream that is a custom object (REST API)
"""
from datetime import datetime, timedelta

from tap_tester import connections, menagerie, runner

from sfbase import SFBaseTest


class SalesforceCustomObjects(SFBaseTest):
    """Test that all fields can be replicated for a stream that is a custom object"""

    # hard code start date to replicate single custom record until CRUD is implemented
    # 'SystemModstamp': '2023-08-04T20:13:50.000000Z',  # record info
    start_date = '2023-08-04T00:00:00Z'

    @staticmethod
    def expected_sync_streams():
        return {
            'TapTester__c',  # 1 created record present, spike on CRUD?
            # 'TapTester__Share',  # TODO no data, fix or omit?
        }

    def custom_objects_test(self):
        """
        Verify that all fields can be replicated for a custom stream

        PREREQUISITE
        Create a custom object and at least one data record for that custom object
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

        # set additional metadata to define replication key for the custom stream
        catalog = test_catalogs[0]
        schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        non_selected_fields = []
        additional_md = [{"breadcrumb": [],
                          "metadata": {
                              "replication-key": "SystemModstamp"
                          }}]

        connections.select_catalog_and_fields_via_metadata(
            conn_id,
            catalog,
            schema_and_metadata,
            additional_md,
            non_selected_fields)

        # grab metadata after performing table-and-field selection to set expectations,
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

                # Verify that all selected / expected fields are sent to the target
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
