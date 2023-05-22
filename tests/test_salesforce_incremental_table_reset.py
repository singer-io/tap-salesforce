import unittest
import datetime
import dateutil.parser
import pytz

from tap_tester import runner, menagerie, connections

from base import SalesforceBaseTest


class SalesforceIncrementalTableReset(SalesforceBaseTest):
    @staticmethod
    def name():
        return "tap_tester_salesforce_incremental_table_reset"

    @staticmethod
    def expected_sync_streams():
        return {
            'Account',
            'Contact',
            'User', 
        }

    @staticmethod
    def convert_state_to_utc(date_str):
        """
        Convert a saved bookmark value of form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def get_states_by_stream(self, current_state):
        """
        Returns the streams from the current state
        """

        stream_to_current_state = {stream : bookmark.get(self.expected_replication_keys()[stream].pop())
                                   for stream, bookmark in current_state['bookmarks'].items()}
        return stream_to_current_state

    def test_run(self):
        print("in test ")
        self.salesforce_api = 'BULK'

        replication_keys = self.expected_replication_keys()

        # SYNC 1
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_sync_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)
        streams_replication_methods = {stream: self.INCREMENTAL
                                       for stream in expected_streams}

        self.set_replication_methods(conn_id, catalog_entries, streams_replication_methods)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()

        first_sync_bookmarks = menagerie.get_state(conn_id)

        # UPDATE STATE for Table Reset
        new_states = {'bookmarks': dict()}
        print("new states is ", new_states )

        for stream, new_state in self.get_states_by_stream(first_sync_bookmarks).items():
            replication_key = list(replication_keys[stream])[0]
            # Remove stream User to simulate table reset
            if stream != 'User':
                new_states['bookmarks'][stream] = {replication_key: new_state}
        print("new states 2 is ", new_states )
        menagerie.set_state(conn_id, new_states)

        # SYNC 2
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        # Test by stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # data from record messages
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']

                # replication key for comparing data
                self.assertEqual(1, len(list(replication_keys[stream])),
                                 msg="Compound primary keys require a change to test expectations")
                replication_key = list(replication_keys[stream])[0]

                # bookmarked states (top level objects)
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks').get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks').get(stream)

                # bookmarked states (actual values)
                first_bookmark_value = first_bookmark_key_value.get(replication_key)
                second_bookmark_value = second_bookmark_key_value.get(replication_key)

                # bookmarked values as utc for comparing against records
                first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                # Verify the second sync bookmark is Equal to the first sync bookmark
                self.assertGreaterEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                # Verify the second sync records respect the previous (simulated) bookmark value for the streams that are not reset
                if stream != 'User' :
                    simulated_bookmark_value = new_states['bookmarks'][stream][replication_key]
                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                            msg="Second sync records do not repect the previous bookmark.")

                # Verify the first sync bookmark value is the max replication key value for a given stream
                for record in first_sync_messages:
                    replication_key_value = record.get(replication_key)
                    self.assertLessEqual(replication_key_value, first_bookmark_value_utc,
                                         msg="First sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                # Verify the second sync bookmark value is the max replication key value for a given stream
                for record in second_sync_messages:
                    replication_key_value = record.get(replication_key)
                    self.assertLessEqual(replication_key_value, second_bookmark_value_utc,
                                         msg="Second sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                # Verify the number of records in the 2nd sync is greater or equal to first 
                # If no new records in between 2 sync, the second will be eual to first for 'User' and will be less for the other streams
                if stream != 'User':
                    self.assertLess(second_sync_count, first_sync_count)
                else:
                    self.assertGreaterEqual(second_sync_count, first_sync_count)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
