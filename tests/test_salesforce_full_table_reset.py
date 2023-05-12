import unittest
import datetime
import pytz

from tap_tester import runner, menagerie, connections

from base import SalesforceBaseTest


class SalesforceFullTableReset(SalesforceBaseTest):
    @staticmethod
    def name():
        return "tap_tester_salesforce_full_table_reset"

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
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Get Streams from the current state
        """
        stream_to_current_state = {stream : bookmark.get(self.expected_replication_keys()[stream].pop())
                                   for stream, bookmark in current_state['bookmarks'].items()}
        return stream_to_current_state

    def test_run(self):
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
        streams_replication_methods = {stream: self.FULL_TABLE
                                       for stream in expected_streams}
        self.set_replication_methods(conn_id, catalog_entries, streams_replication_methods)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_bookmarks = menagerie.get_state(conn_id)

        # UPDATE STATE BETWEEN SYNCS
        new_states = {'bookmarks': dict()}
        for stream, new_state in self.calculated_states_by_stream(first_sync_bookmarks).items():
            replication_key = list(replication_keys[stream])[0]
            # Remove stream User to reset
            if stream != 'User':
                new_states['bookmarks'][stream] = {replication_key: new_state}
        menagerie.set_state(conn_id, new_states)

        # SYNC 2
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_bookmarks = menagerie.get_state(conn_id)

        #Verify if the 2 syncs returned the same set of records
        self.assertEqual(first_sync_bookmarks, second_sync_bookmarks)

        # Test by stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # Verify the number of records in the 2nd sync is the same as the first
                self.assertEqual(second_sync_count, first_sync_count)
