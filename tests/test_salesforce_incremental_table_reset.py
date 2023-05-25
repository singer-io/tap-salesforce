import unittest
import datetime
import dateutil.parser
import pytz
from datetime import datetime, timedelta

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

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = datetime.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

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
        streams_replication_methods = {stream: self.INCREMENTAL
                                       for stream in expected_streams}
        self.set_replication_methods(conn_id, catalog_entries, streams_replication_methods)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()

        first_sync_bookmarks = menagerie.get_state(conn_id)

        # UPDATE STATE for Table Reset
        new_states = {'bookmarks': dict()}
        stream_to_current_state = {stream : bookmark.get(self.expected_replication_keys()[stream].pop())
                                   for stream, bookmark in first_sync_bookmarks['bookmarks'].items()}
        for stream in stream_to_current_state:
            replication_key = list(replication_keys[stream])[0]
            # Remove stream User to reset
            if stream != 'User':
                new_states['bookmarks'][stream] = {replication_key: stream_to_current_state[stream]}
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

                filtered_synced_records = [
                    record for record in second_sync_messages
                    if self.parse_date(record[replication_key]) <=
                    self.parse_date(first_bookmark_value)]

                # Verify the second sync bookmark is Equal to the first sync bookmark
                self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                # Verify the number of records in the 2nd sync is equal to first for the stream that is reset and is less then the firsa for the restt
                if stream != 'User':
                    self.assertLess(len(filtered_synced_records), first_sync_count)
                else:
                    self.assertEqual(len(filtered_synced_records), first_sync_count)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
