import datetime
import dateutil.parser
import pytz

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import SalesforceBaseTest


class SalesforceBookmarks(SalesforceBaseTest):
    def name(self):
        return "tap_tester_salesforce_bookmarks"

    def expected_sync_streams(self):
        return {
            'Account',
            'Contact',
            # 'Lead', # cannot test, need multiple days of data
            # 'Opportunity',  # cannotest, dates are 1 s apart
            'User',
        }

    def convert_state_to_utc(self, date_str):
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
        Look at the bookmarks from a previous sync and set a new bookmark
        value that is 1 day prior. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.
        """

        stream_to_current_state = {stream : bookmark.get(self.expected_replication_keys()[stream].pop())
                                   for stream, bookmark in current_state['bookmarks'].items()}
        stream_to_calculated_state = {stream: "" for stream in self.expected_sync_streams()}

        for stream, state in stream_to_current_state.items():
            # convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            # subtract n days from the state
            n = 3 if stream in {'Lead', 'Opportunity'} else 1
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=n)
            # convert back to string and format
            calculated_state = datetime.datetime.strftime(calculated_state_as_datetime, "%Y-%m-%dT%H:%M:%S.000000Z")
            stream_to_calculated_state[stream] = calculated_state

        return stream_to_calculated_state


    def test_run(self):
        replication_keys = self.expected_replication_keys()

        # SYNC 1
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_sync_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)
        self.set_replication_methods(conn_id, catalog_entries)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        # UPDATE STATE BETWEEN SYNCS
        new_states = {'bookmarks': dict()}
        for stream, new_state in self.calculated_states_by_stream(first_sync_bookmarks).items():
            replication_key = list(replication_keys[stream])[0]
            new_states['bookmarks'][stream] = {replication_key: new_state}
        menagerie.set_state(conn_id, new_states)

        # SYNC 2
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        # Test by stream
        for stream in self.expected_sync_streams():
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

                # Verify the first sync sets a bookmark of the expected form
                self.assertIsNotNone(first_bookmark_key_value)
                self.assertIsNotNone(first_bookmark_key_value.get(replication_key))

                # Verify the second sync sets a bookmark of the expected form
                self.assertIsNotNone(second_bookmark_key_value)
                self.assertIsNotNone(second_bookmark_key_value.get(replication_key))

                # bookmarked states (actual values)
                first_bookmark_value = first_bookmark_key_value.get(replication_key)
                second_bookmark_value = second_bookmark_key_value.get(replication_key)
                # bookmarked values as utc for comparing against records
                first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                # Verify the second sync bookmark is Equal to the first sync bookmark
                self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                # Verify the second sync records respect the previous (simulated) bookmark value
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

                # Verify the number of records in the 2nd sync is less then the first
                self.assertLess(second_sync_count, first_sync_count)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
