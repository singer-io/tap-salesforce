from datetime import datetime, timedelta
from tap_tester import connections, runner, menagerie
from base import SalesforceBaseTest

class SalesforceLookbackWindow(SalesforceBaseTest):

    # subtract the desired amount of seconds form the date and return
    def timedelta_formatted(self, dtime, format, seconds=0):
        date_stripped = datetime.strptime(dtime, format)
        return_date = date_stripped + timedelta(seconds=seconds)

        return datetime.strftime(return_date, format)

    @staticmethod
    def name():
        return 'tap_tester_salesforce_lookback_window'

    def get_properties(self):  # pylint: disable=arguments-differ
        return {
            'start_date' : '2021-11-10T00:00:00Z',
            'instance_url': 'https://singer2-dev-ed.my.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': self.salesforce_api,
            'is_sandbox': 'false',
            'lookback_window': 86400
        }

    def expected_sync_streams(self):
        return {
            'Account'
        }

    def run_test(self):
        # create connection
        conn_id = connections.ensure_connection(self)
        # create state file
        state = {
            'bookmarks':{
                'Account': {
                    'SystemModstamp': '2021-11-10T00:00:00.000000Z'
                }
            }
        }
        # set state file to run in sync mode
        menagerie.set_state(conn_id, state)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # select certain catalogs
        expected_streams = self.expected_sync_streams()
        catalog_entries = [catalog for catalog in found_catalogs
                            if catalog.get('tap_stream_id') in expected_streams]

        # stream and field selection
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # run sync
        self.run_and_verify_sync(conn_id)

        # get synced records
        sync_records = runner.get_records_from_target_output()

        # get replication keys
        expected_replication_keys = self.expected_replication_keys()

        # get bookmark ie. date from which the sync started
        bookmark = state.get('bookmarks').get('Account').get('SystemModstamp')
        # calculate the simulated bookmark by subtracting lookback window seconds
        bookmark_with_lookback_window = self.timedelta_formatted(bookmark, format=self.BOOKMARK_COMPARISON_FORMAT, seconds=-self.get_properties()['lookback_window'])

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # get replication key for stream
                replication_key = list(expected_replication_keys[stream])[0]

                # get records
                records = [record.get('data') for record in sync_records.get(stream).get('messages')
                           if record.get('action') == 'upsert']

                # check for the record if it is between lookback date and bookmark
                is_between = False

                for record in records:
                    replication_key_value = record.get(replication_key)

                    # Verify the sync records respect the (simulated) bookmark value
                    self.assertGreaterEqual(self.parse_date(replication_key_value), self.parse_date(bookmark_with_lookback_window),
                                            msg='The record does not respect the lookback window.')

                    # verify if the record's bookmark value is between bookmark and (simulated) bookmark value
                    if self.parse_date(bookmark_with_lookback_window) <= self.parse_date(replication_key_value) < self.parse_date(bookmark):
                        is_between = True

                    self.assertTrue(is_between, msg='No record found between bookmark and lookback date.')

    def test_run(self):
        # run with REST API
        self.salesforce_api = 'REST'
        self.run_test()

        # run with BULK API
        self.salesforce_api = 'BULK'
        self.run_test()
