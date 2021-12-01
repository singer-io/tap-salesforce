import unittest
from tap_tester import connections, runner

from base import SalesforceBaseTest


class SalesforceStartDateTest(SalesforceBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_salesforce_start_date_test"

    @staticmethod
    def expected_sync_streams():
        return {
            'Account',
            'Contact',
            'Lead',
            'Opportunity',
            'User',
        }
    @unittest.skip("SKIPPING TESTS UNTIL NEW TEST INSTANCE IS AVAILABLE")
    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.assertTrue(self.expected_sync_streams().issubset(self.expected_streams()))

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=1)

        self.start_date = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields,
                                                    select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Sync3s
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)

        replicated_row_count_2 = sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))
        synced_records_2 = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)

                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                if replication_type == self.INCREMENTAL:
                    replication_key = next(iter(self.expected_replication_keys().get(stream)))

                    # Verify replication key is greater or equal to start_date for sync 1
                    replication_dates_1 =[row.get('data').get(replication_key) for row in
                                          synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                          if row.get('data')]
                    for replication_date in replication_dates_1:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date), self.parse_date(self.start_date_1),
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(self.start_date_1) +
                                "Record date: {} ".format(replication_date)
                        )

                    # # Verify replication key is greater or equal to start_date for sync 2
                    replication_dates_2 =[row.get('data').get(replication_key) for row in
                                          synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                          if row.get('data')]
                    for replication_date in replication_dates_2:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date), self.parse_date(self.start_date_2),
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(self.start_date_2) +
                                "Record date: {} ".format(replication_date)
                        )

                    # TODO | https://stitchdata.atlassian.net/browse/SRCE-4824
                    # Set data such that records exist enabling us to be more strict in the
                    # following assertion (using 'assertLess' rather than 'assertLessEqual')

                    # Verify that the 2nd sync with a later start date replicates fewer records
                    # or the same number of records than the first 1st sync.
                    # This depends on existing data
                    self.assertLessEqual(
                        record_count_2, record_count_1,
                        msg="Second sync should result in fewer records\n" +
                        "Sync 1 start_date: {} ".format(self.start_date) +
                        "Sync 1 record_count: {}\n".format(record_count_1) +
                        "Sync 2 start_date: {} ".format(self.start_date_2) +
                        "Sync 2 record_count: {}".format(record_count_2))

                elif replication_type == self.FULL_TABLE:

                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(
                        record_count_2, record_count_1,
                        msg="Second sync should result in fewer records\n" +
                        "Sync 1 start_date: {} ".format(self.start_date) +
                        "Sync 1 record_count: {}\n".format(record_count_1) +
                        "Sync 2 start_date: {} ".format(self.start_date_2) +
                        "Sync 2 record_count: {}".format(record_count_2))

                else:

                    raise Exception(
                        "Expectations are set incorrectly. {} cannot have a replication method of {}".format(
                            stream, replication_type
                        )
                    )
