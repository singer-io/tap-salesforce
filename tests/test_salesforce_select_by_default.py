from tap_tester import connections, runner, menagerie

from sfbase import SFBaseTest


class SFSelectByDefault(SFBaseTest):
    @staticmethod
    def name():
        return "tt_sf_select_by_default"

    @staticmethod
    def streams_to_test():
        return {
            'Account',
            'LoginGeo',
        }

    def setUp(self):
        self.salesforce_api = 'BULK'
        self.start_date = "2021-11-11T00:00:00Z"
        # instantiate connection
        SFSelectByDefault.conn_id = connections.ensure_connection(self)

        # run check mode
        SFSelectByDefault.found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in self.found_catalogs
                                      if catalog.get('tap_stream_id') in self.streams_to_test()]
       
        SFSelectByDefault.test_streams = self.streams_to_test()

        self.perform_and_verify_table_selection(self.conn_id, test_catalogs)
        # run initial sync
        self.run_and_verify_sync_mode(self.conn_id)
        SFSelectByDefault.synced_records = runner.get_records_from_target_output()
        SFSelectByDefault.actual_fields = runner.examine_target_output_for_fields()

    def test_no_unexpected_streams_replicated(self):
        # gather results
        synced_stream_names = set(self.synced_records.keys())
        self.assertSetEqual(synced_stream_names, self.test_streams)

    def test_default_fields_for_streams_are_replicated(self):
        expected_rep_keys = self.get_select_by_default_fields(self.found_catalogs, self.conn_id)
        for stream in self.test_streams:
            with self.subTest(stream=stream):
               # gather results
                fields_replicated = self.actual_fields.get(stream, set())
                # verify that all fields are sent to the target
                # test the combination of all records
                self.assertSetEqual(fields_replicated, expected_rep_keys[stream],
                                    logging=f"verify all fields are replicated for stream {stream}")
