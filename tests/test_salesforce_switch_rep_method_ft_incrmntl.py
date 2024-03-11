from tap_tester import runner, menagerie, connections
from sfbase import SFBaseTest


class SFSwitchRepMethodIncrmntl(SFBaseTest):

    start_date = '2000-11-11T00:00:00Z'#to get max data available for testing
    @staticmethod
    def name():
        return "tt_sf_table_switch_rep_method_ft_incrmntl"


    def expected_sync_streams(self):
        streams = self.switchable_streams() - {'FlowDefinitionView','EntityDefinition', 'EventLogFile'}
        # Excluded the above two streams due to the bug TDL-24514
        return self.partition_streams(streams)


    def test_run(self):
        self.salesforce_api = 'BULK'

        replication_keys = self.expected_replication_keys()
        primary_keys = self.expected_primary_keys()
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
        fulltbl_sync_record_count = self.run_and_verify_sync_mode(conn_id)
        fulltbl_sync_records = runner.get_records_from_target_output()

        fulltbl_sync_bookmarks = menagerie.get_state(conn_id)

        #Switch the replication method from full table to Incremental
        streams_replication_methods = {stream: self.INCREMENTAL
                                       for stream in expected_streams}
        self.set_replication_methods(conn_id, catalog_entries, streams_replication_methods)

        # SYNC 2
        incrmntl_sync_record_count = self.run_and_verify_sync_mode(conn_id)
        incrmntl_sync_records = runner.get_records_from_target_output()
        incrmntl_sync_bookmarks = menagerie.get_state(conn_id)

        # Test by stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # record counts
                fulltbl_sync_count = fulltbl_sync_record_count.get(stream, 0)
                incrmntl_sync_count = incrmntl_sync_record_count.get(stream, 0)
                replication_key = list(replication_keys[stream])[0]

                # Verify at least 1 record was replicated in the Incrmental sync
                self.assertGreater(incrmntl_sync_count, 0,
                                   msg="We are not fully testing bookmarking for {}".format(stream))
                # data from record messages
                """
                If implementing in tap-tester framework the primary key implementation should account
                for compound primary keys
                """
                self.assertEqual(1, len(list(primary_keys[stream])),
                                 msg="Compound primary keys require a change to test expectations")
                primary_key = list(primary_keys[stream])[0]
                fulltbl_sync_messages = [record['data'] for record in
                                       fulltbl_sync_records.get(stream, {}).get('messages')
                                       if record.get('action') == 'upsert']
                filtered_fulltbl_sync_messages = [message for message in fulltbl_sync_messages
                                                  if message[replication_key] >= self.start_date]
                fulltbl_primary_keys = {message[primary_key] for message in filtered_fulltbl_sync_messages}
                incrmntl_sync_messages = [record['data'] for record in
                                        incrmntl_sync_records.get(stream, {}).get('messages')
                                        if record.get('action') == 'upsert']
                incrmntl_primary_keys = {message[primary_key] for message in incrmntl_sync_messages}

                #Verify all records are synced in the second sync
                self.assertTrue(fulltbl_primary_keys.issubset(incrmntl_primary_keys))

                """
                Modify the the activate version assertion accordingly based on the outcome of BUG #TDL-24467
                if needed
                """
                #verify that the last message is not a activate version message for incremental sync
                self.assertNotEqual('activate_version', incrmntl_sync_records[stream]['messages'][-1]['action'])

                #verify that the table version incremented after every sync
                self.assertGreater(incrmntl_sync_records[stream]['table_version'],
                                    fulltbl_sync_records[stream]['table_version'],
                                    msg = "Table version is not incremented after a successful sync")

                # bookmarked states (top level objects)
                incrmntl_bookmark_key_value = incrmntl_sync_bookmarks.get('bookmarks', {}).get(stream)

                # bookmarked states (actual values)
                incrmntl_bookmark_value = incrmntl_bookmark_key_value.get(replication_key)

                # Verify the incremental sync sets a bookmark of the expected form
                self.assertIsNotNone(incrmntl_bookmark_key_value)

                #verify that bookmarks are present after switching to Incremental rep method
                self.assertIsNotNone(incrmntl_bookmark_value)
