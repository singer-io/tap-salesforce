import os
import unittest
from functools import reduce
from singer import metadata
from copy import deepcopy

from base import SalesforceBaseTest


class SalesforceActivateVersionMessages(SalesforceBaseTest):
    def name(self):
        return "tap_tester_salesforce_activate_version_messages"

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'instance_url': 'https://cs95.salesforce.com',
            'quota_percent_total': "80",
            'select_fields_by_default': 'false',
            'api_type': "BULK",
            'is_sandbox': 'true'
        }

    def toggle_replication_method(self, conn_id, catalog, replication_key=None):
        if catalog.get('replication_key'):
            catalog['replication_key'] = None
            catalog['replication_method'] = 'FULL_TABLE'
        else:
            catalog['replication_key'] = replication_key
            catalog['replication_method'] = 'INCREMENTAL'

        return catalog

    def expected_check_streams(self):
        return {
            'Account',
        }

    def expected_sync_streams(self):
        return {
            'Account'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        #verify (subset of) schemas discovered?
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs),
                           0,
                           msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        account_catalog = [c for c in found_catalogs if c['stream_name'] == 'Account'][0]
        account_annotated = menagerie.get_annotated_schema(conn_id, account_catalog['stream_id'])
        account_metadata = metadata.to_map(account_annotated['metadata'])

        #discovered PK's are placed into metadata
        self.assertEqual(account_catalog.get('key_properties',[]), [])
        self.assertEqual(account_metadata.get(()).get('table-key-properties'), ['Id'])

        replication_key = metadata.get(account_metadata, (), 'valid-replication-keys')[0]

        replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': replication_key, "replication-method" : "INCREMENTAL", "selected" : True}}]
        connections.set_non_discoverable_metadata(conn_id, account_catalog, menagerie.get_annotated_schema(conn_id, account_catalog['stream_id']), replication_md)

        # TODO:
        # 1. run with replication_key and no state (incremental)
        #    - should emit activate version message at beginning
        # 2. run a second incremental
        #    - should emit activate version message at beginning
        #    - version should not change
        # 3. switch to full table (remove replication_key)
        #    - should emit activate version message at beginning with new version
        # 4. start a new full table
        #    - should emit activate version message at end with new version

        # Run sync
        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify rows were synced
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_primary_keys())

        replicated_row_count = reduce(lambda accum, c: accum + c, record_count_by_stream.values())

        self.assertGreater(replicated_row_count,
                           0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream))

        print("total replicated row count: {}".format(replicated_row_count))

        records_by_stream = runner.get_records_from_target_output()

        initial_incremental_records = records_by_stream['Account']
        initial_incremental_message = initial_incremental_records['messages'][0]

        self.assertEqual(initial_incremental_message['action'],
                         'activate_version',
                         msg="Expected `activate_version` message to be sent prior to records for incremental sync")

        incremental_version = initial_incremental_records['table_version']

        #run again in full table mode
        replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': None, "replication-method" : "FULL_TABLE", "selected" : True}}]
        connections.set_non_discoverable_metadata(conn_id, account_catalog, menagerie.get_annotated_schema(conn_id, account_catalog['stream_id']), replication_md)


        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream = runner.get_records_from_target_output()
        initial_full_table_records = records_by_stream['Account']
        initial_full_table_message = initial_full_table_records['messages'][0]

        initial_full_table_version = initial_full_table_records['table_version']

        self.assertEqual(initial_full_table_message['action'],
                         'activate_version',
                         msg="Expected `activate_version` message to be sent prior to records for initial full table sync")

        self.assertNotEqual(initial_full_table_version,
                            incremental_version,
                            msg="Expected version for stream Account to be change after switching to full table")

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        records_by_stream = runner.get_records_from_target_output()
        final_full_table_records = records_by_stream['Account']

        initial_full_table_message = final_full_table_records['messages'][0]
        final_full_table_message = final_full_table_records['messages'][-1]

        final_full_table_version = final_full_table_records['table_version']

        self.assertEqual(final_full_table_message['action'],
                         'activate_version',
                         msg="Expected `activate_version` message to be sent after records for subsequent full table syncs")

        self.assertNotEqual(final_full_table_version,
                            initial_full_table_version,
                            msg="Expected version for stream Account to be change after switching to full table")

        self.assertEqual(['Id'], records_by_stream['Account']['key_names'])
