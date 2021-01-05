import os
import unittest
from functools import reduce
from singer import metadata

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

class SalesforceBookmarks(unittest.TestCase):

    def name(self):
        return "tap_tester_salesforce_bookmarks"

    def tap_name(self):
        return "tap-salesforce"

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_SALESFORCE_CLIENT_ID'),
                                    os.getenv('TAP_SALESFORCE_CLIENT_SECRET'),
                                    os.getenv('TAP_SALESFORCE_REFRESH_TOKEN')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_SALESFORCE_CLIENT_ID, TAP_SALESFORCE_CLIENT_SECRET, TAP_SALESFORCE_REFRESH_TOKEN")

    def get_type(self):
        return "platform.salesforce"

    def get_credentials(self):
        return {'refresh_token': os.getenv('TAP_SALESFORCE_REFRESH_TOKEN'),
                'client_id': os.getenv('TAP_SALESFORCE_CLIENT_ID'),
                'client_secret': os.getenv('TAP_SALESFORCE_CLIENT_SECRET')}

    def expected_check_streams(self):
        return {
            'Account', # General Object Type
            'Task', # New fields under this type (TaskSubType)
            'Attachment' # contains base64 field
        }

    def expected_sync_streams(self):
        return {
            'Account'
        }

    def expected_pks(self):
        return {
            'Account': {"Id"}
        }

    def get_properties(self):
       return {
            'start_date' : '2017-01-01 00:00:00',
            'instance_url': 'https://cs95.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': "bulk",
            'is_sandbox': 'true'
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
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        #select certain... catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            replication_key = (metadata.get(c_metadata, (), 'valid-replication-keys') or [])[0]
            if replication_key:
                replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': replication_key, "replication-method" : "INCREMENTAL", "selected" : True}}]
            else:
                replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': None, "replication-method" : "FULL_TABLE", "selected" : True}}]

            connections.set_non_discoverable_metadata(conn_id, c, menagerie.get_annotated_schema(conn_id, c['stream_id']), replication_md)


        # Run sync
        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # verify the last found value was bookmarked
        states = menagerie.get_state(conn_id)["bookmarks"]

        found_catalogs = menagerie.get_catalogs(conn_id)
        #select certain... catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        for catalog in our_catalogs:
            stream_name = catalog['tap_stream_id']
            missing_bookmarks = []
            if states[stream_name]:
                bm_value = states[stream_name].get(catalog['metadata'].get('replication-key'))
                self.assertTrue(bm_value, msg="Invalid bookmark for {}: '{}'".format(stream_name, bm_value))
            else:
                missing_bookmarks.add(stream_name)

        if len(missing_bookmarks) != 0:
            self.fail("No bookmark found for: {}".format(missing_bookmarks))
