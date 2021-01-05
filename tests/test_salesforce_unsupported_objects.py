import os
import unittest
from datetime import (datetime, timedelta)
from functools import reduce
from singer import metadata

from base import SalesforceBaseTest


class SalesforceUnsupportedObjects(SalesforceBaseTest):
    """
    Run the tap in discovery mode, select all tables/fields, and run a short timespan sync of
    all objects to root out any potential issues syncing some objects.
    """

    def name(self):
        return "tap_tester_salesforce_unsupported_objects"

    def get_properties(self):
        return {
            'start_date' : (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%dT00:00:00Z"),
            'instance_url': 'https://na73.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': 'BULK'
        }

    def perform_field_selection(self, conn_id, catalog):
        schema = menagerie.select_catalog(conn_id, catalog)

        annotated = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
        mdata = metadata.to_map(annotated['metadata'])

        replication_key = (metadata.get(mdata, (), 'valid-replication-keys') or [''])[0]

        if replication_key:
            replication_method = 'INCREMENTAL'
        else:
            replication_method = 'FULL_TABLE'

        return {'key_properties':     catalog.get('key_properties'),
                'schema':             schema,
                'tap_stream_id':      catalog.get('tap_stream_id'),
                'replication_method': replication_method,
                'replication_key':    replication_key or None}

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

        #select certain... catalogs
        # TODO: This might need to exclude Datacloud objects. So we don't blow up on permissions issues
        allowed_catalogs = [c for c in found_catalogs if not c['stream'].startswith('Datacloud')]

        for c in allowed_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            if metadata.get(c_metadata, (), 'valid-replication-keys') is None:
                replication_key = None
            else:
                replication_key = (metadata.get(c_metadata, (), 'valid-replication-keys'))[0]

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
