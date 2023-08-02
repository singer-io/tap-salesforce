import math
import unittest
from datetime import datetime, timedelta

from tap_tester import menagerie, connections, LOGGER

from base import SalesforceBaseTest


class SalesforceSyncCanary(SalesforceBaseTest):
    """
    Run the tap in discovery mode, select all tables/fields, and run a short timespan sync of
    all objects to root out any potential issues syncing some objects.
    """

    @staticmethod
    def name():
        return "tap_tester_salesforce_unsupported_objects"

    @staticmethod
    def get_properties():  # pylint: disable=arguments-differ
        return {
            'start_date' : (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%dT00:00:00Z"),
            'instance_url': 'https://singer2-dev-ed.my.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': 'BULK',
            'is_sandbox': 'false'
        }

    def expected_sync_streams(self):
        return self.expected_streams().difference({
            # DATACLOUD_API_DISABLED_EXCEPTION
            'DatacloudAddress',
            'DatacloudCompany',
            'DatacloudContact',
            'DatacloudDandBCompany',
            'DatacloudOwnedEntity',
            'DatacloudPurchaseUsage',
        })


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # partition the found catalogs into 7 groups, 1 for each day of the week to save on both
        #  time and API quota
        weekday = datetime.weekday(datetime.now())  # weekdays 0-6, Mon-Sun
        partition_size = math.ceil(len(found_catalogs)/7)
        # buffer each side of the slice to account for dynamic stream discovery
        start_of_slice = max(partition_size * weekday - 10, 0)
        end_of_slice = min(partition_size * (weekday + 1) + 10, len(found_catalogs) + 1)
        LOGGER.info("Using weekday based subset of found_catalogs, weekday = %s", weekday)

        #select certain... catalogs
        expected_streams = self.expected_sync_streams()
        allowed_catalogs = [catalog
                            for catalog in found_catalogs[start_of_slice:end_of_slice]
                            if not self.is_unsupported_by_bulk_api(catalog['stream_name']) and
                            catalog['stream_name'] in expected_streams]

        self.select_all_streams_and_fields(conn_id, allowed_catalogs)

        # Run sync
        menagerie.set_state(conn_id, {})
        _ = self.run_and_verify_sync(conn_id)
