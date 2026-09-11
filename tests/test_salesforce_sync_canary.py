
from datetime import datetime, timedelta

from tap_tester import menagerie, connections, LOGGER

from sfbase import SFBaseTest


class SalesforceSyncCanary(SFBaseTest):
    """
    Run the tap in discovery mode, select all tables/fields, and run a short timespan sync of
    all objects to root out any potential issues syncing some objects.
    """

    @staticmethod
    def name():
        return "tt_sf_unsupported_objects"

    @staticmethod
    def get_properties():  # pylint: disable=arguments-differ
        return {
            'start_date' : '2024-03-12T00:00:00Z',
            'instance_url': 'https://singer2-dev-ed.my.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': 'BULK',
            'is_sandbox': 'false'
        }

    def expected_sync_streams(self):
        return self.expected_stream_names().difference({
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

        # select certain... catalogs
        expected_streams = self.partition_streams(self.expected_sync_streams())
        allowed_catalogs = [catalog for catalog in found_catalogs
                            if catalog['stream_name'] in expected_streams]

        self.select_all_streams_and_fields(conn_id, allowed_catalogs)
        # Run sync
        menagerie.set_state(conn_id, {})
        record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        actual_streams_with_data ={stream for stream in record_count_by_stream
                                   if record_count_by_stream[stream] > 0}
        self.assertTrue(actual_streams_with_data.issubset(self.get_streams_with_data()),
                        msg = f"New streams with data are synced {actual_streams_with_data.difference(self.get_streams_with_data())}")
