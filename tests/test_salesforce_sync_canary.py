from datetime import datetime, timedelta

from tap_tester import menagerie, connections

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
            'is_sandbox': 'true'
        }

    def expected_sync_streams(self):
        return self.expected_streams().difference({
            'ConnectedApplication',  # INSUFFICIENT_ACCESS
            'FeedAttachment',  # MALFORMED_QUERY must be admin to query
            'FeedComment',  # MALFORMED_QUERY
            'FeedRevision',  # MALFORMED_QUERY
            'FeedItem',  # MALFORMED_QUERY
            'EntitySubscription',  # MALFORMED_QUERY
            'ForecastingQuota',  # INSUFFICIENT_ACCESS
            'DatacloudAddress',  # EXTERNAL_OBJECT_EXCEPTION
            'TopicAssignment',  # Invalid Batch
        })
    @unittest.skip("SKIPPING TESTS UNTIL NEW TEST INSTANCE IS AVAILABLE")
    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        #select certain... catalogs
        # TODO: This might need to exclude Datacloud objects. So we don't blow up on permissions issues
        expected_streams = self.expected_sync_streams()
        allowed_catalogs = [catalog
                            for catalog in found_catalogs
                            if not self.is_unsupported_by_bulk_api(catalog['stream_name']) and
                            catalog['stream_name'] in expected_streams]

        self.select_all_streams_and_fields(conn_id, allowed_catalogs)

        # Run sync
        menagerie.set_state(conn_id, {})
        _ = self.run_and_verify_sync(conn_id)
