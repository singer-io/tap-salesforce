from sfbase import SFBaseTest
from tap_tester import runner, menagerie, LOGGER, connections


class SFAPIQuota(SFBaseTest):
    """
    https://jira.talendforge.org/browse/TDL-23431
    This testcase makes sure we are able to configure the tap with specific
    total quota for apis and verifies if tap is working as per the
    api quota limit set.
    """

    """
    Set a start date well past so that we have enough data to sync to hit the limit
    set a low quota to make the sync fail
    some streams are excluded as they don't have any data
    """

    start_date = '2000-11-23T00:00:00Z'
    total_quota = '1'
    streams_to_exclude = {
             'DatacloudAddress',
             'DatacloudCompany',
             'DatacloudContact',
             'DatacloudDandBCompany',
             'DatacloudOwnedEntity',
             'DatacloudPurchaseUsage',
             'FieldSecurityClassification',
             'ServiceAppointmentStatus',
             'WorkOrderLineItemStatus',
             'WorkOrderStatus',
             'ShiftStatus',
             'WorkStepStatus',
        }

    @staticmethod
    def name():
        return "tt_sf_api_quota_total"

    def streams_to_test(self):
        return self.expected_stream_names().difference(self.streams_to_exclude)

    def test_api_total_quota(self):
        """
        Run the tap in check mode and verify it returns the error for quota limit reached. 
        """
        expected_total_quota_error = "Terminating replication to not continue past configured percentage of 1.0% total quota"

        conn_id = connections.ensure_connection(self)

        # Run a check job using orchestrator (discovery)
        with self.assertRaises(Exception) as ex:
             check_job_name = self.run_and_verify_check_mode(conn_id)

        self.assertIn(expected_total_quota_error, str(ex.exception))

