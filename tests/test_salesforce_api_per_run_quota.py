from sfbase import SFBaseTest
from tap_tester import connections, runner, menagerie
from tap_tester.logger import LOGGER


class SFAPIQuota(SFBaseTest):
    """
    https://jira.talendforge.org/browse/TDL-23431
    This testcase makes sure we are able to configure the tap with specific 
    per_run quota for apis and verifies if tap is working as per the 
    api quota limit set.
    """

    """
    Set a start date well past so that we have enough data to sync to hit the limit
    set a low quota to make the sync fail
    some streams are excluded as they don't have any data 
    """

    start_date = '2000-11-23T00:00:00Z'
    per_run_quota = '1'
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
        return "tt_sf_api_quota"

    def streams_to_test(self):
        return self.expected_stream_names().difference(self.streams_to_exclude)

    @staticmethod
    def streams_to_selected_fields():
        # Select all fields in the selected catalogs (streams)
        return {}

    def test_api_per_run_quota(self):
        """
        Run the tap in check mode and verify it returns the error for quota limit reached. If not, proceed to sync mode.
        For the sync mode, we have a higher total quota set, so it is unlikely to hit the total quota. Noticed that per run quota limit reaches only during the sync mode.
        """
        expected_per_run_error = "Terminating replication due to allotted quota"
        expected_total_quota_error = "Terminating replication to not continue past configured percentage"
        conn_id = connections.ensure_connection(self)

        # Run a check job using orchestrator (discovery)
        try:
             found_catalogs = self.run_and_verify_check_mode(conn_id)
        except Exception as ex:
            self.assertIn(expected_per_run_error, str(ex.exception))

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in self.streams_to_test()]

        # non_selected_fields are none
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        with self.assertRaises(Exception) as ex:
             record_count_by_stream = self.run_and_verify_sync_mode(conn_id)

        self.assertIn(expected_per_run_error, str(ex.exception))

