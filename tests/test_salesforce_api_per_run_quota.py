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
        return self.expected_streams().difference( self.streams_to_exclude )

    @staticmethod
    def streams_to_selected_fields():
        # Select all fields in the selected catalogs (streams)
        return {}

    def test_api_per_run_quota(self):
        """
        Run the tap in check mode and verify it returns the error for quota limit reached. If not, proceed to sync mode.
        """
        expected_per_run_error = "Terminating replication due to allotted quota"
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode( conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in self.streams_to_test()]

        # non_selected_fields are none
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        with self.assertRaises(Exception) as ex:
             record_count_by_stream = self.run_and_verify_sync_mode(conn_id)

        self.assertIn(expected_per_run_error, str(ex.exception) )

