"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
import copy
from tap_tester.base_suite_tests.api_quota_test import APIQuotaTest
from sfbase import SFBaseTest
from tap_tester import runner, menagerie


class SFAPIQuota(APIQuotaTest, SFBaseTest):
    """Test that with only custom fields selected for a stream automatic fields and custom fields are still replicated
    TODO
    This test coveres only BULK api and just couple of streams. We have separate cards to cover these cases
    TDL-23653: [tap-salesforce]: QA - Add all the streams for the all_fields test
    TDL-23654: [tap-salesforce]: QA - Add all-fields testcase for REST API streams
    """

    salesforce_api = 'BULK'
    expected_error = "Terminating replication due to allotted quota of 1.0% per replication"

    @staticmethod
    def name():
        return "tt_sf_api_quota"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2000-11-23T00:00:00Z',
            'instance_url': 'https://singer2-dev-ed.my.salesforce.com',
            'select_fields_by_default': 'true',
            'quota_percent_total': '85',
            'quota_percent_per_run' : '1',
            'api_type': self.salesforce_api,
            'is_sandbox': 'false'
        }
        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return return_value

    def streams_to_test(self):
        return self.expected_streams().difference( self.streams_to_exclude )

    def test_all_streams_sync_records(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                # gather results
                record_count = self.record_count_by_stream
