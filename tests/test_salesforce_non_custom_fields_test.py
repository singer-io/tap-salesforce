"""
Test that with no fields selected for a stream automatic fields and non custom fields  are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFNonCustomFieldsTest(AllFieldsTest, SFBaseTest):
    """Test that with no fields selected for a stream automatic fields and non custom fields are still replicated
    TODO
    This test coveres only BULK api and just couple of streams. We have separate cards to cover these cases
    TDL-23653: [tap-salesforce]: QA - Add all the streams for the all_fields test
    TDL-23654: [tap-salesforce]: QA - Add all-fields testcase for REST API streams
    """

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_non_custom_fields_test"

    def streams_to_test(self):
        streams = {'Account', 'Contact'}
        return streams

    def test_non_custom_fields( self ):
        expected_streams = self.selected_fields
        expected_fields = set()
        for stream in self.streams_to_test():
            for value in expected_streams.get(stream, set()):
                if not value.endswith("__c"):
                    expected_fields.add(value)

        actual = set()
        for stream in self.streams_to_test():
            for value in self.actual_fields.get(stream, set()):
                if not value.endswith("__c"):
                    actual.add(value)
 
        # verify that non custom fields are replicated after the sync
        self.assertSetEqual(expected_fields, actual,
                                    logging=f"verify all non custom fields are replicated for stream {stream}")
