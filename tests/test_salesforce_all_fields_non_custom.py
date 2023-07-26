"""
Test that with only non-custom fields selected for a stream automatic fields and non custom fields  are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFNonCustomFieldsTest(AllFieldsTest, SFBaseTest):
    """Test that with only non-custom fields selected for a stream automatic fields and non custom fields are still replicated
    TODO
    This test coveres only BULK api and just couple of streams. We have separate cards to cover these cases
    TDL-23653: [tap-salesforce]: QA - Add all the streams for the all_fields test
    TDL-23654: [tap-salesforce]: QA - Add all-fields testcase for REST API streams
    """

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_all_fields_non_custom"

    def streams_to_test(self):
        streams = {'Account', 'Contact'}
        return streams

    def test_non_custom_fields( self ):
        for stream in self.streams_to_selected_fields():
            expected_non_custom_fields = self.streams_to_selected_fields().get(stream, set() )
            replicated_non_custom_fields = self.actual_fields.get(stream, set() )

            self.assertIsNotNone( replicated_non_custom_fields, msg = f"Replication didn't return any non-custom fields for stream {stream}" )
            self.assertSetEqual(expected_non_custom_fields, replicated_non_custom_fields,
                 logging=f"verify all non custom fields are replicated for stream {stream}")

    @staticmethod
    def streams_to_selected_fields():
        return SFBaseTest.non_custom_fields

    def test_no_unexpected_streams_replicated(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return

    def test_all_streams_sync_records(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return

    def test_all_fields_for_streams_are_replicated(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return
