"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
import copy
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFCustomFieldsTest(AllFieldsTest, SFBaseTest):
    """Test that with only custom fields selected for a stream automatic fields and custom fields are still replicated
    TODO
    This test coveres only BULK api and just couple of streams. We have separate cards to cover these cases
    TDL-23653: [tap-salesforce]: QA - Add all the streams for the all_fields test
    TDL-23654: [tap-salesforce]: QA - Add all-fields testcase for REST API streams
    """

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_all_fields_custom"

    def streams_to_test(self):
        streams = {'Account', 'Contact'}
        return streams

    @staticmethod
    def streams_to_selected_fields():
        return SFBaseTest.custom_fields

    def test_custom_fields( self ):
        for stream in self.streams_to_selected_fields():
            expected_custom_fields = self.streams_to_selected_fields().get(stream, set() )
            actual_custom_fields = self.actual_fields.get(stream, set() )

            self.assertIsNotNone( actual_custom_fields, msg = f"Replication didn't return any custom fields for stream {stream}" )
            actual_custom_fields.remove('SystemModstamp')
            actual_custom_fields.remove('Id')

            self.assertSetEqual(expected_custom_fields, actual_custom_fields,
                 logging=f"verify all custom fields are replicated for stream {stream}")

    def test_no_unexpected_streams_replicated(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return

    def test_all_streams_sync_records(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return

    def test_all_fields_for_streams_are_replicated(self):
        """TODO - Will be addressed in TDL-23653 and TDL-23654 """
        return
