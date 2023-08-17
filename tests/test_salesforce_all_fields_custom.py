"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
import copy
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest
from tap_tester.logger import LOGGER


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
            replicated_custom_fields = self.actual_fields.get(stream, set() ).difference(self.expected_automatic_fields(stream))

            #Verify at least one custom field is replicated
            self.assertIsNotNone( replicated_custom_fields, msg = f"Replication didn't return any custom fields for stream {stream}" )

            #Verify only custom fields are replicated by checking the field name
            self.assertTrue( self.verify_custom_fields( replicated_custom_fields ), "Replicated some fields that are not custom fields for stream {stream}" )

            """
            TODO: Add this assertion when we do  
                  TDL-23781: [tap-salesforce] QA: Get Custom fields and non-custom fields
            self.assertIsNone(replicated_custom_fields.difference(automatic_fields))
            """
