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
            actual_custom_fields = self.actual_fields.get(stream, set() )

            self.assertIsNotNone( actual_custom_fields, msg = f"Replication didn't return any custom fields for stream {stream}" )
            #Exclude automatic fields from the assertion
            automatic_fields = self.expected_automatic_fields ( stream )

            self.assertSetEqual(expected_custom_fields, actual_custom_fields.difference(automatic_fields),
                 logging=f"verify all custom fields are replicated for stream {stream}")

    #Override this method to exclude automatic fields in the assertion
    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                # gather expectations
                expected_all_keys = self.selected_fields.get(stream, set())

                # gather results
                fields_replicated = self.actual_fields.get(stream, set())
                automatic_fields = self.expected_automatic_fields ( stream )

                # verify that all fields are sent to the target
                # test the combination of all records
                self.assertSetEqual(fields_replicated.difference(automatic_fields), expected_all_keys,
                    logging=f"verify all fields are replicated for stream {stream}")
