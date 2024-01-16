"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest

class SFCustomFieldsTestRest(AllFieldsTest, SFBaseTest):

    salesforce_api = 'REST'

    @staticmethod
    def name():
        return "tt_sf_all_fields_custom_rest"

    def streams_to_test(self):
        return  self.get_custom_fields_streams()

    def streams_to_selected_fields(self):
        found_catalogs = AllFieldsTest.found_catalogs
        conn_id = AllFieldsTest.conn_id
        custom_fields = self.get_custom_fields(found_catalogs, conn_id)
        return custom_fields

    def test_all_fields_for_streams_are_replicated(self):

        selected_streams = self.streams_to_test()
        actual_custom_field_streams = {key for key in self.selected_fields.keys() if self.selected_fields.get(key,set())}
        self.assertSetEqual( selected_streams, actual_custom_field_streams,
                       msg = f"More streams have custom fields actual_custom_field_streams.diff(selected_streams)")
        for stream in selected_streams:
            with self.subTest(stream=stream):
                automatic_fields = self.expected_automatic_fields(stream)
                expected_custom_fields = self.selected_fields.get(stream, set()).union(automatic_fields)
                replicated_custom_fields = self.actual_fields.get(stream, set())

                #Verify that custom and automatic fields are replicated
                self.assertSetEqual(expected_custom_fields, replicated_custom_fields,
                                    msg = f"verify all fields are replicated for stream {stream}")

                #Verify at least one custom field is replicated
                if len(expected_custom_fields) > len(automatic_fields):
                    self.assertGreater(len(replicated_custom_fields.difference(automatic_fields)),0,
                                       msg = f"Replication didn't return any custom fields for stream {stream}")

                #Verify that only custom fields are replicated besides automatic fields
                _, num_non_custom = self.count_custom_non_custom_fields(replicated_custom_fields)
                self.assertEqual(num_non_custom, len(automatic_fields),
                                 msg = f"Replicated some fields that are not custom fields for stream {stream}")
