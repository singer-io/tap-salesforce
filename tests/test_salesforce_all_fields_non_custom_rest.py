"""
Test that with only non-custom fields selected for a stream automatic fields and non custom fields  are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFNonCustomFieldsTestRest(AllFieldsTest, SFBaseTest):

    salesforce_api = 'REST'

    @staticmethod
    def name():
        return "tt_sf_all_fields_non_custom_rest"

    def streams_to_test(self):
        return {
            'Case',
            'PricebookEntry',
            'Profile',
            'PermissionSet',
            'Product2',
            'PromptAction',
        }

    def streams_to_selected_fields(self):
        found_catalogs = AllFieldsTest.found_catalogs
        conn_id = AllFieldsTest.conn_id
        non_custom_fields = self.get_non_custom_fields(found_catalogs, conn_id)
        return non_custom_fields

    def test_non_custom_fields(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                expected_non_custom_fields = self.selected_fields.get(stream,set())
                replicated_non_custom_fields = self.actual_fields.get(stream, set())
                #Verify at least one non-custom field is replicated
                self.assertGreater(len(replicated_non_custom_fields),0,
                                     msg = f"Replication didn't return any non-custom fields for stream {stream}")

                #verify that all the non_custom fields are replicated
                self.assertEqual(replicated_non_custom_fields, expected_non_custom_fields,
                                 msg = f"All non_custom fields are not no replicated for stream {stream}")

                #verify that automatic fields are also replicated along with non_custom_fields
                self.assertTrue(self.expected_automatic_fields(stream).issubset(replicated_non_custom_fields),
                                msg = f"Automatic fields are not replicated for stream {stream}")

                #Verify custom fields are not replicated by checking the field name
                num_custom, _ = self.count_custom_non_custom_fields(replicated_non_custom_fields)
                self.assertEqual(num_custom, 0,
                                 msg = f"Replicated some fields that are custom fields for stream {stream}")

