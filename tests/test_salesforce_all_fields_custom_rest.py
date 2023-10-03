"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
import copy
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest
from tap_tester.logger import LOGGER


class SFCustomFieldsTest(AllFieldsTest, SFBaseTest):

    salesforce_api = 'REST'

    @staticmethod
    def name():
        return "tt_sf_all_fields_custom"


    def streams_to_test(self):
        if self.partitioned_streams:
            return self.partitioned_streams
        return self.partition_streams(self.get_streams_with_data())


    def streams_to_selected_fields(self):
        found_catalogs = AllFieldsTest.found_catalogs
        conn_id = AllFieldsTest.conn_id
        custom_fields = self.get_custom_fields(found_catalogs, conn_id)
        return custom_fields

    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.streams_to_test():

            expected_custom_fields = self.selected_fields.get(stream, set()).union(self.expected_automatic_fields(stream))
            replicated_custom_fields = self.actual_fields.get(stream, set())

            #Verify at least one custom field is replicate
            self.assertIsNotNone(replicated_custom_fields.difference(self.expected_automatic_fields(stream)),
                                 msg = f"Replication didn't return any custom fields for stream {stream}")

            #Verify that custom and automatic fields are replicated
            self.assertSetEqual(expected_custom_fields, replicated_custom_fields,
                                logging=f"verify all fields are replicated for stream {stream}")

            #Verify that only custome fields are replicated besides automatic fields
            num_custom, num_non_custom = self.count_custom_non_custom_fields(replicated_custom_fields)
            self.assertEqual(num_non_custom, len(self.expected_automatic_fields(stream)),
                             "Replicated some fields that are not custom fields for stream {stream}")
