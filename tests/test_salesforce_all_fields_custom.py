"""
Test that with only custom fields selected for a stream automatic fields and custom fields  are still replicated
"""
from tap_tester import menagerie, runner
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest

class SFCustomFieldsTest(AllFieldsTest, SFBaseTest):

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_all_fields_custom"

    def streams_to_test(self):
        return  self.get_custom_fields_streams()

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the found catalogs from menagerie.
        """
        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify the catalog is not empty
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0,
                           logging="A catalog was produced by discovery.")

        # TODO do we want this?
        # Verify the expected streams are present in the catalog
        found_stream_names = {catalog['stream_name'] for catalog in found_catalogs}
        self.assertTrue(self.expected_stream_names().issubset(found_stream_names),
                        logging="Expected streams are present in catalog.")

        return found_catalogs

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

                #Verify at least one custom field is replicated if exists
                if len(expected_custom_fields) > len(automatic_fields):
                    self.assertGreater(len(replicated_custom_fields.difference(automatic_fields)),0,
                                       msg = f"Replication didn't return any custom fields for stream {stream}")

                #Verify that only custom fields are replicated besides automatic fields
                _, num_non_custom = self.count_custom_non_custom_fields(replicated_custom_fields)
                self.assertEqual(num_non_custom, len(automatic_fields),
                                 msg = f"Replicated some fields that are not custom fields for stream {stream}")
