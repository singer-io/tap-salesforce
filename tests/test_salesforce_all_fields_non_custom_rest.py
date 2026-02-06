"""
Test that with only non-custom fields selected for a stream automatic fields and non custom fields  are still replicated
"""

from sfbase import SFBaseTest
from tap_tester import LOGGER, menagerie, runner
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class SFNonCustomFieldsTestRest(AllFieldsTest, SFBaseTest):
    salesforce_api = "REST"

    @staticmethod
    def name():
        return "tt_sf_all_fields_non_custom_rest"

    def streams_to_test(self):
        return {
            "Case",
            "PricebookEntry",
            "Profile",
            "PermissionSet",
            "Product2",
            "PromptAction",
        }

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
        self.assertGreater(
            len(found_catalogs), 0, logging="A catalog was produced by discovery."
        )

        # TODO do we want this?
        # Verify the expected streams are present in the catalog
        found_stream_names = {catalog["stream_name"] for catalog in found_catalogs}
        self.assertTrue(
            self.expected_stream_names().issubset(found_stream_names),
            logging="Expected streams are present in catalog.",
        )

        return found_catalogs

    def streams_to_selected_fields(self):
        found_catalogs = AllFieldsTest.found_catalogs
        conn_id = AllFieldsTest.conn_id
        non_custom_fields = self.get_non_custom_fields(found_catalogs, conn_id)
        return non_custom_fields

    def test_non_custom_fields(self):
        excluded_fields = {"MlFeatureValueMetric"}
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                found_catalog_names = {
                    catalog["tap_stream_id"] for catalog in AllFieldsTest.found_catalogs
                }
                self.assertTrue(self.streams_to_test().issubset(found_catalog_names))
                LOGGER.info("discovered schemas are OK")
                expected_non_custom_fields = (
                    self.selected_fields.get(stream, set()) - excluded_fields
                )
                replicated_non_custom_fields = self.actual_fields.get(stream, set())
                # Verify at least one non-custom field is replicated
                self.assertGreater(
                    len(replicated_non_custom_fields),
                    0,
                    msg=f"Replication didn't return any non-custom fields for stream {stream}",
                )

                # verify that all the non_custom fields are replicated
                self.assertEqual(
                    replicated_non_custom_fields,
                    expected_non_custom_fields,
                    msg=f"All non_custom fields are not no replicated for stream {stream}",
                )

                # verify that automatic fields are also replicated along with non_custom_fields
                self.assertTrue(
                    self.expected_automatic_fields(stream).issubset(
                        replicated_non_custom_fields
                    ),
                    msg=f"Automatic fields are not replicated for stream {stream}",
                )

                # Verify custom fields are not replicated by checking the field name
                num_custom, _ = self.count_custom_non_custom_fields(
                    replicated_non_custom_fields
                )
                self.assertEqual(
                    num_custom,
                    0,
                    msg=f"Replicated some fields that are custom fields for stream {stream}",
                )
