"""
Test that with no fields selected for a stream automatic fields and custom fields  are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFAllFieldsTest(AllFieldsTest, SFBaseTest):
    """Test that with no fields selected for a stream automatic fields and custom fields are still replicated
    TODO
    This test coveres only BULK api and just couple of streams. We have separate cards to cover these cases
    TDL-23653: [tap-salesforce]: QA - Add all the streams for the all_fields test
    TDL-23654: [tap-salesforce]: QA - Add all-fields testcase for REST API streams
    """

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_all_fields_test"

    def streams_to_test(self):
        streams = {'Account', 'Contact'}
        return streams

