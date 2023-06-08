"""
Test that with no fields selected for a stream automatic fields and custom fields  are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from sfbase import SFBaseTest


class SFAllFieldsTest(AllFieldsTest, SFBaseTest):
    """Test that with no fields selected for a stream automatic fields and custom fields are still replicated"""

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_sf_all_fields_test"

    def streams_to_test(self):
        streams = {'Account', 'Contact'}
        return streams

