from sfbase import SFBaseTest
from tap_tester.base_suite_tests.table_reset_test import TableResetTest


class SFTableResetTest(TableResetTest, SFBaseTest):
    """tap-salesforce Table reset test implementation"""

    reset_stream = 'User'

    @staticmethod
    def name():
        return "tt_sf_table_reset"

    def streams_to_test(self):
        return ({'Account', 'Contact', 'User'})
