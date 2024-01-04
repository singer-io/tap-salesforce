import copy
from sfbase import SFBaseTest
from tap_tester.base_suite_tests.table_reset_test import TableResetTest


class SFTableResetTest(TableResetTest, SFBaseTest):
    """tap-salesforce Table reset test implementation
    Currently tests only the stream with Incremental replication method"""

    @staticmethod
    def name():
        return "tt_sf_table_reset"

    def streams_to_test(self):
        return ({'Account', 'Contact', 'User'})

    @property
    def reset_stream(self):
        return ('User')

    def manipulate_state(self, current_state):
        # no state manipulation needed for this tap
        return current_state
