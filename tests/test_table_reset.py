from sfbase import SFBaseTest
from tap_tester.base_suite_tests.table_reset_test import TableResetTest


class SFTableResetTest(TableResetTest, SFBaseTest):
    """tap-salesforce Table reset test implementation"""

    salesforce_api = 'BULK'
    reset_stream = 'User'
    replication_method = "INCREMENTAL"

    @staticmethod
    def name():
        return "tt_sf_table_reset"

    def streams_to_test(self):
        return ({'Account', 'Contact', 'User'})

    @staticmethod
    def convert_state_to_utc(date_str):
        """
        Convert a saved bookmark value of form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")
