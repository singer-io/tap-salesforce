from tap_tester.base_suite_tests.bookmark_test import BookmarkTest
from sfbase import SFBaseTest


class SFBookmarkTest(BookmarkTest, SFBaseTest):

    salesforce_api = 'BULK'
    @staticmethod
    def name():
        return "tt_sf_bookmarks"

    @staticmethod
    def streams_to_test():
        return {
            'User',
            'Publisher',
            'AppDefinition',
        }

    bookmark_format ="%Y-%m-%dT%H:%M:%S.%fZ"

    initial_bookmarks = {}
    streams_replication_method = {}
    def streams_replication_methods(self):
        streams_to_set_rep_method = [catalog['tap_stream_id'] for catalog in BookmarkTest.test_catalogs
                                 if 'forced-replication-method' not in  catalog['metadata'].keys()]
        if len(streams_to_set_rep_method) > 0:
            self.streams_replication_method = {stream: 'INCREMENTAL'
                                       for stream in streams_to_set_rep_method}
        return self.streams_replication_method

    def adjusted_expected_replication_method(self):
        streams_to_set_rep_method = [catalog['tap_stream_id'] for catalog in BookmarkTest.test_catalogs
                                 if 'forced-replication-method' not in  catalog['metadata'].keys()]
        expected_replication_methods = self.expected_replication_method()
        if self.streams_replication_method:
            for stream in streams_to_set_rep_method :
                expected_replication_methods[stream] = self.streams_replication_method[stream]
            return expected_replication_methods
        return expected_replication_methods

