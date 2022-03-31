import unittest
from unittest import mock
import tap_salesforce

# mock 'Salesforce' class
class MockSalesforce:
    rest_requests_attempted = 0
    jobs_completed = 0
    login_timer = None
    def __init__(self, *args, **kwargs):
        return None

    def login(self):
        pass

# mock args and return desired state, catalog and config file
class MockParseArgs:
    config = {}
    discover = None
    properties = None
    state = None
    def __init__(self, config, discover, properties, state):
        self.config = config
        self.discover = discover
        self.properties = properties
        self.state = state

# send args
def get_args(config, discover=None, properties=None, state=None):
    return MockParseArgs(config, discover, properties, state)

class TestLookbackWindow(unittest.TestCase):
    """
        Test cases to verify the lookback window seconds are subtracted from the start date.
    """

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_start_date_lookback_window_default(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify default lookback window (10 seconds) are subtracted from the start date.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce
        # mock config
        mock_config = {
            'refresh_token': 'test_refresh_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'start_date': '2021-01-02T00:00:00Z',
            'api_type': 'REST',
            'select_fields_by_default': 'true'
        }
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        # get start date argument when initializing the class
        actual_adjusted_start_date = kwargs.get('default_start_date')

        # verify 10 seconds are subtracted from start date
        self.assertEqual(actual_adjusted_start_date, '2021-01-01T23:59:50Z')

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_start_date_lookback_window_desired_window(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify user defined lookback window seconds are subtracted from the start date.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce
        # mock config
        mock_config = {
            'refresh_token': 'test_refresh_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'start_date': '2021-01-02T00:00:00Z',
            'api_type': 'REST',
            'select_fields_by_default': 'true',
            'lookback_window': 20
        }
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        # get start date argument when initializing the class
        actual_adjusted_start_date = kwargs.get('default_start_date')

        # verify 20 seconds are subtracted from start date
        self.assertEqual(actual_adjusted_start_date, '2021-01-01T23:59:40Z')

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_salesforce.do_sync')
    def test_state_file_lookback_window_default(self, mocked_do_sync, mocked_parse_args, mocked_Salesforce_class):
        """
            Test cases to verify the lookback window seconds are subtracted from the state file dates.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce
        # mock config
        mock_config = {
            'refresh_token': 'test_refresh_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'start_date': '2021-01-02T00:00:00Z',
            'api_type': 'REST',
            'select_fields_by_default': 'true'
        }
        # mock catalog
        mock_catalog = {
            'streams': [
                {
                    'tap_stream_id': 'Test',
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'replication-method': 'INCREMENTAL',
                                'replication-key': 'SystemModstamp'
                            }
                        }
                    ],
                }
            ]
        }
        # mock state
        mock_state = {
            'bookmarks': {
                'Test': {
                    'JobID': 123,
                    'BatchIDs': [],
                    'version': 123,
                    'SystemModstamp': '2021-01-10T00:00:00.000000Z',
                    'JobHighestBookmarkSeen': '2021-01-10T00:00:00.000000Z'
                }
            }
        }
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, properties=mock_catalog, state=mock_state)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_do_sync.call_args
        # get state value (2nd argument)
        state = args[2]

        # get replication key value
        actual_adjusted_replication_key_date = state.get('bookmarks').get('Test').get('SystemModstamp')
        # get 'JobHighestBookmarkSeen' value
        actual_adjusted_JobHighestBookmarkSeen = state.get('bookmarks').get('Test').get('JobHighestBookmarkSeen')

        # verify 10 seconds are subtracted from both values
        self.assertEqual(actual_adjusted_replication_key_date, '2021-01-09T23:59:50.000000Z')
        self.assertEqual(actual_adjusted_JobHighestBookmarkSeen, '2021-01-09T23:59:50.000000Z')

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_salesforce.do_sync')
    def test_state_file_lookback_window_desired_window(self, mocked_do_sync, mocked_parse_args, mocked_Salesforce_class):
        """
            Test cases to verify user defined lookback window seconds are subtracted from the state file dates.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce
        # mock config
        mock_config = {
            'refresh_token': 'test_refresh_token',
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'start_date': '2021-01-02T00:00:00Z',
            'api_type': 'REST',
            'select_fields_by_default': 'true',
            'lookback_window': 20
        }
        # mock catalog
        mock_catalog = {
            'streams': [
                {
                    'tap_stream_id': 'Test',
                    'metadata': [
                        {
                            'breadcrumb': [],
                            'metadata': {
                                'replication-method': 'INCREMENTAL',
                                'replication-key': 'SystemModstamp'
                            }
                        }
                    ],
                }
            ]
        }
        # mock state
        mock_state = {
            'bookmarks': {
                'Test': {
                    'JobID': 123,
                    'BatchIDs': [],
                    'version': 123,
                    'SystemModstamp': '2021-01-10T00:00:00.000000Z',
                    'JobHighestBookmarkSeen': '2021-01-10T00:00:00.000000Z'
                }
            }
        }
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, properties=mock_catalog, state=mock_state)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_do_sync.call_args
        # get state value (2nd argument)
        state = args[2]

        # get replication key value
        actual_adjusted_replication_key_date = state.get('bookmarks').get('Test').get('SystemModstamp')
        # get 'JobHighestBookmarkSeen' value
        actual_adjusted_JobHighestBookmarkSeen = state.get('bookmarks').get('Test').get('JobHighestBookmarkSeen')

        # verify 10 seconds are subtracted from both values
        self.assertEqual(actual_adjusted_replication_key_date, '2021-01-09T23:59:50.000000Z')
        self.assertEqual(actual_adjusted_JobHighestBookmarkSeen, '2021-01-09T23:59:50.000000Z')
