from tap_salesforce.salesforce import Salesforce
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
        Test cases to verify the lookback window seconds are subtracted from the start date or state file date.
    """

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_default_lookback_window(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify default lookback window (10 seconds) is passed if user has not passed from the config.
        """

        tap_salesforce.CONFIG = {
            'refresh_token': None,
            'client_id': None,
            'client_secret': None,
            'start_date': None
        }
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
        # get lookback_window argument when initializing the class
        actual_lookback_window = kwargs.get('lookback_window')

        # verify 10 seconds was passed as lookback_window
        self.assertEqual(actual_lookback_window, 10)

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_desired_lookback_window(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify user defined lookback window is set when is passed from the config.
        """

        tap_salesforce.CONFIG = {
            'refresh_token': None,
            'client_id': None,
            'client_secret': None,
            'start_date': None
        }
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
        # get lookback_window argument when initializing the class
        actual_lookback_window = kwargs.get('lookback_window')

        # verify 20 seconds was passed as lookback_window
        self.assertEqual(actual_lookback_window, 20)

    def test_default_lookback_window__get_start_date(self):
        """
            Test case to verify 10 seconds are not subtracted from the start date if state and lookback_window is not passed
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z'
        }
        # mock catalog entry
        mock_catalog_entry = {
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

        # create Salesforce object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'True'
        start_date = sf.get_start_date({}, mock_catalog_entry, without_lookback=False)

        # verify the start date is not altered as state is not passed
        self.assertEqual(start_date, '2021-01-02T00:00:00Z')

    def test_desired_lookback_window__get_start_date(self):
        """
            Test case to verify user defined lookback window seconds are not subtracted from the start date if state is not passed
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z',
            'lookback_window': 20
        }
        # mock catalog entry
        mock_catalog_entry = {
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

        # create 'Salesforce' object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'True'
        start_date = sf.get_start_date({}, mock_catalog_entry, without_lookback=False)

        # verify the start date is not altered as state is not passed
        self.assertEqual(start_date, '2021-01-02T00:00:00Z')

    def test_default_lookback_window_with_state__get_start_date(self):
        """
            Test case to verify 10 seconds are subtracted from the state file date if lookback_window is not passed
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z'
        }
        # mock catalog entry
        mock_catalog_entry = {
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
        # mock state
        mock_state = {
            'bookmarks': {
                'Test': {
                    'version': 123,
                    'SystemModstamp': '2021-01-10T00:00:00.000000Z',
                }
            }
        }

        # create 'Salesforce' object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'True'
        start_date = sf.get_start_date(mock_state, mock_catalog_entry, without_lookback=False)

        # verify 10 seconds were subtracted from state file date
        self.assertEqual(start_date, '2021-01-09T23:59:50.000000Z')

    def test_desired_lookback_window_with_state__get_start_date(self):
        """
            Test case to verify used defined lookback window seconds are subtracted from the state file date if lookback_window is passed
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z',
            'lookback_window': 20
        }
        # mock catalog entry
        mock_catalog_entry = {
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
        # mock state
        mock_state = {
            'bookmarks': {
                'Test': {
                    'version': 123,
                    'SystemModstamp': '2021-01-10T00:00:00.000000Z',
                }
            }
        }

        # create 'Salesforce' object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'True'
        start_date = sf.get_start_date(mock_state, mock_catalog_entry, without_lookback=False)

        # verify 20 seconds were subtracted from state file date
        self.assertEqual(start_date, '2021-01-09T23:59:40.000000Z')

    def test_no_lookback_window_subtraction__get_start_date(self):
        """
            Test case to verify start date is not changed when we pass 'with_lookback=False'
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z'
        }
        # mock catalog entry
        mock_catalog_entry = {
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

        # create 'Salesforce' object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'False'
        start_date = sf.get_start_date({}, mock_catalog_entry)

        # verify 20 seconds were subtracted from start date
        self.assertEqual(start_date, '2021-01-02T00:00:00Z')

    def test_no_lookback_window_subtraction_with_state__get_start_date(self):
        """
            Test case to verify state file date is not changed when we pass 'with_lookback=False'
        """

        # mock config
        config = {
            'start_date': '2021-01-02T00:00:00Z'
        }
        # mock catalog entry
        mock_catalog_entry = {
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
        # mock state
        mock_state = {
            'bookmarks': {
                'Test': {
                    'version': 123,
                    'SystemModstamp': '2021-01-10T00:00:00.000000Z',
                }
            }
        }

        # create 'Salesforce' object
        sf = Salesforce(
            refresh_token='test_refresh_token',
            sf_client_id='test_client_id',
            sf_client_secret='test_client_secret',
            default_start_date=config.get('start_date'),
            lookback_window=int(config.get('lookback_window', 10)))

        # function call with apply lookback as 'False'
        start_date = sf.get_start_date(mock_state, mock_catalog_entry)

        # verify 20 seconds were subtracted from state file date
        self.assertEqual(start_date, '2021-01-10T00:00:00.000000Z')
