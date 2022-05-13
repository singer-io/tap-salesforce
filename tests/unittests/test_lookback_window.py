from tap_salesforce.salesforce import Salesforce, DEFAULT_LOOKBACK_WINDOW
import unittest
from unittest import mock
import tap_salesforce

# mock 'Salesforce' class
class MockSalesforce:
    def __init__(self, *args, **kwargs):
        self.rest_requests_attempted = 0
        self.jobs_completed = 0
        self.login_timer = None

    def login(self):
        pass

# mock args and return desired state, catalog and config file
class MockParseArgs:
    def __init__(self, config):
        self.config = config
        self.discover = None
        self.properties = None
        self.state = None

# send args
def get_args(add_lookback_window=False):
    mock_config = {
        'refresh_token': 'test_refresh_token',
        'client_id': 'test_client_id',
        'client_secret': 'test_client_secret',
        'start_date': '2021-01-02T00:00:00Z',
        'api_type': 'REST',
        'select_fields_by_default': 'true'
    }
    if add_lookback_window:
        mock_config['lookback_window'] = 20
    return MockParseArgs(config=mock_config)

class TestLookbackWindow(unittest.TestCase):
    """
        Test cases to verify the lookback window seconds are subtracted from the start date or state file date.
    """

    # start date
    start_date = '2021-01-02T00:00:00Z'
    # bookmark date
    bookmark_date = '2021-01-10T00:00:00.000000Z'

    # mock config without lookback
    config = {
        'start_date': start_date
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
                'SystemModstamp': bookmark_date,
            }
        }
    }

    # salesforce object without lookback
    sf_without_lookback = Salesforce(default_start_date=config.get('start_date'))

    # add lookback in the config
    config['lookback_window'] = 20

    # salesforce object with lookback
    sf_with_lookback = Salesforce(
        default_start_date=config.get('start_date'),
        lookback_window=int(config.get('lookback_window', DEFAULT_LOOKBACK_WINDOW)))

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_default_lookback_window(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify DEFAULT_LOOKBACK_WINDOW (10 seconds) is passed if user has not passed from the config.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce

        # mock parse args
        mocked_parse_args.return_value = get_args()

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        # get lookback_window argument when initializing the class
        actual_lookback_window = kwargs.get('lookback_window')

        # verify DEFAULT_LOOKBACK_WINDOW (10 seconds) was passed as lookback_window
        self.assertEqual(actual_lookback_window, DEFAULT_LOOKBACK_WINDOW)

    @mock.patch('tap_salesforce.Salesforce')
    @mock.patch('singer.utils.parse_args')
    def test_desired_lookback_window(self, mocked_parse_args, mocked_Salesforce_class):
        """
            Test case to verify user defined lookback window is set when is passed from the config.
        """

        mocked_Salesforce_class.side_effect = MockSalesforce

        # mock parse args
        mocked_parse_args.return_value = get_args(add_lookback_window=True)

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
            Test case to verify DEFAULT_LOOKBACK_WINDOW (10 seconds) are not subtracted from the start date if state and lookback_window is not passed
        """
        # function call with apply lookback as 'True'
        start_date = self.sf_without_lookback.get_start_date({}, self.mock_catalog_entry, with_lookback=True)

        # verify the start date is not altered as state is not passed
        self.assertEqual(start_date, self.start_date)

    def test_desired_lookback_window__get_start_date(self):
        """
            Test case to verify user defined lookback window seconds are not subtracted from the start date if state is not passed
        """
        # function call with apply lookback as 'True'
        start_date = self.sf_with_lookback.get_start_date({}, self.mock_catalog_entry, with_lookback=True)

        # verify the start date is not altered as state is not passed
        self.assertEqual(start_date, self.start_date)

    def test_default_lookback_window_with_state__get_start_date(self):
        """
            Test case to verify DEFAULT_LOOKBACK_WINDOW (10 seconds) are subtracted from the state file date if lookback_window is not passed
        """
        # function call with apply lookback as 'True'
        start_date = self.sf_without_lookback.get_start_date(self.mock_state, self.mock_catalog_entry, with_lookback=True)

        # verify DEFAULT_LOOKBACK_WINDOW (10 seconds) were subtracted from state file date
        self.assertEqual(start_date, '2021-01-09T23:59:50.000000Z')

    def test_desired_lookback_window_with_state__get_start_date(self):
        """
            Test case to verify used defined lookback window seconds are subtracted from the state file date if lookback_window is passed
        """
        # function call with apply lookback as 'True'
        start_date = self.sf_with_lookback.get_start_date(self.mock_state, self.mock_catalog_entry, with_lookback=True)

        # verify 20 seconds were subtracted from state file date
        self.assertEqual(start_date, '2021-01-09T23:59:40.000000Z')

    def test_no_lookback_window_subtraction__get_start_date(self):
        """
            Test case to verify start date is not changed when we pass 'with_lookback=False'
        """
        # function call with apply lookback as 'False'
        start_date = self.sf_without_lookback.get_start_date({}, self.mock_catalog_entry, with_lookback=False)

        # verify we did not subtract any seconds from the start date
        self.assertEqual(start_date, self.start_date)

    def test_no_lookback_window_subtraction_with_state__get_start_date(self):
        """
            Test case to verify state file date is not changed when we pass 'with_lookback=False'
        """
        # function call with apply lookback as 'False'
        start_date = self.sf_without_lookback.get_start_date(self.mock_state, self.mock_catalog_entry, with_lookback=False)

        # verify we did not subtract any seconds from state file date
        self.assertEqual(start_date, self.bookmark_date)
