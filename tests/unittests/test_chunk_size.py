import unittest
from tap_salesforce.salesforce import DEFAULT_CHUNK_SIZE
import tap_salesforce
from unittest import mock


class MockSalesforce:
    def __init__(self, *args, **kwargs):
        self.rest_requests_attempted = 0
        self.jobs_completed = 0
        self.login_timer = None

    def login(self):
        pass


class MockParseArgs:
    """Mock the parsed_args() in main"""

    def __init__(self, config):
        self.config = config
        self.discover = None
        self.properties = None
        self.state = None


def get_args(chunk_size=None):
    """Return the MockParseArgs object"""
    mock_config = {
        "refresh_token": None,
        "client_id": None,
        "client_secret": None,
        "start_date": "2021-01-02T00:00:00Z",
    }
    mock_config["chunk_size"] = chunk_size
    return MockParseArgs(mock_config)


@mock.patch("tap_salesforce.Salesforce", side_effect=MockSalesforce)
@mock.patch("singer.utils.parse_args")
class TestChunkSize(unittest.TestCase):
    """Test cases to verify the chunk_size value is set as expected according to config"""

    def test_default_value_in_chunk_size(self, mocked_parsed_args, mocked_Salesforce_class):
        """
        Unit test to ensure that "DEFAULT_CHUNK_SIZE" value is used when "chunk_size" is not passed in config
        """
        mocked_parsed_args.return_value = get_args()

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, DEFAULT_CHUNK_SIZE)

    def test_empty_string_value_in_chunk_size(self, mocked_parse_args, mocked_Salesforce_class):
        """
        Unit test to ensure that"DEFAULT_CHUNK_SIZE" value is used when passed "chunk_size" value is empty string in config
        """

        # mock parse args
        mocked_parse_args.return_value = get_args("")

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, DEFAULT_CHUNK_SIZE)

    def test_zero_value_in_chunk_size(self, mocked_parse_args, mocked_Salesforce_class):
        """
        Unit test to ensure that "DEFAULT_CHUNK_SIZE" value is used when "chunk_size" value is zero in config
        """

        # mock parse args
        mocked_parse_args.return_value = get_args(0)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, DEFAULT_CHUNK_SIZE)

    def test_string_zero_value_in_chunk_size(self, mocked_parse_args, mocked_Salesforce_class):
        """
        Unit test to ensure that "DEFAULT_CHUNK_SIZE" value is used when "chunk_size" value is zero string in config
        """

        # mock parse args
        mocked_parse_args.return_value = get_args("0")

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, DEFAULT_CHUNK_SIZE)

    def test_float_value_in_chunk_size(self, mocked_parse_args, mocked_Salesforce_class):
        """
        Unit test to ensure that int "chunk_size" value is used when float "chunk_size" is passed in config
        """

        # mock parse args
        mocked_parse_args.return_value = get_args(2000.200)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, 2000)
    
    @mock.patch("tap_salesforce.LOGGER.info")
    def test_max_value_in_chunk_size(self, mocked_logger, mocked_parse_args, mocked_Salesforce_class):
        """
        Unit test to ensure that "MAX_CHUNK_SIZE" value is used when "chunk_size" passed in config is greater then the maximum chunk size that API supports.
        """

        # mock parse args
        mocked_parse_args.return_value = get_args(260000)

        # function call
        tap_salesforce.main_impl()

        # get arguments passed during calling 'Salesforce' class
        args, kwargs = mocked_Salesforce_class.call_args
        chunk_size = kwargs.get("chunk_size")
        self.assertEqual(chunk_size, 250000)
        # check if the logger is called with correct logger message
        mocked_logger.assert_called_with('The provided chunk_size value is greater than 250k hence tap will use 250k which is the maximum chunk size the API supports.')

