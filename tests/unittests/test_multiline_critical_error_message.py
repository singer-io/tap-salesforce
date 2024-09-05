import unittest
from unittest import mock
from tap_salesforce import main

# mock "main_impl" and raise multiline error
def raise_error():
    raise Exception("""Error syncing Transaction__c: 400 Client Error: Bad Request for url: https://test.my.salesforce.com/services/async/41.0/job/7502K00000IcACtQAN/batch/test123j/result/test123b Response: <?xml version="1.0" encoding="UTF-8"?><error
       xmlns="http://www.test-force.com/20091/01/asyncapi1/dataload1">
     <exceptionCode>InvalidSessionId</exceptionCode>
     <exceptionMessage>Invalid session id</exceptionMessage>
    </error>""")

class TestMultiLineCriticalErrorMessage(unittest.TestCase):
    """
        Test case to verify every line in the multiline error contains 'CRITICAL'
    """

    @mock.patch("tap_salesforce.LOGGER.critical")
    @mock.patch("tap_salesforce.main_impl")
    def test_multiline_critical_error_message(self, mocked_main_impl, mocked_logger_critical):
        # mock "main_impl" and raise multiline error
        mocked_main_impl.side_effect = raise_error

        # verify "Exception" is raise on function call
        with self.assertRaises(Exception):
            main()

        # verify "LOGGER.critical" is called 5 times, as the error raised contains 5 lines
        self.assertEqual(mocked_logger_critical.call_count, 5)

    @mock.patch("tap_salesforce.singer_utils.parse_args")
    @mock.patch('tap_salesforce.salesforce.requests.Session.post')
    def test_http_406_error_message(self, mocked_post, mocked_parse_args):

        args = mock.MagicMock()
        args.config = {
            "refresh_token": "abc",
            "client_id": "abc",
            "client_secret": "abc",
            "quota_percent_total": 10.1,
            "quota_percent_per_run": 10.1,
            "is_sandbox": True,
            "start_date": "2020-02-04T07:46:29Z",
            "api_type": "abc",
            "lookback_window": "12"
        }
        mocked_parse_args.return_value = args

        # Define the mock response
        mock_response = mock.MagicMock()
        mock_response.status_code = 406
        mocked_post.return_value = mock_response


        # verify "Exception" is raise on function call
        with self.assertRaises(Exception):
            main()

        # verify "requests.Session.post" is called 6 times
        self.assertEqual(mocked_post.call_count, 6)
