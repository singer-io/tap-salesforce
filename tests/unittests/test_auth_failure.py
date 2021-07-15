import unittest
from unittest.mock import Mock
from tap_salesforce import Salesforce

class TestAuthorizationHandling(unittest.TestCase):
    def test_attempt_login_failure_with_retry(self):
        """
        When we see an exception on the login, we expect the error to be raised after 3 tries
        """
        mocked_service_caller = Mock()
        mocked_service_caller._make_request = Mock(side_effect=Exception("this is an example auth exception"))
        salesforce_object = Salesforce(default_start_date="2021-07-15T13:25:54Z")
        with self.assertRaisesRegexp(Exception, "this is an example auth exception") as e:
            salesforce_object.attempt_login()
        self.assertEqual(3, mocked_service_caller._make_request.call_count)


"""
Traceback (most recent call last):
  File "/opt/code/tap-salesforce/tests/unittests/test_auth_failure.py", line 13, in test_attempt_login_failure_with_retry
    Salesforce.attempt_login()
AssertionError: "this is an example auth exception" does not match "attempt_login() missing 1 required positional argument: 'self'"
"""
