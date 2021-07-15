import unittest
from unittest.mock import Mock
from tap_salesforce import Salesforce

class TestAuthorizationHandling(unittest.TestCase):
    def test_attempt_login_failure_with_retry(self):
        """
        When we see an exception on the login, we expect the error to be raised after 3 tries
        """
        salesforce_object = Salesforce(default_start_date="2021-07-15T13:25:54Z")
        salesforce_object.session.post = Mock(side_effect=Exception("this is an example auth exception"))
        with self.assertRaisesRegexp(Exception, "this is an example auth exception") as e:
            salesforce_object.login()
        self.assertEqual(3, salesforce_object.session.post.call_count)
