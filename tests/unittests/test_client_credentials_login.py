import unittest
from unittest import mock
import requests
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.exceptions import TapSalesforceException


class TestClientCredentialsLogin(unittest.TestCase):
    """Unit tests for OAuth2 client_credentials login flow."""

    def _get_sf(self, instance_url="https://myorg.my.salesforce.com"):
        return Salesforce(
            sf_client_id="test_client_id",
            sf_client_secret="test_client_secret",
            instance_url=instance_url,
            default_start_date="2024-01-01T00:00:00Z",
            api_type="REST",
        )

    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_success(self, mock_post):
        """Successful client_credentials login sets access_token."""
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            "access_token": "test_access_token_123",
            "instance_url": "https://myorg.my.salesforce.com",
            "token_type": "Bearer",
        }
        mock_response.raise_for_status = mock.MagicMock()
        mock_post.return_value = mock_response

        sf = self._get_sf()
        sf.login()

        self.assertEqual(sf.access_token, "test_access_token_123")
        mock_post.assert_called_once()

    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_sends_correct_payload(self, mock_post):
        """Login sends grant_type, client_id, client_secret."""
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            "access_token": "token",
            "instance_url": "https://myorg.my.salesforce.com",
        }
        mock_response.raise_for_status = mock.MagicMock()
        mock_post.return_value = mock_response

        sf = self._get_sf()
        sf.login()

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["data"]["grant_type"], "client_credentials")
        self.assertEqual(call_kwargs["data"]["client_id"], "test_client_id")
        self.assertEqual(call_kwargs["data"]["client_secret"], "test_client_secret")

    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_uses_instance_url(self, mock_post):
        """Login POSTs to {instance_url}/services/oauth2/token."""
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            "access_token": "token",
            "instance_url": "https://myorg.my.salesforce.com",
        }
        mock_response.raise_for_status = mock.MagicMock()
        mock_post.return_value = mock_response

        sf = self._get_sf("https://myorg.my.salesforce.com")
        sf.login()

        call_url = mock_post.call_args[0][0]
        self.assertEqual(call_url, "https://myorg.my.salesforce.com/services/oauth2/token")

    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_strips_trailing_slash(self, mock_post):
        """Trailing slash in instance_url is stripped."""
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {
            "access_token": "token",
            "instance_url": "https://myorg.my.salesforce.com",
        }
        mock_response.raise_for_status = mock.MagicMock()
        mock_post.return_value = mock_response

        sf = self._get_sf("https://myorg.my.salesforce.com/")
        sf.login()

        call_url = mock_post.call_args[0][0]
        self.assertEqual(call_url, "https://myorg.my.salesforce.com/services/oauth2/token")

    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_failure_raises_exception(self, mock_post):
        """HTTP error during login raises TapSalesforceException with response text."""
        mock_response = mock.MagicMock()
        mock_response.status_code = 400
        mock_response.headers = {}
        mock_response.text = '{"error":"invalid_grant","error_description":"no client credentials user enabled"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "400 Client Error", response=mock_response)
        mock_post.return_value = mock_response

        sf = self._get_sf()

        with self.assertRaises(TapSalesforceException) as ctx:
            sf.login()

        self.assertIn("no client credentials user enabled", str(ctx.exception))

    @mock.patch("tap_salesforce.salesforce.time.sleep", return_value=None)
    @mock.patch("tap_salesforce.salesforce.requests.Session.post")
    def test_login_connection_error(self, mock_post, mock_sleep):
        """ConnectionError during login retries and eventually raises."""
        mock_post.side_effect = requests.exceptions.ConnectionError("DNS resolution failed")

        sf = self._get_sf()

        with self.assertRaises(Exception):
            sf.login()

        # backoff retries 6 times
        self.assertEqual(mock_post.call_count, 6)


if __name__ == "__main__":
    unittest.main()
