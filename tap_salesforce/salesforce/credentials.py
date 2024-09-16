import threading
import logging
import requests
import backoff
from collections import namedtuple
from simple_salesforce import SalesforceLogin

from tap_salesforce.salesforce.exceptions import RetriableSalesforceAuthenticationError

LOGGER = logging.getLogger(__name__)


OAuthCredentials = namedtuple('OAuthCredentials', (
    "client_id",
    "client_secret",
    "refresh_token"
))

PasswordCredentials = namedtuple('PasswordCredentials', (
    "username",
    "password",
    "security_token"
))


def parse_credentials(config):
    for cls in reversed((OAuthCredentials, PasswordCredentials)):
        creds = cls(*(config.get(key) for key in cls._fields))
        if all(creds):
            return creds

    raise Exception("Cannot create credentials from config.")


class SalesforceAuth():
    def __init__(self, credentials, is_sandbox=False):
        self.is_sandbox = is_sandbox
        self._credentials = credentials
        self._access_token = None
        self._instance_url = None
        self._auth_header = None
        self.login_timer = None

    def login(self):
        """Attempt to login and set the `instance_url` and `access_token` on success."""
        pass

    @property
    def rest_headers(self):
        return {"Authorization": "Bearer {}".format(self._access_token)}

    @property
    def bulk_headers(self):
        return {"X-SFDC-Session": self._access_token,
                "Content-Type": "application/json"}

    @property
    def instance_url(self):
        return self._instance_url

    @classmethod
    def from_credentials(cls, credentials, **kwargs):
        if isinstance(credentials, OAuthCredentials):
            return SalesforceAuthOAuth(credentials, **kwargs)

        if isinstance(credentials, PasswordCredentials):
            return SalesforceAuthPassword(credentials, **kwargs)

        raise Exception("Invalid credentials")


class SalesforceAuthOAuth(SalesforceAuth):
    # The minimum expiration setting for SF Refresh Tokens is 15 minutes
    REFRESH_TOKEN_EXPIRATION_PERIOD = 900

    # Errors that can be retried
    RETRIABLE_SALESFORCE_RESPONSES = [
        {'error': 'invalid_grant', 'error_description': 'expired authorization code'}
    ]

    @property
    def _login_body(self):
        return {'grant_type': 'refresh_token', **self._credentials._asdict()}

    @property
    def _login_url(self):
        login_url = 'https://login.salesforce.com/services/oauth2/token'

        if self.is_sandbox:
            login_url = 'https://test.salesforce.com/services/oauth2/token'

        return login_url

    @backoff.on_exception(
        backoff.expo,
        RetriableSalesforceAuthenticationError,
        max_tries=5,
        factor=4,
        jitter=None
    )
    def login(self):
        resp = None  # Ensure resp is defined outside the try block
        try:
            LOGGER.info("Attempting login via OAuth2")

            resp = requests.post(self._login_url,
                                 data=self._login_body,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})

            resp.raise_for_status()  # This will raise an exception for HTTP errors
            auth = resp.json()

            LOGGER.info("OAuth2 login successful")
            self._access_token = auth['access_token']
            self._instance_url = auth['instance_url']

            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(self.REFRESH_TOKEN_EXPIRATION_PERIOD, self.login)
            self.login_timer.start()
        except requests.exceptions.HTTPError as e:
            error_message = f"{e}, Response from Salesforce: {resp.text}"
            failed_auth_response = resp.json()
            if failed_auth_response in self.RETRIABLE_SALESFORCE_RESPONSES:
                raise RetriableSalesforceAuthenticationError(error_message) from e
            else:
                raise Exception(error_message) from e
        except Exception as e:
            error_message = str(e)
            if resp is not None:
                # Ensure we capture the response body even when an error occurs
                error_message += ", Response from Salesforce: {}".format(resp.text)
            raise Exception(error_message) from e


class SalesforceAuthPassword(SalesforceAuth):
    def login(self):
        login = SalesforceLogin(
            sandbox=self.is_sandbox,
            **self._credentials._asdict()
        )

        self._access_token, host = login
        self._instance_url = "https://" + host

if __name__ == "__main__":
    sfdc_auth = SalesforceAuth.from_credentials(
        is_sandbox=False,
        credentials= parse_credentials({
            "client_id": "secret_client_id",
            "client_secret": "secret_client_secret",
            "refresh_token": "abc123",
        }),
    )
    sfdc_auth.login()
