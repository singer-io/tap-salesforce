import json
import os
import tempfile
import unittest
from unittest import mock

from tap_salesforce.salesforce import Salesforce


def _make_sf(**kwargs):
    defaults = dict(
        refresh_token='initial-refresh-token',
        sf_client_id='client-id',
        sf_client_secret='client-secret',
        default_start_date='2021-01-01T00:00:00Z',
        api_type='REST',
    )
    defaults.update(kwargs)
    return Salesforce(**defaults)


def _mock_login_response(access_token='new-access', instance_url='https://sf.example.com',
                         refresh_token=None):
    payload = {'access_token': access_token, 'instance_url': instance_url}
    if refresh_token is not None:
        payload['refresh_token'] = refresh_token
    resp = mock.MagicMock()
    resp.json.return_value = payload
    return resp


class TestRefreshTokenRotation(unittest.TestCase):

    # ------------------------------------------------------------------ #
    # login() updates self.refresh_token when rotation token is returned  #
    # ------------------------------------------------------------------ #

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_login_updates_refresh_token_when_rotated(self, mock_request):
        """A new refresh_token in the OAuth response replaces the stored one."""
        mock_request.return_value = _mock_login_response(refresh_token='rotated-token')

        sf = _make_sf()
        with mock.patch.object(sf, 'login_timer') as _timer:
            sf.login_timer = mock.MagicMock()
            # Patch threading.Timer so no real background thread starts
            with mock.patch('threading.Timer') as mock_timer_cls:
                mock_timer_cls.return_value = mock.MagicMock()
                sf.login()

        self.assertEqual(sf.refresh_token, 'rotated-token')

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_login_keeps_refresh_token_when_not_in_response(self, mock_request):
        """refresh_token is unchanged when the OAuth response omits it."""
        mock_request.return_value = _mock_login_response()  # no refresh_token key

        sf = _make_sf()
        with mock.patch('threading.Timer') as mock_timer_cls:
            mock_timer_cls.return_value = mock.MagicMock()
            sf.login()

        self.assertEqual(sf.refresh_token, 'initial-refresh-token')

    # ------------------------------------------------------------------ #
    # _write_config is called only when the token changes                 #
    # ------------------------------------------------------------------ #

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_write_config_called_when_token_rotated(self, mock_request):
        """_write_config is called when a rotated refresh token is received."""
        mock_request.return_value = _mock_login_response(refresh_token='rotated-token')

        sf = _make_sf()
        with mock.patch.object(sf, '_write_config') as mock_write_config:
            with mock.patch('threading.Timer') as mock_timer_cls:
                mock_timer_cls.return_value = mock.MagicMock()
                sf.login()

        mock_write_config.assert_called_once()

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_write_config_not_called_when_no_new_token(self, mock_request):
        """_write_config is NOT called when no refresh_token is returned."""
        mock_request.return_value = _mock_login_response()

        sf = _make_sf()
        with mock.patch.object(sf, '_write_config') as mock_write_config:
            with mock.patch('threading.Timer') as mock_timer_cls:
                mock_timer_cls.return_value = mock.MagicMock()
                sf.login()

        mock_write_config.assert_not_called()

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_write_config_not_called_when_same_token_returned(self, mock_request):
        """_write_config is NOT called when the response echoes the existing token."""
        mock_request.return_value = _mock_login_response(
            refresh_token='initial-refresh-token'  # same as the initial value
        )

        sf = _make_sf()
        with mock.patch.object(sf, '_write_config') as mock_write_config:
            with mock.patch('threading.Timer') as mock_timer_cls:
                mock_timer_cls.return_value = mock.MagicMock()
                sf.login()

        mock_write_config.assert_not_called()

    # ------------------------------------------------------------------ #
    # access_token is always updated regardless of rotation               #
    # ------------------------------------------------------------------ #

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_access_token_always_updated(self, mock_request):
        """access_token is refreshed regardless of whether refresh_token rotates."""
        mock_request.return_value = _mock_login_response(
            access_token='brand-new-access-token',
            refresh_token='rotated-token'
        )

        sf = _make_sf()
        with mock.patch('threading.Timer') as mock_timer_cls:
            mock_timer_cls.return_value = mock.MagicMock()
            sf.login()

        self.assertEqual(sf.access_token, 'brand-new-access-token')

    # ------------------------------------------------------------------ #
    # Persistence: callback writes new token to config file               #
    # ------------------------------------------------------------------ #

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_rotated_token_persisted_to_config_file(self, mock_request):
        """_write_config updates the config file with the rotated refresh token."""
        mock_request.return_value = _mock_login_response(refresh_token='rotated-token')

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, 'config.json')
            initial_config = {
                'refresh_token': 'initial-refresh-token',
                'client_id': 'client-id',
                'client_secret': 'client-secret',
                'start_date': '2021-01-01T00:00:00Z',
                'api_type': 'REST',
            }
            with open(config_path, 'w') as f:
                json.dump(initial_config, f)

            sf = _make_sf(config_path=config_path)
            with mock.patch('threading.Timer') as mock_timer_cls:
                mock_timer_cls.return_value = mock.MagicMock()
                sf.login()

            with open(config_path) as f:
                saved_config = json.load(f)

            self.assertEqual(saved_config['refresh_token'], 'rotated-token')
            # Other keys must be preserved
            self.assertEqual(saved_config['client_id'], 'client-id')

    # ------------------------------------------------------------------ #
    # Subsequent login() calls use the updated (rotated) refresh token    #
    # ------------------------------------------------------------------ #

    @mock.patch('tap_salesforce.salesforce.Salesforce._make_request')
    def test_subsequent_login_uses_rotated_token(self, mock_request):
        """After rotation, the second login sends the new refresh_token."""
        first_response = _mock_login_response(refresh_token='second-token')
        second_response = _mock_login_response(refresh_token='third-token')
        mock_request.side_effect = [first_response, second_response]

        sf = _make_sf()
        with mock.patch('threading.Timer') as mock_timer_cls:
            mock_timer_cls.return_value = mock.MagicMock()
            sf.login()
            self.assertEqual(sf.refresh_token, 'second-token')
            sf.login()
            self.assertEqual(sf.refresh_token, 'third-token')

        # Verify the second POST used the rotated token
        second_call_body = mock_request.call_args_list[1][1].get('body') or \
                           mock_request.call_args_list[1][0][2]
        self.assertIn('second-token', str(second_call_body))


if __name__ == '__main__':
    unittest.main()
