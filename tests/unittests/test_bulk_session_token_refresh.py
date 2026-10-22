"""Bulk result downloads must follow the session token the login timer rotates.

A large stream's result files can take longer to drain than the token refresh
interval, so a token captured once at the top of the download outlives itself
and Salesforce rejects it with a 400 InvalidSessionId.
"""
import unittest
from unittest import mock

import requests

from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.bulk import Bulk

RESULT_LIST_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
    '<result>752AAA</result><result>752BBB</result>'
    '</result-list>'
)

INVALID_SESSION_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<error xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
    '<exceptionCode>InvalidSessionId</exceptionCode>'
    '<exceptionMessage>Invalid session id</exceptionMessage>'
    '</error>'
)

RESULT_FILE_CSV = {
    '752AAA': 'Id,Name\r\n001,Alice\r\n',
    '752BBB': 'Id,Name\r\n002,Bob\r\n',
}

CATALOG_ENTRY = {'stream': 'Contact', 'tap_stream_id': 'Contact'}

JOB_ID = '750Ad00000RHw02IAD'
BATCH_ID = '751Ad00000WzmoZIAR'


def _make_sf():
    sf = Salesforce(
        refresh_token='refresh-token',
        sf_client_id='client-id',
        sf_client_secret='client-secret',
        default_start_date='2021-01-01T00:00:00Z',
        api_type='BULK',
    )
    sf.access_token = 'token-A'
    sf.instance_url = 'https://sf.example.com'
    return sf


def _text_response(text):
    resp = mock.MagicMock()
    resp.text = text
    return resp


def _csv_response(body):
    resp = mock.MagicMock()
    resp.iter_content.side_effect = lambda *a, **kw: iter([body])
    return resp


def _transport(sf, rotate_token=True, reject_stale_token=False):
    """Fake Salesforce that mints a new token mid-download, like the login timer.

    With reject_stale_token it also behaves like the real org: any token other
    than the currently valid one is answered with 400 InvalidSessionId.
    """
    calls = []

    def _side_effect(_method, url, headers=None, **_kwargs):
        presented = headers.get('X-SFDC-Session')
        # snapshot: get_batch_results mutates and reuses one dict
        calls.append({'url': url, 'token': presented})

        if reject_stale_token and presented != sf.access_token:
            response = mock.MagicMock()
            response.status_code = 400
            response.text = INVALID_SESSION_XML
            raise requests.exceptions.HTTPError(
                '400 Client Error: Bad Request for url: {} Response: {}'.format(
                    url, INVALID_SESSION_XML),
                response=response)

        if url.endswith('/result'):
            return _text_response(RESULT_LIST_XML)

        result_id = url.rsplit('/', 1)[-1]
        if rotate_token and result_id == '752AAA':
            sf.access_token = 'token-B'
        return _csv_response(RESULT_FILE_CSV[result_id])

    return calls, _side_effect


class TestBulkSessionTokenRefresh(unittest.TestCase):

    @staticmethod
    def _drain(sf, side_effect):
        with mock.patch.object(sf, '_make_request', side_effect=side_effect):
            return list(Bulk(sf).get_batch_results(JOB_ID, BATCH_ID, CATALOG_ENTRY))

    def test_result_file_requests_follow_rotated_token(self):
        """Each result file must carry the token current at the time of that request."""
        sf = _make_sf()
        calls, side_effect = _transport(sf)

        self._drain(sf, side_effect)

        file_calls = [c for c in calls if '/result/' in c['url']]
        self.assertEqual(len(file_calls), 2)
        self.assertEqual(file_calls[0]['token'], 'token-A')
        self.assertEqual(file_calls[1]['token'], 'token-B')

    def test_rotation_midway_does_not_invalidate_session(self):
        """A rotation between result files must not surface as InvalidSessionId."""
        sf = _make_sf()
        _calls, side_effect = _transport(sf, reject_stale_token=True)

        records = self._drain(sf, side_effect)

        self.assertEqual([r['Id'] for r in records], ['001', '002'])

    def test_download_without_rotation_is_unchanged(self):
        """Control: with no rotation the download behaviour must not change."""
        sf = _make_sf()
        calls, side_effect = _transport(sf, rotate_token=False, reject_stale_token=True)

        records = self._drain(sf, side_effect)

        self.assertEqual([r['Id'] for r in records], ['001', '002'])
        self.assertTrue(all(c['token'] == 'token-A' for c in calls))
