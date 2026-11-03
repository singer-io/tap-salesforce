import unittest
from unittest import mock

from tap_salesforce.salesforce.rest import Rest


def _page(records, next_records_url=None):
    resp = mock.MagicMock()
    payload = {'records': records}
    if next_records_url is not None:
        payload['nextRecordsUrl'] = next_records_url
    resp.json.return_value = payload
    return resp


class TestRestPagination(unittest.TestCase):

    def test_sync_records_does_not_resend_query_params_on_cursor_pages(self):
        """GH-212: nextRecordsUrl cursors encode the query server-side; the
        original ?q=... params must not be re-sent on page 2+ (HTTP 431)."""
        sf = mock.MagicMock()
        sf.instance_url = 'https://test.my.salesforce.com'
        sf._make_request.side_effect = [
            _page([{'Id': '001'}], next_records_url='/services/data/v61.0/query/01gR0000001-2'),
            _page([{'Id': '002'}]),
        ]

        rest = Rest(sf)
        records = list(rest._sync_records(
            'https://test.my.salesforce.com/services/data/v61.0/queryAll',
            {'Authorization': 'Bearer token'},
            {'q': 'SELECT Id, Name FROM Opportunity'}))

        self.assertEqual(records, [{'Id': '001'}, {'Id': '002'}])
        self.assertEqual(sf._make_request.call_count, 2)
        first_call = sf._make_request.call_args_list[0]
        second_call = sf._make_request.call_args_list[1]
        self.assertEqual(first_call.kwargs['params'], {'q': 'SELECT Id, Name FROM Opportunity'})
        self.assertEqual(
            second_call.args[1],
            'https://test.my.salesforce.com/services/data/v61.0/query/01gR0000001-2')
        self.assertIsNone(second_call.kwargs['params'])

    def test_sync_records_single_page_keeps_params(self):
        """A single-page result set issues exactly one request with the query params."""
        sf = mock.MagicMock()
        sf.instance_url = 'https://test.my.salesforce.com'
        sf._make_request.side_effect = [_page([{'Id': '001'}])]

        rest = Rest(sf)
        records = list(rest._sync_records(
            'https://test.my.salesforce.com/services/data/v61.0/queryAll',
            {'Authorization': 'Bearer token'},
            {'q': 'SELECT Id FROM Account'}))

        self.assertEqual(records, [{'Id': '001'}])
        self.assertEqual(sf._make_request.call_count, 1)
        only_call = sf._make_request.call_args_list[0]
        self.assertEqual(only_call.kwargs['params'], {'q': 'SELECT Id FROM Account'})
