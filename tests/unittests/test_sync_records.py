"""Unit tests for Rest._sync_records pagination.

Verifies that:
1. A single-page response (no nextRecordsUrl) yields all records and stops.
2. A multi-page response follows nextRecordsUrl until exhausted.
3. An empty records list returns nothing without error.
"""
import unittest
from unittest import mock

from tap_salesforce import Salesforce
from tap_salesforce.salesforce.rest import Rest


def _make_rest():
    sf = Salesforce(default_start_date='2019-01-01T00:00:00Z', api_type="REST")
    sf.instance_url = "https://example.salesforce.com"
    return Rest(sf)


def _mock_response(records, next_records_url=None):
    resp = mock.MagicMock()
    resp.json.return_value = {
        "records": records,
        "nextRecordsUrl": next_records_url,
    }
    return resp


class TestSyncRecordsSinglePage(unittest.TestCase):
    """Single-page responses must yield all records and make exactly one request."""

    def test_single_page_yields_all_records(self):
        rest = _make_rest()
        records = [{"Id": "001"}, {"Id": "002"}]
        resp = _mock_response(records)

        with mock.patch.object(rest.sf, "_make_request", return_value=resp) as mock_req:
            result = list(rest._sync_records("https://example.salesforce.com/query", {}, {}))

        self.assertEqual(result, records)
        mock_req.assert_called_once()

    def test_empty_records_returns_nothing(self):
        rest = _make_rest()
        resp = _mock_response([])

        with mock.patch.object(rest.sf, "_make_request", return_value=resp):
            result = list(rest._sync_records("https://example.salesforce.com/query", {}, {}))

        self.assertEqual(result, [])


class TestSyncRecordsMultiPage(unittest.TestCase):
    """Multi-page responses must follow nextRecordsUrl until it is absent."""

    def test_two_pages_yields_all_records(self):
        rest = _make_rest()
        page1 = _mock_response([{"Id": "001"}], next_records_url="/query/next-1")
        page2 = _mock_response([{"Id": "002"}])

        with mock.patch.object(rest.sf, "_make_request", side_effect=[page1, page2]) as mock_req:
            result = list(rest._sync_records("https://example.salesforce.com/query", {}, {}))

        self.assertEqual(result, [{"Id": "001"}, {"Id": "002"}])
        self.assertEqual(mock_req.call_count, 2)
        # Second call must use the instance_url + nextRecordsUrl
        second_url = mock_req.call_args_list[1][0][1]
        self.assertEqual(second_url, "https://example.salesforce.com/query/next-1")

    def test_three_pages_yields_all_records(self):
        rest = _make_rest()
        pages = [
            _mock_response([{"Id": "001"}], next_records_url="/query/next-1"),
            _mock_response([{"Id": "002"}], next_records_url="/query/next-2"),
            _mock_response([{"Id": "003"}]),
        ]

        with mock.patch.object(rest.sf, "_make_request", side_effect=pages):
            result = list(rest._sync_records("https://example.salesforce.com/query", {}, {}))

        self.assertEqual(result, [{"Id": "001"}, {"Id": "002"}, {"Id": "003"}])

    def test_stops_when_next_records_url_is_none(self):
        """Explicitly confirms the loop exits when nextRecordsUrl is absent."""
        rest = _make_rest()
        page1 = _mock_response([{"Id": "001"}], next_records_url=None)

        with mock.patch.object(rest.sf, "_make_request", return_value=page1) as mock_req:
            result = list(rest._sync_records("https://example.salesforce.com/query", {}, {}))

        mock_req.assert_called_once()
        self.assertEqual(result, [{"Id": "001"}])


if __name__ == "__main__":
    unittest.main()
