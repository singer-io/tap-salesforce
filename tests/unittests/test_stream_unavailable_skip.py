"""Regression tests for TapSalesforceStreamUnavailableException handling.

Previously, Bulk.query() swallowed this exception and simply `return`ed when
nothing had been yielded yet, which looked identical to a genuinely empty,
successful query to sync_records(): incremental streams (without PK
chunking) would have their bookmark advanced to start_time -- permanently
skipping any unread history -- and full-table streams would emit
ACTIVATE_VERSION with zero records, potentially replacing existing
destination data with an empty snapshot.

The exception must now propagate out of sync_records() (bypassing its
trailing bookmark/ACTIVATE_VERSION completion code), and sync_stream() must
catch it and return gracefully (without touching state) so do_sync()'s
per-stream loop continues on to the next selected stream instead of
aborting the whole sync.
"""
import unittest
from unittest import mock

from tap_salesforce import Salesforce, metrics
from tap_salesforce.sync import sync_records, sync_stream
from tap_salesforce.salesforce.exceptions import TapSalesforceStreamUnavailableException


def _make_sf(pk_chunking=False):
    sf = Salesforce(default_start_date='2019-02-04T12:15:00Z', api_type="BULK")
    sf.pk_chunking = pk_chunking
    return sf


class TestSyncRecordsPropagatesStreamUnavailable(unittest.TestCase):
    """Covers sync_records(): the exception must propagate, not be treated
    as a successful empty result."""

    @mock.patch('tap_salesforce.salesforce.Salesforce.query',
                side_effect=TapSalesforceStreamUnavailableException("DatacloudCompany: boom"))
    def test_incremental_stream_bookmark_is_not_advanced(self, _mock_query):
        """Regression test: previously this silently set the bookmark to
        start_time even though zero rows were actually read, permanently
        skipping any unread history."""
        sf = _make_sf()
        catalog_entry = {
            "stream": "DatacloudCompany",
            "tap_stream_id": "DatacloudCompany",
            "schema": {},
            "metadata": [{"breadcrumb": [], "metadata": {"replication-key": "LastModifiedDate"}}],
        }
        state = {}
        counter = metrics.record_counter('DatacloudCompany')

        with self.assertRaises(TapSalesforceStreamUnavailableException):
            sync_records(sf, catalog_entry, state, counter)

        self.assertNotIn('DatacloudCompany', state.get('bookmarks', {}))

    @mock.patch('tap_salesforce.salesforce.Salesforce.query',
                side_effect=TapSalesforceStreamUnavailableException("ExternalEncryptionRootKey: boom"))
    def test_full_table_stream_does_not_emit_activate_version(self, _mock_query):
        """Regression test: previously this emitted ACTIVATE_VERSION with
        zero records for a full-table (no replication-key) stream, which
        could replace existing destination data with an empty snapshot."""
        sf = _make_sf()
        catalog_entry = {
            "stream": "ExternalEncryptionRootKey",
            "tap_stream_id": "ExternalEncryptionRootKey",
            "schema": {},
            "metadata": [],
        }
        state = {}
        counter = metrics.record_counter('ExternalEncryptionRootKey')

        with mock.patch('singer.write_message') as mock_write_message:
            with self.assertRaises(TapSalesforceStreamUnavailableException):
                sync_records(sf, catalog_entry, state, counter)

        mock_write_message.assert_not_called()
        self.assertEqual(counter.value, 0)


class TestSyncStreamSkipsUnavailableStream(unittest.TestCase):
    """Covers sync_stream(): the exception must be caught and handled
    gracefully so do_sync()'s per-stream loop can continue."""

    @mock.patch('tap_salesforce.salesforce.Salesforce.query',
                side_effect=TapSalesforceStreamUnavailableException("DatacloudCompany: boom"))
    def test_sync_stream_does_not_raise(self, _mock_query):
        """sync_stream() must not propagate the exception -- otherwise
        do_sync()'s per-stream loop (which has no try/except of its own)
        would abort syncing every other selected stream too."""
        sf = _make_sf()
        catalog_entry = {
            "stream": "DatacloudCompany",
            "tap_stream_id": "DatacloudCompany",
            "schema": {},
            "metadata": [{"breadcrumb": [], "metadata": {"replication-key": "LastModifiedDate"}}],
        }
        state = {}

        counter = sync_stream(sf, catalog_entry, state)

        self.assertEqual(counter.value, 0)

    @mock.patch('tap_salesforce.salesforce.Salesforce.query',
                side_effect=TapSalesforceStreamUnavailableException("DatacloudCompany: boom"))
    def test_sync_stream_leaves_bookmark_untouched(self, _mock_query):
        """State for the skipped stream must be left exactly as it was
        before the sync attempt -- not advanced to start_time."""
        sf = _make_sf()
        catalog_entry = {
            "stream": "DatacloudCompany",
            "tap_stream_id": "DatacloudCompany",
            "schema": {},
            "metadata": [{"breadcrumb": [], "metadata": {"replication-key": "LastModifiedDate"}}],
        }
        state = {"bookmarks": {"DatacloudCompany": {"LastModifiedDate": "2020-01-01T00:00:00Z"}}}

        sync_stream(sf, catalog_entry, state)

        self.assertEqual(
            state["bookmarks"]["DatacloudCompany"]["LastModifiedDate"],
            "2020-01-01T00:00:00Z",
        )

    @mock.patch('tap_salesforce.salesforce.Salesforce.query',
                side_effect=TapSalesforceStreamUnavailableException("ExternalEncryptionRootKey: boom"))
    def test_sync_stream_full_table_stream_does_not_emit_activate_version(self, _mock_query):
        sf = _make_sf()
        catalog_entry = {
            "stream": "ExternalEncryptionRootKey",
            "tap_stream_id": "ExternalEncryptionRootKey",
            "schema": {},
            "metadata": [],
        }
        state = {}

        with mock.patch('singer.write_message') as mock_write_message:
            counter = sync_stream(sf, catalog_entry, state)

        mock_write_message.assert_not_called()
        self.assertEqual(counter.value, 0)


if __name__ == "__main__":
    unittest.main()
