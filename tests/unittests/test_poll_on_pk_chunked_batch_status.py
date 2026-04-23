import unittest
from unittest import mock
from tap_salesforce import Salesforce
from tap_salesforce.salesforce import Bulk
from tap_salesforce.salesforce.exceptions import TapSalesforceException
import tap_salesforce.salesforce.bulk as bulk_module


def _make_bulk():
    sf = Salesforce(default_start_date='2019-01-01T00:00:00Z', api_type="BULK")
    return Bulk(sf)


def _batches(*states):
    """Return a list of batch dicts with the given states."""
    return [{'id': str(i), 'state': state, 'stateMessage': None}
            for i, state in enumerate(states)]


class TestPollOnPkChunkedBatchStatusMaxPollsExhausted(unittest.TestCase):
    """TapSalesforceException must be raised when max_polls is exhausted with
    batches still pending, so callers cannot treat partial data as success."""

    @mock.patch('tap_salesforce.salesforce.bulk.time.sleep')
    @mock.patch('tap_salesforce.salesforce.bulk.PK_CHUNKED_MAX_POLLS', 3)
    @mock.patch('tap_salesforce.salesforce.bulk.PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP', 0)
    def test_raises_when_max_polls_exhausted(self, _mock_sleep):
        bulk = _make_bulk()

        # _get_batches always returns a Queued batch so has_pending never clears
        with mock.patch.object(
            bulk, '_get_batches',
            return_value=_batches("Queued", "Completed")
        ):
            with self.assertRaises(TapSalesforceException) as ctx:
                bulk._poll_on_pk_chunked_batch_status("test_job_id")

        self.assertIn("Max poll attempts", str(ctx.exception))

    @mock.patch('tap_salesforce.salesforce.bulk.time.sleep')
    @mock.patch('tap_salesforce.salesforce.bulk.PK_CHUNKED_MAX_POLLS', 3)
    @mock.patch('tap_salesforce.salesforce.bulk.PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP', 0)
    def test_exception_message_contains_job_id_and_poll_count(self, _mock_sleep):
        bulk = _make_bulk()

        with mock.patch.object(
            bulk, '_get_batches',
            return_value=_batches("Queued")
        ):
            with self.assertRaises(TapSalesforceException) as ctx:
                bulk._poll_on_pk_chunked_batch_status("my_job_123")

        msg = str(ctx.exception)
        self.assertIn("3", msg)            # max_polls ceiling
        self.assertIn("my_job_123", msg)   # job_id


class TestPollOnPkChunkedBatchStatusNormal(unittest.TestCase):
    """No exception when batches complete before max_polls."""

    @mock.patch('tap_salesforce.salesforce.bulk.time.sleep')
    @mock.patch('tap_salesforce.salesforce.Bulk._get_batches')
    def test_no_exception_when_batches_complete_before_max_polls(self, mock_get_batches, _mock_sleep):
        # First call: Queued; second call (after one poll): Completed
        mock_get_batches.side_effect = [
            _batches("Queued"),
            _batches("Completed"),
        ]

        bulk = _make_bulk()
        result = bulk._poll_on_pk_chunked_batch_status("job_normal")

        self.assertEqual(result['completed'], ['0'])
        self.assertEqual(result['failed'], {})

    @mock.patch('tap_salesforce.salesforce.bulk.time.sleep')
    @mock.patch('tap_salesforce.salesforce.Bulk._get_batches')
    def test_returns_correct_completed_and_failed_batches(self, mock_get_batches, _mock_sleep):
        mock_get_batches.return_value = [
            {'id': 'a', 'state': 'Completed', 'stateMessage': None},
            {'id': 'b', 'state': 'Failed', 'stateMessage': 'QUERY_TIMEOUT'},
        ]

        bulk = _make_bulk()
        result = bulk._poll_on_pk_chunked_batch_status("job_mixed")

        self.assertIn('a', result['completed'])
        self.assertIn('b', result['failed'])
        self.assertEqual(result['failed']['b'], 'QUERY_TIMEOUT')

    @mock.patch('tap_salesforce.salesforce.bulk.time.sleep')
    @mock.patch('tap_salesforce.salesforce.Bulk._get_batches')
    def test_no_pending_batches_skips_loop(self, mock_get_batches, _mock_sleep):
        """If initial batch fetch has no pending batches, loop is never entered."""
        mock_get_batches.return_value = _batches("Completed", "Completed")

        bulk = _make_bulk()
        result = bulk._poll_on_pk_chunked_batch_status("job_complete")

        # sleep should never have been called since no batches were pending
        _mock_sleep.assert_not_called()
        self.assertEqual(len(result['completed']), 2)


if __name__ == '__main__':
    unittest.main()
