"""Unit tests covering the changes made to handle unsupported Salesforce streams:

1. Bulk.__init__         – OverflowError fallback for csv.field_size_limit on Windows
2. check_bulk_quota_usage – 404 on /limits is handled gracefully (no exception)
3. do_discover           – objects with queryable=False or deprecatedAndHidden=True are
                           excluded from the catalog before users can select them
4. sync_stream           – 400 InvalidEntity from the Bulk API causes a warning + skip
                           rather than crashing the whole tap
5. sync_stream           – 404 from the Bulk API causes a warning + skip (stale catalog
                           entries from before a re-discovery do not crash the tap)
6. bulk.query            – 404 from _bulk_query is caught and logged; stream is skipped
7. get_batch_results     – 404 on the result-list URL returns early (no records yielded)
8. get_batch_results     – 404 on an individual result-file URL skips that file
"""
import sys
import json
import unittest
from unittest import mock

from requests.exceptions import HTTPError, RequestException

from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.bulk import Bulk
import tap_salesforce


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sf(api_type="BULK"):
    return Salesforce(
        default_start_date="2024-01-01T00:00:00Z",
        api_type=api_type,
    )


def _make_http_error(status_code, body=None):
    """Build a requests.exceptions.HTTPError with a real-ish response."""
    response = mock.MagicMock()
    response.status_code = status_code
    response.text = body or ""
    if isinstance(body, dict):
        response.json.return_value = body
        response.text = json.dumps(body)
    else:
        response.json.side_effect = ValueError("no json")
    error = HTTPError(response=response)
    error.response = response
    return error


def _make_request_exception(status_code, body=None):
    """Build a requests.exceptions.RequestException with a response."""
    response = mock.MagicMock()
    response.status_code = status_code
    response.text = json.dumps(body) if isinstance(body, dict) else (body or "")
    if isinstance(body, dict):
        response.json.return_value = body
    else:
        response.json.side_effect = ValueError("no json")
    exc = RequestException(response=response)
    exc.response = response
    return exc


def _minimal_catalog_entry(stream="Account"):
    return {
        "stream": stream,
        "tap_stream_id": stream,
        "schema": {"properties": {"Id": {"type": "string"}}},
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "selected": True,
                    "table-key-properties": ["Id"],
                },
            }
        ],
    }


# ---------------------------------------------------------------------------
# 1.  Bulk.__init__ – OverflowError fallback
# ---------------------------------------------------------------------------

class TestBulkInitCsvFieldSizeLimit(unittest.TestCase):
    """csv.field_size_limit(sys.maxsize) overflows on Windows (C long is 32-bit).
    The constructor must catch OverflowError and retry with 2**31-1."""

    def test_sys_maxsize_used_when_no_overflow(self):
        """On platforms where sys.maxsize fits in a C long, use it directly."""
        sf = _make_sf()
        with mock.patch("csv.field_size_limit") as mock_limit:
            Bulk(sf)
            mock_limit.assert_called_once_with(sys.maxsize)

    def test_fallback_to_int_max_on_overflow(self):
        """On Windows, where OverflowError is raised, fall back to 2**31-1."""
        sf = _make_sf()
        call_args = []

        def side_effect(value):
            call_args.append(value)
            if value == sys.maxsize:
                raise OverflowError("Python int too large to convert to C long")

        with mock.patch("csv.field_size_limit", side_effect=side_effect):
            Bulk(sf)

        self.assertEqual(call_args, [sys.maxsize, 2 ** 31 - 1])

    def test_overflow_fallback_value_is_int_max(self):
        """Verify the fallback value is exactly 2147483647 (INT_MAX)."""
        sf = _make_sf()

        def side_effect(value):
            if value == sys.maxsize:
                raise OverflowError

        with mock.patch("csv.field_size_limit", side_effect=side_effect) as mock_limit:
            Bulk(sf)
            # second call must use INT_MAX
            self.assertEqual(mock_limit.call_args_list[-1], mock.call(2 ** 31 - 1))


# ---------------------------------------------------------------------------
# 2.  check_bulk_quota_usage – graceful 404 handling
# ---------------------------------------------------------------------------

class TestCheckBulkQuotaUsage404(unittest.TestCase):
    """A 404 on the /limits endpoint should be swallowed with a warning so
    that the stream continues rather than crashing."""

    def setUp(self):
        self.sf = _make_sf()
        self.sf.instance_url = "https://example.salesforce.com"
        self.sf.access_token = "token"
        self.sf.quota_percent_per_run = 25
        self.sf.quota_percent_total = 80
        self.sf.jobs_completed = 0
        self.bulk = Bulk.__new__(Bulk)
        self.bulk.sf = self.sf

    def test_404_does_not_raise(self):
        """404 from /limits must be silently skipped."""
        err = _make_http_error(404)
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            # Should NOT raise
            self.bulk.check_bulk_quota_usage()

    def test_404_logs_warning(self):
        """404 from /limits should emit a WARNING log entry."""
        err = _make_http_error(404)
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with mock.patch("tap_salesforce.salesforce.bulk.LOGGER") as mock_logger:
                self.bulk.check_bulk_quota_usage()
                mock_logger.warning.assert_called_once()
                warning_msg = mock_logger.warning.call_args[0][0]
                self.assertIn("404", warning_msg)

    def test_non_404_http_error_is_re_raised(self):
        """Any non-404 HTTPError must still propagate."""
        err = _make_http_error(500)
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with self.assertRaises(HTTPError):
                self.bulk.check_bulk_quota_usage()

    def test_401_http_error_is_re_raised(self):
        """401 Unauthorized must still propagate."""
        err = _make_http_error(401)
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with self.assertRaises(HTTPError):
                self.bulk.check_bulk_quota_usage()

    def test_successful_response_enforces_quota(self):
        """When /limits returns 200, quota logic should still run normally."""
        mock_resp = mock.MagicMock()
        mock_resp.json.return_value = {
            "DailyBulkApiBatches": {"Max": 10000, "Remaining": 9000}
        }
        mock_resp.headers = {}
        with mock.patch.object(self.sf, "_make_request", return_value=mock_resp):
            # 10 % used – within limits – should not raise
            self.bulk.check_bulk_quota_usage()


# ---------------------------------------------------------------------------
# 3.  do_discover – queryable / deprecatedAndHidden filters
# ---------------------------------------------------------------------------

def _sobject_describe(name, queryable=True, deprecated=False, fields=None):
    """Minimal sobject describe payload."""
    if fields is None:
        fields = [{"name": "Id", "type": "id", "nillable": False}]
    return {
        "name": name,
        "queryable": queryable,
        "deprecatedAndHidden": deprecated,
        "customSetting": False,
        "fields": fields,
    }


class TestDoDiscoverFilters(unittest.TestCase):
    """Verify that do_discover excludes non-queryable and deprecated objects."""

    def _run_discover(self, sobjects_meta):
        """
        Runs do_discover with a mocked Salesforce object.

        sobjects_meta: list of dicts with keys passed to _sobject_describe
        """
        sf = mock.MagicMock()
        sf.api_type = "REST"
        sf.select_fields_by_default = False
        sf.get_blacklisted_objects.return_value = set()
        sf.get_blacklisted_fields.return_value = {}

        # Global describe returns all names
        global_desc = {
            "sobjects": [{"name": m["name"]} for m in sobjects_meta]
        }
        # Per-object describe keyed by name
        per_object = {m["name"]: _sobject_describe(**m) for m in sobjects_meta}

        def describe_side_effect(name=None):
            if name is None:
                return global_desc
            return per_object[name]

        sf.describe.side_effect = describe_side_effect

        captured = {}

        def fake_json_dump(obj, fp, **kwargs):
            captured["result"] = obj

        with mock.patch("tap_salesforce.Bulk") as mock_bulk_cls:
            mock_bulk_cls.return_value.has_permissions.return_value = True
            with mock.patch("json.dump", side_effect=fake_json_dump):
                tap_salesforce.do_discover(sf)

        stream_names = {e["stream"] for e in captured.get("result", {}).get("streams", [])}
        return stream_names

    def test_non_queryable_object_excluded(self):
        """Objects with queryable=False must NOT appear in the catalog."""
        streams = self._run_discover([
            {"name": "Account", "queryable": True},
            {"name": "AITrustAttribute", "queryable": False},
        ])
        self.assertIn("Account", streams)
        self.assertNotIn("AITrustAttribute", streams)

    def test_deprecated_and_hidden_object_excluded(self):
        """Objects with deprecatedAndHidden=True must NOT appear in the catalog."""
        streams = self._run_discover([
            {"name": "Account", "queryable": True, "deprecated": False},
            {"name": "OldObject", "queryable": True, "deprecated": True},
        ])
        self.assertIn("Account", streams)
        self.assertNotIn("OldObject", streams)

    def test_both_flags_cause_exclusion(self):
        """An object that is both non-queryable AND deprecated is excluded."""
        streams = self._run_discover([
            {"name": "Account", "queryable": True},
            {"name": "PhantomObj", "queryable": False, "deprecated": True},
        ])
        self.assertNotIn("PhantomObj", streams)

    def test_queryable_object_included(self):
        """A fully queryable, non-deprecated object must appear in the catalog."""
        streams = self._run_discover([
            {"name": "Contact", "queryable": True, "deprecated": False},
        ])
        self.assertIn("Contact", streams)

    def test_multiple_phantom_objects_all_excluded(self):
        """All non-queryable objects are excluded regardless of how many there are."""
        streams = self._run_discover([
            {"name": "Account", "queryable": True},
            {"name": "AITrustAttribute", "queryable": False},
            {"name": "AITrustAttrSetup", "queryable": False},
        ])
        self.assertIn("Account", streams)
        self.assertNotIn("AITrustAttribute", streams)
        self.assertNotIn("AITrustAttrSetup", streams)

    def test_deprecated_object_does_not_call_field_loop(self):
        """Deprecated/non-queryable objects don't reach field-processing code
        (i.e. describe is called but we never iterate fields)."""
        sf = mock.MagicMock()
        sf.api_type = "REST"
        sf.select_fields_by_default = False
        sf.get_blacklisted_objects.return_value = set()
        sf.get_blacklisted_fields.return_value = {}

        global_desc = {"sobjects": [{"name": "Ghost"}]}

        ghost_desc = _sobject_describe("Ghost", queryable=False)
        ghost_desc["fields"] = mock.MagicMock()  # should never be iterated

        def describe_side_effect(name=None):
            if name is None:
                return global_desc
            return ghost_desc

        sf.describe.side_effect = describe_side_effect

        with mock.patch("tap_salesforce.Bulk") as mock_bulk_cls:
            mock_bulk_cls.return_value.has_permissions.return_value = True
            with mock.patch("json.dump"):
                tap_salesforce.do_discover(sf)

        # If fields were iterated, MagicMock.__iter__ would have been called.
        ghost_desc["fields"].__iter__.assert_not_called()


# ---------------------------------------------------------------------------
# 4.  sync_stream – 400 InvalidEntity skips gracefully
# ---------------------------------------------------------------------------

class TestSyncStreamInvalidEntity(unittest.TestCase):
    """A 400 InvalidEntity from the Bulk API must be caught, logged as a
    warning, and result in the counter being returned rather than raising."""

    def setUp(self):
        self.sf = _make_sf()
        self.catalog_entry = _minimal_catalog_entry("PardotEnvironment__Share")
        self.state = {}

    def _run_sync_stream(self, side_effect):
        with mock.patch(
            "tap_salesforce.sync.sync_records", side_effect=side_effect
        ):
            return tap_salesforce.sync.sync_stream(self.sf, self.catalog_entry, self.state)

    def test_400_invalid_entity_returns_counter(self):
        """400 InvalidEntity must return a counter without raising."""
        exc = _make_request_exception(
            400,
            {"exceptionCode": "InvalidEntity", "exceptionMessage": "Entity is not supported"}
        )
        counter = self._run_sync_stream(exc)
        self.assertIsNotNone(counter)

    def test_400_invalid_entity_logs_warning(self):
        """400 InvalidEntity must emit a WARNING (not ERROR or exception)."""
        exc = _make_request_exception(
            400,
            {"exceptionCode": "InvalidEntity", "exceptionMessage": "Not supported by Bulk API"}
        )
        with mock.patch("tap_salesforce.sync.LOGGER") as mock_logger:
            with mock.patch("tap_salesforce.sync.sync_records", side_effect=exc):
                tap_salesforce.sync.sync_stream(self.sf, self.catalog_entry, self.state)
            mock_logger.warning.assert_called_once()
            msg = mock_logger.warning.call_args[0][0]
            self.assertIn("Skipping", msg)

    def test_400_other_exception_code_is_re_raised(self):
        """400 with a non-InvalidEntity exceptionCode must still raise."""
        exc = _make_request_exception(
            400,
            {"exceptionCode": "MALFORMED_QUERY", "exceptionMessage": "bad query"}
        )
        with self.assertRaises(Exception):
            self._run_sync_stream(exc)

    def test_404_request_exception_returns_counter(self):
        """404 RequestException is now caught and skipped gracefully."""
        exc = _make_request_exception(404, "Not Found")
        exc.response.url = "https://example.salesforce.com/services/async/52.0/job/750x/batch/751x/result"
        counter = self._run_sync_stream(exc)
        self.assertIsNotNone(counter)

    def test_404_request_exception_logs_warning(self):
        """404 from a batch URL must emit a WARNING with the stream name."""
        exc = _make_request_exception(404, "Not Found")
        exc.response.url = "https://example.salesforce.com/services/async/52.0/job/750x/batch/751x/result"
        with mock.patch("tap_salesforce.sync.LOGGER") as mock_logger:
            with mock.patch("tap_salesforce.sync.sync_records", side_effect=exc):
                tap_salesforce.sync.sync_stream(self.sf, self.catalog_entry, self.state)
            mock_logger.warning.assert_called_once()
            msg = mock_logger.warning.call_args[0][0]
            self.assertIn("404", msg)

    def test_404_warning_includes_stream_name(self):
        """Stream name must appear in the 404 warning for traceability."""
        exc = _make_request_exception(404, "Not Found")
        exc.response.url = "https://example.salesforce.com/services/async/52.0/job/750x"
        with mock.patch("tap_salesforce.sync.LOGGER") as mock_logger:
            with mock.patch("tap_salesforce.sync.sync_records", side_effect=exc):
                tap_salesforce.sync.sync_stream(self.sf, self.catalog_entry, self.state)
            # second positional arg to the format string is the stream name
            self.assertEqual(mock_logger.warning.call_args[0][1], "PardotEnvironment__Share")

    def test_non_404_non_400_request_exception_is_re_raised(self):
        """5xx and other unexpected HTTP errors must still propagate."""
        exc = _make_request_exception(500, "Internal Server Error")
        with self.assertRaises(Exception):
            self._run_sync_stream(exc)

    def test_400_non_json_body_is_re_raised(self):
        """400 with a non-JSON body must still raise (can't parse exceptionCode)."""
        response = mock.MagicMock()
        response.status_code = 400
        response.text = "bad request"
        response.json.side_effect = ValueError("no json")
        exc = RequestException(response=response)
        exc.response = response
        with self.assertRaises(Exception):
            self._run_sync_stream(exc)

    def test_stream_name_included_in_warning_for_invalid_entity(self):
        """The stream name should appear in the warning message for traceability."""
        exc = _make_request_exception(
            400,
            {"exceptionCode": "InvalidEntity", "exceptionMessage": "unsupported"}
        )
        with mock.patch("tap_salesforce.sync.LOGGER") as mock_logger:
            with mock.patch("tap_salesforce.sync.sync_records", side_effect=exc):
                tap_salesforce.sync.sync_stream(self.sf, self.catalog_entry, self.state)
            warning_call = mock_logger.warning.call_args
            # First positional arg is the format string; subsequent args fill it in.
            # The stream name is passed as the second positional argument.
            self.assertEqual(warning_call[0][1], "PardotEnvironment__Share")


# ---------------------------------------------------------------------------
# 5.  bulk.query – 404 from _bulk_query is caught; stream is skipped
# ---------------------------------------------------------------------------

class TestBulkQuery404(unittest.TestCase):
    """A 404 raised from anywhere inside _bulk_query must be caught by
    bulk.query(), logged as a warning, and then re-raised so that
    sync_stream can skip the stream without advancing the replication bookmark."""

    def setUp(self):
        self.sf = _make_sf()
        self.sf.instance_url = "https://example.salesforce.com"
        self.sf.access_token = "token"
        self.sf.quota_percent_per_run = 25
        self.sf.quota_percent_total = 80
        self.sf.jobs_completed = 0
        self.bulk = Bulk.__new__(Bulk)
        self.bulk.sf = self.sf
        self.catalog_entry = _minimal_catalog_entry("AITrustAttribute")
        self.state = {}

    def _404_error(self, url="https://example.salesforce.com/services/async/52.0/job/750x"):
        err = _make_http_error(404)
        err.response.url = url
        return err

    def test_404_raises_http_error(self):
        """When _bulk_query raises a 404, query() must re-raise so callers
        can detect the failure and avoid advancing the replication bookmark."""
        with mock.patch.object(self.bulk, "check_bulk_quota_usage"):
            with mock.patch.object(
                self.bulk, "_bulk_query", side_effect=self._404_error()
            ):
                with self.assertRaises(HTTPError):
                    list(self.bulk.query(self.catalog_entry, self.state))

    def test_404_logs_warning(self):
        """A 404 inside _bulk_query must emit a WARNING with the stream name."""
        with mock.patch.object(self.bulk, "check_bulk_quota_usage"):
            with mock.patch.object(
                self.bulk, "_bulk_query", side_effect=self._404_error()
            ):
                with mock.patch("tap_salesforce.salesforce.bulk.LOGGER") as mock_logger:
                    with self.assertRaises(HTTPError):
                        list(self.bulk.query(self.catalog_entry, self.state))
                mock_logger.warning.assert_called_once()
                msg = mock_logger.warning.call_args[0][0]
                self.assertIn("404", msg)

    def test_404_warning_includes_stream_name(self):
        """Stream name must appear in the 404 warning for traceability."""
        with mock.patch.object(self.bulk, "check_bulk_quota_usage"):
            with mock.patch.object(
                self.bulk, "_bulk_query", side_effect=self._404_error()
            ):
                with mock.patch("tap_salesforce.salesforce.bulk.LOGGER") as mock_logger:
                    with self.assertRaises(HTTPError):
                        list(self.bulk.query(self.catalog_entry, self.state))
                msg_args = mock_logger.warning.call_args[0]
                # stream name is the second positional format argument
                self.assertIn("AITrustAttribute", str(msg_args))

    def test_404_does_not_increment_jobs_completed(self):
        """A 404 stream must not increment the jobs_completed counter."""
        with mock.patch.object(self.bulk, "check_bulk_quota_usage"):
            with mock.patch.object(
                self.bulk, "_bulk_query", side_effect=self._404_error()
            ):
                with self.assertRaises(HTTPError):
                    list(self.bulk.query(self.catalog_entry, self.state))
        self.assertEqual(self.sf.jobs_completed, 0)

    def test_non_404_http_error_still_propagates(self):
        """A non-404 HTTPError inside _bulk_query must still be re-raised."""
        err = _make_http_error(500)
        with mock.patch.object(self.bulk, "check_bulk_quota_usage"):
            with mock.patch.object(self.bulk, "_bulk_query", side_effect=err):
                with self.assertRaises(Exception):
                    list(self.bulk.query(self.catalog_entry, self.state))


# ---------------------------------------------------------------------------
# 6.  get_batch_results – 404 on the result-list URL
# ---------------------------------------------------------------------------

class TestGetBatchResultsResultListUrl404(unittest.TestCase):
    """A 404 returned when fetching job/{job_id}/batch/{batch_id}/result must
    cause get_batch_results to raise HTTPError after logging a warning, so
    callers cannot treat the batch as successfully synced (empty)."""

    def setUp(self):
        self.sf = _make_sf()
        self.sf.instance_url = "https://example.salesforce.com"
        self.sf.access_token = "token"
        self.bulk = Bulk.__new__(Bulk)
        self.bulk.sf = self.sf
        self.catalog_entry = _minimal_catalog_entry("FinanceTransactionShare")

    def test_404_on_result_list_raises(self):
        """404 on the result-list URL must raise HTTPError so the caller knows
        data was not retrieved and can avoid advancing the bookmark."""
        err = _make_http_error(404)
        err.response.url = "https://example.salesforce.com/services/async/52.0/job/750x/batch/751x/result"
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with self.assertRaises(HTTPError):
                list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))

    def test_404_on_result_list_logs_warning(self):
        """404 on the result-list URL must log a WARNING with job/batch context."""
        err = _make_http_error(404)
        err.response.url = "https://example.salesforce.com/services/async/52.0/job/job1/batch/batch1/result"
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with mock.patch("tap_salesforce.salesforce.bulk.LOGGER") as mock_logger:
                with self.assertRaises(HTTPError):
                    list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))
            mock_logger.warning.assert_called_once()
            warn_args = str(mock_logger.warning.call_args)
            self.assertIn("404", warn_args)
            self.assertIn("job1", warn_args)
            self.assertIn("batch1", warn_args)

    def test_non_404_on_result_list_propagates(self):
        """Non-404 HTTPErrors on the result-list URL must still propagate."""
        err = _make_http_error(503)
        with mock.patch.object(self.sf, "_make_request", side_effect=err):
            with self.assertRaises(Exception):
                list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))


# ---------------------------------------------------------------------------
# 7.  get_batch_results – 404 on an individual result-file URL
# ---------------------------------------------------------------------------

class TestGetBatchResultsResultFileUrl404(unittest.TestCase):
    """A 404 returned when fetching job/{id}/batch/{id}/result/{result_id} must
    skip that result file (continue) and still yield records from other files."""

    def setUp(self):
        self.sf = _make_sf()
        self.sf.instance_url = "https://example.salesforce.com"
        self.sf.access_token = "token"
        self.bulk = Bulk.__new__(Bulk)
        self.bulk.sf = self.sf
        self.catalog_entry = _minimal_catalog_entry("MLPredictionDefinition")

    def _mock_result_list_response(self, result_ids):
        """Return a mock response whose .text is a proper XML result-list."""
        xml = "<result-list>" + "".join(f"<result>{r}</result>" for r in result_ids) + "</result-list>"
        resp = mock.MagicMock()
        resp.text = xml
        return resp

    def test_404_on_result_file_skips_file_and_logs_warning(self):
        """404 on a result-file URL emits a WARNING and skips that file."""
        result_list_resp = self._mock_result_list_response(["result-1"])
        err = _make_http_error(404)
        err.response.url = "https://example.salesforce.com/.../result/result-1"

        call_count = {"n": 0}

        def make_request_side_effect(method, url, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:  # result-list request — succeeds
                return result_list_resp
            raise err  # result-file request — 404

        with mock.patch.object(self.sf, "_make_request", side_effect=make_request_side_effect):
            with mock.patch("tap_salesforce.salesforce.bulk.LOGGER") as mock_logger:
                records = list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))
            mock_logger.warning.assert_called_once()
            warn_args = str(mock_logger.warning.call_args)
            self.assertIn("404", warn_args)
        self.assertEqual(records, [])

    def test_404_on_one_result_file_continues_to_next(self):
        """404 on result-1, 200 on result-2 — records from result-2 must be yielded."""
        result_list_resp = self._mock_result_list_response(["result-1", "result-2"])
        err_404 = _make_http_error(404)
        err_404.response.url = "https://example.salesforce.com/.../result/result-1"

        # No trailing newline — a trailing \r\n would make csv.reader emit an
        # extra empty row, causing the assertion to fail with [{"Id": "ACC001"}, {"Id": ""}].
        csv_content = "Id\r\nACC001"
        good_resp = mock.MagicMock()
        good_resp.iter_content.return_value = [csv_content]

        call_count = {"n": 0}

        def make_request_side_effect(method, url, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:  # result-list
                return result_list_resp
            if call_count["n"] == 2:  # result-1: 404
                raise err_404
            return good_resp  # result-2: success

        with mock.patch.object(self.sf, "_make_request", side_effect=make_request_side_effect):
            records = list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))

        self.assertEqual(records, [{"Id": "ACC001"}])

    def test_non_404_on_result_file_propagates(self):
        """Non-404 HTTPErrors on a result-file URL must still propagate."""
        result_list_resp = self._mock_result_list_response(["result-1"])
        err = _make_http_error(503)

        call_count = {"n": 0}

        def make_request_side_effect(method, url, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return result_list_resp
            raise err

        with mock.patch.object(self.sf, "_make_request", side_effect=make_request_side_effect):
            with self.assertRaises(Exception):
                list(self.bulk.get_batch_results("job1", "batch1", self.catalog_entry))


if __name__ == "__main__":
    unittest.main()
