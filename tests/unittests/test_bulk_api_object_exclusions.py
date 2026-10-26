"""
Unit tests for Salesforce.get_blacklisted_objects() (static Bulk API
exclusions only) and for the Bulk API's runtime fallback to REST when
Salesforce rejects job creation for a specific object with "is not
supported by the Bulk API".

The previous approach of predicting Bulk-unsupported objects via name
suffixes (*Share, *Feed, *History, *EventRelation) has been replaced:
Salesforce reports the failure itself at job-creation time, before any
data is read, so the tap now catches that specific error and transparently
re-queries the affected stream through the REST API instead of maintaining
a suffix-based exclusion list.
"""
import unittest
from unittest.mock import MagicMock, patch

import requests
from requests.exceptions import HTTPError

from tap_salesforce.salesforce import (UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS,
                                       Salesforce)
from tap_salesforce.salesforce.bulk import (Bulk,
                                            _bulk_unsupported_entity_message,
                                            _query_level_restriction_message)
from tap_salesforce.salesforce.exceptions import (TapSalesforceException,
                                                  TapSalesforceBulkObjectNotSupportedException)

START_DATE = "2022-01-01T00:00:00.000000Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_sf(api_type="BULK"):
    return Salesforce(default_start_date=START_DATE, api_type=api_type)


def make_http_error(status_code, json_body):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body
    return HTTPError(response=response)


# ---------------------------------------------------------------------------
# Tests for Salesforce.get_blacklisted_objects() — BULK API (static list only)
# ---------------------------------------------------------------------------

class TestGetBlacklistedObjectsBulk(unittest.TestCase):
    """get_blacklisted_objects with BULK api_type returns only the static,
    known-unsupported object set; no name-based prediction is applied."""

    def setUp(self):
        self.sf = make_sf(api_type="BULK")

    def test_static_named_objects_excluded(self):
        blacklisted = self.sf.get_blacklisted_objects()
        for obj in UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS:
            self.assertIn(obj, blacklisted)

    def test_supported_objects_not_excluded(self):
        blacklisted = self.sf.get_blacklisted_objects()
        for name in ["Account", "Contact", "Opportunity", "Lead"]:
            self.assertNotIn(name, blacklisted)

    def test_suffix_named_objects_not_predicted(self):
        """Objects matching the old suffix patterns (*Share, *Feed, *History)
        are no longer excluded by name guessing; only the runtime
        error-handling path in Bulk.query() handles them now. (Note:
        EventRelation objects are intentionally excluded from this check —
        AcceptedEventRelation/DeclinedEventRelation/UndecidedEventRelation are
        each listed explicitly, by literal name, in the pre-existing static
        UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS set on master, not via
        suffix prediction.)"""
        blacklisted = self.sf.get_blacklisted_objects()
        for name in ["AccountShare", "CaseFeed", "ContactHistory", "PardotEnvironment__Share"]:
            self.assertNotIn(name, blacklisted)


# ---------------------------------------------------------------------------
# Tests for Salesforce.get_blacklisted_objects() — REST API
# ---------------------------------------------------------------------------

class TestGetBlacklistedObjectsRest(unittest.TestCase):
    """get_blacklisted_objects with REST api_type — bulk-only static exclusions must NOT apply."""

    def setUp(self):
        self.sf = make_sf(api_type="REST")

    def test_static_bulk_objects_not_in_rest_blacklist(self):
        blacklisted = self.sf.get_blacklisted_objects()
        # TaskStatus is bulk-only — it should NOT appear in the REST blacklist
        self.assertNotIn("TaskStatus", blacklisted)


# ---------------------------------------------------------------------------
# Tests for _bulk_unsupported_entity_message()
# ---------------------------------------------------------------------------

class TestBulkUnsupportedEntityMessage(unittest.TestCase):

    def test_detects_unsupported_entity_message(self):
        response = MagicMock()
        response.json.return_value = [
            {"message": "Entity 'AccountShare' is not supported by the Bulk API.",
             "errorCode": "InvalidEntity"}
        ]
        message = _bulk_unsupported_entity_message(response)
        self.assertIsNotNone(message)
        self.assertIn("AccountShare", message)

    def test_is_case_insensitive(self):
        response = MagicMock()
        response.json.return_value = [
            {"message": "Entity 'CaseFeed' Is Not Supported By The Bulk API.",
             "errorCode": "InvalidEntity"}
        ]
        self.assertIsNotNone(_bulk_unsupported_entity_message(response))

    def test_unrelated_invalid_entity_error_returns_none(self):
        """Same errorCode can be used for unrelated issues; only the specific
        message text is a reliable signal."""
        response = MagicMock()
        response.json.return_value = [
            {"message": "Entity 'Foo' is not queryable.", "errorCode": "InvalidEntity"}
        ]
        self.assertIsNone(_bulk_unsupported_entity_message(response))

    def test_none_response_returns_none(self):
        self.assertIsNone(_bulk_unsupported_entity_message(None))

    def test_non_json_response_returns_none(self):
        response = MagicMock()
        response.json.side_effect = ValueError("not json")
        self.assertIsNone(_bulk_unsupported_entity_message(response))

    def test_empty_list_body_returns_none(self):
        response = MagicMock()
        response.json.return_value = []
        self.assertIsNone(_bulk_unsupported_entity_message(response))

    # -----------------------------------------------------------------
    # Bulk 1.0 `/job` endpoint envelope: {"exceptionCode", "exceptionMessage"}.
    # This dict shape -- not the JSON list shape above -- is what Salesforce
    # actually returns for _create_job() job-creation failures in production.
    # -----------------------------------------------------------------

    def test_detects_unsupported_entity_message_dict_envelope(self):
        response = MagicMock()
        response.json.return_value = {
            "exceptionCode": "InvalidEntity",
            "exceptionMessage": "Entity 'AccountShare' is not supported by the Bulk API."
        }
        message = _bulk_unsupported_entity_message(response)
        self.assertIsNotNone(message)
        self.assertIn("AccountShare", message)

    def test_dict_envelope_is_case_insensitive(self):
        response = MagicMock()
        response.json.return_value = {
            "exceptionCode": "InvalidEntity",
            "exceptionMessage": "Entity 'CaseFeed' Is Not Supported By The Bulk API."
        }
        self.assertIsNotNone(_bulk_unsupported_entity_message(response))

    def test_dict_envelope_unrelated_invalid_entity_error_returns_none(self):
        response = MagicMock()
        response.json.return_value = {
            "exceptionCode": "InvalidEntity",
            "exceptionMessage": "Entity 'Foo' is not queryable."
        }
        self.assertIsNone(_bulk_unsupported_entity_message(response))

    def test_dict_envelope_missing_exception_message_returns_none(self):
        response = MagicMock()
        response.json.return_value = {"exceptionCode": "InvalidEntity"}
        self.assertIsNone(_bulk_unsupported_entity_message(response))


# ---------------------------------------------------------------------------
# Tests for _query_level_restriction_message()
#
# Discovered via manual probing of the Bulk API 2.0 "jobs/query" endpoint
# across a batch of known-tricky sobjects: besides "not supported by the Bulk
# API", Salesforce also rejects job creation with "does not support query"
# (e.g. AggregateResult) and "requires a filter by Id" (objects with a
# mandatory WHERE clause). Both share the same SOQL query as REST
# (Salesforce._build_query_string), so they are query-level restrictions, not
# Bulk-API-specific ones, and must NOT trigger a REST fallback.
# ---------------------------------------------------------------------------

class TestQueryLevelRestrictionMessage(unittest.TestCase):

    def test_detects_does_not_support_query(self):
        response = MagicMock()
        response.json.return_value = [
            {"message": "AggregateResult does not support query",
             "errorCode": "INVALID_TYPE_FOR_OPERATION"}
        ]
        message = _query_level_restriction_message(response)
        self.assertIsNotNone(message)
        self.assertIn("AggregateResult", message)

    def test_detects_requires_a_filter_by(self):
        response = MagicMock()
        response.json.return_value = [
            {"message": "PartnerRole requires a filter by Id, Name or ownerId"
                        " using the equals operator",
             "errorCode": "INVALID_FIELD"}
        ]
        message = _query_level_restriction_message(response)
        self.assertIsNotNone(message)
        self.assertIn("requires a filter by", message)

    def test_is_case_insensitive(self):
        response = MagicMock()
        response.json.return_value = [
            {"message": "Foo Does Not Support Query", "errorCode": "INVALID_TYPE_FOR_OPERATION"}
        ]
        self.assertIsNotNone(_query_level_restriction_message(response))

    def test_bulk_unsupported_message_not_matched(self):
        """The Bulk-specific pattern must only be matched by
        _bulk_unsupported_entity_message, not this classifier."""
        response = MagicMock()
        response.json.return_value = [
            {"message": "Entity 'AccountShare' is not supported by the Bulk API.",
             "errorCode": "InvalidEntity"}
        ]
        self.assertIsNone(_query_level_restriction_message(response))

    def test_unrelated_error_returns_none(self):
        response = MagicMock()
        response.json.return_value = [{"message": "Some other error", "errorCode": "UNKNOWN"}]
        self.assertIsNone(_query_level_restriction_message(response))

    def test_none_response_returns_none(self):
        self.assertIsNone(_query_level_restriction_message(None))


# ---------------------------------------------------------------------------
# Tests for Bulk._create_job() raising TapSalesforceBulkObjectNotSupportedException
# ---------------------------------------------------------------------------

class TestCreateJobUnsupportedEntity(unittest.TestCase):

    def setUp(self):
        self.sf = MagicMock()
        self.sf.instance_url = "https://na1.salesforce.com"
        # csv.field_size_limit(sys.maxsize) overflows a C long on Windows;
        # that is unrelated to the behavior under test here.
        with patch("csv.field_size_limit"):
            self.bulk = Bulk(self.sf)

    def test_create_job_raises_dedicated_exception_on_unsupported_entity(self):
        self.sf._make_request.side_effect = make_http_error(
            400,
            {"exceptionCode": "InvalidEntity",
             "exceptionMessage": "Entity 'AccountShare' is not supported by the Bulk API."})

        with self.assertRaises(TapSalesforceBulkObjectNotSupportedException) as ctx:
            self.bulk._create_job({"stream": "AccountShare"})
        self.assertIn("AccountShare", str(ctx.exception))

    def test_create_job_reraises_unrelated_http_error(self):
        self.sf._make_request.side_effect = make_http_error(
            400, {"exceptionCode": "InvalidEntity",
                  "exceptionMessage": "Entity 'Foo' is not queryable."})

        with self.assertRaises(HTTPError):
            self.bulk._create_job({"stream": "Foo"})

    def test_create_job_reraises_error_with_no_response(self):
        self.sf._make_request.side_effect = requests.exceptions.ConnectionError("boom")

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.bulk._create_job({"stream": "Account"})

    def test_create_job_raises_tap_exception_on_does_not_support_query(self):
        """Query-level restrictions must raise the generic TapSalesforceException
        (fatal, not caught by query()'s REST-fallback handler) -- REST would
        fail identically since it builds the same SOQL query."""
        self.sf._make_request.side_effect = make_http_error(
            400, {"exceptionCode": "INVALID_TYPE_FOR_OPERATION",
                  "exceptionMessage": "AggregateResult does not support query"})

        with self.assertRaises(TapSalesforceException) as ctx:
            self.bulk._create_job({"stream": "AggregateResult"})
        self.assertNotIsInstance(ctx.exception, TapSalesforceBulkObjectNotSupportedException)
        self.assertIn("AggregateResult", str(ctx.exception))

    def test_create_job_raises_tap_exception_on_requires_a_filter_by(self):
        self.sf._make_request.side_effect = make_http_error(
            400, {"exceptionCode": "INVALID_FIELD",
                  "exceptionMessage": "PartnerRole requires a filter by Id"})

        with self.assertRaises(TapSalesforceException) as ctx:
            self.bulk._create_job({"stream": "PartnerRole"})
        self.assertNotIsInstance(ctx.exception, TapSalesforceBulkObjectNotSupportedException)
        self.assertIn("PartnerRole", str(ctx.exception))

    # -----------------------------------------------------------------
    # The list-of-{"message", "errorCode"} shape is retained for backwards
    # compatibility with other Salesforce error responses that use it, even
    # though it is not the shape _create_job()'s Bulk 1.0 `/job` endpoint
    # actually returns.
    # -----------------------------------------------------------------

    def test_create_job_raises_dedicated_exception_on_unsupported_entity_list_envelope(self):
        self.sf._make_request.side_effect = make_http_error(
            400,
            [{"message": "Entity 'AccountShare' is not supported by the Bulk API.",
             "errorCode": "InvalidEntity"}])

        with self.assertRaises(TapSalesforceBulkObjectNotSupportedException) as ctx:
            self.bulk._create_job({"stream": "AccountShare"})
        self.assertIn("AccountShare", str(ctx.exception))


# ---------------------------------------------------------------------------
# Tests for Bulk.query() REST fallback
# ---------------------------------------------------------------------------

class TestBulkQueryRestFallback(unittest.TestCase):

    def setUp(self):
        self.sf = MagicMock()
        self.sf.jobs_completed = 0
        # csv.field_size_limit(sys.maxsize) overflows a C long on Windows;
        # that is unrelated to the behavior under test here.
        with patch("csv.field_size_limit"):
            self.bulk = Bulk(self.sf)

    @patch("tap_salesforce.salesforce.bulk.Rest")
    def test_query_falls_back_to_rest_on_unsupported_entity(self, mock_rest_cls):
        mock_rest_instance = mock_rest_cls.return_value
        mock_rest_instance.query.return_value = iter([{"Id": "1"}, {"Id": "2"}])

        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query",
                          side_effect=TapSalesforceBulkObjectNotSupportedException(
                              "AccountShare: Entity 'AccountShare' is not supported by the Bulk API.")):
            catalog_entry = {"stream": "AccountShare", "tap_stream_id": "AccountShare"}
            records = list(self.bulk.query(catalog_entry, {}))

        self.assertEqual(records, [{"Id": "1"}, {"Id": "2"}])
        mock_rest_cls.assert_called_once_with(self.sf)
        mock_rest_instance.query.assert_called_once_with(catalog_entry, {})

    @patch("tap_salesforce.salesforce.bulk.Rest")
    def test_query_does_not_increment_jobs_completed_on_fallback(self, mock_rest_cls):
        mock_rest_cls.return_value.query.return_value = iter([])

        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query",
                          side_effect=TapSalesforceBulkObjectNotSupportedException("boom")):
            list(self.bulk.query({"stream": "AccountShare"}, {}))

        self.assertEqual(self.sf.jobs_completed, 0)

    def test_query_increments_jobs_completed_on_normal_success(self):
        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query", return_value=iter([{"Id": "1"}])):
            records = list(self.bulk.query({"stream": "Account"}, {}))

        self.assertEqual(records, [{"Id": "1"}])
        self.assertEqual(self.sf.jobs_completed, 1)

    def test_query_other_exceptions_are_not_caught(self):
        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                list(self.bulk.query({"stream": "Account"}, {}))

    def test_query_does_not_fall_back_for_query_level_restrictions(self):
        """A TapSalesforceException raised for a query-level restriction
        (does not support query / requires a filter by) must propagate as a
        fatal error, not trigger the REST fallback -- REST would hit the
        same restriction since it builds the identical SOQL query."""
        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query",
                          side_effect=TapSalesforceException(
                              "AggregateResult: AggregateResult does not support query")):
            with self.assertRaises(TapSalesforceException):
                list(self.bulk.query({"stream": "AggregateResult"}, {}))

    @patch("tap_salesforce.salesforce.bulk.Rest")
    def test_query_does_not_fall_back_to_rest_after_records_already_yielded(self, mock_rest_cls):
        """_create_job() is also invoked from _bulk_query_with_pk_chunking()
        (via _bulk_with_window()), which can yield records from earlier
        completed batches before a later job-creation call fails with
        TapSalesforceBulkObjectNotSupportedException. Falling back to REST
        in that situation would re-query and re-emit those same records as
        duplicates, so query() must re-raise instead of retrying via REST
        once anything has already been yielded for this stream."""

        def fake_bulk_query(_catalog_entry, _state):
            yield {"Id": "1"}
            raise TapSalesforceBulkObjectNotSupportedException(
                "AccountShare: Entity 'AccountShare' is not supported by the Bulk API.")

        with patch.object(self.bulk, "check_bulk_quota_usage"), \
             patch.object(self.bulk, "_bulk_query", side_effect=fake_bulk_query):
            with self.assertRaises(TapSalesforceBulkObjectNotSupportedException):
                list(self.bulk.query({"stream": "AccountShare"}, {}))

        mock_rest_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()

