"""
Unit tests for the discovery-time accessibility probe used for objects in
ORG_ENVIRONMENT_DEPENDENT_BULK_OBJECTS (currently DatacloudCompany and
ExternalEncryptionRootKey).

These objects' Bulk API queries are known to sometimes fail for reasons tied
to the org's own licensing/environment (Data.com license, BYOK key
management connectivity) rather than a universal Bulk API restriction, so
they are intentionally NOT in the static UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS
blacklist (that would hide them from every org, including ones where they
ARE queryable). Instead, Salesforce.check_org_environment_object_accessible()
probes each object individually at discovery time via a minimal REST query,
and do_discover() excludes it from this run's catalog only when the probe
confirms the org-environment restriction.
"""
import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import HTTPError

import tap_salesforce
from tap_salesforce import do_discover
from tap_salesforce.salesforce import (ORG_ENVIRONMENT_DEPENDENT_BULK_OBJECTS,
                                       Salesforce)

START_DATE = "2022-01-01T00:00:00.000000Z"


def make_sf(api_type="BULK"):
    return Salesforce(default_start_date=START_DATE, api_type=api_type)


def make_http_error(status_code, json_body):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body
    return HTTPError(response=response)


# ---------------------------------------------------------------------------
# Tests for Salesforce.check_org_environment_object_accessible()
# ---------------------------------------------------------------------------

class TestCheckOrgEnvironmentObjectAccessible(unittest.TestCase):

    def setUp(self):
        self.sf = make_sf(api_type="BULK")
        self.sf.instance_url = "https://na1.salesforce.com"

    def test_successful_query_returns_true(self):
        with patch.object(self.sf, '_make_request') as mock_request:
            mock_request.return_value = MagicMock()
            self.assertTrue(self.sf.check_org_environment_object_accessible("DatacloudCompany"))

    def test_successful_query_issues_minimal_select_id_limit_1(self):
        with patch.object(self.sf, '_make_request') as mock_request:
            mock_request.return_value = MagicMock()
            self.sf.check_org_environment_object_accessible("DatacloudCompany")

            _args, kwargs = mock_request.call_args
            self.assertEqual(kwargs['params']['q'], "SELECT Id FROM DatacloudCompany LIMIT 1")
            self.assertIn("queryAll", mock_request.call_args[0][1])

    def test_datacloud_api_disabled_exception_returns_false(self):
        self.sf._make_request = MagicMock(
            side_effect=make_http_error(
                400,
                [{"message": ("Failed to process query: DATACLOUD_API_DISABLED_EXCEPTION: "
                              "Your organization doesn't have permission to access the Data.com API"),
                 "errorCode": "INVALID_TYPE_FOR_OPERATION"}]))

        self.assertFalse(self.sf.check_org_environment_object_accessible("DatacloudCompany"))

    def test_external_object_connectivity_failure_returns_false(self):
        self.sf._make_request = MagicMock(
            side_effect=make_http_error(
                400,
                [{"message": ("Failed to process query: EXTERNAL_OBJECT_EXCEPTION: We can't "
                              "connect to your key management service."),
                 "errorCode": "EXTERNAL_OBJECT_EXCEPTION"}]))

        self.assertFalse(self.sf.check_org_environment_object_accessible("ExternalEncryptionRootKey"))

    def test_dict_envelope_error_body_is_handled(self):
        """_create_job()'s Bulk 1.0 /job endpoint uses a dict envelope; the
        REST queryAll endpoint used here may return either shape depending
        on the failure, so both must be handled."""
        self.sf._make_request = MagicMock(
            side_effect=make_http_error(
                400,
                {"exceptionCode": "INVALID_TYPE_FOR_OPERATION",
                 "exceptionMessage": "Failed to process query: DATACLOUD_API_DISABLED_EXCEPTION: nope"}))

        self.assertFalse(self.sf.check_org_environment_object_accessible("DatacloudCompany"))

    def test_unrelated_error_fails_open_and_returns_true(self):
        """An unrecognized failure (e.g. a transient/unrelated error) must
        default to True -- a flaky probe must never silently exclude a
        stream from discovery for every org."""
        self.sf._make_request = MagicMock(
            side_effect=make_http_error(
                500, [{"message": "Some unrelated internal error", "errorCode": "UNKNOWN_EXCEPTION"}]))

        self.assertTrue(self.sf.check_org_environment_object_accessible("DatacloudCompany"))

    def test_non_json_error_body_fails_open_and_returns_true(self):
        response = MagicMock()
        response.status_code = 500
        response.json.side_effect = ValueError("not json")
        response.text = "Internal Server Error"
        self.sf._make_request = MagicMock(side_effect=HTTPError(response=response))

        self.assertTrue(self.sf.check_org_environment_object_accessible("DatacloudCompany"))

    def test_error_with_no_response_fails_open_and_returns_true(self):
        self.sf._make_request = MagicMock(side_effect=HTTPError("boom"))

        self.assertTrue(self.sf.check_org_environment_object_accessible("DatacloudCompany"))


# ---------------------------------------------------------------------------
# Tests for do_discover()'s use of the org-environment accessibility probe
# ---------------------------------------------------------------------------

def _make_describe_response(object_names):
    return {"sobjects": [{"name": name} for name in object_names]}


def _make_field_description(object_name):
    return {
        "fields": [
            {"name": "Id", "type": "id", "nillable": False},
            {"name": "Name", "type": "string", "nillable": True},
        ]
    }


class TestDoDiscoverOrgEnvironmentFiltering(unittest.TestCase):
    """do_discover() must only exclude ORG_ENVIRONMENT_DEPENDENT_BULK_OBJECTS
    from the catalog when the accessibility probe confirms this org cannot
    query them; otherwise they must be discovered normally alongside every
    other stream."""

    def setUp(self):
        self.sf = make_sf(api_type="BULK")
        self.sf.instance_url = "https://na1.salesforce.com"
        self.object_names = ["Account", "DatacloudCompany", "ExternalEncryptionRootKey"]
        self.sf.describe = MagicMock(return_value=_make_describe_response(self.object_names))
        self.sf.get_blacklisted_objects = MagicMock(return_value=set())
        self.sf.get_blacklisted_fields = MagicMock(return_value={})
        self.sf.select_fields_by_default = False
        self.sf.batch_describe = MagicMock(
            side_effect=lambda names: {name: _make_field_description(name) for name in names})

    def _run_discover_and_capture_streams(self):
        captured = {}
        # csv.field_size_limit(sys.maxsize) overflows a C long on Windows when
        # do_discover() constructs a Bulk(sf) instance to check Bulk API
        # permissions, and has_permissions() would otherwise make a real
        # network call; both are unrelated to the behavior under test here.
        with patch("csv.field_size_limit"), \
             patch("tap_salesforce.salesforce.bulk.Bulk.has_permissions", return_value=True), \
             patch("json.dump", side_effect=lambda result, *a, **k: captured.update(result)):
            do_discover(self.sf)
        return {entry['stream'] for entry in captured['streams']}

    def test_both_objects_included_when_accessible(self):
        self.sf.check_org_environment_object_accessible = MagicMock(return_value=True)

        streams = self._run_discover_and_capture_streams()

        self.assertIn("DatacloudCompany", streams)
        self.assertIn("ExternalEncryptionRootKey", streams)
        self.assertEqual(self.sf.check_org_environment_object_accessible.call_count, 2)

    def test_object_excluded_when_probe_reports_inaccessible(self):
        def side_effect(name):
            return name != "DatacloudCompany"
        self.sf.check_org_environment_object_accessible = MagicMock(side_effect=side_effect)

        streams = self._run_discover_and_capture_streams()

        self.assertNotIn("DatacloudCompany", streams)
        self.assertIn("ExternalEncryptionRootKey", streams)
        self.assertIn("Account", streams)

    def test_both_objects_excluded_when_both_inaccessible(self):
        self.sf.check_org_environment_object_accessible = MagicMock(return_value=False)

        streams = self._run_discover_and_capture_streams()

        self.assertNotIn("DatacloudCompany", streams)
        self.assertNotIn("ExternalEncryptionRootKey", streams)
        self.assertIn("Account", streams)

    def test_probe_not_called_for_unrelated_objects(self):
        self.sf.check_org_environment_object_accessible = MagicMock(return_value=True)

        self._run_discover_and_capture_streams()

        probed_names = {call.args[0] for call in self.sf.check_org_environment_object_accessible.call_args_list}
        self.assertEqual(probed_names, ORG_ENVIRONMENT_DEPENDENT_BULK_OBJECTS)

    def test_probe_not_invoked_for_rest_api_type(self):
        """The probe is Bulk-specific: under REST these objects aren't
        statically blacklisted at all, so no probe is needed."""
        self.sf.api_type = tap_salesforce.salesforce.REST_API_TYPE
        self.sf.check_org_environment_object_accessible = MagicMock(return_value=False)

        streams = self._run_discover_and_capture_streams()

        self.sf.check_org_environment_object_accessible.assert_not_called()
        self.assertIn("DatacloudCompany", streams)
        self.assertIn("ExternalEncryptionRootKey", streams)


if __name__ == "__main__":
    unittest.main()
