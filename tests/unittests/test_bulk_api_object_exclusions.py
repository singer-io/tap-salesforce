"""
Unit tests for Bulk API object exclusion logic introduced to filter objects
whose names match the *Share, *Feed, *History, or *EventRelation suffixes at
discovery time, preventing them from being offered to users when they would
fail at Bulk API prepare-time.
"""
import unittest

from tap_salesforce.salesforce import (UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS,
                                       Salesforce, is_unsupported_bulk_object)

START_DATE = "2022-01-01T00:00:00.000000Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_sf(api_type="BULK"):
    return Salesforce(default_start_date=START_DATE, api_type=api_type)


# ---------------------------------------------------------------------------
# Tests for is_unsupported_bulk_object()
# ---------------------------------------------------------------------------

class TestIsUnsupportedBulkObject(unittest.TestCase):
    """Tests for the is_unsupported_bulk_object helper function."""

    # --- suffix: Share ---
    def test_share_suffix_standard_object(self):
        self.assertTrue(is_unsupported_bulk_object("AccountShare"))

    def test_share_suffix_custom_object(self):
        self.assertTrue(is_unsupported_bulk_object("PardotEnvironment__Share"))

    # --- suffix: Feed ---
    def test_feed_suffix_standard_object(self):
        self.assertTrue(is_unsupported_bulk_object("CaseFeed"))

    def test_feed_suffix_custom_object(self):
        self.assertTrue(is_unsupported_bulk_object("MyObject__Feed"))

    # --- suffix: History ---
    def test_history_suffix_standard_object(self):
        self.assertTrue(is_unsupported_bulk_object("ContactHistory"))

    def test_history_suffix_custom_object(self):
        self.assertTrue(is_unsupported_bulk_object("MyObject__History"))

    # --- suffix: EventRelation ---
    def test_eventrelation_suffix_standard(self):
        self.assertTrue(is_unsupported_bulk_object("AcceptedEventRelation"))

    def test_eventrelation_suffix_declined(self):
        self.assertTrue(is_unsupported_bulk_object("DeclinedEventRelation"))

    # --- objects that should NOT be excluded ---
    def test_partial_suffix_match_not_excluded(self):
        # "SharePoint" ends in "Point", not "Share" — must not be excluded
        self.assertFalse(is_unsupported_bulk_object("SharePoint"))

    def test_object_containing_history_not_suffix(self):
        # "HistoryLog" contains "History" but does not end with it
        self.assertFalse(is_unsupported_bulk_object("HistoryLog"))

# ---------------------------------------------------------------------------
# Tests for Salesforce.get_blacklisted_objects() — BULK API
# ---------------------------------------------------------------------------


class TestGetBlacklistedObjectsBulk(unittest.TestCase):
    """get_blacklisted_objects with BULK api_type."""

    def setUp(self):
        self.sf = make_sf(api_type="BULK")

    def test_suffix_share_object_excluded(self):
        names = ["Account", "AccountShare", "Contact"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("AccountShare", blacklisted)

    def test_suffix_feed_object_excluded(self):
        names = ["Case", "CaseFeed"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("CaseFeed", blacklisted)

    def test_suffix_history_object_excluded(self):
        names = ["Contact", "ContactHistory"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("ContactHistory", blacklisted)

    def test_suffix_eventrelation_object_excluded(self):
        names = ["AcceptedEventRelation", "DeclinedEventRelation", "UndecidedEventRelation"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("AcceptedEventRelation", blacklisted)
        self.assertIn("DeclinedEventRelation", blacklisted)
        self.assertIn("UndecidedEventRelation", blacklisted)

    def test_custom_share_object_excluded(self):
        names = ["PardotEnvironment__Share", "Account"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("PardotEnvironment__Share", blacklisted)

    def test_static_named_objects_excluded(self):
        blacklisted = self.sf.get_blacklisted_objects()
        for obj in UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS:
            self.assertIn(obj, blacklisted)

    def test_supported_objects_not_excluded(self):
        names = ["Account", "Contact", "Opportunity", "Lead"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        for name in names:
            self.assertNotIn(name, blacklisted)

    def test_no_object_names_returns_static_set(self):
        """Without object_names, only static blacklists are returned."""
        blacklisted_with = self.sf.get_blacklisted_objects(object_names=["AccountShare"])
        blacklisted_without = self.sf.get_blacklisted_objects()
        # Static entries must be present in both
        self.assertTrue(UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS.issubset(blacklisted_without))
        # Pattern-matched entry only present when names provided
        self.assertIn("AccountShare", blacklisted_with)
        self.assertNotIn("AccountShare", blacklisted_without)

    def test_object_names_as_set_is_accepted(self):
        """object_names can be passed as a set (not just a list)."""
        names = {"AccountShare", "Contact"}
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertIn("AccountShare", blacklisted)
        self.assertNotIn("Contact", blacklisted)


# ---------------------------------------------------------------------------
# Tests for Salesforce.get_blacklisted_objects() — REST API
# ---------------------------------------------------------------------------

class TestGetBlacklistedObjectsRest(unittest.TestCase):
    """get_blacklisted_objects with REST api_type — suffix logic must NOT apply."""

    def setUp(self):
        self.sf = make_sf(api_type="REST")

    def test_share_suffix_not_excluded_for_rest(self):
        names = ["AccountShare", "Account"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertNotIn("AccountShare", blacklisted)

    def test_feed_suffix_not_excluded_for_rest(self):
        names = ["CaseFeed", "Case"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertNotIn("CaseFeed", blacklisted)

    def test_history_suffix_not_excluded_for_rest(self):
        names = ["ContactHistory"]
        blacklisted = self.sf.get_blacklisted_objects(object_names=names)
        self.assertNotIn("ContactHistory", blacklisted)

    def test_static_bulk_objects_not_in_rest_blacklist(self):
        """Objects in UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS are bulk-only exclusions."""
        blacklisted = self.sf.get_blacklisted_objects()
        # TaskStatus is bulk-only — it should NOT appear in the REST blacklist
        self.assertNotIn("TaskStatus", blacklisted)
