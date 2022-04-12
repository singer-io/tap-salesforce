import logging
from typing import Counter
import unittest
from unittest import mock
# from tap_salesforce import main
import tap_salesforce
from tap_salesforce import Salesforce, metrics
from tap_salesforce.sync import sync_records
import json



class TestNullBookmarkTesting(unittest.TestCase):
    @mock.patch('tap_salesforce.salesforce.Salesforce.query', side_effect=lambda test1, test2: [])
    def test_not_null_bookmark_for_incremental_stream(self, mocked_query):
        """
        To ensure that after resolving writebook mark logic not get "Null" key in state

        """
        sf = Salesforce(
            refresh_token="test",
            sf_client_id="test",
            sf_client_secret="test",
            quota_percent_total=None,
            quota_percent_per_run=None,
            is_sandbox=None,
            select_fields_by_default= False,
            default_start_date='2019-02-04T12:15:00Z',
            api_type="BULK")
        
        sf.pk_chunking = True
        sf.instance_url = "https://cds-e-dev-ed.my.salesforce.com"
        catalog_entry = {"stream": "OpportunityLineItem", "schema": {}, "metadata":[], "tap_stream_id": "OpportunityLineItem"}
        state = {}
        counter = metrics.record_counter('OpportunityLineItem')
        sync_records(sf, catalog_entry, state, counter)
        # write state function convert python dictionary to json string
        state = json.dumps(state)
        self.assertEqual(state, '{"bookmarks": {"OpportunityLineItem": {"version": null}}}', "Not get expected state value")