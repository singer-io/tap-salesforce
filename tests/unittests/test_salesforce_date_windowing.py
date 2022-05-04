import datetime
import unittest
from unittest import mock
from tap_salesforce import Salesforce
from tap_salesforce.salesforce import Bulk
from dateutil import tz

def mocked_batch_status(count):
    if count < 2:
        return {"failed": "test"}
    else:
        return {"failed": {}}

@mock.patch('tap_salesforce.salesforce.Bulk._add_batch')
@mock.patch('tap_salesforce.salesforce.Bulk._poll_on_pk_chunked_batch_status', side_effect = mocked_batch_status)
@mock.patch('tap_salesforce.salesforce.Bulk._create_job', side_effect=[1,2,3,4,5])
@mock.patch('tap_salesforce.salesforce.bulk.singer_utils.now')
class TestBulkDateWindow(unittest.TestCase):

    def test_bulk_date_windowing(self, mocked_singer_util_now, mocked_create_job, mocked_batch_status, mocked_add_batch):
        """
        To verify that if data is too large then date windowing mechanism execute properly similar to REST api date windowing
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

        mocked_singer_util_now.return_value = datetime.datetime(2022,5,2,12,15,00, tzinfo=tz.UTC)
        catalog_entry = {'stream': 'User', 'tap_stream_id': 'User', "schema": {}, 'metadata': []}
        Bulk(sf)._bulk_with_window([], catalog_entry, '2019-02-04T12:15:00Z')
        
        self.assertEqual(mocked_add_batch.call_count, 3, "Function is not called expected times")

    def test_bulk_date_windowing_with_max_retries_0(self, mocked_singer_util_now, mocked_create_job, mocked_batch_status, mocked_add_batch):
        """
        To verify that if data is too large then date windowing mechanism execute, 
        but after retrying upto MAX_RETRIES still not get data then raise proper exception
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

        mocked_singer_util_now.return_value = datetime.datetime(2022,5,2,12,15,00, tzinfo=tz.UTC)
        catalog_entry = {'stream': 'User', 'tap_stream_id': 'User', "schema": {}, 'metadata': []}

        with self.assertRaises(Exception) as e:
            Bulk(sf)._bulk_with_window([], catalog_entry, '2019-02-04T12:15:00Z', retries=0)

        self.assertEqual(str(e.exception), 'Ran out of retries attempting to query Salesforce Object User', "Not get expected Exception")

    def test_bulk_date_windowing_with_half_day_range_0(self, mocked_singer_util_now, mocked_create_job, mocked_batch_status, mocked_add_batch):
        """
        To verify that if data is too large then date windowing mechanism execute, 
        but after retrying window goes to 0 days, still not get data then raise proper exception
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

        mocked_singer_util_now.return_value = datetime.datetime(2019,2,5,12,15,00, tzinfo=tz.UTC)
        catalog_entry = {'stream': 'User', 'tap_stream_id': 'User', "schema": {}, 'metadata': []}

        with self.assertRaises(Exception) as e:
            Bulk(sf)._bulk_with_window([], catalog_entry, '2019-02-04T12:15:00Z')

        self.assertEqual(str(e.exception), 'Attempting to query by 0 day range, this would cause infinite looping.', "Not get expected Exception")
