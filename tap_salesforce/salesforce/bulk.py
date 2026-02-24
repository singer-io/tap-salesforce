"""
This module handles all interactions with the Salesforce Bulk API.

It provides functionality for creating, managing, and monitoring Bulk API jobs,
as well as retrieving query results. It supports both standard bulk queries
and PK Chunking for large data volumes.
"""
# pylint: disable=protected-access
import csv
import json
import sys
import time
import tempfile
import singer
from singer import metrics
import requests
from requests.exceptions import RequestException

import xmltodict

from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException)

BATCH_STATUS_POLLING_SLEEP = 20
PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP = 60
ITER_CHUNK_SIZE = 1024
DEFAULT_CHUNK_SIZE = 100000 # Max is 250000

LOGGER = singer.get_logger()

# pylint: disable=inconsistent-return-statements
def find_parent(stream):
    """
    Finds the parent object for certain Salesforce objects.

    This is used for objects like 'AccountCleanInfo' or 'ContactHistory' to
    derive the parent object ('Account' or 'Contact').

    Args:
        stream (str): The name of the Salesforce stream.

    Returns:
        str: The name of the parent stream.
    """
    parent_stream = stream
    if stream.endswith("CleanInfo"):
        parent_stream = stream[:stream.find("CleanInfo")]
    elif stream.endswith("FieldHistory"):
        parent_stream = stream[:stream.find("FieldHistory")]
    elif stream.endswith("History"):
        parent_stream = stream[:stream.find("History")]

    # If the stripped stream ends with "__" we can assume the parent is a custom table
    if parent_stream.endswith("__"):
        parent_stream += 'c'

    return parent_stream


class Bulk():
    """
    A class for interacting with the Salesforce Bulk API.
    """

    bulk_url = "{}/services/async/52.0/{}"

    def __init__(self, sf):
        """
        Initializes the Bulk API client.

        Args:
            sf (Salesforce): The main Salesforce client instance.
        """
        # Set csv max reading size to the platform's max size available.
        csv.field_size_limit(sys.maxsize)
        self.sf = sf

    def has_permissions(self):
        """
        Checks if the user has permissions to use the Bulk API.

        It does this by making a call to the limits endpoint and checking for
        a specific HTTP error that indicates a lack of permissions.

        Returns:
            bool: True if permissions are granted, False otherwise.
        """
        try:
            self.check_bulk_quota_usage()
        except requests.exceptions.HTTPError as err:
            if err.response is not None:
                for error_response_item in err.response.json():
                    if error_response_item.get('errorCode') == 'API_DISABLED_FOR_ORG':
                        return False
        return True

    def query(self, catalog_entry, state):
        """
        Executes a Bulk API query for a given stream.

        This method orchestrates the process of creating a job, adding a batch,
        closing the job, polling for completion, and yielding the records.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            state (dict): The current sync state.

        Yields:
            dict: A dictionary representing a single record from Salesforce.
        """
        self.check_bulk_quota_usage()

        for record in self._bulk_query(catalog_entry, state):
            yield record

        self.sf.jobs_completed += 1

    # pylint: disable=line-too-long
    def check_bulk_quota_usage(self):
        """
        Checks the daily Bulk API quota usage.

        This method queries the Salesforce limits endpoint to ensure that the tap
        does not exceed the configured total quota or the per-run quota.

        Raises:
            TapSalesforceQuotaExceededException: If a quota limit is exceeded.
        """
        endpoint = "limits"
        url = self.sf.data_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer(endpoint):
            resp = self.sf._make_request('GET', url, headers=self.sf._get_standard_headers()).json()

        quota_max = resp['DailyBulkApiBatches']['Max']
        max_requests_for_run = int((self.sf.quota_percent_per_run * quota_max) / 100)

        quota_remaining = resp['DailyBulkApiBatches']['Remaining']
        percent_used = (1 - (quota_remaining / quota_max)) * 100

        if percent_used > self.sf.quota_percent_total:
            total_message = ("Salesforce has reported {}/{} ({:3.2f}%) total Bulk API quota " +
                             "used across all Salesforce Applications. Terminating " +
                             "replication to not continue past configured percentage " +
                             "of {}% total quota.").format(quota_max - quota_remaining,
                                                           quota_max,
                                                           percent_used,
                                                           self.sf.quota_percent_total)
            raise TapSalesforceQuotaExceededException(total_message)
        elif self.sf.jobs_completed > max_requests_for_run:
            partial_message = ("This replication job has completed {} Bulk API jobs ({:3.2f}% of " +
                               "total quota). Terminating replication due to allotted " +
                               "quota of {}% per replication.").format(self.sf.jobs_completed,
                                                                       (self.sf.jobs_completed / quota_max) * 100,
                                                                       self.sf.quota_percent_per_run)
            raise TapSalesforceQuotaExceededException(partial_message)

    def _get_bulk_headers(self):
        """
        Constructs the standard headers for Bulk API requests.

        Returns:
            dict: A dictionary of HTTP headers.
        """
        return {"X-SFDC-Session": self.sf.access_token,
                "Content-Type": "application/json"}

    def _can_pk_chunk_job(self, failure_message): # pylint: disable=no-self-use
        """
        Determines if a failed job can be retried with PK Chunking.

        Args:
            failure_message (str): The failure message from the batch status.

        Returns:
            bool: True if the job can be retried with PK Chunking, False otherwise.
        """
        return "QUERY_TIMEOUT" in failure_message or \
               "Retried more than 15 times" in failure_message or \
               "Failed to write query result" in failure_message

    def _bulk_query(self, catalog_entry, state):
        """
        Internal method to perform a standard Bulk API query.

        If the query fails with a timeout, it will attempt to retry with PK Chunking.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            state (dict): The current sync state.

        Yields:
            dict: A dictionary representing a single record from Salesforce.
        """
        job_id = self._create_job(catalog_entry)
        start_date = self.sf.get_start_date(state, catalog_entry)

        batch_id = self._add_batch(catalog_entry, job_id, start_date)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        if batch_status['state'] == 'Failed':
            if self._can_pk_chunk_job(batch_status['stateMessage']):
                batch_status = self._bulk_query_with_pk_chunking(catalog_entry, start_date)
                job_id = batch_status['job_id']

                # Set pk_chunking to True to indicate that we should write a bookmark differently
                self.sf.pk_chunking = True

                # Add the bulk Job ID and its batches to the state so it can be resumed if necessary
                tap_stream_id = catalog_entry['tap_stream_id']
                state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
                state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batch_status['completed'][:])

                for completed_batch_id in batch_status['completed']:
                    for result in self.get_batch_results(job_id, completed_batch_id, catalog_entry):
                        yield result
                    # Remove the completed batch ID and write state
                    state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"].remove(completed_batch_id)
                    LOGGER.info("Finished syncing batch %s. Removing batch from state.", completed_batch_id)
                    LOGGER.info("Batches to go: %d", len(state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"]))
                    singer.write_state(state)
            else:
                raise TapSalesforceException(batch_status['stateMessage'])
        else:
            for result in self.get_batch_results(job_id, batch_id, catalog_entry):
                yield result

    def _bulk_query_with_pk_chunking(self, catalog_entry, start_date):
        """
        Performs a Bulk API query using PK Chunking.

        This is used for very large tables that might time out with a standard
        bulk query.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            start_date (str): The start date for the query.

        Returns:
            dict: A dictionary containing the status of the batches.
        """
        LOGGER.info("Retrying Bulk Query with PK Chunking")

        # Create a new job
        job_id = self._create_job(catalog_entry, True)

        self._add_batch(catalog_entry, job_id, start_date, False)

        batch_status = self._poll_on_pk_chunked_batch_status(job_id)
        batch_status['job_id'] = job_id

        if batch_status['failed']:
            raise TapSalesforceException("One or more batches failed during PK chunked job")

        # Close the job after all the batches are complete
        self._close_job(job_id)

        return batch_status

    def _create_job(self, catalog_entry, pk_chunking=False):
        """
        Creates a new Bulk API job.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            pk_chunking (bool): Whether to enable PK Chunking for this job.

        Returns:
            str: The ID of the created job.
        """
        url = self.bulk_url.format(self.sf.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "CSV"}

        headers = self._get_bulk_headers()
        headers['Sforce-Disable-Batch-Retry'] = "true"

        if pk_chunking:
            LOGGER.info("ADDING PK CHUNKING HEADER")

            headers['Sforce-Enable-PKChunking'] = "true; chunkSize={}".format(DEFAULT_CHUNK_SIZE)

            # If the stream ends with 'CleanInfo' or 'History', we can PK Chunk on the object's parent
            if any(catalog_entry['stream'].endswith(suffix) for suffix in ["CleanInfo", "History"]):
                parent = find_parent(catalog_entry['stream'])
                headers['Sforce-Enable-PKChunking'] = headers['Sforce-Enable-PKChunking'] + "; parent={}".format(parent)

        with metrics.http_request_timer("create_job") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request(
                'POST',
                url,
                headers=headers,
                body=json.dumps(body))

        job = resp.json()

        return job['id']

    def _add_batch(self, catalog_entry, job_id, start_date, order_by_clause=True):
        """
        Adds a batch (a query) to an existing Bulk API job.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            job_id (str): The ID of the job to add the batch to.
            start_date (str): The start date for the query.
            order_by_clause (bool): Whether to include an ORDER BY clause in the SOQL query.

        Returns:
            str: The ID of the created batch.
        """
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        body = self.sf._build_query_string(catalog_entry, start_date, order_by_clause=order_by_clause)

        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _poll_on_pk_chunked_batch_status(self, job_id):
        """
        Polls the status of all batches in a PK Chunked job until completion.

        Args:
            job_id (str): The ID of the PK Chunked job.

        Returns:
            dict: A dictionary summarizing the status of all batches.
        """
        batches = self._get_batches(job_id)

        while True:
            queued_batches = [b['id'] for b in batches if b['state'] == "Queued"]
            in_progress_batches = [b['id'] for b in batches if b['state'] == "InProgress"]

            if not queued_batches and not in_progress_batches:
                completed_batches = [b['id'] for b in batches if b['state'] == "Completed"]
                failed_batches = [b['id'] for b in batches if b['state'] == "Failed"]
                return {'completed': completed_batches, 'failed': failed_batches}
            else:
                time.sleep(PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP)
                batches = self._get_batches(job_id)

    def _poll_on_batch_status(self, job_id, batch_id):
        """
        Polls the status of a single batch until it completes or fails.

        Args:
            job_id (str): The ID of the job.
            batch_id (str): The ID of the batch to poll.

        Returns:
            dict: The final status of the batch.
        """
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status

    def job_exists(self, job_id):
        """
        Checks if a Bulk API job still exists on Salesforce.

        Args:
            job_id (str): The ID of the job to check.

        Returns:
            bool: True if the job exists, False otherwise.
        """
        try:
            endpoint = "job/{}".format(job_id)
            url = self.bulk_url.format(self.sf.instance_url, endpoint)
            headers = self._get_bulk_headers()

            with metrics.http_request_timer("get_job"):
                self.sf._make_request('GET', url, headers=headers)

            return True # requests will raise for a 400 InvalidJob

        except RequestException as ex:
            if ex.response.headers["Content-Type"] == 'application/json':
                exception_code = ex.response.json()['exceptionCode']
                if exception_code == 'InvalidJob':
                    return False
            raise

    def _get_batches(self, job_id):
        """
        Retrieves a list of all batches for a given job.

        Args:
            job_id (str): The ID of the job.

        Returns:
            list: A list of batch info dictionaries.
        """
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batches"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batches = xmltodict.parse(resp.text,
                                  xml_attribs=False,
                                  force_list=('batchInfo',))['batchInfoList']['batchInfo']

        return batches

    def _get_batch(self, job_id, batch_id):
        """
        Retrieves the status of a single batch.

        Args:
            job_id (str): The ID of the job.
            batch_id (str): The ID of the batch.

        Returns:
            dict: A dictionary containing the batch status information.
        """
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

    def get_batch_results(self, job_id, batch_id, catalog_entry):
        """
        Retrieves and yields the results from a completed batch.

        This method fetches the result IDs for a batch, then downloads and
        parses the CSV results, yielding each row as a record.

        Args:
            job_id (str): The ID of the job.
            batch_id (str): The ID of the batch.
            catalog_entry (dict): The catalog entry for the stream.

        Yields:
            dict: A dictionary representing a single record from Salesforce.
        """
        headers = self._get_bulk_headers()
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer("batch_result_list") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            batch_result_resp = self.sf._make_request('GET', url, headers=headers)

        # Returns a Dict where input:
        #   <result-list><result>1</result><result>2</result></result-list>
        # will return: {'result', ['1', '2']}
        batch_result_list = xmltodict.parse(batch_result_resp.text,
                                            xml_attribs=False,
                                            force_list={'result'})['result-list']

        for result in batch_result_list['result']:
            endpoint = "job/{}/batch/{}/result/{}".format(job_id, batch_id, result)
            url = self.bulk_url.format(self.sf.instance_url, endpoint)
            headers['Content-Type'] = 'text/csv'

            with tempfile.NamedTemporaryFile(mode="w+", encoding="utf8") as csv_file:
                resp = self.sf._make_request('GET', url, headers=headers, stream=True)
                for chunk in resp.iter_content(chunk_size=ITER_CHUNK_SIZE, decode_unicode=True):
                    if chunk:
                        # Replace any NULL bytes in the chunk so it can be safely given to the CSV reader
                        csv_file.write(chunk.replace('\0', ''))

                csv_file.seek(0)
                csv_reader = csv.reader(csv_file,
                                        delimiter=',',
                                        quotechar='"')

                column_name_list = next(csv_reader)

                for line in csv_reader:
                    rec = dict(zip(column_name_list, line))
                    yield rec

    def _close_job(self, job_id):
        """
        Closes a Bulk API job.

        Args:
            job_id (str): The ID of the job to close.
        """
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        body = {"state": "Closed"}

        with metrics.http_request_timer("close_job"):
            self.sf._make_request(
                'POST',
                url,
                headers=self._get_bulk_headers(),
                body=json.dumps(body))

    # pylint: disable=no-self-use
    def _iter_lines(self, response):
        """
        Iterates over the lines of a response.

        This is a clone of the iter_lines function from the requests library,
        with the change to pass keepends=True to handle line breaks within
        quoted CSV values correctly.

        Args:
            response (requests.Response): The HTTP response object.

        Yields:
            str: A line from the response content.
        """
        pending = None

        for chunk in response.iter_content(decode_unicode=True, chunk_size=ITER_CHUNK_SIZE):
            if pending is not None:
                chunk = pending + chunk

            lines = chunk.splitlines(keepends=True)

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield line

        if pending is not None:
            yield pending
