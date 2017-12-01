# pylint: disable=protected-access
import csv
import datetime
import json
import time
import singer
import singer.metrics as metrics
import singer.utils as singer_utils
import xmltodict
from itertools import chain

from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException)

BATCH_STATUS_POLLING_SLEEP = 20
ITER_CHUNK_SIZE = 512

LOGGER = singer.get_logger()

class Bulk(object):

    bulk_url = "{}/services/async/41.0/{}"

    def __init__(self, sf):
        self.sf = sf

    def query(self, catalog_entry, state):
        self.check_bulk_quota_usage()

        for record in self._bulk_query(catalog_entry, state):
            yield record

        self.sf.jobs_completed += 1

    # pylint: disable=line-too-long
    def check_bulk_quota_usage(self):
        endpoint = "limits"
        url = self.sf.data_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer(endpoint):
            resp = self.sf._make_request('GET', url, headers=self.sf._get_standard_headers()).json()

        quota_max = resp['DailyBulkApiRequests']['Max']
        max_requests_for_run = int((self.sf.quota_percent_per_run * quota_max) / 100)

        quota_remaining = resp['DailyBulkApiRequests']['Remaining']
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
        return {"X-SFDC-Session": self.sf.access_token,
                "Content-Type": "application/json"}

    def _bulk_query(self, catalog_entry, state):
        job_id = self._create_job(catalog_entry)
        start_date = self.sf._get_start_date(state, catalog_entry)

        batch_id = self._add_batch(catalog_entry, job_id, start_date)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        if batch_status['state'] == 'Failed':
            if "QUERY_TIMEOUT" in batch_status['stateMessage']:
                LOGGER.info("oh hi mark")
                # Create a new job with the Batching header
                job_id = self._create_job(catalog_entry, True)

                # Add the same batch (without the ORDER BY?)
                batch_id = self._add_batch(catalog_entry, job_id, start_date, True)

                # POLL the batches and get results from ones that finished
                # Keep going until all batches are finished
                batch_status = self._poll_on_pk_chunked_batch_status(job_id, batch_id)

                # If there are failed batches, this is bad
                if batch_status['failed']:
                    raise TapSalesforceException("one or more batches failed...")

                for completed_batch in batch_status['completed']:
                    for result in self._get_batch_results(job_id, completed_batch, catalog_entry):
                        yield result

            raise TapSalesforceException(batch_status['stateMessage'])

        return self._get_batch_results(job_id, batch_id, catalog_entry)

    def _create_job(self, catalog_entry, pk_chunking=False):
        url = self.bulk_url.format(self.sf.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "CSV"}

        headers = self._get_bulk_headers()
        headers['Sforce-Disable-Batch-Retry'] = "true"

        # Enable PK Chunking with a lower-than-default chunk size
        if pk_chunking:
            headers['Sforce-Enable-PKChunking'] = "true; chunkSize=50000"

        with metrics.http_request_timer("create_job") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request(
                'POST',
                url,
                headers=headers,
                body=json.dumps(body))

        job = resp.json()

        return job['id']

    def _add_batch(self, catalog_entry, job_id, start_date, pk_chunking=False):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        body = self.sf._build_query_string(catalog_entry, start_date, pk_chunking=pk_chunking)

        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _poll_on_pk_chunked_batch_status(self, job_id, original_batch_id):
        batches = self._get_batches(job_id)

        completed_batches = []
        failed_batches = []

        while True:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            import pdb
            pdb.set_trace()
            for batch in batches:
                if batch['state'] == "Completed" and batch['id'] not in completed_batches:
                    completed_batches.append(batch['id'])
                if batch['state'] == "Failed" and batch['id'] not in failed_batches:
                    failed_batches.append(batch['id'])

            sum_batches = len(completed_batches) + len(failed_batches)

            if sum_batches == len(batches) + 1:
                break
            else:
                batches = self._get_batches(job_id)

        return {'completed': completed_batches, 'failed': failed_batches}

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status

    def _get_batches(self, job_id):
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
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

    def _get_batch_results(self, job_id, batch_id, catalog_entry):
        """Given a job_id and batch_id, queries the batches results and reads
        CSV lines yielding each line as a record."""
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

            with metrics.http_request_timer("batch_result") as timer:
                timer.tags['sobject'] = catalog_entry['stream']
                result_response = self.sf._make_request('GET', url, headers=headers, stream=True)

            csv_stream = csv.reader(self._iter_lines(result_response),
                                    delimiter=',',
                                    quotechar='"')

            column_name_list = next(csv_stream)

            for line in csv_stream:
                rec = dict(zip(column_name_list, line))
                yield rec

    def _close_job(self, job_id):
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
        """Clone of the iter_lines function from the requests library with the change
        to pass keepends=True in order to ensure that we do not strip the line breaks
        from within a quoted value from the CSV stream."""
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
