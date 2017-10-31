import csv
import json
import singer.metrics as metrics
import time
import xmltodict

from tap_salesforce.salesforce.exceptions import (TapSalesforceException, TapSalesforceQuotaExceededException)

BATCH_STATUS_POLLING_SLEEP = 5
ITER_CHUNK_SIZE = 512

class Bulk(object):

    bulk_url = "{}/services/async/41.0/{}"

    def __init__(self, sf):
        self.sf = sf
        self.jobs_completed = 0

    def query(self, catalog_entry, state):
        self.check_bulk_quota_usage(self.jobs_completed)

        for record in self._bulk_query(catalog_entry, state):
            yield record

        self.jobs_completed += 1

    def check_bulk_quota_usage(self, jobs_completed):
        endpoint = "limits"
        url = self.sf.data_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer(endpoint):
            resp = self.sf._make_request('GET', url, headers=self.sf._get_standard_headers()).json()

        quota_max = resp['DailyBulkApiRequests']['Max']
        max_requests_for_run = int((self.sf.quota_percent_per_run * quota_max) / 100)

        quota_remaining = resp['DailyBulkApiRequests']['Remaining']
        percent_used = (1 - (quota_remaining / quota_max)) * 100

        if percent_used > self.sf.quota_percent_total:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota usage of {}% of {} allotted queries".format(
                self.sf.quota_percent_total,
                quota_max))

        elif self.jobs_completed > max_requests_for_run:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota per run of {}% of {} allotted queries".format(
                self.sf.quota_percent_per_run,
                quota_max))


    def _get_bulk_headers(self):
        return {"X-SFDC-Session": self.sf.access_token,
                "Content-Type": "application/json"}

    def _bulk_query(self, catalog_entry, state):
        job_id = self._create_job(catalog_entry)
        batch_id = self._add_batch(catalog_entry, job_id, state)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        if batch_status['state'] == 'Failed':
            raise TapSalesforceException(batch_status['stateMessage'])
        return self._get_batch_results(job_id, batch_id, catalog_entry, state)

    def _create_job(self, catalog_entry):
        url = self.bulk_url.format(self.sf.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "CSV"}

        with metrics.http_request_timer("create_job") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))

        job = resp.json()

        return job['id']

    def _add_batch(self, catalog_entry, job_id, state):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        start_date = self._get_start_date(state, catalog_entry)
        body = self.sf._build_query_string(catalog_entry, start_date)

        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status

    def _get_batch(self, job_id, batch_id):
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

    def _get_batch_results(self, job_id, batch_id, catalog_entry, state):
        """Given a job_id and batch_id, queries the batches results and reads CSV lines yielding each
        line as a record."""
        headers = self._get_bulk_headers()
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, endpoint)

        with metrics.http_request_timer("batch_result_list") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            batch_result_resp = self.sf._make_request('GET', url, headers=headers)

        # Returns a Dict where an input like: <result-list><result>1</result><result>2</result></result-list>
        # will return: {'result', ['1', '2']}
        batch_result_list = xmltodict.parse(batch_result_resp.text,
                                            xml_attribs=False,
                                            force_list={'result'})['result-list']

        replication_key = catalog_entry['replication_key']

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
            self.sf._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))

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
