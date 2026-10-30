# pylint: disable=protected-access,use-yield-from
import csv
import json
import sys
import time
import tempfile
import singer
import singer.utils as singer_utils
from singer import metrics
import requests
from requests.exceptions import RequestException

import xmltodict

from tap_salesforce.salesforce.rest import API_VERSION, Rest
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException,
    TapSalesforceBulkObjectNotSupportedException, TapSalesforceStreamUnavailableException)

BATCH_STATUS_POLLING_SLEEP = 20
PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP = 60
ITER_CHUNK_SIZE = 1024
DEFAULT_CHUNK_SIZE = 100000 # Max is 250000
MAX_RETRIES = 4
LOGGER = singer.get_logger()


def _job_creation_error_message(response):
    """Returns the Salesforce error message from a Bulk API job-creation
    failure `response`, or None if the response body isn't a recognized
    Bulk API error shape.

    _create_job() calls the Bulk 1.0 `/job` endpoint, whose job-creation
    failures are returned as a single JSON object using the
    exceptionCode/exceptionMessage envelope, e.g.:
        {"exceptionCode": "InvalidEntity",
         "exceptionMessage": "Entity 'X' is not supported by the Bulk API."}
    This dict shape is the one actually produced in production. A JSON list
    of {"message", "errorCode"} objects is also accepted for backwards
    compatibility with other Salesforce error responses that use that shape.
    """
    if response is None:
        return None
    try:
        body = response.json()
    except ValueError:
        return None
    if isinstance(body, dict):
        return body.get('exceptionMessage', '') or ''
    if isinstance(body, list) and body:
        return body[0].get('message', '') or ''
    return None


def _bulk_unsupported_entity_message(response):
    """Returns the Salesforce error message if `response` is a Bulk API
    job-creation failure caused by the target object not being supported by
    the Bulk API (e.g. "Entity 'XxxShare' is not supported by the Bulk API."),
    otherwise returns None.

    Salesforce does not expose a dedicated errorCode for this condition (it
    uses the generic 'InvalidEntity' code for several unrelated issues too),
    so the only reliable signal is this specific message text. This is the
    only one of the known job-creation failure patterns that is actually
    specific to the Bulk API, which is why only this one triggers a REST
    fallback (see _query_level_restriction_message below).
    """
    message = _job_creation_error_message(response)
    if message and 'is not supported by the bulk api' in message.lower():
        return message
    return None


def _query_level_restriction_message(response):
    """Returns the Salesforce error message if `response` is a Bulk API
    job-creation failure caused by a restriction on the query itself, rather
    than a Bulk-API-specific limitation, otherwise returns None.

    Known patterns:
      - "<Object> does not support query" (e.g. AggregateResult)
      - "...requires a filter by Id..." (objects with mandatory WHERE clauses)

    Both the Bulk API and the REST API build the exact same SOQL query via
    Salesforce._build_query_string(), so unlike the Bulk-specific
    "not supported by the Bulk API" error, these failures are NOT fixed by
    falling back to REST -- REST would fail with the identical restriction.
    Objects that hit this should instead be added to
    QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS or QUERY_RESTRICTED_SALESFORCE_OBJECTS
    in tap_salesforce/salesforce/__init__.py.
    """
    message = _job_creation_error_message(response)
    if not message:
        return None
    lowered = message.lower()
    if 'does not support query' in lowered or 'requires a filter by' in lowered:
        return message
    return None


def _is_org_environment_batch_failure(state_message):
    """Returns True if `state_message` (a Bulk API batch's `stateMessage`)
    indicates Salesforce denied or could not complete the query because of a
    condition tied to this org's own environment, licensing, or permissions
    -- not a true failure of the batch/query itself, and not something the
    tap can work around.

    Known patterns:
      - Disabled/unlicensed APIs (see also Blacklisting.md's "Datacloud
        Objects" section, which already documents that these objects "may
        throw errors on request, depending on the licensing and permissions
        of the account being used"):
            InvalidBatch : Failed to process query: DATACLOUD_API_DISABLED_EXCEPTION:
            Your organization doesn't have permission to access the Data.com API
        Other Salesforce APIs disabled for a given org/edition raise the
        same '<NAME>_API_DISABLED_EXCEPTION' (or bare 'API_DISABLED_EXCEPTION')
        pattern, so match on that suffix rather than hardcoding DATACLOUD_ only.
      - External objects whose backing service is unreachable, e.g. querying
        ExternalEncryptionRootKey (a BYOK-backed external object) when the
        org's key management service can't be reached:
            InvalidBatch : Failed to process query: EXTERNAL_OBJECT_EXCEPTION:
            We can't connect to your key management service. Try again, or
            contact Salesforce Customer Support.
        EXTERNAL_OBJECT_EXCEPTION is also used for other, unrelated External
        Object errors, so this only matches when the message also mentions
        connectivity ('connect'), to avoid masking genuinely actionable
        failures under this same generic error code.

    This is intentionally NOT a static object-name blacklist entry: whether
    these failures occur depends on the org's licensing/environment, not the
    object name, so some orgs may be able to query these objects successfully.
    """
    if not state_message:
        return False
    if 'API_DISABLED_EXCEPTION' in state_message:
        return True
    if 'EXTERNAL_OBJECT_EXCEPTION' in state_message and 'connect' in state_message.lower():
        return True
    return False

# pylint: disable=inconsistent-return-statements
def find_parent(stream):
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

    bulk_url = "{}/services/async/{}.0/{}"

    def __init__(self, sf):
        # Set csv max reading size to the platform's max size available.
        csv.field_size_limit(sys.maxsize)
        self.sf = sf

    def has_permissions(self):
        try:
            self.check_bulk_quota_usage()
        except requests.exceptions.HTTPError as err:
            if err.response is not None:
                for error_response_item in err.response.json():
                    if error_response_item.get('errorCode') == 'API_DISABLED_FOR_ORG':
                        return False
        return True

    def query(self, catalog_entry, state):
        self.check_bulk_quota_usage()

        yielded_any = False
        try:
            for record in self._bulk_query(catalog_entry, state):
                yielded_any = True
                yield record
        except TapSalesforceBulkObjectNotSupportedException as ex:
            if yielded_any:
                # _create_job() is also called from
                # _bulk_query_with_pk_chunking() (via _bulk_with_window()),
                # which can run after some batches from an earlier job have
                # already been yielded. Falling back to REST at that point
                # would re-emit those records as duplicates, so only retry
                # via REST when nothing has been yielded yet for this stream.
                raise
            # Salesforce rejected job creation for this specific object (not a
            # blanket/account-wide failure). Nothing has been yielded yet, so
            # it is safe to retry the whole stream via REST from scratch.
            LOGGER.warning(
                "%s: %s Falling back to REST API for this stream.",
                catalog_entry['stream'], ex)
            for record in Rest(self.sf).query(catalog_entry, state):
                yield record
            return
        except TapSalesforceStreamUnavailableException as ex:
            if yielded_any:
                # Shouldn't normally happen (these failures surface at
                # job/batch creation, before any records are yielded), but
                # guard against partial-yield edge cases the same way as the
                # Bulk-unsupported case above.
                raise
            # A condition tied to this org's own environment, licensing, or
            # permissions makes this object entirely unqueryable here, so the
            # REST API would hit the identical restriction -- there is no
            # fallback to attempt. Skip this stream gracefully instead of
            # failing the entire sync.
            LOGGER.warning(
                "%s: %s Skipping this stream; it cannot be queried in your "
                "organization's current environment.",
                catalog_entry['stream'], ex)
            return

        self.sf.jobs_completed += 1

    # pylint: disable=line-too-long
    def check_bulk_quota_usage(self):
        endpoint = "limits"
        url = self.sf.data_url.format(self.sf.instance_url, API_VERSION, endpoint)

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
        return {"X-SFDC-Session": self.sf.access_token,
                "Content-Type": "application/json"}

    def _can_pk_chunk_job(self, failure_message):
        return  "QUERY_TIMEOUT" in failure_message or \
                "Retried more" in failure_message or \
                "Failed to write query result" in failure_message

    def _bulk_query(self, catalog_entry, state):
        job_id = self._create_job(catalog_entry)
        start_date = self.sf.get_start_date(state, catalog_entry)

        batch_id = self._add_batch(catalog_entry, job_id, start_date)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        if batch_status['state'] == 'Failed':
            if _is_org_environment_batch_failure(batch_status['stateMessage']):
                raise TapSalesforceStreamUnavailableException(
                    "{}: {}".format(catalog_entry['stream'], batch_status['stateMessage']))
            if self._can_pk_chunk_job(batch_status['stateMessage']):
                # Get list of batch_status with pk_chunking or date_windowing
                status_list = self._bulk_with_window([], catalog_entry, start_date)

                for batch_status in status_list:
                    job_id = batch_status['job_id']

                    # Set pk_chunking to True to indicate that we should write a bookmark differently
                    self.sf.pk_chunking = True

                    # Add the bulk Job ID and its batches to the state so it can be resumed if necessary
                    tap_stream_id = catalog_entry['tap_stream_id']
                    state = singer.set_bookmark(state, tap_stream_id, 'JobID', job_id)
                    state = singer.set_bookmark(state, tap_stream_id, 'BatchIDs', batch_status['completed'][:])

                    for completed_batch_id in batch_status['completed']:
                        for result in self.get_batch_results(job_id, completed_batch_id, catalog_entry):
                            yield result
                        # Remove the completed batch ID and write state
                        state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"].remove(completed_batch_id)
                        LOGGER.info("Finished syncing batch %s. Removed batch from state.", completed_batch_id)
                        LOGGER.info("Batches to go: %d", len(state['bookmarks'][catalog_entry['tap_stream_id']]["BatchIDs"]))
                        singer.write_state(state)
            else:
                raise TapSalesforceException(batch_status['stateMessage'])
        else:
            for result in self.get_batch_results(job_id, batch_id, catalog_entry):
                yield result

    def _bulk_query_with_pk_chunking(self, catalog_entry, start_date):
        LOGGER.info("Retrying Bulk Query with PK Chunking")

        # Create a new job
        job_id = self._create_job(catalog_entry, True)

        self._add_batch(catalog_entry, job_id, start_date, order_by_clause=False)

        batch_status = self._poll_on_pk_chunked_batch_status(job_id)
        batch_status['job_id'] = job_id

        # Close the job after all the batches are complete
        self._close_job(job_id)

        return batch_status

    def _create_job(self, catalog_entry, pk_chunking=False):
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, "job")
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
            try:
                resp = self.sf._make_request(
                    'POST',
                    url,
                    headers=headers,
                    body=json.dumps(body))
            except RequestException as ex:
                response = getattr(ex, 'response', None)
                bulk_message = _bulk_unsupported_entity_message(response)
                if bulk_message:
                    raise TapSalesforceBulkObjectNotSupportedException(
                        "{}: {}".format(catalog_entry['stream'], bulk_message)) from ex
                query_message = _query_level_restriction_message(response)
                if query_message:
                    raise TapSalesforceException(
                        "{}: {} This is a query-level restriction, not a "
                        "Bulk-API-specific limitation (the REST API builds "
                        "the identical query and would fail the same way), "
                        "so no REST fallback is attempted. Add this object "
                        "to QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS or "
                        "QUERY_RESTRICTED_SALESFORCE_OBJECTS in "
                        "tap_salesforce/salesforce/__init__.py to exclude "
                        "it.".format(catalog_entry['stream'], query_message)) from ex
                raise

        job = resp.json()

        return job['id']

    #pylint: disable=too-many-positional-arguments
    def _add_batch(self, catalog_entry, job_id, start_date, end_date=None, order_by_clause=True):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)

        body = self.sf._build_query_string(catalog_entry, start_date, end_date, order_by_clause=order_by_clause)

        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self.sf._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _poll_on_pk_chunked_batch_status(self, job_id):
        batches = self._get_batches(job_id)

        while True:
            queued_batches = [b['id'] for b in batches if b['state'] == "Queued"]
            in_progress_batches = [b['id'] for b in batches if b['state'] == "InProgress"]

            if not queued_batches and not in_progress_batches:
                completed_batches = [b['id'] for b in batches if b['state'] == "Completed"]
                failed_batches = {b['id']: b.get('stateMessage') for b in batches if b['state'] == "Failed"}
                return {'completed': completed_batches, 'failed': failed_batches}
            else:
                time.sleep(PK_CHUNKED_BATCH_STATUS_POLLING_SLEEP)
                batches = self._get_batches(job_id)

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status

    def job_exists(self, job_id):
        try:
            endpoint = "job/{}".format(job_id)
            url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)
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
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batches"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batches = xmltodict.parse(resp.text,
                                  xml_attribs=False,
                                  force_list=('batchInfo',))['batchInfoList']['batchInfo']

        return batches

    def _get_batch(self, job_id, batch_id):
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self.sf._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

    def get_batch_results(self, job_id, batch_id, catalog_entry):
        """Given a job_id and batch_id, queries the batches results and reads
        CSV lines yielding each line as a record."""
        headers = self._get_bulk_headers()
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)

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
            url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)
            # re-read per file: the login timer rotates access_token mid-download
            headers = self._get_bulk_headers()
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
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.sf.instance_url, API_VERSION, endpoint)
        body = {"state": "Closed"}

        with metrics.http_request_timer("close_job"):
            self.sf._make_request(
                'POST',
                url,
                headers=self._get_bulk_headers(),
                body=json.dumps(body))

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

    def _bulk_with_window(self, status_list, catalog_entry, start_date_str, end_date=None, retries=MAX_RETRIES):
        """Bulk api call with date windowing"""
        sync_start = singer_utils.now()
        if end_date is None:
            end_date = sync_start
            LOGGER.info("Retrying Bulk Query with PK Chunking")
        else:
            LOGGER.info("Retrying Bulk Query with window of date {} to {}".format(start_date_str, end_date.strftime('%Y-%m-%dT%H:%M:%SZ')))

        if retries == 0:
            raise TapSalesforceException("Ran out of retries attempting to query Salesforce Object {}".format(catalog_entry['stream']))

        job_id = self._create_job(catalog_entry, True)
        self._add_batch(catalog_entry, job_id, start_date_str, end_date.strftime('%Y-%m-%dT%H:%M:%SZ'), False)
        batch_status = self._poll_on_pk_chunked_batch_status(job_id)
        batch_status['job_id'] = job_id
        # Close the job after all the batches are complete
        self._close_job(job_id)

        if batch_status['failed']:
            LOGGER.info("Failed Bulk Query with window of date {} to {}".format(start_date_str, end_date.strftime('%Y-%m-%dT%H:%M:%SZ')))
            # If batch_status is failed then reduce date window by half by updating end_date
            end_date = self.sf.get_window_end_date(singer_utils.strptime_with_tz(start_date_str), end_date)

            return self._bulk_with_window(status_list, catalog_entry, start_date_str, end_date, retries - 1)

        else:
            status_list.append(batch_status)

            # If the date range was chunked (an end_date was passed), sync
            # from the end_date -> now
            if end_date < sync_start:
                next_start_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                return self._bulk_with_window(status_list, catalog_entry, next_start_date_str, retries=retries)

            return status_list
