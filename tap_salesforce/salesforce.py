import requests
import re
import csv
import json
import xmltodict
import singer
import singer.metrics as metrics
import time
import threading
from io import StringIO
from singer import metadata

LOGGER = singer.get_logger()

# The minimum expiration setting for SF Refresh Tokens is 15 minutes
REFRESH_TOKEN_EXPIRATION_PERIOD = 900

BATCH_STATUS_POLLING_SLEEP = 5

class TapSalesforceException(Exception):
    pass

class TapSalesforceQuotaExceededException(TapSalesforceException):
    pass

ITER_CHUNK_SIZE = 512

STRING_TYPES = set([
    'id',
    'string',
    'picklist',
    'textarea',
    'phone',
    'url',
    'reference',
    'multipicklist',
    'combobox',
    'encryptedstring',
    'email', # TODO: Unverified
    'complexvalue' # TODO: Unverified
])

NUMBER_TYPES = set([
    'double',
    'currency',
    'percent'
])

DATE_TYPES = set([
    'datetime',
    'date'
])

def field_to_property_schema(field, mdata):
    property_schema = {}

    field_name = field['name']
    sf_type = field['type']
    nillable = field['nillable']

    if sf_type in STRING_TYPES:
        property_schema['type'] = "string"
    elif sf_type in DATE_TYPES:
        property_schema["format"] = "date-time"
        property_schema['type'] = "string"
    elif sf_type == "boolean":
        property_schema['type'] = "boolean"
    elif sf_type in NUMBER_TYPES:
        property_schema['type'] = "number"
    elif sf_type == "address":
        # Addresses are compound fields and we omit those
        property_schema['type'] = "string"
    elif sf_type == "int":
        property_schema['type'] = "integer"
    elif sf_type == "time":
        property_schema['type'] = "string"
    elif sf_type == "anyType":
        return property_schema, mdata # No type = all types
    elif sf_type == 'base64':
        mdata = metadata.write(mdata, ('properties', field_name), "inclusion", "unsupported")
        mdata = metadata.write(mdata, ('properties', field_name), "unsupported-description", "binary data")
        return property_schema, mdata
    elif sf_type == 'location': # geo coordinates are divided into two fields for lat/long
        property_schema['type'] = "number"
        property_schema['multipleOf'] = 0.000001
    else:
        raise TapSalesforceException("Found unsupported type: {}".format(sf_type))

    if nillable:
        property_schema['type'] =  ["null", property_schema['type']]

    return property_schema, mdata

class Salesforce(object):
    # instance_url, endpoint
    data_url = "{}/services/data/v41.0/{}"
    bulk_url = "{}/services/async/41.0/{}"

    def __init__(self,
                 refresh_token=None,
                 token=None,
                 sf_client_id=None,
                 sf_client_secret=None,
                 quota_percent_per_run=None,
                 quota_percent_total=None,
                 is_sandbox=None,
                 select_fields_by_default=None,
                 default_start_date=None):
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        self.quota_percent_per_run = float(quota_percent_per_run) if quota_percent_per_run is not None else 25
        self.quota_percent_total = float(quota_percent_total) if quota_percent_total is not None else 80
        self.is_sandbox = is_sandbox == 'true'
        self.select_fields_by_default = select_fields_by_default == 'true'
        self.default_start_date = default_start_date
        self.rest_requests_attempted = 0
        self.login_timer = None

    def _get_bulk_headers(self):
        return {"X-SFDC-Session": self.access_token,
                "Content-Type": "application/json"}

    def _get_standard_headers(self):
        return {"Authorization": "Bearer {}".format(self.access_token)}

    def check_rest_quota_usage(self, headers):
        match = re.search('^api-usage=(\d+)/(\d+)$', headers.get('Sforce-Limit-Info'))

        if match is None:
            return

        remaining, allotted = map(int, match.groups())

        LOGGER.info("Used {} of {} daily API quota".format(remaining, allotted))

        percent_used_from_total = (remaining / allotted) * 100
        max_requests_for_run = int((self.quota_percent_per_run * allotted) / 100)

        if percent_used_from_total > self.quota_percent_total:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota usage of {}% of {} allotted queries".format(
                                         self.quota_percent_total,
                                         allotted))
        elif self.rest_requests_attempted > max_requests_for_run:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota per run of {}% of {} allotted queries".format(
                                         self.quota_percent_per_run,
                                         allotted))

    def _make_request(self, http_method, url, headers=None, body=None, stream=False):
        if http_method == "GET":
            LOGGER.info("Making %s request to %s", http_method, url)
            resp = self.session.get(url, headers=headers, stream=stream)
        elif http_method == "POST":
            LOGGER.info("Making %s request to %s with body %s", http_method, url, body)
            resp = self.session.post(url, headers=headers, data=body)
        else:
            raise TapSalesforceException("Unsupported HTTP method")

        try:
            resp.raise_for_status()
        except Exception as e:
            raise Exception(str(e) + ", Response from Salesforce: {}".format(resp.text)) from e

        if resp.headers.get('Sforce-Limit-Info') is not None:
            self.rest_requests_attempted += 1
            self.check_rest_quota_usage(resp.headers)

        return resp

    def login(self):
        if self.is_sandbox:
            login_url = 'https://test.salesforce.com/services/oauth2/token'
        else:
            login_url = 'https://login.salesforce.com/services/oauth2/token'

        login_body = {'grant_type': 'refresh_token', 'client_id': self.sf_client_id,
                      'client_secret': self.sf_client_secret, 'refresh_token': self.refresh_token}

        LOGGER.info("Attempting login via OAuth2")

        resp = self.session.post(login_url, data=login_body, headers={"Content-Type": "application/x-www-form-urlencoded"})

        try:
            resp.raise_for_status()
        except Exception as e:
            raise Exception(str(e) + ", Response from Salesforce: {}".format(resp.text)) from e

        LOGGER.info("OAuth2 login successful")

        auth = resp.json()

        self.access_token = auth['access_token']
        self.instance_url = auth['instance_url']
        self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD, self.login)
        self.login_timer.start()

    def describe(self, sobject=None):
        """Describes all objects or a specific object"""
        headers = self._get_standard_headers()
        if sobject is None:
            endpoint = "sobjects"
            endpoint_tag = "sobjects"
            url = self.data_url.format(self.instance_url, endpoint)
        else:
            endpoint = "sobjects/{}/describe".format(sobject)
            endpoint_tag = sobject
            url = self.data_url.format(self.instance_url, endpoint)

        with metrics.http_request_timer("describe") as timer:
            timer.tags['endpoint'] = endpoint_tag
            resp = self._make_request('GET', url, headers=headers)

        return resp.json()

    def check_bulk_quota_usage(self, jobs_completed):
        endpoint = "limits"
        url = self.data_url.format(self.instance_url, endpoint)

        with metrics.http_request_timer(endpoint):
            resp = self._make_request('GET', url, headers=self._get_standard_headers()).json()

        quota_max = resp['DailyBulkApiRequests']['Max']
        max_requests_for_run = int((self.quota_percent_per_run * quota_max) / 100)

        quota_remaining = resp['DailyBulkApiRequests']['Remaining']
        percent_used = (1 - (quota_remaining / quota_max)) * 100

        if percent_used > self.quota_percent_total:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota usage of {}% of {} allotted queries".format(
                self.quota_percent_total,
                quota_max))

        elif jobs_completed > max_requests_for_run:
            raise TapSalesforceQuotaExceededException("Terminating due to exceeding configured quota per run of {}% of {} allotted queries".format(
                self.quota_percent_per_run,
                quota_max))

    def _get_selected_properties(self, catalog_entry):
        mdata = metadata.to_map(catalog_entry['metadata'])
        properties = catalog_entry['schema'].get('properties', {})

        return [k for k, v in properties.items()
                if metadata.get(mdata, ('properties', k), 'selected')
                or metadata.get(mdata, ('properties', k), 'inclusion') == 'automatic']

    def _get_start_date(self, state, catalog_entry):
        replication_key = catalog_entry['replication_key']

        return (singer.get_bookmark(state,
                                    catalog_entry['tap_stream_id'],
                                    replication_key) or self.default_start_date)

    def _build_bulk_query_batch(self, catalog_entry, state):
        selected_properties = self._get_selected_properties(catalog_entry)

        # TODO: If there are no selected properties we should do something smarter
        # do we always need to select the replication key (SystemModstamp, or LastModifiedDate, etc)?
        #

        replication_key = catalog_entry['replication_key']

        if replication_key:
            where_clause = " WHERE {} >= {} ORDER BY {} ASC".format(
                replication_key,
                self._get_start_date(state, catalog_entry),
                replication_key)
        else:
            where_clause = ""

        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry['stream'])

        return query + where_clause

    def _get_batch(self, job_id, batch_id):
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        headers = self._get_bulk_headers()

        with metrics.http_request_timer("get_batch"):
            resp = self._make_request('GET', url, headers=headers)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']

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

    def _get_batch_results(self, job_id, batch_id, catalog_entry, state):
        """Given a job_id and batch_id, queries the batches results and reads CSV lines yielding each
        line as a record."""
        headers = self._get_bulk_headers()
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_url.format(self.instance_url, endpoint)

        with metrics.http_request_timer("batch_result_list") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            batch_result_resp = self._make_request('GET', url, headers=headers)

        # Returns a Dict where an input like: <result-list><result>1</result><result>2</result></result-list>
        # will return: {'result', ['1', '2']}
        batch_result_list = xmltodict.parse(batch_result_resp.text,
                                            xml_attribs=False,
                                            force_list={'result'})['result-list']

        replication_key = catalog_entry['replication_key']

        for result in batch_result_list['result']:
            endpoint = "job/{}/batch/{}/result/{}".format(job_id, batch_id, result)
            url = self.bulk_url.format(self.instance_url, endpoint)
            headers['Content-Type'] = 'text/csv'

            with metrics.http_request_timer("batch_result") as timer:
                timer.tags['sobject'] = catalog_entry['stream']
                result_response = self._make_request('GET', url, headers=headers, stream=True)

            csv_stream = csv.reader(self._iter_lines(result_response),
                                    delimiter=',',
                                    quotechar='"')

            column_name_list = next(csv_stream)

            for line in csv_stream:
                rec = dict(zip(column_name_list, line))
                yield rec

    def _create_job(self, catalog_entry):
        url = self.bulk_url.format(self.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "CSV"}

        with metrics.http_request_timer("create_job") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))

        job = resp.json()

        return job['id']

    def _add_batch(self, catalog_entry, job_id, state):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        body = self._build_bulk_query_batch(catalog_entry, state)
        headers = self._get_bulk_headers()
        headers['Content-Type'] = 'text/csv'

        with metrics.http_request_timer("add_batch") as timer:
            timer.tags['sobject'] = catalog_entry['stream']
            resp = self._make_request('POST', url, headers=headers, body=body)

        batch = xmltodict.parse(resp.text)

        return batch['batchInfo']['id']

    def _close_job(self, job_id):
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        body = {"state": "Closed"}

        with metrics.http_request_timer("close_job"):
            self._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)

        while batch_status['state'] not in ['Completed', 'Failed', 'Not Processed']:
            time.sleep(BATCH_STATUS_POLLING_SLEEP)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)

        return batch_status


    def bulk_query(self, catalog_entry, state):
        job_id = self._create_job(catalog_entry)
        batch_id = self._add_batch(catalog_entry, job_id, state)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        if batch_status['state'] == 'Failed':
            raise TapSalesforceException(batch_status['stateMessage'])
        return self._get_batch_results(job_id, batch_id, catalog_entry, state)
