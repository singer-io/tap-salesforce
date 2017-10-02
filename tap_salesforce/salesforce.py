import requests
import re
import json
import singer
from time import sleep

wait = 5

LOGGER = singer.get_logger()

class TapSalesforceException(Exception):
    pass

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
    'base64',
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

# Change this function to return a dict
# {}

# return an empty map for anyType

def sf_type_to_json_schema(sf_type, nillable):
    # TODO: figure this out  "format": "date-time"
    if sf_type in STRING_TYPES:
        s_type = "string"
    elif sf_type in DATE_TYPES:
        s_type = "string"
    elif sf_type == "boolean":
        s_type = "boolean"
    elif sf_type in NUMBER_TYPES:
        s_type = "number"
    elif sf_type == "address":
        # Addresses are compound fields and we omit those
        s_type = "string"
    elif sf_type == "int":
        s_type = "integer"
    elif sf_type == "time":
        s_type = "string"
    elif sf_type == "anyType":
        s_type = "string" # what?!
    else:
        raise TapSalesforceException("Found unsupported type: {}".format(sf_type))

    if nillable:
        return ["null", s_type]
    else:
        return s_type

class Salesforce(object):
    # instance_url, endpoint
    data_url = "{}/services/data/v40.0/{}"
    bulk_url = "{}/services/async/40.0/{}"

    def __init__(self,
                 refresh_token=None,
                 token=None,
                 sf_client_id=None,
                 sf_client_secret=None,
                 single_run_percent=None,
                 total_quota_percent=None):
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        self.single_run_percent = single_run_percent if single_run_percent is not None else 25
        self.total_quota_percent = total_quota_percent or 80
        self.rest_requests_attempted = 0

    def _get_bulk_headers(self):
        return {"X-SFDC-Session": self.access_token,
                "Content-Type": "application/json"}

    def _get_standard_headers(self):
        return {"Authorization": "Bearer {}".format(self.access_token)}

    def _update_rest_rate_limit(self, headers):
        rate_limit_header = headers.get('Sforce-Limit-Info')
        self.rate_limit = rate_limit_header

    def _handle_rate_limit(self, headers):
        match = re.search('^api-usage=(\d+)/(\d+)$', headers.get('Sforce-Limit-Info'))

        if match is None:
            return

        remaining, allotted = map(int, match.groups())

        LOGGER.info("Used {} of {} daily API quota".format(remaining, allotted))

        percent_used_from_total = (remaining / allotted) * 100
        max_requests_for_run = int((self.single_run_percent * allotted) / 100)

        if percent_used_from_total > self.total_quota_percent:
            raise TapSalesforceException("Terminating due to exceeding configured quota usage of {}% of {} allotted queries".format(
                                         self.total_quota_percent,
                                         allotted))
        elif self.rest_requests_attempted > max_requests_for_run:
            raise TapSalesforceException("Terminating due to exceeding configured quota per run of {}% of {} allotted queries".format(
                                         self.single_run_percent,
                                         allotted))


    def _bulk_update_rate_limit(self):
        url = self.data_url.format(self.instance_url, "limits")
        resp = self.session.get(url, headers=self._get_standard_headers())
        rate_limit_header = resp.headers.get('Sforce-Limit-Info')
        self.rate_limit = rate_limit_header

    def _make_request(self, http_method, url, headers=None, body=None):
        if http_method == "GET":
            LOGGER.info("Making %s request to %s", http_method, url)
            resp = self.session.get(url, headers=headers)
        elif http_method == "POST":
            LOGGER.info("Making %s request to %s with body %s", http_method, url, body)
            resp = self.session.post(url, headers=headers, data=body)
        else:
            raise TapSalesforceException("Unsupported HTTP method")

        resp.raise_for_status()

        self.rest_requests_attempted += 1
        self._handle_rate_limit(resp.headers)

        return resp.json()

    def login(self):
        login_body = {'grant_type': 'refresh_token', 'client_id': self.sf_client_id,
                      'client_secret': self.sf_client_secret, 'refresh_token': self.refresh_token}

        LOGGER.info("Attempting login via OAuth2")

        resp = self.session.post('https://login.salesforce.com/services/oauth2/token', data=login_body)
        resp.raise_for_status()

        LOGGER.info("OAuth2 login successful")

        auth = resp.json()

        self.access_token = auth['access_token']
        self.instance_url = auth['instance_url']

    def describe(self, sobject=None):
        """Describes all objects or a specific object"""
        headers = self._get_standard_headers()
        if sobject is None:
            endpoint = "sobjects"
            url = self.data_url.format(self.instance_url, endpoint)
        else:
            endpoint = "sobjects/{}/describe".format(sobject)
            url = self.data_url.format(self.instance_url, endpoint)

        return self._make_request('GET', url, headers=headers)

    def _build_bulk_query_batch(self, catalog_entry, state):
        selected_properties = [k for k, v in catalog_entry['schema']['properties'].items()
                               if v['selected'] or v['inclusion'] == 'automatic']

        # TODO: If there are no selected properties we should do something smarter
        # do we always need to select the replication key (SystemModstamp, or LastModifiedDate, etc)?
        #

        replication_key = catalog_entry['replication_key']

        if replication_key:
            where_clause = " WHERE {} >= {} ORDER BY {} ASC".format(
                replication_key,
                singer.get_bookmark(state,
                                    catalog_entry['tap_stream_id'],
                                    replication_key),
            replication_key)
        else:
            where_clause = ""

        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry['stream'])

        return query + where_clause

    def _get_batch(self, job_id, batch_id):
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        return self._make_request('GET', url, headers=self._get_bulk_headers())

    def _get_batch_results(self, job_id, batch_id):
        headers = self._get_bulk_headers()
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        batch_result_list = self._make_request('GET', url, headers=headers)

        results = []
        for result in batch_result_list:
            endpoint = "job/{}/batch/{}/result/{}".format(job_id, batch_id, result)
            url = self.bulk_url.format(self.instance_url, endpoint)
            result_response = self._make_request('GET', url, headers=headers)

            for rec in result_response:
                yield rec

    def _create_job(self, catalog_entry):
        url = self.bulk_url.format(self.instance_url, "job")
        body = {"operation": "queryAll", "object": catalog_entry['stream'], "contentType": "JSON"}

        job = self._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))
        return job['id']

    def _add_batch(self, catalog_entry, job_id, state):
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        body = self._build_bulk_query_batch(catalog_entry, state)

        batch = self._make_request('POST', url, headers=self._get_bulk_headers(), body=body)
        return batch['id']

    def _close_job(self, job_id):
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        body = {"state": "Closed"}

        self._make_request('POST', url, headers=self._get_bulk_headers(), body=json.dumps(body))

    def _poll_on_batch_status(self, job_id, batch_id):
        batch_status = self._get_batch(job_id=job_id,
                                       batch_id=batch_id)['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=job_id,
                                           batch_id=batch_id)['state']

        return batch_status


    def bulk_query(self, catalog_entry, state):
        self._bulk_update_rate_limit()

        job_id = self._create_job(catalog_entry)
        batch_id = self._add_batch(catalog_entry, job_id, state)

        self._close_job(job_id)

        batch_status = self._poll_on_batch_status(job_id, batch_id)

        # TODO: should we raise if the batch status is 'failed'?

        return self._get_batch_results(job_id, batch_id)
