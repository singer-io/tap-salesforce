import requests
import json
import singer
from time import sleep

wait = 5

LOGGER = singer.get_logger()

class TapSalesforceException(Exception):
    pass

# TODO: Need to fix these big time for jsonschema when we get data
def sf_type_to_json_schema(sf_type, nillable):
    # TODO: figure this out  "format": "date-time"
    if sf_type == "id":
        s_type = "string"
    elif sf_type == "datetime":
        s_type = "string"
    elif sf_type == "reference":
        s_type = "string"
    elif sf_type == "boolean":
        s_type = "boolean"
    elif sf_type == "string":
        s_type = "string"
    elif sf_type == "picklist":
        s_type = "string"
    elif sf_type == "double":
        s_type = "number"
    elif sf_type == "textarea":
        s_type = "string"
    elif sf_type == "address":
        s_type = "string"
    elif sf_type == "phone":
        s_type = "string"
    elif sf_type == "url":
        s_type = "string"
    elif sf_type == "currency":
        s_type = "string"
    elif sf_type == "int":
        s_type = "integer"
    elif sf_type == "date":
        s_type = "string"
    elif sf_type == "time":
        s_type = "string"
    elif sf_type == "multipicklist":
        s_type = "string"
    elif sf_type == "anyType":
        s_type = "string" # what?!
    elif sf_type == "combobox":
        s_type = "string"
    elif sf_type == "base64":
        s_type = "string"
    elif sf_type == "percent":
        s_type = "string"
    elif sf_type == "email":
        s_type = "string"
    elif sf_type == "complexvalue":
        s_type = "string"
    elif sf_type == "encryptedstring":
        s_type = "string"
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

    def __init__(self, refresh_token=None, token=None, sf_client_id=None, sf_client_secret=None):
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        # init the thing

    def _get_bulk_headers(self):
        return {"X-SFDC-Session": self.access_token,
                "Content-Type": "application/json"}

    def _get_standard_headers(self):
        return {"Authorization": "Bearer {}".format(self.access_token)}

    def _update_rate_limit(self, headers):
        rate_limit_header = headers.get('Sforce-Limit-Info')
        self.rate_limit = rate_limit_header

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
            raise Exception("Unsupported HTTP method")

        self._update_rate_limit(resp.headers)

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
        selected_properties = [k for k, v in catalog_entry.schema.properties.items() if v.selected or v.inclusion == 'automatic']
        # TODO: If there are no selected properties we should do something smarter
        # do we always need to select the replication key (SystemModstamp, or LastModifiedDate, etc)?
        #

        if catalog_entry.replication_key:
            where_clause = " WHERE {} >= {}".format(catalog_entry.replication_key,
                                                   singer.get_bookmark(state,
                                                                       catalog_entry.tap_stream_id,
                                                                       catalog_entry.replication_key))
        else:
            where_clause = ""

        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry.stream)

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

            removeAttributes = lambda rec: {k:rec[k]for k in rec if k != 'attributes'}
            results = [removeAttributes(rec) for rec in result_response]

            for r in results:
                yield r

    def bulk_query(self, catalog_entry, state):
        self._bulk_update_rate_limit()
        url = self.bulk_url.format(self.instance_url, "job")

        headers = self._get_bulk_headers()
        body = {"operation": "queryAll", "object": catalog_entry.stream, "contentType": "JSON"}

        # 1. Create a Job - POST queryAll, Object, ContentType
        job = self._make_request('POST', url, headers=headers, body=json.dumps(body))

        job_id = job['id']
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        body = self._build_bulk_query_batch(catalog_entry, state)

        # 2. Add a batch - POST SOQL to the job - "SELECT ..."
        batch = self._make_request('POST', url, headers=headers, body=body)
        batch_id = batch['id']

        # 3. Close the Job
        body = {"state": "Closed"}
        endpoint = "job/{}".format(job_id)
        url = self.bulk_url.format(self.instance_url, endpoint)
        self._make_request('POST', url, headers=headers, body=json.dumps(body))

        # 4. Get batch results
        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch_id)['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch_id)['state']
        return self._get_batch_results(job_id, batch_id)
