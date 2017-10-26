import requests
import re
import singer
import singer.metrics as metrics
import time
import threading
from singer import metadata
from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.exceptions import (TapSalesforceException, TapSalesforceQuotaExceededException)
LOGGER = singer.get_logger()

# The minimum expiration setting for SF Refresh Tokens is 15 minutes
REFRESH_TOKEN_EXPIRATION_PERIOD = 900

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
        self.api = "BULK"
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
        self.data_url = "{}/services/data/v41.0/{}"

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

    def query(self, catalog_entry, state):
        if self.api == "BULK":
            bulk = Bulk(self)
            return bulk.query(catalog_entry, state)
        elif self.api == "REST":
            #rest
            return self.rest_api.query()
