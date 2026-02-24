"""
This module provides the main interface for interacting with Salesforce.

It includes the `Salesforce` class, which handles authentication, API requests,
and quota management. It also contains utility functions for converting
Salesforce field types to JSON schema and for logging backoff attempts.
"""
import re
import threading
import time
import backoff
import requests
from requests.exceptions import RequestException
import singer
import singer.utils as singer_utils
from singer import metadata, metrics

from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.rest import Rest
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException,
    TapSalesforceQuotaExceededException)

LOGGER = singer.get_logger()

# The minimum expiration setting for SF Refresh Tokens is 15 minutes
REFRESH_TOKEN_EXPIRATION_PERIOD = 900

BULK_API_TYPE = "BULK"
REST_API_TYPE = "REST"

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
    'email',
    'complexvalue',  # TODO: Unverified
    'masterrecord',
    'datacategorygroupreference'
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

BINARY_TYPES = set([
    'base64',
    'byte'
])

LOOSE_TYPES = set([
    'anyType',

    # A calculated field's type can be any of the supported
    # formula data types (see https://developer.salesforce.com/docs/#i1435527)
    'calculated'
])


# The following objects are not supported by the bulk API.
UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS = set(['AssetTokenEvent',
                                               'AttachedContentNote',
                                               'EventWhoRelation',
                                               'QuoteTemplateRichTextData',
                                               'TaskWhoRelation',
                                               'SolutionStatus',
                                               'ContractStatus',
                                               'RecentlyViewed',
                                               'DeclinedEventRelation',
                                               'AcceptedEventRelation',
                                               'TaskStatus',
                                               'PartnerRole',
                                               'TaskPriority',
                                               'CaseStatus',
                                               'UndecidedEventRelation',
                                               'OrderStatus',
                                               'FieldSecurityClassification'])

# The following objects have certain WHERE clause restrictions so we exclude them.
QUERY_RESTRICTED_SALESFORCE_OBJECTS = set(['Announcement',
                                           'ContentDocumentLink',
                                           'CollaborationGroupRecord',
                                           'Vote',
                                           'IdeaComment',
                                           'FieldDefinition',
                                           'PlatformAction',
                                           'UserEntityAccess',
                                           'RelationshipInfo',
                                           'ContentFolderMember',
                                           'ContentFolderItem',
                                           'SearchLayout',
                                           'SiteDetail',
                                           'EntityParticle',
                                           'OwnerChangeOptionInfo',
                                           'DataStatistics',
                                           'UserFieldAccess',
                                           'PicklistValueInfo',
                                           'RelationshipDomain',
                                           'FlexQueueItem',
                                           'NetworkUserHistoryRecent',
                                           'FieldHistoryArchive',
                                           'items_Google_Drive__x',
                                           'DatacloudAddress',
                                           'ContentHubItem',
                                           'RecordActionHistory',
                                           'FlowVersionView',
                                           'FlowVariableView',
                                           'AppTabMember',
                                           'ColorDefinition',
                                           'IconDefinition',])

# The following objects are not supported by the query method being used.
# EntityDefinition added as we have too many objects there for now and queryMore() does not work with that endpoint
QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS = set(['DataType',
                                             'ListViewChartInstance',
                                             'FeedLike',
                                             'OutgoingEmail',
                                             'OutgoingEmailRelation',
                                             'FeedSignal',
                                             'ActivityHistory',
                                             'EmailStatus',
                                             'UserRecordAccess',
                                             'Name',
                                             'AggregateResult',
                                             'OpenActivity',
                                             'ProcessInstanceHistory',
                                             'OwnedContentDocument',
                                             'FolderedContentDocument',
                                             'FeedTrackedChange',
                                             'CombinedAttachment',
                                             'AttachedContentDocument',
                                             'ContentBody',
                                             'NoteAndAttachment',
                                             'LookedUpFromActivity',
                                             'AttachedContentNote',
                                             'QuoteTemplateRichTextData',
                                             'EntityDefinition'])

def log_backoff_attempt(details):
    """
    Logs a backoff attempt.

    This function is used as a callback for the `backoff` library to log
    when a request is being retried.

    Args:
        details (dict): A dictionary containing details about the backoff attempt,
                        including the number of tries.
    """
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))


def field_to_property_schema(field, mdata): # pylint:disable=too-many-branches
    """
    Converts a Salesforce field definition to a JSON schema property.

    This function maps Salesforce data types to their corresponding JSON schema
    types and formats. It also updates the metadata for the field.

    Args:
        field (dict): The Salesforce field definition.
        mdata (dict): The metadata for the stream.

    Returns:
        tuple: A tuple containing the property schema (dict) and updated metadata (dict).
    """
    property_schema = {}

    field_name = field['name']
    sf_type = field['type']

    if sf_type in STRING_TYPES:
        property_schema['type'] = "string"
    elif sf_type in DATE_TYPES:
        date_type = {"type": "string", "format": "date-time"}
        string_type = {"type": ["string", "null"]}
        property_schema["anyOf"] = [date_type, string_type]
    elif sf_type == "boolean":
        property_schema['type'] = "boolean"
    elif sf_type in NUMBER_TYPES:
        property_schema['type'] = "number"
    elif sf_type == "address":
        property_schema['type'] = "object"
        property_schema['properties'] = {
            "street": {"type": ["null", "string"]},
            "state": {"type": ["null", "string"]},
            "postalCode": {"type": ["null", "string"]},
            "city": {"type": ["null", "string"]},
            "country": {"type": ["null", "string"]},
            "longitude": {"type": ["null", "number"]},
            "latitude": {"type": ["null", "number"]},
            "geocodeAccuracy": {"type": ["null", "string"]}
        }
    elif sf_type in ("int", "long"):
        property_schema['type'] = "integer"
    elif sf_type == "time":
        property_schema['type'] = "string"
    elif sf_type in LOOSE_TYPES:
        return property_schema, mdata  # No type = all types
    elif sf_type in BINARY_TYPES:
        mdata = metadata.write(mdata, ('properties', field_name), "inclusion", "unsupported")
        mdata = metadata.write(mdata, ('properties', field_name),
                               "unsupported-description", "binary data")
        return property_schema, mdata
    elif sf_type == 'location':
        # geo coordinates are numbers or objects divided into two fields for lat/long
        property_schema['type'] = ["number", "object", "null"]
        property_schema['properties'] = {
            "longitude": {"type": ["null", "number"]},
            "latitude": {"type": ["null", "number"]}
        }
    elif sf_type == 'json':
        property_schema['type'] = "string"
    else:
        raise TapSalesforceException("Found unsupported type: {}".format(sf_type))

    # The nillable field cannot be trusted
    if field_name != 'Id' and sf_type != 'location' and sf_type not in DATE_TYPES:
        property_schema['type'] = ["null", property_schema['type']]

    return property_schema, mdata

class Salesforce():
    """
    The main class for interacting with the Salesforce API.

    This class handles authentication, session management, and making requests
    to both the REST and Bulk APIs. It also tracks API quota usage.
    """
    # pylint: disable=too-many-instance-attributes,too-many-arguments
    def __init__(self,
                 refresh_token=None,
                 token=None,
                 sf_client_id=None,
                 sf_client_secret=None,
                 quota_percent_per_run=None,
                 quota_percent_total=None,
                 is_sandbox=None,
                 select_fields_by_default=None,
                 default_start_date=None,
                 api_type=None):
        """
        Initializes the Salesforce client.

        Args:
            refresh_token (str, optional): The Salesforce refresh token.
            token (str, optional): A pre-existing access token. Defaults to None.
            sf_client_id (str, optional): The Salesforce client ID.
            sf_client_secret (str, optional): The Salesforce client secret.
            quota_percent_per_run (float, optional): The percentage of the daily
                API quota to use in a single run. Defaults to 25.
            quota_percent_total (float, optional): The total percentage of the
                daily API quota to use. Defaults to 80.
            is_sandbox (bool, optional): Whether to connect to a Salesforce sandbox.
                Defaults to False.
            select_fields_by_default (bool, optional): Whether to select all
                fields by default during discovery. Defaults to False.
            default_start_date (str, optional): The default start date for
                incremental syncs.
            api_type (str, optional): The API type to use ('BULK' or 'REST').
        """
        self.api_type = api_type.upper() if api_type else None
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        if isinstance(quota_percent_per_run, str) and quota_percent_per_run.strip() == '':
            quota_percent_per_run = None
        if isinstance(quota_percent_total, str) and quota_percent_total.strip() == '':
            quota_percent_total = None
        self.quota_percent_per_run = float(
            quota_percent_per_run) if quota_percent_per_run is not None else 25
        self.quota_percent_total = float(
            quota_percent_total) if quota_percent_total is not None else 80
        self.is_sandbox = is_sandbox is True or (isinstance(is_sandbox, str) and is_sandbox.lower() == 'true')
        self.select_fields_by_default = select_fields_by_default is True or (isinstance(select_fields_by_default, str) and select_fields_by_default.lower() == 'true')
        self.default_start_date = default_start_date
        self.rest_requests_attempted = 0
        self.jobs_completed = 0
        self.login_timer = None
        self.data_url = "{}/services/data/v52.0/{}"
        self.tooling_url ="{}/services/data/v52.0/tooling/{}"
        self.pk_chunking = False

        # validate start_date
        singer_utils.strptime(default_start_date)

    def _get_standard_headers(self):
        """
        Constructs the standard headers for REST API requests.

        Returns:
            dict: A dictionary of HTTP headers.
        """
        return {"Authorization": "Bearer {}".format(self.access_token)}

    # pylint: disable=anomalous-backslash-in-string,line-too-long
    def check_rest_quota_usage(self, headers):
        """
        Checks the daily REST API quota usage.

        This method parses the 'Sforce-Limit-Info' header to ensure that the tap
        does not exceed the configured total quota or the per-run quota.

        Args:
            headers (dict): The HTTP headers from a Salesforce API response.

        Raises:
            TapSalesforceQuotaExceededException: If a quota limit is exceeded.
        """
        match = re.search('^api-usage=(\d+)/(\d+)$', headers.get('Sforce-Limit-Info'))

        if match is None:
            return

        remaining, allotted = map(int, match.groups())

        LOGGER.info("Used %s of %s daily REST API quota", remaining, allotted)

        percent_used_from_total = (remaining / allotted) * 100
        max_requests_for_run = int((self.quota_percent_per_run * allotted) / 100)

        if percent_used_from_total > self.quota_percent_total:
            total_message = ("Salesforce has reported {}/{} ({:3.2f}%) total REST quota " +
                             "used across all Salesforce Applications. Terminating " +
                             "replication to not continue past configured percentage " +
                             "of {}% total quota.").format(remaining,
                                                           allotted,
                                                           percent_used_from_total,
                                                           self.quota_percent_total)
            raise TapSalesforceQuotaExceededException(total_message)
        elif self.rest_requests_attempted > max_requests_for_run:
            partial_message = ("This replication job has made {} REST requests ({:3.2f}% of " +
                               "total quota). Terminating replication due to allotted " +
                               "quota of {}% per replication.").format(self.rest_requests_attempted,
                                                                       (self.rest_requests_attempted / allotted) * 100,
                                                                       self.quota_percent_per_run)
            raise TapSalesforceQuotaExceededException(partial_message)

    # pylint: disable=too-many-arguments
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
                          max_tries=10,
                          factor=2,
                          on_backoff=log_backoff_attempt)
    def _make_request(self, http_method, url, headers=None, body=None, stream=False, params=None):
        """
        Makes an HTTP request to the Salesforce API.

        This method includes error handling, timeout management, and quota checking.
        It is decorated with a backoff mechanism for connection errors.

        Args:
            http_method (str): The HTTP method to use ('GET' or 'POST').
            url (str): The URL to request.
            headers (dict, optional): HTTP headers. Defaults to None.
            body (dict, optional): The request body for POST requests. Defaults to None.
            stream (bool, optional): Whether to stream the response. Defaults to False.
            params (dict, optional): URL parameters for GET requests. Defaults to None.

        Returns:
            requests.Response: The HTTP response object.

        Raises:
            TapSalesforceException: For unsupported HTTP methods.
            requests.exceptions.RequestException: For other request-related errors.
        """
        # (30 seconds connect timeout, 30 seconds read timeout)
        # 30 is shorthand for (30, 30)
        request_timeout = 30
        try:
            if http_method == "GET":
                LOGGER.info("Making %s request to %s with params: %s", http_method, url, params)
                resp = self.session.get(url,
                                        headers=headers,
                                        stream=stream,
                                        params=params,
                                        timeout=request_timeout,)
            elif http_method == "POST":
                LOGGER.info("Making %s request to %s with body %s", http_method, url, body)
                resp = self.session.post(url,
                                         headers=headers,
                                         data=body,
                                         timeout=request_timeout,)
            else:
                raise TapSalesforceException("Unsupported HTTP method")
        except requests.exceptions.ConnectionError as connection_err:
            LOGGER.error('Took longer than %s seconds to connect to the server', request_timeout)
            raise connection_err
        except requests.exceptions.Timeout as timeout_err:
            LOGGER.error('Took longer than %s seconds to hear from the server', request_timeout)
            raise timeout_err



        try:
            resp.raise_for_status()
        except RequestException as ex:
            raise ex

        if resp.headers.get('Sforce-Limit-Info') is not None:
            self.rest_requests_attempted += 1
            self.check_rest_quota_usage(resp.headers)

        return resp

    def login(self):
        """
        Authenticates with Salesforce using OAuth 2.0 and obtains an access token.

        This method uses the refresh token to get a new access token. It also
        starts a timer to automatically re-login before the token expires.
        """
        if self.is_sandbox:
            login_url = 'https://test.salesforce.com/services/oauth2/token'
        else:
            login_url = 'https://login.salesforce.com/services/oauth2/token'

        login_body = {'grant_type': 'refresh_token', 'client_id': self.sf_client_id,
                      'client_secret': self.sf_client_secret, 'refresh_token': self.refresh_token}

        LOGGER.info("Attempting login via OAuth2")

        resp = None
        try:
            resp = self._make_request("POST", login_url, body=login_body, headers={"Content-Type": "application/x-www-form-urlencoded"})

            LOGGER.info("OAuth2 login successful")

            auth = resp.json()

            self.access_token = auth['access_token']
            self.instance_url = auth['instance_url']
        except Exception as e:
            error_message = str(e)
            if resp is None and hasattr(e, 'response') and e.response is not None: #pylint:disable=no-member
                resp = e.response #pylint:disable=no-member
            # NB: requests.models.Response is always falsy here. It is false if status code >= 400
            if isinstance(resp, requests.models.Response):
                error_message = error_message + ", Response from Salesforce: {}".format(resp.text)
            raise Exception(error_message) from e
        finally:
            LOGGER.info("Starting new login timer")
            self.login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD, self.login)
            self.login_timer.daemon = True # The timer should be a daemon thread so the process exits.
            self.login_timer.start()

    def describe(self, sobject=None):
        """
        Describes all objects or a specific object in Salesforce.

        This method calls the 'describe' endpoint of the REST API.

        Args:
            sobject (str, optional): The name of a specific SObject to describe.
                If None, describes all objects. Defaults to None.

        Returns:
            dict: The JSON response from the describe endpoint.
        """
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

    # pylint: disable=no-self-use
    def _get_selected_properties(self, catalog_entry):
        """
        Gets a list of selected properties from a catalog entry.

        Args:
            catalog_entry (dict): The catalog entry for a stream.

        Returns:
            list: A list of the names of selected properties.
        """
        mdata = metadata.to_map(catalog_entry['metadata'])
        properties = catalog_entry['schema'].get('properties', {})

        return [k for k in properties.keys()
                if singer.should_sync_field(metadata.get(mdata, ('properties', k), 'inclusion'),
                                            metadata.get(mdata, ('properties', k), 'selected'),
                                            self.select_fields_by_default)]


    def get_start_date(self, state, catalog_entry):
        """
        Determines the start date for a sync operation.

        The start date is determined by looking for a bookmark in the state,
        and falling back to the `default_start_date` in the config if no
        bookmark is found.

        Args:
            state (dict): The current sync state.
            catalog_entry (dict): The catalog entry for the stream.

        Returns:
            str: The start date as a string.
        """
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        return (singer.get_bookmark(state,
                                    catalog_entry['tap_stream_id'],
                                    replication_key) or self.default_start_date)

    def _build_query_string(self, catalog_entry, start_date, end_date=None, order_by_clause=True):
        """
        Builds a SOQL query string for a given stream.

        The query includes all selected fields and a WHERE clause for incremental
        syncs based on the replication key and start/end dates.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            start_date (str): The start date for the query.
            end_date (str, optional): The end date for the query. Defaults to None.
            order_by_clause (bool, optional): Whether to include an ORDER BY clause.
                Defaults to True.

        Returns:
            str: The constructed SOQL query string.
        """
        selected_properties = self._get_selected_properties(catalog_entry)

        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry['stream'])

        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        if replication_key:
            where_clause = " WHERE {} >= {} ".format(
                replication_key,
                start_date)
            if end_date:
                end_date_clause = " AND {} < {}".format(replication_key, end_date)
            else:
                end_date_clause = ""

            order_by = " ORDER BY {} ASC".format(replication_key)
            if order_by_clause:
                return query + where_clause + end_date_clause + order_by

            return query + where_clause + end_date_clause
        else:
            return query

    def query(self, catalog_entry, state):
        """
        Queries Salesforce for a given stream using the specified API type.

        This method delegates the query to either the Bulk or REST API
        implementation based on the configured `api_type`.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            state (dict): The current state of the replication.

        Returns:
            list: A list of records returned by the query.

        Raises:
            TapSalesforceException: If the `api_type` is not set to REST or BULK.
        """
        if self.api_type == BULK_API_TYPE:
            bulk = Bulk(self)
            return bulk.query(catalog_entry, state)
        elif self.api_type == REST_API_TYPE:
            rest = Rest(self)
            return rest.query(catalog_entry, state)
        else:
            raise TapSalesforceException(
                "api_type should be REST or BULK was: {}".format(
                    self.api_type))

    def get_blacklisted_objects(self):
        """
        Gets the set of Salesforce objects that are blacklisted for the current API type.

        Blacklisted objects are those that are not supported by the API or have
        certain restrictions.

        Returns:
            set: A set of blacklisted Salesforce object names.

        Raises:
            TapSalesforceException: If the `api_type` is not set to REST or BULK.
        """
        if self.api_type == BULK_API_TYPE:
            return UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS.union(
                QUERY_RESTRICTED_SALESFORCE_OBJECTS).union(QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS)
        elif self.api_type == REST_API_TYPE:
            return QUERY_RESTRICTED_SALESFORCE_OBJECTS.union(QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS)
        else:
            raise TapSalesforceException(
                "api_type should be REST or BULK was: {}".format(
                    self.api_type))

    # pylint: disable=line-too-long
    def get_blacklisted_fields(self):
        """
        Gets the set of Salesforce fields that are blacklisted for the current API type.

        Blacklisted fields are those that are not supported by the API.

        Returns:
            set: A set of tuples containing the object and field names of blacklisted fields.

        Raises:
            TapSalesforceException: If the `api_type` is not set to REST or BULK.
        """
        if self.api_type == BULK_API_TYPE:
            return {('EntityDefinition', 'RecordTypesSupported'): "this field is unsupported by the Bulk API."}
        elif self.api_type == REST_API_TYPE:
            return {}
        else:
            raise TapSalesforceException(
                "api_type should be REST or BULK was: {}".format(
                    self.api_type))

    
    def soql_query_all(self, soql):
    
    #Execute an API SOQL query and return all records.
    
        headers = self._get_standard_headers()

        url = self.data_url.format(
            self.instance_url,
            "query"
        )

        params = {"q": soql}
        records = []

        while True:
            resp = self._make_request("GET", url, headers=headers, params=params)
            payload = resp.json()

            records.extend(payload.get("records", []))

            next_url = payload.get("nextRecordsUrl")
            if not next_url:
                break

            url = f"{self.instance_url}{next_url}"
            params = None

        return records

    def tooling_query_all(self, soql):
    
    #Execute a Tooling API SOQL query and return all records.
    
        headers = self._get_standard_headers()

        url = self.tooling_url.format(
            self.instance_url,
            "query"
        )

        params = {"q": soql}
        records = []

        while True:
            resp = self._make_request("GET", url, headers=headers, params=params)
            payload = resp.json()

            records.extend(payload.get("records", []))

            next_url = payload.get("nextRecordsUrl")
            if not next_url:
                break

            url = f"{self.instance_url}{next_url}"
            params = None

        return records