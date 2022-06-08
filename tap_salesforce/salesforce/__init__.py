import re
import threading
import time
import backoff
import requests
from requests.exceptions import RequestException
import singer
import singer.utils as singer_utils
from singer import metadata, metrics
from html import escape

from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.rest import Rest
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException,
    TapSalesforceQuotaExceededException)
from tap_salesforce.salesforce.utils import getUniqueElementValueFromXmlString

LOGGER = singer.get_logger()

# The minimum expiration setting for SF Refresh Tokens is 15 minutes
REFRESH_TOKEN_EXPIRATION_PERIOD = 900

BULK_API_TYPE = "BULK"
REST_API_TYPE = "REST"

OAUTH_AUTH_TYPE = "OAUTH"
SOAP_AUTH_TYPE = "SOAP"
DEFAULT_CLIENT_ID_PREFIX = 'Tap-Salesforce'
DEFAULT_API_VERSION = '41.0'

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
                                               'OrderStatus'])

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
                                           'RecordActionHistory',
                                           'FlowVersionView',
                                           'FlowVariableView',
                                           'AppTabMember',
                                           'ColorDefinition',
                                           'IconDefinition',])

# The following objects are not supported by the query method being used.
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
                                             'QuoteTemplateRichTextData'])

def log_backoff_attempt(details):
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))


def field_to_property_schema(field, mdata): # pylint:disable=too-many-branches
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
    # pylint: disable=too-many-instance-attributes,too-many-arguments
    def __init__(self,
                 refresh_token=None,
                 token=None,
                 sf_client_id=None,
                 sf_client_secret=None,
                 sf_username=None,
                 sf_password=None,
                 sf_organization_id=None,
                 sf_security_token=None,
                 quota_percent_per_run=None,
                 quota_percent_total=None,
                 is_sandbox=None,
                 select_fields_by_default=None,
                 default_start_date=None,
                 api_type=None,
                 api_version=DEFAULT_API_VERSION,
                 auth_type=None):
        self.api_type = api_type.upper() if api_type else None
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        self.sf_username = sf_username
        self.sf_password = sf_password
        self.sf_organization_id = sf_organization_id
        self.sf_security_token = sf_security_token
        self.auth_type = auth_type
        self.api_version = api_version

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
        self.pk_chunking = False

        # validate start_date
        singer_utils.strptime(default_start_date)

    def _get_standard_headers(self):
        return {"Authorization": "Bearer {}".format(self.access_token)}

    # pylint: disable=anomalous-backslash-in-string,line-too-long
    def check_rest_quota_usage(self, headers):
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
        request_timeout = 5 * 60 # 5 minute request timeout
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
        if self.auth_type == OAUTH_AUTH_TYPE:
            self._login_oauth()
        elif self.auth_type == SOAP_AUTH_TYPE:
            session_id, instance = self._login_soap()
            self.access_token = session_id
            self.instance_url = instance

    def _login_oauth(self):
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

    def _login_soap(self):
        if self.is_sandbox is not None:
            domain = 'test' if self.is_sandbox else 'login'

        if domain is None:
            domain = 'login'

        soap_url = 'https://{domain}.salesforce.com/services/Soap/u/{sf_version}'
        
        if self.sf_client_id:
            client_id = "{prefix}/{app_name}".format(
                prefix=DEFAULT_CLIENT_ID_PREFIX,
                app_name=self.sf_client_id)
        else:
            client_id = DEFAULT_CLIENT_ID_PREFIX

        soap_url = soap_url.format(domain=domain,
                                sf_version=self.api_version)

        # pylint: disable=E0012,deprecated-method
        username = escape(self.sf_username)
        password = escape(self.sf_password)

        # Check if token authentication is used
        if self.sf_security_token is not None:
            # Security Token Soap request body
            login_soap_request_body = """<?xml version="1.0" encoding="utf-8" ?>
            <env:Envelope
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:urn="urn:partner.soap.sforce.com">
                <env:Header>
                    <urn:CallOptions>
                        <urn:client>{client_id}</urn:client>
                        <urn:defaultNamespace>sf</urn:defaultNamespace>
                    </urn:CallOptions>
                </env:Header>
                <env:Body>
                    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                        <n1:username>{username}</n1:username>
                        <n1:password>{password}{token}</n1:password>
                    </n1:login>
                </env:Body>
            </env:Envelope>""".format(
                username=username, password=password, token=self.sf_security_token,
                client_id=client_id)

        # Check if IP Filtering is used in conjunction with organizationId
        elif self.sf_organization_id is not None:
            # IP Filtering Login Soap request body
            login_soap_request_body = """<?xml version="1.0" encoding="utf-8" ?>
            <soapenv:Envelope
                    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:urn="urn:partner.soap.sforce.com">
                <soapenv:Header>
                    <urn:CallOptions>
                        <urn:client>{client_id}</urn:client>
                        <urn:defaultNamespace>sf</urn:defaultNamespace>
                    </urn:CallOptions>
                    <urn:LoginScopeHeader>
                        <urn:organizationId>{organizationId}</urn:organizationId>
                    </urn:LoginScopeHeader>
                </soapenv:Header>
                <soapenv:Body>
                    <urn:login>
                        <urn:username>{username}</urn:username>
                        <urn:password>{password}</urn:password>
                    </urn:login>
                </soapenv:Body>
            </soapenv:Envelope>""".format(
                username=username, password=password, organizationId=self.sf_organization_id,
                client_id=self.sf_client_id)
        elif username is not None and password is not None:
            # IP Filtering for non self-service users
            login_soap_request_body = """<?xml version="1.0" encoding="utf-8" ?>
            <soapenv:Envelope
                    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:urn="urn:partner.soap.sforce.com">
                <soapenv:Header>
                    <urn:CallOptions>
                        <urn:client>{client_id}</urn:client>
                        <urn:defaultNamespace>sf</urn:defaultNamespace>
                    </urn:CallOptions>
                </soapenv:Header>
                <soapenv:Body>
                    <urn:login>
                        <urn:username>{username}</urn:username>
                        <urn:password>{password}</urn:password>
                    </urn:login>
                </soapenv:Body>
            </soapenv:Envelope>""".format(
                username=username, password=password, client_id=self.sf_client_id)
        else:
            except_code = 'INVALID AUTH'
            except_msg = (
                'You must submit either a security token or organizationId for '
                'authentication'
            )
            raise Exception(except_code, except_msg)

        login_soap_request_headers = {
            'content-type': 'text/xml',
            'charset': 'UTF-8',
            'SOAPAction': 'login'
        }
        LOGGER.info("Attempting login via SOAP {}".format(soap_url))
        response = (self.session or requests).post(
            soap_url, login_soap_request_body, headers=login_soap_request_headers)

        if response.status_code != 200:
            except_code = getUniqueElementValueFromXmlString(
                response.content, 'sf:exceptionCode')
            except_msg = getUniqueElementValueFromXmlString(
                response.content, 'sf:exceptionMessage')
            raise Exception(except_code, except_msg)
        
        LOGGER.info("SOAP login successful")

        session_id = getUniqueElementValueFromXmlString(
            response.content, 'sessionId')
        server_url = getUniqueElementValueFromXmlString(
            response.content, 'serverUrl')

        protocol = server_url.split("://")[0]
        sf_instance = (server_url
                    .replace('http://', '')
                    .replace('https://', '')
                    .split('/')[0]
                    .replace('-api', ''))

        return session_id, "{}://{}".format(protocol, sf_instance)

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

    # pylint: disable=no-self-use
    def _get_selected_properties(self, catalog_entry):
        mdata = metadata.to_map(catalog_entry['metadata'])
        properties = catalog_entry['schema'].get('properties', {})

        return [k for k in properties.keys()
                if singer.should_sync_field(metadata.get(mdata, ('properties', k), 'inclusion'),
                                            metadata.get(mdata, ('properties', k), 'selected'),
                                            self.select_fields_by_default)]


    def get_start_date(self, state, catalog_entry):
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        return (singer.get_bookmark(state,
                                    catalog_entry['tap_stream_id'],
                                    replication_key) or self.default_start_date)

    def _build_query_string(self, catalog_entry, start_date, end_date=None, order_by_clause=True):
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
        if self.api_type == BULK_API_TYPE:
            return {('EntityDefinition', 'RecordTypesSupported'): "this field is unsupported by the Bulk API."}
        elif self.api_type == REST_API_TYPE:
            return {}
        else:
            raise TapSalesforceException(
                "api_type should be REST or BULK was: {}".format(
                    self.api_type))