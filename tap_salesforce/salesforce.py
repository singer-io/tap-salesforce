import requests

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
        raise Exception("Hey now! found: {}".format(sf_type))

    if nillable:
        return ["null", s_type]
    else:
        return s_type

class Salesforce(object):

    # base_url, api_version, endpoint
    base_url = "{}/services/data/{}/{}"

    version = "v40.0"

    def __init__(self, refresh_token=None, token=None, sf_client_id=None, sf_client_secret=None):
        self.refresh_token = refresh_token
        self.token = token
        self.sf_client_id = sf_client_id
        self.sf_client_secret = sf_client_secret
        self.session = requests.Session()
        self.access_token = None
        self.instance_url = None
        # init the thing

    def _update_rate_limit(self, headers):
        rate_limit_header = headers.get('Sforce-Limit-Info')
        self.rate_limit = rate_limit_header

    def login(self):
        # return new-access-token , instance-url
        login_body = {'grant_type': 'refresh_token', 'client_id': self.sf_client_id,
                      'client_secret': self.sf_client_secret, 'refresh_token': self.refresh_token}
        resp = self.session.post('https://login.salesforce.com/services/oauth2/token', login_body).json()
        self.access_token = resp.get('access_token')
        self.instance_url = resp.get('instance_url')

    def describe(self, sobject=None):
        """Describes all objects or a specific object"""
        if sobject is None:
            endpoint = "sobjects"
            url = self.base_url.format(self.instance_url, self.version, endpoint)
            print(url)
            headers = {"Authorization": "Bearer {}".format(self.access_token)}
            resp = self.session.get(url, headers=headers)
            self._update_rate_limit(resp.headers)
            return resp
        else:
            endpoint = "sobjects/{}/describe".format(sobject)
            url = self.base_url.format(self.instance_url, self.version, endpoint)
            print(url)
            headers = {"Authorization": "Bearer {}".format(self.access_token)}
            resp = self.session.get(url, headers=headers)
            self._update_rate_limit(resp.headers)
            return resp
