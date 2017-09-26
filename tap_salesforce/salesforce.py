import requests
import json
from time import sleep

wait = 5

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

    bulk_base_url = "{}/services/async/{}/{}"

    version = "v40.0"
    bulk_version = "40.0"

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

            headers = {"Authorization": "Bearer {}".format(self.access_token)}
            resp = self.session.get(url, headers=headers)
            self._update_rate_limit(resp.headers)
            return resp
        else:
            endpoint = "sobjects/{}/describe".format(sobject)
            url = self.base_url.format(self.instance_url, self.version, endpoint)

            headers = {"Authorization": "Bearer {}".format(self.access_token)}
            resp = self.session.get(url, headers=headers)
            self._update_rate_limit(resp.headers)
            return resp

    def bulk_query(self, catalog_entry):
        endpoint = "job"
        url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)

        headers = {"X-SFDC-Session": "{}".format(self.access_token), "Content-Type": "application/json"}
        #body = {"jobInfo": {"operation": "queryAll", "object": catalog_entry.stream, "contentType": "JSON"}}
        body = {"operation": "queryAll", "object": catalog_entry.stream, "contentType": "JSON"}

        # 1. Create a Job - POST queryAll, Object, ContentType
        job = self.session.post(url, headers=headers, data=json.dumps(body)).json()

        job_id = job['id']
        endpoint = "job/{}/batch".format(job_id)
        url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)
        body = self._build_bulk_query_batch(catalog_entry)

        # 2. Add a batch - POST SOQL to the job - "SELECT ..."
        batch = self.session.post(url, headers=headers, data=body).json()
        batch_id = batch['id']

        # 3. Close the Job
        body = {"state": "Closed"}
        endpoint = "job/{}".format(job_id)
        url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)
        self.session.post(url, headers=headers, data=json.dumps(body))

        # 4. Get batch results
        batch_status = self._get_batch(job_id=batch['jobId'],
                                       batch_id=batch_id)['state']
        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = self._get_batch(job_id=batch['jobId'],
                                           batch_id=batch_id)['state']
        batch_results = self._get_batch_results(job_id, batch_id)
        return batch_results


    def _get_batch(self, job_id, batch_id):
        headers = {"X-SFDC-Session": "{}".format(self.access_token), "Content-Type": "application/json"}
        endpoint = "job/{}/batch/{}".format(job_id, batch_id)
        url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)
        batch_result = self.session.get(url, headers=headers).json()

        return batch_result

    def _get_batch_results(self, job_id, batch_id):
        headers = {"X-SFDC-Session": "{}".format(self.access_token), "Content-Type": "application/json"}
        endpoint = "job/{}/batch/{}/result".format(job_id, batch_id)
        url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)
        batch_result_list = self.session.get(url, headers=headers).json()

        results = []
        for result in batch_result_list:
            endpoint = "job/{}/batch/{}/result/{}".format(job_id, batch_id, result)
            url = self.bulk_base_url.format(self.instance_url, self.bulk_version, endpoint)
            result_response = self.session.get(url, headers=headers).json()

            #TODO: Is there a better way to remove the `attributes` key?
            results.extend([{k:rec[k] for k in rec if k != 'attributes'} for rec in result_response])

            # TODO:
            # remote Id and add id
            # remove attributes field
            # rename table: prefix with "sf_ and replace "__" with "_" (this is probably just stream aliasing used for transmuted legacy connections)
            # filter out nil PKs
            # filter out of bounds updated at values?

        return results



    def _build_bulk_query_batch(self, catalog_entry):
        selected_properties = [k for k, v in catalog_entry.schema.properties.items() if v.selected]
        # TODO: If there are no selected properties we should do something smarter
        # TODO: do we always need to select the replication key (SystemModstamp, or LastModifiedDate, etc)?
        query = "SELECT {} FROM {}".format(",".join(selected_properties), catalog_entry.stream)
        return query
