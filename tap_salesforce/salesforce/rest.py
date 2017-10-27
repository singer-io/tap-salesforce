import pendulum
import singer
from requests.exceptions import HTTPError
from tap_salesforce.salesforce.exceptions import TapSalesforceException

LOGGER = singer.get_logger()

MAX_RETRIES = 4

class Rest(object):

    def __init__(self, sf):
        self.sf = sf

    def query(self, catalog_entry, state):
        start_date = self.sf._get_start_date(state, catalog_entry)
        query = self.sf._build_query_string(catalog_entry, start_date)

        return self._query_recur(query, catalog_entry, start_date)

    def _query_recur(self, query, catalog_entry, start_date_str, end_date=None, retries=MAX_RETRIES):
        params = {"q": query}
        url = "{}/services/data/v41.0/queryAll".format(self.sf.instance_url)
        headers = self.sf._get_standard_headers()

        if end_date is None:
            end_date = pendulum.now()

        if retries == 0:
            raise TapSalesforceException("Ran out of retries attempting to query Salesforce Object {}".format(catalog_entry['stream']))

        try:
            while True:
                resp = self.sf._make_request('GET', url, headers=headers, params=params)
                resp_json = resp.json()

                for rec in resp_json.get('records'):
                    yield rec

                next_records_url = resp_json.get('nextRecordsUrl')

                if next_records_url is None:
                    break
                else:
                    url = "{}{}".format(self.sf.instance_url, next_records_url)

        except HTTPError as ex:
            response = ex.response.json()
            if type(response) is list and response[0].get("errorCode") == "QUERY_TIMEOUT":
                start_date = pendulum.parse(start_date_str)
                day_range = start_date.diff(end_date).in_days()
                LOGGER.info("Salesforce returned QUERY_TIMEOUT querying %d days of %s", day_range, catalog_entry['stream'])
                retryable = True
            else:
                raise ex

        if retryable:
            start_date = pendulum.parse(start_date_str)
            half_day_range = start_date.diff(end_date).in_days() // 2
            end_date = end_date.subtract(days=half_day_range)

            if half_day_range == 0:
                raise TapSalesforceException("Attempting to query by 0 day range, this would cause infinite looping.")

            query = self.sf._build_query_string(catalog_entry, start_date.format("%Y-%m-%dT%H:%M:%SZ"), end_date.format("%Y-%m-%dT%H:%M:%SZ"))
            for record in self._query_recur(query, catalog_entry, start_date_str, end_date, retries-1):
                yield record
