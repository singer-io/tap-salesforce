"""
This module handles all interactions with the Salesforce REST API.

It provides functionality for making paginated queries to the Salesforce
REST API, with automatic retry logic for timeout errors.
"""
# pylint: disable=protected-access
import singer
import singer.utils as singer_utils
from requests.exceptions import HTTPError
from tap_salesforce.salesforce.exceptions import TapSalesforceException

LOGGER = singer.get_logger()

MAX_RETRIES = 4

class Rest():
    """
    A class for interacting with the Salesforce REST API.
    """

    def __init__(self, sf):
        """
        Initializes the REST API client.

        Args:
            sf (Salesforce): The main Salesforce client instance.
        """
        self.sf = sf

    def query(self, catalog_entry, state):
        """
        Executes a REST API query for a given stream.

        This method builds the SOQL query and then calls the recursive query
        method to handle pagination and retries.

        Args:
            catalog_entry (dict): The catalog entry for the stream.
            state (dict): The current sync state.

        Returns:
            generator: A generator that yields records from the query.
        """
        start_date = self.sf.get_start_date(state, catalog_entry)
        query = self.sf._build_query_string(catalog_entry, start_date)

        return self._query_recur(query, catalog_entry, start_date)

    # pylint: disable=too-many-arguments
    def _query_recur(
            self,
            query,
            catalog_entry,
            start_date_str,
            end_date=None,
            retries=MAX_RETRIES):
        """
        Recursively queries the Salesforce REST API, handling timeouts and pagination.

        If a QUERY_TIMEOUT error is received, this method will split the date
        range in half and recursively call itself for each half.

        Args:
            query (str): The SOQL query to execute.
            catalog_entry (dict): The catalog entry for the stream.
            start_date_str (str): The start date for the query as a string.
            end_date (datetime, optional): The end date for the query. Defaults to None.
            retries (int, optional): The number of retries remaining. Defaults to MAX_RETRIES.

        Yields:
            dict: A dictionary representing a single record from Salesforce.

        Raises:
            TapSalesforceException: If the query runs out of retries or if the
                                    date range becomes zero.
        """
        params = {"q": query}
        url = "{}/services/data/v52.0/queryAll".format(self.sf.instance_url)
        headers = self.sf._get_standard_headers()

        sync_start = singer_utils.now()
        if end_date is None:
            end_date = sync_start

        if retries == 0:
            raise TapSalesforceException(
                "Ran out of retries attempting to query Salesforce Object {}".format(
                    catalog_entry['stream']))

        retryable = False
        try:
            for rec in self._sync_records(url, headers, params):
                yield rec

            # If the date range was chunked (an end_date was passed), sync
            # from the end_date -> now
            if end_date < sync_start:
                next_start_date_str = singer_utils.strftime(end_date)
                query = self.sf._build_query_string(catalog_entry, next_start_date_str)
                for record in self._query_recur(
                        query,
                        catalog_entry,
                        next_start_date_str,
                        retries=retries):
                    yield record

        except HTTPError as ex:
            response = ex.response.json()
            if isinstance(response, list) and response[0].get("errorCode") == "QUERY_TIMEOUT":
                start_date = singer_utils.strptime_with_tz(start_date_str)
                day_range = (end_date - start_date).days
                LOGGER.info(
                    "Salesforce returned QUERY_TIMEOUT querying %d days of %s",
                    day_range,
                    catalog_entry['stream'])
                retryable = True
            else:
                raise ex

        if retryable:
            start_date = singer_utils.strptime_with_tz(start_date_str)
            half_day_range = (end_date - start_date) // 2
            end_date = end_date - half_day_range

            if half_day_range.days == 0:
                raise TapSalesforceException(
                    "Attempting to query by 0 day range, this would cause infinite looping.")

            query = self.sf._build_query_string(catalog_entry, singer_utils.strftime(start_date),
                                                singer_utils.strftime(end_date))
            for record in self._query_recur(
                    query,
                    catalog_entry,
                    start_date_str,
                    end_date,
                    retries - 1):
                yield record

    def _sync_records(self, url, headers, params):
        """
        Paginates through a query result and yields records.

        Args:
            url (str): The initial query URL.
            headers (dict): The HTTP headers for the request.
            params (dict): The query parameters.

        Yields:
            dict: A dictionary representing a single record from Salesforce.
        """
        while True:
            resp = self.sf._make_request('GET', url, headers=headers, params=params)
            resp_json = resp.json()

            for rec in resp_json.get('records'):
                yield rec

            next_records_url = resp_json.get('nextRecordsUrl')

            if next_records_url is None:
                break

            url = "{}{}".format(self.sf.instance_url, next_records_url)
