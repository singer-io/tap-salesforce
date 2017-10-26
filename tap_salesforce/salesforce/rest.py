import singer

LOGGER = singer.get_logger()

class Rest(object):

    def __init__(self, sf):
        self.sf = sf

    def query(self, catalog_entry, state):
        query = self.sf._build_query_string(catalog_entry, state)
        #endpoint = "{}/services/data/v41.0/queryAll?q={}".format(self.sf.instance_url, query)
        return self._query_recur(query, catalog_entry, state)
        # build a query
        # execute it
        # catch a Query Timeout and rerun it somehow
        # [    ][        ][                 ]

        # s     es                           e

    def _query_recur(self, query, catalog_entry, state):
        url = "{}/services/data/v41.0/queryAll?q={}".format(self.sf.instance_url, query)
        LOGGER.info(url)
        headers = self.sf._get_standard_headers()
        try:
            while True:
                resp = self.sf._make_request('GET', url, headers=headers)
                resp_json = resp.json()

                for rec in resp_json.get('records'):
                    yield rec

                next_records_url = resp_json.get('nextRecordsUrl')

                if next_records_url is None:
                    break
                else:
                    url = "{}{}".format(self.sf.instance_url, next_records_url)

        except Exception as ex:
            LOGGER.info("raised an exception: %s", ex)
            raise ex

    # Use _make_request because that checks the rest api quota
    # TODO: Build and run a query to the rest API
    #         -- "{}/services/data/v41.0/queryAll?q={query}"
    #       Retryable Error = QUERY_TIMEOUT -- "halved end date"
    #       Retrieve results and continue looping while nextRecordsUrl exists
    #


    # query-string (str "select " (string/join ", " fields)
    #               " from " table-name
    #               (when modified-field
    #                 (str " where " modified-field " >= " job-start-date-str
    #                      (when end-date-str
    #                        (str " and " modified-field " < " end-date-str))
    #                      " order by " modified-field " ASC"
    #                      (when limit
    #                        (str " LIMIT " limit)))))
