from typing import Optional, Tuple, Generator, Dict, List
from datetime import datetime, timedelta
import re
import backoff
from pydantic.main import BaseModel


import singer
import requests

from tap_salesforce.exceptions import (
    SalesforceException,
    TapSalesforceOauthException,
    TapSalesforceQuotaExceededException,
    TapSalesforceInvalidCredentialsException,
    QueryLengthExceedLimit,
    build_salesforce_exception,
)
from tap_salesforce.metrics import Metrics

MAX_QUERY_LENGTH = 10000


LOGGER = singer.get_logger()


def log_backoff_attempt(details):
    LOGGER.info(
        "ConnectionError detected, triggering backoff: %d try", details.get("tries")
    )


class Table(BaseModel):
    name: str
    primary_key: Optional[str]
    replication_key: Optional[str]


class PrimaryKeyNotMatch(Exception):
    pass


class Salesforce:
    client_id: str
    client_secret: str
    session: requests.Session
    quota_percent_total: float
    quota_percent_per_run: float
    is_sandbox: bool

    _access_token: Optional[str] = None
    _instance_url: Optional[str] = None
    _token_expiration_time: Optional[datetime] = None
    _metrics_http_requests: int = 0
    _metrics: Metrics

    # CONSTANTS
    _REFRESH_TOKEN_EXPIRATION_PERIOD = 900
    _API_VERSION = "v52.0"

    def __init__(
        self,
        refresh_token,
        client_id,
        client_secret,
        quota_percent_total: float = 80.0,
        quota_percent_per_run: float = 25.0,
        is_sandbox: bool = False,
    ):
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.is_sandbox = is_sandbox

        self.quota_percent_total = quota_percent_total
        self.quota_percent_per_run = quota_percent_per_run

        self.session = requests.Session()

        self._metrics = Metrics(
            "used %.2f%% of daily Salesforce REST API Quota",
            sample_rate_seconds=60,
            logger=LOGGER,
        )

        self._login()

    def get_tables(self) -> Generator[Tuple[Table, List[str], str], None, None]:
        """returns the supported table names, as well as the replication_key"""
        tables = [
            Table(name="Account", replication_key="SystemModstamp", primary_key="Id"),
            Table(name="Contact", replication_key="SystemModstamp", primary_key="Id"),
            Table(name="ContactHistory", replication_key="CreatedDate"),
            Table(name="Lead", replication_key="SystemModstamp", primary_key="Id"),
            Table(name="Opportunity", replication_key="SystemModstamp", primary_key="Id"),
            Table(name="Campaign", replication_key="SystemModstamp"),
            Table(name="AccountContactRelation", replication_key="SystemModstamp"),
            Table(name="AccountContactRole", replication_key="SystemModstamp"),
            Table(name="OpportunityContactRole", replication_key="SystemModstamp"),
            Table(name="CampaignMember", replication_key="SystemModstamp"),
            Table(name="OpportunityHistory", replication_key="CreatedDate"),
            Table(name="AccountHistory", replication_key="CreatedDate"),
            Table(name="LeadHistory", replication_key="CreatedDate"),
            Table(name="User", replication_key="SystemModstamp"),
            Table(name="Invoice__c", replication_key="SystemModstamp"),
            Table(name="Trial__c", replication_key="SystemModstamp"),
            Table(name="Task", replication_key="SystemModstamp"),
            Table(name="Event", replication_key="SystemModstamp"),
            Table(name="RecordType", replication_key="SystemModstamp"),
            Table(name="OpportunityFieldHistory", replication_key="CreatedDate"),
            Table(name="Product2", replication_key="SystemModstamp"),
            Table(name="OpportunityLineItem", replication_key="SystemModstamp"),
            Table(name="UserRole", replication_key="SystemModstamp"),
            Table(name="Revenue_Lifecycle__c", replication_key="SystemModstamp")
        ]
        for table in tables:
            try:
                fields = self.get_fields(table.name)
                yield (table, fields, table.replication_key)
            except SalesforceException as e:
                if e.code == "NOT_FOUND":
                    LOGGER.info(f"table '{table}' not found, skipping")
                    continue
                raise e

    def describe(self, table: str) -> Dict:
        try:
            resp = self._make_request(
                "GET", f"/services/data/{self._API_VERSION}/sobjects/{table}/describe/"
            )

            return resp.json()
        except requests.exceptions.HTTPError as err:
            if err.response is None:
                raise

            if not err.response.status_code == 404:
                raise

            return {}

    def get_fields(self, table: str) -> List[str]:
        """returns a list of all fields and custom fields of a given table"""
        table_descriptions = self.describe(table)
        fields = [o["name"] for o in table_descriptions["fields"]]
        return fields

    def construct_query(
        self,
        table: Table,
        fields: List[str],
        start_date: datetime,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ):
        replication_key = table.replication_key
        primary_key = table.primary_key

        select_stm = f"SELECT {','.join(set(fields))} "
        from_stm = f"FROM {table.name} "

        if not end_date:
            end_date = datetime.utcnow()

        if replication_key is not None:
            where_stm = f"WHERE {replication_key} >= {start_date.strftime('%Y-%m-%dT%H:%M:%SZ')} "
            where_stm += (
                f" AND {replication_key} < {end_date.strftime('%Y-%m-%dT%H:%M:%SZ')} "
            )
            order_by_stm = f"ORDER BY {replication_key} ASC "
            if primary_key:
                order_by_stm += f",{primary_key} ASC"
        else:
            where_stm = ""
            order_by_stm = ""

        if limit:
            limit_stm = f"LIMIT {limit}"
        else:
            limit_stm = ""
        query = f"{select_stm} {from_stm} {where_stm} {order_by_stm} {limit_stm}"
        return query

    def field_chunker(
        self, fields: List[str], size: int
    ) -> Generator[List[str], None, None]:
        field_chunk = []
        length = 0
        index = 0
        for field in fields:
            index += 1
            length += len(field)
            field_chunk.append(field)
            if (length > size) or (index == len(fields)):
                yield field_chunk
                field_chunk = []
                length = 0

    def merge_records(
        self, paginators: List[Generator[Dict, None, None]], table: Table
    ) -> Generator[Dict, None, None]:
        for records in zip(*paginators):
            merged_record = {}
            primary_key = None
            for record in records:
                if not primary_key:
                    primary_key = record[table.primary_key]
                if primary_key != record[table.primary_key]:
                    raise PrimaryKeyNotMatch(
                        f"couldn't merge records with different primary keys: {primary_key} and {record[table.primary_key]}"
                    )
                merged_record.update(record)

            yield merged_record

    def get_records(
        self,
        table: Table,
        fields: List[str],
        start_date: datetime,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        shrink_window_factor: int = 2,
    ):

        query = self.construct_query(table, fields, start_date, end_date, limit)
        try:
            if len(query) <= MAX_QUERY_LENGTH:
                LOGGER.info(query)
                yield from self._paginate(
                    "GET",
                    f"/services/data/{self._API_VERSION}/queryAll/",
                    params={"q": query},
                )
            elif table.primary_key:
                LOGGER.info(f"query too long {len(query)}, split into subqueries")
                paginators = []
                for field_chunk in self.field_chunker(fields, 8000):
                    field_chunk.append(table.primary_key)
                    field_chunk.append(table.replication_key)
                    query = self.construct_query(
                        table,
                        field_chunk,
                        start_date,
                        end_date,
                        limit,
                    )
                    LOGGER.info(query)
                    paginators.append(
                        self._paginate(
                            "GET",
                            f"/services/data/{self._API_VERSION}/queryAll/",
                            params={"q": query},
                        )
                    )

                yield from self.merge_records(paginators, table)
            else:
                raise QueryLengthExceedLimit(
                    f"query length for table {table.name} is too long. The limit is {MAX_QUERY_LENGTH} characters."
                )

        except SalesforceException as e:
            if e.code != "QUERY_TIMEOUT":
                raise e

            nth = (end_date - start_date).total_seconds() / shrink_window_factor

            # minimum allowed window size to get_records from before raising error...
            if nth < timedelta(days=1).seconds:
                raise e

            LOGGER.info(
                f"get_records in date range [{start_date}, {end_date}] failed with timeout. Shrinking window by factor {shrink_window_factor}"
            )
            for i in range(shrink_window_factor):
                yield from self.get_records(
                    table,
                    fields,
                    start_date=start_date + timedelta(seconds=i * nth),
                    end_date=start_date + timedelta(seconds=((i + 1) * nth)),
                    limit=limit,
                    shrink_window_factor=shrink_window_factor + 1,
                )

    def _paginate(
        self,
        method: str,
        path: str,
        data: Dict = None,
        params: Dict = None,
    ) -> Generator[Dict, None, None]:
        next_page: Optional[str] = path
        while True:
            resp = self._make_request(method, next_page, data=data, params=params)

            resp_data = resp.json()
            yield from resp_data.get("records", [])
            next_page = resp_data.get("nextRecordsUrl")
            if next_page is None:
                return

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.HTTPError,
        ),
        max_tries=5,
        factor=2,
        on_backoff=log_backoff_attempt,
    )
    def _make_request(self, method, path, data=None, params=None) -> requests.Response:
        now = datetime.utcnow()

        if self._token_expiration_time is None or self._token_expiration_time < now:
            self._login()

        headers = {"Authorization": "Bearer {}".format(self._access_token)}

        url = f"{self._instance_url}{path}"
        resp = self.session.request(
            method, url, headers=headers, params=params, data=data
        )

        if resp.status_code < 200 or resp.status_code > 299:
            ex = build_salesforce_exception(resp)
            if ex:
                raise ex
            resp.raise_for_status()

        self._metrics_http_requests += 1
        self._check_rest_quota_usage(resp.headers)

        return resp

    def _login(self):
        if self.is_sandbox:
            login_url = "https://test.salesforce.com/services/oauth2/token"
        else:
            login_url = "https://login.salesforce.com/services/oauth2/token"

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        LOGGER.info("Attempting login via OAuth2")

        try:
            resp = self.session.post(
                login_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )

            resp.raise_for_status()

            LOGGER.info("OAuth2 login successful")
            auth = resp.json()

            self._access_token = auth["access_token"]
            self._instance_url = auth["instance_url"]

            self._token_expiration_time = datetime.utcnow() + timedelta(
                seconds=self._REFRESH_TOKEN_EXPIRATION_PERIOD
            )
        except requests.exceptions.HTTPError as req_ex:
            response_text = None
            if req_ex.response is not None:
                response_text = req_ex.response.text
                LOGGER.exception(response_text or str(req_ex))

                resp_json = req_ex.response.json()

                if (
                    req_ex.response.status_code == 400
                    and resp_json.get("error") == "invalid_grant"
                ):
                    raise TapSalesforceInvalidCredentialsException(
                        f"invalid credentials: (error={resp_json['error']}, description={resp_json['error_description']})"
                    )

            raise TapSalesforceOauthException(
                f"failed to refresh or login using oauth2 credentials {response_text}"
            )

    def _check_rest_quota_usage(self, headers):
        match = re.search(r"^api-usage=(\d+)/(\d+)$", headers.get("Sforce-Limit-Info"))

        if match is None:
            return

        used, total = map(int, match.groups())

        used_percent = (used / total) * 100.0

        self._metrics.gauge(used_percent)

        # ensure that we never get above `self.quota_percent_total` of the daily quota
        # Example:
        # - we want to make sure that if we run the tap multiple times,
        #   that we never spend more than 80% of the quota.
        if used_percent > self.quota_percent_total:
            raise TapSalesforceQuotaExceededException(
                f"Salesforce Daily Quota Usage: {used_percent}% is above the configured limit of {self.quota_percent_total}% of total quota."
            )

        # ensure that each execution of the tap never gets above `self.quota_percent_per_run`.
        # Example:
        # - each execution should not use more than 25% of the quota
        requests_count_percent = float(self._metrics_http_requests / total)
        if requests_count_percent > self.quota_percent_per_run:
            raise TapSalesforceQuotaExceededException(
                f"Salesforce Daily Quota Usage: this execution has spent {requests_count_percent}% of the total quota, aborting due to configured limit of {self.quota_percent_per_run}% of total quota."
            )
