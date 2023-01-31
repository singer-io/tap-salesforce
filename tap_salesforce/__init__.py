#!/usr/bin/env python3
import sys
from typing import Tuple, Optional, List
from datetime import datetime, timezone, date, timedelta
from dateutil.rrule import rrule, WEEKLY


import singer
import singer.utils as singer_utils
import requests


from tap_salesforce.stream import Stream
from tap_salesforce.client import Salesforce, Table
from tap_salesforce.exceptions import (
    TapSalesforceException,
    TapSalesforceQuotaExceededException,
    TapSalesforceInvalidCredentialsException,
)


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["refresh_token", "client_id", "client_secret", "start_date"]

CONFIG = {
    "refresh_token": None,
    "client_id": None,
    "client_secret": None,
    "start_date": None,
}


def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = Salesforce(
        refresh_token=CONFIG["refresh_token"],
        client_id=CONFIG["client_id"],
        client_secret=CONFIG["client_secret"],
    )

    start_date_conf = CONFIG["start_date"]

    config_start = singer_utils.strptime_with_tz(start_date_conf).astimezone(
        timezone.utc
    )
    end_time = datetime.utcnow().astimezone(timezone.utc)

    stream = Stream(args.state)
    sync_fields(sf, stream)

    for table, fields, replication_key in sf.get_tables():
        if not fields:
            LOGGER.info(
                f"skipping stream {table.name} since it does not exist on this account"
            )
            continue

        LOGGER.info(f"processing stream {table.name}")

        start_time = (
            stream.get_stream_state(table.name, replication_key) or config_start
        )

        try:
            if table.name in ["Task", "ContactHistory"]:
                previous_datetime = start_time

                for time_interval in rrule(
                    WEEKLY, dtstart=start_time, until=end_time + timedelta(days=7)
                ):
                    if previous_datetime == time_interval:
                        continue
                    sync(
                        sf,
                        stream,
                        table,
                        fields,
                        start_time=previous_datetime,
                        end_time=time_interval,
                    )
                    previous_datetime = time_interval
            else:
                sync(sf, stream, table, fields, start_time, end_time)
        except requests.exceptions.HTTPError as err:

            url = err.request.url
            method = err.request.method
            if err.response is not None:
                status_code, message, errorCode = parse_exception(err.response)
                status_code = err.response.status_code
                LOGGER.exception(
                    f"{method}: {url}\n{status_code}: {message} => {errorCode}"
                )
            else:
                LOGGER.exception(f"{method}: {url} => {str(err)}")
            raise
        finally:
            stream.write_state()


def sync(
    sf: Salesforce,
    stream: Stream,
    table: Table,
    fields: List[str],
    start_time: datetime,
    end_time: datetime,
    limit: Optional[int] = None,
):
    try:
        for record in sf.get_records(
            table,
            fields,
            start_time,
            end_date=end_time,
            limit=limit,
        ):

            stream.write_record(record, table.name)
            state_value = datetime.fromisoformat(
                record[table.replication_key][: -len("+0000")]
            )
            stream.set_stream_state(table.name, table.replication_key, state_value)
    finally:
        stream.write_state()


def sync_fields(sf: Salesforce, stream: Stream):
    objects = ["Account", "Contact", "Lead", "Opportunity"]
    for object in objects:
        stream_id = f"{object}Fields"
        fields = sf.describe(object).get("fields")
        for field in fields:
            stream.write_record(field, stream_id)


def parse_exception(resp: requests.Response) -> Tuple[int, str, str]:
    data = resp.json()
    err = data[0]
    return resp.status_code, err["message"], err["errorCode"]


@singer_utils.handle_top_exception(LOGGER)
def main():
    try:

        main_impl()
    except TapSalesforceQuotaExceededException as e:
        LOGGER.exception(str(e))
        sys.exit(2)
    except TapSalesforceInvalidCredentialsException as e:
        LOGGER.exception(str(e))
        sys.exit(5)
    except TapSalesforceException as e:
        LOGGER.exception(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
