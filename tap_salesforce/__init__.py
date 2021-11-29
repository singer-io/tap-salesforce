#!/usr/bin/env python3
import sys
from typing import Tuple, Optional, Dict
from datetime import datetime, timezone, date, timedelta
from dateutil.rrule import rrule, WEEKLY


import singer
import singer.utils as singer_utils
import requests


from tap_salesforce.stream import Stream
from tap_salesforce.client import Salesforce, Field
from tap_salesforce.exceptions import (
    TapSalesforceException,
    TapSalesforceQuotaExceededException,
)


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "refresh_token",
    "client_id",
    "client_secret",
    "start_date",
]

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

    for stream_id, fields, replication_key in sf.get_tables():
        if not fields:
            LOGGER.info(
                f"skipping stream {stream_id} since it does not exist on this account"
            )
            continue

        LOGGER.info(f"processing stream {stream_id}")

        start_time = stream.get_stream_state(stream_id, replication_key) or config_start

        try:
            if stream_id in ["Task", "ContactHistory"]:
                previous_datetime = start_time

                for time_interval in rrule(
                    WEEKLY, dtstart=start_time, until=end_time + timedelta(days=7)
                ):
                    if previous_datetime == time_interval:
                        continue
                    sync(
                        sf,
                        stream,
                        stream_id,
                        fields,
                        replication_key,
                        start_time=previous_datetime,
                        end_time=time_interval,
                    )
                    previous_datetime = time_interval
            else:

                sync(
                    sf,
                    stream,
                    stream_id,
                    fields,
                    replication_key,
                    start_time,
                    end_time,
                )
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
    stream_id: str,
    fields: Dict[str, Field],
    replication_key: str,
    start_time: datetime,
    end_time: datetime,
    limit: Optional[int] = None,
):
    try:
        for raw_record in sf.get_records(
            stream_id,
            fields,
            replication_key,
            start_time,
            end_date=end_time,
            limit=limit,
        ):
            record = transform_record(raw_record, fields)

            stream.write_record(record, stream_id)
            state_value = record[replication_key]

            stream.set_stream_state(stream_id, replication_key, state_value)
    finally:
        stream.write_state()


def transform_record(record: Dict, fields: Dict[str, Field]) -> Dict:
    r = {}
    for k, v in record.items():
        field = fields.get(k)
        if field is None:
            continue
        if field.type == "integer" and v == "0.0":
            v = "0"
        elif field.type == "datetime" and v is not None:
            v = datetime.fromisoformat(v[: -len("+0000")])
        elif field.type == "date" and v is not None:
            v = date.fromisoformat(v)
        elif field.nullable and v == "":
            v = None

        r[k] = v

    return r


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
    except TapSalesforceException as e:
        LOGGER.exception(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
