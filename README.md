# tap-salesforce

[![PyPI version](https://badge.fury.io/py/tap-mysql.svg)](https://badge.fury.io/py/tap-salesforce)
[![CircleCI Build Status](https://circleci.com/gh/singer-io/tap-salesforce.png)](https://circleci.com/gh/singer-io/tap-salesforce.png)


[Singer](https://www.singer.io/) tap that extracts data from a [Salesforce](https://www.salesforce.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification).

```bash
$ mkvirtualenv -p python3 tap-salesforce
$ pip install tap-salesforce
$ tap-salesforce --config config.json --discover
$ tap-salesforce --config config.json --properties properties.json --state state.json
```

# Quickstart

## Install the tap

```
> pip install tap-salesforce
```

## Create a Config file

```
{
  "client_id": "secret_client_id",
  "client_secret": "secret_client_secret",
  "instance_url": "https://test.my.salesforce.com",
  "start_date": "2017-11-02T00:00:00Z",
  "api_type": "BULK",
  "select_fields_by_default": true,
  "lookback_window": 10
}
```

The `client_id` and `client_secret` keys are your ECA(External Client App)'s secrets. Find details [here](https://help.salesforce.com/s/articleView?id=xcloud.create_a_local_external_client_app.htm&type=5) on how to create External Client App.
The `instance_url` is My Domain URL which you can find at `My Domain → My Domain Settings → My Domain Details → Current My Domain URL`
 

The `start_date` is used by the tap as a bound on SOQL queries when searching for records.  This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2018-01-08T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

The `api_type` is used to switch the behavior of the tap between using Salesforce's "REST" and "BULK" APIs. When new fields are discovered in Salesforce objects, the `select_fields_by_default` key describes whether or not the tap will select those fields by default.

The `lookback_window` (in seconds) subtracts the desired amount of seconds from the bookmark to sync past data. Recommended value: 10 seconds.

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-salesforce --config config.json --discover > properties.json
```

## Sync Data

To sync data, select fields in the `properties.json` output and run the tap.

```
> tap-salesforce --config config.json --properties properties.json [--state state.json]
```

Copyright &copy; 2017 Stitch
