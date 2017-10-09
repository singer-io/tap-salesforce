#!/usr/bin/env python3
import json
import sys
from tap_salesforce.salesforce import (Salesforce, sf_type_to_property_schema, TapSalesforceException, TapSalesforceQuotaExceededException)

import singer
import singer.metrics as metrics
import singer.schema
from singer import utils
from singer import (Catalog,
                    transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['refresh_token', 'client_id', 'client_secret', 'start_date']

CONFIG = {
    'refresh_token': None,
    'client_id': None,
    'client_secret': None,
    'start_date': None
}

BLACKLISTED_FIELDS = set(['attributes'])

BLACKLISTED_SALESFORCE_OBJECTS = set(['ActivityHistory',
                                      'AssetTokenEvent',
                                      'EmailStatus'])

def get_replication_key(sobject_name, fields):
    fields_list = [f['name'] for f in fields]

    if 'SystemModstamp' in fields_list:
        return 'SystemModstamp'
    elif 'LastModifiedDate' in fields_list:
        return 'LastModifiedDate'
    elif 'CreatedDate' in fields_list:
        return 'CreatedDate'
    elif  'LoginTime' in fields_list and sobject_name == 'LoginHistory':
        return 'LoginTime'
    else:
        return None

def build_state(raw_state, catalog):
    state = {}

    for catalog_entry in catalog.streams:
        tap_stream_id = catalog_entry.tap_stream_id
        replication_key = catalog_entry.replication_key

        if catalog_entry.schema.selected and replication_key:
            replication_key_value = singer.get_bookmark(raw_state,
                                                        tap_stream_id,
                                                        replication_key)

            if replication_key_value:
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              replication_key,
                                              replication_key_value)
            else:
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              replication_key,
                                              CONFIG['start_date'])
    return state

def create_property_schema(field):
    if field['name'] == "Id":
        inclusion = "automatic"
    else:
        inclusion = "available"

    property_schema = sf_type_to_property_schema(field['type'], field['nillable'], inclusion, False)
    return (property_schema, field['compoundFieldName'])

# dumps a catalog to stdout
def do_discover(salesforce):
    # describe all
    global_description = salesforce.describe()

    key_properties = ['Id']

    # for each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject in global_description['sobjects']:
        sobject_name = sobject['name']

        if sobject_name in BLACKLISTED_SALESFORCE_OBJECTS:
            continue

        sobject_description = salesforce.describe(sobject_name)

        fields = sobject_description['fields']
        replication_key = get_replication_key(sobject_name, fields)

        compound_fields = set()
        properties = {}
        found_id_field = False

        for f in fields:
            if f['name'] == "Id":
                found_id_field = True

            property_schema, compound_field_name = create_property_schema(f)

            if compound_field_name:
                compound_fields.add(compound_field_name)

            properties[f['name']] = property_schema

        if replication_key:
            properties[replication_key]['inclusion'] = "automatic"

        if len(compound_fields) > 0:
            LOGGER.info("Not syncing the following compound fields for object {}: {}".format(
                sobject_name,
                ', '.join(sorted(compound_fields))))

        if not found_id_field:
            LOGGER.info("Skipping Salesforce Object %s, as it has no Id field", sobject_name)
            continue

        schema = {
            'type': 'object',
            'additionalProperties': False,
            'selected': False,
            'properties': {k:v for k,v in properties.items() if k not in compound_fields},
            'key_properties': key_properties
        }

        entry = {
            'stream': sobject_name,
            'tap_stream_id': sobject_name,
            'schema': schema,
            'key_properties': key_properties,
            'replication_method': 'FULL_TABLE'
        }

        if replication_key:
            entry['replication_key'] = replication_key
            entry['replication_method'] = 'INCREMENTAL'

        entries.append(entry)

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)

def remove_blacklisted_fields(data):
    return {k:v for k,v in data.items() if k not in BLACKLISTED_FIELDS}

def transform_bulk_data_hook(data, typ, schema):
    # TODO:
    # rename table: prefix with "sf_ and replace "__" with "_" (this is probably just stream aliasing used for transmuted legacy connections)
    # filter out nil PKs
    # filter out of bounds updated at values?

    result = data

    if isinstance(data, dict):
        result = remove_blacklisted_fields(data)

    return result

def do_sync(salesforce, catalog, state):
    # TODO: Before bulk query:
    # filter out unqueryables
          # "Announcement"
          # "ApexPage"
          # "CollaborationGroupRecord"
          # "ContentDocument"
          # "ContentDocumentLink"
          # "FeedItem"
          # "FieldDefinition"
          # "IdeaComment"
          # "ListViewChartInstance"
          # "Order"
          # "PlatformAction"
          # "TopicAssignment"
          # "UserRecordAccess"
          # "Attachment" ; Contains large BLOBs that IAPI v2 can't handle.
          # "DcSocialProfile"
          # ; These Datacloud* objects don't support updated-at
          # "DatacloudCompany"
          # "DatacloudContact"
          # "DatacloudOwnedEntity"
          # "DatacloudPurchaseUsage"
          # "DatacloudSocialHandle"
          # "Vote"

          # ActivityHistory
          # EmailStatus

    # Bulk Data Query
    selected_catalog_entries = [e for e in catalog.streams if e.schema.selected]

    jobs_completed = 0

    for catalog_entry in selected_catalog_entries:
        LOGGER.info('Syncing Salesforce data for stream %s', catalog_entry.stream)
        singer.write_schema(catalog_entry.stream, catalog_entry.schema.to_dict(), catalog_entry.key_properties, catalog_entry.stream_alias)

        with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
            with metrics.job_timer('sync_table') as timer:
                timer.tags['stream'] = catalog_entry.stream

                with metrics.record_counter(catalog_entry.stream) as counter:
                    replication_key = catalog_entry.replication_key

                    salesforce.check_bulk_quota_usage(jobs_completed)
                    for rec in salesforce.bulk_query(catalog_entry, state):
                        counter.increment()
                        rec = transformer.transform(rec, catalog_entry.schema.to_dict())
                        singer.write_record(catalog_entry.stream, rec, catalog_entry.stream_alias)
                        if replication_key:
                            singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  replication_key,
                                                  rec[replication_key])
                            singer.write_state(state)

                    jobs_completed += 1

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    try:
        sf = Salesforce(refresh_token=CONFIG['refresh_token'],
                        sf_client_id=CONFIG['client_id'],
                        sf_client_secret=CONFIG['client_secret'],
                        quota_percent_total=CONFIG.get('quota_percent_total', None),
                        quota_percent_per_run=CONFIG.get('quota_percent_per_run', None),
                        is_sandbox=CONFIG.get('is_sandbox', None))
        sf.login()

        if args.discover:
            do_discover(sf)
        elif args.properties:
            catalog = Catalog.from_dict(args.properties)
            state = build_state(args.state, catalog)
            do_sync(sf, catalog, state)
    finally:
        sf.login_timer.cancel()

def main():
    try:
        main_impl()
    except TapSalesforceQuotaExceededException as e:
        LOGGER.warn(e)
        sys.exit(2)
    except TapSalesforceException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(e)
        raise e
