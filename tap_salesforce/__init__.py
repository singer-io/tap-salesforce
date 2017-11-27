#!/usr/bin/env python3
import json
import sys
import time
import singer
import singer.metrics as metrics
import singer.utils as singer_utils
from singer import (metadata,
                    transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)

import tap_salesforce.salesforce
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException)

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['refresh_token',
                        'client_id',
                        'client_secret',
                        'start_date',
                        'api_type',
                        'select_fields_by_default']

CONFIG = {
    'refresh_token': None,
    'client_id': None,
    'client_secret': None,
    'start_date': None
}

BLACKLISTED_FIELDS = set(['attributes'])


def get_replication_key(sobject_name, fields):
    fields_list = [f['name'] for f in fields]

    if 'SystemModstamp' in fields_list:
        return 'SystemModstamp'
    elif 'LastModifiedDate' in fields_list:
        return 'LastModifiedDate'
    elif 'CreatedDate' in fields_list:
        return 'CreatedDate'
    elif 'LoginTime' in fields_list and sobject_name == 'LoginHistory':
        return 'LoginTime'
    return None


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def build_state(raw_state, catalog):
    state = {}

    for catalog_entry in catalog['streams']:
        tap_stream_id = catalog_entry['tap_stream_id']
        replication_method = catalog_entry.get('replication_method')

        version = singer.get_bookmark(raw_state,
                                      tap_stream_id,
                                      'version')

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_entry.get('replication_key')
            replication_key_value = singer.get_bookmark(raw_state,
                                                        tap_stream_id,
                                                        replication_key)
            if version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', version)
            if replication_key_value is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, replication_key, replication_key_value)
        elif replication_method == 'FULL_TABLE' and version is None:
            if version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', version)

    return state

# pylint: disable=undefined-variable
def create_property_schema(field, mdata):
    field_name = field['name']

    if field_name == "Id":
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = salesforce.field_to_property_schema(field, mdata)

    return (property_schema, field['compoundFieldName'], mdata)


# pylint: disable=too-many-branches,too-many-statements
def do_discover(sf):
    """Describes a Salesforce instance's objects and generates a JSON schema for each field."""
    global_description = sf.describe()

    objects_to_discover = set([o['name']
                               for o in global_description['sobjects']])
    key_properties = ['Id']

    sf_custom_setting_objects = []
    object_to_tag_references = {}

    # For each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject_name in objects_to_discover:

        # Skip blacklisted SF objects depending on the api_type in use
        if sobject_name in sf.get_blacklisted_objects():
            continue

        sobject_description = sf.describe(sobject_name)

        # Cache customSetting and Tag objects to check for blacklisting after
        # all objects have been described
        if sobject_description.get("customSetting"):
            sf_custom_setting_objects.append(sobject_name)
        elif sobject_name.endswith("__Tag"):
            relationship_field = next(
                (f for f in sobject_description["fields"] if f.get("relationshipName") == "Item"),
                None)
            if relationship_field:
                # Map {"Object":"Object__Tag"}
                object_to_tag_references[relationship_field["referenceTo"]
                                         [0]] = sobject_name

        fields = sobject_description['fields']
        replication_key = get_replication_key(sobject_name, fields)

        unsupported_fields = set()
        properties = {}
        mdata = metadata.new()

        found_id_field = False

        # Loop over the object's fields
        for f in fields:
            field_name = f['name']

            if field_name == "Id":
                found_id_field = True

            property_schema, compound_field_name, mdata = create_property_schema(
                f, mdata)

            # Compound fields cannot be queried by the Bulk API
            if compound_field_name and sf.api_type == tap_salesforce.salesforce.BULK_API_TYPE:
                unsupported_fields.add(
                    (compound_field_name, 'cannot query compound fields with bulk API'))

            # Blacklisted fields are dependent on the api_type being used
            field_pair = (sobject_name, field_name)
            if field_pair in sf.get_blacklisted_fields():
                unsupported_fields.add(
                    (field_name, sf.get_blacklisted_fields()[field_pair]))

            inclusion = metadata.get(
                mdata, ('properties', field_name), 'inclusion')

            if sf.select_fields_by_default and inclusion != 'unsupported':
                mdata = metadata.write(
                    mdata, ('properties', field_name), 'selected-by-default', True)

            properties[field_name] = property_schema

        if replication_key:
            mdata = metadata.write(
                mdata, ('properties', replication_key), 'inclusion', 'automatic')

        # There are cases where compound fields are referenced by the associated
        # subfields but are not actually present in the field list
        field_name_set = set([f['name'] for f in fields])
        filtered_unsupported_fields = [f for f in unsupported_fields if f[0] in field_name_set]
        missing_unsupported_field_names = [f[0] for f in unsupported_fields if f[0] not in field_name_set]

        if missing_unsupported_field_names:
            LOGGER.info("Ignoring the following unsupported fields for object %s as they are missing from the field list: %s",
                        sobject_name,
                        ', '.join(sorted(missing_unsupported_field_names)))

        if filtered_unsupported_fields:
            LOGGER.info("Not syncing the following unsupported fields for object %s: %s",
                        sobject_name,
                        ', '.join(sorted([k for k, _ in filtered_unsupported_fields])))

        # Salesforce Objects are skipped when they do not have an Id field
        if not found_id_field:
            LOGGER.info(
                "Skipping Salesforce Object %s, as it has no Id field",
                sobject_name)
            continue

        # Any property added to unsupported_fields has metadata generated and
        # removed
        for prop, description in filtered_unsupported_fields:
            if metadata.get(mdata, ('properties', prop),
                            'selected-by-default'):
                metadata.delete(
                    mdata, ('properties', prop), 'selected-by-default')

            mdata = metadata.write(
                mdata, ('properties', prop), 'unsupported-description', description)
            mdata = metadata.write(
                mdata, ('properties', prop), 'inclusion', 'unsupported')

        if replication_key:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', [replication_key])
        else:
            mdata = metadata.write(
                mdata,
                (),
                'forced-replication-method',
                {
                    'replication-method': 'FULL_TABLE',
                    'reason': 'No replication keys found from the Salesforce API'})

        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': properties,
            'key_properties': key_properties,
        }

        entry = {
            'stream': sobject_name,
            'tap_stream_id': sobject_name,
            'schema': schema,
            'key_properties': key_properties,
            'metadata': metadata.to_list(mdata)
        }

        entries.append(entry)

    # For each custom setting field, remove its associated tag from entries
    # See Blacklisting.md for more information
    unsupported_tag_objects = [object_to_tag_references[f]
                               for f in sf_custom_setting_objects if f in object_to_tag_references]
    if unsupported_tag_objects:
        LOGGER.info(
            "Skipping the following Tag objects, Tags on Custom Settings Salesforce objects " +
            "are not supported by the Bulk API:")
        LOGGER.info(unsupported_tag_objects)
        entries = [e for e in entries if e['stream']
                   not in unsupported_tag_objects]

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)


def remove_blacklisted_fields(data):
    return {k: v for k, v in data.items() if k not in BLACKLISTED_FIELDS}

# pylint: disable=unused-argument
def transform_bulk_data_hook(data, typ, schema):
    result = data
    if isinstance(data, dict):
        result = remove_blacklisted_fields(data)

    # Salesforce Bulk API returns CSV's with empty strings for text fields.
    # When the text field is nillable and the data value is an empty string,
    # change the data so that it is None.
    if data == "" and "null" in schema['type']:
        result = None

    return result


def get_stream_version(catalog_entry, state):
    tap_stream_id = catalog_entry['tap_stream_id']
    replication_key = catalog_entry.get('replication_key')

    stream_version = (singer.get_bookmark(state, tap_stream_id, 'version') or
                      int(time.time() * 1000))

    if replication_key:
        return stream_version

    return int(time.time() * 1000)


def do_sync(sf, catalog, state, start_time):
    for catalog_entry in catalog['streams']:
        mdata = metadata.to_map(catalog_entry['metadata'])
        is_selected = stream_is_selected(mdata)

        if not is_selected:
            continue

        stream = catalog_entry['stream']
        schema = catalog_entry['schema']
        stream_alias = catalog_entry.get('stream_alias')

        replication_key = catalog_entry.get('replication_key')

        bookmark_is_empty = state.get(
            'bookmarks', {}).get(
                catalog_entry['tap_stream_id']) is None
        stream_version = get_stream_version(catalog_entry, state)
        activate_version_message = singer.ActivateVersionMessage(
            stream=(stream_alias or stream), version=stream_version)

        LOGGER.info('Syncing Salesforce data for stream %s', stream)
        singer.write_schema(
            stream,
            schema,
            catalog_entry['key_properties'],
            stream_alias)

        # Tables with a replication_key or an empty bookmark will emit an
        # activate_version at the beginning of their sync
        if replication_key or bookmark_is_empty:
            singer.write_message(activate_version_message)
            state = singer.write_bookmark(state,
                                          catalog_entry['tap_stream_id'],
                                          'version',
                                          stream_version)

        with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
            with metrics.job_timer('sync_table') as timer:
                timer.tags['stream'] = stream

                with metrics.record_counter(stream) as counter:
                    try:
                        for rec in sf.query(catalog_entry, state):
                            counter.increment()
                            rec = transformer.transform(rec, schema)
                            rec = fix_record_anytype(rec, schema)
                            singer.write_message(
                                singer.RecordMessage(
                                    stream=(
                                        stream_alias or stream),
                                    record=rec,
                                    version=stream_version))

                            replication_key_value = replication_key and singer_utils.strptime_with_tz(
                                rec[replication_key])

                            # Before writing a bookmark, make sure Salesforce has not given us a
                            # record with one outside our range
                            if replication_key_value and replication_key_value <= start_time:
                                state = singer.write_bookmark(
                                    state,
                                    catalog_entry['tap_stream_id'],
                                    replication_key,
                                    rec[replication_key])
                                singer.write_state(state)

                        # Tables with no replication_key will send an
                        # activate_version message for the next sync
                        if not replication_key:
                            singer.write_message(activate_version_message)
                            state = singer.write_bookmark(
                                state, catalog_entry['tap_stream_id'], 'version', None)

                        singer.write_state(state)

                    except TapSalesforceException as ex:
                        raise type(ex)("Error syncing {}: {}".format(
                            stream, ex))
                    except Exception as ex:
                        raise Exception(
                            "Unexpected error syncing {}: {}".format(
                                stream, ex)) from ex


def fix_record_anytype(rec, schema):
    """Modifies a record when the schema has no 'type' element due to a SF type of 'anyType.'
    Attempts to set the record's value for that element to an int, float, or string."""
    def try_cast(val, coercion):
        try:
            return coercion(val)
        except BaseException:
            return val

    for k, v in rec.items():
        if schema['properties'][k].get("type") is None:
            val = v
            val = try_cast(v, int)
            val = try_cast(v, float)
            if v in ["true", "false"]:
                val = (v == "true")

            if v == "":
                val = None

            rec[k] = val

    return rec


def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    sf = None
    try:
        sf = Salesforce(
            refresh_token=CONFIG['refresh_token'],
            sf_client_id=CONFIG['client_id'],
            sf_client_secret=CONFIG['client_secret'],
            quota_percent_total=CONFIG.get('quota_percent_total'),
            quota_percent_per_run=CONFIG.get('quota_percent_per_run'),
            is_sandbox=CONFIG.get('is_sandbox'),
            select_fields_by_default=CONFIG.get('select_fields_by_default'),
            default_start_date=CONFIG.get('start_date'),
            api_type=CONFIG.get('api_type'))
        sf.login()

        if args.discover:
            do_discover(sf)
        elif args.properties:
            start_time = singer_utils.now()
            catalog = args.properties
            state = build_state(args.state, catalog)
            do_sync(sf, catalog, state, start_time)
    finally:
        if sf:
            if sf.rest_requests_attempted > 0:
                LOGGER.debug(
                    "This job used %s REST requests towards the Salesforce quota.",
                    sf.rest_requests_attempted)
            if sf.jobs_completed > 0:
                LOGGER.debug(
                    "Replication used %s Bulk API jobs towards the Salesforce quota.",
                    sf.jobs_completed)
            if sf.login_timer:
                sf.login_timer.cancel()


def main():
    try:
        main_impl()
    except TapSalesforceQuotaExceededException as e:
        LOGGER.critical(e)
        sys.exit(2)
    except TapSalesforceException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(e)
        raise e
