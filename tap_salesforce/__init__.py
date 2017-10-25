#!/usr/bin/env python3
import json
import sys
import time
from tap_salesforce.salesforce import (Salesforce, TapSalesforceException, TapSalesforceQuotaExceededException)

import singer
import singer.metrics as metrics
from singer import (metadata,
                    transform,
                    utils,
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
UNSUPPORTED_BULK_API_SALESFORCE_FIELDS = {('EntityDefinition', 'RecordTypesSupported'): "this field is unsupported by the Bulk API."}

# The following objects are not supported by the bulk API.
UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS = set(['ActivityHistory',
                                               'AssetTokenEvent',
                                               'EmailStatus',
                                               'UserRecordAccess',
                                               'Name',
                                               'AggregateResult',
                                               'OpenActivity',
                                               'ProcessInstanceHistory',
                                               'SolutionStatus',
                                               'OwnedContentDocument',
                                               'FolderedContentDocument',
                                               'ContractStatus',
                                               'ContentFolderItem',
                                               'CombinedAttachment',
                                               'RecentlyViewed',
                                               'DeclinedEventRelation',
                                               'ContentBody',
                                               'AcceptedEventRelation',
                                               'LookedUpFromActivity',
                                               'TaskStatus',
                                               'PartnerRole',
                                               'NoteAndAttachment',
                                               'TaskPriority',
                                               'AttachedContentDocument',
                                               'CaseStatus',
                                               'FeedTrackedChange',
                                               'UndecidedEventRelation'])

# The following objects have certain WHERE clause restrictions so we exclude them.
QUERY_RESTRICTED_SALESFORCE_OBJECTS = set(['ContentDocumentLink',
                                           'CollaborationGroupRecord',
                                           'Vote',
                                           'IdeaComment',
                                           'FieldDefinition',
                                           'PlatformAction',
                                           'UserEntityAccess',
                                           'RelationshipInfo',
                                           'ContentFolderMember',
                                           'SearchLayout',
                                           'EntityParticle',
                                           'OwnerChangeOptionInfo',
                                           'DataStatistics',
                                           'UserFieldAccess',
                                           'PicklistValueInfo',
                                           'RelationshipDomain',
                                           'FlexQueueItem'])

# The following objects are not supported by the query method being used.
QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS = set(['ListViewChartInstance',
                                             'FeedLike',
                                             'OutgoingEmail',
                                             'OutgoingEmailRelation',
                                             'FeedSignal'])

BLACKLISTED_SALESFORCE_OBJECTS = UNSUPPORTED_BULK_API_SALESFORCE_OBJECTS.union(QUERY_RESTRICTED_SALESFORCE_OBJECTS).union(QUERY_INCOMPATIBLE_SALESFORCE_OBJECTS)

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

            state = singer.write_bookmark(state, tap_stream_id, 'version', version)
            state = singer.write_bookmark(state, tap_stream_id, replication_key, replication_key_value)
        elif replication_method == 'FULL_TABLE' and version is None:
            state = singer.write_bookmark(state, tap_stream_id, 'version', version)

    return state

def create_property_schema(field, mdata):
    field_name = field['name']

    if field_name == "Id":
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = salesforce.field_to_property_schema(field, mdata)

    return (property_schema, field['compoundFieldName'], mdata)

def do_discover(sf):
    """Describes a Salesforce instance's objects and generates a JSON schema for each field."""
    global_description = sf.describe()

    objects_to_discover = set([o['name'] for o in global_description['sobjects']])
    key_properties = ['Id']

    # For each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject_name in objects_to_discover:

        if sobject_name in BLACKLISTED_SALESFORCE_OBJECTS:
            continue

        sobject_description = sf.describe(sobject_name)

        fields = sobject_description['fields']
        replication_key = get_replication_key(sobject_name, fields)

        unsupported_fields = set()
        properties = {}
        mdata = metadata.new()

        found_id_field = False

        for f in fields:
            field_name = f['name']

            if field_name == "Id":
                found_id_field = True

            property_schema, compound_field_name, mdata = create_property_schema(f, mdata)

            if compound_field_name:
                unsupported_fields.add((compound_field_name, 'cannot query compound fields with bulk API'))

            field_pair = (sobject_name, field_name)
            if field_pair in UNSUPPORTED_BULK_API_SALESFORCE_FIELDS:
                unsupported_fields.add((field_name, UNSUPPORTED_BULK_API_SALESFORCE_FIELDS[field_pair]))

            inclusion = metadata.get(mdata, ('properties', field_name), 'inclusion')

            if sf.select_fields_by_default and inclusion != 'unsupported':
                mdata = metadata.write(mdata, ('properties', field_name), 'selected-by-default', True)

            properties[field_name] = property_schema

        if replication_key:
            mdata = metadata.write(mdata, ('properties', replication_key), 'inclusion', 'automatic')

        if len(unsupported_fields) > 0:
            LOGGER.info("Not syncing the following unsupported fields for object {}: {}".format(
                sobject_name,
                ', '.join(sorted([k for k,_ in unsupported_fields]))))

        if not found_id_field:
            LOGGER.info("Skipping Salesforce Object %s, as it has no Id field", sobject_name)
            continue

        for prop, description in unsupported_fields:
            if metadata.get(mdata, ('properties', prop), 'selected-by-default'):
                metadata.delete(mdata, ('properties', prop), 'selected-by-default')

            mdata = metadata.write(mdata, ('properties', prop), 'unsupported-description', description)
            mdata = metadata.write(mdata, ('properties', prop), 'inclusion', 'unsupported')

        if replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [replication_key])
        else:
            mdata = metadata.write(mdata,
                                   (),
                                   'forced-replication-method',
                                   {'replication_method': 'FULL_TABLE',
                                    'reason': 'No valid replication keys'})

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

    # Salesforce Bulk API returns CSV's with empty strings for text fields. When the text field is nillable
    # and the data value is an empty string, change the data so that it is None.
    if data is "" and "null" in schema['type']:
        result = None

    return result

def get_stream_version(catalog_entry, state):
    tap_stream_id = catalog_entry['tap_stream_id']
    replication_key = catalog_entry.get('replication_key')

    stream_version = (singer.get_bookmark(state, tap_stream_id, 'version') or
                      int(time.time() * 1000))

    if replication_key:
        return stream_version
    else:
        return int(time.time() * 1000)

def do_sync(sf, catalog, state):

    # Bulk Data Query
    jobs_completed = 0

    for catalog_entry in catalog['streams']:
        mdata = metadata.to_map(catalog_entry['metadata'])
        is_selected = stream_is_selected(mdata)

        if not is_selected:
            continue

        stream = catalog_entry['stream']
        schema = catalog_entry['schema']
        stream_alias = catalog_entry.get('stream_alias')

        replication_key = catalog_entry.get('replication_key')

        bookmark_is_empty = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id']) is None
        stream_version = get_stream_version(catalog_entry, state)
        activate_version_message = singer.ActivateVersionMessage(stream=(stream_alias or stream),
                                                                 version=stream_version)

        LOGGER.info('Syncing Salesforce data for stream %s', stream)
        singer.write_schema(stream, schema, catalog_entry['key_properties'], stream_alias)

        # Tables with a replication_key or an empty bookmark will emit an activate_version at the beginning of their sync
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
                      sf.check_bulk_quota_usage(jobs_completed)
                      for rec in sf.bulk_query(catalog_entry, state):
                          counter.increment()
                          rec = transformer.transform(rec, schema)
                          rec = fix_record_anytype(rec, schema)
                          singer.write_message(singer.RecordMessage(stream=(stream_alias or stream), record=rec, version=stream_version))
                          if replication_key:
                              state = singer.write_bookmark(state,
                                                            catalog_entry['tap_stream_id'],
                                                            replication_key,
                                                            rec[replication_key])
                              singer.write_state(state)

                      # Tables with no replication_key will send an activate_version message for the next sync
                      if not replication_key:
                          singer.write_message(activate_version_message)
                          state = singer.write_bookmark(state, catalog_entry['tap_stream_id'], 'version', None)

                      jobs_completed += 1
                      singer.write_state(state)

                  except TapSalesforceException as ex:
                      raise
                  except Exception as ex:
                      raise Exception("Unexpected error syncing {}: {}".format(stream, ex)) from ex

def fix_record_anytype(rec, schema):
    """Modifies a record when the schema has no 'type' element due to a SF type of 'anyType.'
    Attempts to set the record's value for that element to an int, float, or string."""
    def try_cast(val, coercion):
        try:
            return coercion(val)
        except:
            return val

    for k, v in rec.items():
        if schema['properties'][k].get("type") == None:
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
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    try:
        sf = Salesforce(refresh_token=CONFIG['refresh_token'],
                        sf_client_id=CONFIG['client_id'],
                        sf_client_secret=CONFIG['client_secret'],
                        quota_percent_total=CONFIG.get('quota_percent_total'),
                        quota_percent_per_run=CONFIG.get('quota_percent_per_run'),
                        is_sandbox=CONFIG.get('is_sandbox'),
                        select_fields_by_default=CONFIG.get('select_fields_by_default'),
                        default_start_date=CONFIG.get('start_date'))
        sf.login()

        if args.discover:
            do_discover(sf)
        elif args.properties:
            catalog = args.properties
            state = build_state(args.state, catalog)
            do_sync(sf, catalog, state)
    finally:
        if sf.login_timer:
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
