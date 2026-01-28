#!/usr/bin/env python3
import json
import sys
import singer
import singer.utils as singer_utils
from singer import metadata, metrics
import tap_salesforce.salesforce
from tap_salesforce.sync import (sync_stream, resume_syncing_bulk_query, get_stream_version)
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.exceptions import (
    TapSalesforceException, TapSalesforceQuotaExceededException, TapSalesforceBulkAPIDisabledException)

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

FORCED_FULL_TABLE = {
    'BackgroundOperationResult', # Does not support ordering by CreatedDate
    'LoginEvent', # Does not support ordering by CreatedDate
}

def get_replication_key(sobject_name, fields):
    if sobject_name in FORCED_FULL_TABLE:
        return None

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
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_method = catalog_metadata.get((), {}).get('replication-method')

        version = singer.get_bookmark(raw_state,
                                      tap_stream_id,
                                      'version')

        # Preserve state that deals with resuming an incomplete bulk job
        if singer.get_bookmark(raw_state, tap_stream_id, 'JobID'):
            job_id = singer.get_bookmark(raw_state, tap_stream_id, 'JobID')
            batches = singer.get_bookmark(raw_state, tap_stream_id, 'BatchIDs')
            current_bookmark = singer.get_bookmark(raw_state, tap_stream_id, 'JobHighestBookmarkSeen')
            state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
            state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batches)
            state = singer.write_bookmark(state, tap_stream_id, 'JobHighestBookmarkSeen', current_bookmark)

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get((), {}).get('replication-key')
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
            state = singer.write_bookmark(state, tap_stream_id, 'version', version)

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

    return (property_schema, mdata)

def get_entity_definitions_for_object(sf, sobject_name):
    soql = f"""
        SELECT
        DeveloperName,
        DurableId,
        QualifiedApiName
        FROM EntityDefinition
        WHERE QualifiedApiName = '{sobject_name}'
    """
    result = sf.soql_query_all(soql)
    return {
        r["QualifiedApiName"]: r
        for r in result
    }

def get_field_definitions_for_object(sf, sobject_name):
    """Query to get metadata for each field in each object."""
    soql = f"""
    SELECT
        Id,
        DurableId,
        QualifiedApiName,
        DeveloperName,
        Description,
        DataType,
        Label,
        Precision,
        Length,
        Scale,
        IsApiGroupable,
        IsApiSortable,
        IsCompactLayoutable,
        IsCompound,
        IsFieldHistoryTracked,
        IsHighScaleNumber,
        IsHtmlFormatted,
        IsIndexed,
        IsListFilterable,
        IsListSortable,
        IsListVisible,
        IsPolymorphicForeignKey,
        IsSearchPrefilterable,
        IsWorkflowFilterable,
        IsNillable,
        IsCalculated,
        IsNameField,
        EntityDefinitionId,
        MasterLabel,
        ReferenceTargetField,
        ServiceDataTypeId,
        ValueTypeId,
        BusinessOwnerId,
        ComplianceGroup,
        ControllingFieldDefinitionId,
        ExtraTypeInfo
    FROM FieldDefinition
    WHERE EntityDefinition.QualifiedApiName = '{sobject_name}'
    ORDER BY Label ASC NULLS FIRST

    """

    records = sf.soql_query_all(soql)

    return {
        r["QualifiedApiName"]: r
        for r in records
    }

def get_customfield_metadata_for_object(sf, sobject_id, field_name):
    field_name = field_name.replace('__c', '')
    soql = f"""
        SELECT
            TableEnumOrId,
            DeveloperName,
            Metadata
        FROM CustomField
        WHERE TableEnumOrId = '{sobject_id}'
        AND DeveloperName = '{field_name}'

    """

    result = sf.tooling_query_all(soql)
    # return result
    # Map like: My_Field__c → Metadata blob
    return {
       r["DeveloperName"]: r["Metadata"]
       for r in result
    }


# pylint: disable=too-many-branches,too-many-statements
def do_discover(sf):
    """Describes a Salesforce instance's objects and generates a JSON schema for each field."""
    global_description = sf.describe()
    # objects_to_discover = {'PermissionSet'}
    objects_to_discover = {o['name'] for o in global_description['sobjects']}
    key_properties = ['Id']

    sf_custom_setting_objects = []
    object_to_tag_references = {}

    # For each SF Object describe it, loop its fields and build a schema
    entries = []

    # Check if the user has BULK API enabled
    if sf.api_type == 'BULK' and not Bulk(sf).has_permissions():
        raise TapSalesforceBulkAPIDisabledException('This client does not have Bulk API permissions, received "API_DISABLED_FOR_ORG" error code')

    for sobject_name in objects_to_discover:
        # Skip blacklisted SF objects depending on the api_type in use
        # ChangeEvent objects are not queryable via Bulk or REST (undocumented)
        if sobject_name in sf.get_blacklisted_objects() \
           or sobject_name.endswith("ChangeEvent"):
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

        field_definition_map = get_field_definitions_for_object(sf, sobject_name)
        LOGGER.info("Field Definition Map for %s is processed", sobject_name)
        #LOGGER.info("Custom Field Metadata for %s: %s", 'AcctSeed__Account_Tax__c', custom_metadata_map)
        custom_md = None
        for f in fields:
            field_name = f['name']

            if field_name == "Id":
                found_id_field = True

            property_schema, mdata = create_property_schema(
                f, mdata)

            # Compound Address fields and geolocations cannot be queried by the Bulk API
            if f['type'] in ("address", "location") and sf.api_type == tap_salesforce.salesforce.BULK_API_TYPE:
                unsupported_fields.add(
                    (field_name, 'cannot query compound address fields or geolocations with bulk API'))

            # we haven't been able to observe any records with a json field, so we
            # are marking it as unavailable until we have an example to work with
            if f['type'] == "json":
                unsupported_fields.add(
                    (field_name, 'do not currently support json fields - please contact support'))

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
            if field_definition_map:
                field_def = field_definition_map.get(field_name)
            else:
                field_def = None
            if field_name.endswith("__c"):
                entity_definition_map = get_entity_definitions_for_object(sf, sobject_name)
                durable_id = entity_definition_map.get(sobject_name, {}).get('DurableId')
                developer_name = field_def.get('DeveloperName') if field_def else None
                try:
                    custom_md = get_customfield_metadata_for_object(sf, durable_id, developer_name)
                    LOGGER.info("Custom Field Metadata for %s.%s: processed", sobject_name, field_name)
                except Exception as e:
                    LOGGER.error("Error getting custom field metadata for %s.%s: %s", sobject_name, field_name, e)
                    custom_md = None
            if custom_md:
                for key, value in custom_md.items():
                    if value is None:
                        value = "None"

                    mdata = metadata.write(
                        mdata,
                        ('properties', field_name),
                        f"SF_META_{key.upper()}",
                        value
                    )
            
            if field_def:
               field_mapping = {
    'SF_ID': 'Id',
    'SF_DURABLE_ID': 'DurableId',
    'SF_API_NAME': 'QualifiedApiName',
    'SF_DEVELOPER_NAME': 'DeveloperName',
    'SF_DESCRIPTION': 'Description',
    'SF_DATA_TYPE': 'DataType',
    'SF_FULL_NAME': 'FullName',
    'SF_LABEL': 'Label',
    'SF_PRECISION': 'Precision',
    'SF_LENGTH': 'Length',
    'SF_SCALE': 'Scale',

    'SF_IS_API_FILTERABLE': 'IsApiFilterable',
    'SF_IS_API_GROUPABLE': 'IsApiGroupable',
    'SF_IS_API_SORTABLE': 'IsApiSortable',
    'SF_IS_COMPACT_LAYOUTABLE': 'IsCompactLayoutable',
    'SF_IS_COMPOUND': 'IsCompound',
    'SF_IS_EVER_API_ACCESSIBLE': 'IsEverApiAccessible',
    'SF_IS_FIELD_HISTORY_TRACKED': 'IsFieldHistoryTracked',
    'SF_IS_FLS_ENABLED': 'IsFlsEnabled',
    'SF_IS_HIGH_SCALE_NUMBER': 'IsHighScaleNumber',
    'SF_IS_HTML_FORMATTED': 'IsHtmlFormatted',
    'SF_IS_INDEXED': 'IsIndexed',
    'SF_IS_LIST_FILTERABLE': 'IsListFilterable',
    'SF_IS_LIST_SORTABLE': 'IsListSortable',
    'SF_IS_LIST_VISIBLE': 'IsListVisible',
    'SF_IS_POLYMORPHIC_FOREIGN_KEY': 'IsPolymorphicForeignKey',
    'SF_IS_SEARCH_PREFILTERABLE': 'IsSearchPrefilterable',
    'SF_IS_WORKFLOW_FILTERABLE': 'IsWorkflowFilterable',

    'SF_IS_NILLABLE': 'IsNillable',
    'SF_IS_CALCULATED': 'IsCalculated',
    'SF_IS_NAME_FIELD': 'IsNameField',

    'SF_ENTITY_DEFINITION_ID': 'EntityDefinitionId',
    'SF_MASTER_LABEL': 'MasterLabel',

    'SF_REFERENCE_TARGET_FIELD': 'ReferenceTargetField',
    'SF_SERVICE_DATA_TYPE_ID': 'ServiceDataTypeId',
    'SF_VALUE_TYPE_ID': 'ValueTypeId',

    'SF_BUSINESS_OWNER_ID': 'BusinessOwnerId',
    'SF_COMPLIANCE_GROUP': 'ComplianceGroup',
    'SF_CONTROLLING_FIELD_DEFINITION_ID': 'ControllingFieldDefinitionId',

    'SF_EXTRA_TYPE_INFO': 'ExtraTypeInfo'
}

            if field_def:
                for meta_key, sf_key in field_mapping.items():

                    value = field_def.get(sf_key) if field_def else None
                    if value is None:
                        value = 'None'
                    mdata = metadata.write(
                        mdata,
                        ('properties', field_name),
                        meta_key,
                        value
                    )
                    LOGGER.info("Writing metadata for %s.%s: %s = %s", sobject_name, field_name, meta_key, field_def.get(sf_key))
            else:
                LOGGER.info("No field definition found for %s.%s", sobject_name, field_name)
            properties[field_name] = property_schema

        if replication_key:
            mdata = metadata.write(
                mdata, ('properties', replication_key), 'inclusion', 'automatic')

        # There are cases where compound fields are referenced by the associated
        # subfields but are not actually present in the field list
        field_name_set = {f['name'] for f in fields}
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

        mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': properties
        }

        entry = {
            'stream': sobject_name,
            'tap_stream_id': sobject_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }

        entries.append(entry)

    # For each custom setting field, remove its associated tag from entries
    # See Blacklisting.md for more information
    unsupported_tag_objects = [object_to_tag_references[f]
                               for f in sf_custom_setting_objects if f in object_to_tag_references]
    if unsupported_tag_objects:
        LOGGER.info( #pylint:disable=logging-not-lazy
            "Skipping the following Tag objects, Tags on Custom Settings Salesforce objects " +
            "are not supported by the Bulk API:")
        LOGGER.info(unsupported_tag_objects)
        entries = [e for e in entries if e['stream']
                   not in unsupported_tag_objects]

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)

    

def do_sync(sf, catalog, state):
    starting_stream = state.get("current_stream")

    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")

    for catalog_entry in catalog["streams"]:
        stream_version = get_stream_version(catalog_entry, state)
        stream = catalog_entry['stream']
        stream_alias = catalog_entry.get('stream_alias')
        stream_name = catalog_entry["tap_stream_id"]
        activate_version_message = singer.ActivateVersionMessage(
            stream=(stream_alias or stream), version=stream_version)

        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        mdata = metadata.to_map(catalog_entry['metadata'])
        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info("%s: Resuming", stream_name)
                starting_stream = None
            else:
                LOGGER.info("%s: Skipping - already synced", stream_name)
                continue
        else:
            LOGGER.info("%s: Starting", stream_name)

        state["current_stream"] = stream_name
        singer.write_state(state)
        key_properties = metadata.to_map(catalog_entry['metadata']).get((), {}).get('table-key-properties')
        singer.write_schema(
            stream,
            catalog_entry['schema'],
            key_properties,
            replication_key,
            stream_alias)

        job_id = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'JobID')
        if job_id:
            with metrics.record_counter(stream) as counter:
                LOGGER.info("Found JobID from previous Bulk Query. Resuming sync for job: %s", job_id)
                # Resuming a sync should clear out the remaining state once finished
                counter = resume_syncing_bulk_query(sf, catalog_entry, job_id, state, counter)
                LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)
                # Remove Job info from state once we complete this resumed query. One of a few cases could have occurred:
                # 1. The job succeeded, in which case make JobHighestBookmarkSeen the new bookmark
                # 2. The job partially completed, in which case make JobHighestBookmarkSeen the new bookmark, or
                #    existing bookmark if no bookmark exists for the Job.
                # 3. The job completely failed, in which case maintain the existing bookmark, or None if no bookmark
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('JobID', None)
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('BatchIDs', None)
                bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                     .pop('JobHighestBookmarkSeen', None)
                existing_bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                              .pop(replication_key, None)
                state = singer.write_bookmark(
                    state,
                    catalog_entry['tap_stream_id'],
                    replication_key,
                    bookmark or existing_bookmark) # If job is removed, reset to existing bookmark or None
                singer.write_state(state)
        else:
            # Tables with a replication_key or an empty bookmark will emit an
            # activate_version at the beginning of their sync
            bookmark_is_empty = state.get('bookmarks', {}).get(
                catalog_entry['tap_stream_id']) is None

            if replication_key or bookmark_is_empty:
                singer.write_message(activate_version_message)
                state = singer.write_bookmark(state,
                                              catalog_entry['tap_stream_id'],
                                              'version',
                                              stream_version)
            counter = sync_stream(sf, catalog_entry, state)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")

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
            catalog = args.properties
            state = build_state(args.state, catalog)
            do_sync(sf, catalog, state)
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