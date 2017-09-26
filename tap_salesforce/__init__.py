#!/usr/bin/env python3
import json
from tap_salesforce.salesforce import (Salesforce, sf_type_to_json_schema)

import singer
import singer.metrics as metrics
import singer.schema
from singer import utils
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

REQUIRED_CONFIG_KEYS = ['refresh_token', 'token', 'client_id', 'client_secret']

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

# dumps a catalog to stdout
def do_discover(salesforce):
    # describe all
    global_description = salesforce.describe()

    # for each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject in global_description['sobjects']:
        sobject_name = sobject['name']
        sobject_description = salesforce.describe(sobject_name)
        print(salesforce.rate_limit)

        fields = sobject_description['fields']
        replication_key = get_replication_key(sobject_name, fields)

        properties = {f['name']: populate_properties(f) for f in fields}

        if replication_key:
            properties[replication_key].inclusion = "automatic"

        schema = Schema(type='object',
                        selected=False,
                        properties=properties)

        entry = CatalogEntry(
            stream=sobject_name,
            tap_stream_id=sobject_name,
            schema=schema,
            replication_key=replication_key)

        entries.append(entry)

    return Catalog(entries)

def transform_data_hook(data, typ, schema):
    # TODO:
    # remote Id and add id
    # remove attributes field
    # rename table: prefix with "sf_ and replace "__" with "_" (this is probably just stream aliasing used for transmuted legacy connections)
    # filter out nil PKs
    # filter out of bounds updated at values?
    return data

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
    for catalog_entry in selected_catalog_entries:

        #job = salesforce.bulk_query(catalog_entry).json()
        with Transformer(pre_hook=transform_data_hook) as transformer:
             with metrics.record_counter(catalog_entry.stream) as counter:
                for rec in salesforce.bulk_query(catalog_entry):
                    counter.increment()
                    record = transformer.transform(rec, catalog_entry.schema.to_dict())
                    singer.write_record(catalog_entry.stream, record, catalog_entry.stream_alias)


def populate_properties(field):
    if field['name'] == "Id":
        inclusion = "automatic"
    else:
        inclusion = "available"

    result = Schema(inclusion=inclusion, selected=False)
    result.type = sf_type_to_json_schema(field['type'], field['nillable'])
    return result

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    sf = Salesforce(refresh_token=args.config['refresh_token'],
                    sf_client_id=args.config['client_id'],
                    sf_client_secret=args.config['client_secret'])
    sf.login()

    if args.discover:
        with open("/tmp/catalog.json", 'w') as f:
            f.write(json.dumps(do_discover(sf).to_dict()))
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        do_sync(sf, catalog, args.state)
