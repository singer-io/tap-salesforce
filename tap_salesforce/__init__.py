#!/usr/bin/env python3
import json
from tap_salesforce.salesforce import (Salesforce, sf_type_to_json_schema)

import singer
import singer.metrics as metrics
import singer.schema
from singer import utils
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

REQUIRED_CONFIG_KEYS = ['refresh_token', 'token', 'client_id', 'client_secret']

# dumps a catalog to stdout
def do_discover(salesforce):
    # describe all
    global_description = salesforce.describe().json()

    # for each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject in global_description['sobjects']:
        sobject_description = salesforce.describe(sobject['name']).json()
        print(salesforce.rate_limit)
        schema = Schema(type='object',
                        selected=False,
                        properties={f['name']: populate_properties(f) for f in sobject_description['fields']})

        entry = CatalogEntry(
            stream=sobject['name'],
            tap_stream_id=sobject['name'],
            schema=schema)

        entries.append(entry)

    return Catalog(entries)

def populate_properties(field):
    result = Schema(inclusion="available", selected=False)
    result.type = sf_type_to_json_schema(field['type'], field['nillable'])
    return result

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    sf = Salesforce(refresh_token=args.config['refresh_token'],
                    sf_client_id=args.config['client_id'],
                    sf_client_secret=args.config['client_secret'])
    sf.login()

    if args.discover:
        do_discover(sf).dump()
    #elif args.properties:
        #catalog = Catalog.from_dict(args.properties)
        #do_sync(account, catalog, args.state)
