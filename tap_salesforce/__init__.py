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
    r = salesforce.describe()
    j = r.json()

    # for each SF Object describe it, loop its fields and build a schema
    entries = []
    for sobject in j['sobjects']:
        schema = {"type": "object"}
        properties = {}
        sobject_description = salesforce.describe(sobject['name']).json()
        print(salesforce.rate_limit)
        for sobject_field in sobject_description['fields']:
            sobject_field_name = sobject_field['name']
            schema_type = sf_type_to_json_schema(sobject_field['type'], sobject_field['nillable'])
            properties[sobject_field_name] = {"type": schema_type}
        # write out a json file that is 'valid' json-schema
        with open("/tmp/{}.json".format(sobject['name']), 'w') as f:
            schema['properties'] = properties
            f.write(json.dumps(schema))

    #   ['defaultValue'] ?

def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    sf = Salesforce(refresh_token=args.config['refresh_token'],
                    sf_client_id=args.config['client_id'],
                    sf_client_secret=args.config['client_secret'])
    sf.login()

    if args.discover:
        do_discover(sf)
    #elif args.properties:
        #catalog = Catalog.from_dict(args.properties)
        #do_sync(account, catalog, args.state)
