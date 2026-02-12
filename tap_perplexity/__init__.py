#!/usr/bin/env python3
"""
Perplexity AI Tap for Singer.io

This tap extracts data from the Perplexity AI API and outputs
it in Singer format.
"""

import sys
import json
import singer
from singer import metadata, utils
from tap_perplexity.client import PerplexityClient
from tap_perplexity.streams import STREAMS
from tap_perplexity import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'api_key',
    'start_date'
]

# Special API keys that trigger mock mode
MOCK_MODE_KEYS = [
    'mock',
    'mock-api-key',
    'test-api-key',
    'pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    'demo',
]


def discover(client):
    """
    Run discovery mode - output catalog of available streams
    """
    catalog = {'streams': []}
    
    for stream_name, stream_object in STREAMS.items():
        schema = stream_object.load_schema()
        stream_metadata = stream_object.load_metadata()
        
        catalog_entry = {
            'stream': stream_name,
            'tap_stream_id': stream_name,
            'schema': schema,
            'metadata': stream_metadata,
            'key_properties': stream_object.key_properties,
            'replication_method': stream_object.replication_method,
        }
        
        if stream_object.replication_key:
            catalog_entry['replication_key'] = stream_object.replication_key
        
        catalog['streams'].append(catalog_entry)
    
    return catalog


def sync_stream(client, config, state, stream):
    """
    Sync a single stream
    """
    LOGGER.info(f"Syncing stream: {stream.tap_stream_id}")
    
    mdata = metadata.to_map(stream.metadata)
    
    if not metadata.get(mdata, (), 'selected'):
        LOGGER.info(f"Stream {stream.tap_stream_id} not selected, skipping")
        return state
    
    LOGGER.info(f"Stream {stream.tap_stream_id} is selected")
    
    # Get the stream class
    stream_obj = STREAMS.get(stream.tap_stream_id)
    if not stream_obj:
        LOGGER.warning(f"Stream {stream.tap_stream_id} not found in STREAMS")
        return state
    
    # Write schema - convert Schema object to dict if needed
    schema = stream.schema.to_dict() if hasattr(stream.schema, 'to_dict') else stream.schema
    singer.write_schema(
        stream_name=stream.stream,
        schema=schema,
        key_properties=stream.key_properties,
        bookmark_properties=[stream_obj.replication_key] if stream_obj.replication_key else []
    )
    
    # Sync the stream
    state = stream_obj.sync(client, config, state, stream)
    
    return state


def do_sync(client, config, catalog, state):
    """
    Sync all selected streams
    """
    LOGGER.info("Starting sync")
    
    for stream in catalog.get_selected_streams(state):
        state = sync_stream(client, config, state, stream)
        singer.write_state(state)
    
    LOGGER.info("Sync completed")


def main_impl():
    """
    Main implementation
    """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    
    # Check if mock mode should be used
    api_key = config['api_key']
    mock_mode = config.get('mock_mode', False) or api_key in MOCK_MODE_KEYS
    
    if mock_mode or api_key in MOCK_MODE_KEYS:
        LOGGER.info("🎭 Running in MOCK MODE (no real API calls)")
        LOGGER.info("To use real API: Get your API key from https://www.perplexity.ai/")
        from tap_perplexity.mock_client import MockPerplexityClient
        client = MockPerplexityClient(
            api_key=api_key,
            user_agent=config.get('user_agent', 'tap-perplexity-mock')
        )
    else:
        # Initialize real client
        client = PerplexityClient(
            api_key=api_key,
            user_agent=config.get('user_agent', 'tap-perplexity')
        )
    
    if args.discover:
        catalog = discover(client)
        json.dump(catalog, sys.stdout, indent=2)
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(client)
        
        state = args.state or {}
        do_sync(client, config, catalog, state)


def main():
    """
    Main entry point
    """
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
