"""
Sync functionality for Perplexity tap
"""

import singer
from singer import metadata

LOGGER = singer.get_logger()


def sync_stream(client, config, state, catalog_stream):
    """
    Generic stream sync function
    """
    stream_name = catalog_stream.tap_stream_id
    
    LOGGER.info(f"Syncing stream: {stream_name}")
    
    # Get stream metadata
    mdata = metadata.to_map(catalog_stream.metadata)
    
    # Check if stream is selected
    if not metadata.get(mdata, (), 'selected'):
        LOGGER.info(f"Stream {stream_name} not selected, skipping")
        return state
    
    # The actual sync is handled by the stream class
    from tap_perplexity.streams import STREAMS
    
    stream_obj = STREAMS.get(stream_name)
    if stream_obj:
        state = stream_obj.sync(client, config, state, catalog_stream)
    else:
        LOGGER.warning(f"Stream {stream_name} not found")
    
    return state
