"""
Stream definitions for Perplexity AI tap
"""

import os
import json
import singer
from singer import metadata, utils, Transformer
from datetime import datetime, timezone

LOGGER = singer.get_logger()


class Stream:
    """Base stream class"""
    
    tap_stream_id = None
    key_properties = []
    replication_method = None
    replication_key = None
    valid_replication_keys = []
    
    def __init__(self):
        pass
    
    @classmethod
    def get_schema_path(cls):
        """Get path to schema file"""
        return os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'schemas',
            f'{cls.tap_stream_id}.json'
        )
    
    @classmethod
    def load_schema(cls):
        """Load schema from JSON file"""
        schema_path = cls.get_schema_path()
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        return schema
    
    @classmethod
    def load_metadata(cls):
        """Generate metadata for the stream"""
        schema = cls.load_schema()
        mdata = metadata.new()
        
        mdata = metadata.write(mdata, (), 'table-key-properties', cls.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', cls.replication_method)
        
        if cls.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [cls.replication_key])
        
        for field_name in schema.get('properties', {}).keys():
            if field_name in cls.key_properties:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            elif field_name == cls.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')
        
        return metadata.to_list(mdata)
    
    @classmethod
    def sync(cls, client, config, state, catalog_stream):
        """Sync stream data - to be implemented by subclasses"""
        raise NotImplementedError


class ModelsStream(Stream):
    """Stream for Perplexity AI models"""
    
    tap_stream_id = 'models'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    replication_key = None
    
    @classmethod
    def sync(cls, client, config, state, catalog_stream):
        """Sync models stream"""
        LOGGER.info("Syncing models stream")
        
        try:
            models = client.get_models()
            
            # Convert Schema object to dict if needed
            schema = catalog_stream.schema.to_dict() if hasattr(catalog_stream.schema, 'to_dict') else catalog_stream.schema
            
            with Transformer() as transformer:
                for model in models:
                    # Transform the record according to schema
                    transformed_record = transformer.transform(
                        model,
                        schema,
                        metadata.to_map(catalog_stream.metadata)
                    )
                    
                    singer.write_record(
                        stream_name=catalog_stream.stream,
                        record=transformed_record,
                        time_extracted=utils.now()
                    )
            
            LOGGER.info(f"Synced {len(models)} models")
        except Exception as e:
            LOGGER.error(f"Error syncing models: {e}")
            raise
        
        return state


class ChatCompletionsStream(Stream):
    """
    Stream for Chat Completions
    
    Note: Perplexity AI doesn't provide a history/list endpoint for completions.
    This stream serves as a template. In production, you would:
    1. Store completion requests/responses in your own database
    2. Use this tap to extract from that database
    3. Or integrate with logging/monitoring systems
    """
    
    tap_stream_id = 'chat_completions'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    
    @classmethod
    def sync(cls, client, config, state, catalog_stream):
        """
        Sync chat completions stream
        
        This is a placeholder implementation. In a real scenario, you would:
        - Connect to a database where you store completion logs
        - Query for new completions since the last bookmark
        - Write those records
        """
        LOGGER.info("Syncing chat_completions stream (placeholder)")
        LOGGER.warning("chat_completions stream has no data source. Implement your own data source.")
        
        # Placeholder: No actual sync happens
        # bookmark = singer.get_bookmark(state, cls.tap_stream_id, cls.replication_key)
        # if not bookmark:
        #     bookmark = config['start_date']
        
        # In a real implementation:
        # completions = your_database.query_completions(since=bookmark)
        # for completion in completions:
        #     singer.write_record(...)
        #     state = singer.write_bookmark(state, cls.tap_stream_id, cls.replication_key, completion[cls.replication_key])
        
        return state


# Registry of all streams
STREAMS = {
    'models': ModelsStream(),
    'chat_completions': ChatCompletionsStream(),
}
