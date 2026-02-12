"""
Unit tests for streams
"""

import unittest
import json
from unittest.mock import Mock, patch, MagicMock
from tap_perplexity.streams import ModelsStream, ChatCompletionsStream, STREAMS


class TestStreams(unittest.TestCase):
    """Test cases for stream classes"""
    
    def test_streams_registry(self):
        """Test that all streams are registered"""
        self.assertIn('models', STREAMS)
        self.assertIn('chat_completions', STREAMS)
        self.assertEqual(len(STREAMS), 2)
    
    def test_models_stream_properties(self):
        """Test ModelsStream properties"""
        stream = ModelsStream()
        self.assertEqual(stream.tap_stream_id, 'models')
        self.assertEqual(stream.key_properties, ['id'])
        self.assertEqual(stream.replication_method, 'FULL_TABLE')
        self.assertIsNone(stream.replication_key)
    
    def test_chat_completions_stream_properties(self):
        """Test ChatCompletionsStream properties"""
        stream = ChatCompletionsStream()
        self.assertEqual(stream.tap_stream_id, 'chat_completions')
        self.assertEqual(stream.key_properties, ['id'])
        self.assertEqual(stream.replication_method, 'INCREMENTAL')
        self.assertEqual(stream.replication_key, 'created_at')
    
    def test_load_schema_models(self):
        """Test loading schema for models stream"""
        schema = ModelsStream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])
        self.assertIn('object', schema['properties'])
        self.assertIn('created', schema['properties'])
        self.assertIn('owned_by', schema['properties'])
    
    def test_load_schema_chat_completions(self):
        """Test loading schema for chat_completions stream"""
        schema = ChatCompletionsStream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])
        self.assertIn('created_at', schema['properties'])
        self.assertIn('model', schema['properties'])
    
    def test_load_metadata_models(self):
        """Test metadata generation for models stream"""
        metadata = ModelsStream.load_metadata()
        self.assertIsInstance(metadata, list)
        self.assertTrue(len(metadata) > 0)
        
        # Find root metadata (breadcrumb is an empty list)
        root_mdata = next((m for m in metadata if len(m.get('breadcrumb', [])) == 0), None)
        self.assertIsNotNone(root_mdata)
        self.assertEqual(root_mdata['metadata']['table-key-properties'], ['id'])
        self.assertEqual(root_mdata['metadata']['forced-replication-method'], 'FULL_TABLE')
    
    def test_load_metadata_chat_completions(self):
        """Test metadata generation for chat_completions stream"""
        metadata = ChatCompletionsStream.load_metadata()
        self.assertIsInstance(metadata, list)
        
        # Find root metadata (breadcrumb is an empty list)
        root_mdata = next((m for m in metadata if len(m.get('breadcrumb', [])) == 0), None)
        self.assertIsNotNone(root_mdata)
        self.assertEqual(root_mdata['metadata']['table-key-properties'], ['id'])
        self.assertEqual(root_mdata['metadata']['forced-replication-method'], 'INCREMENTAL')
        self.assertEqual(root_mdata['metadata']['valid-replication-keys'], ['created_at'])


class TestModelsStreamSync(unittest.TestCase):
    """Test ModelsStream sync functionality"""
    
    @patch('tap_perplexity.streams.singer.write_record')
    @patch('tap_perplexity.streams.utils.now')
    def test_sync_models(self, mock_now, mock_write_record):
        """Test syncing models stream"""
        # Setup
        mock_client = Mock()
        mock_client.get_models.return_value = [
            {'id': 'model-1', 'object': 'model', 'created': 1234567890, 'owned_by': 'perplexity'},
            {'id': 'model-2', 'object': 'model', 'created': 1234567891, 'owned_by': 'perplexity'}
        ]
        
        mock_catalog_stream = Mock()
        mock_catalog_stream.stream = 'models'
        mock_catalog_stream.schema = ModelsStream.load_schema()
        mock_catalog_stream.metadata = ModelsStream.load_metadata()
        
        config = {}
        state = {}
        
        # Execute
        result_state = ModelsStream.sync(mock_client, config, state, mock_catalog_stream)
        
        # Verify
        self.assertEqual(result_state, state)
        mock_client.get_models.assert_called_once()
        self.assertEqual(mock_write_record.call_count, 2)


class TestChatCompletionsStreamSync(unittest.TestCase):
    """Test ChatCompletionsStream sync functionality"""
    
    def test_sync_chat_completions_placeholder(self):
        """Test chat completions sync (placeholder)"""
        # Setup
        mock_client = Mock()
        mock_catalog_stream = Mock()
        config = {}
        state = {}
        
        # Execute
        result_state = ChatCompletionsStream.sync(mock_client, config, state, mock_catalog_stream)
        
        # Verify - should return state unchanged (placeholder implementation)
        self.assertEqual(result_state, state)


if __name__ == '__main__':
    unittest.main()
