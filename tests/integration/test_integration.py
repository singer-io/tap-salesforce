"""
Integration tests for tap-perplexity

These tests require a valid Perplexity API key in config.json
"""

import unittest
import os
import json
from tap_perplexity.client import PerplexityClient
from tap_perplexity.streams import ModelsStream


class TestIntegration(unittest.TestCase):
    """Integration tests - require valid API credentials"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        # Try to load config
        config_path = os.path.join(os.path.dirname(__file__), '../../config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                cls.config = json.load(f)
                cls.has_config = True
        else:
            cls.has_config = False
            cls.config = {}
    
    def setUp(self):
        """Set up each test"""
        if not self.has_config:
            self.skipTest("No config.json found - skipping integration tests")
        
        self.client = PerplexityClient(
            api_key=self.config.get('api_key'),
            user_agent=self.config.get('user_agent', 'tap-perplexity-test')
        )
    
    def test_get_models_integration(self):
        """Test getting models from real API"""
        if not self.has_config:
            self.skipTest("No config.json found")
        
        try:
            models = self.client.get_models()
            self.assertIsInstance(models, list)
            
            if len(models) > 0:
                # Verify model structure
                model = models[0]
                self.assertIn('id', model)
                print(f"Found {len(models)} models")
                for model in models[:3]:  # Print first 3
                    print(f"  - {model.get('id')}")
        except Exception as e:
            self.fail(f"Failed to get models: {e}")
    
    def test_stream_sync_integration(self):
        """Test syncing models stream with real API"""
        if not self.has_config:
            self.skipTest("No config.json found")
        
        from unittest.mock import Mock
        
        mock_catalog_stream = Mock()
        mock_catalog_stream.stream = 'models'
        mock_catalog_stream.schema = ModelsStream.load_schema()
        mock_catalog_stream.metadata = ModelsStream.load_metadata()
        
        state = {}
        
        try:
            result_state = ModelsStream.sync(
                self.client,
                self.config,
                state,
                mock_catalog_stream
            )
            self.assertIsInstance(result_state, dict)
            print("Models stream sync completed successfully")
        except Exception as e:
            self.fail(f"Failed to sync models stream: {e}")


if __name__ == '__main__':
    unittest.main()
