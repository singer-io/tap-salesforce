"""
Unit tests for Perplexity client
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
from tap_perplexity.client import PerplexityClient


class TestPerplexityClient(unittest.TestCase):
    """Test cases for PerplexityClient"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.api_key = "test-api-key"
        self.client = PerplexityClient(api_key=self.api_key)
    
    def test_init(self):
        """Test client initialization"""
        self.assertEqual(self.client.api_key, self.api_key)
        self.assertEqual(self.client.user_agent, 'tap-perplexity')
        self.assertIn('Authorization', self.client.session.headers)
        self.assertEqual(
            self.client.session.headers['Authorization'],
            f'Bearer {self.api_key}'
        )
    
    def test_custom_user_agent(self):
        """Test client with custom user agent"""
        custom_ua = "my-custom-agent"
        client = PerplexityClient(api_key=self.api_key, user_agent=custom_ua)
        self.assertEqual(client.user_agent, custom_ua)
        self.assertEqual(client.session.headers['User-Agent'], custom_ua)
    
    @patch('tap_perplexity.client.requests.Session.request')
    def test_get_models_success(self, mock_request):
        """Test successful get_models call"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {'id': 'model-1', 'object': 'model', 'owned_by': 'perplexity'},
                {'id': 'model-2', 'object': 'model', 'owned_by': 'perplexity'}
            ]
        }
        mock_response.text = '{"data": []}'
        mock_response.headers = {}
        mock_request.return_value = mock_response
        
        models = self.client.get_models()
        
        self.assertEqual(len(models), 2)
        self.assertEqual(models[0]['id'], 'model-1')
        mock_request.assert_called_once()
    
    @patch('tap_perplexity.client.requests.Session.request')
    def test_get_models_empty(self, mock_request):
        """Test get_models with empty response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_response.text = '{"data": []}'
        mock_response.headers = {}
        mock_request.return_value = mock_response
        
        models = self.client.get_models()
        
        self.assertEqual(len(models), 0)
    
    @patch('tap_perplexity.client.requests.Session.request')
    def test_rate_limit_retry(self, mock_request):
        """Test retry on rate limit"""
        # First call returns 429, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 429
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError()
        
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {'data': []}
        mock_response_success.text = '{"data": []}'
        mock_response_success.headers = {}
        
        mock_request.side_effect = [
            requests.exceptions.HTTPError(response=mock_response_fail),
            mock_response_success
        ]
        
        # Should retry and succeed
        models = self.client.get_models()
        self.assertEqual(len(models), 0)
        self.assertEqual(mock_request.call_count, 2)
    
    def test_is_retryable_error(self):
        """Test retryable error detection"""
        # Connection error should be retryable
        conn_error = requests.exceptions.ConnectionError()
        self.assertTrue(self.client._is_retryable_error(conn_error))
        
        # Timeout should be retryable
        timeout_error = requests.exceptions.Timeout()
        self.assertTrue(self.client._is_retryable_error(timeout_error))
        
        # 429 should be retryable
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        http_error_429 = requests.exceptions.HTTPError(response=mock_response_429)
        self.assertTrue(self.client._is_retryable_error(http_error_429))
        
        # 500 should be retryable
        mock_response_500 = Mock()
        mock_response_500.status_code = 500
        http_error_500 = requests.exceptions.HTTPError(response=mock_response_500)
        self.assertTrue(self.client._is_retryable_error(http_error_500))
        
        # 404 should not be retryable
        mock_response_404 = Mock()
        mock_response_404.status_code = 404
        http_error_404 = requests.exceptions.HTTPError(response=mock_response_404)
        self.assertFalse(self.client._is_retryable_error(http_error_404))


if __name__ == '__main__':
    unittest.main()
