"""
Mock Perplexity API Client for Testing and Demo

This module provides a mock client that works without a real API key.
Perfect for testing, development, and demonstrations.
"""

import time
from datetime import datetime


class MockPerplexityClient:
    """
    Mock client for Perplexity AI API
    
    This client simulates API responses without making real API calls.
    Use for testing, development, or when you don't have an API key yet.
    """
    
    MOCK_MODELS = [
        {
            'id': 'pplx-7b-online',
            'object': 'model',
            'created': 1234567890,
            'owned_by': 'perplexity'
        },
        {
            'id': 'pplx-70b-online',
            'object': 'model',
            'created': 1234567900,
            'owned_by': 'perplexity'
        },
        {
            'id': 'pplx-7b-chat',
            'object': 'model',
            'created': 1234567910,
            'owned_by': 'perplexity'
        },
        {
            'id': 'pplx-70b-chat',
            'object': 'model',
            'created': 1234567920,
            'owned_by': 'perplexity'
        }
    ]
    
    def __init__(self, api_key=None, user_agent='tap-perplexity-mock'):
        """
        Initialize mock client
        
        Args:
            api_key: Ignored in mock mode
            user_agent: User agent string
        """
        self.api_key = api_key or "mock-api-key"
        self.user_agent = user_agent
        self.mock_mode = True
        
    def get_models(self):
        """
        Get list of available models (mock data)
        
        Returns:
            list: Mock model data
        """
        # Simulate API delay
        time.sleep(0.1)
        return self.MOCK_MODELS.copy()
    
    def create_chat_completion(self, model, messages, **kwargs):
        """
        Create a mock chat completion
        
        Args:
            model: Model to use
            messages: List of messages
            **kwargs: Additional parameters
            
        Returns:
            dict: Mock completion response
        """
        # Simulate API delay
        time.sleep(0.2)
        
        return {
            'id': f'mock-completion-{int(time.time())}',
            'object': 'chat.completion',
            'created': int(time.time()),
            'model': model,
            'choices': [
                {
                    'index': 0,
                    'message': {
                        'role': 'assistant',
                        'content': 'This is a mock response for testing purposes.'
                    },
                    'finish_reason': 'stop'
                }
            ],
            'usage': {
                'prompt_tokens': 10,
                'completion_tokens': 20,
                'total_tokens': 30
            }
        }


def get_client(api_key=None, user_agent='tap-perplexity', mock_mode=False):
    """
    Get appropriate client based on mode
    
    Args:
        api_key: API key (or None for mock mode)
        user_agent: User agent string
        mock_mode: If True, use mock client
        
    Returns:
        Client instance (real or mock)
    """
    # Auto-detect mock mode if API key is obviously fake
    if api_key and (
        api_key.startswith('mock-') or 
        api_key == 'pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' or
        api_key == 'test-api-key'
    ):
        mock_mode = True
    
    if mock_mode or not api_key or api_key == 'mock':
        return MockPerplexityClient(api_key=api_key, user_agent=user_agent)
    
    # Import real client only when needed
    from tap_perplexity.client import PerplexityClient
    return PerplexityClient(api_key=api_key, user_agent=user_agent)
