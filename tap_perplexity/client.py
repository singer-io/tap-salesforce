"""
Perplexity API Client
"""

import time
import requests
import singer
import backoff

LOGGER = singer.get_logger()


class PerplexityClient:
    """Client for interacting with Perplexity AI API"""
    
    BASE_URL = "https://api.perplexity.ai"
    
    def __init__(self, api_key, user_agent='tap-perplexity'):
        self.api_key = api_key
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': user_agent
        })
    
    def _is_retryable_error(self, exception):
        """Check if an exception is retryable"""
        if isinstance(exception, requests.exceptions.ConnectionError):
            return True
        if isinstance(exception, requests.exceptions.Timeout):
            return True
        if isinstance(exception, requests.exceptions.RequestException):
            if hasattr(exception, 'response') and exception.response is not None:
                status_code = exception.response.status_code
                # Retry on rate limits and server errors
                return status_code in [429, 500, 502, 503, 504]
        return False
    
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: not PerplexityClient._is_retryable_error(None, e),
        factor=2
    )
    def _make_request(self, method, endpoint, **kwargs):
        """Make an HTTP request with retry logic"""
        url = f"{self.BASE_URL}/{endpoint}"
        
        LOGGER.info(f"Making {method} request to {url}")
        
        response = self.session.request(method, url, **kwargs)
        
        # Log rate limit info if available
        if 'X-RateLimit-Remaining' in response.headers:
            LOGGER.info(f"Rate limit remaining: {response.headers['X-RateLimit-Remaining']}")
        
        response.raise_for_status()
        
        return response.json() if response.text else {}
    
    def get(self, endpoint, params=None):
        """Make a GET request"""
        return self._make_request('GET', endpoint, params=params)
    
    def post(self, endpoint, json_data=None):
        """Make a POST request"""
        return self._make_request('POST', endpoint, json=json_data)
    
    def get_models(self):
        """Get list of available models"""
        try:
            response = self.get('models')
            # Perplexity API returns {"data": [...]}
            return response.get('data', [])
        except Exception as e:
            LOGGER.error(f"Error getting models: {e}")
            raise
    
    def create_chat_completion(self, model, messages, **kwargs):
        """Create a chat completion"""
        payload = {
            'model': model,
            'messages': messages,
            **kwargs
        }
        return self.post('chat/completions', json_data=payload)
