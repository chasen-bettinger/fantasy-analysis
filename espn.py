"""
ESPN API client for Fantasy Football data retrieval.

Provides a robust client for fetching draft history and player data from ESPN's
fantasy football API with proper error handling, rate limiting, and configuration management.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List, Union

import requests
from requests import Response, RequestException

from config import config


class ESPNClient:
    """
    ESPN Fantasy Football API client with proper error handling and rate limiting.
    
    This client provides methods to fetch draft history and player data from ESPN's
    fantasy football API while handling authentication, rate limiting, and error conditions.
    """
    
    def __init__(self, config_instance: Optional[Any] = None) -> None:
        """
        Initialize the ESPN client.
        
        Args:
            config_instance: Configuration instance to use. If None, uses default config.
        """
        self.config = config_instance or config
        self.logger = logging.getLogger(__name__)
        self._last_request_time = 0.0
        
        # Validate that we have the necessary configuration
        if not hasattr(self.config, 'get_espn_headers'):
            raise ValueError("Config instance must provide get_espn_headers() method")
        if not hasattr(self.config, 'API_RATE_LIMIT_DELAY'):
            raise ValueError("Config instance must provide API_RATE_LIMIT_DELAY")
        if not hasattr(self.config, 'API_TIMEOUT'):
            raise ValueError("Config instance must provide API_TIMEOUT")
    
    def _make_request(self, url: str, headers: Optional[Dict[str, str]] = None, 
                      params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
        """
        Make an HTTP request to ESPN API with error handling and rate limiting.
        
        Args:
            url: The URL to request
            headers: Optional headers to include
            params: Optional query parameters
            max_retries: Maximum number of retry attempts
            
        Returns:
            JSON response as a dictionary
            
        Raises:
            RequestException: If request fails after retries
            ValueError: If response is not valid JSON
        """
        # Apply rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        if time_since_last_request < self.config.API_RATE_LIMIT_DELAY:
            sleep_time = self.config.API_RATE_LIMIT_DELAY - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        # Use config headers if none provided
        if headers is None:
            headers = self.config.get_espn_headers()
        
        retry_count = 0
        last_exception = None
        
        while retry_count <= max_retries:
            try:
                self.logger.debug(f"Making request to {url} (attempt {retry_count + 1})")
                
                response = requests.get(
                    url, 
                    headers=headers, 
                    params=params,
                    timeout=self.config.API_TIMEOUT
                )
                
                # Update last request time
                self._last_request_time = time.time()
                
                # Check for HTTP errors
                response.raise_for_status()
                
                # Try to parse JSON
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse JSON response: {e}")
                    raise ValueError(f"Invalid JSON response from ESPN API: {e}")
                
            except requests.exceptions.Timeout as e:
                self.logger.warning(f"Request timeout (attempt {retry_count + 1}): {e}")
                last_exception = e
                retry_count += 1
                
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error (attempt {retry_count + 1}): {e}")
                last_exception = e
                retry_count += 1
                
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP error {e.response.status_code}: {e}")
                
                # Don't retry on client errors (4xx)
                if 400 <= e.response.status_code < 500:
                    raise RequestException(f"Client error {e.response.status_code}: {e}")
                    
                # Retry on server errors (5xx)
                last_exception = e
                retry_count += 1
                
            except RequestException as e:
                self.logger.error(f"Request failed (attempt {retry_count + 1}): {e}")
                last_exception = e
                retry_count += 1
            
            # Wait before retry (exponential backoff)
            if retry_count <= max_retries:
                wait_time = min(2 ** retry_count, 30)  # Cap at 30 seconds
                self.logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        # All retries exhausted
        raise RequestException(f"Request failed after {max_retries + 1} attempts: {last_exception}")
    
    def get_draft_history(self) -> Dict[str, Any]:
        """
        Fetch draft history data from ESPN API.
        
        Returns:
            Draft history data as a dictionary
            
        Raises:
            RequestException: If request fails
            ValueError: If response is invalid
        """
        try:
            url = self.config.get_draft_history_url()
            self.logger.info(f"Fetching draft history from {url}")
            
            # Update headers for draft history request
            headers = self.config.get_espn_headers()
            headers["X-Fantasy-Filter"] = '{"players":{}}'
            
            return self._make_request(url, headers)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch draft history: {e}")
            raise
    
    def get_players(self) -> Dict[str, Any]:
        """
        Fetch player data from ESPN API.
        
        Returns:
            Player data as a dictionary
            
        Raises:
            RequestException: If request fails
            ValueError: If response is invalid
        """
        try:
            url = self.config.get_players_url()
            self.logger.info(f"Fetching players data from {url}")
            
            # Update headers for players request
            headers = self.config.get_espn_headers()
            headers["X-Fantasy-Filter"] = '{"filterActive":null}'
            
            return self._make_request(url, headers)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch players data: {e}")
            raise


# Create a default client instance
_default_client = ESPNClient()


# Backward compatibility functions
def get_draft_history() -> Dict[str, Any]:
    """
    Fetch draft history data from ESPN API.
    
    This function maintains backward compatibility with the original API.
    
    Returns:
        Draft history data as a dictionary
    """
    return _default_client.get_draft_history()


def get_players() -> Dict[str, Any]:
    """
    Fetch player data from ESPN API.
    
    This function maintains backward compatibility with the original API.
    
    Returns:
        Player data as a dictionary
    """
    return _default_client.get_players()
