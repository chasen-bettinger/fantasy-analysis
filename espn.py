"""
ESPN API client for Fantasy Football data retrieval.

Provides a robust client for fetching draft history and player data from ESPN's
fantasy football API with proper error handling, rate limiting, and configuration management.
"""

import json
import logging
import time
import hashlib
import pickle
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Callable

import requests
from requests import Response, RequestException

from config import config

# Configure logging
logger = logging.getLogger(__name__)


def espn_cache(config_instance=None):
    """
    Decorator that provides persistent caching for ESPN API methods.

    Cache keys are generated based on the method name and all parameters.
    Cached data persists across sessions unless manually cleared.

    Args:
        config_instance: Config instance to use for cache settings
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Use provided config or default
            cfg = config_instance or config

            # Skip caching if disabled
            if not cfg.ENABLE_ESPN_CACHE:
                return func(*args, **kwargs)

            # Generate cache key from function name and arguments
            # Skip 'self' parameter for instance methods
            cache_args = args[1:] if args and hasattr(args[0], func.__name__) else args
            cache_key = _generate_cache_key(func.__name__, cache_args, kwargs)

            # Try to load from cache
            cached_data = _load_from_cache(cache_key, cfg)
            if cached_data is not None:
                logger = logging.getLogger(__name__)
                logger.debug(f"Cache hit for {func.__name__} with key {cache_key}")
                return cached_data

            # Cache miss - call the original function
            logger = logging.getLogger(__name__)
            logger.debug(f"Cache miss for {func.__name__} with key {cache_key}")
            result = func(*args, **kwargs)

            # Save to cache
            _save_to_cache(cache_key, result, cfg)

            return result

        return wrapper

    return decorator


def _generate_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """
    Generate a unique cache key based on function name and parameters.

    Args:
        func_name: Name of the function being cached
        args: Positional arguments
        kwargs: Keyword arguments

    Returns:
        MD5 hash string to use as cache key
    """
    # Create a deterministic string from the function name and parameters
    cache_data = {
        "function": func_name,
        "args": args,
        "kwargs": sorted(kwargs.items()),  # Sort for consistency
    }

    # Convert to JSON string and hash
    cache_string = json.dumps(cache_data, sort_keys=True, default=str)
    cache_key = hashlib.md5(cache_string.encode()).hexdigest()

    # Update human-readable cache map
    _update_cache_map(cache_key, func_name, args, kwargs)

    return cache_key


def _update_cache_map(
    cache_key: str, func_name: str, args: tuple, kwargs: dict
) -> None:
    """
    Update the human-readable cache map file.

    Args:
        cache_key: The generated cache key (filename)
        func_name: Name of the function being cached
        args: Positional arguments
        kwargs: Keyword arguments
    """
    cache_map_file = Path(config.ESPN_CACHE_DIR) / "cache_map.json"

    # Load existing cache map or create new one
    cache_map = {}
    if cache_map_file.exists():
        try:
            with open(cache_map_file, "r") as f:
                cache_map = json.load(f)
        except (json.JSONDecodeError, IOError):
            cache_map = {}

    # Create human-readable description
    description = f"{func_name}("
    if args:
        description += f"args={args}"
    if kwargs:
        if args:
            description += ", "
        description += f"kwargs={dict(kwargs)}"
    description += ")"

    # Add entry to cache map
    cache_map[cache_key] = {
        "function": func_name,
        "description": description,
        "args": list(args),
        "kwargs": dict(kwargs),
        "cache_file": f"{cache_key}.json",
        "created_at": datetime.now().isoformat(),
    }

    # Save updated cache map
    try:
        with open(cache_map_file, "w") as f:
            json.dump(cache_map, f, indent=2, sort_keys=True)
    except IOError as e:
        # Don't fail if we can't update the cache map
        logger.debug(f"Could not update cache map: {e}")


def _get_cache_file_path(cache_key: str, cfg) -> Path:
    """
    Get the file path for a cache key.

    Args:
        cache_key: The cache key
        cfg: Configuration instance

    Returns:
        Path object for the cache file
    """
    cache_dir = Path(cfg.ESPN_CACHE_DIR)
    extension = "json" if cfg.ESPN_CACHE_FORMAT == "json" else "pkl"
    return cache_dir / f"{cache_key}.{extension}"


def _load_from_cache(cache_key: str, cfg) -> Optional[Dict[str, Any]]:
    """
    Load data from cache file.

    Args:
        cache_key: The cache key
        cfg: Configuration instance

    Returns:
        Cached data or None if not found/invalid
    """
    try:
        cache_file = _get_cache_file_path(cache_key, cfg)

        if not cache_file.exists():
            return None

        if cfg.ESPN_CACHE_FORMAT == "json":
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        else:  # pickle format
            with open(cache_file, "rb") as f:
                return pickle.load(f)

    except (json.JSONDecodeError, pickle.PickleError, OSError) as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to load cache file {cache_key}: {e}")
        return None


def _save_to_cache(cache_key: str, data: Dict[str, Any], cfg) -> None:
    """
    Save data to cache file.

    Args:
        cache_key: The cache key
        data: Data to cache
        cfg: Configuration instance
    """
    try:
        cache_file = _get_cache_file_path(cache_key, cfg)

        # Ensure cache directory exists
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        if cfg.ESPN_CACHE_FORMAT == "json":
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        else:  # pickle format
            with open(cache_file, "wb") as f:
                pickle.dump(data, f)

    except (OSError, pickle.PickleError) as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to save cache file {cache_key}: {e}")


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
        if not hasattr(self.config, "get_espn_headers"):
            raise ValueError("Config instance must provide get_espn_headers() method")
        if not hasattr(self.config, "API_RATE_LIMIT_DELAY"):
            raise ValueError("Config instance must provide API_RATE_LIMIT_DELAY")
        if not hasattr(self.config, "API_TIMEOUT"):
            raise ValueError("Config instance must provide API_TIMEOUT")

    def _make_request(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
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
                self.logger.debug(
                    f"Making request to {url} (attempt {retry_count + 1})"
                )

                response = requests.get(
                    url, headers=headers, params=params, timeout=self.config.API_TIMEOUT
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
                self.logger.warning(
                    f"Connection error (attempt {retry_count + 1}): {e}"
                )
                last_exception = e
                retry_count += 1

            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP error {e.response.status_code}: {e}")

                # Don't retry on client errors (4xx)
                if 400 <= e.response.status_code < 500:
                    raise RequestException(
                        f"Client error {e.response.status_code}: {e}"
                    )

                # Retry on server errors (5xx)
                last_exception = e
                retry_count += 1

            except RequestException as e:
                self.logger.error(f"Request failed (attempt {retry_count + 1}): {e}")
                last_exception = e
                retry_count += 1

            # Wait before retry (exponential backoff)
            if retry_count <= max_retries:
                wait_time = min(2**retry_count, 30)  # Cap at 30 seconds
                self.logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        # All retries exhausted
        raise RequestException(
            f"Request failed after {max_retries + 1} attempts: {last_exception}"
        )

    @espn_cache()
    def get_draft_history(self, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch draft history data from ESPN API.

        Args:
            season: Season year to fetch (None for default)

        Returns:
            Draft history data as a dictionary

        Raises:
            RequestException: If request fails
            ValueError: If response is invalid
        """
        try:
            url = self.config.get_draft_history_url(season)
            self.logger.info(
                f"Fetching draft history from {url} for season {season or 'default'}"
            )

            # Update headers for draft history request
            headers = self.config.get_espn_headers()
            headers["X-Fantasy-Filter"] = '{"players":{}}'

            return self._make_request(url, headers)

        except Exception as e:
            self.logger.error(f"Failed to fetch draft history: {e}")
            raise

    @espn_cache()
    def get_players(self, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch player data from ESPN API.

        Args:
            season: Season year to fetch (None for default)

        Returns:
            Player data as a dictionary

        Raises:
            RequestException: If request fails
            ValueError: If response is invalid
        """
        try:
            url = self.config.get_players_url(season)
            self.logger.info(
                f"Fetching players data from {url} for season {season or 'default'}"
            )

            # Update headers for players request
            headers = self.config.get_espn_headers()
            headers["X-Fantasy-Filter"] = '{"filterActive":null}'

            return self._make_request(url, headers)

        except Exception as e:
            self.logger.error(f"Failed to fetch players data: {e}")
            raise

    @espn_cache()
    def get_rosters(self, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch roster data from ESPN API.

        Args:
            season: Season year to fetch (None for default)

        Returns:
            Roster data as a dictionary

        Raises:
            RequestException: If request fails
            ValueError: If response is invalid
        """
        try:
            url = self.config.get_roster_url(season)
            self.logger.info(
                f"Fetching roster data from {url} for season {season or 'default'}"
            )

            # Update headers for roster request
            headers = self.config.get_espn_headers()
            # headers["X-Fantasy-Filter"] = '{"teams":{"filterStatsForTopScoringPeriodIds":{"value":2,"additionalValue":["002015","102015","002014","1002014"]}}}'

            return self._make_request(url, headers)

        except Exception as e:
            self.logger.error(f"Failed to fetch roster data: {e}")
            raise

    def clear_cache(self, method_name: Optional[str] = None) -> None:
        """
        Clear cached data.

        Args:
            method_name: Optional method name to clear cache for specific method.
                        If None, clears all ESPN cache.
        """
        if not self.config.ENABLE_ESPN_CACHE:
            self.logger.info("Cache is disabled, nothing to clear")
            return

        cache_dir = Path(self.config.ESPN_CACHE_DIR)
        if not cache_dir.exists():
            self.logger.info("Cache directory doesn't exist, nothing to clear")
            return

        if method_name:
            # Clear cache for specific method - since we use hashed keys, we clear all files
            # This is conservative but ensures we don't miss any cached data for the method
            self.logger.warning(
                f"Clearing all cache files since method-specific clearing "
                f"is not supported with hashed keys"
            )
            removed_files = 0
            for cache_file in cache_dir.glob("*"):
                if cache_file.is_file() and (cache_file.suffix in [".json", ".pkl"]):
                    try:
                        cache_file.unlink()
                        removed_files += 1
                    except OSError as e:
                        self.logger.warning(
                            f"Failed to remove cache file {cache_file}: {e}"
                        )
            self.logger.info(f"Cleared {removed_files} cache files")
        else:
            # Clear all cache files
            removed_files = 0
            for cache_file in cache_dir.rglob("*"):
                if cache_file.is_file():
                    try:
                        cache_file.unlink()
                        removed_files += 1
                    except OSError as e:
                        self.logger.warning(
                            f"Failed to remove cache file {cache_file}: {e}"
                        )

            # Remove empty directories
            try:
                cache_dir.rmdir()
            except OSError:
                pass  # Directory not empty or doesn't exist

            self.logger.info(f"Cleared {removed_files} cache files")

    def get_cache_info(self) -> Dict[str, Any]:
        """
        Get information about the current cache.

        Returns:
            Dictionary with cache information
        """
        if not self.config.ENABLE_ESPN_CACHE:
            return {
                "enabled": False,
                "cache_dir": self.config.ESPN_CACHE_DIR,
                "cache_format": self.config.ESPN_CACHE_FORMAT,
            }

        cache_dir = Path(self.config.ESPN_CACHE_DIR)
        cache_files = []
        total_size = 0

        if cache_dir.exists():
            for cache_file in cache_dir.rglob("*"):
                if cache_file.is_file():
                    file_size = cache_file.stat().st_size
                    total_size += file_size
                    cache_files.append(
                        {
                            "name": cache_file.name,
                            "size": file_size,
                            "modified": cache_file.stat().st_mtime,
                        }
                    )

        return {
            "enabled": True,
            "cache_dir": str(cache_dir),
            "cache_format": self.config.ESPN_CACHE_FORMAT,
            "total_files": len(cache_files),
            "total_size_bytes": total_size,
            "files": cache_files,
        }


# Create a default client instance
_default_client = ESPNClient()


# Backward compatibility functions
def get_draft_history(season: Optional[int] = None) -> Dict[str, Any]:
    """
    Fetch draft history data from ESPN API.

    This function maintains backward compatibility with the original API.

    Args:
        season: Season year to fetch (None for default)

    Returns:
        Draft history data as a dictionary
    """
    return _default_client.get_draft_history(season)


def get_players(season: Optional[int] = None) -> Dict[str, Any]:
    """
    Fetch player data from ESPN API.

    This function maintains backward compatibility with the original API.

    Args:
        season: Season year to fetch (None for default)

    Returns:
        Player data as a dictionary
    """
    return _default_client.get_players(season)


def get_rosters(season: Optional[int] = None) -> Dict[str, Any]:
    """
    Fetch roster data from ESPN API.

    This function maintains backward compatibility with the original API.

    Args:
        season: Season year to fetch (None for default)

    Returns:
        Roster data as a dictionary
    """
    return _default_client.get_rosters(season)
