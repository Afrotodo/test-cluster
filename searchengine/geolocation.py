# """
# geolocation.py

# Centralized geolocation and device detection for search analytics.

# FEATURES:
# - IP-based geolocation using AbstractAPI
# - User-Agent parsing for browser/device/OS detection
# - Multi-layer caching (in-memory LRU + Redis)
# - Graceful fallback on API failures

# USAGE:
#     from searchengine.geolocation import (
#         get_location_from_ip,
#         get_device_info,
#         get_full_client_info
#     )
    
#     # Get location from IP
#     location = get_location_from_ip('8.8.8.8')
#     # Returns: {'city': 'Mountain View', 'country': 'US', ...}
    
#     # Get device info from User-Agent
#     device = get_device_info(request.META.get('HTTP_USER_AGENT'))
#     # Returns: {'browser': 'Chrome', 'device_type': 'Desktop', ...}
    
#     # Get everything at once
#     client_info = get_full_client_info(request)
#     # Returns: {'location': {...}, 'device': {...}, 'ip': '...'}
# """

# import json
# import logging
# import requests
# from functools import lru_cache
# from typing import Dict, Optional, Any

# from decouple import config

# # Optional: user-agents library for parsing
# try:
#     from user_agents import parse as parse_user_agent
#     USER_AGENTS_AVAILABLE = True
# except ImportError:
#     USER_AGENTS_AVAILABLE = False

# # Optional: Redis for persistent caching
# try:
#     import redis
#     REDIS_AVAILABLE = True
# except ImportError:
#     REDIS_AVAILABLE = False


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# logger = logging.getLogger(__name__)

# # AbstractAPI Configuration
# ABSTRACTAPI_KEY = config('ABSTRACTAPI_GEOLOCATION_KEY', default='')
# ABSTRACTAPI_URL = 'https://ipgeolocation.abstractapi.com/v1/'
# API_TIMEOUT = 3  # seconds - don't slow down searches

# # Redis Configuration (for persistent cache)
# REDIS_CACHE_URL = config('REDIS_ANALYTICS_URL', default='redis://localhost:6379/2')
# LOCATION_CACHE_TTL = 86400  # 24 hours

# # Private/local IPs to skip
# PRIVATE_IP_PREFIXES = ('127.', '10.', '192.168.', '172.16.', '172.17.', '172.18.',
#                        '172.19.', '172.20.', '172.21.', '172.22.', '172.23.',
#                        '172.24.', '172.25.', '172.26.', '172.27.', '172.28.',
#                        '172.29.', '172.30.', '172.31.', '169.254.', '::1', 'fe80:')


# # =============================================================================
# # REDIS CACHE CONNECTION
# # =============================================================================

# _redis_cache = None


# def get_redis_cache():
#     """Get Redis connection for geolocation caching."""
#     global _redis_cache
#     if _redis_cache is None and REDIS_AVAILABLE:
#         try:
#             _redis_cache = redis.from_url(
#                 REDIS_CACHE_URL,
#                 decode_responses=True,
#                 socket_connect_timeout=2,
#                 socket_timeout=2
#             )
#             _redis_cache.ping()
#         except Exception as e:
#             logger.warning(f"Redis cache unavailable for geolocation: {e}")
#             _redis_cache = None
#     return _redis_cache


# # =============================================================================
# # IP ADDRESS UTILITIES
# # =============================================================================

# def get_client_ip(request) -> str:
#     """
#     Extract client IP address from Django request.
    
#     Handles various proxy headers (CloudFlare, AWS ALB, nginx, etc.)
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         IP address string
#     """
#     # CloudFlare
#     cf_connecting_ip = request.META.get('HTTP_CF_CONNECTING_IP')
#     if cf_connecting_ip:
#         return cf_connecting_ip.strip()
    
#     # Standard proxy header
#     x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
#     if x_forwarded_for:
#         # Take first IP (original client)
#         return x_forwarded_for.split(',')[0].strip()
    
#     # AWS ALB / other proxies
#     x_real_ip = request.META.get('HTTP_X_REAL_IP')
#     if x_real_ip:
#         return x_real_ip.strip()
    
#     # Direct connection
#     return request.META.get('REMOTE_ADDR', '')


# def is_private_ip(ip_address: str) -> bool:
#     """Check if IP is private/local (not worth looking up)."""
#     if not ip_address:
#         return True
    
#     ip_lower = ip_address.lower()
    
#     if ip_lower in ('localhost', ''):
#         return True
    
#     for prefix in PRIVATE_IP_PREFIXES:
#         if ip_address.startswith(prefix):
#             return True
    
#     return False


# # =============================================================================
# # IP GEOLOCATION (AbstractAPI)
# # =============================================================================

# @lru_cache(maxsize=500)
# def _get_location_from_api(ip_address: str) -> Dict[str, Any]:
#     """
#     Call AbstractAPI to get location data.
    
#     Cached in memory with LRU cache.
    
#     Args:
#         ip_address: IP address to lookup
        
#     Returns:
#         Location dict or empty dict on failure
#     """
#     if not ABSTRACTAPI_KEY:
#         logger.warning("ABSTRACTAPI_GEOLOCATION_KEY not configured")
#         return {}
    
#     try:
#         response = requests.get(
#             ABSTRACTAPI_URL,
#             params={
#                 'api_key': ABSTRACTAPI_KEY,
#                 'ip_address': ip_address
#             },
#             timeout=API_TIMEOUT
#         )
        
#         if response.status_code == 200:
#             data = response.json()
            
#             # Extract timezone safely
#             timezone = None
#             if isinstance(data.get('timezone'), dict):
#                 timezone = data['timezone'].get('name')
#             elif isinstance(data.get('timezone'), str):
#                 timezone = data['timezone']
            
#             # Extract connection/ISP info safely
#             isp = None
#             connection = data.get('connection')
#             if isinstance(connection, dict):
#                 isp = connection.get('isp_name') or connection.get('organization_name')
            
#             return {
#                 'ip': data.get('ip_address'),
#                 'city': data.get('city') or '',
#                 'region': data.get('region') or '',
#                 'region_code': data.get('region_iso_code') or '',
#                 'country': data.get('country') or '',
#                 'country_code': data.get('country_code') or '',
#                 'continent': data.get('continent') or '',
#                 'lat': data.get('latitude'),
#                 'lng': data.get('longitude'),
#                 'postal': data.get('postal_code') or '',
#                 'timezone': timezone or '',
#                 'isp': isp or '',
#                 'is_vpn': data.get('security', {}).get('is_vpn', False) if isinstance(data.get('security'), dict) else False
#             }
        
#         elif response.status_code == 429:
#             logger.warning("AbstractAPI rate limit exceeded")
#             return {}
        
#         else:
#             logger.warning(f"AbstractAPI returned status {response.status_code}")
#             return {}
            
#     except requests.Timeout:
#         logger.warning(f"AbstractAPI timeout for IP {ip_address}")
#         return {}
#     except requests.RequestException as e:
#         logger.warning(f"AbstractAPI request error: {e}")
#         return {}
#     except Exception as e:
#         logger.error(f"AbstractAPI unexpected error: {e}")
#         return {}


# def get_location_from_ip(ip_address: str, use_cache: bool = True) -> Dict[str, Any]:
#     """
#     Get location data from IP address with multi-layer caching.
    
#     Cache layers:
#     1. In-memory LRU cache (fastest)
#     2. Redis cache (persists across restarts)
#     3. AbstractAPI call (slowest, costs money)
    
#     Args:
#         ip_address: IP address to lookup
#         use_cache: Whether to use Redis cache (default True)
        
#     Returns:
#         Location dict with city, country, lat, lng, etc.
#         Empty dict if lookup fails or IP is private.
#     """
#     if not ip_address or is_private_ip(ip_address):
#         return {}
    
#     # Normalize IP
#     ip_address = ip_address.strip()
#     cache_key = f"geo:{ip_address}"
    
#     # Layer 1: Check Redis cache
#     if use_cache:
#         redis_client = get_redis_cache()
#         if redis_client:
#             try:
#                 cached = redis_client.get(cache_key)
#                 if cached:
#                     return json.loads(cached)
#             except Exception as e:
#                 logger.debug(f"Redis cache read error: {e}")
    
#     # Layer 2: Call API (has internal LRU cache)
#     location = _get_location_from_api(ip_address)
    
#     # Layer 3: Store in Redis cache
#     if location and use_cache:
#         redis_client = get_redis_cache()
#         if redis_client:
#             try:
#                 redis_client.setex(
#                     cache_key,
#                     LOCATION_CACHE_TTL,
#                     json.dumps(location)
#                 )
#             except Exception as e:
#                 logger.debug(f"Redis cache write error: {e}")
    
#     return location


# def clear_location_cache(ip_address: str = None):
#     """
#     Clear location cache.
    
#     Args:
#         ip_address: Specific IP to clear, or None to clear all
#     """
#     # Clear LRU cache
#     if ip_address:
#         # Can't selectively clear LRU, so just clear all
#         _get_location_from_api.cache_clear()
#     else:
#         _get_location_from_api.cache_clear()
    
#     # Clear Redis cache
#     redis_client = get_redis_cache()
#     if redis_client:
#         try:
#             if ip_address:
#                 redis_client.delete(f"geo:{ip_address}")
#             else:
#                 # Clear all geo keys (be careful with this)
#                 for key in redis_client.scan_iter("geo:*"):
#                     redis_client.delete(key)
#         except Exception as e:
#             logger.warning(f"Failed to clear Redis geo cache: {e}")


# # =============================================================================
# # USER-AGENT / DEVICE DETECTION
# # =============================================================================

# def get_device_info(user_agent_string: str) -> Dict[str, Any]:
#     """
#     Parse User-Agent string to extract browser, device, and OS info.
    
#     Args:
#         user_agent_string: HTTP User-Agent header value
        
#     Returns:
#         Dict with browser, browser_version, device_type, os, os_version
#     """
#     if not user_agent_string:
#         return {
#             'browser': 'Unknown',
#             'browser_version': '',
#             'device_type': 'Unknown',
#             'os': 'Unknown',
#             'os_version': '',
#             'is_bot': False
#         }
    
#     if USER_AGENTS_AVAILABLE:
#         # Use user-agents library (more accurate)
#         try:
#             ua = parse_user_agent(user_agent_string)
            
#             # Determine device type
#             if ua.is_mobile:
#                 device_type = 'Mobile'
#             elif ua.is_tablet:
#                 device_type = 'Tablet'
#             elif ua.is_pc:
#                 device_type = 'Desktop'
#             elif ua.is_bot:
#                 device_type = 'Bot'
#             else:
#                 device_type = 'Other'
            
#             return {
#                 'browser': ua.browser.family or 'Unknown',
#                 'browser_version': ua.browser.version_string or '',
#                 'device_type': device_type,
#                 'device_brand': ua.device.brand or '',
#                 'device_model': ua.device.model or '',
#                 'os': ua.os.family or 'Unknown',
#                 'os_version': ua.os.version_string or '',
#                 'is_bot': ua.is_bot,
#                 'is_mobile': ua.is_mobile,
#                 'is_tablet': ua.is_tablet,
#                 'is_pc': ua.is_pc
#             }
#         except Exception as e:
#             logger.warning(f"User-agent parsing error: {e}")
    
#     # Fallback: Basic parsing without library
#     return _parse_user_agent_basic(user_agent_string)


# def _parse_user_agent_basic(user_agent_string: str) -> Dict[str, Any]:
#     """
#     Basic User-Agent parsing without external library.
    
#     Less accurate but works without dependencies.
#     """
#     ua = user_agent_string.lower()
    
#     # Detect browser
#     browser = 'Unknown'
#     browser_version = ''
    
#     if 'edg/' in ua:
#         browser = 'Edge'
#     elif 'chrome/' in ua and 'chromium/' not in ua:
#         browser = 'Chrome'
#     elif 'firefox/' in ua:
#         browser = 'Firefox'
#     elif 'safari/' in ua and 'chrome/' not in ua:
#         browser = 'Safari'
#     elif 'opera/' in ua or 'opr/' in ua:
#         browser = 'Opera'
#     elif 'msie' in ua or 'trident/' in ua:
#         browser = 'Internet Explorer'
    
#     # Detect device type
#     device_type = 'Desktop'
#     is_mobile = False
#     is_tablet = False
    
#     if 'mobile' in ua or 'android' in ua and 'tablet' not in ua:
#         if 'tablet' in ua or 'ipad' in ua:
#             device_type = 'Tablet'
#             is_tablet = True
#         else:
#             device_type = 'Mobile'
#             is_mobile = True
#     elif 'tablet' in ua or 'ipad' in ua:
#         device_type = 'Tablet'
#         is_tablet = True
    
#     # Detect OS
#     os_name = 'Unknown'
#     os_version = ''
    
#     if 'windows' in ua:
#         os_name = 'Windows'
#         if 'windows nt 10' in ua:
#             os_version = '10'
#         elif 'windows nt 6.3' in ua:
#             os_version = '8.1'
#         elif 'windows nt 6.1' in ua:
#             os_version = '7'
#     elif 'mac os x' in ua or 'macintosh' in ua:
#         os_name = 'macOS'
#     elif 'iphone' in ua or 'ipad' in ua:
#         os_name = 'iOS'
#     elif 'android' in ua:
#         os_name = 'Android'
#     elif 'linux' in ua:
#         os_name = 'Linux'
    
#     # Detect bot
#     is_bot = any(bot in ua for bot in ['bot', 'crawler', 'spider', 'scraper', 'curl', 'wget'])
#     if is_bot:
#         device_type = 'Bot'
    
#     return {
#         'browser': browser,
#         'browser_version': browser_version,
#         'device_type': device_type,
#         'device_brand': '',
#         'device_model': '',
#         'os': os_name,
#         'os_version': os_version,
#         'is_bot': is_bot,
#         'is_mobile': is_mobile,
#         'is_tablet': is_tablet,
#         'is_pc': device_type == 'Desktop'
#     }


# # =============================================================================
# # COMBINED CLIENT INFO
# # =============================================================================

# def get_full_client_info(request) -> Dict[str, Any]:
#     """
#     Get complete client information from a Django request.
    
#     Combines IP geolocation and device detection into one call.
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         Dict with 'ip', 'location', and 'device' sub-dicts
#     """
#     # Get IP address
#     ip_address = get_client_ip(request)
    
#     # Get User-Agent
#     user_agent = request.META.get('HTTP_USER_AGENT', '')
    
#     # Get location (with caching)
#     location = get_location_from_ip(ip_address)
    
#     # Get device info
#     device = get_device_info(user_agent)
    
#     return {
#         'ip': ip_address,
#         'location': location,
#         'device': device,
#         'user_agent': user_agent
#     }


# def get_location_from_request(request) -> Optional[Dict[str, Any]]:
#     """
#     Get location from Django request with multiple fallbacks.
    
#     Priority:
#     1. URL parameters (city, country, region)
#     2. Session cached location
#     3. CloudFlare headers (if using CF)
#     4. IP geolocation API
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         Location dict or None
#     """
#     location = {}
    
#     # 1. Check URL params
#     url_city = request.GET.get('city', '').strip()
#     url_country = request.GET.get('country', '').strip()
#     url_region = request.GET.get('region', '').strip()
    
#     if url_city:
#         location['city'] = url_city
#     if url_country:
#         location['country'] = url_country
#     if url_region:
#         location['region'] = url_region
    
#     # 2. Check session cache
#     if not location:
#         try:
#             session_location = request.session.get('user_location')
#             if session_location and isinstance(session_location, dict):
#                 location = session_location.copy()
#         except Exception:
#             pass
    
#     # 3. Check CloudFlare headers
#     if not location.get('country_code'):
#         cf_country = request.META.get('HTTP_CF_IPCOUNTRY')
#         if cf_country and cf_country != 'XX':
#             location['country_code'] = cf_country
    
#     # 4. IP Geolocation lookup
#     if not location.get('city'):
#         ip_address = get_client_ip(request)
#         if ip_address and not is_private_ip(ip_address):
#             ip_location = get_location_from_ip(ip_address)
#             if ip_location:
#                 # Merge (don't overwrite existing)
#                 for key, value in ip_location.items():
#                     if value and not location.get(key):
#                         location[key] = value
    
#     # 5. Cache in session
#     if location:
#         try:
#             request.session['user_location'] = location
#         except Exception:
#             pass
    
#     return location if location else None


# # =============================================================================
# # UTILITY FUNCTIONS
# # =============================================================================

# def get_cache_stats() -> Dict[str, Any]:
#     """Get cache statistics for monitoring."""
#     stats = {
#         'lru_cache_info': _get_location_from_api.cache_info()._asdict(),
#         'redis_available': False,
#         'redis_geo_keys': 0
#     }
    
#     redis_client = get_redis_cache()
#     if redis_client:
#         try:
#             stats['redis_available'] = True
#             # Count geo keys (don't do this in production with lots of keys)
#             stats['redis_geo_keys'] = len(list(redis_client.scan_iter("geo:*", count=100)))
#         except Exception:
#             pass
    
#     return stats



# """
# geolocation.py

# Centralized geolocation and device detection for search analytics.

# PRODUCTION FEATURES:
# - API key from environment only (no hardcoding)
# - Retry logic with exponential backoff for API failures
# - Circuit breaker for rate limits (429 errors)
# - Multi-layer caching (in-memory LRU + Redis)
# - Graceful fallback on API failures
# - Proper logging (no print statements)

# FEATURES:
# - IP-based geolocation using AbstractAPI
# - User-Agent parsing for browser/device/OS detection
# - Multi-layer caching (in-memory LRU + Redis)
# - Graceful fallback on API failures

# USAGE:
#     from searchengine.geolocation import (
#         get_location_from_ip,
#         get_device_info,
#         get_full_client_info
#     )
    
#     # Get location from IP
#     location = get_location_from_ip('8.8.8.8')
#     # Returns: {'city': 'Mountain View', 'country': 'US', ...}
    
#     # Get device info from User-Agent
#     device = get_device_info(request.META.get('HTTP_USER_AGENT'))
#     # Returns: {'browser': 'Chrome', 'device_type': 'Desktop', ...}
    
#     # Get everything at once
#     client_info = get_full_client_info(request)
#     # Returns: {'location': {...}, 'device': {...}, 'ip': '...'}
# """

# import json
# import logging
# import time
# import threading
# from functools import lru_cache, wraps
# from typing import Dict, Optional, Any, Callable, Tuple

# import requests
# from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError
# from decouple import config

# # Optional: user-agents library for parsing
# try:
#     from user_agents import parse as parse_user_agent
#     USER_AGENTS_AVAILABLE = True
# except ImportError:
#     USER_AGENTS_AVAILABLE = False

# # Optional: Redis for persistent caching
# try:
#     import redis
#     from redis.exceptions import RedisError
#     REDIS_AVAILABLE = True
# except ImportError:
#     REDIS_AVAILABLE = False
#     RedisError = Exception  # Fallback for type hints


# # =============================================================================
# # LOGGING CONFIGURATION
# # =============================================================================

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION - ALL FROM ENVIRONMENT VARIABLES
# # =============================================================================

# # AbstractAPI Configuration - API key MUST come from environment
# ABSTRACTAPI_KEY = config('ABSTRACTAPI_GEOLOCATION_KEY_V2', default='')
# ABSTRACTAPI_URL = config('ABSTRACTAPI_GEOLOCATION_URL', default='https://ip-intelligence.abstractapi.com/v1/')
# API_TIMEOUT = config('GEOLOCATION_API_TIMEOUT', default=3, cast=int)  # seconds

# # Redis Configuration (for persistent cache)
# REDIS_CACHE_URL = config('REDIS_ANALYTICS_URL', default='redis://localhost:6379/2')
# LOCATION_CACHE_TTL = config('LOCATION_CACHE_TTL', default=86400, cast=int)  # 24 hours

# # Retry Configuration
# API_MAX_RETRIES = config('GEOLOCATION_MAX_RETRIES', default=3, cast=int)
# API_RETRY_DELAY = config('GEOLOCATION_RETRY_DELAY', default=0.5, cast=float)  # Base delay in seconds
# API_RETRY_BACKOFF = config('GEOLOCATION_RETRY_BACKOFF', default=2.0, cast=float)  # Exponential backoff

# # Circuit Breaker Configuration for Rate Limits
# RATE_LIMIT_CIRCUIT_THRESHOLD = config('GEOLOCATION_RATE_LIMIT_THRESHOLD', default=3, cast=int)
# RATE_LIMIT_CIRCUIT_TIMEOUT = config('GEOLOCATION_RATE_LIMIT_TIMEOUT', default=60, cast=int)  # seconds

# # Private/local IPs to skip
# PRIVATE_IP_PREFIXES = ('127.', '10.', '192.168.', '172.16.', '172.17.', '172.18.',
#                        '172.19.', '172.20.', '172.21.', '172.22.', '172.23.',
#                        '172.24.', '172.25.', '172.26.', '172.27.', '172.28.',
#                        '172.29.', '172.30.', '172.31.', '169.254.', '::1', 'fe80:')


# # =============================================================================
# # CIRCUIT BREAKER FOR RATE LIMITS
# # =============================================================================

# class RateLimitCircuitBreaker:
#     """
#     Circuit breaker specifically for API rate limiting (429 errors).
    
#     When rate limited, stops making API calls until the timeout expires.
#     This prevents burning through rate limits and gives the API time to recover.
#     """
    
#     def __init__(
#         self,
#         threshold: int = RATE_LIMIT_CIRCUIT_THRESHOLD,
#         timeout: int = RATE_LIMIT_CIRCUIT_TIMEOUT
#     ):
#         """
#         Initialize circuit breaker.
        
#         Args:
#             threshold: Number of 429s before opening circuit
#             timeout: Seconds to wait before allowing requests again
#         """
#         self.threshold = threshold
#         self.timeout = timeout
        
#         self._rate_limit_count = 0
#         self._circuit_open = False
#         self._circuit_opened_at: Optional[float] = None
#         self._lock = threading.Lock()
    
#     @property
#     def is_open(self) -> bool:
#         """Check if circuit is open (blocking requests)."""
#         with self._lock:
#             if not self._circuit_open:
#                 return False
            
#             # Check if timeout has expired
#             if self._circuit_opened_at and \
#                time.time() - self._circuit_opened_at >= self.timeout:
#                 self._circuit_open = False
#                 self._rate_limit_count = 0
#                 self._circuit_opened_at = None
#                 logger.info("Geolocation rate limit circuit breaker closed (timeout expired)")
#                 return False
            
#             return True
    
#     @property
#     def allows_request(self) -> bool:
#         """Check if a request should be allowed."""
#         return not self.is_open
    
#     def record_rate_limit(self) -> None:
#         """Record a 429 rate limit response."""
#         with self._lock:
#             self._rate_limit_count += 1
            
#             if self._rate_limit_count >= self.threshold:
#                 self._circuit_open = True
#                 self._circuit_opened_at = time.time()
#                 logger.warning(
#                     f"Geolocation rate limit circuit breaker OPEN after {self._rate_limit_count} "
#                     f"rate limits. Will retry in {self.timeout} seconds."
#                 )
    
#     def record_success(self) -> None:
#         """Record a successful request (resets rate limit count)."""
#         with self._lock:
#             if self._rate_limit_count > 0:
#                 self._rate_limit_count = 0
    
#     def reset(self) -> None:
#         """Manually reset the circuit breaker."""
#         with self._lock:
#             self._circuit_open = False
#             self._rate_limit_count = 0
#             self._circuit_opened_at = None
#             logger.info("Geolocation rate limit circuit breaker manually reset")
    
#     def get_status(self) -> Dict[str, Any]:
#         """Get circuit breaker status for monitoring."""
#         with self._lock:
#             remaining_timeout = 0
#             if self._circuit_open and self._circuit_opened_at:
#                 remaining_timeout = max(0, self.timeout - (time.time() - self._circuit_opened_at))
            
#             return {
#                 'is_open': self._circuit_open,
#                 'rate_limit_count': self._rate_limit_count,
#                 'threshold': self.threshold,
#                 'timeout': self.timeout,
#                 'remaining_timeout': round(remaining_timeout, 1),
#                 'opened_at': self._circuit_opened_at
#             }


# # Global circuit breaker instance
# _rate_limit_circuit = RateLimitCircuitBreaker()


# def get_rate_limit_status() -> Dict[str, Any]:
#     """Get the current rate limit circuit breaker status."""
#     return _rate_limit_circuit.get_status()


# def reset_rate_limit_circuit() -> None:
#     """Manually reset the rate limit circuit breaker."""
#     _rate_limit_circuit.reset()


# # =============================================================================
# # RETRY DECORATOR
# # =============================================================================

# def with_api_retry(
#     max_retries: int = API_MAX_RETRIES,
#     base_delay: float = API_RETRY_DELAY,
#     backoff: float = API_RETRY_BACKOFF,
#     retryable_exceptions: tuple = (Timeout, RequestsConnectionError, RequestException)
# ):
#     """
#     Decorator that adds retry logic with exponential backoff for API calls.
    
#     Args:
#         max_retries: Maximum number of retry attempts
#         base_delay: Initial delay between retries (seconds)
#         backoff: Multiplier for exponential backoff
#         retryable_exceptions: Tuple of exceptions to catch and retry
#     """
#     def decorator(func: Callable) -> Callable:
#         @wraps(func)
#         def wrapper(*args, **kwargs) -> Any:
#             last_exception = None
            
#             for attempt in range(max_retries + 1):
#                 try:
#                     result = func(*args, **kwargs)
#                     return result
                    
#                 except retryable_exceptions as e:
#                     last_exception = e
                    
#                     if attempt < max_retries:
#                         delay = base_delay * (backoff ** attempt)
#                         logger.warning(
#                             f"Geolocation API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
#                             f"Retrying in {delay:.2f}s..."
#                         )
#                         time.sleep(delay)
#                     else:
#                         logger.error(
#                             f"Geolocation API call failed after {max_retries + 1} attempts: {e}"
#                         )
            
#             # All retries exhausted
#             return {}
        
#         return wrapper
#     return decorator


# # =============================================================================
# # REDIS CACHE CONNECTION
# # =============================================================================

# _redis_cache = None
# _redis_connection_attempted = False


# def get_redis_cache():
#     """
#     Get Redis connection for geolocation caching.
    
#     Only attempts connection once to avoid repeated failures.
#     """
#     global _redis_cache, _redis_connection_attempted
    
#     if _redis_cache is not None:
#         return _redis_cache
    
#     if _redis_connection_attempted:
#         return None
    
#     _redis_connection_attempted = True
    
#     if not REDIS_AVAILABLE:
#         logger.debug("Redis not available for geolocation caching")
#         return None
    
#     try:
#         _redis_cache = redis.from_url(
#             REDIS_CACHE_URL,
#             decode_responses=True,
#             socket_connect_timeout=2,
#             socket_timeout=2
#         )
#         _redis_cache.ping()
#         logger.info("Redis cache connected for geolocation")
#         return _redis_cache
#     except Exception as e:
#         logger.warning(f"Redis cache unavailable for geolocation: {e}")
#         _redis_cache = None
#         return None


# # =============================================================================
# # IP ADDRESS UTILITIES
# # =============================================================================

# def get_client_ip(request) -> str:
#     """
#     Extract client IP address from Django request.
    
#     Handles various proxy headers (CloudFlare, AWS ALB, nginx, etc.)
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         IP address string
#     """
#     # CloudFlare
#     cf_connecting_ip = request.META.get('HTTP_CF_CONNECTING_IP')
#     if cf_connecting_ip:
#         return cf_connecting_ip.strip()
    
#     # Standard proxy header
#     x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
#     if x_forwarded_for:
#         # Take first IP (original client)
#         return x_forwarded_for.split(',')[0].strip()
    
#     # AWS ALB / other proxies
#     x_real_ip = request.META.get('HTTP_X_REAL_IP')
#     if x_real_ip:
#         return x_real_ip.strip()
    
#     # Direct connection
#     return request.META.get('REMOTE_ADDR', '')


# def is_private_ip(ip_address: str) -> bool:
#     """Check if IP is private/local (not worth looking up)."""
#     if not ip_address:
#         return True
    
#     ip_lower = ip_address.lower()
    
#     if ip_lower in ('localhost', ''):
#         return True
    
#     for prefix in PRIVATE_IP_PREFIXES:
#         if ip_address.startswith(prefix):
#             return True
    
#     return False


# # =============================================================================
# # IP GEOLOCATION (AbstractAPI)
# # =============================================================================

# @lru_cache(maxsize=500)
# def _get_location_from_api_cached(ip_address: str) -> str:
#     """
#     Internal cached API call wrapper.
    
#     Returns JSON string for LRU cache compatibility.
#     """
#     result = _get_location_from_api_internal(ip_address)
#     return json.dumps(result) if result else ''


# @with_api_retry()
# def _get_location_from_api_internal(ip_address: str) -> Dict[str, Any]:
#     """
#     Call AbstractAPI to get location data with retry logic.
    
#     Args:
#         ip_address: IP address to lookup
        
#     Returns:
#         Location dict or empty dict on failure
#     """
#     # Check circuit breaker first
#     if not _rate_limit_circuit.allows_request:
#         logger.debug(f"Rate limit circuit open, skipping API call for {ip_address}")
#         return {}
    
#     if not ABSTRACTAPI_KEY:
#         logger.warning("Key not configured in environment")
#         return {}
    
#     try:
#         response = requests.get(
#             ABSTRACTAPI_URL,
#             params={
#                 'api_key': ABSTRACTAPI_KEY,
#                 'ip_address': ip_address
#             },
#             timeout=API_TIMEOUT
#         )
        
#         if response.status_code == 200:
#             data = response.json()
            
#             # Record success (resets rate limit counter)
#             _rate_limit_circuit.record_success()
            
#             # Extract timezone safely
#             timezone = None
#             if isinstance(data.get('timezone'), dict):
#                 timezone = data['timezone'].get('name')
#             elif isinstance(data.get('timezone'), str):
#                 timezone = data['timezone']
            
#             # Extract connection/ISP info safely
#             isp = None
#             connection = data.get('connection')
#             if isinstance(connection, dict):
#                 isp = connection.get('isp_name') or connection.get('organization_name')
            
#             return {
#                 'ip': data.get('ip_address'),
#                 'city': data.get('city') or '',
#                 'region': data.get('region') or '',
#                 'region_code': data.get('region_iso_code') or '',
#                 'country': data.get('country') or '',
#                 'country_code': data.get('country_code') or '',
#                 'continent': data.get('continent') or '',
#                 'lat': data.get('latitude'),
#                 'lng': data.get('longitude'),
#                 'postal': data.get('postal_code') or '',
#                 'timezone': timezone or '',
#                 'isp': isp or '',
#                 'is_vpn': data.get('security', {}).get('is_vpn', False) if isinstance(data.get('security'), dict) else False
#             }
        
#         elif response.status_code == 429:
#             # Rate limited - record and trigger circuit breaker
#             _rate_limit_circuit.record_rate_limit()
#             logger.warning(
#                 f"AbstractAPI rate limit (429) for IP {ip_address}. "
#                 f"Circuit status: {_rate_limit_circuit.get_status()}"
#             )
#             return {}
        
#         elif response.status_code == 401:
#             logger.error("AbstractAPI authentication failed - check your geolocation key")
#             return {}
        
#         elif response.status_code == 422:
#             logger.warning(f"AbstractAPI invalid IP address: {ip_address}")
#             return {}
        
#         else:
#             logger.warning(f"AbstractAPI returned status {response.status_code} for IP {ip_address}")
#             return {}
            
#     except Timeout:
#         logger.warning(f"AbstractAPI timeout for IP {ip_address}")
#         raise  # Let retry decorator handle it
#     except RequestsConnectionError as e:
#         logger.warning(f"AbstractAPI connection error: {e}")
#         raise  # Let retry decorator handle it
#     except RequestException as e:
#         logger.warning(f"AbstractAPI request error: {e}")
#         raise  # Let retry decorator handle it
#     except json.JSONDecodeError as e:
#         logger.error(f"AbstractAPI returned invalid JSON: {e}")
#         return {}
#     except Exception as e:
#         logger.error(f"AbstractAPI unexpected error: {e}")
#         return {}


# def _get_location_from_api(ip_address: str) -> Dict[str, Any]:
#     """
#     Get location from API with LRU caching.
    
#     Args:
#         ip_address: IP address to lookup
        
#     Returns:
#         Location dict or empty dict
#     """
#     cached_json = _get_location_from_api_cached(ip_address)
#     if cached_json:
#         try:
#             return json.loads(cached_json)
#         except json.JSONDecodeError:
#             pass
#     return {}


# def get_location_from_ip(ip_address: str, use_cache: bool = True) -> Dict[str, Any]:
#     """
#     Get location data from IP address with multi-layer caching.
    
#     Cache layers:
#     1. In-memory LRU cache (fastest)
#     2. Redis cache (persists across restarts)
#     3. AbstractAPI call (slowest, costs money)
    
#     Features:
#     - Retry logic with exponential backoff
#     - Circuit breaker for rate limits
#     - Graceful degradation on failures
    
#     Args:
#         ip_address: IP address to lookup
#         use_cache: Whether to use Redis cache (default True)
        
#     Returns:
#         Location dict with city, country, lat, lng, etc.
#         Empty dict if lookup fails or IP is private.
#     """
#     if not ip_address or is_private_ip(ip_address):
#         return {}
    
#     # Normalize IP
#     ip_address = ip_address.strip()
#     cache_key = f"geo:{ip_address}"
    
#     # Layer 1: Check Redis cache
#     if use_cache:
#         redis_client = get_redis_cache()
#         if redis_client:
#             try:
#                 cached = redis_client.get(cache_key)
#                 if cached:
#                     try:
#                         return json.loads(cached)
#                     except json.JSONDecodeError:
#                         logger.debug(f"Invalid JSON in Redis cache for {ip_address}")
#             except RedisError as e:
#                 logger.debug(f"Redis cache read error: {e}")
#             except Exception as e:
#                 logger.debug(f"Redis cache read unexpected error: {e}")
    
#     # Layer 2: Call API (has internal LRU cache and retry logic)
#     location = _get_location_from_api(ip_address)
    
#     # Layer 3: Store in Redis cache (only if we got valid data)
#     if location and use_cache:
#         redis_client = get_redis_cache()
#         if redis_client:
#             try:
#                 redis_client.setex(
#                     cache_key,
#                     LOCATION_CACHE_TTL,
#                     json.dumps(location)
#                 )
#             except RedisError as e:
#                 logger.debug(f"Redis cache write error: {e}")
#             except Exception as e:
#                 logger.debug(f"Redis cache write unexpected error: {e}")
    
#     return location


# def clear_location_cache(ip_address: str = None) -> Dict[str, Any]:
#     """
#     Clear location cache.
    
#     Args:
#         ip_address: Specific IP to clear, or None to clear all
        
#     Returns:
#         Dict with status information
#     """
#     result = {
#         'lru_cleared': False,
#         'redis_cleared': False,
#         'redis_keys_deleted': 0
#     }
    
#     # Clear LRU cache
#     try:
#         _get_location_from_api_cached.cache_clear()
#         result['lru_cleared'] = True
#     except Exception as e:
#         logger.warning(f"Failed to clear LRU cache: {e}")
    
#     # Clear Redis cache
#     redis_client = get_redis_cache()
#     if redis_client:
#         try:
#             if ip_address:
#                 deleted = redis_client.delete(f"geo:{ip_address}")
#                 result['redis_keys_deleted'] = deleted
#             else:
#                 # Clear all geo keys
#                 keys_deleted = 0
#                 for key in redis_client.scan_iter("geo:*"):
#                     redis_client.delete(key)
#                     keys_deleted += 1
#                 result['redis_keys_deleted'] = keys_deleted
#             result['redis_cleared'] = True
#         except Exception as e:
#             logger.warning(f"Failed to clear Redis geo cache: {e}")
    
#     return result


# # =============================================================================
# # USER-AGENT / DEVICE DETECTION
# # =============================================================================

# def get_device_info(user_agent_string: str) -> Dict[str, Any]:
#     """
#     Parse User-Agent string to extract browser, device, and OS info.
    
#     Args:
#         user_agent_string: HTTP User-Agent header value
        
#     Returns:
#         Dict with browser, browser_version, device_type, os, os_version
#     """
#     if not user_agent_string:
#         return {
#             'browser': 'Unknown',
#             'browser_version': '',
#             'device_type': 'Unknown',
#             'os': 'Unknown',
#             'os_version': '',
#             'is_bot': False,
#             'is_mobile': False,
#             'is_tablet': False,
#             'is_pc': False
#         }
    
#     if USER_AGENTS_AVAILABLE:
#         # Use user-agents library (more accurate)
#         try:
#             ua = parse_user_agent(user_agent_string)
            
#             # Determine device type
#             if ua.is_mobile:
#                 device_type = 'Mobile'
#             elif ua.is_tablet:
#                 device_type = 'Tablet'
#             elif ua.is_pc:
#                 device_type = 'Desktop'
#             elif ua.is_bot:
#                 device_type = 'Bot'
#             else:
#                 device_type = 'Other'
            
#             return {
#                 'browser': ua.browser.family or 'Unknown',
#                 'browser_version': ua.browser.version_string or '',
#                 'device_type': device_type,
#                 'device_brand': ua.device.brand or '',
#                 'device_model': ua.device.model or '',
#                 'os': ua.os.family or 'Unknown',
#                 'os_version': ua.os.version_string or '',
#                 'is_bot': ua.is_bot,
#                 'is_mobile': ua.is_mobile,
#                 'is_tablet': ua.is_tablet,
#                 'is_pc': ua.is_pc
#             }
#         except Exception as e:
#             logger.warning(f"User-agent parsing error: {e}")
    
#     # Fallback: Basic parsing without library
#     return _parse_user_agent_basic(user_agent_string)


# def _parse_user_agent_basic(user_agent_string: str) -> Dict[str, Any]:
#     """
#     Basic User-Agent parsing without external library.
    
#     Less accurate but works without dependencies.
#     """
#     ua = user_agent_string.lower()
    
#     # Detect browser
#     browser = 'Unknown'
#     browser_version = ''
    
#     if 'edg/' in ua:
#         browser = 'Edge'
#     elif 'chrome/' in ua and 'chromium/' not in ua:
#         browser = 'Chrome'
#     elif 'firefox/' in ua:
#         browser = 'Firefox'
#     elif 'safari/' in ua and 'chrome/' not in ua:
#         browser = 'Safari'
#     elif 'opera/' in ua or 'opr/' in ua:
#         browser = 'Opera'
#     elif 'msie' in ua or 'trident/' in ua:
#         browser = 'Internet Explorer'
    
#     # Detect device type
#     device_type = 'Desktop'
#     is_mobile = False
#     is_tablet = False
    
#     if 'mobile' in ua or 'android' in ua and 'tablet' not in ua:
#         if 'tablet' in ua or 'ipad' in ua:
#             device_type = 'Tablet'
#             is_tablet = True
#         else:
#             device_type = 'Mobile'
#             is_mobile = True
#     elif 'tablet' in ua or 'ipad' in ua:
#         device_type = 'Tablet'
#         is_tablet = True
    
#     # Detect OS
#     os_name = 'Unknown'
#     os_version = ''
    
#     if 'windows' in ua:
#         os_name = 'Windows'
#         if 'windows nt 10' in ua:
#             os_version = '10'
#         elif 'windows nt 6.3' in ua:
#             os_version = '8.1'
#         elif 'windows nt 6.1' in ua:
#             os_version = '7'
#     elif 'mac os x' in ua or 'macintosh' in ua:
#         os_name = 'macOS'
#     elif 'iphone' in ua or 'ipad' in ua:
#         os_name = 'iOS'
#     elif 'android' in ua:
#         os_name = 'Android'
#     elif 'linux' in ua:
#         os_name = 'Linux'
    
#     # Detect bot
#     is_bot = any(bot in ua for bot in ['bot', 'crawler', 'spider', 'scraper', 'curl', 'wget'])
#     if is_bot:
#         device_type = 'Bot'
    
#     return {
#         'browser': browser,
#         'browser_version': browser_version,
#         'device_type': device_type,
#         'device_brand': '',
#         'device_model': '',
#         'os': os_name,
#         'os_version': os_version,
#         'is_bot': is_bot,
#         'is_mobile': is_mobile,
#         'is_tablet': is_tablet,
#         'is_pc': device_type == 'Desktop'
#     }


# # =============================================================================
# # COMBINED CLIENT INFO
# # =============================================================================

# def get_full_client_info(request) -> Dict[str, Any]:
#     """
#     Get complete client information from a Django request.
    
#     Combines IP geolocation and device detection into one call.
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         Dict with 'ip', 'location', and 'device' sub-dicts
#     """
#     # Get IP address
#     ip_address = get_client_ip(request)
    
#     # Get User-Agent
#     user_agent = request.META.get('HTTP_USER_AGENT', '')
    
#     # Get location (with caching) - returns empty dict for private IPs
#     location = get_location_from_ip(ip_address)
    
#     # Get device info
#     device = get_device_info(user_agent)
    
#     return {
#         'ip': ip_address,
#         'location': location,
#         'device': device,
#         'user_agent': user_agent
#     }


# def get_location_from_request(request) -> Optional[Dict[str, Any]]:
#     """
#     Get location from Django request with multiple fallbacks.
    
#     Priority:
#     1. URL parameters (city, country, region) - user-provided only
#     2. Session cached location
#     3. CloudFlare headers (if using CF)
#     4. IP geolocation API
    
#     NOTE: This function does NOT provide any test/fallback locations.
#     If location cannot be determined, returns None.
    
#     Args:
#         request: Django HttpRequest object
        
#     Returns:
#         Location dict or None if location cannot be determined
#     """
#     location = {}
    
#     # 1. Check URL params (user-provided location)
#     url_city = request.GET.get('city', '').strip()
#     url_country = request.GET.get('country', '').strip()
#     url_region = request.GET.get('region', '').strip()
    
#     if url_city:
#         location['city'] = url_city[:100]  # Limit length
#     if url_country:
#         location['country'] = url_country[:100]
#     if url_region:
#         location['region'] = url_region[:100]
    
#     # 2. Check session cache
#     if not location:
#         try:
#             session_location = request.session.get('user_location')
#             if session_location and isinstance(session_location, dict):
#                 # Validate session location has real data
#                 if session_location.get('city') or session_location.get('country'):
#                     location = session_location.copy()
#         except Exception as e:
#             logger.debug(f"Error reading session location: {e}")
    
#     # 3. Check CloudFlare headers (highly reliable if using CF)
#     if not location.get('country_code'):
#         cf_country = request.META.get('HTTP_CF_IPCOUNTRY')
#         if cf_country and cf_country not in ('XX', 'T1'):  # XX=unknown, T1=Tor
#             location['country_code'] = cf_country
    
#     # 4. IP Geolocation lookup (API with caching)
#     if not location.get('city'):
#         ip_address = get_client_ip(request)
#         if ip_address and not is_private_ip(ip_address):
#             ip_location = get_location_from_ip(ip_address)
#             if ip_location:
#                 # Merge IP location data (don't overwrite user-provided data)
#                 for key, value in ip_location.items():
#                     if value and not location.get(key):
#                         location[key] = value
    
#     # 5. Cache in session (only if we have valid location data)
#     if location and (location.get('city') or location.get('country')):
#         try:
#             request.session['user_location'] = location
#         except Exception as e:
#             logger.debug(f"Error saving session location: {e}")
    
#     # Return None if no location could be determined
#     # DO NOT provide any fallback/test locations
#     if not location or not (location.get('city') or location.get('country') or location.get('country_code')):
#         return None
    
#     return location


# # =============================================================================
# # UTILITY FUNCTIONS
# # =============================================================================

# def get_cache_stats() -> Dict[str, Any]:
#     """Get cache statistics for monitoring."""
#     stats = {
#         'lru_cache_info': _get_location_from_api_cached.cache_info()._asdict(),
#         'redis_available': False,
#         'redis_geo_keys': 0,
#         'rate_limit_circuit': _rate_limit_circuit.get_status()
#     }
    
#     redis_client = get_redis_cache()
#     if redis_client:
#         try:
#             stats['redis_available'] = True
#             # Count geo keys (limit scan for performance)
#             key_count = 0
#             for _ in redis_client.scan_iter("geo:*", count=100):
#                 key_count += 1
#                 if key_count >= 1000:  # Cap the count
#                     break
#             stats['redis_geo_keys'] = key_count
#             stats['redis_geo_keys_capped'] = key_count >= 1000
#         except Exception as e:
#             logger.debug(f"Error getting cache stats: {e}")
    
#     return stats


# def health_check() -> Dict[str, Any]:
#     """
#     Health check for geolocation service.
    
#     Returns:
#         Dict with health status
#     """
#     result = {
#         'healthy': True,
#         'api_configured': bool(ABSTRACTAPI_KEY),
#         'redis_available': False,
#         'rate_limit_circuit_open': _rate_limit_circuit.is_open,
#         'errors': []
#     }
    
#     # Check API key
#     if not ABSTRACTAPI_KEY:
#         result['errors'].append('geolocation key not configured')
#         result['healthy'] = False
    
#     # Check Redis
#     redis_client = get_redis_cache()
#     if redis_client:
#         try:
#             redis_client.ping()
#             result['redis_available'] = True
#         except Exception as e:
#             result['errors'].append(f'Redis unavailable: {e}')
    
#     # Check circuit breaker
#     if _rate_limit_circuit.is_open:
#         result['errors'].append('Rate limit circuit breaker is OPEN')
#         result['healthy'] = False
    
#     result['rate_limit_status'] = _rate_limit_circuit.get_status()
    
#     return result


"""
geolocation.py

Centralized geolocation and device detection for search analytics.

PRODUCTION FEATURES:
- API key from environment only (no hardcoding)
- Retry logic with exponential backoff for API failures
- Circuit breaker for rate limits (429 errors)
- Multi-layer caching (in-memory LRU + Redis)
- Graceful fallback on API failures
- Proper logging (no print statements)

API: AbstractAPI IP Intelligence (ip-intelligence.abstractapi.com/v1)

Response structure:
    {
        "ip_address": "...",
        "security": { "is_vpn": false, "is_proxy": false, ... },
        "asn": { "asn": 12345, "name": "...", ... },
        "company": { "name": "...", ... },
        "location": {
            "city": "Miami",
            "region": "Florida",
            "region_iso_code": "FL",
            "postal_code": "33197",
            "country": "United States",
            "country_code": "US",
            "continent": "North America",
            "continent_code": "NA",
            "longitude": -80.1946,
            "latitude": 25.7689,
            ...
        },
        "timezone": { "name": "America/New_York", ... },
        "flag": { ... },
        "currency": { ... }
    }

USAGE:
    from searchengine.geolocation import (
        get_location_from_ip,
        get_device_info,
        get_full_client_info
    )
    
    # Get location from IP
    location = get_location_from_ip('8.8.8.8')
    # Returns: {'city': 'Mountain View', 'country': 'US', ...}
    
    # Get device info from User-Agent
    device = get_device_info(request.META.get('HTTP_USER_AGENT'))
    # Returns: {'browser': 'Chrome', 'device_type': 'Desktop', ...}
    
    # Get everything at once
    client_info = get_full_client_info(request)
    # Returns: {'location': {...}, 'device': {...}, 'ip': '...'}
"""

import json
import logging
import time
import threading
from functools import lru_cache, wraps
from typing import Dict, Optional, Any, Callable, Tuple

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError
from decouple import config

# Optional: user-agents library for parsing
try:
    from user_agents import parse as parse_user_agent
    USER_AGENTS_AVAILABLE = True
except ImportError:
    USER_AGENTS_AVAILABLE = False

# Optional: Redis for persistent caching
try:
    import redis
    from redis.exceptions import RedisError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    RedisError = Exception  # Fallback for type hints


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION - ALL FROM ENVIRONMENT VARIABLES
# =============================================================================

# AbstractAPI IP Intelligence Configuration
ABSTRACTAPI_KEY = config('ABSTRACTAPI_GEOLOCATION_KEY_V2', default='')
ABSTRACTAPI_URL = config('ABSTRACTAPI_GEOLOCATION_URL', default='https://ip-intelligence.abstractapi.com/v1/')
API_TIMEOUT = config('GEOLOCATION_API_TIMEOUT', default=3, cast=int)  # seconds

# Redis Configuration (for persistent cache)
REDIS_CACHE_URL = config('REDIS_ANALYTICS_URL', default='redis://localhost:6379/2')
LOCATION_CACHE_TTL = config('LOCATION_CACHE_TTL', default=86400, cast=int)  # 24 hours

# Retry Configuration
API_MAX_RETRIES = config('GEOLOCATION_MAX_RETRIES', default=3, cast=int)
API_RETRY_DELAY = config('GEOLOCATION_RETRY_DELAY', default=0.5, cast=float)  # Base delay in seconds
API_RETRY_BACKOFF = config('GEOLOCATION_RETRY_BACKOFF', default=2.0, cast=float)  # Exponential backoff

# Circuit Breaker Configuration for Rate Limits
RATE_LIMIT_CIRCUIT_THRESHOLD = config('GEOLOCATION_RATE_LIMIT_THRESHOLD', default=3, cast=int)
RATE_LIMIT_CIRCUIT_TIMEOUT = config('GEOLOCATION_RATE_LIMIT_TIMEOUT', default=60, cast=int)  # seconds

# Private/local IPs to skip
PRIVATE_IP_PREFIXES = ('127.', '10.', '192.168.', '172.16.', '172.17.', '172.18.',
                       '172.19.', '172.20.', '172.21.', '172.22.', '172.23.',
                       '172.24.', '172.25.', '172.26.', '172.27.', '172.28.',
                       '172.29.', '172.30.', '172.31.', '169.254.', '::1', 'fe80:')


# =============================================================================
# CIRCUIT BREAKER FOR RATE LIMITS
# =============================================================================

class RateLimitCircuitBreaker:
    """
    Circuit breaker specifically for API rate limiting (429 errors).
    
    When rate limited, stops making API calls until the timeout expires.
    This prevents burning through rate limits and gives the API time to recover.
    """
    
    def __init__(
        self,
        threshold: int = RATE_LIMIT_CIRCUIT_THRESHOLD,
        timeout: int = RATE_LIMIT_CIRCUIT_TIMEOUT
    ):
        self.threshold = threshold
        self.timeout = timeout
        
        self._rate_limit_count = 0
        self._circuit_open = False
        self._circuit_opened_at: Optional[float] = None
        self._lock = threading.Lock()
    
    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests)."""
        with self._lock:
            if not self._circuit_open:
                return False
            
            # Check if timeout has expired
            if self._circuit_opened_at and \
               time.time() - self._circuit_opened_at >= self.timeout:
                self._circuit_open = False
                self._rate_limit_count = 0
                self._circuit_opened_at = None
                logger.info("Geolocation rate limit circuit breaker closed (timeout expired)")
                return False
            
            return True
    
    @property
    def allows_request(self) -> bool:
        """Check if a request should be allowed."""
        return not self.is_open
    
    def record_rate_limit(self) -> None:
        """Record a 429 rate limit response."""
        with self._lock:
            self._rate_limit_count += 1
            
            if self._rate_limit_count >= self.threshold:
                self._circuit_open = True
                self._circuit_opened_at = time.time()
                logger.warning(
                    f"Geolocation rate limit circuit breaker OPEN after {self._rate_limit_count} "
                    f"rate limits. Will retry in {self.timeout} seconds."
                )
    
    def record_success(self) -> None:
        """Record a successful request (resets rate limit count)."""
        with self._lock:
            if self._rate_limit_count > 0:
                self._rate_limit_count = 0
    
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._circuit_open = False
            self._rate_limit_count = 0
            self._circuit_opened_at = None
            logger.info("Geolocation rate limit circuit breaker manually reset")
    
    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status for monitoring."""
        with self._lock:
            remaining_timeout = 0
            if self._circuit_open and self._circuit_opened_at:
                remaining_timeout = max(0, self.timeout - (time.time() - self._circuit_opened_at))
            
            return {
                'is_open': self._circuit_open,
                'rate_limit_count': self._rate_limit_count,
                'threshold': self.threshold,
                'timeout': self.timeout,
                'remaining_timeout': round(remaining_timeout, 1),
                'opened_at': self._circuit_opened_at
            }


# Global circuit breaker instance
_rate_limit_circuit = RateLimitCircuitBreaker()


def get_rate_limit_status() -> Dict[str, Any]:
    """Get the current rate limit circuit breaker status."""
    return _rate_limit_circuit.get_status()


def reset_rate_limit_circuit() -> None:
    """Manually reset the rate limit circuit breaker."""
    _rate_limit_circuit.reset()


# =============================================================================
# RETRY DECORATOR
# =============================================================================

def with_api_retry(
    max_retries: int = API_MAX_RETRIES,
    base_delay: float = API_RETRY_DELAY,
    backoff: float = API_RETRY_BACKOFF,
    retryable_exceptions: tuple = (Timeout, RequestsConnectionError, RequestException)
):
    """
    Decorator that adds retry logic with exponential backoff for API calls.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    return result
                    
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        delay = base_delay * (backoff ** attempt)
                        logger.warning(
                            f"Geolocation API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"Geolocation API call failed after {max_retries + 1} attempts: {e}"
                        )
            
            # All retries exhausted
            return {}
        
        return wrapper
    return decorator


# =============================================================================
# REDIS CACHE CONNECTION
# =============================================================================

_redis_cache = None
_redis_connection_attempted = False


def get_redis_cache():
    """
    Get Redis connection for geolocation caching.
    Only attempts connection once to avoid repeated failures.
    """
    global _redis_cache, _redis_connection_attempted
    
    if _redis_cache is not None:
        return _redis_cache
    
    if _redis_connection_attempted:
        return None
    
    _redis_connection_attempted = True
    
    if not REDIS_AVAILABLE:
        logger.debug("Redis not available for geolocation caching")
        return None
    
    try:
        _redis_cache = redis.from_url(
            REDIS_CACHE_URL,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2
        )
        _redis_cache.ping()
        logger.info("Redis cache connected for geolocation")
        return _redis_cache
    except Exception as e:
        logger.warning(f"Redis cache unavailable for geolocation: {e}")
        _redis_cache = None
        return None


# =============================================================================
# IP ADDRESS UTILITIES
# =============================================================================

def get_client_ip(request) -> str:
    """
    Extract client IP address from Django request.
    Handles various proxy headers (CloudFlare, AWS ALB, nginx, etc.)
    """
    # CloudFlare
    cf_connecting_ip = request.META.get('HTTP_CF_CONNECTING_IP')
    if cf_connecting_ip:
        return cf_connecting_ip.strip()
    
    # Standard proxy header
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        return x_forwarded_for.split(',')[0].strip()
    
    # AWS ALB / other proxies
    x_real_ip = request.META.get('HTTP_X_REAL_IP')
    if x_real_ip:
        return x_real_ip.strip()
    
    # Direct connection
    return request.META.get('REMOTE_ADDR', '')


def is_private_ip(ip_address: str) -> bool:
    """Check if IP is private/local (not worth looking up)."""
    if not ip_address:
        return True
    
    ip_lower = ip_address.lower()
    
    if ip_lower in ('localhost', ''):
        return True
    
    for prefix in PRIVATE_IP_PREFIXES:
        if ip_address.startswith(prefix):
            return True
    
    return False


# =============================================================================
# IP GEOLOCATION (AbstractAPI IP Intelligence)
# =============================================================================

@lru_cache(maxsize=500)
def _get_location_from_api_cached(ip_address: str) -> str:
    """
    Internal cached API call wrapper.
    Returns JSON string for LRU cache compatibility.
    """
    result = _get_location_from_api_internal(ip_address)
    return json.dumps(result) if result else ''


@with_api_retry()
def _get_location_from_api_internal(ip_address: str) -> Dict[str, Any]:
    """
    Call AbstractAPI IP Intelligence to get location data with retry logic.
    
    API Response Structure (ip-intelligence.abstractapi.com/v1):
        {
            "ip_address": "185.197.192.65",
            "security": { "is_vpn": false, "is_proxy": true, ... },
            "asn": { "asn": 136787, "name": "PacketHub S.A.", ... },
            "company": { "name": "PacketHub S.A.", ... },
            "location": {
                "city": "Miami",
                "region": "Florida",
                "region_iso_code": "FL",
                "postal_code": "33197",
                "country": "United States",
                "country_code": "US",
                "continent": "North America",
                "continent_code": "NA",
                "longitude": -80.1946,
                "latitude": 25.7689,
                ...
            },
            "timezone": { "name": "America/New_York", ... },
            "flag": { ... },
            "currency": { ... }
        }
    """
    # Check circuit breaker first
    if not _rate_limit_circuit.allows_request:
        logger.debug(f"Rate limit circuit open, skipping API call for {ip_address}")
        return {}
    
    if not ABSTRACTAPI_KEY:
        logger.warning("ABSTRACTAPI_GEOLOCATION_KEY_V2 not configured in environment")
        return {}
    
    try:
        response = requests.get(
            ABSTRACTAPI_URL,
            params={
                'api_key': ABSTRACTAPI_KEY,
                'ip_address': ip_address
            },
            timeout=API_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Record success (resets rate limit counter)
            _rate_limit_circuit.record_success()
            
            # ── Extract nested location data ──
            loc = data.get('location') or {}
            
            # ── Extract timezone ──
            tz = data.get('timezone') or {}
            timezone = tz.get('name', '') if isinstance(tz, dict) else ''
            
            # ── Extract security info ──
            sec = data.get('security') or {}
            is_vpn = sec.get('is_vpn', False) if isinstance(sec, dict) else False
            is_proxy = sec.get('is_proxy', False) if isinstance(sec, dict) else False
            
            # ── Extract ISP/company info ──
            isp = ''
            company = data.get('company')
            if isinstance(company, dict):
                isp = company.get('name', '')
            if not isp:
                asn = data.get('asn')
                if isinstance(asn, dict):
                    isp = asn.get('name', '')
            
            return {
                'ip': data.get('ip_address', ''),
                'city': loc.get('city') or '',
                'region': loc.get('region') or '',
                'region_code': loc.get('region_iso_code') or '',
                'country': loc.get('country') or '',
                'country_code': loc.get('country_code') or '',
                'continent': loc.get('continent') or '',
                'continent_code': loc.get('continent_code') or '',
                'lat': loc.get('latitude'),
                'lng': loc.get('longitude'),
                'postal': loc.get('postal_code') or '',
                'timezone': timezone,
                'isp': isp,
                'is_vpn': is_vpn,
                'is_proxy': is_proxy,
            }
        
        elif response.status_code == 429:
            # Rate limited - record and trigger circuit breaker
            _rate_limit_circuit.record_rate_limit()
            logger.warning(
                f"AbstractAPI rate limit (429) for IP {ip_address}. "
                f"Circuit status: {_rate_limit_circuit.get_status()}"
            )
            return {}
        
        elif response.status_code == 401:
            logger.error("AbstractAPI authentication failed - check ABSTRACTAPI_GEOLOCATION_KEY_V2")
            return {}
        
        elif response.status_code == 422:
            logger.warning(f"AbstractAPI invalid IP address: {ip_address}")
            return {}
        
        else:
            logger.warning(f"AbstractAPI returned status {response.status_code} for IP {ip_address}")
            return {}
            
    except Timeout:
        logger.warning(f"AbstractAPI timeout for IP {ip_address}")
        raise  # Let retry decorator handle it
    except RequestsConnectionError as e:
        logger.warning(f"AbstractAPI connection error: {e}")
        raise  # Let retry decorator handle it
    except RequestException as e:
        logger.warning(f"AbstractAPI request error: {e}")
        raise  # Let retry decorator handle it
    except json.JSONDecodeError as e:
        logger.error(f"AbstractAPI returned invalid JSON: {e}")
        return {}
    except Exception as e:
        logger.error(f"AbstractAPI unexpected error: {e}")
        return {}


def _get_location_from_api(ip_address: str) -> Dict[str, Any]:
    """
    Get location from API with LRU caching.
    """
    cached_json = _get_location_from_api_cached(ip_address)
    if cached_json:
        try:
            return json.loads(cached_json)
        except json.JSONDecodeError:
            pass
    return {}


def get_location_from_ip(ip_address: str, use_cache: bool = True) -> Dict[str, Any]:
    """
    Get location data from IP address with multi-layer caching.
    
    Cache layers:
    1. In-memory LRU cache (fastest)
    2. Redis cache (persists across restarts)
    3. AbstractAPI call (slowest, costs money)
    
    Returns:
        Location dict with city, country, lat, lng, etc.
        Empty dict if lookup fails or IP is private.
    """
    if not ip_address or is_private_ip(ip_address):
        return {}
    
    # Normalize IP
    ip_address = ip_address.strip()
    cache_key = f"geo:{ip_address}"
    
    # Layer 1: Check Redis cache
    if use_cache:
        redis_client = get_redis_cache()
        if redis_client:
            try:
                cached = redis_client.get(cache_key)
                if cached:
                    try:
                        return json.loads(cached)
                    except json.JSONDecodeError:
                        logger.debug(f"Invalid JSON in Redis cache for {ip_address}")
            except RedisError as e:
                logger.debug(f"Redis cache read error: {e}")
            except Exception as e:
                logger.debug(f"Redis cache read unexpected error: {e}")
    
    # Layer 2: Call API (has internal LRU cache and retry logic)
    location = _get_location_from_api(ip_address)
    
    # Layer 3: Store in Redis cache (only if we got valid data)
    if location and use_cache:
        redis_client = get_redis_cache()
        if redis_client:
            try:
                redis_client.setex(
                    cache_key,
                    LOCATION_CACHE_TTL,
                    json.dumps(location)
                )
            except RedisError as e:
                logger.debug(f"Redis cache write error: {e}")
            except Exception as e:
                logger.debug(f"Redis cache write unexpected error: {e}")
    
    return location


def clear_location_cache(ip_address: str = None) -> Dict[str, Any]:
    """
    Clear location cache.
    
    Args:
        ip_address: Specific IP to clear, or None to clear all
    """
    result = {
        'lru_cleared': False,
        'redis_cleared': False,
        'redis_keys_deleted': 0
    }
    
    # Clear LRU cache
    try:
        _get_location_from_api_cached.cache_clear()
        result['lru_cleared'] = True
    except Exception as e:
        logger.warning(f"Failed to clear LRU cache: {e}")
    
    # Clear Redis cache
    redis_client = get_redis_cache()
    if redis_client:
        try:
            if ip_address:
                deleted = redis_client.delete(f"geo:{ip_address}")
                result['redis_keys_deleted'] = deleted
            else:
                keys_deleted = 0
                for key in redis_client.scan_iter("geo:*"):
                    redis_client.delete(key)
                    keys_deleted += 1
                result['redis_keys_deleted'] = keys_deleted
            result['redis_cleared'] = True
        except Exception as e:
            logger.warning(f"Failed to clear Redis geo cache: {e}")
    
    return result


# =============================================================================
# USER-AGENT / DEVICE DETECTION
# =============================================================================

def get_device_info(user_agent_string: str) -> Dict[str, Any]:
    """
    Parse User-Agent string to extract browser, device, and OS info.
    """
    if not user_agent_string:
        return {
            'browser': 'Unknown',
            'browser_version': '',
            'device_type': 'Unknown',
            'os': 'Unknown',
            'os_version': '',
            'is_bot': False,
            'is_mobile': False,
            'is_tablet': False,
            'is_pc': False
        }
    
    if USER_AGENTS_AVAILABLE:
        try:
            ua = parse_user_agent(user_agent_string)
            
            # Determine device type
            if ua.is_mobile:
                device_type = 'Mobile'
            elif ua.is_tablet:
                device_type = 'Tablet'
            elif ua.is_pc:
                device_type = 'Desktop'
            elif ua.is_bot:
                device_type = 'Bot'
            else:
                device_type = 'Other'
            
            return {
                'browser': ua.browser.family or 'Unknown',
                'browser_version': ua.browser.version_string or '',
                'device_type': device_type,
                'device_brand': ua.device.brand or '',
                'device_model': ua.device.model or '',
                'os': ua.os.family or 'Unknown',
                'os_version': ua.os.version_string or '',
                'is_bot': ua.is_bot,
                'is_mobile': ua.is_mobile,
                'is_tablet': ua.is_tablet,
                'is_pc': ua.is_pc
            }
        except Exception as e:
            logger.warning(f"User-agent parsing error: {e}")
    
    # Fallback: Basic parsing without library
    return _parse_user_agent_basic(user_agent_string)


def _parse_user_agent_basic(user_agent_string: str) -> Dict[str, Any]:
    """
    Basic User-Agent parsing without external library.
    """
    ua = user_agent_string.lower()
    
    # Detect browser
    browser = 'Unknown'
    browser_version = ''
    
    if 'edg/' in ua:
        browser = 'Edge'
    elif 'chrome/' in ua and 'chromium/' not in ua:
        browser = 'Chrome'
    elif 'firefox/' in ua:
        browser = 'Firefox'
    elif 'safari/' in ua and 'chrome/' not in ua:
        browser = 'Safari'
    elif 'opera/' in ua or 'opr/' in ua:
        browser = 'Opera'
    elif 'msie' in ua or 'trident/' in ua:
        browser = 'Internet Explorer'
    
    # Detect device type
    device_type = 'Desktop'
    is_mobile = False
    is_tablet = False
    
    if 'mobile' in ua or 'android' in ua and 'tablet' not in ua:
        if 'tablet' in ua or 'ipad' in ua:
            device_type = 'Tablet'
            is_tablet = True
        else:
            device_type = 'Mobile'
            is_mobile = True
    elif 'tablet' in ua or 'ipad' in ua:
        device_type = 'Tablet'
        is_tablet = True
    
    # Detect OS
    os_name = 'Unknown'
    os_version = ''
    
    if 'windows' in ua:
        os_name = 'Windows'
        if 'windows nt 10' in ua:
            os_version = '10'
        elif 'windows nt 6.3' in ua:
            os_version = '8.1'
        elif 'windows nt 6.1' in ua:
            os_version = '7'
    elif 'mac os x' in ua or 'macintosh' in ua:
        os_name = 'macOS'
    elif 'iphone' in ua or 'ipad' in ua:
        os_name = 'iOS'
    elif 'android' in ua:
        os_name = 'Android'
    elif 'linux' in ua:
        os_name = 'Linux'
    
    # Detect bot
    is_bot = any(bot in ua for bot in ['bot', 'crawler', 'spider', 'scraper', 'curl', 'wget'])
    if is_bot:
        device_type = 'Bot'
    
    return {
        'browser': browser,
        'browser_version': browser_version,
        'device_type': device_type,
        'device_brand': '',
        'device_model': '',
        'os': os_name,
        'os_version': os_version,
        'is_bot': is_bot,
        'is_mobile': is_mobile,
        'is_tablet': is_tablet,
        'is_pc': device_type == 'Desktop'
    }


# =============================================================================
# COMBINED CLIENT INFO
# =============================================================================

def get_full_client_info(request) -> Dict[str, Any]:
    """
    Get complete client information from a Django request.
    Combines IP geolocation and device detection into one call.
    
    NOTE: This does NOT use session caching. For views that get called
    frequently (like the home page), use get_location_from_request() instead
    which caches in the Django session.
    """
    ip_address = get_client_ip(request)
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    location = get_location_from_ip(ip_address)
    device = get_device_info(user_agent)
    
    return {
        'ip': ip_address,
        'location': location,
        'device': device,
        'user_agent': user_agent
    }


def get_location_from_request(request) -> Optional[Dict[str, Any]]:
    """
    Get location from Django request with session caching and multiple fallbacks.
    
    Priority:
    1. URL parameters (city, country, region) - user-provided only
    2. Session cached location
    3. CloudFlare headers (if using CF)
    4. IP geolocation API
    
    API is only called ONCE per session. All subsequent calls read from session.
    
    Returns:
        Location dict or None if location cannot be determined
    """
    location = {}
    
    # 1. Check URL params (user-provided location)
    url_city = request.GET.get('city', '').strip()
    url_country = request.GET.get('country', '').strip()
    url_region = request.GET.get('region', '').strip()
    
    if url_city:
        location['city'] = url_city[:100]
    if url_country:
        location['country'] = url_country[:100]
    if url_region:
        location['region'] = url_region[:100]
    
    # 2. Check session cache
    if not location:
        try:
            session_location = request.session.get('user_location')
            if session_location and isinstance(session_location, dict):
                if session_location.get('city') or session_location.get('country'):
                    location = session_location.copy()
        except Exception as e:
            logger.debug(f"Error reading session location: {e}")
    
    # 3. Check CloudFlare headers (highly reliable if using CF)
    if not location.get('country_code'):
        cf_country = request.META.get('HTTP_CF_IPCOUNTRY')
        if cf_country and cf_country not in ('XX', 'T1'):
            location['country_code'] = cf_country
    
    # 4. IP Geolocation lookup (API with caching)
    if not location.get('city'):
        ip_address = get_client_ip(request)
        if ip_address and not is_private_ip(ip_address):
            ip_location = get_location_from_ip(ip_address)
            if ip_location:
                for key, value in ip_location.items():
                    if value and not location.get(key):
                        location[key] = value
    
    # 5. Cache in session (only if we have valid location data)
    if location and (location.get('city') or location.get('country')):
        try:
            request.session['user_location'] = location
        except Exception as e:
            logger.debug(f"Error saving session location: {e}")
    
    # Return None if no location could be determined
    if not location or not (location.get('city') or location.get('country') or location.get('country_code')):
        return None
    
    return location


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics for monitoring."""
    stats = {
        'lru_cache_info': _get_location_from_api_cached.cache_info()._asdict(),
        'redis_available': False,
        'redis_geo_keys': 0,
        'rate_limit_circuit': _rate_limit_circuit.get_status()
    }
    
    redis_client = get_redis_cache()
    if redis_client:
        try:
            stats['redis_available'] = True
            key_count = 0
            for _ in redis_client.scan_iter("geo:*", count=100):
                key_count += 1
                if key_count >= 1000:
                    break
            stats['redis_geo_keys'] = key_count
            stats['redis_geo_keys_capped'] = key_count >= 1000
        except Exception as e:
            logger.debug(f"Error getting cache stats: {e}")
    
    return stats


def health_check() -> Dict[str, Any]:
    """Health check for geolocation service."""
    result = {
        'healthy': True,
        'api_configured': bool(ABSTRACTAPI_KEY),
        'redis_available': False,
        'rate_limit_circuit_open': _rate_limit_circuit.is_open,
        'errors': []
    }
    
    if not ABSTRACTAPI_KEY:
        result['errors'].append('ABSTRACTAPI_GEOLOCATION_KEY_V2 not configured')
        result['healthy'] = False
    
    redis_client = get_redis_cache()
    if redis_client:
        try:
            redis_client.ping()
            result['redis_available'] = True
        except Exception as e:
            result['errors'].append(f'Redis unavailable: {e}')
    
    if _rate_limit_circuit.is_open:
        result['errors'].append('Rate limit circuit breaker is OPEN')
        result['healthy'] = False
    
    result['rate_limit_status'] = _rate_limit_circuit.get_status()
    
    return result