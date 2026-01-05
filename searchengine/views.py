from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Union
from django.views.decorators.csrf import csrf_exempt

import redis
import typesense
from typesense.exceptions import (
    ObjectNotFound,
    RequestMalformed,
    RequestUnauthorized,
    ServerError,
    ServiceUnavailable,
    Timeout as TypesenseTimeout,
)

from django.conf import settings
from django.core.cache import cache
from django.http import Http404, HttpResponseBadRequest, JsonResponse
from django.shortcuts import redirect, render
from django.utils.html import escape
from django.views.decorators.http import require_GET, require_http_methods

from decouple import config
# Near the top with other imports
try:
    from .redis_analytics import SearchAnalytics
    ANALYTICS_AVAILABLE = True
except ImportError:
    ANALYTICS_AVAILABLE = False
    SearchAnalytics = None

# Local imports - wrap in try/except for graceful degradation
try:
    from .searchapi import get_autocomplete
except ImportError:
    get_autocomplete = None

try:
    from .searchsubmission import process_search_submission
except ImportError:
    process_search_submission = None

try:
    from .word_discovery import word_discovery_multi
except ImportError:
    word_discovery_multi = None

try:
    from .typesense_calculations import (
        detect_query_intent,
        execute_full_search,
        get_facets,
        get_featured_result,
        get_related_searches,
        log_search_event,
    )
except ImportError:
    detect_query_intent = None
    execute_full_search = None
    get_facets = None
    get_featured_result = None
    get_related_searches = None
    log_search_event = None


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logger = logging.getLogger(__name__)

# Near the top with other imports
from .geolocation import (
    get_client_ip,
    get_device_info,
    get_full_client_info,
    get_location_from_request
)
# =============================================================================
# REDIS CLIENT WITH CONNECTION POOLING & ERROR HANDLING
# =============================================================================

class RedisManager:
    """Thread-safe Redis connection manager with pooling and error handling."""
    
    _instance: Optional['RedisManager'] = None
    _client: Optional[redis.Redis] = None
    _pool: Optional[redis.ConnectionPool] = None
    _available: bool = False
    
    def __new__(cls) -> 'RedisManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize Redis connection pool."""
        try:
            self._pool = redis.ConnectionPool(
                host=config('REDIS_HOST', default='localhost'),
                port=config('REDIS_PORT', default=6379, cast=int),
                db=config('REDIS_DB', default=0, cast=int),
                password=config('REDIS_PASSWORD', default=None),
                decode_responses=True,
                max_connections=20,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
            self._client = redis.Redis(connection_pool=self._pool)
            # Test connection
            self._client.ping()
            self._available = True
            logger.info("Redis connection established successfully")
        except (redis.ConnectionError, redis.TimeoutError, Exception) as e:
            logger.warning(f"Redis unavailable: {e}. Falling back to Django cache.")
            self._client = None
            self._available = False
    
    @property
    def client(self) -> Optional[redis.Redis]:
        return self._client
    
    @property
    def available(self) -> bool:
        return self._available
    
    def safe_get(self, key: str, default: Any = None) -> Any:
        """Safely get a value from Redis."""
        if not self._available or not self._client:
            return default
        try:
            value = self._client.get(key)
            return value if value is not None else default
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis get error for key {key}: {e}")
            return default
    
    def safe_set(self, key: str, value: Any, ex: int = 300) -> bool:
        """Safely set a value in Redis."""
        if not self._available or not self._client:
            return False
        try:
            self._client.set(key, value, ex=ex)
            return True
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis set error for key {key}: {e}")
            return False
    
    def safe_hgetall(self, key: str) -> Dict:
        """Safely get all hash values."""
        if not self._available or not self._client:
            return {}
        try:
            return self._client.hgetall(key) or {}
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis hgetall error for key {key}: {e}")
            return {}
    
    def safe_zrevrange(self, key: str, start: int, end: int) -> List:
        """Safely get sorted set range."""
        if not self._available or not self._client:
            return []
        try:
            return self._client.zrevrange(key, start, end) or []
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis zrevrange error for key {key}: {e}")
            return []
    
    def safe_incr(self, key: str, ex: int = 60) -> int:
        """Safely increment a counter with expiry."""
        if not self._available or not self._client:
            return 0
        try:
            pipe = self._client.pipeline()
            pipe.incr(key)
            pipe.expire(key, ex)
            results = pipe.execute()
            return results[0] if results else 0
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis incr error for key {key}: {e}")
            return 0


# Initialize Redis manager
redis_manager = RedisManager()


# =============================================================================
# TYPESENSE CLIENT WITH RETRY LOGIC & ERROR HANDLING
# =============================================================================

class TypesenseManager:
    """Thread-safe Typesense connection manager with error handling."""
    
    _instance: Optional['TypesenseManager'] = None
    _client: Optional[typesense.Client] = None
    _available: bool = False
    _last_check: float = 0
    _check_interval: int = 60  # Re-check availability every 60 seconds
    
    def __new__(cls) -> 'TypesenseManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize Typesense client."""
        try:
            self._client = typesense.Client({
                'api_key': config('TYPESENSE_API_KEY'),
                'nodes': [{
                    'host': config('TYPESENSE_HOST'),
                    'port': config('TYPESENSE_PORT'),
                    'protocol': config('TYPESENSE_PROTOCOL', default='http')
                }],
                'connection_timeout_seconds': 5,
                'num_retries': 3,
            })
            # Test connection
            self._client.collections.retrieve()
            self._available = True
            self._last_check = time.time()
            logger.info("Typesense connection established successfully")
        except Exception as e:
            logger.error(f"Typesense initialization failed: {e}")
            self._client = None
            self._available = False
    
    @property
    def client(self) -> Optional[typesense.Client]:
        # Periodically re-check availability if previously unavailable
        if not self._available and time.time() - self._last_check > self._check_interval:
            self._initialize()
        return self._client
    
    @property
    def available(self) -> bool:
        return self._available and self._client is not None
    
    def search(
        self,
        collection: str,
        params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Execute a search with comprehensive error handling."""
        if not self.available:
            logger.error("Typesense client not available")
            return None
        
        try:
            return self._client.collections[collection].documents.search(params)
        except TypesenseTimeout as e:
            logger.error(f"Typesense timeout: {e}")
            return None
        except ServiceUnavailable as e:
            logger.error(f"Typesense service unavailable: {e}")
            self._available = False
            return None
        except ObjectNotFound as e:
            logger.warning(f"Typesense collection not found: {e}")
            return None
        except RequestMalformed as e:
            logger.error(f"Typesense request malformed: {e}")
            return None
        except ServerError as e:
            logger.error(f"Typesense server error: {e}")
            return None
        except Exception as e:
            logger.error(f"Typesense unexpected error: {e}")
            return None
    
    def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a single document with error handling."""
        if not self.available:
            return None
        
        try:
            return self._client.collections[collection].documents[doc_id].retrieve()
        except ObjectNotFound:
            return None
        except Exception as e:
            logger.error(f"Typesense get_document error: {e}")
            return None



# =============================================================================
# ANALYTICS CLIENT WITH CONNECTION RETRY LOGIC
# =============================================================================

_analytics_client = None
_analytics_last_attempt: float = 0
_analytics_retry_interval: int = 30  # Retry every 30 seconds
_analytics_max_retries: int = 3
_analytics_retry_count: int = 0

def get_analytics():
    """
    Get or create SearchAnalytics instance with connection retry logic.
    
    Implements exponential backoff and maximum retry limits to prevent
    repeated connection attempts that could impact performance.
    """
    global _analytics_client, _analytics_last_attempt, _analytics_retry_count
    
    # Return existing client if available
    if _analytics_client is not None:
        return _analytics_client
    
    # Check if analytics is available at all
    if not ANALYTICS_AVAILABLE:
        return None
    
    # Check if we've exceeded max retries
    if _analytics_retry_count >= _analytics_max_retries:
        # Only retry after extended interval (5 minutes) once max retries exceeded
        extended_interval = 300  # 5 minutes
        if time.time() - _analytics_last_attempt < extended_interval:
            return None
        # Reset retry count for fresh attempt after extended wait
        _analytics_retry_count = 0
    
    # Check if enough time has passed since last attempt
    current_time = time.time()
    # Exponential backoff: 30s, 60s, 120s
    backoff_interval = _analytics_retry_interval * (2 ** _analytics_retry_count)
    
    if current_time - _analytics_last_attempt < backoff_interval:
        return None
    
    # Attempt to create analytics client
    _analytics_last_attempt = current_time
    
    try:
        _analytics_client = SearchAnalytics()
        # Reset retry count on success
        _analytics_retry_count = 0
        logger.info("SearchAnalytics initialized successfully")
        return _analytics_client
    except redis.ConnectionError as e:
        _analytics_retry_count += 1
        logger.warning(
            f"SearchAnalytics Redis connection failed (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}"
        )
    except redis.TimeoutError as e:
        _analytics_retry_count += 1
        logger.warning(
            f"SearchAnalytics Redis timeout (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}"
        )
    except Exception as e:
        _analytics_retry_count += 1
        logger.warning(
            f"Failed to initialize SearchAnalytics (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}"
        )
    
    return None


def reset_analytics_client():
    """
    Reset the analytics client for testing or recovery purposes.
    """
    global _analytics_client, _analytics_last_attempt, _analytics_retry_count
    _analytics_client = None
    _analytics_last_attempt = 0
    _analytics_retry_count = 0


# Initialize Typesense manager
typesense_manager = TypesenseManager()

# Collection name constant
COLLECTION_NAME = 'documents'


# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# Immutable set for faster lookups
SUPPORTED_CITIES: frozenset = frozenset([
    'Atlanta', 'Houston', 'Chicago', 'Detroit',
    'New York', 'Los Angeles', 'Philadelphia', 'Washington',
    'Baltimore', 'Charlotte', 'Memphis', 'New Orleans',
    'Oakland', 'Miami', 'Dallas', 'St. Louis'
])

DEFAULT_CITY: str = 'Atlanta'

# City aliases for fuzzy matching
CITY_ALIASES: Dict[str, str] = {
    'nyc': 'New York',
    'ny': 'New York',
    'la': 'Los Angeles',
    'dc': 'Washington',
    'washington dc': 'Washington',
    'washington d.c.': 'Washington',
    'philly': 'Philadelphia',
    'nola': 'New Orleans',
    'chi': 'Chicago',
    'atl': 'Atlanta',
    'htx': 'Houston',
}

SUBCATEGORY_MAP: Dict[str, List[str]] = {
    'restaurants': ['restaurant', 'food', 'dining', 'catering', 'cafe'],
    'beauty': ['salon', 'barber', 'beauty', 'hair', 'nails', 'spa'],
    'professional': ['lawyer', 'accountant', 'consultant', 'financial', 'insurance'],
    'contractors': ['contractor', 'plumber', 'electrician', 'construction', 'handyman'],
    'realestate': ['real estate', 'realtor', 'property', 'mortgage', 'housing'],
    'retail': ['retail', 'store', 'shop', 'boutique', 'clothing'],
    'creative': ['photography', 'design', 'art', 'music', 'media', 'marketing'],
    'tech': ['technology', 'software', 'web', 'app', 'IT', 'digital'],
}

SCHEMA_MAP: Dict[str, str] = {
    'business': 'business',
    'culture': 'culture',
    'health': 'health',
    'news': 'news',
    'community': 'community',
    'lifestyle': 'lifestyle',
    'education': 'education',
    'media': 'media',
}

TEMPLATE_MAP: Dict[str, str] = {
    'business': 'category_business.html',
    'culture': 'category_culture.html',
    'health': 'category_health.html',
    'news': 'category_news.html',
    'community': 'category_community.html',
    'lifestyle': 'category_lifestyle.html',
    'education': 'category_education.html',
    'media': 'category_media.html',
}

SEARCH_CONFIG: Dict[str, Any] = {
    'max_timestamp_age_seconds': 300,
    'rate_limit_per_minute': 60,
    'min_typing_time_ms': 50,
    'max_query_length': 500,
    'min_query_length': 1,
    'nonce_expiry_seconds': 60,
    'default_per_page': 20,
    'max_per_page': 100,
    'min_per_page': 1,
    'max_page': 1000,
    'cache_timeout': 300,
}

# Track click rate limiting configuration
TRACK_CLICK_CONFIG: Dict[str, Any] = {
    'rate_limit_per_minute': 120,  # Max clicks per session per minute
    'rate_limit_per_hour': 1000,   # Max clicks per session per hour
    'max_url_length': 2000,
    'max_title_length': 500,
    'max_query_length': 500,
    'max_source_length': 200,
    'max_result_id_length': 100,
    'max_session_id_length': 50,
    'max_request_id_length': 100,
    'max_corrected_query_length': 500,
    'max_event_data_size': 10000,  # 10KB max for event data
}


# =============================================================================
# INPUT VALIDATION & SANITIZATION
# =============================================================================

def sanitize_query(query: Any) -> str:
    """
    Sanitize search query to prevent injection and handle edge cases.
    
    Args:
        query: Raw query input (can be any type)
        
    Returns:
        Sanitized query string
    """
    if query is None:
        return ''
    
    # Convert to string
    try:
        query = str(query)
    except (TypeError, ValueError):
        return ''
    
    # Trim whitespace
    query = query.strip()
    
    # Check length
    if len(query) > SEARCH_CONFIG['max_query_length']:
        query = query[:SEARCH_CONFIG['max_query_length']]
    
    # Remove null bytes and control characters
    query = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', query)
    
    # Collapse multiple spaces
    query = re.sub(r'\s+', ' ', query)
    
    # Remove potentially dangerous patterns for Typesense
    # Keep most punctuation for semantic search
    query = re.sub(r'[<>{}|\[\]\\^`]', '', query)
    
    return query


def sanitize_filter_value(value: Any) -> str:
    """
    Sanitize filter values to prevent injection.
    
    Args:
        value: Raw filter value
        
    Returns:
        Sanitized filter value string
    """
    if value is None:
        return ''
    
    try:
        value = str(value).strip()
    except (TypeError, ValueError):
        return ''
    
    # Limit length
    value = value[:200]
    
    # Remove characters that could break Typesense filter syntax
    value = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', value)
    
    return value


def sanitize_url(url: Any, max_length: int = 2000) -> str:
    """
    Sanitize URL input.
    
    Args:
        url: Raw URL input
        max_length: Maximum allowed length
        
    Returns:
        Sanitized URL string
    """
    if url is None:
        return ''
    
    try:
        url = str(url).strip()
    except (TypeError, ValueError):
        return ''
    
    # Limit length
    if len(url) > max_length:
        url = url[:max_length]
    
    # Remove null bytes and control characters
    url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url)
    
    # Basic URL validation - must start with http:// or https://
    if url and not url.startswith(('http://', 'https://')):
        return ''
    
    return url


def sanitize_string(value: Any, max_length: int = 500) -> str:
    """
    General string sanitization.
    
    Args:
        value: Raw string input
        max_length: Maximum allowed length
        
    Returns:
        Sanitized string
    """
    if value is None:
        return ''
    
    try:
        value = str(value).strip()
    except (TypeError, ValueError):
        return ''
    
    # Limit length
    if len(value) > max_length:
        value = value[:max_length]
    
    # Remove null bytes and control characters
    value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
    
    return value


def sanitize_int(value: Any, default: int = 0, min_val: int = None, max_val: int = None) -> int:
    """
    Sanitize integer input with bounds checking.
    
    Args:
        value: Raw integer input
        default: Default value if invalid
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        Validated integer
    """
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    
    if min_val is not None and result < min_val:
        return min_val
    if max_val is not None and result > max_val:
        return max_val
    
    return result


def validate_page(page: Any, default: int = 1) -> int:
    """
    Validate and sanitize page number.
    
    Args:
        page: Raw page input
        default: Default value if invalid
        
    Returns:
        Valid page number
    """
    try:
        page = int(page)
    except (TypeError, ValueError):
        return default
    
    if page < 1:
        return default
    if page > SEARCH_CONFIG['max_page']:
        return SEARCH_CONFIG['max_page']
    
    return page


def validate_per_page(per_page: Any, default: int = 20) -> int:
    """
    Validate and sanitize per_page parameter.
    
    Args:
        per_page: Raw per_page input
        default: Default value if invalid
        
    Returns:
        Valid per_page number
    """
    try:
        per_page = int(per_page)
    except (TypeError, ValueError):
        return default
    
    if per_page < SEARCH_CONFIG['min_per_page']:
        return default
    if per_page > SEARCH_CONFIG['max_per_page']:
        return SEARCH_CONFIG['max_per_page']
    
    return per_page


def validate_sort(sort: Any, allowed: List[str], default: str = 'authority') -> str:
    """
    Validate sort parameter against allowed values.
    
    Args:
        sort: Raw sort input
        allowed: List of allowed sort values
        default: Default value if invalid
        
    Returns:
        Valid sort value
    """
    if not sort:
        return default
    
    sort = str(sort).lower().strip()
    
    if sort in allowed:
        return sort
    
    return default


def get_user_city(request, default: str = DEFAULT_CITY) -> str:
    """
    Get user's city from various sources with validation.
    
    Priority: URL param > Session > Cookie > Default
    
    Args:
        request: Django request object
        default: Default city if none found
        
    Returns:
        Validated city name
    """
    city = ''
    
    # 1. Check URL param first
    city = request.GET.get('city', '')
    
    # 2. Then session
    if not city:
        try:
            city = request.session.get('user_city', '')
        except Exception:
            pass
    
    # 3. Normalize if we have a value
    if city:
        city = city.strip()
        
        # Check aliases (case-insensitive)
        city_lower = city.lower()
        if city_lower in CITY_ALIASES:
            city = CITY_ALIASES[city_lower]
        else:
            # Title case for display
            city = city.title()
    
    # 4. Validate against allowed cities
    if city not in SUPPORTED_CITIES:
        city = default
    
    # 5. Save to session for future requests
    try:
        request.session['user_city'] = city
    except Exception:
        pass  # Session might not be available
    
    return city


# =============================================================================
# CACHING UTILITIES
# =============================================================================

def get_cache_key(*args, prefix: str = 'afrotodo') -> str:
    """
    Generate a consistent cache key from arguments.
    
    Args:
        *args: Values to include in key
        prefix: Key prefix for namespacing
        
    Returns:
        MD5 hash-based cache key
    """
    key_data = ':'.join(str(arg) for arg in args if arg is not None)
    hash_value = hashlib.md5(key_data.encode()).hexdigest()
    return f"{prefix}:{hash_value}"


def safe_cache_get(key: str, default: Any = None) -> Any:
    """Safely get from Django cache with fallback."""
    try:
        result = cache.get(key)
        return result if result is not None else default
    except Exception as e:
        logger.warning(f"Cache get error for key {key}: {e}")
        return default


def safe_cache_set(key: str, value: Any, timeout: int = 300) -> bool:
    """Safely set Django cache with error handling."""
    try:
        cache.set(key, value, timeout)
        return True
    except Exception as e:
        logger.warning(f"Cache set error for key {key}: {e}")
        return False


# =============================================================================
# SECURITY VALIDATION
# =============================================================================

class SearchSecurityValidator:
    """Validates security parameters from search requests."""
    
    @staticmethod
    def validate_timestamp(timestamp_str: Optional[str]) -> Tuple[bool, Optional[str]]:
        """
        Validate timestamp is recent (prevent replay attacks).
        
        Args:
            timestamp_str: Timestamp string in milliseconds
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not timestamp_str:
            return True, None
        
        try:
            timestamp = int(timestamp_str)
            current_time = int(time.time() * 1000)
            age_seconds = (current_time - timestamp) / 1000
            
            # Allow some clock skew
            if age_seconds < -60:
                return False, "Timestamp is in the future"
            
            if age_seconds > SEARCH_CONFIG['max_timestamp_age_seconds']:
                return False, "Request too old"
            
            return True, None
            
        except (ValueError, TypeError):
            return True, None  # Don't block on invalid format
    
    @staticmethod
    def validate_nonce(nonce: Optional[str], session_id: str) -> Tuple[bool, Optional[str]]:
        """
        Validate nonce hasn't been used (prevent replay).
        
        Args:
            nonce: Unique request nonce
            session_id: User session ID
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not nonce or not session_id:
            return True, None
        
        if len(nonce) < 8 or len(nonce) > 64:
            return False, "Invalid nonce format"
        
        cache_key = f"nonce:{session_id}:{nonce}"
        
        if safe_cache_get(cache_key):
            return False, "Nonce already used"
        
        safe_cache_set(cache_key, True, SEARCH_CONFIG['nonce_expiry_seconds'])
        return True, None
    
    @staticmethod
    def check_rate_limit(session_id: str, client_fp: str = '') -> Tuple[bool, Optional[str]]:
        """
        Check if request is within rate limits.
        
        Args:
            session_id: User session ID
            client_fp: Client fingerprint
            
        Returns:
            Tuple of (is_allowed, error_message)
        """
        if not session_id:
            return True, None
        
        rate_key = f"rate:{session_id}:{client_fp or 'unknown'}"
        
        try:
            current_count = safe_cache_get(rate_key, 0)
            
            if current_count >= SEARCH_CONFIG['rate_limit_per_minute']:
                return False, "Rate limit exceeded"
            
            safe_cache_set(rate_key, current_count + 1, 60)
            return True, None
        except Exception:
            return True, None  # Don't block on cache errors
    
    @staticmethod
    def detect_bot(typing_time_ms: Optional[str], request_sequence: Optional[str]) -> Tuple[bool, Optional[str]]:
        """
        Simple bot detection heuristics.
        
        Args:
            typing_time_ms: Time spent typing in milliseconds
            request_sequence: Request sequence number
            
        Returns:
            Tuple of (is_suspicious, reason)
        """
        try:
            typing_time = int(typing_time_ms) if typing_time_ms else 0
            req_seq = int(request_sequence) if request_sequence else 0
            
            if typing_time > 0 and typing_time < SEARCH_CONFIG['min_typing_time_ms']:
                return True, "Typing too fast"
            
            if req_seq > 200:
                return True, "Excessive requests in session"
            
            return False, None
            
        except (ValueError, TypeError):
            return False, None


class TrackClickRateLimiter:
    """Rate limiter specifically for click tracking endpoints."""
    
    @staticmethod
    def check_rate_limit(session_id: str, client_ip: str = '') -> Tuple[bool, Optional[str]]:
        """
        Check if click tracking request is within rate limits.
        
        Uses both per-minute and per-hour limits to prevent abuse.
        
        Args:
            session_id: User session ID
            client_ip: Client IP address
            
        Returns:
            Tuple of (is_allowed, error_message)
        """
        if not session_id:
            # Fall back to IP-based limiting if no session
            if not client_ip:
                return True, None
            identifier = f"ip:{client_ip}"
        else:
            identifier = f"session:{session_id}"
        
        # Check per-minute limit
        minute_key = f"track_click_rate:minute:{identifier}"
        minute_count = redis_manager.safe_incr(minute_key, ex=60)
        
        if minute_count > TRACK_CLICK_CONFIG['rate_limit_per_minute']:
            logger.warning(f"Track click rate limit exceeded (minute) for {identifier}")
            return False, "Rate limit exceeded. Please slow down."
        
        # Check per-hour limit
        hour_key = f"track_click_rate:hour:{identifier}"
        hour_count = redis_manager.safe_incr(hour_key, ex=3600)
        
        if hour_count > TRACK_CLICK_CONFIG['rate_limit_per_hour']:
            logger.warning(f"Track click rate limit exceeded (hour) for {identifier}")
            return False, "Rate limit exceeded. Please try again later."
        
        return True, None


# =============================================================================
# SEARCH PARAMETER EXTRACTION
# =============================================================================

class SearchParams:
    """Extract and validate all search parameters from request."""
    
    def __init__(self, request):
        self.request = request
        
        # Core search - sanitized
        raw_query = request.GET.get('query', '') or request.GET.get('q', '')
        self.query = sanitize_query(raw_query)
        self.alt_mode = request.GET.get('alt_mode', 'y')
        
        # Validate alt_mode
        if self.alt_mode not in ('y', 'n'):
            self.alt_mode = 'y'
        
        # Security
        self.session_id = self._get_session_id()
        self.request_id = sanitize_filter_value(request.GET.get('request_id', ''))[:64]
        self.timestamp = request.GET.get('timestamp', '')[:20]
        self.nonce = request.GET.get('nonce', '')[:64]
        
        # Analytics
        self.source = sanitize_filter_value(request.GET.get('source', 'unknown'))[:50]
        self.device_type = sanitize_filter_value(request.GET.get('device_type', 'unknown'))[:20]
        self.result_count = request.GET.get('result_count', '0')[:10]
        self.typing_time_ms = request.GET.get('typing_time_ms', '0')[:10]
        
        # Rate limiting
        self.client_fp = request.GET.get('client_fp', '')[:64]
        self.request_sequence = request.GET.get('req_seq', '0')[:10]
    
    def _get_session_id(self) -> str:
        """Get or create session ID."""
        session_id = self.request.GET.get('session_id', '')
        
        if session_id and 20 <= len(session_id) <= 50:
            # Validate format (alphanumeric + hyphens)
            if re.match(r'^[a-zA-Z0-9-]+$', session_id):
                return session_id
        
        # Try to get from session
        try:
            if hasattr(self.request, 'session'):
                session_id = self.request.session.get('search_session_id')
                if not session_id:
                    session_id = str(uuid.uuid4())
                    self.request.session['search_session_id'] = session_id
                return session_id
        except Exception:
            pass
        
        return str(uuid.uuid4())
    
    @property
    def is_keyword_search(self) -> bool:
        return self.alt_mode == 'n'
    
    @property
    def is_semantic_search(self) -> bool:
        return self.alt_mode == 'y'
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'query': self.query,
            'alt_mode': self.alt_mode,
            'session_id': self.session_id[:8] + '...' if self.session_id else '',
            'source': self.source,
            'device_type': self.device_type,
        }


# =============================================================================
# TYPESENSE SEARCH HELPERS
# =============================================================================

def typesense_search(
    query: str,
    filter_by: str = '',
    sort_by: str = 'authority_score:desc',
    facet_by: str = '',
    per_page: int = 20,
    page: int = 1,
    query_by: str = 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
    collection: str = COLLECTION_NAME
) -> Optional[Dict[str, Any]]:
    """
    Execute Typesense search with error handling.
    
    Args:
        query: Search query
        filter_by: Filter string
        sort_by: Sort string
        facet_by: Facet fields
        per_page: Results per page
        page: Page number
        query_by: Fields to search
        collection: Collection name
        
    Returns:
        Search results dict or None on error
    """
    search_params = {
        'q': query or '*',
        'query_by': query_by,
        'per_page': min(per_page, SEARCH_CONFIG['max_per_page']),
        'page': min(page, SEARCH_CONFIG['max_page']),
    }
    
    if filter_by:
        search_params['filter_by'] = filter_by
    
    if sort_by:
        search_params['sort_by'] = sort_by
    
    if facet_by:
        search_params['facet_by'] = facet_by
        search_params['max_facet_values'] = 20
    
    return typesense_manager.search(collection, search_params)


def parse_facets(results: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse facet counts from Typesense results.
    
    Args:
        results: Typesense search results
        
    Returns:
        Dict with category and brand facets
    """
    facets: Dict[str, List[Dict[str, Any]]] = {'category': [], 'brand': []}
    
    if not results:
        return facets
    
    for facet in results.get('facet_counts', []):
        field = facet.get('field_name', '')
        counts = facet.get('counts', [])
        
        if field == 'document_category':
            facets['category'] = [
                {'value': c.get('value', ''), 'count': c.get('count', 0)}
                for c in counts if c.get('value')
            ]
        elif field == 'document_brand':
            facets['brand'] = [
                {'value': c.get('value', ''), 'count': c.get('count', 0)}
                for c in counts if c.get('value')
            ]
    
    return facets


def build_filter_string(filters: List[str]) -> str:
    """
    Build Typesense filter string from list of filters.
    
    Args:
        filters: List of filter strings
        
    Returns:
        Combined filter string
    """
    valid_filters = [f for f in filters if f and f.strip()]
    return ' && '.join(valid_filters)


def safe_get_hits(results: Optional[Dict[str, Any]], key: str = 'hits') -> List[Dict[str, Any]]:
    """Safely extract hits from results."""
    if not results:
        return []
    return results.get(key, [])


def safe_get_total(results: Optional[Dict[str, Any]]) -> int:
    """Safely extract total count from results."""
    if not results:
        return 0
    return results.get('found', 0)


# =============================================================================
# ANALYTICS & LOGGING
# =============================================================================

def log_search_analytics(
    params: SearchParams,
    search_type: str,
    result_count: int,
    is_suspicious: bool = False,
    error: Optional[str] = None
) -> None:
    """
    Log search for analytics and monitoring.
    
    Args:
        params: Search parameters
        search_type: Type of search (keyword/semantic)
        result_count: Number of results
        is_suspicious: Whether request was flagged
        error: Error message if any
    """
    try:
        log_data = {
            'query': params.query[:100] if params.query else '',
            'type': search_type,
            'alt_mode': params.alt_mode,
            'results': result_count,
            'session': params.session_id[:8] if params.session_id else 'none',
            'device': params.device_type,
            'source': params.source,
            'suspicious': is_suspicious,
        }
        
        if error:
            log_data['error'] = error[:100]
        
        logger.info(f"Search: {json.dumps(log_data)}")
        
    except Exception as e:
        logger.error(f"Analytics logging error: {e}")


# =============================================================================
# VIEW: HOME
# =============================================================================

def home(request):
    """Home page view."""
    city = get_user_city(request)
    
    context = {
        'city': city,
        'supported_cities': list(SUPPORTED_CITIES),
    }
    
    return render(request, 'home3.html', context)


# =============================================================================
# VIEW: SEARCH SUGGESTIONS (AUTOCOMPLETE)
# =============================================================================

@require_GET
def search_suggestions(request):
    """API endpoint for autocomplete dropdown suggestions."""
    query = sanitize_query(request.GET.get('q', ''))
    
    if not query or len(query) < 2:
        return JsonResponse({'suggestions': []})
    
    if get_autocomplete is None:
        logger.error("get_autocomplete function not available")
        return JsonResponse({'suggestions': [], 'error': 'Service unavailable'})
    
    try:
        results = get_autocomplete(prefix=query, limit=8)
        
        suggestions = [
            {
                'text': item.get('term', ''),
                'display': item.get('display', ''),
                'description': item.get('description', ''),
            }
            for item in results
            if item.get('term')
        ]
        
        return JsonResponse({'suggestions': suggestions})
    
    except Exception as e:
        logger.error(f"Autocomplete error: {e}")
        return JsonResponse({'suggestions': [], 'error': 'Service temporarily unavailable'})


# =============================================================================
# VIEW: MAIN SEARCH
# =============================================================================

def search(request):
    """
    Main search endpoint with security validation and analytics tracking.
    Routes to keyword or semantic search based on alt_mode.
    """
    
    # === 1. EXTRACT & VALIDATE PARAMETERS ===
    params = SearchParams(request)
    page = validate_page(request.GET.get('page', 1))
    per_page = validate_per_page(request.GET.get('per_page', 20))
    
    # Generate request_id if not provided
    request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
    # Get user info for analytics
    user_id = None
    if hasattr(request, 'user') and request.user.is_authenticated:
        user_id = str(request.user.id)
    
    # Get full client info (IP, location, device) using geolocation module
    client_info = get_full_client_info(request)
    client_ip = client_info.get('ip', '')
    location = client_info.get('location') or {}
    device = client_info.get('device') or {}
    user_agent = client_info.get('user_agent', '')
    
    # Extract device details for analytics
    device_type = device.get('device_type', params.device_type or 'unknown')
    browser = device.get('browser', 'Unknown')
    browser_version = device.get('browser_version', '')
    os_name = device.get('os', 'Unknown')
    os_version = device.get('os_version', '')
    is_mobile = device.get('is_mobile', False)
    is_bot = device.get('is_bot', False)
    
    # === 2. START/UPDATE SESSION (Analytics) ===
    analytics = get_analytics()
    if analytics:
        try:
            analytics.start_session(
                session_id=params.session_id,
                user_id=user_id,
                device_type=device_type,
                user_agent=user_agent,
                ip_address=client_ip,
                location=location,
                referrer=request.META.get('HTTP_REFERER'),
                browser=browser,
                browser_version=browser_version,
                os_name=os_name,
                os_version=os_version,
                is_mobile=is_mobile,
                is_bot=is_bot
            )
        except Exception as e:
            logger.warning(f"Analytics start_session error: {e}")
    
    # === 3. EXTRACT FILTERS ===
    source_filter = request.GET.get('source')
    if source_filter in ('home', 'results_page', 'header', None, ''):
        source_filter = None
    
    filters = {
        'category': sanitize_filter_value(request.GET.get('category', '')),
        'source': sanitize_filter_value(source_filter) if source_filter else None,
        'data_type': sanitize_filter_value(request.GET.get('type', '')),
        'time_range': sanitize_filter_value(request.GET.get('time', '')),
        'location': sanitize_filter_value(request.GET.get('location', '')),
        'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
    }
    filters = {k: v for k, v in filters.items() if v}
    
    safe_search = request.GET.get('safe', 'on') == 'on'
    
    # User location coordinates from request parameters
    user_location = None
    try:
        user_lat = request.GET.get('lat')
        user_lng = request.GET.get('lng')
        if user_lat and user_lng:
            lat = float(user_lat)
            lng = float(user_lng)
            if -90 <= lat <= 90 and -180 <= lng <= 180:
                user_location = (lat, lng)
    except (TypeError, ValueError):
        pass
    
    # If no coordinates from params, use geolocation (only if valid location data)
    if not user_location and location:
        loc_lat = location.get('lat')
        loc_lng = location.get('lng')
        # Only use geolocation if we have valid coordinates (not defaults/fallbacks)
        if loc_lat and loc_lng:
            try:
                lat = float(loc_lat)
                lng = float(loc_lng)
                # Verify it's not a default/fallback location (0,0 or similar)
                if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
                    user_location = (lat, lng)
            except (TypeError, ValueError):
                pass
    
    # === 4. SECURITY VALIDATION ===
    validator = SearchSecurityValidator()
    is_suspicious = False
    
    is_valid, error = validator.validate_timestamp(params.timestamp)
    if not is_valid:
        logger.warning(f"Timestamp validation failed: {error}")
    
    is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
    if not is_allowed:
        logger.warning(f"Rate limit exceeded: {params.session_id}")
        return render(request, 'results2.html', {
            'query': params.query,
            'results': [],
            'has_results': False,
            'error': 'Too many requests. Please wait a moment and try again.',
            'session_id': params.session_id,
        })
    
    is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
    if is_suspicious:
        logger.info(f"Suspicious request detected: {reason}")
    
    # Also flag if geolocation detected bot
    if is_bot:
        is_suspicious = True
        logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
    # === 5. EMPTY QUERY ===
    if not params.query:
        return render(request, 'results2.html', {
            'query': '',
            'results': [],
            'has_results': False,
            'session_id': params.session_id,
            'show_trending': True,
        })
    
    # === 6. CHECK CACHE ===
    cache_key = get_cache_key('search', params.query, page, params.alt_mode, json.dumps(filters, sort_keys=True))
    cached_result = safe_cache_get(cache_key)
    
    if cached_result and not filters:
        cached_result['from_cache'] = True
        
        # Track search even for cached results
        if analytics:
            try:
                # Safely get intent type from cached result
                cached_intent = cached_result.get('intent')
                intent_type = None
                if isinstance(cached_intent, dict):
                    intent_type = cached_intent.get('type')
                elif isinstance(cached_intent, str):
                    intent_type = cached_intent
                
                analytics.track_search(
                    session_id=params.session_id,
                    query=params.query,
                    results_count=cached_result.get('total_results', 0),
                    alt_mode=params.alt_mode,
                    user_id=user_id,
                    location=location,
                    device_type=device_type,
                    search_time_ms=0,  # Cached, so instant
                    search_strategy='cached',
                    corrected_query=cached_result.get('corrected_query'),
                    filters_applied=filters,
                    page=page,
                    intent=intent_type,
                    request_id=request_id,
                    browser=browser,
                    os_name=os_name,
                    is_mobile=is_mobile,
                    is_bot=is_bot
                )
            except Exception as e:
                logger.warning(f"Analytics track_search error (cached): {e}")
        
        return render(request, 'results2.html', cached_result)
    
    # === 7. ROUTE BASED ON ALT_MODE ===
    search_start_time = time.time()
    
    if params.is_keyword_search:
        search_type = 'keyword'
        corrected_query = params.query
        was_corrected = False
        word_corrections = []
        corrections = {}
        tuple_array = []
        intent = {}
        
        if detect_query_intent:
            intent = detect_query_intent(corrected_query, tuple_array)
    else:
        search_type = 'semantic'
        
        if word_discovery_multi:
            try:
                corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
                was_corrected = params.query.lower() != corrected_query.lower()
                word_corrections = build_word_corrections(params.query, corrected_query)
            except Exception as e:
                logger.error(f"Word discovery error: {e}")
                corrected_query = params.query
                was_corrected = False
                word_corrections = []
                corrections = {}
                tuple_array = []
        else:
            corrected_query = params.query
            was_corrected = False
            word_corrections = []
            corrections = {}
            tuple_array = []
        
        intent = {}
        if detect_query_intent:
            intent = detect_query_intent(corrected_query, tuple_array)
    
    # === 8. EXECUTE SEARCH ===
    results = []
    total_results = 0
    search_time = 0
    search_strategy = search_type
    
    if execute_full_search:
        try:
            result = execute_full_search(
                query=corrected_query,
                session_id=params.session_id,
                filters=filters,
                page=page,
                per_page=per_page,
                user_location=user_location,
                pos_tags=tuple_array if params.is_semantic_search else [],
                safe_search=safe_search
            )
            
            results = result.get('results', [])
            total_results = result.get('total', 0)
            search_time = result.get('search_time', 0)
            search_strategy = result.get('search_strategy', search_type)
        except Exception as e:
            logger.error(f"Search execution error: {e}")
    
    # Calculate total search time in milliseconds
    search_time_ms = (time.time() - search_start_time) * 1000
    
    # === 9. TRACK SEARCH (Analytics) ===
    if analytics:
        try:
            # Safely get intent type (might be string or dict)
            intent_type = None
            if isinstance(intent, dict):
                intent_type = intent.get('type')
            elif isinstance(intent, str):
                intent_type = intent
            
            analytics.track_search(
                session_id=params.session_id,
                query=params.query,
                results_count=total_results,
                alt_mode=params.alt_mode,
                user_id=user_id,
                location=location,
                device_type=device_type,
                search_time_ms=search_time_ms,
                search_strategy=search_strategy,
                corrected_query=corrected_query if was_corrected else None,
                filters_applied=filters if filters else None,
                page=page,
                intent=intent_type,
                request_id=request_id,
                browser=browser,
                os_name=os_name,
                is_mobile=is_mobile,
                is_bot=is_bot
            )
        except Exception as e:
            logger.warning(f"Analytics track_search error: {e}")
    
    # === 10. ZERO RESULTS HANDLING ===
    suggestions = []
    if not results:
        suggestions = handle_zero_results(params.query, corrected_query, filters)
    
    # === 11. GET SUPPLEMENTARY DATA ===
    facets = {}
    related_searches = []
    featured = None
    
    if results:
        if get_facets:
            try:
                facets = get_facets(corrected_query)
            except Exception:
                pass
        
        if get_related_searches:
            try:
                related_searches = get_related_searches(corrected_query, intent)
            except Exception:
                pass
        
        if page == 1 and get_featured_result:
            try:
                featured = get_featured_result(corrected_query, intent, results)
            except Exception:
                pass
    
    # === 12. CATEGORIZE & PAGINATE ===
    categorized_results = categorize_results(results)
    pagination = build_pagination(page, per_page, total_results)
    
    # === 13. LOG EVENTS ===
    if log_search_event:
        try:
            log_search_event(
                query=params.query,
                corrected_query=corrected_query,
                session_id=params.session_id,
                intent=intent,
                total_results=total_results,
                filters=filters,
                page=page
            )
        except Exception as e:
            logger.warning(f"Search event logging error: {e}")
    
    log_search_analytics(params, search_type, total_results, is_suspicious)
    
    # === 14. BUILD CONTEXT ===
    context = {
        'query': params.query,
        'corrected_query': corrected_query,
        'was_corrected': was_corrected,
        'word_corrections': word_corrections,
        'corrections': corrections,
        'intent': intent,
        'search_type': search_type,
        'alt_mode': params.alt_mode,
        'results': results,
        'categorized_results': categorized_results,
        'total_results': total_results,
        'has_results': len(results) > 0,
        'featured': featured,
        'related_searches': related_searches,
        'filters': filters,
        'facets': facets,
        'safe_search': safe_search,
        'pagination': pagination,
        'page': page,
        'per_page': per_page,
        'suggestions': suggestions,
        'session_id': params.session_id,
        'request_id': request_id,
        'search_time': search_time,
        'search_time_ms': search_time_ms,
        'from_cache': False,
        'device_type': device_type,
        'source': params.source,
        # Add location to context for template use (only if valid)
        'user_city': location.get('city', '') if location else '',
        'user_country': location.get('country', '') if location else '',
    }
    
    # === 15. CACHE RESULTS ===
    if not filters and total_results > 0:
        safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
    return render(request, 'results2.html', context)

# =============================================================================
# VIEW: CATEGORY ROUTER
# =============================================================================

def category_view(request, category_slug: str):
    """
    Generic category view router.
    
    Args:
        request: Django request
        category_slug: URL slug for category
        
    Returns:
        Rendered category page
    """
    # Validate and normalize category slug
    category_slug = str(category_slug).lower().strip()
    
    if not category_slug or category_slug not in SCHEMA_MAP:
        raise Http404("Category not found")
    
    city = get_user_city(request)
    
    # Route to specific handlers
    handlers = {
        'business': business_category,
        'culture': lambda r: culture_category(r, city),
        'health': lambda r: health_category(r, city),
        'news': lambda r: news_category(r, city),
        'community': community_category,
    }
    
    handler = handlers.get(category_slug)
    if handler:
        return handler(request)
    
    # Generic fallback
    return generic_category_view(request, category_slug, city)


def generic_category_view(request, category_slug: str, city: str):
    """Generic category page with faceted search."""
    
    schema = SCHEMA_MAP.get(category_slug)
    if not schema:
        raise Http404("Category not found")
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'relevance'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    # Build filters
    filters = [f'document_schema:={schema}']
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    
    # Sort mapping
    sort_options = {
        'authority': 'authority_score:desc',
        'recent': 'created_at:desc',
        'relevance': '_text_match:desc',
    }
    sort_by = sort_options.get(sort, 'authority_score:desc')
    
    # Execute search
    results = typesense_search(
        query=query or '*',
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    hits = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    context = {
        'city': city,
        'category_slug': category_slug,
        'category_name': category_slug.title(),
        'query': query,
        'results': hits,
        'total': total,
        'facets': facets,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'active_filter_count': sum([bool(selected_category), bool(selected_brand)]),
        'page': page,
        'has_more': (page * 20) < total,
        'sort': sort,
    }
    
    template = TEMPLATE_MAP.get(category_slug, 'category_generic.html')
    return render(request, template, context)


# =============================================================================
# VIEW: BUSINESS CATEGORY
# =============================================================================

def business_category(request):
    """Business directory with faceted search."""
    
    # Get city with validation
    city = get_user_city(request)
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_subcategory = sanitize_filter_value(request.GET.get('subcategory', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    # Validate subcategory
    if selected_subcategory and selected_subcategory not in SUBCATEGORY_MAP:
        selected_subcategory = ''
    
    # Build filters
    filters = ['document_schema:=business']
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    
    # Build search query
    search_query = query if query else '*'
    
    if selected_subcategory and selected_subcategory in SUBCATEGORY_MAP:
        subcategory_terms = ' '.join(SUBCATEGORY_MAP[selected_subcategory])
        if search_query == '*':
            search_query = subcategory_terms
        else:
            search_query = f"{search_query} {subcategory_terms}"
    
    if search_query == '*':
        search_query = city
    
    # Sort mapping
    sort_options = {
        'authority': 'authority_score:desc',
        'recent': 'created_at:desc',
        'name': 'document_title:asc',
    }
    sort_by = sort_options.get(sort, 'authority_score:desc')
    
    # Execute search
    results = typesense_search(
        query=search_query,
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    browse_results = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    # Stats
    stats = {
        'business_count': total,
        'categories_count': len(facets.get('category', [])) or 24,
        'verified_pct': 78,
    }
    
    # Trending searches (could be from Redis in production)
    trending_searches = [
        'black barber near me', 'soul food', 'black accountant',
        'african restaurant', 'natural hair salon', 'black owned clothing',
    ]
    
    active_filter_count = sum([
        bool(selected_subcategory),
        bool(selected_category),
        bool(selected_brand),
    ])
    
    context = {
        'city': city,
        'supported_cities': list(SUPPORTED_CITIES),
        'query': query,
        'selected_subcategory': selected_subcategory,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'results': browse_results,
        'total': total,
        'facets': facets,
        'stats': stats,
        'trending_searches': trending_searches,
        'active_filter_count': active_filter_count,
        'page': page,
        'has_more': (page * 20) < total,
        'sort': sort,
    }
    
    return render(request, 'category_business.html', context)


# =============================================================================
# VIEW: CULTURE CATEGORY
# =============================================================================

def culture_category(request, city: str):
    """Culture & heritage page with faceted search."""
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_topic = sanitize_filter_value(request.GET.get('topic', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    today = date.today()
    
    # Build filters
    filters = ['document_schema:=culture']
    
    # Topic to schema mapping
    valid_topics = {'hbcus', 'film', 'music', 'art', 'literature', 'food', 'fashion', 'history', 'theater', 'events'}
    if selected_topic and selected_topic not in valid_topics:
        selected_topic = ''
    
    if selected_topic == 'hbcus':
        filters = ['document_schema:=education']
    elif selected_topic == 'film':
        filters = ['(document_schema:=culture || document_schema:=media)']
    
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    sort_by = 'authority_score:desc' if sort == 'authority' else 'created_at:desc'
    
    # Main search
    results = typesense_search(
        query=query or '*',
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=18,
        page=page,
    )
    
    browse_results = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    # HBCU spotlight
    hbcu_results = typesense_search(
        query='university college',
        filter_by='document_schema:=education',
        sort_by='authority_score:desc',
        per_page=4,
    )
    hbcus = safe_get_hits(hbcu_results)
    
    # Featured article
    featured_results = typesense_search(
        query='*',
        filter_by='document_schema:=culture && document_category:=history',
        sort_by='authority_score:desc',
        per_page=1,
    )
    
    featured_article = None
    featured_hits = safe_get_hits(featured_results)
    if featured_hits:
        doc = featured_hits[0].get('document', {})
        featured_article = {
            'title': doc.get('document_title'),
            'excerpt': (doc.get('document_summary', '') or '')[:200],
            'url': doc.get('document_url'),
            'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
        }
    
    active_filter_count = sum([
        bool(selected_topic),
        bool(selected_category),
        bool(selected_brand),
    ])
    
    context = {
        'city': city,
        'query': query,
        'today': {'day': today.day, 'month': today.strftime('%b').upper()},
        'featured_article': featured_article,
        'hbcus': hbcus,
        'results': browse_results,
        'total': total,
        'facets': facets,
        'selected_topic': selected_topic,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'active_filter_count': active_filter_count,
        'page': page,
        'has_more': (page * 18) < total,
        'sort': sort,
    }
    
    return render(request, 'category_culture.html', context)


# =============================================================================
# VIEW: HEALTH CATEGORY
# =============================================================================

def health_category(request, city: str):
    """Health resources page with faceted search."""
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    # Build filters
    filters = ['document_schema:=health']
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
    # Execute search
    results = typesense_search(
        query=query or '*',
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    providers = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
    context = {
        'city': city,
        'query': query,
        'providers': providers,
        'results': providers,
        'total': total,
        'facets': facets,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'active_filter_count': active_filter_count,
        'page': page,
        'has_more': (page * 20) < total,
        'sort': sort,
    }
    
    return render(request, 'category_health.html', context)


# =============================================================================
# VIEW: NEWS CATEGORY
# =============================================================================

def news_category(request, city: str = DEFAULT_CITY):
    """News & media page with faceted search."""
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_section = sanitize_filter_value(request.GET.get('section', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', 'recent'), ['authority', 'recent'], 'recent')
    page = validate_page(request.GET.get('page', 1))
    
    # Section to category mapping
    section_category_map = {
        'local': None,
        'national': 'national',
        'politics': 'politics',
        'business': 'business',
        'sports': 'sports',
        'entertainment': 'entertainment',
        'opinion': 'opinion',
    }
    
    # Validate section
    if selected_section and selected_section not in section_category_map:
        selected_section = ''
    
    # Build filters
    filters = ['document_schema:=news']
    
    if selected_section and selected_section in section_category_map:
        section_cat = section_category_map[selected_section]
        if section_cat:
            filters.append(f'document_category:={section_cat}')
    
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
    # Main search
    results = typesense_search(
        query=query or '*',
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    browse_results = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    # Top story
    top_results = typesense_search(
        query='*',
        filter_by='document_schema:=news',
        sort_by='authority_score:desc',
        per_page=1,
    )
    top_hits = safe_get_hits(top_results)
    top_story = top_hits[0].get('document') if top_hits else None
    
    # Sidebar stories
    sidebar_results = typesense_search(
        query='*',
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=4,
    )
    sidebar_hits = safe_get_hits(sidebar_results)
    sidebar_stories = [hit.get('document', {}) for hit in sidebar_hits[1:] if hit.get('document')]
    
    # Local news
    local_results = typesense_search(
        query=city,
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=5,
    )
    local_news = [hit.get('document', {}) for hit in safe_get_hits(local_results) if hit.get('document')]
    
    # Good news
    good_results = typesense_search(
        query='success achievement award grant scholarship wins first',
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=5,
    )
    good_news = [hit.get('document', {}) for hit in safe_get_hits(good_results) if hit.get('document')]
    
    # Opinions
    opinion_results = typesense_search(
        query='*',
        filter_by='document_schema:=news && document_category:=opinion',
        sort_by='created_at:desc',
        per_page=3,
    )
    opinions = [hit.get('document', {}) for hit in safe_get_hits(opinion_results) if hit.get('document')]
    
    news_sources = [f['value'] for f in facets.get('brand', [])[:12]]
    active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
    context = {
        'city': city,
        'query': query,
        'top_story': top_story,
        'sidebar_stories': sidebar_stories,
        'results': browse_results,
        'total': total,
        'facets': facets,
        'local_news': local_news,
        'good_news': good_news,
        'opinions': opinions,
        'news_sources': news_sources,
        'selected_section': selected_section,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'active_filter_count': active_filter_count,
        'page': page,
        'has_more': (page * 20) < total,
        'sort': sort,
    }
    
    return render(request, 'category_news.html', context)


# =============================================================================
# VIEW: COMMUNITY CATEGORY
# =============================================================================

def community_category(request):
    """Community hub with faceted search and dynamic city."""
    
    # Get city
    city = get_user_city(request)
    
    # Get and validate parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    # Category mapping
    help_category_map = {
        'housing': 'housing',
        'legal': 'legal',
        'jobs': 'employment',
        'food': 'food',
        'family': 'family',
        'youth': 'youth',
        'faith': 'faith',
        'nonprofit': 'nonprofit',
        'social_justice': 'social_justice',
    }
    
    # Build filters
    filters = ['document_schema:=community']
    if selected_category:
        mapped = help_category_map.get(selected_category, selected_category)
        filters.append(f'document_category:={mapped}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    
    sort_options = {
        'authority': 'authority_score:desc',
        'recent': 'created_at:desc',
        'name': 'document_title:asc',
    }
    sort_by = sort_options.get(sort, 'authority_score:desc')
    
    # Main search
    search_query = query if query else city
    results = typesense_search(
        query=search_query,
        filter_by=filter_by,
        sort_by=sort_by,
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    browse_results = safe_get_hits(results)
    total = safe_get_total(results)
    facets = parse_facets(results)
    
    # Events
    event_results = typesense_search(
        query=f'{city} event meeting workshop conference',
        filter_by='document_schema:=community',
        sort_by='created_at:desc',
        per_page=5,
    )
    
    upcoming_events = []
    today = date.today()
    for i, hit in enumerate(safe_get_hits(event_results)):
        event = dict(hit)
        future_date = today + timedelta(days=i * 5 + 3)
        event['month'] = future_date.strftime('%b').upper()
        event['day'] = future_date.day
        event['time'] = '10 AM - 2 PM'
        upcoming_events.append(event)
    
    # Churches
    church_results = typesense_search(
        query=f'{city} church baptist AME methodist faith ministry',
        filter_by='document_schema:=community',
        sort_by='authority_score:desc',
        per_page=8,
    )
    churches = safe_get_hits(church_results)
    
    # Organizations
    org_results = typesense_search(
        query=f'{city} NAACP Urban League organization nonprofit foundation',
        filter_by='document_schema:=community',
        sort_by='authority_score:desc',
        per_page=4,
    )
    
    organizations = []
    for hit in safe_get_hits(org_results):
        org = dict(hit)
        title = org.get('document', {}).get('document_title', '')
        words = title.split()[:2]
        org['initials'] = ''.join([w[0] for w in words if w]).upper()[:3]
        organizations.append(org)
    
    # Volunteer opportunities
    vol_results = typesense_search(
        query=f'{city} volunteer service help mentor tutor',
        filter_by='document_schema:=community',
        sort_by='created_at:desc',
        per_page=5,
    )
    
    volunteer_ops = []
    schedules = ['Weekly', 'Ongoing', 'Flexible', 'One-time', 'Monthly']
    for i, hit in enumerate(safe_get_hits(vol_results)):
        vol = dict(hit)
        vol['schedule'] = 'Flexible'
        vol['type'] = schedules[i % len(schedules)]
        volunteer_ops.append(vol)
    
    active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
    context = {
        'city': city,
        'supported_cities': list(SUPPORTED_CITIES),
        'query': query,
        'results': browse_results,
        'total': total,
        'facets': facets,
        'upcoming_events': upcoming_events,
        'churches': churches,
        'organizations': organizations,
        'volunteer_ops': volunteer_ops,
        'selected_category': selected_category,
        'selected_brand': selected_brand,
        'active_filter_count': active_filter_count,
        'page': page,
        'has_more': (page * 20) < total,
        'sort': sort,
    }
    
    return render(request, 'category_community.html', context)


# =============================================================================
# API ENDPOINTS
# =============================================================================

@require_GET
def business_search_api(request):
    """AJAX API for business search."""
    
    city = sanitize_filter_value(request.GET.get('city', DEFAULT_CITY))
    if city not in SUPPORTED_CITIES:
        city = DEFAULT_CITY
    
    query = sanitize_query(request.GET.get('q', ''))
    subcategory = sanitize_filter_value(request.GET.get('subcategory', ''))
    category = sanitize_filter_value(request.GET.get('category', ''))
    brand = sanitize_filter_value(request.GET.get('brand', ''))
    page = validate_page(request.GET.get('page', 1))
    
    # Validate subcategory
    if subcategory and subcategory not in SUBCATEGORY_MAP:
        subcategory = ''
    
    # Build search query
    search_query = query if query else '*'
    if subcategory and subcategory in SUBCATEGORY_MAP:
        terms = ' '.join(SUBCATEGORY_MAP[subcategory])
        search_query = f"{search_query} {terms}" if search_query != '*' else terms
    if search_query == '*':
        search_query = city
    
    # Build filters
    filters = ['document_schema:=business']
    if category:
        filters.append(f'document_category:={category}')
    if brand:
        filters.append(f'document_brand:={brand}')
    
    results = typesense_search(
        query=search_query,
        filter_by=build_filter_string(filters),
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    if not results:
        return JsonResponse({'success': False, 'error': 'Search service unavailable'}, status=503)
    
    hits = []
    for hit in safe_get_hits(results):
        doc = hit.get('document', {})
        hits.append({
            'id': doc.get('id'),
            'title': doc.get('document_title'),
            'summary': (doc.get('document_summary') or '')[:150],
            'url': doc.get('document_url'),
            'category': doc.get('document_category'),
            'brand': doc.get('document_brand'),
            'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
            'keywords': doc.get('primary_keywords', [])[:3],
        })
    
    return JsonResponse({
        'success': True,
        'hits': hits,
        'total': safe_get_total(results),
        'facets': parse_facets(results),
        'page': page,
    })


@require_GET
def community_search_api(request):
    """AJAX API for community search."""
    
    city = sanitize_filter_value(request.GET.get('city', DEFAULT_CITY))
    if city not in SUPPORTED_CITIES:
        city = DEFAULT_CITY
    
    query = sanitize_query(request.GET.get('q', ''))
    category = sanitize_filter_value(request.GET.get('category', ''))
    brand = sanitize_filter_value(request.GET.get('brand', ''))
    page = validate_page(request.GET.get('page', 1))
    
    filters = ['document_schema:=community']
    if category:
        filters.append(f'document_category:={category}')
    if brand:
        filters.append(f'document_brand:={brand}')
    
    results = typesense_search(
        query=query if query else city,
        filter_by=build_filter_string(filters),
        facet_by='document_category,document_brand',
        per_page=20,
        page=page,
    )
    
    if not results:
        return JsonResponse({'success': False, 'error': 'Search service unavailable'}, status=503)
    
    hits = []
    for hit in safe_get_hits(results):
        doc = hit.get('document', {})
        hits.append({
            'id': doc.get('id'),
            'title': doc.get('document_title'),
            'summary': (doc.get('document_summary') or '')[:150],
            'url': doc.get('document_url'),
            'category': doc.get('document_category'),
            'brand': doc.get('document_brand'),
            'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
        })
    
    return JsonResponse({
        'success': True,
        'hits': hits,
        'total': safe_get_total(results),
        'facets': parse_facets(results),
        'page': page,
    })


# =============================================================================
# DETAIL VIEWS
# =============================================================================

def business_detail(request, business_id: str):
    """Individual business detail page."""
    
    # Validate ID format
    if not business_id or len(business_id) > 100:
        raise Http404("Invalid business ID")
    
    # Sanitize ID - allow alphanumeric, hyphens, underscores
    business_id = re.sub(r'[^a-zA-Z0-9_-]', '', business_id)
    
    if not business_id:
        raise Http404("Invalid business ID")
    
    doc = typesense_manager.get_document(COLLECTION_NAME, business_id)
    
    if not doc:
        raise Http404("Business not found")
    
    # Get related businesses
    category = doc.get('document_category', '')
    related_results = typesense_search(
        query=category,
        filter_by=f"document_schema:=business && id:!={business_id}",
        per_page=4,
        query_by='document_category,keywords',
    )
    related = [hit.get('document', {}) for hit in safe_get_hits(related_results) if hit.get('document')]
    
    return render(request, 'business_detail.html', {
        'business': doc,
        'related': related,
    })


def community_by_city(request, city_slug: str):
    """Community page by city slug URL."""
    
    city_map = {
        'atlanta': 'Atlanta',
        'houston': 'Houston',
        'chicago': 'Chicago',
        'detroit': 'Detroit',
        'new-york': 'New York',
        'los-angeles': 'Los Angeles',
        'philadelphia': 'Philadelphia',
        'washington-dc': 'Washington',
    }
    
    city_slug = str(city_slug).lower().strip()
    city = city_map.get(city_slug, DEFAULT_CITY)
    
    try:
        request.session['user_city'] = city
    except Exception:
        pass
    
    return community_category(request)


# =============================================================================
# LEGACY ENDPOINTS
# =============================================================================

@require_GET
def form_submit(request):
    """Legacy form submit endpoint."""
    query = sanitize_query(request.GET.get('query', ''))
    session_id = request.GET.get('session_id', '')[:50]
    
    if not query:
        return JsonResponse({'error': 'No query provided'}, status=400)
    
    if process_search_submission is None:
        return JsonResponse({'error': 'Service unavailable'}, status=503)
    
    try:
        result = process_search_submission(query, session_id)
        return JsonResponse(result)
    except Exception as e:
        logger.error(f"Form submit error: {e}")
        return JsonResponse({'error': 'Search failed'}, status=500)


@require_GET
def search_api(request):
    """JSON API endpoint for programmatic access."""
    query = sanitize_query(request.GET.get('q', '') or request.GET.get('query', ''))
    session_id = request.GET.get('session_id', '')[:50]
    
    if not query:
        return JsonResponse({'error': 'No query provided'}, status=400)
    
    if word_discovery_multi is None or process_search_submission is None:
        return JsonResponse({'error': 'Service unavailable'}, status=503)
    
    try:
        corrections, tuple_array, corrected_query = word_discovery_multi(query)
        result = process_search_submission(corrected_query, session_id)
        
        result['word_discovery'] = {
            'original_query': query,
            'corrected_query': corrected_query,
            'corrections': corrections,
            'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
            'was_corrected': query.lower() != corrected_query.lower()
        }
        
        return JsonResponse(result)
    except Exception as e:
        logger.error(f"Search API error: {e}")
        return JsonResponse({'error': 'Search failed'}, status=500)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_word_corrections(original: str, corrected: str) -> List[Dict[str, Any]]:
    """Build word-by-word correction display."""
    word_corrections = []
    original_words = original.lower().split()
    corrected_words = corrected.lower().split()
    
    for i, orig_word in enumerate(original_words):
        corr_word = corrected_words[i] if i < len(corrected_words) else orig_word
        word_corrections.append({
            'original': orig_word,
            'corrected': corr_word,
            'was_changed': orig_word != corr_word
        })
    
    return word_corrections


def handle_zero_results(original_query: str, corrected_query: str, filters: Dict) -> List[Dict[str, Any]]:
    """Provide helpful suggestions when no results found."""
    suggestions = []
    
    if filters:
        suggestions.append({
            'type': 'remove_filters',
            'message': 'Try removing some filters',
            'action_query': corrected_query,
            'action_filters': {}
        })
    
    words = corrected_query.split()
    if len(words) > 2:
        suggestions.append({
            'type': 'broader_search',
            'message': 'Try a broader search',
            'action_query': ' '.join(words[:2])
        })
    
    if original_query.lower() != corrected_query.lower():
        suggestions.append({
            'type': 'try_original',
            'message': f'Search for "{escape(original_query)}" instead',
            'action_query': original_query
        })
    
    suggestions.append({
        'type': 'help',
        'message': 'Check your spelling or try different keywords'
    })
    
    return suggestions


def categorize_results(results: List[Dict]) -> Dict[str, List[Dict]]:
    """Group results by type for different display treatments."""
    categorized: Dict[str, List[Dict]] = {
        'articles': [],
        'videos': [],
        'products': [],
        'people': [],
        'places': [],
        'services': [],
        'other': []
    }
    
    type_mapping = {
        'article': 'articles',
        'video': 'videos',
        'product': 'products',
        'person': 'people',
        'place': 'places',
        'service': 'services'
    }
    
    for result in results:
        data_type = result.get('data_type', 'other')
        category = type_mapping.get(data_type, 'other')
        categorized[category].append(result)
    
    return {k: v for k, v in categorized.items() if v}


def build_pagination(page: int, per_page: int, total: int) -> Dict[str, Any]:
    """Build pagination info for template."""
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    
    start_page = max(1, page - 2)
    end_page = min(total_pages, page + 2)
    
    return {
        'current_page': page,
        'total_pages': total_pages,
        'has_previous': page > 1,
        'has_next': page < total_pages,
        'previous_page': max(1, page - 1),
        'next_page': min(total_pages, page + 1),
        'page_range': list(range(start_page, end_page + 1)),
        'show_first': start_page > 1,
        'show_last': end_page < total_pages,
        'start_result': min((page - 1) * per_page + 1, total) if total > 0 else 0,
        'end_result': min(page * per_page, total),
        'total_results': total
    }


def add_time_ago(documents: List[Dict]) -> List[Dict]:
    """Add time_ago field to documents for display."""
    now = datetime.now(timezone.utc)
    
    for doc in documents:
        created_at = doc.get('created_at')
        if created_at:
            try:
                if isinstance(created_at, str):
                    created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                else:
                    created = created_at
                
                diff = now - created
                
                if diff.days > 30:
                    doc['time_ago'] = f"{diff.days // 30}mo ago"
                elif diff.days > 0:
                    doc['time_ago'] = f"{diff.days}d ago"
                elif diff.seconds >= 3600:
                    doc['time_ago'] = f"{diff.seconds // 3600}h ago"
                elif diff.seconds >= 60:
                    doc['time_ago'] = f"{diff.seconds // 60}m ago"
                else:
                    doc['time_ago'] = "Just now"
            except Exception:
                doc['time_ago'] = "Today"
        else:
            doc['time_ago'] = "Today"
    
    return documents


# =============================================================================
# UTILITY FUNCTIONS FOR OTHER MODULES
# =============================================================================

def get_featured_businesses(city: str = DEFAULT_CITY, limit: int = 6) -> List[Dict]:
    """Get featured businesses for homepage."""
    results = typesense_search(
        query=city,
        filter_by='document_schema:=business',
        sort_by='authority_score:desc',
        per_page=limit,
    )
    return [hit.get('document', {}) for hit in safe_get_hits(results) if hit.get('document')]


def get_business_categories() -> List[Dict[str, Any]]:
    """Get all unique business categories with counts."""
    results = typesense_search(
        query='*',
        filter_by='document_schema:=business',
        facet_by='document_category',
        per_page=0,
    )
    
    if not results:
        return []
    
    for facet in results.get('facet_counts', []):
        if facet['field_name'] == 'document_category':
            return [
                {'name': c['value'], 'count': c['count']}
                for c in facet['counts']
            ]
    
    return []


def get_trending_searches(category: str, city: str) -> List[str]:
    """Get trending searches from Redis."""
    cache_key = f"trending:{category}:{city.lower()}"
    results = redis_manager.safe_zrevrange(cache_key, 0, 7)
    return list(results) if results else []


def get_featured_listings(category: str, city: str) -> List[Dict]:
    """Get featured/promoted listings from Redis cache."""
    cache_key = f"featured:{category}:{city.lower()}"
    cached = redis_manager.safe_get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except (json.JSONDecodeError, TypeError):
            pass
    return []


def get_category_stats(category: str, city: str) -> Dict[str, str]:
    """Get category statistics from Redis cache."""
    cache_key = f"stats:{category}:{city.lower()}"
    return redis_manager.safe_hgetall(cache_key)


# =============================================================================
# VIEW: TRACK CLICK (Analytics) - Production Ready
# =============================================================================

@csrf_exempt
@require_http_methods(["POST", "GET"])
def track_click(request):
    """
    Track when a user clicks on a search result or other events.
    
    Can be called via:
    - POST with JSON body
    - GET with query parameters (for simple tracking pixel/redirect)
    
    Handles both click events and generic events (page_dwell, related_search_click, etc.)
    
    Includes:
    - Rate limiting per session/IP
    - Input size validation
    - Proper error handling without debug output
    """
    
    # Extract parameters from POST or GET
    if request.method == 'POST':
        try:
            # Validate body size before parsing
            content_length = request.META.get('CONTENT_LENGTH', 0)
            try:
                content_length = int(content_length)
            except (TypeError, ValueError):
                content_length = 0
            
            if content_length > TRACK_CLICK_CONFIG['max_event_data_size']:
                return JsonResponse(
                    {'success': False, 'error': 'Request body too large'},
                    status=413
                )
            
            data = json.loads(request.body)
        except (json.JSONDecodeError, ValueError):
            data = request.POST.dict()
    else:
        data = request.GET.dict()
    
    # Get client IP for rate limiting
    client_ip = get_client_ip(request)
    
    # Extract and validate session_id early for rate limiting
    session_id = sanitize_string(
        data.get('session_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_session_id_length']
    )
    
    # === RATE LIMITING ===
    is_allowed, rate_error = TrackClickRateLimiter.check_rate_limit(session_id, client_ip)
    if not is_allowed:
        return JsonResponse(
            {'success': False, 'error': rate_error},
            status=429
        )
    
    # Check if this is a generic event (not a click)
    event_type = sanitize_string(data.get('event_type', ''), max_length=50)
    
    if event_type and event_type != 'click':
        # Handle generic events (page_dwell, related_search_click, trending_click, pagination_click, etc.)
        analytics = get_analytics()
        if analytics:
            try:
                if session_id:
                    # Get location for event
                    location = get_location_from_request(request)
                    
                    # Get user_id if logged in
                    user_id = None
                    if hasattr(request, 'user') and request.user.is_authenticated:
                        user_id = str(request.user.id)
                    
                    # Sanitize event data - limit size
                    sanitized_data = {}
                    for key, value in data.items():
                        if isinstance(value, str):
                            sanitized_data[key] = value[:500]  # Limit string values
                        elif isinstance(value, (int, float, bool)):
                            sanitized_data[key] = value
                        elif isinstance(value, dict):
                            # Limit nested dicts
                            sanitized_data[key] = {
                                k[:100]: str(v)[:500] 
                                for k, v in list(value.items())[:20]
                            }
                    
                    analytics.track_event(
                        session_id=session_id,
                        event_type=event_type,
                        event_data=sanitized_data,
                        user_id=user_id,
                        location=location
                    )
                return JsonResponse({'success': True})
            except Exception as e:
                logger.error(f"Event tracking error: {e}")
                return JsonResponse(
                    {'success': False, 'error': 'Event tracking failed'},
                    status=500
                )
        return JsonResponse({'success': True})  # Silently succeed if no analytics
    
    # === CLICK TRACKING ===
    
    # Validate and sanitize all input fields with size limits
    clicked_url = sanitize_url(
        data.get('url', ''),
        max_length=TRACK_CLICK_CONFIG['max_url_length']
    )
    query = sanitize_string(
        data.get('query', ''),
        max_length=TRACK_CLICK_CONFIG['max_query_length']
    )
    
    # Validate required fields
    if not session_id:
        return JsonResponse(
            {'success': False, 'error': 'Missing session_id'},
            status=400
        )
    
    if not clicked_url:
        return JsonResponse(
            {'success': False, 'error': 'Missing or invalid URL'},
            status=400
        )
    
    # Sanitize optional fields with size limits
    clicked_position = sanitize_int(data.get('position', 0), default=0, min_val=0, max_val=1000)
    result_id = sanitize_string(
        data.get('result_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_result_id_length']
    )
    result_title = sanitize_string(
        data.get('title', ''),
        max_length=TRACK_CLICK_CONFIG['max_title_length']
    )
    result_source = sanitize_string(
        data.get('source', ''),
        max_length=TRACK_CLICK_CONFIG['max_source_length']
    )
    search_request_id = sanitize_string(
        data.get('request_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_request_id_length']
    )
    
    # Results count from original search
    results_count = sanitize_int(data.get('results_count', 0), default=0, min_val=0, max_val=10000)
    
    # Correction info
    was_corrected = str(data.get('was_corrected', 'false')).lower() == 'true'
    corrected_query = sanitize_string(
        data.get('corrected_query', ''),
        max_length=TRACK_CLICK_CONFIG['max_corrected_query_length']
    )
    
    # Time to click - validate bounds
    time_to_click_ms = None
    raw_time = data.get('time_to_click_ms')
    if raw_time is not None:
        time_to_click_ms = sanitize_int(raw_time, default=0, min_val=0, max_val=3600000)  # Max 1 hour
        if time_to_click_ms == 0:
            time_to_click_ms = None
    
    # User info
    user_id = None
    if hasattr(request, 'user') and request.user.is_authenticated:
        user_id = str(request.user.id)
    
    # Location
    location = get_location_from_request(request)
    
    # Track the click
    analytics = get_analytics()
    if analytics:
        try:
            analytics.track_click(
                session_id=session_id,
                query=query,
                clicked_url=clicked_url,
                clicked_position=clicked_position,
                result_id=result_id,
                result_title=result_title,
                result_source=result_source,
                user_id=user_id,
                time_to_click_ms=time_to_click_ms,
                location=location,
                search_request_id=search_request_id,
                results_count=results_count,
                was_corrected=was_corrected,
                corrected_query=corrected_query
            )
            
            return JsonResponse({'success': True})
        except Exception as e:
            logger.error(f"Click tracking error: {e}")
            return JsonResponse(
                {'success': False, 'error': 'Tracking failed'},
                status=500
            )
    
    return JsonResponse(
        {'success': False, 'error': 'Analytics not available'},
        status=503
    )


@require_GET
def click_redirect(request):
    """
    Redirect-based click tracking.
    
    Usage: /click/?url=https://example.com&session_id=abc&query=test&position=1
    
    Tracks the click then redirects user to the destination URL.
    """
    
    destination_url = sanitize_url(
        request.GET.get('url', ''),
        max_length=TRACK_CLICK_CONFIG['max_url_length']
    )
    
    if not destination_url:
        return HttpResponseBadRequest('Missing or invalid URL parameter')
    
    # Get client IP for rate limiting
    client_ip = get_client_ip(request)
    
    # Track the click (reuse track_click logic)
    session_id = sanitize_string(
        request.GET.get('session_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_session_id_length']
    )
    
    # Rate limit check (but don't block redirect - just skip tracking)
    is_allowed, _ = TrackClickRateLimiter.check_rate_limit(session_id, client_ip)
    
    if is_allowed:
        query = sanitize_string(
            request.GET.get('query', ''),
            max_length=TRACK_CLICK_CONFIG['max_query_length']
        )
        clicked_position = sanitize_int(request.GET.get('position', 0), default=0, min_val=0, max_val=1000)
        result_id = sanitize_string(
            request.GET.get('result_id', ''),
            max_length=TRACK_CLICK_CONFIG['max_result_id_length']
        )
        result_title = sanitize_string(
            request.GET.get('title', ''),
            max_length=TRACK_CLICK_CONFIG['max_title_length']
        )
        result_source = sanitize_string(
            request.GET.get('source', ''),
            max_length=TRACK_CLICK_CONFIG['max_source_length']
        )
        search_request_id = sanitize_string(
            request.GET.get('request_id', ''),
            max_length=TRACK_CLICK_CONFIG['max_request_id_length']
        )
        
        results_count = sanitize_int(request.GET.get('results_count', 0), default=0, min_val=0, max_val=10000)
        was_corrected = request.GET.get('was_corrected', 'false').lower() == 'true'
        corrected_query = sanitize_string(
            request.GET.get('corrected_query', ''),
            max_length=TRACK_CLICK_CONFIG['max_corrected_query_length']
        )
        
        user_id = None
        if hasattr(request, 'user') and request.user.is_authenticated:
            user_id = str(request.user.id)
        
        location = get_location_from_request(request)
        
        analytics = get_analytics()
        if analytics and session_id:
            try:
                analytics.track_click(
                    session_id=session_id,
                    query=query,
                    clicked_url=destination_url,
                    clicked_position=clicked_position,
                    result_id=result_id,
                    result_title=result_title,
                    result_source=result_source,
                    user_id=user_id,
                    time_to_click_ms=None,
                    location=location,
                    search_request_id=search_request_id,
                    results_count=results_count,
                    was_corrected=was_corrected,
                    corrected_query=corrected_query
                )
            except Exception as e:
                logger.warning(f"Click redirect tracking error: {e}")
    
    return redirect(destination_url)