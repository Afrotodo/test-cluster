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
# from .cached_embedding_related_search import get_query_cache

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

# Import user_agents for device parsing
try:
    from user_agents import parse as parse_user_agent
except ImportError:
    parse_user_agent = None


# =============================================================================
# SAFE WRAPPER FUNCTIONS FOR GEOLOCATION
# =============================================================================
# These handle the case where geolocation functions expect different arguments

def _safe_get_device_info(request):
    """
    Safely get device info from request.
    
    Handles the case where get_device_info() expects a user agent STRING
    instead of the request object.
    """
    user_agent = request.META.get('HTTP_USER_AGENT', '')
    
    try:
        # Try passing user agent string (what geolocation.py expects)
        result = get_device_info(user_agent)
        if isinstance(result, dict):
            return result
    except Exception as e:
        pass  # Fall through to manual parsing
    
    # Fallback: return basic device info manually parsed
    ua_lower = user_agent.lower()
    
    is_mobile = any(x in ua_lower for x in ['mobile', 'android', 'iphone', 'ipad'])
    is_bot = any(x in ua_lower for x in ['bot', 'crawler', 'spider', 'scraper'])
    
    # Detect browser
    browser = 'Unknown'
    if 'edg/' in ua_lower or 'edge/' in ua_lower:
        browser = 'Edge'
    elif 'chrome' in ua_lower:
        browser = 'Chrome'
    elif 'firefox' in ua_lower:
        browser = 'Firefox'
    elif 'safari' in ua_lower:
        browser = 'Safari'
    
    # Detect OS
    os_name = 'Unknown'
    if 'windows' in ua_lower:
        os_name = 'Windows'
    elif 'mac os' in ua_lower or 'macos' in ua_lower:
        os_name = 'macOS'
    elif 'linux' in ua_lower:
        os_name = 'Linux'
    elif 'android' in ua_lower:
        os_name = 'Android'
    elif 'iphone' in ua_lower or 'ipad' in ua_lower:
        os_name = 'iOS'
    
    return {
        'device_type': 'mobile' if is_mobile else 'desktop',
        'user_agent': user_agent[:500],
        'browser': browser,
        'browser_version': '',
        'os_name': os_name,
        'os_version': '',
        'is_mobile': is_mobile,
        'is_bot': is_bot,
    }


def _safe_get_client_ip(request):
    """Safely get client IP - won't crash if geolocation module fails."""
    try:
        return get_client_ip(request)
    except Exception:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0].strip()
        return request.META.get('REMOTE_ADDR', '')


def _safe_get_location(request):
    """Safely get location - returns empty dict if anything fails."""
    try:
        result = get_location_from_request(request)
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    return {}


def _safe_get_session_id(request):
    """Safely get or create session ID."""
    try:
        if not request.session.session_key:
            request.session.create()
        return request.session.session_key
    except Exception:
        return str(uuid.uuid4())


# =============================================================================
# DYNAMIC TAB LABEL MAPPINGS
# =============================================================================

# User-friendly labels for document_data_type values (used for tabs)
DATA_TYPE_LABELS: Dict[str, str] = {
    'article': 'Articles',
    'person': 'People',
    'business': 'Businesses',
    'place': 'Places',
    'media': 'Media',
    'event': 'Events',
    'product': 'Products',
}

# User-friendly labels for document_category values
CATEGORY_LABELS: Dict[str, str] = {
    'healthcare_medical': 'Healthcare',
    'fashion': 'Fashion',
    'beauty': 'Beauty',
    'food_recipes': 'Food & Recipes',
    'travel_tourism': 'Travel',
    'entertainment': 'Entertainment',
    'business': 'Business',
    'education': 'Education',
    'technology': 'Technology',
    'sports': 'Sports',
    'finance': 'Finance',
    'real_estate': 'Real Estate',
    'automotive': 'Automotive',
    'lifestyle': 'Lifestyle',
    'news': 'News',
    'culture': 'Culture',
    'politics': 'Politics',
    'science': 'Science',
    'general': 'General',
}

# Icons for data types (SVG paths for template use)
DATA_TYPE_ICONS: Dict[str, str] = {
    'article': 'M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z',
    'person': 'M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z',
    'business': 'M12 7V3H2v18h20V7H12zM6 19H4v-2h2v2zm0-4H4v-2h2v2zm0-4H4V9h2v2zm0-4H4V5h2v2zm4 12H8v-2h2v2zm0-4H8v-2h2v2zm0-4H8V9h2v2zm0-4H8V5h2v2zm10 12h-8v-2h2v-2h-2v-2h2v-2h-2V9h8v10zm-2-8h-2v2h2v-2zm0 4h-2v2h2v-2z',
    'place': 'M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z',
    'media': 'M18 4l2 4h-3l-2-4h-2l2 4h-3l-2-4H8l2 4H7L5 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4h-4zM4 18V8h16v10H4z',
    'event': 'M17 12h-5v5h5v-5zM16 1v2H8V1H6v2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2h-1V1h-2zm3 18H5V8h14v11z',
    'product': 'M18 6h-2c0-2.21-1.79-4-4-4S8 3.79 8 6H6c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-6-2c1.1 0 2 .9 2 2h-4c0-1.1.9-2 2-2zm6 16H6V8h2v2c0 .55.45 1 1 1s1-.45 1-1V8h4v2c0 .55.45 1 1 1s1-.45 1-1V8h2v12z',
}


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
    """
    global _analytics_client, _analytics_last_attempt, _analytics_retry_count
    
    if _analytics_client is not None:
        return _analytics_client
    
    if not ANALYTICS_AVAILABLE:
        return None
    
    if _analytics_retry_count >= _analytics_max_retries:
        extended_interval = 300  # 5 minutes
        if time.time() - _analytics_last_attempt < extended_interval:
            return None
        _analytics_retry_count = 0
    
    current_time = time.time()
    backoff_interval = _analytics_retry_interval * (2 ** _analytics_retry_count)
    
    if current_time - _analytics_last_attempt < backoff_interval:
        return None
    
    _analytics_last_attempt = current_time
    
    try:
        _analytics_client = SearchAnalytics()
        _analytics_retry_count = 0
        logger.info("SearchAnalytics initialized successfully")
        return _analytics_client
    except redis.ConnectionError as e:
        _analytics_retry_count += 1
        logger.warning(f"SearchAnalytics Redis connection failed (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
    except redis.TimeoutError as e:
        _analytics_retry_count += 1
        logger.warning(f"SearchAnalytics Redis timeout (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
    except Exception as e:
        _analytics_retry_count += 1
        logger.warning(f"Failed to initialize SearchAnalytics (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
    
    return None


def reset_analytics_client():
    """Reset the analytics client for testing or recovery purposes."""
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

SUPPORTED_CITIES: frozenset = frozenset([
    'Atlanta', 'Houston', 'Chicago', 'Detroit',
    'New York', 'Los Angeles', 'Philadelphia', 'Washington',
    'Baltimore', 'Charlotte', 'Memphis', 'New Orleans',
    'Oakland', 'Miami', 'Dallas', 'St. Louis'
])

DEFAULT_CITY: str = 'Atlanta'

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

TRACK_CLICK_CONFIG: Dict[str, Any] = {
    'rate_limit_per_minute': 120,
    'rate_limit_per_hour': 1000,
    'max_url_length': 2000,
    'max_title_length': 500,
    'max_query_length': 500,
    'max_source_length': 200,
    'max_result_id_length': 100,
    'max_session_id_length': 50,
    'max_request_id_length': 100,
    'max_corrected_query_length': 500,
    'max_event_data_size': 10000,
}


# =============================================================================
# INPUT VALIDATION & SANITIZATION
# =============================================================================

def sanitize_query(query: Any) -> str:
    """Sanitize search query to prevent injection and handle edge cases."""
    if query is None:
        return ''
    
    try:
        query = str(query)
    except (TypeError, ValueError):
        return ''
    
    query = query.strip()
    
    if len(query) > SEARCH_CONFIG['max_query_length']:
        query = query[:SEARCH_CONFIG['max_query_length']]
    
    query = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', query)
    query = re.sub(r'\s+', ' ', query)
    query = re.sub(r'[<>{}|\[\]\\^`]', '', query)
    
    return query


def sanitize_filter_value(value: Any) -> str:
    """Sanitize filter values to prevent injection."""
    if value is None:
        return ''
    
    try:
        value = str(value).strip()
    except (TypeError, ValueError):
        return ''
    
    value = value[:200]
    value = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', value)
    
    return value


def sanitize_url(url: Any, max_length: int = 2000) -> str:
    """Sanitize URL input."""
    if url is None:
        return ''
    
    try:
        url = str(url).strip()
    except (TypeError, ValueError):
        return ''
    
    if len(url) > max_length:
        url = url[:max_length]
    
    url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url)
    
    if url and not url.startswith(('http://', 'https://')):
        return ''
    
    return url


def sanitize_string(value: Any, max_length: int = 500) -> str:
    """General string sanitization."""
    if value is None:
        return ''
    
    try:
        value = str(value).strip()
    except (TypeError, ValueError):
        return ''
    
    if len(value) > max_length:
        value = value[:max_length]
    
    value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
    
    return value


def sanitize_int(value: Any, default: int = 0, min_val: int = None, max_val: int = None) -> int:
    """Sanitize integer input with bounds checking."""
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
    """Validate and sanitize page number."""
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
    """Validate and sanitize per_page parameter."""
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
    """Validate sort parameter against allowed values."""
    if not sort:
        return default
    
    sort = str(sort).lower().strip()
    
    if sort in allowed:
        return sort
    
    return default


def validate_data_type(data_type: Any) -> str:
    """Validate data_type filter against allowed values."""
    if not data_type:
        return ''
    
    data_type = str(data_type).lower().strip()
    
    # Valid data types (7 categories)
    valid_types = {'article', 'person', 'business', 'place', 'media', 'event', 'product'}
    
    if data_type in valid_types:
        return data_type
    
    return ''


def validate_schema(schema: Any) -> str:
    """Validate document_schema filter against allowed values."""
    if not schema:
        return ''
    
    schema = str(schema).strip()
    
    # Valid schema types (Schema.org)
    valid_schemas = {
        'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
        'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
        'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
        'AudioObject', 'Book', 'Movie', 'MusicRecording'
    }
    
    if schema in valid_schemas:
        return schema
    
    return ''


def get_user_city(request, default: str = DEFAULT_CITY) -> str:
    """Get user's city from various sources with validation."""
    city = ''
    
    city = request.GET.get('city', '')
    
    if not city:
        try:
            city = request.session.get('user_city', '')
        except Exception:
            pass
    
    if city:
        city = city.strip()
        city_lower = city.lower()
        if city_lower in CITY_ALIASES:
            city = CITY_ALIASES[city_lower]
        else:
            city = city.title()
    
    if city not in SUPPORTED_CITIES:
        city = default
    
    try:
        request.session['user_city'] = city
    except Exception:
        pass
    
    return city


# =============================================================================
# CACHING UTILITIES
# =============================================================================

def get_cache_key(*args, prefix: str = 'afrotodo') -> str:
    """Generate a consistent cache key from arguments."""
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
        """Validate timestamp is recent (prevent replay attacks)."""
        if not timestamp_str:
            return True, None
        
        try:
            timestamp = int(timestamp_str)
            current_time = int(time.time() * 1000)
            age_seconds = (current_time - timestamp) / 1000
            
            if age_seconds < -60:
                return False, "Timestamp is in the future"
            
            if age_seconds > SEARCH_CONFIG['max_timestamp_age_seconds']:
                return False, "Request too old"
            
            return True, None
            
        except (ValueError, TypeError):
            return True, None
    
    @staticmethod
    def validate_nonce(nonce: Optional[str], session_id: str) -> Tuple[bool, Optional[str]]:
        """Validate nonce hasn't been used (prevent replay)."""
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
        """Check if request is within rate limits."""
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
            return True, None
    
    @staticmethod
    def detect_bot(typing_time_ms: Optional[str], request_sequence: Optional[str]) -> Tuple[bool, Optional[str]]:
        """Simple bot detection heuristics."""
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
        """Check if click tracking request is within rate limits."""
        if not session_id:
            if not client_ip:
                return True, None
            identifier = f"ip:{client_ip}"
        else:
            identifier = f"session:{session_id}"
        
        minute_key = f"track_click_rate:minute:{identifier}"
        minute_count = redis_manager.safe_incr(minute_key, ex=60)
        
        if minute_count > TRACK_CLICK_CONFIG['rate_limit_per_minute']:
            logger.warning(f"Track click rate limit exceeded (minute) for {identifier}")
            return False, "Rate limit exceeded. Please slow down."
        
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
        
        raw_query = request.GET.get('query', '') or request.GET.get('q', '')
        self.query = sanitize_query(raw_query)
        self.alt_mode = request.GET.get('alt_mode', 'y')
        
        if self.alt_mode not in ('y', 'n'):
            self.alt_mode = 'y'
        
        self.session_id = self._get_session_id()
        self.request_id = sanitize_filter_value(request.GET.get('request_id', ''))[:64]
        self.timestamp = request.GET.get('timestamp', '')[:20]
        self.nonce = request.GET.get('nonce', '')[:64]
        
        self.source = sanitize_filter_value(request.GET.get('source', 'unknown'))[:50]
        self.device_type = sanitize_filter_value(request.GET.get('device_type', 'unknown'))[:20]
        self.result_count = request.GET.get('result_count', '0')[:10]
        self.typing_time_ms = request.GET.get('typing_time_ms', '0')[:10]
        
        self.client_fp = request.GET.get('client_fp', '')[:64]
        self.request_sequence = request.GET.get('req_seq', '0')[:10]
    
    def _get_session_id(self) -> str:
        """Get or create session ID."""
        session_id = self.request.GET.get('session_id', '')
        
        if session_id and 20 <= len(session_id) <= 50:
            if re.match(r'^[a-zA-Z0-9-]+$', session_id):
                return session_id
        
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
    """Execute Typesense search with error handling."""
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
    
    Returns dict with:
    - category: document_category facets with labels
    - brand: document_brand facets
    - data_type: document_data_type facets with labels (for dynamic tabs)
    - schema: document_schema facets
    """
    facets: Dict[str, List[Dict[str, Any]]] = {
        'category': [],
        'brand': [],
        'data_type': [],
        'schema': [],
    }
    
    if not results:
        return facets
    
    for facet in results.get('facet_counts', []):
        field = facet.get('field_name', '')
        counts = facet.get('counts', [])
        
        if field == 'document_category':
            facets['category'] = [
                {
                    'value': c.get('value', ''),
                    'count': c.get('count', 0),
                    'label': CATEGORY_LABELS.get(c.get('value', ''), c.get('value', '').replace('_', ' ').title())
                }
                for c in counts if c.get('value') and c.get('count', 0) > 0
            ]
        
        elif field == 'document_brand':
            facets['brand'] = [
                {
                    'value': c.get('value', ''),
                    'count': c.get('count', 0),
                    'label': c.get('value', '')
                }
                for c in counts if c.get('value')
            ]
        
        elif field == 'document_data_type':
            facets['data_type'] = [
                {
                    'value': c.get('value', ''),
                    'count': c.get('count', 0),
                    'label': DATA_TYPE_LABELS.get(c.get('value', ''), c.get('value', '').title()),
                    'icon': DATA_TYPE_ICONS.get(c.get('value', ''), '')
                }
                for c in counts if c.get('value') and c.get('count', 0) > 0
            ]
        
        elif field == 'document_schema':
            facets['schema'] = [
                {
                    'value': c.get('value', ''),
                    'count': c.get('count', 0),
                    'label': c.get('value', '')
                }
                for c in counts if c.get('value') and c.get('count', 0) > 0
            ]
    
    return facets


def get_dynamic_tab_facets(query: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get facet counts for dynamic tabs without filtering.
    
    This runs a separate query to get the total counts across ALL data types,
    not limited by any current filter. This allows tabs to show accurate counts.
    """
    try:
        results = typesense_search(
            query=query,
            filter_by='',  # No filter - get all facets
            facet_by='document_data_type,document_category,document_schema',
            per_page=0,  # We only need facets, not results
            page=1,
        )
        return parse_facets(results)
    except Exception as e:
        logger.warning(f"Error getting tab facets: {e}")
        return {'data_type': [], 'category': [], 'schema': [], 'brand': []}


def build_filter_string(filters: List[str]) -> str:
    """Build Typesense filter string from list of filters."""
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
    """Log search for analytics and monitoring."""
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
# VIEW: MAIN SEARCH (WITH DYNAMIC TABS)
# =============================================================================

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)
#     page = validate_page(request.GET.get('page', 1))
#     # per_page = validate_per_page(request.GET.get('per_page', 20))
#     per_page = validate_per_page(request.GET.get('per_page', 10))
    
#     request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     client_info = get_full_client_info(request)
#     client_ip = client_info.get('ip', '')
#     location = client_info.get('location') or {}
#     device = client_info.get('device') or {}
#     user_agent = client_info.get('user_agent', '')
    
#     device_type = device.get('device_type', params.device_type or 'unknown')
#     browser = device.get('browser', 'Unknown')
#     browser_version = device.get('browser_version', '')
#     os_name = device.get('os', 'Unknown')
#     os_version = device.get('os_version', '')
#     is_mobile = device.get('is_mobile', False)
#     is_bot = device.get('is_bot', False)
    
#     # === 2. START/UPDATE SESSION (Analytics) ===
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=params.session_id,
#                 user_id=user_id,
#                 device_type=device_type,
#                 user_agent=user_agent,
#                 ip_address=client_ip,
#                 location=location,
#                 referrer=request.META.get('HTTP_REFERER'),
#                 browser=browser,
#                 browser_version=browser_version,
#                 os_name=os_name,
#                 os_version=os_version,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics start_session error: {e}")
    
#     # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None
    
#     # Dynamic tab filters
#     active_data_type = validate_data_type(request.GET.get('data_type', ''))
#     active_category = sanitize_filter_value(request.GET.get('category', ''))
#     active_schema = validate_schema(request.GET.get('schema', ''))
    
#     # Build filters dict
#     filters = {
#         'data_type': active_data_type,
#         'category': active_category,
#         'schema': active_schema,
#         'source': sanitize_filter_value(source_filter) if source_filter else None,
#         'time_range': sanitize_filter_value(request.GET.get('time', '')),
#         'location': sanitize_filter_value(request.GET.get('location', '')),
#         'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
#     }
#     # Remove empty filters
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location coordinates
#     user_location = None
#     try:
#         user_lat = request.GET.get('lat')
#         user_lng = request.GET.get('lng')
#         if user_lat and user_lng:
#             lat = float(user_lat)
#             lng = float(user_lng)
#             if -90 <= lat <= 90 and -180 <= lng <= 180:
#                 user_location = (lat, lng)
#     except (TypeError, ValueError):
#         pass
    
#     if not user_location and location:
#         loc_lat = location.get('lat')
#         loc_lng = location.get('lng')
#         if loc_lat and loc_lng:
#             try:
#                 lat = float(loc_lat)
#                 lng = float(loc_lng)
#                 if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
#                     user_location = (lat, lng)
#             except (TypeError, ValueError):
#                 pass
    
#     # === 4. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error}")
    
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment and try again.',
#             'session_id': params.session_id,
#         })
    
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request detected: {reason}")
    
#     if is_bot:
#         is_suspicious = True
#         logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
#     # === 5. EMPTY QUERY ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': '',
#             'active_category': '',
#             'active_schema': '',
#         })
    
#     # === 6. CHECK CACHE ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
        
#         if analytics:
#             try:
#                 cached_intent = cached_result.get('intent')
#                 intent_type = None
#                 if isinstance(cached_intent, dict):
#                     intent_type = cached_intent.get('type')
#                 elif isinstance(cached_intent, str):
#                     intent_type = cached_intent
                
#                 analytics.track_search(
#                     session_id=params.session_id,
#                     query=params.query,
#                     results_count=cached_result.get('total_results', 0),
#                     alt_mode=params.alt_mode,
#                     user_id=user_id,
#                     location=location,
#                     device_type=device_type,
#                     search_time_ms=0,
#                     search_strategy='cached',
#                     corrected_query=cached_result.get('corrected_query'),
#                     filters_applied=filters,
#                     page=page,
#                     intent=intent_type,
#                     request_id=request_id,
#                     browser=browser,
#                     os_name=os_name,
#                     is_mobile=is_mobile,
#                     is_bot=is_bot
#                 )
#             except Exception as e:
#                 logger.warning(f"Analytics track_search error (cached): {e}")
        
#         return render(request, 'results2.html', cached_result)
    
#     # === 7. ROUTE BASED ON ALT_MODE ===
#     search_start_time = time.time()
    
#     if params.is_keyword_search:
#         search_type = 'keyword'
#         corrected_query = params.query
#         was_corrected = False
#         word_corrections = []
#         corrections = {}
#         tuple_array = []
#         intent = {}
        
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
#     else:
#         search_type = 'semantic'
        
#         if word_discovery_multi:
#             try:
#                 corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#                 was_corrected = params.query.lower() != corrected_query.lower()
#                 word_corrections = build_word_corrections(params.query, corrected_query)
#             except Exception as e:
#                 logger.error(f"Word discovery error: {e}")
#                 corrected_query = params.query
#                 was_corrected = False
#                 word_corrections = []
#                 corrections = {}
#                 tuple_array = []
#         else:
#             corrected_query = params.query
#             was_corrected = False
#             word_corrections = []
#             corrections = {}
#             tuple_array = []
        
#         intent = {}
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
    
#     # === 8. EXECUTE SEARCH ===
#     results = []
#     total_results = 0
#     search_time = 0
#     search_strategy = search_type
    
#     if execute_full_search:
#         try:
#             result = execute_full_search(
#                 query=corrected_query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode, 
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search
#             )
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
#         except Exception as e:
#             logger.error(f"Search execution error: {e}")
    
#     search_time_ms = (time.time() - search_start_time) * 1000
    
#     # === 9. TRACK SEARCH (Analytics) ===
#     if analytics:
#         try:
#             intent_type = None
#             if isinstance(intent, dict):
#                 intent_type = intent.get('type')
#             elif isinstance(intent, str):
#                 intent_type = intent
            
#             analytics.track_search(
#                 session_id=params.session_id,
#                 query=params.query,
#                 results_count=total_results,
#                 alt_mode=params.alt_mode,
#                 user_id=user_id,
#                 location=location,
#                 device_type=device_type,
#                 search_time_ms=search_time_ms,
#                 search_strategy=search_strategy,
#                 corrected_query=corrected_query if was_corrected else None,
#                 filters_applied=filters if filters else None,
#                 page=page,
#                 intent=intent_type,
#                 request_id=request_id,
#                 browser=browser,
#                 os_name=os_name,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics track_search error: {e}")
    
#     # === 10. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(params.query, corrected_query, filters)
    
#     # === 11. GET DYNAMIC TAB FACETS (ALWAYS - even with zero results) ===
#     # This gets facet counts WITHOUT any filters applied
#     # So tabs show total counts across all data types
#     tab_facets = get_dynamic_tab_facets(corrected_query)
#     data_type_facets = tab_facets.get('data_type', [])
#     category_facets = tab_facets.get('category', [])
#     schema_facets = tab_facets.get('schema', [])
    
#     # === 12. GET SUPPLEMENTARY DATA ===
#     facet_total = sum(f.get('count', 0) for f in data_type_facets)
#     facets = {}
#     related_searches = []
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if get_related_searches:
#             try:
#                 related_searches = get_related_searches(corrected_query, intent)
#             except Exception:
#                 pass
        
#         if page == 1 and get_featured_result:
#             try:
#                 featured = get_featured_result(corrected_query, intent, results)
#             except Exception:
#                 pass
    
#     # === 13. CATEGORIZE & PAGINATE ===
#     categorized_results = categorize_results(results)
#     pagination = build_pagination(page, per_page, total_results)
    
#     # === 14. LOG EVENTS ===
#     if log_search_event:
#         try:
#             log_search_event(
#                 query=params.query,
#                 corrected_query=corrected_query,
#                 session_id=params.session_id,
#                 intent=intent,
#                 total_results=total_results,
#                 filters=filters,
#                 page=page
#             )
#         except Exception as e:
#             logger.warning(f"Search event logging error: {e}")
    
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
#         'search_type': search_type,
#         'alt_mode': params.alt_mode,
#         'facet_total': facet_total,

#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Dynamic tab facets (for generating tabs)
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
        
#         # Active filters (for highlighting active tab/filter)
#         'active_data_type': active_data_type,
#         'active_category': active_category,
#         'active_schema': active_schema,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Suggestions (zero results)
#         'suggestions': suggestions,
        
#         # Session & tracking
#         'session_id': params.session_id,
#         'request_id': request_id,
#         'search_time': search_time,
#         'search_time_ms': search_time_ms,
#         'from_cache': False,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings for template use
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
#     return render(request, 'results2.html', context)

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)
#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))
    
#     request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     client_info = get_full_client_info(request)
#     client_ip = client_info.get('ip', '')
#     location = client_info.get('location') or {}
#     device = client_info.get('device') or {}
#     user_agent = client_info.get('user_agent', '')
    
#     device_type = device.get('device_type', params.device_type or 'unknown')
#     browser = device.get('browser', 'Unknown')
#     browser_version = device.get('browser_version', '')
#     os_name = device.get('os', 'Unknown')
#     os_version = device.get('os_version', '')
#     is_mobile = device.get('is_mobile', False)
#     is_bot = device.get('is_bot', False)
    
#     # === 2. START/UPDATE SESSION (Analytics) ===
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=params.session_id,
#                 user_id=user_id,
#                 device_type=device_type,
#                 user_agent=user_agent,
#                 ip_address=client_ip,
#                 location=location,
#                 referrer=request.META.get('HTTP_REFERER'),
#                 browser=browser,
#                 browser_version=browser_version,
#                 os_name=os_name,
#                 os_version=os_version,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics start_session error: {e}")
    
#     # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None
    
#     # Dynamic tab filters
#     active_data_type = validate_data_type(request.GET.get('data_type', ''))
#     active_category = sanitize_filter_value(request.GET.get('category', ''))
#     active_schema = validate_schema(request.GET.get('schema', ''))
    
#     # Build filters dict
#     filters = {
#         'data_type': active_data_type,
#         'category': active_category,
#         'schema': active_schema,
#         'source': sanitize_filter_value(source_filter) if source_filter else None,
#         'time_range': sanitize_filter_value(request.GET.get('time', '')),
#         'location': sanitize_filter_value(request.GET.get('location', '')),
#         'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
#     }
#     # Remove empty filters
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location coordinates
#     user_location = None
#     try:
#         user_lat = request.GET.get('lat')
#         user_lng = request.GET.get('lng')
#         if user_lat and user_lng:
#             lat = float(user_lat)
#             lng = float(user_lng)
#             if -90 <= lat <= 90 and -180 <= lng <= 180:
#                 user_location = (lat, lng)
#     except (TypeError, ValueError):
#         pass
    
#     if not user_location and location:
#         loc_lat = location.get('lat')
#         loc_lng = location.get('lng')
#         if loc_lat and loc_lng:
#             try:
#                 lat = float(loc_lat)
#                 lng = float(loc_lng)
#                 if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
#                     user_location = (lat, lng)
#             except (TypeError, ValueError):
#                 pass
    
#     # === 4. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error}")
    
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment and try again.',
#             'session_id': params.session_id,
#         })
    
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request detected: {reason}")
    
#     if is_bot:
#         is_suspicious = True
#         logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
#     # === 5. EMPTY QUERY ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': '',
#             'active_category': '',
#             'active_schema': '',
#         })
    
#     # === 6. CHECK CACHE ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
        
#         if analytics:
#             try:
#                 cached_intent = cached_result.get('intent')
#                 intent_type = None
#                 if isinstance(cached_intent, dict):
#                     intent_type = cached_intent.get('type')
#                 elif isinstance(cached_intent, str):
#                     intent_type = cached_intent
                
#                 analytics.track_search(
#                     session_id=params.session_id,
#                     query=params.query,
#                     results_count=cached_result.get('total_results', 0),
#                     alt_mode=params.alt_mode,
#                     user_id=user_id,
#                     location=location,
#                     device_type=device_type,
#                     search_time_ms=0,
#                     search_strategy='cached',
#                     corrected_query=cached_result.get('corrected_query'),
#                     filters_applied=filters,
#                     page=page,
#                     intent=intent_type,
#                     request_id=request_id,
#                     browser=browser,
#                     os_name=os_name,
#                     is_mobile=is_mobile,
#                     is_bot=is_bot
#                 )
#             except Exception as e:
#                 logger.warning(f"Analytics track_search error (cached): {e}")
        
#         return render(request, 'results2.html', cached_result)
    
#     # === 7. ROUTE BASED ON ALT_MODE ===
#     search_start_time = time.time()
    
#     if params.is_keyword_search:
#         search_type = 'keyword'
#         corrected_query = params.query
#         was_corrected = False
#         word_corrections = []
#         corrections = {}
#         tuple_array = []
#         intent = {}
        
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
#     else:
#         search_type = 'semantic'
        
#         if word_discovery_multi:
#             try:
#                 corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#                 was_corrected = params.query.lower() != corrected_query.lower()
#                 word_corrections = build_word_corrections(params.query, corrected_query)
#             except Exception as e:
#                 logger.error(f"Word discovery error: {e}")
#                 corrected_query = params.query
#                 was_corrected = False
#                 word_corrections = []
#                 corrections = {}
#                 tuple_array = []
#         else:
#             corrected_query = params.query
#             was_corrected = False
#             word_corrections = []
#             corrections = {}
#             tuple_array = []
        
#         intent = {}
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
    
#     # === 8. EXECUTE SEARCH ===
#     results = []
#     total_results = 0
#     search_time = 0
#     search_strategy = search_type
    
#     if execute_full_search:
#         try:
#             result = execute_full_search(
#                 query=corrected_query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode, 
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search
#             )
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
#         except Exception as e:
#             logger.error(f"Search execution error: {e}")
    
#     search_time_ms = (time.time() - search_start_time) * 1000
    
#     # === 9. TRACK SEARCH (Analytics) ===
#     if analytics:
#         try:
#             intent_type = None
#             if isinstance(intent, dict):
#                 intent_type = intent.get('type')
#             elif isinstance(intent, str):
#                 intent_type = intent
            
#             analytics.track_search(
#                 session_id=params.session_id,
#                 query=params.query,
#                 results_count=total_results,
#                 alt_mode=params.alt_mode,
#                 user_id=user_id,
#                 location=location,
#                 device_type=device_type,
#                 search_time_ms=search_time_ms,
#                 search_strategy=search_strategy,
#                 corrected_query=corrected_query if was_corrected else None,
#                 filters_applied=filters if filters else None,
#                 page=page,
#                 intent=intent_type,
#                 request_id=request_id,
#                 browser=browser,
#                 os_name=os_name,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics track_search error: {e}")
    
#     # === 10. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(params.query, corrected_query, filters)
    
#     # === 11. GET DYNAMIC TAB FACETS (ALWAYS - even with zero results) ===
#     # This gets facet counts WITHOUT any filters applied
#     # So tabs show total counts across all data types
#     tab_facets = get_dynamic_tab_facets(corrected_query)
#     data_type_facets = tab_facets.get('data_type', [])
#     category_facets = tab_facets.get('category', [])
#     schema_facets = tab_facets.get('schema', [])
    
#     # === 11b. CALCULATE DISPLAY TOTALS ===
#     facet_total = sum(f.get('count', 0) for f in data_type_facets)
    
#     # Get the facet count for the active filter (if any)
#     active_filter_count = None
#     if active_data_type:
#         for f in data_type_facets:
#             if f.get('value') == active_data_type:
#                 active_filter_count = f.get('count', 0)
#                 break
    
#     # Determine what total to display and use for pagination
#     if active_filter_count is not None:
#         display_total = active_filter_count
#         pagination_total = active_filter_count
#     else:
#         display_total = facet_total
#         pagination_total = total_results
    
#     # === 12. GET SUPPLEMENTARY DATA ===
#     facets = {}
#     related_searches = []
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if get_related_searches:
#             try:
#                 related_searches = get_related_searches(corrected_query, intent)
#             except Exception:
#                 pass
        
#         if page == 1 and get_featured_result:
#             try:
#                 featured = get_featured_result(corrected_query, intent, results)
#             except Exception:
#                 pass
    
#     # === 13. CATEGORIZE & PAGINATE ===
#     categorized_results = categorize_results(results)
#     pagination = build_pagination(page, per_page, pagination_total)
    
#     # === 14. LOG EVENTS ===
#     if log_search_event:
#         try:
#             log_search_event(
#                 query=params.query,
#                 corrected_query=corrected_query,
#                 session_id=params.session_id,
#                 intent=intent,
#                 total_results=total_results,
#                 filters=filters,
#                 page=page
#             )
#         except Exception as e:
#             logger.warning(f"Search event logging error: {e}")
    
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
#         'search_type': search_type,
#         'alt_mode': params.alt_mode,
#         'facet_total': facet_total,
#         'display_total': display_total,
        
#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Dynamic tab facets (for generating tabs)
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
        
#         # Active filters (for highlighting active tab/filter)
#         'active_data_type': active_data_type,
#         'active_category': active_category,
#         'active_schema': active_schema,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Suggestions (zero results)
#         'suggestions': suggestions,
        
#         # Session & tracking
#         'session_id': params.session_id,
#         'request_id': request_id,
#         'search_time': search_time,
#         'search_time_ms': search_time_ms,
#         'from_cache': False,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings for template use
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
#     return render(request, 'results2.html', context)

# """
# views.py - search function (UPDATED v6.0)

# KEY CHANGES:
# 1. Uses facets returned by execute_full_search instead of separate get_dynamic_tab_facets call
# 2. Facets are now consistent with results (same query, same filtering)
# 3. Pagination total comes from search result, not facets
# 4. Removed section 11b logic that caused mismatch
# """

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)
#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))
    
#     request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     client_info = get_full_client_info(request)
#     client_ip = client_info.get('ip', '')
#     location = client_info.get('location') or {}
#     device = client_info.get('device') or {}
#     user_agent = client_info.get('user_agent', '')
    
#     device_type = device.get('device_type', params.device_type or 'unknown')
#     browser = device.get('browser', 'Unknown')
#     browser_version = device.get('browser_version', '')
#     os_name = device.get('os', 'Unknown')
#     os_version = device.get('os_version', '')
#     is_mobile = device.get('is_mobile', False)
#     is_bot = device.get('is_bot', False)
    
#     # === 2. START/UPDATE SESSION (Analytics) ===
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=params.session_id,
#                 user_id=user_id,
#                 device_type=device_type,
#                 user_agent=user_agent,
#                 ip_address=client_ip,
#                 location=location,
#                 referrer=request.META.get('HTTP_REFERER'),
#                 browser=browser,
#                 browser_version=browser_version,
#                 os_name=os_name,
#                 os_version=os_version,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics start_session error: {e}")
    
#     # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None
    
#     # Dynamic tab filters
#     active_data_type = validate_data_type(request.GET.get('data_type', ''))
#     active_category = sanitize_filter_value(request.GET.get('category', ''))
#     active_schema = validate_schema(request.GET.get('schema', ''))
    
#     # Build filters dict
#     filters = {
#         'data_type': active_data_type,
#         'category': active_category,
#         'schema': active_schema,
#         'source': sanitize_filter_value(source_filter) if source_filter else None,
#         'time_range': sanitize_filter_value(request.GET.get('time', '')),
#         'location': sanitize_filter_value(request.GET.get('location', '')),
#         'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
#     }
#     # Remove empty filters
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location coordinates
#     user_location = None
#     try:
#         user_lat = request.GET.get('lat')
#         user_lng = request.GET.get('lng')
#         if user_lat and user_lng:
#             lat = float(user_lat)
#             lng = float(user_lng)
#             if -90 <= lat <= 90 and -180 <= lng <= 180:
#                 user_location = (lat, lng)
#     except (TypeError, ValueError):
#         pass
    
#     if not user_location and location:
#         loc_lat = location.get('lat')
#         loc_lng = location.get('lng')
#         if loc_lat and loc_lng:
#             try:
#                 lat = float(loc_lat)
#                 lng = float(loc_lng)
#                 if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
#                     user_location = (lat, lng)
#             except (TypeError, ValueError):
#                 pass
    
#     # === 4. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error}")
    
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment and try again.',
#             'session_id': params.session_id,
#         })
    
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request detected: {reason}")
    
#     if is_bot:
#         is_suspicious = True
#         logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
#     # === 5. EMPTY QUERY ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': '',
#             'active_category': '',
#             'active_schema': '',
#         })
    
#     # === 6. CHECK CACHE ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
        
#         if analytics:
#             try:
#                 cached_intent = cached_result.get('intent')
#                 intent_type = None
#                 if isinstance(cached_intent, dict):
#                     intent_type = cached_intent.get('type')
#                 elif isinstance(cached_intent, str):
#                     intent_type = cached_intent
                
#                 analytics.track_search(
#                     session_id=params.session_id,
#                     query=params.query,
#                     results_count=cached_result.get('total_results', 0),
#                     alt_mode=params.alt_mode,
#                     user_id=user_id,
#                     location=location,
#                     device_type=device_type,
#                     search_time_ms=0,
#                     search_strategy='cached',
#                     corrected_query=cached_result.get('corrected_query'),
#                     filters_applied=filters,
#                     page=page,
#                     intent=intent_type,
#                     request_id=request_id,
#                     browser=browser,
#                     os_name=os_name,
#                     is_mobile=is_mobile,
#                     is_bot=is_bot
#                 )
#             except Exception as e:
#                 logger.warning(f"Analytics track_search error (cached): {e}")
        
#         return render(request, 'results2.html', cached_result)
    
#     # === 7. ROUTE BASED ON ALT_MODE ===
#     search_start_time = time.time()
    
#     if params.is_keyword_search:
#         search_type = 'keyword'
#         corrected_query = params.query
#         was_corrected = False
#         word_corrections = []
#         corrections = {}
#         tuple_array = []
#         intent = {}
        
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
#     else:
#         search_type = 'semantic'
        
#         if word_discovery_multi:
#             try:
#                 corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#                 was_corrected = params.query.lower() != corrected_query.lower()
#                 word_corrections = build_word_corrections(params.query, corrected_query)
#             except Exception as e:
#                 logger.error(f"Word discovery error: {e}")
#                 corrected_query = params.query
#                 was_corrected = False
#                 word_corrections = []
#                 corrections = {}
#                 tuple_array = []
#         else:
#             corrected_query = params.query
#             was_corrected = False
#             word_corrections = []
#             corrections = {}
#             tuple_array = []
        
#         intent = {}
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
    
#     # === 8. EXECUTE SEARCH ===
#     results = []
#     total_results = 0
#     search_time = 0
#     search_strategy = search_type
    
#     # NEW: Initialize facets (will be populated from search result)
#     data_type_facets = []
#     category_facets = []
#     schema_facets = []
    
#     if execute_full_search:
#         try:
#             result = execute_full_search(
#                 query=corrected_query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode, 
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search
#             )
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
            
#             # NEW: Get facets from the search result (same query, consistent counts)
#             data_type_facets = result.get('data_type_facets', [])
#             category_facets = result.get('category_facets', [])
#             schema_facets = result.get('schema_facets', [])
            
#         except Exception as e:
#             logger.error(f"Search execution error: {e}")
    
#     search_time_ms = (time.time() - search_start_time) * 1000
    
#     # === 9. TRACK SEARCH (Analytics) ===
#     if analytics:
#         try:
#             intent_type = None
#             if isinstance(intent, dict):
#                 intent_type = intent.get('type')
#             elif isinstance(intent, str):
#                 intent_type = intent
            
#             analytics.track_search(
#                 session_id=params.session_id,
#                 query=params.query,
#                 results_count=total_results,
#                 alt_mode=params.alt_mode,
#                 user_id=user_id,
#                 location=location,
#                 device_type=device_type,
#                 search_time_ms=search_time_ms,
#                 search_strategy=search_strategy,
#                 corrected_query=corrected_query if was_corrected else None,
#                 filters_applied=filters if filters else None,
#                 page=page,
#                 intent=intent_type,
#                 request_id=request_id,
#                 browser=browser,
#                 os_name=os_name,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics track_search error: {e}")
    
#     # === 10. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(params.query, corrected_query, filters)
    
#     # === 11. CALCULATE DISPLAY TOTALS (SIMPLIFIED) ===
#     # NEW: Facets now come from the search result, so they're consistent
#     # No need for separate facet query or complex total calculation
    
#     facet_total = sum(f.get('count', 0) for f in data_type_facets)
    
#     # Display total is simply the facet total (sum of all types)
#     # If no facets, fall back to total_results
#     display_total = facet_total if facet_total > 0 else total_results
    
#     # Pagination total is the actual result count from search
#     # This is now accurate because execute_full_search returns the correct total
#     pagination_total = total_results
    
#     # === 12. GET SUPPLEMENTARY DATA ===
#     facets = {}
#     related_searches = []
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if get_related_searches:
#             try:
#                 related_searches = get_related_searches(corrected_query, intent)
#             except Exception:
#                 pass
        
#         if page == 1 and get_featured_result:
#             try:
#                 featured = get_featured_result(corrected_query, intent, results)
#             except Exception:
#                 pass
    
#     # === 13. CATEGORIZE & PAGINATE ===
#     categorized_results = categorize_results(results)
#     pagination = build_pagination(page, per_page, pagination_total)
    
#     # === 14. LOG EVENTS ===
#     if log_search_event:
#         try:
#             log_search_event(
#                 query=params.query,
#                 corrected_query=corrected_query,
#                 session_id=params.session_id,
#                 intent=intent,
#                 total_results=total_results,
#                 filters=filters,
#                 page=page
#             )
#         except Exception as e:
#             logger.warning(f"Search event logging error: {e}")
    
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
#         'search_type': search_type,
#         'alt_mode': params.alt_mode,
#         'facet_total': facet_total,
#         'display_total': display_total,
        
#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Dynamic tab facets (NOW from search result, not separate query)
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
        
#         # Active filters (for highlighting active tab/filter)
#         'active_data_type': active_data_type,
#         'active_category': active_category,
#         'active_schema': active_schema,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Suggestions (zero results)
#         'suggestions': suggestions,
        
#         # Session & tracking
#         'session_id': params.session_id,
#         'request_id': request_id,
#         'search_time': search_time,
#         'search_time_ms': search_time_ms,
#         'from_cache': False,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings for template use
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
#     return render(request, 'results2.html', context)


# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)
#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))
    
#     request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     client_info = get_full_client_info(request)
#     client_ip = client_info.get('ip', '')
#     location = client_info.get('location') or {}
#     device = client_info.get('device') or {}
#     user_agent = client_info.get('user_agent', '')
    
#     device_type = device.get('device_type', params.device_type or 'unknown')
#     browser = device.get('browser', 'Unknown')
#     browser_version = device.get('browser_version', '')
#     os_name = device.get('os', 'Unknown')
#     os_version = device.get('os_version', '')
#     is_mobile = device.get('is_mobile', False)
#     is_bot = device.get('is_bot', False)
    
#     # === 2. START/UPDATE SESSION (Analytics) ===
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=params.session_id,
#                 user_id=user_id,
#                 device_type=device_type,
#                 user_agent=user_agent,
#                 ip_address=client_ip,
#                 location=location,
#                 referrer=request.META.get('HTTP_REFERER'),
#                 browser=browser,
#                 browser_version=browser_version,
#                 os_name=os_name,
#                 os_version=os_version,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics start_session error: {e}")
    
#     # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None
    
#     # Dynamic tab filters
#     active_data_type = validate_data_type(request.GET.get('data_type', ''))
#     active_category = sanitize_filter_value(request.GET.get('category', ''))
#     active_schema = validate_schema(request.GET.get('schema', ''))
    
#     # Build filters dict
#     filters = {
#         'data_type': active_data_type,
#         'category': active_category,
#         'schema': active_schema,
#         'source': sanitize_filter_value(source_filter) if source_filter else None,
#         'time_range': sanitize_filter_value(request.GET.get('time', '')),
#         'location': sanitize_filter_value(request.GET.get('location', '')),
#         'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
#     }
#     # Remove empty filters
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location coordinates
#     user_location = None
#     try:
#         user_lat = request.GET.get('lat')
#         user_lng = request.GET.get('lng')
#         if user_lat and user_lng:
#             lat = float(user_lat)
#             lng = float(user_lng)
#             if -90 <= lat <= 90 and -180 <= lng <= 180:
#                 user_location = (lat, lng)
#     except (TypeError, ValueError):
#         pass
    
#     if not user_location and location:
#         loc_lat = location.get('lat')
#         loc_lng = location.get('lng')
#         if loc_lat and loc_lng:
#             try:
#                 lat = float(loc_lat)
#                 lng = float(loc_lng)
#                 if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
#                     user_location = (lat, lng)
#             except (TypeError, ValueError):
#                 pass
    
#     # === 4. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error}")
    
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment and try again.',
#             'session_id': params.session_id,
#         })
    
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request detected: {reason}")
    
#     if is_bot:
#         is_suspicious = True
#         logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
#     # === 5. EMPTY QUERY ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': '',
#             'active_category': '',
#             'active_schema': '',
#         })
    
#     # === 6. CHECK CACHE ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
        
#         if analytics:
#             try:
#                 cached_intent = cached_result.get('intent')
#                 intent_type = None
#                 if isinstance(cached_intent, dict):
#                     intent_type = cached_intent.get('type')
#                 elif isinstance(cached_intent, str):
#                     intent_type = cached_intent
                
#                 analytics.track_search(
#                     session_id=params.session_id,
#                     query=params.query,
#                     results_count=cached_result.get('total_results', 0),
#                     alt_mode=params.alt_mode,
#                     user_id=user_id,
#                     location=location,
#                     device_type=device_type,
#                     search_time_ms=0,
#                     search_strategy='cached',
#                     corrected_query=cached_result.get('corrected_query'),
#                     filters_applied=filters,
#                     page=page,
#                     intent=intent_type,
#                     request_id=request_id,
#                     browser=browser,
#                     os_name=os_name,
#                     is_mobile=is_mobile,
#                     is_bot=is_bot
#                 )
#             except Exception as e:
#                 logger.warning(f"Analytics track_search error (cached): {e}")
        
#         return render(request, 'results2.html', cached_result)
    
#     # === 7. ROUTE BASED ON ALT_MODE ===
#     search_start_time = time.time()
    
#     if params.is_keyword_search:
#         search_type = 'keyword'
#         corrected_query = params.query
#         was_corrected = False
#         word_corrections = []
#         corrections = {}
#         tuple_array = []
#         intent = {}
        
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
#     else:
#         search_type = 'semantic'
        
#         if word_discovery_multi:
#             try:
#                 corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#                 was_corrected = params.query.lower() != corrected_query.lower()
#                 word_corrections = build_word_corrections(params.query, corrected_query)
#             except Exception as e:
#                 logger.error(f"Word discovery error: {e}")
#                 corrected_query = params.query
#                 was_corrected = False
#                 word_corrections = []
#                 corrections = {}
#                 tuple_array = []
#         else:
#             corrected_query = params.query
#             was_corrected = False
#             word_corrections = []
#             corrections = {}
#             tuple_array = []
        
#         intent = {}
#         if detect_query_intent:
#             intent = detect_query_intent(corrected_query, tuple_array)
    
#     # === 8. EXECUTE SEARCH ===
#     results = []
#     total_results = 0
#     search_time = 0
#     search_strategy = search_type
    
#     # Initialize facets (will be populated from search result)
#     data_type_facets = []
#     category_facets = []
#     schema_facets = []
#     facet_total = 0
    
#     if execute_full_search:
#         try:
#             result = execute_full_search(
#                 query=corrected_query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode, 
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search
#             )
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
            
#             # Get facets from the search result
#             # data_type_facets are calculated WITHOUT data_type filter (consistent)
#             # category/schema_facets are calculated WITH current filters (scoped)
#             data_type_facets = result.get('data_type_facets', [])
#             category_facets = result.get('category_facets', [])
#             schema_facets = result.get('schema_facets', [])
#             facet_total = result.get('facet_total', 0)
            
#         except Exception as e:
#             logger.error(f"Search execution error: {e}")
    
#     search_time_ms = (time.time() - search_start_time) * 1000
    
#     # === 9. TRACK SEARCH (Analytics) ===
#     if analytics:
#         try:
#             intent_type = None
#             if isinstance(intent, dict):
#                 intent_type = intent.get('type')
#             elif isinstance(intent, str):
#                 intent_type = intent
            
#             analytics.track_search(
#                 session_id=params.session_id,
#                 query=params.query,
#                 results_count=total_results,
#                 alt_mode=params.alt_mode,
#                 user_id=user_id,
#                 location=location,
#                 device_type=device_type,
#                 search_time_ms=search_time_ms,
#                 search_strategy=search_strategy,
#                 corrected_query=corrected_query if was_corrected else None,
#                 filters_applied=filters if filters else None,
#                 page=page,
#                 intent=intent_type,
#                 request_id=request_id,
#                 browser=browser,
#                 os_name=os_name,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics track_search error: {e}")
    
#     # === 10. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(params.query, corrected_query, filters)
    
#     # === 11. USE FACETS FROM SEARCH RESULT ===
#     # Facets now come from execute_full_search:
#     # - data_type_facets: Always calculated WITHOUT data_type filter (consistent tab counts)
#     # - category_facets/schema_facets: Calculated WITH current filters (scoped sidebar)
#     # - facet_total: Sum of all data_type counts (consistent regardless of filter)
    
#     # facet_total is already extracted from result in section 8
#     # If it's 0, calculate from facets as fallback
#     if facet_total == 0:
#         facet_total = sum(f.get('count', 0) for f in data_type_facets)
    
#     # Display total is the facet total (consistent across all tabs)
#     display_total = facet_total if facet_total > 0 else total_results
    
#     # Pagination total is the actual result count for current filter
#     pagination_total = total_results
    
#     # === 12. GET SUPPLEMENTARY DATA ===
#     facets = {}
#     related_searches = []
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if get_related_searches:
#             try:
#                 related_searches = get_related_searches(corrected_query, intent)
#             except Exception:
#                 pass
        
#         if page == 1 and get_featured_result:
#             try:
#                 featured = get_featured_result(corrected_query, intent, results)
#             except Exception:
#                 pass
    
#     # === 13. CATEGORIZE & PAGINATE ===
#     categorized_results = categorize_results(results)
#     pagination = build_pagination(page, per_page, pagination_total)
    
#     # === 14. LOG EVENTS ===
#     if log_search_event:
#         try:
#             log_search_event(
#                 query=params.query,
#                 corrected_query=corrected_query,
#                 session_id=params.session_id,
#                 intent=intent,
#                 total_results=total_results,
#                 filters=filters,
#                 page=page
#             )
#         except Exception as e:
#             logger.warning(f"Search event logging error: {e}")
    
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
#         'search_type': search_type,
#         'alt_mode': params.alt_mode,
#         'facet_total': facet_total,
#         'display_total': display_total,
        
#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Dynamic tab facets (NOW from search result, not separate query)
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
        
#         # Active filters (for highlighting active tab/filter)
#         'active_data_type': active_data_type,
#         'active_category': active_category,
#         'active_schema': active_schema,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Suggestions (zero results)
#         'suggestions': suggestions,
        
#         # Session & tracking
#         'session_id': params.session_id,
#         'request_id': request_id,
#         'search_time': search_time,
#         'search_time_ms': search_time_ms,
#         'from_cache': False,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings for template use
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
#     return render(request, 'results2.html', context)
"""
FIXED: Complete updated search view with semantic cache integration.

BUG FIX: Added proper variable initialization to prevent NameError when:
1. Semantic cache is checked but doesn't hit
2. Embedding service fails
3. cache_key wasn't defined before section 16

Add this import at the top of your views.py file:
from .cached_embedding_related_search import get_query_cache
"""

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
    
#     NEW: Includes semantic query caching for alt_mode=y searches.
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)
#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))
    
#     request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     client_info = get_full_client_info(request)
#     client_ip = client_info.get('ip', '')
#     location = client_info.get('location') or {}
#     device = client_info.get('device') or {}
#     user_agent = client_info.get('user_agent', '')
    
#     device_type = device.get('device_type', params.device_type or 'unknown')
#     browser = device.get('browser', 'Unknown')
#     browser_version = device.get('browser_version', '')
#     os_name = device.get('os', 'Unknown')
#     os_version = device.get('os_version', '')
#     is_mobile = device.get('is_mobile', False)
#     is_bot = device.get('is_bot', False)
    
#     # === 2. START/UPDATE SESSION (Analytics) ===
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=params.session_id,
#                 user_id=user_id,
#                 device_type=device_type,
#                 user_agent=user_agent,
#                 ip_address=client_ip,
#                 location=location,
#                 referrer=request.META.get('HTTP_REFERER'),
#                 browser=browser,
#                 browser_version=browser_version,
#                 os_name=os_name,
#                 os_version=os_version,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics start_session error: {e}")
    
#     # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None
    
#     # Dynamic tab filters
#     active_data_type = validate_data_type(request.GET.get('data_type', ''))
#     active_category = sanitize_filter_value(request.GET.get('category', ''))
#     active_schema = validate_schema(request.GET.get('schema', ''))
    
#     # Build filters dict
#     filters = {
#         'data_type': active_data_type,
#         'category': active_category,
#         'schema': active_schema,
#         'source': sanitize_filter_value(source_filter) if source_filter else None,
#         'time_range': sanitize_filter_value(request.GET.get('time', '')),
#         'location': sanitize_filter_value(request.GET.get('location', '')),
#         'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
#     }
#     # Remove empty filters
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location coordinates
#     user_location = None
#     try:
#         user_lat = request.GET.get('lat')
#         user_lng = request.GET.get('lng')
#         if user_lat and user_lng:
#             lat = float(user_lat)
#             lng = float(user_lng)
#             if -90 <= lat <= 90 and -180 <= lng <= 180:
#                 user_location = (lat, lng)
#     except (TypeError, ValueError):
#         pass
    
#     if not user_location and location:
#         loc_lat = location.get('lat')
#         loc_lng = location.get('lng')
#         if loc_lat and loc_lng:
#             try:
#                 lat = float(loc_lat)
#                 lng = float(loc_lng)
#                 if lat != 0.0 and lng != 0.0 and -90 <= lat <= 90 and -180 <= lng <= 180:
#                     user_location = (lat, lng)
#             except (TypeError, ValueError):
#                 pass
    
#     # === 4. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error}")
    
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment and try again.',
#             'session_id': params.session_id,
#         })
    
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request detected: {reason}")
    
#     if is_bot:
#         is_suspicious = True
#         logger.info(f"Bot detected via User-Agent: {user_agent[:100]}")
    
#     # === 5. EMPTY QUERY ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': '',
#             'active_category': '',
#             'active_schema': '',
#         })
    
#     # === 6. INITIALIZE ALL VARIABLES (FIX: prevent NameError) ===
    
#     # Semantic cache variables
#     semantic_cache = get_query_cache()
#     from_semantic_cache = False
#     semantic_related_searches = []
#     cached_embedding = None
    
#     # Search result variables - initialize ALL upfront
#     results = []
#     total_results = 0
#     search_time = 0
#     search_time_ms = 0
#     search_strategy = 'keyword' if params.is_keyword_search else 'semantic'
#     search_type = 'keyword' if params.is_keyword_search else 'semantic'
#     was_corrected = False
#     corrected_query = params.query
#     word_corrections = []
#     corrections = {}
#     tuple_array = []
#     intent = {}
    
#     # Facets - initialize upfront
#     data_type_facets = []
#     category_facets = []
#     schema_facets = []
#     facet_total = 0
    
#     # Determine if we have active filters
#     has_filters = bool(active_data_type or active_category or active_schema)
    
#     # Generate cache key upfront (FIX: was missing when from_semantic_cache=True)
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
    
#     # === 6A. CHECK SEMANTIC CACHE (for alt_mode=y) ===
#     # Only use semantic cache for:
#     # - alt_mode=y (keyword/dropdown mode)
#     # - No filters applied
#     # - Valid query
#     # - Page 1 only
    
#     if params.alt_mode == 'y' and not has_filters and params.query and page == 1:
#         try:
#             if semantic_cache.should_use_cache(params.query, has_filters=False):
                
#                 # STEP 1: Try exact match first (fast - no embedding needed)
#                 semantic_cached = semantic_cache.get_cached_results(params.query)
                
#                 # STEP 2: If no exact match, try semantic similarity
#                 if not semantic_cached:
#                     try:
#                         cached_embedding = semantic_cache.get_embedding(params.query)
#                         if cached_embedding:
#                             semantic_cached = semantic_cache.find_similar_query(
#                                 embedding=cached_embedding,
#                                 threshold=0.92
#                             )
#                     except Exception as emb_error:
#                         logger.warning(f"Embedding service error: {emb_error}")
#                         cached_embedding = None
#                         semantic_cached = None
                
#                 # STEP 3: If we found a match, use the cached UUIDs
#                 if semantic_cached:
#                     try:
#                         cached_docs = semantic_cache.get_documents_by_uuids(semantic_cached['uuids'])
                        
#                         if cached_docs:  # Make sure we actually got documents
#                             from_semantic_cache = True
#                             results = cached_docs
#                             total_results = len(cached_docs)
#                             search_strategy = 'semantic_cache'
#                             search_time_ms = 0
#                             search_time = 0
                            
#                             # Get related searches for the sidebar
#                             emb = semantic_cached.get('embedding') or cached_embedding
#                             if emb:
#                                 try:
#                                     semantic_related_searches = semantic_cache.get_related_searches(
#                                         embedding=emb,
#                                         limit=5,
#                                         exclude_query=params.query
#                                     )
#                                 except Exception as rel_error:
#                                     logger.warning(f"Related searches error: {rel_error}")
                            
#                             logger.info(
#                                 f"Semantic cache HIT: '{params.query[:50]}' "
#                                 f"(match_type: {semantic_cached.get('match_type')}, "
#                                 f"similarity: {semantic_cached.get('similarity', 'exact')})"
#                             )
#                     except Exception as doc_error:
#                         logger.warning(f"Error fetching cached documents: {doc_error}")
#                         # Reset - will fall through to normal search
#                         from_semantic_cache = False
        
#         except Exception as cache_error:
#             logger.warning(f"Semantic cache error: {cache_error}")
#             # Continue with normal search
#             from_semantic_cache = False
    
#     # === 6B. CHECK EXISTING RESULT CACHE ===
#     # Only check if we didn't already get a semantic cache hit
#     if not from_semantic_cache:
#         cached_result = safe_cache_get(cache_key)
        
#         if cached_result and not filters:
#             cached_result['from_cache'] = True
#             cached_result['from_semantic_cache'] = False
            
#             if analytics:
#                 try:
#                     cached_intent = cached_result.get('intent')
#                     intent_type = None
#                     if isinstance(cached_intent, dict):
#                         intent_type = cached_intent.get('type')
#                     elif isinstance(cached_intent, str):
#                         intent_type = cached_intent
                    
#                     analytics.track_search(
#                         session_id=params.session_id,
#                         query=params.query,
#                         results_count=cached_result.get('total_results', 0),
#                         alt_mode=params.alt_mode,
#                         user_id=user_id,
#                         location=location,
#                         device_type=device_type,
#                         search_time_ms=0,
#                         search_strategy='cached',
#                         corrected_query=cached_result.get('corrected_query'),
#                         filters_applied=filters,
#                         page=page,
#                         intent=intent_type,
#                         request_id=request_id,
#                         browser=browser,
#                         os_name=os_name,
#                         is_mobile=is_mobile,
#                         is_bot=is_bot
#                     )
#                 except Exception as e:
#                     logger.warning(f"Analytics track_search error (cached): {e}")
            
#             return render(request, 'results2.html', cached_result)
    
#     # === 7. ROUTE BASED ON ALT_MODE ===
#     # Only do spell correction etc. if we didn't hit semantic cache
#     if not from_semantic_cache:
#         search_start_time = time.time()
        
#         if params.is_keyword_search:
#             search_type = 'keyword'
#             corrected_query = params.query
#             was_corrected = False
#             word_corrections = []
#             corrections = {}
#             tuple_array = []
#             intent = {}
            
#             if detect_query_intent:
#                 try:
#                     intent = detect_query_intent(corrected_query, tuple_array)
#                 except Exception as e:
#                     logger.warning(f"Intent detection error: {e}")
#                     intent = {}
#         else:
#             search_type = 'semantic'
            
#             if word_discovery_multi:
#                 try:
#                     corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#                     was_corrected = params.query.lower() != corrected_query.lower()
#                     word_corrections = build_word_corrections(params.query, corrected_query)
#                 except Exception as e:
#                     logger.error(f"Word discovery error: {e}")
#                     corrected_query = params.query
#                     was_corrected = False
#                     word_corrections = []
#                     corrections = {}
#                     tuple_array = []
#             else:
#                 corrected_query = params.query
#                 was_corrected = False
#                 word_corrections = []
#                 corrections = {}
#                 tuple_array = []
            
#             intent = {}
#             if detect_query_intent:
#                 try:
#                     intent = detect_query_intent(corrected_query, tuple_array)
#                 except Exception as e:
#                     logger.warning(f"Intent detection error: {e}")
#                     intent = {}
    
#     # === 8. EXECUTE SEARCH ===
#     # Only execute search if we didn't hit semantic cache
#     if not from_semantic_cache:
#         if execute_full_search:
#             try:
#                 result = execute_full_search(
#                     query=corrected_query,
#                     session_id=params.session_id,
#                     filters=filters,
#                     page=page,
#                     per_page=per_page,
#                     alt_mode=params.alt_mode, 
#                     user_location=user_location,
#                     pos_tags=tuple_array if params.is_semantic_search else [],
#                     safe_search=safe_search
#                 )
                
#                 results = result.get('results', [])
#                 total_results = result.get('total', 0)
#                 search_time = result.get('search_time', 0)
#                 search_strategy = result.get('search_strategy', search_type)
                
#                 # Get facets from the search result
#                 data_type_facets = result.get('data_type_facets', [])
#                 category_facets = result.get('category_facets', [])
#                 schema_facets = result.get('schema_facets', [])
#                 facet_total = result.get('facet_total', 0)
                
#             except Exception as e:
#                 logger.error(f"Search execution error: {e}")
#                 # Keep default empty values set in section 6
        
#         search_time_ms = (time.time() - search_start_time) * 1000
        
#         # === 8B. STORE IN SEMANTIC CACHE (NEW) ===
#         # Cache this query for future semantic lookups
#         # Conditions:
#         # - alt_mode=y (keyword mode)
#         # - No filters applied
#         # - Got good results (>= 3)
#         # - Not a bot
#         # - Page 1 only
        
#         if (params.alt_mode == 'y' 
#             and not has_filters 
#             and total_results >= 3 
#             and not is_bot
#             and page == 1):
            
#             try:
#                 # Get embedding if we don't have it yet
#                 if not cached_embedding:
#                     cached_embedding = semantic_cache.get_embedding(corrected_query)
                
#                 if cached_embedding:
#                     # Extract document UUIDs from results (top 20)
#                     uuids = [
#                         r.get('document_uuid') 
#                         for r in results[:20] 
#                         if r.get('document_uuid')
#                     ]
                    
#                     if uuids:
#                         success = semantic_cache.cache_query_results(
#                             query=corrected_query,
#                             embedding=cached_embedding,
#                             document_uuids=uuids
#                         )
#                         if success:
#                             logger.debug(f"Stored in semantic cache: '{corrected_query[:50]}' ({len(uuids)} UUIDs)")
                        
#             except Exception as e:
#                 logger.warning(f"Semantic cache store error: {e}")
    
#     # === 9. TRACK SEARCH (Analytics) ===
#     if analytics:
#         try:
#             intent_type = None
#             if isinstance(intent, dict):
#                 intent_type = intent.get('type')
#             elif isinstance(intent, str):
#                 intent_type = intent
            
#             analytics.track_search(
#                 session_id=params.session_id,
#                 query=params.query,
#                 results_count=total_results,
#                 alt_mode=params.alt_mode,
#                 user_id=user_id,
#                 location=location,
#                 device_type=device_type,
#                 search_time_ms=search_time_ms,
#                 search_strategy=search_strategy,
#                 corrected_query=corrected_query if was_corrected else None,
#                 filters_applied=filters if filters else None,
#                 page=page,
#                 intent=intent_type,
#                 request_id=request_id,
#                 browser=browser,
#                 os_name=os_name,
#                 is_mobile=is_mobile,
#                 is_bot=is_bot
#             )
#         except Exception as e:
#             logger.warning(f"Analytics track_search error: {e}")
    
#     # === 10. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         try:
#             suggestions = handle_zero_results(params.query, corrected_query, filters)
#         except Exception as e:
#             logger.warning(f"Zero results handling error: {e}")
#             suggestions = []
    
#     # === 11. USE FACETS FROM SEARCH RESULT ===
#     if facet_total == 0:
#         facet_total = sum(f.get('count', 0) for f in data_type_facets)
    
#     display_total = facet_total if facet_total > 0 else total_results
#     pagination_total = total_results
    
#     # === 12. GET SUPPLEMENTARY DATA ===
#     facets = {}
#     related_searches = []
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         # Use semantic related searches if available
#         if semantic_related_searches:
#             related_searches = [
#                 {
#                     'query': rs.get('query', ''),
#                     'similarity': rs.get('similarity', 0),
#                     'search_count': rs.get('search_count', 0)
#                 }
#                 for rs in semantic_related_searches
#             ]
#         elif get_related_searches:
#             try:
#                 related_searches = get_related_searches(corrected_query, intent)
#             except Exception:
#                 pass
        
#         if page == 1 and get_featured_result:
#             try:
#                 featured = get_featured_result(corrected_query, intent, results)
#             except Exception:
#                 pass
    
#     # === 13. CATEGORIZE & PAGINATE ===
#     categorized_results = categorize_results(results)
#     pagination = build_pagination(page, per_page, pagination_total)
    
#     # === 14. LOG EVENTS ===
#     if log_search_event:
#         try:
#             log_search_event(
#                 query=params.query,
#                 corrected_query=corrected_query,
#                 session_id=params.session_id,
#                 intent=intent,
#                 total_results=total_results,
#                 filters=filters,
#                 page=page
#             )
#         except Exception as e:
#             logger.warning(f"Search event logging error: {e}")
    
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
#         'search_type': search_type,
#         'alt_mode': params.alt_mode,
#         'facet_total': facet_total,
#         'display_total': display_total,
        
#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Dynamic tab facets
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
        
#         # Active filters
#         'active_data_type': active_data_type,
#         'active_category': active_category,
#         'active_schema': active_schema,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Suggestions
#         'suggestions': suggestions,
        
#         # Session & tracking
#         'session_id': params.session_id,
#         'request_id': request_id,
#         'search_time': search_time,
#         'search_time_ms': search_time_ms,
#         'from_cache': False,
#         'from_semantic_cache': from_semantic_cache,
#         'search_strategy': search_strategy,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0 and not from_semantic_cache:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
#     return render(request, 'results2.html', context)


def search(request):
    """
    Main search endpoint with dynamic tab filtering.
    
    Supports filtering by:
    - data_type: content, service, product, person, media, location (tabs)
    - category: document_category values (secondary filter)
    - schema: document_schema values (tertiary filter)
    
    Related searches are now handled inside execute_full_search().
    """
    
    # === 1. EXTRACT & VALIDATE PARAMETERS ===
    params = SearchParams(request)
    page = validate_page(request.GET.get('page', 1))
    per_page = validate_per_page(request.GET.get('per_page', 10))
    
    request_id = params.request_id or f"{params.session_id}:{time.time()}"
    
    user_id = None
    if hasattr(request, 'user') and request.user.is_authenticated:
        user_id = str(request.user.id)
    
    client_info = get_full_client_info(request)
    client_ip = client_info.get('ip', '')
    location = client_info.get('location') or {}
    device = client_info.get('device') or {}
    user_agent = client_info.get('user_agent', '')
    
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
    
    # === 3. EXTRACT FILTERS (INCLUDING DYNAMIC TAB FILTERS) ===
    source_filter = request.GET.get('source')
    if source_filter in ('home', 'results_page', 'header', None, ''):
        source_filter = None
    
    # Dynamic tab filters
    active_data_type = validate_data_type(request.GET.get('data_type', ''))
    active_category = sanitize_filter_value(request.GET.get('category', ''))
    active_schema = validate_schema(request.GET.get('schema', ''))
    
    # Build filters dict
    filters = {
        'data_type': active_data_type,
        'category': active_category,
        'schema': active_schema,
        'source': sanitize_filter_value(source_filter) if source_filter else None,
        'time_range': sanitize_filter_value(request.GET.get('time', '')),
        'location': sanitize_filter_value(request.GET.get('location', '')),
        'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
    }
    # Remove empty filters
    filters = {k: v for k, v in filters.items() if v}
    
    safe_search = request.GET.get('safe', 'on') == 'on'
    
    # User location coordinates
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
    
    if not user_location and location:
        loc_lat = location.get('lat')
        loc_lng = location.get('lng')
        if loc_lat and loc_lng:
            try:
                lat = float(loc_lat)
                lng = float(loc_lng)
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
            'data_type_facets': [],
            'category_facets': [],
            'schema_facets': [],
            'active_data_type': '',
            'active_category': '',
            'active_schema': '',
        })
    
    # === 6. INITIALIZE ALL VARIABLES ===
    
    # Search result variables - initialize ALL upfront
    results = []
    total_results = 0
    search_time = 0
    search_time_ms = 0
    search_strategy = 'keyword' if params.is_keyword_search else 'semantic'
    search_type = 'keyword' if params.is_keyword_search else 'semantic'
    was_corrected = False
    corrected_query = params.query
    word_corrections = []
    corrections = {}
    tuple_array = []
    intent = {}
    
    # Facets - initialize upfront
    data_type_facets = []
    category_facets = []
    schema_facets = []
    facet_total = 0
    
    # Related searches - initialize upfront
    related_searches = []
    
    # Determine if we have active filters
    has_filters = bool(active_data_type or active_category or active_schema)
    
    # Generate cache key upfront
    cache_key = get_cache_key(
        'search', params.query, page, params.alt_mode,
        active_data_type, active_category, active_schema,
        json.dumps(filters, sort_keys=True)
    )
    
    # === 6B. CHECK EXISTING RESULT CACHE ===
    cached_result = safe_cache_get(cache_key)
    
    if cached_result and not filters:
        cached_result['from_cache'] = True
        cached_result['from_semantic_cache'] = False
        
        if analytics:
            try:
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
                    search_time_ms=0,
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
            try:
                intent = detect_query_intent(corrected_query, tuple_array)
            except Exception as e:
                logger.warning(f"Intent detection error: {e}")
                intent = {}
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
            try:
                intent = detect_query_intent(corrected_query, tuple_array)
            except Exception as e:
                logger.warning(f"Intent detection error: {e}")
                intent = {}
    
    # === 8. EXECUTE SEARCH ===
    if execute_full_search:
        try:
            result = execute_full_search(
                query=corrected_query,
                session_id=params.session_id,
                filters=filters,
                page=page,
                per_page=per_page,
                alt_mode=params.alt_mode, 
                user_location=user_location,
                pos_tags=tuple_array if params.is_semantic_search else [],
                safe_search=safe_search
            )
            
            results = result.get('results', [])
            total_results = result.get('total', 0)
            search_time = result.get('search_time', 0)
            search_strategy = result.get('search_strategy', search_type)
            
            # Get facets from the search result
            data_type_facets = result.get('data_type_facets', [])
            category_facets = result.get('category_facets', [])
            schema_facets = result.get('schema_facets', [])
            facet_total = result.get('facet_total', 0)
            
            # Get related searches from the search result (handled in typesense_calculations.py)
            related_searches = result.get('related_searches', [])
            
        except Exception as e:
            logger.error(f"Search execution error: {e}")
            # Keep default empty values set in section 6
    
    search_time_ms = (time.time() - search_start_time) * 1000
    
    # === 9. TRACK SEARCH (Analytics) ===
    if analytics:
        try:
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
        try:
            suggestions = handle_zero_results(params.query, corrected_query, filters)
        except Exception as e:
            logger.warning(f"Zero results handling error: {e}")
            suggestions = []
    
    # === 11. USE FACETS FROM SEARCH RESULT ===
    if facet_total == 0:
        facet_total = sum(f.get('count', 0) for f in data_type_facets)
    
    display_total = facet_total if facet_total > 0 else total_results
    pagination_total = total_results
    
    # === 12. GET SUPPLEMENTARY DATA ===
    facets = {}
    featured = None
    
    if results:
        if get_facets:
            try:
                facets = get_facets(corrected_query)
            except Exception:
                pass
        
        # Fallback to keyword-based related searches if not provided by execute_full_search
        if not related_searches and get_related_searches:
            try:
                related_searches = get_related_searches(corrected_query, intent)
            except Exception:
                pass
        
        if page == 1 and get_featured_result:
            try:
                featured = get_featured_result(corrected_query, intent, results)
            except Exception:
                pass
    
    # === 13. CATEGORIZE & PAGINATE ===
    categorized_results = categorize_results(results)
    pagination = build_pagination(page, per_page, pagination_total)
    
    # === 14. LOG EVENTS ===
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
    
    # === 15. BUILD CONTEXT ===
    context = {
        # Core search
        'query': params.query,
        'corrected_query': corrected_query,
        'was_corrected': was_corrected,
        'word_corrections': word_corrections,
        'corrections': corrections,
        'intent': intent,
        'search_type': search_type,
        'alt_mode': params.alt_mode,
        'facet_total': facet_total,
        'display_total': display_total,
        
        # Results
        'results': results,
        'categorized_results': categorized_results,
        'total_results': total_results,
        'has_results': len(results) > 0,
        'featured': featured,
        'related_searches': related_searches,
        
        # Filters
        'filters': filters,
        'facets': facets,
        'safe_search': safe_search,
        
        # Dynamic tab facets
        'data_type_facets': data_type_facets,
        'category_facets': category_facets,
        'schema_facets': schema_facets,
        
        # Active filters
        'active_data_type': active_data_type,
        'active_category': active_category,
        'active_schema': active_schema,
        
        # Pagination
        'pagination': pagination,
        'page': page,
        'per_page': per_page,
        
        # Suggestions
        'suggestions': suggestions,
        
        # Session & tracking
        'session_id': params.session_id,
        'request_id': request_id,
        'search_time': search_time,
        'search_time_ms': search_time_ms,
        'from_cache': False,
        'from_semantic_cache': False,
        'search_strategy': search_strategy,
        
        # Device & location
        'device_type': device_type,
        'source': params.source,
        'user_city': location.get('city', '') if location else '',
        'user_country': location.get('country', '') if location else '',
        
        # Label mappings
        'data_type_labels': DATA_TYPE_LABELS,
        'category_labels': CATEGORY_LABELS,
    }
    
    # === 16. CACHE RESULTS ===
    if not filters and total_results > 0:
        safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])
    
    return render(request, 'results2.html', context)

# =============================================================================
# VIEW: CATEGORY ROUTER
# =============================================================================

def category_view(request, category_slug: str):
    """Generic category view router."""
    category_slug = str(category_slug).lower().strip()
    
    if not category_slug or category_slug not in SCHEMA_MAP:
        raise Http404("Category not found")
    
    city = get_user_city(request)
    
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
    
    return generic_category_view(request, category_slug, city)


def generic_category_view(request, category_slug: str, city: str):
    """Generic category page with faceted search."""
    
    schema = SCHEMA_MAP.get(category_slug)
    if not schema:
        raise Http404("Category not found")
    
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'relevance'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    filters = [f'document_schema:={schema}']
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    
    sort_options = {
        'authority': 'authority_score:desc',
        'recent': 'created_at:desc',
        'relevance': '_text_match:desc',
    }
    sort_by = sort_options.get(sort, 'authority_score:desc')
    
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
    
    # Get query parameters
    query = sanitize_query(request.GET.get('q', ''))
    selected_subcategory = sanitize_filter_value(request.GET.get('subcategory', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    # Validate subcategory
    if selected_subcategory and selected_subcategory not in SUBCATEGORY_MAP:
        selected_subcategory = ''
    
    # Build filter - only filter by business schema
    filters = ['document_schema:=business']
    
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    
    # Build search query - NO CITY!
    search_query = '*'  # Default: return ALL businesses
    
    if query:
        search_query = query
    elif selected_subcategory and selected_subcategory in SUBCATEGORY_MAP:
        search_query = ' '.join(SUBCATEGORY_MAP[selected_subcategory])
    
    sort_options = {
        'authority': '_text_match:desc',
        'recent': '_text_match:desc',
        'name': 'document_title:asc',
    }
    sort_by = sort_options.get(sort, '_text_match:desc')
    
    # Execute search directly using typesense_manager
    search_params = {
        'q': search_query,
        'query_by': 'document_title,document_summary,keywords,primary_keywords',
        'filter_by': filter_by,
        'per_page': 20,
        'page': page,
        'facet_by': 'document_category,document_brand',
        'max_facet_values': 20,
    }
    
    # Only add sort_by if not using wildcard
    if search_query != '*':
        search_params['sort_by'] = sort_by
    
    # Debug output
    print(f"\n=== BUSINESS SEARCH ===")
    print(f"Query: '{search_query}'")
    print(f"Filter: '{filter_by}'")
    print(f"Params: {search_params}")
    
    # Execute search
    results = typesense_manager.search(COLLECTION_NAME, search_params)
    
    print(f"Results: {type(results)}, is None: {results is None}")
    if results:
        print(f"Found: {results.get('found', 0)}, Hits: {len(results.get('hits', []))}")
    print("=== END ===\n")
    
    # Process results
    if results:
        browse_results = results.get('hits', [])
        total = results.get('found', 0)
        facets = parse_facets(results)
    else:
        browse_results = []
        total = 0
        facets = {'category': [], 'brand': []}
    
    # Build stats
    stats = {
        'business_count': total,
        'categories_count': len(facets.get('category', [])) or 24,
        'verified_pct': 78,
    }
    
    trending_searches = [
        'black barber near me', 'soul food', 'black accountant',
        'african restaurant', 'natural hair salon', 'black owned clothing',
    ]
    
    active_filter_count = sum([
        bool(selected_subcategory),
        bool(selected_category),
        bool(selected_brand),
    ])
    
    # Build context
    context = {
        'city': '',  # No city filtering
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

def generate_request_id(session_id):
    """Generate unique request ID for tracking."""
    return f"{session_id}:{time.time()}:{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"


def culture_category(request, city: str = None):
    """
    Culture & heritage page with faceted search.
    """
    
    # ==================== START TIMING ====================
    search_start_time = time.time()
    
    # ==================== PARAMETERS ====================
    query = sanitize_query(request.GET.get('q', ''))
    selected_topic = sanitize_filter_value(request.GET.get('topic', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    today = date.today()
    
    # Base filter for culture page
    filters = ['document_schema:=culture']
    
    valid_topics = {'hbcus', 'film', 'music', 'art', 'literature', 'food', 'fashion', 'history', 'theater', 'events'}
    if selected_topic and selected_topic not in valid_topics:
        selected_topic = ''
    
    # Topic-based filtering
    if selected_topic == 'hbcus':
        filters = ['document_category:=hbcu']
    elif selected_topic == 'film':
        filters = ['(document_schema:=culture || document_schema:=media)']
    elif selected_topic == 'music':
        filters = ['document_schema:=culture', 'document_category:=music']
    elif selected_topic == 'art':
        filters = ['document_schema:=culture', 'document_category:=art']
    elif selected_topic == 'literature':
        filters = ['document_schema:=culture', 'document_category:=literature']
    elif selected_topic == 'food':
        filters = ['document_schema:=culture', 'document_category:=food']
    elif selected_topic == 'fashion':
        filters = ['document_schema:=culture', 'document_category:=fashion']
    elif selected_topic == 'history':
        filters = ['document_schema:=culture', 'document_category:=history']
    elif selected_topic == 'theater':
        filters = ['document_schema:=culture', 'document_category:=theater']
    elif selected_topic == 'events':
        filters = ['document_schema:=culture', 'document_category:=events']
    
    # Additional filters from sidebar
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    sort_by = 'authority_score:desc' if sort == 'authority' else 'created_at:desc'
    
    # ==================== MAIN SEARCH ====================
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
    
    # Calculate search time
    search_time_ms = (time.time() - search_start_time) * 1000
    
    # ==================== HBCU SPOTLIGHT ====================
    hbcu_results = typesense_search(
        query='*',
        filter_by='document_category:=hbcu',
        sort_by='authority_score:desc',
        per_page=4,
    )
    hbcus = safe_get_hits(hbcu_results)
    
    # ==================== FEATURED ARTICLE ====================
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
    
    # ==================== ANALYTICS TRACKING ====================
    analytics = get_analytics()
    
    # Ensure session exists (safe)
    session_id = _safe_get_session_id(request)
    
    # Get user info
    user_id = str(request.user.id) if request.user.is_authenticated else None
    device_info = _safe_get_device_info(request)
    location = _safe_get_location(request)
    ip_address = _safe_get_client_ip(request)
    referrer = request.META.get('HTTP_REFERER', '')
    
    # Generate unique request ID for this page load
    request_id = generate_request_id(session_id)
    
    # Track session (call on every page load)
    if analytics:
        try:
            analytics.start_session(
                session_id=session_id,
                user_id=user_id,
                device_type=device_info.get('device_type', 'unknown'),
                user_agent=device_info.get('user_agent', ''),
                ip_address=ip_address,
                location=location if location else None,
                referrer=referrer,
                browser=device_info.get('browser', 'Unknown'),
                browser_version=device_info.get('browser_version', ''),
                os_name=device_info.get('os_name', 'Unknown'),
                os_version=device_info.get('os_version', ''),
                is_mobile=device_info.get('is_mobile', False),
                is_bot=device_info.get('is_bot', False),
            )
        except Exception as e:
            logger.error(f"Failed to start session: {e}")
    
    # Track search
    if analytics:
        try:
            search_query = query if query else f"[browse:{selected_topic or 'all'}]"
            
            analytics.track_search(
                session_id=session_id,
                query=search_query,
                results_count=total,
                alt_mode='n',
                user_id=user_id,
                location=location if location else None,
                device_type=device_info.get('device_type', 'unknown'),
                search_time_ms=search_time_ms,
                search_strategy='faceted',
                filters_applied={
                    'topic': selected_topic,
                    'category': selected_category,
                    'brand': selected_brand,
                    'sort': sort,
                } if any([selected_topic, selected_category, selected_brand]) else None,
                page=page,
                intent='culture_browse',
                request_id=request_id,
                browser=device_info.get('browser', 'Unknown'),
                browser_version=device_info.get('browser_version', ''),
                os_name=device_info.get('os_name', 'Unknown'),
                os_version=device_info.get('os_version', ''),
                is_mobile=device_info.get('is_mobile', False),
                is_bot=device_info.get('is_bot', False),
            )
        except Exception as e:
            logger.error(f"Failed to track search: {e}")
    
    # Track filter/topic selection as events
    if analytics and selected_topic:
        try:
            analytics.track_event(
                session_id=session_id,
                event_type='topic_selected',
                event_data={
                    'topic': selected_topic,
                    'page': 'culture',
                },
                user_id=user_id,
                location=location if location else None,
            )
        except Exception as e:
            logger.error(f"Failed to track topic event: {e}")
    
    if analytics and (selected_category or selected_brand):
        try:
            analytics.track_event(
                session_id=session_id,
                event_type='filter_applied',
                event_data={
                    'category': selected_category,
                    'brand': selected_brand,
                    'page': 'culture',
                },
                user_id=user_id,
                location=location if location else None,
            )
        except Exception as e:
            logger.error(f"Failed to track filter event: {e}")
    
    # ==================== BUILD CONTEXT ====================
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
        
        # Analytics context for client-side tracking
        'analytics_context': {
            'session_id': session_id,
            'request_id': request_id,
            'query': query,
            'results_count': total,
            'page': page,
            'user_id': user_id or '',
        },
    }
    
    return render(request, 'category_culture.html', context)


# =============================================================================
# VIEW: HEALTH CATEGORY
# =============================================================================

def health_category(request, city: str):
    """Health resources page with faceted search."""
    
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
    filters = ['document_schema:=health']
    if selected_category:
        filters.append(f'document_category:={selected_category}')
    if selected_brand:
        filters.append(f'document_brand:={selected_brand}')
    
    filter_by = build_filter_string(filters)
    sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
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
    
    query = sanitize_query(request.GET.get('q', ''))
    selected_section = sanitize_filter_value(request.GET.get('section', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', 'recent'), ['authority', 'recent'], 'recent')
    page = validate_page(request.GET.get('page', 1))
    
    section_category_map = {
        'local': None,
        'national': 'national',
        'politics': 'politics',
        'business': 'business',
        'sports': 'sports',
        'entertainment': 'entertainment',
        'opinion': 'opinion',
    }
    
    if selected_section and selected_section not in section_category_map:
        selected_section = ''
    
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
    
    top_results = typesense_search(
        query='*',
        filter_by='document_schema:=news',
        sort_by='authority_score:desc',
        per_page=1,
    )
    top_hits = safe_get_hits(top_results)
    top_story = top_hits[0].get('document') if top_hits else None
    
    sidebar_results = typesense_search(
        query='*',
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=4,
    )
    sidebar_hits = safe_get_hits(sidebar_results)
    sidebar_stories = [hit.get('document', {}) for hit in sidebar_hits[1:] if hit.get('document')]
    
    local_results = typesense_search(
        query=city,
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=5,
    )
    local_news = [hit.get('document', {}) for hit in safe_get_hits(local_results) if hit.get('document')]
    
    good_results = typesense_search(
        query='success achievement award grant scholarship wins first',
        filter_by='document_schema:=news',
        sort_by='created_at:desc',
        per_page=5,
    )
    good_news = [hit.get('document', {}) for hit in safe_get_hits(good_results) if hit.get('document')]
    
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
    
    city = get_user_city(request)
    
    query = sanitize_query(request.GET.get('q', ''))
    selected_category = sanitize_filter_value(request.GET.get('category', ''))
    selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
    sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
    page = validate_page(request.GET.get('page', 1))
    
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
    
    church_results = typesense_search(
        query=f'{city} church baptist AME methodist faith ministry',
        filter_by='document_schema:=community',
        sort_by='authority_score:desc',
        per_page=8,
    )
    churches = safe_get_hits(church_results)
    
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
    
    if subcategory and subcategory not in SUBCATEGORY_MAP:
        subcategory = ''
    
    search_query = query if query else '*'
    if subcategory and subcategory in SUBCATEGORY_MAP:
        terms = ' '.join(SUBCATEGORY_MAP[subcategory])
        search_query = f"{search_query} {terms}" if search_query != '*' else terms
    if search_query == '*':
        search_query = city
    
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


@require_GET
def facets_api(request):
    """
    API endpoint to get facet counts for dynamic tabs.
    
    Usage: /api/facets/?query=hair+salon
    
    Returns facet counts for data_type, category, and schema.
    """
    query = sanitize_query(request.GET.get('query', '') or request.GET.get('q', ''))
    
    if not query:
        return JsonResponse({'facets': {}})
    
    try:
        facets = get_dynamic_tab_facets(query)
        
        return JsonResponse({
            'success': True,
            'facets': {
                'data_types': facets.get('data_type', []),
                'categories': facets.get('category', []),
                'schemas': facets.get('schema', []),
            }
        })
        
    except Exception as e:
        logger.error(f"Facets API error: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# =============================================================================
# DETAIL VIEWS
# =============================================================================

def business_detail(request, business_id: str):
    """Individual business detail page."""
    
    if not business_id or len(business_id) > 100:
        raise Http404("Invalid business ID")
    
    business_id = re.sub(r'[^a-zA-Z0-9_-]', '', business_id)
    
    if not business_id:
        raise Http404("Invalid business ID")
    
    doc = typesense_manager.get_document(COLLECTION_NAME, business_id)
    
    if not doc:
        raise Http404("Business not found")
    
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
        'content': 'articles',
        'video': 'videos',
        'media': 'videos',
        'product': 'products',
        'person': 'people',
        'place': 'places',
        'location': 'places',
        'service': 'services'
    }
    
    for result in results:
        data_type = result.get('data_type', '') or result.get('document_data_type', 'other')
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
# VIEW: TRACK CLICK (Analytics)
# =============================================================================

@csrf_exempt
@require_http_methods(["POST", "GET"])
def track_click(request):
    """Track when a user clicks on a search result or other events."""
    
    if request.method == 'POST':
        try:
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
    
    client_ip = get_client_ip(request)
    
    session_id = sanitize_string(
        data.get('session_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_session_id_length']
    )
    
    is_allowed, rate_error = TrackClickRateLimiter.check_rate_limit(session_id, client_ip)
    if not is_allowed:
        return JsonResponse(
            {'success': False, 'error': rate_error},
            status=429
        )
    
    event_type = sanitize_string(data.get('event_type', ''), max_length=50)
    
    if event_type and event_type != 'click':
        analytics = get_analytics()
        if analytics:
            try:
                if session_id:
                    location = get_location_from_request(request)
                    
                    user_id = None
                    if hasattr(request, 'user') and request.user.is_authenticated:
                        user_id = str(request.user.id)
                    
                    sanitized_data = {}
                    for key, value in data.items():
                        if isinstance(value, str):
                            sanitized_data[key] = value[:500]
                        elif isinstance(value, (int, float, bool)):
                            sanitized_data[key] = value
                        elif isinstance(value, dict):
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
        return JsonResponse({'success': True})
    
    clicked_url = sanitize_url(
        data.get('url', ''),
        max_length=TRACK_CLICK_CONFIG['max_url_length']
    )
    query = sanitize_string(
        data.get('query', ''),
        max_length=TRACK_CLICK_CONFIG['max_query_length']
    )
    
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
    
    results_count = sanitize_int(data.get('results_count', 0), default=0, min_val=0, max_val=10000)
    
    was_corrected = str(data.get('was_corrected', 'false')).lower() == 'true'
    corrected_query = sanitize_string(
        data.get('corrected_query', ''),
        max_length=TRACK_CLICK_CONFIG['max_corrected_query_length']
    )
    
    time_to_click_ms = None
    raw_time = data.get('time_to_click_ms')
    if raw_time is not None:
        time_to_click_ms = sanitize_int(raw_time, default=0, min_val=0, max_val=3600000)
        if time_to_click_ms == 0:
            time_to_click_ms = None
    
    user_id = None
    if hasattr(request, 'user') and request.user.is_authenticated:
        user_id = str(request.user.id)
    
    location = get_location_from_request(request)
    
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
    """Redirect-based click tracking."""
    
    destination_url = sanitize_url(
        request.GET.get('url', ''),
        max_length=TRACK_CLICK_CONFIG['max_url_length']
    )
    
    if not destination_url:
        return HttpResponseBadRequest('Missing or invalid URL parameter')
    
    client_ip = get_client_ip(request)
    
    session_id = sanitize_string(
        request.GET.get('session_id', ''),
        max_length=TRACK_CLICK_CONFIG['max_session_id_length']
    )
    
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


# =============================================================================
# FOOTER FUNCTIONS
# =============================================================================

def about(request):
    return render(request, 'about.html')


def privacy(request):
    return render(request, 'privacy.html')


def term(request):
    return render(request, 'terms.html')


def contact(request):
    return render(request, 'contact.html')


# =============================================================================
# DEBUG VIEWS (REMOVE IN PRODUCTION)
# =============================================================================

def debug_schema(request):
    """Diagnostic view to inspect Typesense schema and sample documents."""
    
    debug_info = {
        'collection_schema': None,
        'sample_businesses': [],
        'all_fields_in_businesses': set(),
        'location_fields_found': [],
        'errors': [],
    }
    
    try:
        schema = typesense_manager._client.collections[COLLECTION_NAME].retrieve()
        debug_info['collection_schema'] = {
            'name': schema.get('name'),
            'num_documents': schema.get('num_documents'),
            'fields': [
                {
                    'name': f.get('name'),
                    'type': f.get('type'),
                    'facet': f.get('facet', False),
                    'optional': f.get('optional', False),
                }
                for f in schema.get('fields', [])
            ]
        }
    except Exception as e:
        debug_info['errors'].append(f"Schema retrieval error: {e}")
    
    try:
        results = typesense_manager.search(COLLECTION_NAME, {
            'q': '*',
            'query_by': 'document_title',
            'filter_by': 'document_schema:=business',
            'per_page': 5,
        })
        
        if results and results.get('hits'):
            for hit in results['hits']:
                doc = hit.get('document', {})
                debug_info['all_fields_in_businesses'].update(doc.keys())
                debug_info['sample_businesses'].append(doc)
            
            debug_info['all_fields_in_businesses'] = sorted(list(debug_info['all_fields_in_businesses']))
            
            location_keywords = ['city', 'state', 'location', 'address', 'lat', 'lng', 'geo', 'place', 'zip', 'postal', 'country', 'region']
            for field in debug_info['all_fields_in_businesses']:
                field_lower = field.lower()
                if any(kw in field_lower for kw in location_keywords):
                    debug_info['location_fields_found'].append(field)
                    
    except Exception as e:
        debug_info['errors'].append(f"Sample documents error: {e}")
    
    vector_fields = []
    if debug_info.get('collection_schema'):
        for field in debug_info['collection_schema'].get('fields', []):
            if 'vec' in field['name'].lower() or 'embed' in field['name'].lower() or field['type'].startswith('float'):
                vector_fields.append(field)
    debug_info['vector_fields'] = vector_fields
    
    try:
        test_results = typesense_manager.search(COLLECTION_NAME, {
            'q': 'atlanta barbers',
            'query_by': 'document_title,document_summary,keywords,primary_keywords',
            'filter_by': 'document_schema:=business',
            'per_page': 5,
        })
        
        debug_info['atlanta_barbers_test'] = {
            'found': test_results.get('found', 0) if test_results else 0,
            'results': [
                {
                    'title': hit.get('document', {}).get('document_title'),
                    'keywords': hit.get('document', {}).get('keywords', [])[:5],
                    'primary_keywords': hit.get('document', {}).get('primary_keywords', [])[:5],
                }
                for hit in (test_results.get('hits', []) if test_results else [])
            ]
        }
    except Exception as e:
        debug_info['errors'].append(f"Atlanta barbers test error: {e}")
    
    try:
        keyword_sample = typesense_manager.search(COLLECTION_NAME, {
            'q': '*',
            'query_by': 'document_title',
            'filter_by': 'document_schema:=business',
            'per_page': 10,
        })
        
        if keyword_sample and keyword_sample.get('hits'):
            debug_info['keyword_samples'] = [
                {
                    'title': hit.get('document', {}).get('document_title', '')[:50],
                    'keywords': hit.get('document', {}).get('keywords', []),
                    'primary_keywords': hit.get('document', {}).get('primary_keywords', []),
                    'document_brand': hit.get('document', {}).get('document_brand', ''),
                }
                for hit in keyword_sample['hits'][:5]
            ]
    except Exception as e:
        debug_info['errors'].append(f"Keyword sample error: {e}")
    
    return JsonResponse(debug_info, json_dumps_params={'indent': 2})


def debug_business_search(request):
    """Diagnostic endpoint to debug why businesses aren't showing."""
    
    debug_info = {
        'typesense_available': False,
        'total_documents': 0,
        'business_schema_count': 0,
        'sample_documents': [],
        'errors': [],
        'schema_values': [],
    }
    
    debug_info['typesense_available'] = typesense_manager.available
    debug_info['collection_name'] = COLLECTION_NAME
    
    if not typesense_manager.available:
        debug_info['errors'].append("Typesense is not connected")
        return JsonResponse(debug_info, json_dumps_params={'indent': 2})
    
    try:
        all_results = typesense_manager.search(COLLECTION_NAME, {
            'q': '*',
            'query_by': 'document_title',
            'per_page': 10,
            'facet_by': 'document_schema,document_category,document_brand',
            'max_facet_values': 50,
        })
        
        if all_results:
            debug_info['total_documents'] = all_results.get('found', 0)
            debug_info['sample_documents'] = [
                {
                    'id': hit.get('document', {}).get('id'),
                    'title': hit.get('document', {}).get('document_title'),
                    'schema': hit.get('document', {}).get('document_schema'),
                    'category': hit.get('document', {}).get('document_category'),
                    'brand': hit.get('document', {}).get('document_brand'),
                }
                for hit in all_results.get('hits', [])
            ]
            
            for facet in all_results.get('facet_counts', []):
                field = facet.get('field_name')
                counts = [{'value': c['value'], 'count': c['count']} for c in facet.get('counts', [])]
                debug_info[f'{field}_values'] = counts
        else:
            debug_info['errors'].append("Search returned None")
                
    except Exception as e:
        debug_info['errors'].append(f"Search error: {str(e)}")
    
    try:
        business_results = typesense_manager.search(COLLECTION_NAME, {
            'q': '*',
            'query_by': 'document_title',
            'filter_by': 'document_schema:=business',
            'per_page': 5,
        })
        
        if business_results:
            debug_info['business_schema_count'] = business_results.get('found', 0)
    except Exception as e:
        debug_info['errors'].append(f"Business filter error: {str(e)}")
    
    return JsonResponse(debug_info, json_dumps_params={'indent': 2})