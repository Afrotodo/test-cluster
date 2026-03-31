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
from .address_utils import process_address_maps
from .typesense_discovery_bridge import _generate_stable_cache_key, _get_cached_results, fetch_full_documents, _has_real_images



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
from .typesense_discovery_bridge import _generate_stable_cache_key, _get_cached_results, fetch_full_documents

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

# try:
#     from .typesense_calculations import (
#         detect_query_intent,
#         execute_full_search,
#         get_facets,
#         get_featured_result,
#         get_related_searches,
#         log_search_event,
#     )
# except ImportError:
#     detect_query_intent = None
#     execute_full_search = None
#     get_facets = None
#     get_featured_result = None
#     get_related_searches = None
#     log_search_event = None
# Word Discovery v2 - no longer need word_discovery_multi (handled inside bridge)
word_discovery_multi = None

try:
    from .typesense_discovery_bridge import (
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
                host=config('REDIS_LOCATION'),
                port=config('REDIS_PORT', cast=int),
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
COLLECTION_NAME = 'document'


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

from .cached_embedding_related_search import get_popular_queries
from .trending import get_trending_results, cache_trending_result

def home(request):
    city = get_user_city(request)
    
    # Get location from IP geolocation
    client_info = get_full_client_info(request)
    location = client_info.get('location') or {}
    
    # Try most specific to least specific
    location_levels = [
        (location.get('city', ''), 'city'),
        (location.get('region', ''), 'region'),
        (location.get('country', ''), 'country'),
    ]
    
    trending_results = []
    trending_label = 'Your Area'
    
    for loc_value, loc_type in location_levels:
        if loc_value:
            trending_results = get_trending_results(city=loc_value, limit=6)
            if trending_results:
                trending_label = loc_value.title()
                break
    
    # Fallback to general
    if not trending_results:
        trending_results = get_trending_results(city=None, limit=6)
        trending_label = 'Your Area'
    
    context = {
        'city': city,
        'trending_label': trending_label,
        'trending_results': trending_results,
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

        for item in results:
            print(f"DEBUG api item answer={item.get('answer', 'MISSING')!r} entity_type={item.get('entity_type')!r}")

        suggestions = [
            {
                'text': item.get('term', ''),
                'display': item.get('display', ''),
                'description': item.get('description', ''),
                'entity_type': item.get('entity_type', ''),
                'document_uuid': item.get('document_uuid', ''),
                'answer': item.get('answer', ''),
                'answer_type': item.get('answer_type', ''),
            }
            for item in results
            if item.get('term')
        ]
        
        return JsonResponse({'suggestions': suggestions})
    
    except Exception as e:
        logger.error(f"Autocomplete error: {e}")
        return JsonResponse({'suggestions': [], 'error': 'Service temporarily unavailable'})



"""
Complete search view with Images tab support.
Replace your existing search function with this one.
"""



## _------------------------------------------------------ Test Beginning --------------------
def extract_images_from_results(results):
    """
    Extract all images from search results.
    
    Returns a list of image dictionaries with:
    - url: The image URL
    - title: Title from the parent document
    - source_url: URL of the parent document
    - source_name: Site name of the parent document
    - image_type: 'photo', 'logo', etc.
    """
    image_results = []
    seen_urls = set()  # Deduplicate images
    
    for result in results:
        title = result.get('title', 'Untitled')
        source_url = result.get('url', '#')
        source_name = result.get('site_name', result.get('source', 'Unknown'))
        
        # Extract from image_url array
        image_urls = result.get('image_url', [])
        if image_urls:
            if isinstance(image_urls, str):
                image_urls = [image_urls]
            for img_url in image_urls:
                if img_url and img_url not in seen_urls:
                    seen_urls.add(img_url)
                    image_results.append({
                        'url': img_url,
                        'title': title,
                        'source_url': source_url,
                        'source_name': source_name,
                        'image_type': 'photo'
                    })
        
        # Extract from logo_url array
        logo_urls = result.get('logo_url', [])
        if logo_urls:
            if isinstance(logo_urls, str):
                logo_urls = [logo_urls]
            for logo_url in logo_urls:
                if logo_url and logo_url not in seen_urls:
                    seen_urls.add(logo_url)
                    image_results.append({
                        'url': logo_url,
                        'title': f"{title} - Logo",
                        'source_url': source_url,
                        'source_name': source_name,
                        'image_type': 'logo'
                    })
    
    return image_results


def _build_image_pagination(total_image_count, page, img_per_page=40):
    """
    Build pagination dict for image results.
    
    Always returns a valid pagination dict (never None) when there are images.
    Returns None only when there are zero images.
    
    Args:
        total_image_count: Total number of images across all pages
        page: Current page number
        img_per_page: Images per page (default 40)
    """
    if total_image_count == 0:
        return None
    
    img_total_pages = max(1, (total_image_count + img_per_page - 1) // img_per_page)
    
    # Clamp page to valid range
    page = max(1, min(page, img_total_pages))
    
    img_start = (page - 1) * img_per_page
    img_end = min(img_start + img_per_page, total_image_count)
    
    return {
        'current_page': page,
        'total_pages': img_total_pages,
        'has_previous': page > 1,
        'previous_page': page - 1,
        'has_next': page < img_total_pages,
        'next_page': page + 1,
        'page_range': range(max(1, page - 3), min(img_total_pages + 1, page + 4)),
        'total_images': total_image_count,
        'start_result': img_start + 1,
        'end_result': img_end,
    }




def search(request):
    """
    Main search endpoint with dynamic tab filtering.
    
    Supports filtering by:
    - data_type: content, service, product, person, media, location (tabs)
    - category: document_category values (secondary filter)
    - schema: document_schema values (tertiary filter)
    - view: 'images' for image grid view
    
    Related searches are now handled inside execute_full_search().
    """
    
    # === 1. EXTRACT & VALIDATE PARAMETERS ===
    params = SearchParams(request)

    # === 0. PREVENT FORM RESUBMISSION ON TAB CLICKS ===
    # The home page form submits to this view via GET. The browser marks
    # this URL as a "form submission", so every tab click triggers a
    # resubmission warning. Fix: redirect once from the home page submission
    # so the browser treats the results page as a plain GET navigation.
    # The _rd=1 flag prevents an infinite redirect loop.
    if params.query and not request.GET.get('_rd'):
        from urllib.parse import urlparse
        from django.shortcuts import redirect
        referer = request.META.get('HTTP_REFERER', '')
        if referer:
            referer_path = urlparse(referer).path
            current_path = request.path
            if referer_path != current_path:
                return redirect(request.get_full_path() + '&_rd=1')

    page = validate_page(request.GET.get('page', 1))
    per_page = validate_per_page(request.GET.get('per_page', 10))

    # === 1B. EXTRACT QUESTION PATH FIELDS ===
    document_uuid = request.GET.get('document_uuid', '').strip()
    search_source = request.GET.get('search_source', '').strip()
    answer = request.GET.get('answer', '').strip()
    answer_type = request.GET.get('answer_type', '').strip()
    is_question_path = (search_source == 'question' and bool(document_uuid))
    
    # Check for images view
    view_mode = request.GET.get('view', '')
    show_images = view_mode == 'images'
    
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
            # Provide safe defaults so template never hits VariableDoesNotExist
            'display_total': 0,
            'total_results': 0,
            'facet_total': 0,
            'data_type_facets': [],
            'category_facets': [],
            'schema_facets': [],
            'active_data_type': active_data_type,
            'active_category': active_category,
            'active_schema': active_schema,
            'show_images': False,
            'image_results': [],
            'image_count': 0,
            'image_pagination': None,
            'filters': filters,
            'facets': {},
            'safe_search': safe_search,
            'was_corrected': False,
            'corrected_query': params.query,
            'word_corrections': [],
            'corrections': {},
            'intent': {},
            'search_type': 'keyword' if params.is_keyword_search else 'semantic',
            'alt_mode': params.alt_mode,
            'related_searches': [],
            'suggestions': [],
            'featured': None,
            'answer': '',
            'answer_type': '',
            'pagination': None,
            'page': page,
            'per_page': per_page,
            'search_time': 0,
            'search_time_ms': 0,
            'from_cache': False,
            'from_semantic_cache': False,
            'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
            'device_type': device_type,
            'source': params.source,
            'user_city': location.get('city', '') if location else '',
            'user_country': location.get('country', '') if location else '',
            'data_type_labels': DATA_TYPE_LABELS,
            'category_labels': CATEGORY_LABELS,
            'search_source': search_source,
            'document_uuid': document_uuid,
            'is_question_path': is_question_path,
            'request_id': request_id,
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
            'show_images': False,
            'image_results': [],
            'image_count': 0,
            'image_pagination': None,
            # FIX: Added all variables the template needs to prevent VariableDoesNotExist
            'display_total': 0,
            'total_results': 0,
            'facet_total': 0,
            'filters': filters,
            'facets': {},
            'safe_search': safe_search,
            'was_corrected': False,
            'corrected_query': '',
            'word_corrections': [],
            'corrections': {},
            'intent': {},
            'search_type': 'keyword' if params.is_keyword_search else 'semantic',
            'alt_mode': params.alt_mode,
            'related_searches': [],
            'suggestions': [],
            'featured': None,
            'answer': '',
            'answer_type': '',
            'categorized_results': {},
            'pagination': None,
            'page': page,
            'per_page': per_page,
            'search_time': 0,
            'search_time_ms': 0,
            'from_cache': False,
            'from_semantic_cache': False,
            'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
            'device_type': device_type,
            'source': params.source,
            'user_city': location.get('city', '') if location else '',
            'user_country': location.get('country', '') if location else '',
            'data_type_labels': DATA_TYPE_LABELS,
            'category_labels': CATEGORY_LABELS,
            'search_source': search_source,
            'document_uuid': document_uuid,
            'is_question_path': is_question_path,
            'request_id': request_id,
        })
    
    # === 6. INITIALIZE ALL VARIABLES ===
    
    # Search result variables - initialize ALL upfront
    result = {}          # FIX: Initialize result so it's never unbound if execute_full_search fails
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
    
    # Image results - initialize upfront
    image_results = []
    image_count = 0
    image_pagination = None
    
    # Determine if we have active filters
    has_filters = bool(active_data_type or active_category or active_schema)
    
    # === 6A. GENERATE CACHE KEY ===
    # CRITICAL FIX: Include view_mode in the cache key.
    # Without this, a normal search (view_mode='') caches context with
    # image_pagination=None, and a subsequent image-view request
    # (view_mode='images') hits that same cache and gets None pagination.
    # By including view_mode, each view type gets its own cache entry.
    cache_key = get_cache_key(
        'search', params.query, page, params.alt_mode,
        active_data_type, active_category, active_schema,
        view_mode,
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
                query=params.query,
                session_id=params.session_id,
                filters=filters,
                page=page,
                per_page=per_page,
                alt_mode=params.alt_mode,
                user_location=user_location,
                pos_tags=tuple_array if params.is_semantic_search else [],
                safe_search=safe_search,
                search_source=search_source,
                answer=answer if is_question_path else None,
                answer_type=answer_type if is_question_path else None,
                document_uuid=document_uuid if is_question_path else None,
            )
            
            results = result.get('results', [])
            total_results = result.get('total', 0)
            search_time = result.get('search_time', 0)
            search_strategy = result.get('search_strategy', search_type)
            
            # === Get correction info from bridge ===
            bridge_corrected = result.get('corrected_query', params.query)
            if bridge_corrected and bridge_corrected.lower() != params.query.lower():
                corrected_query = bridge_corrected
                was_corrected = True
                word_corrections = result.get('word_discovery', {}).get('corrections', [])
            
            # Get facets from the search result
            data_type_facets = result.get('data_type_facets', [])
            category_facets = result.get('category_facets', [])
            schema_facets = result.get('schema_facets', [])
            facet_total = result.get('facet_total', 0)
            
            # Get related searches from the search result
            related_searches = result.get('related_searches', [])
            
            # =============================================================
            # 8B. IMAGE DATA
            # =============================================================
            # image_count is used to decide if Images tab appears (> 0)
            image_count = result.get('total_image_count', 0)

            if show_images:
                # Load first batch for initial render (infinite scroll loads more)
                try:
                    stable_key = _generate_stable_cache_key(params.session_id, params.query)
                    finished = _get_cached_results(stable_key)

                    if finished and finished.get('all_results'):
                        all_candidates = finished['all_results']
                        has_image = [r for r in all_candidates if _has_real_images(r)]

                        if has_image:
                            # First batch only — scroll loads more via AJAX
                            first_batch = has_image[:20]
                            page_ids = [item['id'] for item in first_batch if item.get('id')]
                            if page_ids:
                                full_docs = fetch_full_documents(page_ids, params.query)
                                image_results = extract_images_from_results(full_docs)
                            else:
                                image_results = []
                        else:
                            image_results = []
                    else:
                        image_results = extract_images_from_results(results)
                except Exception as e:
                    logger.warning(f"Image extraction error: {e}")
                    image_results = []

                # No pagination — infinite scroll handles it
                image_pagination = None
            else:
                image_results = []
                image_pagination = None

        except Exception as e:
            logger.error(f"Search execution error: {e}", exc_info=True)
            # result stays as {} from initialization — all downstream .get() calls are safe
    
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
    
    map_data = process_address_maps(request, results)
    
    # === 15. BUILD CONTEXT ===
    context = {
        # Core search
        'query': params.query,
        **map_data,
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
        # FIX: result is always a dict ({} at minimum), so .get() never raises UnboundLocalError
        'answer': result.get('answer', ''),
        'answer_type': result.get('answer_type', ''),
        
        # Image results
        'show_images': show_images,
        'image_results': image_results,
        'image_count': image_count,
        'image_pagination': image_pagination,
        
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

        # Question path tracking
        'search_source': search_source,
        'document_uuid': document_uuid,
        'is_question_path': is_question_path,
    }
    
    # === 16. CACHE RESULTS ===
    if not filters and total_results > 0:
        safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])

    if results and len(results) > 0:
        trending_city = location.get('city', '') if location else ''
        cache_trending_result(query=params.query, top_result=results[0], city=trending_city)

    # Cache at each location level
    if results and len(results) > 0:
        top_result = results[0]
        
        loc_city = location.get('city', '') if location else ''
        loc_region = location.get('region', '') if location else ''
        loc_country = location.get('country', '') if location else ''
        
        for loc_value in [loc_city, loc_region, loc_country]:
            if loc_value:
                cache_trending_result(query=params.query, top_result=top_result, city=loc_value)
        
        # Always cache to general
        cache_trending_result(query=params.query, top_result=top_result, city=None)
    
    return render(request, 'results2.html', context)

# --------------------------------------------------------- Test End -----------
@require_GET
def load_images(request):
    """
    AJAX endpoint for infinite scroll image loading.
    Returns a JSON array of image dicts for the next batch.
    """
    query = request.GET.get('q', '').strip()
    offset = int(request.GET.get('offset', 0))
    limit = int(request.GET.get('limit', 20))
    session_id = request.GET.get('session_id', '')

    if not query:
        return JsonResponse({'images': [], 'has_more': False})

    try:
        stable_key = _generate_stable_cache_key(session_id, query)
        finished = _get_cached_results(stable_key)

        if not finished or not finished.get('all_results'):
            return JsonResponse({'images': [], 'has_more': False})

        all_candidates = finished['all_results']
        has_image = [r for r in all_candidates if _has_real_images(r)]

        if offset >= len(has_image):
            return JsonResponse({'images': [], 'has_more': False})

        # Slice the batch
        batch = has_image[offset:offset + limit]
        page_ids = [item['id'] for item in batch if item.get('id')]

        if not page_ids:
            return JsonResponse({'images': [], 'has_more': False})

        # Fetch full docs and extract images
        full_docs = fetch_full_documents(page_ids, query)
        images = extract_images_from_results(full_docs)

        has_more = (offset + limit) < len(has_image)

        return JsonResponse({
            'images': images,
            'has_more': has_more,
            'next_offset': offset + limit,
        })

    except Exception as e:
        logger.warning(f"Image load error: {e}")
        return JsonResponse({'images': [], 'has_more': False})
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













# ============================================================================
# DEBUG VIEWS — debug_views.py
# ============================================================================
# In urls.py, add:
#   path('debug/search/', views.debug_search_view, name='debug_search'),
#   path('debug/word-discovery/', views.debug_word_discovery_view, name='debug_word_discovery'),
# ============================================================================

import json
import time
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods, require_GET


# ── Import word discovery ────────────────────────────────────────────────
try:
    from .word_discovery_fulltest import (
        WordDiscovery,
        vocab_cache_wrapper,
        get_fuzzy_suggestions_batch,
        damerau_levenshtein_distance,
        is_pos_compatible,
        normalize_pos_string,
        STOPWORDS,
        GRAMMAR_RULES,
        RAM_CACHE_AVAILABLE,
        vocab_cache,
    )
    WD_AVAILABLE = True
except ImportError:
    try:
        from word_discovery_fulltest import (
            WordDiscovery,
            vocab_cache_wrapper,
            get_fuzzy_suggestions_batch,
            damerau_levenshtein_distance,
            is_pos_compatible,
            normalize_pos_string,
            STOPWORDS,
            GRAMMAR_RULES,
            RAM_CACHE_AVAILABLE,
            vocab_cache,
        )
        WD_AVAILABLE = True
    except ImportError:
        WD_AVAILABLE = False


# ── Import bridge + intent ───────────────────────────────────────────────
try:
    from .typesense_discovery_bridge import (
        execute_full_search,
        run_parallel_prep,
        build_query_profile,
        fetch_candidate_uuids,
        fetch_all_candidate_uuids,
        client as typesense_client,
        COLLECTION_NAME,
    )
    BRIDGE_AVAILABLE = True
except ImportError:
    BRIDGE_AVAILABLE = False

try:
    from .intent_detect import detect_intent
    INTENT_AVAILABLE = True
except ImportError:
    try:
        from intent_detect import detect_intent
        INTENT_AVAILABLE = True
    except ImportError:
        INTENT_AVAILABLE = False


# ============================================================================
# DEBUG SEARCH VIEW — Full Pipeline Trace
# ============================================================================

@csrf_exempt
@require_http_methods(["GET", "POST"])
def debug_search_view(request):
    """
    Debug endpoint for testing the full Typesense bridge pipeline.

    GET  /debug/search/?query=When+was+Prentice+Herman+Polk+born
    POST /debug/search/  { "query": "...", "alt_mode": "y", ... }

    Returns detailed JSON showing every stage including:
    - Word discovery output with term-level detail
    - Intent signals
    - Query profile (entities, locations, field boosts)
    - Stage 1A document pool
    - Stage 1B questions pool with vector distances
    - Overlap between both pools
    - Full pipeline results with AI overview
    """
    if not BRIDGE_AVAILABLE:
        return JsonResponse({"error": "typesense_discovery_bridge not available"}, status=500)

    t0 = time.time()

    # ── Parse params ──────────────────────────────────────────────────
    if request.method == "POST":
        try:
            body = json.loads(request.body)
        except Exception:
            body = {}
        query         = body.get("query", "")
        alt_mode      = body.get("alt_mode", "y")
        page          = int(body.get("page", 1))
        per_page      = int(body.get("per_page", 5))
        session_id    = body.get("session_id", "debug-session")
        search_source = body.get("search_source", "home")
    else:
        query         = request.GET.get("query", "")
        alt_mode      = request.GET.get("alt_mode", "y")
        page          = int(request.GET.get("page", 1))
        per_page      = int(request.GET.get("per_page", 5))
        session_id    = request.GET.get("session_id", "debug-session")
        search_source = request.GET.get("search_source", "home")

    if not query:
        return JsonResponse({
            "error": "Missing 'query' parameter",
            "usage": {
                "GET":  "/debug/search/?query=When+was+Prentice+Herman+Polk+born",
                "POST": '{"query": "When was Prentice Herman Polk born?", "alt_mode": "y"}'
            }
        }, status=400)

    debug  = {}
    errors = []

    # ── Step 1: Word discovery + embedding ────────────────────────────
    discovery = {
        "query": query, "corrected_query": query,
        "terms": [], "ngrams": [], "corrections": [], "stats": {},
    }
    query_embedding = None

    try:
        t1 = time.time()
        discovery, query_embedding = run_parallel_prep(query, skip_embedding=False)
        debug["step1_parallel_prep_ms"] = round((time.time() - t1) * 1000, 2)

        debug["step1_word_discovery"] = {
            "original_query":  discovery.get("query", query),
            "corrected_query": discovery.get("corrected_query", query),
            "corrections":     discovery.get("corrections", []),
            "terms": [
                {
                    "position":      t.get("position"),
                    "word":          t.get("word"),
                    "status":        t.get("status"),
                    "pos":           t.get("pos"),
                    "predicted_pos": t.get("predicted_pos"),
                    "category":      t.get("category"),
                    "rank":          t.get("rank"),
                    "display":       t.get("display"),
                    "entity_type":   t.get("entity_type"),
                    "is_stopword":   t.get("is_stopword"),
                    "part_of_ngram": t.get("part_of_ngram"),
                    "match_count":   t.get("match_count"),
                    # Suggestion fields
                    "suggestion":          t.get("suggestion"),
                    "suggestion_display":  t.get("suggestion_display"),
                    "suggestion_distance": t.get("suggestion_distance"),
                    # Correction fields
                    "corrected":         t.get("corrected"),
                    "corrected_display": t.get("corrected_display"),
                    "distance":          t.get("distance"),
                }
                for t in discovery.get("terms", [])
            ],
            "ngrams": [
                {
                    "type":     n.get("type"),
                    "phrase":   n.get("phrase"),
                    "display":  n.get("display"),
                    "category": n.get("category"),
                    "rank":     n.get("rank"),
                    "positions": n.get("positions"),
                    "words":    n.get("words"),
                }
                for n in discovery.get("ngrams", [])
            ],
            "stats": discovery.get("stats", {}),
        }

        debug["step1_embedding"] = {
            "generated": query_embedding is not None,
            "dims":      len(query_embedding) if query_embedding else 0,
            "sample":    query_embedding[:5] if query_embedding else [],
        }

    except Exception as e:
        errors.append(f"parallel_prep: {str(e)}")

    # ── Step 2: Intent detection ──────────────────────────────────────
    signals = {}
    try:
        if INTENT_AVAILABLE:
            discovery = detect_intent(discovery)
            signals   = discovery.get("signals", {})

            debug["step2_intent_signals"] = {
                "query_mode":            signals.get("query_mode"),
                "question_word":         signals.get("question_word"),
                "has_question_word":     signals.get("has_question_word"),
                "wants_single_result":   signals.get("wants_single_result"),
                "wants_multiple_results": signals.get("wants_multiple_results"),
                "is_local_search":       signals.get("is_local_search"),
                "local_search_strength": signals.get("local_search_strength"),
                "has_location":          signals.get("has_location"),
                "has_location_entity":   signals.get("has_location_entity"),
                "has_person":            signals.get("has_person"),
                "has_organization":      signals.get("has_organization"),
                "has_media":             signals.get("has_media"),
                "has_black_owned":       signals.get("has_black_owned"),
                "has_service_word":      signals.get("has_service_word"),
                "has_product_word":      signals.get("has_product_word"),
                "has_role_word":         signals.get("has_role_word"),
                "has_superlative":       signals.get("has_superlative"),
                "has_temporal":          signals.get("has_temporal"),
                "temporal_direction":    signals.get("temporal_direction"),
                "has_price_signal":      signals.get("has_price_signal"),
                "price_direction":       signals.get("price_direction"),
                "has_negation":          signals.get("has_negation"),
                "has_unknown_terms":     signals.get("has_unknown_terms"),
                "is_comparison_query":   signals.get("is_comparison_query"),
                "is_definition_query":   signals.get("is_definition_query"),
                "action_type":           signals.get("action_type"),
                "service_words":         signals.get("service_words", []),
                "product_words":         signals.get("product_words", []),
                "domains_detected":      signals.get("domains_detected", []),
                "primary_domain":        signals.get("primary_domain"),
                "signal_count":          signals.get("signal_count"),
                "detected_entities":     signals.get("detected_entities", [])[:10],
            }
    except Exception as e:
        errors.append(f"intent_detect: {str(e)}")

    # ── Step 3: Build query profile ───────────────────────────────────
    # v3 MIGRATION NOTE: When word discovery v3 is active, the profile
    # will come directly from discovery output. build_query_profile()
    # will be removed. Check for profile in discovery first.
    profile = {
        "search_terms": query.split(), "cities": [], "states": [],
        "field_boosts": {}, "location_terms": [],
    }

    try:
        t3 = time.time()

        # v3 path: profile already in discovery output
        if "search_terms" in discovery and "cities" in discovery and "field_boosts" in discovery:
            profile = discovery
            debug["step3_source"] = "word_discovery_v3_profile"
        else:
            # v2 path: bridge builds profile
            profile = build_query_profile(discovery, signals=signals)
            debug["step3_source"] = "bridge_build_query_profile"

        debug["step3_build_profile_ms"] = round((time.time() - t3) * 1000, 2)

        debug["step3_query_profile"] = {
            "primary_intent":   profile.get("primary_intent"),
            "search_terms":     profile.get("search_terms", []),
            "location_terms":   profile.get("location_terms", []),
            "cities":           [c.get("name") if isinstance(c, dict) else c
                                 for c in profile.get("cities", [])],
            "states":           [s.get("name") if isinstance(s, dict) else s
                                 for s in profile.get("states", [])],
            "persons": [
                p.get("phrase") or p.get("word")
                for p in profile.get("persons", [])
            ],
            "organizations": [
                o.get("phrase") or o.get("word")
                for o in profile.get("organizations", [])
            ],
            "keywords": [
                k.get("phrase") or k.get("word")
                for k in profile.get("keywords", [])
            ],
            "media": [
                m.get("phrase") or m.get("word")
                for m in profile.get("media", [])
            ],
            "has_person":       profile.get("has_person"),
            "has_organization": profile.get("has_organization"),
            "has_location":     profile.get("has_location"),
            "has_keyword":      profile.get("has_keyword"),
            "has_media":        profile.get("has_media"),
            "field_boosts":     profile.get("field_boosts", {}),
            "intent_scores": {
                "person":       profile.get("person_score", 0),
                "organization": profile.get("organization_score", 0),
                "location":     profile.get("location_score", 0),
                "keyword":      profile.get("keyword_score", 0),
                "media":        profile.get("media_score", 0),
            },
        }

    except Exception as e:
        errors.append(f"build_profile: {str(e)}")

    corrected_query = discovery.get("corrected_query", query)

    # ── Step 4A: Stage 1A — document collection ───────────────────────
    doc_uuids = []
    try:
        t4a = time.time()
        doc_uuids = fetch_candidate_uuids(
            corrected_query, profile, signals=signals, max_results=100
        )
        debug["step4a_stage1_document_ms"] = round((time.time() - t4a) * 1000, 2)
        debug["step4a_stage1_document"] = {
            "count":        len(doc_uuids),
            "uuids_sample": doc_uuids[:10],
        }
    except Exception as e:
        errors.append(f"stage1a: {str(e)}")
        debug["step4a_error"] = str(e)

    # ── Step 4B: Stage 1B — questions collection ──────────────────────
    q_debug = {}
    q_uuids = []
    try:
        t4b = time.time()
        q_debug = _fetch_questions_debug(
            profile, query_embedding, signals=signals, discovery=discovery, max_results=50
        )
        q_uuids = q_debug.get("uuids", [])
        debug["step4b_stage1_questions_ms"] = round((time.time() - t4b) * 1000, 2)
        debug["step4b_stage1_questions"] = {
            "filter_used":          q_debug.get("filter_used"),
            "filter_parts":         q_debug.get("filter_parts", []),
            "location_filter":      q_debug.get("location_filter", "none"),
            "used_fallback":        q_debug.get("used_fallback", False),
            "primary_kws_used":     q_debug.get("primary_kws_used", []),
            "entity_names_used":    q_debug.get("entity_names_used", []),
            "semantic_kws_used":    q_debug.get("semantic_kws_used", []),
            "question_type_filter": q_debug.get("question_type_filter"),
            "total_hits":           q_debug.get("total_hits", 0),
            "accepted":             q_debug.get("accepted", 0),
            "rejected":             q_debug.get("rejected", 0),
            "unique_doc_uuids":     q_debug.get("unique_doc_uuids", 0),
            "matched_questions":    q_debug.get("matched_questions", []),
            "rejected_questions":   q_debug.get("rejected_questions", [])[:10],
            "error":                q_debug.get("error"),
        }
    except Exception as e:
        errors.append(f"stage1b: {str(e)}")
        debug["step4b_error"] = str(e)

    # ── Step 4C: Overlap analysis ─────────────────────────────────────
    try:
        doc_set = set(doc_uuids)
        q_set   = set(q_uuids)
        overlap = doc_set & q_set
        doc_only = doc_set - q_set
        q_only   = q_set - doc_set

        debug["step4c_overlap"] = {
            "document_pool":      len(doc_uuids),
            "questions_pool":     len(q_uuids),
            "overlap_count":      len(overlap),
            "overlap_uuids":      list(overlap)[:10],
            "document_only_count": len(doc_only),
            "questions_only_count": len(q_only),
            "total_merged":       len(doc_set | q_set),
            "merge_order":        "overlap first → document-only → questions-only",
            "interpretation": (
                "HIGH CONFIDENCE — same docs found by both paths"
                if len(overlap) >= 3 else
                "MODERATE — some overlap between paths"
                if overlap else
                "NO OVERLAP — paths found different documents"
            ),
        }
    except Exception as e:
        errors.append(f"overlap: {str(e)}")

    # ── Step 5: Full pipeline ─────────────────────────────────────────
    full_result = {}
    try:
        t5 = time.time()
        full_result = execute_full_search(
            query=query,
            session_id=session_id,
            filters={},
            page=page,
            per_page=per_page,
            alt_mode=alt_mode,
            search_source=search_source,
        )
        debug["step5_full_pipeline_ms"] = round((time.time() - t5) * 1000, 2)
        debug["step5_search_strategy"] = full_result.get("search_strategy")
        debug["step5_semantic_enabled"] = full_result.get("semantic_enabled")
    except Exception as e:
        errors.append(f"execute_full_search: {str(e)}")
        debug["step5_error"] = str(e)

    # ── Build results summary ─────────────────────────────────────────
    results_summary = []
    for i, r in enumerate(full_result.get("results", [])):
        result_entry = {
            "rank":              i + 1,
            "title":             r.get("title"),
            "url":               r.get("url"),
            "data_type":         r.get("data_type"),
            "category":          r.get("category"),
            "schema":            r.get("schema"),
            "authority_score":   r.get("authority_score"),
            "semantic_score":    r.get("semantic_score"),
            "date":              r.get("date"),
            "source":            r.get("source"),
            "location": {
                "city":    r.get("location", {}).get("city"),
                "state":   r.get("location", {}).get("state"),
                "country": r.get("location", {}).get("country"),
            },
            "key_facts_count":   len(r.get("key_facts", [])),
            "key_facts":         r.get("key_facts", [])[:5],
            "has_image":         bool(r.get("image")),
        }

        if r.get("humanized_summary"):
            result_entry["ai_overview"] = r["humanized_summary"]

        results_summary.append(result_entry)

    # ── Final response ────────────────────────────────────────────────
    return JsonResponse({
        "meta": {
            "query":            query,
            "corrected_query":  full_result.get("corrected_query", corrected_query),
            "query_mode":       full_result.get("query_mode"),
            "intent":           full_result.get("intent"),
            "search_strategy":  full_result.get("search_strategy"),
            "semantic_enabled": full_result.get("semantic_enabled"),
            "alt_mode":         alt_mode,
            "search_source":    search_source,
            "total_ms":         round((time.time() - t0) * 1000, 2),
        },
        "counts": {
            "total":             full_result.get("total", 0),
            "facet_total":       full_result.get("facet_total", 0),
            "total_images":      full_result.get("total_image_count", 0),
            "results_this_page": len(full_result.get("results", [])),
        },
        "facets": {
            "data_types":  full_result.get("data_type_facets", []),
            "categories":  full_result.get("category_facets", []),
            "schemas":     full_result.get("schema_facets", []),
        },
        "results":         results_summary,
        "pipeline_debug":  debug,
        "timings":         full_result.get("timings", {}),
        "filters_applied": full_result.get("filters_applied", {}),
        "valid_terms":     full_result.get("valid_terms", []),
        "unknown_terms":   full_result.get("unknown_terms", []),
        "word_discovery":  full_result.get("word_discovery", {}),
        "signals":         full_result.get("signals", {}),
        "errors":          errors if errors else None,
    }, json_dumps_params={"indent": 2})


# ============================================================================
# QUESTIONS DEBUG HELPER — Stage 1B with validation trace
# ============================================================================

def _fetch_questions_debug(
    profile: dict,
    query_embedding: list,
    signals: dict = None,
    discovery: dict = None,
    max_results: int = 50,
) -> dict:
    """
    Stage 1B with full debug output including validation trace.

    Returns matched questions with vector distance, question text,
    answer, question_type, the document_uuid they resolve to,
    and validation accept/reject reasoning.
    """
    if not BRIDGE_AVAILABLE:
        return {"error": "bridge not available", "uuids": [], "matched_questions": []}

    signals = signals or {}

    if not query_embedding:
        return {"error": "no embedding", "uuids": [], "matched_questions": [], "filter_used": ""}

    # ── Import validation function ────────────────────────────────────
    try:
        from .typesense_discovery_bridge import (
            _extract_query_signals,
            _validate_question_hit,
            _normalize_signal,
        )
        VALIDATION_AVAILABLE = True
    except ImportError:
        VALIDATION_AVAILABLE = False

    # ── Build facet filter ────────────────────────────────────────────
    filter_parts = []

    primary_kws = profile.get("primary_keywords", [])
    if not primary_kws:
        primary_kws = [
            k.get("phrase") or k.get("word", "")
            for k in profile.get("keywords", [])
        ]
    primary_kws = [kw for kw in primary_kws if kw][:3]
    if primary_kws:
        kw_values = ",".join([f"`{kw}`" for kw in primary_kws])
        filter_parts.append(f"primary_keywords:[{kw_values}]")

    entity_names = []
    for p in profile.get("persons", []):
        name = p.get("phrase") or p.get("word", "")
        rank = p.get("rank", 0)
        if name and (" " in name or rank > 100):
            entity_names.append(name)
    for o in profile.get("organizations", []):
        name = o.get("phrase") or o.get("word", "")
        rank = o.get("rank", 0)
        if name and (" " in name or rank > 100):
            entity_names.append(name)
    entity_names = [e for e in entity_names if e][:3]
    if entity_names:
        ent_values = ",".join([f"`{e}`" for e in entity_names])
        filter_parts.append(f"entities:[{ent_values}]")

    semantic_kws = profile.get("semantic_keywords", [])
    semantic_kws = [kw for kw in semantic_kws if kw][:3]
    if semantic_kws:
        sem_values = ",".join([f"`{kw}`" for kw in semantic_kws])
        filter_parts.append(f"semantic_keywords:[{sem_values}]")

    question_word = signals.get("question_word") or ""
    question_type_map = {
        "when": "TEMPORAL", "where": "LOCATION", "who": "PERSON",
        "what": "FACTUAL", "which": "FACTUAL", "why": "REASON", "how": "PROCESS",
    }
    question_type = question_type_map.get(question_word.lower(), "")
    if question_type:
        filter_parts.append(f"question_type:={question_type}")

    # ── Location filter ───────────────────────────────────────────────
    location_filter_parts = []

    query_mode = signals.get("query_mode", "explore")
    is_location_subject = (
        query_mode == "answer"
        and signals.get("has_question_word")
        and signals.get("question_word") in ("where",)
        and signals.get("has_location_entity", False)
    )

    if not is_location_subject:
        cities = profile.get("cities", [])
        if cities:
            city_filters = [f"location_city:=`{c['name']}`" for c in cities]
            if len(city_filters) == 1:
                location_filter_parts.append(city_filters[0])
            else:
                location_filter_parts.append("(" + " || ".join(city_filters) + ")")

        states = profile.get("states", [])
        if states:
            state_conditions = []
            for state in states:
                variants = state.get("variants", [state["name"]])
                for variant in variants:
                    state_conditions.append(f"location_state:=`{variant}`")
            if len(state_conditions) == 1:
                location_filter_parts.append(state_conditions[0])
            else:
                location_filter_parts.append("(" + " || ".join(state_conditions) + ")")

    facet_filter = " || ".join(filter_parts) if filter_parts else ""
    location_filter = " && ".join(location_filter_parts) if location_filter_parts else ""

    if facet_filter and location_filter:
        filter_str = f"({facet_filter}) && {location_filter}"
    elif location_filter:
        filter_str = location_filter
    else:
        filter_str = facet_filter

    # ── Extract validation signals ────────────────────────────────────
    query_tokens, query_phrases, primary_subject = set(), [], None
    if VALIDATION_AVAILABLE:
        try:
            query_tokens, query_phrases, primary_subject = _extract_query_signals(
                profile, discovery=discovery
            )
        except Exception:
            pass

    # ── Vector search ─────────────────────────────────────────────────
    embedding_str = ",".join(str(x) for x in query_embedding)

    search_params = {
        "q":              "*",
        "vector_query":   f"embedding:([{embedding_str}], k:{max_results * 2})",
        "per_page":       max_results * 2,
        "include_fields": "question_id,question,answer,answer_type,question_type,"
                          "document_uuid,semantic_uuid,primary_keywords,entities,"
                          "semantic_keywords,authority_rank_score",
    }
    if filter_str:
        search_params["filter_by"] = filter_str

    try:
        search_requests = {"searches": [{"collection": "questions", **search_params}]}
        response = typesense_client.multi_search.perform(search_requests, {})
        result = response["results"][0]
        hits = result.get("hits", [])

        # Fallback logic
        used_fallback = None
        if len(hits) < 5 and filter_str and location_filter:
            fallback_filter = facet_filter if facet_filter else ""
            search_params_fb = dict(search_params)
            if fallback_filter:
                search_params_fb["filter_by"] = fallback_filter
            else:
                search_params_fb.pop("filter_by", None)

            search_requests_fb = {"searches": [{"collection": "questions", **search_params_fb}]}
            response_fb = typesense_client.multi_search.perform(search_requests_fb, {})
            fallback_hits = response_fb["results"][0].get("hits", [])

            if len(fallback_hits) >= 5:
                hits = fallback_hits
                used_fallback = "facet_only"
            else:
                search_params_nf = dict(search_params)
                search_params_nf.pop("filter_by", None)
                search_requests_nf = {"searches": [{"collection": "questions", **search_params_nf}]}
                response_nf = typesense_client.multi_search.perform(search_requests_nf, {})
                hits = response_nf["results"][0].get("hits", [])
                used_fallback = "no_filter"

        # ── Process hits with validation ──────────────────────────────
        matched_questions = []
        rejected_questions = []
        uuids = []
        seen = set()
        accepted_count = 0
        rejected_count = 0

        for hit in hits:
            doc = hit.get("document", {})
            uuid = doc.get("document_uuid")
            vector_distance = hit.get("vector_distance", 1.0)
            semantic_score = round(1 - vector_distance, 4)

            question_entry = {
                "question":            doc.get("question", ""),
                "answer":              doc.get("answer", ""),
                "answer_type":         doc.get("answer_type", ""),
                "question_type":       doc.get("question_type", ""),
                "document_uuid":       uuid,
                "semantic_uuid":       doc.get("semantic_uuid", ""),
                "vector_distance":     round(vector_distance, 4),
                "semantic_score":      semantic_score,
                "confidence":          (
                    "HIGH" if vector_distance < 0.25 else
                    "MEDIUM" if vector_distance < 0.45 else
                    "LOW"
                ),
                "primary_keywords":    doc.get("primary_keywords", [])[:5],
                "entities":            doc.get("entities", [])[:5],
                "semantic_keywords":   doc.get("semantic_keywords", [])[:5],
                "authority_rank_score": doc.get("authority_rank_score", 0),
            }

            # Validate
            is_valid = True
            if VALIDATION_AVAILABLE and query_tokens:
                try:
                    is_valid = _validate_question_hit(
                        hit_doc=doc,
                        query_tokens=query_tokens,
                        query_phrases=query_phrases,
                        primary_subject=primary_subject,
                        min_matches=1,
                    )
                except Exception:
                    is_valid = True

            question_entry["validated"] = is_valid

            if is_valid:
                accepted_count += 1
                matched_questions.append(question_entry)
                if uuid and uuid not in seen:
                    seen.add(uuid)
                    uuids.append(uuid)
            else:
                rejected_count += 1
                question_entry["rejection_reason"] = "failed signal validation"
                rejected_questions.append(question_entry)

            if len(uuids) >= max_results:
                break

        return {
            "filter_used":          filter_str or "none (full scan)",
            "filter_parts":         filter_parts,
            "location_filter":      location_filter or "none",
            "used_fallback":        used_fallback,
            "primary_kws_used":     primary_kws,
            "entity_names_used":    entity_names,
            "semantic_kws_used":    semantic_kws,
            "question_type_filter": question_type or "none",
            "validation_signals": {
                "query_tokens":    sorted(query_tokens) if query_tokens else [],
                "query_phrases":   query_phrases,
                "primary_subject": sorted(primary_subject) if primary_subject else None,
            },
            "total_hits":           len(hits),
            "accepted":             accepted_count,
            "rejected":             rejected_count,
            "unique_doc_uuids":     len(uuids),
            "uuids":                uuids,
            "matched_questions":    matched_questions,
            "rejected_questions":   rejected_questions,
        }

    except Exception as e:
        return {
            "error":             str(e),
            "filter_used":       filter_str,
            "location_filter":   location_filter or "none",
            "uuids":             [],
            "matched_questions": [],
            "rejected_questions": [],
        }


# ============================================================================
# DEBUG WORD DISCOVERY VIEW — Step-by-Step Trace
# ============================================================================

@require_GET
def debug_word_discovery_view(request):
    """
    Debug endpoint that traces every step of word discovery.

    GET /debug/word-discovery/?q=restuarants+in+atlanta

    Returns step-by-step breakdown:
    - Step 1: RAM hash lookup per word
    - Step 2: N-gram detection with rank comparisons
    - Step 3: POS prediction with grammar rules
    - Step 3.5: Suffix refinement
    - Step 4: Best match selection
    - Step 5: Unknown word correction (Redis fuzzy)
    - Step 6-7: Final output with profile
    """
    query = request.GET.get("q", "").strip()

    if not query:
        return JsonResponse({
            "error": "Missing ?q= parameter",
            "usage": "/debug/word-discovery/?q=restuarants+in+atlanta",
        }, status=400)

    if not WD_AVAILABLE:
        return JsonResponse({"error": "word_discovery_fulltest not available"}, status=500)

    trace = {
        "query": query,
        "ram_cache_available": RAM_CACHE_AVAILABLE,
        "ram_cache_loaded": bool(vocab_cache and vocab_cache.loaded) if vocab_cache else False,
        "steps": {},
    }

    t0 = time.perf_counter()

    # ── Tokenize ──────────────────────────────────────────────────────
    words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
    words = [w for w in words if w]
    trace["tokens"] = words

    cache = vocab_cache_wrapper

    # ================================================================
    # STEP 1: RAM Cache Lookup
    # ================================================================
    step1 = []

    for i, word in enumerate(words):
        word_lower = word.lower().strip()
        entry = {"position": i + 1, "word": word_lower}

        if word_lower in STOPWORDS:
            entry["result"] = "STOPWORD"
            entry["pos"] = STOPWORDS[word_lower]
            entry["matches"] = []
            step1.append(entry)
            continue

        matches = cache.get_term_matches(word_lower)

        if matches:
            entry["result"] = "FOUND"
            entry["match_count"] = len(matches)
            entry["matches"] = [
                {
                    "term":        m.get("term", ""),
                    "display":     m.get("display", ""),
                    "category":    m.get("category", ""),
                    "pos":         m.get("pos", ""),
                    "rank":        m.get("rank", 0),
                    "entity_type": m.get("entity_type", ""),
                }
                for m in matches
            ]
            entry["best_by_rank"] = {
                "term":     matches[0].get("term", ""),
                "category": matches[0].get("category", ""),
                "rank":     matches[0].get("rank", 0),
            }
        else:
            entry["result"] = "NOT_IN_RAM"
            entry["match_count"] = 0
            entry["matches"] = []
            nearby = _find_nearby_in_ram(cache, word_lower)
            if nearby:
                entry["nearby_in_ram"] = nearby[:5]

        step1.append(entry)

    trace["steps"]["step1_ram_lookup"] = step1

    # ================================================================
    # STEP 2: N-gram Detection (with rank comparison trace)
    # ================================================================
    step2 = {"checks": [], "ngrams_found": [], "consumed_positions": []}

    consumed = set()

    for n in [4, 3, 2]:
        if len(words) < n:
            continue
        for i in range(len(words) - n + 1):
            if any(p in consumed for p in range(i, i + n)):
                continue

            chunk = words[i:i + n]
            ngram_type = {4: "quadgram", 3: "trigram", 2: "bigram"}[n]

            result = cache.get_ngram(chunk)

            check_entry = {
                "type":     ngram_type,
                "words":    chunk,
                "positions": list(range(i + 1, i + n + 1)),
                "found":    result is not None,
            }

            if result:
                ngram_rank = result.get("rank", 0)
                ngram_category = result.get("category", "")

                # Get individual word best ranks for comparison
                individual_ranks = []
                for j in range(i, i + n):
                    word_matches = step1[j].get("matches", [])
                    best_rank = word_matches[0].get("rank", 0) if word_matches else 0
                    individual_ranks.append({
                        "word": words[j],
                        "best_rank": best_rank,
                    })

                max_individual = max(ir["best_rank"] for ir in individual_ranks)

                check_entry["ngram_rank"] = ngram_rank
                check_entry["ngram_category"] = ngram_category
                check_entry["ngram_display"] = result.get("display", "")
                check_entry["individual_ranks"] = individual_ranks
                check_entry["max_individual_rank"] = max_individual
                check_entry["rank_comparison"] = (
                    f"ngram({ngram_rank}) vs max_individual({max_individual})"
                )

                # v2 behavior: auto-consume (always wins)
                # v3 will compare ranks here
                check_entry["consumed"] = True
                check_entry["reason"] = "v2: auto-consume (v3 will compare ranks)"

                positions = list(range(i, i + n))
                consumed.update(positions)

                step2["ngrams_found"].append({
                    "type":     ngram_type,
                    "words":    chunk,
                    "positions": list(range(i + 1, i + n + 1)),
                    "term":     result.get("term", ""),
                    "display":  result.get("display", ""),
                    "category": ngram_category,
                    "pos":      normalize_pos_string(result.get("pos", "")),
                    "rank":     ngram_rank,
                })

            step2["checks"].append(check_entry)

    step2["consumed_positions"] = sorted(p + 1 for p in consumed)
    trace["steps"]["step2_ngrams"] = step2

    # ================================================================
    # STEP 3: POS Prediction
    # ================================================================
    step3 = []

    for i, word_info in enumerate(step1):
        word = word_info["word"]
        pos_entry = {"position": i + 1, "word": word}

        if word_info["result"] == "STOPWORD":
            pos_entry["predicted_pos"] = word_info["pos"]
            pos_entry["source"] = "stopword"
            step3.append(pos_entry)
            continue

        # Get left neighbor POS
        left_pos = "start" if i == 0 else None
        if left_pos is None:
            for j in range(i - 1, -1, -1):
                prev = step1[j]
                if prev["result"] == "STOPWORD":
                    left_pos = prev["pos"]
                    break
                elif prev["matches"]:
                    left_pos = normalize_pos_string(prev["matches"][0].get("pos", ""))
                    break

        # Get right neighbor POS
        right_pos = "end" if i == len(step1) - 1 else None
        if right_pos is None:
            for j in range(i + 1, len(step1)):
                nxt = step1[j]
                if nxt["result"] == "STOPWORD":
                    right_pos = nxt["pos"]
                    break
                elif nxt["matches"]:
                    right_pos = normalize_pos_string(nxt["matches"][0].get("pos", ""))
                    break

        pos_entry["left_neighbor"] = left_pos
        pos_entry["right_neighbor"] = right_pos
        pos_entry["context"] = f"[{left_pos or '???'}] _{word}_ [{right_pos or '???'}]"

        predicted = None
        rule_used = None

        if left_pos and right_pos:
            predicted = GRAMMAR_RULES.get((left_pos, right_pos))
            if predicted:
                rule_used = f"({left_pos}, {right_pos})"

        if not predicted and left_pos:
            predicted = GRAMMAR_RULES.get((left_pos, None))
            if predicted:
                rule_used = f"({left_pos}, None)"

        if not predicted and right_pos:
            predicted = GRAMMAR_RULES.get((None, right_pos))
            if predicted:
                rule_used = f"(None, {right_pos})"

        if not predicted:
            predicted = [("noun", 0.75)]
            rule_used = "default (no rule matched)"

        pos_entry["predicted_pos_list"] = predicted
        pos_entry["predicted_pos"] = predicted[0][0] if predicted else "noun"
        pos_entry["rule_used"] = rule_used

        step3.append(pos_entry)

    trace["steps"]["step3_pos_prediction"] = step3

    # ================================================================
    # STEP 3.5: Suffix Refinement
    # ================================================================
    step3_5 = []

    # SUFFIX_POS_RULES etc. are defined inline in the class body.
    # Access via an instance to be safe, with fallback.
    try:
        _wd_instance = WordDiscovery(verbose=False)
        SUFFIX_POS_RULES = _wd_instance.SUFFIX_POS_RULES
        SUFFIX_EXCEPTIONS = _wd_instance.SUFFIX_EXCEPTIONS
        MIN_LEN = _wd_instance.MIN_SUFFIX_WORD_LENGTH
    except AttributeError:
        SUFFIX_POS_RULES = {}
        SUFFIX_EXCEPTIONS = frozenset()
        MIN_LEN = 4

    sorted_suffixes = sorted(SUFFIX_POS_RULES.keys(), key=len, reverse=True) if SUFFIX_POS_RULES else []

    for i, word_info in enumerate(step1):
        word = word_info["word"]
        if word_info["result"] == "STOPWORD":
            continue

        suffix_entry = {"position": i + 1, "word": word}

        if len(word) < MIN_LEN:
            suffix_entry["result"] = "too_short"
            step3_5.append(suffix_entry)
            continue

        if word in SUFFIX_EXCEPTIONS:
            suffix_entry["result"] = "exception"
            step3_5.append(suffix_entry)
            continue

        matched_suffix = None
        for suffix in sorted_suffixes:
            if word.endswith(suffix) and len(word) > len(suffix) + 1:
                matched_suffix = suffix
                break

        if matched_suffix:
            suffix_entry["matched_suffix"] = matched_suffix
            suffix_entry["suffix_predictions"] = SUFFIX_POS_RULES[matched_suffix]
            suffix_entry["result"] = "refined"
        else:
            suffix_entry["result"] = "no_suffix_match"

        step3_5.append(suffix_entry)

    trace["steps"]["step3_5_suffix"] = step3_5

    # ================================================================
    # STEP 4: Best Match Selection
    # ================================================================
    step4 = []

    for i, word_info in enumerate(step1):
        word = word_info["word"]
        if word_info["result"] == "STOPWORD":
            continue

        match_entry = {"position": i + 1, "word": word}

        if word_info["result"] == "NOT_IN_RAM":
            match_entry["result"] = "UNKNOWN — deferred to Step 5"
            match_entry["selected"] = None
            step4.append(match_entry)
            continue

        matches = word_info["matches"]
        predicted_pos = step3[i]["predicted_pos"] if i < len(step3) else "noun"
        match_entry["predicted_pos"] = predicted_pos

        compatible = []
        incompatible = []
        for m in matches:
            match_pos = normalize_pos_string(m["pos"])
            if is_pos_compatible(match_pos, predicted_pos):
                compatible.append(m)
            else:
                incompatible.append(m)

        match_entry["compatible_count"] = len(compatible)
        match_entry["incompatible_count"] = len(incompatible)
        match_entry["incompatible"] = [
            f"{m['category']}({normalize_pos_string(m['pos'])})" for m in incompatible
        ]

        if compatible:
            compatible.sort(key=lambda x: x["rank"], reverse=True)
            match_entry["selected"] = compatible[0]
            match_entry["result"] = "SELECTED (POS compatible)"
        elif matches:
            match_entry["selected"] = matches[0]
            match_entry["result"] = "FALLBACK (no POS match, using highest rank)"
        else:
            match_entry["selected"] = None
            match_entry["result"] = "NO MATCHES"

        step4.append(match_entry)

    trace["steps"]["step4_match_selection"] = step4

    # ================================================================
    # STEP 5: Unknown Word Correction (Redis Fuzzy)
    # ================================================================
    step5 = {
        "unknowns": [],
        "words_sent_to_redis": [],
        "corrections": [],
        "outcome": None,
    }

    unknown_words = []
    for i, word_info in enumerate(step1):
        if word_info["result"] == "NOT_IN_RAM":
            predicted_pos = step3[i]["predicted_pos"] if i < len(step3) else "noun"
            unknown_words.append({
                "position": i + 1,
                "word": word_info["word"],
                "predicted_pos": predicted_pos,
            })

    step5["unknowns"] = unknown_words

    if unknown_words:
        words_to_fetch = [u["word"] for u in unknown_words]
        step5["words_sent_to_redis"] = words_to_fetch

        try:
            t_redis = time.perf_counter()
            batch_results = get_fuzzy_suggestions_batch(
                words_to_fetch, limit=10, max_distance=2
            )
            step5["redis_time_ms"] = round((time.perf_counter() - t_redis) * 1000, 2)

            for u in unknown_words:
                word = u["word"]
                predicted_pos = u["predicted_pos"]
                suggestions = batch_results.get(word.lower().strip(), [])

                word_result = {
                    "word": word,
                    "position": u["position"],
                    "predicted_pos": predicted_pos,
                    "total_suggestions": len(suggestions),
                    "suggestions": [
                        {
                            "term":           s["term"],
                            "category":       s.get("category", ""),
                            "pos":            normalize_pos_string(s.get("pos", "unknown")),
                            "rank":           s.get("rank", 0),
                            "distance":       s.get("distance", 99),
                            "pos_compatible": is_pos_compatible(
                                normalize_pos_string(s.get("pos", "unknown")),
                                predicted_pos,
                            ),
                        }
                        for s in suggestions[:10]
                    ],
                }

                compatible_suggestions = [
                    s for s in suggestions
                    if is_pos_compatible(
                        normalize_pos_string(s.get("pos", "unknown")),
                        predicted_pos,
                    )
                ]

                if compatible_suggestions:
                    best = compatible_suggestions[0]
                    word_result["best_compatible"] = {
                        "term":     best["term"],
                        "display":  best.get("display", best["term"]),
                        "distance": best.get("distance", 99),
                        "rank":     best.get("rank", 0),
                        "pos":      normalize_pos_string(best.get("pos", "")),
                        "category": best.get("category", ""),
                    }
                    word_result["outcome"] = (
                        f"SUGGESTION: '{best['term']}' "
                        f"(distance={best.get('distance', '?')}, rank={best.get('rank', 0)}, "
                        f"pos={normalize_pos_string(best.get('pos', ''))})"
                    )
                elif suggestions:
                    best = suggestions[0]
                    word_result["best_any"] = {
                        "term":     best["term"],
                        "distance": best.get("distance", 99),
                        "rank":     best.get("rank", 0),
                        "pos":      normalize_pos_string(best.get("pos", "")),
                    }
                    word_result["outcome"] = (
                        f"SUGGESTION (no POS match): '{best['term']}' "
                        f"(distance={best.get('distance', '?')})"
                    )
                else:
                    word_result["outcome"] = "NO SUGGESTIONS — word stays as-is"

                step5["corrections"].append(word_result)

        except Exception as e:
            step5["redis_error"] = str(e)
    else:
        step5["outcome"] = "No unknown words — Step 5 skipped"

    trace["steps"]["step5_corrections"] = step5

    # ================================================================
    # STEP 6-7: Full pipeline output
    # ================================================================
    try:
        wd = WordDiscovery(verbose=False)
        t_full = time.perf_counter()
        full_output = wd.process(query)
        full_time = (time.perf_counter() - t_full) * 1000

        trace["steps"]["step7_final_output"] = {
            "corrected_query":    full_output.get("corrected_query", ""),
            "processing_time_ms": round(full_time, 2),
            "stats":              full_output.get("stats", {}),
            "corrections":        full_output.get("corrections", []),
            "terms": [
                {
                    "position":      t.get("position"),
                    "word":          t.get("word"),
                    "status":        t.get("status"),
                    "pos":           t.get("pos"),
                    "predicted_pos": t.get("predicted_pos"),
                    "category":      t.get("category", ""),
                    "rank":          t.get("rank", 0),
                    "display":       t.get("display", ""),
                    "entity_type":   t.get("entity_type", ""),
                    "is_stopword":   t.get("is_stopword", False),
                    "part_of_ngram": t.get("part_of_ngram", False),
                    "match_count":   t.get("match_count", 0),
                    "suggestion":          t.get("suggestion"),
                    "suggestion_display":  t.get("suggestion_display"),
                    "suggestion_distance": t.get("suggestion_distance"),
                    "corrected":         t.get("corrected"),
                    "corrected_display": t.get("corrected_display"),
                    "distance":          t.get("distance"),
                }
                for t in full_output.get("terms", [])
            ],
            "ngrams": full_output.get("ngrams", []),
        }

        # v3 MIGRATION: Show profile if present in output
        if "search_terms" in full_output and "cities" in full_output:
            trace["steps"]["step7_v3_profile"] = {
                "search_terms":  full_output.get("search_terms", []),
                "cities":        full_output.get("cities", []),
                "states":        full_output.get("states", []),
                "persons":       full_output.get("persons", []),
                "organizations": full_output.get("organizations", []),
                "keywords":      full_output.get("keywords", []),
                "media":         full_output.get("media", []),
                "primary_intent": full_output.get("primary_intent"),
                "field_boosts":  full_output.get("field_boosts", {}),
            }

    except Exception as e:
        trace["steps"]["step7_final_output"] = {"error": str(e)}

    # ── Intent detection trace ────────────────────────────────────────
    if INTENT_AVAILABLE:
        try:
            full_output_copy = dict(full_output) if 'full_output' in dir() else {}
            if full_output_copy:
                intent_result = detect_intent(full_output_copy)
                sigs = intent_result.get("signals", {})
                trace["steps"]["intent_detection"] = {
                    "query_mode":            sigs.get("query_mode"),
                    "question_word":         sigs.get("question_word"),
                    "wants_single_result":   sigs.get("wants_single_result"),
                    "wants_multiple_results": sigs.get("wants_multiple_results"),
                    "is_local_search":       sigs.get("is_local_search"),
                    "local_search_strength": sigs.get("local_search_strength"),
                    "has_location":          sigs.get("has_location"),
                    "has_person":            sigs.get("has_person"),
                    "has_organization":      sigs.get("has_organization"),
                    "has_service_word":      sigs.get("has_service_word"),
                    "has_product_word":      sigs.get("has_product_word"),
                    "has_superlative":       sigs.get("has_superlative"),
                    "has_temporal":          sigs.get("has_temporal"),
                    "temporal_direction":    sigs.get("temporal_direction"),
                    "has_price_signal":      sigs.get("has_price_signal"),
                    "has_black_owned":       sigs.get("has_black_owned"),
                    "has_unknown_terms":     sigs.get("has_unknown_terms"),
                    "domains_detected":      sigs.get("domains_detected", []),
                    "primary_domain":        sigs.get("primary_domain"),
                    "signal_count":          sigs.get("signal_count"),
                    "detected_entities":     sigs.get("detected_entities", [])[:8],
                }
        except Exception as e:
            trace["steps"]["intent_detection"] = {"error": str(e)}

    total_time = (time.perf_counter() - t0) * 1000
    trace["total_debug_time_ms"] = round(total_time, 2)

    return JsonResponse(trace, json_dumps_params={"indent": 2})


# ============================================================================
# HELPERS
# ============================================================================

def _find_nearby_in_ram(cache, word: str) -> list:
    """
    Generate common typo variants and check RAM cache.
    Helps debug output show what IS in the cache near a misspelling.
    """
    nearby = []
    variants = set()

    # Adjacent transpositions
    for i in range(len(word) - 1):
        swapped = list(word)
        swapped[i], swapped[i + 1] = swapped[i + 1], swapped[i]
        variants.add("".join(swapped))

    # Single character deletions
    for i in range(len(word)):
        variants.add(word[:i] + word[i + 1:])

    variants.discard(word)

    for variant in sorted(variants):
        variant_matches = cache.get_term_matches(variant)
        if variant_matches:
            nearby.append({
                "variant":  variant,
                "distance": damerau_levenshtein_distance(word, variant),
                "top_match": {
                    "term":     variant_matches[0].get("term", ""),
                    "category": variant_matches[0].get("category", ""),
                    "rank":     variant_matches[0].get("rank", 0),
                },
            })

    nearby.sort(key=lambda x: x["distance"])
    return nearby