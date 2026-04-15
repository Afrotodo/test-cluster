# # ============================================================
# # views.py — IMPORTS SECTION
# # Replace everything above your first view function with this
# # ============================================================
 
# import hashlib
# import json
# import logging
# import re
# import time
# import uuid
# import traceback
# from datetime import date, datetime, timedelta, timezone
# from functools import wraps
# from typing import Any, Dict, List, Optional, Tuple, Union
# from concurrent.futures import ThreadPoolExecutor
 
# from django.views.decorators.csrf import csrf_exempt
# from django.conf import settings
# from django.core.cache import cache
# from django.http import Http404, HttpResponseBadRequest, JsonResponse
# from django.shortcuts import redirect, render
# from django.utils.html import escape
# from django.views.decorators.http import require_GET, require_http_methods
 
# import redis
# import typesense
# from typesense.exceptions import (
#     ObjectNotFound,
#     RequestMalformed,
#     RequestUnauthorized,
#     ServerError,
#     ServiceUnavailable,
#     Timeout as TypesenseTimeout,
# )
 
# from decouple import config
 
# from .address_utils import process_address_maps
 
 
# # ── Analytics ────────────────────────────────────────────────────────────────
 
# try:
#     from .redis_analytics import SearchAnalytics
#     ANALYTICS_AVAILABLE = True
# except ImportError:
#     ANALYTICS_AVAILABLE = False
#     SearchAnalytics = None
 
 
# # ── Autocomplete ─────────────────────────────────────────────────────────────
 
# try:
#     from .searchapi import get_autocomplete
# except ImportError:
#     get_autocomplete = None
 
 
# # ── Search submission ─────────────────────────────────────────────────────────
 
# try:
#     from .searchsubmission import process_search_submission
# except ImportError:
#     process_search_submission = None
 
 
# # ── Word Discovery — handled inside bridge, not needed here ──────────────────
 
# word_discovery_multi = None
 
 
# # ── Intent Detection ──────────────────────────────────────────────────────────
 
# try:
#     from .intent_detect import detect_intent
#     INTENT_AVAILABLE = True
# except ImportError:
#     INTENT_AVAILABLE = False
#     detect_intent = None
 
 
# # ── typesense_bridge_v3 — core search functions ───────────────────────────────
 
# try:
#     from .typesense_bridge_v3 import (
#         # Main entry point
#         execute_full_search,
#         # Compatibility stubs
#         detect_query_intent,
#         get_facets,
#         get_featured_result,
#         get_related_searches,
#         log_search_event,
#         # Cache helpers used directly in the view
#         _generate_stable_cache_key,
#         _get_cached_results,
#         _has_real_images,
#         # Document fetching used directly in the view
#         fetch_full_documents,
#         client,  
#     )
#     BRIDGE_AVAILABLE = True
# except ImportError as e:
#     BRIDGE_AVAILABLE = False
#     execute_full_search        = None
#     detect_query_intent        = None
#     get_facets                 = None
#     get_featured_result        = None
#     get_related_searches       = None
#     log_search_event           = None
#     _generate_stable_cache_key = None
#     _get_cached_results        = None
#     _has_real_images           = None
#     fetch_full_documents       = None
#     print(f"⚠️ typesense_bridge_v3 not available: {e}")
 
 
# # ── typesense_bridge_v3 — debug functions ─────────────────────────────────────
 
# try:
#     from .typesense_bridge_v3 import (
#         _run_word_discovery,
#         _run_embedding,
#         run_parallel_prep,
#         _read_v3_profile,
#         build_typesense_params,
#         build_filter_string_without_data_type,
#         _resolve_blend,
#         _extract_authority_score,
#         _compute_text_score,
#         _compute_semantic_score,
#         _domain_relevance,
#         _content_intent_match,
#         _pool_type_multiplier,
#         _score_document,
#         fetch_candidate_uuids,
#         fetch_candidate_uuids_from_questions,
#         fetch_all_candidate_uuids,
#         semantic_rerank_candidates,
#         fetch_candidate_metadata,
#         fetch_candidates_with_metadata,
#         count_all,
#         fetch_documents_by_semantic_uuid,
#         _should_trigger_ai_overview,
#         _build_ai_overview,
#         BLEND_RATIOS,
#         SEMANTIC_DISTANCE_GATE,
#         QUESTION_SEMANTIC_DISTANCE_GATE
#     )
#     DEBUG_BRIDGE_AVAILABLE = True
# except ImportError as e:
#     DEBUG_BRIDGE_AVAILABLE = False
#     DEBUG_BRIDGE_IMPORT_ERROR = str(e)
#     print(f"⚠️ typesense_bridge_v3 debug imports not available: {e}")
 
 
# # ── Thread pool for debug endpoints ──────────────────────────────────────────
 
# _debug_executor = ThreadPoolExecutor(max_workers=4)
 
 


# # =============================================================================
# # LOGGING CONFIGURATION
# # =============================================================================

# logger = logging.getLogger(__name__)

# # Near the top with other imports
# from .geolocation import (
#     get_client_ip,
#     get_device_info,
#     get_full_client_info,
#     get_location_from_request
# )

# # Import user_agents for device parsing
# try:
#     from user_agents import parse as parse_user_agent
# except ImportError:
#     parse_user_agent = None


# # =============================================================================
# # SAFE WRAPPER FUNCTIONS FOR GEOLOCATION
# # =============================================================================
# # These handle the case where geolocation functions expect different arguments

# def _safe_get_device_info(request):
#     """
#     Safely get device info from request.
    
#     Handles the case where get_device_info() expects a user agent STRING
#     instead of the request object.
#     """
#     user_agent = request.META.get('HTTP_USER_AGENT', '')
    
#     try:
#         # Try passing user agent string (what geolocation.py expects)
#         result = get_device_info(user_agent)
#         if isinstance(result, dict):
#             return result
#     except Exception as e:
#         pass  # Fall through to manual parsing
    
#     # Fallback: return basic device info manually parsed
#     ua_lower = user_agent.lower()
    
#     is_mobile = any(x in ua_lower for x in ['mobile', 'android', 'iphone', 'ipad'])
#     is_bot = any(x in ua_lower for x in ['bot', 'crawler', 'spider', 'scraper'])
    
#     # Detect browser
#     browser = 'Unknown'
#     if 'edg/' in ua_lower or 'edge/' in ua_lower:
#         browser = 'Edge'
#     elif 'chrome' in ua_lower:
#         browser = 'Chrome'
#     elif 'firefox' in ua_lower:
#         browser = 'Firefox'
#     elif 'safari' in ua_lower:
#         browser = 'Safari'
    
#     # Detect OS
#     os_name = 'Unknown'
#     if 'windows' in ua_lower:
#         os_name = 'Windows'
#     elif 'mac os' in ua_lower or 'macos' in ua_lower:
#         os_name = 'macOS'
#     elif 'linux' in ua_lower:
#         os_name = 'Linux'
#     elif 'android' in ua_lower:
#         os_name = 'Android'
#     elif 'iphone' in ua_lower or 'ipad' in ua_lower:
#         os_name = 'iOS'
    
#     return {
#         'device_type': 'mobile' if is_mobile else 'desktop',
#         'user_agent': user_agent[:500],
#         'browser': browser,
#         'browser_version': '',
#         'os_name': os_name,
#         'os_version': '',
#         'is_mobile': is_mobile,
#         'is_bot': is_bot,
#     }


# def _safe_get_client_ip(request):
#     """Safely get client IP - won't crash if geolocation module fails."""
#     try:
#         return get_client_ip(request)
#     except Exception:
#         x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
#         if x_forwarded_for:
#             return x_forwarded_for.split(',')[0].strip()
#         return request.META.get('REMOTE_ADDR', '')


# def _safe_get_location(request):
#     """Safely get location - returns empty dict if anything fails."""
#     try:
#         result = get_location_from_request(request)
#         if isinstance(result, dict):
#             return result
#     except Exception:
#         pass
#     return {}


# def _safe_get_session_id(request):
#     """Safely get or create session ID."""
#     try:
#         if not request.session.session_key:
#             request.session.create()
#         return request.session.session_key
#     except Exception:
#         return str(uuid.uuid4())


# # =============================================================================
# # DYNAMIC TAB LABEL MAPPINGS
# # =============================================================================

# # User-friendly labels for document_data_type values (used for tabs)
# DATA_TYPE_LABELS: Dict[str, str] = {
#     'article': 'Articles',
#     'person': 'People',
#     'business': 'Businesses',
#     'place': 'Places',
#     'media': 'Media',
#     'event': 'Events',
#     'product': 'Products',
# }

# # User-friendly labels for document_category values
# CATEGORY_LABELS: Dict[str, str] = {
#     'healthcare_medical': 'Healthcare',
#     'fashion': 'Fashion',
#     'beauty': 'Beauty',
#     'food_recipes': 'Food & Recipes',
#     'travel_tourism': 'Travel',
#     'entertainment': 'Entertainment',
#     'business': 'Business',
#     'education': 'Education',
#     'technology': 'Technology',
#     'sports': 'Sports',
#     'finance': 'Finance',
#     'real_estate': 'Real Estate',
#     'automotive': 'Automotive',
#     'lifestyle': 'Lifestyle',
#     'news': 'News',
#     'culture': 'Culture',
#     'politics': 'Politics',
#     'science': 'Science',
#     'general': 'General',
# }

# # Icons for data types (SVG paths for template use)
# DATA_TYPE_ICONS: Dict[str, str] = {
#     'article': 'M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z',
#     'person': 'M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z',
#     'business': 'M12 7V3H2v18h20V7H12zM6 19H4v-2h2v2zm0-4H4v-2h2v2zm0-4H4V9h2v2zm0-4H4V5h2v2zm4 12H8v-2h2v2zm0-4H8v-2h2v2zm0-4H8V9h2v2zm0-4H8V5h2v2zm10 12h-8v-2h2v-2h-2v-2h2v-2h-2V9h8v10zm-2-8h-2v2h2v-2zm0 4h-2v2h2v-2z',
#     'place': 'M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z',
#     'media': 'M18 4l2 4h-3l-2-4h-2l2 4h-3l-2-4H8l2 4H7L5 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V4h-4zM4 18V8h16v10H4z',
#     'event': 'M17 12h-5v5h5v-5zM16 1v2H8V1H6v2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2h-1V1h-2zm3 18H5V8h14v11z',
#     'product': 'M18 6h-2c0-2.21-1.79-4-4-4S8 3.79 8 6H6c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-6-2c1.1 0 2 .9 2 2h-4c0-1.1.9-2 2-2zm6 16H6V8h2v2c0 .55.45 1 1 1s1-.45 1-1V8h4v2c0 .55.45 1 1 1s1-.45 1-1V8h2v12z',
# }


# # =============================================================================
# # REDIS CLIENT WITH CONNECTION POOLING & ERROR HANDLING
# # =============================================================================

# class RedisManager:
#     """Thread-safe Redis connection manager with pooling and error handling."""
    
#     _instance: Optional['RedisManager'] = None
#     _client: Optional[redis.Redis] = None
#     _pool: Optional[redis.ConnectionPool] = None
#     _available: bool = False
    
#     def __new__(cls) -> 'RedisManager':
#         if cls._instance is None:
#             cls._instance = super().__new__(cls)
#             cls._instance._initialize()
#         return cls._instance
    
#     def _initialize(self) -> None:
#         """Initialize Redis connection pool."""
#         try:
#             self._pool = redis.ConnectionPool(
#                 host=config('REDIS_LOCATION'),
#                 port=config('REDIS_PORT', cast=int),
#                 db=config('REDIS_DB', default=0, cast=int),
#                 password=config('REDIS_PASSWORD', default=None),
#                 decode_responses=True,
#                 max_connections=20,
#                 socket_timeout=5,
#                 socket_connect_timeout=5,
#                 retry_on_timeout=True,
#             )
#             self._client = redis.Redis(connection_pool=self._pool)
#             # Test connection
#             self._client.ping()
#             self._available = True
#             logger.info("Redis connection established successfully")
#         except (redis.ConnectionError, redis.TimeoutError, Exception) as e:
#             logger.warning(f"Redis unavailable: {e}. Falling back to Django cache.")
#             self._client = None
#             self._available = False
    
#     @property
#     def client(self) -> Optional[redis.Redis]:
#         return self._client
    
#     @property
#     def available(self) -> bool:
#         return self._available
    
#     def safe_get(self, key: str, default: Any = None) -> Any:
#         """Safely get a value from Redis."""
#         if not self._available or not self._client:
#             return default
#         try:
#             value = self._client.get(key)
#             return value if value is not None else default
#         except (redis.ConnectionError, redis.TimeoutError) as e:
#             logger.warning(f"Redis get error for key {key}: {e}")
#             return default
    
#     def safe_set(self, key: str, value: Any, ex: int = 300) -> bool:
#         """Safely set a value in Redis."""
#         if not self._available or not self._client:
#             return False
#         try:
#             self._client.set(key, value, ex=ex)
#             return True
#         except (redis.ConnectionError, redis.TimeoutError) as e:
#             logger.warning(f"Redis set error for key {key}: {e}")
#             return False
    
#     def safe_hgetall(self, key: str) -> Dict:
#         """Safely get all hash values."""
#         if not self._available or not self._client:
#             return {}
#         try:
#             return self._client.hgetall(key) or {}
#         except (redis.ConnectionError, redis.TimeoutError) as e:
#             logger.warning(f"Redis hgetall error for key {key}: {e}")
#             return {}
    
#     def safe_zrevrange(self, key: str, start: int, end: int) -> List:
#         """Safely get sorted set range."""
#         if not self._available or not self._client:
#             return []
#         try:
#             return self._client.zrevrange(key, start, end) or []
#         except (redis.ConnectionError, redis.TimeoutError) as e:
#             logger.warning(f"Redis zrevrange error for key {key}: {e}")
#             return []
    
#     def safe_incr(self, key: str, ex: int = 60) -> int:
#         """Safely increment a counter with expiry."""
#         if not self._available or not self._client:
#             return 0
#         try:
#             pipe = self._client.pipeline()
#             pipe.incr(key)
#             pipe.expire(key, ex)
#             results = pipe.execute()
#             return results[0] if results else 0
#         except (redis.ConnectionError, redis.TimeoutError) as e:
#             logger.warning(f"Redis incr error for key {key}: {e}")
#             return 0


# # Initialize Redis manager
# redis_manager = RedisManager()


# # =============================================================================
# # TYPESENSE CLIENT WITH RETRY LOGIC & ERROR HANDLING
# # =============================================================================

# class TypesenseManager:
#     """Thread-safe Typesense connection manager with error handling."""
    
#     _instance: Optional['TypesenseManager'] = None
#     _client: Optional[typesense.Client] = None
#     _available: bool = False
#     _last_check: float = 0
#     _check_interval: int = 60  # Re-check availability every 60 seconds
    
#     def __new__(cls) -> 'TypesenseManager':
#         if cls._instance is None:
#             cls._instance = super().__new__(cls)
#             cls._instance._initialize()
#         return cls._instance
    
#     def _initialize(self) -> None:
#         """Initialize Typesense client."""
#         try:
#             self._client = typesense.Client({
#                 'api_key': config('TYPESENSE_API_KEY'),
#                 'nodes': [{
#                     'host': config('TYPESENSE_HOST'),
#                     'port': config('TYPESENSE_PORT'),
#                     'protocol': config('TYPESENSE_PROTOCOL', default='http')
#                 }],
#                 'connection_timeout_seconds': 5,
#                 'num_retries': 3,
#             })
#             # Test connection
#             self._client.collections.retrieve()
#             self._available = True
#             self._last_check = time.time()
#             logger.info("Typesense connection established successfully")
#         except Exception as e:
#             logger.error(f"Typesense initialization failed: {e}")
#             self._client = None
#             self._available = False
    
#     @property
#     def client(self) -> Optional[typesense.Client]:
#         # Periodically re-check availability if previously unavailable
#         if not self._available and time.time() - self._last_check > self._check_interval:
#             self._initialize()
#         return self._client
    
#     @property
#     def available(self) -> bool:
#         return self._available and self._client is not None
    
#     def search(
#         self,
#         collection: str,
#         params: Dict[str, Any]
#     ) -> Optional[Dict[str, Any]]:
#         """Execute a search with comprehensive error handling."""
#         if not self.available:
#             logger.error("Typesense client not available")
#             return None
        
#         try:
#             return self._client.collections[collection].documents.search(params)
#         except TypesenseTimeout as e:
#             logger.error(f"Typesense timeout: {e}")
#             return None
#         except ServiceUnavailable as e:
#             logger.error(f"Typesense service unavailable: {e}")
#             self._available = False
#             return None
#         except ObjectNotFound as e:
#             logger.warning(f"Typesense collection not found: {e}")
#             return None
#         except RequestMalformed as e:
#             logger.error(f"Typesense request malformed: {e}")
#             return None
#         except ServerError as e:
#             logger.error(f"Typesense server error: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Typesense unexpected error: {e}")
#             return None
    
#     def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
#         """Get a single document with error handling."""
#         if not self.available:
#             return None
        
#         try:
#             return self._client.collections[collection].documents[doc_id].retrieve()
#         except ObjectNotFound:
#             return None
#         except Exception as e:
#             logger.error(f"Typesense get_document error: {e}")
#             return None


# # =============================================================================
# # ANALYTICS CLIENT WITH CONNECTION RETRY LOGIC
# # =============================================================================

# _analytics_client = None
# _analytics_last_attempt: float = 0
# _analytics_retry_interval: int = 30  # Retry every 30 seconds
# _analytics_max_retries: int = 3
# _analytics_retry_count: int = 0


# def get_analytics():
#     """
#     Get or create SearchAnalytics instance with connection retry logic.
#     """
#     global _analytics_client, _analytics_last_attempt, _analytics_retry_count
    
#     if _analytics_client is not None:
#         return _analytics_client
    
#     if not ANALYTICS_AVAILABLE:
#         return None
    
#     if _analytics_retry_count >= _analytics_max_retries:
#         extended_interval = 300  # 5 minutes
#         if time.time() - _analytics_last_attempt < extended_interval:
#             return None
#         _analytics_retry_count = 0
    
#     current_time = time.time()
#     backoff_interval = _analytics_retry_interval * (2 ** _analytics_retry_count)
    
#     if current_time - _analytics_last_attempt < backoff_interval:
#         return None
    
#     _analytics_last_attempt = current_time
    
#     try:
#         _analytics_client = SearchAnalytics()
#         _analytics_retry_count = 0
#         logger.info("SearchAnalytics initialized successfully")
#         return _analytics_client
#     except redis.ConnectionError as e:
#         _analytics_retry_count += 1
#         logger.warning(f"SearchAnalytics Redis connection failed (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
#     except redis.TimeoutError as e:
#         _analytics_retry_count += 1
#         logger.warning(f"SearchAnalytics Redis timeout (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
#     except Exception as e:
#         _analytics_retry_count += 1
#         logger.warning(f"Failed to initialize SearchAnalytics (attempt {_analytics_retry_count}/{_analytics_max_retries}): {e}")
    
#     return None


# def reset_analytics_client():
#     """Reset the analytics client for testing or recovery purposes."""
#     global _analytics_client, _analytics_last_attempt, _analytics_retry_count
#     _analytics_client = None
#     _analytics_last_attempt = 0
#     _analytics_retry_count = 0


# # Initialize Typesense manager
# typesense_manager = TypesenseManager()

# # Collection name constant
# COLLECTION_NAME = 'document'


# # =============================================================================
# # CONFIGURATION CONSTANTS
# # =============================================================================

# SUPPORTED_CITIES: frozenset = frozenset([
#     'Atlanta', 'Houston', 'Chicago', 'Detroit',
#     'New York', 'Los Angeles', 'Philadelphia', 'Washington',
#     'Baltimore', 'Charlotte', 'Memphis', 'New Orleans',
#     'Oakland', 'Miami', 'Dallas', 'St. Louis'
# ])

# DEFAULT_CITY: str = 'Atlanta'

# CITY_ALIASES: Dict[str, str] = {
#     'nyc': 'New York',
#     'ny': 'New York',
#     'la': 'Los Angeles',
#     'dc': 'Washington',
#     'washington dc': 'Washington',
#     'washington d.c.': 'Washington',
#     'philly': 'Philadelphia',
#     'nola': 'New Orleans',
#     'chi': 'Chicago',
#     'atl': 'Atlanta',
#     'htx': 'Houston',
# }

# SUBCATEGORY_MAP: Dict[str, List[str]] = {
#     'restaurants': ['restaurant', 'food', 'dining', 'catering', 'cafe'],
#     'beauty': ['salon', 'barber', 'beauty', 'hair', 'nails', 'spa'],
#     'professional': ['lawyer', 'accountant', 'consultant', 'financial', 'insurance'],
#     'contractors': ['contractor', 'plumber', 'electrician', 'construction', 'handyman'],
#     'realestate': ['real estate', 'realtor', 'property', 'mortgage', 'housing'],
#     'retail': ['retail', 'store', 'shop', 'boutique', 'clothing'],
#     'creative': ['photography', 'design', 'art', 'music', 'media', 'marketing'],
#     'tech': ['technology', 'software', 'web', 'app', 'IT', 'digital'],
# }

# SCHEMA_MAP: Dict[str, str] = {
#     'business': 'business',
#     'culture': 'culture',
#     'health': 'health',
#     'news': 'news',
#     'community': 'community',
#     'lifestyle': 'lifestyle',
#     'education': 'education',
#     'media': 'media',
# }

# TEMPLATE_MAP: Dict[str, str] = {
#     'business': 'category_business.html',
#     'culture': 'category_culture.html',
#     'health': 'category_health.html',
#     'news': 'category_news.html',
#     'community': 'category_community.html',
#     'lifestyle': 'category_lifestyle.html',
#     'education': 'category_education.html',
#     'media': 'category_media.html',
# }

# SEARCH_CONFIG: Dict[str, Any] = {
#     'max_timestamp_age_seconds': 300,
#     'rate_limit_per_minute': 60,
#     'min_typing_time_ms': 50,
#     'max_query_length': 500,
#     'min_query_length': 1,
#     'nonce_expiry_seconds': 60,
#     'default_per_page': 20,
#     'max_per_page': 100,
#     'min_per_page': 1,
#     'max_page': 1000,
#     'cache_timeout': 300,
# }

# TRACK_CLICK_CONFIG: Dict[str, Any] = {
#     'rate_limit_per_minute': 120,
#     'rate_limit_per_hour': 1000,
#     'max_url_length': 2000,
#     'max_title_length': 500,
#     'max_query_length': 500,
#     'max_source_length': 200,
#     'max_result_id_length': 100,
#     'max_session_id_length': 50,
#     'max_request_id_length': 100,
#     'max_corrected_query_length': 500,
#     'max_event_data_size': 10000,
# }


# # =============================================================================
# # INPUT VALIDATION & SANITIZATION
# # =============================================================================

# def sanitize_query(query: Any) -> str:
#     """Sanitize search query to prevent injection and handle edge cases."""
#     if query is None:
#         return ''
    
#     try:
#         query = str(query)
#     except (TypeError, ValueError):
#         return ''
    
#     query = query.strip()
    
#     if len(query) > SEARCH_CONFIG['max_query_length']:
#         query = query[:SEARCH_CONFIG['max_query_length']]
    
#     query = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', query)
#     query = re.sub(r'\s+', ' ', query)
#     query = re.sub(r'[<>{}|\[\]\\^`]', '', query)
    
#     return query


# def sanitize_filter_value(value: Any) -> str:
#     """Sanitize filter values to prevent injection."""
#     if value is None:
#         return ''
    
#     try:
#         value = str(value).strip()
#     except (TypeError, ValueError):
#         return ''
    
#     value = value[:200]
#     value = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', value)
    
#     return value


# def sanitize_url(url: Any, max_length: int = 2000) -> str:
#     """Sanitize URL input."""
#     if url is None:
#         return ''
    
#     try:
#         url = str(url).strip()
#     except (TypeError, ValueError):
#         return ''
    
#     if len(url) > max_length:
#         url = url[:max_length]
    
#     url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url)
    
#     if url and not url.startswith(('http://', 'https://')):
#         return ''
    
#     return url


# def sanitize_string(value: Any, max_length: int = 500) -> str:
#     """General string sanitization."""
#     if value is None:
#         return ''
    
#     try:
#         value = str(value).strip()
#     except (TypeError, ValueError):
#         return ''
    
#     if len(value) > max_length:
#         value = value[:max_length]
    
#     value = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', value)
    
#     return value


# def sanitize_int(value: Any, default: int = 0, min_val: int = None, max_val: int = None) -> int:
#     """Sanitize integer input with bounds checking."""
#     try:
#         result = int(value)
#     except (TypeError, ValueError):
#         return default
    
#     if min_val is not None and result < min_val:
#         return min_val
#     if max_val is not None and result > max_val:
#         return max_val
    
#     return result


# def validate_page(page: Any, default: int = 1) -> int:
#     """Validate and sanitize page number."""
#     try:
#         page = int(page)
#     except (TypeError, ValueError):
#         return default
    
#     if page < 1:
#         return default
#     if page > SEARCH_CONFIG['max_page']:
#         return SEARCH_CONFIG['max_page']
    
#     return page


# def validate_per_page(per_page: Any, default: int = 20) -> int:
#     """Validate and sanitize per_page parameter."""
#     try:
#         per_page = int(per_page)
#     except (TypeError, ValueError):
#         return default
    
#     if per_page < SEARCH_CONFIG['min_per_page']:
#         return default
#     if per_page > SEARCH_CONFIG['max_per_page']:
#         return SEARCH_CONFIG['max_per_page']
    
#     return per_page


# def validate_sort(sort: Any, allowed: List[str], default: str = 'authority') -> str:
#     """Validate sort parameter against allowed values."""
#     if not sort:
#         return default
    
#     sort = str(sort).lower().strip()
    
#     if sort in allowed:
#         return sort
    
#     return default


# def validate_data_type(data_type: Any) -> str:
#     """Validate data_type filter against allowed values."""
#     if not data_type:
#         return ''
    
#     data_type = str(data_type).lower().strip()
    
#     # Valid data types (7 categories)
#     valid_types = {'article', 'person', 'business', 'place', 'media', 'event', 'product'}
    
#     if data_type in valid_types:
#         return data_type
    
#     return ''


# def validate_schema(schema: Any) -> str:
#     """Validate document_schema filter against allowed values."""
#     if not schema:
#         return ''
    
#     schema = str(schema).strip()
    
#     # Valid schema types (Schema.org)
#     valid_schemas = {
#         'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#         'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#         'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#         'AudioObject', 'Book', 'Movie', 'MusicRecording'
#     }
    
#     if schema in valid_schemas:
#         return schema
    
#     return ''


# def get_user_city(request, default: str = DEFAULT_CITY) -> str:
#     """Get user's city from various sources with validation."""
#     city = ''
    
#     city = request.GET.get('city', '')
    
#     if not city:
#         try:
#             city = request.session.get('user_city', '')
#         except Exception:
#             pass
    
#     if city:
#         city = city.strip()
#         city_lower = city.lower()
#         if city_lower in CITY_ALIASES:
#             city = CITY_ALIASES[city_lower]
#         else:
#             city = city.title()
    
#     if city not in SUPPORTED_CITIES:
#         city = default
    
#     try:
#         request.session['user_city'] = city
#     except Exception:
#         pass
    
#     return city


# # =============================================================================
# # CACHING UTILITIES
# # =============================================================================

# def get_cache_key(*args, prefix: str = 'afrotodo') -> str:
#     """Generate a consistent cache key from arguments."""
#     key_data = ':'.join(str(arg) for arg in args if arg is not None)
#     hash_value = hashlib.md5(key_data.encode()).hexdigest()
#     return f"{prefix}:{hash_value}"


# def safe_cache_get(key: str, default: Any = None) -> Any:
#     """Safely get from Django cache with fallback."""
#     try:
#         result = cache.get(key)
#         return result if result is not None else default
#     except Exception as e:
#         logger.warning(f"Cache get error for key {key}: {e}")
#         return default


# def safe_cache_set(key: str, value: Any, timeout: int = 300) -> bool:
#     """Safely set Django cache with error handling."""
#     try:
#         cache.set(key, value, timeout)
#         return True
#     except Exception as e:
#         logger.warning(f"Cache set error for key {key}: {e}")
#         return False


# # =============================================================================
# # SECURITY VALIDATION
# # =============================================================================

# class SearchSecurityValidator:
#     """Validates security parameters from search requests."""
    
#     @staticmethod
#     def validate_timestamp(timestamp_str: Optional[str]) -> Tuple[bool, Optional[str]]:
#         """Validate timestamp is recent (prevent replay attacks)."""
#         if not timestamp_str:
#             return True, None
        
#         try:
#             timestamp = int(timestamp_str)
#             current_time = int(time.time() * 1000)
#             age_seconds = (current_time - timestamp) / 1000
            
#             if age_seconds < -60:
#                 return False, "Timestamp is in the future"
            
#             if age_seconds > SEARCH_CONFIG['max_timestamp_age_seconds']:
#                 return False, "Request too old"
            
#             return True, None
            
#         except (ValueError, TypeError):
#             return True, None
    
#     @staticmethod
#     def validate_nonce(nonce: Optional[str], session_id: str) -> Tuple[bool, Optional[str]]:
#         """Validate nonce hasn't been used (prevent replay)."""
#         if not nonce or not session_id:
#             return True, None
        
#         if len(nonce) < 8 or len(nonce) > 64:
#             return False, "Invalid nonce format"
        
#         cache_key = f"nonce:{session_id}:{nonce}"
        
#         if safe_cache_get(cache_key):
#             return False, "Nonce already used"
        
#         safe_cache_set(cache_key, True, SEARCH_CONFIG['nonce_expiry_seconds'])
#         return True, None
    
#     @staticmethod
#     def check_rate_limit(session_id: str, client_fp: str = '') -> Tuple[bool, Optional[str]]:
#         """Check if request is within rate limits."""
#         if not session_id:
#             return True, None
        
#         rate_key = f"rate:{session_id}:{client_fp or 'unknown'}"
        
#         try:
#             current_count = safe_cache_get(rate_key, 0)
            
#             if current_count >= SEARCH_CONFIG['rate_limit_per_minute']:
#                 return False, "Rate limit exceeded"
            
#             safe_cache_set(rate_key, current_count + 1, 60)
#             return True, None
#         except Exception:
#             return True, None
    
#     @staticmethod
#     def detect_bot(typing_time_ms: Optional[str], request_sequence: Optional[str]) -> Tuple[bool, Optional[str]]:
#         """Simple bot detection heuristics."""
#         try:
#             typing_time = int(typing_time_ms) if typing_time_ms else 0
#             req_seq = int(request_sequence) if request_sequence else 0
            
#             if typing_time > 0 and typing_time < SEARCH_CONFIG['min_typing_time_ms']:
#                 return True, "Typing too fast"
            
#             if req_seq > 200:
#                 return True, "Excessive requests in session"
            
#             return False, None
            
#         except (ValueError, TypeError):
#             return False, None


# class TrackClickRateLimiter:
#     """Rate limiter specifically for click tracking endpoints."""
    
#     @staticmethod
#     def check_rate_limit(session_id: str, client_ip: str = '') -> Tuple[bool, Optional[str]]:
#         """Check if click tracking request is within rate limits."""
#         if not session_id:
#             if not client_ip:
#                 return True, None
#             identifier = f"ip:{client_ip}"
#         else:
#             identifier = f"session:{session_id}"
        
#         minute_key = f"track_click_rate:minute:{identifier}"
#         minute_count = redis_manager.safe_incr(minute_key, ex=60)
        
#         if minute_count > TRACK_CLICK_CONFIG['rate_limit_per_minute']:
#             logger.warning(f"Track click rate limit exceeded (minute) for {identifier}")
#             return False, "Rate limit exceeded. Please slow down."
        
#         hour_key = f"track_click_rate:hour:{identifier}"
#         hour_count = redis_manager.safe_incr(hour_key, ex=3600)
        
#         if hour_count > TRACK_CLICK_CONFIG['rate_limit_per_hour']:
#             logger.warning(f"Track click rate limit exceeded (hour) for {identifier}")
#             return False, "Rate limit exceeded. Please try again later."
        
#         return True, None


# # =============================================================================
# # SEARCH PARAMETER EXTRACTION
# # =============================================================================

# class SearchParams:
#     """Extract and validate all search parameters from request."""
    
#     def __init__(self, request):
#         self.request = request
        
#         raw_query = request.GET.get('query', '') or request.GET.get('q', '')
#         self.query = sanitize_query(raw_query)
#         self.alt_mode = request.GET.get('alt_mode', 'y')
        
#         if self.alt_mode not in ('y', 'n'):
#             self.alt_mode = 'y'
        
#         self.session_id = self._get_session_id()
#         self.request_id = sanitize_filter_value(request.GET.get('request_id', ''))[:64]
#         self.timestamp = request.GET.get('timestamp', '')[:20]
#         self.nonce = request.GET.get('nonce', '')[:64]
        
#         self.source = sanitize_filter_value(request.GET.get('source', 'unknown'))[:50]
#         self.device_type = sanitize_filter_value(request.GET.get('device_type', 'unknown'))[:20]
#         self.result_count = request.GET.get('result_count', '0')[:10]
#         self.typing_time_ms = request.GET.get('typing_time_ms', '0')[:10]
        
#         self.client_fp = request.GET.get('client_fp', '')[:64]
#         self.request_sequence = request.GET.get('req_seq', '0')[:10]
    
#     def _get_session_id(self) -> str:
#         """Get or create session ID."""
#         session_id = self.request.GET.get('session_id', '')
        
#         if session_id and 20 <= len(session_id) <= 50:
#             if re.match(r'^[a-zA-Z0-9-]+$', session_id):
#                 return session_id
        
#         try:
#             if hasattr(self.request, 'session'):
#                 session_id = self.request.session.get('search_session_id')
#                 if not session_id:
#                     session_id = str(uuid.uuid4())
#                     self.request.session['search_session_id'] = session_id
#                 return session_id
#         except Exception:
#             pass
        
#         return str(uuid.uuid4())
    
#     @property
#     def is_keyword_search(self) -> bool:
#         return self.alt_mode == 'n'
    
#     @property
#     def is_semantic_search(self) -> bool:
#         return self.alt_mode == 'y'
    
#     def to_dict(self) -> Dict[str, Any]:
#         return {
#             'query': self.query,
#             'alt_mode': self.alt_mode,
#             'session_id': self.session_id[:8] + '...' if self.session_id else '',
#             'source': self.source,
#             'device_type': self.device_type,
#         }


# # =============================================================================
# # TYPESENSE SEARCH HELPERS
# # =============================================================================

# def typesense_search(
#     query: str,
#     filter_by: str = '',
#     sort_by: str = 'authority_score:desc',
#     facet_by: str = '',
#     per_page: int = 20,
#     page: int = 1,
#     query_by: str = 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#     collection: str = COLLECTION_NAME
# ) -> Optional[Dict[str, Any]]:
#     """Execute Typesense search with error handling."""
#     search_params = {
#         'q': query or '*',
#         'query_by': query_by,
#         'per_page': min(per_page, SEARCH_CONFIG['max_per_page']),
#         'page': min(page, SEARCH_CONFIG['max_page']),
#     }
    
#     if filter_by:
#         search_params['filter_by'] = filter_by
    
#     if sort_by:
#         search_params['sort_by'] = sort_by
    
#     if facet_by:
#         search_params['facet_by'] = facet_by
#         search_params['max_facet_values'] = 20
    
#     return typesense_manager.search(collection, search_params)


# def parse_facets(results: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
#     """
#     Parse facet counts from Typesense results.
    
#     Returns dict with:
#     - category: document_category facets with labels
#     - brand: document_brand facets
#     - data_type: document_data_type facets with labels (for dynamic tabs)
#     - schema: document_schema facets
#     """
#     facets: Dict[str, List[Dict[str, Any]]] = {
#         'category': [],
#         'brand': [],
#         'data_type': [],
#         'schema': [],
#     }
    
#     if not results:
#         return facets
    
#     for facet in results.get('facet_counts', []):
#         field = facet.get('field_name', '')
#         counts = facet.get('counts', [])
        
#         if field == 'document_category':
#             facets['category'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': CATEGORY_LABELS.get(c.get('value', ''), c.get('value', '').replace('_', ' ').title())
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_brand':
#             facets['brand'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': c.get('value', '')
#                 }
#                 for c in counts if c.get('value')
#             ]
        
#         elif field == 'document_data_type':
#             facets['data_type'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': DATA_TYPE_LABELS.get(c.get('value', ''), c.get('value', '').title()),
#                     'icon': DATA_TYPE_ICONS.get(c.get('value', ''), '')
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_schema':
#             facets['schema'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': c.get('value', '')
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
    
#     return facets


# def get_dynamic_tab_facets(query: str) -> Dict[str, List[Dict[str, Any]]]:
#     """
#     Get facet counts for dynamic tabs without filtering.
    
#     This runs a separate query to get the total counts across ALL data types,
#     not limited by any current filter. This allows tabs to show accurate counts.
#     """
#     try:
#         results = typesense_search(
#             query=query,
#             filter_by='',  # No filter - get all facets
#             facet_by='document_data_type,document_category,document_schema',
#             per_page=0,  # We only need facets, not results
#             page=1,
#         )
#         return parse_facets(results)
#     except Exception as e:
#         logger.warning(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': [], 'brand': []}


# def build_filter_string(filters: List[str]) -> str:
#     """Build Typesense filter string from list of filters."""
#     valid_filters = [f for f in filters if f and f.strip()]
#     return ' && '.join(valid_filters)


# def safe_get_hits(results: Optional[Dict[str, Any]], key: str = 'hits') -> List[Dict[str, Any]]:
#     """Safely extract hits from results."""
#     if not results:
#         return []
#     return results.get(key, [])


# def safe_get_total(results: Optional[Dict[str, Any]]) -> int:
#     """Safely extract total count from results."""
#     if not results:
#         return 0
#     return results.get('found', 0)


# # =============================================================================
# # ANALYTICS & LOGGING
# # =============================================================================

# def log_search_analytics(
#     params: SearchParams,
#     search_type: str,
#     result_count: int,
#     is_suspicious: bool = False,
#     error: Optional[str] = None
# ) -> None:
#     """Log search for analytics and monitoring."""
#     try:
#         log_data = {
#             'query': params.query[:100] if params.query else '',
#             'type': search_type,
#             'alt_mode': params.alt_mode,
#             'results': result_count,
#             'session': params.session_id[:8] if params.session_id else 'none',
#             'device': params.device_type,
#             'source': params.source,
#             'suspicious': is_suspicious,
#         }
        
#         if error:
#             log_data['error'] = error[:100]
        
#         logger.info(f"Search: {json.dumps(log_data)}")
        
#     except Exception as e:
#         logger.error(f"Analytics logging error: {e}")


# # =============================================================================
# # VIEW: HOME
# # =============================================================================

# from .cached_embedding_related_search import get_popular_queries
# from .trending import get_trending_results, cache_trending_result

# def home(request):
#     city = get_user_city(request)
    
#     # Get location from IP geolocation
#     client_info = get_full_client_info(request)
#     location = client_info.get('location') or {}
    
#     # Try most specific to least specific
#     location_levels = [
#         (location.get('city', ''), 'city'),
#         (location.get('region', ''), 'region'),
#         (location.get('country', ''), 'country'),
#     ]
    
#     trending_results = []
#     trending_label = 'Your Area'
    
#     for loc_value, loc_type in location_levels:
#         if loc_value:
#             trending_results = get_trending_results(city=loc_value, limit=6)
#             if trending_results:
#                 trending_label = loc_value.title()
#                 break
    
#     # Fallback to general
#     if not trending_results:
#         trending_results = get_trending_results(city=None, limit=6)
#         trending_label = 'Your Area'
    
#     context = {
#         'city': city,
#         'trending_label': trending_label,
#         'trending_results': trending_results,
#         'supported_cities': list(SUPPORTED_CITIES),
#     }
    
#     return render(request, 'home3.html', context)


# # =============================================================================
# # VIEW: SEARCH SUGGESTIONS (AUTOCOMPLETE)
# # =============================================================================

# @require_GET
# def search_suggestions(request):
#     """API endpoint for autocomplete dropdown suggestions."""
#     query = sanitize_query(request.GET.get('q', ''))
    
#     if not query or len(query) < 2:
#         return JsonResponse({'suggestions': []})
    
#     if get_autocomplete is None:
#         logger.error("get_autocomplete function not available")
#         return JsonResponse({'suggestions': [], 'error': 'Service unavailable'})
    
#     try:
#         results = get_autocomplete(prefix=query, limit=8)

#         for item in results:
#             print(f"DEBUG api item answer={item.get('answer', 'MISSING')!r} entity_type={item.get('entity_type')!r}")

#         suggestions = [
#             {
#                 'text': item.get('term', ''),
#                 'display': item.get('display', ''),
#                 'description': item.get('description', ''),
#                 'entity_type': item.get('entity_type', ''),
#                 'document_uuid': item.get('document_uuid', ''),
#                 'answer': item.get('answer', ''),
#                 'answer_type': item.get('answer_type', ''),
#             }
#             for item in results
#             if item.get('term')
#         ]
        
#         return JsonResponse({'suggestions': suggestions})
    
#     except Exception as e:
#         logger.error(f"Autocomplete error: {e}")
#         return JsonResponse({'suggestions': [], 'error': 'Service temporarily unavailable'})



# """
# Complete search view with Images tab support.
# Replace your existing search function with this one.
# """



# ## _------------------------------------------------------ Test Beginning --------------------
# def extract_images_from_results(results):
#     """
#     Extract all images from search results.
    
#     Returns a list of image dictionaries with:
#     - url: The image URL
#     - title: Title from the parent document
#     - source_url: URL of the parent document
#     - source_name: Site name of the parent document
#     - image_type: 'photo', 'logo', etc.
#     """
#     image_results = []
#     seen_urls = set()  # Deduplicate images
    
#     for result in results:
#         title = result.get('title', 'Untitled')
#         source_url = result.get('url', '#')
#         source_name = result.get('site_name', result.get('source', 'Unknown'))
        
#         # Extract from image_url array
#         image_urls = result.get('image_url', [])
#         if image_urls:
#             if isinstance(image_urls, str):
#                 image_urls = [image_urls]
#             for img_url in image_urls:
#                 if img_url and img_url not in seen_urls:
#                     seen_urls.add(img_url)
#                     image_results.append({
#                         'url': img_url,
#                         'title': title,
#                         'source_url': source_url,
#                         'source_name': source_name,
#                         'image_type': 'photo'
#                     })
        
#         # Extract from logo_url array
#         logo_urls = result.get('logo_url', [])
#         if logo_urls:
#             if isinstance(logo_urls, str):
#                 logo_urls = [logo_urls]
#             for logo_url in logo_urls:
#                 if logo_url and logo_url not in seen_urls:
#                     seen_urls.add(logo_url)
#                     image_results.append({
#                         'url': logo_url,
#                         'title': f"{title} - Logo",
#                         'source_url': source_url,
#                         'source_name': source_name,
#                         'image_type': 'logo'
#                     })
    
#     return image_results


# def _build_image_pagination(total_image_count, page, img_per_page=40):
#     """
#     Build pagination dict for image results.
    
#     Always returns a valid pagination dict (never None) when there are images.
#     Returns None only when there are zero images.
    
#     Args:
#         total_image_count: Total number of images across all pages
#         page: Current page number
#         img_per_page: Images per page (default 40)
#     """
#     if total_image_count == 0:
#         return None
    
#     img_total_pages = max(1, (total_image_count + img_per_page - 1) // img_per_page)
    
#     # Clamp page to valid range
#     page = max(1, min(page, img_total_pages))
    
#     img_start = (page - 1) * img_per_page
#     img_end = min(img_start + img_per_page, total_image_count)
    
#     return {
#         'current_page': page,
#         'total_pages': img_total_pages,
#         'has_previous': page > 1,
#         'previous_page': page - 1,
#         'has_next': page < img_total_pages,
#         'next_page': page + 1,
#         'page_range': range(max(1, page - 3), min(img_total_pages + 1, page + 4)),
#         'total_images': total_image_count,
#         'start_result': img_start + 1,
#         'end_result': img_end,
#     }


# import asyncio

# def _run_async(coro):
#     """Run an async coroutine from synchronous code."""
#     try:
#         loop = asyncio.get_event_loop()
#         if loop.is_running():
#             import concurrent.futures
#             with concurrent.futures.ThreadPoolExecutor() as pool:
#                 return pool.submit(asyncio.run, coro).result()
#         else:
#             return loop.run_until_complete(coro)
#     except RuntimeError:
#         return asyncio.run(coro)


# # ============================================================
# # REPLACE YOUR search() FUNCTION WITH THIS
# # ============================================================

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
#     - view: 'images' for image grid view
    
#     Related searches are now handled inside execute_full_search().
    
#     ASYNC BRIDGE COMPATIBILITY:
#       The bridge functions (execute_full_search, _get_cached_results,
#       fetch_full_documents) are now async. This view stays synchronous
#       and wraps those calls with _run_async() to avoid Django's
#       SynchronousOnlyOperation error.
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)

#     # === 0. PREVENT FORM RESUBMISSION ON TAB CLICKS ===
#     if params.query and not request.GET.get('_rd'):
#         from urllib.parse import urlparse
#         from django.shortcuts import redirect
#         referer = request.META.get('HTTP_REFERER', '')
#         if referer:
#             referer_path = urlparse(referer).path
#             current_path = request.path
#             if referer_path != current_path:
#                 return redirect(request.get_full_path() + '&_rd=1')

#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))

#     # === 1B. EXTRACT QUESTION PATH FIELDS ===
#     document_uuid = request.GET.get('document_uuid', '').strip()
#     search_source = request.GET.get('search_source', '').strip()
#     answer = request.GET.get('answer', '').strip()
#     answer_type = request.GET.get('answer_type', '').strip()
#     is_question_path = (search_source == 'question' and bool(document_uuid))
    
#     # Check for images view
#     view_mode = request.GET.get('view', '')
#     show_images = view_mode == 'images'
    
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
#             'display_total': 0,
#             'total_results': 0,
#             'facet_total': 0,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': active_data_type,
#             'active_category': active_category,
#             'active_schema': active_schema,
#             'show_images': False,
#             'image_results': [],
#             'image_count': 0,
#             'image_pagination': None,
#             'filters': filters,
#             'facets': {},
#             'safe_search': safe_search,
#             'was_corrected': False,
#             'corrected_query': params.query,
#             'word_corrections': [],
#             'corrections': {},
#             'intent': {},
#             'search_type': 'keyword' if params.is_keyword_search else 'semantic',
#             'alt_mode': params.alt_mode,
#             'related_searches': [],
#             'suggestions': [],
#             'featured': None,
#             'answer': '',
#             'answer_type': '',
#             'pagination': None,
#             'page': page,
#             'per_page': per_page,
#             'search_time': 0,
#             'search_time_ms': 0,
#             'from_cache': False,
#             'from_semantic_cache': False,
#             'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
#             'device_type': device_type,
#             'source': params.source,
#             'user_city': location.get('city', '') if location else '',
#             'user_country': location.get('country', '') if location else '',
#             'data_type_labels': DATA_TYPE_LABELS,
#             'category_labels': CATEGORY_LABELS,
#             'search_source': search_source,
#             'document_uuid': document_uuid,
#             'is_question_path': is_question_path,
#             'request_id': request_id,
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
#             'show_images': False,
#             'image_results': [],
#             'image_count': 0,
#             'image_pagination': None,
#             'display_total': 0,
#             'total_results': 0,
#             'facet_total': 0,
#             'filters': filters,
#             'facets': {},
#             'safe_search': safe_search,
#             'was_corrected': False,
#             'corrected_query': '',
#             'word_corrections': [],
#             'corrections': {},
#             'intent': {},
#             'search_type': 'keyword' if params.is_keyword_search else 'semantic',
#             'alt_mode': params.alt_mode,
#             'related_searches': [],
#             'suggestions': [],
#             'featured': None,
#             'answer': '',
#             'answer_type': '',
#             'categorized_results': {},
#             'pagination': None,
#             'page': page,
#             'per_page': per_page,
#             'search_time': 0,
#             'search_time_ms': 0,
#             'from_cache': False,
#             'from_semantic_cache': False,
#             'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
#             'device_type': device_type,
#             'source': params.source,
#             'user_city': location.get('city', '') if location else '',
#             'user_country': location.get('country', '') if location else '',
#             'data_type_labels': DATA_TYPE_LABELS,
#             'category_labels': CATEGORY_LABELS,
#             'search_source': search_source,
#             'document_uuid': document_uuid,
#             'is_question_path': is_question_path,
#             'request_id': request_id,
#         })
    
#     # === 6. INITIALIZE ALL VARIABLES ===
    
#     result = {}
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
    
#     data_type_facets = []
#     category_facets = []
#     schema_facets = []
#     facet_total = 0
    
#     related_searches = []
    
#     image_results = []
#     image_count = 0
#     image_pagination = None
    
#     has_filters = bool(active_data_type or active_category or active_schema)
    
#     # === 6A. GENERATE CACHE KEY ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         view_mode,
#         json.dumps(filters, sort_keys=True)
#     )
    
#     # === 6B. CHECK EXISTING RESULT CACHE ===
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
#         cached_result['from_semantic_cache'] = False
        
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
#             try:
#                 intent = detect_query_intent(corrected_query, tuple_array)
#             except Exception as e:
#                 logger.warning(f"Intent detection error: {e}")
#                 intent = {}
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
#             try:
#                 intent = detect_query_intent(corrected_query, tuple_array)
#             except Exception as e:
#                 logger.warning(f"Intent detection error: {e}")
#                 intent = {}
    
#     # === 8. EXECUTE SEARCH ===
#     if execute_full_search:
#         try:
#             # _run_async wraps the async bridge call for sync context
#             result = _run_async(execute_full_search(
#                 query=params.query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode,
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search,
#                 search_source=search_source,
#                 answer=answer if is_question_path else None,
#                 answer_type=answer_type if is_question_path else None,
#                 document_uuid=document_uuid if is_question_path else None,
#             ))
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
            
#             # === Get correction info from bridge ===
#             bridge_corrected = result.get('corrected_query', params.query)
#             if bridge_corrected and bridge_corrected.lower() != params.query.lower():
#                 corrected_query = bridge_corrected
#                 was_corrected = True
#                 word_corrections = result.get('word_discovery', {}).get('corrections', [])
            
#             # Get facets from the search result
#             data_type_facets = result.get('data_type_facets', [])
#             category_facets = result.get('category_facets', [])
#             schema_facets = result.get('schema_facets', [])
#             facet_total = result.get('facet_total', 0)
            
#             # Get related searches from the search result
#             related_searches = result.get('related_searches', [])
            
#             # =============================================================
#             # 8B. IMAGE DATA
#             # =============================================================
#             image_count = result.get('total_image_count', 0)

#             if show_images:
#                 try:
#                     stable_key = _generate_stable_cache_key(params.session_id, params.query)
#                     # _run_async wraps the async bridge call for sync context
#                     finished = _run_async(_get_cached_results(stable_key))

#                     if finished and finished.get('all_results'):
#                         all_candidates = finished['all_results']
#                         has_image = [r for r in all_candidates if _has_real_images(r)]

#                         if has_image:
#                             first_batch = has_image[:20]
#                             page_ids = [item['id'] for item in first_batch if item.get('id')]
#                             if page_ids:
#                                 # _run_async wraps the async bridge call for sync context
#                                 full_docs = _run_async(fetch_full_documents(page_ids, params.query))
#                                 image_results = extract_images_from_results(full_docs)
#                             else:
#                                 image_results = []
#                         else:
#                             image_results = []
#                     else:
#                         image_results = extract_images_from_results(results)
#                 except Exception as e:
#                     logger.warning(f"Image extraction error: {e}")
#                     image_results = []

#                 image_pagination = None
#             else:
#                 image_results = []
#                 image_pagination = None

#         except Exception as e:
#             logger.error(f"Search execution error: {e}", exc_info=True)
    
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
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if not related_searches and get_related_searches:
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
    
#     map_data = process_address_maps(request, results)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         **map_data,
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
#         'answer': result.get('answer', ''),
#         'answer_type': result.get('answer_type', ''),
        
#         # Image results
#         'show_images': show_images,
#         'image_results': image_results,
#         'image_count': image_count,
#         'image_pagination': image_pagination,
        
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
#         'from_semantic_cache': False,
#         'search_strategy': search_strategy,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,

#         # Question path tracking
#         'search_source': search_source,
#         'document_uuid': document_uuid,
#         'is_question_path': is_question_path,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])

#     if results and len(results) > 0:
#         trending_city = location.get('city', '') if location else ''
#         cache_trending_result(query=params.query, top_result=results[0], city=trending_city)

#     if results and len(results) > 0:
#         top_result = results[0]
        
#         loc_city = location.get('city', '') if location else ''
#         loc_region = location.get('region', '') if location else ''
#         loc_country = location.get('country', '') if location else ''
        
#         for loc_value in [loc_city, loc_region, loc_country]:
#             if loc_value:
#                 cache_trending_result(query=params.query, top_result=top_result, city=loc_value)
        
#         cache_trending_result(query=params.query, top_result=top_result, city=None)
    
#     return render(request, 'results2.html', context)

# # --------------------------------------------------------- Test End -----------
# @require_GET
# def load_images(request):
#     """
#     AJAX endpoint for infinite scroll image loading.
#     Returns a JSON array of image dicts for the next batch.
#     """
#     query = request.GET.get('q', '').strip()
#     offset = int(request.GET.get('offset', 0))
#     limit = int(request.GET.get('limit', 20))
#     session_id = request.GET.get('session_id', '')

#     if not query:
#         return JsonResponse({'images': [], 'has_more': False})

#     try:
#         stable_key = _generate_stable_cache_key(session_id, query)
#         finished = _get_cached_results(stable_key)

#         if not finished or not finished.get('all_results'):
#             return JsonResponse({'images': [], 'has_more': False})

#         all_candidates = finished['all_results']
#         has_image = [r for r in all_candidates if _has_real_images(r)]

#         if offset >= len(has_image):
#             return JsonResponse({'images': [], 'has_more': False})

#         # Slice the batch
#         batch = has_image[offset:offset + limit]
#         page_ids = [item['id'] for item in batch if item.get('id')]

#         if not page_ids:
#             return JsonResponse({'images': [], 'has_more': False})

#         # Fetch full docs and extract images
#         full_docs = fetch_full_documents(page_ids, query)
#         images = extract_images_from_results(full_docs)

#         has_more = (offset + limit) < len(has_image)

#         return JsonResponse({
#             'images': images,
#             'has_more': has_more,
#             'next_offset': offset + limit,
#         })

#     except Exception as e:
#         logger.warning(f"Image load error: {e}")
#         return JsonResponse({'images': [], 'has_more': False})
# # =============================================================================
# # VIEW: CATEGORY ROUTER
# # =============================================================================

# def category_view(request, category_slug: str):
#     """Generic category view router."""
#     category_slug = str(category_slug).lower().strip()
    
#     if not category_slug or category_slug not in SCHEMA_MAP:
#         raise Http404("Category not found")
    
#     city = get_user_city(request)
    
#     handlers = {
#         'business': business_category,
#         'culture': lambda r: culture_category(r, city),
#         'health': lambda r: health_category(r, city),
#         'news': lambda r: news_category(r, city),
#         'community': community_category,
#     }
    
#     handler = handlers.get(category_slug)
#     if handler:
#         return handler(request)
    
#     return generic_category_view(request, category_slug, city)


# def generic_category_view(request, category_slug: str, city: str):
#     """Generic category page with faceted search."""
    
#     schema = SCHEMA_MAP.get(category_slug)
#     if not schema:
#         raise Http404("Category not found")
    
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'relevance'], 'authority')
#     page = validate_page(request.GET.get('page', 1))
    
#     filters = [f'document_schema:={schema}']
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
    
#     sort_options = {
#         'authority': 'authority_score:desc',
#         'recent': 'created_at:desc',
#         'relevance': '_text_match:desc',
#     }
#     sort_by = sort_options.get(sort, 'authority_score:desc')
    
#     results = typesense_search(
#         query=query or '*',
#         filter_by=filter_by,
#         sort_by=sort_by,
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     hits = safe_get_hits(results)
#     total = safe_get_total(results)
#     facets = parse_facets(results)
    
#     context = {
#         'city': city,
#         'category_slug': category_slug,
#         'category_name': category_slug.title(),
#         'query': query,
#         'results': hits,
#         'total': total,
#         'facets': facets,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': sum([bool(selected_category), bool(selected_brand)]),
#         'page': page,
#         'has_more': (page * 20) < total,
#         'sort': sort,
#     }
    
#     template = TEMPLATE_MAP.get(category_slug, 'category_generic.html')
#     return render(request, template, context)


# # =============================================================================
# # VIEW: BUSINESS CATEGORY
# # =============================================================================

# def business_category(request):
#     """Business directory with faceted search."""
    
#     # Get query parameters
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_subcategory = sanitize_filter_value(request.GET.get('subcategory', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
#     page = validate_page(request.GET.get('page', 1))
    
#     # Validate subcategory
#     if selected_subcategory and selected_subcategory not in SUBCATEGORY_MAP:
#         selected_subcategory = ''
    
#     # Build filter - only filter by business schema
#     filters = ['document_schema:=business']
    
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
    
#     # Build search query - NO CITY!
#     search_query = '*'  # Default: return ALL businesses
    
#     if query:
#         search_query = query
#     elif selected_subcategory and selected_subcategory in SUBCATEGORY_MAP:
#         search_query = ' '.join(SUBCATEGORY_MAP[selected_subcategory])
    
#     sort_options = {
#         'authority': '_text_match:desc',
#         'recent': '_text_match:desc',
#         'name': 'document_title:asc',
#     }
#     sort_by = sort_options.get(sort, '_text_match:desc')
    
#     # Execute search directly using typesense_manager
#     search_params = {
#         'q': search_query,
#         'query_by': 'document_title,document_summary,keywords,primary_keywords',
#         'filter_by': filter_by,
#         'per_page': 20,
#         'page': page,
#         'facet_by': 'document_category,document_brand',
#         'max_facet_values': 20,
#     }
    
#     # Only add sort_by if not using wildcard
#     if search_query != '*':
#         search_params['sort_by'] = sort_by
    
#     # Debug output
#     print(f"\n=== BUSINESS SEARCH ===")
#     print(f"Query: '{search_query}'")
#     print(f"Filter: '{filter_by}'")
#     print(f"Params: {search_params}")
    
#     # Execute search
#     results = typesense_manager.search(COLLECTION_NAME, search_params)
    
#     print(f"Results: {type(results)}, is None: {results is None}")
#     if results:
#         print(f"Found: {results.get('found', 0)}, Hits: {len(results.get('hits', []))}")
#     print("=== END ===\n")
    
#     # Process results
#     if results:
#         browse_results = results.get('hits', [])
#         total = results.get('found', 0)
#         facets = parse_facets(results)
#     else:
#         browse_results = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # Build stats
#     stats = {
#         'business_count': total,
#         'categories_count': len(facets.get('category', [])) or 24,
#         'verified_pct': 78,
#     }
    
#     trending_searches = [
#         'black barber near me', 'soul food', 'black accountant',
#         'african restaurant', 'natural hair salon', 'black owned clothing',
#     ]
    
#     active_filter_count = sum([
#         bool(selected_subcategory),
#         bool(selected_category),
#         bool(selected_brand),
#     ])
    
#     # Build context
#     context = {
#         'city': '',  # No city filtering
#         'supported_cities': list(SUPPORTED_CITIES),
#         'query': query,
#         'selected_subcategory': selected_subcategory,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
#         'stats': stats,
#         'trending_searches': trending_searches,
#         'active_filter_count': active_filter_count,
#         'page': page,
#         'has_more': (page * 20) < total,
#         'sort': sort,
#     }
    
#     return render(request, 'category_business.html', context)


# # =============================================================================
# # VIEW: CULTURE CATEGORY
# # =============================================================================

# def generate_request_id(session_id):
#     """Generate unique request ID for tracking."""
#     return f"{session_id}:{time.time()}:{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"


# def culture_category(request, city: str = None):
#     """
#     Culture & heritage page with faceted search.
#     """
    
#     # ==================== START TIMING ====================
#     search_start_time = time.time()
    
#     # ==================== PARAMETERS ====================
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_topic = sanitize_filter_value(request.GET.get('topic', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
#     page = validate_page(request.GET.get('page', 1))
    
#     today = date.today()
    
#     # Base filter for culture page
#     filters = ['document_schema:=culture']
    
#     valid_topics = {'hbcus', 'film', 'music', 'art', 'literature', 'food', 'fashion', 'history', 'theater', 'events'}
#     if selected_topic and selected_topic not in valid_topics:
#         selected_topic = ''
    
#     # Topic-based filtering
#     if selected_topic == 'hbcus':
#         filters = ['document_category:=hbcu']
#     elif selected_topic == 'film':
#         filters = ['(document_schema:=culture || document_schema:=media)']
#     elif selected_topic == 'music':
#         filters = ['document_schema:=culture', 'document_category:=music']
#     elif selected_topic == 'art':
#         filters = ['document_schema:=culture', 'document_category:=art']
#     elif selected_topic == 'literature':
#         filters = ['document_schema:=culture', 'document_category:=literature']
#     elif selected_topic == 'food':
#         filters = ['document_schema:=culture', 'document_category:=food']
#     elif selected_topic == 'fashion':
#         filters = ['document_schema:=culture', 'document_category:=fashion']
#     elif selected_topic == 'history':
#         filters = ['document_schema:=culture', 'document_category:=history']
#     elif selected_topic == 'theater':
#         filters = ['document_schema:=culture', 'document_category:=theater']
#     elif selected_topic == 'events':
#         filters = ['document_schema:=culture', 'document_category:=events']
    
#     # Additional filters from sidebar
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
#     sort_by = 'authority_score:desc' if sort == 'authority' else 'created_at:desc'
    
#     # ==================== MAIN SEARCH ====================
#     results = typesense_search(
#         query=query or '*',
#         filter_by=filter_by,
#         sort_by=sort_by,
#         facet_by='document_category,document_brand',
#         per_page=18,
#         page=page,
#     )
    
#     browse_results = safe_get_hits(results)
#     total = safe_get_total(results)
#     facets = parse_facets(results)
    
#     # Calculate search time
#     search_time_ms = (time.time() - search_start_time) * 1000
    
#     # ==================== HBCU SPOTLIGHT ====================
#     hbcu_results = typesense_search(
#         query='*',
#         filter_by='document_category:=hbcu',
#         sort_by='authority_score:desc',
#         per_page=4,
#     )
#     hbcus = safe_get_hits(hbcu_results)
    
#     # ==================== FEATURED ARTICLE ====================
#     featured_results = typesense_search(
#         query='*',
#         filter_by='document_schema:=culture && document_category:=history',
#         sort_by='authority_score:desc',
#         per_page=1,
#     )
    
#     featured_article = None
#     featured_hits = safe_get_hits(featured_results)
#     if featured_hits:
#         doc = featured_hits[0].get('document', {})
#         featured_article = {
#             'title': doc.get('document_title'),
#             'excerpt': (doc.get('document_summary', '') or '')[:200],
#             'url': doc.get('document_url'),
#             'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
#         }
    
#     active_filter_count = sum([
#         bool(selected_topic),
#         bool(selected_category),
#         bool(selected_brand),
#     ])
    
#     # ==================== ANALYTICS TRACKING ====================
#     analytics = get_analytics()
    
#     # Ensure session exists (safe)
#     session_id = _safe_get_session_id(request)
    
#     # Get user info
#     user_id = str(request.user.id) if request.user.is_authenticated else None
#     device_info = _safe_get_device_info(request)
#     location = _safe_get_location(request)
#     ip_address = _safe_get_client_ip(request)
#     referrer = request.META.get('HTTP_REFERER', '')
    
#     # Generate unique request ID for this page load
#     request_id = generate_request_id(session_id)
    
#     # Track session (call on every page load)
#     if analytics:
#         try:
#             analytics.start_session(
#                 session_id=session_id,
#                 user_id=user_id,
#                 device_type=device_info.get('device_type', 'unknown'),
#                 user_agent=device_info.get('user_agent', ''),
#                 ip_address=ip_address,
#                 location=location if location else None,
#                 referrer=referrer,
#                 browser=device_info.get('browser', 'Unknown'),
#                 browser_version=device_info.get('browser_version', ''),
#                 os_name=device_info.get('os_name', 'Unknown'),
#                 os_version=device_info.get('os_version', ''),
#                 is_mobile=device_info.get('is_mobile', False),
#                 is_bot=device_info.get('is_bot', False),
#             )
#         except Exception as e:
#             logger.error(f"Failed to start session: {e}")
    
#     # Track search
#     if analytics:
#         try:
#             search_query = query if query else f"[browse:{selected_topic or 'all'}]"
            
#             analytics.track_search(
#                 session_id=session_id,
#                 query=search_query,
#                 results_count=total,
#                 alt_mode='n',
#                 user_id=user_id,
#                 location=location if location else None,
#                 device_type=device_info.get('device_type', 'unknown'),
#                 search_time_ms=search_time_ms,
#                 search_strategy='faceted',
#                 filters_applied={
#                     'topic': selected_topic,
#                     'category': selected_category,
#                     'brand': selected_brand,
#                     'sort': sort,
#                 } if any([selected_topic, selected_category, selected_brand]) else None,
#                 page=page,
#                 intent='culture_browse',
#                 request_id=request_id,
#                 browser=device_info.get('browser', 'Unknown'),
#                 browser_version=device_info.get('browser_version', ''),
#                 os_name=device_info.get('os_name', 'Unknown'),
#                 os_version=device_info.get('os_version', ''),
#                 is_mobile=device_info.get('is_mobile', False),
#                 is_bot=device_info.get('is_bot', False),
#             )
#         except Exception as e:
#             logger.error(f"Failed to track search: {e}")
    
#     # Track filter/topic selection as events
#     if analytics and selected_topic:
#         try:
#             analytics.track_event(
#                 session_id=session_id,
#                 event_type='topic_selected',
#                 event_data={
#                     'topic': selected_topic,
#                     'page': 'culture',
#                 },
#                 user_id=user_id,
#                 location=location if location else None,
#             )
#         except Exception as e:
#             logger.error(f"Failed to track topic event: {e}")
    
#     if analytics and (selected_category or selected_brand):
#         try:
#             analytics.track_event(
#                 session_id=session_id,
#                 event_type='filter_applied',
#                 event_data={
#                     'category': selected_category,
#                     'brand': selected_brand,
#                     'page': 'culture',
#                 },
#                 user_id=user_id,
#                 location=location if location else None,
#             )
#         except Exception as e:
#             logger.error(f"Failed to track filter event: {e}")
    
#     # ==================== BUILD CONTEXT ====================
#     context = {
#         'city': city,
#         'query': query,
#         'today': {'day': today.day, 'month': today.strftime('%b').upper()},
#         'featured_article': featured_article,
#         'hbcus': hbcus,
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
#         'selected_topic': selected_topic,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
#         'page': page,
#         'has_more': (page * 18) < total,
#         'sort': sort,
        
#         # Analytics context for client-side tracking
#         'analytics_context': {
#             'session_id': session_id,
#             'request_id': request_id,
#             'query': query,
#             'results_count': total,
#             'page': page,
#             'user_id': user_id or '',
#         },
#     }
    
#     return render(request, 'category_culture.html', context)


# # =============================================================================
# # VIEW: HEALTH CATEGORY
# # =============================================================================

# def health_category(request, city: str):
#     """Health resources page with faceted search."""
    
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent'], 'authority')
#     page = validate_page(request.GET.get('page', 1))
    
#     filters = ['document_schema:=health']
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
#     sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
#     results = typesense_search(
#         query=query or '*',
#         filter_by=filter_by,
#         sort_by=sort_by,
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     providers = safe_get_hits(results)
#     total = safe_get_total(results)
#     facets = parse_facets(results)
    
#     active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
#     context = {
#         'city': city,
#         'query': query,
#         'providers': providers,
#         'results': providers,
#         'total': total,
#         'facets': facets,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
#         'page': page,
#         'has_more': (page * 20) < total,
#         'sort': sort,
#     }
    
#     return render(request, 'category_health.html', context)


# # =============================================================================
# # VIEW: NEWS CATEGORY
# # =============================================================================

# def news_category(request, city: str = DEFAULT_CITY):
#     """News & media page with faceted search."""
    
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_section = sanitize_filter_value(request.GET.get('section', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', 'recent'), ['authority', 'recent'], 'recent')
#     page = validate_page(request.GET.get('page', 1))
    
#     section_category_map = {
#         'local': None,
#         'national': 'national',
#         'politics': 'politics',
#         'business': 'business',
#         'sports': 'sports',
#         'entertainment': 'entertainment',
#         'opinion': 'opinion',
#     }
    
#     if selected_section and selected_section not in section_category_map:
#         selected_section = ''
    
#     filters = ['document_schema:=news']
    
#     if selected_section and selected_section in section_category_map:
#         section_cat = section_category_map[selected_section]
#         if section_cat:
#             filters.append(f'document_category:={section_cat}')
    
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
#     sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
#     results = typesense_search(
#         query=query or '*',
#         filter_by=filter_by,
#         sort_by=sort_by,
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     browse_results = safe_get_hits(results)
#     total = safe_get_total(results)
#     facets = parse_facets(results)
    
#     top_results = typesense_search(
#         query='*',
#         filter_by='document_schema:=news',
#         sort_by='authority_score:desc',
#         per_page=1,
#     )
#     top_hits = safe_get_hits(top_results)
#     top_story = top_hits[0].get('document') if top_hits else None
    
#     sidebar_results = typesense_search(
#         query='*',
#         filter_by='document_schema:=news',
#         sort_by='created_at:desc',
#         per_page=4,
#     )
#     sidebar_hits = safe_get_hits(sidebar_results)
#     sidebar_stories = [hit.get('document', {}) for hit in sidebar_hits[1:] if hit.get('document')]
    
#     local_results = typesense_search(
#         query=city,
#         filter_by='document_schema:=news',
#         sort_by='created_at:desc',
#         per_page=5,
#     )
#     local_news = [hit.get('document', {}) for hit in safe_get_hits(local_results) if hit.get('document')]
    
#     good_results = typesense_search(
#         query='success achievement award grant scholarship wins first',
#         filter_by='document_schema:=news',
#         sort_by='created_at:desc',
#         per_page=5,
#     )
#     good_news = [hit.get('document', {}) for hit in safe_get_hits(good_results) if hit.get('document')]
    
#     opinion_results = typesense_search(
#         query='*',
#         filter_by='document_schema:=news && document_category:=opinion',
#         sort_by='created_at:desc',
#         per_page=3,
#     )
#     opinions = [hit.get('document', {}) for hit in safe_get_hits(opinion_results) if hit.get('document')]
    
#     news_sources = [f['value'] for f in facets.get('brand', [])[:12]]
#     active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
#     context = {
#         'city': city,
#         'query': query,
#         'top_story': top_story,
#         'sidebar_stories': sidebar_stories,
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
#         'local_news': local_news,
#         'good_news': good_news,
#         'opinions': opinions,
#         'news_sources': news_sources,
#         'selected_section': selected_section,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
#         'page': page,
#         'has_more': (page * 20) < total,
#         'sort': sort,
#     }
    
#     return render(request, 'category_news.html', context)


# # =============================================================================
# # VIEW: COMMUNITY CATEGORY
# # =============================================================================

# def community_category(request):
#     """Community hub with faceted search and dynamic city."""
    
#     city = get_user_city(request)
    
#     query = sanitize_query(request.GET.get('q', ''))
#     selected_category = sanitize_filter_value(request.GET.get('category', ''))
#     selected_brand = sanitize_filter_value(request.GET.get('brand', ''))
#     sort = validate_sort(request.GET.get('sort', ''), ['authority', 'recent', 'name'], 'authority')
#     page = validate_page(request.GET.get('page', 1))
    
#     help_category_map = {
#         'housing': 'housing',
#         'legal': 'legal',
#         'jobs': 'employment',
#         'food': 'food',
#         'family': 'family',
#         'youth': 'youth',
#         'faith': 'faith',
#         'nonprofit': 'nonprofit',
#         'social_justice': 'social_justice',
#     }
    
#     filters = ['document_schema:=community']
#     if selected_category:
#         mapped = help_category_map.get(selected_category, selected_category)
#         filters.append(f'document_category:={mapped}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = build_filter_string(filters)
    
#     sort_options = {
#         'authority': 'authority_score:desc',
#         'recent': 'created_at:desc',
#         'name': 'document_title:asc',
#     }
#     sort_by = sort_options.get(sort, 'authority_score:desc')
    
#     search_query = query if query else city
#     results = typesense_search(
#         query=search_query,
#         filter_by=filter_by,
#         sort_by=sort_by,
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     browse_results = safe_get_hits(results)
#     total = safe_get_total(results)
#     facets = parse_facets(results)
    
#     event_results = typesense_search(
#         query=f'{city} event meeting workshop conference',
#         filter_by='document_schema:=community',
#         sort_by='created_at:desc',
#         per_page=5,
#     )
    
#     upcoming_events = []
#     today = date.today()
#     for i, hit in enumerate(safe_get_hits(event_results)):
#         event = dict(hit)
#         future_date = today + timedelta(days=i * 5 + 3)
#         event['month'] = future_date.strftime('%b').upper()
#         event['day'] = future_date.day
#         event['time'] = '10 AM - 2 PM'
#         upcoming_events.append(event)
    
#     church_results = typesense_search(
#         query=f'{city} church baptist AME methodist faith ministry',
#         filter_by='document_schema:=community',
#         sort_by='authority_score:desc',
#         per_page=8,
#     )
#     churches = safe_get_hits(church_results)
    
#     org_results = typesense_search(
#         query=f'{city} NAACP Urban League organization nonprofit foundation',
#         filter_by='document_schema:=community',
#         sort_by='authority_score:desc',
#         per_page=4,
#     )
    
#     organizations = []
#     for hit in safe_get_hits(org_results):
#         org = dict(hit)
#         title = org.get('document', {}).get('document_title', '')
#         words = title.split()[:2]
#         org['initials'] = ''.join([w[0] for w in words if w]).upper()[:3]
#         organizations.append(org)
    
#     vol_results = typesense_search(
#         query=f'{city} volunteer service help mentor tutor',
#         filter_by='document_schema:=community',
#         sort_by='created_at:desc',
#         per_page=5,
#     )
    
#     volunteer_ops = []
#     schedules = ['Weekly', 'Ongoing', 'Flexible', 'One-time', 'Monthly']
#     for i, hit in enumerate(safe_get_hits(vol_results)):
#         vol = dict(hit)
#         vol['schedule'] = 'Flexible'
#         vol['type'] = schedules[i % len(schedules)]
#         volunteer_ops.append(vol)
    
#     active_filter_count = sum([bool(selected_category), bool(selected_brand)])
    
#     context = {
#         'city': city,
#         'supported_cities': list(SUPPORTED_CITIES),
#         'query': query,
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
#         'upcoming_events': upcoming_events,
#         'churches': churches,
#         'organizations': organizations,
#         'volunteer_ops': volunteer_ops,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
#         'page': page,
#         'has_more': (page * 20) < total,
#         'sort': sort,
#     }
    
#     return render(request, 'category_community.html', context)


# # =============================================================================
# # API ENDPOINTS
# # =============================================================================

# @require_GET
# def business_search_api(request):
#     """AJAX API for business search."""
    
#     city = sanitize_filter_value(request.GET.get('city', DEFAULT_CITY))
#     if city not in SUPPORTED_CITIES:
#         city = DEFAULT_CITY
    
#     query = sanitize_query(request.GET.get('q', ''))
#     subcategory = sanitize_filter_value(request.GET.get('subcategory', ''))
#     category = sanitize_filter_value(request.GET.get('category', ''))
#     brand = sanitize_filter_value(request.GET.get('brand', ''))
#     page = validate_page(request.GET.get('page', 1))
    
#     if subcategory and subcategory not in SUBCATEGORY_MAP:
#         subcategory = ''
    
#     search_query = query if query else '*'
#     if subcategory and subcategory in SUBCATEGORY_MAP:
#         terms = ' '.join(SUBCATEGORY_MAP[subcategory])
#         search_query = f"{search_query} {terms}" if search_query != '*' else terms
#     if search_query == '*':
#         search_query = city
    
#     filters = ['document_schema:=business']
#     if category:
#         filters.append(f'document_category:={category}')
#     if brand:
#         filters.append(f'document_brand:={brand}')
    
#     results = typesense_search(
#         query=search_query,
#         filter_by=build_filter_string(filters),
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     if not results:
#         return JsonResponse({'success': False, 'error': 'Search service unavailable'}, status=503)
    
#     hits = []
#     for hit in safe_get_hits(results):
#         doc = hit.get('document', {})
#         hits.append({
#             'id': doc.get('id'),
#             'title': doc.get('document_title'),
#             'summary': (doc.get('document_summary') or '')[:150],
#             'url': doc.get('document_url'),
#             'category': doc.get('document_category'),
#             'brand': doc.get('document_brand'),
#             'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
#             'keywords': doc.get('primary_keywords', [])[:3],
#         })
    
#     return JsonResponse({
#         'success': True,
#         'hits': hits,
#         'total': safe_get_total(results),
#         'facets': parse_facets(results),
#         'page': page,
#     })


# @require_GET
# def community_search_api(request):
#     """AJAX API for community search."""
    
#     city = sanitize_filter_value(request.GET.get('city', DEFAULT_CITY))
#     if city not in SUPPORTED_CITIES:
#         city = DEFAULT_CITY
    
#     query = sanitize_query(request.GET.get('q', ''))
#     category = sanitize_filter_value(request.GET.get('category', ''))
#     brand = sanitize_filter_value(request.GET.get('brand', ''))
#     page = validate_page(request.GET.get('page', 1))
    
#     filters = ['document_schema:=community']
#     if category:
#         filters.append(f'document_category:={category}')
#     if brand:
#         filters.append(f'document_brand:={brand}')
    
#     results = typesense_search(
#         query=query if query else city,
#         filter_by=build_filter_string(filters),
#         facet_by='document_category,document_brand',
#         per_page=20,
#         page=page,
#     )
    
#     if not results:
#         return JsonResponse({'success': False, 'error': 'Search service unavailable'}, status=503)
    
#     hits = []
#     for hit in safe_get_hits(results):
#         doc = hit.get('document', {})
#         hits.append({
#             'id': doc.get('id'),
#             'title': doc.get('document_title'),
#             'summary': (doc.get('document_summary') or '')[:150],
#             'url': doc.get('document_url'),
#             'category': doc.get('document_category'),
#             'brand': doc.get('document_brand'),
#             'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
#         })
    
#     return JsonResponse({
#         'success': True,
#         'hits': hits,
#         'total': safe_get_total(results),
#         'facets': parse_facets(results),
#         'page': page,
#     })


# @require_GET
# def facets_api(request):
#     """
#     API endpoint to get facet counts for dynamic tabs.
    
#     Usage: /api/facets/?query=hair+salon
    
#     Returns facet counts for data_type, category, and schema.
#     """
#     query = sanitize_query(request.GET.get('query', '') or request.GET.get('q', ''))
    
#     if not query:
#         return JsonResponse({'facets': {}})
    
#     try:
#         facets = get_dynamic_tab_facets(query)
        
#         return JsonResponse({
#             'success': True,
#             'facets': {
#                 'data_types': facets.get('data_type', []),
#                 'categories': facets.get('category', []),
#                 'schemas': facets.get('schema', []),
#             }
#         })
        
#     except Exception as e:
#         logger.error(f"Facets API error: {e}")
#         return JsonResponse({'success': False, 'error': str(e)}, status=500)


# # =============================================================================
# # DETAIL VIEWS
# # =============================================================================

# def business_detail(request, business_id: str):
#     """Individual business detail page."""
    
#     if not business_id or len(business_id) > 100:
#         raise Http404("Invalid business ID")
    
#     business_id = re.sub(r'[^a-zA-Z0-9_-]', '', business_id)
    
#     if not business_id:
#         raise Http404("Invalid business ID")
    
#     doc = typesense_manager.get_document(COLLECTION_NAME, business_id)
    
#     if not doc:
#         raise Http404("Business not found")
    
#     category = doc.get('document_category', '')
#     related_results = typesense_search(
#         query=category,
#         filter_by=f"document_schema:=business && id:!={business_id}",
#         per_page=4,
#         query_by='document_category,keywords',
#     )
#     related = [hit.get('document', {}) for hit in safe_get_hits(related_results) if hit.get('document')]
    
#     return render(request, 'business_detail.html', {
#         'business': doc,
#         'related': related,
#     })


# def community_by_city(request, city_slug: str):
#     """Community page by city slug URL."""
    
#     city_map = {
#         'atlanta': 'Atlanta',
#         'houston': 'Houston',
#         'chicago': 'Chicago',
#         'detroit': 'Detroit',
#         'new-york': 'New York',
#         'los-angeles': 'Los Angeles',
#         'philadelphia': 'Philadelphia',
#         'washington-dc': 'Washington',
#     }
    
#     city_slug = str(city_slug).lower().strip()
#     city = city_map.get(city_slug, DEFAULT_CITY)
    
#     try:
#         request.session['user_city'] = city
#     except Exception:
#         pass
    
#     return community_category(request)


# # =============================================================================
# # LEGACY ENDPOINTS
# # =============================================================================

# @require_GET
# def form_submit(request):
#     """Legacy form submit endpoint."""
#     query = sanitize_query(request.GET.get('query', ''))
#     session_id = request.GET.get('session_id', '')[:50]
    
#     if not query:
#         return JsonResponse({'error': 'No query provided'}, status=400)
    
#     if process_search_submission is None:
#         return JsonResponse({'error': 'Service unavailable'}, status=503)
    
#     try:
#         result = process_search_submission(query, session_id)
#         return JsonResponse(result)
#     except Exception as e:
#         logger.error(f"Form submit error: {e}")
#         return JsonResponse({'error': 'Search failed'}, status=500)


# @require_GET
# def search_api(request):
#     """JSON API endpoint for programmatic access."""
#     query = sanitize_query(request.GET.get('q', '') or request.GET.get('query', ''))
#     session_id = request.GET.get('session_id', '')[:50]
    
#     if not query:
#         return JsonResponse({'error': 'No query provided'}, status=400)
    
#     if word_discovery_multi is None or process_search_submission is None:
#         return JsonResponse({'error': 'Service unavailable'}, status=503)
    
#     try:
#         corrections, tuple_array, corrected_query = word_discovery_multi(query)
#         result = process_search_submission(corrected_query, session_id)
        
#         result['word_discovery'] = {
#             'original_query': query,
#             'corrected_query': corrected_query,
#             'corrections': corrections,
#             'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
#             'was_corrected': query.lower() != corrected_query.lower()
#         }
        
#         return JsonResponse(result)
#     except Exception as e:
#         logger.error(f"Search API error: {e}")
#         return JsonResponse({'error': 'Search failed'}, status=500)


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def build_word_corrections(original: str, corrected: str) -> List[Dict[str, Any]]:
#     """Build word-by-word correction display."""
#     word_corrections = []
#     original_words = original.lower().split()
#     corrected_words = corrected.lower().split()
    
#     for i, orig_word in enumerate(original_words):
#         corr_word = corrected_words[i] if i < len(corrected_words) else orig_word
#         word_corrections.append({
#             'original': orig_word,
#             'corrected': corr_word,
#             'was_changed': orig_word != corr_word
#         })
    
#     return word_corrections


# def handle_zero_results(original_query: str, corrected_query: str, filters: Dict) -> List[Dict[str, Any]]:
#     """Provide helpful suggestions when no results found."""
#     suggestions = []
    
#     if filters:
#         suggestions.append({
#             'type': 'remove_filters',
#             'message': 'Try removing some filters',
#             'action_query': corrected_query,
#             'action_filters': {}
#         })
    
#     words = corrected_query.split()
#     if len(words) > 2:
#         suggestions.append({
#             'type': 'broader_search',
#             'message': 'Try a broader search',
#             'action_query': ' '.join(words[:2])
#         })
    
#     if original_query.lower() != corrected_query.lower():
#         suggestions.append({
#             'type': 'try_original',
#             'message': f'Search for "{escape(original_query)}" instead',
#             'action_query': original_query
#         })
    
#     suggestions.append({
#         'type': 'help',
#         'message': 'Check your spelling or try different keywords'
#     })
    
#     return suggestions


# def categorize_results(results: List[Dict]) -> Dict[str, List[Dict]]:
#     """Group results by type for different display treatments."""
#     categorized: Dict[str, List[Dict]] = {
#         'articles': [],
#         'videos': [],
#         'products': [],
#         'people': [],
#         'places': [],
#         'services': [],
#         'other': []
#     }
    
#     type_mapping = {
#         'article': 'articles',
#         'content': 'articles',
#         'video': 'videos',
#         'media': 'videos',
#         'product': 'products',
#         'person': 'people',
#         'place': 'places',
#         'location': 'places',
#         'service': 'services'
#     }
    
#     for result in results:
#         data_type = result.get('data_type', '') or result.get('document_data_type', 'other')
#         category = type_mapping.get(data_type, 'other')
#         categorized[category].append(result)
    
#     return {k: v for k, v in categorized.items() if v}


# def build_pagination(page: int, per_page: int, total: int) -> Dict[str, Any]:
#     """Build pagination info for template."""
#     total_pages = max(1, (total + per_page - 1) // per_page)
#     page = min(page, total_pages)
    
#     start_page = max(1, page - 2)
#     end_page = min(total_pages, page + 2)
    
#     return {
#         'current_page': page,
#         'total_pages': total_pages,
#         'has_previous': page > 1,
#         'has_next': page < total_pages,
#         'previous_page': max(1, page - 1),
#         'next_page': min(total_pages, page + 1),
#         'page_range': list(range(start_page, end_page + 1)),
#         'show_first': start_page > 1,
#         'show_last': end_page < total_pages,
#         'start_result': min((page - 1) * per_page + 1, total) if total > 0 else 0,
#         'end_result': min(page * per_page, total),
#         'total_results': total
#     }


# def add_time_ago(documents: List[Dict]) -> List[Dict]:
#     """Add time_ago field to documents for display."""
#     now = datetime.now(timezone.utc)
    
#     for doc in documents:
#         created_at = doc.get('created_at')
#         if created_at:
#             try:
#                 if isinstance(created_at, str):
#                     created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
#                 else:
#                     created = created_at
                
#                 diff = now - created
                
#                 if diff.days > 30:
#                     doc['time_ago'] = f"{diff.days // 30}mo ago"
#                 elif diff.days > 0:
#                     doc['time_ago'] = f"{diff.days}d ago"
#                 elif diff.seconds >= 3600:
#                     doc['time_ago'] = f"{diff.seconds // 3600}h ago"
#                 elif diff.seconds >= 60:
#                     doc['time_ago'] = f"{diff.seconds // 60}m ago"
#                 else:
#                     doc['time_ago'] = "Just now"
#             except Exception:
#                 doc['time_ago'] = "Today"
#         else:
#             doc['time_ago'] = "Today"
    
#     return documents


# # =============================================================================
# # UTILITY FUNCTIONS FOR OTHER MODULES
# # =============================================================================

# def get_featured_businesses(city: str = DEFAULT_CITY, limit: int = 6) -> List[Dict]:
#     """Get featured businesses for homepage."""
#     results = typesense_search(
#         query=city,
#         filter_by='document_schema:=business',
#         sort_by='authority_score:desc',
#         per_page=limit,
#     )
#     return [hit.get('document', {}) for hit in safe_get_hits(results) if hit.get('document')]


# def get_business_categories() -> List[Dict[str, Any]]:
#     """Get all unique business categories with counts."""
#     results = typesense_search(
#         query='*',
#         filter_by='document_schema:=business',
#         facet_by='document_category',
#         per_page=0,
#     )
    
#     if not results:
#         return []
    
#     for facet in results.get('facet_counts', []):
#         if facet['field_name'] == 'document_category':
#             return [
#                 {'name': c['value'], 'count': c['count']}
#                 for c in facet['counts']
#             ]
    
#     return []


# def get_trending_searches(category: str, city: str) -> List[str]:
#     """Get trending searches from Redis."""
#     cache_key = f"trending:{category}:{city.lower()}"
#     results = redis_manager.safe_zrevrange(cache_key, 0, 7)
#     return list(results) if results else []


# def get_featured_listings(category: str, city: str) -> List[Dict]:
#     """Get featured/promoted listings from Redis cache."""
#     cache_key = f"featured:{category}:{city.lower()}"
#     cached = redis_manager.safe_get(cache_key)
#     if cached:
#         try:
#             return json.loads(cached)
#         except (json.JSONDecodeError, TypeError):
#             pass
#     return []


# def get_category_stats(category: str, city: str) -> Dict[str, str]:
#     """Get category statistics from Redis cache."""
#     cache_key = f"stats:{category}:{city.lower()}"
#     return redis_manager.safe_hgetall(cache_key)


# # =============================================================================
# # VIEW: TRACK CLICK (Analytics)
# # =============================================================================

# @csrf_exempt
# @require_http_methods(["POST", "GET"])
# def track_click(request):
#     """Track when a user clicks on a search result or other events."""
    
#     if request.method == 'POST':
#         try:
#             content_length = request.META.get('CONTENT_LENGTH', 0)
#             try:
#                 content_length = int(content_length)
#             except (TypeError, ValueError):
#                 content_length = 0
            
#             if content_length > TRACK_CLICK_CONFIG['max_event_data_size']:
#                 return JsonResponse(
#                     {'success': False, 'error': 'Request body too large'},
#                     status=413
#                 )
            
#             data = json.loads(request.body)
#         except (json.JSONDecodeError, ValueError):
#             data = request.POST.dict()
#     else:
#         data = request.GET.dict()
    
#     client_ip = get_client_ip(request)
    
#     session_id = sanitize_string(
#         data.get('session_id', ''),
#         max_length=TRACK_CLICK_CONFIG['max_session_id_length']
#     )
    
#     is_allowed, rate_error = TrackClickRateLimiter.check_rate_limit(session_id, client_ip)
#     if not is_allowed:
#         return JsonResponse(
#             {'success': False, 'error': rate_error},
#             status=429
#         )
    
#     event_type = sanitize_string(data.get('event_type', ''), max_length=50)
    
#     if event_type and event_type != 'click':
#         analytics = get_analytics()
#         if analytics:
#             try:
#                 if session_id:
#                     location = get_location_from_request(request)
                    
#                     user_id = None
#                     if hasattr(request, 'user') and request.user.is_authenticated:
#                         user_id = str(request.user.id)
                    
#                     sanitized_data = {}
#                     for key, value in data.items():
#                         if isinstance(value, str):
#                             sanitized_data[key] = value[:500]
#                         elif isinstance(value, (int, float, bool)):
#                             sanitized_data[key] = value
#                         elif isinstance(value, dict):
#                             sanitized_data[key] = {
#                                 k[:100]: str(v)[:500] 
#                                 for k, v in list(value.items())[:20]
#                             }
                    
#                     analytics.track_event(
#                         session_id=session_id,
#                         event_type=event_type,
#                         event_data=sanitized_data,
#                         user_id=user_id,
#                         location=location
#                     )
#                 return JsonResponse({'success': True})
#             except Exception as e:
#                 logger.error(f"Event tracking error: {e}")
#                 return JsonResponse(
#                     {'success': False, 'error': 'Event tracking failed'},
#                     status=500
#                 )
#         return JsonResponse({'success': True})
    
#     clicked_url = sanitize_url(
#         data.get('url', ''),
#         max_length=TRACK_CLICK_CONFIG['max_url_length']
#     )
#     query = sanitize_string(
#         data.get('query', ''),
#         max_length=TRACK_CLICK_CONFIG['max_query_length']
#     )
    
#     if not session_id:
#         return JsonResponse(
#             {'success': False, 'error': 'Missing session_id'},
#             status=400
#         )
    
#     if not clicked_url:
#         return JsonResponse(
#             {'success': False, 'error': 'Missing or invalid URL'},
#             status=400
#         )
    
#     clicked_position = sanitize_int(data.get('position', 0), default=0, min_val=0, max_val=1000)
#     result_id = sanitize_string(
#         data.get('result_id', ''),
#         max_length=TRACK_CLICK_CONFIG['max_result_id_length']
#     )
#     result_title = sanitize_string(
#         data.get('title', ''),
#         max_length=TRACK_CLICK_CONFIG['max_title_length']
#     )
#     result_source = sanitize_string(
#         data.get('source', ''),
#         max_length=TRACK_CLICK_CONFIG['max_source_length']
#     )
#     search_request_id = sanitize_string(
#         data.get('request_id', ''),
#         max_length=TRACK_CLICK_CONFIG['max_request_id_length']
#     )
    
#     results_count = sanitize_int(data.get('results_count', 0), default=0, min_val=0, max_val=10000)
    
#     was_corrected = str(data.get('was_corrected', 'false')).lower() == 'true'
#     corrected_query = sanitize_string(
#         data.get('corrected_query', ''),
#         max_length=TRACK_CLICK_CONFIG['max_corrected_query_length']
#     )
    
#     time_to_click_ms = None
#     raw_time = data.get('time_to_click_ms')
#     if raw_time is not None:
#         time_to_click_ms = sanitize_int(raw_time, default=0, min_val=0, max_val=3600000)
#         if time_to_click_ms == 0:
#             time_to_click_ms = None
    
#     user_id = None
#     if hasattr(request, 'user') and request.user.is_authenticated:
#         user_id = str(request.user.id)
    
#     location = get_location_from_request(request)
    
#     analytics = get_analytics()
#     if analytics:
#         try:
#             analytics.track_click(
#                 session_id=session_id,
#                 query=query,
#                 clicked_url=clicked_url,
#                 clicked_position=clicked_position,
#                 result_id=result_id,
#                 result_title=result_title,
#                 result_source=result_source,
#                 user_id=user_id,
#                 time_to_click_ms=time_to_click_ms,
#                 location=location,
#                 search_request_id=search_request_id,
#                 results_count=results_count,
#                 was_corrected=was_corrected,
#                 corrected_query=corrected_query
#             )
            
#             return JsonResponse({'success': True})
#         except Exception as e:
#             logger.error(f"Click tracking error: {e}")
#             return JsonResponse(
#                 {'success': False, 'error': 'Tracking failed'},
#                 status=500
#             )
    
#     return JsonResponse(
#         {'success': False, 'error': 'Analytics not available'},
#         status=503
#     )


# @require_GET
# def click_redirect(request):
#     """Redirect-based click tracking."""
    
#     destination_url = sanitize_url(
#         request.GET.get('url', ''),
#         max_length=TRACK_CLICK_CONFIG['max_url_length']
#     )
    
#     if not destination_url:
#         return HttpResponseBadRequest('Missing or invalid URL parameter')
    
#     client_ip = get_client_ip(request)
    
#     session_id = sanitize_string(
#         request.GET.get('session_id', ''),
#         max_length=TRACK_CLICK_CONFIG['max_session_id_length']
#     )
    
#     is_allowed, _ = TrackClickRateLimiter.check_rate_limit(session_id, client_ip)
    
#     if is_allowed:
#         query = sanitize_string(
#             request.GET.get('query', ''),
#             max_length=TRACK_CLICK_CONFIG['max_query_length']
#         )
#         clicked_position = sanitize_int(request.GET.get('position', 0), default=0, min_val=0, max_val=1000)
#         result_id = sanitize_string(
#             request.GET.get('result_id', ''),
#             max_length=TRACK_CLICK_CONFIG['max_result_id_length']
#         )
#         result_title = sanitize_string(
#             request.GET.get('title', ''),
#             max_length=TRACK_CLICK_CONFIG['max_title_length']
#         )
#         result_source = sanitize_string(
#             request.GET.get('source', ''),
#             max_length=TRACK_CLICK_CONFIG['max_source_length']
#         )
#         search_request_id = sanitize_string(
#             request.GET.get('request_id', ''),
#             max_length=TRACK_CLICK_CONFIG['max_request_id_length']
#         )
        
#         results_count = sanitize_int(request.GET.get('results_count', 0), default=0, min_val=0, max_val=10000)
#         was_corrected = request.GET.get('was_corrected', 'false').lower() == 'true'
#         corrected_query = sanitize_string(
#             request.GET.get('corrected_query', ''),
#             max_length=TRACK_CLICK_CONFIG['max_corrected_query_length']
#         )
        
#         user_id = None
#         if hasattr(request, 'user') and request.user.is_authenticated:
#             user_id = str(request.user.id)
        
#         location = get_location_from_request(request)
        
#         analytics = get_analytics()
#         if analytics and session_id:
#             try:
#                 analytics.track_click(
#                     session_id=session_id,
#                     query=query,
#                     clicked_url=destination_url,
#                     clicked_position=clicked_position,
#                     result_id=result_id,
#                     result_title=result_title,
#                     result_source=result_source,
#                     user_id=user_id,
#                     time_to_click_ms=None,
#                     location=location,
#                     search_request_id=search_request_id,
#                     results_count=results_count,
#                     was_corrected=was_corrected,
#                     corrected_query=corrected_query
#                 )
#             except Exception as e:
#                 logger.warning(f"Click redirect tracking error: {e}")
    
#     return redirect(destination_url)


# # =============================================================================
# # FOOTER FUNCTIONS
# # =============================================================================

# def about(request):
#     return render(request, 'about.html')


# def privacy(request):
#     return render(request, 'privacy.html')


# def term(request):
#     return render(request, 'terms.html')


# def contact(request):
#     return render(request, 'contact.html')










# # ============================================================
# # views_debug.py — DEBUG ENDPOINTS (ASYNC-COMPATIBLE)
# # ============================================================
# #
# # UPDATED FOR WORD DISCOVERY v4 PIPELINE:
# #   - debug_keyword, debug_semantic, debug_question are unchanged
# #     because they consume the same profile dict output.
# #   - debug_word_discovery is fully rewritten to trace the v4
# #     4-step pipeline:
# #       Step 1: Tokenize + Known Sets + RAM Hash Lookup
# #       Step 2: Clean + Resolve (corrections → POS → "in" disambig)
# #       Step 3: N-gram Resolution (single pass)
# #       Step 4: Build Profile + Intent
# #
# # REQUIREMENTS:
# #   - Django 4.1+ (for async view support)
# # ============================================================


# import time
# import traceback
# import asyncio
# from typing import Optional

# from django.http import JsonResponse
# from django.views.decorators.http import require_GET

# # ── typesense client (for debug_question raw search) ─────────────────────────

# import typesense
# from decouple import config

# client = typesense.Client({
#     'api_key':  config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host':     config('TYPESENSE_HOST'),
#         'port':     config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL'),
#     }],
#     'connection_timeout_seconds': 5,
# })


# # ── Intent Detection ──────────────────────────────────────────────────────────

# try:
#     from .intent_detect import detect_intent
#     INTENT_AVAILABLE = True
# except ImportError:
#     INTENT_AVAILABLE = False
#     detect_intent = None


# # ── Bridge imports ────────────────────────────────────────────────────────────

# try:
#     from .typesense_bridge_v3 import (
#         # Async functions
#         _run_word_discovery,
#         _run_embedding,
#         run_parallel_prep,
#         fetch_candidate_uuids,
#         fetch_candidate_uuids_from_questions,
#         semantic_rerank_candidates,
#         fetch_candidate_metadata,
#         fetch_candidates_with_metadata,
#         fetch_full_documents,
#         # Sync functions (CPU-only)
#         _read_v3_profile,
#         build_typesense_params,
#         _resolve_blend,
#         _extract_authority_score,
#         _compute_text_score,
#         _compute_semantic_score,
#         _domain_relevance,
#         _content_intent_match,
#         _pool_type_multiplier,
#         _score_document,
#         count_all,
#         # Validation functions (sync, CPU-only)
#         _normalize_signal,
#         _extract_query_signals,
#         _validate_question_hit,
#         # Constants
#         BLEND_RATIOS,
#         SEMANTIC_DISTANCE_GATE,
#         QUESTION_SEMANTIC_DISTANCE_GATE,
#     )
#     DEBUG_BRIDGE_AVAILABLE = True
# except ImportError as e:
#     DEBUG_BRIDGE_AVAILABLE = False
#     DEBUG_BRIDGE_IMPORT_ERROR = str(e)
#     print(f"⚠️ typesense_bridge_v3 debug imports not available: {e}")


# # ============================================================
# # DEBUG HELPER FUNCTIONS
# # ============================================================

# def _infer_domain(signals: dict) -> Optional[str]:
#     """Infer primary_domain from intent signals."""
#     if signals.get('has_food_word'):
#         return 'food'
#     if signals.get('has_beauty_word'):
#         return 'beauty'
#     if signals.get('has_entertainment_word'):
#         return 'music'
#     if signals.get('has_culture_word'):
#         return 'culture'
#     if signals.get('has_service_word') and signals.get('is_local_search'):
#         return 'business'
#     return signals.get('primary_domain')


# def _patch_signals_domain(signals: dict) -> dict:
#     """Return a copy of signals with primary_domain filled in if null."""
#     if signals.get('primary_domain'):
#         return signals
#     patched = dict(signals)
#     patched['primary_domain'] = _infer_domain(signals)
#     return patched


# def _build_full_result(
#     scored_item: dict,
#     full_doc: dict,
#     signals: dict,
#     source_pool: str = 'document'
# ) -> dict:
#     """Merge a scored metadata item with its full Typesense document."""
#     query_mode = signals.get('query_mode', 'explore')

#     dom_m  = _domain_relevance(scored_item, signals)
#     cint_m = _content_intent_match(scored_item, query_mode)
#     pool_m = _pool_type_multiplier(scored_item, query_mode)

#     return {
#         'rank':          scored_item.get('rank', 0),
#         'document_uuid': scored_item.get('id', ''),
#         'source_pool':   source_pool,

#         'title':                full_doc.get('title', scored_item.get('title', '(no title)')),
#         'data_type':            full_doc.get('data_type', scored_item.get('data_type', '')),
#         'category':             full_doc.get('category', scored_item.get('category', '')),
#         'schema':               full_doc.get('schema', scored_item.get('schema', '')),
#         'url':                  full_doc.get('url', ''),
#         'summary':              full_doc.get('summary', ''),
#         'source':               full_doc.get('source', ''),
#         'black_owned':          full_doc.get('black_owned', scored_item.get('black_owned', False)),
#         'service_type':         full_doc.get('service_type', []),
#         'service_specialties':  full_doc.get('service_specialties', []),
#         'service_rating':       full_doc.get('service_rating'),
#         'service_review_count': full_doc.get('service_review_count'),
#         'service_price_range':  full_doc.get('service_price_range'),
#         'service_hours':        full_doc.get('service_hours'),
#         'location': {
#             'city':    full_doc.get('location', {}).get('city'),
#             'state':   full_doc.get('location', {}).get('state'),
#             'address': full_doc.get('location', {}).get('address'),
#         },
#         'key_facts':     full_doc.get('key_facts', []),
#         'image_url':     full_doc.get('image_url', []),
#         'logo_url':      full_doc.get('logo_url', []),
#         'published_date': full_doc.get('published_date', ''),
#         'semantic_uuid': full_doc.get('semantic_uuid', ''),

#         'scores': {
#             'blended':         round(scored_item.get('blended_score', 0), 4),
#             'text':            round(scored_item.get('text_score', 0), 4),
#             'semantic':        round(scored_item.get('sem_score', 0), 4),
#             'authority':       round(scored_item.get('auth_score', 0), 4),
#             'vector_distance': round(scored_item.get('vector_distance', 1.0), 4),
#         },

#         'multipliers': {
#             'domain_relevance':    round(dom_m, 3),
#             'content_intent':      round(cint_m, 3),
#             'pool_type':           round(pool_m, 3),
#             'combined_multiplier': round(dom_m * cint_m * pool_m, 3),
#         },

#         'authority_inputs': {
#             'authority_score':       scored_item.get('authority_score', 0),
#             'service_rating':        scored_item.get('service_rating', 0),
#             'service_review_count':  scored_item.get('service_review_count', 0),
#             'product_rating':        scored_item.get('product_rating', 0),
#             'product_review_count':  scored_item.get('product_review_count', 0),
#             'recipe_rating':         scored_item.get('recipe_rating', 0),
#             'recipe_review_count':   scored_item.get('recipe_review_count', 0),
#             'media_rating':          scored_item.get('media_rating', 0),
#             'factual_density_score': scored_item.get('factual_density_score', 0),
#             'evergreen_score':       scored_item.get('evergreen_score', 0),
#         },

#         'why_this_score': {
#             'domain_match':    f"primary_domain={signals.get('primary_domain')} "
#                                f"category={scored_item.get('category', '')} "
#                                f"service_type={full_doc.get('service_type', [])}",
#             'content_intent':  f"doc_intent={scored_item.get('content_intent', 'empty')} "
#                                f"mode={query_mode}",
#             'pool_type_note':  f"data_type={scored_item.get('data_type', '')} "
#                                f"allowed_for_{query_mode}="
#                                f"{pool_m == 1.0}",
#             'authority_note':  'no rating/review data' if scored_item.get('auth_score', 0) == 0
#                                else f"auth_score_n={scored_item.get('auth_score', 0):.3f}",
#         },
#     }


# def _debug_error_response(endpoint: str, query: str, err: Exception) -> dict:
#     """Standard error envelope for all debug endpoints."""
#     return {
#         'endpoint':  endpoint,
#         'query':     query,
#         'status':    'error',
#         'error':     str(err),
#         'traceback': traceback.format_exc(),
#     }


# # ============================================================
# # DEBUG ENDPOINT 1 — /debug/keyword/
# # ============================================================

# @require_GET
# async def debug_keyword(request):
#     """
#     Tests the keyword path only.

#     URL:
#         /debug/keyword/?query=restaurants+in+atlanta
#         /debug/keyword/?query=best+soul+food+houston
#     """
#     if not DEBUG_BRIDGE_AVAILABLE:
#         return JsonResponse(
#             {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
#             status=500
#         )

#     query = request.GET.get('query', '').strip()
#     if not query:
#         return JsonResponse({'error': 'query parameter required'}, status=400)

#     t0     = time.time()
#     report = {
#         'endpoint': 'debug_keyword',
#         'query':    query,
#         'status':   'ok',
#         'timings':  {},
#         'steps':    {},
#     }

#     try:
#         # ── Step 1: Word Discovery ────────────────────────────────────────
#         t1        = time.time()
#         discovery = await _run_word_discovery(query)
#         report['timings']['word_discovery_ms'] = round((time.time() - t1) * 1000, 2)

#         report['steps']['word_discovery'] = {
#             'corrected_query': discovery.get('corrected_query', query),
#             'search_terms':    discovery.get('search_terms', []),
#             'cities':          [c['name'] for c in discovery.get('cities', [])],
#             'states':          [s['name'] for s in discovery.get('states', [])],
#             'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
#             'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
#             'primary_intent':  discovery.get('primary_intent', ''),
#             'corrections':     discovery.get('corrections', []),
#             'stats':           discovery.get('stats', {}),
#         }

#         # ── Step 2: Intent Detection ──────────────────────────────────────
#         signals = {}
#         if INTENT_AVAILABLE and detect_intent:
#             t2        = time.time()
#             discovery = await asyncio.to_thread(detect_intent, discovery)
#             signals   = discovery.get('signals', {})
#             report['timings']['intent_ms'] = round((time.time() - t2) * 1000, 2)

#         signals = _patch_signals_domain(signals)

#         report['steps']['intent'] = {
#             'query_mode':             signals.get('query_mode', 'unknown'),
#             'primary_domain':         signals.get('primary_domain', ''),
#             'is_local_search':        signals.get('is_local_search', False),
#             'has_food_word':          signals.get('has_food_word', False),
#             'has_service_word':       signals.get('has_service_word', False),
#             'has_beauty_word':        signals.get('has_beauty_word', False),
#             'has_black_owned':        signals.get('has_black_owned', False),
#             'has_superlative':        signals.get('has_superlative', False),
#             'has_unknown_terms':      signals.get('has_unknown_terms', False),
#             'wants_single_result':    signals.get('wants_single_result', False),
#             'wants_multiple_results': signals.get('wants_multiple_results', False),
#             'local_search_strength':  signals.get('local_search_strength', 'none'),
#             'domain_inferred':        True,
#         }

#         # ── Step 3: Profile ───────────────────────────────────────────────
#         profile = _read_v3_profile(discovery, signals=signals)

#         report['steps']['profile'] = {
#             'search_terms':         profile.get('search_terms', []),
#             'field_boosts':         profile.get('field_boosts', {}),
#             'preferred_data_types': profile.get('preferred_data_types', []),
#             'has_location':         profile.get('has_location', False),
#         }

#         # ── Step 4: Typesense params ──────────────────────────────────────
#         ts_params = build_typesense_params(profile, signals=signals)

#         report['steps']['typesense_params'] = {
#             'q':         ts_params.get('q', ''),
#             'query_by':  ts_params.get('query_by', ''),
#             'filter_by': ts_params.get('filter_by', ''),
#             'sort_by':   ts_params.get('sort_by', ''),
#             'num_typos': ts_params.get('num_typos', 0),
#         }

#         # ── Step 5: Fetch candidates ──────────────────────────────────────
#         t3         = time.time()
#         candidates = await fetch_candidates_with_metadata(query, profile, signals=signals)
#         report['timings']['fetch_candidates_ms'] = round((time.time() - t3) * 1000, 2)

#         report['steps']['candidates'] = {
#             'total_returned': len(candidates),
#         }

#         # ── Step 6: Resolve blend + score ────────────────────────────────
#         t4        = time.time()
#         blend     = _resolve_blend(signals.get('query_mode', 'explore'), signals, candidates)
#         pool_size = len(candidates)

#         for idx, item in enumerate(candidates):
#             _score_document(
#                 idx         = idx,
#                 item        = item,
#                 profile     = profile,
#                 signals     = signals,
#                 blend       = blend,
#                 pool_size   = pool_size,
#                 vector_data = {},
#             )

#         candidates.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(candidates):
#             item['rank'] = i

#         report['timings']['scoring_ms'] = round((time.time() - t4) * 1000, 2)

#         report['steps']['blend_used'] = {
#             'text_match': round(blend['text_match'], 3),
#             'semantic':   round(blend['semantic'], 3),
#             'authority':  round(blend['authority'], 3),
#             'note':       'authority=0 means no service_rating/review_count in data',
#         }

#         # ── Step 7: Fetch full documents for ALL results ──────────────────
#         t5       = time.time()
#         all_ids  = [item.get('id') for item in candidates if item.get('id')]
#         full_docs_list = await fetch_full_documents(all_ids, query)
#         full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
#         report['timings']['fetch_full_docs_ms'] = round((time.time() - t5) * 1000, 2)

#         # ── Step 8: Facets ────────────────────────────────────────────────
#         counts = count_all(candidates)
#         report['steps']['facets']            = counts['facets']
#         report['steps']['total_image_count'] = counts['total_image_count']

#         # ── Full ranked results ───────────────────────────────────────────
#         report['ranked_results'] = [
#             _build_full_result(
#                 scored_item = item,
#                 full_doc    = full_docs_map.get(item.get('id'), {}),
#                 signals     = signals,
#                 source_pool = 'document',
#             )
#             for item in candidates
#         ]

#         report['steps']['total_results'] = len(report['ranked_results'])

#     except Exception as e:
#         report = _debug_error_response('debug_keyword', query, e)

#     report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
#     return JsonResponse(report, json_dumps_params={'indent': 2})


# # ============================================================
# # DEBUG ENDPOINT 2 — /debug/semantic/
# # ============================================================

# @require_GET
# async def debug_semantic(request):
#     """
#     Tests the full semantic path.

#     URL:
#         /debug/semantic/?query=restaurants+in+atlanta
#         /debug/semantic/?query=what+is+soul+food
#     """
#     if not DEBUG_BRIDGE_AVAILABLE:
#         return JsonResponse(
#             {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
#             status=500
#         )

#     query = request.GET.get('query', '').strip()
#     if not query:
#         return JsonResponse({'error': 'query parameter required'}, status=400)

#     t0     = time.time()
#     report = {
#         'endpoint': 'debug_semantic',
#         'query':    query,
#         'status':   'ok',
#         'timings':  {},
#         'steps':    {},
#     }

#     try:
#         # ── Step 1: Word Discovery + embedding in parallel ────────────────
#         t1                         = time.time()
#         discovery, query_embedding = await run_parallel_prep(query)
#         report['timings']['parallel_prep_ms'] = round((time.time() - t1) * 1000, 2)

#         report['steps']['word_discovery'] = {
#             'corrected_query': discovery.get('corrected_query', query),
#             'search_terms':    discovery.get('search_terms', []),
#             'cities':          [c['name'] for c in discovery.get('cities', [])],
#             'states':          [s['name'] for s in discovery.get('states', [])],
#             'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
#             'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
#             'primary_intent':  discovery.get('primary_intent', ''),
#             'stats':           discovery.get('stats', {}),
#         }

#         report['steps']['embedding'] = {
#             'generated':  query_embedding is not None,
#             'dimensions': len(query_embedding) if query_embedding else 0,
#         }

#         # ── Step 2: Intent Detection ──────────────────────────────────────
#         signals = {}
#         if INTENT_AVAILABLE and detect_intent:
#             t2        = time.time()
#             discovery = await asyncio.to_thread(detect_intent, discovery)
#             signals   = discovery.get('signals', {})
#             report['timings']['intent_ms'] = round((time.time() - t2) * 1000, 2)

#         signals    = _patch_signals_domain(signals)
#         query_mode = signals.get('query_mode', 'explore')

#         report['steps']['intent'] = {
#             'query_mode':             query_mode,
#             'primary_domain':         signals.get('primary_domain', ''),
#             'is_local_search':        signals.get('is_local_search', False),
#             'has_food_word':          signals.get('has_food_word', False),
#             'has_service_word':       signals.get('has_service_word', False),
#             'has_black_owned':        signals.get('has_black_owned', False),
#             'has_superlative':        signals.get('has_superlative', False),
#             'has_unknown_terms':      signals.get('has_unknown_terms', False),
#             'wants_single_result':    signals.get('wants_single_result', False),
#             'local_search_strength':  signals.get('local_search_strength', 'none'),
#         }

#         # ── Step 3: Profile ───────────────────────────────────────────────
#         profile = _read_v3_profile(discovery, signals=signals)

#         # ── Step 4: Stage 1A + 1B in parallel ────────────────────────────
#         t3 = time.time()

#         doc_uuids, q_uuids = await asyncio.gather(
#             fetch_candidate_uuids(query, profile, signals, 100),
#             fetch_candidate_uuids_from_questions(
#                 profile, query_embedding, signals, 50, discovery
#             ),
#         )

#         report['timings']['stage1_ms'] = round((time.time() - t3) * 1000, 2)

#         doc_set = set(doc_uuids)
#         q_set   = set(q_uuids)
#         overlap = doc_set & q_set

#         report['steps']['stage1'] = {
#             'document_collection': {'uuid_count': len(doc_uuids)},
#             'questions_collection': {
#                 'uuid_count': len(q_uuids),
#                 'note': 'document_uuids linked from matching questions',
#             },
#             'overlap': {
#                 'count': len(overlap),
#                 'note':  'Found by both paths — highest confidence',
#             },
#         }

#         # ── Step 5: Merge ─────────────────────────────────────────────────
#         merged = []
#         seen   = set()
#         for uid in doc_uuids:
#             if uid in overlap and uid not in seen:
#                 merged.append(uid); seen.add(uid)
#         for uid in doc_uuids:
#             if uid not in seen:
#                 merged.append(uid); seen.add(uid)
#         for uid in q_uuids:
#             if uid not in seen:
#                 merged.append(uid); seen.add(uid)

#         # ── Step 6: Stage 2 — vector rerank ──────────────────────────────
#         t4       = time.time()
#         reranked = await semantic_rerank_candidates(merged, query_embedding, max_to_rerank=500)
#         report['timings']['stage2_rerank_ms'] = round((time.time() - t4) * 1000, 2)

#         vector_data = {
#             item['id']: {
#                 'vector_distance': item.get('vector_distance', 1.0),
#                 'semantic_rank':   item.get('semantic_rank', 999999),
#             }
#             for item in reranked
#         }

#         report['steps']['stage2_rerank'] = {
#             'reranked_count': len(reranked),
#             'top_10_by_vector': [
#                 {
#                     'document_uuid':   r['id'],
#                     'vector_distance': round(r['vector_distance'], 4),
#                     'passes_gate':     r['vector_distance'] < SEMANTIC_DISTANCE_GATE,
#                 }
#                 for r in reranked[:10]
#             ],
#         }

#         # ── Step 7: Stage 3 — distance prune ─────────────────────────────
#         DISTANCE_THRESHOLDS = {
#             'answer':  0.60, 'explore': 0.70, 'compare': 0.65,
#             'browse':  0.85, 'local':   0.85, 'shop':    0.80,
#         }
#         threshold      = DISTANCE_THRESHOLDS.get(query_mode, 0.75)
#         before_prune   = len(merged)
#         survivor_uuids = [
#             u for u in merged
#             if vector_data.get(u, {}).get('vector_distance', 1.0) <= threshold
#         ]

#         report['steps']['stage3_prune'] = {
#             'threshold': threshold,
#             'before':    before_prune,
#             'after':     len(survivor_uuids),
#             'removed':   before_prune - len(survivor_uuids),
#         }

#         # ── Step 8: Stage 4 — metadata fetch ─────────────────────────────
#         t5          = time.time()
#         all_results = await fetch_candidate_metadata(survivor_uuids)
#         report['timings']['stage4_metadata_ms'] = round((time.time() - t5) * 1000, 2)

#         # ── Step 9: Resolve blend + score ────────────────────────────────
#         blend     = _resolve_blend(query_mode, signals, all_results)
#         pool_size = len(all_results)

#         report['steps']['blend_resolved'] = {
#             'text_match': round(blend['text_match'], 3),
#             'semantic':   round(blend['semantic'], 3),
#             'authority':  round(blend['authority'], 3),
#         }

#         t6 = time.time()
#         for idx, item in enumerate(all_results):
#             _score_document(
#                 idx         = idx,
#                 item        = item,
#                 profile     = profile,
#                 signals     = signals,
#                 blend       = blend,
#                 pool_size   = pool_size,
#                 vector_data = vector_data,
#             )

#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#         report['timings']['scoring_ms'] = round((time.time() - t6) * 1000, 2)

#         # ── Step 10: Fetch full documents for ALL results ─────────────────
#         t7       = time.time()
#         all_ids  = [item.get('id') for item in all_results if item.get('id')]
#         full_docs_list = await fetch_full_documents(all_ids, query)
#         full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
#         report['timings']['fetch_full_docs_ms'] = round((time.time() - t7) * 1000, 2)

#         # ── Facets ────────────────────────────────────────────────────────
#         counts = count_all(all_results)
#         report['steps']['facets']            = counts['facets']
#         report['steps']['total_image_count'] = counts['total_image_count']

#         # ── Full ranked results ───────────────────────────────────────────
#         report['ranked_results'] = [
#             _build_full_result(
#                 scored_item = item,
#                 full_doc    = full_docs_map.get(item.get('id'), {}),
#                 signals     = signals,
#                 source_pool = (
#                     'overlap'   if item.get('id') in overlap else
#                     'questions' if item.get('id') in q_set   else
#                     'document'
#                 ),
#             )
#             for item in all_results
#         ]

#         report['steps']['total_results'] = len(report['ranked_results'])

#     except Exception as e:
#         report = _debug_error_response('debug_semantic', query, e)

#     report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
#     return JsonResponse(report, json_dumps_params={'indent': 2})


# # ============================================================
# # DEBUG ENDPOINT 3 — /debug/question/
# # ============================================================

# @require_GET
# async def debug_question(request):
#     """
#     Tests the questions collection vector search with FULL validation.

#     Each result shows the raw vector hit PLUS whether it would pass
#     the bridge's distance gate and token validation.

#     URL:
#         /debug/question/?query=who+was+the+first+mayor+of+savannah
#         /debug/question/?query=what+is+soul+food
#     """
#     if not DEBUG_BRIDGE_AVAILABLE:
#         return JsonResponse(
#             {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
#             status=500
#         )

#     query = request.GET.get('query', '').strip()
#     if not query:
#         return JsonResponse({'error': 'query parameter required'}, status=400)

#     t0     = time.time()
#     report = {
#         'endpoint': 'debug_question',
#         'query':    query,
#         'status':   'ok',
#         'timings':  {},
#         'steps':    {},
#     }

#     try:
#         # ── Step 1: Word Discovery (to get profile for validation) ────────
#         t1        = time.time()
#         discovery = await _run_word_discovery(query)
#         report['timings']['word_discovery_ms'] = round((time.time() - t1) * 1000, 2)

#         profile = _read_v3_profile(discovery)

#         report['steps']['word_discovery'] = {
#             'corrected_query': discovery.get('corrected_query', query),
#             'search_terms':    discovery.get('search_terms', []),
#             'cities':          [c['name'] for c in discovery.get('cities', [])],
#             'states':          [s['name'] for s in discovery.get('states', [])],
#             'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
#             'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
#         }

#         # ── Step 2: Extract validation signals from profile ───────────────
#         query_tokens, query_phrases, primary_subject = _extract_query_signals(
#             profile, discovery=discovery
#         )

#         location_tokens = set()
#         for c in discovery.get('cities', []):
#             location_tokens.update(_normalize_signal(c.get('name', '')))
#         for s in discovery.get('states', []):
#             location_tokens.update(_normalize_signal(s.get('name', '')))

#         report['steps']['validation_signals'] = {
#             'query_tokens':    sorted(query_tokens),
#             'query_phrases':   query_phrases,
#             'primary_subject': sorted(primary_subject) if primary_subject else None,
#             'location_tokens': sorted(location_tokens),
#             'distance_gate':   QUESTION_SEMANTIC_DISTANCE_GATE,
#         }

#         # ── Step 3: Embed the query ───────────────────────────────────────
#         t2              = time.time()
#         query_embedding = await _run_embedding(query)
#         report['timings']['embedding_ms'] = round((time.time() - t2) * 1000, 2)

#         if not query_embedding:
#             return JsonResponse(
#                 {'error': 'Embedding failed — check embedding_client'},
#                 status=500
#             )

#         report['steps']['embedding'] = {
#             'generated':  True,
#             'dimensions': len(query_embedding),
#         }

#         # ── Step 4: Vector search against questions collection ────────────
#         t3            = time.time()
#         embedding_str = ','.join(str(x) for x in query_embedding)
#         TOP_K         = 30

#         search_params = {
#             'q':              '*',
#             'vector_query':   f'embedding:([{embedding_str}], k:{TOP_K})',
#             'per_page':       TOP_K,
#             'include_fields': 'document_uuid,question,answer_type,question_type,'
#                               'primary_keywords,entities,semantic_keywords',
#         }

#         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
#         response        = await asyncio.to_thread(
#             client.multi_search.perform,
#             search_requests, {}
#         )
#         hits            = response['results'][0].get('hits', [])
#         report['timings']['vector_search_ms'] = round((time.time() - t3) * 1000, 2)

#         # ── Step 5: Apply distance gate + token validation ────────────────
#         THRESHOLDS = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65]

#         threshold_counts = {str(t): 0 for t in THRESHOLDS}
#         results          = []
#         accepted_count   = 0
#         rejected_count   = 0

#         for hit in hits:
#             doc      = hit.get('document', {})
#             distance = hit.get('vector_distance', 1.0)

#             for t in THRESHOLDS:
#                 if distance < t:
#                     threshold_counts[str(t)] += 1

#             passes_gate = distance < QUESTION_SEMANTIC_DISTANCE_GATE

#             passes_validation = False
#             validation_reason = ''

#             if not passes_gate:
#                 validation_reason = (
#                     f'REJECTED: distance {distance:.4f} >= '
#                     f'gate {QUESTION_SEMANTIC_DISTANCE_GATE}'
#                 )
#             else:
#                 is_valid = _validate_question_hit(
#                     hit_doc         = doc,
#                     query_tokens    = query_tokens,
#                     query_phrases   = query_phrases,
#                     primary_subject = primary_subject,
#                     min_matches     = 1,
#                 )

#                 if not is_valid:
#                     validation_reason = 'REJECTED: token validation failed (no matching tokens in primary_keywords/entities/semantic_keywords)'
#                 elif location_tokens:
#                     candidate_raw = (
#                         doc.get('primary_keywords', []) +
#                         doc.get('entities', []) +
#                         doc.get('semantic_keywords', [])
#                     )
#                     candidate_tokens = set()
#                     for val in candidate_raw:
#                         if val:
#                             candidate_tokens.update(_normalize_signal(val))

#                     location_found = bool(location_tokens & candidate_tokens)

#                     if not location_found:
#                         validation_reason = (
#                             f'REJECTED: location tokens {sorted(location_tokens)} '
#                             f'not found in candidate'
#                         )
#                     else:
#                         passes_validation = True
#                         validation_reason = 'ACCEPTED: distance + tokens + location all pass'
#                 else:
#                     passes_validation = True
#                     validation_reason = 'ACCEPTED: distance + tokens pass (no location required)'

#             if passes_validation:
#                 accepted_count += 1
#             else:
#                 rejected_count += 1

#             results.append({
#                 'document_uuid':       doc.get('document_uuid', ''),
#                 'question':            doc.get('question', ''),
#                 'question_type':       doc.get('question_type', ''),
#                 'answer_type':         doc.get('answer_type', ''),
#                 'primary_keywords':    doc.get('primary_keywords', []),
#                 'entities':            doc.get('entities', []),
#                 'vector_distance':     round(distance, 4),
#                 'passes_distance_gate': passes_gate,
#                 'passes_validation':   passes_validation,
#                 'validation_reason':   validation_reason,
#                 'passes_0_30':         distance < 0.30,
#                 'passes_0_35':         distance < 0.35,
#                 'passes_0_40':         distance < 0.40,
#                 'passes_0_45':         distance < 0.45,
#                 'passes_0_50':         distance < 0.50,
#                 'passes_0_55':         distance < 0.55,
#                 'passes_0_60':         distance < 0.60,
#                 'passes_0_65':         distance < 0.65,
#                 'document':            {},
#             })

#         results.sort(key=lambda x: x['vector_distance'])

#         # ── Step 6: Fetch full documents for all question hits ────────────
#         t4             = time.time()
#         all_uuids      = [r['document_uuid'] for r in results if r.get('document_uuid')]
#         full_docs_list = await fetch_full_documents(all_uuids, query)
#         full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
#         report['timings']['fetch_full_docs_ms'] = round((time.time() - t4) * 1000, 2)

#         # ── Step 7: Merge full document into each result ──────────────────
#         for r in results:
#             full_doc      = full_docs_map.get(r['document_uuid'], {})
#             r['document'] = {
#                 'title':        full_doc.get('title', ''),
#                 'url':          full_doc.get('url', ''),
#                 'data_type':    full_doc.get('data_type', ''),
#                 'category':     full_doc.get('category', ''),
#                 'summary':      full_doc.get('summary', ''),
#                 'source':       full_doc.get('source', ''),
#                 'black_owned':  full_doc.get('black_owned', False),
#                 'service_type': full_doc.get('service_type', []),
#                 'location': {
#                     'city':    full_doc.get('location', {}).get('city'),
#                     'state':   full_doc.get('location', {}).get('state'),
#                 },
#                 'key_facts':    full_doc.get('key_facts', []),
#                 'image_url':    full_doc.get('image_url', []),
#             }

#         # ── Step 8: Distance distribution + validation summary ────────────
#         distances = [r['vector_distance'] for r in results]

#         report['steps']['validation_summary'] = {
#             'total_raw_hits': len(hits),
#             'accepted':       accepted_count,
#             'rejected':       rejected_count,
#             'distance_gate':  QUESTION_SEMANTIC_DISTANCE_GATE,
#             'note':           'Only accepted results would be returned by the bridge',
#         }

#         report['steps']['threshold_analysis'] = {
#             'total_hits': len(hits),
#             'counts_below_threshold': {
#                 f'below_{str(t).replace(".", "_")}': threshold_counts[str(t)]
#                 for t in THRESHOLDS
#             },
#         }

#         report['steps']['distance_distribution'] = {
#             'min':        round(min(distances, default=1.0), 4),
#             'max':        round(max(distances, default=1.0), 4),
#             'mean':       round(sum(distances) / max(len(distances), 1), 4),
#             'below_0_30': sum(1 for d in distances if d < 0.30),
#             'below_0_35': sum(1 for d in distances if d < 0.35),
#             'below_0_40': sum(1 for d in distances if d < 0.40),
#             'below_0_45': sum(1 for d in distances if d < 0.45),
#             'below_0_50': sum(1 for d in distances if d < 0.50),
#             'below_0_55': sum(1 for d in distances if d < 0.55),
#             'below_0_60': sum(1 for d in distances if d < 0.60),
#             'below_0_65': sum(1 for d in distances if d < 0.65),
#         }

#         report['results']                = results
#         report['steps']['total_results'] = len(results)

#     except Exception as e:
#         report = _debug_error_response('debug_question', query, e)

#     report['timings'] = report.get('timings', {})
#     report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
#     return JsonResponse(report, json_dumps_params={'indent': 2})


# # ============================================================
# # DEBUG ENDPOINT 4 — /api/debug/word-discovery/
# #
# # REWRITTEN FOR v4 PIPELINE:
# #   Step 1: Tokenize + Known Sets + RAM Hash Lookup
# #   Step 2: Clean + Resolve
# #     2a: Fuzzy correction (Tier 1 known sets → Tier 2 RAM → Tier 3 Redis)
# #     2b: POS Prediction + Best Match Selection
# #     2c: "in" Disambiguation
# #   Step 3: N-gram Resolution (single pass on corrected words)
# #   Step 4: Build Profile + Intent
# #
# # Key difference from v3 debug: corrections run BEFORE n-grams
# # and disambiguation, so the trace shows the corrected pipeline
# # order.  Step 5 (re-check n-grams) is gone because n-grams now
# # run on already-corrected words.
# # ============================================================

# async def debug_word_discovery(request):
#     """
#     GET /api/debug/word-discovery/?q=restuarants+in+atl

#     Traces every word through every stage of Word Discovery v4.
#     Shows the full journey of each word from raw input to final
#     profile, with all intermediate states visible.
#     """
#     query = request.GET.get('q', '')
#     if not query:
#         return JsonResponse({'error': 'Missing ?q= parameter'}, status=400)

#     try:
#         from .word_discovery_fulltest import (
#             WordDiscovery, vocab_cache_wrapper, STOPWORDS,
#             LOCATION_SIGNAL_WORDS, LOCATION_CATEGORIES,
#             CITY_CATEGORIES, STATE_CATEGORIES, PERSON_CATEGORIES,
#             ORGANIZATION_CATEGORIES, KEYWORD_CATEGORIES, MEDIA_CATEGORIES,
#             LOCATION_INTENT_FLAGS, KEYWORD_INTENT_FLAGS,
#             normalize_pos_string, is_pos_compatible,
#             RAM_CACHE_AVAILABLE, vocab_cache,
#         )
#     except ImportError:
#         from word_discovery_fulltest import (
#             WordDiscovery, vocab_cache_wrapper, STOPWORDS,
#             LOCATION_SIGNAL_WORDS, LOCATION_CATEGORIES,
#             CITY_CATEGORIES, STATE_CATEGORIES, PERSON_CATEGORIES,
#             ORGANIZATION_CATEGORIES, KEYWORD_CATEGORIES, MEDIA_CATEGORIES,
#             LOCATION_INTENT_FLAGS, KEYWORD_INTENT_FLAGS,
#             normalize_pos_string, is_pos_compatible,
#             RAM_CACHE_AVAILABLE, vocab_cache,
#         )

#     try:
#         from .intent_detect import detect_intent
#         _INTENT_AVAILABLE = True
#     except ImportError:
#         try:
#             from intent_detect import detect_intent
#             _INTENT_AVAILABLE = True
#         except ImportError:
#             _INTENT_AVAILABLE = False

#     def run_trace(query):
#         start = time.perf_counter()
#         trace = {}

#         # ── META ─────────────────────────────────────────────────────
#         trace['meta'] = {
#             'query': query,
#             'pipeline_version': 'v4',
#             'pipeline_order': [
#                 'Step 1: Tokenize + Known Sets + RAM Hash Lookup',
#                 'Step 2: Clean + Resolve (corrections → POS → "in" disambig)',
#                 'Step 3: N-gram Resolution (single pass on corrected words)',
#                 'Step 4: Build Profile + Intent',
#             ],
#             'ram_cache_available': RAM_CACHE_AVAILABLE,
#             'ram_cache_loaded': bool(
#                 RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded
#             ),
#         }

#         words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
#         words = [w for w in words if w]
#         trace['meta']['tokenized_words'] = words

#         wd = WordDiscovery(verbose=False)
#         cache = vocab_cache_wrapper

#         # ═════════════════════════════════════════════════════════════
#         # STEP 1: Tokenize + Known Sets + RAM Hash Lookup
#         # ═════════════════════════════════════════════════════════════
#         word_data = wd._step1_tokenize_and_lookup(words)

#         step1 = []
#         for wd_item in word_data:
#             w = wd_item['word'].lower()
#             entry = {
#                 'position': wd_item['position'],
#                 'word': w,
#                 'status': wd_item['status'],
#                 'is_stopword': wd_item['is_stopword'],
#                 'context_flags': list(wd_item.get('context_flags', [])),
#                 'location_context': wd_item.get('location_context', False),
#             }

#             if wd_item['is_stopword']:
#                 entry['path'] = 'STOPWORD'
#                 entry['pos'] = STOPWORDS.get(w, '')
#             elif wd_item['status'] == 'known':
#                 entry['path'] = 'KNOWN_SET'
#                 entry['category'] = wd_item.get('selected_match', {}).get('category', '')
#                 entry['pos'] = wd_item.get('pos', '')
#                 # Identify which set matched
#                 for set_name in [
#                     'COLORS', 'SUPERLATIVES', 'SIZES', 'COMMON_ADJECTIVES',
#                     'FOOD_DINING', 'SERVICES', 'APPAREL_PRODUCTS', 'BEAUTY',
#                     'CULTURE_COMMUNITY', 'MUSIC_ENTERTAINMENT', 'KNOWN_ACRONYMS',
#                 ]:
#                     if w in getattr(wd, set_name, set()):
#                         entry['known_set'] = set_name
#                         break
#             elif wd_item.get('abbreviation'):
#                 entry['path'] = 'ABBREVIATION'
#                 entry['expanded_to'] = wd_item['abbreviation']['expanded_to']
#                 entry['match_count'] = len(wd_item.get('all_matches', []))
#                 entry['all_matches'] = [
#                     {
#                         'term': m['term'], 'category': m['category'],
#                         'pos': m['pos'], 'rank': m['rank'],
#                     }
#                     for m in wd_item.get('all_matches', [])
#                 ]
#             elif wd_item['status'] == 'resolved':
#                 entry['path'] = 'RAM_HASH'
#                 entry['match_count'] = len(wd_item.get('all_matches', []))
#                 entry['all_matches'] = [
#                     {
#                         'term': m['term'], 'category': m['category'],
#                         'pos': m['pos'], 'rank': m['rank'],
#                     }
#                     for m in wd_item.get('all_matches', [])
#                 ]
#             elif wd_item['status'] == 'unknown':
#                 entry['path'] = 'NOT_FOUND'
#                 direct = cache.get_term_matches(w)
#                 entry['direct_ram_check'] = [
#                     {
#                         'term': m['term'], 'category': m['category'],
#                         'pos': m['pos'], 'rank': m['rank'],
#                     }
#                     for m in direct
#                 ]
#                 entry['ram_empty'] = len(direct) == 0
#             else:
#                 entry['path'] = wd_item['status']

#             step1.append(entry)

#         trace['step1_tokenize'] = {
#             'description': 'Tokenize + Known Sets + RAM Hash Lookup',
#             'words': step1,
#             'summary': {
#                 'stopwords': sum(1 for s in step1 if s.get('is_stopword')),
#                 'known_set': sum(1 for s in step1 if s.get('path') == 'KNOWN_SET'),
#                 'ram_hash': sum(1 for s in step1 if s.get('path') == 'RAM_HASH'),
#                 'abbreviation': sum(1 for s in step1 if s.get('path') == 'ABBREVIATION'),
#                 'not_found': sum(1 for s in step1 if s.get('path') == 'NOT_FOUND'),
#             },
#         }

#         # ═════════════════════════════════════════════════════════════
#         # STEP 2a: Clean + Resolve (fuzzy corrections)
#         #
#         # v4 KEY CHANGE: corrections happen BEFORE n-grams and POS.
#         # This means "restuarant" gets corrected to "restaurant" and
#         # tagged with known_food BEFORE "in" disambiguation runs.
#         # ═════════════════════════════════════════════════════════════
#         has_unknowns = any(wd_item['status'] == 'unknown' for wd_item in word_data)

#         if has_unknowns:
#             corrections = wd._step2_clean_and_resolve(word_data)
#         else:
#             corrections = []

#         step2a = []
#         for wd_item in word_data:
#             if wd_item['status'] not in (
#                 'corrected', 'pos_corrected', 'unknown', 'unknown_suggest',
#             ):
#                 continue

#             entry = {
#                 'position': wd_item['position'],
#                 'word': wd_item['word'],
#                 'status': wd_item['status'],
#                 'predicted_pos': wd_item.get('predicted_pos', ''),
#                 'context_flags_after_correction': list(
#                     wd_item.get('context_flags', [])
#                 ),
#             }

#             if wd_item['status'] == 'corrected':
#                 entry['corrected_to'] = wd_item.get('corrected', '')
#                 entry['distance'] = wd_item.get('distance', 0)
#                 entry['corrected_pos'] = wd_item.get('pos', '')
#                 entry['corrected_category'] = wd_item.get(
#                     'selected_match', {}
#                 ).get('category', '')
#                 entry['corrected_rank'] = wd_item.get(
#                     'selected_match', {}
#                 ).get('rank', 0)
#             elif wd_item['status'] == 'unknown_suggest':
#                 entry['suggestion'] = wd_item.get('suggestion', '')
#                 entry['suggestion_distance'] = wd_item.get(
#                     'suggestion_distance', 0
#                 )
#             elif wd_item['status'] == 'pos_corrected':
#                 entry['corrected_to'] = wd_item.get('corrected', '')
#                 entry['distance'] = wd_item.get('distance', 0)

#             # Show Redis suggestions if available
#             redis_sugg = wd_item.get('redis_suggestions', [])
#             if redis_sugg:
#                 entry['redis_suggestions'] = [
#                     {
#                         'term': s['term'], 'distance': s['distance'],
#                         'rank': s['rank'], 'pos': s['pos'],
#                         'category': s['category'],
#                         'compatible': s.get('compatible', False),
#                     }
#                     for s in redis_sugg[:5]
#                 ]

#             step2a.append(entry)

#         # Find correction sources from the corrections list
#         correction_sources = {}
#         for c in corrections:
#             correction_sources[c.get('position', -1)] = c.get('source', 'unknown')

#         for entry in step2a:
#             entry['correction_source'] = correction_sources.get(
#                 entry['position'], 'none'
#             )

#         trace['step2a_corrections'] = {
#             'description': (
#                 'Clean + Resolve — fuzzy corrections BEFORE n-grams/POS '
#                 '(v4 key change)'
#             ),
#             'skipped': not has_unknowns,
#             'correction_tiers': [
#                 'Tier 1: Fuzzy match against known sets (frozenset, ~0.001ms)',
#                 'Tier 2: Fuzzy match against RAM vocabulary cache (~1-10ms)',
#                 'Tier 3: Batch Redis fuzzy lookup (~30-50ms)',
#             ],
#             'words': step2a,
#             'corrections': corrections,
#             'summary': {
#                 'total_unknowns': sum(
#                     1 for s in step1 if s.get('path') == 'NOT_FOUND'
#                 ),
#                 'corrected': sum(
#                     1 for e in step2a if e['status'] == 'corrected'
#                 ),
#                 'suggested_only': sum(
#                     1 for e in step2a if e['status'] == 'unknown_suggest'
#                 ),
#                 'still_unknown': sum(
#                     1 for e in step2a if e['status'] == 'unknown'
#                 ),
#                 'from_tier1': sum(
#                     1 for e in step2a
#                     if e.get('correction_source') == 'tier1_known_sets'
#                 ),
#                 'from_tier2': sum(
#                     1 for e in step2a
#                     if e.get('correction_source') == 'tier2_ram_cache'
#                 ),
#                 'from_redis': sum(
#                     1 for e in step2a
#                     if e.get('correction_source') == 'redis'
#                 ),
#             },
#         }

#         # ═════════════════════════════════════════════════════════════
#         # STEP 2b: POS Prediction + Best Match Selection
#         #
#         # Now runs on CORRECTED words — POS predictions are reliable.
#         # ═════════════════════════════════════════════════════════════
#         wd._step2_pos_and_select(word_data)

#         step2b = []
#         for i, wd_item in enumerate(word_data):
#             entry = {
#                 'position': wd_item['position'],
#                 'word': wd_item['word'],
#                 'status': wd_item['status'],
#                 'is_stopword': wd_item['is_stopword'],
#             }
#             if not wd_item['is_stopword'] and wd_item['status'] != 'known':
#                 entry['predicted_pos'] = wd_item.get('predicted_pos', '')
#                 entry['predicted_pos_list'] = wd_item.get(
#                     'predicted_pos_list', []
#                 )
#                 sm = wd_item.get('selected_match')
#                 if sm:
#                     entry['selected_match'] = {
#                         'term': sm.get('term', ''),
#                         'category': sm.get('category', ''),
#                         'pos': sm.get('pos', ''),
#                         'rank': sm.get('rank', 0),
#                     }
#                     predicted = wd_item.get('predicted_pos', 'noun')
#                     entry['all_matches_with_compatibility'] = [
#                         {
#                             'term': m['term'],
#                             'category': m['category'],
#                             'pos': m['pos'],
#                             'rank': m['rank'],
#                             'pos_compatible': is_pos_compatible(
#                                 normalize_pos_string(m['pos']), predicted
#                             ),
#                         }
#                         for m in wd_item.get('all_matches', [])
#                     ]
#                 else:
#                     entry['selected_match'] = None

#             step2b.append(entry)

#         trace['step2b_pos_selection'] = {
#             'description': (
#                 'POS Prediction + Best Match Selection '
#                 '(runs on corrected words)'
#             ),
#             'words': step2b,
#         }

#         # ═════════════════════════════════════════════════════════════
#         # STEP 2c: "in" Disambiguation
#         #
#         # v4 KEY CHANGE: runs AFTER corrections, so left-side context
#         # flags are correct.  "restuarant in atl" → "restaurant" now
#         # has known_food flag → Atlanta stays as city.
#         # ═════════════════════════════════════════════════════════════
#         wd._step2_resolve_in_ambiguity(word_data)

#         step2c = []
#         for i, wd_item in enumerate(word_data):
#             if not (wd_item['is_stopword'] and wd_item['word'] == 'in'):
#                 continue
#             if i + 1 >= len(word_data):
#                 continue

#             target = word_data[i + 1]
#             all_m = target.get('all_matches', [])
#             city_m = [
#                 m for m in all_m
#                 if m.get('category', '').lower() in CITY_CATEGORIES
#             ]
#             non_city_m = [
#                 m for m in all_m
#                 if m.get('category', '').lower() not in LOCATION_CATEGORIES
#             ]

#             # Left context
#             left_flags = set()
#             left_words = []
#             for j in range(i - 1, -1, -1):
#                 lw = word_data[j]
#                 if lw['is_stopword'] and lw['word'] not in (
#                     'the', 'a', 'an', 'for', 'and',
#                 ):
#                     break
#                 if lw['is_stopword']:
#                     continue
#                 flags = set(lw.get('context_flags', []))
#                 left_flags.update(flags)
#                 left_words.append({
#                     'word': lw['word'],
#                     'corrected': lw.get('corrected', None),
#                     'flags': list(flags),
#                     'status': lw['status'],
#                 })
#                 if len(left_words) >= 3:
#                     break

#             step2c.append({
#                 'target_word': target['word'],
#                 'target_position': target['position'],
#                 'city_matches': [
#                     {
#                         'term': m['term'],
#                         'category': m['category'],
#                         'rank': m['rank'],
#                     }
#                     for m in city_m
#                 ],
#                 'non_city_matches': [
#                     {
#                         'term': m['term'],
#                         'category': m['category'],
#                         'rank': m['rank'],
#                     }
#                     for m in non_city_m
#                 ],
#                 'left_context': left_words,
#                 'left_flags': sorted(left_flags),
#                 'has_location_intent': bool(left_flags & LOCATION_INTENT_FLAGS),
#                 'has_keyword_intent': bool(left_flags & KEYWORD_INTENT_FLAGS),
#                 'final_selected_category': target.get(
#                     'selected_match', {}
#                 ).get('category', ''),
#                 'final_context_flags': list(
#                     target.get('context_flags', [])
#                 ),
#                 'v4_note': (
#                     'Left context flags are from CORRECTED words — '
#                     'this is the v4 fix for the misspelling bug'
#                 ),
#             })

#         trace['step2c_in_disambiguation'] = {
#             'description': (
#                 '"in" disambiguation — city vs keyword '
#                 '(runs on corrected words with correct flags)'
#             ),
#             'results': step2c,
#         }

#         # ═════════════════════════════════════════════════════════════
#         # STEP 3: N-gram Resolution (single pass on corrected words)
#         #
#         # v4 KEY CHANGE: no Step 5 re-check needed.  Words are already
#         # correct, so n-grams are found on the first and only pass.
#         # ═════════════════════════════════════════════════════════════
#         corrected_words = wd._get_working_words(word_data)
#         ngrams, consumed = wd._step3_resolve_ngrams(corrected_words, word_data)

#         trace['step3_ngrams'] = {
#             'description': (
#                 'N-gram Resolution — single pass on corrected words '
#                 '(no re-check needed)'
#             ),
#             'corrected_words_used': corrected_words,
#             'consumed_positions': sorted(consumed),
#             'ngrams': [
#                 {
#                     'type': ng['type'],
#                     'phrase': ng['phrase'],
#                     'category': ng['category'],
#                     'rank': ng['rank'],
#                     'positions': ng['positions'],
#                     'location_boosted': ng.get('location_boosted', False),
#                     'location_compound': ng.get('location_compound', False),
#                 }
#                 for ng in ngrams
#             ],
#             'v4_note': (
#                 'In v3 this ran BEFORE corrections on misspelled words, '
#                 'then had to re-check in Step 5.  v4 runs once on '
#                 'correct words.'
#             ),
#         }

#         # ═════════════════════════════════════════════════════════════
#         # STEP 4: Build Profile + Intent
#         # ═════════════════════════════════════════════════════════════
#         profile = wd._step4_build_profile(
#             query, word_data, ngrams, consumed, corrections, start,
#         )

#         # Entity routing trace
#         entity_routing = []
#         ALWAYS_KEYWORD = (
#             wd.SUPERLATIVES | wd.COLORS | wd.SIZES |
#             wd.COMMON_ADJECTIVES | wd.APPAREL_PRODUCTS |
#             wd.FOOD_DINING | wd.SERVICES
#         )
#         MODIFIER_POS = frozenset({
#             'adjective', 'adverb', 'verb', 'determiner',
#         })

#         for wd_item in word_data:
#             if wd_item['is_stopword'] or wd_item['position'] in consumed:
#                 continue
#             sm = wd_item.get('selected_match')
#             if not sm:
#                 continue
#             cat = sm.get('category', '').lower()
#             w = wd_item['word'].lower()
#             pred_pos = normalize_pos_string(wd_item.get('predicted_pos'))
#             match_pos = normalize_pos_string(sm.get('pos'))

#             # Where SHOULD it go based on category?
#             if cat in CITY_CATEGORIES:
#                 natural = 'cities'
#             elif cat in STATE_CATEGORIES:
#                 natural = 'states'
#             elif cat in LOCATION_CATEGORIES:
#                 natural = 'location_terms'
#             elif cat in PERSON_CATEGORIES:
#                 natural = 'persons'
#             elif cat in ORGANIZATION_CATEGORIES:
#                 natural = 'organizations'
#             elif cat in MEDIA_CATEGORIES:
#                 natural = 'media'
#             else:
#                 natural = 'keywords'

#             # Was it demoted?
#             demoted = False
#             reason = None
#             if w in ALWAYS_KEYWORD:
#                 demoted = True
#                 reason = f"'{w}' is in ALWAYS_KEYWORD set"
#             elif (
#                 pred_pos in MODIFIER_POS
#                 and match_pos in ('noun', 'proper_noun')
#                 and not is_pos_compatible(match_pos, pred_pos)
#             ):
#                 demoted = True
#                 reason = f"POS gate: predicted={pred_pos}, match={match_pos}"

#             entity_routing.append({
#                 'word': w,
#                 'match_category': sm.get('category', ''),
#                 'match_rank': sm.get('rank', 0),
#                 'natural_destination': natural,
#                 'demoted': demoted,
#                 'demotion_reason': reason,
#                 'final_destination': 'keywords' if demoted else natural,
#             })

#         trace['step4_profile'] = {
#             'description': 'Build Profile — entity routing + search terms',
#             'corrected_query': profile.get('corrected_query', ''),
#             'corrected_display_query': profile.get(
#                 'corrected_display_query', ''
#             ),
#             'search_terms': profile.get('search_terms', []),
#             'entity_routing': entity_routing,
#             'persons': profile.get('persons', []),
#             'organizations': profile.get('organizations', []),
#             'keywords': [
#                 {
#                     'display': k.get('display', ''),
#                     'category': k.get('category', ''),
#                     'rank': k.get('rank', 0),
#                 }
#                 for k in profile.get('keywords', [])
#             ],
#             'cities': profile.get('cities', []),
#             'states': profile.get('states', []),
#             'location_terms': profile.get('location_terms', []),
#             'primary_intent': profile.get('primary_intent', ''),
#             'intent_scores': profile.get('intent_scores', {}),
#             'field_boosts': profile.get('field_boosts', {}),
#             'stats': profile.get('stats', {}),
#         }

#         # ── INTENT DETECTION ─────────────────────────────────────────
#         if _INTENT_AVAILABLE:
#             try:
#                 enriched = detect_intent(profile)
#                 signals = enriched.get('signals', {})
#                 trace['intent_detection'] = {
#                     'available': True,
#                     'query_mode': signals.get('query_mode', ''),
#                     'primary_domain': signals.get('primary_domain'),
#                     'is_local_search': signals.get('is_local_search', False),
#                     'has_food_word': signals.get('has_food_word', False),
#                     'has_service_word': signals.get('has_service_word', False),
#                     'has_black_owned': signals.get('has_black_owned', False),
#                     'has_unknown_terms': signals.get('has_unknown_terms', False),
#                     'has_location_entity': signals.get(
#                         'has_location_entity', False
#                     ),
#                 }
#             except Exception as e:
#                 trace['intent_detection'] = {'error': str(e)}
#         else:
#             trace['intent_detection'] = {'available': False}

#         # ── PROBLEMS DETECTED ─────────────────────────────────────────
#         problems = []

#         for wd_item in word_data:
#             if wd_item['status'] in ('unknown', 'unknown_suggest'):
#                 problems.append({
#                     'type': 'UNCORRECTED_WORD',
#                     'word': wd_item['word'],
#                     'position': wd_item['position'],
#                     'suggestion': wd_item.get('suggestion', 'none'),
#                 })

#             if wd_item['is_stopword'] or wd_item['position'] in consumed:
#                 continue

#             sm = wd_item.get('selected_match')
#             if not sm:
#                 continue

#             all_m = wd_item.get('all_matches', [])
#             has_city = any(
#                 m.get('category', '').lower() in CITY_CATEGORIES
#                 for m in all_m
#             )
#             routed = sm.get('category', '').lower()
#             if has_city and routed not in LOCATION_CATEGORIES:
#                 problems.append({
#                     'type': 'CITY_ROUTED_TO_KEYWORD',
#                     'word': wd_item['word'],
#                     'position': wd_item['position'],
#                     'selected_category': sm.get('category', ''),
#                     'city_match_available': True,
#                 })

#         if not profile.get('cities') and not profile.get('states'):
#             has_loc_signal = any(
#                 wd_item.get('location_context', False)
#                 for wd_item in word_data
#             )
#             if has_loc_signal:
#                 problems.append({
#                     'type': 'LOCATION_SIGNAL_NO_CITY',
#                     'detail': (
#                         'Location signal detected but no cities/states '
#                         'in profile'
#                     ),
#                 })

#         trace['problems_detected'] = problems
#         trace['problem_count'] = len(problems)

#         # ── WORD JOURNEY SUMMARY ──────────────────────────────────────
#         journey = []
#         for wd_item in word_data:
#             j = {
#                 'position': wd_item['position'],
#                 'word': wd_item['word'],
#                 'final_status': wd_item['status'],
#                 'context_flags': list(wd_item.get('context_flags', [])),
#             }
#             if wd_item['status'] == 'corrected':
#                 j['corrected_to'] = wd_item.get('corrected', '')
#                 j['note'] = 'CORRECTED — used for search'
#             elif wd_item['status'] == 'unknown_suggest':
#                 j['suggestion'] = wd_item.get('suggestion', '')
#                 j['note'] = 'ORIGINAL KEPT — suggestion not applied'

#             sm = wd_item.get('selected_match')
#             if sm:
#                 j['final_category'] = sm.get('category', '')
#                 j['final_rank'] = sm.get('rank', 0)

#             journey.append(j)

#         trace['word_journey'] = journey

#         # ── v3 vs v4 COMPARISON NOTE ──────────────────────────────────
#         trace['v4_pipeline_notes'] = {
#             'key_changes': [
#                 'Corrections run BEFORE n-grams (was after in v3)',
#                 'Corrections run BEFORE POS prediction (was after in v3)',
#                 'Corrections run BEFORE "in" disambiguation (was after in v3)',
#                 '"in" disambiguation sees CORRECT context flags (was misspelled in v3)',
#                 'N-grams run once on corrected words (was twice in v3: before + after correction)',
#                 'Step 5 re-check eliminated (not needed when corrections come first)',
#                 'Pipeline reduced from 6+ steps to 4 steps',
#             ],
#             'bug_fixed': (
#                 'v3 bug: "restuarant in atl" → Step 3.5 saw misspelled '
#                 '"restuarant" with no known_food flag → demoted Atlanta '
#                 'to keyword. v4 fix: "restuarant" corrected to "restaurant" '
#                 'with known_food flag BEFORE "in" disambiguation → Atlanta '
#                 'stays as city.'
#             ),
#         }

#         elapsed = (time.perf_counter() - start) * 1000
#         trace['total_ms'] = round(elapsed, 2)

#         return trace

#     result = await asyncio.to_thread(run_trace, query)

#     return JsonResponse(result, safe=False)

# ============================================================
# views.py — IMPORTS SECTION
# Replace everything above your first view function with this
# ============================================================
 # ============================================================
# views.py — IMPORTS SECTION
# Replace everything above your first view function with this
# ============================================================
 
import hashlib
import json
import logging
import re
import time
import uuid
import traceback
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor
 
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.core.cache import cache
from django.http import Http404, HttpResponseBadRequest, JsonResponse
from django.shortcuts import redirect, render
from django.utils.html import escape
from django.views.decorators.http import require_GET, require_http_methods
 
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
 
from decouple import config
 
from .address_utils import process_address_maps
 
 
# ── Analytics ────────────────────────────────────────────────────────────────
 
try:
    from .redis_analytics import SearchAnalytics
    ANALYTICS_AVAILABLE = True
except ImportError:
    ANALYTICS_AVAILABLE = False
    SearchAnalytics = None
 
 
# ── Autocomplete ─────────────────────────────────────────────────────────────
 
try:
    from .searchapi import get_autocomplete
except ImportError:
    get_autocomplete = None
 
 
# ── Search submission ─────────────────────────────────────────────────────────
 
try:
    from .searchsubmission import process_search_submission
except ImportError:
    process_search_submission = None
 
 
# ── Word Discovery — handled inside bridge, not needed here ──────────────────
 
word_discovery_multi = None
 
 
# ── Intent Detection ──────────────────────────────────────────────────────────
 
try:
    from .intent_detect import detect_intent
    INTENT_AVAILABLE = True
except ImportError:
    INTENT_AVAILABLE = False
    detect_intent = None
 
 
# ── typesense_bridge_v3 — core search functions ───────────────────────────────
 
try:
    from .typesense_bridge_v3 import (
        # Main entry point
        execute_full_search,
        # Compatibility stubs
        detect_query_intent,
        get_facets,
        get_featured_result,
        get_related_searches,
        log_search_event,
        # Cache helpers used directly in the view
        _generate_stable_cache_key,
        _get_cached_results,
        _has_real_images,
        # Document fetching used directly in the view
        fetch_full_documents,
        client,  
    )
    BRIDGE_AVAILABLE = True
except ImportError as e:
    BRIDGE_AVAILABLE = False
    execute_full_search        = None
    detect_query_intent        = None
    get_facets                 = None
    get_featured_result        = None
    get_related_searches       = None
    log_search_event           = None
    _generate_stable_cache_key = None
    _get_cached_results        = None
    _has_real_images           = None
    fetch_full_documents       = None
    print(f"⚠️ typesense_bridge_v3 not available: {e}")
 
 
# ── typesense_bridge_v3 — debug functions ─────────────────────────────────────
 
try:
    from .typesense_bridge_v3 import (
        _run_word_discovery,
        _run_embedding,
        run_parallel_prep,
        _read_v3_profile,
        build_typesense_params,
        build_filter_string_without_data_type,
        _resolve_blend,
        _extract_authority_score,
        _compute_text_score,
        _compute_semantic_score,
        _domain_relevance,
        _content_intent_match,
        _pool_type_multiplier,
        _score_document,
        fetch_candidate_uuids,
        fetch_candidate_uuids_from_questions,
        fetch_all_candidate_uuids,
        semantic_rerank_candidates,
        fetch_candidate_metadata,
        fetch_candidates_with_metadata,
        count_all,
        fetch_documents_by_semantic_uuid,
        _should_trigger_ai_overview,
        _build_ai_overview,
        # Validation functions (sync, CPU-only)
        _normalize_signal,
        _extract_query_signals,
        _validate_question_hit,
        # Constants
        BLEND_RATIOS,
        SEMANTIC_DISTANCE_GATE,
        QUESTION_SEMANTIC_DISTANCE_GATE
    )
    DEBUG_BRIDGE_AVAILABLE = True
except ImportError as e:
    DEBUG_BRIDGE_AVAILABLE = False
    DEBUG_BRIDGE_IMPORT_ERROR = str(e)
    print(f"⚠️ typesense_bridge_v3 debug imports not available: {e}")
 
 
# ── Thread pool for debug endpoints ──────────────────────────────────────────
 
_debug_executor = ThreadPoolExecutor(max_workers=4)
 
 


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
    location = get_location_from_request(request) or {}
    
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
    
    if not trending_results:
        trending_results = get_trending_results(city=None, limit=6)
        trending_label = 'Your Area'
    
    context = {
        'city': city,
        'trending_label': trending_label,
        'trending_results': trending_results,
        'supported_cities': list(SUPPORTED_CITIES),
        'user_location': location,
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


import asyncio

def _run_async(coro):
    """Run an async coroutine from synchronous code."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)

# View for Searching 

# def search(request):
#     """
#     Main search endpoint with dynamic tab filtering.
    
#     Supports filtering by:
#     - data_type: content, service, product, person, media, location (tabs)
#     - category: document_category values (secondary filter)
#     - schema: document_schema values (tertiary filter)
    
#     Related searches are now handled inside execute_full_search().
    
#     ASYNC BRIDGE COMPATIBILITY:
#       The bridge functions (execute_full_search, fetch_full_documents)
#       are now async. This view stays synchronous and wraps those calls
#       with _run_async() to avoid Django's SynchronousOnlyOperation error.
#     """
    
#     # === 1. EXTRACT & VALIDATE PARAMETERS ===
#     params = SearchParams(request)

#     # === 0. PREVENT FORM RESUBMISSION ON TAB CLICKS ===
#     if params.query and not request.GET.get('_rd'):
#         from urllib.parse import urlparse
#         from django.shortcuts import redirect
#         referer = request.META.get('HTTP_REFERER', '')
#         if referer:
#             referer_path = urlparse(referer).path
#             current_path = request.path
#             if referer_path != current_path:
#                 return redirect(request.get_full_path() + '&_rd=1')

#     page = validate_page(request.GET.get('page', 1))
#     per_page = validate_per_page(request.GET.get('per_page', 10))

#     # === 1B. EXTRACT QUESTION PATH FIELDS ===
#     document_uuid = request.GET.get('document_uuid', '').strip()
#     search_source = request.GET.get('search_source', '').strip()
#     answer = request.GET.get('answer', '').strip()
#     answer_type = request.GET.get('answer_type', '').strip()
#     is_question_path = (search_source == 'question' and bool(document_uuid))
    
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
#             'display_total': 0,
#             'total_results': 0,
#             'facet_total': 0,
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'active_data_type': active_data_type,
#             'active_category': active_category,
#             'active_schema': active_schema,
#             'filters': filters,
#             'facets': {},
#             'safe_search': safe_search,
#             'was_corrected': False,
#             'corrected_query': params.query,
#             'word_corrections': [],
#             'corrections': {},
#             'intent': {},
#             'search_type': 'keyword' if params.is_keyword_search else 'semantic',
#             'alt_mode': params.alt_mode,
#             'related_searches': [],
#             'suggestions': [],
#             'featured': None,
#             'answer': '',
#             'answer_type': '',
#             'pagination': None,
#             'page': page,
#             'per_page': per_page,
#             'search_time': 0,
#             'search_time_ms': 0,
#             'from_cache': False,
#             'from_semantic_cache': False,
#             'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
#             'device_type': device_type,
#             'source': params.source,
#             'user_city': location.get('city', '') if location else '',
#             'user_country': location.get('country', '') if location else '',
#             'data_type_labels': DATA_TYPE_LABELS,
#             'category_labels': CATEGORY_LABELS,
#             'search_source': search_source,
#             'document_uuid': document_uuid,
#             'is_question_path': is_question_path,
#             'request_id': request_id,
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
#             'display_total': 0,
#             'total_results': 0,
#             'facet_total': 0,
#             'filters': filters,
#             'facets': {},
#             'safe_search': safe_search,
#             'was_corrected': False,
#             'corrected_query': '',
#             'word_corrections': [],
#             'corrections': {},
#             'intent': {},
#             'search_type': 'keyword' if params.is_keyword_search else 'semantic',
#             'alt_mode': params.alt_mode,
#             'related_searches': [],
#             'suggestions': [],
#             'featured': None,
#             'answer': '',
#             'answer_type': '',
#             'categorized_results': {},
#             'pagination': None,
#             'page': page,
#             'per_page': per_page,
#             'search_time': 0,
#             'search_time_ms': 0,
#             'from_cache': False,
#             'from_semantic_cache': False,
#             'search_strategy': 'keyword' if params.is_keyword_search else 'semantic',
#             'device_type': device_type,
#             'source': params.source,
#             'user_city': location.get('city', '') if location else '',
#             'user_country': location.get('country', '') if location else '',
#             'data_type_labels': DATA_TYPE_LABELS,
#             'category_labels': CATEGORY_LABELS,
#             'search_source': search_source,
#             'document_uuid': document_uuid,
#             'is_question_path': is_question_path,
#             'request_id': request_id,
#         })
    
#     # === 6. INITIALIZE ALL VARIABLES ===
    
#     result = {}
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
    
#     data_type_facets = []
#     category_facets = []
#     schema_facets = []
#     facet_total = 0
    
#     related_searches = []
    
#     has_filters = bool(active_data_type or active_category or active_schema)
    
#     # === 6A. GENERATE CACHE KEY ===
#     cache_key = get_cache_key(
#         'search', params.query, page, params.alt_mode,
#         active_data_type, active_category, active_schema,
#         json.dumps(filters, sort_keys=True)
#     )
    
#     # === 6B. CHECK EXISTING RESULT CACHE ===
#     cached_result = safe_cache_get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
#         cached_result['from_semantic_cache'] = False
        
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
#             try:
#                 intent = detect_query_intent(corrected_query, tuple_array)
#             except Exception as e:
#                 logger.warning(f"Intent detection error: {e}")
#                 intent = {}
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
#             try:
#                 intent = detect_query_intent(corrected_query, tuple_array)
#             except Exception as e:
#                 logger.warning(f"Intent detection error: {e}")
#                 intent = {}
    
#     # === 8. EXECUTE SEARCH ===
#     if execute_full_search:
#         try:
#             # _run_async wraps the async bridge call for sync context
#             result = _run_async(execute_full_search(
#                 query=params.query,
#                 session_id=params.session_id,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 alt_mode=params.alt_mode,
#                 user_location=user_location,
#                 pos_tags=tuple_array if params.is_semantic_search else [],
#                 safe_search=safe_search,
#                 search_source=search_source,
#                 answer=answer if is_question_path else None,
#                 answer_type=answer_type if is_question_path else None,
#                 document_uuid=document_uuid if is_question_path else None,
#             ))
            
#             results = result.get('results', [])
#             total_results = result.get('total', 0)
#             search_time = result.get('search_time', 0)
#             search_strategy = result.get('search_strategy', search_type)
            
#             # === Get correction info from bridge ===
#             bridge_corrected = result.get('corrected_query', params.query)
#             if bridge_corrected and bridge_corrected.lower() != params.query.lower():
#                 corrected_query = bridge_corrected
#                 was_corrected = True
#                 word_corrections = result.get('word_discovery', {}).get('corrections', [])
            
#             # Get facets from the search result
#             data_type_facets = result.get('data_type_facets', [])
#             category_facets = result.get('category_facets', [])
#             schema_facets = result.get('schema_facets', [])
#             facet_total = result.get('facet_total', 0)
            
#             # Get related searches from the search result
#             related_searches = result.get('related_searches', [])

#         except Exception as e:
#             logger.error(f"Search execution error: {e}", exc_info=True)
    
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
#     featured = None
    
#     if results:
#         if get_facets:
#             try:
#                 facets = get_facets(corrected_query)
#             except Exception:
#                 pass
        
#         if not related_searches and get_related_searches:
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
    
#     map_data = process_address_maps(request, results)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Core search
#         'query': params.query,
#         **map_data,
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
#         'answer': result.get('answer', ''),
#         'answer_type': result.get('answer_type', ''),
        
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
#         'from_semantic_cache': False,
#         'search_strategy': search_strategy,
        
#         # Device & location
#         'device_type': device_type,
#         'source': params.source,
#         'user_city': location.get('city', '') if location else '',
#         'user_country': location.get('country', '') if location else '',
        
#         # Label mappings
#         'data_type_labels': DATA_TYPE_LABELS,
#         'category_labels': CATEGORY_LABELS,

#         # Question path tracking
#         'search_source': search_source,
#         'document_uuid': document_uuid,
#         'is_question_path': is_question_path,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         safe_cache_set(cache_key, context, SEARCH_CONFIG['cache_timeout'])

#     # NOTE: Trending is now driven by clicks only — see click_redirect view.
#     # No cache_trending_result() calls here.

#     return render(request, 'results2.html', context)

def search(request):
    """
    Main search endpoint with dynamic tab filtering.
    """
    
    # === 1. EXTRACT & VALIDATE PARAMETERS ===
    params = SearchParams(request)

    # === 0. PREVENT FORM RESUBMISSION ON TAB CLICKS ===
    if params.query and not request.GET.get('_rd'):
        from urllib.parse import urlparse
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
    
    active_data_type = validate_data_type(request.GET.get('data_type', ''))
    active_category = sanitize_filter_value(request.GET.get('category', ''))
    active_schema = validate_schema(request.GET.get('schema', ''))
    
    filters = {
        'data_type': active_data_type,
        'category': active_category,
        'schema': active_schema,
        'source': sanitize_filter_value(source_filter) if source_filter else None,
        'time_range': sanitize_filter_value(request.GET.get('time', '')),
        'location': sanitize_filter_value(request.GET.get('location', '')),
        'sort': validate_sort(request.GET.get('sort', ''), ['relevance', 'recent', 'authority'], 'relevance'),
    }
    filters = {k: v for k, v in filters.items() if v}
    
    safe_search = request.GET.get('safe', 'on') == 'on'
    
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
    result = {}
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
    
    data_type_facets = []
    category_facets = []
    schema_facets = []
    facet_total = 0
    
    related_searches = []
    
    image_results = []
    image_count = 0
    image_pagination = None
    
    has_filters = bool(active_data_type or active_category or active_schema)
    
    # === 6A. GENERATE CACHE KEY ===
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
            result = _run_async(execute_full_search(
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
            ))
            
            results = result.get('results', [])
            total_results = result.get('total', 0)
            search_time = result.get('search_time', 0)
            search_strategy = result.get('search_strategy', search_type)
            
            bridge_corrected = result.get('corrected_query', params.query)
            if bridge_corrected and bridge_corrected.lower() != params.query.lower():
                corrected_query = bridge_corrected
                was_corrected = True
                word_corrections = result.get('word_discovery', {}).get('corrections', [])
            
            data_type_facets = result.get('data_type_facets', [])
            category_facets = result.get('category_facets', [])
            schema_facets = result.get('schema_facets', [])
            facet_total = result.get('facet_total', 0)
            
            related_searches = result.get('related_searches', [])
            # DEBUG — remove after testing
            if results:
                print(f"🔗 8B DEBUG: title='{results[0].get('title', '')[:50]}' sem_uuid='{results[0].get('semantic_uuid', 'MISSING')}' id='{results[0].get('id', 'MISSING')}'")
            
            image_count = result.get('total_image_count', 0)
            
# === 8B. SEMANTIC RELATED SEARCHES ===
            if results and fetch_documents_by_semantic_uuid and not is_question_path:
            # if results and fetch_documents_by_semantic_uuid:
                try:
                    top_result = results[0]
                    sem_uuid = top_result.get('semantic_uuid', '')
                    if sem_uuid and not sem_uuid.startswith('sem-solo-'):
                        cluster_docs = _run_async(fetch_documents_by_semantic_uuid(
                            semantic_uuid=sem_uuid,
                            exclude_uuid=top_result.get('id', ''),
                            limit=5,
                        ))
                        if cluster_docs:
                            semantic_related = [
                                {'query': doc['title'], 'url': doc.get('url', '')}
                                for doc in cluster_docs
                                if doc.get('title')
                            ]
                            print(f"🔗 8B semantic_related built: {len(semantic_related)} items")
                            if semantic_related:
                                if related_searches:
                                    existing_titles = {r.get('query', '').lower() for r in related_searches}
                                    for sr in semantic_related:
                                        if sr['query'].lower() not in existing_titles:
                                            related_searches.insert(0, sr)
                                        else:
                                            print(f"🔗 8B DEDUPE: '{sr['query'][:50]}' already in related_searches")
                                else:
                                    related_searches = semantic_related
                            print(f"🔗 8B final related_searches count: {len(related_searches)}")
                except Exception as e:
                    logger.warning(f"Semantic related searches error: {e}")
                    print(f"🔗 8B ERROR: {e}")
            
            # === 8C. IMAGE DATA ===
            if show_images:
                try:
                    stable_key = _generate_stable_cache_key(params.session_id, params.query)
                    finished = _run_async(_get_cached_results(stable_key))

                    if finished and finished.get('all_results'):
                        all_candidates = finished['all_results']
                        has_image = [r for r in all_candidates if _has_real_images(r)]

                        if has_image:
                            first_batch = has_image[:20]
                            page_ids = [item['id'] for item in first_batch if item.get('id')]
                            if page_ids:
                                full_docs = _run_async(fetch_full_documents(page_ids, params.query))
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

                image_pagination = None
            else:
                image_results = []
                image_pagination = None

        except Exception as e:
            logger.error(f"Search execution error: {e}", exc_info=True)
    
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

    # NOTE: Trending is now driven by clicks only — see click_redirect view.

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
    
    # Extract extra fields sent by the beacon for trending
    result_data_type = sanitize_string(data.get('data_type', ''), max_length=100)
    result_schema = sanitize_string(data.get('schema', ''), max_length=100)
    result_summary = sanitize_string(data.get('summary', ''), max_length=500)
    result_image = sanitize_string(data.get('image', ''), max_length=2000)
    result_brand = sanitize_string(data.get('brand', data.get('source', '')), max_length=200)
    
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
        except Exception as e:
            logger.error(f"Click tracking error: {e}")
            return JsonResponse(
                {'success': False, 'error': 'Tracking failed'},
                status=500
            )
        
        # === Feed trending cache (deduplicated per session) ===
        # Store at city, region, country, and general levels so the home
        # page finds matching data regardless of how the visitor's IP resolves.
        if query:
            try:
                from .trending import cache_trending_result
                
                loc_city = location.get('city', '') if location else ''
                loc_region = location.get('region', '') if location else ''
                loc_country = location.get('country', '') if location else ''
                
                clicked_doc = {
                    'title': result_title,
                    'url': clicked_url,
                    'result_id': result_id,
                    'source': result_brand,
                    'category': result_data_type,
                    'schema': result_schema,
                    'summary': result_summary,
                    'image': result_image,
                }
                
                # Store at each location level
                for loc_value in [loc_city, loc_region, loc_country]:
                    if loc_value:
                        cache_trending_result(
                            query=query,
                            top_result=clicked_doc,
                            city=loc_value,
                            session_id=session_id,
                        )
                
                # Always store to general as the global fallback
                cache_trending_result(
                    query=query,
                    top_result=clicked_doc,
                    city=None,
                    session_id=session_id,
                )
            except Exception as e:
                logger.warning(f"Trending cache error in track_click: {e}")
        
        return JsonResponse({'success': True})
    
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
        
        # === Feed trending cache (deduplicated per session) ===
        if session_id and query:
            try:
                from .trending import cache_trending_result
                cache_trending_result(
                    query=query,
                    top_result={
                        'title': result_title,
                        'url': destination_url,
                        'result_id': result_id,
                        'source': result_source,
                    },
                    city=location.get('city', '') if location else None,
                    session_id=session_id,
                )
            except Exception as e:
                logger.warning(f"Trending cache error in click_redirect: {e}")
    
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


# ============================================================
# views_debug.py — DEBUG ENDPOINTS (ASYNC-COMPATIBLE)
# ============================================================
#
# UPDATED FOR WORD DISCOVERY v4 PIPELINE:
#   - debug_keyword, debug_semantic, debug_question are unchanged
#     because they consume the same profile dict output.
#   - debug_word_discovery is fully rewritten to trace the v4
#     4-step pipeline:
#       Step 1: Tokenize + Known Sets + RAM Hash Lookup
#       Step 2: Clean + Resolve (corrections → POS → "in" disambig)
#       Step 3: N-gram Resolution (single pass on corrected words)
#       Step 4: Build Profile + Intent
#
# REQUIREMENTS:
#   - Django 4.1+ (for async view support)
# ============================================================


# ── typesense client (for debug_question raw search) ─────────────────────────
# (client already imported from typesense_bridge_v3 at top of file)


# ============================================================
# DEBUG HELPER FUNCTIONS
# ============================================================

def _infer_domain(signals: dict) -> Optional[str]:
    """Infer primary_domain from intent signals."""
    if signals.get('has_food_word'):
        return 'food'
    if signals.get('has_beauty_word'):
        return 'beauty'
    if signals.get('has_entertainment_word'):
        return 'music'
    if signals.get('has_culture_word'):
        return 'culture'
    if signals.get('has_service_word') and signals.get('is_local_search'):
        return 'business'
    return signals.get('primary_domain')


def _patch_signals_domain(signals: dict) -> dict:
    """Return a copy of signals with primary_domain filled in if null."""
    if signals.get('primary_domain'):
        return signals
    patched = dict(signals)
    patched['primary_domain'] = _infer_domain(signals)
    return patched


def _build_full_result(
    scored_item: dict,
    full_doc: dict,
    signals: dict,
    source_pool: str = 'document'
) -> dict:
    """Merge a scored metadata item with its full Typesense document."""
    query_mode = signals.get('query_mode', 'explore')

    dom_m  = _domain_relevance(scored_item, signals)
    cint_m = _content_intent_match(scored_item, query_mode)
    pool_m = _pool_type_multiplier(scored_item, query_mode)

    return {
        'rank':          scored_item.get('rank', 0),
        'document_uuid': scored_item.get('id', ''),
        'source_pool':   source_pool,

        'title':                full_doc.get('title', scored_item.get('title', '(no title)')),
        'data_type':            full_doc.get('data_type', scored_item.get('data_type', '')),
        'category':             full_doc.get('category', scored_item.get('category', '')),
        'schema':               full_doc.get('schema', scored_item.get('schema', '')),
        'url':                  full_doc.get('url', ''),
        'summary':              full_doc.get('summary', ''),
        'source':               full_doc.get('source', ''),
        'black_owned':          full_doc.get('black_owned', scored_item.get('black_owned', False)),
        'service_type':         full_doc.get('service_type', []),
        'service_specialties':  full_doc.get('service_specialties', []),
        'service_rating':       full_doc.get('service_rating'),
        'service_review_count': full_doc.get('service_review_count'),
        'service_price_range':  full_doc.get('service_price_range'),
        'service_hours':        full_doc.get('service_hours'),
        'location': {
            'city':    full_doc.get('location', {}).get('city'),
            'state':   full_doc.get('location', {}).get('state'),
            'address': full_doc.get('location', {}).get('address'),
        },
        'key_facts':     full_doc.get('key_facts', []),
        'image_url':     full_doc.get('image_url', []),
        'logo_url':      full_doc.get('logo_url', []),
        'published_date': full_doc.get('published_date', ''),
        'semantic_uuid': full_doc.get('semantic_uuid', ''),

        'scores': {
            'blended':         round(scored_item.get('blended_score', 0), 4),
            'text':            round(scored_item.get('text_score', 0), 4),
            'semantic':        round(scored_item.get('sem_score', 0), 4),
            'authority':       round(scored_item.get('auth_score', 0), 4),
            'vector_distance': round(scored_item.get('vector_distance', 1.0), 4),
        },

        'multipliers': {
            'domain_relevance':    round(dom_m, 3),
            'content_intent':      round(cint_m, 3),
            'pool_type':           round(pool_m, 3),
            'combined_multiplier': round(dom_m * cint_m * pool_m, 3),
        },

        'authority_inputs': {
            'authority_score':       scored_item.get('authority_score', 0),
            'service_rating':        scored_item.get('service_rating', 0),
            'service_review_count':  scored_item.get('service_review_count', 0),
            'product_rating':        scored_item.get('product_rating', 0),
            'product_review_count':  scored_item.get('product_review_count', 0),
            'recipe_rating':         scored_item.get('recipe_rating', 0),
            'recipe_review_count':   scored_item.get('recipe_review_count', 0),
            'media_rating':          scored_item.get('media_rating', 0),
            'factual_density_score': scored_item.get('factual_density_score', 0),
            'evergreen_score':       scored_item.get('evergreen_score', 0),
        },

        'why_this_score': {
            'domain_match':    f"primary_domain={signals.get('primary_domain')} "
                               f"category={scored_item.get('category', '')} "
                               f"service_type={full_doc.get('service_type', [])}",
            'content_intent':  f"doc_intent={scored_item.get('content_intent', 'empty')} "
                               f"mode={query_mode}",
            'pool_type_note':  f"data_type={scored_item.get('data_type', '')} "
                               f"allowed_for_{query_mode}="
                               f"{pool_m == 1.0}",
            'authority_note':  'no rating/review data' if scored_item.get('auth_score', 0) == 0
                               else f"auth_score_n={scored_item.get('auth_score', 0):.3f}",
        },
    }


def _debug_error_response(endpoint: str, query: str, err: Exception) -> dict:
    """Standard error envelope for all debug endpoints."""
    return {
        'endpoint':  endpoint,
        'query':     query,
        'status':    'error',
        'error':     str(err),
        'traceback': traceback.format_exc(),
    }


# ============================================================
# DEBUG ENDPOINT 1 — /debug/keyword/
# ============================================================

@require_GET
async def debug_keyword(request):
    """
    Tests the keyword path only.

    URL:
        /debug/keyword/?query=restaurants+in+atlanta
        /debug/keyword/?query=best+soul+food+houston
    """
    if not DEBUG_BRIDGE_AVAILABLE:
        return JsonResponse(
            {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
            status=500
        )

    query = request.GET.get('query', '').strip()
    if not query:
        return JsonResponse({'error': 'query parameter required'}, status=400)

    t0     = time.time()
    report = {
        'endpoint': 'debug_keyword',
        'query':    query,
        'status':   'ok',
        'timings':  {},
        'steps':    {},
    }

    try:
        # ── Step 1: Word Discovery ────────────────────────────────────────
        t1        = time.time()
        discovery = await _run_word_discovery(query)
        report['timings']['word_discovery_ms'] = round((time.time() - t1) * 1000, 2)

        report['steps']['word_discovery'] = {
            'corrected_query': discovery.get('corrected_query', query),
            'search_terms':    discovery.get('search_terms', []),
            'cities':          [c['name'] for c in discovery.get('cities', [])],
            'states':          [s['name'] for s in discovery.get('states', [])],
            'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
            'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
            'primary_intent':  discovery.get('primary_intent', ''),
            'corrections':     discovery.get('corrections', []),
            'stats':           discovery.get('stats', {}),
        }

        # ── Step 2: Intent Detection ──────────────────────────────────────
        signals = {}
        if INTENT_AVAILABLE and detect_intent:
            t2        = time.time()
            discovery = await asyncio.to_thread(detect_intent, discovery)
            signals   = discovery.get('signals', {})
            report['timings']['intent_ms'] = round((time.time() - t2) * 1000, 2)

        signals = _patch_signals_domain(signals)

        report['steps']['intent'] = {
            'query_mode':             signals.get('query_mode', 'unknown'),
            'primary_domain':         signals.get('primary_domain', ''),
            'is_local_search':        signals.get('is_local_search', False),
            'has_food_word':          signals.get('has_food_word', False),
            'has_service_word':       signals.get('has_service_word', False),
            'has_beauty_word':        signals.get('has_beauty_word', False),
            'has_black_owned':        signals.get('has_black_owned', False),
            'has_superlative':        signals.get('has_superlative', False),
            'has_unknown_terms':      signals.get('has_unknown_terms', False),
            'wants_single_result':    signals.get('wants_single_result', False),
            'wants_multiple_results': signals.get('wants_multiple_results', False),
            'local_search_strength':  signals.get('local_search_strength', 'none'),
            'domain_inferred':        True,
        }

        # ── Step 3: Profile ───────────────────────────────────────────────
        profile = _read_v3_profile(discovery, signals=signals)

        report['steps']['profile'] = {
            'search_terms':         profile.get('search_terms', []),
            'field_boosts':         profile.get('field_boosts', {}),
            'preferred_data_types': profile.get('preferred_data_types', []),
            'has_location':         profile.get('has_location', False),
        }

        # ── Step 4: Typesense params ──────────────────────────────────────
        ts_params = build_typesense_params(profile, signals=signals)

        report['steps']['typesense_params'] = {
            'q':         ts_params.get('q', ''),
            'query_by':  ts_params.get('query_by', ''),
            'filter_by': ts_params.get('filter_by', ''),
            'sort_by':   ts_params.get('sort_by', ''),
            'num_typos': ts_params.get('num_typos', 0),
        }

        # ── Step 5: Fetch candidates ──────────────────────────────────────
        t3         = time.time()
        candidates = await fetch_candidates_with_metadata(query, profile, signals=signals)
        report['timings']['fetch_candidates_ms'] = round((time.time() - t3) * 1000, 2)

        report['steps']['candidates'] = {
            'total_returned': len(candidates),
        }

        # ── Step 6: Resolve blend + score ────────────────────────────────
        t4        = time.time()
        blend     = _resolve_blend(signals.get('query_mode', 'explore'), signals, candidates)
        pool_size = len(candidates)

        for idx, item in enumerate(candidates):
            _score_document(
                idx         = idx,
                item        = item,
                profile     = profile,
                signals     = signals,
                blend       = blend,
                pool_size   = pool_size,
                vector_data = {},
            )

        candidates.sort(key=lambda x: -x.get('blended_score', 0))
        for i, item in enumerate(candidates):
            item['rank'] = i

        report['timings']['scoring_ms'] = round((time.time() - t4) * 1000, 2)

        report['steps']['blend_used'] = {
            'text_match': round(blend['text_match'], 3),
            'semantic':   round(blend['semantic'], 3),
            'authority':  round(blend['authority'], 3),
            'note':       'authority=0 means no service_rating/review_count in data',
        }

        # ── Step 7: Fetch full documents for ALL results ──────────────────
        t5       = time.time()
        all_ids  = [item.get('id') for item in candidates if item.get('id')]
        full_docs_list = await fetch_full_documents(all_ids, query)
        full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
        report['timings']['fetch_full_docs_ms'] = round((time.time() - t5) * 1000, 2)

        # ── Step 8: Facets ────────────────────────────────────────────────
        counts = count_all(candidates)
        report['steps']['facets']            = counts['facets']
        report['steps']['total_image_count'] = counts['total_image_count']

        # ── Full ranked results ───────────────────────────────────────────
        report['ranked_results'] = [
            _build_full_result(
                scored_item = item,
                full_doc    = full_docs_map.get(item.get('id'), {}),
                signals     = signals,
                source_pool = 'document',
            )
            for item in candidates
        ]

        report['steps']['total_results'] = len(report['ranked_results'])

    except Exception as e:
        report = _debug_error_response('debug_keyword', query, e)

    report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
    return JsonResponse(report, json_dumps_params={'indent': 2})


# ============================================================
# DEBUG ENDPOINT 2 — /debug/semantic/
# ============================================================

@require_GET
async def debug_semantic(request):
    """
    Tests the full semantic path.

    URL:
        /debug/semantic/?query=restaurants+in+atlanta
        /debug/semantic/?query=what+is+soul+food
    """
    if not DEBUG_BRIDGE_AVAILABLE:
        return JsonResponse(
            {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
            status=500
        )

    query = request.GET.get('query', '').strip()
    if not query:
        return JsonResponse({'error': 'query parameter required'}, status=400)

    t0     = time.time()
    report = {
        'endpoint': 'debug_semantic',
        'query':    query,
        'status':   'ok',
        'timings':  {},
        'steps':    {},
    }

    try:
        # ── Step 1: Word Discovery + embedding in parallel ────────────────
        t1                         = time.time()
        discovery, query_embedding = await run_parallel_prep(query)
        report['timings']['parallel_prep_ms'] = round((time.time() - t1) * 1000, 2)

        report['steps']['word_discovery'] = {
            'corrected_query': discovery.get('corrected_query', query),
            'search_terms':    discovery.get('search_terms', []),
            'cities':          [c['name'] for c in discovery.get('cities', [])],
            'states':          [s['name'] for s in discovery.get('states', [])],
            'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
            'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
            'primary_intent':  discovery.get('primary_intent', ''),
            'stats':           discovery.get('stats', {}),
        }

        report['steps']['embedding'] = {
            'generated':  query_embedding is not None,
            'dimensions': len(query_embedding) if query_embedding else 0,
        }

        # ── Step 2: Intent Detection ──────────────────────────────────────
        signals = {}
        if INTENT_AVAILABLE and detect_intent:
            t2        = time.time()
            discovery = await asyncio.to_thread(detect_intent, discovery)
            signals   = discovery.get('signals', {})
            report['timings']['intent_ms'] = round((time.time() - t2) * 1000, 2)

        signals    = _patch_signals_domain(signals)
        query_mode = signals.get('query_mode', 'explore')

        report['steps']['intent'] = {
            'query_mode':             query_mode,
            'primary_domain':         signals.get('primary_domain', ''),
            'is_local_search':        signals.get('is_local_search', False),
            'has_food_word':          signals.get('has_food_word', False),
            'has_service_word':       signals.get('has_service_word', False),
            'has_black_owned':        signals.get('has_black_owned', False),
            'has_superlative':        signals.get('has_superlative', False),
            'has_unknown_terms':      signals.get('has_unknown_terms', False),
            'wants_single_result':    signals.get('wants_single_result', False),
            'local_search_strength':  signals.get('local_search_strength', 'none'),
        }

        # ── Step 3: Profile ───────────────────────────────────────────────
        profile = _read_v3_profile(discovery, signals=signals)

        # ── Step 4: Stage 1A + 1B in parallel ────────────────────────────
        t3 = time.time()

        doc_uuids, q_uuids = await asyncio.gather(
            fetch_candidate_uuids(query, profile, signals, 100),
            fetch_candidate_uuids_from_questions(
                profile, query_embedding, signals, 50, discovery
            ),
        )

        report['timings']['stage1_ms'] = round((time.time() - t3) * 1000, 2)

        doc_set = set(doc_uuids)
        q_set   = set(q_uuids)
        overlap = doc_set & q_set

        report['steps']['stage1'] = {
            'document_collection': {'uuid_count': len(doc_uuids)},
            'questions_collection': {
                'uuid_count': len(q_uuids),
                'note': 'document_uuids linked from matching questions',
            },
            'overlap': {
                'count': len(overlap),
                'note':  'Found by both paths — highest confidence',
            },
        }

        # ── Step 5: Merge ─────────────────────────────────────────────────
        merged = []
        seen   = set()
        for uid in doc_uuids:
            if uid in overlap and uid not in seen:
                merged.append(uid); seen.add(uid)
        for uid in doc_uuids:
            if uid not in seen:
                merged.append(uid); seen.add(uid)
        for uid in q_uuids:
            if uid not in seen:
                merged.append(uid); seen.add(uid)

        # ── Step 6: Stage 2 — vector rerank ──────────────────────────────
        t4       = time.time()
        reranked = await semantic_rerank_candidates(merged, query_embedding, max_to_rerank=500)
        report['timings']['stage2_rerank_ms'] = round((time.time() - t4) * 1000, 2)

        vector_data = {
            item['id']: {
                'vector_distance': item.get('vector_distance', 1.0),
                'semantic_rank':   item.get('semantic_rank', 999999),
            }
            for item in reranked
        }

        report['steps']['stage2_rerank'] = {
            'reranked_count': len(reranked),
            'top_10_by_vector': [
                {
                    'document_uuid':   r['id'],
                    'vector_distance': round(r['vector_distance'], 4),
                    'passes_gate':     r['vector_distance'] < SEMANTIC_DISTANCE_GATE,
                }
                for r in reranked[:10]
            ],
        }

        # ── Step 7: Stage 3 — distance prune ─────────────────────────────
        DISTANCE_THRESHOLDS = {
            'answer':  0.60, 'explore': 0.70, 'compare': 0.65,
            'browse':  0.85, 'local':   0.85, 'shop':    0.80,
        }
        threshold      = DISTANCE_THRESHOLDS.get(query_mode, 0.75)
        before_prune   = len(merged)
        survivor_uuids = [
            u for u in merged
            if vector_data.get(u, {}).get('vector_distance', 1.0) <= threshold
        ]

        report['steps']['stage3_prune'] = {
            'threshold': threshold,
            'before':    before_prune,
            'after':     len(survivor_uuids),
            'removed':   before_prune - len(survivor_uuids),
        }

        # ── Step 8: Stage 4 — metadata fetch ─────────────────────────────
        t5          = time.time()
        all_results = await fetch_candidate_metadata(survivor_uuids)
        report['timings']['stage4_metadata_ms'] = round((time.time() - t5) * 1000, 2)

        # ── Step 9: Resolve blend + score ────────────────────────────────
        blend     = _resolve_blend(query_mode, signals, all_results)
        pool_size = len(all_results)

        report['steps']['blend_resolved'] = {
            'text_match': round(blend['text_match'], 3),
            'semantic':   round(blend['semantic'], 3),
            'authority':  round(blend['authority'], 3),
        }

        t6 = time.time()
        for idx, item in enumerate(all_results):
            _score_document(
                idx         = idx,
                item        = item,
                profile     = profile,
                signals     = signals,
                blend       = blend,
                pool_size   = pool_size,
                vector_data = vector_data,
            )

        all_results.sort(key=lambda x: -x.get('blended_score', 0))
        for i, item in enumerate(all_results):
            item['rank'] = i

        report['timings']['scoring_ms'] = round((time.time() - t6) * 1000, 2)

        # ── Step 10: Fetch full documents for ALL results ─────────────────
        t7       = time.time()
        all_ids  = [item.get('id') for item in all_results if item.get('id')]
        full_docs_list = await fetch_full_documents(all_ids, query)
        full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
        report['timings']['fetch_full_docs_ms'] = round((time.time() - t7) * 1000, 2)

        # ── Facets ────────────────────────────────────────────────────────
        counts = count_all(all_results)
        report['steps']['facets']            = counts['facets']
        report['steps']['total_image_count'] = counts['total_image_count']

        # ── Full ranked results ───────────────────────────────────────────
        report['ranked_results'] = [
            _build_full_result(
                scored_item = item,
                full_doc    = full_docs_map.get(item.get('id'), {}),
                signals     = signals,
                source_pool = (
                    'overlap'   if item.get('id') in overlap else
                    'questions' if item.get('id') in q_set   else
                    'document'
                ),
            )
            for item in all_results
        ]

        report['steps']['total_results'] = len(report['ranked_results'])

    except Exception as e:
        report = _debug_error_response('debug_semantic', query, e)

    report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
    return JsonResponse(report, json_dumps_params={'indent': 2})


# ============================================================
# DEBUG ENDPOINT 3 — /debug/question/
# ============================================================

@require_GET
async def debug_question(request):
    """
    Tests the questions collection vector search with FULL validation.

    Each result shows the raw vector hit PLUS whether it would pass
    the bridge's distance gate and token validation.

    URL:
        /debug/question/?query=who+was+the+first+mayor+of+savannah
        /debug/question/?query=what+is+soul+food
    """
    if not DEBUG_BRIDGE_AVAILABLE:
        return JsonResponse(
            {'error': f'Debug bridge not available: {DEBUG_BRIDGE_IMPORT_ERROR}'},
            status=500
        )

    query = request.GET.get('query', '').strip()
    if not query:
        return JsonResponse({'error': 'query parameter required'}, status=400)

    t0     = time.time()
    report = {
        'endpoint': 'debug_question',
        'query':    query,
        'status':   'ok',
        'timings':  {},
        'steps':    {},
    }

    try:
        # ── Step 1: Word Discovery (to get profile for validation) ────────
        t1        = time.time()
        discovery = await _run_word_discovery(query)
        report['timings']['word_discovery_ms'] = round((time.time() - t1) * 1000, 2)

        profile = _read_v3_profile(discovery)

        report['steps']['word_discovery'] = {
            'corrected_query': discovery.get('corrected_query', query),
            'search_terms':    discovery.get('search_terms', []),
            'cities':          [c['name'] for c in discovery.get('cities', [])],
            'states':          [s['name'] for s in discovery.get('states', [])],
            'keywords':        [k.get('phrase', '') for k in discovery.get('keywords', [])],
            'persons':         [p.get('phrase', '') for p in discovery.get('persons', [])],
        }

        # ── Step 2: Extract validation signals from profile ───────────────
        query_tokens, query_phrases, primary_subject = _extract_query_signals(
            profile, discovery=discovery
        )

        location_tokens = set()
        for c in discovery.get('cities', []):
            location_tokens.update(_normalize_signal(c.get('name', '')))
        for s in discovery.get('states', []):
            location_tokens.update(_normalize_signal(s.get('name', '')))

        report['steps']['validation_signals'] = {
            'query_tokens':    sorted(query_tokens),
            'query_phrases':   query_phrases,
            'primary_subject': sorted(primary_subject) if primary_subject else None,
            'location_tokens': sorted(location_tokens),
            'distance_gate':   QUESTION_SEMANTIC_DISTANCE_GATE,
        }

        # ── Step 3: Embed the query ───────────────────────────────────────
        t2              = time.time()
        query_embedding = await _run_embedding(query)
        report['timings']['embedding_ms'] = round((time.time() - t2) * 1000, 2)

        if not query_embedding:
            return JsonResponse(
                {'error': 'Embedding failed — check embedding_client'},
                status=500
            )

        report['steps']['embedding'] = {
            'generated':  True,
            'dimensions': len(query_embedding),
        }

        # ── Step 4: Vector search against questions collection ────────────
        t3            = time.time()
        embedding_str = ','.join(str(x) for x in query_embedding)
        TOP_K         = 30

        search_params = {
            'q':              '*',
            'vector_query':   f'embedding:([{embedding_str}], k:{TOP_K})',
            'per_page':       TOP_K,
            'include_fields': 'document_uuid,question,answer_type,question_type,'
                              'primary_keywords,entities,semantic_keywords',
        }

        search_requests = {'searches': [{'collection': 'questions', **search_params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        hits            = response['results'][0].get('hits', [])
        report['timings']['vector_search_ms'] = round((time.time() - t3) * 1000, 2)

        # ── Step 5: Apply distance gate + token validation ────────────────
        THRESHOLDS = [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65]

        threshold_counts = {str(t): 0 for t in THRESHOLDS}
        results          = []
        accepted_count   = 0
        rejected_count   = 0

        for hit in hits:
            doc      = hit.get('document', {})
            distance = hit.get('vector_distance', 1.0)

            for t in THRESHOLDS:
                if distance < t:
                    threshold_counts[str(t)] += 1

            passes_gate = distance < QUESTION_SEMANTIC_DISTANCE_GATE

            passes_validation = False
            validation_reason = ''

            if not passes_gate:
                validation_reason = (
                    f'REJECTED: distance {distance:.4f} >= '
                    f'gate {QUESTION_SEMANTIC_DISTANCE_GATE}'
                )
            else:
                is_valid = _validate_question_hit(
                    hit_doc         = doc,
                    query_tokens    = query_tokens,
                    query_phrases   = query_phrases,
                    primary_subject = primary_subject,
                    min_matches     = 1,
                )

                if not is_valid:
                    validation_reason = 'REJECTED: token validation failed (no matching tokens in primary_keywords/entities/semantic_keywords)'
                elif location_tokens:
                    candidate_raw = (
                        doc.get('primary_keywords', []) +
                        doc.get('entities', []) +
                        doc.get('semantic_keywords', [])
                    )
                    candidate_tokens = set()
                    for val in candidate_raw:
                        if val:
                            candidate_tokens.update(_normalize_signal(val))

                    location_found = bool(location_tokens & candidate_tokens)

                    if not location_found:
                        validation_reason = (
                            f'REJECTED: location tokens {sorted(location_tokens)} '
                            f'not found in candidate'
                        )
                    else:
                        passes_validation = True
                        validation_reason = 'ACCEPTED: distance + tokens + location all pass'
                else:
                    passes_validation = True
                    validation_reason = 'ACCEPTED: distance + tokens pass (no location required)'

            if passes_validation:
                accepted_count += 1
            else:
                rejected_count += 1

            results.append({
                'document_uuid':       doc.get('document_uuid', ''),
                'question':            doc.get('question', ''),
                'question_type':       doc.get('question_type', ''),
                'answer_type':         doc.get('answer_type', ''),
                'primary_keywords':    doc.get('primary_keywords', []),
                'entities':            doc.get('entities', []),
                'vector_distance':     round(distance, 4),
                'passes_distance_gate': passes_gate,
                'passes_validation':   passes_validation,
                'validation_reason':   validation_reason,
                'passes_0_30':         distance < 0.30,
                'passes_0_35':         distance < 0.35,
                'passes_0_40':         distance < 0.40,
                'passes_0_45':         distance < 0.45,
                'passes_0_50':         distance < 0.50,
                'passes_0_55':         distance < 0.55,
                'passes_0_60':         distance < 0.60,
                'passes_0_65':         distance < 0.65,
                'document':            {},
            })

        results.sort(key=lambda x: x['vector_distance'])

        # ── Step 6: Fetch full documents for all question hits ────────────
        t4             = time.time()
        all_uuids      = [r['document_uuid'] for r in results if r.get('document_uuid')]
        full_docs_list = await fetch_full_documents(all_uuids, query)
        full_docs_map  = {doc.get('id'): doc for doc in full_docs_list}
        report['timings']['fetch_full_docs_ms'] = round((time.time() - t4) * 1000, 2)

        # ── Step 7: Merge full document into each result ──────────────────
        for r in results:
            full_doc      = full_docs_map.get(r['document_uuid'], {})
            r['document'] = {
                'title':        full_doc.get('title', ''),
                'url':          full_doc.get('url', ''),
                'data_type':    full_doc.get('data_type', ''),
                'category':     full_doc.get('category', ''),
                'summary':      full_doc.get('summary', ''),
                'source':       full_doc.get('source', ''),
                'black_owned':  full_doc.get('black_owned', False),
                'service_type': full_doc.get('service_type', []),
                'location': {
                    'city':    full_doc.get('location', {}).get('city'),
                    'state':   full_doc.get('location', {}).get('state'),
                },
                'key_facts':    full_doc.get('key_facts', []),
                'image_url':    full_doc.get('image_url', []),
            }

        # ── Step 8: Distance distribution + validation summary ────────────
        distances = [r['vector_distance'] for r in results]

        report['steps']['validation_summary'] = {
            'total_raw_hits': len(hits),
            'accepted':       accepted_count,
            'rejected':       rejected_count,
            'distance_gate':  QUESTION_SEMANTIC_DISTANCE_GATE,
            'note':           'Only accepted results would be returned by the bridge',
        }

        report['steps']['threshold_analysis'] = {
            'total_hits': len(hits),
            'counts_below_threshold': {
                f'below_{str(t).replace(".", "_")}': threshold_counts[str(t)]
                for t in THRESHOLDS
            },
        }

        report['steps']['distance_distribution'] = {
            'min':        round(min(distances, default=1.0), 4),
            'max':        round(max(distances, default=1.0), 4),
            'mean':       round(sum(distances) / max(len(distances), 1), 4),
            'below_0_30': sum(1 for d in distances if d < 0.30),
            'below_0_35': sum(1 for d in distances if d < 0.35),
            'below_0_40': sum(1 for d in distances if d < 0.40),
            'below_0_45': sum(1 for d in distances if d < 0.45),
            'below_0_50': sum(1 for d in distances if d < 0.50),
            'below_0_55': sum(1 for d in distances if d < 0.55),
            'below_0_60': sum(1 for d in distances if d < 0.60),
            'below_0_65': sum(1 for d in distances if d < 0.65),
        }

        report['results']                = results
        report['steps']['total_results'] = len(results)

    except Exception as e:
        report = _debug_error_response('debug_question', query, e)

    report['timings'] = report.get('timings', {})
    report['timings']['total_ms'] = round((time.time() - t0) * 1000, 2)
    return JsonResponse(report, json_dumps_params={'indent': 2})


# ============================================================
# DEBUG ENDPOINT 4 — /api/debug/word-discovery/
# ============================================================

async def debug_word_discovery(request):
    """
    GET /api/debug/word-discovery/?q=restuarants+in+atl

    Traces every word through every stage of Word Discovery v4.
    Shows the full journey of each word from raw input to final
    profile, with all intermediate states visible.
    """
    query = request.GET.get('q', '')
    if not query:
        return JsonResponse({'error': 'Missing ?q= parameter'}, status=400)

    try:
        from .word_discovery_fulltest import (
            WordDiscovery, vocab_cache_wrapper, STOPWORDS,
            LOCATION_SIGNAL_WORDS, LOCATION_CATEGORIES,
            CITY_CATEGORIES, STATE_CATEGORIES, PERSON_CATEGORIES,
            ORGANIZATION_CATEGORIES, KEYWORD_CATEGORIES, MEDIA_CATEGORIES,
            LOCATION_INTENT_FLAGS, KEYWORD_INTENT_FLAGS,
            normalize_pos_string, is_pos_compatible,
            RAM_CACHE_AVAILABLE, vocab_cache,
        )
    except ImportError:
        from word_discovery_fulltest import (
            WordDiscovery, vocab_cache_wrapper, STOPWORDS,
            LOCATION_SIGNAL_WORDS, LOCATION_CATEGORIES,
            CITY_CATEGORIES, STATE_CATEGORIES, PERSON_CATEGORIES,
            ORGANIZATION_CATEGORIES, KEYWORD_CATEGORIES, MEDIA_CATEGORIES,
            LOCATION_INTENT_FLAGS, KEYWORD_INTENT_FLAGS,
            normalize_pos_string, is_pos_compatible,
            RAM_CACHE_AVAILABLE, vocab_cache,
        )

    try:
        from .intent_detect import detect_intent as _detect_intent_local
        _INTENT_AVAILABLE = True
    except ImportError:
        try:
            from intent_detect import detect_intent as _detect_intent_local
            _INTENT_AVAILABLE = True
        except ImportError:
            _INTENT_AVAILABLE = False

    def run_trace(query):
        start = time.perf_counter()
        trace = {}

        # ── META ─────────────────────────────────────────────────────
        trace['meta'] = {
            'query': query,
            'pipeline_version': 'v4',
            'pipeline_order': [
                'Step 1: Tokenize + Known Sets + RAM Hash Lookup',
                'Step 2: Clean + Resolve (corrections → POS → "in" disambig)',
                'Step 3: N-gram Resolution (single pass on corrected words)',
                'Step 4: Build Profile + Intent',
            ],
            'ram_cache_available': RAM_CACHE_AVAILABLE,
            'ram_cache_loaded': bool(
                RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded
            ),
        }

        words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
        words = [w for w in words if w]
        trace['meta']['tokenized_words'] = words

        wd = WordDiscovery(verbose=False)
        cache = vocab_cache_wrapper

        # ═════════════════════════════════════════════════════════════
        # STEP 1: Tokenize + Known Sets + RAM Hash Lookup
        # ═════════════════════════════════════════════════════════════
        word_data = wd._step1_tokenize_and_lookup(words)

        step1 = []
        for wd_item in word_data:
            w = wd_item['word'].lower()
            entry = {
                'position': wd_item['position'],
                'word': w,
                'status': wd_item['status'],
                'is_stopword': wd_item['is_stopword'],
                'context_flags': list(wd_item.get('context_flags', [])),
                'location_context': wd_item.get('location_context', False),
            }

            if wd_item['is_stopword']:
                entry['path'] = 'STOPWORD'
                entry['pos'] = STOPWORDS.get(w, '')
            elif wd_item['status'] == 'known':
                entry['path'] = 'KNOWN_SET'
                entry['category'] = wd_item.get('selected_match', {}).get('category', '')
                entry['pos'] = wd_item.get('pos', '')
                for set_name in [
                    'COLORS', 'SUPERLATIVES', 'SIZES', 'COMMON_ADJECTIVES',
                    'FOOD_DINING', 'SERVICES', 'APPAREL_PRODUCTS', 'BEAUTY',
                    'CULTURE_COMMUNITY', 'MUSIC_ENTERTAINMENT', 'KNOWN_ACRONYMS',
                ]:
                    if w in getattr(wd, set_name, set()):
                        entry['known_set'] = set_name
                        break
            elif wd_item.get('abbreviation'):
                entry['path'] = 'ABBREVIATION'
                entry['expanded_to'] = wd_item['abbreviation']['expanded_to']
                entry['match_count'] = len(wd_item.get('all_matches', []))
                entry['all_matches'] = [
                    {
                        'term': m['term'], 'category': m['category'],
                        'pos': m['pos'], 'rank': m['rank'],
                    }
                    for m in wd_item.get('all_matches', [])
                ]
            elif wd_item['status'] == 'resolved':
                entry['path'] = 'RAM_HASH'
                entry['match_count'] = len(wd_item.get('all_matches', []))
                entry['all_matches'] = [
                    {
                        'term': m['term'], 'category': m['category'],
                        'pos': m['pos'], 'rank': m['rank'],
                    }
                    for m in wd_item.get('all_matches', [])
                ]
            elif wd_item['status'] == 'unknown':
                entry['path'] = 'NOT_FOUND'
                direct = cache.get_term_matches(w)
                entry['direct_ram_check'] = [
                    {
                        'term': m['term'], 'category': m['category'],
                        'pos': m['pos'], 'rank': m['rank'],
                    }
                    for m in direct
                ]
                entry['ram_empty'] = len(direct) == 0
            else:
                entry['path'] = wd_item['status']

            step1.append(entry)

        trace['step1_tokenize'] = {
            'description': 'Tokenize + Known Sets + RAM Hash Lookup',
            'words': step1,
            'summary': {
                'stopwords': sum(1 for s in step1 if s.get('is_stopword')),
                'known_set': sum(1 for s in step1 if s.get('path') == 'KNOWN_SET'),
                'ram_hash': sum(1 for s in step1 if s.get('path') == 'RAM_HASH'),
                'abbreviation': sum(1 for s in step1 if s.get('path') == 'ABBREVIATION'),
                'not_found': sum(1 for s in step1 if s.get('path') == 'NOT_FOUND'),
            },
        }

        # ═════════════════════════════════════════════════════════════
        # STEP 2a: Clean + Resolve (fuzzy corrections)
        # ═════════════════════════════════════════════════════════════
        has_unknowns = any(wd_item['status'] == 'unknown' for wd_item in word_data)

        if has_unknowns:
            corrections = wd._step2_clean_and_resolve(word_data)
        else:
            corrections = []

        step2a = []
        for wd_item in word_data:
            if wd_item['status'] not in (
                'corrected', 'pos_corrected', 'unknown', 'unknown_suggest',
            ):
                continue

            entry = {
                'position': wd_item['position'],
                'word': wd_item['word'],
                'status': wd_item['status'],
                'predicted_pos': wd_item.get('predicted_pos', ''),
                'context_flags_after_correction': list(
                    wd_item.get('context_flags', [])
                ),
            }

            if wd_item['status'] == 'corrected':
                entry['corrected_to'] = wd_item.get('corrected', '')
                entry['distance'] = wd_item.get('distance', 0)
                entry['corrected_pos'] = wd_item.get('pos', '')
                entry['corrected_category'] = wd_item.get(
                    'selected_match', {}
                ).get('category', '')
                entry['corrected_rank'] = wd_item.get(
                    'selected_match', {}
                ).get('rank', 0)
            elif wd_item['status'] == 'unknown_suggest':
                entry['suggestion'] = wd_item.get('suggestion', '')
                entry['suggestion_distance'] = wd_item.get(
                    'suggestion_distance', 0
                )
            elif wd_item['status'] == 'pos_corrected':
                entry['corrected_to'] = wd_item.get('corrected', '')
                entry['distance'] = wd_item.get('distance', 0)

            redis_sugg = wd_item.get('redis_suggestions', [])
            if redis_sugg:
                entry['redis_suggestions'] = [
                    {
                        'term': s['term'], 'distance': s['distance'],
                        'rank': s['rank'], 'pos': s['pos'],
                        'category': s['category'],
                        'compatible': s.get('compatible', False),
                    }
                    for s in redis_sugg[:5]
                ]

            step2a.append(entry)

        correction_sources = {}
        for c in corrections:
            correction_sources[c.get('position', -1)] = c.get('source', 'unknown')

        for entry in step2a:
            entry['correction_source'] = correction_sources.get(
                entry['position'], 'none'
            )

        trace['step2a_corrections'] = {
            'description': (
                'Clean + Resolve — fuzzy corrections BEFORE n-grams/POS '
                '(v4 key change)'
            ),
            'skipped': not has_unknowns,
            'correction_tiers': [
                'Tier 1: Fuzzy match against known sets (frozenset, ~0.001ms)',
                'Tier 2: Fuzzy match against RAM vocabulary cache (~1-10ms)',
                'Tier 3: Batch Redis fuzzy lookup (~30-50ms)',
            ],
            'words': step2a,
            'corrections': corrections,
            'summary': {
                'total_unknowns': sum(
                    1 for s in step1 if s.get('path') == 'NOT_FOUND'
                ),
                'corrected': sum(
                    1 for e in step2a if e['status'] == 'corrected'
                ),
                'suggested_only': sum(
                    1 for e in step2a if e['status'] == 'unknown_suggest'
                ),
                'still_unknown': sum(
                    1 for e in step2a if e['status'] == 'unknown'
                ),
                'from_tier1': sum(
                    1 for e in step2a
                    if e.get('correction_source') == 'tier1_known_sets'
                ),
                'from_tier2': sum(
                    1 for e in step2a
                    if e.get('correction_source') == 'tier2_ram_cache'
                ),
                'from_redis': sum(
                    1 for e in step2a
                    if e.get('correction_source') == 'redis'
                ),
            },
        }

        # ═════════════════════════════════════════════════════════════
        # STEP 2b: POS Prediction + Best Match Selection
        # ═════════════════════════════════════════════════════════════
        wd._step2_pos_and_select(word_data)

        step2b = []
        for i, wd_item in enumerate(word_data):
            entry = {
                'position': wd_item['position'],
                'word': wd_item['word'],
                'status': wd_item['status'],
                'is_stopword': wd_item['is_stopword'],
            }
            if not wd_item['is_stopword'] and wd_item['status'] != 'known':
                entry['predicted_pos'] = wd_item.get('predicted_pos', '')
                entry['predicted_pos_list'] = wd_item.get(
                    'predicted_pos_list', []
                )
                sm = wd_item.get('selected_match')
                if sm:
                    entry['selected_match'] = {
                        'term': sm.get('term', ''),
                        'category': sm.get('category', ''),
                        'pos': sm.get('pos', ''),
                        'rank': sm.get('rank', 0),
                    }
                    predicted = wd_item.get('predicted_pos', 'noun')
                    entry['all_matches_with_compatibility'] = [
                        {
                            'term': m['term'],
                            'category': m['category'],
                            'pos': m['pos'],
                            'rank': m['rank'],
                            'pos_compatible': is_pos_compatible(
                                normalize_pos_string(m['pos']), predicted
                            ),
                        }
                        for m in wd_item.get('all_matches', [])
                    ]
                else:
                    entry['selected_match'] = None

            step2b.append(entry)

        trace['step2b_pos_selection'] = {
            'description': (
                'POS Prediction + Best Match Selection '
                '(runs on corrected words)'
            ),
            'words': step2b,
        }

        # ═════════════════════════════════════════════════════════════
        # STEP 2c: "in" Disambiguation
        # ═════════════════════════════════════════════════════════════
        wd._step2_resolve_in_ambiguity(word_data)

        step2c = []
        for i, wd_item in enumerate(word_data):
            if not (wd_item['is_stopword'] and wd_item['word'] == 'in'):
                continue
            if i + 1 >= len(word_data):
                continue

            target = word_data[i + 1]
            all_m = target.get('all_matches', [])
            city_m = [
                m for m in all_m
                if m.get('category', '').lower() in CITY_CATEGORIES
            ]
            non_city_m = [
                m for m in all_m
                if m.get('category', '').lower() not in LOCATION_CATEGORIES
            ]

            left_flags = set()
            left_words = []
            for j in range(i - 1, -1, -1):
                lw = word_data[j]
                if lw['is_stopword'] and lw['word'] not in (
                    'the', 'a', 'an', 'for', 'and',
                ):
                    break
                if lw['is_stopword']:
                    continue
                flags = set(lw.get('context_flags', []))
                left_flags.update(flags)
                left_words.append({
                    'word': lw['word'],
                    'corrected': lw.get('corrected', None),
                    'flags': list(flags),
                    'status': lw['status'],
                })
                if len(left_words) >= 3:
                    break

            step2c.append({
                'target_word': target['word'],
                'target_position': target['position'],
                'city_matches': [
                    {
                        'term': m['term'],
                        'category': m['category'],
                        'rank': m['rank'],
                    }
                    for m in city_m
                ],
                'non_city_matches': [
                    {
                        'term': m['term'],
                        'category': m['category'],
                        'rank': m['rank'],
                    }
                    for m in non_city_m
                ],
                'left_context': left_words,
                'left_flags': sorted(left_flags),
                'has_location_intent': bool(left_flags & LOCATION_INTENT_FLAGS),
                'has_keyword_intent': bool(left_flags & KEYWORD_INTENT_FLAGS),
                'final_selected_category': target.get(
                    'selected_match', {}
                ).get('category', ''),
                'final_context_flags': list(
                    target.get('context_flags', [])
                ),
                'v4_note': (
                    'Left context flags are from CORRECTED words — '
                    'this is the v4 fix for the misspelling bug'
                ),
            })

        trace['step2c_in_disambiguation'] = {
            'description': (
                '"in" disambiguation — city vs keyword '
                '(runs on corrected words with correct flags)'
            ),
            'results': step2c,
        }

        # ═════════════════════════════════════════════════════════════
        # STEP 3: N-gram Resolution
        # ═════════════════════════════════════════════════════════════
        corrected_words = wd._get_working_words(word_data)
        ngrams, consumed = wd._step3_resolve_ngrams(corrected_words, word_data)

        trace['step3_ngrams'] = {
            'description': (
                'N-gram Resolution — single pass on corrected words '
                '(no re-check needed)'
            ),
            'corrected_words_used': corrected_words,
            'consumed_positions': sorted(consumed),
            'ngrams': [
                {
                    'type': ng['type'],
                    'phrase': ng['phrase'],
                    'category': ng['category'],
                    'rank': ng['rank'],
                    'positions': ng['positions'],
                    'location_boosted': ng.get('location_boosted', False),
                    'location_compound': ng.get('location_compound', False),
                }
                for ng in ngrams
            ],
            'v4_note': (
                'In v3 this ran BEFORE corrections on misspelled words, '
                'then had to re-check in Step 5.  v4 runs once on '
                'correct words.'
            ),
        }

        # ═════════════════════════════════════════════════════════════
        # STEP 4: Build Profile + Intent
        # ═════════════════════════════════════════════════════════════
        profile = wd._step4_build_profile(
            query, word_data, ngrams, consumed, corrections, start,
        )

        entity_routing = []
        ALWAYS_KEYWORD = (
            wd.SUPERLATIVES | wd.COLORS | wd.SIZES |
            wd.COMMON_ADJECTIVES | wd.APPAREL_PRODUCTS |
            wd.FOOD_DINING | wd.SERVICES
        )
        MODIFIER_POS = frozenset({
            'adjective', 'adverb', 'verb', 'determiner',
        })

        for wd_item in word_data:
            if wd_item['is_stopword'] or wd_item['position'] in consumed:
                continue
            sm = wd_item.get('selected_match')
            if not sm:
                continue
            cat = sm.get('category', '').lower()
            w = wd_item['word'].lower()
            pred_pos = normalize_pos_string(wd_item.get('predicted_pos'))
            match_pos = normalize_pos_string(sm.get('pos'))

            if cat in CITY_CATEGORIES:
                natural = 'cities'
            elif cat in STATE_CATEGORIES:
                natural = 'states'
            elif cat in LOCATION_CATEGORIES:
                natural = 'location_terms'
            elif cat in PERSON_CATEGORIES:
                natural = 'persons'
            elif cat in ORGANIZATION_CATEGORIES:
                natural = 'organizations'
            elif cat in MEDIA_CATEGORIES:
                natural = 'media'
            else:
                natural = 'keywords'

            demoted = False
            reason = None
            if w in ALWAYS_KEYWORD:
                demoted = True
                reason = f"'{w}' is in ALWAYS_KEYWORD set"
            elif (
                pred_pos in MODIFIER_POS
                and match_pos in ('noun', 'proper_noun')
                and not is_pos_compatible(match_pos, pred_pos)
            ):
                demoted = True
                reason = f"POS gate: predicted={pred_pos}, match={match_pos}"

            entity_routing.append({
                'word': w,
                'match_category': sm.get('category', ''),
                'match_rank': sm.get('rank', 0),
                'natural_destination': natural,
                'demoted': demoted,
                'demotion_reason': reason,
                'final_destination': 'keywords' if demoted else natural,
            })

        trace['step4_profile'] = {
            'description': 'Build Profile — entity routing + search terms',
            'corrected_query': profile.get('corrected_query', ''),
            'corrected_display_query': profile.get(
                'corrected_display_query', ''
            ),
            'search_terms': profile.get('search_terms', []),
            'entity_routing': entity_routing,
            'persons': profile.get('persons', []),
            'organizations': profile.get('organizations', []),
            'keywords': [
                {
                    'display': k.get('display', ''),
                    'category': k.get('category', ''),
                    'rank': k.get('rank', 0),
                }
                for k in profile.get('keywords', [])
            ],
            'cities': profile.get('cities', []),
            'states': profile.get('states', []),
            'location_terms': profile.get('location_terms', []),
            'primary_intent': profile.get('primary_intent', ''),
            'intent_scores': profile.get('intent_scores', {}),
            'field_boosts': profile.get('field_boosts', {}),
            'stats': profile.get('stats', {}),
        }

        # ── INTENT DETECTION ─────────────────────────────────────────
        if _INTENT_AVAILABLE:
            try:
                enriched = _detect_intent_local(profile)
                signals = enriched.get('signals', {})
                trace['intent_detection'] = {
                    'available': True,
                    'query_mode': signals.get('query_mode', ''),
                    'primary_domain': signals.get('primary_domain'),
                    'is_local_search': signals.get('is_local_search', False),
                    'has_food_word': signals.get('has_food_word', False),
                    'has_service_word': signals.get('has_service_word', False),
                    'has_black_owned': signals.get('has_black_owned', False),
                    'has_unknown_terms': signals.get('has_unknown_terms', False),
                    'has_location_entity': signals.get(
                        'has_location_entity', False
                    ),
                }
            except Exception as e:
                trace['intent_detection'] = {'error': str(e)}
        else:
            trace['intent_detection'] = {'available': False}

        # ── PROBLEMS DETECTED ─────────────────────────────────────────
        problems = []

        for wd_item in word_data:
            if wd_item['status'] in ('unknown', 'unknown_suggest'):
                problems.append({
                    'type': 'UNCORRECTED_WORD',
                    'word': wd_item['word'],
                    'position': wd_item['position'],
                    'suggestion': wd_item.get('suggestion', 'none'),
                })

            if wd_item['is_stopword'] or wd_item['position'] in consumed:
                continue

            sm = wd_item.get('selected_match')
            if not sm:
                continue

            all_m = wd_item.get('all_matches', [])
            has_city = any(
                m.get('category', '').lower() in CITY_CATEGORIES
                for m in all_m
            )
            routed = sm.get('category', '').lower()
            if has_city and routed not in LOCATION_CATEGORIES:
                problems.append({
                    'type': 'CITY_ROUTED_TO_KEYWORD',
                    'word': wd_item['word'],
                    'position': wd_item['position'],
                    'selected_category': sm.get('category', ''),
                    'city_match_available': True,
                })

        if not profile.get('cities') and not profile.get('states'):
            has_loc_signal = any(
                wd_item.get('location_context', False)
                for wd_item in word_data
            )
            if has_loc_signal:
                problems.append({
                    'type': 'LOCATION_SIGNAL_NO_CITY',
                    'detail': (
                        'Location signal detected but no cities/states '
                        'in profile'
                    ),
                })

        trace['problems_detected'] = problems
        trace['problem_count'] = len(problems)

        # ── WORD JOURNEY SUMMARY ──────────────────────────────────────
        journey = []
        for wd_item in word_data:
            j = {
                'position': wd_item['position'],
                'word': wd_item['word'],
                'final_status': wd_item['status'],
                'context_flags': list(wd_item.get('context_flags', [])),
            }
            if wd_item['status'] == 'corrected':
                j['corrected_to'] = wd_item.get('corrected', '')
                j['note'] = 'CORRECTED — used for search'
            elif wd_item['status'] == 'unknown_suggest':
                j['suggestion'] = wd_item.get('suggestion', '')
                j['note'] = 'ORIGINAL KEPT — suggestion not applied'

            sm = wd_item.get('selected_match')
            if sm:
                j['final_category'] = sm.get('category', '')
                j['final_rank'] = sm.get('rank', 0)

            journey.append(j)

        trace['word_journey'] = journey

        # ── v3 vs v4 COMPARISON NOTE ──────────────────────────────────
        trace['v4_pipeline_notes'] = {
            'key_changes': [
                'Corrections run BEFORE n-grams (was after in v3)',
                'Corrections run BEFORE POS prediction (was after in v3)',
                'Corrections run BEFORE "in" disambiguation (was after in v3)',
                '"in" disambiguation sees CORRECT context flags (was misspelled in v3)',
                'N-grams run once on corrected words (was twice in v3: before + after correction)',
                'Step 5 re-check eliminated (not needed when corrections come first)',
                'Pipeline reduced from 6+ steps to 4 steps',
            ],
            'bug_fixed': (
                'v3 bug: "restuarant in atl" → Step 3.5 saw misspelled '
                '"restuarant" with no known_food flag → demoted Atlanta '
                'to keyword. v4 fix: "restuarant" corrected to "restaurant" '
                'with known_food flag BEFORE "in" disambiguation → Atlanta '
                'stays as city.'
            ),
        }

        elapsed = (time.perf_counter() - start) * 1000
        trace['total_ms'] = round(elapsed, 2)

        return trace

    result = await asyncio.to_thread(run_trace, query)

    return JsonResponse(result, safe=False)