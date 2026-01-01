
# from django.views.generic import TemplateView
# from django.conf import settings
# import requests
# from django.http import JsonResponse
# from django.views.decorators.http import require_GET
# from django.views import View
# from django.shortcuts import render, redirect
# from django.http import HttpResponse
# from django.urls import reverse
# import logging
# import json
# from django.utils.decorators import method_decorator
# from django.template.loader import render_to_string
# from django.template.exceptions import TemplateDoesNotExist
# from django.db import connection
# from django.http import JsonResponse
# from django.views import View
# from .searchapi import get_autocomplete
# from decouple import config

# def home(request):
#     return render(request, 'home2.html')



# # #########################   this is the code for the redis api. 


# # def search_suggestions(request):
# #     query = request.GET.get('q', '').strip()
    
# #     if not query or len(query) < 2:
# #         return JsonResponse({'suggestions': []})
    
# #     # Get autocomplete results from Redis
# #     results = get_autocomplete(prefix=query, limit=8)
    
# #     # Transform to match frontend expected format
# #     suggestions = []
# #     for item in results:
# #         suggestions.append({
# #             'text': item['term'],
# #             'display_text': item['display'],
# #             'source_field': item.get('entity_type', ''),
# #             # 'data_type': item.get('category', ''),
# #             'category': item.get('category', ''),  # Add this line
# #         })
    
# #     return JsonResponse({'suggestions': suggestions})

# def search_suggestions(request):
#     query = request.GET.get('q', '').strip()
    
#     if not query or len(query) < 2:
#         return JsonResponse({'suggestions': []})
    
#     results = get_autocomplete(prefix=query, limit=8)
    
#     # Only send display and description
#     suggestions = []
#     for item in results:
#         suggestions.append({
#             'text': item['term'],
#             'display': item['display'],
#             'description': item.get('description', ''),
#         })
    
#     return JsonResponse({'suggestions': suggestions})


# #########################   this is the code for the submission through the search bar. 

# from .searchsubmission import process_search_submission


# def form_submit(request):
#     query = request.GET.get('query', '')
#     session_id = request.GET.get('session_id', '')
    
#     # Process the submission
#     result = process_search_submission(query, session_id)
    
#     return JsonResponse(result)

# from django.http import JsonResponse
# from .searchsubmission import process_search_submission
# from .word_discovery import word_discovery_multi
# import uuid
# from django.shortcuts import render

# from django.http import JsonResponse
# from django.shortcuts import render
# from django.core.cache import cache
# from .word_discovery import word_discovery_multi
# from .typesense_calculations import (
#     execute_full_search,
#     detect_query_intent,
#     get_facets,
#     get_related_searches,
#     get_featured_result,
#     log_search_event
# )
# import uuid


# def search(request):
#     """
#     Production-quality search endpoint.
#     Handles: correction, intent, filters, pagination, facets, features, logging
#     """
    
#     # === 1. EXTRACT ALL PARAMETERS ===
#     query = request.GET.get('query', '').strip()
#     session_id = request.GET.get('session_id', '') or str(uuid.uuid4())
#     page = int(request.GET.get('page', 1))
#     per_page = int(request.GET.get('per_page', 20))
    
#     # Explicit filters from URL
#     filters = {
#         'category': request.GET.get('category'),
#         'source': request.GET.get('source'),
#         'data_type': request.GET.get('type'),
#         'time_range': request.GET.get('time'),          # 'day', 'week', 'month', 'year'
#         'location': request.GET.get('location'),
#         'sort': request.GET.get('sort', 'relevance'),   # 'relevance', 'date', 'rating'
#     }
#     # Remove None values
#     filters = {k: v for k, v in filters.items() if v}
    
#     # Safe search setting
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     # User location (from browser or IP)
#     user_lat = request.GET.get('lat')
#     user_lng = request.GET.get('lng')
#     user_location = (float(user_lat), float(user_lng)) if user_lat and user_lng else None
    
#     # === 2. EMPTY QUERY - SHOW HOMEPAGE ===
#     if not query:
#         return render(request, 'results.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': session_id,
#             'show_trending': True,  # Show trending searches on empty query
#         })
    
#     # === 3. CHECK CACHE FOR REPEATED QUERIES ===
#     cache_key = f"search:{query}:{page}:{hash(frozenset(filters.items()))}"
#     cached_result = cache.get(cache_key)
    
#     if cached_result and not filters:  # Only use cache for unfiltered searches
#         cached_result['from_cache'] = True
#         return render(request, 'results.html', cached_result)
    
#     # === 4. WORD DISCOVERY / SPELL CORRECTION ===
#     corrections, tuple_array, corrected_query = word_discovery_multi(query)
#     was_corrected = query.lower() != corrected_query.lower()
    
#     # Build word-by-word correction display
#     word_corrections = build_word_corrections(query, corrected_query)
    
#     # === 5. DETECT INTENT ===
#     intent = detect_query_intent(corrected_query, tuple_array)
    
#     # === 6. EXECUTE MAIN SEARCH ===
#     result = execute_full_search(
#         query=corrected_query,
#         session_id=session_id,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         pos_tags=tuple_array,
#         safe_search=safe_search
#     )
    
#     results = result.get('results', [])
#     total_results = result.get('total', 0)
    
#     # === 7. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(
#             original_query=query,
#             corrected_query=corrected_query,
#             filters=filters
#         )
    
#     # === 8. GET FACETS (Available Filters) ===
#     facets = get_facets(corrected_query) if results else {}
    
#     # === 9. GET RELATED SEARCHES ===
#     related_searches = get_related_searches(corrected_query, intent) if results else []
    
#     # === 10. GET FEATURED RESULT (Knowledge Panel / Direct Answer) ===
#     featured = None
#     if page == 1:  # Only on first page
#         featured = get_featured_result(corrected_query, intent, results)
    
#     # === 11. CATEGORIZE RESULTS BY TYPE ===
#     categorized_results = categorize_results(results)
    
#     # === 12. BUILD PAGINATION ===
#     pagination = build_pagination(page, per_page, total_results)
    
#     # === 13. LOG SEARCH EVENT ===
#     log_search_event(
#         query=query,
#         corrected_query=corrected_query,
#         session_id=session_id,
#         intent=intent,
#         total_results=total_results,
#         filters=filters,
#         page=page
#     )
    
#     # === 14. BUILD CONTEXT ===
#     context = {
#         # Query info
#         'query': query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
        
#         # Results
#         'results': results,
#         'categorized_results': categorized_results,
#         'total_results': total_results,
#         'has_results': len(results) > 0,
        
#         # Featured content
#         'featured': featured,
#         'related_searches': related_searches,
        
#         # Filters & Facets
#         'filters': filters,
#         'facets': facets,
#         'safe_search': safe_search,
        
#         # Pagination
#         'pagination': pagination,
#         'page': page,
#         'per_page': per_page,
        
#         # Zero results
#         'suggestions': suggestions,
        
#         # Meta
#         'session_id': session_id,
#         'search_time': result.get('search_time', 0),
#         'from_cache': False,
#     }
    
#     # === 15. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         cache.set(cache_key, context, timeout=300)  # 5 minutes
    
#     return render(request, 'results.html', context)


# # === HELPER FUNCTIONS ===

# def build_word_corrections(original: str, corrected: str) -> list:
#     """Builds word-by-word correction display"""
#     word_corrections = []
#     original_words = original.lower().split()
#     corrected_words = corrected.lower().split()
    
#     for i, orig_word in enumerate(original_words):
#         if i < len(corrected_words):
#             corr_word = corrected_words[i]
#             word_corrections.append({
#                 'original': orig_word,
#                 'corrected': corr_word,
#                 'was_changed': orig_word != corr_word
#             })
#         else:
#             word_corrections.append({
#                 'original': orig_word,
#                 'corrected': orig_word,
#                 'was_changed': False
#             })
    
#     return word_corrections


# def handle_zero_results(original_query: str, corrected_query: str, filters: dict) -> list:
#     """
#     Provides helpful suggestions when no results found.
#     Returns list of suggestion objects.
#     """
#     suggestions = []
    
#     # Suggestion 1: Try without filters
#     if filters:
#         suggestions.append({
#             'type': 'remove_filters',
#             'message': 'Try removing some filters',
#             'action_query': corrected_query,
#             'action_filters': {}
#         })
    
#     # Suggestion 2: Try broader terms
#     words = corrected_query.split()
#     if len(words) > 2:
#         shorter_query = ' '.join(words[:2])
#         suggestions.append({
#             'type': 'broader_search',
#             'message': f'Try a broader search',
#             'action_query': shorter_query
#         })
    
#     # Suggestion 3: Check spelling (if already corrected, suggest original)
#     if original_query.lower() != corrected_query.lower():
#         suggestions.append({
#             'type': 'try_original',
#             'message': f'Search for "{original_query}" instead',
#             'action_query': original_query
#         })
    
#     # Suggestion 4: Related topics (would come from your keyword database)
#     suggestions.append({
#         'type': 'help',
#         'message': 'Check your spelling or try different keywords'
#     })
    
#     return suggestions


# def categorize_results(results: list) -> dict:
#     """
#     Groups results by type for different display treatments.
#     """
#     categorized = {
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
#         'video': 'videos',
#         'product': 'products',
#         'person': 'people',
#         'place': 'places',
#         'service': 'services'
#     }
    
#     for result in results:
#         data_type = result.get('data_type', 'other')
#         category = type_mapping.get(data_type, 'other')
#         categorized[category].append(result)
    
#     # Remove empty categories
#     return {k: v for k, v in categorized.items() if v}


# def build_pagination(page: int, per_page: int, total: int) -> dict:
#     """
#     Builds pagination info for template.
#     """
#     total_pages = (total + per_page - 1) // per_page
    
#     # Build page range (show 5 pages around current)
#     start_page = max(1, page - 2)
#     end_page = min(total_pages, page + 2)
    
#     return {
#         'current_page': page,
#         'total_pages': total_pages,
#         'has_previous': page > 1,
#         'has_next': page < total_pages,
#         'previous_page': page - 1,
#         'next_page': page + 1,
#         'page_range': list(range(start_page, end_page + 1)),
#         'show_first': start_page > 1,
#         'show_last': end_page < total_pages,
#         'start_result': (page - 1) * per_page + 1,
#         'end_result': min(page * per_page, total),
#         'total_results': total
#     }

# # def search(request):
# #     """Main search endpoint - renders HTML results like Google"""
# #     query = request.GET.get('query', '').strip()
# #     session_id = request.GET.get('session_id', '')
    
# #     # Generate session_id if not provided
# #     if not session_id:
# #         session_id = str(uuid.uuid4())
    
# #     # Empty query - show search homepage
# #     if not query:
# #         return render(request, 'results.html', {
# #             'query': '',
# #             'results': [],
# #             'has_results': False,
# #             'session_id': session_id,
# #         })
    
# #     # Run the three-pass word discovery pipeline
# #     corrections, tuple_array, corrected_query = word_discovery_multi(query)
    
# #     # Process the submission with the corrected query
# #     result = process_search_submission(corrected_query, session_id)
    
# #     # Build correction info for template
# #     was_corrected = query.lower() != corrected_query.lower()
    
# #     # Build word-by-word correction display
# #     word_corrections = []
# #     original_words = query.lower().split()
# #     corrected_words = corrected_query.lower().split()
    
# #     for i, orig_word in enumerate(original_words):
# #         if i < len(corrected_words):
# #             corr_word = corrected_words[i]
# #             word_corrections.append({
# #                 'original': orig_word,
# #                 'corrected': corr_word,
# #                 'was_changed': orig_word != corr_word
# #             })
# #         else:
# #             word_corrections.append({
# #                 'original': orig_word,
# #                 'corrected': orig_word,
# #                 'was_changed': False
# #             })
    
# #     context = {
# #         'query': query,
# #         'corrected_query': corrected_query,
# #         'was_corrected': was_corrected,
# #         'word_corrections': word_corrections,
# #         'corrections': corrections,
# #         'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
# #         'results': result.get('results', []),
# #         'total_results': result.get('total', 0),
# #         'has_results': len(result.get('results', [])) > 0,
# #         'session_id': session_id,
# #         'search_time': result.get('search_time', 0),
# #     }
    
# #     return render(request, 'results.html', context)


# def search_api(request):
#     """JSON API endpoint for programmatic access"""
#     query = request.GET.get('q', '') or request.GET.get('query', '')
#     session_id = request.GET.get('session_id', '')
    
#     if not query:
#         return JsonResponse({'error': 'No query provided'}, status=400)
    
#     # Run the three-pass word discovery pipeline
#     corrections, tuple_array, corrected_query = word_discovery_multi(query)
    
#     # Process the submission with the corrected query
#     result = process_search_submission(corrected_query, session_id)
    
#     # Add word discovery info to the result
#     result['word_discovery'] = {
#         'original_query': query,
#         'corrected_query': corrected_query,
#         'corrections': corrections,
#         'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
#         'was_corrected': query.lower() != corrected_query.lower()
#     }
    
#     return JsonResponse(result)


# # def form_submit(request):
# #     query = request.GET.get('query', '')
# #     session_id = request.GET.get('session_id', '')
    
# #     if not query:
# #         return JsonResponse({'error': 'No query provided'}, status=400)
    
# #     # Run the three-pass word discovery pipeline
# #     corrections, tuple_array, corrected_query = word_discovery_multi(query)
    
# #     # Process the submission with the corrected query
# #     result = process_search_submission(corrected_query, session_id)
    
# #     # Add word discovery info to the result
# #     result['word_discovery'] = {
# #         'original_query': query,
# #         'corrected_query': corrected_query,
# #         'corrections': corrections,
# #         'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
# #         'was_corrected': query.lower() != corrected_query.lower()
# #     }
    
# #     return JsonResponse(result)

from django.views.generic import TemplateView
from django.conf import settings
import requests
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.views import View
from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.urls import reverse
import logging
import json
import time
import uuid
from django.utils.decorators import method_decorator
from django.template.loader import render_to_string
from django.template.exceptions import TemplateDoesNotExist
from django.db import connection
from django.http import JsonResponse
from django.views import View
from .searchapi import get_autocomplete
from decouple import config
from django.core.cache import cache
import typesense
import redis
from decouple import config
from django.shortcuts import render, get_object_or_404
from django.http import Http404



redis_client = redis.Redis(
    host=config('REDIS_HOST', default='localhost'),
    port=config('REDIS_PORT', default=6379, cast=int),
    db=config('REDIS_DB', default=0, cast=int),
    password=config('REDIS_PASSWORD', default=None),
    decode_responses=True,
)
logger = logging.getLogger(__name__)


typesense_client= typesense.Client({
    'api_key': config('TYPESENSE_API_KEY'),
    'nodes': [{
        'host': config('TYPESENSE_HOST'),
        'port': config('TYPESENSE_PORT'),
        'protocol': config('TYPESENSE_PROTOCOL')
    }],
    'connection_timeout_seconds': 5
})

COLLECTION_NAME = 'documents'




# =============================================================================
# CONFIGURATION
# =============================================================================

SEARCH_CONFIG = {
    'max_timestamp_age_seconds': 300,  # 5 minutes - reject older requests
    'rate_limit_per_minute': 30,       # Max requests per session per minute
    'min_typing_time_ms': 50,          # Bot detection - too fast is suspicious
    'max_query_length': 500,           # Prevent oversized queries
    'nonce_expiry_seconds': 60,        # Nonce can only be used once within this window
}


# =============================================================================
# SECURITY VALIDATION
# =============================================================================

class SearchSecurityValidator:
    """Validates security parameters from search requests"""
    
    @staticmethod
    def validate_timestamp(timestamp_str):
        """
        Validate timestamp is recent (not replay attack)
        Returns: (is_valid, error_message)
        """
        if not timestamp_str:
            return True, None  # Optional - don't block if missing
        
        try:
            timestamp = int(timestamp_str)
            current_time = int(time.time() * 1000)  # Current time in ms
            age_seconds = (current_time - timestamp) / 1000
            
            if age_seconds < -60:  # Allow 60s clock skew
                return False, "Timestamp is in the future"
            
            if age_seconds > SEARCH_CONFIG['max_timestamp_age_seconds']:
                return False, "Request too old"
            
            return True, None
            
        except (ValueError, TypeError):
            return True, None  # Don't block on invalid format
    
    @staticmethod
    def validate_nonce(nonce, session_id):
        """
        Validate nonce hasn't been used before (prevent replay)
        Returns: (is_valid, error_message)
        """
        if not nonce or not session_id:
            return True, None  # Optional
        
        if len(nonce) < 8:
            return False, "Invalid nonce"
        
        # Check if nonce was already used
        cache_key = f"nonce:{session_id}:{nonce}"
        if cache.get(cache_key):
            return False, "Nonce already used"
        
        # Mark nonce as used
        cache.set(cache_key, True, SEARCH_CONFIG['nonce_expiry_seconds'])
        return True, None
    
    @staticmethod
    def validate_session(session_id):
        """
        Validate session ID format
        Returns: (is_valid, error_message)
        """
        if not session_id:
            return True, None  # Optional - will generate one
        
        # Basic UUID format check (loose)
        if len(session_id) < 20 or len(session_id) > 50:
            return False, "Invalid session ID format"
        
        return True, None
    
    @staticmethod
    def check_rate_limit(session_id, client_fp):
        """
        Check if request is within rate limits
        Returns: (is_allowed, error_message)
        """
        if not session_id:
            return True, None
        
        # Use both session_id and fingerprint for rate limiting
        rate_key = f"rate:{session_id}:{client_fp or 'unknown'}"
        
        current_count = cache.get(rate_key, 0)
        
        if current_count >= SEARCH_CONFIG['rate_limit_per_minute']:
            return False, "Rate limit exceeded"
        
        # Increment counter with 60 second expiry
        cache.set(rate_key, current_count + 1, 60)
        return True, None
    
    @staticmethod
    def detect_bot(typing_time_ms, request_sequence):
        """
        Simple bot detection heuristics
        Returns: (is_suspicious, reason)
        """
        try:
            typing_time = int(typing_time_ms) if typing_time_ms else 0
            req_seq = int(request_sequence) if request_sequence else 0
            
            # Too fast typing is suspicious (but not for dropdown selections)
            if typing_time > 0 and typing_time < SEARCH_CONFIG['min_typing_time_ms']:
                return True, "Typing too fast"
            
            # Unusual request patterns
            if req_seq > 100:  # Too many requests in one session
                return True, "Excessive requests"
            
            return False, None
            
        except (ValueError, TypeError):
            return False, None


# =============================================================================
# SEARCH PARAMETER EXTRACTION
# =============================================================================

class SearchParams:
    """Extract and hold all search parameters from request"""
    
    def __init__(self, request):
        self.request = request
        
        # Core search
        self.query = request.GET.get('query', '').strip()[:SEARCH_CONFIG['max_query_length']]
        self.alt_mode = request.GET.get('alt_mode', 'y')  # y=semantic, n=keyword
        
        # Security
        self.session_id = request.GET.get('session_id', '') or str(uuid.uuid4())
        self.request_id = request.GET.get('request_id', '')
        self.timestamp = request.GET.get('timestamp', '')
        self.nonce = request.GET.get('nonce', '')
        
        # Analytics
        self.source = request.GET.get('source', 'unknown')
        self.device_type = request.GET.get('device_type', 'unknown')
        self.result_count = request.GET.get('result_count', '0')
        self.typing_time_ms = request.GET.get('typing_time_ms', '0')
        
        # Rate limiting
        self.client_fp = request.GET.get('client_fp', '')
        self.request_sequence = request.GET.get('req_seq', '0')
    
    @property
    def is_keyword_search(self):
        """True if search came from dropdown (keyword search)"""
        return self.alt_mode == 'n'
    
    @property
    def is_semantic_search(self):
        """True if user typed freely (semantic search)"""
        return self.alt_mode == 'y'
    
    def to_dict(self):
        """Convert to dictionary for logging"""
        return {
            'query': self.query,
            'alt_mode': self.alt_mode,
            'session_id': self.session_id,
            'request_id': self.request_id,
            'source': self.source,
            'device_type': self.device_type,
            'result_count': self.result_count,
            'typing_time_ms': self.typing_time_ms,
            'client_fp': self.client_fp,
        }


# =============================================================================
# ANALYTICS LOGGING
# =============================================================================

def log_search_analytics(params, search_type, result_count, is_suspicious=False):
    """
    Log search for analytics and monitoring
    """
    try:
        logger.info(
            f"Search: query='{params.query}' type={search_type} "
            f"alt_mode={params.alt_mode} results={result_count} "
            f"session={params.session_id[:8] if params.session_id else 'none'}... "
            f"device={params.device_type} source={params.source} "
            f"typing_ms={params.typing_time_ms} suspicious={is_suspicious}"
        )
        
        # Optional: Store in Redis for real-time analytics
        # analytics_data = {
        #     **params.to_dict(),
        #     'search_type': search_type,
        #     'result_count': result_count,
        #     'is_suspicious': is_suspicious,
        #     'timestamp': time.time()
        # }
        # cache.lpush('search_analytics', json.dumps(analytics_data))
        
    except Exception as e:
        logger.error(f"Analytics logging error: {e}")


# =============================================================================
# VIEWS
# =============================================================================

def home(request):
    return render(request, 'home3.html')


def search_suggestions(request):
    """API endpoint for autocomplete dropdown suggestions"""
    query = request.GET.get('q', '').strip()
    
    if not query or len(query) < 2:
        return JsonResponse({'suggestions': []})
    
    results = get_autocomplete(prefix=query, limit=8)
    
    # Only send display and description
    suggestions = []
    for item in results:
        suggestions.append({
            'text': item['term'],
            'display': item['display'],
            'description': item.get('description', ''),
        })
    
    return JsonResponse({'suggestions': suggestions})


# =============================================================================
# MAIN SEARCH VIEW - UPDATED WITH SECURITY & ROUTING
# =============================================================================

from .searchsubmission import process_search_submission
from .word_discovery import word_discovery_multi
from .typesense_calculations import (
    execute_full_search,
    detect_query_intent,
    get_facets,
    get_related_searches,
    get_featured_result,
    log_search_event
)


def search(request):
    """
    Production-quality search endpoint with security validation.
    Routes to keyword or semantic search based on alt_mode.
    
    alt_mode=n (from dropdown) -> Keyword search (skip spell correction)
    alt_mode=y (user typed) -> Semantic search (full pipeline)
    """
    
    # === 1. EXTRACT ALL PARAMETERS (including security) ===
    params = SearchParams(request)
    
    # Legacy parameter extraction (keep existing functionality)
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 20))
    
    # Explicit filters from URL
    # Note: 'source' might contain analytics value 'home' - ignore that
    source_filter = request.GET.get('source')
    if source_filter in ('home', 'results_page', 'header', None, ''):
        source_filter = None  # Not a real filter, just analytics
    
    filters = {
        'category': request.GET.get('category'),
        'source': source_filter,
        'data_type': request.GET.get('type'),
        'time_range': request.GET.get('time'),
        'location': request.GET.get('location'),
        'sort': request.GET.get('sort', 'relevance'),
    }
    filters = {k: v for k, v in filters.items() if v}
    
    safe_search = request.GET.get('safe', 'on') == 'on'
    
    user_lat = request.GET.get('lat')
    user_lng = request.GET.get('lng')
    user_location = (float(user_lat), float(user_lng)) if user_lat and user_lng else None
    
    # === 2. SECURITY VALIDATION ===
    validator = SearchSecurityValidator()
    is_suspicious = False
    
    # Validate timestamp
    is_valid, error = validator.validate_timestamp(params.timestamp)
    if not is_valid:
        logger.warning(f"Timestamp validation failed: {error} - {params.session_id}")
    
    # Check rate limit
    is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
    if not is_allowed:
        logger.warning(f"Rate limit exceeded: {params.session_id}")
        return render(request, 'results2.html', {
            'query': params.query,
            'results': [],
            'has_results': False,
            'error': 'Too many requests. Please wait a moment.',
            'session_id': params.session_id,
        })
    
    # Bot detection (log only, don't block)
    is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
    if is_suspicious:
        logger.info(f"Suspicious request: {reason} - {params.session_id}")
    
    # === 3. EMPTY QUERY - SHOW HOMEPAGE ===
    if not params.query:
        return render(request, 'results2.html', {
            'query': '',
            'results': [],
            'has_results': False,
            'session_id': params.session_id,
            'show_trending': True,
        })
    
    # === 4. CHECK CACHE ===
    cache_key = f"search:{params.query}:{page}:{params.alt_mode}:{hash(frozenset(filters.items()))}"
    cached_result = cache.get(cache_key)
    
    if cached_result and not filters:
        cached_result['from_cache'] = True
        return render(request, 'results2.html', cached_result)
    
    # === 5. ROUTE BASED ON ALT_MODE ===
    # DEBUG LOGGING - remove in production
    logger.info(f"=== SEARCH DEBUG ===")
    logger.info(f"Raw query from URL: '{params.query}'")
    logger.info(f"alt_mode: '{params.alt_mode}'")
    logger.info(f"is_keyword_search: {params.is_keyword_search}")
    
    if params.is_keyword_search:
        # =====================
        # KEYWORD SEARCH (from dropdown)
        # Skip spell correction - user selected exact term
        # =====================
        search_type = 'keyword'
        corrected_query = params.query  # No correction needed
        was_corrected = False
        word_corrections = []
        corrections = {}
        tuple_array = []
        
        logger.info(f"KEYWORD SEARCH - using query directly: '{corrected_query}'")
        
        # Detect intent (optional for keyword search)
        intent = detect_query_intent(corrected_query, tuple_array)
        
    else:
        # =====================
        # SEMANTIC SEARCH (user typed freely)
        # Full pipeline with spell correction
        # =====================
        search_type = 'semantic'
        
        logger.info(f"SEMANTIC SEARCH - running word_discovery_multi on: '{params.query}'")
        
        # Word discovery / spell correction
        corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
        was_corrected = params.query.lower() != corrected_query.lower()
        word_corrections = build_word_corrections(params.query, corrected_query)
        
        logger.info(f"After word_discovery: corrected_query='{corrected_query}', was_corrected={was_corrected}")
        
        # Detect intent
        intent = detect_query_intent(corrected_query, tuple_array)
    
    logger.info(f"Sending to execute_full_search: query='{corrected_query}'")
    
    # === 6. EXECUTE SEARCH ===
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
    
    # === 7. ZERO RESULTS HANDLING ===
    suggestions = []
    if not results:
        suggestions = handle_zero_results(
            original_query=params.query,
            corrected_query=corrected_query,
            filters=filters
        )
    
    # === 8. GET FACETS ===
    facets = get_facets(corrected_query) if results else {}
    
    # === 9. GET RELATED SEARCHES ===
    related_searches = get_related_searches(corrected_query, intent) if results else []
    
    # === 10. GET FEATURED RESULT ===
    featured = None
    if page == 1:
        featured = get_featured_result(corrected_query, intent, results)
    
    # === 11. CATEGORIZE RESULTS ===
    categorized_results = categorize_results(results)
    
    # === 12. BUILD PAGINATION ===
    pagination = build_pagination(page, per_page, total_results)
    
    # === 13. LOG SEARCH EVENT (existing) ===
    log_search_event(
        query=params.query,
        corrected_query=corrected_query,
        session_id=params.session_id,
        intent=intent,
        total_results=total_results,
        filters=filters,
        page=page
    )
    
    # === 14. LOG ANALYTICS (new - with security params) ===
    log_search_analytics(params, search_type, total_results, is_suspicious)
    
    # === 15. BUILD CONTEXT ===
    context = {
        # Query info
        'query': params.query,
        'corrected_query': corrected_query,
        'was_corrected': was_corrected,
        'word_corrections': word_corrections,
        'corrections': corrections,
        'intent': intent,
        
        # Search type (NEW)
        'search_type': search_type,  # 'keyword' or 'semantic'
        'alt_mode': params.alt_mode,
        
        # Results
        'results': results,
        'categorized_results': categorized_results,
        'total_results': total_results,
        'has_results': len(results) > 0,
        
        # Featured content
        'featured': featured,
        'related_searches': related_searches,
        
        # Filters & Facets
        'filters': filters,
        'facets': facets,
        'safe_search': safe_search,
        
        # Pagination
        'pagination': pagination,
        'page': page,
        'per_page': per_page,
        
        # Zero results
        'suggestions': suggestions,
        
        # Meta
        'session_id': params.session_id,
        'request_id': params.request_id,
        'search_time': result.get('search_time', 0),
        'from_cache': False,
        
        # Device info (NEW)
        'device_type': params.device_type,
        'source': params.source,
    }
    
    # === 16. CACHE RESULTS ===
    if not filters and total_results > 0:
        cache.set(cache_key, context, timeout=300)
    
    return render(request, 'results2.html', context)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_word_corrections(original: str, corrected: str) -> list:
    """Builds word-by-word correction display"""
    word_corrections = []
    original_words = original.lower().split()
    corrected_words = corrected.lower().split()
    
    for i, orig_word in enumerate(original_words):
        if i < len(corrected_words):
            corr_word = corrected_words[i]
            word_corrections.append({
                'original': orig_word,
                'corrected': corr_word,
                'was_changed': orig_word != corr_word
            })
        else:
            word_corrections.append({
                'original': orig_word,
                'corrected': orig_word,
                'was_changed': False
            })
    
    return word_corrections


def handle_zero_results(original_query: str, corrected_query: str, filters: dict) -> list:
    """
    Provides helpful suggestions when no results found.
    """
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
        shorter_query = ' '.join(words[:2])
        suggestions.append({
            'type': 'broader_search',
            'message': f'Try a broader search',
            'action_query': shorter_query
        })
    
    if original_query.lower() != corrected_query.lower():
        suggestions.append({
            'type': 'try_original',
            'message': f'Search for "{original_query}" instead',
            'action_query': original_query
        })
    
    suggestions.append({
        'type': 'help',
        'message': 'Check your spelling or try different keywords'
    })
    
    return suggestions


def categorize_results(results: list) -> dict:
    """Groups results by type for different display treatments."""
    categorized = {
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


def build_pagination(page: int, per_page: int, total: int) -> dict:
    """Builds pagination info for template."""
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    
    start_page = max(1, page - 2)
    end_page = min(total_pages, page + 2)
    
    return {
        'current_page': page,
        'total_pages': total_pages,
        'has_previous': page > 1,
        'has_next': page < total_pages,
        'previous_page': page - 1,
        'next_page': page + 1,
        'page_range': list(range(start_page, end_page + 1)),
        'show_first': start_page > 1,
        'show_last': end_page < total_pages,
        'start_result': (page - 1) * per_page + 1,
        'end_result': min(page * per_page, total),
        'total_results': total
    }


# =============================================================================
# OTHER EXISTING VIEWS
# =============================================================================

def form_submit(request):
    """Legacy form submit endpoint"""
    query = request.GET.get('query', '')
    session_id = request.GET.get('session_id', '')
    
    result = process_search_submission(query, session_id)
    
    return JsonResponse(result)


def search_api(request):
    """JSON API endpoint for programmatic access"""
    query = request.GET.get('q', '') or request.GET.get('query', '')
    session_id = request.GET.get('session_id', '')
    
    if not query:
        return JsonResponse({'error': 'No query provided'}, status=400)
    
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


# views.py

def category_view(request, category_slug):
    """Route to category-specific templates with appropriate context."""
    
    city = get_user_city(request)  # Your location detection function
    subcategory = request.GET.get('subcategory')
    
    # Route to appropriate template and build context
    if category_slug == 'business':
        return business_category(request, city, subcategory)
    elif category_slug == 'culture':
        return culture_category(request, city)
    elif category_slug == 'health':
        return health_category(request, city)
    elif category_slug == 'news':
        return news_category(request, city)
    elif category_slug == 'community':
        return community_category(request, city)
    else:
        raise Http404("Category not found")


# views.py

def business_category(request, city, subcategory=None):
    """Business directory page."""
    
    query = request.GET.get('q', '*')
    
    # Simple search without filters
    try:
        results = typesense_client.collections[COLLECTION_NAME].documents.search({
            'q': query if query else '*',
            'query_by': 'name,description',  # Adjust to your actual fields
            'per_page': 20,
            'page': int(request.GET.get('page', 1))
        })
    except Exception as e:
        print(f"Typesense error: {e}")
        results = {'hits': [], 'found': 0}
    
    context = {
        'city': city,
        'subcategory': subcategory,
        'listings': results.get('hits', []),
        'total': results.get('found', 0),
        'featured': [],
        'trending': [],
        'stats': {},
    }
    
    return render(request, 'category_business.html', context)


def culture_category(request, city):
    """Culture & heritage page."""
    from datetime import date
    
    today = date.today()
    
    # Get today's history event (from your database or API)
    history_event = get_history_event(today.month, today.day)
    
    # Get featured article
    featured_article = get_featured_article('culture')
    
    # Get HBCUs (could be static or from DB)
    hbcus = get_hbcu_list(limit=4)
    
    # Get trending music/culture content
    trending_music = get_trending_content('culture', 'music', limit=4)
    
    # Get upcoming cultural events
    events = get_upcoming_events('culture', city, limit=4)
    
    # Get book recommendations
    books = get_book_recommendations(limit=8)
    
    context = {
        'city': city,
        'today': {'day': today.day, 'month': today.strftime('%b').upper()},
        'history_event': history_event,
        'featured_article': featured_article,
        'hbcus': hbcus,
        'trending_music': trending_music,
        'events': events,
        'books': books,
    }
    
    return render(request, 'category_culture.html', context)


def health_category(request, city):
    """Health providers directory page."""
    
    specialty = request.GET.get('specialty')
    
    try:
        results = typesense_client.collections[COLLECTION_NAME].documents.search({
            'q': request.GET.get('q', '*'),
            'query_by': 'name,description',
            'per_page': 20,
            'page': int(request.GET.get('page', 1))
        })
        providers = results.get('hits', [])
        total = results.get('found', 0)
    except Exception as e:
        print(f"Typesense error: {e}")
        providers = []
        total = 0
    
    context = {
        'city': city,
        'specialty': specialty,
        'providers': providers,
        'total': total,
        'stats': {},
    }
    
    return render(request, 'category_health.html', context)


def news_category(request, city):
    """News aggregation page."""
    
    section = request.GET.get('section', 'top')
    
    # Get news articles (from your news aggregation source)
    top_story = get_top_story()
    sidebar_stories = get_news_articles(limit=3, exclude=top_story['id'] if top_story else None)
    top_articles = get_news_articles(section=section, limit=5)
    opinions = get_opinion_articles(limit=3)
    local_news = get_local_news(city, limit=4)
    good_news = get_good_news(limit=4)
    
    context = {
        'city': city,
        'section': section,
        'top_story': top_story,
        'sidebar_stories': sidebar_stories,
        'top_articles': top_articles,
        'opinions': opinions,
        'local_news': local_news,
        'good_news': good_news,
    }
    
    return render(request, 'category_news.html', context)


def community_category(request, city):
    """Community hub page."""
    
    # Get upcoming events
    events = get_upcoming_events('community', city, limit=5)
    
    # Get local organizations
    organizations = get_organizations(city, limit=4)
    
    # Get volunteer opportunities
    volunteer_ops = get_volunteer_opportunities(city, limit=4)
    
    # Get churches/faith communities
    churches = get_churches(city, limit=6)
    
    context = {
        'city': city,
        'events': events,
        'organizations': organizations,
        'volunteer_ops': volunteer_ops,
        'churches': churches,
    }
    
    return render(request, 'category_community.html', context)


# ==================== HELPER FUNCTIONS ====================

def get_user_city(request):
    """Get user's city from session, cookie, or IP geolocation."""
    # Check session first
    if request.session.get('user_city'):
        return request.session['user_city']
    # Default or IP-based detection
    return 'Atlanta'  # Replace with actual detection


def get_featured_listings(category, city):
    """Get featured/promoted listings from Redis cache."""
    cache_key = f"featured:{category}:{city.lower()}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    return []


def get_trending_searches(category, city):
    """Get trending searches from Redis."""
    cache_key = f"trending:{category}:{city.lower()}"
    results = redis_client.zrevrange(cache_key, 0, 7, withscores=False)
    return [r.decode() for r in results] if results else []


def get_category_stats(category, city):
    """Get category statistics from Redis cache."""
    cache_key = f"stats:{category}:{city.lower()}"
    stats = redis_client.hgetall(cache_key)
    if stats:
        return {k.decode(): v.decode() for k, v in stats.items()}
    return {}


def get_history_event(month, day):
    """Get 'This Day in Black History' event."""
    # Query your database or API
    # Return: {'title': '...', 'description': '...', 'year': 1863}
    return None


def get_featured_article(category):
    """Get featured/hero article for a category."""
    # Query your CMS or database
    return None


def get_hbcu_list(limit=4):
    """Get list of HBCUs."""
    # Could be static data or from database
    return []


def get_trending_content(category, subcategory, limit=4):
    """Get trending content for a category."""
    return []


def get_upcoming_events(category, city, limit=5):
    """Get upcoming events."""
    # Query your events table/API
    return []


def get_book_recommendations(limit=8):
    """Get book recommendations."""
    return []


def get_top_story():
    """Get the top news story."""
    return None


def get_news_articles(section=None, limit=5, exclude=None):
    """Get news articles, optionally filtered by section."""
    return []


def get_opinion_articles(limit=3):
    """Get opinion/commentary articles."""
    return []


def get_local_news(city, limit=4):
    """Get local news for a city."""
    return []


def get_good_news(limit=4):
    """Get positive/uplifting news stories."""
    return []


def get_organizations(city, limit=4):
    """Get local organizations."""
    return []


def get_volunteer_opportunities(city, limit=4):
    """Get volunteer opportunities."""
    return []


def get_churches(city, limit=6):
    """Get churches/faith communities."""
    return []