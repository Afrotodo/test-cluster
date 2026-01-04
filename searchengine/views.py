
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
# import time
# import uuid
# from django.utils.decorators import method_decorator
# from django.template.loader import render_to_string
# from django.template.exceptions import TemplateDoesNotExist
# from django.db import connection
# from django.http import JsonResponse
# from django.views import View
# from .searchapi import get_autocomplete
# from decouple import config
# from django.core.cache import cache
# import typesense
# import redis
# from decouple import config
# from django.shortcuts import render, get_object_or_404
# from django.http import Http404



# redis_client = redis.Redis(
#     host=config('REDIS_HOST', default='localhost'),
#     port=config('REDIS_PORT', default=6379, cast=int),
#     db=config('REDIS_DB', default=0, cast=int),
#     password=config('REDIS_PASSWORD', default=None),
#     decode_responses=True,
# )
# logger = logging.getLogger(__name__)


# typesense_client= typesense.Client({
#     'api_key': config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host': config('TYPESENSE_HOST'),
#         'port': config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL')
#     }],
#     'connection_timeout_seconds': 5
# })

# COLLECTION_NAME = 'documents'




# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# SEARCH_CONFIG = {
#     'max_timestamp_age_seconds': 300,  # 5 minutes - reject older requests
#     'rate_limit_per_minute': 30,       # Max requests per session per minute
#     'min_typing_time_ms': 50,          # Bot detection - too fast is suspicious
#     'max_query_length': 500,           # Prevent oversized queries
#     'nonce_expiry_seconds': 60,        # Nonce can only be used once within this window
# }


# # =============================================================================
# # SECURITY VALIDATION
# # =============================================================================

# class SearchSecurityValidator:
#     """Validates security parameters from search requests"""
    
#     @staticmethod
#     def validate_timestamp(timestamp_str):
#         """
#         Validate timestamp is recent (not replay attack)
#         Returns: (is_valid, error_message)
#         """
#         if not timestamp_str:
#             return True, None  # Optional - don't block if missing
        
#         try:
#             timestamp = int(timestamp_str)
#             current_time = int(time.time() * 1000)  # Current time in ms
#             age_seconds = (current_time - timestamp) / 1000
            
#             if age_seconds < -60:  # Allow 60s clock skew
#                 return False, "Timestamp is in the future"
            
#             if age_seconds > SEARCH_CONFIG['max_timestamp_age_seconds']:
#                 return False, "Request too old"
            
#             return True, None
            
#         except (ValueError, TypeError):
#             return True, None  # Don't block on invalid format
    
#     @staticmethod
#     def validate_nonce(nonce, session_id):
#         """
#         Validate nonce hasn't been used before (prevent replay)
#         Returns: (is_valid, error_message)
#         """
#         if not nonce or not session_id:
#             return True, None  # Optional
        
#         if len(nonce) < 8:
#             return False, "Invalid nonce"
        
#         # Check if nonce was already used
#         cache_key = f"nonce:{session_id}:{nonce}"
#         if cache.get(cache_key):
#             return False, "Nonce already used"
        
#         # Mark nonce as used
#         cache.set(cache_key, True, SEARCH_CONFIG['nonce_expiry_seconds'])
#         return True, None
    
#     @staticmethod
#     def validate_session(session_id):
#         """
#         Validate session ID format
#         Returns: (is_valid, error_message)
#         """
#         if not session_id:
#             return True, None  # Optional - will generate one
        
#         # Basic UUID format check (loose)
#         if len(session_id) < 20 or len(session_id) > 50:
#             return False, "Invalid session ID format"
        
#         return True, None
    
#     @staticmethod
#     def check_rate_limit(session_id, client_fp):
#         """
#         Check if request is within rate limits
#         Returns: (is_allowed, error_message)
#         """
#         if not session_id:
#             return True, None
        
#         # Use both session_id and fingerprint for rate limiting
#         rate_key = f"rate:{session_id}:{client_fp or 'unknown'}"
        
#         current_count = cache.get(rate_key, 0)
        
#         if current_count >= SEARCH_CONFIG['rate_limit_per_minute']:
#             return False, "Rate limit exceeded"
        
#         # Increment counter with 60 second expiry
#         cache.set(rate_key, current_count + 1, 60)
#         return True, None
    
#     @staticmethod
#     def detect_bot(typing_time_ms, request_sequence):
#         """
#         Simple bot detection heuristics
#         Returns: (is_suspicious, reason)
#         """
#         try:
#             typing_time = int(typing_time_ms) if typing_time_ms else 0
#             req_seq = int(request_sequence) if request_sequence else 0
            
#             # Too fast typing is suspicious (but not for dropdown selections)
#             if typing_time > 0 and typing_time < SEARCH_CONFIG['min_typing_time_ms']:
#                 return True, "Typing too fast"
            
#             # Unusual request patterns
#             if req_seq > 100:  # Too many requests in one session
#                 return True, "Excessive requests"
            
#             return False, None
            
#         except (ValueError, TypeError):
#             return False, None


# # =============================================================================
# # SEARCH PARAMETER EXTRACTION
# # =============================================================================

# class SearchParams:
#     """Extract and hold all search parameters from request"""
    
#     def __init__(self, request):
#         self.request = request
        
#         # Core search
#         self.query = request.GET.get('query', '').strip()[:SEARCH_CONFIG['max_query_length']]
#         self.alt_mode = request.GET.get('alt_mode', 'y')  # y=semantic, n=keyword
        
#         # Security
#         self.session_id = request.GET.get('session_id', '') or str(uuid.uuid4())
#         self.request_id = request.GET.get('request_id', '')
#         self.timestamp = request.GET.get('timestamp', '')
#         self.nonce = request.GET.get('nonce', '')
        
#         # Analytics
#         self.source = request.GET.get('source', 'unknown')
#         self.device_type = request.GET.get('device_type', 'unknown')
#         self.result_count = request.GET.get('result_count', '0')
#         self.typing_time_ms = request.GET.get('typing_time_ms', '0')
        
#         # Rate limiting
#         self.client_fp = request.GET.get('client_fp', '')
#         self.request_sequence = request.GET.get('req_seq', '0')
    
#     @property
#     def is_keyword_search(self):
#         """True if search came from dropdown (keyword search)"""
#         return self.alt_mode == 'n'
    
#     @property
#     def is_semantic_search(self):
#         """True if user typed freely (semantic search)"""
#         return self.alt_mode == 'y'
    
#     def to_dict(self):
#         """Convert to dictionary for logging"""
#         return {
#             'query': self.query,
#             'alt_mode': self.alt_mode,
#             'session_id': self.session_id,
#             'request_id': self.request_id,
#             'source': self.source,
#             'device_type': self.device_type,
#             'result_count': self.result_count,
#             'typing_time_ms': self.typing_time_ms,
#             'client_fp': self.client_fp,
#         }


# # =============================================================================
# # ANALYTICS LOGGING
# # =============================================================================

# def log_search_analytics(params, search_type, result_count, is_suspicious=False):
#     """
#     Log search for analytics and monitoring
#     """
#     try:
#         logger.info(
#             f"Search: query='{params.query}' type={search_type} "
#             f"alt_mode={params.alt_mode} results={result_count} "
#             f"session={params.session_id[:8] if params.session_id else 'none'}... "
#             f"device={params.device_type} source={params.source} "
#             f"typing_ms={params.typing_time_ms} suspicious={is_suspicious}"
#         )
        
#         # Optional: Store in Redis for real-time analytics
#         # analytics_data = {
#         #     **params.to_dict(),
#         #     'search_type': search_type,
#         #     'result_count': result_count,
#         #     'is_suspicious': is_suspicious,
#         #     'timestamp': time.time()
#         # }
#         # cache.lpush('search_analytics', json.dumps(analytics_data))
        
#     except Exception as e:
#         logger.error(f"Analytics logging error: {e}")


# # =============================================================================
# # VIEWS
# # =============================================================================

# def home(request):
#     return render(request, 'home3.html')


# def search_suggestions(request):
#     """API endpoint for autocomplete dropdown suggestions"""
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


# # =============================================================================
# # MAIN SEARCH VIEW - UPDATED WITH SECURITY & ROUTING
# # =============================================================================

# from .searchsubmission import process_search_submission
# from .word_discovery import word_discovery_multi
# from .typesense_calculations import (
#     execute_full_search,
#     detect_query_intent,
#     get_facets,
#     get_related_searches,
#     get_featured_result,
#     log_search_event
# )


# def search(request):
#     """
#     Production-quality search endpoint with security validation.
#     Routes to keyword or semantic search based on alt_mode.
    
#     alt_mode=n (from dropdown) -> Keyword search (skip spell correction)
#     alt_mode=y (user typed) -> Semantic search (full pipeline)
#     """
    
#     # === 1. EXTRACT ALL PARAMETERS (including security) ===
#     params = SearchParams(request)
    
#     # Legacy parameter extraction (keep existing functionality)
#     page = int(request.GET.get('page', 1))
#     per_page = int(request.GET.get('per_page', 20))
    
#     # Explicit filters from URL
#     # Note: 'source' might contain analytics value 'home' - ignore that
#     source_filter = request.GET.get('source')
#     if source_filter in ('home', 'results_page', 'header', None, ''):
#         source_filter = None  # Not a real filter, just analytics
    
#     filters = {
#         'category': request.GET.get('category'),
#         'source': source_filter,
#         'data_type': request.GET.get('type'),
#         'time_range': request.GET.get('time'),
#         'location': request.GET.get('location'),
#         'sort': request.GET.get('sort', 'relevance'),
#     }
#     filters = {k: v for k, v in filters.items() if v}
    
#     safe_search = request.GET.get('safe', 'on') == 'on'
    
#     user_lat = request.GET.get('lat')
#     user_lng = request.GET.get('lng')
#     user_location = (float(user_lat), float(user_lng)) if user_lat and user_lng else None
    
#     # === 2. SECURITY VALIDATION ===
#     validator = SearchSecurityValidator()
#     is_suspicious = False
    
#     # Validate timestamp
#     is_valid, error = validator.validate_timestamp(params.timestamp)
#     if not is_valid:
#         logger.warning(f"Timestamp validation failed: {error} - {params.session_id}")
    
#     # Check rate limit
#     is_allowed, error = validator.check_rate_limit(params.session_id, params.client_fp)
#     if not is_allowed:
#         logger.warning(f"Rate limit exceeded: {params.session_id}")
#         return render(request, 'results2.html', {
#             'query': params.query,
#             'results': [],
#             'has_results': False,
#             'error': 'Too many requests. Please wait a moment.',
#             'session_id': params.session_id,
#         })
    
#     # Bot detection (log only, don't block)
#     is_suspicious, reason = validator.detect_bot(params.typing_time_ms, params.request_sequence)
#     if is_suspicious:
#         logger.info(f"Suspicious request: {reason} - {params.session_id}")
    
#     # === 3. EMPTY QUERY - SHOW HOMEPAGE ===
#     if not params.query:
#         return render(request, 'results2.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': params.session_id,
#             'show_trending': True,
#         })
    
#     # === 4. CHECK CACHE ===
#     cache_key = f"search:{params.query}:{page}:{params.alt_mode}:{hash(frozenset(filters.items()))}"
#     cached_result = cache.get(cache_key)
    
#     if cached_result and not filters:
#         cached_result['from_cache'] = True
#         return render(request, 'results2.html', cached_result)
    
#     # === 5. ROUTE BASED ON ALT_MODE ===
#     # DEBUG LOGGING - remove in production
#     logger.info(f"=== SEARCH DEBUG ===")
#     logger.info(f"Raw query from URL: '{params.query}'")
#     logger.info(f"alt_mode: '{params.alt_mode}'")
#     logger.info(f"is_keyword_search: {params.is_keyword_search}")
    
#     if params.is_keyword_search:
#         # =====================
#         # KEYWORD SEARCH (from dropdown)
#         # Skip spell correction - user selected exact term
#         # =====================
#         search_type = 'keyword'
#         corrected_query = params.query  # No correction needed
#         was_corrected = False
#         word_corrections = []
#         corrections = {}
#         tuple_array = []
        
#         logger.info(f"KEYWORD SEARCH - using query directly: '{corrected_query}'")
        
#         # Detect intent (optional for keyword search)
#         intent = detect_query_intent(corrected_query, tuple_array)
        
#     else:
#         # =====================
#         # SEMANTIC SEARCH (user typed freely)
#         # Full pipeline with spell correction
#         # =====================
#         search_type = 'semantic'
        
#         logger.info(f"SEMANTIC SEARCH - running word_discovery_multi on: '{params.query}'")
        
#         # Word discovery / spell correction
#         corrections, tuple_array, corrected_query = word_discovery_multi(params.query)
#         was_corrected = params.query.lower() != corrected_query.lower()
#         word_corrections = build_word_corrections(params.query, corrected_query)
        
#         logger.info(f"After word_discovery: corrected_query='{corrected_query}', was_corrected={was_corrected}")
        
#         # Detect intent
#         intent = detect_query_intent(corrected_query, tuple_array)
    
#     logger.info(f"Sending to execute_full_search: query='{corrected_query}'")
    
#     # === 6. EXECUTE SEARCH ===
#     result = execute_full_search(
#         query=corrected_query,
#         session_id=params.session_id,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         pos_tags=tuple_array if params.is_semantic_search else [],
#         safe_search=safe_search
#     )
    
#     results = result.get('results', [])
#     total_results = result.get('total', 0)
    
#     # === 7. ZERO RESULTS HANDLING ===
#     suggestions = []
#     if not results:
#         suggestions = handle_zero_results(
#             original_query=params.query,
#             corrected_query=corrected_query,
#             filters=filters
#         )
    
#     # === 8. GET FACETS ===
#     facets = get_facets(corrected_query) if results else {}
    
#     # === 9. GET RELATED SEARCHES ===
#     related_searches = get_related_searches(corrected_query, intent) if results else []
    
#     # === 10. GET FEATURED RESULT ===
#     featured = None
#     if page == 1:
#         featured = get_featured_result(corrected_query, intent, results)
    
#     # === 11. CATEGORIZE RESULTS ===
#     categorized_results = categorize_results(results)
    
#     # === 12. BUILD PAGINATION ===
#     pagination = build_pagination(page, per_page, total_results)
    
#     # === 13. LOG SEARCH EVENT (existing) ===
#     log_search_event(
#         query=params.query,
#         corrected_query=corrected_query,
#         session_id=params.session_id,
#         intent=intent,
#         total_results=total_results,
#         filters=filters,
#         page=page
#     )
    
#     # === 14. LOG ANALYTICS (new - with security params) ===
#     log_search_analytics(params, search_type, total_results, is_suspicious)
    
#     # === 15. BUILD CONTEXT ===
#     context = {
#         # Query info
#         'query': params.query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'intent': intent,
        
#         # Search type (NEW)
#         'search_type': search_type,  # 'keyword' or 'semantic'
#         'alt_mode': params.alt_mode,
        
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
#         'session_id': params.session_id,
#         'request_id': params.request_id,
#         'search_time': result.get('search_time', 0),
#         'from_cache': False,
        
#         # Device info (NEW)
#         'device_type': params.device_type,
#         'source': params.source,
#     }
    
#     # === 16. CACHE RESULTS ===
#     if not filters and total_results > 0:
#         cache.set(cache_key, context, timeout=300)
    
#     return render(request, 'results2.html', context)


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

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
#     """
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
#         shorter_query = ' '.join(words[:2])
#         suggestions.append({
#             'type': 'broader_search',
#             'message': f'Try a broader search',
#             'action_query': shorter_query
#         })
    
#     if original_query.lower() != corrected_query.lower():
#         suggestions.append({
#             'type': 'try_original',
#             'message': f'Search for "{original_query}" instead',
#             'action_query': original_query
#         })
    
#     suggestions.append({
#         'type': 'help',
#         'message': 'Check your spelling or try different keywords'
#     })
    
#     return suggestions


# def categorize_results(results: list) -> dict:
#     """Groups results by type for different display treatments."""
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
    
#     return {k: v for k, v in categorized.items() if v}


# def build_pagination(page: int, per_page: int, total: int) -> dict:
#     """Builds pagination info for template."""
#     total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    
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


# # =============================================================================
# # OTHER EXISTING VIEWS
# # =============================================================================

# def form_submit(request):
#     """Legacy form submit endpoint"""
#     query = request.GET.get('query', '')
#     session_id = request.GET.get('session_id', '')
    
#     result = process_search_submission(query, session_id)
    
#     return JsonResponse(result)


# def search_api(request):
#     """JSON API endpoint for programmatic access"""
#     query = request.GET.get('q', '') or request.GET.get('query', '')
#     session_id = request.GET.get('session_id', '')
    
#     if not query:
#         return JsonResponse({'error': 'No query provided'}, status=400)
    
#     corrections, tuple_array, corrected_query = word_discovery_multi(query)
#     result = process_search_submission(corrected_query, session_id)
    
#     result['word_discovery'] = {
#         'original_query': query,
#         'corrected_query': corrected_query,
#         'corrections': corrections,
#         'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
#         'was_corrected': query.lower() != corrected_query.lower()
#     }
    
#     return JsonResponse(result)


# # views.py

# def category_view(request, category_slug):
#     """Generic category view with faceted search support."""
    
#     city = get_user_city(request)
    
#     # Map URL slug to document_schema
#     schema_map = {
#         'business': 'business',
#         'culture': 'culture', 
#         'health': 'health',
#         'news': 'news',
#         'community': 'community',
#         'lifestyle': 'lifestyle',
#         'education': 'education',
#         'media': 'media',
#     }
    
#     schema = schema_map.get(category_slug)
#     if not schema:
#         raise Http404("Category not found")
    
#     # Get filter parameters
#     query = request.GET.get('q', '*')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'authority')
#     page = int(request.GET.get('page', 1))
    
#     # Build filters
#     filters = [f'document_schema:={schema}']
    
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # Sort options
#     sort_options = {
#         'authority': 'authority_score:desc',
#         'recent': 'created_at:desc',
#         'relevance': '_text_match:desc',
#     }
#     sort_by = sort_options.get(sort, 'authority_score:desc')
    
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': query if query else '*',
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',
#             'max_facet_values': 20,
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Parse facets
#         facets = {'category': [], 'brand': []}
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 facets['category'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
#             elif facet['field_name'] == 'document_brand':
#                 facets['brand'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
        
#         hits = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         hits = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     context = {
#         'city': city,
#         'category_slug': category_slug,
#         'query': query if query != '*' else '',
        
#         # Results
#         'results': hits,
#         'total': total,
        
#         # Facets
#         'facets': facets,
        
#         # Active filters
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': sum([bool(selected_category), bool(selected_brand)]),
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 20) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     # Route to appropriate template
#     template_map = {
#         'business': 'category_business.html',
#         'culture': 'category_culture.html',
#         'health': 'category_health.html',
#         'news': 'category_news.html',
#         'community': 'category_community.html',
#         'lifestyle': 'category_lifestyle.html',
#         'education': 'category_education.html',
#         'media': 'category_media.html',
#     }
    
#     template = template_map.get(category_slug, 'category_generic.html')
#     return render(request, template, context)


# # views.py

# SUPPORTED_CITIES = [
#     'Atlanta', 'Houston', 'Chicago', 'Detroit', 
#     'New York', 'Los Angeles', 'Philadelphia', 'Washington'
# ]

# # Subcategory mappings for quick filters
# SUBCATEGORY_MAP = {
#     'restaurants': ['restaurant', 'food', 'dining', 'catering', 'cafe'],
#     'beauty': ['salon', 'barber', 'beauty', 'hair', 'nails', 'spa'],
#     'professional': ['lawyer', 'accountant', 'consultant', 'financial', 'insurance'],
#     'contractors': ['contractor', 'plumber', 'electrician', 'construction', 'handyman'],
#     'realestate': ['real estate', 'realtor', 'property', 'mortgage', 'housing'],
#     'retail': ['retail', 'store', 'shop', 'boutique', 'clothing'],
#     'creative': ['photography', 'design', 'art', 'music', 'media', 'marketing'],
#     'tech': ['technology', 'software', 'web', 'app', 'IT', 'digital'],
# }


# def business_category(request):
#     """Business directory page with faceted search and dynamic city selection."""
    
#     # ==================== GET PARAMETERS ====================
#     # City: from URL param, session, or default
#     city = request.GET.get('city', '')
#     if not city:
#         city = request.session.get('user_city', 'Atlanta')
#     else:
#         # Save selected city to session
#         request.session['user_city'] = city
    
#     # Validate city
#     if city not in SUPPORTED_CITIES:
#         city = 'Atlanta'
    
#     # Filter parameters
#     query = request.GET.get('q', '')
#     selected_subcategory = request.GET.get('subcategory', '')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'authority')
#     page = int(request.GET.get('page', 1))
    
#     # ==================== BUILD FILTERS ====================
#     filters = ['document_schema:=business']
    
#     # Apply category filter from facets
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
    
#     # Apply brand filter
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # ==================== BUILD SEARCH QUERY ====================
#     # Start with user query or wildcard
#     search_query = query if query else '*'
    
#     # If subcategory selected, add related terms to search
#     if selected_subcategory and selected_subcategory in SUBCATEGORY_MAP:
#         subcategory_terms = ' '.join(SUBCATEGORY_MAP[selected_subcategory])
#         if search_query == '*':
#             search_query = subcategory_terms
#         else:
#             search_query = f"{search_query} {subcategory_terms}"
    
#     # Add city to search for relevance boosting
#     if search_query == '*':
#         search_query = city
    
#     # ==================== SORT OPTIONS ====================
#     sort_options = {
#         'authority': 'authority_score:desc',
#         'recent': 'created_at:desc',
#         'name': 'document_title:asc',
#     }
#     sort_by = sort_options.get(sort, 'authority_score:desc')
    
#     # ==================== MAIN SEARCH WITH FACETS ====================
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': search_query,
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',
#             'max_facet_values': 20,
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Parse facets
#         facets = {'category': [], 'brand': []}
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 facets['category'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
#             elif facet['field_name'] == 'document_brand':
#                 facets['brand'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
        
#         browse_results = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         browse_results = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # ==================== STATS ====================
#     try:
#         # Get total business count for stats
#         stats_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title',
#             'filter_by': 'document_schema:=business',
#             'per_page': 0,  # Just need count
#         })
#         business_count = stats_results.get('found', 0)
        
#         # Get unique categories count
#         categories_count = len(facets['category']) if facets['category'] else 24
        
#         stats = {
#             'business_count': business_count,
#             'categories_count': categories_count,
#             'verified_pct': 78,  # Placeholder
#         }
#     except Exception as e:
#         print(f"Stats error: {e}")
#         stats = {
#             'business_count': 348,
#             'categories_count': 24,
#             'verified_pct': 78,
#         }
    
#     # ==================== TRENDING SEARCHES ====================
#     trending_searches = [
#         'black barber near me',
#         'soul food',
#         'black accountant',
#         'african restaurant',
#         'natural hair salon',
#         'black owned clothing',
#     ]
    
#     # ==================== CONTEXT ====================
#     active_filter_count = sum([
#         1 if selected_subcategory else 0,
#         1 if selected_category else 0,
#         1 if selected_brand else 0,
#     ])
    
#     context = {
#         # City
#         'city': city,
#         'supported_cities': SUPPORTED_CITIES,
        
#         # Search
#         'query': query,
        
#         # Subcategory (quick filter chips)
#         'selected_subcategory': selected_subcategory,
        
#         # Browse results with facets
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
        
#         # Stats
#         'stats': stats,
        
#         # Trending
#         'trending_searches': trending_searches,
        
#         # Active filters
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 20) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     return render(request, 'category_business.html', context)


# # ==================== BUSINESS SEARCH API ====================
# def business_search_api(request):
#     """
#     API endpoint for AJAX search.
#     Returns JSON for dynamic filtering without page reload.
#     """
#     from django.http import JsonResponse
    
#     city = request.GET.get('city', 'Atlanta')
#     query = request.GET.get('q', '*')
#     subcategory = request.GET.get('subcategory', '')
#     category = request.GET.get('category', '')
#     brand = request.GET.get('brand', '')
#     page = int(request.GET.get('page', 1))
    
#     # Build search query
#     search_query = query if query else '*'
#     if subcategory and subcategory in SUBCATEGORY_MAP:
#         terms = ' '.join(SUBCATEGORY_MAP[subcategory])
#         search_query = f"{search_query} {terms}" if search_query != '*' else terms
    
#     if search_query == '*':
#         search_query = city
    
#     # Build filters
#     filters = ['document_schema:=business']
#     if category:
#         filters.append(f'document_category:={category}')
#     if brand:
#         filters.append(f'document_brand:={brand}')
    
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': search_query,
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': ' && '.join(filters),
#             'facet_by': 'document_category,document_brand',
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Format response
#         hits = []
#         for hit in results.get('hits', []):
#             doc = hit['document']
#             hits.append({
#                 'id': doc.get('id'),
#                 'title': doc.get('document_title'),
#                 'summary': doc.get('document_summary', '')[:150],
#                 'url': doc.get('document_url'),
#                 'category': doc.get('document_category'),
#                 'brand': doc.get('document_brand'),
#                 'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
#                 'keywords': doc.get('primary_keywords', [])[:3],
#             })
        
#         facets = {}
#         for facet in results.get('facet_counts', []):
#             facets[facet['field_name']] = [
#                 {'value': c['value'], 'count': c['count']} 
#                 for c in facet['counts']
#             ]
        
#         return JsonResponse({
#             'success': True,
#             'hits': hits,
#             'total': results.get('found', 0),
#             'facets': facets,
#             'page': page,
#         })
        
#     except Exception as e:
#         return JsonResponse({
#             'success': False,
#             'error': str(e),
#         }, status=500)


# # ==================== BUSINESS DETAIL VIEW ====================
# def business_detail(request, business_id):
#     """
#     Individual business detail page.
#     Shows full information about a single business.
#     """
#     try:
#         # Fetch the specific document
#         doc = typesense_client.collections[COLLECTION_NAME].documents[business_id].retrieve()
        
#         # Get related businesses (same category)
#         category = doc.get('document_category', '')
#         related_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': category,
#             'query_by': 'document_category,keywords',
#             'filter_by': f"document_schema:=business && id:!={business_id}",
#             'per_page': 4,
#         })
#         related = [hit['document'] for hit in related_results.get('hits', [])]
        
#         return render(request, 'business_detail.html', {
#             'business': doc,
#             'related': related,
#         })
        
#     except Exception as e:
#         from django.http import Http404
#         raise Http404("Business not found")


# # ==================== FEATURED BUSINESSES ====================
# def get_featured_businesses(city='Atlanta', limit=6):
#     """
#     Get featured/top-rated businesses for homepage or sidebar.
#     """
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': city,
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=business',
#             'sort_by': 'authority_score:desc',
#             'per_page': limit,
#         })
#         return [hit['document'] for hit in results.get('hits', [])]
#     except Exception as e:
#         print(f"Featured businesses error: {e}")
#         return []


# # ==================== BUSINESS CATEGORIES LIST ====================
# def get_business_categories():
#     """
#     Get all unique business categories with counts.
#     Useful for category index pages.
#     """
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title',
#             'filter_by': 'document_schema:=business',
#             'facet_by': 'document_category',
#             'max_facet_values': 50,
#             'per_page': 0,
#         })
        
#         categories = []
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 categories = [
#                     {'name': c['value'], 'count': c['count']} 
#                     for c in facet['counts']
#                 ]
        
#         return categories
#     except Exception as e:
#         print(f"Categories error: {e}")
#         return []
# # _________________________________________________________________________________________________________________


# def culture_category(request, city):
#     """Culture & heritage page with faceted search."""
    
#     # Get filter parameters
#     query = request.GET.get('q', '*')
#     selected_topic = request.GET.get('topic', '')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'authority')
#     page = int(request.GET.get('page', 1))
    
#     today = date.today()
    
#     # ==================== BUILD FILTERS ====================
#     # Culture schema includes: history, art, music, landmarks, movies, books
#     filters = ['document_schema:=culture']
    
#     # Topic to category mapping (topics are user-friendly, categories are data values)
#     topic_category_map = {
#         'music': 'music',
#         'art': 'culture',  # Art content falls under 'culture' category
#         'literature': 'culture',
#         'film': 'media',
#         'hbcus': 'education',
#         'food': 'food',
#         'fashion': 'culture',
#         'history': 'history',
#         'theater': 'culture',
#         'events': 'culture',
#     }
    
#     # If topic selected, add to schema filter
#     if selected_topic and selected_topic in topic_category_map:
#         # For HBCUs, search education schema
#         if selected_topic == 'hbcus':
#             filters = ['document_schema:=education']
#         # For film, include media schema
#         elif selected_topic == 'film':
#             filters = ['document_schema:=[culture,media]']
    
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
    
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # Sort options
#     sort_by = 'authority_score:desc' if sort == 'authority' else 'created_at:desc'
    
#     # ==================== SEARCH QUERIES ====================
#     try:
#         # Main browse results with facets
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': query if query else '*',
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',
#             'max_facet_values': 20,
#             'per_page': 18,
#             'page': page,
#         })
        
#         # Parse facets
#         facets = {'category': [], 'brand': []}
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 facets['category'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
#             elif facet['field_name'] == 'document_brand':
#                 facets['brand'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
        
#         browse_results = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         browse_results = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # ==================== HBCU SPOTLIGHT ====================
#     try:
#         hbcu_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': 'university college',
#             'query_by': 'document_title,keywords',
#             'filter_by': 'document_schema:=education',
#             'sort_by': 'authority_score:desc',
#             'per_page': 4,
#         })
#         hbcus = hbcu_results.get('hits', [])
#     except Exception as e:
#         print(f"HBCU search error: {e}")
#         hbcus = []
    
#     # ==================== FEATURED ARTICLE ====================
#     try:
#         featured_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title,document_summary',
#             'filter_by': 'document_schema:=culture && document_category:=history',
#             'sort_by': 'authority_score:desc',
#             'per_page': 1,
#         })
#         if featured_results.get('hits'):
#             featured_doc = featured_results['hits'][0]['document']
#             featured_article = {
#                 'title': featured_doc.get('document_title'),
#                 'excerpt': featured_doc.get('document_summary', '')[:200],
#                 'url': featured_doc.get('document_url'),
#                 'image': featured_doc.get('image_url', [None])[0] if featured_doc.get('image_url') else None,
#             }
#         else:
#             featured_article = None
#     except Exception as e:
#         print(f"Featured article error: {e}")
#         featured_article = None
    
#     # ==================== CONTEXT ====================
#     active_filter_count = sum([
#         1 if selected_category else 0,
#         1 if selected_brand else 0,
#     ])
    
#     context = {
#         'city': city,
#         'query': query if query != '*' else '',
        
#         # Today's date for "This Day in History"
#         'today': {
#             'day': today.day,
#             'month': today.strftime('%b').upper(),
#         },
        
#         # Featured content
#         'featured_article': featured_article,
#         'hbcus': hbcus,
#         'history_event': None,  # Implement with your history database
#         'trending_music': [],   # Implement with your trending data
#         'events': [],           # Implement with your events data
#         'books': [],            # Implement with your books data
        
#         # Browse results with facets
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
        
#         # Active filters
#         'selected_topic': selected_topic,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 18) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     return render(request, 'category_culture.html', context)

# # _________________________________________________________________________________________________________________


# def health_category(request, city):
#     """Health resources page with faceted search."""
    
#     # Get filter parameters from URL
#     query = request.GET.get('q', '*')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'authority')
#     page = int(request.GET.get('page', 1))
    
#     # Build filter string
#     filters = ['document_schema:=health']
    
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
    
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # Build sort string
#     if sort == 'recent':
#         sort_by = 'created_at:desc'
#     else:
#         sort_by = 'authority_score:desc'
    
#     try:
#         # Search with facets
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': query if query else '*',
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',  # Enable faceting
#             'max_facet_values': 20,  # Show top 20 values per facet
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Parse facet counts
#         facets = {
#             'category': [],
#             'brand': []
#         }
        
#         for facet in results.get('facet_counts', []):
#             field = facet['field_name']
#             if field == 'document_category':
#                 facets['category'] = [
#                     {'value': c['value'], 'count': c['count']} 
#                     for c in facet['counts']
#                 ]
#             elif field == 'document_brand':
#                 facets['brand'] = [
#                     {'value': c['value'], 'count': c['count']} 
#                     for c in facet['counts']
#                 ]
        
#         providers = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         providers = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # Count active filters for mobile badge
#     active_filter_count = sum([
#         1 if selected_category else 0,
#         1 if selected_brand else 0,
#     ])
    
#     context = {
#         'city': city,
#         'query': query if query != '*' else '',
        
#         # Results
#         'providers': providers,
#         'total': total,
        
#         # Facets for sidebar/filter sheet
#         'facets': facets,
#         'category_count': len(facets['category']),
#         'brand_count': len(facets['brand']),
        
#         # Active filters
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 20) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     return render(request, 'category_health.html', context)
# # _________________________________________________________________________________________________________________


# def news_category(request, city='Atlanta'):
#     """News & media page with faceted search."""
    
#     # Get filter parameters
#     query = request.GET.get('q', '*')
#     selected_section = request.GET.get('section', '')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'recent')  # News defaults to recent
#     page = int(request.GET.get('page', 1))
    
#     # ==================== BUILD FILTERS ====================
#     filters = ['document_schema:=news']
    
#     # Section to category mapping
#     section_category_map = {
#         'local': None,  # Handled separately with city filter
#         'national': 'national',
#         'politics': 'politics',
#         'business': 'business',
#         'sports': 'sports',
#         'entertainment': 'entertainment',
#         'opinion': 'opinion',
#     }
    
#     # Apply section filter
#     if selected_section and selected_section in section_category_map:
#         section_cat = section_category_map[selected_section]
#         if section_cat:
#             filters.append(f'document_category:={section_cat}')
    
#     # Apply category filter from facets
#     if selected_category:
#         filters.append(f'document_category:={selected_category}')
    
#     # Apply brand filter
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # Sort options - news defaults to recent
#     sort_by = 'created_at:desc' if sort == 'recent' else 'authority_score:desc'
    
#     # ==================== MAIN SEARCH WITH FACETS ====================
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': query if query else '*',
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',
#             'max_facet_values': 20,
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Parse facets
#         facets = {'category': [], 'brand': []}
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 facets['category'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
#             elif facet['field_name'] == 'document_brand':
#                 facets['brand'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
        
#         browse_results = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         browse_results = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # ==================== TOP STORY ====================
#     try:
#         top_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title,document_summary',
#             'filter_by': 'document_schema:=news',
#             'sort_by': 'authority_score:desc',
#             'per_page': 1,
#         })
#         top_story = top_results['hits'][0]['document'] if top_results.get('hits') else None
#     except Exception as e:
#         print(f"Top story error: {e}")
#         top_story = None
    
#     # ==================== SIDEBAR STORIES ====================
#     try:
#         sidebar_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title,document_summary',
#             'filter_by': 'document_schema:=news',
#             'sort_by': 'created_at:desc',
#             'per_page': 4,
#             'offset': 1,  # Skip top story
#         })
#         sidebar_stories = [hit['document'] for hit in sidebar_results.get('hits', [])]
#     except Exception as e:
#         print(f"Sidebar stories error: {e}")
#         sidebar_stories = []
    
#     # ==================== LOCAL NEWS ====================
#     try:
#         # Search for news mentioning the city
#         local_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': city,
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=news',
#             'sort_by': 'created_at:desc',
#             'per_page': 5,
#         })
#         local_news = [hit['document'] for hit in local_results.get('hits', [])]
#     except Exception as e:
#         print(f"Local news error: {e}")
#         local_news = []
    
#     # ==================== GOOD NEWS ====================
#     try:
#         # Search for positive/uplifting news
#         good_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': 'success achievement award grant scholarship wins first',
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=news',
#             'sort_by': 'created_at:desc',
#             'per_page': 5,
#         })
#         good_news = [hit['document'] for hit in good_results.get('hits', [])]
#     except Exception as e:
#         print(f"Good news error: {e}")
#         good_news = []
    
#     # ==================== OPINIONS ====================
#     try:
#         opinion_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': '*',
#             'query_by': 'document_title,document_summary',
#             'filter_by': 'document_schema:=news && document_category:=opinion',
#             'sort_by': 'created_at:desc',
#             'per_page': 3,
#         })
#         opinions = [hit['document'] for hit in opinion_results.get('hits', [])]
#     except Exception as e:
#         print(f"Opinions error: {e}")
#         opinions = []
    
#     # ==================== NEWS SOURCES ====================
#     # Get unique sources from facets
#     news_sources = [f['value'] for f in facets['brand'][:12]] if facets['brand'] else []
    
#     # ==================== CONTEXT ====================
#     active_filter_count = sum([
#         1 if selected_category else 0,
#         1 if selected_brand else 0,
#     ])
    
#     context = {
#         'city': city,
#         'query': query if query != '*' else '',
        
#         # Hero section
#         'top_story': top_story,
#         'sidebar_stories': sidebar_stories,
        
#         # Browse results with facets
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
        
#         # Sidebar content
#         'local_news': local_news,
#         'good_news': good_news,
#         'opinions': opinions,
#         'news_sources': news_sources,
        
#         # Active filters
#         'selected_section': selected_section,
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 20) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     return render(request, 'category_news.html', context)


# # ==================== URL CONFIGURATION ====================
# # In urls.py:
# # path('news/', views.news_category, name='news'),
# # Or within category_view router:
# # if category_slug == 'news':
# #     return news_category(request, city)


# # ==================== HELPER: TIME AGO ====================
# def add_time_ago(documents):
#     """Add time_ago field to documents for display."""
#     from datetime import datetime, timezone
    
#     now = datetime.now(timezone.utc)
    
#     for doc in documents:
#         if 'created_at' in doc:
#             try:
#                 # Parse timestamp (adjust format as needed)
#                 created = datetime.fromisoformat(doc['created_at'].replace('Z', '+00:00'))
#                 diff = now - created
                
#                 if diff.days > 0:
#                     doc['time_ago'] = f"{diff.days}d ago"
#                 elif diff.seconds >= 3600:
#                     hours = diff.seconds // 3600
#                     doc['time_ago'] = f"{hours}h ago"
#                 else:
#                     minutes = diff.seconds // 60
#                     doc['time_ago'] = f"{minutes}m ago"
#             except:
#                 doc['time_ago'] = "Today"
#         else:
#             doc['time_ago'] = "Today"
    
#     return documents


# # _________________________________________________________________________________________________________________

# # Supported cities for community pages
# SUPPORTED_CITIES = [
#     'Atlanta', 'Houston', 'Chicago', 'Detroit', 
#     'New York', 'Los Angeles', 'Philadelphia', 'Washington'
# ]


# def community_category(request):
#     """Community hub page with faceted search and dynamic city selection."""
    
#     # ==================== GET PARAMETERS ====================
#     # City: from URL param, session, or default
#     city = request.GET.get('city', '')
#     if not city:
#         city = request.session.get('user_city', 'Atlanta')
#     else:
#         # Save selected city to session
#         request.session['user_city'] = city
    
#     # Validate city
#     if city not in SUPPORTED_CITIES:
#         city = 'Atlanta'
    
#     # Filter parameters
#     query = request.GET.get('q', '*')
#     selected_category = request.GET.get('category', '')
#     selected_brand = request.GET.get('brand', '')
#     sort = request.GET.get('sort', 'authority')
#     page = int(request.GET.get('page', 1))
    
#     # ==================== BUILD FILTERS ====================
#     filters = ['document_schema:=community']
    
#     # Category mapping for help cards
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
    
#     if selected_category:
#         # Map friendly names to data values
#         mapped_category = help_category_map.get(selected_category, selected_category)
#         filters.append(f'document_category:={mapped_category}')
    
#     if selected_brand:
#         filters.append(f'document_brand:={selected_brand}')
    
#     filter_by = ' && '.join(filters)
    
#     # ==================== SORT OPTIONS ====================
#     sort_options = {
#         'authority': 'authority_score:desc',
#         'recent': 'created_at:desc',
#         'name': 'document_title:asc',
#     }
#     sort_by = sort_options.get(sort, 'authority_score:desc')
    
#     # ==================== MAIN SEARCH WITH FACETS ====================
#     try:
#         # Add city to search query if searching
#         search_query = query if query != '*' else '*'
#         if city and search_query == '*':
#             # Boost results mentioning the city
#             search_query = city
        
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': search_query,
#             'query_by': 'document_title,document_summary,keywords,semantic_keywords,primary_keywords',
#             'filter_by': filter_by,
#             'sort_by': sort_by,
#             'facet_by': 'document_category,document_brand',
#             'max_facet_values': 20,
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Parse facets
#         facets = {'category': [], 'brand': []}
#         for facet in results.get('facet_counts', []):
#             if facet['field_name'] == 'document_category':
#                 facets['category'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
#             elif facet['field_name'] == 'document_brand':
#                 facets['brand'] = [{'value': c['value'], 'count': c['count']} for c in facet['counts']]
        
#         browse_results = results.get('hits', [])
#         total = results.get('found', 0)
        
#     except Exception as e:
#         print(f"Typesense error: {e}")
#         browse_results = []
#         total = 0
#         facets = {'category': [], 'brand': []}
    
#     # ==================== UPCOMING EVENTS ====================
#     # Search for event-related content
#     try:
#         event_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': f'{city} event meeting workshop conference',
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=community',
#             'sort_by': 'created_at:desc',
#             'per_page': 5,
#         })
#         upcoming_events = event_results.get('hits', [])
        
#         # Add parsed date info (you'd normally extract from document)
#         import datetime
#         today = datetime.date.today()
#         for i, event in enumerate(upcoming_events):
#             future_date = today + datetime.timedelta(days=i * 5 + 3)
#             event['month'] = future_date.strftime('%b').upper()
#             event['day'] = future_date.day
#             event['time'] = '10 AM - 2 PM'
            
#     except Exception as e:
#         print(f"Events error: {e}")
#         upcoming_events = []
    
#     # ==================== CHURCHES / FAITH COMMUNITIES ====================
#     try:
#         church_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': f'{city} church baptist AME methodist faith ministry',
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=community',
#             'sort_by': 'authority_score:desc',
#             'per_page': 8,
#         })
#         churches = church_results.get('hits', [])
#     except Exception as e:
#         print(f"Churches error: {e}")
#         churches = []
    
#     # ==================== LOCAL ORGANIZATIONS ====================
#     try:
#         org_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': f'{city} NAACP Urban League organization nonprofit foundation',
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=community',
#             'sort_by': 'authority_score:desc',
#             'per_page': 4,
#         })
#         organizations = org_results.get('hits', [])
        
#         # Add initials for logo placeholder
#         for org in organizations:
#             title = org.get('document', {}).get('document_title', '')
#             words = title.split()[:2]
#             org['initials'] = ''.join([w[0] for w in words if w]).upper()[:3]
            
#     except Exception as e:
#         print(f"Organizations error: {e}")
#         organizations = []
    
#     # ==================== VOLUNTEER OPPORTUNITIES ====================
#     try:
#         volunteer_results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': f'{city} volunteer service help mentor tutor',
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': 'document_schema:=community',
#             'sort_by': 'created_at:desc',
#             'per_page': 5,
#         })
#         volunteer_ops = volunteer_results.get('hits', [])
        
#         # Add schedule/type info
#         schedules = ['Weekly', 'Ongoing', 'Flexible', 'One-time', 'Monthly']
#         for i, vol in enumerate(volunteer_ops):
#             vol['schedule'] = 'Flexible'
#             vol['type'] = schedules[i % len(schedules)]
            
#     except Exception as e:
#         print(f"Volunteer error: {e}")
#         volunteer_ops = []
    
#     # ==================== CONTEXT ====================
#     active_filter_count = sum([
#         1 if selected_category else 0,
#         1 if selected_brand else 0,
#     ])
    
#     context = {
#         # City
#         'city': city,
#         'supported_cities': SUPPORTED_CITIES,
        
#         # Search
#         'query': query if query != '*' else '',
        
#         # Browse results with facets
#         'results': browse_results,
#         'total': total,
#         'facets': facets,
        
#         # Special sections
#         'upcoming_events': upcoming_events,
#         'churches': churches,
#         'organizations': organizations,
#         'volunteer_ops': volunteer_ops,
        
#         # Active filters
#         'selected_category': selected_category,
#         'selected_brand': selected_brand,
#         'active_filter_count': active_filter_count,
        
#         # Pagination
#         'page': page,
#         'has_more': (page * 20) < total,
        
#         # Sort
#         'sort': sort,
#     }
    
#     return render(request, 'category_community.html', context)


# # ==================== URL CONFIGURATION ====================
# # In urls.py:
# # path('community/', views.community_category, name='community'),


# # ==================== HELPER: GET USER CITY ====================
# def get_user_city(request, default='Atlanta'):
#     """
#     Get user's city from various sources:
#     1. URL parameter
#     2. Session
#     3. Geolocation (if implemented)
#     4. Default
#     """
#     # Check URL param first
#     city = request.GET.get('city', '')
    
#     # Then session
#     if not city:
#         city = request.session.get('user_city', '')
    
#     # Validate
#     if city not in SUPPORTED_CITIES:
#         city = default
    
#     return city


# # ==================== ALTERNATIVE: CITY-SPECIFIC VIEWS ====================
# def community_by_city(request, city_slug):
#     """
#     URL pattern: /community/atlanta/
#     Alternative routing by city in URL path.
#     """
#     # Map URL slug to city name
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
    
#     city = city_map.get(city_slug.lower(), 'Atlanta')
#     request.session['user_city'] = city
    
#     # Reuse main view with city set
#     return community_category(request)


# # ==================== API: SEARCH COMMUNITY RESOURCES ====================
# def community_search_api(request):
#     """
#     API endpoint for AJAX search.
#     Returns JSON for dynamic filtering without page reload.
#     """
#     from django.http import JsonResponse
    
#     city = request.GET.get('city', 'Atlanta')
#     query = request.GET.get('q', '*')
#     category = request.GET.get('category', '')
#     brand = request.GET.get('brand', '')
#     page = int(request.GET.get('page', 1))
    
#     filters = ['document_schema:=community']
#     if category:
#         filters.append(f'document_category:={category}')
#     if brand:
#         filters.append(f'document_brand:={brand}')
    
#     try:
#         results = typesense_client.collections[COLLECTION_NAME].documents.search({
#             'q': query if query else city,
#             'query_by': 'document_title,document_summary,keywords',
#             'filter_by': ' && '.join(filters),
#             'facet_by': 'document_category,document_brand',
#             'per_page': 20,
#             'page': page,
#         })
        
#         # Format response
#         hits = []
#         for hit in results.get('hits', []):
#             doc = hit['document']
#             hits.append({
#                 'id': doc.get('id'),
#                 'title': doc.get('document_title'),
#                 'summary': doc.get('document_summary', '')[:150],
#                 'url': doc.get('document_url'),
#                 'category': doc.get('document_category'),
#                 'brand': doc.get('document_brand'),
#                 'image': doc.get('image_url', [None])[0] if doc.get('image_url') else None,
#             })
        
#         facets = {}
#         for facet in results.get('facet_counts', []):
#             facets[facet['field_name']] = [
#                 {'value': c['value'], 'count': c['count']} 
#                 for c in facet['counts']
#             ]
        
#         return JsonResponse({
#             'success': True,
#             'hits': hits,
#             'total': results.get('found', 0),
#             'facets': facets,
#             'page': page,
#         })
        
#     except Exception as e:
#         return JsonResponse({
#             'success': False,
#             'error': str(e),
#         }, status=500)



# # ==================== HELPER FUNCTIONS ====================

# # def get_user_city(request):
# #     """Get user's city from session, cookie, or IP geolocation."""
# #     # Check session first
# #     if request.session.get('user_city'):
# #         return request.session['user_city']
# #     # Default or IP-based detection
# #     return 'Atlanta'  # Replace with actual detection


# def get_featured_listings(category, city):
#     """Get featured/promoted listings from Redis cache."""
#     cache_key = f"featured:{category}:{city.lower()}"
#     cached = redis_client.get(cache_key)
#     if cached:
#         return json.loads(cached)
#     return []


# def get_trending_searches(category, city):
#     """Get trending searches from Redis."""
#     cache_key = f"trending:{category}:{city.lower()}"
#     results = redis_client.zrevrange(cache_key, 0, 7, withscores=False)
#     return [r.decode() for r in results] if results else []


# def get_category_stats(category, city):
#     """Get category statistics from Redis cache."""
#     cache_key = f"stats:{category}:{city.lower()}"
#     stats = redis_client.hgetall(cache_key)
#     if stats:
#         return {k.decode(): v.decode() for k, v in stats.items()}
#     return {}


# def get_history_event(month, day):
#     """Get 'This Day in Black History' event."""
#     # Query your database or API
#     # Return: {'title': '...', 'description': '...', 'year': 1863}
#     return None


# def get_featured_article(category):
#     """Get featured/hero article for a category."""
#     # Query your CMS or database
#     return None


# def get_hbcu_list(limit=4):
#     """Get list of HBCUs."""
#     # Could be static data or from database
#     return []


# def get_trending_content(category, subcategory, limit=4):
#     """Get trending content for a category."""
#     return []


# def get_upcoming_events(category, city, limit=5):
#     """Get upcoming events."""
#     # Query your events table/API
#     return []


# def get_book_recommendations(limit=8):
#     """Get book recommendations."""
#     return []


# def get_top_story():
#     """Get the top news story."""
#     return None


# def get_news_articles(section=None, limit=5, exclude=None):
#     """Get news articles, optionally filtered by section."""
#     return []


# def get_opinion_articles(limit=3):
#     """Get opinion/commentary articles."""
#     return []


# def get_local_news(city, limit=4):
#     """Get local news for a city."""
#     return []


# def get_good_news(limit=4):
#     """Get positive/uplifting news stories."""
#     return []


# def get_organizations(city, limit=4):
#     """Get local organizations."""
#     return []


# def get_volunteer_opportunities(city, limit=4):
#     """Get volunteer opportunities."""
#     return []


# def get_churches(city, limit=6):
#     """Get churches/faith communities."""
#     return []


# views.py - Production-Ready AfroTodo Search Engine Views
# =============================================================================
# IMPORTS
# =============================================================================



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


def get_client_ip(request):
    """Extract client IP from request."""
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0].strip()
    else:
        ip = request.META.get('REMOTE_ADDR', '')
    return ip


def get_location_from_request(request):
    """Extract location from request."""
    location = {}
    city = request.GET.get('city', '')
    if not city:
        try:
            city = request.session.get('user_city', '')
        except:
            pass
    if city:
        location['city'] = city
    return location if location else None


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logger = logging.getLogger(__name__)


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
    Main search endpoint with security validation.
    Routes to keyword or semantic search based on alt_mode.
    """
    
    # === 1. EXTRACT & VALIDATE PARAMETERS ===
    params = SearchParams(request)
    page = validate_page(request.GET.get('page', 1))
    per_page = validate_per_page(request.GET.get('per_page', 20))
    
    # Extract filters (ignore analytics values in source)
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
    
    # User location
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
    
    # === 2. SECURITY VALIDATION ===
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
    
    # === 3. EMPTY QUERY ===
    if not params.query:
        return render(request, 'results2.html', {
            'query': '',
            'results': [],
            'has_results': False,
            'session_id': params.session_id,
            'show_trending': True,
        })
    
    # === 4. CHECK CACHE ===
    cache_key = get_cache_key('search', params.query, page, params.alt_mode, json.dumps(filters, sort_keys=True))
    cached_result = safe_cache_get(cache_key)
    
    if cached_result and not filters:
        cached_result['from_cache'] = True
        return render(request, 'results2.html', cached_result)
    
    # === 5. ROUTE BASED ON ALT_MODE ===
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
    
    # === 6. EXECUTE SEARCH ===
    results = []
    total_results = 0
    search_time = 0
    
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
        except Exception as e:
            logger.error(f"Search execution error: {e}")
    
    # === 7. ZERO RESULTS HANDLING ===
    suggestions = []
    if not results:
        suggestions = handle_zero_results(params.query, corrected_query, filters)
    
    # === 8. GET SUPPLEMENTARY DATA ===
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
    
    # === 9. CATEGORIZE & PAGINATE ===
    categorized_results = categorize_results(results)
    pagination = build_pagination(page, per_page, total_results)
    
    # === 10. LOG EVENTS ===
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
    
    # === 11. BUILD CONTEXT ===
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
        'request_id': params.request_id,
        'search_time': search_time,
        'from_cache': False,
        'device_type': params.device_type,
        'source': params.source,
    }
    
    # === 12. CACHE RESULTS ===
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