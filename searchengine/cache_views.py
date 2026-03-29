"""
cache_views.py
API endpoints for vocabulary cache management.

Add to your urls.py:
    from .cache_views import reload_cache_view, cache_status_view, add_to_cache_view
    
    urlpatterns += [
        path('api/cache/reload/', reload_cache_view, name='cache_reload'),
        path('api/cache/add/', add_to_cache_view, name='cache_add'),
        path('api/cache/status/', cache_status_view, name='cache_status'),
    ]
"""

# import json
# import logging
# import hashlib
# import hmac
# from functools import wraps

# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from django.views.decorators.http import require_http_methods
# from django.conf import settings

# from .vocabulary_cache import (
#     vocab_cache,
#     reload_cache_from_dict,
#     reload_cache_from_file,
#     get_cache_status,
#     add_terms_to_cache,
#     add_terms_nosave,
#     reload_cache_nosave,
#     save_cache,
# )
# # from .vocabulary_cache import (
# #     vocab_cache,
# #     reload_cache_from_dict,
# #     reload_cache_from_file,
# #     get_cache_status,
# #     add_terms_to_cache,
# # )

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Secret key for API authentication (set in settings.py or environment)
# # Example: CACHE_API_SECRET = 'your-secret-key-here'
# CACHE_API_SECRET = getattr(settings, 'CACHE_API_SECRET', None)

# # Maximum payload size (50MB should be plenty)
# MAX_PAYLOAD_SIZE = 50 * 1024 * 1024


# # =============================================================================
# # AUTHENTICATION
# # =============================================================================

# def verify_api_key(request):
#     """Verify the API key from request header."""
#     if not CACHE_API_SECRET:
#         # If no secret configured, allow all (for development)
#         logger.warning("CACHE_API_SECRET not set - allowing unauthenticated access")
#         return True
    
#     # Check Authorization header
#     auth_header = request.headers.get('Authorization', '')
    
#     if auth_header.startswith('Bearer '):
#         token = auth_header[7:]
#         if hmac.compare_digest(token, CACHE_API_SECRET):
#             return True
    
#     # Check X-API-Key header
#     api_key = request.headers.get('X-API-Key', '')
#     if api_key and hmac.compare_digest(api_key, CACHE_API_SECRET):
#         return True
    
#     return False


# def require_api_key(view_func):
#     """Decorator to require API key authentication."""
#     @wraps(view_func)
#     def wrapper(request, *args, **kwargs):
#         if not verify_api_key(request):
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Unauthorized - invalid or missing API key'
#             }, status=401)
#         return view_func(request, *args, **kwargs)
#     return wrapper


# # =============================================================================
# # ENDPOINTS
# # =============================================================================

# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def reload_cache_view(request):
#     """
#     Reload vocabulary cache from POST data (REPLACES all data).
    
#     Called by Colab to update the cache.
    
#     Request:
#         POST /api/cache/reload/
#         Headers:
#             Authorization: Bearer <your-secret-key>
#             Content-Type: application/json
#         Body:
#             {
#                 "term:atlanta:us_city": {"term": "atlanta", ...},
#                 "term:georgia:us_state": {"term": "georgia", ...},
#                 ...
#             }
    
#     Response:
#         {
#             "success": true,
#             "message": "Cache reloaded successfully",
#             "stats": {
#                 "term_count": 150000,
#                 "cities": 30000,
#                 "states": 50,
#                 ...
#             }
#         }
#     """
#     try:
#         # Check content length
#         content_length = int(request.headers.get('Content-Length', 0))
#         if content_length > MAX_PAYLOAD_SIZE:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
#             }, status=413)
        
#         # Parse JSON body
#         try:
#             data = json.loads(request.body)
#         except json.JSONDecodeError as e:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Invalid JSON: {str(e)}'
#             }, status=400)
        
#         # Validate data structure
#         if not isinstance(data, dict):
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Expected JSON object with term data'
#             }, status=400)
        
#         if len(data) == 0:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Empty data - nothing to load'
#             }, status=400)
        
#         logger.info(f"Received cache reload request with {len(data):,} terms")
        
#         # Load into cache
#         success = reload_cache_from_dict(data)
        
#         if success:
#             status = get_cache_status()
#             logger.info(f"Cache reloaded successfully: {status}")
            
#             return JsonResponse({
#                 'success': True,
#                 'message': 'Cache reloaded successfully',
#                 'stats': status
#             })
#         else:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Failed to load cache'
#             }, status=500)
    
#     except Exception as e:
#         logger.exception(f"Error reloading cache: {e}")
#         return JsonResponse({
#             'success': False,
#             'error': f'Server error: {str(e)}'
#         }, status=500)


# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def add_to_cache_view(request):
#     """
#     Add new terms to cache WITHOUT overwriting existing ones.
    
#     Request:
#         POST /api/cache/add/
#         Headers:
#             Authorization: Bearer <your-secret-key>
#             Content-Type: application/json
#         Body:
#             {
#                 "term:category:dictionary_word": {"term": "category", ...},
#                 "term:flavor:food": {"term": "flavor", ...},
#                 ...
#             }
    
#     Response:
#         {
#             "success": true,
#             "message": "Added 5,000 new terms (skipped 2,000 existing)",
#             "stats": {
#                 "added": 5000,
#                 "skipped": 2000,
#                 "total_after": 229928
#             }
#         }
#     """
#     try:
#         content_length = int(request.headers.get('Content-Length', 0))
#         if content_length > MAX_PAYLOAD_SIZE:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
#             }, status=413)
        
#         try:
#             data = json.loads(request.body)
#         except json.JSONDecodeError as e:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Invalid JSON: {str(e)}'
#             }, status=400)
        
#         if not isinstance(data, dict) or len(data) == 0:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Expected non-empty JSON object with term data'
#             }, status=400)
        
#         logger.info(f"Received cache ADD request with {len(data):,} terms")
        
#         # Use the add_terms function (merge, not replace)
#         result = add_terms_to_cache(data)
        
#         return JsonResponse({
#             'success': True,
#             'message': f"Added {result['added']:,} new terms (skipped {result['skipped']:,} existing)",
#             'stats': {
#                 'added': result['added'],
#                 'skipped': result['skipped'],
#                 'total_after': vocab_cache.term_count
#             }
#         })
    
#     except Exception as e:
#         logger.exception(f"Error adding to cache: {e}")
#         return JsonResponse({
#             'success': False,
#             'error': f'Server error: {str(e)}'
#         }, status=500)


# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def reload_cache_from_file_view(request):
#     """
#     Reload vocabulary cache from local file.
    
#     Request:
#         POST /api/cache/reload-from-file/
#         Headers:
#             Authorization: Bearer <your-secret-key>
    
#     Response:
#         {
#             "success": true,
#             "message": "Cache reloaded from file",
#             "stats": {...}
#         }
#     """
#     try:
#         success = reload_cache_from_file()
        
#         if success:
#             status = get_cache_status()
#             return JsonResponse({
#                 'success': True,
#                 'message': 'Cache reloaded from file',
#                 'stats': status
#             })
#         else:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Failed to load cache from file (file may not exist)'
#             }, status=500)
    
#     except Exception as e:
#         logger.exception(f"Error reloading cache from file: {e}")
#         return JsonResponse({
#             'success': False,
#             'error': f'Server error: {str(e)}'
#         }, status=500)


# @require_http_methods(["GET"])
# def cache_status_view(request):
#     """
#     Get current cache status.
    
#     Request:
#         GET /api/cache/status/
    
#     Response:
#         {
#             "success": true,
#             "status": {
#                 "loaded": true,
#                 "term_count": 150000,
#                 "cities": 30000,
#                 "states": 50,
#                 "load_time": "2.45s",
#                 "last_updated": "2024-01-15 10:30:00"
#             }
#         }
#     """
#     try:
#         status = get_cache_status()
#         return JsonResponse({
#             'success': True,
#             'status': status
#         })
#     except Exception as e:
#         logger.exception(f"Error getting cache status: {e}")
#         return JsonResponse({
#             'success': False,
#             'error': f'Server error: {str(e)}'
#         }, status=500)


# @require_http_methods(["GET"])
# def cache_test_view(request):
#     """
#     Test cache lookups.
    
#     Request:
#         GET /api/cache/test/?term=atlanta
#         GET /api/cache/test/?query=hbcus+in+georgia
    
#     Response:
#         {
#             "success": true,
#             "term": "atlanta",
#             "is_location": true,
#             "metadata": {...}
#         }
#     """
#     try:
#         term = request.GET.get('term', '')
#         query = request.GET.get('query', '')
        
#         if not vocab_cache.loaded:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Cache not loaded'
#             }, status=503)
        
#         if term:
#             return JsonResponse({
#                 'success': True,
#                 'term': term,
#                 'is_location': vocab_cache.is_location(term),
#                 'is_city': vocab_cache.is_city(term),
#                 'is_state': vocab_cache.is_state(term),
#                 'metadata': vocab_cache.get_term(term)
#             })
        
#         if query:
#             result = vocab_cache.classify_query(query)
#             return JsonResponse({
#                 'success': True,
#                 'query': query,
#                 'result': result
#             })
        
#         return JsonResponse({
#             'success': False,
#             'error': 'Provide ?term= or ?query= parameter'
#         }, status=400)
    
#     except Exception as e:
#         logger.exception(f"Error testing cache: {e}")
#         return JsonResponse({
#             'success': False,
#             'error': f'Server error: {str(e)}'
#         }, status=500)

# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def reload_cache_nosave_view(request):
#     """Reload cache from POST data WITHOUT saving to file."""
#     try:
#         content_length = int(request.headers.get('Content-Length', 0))
#         if content_length > MAX_PAYLOAD_SIZE:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
#             }, status=413)

#         try:
#             data = json.loads(request.body)
#         except json.JSONDecodeError as e:
#             return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)

#         if not isinstance(data, dict) or len(data) == 0:
#             return JsonResponse({'success': False, 'error': 'Expected non-empty JSON object'}, status=400)

#         logger.info(f"Received cache RELOAD-NOSAVE request with {len(data):,} terms")
#         success = reload_cache_nosave(data)

#         if success:
#             return JsonResponse({
#                 'success': True,
#                 'message': f'Cache reloaded with {len(data):,} terms (not saved to file yet)',
#                 'stats': get_cache_status()
#             })
#         else:
#             return JsonResponse({'success': False, 'error': 'Failed to load cache'}, status=500)

#     except Exception as e:
#         logger.exception(f"Error in reload-nosave: {e}")
#         return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def add_to_cache_nosave_view(request):
#     """Add terms to cache WITHOUT saving to file."""
#     try:
#         content_length = int(request.headers.get('Content-Length', 0))
#         if content_length > MAX_PAYLOAD_SIZE:
#             return JsonResponse({
#                 'success': False,
#                 'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
#             }, status=413)

#         try:
#             data = json.loads(request.body)
#         except json.JSONDecodeError as e:
#             return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)

#         if not isinstance(data, dict) or len(data) == 0:
#             return JsonResponse({'success': False, 'error': 'Expected non-empty JSON object'}, status=400)

#         logger.info(f"Received cache ADD-NOSAVE request with {len(data):,} terms")
#         result = add_terms_nosave(data)

#         return JsonResponse({
#             'success': True,
#             'message': f"Added {result['added']:,} terms (not saved to file yet)",
#             'stats': {
#                 'added': result['added'],
#                 'skipped': result['skipped'],
#                 'total_after': vocab_cache.term_count
#             }
#         })

#     except Exception as e:
#         logger.exception(f"Error in add-nosave: {e}")
#         return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


# @csrf_exempt
# @require_http_methods(["POST"])
# @require_api_key
# def save_cache_view(request):
#     """Save current in-memory cache to file. Call once after all chunks are uploaded."""
#     try:
#         if not vocab_cache.loaded:
#             return JsonResponse({
#                 'success': False,
#                 'error': 'Cache not loaded — nothing to save'
#             }, status=400)

#         success = save_cache()

#         if success:
#             status = get_cache_status()
#             return JsonResponse({
#                 'success': True,
#                 'message': f"Cache saved to file ({status['term_count']:,} terms). Restart gunicorn to sync all workers.",
#                 'stats': status
#             })
#         else:
#             return JsonResponse({'success': False, 'error': 'Failed to save cache to file'}, status=500)

#     except Exception as e:
#         logger.exception(f"Error saving cache: {e}")
#         return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)

"""
cache_views.py
API endpoints for vocabulary cache management.

Add to your urls.py:
    from .cache_views import (
        reload_cache_view, cache_status_view, add_to_cache_view,
        reload_cache_from_file_view, cache_test_view,
        reload_cache_nosave_view, add_to_cache_nosave_view, save_cache_view,
        upload_chunk_view, finalize_upload_view,
    )
    
    urlpatterns += [
        path('api/cache/reload/', reload_cache_view, name='cache_reload'),
        path('api/cache/reload-from-file/', reload_cache_from_file_view, name='cache_reload_file'),
        path('api/cache/status/', cache_status_view, name='cache_status'),
        path('api/cache/test/', cache_test_view, name='cache_test'),
        path('api/cache/add/', add_to_cache_view, name='cache_add'),
        path('api/cache/reload-nosave/', reload_cache_nosave_view, name='cache_reload_nosave'),
        path('api/cache/add-nosave/', add_to_cache_nosave_view, name='cache_add_nosave'),
        path('api/cache/save/', save_cache_view, name='cache_save'),
        path('api/cache/upload-chunk/', upload_chunk_view, name='cache_upload_chunk'),
        path('api/cache/finalize/', finalize_upload_view, name='cache_finalize'),
    ]
"""

import json
import logging
import hashlib
import hmac
import shutil
from functools import wraps

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.conf import settings

from .vocabulary_cache import (
    vocab_cache,
    reload_cache_from_dict,
    reload_cache_from_file,
    get_cache_status,
    add_terms_to_cache,
    add_terms_nosave,
    reload_cache_nosave,
    save_cache,
    CACHE_DIR,
    CACHE_FILE,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Secret key for API authentication (set in settings.py or environment)
CACHE_API_SECRET = getattr(settings, 'CACHE_API_SECRET', None)

# Maximum payload size (50MB should be plenty)
MAX_PAYLOAD_SIZE = 50 * 1024 * 1024

# Temp file for chunked uploads
UPLOAD_TEMP_FILE = CACHE_DIR / 'vocabulary_upload_temp.json'


# =============================================================================
# AUTHENTICATION
# =============================================================================

def verify_api_key(request):
    """Verify the API key from request header."""
    if not CACHE_API_SECRET:
        logger.warning("CACHE_API_SECRET not set - allowing unauthenticated access")
        return True
    
    auth_header = request.headers.get('Authorization', '')
    
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if hmac.compare_digest(token, CACHE_API_SECRET):
            return True
    
    api_key = request.headers.get('X-API-Key', '')
    if api_key and hmac.compare_digest(api_key, CACHE_API_SECRET):
        return True
    
    return False


def require_api_key(view_func):
    """Decorator to require API key authentication."""
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not verify_api_key(request):
            return JsonResponse({
                'success': False,
                'error': 'Unauthorized - invalid or missing API key'
            }, status=401)
        return view_func(request, *args, **kwargs)
    return wrapper


# =============================================================================
# ENDPOINTS
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def reload_cache_view(request):
    """Reload vocabulary cache from POST data (REPLACES all data)."""
    try:
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)
        
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON: {str(e)}'
            }, status=400)
        
        if not isinstance(data, dict):
            return JsonResponse({
                'success': False,
                'error': 'Expected JSON object with term data'
            }, status=400)
        
        if len(data) == 0:
            return JsonResponse({
                'success': False,
                'error': 'Empty data - nothing to load'
            }, status=400)
        
        logger.info(f"Received cache reload request with {len(data):,} terms")
        
        success = reload_cache_from_dict(data)
        
        if success:
            status = get_cache_status()
            logger.info(f"Cache reloaded successfully: {status}")
            
            return JsonResponse({
                'success': True,
                'message': 'Cache reloaded successfully',
                'stats': status
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to load cache'
            }, status=500)
    
    except Exception as e:
        logger.exception(f"Error reloading cache: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def add_to_cache_view(request):
    """Add new terms to cache WITHOUT overwriting existing ones."""
    try:
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)
        
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON: {str(e)}'
            }, status=400)
        
        if not isinstance(data, dict) or len(data) == 0:
            return JsonResponse({
                'success': False,
                'error': 'Expected non-empty JSON object with term data'
            }, status=400)
        
        logger.info(f"Received cache ADD request with {len(data):,} terms")
        
        result = add_terms_to_cache(data)
        
        return JsonResponse({
            'success': True,
            'message': f"Added {result['added']:,} new terms (skipped {result['skipped']:,} existing)",
            'stats': {
                'added': result['added'],
                'skipped': result['skipped'],
                'total_after': vocab_cache.term_count
            }
        })
    
    except Exception as e:
        logger.exception(f"Error adding to cache: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def reload_cache_from_file_view(request):
    """Reload vocabulary cache from local file."""
    try:
        success = reload_cache_from_file()
        
        if success:
            status = get_cache_status()
            return JsonResponse({
                'success': True,
                'message': 'Cache reloaded from file',
                'stats': status
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to load cache from file (file may not exist)'
            }, status=500)
    
    except Exception as e:
        logger.exception(f"Error reloading cache from file: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@require_http_methods(["GET"])
def cache_status_view(request):
    """Get current cache status."""
    try:
        status = get_cache_status()
        return JsonResponse({
            'success': True,
            'status': status
        })
    except Exception as e:
        logger.exception(f"Error getting cache status: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@require_http_methods(["GET"])
def cache_test_view(request):
    """Test cache lookups."""
    try:
        term = request.GET.get('term', '')
        query = request.GET.get('query', '')
        
        if not vocab_cache.loaded:
            return JsonResponse({
                'success': False,
                'error': 'Cache not loaded'
            }, status=503)
        
        if term:
            return JsonResponse({
                'success': True,
                'term': term,
                'is_location': vocab_cache.is_location(term),
                'is_city': vocab_cache.is_city(term),
                'is_state': vocab_cache.is_state(term),
                'metadata': vocab_cache.get_term(term)
            })
        
        if query:
            result = vocab_cache.classify_query(query)
            return JsonResponse({
                'success': True,
                'query': query,
                'result': result
            })
        
        return JsonResponse({
            'success': False,
            'error': 'Provide ?term= or ?query= parameter'
        }, status=400)
    
    except Exception as e:
        logger.exception(f"Error testing cache: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def reload_cache_nosave_view(request):
    """Reload cache from POST data WITHOUT saving to file."""
    try:
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)

        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)

        if not isinstance(data, dict) or len(data) == 0:
            return JsonResponse({'success': False, 'error': 'Expected non-empty JSON object'}, status=400)

        logger.info(f"Received cache RELOAD-NOSAVE request with {len(data):,} terms")
        success = reload_cache_nosave(data)

        if success:
            return JsonResponse({
                'success': True,
                'message': f'Cache reloaded with {len(data):,} terms (not saved to file yet)',
                'stats': get_cache_status()
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to load cache'}, status=500)

    except Exception as e:
        logger.exception(f"Error in reload-nosave: {e}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def add_to_cache_nosave_view(request):
    """Add terms to cache WITHOUT saving to file."""
    try:
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)

        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)

        if not isinstance(data, dict) or len(data) == 0:
            return JsonResponse({'success': False, 'error': 'Expected non-empty JSON object'}, status=400)

        logger.info(f"Received cache ADD-NOSAVE request with {len(data):,} terms")
        result = add_terms_nosave(data)

        return JsonResponse({
            'success': True,
            'message': f"Added {result['added']:,} terms (not saved to file yet)",
            'stats': {
                'added': result['added'],
                'skipped': result['skipped'],
                'total_after': vocab_cache.term_count
            }
        })

    except Exception as e:
        logger.exception(f"Error in add-nosave: {e}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def save_cache_view(request):
    """Save current in-memory cache to file. Call once after all chunks are uploaded."""
    try:
        if not vocab_cache.loaded:
            return JsonResponse({
                'success': False,
                'error': 'Cache not loaded — nothing to save'
            }, status=400)

        success = save_cache()

        if success:
            status = get_cache_status()
            return JsonResponse({
                'success': True,
                'message': f"Cache saved to file ({status['term_count']:,} terms). Restart gunicorn to sync all workers.",
                'stats': status
            })
        else:
            return JsonResponse({'success': False, 'error': 'Failed to save cache to file'}, status=500)

    except Exception as e:
        logger.exception(f"Error saving cache: {e}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


# =============================================================================
# FILE-BASED UPLOAD (Production-safe, works with multiple workers)
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def upload_chunk_view(request):
    """
    Upload a chunk of vocabulary data to a temp file on disk.
    No in-memory cache is touched — pure file I/O.
    Works safely with any number of workers.

    Request body:
    {
        "chunk_index": 0,
        "total_chunks": 16,
        "data": { "term:atlanta:us_city": {...}, ... }
    }
    """
    try:
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)

        try:
            body = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': f'Invalid JSON: {str(e)}'}, status=400)

        chunk_index = body.get('chunk_index', 0)
        total_chunks = body.get('total_chunks', 1)
        chunk_data = body.get('data', {})

        if not isinstance(chunk_data, dict) or len(chunk_data) == 0:
            return JsonResponse({'success': False, 'error': 'No data in chunk'}, status=400)

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if chunk_index == 0:
            # First chunk — start fresh temp file
            logger.info(f"Starting new upload: {total_chunks} chunks expected")
            with open(UPLOAD_TEMP_FILE, 'w', encoding='utf-8') as f:
                f.write('{')
                first = True
                for key, value in chunk_data.items():
                    if not first:
                        f.write(',')
                    f.write(f'{json.dumps(key, ensure_ascii=False)}:{json.dumps(value, ensure_ascii=False)}')
                    first = False
        else:
            # Subsequent chunks — append to temp file
            if not UPLOAD_TEMP_FILE.exists():
                return JsonResponse({
                    'success': False,
                    'error': 'No upload in progress. Send chunk_index=0 first.'
                }, status=400)

            with open(UPLOAD_TEMP_FILE, 'a', encoding='utf-8') as f:
                for key, value in chunk_data.items():
                    f.write(f',{json.dumps(key, ensure_ascii=False)}:{json.dumps(value, ensure_ascii=False)}')

        logger.info(f"Chunk {chunk_index + 1}/{total_chunks}: {len(chunk_data):,} terms written to temp file")

        return JsonResponse({
            'success': True,
            'message': f'Chunk {chunk_index + 1}/{total_chunks} written ({len(chunk_data):,} terms)',
            'chunk_index': chunk_index,
            'total_chunks': total_chunks,
        })

    except Exception as e:
        logger.exception(f"Error uploading chunk: {e}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def finalize_upload_view(request):
    """
    Finalize the upload: close the temp JSON file, validate it,
    move it to the cache location, and reload this worker.
    Then call /api/cache/reload-from-file/ to sync other workers.
    """
    try:
        if not UPLOAD_TEMP_FILE.exists():
            return JsonResponse({
                'success': False,
                'error': 'No upload in progress. Nothing to finalize.'
            }, status=400)

        # Close the JSON object
        with open(UPLOAD_TEMP_FILE, 'a', encoding='utf-8') as f:
            f.write('}')

        # Validate the file is valid JSON
        try:
            with open(UPLOAD_TEMP_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            term_count = len(data)
            logger.info(f"Upload finalized: {term_count:,} terms, valid JSON")
        except json.JSONDecodeError as e:
            UPLOAD_TEMP_FILE.unlink(missing_ok=True)
            return JsonResponse({
                'success': False,
                'error': f'Assembled file is not valid JSON: {str(e)}'
            }, status=500)

        # Move temp file to cache file location
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.move(str(UPLOAD_TEMP_FILE), str(CACHE_FILE))
        logger.info(f"Temp file moved to {CACHE_FILE}")

        # Reload THIS worker from the new file
        success = vocab_cache.reload_from_file()

        if success:
            status = get_cache_status()
            return JsonResponse({
                'success': True,
                'message': f'Upload finalized and cache reloaded ({status["term_count"]:,} terms). '
                           f'Call /api/cache/reload-from-file/ to sync other workers.',
                'stats': status
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'File saved but cache reload failed'
            }, status=500)

    except Exception as e:
        logger.exception(f"Error finalizing upload: {e}")
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'}, status=500)