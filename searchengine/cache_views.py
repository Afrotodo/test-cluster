"""
cache_views.py
API endpoints for vocabulary cache management.

Add to your urls.py:
    from .cache_views import reload_cache_view, cache_status_view
    
    urlpatterns += [
        path('api/cache/reload/', reload_cache_view, name='cache_reload'),
        path('api/cache/status/', cache_status_view, name='cache_status'),
    ]
"""

import json
import logging
import hashlib
import hmac
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
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Secret key for API authentication (set in settings.py or environment)
# Example: CACHE_API_SECRET = 'your-secret-key-here'
CACHE_API_SECRET = getattr(settings, 'CACHE_API_SECRET', None)

# Maximum payload size (50MB should be plenty)
MAX_PAYLOAD_SIZE = 50 * 1024 * 1024


# =============================================================================
# AUTHENTICATION
# =============================================================================

def verify_api_key(request):
    """Verify the API key from request header."""
    if not CACHE_API_SECRET:
        # If no secret configured, allow all (for development)
        logger.warning("CACHE_API_SECRET not set - allowing unauthenticated access")
        return True
    
    # Check Authorization header
    auth_header = request.headers.get('Authorization', '')
    
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if hmac.compare_digest(token, CACHE_API_SECRET):
            return True
    
    # Check X-API-Key header
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
    """
    Reload vocabulary cache from POST data.
    
    Called by Colab to update the cache.
    
    Request:
        POST /api/cache/reload/
        Headers:
            Authorization: Bearer <your-secret-key>
            Content-Type: application/json
        Body:
            {
                "term:atlanta:us_city": {"term": "atlanta", ...},
                "term:georgia:us_state": {"term": "georgia", ...},
                ...
            }
    
    Response:
        {
            "success": true,
            "message": "Cache reloaded successfully",
            "stats": {
                "term_count": 150000,
                "cities": 30000,
                "states": 50,
                ...
            }
        }
    """
    try:
        # Check content length
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)
        
        # Parse JSON body
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON: {str(e)}'
            }, status=400)
        
        # Validate data structure
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
        
        # Load into cache
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
def reload_cache_from_file_view(request):
    """
    Reload vocabulary cache from local file.
    
    Request:
        POST /api/cache/reload-from-file/
        Headers:
            Authorization: Bearer <your-secret-key>
    
    Response:
        {
            "success": true,
            "message": "Cache reloaded from file",
            "stats": {...}
        }
    """
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
    """
    Get current cache status.
    
    Request:
        GET /api/cache/status/
    
    Response:
        {
            "success": true,
            "status": {
                "loaded": true,
                "term_count": 150000,
                "cities": 30000,
                "states": 50,
                "bigrams": 5000,
                "load_time": "2.45s",
                "last_updated": "2024-01-15 10:30:00"
            }
        }
    """
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
    """
    Test cache lookups.
    
    Request:
        GET /api/cache/test/?term=atlanta
        GET /api/cache/test/?query=hbcus+in+georgia
    
    Response:
        {
            "success": true,
            "term": "atlanta",
            "is_location": true,
            "metadata": {...}
        }
    """
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
    
# =============================================================================
# ADD THIS TO cache_views.py
# =============================================================================
# This endpoint ADDS new terms without overwriting existing ones.
# Use /api/cache/reload/ for full replace, /api/cache/add/ for merge.
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def add_to_cache_view(request):
    """
    Add new terms to vocabulary cache WITHOUT overwriting existing.
    
    Called by Colab to add new terms while preserving existing data.
    
    Request:
        POST /api/cache/add/
        Headers:
            Authorization: Bearer <your-secret-key>
            Content-Type: application/json
        Body:
            {
                "term:category:dictionary_word": {"term": "category", ...},
                "term:flavor:food": {"term": "flavor", ...},
                ...
            }
    
    Response:
        {
            "success": true,
            "message": "Added 5,000 new terms (skipped 2,000 existing)",
            "stats": {
                "added": 5000,
                "skipped": 2000,
                "total_after": 229928
            }
        }
    """
    try:
        # Check content length
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length > MAX_PAYLOAD_SIZE:
            return JsonResponse({
                'success': False,
                'error': f'Payload too large (max {MAX_PAYLOAD_SIZE // 1024 // 1024}MB)'
            }, status=413)
        
        # Parse JSON body
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON: {str(e)}'
            }, status=400)
        
        # Validate data structure
        if not isinstance(data, dict):
            return JsonResponse({
                'success': False,
                'error': 'Expected JSON object with term data'
            }, status=400)
        
        if len(data) == 0:
            return JsonResponse({
                'success': False,
                'error': 'Empty data - nothing to add'
            }, status=400)
        
        logger.info(f"Received cache ADD request with {len(data):,} terms")
        
        # ADD to cache (not replace)
        added = 0
        skipped = 0
        
        for key, value in data.items():
            # Check if key exists in vocab_cache.terms
            if key in vocab_cache.terms:
                skipped += 1
            else:
                vocab_cache.terms[key] = value
                added += 1
                
                # Also add to location sets if applicable
                term_lower = value.get('term', '').lower()
                category = value.get('category', '')
                
                if category in ('US City', 'City'):
                    vocab_cache.cities.add(term_lower)
                    vocab_cache.locations.add(term_lower)
                elif category in ('US State', 'State'):
                    vocab_cache.states.add(term_lower)
                    vocab_cache.locations.add(term_lower)
                elif category == 'Country':
                    vocab_cache.locations.add(term_lower)
        
        # Update term count
        vocab_cache.term_count = len(vocab_cache.terms)
        vocab_cache.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save updated cache to file
        vocab_cache._save_to_file()
        
        # Update loaded flag if needed
        if not vocab_cache.loaded:
            vocab_cache.loaded = True
        
        logger.info(f"Cache ADD complete: added {added:,}, skipped {skipped:,}, total {vocab_cache.term_count:,}")
        
        return JsonResponse({
            'success': True,
            'message': f'Added {added:,} new terms (skipped {skipped:,} existing)',
            'stats': {
                'added': added,
                'skipped': skipped,
                'total_after': vocab_cache.term_count
            }
        })
    
    except Exception as e:
        logger.exception(f"Error adding to cache: {e}")
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)
