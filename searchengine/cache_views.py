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
from datetime import datetime
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
    
## =============================================================================
# ADD THIS METHOD TO vocabulary_cache.py (inside the VocabularyCache class)
# =============================================================================

def add_terms(self, data: Dict[str, Dict]) -> Dict[str, int]:
    """
    Add new terms WITHOUT clearing existing data.
    Only adds terms that don't already exist.
    
    Args:
        data: Dict with keys like "term:atlanta:us_city" and value dicts
    
    Returns:
        Dict with 'added' and 'skipped' counts
    """
    with self._lock:
        added = 0
        skipped = 0
        
        for key, value in data.items():
            # Skip if already exists
            if key in self.terms:
                skipped += 1
                continue
            
            # Add to terms dict
            self.terms[key] = value
            added += 1
            
            # Parse the key to get term and category
            term_lower = value.get('term', '').lower()
            category = value.get('category', '')
            entity_type = value.get('entity_type', 'unigram')
            
            # Add to appropriate sets/dicts based on category
            if category in ('US City', 'City'):
                self.cities.add(term_lower)
                self.locations.add(term_lower)
            elif category in ('US State', 'State'):
                self.states.add(term_lower)
                self.locations.add(term_lower)
            elif category == 'Country':
                self.locations.add(term_lower)
            
            # Add to bigrams/trigrams if applicable
            if entity_type == 'bigram' or ' ' in term_lower:
                words = term_lower.split()
                if len(words) == 2:
                    self.bigrams[term_lower] = value
                elif len(words) == 3:
                    self.trigrams[term_lower] = value
        
        # Update metadata
        self.term_count = len(self.terms)
        self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save to file (pass the full terms dict)
        self._save_to_file(self.terms)
        
        logger.info(f"Added {added:,} terms, skipped {skipped:,} existing")
        
        return {'added': added, 'skipped': skipped}


# =============================================================================
# ADD THIS IMPORT AT THE TOP OF vocabulary_cache.py (if not already there)
# =============================================================================
# from datetime import datetime


# =============================================================================
# UPDATE cache_views.py - replace the add_to_cache_view function with:
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
@require_api_key
def add_to_cache_view(request):
    """Add new terms to cache WITHOUT overwriting existing."""
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
        
        # Use the new add_terms method
        result = vocab_cache.add_terms(data)
        
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