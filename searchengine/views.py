
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
from django.utils.decorators import method_decorator
from django.template.loader import render_to_string
from django.template.exceptions import TemplateDoesNotExist
from django.db import connection
from django.http import JsonResponse
from django.views import View
from .searchapi import get_autocomplete
from decouple import config

def home(request):
    return render(request, 'home2.html')



# #########################   this is the code for the redis api. 


# def search_suggestions(request):
#     query = request.GET.get('q', '').strip()
    
#     if not query or len(query) < 2:
#         return JsonResponse({'suggestions': []})
    
#     # Get autocomplete results from Redis
#     results = get_autocomplete(prefix=query, limit=8)
    
#     # Transform to match frontend expected format
#     suggestions = []
#     for item in results:
#         suggestions.append({
#             'text': item['term'],
#             'display_text': item['display'],
#             'source_field': item.get('entity_type', ''),
#             # 'data_type': item.get('category', ''),
#             'category': item.get('category', ''),  # Add this line
#         })
    
#     return JsonResponse({'suggestions': suggestions})

def search_suggestions(request):
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


#########################   this is the code for the submission through the search bar. 

from .searchsubmission import process_search_submission


def form_submit(request):
    query = request.GET.get('query', '')
    session_id = request.GET.get('session_id', '')
    
    # Process the submission
    result = process_search_submission(query, session_id)
    
    return JsonResponse(result)

from django.http import JsonResponse
from .searchsubmission import process_search_submission
from .word_discovery import word_discovery_multi
import uuid
from django.shortcuts import render

from django.http import JsonResponse
from django.shortcuts import render
from django.core.cache import cache
from .word_discovery import word_discovery_multi
from .typesense_calculations import (
    execute_full_search,
    detect_query_intent,
    get_facets,
    get_related_searches,
    get_featured_result,
    log_search_event
)
import uuid


def search(request):
    """
    Production-quality search endpoint.
    Handles: correction, intent, filters, pagination, facets, features, logging
    """
    
    # === 1. EXTRACT ALL PARAMETERS ===
    query = request.GET.get('query', '').strip()
    session_id = request.GET.get('session_id', '') or str(uuid.uuid4())
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 20))
    
    # Explicit filters from URL
    filters = {
        'category': request.GET.get('category'),
        'source': request.GET.get('source'),
        'data_type': request.GET.get('type'),
        'time_range': request.GET.get('time'),          # 'day', 'week', 'month', 'year'
        'location': request.GET.get('location'),
        'sort': request.GET.get('sort', 'relevance'),   # 'relevance', 'date', 'rating'
    }
    # Remove None values
    filters = {k: v for k, v in filters.items() if v}
    
    # Safe search setting
    safe_search = request.GET.get('safe', 'on') == 'on'
    
    # User location (from browser or IP)
    user_lat = request.GET.get('lat')
    user_lng = request.GET.get('lng')
    user_location = (float(user_lat), float(user_lng)) if user_lat and user_lng else None
    
    # === 2. EMPTY QUERY - SHOW HOMEPAGE ===
    if not query:
        return render(request, 'results.html', {
            'query': '',
            'results': [],
            'has_results': False,
            'session_id': session_id,
            'show_trending': True,  # Show trending searches on empty query
        })
    
    # === 3. CHECK CACHE FOR REPEATED QUERIES ===
    cache_key = f"search:{query}:{page}:{hash(frozenset(filters.items()))}"
    cached_result = cache.get(cache_key)
    
    if cached_result and not filters:  # Only use cache for unfiltered searches
        cached_result['from_cache'] = True
        return render(request, 'results.html', cached_result)
    
    # === 4. WORD DISCOVERY / SPELL CORRECTION ===
    corrections, tuple_array, corrected_query = word_discovery_multi(query)
    was_corrected = query.lower() != corrected_query.lower()
    
    # Build word-by-word correction display
    word_corrections = build_word_corrections(query, corrected_query)
    
    # === 5. DETECT INTENT ===
    intent = detect_query_intent(corrected_query, tuple_array)
    
    # === 6. EXECUTE MAIN SEARCH ===
    result = execute_full_search(
        query=corrected_query,
        session_id=session_id,
        filters=filters,
        page=page,
        per_page=per_page,
        user_location=user_location,
        pos_tags=tuple_array,
        safe_search=safe_search
    )
    
    results = result.get('results', [])
    total_results = result.get('total', 0)
    
    # === 7. ZERO RESULTS HANDLING ===
    suggestions = []
    if not results:
        suggestions = handle_zero_results(
            original_query=query,
            corrected_query=corrected_query,
            filters=filters
        )
    
    # === 8. GET FACETS (Available Filters) ===
    facets = get_facets(corrected_query) if results else {}
    
    # === 9. GET RELATED SEARCHES ===
    related_searches = get_related_searches(corrected_query, intent) if results else []
    
    # === 10. GET FEATURED RESULT (Knowledge Panel / Direct Answer) ===
    featured = None
    if page == 1:  # Only on first page
        featured = get_featured_result(corrected_query, intent, results)
    
    # === 11. CATEGORIZE RESULTS BY TYPE ===
    categorized_results = categorize_results(results)
    
    # === 12. BUILD PAGINATION ===
    pagination = build_pagination(page, per_page, total_results)
    
    # === 13. LOG SEARCH EVENT ===
    log_search_event(
        query=query,
        corrected_query=corrected_query,
        session_id=session_id,
        intent=intent,
        total_results=total_results,
        filters=filters,
        page=page
    )
    
    # === 14. BUILD CONTEXT ===
    context = {
        # Query info
        'query': query,
        'corrected_query': corrected_query,
        'was_corrected': was_corrected,
        'word_corrections': word_corrections,
        'corrections': corrections,
        'intent': intent,
        
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
        'session_id': session_id,
        'search_time': result.get('search_time', 0),
        'from_cache': False,
    }
    
    # === 15. CACHE RESULTS ===
    if not filters and total_results > 0:
        cache.set(cache_key, context, timeout=300)  # 5 minutes
    
    return render(request, 'results.html', context)


# === HELPER FUNCTIONS ===

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
    Returns list of suggestion objects.
    """
    suggestions = []
    
    # Suggestion 1: Try without filters
    if filters:
        suggestions.append({
            'type': 'remove_filters',
            'message': 'Try removing some filters',
            'action_query': corrected_query,
            'action_filters': {}
        })
    
    # Suggestion 2: Try broader terms
    words = corrected_query.split()
    if len(words) > 2:
        shorter_query = ' '.join(words[:2])
        suggestions.append({
            'type': 'broader_search',
            'message': f'Try a broader search',
            'action_query': shorter_query
        })
    
    # Suggestion 3: Check spelling (if already corrected, suggest original)
    if original_query.lower() != corrected_query.lower():
        suggestions.append({
            'type': 'try_original',
            'message': f'Search for "{original_query}" instead',
            'action_query': original_query
        })
    
    # Suggestion 4: Related topics (would come from your keyword database)
    suggestions.append({
        'type': 'help',
        'message': 'Check your spelling or try different keywords'
    })
    
    return suggestions


def categorize_results(results: list) -> dict:
    """
    Groups results by type for different display treatments.
    """
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
    
    # Remove empty categories
    return {k: v for k, v in categorized.items() if v}


def build_pagination(page: int, per_page: int, total: int) -> dict:
    """
    Builds pagination info for template.
    """
    total_pages = (total + per_page - 1) // per_page
    
    # Build page range (show 5 pages around current)
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

# def search(request):
#     """Main search endpoint - renders HTML results like Google"""
#     query = request.GET.get('query', '').strip()
#     session_id = request.GET.get('session_id', '')
    
#     # Generate session_id if not provided
#     if not session_id:
#         session_id = str(uuid.uuid4())
    
#     # Empty query - show search homepage
#     if not query:
#         return render(request, 'results.html', {
#             'query': '',
#             'results': [],
#             'has_results': False,
#             'session_id': session_id,
#         })
    
#     # Run the three-pass word discovery pipeline
#     corrections, tuple_array, corrected_query = word_discovery_multi(query)
    
#     # Process the submission with the corrected query
#     result = process_search_submission(corrected_query, session_id)
    
#     # Build correction info for template
#     was_corrected = query.lower() != corrected_query.lower()
    
#     # Build word-by-word correction display
#     word_corrections = []
#     original_words = query.lower().split()
#     corrected_words = corrected_query.lower().split()
    
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
    
#     context = {
#         'query': query,
#         'corrected_query': corrected_query,
#         'was_corrected': was_corrected,
#         'word_corrections': word_corrections,
#         'corrections': corrections,
#         'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
#         'results': result.get('results', []),
#         'total_results': result.get('total', 0),
#         'has_results': len(result.get('results', [])) > 0,
#         'session_id': session_id,
#         'search_time': result.get('search_time', 0),
#     }
    
#     return render(request, 'results.html', context)


def search_api(request):
    """JSON API endpoint for programmatic access"""
    query = request.GET.get('q', '') or request.GET.get('query', '')
    session_id = request.GET.get('session_id', '')
    
    if not query:
        return JsonResponse({'error': 'No query provided'}, status=400)
    
    # Run the three-pass word discovery pipeline
    corrections, tuple_array, corrected_query = word_discovery_multi(query)
    
    # Process the submission with the corrected query
    result = process_search_submission(corrected_query, session_id)
    
    # Add word discovery info to the result
    result['word_discovery'] = {
        'original_query': query,
        'corrected_query': corrected_query,
        'corrections': corrections,
        'pos_structure': [{'position': pos, 'pos': tag} for pos, tag in tuple_array],
        'was_corrected': query.lower() != corrected_query.lower()
    }
    
    return JsonResponse(result)


# def form_submit(request):
#     query = request.GET.get('query', '')
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


