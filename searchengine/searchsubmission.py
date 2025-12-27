# """
# Search submission processing module.
# Handles form submissions and uses Redis lookup for term matching and spelling correction.
# """

# from typing import Dict, Any, List, Optional
# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # Import from your Redis lookup module (adjust the import path as needed)
# from .searchapi import (
#     lookup_table,
#     check_spelling,
#     get_term_metadata,
#     process_query_terms,
#     get_autocomplete
# )


# def process_search_submission(query: str, session_id: str = '') -> Dict[str, Any]:
#     """
#     Process a search form submission.
    
#     Args:
#         query: The search query from the form
#         session_id: Optional session identifier
    
#     Returns:
#         Dict containing processed results
#     """
#     result = {
#         'success': True,
#         'original_query': query,
#         'corrected_query': '',
#         'terms': [],
#         'corrections': [],
#         'session_id': session_id,
#         'error': None
#     }
    
#     if not query or not query.strip():
#         result['success'] = False
#         result['error'] = 'Empty query'
#         return result
    
#     try:
#         # Use the main lookup_table function to process the query
#         lookup_result = lookup_table(query)
        
#         if not lookup_result.get('success'):
#             result['success'] = False
#             result['error'] = lookup_result.get('error', 'Lookup failed')
#             return result
        
#         result['terms'] = lookup_result.get('terms', [])
#         result['corrected_query'] = lookup_result.get('normalized_query', query)
#         result['cache_hit'] = lookup_result.get('cache_hit', False)
        
#         # Build list of corrections made
#         for term in result['terms']:
#             if not term.get('exists') and term.get('suggestion'):
#                 result['corrections'].append({
#                     'original': term['word'],
#                     'corrected': term['suggestion'],
#                     'distance': term.get('distance', 0)
#                 })
        
#         # Print processing results to terminal
#         _print_results(result)
        
#         return result
        
#     except Exception as e:
#         result['success'] = False
#         result['error'] = str(e)
#         return result


# def _print_results(result: Dict[str, Any]) -> None:
#     """Print processing results to terminal for debugging."""
#     print("=" * 50)
#     print("SEARCH SUBMISSION PROCESSED:")
#     print(f"  Original Query: {result['original_query']}")
#     print(f"  Corrected Query: {result['corrected_query']}")
#     print(f"  Session ID: {result['session_id']}")
#     print(f"  Cache Hit: {result.get('cache_hit', False)}")
    
#     if result['corrections']:
#         print("  Corrections Made:")
#         for c in result['corrections']:
#             print(f"    '{c['original']}' → '{c['corrected']}' (distance: {c['distance']})")
    
#     if result['terms']:
#         print("  Terms:")
#         for t in result['terms']:
#             status = "✓ found" if t.get('exists') else "✗ not found"
#             print(f"    '{t['word']}' - {status}")
    
#     print("=" * 50)


#     # In searchsubmission.py
# from .word_discovery import word_discovery_multi
# from .redis_lookup import RedisLookupTable

# def process_search_submission(query: str, session_id: str = '') -> dict:
#     redis_client = RedisLookupTable.get_client()
    
#     if not redis_client:
#         return {'success': False, 'error': 'Redis connection failed'}
    
#     corrections, tuple_array, corrected_query = word_discovery_multi(
#         query,
#         redis_client,
#         prefix="afro:dictionary",
#         use_rank=True
#     )
    
#     return {
#         'success': True,
#         'original_query': query,
#         'corrected_query': corrected_query,
#         'corrections': corrections,
#         'session_id': session_id
#     }




"""
searchsubmission.py
Handles form submissions and uses word discovery for spelling correction.
"""

from typing import Dict, Any, List

from .word_discovery import word_discovery_multi
from .searchapi import RedisLookupTable


def process_search_submission(query: str, session_id: str = '') -> Dict[str, Any]:
    """
    Process a search form submission.
    
    Args:
        query: The search query from the form
        session_id: Optional session identifier
    
    Returns:
        Dict containing processed results
    """
    result = {
        'success': True,
        'original_query': query,
        'corrected_query': '',
        'corrections': [],
        'tuple_array': [],
        'session_id': session_id,
        'error': None
    }
    
    # Validate input
    if not query or not query.strip():
        result['success'] = False
        result['error'] = 'Empty query'
        return result
    
    # Get Redis client
    redis_client = RedisLookupTable.get_client()
    
    if not redis_client:
        result['success'] = False
        result['error'] = 'Redis connection failed'
        return result
    
    try:
        # Process query with word discovery
        corrections, tuple_array, corrected_query = word_discovery_multi(
            query,
            redis_client,
            prefix="afro:dictionary",
            use_rank=True
        )
        
        result['corrected_query'] = corrected_query
        result['corrections'] = corrections
        result['tuple_array'] = tuple_array
        
        # Print results for debugging
        _print_results(result)
        
        return result
        
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
        return result


def _print_results(result: Dict[str, Any]) -> None:
    """Print processing results to terminal for debugging."""
    print("=" * 50)
    print("SEARCH SUBMISSION PROCESSED:")
    print(f"  Original Query: {result['original_query']}")
    print(f"  Corrected Query: {result['corrected_query']}")
    print(f"  Session ID: {result['session_id']}")
    
    if result['corrections']:
        print("  Corrections Made:")
        for c in result['corrections']:
            if c.get('is_bigram'):
                print(f"    [BIGRAM] '{c['original']}' → '{c['corrected']}' (distance: {c['edit_distance']})")
            else:
                print(f"    [WORD]   '{c['original']}' → '{c['corrected']}' (distance: {c['edit_distance']})")
    
    print("=" * 50)