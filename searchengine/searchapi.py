


"""
lookup_table.py - Redis-based search preprocessing for Django

This module provides Redis hash and sorted set lookups for:
- Query term validation
- Spelling correction
- Autocomplete suggestions
- Query caching
"""

import json
from pyxdameraulevenshtein import damerau_levenshtein_distance
import redis
from django.conf import settings
from typing import Optional, Dict, List, Any
from decouple import config
import redis
from redis.commands.search.query import Query
from redis.commands.search.field import TextField, NumericField, TagField
import json
import string
from typing import Optional, Dict, Any, List, Set, Tuple
from decouple import config



REDIS_LOCATION=config('REDIS_LOCATION')
REDIS_DATABASE=config('REDIS_DATABASE')
REDIS_PORT=config('REDIS_PORT')
REDIS_PASSWORD=config('REDIS_PASSWORD')
REDIS_USERNAME=config('REDIS_USERNAME')
REDIS_DB=config('REDIS_DB')


# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None
    
#     @staticmethod
#     def edit_distance(s1: str, s2: str) -> int:
#         """Calculate Levenshtein edit distance between two strings"""
#         if len(s1) > len(s2):
#             s1, s2 = s2, s1
        
#         distances = range(len(s1) + 1)
#         for i2, c2 in enumerate(s2):
#             new_distances = [i2 + 1]
#             for i1, c1 in enumerate(s1):
#                 if c1 == c2:
#                     new_distances.append(distances[i1])
#                 else:
#                     new_distances.append(1 + min(
#                         distances[i1],
#                         distances[i1 + 1],
#                         new_distances[-1]
#                     ))
#             distances = new_distances
#         return distances[-1]


# def get_term_metadata(word: str) -> Optional[Dict[str, Any]]:
#     """Get metadata for a term from Redis hash"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         word_lower = word.lower().strip()
#         metadata = client.hgetall(f'term:{word_lower}')
        
#         if metadata:
#             return {
#                 'word': word_lower,
#                 'exists': True,
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def check_spelling(word: str, max_distance: int = 3) -> Dict[str, Any]:
#     """
#     Check if word is spelled correctly and find closest match if not.
    
#     Args:
#         word: The word to check
#         max_distance: Maximum edit distance for suggestions (default: 3)
    
#     Returns:
#         Dict with 'word', 'is_correct', 'suggestion', and optionally 'distance'
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_correct': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     try:
#         word_lower = word.lower().strip()
        
#         # Check if word exists in hash (correct spelling)
#         if client.exists(f'term:{word_lower}'):
#             return {
#                 'word': word,
#                 'is_correct': True,
#                 'suggestion': None
#             }
        
#         # Word is misspelled - find closest match from sorted set
#         all_words = client.zrange('autocomplete', 0, -1)
        
#         if not all_words:
#             return {
#                 'word': word,
#                 'is_correct': False,
#                 'suggestion': None
#             }
        
#         # Find closest word
#         closest_word = None
#         min_distance = float('inf')
        
#         for candidate in all_words:
#             distance = RedisLookupTable.edit_distance(word_lower, candidate)
#             if distance < min_distance:
#                 min_distance = distance
#                 closest_word = candidate
        
#         # Only suggest if within max_distance threshold
#         if closest_word and min_distance <= max_distance:
#             return {
#                 'word': word,
#                 'is_correct': False,
#                 'suggestion': closest_word,
#                 'distance': min_distance
#             }
        
#         return {
#             'word': word,
#             'is_correct': False,
#             'suggestion': None
#         }
        
#     except Exception as e:
#         return {
#             'word': word,
#             'is_correct': False,
#             'suggestion': None,
#             'error': str(e)
#         }


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix, sorted by rank.
    
#     Args:
#         prefix: The prefix to search for
#         limit: Maximum number of results (default: 10)
    
#     Returns:
#         List of matching terms with their metadata, sorted by rank (highest first)
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return []
    
#     try:
#         prefix_lower = prefix.lower().strip()
        
#         # Get matching terms from sorted set using lexicographic range
#         # Fetch more candidates than limit to allow for rank-based sorting
#         fetch_limit = max(limit * 10, 100)
        
#         matches = client.zrangebylex(
#             'autocomplete',
#             f'[{prefix_lower}',
#             f'[{prefix_lower}\xff',
#             start=0,
#             num=fetch_limit
#         )
        
#         results = []
#         for term in matches:
#             metadata = client.hgetall(f'term:{term}')
#             if metadata:
#                 results.append({
#                     'term': term,
#                     'display': metadata.get('display', term),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 })
#             else:
#                 results.append({
#                     'term': term,
#                     'display': term,
#                     'category': '',
#                     'entity_type': '',
#                     'rank': 0,
#                 })
        
#         # Sort by rank descending (highest rank first)
#         results.sort(key=lambda x: x['rank'], reverse=True)
        
#         # Apply limit after sorting
#         results = results[:limit]
        
#         # Optionally remove rank from output if you don't need it in the response
#         # for result in results:
#         #     result.pop('rank', None)
        
#         return results
        
#     except Exception as e:
#         print(f"Autocomplete error: {e}")
#         return []


# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """
#     Save query results to cache with TTL.
    
#     Args:
#         query: The search query
#         results: Results to cache
#         ttl: Time to live in seconds (default: 3600 = 1 hour)
    
#     Returns:
#         True if saved successfully, False otherwise
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# def process_query_terms(query: str) -> List[Dict[str, Any]]:
#     """
#     Process a query by checking each word in the Redis hash.
    
#     Args:
#         query: The search query string
    
#     Returns:
#         List of processed word results with metadata
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return []
    
#     try:
#         words = query.lower().split()
#         results = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
                
#             metadata = client.hgetall(f'term:{word}')
            
#             if metadata:
#                 results.append({
#                     'word': word,
#                     'exists': True,
#                     'display': metadata.get('display', word),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                 })
#             else:
#                 # Word doesn't exist - try to get spelling suggestion
#                 spelling = check_spelling(word)
#                 results.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': spelling.get('suggestion'),
#                     'distance': spelling.get('distance'),
#                 })
        
#         return results
        
#     except Exception as e:
#         print(f"Query processing error: {e}")
#         return []


# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
    
#     This function serves as the primary interface for:
#     - Processing search queries against Redis hash/set
#     - Checking query cache
#     - Getting spelling suggestions
#     - Providing autocomplete results
    
#     Args:
#         query: The search query string
#         check_cache_first: Whether to check cache before processing (default: True)
#         include_suggestions: Include spelling suggestions for unknown words (default: True)
#         autocomplete_prefix: Optional prefix for autocomplete (if provided, returns autocomplete results)
#         autocomplete_limit: Maximum autocomplete results (default: 10)
    
#     Returns:
#         Dict containing:
#         - success: bool
#         - query: original query
#         - normalized_query: processed query with known terms only
#         - terms: list of processed terms with metadata
#         - cache_hit: whether results came from cache
#         - autocomplete: list of autocomplete suggestions (if prefix provided)
#         - error: error message (if any)
    
#     Example usage in Django view:
#         from lookup_table import lookup_table
        
#         def search_api(request):
#             query = request.GET.get('q', '')
#             result = lookup_table(query)
#             return JsonResponse(result)
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache first if enabled
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         # Process query terms
#         terms = process_query_terms(query)
#         response['terms'] = terms
        
#         # Build normalized query from known terms
#         known_terms = []
#         for term in terms:
#             if term.get('exists'):
#                 known_terms.append(term['word'])
#             elif include_suggestions and term.get('suggestion'):
#                 known_terms.append(term['suggestion'])
        
#         response['normalized_query'] = ' '.join(known_terms)
        
#         # Cache the processed result
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # Convenience functions for direct imports
# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     result = lookup_table('', autocomplete_prefix=prefix, autocomplete_limit=limit)
#     return result.get('autocomplete', [])


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return check_spelling(word)



# """
# redis_lookup.py
# Redis-based lookup table for search preprocessing with tiered suggestions.
# """

# import redis
# import json
# import string
# from typing import Dict, List, Optional, Any

# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # Import your Redis config (adjust as needed)
# from django.conf import settings



# # =============================================================================
# # REDIS CLIENT
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # SMART CANDIDATE GENERATION
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 50) -> set:
#     """
#     Generate spelling candidates based on common typo patterns.
    
#     Strategies:
#     1. Keyboard proximity substitutions
#     2. Character transpositions
#     3. Character deletions
#     4. Double letter removal
#     5. Character insertions
    
#     Args:
#         word: The potentially misspelled word
#         max_candidates: Maximum candidates to generate
    
#     Returns:
#         Set of candidate words
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
#     alphabet = string.ascii_lowercase

#     # QWERTY keyboard proximity map
#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 4. Double letter removal
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 5. Single character insertion
#     for i in range(length + 1):
#         for char in alphabet:
#             candidate = word_lower[:i] + char + word_lower[i:]
#             candidates.add(candidate)
#             if len(candidates) >= max_candidates * 2:
#                 break
#         if len(candidates) >= max_candidates * 2:
#             break

#     return set(list(candidates)[:max_candidates])


# # =============================================================================
# # CORE FUNCTIONS
# # =============================================================================

# def get_term_metadata(word: str) -> Optional[Dict[str, Any]]:
#     """Get metadata for a term from Redis hash"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         word_lower = word.lower().strip()
#         metadata = client.hgetall(f'term:{word_lower}')
        
#         if metadata:
#             return {
#                 'word': word_lower,
#                 'exists': True,
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """
#     Get top N words by rank/score from sorted set.
    
#     Args:
#         limit: Number of top words to retrieve
    
#     Returns:
#         List of words with their scores
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         # Get top words by score (highest first)
#         top_words = client.zrevrange('autocomplete', 0, limit - 1, withscores=True)
        
#         results = []
#         for word, score in top_words:
#             results.append({
#                 'term': word,
#                 'score': int(score)
#             })
        
#         return results
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# def batch_check_candidates(candidates: set) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis and get their metadata.
    
#     Args:
#         candidates: Set of candidate words to check
    
#     Returns:
#         List of found words with metadata
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not candidates:
#         return []
    
#     try:
#         pipeline = client.pipeline()
#         candidate_list = list(candidates)
        
#         # Check all candidates at once
#         for candidate in candidate_list:
#             pipeline.hgetall(f'term:{candidate}')
        
#         results = pipeline.execute()
        
#         found = []
#         for candidate, metadata in zip(candidate_list, results):
#             if metadata:
#                 found.append({
#                     'term': candidate,
#                     'display': metadata.get('display', candidate),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 })
        
#         return found
        
#     except Exception as e:
#         print(f"Batch check error: {e}")
#         return []


# # =============================================================================
# # TIERED SUGGESTION SYSTEM
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     fallback_pool_size: int = 200
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function with tiered approach.
    
#     Tier 1: Prefix match (exact typing) - includes exact match + continuations
#     Tier 2: Smart candidates (common typos)
#     Tier 3: Top N fallback (rare cases)
    
#     Args:
#         input_text: The user's input (single word)
#         limit: Maximum suggestions to return
#         max_distance: Maximum edit distance for corrections
#         fallback_pool_size: Number of top words to check in fallback
    
#     Returns:
#         Dict with suggestions and metadata
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: PREFIX MATCH (includes exact match) ===
#         prefix_matches = client.zrangebylex(
#             'autocomplete',
#             f'[{input_lower}',
#             f'[{input_lower}\xff',
#             start=0,
#             num=limit * 10
#         )
        
#         if prefix_matches:
#             response['tier_used'] = 'prefix'
            
#             # Check if exact match exists
#             if input_lower in prefix_matches:
#                 response['exact_match'] = True
            
#             # Get metadata for all matches
#             results = []
#             pipeline = client.pipeline()
#             for term in prefix_matches:
#                 pipeline.hgetall(f'term:{term}')
            
#             metadata_list = pipeline.execute()
            
#             for term, metadata in zip(prefix_matches, metadata_list):
#                 if metadata:
#                     is_exact = (term == input_lower)
#                     results.append({
#                         'term': term,
#                         'display': metadata.get('display', term),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': int(metadata.get('rank', 0)),
#                         'distance': 0,
#                         'is_exact': is_exact
#                     })
            
#             # Sort: exact match first, then by rank descending
#             results.sort(key=lambda x: (not x['is_exact'], -x['rank']))
            
#             # Remove is_exact flag from output (internal use only)
#             for item in results:
#                 item.pop('is_exact', None)
            
#             response['suggestions'] = results[:limit]
#             return response
        
#         # === TIER 2: SMART CANDIDATES ===
#         candidates = generate_candidates_smart(input_lower, max_candidates=50)
#         found_candidates = batch_check_candidates(candidates)
        
#         if found_candidates:
#             response['tier_used'] = 'smart_candidates'
            
#             # Calculate distances and filter
#             results = []
#             for item in found_candidates:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 if distance <= max_distance:
#                     item['distance'] = distance
#                     results.append(item)
            
#             # Sort by distance first, then by rank (higher is better)
#             results.sort(key=lambda x: (x['distance'], -x['rank']))
#             response['suggestions'] = results[:limit]
            
#             if results:
#                 return response
        
#         # === TIER 3: TOP N FALLBACK ===
#         response['tier_used'] = 'fallback'
        
#         top_words = get_top_words_by_rank(limit=fallback_pool_size)
        
#         if not top_words:
#             response['suggestions'] = []
#             return response
        
#         # Get metadata and calculate distances
#         results = []
#         pipeline = client.pipeline()
#         for item in top_words:
#             pipeline.hgetall(f'term:{item["term"]}')
        
#         metadata_list = pipeline.execute()
        
#         for item, metadata in zip(top_words, metadata_list):
#             distance = damerau_levenshtein_distance(input_lower, item['term'])
#             if distance <= max_distance and metadata:
#                 results.append({
#                     'term': item['term'],
#                     'display': metadata.get('display', item['term']),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                     'distance': distance
#                 })
        
#         # Sort by distance first, then by rank
#         results.sort(key=lambda x: (x['distance'], -x['rank']))
#         response['suggestions'] = results[:limit]
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix.
#     Now uses the unified get_suggestions function.
    
#     Args:
#         prefix: The prefix to search for
#         limit: Maximum number of results
    
#     Returns:
#         List of matching terms with metadata
#     """
#     result = get_suggestions(prefix, limit=limit)
#     return result.get('suggestions', [])


# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     Called when user presses space or completes a word.
    
#     Args:
#         word: The word to validate
    
#     Returns:
#         Dict with validation result and suggestion
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     word_lower = word.lower().strip()
    
#     # Check if word exists
#     if client.exists(f'term:{word_lower}'):
#         metadata = client.hgetall(f'term:{word_lower}')
#         return {
#             'word': word,
#             'is_valid': True,
#             'suggestion': None,
#             'metadata': {
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         }
    
#     # Word not found - get suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best['distance'],
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best['display'],
#                 'category': best['category'],
#                 'rank': best['rank'],
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """
#     Save query results to cache with TTL.
    
#     Args:
#         query: The search query
#         results: Results to cache
#         ttl: Time to live in seconds (default: 1 hour)
    
#     Returns:
#         True if saved successfully
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
    
#     Args:
#         query: The search query string
#         check_cache_first: Whether to check cache before processing
#         include_suggestions: Include spelling suggestions for unknown words
#         autocomplete_prefix: Optional prefix for autocomplete
#         autocomplete_limit: Maximum autocomplete results
    
#     Returns:
#         Dict with processed results
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache first if enabled
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         # Process each word in the query
#         words = query.lower().split()
#         terms = []
#         normalized_words = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
            
#             validation = validate_word(word)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'tier_used': validation.get('tier_used'),
#                 })
                
#                 # Use suggestion if available
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # Cache the processed result
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return validate_word(word)

# """
# redis_lookup.py
# Redis-based lookup table for search preprocessing with tiered suggestions.
# """

# import redis
# import json
# import string
# from typing import Dict, List, Optional, Any, Set

# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # Import your Redis config (adjust as needed)
# from django.conf import settings


# =============================================================================
# REDIS CONFIGURATION
# =============================================================================

# REDIS_LOCATION = getattr(settings, 'REDIS_LOCATION', 'localhost')
# REDIS_PORT = getattr(settings, 'REDIS_PORT', 6379)
# REDIS_DB = getattr(settings, 'REDIS_DB', 0)
# REDIS_PASSWORD = getattr(settings, 'REDIS_PASSWORD', None)
# REDIS_USERNAME = getattr(settings, 'REDIS_USERNAME', None)


# # =============================================================================
# # REDIS CLIENT
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # SMART CANDIDATE GENERATION (IMPROVED)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
    
#     Strategies:
#     1. Keyboard proximity substitutions
#     2. Character transpositions (single and double)
#     3. Character deletions
#     4. Double letter removal
#     5. Character insertions
#     6. Vowel substitutions
#     7. Common letter confusions
    
#     Args:
#         word: The potentially misspelled word
#         max_candidates: Maximum candidates to generate
    
#     Returns:
#         Set of candidate words
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     # Don't process very short words
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     # QWERTY keyboard proximity map
#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }
    
#     # Common letter confusions
#     letter_confusions = {
#         'a': 'eo', 'e': 'ai', 'i': 'ey', 'o': 'au', 'u': 'o',
#         'c': 'ks', 's': 'cz', 'k': 'c', 'z': 's',
#         'f': 'ph', 'ph': 'f', 'gh': 'f',
#         'j': 'g', 'g': 'j',
#     }

#     # 1. Keyboard proximity substitutions - O(n * avg_proximity)
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition - O(n)
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Double transposition (swap two pairs) - O(n^2) but limited by word length
#     for i in range(length - 1):
#         # First swap
#         first_swap = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         # Second swap on result
#         for j in range(len(first_swap) - 1):
#             if j != i and j != i - 1:  # Don't undo the first swap
#                 candidate = first_swap[:j] + first_swap[j+1] + first_swap[j] + first_swap[j+2:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#         if len(candidates) >= max_candidates:
#             break

#     # 4. Single character deletion - O(n)
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 5. Double letter removal - O(n)
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 6. Vowel substitution - O(n * 5)
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 7. Common letter confusion substitutions - O(n * avg_confusions)
#     for i in range(length):
#         char = word_lower[i]
#         if char in letter_confusions:
#             for confused_char in letter_confusions[char]:
#                 candidate = word_lower[:i] + confused_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 8. Single character insertion (limited) - O(n * 26) but capped
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     # 9. Missing double letter - O(n * 26)
#     if len(candidates) < max_candidates:
#         for i in range(length):
#             # Double the current character
#             candidate = word_lower[:i] + word_lower[i] + word_lower[i:]
#             candidates.add(candidate)

#     return set(list(candidates)[:max_candidates])


# # =============================================================================
# # CORE FUNCTIONS (OPTIMIZED)
# # =============================================================================

# def get_term_metadata(word: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash - O(1)
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         word_lower = word.lower().strip()
#         metadata = client.hgetall(f'term:{word_lower}')  # O(1) hash lookup
        
#         if metadata:
#             return {
#                 'word': word_lower,
#                 'exists': True,
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'subtext': metadata.get('subtext', ''),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """
#     Get top N words by rank/score from sorted set - O(log n + limit)
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         # O(log n + limit) for sorted set range query
#         top_words = client.zrevrange('autocomplete', 0, limit - 1, withscores=True)
        
#         results = []
#         for word, score in top_words:
#             results.append({
#                 'term': word,
#                 'score': int(score)
#             })
        
#         return results
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis - O(k) where k = number of candidates
#     Uses pipeline for efficiency (single round trip)
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not candidates:
#         return []
    
#     try:
#         pipeline = client.pipeline()
#         candidate_list = list(candidates)
        
#         # Batch all hash lookups - single network round trip
#         for candidate in candidate_list:
#             pipeline.hgetall(f'term:{candidate}')
        
#         results = pipeline.execute()  # O(k) operations in one call
        
#         found = []
#         for candidate, metadata in zip(candidate_list, results):
#             if metadata:
#                 found.append({
#                     'term': candidate,
#                     'display': metadata.get('display', candidate),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'subtext': metadata.get('subtext', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 })
        
#         return found
        
#     except Exception as e:
#         print(f"Batch check error: {e}")
#         return []


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[str]:
#     """
#     Get words matching a prefix from sorted set - O(log n + m)
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         # O(log n + m) where m is number of matches
#         matches = client.zrangebylex(
#             'autocomplete',
#             f'[{prefix}',
#             f'[{prefix}\xff',
#             start=0,
#             num=limit
#         )
#         return matches
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# # =============================================================================
# # SCORING FUNCTION
# # =============================================================================

# def calculate_score(distance: int, rank: int, max_rank: int = 10000) -> float:
#     """
#     Calculate a combined score based on edit distance and word rank.
#     Lower score = better match.
    
#     Formula: score = distance - (rank / max_rank)
    
#     This way:
#     - Distance is the primary factor
#     - Rank breaks ties (higher rank = lower score = better)
    
#     Args:
#         distance: Damerau-Levenshtein distance
#         rank: Word frequency rank (higher = more common)
#         max_rank: Maximum rank for normalization
    
#     Returns:
#         Combined score (lower is better)
#     """
#     # Normalize rank to 0-1 range, then subtract from distance
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # TIERED SUGGESTION SYSTEM (OPTIMIZED)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     fallback_pool_size: int = 200
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function with tiered approach - Near O(1) average case
    
#     Tier 1: Exact match check - O(1)
#     Tier 2: Prefix match - O(log n + m)
#     Tier 3: Smart candidates - O(k) where k = candidates
#     Tier 4: Prefix fuzzy (first 2-3 chars) - O(log n + m)
#     Tier 5: Top N fallback - O(log n + limit)
    
#     Args:
#         input_text: The user's input (single word)
#         limit: Maximum suggestions to return
#         max_distance: Maximum edit distance for corrections
#         fallback_pool_size: Number of top words to check in fallback
    
#     Returns:
#         Dict with suggestions and metadata
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH - O(1) ===
#         exact_metadata = client.hgetall(f'term:{input_lower}')
#         if exact_metadata:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
#             response['suggestions'] = [{
#                 'term': input_lower,
#                 'display': exact_metadata.get('display', input_lower),
#                 'pos': exact_metadata.get('pos', 'unknown'),
#                 'category': exact_metadata.get('category', ''),
#                 'entity_type': exact_metadata.get('entity_type', ''),
#                 'rank': int(exact_metadata.get('rank', 0)),
#                 'distance': 0,
#                 'score': 0
#             }]
#             return response
        
#         # === TIER 2: PREFIX MATCH - O(log n + m) ===
#         prefix_matches = get_prefix_matches(input_lower, limit=limit * 5)
        
#         if prefix_matches:
#             response['tier_used'] = 'prefix'
            
#             # Batch get metadata - O(m)
#             pipeline = client.pipeline()
#             for term in prefix_matches:
#                 pipeline.hgetall(f'term:{term}')
            
#             metadata_list = pipeline.execute()
            
#             results = []
#             for term, metadata in zip(prefix_matches, metadata_list):
#                 if metadata:
#                     distance = damerau_levenshtein_distance(input_lower, term)
#                     rank = int(metadata.get('rank', 0))
#                     score = calculate_score(distance, rank)
                    
#                     results.append({
#                         'term': term,
#                         'display': metadata.get('display', term),
#                         'pos': metadata.get('pos', 'unknown'),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': rank,
#                         'distance': distance,
#                         'score': score
#                     })
            
#             if results:
#                 # Sort by score (lower is better)
#                 results.sort(key=lambda x: x['score'])
#                 response['suggestions'] = results[:limit]
#                 return response
        
#         # === TIER 3: SMART CANDIDATES - O(k) ===
#         candidates = generate_candidates_smart(input_lower, max_candidates=100)
#         found_candidates = batch_check_candidates(candidates)
        
#         if found_candidates:
#             response['tier_used'] = 'smart_candidates'
            
#             results = []
#             for item in found_candidates:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 if distance <= max_distance:
#                     rank = item.get('rank', 0)
#                     score = calculate_score(distance, rank)
#                     item['distance'] = distance
#                     item['score'] = score
#                     results.append(item)
            
#             if results:
#                 # Sort by score (lower is better)
#                 results.sort(key=lambda x: x['score'])
#                 response['suggestions'] = results[:limit]
#                 return response
        
#         # === TIER 4: PREFIX FUZZY - O(log n + m) ===
#         # Try matching first 2-3 characters to find similar words
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 fuzzy_prefix = input_lower[:prefix_len]
#                 fuzzy_matches = get_prefix_matches(fuzzy_prefix, limit=100)
                
#                 if fuzzy_matches:
#                     response['tier_used'] = f'prefix_fuzzy_{prefix_len}'
                    
#                     # Batch get metadata
#                     pipeline = client.pipeline()
#                     for term in fuzzy_matches:
#                         pipeline.hgetall(f'term:{term}')
                    
#                     metadata_list = pipeline.execute()
                    
#                     results = []
#                     for term, metadata in zip(fuzzy_matches, metadata_list):
#                         if metadata:
#                             distance = damerau_levenshtein_distance(input_lower, term)
#                             if distance <= max_distance:
#                                 rank = int(metadata.get('rank', 0))
#                                 score = calculate_score(distance, rank)
                                
#                                 results.append({
#                                     'term': term,
#                                     'display': metadata.get('display', term),
#                                     'pos': metadata.get('pos', 'unknown'),
#                                     'category': metadata.get('category', ''),
#                                     'entity_type': metadata.get('entity_type', ''),
#                                     'rank': rank,
#                                     'distance': distance,
#                                     'score': score
#                                 })
                    
#                     if results:
#                         results.sort(key=lambda x: x['score'])
#                         response['suggestions'] = results[:limit]
#                         return response
        
#         # === TIER 5: TOP N FALLBACK - O(log n + fallback_pool_size) ===
#         response['tier_used'] = 'fallback'
        
#         top_words = get_top_words_by_rank(limit=fallback_pool_size)
        
#         if not top_words:
#             response['suggestions'] = []
#             return response
        
#         # Batch get metadata
#         pipeline = client.pipeline()
#         for item in top_words:
#             pipeline.hgetall(f'term:{item["term"]}')
        
#         metadata_list = pipeline.execute()
        
#         results = []
#         for item, metadata in zip(top_words, metadata_list):
#             if metadata:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 if distance <= max_distance:
#                     rank = int(metadata.get('rank', 0))
#                     score = calculate_score(distance, rank)
                    
#                     results.append({
#                         'term': item['term'],
#                         'display': metadata.get('display', item['term']),
#                         'pos': metadata.get('pos', 'unknown'),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': rank,
#                         'distance': distance,
#                         'score': score
#                     })
        
#         results.sort(key=lambda x: x['score'])
#         response['suggestions'] = results[:limit]
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix - O(log n + limit)
#     """
#     result = get_suggestions(prefix, limit=limit)
#     return result.get('suggestions', [])


# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed - O(1) best case
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     word_lower = word.lower().strip()
    
#     # O(1) hash existence check
#     if client.exists(f'term:{word_lower}'):
#         metadata = client.hgetall(f'term:{word_lower}')  # O(1)
#         return {
#             'word': word,
#             'is_valid': True,
#             'suggestion': None,
#             'metadata': {
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         }
    
#     # Word not found - get suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best['distance'],
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best['display'],
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best['category'],
#                 'rank': best['rank'],
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # CACHE FUNCTIONS - O(1)
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache - O(1)"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)  # O(1)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """
#     Save query results to cache with TTL - O(1)
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))  # O(1)
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     Average case: O(1) with cache hit, O(k) without cache
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache first - O(1)
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         # Process each word in the query
#         words = query.lower().split()
#         terms = []
#         normalized_words = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
            
#             validation = validate_word(word)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                 })
                
#                 # Use suggestion if available
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # Cache the processed result - O(1)
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return validate_word(word)











#-----------------------------------------------------------------   Part 2 ---------------------------------------------------------- #




# # =============================================================================
# # REDIS CLIENT
# # =============================================================================

# import redis
# import json
# import string
# from typing import Optional, Dict, Any, List, Set

# # Your existing config imports would go here
# # from django.conf import settings
# # REDIS_LOCATION = settings.REDIS_LOCATION
# # etc.





# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # SORTED SET CONFIGURATION
# # =============================================================================

# # All sorted sets to search (in priority order)
# SORTED_SETS = [
#     'autocomplete:continent',
#     'autocomplete:country',
#     'autocomplete:us_state',
#     'autocomplete:state',
#     'autocomplete:us_city',
#     'autocomplete:city',
#     'autocomplete:word'
# ]

# # Category suffixes used in keys
# CATEGORY_SUFFIXES = [
#     'continent', 'country', 'us_state', 'state_province', 
#     'us_city', 'city', 'word'
# ]




# # =============================================================================
# # HELPER FUNCTIONS FOR NEW KEY STRUCTURE
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """
#     Extract the base term from a member key.
    
#     Examples:
#         'africa:continent' -> 'africa'
#         'paris:city:fr' -> 'paris'
#         'new york:us_city:ny' -> 'new york'
#         'happy:word' -> 'happy'
#     """
#     if ':' not in member:
#         return member
    
#     parts = member.split(':')
    
#     # Find where the category suffix starts
#     for i, part in enumerate(parts):
#         if part in CATEGORY_SUFFIXES:
#             # Return everything before this part
#             return ':'.join(parts[:i]) if i > 0 else parts[0]
    
#     # If no category suffix found, return first part
#     return parts[0]


# def get_hash_key(member: str) -> str:
#     """
#     Get the hash key for a sorted set member.
    
#     Example: 'africa:continent' -> 'term:africa:continent'
#     """
#     return f'term:{member}'


# # =============================================================================
# # SMART CANDIDATE GENERATION (IMPROVED)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }
    
#     letter_confusions = {
#         'a': 'eo', 'e': 'ai', 'i': 'ey', 'o': 'au', 'u': 'o',
#         'c': 'ks', 's': 'cz', 'k': 'c', 'z': 's',
#         'f': 'ph', 'j': 'g', 'g': 'j',
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Double transposition
#     for i in range(length - 1):
#         first_swap = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         for j in range(len(first_swap) - 1):
#             if j != i and j != i - 1:
#                 candidate = first_swap[:j] + first_swap[j+1] + first_swap[j] + first_swap[j+2:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#         if len(candidates) >= max_candidates:
#             break

#     # 4. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 5. Double letter removal
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 6. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 7. Common letter confusion substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in letter_confusions:
#             for confused_char in letter_confusions[char]:
#                 candidate = word_lower[:i] + confused_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 8. Single character insertion (limited)
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     # 9. Missing double letter
#     if len(candidates) < max_candidates:
#         for i in range(length):
#             candidate = word_lower[:i] + word_lower[i] + word_lower[i:]
#             candidates.add(candidate)

#     return set(list(candidates)[:max_candidates])


# # =============================================================================
# # CORE FUNCTIONS (UPDATED FOR NEW KEY STRUCTURE)
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash - O(1)
    
#     Args:
#         member: The full member key (e.g., 'africa:continent')
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         hash_key = get_hash_key(member)
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'member': member,
#                 'term': base_term,
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[str]:
#     """
#     Find all members in sorted sets that match the exact term.
#     Uses ZRANGEBYLEX for exact prefix match.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     matches = []
#     term_lower = term.lower()
    
#     for ss in SORTED_SETS:
#         try:
#             # Exact match: look for term or term:* pattern
#             # First check exact term
#             score = client.zscore(ss, term_lower)
#             if score is not None:
#                 matches.append(term_lower)
            
#             # Then check term:suffix patterns (if using compound keys)
#             members = client.zrangebylex(ss, f'[{term_lower}:', f'[{term_lower}:\xff')
#             matches.extend(members)
            
#         except Exception as e:
#             continue
    
#     return list(set(matches))


# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """
#     Get top words by rank from hash data.
#     Since sorted set scores are 0, we fetch from hash and sort.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         results = []
#         per_set_limit = max(limit // len(SORTED_SETS), 30)
        
#         for ss in SORTED_SETS:
#             try:
#                 # Get some members
#                 members = client.zrange(ss, 0, per_set_limit - 1)
                
#                 # Fetch their ranks from hash
#                 if members:
#                     pipeline = client.pipeline()
#                     for member in members:
#                         pipeline.hget(f'term:{member}', 'rank')
                    
#                     ranks = pipeline.execute()
                    
#                     for member, rank in zip(members, ranks):
#                         try:
#                             rank_val = int(float(rank)) if rank else 0
#                         except:
#                             rank_val = 0
                        
#                         results.append({
#                             'member': member,
#                             'term': member,
#                             'rank': rank_val
#                         })
#             except:
#                 continue
        
#         # Sort by rank descending
#         results.sort(key=lambda x: x['rank'], reverse=True)
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis - O(k)
    
#     Updated to search across all sorted sets for each candidate.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not candidates:
#         return []
    
#     try:
#         found = []
        
#         for candidate in candidates:
#             # Find all matches for this candidate across sorted sets
#             matches = get_exact_term_matches(candidate)
            
#             if matches:
#                 # Get metadata for first match
#                 pipeline = client.pipeline()
#                 for member in matches:
#                     pipeline.hgetall(get_hash_key(member))
                
#                 metadata_list = pipeline.execute()
                
#                 for member, metadata in zip(matches, metadata_list):
#                     if metadata:
#                         base_term = extract_base_term(member)
#                         found.append({
#                             'member': member,
#                             'term': base_term,
#                             'display': metadata.get('display', base_term),
#                             'pos': metadata.get('pos', 'unknown'),
#                             'category': metadata.get('category', ''),
#                             'entity_type': metadata.get('entity_type', ''),
#                             'rank': int(metadata.get('rank', 0)),
#                         })
        
#         return found
        
#     except Exception as e:
#         print(f"Batch check error: {e}")
#         return []



# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches and fetch rank from hash, then sort by rank.
#     This is the main function for autocomplete.
    
#     Flow:
#     1. ZRANGEBYLEX prefix search (fast, ~2ms)
#     2. Pipeline fetch metadata from hash (~3ms)
#     3. Sort by rank (in memory, <1ms)
    
#     Total: ~5-10ms
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         # Step 1: Get prefix matches from sorted sets
#         matches = get_prefix_matches(prefix, limit=limit * 5)
        
#         if not matches:
#             return []
        
#         # Step 2: Batch fetch metadata from hash
#         pipeline = client.pipeline()
#         for member in matches:
#             pipeline.hgetall(f'term:{member}')
        
#         metadata_list = pipeline.execute()
        
#         # Step 3: Build results with rank
#         results = []
#         for member, metadata in zip(matches, metadata_list):
#             if metadata:
#                 try:
#                     rank = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank = 0
                
#                 results.append({
#                     'member': member,
#                     'term': member,  # For display
#                     'display': metadata.get('display', member),
#                     'pos': metadata.get('pos', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'description': metadata.get('description', ''),
#                     'rank': rank
#                 })
        
#         # Step 4: Sort by rank (highest first)
#         results.sort(key=lambda x: x['rank'], reverse=True)
        
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Prefix match with rank error: {e}")
#         return []
    

# def get_prefix_matches(prefix: str, limit: int = 50) -> List[str]:
#     """
#     Get members from all sorted sets that start with the given prefix.
#     Uses ZRANGEBYLEX (fast) - works because all scores = 0
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         matches = []
#         per_set_limit = max(limit // len(SORTED_SETS), 15)
        
#         for ss in SORTED_SETS:
#             try:
#                 # ZRANGEBYLEX works fast when all scores = 0
#                 members = client.zrangebylex(
#                     ss,
#                     f'[{prefix.lower()}',
#                     f'[{prefix.lower()}\xff',
#                     start=0,
#                     num=per_set_limit
#                 )
#                 matches.extend(members)
#             except Exception as e:
#                 continue
        
#         return matches[:limit * 2]  # Return more, we'll sort and trim later
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# # =============================================================================
# # SCORING FUNCTION
# # =============================================================================

# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """
#     Calculate a combined score based on edit distance and word rank.
#     Lower score = better match.
#     """
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """
#     Calculate the Damerau-Levenshtein distance between two strings.
#     """
#     len1, len2 = len(s1), len(s2)
    
#     # Create distance matrix
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     # Initialize base cases
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     # Fill in the rest of the matrix
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,      # deletion
#                 d[i][j-1] + 1,      # insertion
#                 d[i-1][j-1] + cost  # substitution
#             )
            
#             # Transposition
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# # =============================================================================
# # TIERED SUGGESTION SYSTEM (UPDATED)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     fallback_pool_size: int = 200
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function - UPDATED for score=0 sorted sets.
#     Rank comes from hash, not sorted set score.
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             pipeline = client.pipeline()
#             for member in exact_matches:
#                 pipeline.hgetall(f'term:{member}')
            
#             metadata_list = pipeline.execute()
            
#             results = []
#             for member, metadata in zip(exact_matches, metadata_list):
#                 if metadata:
#                     try:
#                         rank = int(float(metadata.get('rank', 0)))
#                     except:
#                         rank = 0
                    
#                     results.append({
#                         'member': member,
#                         'term': member,
#                         'display': metadata.get('display', member),
#                         'pos': metadata.get('pos', ''),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': rank,
#                         'distance': 0,
#                         'score': -rank  # Lower score = better (negative rank)
#                     })
            
#             if results:
#                 results.sort(key=lambda x: x['rank'], reverse=True)
#                 response['suggestions'] = results[:limit]
#                 return response
        
#         # === TIER 2: PREFIX MATCH (FAST with score=0) ===
#         prefix_results = get_prefix_matches_with_rank(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             results = []
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 score = distance - (item['rank'] / 10000000)  # Lower = better
                
#                 results.append({
#                     **item,
#                     'distance': distance,
#                     'score': score
#                 })
            
#             # Sort by score (lower is better)
#             results.sort(key=lambda x: x['score'])
#             response['suggestions'] = results[:limit]
#             return response
        
#         # === TIER 3: SMART CANDIDATES ===
#         candidates = generate_candidates_smart(input_lower, max_candidates=100)
        
#         if candidates:
#             response['tier_used'] = 'smart_candidates'
            
#             # Check which candidates exist
#             found = []
#             pipeline = client.pipeline()
#             candidate_list = list(candidates)
            
#             for candidate in candidate_list:
#                 pipeline.exists(f'term:{candidate}')
            
#             exists_list = pipeline.execute()
            
#             existing_candidates = [c for c, e in zip(candidate_list, exists_list) if e]
            
#             if existing_candidates:
#                 # Fetch metadata
#                 pipeline = client.pipeline()
#                 for candidate in existing_candidates:
#                     pipeline.hgetall(f'term:{candidate}')
                
#                 metadata_list = pipeline.execute()
                
#                 results = []
#                 for candidate, metadata in zip(existing_candidates, metadata_list):
#                     if metadata:
#                         distance = damerau_levenshtein_distance(input_lower, candidate)
#                         if distance <= max_distance:
#                             try:
#                                 rank = int(float(metadata.get('rank', 0)))
#                             except:
#                                 rank = 0
                            
#                             score = distance - (rank / 10000000)
                            
#                             results.append({
#                                 'member': candidate,
#                                 'term': candidate,
#                                 'display': metadata.get('display', candidate),
#                                 'pos': metadata.get('pos', ''),
#                                 'category': metadata.get('category', ''),
#                                 'entity_type': metadata.get('entity_type', ''),
#                                 'rank': rank,
#                                 'distance': distance,
#                                 'score': score
#                             })
                
#                 if results:
#                     results.sort(key=lambda x: x['score'])
#                     response['suggestions'] = results[:limit]
#                     return response
        
#         # === TIER 4: PREFIX FUZZY ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 fuzzy_prefix = input_lower[:prefix_len]
#                 fuzzy_results = get_prefix_matches_with_rank(fuzzy_prefix, limit=100)
                
#                 if fuzzy_results:
#                     response['tier_used'] = f'prefix_fuzzy_{prefix_len}'
                    
#                     results = []
#                     for item in fuzzy_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'])
#                         if distance <= max_distance:
#                             score = distance - (item['rank'] / 10000000)
#                             results.append({
#                                 **item,
#                                 'distance': distance,
#                                 'score': score
#                             })
                    
#                     if results:
#                         results.sort(key=lambda x: x['score'])
#                         response['suggestions'] = results[:limit]
#                         return response
        
#         # === TIER 5: FALLBACK ===
#         response['tier_used'] = 'fallback'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix.
#     """
#     result = get_suggestions(prefix, limit=limit)
#     return result.get('suggestions', [])


# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     word_lower = word.lower().strip()
    
#     # Check for exact matches with new key structure
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         member = exact_matches[0]
#         metadata = client.hgetall(get_hash_key(member))
        
#         if metadata:
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 }
#             }
    
#     # Word not found - get suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best['distance'],
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best['display'],
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best['category'],
#                 'rank': best['rank'],
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache with TTL"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
#         terms = []
#         normalized_words = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
            
#             validation = validate_word(word)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return validate_word(word)


# # =============================================================================
# # DJANGO VIEW (UPDATED)
# # =============================================================================

# from django.http import JsonResponse

# def search_suggestions(request):
#     query = request.GET.get('q', '').strip()
    
#     if not query or len(query) < 2:
#         return JsonResponse({'suggestions': []})
    
#     # Get autocomplete results from Redis
#     results = get_autocomplete(prefix=query, limit=8)
    
#     # Transform to match frontend expected format
#     suggestions = []
#     for item in results:
#         category = item.get('category', '')
        
#         # Hide "Dictionary Word" label - show empty string instead
#         display_category = '' if category == 'Dictionary Word' else category
        
#         suggestions.append({
#             'text': item['term'],
#             'display_text': item['display'],
#             'source_field': item.get('entity_type', ''),
#             'category': display_category,  # ← NOW USES display_category
#         })
    
#     return JsonResponse({'suggestions': suggestions})


# #  -----------------------------  Part 3 -------------------------------------------------------------------------

# # =============================================================================
# # REDIS CLIENT
# # =============================================================================

# import redis
# import json
# from typing import Optional, Dict, Any, List, Set

# # Your existing config imports would go here
# # from django.conf import settings
# # REDIS_LOCATION = settings.REDIS_LOCATION
# # etc.


# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Category suffixes used in keys (kept for metadata parsing)
# CATEGORY_SUFFIXES = [
#     'continent', 'country', 'us_state', 'state_province', 
#     'us_city', 'city', 'word'
# ]

# # Prefix bucket configuration
# PREFIX_LENGTH = 2  # Use 2-character prefixes

# # Alphabet for candidate generation
# import string


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """
#     Extract the base term from a member key.
    
#     Examples:
#         'africa:continent' -> 'africa'
#         'paris:city:fr' -> 'paris'
#         'new york:us_city:ny' -> 'new york'
#         'happy:word' -> 'happy'
#     """
#     if ':' not in member:
#         return member
    
#     parts = member.split(':')
    
#     # Find where the category suffix starts
#     for i, part in enumerate(parts):
#         if part in CATEGORY_SUFFIXES:
#             # Return everything before this part
#             return ':'.join(parts[:i]) if i > 0 else parts[0]
    
#     # If no category suffix found, return first part
#     return parts[0]


# def get_hash_key(member: str) -> str:
#     """
#     Get the hash key for a term.
    
#     Example: 'africa:continent' -> 'term:africa:continent'
#     """
#     return f'term:{member}'


# def get_prefix(term: str, length: int = PREFIX_LENGTH) -> str:
#     """
#     Extract prefix from a term.
    
#     Example: 'africa' with length=2 -> 'af'
#     """
#     term_clean = term.lower().strip()
#     return term_clean[:length] if len(term_clean) >= length else term_clean


# # =============================================================================
# # CANDIDATE GENERATION (KEPT FOR BACKWARDS COMPATIBILITY)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
    
#     KEPT FOR BACKWARDS COMPATIBILITY with word_discovery.py
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }
    
#     letter_confusions = {
#         'a': 'eo', 'e': 'ai', 'i': 'ey', 'o': 'au', 'u': 'o',
#         'c': 'ks', 's': 'cz', 'k': 'c', 'z': 's',
#         'f': 'ph', 'j': 'g', 'g': 'j',
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Double transposition
#     for i in range(length - 1):
#         first_swap = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         for j in range(len(first_swap) - 1):
#             if j != i and j != i - 1:
#                 candidate = first_swap[:j] + first_swap[j+1] + first_swap[j] + first_swap[j+2:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#         if len(candidates) >= max_candidates:
#             break

#     # 4. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 5. Double letter removal
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 6. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 7. Common letter confusion substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in letter_confusions:
#             for confused_char in letter_confusions[char]:
#                 candidate = word_lower[:i] + confused_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 8. Single character insertion (limited)
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     # 9. Missing double letter
#     if len(candidates) < max_candidates:
#         for i in range(length):
#             candidate = word_lower[:i] + word_lower[i] + word_lower[i:]
#             candidates.add(candidate)

#     return set(list(candidates)[:max_candidates])


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis.
    
#     KEPT FOR BACKWARDS COMPATIBILITY with word_discovery.py
    
#     Checks each candidate against prefix buckets and returns metadata.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not candidates:
#         return []
    
#     try:
#         found = []
        
#         for candidate in candidates:
#             if len(candidate) < PREFIX_LENGTH:
#                 continue
            
#             # Find member keys for this candidate
#             member_keys = find_term_members(candidate)
            
#             if member_keys:
#                 # Get metadata for first match
#                 for member_key in member_keys:
#                     metadata = client.hgetall(f'term:{member_key}')
#                     if metadata:
#                         base_term = extract_base_term(member_key)
#                         found.append({
#                             'member': member_key,
#                             'term': base_term,
#                             'display': metadata.get('display', base_term),
#                             'pos': metadata.get('pos', 'unknown'),
#                             'category': metadata.get('category', ''),
#                             'entity_type': metadata.get('entity_type', ''),
#                             'subtext': metadata.get('subtext', ''),
#                             'rank': int(metadata.get('rank', 0)),
#                         })
#                         break  # Only need first match per candidate
        
#         return found
        
#     except Exception as e:
#         print(f"Batch check error: {e}")
#         return []


# # =============================================================================
# # CORE FUNCTIONS - PREFIX BUCKET APPROACH
# # =============================================================================

# def get_prefix_bucket(prefix: str) -> Dict[str, int]:
#     """
#     Get all terms and their ranks from a prefix bucket.
    
#     Bucket stores: {member_key: rank, ...}
#     Example: {'africa:continent': 9000000, 'afghanistan:country': 8500000}
    
#     Returns: {member_key: rank, ...}
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return {}
    
#     try:
#         bucket_key = f"prefix:{prefix.lower()}"
#         data = client.hgetall(bucket_key)
#         return {k: int(v) for k, v in data.items()}
#     except Exception as e:
#         print(f"Error getting prefix bucket: {e}")
#         return {}


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix.
    
#     Flow:
#     1. Extract 2-char prefix from input
#     2. HGETALL prefix:{xx} - single Redis call
#     3. Filter to match full input prefix
#     4. Sort by rank descending
#     5. Fetch metadata for top results
#     6. Return
    
#     Total: ~2-5ms
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     prefix_lower = prefix.lower().strip()
    
#     if len(prefix_lower) < PREFIX_LENGTH:
#         return []
    
#     try:
#         bucket_prefix = get_prefix(prefix_lower)
        
#         # Step 1: Get bucket (single Redis call)
#         bucket = get_prefix_bucket(bucket_prefix)
        
#         if not bucket:
#             return []
        
#         # Step 2: Filter members where base term matches input prefix
#         matching = []
#         for member_key, rank in bucket.items():
#             base_term = extract_base_term(member_key)
#             if base_term.startswith(prefix_lower):
#                 matching.append((member_key, base_term, rank))
        
#         if not matching:
#             return []
        
#         # Step 3: Sort by rank descending, take top N
#         matching.sort(key=lambda x: x[2], reverse=True)
#         top_matches = matching[:limit * 2]  # Fetch extra in case some lack metadata
        
#         # Step 4: Fetch metadata for top terms
#         pipeline = client.pipeline()
#         for member_key, base_term, rank in top_matches:
#             pipeline.hgetall(f'term:{member_key}')
        
#         metadata_list = pipeline.execute()
        
#         # Step 5: Build results
#         results = []
#         for (member_key, base_term, rank), metadata in zip(top_matches, metadata_list):
#             if metadata:
#                 results.append({
#                     'member': member_key,
#                     'term': base_term,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'description': metadata.get('description', ''),
#                     'rank': rank,
#                 })
#             else:
#                 # Term exists in bucket but no metadata - still include
#                 results.append({
#                     'member': member_key,
#                     'term': base_term,
#                     'display': base_term,
#                     'pos': '',
#                     'category': '',
#                     'entity_type': '',
#                     'description': '',
#                     'rank': rank,
#                 })
            
#             if len(results) >= limit:
#                 break
        
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Autocomplete error: {e}")
#         return []


# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash - O(1)
    
#     Args:
#         member: The full member key (e.g., 'africa:continent')
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         hash_key = get_hash_key(member)
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'member': member,
#                 'term': base_term,
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def term_exists(term: str) -> bool:
#     """
#     Check if a term exists in any prefix bucket.
    
#     Looks for any member key that starts with the term.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return False
    
#     term_lower = term.lower().strip()
    
#     if len(term_lower) < PREFIX_LENGTH:
#         return False
    
#     try:
#         bucket_prefix = get_prefix(term_lower)
#         bucket = get_prefix_bucket(bucket_prefix)
        
#         # Check if any member's base term matches exactly
#         for member_key in bucket.keys():
#             base_term = extract_base_term(member_key)
#             if base_term == term_lower:
#                 return True
        
#         return False
        
#     except Exception as e:
#         print(f"Term exists check error: {e}")
#         return False


# def find_term_members(term: str) -> List[str]:
#     """
#     Find all member keys for a given term.
    
#     Example: 'paris' might return ['paris:city:fr', 'paris:us_city:tx', 'paris:word']
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
    
#     if len(term_lower) < PREFIX_LENGTH:
#         return []
    
#     try:
#         bucket_prefix = get_prefix(term_lower)
#         bucket = get_prefix_bucket(bucket_prefix)
        
#         matches = []
#         for member_key in bucket.keys():
#             base_term = extract_base_term(member_key)
#             if base_term == term_lower:
#                 matches.append(member_key)
        
#         return matches
        
#     except Exception as e:
#         print(f"Find term members error: {e}")
#         return []


# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a single word and return metadata if valid.
    
#     Backwards compatible - same return format as before.
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     word_lower = word.lower().strip()
    
#     if len(word_lower) < PREFIX_LENGTH:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#         }
    
#     # Check if term exists and get its member keys
#     member_keys = find_term_members(word_lower)
    
#     if member_keys:
#         # Get metadata for first match (highest priority)
#         metadata = get_term_metadata(member_keys[0])
#         if metadata:
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': metadata.get('rank', 0),
#                 }
#             }
    
#     # Word not found - get suggestion from autocomplete
#     suggestions = get_autocomplete(word_lower, limit=1)
    
#     if suggestions:
#         best = suggestions[0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'metadata': {
#                 'display': best['display'],
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best.get('category', ''),
#                 'rank': best.get('rank', 0),
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,  # Kept for backwards compatibility, not used
#     fallback_pool_size: int = 200  # Kept for backwards compatibility, not used
# ) -> List[Dict[str, Any]]:
#     """
#     Get suggestions for input text.
    
#     Simplified from 5-tier system to single prefix bucket lookup.
    
#     CHANGED FOR BACKWARDS COMPATIBILITY: Now returns a list directly,
#     not a dict with 'suggestions' key. This matches how word_discovery.py
#     uses this function.
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return []
    
#     if not input_text or not input_text.strip():
#         return []
    
#     input_lower = input_text.lower().strip()
    
#     if len(input_lower) < PREFIX_LENGTH:
#         return []
    
#     try:
#         # Get autocomplete suggestions
#         suggestions = get_autocomplete(input_lower, limit=limit)
        
#         # Format for backwards compatibility
#         results = []
#         for item in suggestions:
#             results.append({
#                 'member': item.get('member', ''),
#                 'term': item['term'],
#                 'display': item['display'],
#                 'pos': item.get('pos', ''),
#                 'category': item.get('category', ''),
#                 'entity_type': item.get('entity_type', ''),
#                 'subtext': item.get('subtext', ''),
#                 'rank': item.get('rank', 0),
#                 'distance': 0,  # No longer calculating distance
#                 'score': -item.get('rank', 0),  # Lower score = better
#             })
        
#         return results
        
#     except Exception as e:
#         print(f"Get suggestions error: {e}")
#         return []


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache with TTL"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     Backwards compatible.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
#         terms = []
#         normalized_words = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
            
#             validation = validate_word(word)
            
#             if validation.get('is_valid'):
#                 terms.append({
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return validate_word(word)


# # =============================================================================
# # INDEX-TIME FUNCTIONS - PREFIX BUCKET MANAGEMENT
# # =============================================================================

# def add_term_to_bucket(member_key: str, rank: int) -> bool:
#     """
#     Add or update a term in its prefix bucket.
    
#     Args:
#         member_key: Full member key (e.g., 'africa:continent')
#         rank: The term's rank/score
    
#     Call this when adding a new term or updating a rank.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not member_key:
#         return False
    
#     try:
#         base_term = extract_base_term(member_key)
        
#         if len(base_term) < PREFIX_LENGTH:
#             return False
        
#         bucket_prefix = get_prefix(base_term)
#         bucket_key = f"prefix:{bucket_prefix}"
        
#         client.hset(bucket_key, member_key, rank)
#         return True
        
#     except Exception as e:
#         print(f"Error adding term to bucket: {e}")
#         return False


# def remove_term_from_bucket(member_key: str) -> bool:
#     """
#     Remove a term from its prefix bucket.
    
#     Args:
#         member_key: Full member key (e.g., 'africa:continent')
    
#     Call this when deleting a term.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not member_key:
#         return False
    
#     try:
#         base_term = extract_base_term(member_key)
        
#         if len(base_term) < PREFIX_LENGTH:
#             return False
        
#         bucket_prefix = get_prefix(base_term)
#         bucket_key = f"prefix:{bucket_prefix}"
        
#         client.hdel(bucket_key, member_key)
#         return True
        
#     except Exception as e:
#         print(f"Error removing term from bucket: {e}")
#         return False


# def update_term_rank(member_key: str, new_rank: int) -> bool:
#     """
#     Update a term's rank in its prefix bucket.
    
#     Convenience function - same as add_term_to_bucket.
#     """
#     return add_term_to_bucket(member_key, new_rank)


# def build_prefix_buckets_from_terms(batch_size: int = 1000) -> Dict[str, Any]:
#     """
#     Build all prefix buckets from existing term:* hashes.
    
#     Run this once to migrate from sorted sets to prefix buckets.
    
#     Returns: {
#         'buckets_created': int,
#         'terms_processed': int,
#         'errors': int
#     }
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {'error': 'Redis connection failed'}
    
#     stats = {
#         'buckets_created': set(),
#         'terms_processed': 0,
#         'errors': 0
#     }
    
#     try:
#         pipeline = client.pipeline()
#         pipeline_count = 0
        
#         # Scan all term:* keys
#         for key in client.scan_iter("term:*", count=batch_size):
#             try:
#                 # Extract member_key from hash key: "term:africa:continent" -> "africa:continent"
#                 member_key = key[5:]  # Remove "term:" prefix
                
#                 # Get rank from metadata
#                 rank_str = client.hget(key, 'rank')
#                 try:
#                     rank = int(float(rank_str)) if rank_str else 0
#                 except:
#                     rank = 0
                
#                 # Get base term for prefix
#                 base_term = extract_base_term(member_key)
                
#                 if len(base_term) < PREFIX_LENGTH:
#                     continue
                
#                 # Add to bucket
#                 bucket_prefix = get_prefix(base_term)
#                 bucket_key = f"prefix:{bucket_prefix}"
                
#                 pipeline.hset(bucket_key, member_key, rank)
#                 pipeline_count += 1
#                 stats['buckets_created'].add(bucket_key)
#                 stats['terms_processed'] += 1
                
#                 # Execute pipeline in batches
#                 if pipeline_count >= batch_size:
#                     pipeline.execute()
#                     pipeline = client.pipeline()
#                     pipeline_count = 0
                
#             except Exception as e:
#                 print(f"Error processing {key}: {e}")
#                 stats['errors'] += 1
#                 continue
        
#         # Execute remaining pipeline
#         if pipeline_count > 0:
#             pipeline.execute()
        
#         stats['buckets_created'] = len(stats['buckets_created'])
#         return stats
        
#     except Exception as e:
#         print(f"Error building prefix buckets: {e}")
#         stats['error'] = str(e)
#         return stats


# def rebuild_bucket(prefix: str) -> Dict[str, Any]:
#     """
#     Rebuild a single prefix bucket from term:* hashes.
    
#     Useful for fixing a specific bucket or updating after changes.
    
#     Returns: {'terms_added': int, 'errors': int}
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return {'error': 'Invalid input'}
    
#     prefix_lower = prefix.lower()
#     bucket_key = f"prefix:{prefix_lower}"
    
#     stats = {'terms_added': 0, 'errors': 0}
    
#     try:
#         # Clear existing bucket
#         client.delete(bucket_key)
        
#         # Find all terms matching this prefix
#         pattern = f"term:{prefix_lower}*"
        
#         pipeline = client.pipeline()
#         terms_to_add = []
        
#         for key in client.scan_iter(pattern):
#             try:
#                 member_key = key[5:]  # Remove "term:" prefix
#                 base_term = extract_base_term(member_key)
                
#                 # Verify prefix matches
#                 if not base_term.startswith(prefix_lower):
#                     continue
                
#                 # Get rank
#                 rank_str = client.hget(key, 'rank')
#                 try:
#                     rank = int(float(rank_str)) if rank_str else 0
#                 except:
#                     rank = 0
                
#                 terms_to_add.append((member_key, rank))
                
#             except Exception as e:
#                 stats['errors'] += 1
#                 continue
        
#         # Add all terms to bucket
#         if terms_to_add:
#             for member_key, rank in terms_to_add:
#                 pipeline.hset(bucket_key, member_key, rank)
#                 stats['terms_added'] += 1
            
#             pipeline.execute()
        
#         return stats
        
#     except Exception as e:
#         print(f"Error rebuilding bucket: {e}")
#         stats['error'] = str(e)
#         return stats


# def get_bucket_stats() -> Dict[str, Any]:
#     """
#     Get statistics about prefix buckets.
    
#     Useful for monitoring and debugging.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {'error': 'Redis connection failed'}
    
#     try:
#         stats = {
#             'total_buckets': 0,
#             'total_terms': 0,
#             'avg_bucket_size': 0,
#             'largest_buckets': [],
#             'smallest_buckets': [],
#             'empty_buckets': 0,
#         }
        
#         bucket_sizes = []
        
#         for key in client.scan_iter("prefix:*"):
#             size = client.hlen(key)
#             stats['total_buckets'] += 1
#             stats['total_terms'] += size
            
#             if size == 0:
#                 stats['empty_buckets'] += 1
            
#             bucket_sizes.append((key, size))
        
#         if bucket_sizes:
#             # Sort by size
#             bucket_sizes.sort(key=lambda x: x[1], reverse=True)
            
#             stats['largest_buckets'] = [
#                 {'bucket': k, 'size': s} 
#                 for k, s in bucket_sizes[:5]
#             ]
#             stats['smallest_buckets'] = [
#                 {'bucket': k, 'size': s} 
#                 for k, s in bucket_sizes[-5:] if s > 0
#             ]
#             stats['avg_bucket_size'] = round(
#                 stats['total_terms'] / stats['total_buckets'], 2
#             ) if stats['total_buckets'] > 0 else 0
        
#         return stats
        
#     except Exception as e:
#         print(f"Error getting bucket stats: {e}")
#         return {'error': str(e)}


# def cleanup_sorted_sets() -> Dict[str, Any]:
#     """
#     Remove old sorted sets after migration to prefix buckets.
    
#     WARNING: Only run this after verifying prefix buckets work correctly!
    
#     Returns: {'deleted': [list of deleted keys]}
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {'error': 'Redis connection failed'}
    
#     # Old sorted set keys
#     SORTED_SETS = [
#         'autocomplete:continent',
#         'autocomplete:country',
#         'autocomplete:us_state',
#         'autocomplete:state',
#         'autocomplete:us_city',
#         'autocomplete:city',
#         'autocomplete:word'
#     ]
    
#     deleted = []
    
#     try:
#         for ss_key in SORTED_SETS:
#             if client.exists(ss_key):
#                 client.delete(ss_key)
#                 deleted.append(ss_key)
        
#         return {'deleted': deleted}
        
#     except Exception as e:
#         print(f"Error cleaning up sorted sets: {e}")
#         return {'error': str(e), 'deleted': deleted}


# # =============================================================================
# # DJANGO VIEW
# # =============================================================================

# from django.http import JsonResponse

# def search_suggestions(request):
#     query = request.GET.get('q', '').strip()
    
#     if not query or len(query) < 2:
#         return JsonResponse({'suggestions': []})
    
#     # Get autocomplete results from Redis
#     results = get_autocomplete(prefix=query, limit=8)
    
#     # Transform to match frontend expected format
#     suggestions = []
#     for item in results:
#         category = item.get('category', '')
        
#         # Hide "Dictionary Word" label - show empty string instead
#         display_category = '' if category == 'Dictionary Word' else category
        
#         suggestions.append({
#             'text': item['term'],
#             'display_text': item['display'],
#             'source_field': item.get('entity_type', ''),
#             'category': display_category,
#         })
    
#     return JsonResponse({'suggestions': suggestions})









# =============================================================================
# REDIS CLIENT working fine code
# =============================================================================

# import redis
# import json
# import string
# from typing import Optional, Dict, Any, List, Set


# # Placeholder config - replace with your actual config



# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # SORTED SET CONFIGURATION
# # =============================================================================

# # All sorted sets to search (in priority order)
# SORTED_SETS = [
#     'autocomplete:continent',
#     'autocomplete:country',
#     'autocomplete:us_state',
#     'autocomplete:state',
#     'autocomplete:us_city',
#     'autocomplete:city',
#     'autocomplete:word'
# ]

# # Category suffixes used in keys - FIXED to match SORTED_SETS
# CATEGORY_SUFFIXES = [
#     'continent', 'country', 'us_state', 'state', 
#     'us_city', 'city', 'word'
# ]


# # =============================================================================
# # HELPER FUNCTIONS FOR KEY STRUCTURE
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """
#     Extract the base term from a member key.
    
#     Examples:
#         'africa:continent' -> 'africa'
#         'paris:city:fr' -> 'paris'
#         'new york:us_city:ny' -> 'new york'
#         'happy:word' -> 'happy'
#     """
#     if ':' not in member:
#         return member
    
#     parts = member.split(':')
    
#     # Find where the category suffix starts
#     for i, part in enumerate(parts):
#         if part in CATEGORY_SUFFIXES:
#             # Return everything before this part
#             return ':'.join(parts[:i]) if i > 0 else parts[0]
    
#     # If no category suffix found, return first part
#     return parts[0]


# def get_hash_key(member: str) -> str:
#     """
#     Get the hash key for a sorted set member.
    
#     Example: 'africa:continent' -> 'term:africa:continent'
#     """
#     return f'term:{member}'


# # =============================================================================
# # SMART CANDIDATE GENERATION
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }
    
#     letter_confusions = {
#         'a': 'eo', 'e': 'ai', 'i': 'ey', 'o': 'au', 'u': 'o',
#         'c': 'ks', 's': 'cz', 'k': 'c', 'z': 's',
#         'f': 'ph', 'j': 'g', 'g': 'j',
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Double transposition
#     for i in range(length - 1):
#         first_swap = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         for j in range(len(first_swap) - 1):
#             if j != i and j != i - 1:
#                 candidate = first_swap[:j] + first_swap[j+1] + first_swap[j] + first_swap[j+2:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#         if len(candidates) >= max_candidates:
#             break

#     # 4. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 5. Double letter removal
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 6. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 7. Common letter confusion substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in letter_confusions:
#             for confused_char in letter_confusions[char]:
#                 candidate = word_lower[:i] + confused_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 8. Single character insertion (limited)
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     # 9. Missing double letter
#     if len(candidates) < max_candidates:
#         for i in range(length):
#             candidate = word_lower[:i] + word_lower[i] + word_lower[i:]
#             candidates.add(candidate)

#     return set(list(candidates)[:max_candidates])


# # =============================================================================
# # SCORING FUNCTION
# # =============================================================================

# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """
#     Calculate a combined score based on edit distance and word rank.
#     Lower score = better match.
#     """
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """
#     Calculate the Damerau-Levenshtein distance between two strings.
#     """
#     len1, len2 = len(s1), len(s2)
    
#     # Create distance matrix
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     # Initialize base cases
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     # Fill in the rest of the matrix
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,      # deletion
#                 d[i][j-1] + 1,      # insertion
#                 d[i-1][j-1] + cost  # substitution
#             )
            
#             # Transposition
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# # =============================================================================
# # CORE FUNCTIONS
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash - O(1)
    
#     Args:
#         member: The full member key (e.g., 'africa:continent')
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         hash_key = get_hash_key(member)
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'member': member,
#                 'term': base_term,
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[str]:
#     """
#     Find all members in sorted sets that match the exact term.
#     Uses ZRANGEBYLEX for exact prefix match.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     matches = []
#     term_lower = term.lower()
    
#     for ss in SORTED_SETS:
#         try:
#             # Exact match: look for term or term:* pattern
#             # First check exact term
#             score = client.zscore(ss, term_lower)
#             if score is not None:
#                 matches.append(term_lower)
            
#             # Then check term:suffix patterns (if using compound keys)
#             members = client.zrangebylex(ss, f'[{term_lower}:', f'[{term_lower}:\xff')
#             matches.extend(members)
            
#         except Exception as e:
#             continue
    
#     # Remove duplicates while preserving order
#     seen = set()
#     unique_matches = []
#     for m in matches:
#         if m not in seen:
#             seen.add(m)
#             unique_matches.append(m)
    
#     return unique_matches


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[str]:
#     """
#     Get members from all sorted sets that start with the given prefix.
#     Uses ZRANGEBYLEX (fast) - works because all scores = 0
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         matches = []
#         per_set_limit = max(limit // len(SORTED_SETS), 15)
        
#         for ss in SORTED_SETS:
#             try:
#                 # ZRANGEBYLEX works fast when all scores = 0
#                 members = client.zrangebylex(
#                     ss,
#                     f'[{prefix.lower()}',
#                     f'[{prefix.lower()}\xff',
#                     start=0,
#                     num=per_set_limit
#                 )
#                 matches.extend(members)
#             except Exception as e:
#                 continue
        
#         return matches[:limit * 2]  # Return more, we'll sort and trim later
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches and fetch rank from hash, then sort by rank.
#     This is the main function for autocomplete.
    
#     Flow:
#     1. ZRANGEBYLEX prefix search (fast, ~2ms)
#     2. Pipeline fetch metadata from hash (~3ms)
#     3. Sort by rank (in memory, <1ms)
    
#     Total: ~5-10ms
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         # Step 1: Get prefix matches from sorted sets
#         matches = get_prefix_matches(prefix, limit=limit * 5)
        
#         if not matches:
#             return []
        
#         # Step 2: Batch fetch metadata from hash
#         pipeline = client.pipeline()
#         for member in matches:
#             pipeline.hgetall(get_hash_key(member))
        
#         metadata_list = pipeline.execute()
        
#         # Step 3: Build results with rank
#         results = []
#         for member, metadata in zip(matches, metadata_list):
#             if metadata:
#                 try:
#                     rank = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank = 0
                
#                 base_term = extract_base_term(member)
#                 results.append({
#                     'member': member,
#                     'term': base_term,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'description': metadata.get('description', ''),
#                     'rank': rank
#                 })
        
#         # Step 4: Sort by rank (highest first)
#         results.sort(key=lambda x: x['rank'], reverse=True)
        
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Prefix match with rank error: {e}")
#         return []


# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """
#     Get top words by rank from hash data.
#     Since sorted set scores are 0, we fetch from hash and sort.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         results = []
#         per_set_limit = max(limit // len(SORTED_SETS), 30)
        
#         for ss in SORTED_SETS:
#             try:
#                 # Get some members
#                 members = client.zrange(ss, 0, per_set_limit - 1)
                
#                 # Fetch their ranks from hash
#                 if members:
#                     pipeline = client.pipeline()
#                     for member in members:
#                         pipeline.hget(get_hash_key(member), 'rank')
                    
#                     ranks = pipeline.execute()
                    
#                     for member, rank in zip(members, ranks):
#                         try:
#                             rank_val = int(float(rank)) if rank else 0
#                         except:
#                             rank_val = 0
                        
#                         results.append({
#                             'member': member,
#                             'term': extract_base_term(member),
#                             'rank': rank_val
#                         })
#             except:
#                 continue
        
#         # Sort by rank descending
#         results.sort(key=lambda x: x['rank'], reverse=True)
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis - O(k)
    
#     Updated to search across all sorted sets for each candidate.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not candidates:
#         return []
    
#     try:
#         found = []
        
#         for candidate in candidates:
#             # Find all matches for this candidate across sorted sets
#             matches = get_exact_term_matches(candidate)
            
#             if matches:
#                 # Get metadata for matches
#                 pipeline = client.pipeline()
#                 for member in matches:
#                     pipeline.hgetall(get_hash_key(member))
                
#                 metadata_list = pipeline.execute()
                
#                 for member, metadata in zip(matches, metadata_list):
#                     if metadata:
#                         base_term = extract_base_term(member)
#                         found.append({
#                             'member': member,
#                             'term': base_term,
#                             'display': metadata.get('display', base_term),
#                             'pos': metadata.get('pos', 'unknown'),
#                             'category': metadata.get('category', ''),
#                             'entity_type': metadata.get('entity_type', ''),
#                             'rank': int(metadata.get('rank', 0)),
#                         })
        
#         return found
        
#     except Exception as e:
#         print(f"Batch check error: {e}")
#         return []


# # =============================================================================
# # TIERED SUGGESTION SYSTEM
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     fallback_pool_size: int = 200
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function - UPDATED for score=0 sorted sets.
#     Rank comes from hash, not sorted set score.
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             pipeline = client.pipeline()
#             for member in exact_matches:
#                 pipeline.hgetall(get_hash_key(member))
            
#             metadata_list = pipeline.execute()
            
#             results = []
#             for member, metadata in zip(exact_matches, metadata_list):
#                 if metadata:
#                     try:
#                         rank = int(float(metadata.get('rank', 0)))
#                     except:
#                         rank = 0
                    
#                     base_term = extract_base_term(member)
#                     results.append({
#                         'member': member,
#                         'term': base_term,
#                         'display': metadata.get('display', base_term),
#                         'pos': metadata.get('pos', ''),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': rank,
#                         'distance': 0,
#                         'score': -rank  # Lower score = better (negative rank)
#                     })
            
#             if results:
#                 results.sort(key=lambda x: x['rank'], reverse=True)
#                 response['suggestions'] = results[:limit]
#                 return response
        
#         # === TIER 2: PREFIX MATCH (FAST with score=0) ===
#         prefix_results = get_prefix_matches_with_rank(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             results = []
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 score = calculate_score(distance, item['rank'])
                
#                 results.append({
#                     **item,
#                     'distance': distance,
#                     'score': score
#                 })
            
#             # Sort by score (lower is better)
#             results.sort(key=lambda x: x['score'])
#             response['suggestions'] = results[:limit]
#             return response
        
#         # === TIER 3: SMART CANDIDATES ===
#         candidates = generate_candidates_smart(input_lower, max_candidates=100)
        
#         if candidates:
#             response['tier_used'] = 'smart_candidates'
            
#             results = []
            
#             for candidate in candidates:
#                 # Find all sorted set matches for this candidate
#                 matches = get_exact_term_matches(candidate)
                
#                 if matches:
#                     # Fetch metadata for all matches
#                     pipeline = client.pipeline()
#                     for member in matches:
#                         pipeline.hgetall(get_hash_key(member))
                    
#                     metadata_list = pipeline.execute()
                    
#                     for member, metadata in zip(matches, metadata_list):
#                         if metadata:
#                             distance = damerau_levenshtein_distance(input_lower, candidate)
#                             if distance <= max_distance:
#                                 try:
#                                     rank = int(float(metadata.get('rank', 0)))
#                                 except:
#                                     rank = 0
                                
#                                 score = calculate_score(distance, rank)
#                                 base_term = extract_base_term(member)
                                
#                                 results.append({
#                                     'member': member,
#                                     'term': base_term,
#                                     'display': metadata.get('display', base_term),
#                                     'pos': metadata.get('pos', ''),
#                                     'category': metadata.get('category', ''),
#                                     'entity_type': metadata.get('entity_type', ''),
#                                     'rank': rank,
#                                     'distance': distance,
#                                     'score': score
#                                 })
            
#             if results:
#                 # Remove duplicates based on member
#                 seen = set()
#                 unique_results = []
#                 for r in results:
#                     if r['member'] not in seen:
#                         seen.add(r['member'])
#                         unique_results.append(r)
                
#                 unique_results.sort(key=lambda x: x['score'])
#                 response['suggestions'] = unique_results[:limit]
#                 return response
        
#         # === TIER 4: PREFIX FUZZY ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 fuzzy_prefix = input_lower[:prefix_len]
#                 fuzzy_results = get_prefix_matches_with_rank(fuzzy_prefix, limit=100)
                
#                 if fuzzy_results:
#                     response['tier_used'] = f'prefix_fuzzy_{prefix_len}'
                    
#                     results = []
#                     for item in fuzzy_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'])
#                         if distance <= max_distance:
#                             score = calculate_score(distance, item['rank'])
#                             results.append({
#                                 **item,
#                                 'distance': distance,
#                                 'score': score
#                             })
                    
#                     if results:
#                         results.sort(key=lambda x: x['score'])
#                         response['suggestions'] = results[:limit]
#                         return response
        
#         # === TIER 5: FALLBACK ===
#         response['tier_used'] = 'fallback'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions for a prefix.
#     """
#     result = get_suggestions(prefix, limit=limit)
#     return result.get('suggestions', [])


# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     """
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     word_lower = word.lower().strip()
    
#     # Check for exact matches with new key structure
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         member = exact_matches[0]
#         metadata = client.hgetall(get_hash_key(member))
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 }
#             }
    
#     # Word not found - get suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best['distance'],
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best['display'],
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best['category'],
#                 'rank': best['rank'],
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache with TTL"""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
#         terms = []
#         normalized_words = []
        
#         for word in words:
#             word = word.strip()
#             if not word:
#                 continue
            
#             validation = validate_word(word)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings"""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups"""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word"""
#     return validate_word(word)


# =============================================================================
# REDIS CLIENT - OPTIMIZED VERSION
# =============================================================================
# Backwards compatible with all existing function signatures
# New features:
#   - Batch operations via Redis pipelines (single round-trip)
#   - Pre-validated data passthrough to avoid duplicate lookups
#   - O(1) lookups via hash-based caching
# =============================================================================

# import redis
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple



# class RedisLookupTable:
#     """Redis-based lookup table for search preprocessing"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # SORTED SET CONFIGURATION
# # =============================================================================

# SORTED_SETS = [
#     'autocomplete:continent',
#     'autocomplete:country',
#     'autocomplete:us_state',
#     'autocomplete:state',
#     'autocomplete:us_city',
#     'autocomplete:city',
#     'autocomplete:word'
# ]

# CATEGORY_SUFFIXES = [
#     'continent', 'country', 'us_state', 'state', 
#     'us_city', 'city', 'word'
# ]

# # O(1) lookup set for category suffixes
# CATEGORY_SUFFIXES_SET: frozenset = frozenset(CATEGORY_SUFFIXES)


# # =============================================================================
# # HELPER FUNCTIONS FOR KEY STRUCTURE
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """
#     Extract the base term from a member key. O(p) where p = parts count (small)
#     """
#     if ':' not in member:
#         return member
    
#     parts = member.split(':')
    
#     for i, part in enumerate(parts):
#         if part in CATEGORY_SUFFIXES_SET:  # O(1) lookup
#             return ':'.join(parts[:i]) if i > 0 else parts[0]
    
#     return parts[0]


# def get_hash_key(member: str) -> str:
#     """Get the hash key for a sorted set member. O(1)"""
#     return f'term:{member}'


# # =============================================================================
# # SMART CANDIDATE GENERATION
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     O(L²) where L = word length, but L is typically small (< 20)
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }
    
#     letter_confusions = {
#         'a': 'eo', 'e': 'ai', 'i': 'ey', 'o': 'au', 'u': 'o',
#         'c': 'ks', 's': 'cz', 'k': 'c', 'z': 's',
#         'f': 'ph', 'j': 'g', 'g': 'j',
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Double transposition (limited)
#     for i in range(length - 1):
#         first_swap = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         for j in range(len(first_swap) - 1):
#             if j != i and j != i - 1:
#                 candidate = first_swap[:j] + first_swap[j+1] + first_swap[j] + first_swap[j+2:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#         if len(candidates) >= max_candidates:
#             break

#     # 4. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 5. Double letter removal
#     for i in range(length - 1):
#         if word_lower[i] == word_lower[i+1]:
#             candidate = word_lower[:i] + word_lower[i+1:]
#             candidates.add(candidate)

#     # 6. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 7. Common letter confusion substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in letter_confusions:
#             for confused_char in letter_confusions[char]:
#                 candidate = word_lower[:i] + confused_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 8. Single character insertion (limited)
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     # 9. Missing double letter
#     if len(candidates) < max_candidates:
#         for i in range(length):
#             candidate = word_lower[:i] + word_lower[i] + word_lower[i:]
#             candidates.add(candidate)

#     return set(list(candidates)[:max_candidates])


# # =============================================================================
# # SCORING FUNCTION
# # =============================================================================

# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """Calculate combined score. O(1)"""
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """
#     Calculate the Damerau-Levenshtein distance. O(m*n) where m,n = string lengths
#     """
#     len1, len2 = len(s1), len(s2)
    
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,
#                 d[i][j-1] + 1,
#                 d[i-1][j-1] + cost
#             )
            
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# # =============================================================================
# # CORE FUNCTIONS - SINGLE ITEM (BACKWARDS COMPATIBLE)
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash - O(1)
#     BACKWARDS COMPATIBLE: Same signature as original
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None
    
#     try:
#         hash_key = get_hash_key(member)
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'member': member,
#                 'term': base_term,
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': int(metadata.get('rank', 0)),
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[str]:
#     """
#     Find all members in sorted sets that match the exact term.
#     O(k * log m) where k = number of sorted sets, m = set size
#     BACKWARDS COMPATIBLE: Same signature as original
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     matches = []
#     term_lower = term.lower()
    
#     for ss in SORTED_SETS:
#         try:
#             score = client.zscore(ss, term_lower)
#             if score is not None:
#                 matches.append(term_lower)
            
#             members = client.zrangebylex(ss, f'[{term_lower}:', f'[{term_lower}:\xff')
#             matches.extend(members)
            
#         except Exception:
#             continue
    
#     seen = set()
#     unique_matches = []
#     for m in matches:
#         if m not in seen:
#             seen.add(m)
#             unique_matches.append(m)
    
#     return unique_matches


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[str]:
#     """
#     Get members from all sorted sets that start with the given prefix.
#     O(k * log m) using ZRANGEBYLEX
#     BACKWARDS COMPATIBLE: Same signature as original
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         matches = []
#         per_set_limit = max(limit // len(SORTED_SETS), 15)
        
#         for ss in SORTED_SETS:
#             try:
#                 members = client.zrangebylex(
#                     ss,
#                     f'[{prefix.lower()}',
#                     f'[{prefix.lower()}\xff',
#                     start=0,
#                     num=per_set_limit
#                 )
#                 matches.extend(members)
#             except Exception:
#                 continue
        
#         return matches[:limit * 2]
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches and fetch rank from hash, then sort by rank.
#     BACKWARDS COMPATIBLE: Same signature as original
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     try:
#         matches = get_prefix_matches(prefix, limit=limit * 5)
        
#         if not matches:
#             return []
        
#         pipeline = client.pipeline()
#         for member in matches:
#             pipeline.hgetall(get_hash_key(member))
        
#         metadata_list = pipeline.execute()
        
#         results = []
#         for member, metadata in zip(matches, metadata_list):
#             if metadata:
#                 try:
#                     rank = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank = 0
                
#                 base_term = extract_base_term(member)
#                 results.append({
#                     'member': member,
#                     'term': base_term,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'description': metadata.get('description', ''),
#                     'rank': rank
#                 })
        
#         results.sort(key=lambda x: x['rank'], reverse=True)
        
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Prefix match with rank error: {e}")
#         return []


# # =============================================================================
# # NEW: BATCHED OPERATIONS - O(1) per item after single Redis round-trip
# # =============================================================================

# def batch_get_exact_term_matches(terms: List[str]) -> Dict[str, List[str]]:
#     """
#     Batch find all members for multiple terms in a SINGLE Redis pipeline.
    
#     Complexity: O(k) Redis round-trips reduced to O(1)
    
#     Args:
#         terms: List of terms to look up
    
#     Returns:
#         Dict mapping term -> list of matching members
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not terms:
#         return {}
    
#     try:
#         pipeline = client.pipeline()
        
#         # Queue all operations
#         ops = []  # Track (term, sorted_set, op_type)
#         for term in terms:
#             term_lower = term.lower()
#             for ss in SORTED_SETS:
#                 pipeline.zscore(ss, term_lower)
#                 ops.append((term_lower, ss, 'zscore'))
#                 pipeline.zrangebylex(ss, f'[{term_lower}:', f'[{term_lower}:\xff')
#                 ops.append((term_lower, ss, 'zrangebylex'))
        
#         # Single round-trip
#         results = pipeline.execute()
        
#         # Process results
#         term_matches: Dict[str, Set[str]] = {t.lower(): set() for t in terms}
        
#         for i, result in enumerate(results):
#             term, ss, op_type = ops[i]
            
#             if op_type == 'zscore' and result is not None:
#                 term_matches[term].add(term)
#             elif op_type == 'zrangebylex' and result:
#                 term_matches[term].update(result)
        
#         # Convert sets to lists
#         return {term: list(matches) for term, matches in term_matches.items()}
        
#     except Exception as e:
#         print(f"Batch exact match error: {e}")
#         return {}


# def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch get metadata for multiple members in a SINGLE Redis pipeline.
    
#     Complexity: O(n) Redis calls reduced to O(1)
    
#     Args:
#         members: List of member keys to look up
    
#     Returns:
#         Dict mapping member -> metadata dict
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not members:
#         return {}
    
#     try:
#         pipeline = client.pipeline()
        
#         for member in members:
#             pipeline.hgetall(get_hash_key(member))
        
#         # Single round-trip
#         results = pipeline.execute()
        
#         # Process results
#         metadata_dict = {}
#         for member, metadata in zip(members, results):
#             if metadata:
#                 base_term = extract_base_term(member)
#                 metadata_dict[member] = {
#                     'member': member,
#                     'term': base_term,
#                     'exists': True,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 }
        
#         return metadata_dict
        
#     except Exception as e:
#         print(f"Batch metadata error: {e}")
#         return {}


# def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Validate multiple words in minimal Redis round-trips.
    
#     Complexity: 
#         OLD: O(n * k * log m) - n words, k sorted sets each
#         NEW: O(2) Redis round-trips regardless of n
    
#     Args:
#         words: List of words to validate
    
#     Returns:
#         Dict mapping word -> validation result with metadata
#     """
#     if not words:
#         return {}
    
#     # Step 1: Batch get all exact matches (1 pipeline call)
#     term_matches = batch_get_exact_term_matches(words)
    
#     # Collect all members that need metadata
#     all_members = set()
#     for matches in term_matches.values():
#         all_members.update(matches)
    
#     # Step 2: Batch get all metadata (1 pipeline call)
#     all_metadata = batch_get_term_metadata(list(all_members))
    
#     # Step 3: Build results (in-memory, O(n))
#     results = {}
#     for word in words:
#         word_lower = word.lower()
#         matches = term_matches.get(word_lower, [])
        
#         if matches:
#             # Get metadata for first match (highest priority)
#             member = matches[0]
#             metadata = all_metadata.get(member, {})
            
#             results[word_lower] = {
#                 'is_valid': True,
#                 'word': word_lower,
#                 'member': member,
#                 'matches': matches,  # All matches for this word
#                 'metadata': metadata
#             }
#         else:
#             results[word_lower] = {
#                 'is_valid': False,
#                 'word': word_lower,
#                 'member': None,
#                 'matches': [],
#                 'metadata': {}
#             }
    
#     return results


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Batch check if candidates exist in Redis.
#     BACKWARDS COMPATIBLE but now uses batched operations internally.
    
#     Complexity:
#         OLD: O(c * k * log m) - c candidates, k sets each
#         NEW: O(2) Redis round-trips
#     """
#     if not candidates:
#         return []
    
#     # Use batch validation
#     validation_results = batch_validate_words_redis(list(candidates))
    
#     # Extract valid results with metadata
#     found = []
#     for word, result in validation_results.items():
#         if result['is_valid'] and result['metadata']:
#             metadata = result['metadata']
#             found.append({
#                 'member': result['member'],
#                 'term': metadata.get('term', word),
#                 'display': metadata.get('display', word),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': metadata.get('rank', 0),
#             })
    
#     return found


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch check multiple bigrams in a SINGLE Redis pipeline.
    
#     Complexity:
#         OLD: O(n) - one call per bigram
#         NEW: O(2) Redis round-trips for all bigrams
    
#     Args:
#         word_pairs: List of (word1, word2) tuples
    
#     Returns:
#         Dict mapping "word1 word2" -> metadata (or empty if not found)
#     """
#     if not word_pairs:
#         return {}
    
#     # Build bigram strings
#     bigrams = [f"{w1.lower()} {w2.lower()}" for w1, w2 in word_pairs]
    
#     # Use batch validation
#     validation_results = batch_validate_words_redis(bigrams)
    
#     # Extract results
#     results = {}
#     for bigram, result in validation_results.items():
#         if result['is_valid'] and result['metadata']:
#             results[bigram] = result['metadata']
    
#     return results


# # =============================================================================
# # VALIDATE WORD - BACKWARDS COMPATIBLE WITH OPTIMIZATION HOOKS
# # =============================================================================

# def validate_word(
#     word: str,
#     _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
    
#     BACKWARDS COMPATIBLE: Same signature as original (extra param has default)
    
#     NEW: If _pre_validated dict is passed, uses O(1) lookup instead of Redis call
    
#     Args:
#         word: The word to validate
#         _pre_validated: Optional pre-computed validation results (internal use)
    
#     Returns:
#         Validation result dict
#     """
#     word_lower = word.lower().strip()
    
#     # O(1) lookup if pre-validated data available
#     if _pre_validated is not None and word_lower in _pre_validated:
#         pre = _pre_validated[word_lower]
#         if pre.get('is_valid'):
#             metadata = pre.get('metadata', {})
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word_lower),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': metadata.get('rank', 0),
#                 }
#             }
    
#     # Original Redis lookup path
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': None,
#             'error': 'Redis connection failed'
#         }
    
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         member = exact_matches[0]
#         metadata = client.hgetall(get_hash_key(member))
        
#         if metadata:
#             base_term = extract_base_term(member)
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': int(metadata.get('rank', 0)),
#                 }
#             }
    
#     # Word not found - get suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best['distance'],
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best['display'],
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best['category'],
#                 'rank': best['rank'],
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # GET SUGGESTIONS - BACKWARDS COMPATIBLE
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     fallback_pool_size: int = 200
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function.
#     BACKWARDS COMPATIBLE: Same signature as original
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             pipeline = client.pipeline()
#             for member in exact_matches:
#                 pipeline.hgetall(get_hash_key(member))
            
#             metadata_list = pipeline.execute()
            
#             results = []
#             for member, metadata in zip(exact_matches, metadata_list):
#                 if metadata:
#                     try:
#                         rank = int(float(metadata.get('rank', 0)))
#                     except:
#                         rank = 0
                    
#                     base_term = extract_base_term(member)
#                     results.append({
#                         'member': member,
#                         'term': base_term,
#                         'display': metadata.get('display', base_term),
#                         'pos': metadata.get('pos', ''),
#                         'category': metadata.get('category', ''),
#                         'entity_type': metadata.get('entity_type', ''),
#                         'rank': rank,
#                         'distance': 0,
#                         'score': -rank
#                     })
            
#             if results:
#                 results.sort(key=lambda x: x['rank'], reverse=True)
#                 response['suggestions'] = results[:limit]
#                 return response
        
#         # === TIER 2: PREFIX MATCH ===
#         prefix_results = get_prefix_matches_with_rank(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             results = []
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'])
#                 score = calculate_score(distance, item['rank'])
                
#                 results.append({
#                     **item,
#                     'distance': distance,
#                     'score': score
#                 })
            
#             results.sort(key=lambda x: x['score'])
#             response['suggestions'] = results[:limit]
#             return response
        
#         # === TIER 3: SMART CANDIDATES (now batched) ===
#         candidates = generate_candidates_smart(input_lower, max_candidates=100)
        
#         if candidates:
#             response['tier_used'] = 'smart_candidates'
            
#             # Batch check all candidates at once
#             found = batch_check_candidates(candidates)
            
#             results = []
#             for item in found:
#                 term = item.get('term', '')
#                 distance = damerau_levenshtein_distance(input_lower, term)
                
#                 if distance <= max_distance:
#                     score = calculate_score(distance, item.get('rank', 0))
#                     results.append({
#                         **item,
#                         'distance': distance,
#                         'score': score
#                     })
            
#             if results:
#                 seen = set()
#                 unique_results = []
#                 for r in results:
#                     if r['member'] not in seen:
#                         seen.add(r['member'])
#                         unique_results.append(r)
                
#                 unique_results.sort(key=lambda x: x['score'])
#                 response['suggestions'] = unique_results[:limit]
#                 return response
        
#         # === TIER 4: PREFIX FUZZY ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 fuzzy_prefix = input_lower[:prefix_len]
#                 fuzzy_results = get_prefix_matches_with_rank(fuzzy_prefix, limit=100)
                
#                 if fuzzy_results:
#                     response['tier_used'] = f'prefix_fuzzy_{prefix_len}'
                    
#                     results = []
#                     for item in fuzzy_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'])
#                         if distance <= max_distance:
#                             score = calculate_score(distance, item['rank'])
#                             results.append({
#                                 **item,
#                                 'distance': distance,
#                                 'score': score
#                             })
                    
#                     if results:
#                         results.sort(key=lambda x: x['score'])
#                         response['suggestions'] = results[:limit]
#                         return response
        
#         # === TIER 5: FALLBACK ===
#         response['tier_used'] = 'fallback'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Get autocomplete suggestions. BACKWARDS COMPATIBLE."""
#     result = get_suggestions(prefix, limit=limit)
#     return result.get('suggestions', [])


# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """Get top words by rank. BACKWARDS COMPATIBLE."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         results = []
#         per_set_limit = max(limit // len(SORTED_SETS), 30)
        
#         for ss in SORTED_SETS:
#             try:
#                 members = client.zrange(ss, 0, per_set_limit - 1)
                
#                 if members:
#                     pipeline = client.pipeline()
#                     for member in members:
#                         pipeline.hget(get_hash_key(member), 'rank')
                    
#                     ranks = pipeline.execute()
                    
#                     for member, rank in zip(members, ranks):
#                         try:
#                             rank_val = int(float(rank)) if rank else 0
#                         except:
#                             rank_val = 0
                        
#                         results.append({
#                             'member': member,
#                             'term': extract_base_term(member),
#                             'rank': rank_val
#                         })
#             except:
#                 continue
        
#         results.sort(key=lambda x: x['rank'], reverse=True)
#         return results[:limit]
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# # =============================================================================
# # CACHE FUNCTIONS - BACKWARDS COMPATIBLE
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache. BACKWARDS COMPATIBLE."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache. BACKWARDS COMPATIBLE."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION - BACKWARDS COMPATIBLE WITH NEW BATCH SUPPORT
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10,
#     # NEW optional parameter for downstream passthrough
#     return_validation_cache: bool = False
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
    
#     BACKWARDS COMPATIBLE: All original parameters work the same.
    
#     NEW: return_validation_cache=True adds '_validation_cache' to response
#          for efficient passthrough to word_discovery.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
        
#         # NEW: Batch validate all words at once (2 Redis round-trips)
#         validation_cache = batch_validate_words_redis(words)
        
#         terms = []
#         normalized_words = []
        
#         for i, word in enumerate(words):
#             word = word.strip()
#             if not word:
#                 continue
            
#             # Use pre-validated data (O(1) lookup)
#             validation = validate_word(word, _pre_validated=validation_cache)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'position': i + 1,  # NEW: Include position
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                     'metadata': validation['metadata']  # NEW: Full metadata
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'position': i + 1,  # NEW: Include position
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                     'metadata': validation.get('metadata', {})
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # NEW: Include validation cache for downstream use
#         if return_validation_cache:
#             response['_validation_cache'] = validation_cache
        
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS - BACKWARDS COMPATIBLE
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings. BACKWARDS COMPATIBLE."""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups. BACKWARDS COMPATIBLE."""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word. BACKWARDS COMPATIBLE."""
#     return validate_word(word)


# import redis
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple
# from decouple import config

# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION')
# REDIS_PORT = config('REDIS_PORT')
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD')
# REDIS_USERNAME = config('REDIS_USERNAME')

# INDEX_NAME = "terms_idx"


# # =============================================================================
# # REDIS CONNECTION
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table using RediSearch"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # QUERY ESCAPING HELPERS
# # =============================================================================

# def escape_query(text: str) -> str:
#     """Escape special characters for RediSearch query"""
#     if not text:
#         return ""
#     special_chars = ['@', '!', '{', '}', '(', ')', '|', '-', '=', '>', '<', 
#                      '[', ']', '"', "'", '~', '*', ':', '\\', '.', ',', '/', '&', '^', '$', '#', ';']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# def escape_tag(text: str) -> str:
#     """Escape special characters for TAG field values"""
#     if not text:
#         return ""
#     special_chars = [',', '.', '<', '>', '{', '}', '[', ']', '"', "'", ':', ';', 
#                      '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', 
#                      '=', '~', '|', '\\', '/']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# # =============================================================================
# # INDEX MANAGEMENT
# # =============================================================================

# def create_index() -> bool:
#     """Create the RediSearch index (run once)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         # Check if index exists
#         try:
#             client.ft(INDEX_NAME).info()
#             print(f"Index '{INDEX_NAME}' already exists")
#             return True
#         except:
#             pass
        
#         # Create index
#         client.execute_command(
#             'FT.CREATE', INDEX_NAME,
#             'ON', 'HASH',
#             'PREFIX', '1', 'term:',
#             'SCHEMA',
#             'term', 'TEXT', 'WEIGHT', '5.0',
#             'display', 'TEXT', 'WEIGHT', '3.0',
#             'category', 'TAG', 'SORTABLE',
#             'description', 'TEXT', 'WEIGHT', '1.0',
#             'pos', 'TAG',
#             'entity_type', 'TAG',
#             'rank', 'NUMERIC', 'SORTABLE'
#         )
#         print(f"Index '{INDEX_NAME}' created successfully")
#         return True
        
#     except Exception as e:
#         print(f"Error creating index: {e}")
#         return False


# def drop_index() -> bool:
#     """Drop the index (keeps the data)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         client.ft(INDEX_NAME).dropindex(delete_documents=False)
#         print(f"Index '{INDEX_NAME}' dropped")
#         return True
#     except Exception as e:
#         print(f"Error dropping index: {e}")
#         return False


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """Extract the base term from a document ID (term:xxx:category -> xxx)"""
#     if not member:
#         return ""
    
#     # Remove 'term:' prefix if present
#     if member.startswith('term:'):
#         member = member[5:]
    
#     # Find last colon and remove category suffix
#     parts = member.rsplit(':', 1)
#     if len(parts) == 2:
#         # Check if last part looks like a category
#         potential_category = parts[1].lower()
#         category_keywords = {'city', 'country', 'state', 'us_city', 'us_state', 
#                             'continent', 'word', 'culture', 'business', 'education',
#                             'fashion', 'food', 'health', 'music', 'sport', 'tech'}
#         if potential_category in category_keywords or len(potential_category) < 15:
#             return parts[0]
    
#     return member


# def parse_search_doc(doc) -> Dict[str, Any]:
#     """Parse a RediSearch document into a standard dict format"""
#     try:
#         rank_val = getattr(doc, 'rank', 0)
#         if rank_val:
#             try:
#                 rank_val = int(float(rank_val))
#             except (ValueError, TypeError):
#                 rank_val = 0
#         else:
#             rank_val = 0
        
#         term = getattr(doc, 'term', '')
        
#         return {
#             'id': doc.id,
#             'member': doc.id,  # For backwards compatibility
#             'term': term,
#             'display': getattr(doc, 'display', term),
#             'description': getattr(doc, 'description', ''),
#             'category': getattr(doc, 'category', ''),
#             'entity_type': getattr(doc, 'entity_type', ''),
#             'pos': getattr(doc, 'pos', ''),
#             'rank': rank_val,
#             'exists': True,
#         }
#     except Exception as e:
#         print(f"Error parsing doc: {e}")
#         return {}


# # =============================================================================
# # CORE SEARCH FUNCTIONS
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash.
#     Uses direct HGETALL for single lookups.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not member:
#         return None
    
#     try:
#         # Ensure proper key format
#         hash_key = member if member.startswith('term:') else f'term:{member}'
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             try:
#                 rank_val = int(float(metadata.get('rank', 0)))
#             except (ValueError, TypeError):
#                 rank_val = 0
            
#             return {
#                 'member': member,
#                 'term': metadata.get('term', base_term),
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': rank_val,
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
#     """
#     Find exact matches for a term using RediSearch.
#     Returns list of matching documents.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
        
#         # Exact phrase match
#         query = f'@term:"{escaped_term}"'
        
#         result = client.ft(INDEX_NAME).search(
#             query,
#             sort_by='rank',
#             sort_order='DESC',
#             num=10
#         )
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Exact match error: {e}")
#         return []


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """
#     Get terms that start with the given prefix using RediSearch.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     prefix_lower = prefix.lower().strip()
#     if len(prefix_lower) < 1:
#         return []
    
#     try:
#         escaped_prefix = escape_query(prefix_lower)
#         query = f"@term:{escaped_prefix}*"
        
#         result = client.ft(INDEX_NAME).search(
#             query,
#             sort_by='rank',
#             sort_order='DESC',
#             num=limit
#         )
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches sorted by rank.
#     Same as get_prefix_matches but kept for backwards compatibility.
#     """
#     return get_prefix_matches(prefix, limit=limit)


# # =============================================================================
# # FUZZY SEARCH (SPELL CORRECTION)
# # =============================================================================

# def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get fuzzy matches using RediSearch Levenshtein distance.
#     %term% = 1 edit distance
#     %%term%% = 2 edit distance
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if len(term_lower) < 3:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
        
#         # Try 1 edit distance first
#         if max_distance >= 1:
#             query = f"@term:%{escaped_term}%"
            
#             result = client.ft(INDEX_NAME).search(
#                 query,
#                 sort_by='rank',
#                 sort_order='DESC',
#                 num=limit
#             )
            
#             if result.docs:
#                 matches = []
#                 for doc in result.docs:
#                     parsed = parse_search_doc(doc)
#                     if parsed:
#                         # Calculate actual distance for scoring
#                         parsed['distance'] = damerau_levenshtein_distance(
#                             term_lower, parsed['term'].lower()
#                         )
#                         matches.append(parsed)
                
#                 # Sort by distance first, then by rank
#                 matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                 return matches[:limit]
        
#         # Try 2 edit distance if no results
#         if max_distance >= 2:
#             query = f"@term:%%{escaped_term}%%"
            
#             result = client.ft(INDEX_NAME).search(
#                 query,
#                 sort_by='rank',
#                 sort_order='DESC',
#                 num=limit
#             )
            
#             matches = []
#             for doc in result.docs:
#                 parsed = parse_search_doc(doc)
#                 if parsed:
#                     parsed['distance'] = damerau_levenshtein_distance(
#                         term_lower, parsed['term'].lower()
#                     )
#                     matches.append(parsed)
            
#             matches.sort(key=lambda x: (x['distance'], -x['rank']))
#             return matches[:limit]
        
#         return []
        
#     except Exception as e:
#         print(f"Fuzzy match error: {e}")
#         return []


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE (kept for scoring)
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """Calculate the Damerau-Levenshtein distance between two strings."""
#     len1, len2 = len(s1), len(s2)
    
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,      # deletion
#                 d[i][j-1] + 1,      # insertion
#                 d[i-1][j-1] + cost  # substitution
#             )
            
#             # transposition
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """Calculate combined score for ranking suggestions."""
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # BATCH OPERATIONS
# # =============================================================================

# def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch get metadata for multiple terms using pipeline.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not members:
#         return {}
    
#     try:
#         pipeline = client.pipeline()
        
#         for member in members:
#             hash_key = member if member.startswith('term:') else f'term:{member}'
#             pipeline.hgetall(hash_key)
        
#         results = pipeline.execute()
        
#         metadata_dict = {}
#         for member, metadata in zip(members, results):
#             if metadata:
#                 base_term = extract_base_term(member)
#                 try:
#                     rank_val = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank_val = 0
                
#                 metadata_dict[member] = {
#                     'member': member,
#                     'term': metadata.get('term', base_term),
#                     'exists': True,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': rank_val,
#                 }
        
#         return metadata_dict
        
#     except Exception as e:
#         print(f"Batch metadata error: {e}")
#         return {}


# def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Validate multiple words using RediSearch.
#     Returns dict mapping word -> validation result with metadata.
#     """
#     if not words:
#         return {}
    
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {}
    
#     results = {}
    
#     try:
#         for word in words:
#             word_lower = word.lower().strip()
#             if not word_lower:
#                 continue
            
#             # Try exact match first
#             matches = get_exact_term_matches(word_lower)
            
#             if matches:
#                 results[word_lower] = {
#                     'is_valid': True,
#                     'word': word_lower,
#                     'member': matches[0].get('id', ''),
#                     'matches': matches,
#                     'metadata': matches[0]
#                 }
#             else:
#                 results[word_lower] = {
#                     'is_valid': False,
#                     'word': word_lower,
#                     'member': None,
#                     'matches': [],
#                     'metadata': {}
#                 }
        
#         return results
        
#     except Exception as e:
#         print(f"Batch validation error: {e}")
#         return {}


# # =============================================================================
# # VALIDATE WORD (SPELL CHECK)
# # =============================================================================

# def validate_word(
#     word: str,
#     _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     """
#     word_lower = word.lower().strip()
    
#     # O(1) lookup if pre-validated data available
#     if _pre_validated is not None and word_lower in _pre_validated:
#         pre = _pre_validated[word_lower]
#         if pre.get('is_valid'):
#             metadata = pre.get('metadata', {})
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word_lower),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': metadata.get('rank', 0),
#                 }
#             }
    
#     # Check for exact match
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         metadata = exact_matches[0]
#         return {
#             'word': word,
#             'is_valid': True,
#             'suggestion': None,
#             'metadata': {
#                 'display': metadata.get('display', word_lower),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'rank': metadata.get('rank', 0),
#             }
#         }
    
#     # Word not found - get fuzzy suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best.get('distance', 0),
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best.get('display', ''),
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best.get('category', ''),
#                 'rank': best.get('rank', 0),
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # GET SUGGESTIONS (UNIFIED SEARCH)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.
#     Tries: Exact -> Prefix -> Fuzzy
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             for match in exact_matches[:limit]:
#                 match['distance'] = 0
#                 match['score'] = -match.get('rank', 0)
            
#             response['suggestions'] = exact_matches[:limit]
#             return response
        
#         # === TIER 2: PREFIX MATCH ===
#         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                 item['distance'] = distance
#                 item['score'] = calculate_score(distance, item.get('rank', 0))
            
#             prefix_results.sort(key=lambda x: x['score'])
#             response['suggestions'] = prefix_results[:limit]
#             return response
        
#         # === TIER 3: FUZZY MATCH ===
#         fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
        
#         if fuzzy_results:
#             response['tier_used'] = 'fuzzy'
            
#             # Filter by max distance and add scores
#             filtered = []
#             for item in fuzzy_results:
#                 if item.get('distance', 99) <= max_distance:
#                     item['score'] = calculate_score(item['distance'], item.get('rank', 0))
#                     filtered.append(item)
            
#             filtered.sort(key=lambda x: x['score'])
#             response['suggestions'] = filtered[:limit]
#             return response
        
#         # === TIER 4: SHORTER PREFIX (fallback) ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 short_prefix = input_lower[:prefix_len]
#                 short_results = get_prefix_matches(short_prefix, limit=50)
                
#                 if short_results:
#                     response['tier_used'] = f'prefix_short_{prefix_len}'
                    
#                     filtered = []
#                     for item in short_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                         if distance <= max_distance:
#                             item['distance'] = distance
#                             item['score'] = calculate_score(distance, item.get('rank', 0))
#                             filtered.append(item)
                    
#                     if filtered:
#                         filtered.sort(key=lambda x: x['score'])
#                         response['suggestions'] = filtered[:limit]
#                         return response
        
#         # === NO RESULTS ===
#         response['tier_used'] = 'none'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # AUTOCOMPLETE
# # =============================================================================

# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions.
#     Returns list of suggestions sorted by rank.
#     """
#     if not prefix or len(prefix.strip()) < 2:
#         return []
    
#     result = get_suggestions(prefix.strip(), limit=limit)
#     return result.get('suggestions', [])


# # =============================================================================
# # TOP WORDS BY RANK
# # =============================================================================

# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """Get top words by rank using RediSearch."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         # Search all, sort by rank descending
#         result = client.ft(INDEX_NAME).search(
#             "*",
#             sort_by='rank',
#             sort_order='DESC',
#             num=limit
#         )
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# # =============================================================================
# # FILTERED SEARCH
# # =============================================================================

# def search_by_category(
#     query: str, 
#     category: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search within a specific category."""
#     client = RedisLookupTable.get_client()
#     if not client or not query or not category:
#         return []
    
#     try:
#         escaped_query = escape_query(query.lower().strip())
#         escaped_category = escape_tag(category)
        
#         search_query = f"@term:{escaped_query}* @category:{{{escaped_category}}}"
        
#         result = client.ft(INDEX_NAME).search(
#             search_query,
#             sort_by='rank',
#             sort_order='DESC',
#             num=limit
#         )
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Category search error: {e}")
#         return []


# def search_by_entity_type(
#     query: str, 
#     entity_type: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search by entity type (unigram, bigram, trigram)."""
#     client = RedisLookupTable.get_client()
#     if not client or not query or not entity_type:
#         return []
    
#     try:
#         escaped_query = escape_query(query.lower().strip())
#         escaped_type = escape_tag(entity_type)
        
#         search_query = f"@term:{escaped_query}* @entity_type:{{{escaped_type}}}"
        
#         result = client.ft(INDEX_NAME).search(
#             search_query,
#             sort_by='rank',
#             sort_order='DESC',
#             num=limit
#         )
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Entity type search error: {e}")
#         return []


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10,
#     return_validation_cache: bool = False
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
        
#         # Batch validate all words
#         validation_cache = batch_validate_words_redis(words)
        
#         terms = []
#         normalized_words = []
        
#         for i, word in enumerate(words):
#             word = word.strip()
#             if not word:
#                 continue
            
#             # Use pre-validated data
#             validation = validate_word(word, _pre_validated=validation_cache)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                     'metadata': validation['metadata']
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                     'metadata': validation.get('metadata', {})
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # Include validation cache for downstream use
#         if return_validation_cache:
#             response['_validation_cache'] = validation_cache
        
#         # Save to cache
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings."""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups."""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word."""
#     return validate_word(word)


# # =============================================================================
# # LEGACY FUNCTIONS (kept for backwards compatibility)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     Kept for backwards compatibility - RediSearch fuzzy search is preferred.
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 4. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 5. Single character insertion
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     return set(list(candidates)[:max_candidates])


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Check if candidates exist in Redis.
#     Kept for backwards compatibility.
#     """
#     if not candidates:
#         return []
    
#     found = []
#     for candidate in list(candidates)[:50]:  # Limit to prevent overload
#         matches = get_exact_term_matches(candidate)
#         if matches:
#             found.append(matches[0])
    
#     return found


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """
#     Check multiple bigrams.
#     Kept for backwards compatibility.
#     """
#     if not word_pairs:
#         return {}
    
#     results = {}
#     for w1, w2 in word_pairs:
#         bigram = f"{w1.lower()} {w2.lower()}"
#         matches = get_exact_term_matches(bigram)
#         if matches:
#             results[bigram] = matches[0]
    
#     return results

# import redis
# from redis.commands.search.query import Query
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple
# from decouple import config


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION', default='localhost')
# REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
# REDIS_USERNAME = config('REDIS_USERNAME', default='')

# INDEX_NAME = "terms_idx"


# # =============================================================================
# # REDIS CONNECTION
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table using RediSearch"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # QUERY ESCAPING HELPERS
# # =============================================================================

# def escape_query(text: str) -> str:
#     """Escape special characters for RediSearch query"""
#     if not text:
#         return ""
#     special_chars = ['@', '!', '{', '}', '(', ')', '|', '-', '=', '>', '<', 
#                      '[', ']', '"', "'", '~', '*', ':', '\\', '.', ',', '/', '&', '^', '$', '#', ';']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# def escape_tag(text: str) -> str:
#     """Escape special characters for TAG field values"""
#     if not text:
#         return ""
#     special_chars = [',', '.', '<', '>', '{', '}', '[', ']', '"', "'", ':', ';', 
#                      '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', 
#                      '=', '~', '|', '\\', '/']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# # =============================================================================
# # INDEX MANAGEMENT
# # =============================================================================

# def create_index() -> bool:
#     """Create the RediSearch index (run once)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         # Check if index exists
#         try:
#             client.ft(INDEX_NAME).info()
#             print(f"Index '{INDEX_NAME}' already exists")
#             return True
#         except:
#             pass
        
#         # Create index
#         client.execute_command(
#             'FT.CREATE', INDEX_NAME,
#             'ON', 'HASH',
#             'PREFIX', '1', 'term:',
#             'SCHEMA',
#             'term', 'TEXT', 'WEIGHT', '5.0',
#             'display', 'TEXT', 'WEIGHT', '3.0',
#             'category', 'TAG', 'SORTABLE',
#             'description', 'TEXT', 'WEIGHT', '1.0',
#             'pos', 'TAG',
#             'entity_type', 'TAG',
#             'rank', 'NUMERIC', 'SORTABLE'
#         )
#         print(f"Index '{INDEX_NAME}' created successfully")
#         return True
        
#     except Exception as e:
#         print(f"Error creating index: {e}")
#         return False


# def drop_index() -> bool:
#     """Drop the index (keeps the data)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         client.ft(INDEX_NAME).dropindex(delete_documents=False)
#         print(f"Index '{INDEX_NAME}' dropped")
#         return True
#     except Exception as e:
#         print(f"Error dropping index: {e}")
#         return False


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """Extract the base term from a document ID (term:xxx:category -> xxx)"""
#     if not member:
#         return ""
    
#     # Remove 'term:' prefix if present
#     if member.startswith('term:'):
#         member = member[5:]
    
#     # Find last colon and remove category suffix
#     parts = member.rsplit(':', 1)
#     if len(parts) == 2:
#         potential_category = parts[1].lower()
#         category_keywords = {'city', 'country', 'state', 'us_city', 'us_state', 
#                             'continent', 'word', 'culture', 'business', 'education',
#                             'fashion', 'food', 'health', 'music', 'sport', 'tech'}
#         if potential_category in category_keywords or len(potential_category) < 15:
#             return parts[0]
    
#     return member


# def parse_search_doc(doc) -> Dict[str, Any]:
#     """Parse a RediSearch document into a standard dict format"""
#     try:
#         rank_val = getattr(doc, 'rank', 0)
#         if rank_val:
#             try:
#                 rank_val = int(float(rank_val))
#             except (ValueError, TypeError):
#                 rank_val = 0
#         else:
#             rank_val = 0
        
#         term = getattr(doc, 'term', '')
        
#         return {
#             'id': doc.id,
#             'member': doc.id,
#             'term': term,
#             'display': getattr(doc, 'display', term),
#             'description': getattr(doc, 'description', ''),
#             'category': getattr(doc, 'category', ''),
#             'entity_type': getattr(doc, 'entity_type', ''),
#             'pos': getattr(doc, 'pos', ''),
#             'rank': rank_val,
#             'exists': True,
#         }
#     except Exception as e:
#         print(f"Error parsing doc: {e}")
#         return {}


# # =============================================================================
# # CORE SEARCH FUNCTIONS
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash.
#     Uses direct HGETALL for single lookups.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not member:
#         return None
    
#     try:
#         hash_key = member if member.startswith('term:') else f'term:{member}'
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             try:
#                 rank_val = int(float(metadata.get('rank', 0)))
#             except (ValueError, TypeError):
#                 rank_val = 0
            
#             return {
#                 'member': member,
#                 'term': metadata.get('term', base_term),
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': rank_val,
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
#     """
#     Find exact matches for a term using RediSearch.
#     Returns list of matching documents.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
#         query_str = f'@term:"{escaped_term}"'
        
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, 10)
#         result = client.ft(INDEX_NAME).search(query)
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Exact match error: {e}")
#         return []


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """
#     Get terms that start with the given prefix using RediSearch.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     prefix_lower = prefix.lower().strip()
#     if len(prefix_lower) < 1:
#         return []
    
#     try:
#         escaped_prefix = escape_query(prefix_lower)
#         query_str = f"@term:{escaped_prefix}*"
        
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches sorted by rank.
#     Same as get_prefix_matches but kept for backwards compatibility.
#     """
#     return get_prefix_matches(prefix, limit=limit)


# # =============================================================================
# # FUZZY SEARCH (SPELL CORRECTION)
# # =============================================================================

# def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get fuzzy matches using RediSearch Levenshtein distance.
#     %term% = 1 edit distance
#     %%term%% = 2 edit distance
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if len(term_lower) < 3:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
        
#         # Try 1 edit distance first
#         if max_distance >= 1:
#             query_str = f"@term:%{escaped_term}%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             result = client.ft(INDEX_NAME).search(query)
            
#             if result.docs:
#                 matches = []
#                 for doc in result.docs:
#                     parsed = parse_search_doc(doc)
#                     if parsed:
#                         parsed['distance'] = damerau_levenshtein_distance(
#                             term_lower, parsed['term'].lower()
#                         )
#                         matches.append(parsed)
                
#                 matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                 return matches[:limit]
        
#         # Try 2 edit distance if no results
#         if max_distance >= 2:
#             query_str = f"@term:%%{escaped_term}%%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             result = client.ft(INDEX_NAME).search(query)
            
#             matches = []
#             for doc in result.docs:
#                 parsed = parse_search_doc(doc)
#                 if parsed:
#                     parsed['distance'] = damerau_levenshtein_distance(
#                         term_lower, parsed['term'].lower()
#                     )
#                     matches.append(parsed)
            
#             matches.sort(key=lambda x: (x['distance'], -x['rank']))
#             return matches[:limit]
        
#         return []
        
#     except Exception as e:
#         print(f"Fuzzy match error: {e}")
#         return []


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """Calculate the Damerau-Levenshtein distance between two strings."""
#     len1, len2 = len(s1), len(s2)
    
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,
#                 d[i][j-1] + 1,
#                 d[i-1][j-1] + cost
#             )
            
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """Calculate combined score for ranking suggestions."""
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # BATCH OPERATIONS
# # =============================================================================

# def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch get metadata for multiple terms using pipeline.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not members:
#         return {}
    
#     try:
#         pipeline = client.pipeline()
        
#         for member in members:
#             hash_key = member if member.startswith('term:') else f'term:{member}'
#             pipeline.hgetall(hash_key)
        
#         results = pipeline.execute()
        
#         metadata_dict = {}
#         for member, metadata in zip(members, results):
#             if metadata:
#                 base_term = extract_base_term(member)
#                 try:
#                     rank_val = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank_val = 0
                
#                 metadata_dict[member] = {
#                     'member': member,
#                     'term': metadata.get('term', base_term),
#                     'exists': True,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': rank_val,
#                 }
        
#         return metadata_dict
        
#     except Exception as e:
#         print(f"Batch metadata error: {e}")
#         return {}


# def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Validate multiple words using RediSearch.
#     Returns dict mapping word -> validation result with metadata.
#     """
#     if not words:
#         return {}
    
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {}
    
#     results = {}
    
#     try:
#         for word in words:
#             word_lower = word.lower().strip()
#             if not word_lower:
#                 continue
            
#             matches = get_exact_term_matches(word_lower)
            
#             if matches:
#                 results[word_lower] = {
#                     'is_valid': True,
#                     'word': word_lower,
#                     'member': matches[0].get('id', ''),
#                     'matches': matches,
#                     'metadata': matches[0]
#                 }
#             else:
#                 results[word_lower] = {
#                     'is_valid': False,
#                     'word': word_lower,
#                     'member': None,
#                     'matches': [],
#                     'metadata': {}
#                 }
        
#         return results
        
#     except Exception as e:
#         print(f"Batch validation error: {e}")
#         return {}


# # =============================================================================
# # VALIDATE WORD (SPELL CHECK)
# # =============================================================================

# def validate_word(
#     word: str,
#     _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     """
#     word_lower = word.lower().strip()
    
#     # O(1) lookup if pre-validated data available
#     if _pre_validated is not None and word_lower in _pre_validated:
#         pre = _pre_validated[word_lower]
#         if pre.get('is_valid'):
#             metadata = pre.get('metadata', {})
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word_lower),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': metadata.get('rank', 0),
#                 }
#             }
    
#     # Check for exact match
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         metadata = exact_matches[0]
#         return {
#             'word': word,
#             'is_valid': True,
#             'suggestion': None,
#             'metadata': {
#                 'display': metadata.get('display', word_lower),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'rank': metadata.get('rank', 0),
#             }
#         }
    
#     # Word not found - get fuzzy suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best.get('distance', 0),
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best.get('display', ''),
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best.get('category', ''),
#                 'rank': best.get('rank', 0),
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # GET SUGGESTIONS (UNIFIED SEARCH)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.
#     Tries: Exact -> Prefix -> Fuzzy
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             for match in exact_matches[:limit]:
#                 match['distance'] = 0
#                 match['score'] = -match.get('rank', 0)
            
#             response['suggestions'] = exact_matches[:limit]
#             return response
        
#         # === TIER 2: PREFIX MATCH ===
#         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                 item['distance'] = distance
#                 item['score'] = calculate_score(distance, item.get('rank', 0))
            
#             prefix_results.sort(key=lambda x: x['score'])
#             response['suggestions'] = prefix_results[:limit]
#             return response
        
#         # === TIER 3: FUZZY MATCH ===
#         fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
        
#         if fuzzy_results:
#             response['tier_used'] = 'fuzzy'
            
#             filtered = []
#             for item in fuzzy_results:
#                 if item.get('distance', 99) <= max_distance:
#                     item['score'] = calculate_score(item['distance'], item.get('rank', 0))
#                     filtered.append(item)
            
#             filtered.sort(key=lambda x: x['score'])
#             response['suggestions'] = filtered[:limit]
#             return response
        
#         # === TIER 4: SHORTER PREFIX (fallback) ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 short_prefix = input_lower[:prefix_len]
#                 short_results = get_prefix_matches(short_prefix, limit=50)
                
#                 if short_results:
#                     response['tier_used'] = f'prefix_short_{prefix_len}'
                    
#                     filtered = []
#                     for item in short_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                         if distance <= max_distance:
#                             item['distance'] = distance
#                             item['score'] = calculate_score(distance, item.get('rank', 0))
#                             filtered.append(item)
                    
#                     if filtered:
#                         filtered.sort(key=lambda x: x['score'])
#                         response['suggestions'] = filtered[:limit]
#                         return response
        
#         # === NO RESULTS ===
#         response['tier_used'] = 'none'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # AUTOCOMPLETE
# # =============================================================================

# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions.
#     Returns list of suggestions sorted by rank.
#     """
#     if not prefix or len(prefix.strip()) < 2:
#         return []
    
#     result = get_suggestions(prefix.strip(), limit=limit)
#     return result.get('suggestions', [])


# # =============================================================================
# # TOP WORDS BY RANK
# # =============================================================================

# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """Get top words by rank using RediSearch."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         query = Query("*").sort_by('rank', asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# # =============================================================================
# # FILTERED SEARCH
# # =============================================================================

# def search_by_category(
#     query_text: str, 
#     category: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search within a specific category."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not category:
#         return []
    
#     try:
#         escaped_query = escape_query(query_text.lower().strip())
#         escaped_category = escape_tag(category)
        
#         query_str = f"@term:{escaped_query}* @category:{{{escaped_category}}}"
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Category search error: {e}")
#         return []


# def search_by_entity_type(
#     query_text: str, 
#     entity_type: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search by entity type (unigram, bigram, trigram)."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not entity_type:
#         return []
    
#     try:
#         escaped_query = escape_query(query_text.lower().strip())
#         escaped_type = escape_tag(entity_type)
        
#         query_str = f"@term:{escaped_query}* @entity_type:{{{escaped_type}}}"
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Entity type search error: {e}")
#         return []


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10,
#     return_validation_cache: bool = False
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
        
#         # Batch validate all words
#         validation_cache = batch_validate_words_redis(words)
        
#         terms = []
#         normalized_words = []
        
#         for i, word in enumerate(words):
#             word = word.strip()
#             if not word:
#                 continue
            
#             # Use pre-validated data
#             validation = validate_word(word, _pre_validated=validation_cache)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                     'metadata': validation['metadata']
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                     'metadata': validation.get('metadata', {})
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # Include validation cache for downstream use
#         if return_validation_cache:
#             response['_validation_cache'] = validation_cache
        
#         # Save to cache
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # LEGACY FUNCTIONS (kept for backwards compatibility)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     Kept for backwards compatibility - RediSearch fuzzy search is preferred.
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 4. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 5. Single character insertion
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     return set(list(candidates)[:max_candidates])


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Check if candidates exist in Redis.
#     Kept for backwards compatibility.
#     """
#     if not candidates:
#         return []
    
#     found = []
#     for candidate in list(candidates)[:50]:
#         matches = get_exact_term_matches(candidate)
#         if matches:
#             found.append(matches[0])
    
#     return found


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """
#     Check multiple bigrams.
#     Kept for backwards compatibility.
#     """
#     if not word_pairs:
#         return {}
    
#     results = {}
#     for w1, w2 in word_pairs:
#         bigram = f"{w1.lower()} {w2.lower()}"
#         matches = get_exact_term_matches(bigram)
#         if matches:
#             results[bigram] = matches[0]
    
#     return results


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings."""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups."""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word."""
#     return validate_word(word)

# import redis
# from redis.commands.search.query import Query
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple
# from decouple import config


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION', default='localhost')
# REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
# REDIS_USERNAME = config('REDIS_USERNAME', default='')

# INDEX_NAME = "terms_idx"


# # =============================================================================
# # REDIS CONNECTION
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table using RediSearch"""
    
#     _client = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Get or create Redis client connection"""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
#             if not REDIS_LOCATION:
#                 print("ERROR: REDIS_LOCATION is empty or not set")
#                 return None
            
#             redis_config = {
#                 'host': REDIS_LOCATION,
#                 'port': REDIS_PORT,
#                 'db': REDIS_DB,
#                 'decode_responses': True,
#                 'socket_connect_timeout': 5,
#                 'socket_timeout': 5,
#             }
            
#             if REDIS_PASSWORD:
#                 redis_config['password'] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config['username'] = REDIS_USERNAME
            
#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client
            
#         except Exception as e:
#             print(f"Redis connection error: {e}")
#             return None


# # =============================================================================
# # QUERY ESCAPING HELPERS
# # =============================================================================

# def escape_query(text: str) -> str:
#     """Escape special characters for RediSearch query"""
#     if not text:
#         return ""
#     special_chars = ['@', '!', '{', '}', '(', ')', '|', '-', '=', '>', '<', 
#                      '[', ']', '"', "'", '~', '*', ':', '\\', '.', ',', '/', '&', '^', '$', '#', ';']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# def escape_tag(text: str) -> str:
#     """Escape special characters for TAG field values"""
#     if not text:
#         return ""
#     special_chars = [',', '.', '<', '>', '{', '}', '[', ']', '"', "'", ':', ';', 
#                      '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', 
#                      '=', '~', '|', '\\', '/']
#     result = text
#     for char in special_chars:
#         result = result.replace(char, f'\\{char}')
#     return result


# # =============================================================================
# # INDEX MANAGEMENT
# # =============================================================================

# def create_index() -> bool:
#     """Create the RediSearch index with STOPWORDS 0 (index all words)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         # Check if index exists
#         try:
#             client.ft(INDEX_NAME).info()
#             print(f"Index '{INDEX_NAME}' already exists")
#             return True
#         except:
#             pass
        
#         # Create index with STOPWORDS 0 to index ALL words including "and", "the", etc.
#         client.execute_command(
#             'FT.CREATE', INDEX_NAME,
#             'ON', 'HASH',
#             'PREFIX', '1', 'term:',
#             'STOPWORDS', '0',  # Index ALL words, no stopwords
#             'SCHEMA',
#             'term', 'TEXT', 'WEIGHT', '5.0',
#             'display', 'TEXT', 'WEIGHT', '3.0',
#             'category', 'TAG', 'SORTABLE',
#             'description', 'TEXT', 'WEIGHT', '1.0',
#             'pos', 'TAG',
#             'entity_type', 'TAG',
#             'rank', 'NUMERIC', 'SORTABLE'
#         )
#         print(f"Index '{INDEX_NAME}' created successfully with STOPWORDS 0")
#         return True
        
#     except Exception as e:
#         print(f"Error creating index: {e}")
#         return False


# def drop_index() -> bool:
#     """Drop the index (keeps the data)"""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False
    
#     try:
#         client.ft(INDEX_NAME).dropindex(delete_documents=False)
#         print(f"Index '{INDEX_NAME}' dropped")
#         return True
#     except Exception as e:
#         print(f"Error dropping index: {e}")
#         return False


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """Extract the base term from a document ID (term:xxx:category -> xxx)"""
#     if not member:
#         return ""
    
#     # Remove 'term:' prefix if present
#     if member.startswith('term:'):
#         member = member[5:]
    
#     # Find last colon and remove category suffix
#     parts = member.rsplit(':', 1)
#     if len(parts) == 2:
#         potential_category = parts[1].lower()
#         category_keywords = {'city', 'country', 'state', 'us_city', 'us_state', 
#                             'continent', 'word', 'culture', 'business', 'education',
#                             'fashion', 'food', 'health', 'music', 'sport', 'tech',
#                             'dictionary_word'}
#         if potential_category in category_keywords or len(potential_category) < 15:
#             return parts[0]
    
#     return member


# def parse_search_doc(doc) -> Dict[str, Any]:
#     """Parse a RediSearch document into a standard dict format"""
#     try:
#         rank_val = getattr(doc, 'rank', 0)
#         if rank_val:
#             try:
#                 rank_val = int(float(rank_val))
#             except (ValueError, TypeError):
#                 rank_val = 0
#         else:
#             rank_val = 0
        
#         term = getattr(doc, 'term', '')
        
#         return {
#             'id': doc.id,
#             'member': doc.id,
#             'term': term,
#             'display': getattr(doc, 'display', term),
#             'description': getattr(doc, 'description', ''),
#             'category': getattr(doc, 'category', ''),
#             'entity_type': getattr(doc, 'entity_type', ''),
#             'pos': getattr(doc, 'pos', ''),
#             'rank': rank_val,
#             'exists': True,
#         }
#     except Exception as e:
#         print(f"Error parsing doc: {e}")
#         return {}


# # =============================================================================
# # CORE SEARCH FUNCTIONS
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term from Redis hash.
#     Uses direct HGETALL for single lookups.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not member:
#         return None
    
#     try:
#         hash_key = member if member.startswith('term:') else f'term:{member}'
#         metadata = client.hgetall(hash_key)
        
#         if metadata:
#             base_term = extract_base_term(member)
#             try:
#                 rank_val = int(float(metadata.get('rank', 0)))
#             except (ValueError, TypeError):
#                 rank_val = 0
            
#             return {
#                 'member': member,
#                 'term': metadata.get('term', base_term),
#                 'exists': True,
#                 'display': metadata.get('display', base_term),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'entity_type': metadata.get('entity_type', ''),
#                 'rank': rank_val,
#             }
#         return None
        
#     except Exception as e:
#         print(f"Error getting term metadata: {e}")
#         return None


# def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
#     """
#     Find exact matches for a term using RediSearch.
#     Returns list of matching documents.
    
#     FIX: Removed @term: prefix - RediSearch works better without field specifier
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
#         # FIX: Use simple quoted search instead of @term:"word"
#         # @term:"word" returns 0 results, "word" returns matches
#         query_str = f'"{escaped_term}"'
        
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, 10)
#         result = client.ft(INDEX_NAME).search(query)
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 # Only include exact matches (term field equals our search term)
#                 if parsed.get('term', '').lower() == term_lower:
#                     matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Exact match error: {e}")
#         return []


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """
#     Get terms that start with the given prefix using RediSearch.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []
    
#     prefix_lower = prefix.lower().strip()
#     if len(prefix_lower) < 1:
#         return []
    
#     try:
#         escaped_prefix = escape_query(prefix_lower)
#         # FIX: Simplified query without @term:
#         query_str = f"{escaped_prefix}*"
        
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 matches.append(parsed)
        
#         return matches
        
#     except Exception as e:
#         print(f"Prefix match error: {e}")
#         return []


# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get prefix matches sorted by rank.
#     Same as get_prefix_matches but kept for backwards compatibility.
#     """
#     return get_prefix_matches(prefix, limit=limit)


# # =============================================================================
# # FUZZY SEARCH (SPELL CORRECTION)
# # =============================================================================

# def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get fuzzy matches using RediSearch Levenshtein distance.
#     %term% = 1 edit distance
#     %%term%% = 2 edit distance
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if len(term_lower) < 3:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
        
#         # Try 1 edit distance first
#         if max_distance >= 1:
#             # FIX: Simplified query without @term:
#             query_str = f"%{escaped_term}%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             result = client.ft(INDEX_NAME).search(query)
            
#             if result.docs:
#                 matches = []
#                 for doc in result.docs:
#                     parsed = parse_search_doc(doc)
#                     if parsed:
#                         parsed['distance'] = damerau_levenshtein_distance(
#                             term_lower, parsed['term'].lower()
#                         )
#                         matches.append(parsed)
                
#                 matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                 return matches[:limit]
        
#         # Try 2 edit distance if no results
#         if max_distance >= 2:
#             # FIX: Simplified query without @term:
#             query_str = f"%%{escaped_term}%%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             result = client.ft(INDEX_NAME).search(query)
            
#             matches = []
#             for doc in result.docs:
#                 parsed = parse_search_doc(doc)
#                 if parsed:
#                     parsed['distance'] = damerau_levenshtein_distance(
#                         term_lower, parsed['term'].lower()
#                     )
#                     matches.append(parsed)
            
#             matches.sort(key=lambda x: (x['distance'], -x['rank']))
#             return matches[:limit]
        
#         return []
        
#     except Exception as e:
#         print(f"Fuzzy match error: {e}")
#         return []


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """Calculate the Damerau-Levenshtein distance between two strings."""
#     len1, len2 = len(s1), len(s2)
    
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j
    
#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i-1] == s2[j-1] else 1
            
#             d[i][j] = min(
#                 d[i-1][j] + 1,
#                 d[i][j-1] + 1,
#                 d[i-1][j-1] + cost
#             )
            
#             if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
#                 d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
#     return d[len1][len2]


# def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
#     """Calculate combined score for ranking suggestions."""
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # BATCH OPERATIONS
# # =============================================================================

# def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch get metadata for multiple terms using pipeline.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not members:
#         return {}
    
#     try:
#         pipeline = client.pipeline()
        
#         for member in members:
#             hash_key = member if member.startswith('term:') else f'term:{member}'
#             pipeline.hgetall(hash_key)
        
#         results = pipeline.execute()
        
#         metadata_dict = {}
#         for member, metadata in zip(members, results):
#             if metadata:
#                 base_term = extract_base_term(member)
#                 try:
#                     rank_val = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank_val = 0
                
#                 metadata_dict[member] = {
#                     'member': member,
#                     'term': metadata.get('term', base_term),
#                     'exists': True,
#                     'display': metadata.get('display', base_term),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'rank': rank_val,
#                 }
        
#         return metadata_dict
        
#     except Exception as e:
#         print(f"Batch metadata error: {e}")
#         return {}


# def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Validate multiple words using RediSearch.
#     Returns dict mapping word -> validation result with metadata.
#     """
#     if not words:
#         return {}
    
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {}
    
#     results = {}
    
#     try:
#         for word in words:
#             word_lower = word.lower().strip()
#             if not word_lower:
#                 continue
            
#             matches = get_exact_term_matches(word_lower)
            
#             if matches:
#                 results[word_lower] = {
#                     'is_valid': True,
#                     'word': word_lower,
#                     'member': matches[0].get('id', ''),
#                     'matches': matches,
#                     'metadata': matches[0]
#                 }
#             else:
#                 results[word_lower] = {
#                     'is_valid': False,
#                     'word': word_lower,
#                     'member': None,
#                     'matches': [],
#                     'metadata': {}
#                 }
        
#         return results
        
#     except Exception as e:
#         print(f"Batch validation error: {e}")
#         return {}


# # =============================================================================
# # VALIDATE WORD (SPELL CHECK)
# # =============================================================================

# def validate_word(
#     word: str,
#     _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Validate a single word and return correction if needed.
#     """
#     word_lower = word.lower().strip()
    
#     # O(1) lookup if pre-validated data available
#     if _pre_validated is not None and word_lower in _pre_validated:
#         pre = _pre_validated[word_lower]
#         if pre.get('is_valid'):
#             metadata = pre.get('metadata', {})
#             return {
#                 'word': word,
#                 'is_valid': True,
#                 'suggestion': None,
#                 'metadata': {
#                     'display': metadata.get('display', word_lower),
#                     'pos': metadata.get('pos', 'unknown'),
#                     'category': metadata.get('category', ''),
#                     'rank': metadata.get('rank', 0),
#                 }
#             }
    
#     # Check for exact match
#     exact_matches = get_exact_term_matches(word_lower)
    
#     if exact_matches:
#         metadata = exact_matches[0]
#         return {
#             'word': word,
#             'is_valid': True,
#             'suggestion': None,
#             'metadata': {
#                 'display': metadata.get('display', word_lower),
#                 'pos': metadata.get('pos', 'unknown'),
#                 'category': metadata.get('category', ''),
#                 'rank': metadata.get('rank', 0),
#             }
#         }
    
#     # Word not found - get fuzzy suggestion
#     result = get_suggestions(word_lower, limit=1, max_distance=2)
    
#     if result['suggestions']:
#         best = result['suggestions'][0]
#         return {
#             'word': word,
#             'is_valid': False,
#             'suggestion': best['term'],
#             'distance': best.get('distance', 0),
#             'score': best.get('score', 0),
#             'tier_used': result['tier_used'],
#             'metadata': {
#                 'display': best.get('display', ''),
#                 'pos': best.get('pos', 'unknown'),
#                 'category': best.get('category', ''),
#                 'rank': best.get('rank', 0),
#             }
#         }
    
#     return {
#         'word': word,
#         'is_valid': False,
#         'suggestion': None
#     }


# # =============================================================================
# # GET SUGGESTIONS (UNIFIED SEARCH)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.
#     Tries: Exact -> Prefix -> Fuzzy
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             for match in exact_matches[:limit]:
#                 match['distance'] = 0
#                 match['score'] = -match.get('rank', 0)
            
#             response['suggestions'] = exact_matches[:limit]
#             return response
        
#         # === TIER 2: PREFIX MATCH ===
#         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                 item['distance'] = distance
#                 item['score'] = calculate_score(distance, item.get('rank', 0))
            
#             prefix_results.sort(key=lambda x: x['score'])
#             response['suggestions'] = prefix_results[:limit]
#             return response
        
#         # === TIER 3: FUZZY MATCH ===
#         fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
        
#         if fuzzy_results:
#             response['tier_used'] = 'fuzzy'
            
#             filtered = []
#             for item in fuzzy_results:
#                 if item.get('distance', 99) <= max_distance:
#                     item['score'] = calculate_score(item['distance'], item.get('rank', 0))
#                     filtered.append(item)
            
#             filtered.sort(key=lambda x: x['score'])
#             response['suggestions'] = filtered[:limit]
#             return response
        
#         # === TIER 4: SHORTER PREFIX (fallback) ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 short_prefix = input_lower[:prefix_len]
#                 short_results = get_prefix_matches(short_prefix, limit=50)
                
#                 if short_results:
#                     response['tier_used'] = f'prefix_short_{prefix_len}'
                    
#                     filtered = []
#                     for item in short_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                         if distance <= max_distance:
#                             item['distance'] = distance
#                             item['score'] = calculate_score(distance, item.get('rank', 0))
#                             filtered.append(item)
                    
#                     if filtered:
#                         filtered.sort(key=lambda x: x['score'])
#                         response['suggestions'] = filtered[:limit]
#                         return response
        
#         # === NO RESULTS ===
#         response['tier_used'] = 'none'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # AUTOCOMPLETE
# # =============================================================================

# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Get autocomplete suggestions.
#     Returns list of suggestions sorted by rank.
#     """
#     if not prefix or len(prefix.strip()) < 2:
#         return []
    
#     result = get_suggestions(prefix.strip(), limit=limit)
#     return result.get('suggestions', [])


# # =============================================================================
# # TOP WORDS BY RANK
# # =============================================================================

# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """Get top words by rank using RediSearch."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []
    
#     try:
#         query = Query("*").sort_by('rank', asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Error getting top words: {e}")
#         return []


# # =============================================================================
# # FILTERED SEARCH
# # =============================================================================

# def search_by_category(
#     query_text: str, 
#     category: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search within a specific category."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not category:
#         return []
    
#     try:
#         escaped_query = escape_query(query_text.lower().strip())
#         escaped_category = escape_tag(category)
        
#         # FIX: Simplified query
#         query_str = f"{escaped_query}* @category:{{{escaped_category}}}"
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Category search error: {e}")
#         return []


# def search_by_entity_type(
#     query_text: str, 
#     entity_type: str, 
#     limit: int = 20
# ) -> List[Dict[str, Any]]:
#     """Search by entity type (unigram, bigram, trigram)."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not entity_type:
#         return []
    
#     try:
#         escaped_query = escape_query(query_text.lower().strip())
#         escaped_type = escape_tag(entity_type)
        
#         # FIX: Simplified query
#         query_str = f"{escaped_query}* @entity_type:{{{escaped_type}}}"
#         query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
#         result = client.ft(INDEX_NAME).search(query)
        
#         results = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 results.append(parsed)
        
#         return results
        
#     except Exception as e:
#         print(f"Entity type search error: {e}")
#         return []


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Check if query results exist in cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return None
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         cached_results = client.get(cache_key)
        
#         if cached_results:
#             return json.loads(cached_results)
#         return None
        
#     except Exception as e:
#         print(f"Cache check error: {e}")
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Save query results to cache."""
#     client = RedisLookupTable.get_client()
    
#     if not client:
#         return False
    
#     try:
#         normalized_query = query.lower().strip()
#         cache_key = f"query_cache:{normalized_query}"
        
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True
        
#     except Exception as e:
#         print(f"Cache save error: {e}")
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10,
#     return_validation_cache: bool = False
# ) -> Dict[str, Any]:
#     """
#     Main API endpoint function for Redis-based search preprocessing.
#     """
#     response = {
#         'success': True,
#         'query': query,
#         'normalized_query': '',
#         'terms': [],
#         'cache_hit': False,
#         'autocomplete': [],
#         'error': None
#     }
    
#     try:
#         # Handle autocomplete request
#         if autocomplete_prefix:
#             response['autocomplete'] = get_autocomplete(
#                 autocomplete_prefix, 
#                 limit=autocomplete_limit
#             )
#             return response
        
#         if not query or not query.strip():
#             response['error'] = 'Empty query'
#             response['success'] = False
#             return response
        
#         # Check cache
#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response['cache_hit'] = True
#                 response['terms'] = cached.get('terms', [])
#                 response['normalized_query'] = cached.get('normalized_query', '')
#                 return response
        
#         words = query.lower().split()
        
#         # Batch validate all words
#         validation_cache = batch_validate_words_redis(words)
        
#         terms = []
#         normalized_words = []
        
#         for i, word in enumerate(words):
#             word = word.strip()
#             if not word:
#                 continue
            
#             # Use pre-validated data
#             validation = validate_word(word, _pre_validated=validation_cache)
            
#             if validation['is_valid']:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': True,
#                     'display': validation['metadata']['display'],
#                     'pos': validation['metadata']['pos'],
#                     'category': validation['metadata']['category'],
#                     'rank': validation['metadata']['rank'],
#                     'metadata': validation['metadata']
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     'position': i + 1,
#                     'word': word,
#                     'exists': False,
#                     'suggestion': validation.get('suggestion'),
#                     'distance': validation.get('distance'),
#                     'score': validation.get('score'),
#                     'tier_used': validation.get('tier_used'),
#                     'metadata': validation.get('metadata', {})
#                 })
                
#                 if include_suggestions and validation.get('suggestion'):
#                     normalized_words.append(validation['suggestion'])
        
#         response['terms'] = terms
#         response['normalized_query'] = ' '.join(normalized_words)
        
#         # Include validation cache for downstream use
#         if return_validation_cache:
#             response['_validation_cache'] = validation_cache
        
#         # Save to cache
#         cache_data = {
#             'terms': terms,
#             'normalized_query': response['normalized_query']
#         }
#         save_to_cache(query, cache_data)
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response


# # =============================================================================
# # LEGACY FUNCTIONS (kept for backwards compatibility)
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates based on common typo patterns.
#     Kept for backwards compatibility - RediSearch fuzzy search is preferred.
#     """
#     candidates = set()
#     word_lower = word.lower()
#     length = len(word_lower)
    
#     if length < 2:
#         return candidates
    
#     alphabet = string.ascii_lowercase
#     vowels = 'aeiou'

#     keyboard_proximity = {
#         'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
#         'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
#         'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
#         'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
#         'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
#         'n': 'bhjm', 'm': 'njk'
#     }

#     # 1. Keyboard proximity substitutions
#     for i in range(length):
#         char = word_lower[i]
#         if char in keyboard_proximity:
#             for nearby_char in keyboard_proximity[char]:
#                 candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
#                 candidates.add(candidate)

#     # 2. Single character transposition
#     for i in range(length - 1):
#         candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
#         candidates.add(candidate)

#     # 3. Single character deletion
#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i+1:]
#         if candidate:
#             candidates.add(candidate)

#     # 4. Vowel substitution
#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidate = word_lower[:i] + v + word_lower[i+1:]
#                     candidates.add(candidate)

#     # 5. Single character insertion
#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidate = word_lower[:i] + char + word_lower[i:]
#                 candidates.add(candidate)
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     return set(list(candidates)[:max_candidates])


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """
#     Check if candidates exist in Redis.
#     Kept for backwards compatibility.
#     """
#     if not candidates:
#         return []
    
#     found = []
#     for candidate in list(candidates)[:50]:
#         matches = get_exact_term_matches(candidate)
#         if matches:
#             found.append(matches[0])
    
#     return found


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """
#     Check multiple bigrams.
#     Kept for backwards compatibility.
#     """
#     if not word_pairs:
#         return {}
    
#     results = {}
#     for w1, w2 in word_pairs:
#         bigram = f"{w1.lower()} {w2.lower()}"
#         matches = get_exact_term_matches(bigram)
#         if matches:
#             results[bigram] = matches[0]
    
#     return results


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for lookup_table with default settings."""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for autocomplete lookups."""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for spell checking a single word."""
#     return validate_word(word)


import redis
from redis.commands.search.query import Query
import json
import string
from typing import Optional, Dict, Any, List, Set, Tuple
from decouple import config


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_LOCATION = config('REDIS_LOCATION', default='localhost')
REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)
REDIS_DB = config('REDIS_DB', default=0, cast=int)
REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
REDIS_USERNAME = config('REDIS_USERNAME', default='')

INDEX_NAME = "terms_idx"


# =============================================================================
# REDIS CONNECTION
# =============================================================================

class RedisLookupTable:
    """Redis-based lookup table using RediSearch"""
    
    _client = None
    
    @classmethod
    def get_client(cls) -> Optional[redis.Redis]:
        """Get or create Redis client connection"""
        if cls._client is not None:
            try:
                cls._client.ping()
                return cls._client
            except (redis.ConnectionError, redis.TimeoutError):
                cls._client = None
        
        try:
            if not REDIS_LOCATION:
                print("ERROR: REDIS_LOCATION is empty or not set")
                return None
            
            redis_config = {
                'host': REDIS_LOCATION,
                'port': REDIS_PORT,
                'db': REDIS_DB,
                'decode_responses': True,
                'socket_connect_timeout': 5,
                'socket_timeout': 5,
            }
            
            if REDIS_PASSWORD:
                redis_config['password'] = REDIS_PASSWORD
            if REDIS_USERNAME:
                redis_config['username'] = REDIS_USERNAME
            
            cls._client = redis.Redis(**redis_config)
            cls._client.ping()
            return cls._client
            
        except Exception as e:
            print(f"Redis connection error: {e}")
            return None


# =============================================================================
# QUERY ESCAPING HELPERS
# =============================================================================

def escape_query(text: str) -> str:
    """Escape special characters for RediSearch query"""
    if not text:
        return ""
    special_chars = ['@', '!', '{', '}', '(', ')', '|', '-', '=', '>', '<', 
                     '[', ']', '"', "'", '~', '*', ':', '\\', '.', ',', '/', '&', '^', '$', '#', ';']
    result = text
    for char in special_chars:
        result = result.replace(char, f'\\{char}')
    return result


def escape_tag(text: str) -> str:
    """Escape special characters for TAG field values"""
    if not text:
        return ""
    special_chars = [',', '.', '<', '>', '{', '}', '[', ']', '"', "'", ':', ';', 
                     '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', 
                     '=', '~', '|', '\\', '/']
    result = text
    for char in special_chars:
        result = result.replace(char, f'\\{char}')
    return result


# =============================================================================
# INDEX MANAGEMENT
# =============================================================================

def create_index() -> bool:
    """Create the RediSearch index with STOPWORDS 0 (index all words)"""
    client = RedisLookupTable.get_client()
    if not client:
        return False
    
    try:
        # Check if index exists
        try:
            client.ft(INDEX_NAME).info()
            print(f"Index '{INDEX_NAME}' already exists")
            return True
        except:
            pass
        
        # Create index with STOPWORDS 0 to index ALL words including "and", "the", etc.
        client.execute_command(
            'FT.CREATE', INDEX_NAME,
            'ON', 'HASH',
            'PREFIX', '1', 'term:',
            'STOPWORDS', '0',  # Index ALL words, no stopwords
            'SCHEMA',
            'term', 'TEXT', 'WEIGHT', '5.0',
            'display', 'TEXT', 'WEIGHT', '3.0',
            'category', 'TAG', 'SORTABLE',
            'description', 'TEXT', 'WEIGHT', '1.0',
            'pos', 'TAG',
            'entity_type', 'TAG',
            'rank', 'NUMERIC', 'SORTABLE'
        )
        print(f"Index '{INDEX_NAME}' created successfully with STOPWORDS 0")
        return True
        
    except Exception as e:
        print(f"Error creating index: {e}")
        return False


def drop_index() -> bool:
    """Drop the index (keeps the data)"""
    client = RedisLookupTable.get_client()
    if not client:
        return False
    
    try:
        client.ft(INDEX_NAME).dropindex(delete_documents=False)
        print(f"Index '{INDEX_NAME}' dropped")
        return True
    except Exception as e:
        print(f"Error dropping index: {e}")
        return False


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_base_term(member: str) -> str:
    """Extract the base term from a document ID (term:xxx:category -> xxx)"""
    if not member:
        return ""
    
    # Remove 'term:' prefix if present
    if member.startswith('term:'):
        member = member[5:]
    
    # Find last colon and remove category suffix
    parts = member.rsplit(':', 1)
    if len(parts) == 2:
        potential_category = parts[1].lower()
        category_keywords = {'city', 'country', 'state', 'us_city', 'us_state', 
                            'continent', 'word', 'culture', 'business', 'education',
                            'fashion', 'food', 'health', 'music', 'sport', 'tech',
                            'dictionary_word'}
        if potential_category in category_keywords or len(potential_category) < 15:
            return parts[0]
    
    return member


def parse_search_doc(doc) -> Dict[str, Any]:
    """Parse a RediSearch document into a standard dict format"""
    try:
        rank_val = getattr(doc, 'rank', 0)
        if rank_val:
            try:
                rank_val = int(float(rank_val))
            except (ValueError, TypeError):
                rank_val = 0
        else:
            rank_val = 0
        
        term = getattr(doc, 'term', '')
        
        return {
            'id': doc.id,
            'member': doc.id,
            'term': term,
            'display': getattr(doc, 'display', term),
            'description': getattr(doc, 'description', ''),
            'category': getattr(doc, 'category', ''),
            'entity_type': getattr(doc, 'entity_type', ''),
            'pos': getattr(doc, 'pos', ''),
            'rank': rank_val,
            'exists': True,
        }
    except Exception as e:
        print(f"Error parsing doc: {e}")
        return {}


# =============================================================================
# CORE SEARCH FUNCTIONS
# =============================================================================

def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a term from Redis hash.
    Uses direct HGETALL for single lookups.
    """
    client = RedisLookupTable.get_client()
    if not client or not member:
        return None
    
    try:
        hash_key = member if member.startswith('term:') else f'term:{member}'
        metadata = client.hgetall(hash_key)
        
        if metadata:
            base_term = extract_base_term(member)
            try:
                rank_val = int(float(metadata.get('rank', 0)))
            except (ValueError, TypeError):
                rank_val = 0
            
            return {
                'member': member,
                'term': metadata.get('term', base_term),
                'exists': True,
                'display': metadata.get('display', base_term),
                'pos': metadata.get('pos', 'unknown'),
                'category': metadata.get('category', ''),
                'description': metadata.get('description', ''),
                'entity_type': metadata.get('entity_type', ''),
                'rank': rank_val,
            }
        return None
        
    except Exception as e:
        print(f"Error getting term metadata: {e}")
        return None


def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
    """
    Find exact matches for a term.
    
    Uses DIRECT KEY LOOKUP instead of RediSearch full-text search.
    This is more reliable for exact matching because:
    - RediSearch "and" returns 445 docs containing "and" anywhere
    - Direct lookup for "term:and:*" finds only exact matches
    
    Key pattern: term:{word}:{category}
    Examples: term:and:dictionary_word, term:tuskegee:us_city
    """
    client = RedisLookupTable.get_client()
    if not client or not term:
        return []
    
    term_lower = term.lower().strip()
    if not term_lower:
        return []
    
    # Handle multi-word terms (replace spaces with underscores in key)
    term_key = term_lower.replace(' ', '_')
    
    try:
        # DIRECT KEY LOOKUP: Find all keys matching term:{word}:*
        pattern = f"term:{term_key}:*"
        keys = client.keys(pattern)
        
        if not keys:
            return []
        
        matches = []
        for key in keys:
            metadata = client.hgetall(key)
            if metadata:
                # Parse the metadata
                try:
                    rank_val = int(float(metadata.get('rank', 0)))
                except (ValueError, TypeError):
                    rank_val = 0
                
                parsed = {
                    'id': key,
                    'member': key,
                    'term': metadata.get('term', term_lower),
                    'display': metadata.get('display', term_lower),
                    'description': metadata.get('description', ''),
                    'category': metadata.get('category', ''),
                    'entity_type': metadata.get('entity_type', ''),
                    'pos': metadata.get('pos', ''),
                    'rank': rank_val,
                    'exists': True,
                }
                matches.append(parsed)
        
        # Sort by rank descending (highest rank first)
        matches.sort(key=lambda x: x.get('rank', 0), reverse=True)
        
        return matches
        
    except Exception as e:
        print(f"Exact match error: {e}")
        return []


def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get terms that start with the given prefix using RediSearch.
    """
    client = RedisLookupTable.get_client()
    if not client or not prefix:
        return []
    
    prefix_lower = prefix.lower().strip()
    if len(prefix_lower) < 1:
        return []
    
    try:
        escaped_prefix = escape_query(prefix_lower)
        # FIX: Simplified query without @term:
        query_str = f"{escaped_prefix}*"
        
        query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)
        
        matches = []
        for doc in result.docs:
            parsed = parse_search_doc(doc)
            if parsed:
                matches.append(parsed)
        
        return matches
        
    except Exception as e:
        print(f"Prefix match error: {e}")
        return []


def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get prefix matches sorted by rank.
    Same as get_prefix_matches but kept for backwards compatibility.
    """
    return get_prefix_matches(prefix, limit=limit)


# =============================================================================
# FUZZY SEARCH (SPELL CORRECTION)
# =============================================================================

def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
    """
    Get fuzzy matches using RediSearch Levenshtein distance.
    %term% = 1 edit distance
    %%term%% = 2 edit distance
    """
    client = RedisLookupTable.get_client()
    if not client or not term:
        return []
    
    term_lower = term.lower().strip()
    if len(term_lower) < 3:
        return []
    
    try:
        escaped_term = escape_query(term_lower)
        
        # Try 1 edit distance first
        if max_distance >= 1:
            # FIX: Simplified query without @term:
            query_str = f"%{escaped_term}%"
            query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
            result = client.ft(INDEX_NAME).search(query)
            
            if result.docs:
                matches = []
                for doc in result.docs:
                    parsed = parse_search_doc(doc)
                    if parsed:
                        parsed['distance'] = damerau_levenshtein_distance(
                            term_lower, parsed['term'].lower()
                        )
                        matches.append(parsed)
                
                matches.sort(key=lambda x: (x['distance'], -x['rank']))
                return matches[:limit]
        
        # Try 2 edit distance if no results
        if max_distance >= 2:
            # FIX: Simplified query without @term:
            query_str = f"%%{escaped_term}%%"
            query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
            result = client.ft(INDEX_NAME).search(query)
            
            matches = []
            for doc in result.docs:
                parsed = parse_search_doc(doc)
                if parsed:
                    parsed['distance'] = damerau_levenshtein_distance(
                        term_lower, parsed['term'].lower()
                    )
                    matches.append(parsed)
            
            matches.sort(key=lambda x: (x['distance'], -x['rank']))
            return matches[:limit]
        
        return []
        
    except Exception as e:
        print(f"Fuzzy match error: {e}")
        return []


# =============================================================================
# DAMERAU-LEVENSHTEIN DISTANCE
# =============================================================================

def damerau_levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate the Damerau-Levenshtein distance between two strings."""
    len1, len2 = len(s1), len(s2)
    
    d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
    
    for i in range(len1 + 1):
        d[i][0] = i
    for j in range(len2 + 1):
        d[0][j] = j
    
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if s1[i-1] == s2[j-1] else 1
            
            d[i][j] = min(
                d[i-1][j] + 1,
                d[i][j-1] + 1,
                d[i-1][j-1] + cost
            )
            
            if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
                d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
    return d[len1][len2]


def calculate_score(distance: int, rank: int, max_rank: int = 10000000) -> float:
    """Calculate combined score for ranking suggestions."""
    rank_bonus = min(rank, max_rank) / max_rank
    return distance - rank_bonus


# =============================================================================
# BATCH OPERATIONS
# =============================================================================

def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Batch get metadata for multiple terms using pipeline.
    """
    client = RedisLookupTable.get_client()
    if not client or not members:
        return {}
    
    try:
        pipeline = client.pipeline()
        
        for member in members:
            hash_key = member if member.startswith('term:') else f'term:{member}'
            pipeline.hgetall(hash_key)
        
        results = pipeline.execute()
        
        metadata_dict = {}
        for member, metadata in zip(members, results):
            if metadata:
                base_term = extract_base_term(member)
                try:
                    rank_val = int(float(metadata.get('rank', 0)))
                except (ValueError, TypeError):
                    rank_val = 0
                
                metadata_dict[member] = {
                    'member': member,
                    'term': metadata.get('term', base_term),
                    'exists': True,
                    'display': metadata.get('display', base_term),
                    'pos': metadata.get('pos', 'unknown'),
                    'category': metadata.get('category', ''),
                    'description': metadata.get('description', ''),
                    'entity_type': metadata.get('entity_type', ''),
                    'rank': rank_val,
                }
        
        return metadata_dict
        
    except Exception as e:
        print(f"Batch metadata error: {e}")
        return {}


def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Validate multiple words using RediSearch.
    Returns dict mapping word -> validation result with metadata.
    """
    if not words:
        return {}
    
    client = RedisLookupTable.get_client()
    if not client:
        return {}
    
    results = {}
    
    try:
        for word in words:
            word_lower = word.lower().strip()
            if not word_lower:
                continue
            
            matches = get_exact_term_matches(word_lower)
            
            if matches:
                results[word_lower] = {
                    'is_valid': True,
                    'word': word_lower,
                    'member': matches[0].get('id', ''),
                    'matches': matches,
                    'metadata': matches[0]
                }
            else:
                results[word_lower] = {
                    'is_valid': False,
                    'word': word_lower,
                    'member': None,
                    'matches': [],
                    'metadata': {}
                }
        
        return results
        
    except Exception as e:
        print(f"Batch validation error: {e}")
        return {}


# =============================================================================
# VALIDATE WORD (SPELL CHECK)
# =============================================================================

def validate_word(
    word: str,
    _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Validate a single word and return correction if needed.
    """
    word_lower = word.lower().strip()
    
    # O(1) lookup if pre-validated data available
    if _pre_validated is not None and word_lower in _pre_validated:
        pre = _pre_validated[word_lower]
        if pre.get('is_valid'):
            metadata = pre.get('metadata', {})
            return {
                'word': word,
                'is_valid': True,
                'suggestion': None,
                'metadata': {
                    'display': metadata.get('display', word_lower),
                    'pos': metadata.get('pos', 'unknown'),
                    'category': metadata.get('category', ''),
                    'rank': metadata.get('rank', 0),
                }
            }
    
    # Check for exact match
    exact_matches = get_exact_term_matches(word_lower)
    
    if exact_matches:
        metadata = exact_matches[0]
        return {
            'word': word,
            'is_valid': True,
            'suggestion': None,
            'metadata': {
                'display': metadata.get('display', word_lower),
                'pos': metadata.get('pos', 'unknown'),
                'category': metadata.get('category', ''),
                'rank': metadata.get('rank', 0),
            }
        }
    
    # Word not found - get fuzzy suggestion
    result = get_suggestions(word_lower, limit=1, max_distance=2)
    
    if result['suggestions']:
        best = result['suggestions'][0]
        return {
            'word': word,
            'is_valid': False,
            'suggestion': best['term'],
            'distance': best.get('distance', 0),
            'score': best.get('score', 0),
            'tier_used': result['tier_used'],
            'metadata': {
                'display': best.get('display', ''),
                'pos': best.get('pos', 'unknown'),
                'category': best.get('category', ''),
                'rank': best.get('rank', 0),
            }
        }
    
    return {
        'word': word,
        'is_valid': False,
        'suggestion': None
    }


# =============================================================================
# GET SUGGESTIONS (UNIFIED SEARCH)
# =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.
#     Tries: Exact -> Prefix -> Fuzzy
#     """
#     client = RedisLookupTable.get_client()
    
#     response = {
#         'success': True,
#         'input': input_text,
#         'suggestions': [],
#         'exact_match': False,
#         'tier_used': None,
#         'error': None
#     }
    
#     if not client:
#         response['success'] = False
#         response['error'] = 'Redis connection failed'
#         return response
    
#     if not input_text or not input_text.strip():
#         response['success'] = False
#         response['error'] = 'Empty input'
#         return response
    
#     input_lower = input_text.lower().strip()
    
#     try:
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             response['tier_used'] = 'exact'
            
#             for match in exact_matches[:limit]:
#                 match['distance'] = 0
#                 match['score'] = -match.get('rank', 0)
            
#             response['suggestions'] = exact_matches[:limit]
#             return response
        
#         # === TIER 2: PREFIX MATCH ===
#         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             response['tier_used'] = 'prefix'
            
#             for item in prefix_results:
#                 distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                 item['distance'] = distance
#                 item['score'] = calculate_score(distance, item.get('rank', 0))
            
#             prefix_results.sort(key=lambda x: x['score'])
#             response['suggestions'] = prefix_results[:limit]
#             return response
        
#         # === TIER 3: FUZZY MATCH ===
#         fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
        
#         if fuzzy_results:
#             response['tier_used'] = 'fuzzy'
            
#             filtered = []
#             for item in fuzzy_results:
#                 if item.get('distance', 99) <= max_distance:
#                     item['score'] = calculate_score(item['distance'], item.get('rank', 0))
#                     filtered.append(item)
            
#             filtered.sort(key=lambda x: x['score'])
#             response['suggestions'] = filtered[:limit]
#             return response
        
#         # === TIER 4: SHORTER PREFIX (fallback) ===
#         for prefix_len in [3, 2]:
#             if len(input_lower) >= prefix_len:
#                 short_prefix = input_lower[:prefix_len]
#                 short_results = get_prefix_matches(short_prefix, limit=50)
                
#                 if short_results:
#                     response['tier_used'] = f'prefix_short_{prefix_len}'
                    
#                     filtered = []
#                     for item in short_results:
#                         distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
#                         if distance <= max_distance:
#                             item['distance'] = distance
#                             item['score'] = calculate_score(distance, item.get('rank', 0))
#                             filtered.append(item)
                    
#                     if filtered:
#                         filtered.sort(key=lambda x: x['score'])
#                         response['suggestions'] = filtered[:limit]
#                         return response
        
#         # === NO RESULTS ===
#         response['tier_used'] = 'none'
#         response['suggestions'] = []
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response
def get_suggestions(
    input_text: str,
    limit: int = 10,
    max_distance: int = 2,
    category: Optional[str] = None
) -> Dict[str, Any]:
    """
    Unified suggestion function using RediSearch.
    Returns COMBINED results: Exact + Prefix + Fuzzy
    Sorted by rank (highest first).
    """
    client = RedisLookupTable.get_client()
    
    response = {
        'success': True,
        'input': input_text,
        'suggestions': [],
        'exact_match': False,
        'tier_used': None,
        'error': None
    }
    
    if not client:
        response['success'] = False
        response['error'] = 'Redis connection failed'
        return response
    
    if not input_text or not input_text.strip():
        response['success'] = False
        response['error'] = 'Empty input'
        return response
    
    input_lower = input_text.lower().strip()
    
    try:
        all_results = []
        seen_terms = set()
        tiers_used = []
        
        # === TIER 1: EXACT MATCH ===
        exact_matches = get_exact_term_matches(input_lower)
        
        if exact_matches:
            response['exact_match'] = True
            tiers_used.append('exact')
            
            for match in exact_matches:
                term_lower = match.get('term', '').lower()
                if term_lower not in seen_terms:
                    match['distance'] = 0
                    match['score'] = -match.get('rank', 0)  # Negative so higher rank = lower score = better
                    all_results.append(match)
                    seen_terms.add(term_lower)
        
        # === TIER 2: PREFIX MATCH (always run, don't stop at exact) ===
        prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
        if prefix_results:
            tiers_used.append('prefix')
            
            for item in prefix_results:
                term_lower = item.get('term', '').lower()
                if term_lower not in seen_terms:
                    distance = damerau_levenshtein_distance(input_lower, term_lower)
                    item['distance'] = distance
                    item['score'] = calculate_score(distance, item.get('rank', 0))
                    all_results.append(item)
                    seen_terms.add(term_lower)
        
        # === TIER 3: FUZZY MATCH (only if we have few results) ===
        if len(all_results) < limit:
            fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
            
            if fuzzy_results:
                tiers_used.append('fuzzy')
                
                for item in fuzzy_results:
                    term_lower = item.get('term', '').lower()
                    if term_lower not in seen_terms:
                        if item.get('distance', 99) <= max_distance:
                            item['score'] = calculate_score(item['distance'], item.get('rank', 0))
                            all_results.append(item)
                            seen_terms.add(term_lower)
        
        # === SORT ALL RESULTS ===
        # Sort by: rank (highest first), then distance (lowest first)
        all_results.sort(key=lambda x: (-x.get('rank', 0), x.get('distance', 99)))
        
        response['suggestions'] = all_results[:limit]
        response['tier_used'] = '+'.join(tiers_used) if tiers_used else 'none'
        
        return response
        
    except Exception as e:
        response['success'] = False
        response['error'] = str(e)
        return response
# ```

# ## What Changed

# | Before | After |
# |--------|-------|
# | Find exact → STOP | Find exact → CONTINUE |
# | Returns only 1 result type | Combines exact + prefix + fuzzy |
# | Sort by score (distance - rank) | Sort by rank first, then distance |
# | Stops at first tier with results | Runs all tiers, deduplicates |

# ## After Replacing

# 1. Restart Django
# 2. Type "africa" 
# 3. Should now see: Africa, African, African American, etc. (sorted by rank)

# **Note:** You still need to add "african" to Redis for it to appear:
# ```
# HSET term:african:dictionary_word term "african" display "African" category "Dictionary Word" description "" pos "['adjective']" entity_type "unigram" rank "1000000"


# =============================================================================
# AUTOCOMPLETE
# =============================================================================

def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get autocomplete suggestions.
    Returns list of suggestions sorted by rank.
    """
    if not prefix or len(prefix.strip()) < 2:
        return []
    
    result = get_suggestions(prefix.strip(), limit=limit)
    return result.get('suggestions', [])


# =============================================================================
# TOP WORDS BY RANK
# =============================================================================

def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
    """Get top words by rank using RediSearch."""
    client = RedisLookupTable.get_client()
    if not client:
        return []
    
    try:
        query = Query("*").sort_by('rank', asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)
        
        results = []
        for doc in result.docs:
            parsed = parse_search_doc(doc)
            if parsed:
                results.append(parsed)
        
        return results
        
    except Exception as e:
        print(f"Error getting top words: {e}")
        return []


# =============================================================================
# FILTERED SEARCH
# =============================================================================

def search_by_category(
    query_text: str, 
    category: str, 
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Search within a specific category."""
    client = RedisLookupTable.get_client()
    if not client or not query_text or not category:
        return []
    
    try:
        escaped_query = escape_query(query_text.lower().strip())
        escaped_category = escape_tag(category)
        
        # FIX: Simplified query
        query_str = f"{escaped_query}* @category:{{{escaped_category}}}"
        query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
        result = client.ft(INDEX_NAME).search(query)
        
        results = []
        for doc in result.docs:
            parsed = parse_search_doc(doc)
            if parsed:
                results.append(parsed)
        
        return results
        
    except Exception as e:
        print(f"Category search error: {e}")
        return []


def search_by_entity_type(
    query_text: str, 
    entity_type: str, 
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Search by entity type (unigram, bigram, trigram)."""
    client = RedisLookupTable.get_client()
    if not client or not query_text or not entity_type:
        return []
    
    try:
        escaped_query = escape_query(query_text.lower().strip())
        escaped_type = escape_tag(entity_type)
        
        # FIX: Simplified query
        query_str = f"{escaped_query}* @entity_type:{{{escaped_type}}}"
        query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
        
        result = client.ft(INDEX_NAME).search(query)
        
        results = []
        for doc in result.docs:
            parsed = parse_search_doc(doc)
            if parsed:
                results.append(parsed)
        
        return results
        
    except Exception as e:
        print(f"Entity type search error: {e}")
        return []


# =============================================================================
# CACHE FUNCTIONS
# =============================================================================

def check_cache(query: str) -> Optional[Dict[str, Any]]:
    """Check if query results exist in cache."""
    client = RedisLookupTable.get_client()
    
    if not client:
        return None
    
    try:
        normalized_query = query.lower().strip()
        cache_key = f"query_cache:{normalized_query}"
        
        cached_results = client.get(cache_key)
        
        if cached_results:
            return json.loads(cached_results)
        return None
        
    except Exception as e:
        print(f"Cache check error: {e}")
        return None


def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
    """Save query results to cache."""
    client = RedisLookupTable.get_client()
    
    if not client:
        return False
    
    try:
        normalized_query = query.lower().strip()
        cache_key = f"query_cache:{normalized_query}"
        
        client.setex(cache_key, ttl, json.dumps(results))
        return True
        
    except Exception as e:
        print(f"Cache save error: {e}")
        return False


# =============================================================================
# MAIN API FUNCTION
# =============================================================================

def lookup_table(
    query: str,
    check_cache_first: bool = True,
    include_suggestions: bool = True,
    autocomplete_prefix: Optional[str] = None,
    autocomplete_limit: int = 10,
    return_validation_cache: bool = False
) -> Dict[str, Any]:
    """
    Main API endpoint function for Redis-based search preprocessing.
    """
    response = {
        'success': True,
        'query': query,
        'normalized_query': '',
        'terms': [],
        'cache_hit': False,
        'autocomplete': [],
        'error': None
    }
    
    try:
        # Handle autocomplete request
        if autocomplete_prefix:
            response['autocomplete'] = get_autocomplete(
                autocomplete_prefix, 
                limit=autocomplete_limit
            )
            return response
        
        if not query or not query.strip():
            response['error'] = 'Empty query'
            response['success'] = False
            return response
        
        # Check cache
        if check_cache_first:
            cached = check_cache(query)
            if cached:
                response['cache_hit'] = True
                response['terms'] = cached.get('terms', [])
                response['normalized_query'] = cached.get('normalized_query', '')
                return response
        
        words = query.lower().split()
        
        # Batch validate all words
        validation_cache = batch_validate_words_redis(words)
        
        terms = []
        normalized_words = []
        
        for i, word in enumerate(words):
            word = word.strip()
            if not word:
                continue
            
            # Use pre-validated data
            validation = validate_word(word, _pre_validated=validation_cache)
            
            if validation['is_valid']:
                terms.append({
                    'position': i + 1,
                    'word': word,
                    'exists': True,
                    'display': validation['metadata']['display'],
                    'pos': validation['metadata']['pos'],
                    'category': validation['metadata']['category'],
                    'rank': validation['metadata']['rank'],
                    'metadata': validation['metadata']
                })
                normalized_words.append(word)
            else:
                terms.append({
                    'position': i + 1,
                    'word': word,
                    'exists': False,
                    'suggestion': validation.get('suggestion'),
                    'distance': validation.get('distance'),
                    'score': validation.get('score'),
                    'tier_used': validation.get('tier_used'),
                    'metadata': validation.get('metadata', {})
                })
                
                if include_suggestions and validation.get('suggestion'):
                    normalized_words.append(validation['suggestion'])
        
        response['terms'] = terms
        response['normalized_query'] = ' '.join(normalized_words)
        
        # Include validation cache for downstream use
        if return_validation_cache:
            response['_validation_cache'] = validation_cache
        
        # Save to cache
        cache_data = {
            'terms': terms,
            'normalized_query': response['normalized_query']
        }
        save_to_cache(query, cache_data)
        
        return response
        
    except Exception as e:
        response['success'] = False
        response['error'] = str(e)
        return response


# =============================================================================
# LEGACY FUNCTIONS (kept for backwards compatibility)
# =============================================================================

def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
    """
    Generate spelling candidates based on common typo patterns.
    Kept for backwards compatibility - RediSearch fuzzy search is preferred.
    """
    candidates = set()
    word_lower = word.lower()
    length = len(word_lower)
    
    if length < 2:
        return candidates
    
    alphabet = string.ascii_lowercase
    vowels = 'aeiou'

    keyboard_proximity = {
        'q': 'wa', 'w': 'qeas', 'e': 'wrds', 'r': 'etdf', 't': 'ryfg',
        'y': 'tugh', 'u': 'yihj', 'i': 'uojk', 'o': 'ipkl', 'p': 'ol',
        'a': 'qwsz', 's': 'awedxz', 'd': 'serfcx', 'f': 'drtgvc', 'g': 'ftyhbv',
        'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
        'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn',
        'n': 'bhjm', 'm': 'njk'
    }

    # 1. Keyboard proximity substitutions
    for i in range(length):
        char = word_lower[i]
        if char in keyboard_proximity:
            for nearby_char in keyboard_proximity[char]:
                candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
                candidates.add(candidate)

    # 2. Single character transposition
    for i in range(length - 1):
        candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
        candidates.add(candidate)

    # 3. Single character deletion
    for i in range(length):
        candidate = word_lower[:i] + word_lower[i+1:]
        if candidate:
            candidates.add(candidate)

    # 4. Vowel substitution
    for i in range(length):
        if word_lower[i] in vowels:
            for v in vowels:
                if v != word_lower[i]:
                    candidate = word_lower[:i] + v + word_lower[i+1:]
                    candidates.add(candidate)

    # 5. Single character insertion
    if len(candidates) < max_candidates // 2:
        for i in range(length + 1):
            for char in alphabet:
                candidate = word_lower[:i] + char + word_lower[i:]
                candidates.add(candidate)
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= max_candidates:
                break

    return set(list(candidates)[:max_candidates])


def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
    """
    Check if candidates exist in Redis.
    Kept for backwards compatibility.
    """
    if not candidates:
        return []
    
    found = []
    for candidate in list(candidates)[:50]:
        matches = get_exact_term_matches(candidate)
        if matches:
            found.append(matches[0])
    
    return found


def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    Check multiple bigrams.
    Kept for backwards compatibility.
    """
    if not word_pairs:
        return {}
    
    results = {}
    for w1, w2 in word_pairs:
        bigram = f"{w1.lower()} {w2.lower()}"
        matches = get_exact_term_matches(bigram)
        if matches:
            results[bigram] = matches[0]
    
    return results


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def lookup(query: str) -> Dict[str, Any]:
    """Shorthand for lookup_table with default settings."""
    return lookup_table(query)


def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Shorthand for autocomplete lookups."""
    return get_autocomplete(prefix, limit)


def spell_check(word: str) -> Dict[str, Any]:
    """Shorthand for spell checking a single word."""
    return validate_word(word)