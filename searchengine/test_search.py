# import os
# import sys
# import django

# # Add the parent directory to path
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')
# django.setup()

# from searchengine.searchapi import get_exact_term_matches, batch_validate_words_redis, lookup_table

# print("=" * 60)
# print("SEARCH VALIDATION DEBUG TEST")
# print("=" * 60)

# # Test 1: Single word search
# print("\nTest 1: get_exact_term_matches('tuskegee')")
# print("-" * 40)
# matches = get_exact_term_matches('tuskegee')
# print(f"Matches found: {len(matches)}")
# for m in matches:
#     print(f"  - term: '{m.get('term')}', id: {m.get('id')}")

# # Test 2: Batch validation
# print("\nTest 2: batch_validate_words_redis")
# print("-" * 40)
# words = ['tuskegee', 'airman', 'ans', 'digs']
# print(f"Testing words: {words}")
# results = batch_validate_words_redis(words)
# for word, data in results.items():
#     print(f"  {word}: is_valid={data.get('is_valid')}, matches={len(data.get('matches', []))}")

# # Test 3: Full lookup
# print("\nTest 3: lookup_table('tuskegee airman ans digs')")
# print("-" * 40)
# result = lookup_table("tuskegee airman ans digs")
# print(f"Success: {result.get('success')}")
# print(f"Error: {result.get('error')}")
# for term in result.get('terms', []):
#     print(f"  {term.get('word')}: exists={term.get('exists')}, suggestion={term.get('suggestion')}")

# # Test 4: Test with correct words
# print("\nTest 4: lookup_table('tuskegee airmen and dogs')")
# print("-" * 40)
# result2 = lookup_table("tuskegee airmen and dogs")
# for term in result2.get('terms', []):
#     print(f"  {term.get('word')}: exists={term.get('exists')}, suggestion={term.get('suggestion')}")

# # Test 5: Test word_discovery_optimized
# print("\nTest 5: process_query_optimized('tuskegee airman ans digs')")
# print("-" * 40)
# try:
#     from searchengine.word_discovery import process_query_optimized
#     discovery = process_query_optimized("tuskegee airman ans digs", verbose=True)
#     print(f"Valid terms: {[t['search_word'] for t in discovery.get('valid_terms', [])]}")
#     print(f"Unknown terms: {[t['word'] for t in discovery.get('unknown_terms', [])]}")
#     print(f"Search strategy: {discovery.get('search_strategy')}")
# except Exception as e:
#     print(f"Error: {e}")

# print("\n" + "=" * 60)
# print("TEST COMPLETE")
# print("=" * 60)


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
#     Returns list of matching documents where term field EXACTLY equals the search term.
    
#     FIX: Increased paging to 100 to find exact matches among many partial matches.
#     Example: Searching "and" returns 445 docs containing "and", but we need 
#     the one where term="and" exactly, which might have low rank.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []
    
#     try:
#         escaped_term = escape_query(term_lower)
#         query_str = f'"{escaped_term}"'
        
#         # FIX: Increased limit from 10 to 100 to find exact matches
#         # Short words like "and" may have low rank and not appear in top 10
#         query = Query(query_str).paging(0, 100)
#         result = client.ft(INDEX_NAME).search(query)
        
#         matches = []
#         for doc in result.docs:
#             parsed = parse_search_doc(doc)
#             if parsed:
#                 # Only include EXACT matches (term field equals our search term)
#                 doc_term = parsed.get('term', '').lower().strip()
#                 if doc_term == term_lower:
#                     matches.append(parsed)
        
#         # Sort by rank descending (highest rank first)
#         matches.sort(key=lambda x: x.get('rank', 0), reverse=True)
        
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

# import os
# import sys

# print("Starting test...")

# # Add the parent directory to path
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# print(f"Path: {sys.path[0]}")

# try:
#     os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')
#     print("Settings module set")
    
#     import django
#     django.setup()
#     print("Django setup complete")
    
# except Exception as e:
#     print(f"Django setup error: {e}")
#     sys.exit(1)

# try:
#     from searchengine.searchapi import get_exact_term_matches
#     print("Import successful")
    
#     print("\nTesting get_exact_term_matches('tuskegee')...")
#     matches = get_exact_term_matches('tuskegee')
#     print(f"Found {len(matches)} matches")
#     for m in matches:
#         print(f"  - {m.get('term')}")

#     print("\nTesting get_exact_term_matches('and')...")
#     matches = get_exact_term_matches('and')
#     print(f"Found {len(matches)} matches")
#     for m in matches:
#         print(f"  - {m.get('term')}")

#     print("\nTesting get_exact_term_matches('airmen')...")
#     matches = get_exact_term_matches('airmen')
#     print(f"Found {len(matches)} matches")
#     for m in matches:
#         print(f"  - {m.get('term')}")

# except Exception as e:
#     import traceback
#     print(f"Error: {e}")
#     traceback.print_exc()

# print("\nDone!")

# from searchapi import get_exact_term_matches, RedisLookupTable

# # Test 1: Connection
# client = RedisLookupTable.get_client()
# print(f"Redis connected: {client is not None}")

# # Test 2: Direct key check
# if client:
#     keys = client.keys("term:and:*")
#     print(f"Keys matching 'term:and:*': {keys}")
    
#     keys = client.keys("term:tuskegee:*")
#     print(f"Keys matching 'term:tuskegee:*': {keys}")

# # Test 3: Function check
# result = get_exact_term_matches('and')
# print(f"get_exact_term_matches('and'): {result}")

# result = get_exact_term_matches('tuskegee')
# print(f"get_exact_term_matches('tuskegee'): {result}")

# import sys
# import os

# # Add the searchengine directory to path
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# # Now use absolute imports
# from searchapi import batch_validate_words_redis, get_exact_term_matches, RedisLookupTable
# from searchapi import batch_validate_words_redis
# from word_discovery import batch_validate_words, word_discovery_full

# query = "tuskegee airman and dos"
# words = query.split()

# # Test 1: batch_validate_words_redis
# print("=== Test 1: batch_validate_words_redis ===")
# validation_cache = batch_validate_words_redis(words)
# for word, result in validation_cache.items():
#     print(f"  {word}: is_valid={result.get('is_valid')}, metadata={result.get('metadata', {}).get('pos')}")

# # Test 2: batch_validate_words (no pre_validated)
# print("\n=== Test 2: batch_validate_words (no pre_validated) ===")
# states = batch_validate_words(words, pre_validated=None)
# for s in states:
#     print(f"  [{s.position}] {s.word}: status={s.status}, pos={s.pos}")

# # Test 3: word_discovery_full
# print("\n=== Test 3: word_discovery_full ===")
# result = word_discovery_full(query, verbose=True)
# print(f"  Valid terms: {[t['word'] for t in result['valid_terms']]}")
# print(f"  Unknown terms: {[t['word'] for t in result['unknown_terms']]}")

# test_diagnostic.py
# test_pass1.py
# test_correction.py


###################################### Works 
# import sys
# import os
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# from searchapi import get_exact_term_matches, generate_candidates_smart, batch_check_candidates
# from word_discovery import (
#     batch_validate_words, 
#     word_discovery_full,
#     predict_pos_from_context,
#     search_with_pos_filter,
#     cached_levenshtein,
#     normalize_pos
# )

# print("=" * 60)
# print("TEST 1: Does 'quick' exist in Redis?")
# print("=" * 60)
# result = get_exact_term_matches('quick')
# if result:
#     print(f"  ✅ FOUND: {result[0]}")
# else:
#     print(f"  ❌ NOT FOUND - You need to add 'quick' to Redis first!")
#     sys.exit(1)

# print("\n" + "=" * 60)
# print("TEST 2: Can we generate 'quick' as a candidate for 'quikc'?")
# print("=" * 60)
# candidates = generate_candidates_smart('quikc', max_candidates=100)
# print(f"  Generated {len(candidates)} candidates")
# if 'quick' in candidates:
#     print(f"  ✅ 'quick' IS in candidates")
# else:
#     print(f"  ❌ 'quick' NOT in candidates")
#     print(f"  First 20 candidates: {list(candidates)[:20]}")

# print("\n" + "=" * 60)
# print("TEST 3: Can batch_check_candidates find 'quick'?")
# print("=" * 60)
# found = batch_check_candidates(candidates)
# print(f"  Found {len(found)} candidates in Redis")
# quick_found = [f for f in found if f.get('term', '').lower() == 'quick']
# if quick_found:
#     print(f"  ✅ 'quick' found: {quick_found[0]}")
# else:
#     print(f"  ❌ 'quick' not in found candidates")
#     print(f"  Found terms: {[f.get('term') for f in found[:10]]}")

# print("\n" + "=" * 60)
# print("TEST 4: POS Context Prediction")
# print("=" * 60)
# # Simulating "the quikc brown" -> left=article, right=adjective
# left_pos = 'article'
# right_pos = 'adjective'
# prediction = predict_pos_from_context(left_pos, right_pos)
# print(f"  Context: left='{left_pos}', right='{right_pos}'")
# if prediction:
#     print(f"  ✅ Predicted POS: '{prediction[0]}' (confidence: {prediction[1]})")
# else:
#     print(f"  ❌ No prediction found")

# print("\n" + "=" * 60)
# print("TEST 5: Search with POS filter for 'quikc'")
# print("=" * 60)
# # Should find 'quick' as adjective
# correction = search_with_pos_filter('quikc', 'adjective', max_distance=2)
# if correction:
#     print(f"  ✅ Found correction:")
#     print(f"     Term: '{correction.get('term')}'")
#     print(f"     POS: {correction.get('pos')}")
#     print(f"     Distance: {correction.get('distance')}")
# else:
#     print(f"  ❌ No correction found")

# print("\n" + "=" * 60)
# print("TEST 6: Full word_discovery_full test")
# print("=" * 60)
# result = word_discovery_full("the quikc brown fox", verbose=True)
# print(f"\n  Final Result:")
# print(f"    Valid terms: {[t['search_word'] for t in result['valid_terms']]}")
# print(f"    Unknown terms: {[t['word'] for t in result['unknown_terms']]}")
# print(f"    Corrected query: '{result['corrected_query']}'")
# print(f"    Strategy: {result.get('search_strategy', 'N/A')}")

# # Check if "quick" is in the corrected output
# if 'quick' in result['corrected_query']:
#     print(f"\n  ✅ SUCCESS! 'quikc' was corrected to 'quick'")
# else:
#     print(f"\n  ❌ FAILED! 'quikc' was not corrected to 'quick'")




# # test_django_flow.py
# import sys
# import os
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# from searchapi import lookup_table
# from word_discovery import word_discovery_full, batch_validate_words

# query = "tuskegee airman"

# print("=" * 60)
# print("SIMULATING DJANGO FLOW")
# print("=" * 60)

# # Step 1: What lookup_table returns (this is what Django uses)
# print("\n=== Step 1: lookup_table() result ===")
# lookup_result = lookup_table(query, return_validation_cache=True)
# print(f"Success: {lookup_result.get('success')}")
# print(f"Terms returned: {len(lookup_result.get('terms', []))}")

# for term in lookup_result.get('terms', []):
#     print(f"  Word: '{term.get('word')}'")
#     print(f"    exists: {term.get('exists')}")
#     print(f"    pos: {term.get('pos')}")
#     print(f"    metadata: {term.get('metadata', {}).get('pos')}")
#     print()

# # Step 2: What word_discovery_full does with that data
# print("\n=== Step 2: word_discovery_full() with pre_validated ===")
# result_with_prevalidated = word_discovery_full(
#     query, 
#     verbose=True, 
#     pre_validated=lookup_result.get('terms', [])
# )
# print(f"Valid terms: {[t['word'] for t in result_with_prevalidated['valid_terms']]}")
# print(f"Unknown terms: {[t['word'] for t in result_with_prevalidated['unknown_terms']]}")

# # Step 3: Compare to direct call (what test script does)
# print("\n=== Step 3: word_discovery_full() WITHOUT pre_validated ===")
# result_direct = word_discovery_full(query, verbose=True, pre_validated=None)
# print(f"Valid terms: {[t['word'] for t in result_direct['valid_terms']]}")
# print(f"Unknown terms: {[t['word'] for t in result_direct['unknown_terms']]}")

# # Summary
# print("\n" + "=" * 60)
# print("COMPARISON")
# print("=" * 60)
# print(f"With pre_validated (Django path):    Valid={len(result_with_prevalidated['valid_terms'])}, Unknown={len(result_with_prevalidated['unknown_terms'])}")
# print(f"Without pre_validated (Test path):   Valid={len(result_direct['valid_terms'])}, Unknown={len(result_direct['unknown_terms'])}")






# This worls very good!

# """
# test_word_discovery_intent.py
# Test that intent detection works inside word_discovery.py

# Run from your Django project directory:
#     python -c "from search.word_discovery import process_query_optimized; r = process_query_optimized('who was the first president'); print('SIGNALS:', r.get('signals', 'NOT FOUND'))"

# Or run this file directly if word_discovery is importable:
#     python test_word_discovery_intent.py
# """

# import sys
# import os

# print("=" * 70)
# print("TEST: word_discovery.py with intent_detect integration")
# print("=" * 70)

# # =============================================================================
# # Step 1: Try to import word_discovery
# # =============================================================================
# print("\n[Step 1] Attempting to import word_discovery...")

# word_discovery = None
# process_query_optimized = None

# # Try different import paths
# import_attempts = [
#     ("from word_discovery import process_query_optimized", "word_discovery"),
#     ("from .word_discovery import process_query_optimized", ".word_discovery"),
#     ("from search.word_discovery import process_query_optimized", "search.word_discovery"),
#     ("from app.search.word_discovery import process_query_optimized", "app.search.word_discovery"),
# ]

# for import_code, module_name in import_attempts:
#     try:
#         print(f"  Trying: {import_code}")
#         exec(import_code)
#         print(f"  ✅ Success: imported from {module_name}")
#         break
#     except ImportError as e:
#         print(f"  ❌ Failed: {e}")
#     except Exception as e:
#         print(f"  ❌ Error: {type(e).__name__}: {e}")
# else:
#     print("\n❌ Could not import word_discovery from any path.")
#     print("\nPlease run this test from your Django project directory,")
#     print("or modify the import statement at the top of this file.")
#     print("\nAlternatively, run this in Django shell:")
#     print("  python manage.py shell")
#     print("  >>> from search.word_discovery import process_query_optimized")
#     print("  >>> result = process_query_optimized('who was the first president')")
#     print("  >>> print('SIGNALS:', result.get('signals', 'NOT FOUND'))")
#     sys.exit(1)

# # =============================================================================
# # Step 2: Test basic query processing
# # =============================================================================
# print("\n[Step 2] Testing process_query_optimized()...")

# test_query = "who was the first president of morehouse"
# print(f"  Query: '{test_query}'")

# try:
#     result = process_query_optimized(test_query)
#     print(f"  ✅ process_query_optimized() returned successfully")
#     print(f"  Keys in result: {list(result.keys())}")
# except Exception as e:
#     print(f"  ❌ Error calling process_query_optimized(): {type(e).__name__}: {e}")
#     import traceback
#     traceback.print_exc()
#     sys.exit(1)

# # =============================================================================
# # Step 3: Check if 'signals' key exists
# # =============================================================================
# print("\n[Step 3] Checking for 'signals' key...")

# if 'signals' in result:
#     print("  ✅ 'signals' key EXISTS in result")
#     signals = result['signals']
#     print(f"  Number of signals: {len(signals)}")
# else:
#     print("  ❌ 'signals' key NOT FOUND in result")
#     print("\n  This means detect_intent() is not being called in word_discovery.py")
#     print("  or it's failing silently.")
#     print("\n  Check that word_discovery.py has:")
#     print("    1. Import at top: from .intent_detect import detect_intent")
#     print("    2. Call at end of process_query_optimized(): output = detect_intent(output)")
#     sys.exit(1)

# # =============================================================================
# # Step 4: Display signals
# # =============================================================================
# print("\n[Step 4] Displaying detected signals...")

# signals = result['signals']

# print("\n  --- Question Signals ---")
# print(f"    has_question_word: {signals.get('has_question_word')}")
# print(f"    question_word: {signals.get('question_word')}")
# print(f"    question_position: {signals.get('question_position')}")

# print("\n  --- Temporal Signals ---")
# print(f"    has_temporal: {signals.get('has_temporal')}")
# print(f"    temporal_direction: {signals.get('temporal_direction')}")
# print(f"    temporal_word: {signals.get('temporal_word')}")

# print("\n  --- Role Signals ---")
# print(f"    has_role_word: {signals.get('has_role_word')}")
# print(f"    role_word: {signals.get('role_word')}")

# print("\n  --- Entity Signals ---")
# print(f"    has_person: {signals.get('has_person')}")
# print(f"    has_organization: {signals.get('has_organization')}")
# print(f"    detected_entities: {signals.get('detected_entities')}")

# print("\n  --- Structure Signals ---")
# print(f"    word_count: {signals.get('word_count')}")
# print(f"    has_verb: {signals.get('has_verb')}")

# # =============================================================================
# # Step 5: Test multiple queries
# # =============================================================================
# print("\n[Step 5] Testing multiple query types...")

# test_queries = [
#     "restaurants near me",
#     "black women leadership", 
#     "billie holiday",
#     "where is africa located",
#     "best soul food in atlanta",
# ]

# for query in test_queries:
#     print(f"\n  Query: '{query}'")
#     try:
#         r = process_query_optimized(query)
#         s = r.get('signals', {})
        
#         # Show key signals
#         key_signals = []
#         if s.get('has_question_word'):
#             key_signals.append(f"question:{s.get('question_word')}")
#         if s.get('has_temporal'):
#             key_signals.append(f"temporal:{s.get('temporal_direction')}")
#         if s.get('is_local_search'):
#             key_signals.append("local_search")
#         if s.get('has_superlative'):
#             key_signals.append(f"superlative:{s.get('superlative_word')}")
#         if s.get('has_service_word'):
#             key_signals.append(f"service:{s.get('service_words')}")
#         if s.get('has_culture_word'):
#             key_signals.append("culture")
#         if s.get('has_food_word'):
#             key_signals.append("food")
#         if s.get('has_person'):
#             key_signals.append("person")
#         if s.get('has_location'):
#             key_signals.append("location")
        
#         if key_signals:
#             print(f"    Signals: {', '.join(key_signals)}")
#         else:
#             print(f"    Signals: (none detected)")
            
#     except Exception as e:
#         print(f"    ❌ Error: {e}")

# # =============================================================================
# # Summary
# # =============================================================================
# print("\n" + "=" * 70)
# print("TEST COMPLETE")
# print("=" * 70)
# print("✅ word_discovery.py successfully integrates intent_detect")
# print("✅ Signals are being added to the output")
# print("\nThe integration is working correctly.")
"""
test_search.py
Test autocomplete, search, and cache lookups.

Usage (from searchengine directory):
    python test_search.py busy
    python test_search.py "busy bee"
    python test_search.py --term busy_bee_cafe
    python test_search.py --all
"""

import sys
import os
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

import django
django.setup()

print("=" * 70)
print("TEST: Search & Autocomplete Debug")
print("=" * 70)


# =============================================================================
# Parse arguments
# =============================================================================

query = ""
term = ""
run_all = False

args = sys.argv[1:]
if "--all" in args:
    run_all = True
elif "--term" in args:
    idx = args.index("--term")
    if idx + 1 < len(args):
        term = args[idx + 1]
elif args:
    query = " ".join([a for a in args if not a.startswith("--")])

if not query and not term and not run_all:
    print("\nUsage:")
    print("  python test_search.py busy")
    print('  python test_search.py "busy bee cafe"')
    print("  python test_search.py --term busy_bee_cafe")
    print("  python test_search.py --all")
    sys.exit(0)


# =============================================================================
# Test a query through all layers
# =============================================================================

def test_query(query, limit=10):
    print(f"\n{'='*70}")
    print(f"TESTING QUERY: \"{query}\"")
    print(f"{'='*70}")

    # --- Layer 1: RAM Cache ---
    print(f"\n--- LAYER 1: RAM CACHE ---")
    try:
        from searchengine.vocabulary_cache import vocab_cache
        if vocab_cache.loaded:
            print(f"  ✅ Cache loaded: {vocab_cache.term_count:,} terms")

            has = vocab_cache.has_term(query)
            print(f"  has_term('{query}'): {has}")

            meta = vocab_cache.get_term(query)
            if meta:
                print(f"  get_term: {meta.get('display')} | {meta.get('category')}")
            else:
                print(f"  get_term: NOT FOUND")

            classified = vocab_cache.classify_query(query)
            print(f"  classify_query:")
            print(f"    locations:  {classified.get('locations', [])}")
            print(f"    bigrams:    {classified.get('bigrams', [])}")
            print(f"    trigrams:   {classified.get('trigrams', [])}")
            print(f"    quadgrams:  {classified.get('quadgrams', [])}")
            print(f"    terms:      {list(classified.get('terms', {}).keys())}")
            print(f"    unknown:    {classified.get('unknown', [])}")
            print(f"    stopwords:  {classified.get('stopwords', [])}")
        else:
            print(f"  ❌ Cache NOT loaded")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 2: Redis Exact Match ---
    print(f"\n--- LAYER 2: REDIS EXACT MATCH ---")
    try:
        from searchengine.searchapi import get_exact_term_matches
        matches = get_exact_term_matches(query)
        print(f"  Found {len(matches)} exact matches")
        for m in matches[:5]:
            print(f"    {m.get('id')} | {m.get('term')} | {m.get('category')} | rank={m.get('rank')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 3: Redis Prefix Match ---
    print(f"\n--- LAYER 3: REDIS PREFIX MATCH ---")
    try:
        from searchengine.searchapi import get_prefix_matches
        matches = get_prefix_matches(query, limit=limit)
        print(f"  Found {len(matches)} prefix matches")
        for m in matches[:10]:
            print(f"    {m.get('term')} | {m.get('category')} | rank={m.get('rank')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 4: Redis Fuzzy Match ---
    print(f"\n--- LAYER 4: REDIS FUZZY MATCH ---")
    try:
        from searchengine.searchapi import get_fuzzy_matches
        matches = get_fuzzy_matches(query, limit=limit)
        print(f"  Found {len(matches)} fuzzy matches")
        for m in matches[:10]:
            print(f"    {m.get('term')} | {m.get('category')} | dist={m.get('distance')} | rank={m.get('rank')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 5: get_suggestions (combined) ---
    print(f"\n--- LAYER 5: get_suggestions (COMBINED) ---")
    try:
        from searchengine.searchapi import get_suggestions
        result = get_suggestions(query, limit=limit)
        print(f"  Tier used:    {result.get('tier_used')}")
        print(f"  Exact match:  {result.get('exact_match')}")
        print(f"  Suggestions:  {len(result.get('suggestions', []))}")
        if result.get('error'):
            print(f"  ❌ ERROR: {result['error']}")
        for s in result.get('suggestions', [])[:10]:
            print(f"    {s.get('term')} | {s.get('display')} | {s.get('category')} | rank={s.get('rank')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 6: get_autocomplete (what the dropdown calls) ---
    print(f"\n--- LAYER 6: get_autocomplete (DROPDOWN) ---")
    try:
        from searchengine.searchapi import get_autocomplete
        results = get_autocomplete(prefix=query, limit=8)
        print(f"  Results: {len(results)}")
        for r in results:
            print(f"    {r.get('term')} | {r.get('display')} | {r.get('category')} | rank={r.get('rank')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # --- Layer 7: API endpoint (what the frontend calls) ---
    print(f"\n--- LAYER 7: API ENDPOINT (search_suggestions view) ---")
    try:
        from django.test import RequestFactory
        from searchengine.views import search_suggestions
        factory = RequestFactory()
        request = factory.get(f'/api/search/?q={query}')
        response = search_suggestions(request)
        data = json.loads(response.content)
        suggestions = data.get('suggestions', [])
        print(f"  Suggestions: {len(suggestions)}")
        for s in suggestions:
            print(f"    text={s.get('text')} | display={s.get('display')} | desc={s.get('description', '')[:50]}")
        if data.get('error'):
            print(f"  ❌ ERROR: {data['error']}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    print(f"\n{'='*70}")


# =============================================================================
# Test a specific term
# =============================================================================

def test_term(term):
    print(f"\n{'='*70}")
    print(f"TESTING TERM: \"{term}\"")
    print(f"{'='*70}")

    # RAM Cache
    print(f"\n--- RAM CACHE ---")
    try:
        from searchengine.vocabulary_cache import vocab_cache
        meta = vocab_cache.get_term(term)
        if meta:
            print(f"  ✅ FOUND: {json.dumps(meta, indent=4, default=str)}")
        else:
            print(f"  ❌ NOT FOUND in RAM cache")

            alt = term.replace('_', ' ')
            meta2 = vocab_cache.get_term(alt)
            if meta2:
                print(f"  💡 BUT FOUND as '{alt}': {meta2.get('display')} | {meta2.get('category')}")

            alt2 = term.replace(' ', '_')
            meta3 = vocab_cache.get_term(alt2)
            if meta3:
                print(f"  💡 BUT FOUND as '{alt2}': {meta3.get('display')} | {meta3.get('category')}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # Redis Direct
    print(f"\n--- REDIS DIRECT ---")
    try:
        from searchengine.searchapi import RedisLookupTable
        client = RedisLookupTable.get_client()
        if client:
            found_any = False
            for suffix in ['business_location', 'us_city', 'us_state', 'dictionary_word', 'keyword', 'food', 'music', 'education']:
                key = f"term:{term}:{suffix}"
                data = client.hgetall(key)
                if data:
                    found_any = True
                    print(f"  ✅ FOUND: {key}")
                    for k, v in list(data.items())[:8]:
                        print(f"    {k}: {v}")
                    break

            if not found_any:
                keys = client.keys(f"term:{term}:*")
                if keys:
                    print(f"  ✅ Found keys: {keys[:5]}")
                else:
                    print(f"  ❌ NOT FOUND in Redis")

                    alt = term.replace('_', ' ')
                    alt_keys = client.keys(f"term:{alt}:*")
                    if alt_keys:
                        print(f"  💡 BUT FOUND with spaces: {alt_keys[:5]}")

                    alt2 = term.replace(' ', '_')
                    alt2_keys = client.keys(f"term:{alt2}:*")
                    if alt2_keys:
                        print(f"  💡 BUT FOUND with underscores: {alt2_keys[:5]}")
        else:
            print(f"  ❌ Redis not connected")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    # RediSearch
    print(f"\n--- REDISEARCH ---")
    try:
        from searchengine.searchapi import get_exact_term_matches
        matches = get_exact_term_matches(term)
        if matches:
            print(f"  ✅ FOUND {len(matches)} matches:")
            for m in matches:
                print(f"    {m.get('id')} | {m.get('term')} | {m.get('category')} | rank={m.get('rank')}")
        else:
            print(f"  ❌ NOT FOUND via RediSearch")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


# =============================================================================
# Run all tests
# =============================================================================

def test_all():
    print(f"\n{'='*70}")
    print(f"RUNNING ALL TESTS")
    print(f"{'='*70}")

    passed = 0
    failed = 0

    # Test 1: Cache status
    print(f"\n--- TEST 1: Cache Status ---")
    try:
        from searchengine.vocabulary_cache import vocab_cache
        if vocab_cache.loaded and vocab_cache.term_count > 0:
            print(f"  ✅ PASS: {vocab_cache.term_count:,} terms loaded")
            print(f"     unigrams={len(vocab_cache.unigrams):,}, bigrams={len(vocab_cache.bigrams):,}, trigrams={len(vocab_cache.trigrams):,}, quadgrams={len(vocab_cache.quadgrams):,}")
            passed += 1
        else:
            print(f"  ❌ FAIL: Cache not loaded")
            failed += 1
    except Exception as e:
        print(f"  ❌ FAIL: {e}")
        failed += 1

    # Test 2: Redis connection
    print(f"\n--- TEST 2: Redis Connection ---")
    try:
        from searchengine.searchapi import RedisLookupTable
        client = RedisLookupTable.get_client()
        if client and client.ping():
            print(f"  ✅ PASS: Redis connected")
            passed += 1
        else:
            print(f"  ❌ FAIL: Redis not connected")
            failed += 1
    except Exception as e:
        print(f"  ❌ FAIL: {e}")
        failed += 1

    # Test 3: Known terms
    print(f"\n--- TEST 3: Known Term Lookups ---")
    test_terms = ["atlanta", "georgia", "hbcus", "jazz"]
    for t in test_terms:
        try:
            from searchengine.searchapi import get_exact_term_matches
            matches = get_exact_term_matches(t)
            if matches:
                print(f"  ✅ PASS: '{t}' found ({len(matches)} matches)")
                passed += 1
            else:
                print(f"  ❌ FAIL: '{t}' not found")
                failed += 1
        except Exception as e:
            print(f"  ❌ FAIL: '{t}' error: {e}")
            failed += 1

    # Test 4: Business terms
    print(f"\n--- TEST 4: Business Term Lookups ---")
    business_terms = ["busy_bee_cafe", "slutty_vegan_edgewood", "portrait_coffee"]
    for t in business_terms:
        try:
            from searchengine.searchapi import get_exact_term_matches, RedisLookupTable
            matches = get_exact_term_matches(t)
            if matches:
                print(f"  ✅ PASS: '{t}' found ({matches[0].get('category')})")
                passed += 1
            else:
                print(f"  ❌ FAIL: '{t}' not found via exact match")
                client = RedisLookupTable.get_client()
                if client:
                    keys = client.keys(f"term:{t}:*")
                    if keys:
                        print(f"         💡 But exists in Redis: {keys}")
                    else:
                        print(f"         ❌ Not in Redis either")
                failed += 1
        except Exception as e:
            print(f"  ❌ FAIL: '{t}' error: {e}")
            failed += 1

    # Test 5: Autocomplete
    print(f"\n--- TEST 5: Autocomplete ---")
    autocomplete_tests = ["atl", "busy", "jazz", "portrait"]
    for q in autocomplete_tests:
        try:
            from searchengine.searchapi import get_autocomplete
            results = get_autocomplete(prefix=q, limit=5)
            if results:
                terms = [r.get('term', '') for r in results[:3]]
                print(f"  ✅ PASS: '{q}' -> {len(results)} results: {terms}")
                passed += 1
            else:
                print(f"  ❌ FAIL: '{q}' -> 0 results")
                failed += 1
        except Exception as e:
            print(f"  ❌ FAIL: '{q}' error: {e}")
            failed += 1

    # Test 6: API endpoint
    print(f"\n--- TEST 6: API Endpoint ---")
    try:
        from django.test import RequestFactory
        from searchengine.views import search_suggestions
        factory = RequestFactory()

        for q in ["atlanta", "busy", "jazz"]:
            request = factory.get(f'/api/search/?q={q}')
            response = search_suggestions(request)
            data = json.loads(response.content)
            suggestions = data.get('suggestions', [])
            if suggestions:
                texts = [s.get('text', '') for s in suggestions[:3]]
                print(f"  ✅ PASS: /api/search/?q={q} -> {len(suggestions)} suggestions: {texts}")
                passed += 1
            else:
                print(f"  ❌ FAIL: /api/search/?q={q} -> 0 suggestions")
                if data.get('error'):
                    print(f"         Error: {data['error']}")
                failed += 1
    except Exception as e:
        print(f"  ❌ FAIL: API endpoint error: {e}")
        failed += 1

    # Summary
    total = passed + failed
    print(f"\n{'='*70}")
    if failed == 0:
        print(f"✅ ALL TESTS PASSED: {passed}/{total}")
    else:
        print(f"❌ {failed} TESTS FAILED: {passed}/{total} passed")
    print(f"{'='*70}")


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    if run_all:
        test_all()
    elif term:
        test_term(term)
    elif query:
        test_query(query)