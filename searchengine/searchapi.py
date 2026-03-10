


# """
# lookup_table.py - Redis-based search preprocessing for Django

# This module provides Redis hash and sorted set lookups for:
# - Query term validation
# - Spelling correction
# - Autocomplete suggestions
# - Query caching
# """

# import json
# from pyxdameraulevenshtein import damerau_levenshtein_distance
# import redis
# from django.conf import settings
# from typing import Optional, Dict, List, Any
# from decouple import config
# import redis
# from redis.commands.search.query import Query
# from redis.commands.search.field import TextField, NumericField, TagField
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple
# from decouple import config



# REDIS_LOCATION=config('REDIS_LOCATION')
# REDIS_DATABASE=config('REDIS_DATABASE')
# REDIS_PORT=config('REDIS_PORT')
# REDIS_PASSWORD=config('REDIS_PASSWORD')
# REDIS_USERNAME=config('REDIS_USERNAME')
# REDIS_DB=config('REDIS_DB')



# import redis
# from redis.commands.search.query import Query
# import json
# import string
# from typing import Optional, Dict, Any, List, Set, Tuple
# from decouple import config


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION')
# REDIS_PORT = config('REDIS_PORT', cast=int)
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
#     Find exact matches for a term.
    
#     Uses DIRECT KEY LOOKUP instead of RediSearch full-text search.
#     This is more reliable for exact matching because:
#     - RediSearch "and" returns 445 docs containing "and" anywhere
#     - Direct lookup for "term:and:*" finds only exact matches
    
#     Key pattern: term:{word}:{category}
#     Examples: term:and:dictionary_word, term:tuskegee:us_city
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []
    
#     # Handle multi-word terms (replace spaces with underscores in key)
#     term_key = term_lower.replace(' ', '_')
    
#     try:
#         # DIRECT KEY LOOKUP: Find all keys matching term:{word}:*
#         pattern = f"term:{term_key}:*"
#         keys = client.keys(pattern)
        
#         if not keys:
#             return []
        
#         matches = []
#         for key in keys:
#             metadata = client.hgetall(key)
#             if metadata:
#                 # Parse the metadata
#                 try:
#                     rank_val = int(float(metadata.get('rank', 0)))
#                 except (ValueError, TypeError):
#                     rank_val = 0
                
#                 parsed = {
#                     'id': key,
#                     'member': key,
#                     'term': metadata.get('term', term_lower),
#                     'display': metadata.get('display', term_lower),
#                     'description': metadata.get('description', ''),
#                     'category': metadata.get('category', ''),
#                     'entity_type': metadata.get('entity_type', ''),
#                     'pos': metadata.get('pos', ''),
#                     'rank': rank_val,
#                     'exists': True,
#                 }
#                 matches.append(parsed)
        
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
#         # After
#         # query_str = f"@term:{escaped_prefix}*"
        
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

# # def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
# #     """
# #     Get fuzzy matches using RediSearch Levenshtein distance.
# #     %term% = 1 edit distance
# #     %%term%% = 2 edit distance
# #     """
# #     client = RedisLookupTable.get_client()
# #     if not client or not term:
# #         return []
    
# #     term_lower = term.lower().strip()
# #     if len(term_lower) < 3:
# #         return []
    
# #     try:
# #         escaped_term = escape_query(term_lower)
        
# #         # Try 1 edit distance first
# #         if max_distance >= 1:
# #             # FIX: Simplified query without @term:
# #             query_str = f"%{escaped_term}%"
# #             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
# #             result = client.ft(INDEX_NAME).search(query)
            
# #             if result.docs:
# #                 matches = []
# #                 for doc in result.docs:
# #                     parsed = parse_search_doc(doc)
# #                     if parsed:
# #                         parsed['distance'] = damerau_levenshtein_distance(
# #                             term_lower, parsed['term'].lower()
# #                         )
# #                         matches.append(parsed)
                
# #                 matches.sort(key=lambda x: (x['distance'], -x['rank']))
# #                 return matches[:limit]
        
# #         # Try 2 edit distance if no results
# #         if max_distance >= 2:
# #             # FIX: Simplified query without @term:
# #             query_str = f"%%{escaped_term}%%"
# #             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
# #             result = client.ft(INDEX_NAME).search(query)
            
# #             matches = []
# #             for doc in result.docs:
# #                 parsed = parse_search_doc(doc)
# #                 if parsed:
# #                     parsed['distance'] = damerau_levenshtein_distance(
# #                         term_lower, parsed['term'].lower()
# #                     )
# #                     matches.append(parsed)
            
# #             matches.sort(key=lambda x: (x['distance'], -x['rank']))
# #             return matches[:limit]
        
# #         return []
        
# #     except Exception as e:
# #         print(f"Fuzzy match error: {e}")
# #         return []

# def get_fuzzy_matches(term: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get fuzzy matches using RediSearch Levenshtein distance.
#     %term% = 1 edit distance
#     %%term%% = 2 edit distance
    
#     For multi-word queries, only fuzzy matches the last word.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []
    
#     term_lower = term.lower().strip()
#     if len(term_lower) < 3:
#         return []
    
#     try:
#         # Split into words
#         words = term_lower.split()
        
#         if len(words) > 1:
#             # Multi-word: fuzzy match only the last word
#             prefix_words = ' '.join(words[:-1])
#             last_word = words[-1]
            
#             if len(last_word) < 3:
#                 return []
            
#             escaped_prefix = escape_query(prefix_words)
#             escaped_last = escape_query(last_word)
            
#             # Try 1 edit distance first
#             if max_distance >= 1:
#                 query_str = f"{escaped_prefix} %{escaped_last}%"
#                 query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
                
#                 try:
#                     result = client.ft(INDEX_NAME).search(query)
                    
#                     if result.docs:
#                         matches = []
#                         for doc in result.docs:
#                             parsed = parse_search_doc(doc)
#                             if parsed:
#                                 parsed['distance'] = damerau_levenshtein_distance(
#                                     term_lower, parsed['term'].lower()
#                                 )
#                                 matches.append(parsed)
                        
#                         matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                         return matches[:limit]
#                 except Exception as e:
#                     print(f"Fuzzy match error (multi-word, 1 edit): {e}")
            
#             # Try 2 edit distance
#             if max_distance >= 2:
#                 query_str = f"{escaped_prefix} %%{escaped_last}%%"
#                 query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
                
#                 try:
#                     result = client.ft(INDEX_NAME).search(query)
                    
#                     matches = []
#                     for doc in result.docs:
#                         parsed = parse_search_doc(doc)
#                         if parsed:
#                             parsed['distance'] = damerau_levenshtein_distance(
#                                 term_lower, parsed['term'].lower()
#                             )
#                             matches.append(parsed)
                    
#                     matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                     return matches[:limit]
#                 except Exception as e:
#                     print(f"Fuzzy match error (multi-word, 2 edit): {e}")
            
#             return []
        
#         # Single word - original logic
#         escaped_term = escape_query(term_lower)
        
#         # Try 1 edit distance first
#         if max_distance >= 1:
#             query_str = f"%{escaped_term}%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             try:
#                 result = client.ft(INDEX_NAME).search(query)
                
#                 if result.docs:
#                     matches = []
#                     for doc in result.docs:
#                         parsed = parse_search_doc(doc)
#                         if parsed:
#                             parsed['distance'] = damerau_levenshtein_distance(
#                                 term_lower, parsed['term'].lower()
#                             )
#                             matches.append(parsed)
                    
#                     matches.sort(key=lambda x: (x['distance'], -x['rank']))
#                     return matches[:limit]
#             except Exception as e:
#                 print(f"Fuzzy match error (single word, 1 edit): {e}")
        
#         # Try 2 edit distance if no results
#         if max_distance >= 2:
#             query_str = f"%%{escaped_term}%%"
#             query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
#             try:
#                 result = client.ft(INDEX_NAME).search(query)
                
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
#             except Exception as e:
#                 print(f"Fuzzy match error (single word, 2 edit): {e}")
        
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

# # def get_suggestions(
# #     input_text: str,
# #     limit: int = 10,
# #     max_distance: int = 2,
# #     category: Optional[str] = None
# # ) -> Dict[str, Any]:
# #     """
# #     Unified suggestion function using RediSearch.
# #     Tries: Exact -> Prefix -> Fuzzy
# #     """
# #     client = RedisLookupTable.get_client()
    
# #     response = {
# #         'success': True,
# #         'input': input_text,
# #         'suggestions': [],
# #         'exact_match': False,
# #         'tier_used': None,
# #         'error': None
# #     }
    
# #     if not client:
# #         response['success'] = False
# #         response['error'] = 'Redis connection failed'
# #         return response
    
# #     if not input_text or not input_text.strip():
# #         response['success'] = False
# #         response['error'] = 'Empty input'
# #         return response
    
# #     input_lower = input_text.lower().strip()
    
# #     try:
# #         # === TIER 1: EXACT MATCH ===
# #         exact_matches = get_exact_term_matches(input_lower)
        
# #         if exact_matches:
# #             response['exact_match'] = True
# #             response['tier_used'] = 'exact'
            
# #             for match in exact_matches[:limit]:
# #                 match['distance'] = 0
# #                 match['score'] = -match.get('rank', 0)
            
# #             response['suggestions'] = exact_matches[:limit]
# #             return response
        
# #         # === TIER 2: PREFIX MATCH ===
# #         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
# #         if prefix_results:
# #             response['tier_used'] = 'prefix'
            
# #             for item in prefix_results:
# #                 distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
# #                 item['distance'] = distance
# #                 item['score'] = calculate_score(distance, item.get('rank', 0))
            
# #             prefix_results.sort(key=lambda x: x['score'])
# #             response['suggestions'] = prefix_results[:limit]
# #             return response
        
# #         # === TIER 3: FUZZY MATCH ===
# #         fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
        
# #         if fuzzy_results:
# #             response['tier_used'] = 'fuzzy'
            
# #             filtered = []
# #             for item in fuzzy_results:
# #                 if item.get('distance', 99) <= max_distance:
# #                     item['score'] = calculate_score(item['distance'], item.get('rank', 0))
# #                     filtered.append(item)
            
# #             filtered.sort(key=lambda x: x['score'])
# #             response['suggestions'] = filtered[:limit]
# #             return response
        
# #         # === TIER 4: SHORTER PREFIX (fallback) ===
# #         for prefix_len in [3, 2]:
# #             if len(input_lower) >= prefix_len:
# #                 short_prefix = input_lower[:prefix_len]
# #                 short_results = get_prefix_matches(short_prefix, limit=50)
                
# #                 if short_results:
# #                     response['tier_used'] = f'prefix_short_{prefix_len}'
                    
# #                     filtered = []
# #                     for item in short_results:
# #                         distance = damerau_levenshtein_distance(input_lower, item['term'].lower())
# #                         if distance <= max_distance:
# #                             item['distance'] = distance
# #                             item['score'] = calculate_score(distance, item.get('rank', 0))
# #                             filtered.append(item)
                    
# #                     if filtered:
# #                         filtered.sort(key=lambda x: x['score'])
# #                         response['suggestions'] = filtered[:limit]
# #                         return response
        
# #         # === NO RESULTS ===
# #         response['tier_used'] = 'none'
# #         response['suggestions'] = []
# #         return response
        
# #     except Exception as e:
# #         response['success'] = False
# #         response['error'] = str(e)
# #         return response
# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.
#     Returns COMBINED results: Exact + Prefix + Fuzzy
#     Sorted by rank (highest first).
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
#         all_results = []
#         seen_terms = set()
#         tiers_used = []
        
#         # === TIER 1: EXACT MATCH ===
#         exact_matches = get_exact_term_matches(input_lower)
        
#         if exact_matches:
#             response['exact_match'] = True
#             tiers_used.append('exact')
            
#             for match in exact_matches:
#                 term_lower = match.get('term', '').lower()
#                 if term_lower not in seen_terms:
#                     match['distance'] = 0
#                     match['score'] = -match.get('rank', 0)  # Negative so higher rank = lower score = better
#                     all_results.append(match)
#                     seen_terms.add(term_lower)
        
#         # === TIER 2: PREFIX MATCH (always run, don't stop at exact) ===
#         prefix_results = get_prefix_matches(input_lower, limit=limit * 3)
        
#         if prefix_results:
#             tiers_used.append('prefix')
            
#             for item in prefix_results:
#                 term_lower = item.get('term', '').lower()
#                 if term_lower not in seen_terms:
#                     distance = damerau_levenshtein_distance(input_lower, term_lower)
#                     item['distance'] = distance
#                     item['score'] = calculate_score(distance, item.get('rank', 0))
#                     all_results.append(item)
#                     seen_terms.add(term_lower)
        
#         # === TIER 3: FUZZY MATCH (only if we have few results) ===
#         if len(all_results) < limit:
#             fuzzy_results = get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance)
            
#             if fuzzy_results:
#                 tiers_used.append('fuzzy')
                
#                 for item in fuzzy_results:
#                     term_lower = item.get('term', '').lower()
#                     if term_lower not in seen_terms:
#                         if item.get('distance', 99) <= max_distance:
#                             item['score'] = calculate_score(item['distance'], item.get('rank', 0))
#                             all_results.append(item)
#                             seen_terms.add(term_lower)
        
#         # === SORT ALL RESULTS ===
#         # Sort by: rank (highest first), then distance (lowest first)
#         all_results.sort(key=lambda x: (-x.get('rank', 0), x.get('distance', 99)))
        
#         response['suggestions'] = all_results[:limit]
#         response['tier_used'] = '+'.join(tiers_used) if tiers_used else 'none'
        
#         return response
        
#     except Exception as e:
#         response['success'] = False
#         response['error'] = str(e)
#         return response
# # ```

# # ## What Changed

# # | Before | After |
# # |--------|-------|
# # | Find exact → STOP | Find exact → CONTINUE |
# # | Returns only 1 result type | Combines exact + prefix + fuzzy |
# # | Sort by score (distance - rank) | Sort by rank first, then distance |
# # | Stops at first tier with results | Runs all tiers, deduplicates |

# # ## After Replacing

# # 1. Restart Django
# # 2. Type "africa" 
# # 3. Should now see: Africa, African, African American, etc. (sorted by rank)

# # **Note:** You still need to add "african" to Redis for it to appear:
# # ```
# # HSET term:african:dictionary_word term "african" display "African" category "Dictionary Word" description "" pos "['adjective']" entity_type "unigram" rank "1000000"


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



"""
lookup_table.py - Redis-based search preprocessing for Django

This module provides Redis hash and sorted set lookups for:
- Query term validation
- Spelling correction
- Autocomplete suggestions (terms + questions)
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


import redis
from redis.commands.search.query import Query
import json
import string
from typing import Optional, Dict, Any, List, Set, Tuple
from decouple import config


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_LOCATION = config('REDIS_LOCATION')
REDIS_PORT = config('REDIS_PORT', cast=int)
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
        
        client.execute_command(
            'FT.CREATE', INDEX_NAME,
            'ON', 'HASH',
            'PREFIX', '1', 'term:',
            'STOPWORDS', '0',
            'SCHEMA',
            'term',        'TEXT',    'WEIGHT', '5.0',
            'display',     'TEXT',    'WEIGHT', '3.0',
            'category',    'TAG',     'SORTABLE',
            'description', 'TEXT',    'WEIGHT', '1.0',
            'pos',         'TAG',
            'entity_type', 'TAG',
            'rank',        'NUMERIC', 'SORTABLE'
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
        
        matches.sort(key=lambda x: x.get('rank', 0), reverse=True)
        return matches
        
    except Exception as e:
        print(f"Exact match error: {e}")
        return []


# =============================================================================
# v5: QUESTION LOOKUP — DIRECT KEY SCAN
# =============================================================================

def get_question_matches(prefix: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Find question hashes whose term starts with the given prefix.

    Uses direct key scan on question:* rather than RediSearch so this
    works even before the index is rebuilt with the question: prefix.

    Key pattern : question:{slugified-question}
    Returns     : list of dicts with entity_type='question' and
                  document_uuid / semantic_uuid / cluster_uuid populated.

    NOTE: This is intentionally kept on the keyword path only.
          No fuzzy / semantic matching — questions are pre-generated
          and an exact prefix scan is sufficient.
    """
    client = RedisLookupTable.get_client()
    if not client or not prefix:
        return []

    prefix_lower = prefix.lower().strip()
    if len(prefix_lower) < 2:
        return []

    # Build a slug-style prefix the same way insert_questions_to_redis() does
    import re
    slug_prefix = re.sub(r'[^a-z0-9\s-]', '', prefix_lower)
    slug_prefix = re.sub(r'\s+', '-', slug_prefix.strip())

    try:
        pattern = f"question:{slug_prefix}*"
        keys = client.keys(pattern)

        if not keys:
            return []

        matches = []
        for key in keys[:limit * 2]:   # fetch a few extra, trim after sort
            metadata = client.hgetall(key)
            if not metadata:
                continue

            try:
                rank_val = int(float(metadata.get('rank', 0)))
            except (ValueError, TypeError):
                rank_val = 0

            matches.append({
                'id': key,
                'member': key,
                'term': metadata.get('term', ''),
                'display': metadata.get('display', ''),
                'description': '',
                'category': 'question',
                'entity_type': 'question',
                'pos': 'question',
                'rank': rank_val,
                'exists': True,
                'document_uuid': metadata.get('document_uuid', ''),
                'semantic_uuid': metadata.get('semantic_uuid', ''),
                'cluster_uuid': metadata.get('cluster_uuid', ''),
            })

        matches.sort(key=lambda x: x.get('rank', 0), reverse=True)
        return matches[:limit]

    except Exception as e:
        print(f"Question match error: {e}")
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
    
    For multi-word queries, only fuzzy matches the last word.
    """
    client = RedisLookupTable.get_client()
    if not client or not term:
        return []
    
    term_lower = term.lower().strip()
    if len(term_lower) < 3:
        return []
    
    try:
        words = term_lower.split()
        
        if len(words) > 1:
            prefix_words = ' '.join(words[:-1])
            last_word = words[-1]
            
            if len(last_word) < 3:
                return []
            
            escaped_prefix = escape_query(prefix_words)
            escaped_last = escape_query(last_word)
            
            if max_distance >= 1:
                query_str = f"{escaped_prefix} %{escaped_last}%"
                query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
                
                try:
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
                except Exception as e:
                    print(f"Fuzzy match error (multi-word, 1 edit): {e}")
            
            if max_distance >= 2:
                query_str = f"{escaped_prefix} %%{escaped_last}%%"
                query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
                
                try:
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
                except Exception as e:
                    print(f"Fuzzy match error (multi-word, 2 edit): {e}")
            
            return []
        
        # Single word
        escaped_term = escape_query(term_lower)
        
        if max_distance >= 1:
            query_str = f"%{escaped_term}%"
            query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
            try:
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
            except Exception as e:
                print(f"Fuzzy match error (single word, 1 edit): {e}")
        
        if max_distance >= 2:
            query_str = f"%%{escaped_term}%%"
            query = Query(query_str).sort_by('rank', asc=False).paging(0, limit)
            
            try:
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
            except Exception as e:
                print(f"Fuzzy match error (single word, 2 edit): {e}")
        
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
    """Batch get metadata for multiple terms using pipeline."""
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
    """Validate a single word and return correction if needed."""
    word_lower = word.lower().strip()
    
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

    NOTE: Questions are NOT included here — they are added separately
    in get_autocomplete() so they stay on the keyword path only.
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
                    match['score'] = -match.get('rank', 0)
                    all_results.append(match)
                    seen_terms.add(term_lower)
        
        # === TIER 2: PREFIX MATCH (always run) ===
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
        
        # === TIER 3: FUZZY MATCH (only if few results) ===
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
        
        # Sort by rank first, then distance
        all_results.sort(key=lambda x: (-x.get('rank', 0), x.get('distance', 99)))
        
        response['suggestions'] = all_results[:limit]
        response['tier_used'] = '+'.join(tiers_used) if tiers_used else 'none'
        
        return response
        
    except Exception as e:
        response['success'] = False
        response['error'] = str(e)
        return response


# =============================================================================
# AUTOCOMPLETE — v5: questions prepended to term suggestions
# =============================================================================

def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get autocomplete suggestions.

    v5 CHANGE: Now returns questions + terms combined.
    - Questions come first (up to 3), sourced from get_question_matches()
    - Remaining slots filled with normal term suggestions
    - Frontend can split by entity_type == 'question' to render separately

    Returns list sorted as: [questions...] + [terms sorted by rank]
    """
    if not prefix or len(prefix.strip()) < 2:
        return []

    prefix_clean = prefix.strip()

    # --- Questions (keyword path, no fuzzy/semantic) ---
    question_results = get_question_matches(prefix_clean, limit=3)

    # --- Terms (existing path, unchanged) ---
    term_limit = limit - len(question_results)
    term_results = get_suggestions(prefix_clean, limit=term_limit)
    term_suggestions = term_results.get('suggestions', [])

    # Questions first, then terms
    return question_results + term_suggestions


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
        
        if return_validation_cache:
            response['_validation_cache'] = validation_cache
        
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

    for i in range(length):
        char = word_lower[i]
        if char in keyboard_proximity:
            for nearby_char in keyboard_proximity[char]:
                candidate = word_lower[:i] + nearby_char + word_lower[i+1:]
                candidates.add(candidate)

    for i in range(length - 1):
        candidate = word_lower[:i] + word_lower[i+1] + word_lower[i] + word_lower[i+2:]
        candidates.add(candidate)

    for i in range(length):
        candidate = word_lower[:i] + word_lower[i+1:]
        if candidate:
            candidates.add(candidate)

    for i in range(length):
        if word_lower[i] in vowels:
            for v in vowels:
                if v != word_lower[i]:
                    candidate = word_lower[:i] + v + word_lower[i+1:]
                    candidates.add(candidate)

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
    """Check if candidates exist in Redis. Kept for backwards compatibility."""
    if not candidates:
        return []
    
    found = []
    for candidate in list(candidates)[:50]:
        matches = get_exact_term_matches(candidate)
        if matches:
            found.append(matches[0])
    
    return found


def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
    """Check multiple bigrams. Kept for backwards compatibility."""
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















