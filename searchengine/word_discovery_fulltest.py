
# """
# word_discovery_fulltest.py
# ====================
# Optimized word discovery with POS-aware selection and n-gram detection.

# NOW USES RAM CACHE (vocabulary_cache.py) for Steps 1-4, 6-7.
# Only Step 5 (fuzzy spelling correction) hits Redis.

# STRATEGY:
#     Step 1: Collect ALL data in one pass (RAM)
#             - All word matches (don't pick winner yet)
#             - All bigrams, trigrams, quadgrams
    
#     Step 2: Mark n-gram positions (longest first)
#             - Quadgrams → Trigrams → Bigrams
#             - Consumed positions are marked
    
#     Step 3: Determine POS for remaining words
#             - Use grammar rules based on neighbors
#             - Unknown neighbors → default to noun
    
#     Step 4: Select best match (POS + Rank)
#             - Filter by predicted POS
#             - Then sort by rank
#             - Word Discovery is the brain - Typesense receives the decision
    
#     Step 5: Handle unknowns (spelling correction)
#             - Use predicted POS to filter corrections
#             - Sort by distance, then rank
#             - Preserve original position
#             - THIS IS THE ONLY STEP THAT USES REDIS
    
#     Step 6: Re-check n-grams after correction (RAM)
#             - Corrected words may form new n-grams
    
#     Step 7: Build final output for Typesense

# PERFORMANCE:
#     - Steps 1-4, 6-7: RAM only (~0.01ms per lookup)
#     - Step 5: Redis only for unknown words (~50ms per correction)
#     - Typical query (no typos): ~1-5ms
#     - Query with 1-2 typos: ~50-150ms
# """

# import json
# import time
# import redis
# from typing import Dict, Any, List, Optional, Tuple, Set
# from decouple import config



# STOPWORD_POS = {
#     'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
#     'this': 'determiner', 'that': 'determiner',
#     'these': 'determiner', 'those': 'determiner',
#     'some': 'determiner', 'any': 'determiner',
#     'each': 'determiner', 'every': 'determiner',
#     'all': 'determiner', 'both': 'determiner',
#     'no': 'determiner', 'few': 'determiner',
#     'more': 'determiner', 'most': 'determiner',
#     'other': 'determiner', 'such': 'determiner',
#     'own': 'determiner', 'my': 'determiner', 'your': 'determiner',
#     'his': 'determiner', 'its': 'determiner', 'our': 'determiner',
#     'their': 'determiner', 'her': 'determiner',
#     'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#     'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#     'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
#     'into': 'preposition', 'through': 'preposition',
#     'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
#     'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
#     'above': 'preposition', 'below': 'preposition',
#     'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#     'so': 'conjunction', 'than': 'conjunction',
#     'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#     'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#     'me': 'pronoun', 'him': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
#     'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
#     'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#     'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
#     'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#     'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#     'will': 'modal', 'would': 'modal', 'could': 'modal',
#     'should': 'modal', 'may': 'modal', 'might': 'modal',
#     'must': 'modal', 'can': 'modal',
#     'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
#     'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
#     'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
#     'only': 'adverb', 'not': 'adverb', 'same': 'adverb', 'once': 'adverb',
#     'so': 'adverb',
# }
# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION')
# REDIS_PORT = config('REDIS_PORT', cast=int)
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
# REDIS_USERNAME = config('REDIS_USERNAME', default='')


# # =============================================================================
# # REDIS CONNECTION (Only for spelling correction - Step 5)
# # =============================================================================

# class RedisClient:
#     """Redis client for spelling correction only."""
    
#     _client: Optional[redis.Redis] = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
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
# # RAM CACHE - Uses vocabulary_cache.py singleton
# # =============================================================================

# try:
#     from .vocabulary_cache import vocab_cache
#     RAM_CACHE_AVAILABLE = True
#     print("✅ vocab_cache imported from .vocabulary_cache (RAM)")
# except ImportError:
#     try:
#         from vocabulary_cache import vocab_cache
#         RAM_CACHE_AVAILABLE = True
#         print("✅ vocab_cache imported from vocabulary_cache (RAM)")
#     except ImportError:
#         RAM_CACHE_AVAILABLE = False
#         vocab_cache = None
#         print("⚠️ vocabulary_cache not available — falling back to Redis for all lookups")


# class VocabCache:
#     """
#     RAM-based vocabulary cache wrapper.
    
#     Uses vocab_cache singleton from vocabulary_cache.py for O(1) lookups.
#     Falls back to Redis if RAM cache is not available.
#     """
    
#     def __init__(self):
#         self._ram = vocab_cache if RAM_CACHE_AVAILABLE else None
#         self._redis_client = None
    
#     def _get_redis_client(self) -> Optional[redis.Redis]:
#         """Get Redis client (fallback only)."""
#         if self._redis_client is None:
#             self._redis_client = RedisClient.get_client()
#         return self._redis_client
    
#     def get_term_matches(self, word: str) -> List[Dict[str, Any]]:
#         """
#         Get ALL matches for a word from RAM cache.
#         Returns list of all category variants, sorted by rank desc.
#         """
#         # === RAM PATH (fast) ===
#         if self._ram and self._ram.loaded:
#             matches = self._ram.get_all_term_matches(word.lower().strip())
#             if not matches:
#                 return []
            
#             # Normalize to the format WordDiscovery expects
#             normalized = []
#             for m in matches:
#                 rank = self._parse_rank(m.get('rank', 0))
#                 normalized.append({
#                     'term': m.get('term', word.lower()),
#                     'display': m.get('display', word.lower()),
#                     'category': m.get('category', ''),
#                     'description': m.get('description', ''),
#                     'pos': self._normalize_pos(m.get('pos')),
#                     'entity_type': m.get('entity_type', 'unigram'),
#                     'rank': rank,
#                 })
            
#             # Sort by rank descending
#             normalized.sort(key=lambda x: x['rank'], reverse=True)
#             return normalized
        
#         # === REDIS FALLBACK (slow) ===
#         client = self._get_redis_client()
#         if not client:
#             return []
        
#         word_lower = word.lower().strip()
#         pattern = f"term:{word_lower}:*"
        
#         try:
#             keys = client.keys(pattern)
#             if not keys:
#                 return []
            
#             matches = []
#             for key in keys:
#                 metadata = client.hgetall(key)
#                 if metadata:
#                     rank = self._parse_rank(metadata.get('rank', 0))
#                     matches.append({
#                         'term': metadata.get('term', word_lower),
#                         'display': metadata.get('display', word_lower),
#                         'category': metadata.get('category', ''),
#                         'description': metadata.get('description', ''),
#                         'pos': self._normalize_pos(metadata.get('pos')),
#                         'entity_type': metadata.get('entity_type', 'unigram'),
#                         'rank': rank,
#                     })
            
#             matches.sort(key=lambda x: x['rank'], reverse=True)
#             return matches
            
#         except Exception as e:
#             print(f"Error getting term matches for '{word}': {e}")
#             return []
    
#     def get_ngram(self, words: List[str]) -> Optional[Dict[str, Any]]:
#         """
#         Check if words form an n-gram (bigram, trigram, quadgram).
#         Uses RAM cache first, falls back to Redis.
#         """
#         if len(words) < 2:
#             return None
        
#         # === RAM PATH (fast) ===
#         if self._ram and self._ram.loaded:
#             words_lower = [w.lower() for w in words]
#             metadata = None
            
#             if len(words_lower) == 2:
#                 metadata = self._ram.get_bigram(words_lower[0], words_lower[1])
#             elif len(words_lower) == 3:
#                 metadata = self._ram.get_trigram(words_lower[0], words_lower[1], words_lower[2])
#             elif len(words_lower) == 4:
#                 metadata = self._ram.get_quadgram(words_lower[0], words_lower[1], words_lower[2], words_lower[3])
            
#             if not metadata:
#                 return None
            
#             phrase = ' '.join(words_lower)
#             ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'
            
#             return {
#                 'key': f"term:{phrase}:{metadata.get('category', '')}",
#                 'term': metadata.get('term', phrase),
#                 'display': metadata.get('display', phrase.title()),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'pos': self._normalize_pos(metadata.get('pos')),
#                 'entity_type': metadata.get('entity_type', ngram_type),
#                 'rank': self._parse_rank(metadata.get('rank', 0)),
#                 'words': words,
#                 'ngram_type': ngram_type,
#             }
        
#         # === REDIS FALLBACK (slow) ===
#         client = self._get_redis_client()
#         if not client:
#             return None
        
#         phrase = ' '.join(w.lower() for w in words)
#         phrase_underscore = '_'.join(w.lower() for w in words)

#         # Try underscore version first (most common in Redis)
#         pattern = f"term:{phrase_underscore}:*"
#         try:
#             keys = client.keys(pattern)
#         except Exception:
#             keys = []

#         # If not found, try space version
#         if not keys:
#             pattern = f"term:{phrase}:*"
#             try:
#                 keys = client.keys(pattern)
#             except Exception:
#                 keys = []
        
#         if not keys:
#             return None
        
#         try:
#             key = keys[0]
#             metadata = client.hgetall(key)
            
#             if metadata:
#                 rank = self._parse_rank(metadata.get('rank', 0))
#                 ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'
                
#                 return {
#                     'key': key,
#                     'term': metadata.get('term', phrase),
#                     'display': metadata.get('display', phrase.title()),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'pos': self._normalize_pos(metadata.get('pos')),
#                     'entity_type': metadata.get('entity_type', ngram_type),
#                     'rank': rank,
#                     'words': words,
#                     'ngram_type': ngram_type,
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error getting ngram for '{phrase}': {e}")
#             return None
    
#     def _normalize_pos(self, pos_value: Any) -> str:
#         """Normalize POS value from various formats."""
#         if pos_value is None:
#             return 'unknown'
        
#         if isinstance(pos_value, str):
#             if pos_value.startswith('['):
#                 try:
#                     parsed = json.loads(pos_value)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 try:
#                     fixed = pos_value.replace("'", '"')
#                     parsed = json.loads(fixed)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 if pos_value.startswith("['") and pos_value.endswith("']"):
#                     inner = pos_value[2:-2]
#                     return inner.lower()
            
#             return pos_value.lower()
        
#         if isinstance(pos_value, list):
#             return str(pos_value[0]).lower() if pos_value else 'unknown'
        
#         return str(pos_value).lower()
    
#     def _parse_rank(self, rank_value: Any) -> int:
#         """Parse rank to integer."""
#         try:
#             return int(float(rank_value))
#         except (ValueError, TypeError):
#             return 0


# # Global cache instance
# vocab_cache_wrapper = VocabCache()


# # =============================================================================
# # STOPWORDS
# # =============================================================================

# STOPWORDS = {
#     # Determiners/Articles
#     "the": "determiner", "a": "article", "an": "article",
#     "this": "determiner", "that": "determiner", "these": "determiner", "those": "determiner",
#     "my": "determiner", "your": "determiner", "his": "determiner", "her": "determiner",
#     "its": "determiner", "our": "determiner", "their": "determiner",
#     "some": "determiner", "any": "determiner", "no": "determiner",
#     "every": "determiner", "each": "determiner", "all": "determiner",
    
#     # Prepositions
#     "in": "preposition", "on": "preposition", "at": "preposition", "to": "preposition",
#     "for": "preposition", "of": "preposition", "with": "preposition", "by": "preposition",
#     "from": "preposition", "about": "preposition", "into": "preposition", "through": "preposition",
#     "during": "preposition", "before": "preposition", "after": "preposition",
#     "above": "preposition", "below": "preposition", "between": "preposition",
#     "under": "preposition", "over": "preposition", "near": "preposition",
    
#     # Conjunctions
#     "and": "conjunction", "or": "conjunction", "but": "conjunction",
#     "nor": "conjunction", "so": "conjunction", "yet": "conjunction",
    
#     # Pronouns
#     "i": "pronoun", "you": "pronoun", "he": "pronoun", "she": "pronoun",
#     "it": "pronoun", "we": "pronoun", "they": "pronoun",
#     "me": "pronoun", "him": "pronoun", "her": "pronoun", "us": "pronoun", "them": "pronoun",
#     "who": "pronoun", "whom": "pronoun", "what": "pronoun", "which": "pronoun",
#     "whose": "pronoun", "whoever": "pronoun", "whatever": "pronoun",
    
#     # Be verbs
#     "is": "be", "are": "be", "was": "be", "were": "be",
#     "be": "be", "been": "be", "being": "be",
#     "am": "be",
    
#     # Auxiliary/Modal verbs
#     "have": "auxiliary", "has": "auxiliary", "had": "auxiliary",
#     "do": "auxiliary", "does": "auxiliary", "did": "auxiliary",
#     "will": "modal", "would": "modal", "could": "modal", "should": "modal",
#     "may": "modal", "might": "modal", "must": "modal", "can": "modal",
    
#     # Other common words
#     "not": "negation", "no": "negation",
#     "as": "conjunction", "if": "conjunction", "when": "conjunction",
#     "than": "conjunction", "because": "conjunction", "while": "conjunction",
#     "where": "adverb", "how": "adverb", "why": "adverb",
#     "very": "adverb", "just": "adverb", "also": "adverb",
#     "only": "adverb", "even": "adverb", "still": "adverb",
#     "then": "adverb", "now": "adverb", "here": "adverb", "there": "adverb",
# }


# # =============================================================================
# # GRAMMAR RULES FOR POS PREDICTION
# # =============================================================================

# GRAMMAR_RULES = {
#     # =========================================================================
#     # BOTH NEIGHBORS KNOWN
#     # =========================================================================
    
#     ("determiner", "noun"): [("adjective", 0.95), ("noun", 0.60)],
#     ("determiner", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("determiner", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("determiner", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("determiner", "conjunction"): [("noun", 0.85)],
#     ("determiner", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     ("article", "noun"): [("adjective", 0.95), ("noun", 0.55)],
#     ("article", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("article", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("article", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("article", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("article", "conjunction"): [("noun", 0.85)],
#     ("article", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     ("adjective", "noun"): [("adjective", 0.90), ("noun", 0.50)],
#     ("adjective", "adjective"): [("adjective", 0.85), ("noun", 0.65), ("adverb", 0.50)],
#     ("adjective", "verb"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "preposition"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "adverb"): [("noun", 0.85), ("verb", 0.65)],
#     ("adjective", "conjunction"): [("noun", 0.90)],
#     ("adjective", "end"): [("noun", 0.95), ("proper_noun", 0.80)],
    
#     ("noun", "noun"): [("verb", 0.75), ("adjective", 0.65), ("noun", 0.50)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.80), ("adverb", 0.55)],
#     ("noun", "adverb"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "preposition"): [("verb", 0.85), ("noun", 0.55)],
#     ("noun", "verb"): [("adverb", 0.85), ("noun", 0.60)],
#     ("noun", "conjunction"): [("verb", 0.80), ("noun", 0.60)],
#     ("noun", "determiner"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "article"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "pronoun"): [("verb", 0.90)],
#     ("noun", "end"): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],
    
#     ("proper_noun", "noun"): [("verb", 0.80), ("noun", 0.60)],
#     ("proper_noun", "adjective"): [("verb", 0.90), ("be", 0.80)],
#     ("proper_noun", "adverb"): [("verb", 0.90)],
#     ("proper_noun", "preposition"): [("verb", 0.85)],
#     ("proper_noun", "verb"): [("adverb", 0.85), ("proper_noun", 0.60)],
#     ("proper_noun", "conjunction"): [("verb", 0.80)],
#     ("proper_noun", "end"): [("verb", 0.75), ("proper_noun", 0.70)],
    
#     ("verb", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("adverb", 0.60)],
#     ("verb", "adjective"): [("adverb", 0.90), ("noun", 0.55)],
#     ("verb", "adverb"): [("adverb", 0.85), ("noun", 0.60)],
#     ("verb", "verb"): [("adverb", 0.85), ("noun", 0.60), ("preposition", 0.50)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.75), ("pronoun", 0.55)],
#     ("verb", "conjunction"): [("noun", 0.85), ("adverb", 0.65)],
#     ("verb", "determiner"): [("adverb", 0.80), ("noun", 0.65)],
#     ("verb", "article"): [("adverb", 0.80), ("noun", 0.65)],
#     ("verb", "pronoun"): [("adverb", 0.80), ("preposition", 0.65)],
#     ("verb", "end"): [("noun", 0.85), ("adverb", 0.75), ("proper_noun", 0.65)],
    
#     ("preposition", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
#     ("preposition", "adjective"): [("adverb", 0.90), ("adjective", 0.65)],
#     ("preposition", "adverb"): [("adjective", 0.80), ("noun", 0.70)],
#     ("preposition", "verb"): [("noun", 0.90), ("proper_noun", 0.75), ("pronoun", 0.60)],
#     ("preposition", "preposition"): [("noun", 0.85), ("proper_noun", 0.75)],
#     ("preposition", "conjunction"): [("noun", 0.85), ("proper_noun", 0.70)],
#     ("preposition", "determiner"): [("noun", 0.80), ("adjective", 0.65)],
#     ("preposition", "article"): [("noun", 0.80), ("adjective", 0.65)],
#     ("preposition", "pronoun"): [("noun", 0.75), ("verb", 0.60)],
#     ("preposition", "end"): [("noun", 0.95), ("proper_noun", 0.85), ("adjective", 0.60)],
    
#     ("pronoun", "noun"): [("verb", 0.95), ("be", 0.80)],
#     ("pronoun", "adjective"): [("verb", 0.95), ("be", 0.85), ("adverb", 0.55)],
#     ("pronoun", "adverb"): [("verb", 0.95), ("be", 0.75)],
#     ("pronoun", "preposition"): [("verb", 0.90), ("be", 0.70)],
#     ("pronoun", "verb"): [("adverb", 0.85), ("modal", 0.70)],
#     ("pronoun", "conjunction"): [("verb", 0.85)],
#     ("pronoun", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     ("be", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
#     ("be", "adjective"): [("adverb", 0.95), ("adjective", 0.60)],
#     ("be", "adverb"): [("adjective", 0.85), ("verb", 0.70)],
#     ("be", "verb"): [("adverb", 0.90), ("noun", 0.55)],
#     ("be", "preposition"): [("noun", 0.90), ("adverb", 0.70)],
#     ("be", "conjunction"): [("adjective", 0.80), ("noun", 0.65)],
#     ("be", "end"): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.60)],
    
#     ("adverb", "noun"): [("adjective", 0.90), ("verb", 0.65)],
#     ("adverb", "adjective"): [("adverb", 0.85), ("adjective", 0.70)],
#     ("adverb", "adverb"): [("adverb", 0.80), ("verb", 0.65)],
#     ("adverb", "verb"): [("adverb", 0.90), ("noun", 0.50)],
#     ("adverb", "preposition"): [("verb", 0.85), ("noun", 0.65)],
#     ("adverb", "conjunction"): [("verb", 0.80), ("adjective", 0.65)],
#     ("adverb", "end"): [("adjective", 0.85), ("verb", 0.75), ("noun", 0.60)],
    
#     ("conjunction", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("verb", 0.55)],
#     ("conjunction", "adjective"): [("adverb", 0.85), ("noun", 0.65)],
#     ("conjunction", "adverb"): [("noun", 0.80), ("verb", 0.70)],
#     ("conjunction", "verb"): [("noun", 0.90), ("pronoun", 0.75)],
#     ("conjunction", "preposition"): [("noun", 0.85), ("pronoun", 0.70)],
#     ("conjunction", "conjunction"): [("noun", 0.80)],
#     ("conjunction", "end"): [("noun", 0.90), ("proper_noun", 0.75)],
    
#     ("modal", "noun"): [("verb", 0.90), ("adverb", 0.65)],
#     ("modal", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("modal", "adverb"): [("verb", 0.95)],
#     ("modal", "verb"): [("adverb", 0.90)],
#     ("modal", "preposition"): [("verb", 0.85)],
#     ("modal", "end"): [("verb", 0.95), ("noun", 0.55)],
    
#     ("auxiliary", "noun"): [("verb", 0.90), ("adjective", 0.65)],
#     ("auxiliary", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("auxiliary", "adverb"): [("verb", 0.95)],
#     ("auxiliary", "verb"): [("adverb", 0.90)],
#     ("auxiliary", "preposition"): [("verb", 0.85)],
#     ("auxiliary", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     ("participle", "noun"): [("adjective", 0.85), ("noun", 0.65)],
#     ("participle", "adjective"): [("adverb", 0.85), ("noun", 0.60)],
#     ("participle", "adverb"): [("noun", 0.80), ("adjective", 0.65)],
#     ("participle", "verb"): [("adverb", 0.80), ("noun", 0.65)],
#     ("participle", "preposition"): [("noun", 0.85), ("adverb", 0.65)],
#     ("participle", "end"): [("noun", 0.90), ("adverb", 0.70)],
    
#     ("gerund", "noun"): [("adjective", 0.85), ("noun", 0.70)],
#     ("gerund", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
#     ("gerund", "adverb"): [("noun", 0.80)],
#     ("gerund", "verb"): [("adverb", 0.80), ("noun", 0.60)],
#     ("gerund", "preposition"): [("noun", 0.85)],
#     ("gerund", "end"): [("noun", 0.90), ("adverb", 0.65)],
    
#     ("negation", "noun"): [("adjective", 0.85), ("verb", 0.75)],
#     ("negation", "adjective"): [("adverb", 0.90)],
#     ("negation", "adverb"): [("verb", 0.85), ("adjective", 0.70)],
#     ("negation", "verb"): [("adverb", 0.90)],
#     ("negation", "preposition"): [("verb", 0.80)],
#     ("negation", "end"): [("verb", 0.85), ("adjective", 0.70)],
    
#     ("quantifier", "noun"): [("adjective", 0.90), ("noun", 0.60)],
#     ("quantifier", "adjective"): [("adverb", 0.85), ("adjective", 0.65)],
#     ("quantifier", "adverb"): [("adjective", 0.80)],
#     ("quantifier", "verb"): [("noun", 0.85)],
#     ("quantifier", "preposition"): [("noun", 0.85)],
#     ("quantifier", "end"): [("noun", 0.95), ("adjective", 0.65)],
    
#     ("numeral", "noun"): [("adjective", 0.85), ("noun", 0.70)],
#     ("numeral", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
#     ("numeral", "adverb"): [("noun", 0.80)],
#     ("numeral", "verb"): [("noun", 0.85)],
#     ("numeral", "preposition"): [("noun", 0.85)],
#     ("numeral", "end"): [("noun", 0.95)],
    
#     # =========================================================================
#     # ONLY LEFT NEIGHBOR KNOWN
#     # =========================================================================
    
#     ("determiner", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
#     ("article", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
#     ("adjective", None): [("noun", 0.95), ("proper_noun", 0.75)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],
#     ("proper_noun", None): [("verb", 0.80), ("proper_noun", 0.70), ("noun", 0.55)],
#     ("verb", None): [("noun", 0.80), ("adverb", 0.75), ("adjective", 0.60), ("proper_noun", 0.50)],
#     ("preposition", None): [("noun", 0.90), ("proper_noun", 0.85), ("adjective", 0.60)],
#     ("pronoun", None): [("verb", 0.95), ("be", 0.80), ("modal", 0.65)],
#     ("be", None): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.65), ("verb", 0.55)],
#     ("adverb", None): [("adjective", 0.85), ("verb", 0.75), ("adverb", 0.60)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.75), ("proper_noun", 0.65), ("verb", 0.55)],
#     ("modal", None): [("verb", 0.95), ("adverb", 0.65)],
#     ("auxiliary", None): [("verb", 0.95), ("adverb", 0.65)],
#     ("participle", None): [("noun", 0.85), ("adverb", 0.70)],
#     ("gerund", None): [("noun", 0.90), ("adverb", 0.60)],
#     ("negation", None): [("verb", 0.85), ("adjective", 0.75), ("adverb", 0.60)],
#     ("quantifier", None): [("noun", 0.95), ("adjective", 0.70)],
#     ("numeral", None): [("noun", 0.95), ("adjective", 0.60)],
    
#     # =========================================================================
#     # ONLY RIGHT NEIGHBOR KNOWN
#     # =========================================================================
    
#     (None, "noun"): [("adjective", 0.95), ("determiner", 0.85), ("noun", 0.60), ("proper_noun", 0.50)],
#     (None, "proper_noun"): [("adjective", 0.80), ("preposition", 0.75), ("verb", 0.65)],
#     (None, "adjective"): [("adverb", 0.90), ("adjective", 0.70), ("determiner", 0.60)],
#     (None, "adverb"): [("verb", 0.85), ("adverb", 0.75), ("be", 0.60)],
#     (None, "verb"): [("noun", 0.90), ("pronoun", 0.85), ("adverb", 0.70), ("proper_noun", 0.60)],
#     (None, "preposition"): [("noun", 0.90), ("verb", 0.80), ("proper_noun", 0.65)],
#     (None, "determiner"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
#     (None, "article"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
#     (None, "pronoun"): [("verb", 0.85), ("preposition", 0.75), ("conjunction", 0.65)],
#     (None, "conjunction"): [("noun", 0.90), ("verb", 0.75), ("adjective", 0.60)],
#     (None, "be"): [("noun", 0.90), ("pronoun", 0.85), ("proper_noun", 0.70)],
#     (None, "modal"): [("noun", 0.90), ("pronoun", 0.85)],
#     (None, "auxiliary"): [("noun", 0.90), ("pronoun", 0.85)],
#     (None, "participle"): [("be", 0.85), ("adverb", 0.75), ("noun", 0.60)],
#     (None, "gerund"): [("preposition", 0.80), ("verb", 0.75), ("be", 0.65)],
#     (None, "negation"): [("verb", 0.85), ("be", 0.80), ("modal", 0.70)],
#     (None, "quantifier"): [("preposition", 0.80), ("verb", 0.70)],
#     (None, "numeral"): [("preposition", 0.80), ("verb", 0.70), ("determiner", 0.60)],
#     (None, "end"): [("noun", 0.90), ("proper_noun", 0.80), ("verb", 0.65), ("adjective", 0.55)],
    
#     # =========================================================================
#     # SPECIAL CASES - Start of query
#     # =========================================================================
    
#     ("start", "noun"): [("adjective", 0.90), ("determiner", 0.85)],
#     ("start", "adjective"): [("adverb", 0.85), ("determiner", 0.75)],
#     ("start", "verb"): [("noun", 0.90), ("pronoun", 0.80)],
#     ("start", "adverb"): [("verb", 0.80), ("adjective", 0.70)],
#     ("start", "preposition"): [("noun", 0.85), ("verb", 0.70)],
#     ("start", "end"): [("noun", 0.90), ("proper_noun", 0.85), ("verb", 0.60)],
# }


# # POS compatibility for matching predictions to candidates
# POS_COMPATIBILITY = {
#     'noun': {'noun', 'proper_noun'},
#     'proper_noun': {'noun', 'proper_noun'},
#     'verb': {'verb', 'participle', 'gerund'},
#     'adjective': {'adjective', 'participle'},
#     'adverb': {'adverb'},
#     'pronoun': {'pronoun'},
#     'determiner': {'determiner', 'article'},
#     'article': {'article', 'determiner'},
#     'preposition': {'preposition'},
#     'conjunction': {'conjunction'},
# }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """Calculate Damerau-Levenshtein distance."""
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


# def is_pos_compatible(candidate_pos: str, predicted_pos: str) -> bool:
#     """Check if candidate POS is compatible with predicted POS."""
#     candidate_pos = normalize_pos_string(candidate_pos)
#     predicted_pos = normalize_pos_string(predicted_pos)
    
#     if candidate_pos == predicted_pos:
#         return True
    
#     compatible_set = POS_COMPATIBILITY.get(predicted_pos, {predicted_pos})
#     return candidate_pos in compatible_set


# def normalize_pos_string(pos_value: any) -> str:
#     """Normalize POS value from various formats to clean string."""
#     if pos_value is None:
#         return 'unknown'
    
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 parsed = json.loads(pos_value)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             try:
#                 fixed = pos_value.replace("'", '"')
#                 parsed = json.loads(fixed)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             if pos_value.startswith("['") and pos_value.endswith("']"):
#                 inner = pos_value[2:-2]
#                 return inner.lower()
        
#         return pos_value.lower().strip()
    
#     if isinstance(pos_value, list):
#         return str(pos_value[0]).lower() if pos_value else 'unknown'
    
#     return str(pos_value).lower()


# def get_fuzzy_suggestions(word: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get spelling suggestions from Redis using fuzzy search.
#     This is the ONLY Redis call in the pipeline (for unknown words).
#     """
#     client = RedisClient.get_client()
#     if not client:
#         return []
    
#     word_lower = word.lower().strip()
#     if len(word_lower) < 2:
#         return []
    
#     try:
#         suggestions = []
        
#         for fuzzy_level in ['%', '%%']:
#             if fuzzy_level == '%%' and suggestions:
#                 break
            
#             query = f"{fuzzy_level}{word_lower}{fuzzy_level}"
            
#             try:
#                 result = client.execute_command(
#                     'FT.SEARCH', 'terms_idx', query,
#                     'SORTBY', 'rank', 'DESC',
#                     'LIMIT', '0', str(limit * 3)
#                 )
                
#                 if result and len(result) > 1:
#                     i = 1
#                     while i < len(result):
#                         key = result[i]
#                         fields = result[i + 1] if i + 1 < len(result) else []
                        
#                         metadata = {}
#                         for j in range(0, len(fields), 2):
#                             if j + 1 < len(fields):
#                                 metadata[fields[j]] = fields[j + 1]
                        
#                         if metadata:
#                             term = metadata.get('term', '')
#                             distance = damerau_levenshtein_distance(word_lower, term.lower())
                            
#                             if distance <= max_distance and distance > 0:
#                                 rank = metadata.get('rank', 0)
#                                 try:
#                                     rank = int(float(rank))
#                                 except (ValueError, TypeError):
#                                     rank = 0
                                
#                                 pos = metadata.get('pos', 'unknown')
#                                 if isinstance(pos, str) and pos.startswith('['):
#                                     try:
#                                         parsed = json.loads(pos)
#                                         pos = parsed[0] if parsed else 'unknown'
#                                     except:
#                                         pass
#                                 pos = str(pos).lower()
                                
#                                 suggestions.append({
#                                     'term': term,
#                                     'display': metadata.get('display', term),
#                                     'category': metadata.get('category', ''),
#                                     'description': metadata.get('description', ''),
#                                     'pos': pos,
#                                     'entity_type': metadata.get('entity_type', 'unigram'),
#                                     'rank': rank,
#                                     'distance': distance,
#                                 })
                        
#                         i += 2
                        
#             except Exception as e:
#                 print(f"Fuzzy search error for '{word}': {e}")
        
#         suggestions.sort(key=lambda x: (x['distance'], -x['rank']))
        
#         seen = set()
#         unique = []
#         for s in suggestions:
#             if s['term'].lower() not in seen:
#                 seen.add(s['term'].lower())
#                 unique.append(s)
        
#         return unique[:limit]
        
#     except Exception as e:
#         print(f"Error getting suggestions for '{word}': {e}")
#         return []

# def get_fuzzy_suggestions_batch(
#         words: List[str], 
#         limit: int = 10, 
#         max_distance: int = 2
#     ) -> Dict[str, List[Dict[str, Any]]]:
#         """
#         Get fuzzy suggestions for ALL words in ONE Redis pipeline call.
#         Returns dict of {word: [suggestions]}
#         """
#         client = RedisClient.get_client()
#         if not client:
#             return {word: [] for word in words}

#         unique_words = list({w.lower().strip() for w in words if len(w.strip()) >= 2})
        
#         if not unique_words:
#             return {}

#         # -------------------------------------------------------------------------
#         # Build ONE pipeline with ALL fuzzy searches at once
#         # -------------------------------------------------------------------------
#         pipe = client.pipeline(transaction=False)  # transaction=False = faster

#         for word in unique_words:
#             query = f"%{word}%"
#             pipe.execute_command(
#                 'FT.SEARCH', 'terms_idx', query,
#                 'SORTBY', 'rank', 'DESC',
#                 'LIMIT', '0', str(limit * 3)
#             )

#         # ONE round trip to Redis for ALL words
#         try:
#             all_results = pipe.execute()
#         except Exception as e:
#             print(f"Pipeline batch error: {e}")
#             return {word: [] for word in unique_words}

#         # -------------------------------------------------------------------------
#         # Map results back to each word
#         # -------------------------------------------------------------------------
#         batch_suggestions = {}

#         for word, result in zip(unique_words, all_results):
#             suggestions = []

#             if not result or len(result) <= 1:
#                 batch_suggestions[word] = []
#                 continue

#             i = 1
#             while i < len(result):
#                 key = result[i]
#                 fields = result[i + 1] if i + 1 < len(result) else []

#                 metadata = {}
#                 for j in range(0, len(fields), 2):
#                     if j + 1 < len(fields):
#                         metadata[fields[j]] = fields[j + 1]

#                 if metadata:
#                     term = metadata.get('term', '')
#                     distance = damerau_levenshtein_distance(word, term.lower())

#                     if distance <= max_distance and distance > 0:
#                         rank = metadata.get('rank', 0)
#                         try:
#                             rank = int(float(rank))
#                         except (ValueError, TypeError):
#                             rank = 0

#                         pos = metadata.get('pos', 'unknown')
#                         if isinstance(pos, str) and pos.startswith('['):
#                             try:
#                                 parsed = json.loads(pos)
#                                 pos = parsed[0] if parsed else 'unknown'
#                             except:
#                                 pass
#                         pos = str(pos).lower()

#                         suggestions.append({
#                             'term': term,
#                             'display': metadata.get('display', term),
#                             'category': metadata.get('category', ''),
#                             'description': metadata.get('description', ''),
#                             'pos': pos,
#                             'entity_type': metadata.get('entity_type', 'unigram'),
#                             'rank': rank,
#                             'distance': distance,
#                         })

#                 i += 2

#             # If % fuzzy found nothing, try %% (wider fuzzy)
#             if not suggestions:
#                 try:
#                     wider_result = client.execute_command(
#                         'FT.SEARCH', 'terms_idx', f"%%{word}%%",
#                         'SORTBY', 'rank', 'DESC',
#                         'LIMIT', '0', str(limit * 3)
#                     )
#                     i = 1
#                     while i < len(wider_result):
#                         fields = wider_result[i + 1] if i + 1 < len(wider_result) else []
#                         metadata = {}
#                         for j in range(0, len(fields), 2):
#                             if j + 1 < len(fields):
#                                 metadata[fields[j]] = fields[j + 1]
#                         if metadata:
#                             term = metadata.get('term', '')
#                             distance = damerau_levenshtein_distance(word, term.lower())
#                             if distance <= max_distance and distance > 0:
#                                 rank = metadata.get('rank', 0)
#                                 try:
#                                     rank = int(float(rank))
#                                 except (ValueError, TypeError):
#                                     rank = 0
#                                 pos = metadata.get('pos', 'unknown')
#                                 if isinstance(pos, str) and pos.startswith('['):
#                                     try:
#                                         parsed = json.loads(pos)
#                                         pos = parsed[0] if parsed else 'unknown'
#                                     except:
#                                         pass
#                                 pos = str(pos).lower()
#                                 suggestions.append({
#                                     'term': term,
#                                     'display': metadata.get('display', term),
#                                     'category': metadata.get('category', ''),
#                                     'description': metadata.get('description', ''),
#                                     'pos': pos,
#                                     'entity_type': metadata.get('entity_type', 'unigram'),
#                                     'rank': rank,
#                                     'distance': distance,
#                                 })
#                         i += 2
#                 except Exception as e:
#                     print(f"Wider fuzzy search error for '{word}': {e}")

#             # Sort by distance then rank
#             suggestions.sort(key=lambda x: (x['distance'], -x['rank']))

#             # Deduplicate
#             seen = set()
#             unique = []
#             for s in suggestions:
#                 if s['term'].lower() not in seen:
#                     seen.add(s['term'].lower())
#                     unique.append(s)

#             batch_suggestions[word] = unique[:limit]

#         return batch_suggestions


# # =============================================================================
# # MAIN WORD DISCOVERY CLASS
# # =============================================================================

# class WordDiscovery:
#     """
#     Word Discovery Engine - The brain behind Typesense queries.
    
#     Uses RAM cache (vocabulary_cache.py) for all lookups.
#     Only uses Redis for fuzzy spelling correction (Step 5).
#     """
    
#     def __init__(self, verbose: bool = False):
#         self.verbose = verbose
#         self.cache = vocab_cache_wrapper
    
#     def process(self, query: str) -> Dict[str, Any]:
#         """Main entry point - process a query through all steps."""
#         start_time = time.perf_counter()
        
#         if self.verbose:
#             print("\n" + "=" * 70)
#             print(f"🔍 WORD DISCOVERY: '{query}'")
#             print(f"   RAM cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
#             print("=" * 70)
        
#         if not query or not query.strip():
#             return self._empty_result(query)
        
#         words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
#         words = [w for w in words if w]
        
#         if len(words) == 1:
#             return self._process_single_word(words[0], start_time)
        
#         # Step 1: Collect ALL data (RAM)
#         word_data = self._step1_collect_all_data(words)
        
#         # Step 2: Mark n-gram positions (RAM)
#         ngrams, consumed_positions = self._step2_detect_ngrams(words, word_data)
        
#         # Step 3: Determine POS for remaining words
#         self._step3_determine_pos(word_data, consumed_positions)

#         # Step 3.5: Refine POS from suffixes
#         self._step3_5_refine_pos_from_suffix(word_data)
        
#         # Step 4: Select best match (POS + Rank) (RAM)
#         self._step4_select_best_match(word_data, consumed_positions)
        
#         # Step 5: Handle unknowns (REDIS - only step that uses Redis)
#         corrections = self._step5_correct_unknowns(word_data)
        
#         # Step 6: Re-check n-grams after correction (RAM)
#         corrected_words = self._get_working_words(word_data)
#         new_ngrams, new_consumed = self._step6_recheck_ngrams(
#             corrected_words, word_data, consumed_positions
#         )
#         ngrams.extend(new_ngrams)
#         consumed_positions.update(new_consumed)
        
#         # Step 7: Build final output
#         output = self._step7_build_output(
#             query, word_data, ngrams, consumed_positions, corrections, start_time
#         )
        
#         return output
    
#     # =========================================================================
#     # STEP 1: Collect ALL Data (RAM)
#     # =========================================================================
    
#     def _step1_collect_all_data(self, words: List[str]) -> List[Dict[str, Any]]:
#         """Collect all matches for each word from RAM cache."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("📖 STEP 1: Collect ALL Data (RAM)")
#             print("-" * 70)
        
#         word_data = []
        
#         for i, word in enumerate(words):
#             position = i + 1
#             word_lower = word.lower().strip()
            
#             # Check stopwords first
#             if word_lower in STOPWORDS:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': True,
#                     'pos': STOPWORDS[word_lower],
#                     'predicted_pos': None,
#                     'all_matches': [],

#                     'selected_match': {
#                         'term': word_lower,
#                         'display': word_lower,
#                         'category': 'stopword',
#                         'description': '',
#                         'pos': STOPWORDS[word_lower],
#                         'entity_type': 'stopword',
#                         'rank': 0,
#                     },
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → STOPWORD ({STOPWORDS[word_lower]})")
#                 continue
            
#             # Get ALL matches from RAM cache
#             matches = self.cache.get_term_matches(word_lower)
            
#             if matches:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'found',
#                     'is_stopword': False,
#                     'pos': None,
#                     'predicted_pos': None,
#                     'all_matches': matches,
#                     'selected_match': None,
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → FOUND ({len(matches)} matches)")
#                     for m in matches[:3]:
#                         print(f"       - {m['category']}: pos={m['pos']}, rank={m['rank']}")
#                     if len(matches) > 3:
#                         print(f"       ... and {len(matches) - 3} more")
#             else:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'unknown',
#                     'is_stopword': False,
#                     'pos': None,
#                     'predicted_pos': None,
#                     'all_matches': [],
#                     'selected_match': None,
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → UNKNOWN")
        
#         return word_data

    
#     # =========================================================================
#     # STEP 2: Detect N-grams (RAM)
#     # =========================================================================
    
#     def _step2_detect_ngrams(
#         self, 
#         words: List[str],
#         word_data: List[Dict[str, Any]]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """Detect all n-grams using RAM cache, prioritizing longest matches."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔗 STEP 2: Detect N-grams (RAM)")
#             print("-" * 70)
        
#         ngrams = []
#         consumed = set()
        
#         # Check quadgrams first
#         if len(words) >= 4:
#             for i in range(len(words) - 3):
#                 if any(p in consumed for p in [i, i+1, i+2, i+3]):
#                     continue
                
#                 quad = words[i:i+4]
#                 result = self.cache.get_ngram(quad)
                
#                 if result:
#                     positions = [i+1, i+2, i+3, i+4]
#                     ngrams.append({
#                         'type': 'quadgram',
#                         'positions': positions,
#                         'words': quad,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1, i+2, i+3])
                    
#                     if self.verbose:
#                         print(f"  ✅ QUADGRAM: '{' '.join(quad)}'")
#                         print(f"       Category: {result['category']}")
        
#         # Check trigrams
#         if len(words) >= 3:
#             for i in range(len(words) - 2):
#                 if any(p in consumed for p in [i, i+1, i+2]):
#                     continue
                
#                 tri = words[i:i+3]
#                 result = self.cache.get_ngram(tri)
                
#                 if result:
#                     positions = [i+1, i+2, i+3]
#                     ngrams.append({
#                         'type': 'trigram',
#                         'positions': positions,
#                         'words': tri,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1, i+2])
                    
#                     if self.verbose:
#                         print(f"  ✅ TRIGRAM: '{' '.join(tri)}'")
#                         print(f"       Category: {result['category']}")
        
#         # Check bigrams
#         if len(words) >= 2:
#             for i in range(len(words) - 1):
#                 if any(p in consumed for p in [i, i+1]):
#                     continue
                
#                 bi = words[i:i+2]
#                 result = self.cache.get_ngram(bi)
                
#                 if result:
#                     positions = [i+1, i+2]
#                     ngrams.append({
#                         'type': 'bigram',
#                         'positions': positions,
#                         'words': bi,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1])
                    
#                     if self.verbose:
#                         print(f"  ✅ BIGRAM: '{' '.join(bi)}'")
#                         print(f"       Category: {result['category']}")
        
#         if not ngrams and self.verbose:
#             print("  (no n-grams found)")
        
#         return ngrams, consumed

#     # =========================================================================
#     # STEP 3: Determine POS for ALL Words
#     # =========================================================================
    
#     def _step3_determine_pos(
#     self,
#     word_data: List[Dict[str, Any]],
#     consumed_positions: Set[int]
#     ) -> None:
#         """Determine POS for ALL words using grammar rules — multi-pass for adjacent unknowns."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🧠 STEP 3: Determine POS (Grammar Rules — Multi-Pass)")
#             print("-" * 70)

#         def get_nearest_known_pos(index: int, direction: int) -> Optional[str]:
#             """
#             Walk the array left (direction=-1) or right (direction=1)
#             past unknown words until a known POS is found.
#             Returns that POS as the context anchor, or None if not found.
#             """
#             i = index + direction
#             while 0 <= i < len(word_data):
#                 wd = word_data[i]
#                 if wd['is_stopword']:
#                     return wd['pos']
#                 if wd.get('pos'):
#                     return wd['pos']
#                 if wd.get('predicted_pos'):
#                     return wd['predicted_pos']
#                 if wd['all_matches']:
#                     return normalize_pos_string(wd['all_matches'][0]['pos'])
#                 i += direction
#             return None

#         def resolve_word(i: int, wd: Dict[str, Any]) -> bool:
#             """
#             Try to resolve POS for a single word using its nearest known neighbors.
#             Returns True if a prediction was made, False if neighbors were still unknown.
#             """
#             position = wd['position']
#             pos_index = position - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions

#             if wd['is_stopword']:
#                 wd['predicted_pos'] = wd['pos']
#                 wd['predicted_pos_list'] = [(wd['pos'], 1.0)]
#                 return True

#             # Walk left to find nearest known POS
#             left_pos = None
#             if i == 0:
#                 left_pos = 'start'
#             else:
#                 left_pos = get_nearest_known_pos(i, direction=-1)

#             # Walk right to find nearest known POS
#             right_pos = None
#             if i == len(word_data) - 1:
#                 right_pos = 'end'
#             else:
#                 right_pos = get_nearest_known_pos(i, direction=1)

#             # If both neighbors are still unknown, we cannot resolve yet
#             if left_pos is None and right_pos is None:
#                 return False

#             # Apply grammar rules with whatever context we have
#             predicted_pos = None

#             if left_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))

#             if not predicted_pos and left_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, None))

#             if not predicted_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((None, right_pos))

#             if not predicted_pos:
#                 predicted_pos = [('noun', 0.75)]

#             # Passive voice lookahead
#             has_be_before = any(
#                 word_data[j]['is_stopword'] and word_data[j]['pos'] == 'be'
#                 for j in range(0, i)
#             )
#             next_ends_ed = False
#             if i + 1 < len(word_data):
#                 next_word_clean = word_data[i + 1]['word'].rstrip('?!.,;:')
#                 next_ends_ed = next_word_clean.endswith('ed')

#             if has_be_before and next_ends_ed and not wd['is_stopword']:
#                 is_known_adverb = any(
#                     m['pos'] in ('adverb',)
#                     for m in wd.get('all_matches', [])
#                 )
#                 if not is_known_adverb:
#                     predicted_pos = [
#                         ('noun', 0.95),
#                         ('proper_noun', 0.85),
#                         ('adjective', 0.4),
#                         ('adverb', 0.3),
#                     ]
#                     if self.verbose:
#                         print(f"       ⚡ Passive voice: [be]..._{wd['word']}_...[-ed] → noun (0.95)")

#             if isinstance(predicted_pos, list):
#                 wd['predicted_pos_list'] = predicted_pos
#                 wd['predicted_pos'] = predicted_pos[0][0] if predicted_pos else 'noun'
#             else:
#                 wd['predicted_pos_list'] = [(predicted_pos, 0.90)]
#                 wd['predicted_pos'] = predicted_pos

#             if self.verbose:
#                 context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
#                 ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
#                 print(f"  [{position}] '{wd['word']}' → Predicted: {wd['predicted_pos_list']}{ngram_note}")
#                 print(f"       Context: {context}")

#             return True

#         # -------------------------------------------------------------------------
#         # Multi-pass loop
#         # Pass 1: resolve words with at least one known neighbor
#         # Pass 2+: resolved words from previous pass become known neighbors
#         #          for remaining unknowns
#         # Stop when no progress is made (truly isolated unknowns default to noun)
#         # -------------------------------------------------------------------------
#         max_passes = len(word_data)
#         resolved = set()

#         for pass_num in range(max_passes):
#             progress = False

#             if self.verbose and pass_num > 0:
#                 print(f"\n  ── Pass {pass_num + 1} ──")

#             for i, wd in enumerate(word_data):
#                 if i in resolved:
#                     continue

#                 success = resolve_word(i, wd)

#                 if success:
#                     resolved.add(i)
#                     progress = True

#             # Stop when all resolved or no progress made
#             if not progress or len(resolved) == len(word_data):
#                 break

#         # Any word still unresolved (fully surrounded by unknowns) defaults to noun
#         for i, wd in enumerate(word_data):
#             if i not in resolved:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{wd['word']}' → No context found, defaulting to noun")
#                 wd['predicted_pos'] = 'noun'
#                 wd['predicted_pos_list'] = [('noun', 0.75)]
#     # def _step3_determine_pos(
#     #     self, 
#     #     word_data: List[Dict[str, Any]], 
#     #     consumed_positions: Set[int]
#     # ) -> None:
#     #     """Determine POS for ALL words using grammar rules."""
#     #     if self.verbose:
#     #         print("\n" + "-" * 70)
#     #         print("🧠 STEP 3: Determine POS (Grammar Rules)")
#     #         print("-" * 70)
        
#     #     for i, wd in enumerate(word_data):
#     #         position = wd['position']
            
#     #         pos_index = position - 1
#     #         wd['part_of_ngram'] = pos_index in consumed_positions
            
#     #         if wd['is_stopword']:
#     #             wd['predicted_pos'] = wd['pos']
#     #             wd['predicted_pos_list'] = [(wd['pos'], 1.0)]
#     #             if self.verbose:
#     #                 print(f"  [{position}] '{wd['word']}' → Stopword ({wd['pos']})")
#     #             continue
            
#     #         # Get left neighbor POS
#     #         left_pos = None
#     #         if i > 0:
#     #             left_wd = word_data[i - 1]
#     #             if left_wd['is_stopword']:
#     #                 left_pos = left_wd['pos']
#     #             elif left_wd.get('predicted_pos'):
#     #                 pred = left_wd['predicted_pos']
#     #                 if isinstance(pred, tuple):
#     #                     left_pos = pred[0]
#     #                 else:
#     #                     left_pos = pred
#     #             elif left_wd['all_matches']:
#     #                 raw_pos = left_wd['all_matches'][0]['pos']
#     #                 left_pos = normalize_pos_string(raw_pos)
#     #         else:
#     #             left_pos = 'start'
            
#     #         # Get right neighbor POS
#     #         right_pos = None
#     #         if i < len(word_data) - 1:
#     #             right_wd = word_data[i + 1]
#     #             if right_wd['is_stopword']:
#     #                 right_pos = right_wd['pos']
#     #             elif right_wd['all_matches']:
#     #                 raw_pos = right_wd['all_matches'][0]['pos']
#     #                 right_pos = normalize_pos_string(raw_pos)
#     #         else:
#     #             right_pos = 'end'
            
#     #         # Apply grammar rules
#     #         predicted_pos = None
            
#     #         if left_pos and right_pos:
#     #             predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))
            
#     #         if not predicted_pos and left_pos:
#     #             predicted_pos = GRAMMAR_RULES.get((left_pos, None))
            
#     #         if not predicted_pos and right_pos:
#     #             predicted_pos = GRAMMAR_RULES.get((None, right_pos))
            
#     #         if not predicted_pos:
#     #             predicted_pos = [('noun', 0.75)]
            
#     #         # Passive voice lookahead
#     #         has_be_before = any(
#     #             word_data[j]['is_stopword'] and word_data[j]['pos'] == 'be'
#     #             for j in range(0, i)
#     #         )
#     #         next_ends_ed = False
#     #         if i + 1 < len(word_data):
#     #             next_word_clean = word_data[i + 1]['word'].rstrip('?!.,;:')
#     #             next_ends_ed = next_word_clean.endswith('ed')

#     #         if has_be_before and next_ends_ed and not wd['is_stopword']:
#     #             is_known_adverb = any(
#     #                 m['pos'] in ('adverb',)
#     #                 for m in wd.get('all_matches', [])
#     #             )
#     #             if not is_known_adverb:
#     #                 predicted_pos = [
#     #                     ('noun', 0.95),
#     #                     ('proper_noun', 0.85),
#     #                     ('adjective', 0.4),
#     #                     ('adverb', 0.3),
#     #                 ]
#     #                 if self.verbose:
#     #                     print(f"       ⚡ Passive voice detected: [be]..._{wd['word']}_...[-ed] → noun (0.95)")

#     #         # Store both formats
#     #         if isinstance(predicted_pos, list):
#     #             wd['predicted_pos_list'] = predicted_pos
#     #             wd['predicted_pos'] = predicted_pos[0][0] if predicted_pos else 'noun'
#     #         else:
#     #             wd['predicted_pos_list'] = [(predicted_pos, 0.90)]
#     #             wd['predicted_pos'] = predicted_pos
            
#     #         if self.verbose:
#     #             context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
#     #             ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
#     #             print(f"  [{position}] '{wd['word']}' → Predicted: {wd['predicted_pos_list']}{ngram_note}")
#     #             print(f"       Context: {context}")

#     # =========================================================================
#     # STEP 3.5: Refine POS from Word Suffixes
#     # =========================================================================
    
#     SUFFIX_POS_RULES = {
#         'ing': [('gerund', 0.80), ('adjective', 0.75), ('verb', 0.70), ('noun', 0.50)],
#         'ed': [('verb', 0.85), ('adjective', 0.75), ('participle', 0.70)],
#         'ly': [('adverb', 0.95), ('adjective', 0.40)],
#         'tion': [('noun', 0.95)],
#         'sion': [('noun', 0.95)],
#         'ment': [('noun', 0.95)],
#         'ness': [('noun', 0.95)],
#         'ity': [('noun', 0.95)],
#         'ence': [('noun', 0.90)],
#         'ance': [('noun', 0.90)],
#         'ure': [('noun', 0.80)],
#         'ism': [('noun', 0.95)],
#         'ist': [('noun', 0.90)],
#         'ery': [('noun', 0.85)],
#         'ory': [('noun', 0.80), ('adjective', 0.60)],
#         'age': [('noun', 0.80)],
#         'ship': [('noun', 0.95)],
#         'dom': [('noun', 0.90)],
#         'hood': [('noun', 0.95)],
#         'ling': [('noun', 0.80)],
#         'ful': [('adjective', 0.95)],
#         'less': [('adjective', 0.95)],
#         'able': [('adjective', 0.90)],
#         'ible': [('adjective', 0.90)],
#         'ous': [('adjective', 0.95)],
#         'ious': [('adjective', 0.95)],
#         'ive': [('adjective', 0.90)],
#         'al': [('adjective', 0.85), ('noun', 0.50)],
#         'ial': [('adjective', 0.90)],
#         'ical': [('adjective', 0.90)],
#         'ish': [('adjective', 0.85)],
#         'ern': [('adjective', 0.80)],
#         'ese': [('adjective', 0.80), ('noun', 0.70)],
#         'ian': [('adjective', 0.80), ('noun', 0.75)],
#         'ean': [('adjective', 0.80)],
#         'ic': [('adjective', 0.85)],
#         'est': [('adjective', 0.85)],
#         'ent': [('adjective', 0.75), ('noun', 0.65)],
#         'ant': [('adjective', 0.75), ('noun', 0.65)],
#         'ify': [('verb', 0.95)],
#         'ize': [('verb', 0.95)],
#         'ise': [('verb', 0.90)],
#         'ate': [('verb', 0.80), ('adjective', 0.60), ('noun', 0.50)],
#         'en': [('verb', 0.70), ('adjective', 0.60)],
#         'er': [('noun', 0.75), ('adjective', 0.65)],
#         'or': [('noun', 0.80)],
#         'ee': [('noun', 0.85)],
#         'eer': [('noun', 0.85)],
#     }
    
#     SUFFIX_EXCEPTIONS = frozenset({
#         'ring', 'king', 'sing', 'bring', 'thing', 'string', 'spring',
#         'wing', 'swing', 'sting', 'cling', 'fling', 'sling', 'wring',
#         'bling', 'ding', 'ping', 'zing', 'ming', 'bing', 'ling',
#         'beijing', 'sterling', 'viking', 'darling', 'ceiling',
#         'feeling', 'dealing', 'healing', 'meaning', 'evening',
#         'bed', 'red', 'shed', 'led', 'fed', 'wed', 'sled',
#         'fly', 'ply', 'sly', 'holy', 'ugly', 'bully', 'belly',
#         'lily', 'jelly', 'jolly', 'rally', 'ally', 'tally',
#         'family', 'italy', 'july', 'daily', 'early',
#         'water', 'after', 'under', 'over', 'never', 'ever', 'other',
#         'rather', 'either', 'neither', 'whether', 'together',
#         'butter', 'letter', 'matter', 'better', 'bitter', 'litter',
#         'dinner', 'inner', 'upper', 'lower', 'power', 'flower',
#         'tower', 'shower', 'silver', 'river', 'liver', 'cover',
#         'fever', 'clever', 'finger', 'ginger', 'tiger', 'anger',
#         'danger', 'hunger', 'umber', 'number', 'member', 'timber',
#         'order', 'border', 'murder', 'wonder', 'thunder', 'super',
#         'paper', 'copper', 'proper', 'whisper', 'chapter', 'master',
#         'sister', 'mister', 'winter', 'center', 'corner', 'monster',
#         'soccer', 'cancer', 'rubber', 'hammer', 'summer', 'manner',
#         'animal', 'hospital', 'capital', 'metal', 'total', 'eral',
#         'canal', 'eral', 'general', 'mineral', 'festival', 'signal',
#         'journal', 'rival', 'survival', 'arrival', 'proposal',
#         'awful',
#         'age', 'page', 'sage', 'cage', 'rage', 'wage', 'stage',
#         'cement', 'moment', 'comment', 'segment', 'element',
#         'music', 'magic', 'basic', 'logic', 'topic', 'panic',
#         'fabric', 'garlic', 'picnic', 'traffic', 'public',
#         'west', 'east', 'nest', 'rest', 'test', 'best', 'chest',
#         'quest', 'guest', 'forest', 'harvest', 'interest', 'protest',
#         'request', 'suggest', 'modest', 'honest', 'invest',
#         'gate', 'late', 'mate', 'date', 'fate', 'rate', 'state',
#         'plate', 'private', 'climate', 'chocolate', 'senate',
#         'debate', 'estate', 'ultimate', 'intimate', 'accurate',
#         'delicate', 'separate', 'desperate', 'moderate', 'adequate',
#         'fish', 'dish', 'wish', 'finish', 'publish', 'polish',
#         'vanish', 'perish', 'cherish', 'nourish', 'flourish',
#     })
    
#     MIN_SUFFIX_WORD_LENGTH = 4
    
#     def _step3_5_refine_pos_from_suffix(self, word_data: List[Dict[str, Any]]) -> None:
#         """Refine POS predictions using word suffixes."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔤 STEP 3.5: Refine POS from Suffixes")
#             print("-" * 70)
        
#         for wd in word_data:
#             if wd['is_stopword']:
#                 continue
            
#             word = wd.get('corrected', wd['word']).lower()
            
#             if len(word) < self.MIN_SUFFIX_WORD_LENGTH:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Too short, skipping")
#                 continue
            
#             if word in self.SUFFIX_EXCEPTIONS:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Exception, skipping")
#                 continue
            
#             matched_suffix = None
#             suffix_predictions = None
            
#             sorted_suffixes = sorted(self.SUFFIX_POS_RULES.keys(), key=len, reverse=True)
            
#             for suffix in sorted_suffixes:
#                 if word.endswith(suffix) and len(word) > len(suffix) + 1:
#                     matched_suffix = suffix
#                     suffix_predictions = self.SUFFIX_POS_RULES[suffix]
#                     break
            
#             if not matched_suffix:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → No suffix match")
#                 continue
            
#             current_list = wd.get('predicted_pos_list', [])
#             current_primary = wd.get('predicted_pos', 'noun')
            
#             refined = {}
            
#             for pos, conf in current_list:
#                 refined[pos] = conf
            
#             for pos, suffix_conf in suffix_predictions:
#                 if pos in refined:
#                     existing = refined[pos]
#                     boosted = min((existing + suffix_conf) / 2 + 0.10, 0.98)
#                     refined[pos] = boosted
#                 else:
#                     refined[pos] = suffix_conf * 0.85
            
#             refined_list = sorted(refined.items(), key=lambda x: -x[1])
            
#             wd['predicted_pos_list'] = refined_list
#             wd['predicted_pos'] = refined_list[0][0] if refined_list else current_primary
#             wd['suffix_detected'] = matched_suffix
            
#             if self.verbose:
#                 changed = wd['predicted_pos'] != current_primary
#                 change_marker = " ⚡ CHANGED" if changed else ""
#                 print(f"  [{wd['position']}] '{word}' → Suffix: -{matched_suffix}{change_marker}")
#                 print(f"       Before: {current_list}")
#                 print(f"       After:  {refined_list}")

#     # =========================================================================
#     # STEP 4: Select Best Match (POS + Rank) (RAM)
#     # =========================================================================
    
#     def _step4_select_best_match(
#         self, 
#         word_data: List[Dict[str, Any]], 
#         consumed_positions: Set[int]
#     ) -> None:
#         """For ALL words with multiple matches, select based on POS + rank."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🎯 STEP 4: Select Best Match (POS + Rank)")
#             print("-" * 70)
        
#         for wd in word_data:
#             position = wd['position']
            
#             if wd['is_stopword']:
#                 continue
            
#             if wd['status'] == 'unknown':
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Unknown (defer to Step 5)")
#                 continue
            
#             matches = wd['all_matches']
#             if not matches:
#                 continue
            
#             predicted_pos = wd['predicted_pos']
            
#             compatible_matches = []
#             for m in matches:
#                 match_pos = normalize_pos_string(m['pos'])
#                 if is_pos_compatible(match_pos, predicted_pos):
#                     compatible_matches.append(m)
            
#             pos_index = position - 1
#             is_in_ngram = pos_index in consumed_positions
#             ngram_note = " (in n-gram)" if is_in_ngram else ""
            
#             if compatible_matches:
#                 compatible_matches.sort(key=lambda x: x['rank'], reverse=True)
#                 best = compatible_matches[0]
#                 wd['selected_match'] = best
#                 wd['pos'] = normalize_pos_string(best['pos'])
#                 wd['status'] = 'valid'
                
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Selected: {best['category']} (pos={normalize_pos_string(best['pos'])}, rank={best['rank']}){ngram_note}")
#                     if len(matches) > 1:
#                         rejected = [f"{m['category']}({normalize_pos_string(m['pos'])})" for m in matches if m != best]
#                         print(f"       Rejected: {rejected}")
#             else:
#                 best = matches[0]
#                 wd['selected_match'] = best
#                 wd['pos'] = normalize_pos_string(best['pos'])
#                 wd['status'] = 'valid'
                
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Fallback: {best['category']} (no POS match, using rank){ngram_note}")


#     # =========================================================================
#     # STEP 5: Correct Unknown Words (REDIS - only Redis step)
#     # =========================================================================
    
#     # def _step5_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     #     """Correct unknown/misspelled words using Redis fuzzy search."""
        
#     #     unknowns = []
#     #     pos_mismatches = []
        
#     #     for wd in word_data:
#     #         if wd['status'] == 'unknown':
#     #             unknowns.append(wd)
#     #         # elif (
#     #         #     wd['status'] == 'valid'
#     #         #     and not wd['is_stopword']
#     #         #     and wd.get('predicted_pos')
#     #         #     and wd.get('pos')
#     #         #     and wd['predicted_pos'] != wd['pos']
#     #         # ):
#     #         #     predicted_list = wd.get('predicted_pos_list', [])
#     #         #     top_confidence = predicted_list[0][1] if predicted_list else 0
#     #         #     if top_confidence >= 0.90:
#     #         #         pos_mismatches.append(wd)
#     #         elif (
#     #             wd['status'] == 'valid'
#     #             and not wd['is_stopword']
#     #             and wd.get('predicted_pos')
#     #             and wd.get('pos')
#     #             and wd['predicted_pos'] != wd['pos']
#     #         ):
#     #             predicted_list = wd.get('predicted_pos_list', [])
#     #             top_confidence = predicted_list[0][1] if predicted_list else 0
#     #             word_rank = wd.get('selected_match', {}).get('rank', 0) or 0
#     #             # Only POS-correct low-rank words (likely typos, not real words)
#     #             # High-rank words are common in our corpus — trust them
#     #             if top_confidence >= 0.90 and word_rank < 200:
#     #                 pos_mismatches.append(wd)
        
#     #     if not unknowns and not pos_mismatches:
#     #         if self.verbose:
#     #             print("\n" + "-" * 70)
#     #             print("🔧 STEP 5: Correct Unknowns")
#     #             print("-" * 70)
#     #             print("  (no unknown words)")
#     #         return []
        
#     #     if self.verbose:
#     #         print("\n" + "-" * 70)
#     #         print("🔧 STEP 5: Correct Unknowns (Redis Fuzzy)")
#     #         print("-" * 70)
        
#     #     corrections = []
        
#     #     # Process unknowns
#     #     for wd in unknowns:
#     #         word = wd['word']
#     #         position = wd['position']
#     #         predicted_pos = wd['predicted_pos'] or 'noun'
            
#     #         suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
#     #         if not suggestions:
#     #             if self.verbose:
#     #                 print(f"  [{position}] '{word}' → No suggestions found")
#     #             continue
            
#     #         compatible = [s for s in suggestions if is_pos_compatible(s['pos'], predicted_pos)]
            
#     #         if compatible:
#     #             compatible.sort(key=lambda x: (x['distance'], -x['rank']))
#     #             best = compatible[0]
#     #         else:
#     #             best = suggestions[0]
            
#     #         wd['status'] = 'corrected'
#     #         wd['corrected'] = best['term']
#     #         wd['corrected_display'] = best['display']
#     #         wd['pos'] = best['pos']
#     #         wd['distance'] = best['distance']
#     #         wd['selected_match'] = {
#     #             'term': best['term'],
#     #             'display': best['display'],
#     #             'category': best['category'],
#     #             'description': best['description'],
#     #             'pos': best['pos'],
#     #             'entity_type': best['entity_type'],
#     #             'rank': best['rank'],
#     #         }
            
#     #         corrections.append({
#     #             'position': position,
#     #             'original': word,
#     #             'corrected': best['term'],
#     #             'distance': best['distance'],
#     #             'pos': best['pos'],
#     #             'category': best['category'],
#     #         })
            
#     #         if self.verbose:
#     #             print(f"  [{position}] '{word}' → '{best['term']}' (distance={best['distance']}, pos={best['pos']})")
#     #             if len(suggestions) > 1:
#     #                 others = [s['term'] for s in suggestions[1:4] if s['term'] != best['term']]
#     #                 if others:
#     #                     print(f"       Other options: {others}")
        
#     #     # Process POS mismatches
#     #     for wd in pos_mismatches:
#     #         word = wd['word']
#     #         position = wd['position']
#     #         predicted_pos = wd['predicted_pos']
#     #         original_pos = wd['pos']
            
#     #         suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
#     #         if not suggestions:
#     #             if self.verbose:
#     #                 print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no suggestions found")
#     #             continue
            
#     #         # compatible = [
#     #         #     s for s in suggestions
#     #         #     if is_pos_compatible(s['pos'], predicted_pos)
#     #         #     and s['term'] != word
#     #         #     and s['distance'] <= 2
#     #         # ]
#     #         compatible = [
#     #             s for s in suggestions
#     #             if is_pos_compatible(s['pos'], predicted_pos)
#     #             and s['term'] != word
#     #             and s['distance'] <= 1
#     #             and s['rank'] > word_rank * 3
#     #         ]
            
#     #         if not compatible:
#     #             if self.verbose:
#     #                 print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no compatible alternative found")
#     #             continue
            
#     #         compatible.sort(key=lambda x: (x['distance'], -x['rank']))
#     #         best = compatible[0]
            
#     #         wd['status'] = 'pos_corrected'
#     #         wd['corrected'] = best['term']
#     #         wd['corrected_display'] = best['display']
#     #         wd['original_pos'] = original_pos
#     #         wd['pos'] = best['pos']
#     #         wd['distance'] = best['distance']
#     #         wd['selected_match'] = {
#     #             'term': best['term'],
#     #             'display': best['display'],
#     #             'category': best['category'],
#     #             'description': best['description'],
#     #             'pos': best['pos'],
#     #             'entity_type': best['entity_type'],
#     #             'rank': best['rank'],
#     #         }
            
#     #         corrections.append({
#     #             'position': position,
#     #             'original': word,
#     #             'corrected': best['term'],
#     #             'distance': best['distance'],
#     #             'pos': best['pos'],
#     #             'category': best['category'],
#     #             'correction_type': 'pos_mismatch',
#     #         })
            
#     #         if self.verbose:
#     #             print(f"  [{position}] '{word}' → '{best['term']}' (POS: {original_pos}→{best['pos']}, distance={best['distance']})")
#     #             if len(compatible) > 1:
#     #                 others = [s['term'] for s in compatible[1:4] if s['term'] != best['term']]
#     #                 if others:
#     #                     print(f"       Other options: {others}")
        
#     #     return corrections

#     def _step5_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

#         unknowns = []
#         pos_mismatches = []

#         for wd in word_data:
#             if wd['status'] == 'unknown':
#                 unknowns.append(wd)
#             elif (
#                 wd['status'] == 'valid'
#                 and not wd['is_stopword']
#                 and wd.get('predicted_pos')
#                 and wd.get('pos')
#                 and wd['predicted_pos'] != wd['pos']
#             ):
#                 predicted_list = wd.get('predicted_pos_list', [])
#                 top_confidence = predicted_list[0][1] if predicted_list else 0
#                 word_rank = wd.get('selected_match', {}).get('rank', 0) or 0
#                 if top_confidence >= 0.90 and word_rank < 200:
#                     pos_mismatches.append(wd)

#         if not unknowns and not pos_mismatches:
#             if self.verbose:
#                 print("\n" + "-" * 70)
#                 print("🔧 STEP 5: Correct Unknowns")
#                 print("-" * 70)
#                 print("  (no unknown words)")
#             return []

#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔧 STEP 5: Correct Unknowns (Redis — TRUE Pipeline Batch)")
#             print("-" * 70)
#             print(f"  Unknowns:       {len(unknowns)}")
#             print(f"  POS mismatches: {len(pos_mismatches)}")

#         all_words_to_correct = [
#             {
#                 'wd': wd,
#                 'word': wd['word'],
#                 'predicted_pos': wd.get('predicted_pos') or 'noun',
#                 'correction_type': 'unknown'
#             }
#             for wd in unknowns
#         ] + [
#             {
#                 'wd': wd,
#                 'word': wd['word'],
#                 'predicted_pos': wd.get('predicted_pos') or 'noun',
#                 'correction_type': 'pos_mismatch',
#                 'word_rank': wd.get('selected_match', {}).get('rank', 0) or 0
#             }
#             for wd in pos_mismatches
#         ]

#         # -------------------------------------------------------------------------
#         # REDIS: All unknowns sent as ONE batch pipeline call
#         # -------------------------------------------------------------------------
#         words_to_fetch = [item['word'] for item in all_words_to_correct]

#         if self.verbose:
#             print(f"  Sending to Redis pipeline: {words_to_fetch}")

#         batch_suggestions = get_fuzzy_suggestions_batch(
#             words_to_fetch, limit=10, max_distance=2
#         )

#         # -------------------------------------------------------------------------
#         # Map results back to word_data
#         # -------------------------------------------------------------------------
#         corrections = []

#         for item in all_words_to_correct:
#             wd = item['wd']
#             word = item['word']
#             position = wd['position']
#             predicted_pos = item['predicted_pos']
#             correction_type = item['correction_type']

#             suggestions = batch_suggestions.get(word.lower().strip(), [])

#             # Mark compatible and save onto wd for debug output
#             for s in suggestions:
#                 s['compatible'] = is_pos_compatible(s['pos'], predicted_pos)
#             wd['redis_suggestions'] = suggestions

#             if not suggestions:
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → No suggestions found")
#                 continue

#             compatible = [s for s in suggestions if s['compatible']]

#             if correction_type == 'pos_mismatch':
#                 word_rank = item.get('word_rank', 0)
#                 compatible = [
#                     s for s in compatible
#                     if s['term'] != word
#                     and s['distance'] <= 1
#                     and s['rank'] > word_rank * 3
#                 ]

#             if compatible:
#                 compatible.sort(key=lambda x: (
#                     x['distance'],
#                     self._pos_match_score(x['pos'], predicted_pos),
#                     -x['rank']
#                 ))
#                 best = compatible[0]
#             else:
#                 best = suggestions[0]

#             if self.verbose:
#                 print(f"  [{position}] '{word}' → '{best['term']}' "
#                     f"(distance={best['distance']}, pos={best['pos']}, rank={best['rank']})")

#             status = 'corrected' if correction_type == 'unknown' else 'pos_corrected'
#             wd['status'] = status
#             wd['corrected'] = best['term']
#             wd['corrected_display'] = best['display']
#             wd['pos'] = best['pos']
#             wd['distance'] = best['distance']
#             wd['selected_match'] = {
#                 'term': best['term'],
#                 'display': best['display'],
#                 'category': best['category'],
#                 'description': best['description'],
#                 'pos': best['pos'],
#                 'entity_type': best['entity_type'],
#                 'rank': best['rank'],
#             }

#             corrections.append({
#                 'position': position,
#                 'original': word,
#                 'corrected': best['term'],
#                 'distance': best['distance'],
#                 'pos': best['pos'],
#                 'category': best['category'],
#                 'correction_type': correction_type,
#             })

#         return corrections


#     def _pos_match_score(self, pos_value: Any, predicted_pos: str) -> int:
#         """
#         Returns 0 if pos_value exactly matches predicted_pos,
#         1 if compatible but not exact.
#         Used to sort Redis suggestions — POS exactness before rank.
#         """
#         if isinstance(pos_value, list):
#             pos_list = pos_value
#         elif isinstance(pos_value, str) and pos_value.startswith('['):
#             import ast
#             try:
#                 pos_list = ast.literal_eval(pos_value)
#             except:
#                 pos_list = [pos_value]
#         else:
#             pos_list = [pos_value]

#         for p in pos_list:
#             if p == predicted_pos:
#                 return 0  # exact match
#         return 1  # compatible but not exact



#     # =========================================================================
#     # STEP 6: Re-check N-grams After Correction (RAM)
#     # =========================================================================
    
#     def _step6_recheck_ngrams(
#         self,
#         corrected_words: List[str],
#         word_data: List[Dict[str, Any]],
#         already_consumed: Set[int]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """Re-check for n-grams using corrected words (RAM cache)."""
#         has_corrections = any(wd['status'] in ('corrected', 'pos_corrected') for wd in word_data)
        
#         if not has_corrections:
#             if self.verbose:
#                 print("\n" + "-" * 70)
#                 print("🔄 STEP 6: Re-check N-grams After Correction")
#                 print("-" * 70)
#                 print("  (no corrections, skipping)")
#             return [], set()
        
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔄 STEP 6: Re-check N-grams After Correction (RAM)")
#             print("-" * 70)
        
#         new_ngrams = []
#         new_consumed = set()
#         words = corrected_words
        
#         # Check trigrams
#         if len(words) >= 3:
#             for i in range(len(words) - 2):
#                 if any(p in already_consumed or p in new_consumed for p in [i, i+1, i+2]):
#                     continue
                
#                 relevant_words = word_data[i:i+3]
#                 if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
#                     continue
                
#                 tri = words[i:i+3]
#                 result = self.cache.get_ngram(tri)
                
#                 if result:
#                     positions = [i+1, i+2, i+3]
#                     new_ngrams.append({
#                         'type': 'trigram',
#                         'positions': positions,
#                         'words': tri,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                         'from_correction': True,
#                     })
#                     new_consumed.update([i, i+1, i+2])
                    
#                     if self.verbose:
#                         print(f"  ✅ NEW TRIGRAM: '{' '.join(tri)}' (after correction)")
        
#         # Check bigrams
#         if len(words) >= 2:
#             for i in range(len(words) - 1):
#                 if any(p in already_consumed or p in new_consumed for p in [i, i+1]):
#                     continue
                
#                 relevant_words = word_data[i:i+2]
#                 if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
#                     continue
                
#                 bi = words[i:i+2]
#                 result = self.cache.get_ngram(bi)
                
#                 if result:
#                     positions = [i+1, i+2]
#                     new_ngrams.append({
#                         'type': 'bigram',
#                         'positions': positions,
#                         'words': bi,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                         'from_correction': True,
#                     })
#                     new_consumed.update([i, i+1])
                    
#                     if self.verbose:
#                         print(f"  ✅ NEW BIGRAM: '{' '.join(bi)}' (after correction)")
        
#         if not new_ngrams and self.verbose:
#             print("  (no new n-grams found)")
        
#         return new_ngrams, new_consumed
    
#     # =========================================================================
#     # STEP 7: Build Final Output
#     # =========================================================================
    
#     def _step7_build_output(
#         self,
#         query: str,
#         word_data: List[Dict[str, Any]],
#         ngrams: List[Dict[str, Any]],
#         consumed_positions: Set[int],
#         corrections: List[Dict[str, Any]],
#         start_time: float
#     ) -> Dict[str, Any]:
#         """Build the final output structure for Typesense."""
#         corrected_words = self._get_working_words(word_data)
#         corrected_query = ' '.join(corrected_words)
        
#         for wd in word_data:
#             pos_index = wd['position'] - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions
        
#         terms = []
#         for wd in word_data:
#             pos_value = wd.get('pos') or wd.get('predicted_pos') or 'unknown'
#             pos_value = normalize_pos_string(pos_value)
#             predicted_pos = wd.get('predicted_pos')
#             if predicted_pos:
#                 predicted_pos = normalize_pos_string(predicted_pos)
            
#             term = {
#                 'position': wd['position'],
#                 'word': wd['word'],
#                 'status': wd['status'],
#                 'is_stopword': wd['is_stopword'],
#                 'part_of_ngram': wd.get('part_of_ngram', False),
#                 'pos': pos_value,
#                 'predicted_pos': predicted_pos,
#             }
            
#             if wd.get('selected_match'):
#                 sm = wd['selected_match']
#                 term['category'] = sm.get('category', '')
#                 term['description'] = sm.get('description', '')
#                 term['display'] = sm.get('display', wd['word'])
#                 term['rank'] = sm.get('rank', 0)
#                 term['entity_type'] = sm.get('entity_type', 'unigram')
#             elif wd.get('all_matches') and len(wd['all_matches']) > 0:
#                 best_match = wd['all_matches'][0]
#                 term['category'] = best_match.get('category', '')
#                 term['description'] = best_match.get('description', '')
#                 term['display'] = best_match.get('display', wd['word'])
#                 term['rank'] = best_match.get('rank', 0)
#                 term['entity_type'] = best_match.get('entity_type', 'unigram')
#             else:
#                 term['category'] = wd.get('category', '')
#                 term['description'] = ''
#                 term['display'] = wd['word']
#                 term['rank'] = 0
#                 term['entity_type'] = 'unigram'
            
#             if wd['status'] in ('corrected', 'pos_corrected'):
#                 term['corrected'] = wd.get('corrected')
#                 term['corrected_display'] = wd.get('corrected_display')
#                 term['distance'] = wd.get('distance')
            
#             term['match_count'] = len(wd.get('all_matches', []))
            
#             terms.append(term)
        
#         elapsed = (time.perf_counter() - start_time) * 1000
        
#         output = {
#             'query': query,
#             'corrected_query': corrected_query,
#             'processing_time_ms': round(elapsed, 2),
#             'terms': terms,
#             'ngrams': ngrams,
#             'corrections': corrections,
#             'stats': {
#                 'total_words': len(word_data),
#                 'valid_words': sum(1 for wd in word_data if wd['status'] == 'valid'),
#                 'corrected_words': sum(1 for wd in word_data if wd['status'] == 'corrected'),
#                 'unknown_words': sum(1 for wd in word_data if wd['status'] == 'unknown'),
#                 'stopwords': sum(1 for wd in word_data if wd['is_stopword']),
#                 'ngram_count': len(ngrams),
#             }
#         }
        
#         if self.verbose:
#             print("\n" + "=" * 70)
#             print("📊 FINAL OUTPUT")
#             print("=" * 70)
#             print(f"  Query: '{query}'")
#             print(f"  Corrected: '{corrected_query}'")
#             print(f"  Time: {elapsed:.2f}ms")
#             print(f"  Stats: {output['stats']}")
#             print(f"  Source: {'RAM cache' if RAM_CACHE_AVAILABLE else 'Redis fallback'}")
        
#         return output
    
#     # =========================================================================
#     # Helper Methods
#     # =========================================================================
    
#     def _get_working_words(self, word_data: List[Dict[str, Any]]) -> List[str]:
#         """Get the working word list (using corrected words where available)."""
#         words = []
#         for wd in word_data:
#             if wd['status'] in ('corrected', 'pos_corrected'):
#                 words.append(wd['corrected'])
#             else:
#                 words.append(wd['word'])
#         return words
    
#     def _empty_result(self, query: str) -> Dict[str, Any]:
#         """Return empty result structure."""
#         return {
#             'query': query or '',
#             'corrected_query': '',
#             'processing_time_ms': 0,
#             'terms': [],
#             'ngrams': [],
#             'corrections': [],
#             'stats': {
#                 'total_words': 0,
#                 'valid_words': 0,
#                 'corrected_words': 0,
#                 'unknown_words': 0,
#                 'stopwords': 0,
#                 'ngram_count': 0,
#             }
#         }
    
#     def _process_single_word(self, word: str, start_time: float) -> Dict[str, Any]:
#         """Handle single word queries."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("📖 Single Word Query")
#             print("-" * 70)
        
#         word_lower = word.lower().strip()
        
#         # Check stopword
#         if word_lower in STOPWORDS:
#             elapsed = (time.perf_counter() - start_time) * 1000
#             return {
#                 'query': word,
#                 'corrected_query': word_lower,
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': True,
#                     'part_of_ngram': False,
#                     'pos': STOPWORDS[word_lower],
#                     'category': 'stopword',
#                     'description': '',
#                     'display': word_lower,
#                     'rank': 0,
#                     'match_count': 0,
#                 }],
#                 'ngrams': [],
#                 'corrections': [],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 1,
#                     'corrected_words': 0,
#                     'unknown_words': 0,
#                     'stopwords': 1,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Get matches from RAM cache
#         matches = self.cache.get_term_matches(word_lower)
        
#         if matches:
#             best = matches[0]
#             elapsed = (time.perf_counter() - start_time) * 1000
            
#             if self.verbose:
#                 print(f"  '{word_lower}' → Found ({len(matches)} matches)")
#                 print(f"  Selected: {best['category']} (rank={best['rank']})")
            
#             return {
#                 'query': word,
#                 'corrected_query': word_lower,
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': False,
#                     'part_of_ngram': False,
#                     'pos': best['pos'],
#                     'category': best['category'],
#                     'description': best['description'],
#                     'display': best['display'],
#                     'rank': best['rank'],
#                     'entity_type': best['entity_type'],
#                     'match_count': len(matches),
#                 }],
#                 'ngrams': [],
#                 'corrections': [],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 1,
#                     'corrected_words': 0,
#                     'unknown_words': 0,
#                     'stopwords': 0,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Unknown - try Redis fuzzy correction
#         suggestions = get_fuzzy_suggestions(word_lower, limit=5, max_distance=2)
        
#         if suggestions:
#             best = suggestions[0]
#             elapsed = (time.perf_counter() - start_time) * 1000
            
#             if self.verbose:
#                 print(f"  '{word_lower}' → Corrected to '{best['term']}' (distance={best['distance']})")
            
#             return {
#                 'query': word,
#                 'corrected_query': best['term'],
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'corrected',
#                     'is_stopword': False,
#                     'part_of_ngram': False,
#                     'pos': best['pos'],
#                     'predicted_pos': 'noun',
#                     'category': best['category'],
#                     'description': best['description'],
#                     'display': best['display'],
#                     'corrected': best['term'],
#                     'corrected_display': best['display'],
#                     'distance': best['distance'],
#                     'rank': best['rank'],
#                     'entity_type': best['entity_type'],
#                     'match_count': 0,
#                 }],
#                 'ngrams': [],
#                 'corrections': [{
#                     'position': 1,
#                     'original': word_lower,
#                     'corrected': best['term'],
#                     'distance': best['distance'],
#                     'pos': best['pos'],
#                     'category': best['category'],
#                 }],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 0,
#                     'corrected_words': 1,
#                     'unknown_words': 0,
#                     'stopwords': 0,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Truly unknown
#         elapsed = (time.perf_counter() - start_time) * 1000
        
#         if self.verbose:
#             print(f"  '{word_lower}' → Unknown (no suggestions)")
        
#         return {
#             'query': word,
#             'corrected_query': word_lower,
#             'processing_time_ms': round(elapsed, 2),
#             'terms': [{
#                 'position': 1,
#                 'word': word_lower,
#                 'status': 'unknown',
#                 'is_stopword': False,
#                 'part_of_ngram': False,
#                 'pos': 'unknown',
#                 'predicted_pos': 'noun',
#                 'category': '',
#                 'description': '',
#                 'display': word_lower,
#                 'rank': 0,
#                 'match_count': 0,
#             }],
#             'ngrams': [],
#             'corrections': [],
#             'stats': {
#                 'total_words': 1,
#                 'valid_words': 0,
#                 'corrected_words': 0,
#                 'unknown_words': 1,
#                 'stopwords': 0,
#                 'ngram_count': 0,
#             }
#         }


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def process_query(query: str, verbose: bool = False) -> Dict[str, Any]:
#     """Main entry point - process a query through word discovery."""
#     wd = WordDiscovery(verbose=verbose)
#     return wd.process(query)


# def print_output(output: Dict[str, Any]) -> None:
#     """Print the output in a readable JSON format."""
#     print("\n" + "=" * 70)
#     print("📄 COMPLETE OUTPUT (JSON)")
#     print("=" * 70)
#     print(json.dumps(output, indent=2))


# # =============================================================================
# # MAIN - TEST SCRIPT
# # =============================================================================

# def main():
#     """Run test queries."""
#     import sys
    
#     test_queries = [
#         "where is africa located",
#         "where is african located",
#         "african food near me",
#         "when was slavery abolished",
#         "where is quickly located"
#     ]
    
#     if len(sys.argv) > 1:
#         query = ' '.join(sys.argv[1:])
#         test_queries = [query]
    
#     print("\n" + "#" * 70)
#     print("# WORD DISCOVERY V2 - TEST")
#     print(f"# RAM Cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
#     print("#" * 70)
    
#     for query in test_queries:
#         output = process_query(query, verbose=True)
#         print_output(output)
#         print("\n" + "=" * 70 + "\n")


# if __name__ == "__main__":
#     main()





# PART 2
# """
# word_discovery_fulltest.py
# ====================
# Optimized word discovery with POS-aware selection and n-gram detection.

# NOW USES RAM CACHE (vocabulary_cache.py) for Steps 1-4, 6-7.
# Only Step 5 (fuzzy spelling correction) hits Redis.

# STRATEGY:
#     Step 1: Collect ALL data in one pass (RAM)
#             - All word matches (don't pick winner yet)
#             - All bigrams, trigrams, quadgrams
    
#     Step 2: Mark n-gram positions (longest first)
#             - Quadgrams → Trigrams → Bigrams
#             - Consumed positions are marked
    
#     Step 3: Determine POS for remaining words
#             - Use grammar rules based on neighbors
#             - Unknown neighbors → default to noun
    
#     Step 4: Select best match (POS + Rank)
#             - Filter by predicted POS
#             - Then sort by rank
#             - Word Discovery is the brain - Typesense receives the decision
    
#     Step 5: Handle unknowns (spelling correction)
#             - Use predicted POS to filter corrections
#             - Sort by distance, then rank
#             - Preserve original position
#             - THIS IS THE ONLY STEP THAT USES REDIS
#             - IMPORTANT: words not in RAM hash keep their original form
#               for the search. Fuzzy suggestions surface as hints only
#               (correction_type='suggestion') so the semantic path can
#               still find results via embedding + vector search.
    
#     Step 6: Re-check n-grams after correction (RAM)
#             - Corrected words may form new n-grams
    
#     Step 7: Build final output for Typesense

# PERFORMANCE:
#     - Steps 1-4, 6-7: RAM only (~0.01ms per lookup)
#     - Step 5: Redis only for unknown words (~50ms per correction)
#     - Typical query (no typos): ~1-5ms
#     - Query with 1-2 typos: ~50-150ms

# WORD STATUS GUIDE:
#     'valid'          - found in RAM hash, selected match confirmed
#     'corrected'      - was NOT in RAM hash but IS a genuine typo;
#                        word has been replaced for the search
#                        NOTE: currently unused — see patch note in Step 5
#     'pos_corrected'  - found in RAM hash but wrong POS; replaced with
#                        a better-fitting word (genuine typo path)
#     'unknown'        - not in RAM hash, no fuzzy suggestion found;
#                        original word passed through as-is
#     'unknown_suggest'- not in RAM hash, fuzzy suggestion available;
#                        original word KEPT for search, suggestion shown
#                        as "did you mean?" hint only
# """

# import json
# import time
# import redis
# from typing import Dict, Any, List, Optional, Tuple, Set
# from decouple import config



# STOPWORD_POS = {
#     'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
#     'this': 'determiner', 'that': 'determiner',
#     'these': 'determiner', 'those': 'determiner',
#     'some': 'determiner', 'any': 'determiner',
#     'each': 'determiner', 'every': 'determiner',
#     'all': 'determiner', 'both': 'determiner',
#     'no': 'determiner', 'few': 'determiner',
#     'more': 'determiner', 'most': 'determiner',
#     'other': 'determiner', 'such': 'determiner',
#     'own': 'determiner', 'my': 'determiner', 'your': 'determiner',
#     'his': 'determiner', 'its': 'determiner', 'our': 'determiner',
#     'their': 'determiner', 'her': 'determiner',
#     'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#     'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#     'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
#     'into': 'preposition', 'through': 'preposition',
#     'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
#     'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
#     'above': 'preposition', 'below': 'preposition',
#     'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#     'so': 'conjunction', 'than': 'conjunction',
#     'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#     'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#     'me': 'pronoun', 'him': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
#     'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
#     'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#     'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
#     'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#     'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#     'will': 'modal', 'would': 'modal', 'could': 'modal',
#     'should': 'modal', 'may': 'modal', 'might': 'modal',
#     'must': 'modal', 'can': 'modal',
#     'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
#     'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
#     'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
#     'only': 'adverb', 'not': 'adverb', 'same': 'adverb', 'once': 'adverb',
#     'so': 'adverb',
# }

# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION')
# REDIS_PORT = config('REDIS_PORT', cast=int)
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
# REDIS_USERNAME = config('REDIS_USERNAME', default='')


# # =============================================================================
# # REDIS CONNECTION (Only for spelling correction - Step 5)
# # =============================================================================

# class RedisClient:
#     """Redis client for spelling correction only."""
    
#     _client: Optional[redis.Redis] = None
    
#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None
        
#         try:
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
# # RAM CACHE - Uses vocabulary_cache.py singleton
# # =============================================================================

# try:
#     from .vocabulary_cache import vocab_cache
#     RAM_CACHE_AVAILABLE = True
#     print("✅ vocab_cache imported from .vocabulary_cache (RAM)")
# except ImportError:
#     try:
#         from vocabulary_cache import vocab_cache
#         RAM_CACHE_AVAILABLE = True
#         print("✅ vocab_cache imported from vocabulary_cache (RAM)")
#     except ImportError:
#         RAM_CACHE_AVAILABLE = False
#         vocab_cache = None
#         print("⚠️ vocabulary_cache not available — falling back to Redis for all lookups")


# class VocabCache:
#     """
#     RAM-based vocabulary cache wrapper.
    
#     Uses vocab_cache singleton from vocabulary_cache.py for O(1) lookups.
#     Falls back to Redis if RAM cache is not available.
#     """
    
#     def __init__(self):
#         self._ram = vocab_cache if RAM_CACHE_AVAILABLE else None
#         self._redis_client = None
    
#     def _get_redis_client(self) -> Optional[redis.Redis]:
#         """Get Redis client (fallback only)."""
#         if self._redis_client is None:
#             self._redis_client = RedisClient.get_client()
#         return self._redis_client
    
#     def get_term_matches(self, word: str) -> List[Dict[str, Any]]:
#         """
#         Get ALL matches for a word from RAM cache.
#         Returns list of all category variants, sorted by rank desc.
#         """
#         # === RAM PATH (fast) ===
#         if self._ram and self._ram.loaded:
#             matches = self._ram.get_all_term_matches(word.lower().strip())
#             if not matches:
#                 return []
            
#             normalized = []
#             for m in matches:
#                 rank = self._parse_rank(m.get('rank', 0))
#                 normalized.append({
#                     'term': m.get('term', word.lower()),
#                     'display': m.get('display', word.lower()),
#                     'category': m.get('category', ''),
#                     'description': m.get('description', ''),
#                     'pos': self._normalize_pos(m.get('pos')),
#                     'entity_type': m.get('entity_type', 'unigram'),
#                     'rank': rank,
#                 })
            
#             normalized.sort(key=lambda x: x['rank'], reverse=True)
#             return normalized
        
#         # === REDIS FALLBACK (slow) ===
#         client = self._get_redis_client()
#         if not client:
#             return []
        
#         word_lower = word.lower().strip()
#         pattern = f"term:{word_lower}:*"
        
#         try:
#             keys = client.keys(pattern)
#             if not keys:
#                 return []
            
#             matches = []
#             for key in keys:
#                 metadata = client.hgetall(key)
#                 if metadata:
#                     rank = self._parse_rank(metadata.get('rank', 0))
#                     matches.append({
#                         'term': metadata.get('term', word_lower),
#                         'display': metadata.get('display', word_lower),
#                         'category': metadata.get('category', ''),
#                         'description': metadata.get('description', ''),
#                         'pos': self._normalize_pos(metadata.get('pos')),
#                         'entity_type': metadata.get('entity_type', 'unigram'),
#                         'rank': rank,
#                     })
            
#             matches.sort(key=lambda x: x['rank'], reverse=True)
#             return matches
            
#         except Exception as e:
#             print(f"Error getting term matches for '{word}': {e}")
#             return []
    
#     def get_ngram(self, words: List[str]) -> Optional[Dict[str, Any]]:
#         """
#         Check if words form an n-gram (bigram, trigram, quadgram).
#         Uses RAM cache first, falls back to Redis.
#         """
#         if len(words) < 2:
#             return None
        
#         # === RAM PATH (fast) ===
#         if self._ram and self._ram.loaded:
#             words_lower = [w.lower() for w in words]
#             metadata = None
            
#             if len(words_lower) == 2:
#                 metadata = self._ram.get_bigram(words_lower[0], words_lower[1])
#             elif len(words_lower) == 3:
#                 metadata = self._ram.get_trigram(words_lower[0], words_lower[1], words_lower[2])
#             elif len(words_lower) == 4:
#                 metadata = self._ram.get_quadgram(words_lower[0], words_lower[1], words_lower[2], words_lower[3])
            
#             if not metadata:
#                 return None
            
#             phrase = ' '.join(words_lower)
#             ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'
            
#             return {
#                 'key': f"term:{phrase}:{metadata.get('category', '')}",
#                 'term': metadata.get('term', phrase),
#                 'display': metadata.get('display', phrase.title()),
#                 'category': metadata.get('category', ''),
#                 'description': metadata.get('description', ''),
#                 'pos': self._normalize_pos(metadata.get('pos')),
#                 'entity_type': metadata.get('entity_type', ngram_type),
#                 'rank': self._parse_rank(metadata.get('rank', 0)),
#                 'words': words,
#                 'ngram_type': ngram_type,
#             }
        
#         # === REDIS FALLBACK (slow) ===
#         client = self._get_redis_client()
#         if not client:
#             return None
        
#         phrase = ' '.join(w.lower() for w in words)
#         phrase_underscore = '_'.join(w.lower() for w in words)

#         pattern = f"term:{phrase_underscore}:*"
#         try:
#             keys = client.keys(pattern)
#         except Exception:
#             keys = []

#         if not keys:
#             pattern = f"term:{phrase}:*"
#             try:
#                 keys = client.keys(pattern)
#             except Exception:
#                 keys = []
        
#         if not keys:
#             return None
        
#         try:
#             key = keys[0]
#             metadata = client.hgetall(key)
            
#             if metadata:
#                 rank = self._parse_rank(metadata.get('rank', 0))
#                 ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'
                
#                 return {
#                     'key': key,
#                     'term': metadata.get('term', phrase),
#                     'display': metadata.get('display', phrase.title()),
#                     'category': metadata.get('category', ''),
#                     'description': metadata.get('description', ''),
#                     'pos': self._normalize_pos(metadata.get('pos')),
#                     'entity_type': metadata.get('entity_type', ngram_type),
#                     'rank': rank,
#                     'words': words,
#                     'ngram_type': ngram_type,
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error getting ngram for '{phrase}': {e}")
#             return None
    
#     def _normalize_pos(self, pos_value: Any) -> str:
#         """Normalize POS value from various formats."""
#         if pos_value is None:
#             return 'unknown'
        
#         if isinstance(pos_value, str):
#             if pos_value.startswith('['):
#                 try:
#                     parsed = json.loads(pos_value)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 try:
#                     fixed = pos_value.replace("'", '"')
#                     parsed = json.loads(fixed)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 if pos_value.startswith("['") and pos_value.endswith("']"):
#                     inner = pos_value[2:-2]
#                     return inner.lower()
            
#             return pos_value.lower()
        
#         if isinstance(pos_value, list):
#             return str(pos_value[0]).lower() if pos_value else 'unknown'
        
#         return str(pos_value).lower()
    
#     def _parse_rank(self, rank_value: Any) -> int:
#         """Parse rank to integer."""
#         try:
#             return int(float(rank_value))
#         except (ValueError, TypeError):
#             return 0


# # Global cache instance
# vocab_cache_wrapper = VocabCache()


# # =============================================================================
# # STOPWORDS
# # =============================================================================

# STOPWORDS = {
#     # Determiners/Articles
#     "the": "determiner", "a": "article", "an": "article",
#     "this": "determiner", "that": "determiner", "these": "determiner", "those": "determiner",
#     "my": "determiner", "your": "determiner", "his": "determiner", "her": "determiner",
#     "its": "determiner", "our": "determiner", "their": "determiner",
#     "some": "determiner", "any": "determiner", "no": "determiner",
#     "every": "determiner", "each": "determiner", "all": "determiner",
    
#     # Prepositions
#     "in": "preposition", "on": "preposition", "at": "preposition", "to": "preposition",
#     "for": "preposition", "of": "preposition", "with": "preposition", "by": "preposition",
#     "from": "preposition", "about": "preposition", "into": "preposition", "through": "preposition",
#     "during": "preposition", "before": "preposition", "after": "preposition",
#     "above": "preposition", "below": "preposition", "between": "preposition",
#     "under": "preposition", "over": "preposition", "near": "preposition",
    
#     # Conjunctions
#     "and": "conjunction", "or": "conjunction", "but": "conjunction",
#     "nor": "conjunction", "so": "conjunction", "yet": "conjunction",
    
#     # Pronouns
#     "i": "pronoun", "you": "pronoun", "he": "pronoun", "she": "pronoun",
#     "it": "pronoun", "we": "pronoun", "they": "pronoun",
#     "me": "pronoun", "him": "pronoun", "her": "pronoun", "us": "pronoun", "them": "pronoun",
#     "who": "pronoun", "whom": "pronoun", "what": "pronoun", "which": "pronoun",
#     "whose": "pronoun", "whoever": "pronoun", "whatever": "pronoun",
    
#     # Be verbs
#     "is": "be", "are": "be", "was": "be", "were": "be",
#     "be": "be", "been": "be", "being": "be",
#     "am": "be",
    
#     # Auxiliary/Modal verbs
#     "have": "auxiliary", "has": "auxiliary", "had": "auxiliary",
#     "do": "auxiliary", "does": "auxiliary", "did": "auxiliary",
#     "will": "modal", "would": "modal", "could": "modal", "should": "modal",
#     "may": "modal", "might": "modal", "must": "modal", "can": "modal",
    
#     # Other common words
#     "not": "negation", "no": "negation",
#     "as": "conjunction", "if": "conjunction", "when": "conjunction",
#     "than": "conjunction", "because": "conjunction", "while": "conjunction",
#     "where": "adverb", "how": "adverb", "why": "adverb",
#     "very": "adverb", "just": "adverb", "also": "adverb",
#     "only": "adverb", "even": "adverb", "still": "adverb",
#     "then": "adverb", "now": "adverb", "here": "adverb", "there": "adverb",
# }


# # =============================================================================
# # GRAMMAR RULES FOR POS PREDICTION
# # =============================================================================

# GRAMMAR_RULES = {
#     # =========================================================================
#     # BOTH NEIGHBORS KNOWN
#     # =========================================================================
    
#     ("determiner", "noun"): [("adjective", 0.95), ("noun", 0.60)],
#     ("determiner", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("determiner", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("determiner", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("determiner", "conjunction"): [("noun", 0.85)],
#     ("determiner", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     ("article", "noun"): [("adjective", 0.95), ("noun", 0.55)],
#     ("article", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("article", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("article", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("article", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("article", "conjunction"): [("noun", 0.85)],
#     ("article", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     ("adjective", "noun"): [("adjective", 0.90), ("noun", 0.50)],
#     ("adjective", "adjective"): [("adjective", 0.85), ("noun", 0.65), ("adverb", 0.50)],
#     ("adjective", "verb"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "preposition"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "adverb"): [("noun", 0.85), ("verb", 0.65)],
#     ("adjective", "conjunction"): [("noun", 0.90)],
#     ("adjective", "end"): [("noun", 0.95), ("proper_noun", 0.80)],
    
#     ("noun", "noun"): [("verb", 0.75), ("adjective", 0.65), ("noun", 0.50)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.80), ("adverb", 0.55)],
#     ("noun", "adverb"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "preposition"): [("verb", 0.85), ("noun", 0.55)],
#     ("noun", "verb"): [("adverb", 0.85), ("noun", 0.60)],
#     ("noun", "conjunction"): [("verb", 0.80), ("noun", 0.60)],
#     ("noun", "determiner"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "article"): [("verb", 0.90), ("be", 0.70)],
#     ("noun", "pronoun"): [("verb", 0.90)],
#     ("noun", "end"): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],
    
#     ("proper_noun", "noun"): [("verb", 0.80), ("noun", 0.60)],
#     ("proper_noun", "adjective"): [("verb", 0.90), ("be", 0.80)],
#     ("proper_noun", "adverb"): [("verb", 0.90)],
#     ("proper_noun", "preposition"): [("verb", 0.85)],
#     ("proper_noun", "verb"): [("adverb", 0.85), ("proper_noun", 0.60)],
#     ("proper_noun", "conjunction"): [("verb", 0.80)],
#     ("proper_noun", "end"): [("verb", 0.75), ("proper_noun", 0.70)],
    
#     ("verb", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("adverb", 0.60)],
#     ("verb", "adjective"): [("adverb", 0.90), ("noun", 0.55)],
#     ("verb", "adverb"): [("adverb", 0.85), ("noun", 0.60)],
#     ("verb", "verb"): [("adverb", 0.85), ("noun", 0.60), ("preposition", 0.50)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.75), ("pronoun", 0.55)],
#     ("verb", "conjunction"): [("noun", 0.85), ("adverb", 0.65)],
#     ("verb", "determiner"): [("adverb", 0.80), ("noun", 0.65)],
#     ("verb", "article"): [("adverb", 0.80), ("noun", 0.65)],
#     ("verb", "pronoun"): [("adverb", 0.80), ("preposition", 0.65)],
#     ("verb", "end"): [("noun", 0.85), ("adverb", 0.75), ("proper_noun", 0.65)],
    
#     ("preposition", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
#     ("preposition", "adjective"): [("adverb", 0.90), ("adjective", 0.65)],
#     ("preposition", "adverb"): [("adjective", 0.80), ("noun", 0.70)],
#     ("preposition", "verb"): [("noun", 0.90), ("proper_noun", 0.75), ("pronoun", 0.60)],
#     ("preposition", "preposition"): [("noun", 0.85), ("proper_noun", 0.75)],
#     ("preposition", "conjunction"): [("noun", 0.85), ("proper_noun", 0.70)],
#     ("preposition", "determiner"): [("noun", 0.80), ("adjective", 0.65)],
#     ("preposition", "article"): [("noun", 0.80), ("adjective", 0.65)],
#     ("preposition", "pronoun"): [("noun", 0.75), ("verb", 0.60)],
#     ("preposition", "end"): [("noun", 0.95), ("proper_noun", 0.85), ("adjective", 0.60)],
    
#     ("pronoun", "noun"): [("verb", 0.95), ("be", 0.80)],
#     ("pronoun", "adjective"): [("verb", 0.95), ("be", 0.85), ("adverb", 0.55)],
#     ("pronoun", "adverb"): [("verb", 0.95), ("be", 0.75)],
#     ("pronoun", "preposition"): [("verb", 0.90), ("be", 0.70)],
#     ("pronoun", "verb"): [("adverb", 0.85), ("modal", 0.70)],
#     ("pronoun", "conjunction"): [("verb", 0.85)],
#     ("pronoun", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     ("be", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
#     ("be", "adjective"): [("adverb", 0.95), ("adjective", 0.60)],
#     ("be", "adverb"): [("adjective", 0.85), ("verb", 0.70)],
#     ("be", "verb"): [("adverb", 0.90), ("noun", 0.55)],
#     ("be", "preposition"): [("noun", 0.90), ("adverb", 0.70)],
#     ("be", "conjunction"): [("adjective", 0.80), ("noun", 0.65)],
#     ("be", "end"): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.60)],
    
#     ("adverb", "noun"): [("adjective", 0.90), ("verb", 0.65)],
#     ("adverb", "adjective"): [("adverb", 0.85), ("adjective", 0.70)],
#     ("adverb", "adverb"): [("adverb", 0.80), ("verb", 0.65)],
#     ("adverb", "verb"): [("adverb", 0.90), ("noun", 0.50)],
#     ("adverb", "preposition"): [("verb", 0.85), ("noun", 0.65)],
#     ("adverb", "conjunction"): [("verb", 0.80), ("adjective", 0.65)],
#     ("adverb", "end"): [("adjective", 0.85), ("verb", 0.75), ("noun", 0.60)],
    
#     ("conjunction", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("verb", 0.55)],
#     ("conjunction", "adjective"): [("adverb", 0.85), ("noun", 0.65)],
#     ("conjunction", "adverb"): [("noun", 0.80), ("verb", 0.70)],
#     ("conjunction", "verb"): [("noun", 0.90), ("pronoun", 0.75)],
#     ("conjunction", "preposition"): [("noun", 0.85), ("pronoun", 0.70)],
#     ("conjunction", "conjunction"): [("noun", 0.80)],
#     ("conjunction", "end"): [("noun", 0.90), ("proper_noun", 0.75)],
    
#     ("modal", "noun"): [("verb", 0.90), ("adverb", 0.65)],
#     ("modal", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("modal", "adverb"): [("verb", 0.95)],
#     ("modal", "verb"): [("adverb", 0.90)],
#     ("modal", "preposition"): [("verb", 0.85)],
#     ("modal", "end"): [("verb", 0.95), ("noun", 0.55)],
    
#     ("auxiliary", "noun"): [("verb", 0.90), ("adjective", 0.65)],
#     ("auxiliary", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("auxiliary", "adverb"): [("verb", 0.95)],
#     ("auxiliary", "verb"): [("adverb", 0.90)],
#     ("auxiliary", "preposition"): [("verb", 0.85)],
#     ("auxiliary", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     ("participle", "noun"): [("adjective", 0.85), ("noun", 0.65)],
#     ("participle", "adjective"): [("adverb", 0.85), ("noun", 0.60)],
#     ("participle", "adverb"): [("noun", 0.80), ("adjective", 0.65)],
#     ("participle", "verb"): [("adverb", 0.80), ("noun", 0.65)],
#     ("participle", "preposition"): [("noun", 0.85), ("adverb", 0.65)],
#     ("participle", "end"): [("noun", 0.90), ("adverb", 0.70)],
    
#     ("gerund", "noun"): [("adjective", 0.85), ("noun", 0.70)],
#     ("gerund", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
#     ("gerund", "adverb"): [("noun", 0.80)],
#     ("gerund", "verb"): [("adverb", 0.80), ("noun", 0.60)],
#     ("gerund", "preposition"): [("noun", 0.85)],
#     ("gerund", "end"): [("noun", 0.90), ("adverb", 0.65)],
    
#     ("negation", "noun"): [("adjective", 0.85), ("verb", 0.75)],
#     ("negation", "adjective"): [("adverb", 0.90)],
#     ("negation", "adverb"): [("verb", 0.85), ("adjective", 0.70)],
#     ("negation", "verb"): [("adverb", 0.90)],
#     ("negation", "preposition"): [("verb", 0.80)],
#     ("negation", "end"): [("verb", 0.85), ("adjective", 0.70)],
    
#     ("quantifier", "noun"): [("adjective", 0.90), ("noun", 0.60)],
#     ("quantifier", "adjective"): [("adverb", 0.85), ("adjective", 0.65)],
#     ("quantifier", "adverb"): [("adjective", 0.80)],
#     ("quantifier", "verb"): [("noun", 0.85)],
#     ("quantifier", "preposition"): [("noun", 0.85)],
#     ("quantifier", "end"): [("noun", 0.95), ("adjective", 0.65)],
    
#     ("numeral", "noun"): [("adjective", 0.85), ("noun", 0.70)],
#     ("numeral", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
#     ("numeral", "adverb"): [("noun", 0.80)],
#     ("numeral", "verb"): [("noun", 0.85)],
#     ("numeral", "preposition"): [("noun", 0.85)],
#     ("numeral", "end"): [("noun", 0.95)],
    
#     # =========================================================================
#     # ONLY LEFT NEIGHBOR KNOWN
#     # =========================================================================
    
#     ("determiner", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
#     ("article", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
#     ("adjective", None): [("noun", 0.95), ("proper_noun", 0.75)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],
#     ("proper_noun", None): [("verb", 0.80), ("proper_noun", 0.70), ("noun", 0.55)],
#     ("verb", None): [("noun", 0.80), ("adverb", 0.75), ("adjective", 0.60), ("proper_noun", 0.50)],
#     ("preposition", None): [("noun", 0.90), ("proper_noun", 0.85), ("adjective", 0.60)],
#     ("pronoun", None): [("verb", 0.95), ("be", 0.80), ("modal", 0.65)],
#     ("be", None): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.65), ("verb", 0.55)],
#     ("adverb", None): [("adjective", 0.85), ("verb", 0.75), ("adverb", 0.60)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.75), ("proper_noun", 0.65), ("verb", 0.55)],
#     ("modal", None): [("verb", 0.95), ("adverb", 0.65)],
#     ("auxiliary", None): [("verb", 0.95), ("adverb", 0.65)],
#     ("participle", None): [("noun", 0.85), ("adverb", 0.70)],
#     ("gerund", None): [("noun", 0.90), ("adverb", 0.60)],
#     ("negation", None): [("verb", 0.85), ("adjective", 0.75), ("adverb", 0.60)],
#     ("quantifier", None): [("noun", 0.95), ("adjective", 0.70)],
#     ("numeral", None): [("noun", 0.95), ("adjective", 0.60)],
    
#     # =========================================================================
#     # ONLY RIGHT NEIGHBOR KNOWN
#     # =========================================================================
    
#     (None, "noun"): [("adjective", 0.95), ("determiner", 0.85), ("noun", 0.60), ("proper_noun", 0.50)],
#     (None, "proper_noun"): [("adjective", 0.80), ("preposition", 0.75), ("verb", 0.65)],
#     (None, "adjective"): [("adverb", 0.90), ("adjective", 0.70), ("determiner", 0.60)],
#     (None, "adverb"): [("verb", 0.85), ("adverb", 0.75), ("be", 0.60)],
#     (None, "verb"): [("noun", 0.90), ("pronoun", 0.85), ("adverb", 0.70), ("proper_noun", 0.60)],
#     (None, "preposition"): [("noun", 0.90), ("verb", 0.80), ("proper_noun", 0.65)],
#     (None, "determiner"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
#     (None, "article"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
#     (None, "pronoun"): [("verb", 0.85), ("preposition", 0.75), ("conjunction", 0.65)],
#     (None, "conjunction"): [("noun", 0.90), ("verb", 0.75), ("adjective", 0.60)],
#     (None, "be"): [("noun", 0.90), ("pronoun", 0.85), ("proper_noun", 0.70)],
#     (None, "modal"): [("noun", 0.90), ("pronoun", 0.85)],
#     (None, "auxiliary"): [("noun", 0.90), ("pronoun", 0.85)],
#     (None, "participle"): [("be", 0.85), ("adverb", 0.75), ("noun", 0.60)],
#     (None, "gerund"): [("preposition", 0.80), ("verb", 0.75), ("be", 0.65)],
#     (None, "negation"): [("verb", 0.85), ("be", 0.80), ("modal", 0.70)],
#     (None, "quantifier"): [("preposition", 0.80), ("verb", 0.70)],
#     (None, "numeral"): [("preposition", 0.80), ("verb", 0.70), ("determiner", 0.60)],
#     (None, "end"): [("noun", 0.90), ("proper_noun", 0.80), ("verb", 0.65), ("adjective", 0.55)],
    
#     # =========================================================================
#     # SPECIAL CASES - Start of query
#     # =========================================================================
    
#     ("start", "noun"): [("adjective", 0.90), ("determiner", 0.85)],
#     ("start", "adjective"): [("adverb", 0.85), ("determiner", 0.75)],
#     ("start", "verb"): [("noun", 0.90), ("pronoun", 0.80)],
#     ("start", "adverb"): [("verb", 0.80), ("adjective", 0.70)],
#     ("start", "preposition"): [("noun", 0.85), ("verb", 0.70)],
#     ("start", "end"): [("noun", 0.90), ("proper_noun", 0.85), ("verb", 0.60)],
# }


# # POS compatibility for matching predictions to candidates
# POS_COMPATIBILITY = {
#     'noun': {'noun', 'proper_noun'},
#     'proper_noun': {'noun', 'proper_noun'},
#     'verb': {'verb', 'participle', 'gerund'},
#     'adjective': {'adjective', 'participle'},
#     'adverb': {'adverb'},
#     'pronoun': {'pronoun'},
#     'determiner': {'determiner', 'article'},
#     'article': {'article', 'determiner'},
#     'preposition': {'preposition'},
#     'conjunction': {'conjunction'},
# }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """Calculate Damerau-Levenshtein distance."""
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


# def is_pos_compatible(candidate_pos: str, predicted_pos: str) -> bool:
#     """Check if candidate POS is compatible with predicted POS."""
#     candidate_pos = normalize_pos_string(candidate_pos)
#     predicted_pos = normalize_pos_string(predicted_pos)
    
#     if candidate_pos == predicted_pos:
#         return True
    
#     compatible_set = POS_COMPATIBILITY.get(predicted_pos, {predicted_pos})
#     return candidate_pos in compatible_set


# def normalize_pos_string(pos_value: any) -> str:
#     """Normalize POS value from various formats to clean string."""
#     if pos_value is None:
#         return 'unknown'
    
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 parsed = json.loads(pos_value)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             try:
#                 fixed = pos_value.replace("'", '"')
#                 parsed = json.loads(fixed)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             if pos_value.startswith("['") and pos_value.endswith("']"):
#                 inner = pos_value[2:-2]
#                 return inner.lower()
        
#         return pos_value.lower().strip()
    
#     if isinstance(pos_value, list):
#         return str(pos_value[0]).lower() if pos_value else 'unknown'
    
#     return str(pos_value).lower()


# def get_fuzzy_suggestions(word: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
#     """
#     Get spelling suggestions from Redis using fuzzy search.
#     This is the ONLY Redis call in the pipeline (for unknown words).
#     """
#     client = RedisClient.get_client()
#     if not client:
#         return []
    
#     word_lower = word.lower().strip()
#     if len(word_lower) < 2:
#         return []
    
#     try:
#         suggestions = []
        
#         for fuzzy_level in ['%', '%%']:
#             if fuzzy_level == '%%' and suggestions:
#                 break
            
#             query = f"{fuzzy_level}{word_lower}{fuzzy_level}"
            
#             try:
#                 result = client.execute_command(
#                     'FT.SEARCH', 'terms_idx', query,
#                     'SORTBY', 'rank', 'DESC',
#                     'LIMIT', '0', str(limit * 3)
#                 )
                
#                 if result and len(result) > 1:
#                     i = 1
#                     while i < len(result):
#                         key = result[i]
#                         fields = result[i + 1] if i + 1 < len(result) else []
                        
#                         metadata = {}
#                         for j in range(0, len(fields), 2):
#                             if j + 1 < len(fields):
#                                 metadata[fields[j]] = fields[j + 1]
                        
#                         if metadata:
#                             term = metadata.get('term', '')
#                             distance = damerau_levenshtein_distance(word_lower, term.lower())
                            
#                             if distance <= max_distance and distance > 0:
#                                 rank = metadata.get('rank', 0)
#                                 try:
#                                     rank = int(float(rank))
#                                 except (ValueError, TypeError):
#                                     rank = 0
                                
#                                 pos = metadata.get('pos', 'unknown')
#                                 if isinstance(pos, str) and pos.startswith('['):
#                                     try:
#                                         parsed = json.loads(pos)
#                                         pos = parsed[0] if parsed else 'unknown'
#                                     except:
#                                         pass
#                                 pos = str(pos).lower()
                                
#                                 suggestions.append({
#                                     'term': term,
#                                     'display': metadata.get('display', term),
#                                     'category': metadata.get('category', ''),
#                                     'description': metadata.get('description', ''),
#                                     'pos': pos,
#                                     'entity_type': metadata.get('entity_type', 'unigram'),
#                                     'rank': rank,
#                                     'distance': distance,
#                                 })
                        
#                         i += 2
                        
#             except Exception as e:
#                 print(f"Fuzzy search error for '{word}': {e}")
        
#         suggestions.sort(key=lambda x: (x['distance'], -x['rank']))
        
#         seen = set()
#         unique = []
#         for s in suggestions:
#             if s['term'].lower() not in seen:
#                 seen.add(s['term'].lower())
#                 unique.append(s)
        
#         return unique[:limit]
        
#     except Exception as e:
#         print(f"Error getting suggestions for '{word}': {e}")
#         return []


# def get_fuzzy_suggestions_batch(
#         words: List[str],
#         limit: int = 10,
#         max_distance: int = 2
#     ) -> Dict[str, List[Dict[str, Any]]]:
#     """
#     Get fuzzy suggestions for ALL words in ONE Redis pipeline call.
#     Returns dict of {word: [suggestions]}
#     """
#     client = RedisClient.get_client()
#     if not client:
#         return {word: [] for word in words}

#     unique_words = list({w.lower().strip() for w in words if len(w.strip()) >= 2})

#     if not unique_words:
#         return {}

#     pipe = client.pipeline(transaction=False)

#     for word in unique_words:
#         query = f"%{word}%"
#         pipe.execute_command(
#             'FT.SEARCH', 'terms_idx', query,
#             'SORTBY', 'rank', 'DESC',
#             'LIMIT', '0', str(limit * 3)
#         )

#     try:
#         all_results = pipe.execute()
#     except Exception as e:
#         print(f"Pipeline batch error: {e}")
#         return {word: [] for word in unique_words}

#     batch_suggestions = {}

#     for word, result in zip(unique_words, all_results):
#         suggestions = []

#         if not result or len(result) <= 1:
#             batch_suggestions[word] = []
#             continue

#         i = 1
#         while i < len(result):
#             key = result[i]
#             fields = result[i + 1] if i + 1 < len(result) else []

#             metadata = {}
#             for j in range(0, len(fields), 2):
#                 if j + 1 < len(fields):
#                     metadata[fields[j]] = fields[j + 1]

#             if metadata:
#                 term = metadata.get('term', '')
#                 distance = damerau_levenshtein_distance(word, term.lower())

#                 if distance <= max_distance and distance > 0:
#                     rank = metadata.get('rank', 0)
#                     try:
#                         rank = int(float(rank))
#                     except (ValueError, TypeError):
#                         rank = 0

#                     pos = metadata.get('pos', 'unknown')
#                     if isinstance(pos, str) and pos.startswith('['):
#                         try:
#                             parsed = json.loads(pos)
#                             pos = parsed[0] if parsed else 'unknown'
#                         except:
#                             pass
#                     pos = str(pos).lower()

#                     suggestions.append({
#                         'term': term,
#                         'display': metadata.get('display', term),
#                         'category': metadata.get('category', ''),
#                         'description': metadata.get('description', ''),
#                         'pos': pos,
#                         'entity_type': metadata.get('entity_type', 'unigram'),
#                         'rank': rank,
#                         'distance': distance,
#                     })

#             i += 2

#         # If % fuzzy found nothing, try %% (wider fuzzy)
#         if not suggestions:
#             try:
#                 wider_result = client.execute_command(
#                     'FT.SEARCH', 'terms_idx', f"%%{word}%%",
#                     'SORTBY', 'rank', 'DESC',
#                     'LIMIT', '0', str(limit * 3)
#                 )
#                 i = 1
#                 while i < len(wider_result):
#                     fields = wider_result[i + 1] if i + 1 < len(wider_result) else []
#                     metadata = {}
#                     for j in range(0, len(fields), 2):
#                         if j + 1 < len(fields):
#                             metadata[fields[j]] = fields[j + 1]
#                     if metadata:
#                         term = metadata.get('term', '')
#                         distance = damerau_levenshtein_distance(word, term.lower())
#                         if distance <= max_distance and distance > 0:
#                             rank = metadata.get('rank', 0)
#                             try:
#                                 rank = int(float(rank))
#                             except (ValueError, TypeError):
#                                 rank = 0
#                             pos = metadata.get('pos', 'unknown')
#                             if isinstance(pos, str) and pos.startswith('['):
#                                 try:
#                                     parsed = json.loads(pos)
#                                     pos = parsed[0] if parsed else 'unknown'
#                                 except:
#                                     pass
#                             pos = str(pos).lower()
#                             suggestions.append({
#                                 'term': term,
#                                 'display': metadata.get('display', term),
#                                 'category': metadata.get('category', ''),
#                                 'description': metadata.get('description', ''),
#                                 'pos': pos,
#                                 'entity_type': metadata.get('entity_type', 'unigram'),
#                                 'rank': rank,
#                                 'distance': distance,
#                             })
#                     i += 2
#             except Exception as e:
#                 print(f"Wider fuzzy search error for '{word}': {e}")

#         suggestions.sort(key=lambda x: (x['distance'], -x['rank']))

#         seen = set()
#         unique = []
#         for s in suggestions:
#             if s['term'].lower() not in seen:
#                 seen.add(s['term'].lower())
#                 unique.append(s)

#         batch_suggestions[word] = unique[:limit]

#     return batch_suggestions


# # =============================================================================
# # MAIN WORD DISCOVERY CLASS
# # =============================================================================

# class WordDiscovery:
#     """
#     Word Discovery Engine - The brain behind Typesense queries.
    
#     Uses RAM cache (vocabulary_cache.py) for all lookups.
#     Only uses Redis for fuzzy spelling correction (Step 5).
#     """
    
#     def __init__(self, verbose: bool = False):
#         self.verbose = verbose
#         self.cache = vocab_cache_wrapper
    
#     def process(self, query: str) -> Dict[str, Any]:
#         """Main entry point - process a query through all steps."""
#         start_time = time.perf_counter()
        
#         if self.verbose:
#             print("\n" + "=" * 70)
#             print(f"🔍 WORD DISCOVERY: '{query}'")
#             print(f"   RAM cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
#             print("=" * 70)
        
#         if not query or not query.strip():
#             return self._empty_result(query)
        
#         words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
#         words = [w for w in words if w]
        
#         if len(words) == 1:
#             return self._process_single_word(words[0], start_time)
        
#         # Step 1: Collect ALL data (RAM)
#         word_data = self._step1_collect_all_data(words)
        
#         # Step 2: Mark n-gram positions (RAM)
#         ngrams, consumed_positions = self._step2_detect_ngrams(words, word_data)
        
#         # Step 3: Determine POS for remaining words
#         self._step3_determine_pos(word_data, consumed_positions)

#         # Step 3.5: Refine POS from suffixes
#         self._step3_5_refine_pos_from_suffix(word_data)
        
#         # Step 4: Select best match (POS + Rank) (RAM)
#         self._step4_select_best_match(word_data, consumed_positions)
        
#         # Step 5: Handle unknowns (REDIS - only step that uses Redis)
#         corrections = self._step5_correct_unknowns(word_data)
        
#         # Step 6: Re-check n-grams after correction (RAM)
#         corrected_words = self._get_working_words(word_data)
#         new_ngrams, new_consumed = self._step6_recheck_ngrams(
#             corrected_words, word_data, consumed_positions
#         )
#         ngrams.extend(new_ngrams)
#         consumed_positions.update(new_consumed)
        
#         # Step 7: Build final output
#         output = self._step7_build_output(
#             query, word_data, ngrams, consumed_positions, corrections, start_time
#         )
        
#         return output
    
#     # =========================================================================
#     # STEP 1: Collect ALL Data (RAM)
#     # =========================================================================
    
#     def _step1_collect_all_data(self, words: List[str]) -> List[Dict[str, Any]]:
#         """Collect all matches for each word from RAM cache."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("📖 STEP 1: Collect ALL Data (RAM)")
#             print("-" * 70)
        
#         word_data = []
        
#         for i, word in enumerate(words):
#             position = i + 1
#             word_lower = word.lower().strip()
            
#             if word_lower in STOPWORDS:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': True,
#                     'pos': STOPWORDS[word_lower],
#                     'predicted_pos': None,
#                     'all_matches': [],
#                     'selected_match': {
#                         'term': word_lower,
#                         'display': word_lower,
#                         'category': 'stopword',
#                         'description': '',
#                         'pos': STOPWORDS[word_lower],
#                         'entity_type': 'stopword',
#                         'rank': 0,
#                     },
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → STOPWORD ({STOPWORDS[word_lower]})")
#                 continue
            
#             matches = self.cache.get_term_matches(word_lower)
            
#             if matches:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'found',
#                     'is_stopword': False,
#                     'pos': None,
#                     'predicted_pos': None,
#                     'all_matches': matches,
#                     'selected_match': None,
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → FOUND ({len(matches)} matches)")
#                     for m in matches[:3]:
#                         print(f"       - {m['category']}: pos={m['pos']}, rank={m['rank']}")
#                     if len(matches) > 3:
#                         print(f"       ... and {len(matches) - 3} more")
#             else:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'unknown',
#                     'is_stopword': False,
#                     'pos': None,
#                     'predicted_pos': None,
#                     'all_matches': [],
#                     'selected_match': None,
#                 })
                
#                 if self.verbose:
#                     print(f"  [{position}] '{word_lower}' → UNKNOWN")
        
#         return word_data

    
#     # =========================================================================
#     # STEP 2: Detect N-grams (RAM)
#     # =========================================================================
    
#     def _step2_detect_ngrams(
#         self,
#         words: List[str],
#         word_data: List[Dict[str, Any]]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """Detect all n-grams using RAM cache, prioritizing longest matches."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔗 STEP 2: Detect N-grams (RAM)")
#             print("-" * 70)
        
#         ngrams = []
#         consumed = set()
        
#         if len(words) >= 4:
#             for i in range(len(words) - 3):
#                 if any(p in consumed for p in [i, i+1, i+2, i+3]):
#                     continue
                
#                 quad = words[i:i+4]
#                 result = self.cache.get_ngram(quad)
                
#                 if result:
#                     positions = [i+1, i+2, i+3, i+4]
#                     ngrams.append({
#                         'type': 'quadgram',
#                         'positions': positions,
#                         'words': quad,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1, i+2, i+3])
                    
#                     if self.verbose:
#                         print(f"  ✅ QUADGRAM: '{' '.join(quad)}'")
#                         print(f"       Category: {result['category']}")
        
#         if len(words) >= 3:
#             for i in range(len(words) - 2):
#                 if any(p in consumed for p in [i, i+1, i+2]):
#                     continue
                
#                 tri = words[i:i+3]
#                 result = self.cache.get_ngram(tri)
                
#                 if result:
#                     positions = [i+1, i+2, i+3]
#                     ngrams.append({
#                         'type': 'trigram',
#                         'positions': positions,
#                         'words': tri,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1, i+2])
                    
#                     if self.verbose:
#                         print(f"  ✅ TRIGRAM: '{' '.join(tri)}'")
#                         print(f"       Category: {result['category']}")
        
#         if len(words) >= 2:
#             for i in range(len(words) - 1):
#                 if any(p in consumed for p in [i, i+1]):
#                     continue
                
#                 bi = words[i:i+2]
#                 result = self.cache.get_ngram(bi)
                
#                 if result:
#                     positions = [i+1, i+2]
#                     ngrams.append({
#                         'type': 'bigram',
#                         'positions': positions,
#                         'words': bi,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                     })
#                     consumed.update([i, i+1])
                    
#                     if self.verbose:
#                         print(f"  ✅ BIGRAM: '{' '.join(bi)}'")
#                         print(f"       Category: {result['category']}")
        
#         if not ngrams and self.verbose:
#             print("  (no n-grams found)")
        
#         return ngrams, consumed

#     # =========================================================================
#     # STEP 3: Determine POS for ALL Words
#     # =========================================================================
    
#     def _step3_determine_pos(
#         self,
#         word_data: List[Dict[str, Any]],
#         consumed_positions: Set[int]
#     ) -> None:
#         """Determine POS for ALL words using grammar rules — multi-pass for adjacent unknowns."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🧠 STEP 3: Determine POS (Grammar Rules — Multi-Pass)")
#             print("-" * 70)

#         def get_nearest_known_pos(index: int, direction: int) -> Optional[str]:
#             i = index + direction
#             while 0 <= i < len(word_data):
#                 wd = word_data[i]
#                 if wd['is_stopword']:
#                     return wd['pos']
#                 if wd.get('pos'):
#                     return wd['pos']
#                 if wd.get('predicted_pos'):
#                     return wd['predicted_pos']
#                 if wd['all_matches']:
#                     return normalize_pos_string(wd['all_matches'][0]['pos'])
#                 i += direction
#             return None

#         def resolve_word(i: int, wd: Dict[str, Any]) -> bool:
#             position = wd['position']
#             pos_index = position - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions

#             if wd['is_stopword']:
#                 wd['predicted_pos'] = wd['pos']
#                 wd['predicted_pos_list'] = [(wd['pos'], 1.0)]
#                 return True

#             left_pos = None
#             if i == 0:
#                 left_pos = 'start'
#             else:
#                 left_pos = get_nearest_known_pos(i, direction=-1)

#             right_pos = None
#             if i == len(word_data) - 1:
#                 right_pos = 'end'
#             else:
#                 right_pos = get_nearest_known_pos(i, direction=1)

#             if left_pos is None and right_pos is None:
#                 return False

#             predicted_pos = None

#             if left_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))

#             if not predicted_pos and left_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, None))

#             if not predicted_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((None, right_pos))

#             if not predicted_pos:
#                 predicted_pos = [('noun', 0.75)]

#             has_be_before = any(
#                 word_data[j]['is_stopword'] and word_data[j]['pos'] == 'be'
#                 for j in range(0, i)
#             )
#             next_ends_ed = False
#             if i + 1 < len(word_data):
#                 next_word_clean = word_data[i + 1]['word'].rstrip('?!.,;:')
#                 next_ends_ed = next_word_clean.endswith('ed')

#             if has_be_before and next_ends_ed and not wd['is_stopword']:
#                 is_known_adverb = any(
#                     m['pos'] in ('adverb',)
#                     for m in wd.get('all_matches', [])
#                 )
#                 if not is_known_adverb:
#                     predicted_pos = [
#                         ('noun', 0.95),
#                         ('proper_noun', 0.85),
#                         ('adjective', 0.4),
#                         ('adverb', 0.3),
#                     ]
#                     if self.verbose:
#                         print(f"       ⚡ Passive voice: [be]..._{wd['word']}_...[-ed] → noun (0.95)")

#             if isinstance(predicted_pos, list):
#                 wd['predicted_pos_list'] = predicted_pos
#                 wd['predicted_pos'] = predicted_pos[0][0] if predicted_pos else 'noun'
#             else:
#                 wd['predicted_pos_list'] = [(predicted_pos, 0.90)]
#                 wd['predicted_pos'] = predicted_pos

#             if self.verbose:
#                 context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
#                 ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
#                 print(f"  [{position}] '{wd['word']}' → Predicted: {wd['predicted_pos_list']}{ngram_note}")
#                 print(f"       Context: {context}")

#             return True

#         max_passes = len(word_data)
#         resolved = set()

#         for pass_num in range(max_passes):
#             progress = False

#             if self.verbose and pass_num > 0:
#                 print(f"\n  ── Pass {pass_num + 1} ──")

#             for i, wd in enumerate(word_data):
#                 if i in resolved:
#                     continue

#                 success = resolve_word(i, wd)

#                 if success:
#                     resolved.add(i)
#                     progress = True

#             if not progress or len(resolved) == len(word_data):
#                 break

#         for i, wd in enumerate(word_data):
#             if i not in resolved:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{wd['word']}' → No context found, defaulting to noun")
#                 wd['predicted_pos'] = 'noun'
#                 wd['predicted_pos_list'] = [('noun', 0.75)]

#     # =========================================================================
#     # STEP 3.5: Refine POS from Word Suffixes
#     # =========================================================================
    
#     SUFFIX_POS_RULES = {
#         'ing': [('gerund', 0.80), ('adjective', 0.75), ('verb', 0.70), ('noun', 0.50)],
#         'ed': [('verb', 0.85), ('adjective', 0.75), ('participle', 0.70)],
#         'ly': [('adverb', 0.95), ('adjective', 0.40)],
#         'tion': [('noun', 0.95)],
#         'sion': [('noun', 0.95)],
#         'ment': [('noun', 0.95)],
#         'ness': [('noun', 0.95)],
#         'ity': [('noun', 0.95)],
#         'ence': [('noun', 0.90)],
#         'ance': [('noun', 0.90)],
#         'ure': [('noun', 0.80)],
#         'ism': [('noun', 0.95)],
#         'ist': [('noun', 0.90)],
#         'ery': [('noun', 0.85)],
#         'ory': [('noun', 0.80), ('adjective', 0.60)],
#         'age': [('noun', 0.80)],
#         'ship': [('noun', 0.95)],
#         'dom': [('noun', 0.90)],
#         'hood': [('noun', 0.95)],
#         'ling': [('noun', 0.80)],
#         'ful': [('adjective', 0.95)],
#         'less': [('adjective', 0.95)],
#         'able': [('adjective', 0.90)],
#         'ible': [('adjective', 0.90)],
#         'ous': [('adjective', 0.95)],
#         'ious': [('adjective', 0.95)],
#         'ive': [('adjective', 0.90)],
#         'al': [('adjective', 0.85), ('noun', 0.50)],
#         'ial': [('adjective', 0.90)],
#         'ical': [('adjective', 0.90)],
#         'ish': [('adjective', 0.85)],
#         'ern': [('adjective', 0.80)],
#         'ese': [('adjective', 0.80), ('noun', 0.70)],
#         'ian': [('adjective', 0.80), ('noun', 0.75)],
#         'ean': [('adjective', 0.80)],
#         'ic': [('adjective', 0.85)],
#         'est': [('adjective', 0.85)],
#         'ent': [('adjective', 0.75), ('noun', 0.65)],
#         'ant': [('adjective', 0.75), ('noun', 0.65)],
#         'ify': [('verb', 0.95)],
#         'ize': [('verb', 0.95)],
#         'ise': [('verb', 0.90)],
#         'ate': [('verb', 0.80), ('adjective', 0.60), ('noun', 0.50)],
#         'en': [('verb', 0.70), ('adjective', 0.60)],
#         'er': [('noun', 0.75), ('adjective', 0.65)],
#         'or': [('noun', 0.80)],
#         'ee': [('noun', 0.85)],
#         'eer': [('noun', 0.85)],
#     }
    
#     SUFFIX_EXCEPTIONS = frozenset({
#         'ring', 'king', 'sing', 'bring', 'thing', 'string', 'spring',
#         'wing', 'swing', 'sting', 'cling', 'fling', 'sling', 'wring',
#         'bling', 'ding', 'ping', 'zing', 'ming', 'bing', 'ling',
#         'beijing', 'sterling', 'viking', 'darling', 'ceiling',
#         'feeling', 'dealing', 'healing', 'meaning', 'evening',
#         'bed', 'red', 'shed', 'led', 'fed', 'wed', 'sled',
#         'fly', 'ply', 'sly', 'holy', 'ugly', 'bully', 'belly',
#         'lily', 'jelly', 'jolly', 'rally', 'ally', 'tally',
#         'family', 'italy', 'july', 'daily', 'early',
#         'water', 'after', 'under', 'over', 'never', 'ever', 'other',
#         'rather', 'either', 'neither', 'whether', 'together',
#         'butter', 'letter', 'matter', 'better', 'bitter', 'litter',
#         'dinner', 'inner', 'upper', 'lower', 'power', 'flower',
#         'tower', 'shower', 'silver', 'river', 'liver', 'cover',
#         'fever', 'clever', 'finger', 'ginger', 'tiger', 'anger',
#         'danger', 'hunger', 'umber', 'number', 'member', 'timber',
#         'order', 'border', 'murder', 'wonder', 'thunder', 'super',
#         'paper', 'copper', 'proper', 'whisper', 'chapter', 'master',
#         'sister', 'mister', 'winter', 'center', 'corner', 'monster',
#         'soccer', 'cancer', 'rubber', 'hammer', 'summer', 'manner',
#         'animal', 'hospital', 'capital', 'metal', 'total', 'eral',
#         'canal', 'eral', 'general', 'mineral', 'festival', 'signal',
#         'journal', 'rival', 'survival', 'arrival', 'proposal',
#         'awful',
#         'age', 'page', 'sage', 'cage', 'rage', 'wage', 'stage',
#         'cement', 'moment', 'comment', 'segment', 'element',
#         'music', 'magic', 'basic', 'logic', 'topic', 'panic',
#         'fabric', 'garlic', 'picnic', 'traffic', 'public',
#         'west', 'east', 'nest', 'rest', 'test', 'best', 'chest',
#         'quest', 'guest', 'forest', 'harvest', 'interest', 'protest',
#         'request', 'suggest', 'modest', 'honest', 'invest',
#         'gate', 'late', 'mate', 'date', 'fate', 'rate', 'state',
#         'plate', 'private', 'climate', 'chocolate', 'senate',
#         'debate', 'estate', 'ultimate', 'intimate', 'accurate',
#         'delicate', 'separate', 'desperate', 'moderate', 'adequate',
#         'fish', 'dish', 'wish', 'finish', 'publish', 'polish',
#         'vanish', 'perish', 'cherish', 'nourish', 'flourish',
#     })
    
#     MIN_SUFFIX_WORD_LENGTH = 4
    
#     def _step3_5_refine_pos_from_suffix(self, word_data: List[Dict[str, Any]]) -> None:
#         """Refine POS predictions using word suffixes."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔤 STEP 3.5: Refine POS from Suffixes")
#             print("-" * 70)
        
#         for wd in word_data:
#             if wd['is_stopword']:
#                 continue
            
#             word = wd.get('corrected', wd['word']).lower()
            
#             if len(word) < self.MIN_SUFFIX_WORD_LENGTH:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Too short, skipping")
#                 continue
            
#             if word in self.SUFFIX_EXCEPTIONS:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Exception, skipping")
#                 continue
            
#             matched_suffix = None
#             suffix_predictions = None
            
#             sorted_suffixes = sorted(self.SUFFIX_POS_RULES.keys(), key=len, reverse=True)
            
#             for suffix in sorted_suffixes:
#                 if word.endswith(suffix) and len(word) > len(suffix) + 1:
#                     matched_suffix = suffix
#                     suffix_predictions = self.SUFFIX_POS_RULES[suffix]
#                     break
            
#             if not matched_suffix:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → No suffix match")
#                 continue
            
#             current_list = wd.get('predicted_pos_list', [])
#             current_primary = wd.get('predicted_pos', 'noun')
            
#             refined = {}
            
#             for pos, conf in current_list:
#                 refined[pos] = conf
            
#             for pos, suffix_conf in suffix_predictions:
#                 if pos in refined:
#                     existing = refined[pos]
#                     boosted = min((existing + suffix_conf) / 2 + 0.10, 0.98)
#                     refined[pos] = boosted
#                 else:
#                     refined[pos] = suffix_conf * 0.85
            
#             refined_list = sorted(refined.items(), key=lambda x: -x[1])
            
#             wd['predicted_pos_list'] = refined_list
#             wd['predicted_pos'] = refined_list[0][0] if refined_list else current_primary
#             wd['suffix_detected'] = matched_suffix
            
#             if self.verbose:
#                 changed = wd['predicted_pos'] != current_primary
#                 change_marker = " ⚡ CHANGED" if changed else ""
#                 print(f"  [{wd['position']}] '{word}' → Suffix: -{matched_suffix}{change_marker}")
#                 print(f"       Before: {current_list}")
#                 print(f"       After:  {refined_list}")

#     # =========================================================================
#     # STEP 4: Select Best Match (POS + Rank) (RAM)
#     # =========================================================================
    
#     def _step4_select_best_match(
#         self,
#         word_data: List[Dict[str, Any]],
#         consumed_positions: Set[int]
#     ) -> None:
#         """For ALL words with multiple matches, select based on POS + rank."""
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🎯 STEP 4: Select Best Match (POS + Rank)")
#             print("-" * 70)
        
#         for wd in word_data:
#             position = wd['position']
            
#             if wd['is_stopword']:
#                 continue
            
#             if wd['status'] == 'unknown':
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Unknown (defer to Step 5)")
#                 continue
            
#             matches = wd['all_matches']
#             if not matches:
#                 continue
            
#             predicted_pos = wd['predicted_pos']
            
#             compatible_matches = []
#             for m in matches:
#                 match_pos = normalize_pos_string(m['pos'])
#                 if is_pos_compatible(match_pos, predicted_pos):
#                     compatible_matches.append(m)
            
#             pos_index = position - 1
#             is_in_ngram = pos_index in consumed_positions
#             ngram_note = " (in n-gram)" if is_in_ngram else ""
            
#             if compatible_matches:
#                 compatible_matches.sort(key=lambda x: x['rank'], reverse=True)
#                 best = compatible_matches[0]
#                 wd['selected_match'] = best
#                 wd['pos'] = normalize_pos_string(best['pos'])
#                 wd['status'] = 'valid'
                
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Selected: {best['category']} (pos={normalize_pos_string(best['pos'])}, rank={best['rank']}){ngram_note}")
#                     if len(matches) > 1:
#                         rejected = [f"{m['category']}({normalize_pos_string(m['pos'])})" for m in matches if m != best]
#                         print(f"       Rejected: {rejected}")
#             else:
#                 best = matches[0]
#                 wd['selected_match'] = best
#                 wd['pos'] = normalize_pos_string(best['pos'])
#                 wd['status'] = 'valid'
                
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Fallback: {best['category']} (no POS match, using rank){ngram_note}")

#     # =========================================================================
#     # STEP 5: Correct Unknown Words (REDIS - only Redis step)
#     #
#     # PATCH: Words not found in the RAM hash are NOT replaced in the search
#     # query. The fuzzy suggestion is stored as a hint only
#     # (correction_type='suggestion', status='unknown_suggest') so the
#     # semantic path (graph → embedding → vector search) can still find
#     # results using the original word.
#     #
#     # Words found in the RAM hash but with the wrong POS (pos_mismatch)
#     # ARE replaced because they are genuine typos — a similar word with
#     # the correct POS exists in the hash.
#     # =========================================================================

#     def _step5_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#         """
#         Handle unknown and POS-mismatched words via Redis fuzzy search.

#         - unknown words     → keep original for search; fuzzy match shown as
#                               "did you mean?" hint only (correction_type='suggestion')
#         - pos_mismatch words → replace word (genuine typo found in RAM hash
#                                with wrong POS)
#         """
#         unknowns = []
#         pos_mismatches = []

#         for wd in word_data:
#             if wd['status'] == 'unknown':
#                 unknowns.append(wd)
#             elif (
#                 wd['status'] == 'valid'
#                 and not wd['is_stopword']
#                 and wd.get('predicted_pos')
#                 and wd.get('pos')
#                 and wd['predicted_pos'] != wd['pos']
#             ):
#                 predicted_list = wd.get('predicted_pos_list', [])
#                 top_confidence = predicted_list[0][1] if predicted_list else 0
#                 word_rank = wd.get('selected_match', {}).get('rank', 0) or 0
#                 if top_confidence >= 0.90 and word_rank < 200:
#                     pos_mismatches.append(wd)

#         if not unknowns and not pos_mismatches:
#             if self.verbose:
#                 print("\n" + "-" * 70)
#                 print("🔧 STEP 5: Correct Unknowns")
#                 print("-" * 70)
#                 print("  (no unknown words)")
#             return []

#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔧 STEP 5: Correct Unknowns (Redis — TRUE Pipeline Batch)")
#             print("-" * 70)
#             print(f"  Unknowns:       {len(unknowns)}")
#             print(f"  POS mismatches: {len(pos_mismatches)}")

#         all_words_to_correct = [
#             {
#                 'wd': wd,
#                 'word': wd['word'],
#                 'predicted_pos': wd.get('predicted_pos') or 'noun',
#                 'correction_type': 'unknown',
#             }
#             for wd in unknowns
#         ] + [
#             {
#                 'wd': wd,
#                 'word': wd['word'],
#                 'predicted_pos': wd.get('predicted_pos') or 'noun',
#                 'correction_type': 'pos_mismatch',
#                 'word_rank': wd.get('selected_match', {}).get('rank', 0) or 0,
#             }
#             for wd in pos_mismatches
#         ]

#         words_to_fetch = [item['word'] for item in all_words_to_correct]

#         if self.verbose:
#             print(f"  Sending to Redis pipeline: {words_to_fetch}")

#         batch_suggestions = get_fuzzy_suggestions_batch(
#             words_to_fetch, limit=10, max_distance=2
#         )

#         corrections = []

#         for item in all_words_to_correct:
#             wd = item['wd']
#             word = item['word']
#             position = wd['position']
#             predicted_pos = item['predicted_pos']
#             correction_type = item['correction_type']

#             suggestions = batch_suggestions.get(word.lower().strip(), [])

#             for s in suggestions:
#                 s['compatible'] = is_pos_compatible(s['pos'], predicted_pos)
#             wd['redis_suggestions'] = suggestions

#             if not suggestions:
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → No suggestions found")
#                 continue

#             compatible = [s for s in suggestions if s['compatible']]

#             if correction_type == 'pos_mismatch':
#                 word_rank = item.get('word_rank', 0)
#                 compatible = [
#                     s for s in compatible
#                     if s['term'] != word
#                     and s['distance'] <= 1
#                     and s['rank'] > word_rank * 3
#                 ]

#             if compatible:
#                 compatible.sort(key=lambda x: (
#                     x['distance'],
#                     self._pos_match_score(x['pos'], predicted_pos),
#                     -x['rank']
#                 ))
#                 best = compatible[0]
#             else:
#                 best = suggestions[0]

#             if correction_type == 'pos_mismatch':
#                 # Genuine typo — safe to replace the word in the search query
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → REPLACED with '{best['term']}' "
#                           f"(pos_mismatch, distance={best['distance']})")

#                 wd['status'] = 'pos_corrected'
#                 wd['corrected'] = best['term']
#                 wd['corrected_display'] = best['display']
#                 wd['pos'] = best['pos']
#                 wd['distance'] = best['distance']
#                 wd['selected_match'] = {
#                     'term': best['term'],
#                     'display': best['display'],
#                     'category': best['category'],
#                     'description': best['description'],
#                     'pos': best['pos'],
#                     'entity_type': best['entity_type'],
#                     'rank': best['rank'],
#                 }
#                 corrections.append({
#                     'position': position,
#                     'original': word,
#                     'corrected': best['term'],
#                     'distance': best['distance'],
#                     'pos': best['pos'],
#                     'category': best['category'],
#                     'correction_type': 'pos_mismatch',
#                 })

#             else:
#                 # Unknown word — NOT in RAM hash.
#                 # Keep original word for the search so the semantic path
#                 # (graph → embedding → vector search) can still find results.
#                 # Surface the fuzzy match as a "did you mean?" hint only.
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → KEPT for search (not in RAM hash). "
#                           f"Suggestion: '{best['term']}' (distance={best['distance']})")

#                 wd['status'] = 'unknown_suggest'   # original word used in search
#                 wd['suggestion'] = best['term']
#                 wd['suggestion_display'] = best['display']
#                 wd['suggestion_distance'] = best['distance']
#                 # Do NOT set wd['corrected'] — _get_working_words will fall
#                 # through to wd['word'] (the original) for unknown_suggest.
#                 corrections.append({
#                     'position': position,
#                     'original': word,
#                     'corrected': best['term'],      # shown in UI as hint
#                     'distance': best['distance'],
#                     'pos': best['pos'],
#                     'category': best['category'],
#                     'correction_type': 'suggestion',  # hint only, not a replacement
#                 })

#         return corrections

#     def _pos_match_score(self, pos_value: Any, predicted_pos: str) -> int:
#         """
#         Returns 0 if pos_value exactly matches predicted_pos,
#         1 if compatible but not exact.
#         Used to sort Redis suggestions — POS exactness before rank.
#         """
#         if isinstance(pos_value, list):
#             pos_list = pos_value
#         elif isinstance(pos_value, str) and pos_value.startswith('['):
#             import ast
#             try:
#                 pos_list = ast.literal_eval(pos_value)
#             except:
#                 pos_list = [pos_value]
#         else:
#             pos_list = [pos_value]

#         for p in pos_list:
#             if p == predicted_pos:
#                 return 0
#         return 1

#     # =========================================================================
#     # STEP 6: Re-check N-grams After Correction (RAM)
#     # =========================================================================
    
#     def _step6_recheck_ngrams(
#         self,
#         corrected_words: List[str],
#         word_data: List[Dict[str, Any]],
#         already_consumed: Set[int]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """Re-check for n-grams using corrected words (RAM cache)."""
#         has_corrections = any(wd['status'] in ('corrected', 'pos_corrected') for wd in word_data)
        
#         if not has_corrections:
#             if self.verbose:
#                 print("\n" + "-" * 70)
#                 print("🔄 STEP 6: Re-check N-grams After Correction")
#                 print("-" * 70)
#                 print("  (no corrections, skipping)")
#             return [], set()
        
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔄 STEP 6: Re-check N-grams After Correction (RAM)")
#             print("-" * 70)
        
#         new_ngrams = []
#         new_consumed = set()
#         words = corrected_words
        
#         if len(words) >= 3:
#             for i in range(len(words) - 2):
#                 if any(p in already_consumed or p in new_consumed for p in [i, i+1, i+2]):
#                     continue
                
#                 relevant_words = word_data[i:i+3]
#                 if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
#                     continue
                
#                 tri = words[i:i+3]
#                 result = self.cache.get_ngram(tri)
                
#                 if result:
#                     positions = [i+1, i+2, i+3]
#                     new_ngrams.append({
#                         'type': 'trigram',
#                         'positions': positions,
#                         'words': tri,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                         'from_correction': True,
#                     })
#                     new_consumed.update([i, i+1, i+2])
                    
#                     if self.verbose:
#                         print(f"  ✅ NEW TRIGRAM: '{' '.join(tri)}' (after correction)")
        
#         if len(words) >= 2:
#             for i in range(len(words) - 1):
#                 if any(p in already_consumed or p in new_consumed for p in [i, i+1]):
#                     continue
                
#                 relevant_words = word_data[i:i+2]
#                 if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
#                     continue
                
#                 bi = words[i:i+2]
#                 result = self.cache.get_ngram(bi)
                
#                 if result:
#                     positions = [i+1, i+2]
#                     new_ngrams.append({
#                         'type': 'bigram',
#                         'positions': positions,
#                         'words': bi,
#                         'phrase': result['term'],
#                         'display': result['display'],
#                         'category': result['category'],
#                         'description': result['description'],
#                         'pos': normalize_pos_string(result['pos']),
#                         'rank': result['rank'],
#                         'from_correction': True,
#                     })
#                     new_consumed.update([i, i+1])
                    
#                     if self.verbose:
#                         print(f"  ✅ NEW BIGRAM: '{' '.join(bi)}' (after correction)")
        
#         if not new_ngrams and self.verbose:
#             print("  (no new n-grams found)")
        
#         return new_ngrams, new_consumed
    
#     # =========================================================================
#     # STEP 7: Build Final Output
#     # =========================================================================
    
#     def _step7_build_output(
#         self,
#         query: str,
#         word_data: List[Dict[str, Any]],
#         ngrams: List[Dict[str, Any]],
#         consumed_positions: Set[int],
#         corrections: List[Dict[str, Any]],
#         start_time: float
#     ) -> Dict[str, Any]:
#         """Build the final output structure for Typesense."""
#         corrected_words = self._get_working_words(word_data)
#         corrected_query = ' '.join(corrected_words)
        
#         for wd in word_data:
#             pos_index = wd['position'] - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions
        
#         terms = []
#         for wd in word_data:
#             pos_value = wd.get('pos') or wd.get('predicted_pos') or 'unknown'
#             pos_value = normalize_pos_string(pos_value)
#             predicted_pos = wd.get('predicted_pos')
#             if predicted_pos:
#                 predicted_pos = normalize_pos_string(predicted_pos)
            
#             term = {
#                 'position': wd['position'],
#                 'word': wd['word'],
#                 'status': wd['status'],
#                 'is_stopword': wd['is_stopword'],
#                 'part_of_ngram': wd.get('part_of_ngram', False),
#                 'pos': pos_value,
#                 'predicted_pos': predicted_pos,
#             }
            
#             if wd.get('selected_match'):
#                 sm = wd['selected_match']
#                 term['category'] = sm.get('category', '')
#                 term['description'] = sm.get('description', '')
#                 term['display'] = sm.get('display', wd['word'])
#                 term['rank'] = sm.get('rank', 0)
#                 term['entity_type'] = sm.get('entity_type', 'unigram')
#             elif wd.get('all_matches') and len(wd['all_matches']) > 0:
#                 best_match = wd['all_matches'][0]
#                 term['category'] = best_match.get('category', '')
#                 term['description'] = best_match.get('description', '')
#                 term['display'] = best_match.get('display', wd['word'])
#                 term['rank'] = best_match.get('rank', 0)
#                 term['entity_type'] = best_match.get('entity_type', 'unigram')
#             else:
#                 term['category'] = wd.get('category', '')
#                 term['description'] = ''
#                 term['display'] = wd['word']
#                 term['rank'] = 0
#                 term['entity_type'] = 'unigram'
            
#             # Only attach corrected/display fields for genuine replacements
#             if wd['status'] in ('corrected', 'pos_corrected'):
#                 term['corrected'] = wd.get('corrected')
#                 term['corrected_display'] = wd.get('corrected_display')
#                 term['distance'] = wd.get('distance')

#             # Attach suggestion fields for hint-only unknowns
#             if wd['status'] == 'unknown_suggest':
#                 term['suggestion'] = wd.get('suggestion')
#                 term['suggestion_display'] = wd.get('suggestion_display')
#                 term['suggestion_distance'] = wd.get('suggestion_distance')
            
#             term['match_count'] = len(wd.get('all_matches', []))
            
#             terms.append(term)
        
#         elapsed = (time.perf_counter() - start_time) * 1000
        
#         output = {
#             'query': query,
#             'corrected_query': corrected_query,
#             'processing_time_ms': round(elapsed, 2),
#             'terms': terms,
#             'ngrams': ngrams,
#             'corrections': corrections,
#             'stats': {
#                 'total_words': len(word_data),
#                 'valid_words': sum(1 for wd in word_data if wd['status'] == 'valid'),
#                 'corrected_words': sum(1 for wd in word_data if wd['status'] in ('corrected', 'pos_corrected')),
#                 'unknown_words': sum(1 for wd in word_data if wd['status'] in ('unknown', 'unknown_suggest')),
#                 'stopwords': sum(1 for wd in word_data if wd['is_stopword']),
#                 'ngram_count': len(ngrams),
#             }
#         }
        
#         if self.verbose:
#             print("\n" + "=" * 70)
#             print("📊 FINAL OUTPUT")
#             print("=" * 70)
#             print(f"  Query: '{query}'")
#             print(f"  Corrected: '{corrected_query}'")
#             print(f"  Time: {elapsed:.2f}ms")
#             print(f"  Stats: {output['stats']}")
#             print(f"  Source: {'RAM cache' if RAM_CACHE_AVAILABLE else 'Redis fallback'}")
        
#         return output
    
#     # =========================================================================
#     # Helper Methods
#     # =========================================================================
    
#     def _get_working_words(self, word_data: List[Dict[str, Any]]) -> List[str]:
#         """
#         Build the word list used for the actual search.

#         Only replaces a word when it is a confirmed genuine typo:
#           - status='corrected'     (was unknown, replaced — currently unused,
#                                     kept for forward compatibility)
#           - status='pos_corrected' (was in RAM hash with wrong POS, replaced)

#         Words with status 'unknown' or 'unknown_suggest' are NOT in the RAM
#         hash. Their original form is passed through so the semantic path
#         (graph → embedding → vector search) can still find results.
#         """
#         words = []
#         for wd in word_data:
#             if wd['status'] in ('corrected', 'pos_corrected') and wd.get('corrected'):
#                 words.append(wd['corrected'])
#             else:
#                 words.append(wd['word'])
#         return words
    
#     def _empty_result(self, query: str) -> Dict[str, Any]:
#         """Return empty result structure."""
#         return {
#             'query': query or '',
#             'corrected_query': '',
#             'processing_time_ms': 0,
#             'terms': [],
#             'ngrams': [],
#             'corrections': [],
#             'stats': {
#                 'total_words': 0,
#                 'valid_words': 0,
#                 'corrected_words': 0,
#                 'unknown_words': 0,
#                 'stopwords': 0,
#                 'ngram_count': 0,
#             }
#         }
    
#     def _process_single_word(self, word: str, start_time: float) -> Dict[str, Any]:
#         """
#         Handle single word queries.

#         PATCH: When a word is not in the RAM hash but a fuzzy suggestion
#         exists, the original word is kept as corrected_query so the semantic
#         path can search with it. The suggestion is returned with
#         correction_type='suggestion' for "did you mean?" display only.
#         """
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("📖 Single Word Query")
#             print("-" * 70)
        
#         word_lower = word.lower().strip()
        
#         # Check stopword
#         if word_lower in STOPWORDS:
#             elapsed = (time.perf_counter() - start_time) * 1000
#             return {
#                 'query': word,
#                 'corrected_query': word_lower,
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': True,
#                     'part_of_ngram': False,
#                     'pos': STOPWORDS[word_lower],
#                     'category': 'stopword',
#                     'description': '',
#                     'display': word_lower,
#                     'rank': 0,
#                     'match_count': 0,
#                 }],
#                 'ngrams': [],
#                 'corrections': [],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 1,
#                     'corrected_words': 0,
#                     'unknown_words': 0,
#                     'stopwords': 1,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Get matches from RAM cache
#         matches = self.cache.get_term_matches(word_lower)
        
#         if matches:
#             best = matches[0]
#             elapsed = (time.perf_counter() - start_time) * 1000
            
#             if self.verbose:
#                 print(f"  '{word_lower}' → Found ({len(matches)} matches)")
#                 print(f"  Selected: {best['category']} (rank={best['rank']})")
            
#             return {
#                 'query': word,
#                 'corrected_query': word_lower,
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'valid',
#                     'is_stopword': False,
#                     'part_of_ngram': False,
#                     'pos': best['pos'],
#                     'category': best['category'],
#                     'description': best['description'],
#                     'display': best['display'],
#                     'rank': best['rank'],
#                     'entity_type': best['entity_type'],
#                     'match_count': len(matches),
#                 }],
#                 'ngrams': [],
#                 'corrections': [],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 1,
#                     'corrected_words': 0,
#                     'unknown_words': 0,
#                     'stopwords': 0,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Not in RAM hash — try Redis fuzzy suggestion
#         suggestions = get_fuzzy_suggestions(word_lower, limit=5, max_distance=2)
#         elapsed = (time.perf_counter() - start_time) * 1000

#         if suggestions:
#             best = suggestions[0]

#             if self.verbose:
#                 print(f"  '{word_lower}' → Not in RAM hash. "
#                       f"Suggestion: '{best['term']}' (distance={best['distance']}). "
#                       f"Original kept for search.")

#             # PATCH: keep original word as corrected_query so the semantic
#             # path searches with it. Suggestion is a hint only.
#             return {
#                 'query': word,
#                 'corrected_query': word_lower,          # original — NOT best['term']
#                 'processing_time_ms': round(elapsed, 2),
#                 'terms': [{
#                     'position': 1,
#                     'word': word_lower,
#                     'status': 'unknown_suggest',         # hint only, not replaced
#                     'is_stopword': False,
#                     'part_of_ngram': False,
#                     'pos': 'unknown',
#                     'predicted_pos': 'noun',
#                     'category': '',
#                     'description': '',
#                     'display': word_lower,
#                     'rank': 0,
#                     'match_count': 0,
#                     'suggestion': best['term'],
#                     'suggestion_display': best['display'],
#                     'suggestion_distance': best['distance'],
#                 }],
#                 'ngrams': [],
#                 'corrections': [{
#                     'position': 1,
#                     'original': word_lower,
#                     'corrected': best['term'],
#                     'distance': best['distance'],
#                     'pos': best['pos'],
#                     'category': best['category'],
#                     'correction_type': 'suggestion',     # hint only
#                 }],
#                 'stats': {
#                     'total_words': 1,
#                     'valid_words': 0,
#                     'corrected_words': 0,
#                     'unknown_words': 1,
#                     'stopwords': 0,
#                     'ngram_count': 0,
#                 }
#             }
        
#         # Truly unknown — no suggestion available, pass original through
#         if self.verbose:
#             print(f"  '{word_lower}' → Unknown (no suggestions). Original passed through.")
        
#         return {
#             'query': word,
#             'corrected_query': word_lower,
#             'processing_time_ms': round(elapsed, 2),
#             'terms': [{
#                 'position': 1,
#                 'word': word_lower,
#                 'status': 'unknown',
#                 'is_stopword': False,
#                 'part_of_ngram': False,
#                 'pos': 'unknown',
#                 'predicted_pos': 'noun',
#                 'category': '',
#                 'description': '',
#                 'display': word_lower,
#                 'rank': 0,
#                 'match_count': 0,
#             }],
#             'ngrams': [],
#             'corrections': [],
#             'stats': {
#                 'total_words': 1,
#                 'valid_words': 0,
#                 'corrected_words': 0,
#                 'unknown_words': 1,
#                 'stopwords': 0,
#                 'ngram_count': 0,
#             }
#         }


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def process_query(query: str, verbose: bool = False) -> Dict[str, Any]:
#     """Main entry point - process a query through word discovery."""
#     wd = WordDiscovery(verbose=verbose)
#     return wd.process(query)


# def print_output(output: Dict[str, Any]) -> None:
#     """Print the output in a readable JSON format."""
#     print("\n" + "=" * 70)
#     print("📄 COMPLETE OUTPUT (JSON)")
#     print("=" * 70)
#     print(json.dumps(output, indent=2))


# # =============================================================================
# # MAIN - TEST SCRIPT
# # =============================================================================

# def main():
#     """Run test queries."""
#     import sys
    
#     test_queries = [
#         "where is africa located",
#         "where is african located",
#         "african food near me",
#         "when was slavery abolished",
#         "where is quickly located"
#     ]
    
#     if len(sys.argv) > 1:
#         query = ' '.join(sys.argv[1:])
#         test_queries = [query]
    
#     print("\n" + "#" * 70)
#     print("# WORD DISCOVERY V2 - TEST")
#     print(f"# RAM Cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
#     print("#" * 70)
    
#     for query in test_queries:
#         output = process_query(query, verbose=True)
#         print_output(output)
#         print("\n" + "=" * 70 + "\n")


# if __name__ == "__main__":
#     main()


"""
word_discovery_v3.py
====================
Word Discovery v3 — Complete profile builder.

Returns a COMPLETE profile with all entity classification, location
detection, and search term routing done. The bridge takes the profile
and builds Typesense params — no re-analysis.

PIPELINE (6 Steps):
    Step 1: Tokenize + RAM Hash Lookup + Inline Triggers
    Step 2: Context-Aware N-gram Resolution (rank comparison + signals)
    Step 3: POS Determination + Best Match Selection (merged from v2 3, 3.5, 4)
    Step 4: Batch Correct Unknowns (Redis fuzzy — only Redis step)
    Step 5: Re-check N-grams After Correction
    Step 6: Build Complete Profile (entities, locations, search terms, boosts)

TWO ARRAYS maintained throughout:
    query_array     — never changes. What the user typed. Used for search.
    corrected_array — updated when Step 4 finds corrections. "did you mean?"

PERFORMANCE:
    Steps 1-3, 5-6: RAM only (~0.01ms per lookup)
    Step 4: Redis only for unknown words (~50ms per correction)
    Typical query (no typos): ~1-3ms
    Query with 1-2 typos: ~50-100ms

WORD STATUS GUIDE:
    'resolved'       - found in RAM hash, match selected
    'stopword'       - recognized stopword with known POS
    'unknown'        - not in RAM hash, no fuzzy suggestion found;
                       original word passed through as-is
    'unknown_suggest'- not in RAM hash, fuzzy suggestion available;
                       original word KEPT for search, suggestion shown
                       as "did you mean?" hint only
    'pos_corrected'  - found in RAM hash but wrong POS; replaced with
                       a better-fitting word (genuine typo path)
    'corrected'      - was NOT in RAM hash but IS a genuine typo;
                       word has been replaced for the search
                       NOTE: currently unused — kept for forward compat
"""

import json
import time
import redis
from typing import Dict, Any, List, Optional, Tuple, Set
from decouple import config


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_LOCATION = config('REDIS_LOCATION')
REDIS_PORT = config('REDIS_PORT', cast=int)
REDIS_DB = config('REDIS_DB', default=0, cast=int)
REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
REDIS_USERNAME = config('REDIS_USERNAME', default='')

# N-gram rank boost multiplier for location n-grams when location_context is set
NGRAM_LOCATION_BOOST = 1.5


# =============================================================================
# REDIS CONNECTION (Only for spelling correction — Step 4)
# =============================================================================

class RedisClient:
    """Redis client for spelling correction only."""

    _client: Optional[redis.Redis] = None

    @classmethod
    def get_client(cls) -> Optional[redis.Redis]:
        if cls._client is not None:
            try:
                cls._client.ping()
                return cls._client
            except (redis.ConnectionError, redis.TimeoutError):
                cls._client = None

        try:
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
# RAM CACHE — Uses vocabulary_cache.py singleton
# =============================================================================

try:
    from .vocabulary_cache import vocab_cache
    RAM_CACHE_AVAILABLE = True
    print("✅ vocab_cache imported from .vocabulary_cache (RAM)")
except ImportError:
    try:
        from vocabulary_cache import vocab_cache
        RAM_CACHE_AVAILABLE = True
        print("✅ vocab_cache imported from vocabulary_cache (RAM)")
    except ImportError:
        RAM_CACHE_AVAILABLE = False
        vocab_cache = None
        print("⚠️ vocabulary_cache not available — falling back to Redis for all lookups")


class VocabCache:
    """
    RAM-based vocabulary cache wrapper.

    Uses vocab_cache singleton from vocabulary_cache.py for O(1) lookups.
    Falls back to Redis if RAM cache is not available.
    """

    def __init__(self):
        self._ram = vocab_cache if RAM_CACHE_AVAILABLE else None
        self._redis_client = None

    def _get_redis_client(self) -> Optional[redis.Redis]:
        """Get Redis client (fallback only)."""
        if self._redis_client is None:
            self._redis_client = RedisClient.get_client()
        return self._redis_client

    def get_term_matches(self, word: str) -> List[Dict[str, Any]]:
        """
        Get ALL matches for a word from RAM cache.
        Returns list of all category variants, sorted by rank desc.
        """
        # === RAM PATH (fast) ===
        if self._ram and self._ram.loaded:
            matches = self._ram.get_all_term_matches(word.lower().strip())
            if not matches:
                return []

            normalized = []
            for m in matches:
                rank = self._parse_rank(m.get('rank', 0))
                normalized.append({
                    'term': m.get('term', word.lower()),
                    'display': m.get('display', word.lower()),
                    'category': m.get('category', ''),
                    'description': m.get('description', ''),
                    'pos': self._normalize_pos(m.get('pos')),
                    'entity_type': m.get('entity_type', 'unigram'),
                    'rank': rank,
                })

            normalized.sort(key=lambda x: x['rank'], reverse=True)
            return normalized

        # === REDIS FALLBACK (slow) ===
        client = self._get_redis_client()
        if not client:
            return []

        word_lower = word.lower().strip()
        pattern = f"term:{word_lower}:*"

        try:
            keys = client.keys(pattern)
            if not keys:
                return []

            matches = []
            for key in keys:
                metadata = client.hgetall(key)
                if metadata:
                    rank = self._parse_rank(metadata.get('rank', 0))
                    matches.append({
                        'term': metadata.get('term', word_lower),
                        'display': metadata.get('display', word_lower),
                        'category': metadata.get('category', ''),
                        'description': metadata.get('description', ''),
                        'pos': self._normalize_pos(metadata.get('pos')),
                        'entity_type': metadata.get('entity_type', 'unigram'),
                        'rank': rank,
                    })

            matches.sort(key=lambda x: x['rank'], reverse=True)
            return matches

        except Exception as e:
            print(f"Error getting term matches for '{word}': {e}")
            return []

    def get_ngram(self, words: List[str]) -> Optional[Dict[str, Any]]:
        """
        Check if words form an n-gram (bigram, trigram, quadgram).
        Uses RAM cache first, falls back to Redis.
        """
        if len(words) < 2:
            return None

        # === RAM PATH (fast) ===
        if self._ram and self._ram.loaded:
            words_lower = [w.lower() for w in words]
            metadata = None

            if len(words_lower) == 2:
                metadata = self._ram.get_bigram(words_lower[0], words_lower[1])
            elif len(words_lower) == 3:
                metadata = self._ram.get_trigram(words_lower[0], words_lower[1], words_lower[2])
            elif len(words_lower) == 4:
                metadata = self._ram.get_quadgram(words_lower[0], words_lower[1], words_lower[2], words_lower[3])

            if not metadata:
                return None

            phrase = ' '.join(words_lower)
            ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'

            return {
                'key': f"term:{phrase}:{metadata.get('category', '')}",
                'term': metadata.get('term', phrase),
                'display': metadata.get('display', phrase.title()),
                'category': metadata.get('category', ''),
                'description': metadata.get('description', ''),
                'pos': self._normalize_pos(metadata.get('pos')),
                'entity_type': metadata.get('entity_type', ngram_type),
                'rank': self._parse_rank(metadata.get('rank', 0)),
                'words': words,
                'ngram_type': ngram_type,
            }

        # === REDIS FALLBACK (slow) ===
        client = self._get_redis_client()
        if not client:
            return None

        phrase = ' '.join(w.lower() for w in words)
        phrase_underscore = '_'.join(w.lower() for w in words)

        pattern = f"term:{phrase_underscore}:*"
        try:
            keys = client.keys(pattern)
        except Exception:
            keys = []

        if not keys:
            pattern = f"term:{phrase}:*"
            try:
                keys = client.keys(pattern)
            except Exception:
                keys = []

        if not keys:
            return None

        try:
            key = keys[0]
            metadata = client.hgetall(key)

            if metadata:
                rank = self._parse_rank(metadata.get('rank', 0))
                ngram_type = 'bigram' if len(words) == 2 else 'trigram' if len(words) == 3 else 'quadgram'

                return {
                    'key': key,
                    'term': metadata.get('term', phrase),
                    'display': metadata.get('display', phrase.title()),
                    'category': metadata.get('category', ''),
                    'description': metadata.get('description', ''),
                    'pos': self._normalize_pos(metadata.get('pos')),
                    'entity_type': metadata.get('entity_type', ngram_type),
                    'rank': rank,
                    'words': words,
                    'ngram_type': ngram_type,
                }

            return None

        except Exception as e:
            print(f"Error getting ngram for '{phrase}': {e}")
            return None

    def _normalize_pos(self, pos_value: Any) -> str:
        """Normalize POS value from various formats."""
        if pos_value is None:
            return 'unknown'

        if isinstance(pos_value, str):
            if pos_value.startswith('['):
                try:
                    parsed = json.loads(pos_value)
                    if isinstance(parsed, list) and parsed:
                        return str(parsed[0]).lower()
                except json.JSONDecodeError:
                    pass

                try:
                    fixed = pos_value.replace("'", '"')
                    parsed = json.loads(fixed)
                    if isinstance(parsed, list) and parsed:
                        return str(parsed[0]).lower()
                except json.JSONDecodeError:
                    pass

                if pos_value.startswith("['") and pos_value.endswith("']"):
                    inner = pos_value[2:-2]
                    return inner.lower()

            return pos_value.lower()

        if isinstance(pos_value, list):
            return str(pos_value[0]).lower() if pos_value else 'unknown'

        return str(pos_value).lower()

    def _parse_rank(self, rank_value: Any) -> int:
        """Parse rank to integer."""
        try:
            return int(float(rank_value))
        except (ValueError, TypeError):
            return 0


# Global cache instance
vocab_cache_wrapper = VocabCache()


# =============================================================================
# STOPWORDS
# =============================================================================

STOPWORDS = {
    # Determiners/Articles
    "the": "determiner", "a": "article", "an": "article",
    "this": "determiner", "that": "determiner", "these": "determiner", "those": "determiner",
    "my": "determiner", "your": "determiner", "his": "determiner", "her": "determiner",
    "its": "determiner", "our": "determiner", "their": "determiner",
    "some": "determiner", "any": "determiner", "no": "determiner",
    "every": "determiner", "each": "determiner", "all": "determiner",

    # Prepositions
    "in": "preposition", "on": "preposition", "at": "preposition", "to": "preposition",
    "for": "preposition", "of": "preposition", "with": "preposition", "by": "preposition",
    "from": "preposition", "about": "preposition", "into": "preposition", "through": "preposition",
    "during": "preposition", "before": "preposition", "after": "preposition",
    "above": "preposition", "below": "preposition", "between": "preposition",
    "under": "preposition", "over": "preposition", "near": "preposition",

    # Conjunctions
    "and": "conjunction", "or": "conjunction", "but": "conjunction",
    "nor": "conjunction", "so": "conjunction", "yet": "conjunction",

    # Pronouns
    "i": "pronoun", "you": "pronoun", "he": "pronoun", "she": "pronoun",
    "it": "pronoun", "we": "pronoun", "they": "pronoun",
    "me": "pronoun", "him": "pronoun", "her": "pronoun", "us": "pronoun", "them": "pronoun",
    "who": "pronoun", "whom": "pronoun", "what": "pronoun", "which": "pronoun",
    "whose": "pronoun", "whoever": "pronoun", "whatever": "pronoun",

    # Be verbs
    "is": "be", "are": "be", "was": "be", "were": "be",
    "be": "be", "been": "be", "being": "be",
    "am": "be",

    # Auxiliary/Modal verbs
    "have": "auxiliary", "has": "auxiliary", "had": "auxiliary",
    "do": "auxiliary", "does": "auxiliary", "did": "auxiliary",
    "will": "modal", "would": "modal", "could": "modal", "should": "modal",
    "may": "modal", "might": "modal", "must": "modal", "can": "modal",

    # Other common words
    "not": "negation", "no": "negation",
    "as": "conjunction", "if": "conjunction", "when": "conjunction",
    "than": "conjunction", "because": "conjunction", "while": "conjunction",
    "where": "adverb", "how": "adverb", "why": "adverb",
    "very": "adverb", "just": "adverb", "also": "adverb",
    "only": "adverb", "even": "adverb", "still": "adverb",
    "then": "adverb", "now": "adverb", "here": "adverb", "there": "adverb",
}

# Location context signal words — when these stopwords appear,
# the NEXT non-stopword gets location_context=True
LOCATION_SIGNAL_WORDS = frozenset({
    'in', 'near', 'around', 'from', 'at', 'outside', 'within',
})


# =============================================================================
# ABBREVIATION TRIGGERS (NEW in v3)
# =============================================================================
# O(1) lookup during tokenization. When triggered, word is resolved
# with the full hash entry as if found directly.

ABBREVIATION_TRIGGERS = {
    # --- US Cities ---
    'atl': 'atlanta',
    'chi': 'chicago',
    'dc': 'washington dc',
    'la': 'los angeles',
    'philly': 'philadelphia',
    'nola': 'new orleans',
    'bmore': 'baltimore',
    'nyc': 'new york city',
    'sf': 'san francisco',
    'stl': 'st louis',
    'kc': 'kansas city',
    'det': 'detroit',
    'hou': 'houston',
    'dal': 'dallas',
    'den': 'denver',
    'mem': 'memphis',
    'jax': 'jacksonville',

    # --- US States ---
    'nc': 'north carolina',
    'sc': 'south carolina',
    'ny': 'new york',
    'ga': 'georgia',
    'fl': 'florida',
    'tx': 'texas',
    'va': 'virginia',
    'md': 'maryland',
    'al': 'alabama',
    'tn': 'tennessee',
    'ms': 'mississippi',
    'nj': 'new jersey',
    'ct': 'connecticut',
    'pa': 'pennsylvania',
    'oh': 'ohio',
    'il': 'illinois',
    'mi': 'michigan',
    'wi': 'wisconsin',
    'mn': 'minnesota',
    'co': 'colorado',
    'az': 'arizona',
    'wa': 'washington',
    'or': 'oregon',
    'ca': 'california',
    'ma': 'massachusetts',
    'in': 'indiana',  # NOTE: also a stopword — handled specially in Step 1
}

# Words where the abbreviation only triggers when NOT used as a stopword.
# "in" is both a preposition and Indiana's abbreviation.
# We only expand "in" → "indiana" when it appears to be a location context
# (e.g., end of query: "restaurants in" is a preposition, but
#  "born in" is ambiguous). For safety, we skip "in" entirely —
# it's always a preposition in search queries.
ABBREVIATION_SKIP_IF_STOPWORD = frozenset({'in', 'or'})


# =============================================================================
# GRAMMAR RULES FOR POS PREDICTION
# =============================================================================

GRAMMAR_RULES = {
    # =========================================================================
    # BOTH NEIGHBORS KNOWN
    # =========================================================================

    ("determiner", "noun"): [("adjective", 0.95), ("noun", 0.60)],
    ("determiner", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
    ("determiner", "verb"): [("noun", 0.90), ("adjective", 0.65)],
    ("determiner", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
    ("determiner", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
    ("determiner", "conjunction"): [("noun", 0.85)],
    ("determiner", "end"): [("noun", 0.95), ("adjective", 0.70)],

    ("article", "noun"): [("adjective", 0.95), ("noun", 0.55)],
    ("article", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
    ("article", "verb"): [("noun", 0.90), ("adjective", 0.65)],
    ("article", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
    ("article", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
    ("article", "conjunction"): [("noun", 0.85)],
    ("article", "end"): [("noun", 0.95), ("adjective", 0.70)],

    ("adjective", "noun"): [("adjective", 0.90), ("noun", 0.50)],
    ("adjective", "adjective"): [("adjective", 0.85), ("noun", 0.65), ("adverb", 0.50)],
    ("adjective", "verb"): [("noun", 0.90), ("proper_noun", 0.75)],
    ("adjective", "preposition"): [("noun", 0.90), ("proper_noun", 0.75)],
    ("adjective", "adverb"): [("noun", 0.85), ("verb", 0.65)],
    ("adjective", "conjunction"): [("noun", 0.90)],
    ("adjective", "end"): [("noun", 0.95), ("proper_noun", 0.80)],

    ("noun", "noun"): [("verb", 0.75), ("adjective", 0.65), ("noun", 0.50)],
    ("noun", "adjective"): [("verb", 0.90), ("be", 0.80), ("adverb", 0.55)],
    ("noun", "adverb"): [("verb", 0.90), ("be", 0.70)],
    ("noun", "preposition"): [("verb", 0.85), ("noun", 0.55)],
    ("noun", "verb"): [("adverb", 0.85), ("noun", 0.60)],
    ("noun", "conjunction"): [("verb", 0.80), ("noun", 0.60)],
    ("noun", "determiner"): [("verb", 0.90), ("be", 0.70)],
    ("noun", "article"): [("verb", 0.90), ("be", 0.70)],
    ("noun", "pronoun"): [("verb", 0.90)],
    ("noun", "end"): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],

    ("proper_noun", "noun"): [("verb", 0.80), ("noun", 0.60)],
    ("proper_noun", "adjective"): [("verb", 0.90), ("be", 0.80)],
    ("proper_noun", "adverb"): [("verb", 0.90)],
    ("proper_noun", "preposition"): [("verb", 0.85)],
    ("proper_noun", "verb"): [("adverb", 0.85), ("proper_noun", 0.60)],
    ("proper_noun", "conjunction"): [("verb", 0.80)],
    ("proper_noun", "end"): [("verb", 0.75), ("proper_noun", 0.70)],

    ("verb", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("adverb", 0.60)],
    ("verb", "adjective"): [("adverb", 0.90), ("noun", 0.55)],
    ("verb", "adverb"): [("adverb", 0.85), ("noun", 0.60)],
    ("verb", "verb"): [("adverb", 0.85), ("noun", 0.60), ("preposition", 0.50)],
    ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.75), ("pronoun", 0.55)],
    ("verb", "conjunction"): [("noun", 0.85), ("adverb", 0.65)],
    ("verb", "determiner"): [("adverb", 0.80), ("noun", 0.65)],
    ("verb", "article"): [("adverb", 0.80), ("noun", 0.65)],
    ("verb", "pronoun"): [("adverb", 0.80), ("preposition", 0.65)],
    ("verb", "end"): [("noun", 0.85), ("adverb", 0.75), ("proper_noun", 0.65)],

    ("preposition", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
    ("preposition", "adjective"): [("adverb", 0.90), ("adjective", 0.65)],
    ("preposition", "adverb"): [("adjective", 0.80), ("noun", 0.70)],
    ("preposition", "verb"): [("noun", 0.90), ("proper_noun", 0.75), ("pronoun", 0.60)],
    ("preposition", "preposition"): [("noun", 0.85), ("proper_noun", 0.75)],
    ("preposition", "conjunction"): [("noun", 0.85), ("proper_noun", 0.70)],
    ("preposition", "determiner"): [("noun", 0.80), ("adjective", 0.65)],
    ("preposition", "article"): [("noun", 0.80), ("adjective", 0.65)],
    ("preposition", "pronoun"): [("noun", 0.75), ("verb", 0.60)],
    ("preposition", "end"): [("noun", 0.95), ("proper_noun", 0.85), ("adjective", 0.60)],

    ("pronoun", "noun"): [("verb", 0.95), ("be", 0.80)],
    ("pronoun", "adjective"): [("verb", 0.95), ("be", 0.85), ("adverb", 0.55)],
    ("pronoun", "adverb"): [("verb", 0.95), ("be", 0.75)],
    ("pronoun", "preposition"): [("verb", 0.90), ("be", 0.70)],
    ("pronoun", "verb"): [("adverb", 0.85), ("modal", 0.70)],
    ("pronoun", "conjunction"): [("verb", 0.85)],
    ("pronoun", "end"): [("verb", 0.90), ("noun", 0.60)],

    ("be", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
    ("be", "adjective"): [("adverb", 0.95), ("adjective", 0.60)],
    ("be", "adverb"): [("adjective", 0.85), ("verb", 0.70)],
    ("be", "verb"): [("adverb", 0.90), ("noun", 0.55)],
    ("be", "preposition"): [("noun", 0.90), ("adverb", 0.70)],
    ("be", "conjunction"): [("adjective", 0.80), ("noun", 0.65)],
    ("be", "end"): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.60)],

    ("adverb", "noun"): [("adjective", 0.90), ("verb", 0.65)],
    ("adverb", "adjective"): [("adverb", 0.85), ("adjective", 0.70)],
    ("adverb", "adverb"): [("adverb", 0.80), ("verb", 0.65)],
    ("adverb", "verb"): [("adverb", 0.90), ("noun", 0.50)],
    ("adverb", "preposition"): [("verb", 0.85), ("noun", 0.65)],
    ("adverb", "conjunction"): [("verb", 0.80), ("adjective", 0.65)],
    ("adverb", "end"): [("adjective", 0.85), ("verb", 0.75), ("noun", 0.60)],

    ("conjunction", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("verb", 0.55)],
    ("conjunction", "adjective"): [("adverb", 0.85), ("noun", 0.65)],
    ("conjunction", "adverb"): [("noun", 0.80), ("verb", 0.70)],
    ("conjunction", "verb"): [("noun", 0.90), ("pronoun", 0.75)],
    ("conjunction", "preposition"): [("noun", 0.85), ("pronoun", 0.70)],
    ("conjunction", "conjunction"): [("noun", 0.80)],
    ("conjunction", "end"): [("noun", 0.90), ("proper_noun", 0.75)],

    ("modal", "noun"): [("verb", 0.90), ("adverb", 0.65)],
    ("modal", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
    ("modal", "adverb"): [("verb", 0.95)],
    ("modal", "verb"): [("adverb", 0.90)],
    ("modal", "preposition"): [("verb", 0.85)],
    ("modal", "end"): [("verb", 0.95), ("noun", 0.55)],

    ("auxiliary", "noun"): [("verb", 0.90), ("adjective", 0.65)],
    ("auxiliary", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
    ("auxiliary", "adverb"): [("verb", 0.95)],
    ("auxiliary", "verb"): [("adverb", 0.90)],
    ("auxiliary", "preposition"): [("verb", 0.85)],
    ("auxiliary", "end"): [("verb", 0.90), ("noun", 0.60)],

    ("participle", "noun"): [("adjective", 0.85), ("noun", 0.65)],
    ("participle", "adjective"): [("adverb", 0.85), ("noun", 0.60)],
    ("participle", "adverb"): [("noun", 0.80), ("adjective", 0.65)],
    ("participle", "verb"): [("adverb", 0.80), ("noun", 0.65)],
    ("participle", "preposition"): [("noun", 0.85), ("adverb", 0.65)],
    ("participle", "end"): [("noun", 0.90), ("adverb", 0.70)],

    ("gerund", "noun"): [("adjective", 0.85), ("noun", 0.70)],
    ("gerund", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
    ("gerund", "adverb"): [("noun", 0.80)],
    ("gerund", "verb"): [("adverb", 0.80), ("noun", 0.60)],
    ("gerund", "preposition"): [("noun", 0.85)],
    ("gerund", "end"): [("noun", 0.90), ("adverb", 0.65)],

    ("negation", "noun"): [("adjective", 0.85), ("verb", 0.75)],
    ("negation", "adjective"): [("adverb", 0.90)],
    ("negation", "adverb"): [("verb", 0.85), ("adjective", 0.70)],
    ("negation", "verb"): [("adverb", 0.90)],
    ("negation", "preposition"): [("verb", 0.80)],
    ("negation", "end"): [("verb", 0.85), ("adjective", 0.70)],

    ("quantifier", "noun"): [("adjective", 0.90), ("noun", 0.60)],
    ("quantifier", "adjective"): [("adverb", 0.85), ("adjective", 0.65)],
    ("quantifier", "adverb"): [("adjective", 0.80)],
    ("quantifier", "verb"): [("noun", 0.85)],
    ("quantifier", "preposition"): [("noun", 0.85)],
    ("quantifier", "end"): [("noun", 0.95), ("adjective", 0.65)],

    ("numeral", "noun"): [("adjective", 0.85), ("noun", 0.70)],
    ("numeral", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
    ("numeral", "adverb"): [("noun", 0.80)],
    ("numeral", "verb"): [("noun", 0.85)],
    ("numeral", "preposition"): [("noun", 0.85)],
    ("numeral", "end"): [("noun", 0.95)],

    # =========================================================================
    # ONLY LEFT NEIGHBOR KNOWN
    # =========================================================================

    ("determiner", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
    ("article", None): [("noun", 0.90), ("adjective", 0.80), ("proper_noun", 0.60)],
    ("adjective", None): [("noun", 0.95), ("proper_noun", 0.75)],
    ("noun", None): [("verb", 0.80), ("noun", 0.65), ("proper_noun", 0.55)],
    ("proper_noun", None): [("verb", 0.80), ("proper_noun", 0.70), ("noun", 0.55)],
    ("verb", None): [("noun", 0.80), ("adverb", 0.75), ("adjective", 0.60), ("proper_noun", 0.50)],
    ("preposition", None): [("noun", 0.90), ("proper_noun", 0.85), ("adjective", 0.60)],
    ("pronoun", None): [("verb", 0.95), ("be", 0.80), ("modal", 0.65)],
    ("be", None): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.65), ("verb", 0.55)],
    ("adverb", None): [("adjective", 0.85), ("verb", 0.75), ("adverb", 0.60)],
    ("conjunction", None): [("noun", 0.85), ("pronoun", 0.75), ("proper_noun", 0.65), ("verb", 0.55)],
    ("modal", None): [("verb", 0.95), ("adverb", 0.65)],
    ("auxiliary", None): [("verb", 0.95), ("adverb", 0.65)],
    ("participle", None): [("noun", 0.85), ("adverb", 0.70)],
    ("gerund", None): [("noun", 0.90), ("adverb", 0.60)],
    ("negation", None): [("verb", 0.85), ("adjective", 0.75), ("adverb", 0.60)],
    ("quantifier", None): [("noun", 0.95), ("adjective", 0.70)],
    ("numeral", None): [("noun", 0.95), ("adjective", 0.60)],

    # =========================================================================
    # ONLY RIGHT NEIGHBOR KNOWN
    # =========================================================================

    (None, "noun"): [("adjective", 0.95), ("determiner", 0.85), ("noun", 0.60), ("proper_noun", 0.50)],
    (None, "proper_noun"): [("adjective", 0.80), ("preposition", 0.75), ("verb", 0.65)],
    (None, "adjective"): [("adverb", 0.90), ("adjective", 0.70), ("determiner", 0.60)],
    (None, "adverb"): [("verb", 0.85), ("adverb", 0.75), ("be", 0.60)],
    (None, "verb"): [("noun", 0.90), ("pronoun", 0.85), ("adverb", 0.70), ("proper_noun", 0.60)],
    (None, "preposition"): [("noun", 0.90), ("verb", 0.80), ("proper_noun", 0.65)],
    (None, "determiner"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
    (None, "article"): [("verb", 0.90), ("preposition", 0.75), ("conjunction", 0.60)],
    (None, "pronoun"): [("verb", 0.85), ("preposition", 0.75), ("conjunction", 0.65)],
    (None, "conjunction"): [("noun", 0.90), ("verb", 0.75), ("adjective", 0.60)],
    (None, "be"): [("noun", 0.90), ("pronoun", 0.85), ("proper_noun", 0.70)],
    (None, "modal"): [("noun", 0.90), ("pronoun", 0.85)],
    (None, "auxiliary"): [("noun", 0.90), ("pronoun", 0.85)],
    (None, "participle"): [("be", 0.85), ("adverb", 0.75), ("noun", 0.60)],
    (None, "gerund"): [("preposition", 0.80), ("verb", 0.75), ("be", 0.65)],
    (None, "negation"): [("verb", 0.85), ("be", 0.80), ("modal", 0.70)],
    (None, "quantifier"): [("preposition", 0.80), ("verb", 0.70)],
    (None, "numeral"): [("preposition", 0.80), ("verb", 0.70), ("determiner", 0.60)],
    (None, "end"): [("noun", 0.90), ("proper_noun", 0.80), ("verb", 0.65), ("adjective", 0.55)],

    # =========================================================================
    # SPECIAL CASES — Start of query
    # =========================================================================

    ("start", "noun"): [("adjective", 0.90), ("determiner", 0.85)],
    ("start", "adjective"): [("adverb", 0.85), ("determiner", 0.75)],
    ("start", "verb"): [("noun", 0.90), ("pronoun", 0.80)],
    ("start", "adverb"): [("verb", 0.80), ("adjective", 0.70)],
    ("start", "preposition"): [("noun", 0.85), ("verb", 0.70)],
    ("start", "end"): [("noun", 0.90), ("proper_noun", 0.85), ("verb", 0.60)],
}


# POS compatibility for matching predictions to candidates
POS_COMPATIBILITY = {
    'noun': {'noun', 'proper_noun'},
    'proper_noun': {'noun', 'proper_noun'},
    'verb': {'verb', 'participle', 'gerund'},
    'adjective': {'adjective', 'participle'},
    'adverb': {'adverb'},
    'pronoun': {'pronoun'},
    'determiner': {'determiner', 'article'},
    'article': {'article', 'determiner'},
    'preposition': {'preposition'},
    'conjunction': {'conjunction'},
}


# =============================================================================
# SUFFIX POS RULES (for Step 3 suffix refinement)
# =============================================================================

SUFFIX_POS_RULES = {
    'ing': [('gerund', 0.80), ('adjective', 0.75), ('verb', 0.70), ('noun', 0.50)],
    'ed': [('verb', 0.85), ('adjective', 0.75), ('participle', 0.70)],
    'ly': [('adverb', 0.95), ('adjective', 0.40)],
    'tion': [('noun', 0.95)],
    'sion': [('noun', 0.95)],
    'ment': [('noun', 0.95)],
    'ness': [('noun', 0.95)],
    'ity': [('noun', 0.95)],
    'ence': [('noun', 0.90)],
    'ance': [('noun', 0.90)],
    'ure': [('noun', 0.80)],
    'ism': [('noun', 0.95)],
    'ist': [('noun', 0.90)],
    'ery': [('noun', 0.85)],
    'ory': [('noun', 0.80), ('adjective', 0.60)],
    'age': [('noun', 0.80)],
    'ship': [('noun', 0.95)],
    'dom': [('noun', 0.90)],
    'hood': [('noun', 0.95)],
    'ling': [('noun', 0.80)],
    'ful': [('adjective', 0.95)],
    'less': [('adjective', 0.95)],
    'able': [('adjective', 0.90)],
    'ible': [('adjective', 0.90)],
    'ous': [('adjective', 0.95)],
    'ious': [('adjective', 0.95)],
    'ive': [('adjective', 0.90)],
    'al': [('adjective', 0.85), ('noun', 0.50)],
    'ial': [('adjective', 0.90)],
    'ical': [('adjective', 0.90)],
    'ish': [('adjective', 0.85)],
    'ern': [('adjective', 0.80)],
    'ese': [('adjective', 0.80), ('noun', 0.70)],
    'ian': [('adjective', 0.80), ('noun', 0.75)],
    'ean': [('adjective', 0.80)],
    'ic': [('adjective', 0.85)],
    'est': [('adjective', 0.85)],
    'ent': [('adjective', 0.75), ('noun', 0.65)],
    'ant': [('adjective', 0.75), ('noun', 0.65)],
    'ify': [('verb', 0.95)],
    'ize': [('verb', 0.95)],
    'ise': [('verb', 0.90)],
    'ate': [('verb', 0.80), ('adjective', 0.60), ('noun', 0.50)],
    'en': [('verb', 0.70), ('adjective', 0.60)],
    'er': [('noun', 0.75), ('adjective', 0.65)],
    'or': [('noun', 0.80)],
    'ee': [('noun', 0.85)],
    'eer': [('noun', 0.85)],
}

SUFFIX_EXCEPTIONS = frozenset({
    'ring', 'king', 'sing', 'bring', 'thing', 'string', 'spring',
    'wing', 'swing', 'sting', 'cling', 'fling', 'sling', 'wring',
    'bling', 'ding', 'ping', 'zing', 'ming', 'bing', 'ling',
    'beijing', 'sterling', 'viking', 'darling', 'ceiling',
    'feeling', 'dealing', 'healing', 'meaning', 'evening',
    'bed', 'red', 'shed', 'led', 'fed', 'wed', 'sled',
    'fly', 'ply', 'sly', 'holy', 'ugly', 'bully', 'belly',
    'lily', 'jelly', 'jolly', 'rally', 'ally', 'tally',
    'family', 'italy', 'july', 'daily', 'early',
    'water', 'after', 'under', 'over', 'never', 'ever', 'other',
    'rather', 'either', 'neither', 'whether', 'together',
    'butter', 'letter', 'matter', 'better', 'bitter', 'litter',
    'dinner', 'inner', 'upper', 'lower', 'power', 'flower',
    'tower', 'shower', 'silver', 'river', 'liver', 'cover',
    'fever', 'clever', 'finger', 'ginger', 'tiger', 'anger',
    'danger', 'hunger', 'umber', 'number', 'member', 'timber',
    'order', 'border', 'murder', 'wonder', 'thunder', 'super',
    'paper', 'copper', 'proper', 'whisper', 'chapter', 'master',
    'sister', 'mister', 'winter', 'center', 'corner', 'monster',
    'soccer', 'cancer', 'rubber', 'hammer', 'summer', 'manner',
    'animal', 'hospital', 'capital', 'metal', 'total', 'eral',
    'canal', 'eral', 'general', 'mineral', 'festival', 'signal',
    'journal', 'rival', 'survival', 'arrival', 'proposal',
    'awful',
    'age', 'page', 'sage', 'cage', 'rage', 'wage', 'stage',
    'cement', 'moment', 'comment', 'segment', 'element',
    'music', 'magic', 'basic', 'logic', 'topic', 'panic',
    'fabric', 'garlic', 'picnic', 'traffic', 'public',
    'west', 'east', 'nest', 'rest', 'test', 'best', 'chest',
    'quest', 'guest', 'forest', 'harvest', 'interest', 'protest',
    'request', 'suggest', 'modest', 'honest', 'invest',
    'gate', 'late', 'mate', 'date', 'fate', 'rate', 'state',
    'plate', 'private', 'climate', 'chocolate', 'senate',
    'debate', 'estate', 'ultimate', 'intimate', 'accurate',
    'delicate', 'separate', 'desperate', 'moderate', 'adequate',
    'fish', 'dish', 'wish', 'finish', 'publish', 'polish',
    'vanish', 'perish', 'cherish', 'nourish', 'flourish',
})

MIN_SUFFIX_WORD_LENGTH = 4


# =============================================================================
# ENTITY CLASSIFICATION CATEGORIES (for Step 6 profile building)
# =============================================================================

PERSON_CATEGORIES = frozenset({
    'person', 'historical figure', 'celebrity', 'athlete', 'politician',
    'musician', 'actor', 'author', 'artist', 'scientist', 'activist',
    'entrepreneur', 'religious figure', 'philosopher',
})

ORGANIZATION_CATEGORIES = frozenset({
    'organization', 'company', 'business', 'brand', 'hbcu',
    'university', 'college', 'nonprofit', 'agency', 'institution',
    'corporation', 'association',
})

LOCATION_CATEGORIES = frozenset({
    'us city', 'us state', 'us county', 'city', 'state', 'country',
    'location', 'continent', 'region', 'neighborhood', 'territory',
})

CITY_CATEGORIES = frozenset({
    'us city', 'city',
})

STATE_CATEGORIES = frozenset({
    'us state', 'state',
})

KEYWORD_CATEGORIES = frozenset({
    'keyword', 'topic', 'primary keyword', 'concept', 'theme',
    'cuisine', 'food', 'service', 'industry', 'profession',
    'activity', 'sport', 'style', 'genre', 'tradition',
})

MEDIA_CATEGORIES = frozenset({
    'song', 'movie', 'album', 'book', 'tv show', 'film',
    'documentary', 'podcast', 'series', 'show',
})

# POS values that qualify a term as a search term
SEARCHABLE_POS = frozenset({
    'noun', 'proper_noun', 'unknown',
})


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def damerau_levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Damerau-Levenshtein distance."""
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


def normalize_pos_string(pos_value: any) -> str:
    """Normalize POS value from various formats to clean string."""
    if pos_value is None:
        return 'unknown'

    if isinstance(pos_value, str):
        if pos_value.startswith('['):
            try:
                parsed = json.loads(pos_value)
                if isinstance(parsed, list) and parsed:
                    return str(parsed[0]).lower()
            except json.JSONDecodeError:
                pass

            try:
                fixed = pos_value.replace("'", '"')
                parsed = json.loads(fixed)
                if isinstance(parsed, list) and parsed:
                    return str(parsed[0]).lower()
            except json.JSONDecodeError:
                pass

            if pos_value.startswith("['") and pos_value.endswith("']"):
                inner = pos_value[2:-2]
                return inner.lower()

        return pos_value.lower().strip()

    if isinstance(pos_value, list):
        return str(pos_value[0]).lower() if pos_value else 'unknown'

    return str(pos_value).lower()


def is_pos_compatible(candidate_pos: str, predicted_pos: str) -> bool:
    """Check if candidate POS is compatible with predicted POS."""
    candidate_pos = normalize_pos_string(candidate_pos)
    predicted_pos = normalize_pos_string(predicted_pos)

    if candidate_pos == predicted_pos:
        return True

    compatible_set = POS_COMPATIBILITY.get(predicted_pos, {predicted_pos})
    return candidate_pos in compatible_set


def get_fuzzy_suggestions_batch(
        words: List[str],
        limit: int = 10,
        max_distance: int = 2
    ) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get fuzzy suggestions for ALL words in ONE Redis pipeline call.
    Returns dict of {word: [suggestions]}
    """
    client = RedisClient.get_client()
    if not client:
        return {word: [] for word in words}

    unique_words = list({w.lower().strip() for w in words if len(w.strip()) >= 2})

    if not unique_words:
        return {}

    pipe = client.pipeline(transaction=False)

    for word in unique_words:
        query = f"%{word}%"
        pipe.execute_command(
            'FT.SEARCH', 'terms_idx', query,
            'SORTBY', 'rank', 'DESC',
            'LIMIT', '0', str(limit * 3)
        )

    try:
        all_results = pipe.execute()
    except Exception as e:
        print(f"Pipeline batch error: {e}")
        return {word: [] for word in unique_words}

    batch_suggestions = {}

    for word, result in zip(unique_words, all_results):
        suggestions = []

        if not result or len(result) <= 1:
            batch_suggestions[word] = []
            continue

        i = 1
        while i < len(result):
            key = result[i]
            fields = result[i + 1] if i + 1 < len(result) else []

            metadata = {}
            for j in range(0, len(fields), 2):
                if j + 1 < len(fields):
                    metadata[fields[j]] = fields[j + 1]

            if metadata:
                term = metadata.get('term', '')
                distance = damerau_levenshtein_distance(word, term.lower())

                if distance <= max_distance and distance > 0:
                    rank = metadata.get('rank', 0)
                    try:
                        rank = int(float(rank))
                    except (ValueError, TypeError):
                        rank = 0

                    pos = metadata.get('pos', 'unknown')
                    if isinstance(pos, str) and pos.startswith('['):
                        try:
                            parsed = json.loads(pos)
                            pos = parsed[0] if parsed else 'unknown'
                        except:
                            pass
                    pos = str(pos).lower()

                    suggestions.append({
                        'term': term,
                        'display': metadata.get('display', term),
                        'category': metadata.get('category', ''),
                        'description': metadata.get('description', ''),
                        'pos': pos,
                        'entity_type': metadata.get('entity_type', 'unigram'),
                        'rank': rank,
                        'distance': distance,
                    })

            i += 2

        # If % fuzzy found nothing, try %% (wider fuzzy)
        if not suggestions:
            try:
                wider_result = client.execute_command(
                    'FT.SEARCH', 'terms_idx', f"%%{word}%%",
                    'SORTBY', 'rank', 'DESC',
                    'LIMIT', '0', str(limit * 3)
                )
                i = 1
                while i < len(wider_result):
                    fields = wider_result[i + 1] if i + 1 < len(wider_result) else []
                    metadata = {}
                    for j in range(0, len(fields), 2):
                        if j + 1 < len(fields):
                            metadata[fields[j]] = fields[j + 1]
                    if metadata:
                        term = metadata.get('term', '')
                        distance = damerau_levenshtein_distance(word, term.lower())
                        if distance <= max_distance and distance > 0:
                            rank = metadata.get('rank', 0)
                            try:
                                rank = int(float(rank))
                            except (ValueError, TypeError):
                                rank = 0
                            pos = metadata.get('pos', 'unknown')
                            if isinstance(pos, str) and pos.startswith('['):
                                try:
                                    parsed = json.loads(pos)
                                    pos = parsed[0] if parsed else 'unknown'
                                except:
                                    pass
                            pos = str(pos).lower()
                            suggestions.append({
                                'term': term,
                                'display': metadata.get('display', term),
                                'category': metadata.get('category', ''),
                                'description': metadata.get('description', ''),
                                'pos': pos,
                                'entity_type': metadata.get('entity_type', 'unigram'),
                                'rank': rank,
                                'distance': distance,
                            })
                    i += 2
            except Exception as e:
                print(f"Wider fuzzy search error for '{word}': {e}")

        suggestions.sort(key=lambda x: (x['distance'], -x['rank']))

        seen = set()
        unique = []
        for s in suggestions:
            if s['term'].lower() not in seen:
                seen.add(s['term'].lower())
                unique.append(s)

        batch_suggestions[word] = unique[:limit]

    return batch_suggestions


def get_fuzzy_suggestions(word: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
    """
    Get spelling suggestions from Redis using fuzzy search.
    Single-word convenience wrapper around the batch function.
    """
    results = get_fuzzy_suggestions_batch([word], limit=limit, max_distance=max_distance)
    return results.get(word.lower().strip(), [])


# =============================================================================
# =============================================================================
#
#   MAIN WORD DISCOVERY v3 CLASS
#
# =============================================================================
# =============================================================================

class WordDiscovery:
    """
    Word Discovery v3 — Complete profile builder.

    Returns a complete profile with all entity classification, location
    detection, and search term routing done. The bridge takes the profile
    and builds Typesense params — no re-analysis.

    Uses RAM cache (vocabulary_cache.py) for all lookups.
    Only uses Redis for fuzzy spelling correction (Step 4).
    """

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.cache = vocab_cache_wrapper

    def process(self, query: str) -> Dict[str, Any]:
        """Main entry point — process a query through all 6 steps."""
        start_time = time.perf_counter()

        if self.verbose:
            print("\n" + "=" * 70)
            print(f"🔍 WORD DISCOVERY v3: '{query}'")
            print(f"   RAM cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
            print("=" * 70)

        if not query or not query.strip():
            return self._empty_result(query)

        words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
        words = [w for w in words if w]

        if len(words) == 1:
            return self._process_single_word(words[0], start_time)

        # =================================================================
        # STEP 1: Tokenize + RAM Hash Lookup + Inline Triggers
        # =================================================================
        word_data = self._step1_tokenize_and_lookup(words)

        # =================================================================
        # STEP 2: Context-Aware N-gram Resolution
        # =================================================================
        ngrams, consumed_positions = self._step2_resolve_ngrams(words, word_data)

        # =================================================================
        # STEP 3: POS Determination + Best Match Selection (merged)
        # =================================================================
        self._step3_pos_and_select(word_data, consumed_positions)

        # =================================================================
        # STEP 4: Batch Correct Unknowns (Redis — only Redis step)
        # =================================================================
        corrections = self._step4_correct_unknowns(word_data)

        # =================================================================
        # STEP 5: Re-check N-grams After Correction
        # =================================================================
        corrected_words = self._get_working_words(word_data)
        new_ngrams, new_consumed = self._step5_recheck_ngrams(
            corrected_words, word_data, consumed_positions
        )
        ngrams.extend(new_ngrams)
        consumed_positions.update(new_consumed)

        # =================================================================
        # STEP 6: Build Complete Profile
        # =================================================================
        profile = self._step6_build_profile(
            query, word_data, ngrams, consumed_positions, corrections, start_time
        )

        return profile


    # =========================================================================
    # =========================================================================
    # STEP 1: Tokenize + RAM Hash Lookup + Inline Triggers
    # =========================================================================
    # =========================================================================
    #
    # Take the raw query, split into words, look up each in RAM hash.
    # Also detect abbreviation triggers (atl→atlanta, nc→north carolina)
    # and set location_context flags on words following location signal
    # stopwords (in, near, from, etc.)
    #
    # Output: word_data list with status, matches, context_flags per word.
    # =========================================================================

    def _step1_tokenize_and_lookup(self, words: List[str]) -> List[Dict[str, Any]]:
        """Step 1: Tokenize + RAM Hash Lookup + Inline Triggers."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("📖 STEP 1: Tokenize + RAM Hash Lookup + Inline Triggers")
            print("-" * 70)

        word_data = []
        location_signal_pending = False  # set True after location signal stopwords

        for i, word in enumerate(words):
            position = i
            word_lower = word.lower().strip()

            # --- Check stopword ---
            if word_lower in STOPWORDS:
                # Check if this stopword is also an abbreviation trigger
                # (e.g., "in" = Indiana, "or" = Oregon)
                # Skip expansion for words that are primarily stopwords
                is_location_signal = word_lower in LOCATION_SIGNAL_WORDS
                context_flags = ['location_signal'] if is_location_signal else []

                word_data.append({
                    'position': position,
                    'word': word_lower,
                    'status': 'stopword',
                    'is_stopword': True,
                    'pos': STOPWORDS[word_lower],
                    'predicted_pos': STOPWORDS[word_lower],
                    'predicted_pos_list': [(STOPWORDS[word_lower], 1.0)],
                    'all_matches': [],
                    'selected_match': {
                        'term': word_lower,
                        'display': word_lower,
                        'category': 'stopword',
                        'description': '',
                        'pos': STOPWORDS[word_lower],
                        'entity_type': 'stopword',
                        'rank': 0,
                    },
                    'context_flags': context_flags,
                    'location_context': False,
                })

                if is_location_signal:
                    location_signal_pending = True

                if self.verbose:
                    sig = " [LOCATION_SIGNAL]" if is_location_signal else ""
                    print(f"  [{position}] '{word_lower}' → STOPWORD ({STOPWORDS[word_lower]}){sig}")
                continue

            # --- Check abbreviation triggers ---
            expanded = None
            if word_lower in ABBREVIATION_TRIGGERS and word_lower not in ABBREVIATION_SKIP_IF_STOPWORD:
                expanded_term = ABBREVIATION_TRIGGERS[word_lower]
                # Look up the expanded term in RAM hash
                expanded_matches = self.cache.get_term_matches(expanded_term)
                if not expanded_matches:
                    # Try with underscores for multi-word expansions
                    expanded_matches = self.cache.get_term_matches(expanded_term.replace(' ', '_'))

                if expanded_matches:
                    expanded = {
                        'original_abbreviation': word_lower,
                        'expanded_to': expanded_term,
                        'matches': expanded_matches,
                    }

            # --- RAM hash lookup ---
            has_location_ctx = location_signal_pending
            location_signal_pending = False  # consume the signal

            if expanded:
                matches = expanded['matches']
                context_flags = ['abbreviation_expanded']
                if has_location_ctx:
                    context_flags.append('location_context')

                word_data.append({
                    'position': position,
                    'word': word_lower,
                    'status': 'resolved',
                    'is_stopword': False,
                    'pos': None,
                    'predicted_pos': None,
                    'predicted_pos_list': [],
                    'all_matches': matches,
                    'selected_match': None,
                    'context_flags': context_flags,
                    'location_context': has_location_ctx,
                    'abbreviation': expanded,
                })

                if self.verbose:
                    print(f"  [{position}] '{word_lower}' → ABBREVIATION → '{expanded['expanded_to']}' "
                          f"({len(matches)} matches)")
            else:
                matches = self.cache.get_term_matches(word_lower)
                context_flags = []
                if has_location_ctx:
                    context_flags.append('location_context')

                if matches:
                    word_data.append({
                        'position': position,
                        'word': word_lower,
                        'status': 'resolved',
                        'is_stopword': False,
                        'pos': None,
                        'predicted_pos': None,
                        'predicted_pos_list': [],
                        'all_matches': matches,
                        'selected_match': None,
                        'context_flags': context_flags,
                        'location_context': has_location_ctx,
                    })

                    if self.verbose:
                        loc = " [LOCATION_CONTEXT]" if has_location_ctx else ""
                        print(f"  [{position}] '{word_lower}' → RESOLVED ({len(matches)} matches){loc}")
                        for m in matches[:3]:
                            print(f"       - {m['category']}: pos={m['pos']}, rank={m['rank']}")
                        if len(matches) > 3:
                            print(f"       ... and {len(matches) - 3} more")
                else:
                    word_data.append({
                        'position': position,
                        'word': word_lower,
                        'status': 'unknown',
                        'is_stopword': False,
                        'pos': None,
                        'predicted_pos': None,
                        'predicted_pos_list': [],
                        'all_matches': [],
                        'selected_match': None,
                        'context_flags': context_flags,
                        'location_context': has_location_ctx,
                    })

                    if self.verbose:
                        loc = " [LOCATION_CONTEXT]" if has_location_ctx else ""
                        print(f"  [{position}] '{word_lower}' → UNKNOWN{loc}")

        return word_data


    # =========================================================================
    # =========================================================================
    # STEP 2: Context-Aware N-gram Resolution
    # =========================================================================
    # =========================================================================
    #
    # For each potential n-gram window (quad → tri → bi, longest first):
    #   1. Check if n-gram exists in RAM hash
    #   2. Compare its rank against the best individual word rank
    #   3. Use context signals to boost location n-grams
    #   4. Only consume positions if the n-gram wins rank comparison
    #
    # Key difference from v2: n-grams are NOT auto-consumed.
    # They must beat the unigrams on rank (with optional location boost).
    # =========================================================================

    def _step2_resolve_ngrams(
        self,
        words: List[str],
        word_data: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], Set[int]]:
        """Step 2: Context-Aware N-gram Resolution (rank comparison + signals)."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🔗 STEP 2: Context-Aware N-gram Resolution")
            print("-" * 70)

        ngrams = []
        consumed = set()

        def _get_max_unigram_rank(indices: List[int]) -> int:
            """Get the highest rank among individual word matches at given indices."""
            max_rank = 0
            for idx in indices:
                if idx < len(word_data):
                    wd = word_data[idx]
                    if wd['all_matches']:
                        best_rank = max(m['rank'] for m in wd['all_matches'])
                        if best_rank > max_rank:
                            max_rank = best_rank
            return max_rank

        def _has_location_context(indices: List[int]) -> bool:
            """Check if any word in the window has location_context set."""
            for idx in indices:
                if idx < len(word_data) and word_data[idx].get('location_context'):
                    return True
            return False

        def _is_location_category(category: str) -> bool:
            """Check if category is a location type."""
            return category.lower() in LOCATION_CATEGORIES

        def _try_ngram(word_slice: List[str], indices: List[int]) -> bool:
            """
            Try to form an n-gram from words at given indices.
            Returns True if n-gram was accepted and positions consumed.
            """
            # Skip if any position already consumed or is a stopword
            for idx in indices:
                if idx in consumed:
                    return False
                if idx < len(word_data) and word_data[idx]['is_stopword']:
                    return False

            result = self.cache.get_ngram(word_slice)
            if not result:
                return False

            ngram_rank = result['rank']
            max_unigram_rank = _get_max_unigram_rank(indices)

            # Apply location context boost if applicable
            effective_ngram_rank = ngram_rank
            has_loc_ctx = _has_location_context(indices)
            if has_loc_ctx and _is_location_category(result.get('category', '')):
                effective_ngram_rank = int(ngram_rank * NGRAM_LOCATION_BOOST)

            # N-gram must beat the best unigram
            if effective_ngram_rank <= max_unigram_rank:
                if self.verbose:
                    print(f"  ❌ {result['ngram_type'].upper()}: '{' '.join(word_slice)}' "
                          f"(ngram_rank={ngram_rank}, effective={effective_ngram_rank}, "
                          f"max_unigram={max_unigram_rank}) → LOST")
                return False

            # N-gram wins — consume positions
            positions = [idx + 1 for idx in indices]  # 1-based for output
            ngram_entry = {
                'type': result['ngram_type'],
                'positions': positions,
                'words': word_slice,
                'phrase': result['term'],
                'display': result['display'],
                'category': result['category'],
                'description': result['description'],
                'pos': normalize_pos_string(result['pos']),
                'rank': ngram_rank,
                'location_boosted': has_loc_ctx and _is_location_category(result.get('category', '')),
            }
            ngrams.append(ngram_entry)
            consumed.update(indices)

            if self.verbose:
                boost_note = f" [LOC_BOOST: {ngram_rank}→{effective_ngram_rank}]" if ngram_entry['location_boosted'] else ""
                print(f"  ✅ {result['ngram_type'].upper()}: '{' '.join(word_slice)}' "
                      f"(rank={ngram_rank}, vs unigram={max_unigram_rank}){boost_note}")
                print(f"       Category: {result['category']}")

            return True

        # --- Quadgrams (longest first) ---
        if len(words) >= 4:
            for i in range(len(words) - 3):
                _try_ngram(words[i:i+4], [i, i+1, i+2, i+3])

        # --- Trigrams ---
        if len(words) >= 3:
            for i in range(len(words) - 2):
                _try_ngram(words[i:i+3], [i, i+1, i+2])

        # --- Bigrams ---
        if len(words) >= 2:
            for i in range(len(words) - 1):
                _try_ngram(words[i:i+2], [i, i+1])

        if not ngrams and self.verbose:
            print("  (no n-grams found)")

        return ngrams, consumed


    # =========================================================================
    # =========================================================================
    # STEP 3: POS Determination + Best Match Selection (merged)
    # =========================================================================
    # =========================================================================
    #
    # Merged from v2 Steps 3, 3.5, and 4 into a single pass:
    #   1. Get left/right neighbor POS (from resolved words, stopwords, n-gram edges)
    #   2. Look up grammar rule → predicted POS list with confidence
    #   3. Apply suffix refinement (boost/adjust based on word ending)
    #   4. Filter matches by compatible POS
    #   5. Sort compatible matches by rank (highest wins)
    #   6. Set selected match
    #
    # Multi-pass for adjacent unknowns — same logic as v2.
    # Grammar rules and suffix rules are UNCHANGED from v2.
    # =========================================================================

    def _step3_pos_and_select(
        self,
        word_data: List[Dict[str, Any]],
        consumed_positions: Set[int]
    ) -> None:
        """Step 3: POS Determination + Best Match Selection (merged)."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🧠 STEP 3: POS Determination + Best Match Selection")
            print("-" * 70)

        # --- Helper: find nearest known POS in a direction ---
        def get_nearest_known_pos(index: int, direction: int) -> Optional[str]:
            i = index + direction
            while 0 <= i < len(word_data):
                wd = word_data[i]
                if wd['is_stopword']:
                    return wd['pos']
                if wd.get('pos'):
                    return wd['pos']
                if wd.get('predicted_pos'):
                    return wd['predicted_pos']
                if wd['all_matches']:
                    return normalize_pos_string(wd['all_matches'][0]['pos'])
                i += direction
            return None

        # --- Helper: apply suffix refinement to POS predictions ---
        def apply_suffix_refinement(word: str, current_list: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
            """Apply suffix rules to refine POS predictions. Returns updated list."""
            if len(word) < MIN_SUFFIX_WORD_LENGTH:
                return current_list
            if word in SUFFIX_EXCEPTIONS:
                return current_list

            # Find matching suffix (longest first)
            matched_suffix = None
            suffix_predictions = None
            sorted_suffixes = sorted(SUFFIX_POS_RULES.keys(), key=len, reverse=True)
            for suffix in sorted_suffixes:
                if word.endswith(suffix) and len(word) > len(suffix) + 1:
                    matched_suffix = suffix
                    suffix_predictions = SUFFIX_POS_RULES[suffix]
                    break

            if not matched_suffix:
                return current_list

            # Merge suffix predictions with grammar predictions
            refined = {}
            for pos, conf in current_list:
                refined[pos] = conf
            for pos, suffix_conf in suffix_predictions:
                if pos in refined:
                    existing = refined[pos]
                    boosted = min((existing + suffix_conf) / 2 + 0.10, 0.98)
                    refined[pos] = boosted
                else:
                    refined[pos] = suffix_conf * 0.85

            return sorted(refined.items(), key=lambda x: -x[1])

        # --- Main resolution loop (multi-pass for adjacent unknowns) ---
        def resolve_word(i: int, wd: Dict[str, Any]) -> bool:
            """Resolve POS and select best match for one word. Returns True if resolved."""
            position = wd['position']
            pos_index = position
            wd['part_of_ngram'] = pos_index in consumed_positions

            if wd['is_stopword']:
                return True

            # --- Get neighbor POS ---
            left_pos = None
            if i == 0:
                left_pos = 'start'
            else:
                left_pos = get_nearest_known_pos(i, direction=-1)

            right_pos = None
            if i == len(word_data) - 1:
                right_pos = 'end'
            else:
                right_pos = get_nearest_known_pos(i, direction=1)

            if left_pos is None and right_pos is None:
                return False  # can't resolve yet — adjacent unknowns

            # --- Look up grammar rule ---
            predicted_pos = None

            if left_pos and right_pos:
                predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))

            if not predicted_pos and left_pos:
                predicted_pos = GRAMMAR_RULES.get((left_pos, None))

            if not predicted_pos and right_pos:
                predicted_pos = GRAMMAR_RULES.get((None, right_pos))

            if not predicted_pos:
                predicted_pos = [('noun', 0.75)]

            # --- Passive voice heuristic (same as v2) ---
            has_be_before = any(
                word_data[j]['is_stopword'] and word_data[j]['pos'] == 'be'
                for j in range(0, i)
            )
            next_ends_ed = False
            if i + 1 < len(word_data):
                next_word_clean = word_data[i + 1]['word'].rstrip('?!.,;:')
                next_ends_ed = next_word_clean.endswith('ed')

            if has_be_before and next_ends_ed and not wd['is_stopword']:
                is_known_adverb = any(
                    m['pos'] in ('adverb',)
                    for m in wd.get('all_matches', [])
                )
                if not is_known_adverb:
                    predicted_pos = [
                        ('noun', 0.95),
                        ('proper_noun', 0.85),
                        ('adjective', 0.4),
                        ('adverb', 0.3),
                    ]

            if isinstance(predicted_pos, list):
                pos_list = predicted_pos
                primary_pos = predicted_pos[0][0] if predicted_pos else 'noun'
            else:
                pos_list = [(predicted_pos, 0.90)]
                primary_pos = predicted_pos

            # --- Apply suffix refinement (merged from v2 Step 3.5) ---
            working_word = wd.get('corrected', wd['word']).lower()
            pos_list = apply_suffix_refinement(working_word, pos_list)
            primary_pos = pos_list[0][0] if pos_list else primary_pos

            wd['predicted_pos_list'] = pos_list
            wd['predicted_pos'] = primary_pos

            if self.verbose:
                context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
                ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
                suffix_note = ""
                if wd.get('suffix_detected'):
                    suffix_note = f" [suffix: -{wd['suffix_detected']}]"
                print(f"  [{position}] '{wd['word']}' → Predicted: {pos_list}{ngram_note}{suffix_note}")
                print(f"       Context: {context}")

            # --- Select best match (merged from v2 Step 4) ---
            if wd['status'] == 'unknown':
                # No matches — defer to Step 4
                return True

            matches = wd['all_matches']
            if not matches:
                return True

            # Filter by compatible POS
            compatible_matches = []
            for m in matches:
                match_pos = normalize_pos_string(m['pos'])
                if is_pos_compatible(match_pos, primary_pos):
                    compatible_matches.append(m)

            if compatible_matches:
                compatible_matches.sort(key=lambda x: x['rank'], reverse=True)
                best = compatible_matches[0]
            else:
                # No POS match — fall back to highest rank
                best = matches[0]

            wd['selected_match'] = best
            wd['pos'] = normalize_pos_string(best['pos'])
            wd['status'] = 'resolved'

            if self.verbose:
                pos_index_in = position
                ngram_note2 = " (in n-gram)" if pos_index_in in consumed_positions else ""
                print(f"       Selected: {best['category']} (pos={normalize_pos_string(best['pos'])}, rank={best['rank']}){ngram_note2}")
                if len(matches) > 1:
                    rejected = [f"{m['category']}({normalize_pos_string(m['pos'])})" for m in matches if m != best]
                    if rejected:
                        print(f"       Rejected: {rejected}")

            return True

        # --- Multi-pass loop ---
        max_passes = len(word_data)
        resolved = set()

        for pass_num in range(max_passes):
            progress = False

            if self.verbose and pass_num > 0:
                print(f"\n  ── Pass {pass_num + 1} ──")

            for i, wd in enumerate(word_data):
                if i in resolved:
                    continue

                success = resolve_word(i, wd)

                if success:
                    resolved.add(i)
                    progress = True

            if not progress or len(resolved) == len(word_data):
                break

        # --- Default unresolved words to noun ---
        for i, wd in enumerate(word_data):
            if i not in resolved:
                if self.verbose:
                    print(f"  [{wd['position']}] '{wd['word']}' → No context found, defaulting to noun")
                wd['predicted_pos'] = 'noun'
                wd['predicted_pos_list'] = [('noun', 0.75)]


    # =========================================================================
    # =========================================================================
    # STEP 4: Batch Correct Unknowns (Redis — only Redis step)
    # =========================================================================
    # =========================================================================
    #
    # Collect ALL unknown and POS-mismatched positions.
    # Send them as one batch to RediSearch fuzzy search via Redis pipeline.
    # This is the ONLY Redis call in the entire pipeline.
    #
    # - unknown words     → keep original for search; fuzzy match shown as
    #                       "did you mean?" hint only (correction_type='suggestion')
    # - pos_mismatch words → replace word (genuine typo found in RAM hash
    #                        with wrong POS)
    # =========================================================================

    def _step4_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Step 4: Batch Correct Unknowns (Redis fuzzy — only Redis step)."""
        unknowns = []
        pos_mismatches = []

        for wd in word_data:
            if wd['status'] == 'unknown':
                unknowns.append(wd)
            elif (
                wd['status'] == 'resolved'
                and not wd['is_stopword']
                and wd.get('predicted_pos')
                and wd.get('pos')
                and wd['predicted_pos'] != wd['pos']
            ):
                predicted_list = wd.get('predicted_pos_list', [])
                top_confidence = predicted_list[0][1] if predicted_list else 0
                word_rank = wd.get('selected_match', {}).get('rank', 0) or 0
                if top_confidence >= 0.90 and word_rank < 200:
                    pos_mismatches.append(wd)

        if not unknowns and not pos_mismatches:
            if self.verbose:
                print("\n" + "-" * 70)
                print("🔧 STEP 4: Batch Correct Unknowns (Redis)")
                print("-" * 70)
                print("  (no unknown words)")
            return []

        if self.verbose:
            print("\n" + "-" * 70)
            print("🔧 STEP 4: Batch Correct Unknowns (Redis — Pipeline Batch)")
            print("-" * 70)
            print(f"  Unknowns:       {len(unknowns)}")
            print(f"  POS mismatches: {len(pos_mismatches)}")

        all_words_to_correct = [
            {
                'wd': wd,
                'word': wd['word'],
                'predicted_pos': wd.get('predicted_pos') or 'noun',
                'correction_type': 'unknown',
            }
            for wd in unknowns
        ] + [
            {
                'wd': wd,
                'word': wd['word'],
                'predicted_pos': wd.get('predicted_pos') or 'noun',
                'correction_type': 'pos_mismatch',
                'word_rank': wd.get('selected_match', {}).get('rank', 0) or 0,
            }
            for wd in pos_mismatches
        ]

        words_to_fetch = [item['word'] for item in all_words_to_correct]

        if self.verbose:
            print(f"  Sending to Redis pipeline: {words_to_fetch}")

        batch_suggestions = get_fuzzy_suggestions_batch(
            words_to_fetch, limit=10, max_distance=2
        )

        corrections = []

        for item in all_words_to_correct:
            wd = item['wd']
            word = item['word']
            position = wd['position']
            predicted_pos = item['predicted_pos']
            correction_type = item['correction_type']

            suggestions = batch_suggestions.get(word.lower().strip(), [])

            for s in suggestions:
                s['compatible'] = is_pos_compatible(s['pos'], predicted_pos)
            wd['redis_suggestions'] = suggestions

            if not suggestions:
                if self.verbose:
                    print(f"  [{position}] '{word}' → No suggestions found")
                continue

            compatible = [s for s in suggestions if s['compatible']]

            if correction_type == 'pos_mismatch':
                word_rank = item.get('word_rank', 0)
                compatible = [
                    s for s in compatible
                    if s['term'] != word
                    and s['distance'] <= 1
                    and s['rank'] > word_rank * 3
                ]

            if compatible:
                compatible.sort(key=lambda x: (
                    x['distance'],
                    self._pos_match_score(x['pos'], predicted_pos),
                    -x['rank']
                ))
                best = compatible[0]
            else:
                best = suggestions[0]

            if correction_type == 'pos_mismatch':
                # Genuine typo — replace word in search query
                if self.verbose:
                    print(f"  [{position}] '{word}' → REPLACED with '{best['term']}' "
                          f"(pos_mismatch, distance={best['distance']})")

                wd['status'] = 'pos_corrected'
                wd['corrected'] = best['term']
                wd['corrected_display'] = best['display']
                wd['pos'] = best['pos']
                wd['distance'] = best['distance']
                wd['selected_match'] = {
                    'term': best['term'],
                    'display': best['display'],
                    'category': best['category'],
                    'description': best['description'],
                    'pos': best['pos'],
                    'entity_type': best['entity_type'],
                    'rank': best['rank'],
                }
                corrections.append({
                    'position': position,
                    'original': word,
                    'corrected': best['term'],
                    'distance': best['distance'],
                    'pos': best['pos'],
                    'category': best['category'],
                    'correction_type': 'pos_mismatch',
                })

            else:
                # Unknown word — keep original for search, suggestion is hint only
                if self.verbose:
                    print(f"  [{position}] '{word}' → KEPT for search (not in RAM hash). "
                          f"Suggestion: '{best['term']}' (distance={best['distance']})")

                wd['status'] = 'unknown_suggest'
                wd['suggestion'] = best['term']
                wd['suggestion_display'] = best['display']
                wd['suggestion_distance'] = best['distance']
                corrections.append({
                    'position': position,
                    'original': word,
                    'corrected': best['term'],
                    'distance': best['distance'],
                    'pos': best['pos'],
                    'category': best['category'],
                    'correction_type': 'suggestion',
                })

        return corrections

    def _pos_match_score(self, pos_value: Any, predicted_pos: str) -> int:
        """
        Returns 0 if pos_value exactly matches predicted_pos,
        1 if compatible but not exact.
        """
        if isinstance(pos_value, list):
            pos_list = pos_value
        elif isinstance(pos_value, str) and pos_value.startswith('['):
            import ast
            try:
                pos_list = ast.literal_eval(pos_value)
            except:
                pos_list = [pos_value]
        else:
            pos_list = [pos_value]

        for p in pos_list:
            if p == predicted_pos:
                return 0
        return 1


    # =========================================================================
    # =========================================================================
    # STEP 5: Re-check N-grams After Correction
    # =========================================================================
    # =========================================================================
    #
    # After fuzzy correction fills in unknown positions, re-check for
    # n-grams that might now be detectable. Uses the same rank comparison
    # logic from Step 2 (not auto-consume).
    #
    # Example: "noth carolina" → "noth" corrected to "north" →
    #          re-check finds "north_carolina" bigram → rank wins → consume.
    # =========================================================================

    def _step5_recheck_ngrams(
        self,
        corrected_words: List[str],
        word_data: List[Dict[str, Any]],
        already_consumed: Set[int]
    ) -> Tuple[List[Dict[str, Any]], Set[int]]:
        """Step 5: Re-check N-grams After Correction (RAM cache)."""
        has_corrections = any(wd['status'] in ('corrected', 'pos_corrected') for wd in word_data)

        if not has_corrections:
            if self.verbose:
                print("\n" + "-" * 70)
                print("🔄 STEP 5: Re-check N-grams After Correction")
                print("-" * 70)
                print("  (no corrections, skipping)")
            return [], set()

        if self.verbose:
            print("\n" + "-" * 70)
            print("🔄 STEP 5: Re-check N-grams After Correction (RAM)")
            print("-" * 70)

        new_ngrams = []
        new_consumed = set()
        words = corrected_words

        def _get_max_unigram_rank(indices: List[int]) -> int:
            max_rank = 0
            for idx in indices:
                if idx < len(word_data):
                    wd = word_data[idx]
                    if wd['all_matches']:
                        best_rank = max(m['rank'] for m in wd['all_matches'])
                        if best_rank > max_rank:
                            max_rank = best_rank
            return max_rank

        def _has_location_context(indices: List[int]) -> bool:
            for idx in indices:
                if idx < len(word_data) and word_data[idx].get('location_context'):
                    return True
            return False

        def _is_location_category(category: str) -> bool:
            return category.lower() in LOCATION_CATEGORIES

        # --- Trigrams ---
        if len(words) >= 3:
            for i in range(len(words) - 2):
                if any(p in already_consumed or p in new_consumed for p in [i, i+1, i+2]):
                    continue

                relevant_words = word_data[i:i+3]
                if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
                    continue

                tri = words[i:i+3]
                result = self.cache.get_ngram(tri)

                if result:
                    ngram_rank = result['rank']
                    max_uni = _get_max_unigram_rank([i, i+1, i+2])
                    effective = ngram_rank
                    has_loc = _has_location_context([i, i+1, i+2])
                    if has_loc and _is_location_category(result.get('category', '')):
                        effective = int(ngram_rank * NGRAM_LOCATION_BOOST)

                    if effective > max_uni:
                        positions = [i+1, i+2, i+3]
                        new_ngrams.append({
                            'type': 'trigram',
                            'positions': positions,
                            'words': tri,
                            'phrase': result['term'],
                            'display': result['display'],
                            'category': result['category'],
                            'description': result['description'],
                            'pos': normalize_pos_string(result['pos']),
                            'rank': ngram_rank,
                            'from_correction': True,
                            'location_boosted': has_loc and _is_location_category(result.get('category', '')),
                        })
                        new_consumed.update([i, i+1, i+2])

                        if self.verbose:
                            print(f"  ✅ NEW TRIGRAM: '{' '.join(tri)}' (after correction, rank={ngram_rank})")

        # --- Bigrams ---
        if len(words) >= 2:
            for i in range(len(words) - 1):
                if any(p in already_consumed or p in new_consumed for p in [i, i+1]):
                    continue

                relevant_words = word_data[i:i+2]
                if not any(wd['status'] in ('corrected', 'pos_corrected') for wd in relevant_words):
                    continue

                bi = words[i:i+2]
                result = self.cache.get_ngram(bi)

                if result:
                    ngram_rank = result['rank']
                    max_uni = _get_max_unigram_rank([i, i+1])
                    effective = ngram_rank
                    has_loc = _has_location_context([i, i+1])
                    if has_loc and _is_location_category(result.get('category', '')):
                        effective = int(ngram_rank * NGRAM_LOCATION_BOOST)

                    if effective > max_uni:
                        positions = [i+1, i+2]
                        new_ngrams.append({
                            'type': 'bigram',
                            'positions': positions,
                            'words': bi,
                            'phrase': result['term'],
                            'display': result['display'],
                            'category': result['category'],
                            'description': result['description'],
                            'pos': normalize_pos_string(result['pos']),
                            'rank': ngram_rank,
                            'from_correction': True,
                            'location_boosted': has_loc and _is_location_category(result.get('category', '')),
                        })
                        new_consumed.update([i, i+1])

                        if self.verbose:
                            print(f"  ✅ NEW BIGRAM: '{' '.join(bi)}' (after correction, rank={ngram_rank})")

        if not new_ngrams and self.verbose:
            print("  (no new n-grams found)")

        return new_ngrams, new_consumed


    # =========================================================================
    # =========================================================================
    # STEP 6: Build Complete Profile
    # =========================================================================
    # =========================================================================
    #
    # This is the KEY optimization of v3.
    #
    # Instead of returning raw terms and n-grams for the bridge to
    # re-analyze, v3 builds the COMPLETE profile:
    #   - Entities classified (persons, organizations, keywords, media)
    #   - Locations classified (cities, states, with variants)
    #   - Search terms (POS-filtered: nouns only)
    #   - Field boosts computed
    #   - Primary intent scored
    #   - Query array + corrected array maintained
    #
    # The bridge reads this profile directly — no more build_query_profile().
    # =========================================================================

    def _step6_build_profile(
        self,
        query: str,
        word_data: List[Dict[str, Any]],
        ngrams: List[Dict[str, Any]],
        consumed_positions: Set[int],
        corrections: List[Dict[str, Any]],
        start_time: float
    ) -> Dict[str, Any]:
        """Step 6: Build Complete Profile (entities, locations, search terms, boosts)."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("📊 STEP 6: Build Complete Profile")
            print("-" * 70)

        # --- Build query_array and corrected_array ---
        query_array = []
        corrected_array = []

        for wd in word_data:
            query_array.append({
                'position': wd['position'],
                'word': wd['word'],
                'status': wd['status'],
            })

            if wd['status'] in ('corrected', 'pos_corrected') and wd.get('corrected'):
                corrected_array.append({
                    'position': wd['position'],
                    'word': wd['corrected'],
                })
            elif wd['status'] == 'unknown_suggest' and wd.get('suggestion'):
                # corrected_array uses suggestion for "did you mean?" display
                corrected_array.append({
                    'position': wd['position'],
                    'word': wd['suggestion'],
                })
            else:
                corrected_array.append({
                    'position': wd['position'],
                    'word': wd['word'],
                })

        # --- Build corrected_query string ---
        working_words = self._get_working_words(word_data)
        corrected_query = ' '.join(working_words)

        # --- Build corrected_display_query (uses suggestions for display) ---
        display_words = []
        for wd in word_data:
            if wd['status'] in ('corrected', 'pos_corrected') and wd.get('corrected'):
                display_words.append(wd['corrected'])
            elif wd['status'] == 'unknown_suggest' and wd.get('suggestion'):
                display_words.append(wd['suggestion'])
            else:
                display_words.append(wd['word'])
        corrected_display_query = ' '.join(display_words)

        # --- Mark part_of_ngram on word_data ---
        for wd in word_data:
            pos_index = wd['position']
            wd['part_of_ngram'] = pos_index in consumed_positions

        # --- Classify entities ---
        persons = []
        organizations = []
        keywords = []
        media = []
        cities = []
        states = []
        location_terms = []

        intent_scores = {
            'person': 0,
            'organization': 0,
            'location': 0,
            'keyword': 0,
            'media': 0,
        }

        # Classify from n-grams first
        for ng in ngrams:
            cat_lower = ng.get('category', '').lower()
            rank = ng.get('rank', 0)
            entry = {
                'phrase': ng['phrase'],
                'display': ng['display'],
                'rank': rank,
                'category': ng['category'],
                'type': ng['type'],
            }

            if cat_lower in PERSON_CATEGORIES:
                persons.append(entry)
                intent_scores['person'] += rank
            elif cat_lower in ORGANIZATION_CATEGORIES:
                organizations.append(entry)
                intent_scores['organization'] += rank
            elif cat_lower in LOCATION_CATEGORIES:
                intent_scores['location'] += rank
                # Sub-classify cities vs states
                if cat_lower in CITY_CATEGORIES:
                    cities.append({'name': ng['display'], 'rank': rank, 'category': ng['category']})
                elif cat_lower in STATE_CATEGORIES:
                    states.append({
                        'name': ng['display'],
                        'rank': rank,
                        'category': ng['category'],
                        'variants': self._get_state_variants(ng['display']),
                    })
                else:
                    location_terms.extend(ng.get('words', []))
            elif cat_lower in MEDIA_CATEGORIES:
                media.append(entry)
                intent_scores['media'] += rank
            elif cat_lower in KEYWORD_CATEGORIES:
                keywords.append(entry)
                intent_scores['keyword'] += rank
            else:
                # Default: treat as keyword
                keywords.append(entry)
                intent_scores['keyword'] += rank

        # Classify from individual terms (not consumed by n-grams)
        for wd in word_data:
            if wd['is_stopword']:
                continue
            pos_index = wd['position']
            if pos_index in consumed_positions:
                continue  # already classified via n-gram

            sm = wd.get('selected_match')
            if not sm:
                continue

            cat_lower = sm.get('category', '').lower()
            rank = sm.get('rank', 0)
            entry = {
                'phrase': sm.get('term', wd['word']),
                'display': sm.get('display', wd['word']),
                'rank': rank,
                'category': sm.get('category', ''),
                'type': 'unigram',
            }

            if cat_lower in PERSON_CATEGORIES:
                persons.append(entry)
                intent_scores['person'] += rank
            elif cat_lower in ORGANIZATION_CATEGORIES:
                organizations.append(entry)
                intent_scores['organization'] += rank
            elif cat_lower in LOCATION_CATEGORIES:
                intent_scores['location'] += rank
                if cat_lower in CITY_CATEGORIES:
                    cities.append({'name': sm['display'], 'rank': rank, 'category': sm['category']})
                elif cat_lower in STATE_CATEGORIES:
                    states.append({
                        'name': sm['display'],
                        'rank': rank,
                        'category': sm['category'],
                        'variants': self._get_state_variants(sm['display']),
                    })
                else:
                    location_terms.append(wd['word'])
            elif cat_lower in MEDIA_CATEGORIES:
                media.append(entry)
                intent_scores['media'] += rank
            elif cat_lower in KEYWORD_CATEGORIES:
                keywords.append(entry)
                intent_scores['keyword'] += rank
            else:
                if rank > 0:
                    keywords.append(entry)
                    intent_scores['keyword'] += rank

        # --- Determine primary intent ---
        primary_intent = 'general'
        max_score = 0
        for intent_type, score in intent_scores.items():
            if score > max_score:
                max_score = score
                primary_intent = intent_type

        # --- Build search terms (nouns only — POS filtered) ---
        search_terms = []
        for wd in word_data:
            if wd['is_stopword']:
                continue
            pos_index = wd['position']
            if pos_index in consumed_positions:
                continue  # n-gram handles this

            effective_pos = wd.get('pos') or wd.get('predicted_pos') or 'unknown'
            effective_pos = normalize_pos_string(effective_pos)

            if effective_pos in SEARCHABLE_POS:
                working_word = wd.get('corrected', wd['word'])
                search_terms.append(working_word)

        # Add n-gram phrases to search terms
        for ng in ngrams:
            search_terms.append(ng['phrase'])

        # --- Compute field boosts (base — mode adjustments by bridge after intent) ---
        field_boosts = {
            'document_title': 10,
            'entity_names': 2,
            'primary_keywords': 15,
            'key_facts': 3,
            'semantic_keywords': 7,
        }

        if persons:
            field_boosts['entity_names'] += 8
            field_boosts['document_title'] += 5
        if organizations:
            field_boosts['entity_names'] += 5
            field_boosts['primary_keywords'] += 3
        if keywords:
            field_boosts['primary_keywords'] += 2
            field_boosts['semantic_keywords'] += 3
            field_boosts['key_facts'] += 2
        if media:
            field_boosts['document_title'] += 8
            field_boosts['primary_keywords'] += 5

        has_unknowns = any(wd['status'] in ('unknown', 'unknown_suggest') for wd in word_data)
        if has_unknowns:
            # Cast wider net for unknown terms
            for key in field_boosts:
                field_boosts[key] += 2

        # --- Build terms list (full detail for debugging and downstream) ---
        terms = []
        for wd in word_data:
            pos_value = wd.get('pos') or wd.get('predicted_pos') or 'unknown'
            pos_value = normalize_pos_string(pos_value)
            predicted_pos = wd.get('predicted_pos')
            if predicted_pos:
                predicted_pos = normalize_pos_string(predicted_pos)

            term = {
                'position': wd['position'],
                'word': wd['word'],
                'status': wd['status'],
                'is_stopword': wd['is_stopword'],
                'part_of_ngram': wd.get('part_of_ngram', False),
                'pos': pos_value,
                'predicted_pos': predicted_pos,
                'context_flags': wd.get('context_flags', []),
            }

            if wd.get('selected_match'):
                sm = wd['selected_match']
                term['category'] = sm.get('category', '')
                term['description'] = sm.get('description', '')
                term['display'] = sm.get('display', wd['word'])
                term['rank'] = sm.get('rank', 0)
                term['entity_type'] = sm.get('entity_type', 'unigram')
            elif wd.get('all_matches') and len(wd['all_matches']) > 0:
                best_match = wd['all_matches'][0]
                term['category'] = best_match.get('category', '')
                term['description'] = best_match.get('description', '')
                term['display'] = best_match.get('display', wd['word'])
                term['rank'] = best_match.get('rank', 0)
                term['entity_type'] = best_match.get('entity_type', 'unigram')
            else:
                term['category'] = ''
                term['description'] = ''
                term['display'] = wd['word']
                term['rank'] = 0
                term['entity_type'] = 'unigram'

            # Attach correction fields
            if wd['status'] in ('corrected', 'pos_corrected'):
                term['corrected'] = wd.get('corrected')
                term['corrected_display'] = wd.get('corrected_display')
                term['distance'] = wd.get('distance')

            if wd['status'] == 'unknown_suggest':
                term['suggestion'] = wd.get('suggestion')
                term['suggestion_display'] = wd.get('suggestion_display')
                term['suggestion_distance'] = wd.get('suggestion_distance')

            # Abbreviation info
            if wd.get('abbreviation'):
                term['abbreviation'] = {
                    'original': wd['abbreviation']['original_abbreviation'],
                    'expanded_to': wd['abbreviation']['expanded_to'],
                }

            term['match_count'] = len(wd.get('all_matches', []))

            terms.append(term)

        # --- Timing ---
        elapsed = (time.perf_counter() - start_time) * 1000

        # --- Stats ---
        stats = {
            'total_words': len(word_data),
            'valid_words': sum(1 for wd in word_data if wd['status'] == 'resolved'),
            'corrected_words': sum(1 for wd in word_data if wd['status'] in ('corrected', 'pos_corrected')),
            'unknown_words': sum(1 for wd in word_data if wd['status'] in ('unknown', 'unknown_suggest')),
            'stopwords': sum(1 for wd in word_data if wd['is_stopword']),
            'ngram_count': len(ngrams),
        }

        # --- Assemble complete profile ---
        profile = {
            # Arrays
            'query_array': query_array,
            'corrected_array': corrected_array,

            # Derived strings
            'query': query,
            'corrected_query': corrected_query,
            'corrected_display_query': corrected_display_query,
            'processing_time_ms': round(elapsed, 2),

            # Search terms (POS-filtered: nouns only)
            'search_terms': search_terms,

            # Classified entities
            'persons': persons,
            'organizations': organizations,
            'keywords': keywords,
            'media': media,

            # Locations (ready for Typesense filters)
            'cities': cities,
            'states': states,
            'location_terms': location_terms,

            # Intent scores
            'primary_intent': primary_intent,
            'intent_scores': intent_scores,

            # Field boosts (base — mode adjustments applied by bridge)
            'field_boosts': field_boosts,

            # Corrections for UI
            'corrections': corrections,

            # Stats
            'stats': stats,

            # Raw data (for intent detection and debugging)
            'terms': terms,
            'ngrams': ngrams,
        }

        if self.verbose:
            print("\n" + "=" * 70)
            print("📊 COMPLETE PROFILE")
            print("=" * 70)
            print(f"  Query:           '{query}'")
            print(f"  Corrected:       '{corrected_query}'")
            print(f"  Display:         '{corrected_display_query}'")
            print(f"  Search terms:    {search_terms}")
            print(f"  Primary intent:  {primary_intent}")
            print(f"  Intent scores:   {intent_scores}")
            print(f"  Persons:         {[p['display'] for p in persons]}")
            print(f"  Organizations:   {[o['display'] for o in organizations]}")
            print(f"  Keywords:        {[k['display'] for k in keywords]}")
            print(f"  Media:           {[m['display'] for m in media]}")
            print(f"  Cities:          {[c['name'] for c in cities]}")
            print(f"  States:          {[s['name'] for s in states]}")
            print(f"  Location terms:  {location_terms}")
            print(f"  Field boosts:    {field_boosts}")
            print(f"  Time:            {elapsed:.2f}ms")
            print(f"  Stats:           {stats}")
            print(f"  Source:          {'RAM cache' if RAM_CACHE_AVAILABLE else 'Redis fallback'}")

        return profile


    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_working_words(self, word_data: List[Dict[str, Any]]) -> List[str]:
        """
        Build the word list used for the actual search.

        Only replaces a word when it is a confirmed genuine typo:
          - status='corrected'     (was unknown, replaced — currently unused)
          - status='pos_corrected' (was in RAM hash with wrong POS, replaced)

        Words with status 'unknown' or 'unknown_suggest' are NOT in the RAM
        hash. Their original form is passed through so the semantic path
        (graph → embedding → vector search) can still find results.
        """
        words = []
        for wd in word_data:
            if wd['status'] in ('corrected', 'pos_corrected') and wd.get('corrected'):
                words.append(wd['corrected'])
            else:
                words.append(wd['word'])
        return words

    def _get_state_variants(self, state_name: str) -> List[str]:
        """Get name variants for a state (full name + common abbreviation)."""
        STATE_ABBREVS = {
            'alabama': ['Alabama', 'AL'],
            'alaska': ['Alaska', 'AK'],
            'arizona': ['Arizona', 'AZ'],
            'arkansas': ['Arkansas', 'AR'],
            'california': ['California', 'CA'],
            'colorado': ['Colorado', 'CO'],
            'connecticut': ['Connecticut', 'CT'],
            'delaware': ['Delaware', 'DE'],
            'florida': ['Florida', 'FL'],
            'georgia': ['Georgia', 'GA'],
            'hawaii': ['Hawaii', 'HI'],
            'idaho': ['Idaho', 'ID'],
            'illinois': ['Illinois', 'IL'],
            'indiana': ['Indiana', 'IN'],
            'iowa': ['Iowa', 'IA'],
            'kansas': ['Kansas', 'KS'],
            'kentucky': ['Kentucky', 'KY'],
            'louisiana': ['Louisiana', 'LA'],
            'maine': ['Maine', 'ME'],
            'maryland': ['Maryland', 'MD'],
            'massachusetts': ['Massachusetts', 'MA'],
            'michigan': ['Michigan', 'MI'],
            'minnesota': ['Minnesota', 'MN'],
            'mississippi': ['Mississippi', 'MS'],
            'missouri': ['Missouri', 'MO'],
            'montana': ['Montana', 'MT'],
            'nebraska': ['Nebraska', 'NE'],
            'nevada': ['Nevada', 'NV'],
            'new hampshire': ['New Hampshire', 'NH'],
            'new jersey': ['New Jersey', 'NJ'],
            'new mexico': ['New Mexico', 'NM'],
            'new york': ['New York', 'NY'],
            'north carolina': ['North Carolina', 'NC'],
            'north dakota': ['North Dakota', 'ND'],
            'ohio': ['Ohio', 'OH'],
            'oklahoma': ['Oklahoma', 'OK'],
            'oregon': ['Oregon', 'OR'],
            'pennsylvania': ['Pennsylvania', 'PA'],
            'rhode island': ['Rhode Island', 'RI'],
            'south carolina': ['South Carolina', 'SC'],
            'south dakota': ['South Dakota', 'SD'],
            'tennessee': ['Tennessee', 'TN'],
            'texas': ['Texas', 'TX'],
            'utah': ['Utah', 'UT'],
            'vermont': ['Vermont', 'VT'],
            'virginia': ['Virginia', 'VA'],
            'washington': ['Washington', 'WA'],
            'west virginia': ['West Virginia', 'WV'],
            'wisconsin': ['Wisconsin', 'WI'],
            'wyoming': ['Wyoming', 'WY'],
            'district of columbia': ['District of Columbia', 'DC', 'Washington DC'],
        }

        return STATE_ABBREVS.get(state_name.lower(), [state_name])

    def _empty_result(self, query: str) -> Dict[str, Any]:
        """Return empty profile structure."""
        return {
            'query_array': [],
            'corrected_array': [],
            'query': query or '',
            'corrected_query': '',
            'corrected_display_query': '',
            'processing_time_ms': 0,
            'search_terms': [],
            'persons': [],
            'organizations': [],
            'keywords': [],
            'media': [],
            'cities': [],
            'states': [],
            'location_terms': [],
            'primary_intent': 'general',
            'intent_scores': {
                'person': 0, 'organization': 0,
                'location': 0, 'keyword': 0, 'media': 0,
            },
            'field_boosts': {
                'document_title': 10, 'entity_names': 2,
                'primary_keywords': 15, 'key_facts': 3,
                'semantic_keywords': 7,
            },
            'corrections': [],
            'stats': {
                'total_words': 0, 'valid_words': 0,
                'corrected_words': 0, 'unknown_words': 0,
                'stopwords': 0, 'ngram_count': 0,
            },
            'terms': [],
            'ngrams': [],
        }

    def _process_single_word(self, word: str, start_time: float) -> Dict[str, Any]:
        """
        Handle single word queries.

        When a word is not in the RAM hash but a fuzzy suggestion
        exists, the original word is kept as corrected_query so the semantic
        path can search with it. The suggestion is returned with
        correction_type='suggestion' for "did you mean?" display only.
        """
        if self.verbose:
            print("\n" + "-" * 70)
            print("📖 Single Word Query")
            print("-" * 70)

        word_lower = word.lower().strip()

        # --- Check abbreviation trigger ---
        if word_lower in ABBREVIATION_TRIGGERS and word_lower not in ABBREVIATION_SKIP_IF_STOPWORD:
            expanded_term = ABBREVIATION_TRIGGERS[word_lower]
            expanded_matches = self.cache.get_term_matches(expanded_term)
            if not expanded_matches:
                expanded_matches = self.cache.get_term_matches(expanded_term.replace(' ', '_'))

            if expanded_matches:
                best = expanded_matches[0]
                elapsed = (time.perf_counter() - start_time) * 1000

                if self.verbose:
                    print(f"  '{word_lower}' → ABBREVIATION → '{expanded_term}' ({len(expanded_matches)} matches)")

                # Classify the expanded term
                cat_lower = best.get('category', '').lower()
                profile = self._empty_result(word)
                profile['query'] = word
                profile['corrected_query'] = expanded_term
                profile['corrected_display_query'] = expanded_term
                profile['processing_time_ms'] = round(elapsed, 2)
                profile['query_array'] = [{'position': 0, 'word': word_lower, 'status': 'resolved'}]
                profile['corrected_array'] = [{'position': 0, 'word': expanded_term}]
                profile['search_terms'] = [expanded_term]
                profile['stats'] = {
                    'total_words': 1, 'valid_words': 1,
                    'corrected_words': 0, 'unknown_words': 0,
                    'stopwords': 0, 'ngram_count': 0,
                }
                profile['terms'] = [{
                    'position': 0,
                    'word': word_lower,
                    'status': 'resolved',
                    'is_stopword': False,
                    'part_of_ngram': False,
                    'pos': best['pos'],
                    'category': best['category'],
                    'description': best['description'],
                    'display': best['display'],
                    'rank': best['rank'],
                    'entity_type': best['entity_type'],
                    'match_count': len(expanded_matches),
                    'abbreviation': {
                        'original': word_lower,
                        'expanded_to': expanded_term,
                    },
                }]

                # Classify into entity lists
                if cat_lower in CITY_CATEGORIES:
                    profile['cities'] = [{'name': best['display'], 'rank': best['rank'], 'category': best['category']}]
                    profile['intent_scores']['location'] = best['rank']
                    profile['primary_intent'] = 'location'
                elif cat_lower in STATE_CATEGORIES:
                    profile['states'] = [{
                        'name': best['display'], 'rank': best['rank'],
                        'category': best['category'],
                        'variants': self._get_state_variants(best['display']),
                    }]
                    profile['intent_scores']['location'] = best['rank']
                    profile['primary_intent'] = 'location'
                elif cat_lower in LOCATION_CATEGORIES:
                    profile['location_terms'] = [expanded_term]
                    profile['intent_scores']['location'] = best['rank']
                    profile['primary_intent'] = 'location'
                elif cat_lower in PERSON_CATEGORIES:
                    profile['persons'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                    profile['intent_scores']['person'] = best['rank']
                    profile['primary_intent'] = 'person'
                elif cat_lower in ORGANIZATION_CATEGORIES:
                    profile['organizations'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                    profile['intent_scores']['organization'] = best['rank']
                    profile['primary_intent'] = 'organization'
                else:
                    profile['keywords'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                    profile['intent_scores']['keyword'] = best['rank']
                    profile['primary_intent'] = 'keyword'

                return profile

        # --- Check stopword ---
        if word_lower in STOPWORDS:
            elapsed = (time.perf_counter() - start_time) * 1000
            profile = self._empty_result(word)
            profile['query'] = word
            profile['corrected_query'] = word_lower
            profile['corrected_display_query'] = word_lower
            profile['processing_time_ms'] = round(elapsed, 2)
            profile['query_array'] = [{'position': 0, 'word': word_lower, 'status': 'stopword'}]
            profile['corrected_array'] = [{'position': 0, 'word': word_lower}]
            profile['stats'] = {
                'total_words': 1, 'valid_words': 1,
                'corrected_words': 0, 'unknown_words': 0,
                'stopwords': 1, 'ngram_count': 0,
            }
            profile['terms'] = [{
                'position': 0,
                'word': word_lower,
                'status': 'stopword',
                'is_stopword': True,
                'part_of_ngram': False,
                'pos': STOPWORDS[word_lower],
                'category': 'stopword',
                'description': '',
                'display': word_lower,
                'rank': 0,
                'match_count': 0,
            }]
            return profile

        # --- RAM hash lookup ---
        matches = self.cache.get_term_matches(word_lower)

        if matches:
            best = matches[0]
            elapsed = (time.perf_counter() - start_time) * 1000

            if self.verbose:
                print(f"  '{word_lower}' → Found ({len(matches)} matches)")
                print(f"  Selected: {best['category']} (rank={best['rank']})")

            profile = self._empty_result(word)
            profile['query'] = word
            profile['corrected_query'] = word_lower
            profile['corrected_display_query'] = word_lower
            profile['processing_time_ms'] = round(elapsed, 2)
            profile['query_array'] = [{'position': 0, 'word': word_lower, 'status': 'resolved'}]
            profile['corrected_array'] = [{'position': 0, 'word': word_lower}]
            profile['search_terms'] = [word_lower]
            profile['stats'] = {
                'total_words': 1, 'valid_words': 1,
                'corrected_words': 0, 'unknown_words': 0,
                'stopwords': 0, 'ngram_count': 0,
            }
            profile['terms'] = [{
                'position': 0,
                'word': word_lower,
                'status': 'resolved',
                'is_stopword': False,
                'part_of_ngram': False,
                'pos': best['pos'],
                'category': best['category'],
                'description': best['description'],
                'display': best['display'],
                'rank': best['rank'],
                'entity_type': best['entity_type'],
                'match_count': len(matches),
            }]

            # Classify
            cat_lower = best.get('category', '').lower()
            if cat_lower in CITY_CATEGORIES:
                profile['cities'] = [{'name': best['display'], 'rank': best['rank'], 'category': best['category']}]
                profile['intent_scores']['location'] = best['rank']
                profile['primary_intent'] = 'location'
            elif cat_lower in STATE_CATEGORIES:
                profile['states'] = [{'name': best['display'], 'rank': best['rank'], 'category': best['category'], 'variants': self._get_state_variants(best['display'])}]
                profile['intent_scores']['location'] = best['rank']
                profile['primary_intent'] = 'location'
            elif cat_lower in LOCATION_CATEGORIES:
                profile['location_terms'] = [word_lower]
                profile['intent_scores']['location'] = best['rank']
                profile['primary_intent'] = 'location'
            elif cat_lower in PERSON_CATEGORIES:
                profile['persons'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                profile['intent_scores']['person'] = best['rank']
                profile['primary_intent'] = 'person'
            elif cat_lower in ORGANIZATION_CATEGORIES:
                profile['organizations'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                profile['intent_scores']['organization'] = best['rank']
                profile['primary_intent'] = 'organization'
            elif cat_lower in MEDIA_CATEGORIES:
                profile['media'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                profile['intent_scores']['media'] = best['rank']
                profile['primary_intent'] = 'media'
            else:
                profile['keywords'] = [{'phrase': best['term'], 'display': best['display'], 'rank': best['rank'], 'category': best['category'], 'type': 'unigram'}]
                profile['intent_scores']['keyword'] = best['rank']
                profile['primary_intent'] = 'keyword'

            return profile

        # --- Not in RAM hash — try Redis fuzzy suggestion ---
        suggestions = get_fuzzy_suggestions(word_lower, limit=5, max_distance=2)
        elapsed = (time.perf_counter() - start_time) * 1000

        if suggestions:
            best = suggestions[0]

            if self.verbose:
                print(f"  '{word_lower}' → Not in RAM hash. "
                      f"Suggestion: '{best['term']}' (distance={best['distance']}). "
                      f"Original kept for search.")

            profile = self._empty_result(word)
            profile['query'] = word
            profile['corrected_query'] = word_lower
            profile['corrected_display_query'] = best['term']
            profile['processing_time_ms'] = round(elapsed, 2)
            profile['query_array'] = [{'position': 0, 'word': word_lower, 'status': 'unknown_suggest'}]
            profile['corrected_array'] = [{'position': 0, 'word': best['term']}]
            profile['search_terms'] = [word_lower]
            profile['corrections'] = [{
                'position': 0,
                'original': word_lower,
                'corrected': best['term'],
                'distance': best['distance'],
                'pos': best['pos'],
                'category': best['category'],
                'correction_type': 'suggestion',
            }]
            profile['stats'] = {
                'total_words': 1, 'valid_words': 0,
                'corrected_words': 0, 'unknown_words': 1,
                'stopwords': 0, 'ngram_count': 0,
            }
            profile['terms'] = [{
                'position': 0,
                'word': word_lower,
                'status': 'unknown_suggest',
                'is_stopword': False,
                'part_of_ngram': False,
                'pos': 'unknown',
                'predicted_pos': 'noun',
                'category': '',
                'description': '',
                'display': word_lower,
                'rank': 0,
                'match_count': 0,
                'suggestion': best['term'],
                'suggestion_display': best['display'],
                'suggestion_distance': best['distance'],
            }]
            return profile

        # --- Truly unknown — no suggestion available ---
        if self.verbose:
            print(f"  '{word_lower}' → Unknown (no suggestions). Original passed through.")

        profile = self._empty_result(word)
        profile['query'] = word
        profile['corrected_query'] = word_lower
        profile['corrected_display_query'] = word_lower
        profile['processing_time_ms'] = round(elapsed, 2)
        profile['query_array'] = [{'position': 0, 'word': word_lower, 'status': 'unknown'}]
        profile['corrected_array'] = [{'position': 0, 'word': word_lower}]
        profile['search_terms'] = [word_lower]
        profile['stats'] = {
            'total_words': 1, 'valid_words': 0,
            'corrected_words': 0, 'unknown_words': 1,
            'stopwords': 0, 'ngram_count': 0,
        }
        profile['terms'] = [{
            'position': 0,
            'word': word_lower,
            'status': 'unknown',
            'is_stopword': False,
            'part_of_ngram': False,
            'pos': 'unknown',
            'predicted_pos': 'noun',
            'category': '',
            'description': '',
            'display': word_lower,
            'rank': 0,
            'match_count': 0,
        }]
        return profile


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def process_query(query: str, verbose: bool = False) -> Dict[str, Any]:
    """Main entry point — process a query through word discovery v3."""
    wd = WordDiscovery(verbose=verbose)
    return wd.process(query)


def print_output(output: Dict[str, Any]) -> None:
    """Print the output in a readable JSON format."""
    print("\n" + "=" * 70)
    print("📄 COMPLETE PROFILE (JSON)")
    print("=" * 70)
    print(json.dumps(output, indent=2))


# =============================================================================
# MAIN — TEST SCRIPT
# =============================================================================

def main():
    """Run test queries."""
    import sys

    test_queries = [
        "black restaurants in north carolina",
        "north face jackets",
        "georgia peach cobbler",
        "restaurants in georgia",
        "where is africa located",
        "african food near me",
        "resturants in atl",
        "best hbcus in nc",
    ]

    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])
        test_queries = [query]

    print("\n" + "#" * 70)
    print("# WORD DISCOVERY v3 — TEST")
    print(f"# RAM Cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
    print("#" * 70)

    for query in test_queries:
        output = process_query(query, verbose=True)
        print_output(output)
        print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    main()