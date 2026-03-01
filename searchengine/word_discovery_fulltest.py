# # word_discovery_fulltest.py
# """
# word_discovery_v2.py
# ====================
# Optimized word discovery with POS-aware selection and n-gram detection.

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
    
#     Step 6: Re-check n-grams after correction (RAM)
#             - Corrected words may form new n-grams
    
#     Step 7: Build final output for Typesense

# PERFORMANCE:
#     - Steps 1-4, 6-7: RAM only (~0.01ms per lookup)
#     - Step 5: Redis only for unknown words (~50ms per correction)
#     - Typical query (no typos): ~1-5ms
#     - Query with 1-2 typos: ~50-150ms

# EDGE CASES HANDLED:
#     - Multiple meanings for same word (black = city vs adjective)
#     - Misspelled words preserve position
#     - Adjacent unknown words
#     - N-grams with corrected words
#     - All words unknown
#     - Single word queries
#     - Stopwords in n-grams
# """

# import json
# import time
# import redis
# from typing import Dict, Any, List, Optional, Tuple, Set
# from decouple import config


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION = config('REDIS_LOCATION')
# REDIS_PORT = config('REDIS_PORT', cast=int)
# REDIS_DB = config('REDIS_DB', default=0, cast=int)
# REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
# REDIS_USERNAME = config('REDIS_USERNAME', default='')


# # =============================================================================
# # REDIS CONNECTION (Only for spelling correction)
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
# # RAM CACHE SIMULATOR (Replace with your actual vocab_cache)
# # =============================================================================

# class VocabCache:
#     """
#     RAM-based vocabulary cache.
    
#     In production, replace this with your actual vocab_cache implementation.
#     This version uses Redis to simulate RAM lookups for testing.
#     """
    
#     def __init__(self):
#         self._client = None
#         self._loaded = False
    
#     def _get_client(self) -> Optional[redis.Redis]:
#         if self._client is None:
#             self._client = RedisClient.get_client()
#         return self._client
    
#     def get_term_matches(self, word: str) -> List[Dict[str, Any]]:
#         """
#         Get ALL matches for a word (not just highest rank).
#         Returns list of all category variants.
#         """
#         client = self._get_client()
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
#                         'key': key,
#                         'term': metadata.get('term', word_lower),
#                         'display': metadata.get('display', word_lower),
#                         'category': metadata.get('category', ''),
#                         'description': metadata.get('description', ''),
#                         'pos': self._normalize_pos(metadata.get('pos')),
#                         'entity_type': metadata.get('entity_type', 'unigram'),
#                         'rank': rank,
#                     })
            
#             # Sort by rank descending
#             matches.sort(key=lambda x: x['rank'], reverse=True)
#             return matches
            
#         except Exception as e:
#             print(f"Error getting term matches for '{word}': {e}")
#             return []
    
#     def get_ngram(self, words: List[str]) -> Optional[Dict[str, Any]]:
#         """
#         Check if words form an n-gram (bigram, trigram, quadgram).
#         """
#         client = self._get_client()
#         if not client or len(words) < 2:
#             return None
        
#         # phrase = ' '.join(w.lower() for w in words)
#         # pattern = f"term:{phrase}:*"
#         phrase = ' '.join(w.lower() for w in words)
#         phrase_underscore = '_'.join(w.lower() for w in words)

#         # Try underscore version first (most common in Redis)
#         pattern = f"term:{phrase_underscore}:*"
#         keys = client.keys(pattern)

#         # If not found, try space version
#         if not keys:
#             pattern = f"term:{phrase}:*"
#             keys = client.keys(pattern)
        
#         try:
#             keys = client.keys(pattern)
#             if not keys:
#                 return None
            
#             # Get first match (highest priority)
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
#             # Handle JSON-like string: "['noun']" -> "noun"
#             if pos_value.startswith('['):
#                 # Try standard JSON first
#                 try:
#                     parsed = json.loads(pos_value)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 # Try replacing single quotes with double quotes (Python repr format)
#                 try:
#                     fixed = pos_value.replace("'", '"')
#                     parsed = json.loads(fixed)
#                     if isinstance(parsed, list) and parsed:
#                         return str(parsed[0]).lower()
#                 except json.JSONDecodeError:
#                     pass
                
#                 # Manual extraction as last resort: ['noun'] -> noun
#                 if pos_value.startswith("['") and pos_value.endswith("']"):
#                     inner = pos_value[2:-2]  # Remove [' and ']
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
# vocab_cache = VocabCache()


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

# # Format: (left_pos, right_pos) -> predicted_pos
# # None means unknown/missing neighbor
# # "end" means end of query

# GRAMMAR_RULES = {
#     # === BOTH NEIGHBORS KNOWN ===
    
# #     # Determiner/Article patterns
# #     ("determiner", "noun"): "adjective",
# #     ("determiner", "adjective"): "adverb",
# #     ("determiner", "verb"): "noun",
# #     ("determiner", "preposition"): "noun",
# #     ("article", "noun"): "adjective",
# #     ("article", "adjective"): "adverb",
# #     ("article", "verb"): "noun",
# #     ("article", "preposition"): "noun",
    
# #     # Adjective patterns
# #     ("adjective", "noun"): "adjective",  # "big red ball" - red is adjective
# #     ("adjective", "verb"): "noun",
# #     ("adjective", "preposition"): "noun",
# #     ("adjective", "adjective"): "noun",  # "old old man" - middle could be noun
    
# #     # Noun patterns
# #     ("noun", "noun"): "verb",  # "dog cat runs" - middle should be verb? Actually rare
# #     ("noun", "adjective"): "verb",  # "dog big" → "dog is big" - missing verb
# #     ("noun", "adverb"): "verb",
# #     ("noun", "preposition"): "verb",
# #     ("noun", "verb"): "adverb",
    
# #     # Verb patterns
# #     ("verb", "noun"): "adjective",  # "eat good food" - good is adjective
# #     ("verb", "adjective"): "adverb",  # "run very fast" - very is adverb
# #     ("verb", "verb"): "adverb",
# #     ("verb", "preposition"): "noun",
    
# #     # Preposition patterns
# #     ("preposition", "noun"): "adjective",  # "in big city" - big is adjective
# #     ("preposition", "adjective"): "adverb",
# #     ("preposition", "verb"): "noun",
# #     ("preposition", "preposition"): "noun",
# #     ("preposition", "end"): "noun",  # "in ___" at end → noun
    
# #     # Pronoun patterns
# #     ("pronoun", "noun"): "verb",
# #     ("pronoun", "adjective"): "verb",
# #     ("pronoun", "adverb"): "verb",
# #     ("pronoun", "preposition"): "verb",
# #     ("pronoun", "verb"): "adverb",
    
# #     # Be verb patterns
# #     ("be", "noun"): "adjective",
# #     ("be", "adjective"): "adverb",
# #     ("be", "preposition"): "noun",
# #     ("be", "verb"): "adverb",
    
# #     # Adverb patterns
# #     ("adverb", "noun"): "adjective",
# #     ("adverb", "adjective"): "adverb",
# #     ("adverb", "verb"): "adverb",
    
# #     # === ONLY LEFT NEIGHBOR KNOWN ===
# #     ("determiner", None): "noun",
# #     ("article", None): "noun",
# #     ("adjective", None): "noun",
# #     ("noun", None): "verb",
# #     ("verb", None): "noun",
# #     ("preposition", None): "noun",
# #     ("pronoun", None): "verb",
# #     ("be", None): "adjective",
# #     ("adverb", None): "adjective",
# #     ("conjunction", None): "noun",
    
# #     # === ONLY RIGHT NEIGHBOR KNOWN ===
# #     (None, "noun"): "adjective",
# #     (None, "adjective"): "adverb",
# #     (None, "verb"): "noun",
# #     (None, "adverb"): "verb",
# #     (None, 'adjective'): 'adjective',
# #     (None, "preposition"): "noun",
# #     (None, "determiner"): "verb",
# #     (None, "pronoun"): "verb",
# #     (None, "conjunction"): "noun",
# #     (None, "end"): "noun",

# # =========================================================================
#     # BOTH NEIGHBORS KNOWN
#     # =========================================================================
    
#     # --- Determiner patterns ---
#     ("determiner", "noun"): [("adjective", 0.95), ("noun", 0.60)],
#     ("determiner", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("determiner", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("determiner", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("determiner", "conjunction"): [("noun", 0.85)],
#     ("determiner", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     # --- Article patterns ---
#     ("article", "noun"): [("adjective", 0.95), ("noun", 0.55)],
#     ("article", "adjective"): [("adverb", 0.90), ("adjective", 0.70)],
#     ("article", "verb"): [("noun", 0.90), ("adjective", 0.65)],
#     ("article", "preposition"): [("noun", 0.90), ("adjective", 0.60)],
#     ("article", "adverb"): [("adjective", 0.85), ("noun", 0.70)],
#     ("article", "conjunction"): [("noun", 0.85)],
#     ("article", "end"): [("noun", 0.95), ("adjective", 0.70)],
    
#     # --- Adjective patterns ---
#     ("adjective", "noun"): [("adjective", 0.90), ("noun", 0.50)],
#     ("adjective", "adjective"): [("adjective", 0.85), ("noun", 0.65), ("adverb", 0.50)],
#     ("adjective", "verb"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "preposition"): [("noun", 0.90), ("proper_noun", 0.75)],
#     ("adjective", "adverb"): [("noun", 0.85), ("verb", 0.65)],
#     ("adjective", "conjunction"): [("noun", 0.90)],
#     ("adjective", "end"): [("noun", 0.95), ("proper_noun", 0.80)],
    
#     # --- Noun patterns ---
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
    
#     # --- Proper noun patterns ---
#     ("proper_noun", "noun"): [("verb", 0.80), ("noun", 0.60)],
#     ("proper_noun", "adjective"): [("verb", 0.90), ("be", 0.80)],
#     ("proper_noun", "adverb"): [("verb", 0.90)],
#     ("proper_noun", "preposition"): [("verb", 0.85)],
#     ("proper_noun", "verb"): [("adverb", 0.85), ("proper_noun", 0.60)],
#     ("proper_noun", "conjunction"): [("verb", 0.80)],
#     ("proper_noun", "end"): [("verb", 0.75), ("proper_noun", 0.70)],
    
#     # --- Verb patterns ---
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
    
#     # --- Preposition patterns ---
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
    
#     # --- Pronoun patterns ---
#     ("pronoun", "noun"): [("verb", 0.95), ("be", 0.80)],
#     ("pronoun", "adjective"): [("verb", 0.95), ("be", 0.85), ("adverb", 0.55)],
#     ("pronoun", "adverb"): [("verb", 0.95), ("be", 0.75)],
#     ("pronoun", "preposition"): [("verb", 0.90), ("be", 0.70)],
#     ("pronoun", "verb"): [("adverb", 0.85), ("modal", 0.70)],
#     ("pronoun", "conjunction"): [("verb", 0.85)],
#     ("pronoun", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     # --- Be verb patterns ---
#     ("be", "noun"): [("adjective", 0.90), ("determiner", 0.80), ("adverb", 0.55)],
#     ("be", "adjective"): [("adverb", 0.95), ("adjective", 0.60)],
#     ("be", "adverb"): [("adjective", 0.85), ("verb", 0.70)],
#     ("be", "verb"): [("adverb", 0.90), ("noun", 0.55)],
#     ("be", "preposition"): [("noun", 0.90), ("adverb", 0.70)],
#     ("be", "conjunction"): [("adjective", 0.80), ("noun", 0.65)],
#     ("be", "end"): [("adjective", 0.90), ("noun", 0.75), ("adverb", 0.60)],
    
#     # --- Adverb patterns ---
#     ("adverb", "noun"): [("adjective", 0.90), ("verb", 0.65)],
#     ("adverb", "adjective"): [("adverb", 0.85), ("adjective", 0.70)],
#     ("adverb", "adverb"): [("adverb", 0.80), ("verb", 0.65)],
#     ("adverb", "verb"): [("adverb", 0.90), ("noun", 0.50)],
#     ("adverb", "preposition"): [("verb", 0.85), ("noun", 0.65)],
#     ("adverb", "conjunction"): [("verb", 0.80), ("adjective", 0.65)],
#     ("adverb", "end"): [("adjective", 0.85), ("verb", 0.75), ("noun", 0.60)],
    
#     # --- Conjunction patterns ---
#     ("conjunction", "noun"): [("adjective", 0.85), ("determiner", 0.80), ("verb", 0.55)],
#     ("conjunction", "adjective"): [("adverb", 0.85), ("noun", 0.65)],
#     ("conjunction", "adverb"): [("noun", 0.80), ("verb", 0.70)],
#     ("conjunction", "verb"): [("noun", 0.90), ("pronoun", 0.75)],
#     ("conjunction", "preposition"): [("noun", 0.85), ("pronoun", 0.70)],
#     ("conjunction", "conjunction"): [("noun", 0.80)],
#     ("conjunction", "end"): [("noun", 0.90), ("proper_noun", 0.75)],
    
#     # --- Modal verb patterns ---
#     ("modal", "noun"): [("verb", 0.90), ("adverb", 0.65)],
#     ("modal", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("modal", "adverb"): [("verb", 0.95)],
#     ("modal", "verb"): [("adverb", 0.90)],
#     ("modal", "preposition"): [("verb", 0.85)],
#     ("modal", "end"): [("verb", 0.95), ("noun", 0.55)],
    
#     # --- Auxiliary verb patterns ---
#     ("auxiliary", "noun"): [("verb", 0.90), ("adjective", 0.65)],
#     ("auxiliary", "adjective"): [("verb", 0.85), ("adverb", 0.70)],
#     ("auxiliary", "adverb"): [("verb", 0.95)],
#     ("auxiliary", "verb"): [("adverb", 0.90)],
#     ("auxiliary", "preposition"): [("verb", 0.85)],
#     ("auxiliary", "end"): [("verb", 0.90), ("noun", 0.60)],
    
#     # --- Participle patterns ---
#     ("participle", "noun"): [("adjective", 0.85), ("noun", 0.65)],
#     ("participle", "adjective"): [("adverb", 0.85), ("noun", 0.60)],
#     ("participle", "adverb"): [("noun", 0.80), ("adjective", 0.65)],
#     ("participle", "verb"): [("adverb", 0.80), ("noun", 0.65)],
#     ("participle", "preposition"): [("noun", 0.85), ("adverb", 0.65)],
#     ("participle", "end"): [("noun", 0.90), ("adverb", 0.70)],
    
#     # --- Gerund patterns ---
#     ("gerund", "noun"): [("adjective", 0.85), ("noun", 0.70)],
#     ("gerund", "adjective"): [("adverb", 0.80), ("noun", 0.65)],
#     ("gerund", "adverb"): [("noun", 0.80)],
#     ("gerund", "verb"): [("adverb", 0.80), ("noun", 0.60)],
#     ("gerund", "preposition"): [("noun", 0.85)],
#     ("gerund", "end"): [("noun", 0.90), ("adverb", 0.65)],
    
#     # --- Negation patterns ---
#     ("negation", "noun"): [("adjective", 0.85), ("verb", 0.75)],
#     ("negation", "adjective"): [("adverb", 0.90)],
#     ("negation", "adverb"): [("verb", 0.85), ("adjective", 0.70)],
#     ("negation", "verb"): [("adverb", 0.90)],
#     ("negation", "preposition"): [("verb", 0.80)],
#     ("negation", "end"): [("verb", 0.85), ("adjective", 0.70)],
    
#     # --- Quantifier patterns ---
#     ("quantifier", "noun"): [("adjective", 0.90), ("noun", 0.60)],
#     ("quantifier", "adjective"): [("adverb", 0.85), ("adjective", 0.65)],
#     ("quantifier", "adverb"): [("adjective", 0.80)],
#     ("quantifier", "verb"): [("noun", 0.85)],
#     ("quantifier", "preposition"): [("noun", 0.85)],
#     ("quantifier", "end"): [("noun", 0.95), ("adjective", 0.65)],
    
#     # --- Numeral patterns ---
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
#     # SPECIAL CASES - Start of query (no left neighbor, explicit start marker)
#     # =========================================================================
    
#     ("start", "noun"): [("adjective", 0.90), ("determiner", 0.85)],
#     ("start", "adjective"): [("adverb", 0.85), ("determiner", 0.75)],
#     ("start", "verb"): [("noun", 0.90), ("pronoun", 0.80)],
#     ("start", "adverb"): [("verb", 0.80), ("adjective", 0.70)],
#     ("start", "preposition"): [("noun", 0.85), ("verb", 0.70)],
#     ("start", "end"): [("noun", 0.90), ("proper_noun", 0.85), ("verb", 0.60)],  # Single word query

#  }


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
#     # Normalize both POS values first
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
#         # Handle JSON-like string: "['noun']" -> "noun"
#         if pos_value.startswith('['):
#             # Try standard JSON first
#             try:
#                 parsed = json.loads(pos_value)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             # Try replacing single quotes with double quotes (Python repr format)
#             try:
#                 fixed = pos_value.replace("'", '"')
#                 parsed = json.loads(fixed)
#                 if isinstance(parsed, list) and parsed:
#                     return str(parsed[0]).lower()
#             except json.JSONDecodeError:
#                 pass
            
#             # Manual extraction as last resort: ['noun'] -> noun
#             if pos_value.startswith("['") and pos_value.endswith("']"):
#                 inner = pos_value[2:-2]  # Remove [' and ']
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
#         # Use RediSearch fuzzy query
#         suggestions = []
        
#         # Try 1 edit distance first, then 2
#         for fuzzy_level in ['%', '%%']:
#             if fuzzy_level == '%%' and suggestions:
#                 break  # Already have results from level 1
            
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
                            
#                             if distance <= max_distance and distance > 0:  # Exclude exact matches
#                                 rank = metadata.get('rank', 0)
#                                 try:
#                                     rank = int(float(rank))
#                                 except (ValueError, TypeError):
#                                     rank = 0
                                
#                                 # Normalize POS
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
        
#         # Sort by distance first, then by rank (higher is better)
#         suggestions.sort(key=lambda x: (x['distance'], -x['rank']))
        
#         # Remove duplicates (keep first occurrence)
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


# # =============================================================================
# # MAIN WORD DISCOVERY CLASS
# # =============================================================================

# class WordDiscovery:
#     """
#     Word Discovery Engine - The brain behind Typesense queries.
    
#     Processes queries to:
#     1. Identify known words and their meanings
#     2. Detect n-grams (bigrams, trigrams, quadgrams)
#     3. Correct misspellings while preserving position
#     4. Select the RIGHT meaning based on POS context
#     5. Build enriched output for Typesense
#     """
    
#     def __init__(self, verbose: bool = False):
#         self.verbose = verbose
#         self.cache = vocab_cache
    
#     def process(self, query: str) -> Dict[str, Any]:
#         """
#         Main entry point - process a query through all steps.
#         """
#         start_time = time.perf_counter()
        
#         if self.verbose:
#             print("\n" + "=" * 70)
#             print(f"🔍 WORD DISCOVERY: '{query}'")
#             print("=" * 70)
        
#         # Handle empty query
#         if not query or not query.strip():
#             return self._empty_result(query)
        
#         # Stripping the last punctuation or the trailing space
#         # words = query.lower().split()
#         words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
#         words = [w for w in words if w]
        
#         # Edge case: single word
#         if len(words) == 1:
#             return self._process_single_word(words[0], start_time)
        
#         # =====================================================================
#         # STEP 1: Collect ALL data (RAM)
#         # =====================================================================
#         word_data = self._step1_collect_all_data(words)
        
#         # =====================================================================
#         # STEP 2: Mark n-gram positions (longest first)
#         # =====================================================================
#         ngrams, consumed_positions = self._step2_detect_ngrams(words, word_data)
        
#         # =====================================================================
#         # STEP 3: Determine POS for remaining words
#         # =====================================================================
#         self._step3_determine_pos(word_data, consumed_positions)

#         # =====================================================================
#         # STEP 3.5: Refine POS from suffixes
#         # =====================================================================
#         self._step3_5_refine_pos_from_suffix(word_data)
        
#         # =====================================================================
#         # STEP 4: Select best match (POS + Rank)
#         # =====================================================================
#         self._step4_select_best_match(word_data, consumed_positions)
        
#         # =====================================================================
#         # STEP 5: Handle unknowns (spelling correction)
#         # =====================================================================
#         corrections = self._step5_correct_unknowns(word_data)
        
#         # =====================================================================
#         # STEP 6: Re-check n-grams after correction
#         # =====================================================================
#         corrected_words = self._get_working_words(word_data)
#         new_ngrams, new_consumed = self._step6_recheck_ngrams(
#             corrected_words, word_data, consumed_positions
#         )
#         ngrams.extend(new_ngrams)
#         consumed_positions.update(new_consumed)
        
#         # =====================================================================
#         # STEP 7: Build final output
#         # =====================================================================
#         output = self._step7_build_output(
#             query, word_data, ngrams, consumed_positions, corrections, start_time
#         )
        
#         return output
    
#     # =========================================================================
#     # STEP 1: Collect ALL Data
#     # =========================================================================
    
#     def _step1_collect_all_data(self, words: List[str]) -> List[Dict[str, Any]]:
#         """
#         Collect all matches for each word (don't pick winner yet).
#         Also collect all possible n-grams.
#         """
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
            
#             # Get ALL matches from cache
#             matches = self.cache.get_term_matches(word_lower)
            
#             if matches:
#                 word_data.append({
#                     'position': position,
#                     'word': word_lower,
#                     'status': 'found',
#                     'is_stopword': False,
#                     'pos': None,  # Will be determined in Step 3
#                     'predicted_pos': None,
#                     'all_matches': matches,
#                     'selected_match': None,  # Will be selected in Step 4
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
#     # STEP 2: Detect N-grams (Longest First)
#     # =========================================================================
    
#     def _step2_detect_ngrams(
#         self, 
#         words: List[str],
#         word_data: List[Dict[str, Any]]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """
#         Detect all n-grams, prioritizing longest matches.
#         Returns (ngrams_found, consumed_positions)
#         """
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔗 STEP 2: Detect N-grams (Longest First)")
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
    

# # =========================================================================
#     # STEP 3: Determine POS for ALL Words (including n-gram members)
#     # =========================================================================
    
#     def _step3_determine_pos(
#         self, 
#         word_data: List[Dict[str, Any]], 
#         consumed_positions: Set[int]
#     ) -> None:
#         """
#         Determine POS for ALL words using grammar rules.
        
#         IMPORTANT: Run POS on EVERY word, including those in n-grams.
#         This ensures "black" gets adjective (not city) even when part of "black owned".
        
#         Stores:
#         - predicted_pos: string (e.g., 'noun') for Step 4 compatibility
#         - predicted_pos_list: list of tuples (e.g., [('noun', 0.95), ('proper_noun', 0.85)]) for Step 5
#         """
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🧠 STEP 3: Determine POS (Grammar Rules)")
#             print("-" * 70)
        
#         for i, wd in enumerate(word_data):
#             position = wd['position']
            
#             # Mark if part of n-gram (for reference), but DON'T skip
#             pos_index = position - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions
            
#             # Skip stopwords (already have POS)
#             if wd['is_stopword']:
#                 wd['predicted_pos'] = wd['pos']
#                 wd['predicted_pos_list'] = [(wd['pos'], 1.0)]
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Stopword ({wd['pos']})")
#                 continue
            
#             # Get left neighbor POS (normalized!)
#             left_pos = None
#             if i > 0:
#                 left_wd = word_data[i - 1]
#                 if left_wd['is_stopword']:
#                     left_pos = left_wd['pos']
#                 elif left_wd.get('predicted_pos'):
#                     # Handle both string and tuple formats
#                     pred = left_wd['predicted_pos']
#                     if isinstance(pred, tuple):
#                         left_pos = pred[0]
#                     else:
#                         left_pos = pred
#                 elif left_wd['all_matches']:
#                     # Use highest-ranked match's POS as hint - NORMALIZE IT!
#                     raw_pos = left_wd['all_matches'][0]['pos']
#                     left_pos = normalize_pos_string(raw_pos)
#             else:
#                 # First word in query - use 'start' marker for LOCAL_CONTEXT_RULES
#                 left_pos = 'start'
            
#             # Get right neighbor POS (normalized!)
#             right_pos = None
#             if i < len(word_data) - 1:
#                 right_wd = word_data[i + 1]
#                 if right_wd['is_stopword']:
#                     right_pos = right_wd['pos']
#                 elif right_wd['all_matches']:
#                     # NORMALIZE the POS!
#                     raw_pos = right_wd['all_matches'][0]['pos']
#                     right_pos = normalize_pos_string(raw_pos)
#             else:
#                 right_pos = 'end'
            
#             # Apply grammar rules - try LOCAL_CONTEXT_RULES first, then GRAMMAR_RULES
#             predicted_pos = None
            
#             # Try both neighbors with LOCAL_CONTEXT_RULES (has confidence scores)
#             if left_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))
#                 if not predicted_pos:
#                     # Fall back to GRAMMAR_RULES
#                     simple_pos = GRAMMAR_RULES.get((left_pos, right_pos))
#                     if simple_pos:
#                         predicted_pos = [(simple_pos, 0.90)]
            
#             # Try left only
#             if not predicted_pos and left_pos:
#                 predicted_pos = GRAMMAR_RULES.get((left_pos, None))
#                 if not predicted_pos:
#                     simple_pos = GRAMMAR_RULES.get((left_pos, None))
#                     if simple_pos:
#                         predicted_pos = [(simple_pos, 0.85)]
            
#             # Try right only
#             if not predicted_pos and right_pos:
#                 predicted_pos = GRAMMAR_RULES.get((None, right_pos))
#                 if not predicted_pos:
#                     simple_pos = GRAMMAR_RULES.get((None, right_pos))
#                     if simple_pos:
#                         predicted_pos = [(simple_pos, 0.85)]
            
#             # Default to noun
#             if not predicted_pos:
#                 predicted_pos = [('noun', 0.75)]
            
#             # ---------------------------------------------------------
#             # PASSIVE VOICE LOOKAHEAD: [be] _X_ [-ed] → X is a noun
#             # "where is african located" → african = noun (subject)
#             # ---------------------------------------------------------
#             has_be_before = any(
#                 word_data[j]['is_stopword'] and word_data[j]['pos'] == 'be'
#                 for j in range(0, i)
#             )
#             next_ends_ed = False
#             if i + 1 < len(word_data):
#                 next_word_clean = word_data[i + 1]['word'].rstrip('?!.,;:')
#                 next_ends_ed = next_word_clean.endswith('ed')

#             if has_be_before and next_ends_ed and not wd['is_stopword']:
#                 # Check it's not already a known adverb in dictionary
#                 is_known_adverb = any(
#                     m['pos'] in ('adverb',)
#                     for m in wd.get('all_matches', [])
#                 )
#                 if not is_known_adverb:
#                     # Override: this word is the subject of a passive construction
#                     predicted_pos = [
#                         ('noun', 0.95),
#                         ('proper_noun', 0.85),
#                         ('adjective', 0.4),
#                         ('adverb', 0.3),
#                     ]
#                     if self.verbose:
#                         print(f"       ⚡ Passive voice detected: [be]..._{wd['word']}_...[-ed] → noun (0.95)")

#             # Store both formats for compatibility
#             if isinstance(predicted_pos, list):
#                 # LOCAL_CONTEXT_RULES format: [('noun', 0.95), ('proper_noun', 0.85)]
#                 wd['predicted_pos_list'] = predicted_pos
#                 wd['predicted_pos'] = predicted_pos[0][0] if predicted_pos else 'noun'
#             else:
#                 # GRAMMAR_RULES format: 'noun' (shouldn't happen now, but just in case)
#                 wd['predicted_pos_list'] = [(predicted_pos, 0.90)]
#                 wd['predicted_pos'] = predicted_pos
            
#             if self.verbose:
#                 context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
#                 ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
#                 print(f"  [{position}] '{wd['word']}' → Predicted: {wd['predicted_pos_list']}{ngram_note}")
#                 print(f"       Context: {context}")
# # =========================================================================
#     # STEP 3.5: Refine POS from Word Suffixes
#     # =========================================================================
    
#     # Suffix rules - maps suffix to likely POS with confidence
#     # Format: suffix -> [(pos, confidence), ...]
#     SUFFIX_POS_RULES = {
#         # Verb/Gerund/Adjective suffixes
#         'ing': [('gerund', 0.80), ('adjective', 0.75), ('verb', 0.70), ('noun', 0.50)],
#         'ed': [('verb', 0.85), ('adjective', 0.75), ('participle', 0.70)],
        
#         # Adverb suffixes
#         'ly': [('adverb', 0.95), ('adjective', 0.40)],
        
#         # Noun suffixes
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
        
#         # Adjective suffixes
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
        
#         # Verb suffixes
#         'ify': [('verb', 0.95)],
#         'ize': [('verb', 0.95)],
#         'ise': [('verb', 0.90)],
#         'ate': [('verb', 0.80), ('adjective', 0.60), ('noun', 0.50)],
#         'en': [('verb', 0.70), ('adjective', 0.60)],
        
#         # Person/Agent suffixes
#         'er': [('noun', 0.75), ('adjective', 0.65)],
#         'or': [('noun', 0.80)],
#         'ee': [('noun', 0.85)],
#         'eer': [('noun', 0.85)],
#     }
    
#     # Words that look like they have a suffix but don't follow the rule
#     SUFFIX_EXCEPTIONS = frozenset({
#         # -ing words that are NOT gerunds
#         'ring', 'king', 'sing', 'bring', 'thing', 'string', 'spring',
#         'wing', 'swing', 'sting', 'cling', 'fling', 'sling', 'wring',
#         'bling', 'ding', 'ping', 'zing', 'ming', 'bing', 'ling',
#         'beijing', 'sterling', 'viking', 'darling', 'ceiling',
#         'feeling', 'dealing', 'healing', 'meaning', 'evening',
        
#         # -ed words that are NOT verbs
#         'bed', 'red', 'shed', 'led', 'fed', 'wed', 'sled',
        
#         # -ly words that are NOT adverbs
#         'fly', 'ply', 'sly', 'holy', 'ugly', 'bully', 'belly',
#         'lily', 'jelly', 'jolly', 'rally', 'ally', 'tally',
#         'family', 'italy', 'july', 'daily', 'early',
        
#         # -er words that are NOT agents
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
        
#         # -al words that are NOT adjectives
#         'animal', 'hospital', 'capital', 'metal', 'total', 'eral',
#         'canal', 'eral', 'general', 'mineral', 'festival', 'signal',
#         'journal', 'rival', 'survival', 'arrival', 'proposal',
        
#         # -ful words that are exceptions
#         'awful',
        
#         # -age words that are NOT nouns
#         'age', 'page', 'sage', 'cage', 'rage', 'wage', 'stage',
        
#         # -ment words that are exceptions
#         'cement', 'moment', 'comment', 'segment', 'element',
        
#         # -ness words that are exceptions (very few)
        
#         # -ic words that are NOT adjectives
#         'music', 'magic', 'basic', 'logic', 'topic', 'panic',
#         'fabric', 'garlic', 'picnic', 'traffic', 'public',
        
#         # -est words that are NOT superlatives
#         'west', 'east', 'nest', 'rest', 'test', 'best', 'chest',
#         'quest', 'guest', 'forest', 'harvest', 'interest', 'protest',
#         'request', 'suggest', 'modest', 'honest', 'invest',
        
#         # -ate words that are NOT verbs
#         'gate', 'late', 'mate', 'date', 'fate', 'rate', 'state',
#         'plate', 'private', 'climate', 'chocolate', 'senate',
#         'debate', 'estate', 'ultimate', 'intimate', 'accurate',
#         'delicate', 'separate', 'desperate', 'moderate', 'adequate',
        
#         # -ish words that are NOT adjectives  
#         'fish', 'dish', 'wish', 'finish', 'publish', 'polish',
#         'vanish', 'perish', 'cherish', 'nourish', 'flourish',
#     })
    
#     # Minimum word length to apply suffix rules (avoid false positives on short words)
#     MIN_SUFFIX_WORD_LENGTH = 4
    
#     def _step3_5_refine_pos_from_suffix(
#         self,
#         word_data: List[Dict[str, Any]]
#     ) -> None:
#         """
#         Refine POS predictions using word suffixes.
        
#         Runs on ALL words (known and unknown).
#         Does NOT replace predictions - only refines confidence scores.
        
#         For known words:
#         - If suffix suggests adjective but context predicted verb, boost adjective
#         - Example: "running shoes" → boost adjective for "running"
        
#         For unknown words:
#         - Suffix gives strong signal for what POS the correction should have
#         - Example: "beautful" (misspelled) → suffix -ful → adjective
#         """
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🔤 STEP 3.5: Refine POS from Suffixes")
#             print("-" * 70)
        
#         for wd in word_data:
#             # Skip stopwords
#             if wd['is_stopword']:
#                 continue
            
#             word = wd.get('corrected', wd['word']).lower()
            
#             # Skip short words
#             if len(word) < self.MIN_SUFFIX_WORD_LENGTH:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Too short, skipping")
#                 continue
            
#             # Skip exceptions
#             if word in self.SUFFIX_EXCEPTIONS:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → Exception, skipping")
#                 continue
            
#             # Find matching suffix (try longest first)
#             matched_suffix = None
#             suffix_predictions = None
            
#             # Sort suffixes by length descending so we match longest first
#             # e.g., "ical" before "al", "ious" before "ous"
#             sorted_suffixes = sorted(
#                 self.SUFFIX_POS_RULES.keys(),
#                 key=len,
#                 reverse=True
#             )
            
#             for suffix in sorted_suffixes:
#                 if word.endswith(suffix) and len(word) > len(suffix) + 1:
#                     matched_suffix = suffix
#                     suffix_predictions = self.SUFFIX_POS_RULES[suffix]
#                     break
            
#             if not matched_suffix:
#                 if self.verbose:
#                     print(f"  [{wd['position']}] '{word}' → No suffix match")
#                 continue
            
#             # Get current predictions
#             current_list = wd.get('predicted_pos_list', [])
#             current_primary = wd.get('predicted_pos', 'noun')
            
#             # Build refined list
#             # Merge context predictions with suffix predictions
#             # Context predictions are weighted by their original confidence
#             # Suffix predictions boost or add POS options
            
#             refined = {}
            
#             # Add context predictions
#             for pos, conf in current_list:
#                 refined[pos] = conf
            
#             # Merge suffix predictions
#             for pos, suffix_conf in suffix_predictions:
#                 if pos in refined:
#                     # Boost: average of context and suffix confidence
#                     # But cap at 0.98
#                     existing = refined[pos]
#                     boosted = min((existing + suffix_conf) / 2 + 0.10, 0.98)
#                     refined[pos] = boosted
#                 else:
#                     # Add new POS from suffix (slightly reduced since context didn't predict it)
#                     refined[pos] = suffix_conf * 0.85
            
#             # Sort by confidence descending
#             refined_list = sorted(refined.items(), key=lambda x: -x[1])
            
#             # Update word data
#             wd['predicted_pos_list'] = refined_list
#             wd['predicted_pos'] = refined_list[0][0] if refined_list else current_primary
#             wd['suffix_detected'] = matched_suffix
            
#             if self.verbose:
#                 changed = wd['predicted_pos'] != current_primary
#                 change_marker = " ⚡ CHANGED" if changed else ""
#                 print(f"  [{wd['position']}] '{word}' → Suffix: -{matched_suffix}{change_marker}")
#                 print(f"       Before: {current_list}")
#                 print(f"       After:  {refined_list}")


#      # =========================================================================
#     # STEP 4: Select Best Match for ALL Words (including n-gram members)
#     # =========================================================================
    
#     def _step4_select_best_match(
#         self, 
#         word_data: List[Dict[str, Any]], 
#         consumed_positions: Set[int]
#     ) -> None:
#         """
#         For ALL words with multiple matches, select based on POS + rank.
        
#         IMPORTANT: Select best match for EVERY word, including those in n-grams.
#         This ensures "black" gets Dictionary Word (adjective) not US City (noun).
#         """
#         if self.verbose:
#             print("\n" + "-" * 70)
#             print("🎯 STEP 4: Select Best Match (POS + Rank)")
#             print("-" * 70)
        
#         for wd in word_data:
#             position = wd['position']
            
#             # Skip stopwords (already have selected_match)
#             if wd['is_stopword']:
#                 continue
            
#             # Skip unknowns (will be handled in Step 5)
#             if wd['status'] == 'unknown':
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Unknown (defer to Step 5)")
#                 continue
            
#             matches = wd['all_matches']
#             if not matches:
#                 continue
            
#             predicted_pos = wd['predicted_pos']
            
#             # Filter matches by POS compatibility (normalize POS for comparison)
#             compatible_matches = []
#             for m in matches:
#                 match_pos = normalize_pos_string(m['pos'])
#                 if is_pos_compatible(match_pos, predicted_pos):
#                     compatible_matches.append(m)
            
#             # Determine n-gram status for logging
#             pos_index = position - 1
#             is_in_ngram = pos_index in consumed_positions
#             ngram_note = " (in n-gram)" if is_in_ngram else ""
            
#             # Select best match
#             if compatible_matches:
#                 # Sort by rank (highest first)
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
#                 # No POS match - fall back to highest rank
#                 best = matches[0]  # Already sorted by rank
#                 wd['selected_match'] = best
#                 wd['pos'] = normalize_pos_string(best['pos'])
#                 wd['status'] = 'valid'
                
#                 if self.verbose:
#                     print(f"  [{position}] '{wd['word']}' → Fallback: {best['category']} (no POS match, using rank){ngram_note}")
 
#     # =========================================================================
#     # STEP 5: Correct Unknown Words + POS Mismatches
#     # =========================================================================
    
#     def _step5_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#         """
#         Correct unknown/misspelled words using fuzzy search.
#         Also corrects valid words where predicted POS doesn't match dictionary POS
#         with high confidence (contextual correction).
#         Position is preserved.
#         """
        
#         # --- Build candidate list ---
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
#                 # Check confidence threshold - only act on high-confidence predictions
#                 predicted_list = wd.get('predicted_pos_list', [])
#                 top_confidence = predicted_list[0][1] if predicted_list else 0
#                 if top_confidence >= 0.90:
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
#             print("🔧 STEP 5: Correct Unknowns (Redis Fuzzy)")
#             print("-" * 70)
        
#         corrections = []
        
#         # --- Process unknowns (existing logic, unchanged) ---
#         for wd in unknowns:
#             word = wd['word']
#             position = wd['position']
#             predicted_pos = wd['predicted_pos'] or 'noun'
            
#             # Get fuzzy suggestions
#             suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
#             if not suggestions:
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → No suggestions found")
#                 continue
            
#             # Filter by POS compatibility
#             compatible = [s for s in suggestions if is_pos_compatible(s['pos'], predicted_pos)]
            
#             # Select best correction
#             if compatible:
#                 # Sort by distance, then rank
#                 compatible.sort(key=lambda x: (x['distance'], -x['rank']))
#                 best = compatible[0]
#             else:
#                 # No POS match - use closest by distance
#                 best = suggestions[0]
            
#             # Update word data (position preserved!)
#             wd['status'] = 'corrected'
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
#             })
            
#             if self.verbose:
#                 print(f"  [{position}] '{word}' → '{best['term']}' (distance={best['distance']}, pos={best['pos']})")
#                 if len(suggestions) > 1:
#                     others = [s['term'] for s in suggestions[1:4] if s['term'] != best['term']]
#                     if others:
#                         print(f"       Other options: {others}")
        
#         # --- Process POS mismatches (new logic) ---
#         for wd in pos_mismatches:
#             word = wd['word']
#             position = wd['position']
#             predicted_pos = wd['predicted_pos']
#             original_pos = wd['pos']
            
#             # Tighter distance for valid words - they're not typos
#             suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
#             if not suggestions:
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no suggestions found")
#                 continue
            
#             # STRICT: only accept suggestions that match the predicted POS
#             compatible = [
#                 s for s in suggestions
#                 if is_pos_compatible(s['pos'], predicted_pos)
#                 and s['term'] != word  # must be a different word
#                 and s['distance'] <= 2  # keep it close
#             ]
            
#             if not compatible:
#                 # No POS-compatible alternative found - leave the word alone
#                 if self.verbose:
#                     print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no compatible alternative found")
#                 continue
            
#             # Sort by distance first, then rank
#             compatible.sort(key=lambda x: (x['distance'], -x['rank']))
#             best = compatible[0]
            
#             # Update word data
#             wd['status'] = 'pos_corrected'
#             wd['corrected'] = best['term']
#             wd['corrected_display'] = best['display']
#             wd['original_pos'] = original_pos
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
#                 'correction_type': 'pos_mismatch',
#             })
            
#             if self.verbose:
#                 print(f"  [{position}] '{word}' → '{best['term']}' (POS: {original_pos}→{best['pos']}, distance={best['distance']})")
#                 if len(compatible) > 1:
#                     others = [s['term'] for s in compatible[1:4] if s['term'] != best['term']]
#                     if others:
#                         print(f"       Other options: {others}")
        
#         return corrections
#     # =========================================================================
#     # STEP 6: Re-check N-grams After Correction
#     # =========================================================================
    
#     def _step6_recheck_ngrams(
#         self,
#         corrected_words: List[str],
#         word_data: List[Dict[str, Any]],
#         already_consumed: Set[int]
#     ) -> Tuple[List[Dict[str, Any]], Set[int]]:
#         """
#         Re-check for n-grams using corrected words.
#         Only checks positions not already consumed.
#         """
#         # Check if any words were corrected
#         # has_corrections = any(wd['status'] == 'corrected' for wd in word_data)
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
#             print("🔄 STEP 6: Re-check N-grams After Correction")
#             print("-" * 70)
        
#         new_ngrams = []
#         new_consumed = set()
        
#         words = corrected_words
        
#         # Check trigrams (quadgrams less common for corrected words)
#         if len(words) >= 3:
#             for i in range(len(words) - 2):
#                 if any(p in already_consumed or p in new_consumed for p in [i, i+1, i+2]):
#                     continue
                
#                 # Only check if at least one word was corrected in this range
#                 relevant_words = word_data[i:i+3]
#                 # if not any(wd['status'] == 'corrected' for wd in relevant_words):
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
#                 if not any(wd['status'] == 'corrected' for wd in relevant_words):
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
#         """
#         Build the final output structure for Typesense.
        
#         IMPORTANT: Include category/rank for ALL terms, even those in n-grams.
#         This allows the bridge to use individual term metadata for better decisions.
#         """
#         # Build corrected query
#         corrected_words = self._get_working_words(word_data)
#         corrected_query = ' '.join(corrected_words)
        
#         # Mark words that are part of n-grams
#         for wd in word_data:
#             pos_index = wd['position'] - 1
#             wd['part_of_ngram'] = pos_index in consumed_positions
        
#         # Build terms list
#         terms = []
#         for wd in word_data:
#             # Normalize POS for output
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
            
#             # Add selected match info if available
#             if wd.get('selected_match'):
#                 sm = wd['selected_match']
#                 term['category'] = sm.get('category', '')
#                 term['description'] = sm.get('description', '')
#                 term['display'] = sm.get('display', wd['word'])
#                 term['rank'] = sm.get('rank', 0)
#                 term['entity_type'] = sm.get('entity_type', 'unigram')
            
#             # KEY FIX: If no selected_match but has all_matches, use the best one
#             # This ensures terms in n-grams still have their category/rank
#             elif wd.get('all_matches') and len(wd['all_matches']) > 0:
#                 # Use highest rank match (all_matches is already sorted by rank desc)
#                 best_match = wd['all_matches'][0]
#                 term['category'] = best_match.get('category', '')
#                 term['description'] = best_match.get('description', '')
#                 term['display'] = best_match.get('display', wd['word'])
#                 term['rank'] = best_match.get('rank', 0)
#                 term['entity_type'] = best_match.get('entity_type', 'unigram')
            
#             # Fallback for stopwords or truly unknown words
#             else:
#                 term['category'] = wd.get('category', '')
#                 term['description'] = ''
#                 term['display'] = wd['word']
#                 term['rank'] = 0
#                 term['entity_type'] = 'unigram'
            
#             # Add correction info
#             # if wd['status'] == 'corrected':
#             #     term['corrected'] = wd.get('corrected')
#             #     term['corrected_display'] = wd.get('corrected_display')
#             #     term['distance'] = wd.get('distance')
#             # Add correction info

#             if wd['status'] in ('corrected', 'pos_corrected'):
#                 term['corrected'] = wd.get('corrected')
#                 term['corrected_display'] = wd.get('corrected_display')
#                 term['distance'] = wd.get('distance')
            
#             # Add match count for debugging
#             term['match_count'] = len(wd.get('all_matches', []))
            
#             terms.append(term)
        
#         # Calculate timing
#         elapsed = (time.perf_counter() - start_time) * 1000
        
#         # Build output
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
        
#         return output
    
#     # =========================================================================
#     # Helper Methods
#     # =========================================================================
    
#     # def _get_working_words(self, word_data: List[Dict[str, Any]]) -> List[str]:
#     #     """Get the working word list (using corrected words where available)."""
#     #     words = []
#     #     for wd in word_data:
#     #         if wd['status'] == 'corrected':
#     #             words.append(wd['corrected'])
#     #         else:
#     #             words.append(wd['word'])
#     #     return words
    
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
        
#         # Get matches
#         matches = self.cache.get_term_matches(word_lower)
        
#         if matches:
#             # Single word - just pick highest rank (no POS context)
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
        
#         # Unknown - try correction
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
#     """
#     Main entry point - process a query through word discovery.
    
#     Args:
#         query: The search query string
#         verbose: Whether to print debug output
    
#     Returns:
#         Dict with terms, ngrams, corrections, and metadata for Typesense
#     """
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
#         "where is african located",
#         "where is american located",
#         "african food near me",
#         "when was slavery abolished",
#         "where is quickly located"
  
# ]
    
#     # Use command line argument if provided
#     if len(sys.argv) > 1:
#         query = ' '.join(sys.argv[1:])
#         test_queries = [query]
    
#     print("\n" + "#" * 70)
#     print("# WORD DISCOVERY V2 - TEST")
#     print("#" * 70)
    
#     for query in test_queries:
#         output = process_query(query, verbose=True)
#         print_output(output)
#         print("\n" + "=" * 70 + "\n")


# if __name__ == "__main__":
#     main()









"""
word_discovery_v2.py
====================
Optimized word discovery with POS-aware selection and n-gram detection.

NOW USES RAM CACHE (vocabulary_cache.py) for Steps 1-4, 6-7.
Only Step 5 (fuzzy spelling correction) hits Redis.

STRATEGY:
    Step 1: Collect ALL data in one pass (RAM)
            - All word matches (don't pick winner yet)
            - All bigrams, trigrams, quadgrams
    
    Step 2: Mark n-gram positions (longest first)
            - Quadgrams → Trigrams → Bigrams
            - Consumed positions are marked
    
    Step 3: Determine POS for remaining words
            - Use grammar rules based on neighbors
            - Unknown neighbors → default to noun
    
    Step 4: Select best match (POS + Rank)
            - Filter by predicted POS
            - Then sort by rank
            - Word Discovery is the brain - Typesense receives the decision
    
    Step 5: Handle unknowns (spelling correction)
            - Use predicted POS to filter corrections
            - Sort by distance, then rank
            - Preserve original position
            - THIS IS THE ONLY STEP THAT USES REDIS
    
    Step 6: Re-check n-grams after correction (RAM)
            - Corrected words may form new n-grams
    
    Step 7: Build final output for Typesense

PERFORMANCE:
    - Steps 1-4, 6-7: RAM only (~0.01ms per lookup)
    - Step 5: Redis only for unknown words (~50ms per correction)
    - Typical query (no typos): ~1-5ms
    - Query with 1-2 typos: ~50-150ms
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


# =============================================================================
# REDIS CONNECTION (Only for spelling correction - Step 5)
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
# RAM CACHE - Uses vocabulary_cache.py singleton
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
            
            # Normalize to the format WordDiscovery expects
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
            
            # Sort by rank descending
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

        # Try underscore version first (most common in Redis)
        pattern = f"term:{phrase_underscore}:*"
        try:
            keys = client.keys(pattern)
        except Exception:
            keys = []

        # If not found, try space version
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
    # SPECIAL CASES - Start of query
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


def is_pos_compatible(candidate_pos: str, predicted_pos: str) -> bool:
    """Check if candidate POS is compatible with predicted POS."""
    candidate_pos = normalize_pos_string(candidate_pos)
    predicted_pos = normalize_pos_string(predicted_pos)
    
    if candidate_pos == predicted_pos:
        return True
    
    compatible_set = POS_COMPATIBILITY.get(predicted_pos, {predicted_pos})
    return candidate_pos in compatible_set


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


def get_fuzzy_suggestions(word: str, limit: int = 10, max_distance: int = 2) -> List[Dict[str, Any]]:
    """
    Get spelling suggestions from Redis using fuzzy search.
    This is the ONLY Redis call in the pipeline (for unknown words).
    """
    client = RedisClient.get_client()
    if not client:
        return []
    
    word_lower = word.lower().strip()
    if len(word_lower) < 2:
        return []
    
    try:
        suggestions = []
        
        for fuzzy_level in ['%', '%%']:
            if fuzzy_level == '%%' and suggestions:
                break
            
            query = f"{fuzzy_level}{word_lower}{fuzzy_level}"
            
            try:
                result = client.execute_command(
                    'FT.SEARCH', 'terms_idx', query,
                    'SORTBY', 'rank', 'DESC',
                    'LIMIT', '0', str(limit * 3)
                )
                
                if result and len(result) > 1:
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
                            distance = damerau_levenshtein_distance(word_lower, term.lower())
                            
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
                print(f"Fuzzy search error for '{word}': {e}")
        
        suggestions.sort(key=lambda x: (x['distance'], -x['rank']))
        
        seen = set()
        unique = []
        for s in suggestions:
            if s['term'].lower() not in seen:
                seen.add(s['term'].lower())
                unique.append(s)
        
        return unique[:limit]
        
    except Exception as e:
        print(f"Error getting suggestions for '{word}': {e}")
        return []


# =============================================================================
# MAIN WORD DISCOVERY CLASS
# =============================================================================

class WordDiscovery:
    """
    Word Discovery Engine - The brain behind Typesense queries.
    
    Uses RAM cache (vocabulary_cache.py) for all lookups.
    Only uses Redis for fuzzy spelling correction (Step 5).
    """
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.cache = vocab_cache_wrapper
    
    def process(self, query: str) -> Dict[str, Any]:
        """Main entry point - process a query through all steps."""
        start_time = time.perf_counter()
        
        if self.verbose:
            print("\n" + "=" * 70)
            print(f"🔍 WORD DISCOVERY: '{query}'")
            print(f"   RAM cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
            print("=" * 70)
        
        if not query or not query.strip():
            return self._empty_result(query)
        
        words = [w.strip('?!.,;:"\'"()[]{}') for w in query.lower().split()]
        words = [w for w in words if w]
        
        if len(words) == 1:
            return self._process_single_word(words[0], start_time)
        
        # Step 1: Collect ALL data (RAM)
        word_data = self._step1_collect_all_data(words)
        
        # Step 2: Mark n-gram positions (RAM)
        ngrams, consumed_positions = self._step2_detect_ngrams(words, word_data)
        
        # Step 3: Determine POS for remaining words
        self._step3_determine_pos(word_data, consumed_positions)

        # Step 3.5: Refine POS from suffixes
        self._step3_5_refine_pos_from_suffix(word_data)
        
        # Step 4: Select best match (POS + Rank) (RAM)
        self._step4_select_best_match(word_data, consumed_positions)
        
        # Step 5: Handle unknowns (REDIS - only step that uses Redis)
        corrections = self._step5_correct_unknowns(word_data)
        
        # Step 6: Re-check n-grams after correction (RAM)
        corrected_words = self._get_working_words(word_data)
        new_ngrams, new_consumed = self._step6_recheck_ngrams(
            corrected_words, word_data, consumed_positions
        )
        ngrams.extend(new_ngrams)
        consumed_positions.update(new_consumed)
        
        # Step 7: Build final output
        output = self._step7_build_output(
            query, word_data, ngrams, consumed_positions, corrections, start_time
        )
        
        return output
    
    # =========================================================================
    # STEP 1: Collect ALL Data (RAM)
    # =========================================================================
    
    def _step1_collect_all_data(self, words: List[str]) -> List[Dict[str, Any]]:
        """Collect all matches for each word from RAM cache."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("📖 STEP 1: Collect ALL Data (RAM)")
            print("-" * 70)
        
        word_data = []
        
        for i, word in enumerate(words):
            position = i + 1
            word_lower = word.lower().strip()
            
            # Check stopwords first
            if word_lower in STOPWORDS:
                word_data.append({
                    'position': position,
                    'word': word_lower,
                    'status': 'valid',
                    'is_stopword': True,
                    'pos': STOPWORDS[word_lower],
                    'predicted_pos': None,
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
                })
                
                if self.verbose:
                    print(f"  [{position}] '{word_lower}' → STOPWORD ({STOPWORDS[word_lower]})")
                continue
            
            # Get ALL matches from RAM cache
            matches = self.cache.get_term_matches(word_lower)
            
            if matches:
                word_data.append({
                    'position': position,
                    'word': word_lower,
                    'status': 'found',
                    'is_stopword': False,
                    'pos': None,
                    'predicted_pos': None,
                    'all_matches': matches,
                    'selected_match': None,
                })
                
                if self.verbose:
                    print(f"  [{position}] '{word_lower}' → FOUND ({len(matches)} matches)")
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
                    'all_matches': [],
                    'selected_match': None,
                })
                
                if self.verbose:
                    print(f"  [{position}] '{word_lower}' → UNKNOWN")
        
        return word_data
    
    # =========================================================================
    # STEP 2: Detect N-grams (RAM)
    # =========================================================================
    
    def _step2_detect_ngrams(
        self, 
        words: List[str],
        word_data: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], Set[int]]:
        """Detect all n-grams using RAM cache, prioritizing longest matches."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🔗 STEP 2: Detect N-grams (RAM)")
            print("-" * 70)
        
        ngrams = []
        consumed = set()
        
        # Check quadgrams first
        if len(words) >= 4:
            for i in range(len(words) - 3):
                if any(p in consumed for p in [i, i+1, i+2, i+3]):
                    continue
                
                quad = words[i:i+4]
                result = self.cache.get_ngram(quad)
                
                if result:
                    positions = [i+1, i+2, i+3, i+4]
                    ngrams.append({
                        'type': 'quadgram',
                        'positions': positions,
                        'words': quad,
                        'phrase': result['term'],
                        'display': result['display'],
                        'category': result['category'],
                        'description': result['description'],
                        'pos': normalize_pos_string(result['pos']),
                        'rank': result['rank'],
                    })
                    consumed.update([i, i+1, i+2, i+3])
                    
                    if self.verbose:
                        print(f"  ✅ QUADGRAM: '{' '.join(quad)}'")
                        print(f"       Category: {result['category']}")
        
        # Check trigrams
        if len(words) >= 3:
            for i in range(len(words) - 2):
                if any(p in consumed for p in [i, i+1, i+2]):
                    continue
                
                tri = words[i:i+3]
                result = self.cache.get_ngram(tri)
                
                if result:
                    positions = [i+1, i+2, i+3]
                    ngrams.append({
                        'type': 'trigram',
                        'positions': positions,
                        'words': tri,
                        'phrase': result['term'],
                        'display': result['display'],
                        'category': result['category'],
                        'description': result['description'],
                        'pos': normalize_pos_string(result['pos']),
                        'rank': result['rank'],
                    })
                    consumed.update([i, i+1, i+2])
                    
                    if self.verbose:
                        print(f"  ✅ TRIGRAM: '{' '.join(tri)}'")
                        print(f"       Category: {result['category']}")
        
        # Check bigrams
        if len(words) >= 2:
            for i in range(len(words) - 1):
                if any(p in consumed for p in [i, i+1]):
                    continue
                
                bi = words[i:i+2]
                result = self.cache.get_ngram(bi)
                
                if result:
                    positions = [i+1, i+2]
                    ngrams.append({
                        'type': 'bigram',
                        'positions': positions,
                        'words': bi,
                        'phrase': result['term'],
                        'display': result['display'],
                        'category': result['category'],
                        'description': result['description'],
                        'pos': normalize_pos_string(result['pos']),
                        'rank': result['rank'],
                    })
                    consumed.update([i, i+1])
                    
                    if self.verbose:
                        print(f"  ✅ BIGRAM: '{' '.join(bi)}'")
                        print(f"       Category: {result['category']}")
        
        if not ngrams and self.verbose:
            print("  (no n-grams found)")
        
        return ngrams, consumed

    # =========================================================================
    # STEP 3: Determine POS for ALL Words
    # =========================================================================
    
    def _step3_determine_pos(
        self, 
        word_data: List[Dict[str, Any]], 
        consumed_positions: Set[int]
    ) -> None:
        """Determine POS for ALL words using grammar rules."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🧠 STEP 3: Determine POS (Grammar Rules)")
            print("-" * 70)
        
        for i, wd in enumerate(word_data):
            position = wd['position']
            
            pos_index = position - 1
            wd['part_of_ngram'] = pos_index in consumed_positions
            
            if wd['is_stopword']:
                wd['predicted_pos'] = wd['pos']
                wd['predicted_pos_list'] = [(wd['pos'], 1.0)]
                if self.verbose:
                    print(f"  [{position}] '{wd['word']}' → Stopword ({wd['pos']})")
                continue
            
            # Get left neighbor POS
            left_pos = None
            if i > 0:
                left_wd = word_data[i - 1]
                if left_wd['is_stopword']:
                    left_pos = left_wd['pos']
                elif left_wd.get('predicted_pos'):
                    pred = left_wd['predicted_pos']
                    if isinstance(pred, tuple):
                        left_pos = pred[0]
                    else:
                        left_pos = pred
                elif left_wd['all_matches']:
                    raw_pos = left_wd['all_matches'][0]['pos']
                    left_pos = normalize_pos_string(raw_pos)
            else:
                left_pos = 'start'
            
            # Get right neighbor POS
            right_pos = None
            if i < len(word_data) - 1:
                right_wd = word_data[i + 1]
                if right_wd['is_stopword']:
                    right_pos = right_wd['pos']
                elif right_wd['all_matches']:
                    raw_pos = right_wd['all_matches'][0]['pos']
                    right_pos = normalize_pos_string(raw_pos)
            else:
                right_pos = 'end'
            
            # Apply grammar rules
            predicted_pos = None
            
            if left_pos and right_pos:
                predicted_pos = GRAMMAR_RULES.get((left_pos, right_pos))
            
            if not predicted_pos and left_pos:
                predicted_pos = GRAMMAR_RULES.get((left_pos, None))
            
            if not predicted_pos and right_pos:
                predicted_pos = GRAMMAR_RULES.get((None, right_pos))
            
            if not predicted_pos:
                predicted_pos = [('noun', 0.75)]
            
            # Passive voice lookahead
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
                    if self.verbose:
                        print(f"       ⚡ Passive voice detected: [be]..._{wd['word']}_...[-ed] → noun (0.95)")

            # Store both formats
            if isinstance(predicted_pos, list):
                wd['predicted_pos_list'] = predicted_pos
                wd['predicted_pos'] = predicted_pos[0][0] if predicted_pos else 'noun'
            else:
                wd['predicted_pos_list'] = [(predicted_pos, 0.90)]
                wd['predicted_pos'] = predicted_pos
            
            if self.verbose:
                context = f"[{left_pos or '???'}] _{wd['word']}_ [{right_pos or '???'}]"
                ngram_note = " (in n-gram)" if wd['part_of_ngram'] else ""
                print(f"  [{position}] '{wd['word']}' → Predicted: {wd['predicted_pos_list']}{ngram_note}")
                print(f"       Context: {context}")

    # =========================================================================
    # STEP 3.5: Refine POS from Word Suffixes
    # =========================================================================
    
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
    
    def _step3_5_refine_pos_from_suffix(self, word_data: List[Dict[str, Any]]) -> None:
        """Refine POS predictions using word suffixes."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🔤 STEP 3.5: Refine POS from Suffixes")
            print("-" * 70)
        
        for wd in word_data:
            if wd['is_stopword']:
                continue
            
            word = wd.get('corrected', wd['word']).lower()
            
            if len(word) < self.MIN_SUFFIX_WORD_LENGTH:
                if self.verbose:
                    print(f"  [{wd['position']}] '{word}' → Too short, skipping")
                continue
            
            if word in self.SUFFIX_EXCEPTIONS:
                if self.verbose:
                    print(f"  [{wd['position']}] '{word}' → Exception, skipping")
                continue
            
            matched_suffix = None
            suffix_predictions = None
            
            sorted_suffixes = sorted(self.SUFFIX_POS_RULES.keys(), key=len, reverse=True)
            
            for suffix in sorted_suffixes:
                if word.endswith(suffix) and len(word) > len(suffix) + 1:
                    matched_suffix = suffix
                    suffix_predictions = self.SUFFIX_POS_RULES[suffix]
                    break
            
            if not matched_suffix:
                if self.verbose:
                    print(f"  [{wd['position']}] '{word}' → No suffix match")
                continue
            
            current_list = wd.get('predicted_pos_list', [])
            current_primary = wd.get('predicted_pos', 'noun')
            
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
            
            refined_list = sorted(refined.items(), key=lambda x: -x[1])
            
            wd['predicted_pos_list'] = refined_list
            wd['predicted_pos'] = refined_list[0][0] if refined_list else current_primary
            wd['suffix_detected'] = matched_suffix
            
            if self.verbose:
                changed = wd['predicted_pos'] != current_primary
                change_marker = " ⚡ CHANGED" if changed else ""
                print(f"  [{wd['position']}] '{word}' → Suffix: -{matched_suffix}{change_marker}")
                print(f"       Before: {current_list}")
                print(f"       After:  {refined_list}")

    # =========================================================================
    # STEP 4: Select Best Match (POS + Rank) (RAM)
    # =========================================================================
    
    def _step4_select_best_match(
        self, 
        word_data: List[Dict[str, Any]], 
        consumed_positions: Set[int]
    ) -> None:
        """For ALL words with multiple matches, select based on POS + rank."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("🎯 STEP 4: Select Best Match (POS + Rank)")
            print("-" * 70)
        
        for wd in word_data:
            position = wd['position']
            
            if wd['is_stopword']:
                continue
            
            if wd['status'] == 'unknown':
                if self.verbose:
                    print(f"  [{position}] '{wd['word']}' → Unknown (defer to Step 5)")
                continue
            
            matches = wd['all_matches']
            if not matches:
                continue
            
            predicted_pos = wd['predicted_pos']
            
            compatible_matches = []
            for m in matches:
                match_pos = normalize_pos_string(m['pos'])
                if is_pos_compatible(match_pos, predicted_pos):
                    compatible_matches.append(m)
            
            pos_index = position - 1
            is_in_ngram = pos_index in consumed_positions
            ngram_note = " (in n-gram)" if is_in_ngram else ""
            
            if compatible_matches:
                compatible_matches.sort(key=lambda x: x['rank'], reverse=True)
                best = compatible_matches[0]
                wd['selected_match'] = best
                wd['pos'] = normalize_pos_string(best['pos'])
                wd['status'] = 'valid'
                
                if self.verbose:
                    print(f"  [{position}] '{wd['word']}' → Selected: {best['category']} (pos={normalize_pos_string(best['pos'])}, rank={best['rank']}){ngram_note}")
                    if len(matches) > 1:
                        rejected = [f"{m['category']}({normalize_pos_string(m['pos'])})" for m in matches if m != best]
                        print(f"       Rejected: {rejected}")
            else:
                best = matches[0]
                wd['selected_match'] = best
                wd['pos'] = normalize_pos_string(best['pos'])
                wd['status'] = 'valid'
                
                if self.verbose:
                    print(f"  [{position}] '{wd['word']}' → Fallback: {best['category']} (no POS match, using rank){ngram_note}")
 
    # =========================================================================
    # STEP 5: Correct Unknown Words (REDIS - only Redis step)
    # =========================================================================
    
    def _step5_correct_unknowns(self, word_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Correct unknown/misspelled words using Redis fuzzy search."""
        
        unknowns = []
        pos_mismatches = []
        
        for wd in word_data:
            if wd['status'] == 'unknown':
                unknowns.append(wd)
            # elif (
            #     wd['status'] == 'valid'
            #     and not wd['is_stopword']
            #     and wd.get('predicted_pos')
            #     and wd.get('pos')
            #     and wd['predicted_pos'] != wd['pos']
            # ):
            #     predicted_list = wd.get('predicted_pos_list', [])
            #     top_confidence = predicted_list[0][1] if predicted_list else 0
            #     if top_confidence >= 0.90:
            #         pos_mismatches.append(wd)
            elif (
                wd['status'] == 'valid'
                and not wd['is_stopword']
                and wd.get('predicted_pos')
                and wd.get('pos')
                and wd['predicted_pos'] != wd['pos']
            ):
                predicted_list = wd.get('predicted_pos_list', [])
                top_confidence = predicted_list[0][1] if predicted_list else 0
                word_rank = wd.get('selected_match', {}).get('rank', 0) or 0
                # Only POS-correct low-rank words (likely typos, not real words)
                # High-rank words are common in our corpus — trust them
                if top_confidence >= 0.90 and word_rank < 200:
                    pos_mismatches.append(wd)
        
        if not unknowns and not pos_mismatches:
            if self.verbose:
                print("\n" + "-" * 70)
                print("🔧 STEP 5: Correct Unknowns")
                print("-" * 70)
                print("  (no unknown words)")
            return []
        
        if self.verbose:
            print("\n" + "-" * 70)
            print("🔧 STEP 5: Correct Unknowns (Redis Fuzzy)")
            print("-" * 70)
        
        corrections = []
        
        # Process unknowns
        for wd in unknowns:
            word = wd['word']
            position = wd['position']
            predicted_pos = wd['predicted_pos'] or 'noun'
            
            suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
            if not suggestions:
                if self.verbose:
                    print(f"  [{position}] '{word}' → No suggestions found")
                continue
            
            compatible = [s for s in suggestions if is_pos_compatible(s['pos'], predicted_pos)]
            
            if compatible:
                compatible.sort(key=lambda x: (x['distance'], -x['rank']))
                best = compatible[0]
            else:
                best = suggestions[0]
            
            wd['status'] = 'corrected'
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
            })
            
            if self.verbose:
                print(f"  [{position}] '{word}' → '{best['term']}' (distance={best['distance']}, pos={best['pos']})")
                if len(suggestions) > 1:
                    others = [s['term'] for s in suggestions[1:4] if s['term'] != best['term']]
                    if others:
                        print(f"       Other options: {others}")
        
        # Process POS mismatches
        for wd in pos_mismatches:
            word = wd['word']
            position = wd['position']
            predicted_pos = wd['predicted_pos']
            original_pos = wd['pos']
            
            suggestions = get_fuzzy_suggestions(word, limit=10, max_distance=2)
            
            if not suggestions:
                if self.verbose:
                    print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no suggestions found")
                continue
            
            # compatible = [
            #     s for s in suggestions
            #     if is_pos_compatible(s['pos'], predicted_pos)
            #     and s['term'] != word
            #     and s['distance'] <= 2
            # ]
            compatible = [
                s for s in suggestions
                if is_pos_compatible(s['pos'], predicted_pos)
                and s['term'] != word
                and s['distance'] <= 1
                and s['rank'] > word_rank * 3
            ]
            
            if not compatible:
                if self.verbose:
                    print(f"  [{position}] '{word}' → POS mismatch ({original_pos}→{predicted_pos}), no compatible alternative found")
                continue
            
            compatible.sort(key=lambda x: (x['distance'], -x['rank']))
            best = compatible[0]
            
            wd['status'] = 'pos_corrected'
            wd['corrected'] = best['term']
            wd['corrected_display'] = best['display']
            wd['original_pos'] = original_pos
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
            
            if self.verbose:
                print(f"  [{position}] '{word}' → '{best['term']}' (POS: {original_pos}→{best['pos']}, distance={best['distance']})")
                if len(compatible) > 1:
                    others = [s['term'] for s in compatible[1:4] if s['term'] != best['term']]
                    if others:
                        print(f"       Other options: {others}")
        
        return corrections

    # =========================================================================
    # STEP 6: Re-check N-grams After Correction (RAM)
    # =========================================================================
    
    def _step6_recheck_ngrams(
        self,
        corrected_words: List[str],
        word_data: List[Dict[str, Any]],
        already_consumed: Set[int]
    ) -> Tuple[List[Dict[str, Any]], Set[int]]:
        """Re-check for n-grams using corrected words (RAM cache)."""
        has_corrections = any(wd['status'] in ('corrected', 'pos_corrected') for wd in word_data)
        
        if not has_corrections:
            if self.verbose:
                print("\n" + "-" * 70)
                print("🔄 STEP 6: Re-check N-grams After Correction")
                print("-" * 70)
                print("  (no corrections, skipping)")
            return [], set()
        
        if self.verbose:
            print("\n" + "-" * 70)
            print("🔄 STEP 6: Re-check N-grams After Correction (RAM)")
            print("-" * 70)
        
        new_ngrams = []
        new_consumed = set()
        words = corrected_words
        
        # Check trigrams
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
                        'rank': result['rank'],
                        'from_correction': True,
                    })
                    new_consumed.update([i, i+1, i+2])
                    
                    if self.verbose:
                        print(f"  ✅ NEW TRIGRAM: '{' '.join(tri)}' (after correction)")
        
        # Check bigrams
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
                        'rank': result['rank'],
                        'from_correction': True,
                    })
                    new_consumed.update([i, i+1])
                    
                    if self.verbose:
                        print(f"  ✅ NEW BIGRAM: '{' '.join(bi)}' (after correction)")
        
        if not new_ngrams and self.verbose:
            print("  (no new n-grams found)")
        
        return new_ngrams, new_consumed
    
    # =========================================================================
    # STEP 7: Build Final Output
    # =========================================================================
    
    def _step7_build_output(
        self,
        query: str,
        word_data: List[Dict[str, Any]],
        ngrams: List[Dict[str, Any]],
        consumed_positions: Set[int],
        corrections: List[Dict[str, Any]],
        start_time: float
    ) -> Dict[str, Any]:
        """Build the final output structure for Typesense."""
        corrected_words = self._get_working_words(word_data)
        corrected_query = ' '.join(corrected_words)
        
        for wd in word_data:
            pos_index = wd['position'] - 1
            wd['part_of_ngram'] = pos_index in consumed_positions
        
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
                term['category'] = wd.get('category', '')
                term['description'] = ''
                term['display'] = wd['word']
                term['rank'] = 0
                term['entity_type'] = 'unigram'
            
            if wd['status'] in ('corrected', 'pos_corrected'):
                term['corrected'] = wd.get('corrected')
                term['corrected_display'] = wd.get('corrected_display')
                term['distance'] = wd.get('distance')
            
            term['match_count'] = len(wd.get('all_matches', []))
            
            terms.append(term)
        
        elapsed = (time.perf_counter() - start_time) * 1000
        
        output = {
            'query': query,
            'corrected_query': corrected_query,
            'processing_time_ms': round(elapsed, 2),
            'terms': terms,
            'ngrams': ngrams,
            'corrections': corrections,
            'stats': {
                'total_words': len(word_data),
                'valid_words': sum(1 for wd in word_data if wd['status'] == 'valid'),
                'corrected_words': sum(1 for wd in word_data if wd['status'] == 'corrected'),
                'unknown_words': sum(1 for wd in word_data if wd['status'] == 'unknown'),
                'stopwords': sum(1 for wd in word_data if wd['is_stopword']),
                'ngram_count': len(ngrams),
            }
        }
        
        if self.verbose:
            print("\n" + "=" * 70)
            print("📊 FINAL OUTPUT")
            print("=" * 70)
            print(f"  Query: '{query}'")
            print(f"  Corrected: '{corrected_query}'")
            print(f"  Time: {elapsed:.2f}ms")
            print(f"  Stats: {output['stats']}")
            print(f"  Source: {'RAM cache' if RAM_CACHE_AVAILABLE else 'Redis fallback'}")
        
        return output
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _get_working_words(self, word_data: List[Dict[str, Any]]) -> List[str]:
        """Get the working word list (using corrected words where available)."""
        words = []
        for wd in word_data:
            if wd['status'] in ('corrected', 'pos_corrected'):
                words.append(wd['corrected'])
            else:
                words.append(wd['word'])
        return words
    
    def _empty_result(self, query: str) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            'query': query or '',
            'corrected_query': '',
            'processing_time_ms': 0,
            'terms': [],
            'ngrams': [],
            'corrections': [],
            'stats': {
                'total_words': 0,
                'valid_words': 0,
                'corrected_words': 0,
                'unknown_words': 0,
                'stopwords': 0,
                'ngram_count': 0,
            }
        }
    
    def _process_single_word(self, word: str, start_time: float) -> Dict[str, Any]:
        """Handle single word queries."""
        if self.verbose:
            print("\n" + "-" * 70)
            print("📖 Single Word Query")
            print("-" * 70)
        
        word_lower = word.lower().strip()
        
        # Check stopword
        if word_lower in STOPWORDS:
            elapsed = (time.perf_counter() - start_time) * 1000
            return {
                'query': word,
                'corrected_query': word_lower,
                'processing_time_ms': round(elapsed, 2),
                'terms': [{
                    'position': 1,
                    'word': word_lower,
                    'status': 'valid',
                    'is_stopword': True,
                    'part_of_ngram': False,
                    'pos': STOPWORDS[word_lower],
                    'category': 'stopword',
                    'description': '',
                    'display': word_lower,
                    'rank': 0,
                    'match_count': 0,
                }],
                'ngrams': [],
                'corrections': [],
                'stats': {
                    'total_words': 1,
                    'valid_words': 1,
                    'corrected_words': 0,
                    'unknown_words': 0,
                    'stopwords': 1,
                    'ngram_count': 0,
                }
            }
        
        # Get matches from RAM cache
        matches = self.cache.get_term_matches(word_lower)
        
        if matches:
            best = matches[0]
            elapsed = (time.perf_counter() - start_time) * 1000
            
            if self.verbose:
                print(f"  '{word_lower}' → Found ({len(matches)} matches)")
                print(f"  Selected: {best['category']} (rank={best['rank']})")
            
            return {
                'query': word,
                'corrected_query': word_lower,
                'processing_time_ms': round(elapsed, 2),
                'terms': [{
                    'position': 1,
                    'word': word_lower,
                    'status': 'valid',
                    'is_stopword': False,
                    'part_of_ngram': False,
                    'pos': best['pos'],
                    'category': best['category'],
                    'description': best['description'],
                    'display': best['display'],
                    'rank': best['rank'],
                    'entity_type': best['entity_type'],
                    'match_count': len(matches),
                }],
                'ngrams': [],
                'corrections': [],
                'stats': {
                    'total_words': 1,
                    'valid_words': 1,
                    'corrected_words': 0,
                    'unknown_words': 0,
                    'stopwords': 0,
                    'ngram_count': 0,
                }
            }
        
        # Unknown - try Redis fuzzy correction
        suggestions = get_fuzzy_suggestions(word_lower, limit=5, max_distance=2)
        
        if suggestions:
            best = suggestions[0]
            elapsed = (time.perf_counter() - start_time) * 1000
            
            if self.verbose:
                print(f"  '{word_lower}' → Corrected to '{best['term']}' (distance={best['distance']})")
            
            return {
                'query': word,
                'corrected_query': best['term'],
                'processing_time_ms': round(elapsed, 2),
                'terms': [{
                    'position': 1,
                    'word': word_lower,
                    'status': 'corrected',
                    'is_stopword': False,
                    'part_of_ngram': False,
                    'pos': best['pos'],
                    'predicted_pos': 'noun',
                    'category': best['category'],
                    'description': best['description'],
                    'display': best['display'],
                    'corrected': best['term'],
                    'corrected_display': best['display'],
                    'distance': best['distance'],
                    'rank': best['rank'],
                    'entity_type': best['entity_type'],
                    'match_count': 0,
                }],
                'ngrams': [],
                'corrections': [{
                    'position': 1,
                    'original': word_lower,
                    'corrected': best['term'],
                    'distance': best['distance'],
                    'pos': best['pos'],
                    'category': best['category'],
                }],
                'stats': {
                    'total_words': 1,
                    'valid_words': 0,
                    'corrected_words': 1,
                    'unknown_words': 0,
                    'stopwords': 0,
                    'ngram_count': 0,
                }
            }
        
        # Truly unknown
        elapsed = (time.perf_counter() - start_time) * 1000
        
        if self.verbose:
            print(f"  '{word_lower}' → Unknown (no suggestions)")
        
        return {
            'query': word,
            'corrected_query': word_lower,
            'processing_time_ms': round(elapsed, 2),
            'terms': [{
                'position': 1,
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
            }],
            'ngrams': [],
            'corrections': [],
            'stats': {
                'total_words': 1,
                'valid_words': 0,
                'corrected_words': 0,
                'unknown_words': 1,
                'stopwords': 0,
                'ngram_count': 0,
            }
        }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def process_query(query: str, verbose: bool = False) -> Dict[str, Any]:
    """Main entry point - process a query through word discovery."""
    wd = WordDiscovery(verbose=verbose)
    return wd.process(query)


def print_output(output: Dict[str, Any]) -> None:
    """Print the output in a readable JSON format."""
    print("\n" + "=" * 70)
    print("📄 COMPLETE OUTPUT (JSON)")
    print("=" * 70)
    print(json.dumps(output, indent=2))


# =============================================================================
# MAIN - TEST SCRIPT
# =============================================================================

def main():
    """Run test queries."""
    import sys
    
    test_queries = [
        "where is africa located",
        "where is african located",
        "african food near me",
        "when was slavery abolished",
        "where is quickly located"
    ]
    
    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])
        test_queries = [query]
    
    print("\n" + "#" * 70)
    print("# WORD DISCOVERY V2 - TEST")
    print(f"# RAM Cache: {'✅ loaded' if RAM_CACHE_AVAILABLE and vocab_cache and vocab_cache.loaded else '❌ not available'}")
    print("#" * 70)
    
    for query in test_queries:
        output = process_query(query, verbose=True)
        print_output(output)
        print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    main()