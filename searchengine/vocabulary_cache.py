# """
# vocabulary_cache.py
# In-memory cache for fast term lookups.

# Loads vocabulary data from:
# 1. API endpoint (POST from Colab)
# 2. Local JSON file (backup/startup)

# PERFORMANCE:
#     - Redis lookup: ~300ms per term
#     - Cache lookup: ~0.001ms per term
#     - Speedup: ~200,000x
# """

# import json
# import logging
# import time
# from pathlib import Path
# from typing import Dict, Set, Optional, Any
# from threading import Lock
# from dataclasses import dataclass, field

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Categories that indicate a location (lowercase for comparison)
# LOCATION_CATEGORIES = frozenset({
#     'us_city', 'us city', 'city',
#     'us_state', 'us state', 'state',
#     'location', 'region', 'country', 'neighborhood',
# })

# # Cache file path (backup for startup)
# CACHE_DIR = Path(__file__).parent / 'cache' / 'data'
# CACHE_FILE = CACHE_DIR / 'vocabulary_data.json'

# # Stopwords - common words that still need POS tagging for grammar context
# DEFAULT_STOPWORDS = frozenset({
#     'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'and',
#     'or', 'but', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
#     'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
#     'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
#     'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
#     'what', 'which', 'who', 'whom', 'where', 'when', 'why', 'how',
#     'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
#     'some', 'such', 'no', 'not', 'only', 'own', 'same', 'so', 'than',
#     'too', 'very', 'just', 'also', 'now', 'here', 'there', 'then',
#     'from', 'by', 'into', 'through', 'during', 'before', 'after',
#     'me', 'my', 'your', 'his', 'her', 'its', 'our', 'their',
# })

# # POS mapping for stopwords (used when not found in main vocabulary)
# STOPWORD_POS = {
#     # Determiners / Articles
#     'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
#     'this': 'determiner', 'that': 'determiner', 
#     'these': 'determiner', 'those': 'determiner',
#     'some': 'determiner', 'any': 'determiner',
#     'each': 'determiner', 'every': 'determiner',
#     'all': 'determiner', 'both': 'determiner',
#     'no': 'determiner', 'few': 'determiner',
#     'more': 'determiner', 'most': 'determiner',
#     'other': 'determiner', 'such': 'determiner',
    
#     # Prepositions
#     'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#     'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#     'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
#     'into': 'preposition', 'through': 'preposition',
#     'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
#     'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
#     'above': 'preposition', 'below': 'preposition',
    
#     # Conjunctions
#     'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#     'so': 'conjunction', 'than': 'conjunction',
    
#     # Pronouns
#     'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#     'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#     'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
#     'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
    
#     # Possessive determiners
#     'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
#     'her': 'determiner', 'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
#     'own': 'determiner',
    
#     # Be verbs
#     'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#     'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
    
#     # Auxiliary / Modal verbs
#     'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#     'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#     'will': 'modal', 'would': 'modal', 'could': 'modal',
#     'should': 'modal', 'may': 'modal', 'might': 'modal',
#     'must': 'modal', 'can': 'modal',
    
#     # Adverbs
#     'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
#     'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
#     'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
#     'only': 'adverb', 'not': 'adverb', 'same': 'adverb',
    
#     # Quantifiers
#     'once': 'adverb',
# }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def is_location_category(category: str) -> bool:
#     """Check if a category indicates a location."""
#     if not category:
#         return False
#     cat_lower = category.lower().replace(' ', '_')
#     return (
#         cat_lower in LOCATION_CATEGORIES or
#         'city' in cat_lower or
#         'state' in cat_lower
#     )


# def parse_pos(pos_value: Any) -> str:
#     """Parse POS from various formats like \"['noun']\" or ['noun'] or 'noun'."""
#     if not pos_value:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 return parsed[0] if parsed else 'unknown'
#             except (json.JSONDecodeError, IndexError):
#                 return pos_value.strip("[]'\"")
#         return pos_value
    
#     # Handle actual list
#     if isinstance(pos_value, list):
#         return pos_value[0] if pos_value else 'unknown'
    
#     return str(pos_value)


# # =============================================================================
# # VOCABULARY CACHE CLASS
# # =============================================================================

# @dataclass
# class VocabularyCache:
#     """In-memory cache for O(1) vocabulary lookups."""
    
#     # Core data structures
#     cities: Set[str] = field(default_factory=set)
#     states: Set[str] = field(default_factory=set)
#     locations: Set[str] = field(default_factory=set)
#     bigrams: Dict[str, Dict] = field(default_factory=dict)
#     trigrams: Dict[str, Dict] = field(default_factory=dict)
#     terms: Dict[str, Dict] = field(default_factory=dict)
#     stopwords: Set[str] = field(default_factory=lambda: set(DEFAULT_STOPWORDS))
    
#     # Metadata
#     loaded: bool = False
#     load_time: float = 0.0
#     term_count: int = 0
#     load_source: str = ""
#     last_updated: str = ""
    
#     # Thread safety
#     _lock: Lock = field(default_factory=Lock)
    
#     # -------------------------------------------------------------------------
#     # FAST LOOKUP METHODS (O(1))
#     # -------------------------------------------------------------------------
    
#     def is_city(self, term: str) -> bool:
#         """Check if term is a city. O(1)"""
#         return term.lower() in self.cities
    
#     def is_state(self, term: str) -> bool:
#         """Check if term is a state. O(1)"""
#         return term.lower() in self.states
    
#     def is_location(self, term: str) -> bool:
#         """Check if term is any location type. O(1)"""
#         return term.lower() in self.locations
    
#     def is_stopword(self, term: str) -> bool:
#         """Check if term is a stopword. O(1)"""
#         return term.lower() in self.stopwords
    
#     def is_bigram(self, word1: str, word2: str) -> bool:
#         """Check if two words form a bigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return key in self.bigrams
    
#     def is_trigram(self, word1: str, word2: str, word3: str) -> bool:
#         """Check if three words form a trigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return key in self.trigrams
    
#     def has_term(self, term: str) -> bool:
#         """Check if term exists. O(1)"""
#         return term.lower() in self.terms
    
#     def get_term(self, term: str) -> Optional[Dict]:
#         """Get term metadata. O(1)"""
#         return self.terms.get(term.lower())
    
#     def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
#         """Get bigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return self.bigrams.get(key)
    
#     def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
#         """Get trigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return self.trigrams.get(key)
    
#     # -------------------------------------------------------------------------
#     # QUERY CLASSIFICATION (Single Pass)
#     # -------------------------------------------------------------------------
    
#     def get_stopword_pos(self, word: str) -> str:
#         """Get POS for a stopword."""
#         return STOPWORD_POS.get(word.lower(), 'unknown')
    
#     def classify_query(self, query: str) -> Dict[str, Any]:
#         """
#         Classify all terms in a query in a single pass.
        
#         Stopwords ARE included with POS information for grammar context.
        
#         Returns:
#             {
#                 "locations": ["georgia", "atlanta"],
#                 "bigrams": ["new york"],
#                 "trigrams": [],
#                 "terms": {"georgia": {...}, "the": {...}, ...},
#                 "unknown": [],
#                 "stopwords": ["the", "in"],  # Tracked but included in terms
#             }
#         """
#         words = query.lower().split()
        
#         result = {
#             'locations': [],
#             'bigrams': [],
#             'trigrams': [],
#             'terms': {},
#             'unknown': [],
#             'stopwords': [],  # Track which words are stopwords (but still include them)
#         }
        
#         used_indices = set()
        
#         # Pass 1: Trigrams
#         for i in range(len(words) - 2):
#             if i in used_indices:
#                 continue
#             trigram_key = f"{words[i]} {words[i+1]} {words[i+2]}"
#             if trigram_key in self.trigrams:
#                 result['trigrams'].append(trigram_key)
#                 metadata = self.trigrams[trigram_key]
#                 result['terms'][trigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(trigram_key)
#                 used_indices.update([i, i+1, i+2])
        
#         # Pass 2: Bigrams
#         for i in range(len(words) - 1):
#             if i in used_indices or i+1 in used_indices:
#                 continue
#             bigram_key = f"{words[i]} {words[i+1]}"
#             if bigram_key in self.bigrams:
#                 result['bigrams'].append(bigram_key)
#                 metadata = self.bigrams[bigram_key]
#                 result['terms'][bigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(bigram_key)
#                 used_indices.update([i, i+1])
        
#         # Pass 3: Individual words (including stopwords)
#         for i, word in enumerate(words):
#             if i in used_indices:
#                 continue
            
#             # Check if it's in our vocabulary first
#             if word in self.terms:
#                 metadata = self.terms[word]
#                 result['terms'][word] = metadata
#                 if word in self.locations:
#                     result['locations'].append(word)
#                 # Also track if it's a stopword
#                 if word in self.stopwords:
#                     result['stopwords'].append(word)
            
#             # If it's a stopword but NOT in vocabulary, still include with POS
#             elif word in self.stopwords:
#                 result['stopwords'].append(word)
#                 result['terms'][word] = {
#                     'term': word,
#                     'category': 'Dictionary Word',
#                     'pos': self.get_stopword_pos(word),
#                     'entity_type': 'stopword',
#                     'is_stopword': True,
#                 }
            
#             # Unknown word
#             else:
#                 result['unknown'].append(word)
        
#         return result
    
#     # -------------------------------------------------------------------------
#     # LOADING METHODS
#     # -------------------------------------------------------------------------
    
#     def load_from_dict(self, data: Dict[str, Dict], source: str = "api") -> bool:
#         """
#         Load vocabulary from a dictionary (from Colab endpoint or JSON file).
        
#         Args:
#             data: Dict with keys like "term:atlanta:us_city" and value dicts
#             source: Where the data came from (for logging)
        
#         Returns:
#             True if successful
#         """
#         with self._lock:
#             start_time = time.perf_counter()
#             logger.info(f"Loading vocabulary cache from {source}...")
            
#             # Clear existing data
#             self.cities.clear()
#             self.states.clear()
#             self.locations.clear()
#             self.bigrams.clear()
#             self.trigrams.clear()
#             self.terms.clear()
            
#             loaded = 0
#             bigram_count = 0
#             trigram_count = 0
#             city_count = 0
#             state_count = 0
            
#             for key, metadata in data.items():
#                 try:
#                     # Parse key format: "term:word:category_slug"
#                     parts = key.split(':')
#                     if len(parts) >= 2:
#                         term = parts[1].lower()
#                     else:
#                         term = metadata.get('term', '').lower()
                    
#                     if not term:
#                         continue
                    
#                     # Get category
#                     category = metadata.get('category', '')
#                     category_lower = category.lower().replace(' ', '_')
                    
#                     # Get entity type
#                     entity_type = metadata.get('entity_type', 'unigram')
                    
#                     # Store based on entity type
#                     if entity_type == 'trigram' or term.count(' ') == 2:
#                         self.trigrams[term] = metadata
#                         trigram_count += 1
#                         if is_location_category(category):
#                             self.locations.add(term)
                    
#                     elif entity_type == 'bigram' or ' ' in term:
#                         self.bigrams[term] = metadata
#                         bigram_count += 1
#                         if is_location_category(category):
#                             self.locations.add(term)
                    
#                     else:
#                         # Unigram (single word)
#                         self.terms[term] = metadata
                        
#                         # Categorize
#                         if category_lower in ('us_city', 'us city', 'city'):
#                             self.cities.add(term)
#                             self.locations.add(term)
#                             city_count += 1
#                         elif category_lower in ('us_state', 'us state', 'state'):
#                             self.states.add(term)
#                             self.locations.add(term)
#                             state_count += 1
#                         elif is_location_category(category):
#                             self.locations.add(term)
                    
#                     loaded += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing key '{key}': {e}")
#                     continue
            
#             # Update metadata
#             self.term_count = loaded
#             self.load_time = time.perf_counter() - start_time
#             self.load_source = source
#             self.last_updated = time.strftime('%Y-%m-%d %H:%M:%S')
#             self.loaded = True
            
#             logger.info(
#                 f"Cache loaded: {loaded:,} terms, "
#                 f"{city_count:,} cities, {state_count:,} states, "
#                 f"{bigram_count:,} bigrams, {trigram_count:,} trigrams "
#                 f"in {self.load_time:.2f}s"
#             )
            
#             # Save to file as backup
#             self._save_to_file(data)
            
#             return True
    
#     def load_from_file(self) -> bool:
#         """Load from local JSON file (for startup)."""
#         if not CACHE_FILE.exists():
#             logger.info(f"No cache file found at {CACHE_FILE}")
#             return False
        
#         try:
#             logger.info(f"Loading vocabulary cache from {CACHE_FILE}...")
#             with open(CACHE_FILE, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#             return self.load_from_dict(data, source="file")
#         except Exception as e:
#             logger.error(f"Failed to load from file: {e}")
#             return False
    
#     def _save_to_file(self, data: Dict) -> bool:
#         """Save data to local file as backup."""
#         try:
#             CACHE_DIR.mkdir(parents=True, exist_ok=True)
#             with open(CACHE_FILE, 'w', encoding='utf-8') as f:
#                 json.dump(data, f, ensure_ascii=False)
#             logger.info(f"Cache saved to {CACHE_FILE}")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save cache file: {e}")
#             return False
    
#     def load(self) -> bool:
#         """Load cache from available source (file or wait for API)."""
#         if self.loaded:
#             return True
#         return self.load_from_file()
    
#     def reload_from_file(self) -> bool:
#         """Force reload from file."""
#         with self._lock:
#             self.loaded = False
#         return self.load_from_file()
    
#     # -------------------------------------------------------------------------
#     # STATUS
#     # -------------------------------------------------------------------------
    
#     def status(self) -> Dict[str, Any]:
#         """Get cache status."""
#         return {
#             'loaded': self.loaded,
#             'term_count': self.term_count,
#             'cities': len(self.cities),
#             'states': len(self.states),
#             'locations': len(self.locations),
#             'bigrams': len(self.bigrams),
#             'trigrams': len(self.trigrams),
#             'load_time': f"{self.load_time:.2f}s",
#             'load_source': self.load_source,
#             'last_updated': self.last_updated,
#         }
    
#     def __repr__(self) -> str:
#         return (
#             f"VocabularyCache(loaded={self.loaded}, "
#             f"terms={self.term_count:,}, "
#             f"cities={len(self.cities):,}, "
#             f"states={len(self.states):,}, "
#             f"bigrams={len(self.bigrams):,})"
#         )


# # =============================================================================
# # SINGLETON INSTANCE
# # =============================================================================

# vocab_cache = VocabularyCache()


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def is_city(term: str) -> bool:
#     return vocab_cache.is_city(term)

# def is_state(term: str) -> bool:
#     return vocab_cache.is_state(term)

# def is_location(term: str) -> bool:
#     return vocab_cache.is_location(term)

# def get_term(term: str) -> Optional[Dict]:
#     return vocab_cache.get_term(term)

# def classify_query(query: str) -> Dict[str, Any]:
#     return vocab_cache.classify_query(query)

# def ensure_loaded() -> bool:
#     """Ensure cache is loaded. Called by apps.py at startup."""
#     if not vocab_cache.loaded:
#         return vocab_cache.load()
#     return True

# def get_cache_status() -> Dict[str, Any]:
#     return vocab_cache.status()

# def reload_cache_from_dict(data: Dict) -> bool:
#     return vocab_cache.load_from_dict(data, source="api")

# def reload_cache_from_file() -> bool:
#     return vocab_cache.reload_from_file()


# # =============================================================================
# # CLI FOR TESTING
# # =============================================================================

# if __name__ == "__main__":
#     import argparse
    
#     parser = argparse.ArgumentParser(description="Vocabulary Cache")
#     parser.add_argument('--status', action='store_true', help='Show status')
#     parser.add_argument('--load', action='store_true', help='Load from file')
#     parser.add_argument('--test', action='store_true', help='Test lookups')
#     parser.add_argument('--query', type=str, help='Classify a query')
    
#     args = parser.parse_args()
    
#     if args.load:
#         vocab_cache.load_from_file()
    
#     if args.status:
#         print(vocab_cache.status())
    
#     if args.test:
#         vocab_cache.load()
#         tests = ["atlanta", "georgia", "new york", "xyz123"]
#         for t in tests:
#             print(f"{t}: location={vocab_cache.is_location(t)}, data={vocab_cache.get_term(t)}")
    
#     if args.query:
#         vocab_cache.load()
#         result = vocab_cache.classify_query(args.query)
#         print(json.dumps(result, indent=2))


# """
# vocabulary_cache.py
# In-memory cache for fast term lookups.

# Loads vocabulary data from:
# 1. API endpoint (POST from Colab)
# 2. Local JSON file (backup/startup)

# Supports:
# - Unigrams (single words)
# - Bigrams (two words)
# - Trigrams (three words)
# - Quadgrams (four words)
# - Any n-gram

# PERFORMANCE:
#     - Redis lookup: ~300ms per term
#     - Cache lookup: ~0.001ms per term
#     - Speedup: ~200,000x
# """

# import json
# import logging
# import time
# from pathlib import Path
# from typing import Dict, Set, Optional, Any
# from threading import Lock
# from dataclasses import dataclass, field
# from datetime import datetime

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Categories that indicate a location (lowercase for comparison)
# LOCATION_CATEGORIES = frozenset({
#     'us_city', 'us city', 'city',
#     'us_state', 'us state', 'state',
#     'location', 'region', 'country', 'neighborhood',
# })

# # Cache file path (backup for startup)
# CACHE_DIR = Path(__file__).parent / 'cache' / 'data'
# CACHE_FILE = CACHE_DIR / 'vocabulary_data.json'

# # Stopwords - common words that still need POS tagging for grammar context
# DEFAULT_STOPWORDS = frozenset({
#     'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'and',
#     'or', 'but', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
#     'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
#     'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
#     'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
#     'what', 'which', 'who', 'whom', 'where', 'when', 'why', 'how',
#     'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
#     'some', 'such', 'no', 'not', 'only', 'own', 'same', 'so', 'than',
#     'too', 'very', 'just', 'also', 'now', 'here', 'there', 'then',
#     'from', 'by', 'into', 'through', 'during', 'before', 'after',
#     'me', 'my', 'your', 'his', 'her', 'its', 'our', 'their',
# })

# # POS mapping for stopwords (used when not found in main vocabulary)
# STOPWORD_POS = {
#     # Determiners / Articles
#     'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
#     'this': 'determiner', 'that': 'determiner', 
#     'these': 'determiner', 'those': 'determiner',
#     'some': 'determiner', 'any': 'determiner',
#     'each': 'determiner', 'every': 'determiner',
#     'all': 'determiner', 'both': 'determiner',
#     'no': 'determiner', 'few': 'determiner',
#     'more': 'determiner', 'most': 'determiner',
#     'other': 'determiner', 'such': 'determiner',
    
#     # Prepositions
#     'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#     'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#     'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
#     'into': 'preposition', 'through': 'preposition',
#     'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
#     'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
#     'above': 'preposition', 'below': 'preposition',
    
#     # Conjunctions
#     'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#     'so': 'conjunction', 'than': 'conjunction',
    
#     # Pronouns
#     'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#     'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#     'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
#     'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
    
#     # Possessive determiners
#     'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
#     'her': 'determiner', 'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
#     'own': 'determiner',
    
#     # Be verbs
#     'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#     'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
    
#     # Auxiliary / Modal verbs
#     'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#     'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#     'will': 'modal', 'would': 'modal', 'could': 'modal',
#     'should': 'modal', 'may': 'modal', 'might': 'modal',
#     'must': 'modal', 'can': 'modal',
    
#     # Adverbs
#     'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
#     'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
#     'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
#     'only': 'adverb', 'not': 'adverb', 'same': 'adverb',
    
#     # Quantifiers
#     'once': 'adverb',
# }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def is_location_category(category: str) -> bool:
#     """Check if a category indicates a location."""
#     if not category:
#         return False
#     cat_lower = category.lower().replace(' ', '_')
#     return (
#         cat_lower in LOCATION_CATEGORIES or
#         'city' in cat_lower or
#         'state' in cat_lower
#     )


# def parse_pos(pos_value: Any) -> str:
#     """Parse POS from various formats like \"['noun']\" or ['noun'] or 'noun'."""
#     if not pos_value:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 return parsed[0] if parsed else 'unknown'
#             except (json.JSONDecodeError, IndexError):
#                 return pos_value.strip("[]'\"")
#         return pos_value
    
#     # Handle actual list
#     if isinstance(pos_value, list):
#         return pos_value[0] if pos_value else 'unknown'
    
#     return str(pos_value)


# def get_ngram_type(term: str, entity_type: str = None) -> str:
#     """
#     Determine the n-gram type based on entity_type or word count.
    
#     Returns: 'unigram', 'bigram', 'trigram', 'quadgram', or 'ngram'
#     """
#     # If entity_type is explicitly set, use it
#     if entity_type and entity_type in ('unigram', 'bigram', 'trigram', 'quadgram', 'ngram'):
#         return entity_type
    
#     # Otherwise, count words
#     word_count = len(term.split())
    
#     if word_count == 1:
#         return 'unigram'
#     elif word_count == 2:
#         return 'bigram'
#     elif word_count == 3:
#         return 'trigram'
#     elif word_count == 4:
#         return 'quadgram'
#     else:
#         return 'ngram'


# # =============================================================================
# # VOCABULARY CACHE CLASS
# # =============================================================================

# @dataclass
# class VocabularyCache:
#     """In-memory cache for O(1) vocabulary lookups."""
    
#     # Core data structures
#     cities: Set[str] = field(default_factory=set)
#     states: Set[str] = field(default_factory=set)
#     locations: Set[str] = field(default_factory=set)
    
#     # N-gram storage (by word count)
#     unigrams: Dict[str, Dict] = field(default_factory=dict)  # Single words
#     bigrams: Dict[str, Dict] = field(default_factory=dict)   # Two words
#     trigrams: Dict[str, Dict] = field(default_factory=dict)  # Three words
#     quadgrams: Dict[str, Dict] = field(default_factory=dict) # Four words
#     ngrams: Dict[str, Dict] = field(default_factory=dict)    # 5+ words
    
#     # Legacy alias (points to unigrams for backward compatibility)
#     @property
#     def terms(self) -> Dict[str, Dict]:
#         return self.unigrams
    
#     stopwords: Set[str] = field(default_factory=lambda: set(DEFAULT_STOPWORDS))
    
#     # Metadata
#     loaded: bool = False
#     load_time: float = 0.0
#     term_count: int = 0
#     load_source: str = ""
#     last_updated: str = ""
    
#     # Thread safety
#     _lock: Lock = field(default_factory=Lock)
    
#     # Store the raw data for saving (key -> metadata)
#     _raw_data: Dict[str, Dict] = field(default_factory=dict)
    
#     # -------------------------------------------------------------------------
#     # FAST LOOKUP METHODS (O(1))
#     # -------------------------------------------------------------------------
    
#     def is_city(self, term: str) -> bool:
#         """Check if term is a city. O(1)"""
#         return term.lower() in self.cities
    
#     def is_state(self, term: str) -> bool:
#         """Check if term is a state. O(1)"""
#         return term.lower() in self.states
    
#     def is_location(self, term: str) -> bool:
#         """Check if term is any location type. O(1)"""
#         return term.lower() in self.locations
    
#     def is_stopword(self, term: str) -> bool:
#         """Check if term is a stopword. O(1)"""
#         return term.lower() in self.stopwords
    
#     def is_bigram(self, word1: str, word2: str) -> bool:
#         """Check if two words form a bigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return key in self.bigrams
    
#     def is_trigram(self, word1: str, word2: str, word3: str) -> bool:
#         """Check if three words form a trigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return key in self.trigrams
    
#     def is_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> bool:
#         """Check if four words form a quadgram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
#         return key in self.quadgrams
    
#     def has_term(self, term: str) -> bool:
#         """Check if term exists in any n-gram dict. O(1)"""
#         term_lower = term.lower()
#         return (
#             term_lower in self.unigrams or
#             term_lower in self.bigrams or
#             term_lower in self.trigrams or
#             term_lower in self.quadgrams or
#             term_lower in self.ngrams
#         )
    
#     def get_term(self, term: str) -> Optional[Dict]:
#         """Get term metadata from appropriate n-gram dict. O(1)"""
#         term_lower = term.lower()
#         return (
#             self.unigrams.get(term_lower) or
#             self.bigrams.get(term_lower) or
#             self.trigrams.get(term_lower) or
#             self.quadgrams.get(term_lower) or
#             self.ngrams.get(term_lower)
#         )
    
#     def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
#         """Get bigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return self.bigrams.get(key)
    
#     def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
#         """Get trigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return self.trigrams.get(key)
    
#     def get_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> Optional[Dict]:
#         """Get quadgram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
#         return self.quadgrams.get(key)
    
#     # -------------------------------------------------------------------------
#     # INTERNAL: Store term in appropriate dict
#     # -------------------------------------------------------------------------
    
#     def _store_term(self, term: str, metadata: Dict, category: str = None, entity_type: str = None) -> str:
#         """
#         Store a term in the appropriate n-gram dictionary.
        
#         Returns the ngram_type used for storage.
#         """
#         term_lower = term.lower()
#         category = category or metadata.get('category', '')
#         entity_type = entity_type or metadata.get('entity_type', '')
        
#         # Determine n-gram type
#         ngram_type = get_ngram_type(term_lower, entity_type)
        
#         # Store in appropriate dict
#         if ngram_type == 'unigram':
#             self.unigrams[term_lower] = metadata
#         elif ngram_type == 'bigram':
#             self.bigrams[term_lower] = metadata
#         elif ngram_type == 'trigram':
#             self.trigrams[term_lower] = metadata
#         elif ngram_type == 'quadgram':
#             self.quadgrams[term_lower] = metadata
#         else:
#             self.ngrams[term_lower] = metadata
        
#         # Handle location categories
#         category_lower = category.lower().replace(' ', '_')
#         if category_lower in ('us_city', 'us city', 'city'):
#             self.cities.add(term_lower)
#             self.locations.add(term_lower)
#         elif category_lower in ('us_state', 'us state', 'state'):
#             self.states.add(term_lower)
#             self.locations.add(term_lower)
#         elif is_location_category(category):
#             self.locations.add(term_lower)
        
#         return ngram_type
    
#     # -------------------------------------------------------------------------
#     # QUERY CLASSIFICATION (Single Pass)
#     # -------------------------------------------------------------------------
    
#     def get_stopword_pos(self, word: str) -> str:
#         """Get POS for a stopword."""
#         return STOPWORD_POS.get(word.lower(), 'unknown')
    
#     def classify_query(self, query: str) -> Dict[str, Any]:
#         """
#         Classify all terms in a query in a single pass.
        
#         Checks for quadgrams, trigrams, bigrams, then unigrams.
#         Stopwords ARE included with POS information for grammar context.
        
#         Returns:
#             {
#                 "locations": ["georgia", "atlanta"],
#                 "quadgrams": [],
#                 "trigrams": [],
#                 "bigrams": ["new york"],
#                 "terms": {"georgia": {...}, "the": {...}, ...},
#                 "unknown": [],
#                 "stopwords": ["the", "in"],
#             }
#         """
#         words = query.lower().split()
        
#         result = {
#             'locations': [],
#             'quadgrams': [],
#             'trigrams': [],
#             'bigrams': [],
#             'terms': {},
#             'unknown': [],
#             'stopwords': [],
#         }
        
#         used_indices = set()
        
#         # Pass 1: Quadgrams (4 words)
#         for i in range(len(words) - 3):
#             if any(idx in used_indices for idx in [i, i+1, i+2, i+3]):
#                 continue
#             quad_key = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]}"
#             if quad_key in self.quadgrams:
#                 result['quadgrams'].append(quad_key)
#                 metadata = self.quadgrams[quad_key]
#                 result['terms'][quad_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(quad_key)
#                 used_indices.update([i, i+1, i+2, i+3])
        
#         # Pass 2: Trigrams (3 words)
#         for i in range(len(words) - 2):
#             if any(idx in used_indices for idx in [i, i+1, i+2]):
#                 continue
#             trigram_key = f"{words[i]} {words[i+1]} {words[i+2]}"
#             if trigram_key in self.trigrams:
#                 result['trigrams'].append(trigram_key)
#                 metadata = self.trigrams[trigram_key]
#                 result['terms'][trigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(trigram_key)
#                 used_indices.update([i, i+1, i+2])
        
#         # Pass 3: Bigrams (2 words)
#         for i in range(len(words) - 1):
#             if i in used_indices or i+1 in used_indices:
#                 continue
#             bigram_key = f"{words[i]} {words[i+1]}"
#             if bigram_key in self.bigrams:
#                 result['bigrams'].append(bigram_key)
#                 metadata = self.bigrams[bigram_key]
#                 result['terms'][bigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(bigram_key)
#                 used_indices.update([i, i+1])
        
#         # Pass 4: Individual words (unigrams + stopwords)
#         for i, word in enumerate(words):
#             if i in used_indices:
#                 continue
            
#             # Check if it's in our unigrams first
#             if word in self.unigrams:
#                 metadata = self.unigrams[word]
#                 result['terms'][word] = metadata
#                 if word in self.locations:
#                     result['locations'].append(word)
#                 if word in self.stopwords:
#                     result['stopwords'].append(word)
            
#             # If it's a stopword but NOT in vocabulary, still include with POS
#             elif word in self.stopwords:
#                 result['stopwords'].append(word)
#                 result['terms'][word] = {
#                     'term': word,
#                     'category': 'Dictionary Word',
#                     'pos': self.get_stopword_pos(word),
#                     'entity_type': 'stopword',
#                     'is_stopword': True,
#                 }
            
#             # Unknown word
#             else:
#                 result['unknown'].append(word)
        
#         return result
    
#     # -------------------------------------------------------------------------
#     # LOADING METHODS
#     # -------------------------------------------------------------------------
    
#     def load_from_dict(self, data: Dict[str, Dict], source: str = "api") -> bool:
#         """
#         Load vocabulary from a dictionary (from Colab endpoint or JSON file).
#         REPLACES all existing data.
        
#         Args:
#             data: Dict with keys like "term:atlanta:us_city" and value dicts
#             source: Where the data came from (for logging)
        
#         Returns:
#             True if successful
#         """
#         with self._lock:
#             start_time = time.perf_counter()
#             logger.info(f"Loading vocabulary cache from {source}...")
            
#             # Clear ALL existing data
#             self.cities.clear()
#             self.states.clear()
#             self.locations.clear()
#             self.unigrams.clear()
#             self.bigrams.clear()
#             self.trigrams.clear()
#             self.quadgrams.clear()
#             self.ngrams.clear()
#             self._raw_data.clear()
            
#             # Counters
#             counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}
#             city_count = 0
#             state_count = 0
            
#             for key, metadata in data.items():
#                 try:
#                     # Store raw data
#                     self._raw_data[key] = metadata
                    
#                     # Parse key format: "term:word:category_slug"
#                     parts = key.split(':')
#                     if len(parts) >= 2:
#                         term = parts[1].lower()
#                     else:
#                         term = metadata.get('term', '').lower()
                    
#                     if not term:
#                         continue
                    
#                     category = metadata.get('category', '')
#                     entity_type = metadata.get('entity_type', '')
                    
#                     # Store in appropriate dict
#                     ngram_type = self._store_term(term, metadata, category, entity_type)
#                     counts[ngram_type] = counts.get(ngram_type, 0) + 1
                    
#                     # Count locations
#                     category_lower = category.lower().replace(' ', '_')
#                     if category_lower in ('us_city', 'us city', 'city'):
#                         city_count += 1
#                     elif category_lower in ('us_state', 'us state', 'state'):
#                         state_count += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing key '{key}': {e}")
#                     continue
            
#             # Update metadata
#             self.term_count = len(self._raw_data)
#             self.load_time = time.perf_counter() - start_time
#             self.load_source = source
#             self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             self.loaded = True
            
#             logger.info(
#                 f"Cache loaded: {self.term_count:,} total, "
#                 f"{counts['unigram']:,} unigrams, {counts['bigram']:,} bigrams, "
#                 f"{counts['trigram']:,} trigrams, {counts['quadgram']:,} quadgrams, "
#                 f"{counts['ngram']:,} ngrams, "
#                 f"{city_count:,} cities, {state_count:,} states "
#                 f"in {self.load_time:.2f}s"
#             )
            
#             # Save to file as backup
#             self._save_to_file(self._raw_data)
            
#             return True
    
#     def add_terms(self, data: Dict[str, Dict]) -> Dict[str, int]:
#         """
#         Add new terms WITHOUT clearing existing data.
#         Only adds terms that don't already exist.
#         Supports unigrams, bigrams, trigrams, quadgrams, and n-grams.
        
#         Args:
#             data: Dict with keys like "term:atlanta:us_city" and value dicts
        
#         Returns:
#             Dict with 'added', 'skipped', and counts by type
#         """
#         with self._lock:
#             added = 0
#             skipped = 0
#             counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}
            
#             for key, metadata in data.items():
#                 # Skip if already exists in raw data
#                 if key in self._raw_data:
#                     skipped += 1
#                     continue
                
#                 try:
#                     # Add to raw data
#                     self._raw_data[key] = metadata
                    
#                     # Parse key format: "term:word:category_slug"
#                     parts = key.split(':')
#                     if len(parts) >= 2:
#                         term = parts[1].lower()
#                     else:
#                         term = metadata.get('term', '').lower()
                    
#                     if not term:
#                         continue
                    
#                     category = metadata.get('category', '')
#                     entity_type = metadata.get('entity_type', '')
                    
#                     # Store in appropriate dict
#                     ngram_type = self._store_term(term, metadata, category, entity_type)
#                     counts[ngram_type] = counts.get(ngram_type, 0) + 1
#                     added += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing key '{key}': {e}")
#                     continue
            
#             # Update metadata
#             self.term_count = len(self._raw_data)
#             self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
#             # Save to file (the full combined data)
#             self._save_to_file(self._raw_data)
            
#             logger.info(
#                 f"Added {added:,} terms (skipped {skipped:,}): "
#                 f"{counts['unigram']} unigrams, {counts['bigram']} bigrams, "
#                 f"{counts['trigram']} trigrams, {counts['quadgram']} quadgrams"
#             )
            
#             return {
#                 'added': added,
#                 'skipped': skipped,
#                 'unigrams_added': counts['unigram'],
#                 'bigrams_added': counts['bigram'],
#                 'trigrams_added': counts['trigram'],
#                 'quadgrams_added': counts['quadgram'],
#                 'ngrams_added': counts['ngram'],
#             }
    
#     def load_from_file(self) -> bool:
#         """Load from local JSON file (for startup)."""
#         if not CACHE_FILE.exists():
#             logger.info(f"No cache file found at {CACHE_FILE}")
#             return False
        
#         try:
#             logger.info(f"Loading vocabulary cache from {CACHE_FILE}...")
#             with open(CACHE_FILE, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#             return self.load_from_dict(data, source="file")
#         except Exception as e:
#             logger.error(f"Failed to load from file: {e}")
#             return False
    
#     def _save_to_file(self, data: Dict) -> bool:
#         """Save data to local file as backup."""
#         try:
#             CACHE_DIR.mkdir(parents=True, exist_ok=True)
#             with open(CACHE_FILE, 'w', encoding='utf-8') as f:
#                 json.dump(data, f, ensure_ascii=False)
#             logger.info(f"Cache saved to {CACHE_FILE} ({len(data):,} terms)")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save cache file: {e}")
#             return False
    
#     def load(self) -> bool:
#         """Load cache from available source (file or wait for API)."""
#         if self.loaded:
#             return True
#         return self.load_from_file()
    
#     def reload_from_file(self) -> bool:
#         """Force reload from file."""
#         with self._lock:
#             self.loaded = False
#         return self.load_from_file()
    
#     # -------------------------------------------------------------------------
#     # STATUS
#     # -------------------------------------------------------------------------
    
#     def status(self) -> Dict[str, Any]:
#         """Get cache status."""
#         return {
#             'loaded': self.loaded,
#             'term_count': self.term_count,
#             'unigrams': len(self.unigrams),
#             'bigrams': len(self.bigrams),
#             'trigrams': len(self.trigrams),
#             'quadgrams': len(self.quadgrams),
#             'ngrams': len(self.ngrams),
#             'cities': len(self.cities),
#             'states': len(self.states),
#             'locations': len(self.locations),
#             'load_time': f"{self.load_time:.2f}s",
#             'load_source': self.load_source,
#             'last_updated': self.last_updated,
#         }
    
#     def __repr__(self) -> str:
#         return (
#             f"VocabularyCache(loaded={self.loaded}, "
#             f"terms={self.term_count:,}, "
#             f"unigrams={len(self.unigrams):,}, "
#             f"bigrams={len(self.bigrams):,}, "
#             f"trigrams={len(self.trigrams):,}, "
#             f"quadgrams={len(self.quadgrams):,})"
#         )


# # =============================================================================
# # SINGLETON INSTANCE
# # =============================================================================

# vocab_cache = VocabularyCache()


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def is_city(term: str) -> bool:
#     return vocab_cache.is_city(term)

# def is_state(term: str) -> bool:
#     return vocab_cache.is_state(term)

# def is_location(term: str) -> bool:
#     return vocab_cache.is_location(term)

# def get_term(term: str) -> Optional[Dict]:
#     return vocab_cache.get_term(term)

# def classify_query(query: str) -> Dict[str, Any]:
#     return vocab_cache.classify_query(query)

# def ensure_loaded() -> bool:
#     """Ensure cache is loaded. Called by apps.py at startup."""
#     if not vocab_cache.loaded:
#         return vocab_cache.load()
#     return True

# def get_cache_status() -> Dict[str, Any]:
#     return vocab_cache.status()

# def reload_cache_from_dict(data: Dict) -> bool:
#     return vocab_cache.load_from_dict(data, source="api")

# def reload_cache_from_file() -> bool:
#     return vocab_cache.reload_from_file()

# def add_terms_to_cache(data: Dict) -> Dict[str, int]:
#     """Add new terms without overwriting existing ones."""
#     return vocab_cache.add_terms(data)


# # =============================================================================
# # CLI FOR TESTING
# # =============================================================================

# if __name__ == "__main__":
#     import argparse
    
#     parser = argparse.ArgumentParser(description="Vocabulary Cache")
#     parser.add_argument('--status', action='store_true', help='Show status')
#     parser.add_argument('--load', action='store_true', help='Load from file')
#     parser.add_argument('--test', action='store_true', help='Test lookups')
#     parser.add_argument('--query', type=str, help='Classify a query')
    
#     args = parser.parse_args()
    
#     if args.load:
#         vocab_cache.load_from_file()
    
#     if args.status:
#         print(vocab_cache.status())
    
#     if args.test:
#         vocab_cache.load()
#         tests = ["atlanta", "georgia", "new york", "xyz123"]
#         for t in tests:
#             print(f"{t}: location={vocab_cache.is_location(t)}, data={vocab_cache.get_term(t)}")
    
#     if args.query:
#         vocab_cache.load()
#         result = vocab_cache.classify_query(args.query)
#         print(json.dumps(result, indent=2))


# """
# vocabulary_cache.py
# In-memory cache for fast term lookups.

# Loads vocabulary data from:
# 1. API endpoint (POST from Colab)
# 2. Local JSON file (backup/startup)

# Supports:
# - Unigrams (single words)
# - Bigrams (two words)
# - Trigrams (three words)
# - Quadgrams (four words)
# - Any n-gram

# PERFORMANCE:
#     - Redis lookup: ~300ms per term
#     - Cache lookup: ~0.001ms per term
#     - Speedup: ~200,000x
# """

# import json
# import logging
# import time
# from pathlib import Path
# from typing import Dict, Set, Optional, Any
# from threading import Lock
# from dataclasses import dataclass, field
# from datetime import datetime

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Categories that indicate a location (lowercase for comparison)
# LOCATION_CATEGORIES = frozenset({
#     'us_city', 'us city', 'city',
#     'us_state', 'us state', 'state',
#     'location', 'region', 'country', 'neighborhood',
# })

# # Cache file path (backup for startup)
# CACHE_DIR = Path(__file__).parent / 'cache' / 'data'
# CACHE_FILE = CACHE_DIR / 'vocabulary_data.json'

# # Stopwords - common words that still need POS tagging for grammar context
# DEFAULT_STOPWORDS = frozenset({
#     'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'and',
#     'or', 'but', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
#     'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
#     'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
#     'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
#     'what', 'which', 'who', 'whom', 'where', 'when', 'why', 'how',
#     'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
#     'some', 'such', 'no', 'not', 'only', 'own', 'same', 'so', 'than',
#     'too', 'very', 'just', 'also', 'now', 'here', 'there', 'then',
#     'from', 'by', 'into', 'through', 'during', 'before', 'after',
#     'me', 'my', 'your', 'his', 'her', 'its', 'our', 'their',
# })

# # POS mapping for stopwords (used when not found in main vocabulary)
# STOPWORD_POS = {
#     # Determiners / Articles
#     'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
#     'this': 'determiner', 'that': 'determiner', 
#     'these': 'determiner', 'those': 'determiner',
#     'some': 'determiner', 'any': 'determiner',
#     'each': 'determiner', 'every': 'determiner',
#     'all': 'determiner', 'both': 'determiner',
#     'no': 'determiner', 'few': 'determiner',
#     'more': 'determiner', 'most': 'determiner',
#     'other': 'determiner', 'such': 'determiner',
    
#     # Prepositions
#     'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#     'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#     'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
#     'into': 'preposition', 'through': 'preposition',
#     'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
#     'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
#     'above': 'preposition', 'below': 'preposition',
    
#     # Conjunctions
#     'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#     'so': 'conjunction', 'than': 'conjunction',
    
#     # Pronouns
#     'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#     'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#     'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
#     'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
    
#     # Possessive determiners
#     'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
#     'her': 'determiner', 'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
#     'own': 'determiner',
    
#     # Be verbs
#     'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#     'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
    
#     # Auxiliary / Modal verbs
#     'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#     'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#     'will': 'modal', 'would': 'modal', 'could': 'modal',
#     'should': 'modal', 'may': 'modal', 'might': 'modal',
#     'must': 'modal', 'can': 'modal',
    
#     # Adverbs
#     'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
#     'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
#     'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
#     'only': 'adverb', 'not': 'adverb', 'same': 'adverb',
    
#     # Quantifiers
#     'once': 'adverb',
# }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def is_location_category(category: str) -> bool:
#     """Check if a category indicates a location."""
#     if not category:
#         return False
#     cat_lower = category.lower().replace(' ', '_')
#     return (
#         cat_lower in LOCATION_CATEGORIES or
#         'city' in cat_lower or
#         'state' in cat_lower
#     )


# def parse_pos(pos_value: Any) -> str:
#     """Parse POS from various formats like \"['noun']\" or ['noun'] or 'noun'."""
#     if not pos_value:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 return parsed[0] if parsed else 'unknown'
#             except (json.JSONDecodeError, IndexError):
#                 return pos_value.strip("[]'\"")
#         return pos_value
    
#     # Handle actual list
#     if isinstance(pos_value, list):
#         return pos_value[0] if pos_value else 'unknown'
    
#     return str(pos_value)


# def get_ngram_type(term: str, entity_type: str = None) -> str:
#     """
#     Determine the n-gram type based on entity_type or word count.
    
#     Returns: 'unigram', 'bigram', 'trigram', 'quadgram', or 'ngram'
#     """
#     # If entity_type is explicitly set, use it
#     if entity_type and entity_type in ('unigram', 'bigram', 'trigram', 'quadgram', 'ngram'):
#         return entity_type
    
#     # Otherwise, count words
#     word_count = len(term.split())
    
#     if word_count == 1:
#         return 'unigram'
#     elif word_count == 2:
#         return 'bigram'
#     elif word_count == 3:
#         return 'trigram'
#     elif word_count == 4:
#         return 'quadgram'
#     else:
#         return 'ngram'


# # =============================================================================
# # VOCABULARY CACHE CLASS
# # =============================================================================

# @dataclass
# class VocabularyCache:
#     """In-memory cache for O(1) vocabulary lookups."""
    
#     # Core data structures
#     cities: Set[str] = field(default_factory=set)
#     states: Set[str] = field(default_factory=set)
#     locations: Set[str] = field(default_factory=set)
    
#     # N-gram storage (by word count)
#     unigrams: Dict[str, Dict] = field(default_factory=dict)  # Single words
#     bigrams: Dict[str, Dict] = field(default_factory=dict)   # Two words
#     trigrams: Dict[str, Dict] = field(default_factory=dict)  # Three words
#     quadgrams: Dict[str, Dict] = field(default_factory=dict) # Four words
#     ngrams: Dict[str, Dict] = field(default_factory=dict)    # 5+ words
    
#     # Legacy alias (points to unigrams for backward compatibility)
#     @property
#     def terms(self) -> Dict[str, Dict]:
#         return self.unigrams
    
#     stopwords: Set[str] = field(default_factory=lambda: set(DEFAULT_STOPWORDS))
    
#     # Metadata
#     loaded: bool = False
#     load_time: float = 0.0
#     term_count: int = 0
#     load_source: str = ""
#     last_updated: str = ""
    
#     # Thread safety
#     _lock: Lock = field(default_factory=Lock)
    
#     # Store the raw data for saving (key -> metadata)
#     _raw_data: Dict[str, Dict] = field(default_factory=dict)
    
#     # -------------------------------------------------------------------------
#     # FAST LOOKUP METHODS (O(1))
#     # -------------------------------------------------------------------------
    
#     def is_city(self, term: str) -> bool:
#         """Check if term is a city. O(1)"""
#         return term.lower() in self.cities
    
#     def is_state(self, term: str) -> bool:
#         """Check if term is a state. O(1)"""
#         return term.lower() in self.states
    
#     def is_location(self, term: str) -> bool:
#         """Check if term is any location type. O(1)"""
#         return term.lower() in self.locations
    
#     def is_stopword(self, term: str) -> bool:
#         """Check if term is a stopword. O(1)"""
#         return term.lower() in self.stopwords
    
#     def is_bigram(self, word1: str, word2: str) -> bool:
#         """Check if two words form a bigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return key in self.bigrams
    
#     def is_trigram(self, word1: str, word2: str, word3: str) -> bool:
#         """Check if three words form a trigram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return key in self.trigrams
    
#     def is_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> bool:
#         """Check if four words form a quadgram. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
#         return key in self.quadgrams
    
#     def has_term(self, term: str) -> bool:
#         """Check if term exists in any n-gram dict. O(1)"""
#         term_lower = term.lower()
#         return (
#             term_lower in self.unigrams or
#             term_lower in self.bigrams or
#             term_lower in self.trigrams or
#             term_lower in self.quadgrams or
#             term_lower in self.ngrams
#         )
    
#     def get_term(self, term: str) -> Optional[Dict]:
#         """Get term metadata from appropriate n-gram dict. O(1)"""
#         term_lower = term.lower()
#         return (
#             self.unigrams.get(term_lower) or
#             self.bigrams.get(term_lower) or
#             self.trigrams.get(term_lower) or
#             self.quadgrams.get(term_lower) or
#             self.ngrams.get(term_lower)
#         )
    
#     def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
#         """Get bigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()}"
#         return self.bigrams.get(key)
    
#     def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
#         """Get trigram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
#         return self.trigrams.get(key)
    
#     def get_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> Optional[Dict]:
#         """Get quadgram metadata. O(1)"""
#         key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
#         return self.quadgrams.get(key)
    
#     # -------------------------------------------------------------------------
#     # INTERNAL: Store term in appropriate dict
#     # -------------------------------------------------------------------------
    
#     def _store_term(self, term: str, metadata: Dict, category: str = None, entity_type: str = None) -> str:
#         """
#         Store a term in the appropriate n-gram dictionary.
        
#         Returns the ngram_type used for storage.
#         """
#         term_lower = term.lower()
#         category = category or metadata.get('category', '')
#         entity_type = entity_type or metadata.get('entity_type', '')
        
#         # Determine n-gram type
#         ngram_type = get_ngram_type(term_lower, entity_type)
        
#         # Store in appropriate dict
#         if ngram_type == 'unigram':
#             self.unigrams[term_lower] = metadata
#         elif ngram_type == 'bigram':
#             self.bigrams[term_lower] = metadata
#         elif ngram_type == 'trigram':
#             self.trigrams[term_lower] = metadata
#         elif ngram_type == 'quadgram':
#             self.quadgrams[term_lower] = metadata
#         else:
#             self.ngrams[term_lower] = metadata
        
#         # Handle location categories
#         category_lower = category.lower().replace(' ', '_')
#         if category_lower in ('us_city', 'us city', 'city'):
#             self.cities.add(term_lower)
#             self.locations.add(term_lower)
#         elif category_lower in ('us_state', 'us state', 'state'):
#             self.states.add(term_lower)
#             self.locations.add(term_lower)
#         elif is_location_category(category):
#             self.locations.add(term_lower)
        
#         return ngram_type
    
#     # -------------------------------------------------------------------------
#     # QUERY CLASSIFICATION (Single Pass)
#     # -------------------------------------------------------------------------
    
#     def get_stopword_pos(self, word: str) -> str:
#         """Get POS for a stopword."""
#         return STOPWORD_POS.get(word.lower(), 'unknown')
    
#     def classify_query(self, query: str) -> Dict[str, Any]:
#         """
#         Classify all terms in a query in a single pass.
        
#         Checks for quadgrams, trigrams, bigrams, then unigrams.
#         Stopwords ARE included with POS information for grammar context.
        
#         Returns:
#             {
#                 "locations": ["georgia", "atlanta"],
#                 "quadgrams": [],
#                 "trigrams": [],
#                 "bigrams": ["new york"],
#                 "terms": {"georgia": {...}, "the": {...}, ...},
#                 "unknown": [],
#                 "stopwords": ["the", "in"],
#             }
#         """
#         words = query.lower().split()
        
#         result = {
#             'locations': [],
#             'quadgrams': [],
#             'trigrams': [],
#             'bigrams': [],
#             'terms': {},
#             'unknown': [],
#             'stopwords': [],
#         }
        
#         used_indices = set()
        
#         # Pass 1: Quadgrams (4 words)
#         for i in range(len(words) - 3):
#             if any(idx in used_indices for idx in [i, i+1, i+2, i+3]):
#                 continue
#             quad_key = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]}"
#             if quad_key in self.quadgrams:
#                 result['quadgrams'].append(quad_key)
#                 metadata = self.quadgrams[quad_key]
#                 result['terms'][quad_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(quad_key)
#                 used_indices.update([i, i+1, i+2, i+3])
        
#         # Pass 2: Trigrams (3 words)
#         for i in range(len(words) - 2):
#             if any(idx in used_indices for idx in [i, i+1, i+2]):
#                 continue
#             trigram_key = f"{words[i]} {words[i+1]} {words[i+2]}"
#             if trigram_key in self.trigrams:
#                 result['trigrams'].append(trigram_key)
#                 metadata = self.trigrams[trigram_key]
#                 result['terms'][trigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(trigram_key)
#                 used_indices.update([i, i+1, i+2])
        
#         # Pass 3: Bigrams (2 words)
#         for i in range(len(words) - 1):
#             if i in used_indices or i+1 in used_indices:
#                 continue
#             bigram_key = f"{words[i]} {words[i+1]}"
#             if bigram_key in self.bigrams:
#                 result['bigrams'].append(bigram_key)
#                 metadata = self.bigrams[bigram_key]
#                 result['terms'][bigram_key] = metadata
#                 if is_location_category(metadata.get('category', '')):
#                     result['locations'].append(bigram_key)
#                 used_indices.update([i, i+1])
        
#         # Pass 4: Individual words (unigrams + stopwords)
#         for i, word in enumerate(words):
#             if i in used_indices:
#                 continue
            
#             # Check if it's in our unigrams first
#             if word in self.unigrams:
#                 metadata = self.unigrams[word]
#                 result['terms'][word] = metadata
#                 if word in self.locations:
#                     result['locations'].append(word)
#                 if word in self.stopwords:
#                     result['stopwords'].append(word)
            
#             # If it's a stopword but NOT in vocabulary, still include with POS
#             elif word in self.stopwords:
#                 result['stopwords'].append(word)
#                 result['terms'][word] = {
#                     'term': word,
#                     'category': 'Dictionary Word',
#                     'pos': self.get_stopword_pos(word),
#                     'entity_type': 'stopword',
#                     'is_stopword': True,
#                 }
            
#             # Unknown word
#             else:
#                 result['unknown'].append(word)
        
#         return result
    
#     # -------------------------------------------------------------------------
#     # LOADING METHODS
#     # -------------------------------------------------------------------------
    
#     def load_from_dict(self, data: Dict[str, Dict], source: str = "api") -> bool:
#         """
#         Load vocabulary from a dictionary (from Colab endpoint or JSON file).
#         REPLACES all existing data.
        
#         Args:
#             data: Dict with keys like "term:atlanta:us_city" and value dicts
#             source: Where the data came from (for logging)
        
#         Returns:
#             True if successful
#         """
#         with self._lock:
#             start_time = time.perf_counter()
#             logger.info(f"Loading vocabulary cache from {source}...")
            
#             # Clear ALL existing data
#             self.cities.clear()
#             self.states.clear()
#             self.locations.clear()
#             self.unigrams.clear()
#             self.bigrams.clear()
#             self.trigrams.clear()
#             self.quadgrams.clear()
#             self.ngrams.clear()
#             self._raw_data.clear()
            
#             # Counters
#             counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}
#             city_count = 0
#             state_count = 0
            
#             for key, metadata in data.items():
#                 try:
#                     # Store raw data
#                     self._raw_data[key] = metadata
                    
#                     # Parse key format: "term:word:category_slug"
#                     parts = key.split(':')
#                     if len(parts) >= 2:
#                         term = parts[1].lower()
#                     else:
#                         term = metadata.get('term', '').lower()
                    
#                     if not term:
#                         continue
                    
#                     category = metadata.get('category', '')
#                     entity_type = metadata.get('entity_type', '')
                    
#                     # Store in appropriate dict
#                     ngram_type = self._store_term(term, metadata, category, entity_type)
#                     counts[ngram_type] = counts.get(ngram_type, 0) + 1
                    
#                     # Count locations
#                     category_lower = category.lower().replace(' ', '_')
#                     if category_lower in ('us_city', 'us city', 'city'):
#                         city_count += 1
#                     elif category_lower in ('us_state', 'us state', 'state'):
#                         state_count += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing key '{key}': {e}")
#                     continue
            
#             # Update metadata
#             self.term_count = len(self._raw_data)
#             self.load_time = time.perf_counter() - start_time
#             self.load_source = source
#             self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             self.loaded = True
            
#             logger.info(
#                 f"Cache loaded: {self.term_count:,} total, "
#                 f"{counts['unigram']:,} unigrams, {counts['bigram']:,} bigrams, "
#                 f"{counts['trigram']:,} trigrams, {counts['quadgram']:,} quadgrams, "
#                 f"{counts['ngram']:,} ngrams, "
#                 f"{city_count:,} cities, {state_count:,} states "
#                 f"in {self.load_time:.2f}s"
#             )
            
#             # Save to file as backup
#             self._save_to_file(self._raw_data)
            
#             return True
    
#     def add_terms(self, data: Dict[str, Dict]) -> Dict[str, int]:
#         """
#         Add new terms WITHOUT clearing existing data.
#         Only adds terms that don't already exist.
#         Supports unigrams, bigrams, trigrams, quadgrams, and n-grams.
        
#         FIX: Ensures existing data is loaded from file first, so new terms
#         are MERGED with existing vocabulary instead of replacing it.
        
#         Args:
#             data: Dict with keys like "term:atlanta:us_city" and value dicts
        
#         Returns:
#             Dict with 'added', 'skipped', and counts by type
#         """
#         # =====================================================================
#         # FIX: Load existing data from file FIRST if cache is empty
#         # This prevents losing 224,928+ existing terms when adding new ones
#         # =====================================================================
#         if not self.loaded:
#             logger.info("Cache not loaded — loading from file before adding terms...")
#             self.load_from_file()
        
#         with self._lock:
#             added = 0
#             skipped = 0
#             counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}
            
#             for key, metadata in data.items():
#                 # Skip if already exists in raw data
#                 if key in self._raw_data:
#                     skipped += 1
#                     continue
                
#                 try:
#                     # Add to raw data
#                     self._raw_data[key] = metadata
                    
#                     # Parse key format: "term:word:category_slug"
#                     parts = key.split(':')
#                     if len(parts) >= 2:
#                         term = parts[1].lower()
#                     else:
#                         term = metadata.get('term', '').lower()
                    
#                     if not term:
#                         continue
                    
#                     category = metadata.get('category', '')
#                     entity_type = metadata.get('entity_type', '')
                    
#                     # Store in appropriate dict
#                     ngram_type = self._store_term(term, metadata, category, entity_type)
#                     counts[ngram_type] = counts.get(ngram_type, 0) + 1
#                     added += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing key '{key}': {e}")
#                     continue
            
#             # Update metadata
#             self.term_count = len(self._raw_data)
#             self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
#             # Save to file (the full combined data — existing + new)
#             self._save_to_file(self._raw_data)
            
#             logger.info(
#                 f"Added {added:,} terms (skipped {skipped:,}): "
#                 f"{counts['unigram']} unigrams, {counts['bigram']} bigrams, "
#                 f"{counts['trigram']} trigrams, {counts['quadgram']} quadgrams"
#             )
            
#             return {
#                 'added': added,
#                 'skipped': skipped,
#                 'unigrams_added': counts['unigram'],
#                 'bigrams_added': counts['bigram'],
#                 'trigrams_added': counts['trigram'],
#                 'quadgrams_added': counts['quadgram'],
#                 'ngrams_added': counts['ngram'],
#             }
    
#     def load_from_file(self) -> bool:
#         """Load from local JSON file (for startup)."""
#         if not CACHE_FILE.exists():
#             logger.info(f"No cache file found at {CACHE_FILE}")
#             return False
        
#         try:
#             logger.info(f"Loading vocabulary cache from {CACHE_FILE}...")
#             with open(CACHE_FILE, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#             return self.load_from_dict(data, source="file")
#         except Exception as e:
#             logger.error(f"Failed to load from file: {e}")
#             return False
    
#     def _save_to_file(self, data: Dict) -> bool:
#         """Save data to local file as backup."""
#         try:
#             CACHE_DIR.mkdir(parents=True, exist_ok=True)
#             with open(CACHE_FILE, 'w', encoding='utf-8') as f:
#                 json.dump(data, f, ensure_ascii=False)
#             logger.info(f"Cache saved to {CACHE_FILE} ({len(data):,} terms)")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save cache file: {e}")
#             return False
    
#     def load(self) -> bool:
#         """Load cache from available source (file or wait for API)."""
#         if self.loaded:
#             return True
#         return self.load_from_file()
    
#     def reload_from_file(self) -> bool:
#         """Force reload from file."""
#         with self._lock:
#             self.loaded = False
#         return self.load_from_file()
    
#     # -------------------------------------------------------------------------
#     # STATUS
#     # -------------------------------------------------------------------------
    
#     def status(self) -> Dict[str, Any]:
#         """Get cache status."""
#         return {
#             'loaded': self.loaded,
#             'term_count': self.term_count,
#             'unigrams': len(self.unigrams),
#             'bigrams': len(self.bigrams),
#             'trigrams': len(self.trigrams),
#             'quadgrams': len(self.quadgrams),
#             'ngrams': len(self.ngrams),
#             'cities': len(self.cities),
#             'states': len(self.states),
#             'locations': len(self.locations),
#             'load_time': f"{self.load_time:.2f}s",
#             'load_source': self.load_source,
#             'last_updated': self.last_updated,
#         }
    
#     def __repr__(self) -> str:
#         return (
#             f"VocabularyCache(loaded={self.loaded}, "
#             f"terms={self.term_count:,}, "
#             f"unigrams={len(self.unigrams):,}, "
#             f"bigrams={len(self.bigrams):,}, "
#             f"trigrams={len(self.trigrams):,}, "
#             f"quadgrams={len(self.quadgrams):,})"
#         )


# # =============================================================================
# # SINGLETON INSTANCE
# # =============================================================================

# vocab_cache = VocabularyCache()


# # =============================================================================
# # CONVENIENCE FUNCTIONS
# # =============================================================================

# def is_city(term: str) -> bool:
#     return vocab_cache.is_city(term)

# def is_state(term: str) -> bool:
#     return vocab_cache.is_state(term)

# def is_location(term: str) -> bool:
#     return vocab_cache.is_location(term)

# def get_term(term: str) -> Optional[Dict]:
#     return vocab_cache.get_term(term)

# def classify_query(query: str) -> Dict[str, Any]:
#     return vocab_cache.classify_query(query)

# def ensure_loaded() -> bool:
#     """Ensure cache is loaded. Called by apps.py at startup."""
#     if not vocab_cache.loaded:
#         return vocab_cache.load()
#     return True

# def get_cache_status() -> Dict[str, Any]:
#     return vocab_cache.status()

# def reload_cache_from_dict(data: Dict) -> bool:
#     return vocab_cache.load_from_dict(data, source="api")

# def reload_cache_from_file() -> bool:
#     return vocab_cache.reload_from_file()

# def add_terms_to_cache(data: Dict) -> Dict[str, int]:
#     """Add new terms without overwriting existing ones."""
#     return vocab_cache.add_terms(data)


# # =============================================================================
# # CLI FOR TESTING
# # =============================================================================

# if __name__ == "__main__":
#     import argparse
    
#     parser = argparse.ArgumentParser(description="Vocabulary Cache")
#     parser.add_argument('--status', action='store_true', help='Show status')
#     parser.add_argument('--load', action='store_true', help='Load from file')
#     parser.add_argument('--test', action='store_true', help='Test lookups')
#     parser.add_argument('--query', type=str, help='Classify a query')
    
#     args = parser.parse_args()
    
#     if args.load:
#         vocab_cache.load_from_file()
    
#     if args.status:
#         print(vocab_cache.status())
    
#     if args.test:
#         vocab_cache.load()
#         tests = ["atlanta", "georgia", "new york", "xyz123"]
#         for t in tests:
#             print(f"{t}: location={vocab_cache.is_location(t)}, data={vocab_cache.get_term(t)}")
    
#     if args.query:
#         vocab_cache.load()
#         result = vocab_cache.classify_query(args.query)
#         print(json.dumps(result, indent=2))


"""
vocabulary_cache.py
In-memory cache for fast term lookups.

Loads vocabulary data from:
1. API endpoint (POST from Colab)
2. Local JSON file (backup/startup)

Supports:
- Unigrams (single words)
- Bigrams (two words)
- Trigrams (three words)
- Quadgrams (four words)
- Any n-gram
- MULTI-MATCH: Multiple category entries per term (e.g., "africa" = Continent + Country + US City)

PERFORMANCE:
    - Redis lookup: ~300ms per term
    - Cache lookup: ~0.001ms per term
    - Speedup: ~200,000x
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, Set, List, Optional, Any
from threading import Lock
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

LOCATION_CATEGORIES = frozenset({
    'us_city', 'us city', 'city',
    'us_state', 'us state', 'state',
    'location', 'region', 'country', 'neighborhood',
    'continent',
})

CACHE_DIR = Path(__file__).parent / 'cache' / 'data'
CACHE_FILE = CACHE_DIR / 'vocabulary_data.json'

DEFAULT_STOPWORDS = frozenset({
    'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'and',
    'or', 'but', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
    'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
    'what', 'which', 'who', 'whom', 'where', 'when', 'why', 'how',
    'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
    'some', 'such', 'no', 'not', 'only', 'own', 'same', 'so', 'than',
    'too', 'very', 'just', 'also', 'now', 'here', 'there', 'then',
    'from', 'by', 'into', 'through', 'during', 'before', 'after',
    'me', 'my', 'your', 'his', 'her', 'its', 'our', 'their',
})

STOPWORD_POS = {
    'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
    'this': 'determiner', 'that': 'determiner',
    'these': 'determiner', 'those': 'determiner',
    'some': 'determiner', 'any': 'determiner',
    'each': 'determiner', 'every': 'determiner',
    'all': 'determiner', 'both': 'determiner',
    'no': 'determiner', 'few': 'determiner',
    'more': 'determiner', 'most': 'determiner',
    'other': 'determiner', 'such': 'determiner',
    'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
    'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
    'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
    'into': 'preposition', 'through': 'preposition',
    'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
    'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
    'above': 'preposition', 'below': 'preposition',
    'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
    'so': 'conjunction', 'than': 'conjunction',
    'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
    'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
    'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
    'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
    'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
    'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
    'own': 'determiner',
    'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
    'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
    'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
    'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
    'will': 'modal', 'would': 'modal', 'could': 'modal',
    'should': 'modal', 'may': 'modal', 'might': 'modal',
    'must': 'modal', 'can': 'modal',
    'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
    'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
    'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
    'only': 'adverb', 'not': 'adverb', 'same': 'adverb',
    'once': 'adverb',
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_location_category(category: str) -> bool:
    if not category:
        return False
    cat_lower = category.lower().replace(' ', '_')
    return (
        cat_lower in LOCATION_CATEGORIES or
        'city' in cat_lower or
        'state' in cat_lower or
        'continent' in cat_lower
    )


def parse_pos(pos_value: Any) -> str:
    if not pos_value:
        return 'unknown'
    if isinstance(pos_value, str):
        if pos_value.startswith('['):
            try:
                parsed = json.loads(pos_value.replace("'", '"'))
                return parsed[0] if parsed else 'unknown'
            except (json.JSONDecodeError, IndexError):
                return pos_value.strip("[]'\"")
        return pos_value
    if isinstance(pos_value, list):
        return pos_value[0] if pos_value else 'unknown'
    return str(pos_value)


def get_ngram_type(term: str, entity_type: str = None) -> str:
    if entity_type and entity_type in ('unigram', 'bigram', 'trigram', 'quadgram', 'ngram'):
        return entity_type
    word_count = len(term.split())
    if word_count == 1:
        return 'unigram'
    elif word_count == 2:
        return 'bigram'
    elif word_count == 3:
        return 'trigram'
    elif word_count == 4:
        return 'quadgram'
    else:
        return 'ngram'


# =============================================================================
# VOCABULARY CACHE CLASS
# =============================================================================

@dataclass
class VocabularyCache:
    """In-memory cache for O(1) vocabulary lookups."""

    # Core data structures
    cities: Set[str] = field(default_factory=set)
    states: Set[str] = field(default_factory=set)
    locations: Set[str] = field(default_factory=set)

    # N-gram storage — BEST match per term (highest rank)
    unigrams: Dict[str, Dict] = field(default_factory=dict)
    bigrams: Dict[str, Dict] = field(default_factory=dict)
    trigrams: Dict[str, Dict] = field(default_factory=dict)
    quadgrams: Dict[str, Dict] = field(default_factory=dict)
    ngrams: Dict[str, Dict] = field(default_factory=dict)

    # MULTI-MATCH storage — ALL matches per term, sorted by rank desc
    unigram_matches: Dict[str, List[Dict]] = field(default_factory=dict)
    bigram_matches: Dict[str, List[Dict]] = field(default_factory=dict)
    trigram_matches: Dict[str, List[Dict]] = field(default_factory=dict)
    quadgram_matches: Dict[str, List[Dict]] = field(default_factory=dict)
    ngram_matches: Dict[str, List[Dict]] = field(default_factory=dict)

    # Legacy alias
    @property
    def terms(self) -> Dict[str, Dict]:
        return self.unigrams

    @property
    def term_matches(self) -> Dict[str, List[Dict]]:
        return self.unigram_matches

    stopwords: Set[str] = field(default_factory=lambda: set(DEFAULT_STOPWORDS))

    # Metadata
    loaded: bool = False
    load_time: float = 0.0
    term_count: int = 0
    load_source: str = ""
    last_updated: str = ""

    # Thread safety
    _lock: Lock = field(default_factory=Lock)

    # Raw data for saving
    _raw_data: Dict[str, Dict] = field(default_factory=dict)

    # -------------------------------------------------------------------------
    # FAST LOOKUP METHODS (O(1))
    # -------------------------------------------------------------------------

    def is_city(self, term: str) -> bool:
        return term.lower() in self.cities

    def is_state(self, term: str) -> bool:
        return term.lower() in self.states

    def is_location(self, term: str) -> bool:
        return term.lower() in self.locations

    def is_stopword(self, term: str) -> bool:
        return term.lower() in self.stopwords

    def is_bigram(self, word1: str, word2: str) -> bool:
        key = f"{word1.lower()} {word2.lower()}"
        return key in self.bigrams

    def is_trigram(self, word1: str, word2: str, word3: str) -> bool:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        return key in self.trigrams

    def is_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> bool:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
        return key in self.quadgrams

    def has_term(self, term: str) -> bool:
        term_lower = term.lower()
        return (
            term_lower in self.unigrams or
            term_lower in self.bigrams or
            term_lower in self.trigrams or
            term_lower in self.quadgrams or
            term_lower in self.ngrams
        )

    def get_term(self, term: str) -> Optional[Dict]:
        """Get BEST match (highest rank). O(1)"""
        term_lower = term.lower()
        return (
            self.unigrams.get(term_lower) or
            self.bigrams.get(term_lower) or
            self.trigrams.get(term_lower) or
            self.quadgrams.get(term_lower) or
            self.ngrams.get(term_lower)
        )

    def get_all_term_matches(self, term: str) -> List[Dict]:
        """
        Get ALL category matches for a term, sorted by rank desc. O(1)

        Example: get_all_term_matches("africa") returns:
            [
                {"term": "africa", "category": "Country", "rank": 50000, ...},
                {"term": "africa", "category": "Continent", "rank": 792, ...},
                {"term": "africa", "category": "US City", "rank": 0, ...},
            ]
        """
        term_lower = term.lower()
        matches = (
            self.unigram_matches.get(term_lower) or
            self.bigram_matches.get(term_lower) or
            self.trigram_matches.get(term_lower) or
            self.quadgram_matches.get(term_lower) or
            self.ngram_matches.get(term_lower)
        )
        if matches:
            return matches
        single = self.get_term(term_lower)
        return [single] if single else []

    def get_all_bigram_matches(self, word1: str, word2: str) -> List[Dict]:
        key = f"{word1.lower()} {word2.lower()}"
        matches = self.bigram_matches.get(key)
        if matches:
            return matches
        single = self.bigrams.get(key)
        return [single] if single else []

    def get_all_trigram_matches(self, word1: str, word2: str, word3: str) -> List[Dict]:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        matches = self.trigram_matches.get(key)
        if matches:
            return matches
        single = self.trigrams.get(key)
        return [single] if single else []

    def get_all_quadgram_matches(self, word1: str, word2: str, word3: str, word4: str) -> List[Dict]:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
        matches = self.quadgram_matches.get(key)
        if matches:
            return matches
        single = self.quadgrams.get(key)
        return [single] if single else []

    def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
        key = f"{word1.lower()} {word2.lower()}"
        return self.bigrams.get(key)

    def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        return self.trigrams.get(key)

    def get_quadgram(self, word1: str, word2: str, word3: str, word4: str) -> Optional[Dict]:
        key = f"{word1.lower()} {word2.lower()} {word3.lower()} {word4.lower()}"
        return self.quadgrams.get(key)

    # -------------------------------------------------------------------------
    # INTERNAL: Store term in appropriate dict
    # -------------------------------------------------------------------------

    def _store_term(self, term: str, metadata: Dict, category: str = None, entity_type: str = None) -> str:
        """
        Store a term in the appropriate n-gram dictionary.

        - ALL matches go in the *_matches dict (preserves every category)
        - BEST match (highest rank) goes in the primary dict
        """
        term_lower = term.lower()
        category = category or metadata.get('category', '')
        entity_type = entity_type or metadata.get('entity_type', '')

        ngram_type = get_ngram_type(term_lower, entity_type)

        try:
            new_rank = int(float(metadata.get('rank', 0)))
        except (ValueError, TypeError):
            new_rank = 0

        # Select correct dicts
        if ngram_type == 'unigram':
            primary_dict = self.unigrams
            matches_dict = self.unigram_matches
        elif ngram_type == 'bigram':
            primary_dict = self.bigrams
            matches_dict = self.bigram_matches
        elif ngram_type == 'trigram':
            primary_dict = self.trigrams
            matches_dict = self.trigram_matches
        elif ngram_type == 'quadgram':
            primary_dict = self.quadgrams
            matches_dict = self.quadgram_matches
        else:
            primary_dict = self.ngrams
            matches_dict = self.ngram_matches

        # --- Store ALL matches ---
        if term_lower not in matches_dict:
            matches_dict[term_lower] = []

        existing_categories = {m.get('category', '').lower() for m in matches_dict[term_lower]}
        if category.lower() not in existing_categories:
            matches_dict[term_lower].append(metadata)
            matches_dict[term_lower].sort(
                key=lambda m: int(float(m.get('rank', 0))) if m.get('rank') else 0,
                reverse=True
            )

        # --- Store BEST match (highest rank) ---
        existing = primary_dict.get(term_lower)
        if existing is None:
            primary_dict[term_lower] = metadata
        else:
            try:
                existing_rank = int(float(existing.get('rank', 0)))
            except (ValueError, TypeError):
                existing_rank = 0
            if new_rank > existing_rank:
                primary_dict[term_lower] = metadata

        # Handle location categories
        category_lower = category.lower().replace(' ', '_')
        if category_lower in ('us_city', 'us city', 'city'):
            self.cities.add(term_lower)
            self.locations.add(term_lower)
        elif category_lower in ('us_state', 'us state', 'state'):
            self.states.add(term_lower)
            self.locations.add(term_lower)
        elif is_location_category(category):
            self.locations.add(term_lower)

        return ngram_type

    # -------------------------------------------------------------------------
    # QUERY CLASSIFICATION
    # -------------------------------------------------------------------------

    def get_stopword_pos(self, word: str) -> str:
        return STOPWORD_POS.get(word.lower(), 'unknown')

    def classify_query(self, query: str) -> Dict[str, Any]:
        words = query.lower().split()

        result = {
            'locations': [],
            'quadgrams': [],
            'trigrams': [],
            'bigrams': [],
            'terms': {},
            'unknown': [],
            'stopwords': [],
        }

        used_indices = set()

        # Pass 1: Quadgrams
        for i in range(len(words) - 3):
            if any(idx in used_indices for idx in [i, i+1, i+2, i+3]):
                continue
            quad_key = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]}"
            if quad_key in self.quadgrams:
                result['quadgrams'].append(quad_key)
                metadata = self.quadgrams[quad_key]
                result['terms'][quad_key] = metadata
                if is_location_category(metadata.get('category', '')):
                    result['locations'].append(quad_key)
                used_indices.update([i, i+1, i+2, i+3])

        # Pass 2: Trigrams
        for i in range(len(words) - 2):
            if any(idx in used_indices for idx in [i, i+1, i+2]):
                continue
            trigram_key = f"{words[i]} {words[i+1]} {words[i+2]}"
            if trigram_key in self.trigrams:
                result['trigrams'].append(trigram_key)
                metadata = self.trigrams[trigram_key]
                result['terms'][trigram_key] = metadata
                if is_location_category(metadata.get('category', '')):
                    result['locations'].append(trigram_key)
                used_indices.update([i, i+1, i+2])

        # Pass 3: Bigrams
        for i in range(len(words) - 1):
            if i in used_indices or i+1 in used_indices:
                continue
            bigram_key = f"{words[i]} {words[i+1]}"
            if bigram_key in self.bigrams:
                result['bigrams'].append(bigram_key)
                metadata = self.bigrams[bigram_key]
                result['terms'][bigram_key] = metadata
                if is_location_category(metadata.get('category', '')):
                    result['locations'].append(bigram_key)
                used_indices.update([i, i+1])

        # Pass 4: Unigrams + stopwords
        for i, word in enumerate(words):
            if i in used_indices:
                continue

            if word in self.unigrams:
                metadata = self.unigrams[word]
                result['terms'][word] = metadata
                if word in self.locations:
                    result['locations'].append(word)
                if word in self.stopwords:
                    result['stopwords'].append(word)

            elif word in self.stopwords:
                result['stopwords'].append(word)
                result['terms'][word] = {
                    'term': word,
                    'category': 'Dictionary Word',
                    'pos': self.get_stopword_pos(word),
                    'entity_type': 'stopword',
                    'is_stopword': True,
                }

            else:
                result['unknown'].append(word)

        return result

    # -------------------------------------------------------------------------
    # LOADING METHODS
    # -------------------------------------------------------------------------

    def load_from_dict(self, data: Dict[str, Dict], source: str = "api") -> bool:
        with self._lock:
            start_time = time.perf_counter()
            logger.info(f"Loading vocabulary cache from {source}...")

            # Clear ALL existing data
            self.cities.clear()
            self.states.clear()
            self.locations.clear()
            self.unigrams.clear()
            self.bigrams.clear()
            self.trigrams.clear()
            self.quadgrams.clear()
            self.ngrams.clear()
            self.unigram_matches.clear()
            self.bigram_matches.clear()
            self.trigram_matches.clear()
            self.quadgram_matches.clear()
            self.ngram_matches.clear()
            self._raw_data.clear()

            counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}
            city_count = 0
            state_count = 0

            for key, metadata in data.items():
                try:
                    self._raw_data[key] = metadata

                    parts = key.split(':')
                    if len(parts) >= 2:
                        term = parts[1].lower()
                    else:
                        term = metadata.get('term', '').lower()

                    if not term:
                        continue

                    category = metadata.get('category', '')
                    entity_type = metadata.get('entity_type', '')

                    ngram_type = self._store_term(term, metadata, category, entity_type)
                    counts[ngram_type] = counts.get(ngram_type, 0) + 1

                    category_lower = category.lower().replace(' ', '_')
                    if category_lower in ('us_city', 'us city', 'city'):
                        city_count += 1
                    elif category_lower in ('us_state', 'us state', 'state'):
                        state_count += 1

                except Exception as e:
                    logger.warning(f"Error processing key '{key}': {e}")
                    continue

            # Count multi-match terms
            multi_match_count = sum(
                1 for matches in self.unigram_matches.values() if len(matches) > 1
            ) + sum(
                1 for matches in self.bigram_matches.values() if len(matches) > 1
            ) + sum(
                1 for matches in self.trigram_matches.values() if len(matches) > 1
            )

            self.term_count = len(self._raw_data)
            self.load_time = time.perf_counter() - start_time
            self.load_source = source
            self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.loaded = True

            logger.info(
                f"Cache loaded: {self.term_count:,} total, "
                f"{counts['unigram']:,} unigrams, {counts['bigram']:,} bigrams, "
                f"{counts['trigram']:,} trigrams, {counts['quadgram']:,} quadgrams, "
                f"{counts['ngram']:,} ngrams, "
                f"{city_count:,} cities, {state_count:,} states, "
                f"{multi_match_count:,} multi-category terms "
                f"in {self.load_time:.2f}s"
            )

            self._save_to_file(self._raw_data)
            return True

    def add_terms(self, data: Dict[str, Dict]) -> Dict[str, int]:
        if not self.loaded:
            logger.info("Cache not loaded — loading from file before adding terms...")
            self.load_from_file()

        with self._lock:
            added = 0
            skipped = 0
            counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}

            for key, metadata in data.items():
                if key in self._raw_data:
                    skipped += 1
                    continue

                try:
                    self._raw_data[key] = metadata

                    parts = key.split(':')
                    if len(parts) >= 2:
                        term = parts[1].lower()
                    else:
                        term = metadata.get('term', '').lower()

                    if not term:
                        continue

                    category = metadata.get('category', '')
                    entity_type = metadata.get('entity_type', '')

                    ngram_type = self._store_term(term, metadata, category, entity_type)
                    counts[ngram_type] = counts.get(ngram_type, 0) + 1
                    added += 1

                except Exception as e:
                    logger.warning(f"Error processing key '{key}': {e}")
                    continue

            self.term_count = len(self._raw_data)
            self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._save_to_file(self._raw_data)

            logger.info(
                f"Added {added:,} terms (skipped {skipped:,}): "
                f"{counts['unigram']} unigrams, {counts['bigram']} bigrams, "
                f"{counts['trigram']} trigrams, {counts['quadgram']} quadgrams"
            )

            return {
                'added': added,
                'skipped': skipped,
                'unigrams_added': counts['unigram'],
                'bigrams_added': counts['bigram'],
                'trigrams_added': counts['trigram'],
                'quadgrams_added': counts['quadgram'],
                'ngrams_added': counts['ngram'],
            }

    def load_from_file(self) -> bool:
        if not CACHE_FILE.exists():
            logger.info(f"No cache file found at {CACHE_FILE}")
            return False

        try:
            logger.info(f"Loading vocabulary cache from {CACHE_FILE}...")
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return self.load_from_dict(data, source="file")
        except Exception as e:
            logger.error(f"Failed to load from file: {e}")
            return False

    def _save_to_file(self, data: Dict) -> bool:
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logger.info(f"Cache saved to {CACHE_FILE} ({len(data):,} terms)")
            return True
        except Exception as e:
            logger.error(f"Failed to save cache file: {e}")
            return False

    def load(self) -> bool:
        if self.loaded:
            return True
        return self.load_from_file()

    def reload_from_file(self) -> bool:
        with self._lock:
            self.loaded = False
        return self.load_from_file()

    # -------------------------------------------------------------------------
    # STATUS
    # -------------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        multi_match_count = sum(
            1 for matches in self.unigram_matches.values() if len(matches) > 1
        ) + sum(
            1 for matches in self.bigram_matches.values() if len(matches) > 1
        ) + sum(
            1 for matches in self.trigram_matches.values() if len(matches) > 1
        )

        return {
            'loaded': self.loaded,
            'term_count': self.term_count,
            'unigrams': len(self.unigrams),
            'bigrams': len(self.bigrams),
            'trigrams': len(self.trigrams),
            'quadgrams': len(self.quadgrams),
            'ngrams': len(self.ngrams),
            'multi_category_terms': multi_match_count,
            'cities': len(self.cities),
            'states': len(self.states),
            'locations': len(self.locations),
            'load_time': f"{self.load_time:.2f}s",
            'load_source': self.load_source,
            'last_updated': self.last_updated,
        }

    def __repr__(self) -> str:
        return (
            f"VocabularyCache(loaded={self.loaded}, "
            f"terms={self.term_count:,}, "
            f"unigrams={len(self.unigrams):,}, "
            f"bigrams={len(self.bigrams):,}, "
            f"trigrams={len(self.trigrams):,}, "
            f"quadgrams={len(self.quadgrams):,})"
        )


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

vocab_cache = VocabularyCache()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def is_city(term: str) -> bool:
    return vocab_cache.is_city(term)

def is_state(term: str) -> bool:
    return vocab_cache.is_state(term)

def is_location(term: str) -> bool:
    return vocab_cache.is_location(term)

def get_term(term: str) -> Optional[Dict]:
    return vocab_cache.get_term(term)

def get_all_term_matches(term: str) -> List[Dict]:
    return vocab_cache.get_all_term_matches(term)

def classify_query(query: str) -> Dict[str, Any]:
    return vocab_cache.classify_query(query)

def ensure_loaded() -> bool:
    if not vocab_cache.loaded:
        return vocab_cache.load()
    return True

def get_cache_status() -> Dict[str, Any]:
    return vocab_cache.status()

def reload_cache_from_dict(data: Dict) -> bool:
    return vocab_cache.load_from_dict(data, source="api")

def reload_cache_from_file() -> bool:
    return vocab_cache.reload_from_file()

def add_terms_to_cache(data: Dict) -> Dict[str, int]:
    return vocab_cache.add_terms(data)


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Vocabulary Cache")
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--load', action='store_true', help='Load from file')
    parser.add_argument('--test', action='store_true', help='Test lookups')
    parser.add_argument('--query', type=str, help='Classify a query')
    parser.add_argument('--term', type=str, help='Get all matches for a term')

    args = parser.parse_args()

    if args.load:
        vocab_cache.load_from_file()

    if args.status:
        print(json.dumps(vocab_cache.status(), indent=2))

    if args.test:
        vocab_cache.load()
        tests = ["atlanta", "georgia", "africa", "new york", "xyz123"]
        for t in tests:
            matches = vocab_cache.get_all_term_matches(t)
            print(f"\n{t}: {len(matches)} matches")
            for m in matches:
                print(f"  - {m.get('category')}: rank={m.get('rank')}")

    if args.query:
        vocab_cache.load()
        result = vocab_cache.classify_query(args.query)
        print(json.dumps(result, indent=2))

    if args.term:
        vocab_cache.load()
        matches = vocab_cache.get_all_term_matches(args.term)
        print(f"Matches for '{args.term}': {len(matches)}")
        for m in matches:
            print(json.dumps(m, indent=2))

def add_terms_nosave(self, data: Dict[str, Dict]) -> Dict[str, int]:
        """Same as add_terms but does NOT save to file after each call."""
        if not self.loaded:
            logger.info("Cache not loaded — loading from file before adding terms...")
            self.load_from_file()

        with self._lock:
            added = 0
            skipped = 0
            counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}

            for key, metadata in data.items():
                if key in self._raw_data:
                    skipped += 1
                    continue

                try:
                    self._raw_data[key] = metadata
                    parts = key.split(':')
                    if len(parts) >= 2:
                        term = parts[1].lower()
                    else:
                        term = metadata.get('term', '').lower()
                    if not term:
                        continue

                    category = metadata.get('category', '')
                    entity_type = metadata.get('entity_type', '')
                    ngram_type = self._store_term(term, metadata, category, entity_type)
                    counts[ngram_type] = counts.get(ngram_type, 0) + 1
                    added += 1

                except Exception as e:
                    logger.warning(f"Error processing key '{key}': {e}")
                    continue

            self.term_count = len(self._raw_data)
            self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            logger.info(
                f"Added {added:,} terms (skipped {skipped:,}) [NO SAVE]: "
                f"{counts['unigram']} unigrams, {counts['bigram']} bigrams, "
                f"{counts['trigram']} trigrams, {counts['quadgram']} quadgrams"
            )

            return {
                'added': added,
                'skipped': skipped,
                'unigrams_added': counts['unigram'],
                'bigrams_added': counts['bigram'],
                'trigrams_added': counts['trigram'],
                'quadgrams_added': counts['quadgram'],
                'ngrams_added': counts['ngram'],
            }

def load_from_dict_nosave(self, data: Dict[str, Dict], source: str = "api") -> bool:
        """Same as load_from_dict but does NOT save to file."""
        with self._lock:
            start_time = time.perf_counter()
            logger.info(f"Loading vocabulary cache from {source} (no save)...")

            self.cities.clear()
            self.states.clear()
            self.locations.clear()
            self.unigrams.clear()
            self.bigrams.clear()
            self.trigrams.clear()
            self.quadgrams.clear()
            self.ngrams.clear()
            self.unigram_matches.clear()
            self.bigram_matches.clear()
            self.trigram_matches.clear()
            self.quadgram_matches.clear()
            self.ngram_matches.clear()
            self._raw_data.clear()

            counts = {'unigram': 0, 'bigram': 0, 'trigram': 0, 'quadgram': 0, 'ngram': 0}

            for key, metadata in data.items():
                try:
                    self._raw_data[key] = metadata
                    parts = key.split(':')
                    if len(parts) >= 2:
                        term = parts[1].lower()
                    else:
                        term = metadata.get('term', '').lower()
                    if not term:
                        continue
                    category = metadata.get('category', '')
                    entity_type = metadata.get('entity_type', '')
                    ngram_type = self._store_term(term, metadata, category, entity_type)
                    counts[ngram_type] = counts.get(ngram_type, 0) + 1
                except Exception as e:
                    logger.warning(f"Error processing key '{key}': {e}")
                    continue

            self.term_count = len(self._raw_data)
            self.load_time = time.perf_counter() - start_time
            self.load_source = source
            self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.loaded = True

            logger.info(f"Cache loaded (no save): {self.term_count:,} terms in {self.load_time:.2f}s")
            return True

def save(self) -> bool:
        """Explicitly save current cache data to file."""
        with self._lock:
            return self._save_to_file(self._raw_data)
        

def add_terms_nosave(data: Dict) -> Dict[str, int]:
    return vocab_cache.add_terms_nosave(data)

def reload_cache_nosave(data: Dict) -> bool:
    return vocab_cache.load_from_dict_nosave(data, source="api")

def save_cache() -> bool:
    return vocab_cache.save()