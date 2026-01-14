"""
vocabulary_cache.py
In-memory cache for fast term lookups.

Loads vocabulary data from:
1. API endpoint (POST from Colab)
2. Local JSON file (backup/startup)

PERFORMANCE:
    - Redis lookup: ~300ms per term
    - Cache lookup: ~0.001ms per term
    - Speedup: ~200,000x
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, Set, Optional, Any
from threading import Lock
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Categories that indicate a location (lowercase for comparison)
LOCATION_CATEGORIES = frozenset({
    'us_city', 'us city', 'city',
    'us_state', 'us state', 'state',
    'location', 'region', 'country', 'neighborhood',
})

# Cache file path (backup for startup)
CACHE_DIR = Path(__file__).parent / 'cache' / 'data'
CACHE_FILE = CACHE_DIR / 'vocabulary_data.json'

# Stopwords - common words that still need POS tagging for grammar context
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

# POS mapping for stopwords (used when not found in main vocabulary)
STOPWORD_POS = {
    # Determiners / Articles
    'a': 'determiner', 'an': 'determiner', 'the': 'determiner',
    'this': 'determiner', 'that': 'determiner', 
    'these': 'determiner', 'those': 'determiner',
    'some': 'determiner', 'any': 'determiner',
    'each': 'determiner', 'every': 'determiner',
    'all': 'determiner', 'both': 'determiner',
    'no': 'determiner', 'few': 'determiner',
    'more': 'determiner', 'most': 'determiner',
    'other': 'determiner', 'such': 'determiner',
    
    # Prepositions
    'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
    'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
    'from': 'preposition', 'by': 'preposition', 'with': 'preposition',
    'into': 'preposition', 'through': 'preposition',
    'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
    'about': 'preposition', 'between': 'preposition', 'under': 'preposition',
    'above': 'preposition', 'below': 'preposition',
    
    # Conjunctions
    'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
    'so': 'conjunction', 'than': 'conjunction',
    
    # Pronouns
    'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
    'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
    'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun', 'us': 'pronoun', 'them': 'pronoun',
    'what': 'pronoun', 'which': 'pronoun', 'who': 'pronoun', 'whom': 'pronoun',
    
    # Possessive determiners
    'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
    'her': 'determiner', 'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
    'own': 'determiner',
    
    # Be verbs
    'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
    'be': 'be', 'been': 'be', 'being': 'be', 'am': 'be',
    
    # Auxiliary / Modal verbs
    'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
    'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
    'will': 'modal', 'would': 'modal', 'could': 'modal',
    'should': 'modal', 'may': 'modal', 'might': 'modal',
    'must': 'modal', 'can': 'modal',
    
    # Adverbs
    'where': 'adverb', 'when': 'adverb', 'why': 'adverb', 'how': 'adverb',
    'here': 'adverb', 'there': 'adverb', 'then': 'adverb', 'now': 'adverb',
    'too': 'adverb', 'very': 'adverb', 'just': 'adverb', 'also': 'adverb',
    'only': 'adverb', 'not': 'adverb', 'same': 'adverb',
    
    # Quantifiers
    'once': 'adverb',
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def is_location_category(category: str) -> bool:
    """Check if a category indicates a location."""
    if not category:
        return False
    cat_lower = category.lower().replace(' ', '_')
    return (
        cat_lower in LOCATION_CATEGORIES or
        'city' in cat_lower or
        'state' in cat_lower
    )


def parse_pos(pos_value: Any) -> str:
    """Parse POS from various formats like \"['noun']\" or ['noun'] or 'noun'."""
    if not pos_value:
        return 'unknown'
    
    # Handle string that looks like a list: "['noun']"
    if isinstance(pos_value, str):
        if pos_value.startswith('['):
            try:
                parsed = json.loads(pos_value.replace("'", '"'))
                return parsed[0] if parsed else 'unknown'
            except (json.JSONDecodeError, IndexError):
                return pos_value.strip("[]'\"")
        return pos_value
    
    # Handle actual list
    if isinstance(pos_value, list):
        return pos_value[0] if pos_value else 'unknown'
    
    return str(pos_value)


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
    bigrams: Dict[str, Dict] = field(default_factory=dict)
    trigrams: Dict[str, Dict] = field(default_factory=dict)
    terms: Dict[str, Dict] = field(default_factory=dict)
    stopwords: Set[str] = field(default_factory=lambda: set(DEFAULT_STOPWORDS))
    
    # Metadata
    loaded: bool = False
    load_time: float = 0.0
    term_count: int = 0
    load_source: str = ""
    last_updated: str = ""
    
    # Thread safety
    _lock: Lock = field(default_factory=Lock)
    
    # -------------------------------------------------------------------------
    # FAST LOOKUP METHODS (O(1))
    # -------------------------------------------------------------------------
    
    def is_city(self, term: str) -> bool:
        """Check if term is a city. O(1)"""
        return term.lower() in self.cities
    
    def is_state(self, term: str) -> bool:
        """Check if term is a state. O(1)"""
        return term.lower() in self.states
    
    def is_location(self, term: str) -> bool:
        """Check if term is any location type. O(1)"""
        return term.lower() in self.locations
    
    def is_stopword(self, term: str) -> bool:
        """Check if term is a stopword. O(1)"""
        return term.lower() in self.stopwords
    
    def is_bigram(self, word1: str, word2: str) -> bool:
        """Check if two words form a bigram. O(1)"""
        key = f"{word1.lower()} {word2.lower()}"
        return key in self.bigrams
    
    def is_trigram(self, word1: str, word2: str, word3: str) -> bool:
        """Check if three words form a trigram. O(1)"""
        key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        return key in self.trigrams
    
    def has_term(self, term: str) -> bool:
        """Check if term exists. O(1)"""
        return term.lower() in self.terms
    
    def get_term(self, term: str) -> Optional[Dict]:
        """Get term metadata. O(1)"""
        return self.terms.get(term.lower())
    
    def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
        """Get bigram metadata. O(1)"""
        key = f"{word1.lower()} {word2.lower()}"
        return self.bigrams.get(key)
    
    def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
        """Get trigram metadata. O(1)"""
        key = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        return self.trigrams.get(key)
    
    # -------------------------------------------------------------------------
    # QUERY CLASSIFICATION (Single Pass)
    # -------------------------------------------------------------------------
    
    def get_stopword_pos(self, word: str) -> str:
        """Get POS for a stopword."""
        return STOPWORD_POS.get(word.lower(), 'unknown')
    
    def classify_query(self, query: str) -> Dict[str, Any]:
        """
        Classify all terms in a query in a single pass.
        
        Stopwords ARE included with POS information for grammar context.
        
        Returns:
            {
                "locations": ["georgia", "atlanta"],
                "bigrams": ["new york"],
                "trigrams": [],
                "terms": {"georgia": {...}, "the": {...}, ...},
                "unknown": [],
                "stopwords": ["the", "in"],  # Tracked but included in terms
            }
        """
        words = query.lower().split()
        
        result = {
            'locations': [],
            'bigrams': [],
            'trigrams': [],
            'terms': {},
            'unknown': [],
            'stopwords': [],  # Track which words are stopwords (but still include them)
        }
        
        used_indices = set()
        
        # Pass 1: Trigrams
        for i in range(len(words) - 2):
            if i in used_indices:
                continue
            trigram_key = f"{words[i]} {words[i+1]} {words[i+2]}"
            if trigram_key in self.trigrams:
                result['trigrams'].append(trigram_key)
                metadata = self.trigrams[trigram_key]
                result['terms'][trigram_key] = metadata
                if is_location_category(metadata.get('category', '')):
                    result['locations'].append(trigram_key)
                used_indices.update([i, i+1, i+2])
        
        # Pass 2: Bigrams
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
        
        # Pass 3: Individual words (including stopwords)
        for i, word in enumerate(words):
            if i in used_indices:
                continue
            
            # Check if it's in our vocabulary first
            if word in self.terms:
                metadata = self.terms[word]
                result['terms'][word] = metadata
                if word in self.locations:
                    result['locations'].append(word)
                # Also track if it's a stopword
                if word in self.stopwords:
                    result['stopwords'].append(word)
            
            # If it's a stopword but NOT in vocabulary, still include with POS
            elif word in self.stopwords:
                result['stopwords'].append(word)
                result['terms'][word] = {
                    'term': word,
                    'category': 'Dictionary Word',
                    'pos': self.get_stopword_pos(word),
                    'entity_type': 'stopword',
                    'is_stopword': True,
                }
            
            # Unknown word
            else:
                result['unknown'].append(word)
        
        return result
    
    # -------------------------------------------------------------------------
    # LOADING METHODS
    # -------------------------------------------------------------------------
    
    def load_from_dict(self, data: Dict[str, Dict], source: str = "api") -> bool:
        """
        Load vocabulary from a dictionary (from Colab endpoint or JSON file).
        
        Args:
            data: Dict with keys like "term:atlanta:us_city" and value dicts
            source: Where the data came from (for logging)
        
        Returns:
            True if successful
        """
        with self._lock:
            start_time = time.perf_counter()
            logger.info(f"Loading vocabulary cache from {source}...")
            
            # Clear existing data
            self.cities.clear()
            self.states.clear()
            self.locations.clear()
            self.bigrams.clear()
            self.trigrams.clear()
            self.terms.clear()
            
            loaded = 0
            bigram_count = 0
            trigram_count = 0
            city_count = 0
            state_count = 0
            
            for key, metadata in data.items():
                try:
                    # Parse key format: "term:word:category_slug"
                    parts = key.split(':')
                    if len(parts) >= 2:
                        term = parts[1].lower()
                    else:
                        term = metadata.get('term', '').lower()
                    
                    if not term:
                        continue
                    
                    # Get category
                    category = metadata.get('category', '')
                    category_lower = category.lower().replace(' ', '_')
                    
                    # Get entity type
                    entity_type = metadata.get('entity_type', 'unigram')
                    
                    # Store based on entity type
                    if entity_type == 'trigram' or term.count(' ') == 2:
                        self.trigrams[term] = metadata
                        trigram_count += 1
                        if is_location_category(category):
                            self.locations.add(term)
                    
                    elif entity_type == 'bigram' or ' ' in term:
                        self.bigrams[term] = metadata
                        bigram_count += 1
                        if is_location_category(category):
                            self.locations.add(term)
                    
                    else:
                        # Unigram (single word)
                        self.terms[term] = metadata
                        
                        # Categorize
                        if category_lower in ('us_city', 'us city', 'city'):
                            self.cities.add(term)
                            self.locations.add(term)
                            city_count += 1
                        elif category_lower in ('us_state', 'us state', 'state'):
                            self.states.add(term)
                            self.locations.add(term)
                            state_count += 1
                        elif is_location_category(category):
                            self.locations.add(term)
                    
                    loaded += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing key '{key}': {e}")
                    continue
            
            # Update metadata
            self.term_count = loaded
            self.load_time = time.perf_counter() - start_time
            self.load_source = source
            self.last_updated = time.strftime('%Y-%m-%d %H:%M:%S')
            self.loaded = True
            
            logger.info(
                f"Cache loaded: {loaded:,} terms, "
                f"{city_count:,} cities, {state_count:,} states, "
                f"{bigram_count:,} bigrams, {trigram_count:,} trigrams "
                f"in {self.load_time:.2f}s"
            )
            
            # Save to file as backup
            self._save_to_file(data)
            
            return True
    
    def load_from_file(self) -> bool:
        """Load from local JSON file (for startup)."""
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
        """Save data to local file as backup."""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logger.info(f"Cache saved to {CACHE_FILE}")
            return True
        except Exception as e:
            logger.error(f"Failed to save cache file: {e}")
            return False
    
    def load(self) -> bool:
        """Load cache from available source (file or wait for API)."""
        if self.loaded:
            return True
        return self.load_from_file()
    
    def reload_from_file(self) -> bool:
        """Force reload from file."""
        with self._lock:
            self.loaded = False
        return self.load_from_file()
    
    # -------------------------------------------------------------------------
    # STATUS
    # -------------------------------------------------------------------------
    
    def status(self) -> Dict[str, Any]:
        """Get cache status."""
        return {
            'loaded': self.loaded,
            'term_count': self.term_count,
            'cities': len(self.cities),
            'states': len(self.states),
            'locations': len(self.locations),
            'bigrams': len(self.bigrams),
            'trigrams': len(self.trigrams),
            'load_time': f"{self.load_time:.2f}s",
            'load_source': self.load_source,
            'last_updated': self.last_updated,
        }
    
    def __repr__(self) -> str:
        return (
            f"VocabularyCache(loaded={self.loaded}, "
            f"terms={self.term_count:,}, "
            f"cities={len(self.cities):,}, "
            f"states={len(self.states):,}, "
            f"bigrams={len(self.bigrams):,})"
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

def classify_query(query: str) -> Dict[str, Any]:
    return vocab_cache.classify_query(query)

def ensure_loaded() -> bool:
    """Ensure cache is loaded. Called by apps.py at startup."""
    if not vocab_cache.loaded:
        return vocab_cache.load()
    return True

def get_cache_status() -> Dict[str, Any]:
    return vocab_cache.status()

def reload_cache_from_dict(data: Dict) -> bool:
    return vocab_cache.load_from_dict(data, source="api")

def reload_cache_from_file() -> bool:
    return vocab_cache.reload_from_file()


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
    
    args = parser.parse_args()
    
    if args.load:
        vocab_cache.load_from_file()
    
    if args.status:
        print(vocab_cache.status())
    
    if args.test:
        vocab_cache.load()
        tests = ["atlanta", "georgia", "new york", "xyz123"]
        for t in tests:
            print(f"{t}: location={vocab_cache.is_location(t)}, data={vocab_cache.get_term(t)}")
    
    if args.query:
        vocab_cache.load()
        result = vocab_cache.classify_query(args.query)
        print(json.dumps(result, indent=2))