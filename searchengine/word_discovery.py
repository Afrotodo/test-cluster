
"""
word_discovery.py
Five-pass word validation, POS prediction, correction, and filter extraction.

WORKFLOW:
    Pass 1: Validate words against RAM cache
    Pass 2: Grammar pattern detection - predict POS for unknowns
    Pass 3: Spelling correction (Redis) - filtered by predicted POS, ranked by distance + score
    Pass 4: Bigram/Trigram detection (RAM)
    Pass 5: Extract filter instructions for Typesense

PERFORMANCE:
    - Pass 1, 2, 4, 5: RAM only (~0.01ms per word)
    - Pass 3: Redis only for UNKNOWN words (~50ms per correction)
    - If all words valid: ~1ms total
    - If 1-2 typos: ~50-100ms total
"""

import json
import logging
import time
from typing import Dict, Any, List, Tuple, Optional, Set
from .intent_detect import detect_intent


try:
    from .vocabulary_cache import vocab_cache, ensure_loaded
    CACHE_AVAILABLE = True
except ImportError:
    try:
        from vocabulary_cache import vocab_cache, ensure_loaded
        CACHE_AVAILABLE = True
    except ImportError:
        CACHE_AVAILABLE = False
        vocab_cache = None

try:
    from .intent_detect import detect_intent, print_intent_debug
except ImportError:
    from intent_detect import detect_intent, print_intent_debug

# Import Redis functions (ONLY for spelling correction)
try:
    from .searchapi import (
        RedisLookupTable,
        get_fuzzy_matches,
        get_suggestions,
        damerau_levenshtein_distance,
    )
    REDIS_AVAILABLE = True
except ImportError:
    try:
        from searchapi import (
            RedisLookupTable,
            get_fuzzy_matches,
            get_suggestions,
            damerau_levenshtein_distance,
        )
        REDIS_AVAILABLE = True
    except ImportError:
        REDIS_AVAILABLE = False
        
        # Fallback distance function
        def damerau_levenshtein_distance(s1: str, s2: str) -> int:
            """Fallback Damerau-Levenshtein distance."""
            len1, len2 = len(s1), len(s2)
            d = [[0] * (len2 + 1) for _ in range(len1 + 1)]
            for i in range(len1 + 1):
                d[i][0] = i
            for j in range(len2 + 1):
                d[0][j] = j
            for i in range(1, len1 + 1):
                for j in range(1, len2 + 1):
                    cost = 0 if s1[i-1] == s2[j-1] else 1
                    d[i][j] = min(d[i-1][j] + 1, d[i][j-1] + 1, d[i-1][j-1] + cost)
                    if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
                        d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
            return d[len1][len2]


# =============================================================================
# LOGGING SETUP
# =============================================================================

logger = logging.getLogger(__name__)
DEBUG_TIMING = False


# =============================================================================
# CONSTANTS
# =============================================================================

ALLOWED_POS = frozenset({
    "pronoun", "noun", "verb", "article", "adjective",
    "preposition", "adverb", "be", "modal", "auxiliary",
    "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
    "quantifier", "numeral", "participle", "gerund",
    "infinitive_marker", "particle", "negation", "conjunction", "interjection"
})

LOCATION_CATEGORIES = frozenset({
    "city", "state", "neighborhood", "region", "country",
    "us_city", "us city", "us_state", "us state", "location"
})

TEMPORAL_TERMS = {
    "first": {"temporal_type": "oldest", "sort_field": "time_period_start", "sort_order": "asc"},
    "oldest": {"temporal_type": "oldest", "sort_field": "time_period_start", "sort_order": "asc"},
    "earliest": {"temporal_type": "oldest", "sort_field": "time_period_start", "sort_order": "asc"},
    "last": {"temporal_type": "newest", "sort_field": "time_period_start", "sort_order": "desc"},
    "latest": {"temporal_type": "newest", "sort_field": "published_date", "sort_order": "desc"},
    "recent": {"temporal_type": "newest", "sort_field": "published_date", "sort_order": "desc"},
    "newest": {"temporal_type": "newest", "sort_field": "published_date", "sort_order": "desc"},
}

# Category to filter field mapping
CATEGORY_TO_FILTER = {
    # Locations
    "us_city": {"field": "location_city", "type": "location"},
    "us city": {"field": "location_city", "type": "location"},
    "city": {"field": "location_city", "type": "location"},
    "us_state": {"field": "location_state", "type": "location"},
    "us state": {"field": "location_state", "type": "location"},
    "state": {"field": "location_state", "type": "location"},
    "country": {"field": "location_country", "type": "location"},
    "location": {"field": "location_state", "type": "location"},
    
    # Keywords/Topics - these become primary_keywords filters
    "education": {"field": "primary_keywords", "type": "keyword"},
    "culture": {"field": "primary_keywords", "type": "keyword"},
    "business": {"field": "primary_keywords", "type": "keyword"},
    "sports": {"field": "primary_keywords", "type": "keyword"},
    "music": {"field": "primary_keywords", "type": "keyword"},
    "art": {"field": "primary_keywords", "type": "keyword"},
    "history": {"field": "primary_keywords", "type": "keyword"},
    "food": {"field": "primary_keywords", "type": "keyword"},
    "fashion": {"field": "primary_keywords", "type": "keyword"},
    "health": {"field": "primary_keywords", "type": "keyword"},
    "technology": {"field": "primary_keywords", "type": "keyword"},
    "religion": {"field": "primary_keywords", "type": "keyword"},
    "politics": {"field": "primary_keywords", "type": "keyword"},
    
    # Entity types
    "person": {"field": "entity_names", "type": "entity"},
    "organization": {"field": "entity_names", "type": "entity"},
}

# Grammar rules for predicting POS of unknown words
# Format: (left_pos, right_pos) -> [(predicted_pos, confidence), ...]
LOCAL_CONTEXT_RULES = {
    # Both neighbors known
    ("determiner", "noun"): [("adjective", 0.95)],
    ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
    ("determiner", "verb"): [("noun", 0.90)],
    ("determiner", "preposition"): [("noun", 0.90)],
    ("article", "noun"): [("adjective", 0.95)],
    ("article", "verb"): [("noun", 0.90)],
    ("article", "preposition"): [("noun", 0.90)],
    ("adjective", "noun"): [("adjective", 0.85)],
    ("adjective", "verb"): [("noun", 0.90)],
    ("adjective", "preposition"): [("noun", 0.90)],
    ("noun", "noun"): [("verb", 0.85)],
    ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
    ("noun", "adverb"): [("verb", 0.90)],
    ("noun", "preposition"): [("verb", 0.85)],
    ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
    ("verb", "adjective"): [("adverb", 0.85)],
    ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
    ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
    ("preposition", "verb"): [("noun", 0.85)],
    ("preposition", "preposition"): [("noun", 0.80)],
    ("preposition", "end"): [("noun", 0.90), ("proper_noun", 0.85)],
    ("pronoun", "noun"): [("verb", 0.90)],
    ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
    ("pronoun", "preposition"): [("verb", 0.90)],
    ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
    ("be", "adjective"): [("adverb", 0.90)],
    ("be", "preposition"): [("noun", 0.85)],
    
    # Only left neighbor known
    ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
    ("article", None): [("noun", 0.85), ("adjective", 0.80)],
    ("adjective", None): [("noun", 0.90)],
    ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
    ("verb", None): [("noun", 0.75), ("adverb", 0.65), ("adjective", 0.60)],
    ("preposition", None): [("noun", 0.85), ("proper_noun", 0.80)],
    ("noun", None): [("verb", 0.80), ("noun", 0.60)],
    ("adverb", None): [("adjective", 0.80), ("verb", 0.75)],
    ("be", None): [("adjective", 0.85), ("noun", 0.75)],
    
    # Only right neighbor known
    (None, "noun"): [("adjective", 0.90), ("determiner", 0.85)],
    (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
    (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75)],
    (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
    (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
    (None, "determiner"): [("verb", 0.85), ("preposition", 0.75)],
    (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
}


# =============================================================================
# CACHE INITIALIZATION
# =============================================================================

def _ensure_cache_loaded() -> bool:
    """Ensure vocabulary cache is loaded."""
    global CACHE_AVAILABLE
    
    if not CACHE_AVAILABLE:
        return False
    
    try:
        if not vocab_cache.loaded:
            logger.info("Loading vocabulary cache...")
            ensure_loaded()
        return vocab_cache.loaded
    except Exception as e:
        logger.error(f"Failed to load vocabulary cache: {e}")
        CACHE_AVAILABLE = False
        return False


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_pos(pos_value: Any) -> str:
    """Normalize POS value from various formats."""
    if pos_value is None:
        return 'unknown'
    
    # Handle JSON string: '["noun"]' -> "noun"
    if isinstance(pos_value, str) and pos_value.startswith('['):
        try:
            pos_value = json.loads(pos_value)
        except json.JSONDecodeError:
            pass
    
    # Handle list: ["noun"] -> "noun"
    if isinstance(pos_value, list):
        pos_value = pos_value[0] if pos_value else 'unknown'
    
    pos_lower = str(pos_value).lower()
    
    # Normalize location types to proper_noun
    if pos_lower in LOCATION_CATEGORIES or 'city' in pos_lower or 'state' in pos_lower:
        return 'proper_noun'
    
    return pos_lower


def is_location_category(category: str) -> bool:
    """Check if category indicates a location."""
    if not category:
        return False
    cat_lower = category.lower().replace(' ', '_')
    return cat_lower in LOCATION_CATEGORIES or 'city' in cat_lower or 'state' in cat_lower


def get_rank_score(metadata: Dict) -> int:
    """Extract rank score from metadata."""
    rank = metadata.get('rank', 0)
    if isinstance(rank, str):
        try:
            return int(float(rank))
        except (ValueError, TypeError):
            return 0
    return int(rank) if rank else 0


# =============================================================================
# NEW: CATEGORY SUMMARY BUILDER
# =============================================================================

def build_category_summary(
    results: List[Dict[str, Any]],
    ngrams: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Build a summary of category types found in the query.
    
    This helps Typesense understand what types of terms are present
    so it can adjust field weights accordingly.
    
    Args:
        results: List of word results from passes 1-3
        ngrams: List of detected ngrams from pass 4
    
    Returns:
        Dict with boolean flags for each category type
    """
    # Track which positions are covered by ngrams
    ngram_positions = set()
    for ngram in ngrams:
        ngram_positions.update(ngram.get('positions', []))
    
    # Category type flags
    has_person = False
    has_location = False
    has_topic = False
    has_song_title = False
    has_media = False
    has_food = False
    has_business = False
    has_culture = False
    has_entity = False
    
    # Location categories
    location_cats = {'city', 'us city', 'us_city', 'state', 'us state', 'us_state', 
                     'country', 'location', 'region', 'neighborhood', 'continent'}
    
    # Topic categories
    topic_cats = {'culture', 'keyword', 'education', 'health', 'politics', 
                  'history', 'religion', 'concept', 'community', 'historical event'}
    
    # Media categories
    media_cats = {'song title', 'song', 'music', 'media', 'entertainment', 
                  'tv show', 'movie', 'album', 'book'}
    
    # Check ngrams first (they take priority)
    for ngram in ngrams:
        metadata = ngram.get('metadata', {})
        category = metadata.get('category', '').lower()
        
        if category in {'person', 'name'}:
            has_person = True
            has_entity = True
        elif category in location_cats:
            has_location = True
        elif category in {'song title', 'song', 'album'}:
            has_song_title = True
            has_media = True
        elif category in media_cats:
            has_media = True
        elif category == 'food':
            has_food = True
            has_topic = True
        elif category in {'business', 'organization'}:
            has_business = True
            has_entity = True
        elif category == 'culture':
            has_culture = True
            has_topic = True
        elif category in topic_cats:
            has_topic = True
    
    # Check individual terms (skip those in ngrams)
    for result in results:
        position = result.get('position', 0)
        
        # Skip if part of an ngram
        if position in ngram_positions:
            continue
        
        # Skip stopwords and unknowns
        if result.get('category') == 'stopword':
            continue
        if result.get('status') == 'unknown':
            continue
        
        category = result.get('category', '').lower()
        
        # Also check metadata category
        if not category:
            metadata = result.get('metadata', {})
            category = metadata.get('category', '').lower()
        
        if category in {'person', 'name'}:
            has_person = True
            has_entity = True
        elif category in location_cats:
            has_location = True
        elif category in {'song title', 'song', 'album'}:
            has_song_title = True
            has_media = True
        elif category in media_cats:
            has_media = True
        elif category == 'food':
            has_food = True
            has_topic = True
        elif category in {'business', 'organization'}:
            has_business = True
            has_entity = True
        elif category == 'culture':
            has_culture = True
            has_topic = True
        elif category in topic_cats:
            has_topic = True
    
    return {
        'has_person': has_person,
        'has_location': has_location,
        'has_topic': has_topic,
        'has_song_title': has_song_title,
        'has_media': has_media,
        'has_food': has_food,
        'has_business': has_business,
        'has_culture': has_culture,
        'has_entity': has_entity
    }


# =============================================================================
# NEW: DEBUG PRINT FUNCTION
# =============================================================================

def print_discovery_debug(output: Dict[str, Any]) -> None:
    """
    Print word discovery output in a readable format for debugging.
    
    Call this after process_query_optimized() to see the enriched data.
    
    Args:
        output: The dict returned by process_query_optimized()
    """
    print("\n" + "=" * 70)
    print("📚 WORD DISCOVERY DEBUG OUTPUT")
    print("=" * 70)
    
    print(f"\n🔍 Query: '{output.get('query', '')}'")
    print(f"✏️  Corrected: '{output.get('corrected_query', '')}'")
    print(f"⏱️  Processing time: {output.get('processing_time_ms', 0):.2f}ms")
    
    # Terms
    print("\n" + "-" * 70)
    print("📝 TERMS (Individual Words)")
    print("-" * 70)
    terms = output.get('terms', [])
    if terms:
        print(f"{'Word':<15} {'Status':<10} {'Category':<20} {'POS':<12} {'Rank':<8}")
        print("-" * 70)
        for term in terms:
            word = term.get('search_word', term.get('word', ''))
            status = term.get('status', 'unknown')
            category = term.get('category', '-')
            if category:
                category = category[:20]
            else:
                category = '-'
            pos = term.get('pos', 'unknown')
            rank = term.get('rank', term.get('score', 0))
            print(f"{word:<15} {status:<10} {category:<20} {pos:<12} {rank:<8}")
    else:
        print("  (no terms)")
    
    # Ngrams
    print("\n" + "-" * 70)
    print("🔗 NGRAMS (Bigrams/Trigrams)")
    print("-" * 70)
    ngrams = output.get('ngrams', [])
    if ngrams:
        print(f"{'Phrase':<25} {'Type':<10} {'Category':<20} {'Rank':<8}")
        print("-" * 70)
        for ngram in ngrams:
            phrase = ngram.get('ngram', '')
            ngram_type = ngram.get('type', 'unknown')
            category = ngram.get('category', '-')
            if category:
                category = category[:20]
            else:
                category = '-'
            rank = ngram.get('rank', 0)
            print(f"{phrase:<25} {ngram_type:<10} {category:<20} {rank:<8}")
    else:
        print("  (no ngrams detected)")
    
    # Category Summary
    print("\n" + "-" * 70)
    print("📊 CATEGORY SUMMARY")
    print("-" * 70)
    summary = output.get('category_summary', {})
    if summary:
        for key, value in summary.items():
            flag = "✅" if value else "❌"
            print(f"  {flag} {key}: {value}")
    else:
        print("  (no category summary)")
    
    # Corrections
    corrections = output.get('corrections', [])
    if corrections:
        print("\n" + "-" * 70)
        print("🔧 CORRECTIONS")
        print("-" * 70)
        for corr in corrections:
            original = corr.get('original', '')
            corrected = corr.get('corrected', '')
            distance = corr.get('distance', 0)
            print(f"  '{original}' → '{corrected}' (distance: {distance})")
    
    # Sort instruction
    sort_info = output.get('sort')
    if sort_info:
        print("\n" + "-" * 70)
        print("📈 SORT INSTRUCTION")
        print("-" * 70)
        print(f"  Field: {sort_info.get('field')}")
        print(f"  Order: {sort_info.get('order')}")
        print(f"  Term: {sort_info.get('term')}")
    
    print("\n" + "=" * 70)


# =============================================================================
# PASS 1: WORD VALIDATION (RAM)
# =============================================================================

def validate_words_ram(words: List[str], verbose: bool = False) -> List[Dict[str, Any]]:
    """
    Pass 1: Validate each word against RAM cache.
    
    Returns list of word results with status (valid/unknown) and metadata.
    """
    if verbose:
        print("\n" + "=" * 60)
        print("PASS 1: WORD VALIDATION (RAM)")
        print("=" * 60)
    
    start_time = time.perf_counter()
    results = []
    
    if not _ensure_cache_loaded():
        logger.warning("Cache not available, all words marked unknown")
        for i, word in enumerate(words):
            results.append({
                'position': i + 1,
                'word': word.lower(),
                'status': 'unknown',
                'pos': 'unknown',
                'score': 0,
                'category': '',
                'metadata': {}
            })
        return results
    
    for i, word in enumerate(words):
        word_lower = word.lower().strip()
        
        # Check RAM cache (O(1) lookup)
        metadata = vocab_cache.get_term(word_lower)
        
        if metadata:
            pos = normalize_pos(metadata.get('pos', 'unknown'))
            score = get_rank_score(metadata)
            category = metadata.get('category', '')
            
            if verbose:
                print(f"  [{i+1}] '{word_lower}' → VALID (pos={pos}, score={score}, category={category})")
            
            results.append({
                'position': i + 1,
                'word': word_lower,
                'status': 'valid',
                'pos': pos,
                'score': score,
                'category': category,
                'metadata': metadata
            })
        
        # Check stopwords (still valid, have POS)
        elif vocab_cache.is_stopword(word_lower):
            pos = vocab_cache.get_stopword_pos(word_lower)
            
            if verbose:
                print(f"  [{i+1}] '{word_lower}' → VALID (stopword, pos={pos})")
            
            results.append({
                'position': i + 1,
                'word': word_lower,
                'status': 'valid',
                'pos': pos,
                'score': 0,
                'category': 'stopword',
                'metadata': {'pos': pos, 'is_stopword': True}
            })
        
        else:
            if verbose:
                print(f"  [{i+1}] '{word_lower}' → UNKNOWN")
            
            results.append({
                'position': i + 1,
                'word': word_lower,
                'status': 'unknown',
                'pos': 'unknown',
                'score': 0,
                'category': '',
                'metadata': {}
            })
    
    elapsed = (time.perf_counter() - start_time) * 1000
    
    if verbose:
        valid_count = sum(1 for r in results if r['status'] == 'valid')
        unknown_count = len(results) - valid_count
        print(f"\n  Completed in {elapsed:.2f}ms | Valid: {valid_count}, Unknown: {unknown_count}")
    
    return results


# =============================================================================
# PASS 2: GRAMMAR PATTERN DETECTION (RAM)
# =============================================================================

def predict_pos_for_unknowns(
    results: List[Dict[str, Any]], 
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """
    Pass 2: Predict POS for unknown words using grammar patterns.
    
    Uses the POS of neighboring words to predict what the unknown should be.
    """
    if verbose:
        print("\n" + "=" * 60)
        print("PASS 2: GRAMMAR PATTERN DETECTION (RAM)")
        print("=" * 60)
    
    start_time = time.perf_counter()
    
    unknowns = [r for r in results if r['status'] == 'unknown']
    
    if not unknowns:
        if verbose:
            print("  No unknown words to process")
        return results
    
    for unknown in unknowns:
        position = unknown['position']
        word = unknown['word']
        
        # Get left neighbor POS
        left_pos = None
        if position > 1:
            left_result = next((r for r in results if r['position'] == position - 1), None)
            if left_result and left_result['status'] == 'valid':
                left_pos = left_result['pos']
        
        # Get right neighbor POS
        right_pos = None
        right_result = next((r for r in results if r['position'] == position + 1), None)
        if right_result and right_result['status'] == 'valid':
            right_pos = right_result['pos']
        elif right_result is None:
            right_pos = 'end'  # End of query
        
        # Predict POS using grammar rules
        predicted_pos = None
        confidence = 0.0
        
        # Try both neighbors
        if left_pos and right_pos:
            key = (left_pos, right_pos)
            if key in LOCAL_CONTEXT_RULES:
                predicted_pos, confidence = LOCAL_CONTEXT_RULES[key][0]
        
        # Try left neighbor only
        if not predicted_pos and left_pos:
            key = (left_pos, None)
            if key in LOCAL_CONTEXT_RULES:
                predicted_pos, confidence = LOCAL_CONTEXT_RULES[key][0]
        
        # Try right neighbor only
        if not predicted_pos and right_pos and right_pos != 'end':
            key = (None, right_pos)
            if key in LOCAL_CONTEXT_RULES:
                predicted_pos, confidence = LOCAL_CONTEXT_RULES[key][0]
        
        # Default to noun if no pattern matches
        if not predicted_pos:
            predicted_pos = 'noun'
            confidence = 0.5
        
        unknown['predicted_pos'] = predicted_pos
        unknown['pos_confidence'] = confidence
        
        if verbose:
            context_str = f"[{left_pos or '???'}] _{word}_ [{right_pos or '???'}]"
            print(f"  '{word}' → Predicted: {predicted_pos} (confidence: {confidence:.0%})")
            print(f"           Context: {context_str}")
    
    elapsed = (time.perf_counter() - start_time) * 1000
    
    if verbose:
        print(f"\n  Completed in {elapsed:.2f}ms | Predictions: {len(unknowns)}")
    
    return results


# =============================================================================
# PASS 3: SPELLING CORRECTION (REDIS)
# =============================================================================

def correct_unknown_words(
    results: List[Dict[str, Any]],
    max_distance: int = 2,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """
    Pass 3: Correct unknown words using Redis fuzzy search.
    
    Enhanced with:
    - Keyboard-aware distance scoring
    - POS-based candidate scoring
    - Bigram/trigram context bonuses
    - Weighted combined scoring
    
    Only processes words marked as 'unknown'.
    """
    if verbose:
        print("\n" + "=" * 60)
        print("PASS 3: SPELLING CORRECTION (REDIS)")
        print("=" * 60)
    
    start_time = time.perf_counter()
    
    unknowns = [r for r in results if r['status'] == 'unknown']
    
    if not unknowns:
        if verbose:
            print("  No unknown words to correct")
        return results
    
    if not REDIS_AVAILABLE:
        if verbose:
            print("  Redis not available, skipping correction")
        return results
    
    # --- Handle adjacent unknown pairs first ---
    adjacent_pairs = find_adjacent_unknown_pairs(results)
    corrected_positions = set()
    
    for pos1, pos2, word1, word2 in adjacent_pairs:
        if pos1 in corrected_positions or pos2 in corrected_positions:
            continue
        
        # Try to correct as a known bigram
        pair_result = correct_pair_as_bigram(word1, word2, vocab_cache, max_combined_distance=4)
        
        if pair_result:
            corrected1, corrected2, bigram_meta, combined_dist = pair_result
            
            if verbose:
                print(f"\n  PAIR CORRECTION: '{word1} {word2}' → '{corrected1} {corrected2}' (dist={combined_dist})")
            
            # Update both results
            for r in results:
                if r['position'] == pos1:
                    r['status'] = 'corrected'
                    r['corrected'] = corrected1
                    r['corrected_pos'] = bigram_meta.get('pos1', 'unknown')
                    r['distance'] = combined_dist
                    r['pair_corrected'] = True
                    corrected_positions.add(pos1)
                
                elif r['position'] == pos2:
                    r['status'] = 'corrected'
                    r['corrected'] = corrected2
                    r['corrected_pos'] = bigram_meta.get('pos2', 'unknown')
                    r['distance'] = combined_dist
                    r['pair_corrected'] = True
                    corrected_positions.add(pos2)
    
    # --- Process remaining unknowns individually ---
    for unknown in unknowns:
        # Skip if already corrected as part of a pair
        if unknown['position'] in corrected_positions:
            continue
        
        word = unknown['word']
        predicted_pos = unknown.get('predicted_pos', 'noun')
        pos_confidence = unknown.get('pos_confidence', 0.5)
        
        if verbose:
            print(f"\n  Correcting '{word}' (expected POS: {predicted_pos}, conf: {pos_confidence:.0%})...")
        
        # --- Get context words for bigram scoring ---
        left_word, right_word = get_context_words(results, unknown['position'])
        
        if verbose and (left_word or right_word):
            print(f"    Context: [{left_word or '???'}] _{word}_ [{right_word or '???'}]")
        
        # Get fuzzy matches from Redis
        suggestion_result = get_suggestions(word, limit=20, max_distance=max_distance)
        candidates = suggestion_result.get('suggestions', [])
        
        if not candidates:
            if verbose:
                print(f"    No candidates found")
            continue
        
        # --- Score each candidate using combined scoring ---
        scored_candidates = []
        
        for candidate in candidates:
            candidate_term = candidate.get('term', '')
            
            # Basic distance check
            edit_dist = damerau_levenshtein_distance(word, candidate_term.lower())
            if edit_dist > max_distance:
                continue
            
            # Score the candidate with all signals
            scored = score_candidate(
                candidate=candidate,
                unknown_word=word,
                predicted_pos=predicted_pos,
                pos_confidence=pos_confidence,
                left_word=left_word,
                right_word=right_word,
                vocab_cache=vocab_cache
            )
            
            scored_candidates.append(scored)
        
        if not scored_candidates:
            if verbose:
                print(f"    No candidates within distance {max_distance}")
            continue
        
        # --- Sort by final_score descending (higher = better) ---
        scored_candidates.sort(key=lambda x: -x.get('final_score', 0))
        
        # Select best candidate
        best = scored_candidates[0]
        
        if verbose:
            print(f"    Candidates ({len(scored_candidates)}):")
            for i, c in enumerate(scored_candidates[:5]):
                marker = "✓" if i == 0 else " "
                term = c.get('term', '')
                dist = c.get('edit_distance', 0)
                kb_dist = c.get('keyboard_distance', 0)
                pos_sc = c.get('pos_score', 0)
                bg_bonus = c.get('bigram_bonus', 0)
                final = c.get('final_score', 0)
                print(f"      {marker} '{term}' → edit={dist}, kb={kb_dist:.1f}, pos={pos_sc:.0f}, bigram={bg_bonus:.0f}, FINAL={final:.1f}")
        
        # Normalize POS from candidate
        best_pos = best.get('pos', 'unknown')
        if isinstance(best_pos, list):
            best_pos = best_pos[0] if best_pos else 'unknown'
        best_pos = str(best_pos).lower()
        
        # Get word score
        best_score = best.get('rank', 0)
        if isinstance(best_score, str):
            best_score = int(float(best_score)) if best_score else 0
        
        # Get category from best candidate
        best_category = best.get('category', '')
        
        # Update the result
        unknown['status'] = 'corrected'
        unknown['corrected'] = best.get('term', '')
        unknown['corrected_pos'] = best_pos
        unknown['corrected_score'] = best_score
        unknown['category'] = best_category  # Update category from correction
        unknown['distance'] = best.get('edit_distance', 0)
        unknown['keyboard_distance'] = best.get('keyboard_distance', 0)
        unknown['pos_score'] = best.get('pos_score', 0)
        unknown['bigram_bonus'] = best.get('bigram_bonus', 0)
        unknown['final_score'] = best.get('final_score', 0)
        unknown['metadata'] = best
        
        if verbose:
            print(f"    Selected: '{word}' → '{best.get('term', '')}' (score={best.get('final_score', 0):.1f})")
    
    elapsed = (time.perf_counter() - start_time) * 1000
    
    if verbose:
        corrected_count = sum(1 for r in results if r['status'] == 'corrected')
        print(f"\n  Completed in {elapsed:.2f}ms | Corrected: {corrected_count}/{len(unknowns)}")
    
    return results


# =============================================================================
# PASS 4: BIGRAM/TRIGRAM DETECTION (RAM)
# =============================================================================

def detect_ngrams(
    results: List[Dict[str, Any]],
    verbose: bool = False
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Pass 4: Detect bigrams and trigrams in the corrected query.
    
    Returns:
        Tuple of (updated_results, ngrams_found)
    """
    if verbose:
        print("\n" + "=" * 60)
        print("PASS 4: BIGRAM/TRIGRAM DETECTION (RAM)")
        print("=" * 60)
    
    start_time = time.perf_counter()
    
    if not _ensure_cache_loaded():
        if verbose:
            print("  Cache not available, skipping ngram detection")
        return results, []
    
    # Build word list (using corrected words where available)
    words = []
    for r in results:
        if r['status'] == 'corrected':
            words.append(r['corrected'].lower())
        else:
            words.append(r['word'].lower())
    
    ngrams_found = []
    positions_used = set()
    
    # Check trigrams first
    for i in range(len(words) - 2):
        if i in positions_used:
            continue
        
        trigram_meta = vocab_cache.get_trigram(words[i], words[i+1], words[i+2])
        
        if trigram_meta:
            trigram_str = f"{words[i]} {words[i+1]} {words[i+2]}"
            
            if verbose:
                category = trigram_meta.get('category', '')
                rank = trigram_meta.get('rank', 0)
                print(f"  TRIGRAM: '{trigram_str}' (category: {category}, rank: {rank})")
            
            ngrams_found.append({
                'type': 'trigram',
                'positions': [i+1, i+2, i+3],
                'words': [words[i], words[i+1], words[i+2]],
                'ngram': trigram_str,
                'metadata': trigram_meta
            })
            positions_used.update([i, i+1, i+2])
    
    # Check bigrams
    for i in range(len(words) - 1):
        if i in positions_used or i+1 in positions_used:
            continue
        
        bigram_meta = vocab_cache.get_bigram(words[i], words[i+1])
        
        if bigram_meta:
            bigram_str = f"{words[i]} {words[i+1]}"
            
            if verbose:
                category = bigram_meta.get('category', '')
                rank = bigram_meta.get('rank', 0)
                print(f"  BIGRAM: '{bigram_str}' (category: {category}, rank: {rank})")
            
            ngrams_found.append({
                'type': 'bigram',
                'positions': [i+1, i+2],
                'words': [words[i], words[i+1]],
                'ngram': bigram_str,
                'metadata': bigram_meta
            })
            positions_used.update([i, i+1])
    
    elapsed = (time.perf_counter() - start_time) * 1000
    
    if verbose:
        if not ngrams_found:
            print("  No bigrams/trigrams found")
        print(f"\n  Completed in {elapsed:.2f}ms | Found: {len(ngrams_found)} ngrams")
    
    return results, ngrams_found


# =============================================================================
# PASS 5: EXTRACT FILTERS (RAM)
# =============================================================================

def extract_filters(
    results: List[Dict[str, Any]],
    ngrams: List[Dict[str, Any]],
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Pass 5: Extract filter instructions for Typesense.
    
    Returns:
        {
            'filters': [{'field': 'primary_keywords', 'value': 'hbcu'}, ...],
            'sort': {'field': 'time_period_start', 'order': 'asc'} or None,
            'locations': [{'field': 'location_state', 'values': ['Georgia', 'GA']}]
        }
    """
    if verbose:
        print("\n" + "=" * 60)
        print("PASS 5: EXTRACT FILTERS (RAM)")
        print("=" * 60)
    
    start_time = time.perf_counter()
    
    filters = []
    locations = []
    sort_instruction = None
    used_positions = set()
    
    # Mark positions used by ngrams
    for ngram in ngrams:
        used_positions.update(ngram['positions'])
    
    # Process ngrams first (they take priority)
    for ngram in ngrams:
        metadata = ngram.get('metadata', {})
        category = metadata.get('category', '').lower()
        ngram_str = ngram['ngram']
        
        # Check if it's a location
        if is_location_category(category):
            display = metadata.get('display', ngram_str.title())
            
            # Get variants (e.g., "New York" -> ["New York", "NY"])
            variants = metadata.get('filter_variants', [display])
            if display not in variants:
                variants.insert(0, display)
            
            filter_field = 'location_city' if 'city' in category else 'location_state'
            
            locations.append({
                'field': filter_field,
                'values': variants,
                'term': ngram_str
            })
            
            if verbose:
                print(f"  '{ngram_str}' → LOCATION: {filter_field}={variants}")
        
        else:
            # It's a keyword/entity filter
            filter_field = metadata.get('filter_field', 'primary_keywords')
            filter_value = metadata.get('filter_value', ngram_str)
            
            filters.append({
                'field': filter_field,
                'value': filter_value,
                'term': ngram_str
            })
            
            if verbose:
                print(f"  '{ngram_str}' → FILTER: {filter_field}={filter_value}")
    
    # Process individual words
    for result in results:
        position = result['position']
        
        # Skip if part of an ngram
        if position in used_positions:
            continue
        
        # Skip stopwords and unknowns
        if result.get('category') == 'stopword':
            continue
        if result['status'] == 'unknown':
            continue
        
        # Get the word (use corrected if available)
        word = result.get('corrected', result['word'])
        metadata = result.get('metadata', {})
        category = metadata.get('category', '').lower()
        
        # Check for temporal terms
        if word in TEMPORAL_TERMS:
            temporal = TEMPORAL_TERMS[word]
            sort_instruction = {
                'field': temporal['sort_field'],
                'order': temporal['sort_order'],
                'term': word
            }
            
            if verbose:
                print(f"  '{word}' → SORT: {temporal['sort_field']} {temporal['sort_order']}")
            continue
        
        # Check if it's a location
        if is_location_category(category):
            display = metadata.get('display', word.title())
            
            # Get state abbreviation if available
            variants = metadata.get('filter_variants', [])
            if not variants:
                variants = [display]
                # Add common abbreviations
                state_abbrevs = {
                    'georgia': 'GA', 'florida': 'FL', 'texas': 'TX',
                    'california': 'CA', 'new york': 'NY', 'alabama': 'AL',
                    'louisiana': 'LA', 'mississippi': 'MS', 'tennessee': 'TN',
                    'north carolina': 'NC', 'south carolina': 'SC', 'virginia': 'VA',
                    'maryland': 'MD', 'ohio': 'OH', 'pennsylvania': 'PA',
                    'michigan': 'MI', 'illinois': 'IL', 'missouri': 'MO',
                    'arkansas': 'AR', 'oklahoma': 'OK', 'kentucky': 'KY',
                    'west virginia': 'WV', 'delaware': 'DE', 'washington': 'WA',
                    'oregon': 'OR', 'colorado': 'CO', 'arizona': 'AZ',
                    'nevada': 'NV', 'utah': 'UT', 'new mexico': 'NM',
                    'massachusetts': 'MA', 'connecticut': 'CT', 'new jersey': 'NJ',
                    'district of columbia': 'DC', 'washington dc': 'DC',
                }
                abbrev = state_abbrevs.get(word.lower())
                if abbrev:
                    variants.append(abbrev)
            
            filter_field = 'location_city' if 'city' in category else 'location_state'
            
            locations.append({
                'field': filter_field,
                'values': variants,
                'term': word
            })
            
            if verbose:
                print(f"  '{word}' → LOCATION: {filter_field}={variants}")
            continue
        
        # Check if it's a keyword/filter term
        filter_field = metadata.get('filter_field')
        filter_value = metadata.get('filter_value', word)
        
        if filter_field:
            filters.append({
                'field': filter_field,
                'value': filter_value,
                'term': word
            })
            
            if verbose:
                print(f"  '{word}' → FILTER: {filter_field}={filter_value}")
        
        # If category maps to a filter field
        elif category and category in CATEGORY_TO_FILTER:
            mapping = CATEGORY_TO_FILTER[category]
            filters.append({
                'field': mapping['field'],
                'value': word,
                'term': word
            })
            
            if verbose:
                print(f"  '{word}' → FILTER: {mapping['field']}={word}")
    
    elapsed = (time.perf_counter() - start_time) * 1000
    
    if verbose:
        print(f"\n  Completed in {elapsed:.2f}ms")
        print(f"  Filters: {len(filters)}, Locations: {len(locations)}, Sort: {sort_instruction is not None}")
    
    return {
        'filters': filters,
        'locations': locations,
        'sort': sort_instruction
    }


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def process_query_optimized(query: str, verbose: bool = False) -> Dict[str, Any]:
    """
    Main entry point: Process query through all five passes.
    
    WORKFLOW:
        Pass 1: Validate words against RAM cache
        Pass 2: Grammar pattern detection - predict POS for unknowns
        Pass 3: Spelling correction (Redis) - filtered by POS, ranked by distance + score
        Pass 4: Bigram/Trigram detection (RAM)
        Pass 5: Extract filter instructions for Typesense
    
    Args:
        query: The search query string
        verbose: Whether to print debug output
    
    Returns:
        Dict with filters, locations, sort, corrections, terms, ngrams, 
        category_summary, and metadata
    """
    print(f"🔴 ENTERED process_query_optimized with query: {query}")
    overall_start = time.perf_counter()
    
    if verbose:
        print("\n" + "=" * 60)
        print(f"PROCESSING QUERY: '{query}'")
        print("=" * 60)
    
    # Handle empty query
    if not query or not query.strip():
        return {
            'query': query or '',
            'corrected_query': '',
            'valid_count': 0,
            'unknown_count': 0,
            'corrections': [],
            'filters': [],
            'locations': [],
            'sort': None,
            'terms': [],
            'ngrams': [],
            'total_score': 0,
            'average_score': 0,
            'max_score': 0,
            'processing_time_ms': 0,
            'category_summary': {
                'has_person': False,
                'has_location': False,
                'has_topic': False,
                'has_song_title': False,
                'has_media': False,
                'has_food': False,
                'has_business': False,
                'has_culture': False,
                'has_entity': False
            }
        }
    
    words = query.split()
    
    # =========================================================================
    # PASS 1: Word Validation (RAM)
    # =========================================================================
    results = validate_words_ram(words, verbose=verbose)
    
    # =========================================================================
    # PASS 2: Grammar Pattern Detection (RAM)
    # =========================================================================
    results = predict_pos_for_unknowns(results, verbose=verbose)
    
    # =========================================================================
    # PASS 3: Spelling Correction (Redis - only for unknowns)
    # =========================================================================
    results = correct_unknown_words(results, verbose=verbose)
    
    # =========================================================================
    # PASS 4: Bigram/Trigram Detection (RAM)
    # =========================================================================
    results, ngrams = detect_ngrams(results, verbose=verbose)
    
    # =========================================================================
    # PASS 5: Extract Filters (RAM)
    # =========================================================================
    filter_result = extract_filters(results, ngrams, verbose=verbose)
    
    # =========================================================================
    # BUILD OUTPUT
    # =========================================================================
    
    # Build corrected query
    corrected_words = []
    for r in results:
        if r['status'] == 'corrected':
            corrected_words.append(r['corrected'])
        else:
            corrected_words.append(r['word'])
    corrected_query = ' '.join(corrected_words)
    
    # Count valid/unknown
    valid_count = sum(1 for r in results if r['status'] in ('valid', 'corrected'))
    unknown_count = sum(1 for r in results if r['status'] == 'unknown')
    
    # Collect corrections
    corrections = []
    for r in results:
        if r['status'] == 'corrected':
            corrections.append({
                'original': r['word'],
                'corrected': r['corrected'],
                'predicted_pos': r.get('predicted_pos', 'unknown'),
                'corrected_pos': r.get('corrected_pos', 'unknown'),
                'distance': r.get('distance', 0),
                'score': r.get('corrected_score', 0),
                'pos_match': r.get('pos_match', False)
            })
    
    # Calculate scores
    scores = [r.get('score', 0) for r in results if r['status'] in ('valid', 'corrected')]
    scores.extend([r.get('corrected_score', 0) for r in results if r['status'] == 'corrected'])
    total_score = sum(scores)
    average_score = total_score / len(scores) if scores else 0
    max_score = max(scores) if scores else 0
    
    # Build terms list for output - ENRICHED with rank and category
    terms = []
    for r in results:
        term_word = r.get('corrected', r['word'])
        
        # Get rank from metadata or corrected_score
        rank = r.get('corrected_score', r.get('score', 0))
        if not rank:
            metadata = r.get('metadata', {})
            rank = metadata.get('rank', 0)
            if isinstance(rank, str):
                rank = int(float(rank)) if rank else 0
        
        # Get category from result or metadata
        category = r.get('category', '')
        if not category:
            metadata = r.get('metadata', {})
            category = metadata.get('category', '')
        
        terms.append({
            'word': r['word'],
            'search_word': term_word,
            'status': r['status'],
            'pos': r.get('corrected_pos', r['pos']),
            'score': r.get('corrected_score', r.get('score', 0)),
            'rank': rank,
            'category': category
        })
    
    # Build enriched ngrams list
    enriched_ngrams = []
    for ngram in ngrams:
        metadata = ngram.get('metadata', {})
        
        # Extract rank
        rank = metadata.get('rank', 0)
        if isinstance(rank, str):
            rank = int(float(rank)) if rank else 0
        
        # Extract category
        category = metadata.get('category', '')
        
        enriched_ngrams.append({
            'type': ngram.get('type', 'unknown'),
            'positions': ngram.get('positions', []),
            'words': ngram.get('words', []),
            'ngram': ngram.get('ngram', ''),
            'category': category,
            'rank': rank,
            'metadata': metadata
        })
    
    # Build category summary
    category_summary = build_category_summary(results, ngrams)
    
    # =========================================================================
    # FINAL OUTPUT
    # =========================================================================
    
    elapsed = (time.perf_counter() - overall_start) * 1000
    
    output = {
        'query': query,
        'corrected_query': corrected_query,
        'valid_count': valid_count,
        'unknown_count': unknown_count,
        'corrections': corrections,
        'filters': filter_result['filters'],
        'locations': filter_result['locations'],
        'sort': filter_result['sort'],
        'ngrams': enriched_ngrams,
        'terms': terms,
        'total_score': total_score,
        'average_score': round(average_score, 2),
        'max_score': max_score,
        'processing_time_ms': round(elapsed, 2),
        'category_summary': category_summary
    }
    
    if verbose:
        print("\n" + "=" * 60)
        print("FINAL OUTPUT")
        print("=" * 60)
        print(f"  Query: '{query}'")
        print(f"  Corrected: '{corrected_query}'")
        print(f"  Valid: {valid_count}, Unknown: {unknown_count}")
        print(f"  Corrections: {len(corrections)}")
        print(f"  Filters: {len(filter_result['filters'])}")
        print(f"  Locations: {len(filter_result['locations'])}")
        print(f"  Sort: {filter_result['sort']}")
        print(f"  Category Summary: {category_summary}")
        print(f"  Total time: {elapsed:.2f}ms")
        print("=" * 60)

  
    output = detect_intent(output)
    print_intent_debug(output)
    return output


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_filters(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get filter instructions from process_query_optimized result."""
    return result.get('filters', [])


def get_locations(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get location filters from process_query_optimized result."""
    return result.get('locations', [])


def get_sort(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get sort instruction from process_query_optimized result."""
    return result.get('sort')


def get_corrections(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get spelling corrections from process_query_optimized result."""
    return result.get('corrections', [])


def get_corrected_query(result: Dict[str, Any]) -> str:
    """Get corrected query string from process_query_optimized result."""
    return result.get('corrected_query', result.get('query', ''))


def get_category_summary(result: Dict[str, Any]) -> Dict[str, bool]:
    """Get category summary from process_query_optimized result."""
    return result.get('category_summary', {})


# =============================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# =============================================================================

def word_discovery_multi(
    query: str,
    redis_client=None,
    prefix: str = "prefix",
    verbose: bool = False
) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
    """
    Legacy function for backwards compatibility.
    
    Returns:
        Tuple of (corrections, tuple_array, corrected_query)
    """
    result = process_query_optimized(query, verbose=verbose)
    
    corrections = result.get('corrections', [])
    corrected_query = result.get('corrected_query', query)
    
    # Build tuple array of (position, POS)
    tuple_array = []
    for i, term in enumerate(result.get('terms', [])):
        pos = term.get('pos', 'unknown')
        tuple_array.append((i + 1, pos))
    
    return corrections, tuple_array, corrected_query


def validate_word(word: str) -> Dict[str, Any]:
    """Validate a single word. Returns dict with is_valid and metadata."""
    if not _ensure_cache_loaded():
        return {'is_valid': False, 'word': word}
    
    word_lower = word.lower().strip()
    metadata = vocab_cache.get_term(word_lower)
    
    if metadata:
        return {
            'is_valid': True,
            'word': word_lower,
            'metadata': metadata
        }
    
    if vocab_cache.is_stopword(word_lower):
        return {
            'is_valid': True,
            'word': word_lower,
            'metadata': {'pos': vocab_cache.get_stopword_pos(word_lower), 'is_stopword': True}
        }
    
    return {'is_valid': False, 'word': word_lower}


def get_term_metadata(term: str) -> Optional[Dict[str, Any]]:
    """Get metadata for a term from RAM cache."""
    if not _ensure_cache_loaded():
        return None
    return vocab_cache.get_term(term.lower().strip())


def check_bigram_exists(word1: str, word2: str) -> Optional[Dict[str, Any]]:
    """Check if two words form a bigram."""
    if not _ensure_cache_loaded():
        return None
    return vocab_cache.get_bigram(word1.lower(), word2.lower())


def extract_locations_from_query(query: str) -> List[Dict[str, Any]]:
    """Extract locations from query using process_query_optimized."""
    result = process_query_optimized(query, verbose=False)
    return result.get('locations', [])


# =============================================================================
# CACHE STATUS
# =============================================================================

def get_cache_status() -> Dict[str, Any]:
    """Get current cache status."""
    if CACHE_AVAILABLE and vocab_cache:
        return {
            'cache_available': True,
            'cache_loaded': vocab_cache.loaded,
            **vocab_cache.status()
        }
    return {
        'cache_available': False,
        'cache_loaded': False,
        'redis_available': REDIS_AVAILABLE
    }


# =============================================================================
# CONSTANTS FOR CORRECTION SCORING
# =============================================================================

# QWERTY keyboard layout - each key maps to its row and column position
KEYBOARD_LAYOUT = {
    # Row 0 (number row)
    '1': (0, 0), '2': (0, 1), '3': (0, 2), '4': (0, 3), '5': (0, 4),
    '6': (0, 5), '7': (0, 6), '8': (0, 7), '9': (0, 8), '0': (0, 9),
    # Row 1
    'q': (1, 0), 'w': (1, 1), 'e': (1, 2), 'r': (1, 3), 't': (1, 4),
    'y': (1, 5), 'u': (1, 6), 'i': (1, 7), 'o': (1, 8), 'p': (1, 9),
    # Row 2
    'a': (2, 0), 's': (2, 1), 'd': (2, 2), 'f': (2, 3), 'g': (2, 4),
    'h': (2, 5), 'j': (2, 6), 'k': (2, 7), 'l': (2, 8),
    # Row 3
    'z': (3, 0), 'x': (3, 1), 'c': (3, 2), 'v': (3, 3), 'b': (3, 4),
    'n': (3, 5), 'm': (3, 6),
}

# Pre-computed keyboard neighbors for fast lookup
KEYBOARD_NEIGHBORS = {
    'q': {'w', 'a', 's'},
    'w': {'q', 'e', 'a', 's', 'd'},
    'e': {'w', 'r', 's', 'd', 'f'},
    'r': {'e', 't', 'd', 'f', 'g'},
    't': {'r', 'y', 'f', 'g', 'h'},
    'y': {'t', 'u', 'g', 'h', 'j'},
    'u': {'y', 'i', 'h', 'j', 'k'},
    'i': {'u', 'o', 'j', 'k', 'l'},
    'o': {'i', 'p', 'k', 'l'},
    'p': {'o', 'l'},
    'a': {'q', 'w', 's', 'z', 'x'},
    's': {'q', 'w', 'e', 'a', 'd', 'z', 'x', 'c'},
    'd': {'w', 'e', 'r', 's', 'f', 'x', 'c', 'v'},
    'f': {'e', 'r', 't', 'd', 'g', 'c', 'v', 'b'},
    'g': {'r', 't', 'y', 'f', 'h', 'v', 'b', 'n'},
    'h': {'t', 'y', 'u', 'g', 'j', 'b', 'n', 'm'},
    'j': {'y', 'u', 'i', 'h', 'k', 'n', 'm'},
    'k': {'u', 'i', 'o', 'j', 'l', 'm'},
    'l': {'i', 'o', 'p', 'k'},
    'z': {'a', 's', 'x'},
    'x': {'a', 's', 'd', 'z', 'c'},
    'c': {'s', 'd', 'f', 'x', 'v'},
    'v': {'d', 'f', 'g', 'c', 'b'},
    'b': {'f', 'g', 'h', 'v', 'n'},
    'n': {'g', 'h', 'j', 'b', 'm'},
    'm': {'h', 'j', 'k', 'n'},
}

# POS compatibility groups
POS_COMPATIBILITY_GROUPS = {
    'noun': {'noun', 'proper_noun'},
    'proper_noun': {'proper_noun', 'noun'},
    'verb': {'verb', 'participle', 'gerund'},
    'participle': {'participle', 'verb', 'adjective'},
    'gerund': {'gerund', 'verb', 'noun'},
    'adjective': {'adjective', 'participle'},
    'adverb': {'adverb'},
    'pronoun': {'pronoun', 'relative_pronoun', 'wh_pronoun'},
    'relative_pronoun': {'relative_pronoun', 'pronoun'},
    'wh_pronoun': {'wh_pronoun', 'pronoun'},
    'determiner': {'determiner', 'article'},
    'article': {'article', 'determiner'},
    'preposition': {'preposition'},
    'conjunction': {'conjunction'},
    'interjection': {'interjection'},
    'be': {'be', 'verb', 'auxiliary'},
    'auxiliary': {'auxiliary', 'verb', 'modal'},
    'modal': {'modal', 'auxiliary', 'verb'},
}

# Tunable weights for correction scoring
CORRECTION_WEIGHTS = {
    'distance': 100,
    'keyboard': 30,
    'pos_match': 80,
    'pos_compatible': 50,
    'bigram': 120,
    'trigram': 150,
    'frequency': 0.01,
}


# =============================================================================
# KEYBOARD DISTANCE FUNCTIONS
# =============================================================================

def keyboard_distance(char1: str, char2: str) -> int:
    """Calculate keyboard distance between two characters."""
    char1 = char1.lower()
    char2 = char2.lower()
    
    if char1 == char2:
        return 0
    
    if char1 in KEYBOARD_NEIGHBORS and char2 in KEYBOARD_NEIGHBORS[char1]:
        return 1
    
    return 2


def keyboard_aware_distance(word1: str, word2: str) -> float:
    """Calculate edit distance with reduced penalty for adjacent-key typos."""
    s1 = word1.lower()
    s2 = word2.lower()
    
    len1, len2 = len(s1), len(s2)
    d = [[0.0] * (len2 + 1) for _ in range(len1 + 1)]
    
    for i in range(len1 + 1):
        d[i][0] = float(i)
    for j in range(len2 + 1):
        d[0][j] = float(j)
    
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if s1[i-1] == s2[j-1]:
                cost = 0.0
            else:
                kb_dist = keyboard_distance(s1[i-1], s2[j-1])
                if kb_dist == 1:
                    cost = 0.5
                else:
                    cost = 1.0
            
            d[i][j] = min(
                d[i-1][j] + 1.0,
                d[i][j-1] + 1.0,
                d[i-1][j-1] + cost
            )
            
            if i > 1 and j > 1 and s1[i-1] == s2[j-2] and s1[i-2] == s2[j-1]:
                d[i][j] = min(d[i][j], d[i-2][j-2] + cost)
    
    return d[len1][len2]


# =============================================================================
# POS SCORING FUNCTIONS
# =============================================================================

def get_pos_compatibility_score(candidate_pos: str, predicted_pos: str) -> float:
    """Check how compatible a candidate POS is with the predicted POS."""
    candidate_pos = candidate_pos.lower()
    predicted_pos = predicted_pos.lower()
    
    if candidate_pos == predicted_pos:
        return 1.0
    
    compatible_set = POS_COMPATIBILITY_GROUPS.get(predicted_pos, {predicted_pos})
    if candidate_pos in compatible_set:
        return 0.8
    
    candidate_compatible = POS_COMPATIBILITY_GROUPS.get(candidate_pos, {candidate_pos})
    if predicted_pos in candidate_compatible:
        return 0.7
    
    return 0.0


def calculate_pos_score(
    candidate_pos: str,
    predicted_pos: str,
    confidence: float
) -> float:
    """Calculate weighted POS match score."""
    compatibility = get_pos_compatibility_score(candidate_pos, predicted_pos)
    
    if compatibility == 1.0:
        return confidence * CORRECTION_WEIGHTS['pos_match']
    elif compatibility > 0:
        return confidence * compatibility * CORRECTION_WEIGHTS['pos_compatible']
    else:
        return -10


# =============================================================================
# BIGRAM-AWARE SCORING FUNCTIONS
# =============================================================================

def get_context_words(
    results: List[Dict[str, Any]],
    position: int
) -> Tuple[Optional[str], Optional[str]]:
    """Extract left and right neighbor words for a given position."""
    left_word = None
    right_word = None
    
    for r in results:
        if r['position'] == position - 1:
            if r['status'] == 'corrected':
                left_word = r.get('corrected', r['word']).lower()
            else:
                left_word = r['word'].lower()
        
        elif r['position'] == position + 1:
            if r['status'] == 'corrected':
                right_word = r.get('corrected', r['word']).lower()
            else:
                right_word = r['word'].lower()
    
    return left_word, right_word


def get_bigram_bonus(
    candidate_term: str,
    left_word: Optional[str],
    right_word: Optional[str],
    vocab_cache
) -> float:
    """Calculate bonus if candidate forms a known bigram with neighbors."""
    bonus = 0.0
    candidate_lower = candidate_term.lower()
    
    if left_word:
        bigram_meta = vocab_cache.get_bigram(left_word, candidate_lower)
        if bigram_meta:
            bonus += CORRECTION_WEIGHTS['bigram']
    
    if right_word:
        bigram_meta = vocab_cache.get_bigram(candidate_lower, right_word)
        if bigram_meta:
            bonus += CORRECTION_WEIGHTS['bigram']
    
    return bonus


def get_trigram_bonus(
    candidate_term: str,
    left_word: Optional[str],
    right_word: Optional[str],
    vocab_cache
) -> float:
    """Calculate bonus if candidate forms a known trigram with neighbors."""
    if not left_word or not right_word:
        return 0.0
    
    candidate_lower = candidate_term.lower()
    
    trigram_meta = vocab_cache.get_trigram(left_word, candidate_lower, right_word)
    if trigram_meta:
        return CORRECTION_WEIGHTS['trigram']
    
    return 0.0


# =============================================================================
# COMBINED SCORING FUNCTION
# =============================================================================

def calculate_correction_score(
    edit_distance: int,
    keyboard_dist: float,
    pos_score: float,
    bigram_bonus: float,
    trigram_bonus: float,
    word_score: int
) -> float:
    """Calculate combined correction score for ranking candidates."""
    score = 0.0
    
    score -= edit_distance * CORRECTION_WEIGHTS['distance']
    score -= keyboard_dist * CORRECTION_WEIGHTS['keyboard']
    
    score += pos_score
    score += bigram_bonus
    score += trigram_bonus
    score += word_score * CORRECTION_WEIGHTS['frequency']
    
    return score


# =============================================================================
# COMPOUND WORD FUNCTIONS
# =============================================================================

def try_split_word(
    unknown_word: str,
    vocab_cache,
    min_part_length: int = 2
) -> List[Tuple[str, str, float]]:
    """Attempt to split a compound word into two valid words."""
    word = unknown_word.lower()
    valid_splits = []
    
    for i in range(min_part_length, len(word) - min_part_length + 1):
        part1 = word[:i]
        part2 = word[i:]
        
        meta1 = vocab_cache.get_term(part1)
        meta2 = vocab_cache.get_term(part2)
        
        if meta1 and meta2:
            score1 = meta1.get('rank', 0)
            score2 = meta2.get('rank', 0)
            if isinstance(score1, str):
                score1 = int(float(score1)) if score1 else 0
            if isinstance(score2, str):
                score2 = int(float(score2)) if score2 else 0
            
            combined_score = score1 + score2
            valid_splits.append((part1, part2, combined_score))
    
    valid_splits.sort(key=lambda x: -x[2])
    
    return valid_splits


def try_merge_words(
    word1: str,
    word2: str,
    vocab_cache
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Attempt to merge two words into a valid compound word."""
    merged = (word1 + word2).lower()
    
    metadata = vocab_cache.get_term(merged)
    if metadata:
        return (merged, metadata)
    
    return None


def check_compound_candidates(
    results: List[Dict[str, Any]],
    vocab_cache,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """Scan results for split/merge opportunities and update accordingly."""
    if verbose:
        print("\n  Checking compound candidates...")
    
    updated_results = []
    skip_next = False
    
    for i, result in enumerate(results):
        if skip_next:
            skip_next = False
            continue
        
        if i < len(results) - 1:
            next_result = results[i + 1]
            
            if result['status'] == 'unknown' or next_result['status'] == 'unknown':
                word1 = result['word']
                word2 = next_result['word']
                
                merge_result = try_merge_words(word1, word2, vocab_cache)
                
                if merge_result:
                    merged_word, metadata = merge_result
                    
                    if verbose:
                        print(f"    MERGE: '{word1}' + '{word2}' → '{merged_word}'")
                    
                    updated_results.append({
                        'position': result['position'],
                        'word': f"{word1} {word2}",
                        'original_words': [word1, word2],
                        'status': 'merged',
                        'corrected': merged_word,
                        'pos': metadata.get('pos', 'unknown'),
                        'score': metadata.get('rank', 0),
                        'category': metadata.get('category', ''),
                        'metadata': metadata
                    })
                    
                    skip_next = True
                    continue
        
        if result['status'] == 'unknown':
            splits = try_split_word(result['word'], vocab_cache)
            
            if splits:
                best_split = splits[0]
                word1, word2, score = best_split
                
                if verbose:
                    print(f"    SPLIT: '{result['word']}' → '{word1}' + '{word2}'")
                
                meta1 = vocab_cache.get_term(word1)
                meta2 = vocab_cache.get_term(word2)
                
                updated_results.append({
                    'position': result['position'],
                    'word': result['word'],
                    'original_word': result['word'],
                    'status': 'split',
                    'corrected': word1,
                    'pos': meta1.get('pos', 'unknown') if meta1 else 'unknown',
                    'score': meta1.get('rank', 0) if meta1 else 0,
                    'category': meta1.get('category', '') if meta1 else '',
                    'metadata': meta1 or {}
                })
                
                updated_results.append({
                    'position': result['position'] + 0.5,
                    'word': '',
                    'original_word': result['word'],
                    'status': 'split',
                    'corrected': word2,
                    'pos': meta2.get('pos', 'unknown') if meta2 else 'unknown',
                    'score': meta2.get('rank', 0) if meta2 else 0,
                    'category': meta2.get('category', '') if meta2 else '',
                    'metadata': meta2 or {}
                })
                
                continue
        
        updated_results.append(result)
    
    for i, r in enumerate(updated_results):
        r['position'] = i + 1
    
    return updated_results


# =============================================================================
# ADJACENT UNKNOWN PAIR FUNCTIONS
# =============================================================================

def find_adjacent_unknown_pairs(
    results: List[Dict[str, Any]]
) -> List[Tuple[int, int, str, str]]:
    """Find pairs of adjacent unknown words."""
    pairs = []
    
    for i in range(len(results) - 1):
        current = results[i]
        next_result = results[i + 1]
        
        if current['status'] == 'unknown' and next_result['status'] == 'unknown':
            pairs.append((
                current['position'],
                next_result['position'],
                current['word'],
                next_result['word']
            ))
    
    return pairs


def correct_pair_as_bigram(
    word1: str,
    word2: str,
    vocab_cache,
    max_combined_distance: int = 4
) -> Optional[Tuple[str, str, Dict[str, Any], float]]:
    """Attempt to jointly correct two unknown words as a known bigram."""
    word1 = word1.lower()
    word2 = word2.lower()
    
    if not hasattr(vocab_cache, 'get_all_bigrams') and not hasattr(vocab_cache, 'bigrams'):
        return _correct_pair_fallback(word1, word2, vocab_cache, max_combined_distance)
    
    best_match = None
    best_distance = float('inf')
    
    bigrams = getattr(vocab_cache, 'bigrams', {})
    
    for bigram_key, bigram_meta in bigrams.items():
        if '|' in bigram_key:
            bg_word1, bg_word2 = bigram_key.split('|', 1)
        elif ' ' in bigram_key:
            parts = bigram_key.split(' ', 1)
            if len(parts) == 2:
                bg_word1, bg_word2 = parts
            else:
                continue
        else:
            continue
        
        dist1 = damerau_levenshtein_distance(word1, bg_word1.lower())
        dist2 = damerau_levenshtein_distance(word2, bg_word2.lower())
        combined_dist = dist1 + dist2
        
        if combined_dist <= max_combined_distance and combined_dist < best_distance:
            best_match = (bg_word1, bg_word2, bigram_meta, combined_dist)
            best_distance = combined_dist
    
    return best_match


def _correct_pair_fallback(
    word1: str,
    word2: str,
    vocab_cache,
    max_combined_distance: int
) -> Optional[Tuple[str, str, Dict[str, Any], float]]:
    """Fallback method when we can't iterate all bigrams."""
    return None


def damerau_levenshtein_distance(s1: str, s2: str) -> int:
    """Standard Damerau-Levenshtein distance."""
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


# =============================================================================
# INTEGRATION HELPER
# =============================================================================

def score_candidate(
    candidate: Dict[str, Any],
    unknown_word: str,
    predicted_pos: str,
    pos_confidence: float,
    left_word: Optional[str],
    right_word: Optional[str],
    vocab_cache
) -> Dict[str, Any]:
    """Score a single candidate - combines all scoring signals."""
    candidate_term = candidate.get('term', '')
    candidate_pos = candidate.get('pos', 'unknown')
    if isinstance(candidate_pos, list):
        candidate_pos = candidate_pos[0] if candidate_pos else 'unknown'
    candidate_pos = str(candidate_pos).lower()
    
    word_score = candidate.get('rank', 0)
    if isinstance(word_score, str):
        word_score = int(float(word_score)) if word_score else 0
    
    edit_dist = damerau_levenshtein_distance(unknown_word.lower(), candidate_term.lower())
    kb_dist = keyboard_aware_distance(unknown_word.lower(), candidate_term.lower())
    pos_score = calculate_pos_score(candidate_pos, predicted_pos, pos_confidence)
    bigram_bonus = get_bigram_bonus(candidate_term, left_word, right_word, vocab_cache)
    trigram_bonus = get_trigram_bonus(candidate_term, left_word, right_word, vocab_cache)
    
    final_score = calculate_correction_score(
        edit_dist,
        kb_dist,
        pos_score,
        bigram_bonus,
        trigram_bonus,
        word_score
    )
    
    candidate['edit_distance'] = edit_dist
    candidate['keyboard_distance'] = kb_dist
    candidate['pos_score'] = pos_score
    candidate['bigram_bonus'] = bigram_bonus
    candidate['trigram_bonus'] = trigram_bonus
    candidate['final_score'] = final_score
    
    return candidate


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("Testing keyboard distance...")
    print(f"  'e' to 'r': {keyboard_distance('e', 'r')} (expected: 1)")
    print(f"  'e' to 'e': {keyboard_distance('e', 'e')} (expected: 0)")
    print(f"  'e' to 'p': {keyboard_distance('e', 'p')} (expected: 2)")
    
    print("\nTesting keyboard-aware distance...")
    print(f"  'bleu' to 'blue': {keyboard_aware_distance('bleu', 'blue')}")
    print(f"  'bleu' to 'blew': {keyboard_aware_distance('bleu', 'blew')}")
    print(f"  'teh' to 'the': {keyboard_aware_distance('teh', 'the')}")
    
    print("\nTesting POS compatibility...")
    print(f"  noun/noun: {get_pos_compatibility_score('noun', 'noun')} (expected: 1.0)")
    print(f"  noun/proper_noun: {get_pos_compatibility_score('noun', 'proper_noun')} (expected: 0.8)")
    print(f"  noun/verb: {get_pos_compatibility_score('noun', 'verb')} (expected: 0.0)")
    
    print("\n✓ All basic tests completed")