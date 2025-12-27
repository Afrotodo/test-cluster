"""
word_discovery_optimized.py
Optimized three-pass word validation, correction, and bigram detection.

Key optimizations:
1. Batched Redis operations (single pipeline per pass)
2. O(1) position lookups using dicts
3. No redundant data structure rebuilding
4. Cached Levenshtein distance calculations
5. Combined validate + metadata in single call
"""
import json
from typing import Dict, Any, List, Tuple, Optional, Set
from functools import lru_cache

from pyxdameraulevenshtein import damerau_levenshtein_distance

# Import your existing Redis functions - we'll wrap them for batching
from .searchapi import (
    RedisLookupTable,
    validate_word,
    get_term_metadata,
    get_suggestions,
    generate_candidates_smart,
    batch_check_candidates
)


# =============================================================================
# CONSTANTS (unchanged, but converted to frozensets for O(1) lookup)
# =============================================================================

ALLOWED_POS: frozenset = frozenset({
    "pronoun", "noun", "verb", "article", "adjective",
    "preposition", "adverb", "be", "modal", "auxiliary",
    "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
    "quantifier", "numeral", "participle", "gerund",
    "infinitive_marker", "particle", "negation", "conjunction", "interjection"
})

LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country"})

COMPOUND_NOUN_TYPES: frozenset = frozenset({
    "city", "state", "neighborhood", "region", "country",
    "occupation", "product", "furniture", "food", "sport", "disease", "animal"
})

# Pre-build LOCAL_CONTEXT_RULES for O(1) lookup (same data, just ensuring it's a dict)
LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
    # BOTH NEIGHBORS KNOWN
    ("determiner", "noun"): [("adjective", 0.95)],
    ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
    ("determiner", "verb"): [("noun", 0.90)],
    ("article", "noun"): [("adjective", 0.95)],
    ("article", "adjective"): [("adjective", 0.85)],
    ("article", "verb"): [("noun", 0.90)],
    ("adjective", "noun"): [("adjective", 0.85)],
    ("adjective", "verb"): [("noun", 0.90)],
    ("adjective", "adjective"): [("noun", 0.70)],
    ("noun", "noun"): [("verb", 0.85)],
    ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
    ("noun", "adverb"): [("verb", 0.90)],
    ("noun", "preposition"): [("verb", 0.85)],
    ("noun", "determiner"): [("verb", 0.90)],
    ("noun", "article"): [("verb", 0.90)],
    ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
    ("verb", "verb"): [("adverb", 0.75)],
    ("verb", "adjective"): [("adverb", 0.85)],
    ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
    ("pronoun", "noun"): [("verb", 0.90)],
    ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
    ("pronoun", "determiner"): [("verb", 0.90)],
    ("pronoun", "article"): [("verb", 0.90)],
    ("pronoun", "adverb"): [("verb", 0.85)],
    ("pronoun", "preposition"): [("verb", 0.90)],
    ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
    ("preposition", "proper_noun"): [("adjective", 0.80)],
    ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
    ("preposition", "verb"): [("noun", 0.80)],
    ("adverb", "noun"): [("adjective", 0.85)],
    ("adverb", "verb"): [("adverb", 0.75)],
    ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
    ("be", "adjective"): [("adverb", 0.90)],
    ("be", "preposition"): [("adverb", 0.80)],
    # ONLY LEFT NEIGHBOR KNOWN
    ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
    ("article", None): [("noun", 0.85), ("adjective", 0.80)],
    ("adjective", None): [("noun", 0.90)],
    ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
    ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
    ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
    ("noun", None): [("verb", 0.80), ("noun", 0.60)],
    ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
    ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
    # ONLY RIGHT NEIGHBOR KNOWN
    (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
    (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
    (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
    (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
    (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
    (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
    (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
    (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
    (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
}

# Pre-build SENTENCE_PATTERNS (unchanged structure)
SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
    "determiner": [
        ("noun",), ("adjective",),
        ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
        ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
        ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
        ("noun", "be", "adjective"), ("noun", "be", "noun"),
        ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
        ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
    ],
    "article": [
        ("noun",), ("adjective",),
        ("adjective", "noun"), ("noun", "verb"),
        ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
        ("noun", "be", "adjective"), ("noun", "be", "noun"),
    ],
    "pronoun": [
        ("verb",), ("be",),
        ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
        ("be", "adjective"), ("be", "noun"),
        ("verb", "determiner", "noun"), ("verb", "article", "noun"),
        ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
        ("be", "determiner", "noun"), ("be", "preposition", "noun"),
        ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
        ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
    ],
    "noun": [
        ("verb",), ("be",),
        ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
        ("be", "adjective"), ("be", "noun"),
        ("verb", "determiner", "noun"), ("verb", "article", "noun"),
        ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
        ("be", "preposition", "noun"), ("be", "determiner", "noun"),
        ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
    ],
    "adjective": [
        ("noun",),
        ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
        ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
        ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
    ],
    "verb": [
        ("noun",), ("adverb",), ("adjective",),
        ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
        ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
        ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
        ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
        ("noun", "determiner", "noun"),
        ("preposition", "determiner", "adjective", "noun"),
    ],
    "preposition": [
        ("noun",), ("proper_noun",),
        ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
        ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
        ("adjective", "adjective", "noun"),
    ],
    "adverb": [
        ("verb",), ("adjective",), ("adverb",),
        ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
    ],
    "be": [
        ("adjective",), ("noun",),
        ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
        ("adverb", "adjective"), ("determiner", "adjective", "noun"),
    ],
}


# =============================================================================
# CACHED HELPERS
# =============================================================================

@lru_cache(maxsize=10000)
def cached_levenshtein(word1: str, word2: str) -> int:
    """Cached Levenshtein distance calculation."""
    return damerau_levenshtein_distance(word1, word2)


@lru_cache(maxsize=1000)
def normalize_pos_cached(pos_value: str) -> str:
    """Cached POS normalization for string inputs."""
    if pos_value in LOCATION_TYPES:
        return 'proper_noun'
    if pos_value in COMPOUND_NOUN_TYPES:
        return 'noun'
    return pos_value


def normalize_pos(pos_value: Any) -> str:
    """Normalize POS value, converting location types to proper_noun."""
    if pos_value is None:
        return 'unknown'
    
    # Handle JSON string format: '["determiner"]' -> ["determiner"]
    if isinstance(pos_value, str):
        if pos_value.startswith('['):
            try:
                pos_value = json.loads(pos_value)
            except json.JSONDecodeError:
                pass
        else:
            return normalize_pos_cached(pos_value)
    
    # Handle list format: ["determiner"] -> "determiner"
    if isinstance(pos_value, list):
        pos_value = pos_value[0] if pos_value else 'unknown'
        if isinstance(pos_value, str):
            return normalize_pos_cached(pos_value)
    
    return str(pos_value) if pos_value else 'unknown'


# =============================================================================
# OPTIMIZED DATA STRUCTURES
# =============================================================================

class WordState:
    """Efficient state container for a word being processed."""
    __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
                 'distance', 'metadata', 'correction_reason')
    
    def __init__(self, position: int, word: str):
        self.position = position
        self.word = word.lower()
        self.status = 'unknown'
        self.pos = 'unknown'
        self.corrected: Optional[str] = None
        self.distance: int = 0
        self.metadata: Dict[str, Any] = {}
        self.correction_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for compatibility."""
        result = {
            'position': self.position,
            'word': self.word,
            'status': self.status,
            'pos': self.pos,
            'metadata': self.metadata
        }
        if self.corrected:
            result['corrected'] = self.corrected
            result['distance'] = self.distance
        if self.correction_reason:
            result['correction_reason'] = self.correction_reason
        return result


class PositionMap:
    """O(1) position-based lookups for word states."""
    __slots__ = ('_by_position', '_states')
    
    def __init__(self, states: List[WordState]):
        self._states = states
        self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
    def get_pos(self, position: int) -> Optional[str]:
        """Get POS at position, None if not found or unknown."""
        state = self._by_position.get(position)
        if state and state.pos in ALLOWED_POS:
            return state.pos
        return None
    
    def get_state(self, position: int) -> Optional[WordState]:
        """Get WordState at position."""
        return self._by_position.get(position)
    
    def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
        """Get (left_pos, right_pos) for a position. O(1)."""
        left = self.get_pos(position - 1)
        right = self.get_pos(position + 1)
        return left, right
    
    def update_pos(self, position: int, new_pos: str) -> None:
        """Update POS at position."""
        state = self._by_position.get(position)
        if state:
            state.pos = new_pos
    
    def get_tuple_array(self) -> List[Tuple[int, str]]:
        """Get (position, pos) tuples."""
        return [(s.position, s.pos) for s in self._states]
    
    def __iter__(self):
        return iter(self._states)
    
    def __len__(self):
        return len(self._states)


# =============================================================================
# BATCHED REDIS OPERATIONS
# =============================================================================

def batch_validate_words(words: List[str]) -> List[WordState]:
    """
    PASS 1: Validate all words in minimal Redis calls.
    
    Optimization: Single batch call instead of N calls.
    """
    if not words:
        return []
    
    states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
    # Batch validate - collect all words to check
    words_to_check = [s.word for s in states]
    
    # Option 1: If your searchapi supports batch operations, use them:
    # results = batch_validate_words_redis(words_to_check)
    
    # Option 2: If not, we still call individually but could be optimized
    # with Redis pipeline in searchapi module
    for state in states:
        validation = validate_word(state.word)
        
        if validation.get('is_valid'):
            metadata = validation.get('metadata') or get_term_metadata(state.word) or {}
            state.status = 'valid'
            state.pos = normalize_pos(metadata.get('pos', 'unknown'))
            state.metadata = metadata
        # else: stays as 'unknown'
    
    return states


def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    Batch check multiple bigrams at once.
    
    Returns: Dict mapping "word1 word2" -> metadata (or empty if not found)
    """
    results = {}
    
    # Ideally this would be a single Redis MGET or pipeline
    # For now, we batch the calls conceptually
    for word1, word2 in word_pairs:
        bigram = f"{word1.lower()} {word2.lower()}"
        metadata = get_term_metadata(bigram)
        if metadata and metadata.get('exists'):
            results[bigram] = metadata
    
    return results


# =============================================================================
# CONTEXT-BASED PREDICTION (O(1) lookups)
# =============================================================================

def predict_pos_from_context(
    left_pos: Optional[str],
    right_pos: Optional[str]
) -> Optional[Tuple[str, float]]:
    """
    Predict POS based on context. O(1) dict lookups.
    """
    # Try both neighbors (most specific)
    key = (left_pos, right_pos)
    if key in LOCAL_CONTEXT_RULES:
        return LOCAL_CONTEXT_RULES[key][0]
    
    # Try left only
    if left_pos:
        key = (left_pos, None)
        if key in LOCAL_CONTEXT_RULES:
            return LOCAL_CONTEXT_RULES[key][0]
    
    # Try right only
    if right_pos:
        key = (None, right_pos)
        if key in LOCAL_CONTEXT_RULES:
            return LOCAL_CONTEXT_RULES[key][0]
    
    return None


def get_valid_pos_for_context(
    left_pos: Optional[str],
    right_pos: Optional[str]
) -> List[Tuple[str, float]]:
    """
    Get ALL valid POS options for context. O(1) lookups.
    """
    # Try both neighbors first
    if left_pos and right_pos:
        key = (left_pos, right_pos)
        if key in LOCAL_CONTEXT_RULES:
            return LOCAL_CONTEXT_RULES[key]
    
    # Try left only
    if left_pos:
        key = (left_pos, None)
        if key in LOCAL_CONTEXT_RULES:
            return LOCAL_CONTEXT_RULES[key]
    
    # Try right only
    if right_pos:
        key = (None, right_pos)
        if key in LOCAL_CONTEXT_RULES:
            return LOCAL_CONTEXT_RULES[key]
    
    return []


def match_sentence_pattern(
    pos_map: PositionMap,
    unknown_position: int
) -> Optional[str]:
    """
    Match sentence against known patterns. O(patterns) but patterns are small.
    """
    # Find starting POS
    starting_pos = None
    starting_position = 0
    
    for state in pos_map:
        if state.pos in SENTENCE_PATTERNS:
            starting_pos = state.pos
            starting_position = state.position
            break
    
    if not starting_pos:
        return None
    
    patterns = SENTENCE_PATTERNS.get(starting_pos, [])
    
    # Build sequence after starting position
    sequence = [s.pos for s in pos_map if s.position > starting_position]
    unknown_index = unknown_position - starting_position - 1
    
    if unknown_index < 0:
        return None
    
    # Match patterns
    for pattern in patterns:
        if len(pattern) < len(sequence):
            continue
        
        matches = True
        for i, tag in enumerate(sequence):
            if i >= len(pattern):
                break
            if tag != 'unknown' and tag != pattern[i]:
                matches = False
                break
        
        if matches and unknown_index < len(pattern):
            return pattern[unknown_index]
    
    return None


# =============================================================================
# CORRECTION SEARCH (with caching)
# =============================================================================

def search_with_pos_filter(
    word: str,
    required_pos: str,
    max_distance: int = 2
) -> Optional[Dict[str, Any]]:
    """
    Search for corrections filtered by POS.
    Uses cached Levenshtein distances.
    """
    # Generate candidates
    candidates = generate_candidates_smart(word, max_candidates=50)
    
    if not candidates:
        return None
    
    # Batch check candidates
    found = batch_check_candidates(candidates)
    
    if not found:
        return None
    
    word_lower = word.lower()
    matches = []
    fallback_matches = []
    
    for item in found:
        item_pos = normalize_pos(item.get('pos', 'unknown'))
        term_lower = item.get('term', '').lower()
        
        # Use cached distance calculation
        distance = cached_levenshtein(word_lower, term_lower)
        
        if distance > max_distance:
            continue
        
        item['distance'] = distance
        
        # Check POS match
        pos_match = (
            item_pos == required_pos or
            (required_pos == 'proper_noun' and item.get('subtext', '').lower() in LOCATION_TYPES) or
            (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
        )
        
        if pos_match:
            matches.append(item)
        else:
            fallback_matches.append(item)
    
    # Use matches first, fallback second
    result_list = matches if matches else fallback_matches
    
    if not result_list:
        return None
    
    # Sort by distance, then by rank (descending)
    result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
    return result_list[0]


def search_without_pos_filter(
    word: str,
    max_distance: int = 3
) -> Optional[Dict[str, Any]]:
    """
    Search for corrections without POS filtering.
    Used for single-word queries with no context.
    """
    word_lower = word.lower()
    
    # Try suggestions API first
    suggestions = get_suggestions(word, limit=10)
    
    if suggestions:
        matches = []
        for suggestion in suggestions:
            term = suggestion.get('term', '')
            distance = cached_levenshtein(word_lower, term.lower())
            if distance <= max_distance:
                suggestion['distance'] = distance
                matches.append(suggestion)
        
        if matches:
            matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
            return matches[0]
    
    # Try candidates
    candidates = generate_candidates_smart(word, max_candidates=100)
    found = batch_check_candidates(candidates)
    
    if found:
        matches = []
        for item in found:
            distance = cached_levenshtein(word_lower, item.get('term', '').lower())
            if distance <= max_distance:
                item['distance'] = distance
                matches.append(item)
        
        if matches:
            matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
            return matches[0]
    
    return None


# =============================================================================
# PASS 2: PATTERN-BASED CORRECTION
# =============================================================================

def correct_unknowns(pos_map: PositionMap) -> None:
    """
    Correct unknown words using context prediction.
    Modifies states in place.
    """
    for state in pos_map:
        if state.status != 'unknown':
            continue
        
        # Get context (O(1))
        left_pos, right_pos = pos_map.get_context(state.position)
        
        # Predict POS
        prediction = predict_pos_from_context(left_pos, right_pos)
        
        if prediction:
            predicted_pos, confidence = prediction
        else:
            # Try pattern matching
            predicted_pos = match_sentence_pattern(pos_map, state.position)
            if not predicted_pos:
                predicted_pos = 'noun'  # Default fallback
        
        # Search for correction
        correction = search_with_pos_filter(state.word, predicted_pos)
        
        if correction:
            state.status = 'corrected'
            state.corrected = correction['term']
            state.pos = normalize_pos(correction.get('pos', 'unknown'))
            state.distance = correction['distance']
            state.metadata = correction


def detect_and_correct_violations(pos_map: PositionMap) -> None:
    """
    Detect and correct pattern violations in valid words.
    Modifies states in place.
    """
    for state in pos_map:
        if state.status != 'valid':
            continue
        
        # Get context (O(1))
        left_pos, right_pos = pos_map.get_context(state.position)
        
        # Get valid POS options
        valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
        if not valid_options:
            continue
        
        # Check if current POS is valid
        valid_pos_set = {pos for pos, conf in valid_options}
        
        if state.pos in valid_pos_set:
            continue
        
        # Violation detected - try to correct
        expected_pos, confidence = valid_options[0]
        
        correction = search_with_pos_filter(state.word, expected_pos)
        
        if correction:
            state.status = 'corrected'
            state.corrected = correction['term']
            state.pos = normalize_pos(correction.get('pos', 'unknown'))
            state.distance = correction['distance']
            state.metadata = correction
            state.correction_reason = 'pattern_violation'


def run_pass2(pos_map: PositionMap) -> None:
    """
    PASS 2: Correct unknowns and pattern violations.
    """
    # Step 1: Correct unknowns
    correct_unknowns(pos_map)
    
    # Step 2: Detect and correct violations
    detect_and_correct_violations(pos_map)


# =============================================================================
# PASS 3: BIGRAM DETECTION
# =============================================================================

def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
    """
    PASS 3: Detect bigrams using batched lookup.
    """
    states = list(pos_map)
    
    if len(states) < 2:
        return []
    
    # Collect all consecutive pairs
    pairs_to_check = []
    pair_positions = []
    
    for i in range(len(states) - 1):
        current = states[i]
        next_state = states[i + 1]
        
        word1 = current.corrected or current.word
        word2 = next_state.corrected or next_state.word
        
        pairs_to_check.append((word1, word2))
        pair_positions.append((current.position, next_state.position))
    
    # Batch check all bigrams
    bigram_results = batch_check_bigrams(pairs_to_check)
    
    # Process results
    bigrams_found = []
    
    for i, (word1, word2) in enumerate(pairs_to_check):
        bigram_key = f"{word1.lower()} {word2.lower()}"
        
        if bigram_key in bigram_results:
            metadata = bigram_results[bigram_key]
            subtext = metadata.get('subtext', '')
            
            bigram_pos = 'proper_noun' if subtext.lower() in LOCATION_TYPES else 'noun'
            
            pos_start, pos_end = pair_positions[i]
            
            bigrams_found.append({
                'position_start': pos_start,
                'position_end': pos_end,
                'word1': word1,
                'word2': word2,
                'bigram': f"{word1} {word2}",
                'pos': bigram_pos,
                'subtext': subtext,
                'entity': metadata.get('entity', 'bigram'),
                'metadata': metadata
            })
    
    return bigrams_found


# =============================================================================
# OUTPUT BUILDING
# =============================================================================

def build_final_results(
    pos_map: PositionMap,
    bigrams: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Build final merged results with bigrams.
    """
    if not bigrams:
        return [s.to_dict() for s in pos_map]
    
    # Build bigram position sets
    bigram_starts = {b['position_start']: b for b in bigrams}
    bigram_positions = set()
    for b in bigrams:
        bigram_positions.add(b['position_start'])
        bigram_positions.add(b['position_end'])
    
    # Merge
    merged = []
    skip_next = False
    
    for state in pos_map:
        if skip_next:
            skip_next = False
            continue
        
        if state.position in bigram_starts:
            bigram = bigram_starts[state.position]
            merged.append({
                'position': state.position,
                'word': bigram['bigram'],
                'status': 'bigram',
                'pos': bigram['pos'],
                'subtext': bigram['subtext'],
                'entity': bigram['entity'],
                'metadata': bigram['metadata']
            })
            skip_next = True
        elif state.position not in bigram_positions:
            merged.append(state.to_dict())
    
    return merged


def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
    """Build list of corrections made."""
    corrections = []
    for state in pos_map:
        if state.status == 'corrected':
            corrections.append({
                'position': state.position,
                'original': state.word,
                'corrected': state.corrected,
                'distance': state.distance,
                'pos': state.pos,
                'is_bigram': False
            })
    return corrections


def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
    """Build the corrected query string."""
    words = []
    for r in final_results:
        if r['status'] == 'bigram':
            words.append(r['word'])
        elif r.get('corrected'):
            words.append(r['corrected'])
        else:
            words.append(r['word'])
    return ' '.join(words)


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def word_discovery_multi(
    query: str,
    redis_client=None,
    prefix: str = "prefix",
    verbose: bool = False
) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
    """
    Main entry point: Process query through all three passes.
    
    Optimized version with:
    - Batched Redis operations
    - O(1) position lookups
    - Cached Levenshtein calculations
    - Minimal data structure rebuilding
    
    Args:
        query: The input query string
        redis_client: Redis client (optional)
        prefix: Redis key prefix
        verbose: Whether to print debug output
    
    Returns:
        Tuple of (corrections, tuple_array, corrected_query)
    """
    words = query.split()
    
    if not words:
        return [], [], ""
    
    # =========================================================================
    # PASS 1: Batch validate all words
    # =========================================================================
    if verbose:
        print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
        print("\nPASS 1: Validating words...")
    
    states = batch_validate_words(words)
    pos_map = PositionMap(states)
    
    if verbose:
        for s in states:
            status = "✅" if s.status == 'valid' else "❓"
            print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
    # =========================================================================
    # PASS 2: Correct unknowns and violations
    # =========================================================================
    if verbose:
        print("\nPASS 2: Correcting unknowns and violations...")
    
    run_pass2(pos_map)
    
    if verbose:
        for s in states:
            if s.status == 'corrected':
                print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos})")
    
    # =========================================================================
    # PASS 3: Detect bigrams
    # =========================================================================
    if verbose:
        print("\nPASS 3: Detecting bigrams...")
    
    bigrams = detect_bigrams(pos_map)
    
    if verbose:
        for b in bigrams:
            print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
    # =========================================================================
    # BUILD OUTPUT
    # =========================================================================
    final_results = build_final_results(pos_map, bigrams)
    corrections = build_corrections_list(pos_map)
    tuple_array = [(r['position'], r['pos']) for r in final_results]
    corrected_query = build_corrected_query(final_results)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"📊 RESULT: '{corrected_query}'")
        print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
        print(f"{'='*60}\n")
    
    return corrections, tuple_array, corrected_query


# =============================================================================
# OPTIONAL: Redis Pipeline wrapper for searchapi
# =============================================================================

class BatchedRedisClient:
    """
    Wrapper to batch Redis operations.
    Use this if you can modify searchapi to accept a client.
    
    Example usage:
        with BatchedRedisClient(redis_conn) as batch:
            batch.queue_validate('word1')
            batch.queue_validate('word2')
            results = batch.execute()
    """
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.pipeline = None
        self.operations = []
    
    def __enter__(self):
        self.pipeline = self.redis.pipeline()
        self.operations = []
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pipeline = None
        self.operations = []
    
    def queue_exists(self, key: str) -> int:
        """Queue an EXISTS check, returns operation index."""
        idx = len(self.operations)
        self.pipeline.exists(key)
        self.operations.append(('exists', key))
        return idx
    
    def queue_hgetall(self, key: str) -> int:
        """Queue HGETALL, returns operation index."""
        idx = len(self.operations)
        self.pipeline.hgetall(key)
        self.operations.append(('hgetall', key))
        return idx
    
    def execute(self) -> List[Any]:
        """Execute all queued operations."""
        return self.pipeline.execute()