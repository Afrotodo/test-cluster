# """
# word_discovery_optimized.py
# Optimized three-pass word validation, correction, and bigram detection.

# Key optimizations:
# 1. Batched Redis operations (single pipeline per pass)
# 2. O(1) position lookups using dicts
# 3. No redundant data structure rebuilding
# 4. Cached Levenshtein distance calculations
# 5. Combined validate + metadata in single call
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache
# from decouple import config

# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # Import your existing Redis functions - we'll wrap them for batching
# from .searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata,
#     get_suggestions,
#     generate_candidates_smart,
#     batch_check_candidates
# )


# # =============================================================================
# # CONSTANTS (unchanged, but converted to frozensets for O(1) lookup)
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-build LOCAL_CONTEXT_RULES for O(1) lookup (same data, just ensuring it's a dict)
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }

# # Pre-build SENTENCE_PATTERNS (unchanged structure)
# SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
#     "determiner": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
#     ],
#     "article": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#     ],
#     "pronoun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"), ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
#     ],
#     "noun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"), ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
#     ],
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
#         ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
#     ],
#     "verb": [
#         ("noun",), ("adverb",), ("adjective",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
#     "preposition": [
#         ("noun",), ("proper_noun",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
#     "adverb": [
#         ("verb",), ("adjective",), ("adverb",),
#         ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
#     ],
#     "be": [
#         ("adjective",), ("noun",),
#         ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
#         ("adverb", "adjective"), ("determiner", "adjective", "noun"),
#     ],
# }


# # =============================================================================
# # CACHED HELPERS
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     return damerau_levenshtein_distance(word1, word2)


# @lru_cache(maxsize=1000)
# def normalize_pos_cached(pos_value: str) -> str:
#     """Cached POS normalization for string inputs."""
#     if pos_value in LOCATION_TYPES:
#         return 'proper_noun'
#     if pos_value in COMPOUND_NOUN_TYPES:
#         return 'noun'
#     return pos_value


# def normalize_pos(pos_value: Any) -> str:
#     """Normalize POS value, converting location types to proper_noun."""
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle JSON string format: '["determiner"]' -> ["determiner"]
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 pos_value = json.loads(pos_value)
#             except json.JSONDecodeError:
#                 pass
#         else:
#             return normalize_pos_cached(pos_value)
    
#     # Handle list format: ["determiner"] -> "determiner"
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
#         if isinstance(pos_value, str):
#             return normalize_pos_cached(pos_value)
    
#     return str(pos_value) if pos_value else 'unknown'


# # =============================================================================
# # OPTIMIZED DATA STRUCTURES
# # =============================================================================

# class WordState:
#     """Efficient state container for a word being processed."""
#     __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
#                  'distance', 'metadata', 'correction_reason')
    
#     def __init__(self, position: int, word: str):
#         self.position = position
#         self.word = word.lower()
#         self.status = 'unknown'
#         self.pos = 'unknown'
#         self.corrected: Optional[str] = None
#         self.distance: int = 0
#         self.metadata: Dict[str, Any] = {}
#         self.correction_reason: Optional[str] = None
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dict for compatibility."""
#         result = {
#             'position': self.position,
#             'word': self.word,
#             'status': self.status,
#             'pos': self.pos,
#             'metadata': self.metadata
#         }
#         if self.corrected:
#             result['corrected'] = self.corrected
#             result['distance'] = self.distance
#         if self.correction_reason:
#             result['correction_reason'] = self.correction_reason
#         return result


# class PositionMap:
#     """O(1) position-based lookups for word states."""
#     __slots__ = ('_by_position', '_states')
    
#     def __init__(self, states: List[WordState]):
#         self._states = states
#         self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
#     def get_pos(self, position: int) -> Optional[str]:
#         """Get POS at position, None if not found or unknown."""
#         state = self._by_position.get(position)
#         if state and state.pos in ALLOWED_POS:
#             return state.pos
#         return None
    
#     def get_state(self, position: int) -> Optional[WordState]:
#         """Get WordState at position."""
#         return self._by_position.get(position)
    
#     def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
#         """Get (left_pos, right_pos) for a position. O(1)."""
#         left = self.get_pos(position - 1)
#         right = self.get_pos(position + 1)
#         return left, right
    
#     def update_pos(self, position: int, new_pos: str) -> None:
#         """Update POS at position."""
#         state = self._by_position.get(position)
#         if state:
#             state.pos = new_pos
    
#     def get_tuple_array(self) -> List[Tuple[int, str]]:
#         """Get (position, pos) tuples."""
#         return [(s.position, s.pos) for s in self._states]
    
#     def __iter__(self):
#         return iter(self._states)
    
#     def __len__(self):
#         return len(self._states)


# # =============================================================================
# # BATCHED REDIS OPERATIONS
# # =============================================================================

# def batch_validate_words(words: List[str]) -> List[WordState]:
#     """
#     PASS 1: Validate all words in minimal Redis calls.
    
#     Optimization: Single batch call instead of N calls.
#     """
#     if not words:
#         return []
    
#     states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
#     # Batch validate - collect all words to check
#     words_to_check = [s.word for s in states]
    
#     # Option 1: If your searchapi supports batch operations, use them:
#     # results = batch_validate_words_redis(words_to_check)
    
#     # Option 2: If not, we still call individually but could be optimized
#     # with Redis pipeline in searchapi module
#     for state in states:
#         validation = validate_word(state.word)
        
#         if validation.get('is_valid'):
#             metadata = validation.get('metadata') or get_term_metadata(state.word) or {}
#             state.status = 'valid'
#             state.pos = normalize_pos(metadata.get('pos', 'unknown'))
#             state.metadata = metadata
#         # else: stays as 'unknown'
    
#     return states


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """
#     Batch check multiple bigrams at once.
    
#     Returns: Dict mapping "word1 word2" -> metadata (or empty if not found)
#     """
#     results = {}
    
#     # Ideally this would be a single Redis MGET or pipeline
#     # For now, we batch the calls conceptually
#     for word1, word2 in word_pairs:
#         bigram = f"{word1.lower()} {word2.lower()}"
#         metadata = get_term_metadata(bigram)
#         if metadata and metadata.get('exists'):
#             results[bigram] = metadata
    
#     return results


# # =============================================================================
# # CONTEXT-BASED PREDICTION (O(1) lookups)
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """
#     Predict POS based on context. O(1) dict lookups.
#     """
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """
#     Get ALL valid POS options for context. O(1) lookups.
#     """
#     # Try both neighbors first
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def match_sentence_pattern(
#     pos_map: PositionMap,
#     unknown_position: int
# ) -> Optional[str]:
#     """
#     Match sentence against known patterns. O(patterns) but patterns are small.
#     """
#     # Find starting POS
#     starting_pos = None
#     starting_position = 0
    
#     for state in pos_map:
#         if state.pos in SENTENCE_PATTERNS:
#             starting_pos = state.pos
#             starting_position = state.position
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
    
#     # Build sequence after starting position
#     sequence = [s.pos for s in pos_map if s.position > starting_position]
#     unknown_index = unknown_position - starting_position - 1
    
#     if unknown_index < 0:
#         return None
    
#     # Match patterns
#     for pattern in patterns:
#         if len(pattern) < len(sequence):
#             continue
        
#         matches = True
#         for i, tag in enumerate(sequence):
#             if i >= len(pattern):
#                 break
#             if tag != 'unknown' and tag != pattern[i]:
#                 matches = False
#                 break
        
#         if matches and unknown_index < len(pattern):
#             return pattern[unknown_index]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH (with caching)
# # =============================================================================

# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by POS.
#     Uses cached Levenshtein distances.
#     """
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batch check candidates
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # Use cached distance calculation
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
        
#         # Check POS match
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('subtext', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     # Use matches first, fallback second
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     # Sort by distance, then by rank (descending)
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Used for single-word queries with no context.
#     """
#     word_lower = word.lower()
    
#     # Try suggestions API first
#     suggestions = get_suggestions(word, limit=10)
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = cached_levenshtein(word_lower, term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     # Try candidates
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = cached_levenshtein(word_lower, item.get('term', '').lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     return None


# # =============================================================================
# # PASS 2: PATTERN-BASED CORRECTION
# # =============================================================================

# def correct_unknowns(pos_map: PositionMap) -> None:
#     """
#     Correct unknown words using context prediction.
#     Modifies states in place.
#     """
#     for state in pos_map:
#         if state.status != 'unknown':
#             continue
        
#         # Get context (O(1))
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # Predict POS
#         prediction = predict_pos_from_context(left_pos, right_pos)
        
#         if prediction:
#             predicted_pos, confidence = prediction
#         else:
#             # Try pattern matching
#             predicted_pos = match_sentence_pattern(pos_map, state.position)
#             if not predicted_pos:
#                 predicted_pos = 'noun'  # Default fallback
        
#         # Search for correction
#         correction = search_with_pos_filter(state.word, predicted_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction


# def detect_and_correct_violations(pos_map: PositionMap) -> None:
#     """
#     Detect and correct pattern violations in valid words.
#     Modifies states in place.
#     """
#     for state in pos_map:
#         if state.status != 'valid':
#             continue
        
#         # Get context (O(1))
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # Get valid POS options
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         # Check if current POS is valid
#         valid_pos_set = {pos for pos, conf in valid_options}
        
#         if state.pos in valid_pos_set:
#             continue
        
#         # Violation detected - try to correct
#         expected_pos, confidence = valid_options[0]
        
#         correction = search_with_pos_filter(state.word, expected_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#             state.correction_reason = 'pattern_violation'


# def run_pass2(pos_map: PositionMap) -> None:
#     """
#     PASS 2: Correct unknowns and pattern violations.
#     """
#     # Step 1: Correct unknowns
#     correct_unknowns(pos_map)
    
#     # Step 2: Detect and correct violations
#     detect_and_correct_violations(pos_map)


# # =============================================================================
# # PASS 3: BIGRAM DETECTION
# # =============================================================================

# def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """
#     PASS 3: Detect bigrams using batched lookup.
#     """
#     states = list(pos_map)
    
#     if len(states) < 2:
#         return []
    
#     # Collect all consecutive pairs
#     pairs_to_check = []
#     pair_positions = []
    
#     for i in range(len(states) - 1):
#         current = states[i]
#         next_state = states[i + 1]
        
#         word1 = current.corrected or current.word
#         word2 = next_state.corrected or next_state.word
        
#         pairs_to_check.append((word1, word2))
#         pair_positions.append((current.position, next_state.position))
    
#     # Batch check all bigrams
#     bigram_results = batch_check_bigrams(pairs_to_check)
    
#     # Process results
#     bigrams_found = []
    
#     for i, (word1, word2) in enumerate(pairs_to_check):
#         bigram_key = f"{word1.lower()} {word2.lower()}"
        
#         if bigram_key in bigram_results:
#             metadata = bigram_results[bigram_key]
#             subtext = metadata.get('subtext', '')
            
#             bigram_pos = 'proper_noun' if subtext.lower() in LOCATION_TYPES else 'noun'
            
#             pos_start, pos_end = pair_positions[i]
            
#             bigrams_found.append({
#                 'position_start': pos_start,
#                 'position_end': pos_end,
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'subtext': subtext,
#                 'entity': metadata.get('entity', 'bigram'),
#                 'metadata': metadata
#             })
    
#     return bigrams_found


# # =============================================================================
# # OUTPUT BUILDING
# # =============================================================================

# def build_final_results(
#     pos_map: PositionMap,
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """
#     Build final merged results with bigrams.
#     """
#     if not bigrams:
#         return [s.to_dict() for s in pos_map]
    
#     # Build bigram position sets
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     # Merge
#     merged = []
#     skip_next = False
    
#     for state in pos_map:
#         if skip_next:
#             skip_next = False
#             continue
        
#         if state.position in bigram_starts:
#             bigram = bigram_starts[state.position]
#             merged.append({
#                 'position': state.position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif state.position not in bigram_positions:
#             merged.append(state.to_dict())
    
#     return merged


# def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """Build list of corrections made."""
#     corrections = []
#     for state in pos_map:
#         if state.status == 'corrected':
#             corrections.append({
#                 'position': state.position,
#                 'original': state.word,
#                 'corrected': state.corrected,
#                 'distance': state.distance,
#                 'pos': state.pos,
#                 'is_bigram': False
#             })
#     return corrections


# def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
#     """Build the corrected query string."""
#     words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             words.append(r['word'])
#         elif r.get('corrected'):
#             words.append(r['corrected'])
#         else:
#             words.append(r['word'])
#     return ' '.join(words)


# # =============================================================================
# # MAIN ORCHESTRATOR
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     Optimized version with:
#     - Batched Redis operations
#     - O(1) position lookups
#     - Cached Levenshtein calculations
#     - Minimal data structure rebuilding
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional)
#         prefix: Redis key prefix
#         verbose: Whether to print debug output
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Batch validate all words
#     # =========================================================================
#     if verbose:
#         print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
#         print("\nPASS 1: Validating words...")
    
#     states = batch_validate_words(words)
#     pos_map = PositionMap(states)
    
#     if verbose:
#         for s in states:
#             status = "✅" if s.status == 'valid' else "❓"
#             print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
#     # =========================================================================
#     # PASS 2: Correct unknowns and violations
#     # =========================================================================
#     if verbose:
#         print("\nPASS 2: Correcting unknowns and violations...")
    
#     run_pass2(pos_map)
    
#     if verbose:
#         for s in states:
#             if s.status == 'corrected':
#                 print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos})")
    
#     # =========================================================================
#     # PASS 3: Detect bigrams
#     # =========================================================================
#     if verbose:
#         print("\nPASS 3: Detecting bigrams...")
    
#     bigrams = detect_bigrams(pos_map)
    
#     if verbose:
#         for b in bigrams:
#             print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
#     # =========================================================================
#     # BUILD OUTPUT
#     # =========================================================================
#     final_results = build_final_results(pos_map, bigrams)
#     corrections = build_corrections_list(pos_map)
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
#     corrected_query = build_corrected_query(final_results)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 RESULT: '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
#         print(f"{'='*60}\n")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # OPTIONAL: Redis Pipeline wrapper for searchapi
# # =============================================================================

# class BatchedRedisClient:
#     """
#     Wrapper to batch Redis operations.
#     Use this if you can modify searchapi to accept a client.
    
#     Example usage:
#         with BatchedRedisClient(redis_conn) as batch:
#             batch.queue_validate('word1')
#             batch.queue_validate('word2')
#             results = batch.execute()
#     """
    
#     def __init__(self, redis_client):
#         self.redis = redis_client
#         self.pipeline = None
#         self.operations = []
    
#     def __enter__(self):
#         self.pipeline = self.redis.pipeline()
#         self.operations = []
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.pipeline = None
#         self.operations = []
    
#     def queue_exists(self, key: str) -> int:
#         """Queue an EXISTS check, returns operation index."""
#         idx = len(self.operations)
#         self.pipeline.exists(key)
#         self.operations.append(('exists', key))
#         return idx
    
#     def queue_hgetall(self, key: str) -> int:
#         """Queue HGETALL, returns operation index."""
#         idx = len(self.operations)
#         self.pipeline.hgetall(key)
#         self.operations.append(('hgetall', key))
#         return idx
    
#     def execute(self) -> List[Any]:
#         """Execute all queued operations."""
#         return self.pipeline.execute()



# """
# word_discovery_optimized.py
# Optimized three-pass word validation, correction, and bigram detection.

# BACKWARDS COMPATIBLE: All existing function signatures work unchanged.

# Key optimizations:
# 1. Pre-validated data passthrough (eliminates duplicate Redis calls)
# 2. Batched Redis operations (single pipeline per operation type)
# 3. O(1) position lookups using PositionMap
# 4. O(1) context rule lookups using dict keys
# 5. Cached Levenshtein distance calculations
# 6. Lazy evaluation where possible

# Complexity improvements:
# - Validation: O(n * k * log m) -> O(1) with pre_validated
# - Bigram detection: O(n) Redis calls -> O(2) Redis round-trips
# - Context lookup: O(1) via dict
# - Correction search: O(c * k * log m) -> O(2) Redis round-trips per unknown
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Import Redis functions - these now support batching
# from .searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata,
#     get_suggestions,
#     generate_candidates_smart,
#     batch_check_candidates,
#     batch_validate_words_redis,
#     batch_check_bigrams,
#     batch_get_term_metadata,
#     damerau_levenshtein_distance as _python_levenshtein
# )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }

# SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
#     "determiner": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
#     ],
#     "article": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#     ],
#     "pronoun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"), ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
#     ],
#     "noun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"), ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
#     ],
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
#         ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
#     ],
#     "verb": [
#         ("noun",), ("adverb",), ("adjective",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
#     "preposition": [
#         ("noun",), ("proper_noun",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
#     "adverb": [
#         ("verb",), ("adjective",), ("adverb",),
#         ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
#     ],
#     "be": [
#         ("adjective",), ("noun",),
#         ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
#         ("adverb", "adjective"), ("determiner", "adjective", "noun"),
#     ],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """
#     Cached Levenshtein distance calculation.
#     Complexity: O(1) for cached values, O(m*n) for new calculations
#     """
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# @lru_cache(maxsize=1000)
# def normalize_pos_cached(pos_value: str) -> str:
#     """
#     Cached POS normalization for string inputs.
#     Complexity: O(1) dict lookup
#     """
#     if pos_value in LOCATION_TYPES:
#         return 'proper_noun'
#     if pos_value in COMPOUND_NOUN_TYPES:
#         return 'noun'
#     return pos_value


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value, converting location types to proper_noun.
#     Complexity: O(1) for cached strings, O(p) for parsing where p = parts
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 pos_value = json.loads(pos_value)
#             except json.JSONDecodeError:
#                 pass
#         else:
#             return normalize_pos_cached(pos_value)
    
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
#         if isinstance(pos_value, str):
#             return normalize_pos_cached(pos_value)
    
#     return str(pos_value) if pos_value else 'unknown'


# # =============================================================================
# # OPTIMIZED DATA STRUCTURES
# # =============================================================================

# class WordState:
#     """
#     Efficient state container for a word being processed.
#     Uses __slots__ for memory efficiency and faster attribute access.
#     """
#     __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
#                  'distance', 'metadata', 'correction_reason', 'member')
    
#     def __init__(self, position: int, word: str):
#         self.position = position
#         self.word = word.lower()
#         self.status = 'unknown'
#         self.pos = 'unknown'
#         self.corrected: Optional[str] = None
#         self.distance: int = 0
#         self.metadata: Dict[str, Any] = {}
#         self.correction_reason: Optional[str] = None
#         self.member: Optional[str] = None
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dict for compatibility. O(1)"""
#         result = {
#             'position': self.position,
#             'word': self.word,
#             'status': self.status,
#             'pos': self.pos,
#             'metadata': self.metadata
#         }
#         if self.corrected:
#             result['corrected'] = self.corrected
#             result['distance'] = self.distance
#         if self.correction_reason:
#             result['correction_reason'] = self.correction_reason
#         return result


# class PositionMap:
#     """
#     O(1) position-based lookups for word states.
#     All operations are O(1) dict lookups.
#     """
#     __slots__ = ('_by_position', '_states')
    
#     def __init__(self, states: List[WordState]):
#         self._states = states
#         self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
#     def get_pos(self, position: int) -> Optional[str]:
#         """Get POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state and state.pos in ALLOWED_POS:
#             return state.pos
#         return None
    
#     def get_state(self, position: int) -> Optional[WordState]:
#         """Get WordState at position. O(1)"""
#         return self._by_position.get(position)
    
#     def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
#         """Get (left_pos, right_pos) for a position. O(1)"""
#         return self.get_pos(position - 1), self.get_pos(position + 1)
    
#     def update_pos(self, position: int, new_pos: str) -> None:
#         """Update POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state:
#             state.pos = new_pos
    
#     def get_tuple_array(self) -> List[Tuple[int, str]]:
#         """Get (position, pos) tuples. O(n)"""
#         return [(s.position, s.pos) for s in self._states]
    
#     def __iter__(self):
#         return iter(self._states)
    
#     def __len__(self):
#         return len(self._states)


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """
#     Predict POS based on context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """
#     Get ALL valid POS options for context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def match_sentence_pattern(
#     pos_map: PositionMap,
#     unknown_position: int
# ) -> Optional[str]:
#     """
#     Match sentence against known patterns.
#     Complexity: O(p * s) where p = patterns, s = sequence length (both small)
#     """
#     starting_pos = None
#     starting_position = 0
    
#     for state in pos_map:
#         if state.pos in SENTENCE_PATTERNS:
#             starting_pos = state.pos
#             starting_position = state.position
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [s.pos for s in pos_map if s.position > starting_position]
#     unknown_index = unknown_position - starting_position - 1
    
#     if unknown_index < 0:
#         return None
    
#     for pattern in patterns:
#         if len(pattern) < len(sequence):
#             continue
        
#         matches = True
#         for i, tag in enumerate(sequence):
#             if i >= len(pattern):
#                 break
#             if tag != 'unknown' and tag != pattern[i]:
#                 matches = False
#                 break
        
#         if matches and unknown_index < len(pattern):
#             return pattern[unknown_index]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - O(2) Redis round-trips per call
# # =============================================================================

# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by POS.
#     Complexity: O(2) Redis round-trips (batched candidate check)
#     """
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batched check - O(2) Redis round-trips
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # O(1) cached lookup
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
        
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Complexity: O(2) Redis round-trips
#     """
#     word_lower = word.lower()
    
#     suggestions_result = get_suggestions(word, limit=10)
#     suggestions = suggestions_result.get('suggestions', [])
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = cached_levenshtein(word_lower, term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = cached_levenshtein(word_lower, item.get('term', '').lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     return None


# # =============================================================================
# # PASS 1: VALIDATION - O(1) with pre_validated, O(2) Redis calls otherwise
# # =============================================================================

# def batch_validate_words(
#     words: List[str],
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> List[WordState]:
#     """
#     PASS 1: Validate all words.
    
#     BACKWARDS COMPATIBLE: Works without pre_validated (original behavior)
    
#     Complexity:
#         With pre_validated: O(n) - just dict lookups, NO Redis calls
#         Without pre_validated: O(2) Redis round-trips total
    
#     Args:
#         words: List of words to validate
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         List of WordState objects
#     """
#     if not words:
#         return []
    
#     states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
#     if pre_validated is not None:
#         # O(1) lookup path - build lookup dict
#         validated_lookup: Dict[str, Dict[str, Any]] = {}
        
#         for item in pre_validated:
#             word_key = item.get('word', '').lower()
#             if word_key:
#                 validated_lookup[word_key] = item
        
#         # Process each state with O(1) lookups
#         for state in states:
#             if state.word in validated_lookup:
#                 item = validated_lookup[state.word]
                
#                 if item.get('exists'):
#                     state.status = 'valid'
#                     state.pos = normalize_pos(item.get('pos', item.get('metadata', {}).get('pos', 'unknown')))
#                     state.metadata = item.get('metadata', item)
#                     state.member = item.get('member')
#                 # else: stays as 'unknown', will be corrected in Pass 2
        
#         return states
    
#     # Original path - batch Redis lookup
#     validation_cache = batch_validate_words_redis([s.word for s in states])
    
#     for state in states:
#         if state.word in validation_cache:
#             result = validation_cache[state.word]
            
#             if result.get('is_valid'):
#                 state.status = 'valid'
#                 metadata = result.get('metadata', {})
#                 state.pos = normalize_pos(metadata.get('pos', 'unknown'))
#                 state.metadata = metadata
#                 state.member = result.get('member')
    
#     return states


# # =============================================================================
# # PASS 2: CORRECTION - O(u * 2) Redis round-trips where u = unknowns
# # =============================================================================

# def correct_unknowns(pos_map: PositionMap) -> None:
#     """
#     Correct unknown words using context prediction.
#     Modifies states in place.
    
#     Complexity: O(u * 2) Redis round-trips where u = unknown words
#     """
#     for state in pos_map:
#         if state.status != 'unknown':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) prediction
#         prediction = predict_pos_from_context(left_pos, right_pos)
        
#         if prediction:
#             predicted_pos, confidence = prediction
#         else:
#             predicted_pos = match_sentence_pattern(pos_map, state.position)
#             if not predicted_pos:
#                 predicted_pos = 'noun'
        
#         # O(2) Redis round-trips per unknown
#         correction = search_with_pos_filter(state.word, predicted_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction


# def detect_and_correct_violations(pos_map: PositionMap) -> None:
#     """
#     Detect and correct pattern violations in valid words.
#     Modifies states in place.
    
#     Complexity: O(v * 2) Redis round-trips where v = violations
#     """
#     for state in pos_map:
#         if state.status != 'valid':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) valid options lookup
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_set = {pos for pos, conf in valid_options}
        
#         if state.pos in valid_pos_set:
#             continue
        
#         expected_pos, confidence = valid_options[0]
        
#         # O(2) Redis round-trips per violation
#         correction = search_with_pos_filter(state.word, expected_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#             state.correction_reason = 'pattern_violation'


# def run_pass2(pos_map: PositionMap) -> None:
#     """
#     PASS 2: Correct unknowns and pattern violations.
    
#     Complexity: O((u + v) * 2) Redis round-trips
#     """
#     correct_unknowns(pos_map)
#     detect_and_correct_violations(pos_map)


# # =============================================================================
# # PASS 3: BIGRAM DETECTION - O(2) Redis round-trips total
# # =============================================================================

# def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """
#     PASS 3: Detect bigrams using BATCHED lookup.
    
#     Complexity:
#         OLD: O(n) Redis calls
#         NEW: O(2) Redis round-trips regardless of n
#     """
#     states = list(pos_map)
    
#     if len(states) < 2:
#         return []
    
#     # Collect all consecutive pairs
#     pairs_to_check = []
#     pair_positions = []
    
#     for i in range(len(states) - 1):
#         current = states[i]
#         next_state = states[i + 1]
        
#         word1 = current.corrected or current.word
#         word2 = next_state.corrected or next_state.word
        
#         pairs_to_check.append((word1, word2))
#         pair_positions.append((current.position, next_state.position))
    
#     # BATCHED check - O(2) Redis round-trips for ALL bigrams
#     bigram_results = batch_check_bigrams(pairs_to_check)
    
#     # Process results - O(n)
#     bigrams_found = []
    
#     for i, (word1, word2) in enumerate(pairs_to_check):
#         bigram_key = f"{word1.lower()} {word2.lower()}"
        
#         if bigram_key in bigram_results:
#             metadata = bigram_results[bigram_key]
#             category = metadata.get('category', '')
            
#             bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
            
#             pos_start, pos_end = pair_positions[i]
            
#             bigrams_found.append({
#                 'position_start': pos_start,
#                 'position_end': pos_end,
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'subtext': category,
#                 'entity': metadata.get('entity_type', 'bigram'),
#                 'metadata': metadata
#             })
    
#     return bigrams_found


# # =============================================================================
# # OUTPUT BUILDING - O(n) in-memory operations
# # =============================================================================

# def build_final_results(
#     pos_map: PositionMap,
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Build final merged results with bigrams. O(n)"""
#     if not bigrams:
#         return [s.to_dict() for s in pos_map]
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for state in pos_map:
#         if skip_next:
#             skip_next = False
#             continue
        
#         if state.position in bigram_starts:
#             bigram = bigram_starts[state.position]
#             merged.append({
#                 'position': state.position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif state.position not in bigram_positions:
#             merged.append(state.to_dict())
    
#     return merged


# def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """Build list of corrections made. O(n)"""
#     corrections = []
#     for state in pos_map:
#         if state.status == 'corrected':
#             corrections.append({
#                 'position': state.position,
#                 'original': state.word,
#                 'corrected': state.corrected,
#                 'distance': state.distance,
#                 'pos': state.pos,
#                 'is_bigram': False
#             })
#     return corrections


# def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
#     """Build the corrected query string. O(n)"""
#     words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             words.append(r['word'])
#         elif r.get('corrected'):
#             words.append(r['corrected'])
#         else:
#             words.append(r['word'])
#     return ' '.join(words)


# # =============================================================================
# # MAIN ORCHESTRATOR - BACKWARDS COMPATIBLE
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     # NEW optional parameter - backwards compatible
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     BACKWARDS COMPATIBLE: All original parameters work the same.
    
#     NEW: pre_validated parameter allows passing data from lookup_table()
#          to skip duplicate Redis calls.
    
#     Complexity:
#         With pre_validated: O(n + (u+v)*2 + 2) where n=words, u=unknowns, v=violations
#         Without pre_validated: O(n*k*log(m) + (u+v)*2 + 2)
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, unused but kept for compatibility)
#         prefix: Redis key prefix (unused but kept for compatibility)
#         verbose: Whether to print debug output
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate - O(1) with pre_validated, O(2) otherwise
#     # =========================================================================
#     if verbose:
#         print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
#         print(f"\nPASS 1: Validating words...")
#         if pre_validated:
#             print("   (Using pre-validated data - O(1) lookups)")
    
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
    
#     if verbose:
#         for s in states:
#             status = "✅" if s.status == 'valid' else "❓"
#             print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
#     # =========================================================================
#     # PASS 2: Correct unknowns and violations - O((u+v) * 2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 2: Correcting unknowns and violations...")
    
#     run_pass2(pos_map)
    
#     if verbose:
#         for s in states:
#             if s.status == 'corrected':
#                 reason = f" ({s.correction_reason})" if s.correction_reason else ""
#                 print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos}){reason}")
    
#     # =========================================================================
#     # PASS 3: Detect bigrams - O(2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 3: Detecting bigrams...")
    
#     bigrams = detect_bigrams(pos_map)
    
#     if verbose:
#         for b in bigrams:
#             print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
#     # =========================================================================
#     # BUILD OUTPUT - O(n)
#     # =========================================================================
#     final_results = build_final_results(pos_map, bigrams)
#     corrections = build_corrections_list(pos_map)
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
#     corrected_query = build_corrected_query(final_results)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 RESULT: '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
#         print(f"{'='*60}\n")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # CONVENIENCE WRAPPER FOR OPTIMIZED FLOW
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing.
    
#     Combines lookup_table + word_discovery in optimal way:
#     - Single validation pass (no duplicate Redis calls)
#     - Batched bigram detection
#     - Full metadata passthrough
    
#     Complexity: O(2 + (u+v)*2 + 2) = O(4 + 2*(u+v))
#         - 2 round-trips for initial validation
#         - 2 round-trips per unknown/violation correction
#         - 2 round-trips for bigram detection
    
#     Args:
#         query: The input query
#         verbose: Print debug output
    
#     Returns:
#         Dict with all results including terms, corrections, and corrected query
#     """
#     from .searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result['success']:
#         return lookup_result
    
#     # Step 2: Word discovery with pre-validated data (skips duplicate calls)
#     corrections, tuple_array, corrected_query = word_discovery_multi(
#         query,
#         verbose=verbose,
#         pre_validated=lookup_result['terms']
#     )
    
#     # Step 3: Combine results
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': lookup_result['terms'],
#         'corrections': corrections,
#         'tuple_array': tuple_array,
#         'cache_hit': lookup_result.get('cache_hit', False)
#     }

# """
# word_discovery_optimized.py
# Optimized three-pass word validation, correction, and bigram detection.

# BACKWARDS COMPATIBLE: All existing function signatures work unchanged.

# Key optimizations:
# 1. Pre-validated data passthrough (eliminates duplicate Redis calls)
# 2. Batched Redis operations (single pipeline per operation type)
# 3. O(1) position lookups using PositionMap
# 4. O(1) context rule lookups using dict keys
# 5. Cached Levenshtein distance calculations
# 6. Lazy evaluation where possible

# Complexity improvements:
# - Validation: O(n * k * log m) -> O(1) with pre_validated
# - Bigram detection: O(n) Redis calls -> O(2) Redis round-trips
# - Context lookup: O(1) via dict
# - Correction search: O(c * k * log m) -> O(2) Redis round-trips per unknown
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Import Redis functions - these now support batching
# from .searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata,
#     get_suggestions,
#     generate_candidates_smart,
#     batch_check_candidates,
#     batch_validate_words_redis,
#     batch_check_bigrams,
#     batch_get_term_metadata,
#     damerau_levenshtein_distance as _python_levenshtein
# )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }

# SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
#     "determiner": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
#     ],
#     "article": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#     ],
#     "pronoun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"), ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
#     ],
#     "noun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"), ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
#     ],
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
#         ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
#     ],
#     "verb": [
#         ("noun",), ("adverb",), ("adjective",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
#     "preposition": [
#         ("noun",), ("proper_noun",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
#     "adverb": [
#         ("verb",), ("adjective",), ("adverb",),
#         ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
#     ],
#     "be": [
#         ("adjective",), ("noun",),
#         ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
#         ("adverb", "adjective"), ("determiner", "adjective", "noun"),
#     ],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """
#     Cached Levenshtein distance calculation.
#     Complexity: O(1) for cached values, O(m*n) for new calculations
#     """
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# @lru_cache(maxsize=1000)
# def normalize_pos_cached(pos_value: str) -> str:
#     """
#     Cached POS normalization for string inputs.
#     Complexity: O(1) dict lookup
#     """
#     if pos_value in LOCATION_TYPES:
#         return 'proper_noun'
#     if pos_value in COMPOUND_NOUN_TYPES:
#         return 'noun'
#     return pos_value


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value, converting location types to proper_noun.
#     Complexity: O(1) for cached strings, O(p) for parsing where p = parts
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 pos_value = json.loads(pos_value)
#             except json.JSONDecodeError:
#                 pass
#         else:
#             return normalize_pos_cached(pos_value)
    
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
#         if isinstance(pos_value, str):
#             return normalize_pos_cached(pos_value)
    
#     return str(pos_value) if pos_value else 'unknown'


# # =============================================================================
# # OPTIMIZED DATA STRUCTURES
# # =============================================================================

# class WordState:
#     """
#     Efficient state container for a word being processed.
#     Uses __slots__ for memory efficiency and faster attribute access.
#     """
#     __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
#                  'distance', 'metadata', 'correction_reason', 'member')
    
#     def __init__(self, position: int, word: str):
#         self.position = position
#         self.word = word.lower()
#         self.status = 'unknown'
#         self.pos = 'unknown'
#         self.corrected: Optional[str] = None
#         self.distance: int = 0
#         self.metadata: Dict[str, Any] = {}
#         self.correction_reason: Optional[str] = None
#         self.member: Optional[str] = None
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dict for compatibility. O(1)"""
#         result = {
#             'position': self.position,
#             'word': self.word,
#             'status': self.status,
#             'pos': self.pos,
#             'metadata': self.metadata
#         }
#         if self.corrected:
#             result['corrected'] = self.corrected
#             result['distance'] = self.distance
#         if self.correction_reason:
#             result['correction_reason'] = self.correction_reason
#         return result


# class PositionMap:
#     """
#     O(1) position-based lookups for word states.
#     All operations are O(1) dict lookups.
#     """
#     __slots__ = ('_by_position', '_states')
    
#     def __init__(self, states: List[WordState]):
#         self._states = states
#         self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
#     def get_pos(self, position: int) -> Optional[str]:
#         """Get POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state and state.pos in ALLOWED_POS:
#             return state.pos
#         return None
    
#     def get_state(self, position: int) -> Optional[WordState]:
#         """Get WordState at position. O(1)"""
#         return self._by_position.get(position)
    
#     def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
#         """Get (left_pos, right_pos) for a position. O(1)"""
#         return self.get_pos(position - 1), self.get_pos(position + 1)
    
#     def update_pos(self, position: int, new_pos: str) -> None:
#         """Update POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state:
#             state.pos = new_pos
    
#     def get_tuple_array(self) -> List[Tuple[int, str]]:
#         """Get (position, pos) tuples. O(n)"""
#         return [(s.position, s.pos) for s in self._states]
    
#     def __iter__(self):
#         return iter(self._states)
    
#     def __len__(self):
#         return len(self._states)


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """
#     Predict POS based on context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """
#     Get ALL valid POS options for context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def match_sentence_pattern(
#     pos_map: PositionMap,
#     unknown_position: int
# ) -> Optional[str]:
#     """
#     Match sentence against known patterns.
#     Complexity: O(p * s) where p = patterns, s = sequence length (both small)
#     """
#     starting_pos = None
#     starting_position = 0
    
#     for state in pos_map:
#         if state.pos in SENTENCE_PATTERNS:
#             starting_pos = state.pos
#             starting_position = state.position
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [s.pos for s in pos_map if s.position > starting_position]
#     unknown_index = unknown_position - starting_position - 1
    
#     if unknown_index < 0:
#         return None
    
#     for pattern in patterns:
#         if len(pattern) < len(sequence):
#             continue
        
#         matches = True
#         for i, tag in enumerate(sequence):
#             if i >= len(pattern):
#                 break
#             if tag != 'unknown' and tag != pattern[i]:
#                 matches = False
#                 break
        
#         if matches and unknown_index < len(pattern):
#             return pattern[unknown_index]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - O(2) Redis round-trips per call
# # =============================================================================

# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by POS.
#     Complexity: O(2) Redis round-trips (batched candidate check)
#     """
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batched check - O(2) Redis round-trips
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # O(1) cached lookup
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
        
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Complexity: O(2) Redis round-trips
#     """
#     word_lower = word.lower()
    
#     suggestions_result = get_suggestions(word, limit=10)
#     suggestions = suggestions_result.get('suggestions', [])
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = cached_levenshtein(word_lower, term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = cached_levenshtein(word_lower, item.get('term', '').lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     return None


# # =============================================================================
# # PASS 1: VALIDATION - O(1) with pre_validated, O(2) Redis calls otherwise
# # =============================================================================

# def batch_validate_words(
#     words: List[str],
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> List[WordState]:
#     """
#     PASS 1: Validate all words.
    
#     BACKWARDS COMPATIBLE: Works without pre_validated (original behavior)
    
#     Complexity:
#         With pre_validated: O(n) - just dict lookups, NO Redis calls
#         Without pre_validated: O(2) Redis round-trips total
    
#     Args:
#         words: List of words to validate
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         List of WordState objects
#     """
#     if not words:
#         return []
    
#     states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
#     if pre_validated is not None:
#         # O(1) lookup path - build lookup dict
#         validated_lookup: Dict[str, Dict[str, Any]] = {}
        
#         for item in pre_validated:
#             word_key = item.get('word', '').lower()
#             if word_key:
#                 validated_lookup[word_key] = item
        
#         # Process each state with O(1) lookups
#         for state in states:
#             if state.word in validated_lookup:
#                 item = validated_lookup[state.word]
                
#                 if item.get('exists'):
#                     state.status = 'valid'
#                     state.pos = normalize_pos(item.get('pos', item.get('metadata', {}).get('pos', 'unknown')))
#                     state.metadata = item.get('metadata', item)
#                     state.member = item.get('member')
#                 # else: stays as 'unknown', will be corrected in Pass 2
        
#         return states
    
#     # Original path - batch Redis lookup
#     validation_cache = batch_validate_words_redis([s.word for s in states])
    
#     for state in states:
#         if state.word in validation_cache:
#             result = validation_cache[state.word]
            
#             if result.get('is_valid'):
#                 state.status = 'valid'
#                 metadata = result.get('metadata', {})
#                 state.pos = normalize_pos(metadata.get('pos', 'unknown'))
#                 state.metadata = metadata
#                 state.member = result.get('member')
    
#     return states


# # =============================================================================
# # PASS 2: CORRECTION - O(u * 2) Redis round-trips where u = unknowns
# # =============================================================================

# def correct_unknowns(pos_map: PositionMap) -> None:
#     """
#     Correct unknown words using context prediction.
#     Modifies states in place.
    
#     Complexity: O(u * 2) Redis round-trips where u = unknown words
#     """
#     for state in pos_map:
#         if state.status != 'unknown':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) prediction
#         prediction = predict_pos_from_context(left_pos, right_pos)
        
#         if prediction:
#             predicted_pos, confidence = prediction
#         else:
#             predicted_pos = match_sentence_pattern(pos_map, state.position)
#             if not predicted_pos:
#                 predicted_pos = 'noun'
        
#         # O(2) Redis round-trips per unknown
#         correction = search_with_pos_filter(state.word, predicted_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction


# def detect_and_correct_violations(pos_map: PositionMap) -> None:
#     """
#     Detect and correct pattern violations in valid words.
#     Modifies states in place.
    
#     Complexity: O(v * 2) Redis round-trips where v = violations
#     """
#     for state in pos_map:
#         if state.status != 'valid':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) valid options lookup
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_set = {pos for pos, conf in valid_options}
        
#         if state.pos in valid_pos_set:
#             continue
        
#         expected_pos, confidence = valid_options[0]
        
#         # O(2) Redis round-trips per violation
#         correction = search_with_pos_filter(state.word, expected_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#             state.correction_reason = 'pattern_violation'


# def run_pass2(pos_map: PositionMap) -> None:
#     """
#     PASS 2: Correct unknowns and pattern violations.
    
#     Complexity: O((u + v) * 2) Redis round-trips
#     """
#     correct_unknowns(pos_map)
#     detect_and_correct_violations(pos_map)


# # =============================================================================
# # PASS 3: BIGRAM DETECTION - O(2) Redis round-trips total
# # =============================================================================

# def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """
#     PASS 3: Detect bigrams using BATCHED lookup.
    
#     Complexity:
#         OLD: O(n) Redis calls
#         NEW: O(2) Redis round-trips regardless of n
#     """
#     states = list(pos_map)
    
#     if len(states) < 2:
#         return []
    
#     # Collect all consecutive pairs
#     pairs_to_check = []
#     pair_positions = []
    
#     for i in range(len(states) - 1):
#         current = states[i]
#         next_state = states[i + 1]
        
#         word1 = current.corrected or current.word
#         word2 = next_state.corrected or next_state.word
        
#         pairs_to_check.append((word1, word2))
#         pair_positions.append((current.position, next_state.position))
    
#     # BATCHED check - O(2) Redis round-trips for ALL bigrams
#     bigram_results = batch_check_bigrams(pairs_to_check)
    
#     # Process results - O(n)
#     bigrams_found = []
    
#     for i, (word1, word2) in enumerate(pairs_to_check):
#         bigram_key = f"{word1.lower()} {word2.lower()}"
        
#         if bigram_key in bigram_results:
#             metadata = bigram_results[bigram_key]
#             category = metadata.get('category', '')
            
#             bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
            
#             pos_start, pos_end = pair_positions[i]
            
#             bigrams_found.append({
#                 'position_start': pos_start,
#                 'position_end': pos_end,
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'subtext': category,
#                 'entity': metadata.get('entity_type', 'bigram'),
#                 'metadata': metadata
#             })
    
#     return bigrams_found


# # =============================================================================
# # OUTPUT BUILDING - O(n) in-memory operations
# # =============================================================================

# def build_final_results(
#     pos_map: PositionMap,
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Build final merged results with bigrams. O(n)"""
#     if not bigrams:
#         return [s.to_dict() for s in pos_map]
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for state in pos_map:
#         if skip_next:
#             skip_next = False
#             continue
        
#         if state.position in bigram_starts:
#             bigram = bigram_starts[state.position]
#             merged.append({
#                 'position': state.position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif state.position not in bigram_positions:
#             merged.append(state.to_dict())
    
#     return merged


# def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """Build list of corrections made. O(n)"""
#     corrections = []
#     for state in pos_map:
#         if state.status == 'corrected':
#             corrections.append({
#                 'position': state.position,
#                 'original': state.word,
#                 'corrected': state.corrected,
#                 'distance': state.distance,
#                 'pos': state.pos,
#                 'is_bigram': False
#             })
#     return corrections


# def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
#     """Build the corrected query string. O(n)"""
#     words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             words.append(r['word'])
#         elif r.get('corrected'):
#             words.append(r['corrected'])
#         else:
#             words.append(r['word'])
#     return ' '.join(words)


# # =============================================================================
# # MAIN ORCHESTRATOR - BACKWARDS COMPATIBLE
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     # NEW optional parameter - backwards compatible
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     BACKWARDS COMPATIBLE: All original parameters work the same.
    
#     NEW: pre_validated parameter allows passing data from lookup_table()
#          to skip duplicate Redis calls.
    
#     Complexity:
#         With pre_validated: O(n + (u+v)*2 + 2) where n=words, u=unknowns, v=violations
#         Without pre_validated: O(n*k*log(m) + (u+v)*2 + 2)
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, unused but kept for compatibility)
#         prefix: Redis key prefix (unused but kept for compatibility)
#         verbose: Whether to print debug output
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate - O(1) with pre_validated, O(2) otherwise
#     # =========================================================================
#     if verbose:
#         print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
#         print(f"\nPASS 1: Validating words...")
#         if pre_validated:
#             print("   (Using pre-validated data - O(1) lookups)")
    
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
    
#     if verbose:
#         for s in states:
#             status = "✅" if s.status == 'valid' else "❓"
#             print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
#     # =========================================================================
#     # PASS 2: Correct unknowns and violations - O((u+v) * 2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 2: Correcting unknowns and violations...")
    
#     run_pass2(pos_map)
    
#     if verbose:
#         for s in states:
#             if s.status == 'corrected':
#                 reason = f" ({s.correction_reason})" if s.correction_reason else ""
#                 print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos}){reason}")
    
#     # =========================================================================
#     # PASS 3: Detect bigrams - O(2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 3: Detecting bigrams...")
    
#     bigrams = detect_bigrams(pos_map)
    
#     if verbose:
#         for b in bigrams:
#             print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
#     # =========================================================================
#     # BUILD OUTPUT - O(n)
#     # =========================================================================
#     final_results = build_final_results(pos_map, bigrams)
#     corrections = build_corrections_list(pos_map)
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
#     corrected_query = build_corrected_query(final_results)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 RESULT: '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
#         print(f"{'='*60}\n")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # NEW: FULL PROCESSING WITH CATEGORIZED TERMS FOR SEARCH INTEGRATION
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for Typesense search integration.
    
#     Returns terms separated by status for filtering strategy:
#     - valid_terms: Use for STRICT Typesense filter
#     - unknown_terms: Use for LOOSE search or embedding
#     - corrected_terms: Misspellings that were fixed
#     - bigram_terms: Multi-word entities
    
#     Args:
#         query: The input query
#         verbose: Print debug output
#         pre_validated: Optional pre-computed validation
    
#     Returns:
#         Dict with categorized terms for search strategy
#     """
#     words = query.split()
    
#     if not words:
#         return {
#             'success': True,
#             'query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': [],
#             'corrected_query': '',
#             'has_unknown': False,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': 0,
#             'total_count': 0
#         }
    
#     # Run the three passes
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
#     run_pass2(pos_map)
#     bigrams = detect_bigrams(pos_map)
    
#     # Build final results
#     final_results = build_final_results(pos_map, bigrams)
#     corrected_query = build_corrected_query(final_results)
    
#     # Categorize terms for search
#     valid_terms = []
#     unknown_terms = []
#     corrected_terms = []
#     bigram_terms = []
#     search_terms = []  # The actual terms to search with
    
#     for term in final_results:
#         status = term.get('status', 'unknown')
#         word = term.get('word', '')
#         corrected = term.get('corrected')
#         pos = term.get('pos', 'unknown')
        
#         # Determine the search term (corrected version if available)
#         search_word = corrected if corrected else word
        
#         term_info = {
#             'word': word,
#             'search_word': search_word,
#             'pos': pos,
#             'status': status,
#             'metadata': term.get('metadata', {})
#         }
        
#         if status == 'valid':
#             valid_terms.append(term_info)
#             search_terms.append(search_word)
#         elif status == 'corrected':
#             corrected_terms.append({
#                 **term_info,
#                 'original': word,
#                 'corrected': corrected
#             })
#             search_terms.append(search_word)
#             # Corrected words are now "valid" for filtering
#             valid_terms.append(term_info)
#         elif status == 'bigram':
#             bigram_terms.append(term_info)
#             search_terms.append(search_word)
#             # Bigrams are valid entities
#             valid_terms.append(term_info)
#         else:  # unknown
#             unknown_terms.append(term_info)
#             search_terms.append(search_word)  # Still include in search
    
#     # Calculate flags for search strategy
#     total_terms = len(final_results)
#     unknown_count = len(unknown_terms)
#     valid_count = len(valid_terms)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({valid_count}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({unknown_count}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(t['original'], t['corrected']) for t in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': final_results,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         # Flags for search strategy
#         'has_unknown': unknown_count > 0,
#         'all_unknown': unknown_count == total_terms and total_terms > 0,
#         'valid_count': valid_count,
#         'unknown_count': unknown_count,
#         'total_count': total_terms
#     }


# # =============================================================================
# # CONVENIENCE WRAPPER FOR OPTIMIZED FLOW
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     Combines lookup_table + word_discovery in optimal way:
#     - Single validation pass (no duplicate Redis calls)
#     - Batched bigram detection
#     - Categorized output for search strategy
    
#     Complexity: O(2 + (u+v)*2 + 2) = O(4 + 2*(u+v))
    
#     Args:
#         query: The input query
#         verbose: Print debug output
    
#     Returns:
#         Dict with:
#         - valid_terms: For strict Typesense filtering
#         - unknown_terms: For loose search / embedding
#         - search_terms: Final terms to use
#         - corrected_query: The corrected query string
#         - search_strategy: 'strict', 'mixed', or 'semantic'
#     """
#     from .searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         # Return basic structure even on failure
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Full word discovery with categorized output
#     result = word_discovery_full(
#         query,
#         verbose=verbose,
#         pre_validated=lookup_result.get('terms', [])
#     )
    
#     # Step 3: Determine search strategy
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     if verbose:
#         print(f"   Search strategy: {result['search_strategy'].upper()}")
    
#     return result


# # =============================================================================
# # HELPER FUNCTIONS FOR TYPESENSE INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """
#     Determine the best search strategy based on word discovery results.
    
#     Args:
#         discovery_result: Output from process_query_optimized() or word_discovery_full()
    
#     Returns:
#         'strict' - Use keyword filter (2+ valid terms)
#         'mixed' - One filter + loose search (1 valid term)
#         'semantic' - Use embedding search (0 valid terms)
#     """
#     valid_count = discovery_result.get('valid_count', 0)
    
#     if valid_count >= 2:
#         return 'strict'
#     elif valid_count == 1:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get the terms to use for strict Typesense filtering.
    
#     These are valid/corrected/bigram terms that exist in your dictionary.
#     Use these to FILTER results before semantic ranking.
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of terms (search_word) that are valid/corrected/bigram
#     """
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get the terms that are unknown (for loose search or embedding).
    
#     These terms are NOT in your dictionary, so:
#     - Don't use them for strict filtering
#     - Include them in the general search query
#     - Let embedding handle semantic matching
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of unknown terms
#     """
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get all terms to include in the search query.
    
#     This includes both valid and unknown terms, with corrections applied.
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of all search terms
#     """
#     return discovery_result.get('search_terms', [])

# """
# word_discovery_optimized.py
# Optimized three-pass word validation, correction, and bigram detection.

# BACKWARDS COMPATIBLE: All existing function signatures work unchanged.

# Key optimizations:
# 1. Pre-validated data passthrough (eliminates duplicate Redis calls)
# 2. Batched Redis operations (single pipeline per operation type)
# 3. O(1) position lookups using PositionMap
# 4. O(1) context rule lookups using dict keys
# 5. Cached Levenshtein distance calculations
# 6. Lazy evaluation where possible

# Complexity improvements:
# - Validation: O(n * k * log m) -> O(1) with pre_validated
# - Bigram detection: O(n) Redis calls -> O(2) Redis round-trips
# - Context lookup: O(1) via dict
# - Correction search: O(c * k * log m) -> O(2) Redis round-trips per unknown
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Import Redis functions - these now support batching
# from searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata,
#     get_suggestions,
#     generate_candidates_smart,
#     batch_check_candidates,
#     batch_validate_words_redis,
#     batch_check_bigrams,
#     batch_get_term_metadata,
#     damerau_levenshtein_distance as _python_levenshtein
# )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }

# SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
#     "determiner": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
#     ],
#     "article": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#     ],
#     "pronoun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"), ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
#     ],
#     "noun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"), ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
#     ],
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
#         ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
#     ],
#     "verb": [
#         ("noun",), ("adverb",), ("adjective",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
#     "preposition": [
#         ("noun",), ("proper_noun",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
#     "adverb": [
#         ("verb",), ("adjective",), ("adverb",),
#         ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
#     ],
#     "be": [
#         ("adjective",), ("noun",),
#         ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
#         ("adverb", "adjective"), ("determiner", "adjective", "noun"),
#     ],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """
#     Cached Levenshtein distance calculation.
#     Complexity: O(1) for cached values, O(m*n) for new calculations
#     """
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# @lru_cache(maxsize=1000)
# def normalize_pos_cached(pos_value: str) -> str:
#     """
#     Cached POS normalization for string inputs.
#     Complexity: O(1) dict lookup
#     """
#     if pos_value in LOCATION_TYPES:
#         return 'proper_noun'
#     if pos_value in COMPOUND_NOUN_TYPES:
#         return 'noun'
#     return pos_value


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value, converting location types to proper_noun.
#     Complexity: O(1) for cached strings, O(p) for parsing where p = parts
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     if isinstance(pos_value, str):
#         if pos_value.startswith('['):
#             try:
#                 pos_value = json.loads(pos_value)
#             except json.JSONDecodeError:
#                 pass
#         else:
#             return normalize_pos_cached(pos_value)
    
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
#         if isinstance(pos_value, str):
#             return normalize_pos_cached(pos_value)
    
#     return str(pos_value) if pos_value else 'unknown'


# # =============================================================================
# # OPTIMIZED DATA STRUCTURES
# # =============================================================================

# class WordState:
#     """
#     Efficient state container for a word being processed.
#     Uses __slots__ for memory efficiency and faster attribute access.
#     """
#     __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
#                  'distance', 'metadata', 'correction_reason', 'member')
    
#     def __init__(self, position: int, word: str):
#         self.position = position
#         self.word = word.lower()
#         self.status = 'unknown'
#         self.pos = 'unknown'
#         self.corrected: Optional[str] = None
#         self.distance: int = 0
#         self.metadata: Dict[str, Any] = {}
#         self.correction_reason: Optional[str] = None
#         self.member: Optional[str] = None
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dict for compatibility. O(1)"""
#         result = {
#             'position': self.position,
#             'word': self.word,
#             'status': self.status,
#             'pos': self.pos,
#             'metadata': self.metadata
#         }
#         if self.corrected:
#             result['corrected'] = self.corrected
#             result['distance'] = self.distance
#         if self.correction_reason:
#             result['correction_reason'] = self.correction_reason
#         return result


# class PositionMap:
#     """
#     O(1) position-based lookups for word states.
#     All operations are O(1) dict lookups.
#     """
#     __slots__ = ('_by_position', '_states')
    
#     def __init__(self, states: List[WordState]):
#         self._states = states
#         self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
#     def get_pos(self, position: int) -> Optional[str]:
#         """Get POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state and state.pos in ALLOWED_POS:
#             return state.pos
#         return None
    
#     def get_state(self, position: int) -> Optional[WordState]:
#         """Get WordState at position. O(1)"""
#         return self._by_position.get(position)
    
#     def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
#         """Get (left_pos, right_pos) for a position. O(1)"""
#         return self.get_pos(position - 1), self.get_pos(position + 1)
    
#     def update_pos(self, position: int, new_pos: str) -> None:
#         """Update POS at position. O(1)"""
#         state = self._by_position.get(position)
#         if state:
#             state.pos = new_pos
    
#     def get_tuple_array(self) -> List[Tuple[int, str]]:
#         """Get (position, pos) tuples. O(n)"""
#         return [(s.position, s.pos) for s in self._states]
    
#     def __iter__(self):
#         return iter(self._states)
    
#     def __len__(self):
#         return len(self._states)


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """
#     Predict POS based on context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """
#     Get ALL valid POS options for context.
#     Complexity: O(1) - max 3 dict lookups
#     """
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def match_sentence_pattern(
#     pos_map: PositionMap,
#     unknown_position: int
# ) -> Optional[str]:
#     """
#     Match sentence against known patterns.
#     Complexity: O(p * s) where p = patterns, s = sequence length (both small)
#     """
#     starting_pos = None
#     starting_position = 0
    
#     for state in pos_map:
#         if state.pos in SENTENCE_PATTERNS:
#             starting_pos = state.pos
#             starting_position = state.position
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [s.pos for s in pos_map if s.position > starting_position]
#     unknown_index = unknown_position - starting_position - 1
    
#     if unknown_index < 0:
#         return None
    
#     for pattern in patterns:
#         if len(pattern) < len(sequence):
#             continue
        
#         matches = True
#         for i, tag in enumerate(sequence):
#             if i >= len(pattern):
#                 break
#             if tag != 'unknown' and tag != pattern[i]:
#                 matches = False
#                 break
        
#         if matches and unknown_index < len(pattern):
#             return pattern[unknown_index]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - O(2) Redis round-trips per call
# # =============================================================================

# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by POS.
#     Complexity: O(2) Redis round-trips (batched candidate check)
#     """
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batched check - O(2) Redis round-trips
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # O(1) cached lookup
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
        
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Complexity: O(2) Redis round-trips
#     """
#     word_lower = word.lower()
    
#     suggestions_result = get_suggestions(word, limit=10)
#     suggestions = suggestions_result.get('suggestions', [])
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = cached_levenshtein(word_lower, term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = cached_levenshtein(word_lower, item.get('term', '').lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     return None


# # =============================================================================
# # PASS 1: VALIDATION - O(1) with pre_validated, O(2) Redis calls otherwise
# # =============================================================================

# def batch_validate_words(
#     words: List[str],
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> List[WordState]:
#     """
#     PASS 1: Validate all words.
    
#     BACKWARDS COMPATIBLE: Works without pre_validated (original behavior)
    
#     Complexity:
#         With pre_validated: O(n) - just dict lookups, NO Redis calls
#         Without pre_validated: O(2) Redis round-trips total
    
#     Args:
#         words: List of words to validate
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         List of WordState objects
#     """
#     if not words:
#         return []
    
#     states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
#     if pre_validated is not None:
#         # O(1) lookup path - build lookup dict
#         validated_lookup: Dict[str, Dict[str, Any]] = {}
        
#         for item in pre_validated:
#             word_key = item.get('word', '').lower()
#             if word_key:
#                 validated_lookup[word_key] = item
        
#         # Process each state with O(1) lookups
#         for state in states:
#             if state.word in validated_lookup:
#                 item = validated_lookup[state.word]
                
#                 if item.get('exists'):
#                     state.status = 'valid'
#                     state.pos = normalize_pos(item.get('pos', item.get('metadata', {}).get('pos', 'unknown')))
#                     state.metadata = item.get('metadata', item)
#                     state.member = item.get('member')
#                 # else: stays as 'unknown', will be corrected in Pass 2
        
#         return states
    
#     # Original path - batch Redis lookup
#     validation_cache = batch_validate_words_redis([s.word for s in states])
    
#     for state in states:
#         if state.word in validation_cache:
#             result = validation_cache[state.word]
            
#             if result.get('is_valid'):
#                 state.status = 'valid'
#                 metadata = result.get('metadata', {})
#                 state.pos = normalize_pos(metadata.get('pos', 'unknown'))
#                 state.metadata = metadata
#                 state.member = result.get('member')
    
#     return states


# # =============================================================================
# # PASS 2: CORRECTION - O(u * 2) Redis round-trips where u = unknowns
# # =============================================================================

# def correct_unknowns(pos_map: PositionMap) -> None:
#     """
#     Correct unknown words using context prediction.
#     Modifies states in place.
    
#     Complexity: O(u * 2) Redis round-trips where u = unknown words
#     """
#     for state in pos_map:
#         if state.status != 'unknown':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) prediction
#         prediction = predict_pos_from_context(left_pos, right_pos)
        
#         if prediction:
#             predicted_pos, confidence = prediction
#         else:
#             predicted_pos = match_sentence_pattern(pos_map, state.position)
#             if not predicted_pos:
#                 predicted_pos = 'noun'
        
#         # O(2) Redis round-trips per unknown
#         correction = search_with_pos_filter(state.word, predicted_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction


# def detect_and_correct_violations(pos_map: PositionMap) -> None:
#     """
#     Detect and correct pattern violations in valid words.
#     Modifies states in place.
    
#     Complexity: O(v * 2) Redis round-trips where v = violations
#     """
#     for state in pos_map:
#         if state.status != 'valid':
#             continue
        
#         # O(1) context lookup
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # O(1) valid options lookup
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_set = {pos for pos, conf in valid_options}
        
#         if state.pos in valid_pos_set:
#             continue
        
#         expected_pos, confidence = valid_options[0]
        
#         # O(2) Redis round-trips per violation
#         correction = search_with_pos_filter(state.word, expected_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#             state.correction_reason = 'pattern_violation'


# def run_pass2(pos_map: PositionMap) -> None:
#     """
#     PASS 2: Correct unknowns and pattern violations.
    
#     Complexity: O((u + v) * 2) Redis round-trips
#     """
#     correct_unknowns(pos_map)
#     detect_and_correct_violations(pos_map)


# # =============================================================================
# # PASS 3: BIGRAM DETECTION - O(2) Redis round-trips total
# # =============================================================================

# def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """
#     PASS 3: Detect bigrams using BATCHED lookup.
    
#     Complexity:
#         OLD: O(n) Redis calls
#         NEW: O(2) Redis round-trips regardless of n
#     """
#     states = list(pos_map)
    
#     if len(states) < 2:
#         return []
    
#     # Collect all consecutive pairs
#     pairs_to_check = []
#     pair_positions = []
    
#     for i in range(len(states) - 1):
#         current = states[i]
#         next_state = states[i + 1]
        
#         word1 = current.corrected or current.word
#         word2 = next_state.corrected or next_state.word
        
#         pairs_to_check.append((word1, word2))
#         pair_positions.append((current.position, next_state.position))
    
#     # BATCHED check - O(2) Redis round-trips for ALL bigrams
#     bigram_results = batch_check_bigrams(pairs_to_check)
    
#     # Process results - O(n)
#     bigrams_found = []
    
#     for i, (word1, word2) in enumerate(pairs_to_check):
#         bigram_key = f"{word1.lower()} {word2.lower()}"
        
#         if bigram_key in bigram_results:
#             metadata = bigram_results[bigram_key]
#             category = metadata.get('category', '')
            
#             bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
            
#             pos_start, pos_end = pair_positions[i]
            
#             bigrams_found.append({
#                 'position_start': pos_start,
#                 'position_end': pos_end,
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'subtext': category,
#                 'entity': metadata.get('entity_type', 'bigram'),
#                 'metadata': metadata
#             })
    
#     return bigrams_found


# # =============================================================================
# # OUTPUT BUILDING - O(n) in-memory operations
# # =============================================================================

# def build_final_results(
#     pos_map: PositionMap,
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Build final merged results with bigrams. O(n)"""
#     if not bigrams:
#         return [s.to_dict() for s in pos_map]
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for state in pos_map:
#         if skip_next:
#             skip_next = False
#             continue
        
#         if state.position in bigram_starts:
#             bigram = bigram_starts[state.position]
#             merged.append({
#                 'position': state.position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif state.position not in bigram_positions:
#             merged.append(state.to_dict())
    
#     return merged


# def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """Build list of corrections made. O(n)"""
#     corrections = []
#     for state in pos_map:
#         if state.status == 'corrected':
#             corrections.append({
#                 'position': state.position,
#                 'original': state.word,
#                 'corrected': state.corrected,
#                 'distance': state.distance,
#                 'pos': state.pos,
#                 'is_bigram': False
#             })
#     return corrections


# def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
#     """Build the corrected query string. O(n)"""
#     words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             words.append(r['word'])
#         elif r.get('corrected'):
#             words.append(r['corrected'])
#         else:
#             words.append(r['word'])
#     return ' '.join(words)


# # =============================================================================
# # MAIN ORCHESTRATOR - BACKWARDS COMPATIBLE
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     # NEW optional parameter - backwards compatible
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     BACKWARDS COMPATIBLE: All original parameters work the same.
    
#     NEW: pre_validated parameter allows passing data from lookup_table()
#          to skip duplicate Redis calls.
    
#     Complexity:
#         With pre_validated: O(n + (u+v)*2 + 2) where n=words, u=unknowns, v=violations
#         Without pre_validated: O(n*k*log(m) + (u+v)*2 + 2)
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, unused but kept for compatibility)
#         prefix: Redis key prefix (unused but kept for compatibility)
#         verbose: Whether to print debug output
#         pre_validated: Optional pre-computed validation from lookup_table()
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate - O(1) with pre_validated, O(2) otherwise
#     # =========================================================================
#     if verbose:
#         print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
#         print(f"\nPASS 1: Validating words...")
#         if pre_validated:
#             print("   (Using pre-validated data - O(1) lookups)")
    
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
    
#     if verbose:
#         for s in states:
#             status = "✅" if s.status == 'valid' else "❓"
#             print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
#     # =========================================================================
#     # PASS 2: Correct unknowns and violations - O((u+v) * 2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 2: Correcting unknowns and violations...")
    
#     run_pass2(pos_map)
    
#     if verbose:
#         for s in states:
#             if s.status == 'corrected':
#                 reason = f" ({s.correction_reason})" if s.correction_reason else ""
#                 print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos}){reason}")
    
#     # =========================================================================
#     # PASS 3: Detect bigrams - O(2)
#     # =========================================================================
#     if verbose:
#         print("\nPASS 3: Detecting bigrams...")
    
#     bigrams = detect_bigrams(pos_map)
    
#     if verbose:
#         for b in bigrams:
#             print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
#     # =========================================================================
#     # BUILD OUTPUT - O(n)
#     # =========================================================================
#     final_results = build_final_results(pos_map, bigrams)
#     corrections = build_corrections_list(pos_map)
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
#     corrected_query = build_corrected_query(final_results)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 RESULT: '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
#         print(f"{'='*60}\n")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # NEW: FULL PROCESSING WITH CATEGORIZED TERMS FOR SEARCH INTEGRATION
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for Typesense search integration.
    
#     Returns terms separated by status for filtering strategy:
#     - valid_terms: Use for STRICT Typesense filter
#     - unknown_terms: Use for LOOSE search or embedding
#     - corrected_terms: Misspellings that were fixed
#     - bigram_terms: Multi-word entities
    
#     Args:
#         query: The input query
#         verbose: Print debug output
#         pre_validated: Optional pre-computed validation
    
#     Returns:
#         Dict with categorized terms for search strategy
#     """
#     words = query.split()
    
#     if not words:
#         return {
#             'success': True,
#             'query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': [],
#             'corrected_query': '',
#             'has_unknown': False,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': 0,
#             'total_count': 0
#         }
    
#     # Run the three passes
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
#     run_pass2(pos_map)
#     bigrams = detect_bigrams(pos_map)
    
#     # Build final results
#     final_results = build_final_results(pos_map, bigrams)
#     corrected_query = build_corrected_query(final_results)
    
#     # Categorize terms for search
#     valid_terms = []
#     unknown_terms = []
#     corrected_terms = []
#     bigram_terms = []
#     search_terms = []  # The actual terms to search with
    
#     for term in final_results:
#         status = term.get('status', 'unknown')
#         word = term.get('word', '')
#         corrected = term.get('corrected')
#         pos = term.get('pos', 'unknown')
        
#         # Determine the search term (corrected version if available)
#         search_word = corrected if corrected else word
        
#         term_info = {
#             'word': word,
#             'search_word': search_word,
#             'pos': pos,
#             'status': status,
#             'metadata': term.get('metadata', {})
#         }
        
#         if status == 'valid':
#             valid_terms.append(term_info)
#             search_terms.append(search_word)
#         elif status == 'corrected':
#             corrected_terms.append({
#                 **term_info,
#                 'original': word,
#                 'corrected': corrected
#             })
#             search_terms.append(search_word)
#             # Corrected words are now "valid" for filtering
#             valid_terms.append(term_info)
#         elif status == 'bigram':
#             bigram_terms.append(term_info)
#             search_terms.append(search_word)
#             # Bigrams are valid entities
#             valid_terms.append(term_info)
#         else:  # unknown
#             unknown_terms.append(term_info)
#             search_terms.append(search_word)  # Still include in search
    
#     # Calculate flags for search strategy
#     total_terms = len(final_results)
#     unknown_count = len(unknown_terms)
#     valid_count = len(valid_terms)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({valid_count}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({unknown_count}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(t['original'], t['corrected']) for t in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': final_results,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         # Flags for search strategy
#         'has_unknown': unknown_count > 0,
#         'all_unknown': unknown_count == total_terms and total_terms > 0,
#         'valid_count': valid_count,
#         'unknown_count': unknown_count,
#         'total_count': total_terms
#     }


# # =============================================================================
# # CONVENIENCE WRAPPER FOR OPTIMIZED FLOW
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     Combines lookup_table + word_discovery in optimal way:
#     - Single validation pass (no duplicate Redis calls)
#     - Batched bigram detection
#     - Categorized output for search strategy
    
#     Complexity: O(2 + (u+v)*2 + 2) = O(4 + 2*(u+v))
    
#     Args:
#         query: The input query
#         verbose: Print debug output
    
#     Returns:
#         Dict with:
#         - valid_terms: For strict Typesense filtering
#         - unknown_terms: For loose search / embedding
#         - search_terms: Final terms to use
#         - corrected_query: The corrected query string
#         - search_strategy: 'strict', 'mixed', or 'semantic'
#     """
#     from .searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         # Return basic structure even on failure
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Full word discovery with categorized output
#     result = word_discovery_full(
#         query,
#         verbose=verbose,
#         pre_validated=lookup_result.get('terms', [])
#     )
    
#     # Step 3: Determine search strategy
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     if verbose:
#         print(f"   Search strategy: {result['search_strategy'].upper()}")
    
#     return result


# # =============================================================================
# # HELPER FUNCTIONS FOR TYPESENSE INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """
#     Determine the best search strategy based on word discovery results.
    
#     Args:
#         discovery_result: Output from process_query_optimized() or word_discovery_full()
    
#     Returns:
#         'strict' - Use keyword filter (2+ valid terms)
#         'mixed' - One filter + loose search (1 valid term)
#         'semantic' - Use embedding search (0 valid terms)
#     """
#     valid_count = discovery_result.get('valid_count', 0)
    
#     if valid_count >= 2:
#         return 'strict'
#     elif valid_count == 1:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get the terms to use for strict Typesense filtering.
    
#     These are valid/corrected/bigram terms that exist in your dictionary.
#     Use these to FILTER results before semantic ranking.
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of terms (search_word) that are valid/corrected/bigram
#     """
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get the terms that are unknown (for loose search or embedding).
    
#     These terms are NOT in your dictionary, so:
#     - Don't use them for strict filtering
#     - Include them in the general search query
#     - Let embedding handle semantic matching
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of unknown terms
#     """
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """
#     Get all terms to include in the search query.
    
#     This includes both valid and unknown terms, with corrections applied.
    
#     Args:
#         discovery_result: Output from process_query_optimized()
    
#     Returns:
#         List of all search terms
#     """
#     return discovery_result.get('search_terms', [])

# """
# word_discovery.py
# Three-pass word validation, correction, and bigram detection.

# Pass 1: Validate words against Redis dictionary
# Pass 2: Correct unknown/misspelled words using POS context patterns
# Pass 3: Detect bigrams (multi-word entities)
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Handle both relative and absolute imports
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )
# except ImportError:
#     from searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country", "us_city", "us_state"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
#     ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
#     (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
# }

# SENTENCE_PATTERNS: Dict[str, List[Tuple[str, ...]]] = {
#     "determiner": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"), ("noun", "noun"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"), ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"), ("noun", "verb", "adjective", "noun"),
#     ],
#     "article": [
#         ("noun",), ("adjective",),
#         ("adjective", "noun"), ("noun", "verb"),
#         ("adjective", "noun", "verb"), ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"), ("noun", "be", "noun"),
#     ],
#     "pronoun": [
#         ("verb",), ("be",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"), ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"), ("be", "determiner", "adjective", "noun"),
#     ],
#     "noun": [
#         ("verb",), ("be",), ("conjunction",),
#         ("verb", "noun"), ("verb", "adverb"), ("verb", "adjective"),
#         ("be", "adjective"), ("be", "noun"),
#         ("conjunction", "noun"), ("conjunction", "adjective"),
#         ("verb", "determiner", "noun"), ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"), ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"), ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"), ("verb", "preposition", "determiner", "noun"),
#     ],
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"), ("noun", "be"), ("adjective", "noun"),
#         ("noun", "verb", "adverb"), ("noun", "be", "adjective"),
#         ("noun", "be", "noun"), ("noun", "verb", "noun"), ("adjective", "noun", "verb"),
#     ],
#     "verb": [
#         ("noun",), ("adverb",), ("adjective",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("preposition", "noun"), ("adverb", "adverb"), ("noun", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"), ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
#     "preposition": [
#         ("noun",), ("proper_noun",),
#         ("determiner", "noun"), ("article", "noun"), ("adjective", "noun"),
#         ("determiner", "adjective", "noun"), ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
#     "adverb": [
#         ("verb",), ("adjective",), ("adverb",),
#         ("verb", "noun"), ("verb", "determiner", "noun"), ("adjective", "noun"),
#     ],
#     "be": [
#         ("adjective",), ("noun",),
#         ("determiner", "noun"), ("article", "noun"), ("preposition", "noun"),
#         ("adverb", "adjective"), ("determiner", "adjective", "noun"),
#     ],
#     "conjunction": [
#         ("noun",), ("pronoun",), ("determiner",),
#         ("noun", "verb"), ("pronoun", "verb"),
#         ("determiner", "noun"), ("adjective", "noun"),
#     ],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value to a simple string.
    
#     Handles:
#     - None -> 'unknown'
#     - "['noun']" (string) -> 'noun'
#     - ['noun'] (list) -> 'noun'
#     - 'noun' (string) -> 'noun'
#     - Location categories -> 'proper_noun'
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         pos_value = pos_value.strip()
#         if pos_value.startswith('[') and pos_value.endswith(']'):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 if isinstance(parsed, list) and parsed:
#                     pos_value = parsed[0]
#                 else:
#                     pos_value = 'unknown'
#             except (json.JSONDecodeError, ValueError):
#                 # Try manual parsing: "['noun']" -> "noun"
#                 inner = pos_value[1:-1].strip()
#                 if inner.startswith("'") and inner.endswith("'"):
#                     pos_value = inner[1:-1]
#                 elif inner.startswith('"') and inner.endswith('"'):
#                     pos_value = inner[1:-1]
#                 else:
#                     pos_value = inner
    
#     # Handle actual list: ['noun']
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Ensure it's a string
#     if not isinstance(pos_value, str):
#         pos_value = str(pos_value) if pos_value else 'unknown'
    
#     # Normalize location types to proper_noun
#     pos_lower = pos_value.lower().strip()
    
#     if pos_lower in ('proper noun', 'proper_noun'):
#         return 'proper_noun'
    
#     if pos_lower in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     # Return as-is if it's a valid POS
#     if pos_lower in ALLOWED_POS:
#         return pos_lower
    
#     return pos_value.lower() if pos_value else 'unknown'


# # =============================================================================
# # OPTIMIZED DATA STRUCTURES
# # =============================================================================

# class WordState:
#     """Efficient state container for a word being processed."""
#     __slots__ = ('position', 'word', 'status', 'pos', 'corrected', 
#                  'distance', 'metadata', 'correction_reason', 'member')
    
#     def __init__(self, position: int, word: str):
#         self.position = position
#         self.word = word.lower()
#         self.status = 'unknown'
#         self.pos = 'unknown'
#         self.corrected: Optional[str] = None
#         self.distance: int = 0
#         self.metadata: Dict[str, Any] = {}
#         self.correction_reason: Optional[str] = None
#         self.member: Optional[str] = None
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dict for compatibility."""
#         result = {
#             'position': self.position,
#             'word': self.word,
#             'status': self.status,
#             'pos': self.pos,
#             'metadata': self.metadata
#         }
#         if self.corrected:
#             result['corrected'] = self.corrected
#             result['distance'] = self.distance
#         if self.correction_reason:
#             result['correction_reason'] = self.correction_reason
#         return result


# class PositionMap:
#     """O(1) position-based lookups for word states."""
#     __slots__ = ('_by_position', '_states')
    
#     def __init__(self, states: List[WordState]):
#         self._states = states
#         self._by_position: Dict[int, WordState] = {s.position: s for s in states}
    
#     def get_pos(self, position: int) -> Optional[str]:
#         """Get POS at position. Returns None if not a valid/recognized POS."""
#         state = self._by_position.get(position)
#         if state and state.pos and state.pos != 'unknown':
#             # Check if it's a recognized POS
#             if state.pos in ALLOWED_POS:
#                 return state.pos
#         return None
    
#     def get_state(self, position: int) -> Optional[WordState]:
#         """Get WordState at position."""
#         return self._by_position.get(position)
    
#     def get_context(self, position: int) -> Tuple[Optional[str], Optional[str]]:
#         """Get (left_pos, right_pos) for a position."""
#         return self.get_pos(position - 1), self.get_pos(position + 1)
    
#     def update_pos(self, position: int, new_pos: str) -> None:
#         """Update POS at position."""
#         state = self._by_position.get(position)
#         if state:
#             state.pos = new_pos
    
#     def get_tuple_array(self) -> List[Tuple[int, str]]:
#         """Get (position, pos) tuples."""
#         return [(s.position, s.pos) for s in self._states]
    
#     def __iter__(self):
#         return iter(self._states)
    
#     def __len__(self):
#         return len(self._states)


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on neighboring words' POS tags."""
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """Get ALL valid POS options for a given context."""
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def match_sentence_pattern(
#     pos_map: PositionMap,
#     unknown_position: int
# ) -> Optional[str]:
#     """Match sentence against known patterns to predict POS."""
#     starting_pos = None
#     starting_position = 0
    
#     for state in pos_map:
#         if state.pos in SENTENCE_PATTERNS:
#             starting_pos = state.pos
#             starting_position = state.position
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [s.pos for s in pos_map if s.position > starting_position]
#     unknown_index = unknown_position - starting_position - 1
    
#     if unknown_index < 0:
#         return None
    
#     for pattern in patterns:
#         if len(pattern) < len(sequence):
#             continue
        
#         matches = True
#         for i, tag in enumerate(sequence):
#             if i >= len(pattern):
#                 break
#             if tag != 'unknown' and tag != pattern[i]:
#                 matches = False
#                 break
        
#         if matches and unknown_index < len(pattern):
#             return pattern[unknown_index]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - Find corrections filtered by POS
# # =============================================================================

# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by required POS.
    
#     1. Generate spelling candidates
#     2. Check which exist in Redis
#     3. Filter by matching POS
#     4. Return closest match
#     """
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Check which candidates exist in Redis
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # Calculate distance
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
#         item['normalized_pos'] = item_pos
        
#         # Check if POS matches what we need
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     # Prefer POS matches, fall back to any match
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     # Sort by distance first, then by rank (higher rank = better)
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """Search for corrections without POS filtering."""
#     word_lower = word.lower()
    
#     # Try suggestions first
#     suggestions_result = get_suggestions(word, limit=10)
#     suggestions = suggestions_result.get('suggestions', [])
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = cached_levenshtein(word_lower, term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     # Try generated candidates
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = cached_levenshtein(word_lower, item.get('term', '').lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             return matches[0]
    
#     return None


# # =============================================================================
# # PASS 1: VALIDATION - Check each word against Redis
# # =============================================================================

# def batch_validate_words(
#     words: List[str],
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> List[WordState]:
#     """
#     PASS 1: Validate all words against Redis dictionary.
    
#     Args:
#         words: List of words to validate
#         pre_validated: Optional pre-computed validation data
    
#     Returns:
#         List of WordState objects with status='valid' or status='unknown'
#     """
#     if not words:
#         return []
    
#     states = [WordState(i + 1, word) for i, word in enumerate(words)]
    
#     # If we have pre-validated data, use it (O(1) lookups)
#     if pre_validated is not None:
#         validated_lookup: Dict[str, Dict[str, Any]] = {}
        
#         for item in pre_validated:
#             word_key = item.get('word', '').lower()
#             if word_key:
#                 validated_lookup[word_key] = item
        
#         for state in states:
#             if state.word in validated_lookup:
#                 item = validated_lookup[state.word]
                
#                 if item.get('exists'):
#                     state.status = 'valid'
#                     raw_pos = item.get('pos') or item.get('metadata', {}).get('pos', 'unknown')
#                     state.pos = normalize_pos(raw_pos)
#                     state.metadata = item.get('metadata', item)
#                     state.member = item.get('member')
        
#         return states
    
#     # Otherwise, batch validate via Redis
#     validation_cache = batch_validate_words_redis([s.word for s in states])
    
#     for state in states:
#         if state.word in validation_cache:
#             result = validation_cache[state.word]
            
#             if result.get('is_valid'):
#                 state.status = 'valid'
#                 metadata = result.get('metadata', {})
#                 raw_pos = metadata.get('pos', 'unknown')
#                 state.pos = normalize_pos(raw_pos)
#                 state.metadata = metadata
#                 state.member = result.get('member')
    
#     return states


# # =============================================================================
# # PASS 2: CORRECTION - Fix unknown words using POS context
# # =============================================================================

# def correct_unknowns(pos_map: PositionMap) -> None:
#     """
#     Correct unknown words using context-based POS prediction.
    
#     For each unknown word:
#     1. Look at neighbors' POS tags
#     2. Predict what POS this word should be
#     3. Find closest spelling match with that POS
#     """
#     for state in pos_map:
#         if state.status != 'unknown':
#             continue
        
#         # Get POS of neighboring words
#         left_pos, right_pos = pos_map.get_context(state.position)
        
#         # Predict required POS based on context
#         prediction = predict_pos_from_context(left_pos, right_pos)
        
#         if prediction:
#             predicted_pos, confidence = prediction
#         else:
#             # Try sentence pattern matching
#             predicted_pos = match_sentence_pattern(pos_map, state.position)
#             if not predicted_pos:
#                 predicted_pos = 'noun'  # Default fallback
        
#         # Search for correction with required POS
#         correction = search_with_pos_filter(state.word, predicted_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#         else:
#             # Try without POS filter as last resort
#             correction = search_without_pos_filter(state.word)
#             if correction:
#                 state.status = 'corrected'
#                 state.corrected = correction['term']
#                 state.pos = normalize_pos(correction.get('pos', 'unknown'))
#                 state.distance = correction['distance']
#                 state.metadata = correction


# def detect_and_correct_violations(pos_map: PositionMap) -> None:
#     """
#     Detect and correct pattern violations in valid words.
    
#     If a valid word's POS doesn't fit the context pattern,
#     try to find an alternative form with the correct POS.
#     """
#     for state in pos_map:
#         if state.status != 'valid':
#             continue
        
#         left_pos, right_pos = pos_map.get_context(state.position)
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_set = {pos for pos, conf in valid_options}
        
#         # Skip if current POS is already valid for context
#         if state.pos in valid_pos_set:
#             continue
        
#         expected_pos, confidence = valid_options[0]
        
#         # Try to find alternative with correct POS
#         correction = search_with_pos_filter(state.word, expected_pos)
        
#         if correction:
#             state.status = 'corrected'
#             state.corrected = correction['term']
#             state.pos = normalize_pos(correction.get('pos', 'unknown'))
#             state.distance = correction['distance']
#             state.metadata = correction
#             state.correction_reason = 'pattern_violation'


# def run_pass2(pos_map: PositionMap) -> None:
#     """PASS 2: Correct unknowns and pattern violations."""
#     correct_unknowns(pos_map)
#     detect_and_correct_violations(pos_map)


# # =============================================================================
# # PASS 3: BIGRAM DETECTION - Find multi-word entities
# # =============================================================================

# def detect_bigrams(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """
#     PASS 3: Detect bigrams (two-word entities like "New York").
#     """
#     states = list(pos_map)
    
#     if len(states) < 2:
#         return []
    
#     # Collect all consecutive pairs
#     pairs_to_check = []
#     pair_positions = []
    
#     for i in range(len(states) - 1):
#         current = states[i]
#         next_state = states[i + 1]
        
#         word1 = current.corrected or current.word
#         word2 = next_state.corrected or next_state.word
        
#         pairs_to_check.append((word1, word2))
#         pair_positions.append((current.position, next_state.position))
    
#     # Batch check all bigrams
#     bigram_results = batch_check_bigrams(pairs_to_check)
    
#     # Process results
#     bigrams_found = []
    
#     for i, (word1, word2) in enumerate(pairs_to_check):
#         bigram_key = f"{word1.lower()} {word2.lower()}"
        
#         if bigram_key in bigram_results:
#             metadata = bigram_results[bigram_key]
#             category = metadata.get('category', '')
            
#             bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
            
#             pos_start, pos_end = pair_positions[i]
            
#             bigrams_found.append({
#                 'position_start': pos_start,
#                 'position_end': pos_end,
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'subtext': category,
#                 'entity': metadata.get('entity_type', 'bigram'),
#                 'metadata': metadata
#             })
    
#     return bigrams_found


# # =============================================================================
# # OUTPUT BUILDING
# # =============================================================================

# def build_final_results(
#     pos_map: PositionMap,
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Build final merged results with bigrams."""
#     if not bigrams:
#         return [s.to_dict() for s in pos_map]
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for state in pos_map:
#         if skip_next:
#             skip_next = False
#             continue
        
#         if state.position in bigram_starts:
#             bigram = bigram_starts[state.position]
#             merged.append({
#                 'position': state.position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif state.position not in bigram_positions:
#             merged.append(state.to_dict())
    
#     return merged


# def build_corrections_list(pos_map: PositionMap) -> List[Dict[str, Any]]:
#     """Build list of corrections made."""
#     corrections = []
#     for state in pos_map:
#         if state.status == 'corrected':
#             corrections.append({
#                 'position': state.position,
#                 'original': state.word,
#                 'corrected': state.corrected,
#                 'distance': state.distance,
#                 'pos': state.pos,
#                 'is_bigram': False
#             })
#     return corrections


# def build_corrected_query(final_results: List[Dict[str, Any]]) -> str:
#     """Build the corrected query string."""
#     words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             words.append(r['word'])
#         elif r.get('corrected'):
#             words.append(r['corrected'])
#         else:
#             words.append(r['word'])
#     return ' '.join(words)


# # =============================================================================
# # MAIN ENTRY POINTS
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # PASS 1: Validate
#     if verbose:
#         print(f"\n{'='*60}\n🔍 PROCESSING: '{query}'\n{'='*60}")
#         print(f"\nPASS 1: Validating words...")
    
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
    
#     if verbose:
#         for s in states:
#             status = "✅" if s.status == 'valid' else "❓"
#             print(f"   {status} [{s.position}] '{s.word}' -> {s.pos}")
    
#     # PASS 2: Correct unknowns and violations
#     if verbose:
#         print("\nPASS 2: Correcting unknowns and violations...")
    
#     run_pass2(pos_map)
    
#     if verbose:
#         for s in states:
#             if s.status == 'corrected':
#                 reason = f" ({s.correction_reason})" if s.correction_reason else ""
#                 print(f"   🔧 [{s.position}] '{s.word}' -> '{s.corrected}' ({s.pos}){reason}")
    
#     # PASS 3: Detect bigrams
#     if verbose:
#         print("\nPASS 3: Detecting bigrams...")
    
#     bigrams = detect_bigrams(pos_map)
    
#     if verbose:
#         for b in bigrams:
#             print(f"   📎 '{b['bigram']}' ({b['subtext']})")
    
#     # Build output
#     final_results = build_final_results(pos_map, bigrams)
#     corrections = build_corrections_list(pos_map)
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
#     corrected_query = build_corrected_query(final_results)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 RESULT: '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}, Bigrams: {len(bigrams)}")
#         print(f"{'='*60}\n")
    
#     return corrections, tuple_array, corrected_query


# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for search integration.
    
#     Returns:
#         Dict with valid_terms, unknown_terms, corrected_terms, bigram_terms, etc.
#     """
#     words = query.split()
    
#     if not words:
#         return {
#             'success': True,
#             'query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': [],
#             'corrected_query': '',
#             'has_unknown': False,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': 0,
#             'total_count': 0
#         }
    
#     import time
#     times = {}
#         # PASS 1: Validate
#     t1 = time.time()
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
#     times['pass1_validate'] = round((time.time() - t1) * 1000, 2)
    
#     # PASS 2: Correct
#     t2 = time.time()
#     run_pass2(pos_map)
#     times['pass2_correct'] = round((time.time() - t2) * 1000, 2)
    
#     # PASS 3: Bigrams
#     t3 = time.time()
#     bigrams = detect_bigrams(pos_map)
#     times['pass3_bigrams'] = round((time.time() - t3) * 1000, 2)
    
#     # Build results
#     t4 = time.time()
#     final_results = build_final_results(pos_map, bigrams)
#     corrected_query = build_corrected_query(final_results)
#     times['build_results'] = round((time.time() - t4) * 1000, 2)
    
#     print(f"⏱️ WORD_DISCOVERY_FULL TIMING: {times}")
    
#     # Run the three passes
#     states = batch_validate_words(words, pre_validated=pre_validated)
#     pos_map = PositionMap(states)
#     run_pass2(pos_map)
#     bigrams = detect_bigrams(pos_map)
    
#     # Build final results
#     final_results = build_final_results(pos_map, bigrams)
#     corrected_query = build_corrected_query(final_results)
    
#     # Categorize terms for search
#     valid_terms = []
#     unknown_terms = []
#     corrected_terms = []
#     bigram_terms = []
#     search_terms = []
    
#     for term in final_results:
#         status = term.get('status', 'unknown')
#         word = term.get('word', '')
#         corrected = term.get('corrected')
#         pos = term.get('pos', 'unknown')
        
#         search_word = corrected if corrected else word
        
#         term_info = {
#             'word': word,
#             'search_word': search_word,
#             'pos': pos,
#             'status': status,
#             'metadata': term.get('metadata', {})
#         }
        
#         if status == 'valid':
#             valid_terms.append(term_info)
#             search_terms.append(search_word)
#         elif status == 'corrected':
#             corrected_terms.append({
#                 **term_info,
#                 'original': word,
#                 'corrected': corrected
#             })
#             search_terms.append(search_word)
#             valid_terms.append(term_info)
#         elif status == 'bigram':
#             bigram_terms.append(term_info)
#             search_terms.append(search_word)
#             valid_terms.append(term_info)
#         else:  # unknown
#             unknown_terms.append(term_info)
#             search_terms.append(search_word)
    
#     total_terms = len(final_results)
#     unknown_count = len(unknown_terms)
#     valid_count = len(valid_terms)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({valid_count}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({unknown_count}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(t['original'], t['corrected']) for t in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': final_results,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         'has_unknown': unknown_count > 0,
#         'all_unknown': unknown_count == total_terms and total_terms > 0,
#         'valid_count': valid_count,
#         'unknown_count': unknown_count,
#         'total_count': total_terms
#     }


# # =============================================================================
# # HELPER FUNCTIONS FOR SEARCH INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """Determine search strategy based on word discovery results."""
#     valid_count = discovery_result.get('valid_count', 0)
    
#     if valid_count >= 2:
#         return 'strict'
#     elif valid_count == 1:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get terms for strict filtering."""
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get unknown terms for loose search."""
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get all search terms."""
#     return discovery_result.get('search_terms', [])




# # =============================================================================
# # CONVENIENCE WRAPPER FOR OPTIMIZED FLOW
# # =============================================================================

# # def process_query_optimized(
# #     query: str,
# #     verbose: bool = False
# # ) -> Dict[str, Any]:
# #     """
# #     Optimized end-to-end query processing for Typesense search.
    
# #     Combines lookup_table + word_discovery in optimal way:
# #     - Single validation pass (no duplicate Redis calls)
# #     - Batched bigram detection
# #     - Categorized output for search strategy
    
# #     Args:
# #         query: The input query
# #         verbose: Print debug output
    
# #     Returns:
# #         Dict with:
# #         - valid_terms: For strict Typesense filtering
# #         - unknown_terms: For loose search / embedding
# #         - search_terms: Final terms to use
# #         - corrected_query: The corrected query string
# #         - search_strategy: 'strict', 'mixed', or 'semantic'
# #     """
# #     # Handle imports
# #     try:
# #         from .searchapi import lookup_table
# #     except ImportError:
# #         from searchapi import lookup_table
    
# #     # Step 1: Lookup with validation cache
# #     lookup_result = lookup_table(query, return_validation_cache=True)
    
# #     if not lookup_result.get('success', False):
# #         # Return basic structure even on failure
# #         words = query.split()
# #         return {
# #             'success': False,
# #             'query': query,
# #             'corrected_query': query,
# #             'terms': [],
# #             'valid_terms': [],
# #             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
# #                              for w in words],
# #             'corrected_terms': [],
# #             'bigram_terms': [],
# #             'search_terms': words,
# #             'has_unknown': True,
# #             'all_unknown': True,
# #             'valid_count': 0,
# #             'unknown_count': len(words),
# #             'total_count': len(words),
# #             'search_strategy': 'semantic',
# #             'cache_hit': False,
# #             'error': lookup_result.get('error', 'Lookup failed')
# #         }
    
# #     # Step 2: Full word discovery with categorized output
# #     result = word_discovery_full(
# #         query,
# #         verbose=verbose,
# #         pre_validated=lookup_result.get('terms', [])
# #     )
    
# #     # Step 3: Determine search strategy
# #     result['search_strategy'] = get_search_strategy(result)
# #     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
# #     if verbose:
# #         print(f"   Search strategy: {result['search_strategy'].upper()}")
    
# #     return result



# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     import time
#     times = {}
    
#     try:
#         from .searchapi import lookup_table
#     except ImportError:
#         from searchapi import lookup_table
    
#     # Step 1: Lookup
#     t1 = time.time()
#     lookup_result = lookup_table(query, return_validation_cache=True)
#     times['lookup_table'] = round((time.time() - t1) * 1000, 2)
    
#     if not lookup_result.get('success', False):
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Word discovery
#     t2 = time.time()
#     result = word_discovery_full(
#         query,
#         verbose=verbose,
#         pre_validated=lookup_result.get('terms', [])
#     )
#     times['word_discovery_full'] = round((time.time() - t2) * 1000, 2)
    
#     # Step 3: Strategy
#     t3 = time.time()
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
#     times['strategy'] = round((time.time() - t3) * 1000, 2)
    
#     print(f"⏱️ WORD_DISCOVERY TIMING: {times}")
    
#     return result
# # =============================================================================
# # TEST FUNCTION
# # =============================================================================

# def test_word_discovery():
#     """Test function to verify word discovery is working."""
#     test_queries = [
#         "tuskegee airman and dos",
#         "the quikc brown fox",
#         "new york city",
#         "running fast",
#     ]
    
#     print("=" * 60)
#     print("WORD DISCOVERY TEST")
#     print("=" * 60)
    
#     for query in test_queries:
#         print(f"\nQuery: '{query}'")
#         result = word_discovery_full(query, verbose=False)
        
#         print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
#         print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
#         print(f"  Corrected: {result['corrected_query']}")
#         print(f"  Strategy: {get_search_strategy(result)}")


# if __name__ == "__main__":
# #     test_word_discovery()

# """
# word_discovery.py
# OPTIMIZED: Single-pass word validation, correction, and bigram detection.

# Key Changes:
# - ONE batch Redis call upfront (not multiple passes)
# - Only correct UNKNOWN words (don't touch valid words)
# - Bigram detection in same loop
# - O(1) lookups using pre-fetched data
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Handle both relative and absolute imports
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )
# except ImportError:
#     from searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country", "us_city", "us_state"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
#     ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
#     (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value to a simple string.
    
#     Handles:
#     - None -> 'unknown'
#     - "['noun']" (string) -> 'noun'
#     - ['noun'] (list) -> 'noun'
#     - 'noun' (string) -> 'noun'
#     - Location categories -> 'proper_noun'
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         pos_value = pos_value.strip()
#         if pos_value.startswith('[') and pos_value.endswith(']'):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 if isinstance(parsed, list) and parsed:
#                     pos_value = parsed[0]
#                 else:
#                     pos_value = 'unknown'
#             except (json.JSONDecodeError, ValueError):
#                 # Try manual parsing: "['noun']" -> "noun"
#                 inner = pos_value[1:-1].strip()
#                 if inner.startswith("'") and inner.endswith("'"):
#                     pos_value = inner[1:-1]
#                 elif inner.startswith('"') and inner.endswith('"'):
#                     pos_value = inner[1:-1]
#                 else:
#                     pos_value = inner
    
#     # Handle actual list: ['noun']
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Ensure it's a string
#     if not isinstance(pos_value, str):
#         pos_value = str(pos_value) if pos_value else 'unknown'
    
#     # Normalize location types to proper_noun
#     pos_lower = pos_value.lower().strip()
    
#     if pos_lower in ('proper noun', 'proper_noun'):
#         return 'proper_noun'
    
#     if pos_lower in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     # Return as-is if it's a valid POS
#     if pos_lower in ALLOWED_POS:
#         return pos_lower
    
#     return pos_value.lower() if pos_value else 'unknown'


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on neighboring words' POS tags."""
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - Only for UNKNOWN words
# # =============================================================================

# def find_correction_for_unknown(
#     word: str,
#     left_pos: Optional[str],
#     right_pos: Optional[str],
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Find correction for an unknown word using POS context.
    
#     1. Predict expected POS from neighbors
#     2. Generate candidates
#     3. Filter by POS match
#     4. Return closest by distance
#     """
#     # Predict what POS this word should be
#     prediction = predict_pos_from_context(left_pos, right_pos)
#     predicted_pos = prediction[0] if prediction else 'noun'  # Default to noun
    
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=15)
    
#     if not candidates:
#         return None
    
#     # Batch check which candidates exist
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # Calculate distance
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
#         item['normalized_pos'] = item_pos
        
#         # Check if POS matches prediction
#         pos_match = (
#             item_pos == predicted_pos or
#             (predicted_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (predicted_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     # Prefer POS matches, fall back to any match
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     # Sort by distance first, then by rank (higher rank = better)
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# # =============================================================================
# # SINGLE-PASS WORD DISCOVERY
# # =============================================================================

# def word_discovery_single_pass(
#     query: str,
#     pre_validated: Optional[List[Dict[str, Any]]] = None,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     OPTIMIZED: Single-pass word discovery.
    
#     1. Batch validate all words (single Redis call)
#     2. Single loop: process each word + detect bigrams
#        - Valid word -> keep it
#        - Unknown word -> find correction using POS context
#     3. Return categorized results
#     """
#     words = query.split()
    
#     if not words:
#         return _empty_result(query)
    
#     # =========================================================================
#     # STEP 1: Batch validate all words - ONE Redis call
#     # =========================================================================
    
#     if pre_validated:
#         # Use pre-validated data (from lookup_table)
#         word_data = {}
#         for item in pre_validated:
#             w = item.get('word', '').lower()
#             if w:
#                 word_data[w] = {
#                     'exists': item.get('exists', False),
#                     'pos': normalize_pos(item.get('pos') or item.get('metadata', {}).get('pos')),
#                     'metadata': item.get('metadata', item)
#                 }
#     else:
#         # Batch validate via Redis
#         validation_cache = batch_validate_words_redis(words)
#         word_data = {}
#         for word in words:
#             w = word.lower()
#             if w in validation_cache:
#                 result = validation_cache[w]
#                 word_data[w] = {
#                     'exists': result.get('is_valid', False),
#                     'pos': normalize_pos(result.get('metadata', {}).get('pos', 'unknown')),
#                     'metadata': result.get('metadata', {})
#                 }
#             else:
#                 word_data[w] = {'exists': False, 'pos': 'unknown', 'metadata': {}}
    
#     # =========================================================================
#     # STEP 2: Prepare bigram checking - ONE Redis call
#     # =========================================================================
    
#     bigram_pairs = []
#     for i in range(len(words) - 1):
#         bigram_pairs.append((words[i].lower(), words[i + 1].lower()))
    
#     bigram_results = batch_check_bigrams(bigram_pairs) if bigram_pairs else {}
    
#     # =========================================================================
#     # STEP 3: Single loop - process words and detect bigrams
#     # =========================================================================
    
#     processed = []  # Final processed words
#     corrections = []  # Corrections made
#     bigrams_found = []  # Bigrams detected
#     skip_next = False
    
#     for i, word in enumerate(words):
#         if skip_next:
#             skip_next = False
#             continue
        
#         word_lower = word.lower()
#         data = word_data.get(word_lower, {'exists': False, 'pos': 'unknown', 'metadata': {}})
        
#         # Check for bigram with next word
#         if i < len(words) - 1:
#             next_word = words[i + 1].lower()
#             bigram_key = f"{word_lower} {next_word}"
            
#             if bigram_key in bigram_results:
#                 # Found a bigram!
#                 bigram_meta = bigram_results[bigram_key]
#                 category = bigram_meta.get('category', '')
#                 bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': f"{word} {words[i + 1]}",
#                     'search_word': f"{word} {words[i + 1]}",
#                     'status': 'bigram',
#                     'pos': bigram_pos,
#                     'metadata': bigram_meta
#                 })
                
#                 bigrams_found.append({
#                     'bigram': f"{word} {words[i + 1]}",
#                     'category': category
#                 })
                
#                 skip_next = True
#                 continue
        
#         # Process single word
#         if data['exists']:
#             # Word is valid - keep it as is
#             processed.append({
#                 'position': i + 1,
#                 'word': word,
#                 'search_word': word,
#                 'status': 'valid',
#                 'pos': data['pos'],
#                 'metadata': data['metadata']
#             })
#         else:
#             # Word is unknown - try to correct it
#             left_pos = processed[-1]['pos'] if processed else None
            
#             # Look ahead for right POS (if next word is valid)
#             right_pos = None
#             if i < len(words) - 1:
#                 next_word_lower = words[i + 1].lower()
#                 next_data = word_data.get(next_word_lower, {})
#                 if next_data.get('exists'):
#                     right_pos = next_data.get('pos')
            
#             # Find correction
#             correction = find_correction_for_unknown(word, left_pos, right_pos)
            
#             if correction:
#                 corrected_word = correction.get('term', word)
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': corrected_word,
#                     'status': 'corrected',
#                     'pos': normalize_pos(correction.get('pos', 'unknown')),
#                     'metadata': correction,
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0)
#                 })
#                 corrections.append({
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0)
#                 })
#             else:
#                 # No correction found - keep original
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': word,
#                     'status': 'unknown',
#                     'pos': 'unknown',
#                     'metadata': {}
#                 })
    
#     # =========================================================================
#     # STEP 4: Build results
#     # =========================================================================
    
#     valid_terms = [p for p in processed if p['status'] in ('valid', 'corrected', 'bigram')]
#     unknown_terms = [p for p in processed if p['status'] == 'unknown']
#     corrected_terms = [p for p in processed if p['status'] == 'corrected']
#     bigram_terms = [p for p in processed if p['status'] == 'bigram']
    
#     search_terms = [p['search_word'] for p in processed]
#     corrected_query = ' '.join(search_terms)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({len(valid_terms)}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({len(unknown_terms)}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(c['original'], c['corrected']) for c in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': processed,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         'has_unknown': len(unknown_terms) > 0,
#         'all_unknown': len(unknown_terms) == len(processed) and len(processed) > 0,
#         'valid_count': len(valid_terms),
#         'unknown_count': len(unknown_terms),
#         'total_count': len(processed)
#     }


# def _empty_result(query: str) -> Dict[str, Any]:
#     """Return empty result structure."""
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': '',
#         'terms': [],
#         'valid_terms': [],
#         'unknown_terms': [],
#         'corrected_terms': [],
#         'bigram_terms': [],
#         'search_terms': [],
#         'has_unknown': False,
#         'all_unknown': True,
#         'valid_count': 0,
#         'unknown_count': 0,
#         'total_count': 0
#     }


# # =============================================================================
# # PUBLIC API - Compatible with existing code
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for search integration.
#     This is the main entry point - now uses single-pass processing.
#     """
#     return word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)


# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Legacy entry point for compatibility.
#     Returns: (corrections, tuple_array, corrected_query)
#     """
#     result = word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)
    
#     corrections = result.get('corrected_terms', [])
#     tuple_array = [(t['position'], t['pos']) for t in result.get('terms', [])]
#     corrected_query = result.get('corrected_query', query)
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # HELPER FUNCTIONS FOR SEARCH INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """Determine search strategy based on word discovery results."""
#     valid_count = discovery_result.get('valid_count', 0)
    
#     if valid_count >= 2:
#         return 'strict'
#     elif valid_count == 1:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get terms for strict filtering."""
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get unknown terms for loose search."""
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get all search terms."""
#     return discovery_result.get('search_terms', [])


# # =============================================================================
# # OPTIMIZED QUERY PROCESSING
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     - Single validation pass
#     - Batched bigram detection
#     - Categorized output for search strategy
#     """
#     try:
#         from .searchapi import lookup_table
#     except ImportError:
#         from searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Single-pass word discovery
#     result = word_discovery_single_pass(
#         query,
#         pre_validated=lookup_result.get('terms', []),
#         verbose=verbose
#     )
    
#     # Step 3: Determine search strategy
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     return result


# # =============================================================================
# # TEST FUNCTION
# # =============================================================================

# def test_word_discovery():
#     """Test function to verify word discovery is working."""
#     test_queries = [
#         "tuskegee airmen",
#         "the quikc brown fox",
#         "new york city",
#         "black doctors",
#         "african american history",
#     ]
    
#     print("=" * 60)
#     print("WORD DISCOVERY TEST (SINGLE-PASS)")
#     print("=" * 60)
    
#     for query in test_queries:
#         print(f"\nQuery: '{query}'")
#         result = word_discovery_single_pass(query, verbose=False)
        
#         print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
#         print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
#         print(f"  Corrected: {result['corrected_query']}")
#         print(f"  Strategy: {get_search_strategy(result)}")


# if __name__ == "__main__":
#     test_word_discovery()



#                                                                                   PART 2   This works perfectly 

# """
# word_discovery.py
# OPTIMIZED: Single-pass word validation, correction, and bigram detection.

# Key Changes:
# - ONE batch Redis call upfront (not multiple passes)
# - Only correct UNKNOWN words (don't touch valid words)
# - Bigram detection in same loop
# - O(1) lookups using pre-fetched data
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Handle both relative and absolute imports
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )
# except ImportError:
#     from searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })

# LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country", "us_city", "us_state"})

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
#     ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
#     (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value to a simple string.
    
#     Handles:
#     - None -> 'unknown'
#     - "['noun']" (string) -> 'noun'
#     - ['noun'] (list) -> 'noun'
#     - 'noun' (string) -> 'noun'
#     - Location categories -> 'proper_noun'
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         pos_value = pos_value.strip()
#         if pos_value.startswith('[') and pos_value.endswith(']'):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 if isinstance(parsed, list) and parsed:
#                     pos_value = parsed[0]
#                 else:
#                     pos_value = 'unknown'
#             except (json.JSONDecodeError, ValueError):
#                 # Try manual parsing: "['noun']" -> "noun"
#                 inner = pos_value[1:-1].strip()
#                 if inner.startswith("'") and inner.endswith("'"):
#                     pos_value = inner[1:-1]
#                 elif inner.startswith('"') and inner.endswith('"'):
#                     pos_value = inner[1:-1]
#                 else:
#                     pos_value = inner
    
#     # Handle actual list: ['noun']
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Ensure it's a string
#     if not isinstance(pos_value, str):
#         pos_value = str(pos_value) if pos_value else 'unknown'
    
#     # Normalize location types to proper_noun
#     pos_lower = pos_value.lower().strip()
    
#     if pos_lower in ('proper noun', 'proper_noun'):
#         return 'proper_noun'
    
#     if pos_lower in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     # Return as-is if it's a valid POS
#     if pos_lower in ALLOWED_POS:
#         return pos_lower
    
#     return pos_value.lower() if pos_value else 'unknown'


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on neighboring words' POS tags."""
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - Only for UNKNOWN words
# # =============================================================================

# def find_correction_for_unknown(
#     word: str,
#     left_pos: Optional[str],
#     right_pos: Optional[str],
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Find correction for an unknown word using POS context.
    
#     1. Predict expected POS from neighbors
#     2. Generate candidates
#     3. Filter by POS match
#     4. Return closest by distance
#     """
#     # Predict what POS this word should be
#     prediction = predict_pos_from_context(left_pos, right_pos)
#     predicted_pos = prediction[0] if prediction else 'noun'  # Default to noun
    
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batch check which candidates exist
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # Calculate distance
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
#         item['normalized_pos'] = item_pos
        
#         # Check if POS matches prediction
#         pos_match = (
#             item_pos == predicted_pos or
#             (predicted_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (predicted_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     # Prefer POS matches, fall back to any match
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     # Sort by distance first, then by rank (higher rank = better)
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# # =============================================================================
# # SINGLE-PASS WORD DISCOVERY
# # =============================================================================

# def word_discovery_single_pass(
#     query: str,
#     pre_validated: Optional[List[Dict[str, Any]]] = None,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     OPTIMIZED: Single-pass word discovery.
    
#     1. Batch validate all words (single Redis call)
#     2. Single loop: process each word + detect bigrams
#        - Valid word -> keep it
#        - Unknown word -> find correction using POS context
#     3. Return categorized results
#     """
#     words = query.split()
    
#     if not words:
#         return _empty_result(query)
    
#     # =========================================================================
#     # STEP 1: Batch validate all words - ONE Redis call
#     # =========================================================================
    
#     if pre_validated:
#         # Use pre-validated data (from lookup_table)
#         word_data = {}
#         for item in pre_validated:
#             w = item.get('word', '').lower()
#             if w:
#                 word_data[w] = {
#                     'exists': item.get('exists', False),
#                     'pos': normalize_pos(item.get('pos') or item.get('metadata', {}).get('pos')),
#                     'metadata': item.get('metadata', item)
#                 }
#     else:
#         # Batch validate via Redis
#         validation_cache = batch_validate_words_redis(words)
#         word_data = {}
#         for word in words:
#             w = word.lower()
#             if w in validation_cache:
#                 result = validation_cache[w]
#                 word_data[w] = {
#                     'exists': result.get('is_valid', False),
#                     'pos': normalize_pos(result.get('metadata', {}).get('pos', 'unknown')),
#                     'metadata': result.get('metadata', {})
#                 }
#             else:
#                 word_data[w] = {'exists': False, 'pos': 'unknown', 'metadata': {}}
    
#     # =========================================================================
#     # STEP 2: Prepare bigram checking - ONE Redis call
#     # =========================================================================
    
#     bigram_pairs = []
#     for i in range(len(words) - 1):
#         bigram_pairs.append((words[i].lower(), words[i + 1].lower()))
    
#     bigram_results = batch_check_bigrams(bigram_pairs) if bigram_pairs else {}
    
#     # =========================================================================
#     # STEP 3: Single loop - process words and detect bigrams
#     # =========================================================================
    
#     processed = []  # Final processed words
#     corrections = []  # Corrections made
#     bigrams_found = []  # Bigrams detected
#     skip_next = False
    
#     for i, word in enumerate(words):
#         if skip_next:
#             skip_next = False
#             continue
        
#         word_lower = word.lower()
#         data = word_data.get(word_lower, {'exists': False, 'pos': 'unknown', 'metadata': {}})
        
#         # Check for bigram with next word
#         if i < len(words) - 1:
#             next_word = words[i + 1].lower()
#             bigram_key = f"{word_lower} {next_word}"
            
#             if bigram_key in bigram_results:
#                 # Found a bigram!
#                 bigram_meta = bigram_results[bigram_key]
#                 category = bigram_meta.get('category', '')
#                 bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': f"{word} {words[i + 1]}",
#                     'search_word': f"{word} {words[i + 1]}",
#                     'status': 'bigram',
#                     'pos': bigram_pos,
#                     'metadata': bigram_meta
#                 })
                
#                 bigrams_found.append({
#                     'bigram': f"{word} {words[i + 1]}",
#                     'category': category
#                 })
                
#                 skip_next = True
#                 continue
        
#         # Process single word
#         if data['exists']:
#             # Word is valid - keep it as is
#             processed.append({
#                 'position': i + 1,
#                 'word': word,
#                 'search_word': word,
#                 'status': 'valid',
#                 'pos': data['pos'],
#                 'metadata': data['metadata']
#             })
#         else:
#             # Word is unknown - try to correct it
#             left_pos = processed[-1]['pos'] if processed else None
            
#             # Look ahead for right POS (if next word is valid)
#             right_pos = None
#             if i < len(words) - 1:
#                 next_word_lower = words[i + 1].lower()
#                 next_data = word_data.get(next_word_lower, {})
#                 if next_data.get('exists'):
#                     right_pos = next_data.get('pos')
            
#             # Find correction
#             correction = find_correction_for_unknown(word, left_pos, right_pos)
            
#             if correction:
#                 corrected_word = correction.get('term', word)
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': corrected_word,
#                     'status': 'corrected',
#                     'pos': normalize_pos(correction.get('pos', 'unknown')),
#                     'metadata': correction,
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0)
#                 })
#                 corrections.append({
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0)
#                 })
#             else:
#                 # No correction found - keep original
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': word,
#                     'status': 'unknown',
#                     'pos': 'unknown',
#                     'metadata': {}
#                 })
    
#     # =========================================================================
#     # STEP 4: Build results
#     # =========================================================================
    
#     valid_terms = [p for p in processed if p['status'] in ('valid', 'corrected', 'bigram')]
#     unknown_terms = [p for p in processed if p['status'] == 'unknown']
#     corrected_terms = [p for p in processed if p['status'] == 'corrected']
#     bigram_terms = [p for p in processed if p['status'] == 'bigram']
    
#     search_terms = [p['search_word'] for p in processed]
#     corrected_query = ' '.join(search_terms)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({len(valid_terms)}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({len(unknown_terms)}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(c['original'], c['corrected']) for c in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': processed,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         'has_unknown': len(unknown_terms) > 0,
#         'all_unknown': len(unknown_terms) == len(processed) and len(processed) > 0,
#         'valid_count': len(valid_terms),
#         'unknown_count': len(unknown_terms),
#         'total_count': len(processed)
#     }


# def _empty_result(query: str) -> Dict[str, Any]:
#     """Return empty result structure."""
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': '',
#         'terms': [],
#         'valid_terms': [],
#         'unknown_terms': [],
#         'corrected_terms': [],
#         'bigram_terms': [],
#         'search_terms': [],
#         'has_unknown': False,
#         'all_unknown': True,
#         'valid_count': 0,
#         'unknown_count': 0,
#         'total_count': 0
#     }


# # =============================================================================
# # PUBLIC API - Compatible with existing code
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for search integration.
#     This is the main entry point - now uses single-pass processing.
#     """
#     return word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)


# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Legacy entry point for compatibility.
#     Returns: (corrections, tuple_array, corrected_query)
#     """
#     result = word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)
    
#     corrections = result.get('corrected_terms', [])
#     tuple_array = [(t['position'], t['pos']) for t in result.get('terms', [])]
#     corrected_query = result.get('corrected_query', query)
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # HELPER FUNCTIONS FOR SEARCH INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """Determine search strategy based on word discovery results."""
#     valid_count = discovery_result.get('valid_count', 0)
    
#     if valid_count >= 2:
#         return 'strict'
#     elif valid_count == 1:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get terms for strict filtering."""
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get unknown terms for loose search."""
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get all search terms."""
#     return discovery_result.get('search_terms', [])


# # =============================================================================
# # OPTIMIZED QUERY PROCESSING
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     - Single validation pass
#     - Batched bigram detection
#     - Categorized output for search strategy
#     """
#     try:
#         from .searchapi import lookup_table
#     except ImportError:
#         from searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Single-pass word discovery
#     result = word_discovery_single_pass(
#         query,
#         pre_validated=lookup_result.get('terms', []),
#         verbose=verbose
#     )
    
#     # Step 3: Determine search strategy
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     return result


# # =============================================================================
# # TEST FUNCTION
# # =============================================================================

# def test_word_discovery():
#     """Test function to verify word discovery is working."""
#     test_queries = [
#         "tuskegee airmen",
#         "the quikc brown fox",
#         "new york city",
#         "black doctors",
#         "african american history",
#     ]
    
#     print("=" * 60)
#     print("WORD DISCOVERY TEST (SINGLE-PASS)")
#     print("=" * 60)
    
#     for query in test_queries:
#         print(f"\nQuery: '{query}'")
#         result = word_discovery_single_pass(query, verbose=False)
        
#         print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
#         print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
#         print(f"  Corrected: {result['corrected_query']}")
#         print(f"  Strategy: {get_search_strategy(result)}")


# if __name__ == "__main__":
#     test_word_discovery()



# """
# word_discovery.py
# OPTIMIZED: Single-pass word validation, correction, and bigram detection.
# UPDATED: Score-based search strategy selection using Redis rank field.

# Key Changes:
# - ONE batch Redis call upfront (not multiple passes)
# - Only correct UNKNOWN words (don't touch valid words)
# - Bigram detection in same loop
# - O(1) lookups using pre-fetched data
# - NEW: Extract rank scores from Redis for strategy selection
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Handle both relative and absolute imports
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )
# except ImportError:
#     from searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })


# LOCATION_TYPES: frozenset = frozenset({
#     # Lowercase with underscore
#     "city", "state", "neighborhood", "region", "country", "us_city", "us_state",
#     # Lowercase with space (matches Redis "US City".lower() → "us city")
#     "us city", "us state",
#     # Other variations
#     "location"
# })

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # =============================================================================
# # SCORE-BASED STRATEGY THRESHOLDS
# # =============================================================================

# SCORE_THRESHOLD_STRICT = 2000   # avg_score > 2000 → strict keyword search
# SCORE_THRESHOLD_MIXED = 500     # avg_score 500-2000 → mixed search
# # avg_score < 500 → semantic search

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
#     ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
#     (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value to a simple string.
    
#     Handles:
#     - None -> 'unknown'
#     - "['noun']" (string) -> 'noun'
#     - ['noun'] (list) -> 'noun'
#     - 'noun' (string) -> 'noun'
#     - Location categories -> 'proper_noun'
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         pos_value = pos_value.strip()
#         if pos_value.startswith('[') and pos_value.endswith(']'):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 if isinstance(parsed, list) and parsed:
#                     pos_value = parsed[0]
#                 else:
#                     pos_value = 'unknown'
#             except (json.JSONDecodeError, ValueError):
#                 # Try manual parsing: "['noun']" -> "noun"
#                 inner = pos_value[1:-1].strip()
#                 if inner.startswith("'") and inner.endswith("'"):
#                     pos_value = inner[1:-1]
#                 elif inner.startswith('"') and inner.endswith('"'):
#                     pos_value = inner[1:-1]
#                 else:
#                     pos_value = inner
    
#     # Handle actual list: ['noun']
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Ensure it's a string
#     if not isinstance(pos_value, str):
#         pos_value = str(pos_value) if pos_value else 'unknown'
    
#     # Normalize location types to proper_noun
#     pos_lower = pos_value.lower().strip()
    
#     if pos_lower in ('proper noun', 'proper_noun'):
#         return 'proper_noun'
    
#     if pos_lower in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     # Return as-is if it's a valid POS
#     if pos_lower in ALLOWED_POS:
#         return pos_lower
    
#     return pos_value.lower() if pos_value else 'unknown'


# def extract_rank_score(metadata: Dict[str, Any]) -> int:
#     """
#     Extract rank score from metadata.
    
#     Handles various formats:
#     - int: 1234
#     - str: "1234"
#     - missing: 0
#     """
#     rank = metadata.get('rank', 0)
    
#     if isinstance(rank, int):
#         return rank
    
#     if isinstance(rank, float):
#         return int(rank)
    
#     if isinstance(rank, str):
#         try:
#             return int(rank)
#         except (ValueError, TypeError):
#             return 0
    
#     return 0


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on neighboring words' POS tags."""
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - Only for UNKNOWN words
# # =============================================================================

# def find_correction_for_unknown(
#     word: str,
#     left_pos: Optional[str],
#     right_pos: Optional[str],
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Find correction for an unknown word using POS context.
    
#     1. Predict expected POS from neighbors
#     2. Generate candidates
#     3. Filter by POS match
#     4. Return closest by distance
#     """
#     # Predict what POS this word should be
#     prediction = predict_pos_from_context(left_pos, right_pos)
#     predicted_pos = prediction[0] if prediction else 'noun'  # Default to noun
    
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batch check which candidates exist
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     matches = []
#     fallback_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
        
#         # Calculate distance
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
#         item['normalized_pos'] = item_pos
        
#         # Check if POS matches prediction
#         pos_match = (
#             item_pos == predicted_pos or
#             (predicted_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
#             (predicted_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
#         )
        
#         if pos_match:
#             matches.append(item)
#         else:
#             fallback_matches.append(item)
    
#     # Prefer POS matches, fall back to any match
#     result_list = matches if matches else fallback_matches
    
#     if not result_list:
#         return None
    
#     # Sort by distance first, then by rank (higher rank = better)
#     result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     return result_list[0]


# # =============================================================================
# # SINGLE-PASS WORD DISCOVERY
# # =============================================================================

# def word_discovery_single_pass(
#     query: str,
#     pre_validated: Optional[List[Dict[str, Any]]] = None,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     OPTIMIZED: Single-pass word discovery with score extraction.
    
#     1. Batch validate all words (single Redis call)
#     2. Single loop: process each word + detect bigrams
#        - Valid word -> keep it, extract rank score
#        - Unknown word -> find correction using POS context
#     3. Calculate score totals for strategy selection
#     4. Return categorized results with scores
#     """
#     words = query.split()
    
#     if not words:
#         return _empty_result(query)
    
#     # =========================================================================
#     # STEP 1: Batch validate all words - ONE Redis call
#     # =========================================================================
    
#     if pre_validated:
#         # Use pre-validated data (from lookup_table)
#         word_data = {}
#         for item in pre_validated:
#             w = item.get('word', '').lower()
#             if w:
#                 metadata = item.get('metadata', item)
#                 word_data[w] = {
#                     'exists': item.get('exists', False),
#                     'pos': normalize_pos(item.get('pos') or metadata.get('pos')),
#                     'metadata': metadata
#                 }
#     else:
#         # Batch validate via Redis
#         validation_cache = batch_validate_words_redis(words)
#         word_data = {}
#         for word in words:
#             w = word.lower()
#             if w in validation_cache:
#                 result = validation_cache[w]
#                 word_data[w] = {
#                     'exists': result.get('is_valid', False),
#                     'pos': normalize_pos(result.get('metadata', {}).get('pos', 'unknown')),
#                     'metadata': result.get('metadata', {})
#                 }
#             else:
#                 word_data[w] = {'exists': False, 'pos': 'unknown', 'metadata': {}}
    
#     # =========================================================================
#     # STEP 2: Prepare bigram checking - ONE Redis call
#     # =========================================================================
    
#     bigram_pairs = []
#     for i in range(len(words) - 1):
#         bigram_pairs.append((words[i].lower(), words[i + 1].lower()))
    
#     bigram_results = batch_check_bigrams(bigram_pairs) if bigram_pairs else {}
    
#     # =========================================================================
#     # STEP 3: Single loop - process words and detect bigrams
#     # =========================================================================
    
#     processed = []  # Final processed words
#     corrections = []  # Corrections made
#     bigrams_found = []  # Bigrams detected
#     skip_next = False
    
#     for i, word in enumerate(words):
#         if skip_next:
#             skip_next = False
#             continue
        
#         word_lower = word.lower()
#         data = word_data.get(word_lower, {'exists': False, 'pos': 'unknown', 'metadata': {}})
        
#         # Check for bigram with next word
#         if i < len(words) - 1:
#             next_word = words[i + 1].lower()
#             bigram_key = f"{word_lower} {next_word}"
            
#             if bigram_key in bigram_results:
#                 # Found a bigram!
#                 bigram_meta = bigram_results[bigram_key]
#                 category = bigram_meta.get('category', '')
#                 bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
                
#                 # Extract rank score (bigrams get 1.5x multiplier for being multi-word)
#                 bigram_rank = extract_rank_score(bigram_meta)
#                 bigram_rank = int(bigram_rank * 1.5)  # Bigram bonus
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': f"{word} {words[i + 1]}",
#                     'search_word': f"{word} {words[i + 1]}",
#                     'status': 'bigram',
#                     'pos': bigram_pos,
#                     'metadata': bigram_meta,
#                     'rank_score': bigram_rank
#                 })
                
#                 bigrams_found.append({
#                     'bigram': f"{word} {words[i + 1]}",
#                     'category': category,
#                     'rank_score': bigram_rank
#                 })
                
#                 skip_next = True
#                 continue
        
#         # Process single word
#         if data['exists']:
#             # Word is valid - keep it as is
#             # Extract rank score from metadata
#             rank_score = extract_rank_score(data['metadata'])
            
#             processed.append({
#                 'position': i + 1,
#                 'word': word,
#                 'search_word': word,
#                 'status': 'valid',
#                 'pos': data['pos'],
#                 'metadata': data['metadata'],
#                 'rank_score': rank_score
#             })
#         else:
#             # Word is unknown - try to correct it
#             left_pos = processed[-1]['pos'] if processed else None
            
#             # Look ahead for right POS (if next word is valid)
#             right_pos = None
#             if i < len(words) - 1:
#                 next_word_lower = words[i + 1].lower()
#                 next_data = word_data.get(next_word_lower, {})
#                 if next_data.get('exists'):
#                     right_pos = next_data.get('pos')
            
#             # Find correction
#             correction = find_correction_for_unknown(word, left_pos, right_pos)
            
#             if correction:
#                 corrected_word = correction.get('term', word)
#                 correction_rank = extract_rank_score(correction)
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': corrected_word,
#                     'status': 'corrected',
#                     'pos': normalize_pos(correction.get('pos', 'unknown')),
#                     'metadata': correction,
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0),
#                     'rank_score': correction_rank
#                 })
#                 corrections.append({
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0),
#                     'rank_score': correction_rank
#                 })
#             else:
#                 # No correction found - keep original with zero score
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': word,
#                     'status': 'unknown',
#                     'pos': 'unknown',
#                     'metadata': {},
#                     'rank_score': 0
#                 })
    
#     # =========================================================================
#     # STEP 4: Build results with score calculations
#     # =========================================================================
    
#     valid_terms = [p for p in processed if p['status'] in ('valid', 'corrected', 'bigram')]
#     unknown_terms = [p for p in processed if p['status'] == 'unknown']
#     corrected_terms = [p for p in processed if p['status'] == 'corrected']
#     bigram_terms = [p for p in processed if p['status'] == 'bigram']
    
#     search_terms = [p['search_word'] for p in processed]
#     corrected_query = ' '.join(search_terms)
    
#     # Calculate score totals for strategy selection
#     total_score = sum(p.get('rank_score', 0) for p in processed)
#     scored_terms = [p for p in processed if p.get('rank_score', 0) > 0]
#     average_score = total_score / len(processed) if processed else 0
#     max_score = max((p.get('rank_score', 0) for p in processed), default=0)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({len(valid_terms)}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({len(unknown_terms)}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(c['original'], c['corrected']) for c in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"   📈 Scores: total={total_score}, avg={average_score:.1f}, max={max_score}")
#         print(f"   Term scores: {[(t['search_word'], t.get('rank_score', 0)) for t in processed]}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': processed,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'search_terms': search_terms,
#         'has_unknown': len(unknown_terms) > 0,
#         'all_unknown': len(unknown_terms) == len(processed) and len(processed) > 0,
#         'valid_count': len(valid_terms),
#         'unknown_count': len(unknown_terms),
#         'total_count': len(processed),
#         # Score-based metrics for strategy selection
#         'total_score': total_score,
#         'average_score': round(average_score, 1),
#         'max_score': max_score,
#         'scored_term_count': len(scored_terms)
#     }


# def _empty_result(query: str) -> Dict[str, Any]:
#     """Return empty result structure."""
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': '',
#         'terms': [],
#         'valid_terms': [],
#         'unknown_terms': [],
#         'corrected_terms': [],
#         'bigram_terms': [],
#         'search_terms': [],
#         'has_unknown': False,
#         'all_unknown': True,
#         'valid_count': 0,
#         'unknown_count': 0,
#         'total_count': 0,
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'scored_term_count': 0
#     }


# # =============================================================================
# # PUBLIC API - Compatible with existing code
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for search integration.
#     This is the main entry point - now uses single-pass processing.
#     """
#     return word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)


# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Legacy entry point for compatibility.
#     Returns: (corrections, tuple_array, corrected_query)
#     """
#     result = word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)
    
#     corrections = result.get('corrected_terms', [])
#     tuple_array = [(t['position'], t['pos']) for t in result.get('terms', [])]
#     corrected_query = result.get('corrected_query', query)
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # HELPER FUNCTIONS FOR SEARCH INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """
#     Determine search strategy based on SCORE, not just count.
    
#     Thresholds:
#     - avg_score > 2000: User knows domain vocabulary → strict keyword search
#     - avg_score 500-2000: Partial domain knowledge → mixed search  
#     - avg_score < 500: Generic/conceptual query → semantic search
    
#     High scores indicate terms that appear frequently in your corpus,
#     meaning the user is "speaking the domain language" and keyword
#     matching will be effective.
    
#     Low scores indicate generic vocabulary or terms not in your corpus,
#     meaning semantic/vector search will better understand intent.
#     """
#     average_score = discovery_result.get('average_score', 0)
#     valid_count = discovery_result.get('valid_count', 0)
    
#     # Must have at least one valid term to use strict/mixed
#     if valid_count == 0:
#         return 'semantic'
    
#     # Score-based strategy selection
#     if average_score > SCORE_THRESHOLD_STRICT:
#         return 'strict'
#     elif average_score > SCORE_THRESHOLD_MIXED:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get terms for strict filtering."""
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get unknown terms for loose search."""
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get all search terms."""
#     return discovery_result.get('search_terms', [])


# def get_term_scores(discovery_result: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """Get list of terms with their scores."""
#     return [
#         {
#             'term': t['search_word'],
#             'score': t.get('rank_score', 0),
#             'status': t['status']
#         }
#         for t in discovery_result.get('terms', [])
#     ]


# def get_high_score_terms(discovery_result: Dict[str, Any], min_score: int = 500) -> List[str]:
#     """Get only terms with score above threshold."""
#     return [
#         t['search_word']
#         for t in discovery_result.get('terms', [])
#         if t.get('rank_score', 0) >= min_score
#     ]


# # =============================================================================
# # OPTIMIZED QUERY PROCESSING
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     - Single validation pass
#     - Batched bigram detection
#     - Categorized output for search strategy
#     - Score-based strategy selection
#     """
#     try:
#         from .searchapi import lookup_table
#     except ImportError:
#         from searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown', 'rank_score': 0} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'total_score': 0,
#             'average_score': 0,
#             'max_score': 0,
#             'scored_term_count': 0,
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Single-pass word discovery
#     result = word_discovery_single_pass(
#         query,
#         pre_validated=lookup_result.get('terms', []),
#         verbose=verbose
#     )
    
#     # Step 3: Determine search strategy using scores
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     return result


# # =============================================================================
# # TEST FUNCTION
# # =============================================================================

# def test_word_discovery():
#     """Test function to verify word discovery and scoring is working."""
#     test_queries = [
#         "tuskegee airmen",           # Should be high score (domain terms)
#         "the quick brown fox",        # Should be low score (generic)
#         "new york city",              # Should be high score (location bigram)
#         "black doctors",              # Should be medium score
#         "african american history",   # Should be high score
#         "how did people vote",        # Should be low score (generic phrasing)
#         "hbcu civil rights",          # Should be high score
#     ]
    
#     print("=" * 70)
#     print("WORD DISCOVERY TEST (SCORE-BASED STRATEGY)")
#     print("=" * 70)
    
#     for query in test_queries:
#         print(f"\nQuery: '{query}'")
#         result = word_discovery_single_pass(query, verbose=False)
        
#         print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
#         print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
#         print(f"  Corrected: {result['corrected_query']}")
#         print(f"  📊 Scores: total={result['total_score']}, avg={result['average_score']}, max={result['max_score']}")
#         print(f"  Term breakdown:")
#         for term in result['terms']:
#             print(f"      • {term['search_word']}: {term.get('rank_score', 0)} pts ({term['status']})")
#         print(f"  🎯 Strategy: {get_search_strategy(result).upper()}")


# if __name__ == "__main__":
#     test_word_discovery()



# """
# word_discovery.py
# OPTIMIZED: Single-pass word validation, correction, and bigram detection.
# UPDATED: Score-based search strategy selection using Redis rank field.

# Key Changes:
# - ONE batch Redis call upfront (not multiple passes)
# - Only correct UNKNOWN words (don't touch valid words)
# - Bigram detection in same loop
# - O(1) lookups using pre-fetched data
# - NEW: Extract rank scores from Redis for strategy selection
# - NEW: Location-aware correction (preposition triggers location preference)
# - NEW: Bigram existence as tiebreaker for corrections
# - NEW: Post-correction bigram merge
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional, Set
# from functools import lru_cache

# # Try to import the fast C implementation, fall back to pure Python
# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
#     USE_FAST_LEVENSHTEIN = True
# except ImportError:
#     USE_FAST_LEVENSHTEIN = False

# # Handle both relative and absolute imports
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )
# except ImportError:
#     from searchapi import (
#         RedisLookupTable,
#         validate_word,
#         get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_validate_words_redis,
#         batch_check_bigrams,
#         batch_get_term_metadata,
#         damerau_levenshtein_distance as _python_levenshtein
#     )


# # =============================================================================
# # CONSTANTS - frozensets for O(1) lookup
# # =============================================================================

# ALLOWED_POS: frozenset = frozenset({
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# })


# LOCATION_TYPES: frozenset = frozenset({
#     # Lowercase with underscore
#     "city", "state", "neighborhood", "region", "country", "us_city", "us_state",
#     # Lowercase with space (matches Redis "US City".lower() → "us city")
#     "us city", "us state",
#     # Other variations
#     "location"
# })

# COMPOUND_NOUN_TYPES: frozenset = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# })

# # =============================================================================
# # LOCATION PREPOSITIONS - triggers location-aware correction
# # =============================================================================

# LOCATION_PREPOSITIONS: frozenset = frozenset({
#     "in", "near", "around", "at", "from", "to", "toward", "towards",
#     "outside", "inside", "within", "throughout"
# })

# # =============================================================================
# # SCORE-BASED STRATEGY THRESHOLDS
# # =============================================================================

# SCORE_THRESHOLD_STRICT = 2000   # avg_score > 2000 → strict keyword search
# SCORE_THRESHOLD_MIXED = 500     # avg_score 500-2000 → mixed search
# # avg_score < 500 → semantic search

# # Boost multipliers for correction scoring
# LOCATION_BOOST = 2.0      # Boost for location candidates when preposition precedes
# BIGRAM_BOOST = 3.0        # Boost for candidates that form known bigrams

# # Pre-built dict for O(1) context rule lookup
# LOCAL_CONTEXT_RULES: Dict[Tuple[Optional[str], Optional[str]], List[Tuple[str, float]]] = {
#     # BOTH NEIGHBORS KNOWN
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
#     ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
#     ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
#     # ONLY LEFT NEIGHBOR KNOWN
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
#     ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
#     # ONLY RIGHT NEIGHBOR KNOWN
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
#     (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
# }


# # =============================================================================
# # CACHED HELPERS - O(1) after first call
# # =============================================================================

# @lru_cache(maxsize=10000)
# def cached_levenshtein(word1: str, word2: str) -> int:
#     """Cached Levenshtein distance calculation."""
#     if USE_FAST_LEVENSHTEIN:
#         return _fast_levenshtein(word1, word2)
#     return _python_levenshtein(word1, word2)


# def normalize_pos(pos_value: Any) -> str:
#     """
#     Normalize POS value to a simple string.
    
#     Handles:
#     - None -> 'unknown'
#     - "['noun']" (string) -> 'noun'
#     - ['noun'] (list) -> 'noun'
#     - 'noun' (string) -> 'noun'
#     - Location categories -> 'proper_noun'
#     """
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle string that looks like a list: "['noun']"
#     if isinstance(pos_value, str):
#         pos_value = pos_value.strip()
#         if pos_value.startswith('[') and pos_value.endswith(']'):
#             try:
#                 parsed = json.loads(pos_value.replace("'", '"'))
#                 if isinstance(parsed, list) and parsed:
#                     pos_value = parsed[0]
#                 else:
#                     pos_value = 'unknown'
#             except (json.JSONDecodeError, ValueError):
#                 # Try manual parsing: "['noun']" -> "noun"
#                 inner = pos_value[1:-1].strip()
#                 if inner.startswith("'") and inner.endswith("'"):
#                     pos_value = inner[1:-1]
#                 elif inner.startswith('"') and inner.endswith('"'):
#                     pos_value = inner[1:-1]
#                 else:
#                     pos_value = inner
    
#     # Handle actual list: ['noun']
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Ensure it's a string
#     if not isinstance(pos_value, str):
#         pos_value = str(pos_value) if pos_value else 'unknown'
    
#     # Normalize location types to proper_noun
#     pos_lower = pos_value.lower().strip()
    
#     if pos_lower in ('proper noun', 'proper_noun'):
#         return 'proper_noun'
    
#     if pos_lower in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     # Return as-is if it's a valid POS
#     if pos_lower in ALLOWED_POS:
#         return pos_lower
    
#     return pos_value.lower() if pos_value else 'unknown'


# def is_location_category(category: str) -> bool:
#     """Check if a category indicates a location."""
#     if not category:
#         return False
#     return category.lower().strip() in LOCATION_TYPES


# def extract_rank_score(metadata: Dict[str, Any]) -> int:
#     """
#     Extract rank score from metadata.
    
#     Handles various formats:
#     - int: 1234
#     - str: "1234"
#     - missing: 0
#     """
#     rank = metadata.get('rank', 0)
    
#     if isinstance(rank, int):
#         return rank
    
#     if isinstance(rank, float):
#         return int(rank)
    
#     if isinstance(rank, str):
#         try:
#             return int(rank)
#         except (ValueError, TypeError):
#             return 0
    
#     return 0


# # =============================================================================
# # CONTEXT-BASED PREDICTION - O(1) dict lookups
# # =============================================================================

# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on neighboring words' POS tags."""
#     # Try both neighbors (most specific)
#     key = (left_pos, right_pos)
#     if key in LOCAL_CONTEXT_RULES:
#         return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# # =============================================================================
# # CORRECTION SEARCH - Only for UNKNOWN words
# # =============================================================================

# def find_correction_for_unknown(
#     word: str,
#     left_pos: Optional[str],
#     right_pos: Optional[str],
#     max_distance: int = 2,
#     expect_location: bool = False,
#     previous_word: Optional[str] = None,
#     bigram_results: Optional[Dict[str, Dict]] = None
# ) -> Optional[Dict[str, Any]]:
#     """
#     Find correction for an unknown word using POS context.
    
#     1. Predict expected POS from neighbors
#     2. Generate candidates
#     3. Filter by POS match
#     4. Score candidates with boosts for:
#        - Location category (when expect_location=True)
#        - Bigram existence (when previous_word + candidate forms known bigram)
#     5. Return best match by adjusted score
    
#     Args:
#         word: The unknown word to correct
#         left_pos: POS of previous word
#         right_pos: POS of next word
#         max_distance: Maximum edit distance for candidates
#         expect_location: If True, boost location category candidates (preposition preceded)
#         previous_word: The previous word (for bigram checking)
#         bigram_results: Pre-fetched bigram lookup results
#     """
#     # Predict what POS this word should be
#     prediction = predict_pos_from_context(left_pos, right_pos)
#     predicted_pos = prediction[0] if prediction else 'noun'  # Default to noun
    
#     # If we expect a location, also accept proper_noun
#     if expect_location and predicted_pos == 'noun':
#         predicted_pos = 'proper_noun'
    
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     if not candidates:
#         return None
    
#     # Batch check which candidates exist
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         return None
    
#     word_lower = word.lower()
#     previous_word_lower = previous_word.lower() if previous_word else None
    
#     scored_matches = []
    
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
#         term_lower = item.get('term', '').lower()
#         category = item.get('category', '')
        
#         # Calculate distance
#         distance = cached_levenshtein(word_lower, term_lower)
        
#         if distance > max_distance:
#             continue
        
#         item['distance'] = distance
#         item['normalized_pos'] = item_pos
        
#         # Base rank score
#         base_rank = item.get('rank', 0)
#         adjusted_rank = base_rank
        
#         # =================================================================
#         # CHANGE 1: Location boost when preposition precedes
#         # =================================================================
#         if expect_location and is_location_category(category):
#             adjusted_rank = int(adjusted_rank * LOCATION_BOOST)
#             item['location_boosted'] = True
        
#         # =================================================================
#         # CHANGE 2: Bigram existence boost
#         # =================================================================
#         if previous_word_lower and bigram_results is not None:
#             potential_bigram = f"{previous_word_lower} {term_lower}"
#             if potential_bigram in bigram_results:
#                 adjusted_rank = int(adjusted_rank * BIGRAM_BOOST)
#                 item['bigram_boosted'] = True
#                 item['forms_bigram_with'] = previous_word_lower
        
#         item['adjusted_rank'] = adjusted_rank
        
#         # Check if POS matches prediction
#         pos_match = (
#             item_pos == predicted_pos or
#             (predicted_pos == 'proper_noun' and is_location_category(category)) or
#             (predicted_pos == 'noun' and item_pos in ('noun', 'proper_noun')) or
#             (expect_location and is_location_category(category))  # Accept any location when expected
#         )
        
#         item['pos_match'] = pos_match
#         scored_matches.append(item)
    
#     if not scored_matches:
#         return None
    
#     # Sort by: POS match first, then distance, then adjusted rank (higher = better)
#     scored_matches.sort(key=lambda x: (
#         0 if x.get('pos_match') else 1,  # POS matches first
#         x['distance'],                    # Lower distance better
#         -x.get('adjusted_rank', 0)        # Higher rank better
#     ))
    
#     return scored_matches[0]


# # =============================================================================
# # SINGLE-PASS WORD DISCOVERY
# # =============================================================================

# def word_discovery_single_pass(
#     query: str,
#     pre_validated: Optional[List[Dict[str, Any]]] = None,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     OPTIMIZED: Single-pass word discovery with score extraction.
    
#     1. Batch validate all words (single Redis call)
#     2. Single loop: process each word + detect bigrams
#        - Valid word -> keep it, extract rank score
#        - Unknown word -> find correction using POS context + location hints
#     3. Post-correction bigram merge (check if corrected words form bigrams)
#     4. Calculate score totals for strategy selection
#     5. Return categorized results with scores
#     """
#     words = query.split()
    
#     if not words:
#         return _empty_result(query)
    
#     # =========================================================================
#     # STEP 1: Batch validate all words - ONE Redis call
#     # =========================================================================
    
#     if pre_validated:
#         # Use pre-validated data (from lookup_table)
#         word_data = {}
#         for item in pre_validated:
#             w = item.get('word', '').lower()
#             if w:
#                 metadata = item.get('metadata', item)
#                 word_data[w] = {
#                     'exists': item.get('exists', False),
#                     'pos': normalize_pos(item.get('pos') or metadata.get('pos')),
#                     'metadata': metadata
#                 }
#     else:
#         # Batch validate via Redis
#         validation_cache = batch_validate_words_redis(words)
#         word_data = {}
#         for word in words:
#             w = word.lower()
#             if w in validation_cache:
#                 result = validation_cache[w]
#                 word_data[w] = {
#                     'exists': result.get('is_valid', False),
#                     'pos': normalize_pos(result.get('metadata', {}).get('pos', 'unknown')),
#                     'metadata': result.get('metadata', {})
#                 }
#             else:
#                 word_data[w] = {'exists': False, 'pos': 'unknown', 'metadata': {}}
    
#     # =========================================================================
#     # STEP 2: Prepare bigram checking - ONE Redis call
#     # =========================================================================
    
#     bigram_pairs = []
#     for i in range(len(words) - 1):
#         bigram_pairs.append((words[i].lower(), words[i + 1].lower()))
    
#     bigram_results = batch_check_bigrams(bigram_pairs) if bigram_pairs else {}
    
#     # =========================================================================
#     # STEP 3: Single loop - process words and detect bigrams
#     # =========================================================================
    
#     processed = []  # Final processed words
#     corrections = []  # Corrections made
#     bigrams_found = []  # Bigrams detected
#     skip_next = False
    
#     for i, word in enumerate(words):
#         if skip_next:
#             skip_next = False
#             continue
        
#         word_lower = word.lower()
#         data = word_data.get(word_lower, {'exists': False, 'pos': 'unknown', 'metadata': {}})
        
#         # Check for bigram with next word
#         if i < len(words) - 1:
#             next_word = words[i + 1].lower()
#             bigram_key = f"{word_lower} {next_word}"
            
#             if bigram_key in bigram_results:
#                 # Found a bigram!
#                 bigram_meta = bigram_results[bigram_key]
#                 category = bigram_meta.get('category', '')
#                 bigram_pos = 'proper_noun' if is_location_category(category) else 'noun'
                
#                 # Extract rank score (bigrams get 1.5x multiplier for being multi-word)
#                 bigram_rank = extract_rank_score(bigram_meta)
#                 bigram_rank = int(bigram_rank * 1.5)  # Bigram bonus
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': f"{word} {words[i + 1]}",
#                     'search_word': f"{word} {words[i + 1]}",
#                     'status': 'bigram',
#                     'pos': bigram_pos,
#                     'metadata': bigram_meta,
#                     'rank_score': bigram_rank,
#                     'is_location': is_location_category(category)
#                 })
                
#                 bigrams_found.append({
#                     'bigram': f"{word} {words[i + 1]}",
#                     'category': category,
#                     'rank_score': bigram_rank
#                 })
                
#                 skip_next = True
#                 continue
        
#         # Process single word
#         if data['exists']:
#             # Word is valid - keep it as is
#             # Extract rank score from metadata
#             rank_score = extract_rank_score(data['metadata'])
#             category = data['metadata'].get('category', '')
            
#             processed.append({
#                 'position': i + 1,
#                 'word': word,
#                 'search_word': word,
#                 'status': 'valid',
#                 'pos': data['pos'],
#                 'metadata': data['metadata'],
#                 'rank_score': rank_score,
#                 'is_location': is_location_category(category)
#             })
#         else:
#             # Word is unknown - try to correct it
#             left_pos = processed[-1]['pos'] if processed else None
#             previous_word = processed[-1]['search_word'] if processed else None
            
#             # =================================================================
#             # CHANGE 1: Check if previous word is location preposition
#             # =================================================================
#             expect_location = False
#             if processed:
#                 prev_word_lower = processed[-1]['search_word'].lower()
#                 prev_pos = processed[-1]['pos']
#                 # Check both the word itself and its POS
#                 if prev_word_lower in LOCATION_PREPOSITIONS or prev_pos == 'preposition':
#                     expect_location = True
            
#             # Look ahead for right POS (if next word is valid)
#             right_pos = None
#             if i < len(words) - 1:
#                 next_word_lower = words[i + 1].lower()
#                 next_data = word_data.get(next_word_lower, {})
#                 if next_data.get('exists'):
#                     right_pos = next_data.get('pos')
            
#             # Find correction with location hints and bigram awareness
#             correction = find_correction_for_unknown(
#                 word=word,
#                 left_pos=left_pos,
#                 right_pos=right_pos,
#                 expect_location=expect_location,
#                 previous_word=previous_word,
#                 bigram_results=bigram_results
#             )
            
#             if correction:
#                 corrected_word = correction.get('term', word)
#                 correction_rank = correction.get('adjusted_rank', extract_rank_score(correction))
#                 category = correction.get('category', '')
                
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': corrected_word,
#                     'status': 'corrected',
#                     'pos': normalize_pos(correction.get('pos', 'unknown')),
#                     'metadata': correction,
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0),
#                     'rank_score': correction_rank,
#                     'is_location': is_location_category(category),
#                     'location_boosted': correction.get('location_boosted', False),
#                     'bigram_boosted': correction.get('bigram_boosted', False)
#                 })
#                 corrections.append({
#                     'original': word,
#                     'corrected': corrected_word,
#                     'distance': correction.get('distance', 0),
#                     'rank_score': correction_rank,
#                     'location_boosted': correction.get('location_boosted', False),
#                     'bigram_boosted': correction.get('bigram_boosted', False)
#                 })
#             else:
#                 # No correction found - keep original with zero score
#                 processed.append({
#                     'position': i + 1,
#                     'word': word,
#                     'search_word': word,
#                     'status': 'unknown',
#                     'pos': 'unknown',
#                     'metadata': {},
#                     'rank_score': 0,
#                     'is_location': False
#                 })
    
#     # =========================================================================
#     # STEP 4: Post-correction bigram merge - ONE additional Redis call
#     # =================================================================
#     # Check if adjacent words (after correction) form bigrams we didn't catch
#     # =========================================================================
    
#     if len(processed) >= 2:
#         # Build list of corrected adjacent pairs to check
#         corrected_pairs = []
#         for i in range(len(processed) - 1):
#             # Only check if at least one word was corrected AND neither is already a bigram
#             if (processed[i]['status'] in ('corrected', 'valid') and 
#                 processed[i + 1]['status'] in ('corrected', 'valid') and
#                 processed[i]['status'] != 'bigram' and 
#                 processed[i + 1]['status'] != 'bigram'):
                
#                 w1 = processed[i]['search_word'].lower()
#                 w2 = processed[i + 1]['search_word'].lower()
                
#                 # Skip if we already checked this pair
#                 bigram_key = f"{w1} {w2}"
#                 if bigram_key not in bigram_results:
#                     corrected_pairs.append((w1, w2))
        
#         # Batch check corrected pairs - ONE Redis call
#         if corrected_pairs:
#             corrected_bigram_results = batch_check_bigrams(corrected_pairs)
            
#             # Merge adjacent words that form bigrams
#             merged_processed = []
#             skip_next_merge = False
            
#             for i, item in enumerate(processed):
#                 if skip_next_merge:
#                     skip_next_merge = False
#                     continue
                
#                 # Check if this + next form a newly discovered bigram
#                 if i < len(processed) - 1:
#                     w1 = item['search_word'].lower()
#                     w2 = processed[i + 1]['search_word'].lower()
#                     bigram_key = f"{w1} {w2}"
                    
#                     if bigram_key in corrected_bigram_results:
#                         # Merge into bigram!
#                         bigram_meta = corrected_bigram_results[bigram_key]
#                         category = bigram_meta.get('category', '')
#                         bigram_pos = 'proper_noun' if is_location_category(category) else 'noun'
#                         bigram_rank = extract_rank_score(bigram_meta)
#                         bigram_rank = int(bigram_rank * 1.5)
                        
#                         merged_processed.append({
#                             'position': item['position'],
#                             'word': f"{item['word']} {processed[i + 1]['word']}",
#                             'search_word': f"{item['search_word']} {processed[i + 1]['search_word']}",
#                             'status': 'bigram_merged',  # Indicates post-correction merge
#                             'pos': bigram_pos,
#                             'metadata': bigram_meta,
#                             'rank_score': bigram_rank,
#                             'is_location': is_location_category(category),
#                             'merged_from': [item, processed[i + 1]]
#                         })
                        
#                         bigrams_found.append({
#                             'bigram': f"{item['search_word']} {processed[i + 1]['search_word']}",
#                             'category': category,
#                             'rank_score': bigram_rank,
#                             'merged': True
#                         })
                        
#                         skip_next_merge = True
#                         continue
                
#                 merged_processed.append(item)
            
#             processed = merged_processed
    
#     # =========================================================================
#     # STEP 5: Build results with score calculations
#     # =========================================================================
    
#     valid_terms = [p for p in processed if p['status'] in ('valid', 'corrected', 'bigram', 'bigram_merged')]
#     unknown_terms = [p for p in processed if p['status'] == 'unknown']
#     corrected_terms = [p for p in processed if p['status'] == 'corrected']
#     bigram_terms = [p for p in processed if p['status'] in ('bigram', 'bigram_merged')]
#     location_terms = [p for p in processed if p.get('is_location', False)]
    
#     search_terms = [p['search_word'] for p in processed]
#     corrected_query = ' '.join(search_terms)
    
#     # Calculate score totals for strategy selection
#     total_score = sum(p.get('rank_score', 0) for p in processed)
#     scored_terms = [p for p in processed if p.get('rank_score', 0) > 0]
#     average_score = total_score / len(processed) if processed else 0
#     max_score = max((p.get('rank_score', 0) for p in processed), default=0)
    
#     if verbose:
#         print(f"\n{'='*60}")
#         print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
#         print(f"   Valid terms ({len(valid_terms)}): {[t['search_word'] for t in valid_terms]}")
#         print(f"   Unknown terms ({len(unknown_terms)}): {[t['word'] for t in unknown_terms]}")
#         print(f"   Corrected: {[(c['original'], c['corrected']) for c in corrected_terms]}")
#         print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
#         print(f"   Locations: {[t['search_word'] for t in location_terms]}")
#         print(f"   Search terms: {search_terms}")
#         print(f"   📈 Scores: total={total_score}, avg={average_score:.1f}, max={max_score}")
#         print(f"   Term scores: {[(t['search_word'], t.get('rank_score', 0)) for t in processed]}")
#         print(f"{'='*60}\n")
    
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': corrected_query,
#         'terms': processed,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'corrected_terms': corrected_terms,
#         'bigram_terms': bigram_terms,
#         'location_terms': location_terms,  # NEW: Easy access to detected locations
#         'search_terms': search_terms,
#         'has_unknown': len(unknown_terms) > 0,
#         'all_unknown': len(unknown_terms) == len(processed) and len(processed) > 0,
#         'valid_count': len(valid_terms),
#         'unknown_count': len(unknown_terms),
#         'total_count': len(processed),
#         # Score-based metrics for strategy selection
#         'total_score': total_score,
#         'average_score': round(average_score, 1),
#         'max_score': max_score,
#         'scored_term_count': len(scored_terms)
#     }


# def _empty_result(query: str) -> Dict[str, Any]:
#     """Return empty result structure."""
#     return {
#         'success': True,
#         'query': query,
#         'corrected_query': '',
#         'terms': [],
#         'valid_terms': [],
#         'unknown_terms': [],
#         'corrected_terms': [],
#         'bigram_terms': [],
#         'location_terms': [],
#         'search_terms': [],
#         'has_unknown': False,
#         'all_unknown': True,
#         'valid_count': 0,
#         'unknown_count': 0,
#         'total_count': 0,
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'scored_term_count': 0
#     }


# # =============================================================================
# # PUBLIC API - Compatible with existing code
# # =============================================================================

# def word_discovery_full(
#     query: str,
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Dict[str, Any]:
#     """
#     Full word discovery with categorized output for search integration.
#     This is the main entry point - now uses single-pass processing.
#     """
#     return word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)


# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False,
#     pre_validated: Optional[List[Dict[str, Any]]] = None
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Legacy entry point for compatibility.
#     Returns: (corrections, tuple_array, corrected_query)
#     """
#     result = word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)
    
#     corrections = result.get('corrected_terms', [])
#     tuple_array = [(t['position'], t['pos']) for t in result.get('terms', [])]
#     corrected_query = result.get('corrected_query', query)
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # HELPER FUNCTIONS FOR SEARCH INTEGRATION
# # =============================================================================

# def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
#     """
#     Determine search strategy based on SCORE, not just count.
    
#     Thresholds:
#     - avg_score > 2000: User knows domain vocabulary → strict keyword search
#     - avg_score 500-2000: Partial domain knowledge → mixed search  
#     - avg_score < 500: Generic/conceptual query → semantic search
    
#     High scores indicate terms that appear frequently in your corpus,
#     meaning the user is "speaking the domain language" and keyword
#     matching will be effective.
    
#     Low scores indicate generic vocabulary or terms not in your corpus,
#     meaning semantic/vector search will better understand intent.
#     """
#     average_score = discovery_result.get('average_score', 0)
#     valid_count = discovery_result.get('valid_count', 0)
    
#     # Must have at least one valid term to use strict/mixed
#     if valid_count == 0:
#         return 'semantic'
    
#     # Score-based strategy selection
#     if average_score > SCORE_THRESHOLD_STRICT:
#         return 'strict'
#     elif average_score > SCORE_THRESHOLD_MIXED:
#         return 'mixed'
#     else:
#         return 'semantic'


# def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get terms for strict filtering."""
#     return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


# def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get unknown terms for loose search."""
#     return [t['word'] for t in discovery_result.get('unknown_terms', [])]


# def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
#     """Get all search terms."""
#     return discovery_result.get('search_terms', [])


# def get_term_scores(discovery_result: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """Get list of terms with their scores."""
#     return [
#         {
#             'term': t['search_word'],
#             'score': t.get('rank_score', 0),
#             'status': t['status']
#         }
#         for t in discovery_result.get('terms', [])
#     ]


# def get_high_score_terms(discovery_result: Dict[str, Any], min_score: int = 500) -> List[str]:
#     """Get only terms with score above threshold."""
#     return [
#         t['search_word']
#         for t in discovery_result.get('terms', [])
#         if t.get('rank_score', 0) >= min_score
#     ]


# def get_location_terms(discovery_result: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """
#     Get detected location terms from discovery result.
    
#     Returns list of dicts with:
#     - term: The location name
#     - category: The location type (US City, US State, etc.)
#     - is_bigram: Whether it's a multi-word location
#     """
#     return [
#         {
#             'term': t['search_word'],
#             'category': t.get('metadata', {}).get('category', ''),
#             'is_bigram': t['status'] in ('bigram', 'bigram_merged')
#         }
#         for t in discovery_result.get('location_terms', [])
#     ]


# # =============================================================================
# # OPTIMIZED QUERY PROCESSING
# # =============================================================================

# def process_query_optimized(
#     query: str,
#     verbose: bool = False
# ) -> Dict[str, Any]:
#     """
#     Optimized end-to-end query processing for Typesense search.
    
#     - Single validation pass
#     - Batched bigram detection
#     - Categorized output for search strategy
#     - Score-based strategy selection
#     """
#     try:
#         from .searchapi import lookup_table
#     except ImportError:
#         from searchapi import lookup_table
    
#     # Step 1: Lookup with validation cache
#     lookup_result = lookup_table(query, return_validation_cache=True)
    
#     if not lookup_result.get('success', False):
#         words = query.split()
#         return {
#             'success': False,
#             'query': query,
#             'corrected_query': query,
#             'terms': [],
#             'valid_terms': [],
#             'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown', 'rank_score': 0} 
#                              for w in words],
#             'corrected_terms': [],
#             'bigram_terms': [],
#             'location_terms': [],
#             'search_terms': words,
#             'has_unknown': True,
#             'all_unknown': True,
#             'valid_count': 0,
#             'unknown_count': len(words),
#             'total_count': len(words),
#             'total_score': 0,
#             'average_score': 0,
#             'max_score': 0,
#             'scored_term_count': 0,
#             'search_strategy': 'semantic',
#             'cache_hit': False,
#             'error': lookup_result.get('error', 'Lookup failed')
#         }
    
#     # Step 2: Single-pass word discovery
#     result = word_discovery_single_pass(
#         query,
#         pre_validated=lookup_result.get('terms', []),
#         verbose=verbose
#     )
    
#     # Step 3: Determine search strategy using scores
#     result['search_strategy'] = get_search_strategy(result)
#     result['cache_hit'] = lookup_result.get('cache_hit', False)
    
#     return result


# # =============================================================================
# # TEST FUNCTION
# # =============================================================================

# def test_word_discovery():
#     """Test function to verify word discovery and scoring is working."""
#     test_queries = [
#         "tuskegee airmen",           # Should be high score (domain terms)
#         "the quick brown fox",        # Should be low score (generic)
#         "new york city",              # Should be high score (location bigram)
#         "black doctors",              # Should be medium score
#         "african american history",   # Should be high score
#         "how did people vote",        # Should be low score (generic phrasing)
#         "hbcu civil rights",          # Should be high score
#         # NEW: Location-aware correction tests
#         "restaurants in atlenta",     # Should correct to "atlanta" (location after preposition)
#         "hbcus near new yrok",        # Should correct "yrok" to "york" AND form bigram "new york"
#         "hotels in goergia",          # Should correct to "georgia"
#     ]
    
#     print("=" * 70)
#     print("WORD DISCOVERY TEST (SCORE-BASED STRATEGY + LOCATION AWARE)")
#     print("=" * 70)
    
#     for query in test_queries:
#         print(f"\nQuery: '{query}'")
#         result = word_discovery_single_pass(query, verbose=False)
        
#         print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
#         print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
#         print(f"  Corrected: {result['corrected_query']}")
#         print(f"  Locations: {[t['search_word'] for t in result.get('location_terms', [])]}")
#         print(f"  📊 Scores: total={result['total_score']}, avg={result['average_score']}, max={result['max_score']}")
#         print(f"  Term breakdown:")
#         for term in result['terms']:
#             boosts = []
#             if term.get('location_boosted'):
#                 boosts.append('LOC')
#             if term.get('bigram_boosted'):
#                 boosts.append('BIGRAM')
#             boost_str = f" [{','.join(boosts)}]" if boosts else ""
#             print(f"      • {term['search_word']}: {term.get('rank_score', 0)} pts ({term['status']}){boost_str}")
#         print(f"  🎯 Strategy: {get_search_strategy(result).upper()}")


# if __name__ == "__main__":
#     test_word_discovery()


# """
# word_discovery.py
# Three-pass word validation, correction, and bigram detection.

# Pass 1: Validate each word
# Pass 2: Pattern-based correction for unknowns
# Pass 3: Bigram detection

# PERFORMANCE OPTIMIZATION:
# - Uses in-memory vocabulary cache for O(1) lookups
# - Falls back to Redis only for cache misses and spelling corrections
# - Typical query: 0.01ms (cached) vs 300ms per word (Redis)
# """

# import json
# import logging
# import time
# from typing import Dict, Any, List, Tuple, Optional

# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # =============================================================================
# # IMPORTS - Cache first, Redis as fallback
# # =============================================================================

# # Try to import vocabulary cache (fast path)
# try:
#     from .vocabulary_cache import vocab_cache, ensure_loaded
#     CACHE_AVAILABLE = True
# except ImportError:
#     try:
#         from vocabulary_cache import vocab_cache, ensure_loaded
#         CACHE_AVAILABLE = True
#     except ImportError:
#         CACHE_AVAILABLE = False
#         vocab_cache = None

# # Import Redis functions (fallback and spelling correction)
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word as redis_validate_word,
#         get_term_metadata as redis_get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_check_bigrams,
#     )
#     REDIS_AVAILABLE = True
# except ImportError:
#     try:
#         from searchapi import (
#             RedisLookupTable,
#             validate_word as redis_validate_word,
#             get_term_metadata as redis_get_term_metadata,
#             get_suggestions,
#             generate_candidates_smart,
#             batch_check_candidates,
#             batch_check_bigrams,
#         )
#         REDIS_AVAILABLE = True
#     except ImportError:
#         REDIS_AVAILABLE = False


# # =============================================================================
# # LOGGING SETUP
# # =============================================================================

# logger = logging.getLogger(__name__)

# # Set to True to see detailed timing in logs
# DEBUG_TIMING = False


# # =============================================================================
# # CONSTANTS
# # =============================================================================

# ALLOWED_POS = {
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# }

# LOCATION_TYPES = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "us city", "us_city", "us state", "us_state", "location"
# })

# COMPOUND_NOUN_TYPES = {
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", 
#     "disease", "animal", "us city", "us_city", "us state", "us_state"
# }

# SENTENCE_PATTERNS = {
#     # ==========================================================================
#     # DETERMINER patterns (Redis returns "determiner" for "the", "a", "an")
#     # ==========================================================================
#     "determiner": [
#         ("noun",),
#         ("adjective",),
#         ("adjective", "noun"),
#         ("noun", "verb"),
#         ("noun", "noun"),
#         ("adjective", "noun", "verb"),
#         ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"),
#         ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"),
#         ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"),
#         ("noun", "verb", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # ARTICLE patterns (alias for determiner, kept for compatibility)
#     # ==========================================================================
#     "article": [
#         ("noun",),
#         ("adjective",),
#         ("adjective", "noun"),
#         ("noun", "verb"),
#         ("adjective", "noun", "verb"),
#         ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#     ],
    
#     # ==========================================================================
#     # PRONOUN patterns
#     # ==========================================================================
#     "pronoun": [
#         ("verb",),
#         ("be",),
#         ("verb", "noun"),
#         ("verb", "adverb"),
#         ("verb", "adjective"),
#         ("be", "adjective"),
#         ("be", "noun"),
#         ("verb", "determiner", "noun"),
#         ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"),
#         ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"),
#         ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"),
#         ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"),
#         ("be", "determiner", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # NOUN patterns
#     # ==========================================================================
#     "noun": [
#         ("verb",),
#         ("be",),
#         ("verb", "noun"),
#         ("verb", "adverb"),
#         ("verb", "adjective"),
#         ("be", "adjective"),
#         ("be", "noun"),
#         ("verb", "determiner", "noun"),
#         ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"),
#         ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"),
#         ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"),
#     ],
    
#     # ==========================================================================
#     # ADJECTIVE patterns
#     # ==========================================================================
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"),
#         ("noun", "be"),
#         ("adjective", "noun"),
#         ("noun", "verb", "adverb"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#         ("noun", "verb", "noun"),
#         ("adjective", "noun", "verb"),
#     ],
    
#     # ==========================================================================
#     # VERB patterns
#     # ==========================================================================
#     "verb": [
#         ("noun",),
#         ("adverb",),
#         ("adjective",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("adjective", "noun"),
#         ("preposition", "noun"),
#         ("adverb", "adverb"),
#         ("noun", "noun"),
#         ("determiner", "adjective", "noun"),
#         ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"),
#         ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # PREPOSITION patterns
#     # ==========================================================================
#     "preposition": [
#         ("noun",),
#         ("proper_noun",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("adjective", "noun"),
#         ("determiner", "adjective", "noun"),
#         ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # ADVERB patterns
#     # ==========================================================================
#     "adverb": [
#         ("verb",),
#         ("adjective",),
#         ("adverb",),
#         ("verb", "noun"),
#         ("verb", "determiner", "noun"),
#         ("adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # BE verb patterns
#     # ==========================================================================
#     "be": [
#         ("adjective",),
#         ("noun",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("preposition", "noun"),
#         ("adverb", "adjective"),
#         ("determiner", "adjective", "noun"),
#     ],
# }


# LOCAL_CONTEXT_RULES = {
#     # ==========================================================================
#     # BOTH NEIGHBORS KNOWN (highest confidence)
#     # ==========================================================================
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
    
#     # ==========================================================================
#     # ONLY LEFT NEIGHBOR KNOWN
#     # ==========================================================================
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
    
#     # ==========================================================================
#     # ONLY RIGHT NEIGHBOR KNOWN
#     # ==========================================================================
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }


# # =============================================================================
# # CACHE INITIALIZATION
# # =============================================================================

# def _ensure_cache_loaded():
#     """Ensure vocabulary cache is loaded. Called internally."""
#     global CACHE_AVAILABLE
    
#     if not CACHE_AVAILABLE:
#         return False
    
#     try:
#         if not vocab_cache.loaded:
#             logger.info("Loading vocabulary cache...")
#             ensure_loaded()
#         return vocab_cache.loaded
#     except Exception as e:
#         logger.error(f"Failed to load vocabulary cache: {e}")
#         CACHE_AVAILABLE = False
#         return False


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def normalize_pos(pos_value: Any) -> str:
#     """Normalize POS value, converting location types to proper_noun."""
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle JSON string format: '["determiner"]' -> ["determiner"]
#     if isinstance(pos_value, str) and pos_value.startswith('['):
#         try:
#             pos_value = json.loads(pos_value)
#         except json.JSONDecodeError:
#             pass
    
#     # Handle list format: ["determiner"] -> "determiner"
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Normalize location types
#     pos_lower = pos_value.lower() if isinstance(pos_value, str) else str(pos_value)
    
#     if pos_lower in LOCATION_TYPES or 'city' in pos_lower or 'state' in pos_lower:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     return pos_value


# def is_compound_type(subtext: str) -> bool:
#     """Check if subtext indicates a compound noun type."""
#     if not subtext:
#         return False
#     return subtext.lower() in COMPOUND_NOUN_TYPES


# def is_location_type(category: str) -> bool:
#     """Check if category indicates a location type."""
#     if not category:
#         return False
#     category_lower = category.lower().replace(' ', '_')
#     return (
#         category_lower in LOCATION_TYPES or
#         'city' in category_lower or
#         'state' in category_lower or
#         'location' in category_lower
#     )


# # =============================================================================
# # UNIFIED LOOKUP FUNCTIONS (Cache first, Redis fallback)
# # =============================================================================

# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a word - check if it exists in vocabulary.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word: The word to validate
    
#     Returns:
#         Dict with 'is_valid', 'metadata', etc.
#     """
#     word_lower = word.lower().strip()
    
#     if not word_lower:
#         return {'is_valid': False, 'word': word}
    
#     start_time = time.perf_counter() if DEBUG_TIMING else None
    
#     # Try cache first (fast path)
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_term(word_lower)
        
#         if metadata:
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Cache hit for '{word}': {elapsed:.3f}ms")
            
#             return {
#                 'is_valid': True,
#                 'word': word_lower,
#                 'metadata': metadata,
#                 'source': 'cache'
#             }
        
#         # Check if it's a stopword (still valid, just no metadata)
#         if vocab_cache.is_stopword(word_lower):
#             return {
#                 'is_valid': True,
#                 'word': word_lower,
#                 'metadata': {'pos': 'stopword', 'category': 'stopword'},
#                 'source': 'cache'
#             }
    
#     # Fall back to Redis (slow path)
#     if REDIS_AVAILABLE:
#         if DEBUG_TIMING:
#             redis_start = time.perf_counter()
        
#         result = redis_validate_word(word)
        
#         if DEBUG_TIMING:
#             elapsed = (time.perf_counter() - redis_start) * 1000
#             logger.debug(f"Redis lookup for '{word}': {elapsed:.3f}ms")
        
#         if result.get('is_valid'):
#             result['source'] = 'redis'
#             return result
    
#     # Not found anywhere
#     return {
#         'is_valid': False,
#         'word': word_lower,
#         'source': 'not_found'
#     }


# def get_term_metadata(term: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         term: The term to look up
    
#     Returns:
#         Metadata dict or None
#     """
#     term_lower = term.lower().strip()
    
#     if not term_lower:
#         return None
    
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_term(term_lower)
#         if metadata:
#             return metadata
    
#     # Fall back to Redis
#     if REDIS_AVAILABLE:
#         return redis_get_term_metadata(term)
    
#     return None


# def check_bigram_exists(word1: str, word2: str) -> Optional[Dict[str, Any]]:
#     """
#     Check if two words form a bigram.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word1: First word
#         word2: Second word
    
#     Returns:
#         Bigram metadata if found, None otherwise
#     """
#     word1_lower = word1.lower().strip()
#     word2_lower = word2.lower().strip()
    
#     if not word1_lower or not word2_lower:
#         return None
    
#     start_time = time.perf_counter() if DEBUG_TIMING else None
    
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_bigram(word1_lower, word2_lower)
        
#         if metadata:
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Cache bigram hit '{word1} {word2}': {elapsed:.3f}ms")
            
#             # Ensure 'exists' key for compatibility
#             metadata['exists'] = True
#             return metadata
    
#     # Fall back to Redis
#     if REDIS_AVAILABLE:
#         bigram = f"{word1_lower} {word2_lower}"
#         metadata = redis_get_term_metadata(bigram)
        
#         if metadata and metadata.get('exists'):
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Redis bigram hit '{word1} {word2}': {elapsed:.3f}ms")
#             return metadata
    
#     return None


# def check_trigram_exists(word1: str, word2: str, word3: str) -> Optional[Dict[str, Any]]:
#     """
#     Check if three words form a trigram.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word1: First word
#         word2: Second word
#         word3: Third word
    
#     Returns:
#         Trigram metadata if found, None otherwise
#     """
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_trigram(word1, word2, word3)
#         if metadata:
#             metadata['exists'] = True
#             return metadata
    
#     # Fall back to checking overlapping bigrams
#     bigram1 = check_bigram_exists(word1, word2)
#     bigram2 = check_bigram_exists(word2, word3)
    
#     if bigram1 and bigram2:
#         return {
#             'exists': True,
#             'trigram': f"{word1} {word2} {word3}",
#             'parts': [bigram1, bigram2],
#             'category': 'trigram'
#         }
    
#     return None


# # =============================================================================
# # PASS 1: WORD VALIDATION
# # =============================================================================

# def validate_words(words: List[str], verbose: bool = False) -> List[Dict[str, Any]]:
#     """
#     Pass 1: Validate each word in the query.
    
#     Args:
#         words: List of words from the query
#         verbose: Whether to print debug output
    
#     Returns:
#         List of validation results for each word
#     """
#     results = []
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 1: WORD VALIDATION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     for position, word in enumerate(words, start=1):
#         if verbose:
#             print(f"\n📍 Position {position}: Checking '{word}'...")
        
#         validation = validate_word(word)
        
#         if validation.get('is_valid'):
#             # Get metadata (may already be in validation result)
#             metadata = validation.get('metadata') or get_term_metadata(word) or {}
#             pos = normalize_pos(metadata.get('pos', 'unknown'))
#             category = metadata.get('category', '')
            
#             if verbose:
#                 print(f"   ✅ VALID (source: {validation.get('source', 'unknown')})")
#                 print(f"      POS: {pos}")
#                 print(f"      Category: {category}")
#                 print(f"      Rank: {metadata.get('rank', 0)}")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'valid',
#                 'pos': pos,
#                 'category': category,
#                 'metadata': metadata
#             })
#         else:
#             if verbose:
#                 print(f"   ❓ UNKNOWN")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'unknown',
#                 'pos': 'unknown',
#                 'category': '',
#                 'metadata': {}
#             })
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 1 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 1 (validate_words) completed in {elapsed:.2f}ms for {len(words)} words")
    
#     return results


# # =============================================================================
# # PASS 2: PATTERN-BASED CORRECTION
# # =============================================================================

# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3,
#     verbose: bool = False
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Used as fallback for single-word queries with no context.
    
#     NOTE: This always uses Redis as it requires fuzzy matching.
#     """
#     if not REDIS_AVAILABLE:
#         return None
    
#     if verbose:
#         print(f"      🔍 Fallback search for '{word}' (no POS filter)...")
    
#     # Try the suggestions API first
#     suggestions = get_suggestions(word, limit=10)
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = damerau_levenshtein_distance(word.lower(), term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             best = matches[0]
#             if verbose:
#                 print(f"      ✅ Suggestion found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     # Try with candidates
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             best = matches[0]
#             if verbose:
#                 print(f"      ✅ Candidate found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     if verbose:
#         print(f"      ❌ No matches found within distance {max_distance}")
    
#     return None


# def is_short_query(validation_results: List[Dict[str, Any]]) -> bool:
#     """Check if this is a short query (1-2 words)."""
#     return len(validation_results) <= 2


# def has_context(position: int, tuple_array: List[Tuple[int, str]]) -> bool:
#     """Check if a position has any known context (valid neighbors)."""
#     for pos, tag in tuple_array:
#         if pos == position - 1 and tag in ALLOWED_POS:
#             return True
#         if pos == position + 1 and tag in ALLOWED_POS:
#             return True
#     return False


# def build_tuple_array(validation_results: List[Dict[str, Any]]) -> List[Tuple[int, str]]:
#     """Build tuple array of (position, POS) from validation results."""
#     return [(r['position'], r['pos']) for r in validation_results]


# def get_left_right_context(
#     position: int,
#     tuple_array: List[Tuple[int, str]]
# ) -> Tuple[Optional[str], Optional[str]]:
#     """Get POS of left and right neighbors for a position."""
#     left_pos = None
#     right_pos = None
    
#     for pos, tag in tuple_array:
#         if pos == position - 1 and tag in ALLOWED_POS:
#             left_pos = tag
#             break
    
#     for pos, tag in tuple_array:
#         if pos == position + 1 and tag in ALLOWED_POS:
#             right_pos = tag
#             break
    
#     return left_pos, right_pos


# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on left/right context using LOCAL_CONTEXT_RULES."""
#     # Try both neighbors
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left neighbor only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right neighbor only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def match_sentence_pattern(
#     tuple_array: List[Tuple[int, str]],
#     unknown_position: int
# ) -> Optional[str]:
#     """Match sentence against known patterns to predict POS for unknown."""
#     # Find the starting POS
#     starting_pos = None
#     for pos, tag in tuple_array:
#         if tag in SENTENCE_PATTERNS:
#             starting_pos = tag
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [tag for pos, tag in tuple_array if pos > 1]
#     unknown_index = unknown_position - 2
    
#     for pattern in patterns:
#         if len(pattern) >= len(sequence):
#             matches = True
#             for i, tag in enumerate(sequence):
#                 if tag != 'unknown' and i < len(pattern) and tag != pattern[i]:
#                     matches = False
#                     break
            
#             if matches and unknown_index < len(pattern):
#                 return pattern[unknown_index]
    
#     return None


# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2,
#     verbose: bool = False
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by required POS.
    
#     NOTE: This always uses Redis as it requires fuzzy matching.
#     """
#     if not REDIS_AVAILABLE:
#         return None
    
#     if verbose:
#         print(f"      🔍 Searching for {required_pos} near '{word}'...")
    
#     candidates = generate_candidates_smart(word, max_candidates=50)
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         if verbose:
#             print(f"      ❌ No candidates found")
#         return None
    
#     matches = []
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
        
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and is_location_type(item.get('category', ''))) or
#             (required_pos == 'noun' and item_pos in ['noun', 'proper_noun'])
#         )
        
#         if pos_match:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         # Fallback: try without POS filter
#         if verbose:
#             print(f"      🔄 No {required_pos} found, trying any POS...")
#         for item in found:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         if verbose:
#             print(f"      ❌ No matches within distance {max_distance}")
#         return None
    
#     matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     best = matches[0]
#     if verbose:
#         print(f"      ✅ Found: '{best['term']}' (distance: {best['distance']}, POS: {best.get('pos', 'unknown')})")
    
#     return best


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """Get ALL valid POS options for a given context."""
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def detect_pattern_violations(
#     validation_results: List[Dict[str, Any]],
#     tuple_array: List[Tuple[int, str]]
# ) -> List[Dict[str, Any]]:
#     """Detect words that are valid but violate grammatical patterns."""
#     violations = []
    
#     for i, result in enumerate(validation_results):
#         if result['status'] != 'valid':
#             continue
        
#         position = result['position']
#         current_pos = result['pos']
        
#         left_pos, right_pos = get_left_right_context(position, tuple_array)
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_list = [pos for pos, confidence in valid_options]
        
#         if current_pos not in valid_pos_list:
#             expected_pos, confidence = valid_options[0]
            
#             violations.append({
#                 'position': position,
#                 'word': result['word'],
#                 'current_pos': current_pos,
#                 'expected_pos': expected_pos,
#                 'valid_options': valid_pos_list,
#                 'confidence': confidence,
#                 'context': (left_pos, right_pos)
#             })
    
#     return violations


# def correct_pattern_violations(
#     validation_results: List[Dict[str, Any]],
#     violations: List[Dict[str, Any]],
#     tuple_array: List[Tuple[int, str]],
#     verbose: bool = False
# ) -> List[Dict[str, Any]]:
#     """Attempt to correct words that violate grammatical patterns."""
#     for violation in violations:
#         position = violation['position']
#         word = violation['word']
#         expected_pos = violation['expected_pos']
        
#         if verbose:
#             print(f"\n📍 Position {position}: Pattern violation for '{word}'...")
#             print(f"   Current POS: {violation['current_pos']}")
#             print(f"   Expected POS: {expected_pos} (confidence: {violation['confidence']:.0%})")
#             print(f"   Context: [{violation['context'][0]}] _{word}_ [{violation['context'][1]}]")
        
#         correction = search_with_pos_filter(word, expected_pos, verbose=verbose)
        
#         if correction:
#             for result in validation_results:
#                 if result['position'] == position:
#                     result['status'] = 'corrected'
#                     result['corrected'] = correction['term']
#                     result['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                     result['distance'] = correction['distance']
#                     result['metadata'] = correction
#                     result['correction_reason'] = 'pattern_violation'
                    
#                     for i, (pos, tag) in enumerate(tuple_array):
#                         if pos == position:
#                             tuple_array[i] = (pos, result['pos'])
#                             break
#                     break
#         elif verbose:
#             print(f"   ❌ No correction found for '{word}'")
    
#     return validation_results


# def predict_pos_for_unknowns(
#     validation_results: List[Dict[str, Any]],
#     verbose: bool = False
# ) -> List[Dict[str, Any]]:
#     """
#     Pass 2: Predict POS and correct unknown words AND pattern violations.
#     """
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 2: PATTERN-BASED CORRECTION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     tuple_array = build_tuple_array(validation_results)
    
#     # STEP 1: Correct unknown words
#     unknowns = [r for r in validation_results if r['status'] == 'unknown']
    
#     if unknowns:
#         if verbose:
#             print(f"\n   Found {len(unknowns)} unknown word(s)")
        
#         for unknown in unknowns:
#             position = unknown['position']
#             word = unknown['word']
            
#             if verbose:
#                 print(f"\n📍 Position {position}: Correcting '{word}'...")
            
#             left_pos, right_pos = get_left_right_context(position, tuple_array)
            
#             if verbose:
#                 print(f"   Context: [{left_pos}] _{word}_ [{right_pos}]")
            
#             prediction = predict_pos_from_context(left_pos, right_pos)
            
#             if prediction:
#                 predicted_pos, confidence = prediction
#                 if verbose:
#                     print(f"   📊 Context prediction: {predicted_pos} (confidence: {confidence:.0%})")
#             else:
#                 predicted_pos = match_sentence_pattern(tuple_array, position)
#                 if predicted_pos:
#                     if verbose:
#                         print(f"   📊 Pattern prediction: {predicted_pos}")
#                 else:
#                     predicted_pos = 'noun'
#                     if verbose:
#                         print(f"   📊 Default prediction: {predicted_pos}")
            
#             correction = search_with_pos_filter(word, predicted_pos, verbose=verbose)
            
#             if correction:
#                 unknown['status'] = 'corrected'
#                 unknown['corrected'] = correction['term']
#                 unknown['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                 unknown['distance'] = correction['distance']
#                 unknown['metadata'] = correction
                
#                 for i, (pos, tag) in enumerate(tuple_array):
#                     if pos == position:
#                         tuple_array[i] = (pos, unknown['pos'])
#                         break
#             elif verbose:
#                 print(f"   ❌ No correction found for '{word}'")
#     elif verbose:
#         print("\n   ✅ No unknown words to correct")
    
#     # STEP 2: Detect and correct pattern violations
#     if verbose:
#         print("\n" + "-" * 40)
#         print("   Checking for pattern violations...")
#         print("-" * 40)
    
#     tuple_array = build_tuple_array(validation_results)
#     violations = detect_pattern_violations(validation_results, tuple_array)
    
#     if violations:
#         if verbose:
#             print(f"\n   Found {len(violations)} pattern violation(s)")
#         validation_results = correct_pattern_violations(
#             validation_results, violations, tuple_array, verbose=verbose
#         )
#     elif verbose:
#         print("\n   ✅ No pattern violations detected")
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 2 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 2 (predict_pos_for_unknowns) completed in {elapsed:.2f}ms")
    
#     return validation_results


# # =============================================================================
# # PASS 3: BIGRAM DETECTION
# # =============================================================================

# def detect_bigrams(
#     validation_results: List[Dict[str, Any]],
#     verbose: bool = False
# ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
#     """
#     Pass 3: Detect bigrams in the corrected words.
#     """
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 3: BIGRAM DETECTION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     bigrams_found = []
#     positions_to_merge = set()
    
#     for i in range(len(validation_results) - 1):
#         current = validation_results[i]
#         next_item = validation_results[i + 1]
        
#         if current['position'] in positions_to_merge:
#             continue
        
#         word1 = current.get('corrected', current['word'])
#         word2 = next_item.get('corrected', next_item['word'])
        
#         if verbose:
#             print(f"\n📍 Checking: '{word1}' + '{word2}'...")
        
#         bigram_metadata = check_bigram_exists(word1, word2)
        
#         if bigram_metadata:
#             category = bigram_metadata.get('category', '')
#             subtext = bigram_metadata.get('subtext', category)
#             entity = bigram_metadata.get('entity', 'bigram')
            
#             if verbose:
#                 print(f"   ✅ BIGRAM FOUND")
#                 print(f"      Display: {bigram_metadata.get('display', '')}")
#                 print(f"      Category: {category}")
#                 print(f"      Entity: {entity}")
            
#             if is_location_type(category) or is_location_type(subtext):
#                 bigram_pos = 'proper_noun'
#             else:
#                 bigram_pos = 'noun'
            
#             bigrams_found.append({
#                 'position_start': current['position'],
#                 'position_end': next_item['position'],
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'category': category,
#                 'subtext': subtext,
#                 'entity': entity,
#                 'metadata': bigram_metadata
#             })
            
#             positions_to_merge.add(current['position'])
#             positions_to_merge.add(next_item['position'])
#         elif verbose:
#             print(f"   ❌ Not a bigram")
    
#     if not bigrams_found and verbose:
#         print("\n   ✅ No bigrams detected")
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 3 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 3 (detect_bigrams) completed in {elapsed:.2f}ms")
    
#     return validation_results, bigrams_found


# def merge_bigrams_into_result(
#     validation_results: List[Dict[str, Any]],
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Merge detected bigrams into the final result."""
#     if not bigrams:
#         return validation_results
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for result in validation_results:
#         if skip_next:
#             skip_next = False
#             continue
        
#         position = result['position']
        
#         if position in bigram_starts:
#             bigram = bigram_starts[position]
#             merged.append({
#                 'position': position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'category': bigram['category'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif position not in bigram_positions:
#             merged.append(result)
    
#     return merged


# # =============================================================================
# # SINGLE-PASS QUERY CLASSIFICATION (FAST PATH)
# # =============================================================================

# def word_discovery_single_pass(query: str) -> Dict[str, Dict[str, Any]]:
#     """
#     Fast single-pass query classification using cache.
    
#     This is the FAST PATH for queries that don't need spelling correction.
#     Uses vocabulary cache for O(1) lookups.
    
#     Args:
#         query: The input query string
    
#     Returns:
#         Dict mapping terms to their metadata
#     """
#     if not query or not query.strip():
#         return {}
    
#     start_time = time.perf_counter()
    
#     # Use cache's classify_query if available (fastest)
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         result = vocab_cache.classify_query(query)
        
#         elapsed = (time.perf_counter() - start_time) * 1000
#         logger.debug(f"word_discovery_single_pass (cache) completed in {elapsed:.2f}ms")
        
#         # Convert to expected format
#         return result.get('terms', {})
    
#     # Fall back to word-by-word validation
#     words = query.lower().split()
#     results = {}
    
#     for word in words:
#         metadata = get_term_metadata(word)
#         if metadata:
#             results[word] = metadata
    
#     elapsed = (time.perf_counter() - start_time) * 1000
#     logger.debug(f"word_discovery_single_pass (fallback) completed in {elapsed:.2f}ms")
    
#     return results


# # =============================================================================
# # MAIN ORCHESTRATOR
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, ignored - uses internal connections)
#         prefix: Redis key prefix (optional, ignored)
#         verbose: Whether to print debug output
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     overall_start = time.perf_counter()
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print(f"🔍 PROCESSING QUERY: '{query}'")
#         print("=" * 60)
        
#         # Show cache status
#         if CACHE_AVAILABLE and vocab_cache.loaded:
#             print(f"   Cache: LOADED ({vocab_cache.term_count} terms)")
#         else:
#             print(f"   Cache: NOT AVAILABLE (using Redis)")
    
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate each word
#     # =========================================================================
#     validation_results = validate_words(words, verbose=verbose)
    
#     # =========================================================================
#     # PASS 2: Pattern-based correction for unknowns
#     # =========================================================================
#     validation_results = predict_pos_for_unknowns(validation_results, verbose=verbose)
    
#     # =========================================================================
#     # PASS 3: Bigram detection
#     # =========================================================================
#     validation_results, bigrams = detect_bigrams(validation_results, verbose=verbose)
    
#     # Merge bigrams into results
#     final_results = merge_bigrams_into_result(validation_results, bigrams)
    
#     # =========================================================================
#     # BUILD OUTPUT
#     # =========================================================================
    
#     corrections = []
#     for r in validation_results:
#         if r['status'] == 'corrected':
#             corrections.append({
#                 'position': r['position'],
#                 'original': r['word'],
#                 'corrected': r['corrected'],
#                 'distance': r.get('distance', 0),
#                 'pos': r['pos'],
#                 'is_bigram': False
#             })
    
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
    
#     corrected_words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             corrected_words.append(r['word'])
#         elif r['status'] == 'corrected':
#             corrected_words.append(r['corrected'])
#         else:
#             corrected_words.append(r['word'])
    
#     corrected_query = ' '.join(corrected_words)
    
#     # =========================================================================
#     # FINAL SUMMARY
#     # =========================================================================
#     overall_elapsed = (time.perf_counter() - overall_start) * 1000
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print("📊 FINAL SUMMARY")
#         print("=" * 60)
#         print(f"   Original:    '{query}'")
#         print(f"   Corrected:   '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}")
#         print(f"   Bigrams:     {len(bigrams)}")
#         print(f"   Total time:  {overall_elapsed:.2f}ms")
        
#         if corrections:
#             print("\n   Word corrections:")
#             for c in corrections:
#                 print(f"      • '{c['original']}' → '{c['corrected']}' ({c['pos']})")
        
#         if bigrams:
#             print("\n   Bigrams detected:")
#             for b in bigrams:
#                 print(f"      • '{b['bigram']}' ({b.get('category', b.get('subtext', ''))})")
        
#         print("\n   Final structure:")
#         for r in final_results:
#             print(f"      [{r['position']}] {r['word']} → {r['pos']}")
        
#         print("=" * 60 + "\n")
    
#     logger.info(f"Query '{query}' processed in {overall_elapsed:.2f}ms (corrections: {len(corrections)}, bigrams: {len(bigrams)})")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # LOCATION EXTRACTION (for search filtering)
# # =============================================================================

# def extract_locations_from_query(query: str) -> List[Dict[str, Any]]:
#     """
#     Extract location entities from a query.
    
#     This is useful for filtering search results by location.
    
#     Args:
#         query: The input query string
    
#     Returns:
#         List of location dicts with 'term', 'category', 'type'
#     """
#     locations = []
    
#     # Use cache's classify_query if available
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         result = vocab_cache.classify_query(query)
        
#         for loc in result.get('locations', []):
#             metadata = result.get('terms', {}).get(loc, {})
#             locations.append({
#                 'term': loc,
#                 'category': metadata.get('category', 'location'),
#                 'type': 'city' if 'city' in metadata.get('category', '').lower() else 'state'
#             })
        
#         return locations
    
#     # Fall back to word-by-word check
#     words = query.lower().split()
    
#     for word in words:
#         metadata = get_term_metadata(word)
#         if metadata and is_location_type(metadata.get('category', '')):
#             locations.append({
#                 'term': word,
#                 'category': metadata.get('category', 'location'),
#                 'type': 'city' if 'city' in metadata.get('category', '').lower() else 'state'
#             })
    
#     # Check bigrams
#     for i in range(len(words) - 1):
#         bigram_meta = check_bigram_exists(words[i], words[i + 1])
#         if bigram_meta and is_location_type(bigram_meta.get('category', '')):
#             locations.append({
#                 'term': f"{words[i]} {words[i + 1]}",
#                 'category': bigram_meta.get('category', 'location'),
#                 'type': 'city' if 'city' in bigram_meta.get('category', '').lower() else 'state'
#             })
    
#     return locations


# # =============================================================================
# # CACHE STATUS / MANAGEMENT
# # =============================================================================

# def get_cache_status() -> Dict[str, Any]:
#     """Get current cache status for monitoring."""
#     if CACHE_AVAILABLE and vocab_cache:
#         return {
#             'cache_available': True,
#             'cache_loaded': vocab_cache.loaded,
#             **vocab_cache.status()
#         }
#     else:
#         return {
#             'cache_available': False,
#             'cache_loaded': False,
#             'using_redis': REDIS_AVAILABLE
#         }


# def reload_cache() -> bool:
#     """Force reload the vocabulary cache from Redis."""
#     if CACHE_AVAILABLE and vocab_cache:
#         return vocab_cache.reload()
#     return False

# """
# word_discovery.py
# Three-pass word validation, correction, and bigram detection.

# Pass 1: Validate each word
# Pass 2: Pattern-based correction for unknowns
# Pass 3: Bigram detection

# PERFORMANCE OPTIMIZATION:
# - Uses in-memory vocabulary cache for O(1) lookups
# - Falls back to Redis only for cache misses and spelling corrections
# - Typical query: 0.01ms (cached) vs 300ms per word (Redis)
# """

# import json
# import logging
# import time
# from typing import Dict, Any, List, Tuple, Optional

# from pyxdameraulevenshtein import damerau_levenshtein_distance

# # =============================================================================
# # IMPORTS - Cache first, Redis as fallback
# # =============================================================================

# # Try to import vocabulary cache (fast path)
# try:
#     from .vocabulary_cache import vocab_cache, ensure_loaded
#     CACHE_AVAILABLE = True
# except ImportError:
#     try:
#         from vocabulary_cache import vocab_cache, ensure_loaded
#         CACHE_AVAILABLE = True
#     except ImportError:
#         CACHE_AVAILABLE = False
#         vocab_cache = None

# # Import Redis functions (fallback and spelling correction)
# try:
#     from .searchapi import (
#         RedisLookupTable,
#         validate_word as redis_validate_word,
#         get_term_metadata as redis_get_term_metadata,
#         get_suggestions,
#         generate_candidates_smart,
#         batch_check_candidates,
#         batch_check_bigrams,
#     )
#     REDIS_AVAILABLE = True
# except ImportError:
#     try:
#         from searchapi import (
#             RedisLookupTable,
#             validate_word as redis_validate_word,
#             get_term_metadata as redis_get_term_metadata,
#             get_suggestions,
#             generate_candidates_smart,
#             batch_check_candidates,
#             batch_check_bigrams,
#         )
#         REDIS_AVAILABLE = True
#     except ImportError:
#         REDIS_AVAILABLE = False


# # =============================================================================
# # LOGGING SETUP
# # =============================================================================

# logger = logging.getLogger(__name__)

# # Set to True to see detailed timing in logs
# DEBUG_TIMING = False


# # =============================================================================
# # CONSTANTS
# # =============================================================================

# ALLOWED_POS = {
#     "pronoun", "noun", "verb", "article", "adjective",
#     "preposition", "adverb", "be", "modal", "auxiliary",
#     "proper_noun", "relative_pronoun", "wh_pronoun", "determiner",
#     "quantifier", "numeral", "participle", "gerund",
#     "infinitive_marker", "particle", "negation", "conjunction", "interjection"
# }

# LOCATION_TYPES = frozenset({
#     "city", "state", "neighborhood", "region", "country",
#     "us city", "us_city", "us state", "us_state", "location"
# })

# COMPOUND_NOUN_TYPES = {
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", 
#     "disease", "animal", "us city", "us_city", "us state", "us_state"
# }

# SENTENCE_PATTERNS = {
#     # ==========================================================================
#     # DETERMINER patterns (Redis returns "determiner" for "the", "a", "an")
#     # ==========================================================================
#     "determiner": [
#         ("noun",),
#         ("adjective",),
#         ("adjective", "noun"),
#         ("noun", "verb"),
#         ("noun", "noun"),
#         ("adjective", "noun", "verb"),
#         ("adjective", "adjective", "noun"),
#         ("noun", "verb", "adverb"),
#         ("noun", "verb", "noun"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#         ("adjective", "noun", "verb", "noun"),
#         ("adjective", "noun", "be", "adjective"),
#         ("noun", "verb", "determiner", "noun"),
#         ("noun", "verb", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # ARTICLE patterns (alias for determiner, kept for compatibility)
#     # ==========================================================================
#     "article": [
#         ("noun",),
#         ("adjective",),
#         ("adjective", "noun"),
#         ("noun", "verb"),
#         ("adjective", "noun", "verb"),
#         ("adjective", "adjective", "noun"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#     ],
    
#     # ==========================================================================
#     # PRONOUN patterns
#     # ==========================================================================
#     "pronoun": [
#         ("verb",),
#         ("be",),
#         ("verb", "noun"),
#         ("verb", "adverb"),
#         ("verb", "adjective"),
#         ("be", "adjective"),
#         ("be", "noun"),
#         ("verb", "determiner", "noun"),
#         ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"),
#         ("verb", "preposition", "noun"),
#         ("be", "determiner", "noun"),
#         ("be", "preposition", "noun"),
#         ("verb", "determiner", "adjective", "noun"),
#         ("verb", "article", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"),
#         ("be", "determiner", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # NOUN patterns
#     # ==========================================================================
#     "noun": [
#         ("verb",),
#         ("be",),
#         ("verb", "noun"),
#         ("verb", "adverb"),
#         ("verb", "adjective"),
#         ("be", "adjective"),
#         ("be", "noun"),
#         ("verb", "determiner", "noun"),
#         ("verb", "article", "noun"),
#         ("verb", "adjective", "noun"),
#         ("verb", "preposition", "noun"),
#         ("be", "preposition", "noun"),
#         ("be", "determiner", "noun"),
#         ("verb", "determiner", "adjective", "noun"),
#         ("verb", "preposition", "determiner", "noun"),
#     ],
    
#     # ==========================================================================
#     # ADJECTIVE patterns
#     # ==========================================================================
#     "adjective": [
#         ("noun",),
#         ("noun", "verb"),
#         ("noun", "be"),
#         ("adjective", "noun"),
#         ("noun", "verb", "adverb"),
#         ("noun", "be", "adjective"),
#         ("noun", "be", "noun"),
#         ("noun", "verb", "noun"),
#         ("adjective", "noun", "verb"),
#     ],
    
#     # ==========================================================================
#     # VERB patterns
#     # ==========================================================================
#     "verb": [
#         ("noun",),
#         ("adverb",),
#         ("adjective",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("adjective", "noun"),
#         ("preposition", "noun"),
#         ("adverb", "adverb"),
#         ("noun", "noun"),
#         ("determiner", "adjective", "noun"),
#         ("article", "adjective", "noun"),
#         ("preposition", "determiner", "noun"),
#         ("preposition", "adjective", "noun"),
#         ("noun", "determiner", "noun"),
#         ("preposition", "determiner", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # PREPOSITION patterns
#     # ==========================================================================
#     "preposition": [
#         ("noun",),
#         ("proper_noun",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("adjective", "noun"),
#         ("determiner", "adjective", "noun"),
#         ("article", "adjective", "noun"),
#         ("adjective", "adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # ADVERB patterns
#     # ==========================================================================
#     "adverb": [
#         ("verb",),
#         ("adjective",),
#         ("adverb",),
#         ("verb", "noun"),
#         ("verb", "determiner", "noun"),
#         ("adjective", "noun"),
#     ],
    
#     # ==========================================================================
#     # BE verb patterns
#     # ==========================================================================
#     "be": [
#         ("adjective",),
#         ("noun",),
#         ("determiner", "noun"),
#         ("article", "noun"),
#         ("preposition", "noun"),
#         ("adverb", "adjective"),
#         ("determiner", "adjective", "noun"),
#     ],
# }


# LOCAL_CONTEXT_RULES = {
#     # ==========================================================================
#     # BOTH NEIGHBORS KNOWN (highest confidence)
#     # ==========================================================================
#     ("determiner", "noun"): [("adjective", 0.95)],
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],
#     ("determiner", "verb"): [("noun", 0.90)],
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
#     ("adjective", "noun"): [("adjective", 0.85)],
#     ("adjective", "verb"): [("noun", 0.90)],
#     ("adjective", "adjective"): [("noun", 0.70)],
#     ("noun", "noun"): [("verb", 0.85)],
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("noun", "adverb"): [("verb", 0.90)],
#     ("noun", "preposition"): [("verb", 0.85)],
#     ("noun", "determiner"): [("verb", 0.90)],
#     ("noun", "article"): [("verb", 0.90)],
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],
#     ("verb", "verb"): [("adverb", 0.75)],
#     ("verb", "adjective"): [("adverb", 0.85)],
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],
#     ("pronoun", "noun"): [("verb", 0.90)],
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],
#     ("pronoun", "determiner"): [("verb", 0.90)],
#     ("pronoun", "article"): [("verb", 0.90)],
#     ("pronoun", "adverb"): [("verb", 0.85)],
#     ("pronoun", "preposition"): [("verb", 0.90)],
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],
#     ("preposition", "verb"): [("noun", 0.80)],
#     ("adverb", "noun"): [("adjective", 0.85)],
#     ("adverb", "verb"): [("adverb", 0.75)],
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],
#     ("be", "adjective"): [("adverb", 0.90)],
#     ("be", "preposition"): [("adverb", 0.80)],
    
#     # ==========================================================================
#     # ONLY LEFT NEIGHBOR KNOWN
#     # ==========================================================================
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],
#     ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
#     ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
    
#     # ==========================================================================
#     # ONLY RIGHT NEIGHBOR KNOWN
#     # ==========================================================================
#     (None, "noun"): [("adjective", 0.90), ("determiner", 0.85), ("article", 0.85)],
#     (None, "verb"): [("noun", 0.85), ("pronoun", 0.80), ("adverb", 0.70)],
#     (None, "adjective"): [("adverb", 0.85), ("determiner", 0.75), ("article", 0.75)],
#     (None, "adverb"): [("verb", 0.80), ("adverb", 0.70)],
#     (None, "preposition"): [("noun", 0.85), ("verb", 0.80)],
#     (None, "determiner"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "article"): [("verb", 0.85), ("preposition", 0.75), ("noun", 0.70)],
#     (None, "pronoun"): [("verb", 0.75), ("preposition", 0.70), ("conjunction", 0.65)],
#     (None, "proper_noun"): [("preposition", 0.80), ("verb", 0.75)],
# }


# # =============================================================================
# # SCORE THRESHOLDS FOR STRATEGY SELECTION
# # =============================================================================

# # These thresholds determine search strategy based on term scores
# STRATEGY_THRESHOLDS = {
#     'strict': {
#         'min_total_score': 1000,      # Total score across all terms
#         'min_average_score': 300,     # Average score per term
#         'min_valid_ratio': 0.7,       # Ratio of valid terms
#     },
#     'mixed': {
#         'min_total_score': 500,
#         'min_average_score': 150,
#         'min_valid_ratio': 0.5,
#     },
#     # Below these thresholds -> semantic search
# }


# # =============================================================================
# # CACHE INITIALIZATION
# # =============================================================================

# def _ensure_cache_loaded():
#     """Ensure vocabulary cache is loaded. Called internally."""
#     global CACHE_AVAILABLE
    
#     if not CACHE_AVAILABLE:
#         return False
    
#     try:
#         if not vocab_cache.loaded:
#             logger.info("Loading vocabulary cache...")
#             ensure_loaded()
#         return vocab_cache.loaded
#     except Exception as e:
#         logger.error(f"Failed to load vocabulary cache: {e}")
#         CACHE_AVAILABLE = False
#         return False


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def normalize_pos(pos_value: Any) -> str:
#     """Normalize POS value, converting location types to proper_noun."""
#     if pos_value is None:
#         return 'unknown'
    
#     # Handle JSON string format: '["determiner"]' -> ["determiner"]
#     if isinstance(pos_value, str) and pos_value.startswith('['):
#         try:
#             pos_value = json.loads(pos_value)
#         except json.JSONDecodeError:
#             pass
    
#     # Handle list format: ["determiner"] -> "determiner"
#     if isinstance(pos_value, list):
#         pos_value = pos_value[0] if pos_value else 'unknown'
    
#     # Normalize location types
#     pos_lower = pos_value.lower() if isinstance(pos_value, str) else str(pos_value)
    
#     if pos_lower in LOCATION_TYPES or 'city' in pos_lower or 'state' in pos_lower:
#         return 'proper_noun'
    
#     if pos_lower in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     return pos_value


# def is_compound_type(subtext: str) -> bool:
#     """Check if subtext indicates a compound noun type."""
#     if not subtext:
#         return False
#     return subtext.lower() in COMPOUND_NOUN_TYPES


# def is_location_type(category: str) -> bool:
#     """Check if category indicates a location type."""
#     if not category:
#         return False
#     category_lower = category.lower().replace(' ', '_')
#     return (
#         category_lower in LOCATION_TYPES or
#         'city' in category_lower or
#         'state' in category_lower or
#         'location' in category_lower
#     )


# # =============================================================================
# # UNIFIED LOOKUP FUNCTIONS (Cache first, Redis fallback)
# # =============================================================================

# def validate_word(word: str) -> Dict[str, Any]:
#     """
#     Validate a word - check if it exists in vocabulary.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word: The word to validate
    
#     Returns:
#         Dict with 'is_valid', 'metadata', etc.
#     """
#     word_lower = word.lower().strip()
    
#     if not word_lower:
#         return {'is_valid': False, 'word': word}
    
#     start_time = time.perf_counter() if DEBUG_TIMING else None
    
#     # Try cache first (fast path)
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_term(word_lower)
        
#         if metadata:
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Cache hit for '{word}': {elapsed:.3f}ms")
            
#             return {
#                 'is_valid': True,
#                 'word': word_lower,
#                 'metadata': metadata,
#                 'source': 'cache'
#             }
        
#         # Check if it's a stopword (still valid, just no metadata)
#         if vocab_cache.is_stopword(word_lower):
#             return {
#                 'is_valid': True,
#                 'word': word_lower,
#                 'metadata': {'pos': 'stopword', 'category': 'stopword'},
#                 'source': 'cache'
#             }
    
#     # Fall back to Redis (slow path)
#     if REDIS_AVAILABLE:
#         if DEBUG_TIMING:
#             redis_start = time.perf_counter()
        
#         result = redis_validate_word(word)
        
#         if DEBUG_TIMING:
#             elapsed = (time.perf_counter() - redis_start) * 1000
#             logger.debug(f"Redis lookup for '{word}': {elapsed:.3f}ms")
        
#         if result.get('is_valid'):
#             result['source'] = 'redis'
#             return result
    
#     # Not found anywhere
#     return {
#         'is_valid': False,
#         'word': word_lower,
#         'source': 'not_found'
#     }


# def get_term_metadata(term: str) -> Optional[Dict[str, Any]]:
#     """
#     Get metadata for a term.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         term: The term to look up
    
#     Returns:
#         Metadata dict or None
#     """
#     term_lower = term.lower().strip()
    
#     if not term_lower:
#         return None
    
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_term(term_lower)
#         if metadata:
#             return metadata
    
#     # Fall back to Redis
#     if REDIS_AVAILABLE:
#         return redis_get_term_metadata(term)
    
#     return None


# def check_bigram_exists(word1: str, word2: str) -> Optional[Dict[str, Any]]:
#     """
#     Check if two words form a bigram.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word1: First word
#         word2: Second word
    
#     Returns:
#         Bigram metadata if found, None otherwise
#     """
#     word1_lower = word1.lower().strip()
#     word2_lower = word2.lower().strip()
    
#     if not word1_lower or not word2_lower:
#         return None
    
#     start_time = time.perf_counter() if DEBUG_TIMING else None
    
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_bigram(word1_lower, word2_lower)
        
#         if metadata:
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Cache bigram hit '{word1} {word2}': {elapsed:.3f}ms")
            
#             # Ensure 'exists' key for compatibility
#             metadata['exists'] = True
#             return metadata
    
#     # Fall back to Redis
#     if REDIS_AVAILABLE:
#         bigram = f"{word1_lower} {word2_lower}"
#         metadata = redis_get_term_metadata(bigram)
        
#         if metadata and metadata.get('exists'):
#             if DEBUG_TIMING:
#                 elapsed = (time.perf_counter() - start_time) * 1000
#                 logger.debug(f"Redis bigram hit '{word1} {word2}': {elapsed:.3f}ms")
#             return metadata
    
#     return None


# def check_trigram_exists(word1: str, word2: str, word3: str) -> Optional[Dict[str, Any]]:
#     """
#     Check if three words form a trigram.
#     Uses cache first, falls back to Redis if needed.
    
#     Args:
#         word1: First word
#         word2: Second word
#         word3: Third word
    
#     Returns:
#         Trigram metadata if found, None otherwise
#     """
#     # Try cache first
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         metadata = vocab_cache.get_trigram(word1, word2, word3)
#         if metadata:
#             metadata['exists'] = True
#             return metadata
    
#     # Fall back to checking overlapping bigrams
#     bigram1 = check_bigram_exists(word1, word2)
#     bigram2 = check_bigram_exists(word2, word3)
    
#     if bigram1 and bigram2:
#         return {
#             'exists': True,
#             'trigram': f"{word1} {word2} {word3}",
#             'parts': [bigram1, bigram2],
#             'category': 'trigram'
#         }
    
#     return None


# # =============================================================================
# # PASS 1: WORD VALIDATION
# # =============================================================================

# def validate_words(words: List[str], verbose: bool = False) -> List[Dict[str, Any]]:
#     """
#     Pass 1: Validate each word in the query.
    
#     Args:
#         words: List of words from the query
#         verbose: Whether to print debug output
    
#     Returns:
#         List of validation results for each word
#     """
#     results = []
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 1: WORD VALIDATION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     for position, word in enumerate(words, start=1):
#         if verbose:
#             print(f"\n📍 Position {position}: Checking '{word}'...")
        
#         validation = validate_word(word)
        
#         if validation.get('is_valid'):
#             # Get metadata (may already be in validation result)
#             metadata = validation.get('metadata') or get_term_metadata(word) or {}
#             pos = normalize_pos(metadata.get('pos', 'unknown'))
#             category = metadata.get('category', '')
            
#             if verbose:
#                 print(f"   ✅ VALID (source: {validation.get('source', 'unknown')})")
#                 print(f"      POS: {pos}")
#                 print(f"      Category: {category}")
#                 print(f"      Rank: {metadata.get('rank', 0)}")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'valid',
#                 'pos': pos,
#                 'category': category,
#                 'metadata': metadata
#             })
#         else:
#             if verbose:
#                 print(f"   ❓ UNKNOWN")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'unknown',
#                 'pos': 'unknown',
#                 'category': '',
#                 'metadata': {}
#             })
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 1 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 1 (validate_words) completed in {elapsed:.2f}ms for {len(words)} words")
    
#     return results


# # =============================================================================
# # PASS 2: PATTERN-BASED CORRECTION
# # =============================================================================

# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3,
#     verbose: bool = False
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Used as fallback for single-word queries with no context.
    
#     NOTE: This always uses Redis as it requires fuzzy matching.
#     """
#     if not REDIS_AVAILABLE:
#         return None
    
#     if verbose:
#         print(f"      🔍 Fallback search for '{word}' (no POS filter)...")
    
#     # Try the suggestions API first
#     suggestions = get_suggestions(word, limit=10)
    
#     if suggestions:
#         matches = []
#         for suggestion in suggestions:
#             term = suggestion.get('term', '')
#             distance = damerau_levenshtein_distance(word.lower(), term.lower())
#             if distance <= max_distance:
#                 suggestion['distance'] = distance
#                 matches.append(suggestion)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             best = matches[0]
#             if verbose:
#                 print(f"      ✅ Suggestion found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     # Try with candidates
#     candidates = generate_candidates_smart(word, max_candidates=100)
#     found = batch_check_candidates(candidates)
    
#     if found:
#         matches = []
#         for item in found:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
        
#         if matches:
#             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
#             best = matches[0]
#             if verbose:
#                 print(f"      ✅ Candidate found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     if verbose:
#         print(f"      ❌ No matches found within distance {max_distance}")
    
#     return None


# def is_short_query(validation_results: List[Dict[str, Any]]) -> bool:
#     """Check if this is a short query (1-2 words)."""
#     return len(validation_results) <= 2


# def has_context(position: int, tuple_array: List[Tuple[int, str]]) -> bool:
#     """Check if a position has any known context (valid neighbors)."""
#     for pos, tag in tuple_array:
#         if pos == position - 1 and tag in ALLOWED_POS:
#             return True
#         if pos == position + 1 and tag in ALLOWED_POS:
#             return True
#     return False


# def build_tuple_array(validation_results: List[Dict[str, Any]]) -> List[Tuple[int, str]]:
#     """Build tuple array of (position, POS) from validation results."""
#     return [(r['position'], r['pos']) for r in validation_results]


# def get_left_right_context(
#     position: int,
#     tuple_array: List[Tuple[int, str]]
# ) -> Tuple[Optional[str], Optional[str]]:
#     """Get POS of left and right neighbors for a position."""
#     left_pos = None
#     right_pos = None
    
#     for pos, tag in tuple_array:
#         if pos == position - 1 and tag in ALLOWED_POS:
#             left_pos = tag
#             break
    
#     for pos, tag in tuple_array:
#         if pos == position + 1 and tag in ALLOWED_POS:
#             right_pos = tag
#             break
    
#     return left_pos, right_pos


# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """Predict POS based on left/right context using LOCAL_CONTEXT_RULES."""
#     # Try both neighbors
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try left neighbor only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     # Try right neighbor only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key][0]
    
#     return None


# def match_sentence_pattern(
#     tuple_array: List[Tuple[int, str]],
#     unknown_position: int
# ) -> Optional[str]:
#     """Match sentence against known patterns to predict POS for unknown."""
#     # Find the starting POS
#     starting_pos = None
#     for pos, tag in tuple_array:
#         if tag in SENTENCE_PATTERNS:
#             starting_pos = tag
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
#     sequence = [tag for pos, tag in tuple_array if pos > 1]
#     unknown_index = unknown_position - 2
    
#     for pattern in patterns:
#         if len(pattern) >= len(sequence):
#             matches = True
#             for i, tag in enumerate(sequence):
#                 if tag != 'unknown' and i < len(pattern) and tag != pattern[i]:
#                     matches = False
#                     break
            
#             if matches and unknown_index < len(pattern):
#                 return pattern[unknown_index]
    
#     return None


# def search_with_pos_filter(
#     word: str,
#     required_pos: str,
#     max_distance: int = 2,
#     verbose: bool = False
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by required POS.
    
#     NOTE: This always uses Redis as it requires fuzzy matching.
#     """
#     if not REDIS_AVAILABLE:
#         return None
    
#     if verbose:
#         print(f"      🔍 Searching for {required_pos} near '{word}'...")
    
#     candidates = generate_candidates_smart(word, max_candidates=50)
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         if verbose:
#             print(f"      ❌ No candidates found")
#         return None
    
#     matches = []
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
        
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and is_location_type(item.get('category', ''))) or
#             (required_pos == 'noun' and item_pos in ['noun', 'proper_noun'])
#         )
        
#         if pos_match:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         # Fallback: try without POS filter
#         if verbose:
#             print(f"      🔄 No {required_pos} found, trying any POS...")
#         for item in found:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         if verbose:
#             print(f"      ❌ No matches within distance {max_distance}")
#         return None
    
#     matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     best = matches[0]
#     if verbose:
#         print(f"      ✅ Found: '{best['term']}' (distance: {best['distance']}, POS: {best.get('pos', 'unknown')})")
    
#     return best


# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """Get ALL valid POS options for a given context."""
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def detect_pattern_violations(
#     validation_results: List[Dict[str, Any]],
#     tuple_array: List[Tuple[int, str]]
# ) -> List[Dict[str, Any]]:
#     """Detect words that are valid but violate grammatical patterns."""
#     violations = []
    
#     for i, result in enumerate(validation_results):
#         if result['status'] != 'valid':
#             continue
        
#         position = result['position']
#         current_pos = result['pos']
        
#         left_pos, right_pos = get_left_right_context(position, tuple_array)
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             continue
        
#         valid_pos_list = [pos for pos, confidence in valid_options]
        
#         if current_pos not in valid_pos_list:
#             expected_pos, confidence = valid_options[0]
            
#             violations.append({
#                 'position': position,
#                 'word': result['word'],
#                 'current_pos': current_pos,
#                 'expected_pos': expected_pos,
#                 'valid_options': valid_pos_list,
#                 'confidence': confidence,
#                 'context': (left_pos, right_pos)
#             })
    
#     return violations


# def correct_pattern_violations(
#     validation_results: List[Dict[str, Any]],
#     violations: List[Dict[str, Any]],
#     tuple_array: List[Tuple[int, str]],
#     verbose: bool = False
# ) -> List[Dict[str, Any]]:
#     """Attempt to correct words that violate grammatical patterns."""
#     for violation in violations:
#         position = violation['position']
#         word = violation['word']
#         expected_pos = violation['expected_pos']
        
#         if verbose:
#             print(f"\n📍 Position {position}: Pattern violation for '{word}'...")
#             print(f"   Current POS: {violation['current_pos']}")
#             print(f"   Expected POS: {expected_pos} (confidence: {violation['confidence']:.0%})")
#             print(f"   Context: [{violation['context'][0]}] _{word}_ [{violation['context'][1]}]")
        
#         correction = search_with_pos_filter(word, expected_pos, verbose=verbose)
        
#         if correction:
#             for result in validation_results:
#                 if result['position'] == position:
#                     result['status'] = 'corrected'
#                     result['corrected'] = correction['term']
#                     result['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                     result['distance'] = correction['distance']
#                     result['metadata'] = correction
#                     result['correction_reason'] = 'pattern_violation'
                    
#                     for i, (pos, tag) in enumerate(tuple_array):
#                         if pos == position:
#                             tuple_array[i] = (pos, result['pos'])
#                             break
#                     break
#         elif verbose:
#             print(f"   ❌ No correction found for '{word}'")
    
#     return validation_results


# def predict_pos_for_unknowns(
#     validation_results: List[Dict[str, Any]],
#     verbose: bool = False
# ) -> List[Dict[str, Any]]:
#     """
#     Pass 2: Predict POS and correct unknown words AND pattern violations.
#     """
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 2: PATTERN-BASED CORRECTION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     tuple_array = build_tuple_array(validation_results)
    
#     # STEP 1: Correct unknown words
#     unknowns = [r for r in validation_results if r['status'] == 'unknown']
    
#     if unknowns:
#         if verbose:
#             print(f"\n   Found {len(unknowns)} unknown word(s)")
        
#         for unknown in unknowns:
#             position = unknown['position']
#             word = unknown['word']
            
#             if verbose:
#                 print(f"\n📍 Position {position}: Correcting '{word}'...")
            
#             left_pos, right_pos = get_left_right_context(position, tuple_array)
            
#             if verbose:
#                 print(f"   Context: [{left_pos}] _{word}_ [{right_pos}]")
            
#             prediction = predict_pos_from_context(left_pos, right_pos)
            
#             if prediction:
#                 predicted_pos, confidence = prediction
#                 if verbose:
#                     print(f"   📊 Context prediction: {predicted_pos} (confidence: {confidence:.0%})")
#             else:
#                 predicted_pos = match_sentence_pattern(tuple_array, position)
#                 if predicted_pos:
#                     if verbose:
#                         print(f"   📊 Pattern prediction: {predicted_pos}")
#                 else:
#                     predicted_pos = 'noun'
#                     if verbose:
#                         print(f"   📊 Default prediction: {predicted_pos}")
            
#             correction = search_with_pos_filter(word, predicted_pos, verbose=verbose)
            
#             if correction:
#                 unknown['status'] = 'corrected'
#                 unknown['corrected'] = correction['term']
#                 unknown['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                 unknown['distance'] = correction['distance']
#                 unknown['metadata'] = correction
                
#                 for i, (pos, tag) in enumerate(tuple_array):
#                     if pos == position:
#                         tuple_array[i] = (pos, unknown['pos'])
#                         break
#             elif verbose:
#                 print(f"   ❌ No correction found for '{word}'")
#     elif verbose:
#         print("\n   ✅ No unknown words to correct")
    
#     # STEP 2: Detect and correct pattern violations
#     if verbose:
#         print("\n" + "-" * 40)
#         print("   Checking for pattern violations...")
#         print("-" * 40)
    
#     tuple_array = build_tuple_array(validation_results)
#     violations = detect_pattern_violations(validation_results, tuple_array)
    
#     if violations:
#         if verbose:
#             print(f"\n   Found {len(violations)} pattern violation(s)")
#         validation_results = correct_pattern_violations(
#             validation_results, violations, tuple_array, verbose=verbose
#         )
#     elif verbose:
#         print("\n   ✅ No pattern violations detected")
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 2 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 2 (predict_pos_for_unknowns) completed in {elapsed:.2f}ms")
    
#     return validation_results


# # =============================================================================
# # PASS 3: BIGRAM DETECTION
# # =============================================================================

# def detect_bigrams(
#     validation_results: List[Dict[str, Any]],
#     verbose: bool = False
# ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
#     """
#     Pass 3: Detect bigrams in the corrected words.
#     """
#     if verbose:
#         print("\n" + "=" * 60)
#         print("PASS 3: BIGRAM DETECTION")
#         print("=" * 60)
    
#     start_time = time.perf_counter()
    
#     bigrams_found = []
#     positions_to_merge = set()
    
#     for i in range(len(validation_results) - 1):
#         current = validation_results[i]
#         next_item = validation_results[i + 1]
        
#         if current['position'] in positions_to_merge:
#             continue
        
#         word1 = current.get('corrected', current['word'])
#         word2 = next_item.get('corrected', next_item['word'])
        
#         if verbose:
#             print(f"\n📍 Checking: '{word1}' + '{word2}'...")
        
#         bigram_metadata = check_bigram_exists(word1, word2)
        
#         if bigram_metadata:
#             category = bigram_metadata.get('category', '')
#             subtext = bigram_metadata.get('subtext', category)
#             entity = bigram_metadata.get('entity', 'bigram')
            
#             if verbose:
#                 print(f"   ✅ BIGRAM FOUND")
#                 print(f"      Display: {bigram_metadata.get('display', '')}")
#                 print(f"      Category: {category}")
#                 print(f"      Entity: {entity}")
            
#             if is_location_type(category) or is_location_type(subtext):
#                 bigram_pos = 'proper_noun'
#             else:
#                 bigram_pos = 'noun'
            
#             bigrams_found.append({
#                 'position_start': current['position'],
#                 'position_end': next_item['position'],
#                 'word1': word1,
#                 'word2': word2,
#                 'bigram': f"{word1} {word2}",
#                 'pos': bigram_pos,
#                 'category': category,
#                 'subtext': subtext,
#                 'entity': entity,
#                 'metadata': bigram_metadata
#             })
            
#             positions_to_merge.add(current['position'])
#             positions_to_merge.add(next_item['position'])
#         elif verbose:
#             print(f"   ❌ Not a bigram")
    
#     if not bigrams_found and verbose:
#         print("\n   ✅ No bigrams detected")
    
#     elapsed = (time.perf_counter() - start_time) * 1000
    
#     if verbose:
#         print(f"\n   Pass 3 completed in {elapsed:.2f}ms")
    
#     logger.debug(f"Pass 3 (detect_bigrams) completed in {elapsed:.2f}ms")
    
#     return validation_results, bigrams_found


# def merge_bigrams_into_result(
#     validation_results: List[Dict[str, Any]],
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """Merge detected bigrams into the final result."""
#     if not bigrams:
#         return validation_results
    
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     merged = []
#     skip_next = False
    
#     for result in validation_results:
#         if skip_next:
#             skip_next = False
#             continue
        
#         position = result['position']
        
#         if position in bigram_starts:
#             bigram = bigram_starts[position]
#             merged.append({
#                 'position': position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'category': bigram['category'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif position not in bigram_positions:
#             merged.append(result)
    
#     return merged


# # =============================================================================
# # SINGLE-PASS QUERY CLASSIFICATION (FAST PATH)
# # =============================================================================

# def word_discovery_single_pass(query: str) -> Dict[str, Dict[str, Any]]:
#     """
#     Fast single-pass query classification using cache.
    
#     This is the FAST PATH for queries that don't need spelling correction.
#     Uses vocabulary cache for O(1) lookups.
    
#     Args:
#         query: The input query string
    
#     Returns:
#         Dict mapping terms to their metadata
#     """
#     if not query or not query.strip():
#         return {}
    
#     start_time = time.perf_counter()
    
#     # Use cache's classify_query if available (fastest)
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         result = vocab_cache.classify_query(query)
        
#         elapsed = (time.perf_counter() - start_time) * 1000
#         logger.debug(f"word_discovery_single_pass (cache) completed in {elapsed:.2f}ms")
        
#         # Convert to expected format
#         return result.get('terms', {})
    
#     # Fall back to word-by-word validation
#     words = query.lower().split()
#     results = {}
    
#     for word in words:
#         metadata = get_term_metadata(word)
#         if metadata:
#             results[word] = metadata
    
#     elapsed = (time.perf_counter() - start_time) * 1000
#     logger.debug(f"word_discovery_single_pass (fallback) completed in {elapsed:.2f}ms")
    
#     return results


# # =============================================================================
# # MAIN ORCHESTRATOR
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix",
#     verbose: bool = False
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, ignored - uses internal connections)
#         prefix: Redis key prefix (optional, ignored)
#         verbose: Whether to print debug output
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     overall_start = time.perf_counter()
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print(f"🔍 PROCESSING QUERY: '{query}'")
#         print("=" * 60)
        
#         # Show cache status
#         if CACHE_AVAILABLE and vocab_cache.loaded:
#             print(f"   Cache: LOADED ({vocab_cache.term_count} terms)")
#         else:
#             print(f"   Cache: NOT AVAILABLE (using Redis)")
    
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate each word
#     # =========================================================================
#     validation_results = validate_words(words, verbose=verbose)
    
#     # =========================================================================
#     # PASS 2: Pattern-based correction for unknowns
#     # =========================================================================
#     validation_results = predict_pos_for_unknowns(validation_results, verbose=verbose)
    
#     # =========================================================================
#     # PASS 3: Bigram detection
#     # =========================================================================
#     validation_results, bigrams = detect_bigrams(validation_results, verbose=verbose)
    
#     # Merge bigrams into results
#     final_results = merge_bigrams_into_result(validation_results, bigrams)
    
#     # =========================================================================
#     # BUILD OUTPUT
#     # =========================================================================
    
#     corrections = []
#     for r in validation_results:
#         if r['status'] == 'corrected':
#             corrections.append({
#                 'position': r['position'],
#                 'original': r['word'],
#                 'corrected': r['corrected'],
#                 'distance': r.get('distance', 0),
#                 'pos': r['pos'],
#                 'is_bigram': False
#             })
    
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
    
#     corrected_words = []
#     for r in final_results:
#         if r['status'] == 'bigram':
#             corrected_words.append(r['word'])
#         elif r['status'] == 'corrected':
#             corrected_words.append(r['corrected'])
#         else:
#             corrected_words.append(r['word'])
    
#     corrected_query = ' '.join(corrected_words)
    
#     # =========================================================================
#     # FINAL SUMMARY
#     # =========================================================================
#     overall_elapsed = (time.perf_counter() - overall_start) * 1000
    
#     if verbose:
#         print("\n" + "=" * 60)
#         print("📊 FINAL SUMMARY")
#         print("=" * 60)
#         print(f"   Original:    '{query}'")
#         print(f"   Corrected:   '{corrected_query}'")
#         print(f"   Corrections: {len(corrections)}")
#         print(f"   Bigrams:     {len(bigrams)}")
#         print(f"   Total time:  {overall_elapsed:.2f}ms")
        
#         if corrections:
#             print("\n   Word corrections:")
#             for c in corrections:
#                 print(f"      • '{c['original']}' → '{c['corrected']}' ({c['pos']})")
        
#         if bigrams:
#             print("\n   Bigrams detected:")
#             for b in bigrams:
#                 print(f"      • '{b['bigram']}' ({b.get('category', b.get('subtext', ''))})")
        
#         print("\n   Final structure:")
#         for r in final_results:
#             print(f"      [{r['position']}] {r['word']} → {r['pos']}")
        
#         print("=" * 60 + "\n")
    
#     logger.info(f"Query '{query}' processed in {overall_elapsed:.2f}ms (corrections: {len(corrections)}, bigrams: {len(bigrams)})")
    
#     return corrections, tuple_array, corrected_query


# # =============================================================================
# # LOCATION EXTRACTION (for search filtering)
# # =============================================================================

# def extract_locations_from_query(query: str) -> List[Dict[str, Any]]:
#     """
#     Extract location entities from a query.
    
#     This is useful for filtering search results by location.
    
#     Args:
#         query: The input query string
    
#     Returns:
#         List of location dicts with 'term', 'category', 'type'
#     """
#     locations = []
    
#     # Use cache's classify_query if available
#     if CACHE_AVAILABLE and _ensure_cache_loaded():
#         result = vocab_cache.classify_query(query)
        
#         for loc in result.get('locations', []):
#             metadata = result.get('terms', {}).get(loc, {})
#             locations.append({
#                 'term': loc,
#                 'category': metadata.get('category', 'location'),
#                 'type': 'city' if 'city' in metadata.get('category', '').lower() else 'state'
#             })
        
#         return locations
    
#     # Fall back to word-by-word check
#     words = query.lower().split()
    
#     for word in words:
#         metadata = get_term_metadata(word)
#         if metadata and is_location_type(metadata.get('category', '')):
#             locations.append({
#                 'term': word,
#                 'category': metadata.get('category', 'location'),
#                 'type': 'city' if 'city' in metadata.get('category', '').lower() else 'state'
#             })
    
#     # Check bigrams
#     for i in range(len(words) - 1):
#         bigram_meta = check_bigram_exists(words[i], words[i + 1])
#         if bigram_meta and is_location_type(bigram_meta.get('category', '')):
#             locations.append({
#                 'term': f"{words[i]} {words[i + 1]}",
#                 'category': bigram_meta.get('category', 'location'),
#                 'type': 'city' if 'city' in bigram_meta.get('category', '').lower() else 'state'
#             })
    
#     return locations


# # =============================================================================
# # CACHE STATUS / MANAGEMENT
# # =============================================================================

# def get_cache_status() -> Dict[str, Any]:
#     """Get current cache status for monitoring."""
#     if CACHE_AVAILABLE and vocab_cache:
#         return {
#             'cache_available': True,
#             'cache_loaded': vocab_cache.loaded,
#             **vocab_cache.status()
#         }
#     else:
#         return {
#             'cache_available': False,
#             'cache_loaded': False,
#             'using_redis': REDIS_AVAILABLE
#         }


# def reload_cache() -> bool:
#     """Force reload the vocabulary cache from Redis."""
#     if CACHE_AVAILABLE and vocab_cache:
#         return vocab_cache.reload()
#     return False


# # =============================================================================
# # API FUNCTIONS FOR typesense_calculations.py
# # =============================================================================
# # These functions provide the interface expected by typesense_calculations.py

# def process_query_optimized(query: str, verbose: bool = False) -> Dict[str, Any]:
#     """
#     Process query and return comprehensive result for search strategy selection.
    
#     This is the main entry point used by typesense_calculations.py.
    
#     Args:
#         query: The search query string
#         verbose: Whether to print debug output
    
#     Returns:
#         Dict with:
#             - valid_count: Number of valid terms
#             - unknown_count: Number of unknown terms
#             - search_strategy: 'strict', 'mixed', or 'semantic'
#             - corrected_query: Query with corrections applied
#             - corrected_terms: List of correction dicts
#             - total_score: Sum of all term rank scores
#             - average_score: Average rank score
#             - max_score: Highest rank score
#             - terms: List of term dicts with metadata
#     """
#     overall_start = time.perf_counter()
    
#     if not query or not query.strip():
#         return {
#             'valid_count': 0,
#             'unknown_count': 0,
#             'search_strategy': 'semantic',
#             'corrected_query': query or '',
#             'corrected_terms': [],
#             'total_score': 0,
#             'average_score': 0,
#             'max_score': 0,
#             'terms': []
#         }
    
#     # Run the three-pass processing
#     corrections, tuple_array, corrected_query = word_discovery_multi(
#         query, verbose=verbose
#     )
    
#     # Build term list with metadata
#     words = query.split()
#     terms = []
#     valid_count = 0
#     unknown_count = 0
#     total_score = 0
#     max_score = 0
    
#     for i, word in enumerate(words):
#         word_lower = word.lower()
#         metadata = get_term_metadata(word_lower) or {}
        
#         # Check if this word was corrected
#         correction = next(
#             (c for c in corrections if c.get('original', '').lower() == word_lower),
#             None
#         )
        
#         if correction:
#             # Use corrected word's metadata
#             corrected_word = correction.get('corrected', word_lower)
#             metadata = get_term_metadata(corrected_word) or metadata
#             search_word = corrected_word
#             status = 'corrected'
#         else:
#             search_word = word_lower
#             validation = validate_word(word_lower)
#             status = 'valid' if validation.get('is_valid') else 'unknown'
        
#         # Get rank score
#         rank_score = metadata.get('rank', 0)
#         if isinstance(rank_score, str):
#             try:
#                 rank_score = int(rank_score)
#             except ValueError:
#                 rank_score = 0
        
#         if status in ('valid', 'corrected'):
#             valid_count += 1
#             total_score += rank_score
#             max_score = max(max_score, rank_score)
#         else:
#             unknown_count += 1
        
#         terms.append({
#             'original': word_lower,
#             'search_word': search_word,
#             'status': status,
#             'pos': normalize_pos(metadata.get('pos', 'unknown')),
#             'category': metadata.get('category', ''),
#             'rank_score': rank_score,
#             'metadata': metadata
#         })
    
#     # Calculate average score
#     average_score = total_score / valid_count if valid_count > 0 else 0
    
#     # Determine search strategy based on scores
#     search_strategy = _determine_search_strategy(
#         valid_count=valid_count,
#         unknown_count=unknown_count,
#         total_score=total_score,
#         average_score=average_score,
#         terms=terms
#     )
    
#     elapsed = (time.perf_counter() - overall_start) * 1000
    
#     if verbose:
#         print(f"\n📊 process_query_optimized completed in {elapsed:.2f}ms")
#         print(f"   Strategy: {search_strategy}")
#         print(f"   Valid: {valid_count}, Unknown: {unknown_count}")
#         print(f"   Scores: total={total_score}, avg={average_score:.0f}, max={max_score}")
    
#     return {
#         'valid_count': valid_count,
#         'unknown_count': unknown_count,
#         'search_strategy': search_strategy,
#         'corrected_query': corrected_query,
#         'corrected_terms': corrections,
#         'total_score': total_score,
#         'average_score': round(average_score, 2),
#         'max_score': max_score,
#         'terms': terms
#     }


# def _determine_search_strategy(
#     valid_count: int,
#     unknown_count: int,
#     total_score: int,
#     average_score: float,
#     terms: List[Dict[str, Any]]
# ) -> str:
#     """
#     Determine search strategy based on term validation and scores.
    
#     Returns:
#         'strict' - High confidence in terms, use text search
#         'mixed' - Medium confidence, combine text + vector
#         'semantic' - Low confidence, rely on vector search
#     """
#     total_terms = valid_count + unknown_count
    
#     if total_terms == 0:
#         return 'semantic'
    
#     valid_ratio = valid_count / total_terms
    
#     # Check for strict strategy
#     strict_thresholds = STRATEGY_THRESHOLDS['strict']
#     if (total_score >= strict_thresholds['min_total_score'] and
#         average_score >= strict_thresholds['min_average_score'] and
#         valid_ratio >= strict_thresholds['min_valid_ratio']):
#         return 'strict'
    
#     # Check for mixed strategy
#     mixed_thresholds = STRATEGY_THRESHOLDS['mixed']
#     if (total_score >= mixed_thresholds['min_total_score'] and
#         average_score >= mixed_thresholds['min_average_score'] and
#         valid_ratio >= mixed_thresholds['min_valid_ratio']):
#         return 'mixed'
    
#     # Default to semantic
#     return 'semantic'


# def get_search_strategy(result: Dict[str, Any]) -> str:
#     """
#     Get search strategy from process_query_optimized result.
    
#     Args:
#         result: Result from process_query_optimized
    
#     Returns:
#         Search strategy string: 'strict', 'mixed', or 'semantic'
#     """
#     return result.get('search_strategy', 'semantic')


# def get_filter_terms(result: Dict[str, Any]) -> List[str]:
#     """
#     Get valid terms suitable for filtering/strict search.
    
#     These are terms that were validated or corrected successfully.
    
#     Args:
#         result: Result from process_query_optimized
    
#     Returns:
#         List of valid search terms
#     """
#     terms = result.get('terms', [])
#     return [
#         t['search_word'] for t in terms
#         if t.get('status') in ('valid', 'corrected', 'bigram')
#     ]


# def get_loose_terms(result: Dict[str, Any]) -> List[str]:
#     """
#     Get unknown/unvalidated terms.
    
#     These terms couldn't be validated and may need semantic search.
    
#     Args:
#         result: Result from process_query_optimized
    
#     Returns:
#         List of unknown terms
#     """
#     terms = result.get('terms', [])
#     return [
#         t['original'] for t in terms
#         if t.get('status') == 'unknown'
#     ]


# def get_all_search_terms(result: Dict[str, Any]) -> List[str]:
#     """
#     Get all search terms (valid and unknown).
    
#     Args:
#         result: Result from process_query_optimized
    
#     Returns:
#         List of all search terms
#     """
#     terms = result.get('terms', [])
#     return [t['search_word'] for t in terms]


# def get_term_scores(result: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """
#     Get list of terms with their rank scores.
    
#     Args:
#         result: Result from process_query_optimized
    
#     Returns:
#         List of dicts with 'term' and 'score'
#     """
#     terms = result.get('terms', [])
#     return [
#         {'term': t['search_word'], 'score': t.get('rank_score', 0)}
#         for t in terms
#     ]


# def get_high_score_terms(result: Dict[str, Any], min_score: int = 500) -> List[str]:
#     """
#     Get terms with rank score above threshold.
    
#     High-scoring terms are good candidates for strict/exact matching.
    
#     Args:
#         result: Result from process_query_optimized
#         min_score: Minimum rank score threshold
    
#     Returns:
#         List of high-scoring terms
#     """
#     terms = result.get('terms', [])
#     return [
#         t['search_word'] for t in terms
#         if t.get('rank_score', 0) >= min_score
#     ]

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

# =============================================================================
# IMPORTS - Cache first, Redis as fallback
# =============================================================================

# Try to import vocabulary cache (fast path)
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
                print(f"  [{i+1}] '{word_lower}' → VALID (pos={pos}, score={score})")
            
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
    
    - Only processes words marked as 'unknown'
    - Uses predicted POS to filter candidates
    - Ranks by: distance ASC, then score DESC
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
    
    for unknown in unknowns:
        word = unknown['word']
        predicted_pos = unknown.get('predicted_pos', 'noun')
        
        if verbose:
            print(f"\n  Correcting '{word}' (expected POS: {predicted_pos})...")
        
        # Get fuzzy matches from Redis
        suggestion_result = get_suggestions(word, limit=20, max_distance=max_distance)
        candidates = suggestion_result.get('suggestions', [])
        
        if not candidates:
            if verbose:
                print(f"    No candidates found")
            continue
        
        # Filter by POS and calculate ranking score
        scored_candidates = []
        for candidate in candidates:
            candidate_pos = normalize_pos(candidate.get('pos', 'unknown'))
            candidate_term = candidate.get('term', '')
            candidate_score = get_rank_score(candidate)
            
            # Calculate edit distance
            distance = damerau_levenshtein_distance(word, candidate_term.lower())
            
            if distance > max_distance:
                continue
            
            # Check POS match (with flexibility)
            pos_match = False
            if candidate_pos == predicted_pos:
                pos_match = True
            elif predicted_pos == 'noun' and candidate_pos in ('noun', 'proper_noun'):
                pos_match = True
            elif predicted_pos == 'proper_noun' and candidate_pos in ('noun', 'proper_noun'):
                pos_match = True
            
            scored_candidates.append({
                'term': candidate_term,
                'pos': candidate_pos,
                'score': candidate_score,
                'distance': distance,
                'pos_match': pos_match,
                'metadata': candidate
            })
        
        if not scored_candidates:
            if verbose:
                print(f"    No candidates within distance {max_distance}")
            continue
        
        # Sort: POS match first, then distance ASC, then score DESC
        scored_candidates.sort(key=lambda x: (
            0 if x['pos_match'] else 1,  # POS matches first
            x['distance'],                # Then lowest distance
            -x['score']                   # Then highest score
        ))
        
        # Select best candidate
        best = scored_candidates[0]
        
        if verbose:
            print(f"    Candidates ({len(scored_candidates)}):")
            for i, c in enumerate(scored_candidates[:5]):
                marker = "✓" if i == 0 else " "
                pos_marker = "(POS match)" if c['pos_match'] else ""
                print(f"      {marker} '{c['term']}' → dist={c['distance']}, score={c['score']} {pos_marker}")
        
        # Update the result
        unknown['status'] = 'corrected'
        unknown['corrected'] = best['term']
        unknown['corrected_pos'] = best['pos']
        unknown['corrected_score'] = best['score']
        unknown['distance'] = best['distance']
        unknown['pos_match'] = best['pos_match']
        unknown['metadata'] = best['metadata']
        
        if verbose:
            print(f"    Selected: '{word}' → '{best['term']}'")
    
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
                print(f"  TRIGRAM: '{trigram_str}'")
            
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
                print(f"  BIGRAM: '{bigram_str}' (category: {category})")
            
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
        Dict with filters, locations, sort, corrections, and metadata
    """
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
            'total_score': 0,
            'average_score': 0,
            'max_score': 0,
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
    
    # Build terms list for output
    terms = []
    for r in results:
        term_word = r.get('corrected', r['word'])
        terms.append({
            'word': r['word'],
            'search_word': term_word,
            'status': r['status'],
            'pos': r.get('corrected_pos', r['pos']),
            'score': r.get('corrected_score', r.get('score', 0)),
            'category': r.get('category', '')
        })
    
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
        'ngrams': ngrams,
        'terms': terms,
        'total_score': total_score,
        'average_score': round(average_score, 2),
        'max_score': max_score,
        'processing_time_ms': round(elapsed, 2)
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
        print(f"  Total time: {elapsed:.2f}ms")
        print("=" * 60)
    
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