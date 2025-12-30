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

"""
word_discovery.py
OPTIMIZED: Single-pass word validation, correction, and bigram detection.

Key Changes:
- ONE batch Redis call upfront (not multiple passes)
- Only correct UNKNOWN words (don't touch valid words)
- Bigram detection in same loop
- O(1) lookups using pre-fetched data
"""
import json
from typing import Dict, Any, List, Tuple, Optional, Set
from functools import lru_cache

# Try to import the fast C implementation, fall back to pure Python
try:
    from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_levenshtein
    USE_FAST_LEVENSHTEIN = True
except ImportError:
    USE_FAST_LEVENSHTEIN = False

# Handle both relative and absolute imports
try:
    from .searchapi import (
        RedisLookupTable,
        validate_word,
        get_term_metadata,
        get_suggestions,
        generate_candidates_smart,
        batch_check_candidates,
        batch_validate_words_redis,
        batch_check_bigrams,
        batch_get_term_metadata,
        damerau_levenshtein_distance as _python_levenshtein
    )
except ImportError:
    from searchapi import (
        RedisLookupTable,
        validate_word,
        get_term_metadata,
        get_suggestions,
        generate_candidates_smart,
        batch_check_candidates,
        batch_validate_words_redis,
        batch_check_bigrams,
        batch_get_term_metadata,
        damerau_levenshtein_distance as _python_levenshtein
    )


# =============================================================================
# CONSTANTS - frozensets for O(1) lookup
# =============================================================================

ALLOWED_POS: frozenset = frozenset({
    "pronoun", "noun", "verb", "article", "adjective",
    "preposition", "adverb", "be", "modal", "auxiliary",
    "proper_noun", "proper noun", "relative_pronoun", "wh_pronoun", "determiner",
    "quantifier", "numeral", "participle", "gerund",
    "infinitive_marker", "particle", "negation", "conjunction", "interjection"
})

LOCATION_TYPES: frozenset = frozenset({"city", "state", "neighborhood", "region", "country", "us_city", "us_state"})

COMPOUND_NOUN_TYPES: frozenset = frozenset({
    "city", "state", "neighborhood", "region", "country",
    "occupation", "product", "furniture", "food", "sport", "disease", "animal"
})

# Pre-built dict for O(1) context rule lookup
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
    ("noun", "noun"): [("verb", 0.85), ("conjunction", 0.80)],
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
    ("conjunction", "noun"): [("noun", 0.85), ("adjective", 0.80)],
    ("conjunction", "verb"): [("noun", 0.85), ("pronoun", 0.80)],
    # ONLY LEFT NEIGHBOR KNOWN
    ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
    ("article", None): [("noun", 0.85), ("adjective", 0.80)],
    ("adjective", None): [("noun", 0.90)],
    ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
    ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
    ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
    ("noun", None): [("verb", 0.80), ("noun", 0.60), ("conjunction", 0.50)],
    ("adverb", None): [("adjective", 0.80), ("verb", 0.75), ("adverb", 0.70)],
    ("be", None): [("adjective", 0.85), ("noun", 0.75), ("determiner", 0.70)],
    ("conjunction", None): [("noun", 0.85), ("pronoun", 0.80), ("determiner", 0.75)],
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
    (None, "conjunction"): [("noun", 0.85), ("verb", 0.80)],
}


# =============================================================================
# CACHED HELPERS - O(1) after first call
# =============================================================================

@lru_cache(maxsize=10000)
def cached_levenshtein(word1: str, word2: str) -> int:
    """Cached Levenshtein distance calculation."""
    if USE_FAST_LEVENSHTEIN:
        return _fast_levenshtein(word1, word2)
    return _python_levenshtein(word1, word2)


def normalize_pos(pos_value: Any) -> str:
    """
    Normalize POS value to a simple string.
    
    Handles:
    - None -> 'unknown'
    - "['noun']" (string) -> 'noun'
    - ['noun'] (list) -> 'noun'
    - 'noun' (string) -> 'noun'
    - Location categories -> 'proper_noun'
    """
    if pos_value is None:
        return 'unknown'
    
    # Handle string that looks like a list: "['noun']"
    if isinstance(pos_value, str):
        pos_value = pos_value.strip()
        if pos_value.startswith('[') and pos_value.endswith(']'):
            try:
                parsed = json.loads(pos_value.replace("'", '"'))
                if isinstance(parsed, list) and parsed:
                    pos_value = parsed[0]
                else:
                    pos_value = 'unknown'
            except (json.JSONDecodeError, ValueError):
                # Try manual parsing: "['noun']" -> "noun"
                inner = pos_value[1:-1].strip()
                if inner.startswith("'") and inner.endswith("'"):
                    pos_value = inner[1:-1]
                elif inner.startswith('"') and inner.endswith('"'):
                    pos_value = inner[1:-1]
                else:
                    pos_value = inner
    
    # Handle actual list: ['noun']
    if isinstance(pos_value, list):
        pos_value = pos_value[0] if pos_value else 'unknown'
    
    # Ensure it's a string
    if not isinstance(pos_value, str):
        pos_value = str(pos_value) if pos_value else 'unknown'
    
    # Normalize location types to proper_noun
    pos_lower = pos_value.lower().strip()
    
    if pos_lower in ('proper noun', 'proper_noun'):
        return 'proper_noun'
    
    if pos_lower in LOCATION_TYPES:
        return 'proper_noun'
    
    if pos_lower in COMPOUND_NOUN_TYPES:
        return 'noun'
    
    # Return as-is if it's a valid POS
    if pos_lower in ALLOWED_POS:
        return pos_lower
    
    return pos_value.lower() if pos_value else 'unknown'


# =============================================================================
# CONTEXT-BASED PREDICTION - O(1) dict lookups
# =============================================================================

def predict_pos_from_context(
    left_pos: Optional[str],
    right_pos: Optional[str]
) -> Optional[Tuple[str, float]]:
    """Predict POS based on neighboring words' POS tags."""
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


# =============================================================================
# CORRECTION SEARCH - Only for UNKNOWN words
# =============================================================================

def find_correction_for_unknown(
    word: str,
    left_pos: Optional[str],
    right_pos: Optional[str],
    max_distance: int = 2
) -> Optional[Dict[str, Any]]:
    """
    Find correction for an unknown word using POS context.
    
    1. Predict expected POS from neighbors
    2. Generate candidates
    3. Filter by POS match
    4. Return closest by distance
    """
    # Predict what POS this word should be
    prediction = predict_pos_from_context(left_pos, right_pos)
    predicted_pos = prediction[0] if prediction else 'noun'  # Default to noun
    
    # Generate candidates
    candidates = generate_candidates_smart(word, max_candidates=50)
    
    if not candidates:
        return None
    
    # Batch check which candidates exist
    found = batch_check_candidates(candidates)
    
    if not found:
        return None
    
    word_lower = word.lower()
    matches = []
    fallback_matches = []
    
    for item in found:
        item_pos = normalize_pos(item.get('pos', 'unknown'))
        term_lower = item.get('term', '').lower()
        
        # Calculate distance
        distance = cached_levenshtein(word_lower, term_lower)
        
        if distance > max_distance:
            continue
        
        item['distance'] = distance
        item['normalized_pos'] = item_pos
        
        # Check if POS matches prediction
        pos_match = (
            item_pos == predicted_pos or
            (predicted_pos == 'proper_noun' and item.get('category', '').lower() in LOCATION_TYPES) or
            (predicted_pos == 'noun' and item_pos in ('noun', 'proper_noun'))
        )
        
        if pos_match:
            matches.append(item)
        else:
            fallback_matches.append(item)
    
    # Prefer POS matches, fall back to any match
    result_list = matches if matches else fallback_matches
    
    if not result_list:
        return None
    
    # Sort by distance first, then by rank (higher rank = better)
    result_list.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
    return result_list[0]


# =============================================================================
# SINGLE-PASS WORD DISCOVERY
# =============================================================================

def word_discovery_single_pass(
    query: str,
    pre_validated: Optional[List[Dict[str, Any]]] = None,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    OPTIMIZED: Single-pass word discovery.
    
    1. Batch validate all words (single Redis call)
    2. Single loop: process each word + detect bigrams
       - Valid word -> keep it
       - Unknown word -> find correction using POS context
    3. Return categorized results
    """
    words = query.split()
    
    if not words:
        return _empty_result(query)
    
    # =========================================================================
    # STEP 1: Batch validate all words - ONE Redis call
    # =========================================================================
    
    if pre_validated:
        # Use pre-validated data (from lookup_table)
        word_data = {}
        for item in pre_validated:
            w = item.get('word', '').lower()
            if w:
                word_data[w] = {
                    'exists': item.get('exists', False),
                    'pos': normalize_pos(item.get('pos') or item.get('metadata', {}).get('pos')),
                    'metadata': item.get('metadata', item)
                }
    else:
        # Batch validate via Redis
        validation_cache = batch_validate_words_redis(words)
        word_data = {}
        for word in words:
            w = word.lower()
            if w in validation_cache:
                result = validation_cache[w]
                word_data[w] = {
                    'exists': result.get('is_valid', False),
                    'pos': normalize_pos(result.get('metadata', {}).get('pos', 'unknown')),
                    'metadata': result.get('metadata', {})
                }
            else:
                word_data[w] = {'exists': False, 'pos': 'unknown', 'metadata': {}}
    
    # =========================================================================
    # STEP 2: Prepare bigram checking - ONE Redis call
    # =========================================================================
    
    bigram_pairs = []
    for i in range(len(words) - 1):
        bigram_pairs.append((words[i].lower(), words[i + 1].lower()))
    
    bigram_results = batch_check_bigrams(bigram_pairs) if bigram_pairs else {}
    
    # =========================================================================
    # STEP 3: Single loop - process words and detect bigrams
    # =========================================================================
    
    processed = []  # Final processed words
    corrections = []  # Corrections made
    bigrams_found = []  # Bigrams detected
    skip_next = False
    
    for i, word in enumerate(words):
        if skip_next:
            skip_next = False
            continue
        
        word_lower = word.lower()
        data = word_data.get(word_lower, {'exists': False, 'pos': 'unknown', 'metadata': {}})
        
        # Check for bigram with next word
        if i < len(words) - 1:
            next_word = words[i + 1].lower()
            bigram_key = f"{word_lower} {next_word}"
            
            if bigram_key in bigram_results:
                # Found a bigram!
                bigram_meta = bigram_results[bigram_key]
                category = bigram_meta.get('category', '')
                bigram_pos = 'proper_noun' if category.lower() in LOCATION_TYPES else 'noun'
                
                processed.append({
                    'position': i + 1,
                    'word': f"{word} {words[i + 1]}",
                    'search_word': f"{word} {words[i + 1]}",
                    'status': 'bigram',
                    'pos': bigram_pos,
                    'metadata': bigram_meta
                })
                
                bigrams_found.append({
                    'bigram': f"{word} {words[i + 1]}",
                    'category': category
                })
                
                skip_next = True
                continue
        
        # Process single word
        if data['exists']:
            # Word is valid - keep it as is
            processed.append({
                'position': i + 1,
                'word': word,
                'search_word': word,
                'status': 'valid',
                'pos': data['pos'],
                'metadata': data['metadata']
            })
        else:
            # Word is unknown - try to correct it
            left_pos = processed[-1]['pos'] if processed else None
            
            # Look ahead for right POS (if next word is valid)
            right_pos = None
            if i < len(words) - 1:
                next_word_lower = words[i + 1].lower()
                next_data = word_data.get(next_word_lower, {})
                if next_data.get('exists'):
                    right_pos = next_data.get('pos')
            
            # Find correction
            correction = find_correction_for_unknown(word, left_pos, right_pos)
            
            if correction:
                corrected_word = correction.get('term', word)
                processed.append({
                    'position': i + 1,
                    'word': word,
                    'search_word': corrected_word,
                    'status': 'corrected',
                    'pos': normalize_pos(correction.get('pos', 'unknown')),
                    'metadata': correction,
                    'original': word,
                    'corrected': corrected_word,
                    'distance': correction.get('distance', 0)
                })
                corrections.append({
                    'original': word,
                    'corrected': corrected_word,
                    'distance': correction.get('distance', 0)
                })
            else:
                # No correction found - keep original
                processed.append({
                    'position': i + 1,
                    'word': word,
                    'search_word': word,
                    'status': 'unknown',
                    'pos': 'unknown',
                    'metadata': {}
                })
    
    # =========================================================================
    # STEP 4: Build results
    # =========================================================================
    
    valid_terms = [p for p in processed if p['status'] in ('valid', 'corrected', 'bigram')]
    unknown_terms = [p for p in processed if p['status'] == 'unknown']
    corrected_terms = [p for p in processed if p['status'] == 'corrected']
    bigram_terms = [p for p in processed if p['status'] == 'bigram']
    
    search_terms = [p['search_word'] for p in processed]
    corrected_query = ' '.join(search_terms)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"📊 WORD DISCOVERY RESULTS: '{query}'")
        print(f"   Valid terms ({len(valid_terms)}): {[t['search_word'] for t in valid_terms]}")
        print(f"   Unknown terms ({len(unknown_terms)}): {[t['word'] for t in unknown_terms]}")
        print(f"   Corrected: {[(c['original'], c['corrected']) for c in corrected_terms]}")
        print(f"   Bigrams: {[t['word'] for t in bigram_terms]}")
        print(f"   Search terms: {search_terms}")
        print(f"{'='*60}\n")
    
    return {
        'success': True,
        'query': query,
        'corrected_query': corrected_query,
        'terms': processed,
        'valid_terms': valid_terms,
        'unknown_terms': unknown_terms,
        'corrected_terms': corrected_terms,
        'bigram_terms': bigram_terms,
        'search_terms': search_terms,
        'has_unknown': len(unknown_terms) > 0,
        'all_unknown': len(unknown_terms) == len(processed) and len(processed) > 0,
        'valid_count': len(valid_terms),
        'unknown_count': len(unknown_terms),
        'total_count': len(processed)
    }


def _empty_result(query: str) -> Dict[str, Any]:
    """Return empty result structure."""
    return {
        'success': True,
        'query': query,
        'corrected_query': '',
        'terms': [],
        'valid_terms': [],
        'unknown_terms': [],
        'corrected_terms': [],
        'bigram_terms': [],
        'search_terms': [],
        'has_unknown': False,
        'all_unknown': True,
        'valid_count': 0,
        'unknown_count': 0,
        'total_count': 0
    }


# =============================================================================
# PUBLIC API - Compatible with existing code
# =============================================================================

def word_discovery_full(
    query: str,
    verbose: bool = False,
    pre_validated: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Full word discovery with categorized output for search integration.
    This is the main entry point - now uses single-pass processing.
    """
    return word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)


def word_discovery_multi(
    query: str,
    redis_client=None,
    prefix: str = "prefix",
    verbose: bool = False,
    pre_validated: Optional[List[Dict[str, Any]]] = None
) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
    """
    Legacy entry point for compatibility.
    Returns: (corrections, tuple_array, corrected_query)
    """
    result = word_discovery_single_pass(query, pre_validated=pre_validated, verbose=verbose)
    
    corrections = result.get('corrected_terms', [])
    tuple_array = [(t['position'], t['pos']) for t in result.get('terms', [])]
    corrected_query = result.get('corrected_query', query)
    
    return corrections, tuple_array, corrected_query


# =============================================================================
# HELPER FUNCTIONS FOR SEARCH INTEGRATION
# =============================================================================

def get_search_strategy(discovery_result: Dict[str, Any]) -> str:
    """Determine search strategy based on word discovery results."""
    valid_count = discovery_result.get('valid_count', 0)
    
    if valid_count >= 2:
        return 'strict'
    elif valid_count == 1:
        return 'mixed'
    else:
        return 'semantic'


def get_filter_terms(discovery_result: Dict[str, Any]) -> List[str]:
    """Get terms for strict filtering."""
    return [t['search_word'] for t in discovery_result.get('valid_terms', [])]


def get_loose_terms(discovery_result: Dict[str, Any]) -> List[str]:
    """Get unknown terms for loose search."""
    return [t['word'] for t in discovery_result.get('unknown_terms', [])]


def get_all_search_terms(discovery_result: Dict[str, Any]) -> List[str]:
    """Get all search terms."""
    return discovery_result.get('search_terms', [])


# =============================================================================
# OPTIMIZED QUERY PROCESSING
# =============================================================================

def process_query_optimized(
    query: str,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Optimized end-to-end query processing for Typesense search.
    
    - Single validation pass
    - Batched bigram detection
    - Categorized output for search strategy
    """
    try:
        from .searchapi import lookup_table
    except ImportError:
        from searchapi import lookup_table
    
    # Step 1: Lookup with validation cache
    lookup_result = lookup_table(query, return_validation_cache=True)
    
    if not lookup_result.get('success', False):
        words = query.split()
        return {
            'success': False,
            'query': query,
            'corrected_query': query,
            'terms': [],
            'valid_terms': [],
            'unknown_terms': [{'word': w, 'search_word': w, 'pos': 'unknown', 'status': 'unknown'} 
                             for w in words],
            'corrected_terms': [],
            'bigram_terms': [],
            'search_terms': words,
            'has_unknown': True,
            'all_unknown': True,
            'valid_count': 0,
            'unknown_count': len(words),
            'total_count': len(words),
            'search_strategy': 'semantic',
            'cache_hit': False,
            'error': lookup_result.get('error', 'Lookup failed')
        }
    
    # Step 2: Single-pass word discovery
    result = word_discovery_single_pass(
        query,
        pre_validated=lookup_result.get('terms', []),
        verbose=verbose
    )
    
    # Step 3: Determine search strategy
    result['search_strategy'] = get_search_strategy(result)
    result['cache_hit'] = lookup_result.get('cache_hit', False)
    
    return result


# =============================================================================
# TEST FUNCTION
# =============================================================================

def test_word_discovery():
    """Test function to verify word discovery is working."""
    test_queries = [
        "tuskegee airmen",
        "the quikc brown fox",
        "new york city",
        "black doctors",
        "african american history",
    ]
    
    print("=" * 60)
    print("WORD DISCOVERY TEST (SINGLE-PASS)")
    print("=" * 60)
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        result = word_discovery_single_pass(query, verbose=False)
        
        print(f"  Valid: {[t['search_word'] for t in result['valid_terms']]}")
        print(f"  Unknown: {[t['word'] for t in result['unknown_terms']]}")
        print(f"  Corrected: {result['corrected_query']}")
        print(f"  Strategy: {get_search_strategy(result)}")


if __name__ == "__main__":
    test_word_discovery()