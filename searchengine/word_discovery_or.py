

# import json
# import string
# import time
# from typing import Dict, List, Set, Tuple, Optional, Any

# # Use the fast C implementation
# from pyxdameraulevenshtein import damerau_levenshtein_distance


# from .searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata
# )


# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix"
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Process a query by validating/correcting each word.
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional, will get from RedisLookupTable)
#         prefix: Redis key prefix
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
    
#     words = query.split()
#     corrections = []
#     tuple_array = []
#     corrected_words = []
    
#     print("=" * 60)
#     print(f"🔍 PROCESSING QUERY: '{query}'")
#     print("=" * 60)
    
#     for position, word in enumerate(words, start=1):
#         print(f"\n📍 Position {position}: Checking '{word}'...")
        
#         # Validate the word
#         validation = validate_word(word)
        
#         if validation['is_valid']:
#             # Word exists in dictionary
#             metadata = validation.get('metadata', {})
#             pos = metadata.get('pos', 'unknown')
#             rank = metadata.get('rank', 0)
            
#             print(f"   ✅ VALID")
#             print(f"      POS: {pos}")
#             print(f"      Rank: {rank}")
            
#             tuple_array.append((position, pos))
#             corrected_words.append(word.lower())
        
#         elif validation.get('suggestion'):
#             # Word not found, but we have a suggestion
#             suggestion = validation['suggestion']
#             distance = validation.get('distance', 0)
#             tier = validation.get('tier_used', 'unknown')
#             metadata = validation.get('metadata', {})
#             pos = metadata.get('pos', 'unknown')
#             rank = metadata.get('rank', 0)
            
#             print(f"   🔧 CORRECTED: '{word}' → '{suggestion}'")
#             print(f"      Distance: {distance}")
#             print(f"      Tier: {tier}")
#             print(f"      POS: {pos}")
#             print(f"      Rank: {rank}")
            
#             corrections.append({
#                 'position': position,
#                 'original': word,
#                 'corrected': suggestion,
#                 'distance': distance,
#                 'pos': pos,
#                 'rank': rank,
#                 'tier_used': tier
#             })
            
#             tuple_array.append((position, pos))
#             corrected_words.append(suggestion)
        
#         else:
#             # Word not found, no suggestion
#             print(f"   ❌ NOT FOUND - N" )



# # ______________________________________________________   


# """
# word_discovery.py
# Three-pass word validation, correction, and bigram detection.

# Pass 1: Validate each word
# Pass 2: Pattern-based correction for unknowns
# Pass 3: Bigram detection
# """
# import json
# from typing import Dict, Any, List, Tuple, Optional


# from .searchapi import (
#     RedisLookupTable,
#     validate_word,
#     get_term_metadata,
#     get_suggestions,
#     generate_candidates_smart,
#     batch_check_candidates
# )

# from pyxdameraulevenshtein import damerau_levenshtein_distance


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

# LOCATION_TYPES = {"city", "state", "neighborhood", "region", "country"}

# COMPOUND_NOUN_TYPES = {
#     "city", "state", "neighborhood", "region", "country",
#     "occupation", "product", "furniture", "food", "sport", "disease", "animal"
# }

# SENTENCE_PATTERNS = {
#     # ==========================================================================
#     # DETERMINER patterns (Redis returns "determiner" for "the", "a", "an")
#     # ==========================================================================
#     "determiner": [
#         # Single word after determiner
#         ("noun",),                                      # the car
#         ("adjective",),                                 # the blue (implied noun)
        
#         # Two words after determiner
#         ("adjective", "noun"),                          # the blue car
#         ("noun", "verb"),                               # the car runs
#         ("noun", "noun"),                               # the city center (compound)
        
#         # Three words after determiner
#         ("adjective", "noun", "verb"),                  # the blue car runs
#         ("adjective", "adjective", "noun"),             # the big blue car
#         ("noun", "verb", "adverb"),                     # the car runs fast
#         ("noun", "verb", "noun"),                       # the dog sees cats
#         ("noun", "be", "adjective"),                    # the car is blue
#         ("noun", "be", "noun"),                         # the car is junk
        
#         # Four words after determiner
#         ("adjective", "noun", "verb", "noun"),          # the blue car hits walls
#         ("adjective", "noun", "be", "adjective"),       # the blue car is fast
#         ("noun", "verb", "determiner", "noun"),         # the dog sees the cat
#         ("noun", "verb", "adjective", "noun"),          # the dog sees blue cars
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
#         # Single word after pronoun
#         ("verb",),                                      # I run
#         ("be",),                                        # I am
        
#         # Two words after pronoun
#         ("verb", "noun"),                               # I see cars
#         ("verb", "adverb"),                             # I run fast
#         ("verb", "adjective"),                          # I feel happy
#         ("be", "adjective"),                            # I am happy
#         ("be", "noun"),                                 # I am doctor
        
#         # Three words after pronoun
#         ("verb", "determiner", "noun"),                 # I see the car
#         ("verb", "article", "noun"),                    # I see a car
#         ("verb", "adjective", "noun"),                  # I see blue cars
#         ("verb", "preposition", "noun"),                # I go to school
#         ("be", "determiner", "noun"),                   # I am the boss
#         ("be", "preposition", "noun"),                  # I am in trouble
        
#         # Four words after pronoun
#         ("verb", "determiner", "adjective", "noun"),    # I see the blue car
#         ("verb", "article", "adjective", "noun"),       # I see a blue car
#         ("verb", "preposition", "determiner", "noun"),  # I go to the store
#         ("be", "determiner", "adjective", "noun"),      # I am the best player
#     ],
    
#     # ==========================================================================
#     # NOUN patterns
#     # ==========================================================================
#     "noun": [
#         # Single word after noun
#         ("verb",),                                      # dogs run
#         ("be",),                                        # dogs are
        
#         # Two words after noun
#         ("verb", "noun"),                               # dogs chase cats
#         ("verb", "adverb"),                             # dogs run fast
#         ("verb", "adjective"),                          # dogs seem happy
#         ("be", "adjective"),                            # dogs are happy
#         ("be", "noun"),                                 # dogs are animals
        
#         # Three words after noun
#         ("verb", "determiner", "noun"),                 # dogs chase the cat
#         ("verb", "article", "noun"),                    # dogs chase a cat
#         ("verb", "adjective", "noun"),                  # dogs chase small cats
#         ("verb", "preposition", "noun"),                # dogs run to parks
#         ("be", "preposition", "noun"),                  # dogs are in parks
#         ("be", "determiner", "noun"),                   # dogs are the best
        
#         # Four words after noun
#         ("verb", "determiner", "adjective", "noun"),    # dogs chase the small cat
#         ("verb", "preposition", "determiner", "noun"),  # dogs run to the park
#     ],
    
#     # ==========================================================================
#     # ADJECTIVE patterns
#     # ==========================================================================
#     "adjective": [
#         # Single word after adjective
#         ("noun",),                                      # blue car
        
#         # Two words after adjective
#         ("noun", "verb"),                               # blue cars run
#         ("noun", "be"),                                 # blue cars are
#         ("adjective", "noun"),                          # big blue car
        
#         # Three words after adjective
#         ("noun", "verb", "adverb"),                     # blue cars run fast
#         ("noun", "be", "adjective"),                    # blue cars are fast
#         ("noun", "be", "noun"),                         # blue cars are vehicles
#         ("noun", "verb", "noun"),                       # blue cars hit walls
#         ("adjective", "noun", "verb"),                  # big blue cars run
#     ],
    
#     # ==========================================================================
#     # VERB patterns
#     # ==========================================================================
#     "verb": [
#         # Single word after verb
#         ("noun",),                                      # see cars
#         ("adverb",),                                    # run fast
#         ("adjective",),                                 # feel happy
        
#         # Two words after verb
#         ("determiner", "noun"),                         # see the car
#         ("article", "noun"),                            # see a car
#         ("adjective", "noun"),                          # see blue cars
#         ("preposition", "noun"),                        # go to school
#         ("adverb", "adverb"),                           # run very fast
#         ("noun", "noun"),                               # give dogs treats
        
#         # Three words after verb
#         ("determiner", "adjective", "noun"),            # see the blue car
#         ("article", "adjective", "noun"),               # see a blue car
#         ("preposition", "determiner", "noun"),          # go to the store
#         ("preposition", "adjective", "noun"),           # go to big stores
#         ("noun", "determiner", "noun"),                 # give dogs the treat
        
#         # Four words after verb
#         ("preposition", "determiner", "adjective", "noun"),  # go to the big store
#     ],
    
#     # ==========================================================================
#     # PREPOSITION patterns
#     # ==========================================================================
#     "preposition": [
#         # Single word after preposition
#         ("noun",),                                      # to school
#         ("proper_noun",),                               # to Paris
        
#         # Two words after preposition
#         ("determiner", "noun"),                         # to the store
#         ("article", "noun"),                            # to a store
#         ("adjective", "noun"),                          # to big stores
        
#         # Three words after preposition
#         ("determiner", "adjective", "noun"),            # to the big store
#         ("article", "adjective", "noun"),               # to a big store
#         ("adjective", "adjective", "noun"),             # to very big stores
#     ],
    
#     # ==========================================================================
#     # ADVERB patterns
#     # ==========================================================================
#     "adverb": [
#         ("verb",),                                      # quickly run
#         ("adjective",),                                 # very blue
#         ("adverb",),                                    # very quickly
#         ("verb", "noun"),                               # quickly see cars
#         ("verb", "determiner", "noun"),                 # quickly see the car
#         ("adjective", "noun"),                          # very blue car
#     ],
    
#     # ==========================================================================
#     # BE verb patterns
#     # ==========================================================================
#     "be": [
#         ("adjective",),                                 # is happy
#         ("noun",),                                      # is doctor
#         ("determiner", "noun"),                         # is the boss
#         ("article", "noun"),                            # is a doctor
#         ("preposition", "noun"),                        # is in trouble
#         ("adverb", "adjective"),                        # is very happy
#         ("determiner", "adjective", "noun"),            # is the best player
#     ],
# }


# LOCAL_CONTEXT_RULES = {
#     # ==========================================================================
#     # BOTH NEIGHBORS KNOWN (highest confidence)
#     # ==========================================================================
    
#     # Determiner on left
#     ("determiner", "noun"): [("adjective", 0.95)],           # the _?_ car → adjective
#     ("determiner", "adjective"): [("adjective", 0.85), ("adverb", 0.70)],  # the _?_ blue → adjective/adverb
#     ("determiner", "verb"): [("noun", 0.90)],                # the _?_ runs → noun
    
#     # Article on left (alias for determiner)
#     ("article", "noun"): [("adjective", 0.95)],
#     ("article", "adjective"): [("adjective", 0.85)],
#     ("article", "verb"): [("noun", 0.90)],
    
#     # Adjective on left
#     ("adjective", "noun"): [("adjective", 0.85)],            # big _?_ car → adjective
#     ("adjective", "verb"): [("noun", 0.90)],                 # blue _?_ runs → noun
#     ("adjective", "adjective"): [("noun", 0.70)],            # big blue _?_ → could be noun
    
#     # Noun on left
#     ("noun", "noun"): [("verb", 0.85)],                      # car _?_ house → verb (unlikely but possible)
#     ("noun", "adjective"): [("verb", 0.90), ("be", 0.85)],   # car _?_ blue → verb/be
#     ("noun", "adverb"): [("verb", 0.90)],                    # car _?_ fast → verb
#     ("noun", "preposition"): [("verb", 0.85)],               # car _?_ to → verb
#     ("noun", "determiner"): [("verb", 0.90)],                # car _?_ the → verb
#     ("noun", "article"): [("verb", 0.90)],                   # car _?_ a → verb
    
#     # Verb on left
#     ("verb", "noun"): [("adjective", 0.80), ("determiner", 0.75)],  # see _?_ cars → adjective/determiner
#     ("verb", "verb"): [("adverb", 0.75)],                    # run _?_ jump → adverb
#     ("verb", "adjective"): [("adverb", 0.85)],               # run _?_ fast → adverb
#     ("verb", "preposition"): [("noun", 0.85), ("adverb", 0.70)],    # go _?_ to → noun/adverb
    
#     # Pronoun on left
#     ("pronoun", "noun"): [("verb", 0.90)],                   # I _?_ cars → verb
#     ("pronoun", "adjective"): [("verb", 0.90), ("be", 0.85)],  # I _?_ happy → verb/be
#     ("pronoun", "determiner"): [("verb", 0.90)],             # I _?_ the → verb
#     ("pronoun", "article"): [("verb", 0.90)],                # I _?_ a → verb
#     ("pronoun", "adverb"): [("verb", 0.85)],                 # I _?_ quickly → verb
#     ("pronoun", "preposition"): [("verb", 0.90)],            # I _?_ to → verb
    
#     # Preposition on left
#     ("preposition", "noun"): [("adjective", 0.85), ("determiner", 0.80)],  # to _?_ school → adjective/determiner
#     ("preposition", "proper_noun"): [("adjective", 0.80)],
#     ("preposition", "adjective"): [("determiner", 0.85), ("adverb", 0.70)],  # to _?_ big → determiner
#     ("preposition", "verb"): [("noun", 0.80)],               # to _?_ run → noun (gerund context)
    
#     # Adverb on left
#     ("adverb", "noun"): [("adjective", 0.85)],               # very _?_ car → adjective
#     ("adverb", "verb"): [("adverb", 0.75)],                  # very _?_ run → adverb
    
#     # Be verb on left
#     ("be", "noun"): [("adjective", 0.85), ("determiner", 0.80)],   # is _?_ doctor → adjective/determiner
#     ("be", "adjective"): [("adverb", 0.90)],                 # is _?_ happy → adverb
#     ("be", "preposition"): [("adverb", 0.80)],               # is _?_ in → adverb
    
#     # ==========================================================================
#     # ONLY LEFT NEIGHBOR KNOWN
#     # ==========================================================================
    
#     ("determiner", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("article", None): [("noun", 0.85), ("adjective", 0.80)],
#     ("adjective", None): [("noun", 0.90)],
#     ("pronoun", None): [("verb", 0.90), ("be", 0.80)],
#     ("verb", None): [("noun", 0.75), ("determiner", 0.70), ("adverb", 0.65), ("adjective", 0.60)],
#     ("preposition", None): [("noun", 0.80), ("determiner", 0.75), ("proper_noun", 0.70), ("adjective", 0.65)],
#     ("noun", None): [("verb", 0.80), ("noun", 0.60)],        # compound nouns possible
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
    
#     if pos_value in LOCATION_TYPES:
#         return 'proper_noun'
    
#     if pos_value in COMPOUND_NOUN_TYPES:
#         return 'noun'
    
#     return pos_value


# def is_compound_type(subtext: str) -> bool:
#     """Check if subtext indicates a compound noun type."""
#     if not subtext:
#         return False
#     return subtext.lower() in COMPOUND_NOUN_TYPES


# def is_location_type(subtext: str) -> bool:
#     """Check if subtext indicates a location type."""
#     if not subtext:
#         return False
#     return subtext.lower() in LOCATION_TYPES


# # =============================================================================
# # PASS 1: WORD VALIDATION
# # =============================================================================

# def validate_words(words: List[str]) -> List[Dict[str, Any]]:
#     """
#     Pass 1: Validate each word in the query.
    
#     Args:
#         words: List of words from the query
    
#     Returns:
#         List of validation results for each word
#     """
#     results = []
    
#     print("\n" + "=" * 60)
#     print("PASS 1: WORD VALIDATION")
#     print("=" * 60)
    
#     for position, word in enumerate(words, start=1):
#         print(f"\n📍 Position {position}: Checking '{word}'...")
        
#         validation = validate_word(word)
        
#         if validation['is_valid']:
#             # Fetch the actual metadata from the hash
#             metadata = get_term_metadata(word) or {}
#             pos = normalize_pos(metadata.get('pos', 'unknown'))
            
#             print(f"   ✅ VALID")
#             print(f"      POS: {pos}")
#             print(f"      Rank: {metadata.get('rank', 0)}")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'valid',
#                 'pos': pos,
#                 'metadata': metadata
#             })
#         else:
#             print(f"   ❓ UNKNOWN")
            
#             results.append({
#                 'position': position,
#                 'word': word.lower(),
#                 'status': 'unknown',
#                 'pos': 'unknown',
#                 'metadata': {}
#             })
    
#     return results


# # # =============================================================================
# # # PASS 2: PATTERN-BASED CORRECTION
# # # =============================================================================

# # # =============================================================================
# # # ENHANCED SEARCH FOR SHORT QUERIES
# # # =============================================================================

# # def search_without_pos_filter(
# #     word: str,
# #     max_distance: int = 3
# # ) -> Optional[Dict[str, Any]]:
# #     """
# #     Search for corrections without POS filtering.
# #     Used as fallback for single-word queries with no context.
    
# #     Args:
# #         word: The unknown/misspelled word
# #         max_distance: Maximum edit distance (higher for no-context searches)
    
# #     Returns:
# #         Best matching correction or None
# #     """
# #     print(f"      🔍 Fallback search for '{word}' (no POS filter)...")
    
# #     # Try the suggestions API first (often better for fuzzy matching)
# #     suggestions = get_suggestions(word, limit=10)
    
# #     if suggestions:
# #         matches = []
# #         for suggestion in suggestions:
# #             term = suggestion.get('term', '')
# #             distance = damerau_levenshtein_distance(word.lower(), term.lower())
# #             if distance <= max_distance:
# #                 suggestion['distance'] = distance
# #                 matches.append(suggestion)
        
# #         if matches:
# #             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
# #             best = matches[0]
# #             print(f"      ✅ Suggestion found: '{best['term']}' (distance: {best['distance']})")
# #             return best
    
# #     # Try with more candidates and higher distance tolerance
# #     candidates = generate_candidates_smart(word, max_candidates=100)
# #     found = batch_check_candidates(candidates)
    
# #     if found:
# #         matches = []
# #         for item in found:
# #             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
# #             if distance <= max_distance:
# #                 item['distance'] = distance
# #                 matches.append(item)
        
# #         if matches:
# #             matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
# #             best = matches[0]
# #             print(f"      ✅ Candidate found: '{best['term']}' (distance: {best['distance']})")
# #             return best
    
# #     print(f"      ❌ No matches found within distance {max_distance}")
# #     return None


# # def is_short_query(validation_results: List[Dict[str, Any]]) -> bool:
# #     """Check if this is a short query (1-2 words)."""
# #     return len(validation_results) <= 2


# # def has_context(position: int, tuple_array: List[Tuple[int, str]]) -> bool:
# #     """Check if a position has any known context (valid neighbors)."""
# #     for pos, tag in tuple_array:
# #         if pos == position - 1 and tag in ALLOWED_POS:
# #             return True
# #         if pos == position + 1 and tag in ALLOWED_POS:
# #             return True
# #     return False


# # def build_tuple_array(validation_results: List[Dict[str, Any]]) -> List[Tuple[int, str]]:
# #     """
# #     Build tuple array of (position, POS) from validation results.
    
# #     Args:
# #         validation_results: Results from Pass 1
    
# #     Returns:
# #         List of (position, pos) tuples
# #     """
# #     return [(r['position'], r['pos']) for r in validation_results]


# # def get_left_right_context(
# #     position: int,
# #     tuple_array: List[Tuple[int, str]]
# # ) -> Tuple[Optional[str], Optional[str]]:
# #     """
# #     Get POS of left and right neighbors for a position.
    
# #     Args:
# #         position: 1-based position of the unknown word
# #         tuple_array: List of (position, pos) tuples
    
# #     Returns:
# #         Tuple of (left_pos, right_pos), None if no neighbor or neighbor is unknown
# #     """
# #     left_pos = None
# #     right_pos = None
    
# #     # Find left neighbor
# #     for pos, tag in tuple_array:
# #         if pos == position - 1 and tag in ALLOWED_POS:
# #             left_pos = tag
# #             break
    
# #     # Find right neighbor
# #     for pos, tag in tuple_array:
# #         if pos == position + 1 and tag in ALLOWED_POS:
# #             right_pos = tag
# #             break
    
# #     return left_pos, right_pos


# # def predict_pos_from_context(
# #     left_pos: Optional[str],
# #     right_pos: Optional[str]
# # ) -> Optional[Tuple[str, float]]:
# #     """
# #     Predict POS based on left/right context using LOCAL_CONTEXT_RULES.
    
# #     Args:
# #         left_pos: POS of left neighbor (or None)
# #         right_pos: POS of right neighbor (or None)
    
# #     Returns:
# #         Tuple of (predicted_pos, confidence) or None
# #     """
# #     # Try both neighbors
# #     if left_pos and right_pos:
# #         key = (left_pos, right_pos)
# #         if key in LOCAL_CONTEXT_RULES:
# #             return LOCAL_CONTEXT_RULES[key][0]
    
# #     # Try left neighbor only
# #     if left_pos:
# #         key = (left_pos, None)
# #         if key in LOCAL_CONTEXT_RULES:
# #             return LOCAL_CONTEXT_RULES[key][0]
    
# #     # Try right neighbor only
# #     if right_pos:
# #         key = (None, right_pos)
# #         if key in LOCAL_CONTEXT_RULES:
# #             return LOCAL_CONTEXT_RULES[key][0]
    
# #     return None


# # def match_sentence_pattern(
# #     tuple_array: List[Tuple[int, str]],
# #     unknown_position: int
# # ) -> Optional[str]:
# #     """
# #     Match sentence against known patterns to predict POS for unknown.
    
# #     Args:
# #         tuple_array: List of (position, pos) tuples
# #         unknown_position: Position of the unknown word
    
# #     Returns:
# #         Predicted POS or None
# #     """
# #     # Find the starting POS (first known POS in sentence)
# #     starting_pos = None
# #     for pos, tag in tuple_array:
# #         if tag in SENTENCE_PATTERNS:
# #             starting_pos = tag
# #             break
    
# #     if not starting_pos:
# #         return None
    
# #     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
    
# #     # Build current sequence (excluding starting pos)
# #     sequence = [tag for pos, tag in tuple_array if pos > 1]
# #     unknown_index = unknown_position - 2  # Adjust for 0-based index after starting pos
    
# #     # Try to match patterns
# #     for pattern in patterns:
# #         if len(pattern) >= len(sequence):
# #             # Check if pattern matches known positions
# #             matches = True
# #             for i, tag in enumerate(sequence):
# #                 if tag != 'unknown' and i < len(pattern) and tag != pattern[i]:
# #                     matches = False
# #                     break
            
# #             if matches and unknown_index < len(pattern):
# #                 return pattern[unknown_index]
    
# #     return None


# # def search_with_pos_filter(
# #     word: str,
# #     required_pos: str,
# #     max_distance: int = 2
# # ) -> Optional[Dict[str, Any]]:
# #     """
# #     Search for corrections filtered by required POS.
    
# #     Args:
# #         word: The unknown/misspelled word
# #         required_pos: The POS the correction should have
# #         max_distance: Maximum edit distance
    
# #     Returns:
# #         Best matching correction or None
# #     """
# #     print(f"      🔍 Searching for {required_pos} near '{word}'...")
    
# #     # Generate candidates
# #     candidates = generate_candidates_smart(word, max_candidates=50)
    
# #     # Batch check candidates
# #     found = batch_check_candidates(candidates)
    
# #     if not found:
# #         print(f"      ❌ No candidates found")
# #         return None
    
# #     # Filter by POS and calculate distance
# #     matches = []
# #     for item in found:
# #         item_pos = normalize_pos(item.get('pos', 'unknown'))
        
# #         # Check if POS matches (or is compatible)
# #         pos_match = (
# #             item_pos == required_pos or
# #             (required_pos == 'proper_noun' and item.get('subtext', '').lower() in LOCATION_TYPES) or
# #             (required_pos == 'noun' and item_pos in ['noun', 'proper_noun'])
# #         )
        
# #         if pos_match:
# #             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
# #             if distance <= max_distance:
# #                 item['distance'] = distance
# #                 matches.append(item)
    
# #     if not matches:
# #         # Fallback: try without POS filter
# #         print(f"      🔄 No {required_pos} found, trying any POS...")
# #         for item in found:
# #             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
# #             if distance <= max_distance:
# #                 item['distance'] = distance
# #                 matches.append(item)
    
# #     if not matches:
# #         print(f"      ❌ No matches within distance {max_distance}")
# #         return None
    
# #     # Sort by distance, then by rank
# #     matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
# #     best = matches[0]
# #     print(f"      ✅ Found: '{best['term']}' (distance: {best['distance']}, POS: {best.get('pos', 'unknown')})")
    
# #     return best


# # def predict_pos_for_unknowns(
# #     validation_results: List[Dict[str, Any]]
# # ) -> List[Dict[str, Any]]:
# #     """
# #     Pass 2: Predict POS and correct unknown words.
    
# #     Args:
# #         validation_results: Results from Pass 1
    
# #     Returns:
# #         Updated results with corrections
# #     """
# #     print("\n" + "=" * 60)
# #     print("PASS 2: PATTERN-BASED CORRECTION")
# #     print("=" * 60)
    
# #     # Build tuple array
# #     tuple_array = build_tuple_array(validation_results)
    
# #     # Find unknowns
# #     unknowns = [r for r in validation_results if r['status'] == 'unknown']
    
# #     if not unknowns:
# #         print("\n   ✅ No unknown words to correct")
# #         return validation_results
    
# #     print(f"\n   Found {len(unknowns)} unknown word(s)")
    
# #     # Process each unknown
# #     for unknown in unknowns:
# #         position = unknown['position']
# #         word = unknown['word']
        
# #         print(f"\n📍 Position {position}: Correcting '{word}'...")
        
# #         # Get context
# #         left_pos, right_pos = get_left_right_context(position, tuple_array)
# #         print(f"   Context: [{left_pos}] _{word}_ [{right_pos}]")
        
# #         # Predict POS from context
# #         prediction = predict_pos_from_context(left_pos, right_pos)
        
# #         if prediction:
# #             predicted_pos, confidence = prediction
# #             print(f"   📊 Context prediction: {predicted_pos} (confidence: {confidence:.0%})")
# #         else:
# #             # Try pattern matching
# #             predicted_pos = match_sentence_pattern(tuple_array, position)
# #             if predicted_pos:
# #                 print(f"   📊 Pattern prediction: {predicted_pos}")
# #             else:
# #                 predicted_pos = 'noun'  # Default fallback
# #                 print(f"   📊 Default prediction: {predicted_pos}")
        
# #         # Search for correction with predicted POS
# #         correction = search_with_pos_filter(word, predicted_pos)
        
# #         if correction:
# #             # Update the result
# #             unknown['status'] = 'corrected'
# #             unknown['corrected'] = correction['term']
# #             unknown['pos'] = normalize_pos(correction.get('pos', 'unknown'))
# #             unknown['distance'] = correction['distance']
# #             unknown['metadata'] = correction
            
# #             # Update tuple array for subsequent predictions
# #             for i, (pos, tag) in enumerate(tuple_array):
# #                 if pos == position:
# #                     tuple_array[i] = (pos, unknown['pos'])
# #                     break
# #         else:
# #             print(f"   ❌ No correction found for '{word}'")
    
# #     return validation_results



# # =============================================================================
# # PASS 2: PATTERN-BASED CORRECTION
# # =============================================================================

# # =============================================================================
# # ENHANCED SEARCH FOR SHORT QUERIES
# # =============================================================================

# def search_without_pos_filter(
#     word: str,
#     max_distance: int = 3
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections without POS filtering.
#     Used as fallback for single-word queries with no context.
    
#     Args:
#         word: The unknown/misspelled word
#         max_distance: Maximum edit distance (higher for no-context searches)
    
#     Returns:
#         Best matching correction or None
#     """
#     print(f"      🔍 Fallback search for '{word}' (no POS filter)...")
    
#     # Try the suggestions API first (often better for fuzzy matching)
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
#             print(f"      ✅ Suggestion found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     # Try with more candidates and higher distance tolerance
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
#             print(f"      ✅ Candidate found: '{best['term']}' (distance: {best['distance']})")
#             return best
    
#     print(f"      ❌ No matches found within distance {max_distance}")
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
#     """
#     Build tuple array of (position, POS) from validation results.
    
#     Args:
#         validation_results: Results from Pass 1
    
#     Returns:
#         List of (position, pos) tuples
#     """
#     return [(r['position'], r['pos']) for r in validation_results]


# def get_left_right_context(
#     position: int,
#     tuple_array: List[Tuple[int, str]]
# ) -> Tuple[Optional[str], Optional[str]]:
#     """
#     Get POS of left and right neighbors for a position.
    
#     Args:
#         position: 1-based position of the unknown word
#         tuple_array: List of (position, pos) tuples
    
#     Returns:
#         Tuple of (left_pos, right_pos), None if no neighbor or neighbor is unknown
#     """
#     left_pos = None
#     right_pos = None
    
#     # Find left neighbor
#     for pos, tag in tuple_array:
#         if pos == position - 1 and tag in ALLOWED_POS:
#             left_pos = tag
#             break
    
#     # Find right neighbor
#     for pos, tag in tuple_array:
#         if pos == position + 1 and tag in ALLOWED_POS:
#             right_pos = tag
#             break
    
#     return left_pos, right_pos


# def predict_pos_from_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> Optional[Tuple[str, float]]:
#     """
#     Predict POS based on left/right context using LOCAL_CONTEXT_RULES.
    
#     Args:
#         left_pos: POS of left neighbor (or None)
#         right_pos: POS of right neighbor (or None)
    
#     Returns:
#         Tuple of (predicted_pos, confidence) or None
#     """
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
#     """
#     Match sentence against known patterns to predict POS for unknown.
    
#     Args:
#         tuple_array: List of (position, pos) tuples
#         unknown_position: Position of the unknown word
    
#     Returns:
#         Predicted POS or None
#     """
#     # Find the starting POS (first known POS in sentence)
#     starting_pos = None
#     for pos, tag in tuple_array:
#         if tag in SENTENCE_PATTERNS:
#             starting_pos = tag
#             break
    
#     if not starting_pos:
#         return None
    
#     patterns = SENTENCE_PATTERNS.get(starting_pos, [])
    
#     # Build current sequence (excluding starting pos)
#     sequence = [tag for pos, tag in tuple_array if pos > 1]
#     unknown_index = unknown_position - 2  # Adjust for 0-based index after starting pos
    
#     # Try to match patterns
#     for pattern in patterns:
#         if len(pattern) >= len(sequence):
#             # Check if pattern matches known positions
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
#     max_distance: int = 2
# ) -> Optional[Dict[str, Any]]:
#     """
#     Search for corrections filtered by required POS.
    
#     Args:
#         word: The unknown/misspelled word
#         required_pos: The POS the correction should have
#         max_distance: Maximum edit distance
    
#     Returns:
#         Best matching correction or None
#     """
#     print(f"      🔍 Searching for {required_pos} near '{word}'...")
    
#     # Generate candidates
#     candidates = generate_candidates_smart(word, max_candidates=50)
    
#     # Batch check candidates
#     found = batch_check_candidates(candidates)
    
#     if not found:
#         print(f"      ❌ No candidates found")
#         return None
    
#     # Filter by POS and calculate distance
#     matches = []
#     for item in found:
#         item_pos = normalize_pos(item.get('pos', 'unknown'))
        
#         # Check if POS matches (or is compatible)
#         pos_match = (
#             item_pos == required_pos or
#             (required_pos == 'proper_noun' and item.get('subtext', '').lower() in LOCATION_TYPES) or
#             (required_pos == 'noun' and item_pos in ['noun', 'proper_noun'])
#         )
        
#         if pos_match:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         # Fallback: try without POS filter
#         print(f"      🔄 No {required_pos} found, trying any POS...")
#         for item in found:
#             distance = damerau_levenshtein_distance(word.lower(), item['term'].lower())
#             if distance <= max_distance:
#                 item['distance'] = distance
#                 matches.append(item)
    
#     if not matches:
#         print(f"      ❌ No matches within distance {max_distance}")
#         return None
    
#     # Sort by distance, then by rank
#     matches.sort(key=lambda x: (x['distance'], -x.get('rank', 0)))
    
#     best = matches[0]
#     print(f"      ✅ Found: '{best['term']}' (distance: {best['distance']}, POS: {best.get('pos', 'unknown')})")
    
#     return best


# # def detect_pattern_violations(
# #     validation_results: List[Dict[str, Any]],
# #     tuple_array: List[Tuple[int, str]]
# # ) -> List[Dict[str, Any]]:
# #     """
# #     Detect words that are valid but violate grammatical patterns.
    
# #     Args:
# #         validation_results: Results from Pass 1
# #         tuple_array: List of (position, pos) tuples
    
# #     Returns:
# #         List of violations with position, word, current_pos, and expected_pos
# #     """
# #     violations = []
    
# #     for i, result in enumerate(validation_results):
# #         if result['status'] != 'valid':
# #             continue
        
# #         position = result['position']
# #         current_pos = result['pos']
        
# #         # Get context
# #         left_pos, right_pos = get_left_right_context(position, tuple_array)
        
# #         # Predict what POS should be here based on context
# #         prediction = predict_pos_from_context(left_pos, right_pos)
        
# #         if prediction:
# #             expected_pos, confidence = prediction
            
# #             # Check if current POS matches expected
# #             if current_pos != expected_pos and confidence >= 0.80:
# #                 violations.append({
# #                     'position': position,
# #                     'word': result['word'],
# #                     'current_pos': current_pos,
# #                     'expected_pos': expected_pos,
# #                     'confidence': confidence,
# #                     'context': (left_pos, right_pos)
# #                 })
    
# #     return violations
# def get_valid_pos_for_context(
#     left_pos: Optional[str],
#     right_pos: Optional[str]
# ) -> List[Tuple[str, float]]:
#     """
#     Get ALL valid POS options for a given context.
    
#     Args:
#         left_pos: POS of left neighbor (or None)
#         right_pos: POS of right neighbor (or None)
    
#     Returns:
#         List of (valid_pos, confidence) tuples, or empty list if no rules match
#     """
#     # Try both neighbors first (most specific)
#     if left_pos and right_pos:
#         key = (left_pos, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     # Try left neighbor only
#     if left_pos:
#         key = (left_pos, None)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     # Try right neighbor only
#     if right_pos:
#         key = (None, right_pos)
#         if key in LOCAL_CONTEXT_RULES:
#             return LOCAL_CONTEXT_RULES[key]
    
#     return []


# def detect_pattern_violations(
#     validation_results: List[Dict[str, Any]],
#     tuple_array: List[Tuple[int, str]]
# ) -> List[Dict[str, Any]]:
#     """
#     Detect words that are valid but violate grammatical patterns.
    
#     Args:
#         validation_results: Results from Pass 1
#         tuple_array: List of (position, pos) tuples
    
#     Returns:
#         List of violations with position, word, current_pos, and expected_pos
#     """
#     violations = []
    
#     for i, result in enumerate(validation_results):
#         if result['status'] != 'valid':
#             continue
        
#         position = result['position']
#         current_pos = result['pos']
        
#         # Get context
#         left_pos, right_pos = get_left_right_context(position, tuple_array)
        
#         # Get ALL valid POS options for this context
#         valid_options = get_valid_pos_for_context(left_pos, right_pos)
        
#         if not valid_options:
#             # No rules for this context, skip
#             continue
        
#         # Extract just the POS values (ignore confidence)
#         valid_pos_list = [pos for pos, confidence in valid_options]
        
#         # Check if current POS is in the valid options
#         if current_pos not in valid_pos_list:
#             # Find the best expected POS (highest confidence)
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
#     tuple_array: List[Tuple[int, str]]
# ) -> List[Dict[str, Any]]:
#     """
#     Attempt to correct words that violate grammatical patterns.
    
#     Args:
#         validation_results: Results from Pass 1
#         violations: Detected pattern violations
#         tuple_array: List of (position, pos) tuples
    
#     Returns:
#         Updated validation results
#     """
#     for violation in violations:
#         position = violation['position']
#         word = violation['word']
#         expected_pos = violation['expected_pos']
        
#         print(f"\n📍 Position {position}: Pattern violation for '{word}'...")
#         print(f"   Current POS: {violation['current_pos']}")
#         print(f"   Expected POS: {expected_pos} (confidence: {violation['confidence']:.0%})")
#         print(f"   Context: [{violation['context'][0]}] _{word}_ [{violation['context'][1]}]")
        
#         # Search for a word with the expected POS
#         correction = search_with_pos_filter(word, expected_pos)
        
#         if correction:
#             # Find and update the result
#             for result in validation_results:
#                 if result['position'] == position:
#                     result['status'] = 'corrected'
#                     result['corrected'] = correction['term']
#                     result['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                     result['distance'] = correction['distance']
#                     result['metadata'] = correction
#                     result['correction_reason'] = 'pattern_violation'
                    
#                     # Update tuple array
#                     for i, (pos, tag) in enumerate(tuple_array):
#                         if pos == position:
#                             tuple_array[i] = (pos, result['pos'])
#                             break
#                     break
#         else:
#             print(f"   ❌ No correction found for '{word}'")
    
#     return validation_results


# def predict_pos_for_unknowns(
#     validation_results: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """
#     Pass 2: Predict POS and correct unknown words AND pattern violations.
    
#     Args:
#         validation_results: Results from Pass 1
    
#     Returns:
#         Updated results with corrections
#     """
#     print("\n" + "=" * 60)
#     print("PASS 2: PATTERN-BASED CORRECTION")
#     print("=" * 60)
    
#     # Build tuple array
#     tuple_array = build_tuple_array(validation_results)
    
#     # =========================================================================
#     # STEP 1: Correct unknown words
#     # =========================================================================
#     unknowns = [r for r in validation_results if r['status'] == 'unknown']
    
#     if unknowns:
#         print(f"\n   Found {len(unknowns)} unknown word(s)")
        
#         for unknown in unknowns:
#             position = unknown['position']
#             word = unknown['word']
            
#             print(f"\n📍 Position {position}: Correcting '{word}'...")
            
#             # Get context
#             left_pos, right_pos = get_left_right_context(position, tuple_array)
#             print(f"   Context: [{left_pos}] _{word}_ [{right_pos}]")
            
#             # Predict POS from context
#             prediction = predict_pos_from_context(left_pos, right_pos)
            
#             if prediction:
#                 predicted_pos, confidence = prediction
#                 print(f"   📊 Context prediction: {predicted_pos} (confidence: {confidence:.0%})")
#             else:
#                 # Try pattern matching
#                 predicted_pos = match_sentence_pattern(tuple_array, position)
#                 if predicted_pos:
#                     print(f"   📊 Pattern prediction: {predicted_pos}")
#                 else:
#                     predicted_pos = 'noun'  # Default fallback
#                     print(f"   📊 Default prediction: {predicted_pos}")
            
#             # Search for correction with predicted POS
#             correction = search_with_pos_filter(word, predicted_pos)
            
#             if correction:
#                 # Update the result
#                 unknown['status'] = 'corrected'
#                 unknown['corrected'] = correction['term']
#                 unknown['pos'] = normalize_pos(correction.get('pos', 'unknown'))
#                 unknown['distance'] = correction['distance']
#                 unknown['metadata'] = correction
                
#                 # Update tuple array for subsequent predictions
#                 for i, (pos, tag) in enumerate(tuple_array):
#                     if pos == position:
#                         tuple_array[i] = (pos, unknown['pos'])
#                         break
#             else:
#                 print(f"   ❌ No correction found for '{word}'")
#     else:
#         print("\n   ✅ No unknown words to correct")
    
#     # =========================================================================
#     # STEP 2: Detect and correct pattern violations among valid words
#     # =========================================================================
#     print("\n" + "-" * 40)
#     print("   Checking for pattern violations...")
#     print("-" * 40)
    
#     # Rebuild tuple array after unknown corrections
#     tuple_array = build_tuple_array(validation_results)
    
#     # Detect violations
#     violations = detect_pattern_violations(validation_results, tuple_array)
    
#     if violations:
#         print(f"\n   Found {len(violations)} pattern violation(s)")
#         validation_results = correct_pattern_violations(
#             validation_results, violations, tuple_array
#         )
#     else:
#         print("\n   ✅ No pattern violations detected")
    
#     return validation_results

# # =============================================================================
# # PASS 3: BIGRAM DETECTION
# # =============================================================================

# def check_bigram_exists(word1: str, word2: str) -> Optional[Dict[str, Any]]:
#     """
#     Check if two words form a bigram in Redis.
    
#     Args:
#         word1: First word
#         word2: Second word
    
#     Returns:
#         Bigram metadata if found, None otherwise
#     """
#     bigram = f"{word1.lower()} {word2.lower()}"
#     metadata = get_term_metadata(bigram)
    
#     if metadata and metadata.get('exists'):
#         return metadata
    
#     return None


# def detect_bigrams(
#     validation_results: List[Dict[str, Any]]
# ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
#     """
#     Pass 3: Detect bigrams in the corrected words.
    
#     Args:
#         validation_results: Results from Pass 2
    
#     Returns:
#         Tuple of (updated_results, detected_bigrams)
#     """
#     print("\n" + "=" * 60)
#     print("PASS 3: BIGRAM DETECTION")
#     print("=" * 60)
    
#     bigrams_found = []
#     positions_to_merge = set()
    
#     # Check consecutive word pairs
#     for i in range(len(validation_results) - 1):
#         current = validation_results[i]
#         next_item = validation_results[i + 1]
        
#         # Skip if either position is already part of a bigram
#         if current['position'] in positions_to_merge:
#             continue
        
#         # Get the actual words (corrected if available)
#         word1 = current.get('corrected', current['word'])
#         word2 = next_item.get('corrected', next_item['word'])
        
#         print(f"\n📍 Checking: '{word1}' + '{word2}'...")
        
#         # Check if bigram exists
#         bigram_metadata = check_bigram_exists(word1, word2)
        
#         if bigram_metadata:
#             subtext = bigram_metadata.get('subtext', '')
#             entity = bigram_metadata.get('entity', 'bigram')
            
#             print(f"   ✅ BIGRAM FOUND")
#             print(f"      Display: {bigram_metadata.get('display', '')}")
#             print(f"      Subtext: {subtext}")
#             print(f"      Entity: {entity}")
            
#             # Determine POS for bigram
#             if is_location_type(subtext):
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
#                 'subtext': subtext,
#                 'entity': entity,
#                 'metadata': bigram_metadata
#             })
            
#             positions_to_merge.add(current['position'])
#             positions_to_merge.add(next_item['position'])
#         else:
#             print(f"   ❌ Not a bigram")
    
#     if not bigrams_found:
#         print("\n   ✅ No bigrams detected")
    
#     return validation_results, bigrams_found


# def merge_bigrams_into_result(
#     validation_results: List[Dict[str, Any]],
#     bigrams: List[Dict[str, Any]]
# ) -> List[Dict[str, Any]]:
#     """
#     Merge detected bigrams into the final result.
    
#     Args:
#         validation_results: Results from Pass 2
#         bigrams: Detected bigrams from Pass 3
    
#     Returns:
#         Final merged results
#     """
#     if not bigrams:
#         return validation_results
    
#     # Build position map for bigrams
#     bigram_starts = {b['position_start']: b for b in bigrams}
#     bigram_positions = set()
#     for b in bigrams:
#         bigram_positions.add(b['position_start'])
#         bigram_positions.add(b['position_end'])
    
#     # Build merged result
#     merged = []
#     skip_next = False
    
#     for result in validation_results:
#         if skip_next:
#             skip_next = False
#             continue
        
#         position = result['position']
        
#         if position in bigram_starts:
#             # This is the start of a bigram
#             bigram = bigram_starts[position]
#             merged.append({
#                 'position': position,
#                 'word': bigram['bigram'],
#                 'status': 'bigram',
#                 'pos': bigram['pos'],
#                 'subtext': bigram['subtext'],
#                 'entity': bigram['entity'],
#                 'metadata': bigram['metadata']
#             })
#             skip_next = True
#         elif position not in bigram_positions:
#             # Regular word (not part of any bigram)
#             merged.append(result)
    
#     return merged


# # =============================================================================
# # MAIN ORCHESTRATOR
# # =============================================================================

# def word_discovery_multi(
#     query: str,
#     redis_client=None,
#     prefix: str = "prefix"
# ) -> Tuple[List[Dict[str, Any]], List[Tuple[int, str]], str]:
#     """
#     Main entry point: Process query through all three passes.
    
#     Args:
#         query: The input query string
#         redis_client: Redis client (optional)
#         prefix: Redis key prefix
    
#     Returns:
#         Tuple of (corrections, tuple_array, corrected_query)
#     """
#     print("\n" + "=" * 60)
#     print(f"🔍 PROCESSING QUERY: '{query}'")
#     print("=" * 60)
    
#     words = query.split()
    
#     if not words:
#         return [], [], ""
    
#     # =========================================================================
#     # PASS 1: Validate each word
#     # =========================================================================
#     validation_results = validate_words(words)
    
#     # =========================================================================
#     # PASS 2: Pattern-based correction for unknowns
#     # =========================================================================
#     validation_results = predict_pos_for_unknowns(validation_results)
    
#     # =========================================================================
#     # PASS 3: Bigram detection
#     # =========================================================================
#     validation_results, bigrams = detect_bigrams(validation_results)
    
#     # Merge bigrams into results
#     final_results = merge_bigrams_into_result(validation_results, bigrams)
    
#     # =========================================================================
#     # BUILD OUTPUT
#     # =========================================================================
    
#     # Build corrections list
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
    
#     # Add bigram info to corrections if the original was misspelled
#     for b in bigrams:
#         # Check if either word in the bigram was corrected
#         for r in validation_results:
#             if r['position'] in [b['position_start'], b['position_end']]:
#                 if r['status'] == 'corrected':
#                     # Already in corrections
#                     pass
    
#     # Build tuple array
#     tuple_array = [(r['position'], r['pos']) for r in final_results]
    
#     # Build corrected query
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
#     print("\n" + "=" * 60)
#     print("📊 FINAL SUMMARY")
#     print("=" * 60)
#     print(f"   Original:    '{query}'")
#     print(f"   Corrected:   '{corrected_query}'")
#     print(f"   Corrections: {len(corrections)}")
#     print(f"   Bigrams:     {len(bigrams)}")
    
#     if corrections:
#         print("\n   Word corrections:")
#         for c in corrections:
#             print(f"      • '{c['original']}' → '{c['corrected']}' ({c['pos']})")
    
#     if bigrams:
#         print("\n   Bigrams detected:")
#         for b in bigrams:
#             print(f"      • '{b['bigram']}' ({b['subtext']})")
    
#     print("\n   Final structure:")
#     for r in final_results:
#         print(f"      [{r['position']}] {r['word']} → {r['pos']}")
    
#     print("=" * 60 + "\n")
    
#     return corrections, tuple_array, corrected_query





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