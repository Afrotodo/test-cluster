# #!/usr/bin/env python3
# """
# COMPREHENSIVE TEST SCRIPT FOR word_discovery.py
# ===============================================

# This test script:
# 1. FORCES 100% Redis usage - bypasses RAM cache entirely
# 2. Tests complex edge cases, not simple queries
# 3. Tests the full pipeline: validation, POS prediction, spelling correction,
#    bigram/trigram detection, filter extraction, and intent detection

# Run with: python test_word_discovery_redis.py

# Requirements:
# - Redis must be running and populated with vocabulary data
# - searchapi.py must be accessible with Redis functions
# """

# import sys
# import os
# import time
# import json
# from typing import Dict, Any, List, Optional, Tuple
# from dataclasses import dataclass
# from enum import Enum
# import traceback

# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Force Redis mode - set this BEFORE importing word_discovery
# FORCE_REDIS_MODE = True
# VERBOSE_OUTPUT = True
# STOP_ON_FAILURE = False  # Set True to stop at first failure

# # =============================================================================
# # MOCK THE RAM CACHE TO FORCE REDIS USAGE
# # =============================================================================

# class EmptyVocabCache:
#     """
#     Mock vocabulary cache that returns nothing.
#     This forces the system to rely on Redis for all lookups.
#     """
#     def __init__(self):
#         self.loaded = False
#         self._call_log = []
    
#     def get_term(self, word: str) -> Optional[Dict]:
#         """Always return None - forces word to be marked as 'unknown'"""
#         self._call_log.append(('get_term', word))
#         return None
    
#     def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
#         """Always return None - bigrams must come from Redis"""
#         self._call_log.append(('get_bigram', word1, word2))
#         return None
    
#     def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
#         """Always return None - trigrams must come from Redis"""
#         self._call_log.append(('get_trigram', word1, word2, word3))
#         return None
    
#     def is_stopword(self, word: str) -> bool:
#         """Return True for common stopwords only"""
#         stopwords = {
#             'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
#             'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
#             'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
#             'should', 'may', 'might', 'must', 'can', 'and', 'or', 'but', 'if',
#             'then', 'else', 'when', 'where', 'why', 'how', 'what', 'which',
#             'who', 'whom', 'this', 'that', 'these', 'those', 'i', 'you', 'he',
#             'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my',
#             'your', 'his', 'its', 'our', 'their', 'about', 'into', 'through',
#             'during', 'before', 'after', 'above', 'below', 'between', 'under',
#             'again', 'further', 'once', 'here', 'there', 'all', 'each', 'few',
#             'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only',
#             'own', 'same', 'so', 'than', 'too', 'very', 'just', 'also'
#         }
#         self._call_log.append(('is_stopword', word))
#         return word.lower() in stopwords
    
#     def get_stopword_pos(self, word: str) -> str:
#         """Return POS for stopwords"""
#         stopword_pos = {
#             'the': 'article', 'a': 'article', 'an': 'article',
#             'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#             'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#             'with': 'preposition', 'by': 'preposition', 'from': 'preposition',
#             'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be', 'be': 'be',
#             'been': 'be', 'being': 'be',
#             'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
#             'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
#             'will': 'modal', 'would': 'modal', 'could': 'modal',
#             'should': 'modal', 'may': 'modal', 'might': 'modal',
#             'must': 'modal', 'can': 'modal',
#             'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#             'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#             'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#             'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun',
#             'us': 'pronoun', 'them': 'pronoun',
#             'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
#             'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
#             'this': 'determiner', 'that': 'determiner',
#             'these': 'determiner', 'those': 'determiner',
#             'what': 'wh_pronoun', 'which': 'wh_pronoun', 'who': 'wh_pronoun',
#             'whom': 'wh_pronoun', 'where': 'wh_pronoun', 'when': 'wh_pronoun',
#             'why': 'wh_pronoun', 'how': 'wh_pronoun',
#             'not': 'negation', 'no': 'negation',
#             'about': 'preposition', 'into': 'preposition', 'through': 'preposition',
#         }
#         self._call_log.append(('get_stopword_pos', word))
#         return stopword_pos.get(word.lower(), 'unknown')
    
#     def status(self) -> Dict[str, Any]:
#         return {
#             'loaded': False,
#             'terms_count': 0,
#             'bigrams_count': 0,
#             'trigrams_count': 0,
#             'mode': 'REDIS_TEST_MODE'
#         }
    
#     def get_call_log(self) -> List[Tuple]:
#         """Get log of all calls made to this cache"""
#         return self._call_log
    
#     def clear_call_log(self):
#         """Clear the call log"""
#         self._call_log = []


# # =============================================================================
# # REDIS-BACKED VOCAB CACHE FOR TESTING
# # =============================================================================

# class RedisVocabCache:
#     """
#     Vocabulary cache that uses Redis for ALL lookups.
#     This is what we use for testing.
#     """
#     def __init__(self, redis_client=None):
#         self.loaded = True  # Pretend we're loaded so code doesn't skip
#         self.redis_client = redis_client
#         self._call_log = []
#         self._redis_available = False
        
#         # Try to import Redis functions
#         try:
#             from searchapi import (
#                 RedisLookupTable,
#                 get_fuzzy_matches,
#                 get_suggestions,
#             )
#             self._get_suggestions = get_suggestions
#             self._get_fuzzy_matches = get_fuzzy_matches
#             self._redis_available = True
#             print("✅ Redis functions imported successfully")
#         except ImportError as e:
#             print(f"⚠️ Could not import Redis functions: {e}")
#             self._redis_available = False
    
#     def _redis_lookup(self, key: str, hash_name: str = "vocabulary") -> Optional[Dict]:
#         """Direct Redis hash lookup"""
#         if not self._redis_available:
#             return None
#         try:
#             # This would need to be implemented based on your Redis structure
#             # For now, return None and let get_suggestions handle it
#             return None
#         except Exception as e:
#             print(f"Redis lookup error: {e}")
#             return None
    
#     def get_term(self, word: str) -> Optional[Dict]:
#         """Look up term in Redis"""
#         self._call_log.append(('get_term', word))
        
#         if not self._redis_available:
#             return None
        
#         try:
#             # Use get_suggestions with distance 0 to find exact match
#             result = self._get_suggestions(word, limit=5, max_distance=0)
#             suggestions = result.get('suggestions', [])
            
#             for s in suggestions:
#                 if s.get('term', '').lower() == word.lower():
#                     return {
#                         'word': s.get('term'),
#                         'pos': s.get('pos', 'unknown'),
#                         'rank': s.get('rank', 0),
#                         'category': s.get('category', ''),
#                         'display': s.get('display', s.get('term')),
#                     }
#             return None
#         except Exception as e:
#             print(f"Error in get_term({word}): {e}")
#             return None
    
#     def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
#         """Look up bigram in Redis"""
#         self._call_log.append(('get_bigram', word1, word2))
        
#         if not self._redis_available:
#             return None
        
#         try:
#             # Search for the bigram phrase
#             bigram = f"{word1} {word2}"
#             result = self._get_suggestions(bigram, limit=5, max_distance=0)
#             suggestions = result.get('suggestions', [])
            
#             for s in suggestions:
#                 term = s.get('term', '').lower()
#                 if term == bigram.lower():
#                     return {
#                         'bigram': s.get('term'),
#                         'pos': s.get('pos', 'unknown'),
#                         'rank': s.get('rank', 0),
#                         'category': s.get('category', ''),
#                     }
#             return None
#         except Exception as e:
#             print(f"Error in get_bigram({word1}, {word2}): {e}")
#             return None
    
#     def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
#         """Look up trigram in Redis"""
#         self._call_log.append(('get_trigram', word1, word2, word3))
        
#         if not self._redis_available:
#             return None
        
#         try:
#             trigram = f"{word1} {word2} {word3}"
#             result = self._get_suggestions(trigram, limit=5, max_distance=0)
#             suggestions = result.get('suggestions', [])
            
#             for s in suggestions:
#                 term = s.get('term', '').lower()
#                 if term == trigram.lower():
#                     return {
#                         'trigram': s.get('term'),
#                         'pos': s.get('pos', 'unknown'),
#                         'rank': s.get('rank', 0),
#                         'category': s.get('category', ''),
#                     }
#             return None
#         except Exception as e:
#             print(f"Error in get_trigram: {e}")
#             return None
    
#     def is_stopword(self, word: str) -> bool:
#         """Check if word is a stopword"""
#         stopwords = {
#             'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
#             'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
#             'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
#             'should', 'may', 'might', 'must', 'can', 'and', 'or', 'but', 'if',
#             'then', 'else', 'when', 'where', 'why', 'how', 'what', 'which',
#             'who', 'whom', 'this', 'that', 'these', 'those', 'i', 'you', 'he',
#             'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my',
#             'your', 'his', 'its', 'our', 'their', 'about', 'into', 'show', 'find',
#             'search', 'looking', 'want', 'need', 'give', 'tell', 'list'
#         }
#         return word.lower() in stopwords
    
#     def get_stopword_pos(self, word: str) -> str:
#         """Return POS for stopwords"""
#         stopword_pos = {
#             'the': 'article', 'a': 'article', 'an': 'article',
#             'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
#             'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
#             'with': 'preposition', 'by': 'preposition', 'from': 'preposition',
#             'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be',
#             'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
#             'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
#             'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
#             'what': 'wh_pronoun', 'which': 'wh_pronoun', 'who': 'wh_pronoun',
#             'show': 'verb', 'find': 'verb', 'search': 'verb', 'looking': 'verb',
#             'want': 'verb', 'need': 'verb', 'give': 'verb', 'tell': 'verb',
#             'list': 'verb',
#         }
#         return stopword_pos.get(word.lower(), 'unknown')
    
#     def status(self) -> Dict[str, Any]:
#         return {
#             'loaded': True,
#             'mode': 'REDIS_DIRECT',
#             'redis_available': self._redis_available
#         }
    
#     def get_call_log(self) -> List[Tuple]:
#         return self._call_log
    
#     def clear_call_log(self):
#         self._call_log = []


# # =============================================================================
# # TEST CASE DEFINITIONS
# # =============================================================================

# class TestCategory(Enum):
#     SPELLING_CORRECTION = "Spelling Correction"
#     BIGRAM_DETECTION = "Bigram Detection"
#     TRIGRAM_DETECTION = "Trigram Detection"
#     LOCATION_EXTRACTION = "Location Extraction"
#     PERSON_DETECTION = "Person Detection"
#     TEMPORAL_SORTING = "Temporal Sorting"
#     COMPOUND_WORDS = "Compound Words"
#     ADJACENT_TYPOS = "Adjacent Typos"
#     GRAMMAR_PREDICTION = "Grammar/POS Prediction"
#     FILTER_EXTRACTION = "Filter Extraction"
#     MIXED_COMPLEX = "Mixed Complex Queries"
#     EDGE_CASES = "Edge Cases"
#     INTENT_DETECTION = "Intent Detection"


# @dataclass
# class TestCase:
#     """Represents a single test case"""
#     name: str
#     query: str
#     category: TestCategory
#     expected: Dict[str, Any]
#     description: str = ""
    
#     def __str__(self):
#         return f"[{self.category.value}] {self.name}: '{self.query}'"


# # =============================================================================
# # COMPREHENSIVE TEST CASES
# # =============================================================================

# TEST_CASES = [
#     # =========================================================================
#     # SPELLING CORRECTION - Keyboard-aware typos
#     # =========================================================================
#     TestCase(
#         name="Adjacent key typo - 'teh' -> 'the'",
#         query="teh first black president",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'the',
#         },
#         description="Tests keyboard-adjacent typo correction (e/h are adjacent)"
#     ),
#     TestCase(
#         name="Transposition typo - 'hte' -> 'the'",
#         query="hte history of jazz",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'the',
#         },
#         description="Tests Damerau-Levenshtein transposition detection"
#     ),
#     TestCase(
#         name="Double letter typo - 'Atllanta'",
#         query="musicians from Atllanta Georgia",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'atlanta',
#         },
#         description="Tests double letter insertion typo"
#     ),
#     TestCase(
#         name="Missing letter - 'Harlm' -> 'Harlem'",
#         query="artists in Harlm renaissance",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'harlem',
#         },
#         description="Tests missing letter typo"
#     ),
#     TestCase(
#         name="Multiple typos in query",
#         query="teh frist balck presedent of america",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'correction_count_min': 3,
#         },
#         description="Tests multiple simultaneous typos"
#     ),
#     TestCase(
#         name="Phonetic typo - 'filosophy' -> 'philosophy'",
#         query="african filosophy and religion",
#         category=TestCategory.SPELLING_CORRECTION,
#         expected={
#             'has_correction': True,
#         },
#         description="Tests phonetically similar typo"
#     ),

#     # =========================================================================
#     # BIGRAM DETECTION - Two-word phrases
#     # =========================================================================
#     TestCase(
#         name="Person bigram - 'Martin Luther'",
#         query="Martin Luther King speeches",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#             'ngram_contains': 'martin luther',
#         },
#         description="Tests detection of famous person name bigram"
#     ),
#     TestCase(
#         name="Location bigram - 'New York'",
#         query="jazz clubs in New York city",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#             'ngram_contains': 'new york',
#         },
#         description="Tests detection of city name bigram"
#     ),
#     TestCase(
#         name="Concept bigram - 'civil rights'",
#         query="civil rights movement leaders",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#             'ngram_contains': 'civil rights',
#         },
#         description="Tests detection of concept/topic bigram"
#     ),
#     TestCase(
#         name="Institution bigram - 'Howard University'",
#         query="Howard University alumni",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#             'ngram_contains': 'howard university',
#         },
#         description="Tests detection of institution name bigram"
#     ),
#     TestCase(
#         name="Historical bigram - 'Black Panther'",
#         query="Black Panther Party history",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#         },
#         description="Tests detection of historical organization bigram"
#     ),

#     # =========================================================================
#     # TRIGRAM DETECTION - Three-word phrases
#     # =========================================================================
#     TestCase(
#         name="Person trigram - 'Martin Luther King'",
#         query="Martin Luther King Jr biography",
#         category=TestCategory.TRIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#             'ngram_type': 'trigram',
#         },
#         description="Tests detection of three-word person name"
#     ),
#     TestCase(
#         name="Event trigram - 'March on Washington'",
#         query="March on Washington 1963",
#         category=TestCategory.TRIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#         },
#         description="Tests detection of historical event trigram"
#     ),
#     TestCase(
#         name="Concept trigram - 'historically black colleges'",
#         query="historically black colleges and universities",
#         category=TestCategory.TRIGRAM_DETECTION,
#         expected={
#             'has_ngram': True,
#         },
#         description="Tests detection of concept trigram (HBCU)"
#     ),

#     # =========================================================================
#     # LOCATION EXTRACTION
#     # =========================================================================
#     TestCase(
#         name="US State - Georgia",
#         query="musicians from Georgia",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'has_location': True,
#             'location_field': 'location_state',
#         },
#         description="Tests extraction of US state as location filter"
#     ),
#     TestCase(
#         name="US City - Atlanta",
#         query="restaurants in Atlanta",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'has_location': True,
#             'location_field': 'location_city',
#         },
#         description="Tests extraction of US city as location filter"
#     ),
#     TestCase(
#         name="City + State combo",
#         query="history of Birmingham Alabama",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'location_count_min': 2,
#         },
#         description="Tests extraction of both city and state"
#     ),
#     TestCase(
#         name="Multi-word city - New Orleans",
#         query="jazz musicians from New Orleans Louisiana",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'has_location': True,
#             'ngram_contains': 'new orleans',
#         },
#         description="Tests multi-word city detection as bigram + location"
#     ),
#     TestCase(
#         name="Neighborhood location - Harlem",
#         query="poets from Harlem",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'has_location': True,
#         },
#         description="Tests neighborhood/district as location"
#     ),
#     TestCase(
#         name="Region - Deep South",
#         query="music from the Deep South",
#         category=TestCategory.LOCATION_EXTRACTION,
#         expected={
#             'has_ngram': True,
#         },
#         description="Tests regional bigram detection"
#     ),

#     # =========================================================================
#     # PERSON DETECTION
#     # =========================================================================
#     TestCase(
#         name="Single name person",
#         query="biography of Malcolm X",
#         category=TestCategory.PERSON_DETECTION,
#         expected={
#             'category_has_person': True,
#         },
#         description="Tests detection of single-name famous person"
#     ),
#     TestCase(
#         name="Full name person",
#         query="Rosa Parks and the bus boycott",
#         category=TestCategory.PERSON_DETECTION,
#         expected={
#             'category_has_person': True,
#             'has_ngram': True,
#         },
#         description="Tests detection of full name as bigram + person category"
#     ),
#     TestCase(
#         name="Person with title",
#         query="Dr Martin Luther King speeches",
#         category=TestCategory.PERSON_DETECTION,
#         expected={
#             'category_has_person': True,
#         },
#         description="Tests person detection with honorific title"
#     ),

#     # =========================================================================
#     # TEMPORAL SORTING
#     # =========================================================================
#     TestCase(
#         name="Temporal - 'first'",
#         query="first black mayor of Atlanta",
#         category=TestCategory.TEMPORAL_SORTING,
#         expected={
#             'has_sort': True,
#             'sort_order': 'asc',
#         },
#         description="Tests 'first' triggers ascending sort"
#     ),
#     TestCase(
#         name="Temporal - 'oldest'",
#         query="oldest HBCU in America",
#         category=TestCategory.TEMPORAL_SORTING,
#         expected={
#             'has_sort': True,
#             'sort_order': 'asc',
#         },
#         description="Tests 'oldest' triggers ascending sort"
#     ),
#     TestCase(
#         name="Temporal - 'latest'",
#         query="latest news about civil rights",
#         category=TestCategory.TEMPORAL_SORTING,
#         expected={
#             'has_sort': True,
#             'sort_order': 'desc',
#         },
#         description="Tests 'latest' triggers descending sort"
#     ),
#     TestCase(
#         name="Temporal - 'recent'",
#         query="recent achievements in black history",
#         category=TestCategory.TEMPORAL_SORTING,
#         expected={
#             'has_sort': True,
#             'sort_order': 'desc',
#         },
#         description="Tests 'recent' triggers descending sort"
#     ),
#     TestCase(
#         name="Temporal - 'earliest'",
#         query="earliest black churches in America",
#         category=TestCategory.TEMPORAL_SORTING,
#         expected={
#             'has_sort': True,
#             'sort_order': 'asc',
#         },
#         description="Tests 'earliest' triggers ascending sort"
#     ),

#     # =========================================================================
#     # GRAMMAR/POS PREDICTION
#     # =========================================================================
#     TestCase(
#         name="POS prediction - adjective slot",
#         query="the beutiful music of jazz",
#         category=TestCategory.GRAMMAR_PREDICTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'beautiful',
#         },
#         description="Tests POS prediction: 'the ___ music' -> adjective"
#     ),
#     TestCase(
#         name="POS prediction - noun slot",
#         query="history of the movment",
#         category=TestCategory.GRAMMAR_PREDICTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'movement',
#         },
#         description="Tests POS prediction: 'the ___' -> noun"
#     ),
#     TestCase(
#         name="POS prediction - verb slot",
#         query="musicians who perfomed at Apollo",
#         category=TestCategory.GRAMMAR_PREDICTION,
#         expected={
#             'has_correction': True,
#             'corrected_contains': 'performed',
#         },
#         description="Tests POS prediction: 'who ___' -> verb"
#     ),

#     # =========================================================================
#     # FILTER EXTRACTION
#     # =========================================================================
#     TestCase(
#         name="Topic filter - education",
#         query="education in black communities",
#         category=TestCategory.FILTER_EXTRACTION,
#         expected={
#             'has_filter': True,
#         },
#         description="Tests extraction of topic/keyword filter"
#     ),
#     TestCase(
#         name="Topic filter - music",
#         query="jazz music history",
#         category=TestCategory.FILTER_EXTRACTION,
#         expected={
#             'category_has_media': True,
#         },
#         description="Tests music/media category detection"
#     ),
#     TestCase(
#         name="Topic filter - sports",
#         query="black athletes in baseball",
#         category=TestCategory.FILTER_EXTRACTION,
#         expected={
#             'has_filter': True,
#         },
#         description="Tests sports category filter"
#     ),

#     # =========================================================================
#     # MIXED COMPLEX QUERIES
#     # =========================================================================
#     TestCase(
#         name="Complex - person + location + typo",
#         query="Martin Luther King speches in Atlenta",
#         category=TestCategory.MIXED_COMPLEX,
#         expected={
#             'has_ngram': True,
#             'has_correction': True,
#             'has_location': True,
#         },
#         description="Tests combination: person bigram + location + spelling correction"
#     ),
#     TestCase(
#         name="Complex - temporal + location + topic",
#         query="first black owned business in Chicago",
#         category=TestCategory.MIXED_COMPLEX,
#         expected={
#             'has_sort': True,
#             'has_location': True,
#             'category_has_business': True,
#         },
#         description="Tests combination: temporal + location + business category"
#     ),
#     TestCase(
#         name="Complex - multiple bigrams",
#         query="Rosa Parks and Martin Luther King",
#         category=TestCategory.MIXED_COMPLEX,
#         expected={
#             'ngram_count_min': 2,
#         },
#         description="Tests detection of multiple bigrams in same query"
#     ),
#     TestCase(
#         name="Complex - typo in bigram",
#         query="Marin Luther King birthday",
#         category=TestCategory.MIXED_COMPLEX,
#         expected={
#             'has_correction': True,
#         },
#         description="Tests correction when first word of bigram has typo"
#     ),
#     TestCase(
#         name="Complex - all elements",
#         query="frist black female mayor of Atlenta Georgia",
#         category=TestCategory.MIXED_COMPLEX,
#         expected={
#             'has_correction': True,
#             'has_location': True,
#             'has_sort': True,
#         },
#         description="Tests: temporal + typos + location (city + state)"
#     ),

#     # =========================================================================
#     # EDGE CASES
#     # =========================================================================
#     TestCase(
#         name="Empty query",
#         query="",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'valid_count': 0,
#         },
#         description="Tests handling of empty query"
#     ),
#     TestCase(
#         name="Single word query",
#         query="jazz",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'term_count': 1,
#         },
#         description="Tests single word query handling"
#     ),
#     TestCase(
#         name="All stopwords",
#         query="the and of in to",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'all_stopwords': True,
#         },
#         description="Tests query with only stopwords"
#     ),
#     TestCase(
#         name="Very long query",
#         query="the history and cultural significance of jazz music in the African American community during the Harlem Renaissance period in New York City",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'term_count_min': 10,
#         },
#         description="Tests handling of very long query"
#     ),
#     TestCase(
#         name="Numbers in query",
#         query="events in 1963 civil rights",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'valid_count_min': 1,
#         },
#         description="Tests handling of numbers in query"
#     ),
#     TestCase(
#         name="Mixed case",
#         query="MARTIN luther KING JR",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'has_ngram': True,
#         },
#         description="Tests case-insensitive handling"
#     ),
#     TestCase(
#         name="Query with punctuation",
#         query="Who was Martin Luther King?",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'term_count_min': 1,
#         },
#         description="Tests handling of punctuation"
#     ),
#     TestCase(
#         name="Repeated words",
#         query="the the the music",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'valid_count_min': 1,
#         },
#         description="Tests handling of repeated words"
#     ),
#     TestCase(
#         name="Unicode characters",
#         query="café culture in Harlem",
#         category=TestCategory.EDGE_CASES,
#         expected={
#             'has_location': True,
#         },
#         description="Tests handling of unicode characters"
#     ),

#     # =========================================================================
#     # INTENT DETECTION
#     # =========================================================================
#     TestCase(
#         name="Intent - question",
#         query="who was the first black president",
#         category=TestCategory.INTENT_DETECTION,
#         expected={
#             'has_sort': True,
#         },
#         description="Tests question intent with temporal element"
#     ),
#     TestCase(
#         name="Intent - search/find",
#         query="find musicians from Atlanta",
#         category=TestCategory.INTENT_DETECTION,
#         expected={
#             'has_location': True,
#         },
#         description="Tests search intent with location"
#     ),
#     TestCase(
#         name="Intent - list/show",
#         query="list all HBCUs in Georgia",
#         category=TestCategory.INTENT_DETECTION,
#         expected={
#             'has_location': True,
#         },
#         description="Tests list intent"
#     ),

#     # =========================================================================
#     # ADJACENT TYPOS (Two unknown words next to each other)
#     # =========================================================================
#     TestCase(
#         name="Adjacent typos - two misspelled words",
#         query="Marin Luthr King speeches",
#         category=TestCategory.ADJACENT_TYPOS,
#         expected={
#             'correction_count_min': 2,
#         },
#         description="Tests correction of two adjacent misspelled words"
#     ),
#     TestCase(
#         name="Adjacent typos in location",
#         query="musicians from New Yrok City",
#         category=TestCategory.ADJACENT_TYPOS,
#         expected={
#             'has_correction': True,
#         },
#         description="Tests typo in multi-word location"
#     ),

#     # =========================================================================
#     # SONG TITLES AND MEDIA
#     # =========================================================================
#     TestCase(
#         name="Song title detection",
#         query="Strange Fruit by Billie Holiday",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'category_has_media': True,
#         },
#         description="Tests detection of song title"
#     ),
#     TestCase(
#         name="Album detection",
#         query="Kind of Blue Miles Davis",
#         category=TestCategory.BIGRAM_DETECTION,
#         expected={
#             'category_has_media': True,
#         },
#         description="Tests detection of album title"
#     ),
# ]


# # =============================================================================
# # TEST RUNNER
# # =============================================================================

# class TestResult:
#     """Stores result of a single test"""
#     def __init__(self, test_case: TestCase):
#         self.test_case = test_case
#         self.passed = False
#         self.output = None
#         self.error = None
#         self.failures = []
#         self.execution_time_ms = 0


# def check_expectation(output: Dict[str, Any], key: str, expected_value: Any) -> Tuple[bool, str]:
#     """Check a single expectation against the output"""
    
#     if key == 'has_correction':
#         actual = len(output.get('corrections', [])) > 0
#         if actual != expected_value:
#             return False, f"Expected corrections: {expected_value}, got {len(output.get('corrections', []))} corrections"
#         return True, ""
    
#     elif key == 'corrected_contains':
#         corrected = output.get('corrected_query', '').lower()
#         if expected_value.lower() not in corrected:
#             return False, f"Expected corrected query to contain '{expected_value}', got '{corrected}'"
#         return True, ""
    
#     elif key == 'correction_count_min':
#         actual = len(output.get('corrections', []))
#         if actual < expected_value:
#             return False, f"Expected at least {expected_value} corrections, got {actual}"
#         return True, ""
    
#     elif key == 'has_ngram':
#         actual = len(output.get('ngrams', [])) > 0
#         if actual != expected_value:
#             return False, f"Expected ngrams: {expected_value}, got {len(output.get('ngrams', []))} ngrams"
#         return True, ""
    
#     elif key == 'ngram_contains':
#         ngrams = output.get('ngrams', [])
#         found = any(expected_value.lower() in ng.get('ngram', '').lower() for ng in ngrams)
#         if not found:
#             ngram_list = [ng.get('ngram', '') for ng in ngrams]
#             return False, f"Expected ngram containing '{expected_value}', found: {ngram_list}"
#         return True, ""
    
#     elif key == 'ngram_type':
#         ngrams = output.get('ngrams', [])
#         found = any(ng.get('type') == expected_value for ng in ngrams)
#         if not found:
#             types = [ng.get('type') for ng in ngrams]
#             return False, f"Expected ngram type '{expected_value}', found types: {types}"
#         return True, ""
    
#     elif key == 'ngram_count_min':
#         actual = len(output.get('ngrams', []))
#         if actual < expected_value:
#             return False, f"Expected at least {expected_value} ngrams, got {actual}"
#         return True, ""
    
#     elif key == 'has_location':
#         actual = len(output.get('locations', [])) > 0
#         if actual != expected_value:
#             return False, f"Expected locations: {expected_value}, got {len(output.get('locations', []))} locations"
#         return True, ""
    
#     elif key == 'location_field':
#         locations = output.get('locations', [])
#         found = any(loc.get('field') == expected_value for loc in locations)
#         if not found:
#             fields = [loc.get('field') for loc in locations]
#             return False, f"Expected location field '{expected_value}', found: {fields}"
#         return True, ""
    
#     elif key == 'location_count_min':
#         actual = len(output.get('locations', []))
#         if actual < expected_value:
#             return False, f"Expected at least {expected_value} locations, got {actual}"
#         return True, ""
    
#     elif key == 'has_sort':
#         actual = output.get('sort') is not None
#         if actual != expected_value:
#             return False, f"Expected sort: {expected_value}, got sort={output.get('sort')}"
#         return True, ""
    
#     elif key == 'sort_order':
#         sort_info = output.get('sort', {})
#         actual = sort_info.get('order') if sort_info else None
#         if actual != expected_value:
#             return False, f"Expected sort order '{expected_value}', got '{actual}'"
#         return True, ""
    
#     elif key == 'has_filter':
#         actual = len(output.get('filters', [])) > 0
#         if actual != expected_value:
#             return False, f"Expected filters: {expected_value}, got {len(output.get('filters', []))} filters"
#         return True, ""
    
#     elif key == 'valid_count':
#         actual = output.get('valid_count', 0)
#         if actual != expected_value:
#             return False, f"Expected valid_count={expected_value}, got {actual}"
#         return True, ""
    
#     elif key == 'valid_count_min':
#         actual = output.get('valid_count', 0)
#         if actual < expected_value:
#             return False, f"Expected valid_count >= {expected_value}, got {actual}"
#         return True, ""
    
#     elif key == 'term_count':
#         actual = len(output.get('terms', []))
#         if actual != expected_value:
#             return False, f"Expected {expected_value} terms, got {actual}"
#         return True, ""
    
#     elif key == 'term_count_min':
#         actual = len(output.get('terms', []))
#         if actual < expected_value:
#             return False, f"Expected at least {expected_value} terms, got {actual}"
#         return True, ""
    
#     elif key == 'all_stopwords':
#         terms = output.get('terms', [])
#         all_stop = all(t.get('category') == 'stopword' or t.get('status') == 'valid' for t in terms)
#         if all_stop != expected_value:
#             return False, f"Expected all stopwords: {expected_value}"
#         return True, ""
    
#     elif key.startswith('category_has_'):
#         category_key = key.replace('category_has_', 'has_')
#         summary = output.get('category_summary', {})
#         actual = summary.get(category_key, False)
#         if actual != expected_value:
#             return False, f"Expected category_summary['{category_key}']={expected_value}, got {actual}"
#         return True, ""
    
#     return True, ""  # Unknown key, pass by default


# def run_single_test(test_case: TestCase, process_func, verbose: bool = False) -> TestResult:
#     """Run a single test case"""
#     result = TestResult(test_case)
    
#     start_time = time.perf_counter()
    
#     try:
#         # Run the query through the processor
#         output = process_func(test_case.query, verbose=False)
#         result.output = output
        
#         # Check all expectations
#         all_passed = True
#         for key, expected_value in test_case.expected.items():
#             passed, message = check_expectation(output, key, expected_value)
#             if not passed:
#                 all_passed = False
#                 result.failures.append(message)
        
#         result.passed = all_passed
        
#     except Exception as e:
#         result.error = str(e)
#         result.passed = False
#         if verbose:
#             traceback.print_exc()
    
#     result.execution_time_ms = (time.perf_counter() - start_time) * 1000
    
#     return result


# def run_all_tests(process_func, verbose: bool = True) -> Dict[str, Any]:
#     """Run all test cases and return summary"""
    
#     print("\n" + "=" * 80)
#     print("🧪 WORD DISCOVERY COMPREHENSIVE TEST SUITE")
#     print("=" * 80)
#     print(f"Mode: 100% REDIS (RAM cache bypassed)")
#     print(f"Total test cases: {len(TEST_CASES)}")
#     print("=" * 80)
    
#     results = []
#     passed_count = 0
#     failed_count = 0
#     error_count = 0
    
#     # Group tests by category
#     by_category = {}
#     for tc in TEST_CASES:
#         cat = tc.category.value
#         if cat not in by_category:
#             by_category[cat] = []
#         by_category[cat].append(tc)
    
#     # Run tests by category
#     for category_name, test_cases in by_category.items():
#         print(f"\n{'─' * 80}")
#         print(f"📂 {category_name} ({len(test_cases)} tests)")
#         print('─' * 80)
        
#         for tc in test_cases:
#             result = run_single_test(tc, process_func, verbose=verbose)
#             results.append(result)
            
#             if result.error:
#                 error_count += 1
#                 status = "❌ ERROR"
#             elif result.passed:
#                 passed_count += 1
#                 status = "✅ PASS"
#             else:
#                 failed_count += 1
#                 status = "❌ FAIL"
            
#             print(f"  {status} | {tc.name}")
#             print(f"         Query: '{tc.query}'")
            
#             if result.error:
#                 print(f"         Error: {result.error}")
#             elif not result.passed:
#                 for failure in result.failures:
#                     print(f"         ⚠️  {failure}")
            
#             if verbose and result.output:
#                 print(f"         Time: {result.execution_time_ms:.2f}ms")
#                 if result.output.get('corrections'):
#                     print(f"         Corrections: {result.output['corrections']}")
#                 if result.output.get('ngrams'):
#                     ngrams = [ng.get('ngram') for ng in result.output['ngrams']]
#                     print(f"         Ngrams: {ngrams}")
#                 if result.output.get('locations'):
#                     locs = [loc.get('term') for loc in result.output['locations']]
#                     print(f"         Locations: {locs}")
#                 if result.output.get('sort'):
#                     print(f"         Sort: {result.output['sort']}")
            
#             if STOP_ON_FAILURE and not result.passed:
#                 print("\n⛔ STOPPING - STOP_ON_FAILURE is enabled")
#                 break
        
#         if STOP_ON_FAILURE and failed_count > 0:
#             break
    
#     # Print summary
#     total = len(results)
#     print("\n" + "=" * 80)
#     print("📊 TEST SUMMARY")
#     print("=" * 80)
#     print(f"  Total:  {total}")
#     print(f"  ✅ Passed: {passed_count} ({100*passed_count/total:.1f}%)")
#     print(f"  ❌ Failed: {failed_count} ({100*failed_count/total:.1f}%)")
#     print(f"  💥 Errors: {error_count} ({100*error_count/total:.1f}%)")
#     print("=" * 80)
    
#     # Print failed tests summary
#     if failed_count > 0 or error_count > 0:
#         print("\n❌ FAILED/ERROR TESTS:")
#         for r in results:
#             if not r.passed:
#                 print(f"  - {r.test_case.name}: {r.test_case.query}")
#                 if r.error:
#                     print(f"    Error: {r.error}")
#                 for f in r.failures:
#                     print(f"    {f}")
    
#     return {
#         'total': total,
#         'passed': passed_count,
#         'failed': failed_count,
#         'errors': error_count,
#         'results': results
#     }


# # =============================================================================
# # MAIN ENTRY POINT
# # =============================================================================

# def main():
#     """Main entry point for test script"""
    
#     print("\n" + "=" * 80)
#     print("🔧 INITIALIZING TEST ENVIRONMENT")
#     print("=" * 80)
    
#     # Create Redis-backed vocab cache for testing
#     redis_cache = RedisVocabCache()
    
#     # Try to import word_discovery
#     try:
#         import word_discovery
#         print("✅ word_discovery module imported")
        
#         # Monkey-patch the vocab_cache to use our Redis-backed version
#         word_discovery.vocab_cache = redis_cache
#         word_discovery.CACHE_AVAILABLE = True
        
#         # Also patch the _ensure_cache_loaded function
#         def mock_ensure_loaded():
#             return True
#         word_discovery._ensure_cache_loaded = mock_ensure_loaded
        
#         print("✅ Patched vocab_cache to use Redis-backed cache")
        
#         # Use the actual process function
#         process_func = word_discovery.process_query_optimized
        
#     except ImportError as e:
#         print(f"⚠️ Could not import word_discovery: {e}")
#         print("Creating standalone test function...")
        
#         # Create a minimal test function that just tests Redis
#         def process_func(query: str, verbose: bool = False) -> Dict[str, Any]:
#             """Minimal process function for testing Redis connectivity"""
#             words = query.split() if query else []
            
#             terms = []
#             corrections = []
#             ngrams = []
#             locations = []
            
#             for i, word in enumerate(words):
#                 word_lower = word.lower().strip()
                
#                 # Check if stopword
#                 if redis_cache.is_stopword(word_lower):
#                     terms.append({
#                         'word': word_lower,
#                         'status': 'valid',
#                         'pos': redis_cache.get_stopword_pos(word_lower),
#                         'category': 'stopword'
#                     })
#                     continue
                
#                 # Try Redis lookup
#                 metadata = redis_cache.get_term(word_lower)
                
#                 if metadata:
#                     category = metadata.get('category', '')
#                     terms.append({
#                         'word': word_lower,
#                         'status': 'valid',
#                         'pos': metadata.get('pos', 'unknown'),
#                         'category': category,
#                         'rank': metadata.get('rank', 0)
#                     })
                    
#                     # Check for location
#                     if 'city' in category.lower() or 'state' in category.lower():
#                         locations.append({
#                             'field': 'location_city' if 'city' in category.lower() else 'location_state',
#                             'values': [metadata.get('display', word_lower)],
#                             'term': word_lower
#                         })
#                 else:
#                     terms.append({
#                         'word': word_lower,
#                         'status': 'unknown',
#                         'pos': 'unknown',
#                         'category': ''
#                     })
            
#             # Check for bigrams
#             for i in range(len(words) - 1):
#                 bigram_meta = redis_cache.get_bigram(words[i].lower(), words[i+1].lower())
#                 if bigram_meta:
#                     ngrams.append({
#                         'type': 'bigram',
#                         'ngram': f"{words[i].lower()} {words[i+1].lower()}",
#                         'positions': [i+1, i+2],
#                         'metadata': bigram_meta
#                     })
            
#             # Check for sort
#             sort_instruction = None
#             temporal_terms = {
#                 'first': {'field': 'time_period_start', 'order': 'asc'},
#                 'oldest': {'field': 'time_period_start', 'order': 'asc'},
#                 'earliest': {'field': 'time_period_start', 'order': 'asc'},
#                 'last': {'field': 'time_period_start', 'order': 'desc'},
#                 'latest': {'field': 'published_date', 'order': 'desc'},
#                 'recent': {'field': 'published_date', 'order': 'desc'},
#                 'newest': {'field': 'published_date', 'order': 'desc'},
#             }
#             for word in words:
#                 if word.lower() in temporal_terms:
#                     sort_instruction = temporal_terms[word.lower()]
#                     sort_instruction['term'] = word.lower()
#                     break
            
#             return {
#                 'query': query,
#                 'corrected_query': query,
#                 'terms': terms,
#                 'ngrams': ngrams,
#                 'locations': locations,
#                 'filters': [],
#                 'sort': sort_instruction,
#                 'corrections': corrections,
#                 'valid_count': sum(1 for t in terms if t['status'] == 'valid'),
#                 'unknown_count': sum(1 for t in terms if t['status'] == 'unknown'),
#                 'category_summary': {
#                     'has_person': False,
#                     'has_location': len(locations) > 0,
#                     'has_topic': False,
#                     'has_song_title': False,
#                     'has_media': False,
#                     'has_food': False,
#                     'has_business': False,
#                     'has_culture': False,
#                     'has_entity': False
#                 }
#             }
    
#     # Check Redis connectivity
#     print("\n🔍 Testing Redis connectivity...")
#     test_result = redis_cache.get_term("jazz")
#     if test_result:
#         print(f"✅ Redis connected - test lookup for 'jazz': {test_result}")
#     else:
#         print("⚠️ Redis lookup returned None for 'jazz' - Redis may not be connected or term not found")
    
#     # Run all tests
#     summary = run_all_tests(process_func, verbose=VERBOSE_OUTPUT)
    
#     # Exit with appropriate code
#     if summary['failed'] > 0 or summary['errors'] > 0:
#         sys.exit(1)
#     else:
#         sys.exit(0)


# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
"""
WORD DISCOVERY TEST v2 - FIXED PATCHING
=======================================

This version properly patches word_discovery.py to:
1. Force ALL lookups through Redis (not RAM cache)
2. Ensure spelling correction runs
3. Debug each pass to see what's happening

Run: python test_word_discovery_v2.py
"""

import sys
import os
import time
import json
from typing import Dict, Any, List, Optional, Tuple

# =============================================================================
# STEP 1: Import Redis functions FIRST
# =============================================================================

print("=" * 70)
print("🔧 INITIALIZING TEST ENVIRONMENT")
print("=" * 70)

REDIS_AVAILABLE = False
get_suggestions = None
get_fuzzy_matches = None

try:
    from searchapi import (
        get_suggestions,
        get_fuzzy_matches,
        damerau_levenshtein_distance,
    )
    REDIS_AVAILABLE = True
    print("✅ Redis functions imported from searchapi")
except ImportError as e:
    print(f"❌ Failed to import searchapi: {e}")
    sys.exit(1)

# =============================================================================
# STEP 2: Test Redis connectivity
# =============================================================================

print("\n🔍 Testing Redis connectivity...")

test_result = get_suggestions("jazz", limit=5, max_distance=0)
print(f"  get_suggestions('jazz') returned: {type(test_result)}")
print(f"  Keys: {test_result.keys() if isinstance(test_result, dict) else 'N/A'}")

suggestions = test_result.get('suggestions', [])
print(f"  Suggestions count: {len(suggestions)}")
if suggestions:
    print(f"  First suggestion: {suggestions[0]}")
    print("✅ Redis is working!")
else:
    print("⚠️ Redis returned no suggestions for 'jazz'")

# Test fuzzy matching
print("\n🔍 Testing fuzzy matching (typo correction)...")
fuzzy_result = get_suggestions("jaz", limit=5, max_distance=2)
fuzzy_suggestions = fuzzy_result.get('suggestions', [])
print(f"  get_suggestions('jaz', max_distance=2) returned {len(fuzzy_suggestions)} suggestions")
if fuzzy_suggestions:
    for s in fuzzy_suggestions[:3]:
        print(f"    - {s.get('term')} (category: {s.get('category')})")

# =============================================================================
# STEP 3: Create Redis-backed vocab cache
# =============================================================================

class RedisVocabCache:
    """
    Vocabulary cache that uses Redis for ALL lookups.
    """
    def __init__(self):
        self.loaded = True  # Pretend loaded so word_discovery uses us
        self._call_log = []
        
        # Stopwords with their POS
        self._stopwords = {
            'the': 'article', 'a': 'article', 'an': 'article',
            'in': 'preposition', 'on': 'preposition', 'at': 'preposition',
            'to': 'preposition', 'for': 'preposition', 'of': 'preposition',
            'with': 'preposition', 'by': 'preposition', 'from': 'preposition',
            'is': 'be', 'are': 'be', 'was': 'be', 'were': 'be', 'be': 'be',
            'been': 'be', 'being': 'be',
            'have': 'auxiliary', 'has': 'auxiliary', 'had': 'auxiliary',
            'do': 'auxiliary', 'does': 'auxiliary', 'did': 'auxiliary',
            'will': 'modal', 'would': 'modal', 'could': 'modal',
            'should': 'modal', 'may': 'modal', 'might': 'modal',
            'must': 'modal', 'can': 'modal',
            'and': 'conjunction', 'or': 'conjunction', 'but': 'conjunction',
            'if': 'conjunction', 'then': 'adverb', 'else': 'adverb',
            'i': 'pronoun', 'you': 'pronoun', 'he': 'pronoun', 'she': 'pronoun',
            'it': 'pronoun', 'we': 'pronoun', 'they': 'pronoun',
            'me': 'pronoun', 'him': 'pronoun', 'her': 'pronoun',
            'us': 'pronoun', 'them': 'pronoun',
            'my': 'determiner', 'your': 'determiner', 'his': 'determiner',
            'its': 'determiner', 'our': 'determiner', 'their': 'determiner',
            'this': 'determiner', 'that': 'determiner',
            'these': 'determiner', 'those': 'determiner',
            'what': 'wh_pronoun', 'which': 'wh_pronoun', 'who': 'wh_pronoun',
            'whom': 'wh_pronoun', 'where': 'wh_pronoun', 'when': 'wh_pronoun',
            'why': 'wh_pronoun', 'how': 'wh_pronoun',
            'not': 'negation', 'no': 'negation',
            'about': 'preposition', 'into': 'preposition', 'through': 'preposition',
            'during': 'preposition', 'before': 'preposition', 'after': 'preposition',
            'above': 'preposition', 'below': 'preposition', 'between': 'preposition',
        }
    
    def get_term(self, word: str) -> Optional[Dict]:
        """Look up term via Redis - returns None to force correction pass"""
        self._call_log.append(('get_term', word))
        word_lower = word.lower().strip()
        
        # Use Redis to check if term exists
        result = get_suggestions(word_lower, limit=5, max_distance=0)
        suggestions = result.get('suggestions', [])
        
        for s in suggestions:
            if s.get('term', '').lower() == word_lower:
                return {
                    'word': s.get('term'),
                    'pos': s.get('pos', 'unknown'),
                    'rank': s.get('rank', 0),
                    'category': s.get('category', ''),
                    'display': s.get('display', s.get('term')),
                    'filter_field': s.get('filter_field'),
                    'filter_value': s.get('filter_value'),
                }
        
        return None  # Not found - will be marked as unknown
    
    def get_bigram(self, word1: str, word2: str) -> Optional[Dict]:
        """Look up bigram via Redis"""
        self._call_log.append(('get_bigram', word1, word2))
        
        bigram = f"{word1.lower()} {word2.lower()}"
        result = get_suggestions(bigram, limit=5, max_distance=0)
        suggestions = result.get('suggestions', [])
        
        for s in suggestions:
            if s.get('term', '').lower() == bigram:
                return {
                    'bigram': s.get('term'),
                    'pos': s.get('pos', 'unknown'),
                    'rank': s.get('rank', 0),
                    'category': s.get('category', ''),
                    'display': s.get('display', s.get('term')),
                }
        
        return None
    
    def get_trigram(self, word1: str, word2: str, word3: str) -> Optional[Dict]:
        """Look up trigram via Redis"""
        self._call_log.append(('get_trigram', word1, word2, word3))
        
        trigram = f"{word1.lower()} {word2.lower()} {word3.lower()}"
        result = get_suggestions(trigram, limit=5, max_distance=0)
        suggestions = result.get('suggestions', [])
        
        for s in suggestions:
            if s.get('term', '').lower() == trigram:
                return {
                    'trigram': s.get('term'),
                    'pos': s.get('pos', 'unknown'),
                    'rank': s.get('rank', 0),
                    'category': s.get('category', ''),
                    'display': s.get('display', s.get('term')),
                }
        
        return None
    
    def is_stopword(self, word: str) -> bool:
        """Check if word is a stopword"""
        return word.lower().strip() in self._stopwords
    
    def get_stopword_pos(self, word: str) -> str:
        """Get POS for stopword"""
        return self._stopwords.get(word.lower().strip(), 'unknown')
    
    def status(self) -> Dict[str, Any]:
        return {
            'loaded': True,
            'mode': 'REDIS_DIRECT',
            'calls': len(self._call_log)
        }
    
    # For bigram iteration in correct_pair_as_bigram
    @property
    def bigrams(self) -> Dict:
        """Return empty dict - we don't iterate bigrams"""
        return {}


# =============================================================================
# STEP 4: Import and patch word_discovery
# =============================================================================

print("\n🔧 Importing and patching word_discovery...")

try:
    import word_discovery
    print(f"✅ word_discovery imported")
    print(f"   CACHE_AVAILABLE: {word_discovery.CACHE_AVAILABLE}")
    print(f"   REDIS_AVAILABLE: {word_discovery.REDIS_AVAILABLE}")
except ImportError as e:
    print(f"❌ Failed to import word_discovery: {e}")
    sys.exit(1)

# Create our Redis-backed cache
redis_cache = RedisVocabCache()

# Patch the module
word_discovery.vocab_cache = redis_cache
word_discovery.CACHE_AVAILABLE = True

# Patch _ensure_cache_loaded to always return True
original_ensure_loaded = word_discovery._ensure_cache_loaded
def patched_ensure_loaded():
    return True
word_discovery._ensure_cache_loaded = patched_ensure_loaded

print("✅ Patched vocab_cache with Redis-backed version")

# =============================================================================
# STEP 5: Debug a single query to see what's happening
# =============================================================================

def debug_query(query: str) -> Dict[str, Any]:
    """Run a query with detailed debugging"""
    print("\n" + "=" * 70)
    print(f"🔬 DEBUGGING QUERY: '{query}'")
    print("=" * 70)
    
    words = query.split()
    
    # PASS 1: Validate words
    print("\n📍 PASS 1: Word Validation")
    print("-" * 50)
    
    results = word_discovery.validate_words_ram(words, verbose=False)
    
    for r in results:
        status = r['status']
        word = r['word']
        pos = r.get('pos', 'unknown')
        category = r.get('category', '')
        
        if status == 'valid':
            print(f"  ✅ '{word}' → VALID (pos={pos}, category={category})")
        elif status == 'unknown':
            print(f"  ❓ '{word}' → UNKNOWN (will try to correct)")
        else:
            print(f"  ⚠️ '{word}' → {status}")
    
    unknown_count = sum(1 for r in results if r['status'] == 'unknown')
    print(f"\n  Unknown words: {unknown_count}")
    
    # PASS 2: POS Prediction
    print("\n📍 PASS 2: POS Prediction")
    print("-" * 50)
    
    results = word_discovery.predict_pos_for_unknowns(results, verbose=False)
    
    for r in results:
        if r['status'] == 'unknown':
            predicted = r.get('predicted_pos', 'none')
            conf = r.get('pos_confidence', 0)
            print(f"  '{r['word']}' → predicted POS: {predicted} (confidence: {conf:.0%})")
    
    # PASS 3: Spelling Correction
    print("\n📍 PASS 3: Spelling Correction (Redis)")
    print("-" * 50)
    
    # Test Redis correction directly for unknown words
    for r in results:
        if r['status'] == 'unknown':
            word = r['word']
            print(f"\n  Correcting '{word}'...")
            
            # Get suggestions from Redis
            suggestion_result = get_suggestions(word, limit=10, max_distance=2)
            suggestions = suggestion_result.get('suggestions', [])
            
            print(f"    Redis returned {len(suggestions)} suggestions:")
            for s in suggestions[:5]:
                term = s.get('term', '')
                category = s.get('category', '')
                print(f"      - '{term}' (category: {category})")
    
    results = word_discovery.correct_unknown_words(results, verbose=False)
    
    corrections = [r for r in results if r['status'] == 'corrected']
    print(f"\n  Corrections made: {len(corrections)}")
    for r in corrections:
        print(f"    '{r['word']}' → '{r.get('corrected', '?')}'")
    
    # PASS 4: Ngram Detection
    print("\n📍 PASS 4: Ngram Detection (Redis)")
    print("-" * 50)
    
    # Build word list
    corrected_words = []
    for r in results:
        if r['status'] == 'corrected':
            corrected_words.append(r['corrected'].lower())
        else:
            corrected_words.append(r['word'].lower())
    
    print(f"  Words to check: {corrected_words}")
    
    # Check trigrams manually
    print("\n  Checking trigrams:")
    for i in range(len(corrected_words) - 2):
        trigram = f"{corrected_words[i]} {corrected_words[i+1]} {corrected_words[i+2]}"
        result = get_suggestions(trigram, limit=3, max_distance=0)
        suggestions = result.get('suggestions', [])
        if suggestions:
            print(f"    ✅ TRIGRAM FOUND: '{trigram}'")
        else:
            print(f"    ❌ '{trigram}' - not found")
    
    # Check bigrams manually
    print("\n  Checking bigrams:")
    for i in range(len(corrected_words) - 1):
        bigram = f"{corrected_words[i]} {corrected_words[i+1]}"
        result = get_suggestions(bigram, limit=3, max_distance=0)
        suggestions = result.get('suggestions', [])
        if suggestions:
            print(f"    ✅ BIGRAM FOUND: '{bigram}' → {suggestions[0].get('category', 'no category')}")
        else:
            print(f"    ❌ '{bigram}' - not found")
    
    results, ngrams = word_discovery.detect_ngrams(results, verbose=False)
    
    print(f"\n  Ngrams detected by word_discovery: {len(ngrams)}")
    for ng in ngrams:
        print(f"    {ng.get('type')}: '{ng.get('ngram')}' (category: {ng.get('category', 'none')})")
    
    # PASS 5: Filter extraction
    print("\n📍 PASS 5: Filter Extraction")
    print("-" * 50)
    
    filter_result = word_discovery.extract_filters(results, ngrams, verbose=False)
    
    print(f"  Filters: {filter_result.get('filters', [])}")
    print(f"  Locations: {filter_result.get('locations', [])}")
    print(f"  Sort: {filter_result.get('sort')}")
    
    # Full pipeline
    print("\n📍 FULL PIPELINE RESULT:")
    print("-" * 50)
    
    output = word_discovery.process_query_optimized(query, verbose=False)
    
    print(f"  Query: '{output.get('query')}'")
    print(f"  Corrected: '{output.get('corrected_query')}'")
    print(f"  Corrections: {output.get('corrections')}")
    print(f"  Ngrams: {[ng.get('ngram') for ng in output.get('ngrams', [])]}")
    print(f"  Locations: {output.get('locations')}")
    print(f"  Filters: {output.get('filters')}")
    print(f"  Sort: {output.get('sort')}")
    print(f"  Category Summary: {output.get('category_summary')}")
    
    return output


# =============================================================================
# STEP 6: Run debug on test queries
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🧪 RUNNING DEBUG TESTS")
    print("=" * 70)
    
    test_queries = [
        # Simple typo test
        "jaz music",
        
        # Bigram test
        "Martin Luther King",
        
        # Location test
        "musicians from Atlanta Georgia",
        
        # Typo + location
        "artists in Atlenta",
        
        # Complex
        "teh first black president",
    ]
    
    for query in test_queries:
        debug_query(query)
        print("\n" + "=" * 70)
    
    # =============================================================================
    # STEP 7: Summary of findings
    # =============================================================================
    
    print("\n" + "=" * 70)
    print("📊 DIAGNOSTIC SUMMARY")
    print("=" * 70)
    
    print("""
    Based on the debug output above, check:
    
    1. PASS 1 - Are words being marked as 'unknown' or 'valid'?
       - If ALL words are 'valid', correction will be skipped
       - This happens if Redis returns exact matches for typos
    
    2. PASS 3 - Is Redis returning fuzzy suggestions?
       - Check "Redis returned X suggestions" lines
       - If 0 suggestions, Redis fuzzy search may not be working
    
    3. PASS 4 - Are bigrams/trigrams being found?
       - Check the manual bigram/trigram checks
       - If not found, they may not exist in Redis
    
    4. Categories - Are categories being set correctly?
       - Check if Redis returns 'category' field
       - Location categories should be: city, us_city, state, us_state, etc.
       - Person categories should be: person, name
    """)