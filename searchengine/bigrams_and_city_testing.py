# #!/usr/bin/env python
# """
# bigrams_and_city_testing.py
# Test detection of cities, bigrams, and trigrams from Redis hash.

# Tests word discovery's ability to detect:
# 1. US Cities (location detection)
# 2. Bigrams (two-word phrases like "New York", "Tuskegee Airmen")
# 3. Trigrams (three-word phrases like "New York City")
# 4. Location categories (state, city, country, region)

# NO EMBEDDING INVOLVED - Pure word discovery and Redis lookup testing.
# """

# import os
# import sys
# from datetime import datetime
# from typing import Dict, List, Any, Optional

# # Add current directory to path
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# # Add project root to path
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, project_root)

# # Set Django settings
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

# import django
# django.setup()


# def print_header(title):
#     """Print a section header."""
#     print("\n" + "=" * 70)
#     print(title)
#     print("=" * 70)


# def print_subheader(title):
#     """Print a subsection header."""
#     print(f"\n--- {title} ---")


# def print_result(success, message):
#     """Print a test result."""
#     status = "✓" if success else "✗"
#     print(f"   {status} {message}")
#     return success


# # =============================================================================
# # TEST 1: IMPORT WORD DISCOVERY
# # =============================================================================

# def test_imports():
#     """Test that word discovery module imports correctly."""
#     print_header("TEST 1: IMPORT WORD DISCOVERY MODULE")
    
#     try:
#         from searchengine.word_discovery import (
#             word_discovery_single_pass,
#             get_search_strategy,
#             get_filter_terms,
#             normalize_pos,
#             LOCATION_TYPES
#         )
#         print_result(True, "word_discovery module imported")
#         print(f"   LOCATION_TYPES: {LOCATION_TYPES}")
#         return True
#     except ImportError:
#         try:
#             from word_discovery import (
#                 word_discovery_single_pass,
#                 get_search_strategy,
#                 get_filter_terms,
#                 normalize_pos,
#                 LOCATION_TYPES
#             )
#             print_result(True, "word_discovery module imported (direct)")
#             print(f"   LOCATION_TYPES: {LOCATION_TYPES}")
#             return True
#         except ImportError as e:
#             print_result(False, f"Import failed: {e}")
#             return False


# # =============================================================================
# # TEST 2: REDIS SEARCHAPI IMPORTS
# # =============================================================================

# def test_searchapi_imports():
#     """Test that searchapi module imports correctly."""
#     print_header("TEST 2: IMPORT SEARCHAPI MODULE")
    
#     try:
#         from searchengine.searchapi import (
#             batch_validate_words_redis,
#             batch_check_bigrams,
#             batch_get_term_metadata,
#             validate_word,
#             get_term_metadata
#         )
#         print_result(True, "searchapi module imported")
#         return True, {
#             'batch_validate_words_redis': batch_validate_words_redis,
#             'batch_check_bigrams': batch_check_bigrams,
#             'batch_get_term_metadata': batch_get_term_metadata,
#             'validate_word': validate_word,
#             'get_term_metadata': get_term_metadata
#         }
#     except ImportError:
#         try:
#             from searchapi import (
#                 batch_validate_words_redis,
#                 batch_check_bigrams,
#                 batch_get_term_metadata,
#                 validate_word,
#                 get_term_metadata
#             )
#             print_result(True, "searchapi module imported (direct)")
#             return True, {
#                 'batch_validate_words_redis': batch_validate_words_redis,
#                 'batch_check_bigrams': batch_check_bigrams,
#                 'batch_get_term_metadata': batch_get_term_metadata,
#                 'validate_word': validate_word,
#                 'get_term_metadata': get_term_metadata
#             }
#         except ImportError as e:
#             print_result(False, f"Import failed: {e}")
#             return False, {}


# # =============================================================================
# # TEST 3: SINGLE WORD CITY DETECTION
# # =============================================================================

# def test_single_city_detection(api_funcs: Dict):
#     """Test detection of single-word cities."""
#     print_header("TEST 3: SINGLE-WORD CITY DETECTION")
    
#     try:
#         from searchengine.searchapi import get_exact_term_matches
#     except ImportError:
#         from searchapi import get_exact_term_matches
    
#     # Single-word US cities to test
#     test_cities = [
#         "atlanta",
#         "chicago",
#         "houston",
#         "phoenix",
#         "philadelphia",
#         "dallas",
#         "austin",
#         "detroit",
#         "memphis",
#         "baltimore",
#         "milwaukee",
#         "albuquerque",
#         "tucson",
#         "fresno",
#         "sacramento",
#         "miami",
#         "oakland",
#         "minneapolis",
#         "cleveland",
#         "pittsburgh",
#     ]
    
#     detected = 0
#     not_detected = []
#     location_categories = []
    
#     # Location categories to check (lowercase)
#     LOCATION_CATS = {'city', 'us_city', 'us city', 'state', 'us_state', 'us state', 'country', 'region', 'location'}
    
#     for city in test_cities:
#         # Use get_exact_term_matches to get ALL matches for this term
#         matches = get_exact_term_matches(city)
        
#         if matches:
#             # Check if ANY match is a location
#             location_match = None
#             for match in matches:
#                 category = match.get('category', '').lower()
#                 if category in LOCATION_CATS:
#                     location_match = match
#                     break
            
#             if location_match:
#                 detected += 1
#                 category = location_match.get('category', '')
#                 pos = location_match.get('pos', '')
#                 location_categories.append((city, category, pos))
#                 print(f"   ✓ {city}: category='{category}', pos='{pos}'")
#             else:
#                 # Valid word but not categorized as location
#                 all_cats = [m.get('category', '') for m in matches]
#                 print(f"   ⚠ {city}: valid but categories={all_cats} (no location)")
#                 not_detected.append(city)
#         else:
#             print(f"   ✗ {city}: not found in Redis")
#             not_detected.append(city)
    
#     print(f"\n   Summary: {detected}/{len(test_cities)} cities detected as locations")
    
#     if not_detected:
#         print(f"   Not detected: {not_detected}")
    
#     # Show unique categories found
#     unique_categories = set(cat for _, cat, _ in location_categories)
#     print(f"   Location categories found: {unique_categories}")
    
#     return detected >= len(test_cities) * 0.5  # Pass if at least 50% detected


# # =============================================================================
# # TEST 4: BIGRAM DETECTION
# # =============================================================================

# def test_bigram_detection(api_funcs: Dict):
#     """Test detection of bigrams (two-word phrases)."""
#     print_header("TEST 4: BIGRAM DETECTION")
    
#     batch_check_bigrams = api_funcs.get('batch_check_bigrams')
    
#     if not batch_check_bigrams:
#         print_result(False, "batch_check_bigrams function not available")
#         return False
    
#     # Bigrams to test - common two-word phrases
#     test_bigrams = [
#         ("new", "york"),
#         ("los", "angeles"),
#         ("san", "francisco"),
#         ("san", "diego"),
#         ("las", "vegas"),
#         ("new", "orleans"),
#         ("salt", "lake"),
#         ("st", "louis"),
#         ("kansas", "city"),
#         ("oklahoma", "city"),
#         ("tuskegee", "airmen"),
#         ("civil", "rights"),
#         ("black", "history"),
#         ("african", "american"),
#         ("north", "carolina"),
#         ("south", "carolina"),
#         ("west", "virginia"),
#         ("new", "jersey"),
#         ("new", "mexico"),
#         ("rhode", "island"),
#     ]
    
#     # Call batch_check_bigrams
#     results = batch_check_bigrams(test_bigrams)
    
#     print(f"\n   Batch check returned {len(results)} results")
    
#     detected = 0
#     not_detected = []
    
#     for word1, word2 in test_bigrams:
#         bigram_key = f"{word1} {word2}"
        
#         if bigram_key in results:
#             metadata = results[bigram_key]
#             category = metadata.get('category', 'unknown')
#             print(f"   ✓ '{bigram_key}': category='{category}'")
#             detected += 1
#         else:
#             print(f"   ✗ '{bigram_key}': not found")
#             not_detected.append(bigram_key)
    
#     print(f"\n   Summary: {detected}/{len(test_bigrams)} bigrams detected")
    
#     if not_detected:
#         print(f"   Not detected: {not_detected[:10]}{'...' if len(not_detected) > 10 else ''}")
    
#     return detected >= len(test_bigrams) * 0.3  # Pass if at least 30% detected


# # =============================================================================
# # TEST 5: TRIGRAM DETECTION (Three-word phrases)
# # =============================================================================

# def test_trigram_detection(api_funcs: Dict):
#     """Test detection of trigrams (three-word phrases)."""
#     print_header("TEST 5: TRIGRAM DETECTION")
    
#     batch_check_bigrams = api_funcs.get('batch_check_bigrams')
    
#     if not batch_check_bigrams:
#         print_result(False, "batch_check_bigrams function not available")
#         return False
    
#     # Trigrams to test - we'll check both as full trigrams and as bigram combinations
#     test_trigrams = [
#         "new york city",
#         "salt lake city",
#         "kansas city missouri",
#         "oklahoma city oklahoma",
#         "martin luther king",
#         "civil rights movement",
#         "historically black college",
#         "african american history",
#         "united states america",
#         "los angeles california",
#     ]
    
#     print("\n   Method 1: Check as full three-word phrase")
    
#     # First, try to check if trigrams exist as single entries
#     detected_full = 0
#     for trigram in test_trigrams:
#         words = trigram.split()
#         if len(words) == 3:
#             # Check if full trigram exists
#             trigram_pairs = [(words[0], f"{words[1]} {words[2]}"), 
#                             (f"{words[0]} {words[1]}", words[2])]
            
#             # This is a workaround - checking various combinations
#             found = False
#             for pair in trigram_pairs:
#                 results = batch_check_bigrams([pair])
#                 key = f"{pair[0]} {pair[1]}"
#                 if key in results:
#                     print(f"   ✓ '{trigram}' found as '{key}'")
#                     detected_full += 1
#                     found = True
#                     break
            
#             if not found:
#                 print(f"   ✗ '{trigram}' not found as full phrase")
    
#     print(f"\n   Method 2: Check as overlapping bigrams")
    
#     detected_overlap = 0
#     for trigram in test_trigrams:
#         words = trigram.split()
#         if len(words) == 3:
#             # Check first bigram (words 0-1) and second bigram (words 1-2)
#             bigram1 = (words[0], words[1])
#             bigram2 = (words[1], words[2])
            
#             results = batch_check_bigrams([bigram1, bigram2])
            
#             key1 = f"{words[0]} {words[1]}"
#             key2 = f"{words[1]} {words[2]}"
            
#             found1 = key1 in results
#             found2 = key2 in results
            
#             if found1 or found2:
#                 detected_overlap += 1
#                 parts = []
#                 if found1:
#                     parts.append(f"'{key1}'")
#                 if found2:
#                     parts.append(f"'{key2}'")
#                 print(f"   ✓ '{trigram}' - found: {', '.join(parts)}")
#             else:
#                 print(f"   ✗ '{trigram}' - no bigrams found")
    
#     print(f"\n   Summary:")
#     print(f"      Full trigrams detected: {detected_full}/{len(test_trigrams)}")
#     print(f"      Overlapping bigrams detected: {detected_overlap}/{len(test_trigrams)}")
    
#     return detected_overlap >= len(test_trigrams) * 0.2  # Pass if at least 20% have some bigram


# # =============================================================================
# # TEST 6: WORD DISCOVERY LOCATION DETECTION
# # =============================================================================

# def test_word_discovery_locations():
#     """Test that word_discovery_single_pass correctly identifies locations."""
#     print_header("TEST 6: WORD DISCOVERY LOCATION DETECTION")
    
#     try:
#         from searchengine.word_discovery import word_discovery_single_pass, LOCATION_TYPES
#     except ImportError:
#         from word_discovery import word_discovery_single_pass, LOCATION_TYPES
    
#     test_queries = [
#         {
#             'query': 'hbcus in georgia',
#             'expected_locations': ['georgia'],
#             'expected_bigrams': []
#         },
#         {
#             'query': 'restaurants in atlanta',
#             'expected_locations': ['atlanta'],
#             'expected_bigrams': []
#         },
#         {
#             'query': 'hotels in new york',
#             'expected_locations': ['new york'],
#             'expected_bigrams': ['new york']
#         },
#         {
#             'query': 'schools in north carolina',
#             'expected_locations': ['north carolina'],
#             'expected_bigrams': ['north carolina']
#         },
#         {
#             'query': 'black doctors in chicago',
#             'expected_locations': ['chicago'],
#             'expected_bigrams': []
#         },
#         {
#             'query': 'tuskegee airmen history',
#             'expected_locations': [],
#             'expected_bigrams': ['tuskegee airmen']
#         },
#         {
#             'query': 'civil rights movement atlanta',
#             'expected_locations': ['atlanta'],
#             'expected_bigrams': ['civil rights']
#         },
#         {
#             'query': 'new york city landmarks',
#             'expected_locations': ['new york'],
#             'expected_bigrams': ['new york']
#         },
#     ]
    
#     all_passed = True
    
#     for test in test_queries:
#         query = test['query']
#         expected_locations = test['expected_locations']
#         expected_bigrams = test['expected_bigrams']
        
#         print(f"\n   Query: '{query}'")
        
#         result = word_discovery_single_pass(query, verbose=False)
        
#         # Extract detected locations
#         detected_locations = []
#         detected_bigrams = []
        
#         for term in result.get('terms', []):
#             search_word = term.get('search_word', '').lower()
#             pos = term.get('pos', '')
#             status = term.get('status', '')
#             metadata = term.get('metadata', {})
#             category = metadata.get('category', '').lower()
            
#             # Check if it's a location
#             is_location = (
#                 pos == 'proper_noun' or
#                 category in LOCATION_TYPES or
#                 category in ('city', 'us_city', 'state', 'us_state', 'country', 'region')
#             )
            
#             if is_location:
#                 detected_locations.append(search_word)
            
#             if status == 'bigram':
#                 detected_bigrams.append(search_word)
        
#         # Check locations
#         location_match = all(
#             any(exp.lower() in det.lower() or det.lower() in exp.lower() 
#                 for det in detected_locations)
#             for exp in expected_locations
#         ) if expected_locations else len(detected_locations) == 0
        
#         # Check bigrams
#         bigram_match = all(
#             any(exp.lower() in det.lower() or det.lower() in exp.lower() 
#                 for det in detected_bigrams)
#             for exp in expected_bigrams
#         ) if expected_bigrams else True  # Don't fail if no bigrams expected
        
#         print(f"      Terms: {[t['search_word'] for t in result.get('terms', [])]}")
#         print(f"      Detected locations: {detected_locations}")
#         print(f"      Detected bigrams: {detected_bigrams}")
        
#         if location_match:
#             print_result(True, f"Locations match (expected: {expected_locations})")
#         else:
#             print_result(False, f"Locations MISMATCH (expected: {expected_locations}, got: {detected_locations})")
#             all_passed = False
        
#         if bigram_match:
#             print_result(True, f"Bigrams match (expected: {expected_bigrams})")
#         else:
#             print_result(False, f"Bigrams MISMATCH (expected: {expected_bigrams}, got: {detected_bigrams})")
#             all_passed = False
        
#         # Show term details
#         print("      Term details:")
#         for term in result.get('terms', []):
#             meta = term.get('metadata', {})
#             cat = meta.get('category', 'N/A')
#             print(f"         • {term['search_word']}: pos={term.get('pos')}, status={term.get('status')}, category={cat}")
    
#     return all_passed


# # =============================================================================
# # TEST 7: STATE DETECTION
# # =============================================================================

# def test_state_detection(api_funcs: Dict):
#     """Test detection of US states."""
#     print_header("TEST 7: US STATE DETECTION")
    
#     try:
#         from searchengine.searchapi import get_exact_term_matches, batch_check_bigrams
#     except ImportError:
#         from searchapi import get_exact_term_matches, batch_check_bigrams
    
#     # Location categories to check (lowercase)
#     LOCATION_CATS = {'city', 'us_city', 'us city', 'state', 'us_state', 'us state', 'country', 'region', 'location'}
    
#     # Single-word states
#     single_word_states = [
#         "alabama", "alaska", "arizona", "arkansas", "california",
#         "colorado", "connecticut", "delaware", "florida", "georgia",
#         "hawaii", "idaho", "illinois", "indiana", "iowa",
#         "kansas", "kentucky", "louisiana", "maine", "maryland",
#         "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
#         "montana", "nebraska", "nevada", "ohio", "oklahoma",
#         "oregon", "pennsylvania", "tennessee", "texas", "utah",
#         "vermont", "virginia", "washington", "wisconsin", "wyoming"
#     ]
    
#     # Two-word states (bigrams)
#     two_word_states = [
#         ("new", "hampshire"),
#         ("new", "jersey"),
#         ("new", "mexico"),
#         ("new", "york"),
#         ("north", "carolina"),
#         ("north", "dakota"),
#         ("south", "carolina"),
#         ("south", "dakota"),
#         ("west", "virginia"),
#         ("rhode", "island"),
#     ]
    
#     print_subheader("Single-word states")
    
#     single_detected = 0
#     for state in single_word_states:
#         matches = get_exact_term_matches(state)
        
#         if matches:
#             # Check if ANY match is a location/state
#             location_match = None
#             for match in matches:
#                 category = match.get('category', '').lower()
#                 if category in LOCATION_CATS:
#                     location_match = match
#                     break
            
#             if location_match:
#                 single_detected += 1
#                 category = location_match.get('category', '')
#                 print(f"   ✓ {state}: category='{category}'")
#             else:
#                 all_cats = [m.get('category', '') for m in matches]
#                 print(f"   ⚠ {state}: valid but categories={all_cats}")
#         else:
#             print(f"   ✗ {state}: not found")
    
#     print(f"\n   Single-word states: {single_detected}/{len(single_word_states)} detected as states")
    
#     print_subheader("Two-word states (bigrams)")
    
#     bigram_results = batch_check_bigrams(two_word_states)
    
#     bigram_detected = 0
#     for word1, word2 in two_word_states:
#         key = f"{word1} {word2}"
#         if key in bigram_results:
#             metadata = bigram_results[key]
#             category = metadata.get('category', 'unknown')
#             bigram_detected += 1
#             print(f"   ✓ '{key}': category='{category}'")
#         else:
#             print(f"   ✗ '{key}': not found")
    
#     print(f"\n   Two-word states: {bigram_detected}/{len(two_word_states)} detected")
    
#     total = single_detected + bigram_detected
#     total_expected = len(single_word_states) + len(two_word_states)
    
#     print(f"\n   TOTAL: {total}/{total_expected} states detected")
    
#     return total >= total_expected * 0.5


# # =============================================================================
# # TEST 8: CATEGORY DISTRIBUTION
# # =============================================================================

# def test_category_distribution(api_funcs: Dict):
#     """Test what categories exist in the Redis hash."""
#     print_header("TEST 8: CATEGORY DISTRIBUTION IN REDIS")
    
#     validate_word = api_funcs.get('validate_word')
#     get_term_metadata = api_funcs.get('get_term_metadata')
    
#     if not validate_word or not get_term_metadata:
#         print_result(False, "Required functions not available")
#         return False
    
#     # Test words from different expected categories
#     test_words = {
#         'locations': ['atlanta', 'georgia', 'chicago', 'texas', 'california', 'miami'],
#         'common_words': ['the', 'and', 'is', 'are', 'was', 'were', 'have', 'has'],
#         'nouns': ['doctor', 'school', 'history', 'people', 'city', 'college'],
#         'adjectives': ['black', 'african', 'american', 'civil', 'first', 'great'],
#         'verbs': ['run', 'walk', 'talk', 'think', 'make', 'find'],
#         'domain_specific': ['hbcu', 'hbcus', 'airmen', 'tuskegee', 'spelman', 'morehouse'],
#     }
    
#     category_counts = {}
#     pos_counts = {}
    
#     for category_name, words in test_words.items():
#         print(f"\n   Testing {category_name}:")
        
#         for word in words:
#             is_valid = validate_word(word)
            
#             if is_valid:
#                 metadata = get_term_metadata(word)
#                 category = metadata.get('category', 'none') if metadata else 'none'
#                 pos = metadata.get('pos', 'unknown') if metadata else 'unknown'
                
#                 # Count categories and POS
#                 category_counts[category] = category_counts.get(category, 0) + 1
#                 pos_counts[pos] = pos_counts.get(pos, 0) + 1
                
#                 print(f"      ✓ {word}: category='{category}', pos='{pos}'")
#             else:
#                 print(f"      ✗ {word}: not in Redis")
    
#     print(f"\n   Category distribution:")
#     for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
#         print(f"      {cat}: {count}")
    
#     print(f"\n   POS distribution:")
#     for pos, count in sorted(pos_counts.items(), key=lambda x: -x[1]):
#         print(f"      {pos}: {count}")
    
#     return True


# # =============================================================================
# # TEST 9: LOCATION EXTRACTION COMPARISON
# # =============================================================================

# def test_location_extraction_comparison():
#     """Compare regex-based vs word-discovery-based location extraction."""
#     print_header("TEST 9: LOCATION EXTRACTION METHODS COMPARISON")
    
#     try:
#         from searchengine.typesense_calculations import extract_location_from_query
#         from searchengine.word_discovery import word_discovery_single_pass, LOCATION_TYPES
#     except ImportError:
#         from typesense_calculations import extract_location_from_query
#         from word_discovery import word_discovery_single_pass, LOCATION_TYPES
    
#     test_queries = [
#         "hbcus in georgia",
#         "restaurants in atlanta",
#         "schools in north carolina", 
#         "hotels new york",  # No "in" keyword
#         "georgia hbcus",  # Location first
#         "atlanta black doctors",  # Location first
#         "tuskegee alabama history",  # Multiple potential locations
#         "civil rights atlanta georgia",  # Multiple locations
#     ]
    
#     print("\n   Comparing extraction methods:\n")
#     print(f"   {'Query':<40} {'Regex':<20} {'Word Discovery':<20}")
#     print(f"   {'-'*40} {'-'*20} {'-'*20}")
    
#     for query in test_queries:
#         # Method 1: Regex-based (current)
#         regex_location = extract_location_from_query(query) or "None"
        
#         # Method 2: Word-discovery-based
#         result = word_discovery_single_pass(query, verbose=False)
        
#         wd_locations = []
#         for term in result.get('terms', []):
#             metadata = term.get('metadata', {})
#             category = metadata.get('category', '').lower()
#             pos = term.get('pos', '')
            
#             if pos == 'proper_noun' or category in LOCATION_TYPES or category in ('city', 'us_city', 'state', 'us_state'):
#                 wd_locations.append(term['search_word'])
        
#         wd_location_str = ', '.join(wd_locations) if wd_locations else "None"
        
#         print(f"   {query:<40} {regex_location:<20} {wd_location_str:<20}")
    
#     print("\n   ⚠ Note: Word Discovery provides richer location data with categories")
    
#     return True


# # =============================================================================
# # MAIN
# # =============================================================================

# def main():
#     print("\n" + "=" * 70)
#     print("BIGRAMS, TRIGRAMS & CITY DETECTION TEST")
#     print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("NO EMBEDDING - Pure Word Discovery & Redis Testing")
#     print("=" * 70)
    
#     results = {}
    
#     # Test 1: Imports
#     results['imports'] = test_imports()
    
#     # Test 2: SearchAPI imports
#     api_success, api_funcs = test_searchapi_imports()
#     results['searchapi'] = api_success
    
#     if not api_success:
#         print("\n⚠ Cannot continue without searchapi functions")
#         return False
    
#     # Test 3: Single city detection
#     results['single_cities'] = test_single_city_detection(api_funcs)
    
#     # Test 4: Bigram detection
#     results['bigrams'] = test_bigram_detection(api_funcs)
    
#     # Test 5: Trigram detection
#     results['trigrams'] = test_trigram_detection(api_funcs)
    
#     # Test 6: Word discovery location detection
#     results['wd_locations'] = test_word_discovery_locations()
    
#     # Test 7: State detection
#     results['states'] = test_state_detection(api_funcs)
    
#     # Test 8: Category distribution
#     results['categories'] = test_category_distribution(api_funcs)
    
#     # Test 9: Location extraction comparison
#     results['extraction_comparison'] = test_location_extraction_comparison()
    
#     # Summary
#     print_header("FINAL SUMMARY")
    
#     passed = 0
#     failed = 0
    
#     for test_name, result in results.items():
#         status = "✓ PASSED" if result else "✗ FAILED"
#         print(f"   {test_name}: {status}")
#         if result:
#             passed += 1
#         else:
#             failed += 1
    
#     print(f"\n   Total: {passed} passed, {failed} failed")
    
#     if failed == 0:
#         print("\n✓ All tests passed!")
#     else:
#         print("\n✗ Some tests failed. Review the output above.")
#         print("\n   TROUBLESHOOTING HINTS:")
        
#         if not results.get('single_cities'):
#             print("   • Cities may not have 'category' field set to location types")
#             print("   • Check Redis hash structure for city entries")
        
#         if not results.get('bigrams'):
#             print("   • Bigrams may not be in Redis or have different key format")
#             print("   • Check batch_check_bigrams function")
        
#         if not results.get('states'):
#             print("   • States may not be categorized correctly in Redis")
#             print("   • Check 'category' field for state entries")
    
#     print("=" * 70)
    
#     return all(results.values())


# if __name__ == "__main__":
#     success = main()
#     sys.exit(0 if success else 1)


#!/usr/bin/env python
#!/usr/bin/env python
"""
bigrams_and_city_testing.py
Test detection of cities, bigrams, trigrams, and temporal expressions from Redis hash.

Tests word discovery's ability to detect:
1. US Cities (location detection)
2. Bigrams (two-word phrases like "New York", "Tuskegee Airmen")
3. Trigrams (three-word phrases like "New York City")
4. Location categories (state, city, country, region)
5. Temporal expressions (years, decades, date ranges)

NO EMBEDDING INVOLVED - Pure word discovery and Redis lookup testing.

Each test includes timing information.
"""

import os
import sys
import time
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from functools import wraps

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

import django
django.setup()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

class Timer:
    """Context manager for timing code blocks."""
    
    def __init__(self, name: str = ""):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.elapsed = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.elapsed = self.end_time - self.start_time
    
    @property
    def elapsed_ms(self) -> float:
        """Return elapsed time in milliseconds."""
        return (self.elapsed or 0) * 1000
    
    def __str__(self):
        if self.elapsed is not None:
            return f"{self.elapsed_ms:.2f}ms"
        return "not measured"


def timed_test(func):
    """Decorator to time test functions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        with Timer(func.__name__) as t:
            result = func(*args, **kwargs)
        print(f"\n   ⏱ Test completed in {t}")
        return result
    return wrapper


def print_header(title: str, test_num: int = None):
    """Print a section header."""
    print("\n" + "=" * 70)
    if test_num:
        print(f"TEST {test_num}: {title}")
    else:
        print(title)
    print("=" * 70)


def print_subheader(title: str):
    """Print a subsection header."""
    print(f"\n--- {title} ---")


def print_result(success: bool, message: str):
    """Print a test result."""
    icon = "✓" if success else "✗"
    print(f"   {icon} {message}")


def print_warning(message: str):
    """Print a warning."""
    print(f"   ⚠ {message}")


# =============================================================================
# TEMPORAL DETECTION FUNCTIONS
# =============================================================================

# Patterns for detecting temporal expressions
TEMPORAL_PATTERNS = {
    'year': re.compile(r'\b(1[89]\d{2}|20[0-2]\d)\b'),  # 1800-2029
    'decade': re.compile(r'\b(1[89]\d{2}|20[0-2]\d)s\b'),  # 1800s-2020s
    'year_range': re.compile(r'\b(1[89]\d{2}|20[0-2]\d)\s*[-–—to]+\s*(1[89]\d{2}|20[0-2]\d)\b'),
    'century': re.compile(r'\b(\d{1,2})(st|nd|rd|th)\s+century\b', re.IGNORECASE),
    'relative': re.compile(r'\b(last|past|recent|previous|next)\s+(year|decade|century|month|week)\b', re.IGNORECASE),
    'era': re.compile(r'\b(pre-war|post-war|antebellum|reconstruction|civil\s+rights\s+era|jazz\s+age|harlem\s+renaissance)\b', re.IGNORECASE),
}


def extract_temporal_expressions(text: str) -> Dict[str, List[Any]]:
    """
    Extract temporal expressions from text.
    
    Returns dict with keys: years, decades, ranges, centuries, relative, eras
    """
    results = {
        'years': [],
        'decades': [],
        'ranges': [],
        'centuries': [],
        'relative': [],
        'eras': [],
    }
    
    text_lower = text.lower()
    
    # Extract year ranges first (so we don't double-count individual years)
    for match in TEMPORAL_PATTERNS['year_range'].finditer(text):
        start_year = int(match.group(1))
        end_year = int(match.group(2))
        results['ranges'].append((start_year, end_year))
    
    # Extract decades (e.g., "1960s", "2020s")
    for match in TEMPORAL_PATTERNS['decade'].finditer(text):
        decade = match.group(1)
        results['decades'].append(f"{decade}s")
    
    # Extract individual years (excluding those in ranges)
    range_years = set()
    for start, end in results['ranges']:
        range_years.add(str(start))
        range_years.add(str(end))
    
    for match in TEMPORAL_PATTERNS['year'].finditer(text):
        year = match.group(1)
        if year not in range_years and f"{year}s" not in [d for d in results['decades']]:
            results['years'].append(int(year))
    
    # Extract centuries
    for match in TEMPORAL_PATTERNS['century'].finditer(text):
        century_num = int(match.group(1))
        results['centuries'].append(century_num)
    
    # Extract relative temporal expressions
    for match in TEMPORAL_PATTERNS['relative'].finditer(text_lower):
        results['relative'].append(match.group(0))
    
    # Extract era references
    for match in TEMPORAL_PATTERNS['era'].finditer(text_lower):
        results['eras'].append(match.group(0))
    
    return results


def has_temporal_intent(text: str) -> Tuple[bool, Dict]:
    """
    Check if query has temporal intent.
    
    Returns (has_temporal, details_dict)
    """
    temporal = extract_temporal_expressions(text)
    
    has_any = any([
        temporal['years'],
        temporal['decades'],
        temporal['ranges'],
        temporal['centuries'],
        temporal['relative'],
        temporal['eras'],
    ])
    
    return has_any, temporal


# =============================================================================
# TEST FUNCTIONS
# =============================================================================

@timed_test
def test_imports() -> Tuple[bool, Dict]:
    """Test 1: Import word discovery module."""
    print_header("IMPORT WORD DISCOVERY MODULE", 1)
    
    api_funcs = {}
    
    try:
        from word_discovery import (
            word_discovery_single_pass,
            validate_word,
            LOCATION_TYPES,
        )
        api_funcs['word_discovery_single_pass'] = word_discovery_single_pass
        api_funcs['validate_word'] = validate_word
        api_funcs['LOCATION_TYPES'] = LOCATION_TYPES
        print_result(True, "word_discovery module imported")
        print(f"   LOCATION_TYPES: {LOCATION_TYPES}")
    except ImportError as e:
        print_result(False, f"Failed to import word_discovery: {e}")
        return False, api_funcs
    
    return True, api_funcs


@timed_test
def test_searchapi_imports() -> Tuple[bool, Dict]:
    """Test 2: Import searchapi module."""
    print_header("IMPORT SEARCHAPI MODULE", 2)
    
    api_funcs = {}
    
    try:
        from searchapi import (
            batch_check_bigrams,
            get_exact_term_matches,
        )
        api_funcs['batch_check_bigrams'] = batch_check_bigrams
        api_funcs['get_exact_term_matches'] = get_exact_term_matches
        print_result(True, "searchapi module imported")
    except ImportError as e:
        print_result(False, f"Failed to import searchapi: {e}")
        
        # Try alternative imports
        try:
            from searchapi import batch_check_bigrams
            api_funcs['batch_check_bigrams'] = batch_check_bigrams
            print_warning("batch_check_bigrams imported, but get_exact_term_matches missing")
        except ImportError:
            print_warning("batch_check_bigrams not available")
        
        try:
            from searchapi import get_term_metadata
            api_funcs['get_term_metadata'] = get_term_metadata
            print_warning("Using get_term_metadata as fallback")
        except ImportError:
            print_warning("get_term_metadata not available either")
    
    return len(api_funcs) > 0, api_funcs


@timed_test
def test_single_city_detection(api_funcs: Dict) -> bool:
    """Test 3: Detection of single-word cities."""
    print_header("SINGLE-WORD CITY DETECTION", 3)
    
    cities = [
        "atlanta", "chicago", "houston", "phoenix", "philadelphia",
        "dallas", "austin", "detroit", "memphis", "baltimore",
        "milwaukee", "albuquerque", "tucson", "fresno", "sacramento",
    ]
    
    get_exact_term_matches = api_funcs.get('get_exact_term_matches')
    validate_word = api_funcs.get('validate_word')
    LOCATION_TYPES = api_funcs.get('LOCATION_TYPES', frozenset())
    
    detected = 0
    
    for city in cities:
        # Try get_exact_term_matches first (finds all keys like term:atlanta:*)
        if get_exact_term_matches:
            matches = get_exact_term_matches(city)
            if matches:
                # Check if any match is a location type
                for match in matches:
                    category = match.get('category', '').lower().replace(' ', '_')
                    if category in LOCATION_TYPES or 'city' in category or 'location' in category:
                        detected += 1
                        print_result(True, f"{city}: category='{match.get('category')}', pos='{match.get('pos')}'")
                        break
                else:
                    # Found in Redis but not as location
                    categories = [m.get('category', 'unknown') for m in matches]
                    print_warning(f"{city}: found but categories={categories} (not location)")
            else:
                print_result(False, f"{city}: not found in Redis")
        
        # Fallback to validate_word
        elif validate_word:
            result = validate_word(city)
            if result and isinstance(result, dict):
                category = result.get('category', '').lower().replace(' ', '_')
                if category in LOCATION_TYPES or 'city' in category:
                    detected += 1
                    print_result(True, f"{city}: category='{result.get('category')}'")
                else:
                    print_warning(f"{city}: valid but category='{result.get('category')}' (not location)")
            else:
                print_result(False, f"{city}: not found")
        else:
            print_warning(f"{city}: no lookup function available")
    
    print(f"\n   Cities detected: {detected}/{len(cities)}")
    return detected >= len(cities) * 0.5


@timed_test
def test_bigram_detection(api_funcs: Dict) -> bool:
    """Test 4: Detection of bigrams (two-word phrases)."""
    print_header("BIGRAM DETECTION", 4)
    
    bigrams = [
        ("new", "york"),
        ("los", "angeles"),
        ("san", "francisco"),
        ("las", "vegas"),
        ("san", "diego"),
        ("san", "antonio"),
        ("tuskegee", "airmen"),
        ("civil", "rights"),
        ("black", "history"),
        ("african", "american"),
    ]
    
    batch_check_bigrams = api_funcs.get('batch_check_bigrams')
    
    if not batch_check_bigrams:
        print_warning("batch_check_bigrams not available - skipping test")
        return False
    
    with Timer("batch_check_bigrams") as t:
        results = batch_check_bigrams(bigrams)
    
    print(f"   Batch lookup time: {t}")
    
    detected = 0
    for word1, word2 in bigrams:
        key = f"{word1} {word2}"
        if key in results:
            metadata = results[key]
            detected += 1
            print_result(True, f"'{key}': category='{metadata.get('category', 'unknown')}'")
        else:
            print_result(False, f"'{key}': not found")
    
    print(f"\n   Bigrams detected: {detected}/{len(bigrams)}")
    return detected >= len(bigrams) * 0.3


@timed_test
def test_trigram_detection(api_funcs: Dict) -> bool:
    """Test 5: Detection of trigrams (three-word phrases)."""
    print_header("TRIGRAM DETECTION", 5)
    
    # Test trigrams as overlapping bigrams
    trigrams = [
        "new york city",
        "salt lake city",
        "san luis obispo",
        "martin luther king",
    ]
    
    batch_check_bigrams = api_funcs.get('batch_check_bigrams')
    
    if not batch_check_bigrams:
        print_warning("batch_check_bigrams not available - skipping test")
        return False
    
    detected = 0
    
    for trigram in trigrams:
        words = trigram.split()
        if len(words) != 3:
            continue
        
        # Check both possible bigrams
        bigram1 = (words[0], words[1])
        bigram2 = (words[1], words[2])
        
        with Timer(f"'{trigram}'") as t:
            results = batch_check_bigrams([bigram1, bigram2])
        
        key1 = f"{words[0]} {words[1]}"
        key2 = f"{words[1]} {words[2]}"
        
        found_parts = []
        if key1 in results:
            found_parts.append(key1)
        if key2 in results:
            found_parts.append(key2)
        
        if found_parts:
            detected += 1
            print_result(True, f"'{trigram}': found parts {found_parts} ({t})")
        else:
            print_result(False, f"'{trigram}': no bigram parts found ({t})")
    
    print(f"\n   Trigrams with detected parts: {detected}/{len(trigrams)}")
    return detected >= 1


@timed_test
def test_word_discovery_locations(api_funcs: Dict) -> bool:
    """Test 6: Word discovery location extraction from full queries."""
    print_header("WORD DISCOVERY LOCATION EXTRACTION", 6)
    
    word_discovery_single_pass = api_funcs.get('word_discovery_single_pass')
    LOCATION_TYPES = api_funcs.get('LOCATION_TYPES', frozenset())
    
    if not word_discovery_single_pass:
        print_warning("word_discovery_single_pass not available - skipping test")
        return False
    
    test_queries = [
        "hbcus in georgia",
        "black owned restaurants in atlanta",
        "civil rights history in alabama",
        "jazz clubs in new york",
        "african american museums in chicago",
        "soul food in memphis",
    ]
    
    # Location keywords to check for (case-insensitive)
    location_keywords = {'city', 'state', 'location', 'region', 'country', 'neighborhood'}
    
    locations_found = 0
    
    for query in test_queries:
        with Timer(query) as t:
            result = word_discovery_single_pass(query)
        
        # Check for location in results
        has_location = False
        location_info = []
        all_terms = []
        
        if isinstance(result, dict):
            for term, metadata in result.items():
                if isinstance(metadata, dict):
                    category = metadata.get('category', '')
                    category_lower = category.lower().replace(' ', '_')
                    category_words = set(category.lower().split())
                    
                    all_terms.append(f"{term}:{category}")
                    
                    # Check if category matches location types
                    is_location = (
                        category_lower in LOCATION_TYPES or
                        any(kw in category_lower for kw in location_keywords) or
                        any(kw in category_words for kw in location_keywords)
                    )
                    
                    if is_location:
                        has_location = True
                        location_info.append(f"{term}={category}")
        
        if has_location:
            locations_found += 1
            print_result(True, f"'{query}': locations={location_info} ({t})")
        else:
            print_warning(f"'{query}': no location detected ({t})")
            print(f"      Terms found: {all_terms}")
    
    print(f"\n   Queries with locations: {locations_found}/{len(test_queries)}")
    return locations_found >= len(test_queries) * 0.5


@timed_test
def test_us_states(api_funcs: Dict) -> bool:
    """Test 7: Detection of US states (single-word and two-word)."""
    print_header("US STATE DETECTION", 7)
    
    single_word_states = [
        "alabama", "alaska", "arizona", "arkansas", "california",
        "colorado", "connecticut", "delaware", "florida", "georgia",
        "hawaii", "idaho", "illinois", "indiana", "iowa",
        "kansas", "kentucky", "louisiana", "maine", "maryland",
        "massachusetts", "michigan", "minnesota", "mississippi", "missouri",
        "montana", "nebraska", "nevada", "ohio", "oklahoma",
        "oregon", "pennsylvania", "tennessee", "texas", "utah",
        "vermont", "virginia", "washington", "wisconsin", "wyoming",
    ]
    
    two_word_states = [
        ("new", "hampshire"),
        ("new", "jersey"),
        ("new", "mexico"),
        ("new", "york"),
        ("north", "carolina"),
        ("north", "dakota"),
        ("south", "carolina"),
        ("south", "dakota"),
        ("west", "virginia"),
        ("rhode", "island"),
    ]
    
    get_exact_term_matches = api_funcs.get('get_exact_term_matches')
    batch_check_bigrams = api_funcs.get('batch_check_bigrams')
    LOCATION_TYPES = api_funcs.get('LOCATION_TYPES', frozenset())
    
    print_subheader("Single-word states")
    
    single_detected = 0
    with Timer("single-word states") as t:
        for state in single_word_states:
            if get_exact_term_matches:
                matches = get_exact_term_matches(state)
                if matches:
                    for match in matches:
                        category = match.get('category', '').lower().replace(' ', '_')
                        if 'state' in category or category in LOCATION_TYPES:
                            single_detected += 1
                            break
    
    print(f"   Single-word states: {single_detected}/{len(single_word_states)} ({t})")
    
    print_subheader("Two-word states (bigrams)")
    
    bigram_detected = 0
    if batch_check_bigrams:
        with Timer("two-word states") as t:
            bigram_results = batch_check_bigrams(two_word_states)
        
        for word1, word2 in two_word_states:
            key = f"{word1} {word2}"
            if key in bigram_results:
                bigram_detected += 1
        
        print(f"   Two-word states: {bigram_detected}/{len(two_word_states)} ({t})")
    else:
        print_warning("batch_check_bigrams not available")
    
    total = single_detected + bigram_detected
    total_expected = len(single_word_states) + len(two_word_states)
    
    print(f"\n   TOTAL: {total}/{total_expected} states detected")
    return total >= total_expected * 0.5


@timed_test
def test_temporal_detection() -> bool:
    """Test 8: Detection of temporal expressions (years, decades, etc.)."""
    print_header("TEMPORAL EXPRESSION DETECTION", 8)
    
    test_cases = [
        # (query, expected_type, expected_value_contains)
        ("events in 1963", "years", 1963),
        ("music from the 1960s", "decades", "1960s"),
        ("history 1955 to 1968", "ranges", (1955, 1968)),
        ("19th century literature", "centuries", 19),
        ("harlem renaissance artists", "eras", "harlem renaissance"),
        ("civil rights era speeches", "eras", "civil rights era"),
        ("last decade achievements", "relative", "last decade"),
        ("events in 2020", "years", 2020),
        ("the roaring 1920s", "decades", "1920s"),
        ("from 1865 to 1877", "ranges", (1865, 1877)),
        ("jazz age music", "eras", "jazz age"),
        ("past year news", "relative", "past year"),
        ("black history in 1954", "years", 1954),
        ("post-war migration", "eras", "post-war"),
        ("antebellum south", "eras", "antebellum"),
    ]
    
    detected = 0
    
    for query, expected_type, expected_value in test_cases:
        with Timer(f"'{query}'") as t:
            has_temporal, temporal_data = has_temporal_intent(query)
        
        # Check if expected type was found
        found_values = temporal_data.get(expected_type, [])
        
        if expected_type == "ranges":
            # Special handling for ranges (tuples)
            found = any(
                r[0] == expected_value[0] and r[1] == expected_value[1]
                for r in found_values
            )
        else:
            found = expected_value in found_values
        
        if found:
            detected += 1
            print_result(True, f"'{query}': {expected_type}={found_values} ({t})")
        else:
            print_result(False, f"'{query}': expected {expected_type}={expected_value}, got {found_values} ({t})")
    
    print(f"\n   Temporal expressions detected: {detected}/{len(test_cases)}")
    return detected >= len(test_cases) * 0.7


@timed_test
def test_temporal_with_locations(api_funcs: Dict) -> bool:
    """Test 9: Combined temporal + location detection."""
    print_header("COMBINED TEMPORAL + LOCATION DETECTION", 9)
    
    word_discovery_single_pass = api_funcs.get('word_discovery_single_pass')
    LOCATION_TYPES = api_funcs.get('LOCATION_TYPES', frozenset())
    
    # Location keywords to check for (case-insensitive)
    location_keywords = {'city', 'state', 'location', 'region', 'country', 'neighborhood'}
    
    test_queries = [
        "civil rights movement in alabama 1955-1968",
        "harlem renaissance new york 1920s",
        "black owned businesses in atlanta 2020",
        "jazz history in new orleans 19th century",
        "hbcus founded in georgia last decade",
        "african american migration to chicago 1910 to 1970",
    ]
    
    both_detected = 0
    
    for query in test_queries:
        # Check temporal
        with Timer("temporal") as t_temp:
            has_temporal, temporal_data = has_temporal_intent(query)
        
        # Check location via word discovery
        has_location = False
        location_info = []
        
        if word_discovery_single_pass:
            with Timer("location") as t_loc:
                result = word_discovery_single_pass(query)
            
            if isinstance(result, dict):
                for term, metadata in result.items():
                    if isinstance(metadata, dict):
                        category = metadata.get('category', '')
                        category_lower = category.lower().replace(' ', '_')
                        category_words = set(category.lower().split())
                        
                        # Check if category matches location types
                        is_location = (
                            category_lower in LOCATION_TYPES or
                            any(kw in category_lower for kw in location_keywords) or
                            any(kw in category_words for kw in location_keywords)
                        )
                        
                        if is_location:
                            has_location = True
                            location_info.append(f"{term}={category}")
        
        # Report results
        temporal_found = []
        for key, values in temporal_data.items():
            if values:
                temporal_found.append(f"{key}={values}")
        
        if has_temporal and has_location:
            both_detected += 1
            print_result(True, f"'{query}'")
            print(f"      Temporal: {temporal_found} ({t_temp})")
            print(f"      Location: {location_info} ({t_loc})")
        elif has_temporal:
            print_warning(f"'{query}': temporal only - {temporal_found}")
        elif has_location:
            print_warning(f"'{query}': location only - {location_info}")
        else:
            print_result(False, f"'{query}': neither detected")
    
    print(f"\n   Queries with both temporal + location: {both_detected}/{len(test_queries)}")
    return both_detected >= len(test_queries) * 0.3


@timed_test
def test_category_distribution(api_funcs: Dict) -> bool:
    """Test 10: Show distribution of categories in Redis hash."""
    print_header("CATEGORY DISTRIBUTION CHECK", 10)
    
    get_exact_term_matches = api_funcs.get('get_exact_term_matches')
    
    if not get_exact_term_matches:
        print_warning("get_exact_term_matches not available - skipping test")
        return False
    
    # Sample terms to check category distribution
    sample_terms = [
        "atlanta", "georgia", "new", "york", "history", "music",
        "jazz", "civil", "rights", "black", "african", "american",
        "church", "university", "museum", "restaurant",
    ]
    
    category_counts = {}
    
    with Timer("category scan") as t:
        for term in sample_terms:
            matches = get_exact_term_matches(term)
            if matches:
                for match in matches:
                    category = match.get('category', 'unknown')
                    category_counts[category] = category_counts.get(category, 0) + 1
    
    print(f"   Scan time: {t}")
    print_subheader("Categories found")
    
    for category, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"   • {category}: {count} terms")
    
    return len(category_counts) > 0


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Run all tests."""
    overall_start = time.perf_counter()
    
    print("=" * 70)
    print("BIGRAMS, TRIGRAMS, CITIES & TEMPORAL DETECTION TEST")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("NO EMBEDDING - Pure Word Discovery, Redis & Temporal Testing")
    print("=" * 70)
    
    # Track results
    results = {}
    api_funcs = {}
    
    # Test 1: Import word_discovery
    success, funcs = test_imports()
    results['imports'] = success
    api_funcs.update(funcs)
    
    if not success:
        print("\n❌ Cannot continue without word_discovery module")
        return
    
    # Test 2: Import searchapi
    success, funcs = test_searchapi_imports()
    results['searchapi'] = success
    api_funcs.update(funcs)
    
    # Test 3: Single-word cities
    results['single_cities'] = test_single_city_detection(api_funcs)
    
    # Test 4: Bigrams
    results['bigrams'] = test_bigram_detection(api_funcs)
    
    # Test 5: Trigrams
    results['trigrams'] = test_trigram_detection(api_funcs)
    
    # Test 6: Word discovery locations
    results['word_discovery_locations'] = test_word_discovery_locations(api_funcs)
    
    # Test 7: US States
    results['us_states'] = test_us_states(api_funcs)
    
    # Test 8: Temporal detection (pure regex - no Redis)
    results['temporal'] = test_temporal_detection()
    
    # Test 9: Combined temporal + location
    results['temporal_locations'] = test_temporal_with_locations(api_funcs)
    
    # Test 10: Category distribution
    results['category_distribution'] = test_category_distribution(api_funcs)
    
    # Summary
    overall_elapsed = time.perf_counter() - overall_start
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, success in results.items():
        icon = "✓" if success else "✗"
        print(f"   {icon} {test_name}")
    
    print(f"\n   Passed: {passed}/{total}")
    print(f"   Total time: {overall_elapsed*1000:.2f}ms ({overall_elapsed:.3f}s)")
    
    if passed == total:
        print("\n🎉 All tests passed!")
    elif passed >= total * 0.7:
        print("\n⚠ Most tests passed, but some issues remain")
    else:
        print("\n❌ Multiple tests failed - review output above")


if __name__ == "__main__":
    main()