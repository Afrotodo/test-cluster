"""
test_cache_system.py
Comprehensive test script for vocabulary cache system.

Run this BEFORE deploying to production.

Usage:
    1. Make sure your local Django server is running:
       python manage.py runserver
    
    2. Run this script:
       python test_cache_system.py

    3. Or test without server (cache only):
       python test_cache_system.py --no-server
"""

import json
import time
import sys
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

# Local server URL
LOCAL_SERVER = "http://127.0.0.1:8000"

# API secret (must match your local .env)
API_SECRET = "test-secret-123"

# Test data matching your Redis hash format
TEST_VOCABULARY = {
    # Cities
    "term:atlanta:us_city": {
        "term": "atlanta",
        "display": "Atlanta",
        "category": "US City",
        "description": "Atlanta, GA, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 100
    },
    "term:chicago:us_city": {
        "term": "chicago",
        "display": "Chicago",
        "category": "US City",
        "description": "Chicago, IL, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 95
    },
    "term:houston:us_city": {
        "term": "houston",
        "display": "Houston",
        "category": "US City",
        "description": "Houston, TX, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 90
    },
    
    # States
    "term:georgia:us_state": {
        "term": "georgia",
        "display": "Georgia",
        "category": "US State",
        "description": "Georgia, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 100
    },
    "term:texas:us_state": {
        "term": "texas",
        "display": "Texas",
        "category": "US State",
        "description": "Texas, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 95
    },
    "term:california:us_state": {
        "term": "california",
        "display": "California",
        "category": "US State",
        "description": "California, USA",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 90
    },
    
    # Bigrams (two-word locations)
    "term:new york:us_state": {
        "term": "new york",
        "display": "New York",
        "category": "US State",
        "description": "New York, USA",
        "pos": "['noun']",
        "entity_type": "bigram",
        "rank": 100
    },
    "term:los angeles:us_city": {
        "term": "los angeles",
        "display": "Los Angeles",
        "category": "US City",
        "description": "Los Angeles, CA, USA",
        "pos": "['noun']",
        "entity_type": "bigram",
        "rank": 95
    },
    "term:civil rights:keyword": {
        "term": "civil rights",
        "display": "Civil Rights",
        "category": "Keyword",
        "description": "",
        "pos": "['noun']",
        "entity_type": "bigram",
        "rank": 85
    },
    
    # Dictionary words
    "term:the:dictionary_word": {
        "term": "the",
        "display": "the",
        "category": "Dictionary Word",
        "description": "",
        "pos": "['determiner']",
        "entity_type": "unigram",
        "rank": 0
    },
    "term:in:dictionary_word": {
        "term": "in",
        "display": "in",
        "category": "Dictionary Word",
        "description": "",
        "pos": "['preposition']",
        "entity_type": "unigram",
        "rank": 0
    },
    "term:blue:dictionary_word": {
        "term": "blue",
        "display": "blue",
        "category": "Dictionary Word",
        "description": "",
        "pos": "['adjective']",
        "entity_type": "unigram",
        "rank": 50
    },
    "term:car:dictionary_word": {
        "term": "car",
        "display": "car",
        "category": "Dictionary Word",
        "description": "",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 60
    },
    
    # Keywords
    "term:hbcus:keyword": {
        "term": "hbcus",
        "display": "HBCUs",
        "category": "Keyword",
        "description": "Historically Black Colleges and Universities",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 80
    },
    "term:jazz:keyword": {
        "term": "jazz",
        "display": "Jazz",
        "category": "Keyword",
        "description": "",
        "pos": "['noun']",
        "entity_type": "unigram",
        "rank": 75
    },
    
    # Trigram
    "term:new york city:us_city": {
        "term": "new york city",
        "display": "New York City",
        "category": "US City",
        "description": "New York City, NY, USA",
        "pos": "['noun']",
        "entity_type": "trigram",
        "rank": 100
    },
}


# =============================================================================
# TEST UTILITIES
# =============================================================================

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def ok(self, test_name):
        self.passed += 1
        print(f"   ✅ {test_name}")
    
    def fail(self, test_name, reason=""):
        self.failed += 1
        self.errors.append((test_name, reason))
        print(f"   ❌ {test_name}: {reason}")
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"RESULTS: {self.passed}/{total} passed")
        if self.errors:
            print(f"\nFailures:")
            for name, reason in self.errors:
                print(f"   • {name}: {reason}")
        print(f"{'='*60}")
        return self.failed == 0


results = TestResult()


# =============================================================================
# TEST 1: VOCABULARY CACHE (Standalone)
# =============================================================================

def test_vocabulary_cache_standalone():
    """Test vocabulary_cache.py without Django server."""
    print("\n" + "="*60)
    print("TEST 1: VOCABULARY CACHE (Standalone)")
    print("="*60)
    
    try:
        # Import the cache module
        # Adjust path if needed based on your project structure
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        
        try:
            from vocabulary_cache import VocabularyCache, is_location_category, STOPWORD_POS
        except ImportError:
            from searchengine.vocabulary_cache import VocabularyCache, is_location_category, STOPWORD_POS
        
        results.ok("Import vocabulary_cache")
    except Exception as e:
        results.fail("Import vocabulary_cache", str(e))
        return False
    
    # Create fresh cache instance for testing
    cache = VocabularyCache()
    
    # Test 1.1: Load from dict
    print("\n   Loading test data...")
    try:
        success = cache.load_from_dict(TEST_VOCABULARY, source="test")
        if success and cache.loaded:
            results.ok(f"Load from dict ({cache.term_count} terms)")
        else:
            results.fail("Load from dict", "Failed to load")
            return False
    except Exception as e:
        results.fail("Load from dict", str(e))
        return False
    
    # Test 1.2: City lookups
    print("\n   Testing city lookups...")
    cities_to_test = [("atlanta", True), ("chicago", True), ("xyz123", False)]
    for city, expected in cities_to_test:
        actual = cache.is_city(city)
        if actual == expected:
            results.ok(f"is_city('{city}') = {actual}")
        else:
            results.fail(f"is_city('{city}')", f"expected {expected}, got {actual}")
    
    # Test 1.3: State lookups
    print("\n   Testing state lookups...")
    states_to_test = [("georgia", True), ("texas", True), ("atlantis", False)]
    for state, expected in states_to_test:
        actual = cache.is_state(state)
        if actual == expected:
            results.ok(f"is_state('{state}') = {actual}")
        else:
            results.fail(f"is_state('{state}')", f"expected {expected}, got {actual}")
    
    # Test 1.4: Location lookups (cities + states)
    print("\n   Testing location lookups...")
    locations_to_test = [("atlanta", True), ("georgia", True), ("hbcus", False)]
    for loc, expected in locations_to_test:
        actual = cache.is_location(loc)
        if actual == expected:
            results.ok(f"is_location('{loc}') = {actual}")
        else:
            results.fail(f"is_location('{loc}')", f"expected {expected}, got {actual}")
    
    # Test 1.5: Bigram lookups
    print("\n   Testing bigram lookups...")
    bigrams_to_test = [
        (("new", "york"), True),
        (("los", "angeles"), True),
        (("civil", "rights"), True),
        (("not", "abigram"), False),
    ]
    for (w1, w2), expected in bigrams_to_test:
        actual = cache.is_bigram(w1, w2)
        if actual == expected:
            results.ok(f"is_bigram('{w1}', '{w2}') = {actual}")
        else:
            results.fail(f"is_bigram('{w1}', '{w2}')", f"expected {expected}, got {actual}")
    
    # Test 1.6: Trigram lookups
    print("\n   Testing trigram lookups...")
    actual = cache.is_trigram("new", "york", "city")
    if actual:
        results.ok("is_trigram('new', 'york', 'city') = True")
    else:
        results.fail("is_trigram('new', 'york', 'city')", "expected True")
    
    # Test 1.7: Get term metadata
    print("\n   Testing get_term...")
    term_data = cache.get_term("atlanta")
    if term_data and term_data.get("category") == "US City":
        results.ok(f"get_term('atlanta') returned correct category")
    else:
        results.fail("get_term('atlanta')", f"unexpected data: {term_data}")
    
    # Test 1.8: Stopword detection
    print("\n   Testing stopword detection...")
    if cache.is_stopword("the") and cache.is_stopword("in"):
        results.ok("Stopwords 'the' and 'in' detected")
    else:
        results.fail("Stopword detection", "Failed to detect stopwords")
    
    # Test 1.9: Stopword POS mapping
    print("\n   Testing stopword POS mapping...")
    expected_pos = [("the", "determiner"), ("in", "preposition"), ("and", "conjunction")]
    for word, expected in expected_pos:
        actual = cache.get_stopword_pos(word)
        if actual == expected:
            results.ok(f"get_stopword_pos('{word}') = '{actual}'")
        else:
            results.fail(f"get_stopword_pos('{word}')", f"expected '{expected}', got '{actual}'")
    
    # Test 1.10: Classify query
    print("\n   Testing classify_query...")
    test_queries = [
        ("hbcus in georgia", {
            "expected_locations": ["georgia"],
            "expected_stopwords": ["in"],
            "expected_terms": ["hbcus", "in", "georgia"],
        }),
        ("jazz in new york", {
            "expected_locations": ["new york"],
            "expected_bigrams": ["new york"],
            "expected_terms": ["jazz", "in", "new york"],
        }),
        ("the blue car", {
            "expected_stopwords": ["the"],
            "expected_terms": ["the", "blue", "car"],
        }),
    ]
    
    for query, expectations in test_queries:
        result = cache.classify_query(query)
        
        # Check locations
        if "expected_locations" in expectations:
            if set(result.get("locations", [])) == set(expectations["expected_locations"]):
                results.ok(f"classify_query('{query}') - locations correct")
            else:
                results.fail(f"classify_query('{query}') - locations", 
                           f"expected {expectations['expected_locations']}, got {result.get('locations')}")
        
        # Check bigrams
        if "expected_bigrams" in expectations:
            if set(result.get("bigrams", [])) == set(expectations["expected_bigrams"]):
                results.ok(f"classify_query('{query}') - bigrams correct")
            else:
                results.fail(f"classify_query('{query}') - bigrams",
                           f"expected {expectations['expected_bigrams']}, got {result.get('bigrams')}")
        
        # Check stopwords included
        if "expected_stopwords" in expectations:
            if set(result.get("stopwords", [])) == set(expectations["expected_stopwords"]):
                results.ok(f"classify_query('{query}') - stopwords tracked")
            else:
                results.fail(f"classify_query('{query}') - stopwords",
                           f"expected {expectations['expected_stopwords']}, got {result.get('stopwords')}")
    
    # Test 1.11: Performance test
    print("\n   Testing lookup performance...")
    iterations = 10000
    start = time.perf_counter()
    for _ in range(iterations):
        cache.is_city("atlanta")
        cache.is_state("georgia")
        cache.is_bigram("new", "york")
    elapsed = time.perf_counter() - start
    avg_time_us = (elapsed / (iterations * 3)) * 1_000_000
    
    if avg_time_us < 10:  # Should be under 10 microseconds
        results.ok(f"Lookup performance: {avg_time_us:.2f}µs per lookup")
    else:
        results.fail("Lookup performance", f"{avg_time_us:.2f}µs is too slow")
    
    # Test 1.12: Status
    print("\n   Testing status...")
    status = cache.status()
    required_keys = ["loaded", "term_count", "cities", "states", "bigrams"]
    if all(k in status for k in required_keys):
        results.ok(f"Status contains all required keys")
        print(f"      Status: {json.dumps(status, indent=6)}")
    else:
        results.fail("Status", f"Missing keys: {set(required_keys) - set(status.keys())}")
    
    return True


# =============================================================================
# TEST 2: API ENDPOINTS (Requires Running Server)
# =============================================================================

def test_api_endpoints():
    """Test cache API endpoints (requires running Django server)."""
    print("\n" + "="*60)
    print("TEST 2: API ENDPOINTS")
    print("="*60)
    
    try:
        import requests
    except ImportError:
        results.fail("Import requests", "pip install requests")
        return False
    
    results.ok("Import requests")
    
    # Test 2.1: Check server is running
    print("\n   Checking server connection...")
    try:
        response = requests.get(f"{LOCAL_SERVER}/api/cache/status/", timeout=5)
        if response.status_code == 200:
            results.ok(f"Server is running at {LOCAL_SERVER}")
        else:
            results.fail("Server connection", f"Status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        results.fail("Server connection", f"Cannot connect to {LOCAL_SERVER}")
        print(f"\n   ⚠️  Make sure your Django server is running:")
        print(f"      python manage.py runserver")
        return False
    
    # Test 2.2: Get initial status
    print("\n   Testing /api/cache/status/...")
    try:
        response = requests.get(f"{LOCAL_SERVER}/api/cache/status/")
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                results.ok("GET /api/cache/status/ returns success")
            else:
                results.fail("GET /api/cache/status/", "success=false")
        else:
            results.fail("GET /api/cache/status/", f"Status {response.status_code}")
    except Exception as e:
        results.fail("GET /api/cache/status/", str(e))
    
    # Test 2.3: Reload cache without auth (should fail)
    print("\n   Testing auth protection...")
    try:
        response = requests.post(
            f"{LOCAL_SERVER}/api/cache/reload/",
            json={"test": "data"},
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 401:
            results.ok("POST without auth returns 401")
        else:
            results.fail("Auth protection", f"Expected 401, got {response.status_code}")
    except Exception as e:
        results.fail("Auth protection", str(e))
    
    # Test 2.4: Reload cache with auth
    print("\n   Testing /api/cache/reload/...")
    try:
        response = requests.post(
            f"{LOCAL_SERVER}/api/cache/reload/",
            json=TEST_VOCABULARY,
            headers={
                "Authorization": f"Bearer {API_SECRET}",
                "Content-Type": "application/json"
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                stats = data.get("stats", {})
                results.ok(f"POST /api/cache/reload/ loaded {stats.get('term_count', 0)} terms")
            else:
                results.fail("POST /api/cache/reload/", data.get("error", "unknown error"))
        else:
            results.fail("POST /api/cache/reload/", f"Status {response.status_code}: {response.text[:200]}")
    except Exception as e:
        results.fail("POST /api/cache/reload/", str(e))
    
    # Test 2.5: Verify cache loaded
    print("\n   Verifying cache loaded...")
    try:
        response = requests.get(f"{LOCAL_SERVER}/api/cache/status/")
        if response.status_code == 200:
            data = response.json()
            status = data.get("status", {})
            if status.get("loaded") and status.get("term_count", 0) > 0:
                results.ok(f"Cache is loaded with {status['term_count']} terms")
            else:
                results.fail("Cache verification", f"Cache not loaded: {status}")
        else:
            results.fail("Cache verification", f"Status {response.status_code}")
    except Exception as e:
        results.fail("Cache verification", str(e))
    
    # Test 2.6: Test term lookup endpoint
    print("\n   Testing /api/cache/test/?term=...")
    test_terms = [
        ("atlanta", True, "US City"),
        ("georgia", True, "US State"),
        ("xyz123notreal", False, None),
    ]
    for term, should_exist, expected_category in test_terms:
        try:
            response = requests.get(f"{LOCAL_SERVER}/api/cache/test/?term={term}")
            if response.status_code == 200:
                data = response.json()
                is_location = data.get("is_location", False)
                metadata = data.get("metadata")
                
                if should_exist:
                    if metadata and metadata.get("category") == expected_category:
                        results.ok(f"GET /api/cache/test/?term={term} - correct category")
                    else:
                        results.fail(f"GET /api/cache/test/?term={term}", 
                                   f"expected category '{expected_category}', got {metadata}")
                else:
                    if not metadata:
                        results.ok(f"GET /api/cache/test/?term={term} - correctly not found")
                    else:
                        results.fail(f"GET /api/cache/test/?term={term}",
                                   f"should not exist but got {metadata}")
            else:
                results.fail(f"GET /api/cache/test/?term={term}", f"Status {response.status_code}")
        except Exception as e:
            results.fail(f"GET /api/cache/test/?term={term}", str(e))
    
    # Test 2.7: Test query classification endpoint
    print("\n   Testing /api/cache/test/?query=...")
    try:
        response = requests.get(f"{LOCAL_SERVER}/api/cache/test/?query=hbcus+in+georgia")
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", {})
            
            if "georgia" in result.get("locations", []):
                results.ok("Query classification found location 'georgia'")
            else:
                results.fail("Query classification", f"'georgia' not in locations: {result}")
            
            if "in" in result.get("stopwords", []):
                results.ok("Query classification tracked stopword 'in'")
            else:
                results.fail("Query classification", f"'in' not in stopwords: {result}")
        else:
            results.fail("Query classification", f"Status {response.status_code}")
    except Exception as e:
        results.fail("Query classification", str(e))
    
    return True


# =============================================================================
# TEST 3: WORD DISCOVERY INTEGRATION
# =============================================================================

def test_word_discovery_integration():
    """Test word_discovery.py uses the cache correctly."""
    print("\n" + "="*60)
    print("TEST 3: WORD DISCOVERY INTEGRATION")
    print("="*60)
    
    try:
        # Import word_discovery
        try:
            from word_discovery import (
                validate_word, get_term_metadata, check_bigram_exists,
                word_discovery_single_pass, get_cache_status
            )
        except ImportError:
            from searchengine.word_discovery import (
                validate_word, get_term_metadata, check_bigram_exists,
                word_discovery_single_pass, get_cache_status
            )
        
        results.ok("Import word_discovery")
    except Exception as e:
        results.fail("Import word_discovery", str(e))
        return False
    
    # First ensure cache is loaded
    try:
        from vocabulary_cache import vocab_cache
    except ImportError:
        from searchengine.vocabulary_cache import vocab_cache
    
    if not vocab_cache.loaded:
        print("\n   Loading cache for word_discovery tests...")
        vocab_cache.load_from_dict(TEST_VOCABULARY, source="test")
    
    # Test 3.1: validate_word uses cache
    print("\n   Testing validate_word with cache...")
    test_words = [
        ("atlanta", True),
        ("georgia", True),
        ("xyz123notreal", False),
    ]
    for word, should_be_valid in test_words:
        start = time.perf_counter()
        result = validate_word(word)
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        is_valid = result.get("is_valid", False)
        source = result.get("source", "unknown")
        
        if is_valid == should_be_valid:
            results.ok(f"validate_word('{word}') = {is_valid} in {elapsed_ms:.2f}ms (source: {source})")
        else:
            results.fail(f"validate_word('{word}')", f"expected {should_be_valid}, got {is_valid}")
    
    # Test 3.2: get_term_metadata uses cache
    print("\n   Testing get_term_metadata with cache...")
    metadata = get_term_metadata("atlanta")
    if metadata and metadata.get("category") == "US City":
        results.ok("get_term_metadata('atlanta') returns correct data")
    else:
        results.fail("get_term_metadata('atlanta')", f"unexpected: {metadata}")
    
    # Test 3.3: check_bigram_exists uses cache
    print("\n   Testing check_bigram_exists with cache...")
    bigram_tests = [
        (("new", "york"), True),
        (("not", "abigram"), False),
    ]
    for (w1, w2), expected in bigram_tests:
        result = check_bigram_exists(w1, w2)
        actual = result is not None
        if actual == expected:
            results.ok(f"check_bigram_exists('{w1}', '{w2}') = {actual}")
        else:
            results.fail(f"check_bigram_exists('{w1}', '{w2}')", f"expected {expected}")
    
    # Test 3.4: word_discovery_single_pass performance
    print("\n   Testing word_discovery_single_pass performance...")
    query = "hbcus in georgia"
    
    start = time.perf_counter()
    result = word_discovery_single_pass(query)
    elapsed_ms = (time.perf_counter() - start) * 1000
    
    if elapsed_ms < 10:  # Should be under 10ms
        results.ok(f"word_discovery_single_pass completed in {elapsed_ms:.2f}ms")
    else:
        results.fail("word_discovery_single_pass performance", f"{elapsed_ms:.2f}ms is too slow")
    
    # Test 3.5: get_cache_status works
    print("\n   Testing get_cache_status...")
    status = get_cache_status()
    if status.get("loaded"):
        results.ok(f"get_cache_status() shows cache loaded")
    else:
        results.fail("get_cache_status()", "Cache not loaded")
    
    return True


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*60)
    print("   VOCABULARY CACHE SYSTEM - COMPREHENSIVE TESTS")
    print("="*60)
    
    # Check for --no-server flag
    skip_server_tests = "--no-server" in sys.argv
    
    # Test 1: Vocabulary cache standalone
    test_vocabulary_cache_standalone()
    
    # Test 2: API endpoints (requires server)
    if skip_server_tests:
        print("\n" + "="*60)
        print("TEST 2: API ENDPOINTS - SKIPPED (--no-server)")
        print("="*60)
    else:
        test_api_endpoints()
    
    # Test 3: Word discovery integration
    test_word_discovery_integration()
    
    # Summary
    success = results.summary()
    
    if success:
        print("\n✅ ALL TESTS PASSED - Ready to deploy!")
    else:
        print("\n❌ SOME TESTS FAILED - Fix issues before deploying")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())