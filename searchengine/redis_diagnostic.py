#!/usr/bin/env python3
"""
REDIS DIAGNOSTIC SCRIPT
=======================

This script directly tests your Redis API to understand:
1. What data format Redis returns
2. Whether bigrams/trigrams exist in Redis
3. What categories and metadata are available
4. Why word_discovery might not be detecting things

Run: python redis_diagnostic.py
"""

import sys
import json
import time
from typing import Dict, Any, List, Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

# Test queries to examine
TEST_TERMS = [
    # Single words
    "jazz",
    "atlanta",
    "georgia",
    "harlem",
    "music",
    "king",
    "martin",
    "luther",
    "rosa",
    "parks",
    "civil",
    "rights",
    
    # Bigrams (test if they exist as single entries)
    "martin luther",
    "rosa parks",
    "new york",
    "civil rights",
    "harlem renaissance",
    
    # Trigrams
    "martin luther king",
    
    # Misspellings (for correction testing)
    "atlenta",
    "jaz",
    "musci",
    "Marin",
]

# =============================================================================
# REDIS API IMPORTS
# =============================================================================

print("=" * 70)
print("🔧 REDIS DIAGNOSTIC TOOL")
print("=" * 70)

try:
    from searchapi import (
        RedisLookupTable,
        get_fuzzy_matches,
        get_suggestions,
        damerau_levenshtein_distance,
    )
    print("✅ Successfully imported Redis functions from searchapi")
    REDIS_AVAILABLE = True
except ImportError as e:
    print(f"❌ Failed to import from searchapi: {e}")
    REDIS_AVAILABLE = False

# Try alternative import
if not REDIS_AVAILABLE:
    try:
        sys.path.insert(0, '.')
        from searchapi import get_suggestions, get_fuzzy_matches
        print("✅ Successfully imported Redis functions (alternative path)")
        REDIS_AVAILABLE = True
    except ImportError as e:
        print(f"❌ Alternative import also failed: {e}")

# =============================================================================
# DIAGNOSTIC FUNCTIONS
# =============================================================================

def test_single_term(term: str) -> Dict[str, Any]:
    """Test a single term lookup and return full details"""
    print(f"\n{'─' * 60}")
    print(f"🔍 Testing term: '{term}'")
    print('─' * 60)
    
    result = {
        'term': term,
        'found': False,
        'exact_match': None,
        'fuzzy_matches': [],
        'suggestions': [],
        'raw_response': None,
        'error': None
    }
    
    if not REDIS_AVAILABLE:
        result['error'] = "Redis not available"
        print("  ❌ Redis not available")
        return result
    
    try:
        # Get suggestions (this is the main API used in word_discovery)
        start = time.perf_counter()
        suggestion_result = get_suggestions(term, limit=10, max_distance=2)
        elapsed = (time.perf_counter() - start) * 1000
        
        result['raw_response'] = suggestion_result
        result['response_time_ms'] = elapsed
        
        print(f"  ⏱️  Response time: {elapsed:.2f}ms")
        print(f"  📦 Raw response type: {type(suggestion_result)}")
        
        # Print raw response structure
        if isinstance(suggestion_result, dict):
            print(f"  📦 Response keys: {list(suggestion_result.keys())}")
            
            suggestions = suggestion_result.get('suggestions', [])
            result['suggestions'] = suggestions
            
            print(f"  📊 Number of suggestions: {len(suggestions)}")
            
            if suggestions:
                print(f"\n  📋 SUGGESTIONS:")
                for i, s in enumerate(suggestions[:5]):  # First 5
                    print(f"      [{i}] {json.dumps(s, indent=8)}")
                
                # Check for exact match
                for s in suggestions:
                    s_term = s.get('term', '').lower()
                    if s_term == term.lower():
                        result['found'] = True
                        result['exact_match'] = s
                        print(f"\n  ✅ EXACT MATCH FOUND:")
                        print(f"      Term: {s.get('term')}")
                        print(f"      POS: {s.get('pos')}")
                        print(f"      Category: {s.get('category')}")
                        print(f"      Rank: {s.get('rank')}")
                        print(f"      All fields: {list(s.keys())}")
                        break
                
                if not result['found']:
                    print(f"\n  ⚠️ No exact match - closest suggestions shown above")
            else:
                print("  ⚠️ No suggestions returned")
        else:
            print(f"  ⚠️ Unexpected response type: {type(suggestion_result)}")
            print(f"  📦 Raw response: {suggestion_result}")
            
    except Exception as e:
        result['error'] = str(e)
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    return result


def test_fuzzy_matches(term: str) -> Dict[str, Any]:
    """Test fuzzy matching specifically"""
    print(f"\n{'─' * 60}")
    print(f"🔎 Testing FUZZY matches for: '{term}'")
    print('─' * 60)
    
    if not REDIS_AVAILABLE:
        print("  ❌ Redis not available")
        return {'error': 'Redis not available'}
    
    try:
        result = get_fuzzy_matches(term, max_distance=2, limit=10)
        print(f"  📦 Raw fuzzy result: {json.dumps(result, indent=4)[:500]}")
        return result
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return {'error': str(e)}


def analyze_data_structure(results: List[Dict]) -> None:
    """Analyze the data structure returned by Redis"""
    print("\n" + "=" * 70)
    print("📊 DATA STRUCTURE ANALYSIS")
    print("=" * 70)
    
    all_fields = set()
    all_categories = set()
    all_pos = set()
    
    for r in results:
        if r.get('exact_match'):
            match = r['exact_match']
            all_fields.update(match.keys())
            if match.get('category'):
                all_categories.add(match['category'])
            if match.get('pos'):
                pos = match['pos']
                if isinstance(pos, list):
                    all_pos.update(pos)
                else:
                    all_pos.add(pos)
        
        for s in r.get('suggestions', []):
            all_fields.update(s.keys())
            if s.get('category'):
                all_categories.add(s['category'])
            if s.get('pos'):
                pos = s['pos']
                if isinstance(pos, list):
                    all_pos.update(pos)
                else:
                    all_pos.add(pos)
    
    print(f"\n📋 All fields found in responses:")
    for f in sorted(all_fields):
        print(f"    - {f}")
    
    print(f"\n📋 All categories found:")
    for c in sorted(all_categories):
        print(f"    - {c}")
    
    print(f"\n📋 All POS tags found:")
    for p in sorted(all_pos):
        print(f"    - {p}")


def check_bigram_storage() -> None:
    """Check how bigrams are stored in Redis"""
    print("\n" + "=" * 70)
    print("🔗 BIGRAM STORAGE CHECK")
    print("=" * 70)
    
    test_bigrams = [
        ("martin", "luther"),
        ("rosa", "parks"),
        ("new", "york"),
        ("civil", "rights"),
        ("harlem", "renaissance"),
    ]
    
    for word1, word2 in test_bigrams:
        print(f"\n  Testing bigram: '{word1} {word2}'")
        
        # Test different key formats
        formats_to_test = [
            f"{word1} {word2}",           # Space separated
            f"{word1}|{word2}",            # Pipe separated
            f"{word1}_{word2}",            # Underscore separated
            f"{word1}{word2}",             # No separator
        ]
        
        for fmt in formats_to_test:
            if REDIS_AVAILABLE:
                result = get_suggestions(fmt, limit=3, max_distance=0)
                suggestions = result.get('suggestions', [])
                if suggestions:
                    print(f"    ✅ Found with format '{fmt}': {suggestions[0].get('term')}")
                    print(f"       Category: {suggestions[0].get('category')}")
                    break
        else:
            print(f"    ❌ Not found in any format")


def check_location_categories() -> None:
    """Check location-related categories"""
    print("\n" + "=" * 70)
    print("📍 LOCATION CATEGORY CHECK")
    print("=" * 70)
    
    location_terms = [
        "atlanta",
        "georgia",
        "chicago",
        "new york",
        "harlem",
        "alabama",
        "mississippi",
        "memphis",
        "detroit",
    ]
    
    location_categories_found = {}
    
    for term in location_terms:
        if REDIS_AVAILABLE:
            result = get_suggestions(term, limit=5, max_distance=0)
            suggestions = result.get('suggestions', [])
            
            for s in suggestions:
                if s.get('term', '').lower() == term.lower():
                    cat = s.get('category', 'NO_CATEGORY')
                    location_categories_found[term] = cat
                    print(f"  '{term}' → category: '{cat}'")
                    break
            else:
                print(f"  '{term}' → NOT FOUND or no exact match")
    
    print(f"\n  Unique location categories: {set(location_categories_found.values())}")


def run_word_discovery_simulation(query: str) -> None:
    """Simulate what word_discovery does and show each step"""
    print("\n" + "=" * 70)
    print(f"🔬 SIMULATING word_discovery FOR: '{query}'")
    print("=" * 70)
    
    words = query.split()
    
    print(f"\n  PASS 1: Word Validation")
    print("  " + "-" * 50)
    
    for i, word in enumerate(words):
        word_lower = word.lower()
        
        # Check stopwords
        stopwords = {'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
                     'by', 'from', 'is', 'are', 'was', 'were', 'and', 'or', 'but'}
        
        if word_lower in stopwords:
            print(f"    [{i+1}] '{word}' → STOPWORD")
            continue
        
        if REDIS_AVAILABLE:
            result = get_suggestions(word_lower, limit=5, max_distance=0)
            suggestions = result.get('suggestions', [])
            
            found = False
            for s in suggestions:
                if s.get('term', '').lower() == word_lower:
                    found = True
                    print(f"    [{i+1}] '{word}' → VALID")
                    print(f"         POS: {s.get('pos')}, Category: {s.get('category')}")
                    break
            
            if not found:
                print(f"    [{i+1}] '{word}' → UNKNOWN (would trigger correction)")
    
    print(f"\n  PASS 4: Bigram Detection")
    print("  " + "-" * 50)
    
    for i in range(len(words) - 1):
        bigram = f"{words[i].lower()} {words[i+1].lower()}"
        
        if REDIS_AVAILABLE:
            result = get_suggestions(bigram, limit=3, max_distance=0)
            suggestions = result.get('suggestions', [])
            
            found = False
            for s in suggestions:
                if s.get('term', '').lower() == bigram:
                    found = True
                    print(f"    BIGRAM FOUND: '{bigram}'")
                    print(f"         Category: {s.get('category')}")
                    break
            
            if not found:
                print(f"    '{bigram}' → not a known bigram")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("Starting Redis diagnostics...")
    print("=" * 70)
    
    if not REDIS_AVAILABLE:
        print("\n❌ Cannot run diagnostics - Redis functions not available")
        print("Make sure searchapi.py is in your Python path")
        sys.exit(1)
    
    # Test all terms
    results = []
    for term in TEST_TERMS:
        result = test_single_term(term)
        results.append(result)
    
    # Analyze structure
    analyze_data_structure(results)
    
    # Check bigram storage
    check_bigram_storage()
    
    # Check location categories
    check_location_categories()
    
    # Simulate word_discovery
    test_queries = [
        "jazz musicians from Atlanta Georgia",
        "Martin Luther King speeches",
        "civil rights movement",
        "Harlem Renaissance artists",
    ]
    
    for q in test_queries:
        run_word_discovery_simulation(q)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 DIAGNOSTIC SUMMARY")
    print("=" * 70)
    
    found_count = sum(1 for r in results if r.get('found'))
    print(f"  Terms tested: {len(TEST_TERMS)}")
    print(f"  Exact matches found: {found_count}")
    print(f"  Terms not found: {len(TEST_TERMS) - found_count}")
    
    missing = [r['term'] for r in results if not r.get('found') and not r.get('error')]
    if missing:
        print(f"\n  Missing terms (may need to be added to Redis):")
        for m in missing:
            print(f"    - {m}")
    
    print("\n" + "=" * 70)
    print("✅ Diagnostics complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()