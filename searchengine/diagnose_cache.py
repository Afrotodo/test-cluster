# """
# diagnose_cache.py
# Diagnostic script to check what's in your RAM cache.

# Run this in the same directory as your word_discovery.py and vocabulary_cache.py

# USAGE:
#     python diagnose_cache.py
# """

# import sys

# # =============================================================================
# # IMPORT CACHE
# # =============================================================================

# print("=" * 60)
# print("CACHE DIAGNOSTIC")
# print("=" * 60)

# try:
#     from vocabulary_cache import vocab_cache, ensure_loaded
#     CACHE_AVAILABLE = True
#     print("✓ vocabulary_cache imported successfully")
# except ImportError as e:
#     print(f"✗ Could not import vocabulary_cache: {e}")
#     CACHE_AVAILABLE = False
#     sys.exit(1)

# # =============================================================================
# # LOAD CACHE
# # =============================================================================

# print("\nLoading cache...")
# try:
#     ensure_loaded()
#     print(f"✓ Cache loaded: {vocab_cache.loaded}")
# except Exception as e:
#     print(f"✗ Failed to load cache: {e}")
#     sys.exit(1)

# # =============================================================================
# # CACHE STATUS
# # =============================================================================

# print("\n" + "-" * 60)
# print("CACHE STATUS")
# print("-" * 60)

# try:
#     status = vocab_cache.status()
#     for key, value in status.items():
#         print(f"  {key}: {value}")
# except Exception as e:
#     print(f"  Error getting status: {e}")

# # =============================================================================
# # TEST COMMON WORDS
# # =============================================================================

# print("\n" + "-" * 60)
# print("TESTING COMMON WORDS")
# print("-" * 60)

# test_words = [
#     # Basic words that should definitely be in vocab
#     'the', 'is', 'a', 'to', 'and', 'of', 'in', 'that', 'it', 'was',
#     # Nouns
#     'dog', 'cat', 'music', 'book', 'car', 'city', 'school', 'house',
#     # Adjectives
#     'blue', 'red', 'quick', 'brown', 'happy', 'good', 'new', 'old',
#     # Verbs
#     'go', 'want', 'love', 'play', 'see', 'have', 'make', 'get',
#     # Problem words from your test
#     'bleu', 'hungry', 'favorite', 'color', 'today', 'sky', 'fox',
#     # Proper nouns / locations
#     'georgia', 'atlanta', 'new york', 'african', 'american',
# ]

# in_cache = []
# in_stopwords = []
# not_found = []

# for word in test_words:
#     word_lower = word.lower().strip()
    
#     # Check main cache
#     metadata = vocab_cache.get_term(word_lower)
    
#     if metadata:
#         pos = metadata.get('pos', '?')
#         score = metadata.get('rank', 0)
#         in_cache.append((word_lower, pos, score))
#     elif vocab_cache.is_stopword(word_lower):
#         sw_pos = vocab_cache.get_stopword_pos(word_lower)
#         in_stopwords.append((word_lower, sw_pos))
#     else:
#         not_found.append(word_lower)

# print(f"\n✓ IN MAIN CACHE ({len(in_cache)}):")
# for word, pos, score in in_cache[:20]:  # Show first 20
#     print(f"    {word:15} pos={str(pos):20} score={score}")
# if len(in_cache) > 20:
#     print(f"    ... and {len(in_cache) - 20} more")

# print(f"\n✓ IN STOPWORDS ({len(in_stopwords)}):")
# for word, pos in in_stopwords:
#     print(f"    {word:15} pos={pos}")

# print(f"\n✗ NOT FOUND ({len(not_found)}):")
# for word in not_found:
#     print(f"    {word}")

# # =============================================================================
# # CHECK BIGRAMS
# # =============================================================================

# print("\n" + "-" * 60)
# print("TESTING BIGRAMS")
# print("-" * 60)

# test_bigrams = [
#     ('new', 'york'),
#     ('african', 'american'),
#     ('civil', 'rights'),
#     ('high', 'school'),
#     ('los', 'angeles'),
# ]

# for word1, word2 in test_bigrams:
#     bigram_meta = vocab_cache.get_bigram(word1, word2)
#     if bigram_meta:
#         print(f"  ✓ '{word1} {word2}' → FOUND")
#     else:
#         print(f"  ✗ '{word1} {word2}' → NOT FOUND")

# # =============================================================================
# # SAMPLE RAW DATA
# # =============================================================================

# print("\n" + "-" * 60)
# print("SAMPLE RAW CACHE DATA")
# print("-" * 60)

# # Try to peek at the raw data structure
# if hasattr(vocab_cache, 'terms') and vocab_cache.terms:
#     print(f"\nTerms dict has {len(vocab_cache.terms)} entries")
#     print("First 5 entries:")
#     for i, (key, value) in enumerate(list(vocab_cache.terms.items())[:5]):
#         print(f"  '{key}': {value}")

# if hasattr(vocab_cache, 'stopwords') and vocab_cache.stopwords:
#     print(f"\nStopwords dict has {len(vocab_cache.stopwords)} entries")
#     print("First 5 entries:")
#     for i, (key, value) in enumerate(list(vocab_cache.stopwords.items())[:5]):
#         print(f"  '{key}': {value}")

# # =============================================================================
# # CHECK SPECIFIC PROBLEM WORD
# # =============================================================================

# print("\n" + "-" * 60)
# print("DETAILED CHECK: 'blue' vs 'bleu'")
# print("-" * 60)

# for word in ['blue', 'bleu', 'Blue', 'BLUE']:
#     meta = vocab_cache.get_term(word)
#     meta_lower = vocab_cache.get_term(word.lower())
#     print(f"  get_term('{word}'): {meta}")
#     if word != word.lower():
#         print(f"  get_term('{word.lower()}'): {meta_lower}")

# # =============================================================================
# # DONE
# # =============================================================================

# print("\n" + "=" * 60)
# print("DIAGNOSTIC COMPLETE")
# print("=" * 60)

# if not_found:
#     print(f"\n⚠️  {len(not_found)} common words NOT in cache.")
#     print("   This explains why valid words are being flagged as unknown.")
#     print("   Check your vocabulary_cache.py data loading.")
# else:
#     print("\n✓ All common test words found in cache.")

"""
diagnose_full_system.py
Comprehensive diagnostic to check RAM cache, cache file, and Redis.

This will tell you exactly where your vocabulary data is and isn't.

USAGE:
    python diagnose_full_system.py
"""

import json
import os
import sys
from pathlib import Path
from collections import Counter

# =============================================================================
# SECTION 1: CHECK THE CACHE FILE ON DISK
# =============================================================================

print("=" * 70)
print("SECTION 1: CACHE FILE ON DISK")
print("=" * 70)

# Find the cache file
script_dir = Path(__file__).parent
cache_file = script_dir / 'cache' / 'data' / 'vocabulary_data.json'

print(f"\nLooking for cache file at: {cache_file}")

if cache_file.exists():
    file_size = cache_file.stat().st_size
    print(f"✓ File exists")
    print(f"  Size: {file_size:,} bytes ({file_size / 1024:.1f} KB)")
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            file_data = json.load(f)
        
        print(f"  Total entries: {len(file_data):,}")
        
        # Analyze categories
        categories = Counter()
        entity_types = Counter()
        pos_values = Counter()
        
        for key, value in file_data.items():
            cat = value.get('category', 'unknown')
            categories[cat] += 1
            
            etype = value.get('entity_type', 'unknown')
            entity_types[etype] += 1
            
            pos = value.get('pos', 'unknown')
            pos_values[str(pos)] += 1
        
        print(f"\n  Categories breakdown:")
        for cat, count in categories.most_common(20):
            print(f"    {cat}: {count:,}")
        
        print(f"\n  Entity types breakdown:")
        for etype, count in entity_types.most_common():
            print(f"    {etype}: {count:,}")
        
        print(f"\n  POS values (top 10):")
        for pos, count in pos_values.most_common(10):
            print(f"    {pos}: {count:,}")
        
        # Sample entries
        print(f"\n  Sample entries (first 5):")
        for i, (key, value) in enumerate(list(file_data.items())[:5]):
            print(f"    {key}: {value}")
            
    except json.JSONDecodeError as e:
        print(f"✗ Error reading JSON: {e}")
    except Exception as e:
        print(f"✗ Error: {e}")
else:
    print(f"✗ File does not exist")
    print(f"  Checking alternative locations...")
    
    # Check alternative locations
    alternatives = [
        script_dir / 'vocabulary_data.json',
        script_dir / 'data' / 'vocabulary_data.json',
        Path('cache/data/vocabulary_data.json'),
        Path('vocabulary_data.json'),
    ]
    
    for alt in alternatives:
        if alt.exists():
            print(f"  Found at: {alt}")

# =============================================================================
# SECTION 2: CHECK THE RAM CACHE (vocab_cache object)
# =============================================================================

print("\n" + "=" * 70)
print("SECTION 2: RAM CACHE (vocab_cache in memory)")
print("=" * 70)

try:
    from vocabulary_cache import vocab_cache, ensure_loaded
    
    print(f"\n✓ vocabulary_cache imported")
    print(f"  Cache loaded: {vocab_cache.loaded}")
    
    # Try to load if not loaded
    if not vocab_cache.loaded:
        print(f"\n  Attempting to load cache...")
        result = ensure_loaded()
        print(f"  Load result: {result}")
    
    # Get status
    status = vocab_cache.status()
    print(f"\n  Cache Status:")
    for key, value in status.items():
        print(f"    {key}: {value}")
    
    # Check actual data structures
    print(f"\n  Data Structure Sizes:")
    print(f"    terms dict: {len(vocab_cache.terms):,} entries")
    print(f"    bigrams dict: {len(vocab_cache.bigrams):,} entries")
    print(f"    trigrams dict: {len(vocab_cache.trigrams):,} entries")
    print(f"    cities set: {len(vocab_cache.cities):,} entries")
    print(f"    states set: {len(vocab_cache.states):,} entries")
    print(f"    locations set: {len(vocab_cache.locations):,} entries")
    print(f"    stopwords set: {len(vocab_cache.stopwords):,} entries")
    
    # Analyze what's in terms
    if vocab_cache.terms:
        print(f"\n  Terms Analysis:")
        term_categories = Counter()
        term_pos = Counter()
        
        for term, meta in vocab_cache.terms.items():
            cat = meta.get('category', 'unknown')
            term_categories[cat] += 1
            pos = meta.get('pos', 'unknown')
            term_pos[str(pos)] += 1
        
        print(f"    Categories:")
        for cat, count in term_categories.most_common(10):
            print(f"      {cat}: {count:,}")
        
        print(f"    POS values:")
        for pos, count in term_pos.most_common(10):
            print(f"      {pos}: {count:,}")
    
except ImportError as e:
    print(f"✗ Could not import vocabulary_cache: {e}")
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# SECTION 3: CHECK REDIS (if available)
# =============================================================================

print("\n" + "=" * 70)
print("SECTION 3: REDIS DATABASE")
print("=" * 70)

try:
    # Try to import Redis client
    try:
        from searchapi import RedisLookupTable, get_suggestions
        redis_available = True
        print(f"\n✓ Redis module imported")
    except ImportError:
        redis_available = False
        print(f"\n✗ Could not import searchapi module")
    
    if redis_available:
        # Test a few lookups
        test_words = ['dog', 'blue', 'music', 'atlanta', 'happy', 'the']
        
        print(f"\n  Testing Redis lookups:")
        for word in test_words:
            try:
                result = get_suggestions(word, limit=3, max_distance=0)
                suggestions = result.get('suggestions', [])
                if suggestions:
                    first = suggestions[0]
                    print(f"    '{word}': ✓ Found (term={first.get('term')}, pos={first.get('pos')})")
                else:
                    print(f"    '{word}': ✗ Not found")
            except Exception as e:
                print(f"    '{word}': Error - {e}")
        
        # Try to get total count if possible
        try:
            # This depends on your Redis setup
            from redis import Redis
            r = Redis(host='localhost', port=6379, decode_responses=True)
            
            # Count keys with your prefix
            keys = r.keys('prefix:term:*')
            print(f"\n  Redis key count (prefix:term:*): {len(keys):,}")
            
            # Sample some keys
            if keys:
                print(f"  Sample keys:")
                for key in list(keys)[:5]:
                    print(f"    {key}")
        except Exception as e:
            print(f"\n  Could not count Redis keys: {e}")
            
except Exception as e:
    print(f"✗ Redis check failed: {e}")

# =============================================================================
# SECTION 4: MEMORY USAGE
# =============================================================================

print("\n" + "=" * 70)
print("SECTION 4: SYSTEM MEMORY")
print("=" * 70)

try:
    import psutil
    
    # System memory
    mem = psutil.virtual_memory()
    print(f"\n  System RAM:")
    print(f"    Total: {mem.total / (1024**3):.1f} GB")
    print(f"    Available: {mem.available / (1024**3):.1f} GB")
    print(f"    Used: {mem.percent}%")
    
    # Process memory
    process = psutil.Process(os.getpid())
    proc_mem = process.memory_info()
    print(f"\n  This Process:")
    print(f"    RSS (Resident): {proc_mem.rss / (1024**2):.1f} MB")
    print(f"    VMS (Virtual): {proc_mem.vms / (1024**2):.1f} MB")
    
except ImportError:
    print("\n  psutil not installed - run: pip install psutil")
except Exception as e:
    print(f"\n  Error getting memory info: {e}")

# =============================================================================
# SECTION 5: DIAGNOSIS SUMMARY
# =============================================================================

print("\n" + "=" * 70)
print("DIAGNOSIS SUMMARY")
print("=" * 70)

issues = []
recommendations = []

# Check file
try:
    if not cache_file.exists():
        issues.append("Cache file does not exist")
        recommendations.append("Run your Colab notebook to populate the cache")
    elif len(file_data) < 100:
        issues.append(f"Cache file only has {len(file_data)} entries (expected thousands)")
        recommendations.append("Your Colab export may only be sending locations, not full vocabulary")
except:
    pass

# Check RAM cache
try:
    if len(vocab_cache.terms) < 100:
        issues.append(f"RAM cache only has {len(vocab_cache.terms)} terms (expected thousands)")
        recommendations.append("The cache file needs more vocabulary data")
except:
    pass

# Print summary
if issues:
    print("\n⚠️  ISSUES FOUND:")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
    
    print("\n📋 RECOMMENDATIONS:")
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec}")
else:
    print("\n✓ No obvious issues found")

print("\n" + "=" * 70)