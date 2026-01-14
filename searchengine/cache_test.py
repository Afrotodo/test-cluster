#!/usr/bin/env python
"""
cache_feasibility_test.py
Test script to determine if in-memory caching is feasible for your Redis data.

This script will:
1. Audit your Redis hash - count keys by category (using your existing functions)
2. Load data into Python memory
3. Measure memory usage
4. Compare Redis lookup speed vs in-memory lookup speed
5. Give you a recommendation

Run this BEFORE making any changes to your app.
"""

import os
import sys
import time
import gc
from datetime import datetime
from typing import Dict, Set, List, Any, Tuple
from collections import defaultdict

# Add project paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')
import django
django.setup()

# Import your existing functions (same as bigrams_and_city_testing.py)
try:
    from searchapi import get_exact_term_matches, batch_check_bigrams
    SEARCHAPI_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import from searchapi: {e}")
    SEARCHAPI_AVAILABLE = False

try:
    from word_discovery import word_discovery_single_pass, validate_word, LOCATION_TYPES
    WORD_DISCOVERY_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import from word_discovery: {e}")
    WORD_DISCOVERY_AVAILABLE = False


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

class Timer:
    """Context manager for timing code blocks."""
    
    def __init__(self, name: str = ""):
        self.name = name
        self.start_time = None
        self.elapsed = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start_time
    
    @property
    def ms(self) -> float:
        return (self.elapsed or 0) * 1000
    
    @property
    def seconds(self) -> float:
        return self.elapsed or 0
    
    def __str__(self):
        if self.elapsed is None:
            return "not measured"
        if self.elapsed < 1:
            return f"{self.ms:.2f}ms"
        return f"{self.seconds:.2f}s"


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        # Fallback without psutil
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def format_number(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def print_header(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_step(step_num: int, title: str):
    print(f"\n{'─' * 70}")
    print(f"STEP {step_num}: {title}")
    print('─' * 70)


def print_result(label: str, value: Any, indent: int = 3):
    spaces = " " * indent
    print(f"{spaces}{label}: {value}")


def print_table(headers: List[str], rows: List[List[Any]], indent: int = 3):
    """Print a simple table."""
    spaces = " " * indent
    
    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    
    # Print header
    header_line = " │ ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(f"{spaces}{header_line}")
    print(f"{spaces}{'─' * len(header_line)}")
    
    # Print rows
    for row in rows:
        row_line = " │ ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        print(f"{spaces}{row_line}")


# =============================================================================
# STEP 1: AUDIT REDIS DATA (using your existing functions)
# =============================================================================

def step1_audit_redis() -> Tuple[bool, Dict]:
    """Audit Redis by testing lookups with your existing functions."""
    print_step(1, "AUDIT YOUR REDIS DATA")
    
    results = {
        'categories': defaultdict(int),
        'sample_data': defaultdict(list),
        'functions_available': [],
    }
    
    # Check what functions are available
    if SEARCHAPI_AVAILABLE:
        results['functions_available'].append('get_exact_term_matches')
        results['functions_available'].append('batch_check_bigrams')
        print_result("searchapi functions", "✓ Available")
    else:
        print_result("searchapi functions", "✗ Not available")
    
    if WORD_DISCOVERY_AVAILABLE:
        results['functions_available'].append('word_discovery_single_pass')
        results['functions_available'].append('validate_word')
        print_result("word_discovery functions", "✓ Available")
    else:
        print_result("word_discovery functions", "✗ Not available")
    
    if not results['functions_available']:
        print("\n   ✗ No functions available to audit Redis")
        return False, results
    
    # Test a bunch of known terms to see what categories exist
    test_terms = [
        # Cities
        "atlanta", "chicago", "houston", "phoenix", "dallas",
        "detroit", "memphis", "baltimore", "miami", "oakland",
        "seattle", "denver", "boston", "portland", "charlotte",
        # States
        "georgia", "texas", "california", "florida", "alabama",
        "mississippi", "louisiana", "tennessee", "ohio", "michigan",
        # Common words
        "the", "and", "is", "are", "was", "have", "been", "will",
        "history", "music", "school", "doctor", "restaurant", "hotel",
        "black", "african", "american", "civil", "rights",
        # Domain specific
        "hbcu", "hbcus", "airmen", "tuskegee", "spelman", "morehouse",
        "jazz", "blues", "gospel", "soul", "church", "university",
    ]
    
    print("\n   Sampling terms to discover categories...")
    
    with Timer("term_audit") as t:
        for term in test_terms:
            if SEARCHAPI_AVAILABLE:
                matches = get_exact_term_matches(term)
                if matches:
                    for match in matches:
                        category = match.get('category', 'unknown')
                        results['categories'][category] += 1
                        if len(results['sample_data'][category]) < 3:
                            results['sample_data'][category].append(term)
    
    print_result("Audit time", str(t))
    
    # Print category breakdown
    if results['categories']:
        print("\n   Categories found in your Redis:")
        
        rows = []
        total = sum(results['categories'].values())
        for cat, count in sorted(results['categories'].items(), key=lambda x: -x[1]):
            pct = (count / total * 100) if total > 0 else 0
            samples = ", ".join(results['sample_data'].get(cat, []))
            rows.append([cat, str(count), f"{pct:.1f}%", samples])
        
        print_table(["Category", "Count", "Percent", "Sample Terms"], rows)
        
        print(f"\n   Note: This is a sample of {len(test_terms)} terms, not your full Redis data.")
        print("   The actual counts will be much higher.")
    
    # Test bigrams
    print("\n   Testing bigram detection...")
    
    test_bigrams = [
        ("new", "york"), ("los", "angeles"), ("san", "francisco"),
        ("north", "carolina"), ("civil", "rights"), ("tuskegee", "airmen"),
    ]
    
    if SEARCHAPI_AVAILABLE:
        with Timer("bigram_test") as t:
            bigram_results = batch_check_bigrams(test_bigrams)
        
        print_result("Bigram test time", str(t))
        print_result("Bigrams found", f"{len(bigram_results)}/{len(test_bigrams)}")
        
        for key, meta in bigram_results.items():
            cat = meta.get('category', 'unknown')
            results['categories'][f"bigram:{cat}"] += 1
    
    return True, results


# =============================================================================
# STEP 2: BUILD IN-MEMORY CACHE (simulate what we'd load)
# =============================================================================

def step2_load_into_memory(audit_results: Dict) -> Tuple[bool, Dict]:
    """Build in-memory cache structures and measure memory."""
    print_step(2, "BUILD IN-MEMORY CACHE STRUCTURES")
    
    results = {
        'cities': set(),
        'states': set(),
        'bigrams': {},
        'all_terms': {},
        'load_time': 0,
        'memory_before': 0,
        'memory_after': 0,
    }
    
    if not SEARCHAPI_AVAILABLE:
        print_result("Status", "✗ searchapi not available")
        return False, results
    
    # Force garbage collection and measure baseline
    gc.collect()
    results['memory_before'] = get_memory_usage_mb()
    print_result("Memory before loading", f"{results['memory_before']:.1f} MB")
    
    # Terms to load - in production this would come from a full Redis scan
    # For this test, we'll use a representative sample
    
    # US Cities (sample - in production you'd load all from Redis)
    us_cities = [
        "atlanta", "chicago", "houston", "phoenix", "philadelphia", "dallas",
        "austin", "detroit", "memphis", "baltimore", "milwaukee", "albuquerque",
        "tucson", "fresno", "sacramento", "miami", "oakland", "minneapolis",
        "cleveland", "pittsburgh", "seattle", "denver", "boston", "portland",
        "charlotte", "nashville", "louisville", "richmond", "birmingham",
        "jackson", "montgomery", "mobile", "savannah", "charleston", "columbia",
        "raleigh", "durham", "greensboro", "winston", "fayetteville", "wilmington",
        "norfolk", "hampton", "newport", "alexandria", "arlington", "fairfax",
    ]
    
    # US States
    us_states = [
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
        "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
        "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
        "maine", "maryland", "massachusetts", "michigan", "minnesota",
        "mississippi", "missouri", "montana", "nebraska", "nevada",
        "ohio", "oklahoma", "oregon", "pennsylvania", "tennessee", "texas",
        "utah", "vermont", "virginia", "washington", "wisconsin", "wyoming",
    ]
    
    # Bigrams
    bigram_pairs = [
        ("new", "york"), ("los", "angeles"), ("san", "francisco"), ("san", "diego"),
        ("las", "vegas"), ("new", "orleans"), ("kansas", "city"), ("oklahoma", "city"),
        ("salt", "lake"), ("st", "louis"), ("new", "hampshire"), ("new", "jersey"),
        ("new", "mexico"), ("north", "carolina"), ("south", "carolina"),
        ("north", "dakota"), ("south", "dakota"), ("west", "virginia"),
        ("rhode", "island"), ("civil", "rights"), ("tuskegee", "airmen"),
        ("african", "american"), ("martin", "luther"), ("rosa", "parks"),
    ]
    
    print("\n   Loading terms into memory structures...")
    
    with Timer("load") as t:
        # Load cities
        for city in us_cities:
            results['cities'].add(city.lower())
            results['all_terms'][city.lower()] = {'category': 'US City'}
        
        # Load states
        for state in us_states:
            results['states'].add(state.lower())
            results['all_terms'][state.lower()] = {'category': 'US State'}
        
        # Load bigrams (verify they exist via your function)
        if SEARCHAPI_AVAILABLE:
            bigram_results = batch_check_bigrams(bigram_pairs)
            for key, meta in bigram_results.items():
                results['bigrams'][key.lower()] = meta
                results['all_terms'][key.lower()] = meta
    
    results['load_time'] = t.seconds
    
    # Measure memory after loading
    gc.collect()
    results['memory_after'] = get_memory_usage_mb()
    memory_used = results['memory_after'] - results['memory_before']
    
    print_result("Load time", str(t))
    print_result("Memory after loading", f"{results['memory_after']:.1f} MB")
    print_result("Memory used by cache", f"{memory_used:.1f} MB")
    
    print("\n   Cache contents:")
    rows = [
        ["Cities (Set)", str(len(results['cities'])), "O(1) lookup"],
        ["States (Set)", str(len(results['states'])), "O(1) lookup"],
        ["Bigrams (Dict)", str(len(results['bigrams'])), "O(1) lookup"],
        ["All Terms (Dict)", str(len(results['all_terms'])), "O(1) lookup"],
    ]
    print_table(["Structure", "Count", "Lookup Speed"], rows)
    
    print(f"\n   Note: This is a SAMPLE cache with ~{len(results['all_terms'])} terms.")
    print("   Your full cache would include all terms from Redis.")
    print("   Memory scales roughly: 1 MB per 3,000-5,000 terms.")
    
    return True, results


# =============================================================================
# STEP 3: COMPARE LOOKUP SPEEDS
# =============================================================================

def step3_compare_lookup_speeds(memory_cache: Dict) -> Tuple[bool, Dict]:
    """Compare Redis lookup speed vs in-memory lookup speed."""
    print_step(3, "COMPARE LOOKUP SPEEDS")
    
    results = {
        'redis_times': [],
        'memory_times': [],
        'redis_avg': 0,
        'memory_avg': 0,
        'speedup': 0,
    }
    
    if not SEARCHAPI_AVAILABLE:
        print("   ✗ searchapi not available for comparison")
        return False, results
    
    # Test terms
    test_terms = [
        "atlanta", "georgia", "chicago", "texas", "california",
        "memphis", "detroit", "florida", "alabama", "jazz",
        "history", "music", "school", "doctor", "restaurant",
        "university", "college", "church", "museum", "hotel",
        "phoenix", "dallas", "austin", "boston", "denver",
    ]
    
    cities_cache = memory_cache.get('cities', set())
    states_cache = memory_cache.get('states', set())
    all_terms_cache = memory_cache.get('all_terms', {})
    
    # Warm up
    print("\n   Warming up caches...")
    for term in test_terms[:3]:
        _ = term.lower() in cities_cache
        try:
            _ = get_exact_term_matches(term)
        except:
            pass
    
    # Test Redis lookups (using your existing function)
    print("   Testing Redis lookups (25 terms)...")
    redis_times = []
    
    for term in test_terms:
        with Timer() as t:
            try:
                result = get_exact_term_matches(term)
            except:
                pass
        redis_times.append(t.ms)
    
    # Test in-memory lookups
    print("   Testing in-memory lookups (25 terms)...")
    memory_times = []
    
    for term in test_terms:
        with Timer() as t:
            term_lower = term.lower()
            is_city = term_lower in cities_cache
            is_state = term_lower in states_cache
            metadata = all_terms_cache.get(term_lower)
        memory_times.append(t.ms)
    
    # Calculate averages
    results['redis_times'] = redis_times
    results['memory_times'] = memory_times
    results['redis_avg'] = sum(redis_times) / len(redis_times) if redis_times else 0
    results['memory_avg'] = sum(memory_times) / len(memory_times) if memory_times else 0
    results['speedup'] = results['redis_avg'] / results['memory_avg'] if results['memory_avg'] > 0 else 0
    
    # Print comparison
    print("\n   Individual lookup times:")
    print("\n   Term                    Redis        Memory       Speedup")
    print("   " + "─" * 60)
    
    for i, term in enumerate(test_terms):
        redis_t = redis_times[i] if i < len(redis_times) else 0
        memory_t = memory_times[i] if i < len(memory_times) else 0
        speedup = redis_t / memory_t if memory_t > 0 else 0
        print(f"   {term:<22} {redis_t:>8.2f}ms   {memory_t:>8.4f}ms   {speedup:>8.0f}x")
    
    print("   " + "─" * 60)
    print(f"   {'AVERAGE':<22} {results['redis_avg']:>8.2f}ms   {results['memory_avg']:>8.4f}ms   {results['speedup']:>8.0f}x")
    
    return True, results


# =============================================================================
# STEP 4: SIMULATE FULL QUERY PROCESSING
# =============================================================================

def step4_simulate_query_processing(memory_cache: Dict) -> Tuple[bool, Dict]:
    """Simulate processing full search queries."""
    print_step(4, "SIMULATE FULL QUERY PROCESSING")
    
    results = {
        'queries': [],
        'redis_total': 0,
        'memory_total': 0,
    }
    
    if not WORD_DISCOVERY_AVAILABLE:
        print("   ⚠ word_discovery_single_pass not available")
        return False, results
    
    cities_cache = memory_cache.get('cities', set())
    states_cache = memory_cache.get('states', set())
    bigrams_cache = memory_cache.get('bigrams', {})
    all_terms_cache = memory_cache.get('all_terms', {})
    
    test_queries = [
        "hbcus in georgia",
        "black owned restaurants in atlanta",
        "civil rights history in alabama",
        "jazz clubs in new york",
        "african american museums in chicago",
        "soul food in memphis",
        "hotels in los angeles",
        "schools in north carolina",
        "tuskegee airmen history",
        "harlem renaissance new york 1920s",
    ]
    
    print("\n   Processing queries with both methods...")
    print("\n   Query                                      Redis        Memory")
    print("   " + "─" * 65)
    
    for query in test_queries:
        # Method 1: Current approach (Redis via word_discovery)
        redis_time = 0
        with Timer() as t:
            try:
                result = word_discovery_single_pass(query)
            except:
                pass
        redis_time = t.ms
        
        # Method 2: In-memory approach (simulated)
        with Timer() as t:
            words = query.lower().split()
            query_result = {
                'locations': [],
                'bigrams': [],
                'terms': [],
            }
            
            # Check individual words
            for word in words:
                if word in cities_cache:
                    query_result['locations'].append(word)
                elif word in states_cache:
                    query_result['locations'].append(word)
                
                if word in all_terms_cache:
                    query_result['terms'].append(word)
            
            # Check bigrams
            for i in range(len(words) - 1):
                bigram = f"{words[i]} {words[i+1]}"
                if bigram in bigrams_cache:
                    query_result['bigrams'].append(bigram)
        
        memory_time = t.ms
        
        results['queries'].append({
            'query': query,
            'redis_time': redis_time,
            'memory_time': memory_time,
        })
        results['redis_total'] += redis_time
        results['memory_total'] += memory_time
        
        print(f"   {query:<40} {redis_time:>8.0f}ms   {memory_time:>8.4f}ms")
    
    print("   " + "─" * 65)
    
    speedup = results['redis_total'] / results['memory_total'] if results['memory_total'] > 0 else 0
    print(f"   {'TOTAL':<40} {results['redis_total']:>8.0f}ms   {results['memory_total']:>8.4f}ms")
    print(f"\n   Overall speedup: {speedup:,.0f}x faster with in-memory cache")
    
    return True, results


# =============================================================================
# STEP 5: GENERATE RECOMMENDATION
# =============================================================================

def step5_recommendation(audit: Dict, memory: Dict, lookup: Dict, query: Dict):
    """Generate final recommendation."""
    print_step(5, "RECOMMENDATION")
    
    total_terms = len(memory.get('all_terms', {}))
    memory_used = memory.get('memory_after', 0) - memory.get('memory_before', 0)
    load_time = memory.get('load_time', 0)
    speedup = lookup.get('speedup', 0)
    
    print("\n   Summary:")
    rows = [
        ["Total terms to cache", format_number(total_terms)],
        ["Memory required", f"{memory_used:.1f} MB"],
        ["One-time load time", f"{load_time:.1f} seconds"],
        ["Lookup speedup", f"{speedup:,.0f}x faster"],
        ["Query processing speedup", f"{query.get('redis_total', 0) / max(query.get('memory_total', 1), 0.001):,.0f}x faster"],
    ]
    print_table(["Metric", "Value"], rows)
    
    # Decision criteria
    print("\n   Feasibility check:")
    
    checks = []
    
    # Memory check
    if memory_used < 50:
        checks.append(("Memory usage < 50 MB", "✓ PASS", "Safe for any server"))
    elif memory_used < 200:
        checks.append(("Memory usage < 200 MB", "✓ PASS", "Fine for most servers"))
    else:
        checks.append(("Memory usage", "⚠ WARNING", f"{memory_used:.0f} MB is significant"))
    
    # Load time check
    if load_time < 5:
        checks.append(("Load time < 5 seconds", "✓ PASS", "Fast startup"))
    elif load_time < 30:
        checks.append(("Load time < 30 seconds", "✓ PASS", "Acceptable startup"))
    else:
        checks.append(("Load time", "⚠ WARNING", "Consider lazy loading"))
    
    # Speedup check
    if speedup > 1000:
        checks.append(("Speedup > 1000x", "✓ PASS", "Massive improvement"))
    elif speedup > 100:
        checks.append(("Speedup > 100x", "✓ PASS", "Significant improvement"))
    else:
        checks.append(("Speedup", "⚠ CHECK", "May not be worth the complexity"))
    
    for check, status, note in checks:
        print(f"   {status} {check}: {note}")
    
    # Final recommendation
    all_pass = all("PASS" in c[1] for c in checks)
    
    print("\n   " + "─" * 50)
    if all_pass:
        print("   ✓ RECOMMENDATION: Implement in-memory caching")
        print("\n   Next steps:")
        print("   1. Create vocabulary_cache.py with Sets and Dicts")
        print("   2. Load cache in Django's apps.py ready() method")
        print("   3. Modify word_discovery.py to check cache first")
        print("   4. Keep Redis as fallback for cache misses")
        print("   5. Re-run your test suite to verify speedup")
    else:
        print("   ⚠ RECOMMENDATION: Review warnings before proceeding")
    
    print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("CACHE FEASIBILITY TEST")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print("""
This script will:
  1. Audit your Redis data (count keys by category)
  2. Load data into Python memory (measure memory usage)
  3. Compare lookup speeds (Redis vs in-memory)
  4. Simulate full query processing
  5. Give you a recommendation
    """)
    
    overall_start = time.perf_counter()
    
    # Step 1: Audit Redis
    success, audit_results = step1_audit_redis()
    if not success:
        print("\n❌ Cannot proceed without Redis connection")
        return
    
    # Step 2: Load into memory
    success, memory_results = step2_load_into_memory(audit_results)
    if not success:
        print("\n❌ Failed to load data into memory")
        return
    
    # Step 3: Compare lookup speeds
    success, lookup_results = step3_compare_lookup_speeds(memory_results)
    
    # Step 4: Simulate query processing
    success, query_results = step4_simulate_query_processing(memory_results)
    
    # Step 5: Recommendation
    step5_recommendation(audit_results, memory_results, lookup_results, query_results)
    
    # Final timing
    overall_elapsed = time.perf_counter() - overall_start
    print("=" * 70)
    print(f"Total test time: {overall_elapsed:.1f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    main()