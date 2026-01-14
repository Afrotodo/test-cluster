#!/usr/bin/env python
"""
embedding_testing.py
Comprehensive testing for embedding service and Typesense search.

Tests:
1. Embedding service connection
2. Word discovery and location detection
3. Location extraction from queries
4. Full search pipeline
5. Location filtering in search results
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

import django
django.setup()


def print_header(title):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def print_result(success, message):
    """Print a test result."""
    status = "✓" if success else "✗"
    print(f"   {status} {message}")
    return success


# =============================================================================
# TEST 1: EMBEDDING SERVICE CONNECTION
# =============================================================================

def test_embedding_service():
    """Test connection to the embedding service."""
    print_header("TEST 1: EMBEDDING SERVICE CONNECTION")
    
    from decouple import config
    
    try:
        embedding_url = config('EMBEDDING_SERVICE_URL')
        print(f"\n1. Embedding URL configured: {embedding_url}")
    except Exception as e:
        print(f"\n1. ✗ FAILED to get EMBEDDING_SERVICE_URL: {e}")
        return False
    
    # Extract base URL (remove /embed if present)
    base_url = embedding_url.replace('/embed', '').replace('/batch', '').rstrip('/')
    
    print(f"\n2. Testing health endpoint: {base_url}/health")
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        data = response.json()
        print(f"   Response: {data}")
        
        if data.get('status') == 'ok' and data.get('model_loaded'):
            print_result(True, "Health check passed - model loaded")
        else:
            print_result(False, f"Health check failed: {data}")
            return False
    except requests.exceptions.Timeout:
        print_result(False, "Connection timed out - is the service running?")
        return False
    except requests.exceptions.ConnectionError as e:
        print_result(False, f"Connection error: {e}")
        return False
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False
    
    print(f"\n3. Testing embedding generation: {embedding_url}")
    try:
        response = requests.post(
            embedding_url,
            json={"text": "test query"},
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        
        embedding = data.get('embedding', [])
        dimensions = data.get('dimensions', len(embedding))
        time_ms = data.get('time_ms', 'N/A')
        
        print(f"   Embedding dimensions: {dimensions}")
        print(f"   Generation time: {time_ms}ms")
        print(f"   First 5 values: {embedding[:5]}")
        
        if dimensions > 0:
            print_result(True, f"Embedding generated successfully ({dimensions}D)")
            return True
        else:
            print_result(False, "Empty embedding returned")
            return False
            
    except Exception as e:
        print_result(False, f"Embedding generation failed: {e}")
        return False


# =============================================================================
# TEST 2: WORD DISCOVERY
# =============================================================================

def test_word_discovery():
    """Test word discovery module."""
    print_header("TEST 2: WORD DISCOVERY")
    
    try:
        from searchengine.word_discovery import (
            word_discovery_single_pass,
            get_search_strategy,
            get_filter_terms
        )
        print_result(True, "Word discovery module imported")
    except ImportError as e:
        try:
            from word_discovery import (
                word_discovery_single_pass,
                get_search_strategy,
                get_filter_terms
            )
            print_result(True, "Word discovery module imported (direct)")
        except ImportError as e2:
            print_result(False, f"Import failed: {e2}")
            return False
    
    test_queries = [
        ("hbcus in georgia", ["hbcus", "georgia"], "georgia"),
        ("black doctors in atlanta", ["black", "doctors", "atlanta"], "atlanta"),
        ("tuskegee airmen", ["tuskegee", "airmen"], None),
        ("restaurants in new york", ["restaurants", "new", "york"], "new york"),
    ]
    
    all_passed = True
    
    for query, expected_terms, expected_location in test_queries:
        print(f"\n   Testing: '{query}'")
        
        try:
            result = word_discovery_single_pass(query, verbose=False)
            
            valid_terms = [t['search_word'].lower() for t in result.get('valid_terms', [])]
            strategy = get_search_strategy(result)
            total_score = result.get('total_score', 0)
            avg_score = result.get('average_score', 0)
            
            print(f"      Valid terms: {valid_terms}")
            print(f"      Strategy: {strategy}")
            print(f"      Scores: total={total_score}, avg={avg_score}")
            
            # Check for location detection
            location_terms = [
                t for t in result.get('terms', [])
                if t.get('pos') == 'proper_noun' or 
                   t.get('metadata', {}).get('category', '').lower() in ('state', 'city', 'us_state', 'us_city')
            ]
            
            if location_terms:
                print(f"      📍 Detected locations: {[t['search_word'] for t in location_terms]}")
            
            # Check term scores
            for term in result.get('terms', []):
                score = term.get('rank_score', 0)
                pos = term.get('pos', 'unknown')
                print(f"         • {term['search_word']}: {score} pts ({pos})")
            
        except Exception as e:
            print_result(False, f"Error: {e}")
            all_passed = False
    
    return all_passed


# =============================================================================
# TEST 3: LOCATION EXTRACTION
# =============================================================================

def test_location_extraction():
    """Test location extraction from queries."""
    print_header("TEST 3: LOCATION EXTRACTION")
    
    try:
        from searchengine.typesense_calculations import (
            extract_location_from_query,
            build_filter_string
        )
        print_result(True, "Location extraction functions imported")
    except ImportError:
        try:
            from typesense_calculations import (
                extract_location_from_query,
                build_filter_string
            )
            print_result(True, "Location extraction functions imported (direct)")
        except ImportError as e:
            print_result(False, f"Import failed: {e}")
            return False
    
    test_cases = [
        ("hbcus in georgia", "georgia"),
        ("restaurants in atlanta", "atlanta"),
        ("schools near charlotte", "charlotte"),
        ("doctors in north carolina", "north carolina"),
        ("hotels at new york", "new york"),
        ("tuskegee airmen", None),  # No location
        ("black history", None),  # No location
    ]
    
    all_passed = True
    
    print("\n   Testing extract_location_from_query():")
    for query, expected in test_cases:
        result = extract_location_from_query(query)
        
        if expected is None:
            success = result is None
            status = "✓" if success else "✗"
            print(f"      {status} '{query}' → {result} (expected: None)")
        else:
            success = result is not None and expected.lower() in result.lower()
            status = "✓" if success else "✗"
            print(f"      {status} '{query}' → {result} (expected: {expected})")
        
        if not success:
            all_passed = False
    
    print("\n   Testing build_filter_string():")
    
    # Test with location
    filter_str = build_filter_string(location="Georgia")
    print(f"      Location='Georgia' → {filter_str}")
    if "location_state:=Georgia" in filter_str:
        print_result(True, "Filter includes location_state")
    else:
        print_result(False, "Filter missing location_state")
        all_passed = False
    
    # Test with filters dict
    filter_str = build_filter_string(filters={'data_type': 'article', 'location': 'Atlanta'})
    print(f"      filters={{data_type, location}} → {filter_str}")
    if "document_data_type:=article" in filter_str and "location" in filter_str.lower():
        print_result(True, "Filter includes both data_type and location")
    else:
        print_result(False, "Filter missing components")
        all_passed = False
    
    return all_passed


# =============================================================================
# TEST 4: TYPESENSE CONNECTION
# =============================================================================

def test_typesense_connection():
    """Test Typesense connection and basic search."""
    print_header("TEST 4: TYPESENSE CONNECTION")
    
    try:
        from searchengine.typesense_calculations import client, COLLECTION_NAME
        print_result(True, "Typesense client imported")
    except ImportError:
        try:
            from typesense_calculations import client, COLLECTION_NAME
            print_result(True, "Typesense client imported (direct)")
        except ImportError as e:
            print_result(False, f"Import failed: {e}")
            return False
    
    print(f"\n   Collection: {COLLECTION_NAME}")
    
    # Test basic search
    print("\n   Testing basic search...")
    try:
        response = client.collections[COLLECTION_NAME].documents.search({
            'q': 'test',
            'query_by': 'document_title',
            'per_page': 1
        })
        
        found = response.get('found', 0)
        print(f"      Found: {found} documents")
        print_result(True, "Basic search works")
        
    except Exception as e:
        print_result(False, f"Search failed: {e}")
        return False
    
    # Test vector search capability
    print("\n   Testing vector search capability...")
    try:
        # Get a sample embedding
        from searchengine.typesense_calculations import get_query_embedding
        
        embedding = get_query_embedding("test query")
        
        if embedding:
            embedding_str = ','.join(str(x) for x in embedding[:10])  # Just first 10 for logging
            print(f"      Embedding generated (first 10): [{embedding_str}...]")
            
            # Try vector search
            response = client.collections[COLLECTION_NAME].documents.search({
                'q': '*',
                'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:5)",
                'per_page': 5,
                'exclude_fields': 'embedding'
            })
            
            found = response.get('found', 0)
            hits = len(response.get('hits', []))
            print(f"      Vector search found: {found} documents, returned: {hits} hits")
            print_result(True, "Vector search works")
        else:
            print_result(False, "Could not generate embedding")
            return False
            
    except Exception as e:
        print_result(False, f"Vector search failed: {e}")
        return False
    
    return True


# =============================================================================
# TEST 5: FULL SEARCH PIPELINE
# =============================================================================

def test_full_search_pipeline():
    """Test the complete search pipeline."""
    print_header("TEST 5: FULL SEARCH PIPELINE")
    
    try:
        from searchengine.typesense_calculations import execute_full_search
        print_result(True, "execute_full_search imported")
    except ImportError:
        try:
            from typesense_calculations import execute_full_search
            print_result(True, "execute_full_search imported (direct)")
        except ImportError as e:
            print_result(False, f"Import failed: {e}")
            return False
    
    test_queries = [
        {
            'query': 'hbcus in georgia',
            'expected_strategy': ['semantic', 'two_stage_strict', 'two_stage_mixed'],
            'check_location': 'georgia'
        },
        {
            'query': 'black doctors',
            'expected_strategy': ['semantic', 'two_stage_strict', 'two_stage_mixed'],
            'check_location': None
        },
        {
            'query': 'tuskegee airmen history',
            'expected_strategy': ['semantic', 'two_stage_strict', 'two_stage_mixed'],
            'check_location': None
        },
    ]
    
    all_passed = True
    
    for test in test_queries:
        query = test['query']
        print(f"\n   Testing: '{query}'")
        
        try:
            t0 = time.time()
            result = execute_full_search(
                query=query,
                session_id='test-session',
                filters=None,
                page=1,
                per_page=10,
                alt_mode='y'  # Force semantic to test embedding
            )
            elapsed = round((time.time() - t0) * 1000, 2)
            
            # Check results
            strategy = result.get('search_strategy', 'unknown')
            results_count = len(result.get('results', []))
            semantic_enabled = result.get('semantic_enabled', False)
            valid_terms = result.get('valid_terms', [])
            
            print(f"      Strategy: {strategy}")
            print(f"      Results: {results_count}")
            print(f"      Semantic enabled: {semantic_enabled}")
            print(f"      Valid terms: {valid_terms}")
            print(f"      Time: {elapsed}ms")
            
            # Check word discovery scores
            wd = result.get('word_discovery', {})
            print(f"      Scores: total={wd.get('total_score', 0)}, avg={wd.get('average_score', 0)}")
            
            # Check filters applied
            filters_applied = result.get('filters_applied', {})
            location_filter = filters_applied.get('location')
            print(f"      Location filter: {location_filter}")
            
            if test['check_location']:
                if location_filter and test['check_location'].lower() in location_filter.lower():
                    print_result(True, f"Location '{test['check_location']}' detected")
                else:
                    print_result(False, f"Location '{test['check_location']}' NOT detected")
                    all_passed = False
            
            # Show top results
            if results_count > 0:
                print("      Top 3 results:")
                for i, r in enumerate(result.get('results', [])[:3]):
                    title = r.get('title', 'No title')[:50]
                    score = r.get('score', 0)
                    loc = r.get('location', {})
                    loc_str = f" [{loc.get('state') or loc.get('city') or ''}]" if any(loc.values()) else ""
                    print(f"         {i+1}. {title}... (score: {score:.3f}){loc_str}")
            
        except Exception as e:
            print_result(False, f"Error: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False
    
    return all_passed


# =============================================================================
# TEST 6: LOCATION FILTERING IN RESULTS
# =============================================================================

def test_location_filtering():
    """Test that location terms actually filter results."""
    print_header("TEST 6: LOCATION FILTERING IN RESULTS")
    
    try:
        from searchengine.typesense_calculations import execute_full_search
    except ImportError:
        from typesense_calculations import execute_full_search
    
    # Search with location
    query_with_location = "hbcus in georgia"
    query_without_location = "hbcus"
    
    print(f"\n1. Searching: '{query_without_location}' (no location)")
    result_without = execute_full_search(
        query=query_without_location,
        session_id='test-session',
        page=1,
        per_page=20,
        alt_mode='y'
    )
    
    print(f"\n2. Searching: '{query_with_location}' (with location)")
    result_with = execute_full_search(
        query=query_with_location,
        session_id='test-session',
        page=1,
        per_page=20,
        alt_mode='y'
    )
    
    # Analyze results
    print("\n3. Comparing results:")
    
    count_without = len(result_without.get('results', []))
    count_with = len(result_with.get('results', []))
    
    print(f"   Without location: {count_without} results")
    print(f"   With location: {count_with} results")
    
    # Check if Georgia results are in the location-filtered search
    georgia_count = 0
    non_georgia_count = 0
    
    for r in result_with.get('results', []):
        loc = r.get('location', {})
        state = (loc.get('state') or '').lower()
        city = (loc.get('city') or '').lower()
        
        if 'georgia' in state or 'atlanta' in city:
            georgia_count += 1
        else:
            non_georgia_count += 1
    
    print(f"\n   In '{query_with_location}' results:")
    print(f"      Georgia results: {georgia_count}")
    print(f"      Non-Georgia results: {non_georgia_count}")
    
    if count_with > 0:
        georgia_percentage = (georgia_count / count_with) * 100
        print(f"      Georgia percentage: {georgia_percentage:.1f}%")
        
        if georgia_percentage > 50:
            print_result(True, "Location filtering is working (>50% Georgia results)")
            return True
        elif georgia_percentage > 20:
            print_result(False, "Location filtering is WEAK (20-50% Georgia results)")
            print("      ⚠ Location is detected but not filtering effectively")
            return False
        else:
            print_result(False, "Location filtering is NOT WORKING (<20% Georgia results)")
            return False
    else:
        print_result(False, "No results returned")
        return False


# =============================================================================
# TEST 7: CHECK TYPESENSE SCHEMA FOR LOCATION FIELDS
# =============================================================================

def test_typesense_schema():
    """Check if Typesense has location fields indexed."""
    print_header("TEST 7: TYPESENSE SCHEMA CHECK")
    
    try:
        from searchengine.typesense_calculations import client, COLLECTION_NAME
    except ImportError:
        from typesense_calculations import client, COLLECTION_NAME
    
    try:
        schema = client.collections[COLLECTION_NAME].retrieve()
        fields = schema.get('fields', [])
        
        print(f"\n   Collection: {COLLECTION_NAME}")
        print(f"   Total fields: {len(fields)}")
        
        # Check for location fields
        location_fields = [f for f in fields if 'location' in f['name'].lower()]
        
        print(f"\n   Location-related fields:")
        for field in location_fields:
            facet = "facet" if field.get('facet') else ""
            index = "indexed" if field.get('index', True) else "not indexed"
            print(f"      • {field['name']}: {field['type']} ({index}) {facet}")
        
        # Check for specific fields we need
        required_fields = ['location_state', 'location_city', 'location_country']
        missing_fields = []
        
        field_names = [f['name'] for f in fields]
        for req in required_fields:
            if req in field_names:
                print_result(True, f"Field '{req}' exists")
            else:
                print_result(False, f"Field '{req}' MISSING")
                missing_fields.append(req)
        
        if missing_fields:
            print(f"\n   ⚠ Missing fields may prevent location filtering!")
            return False
        
        return True
        
    except Exception as e:
        print_result(False, f"Schema check failed: {e}")
        return False


# =============================================================================
# TEST 8: SAMPLE DOCUMENT CHECK
# =============================================================================

def test_sample_documents():
    """Check sample documents for location data."""
    print_header("TEST 8: SAMPLE DOCUMENT CHECK")
    
    try:
        from searchengine.typesense_calculations import client, COLLECTION_NAME
    except ImportError:
        from typesense_calculations import client, COLLECTION_NAME
    
    # Search for documents that should have Georgia
    print("\n   Searching for documents with 'Georgia' in content...")
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search({
            'q': 'georgia hbcu',
            'query_by': 'key_facts,document_title,primary_keywords',
            'per_page': 5,
            'include_fields': 'document_title,location_state,location_city,primary_keywords'
        })
        
        hits = response.get('hits', [])
        print(f"   Found: {len(hits)} documents")
        
        for i, hit in enumerate(hits):
            doc = hit.get('document', {})
            title = doc.get('document_title', 'No title')[:40]
            state = doc.get('location_state', 'None')
            city = doc.get('location_city', 'None')
            keywords = doc.get('primary_keywords', [])[:3]
            
            print(f"\n   {i+1}. {title}...")
            print(f"      State: {state}")
            print(f"      City: {city}")
            print(f"      Keywords: {keywords}")
        
        # Check if any have location data
        docs_with_state = [h for h in hits if h.get('document', {}).get('location_state')]
        
        if docs_with_state:
            print_result(True, f"{len(docs_with_state)}/{len(hits)} documents have location_state")
            return True
        else:
            print_result(False, "No documents have location_state populated!")
            print("      ⚠ Your documents may not have location data indexed")
            return False
            
    except Exception as e:
        print_result(False, f"Document check failed: {e}")
        return False


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 60)
    print("EMBEDDING & TYPESENSE COMPREHENSIVE TEST")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    results = {}
    
    # Run all tests
    results['embedding_service'] = test_embedding_service()
    results['word_discovery'] = test_word_discovery()
    results['location_extraction'] = test_location_extraction()
    results['typesense_connection'] = test_typesense_connection()
    results['full_pipeline'] = test_full_search_pipeline()
    results['location_filtering'] = test_location_filtering()
    results['typesense_schema'] = test_typesense_schema()
    results['sample_documents'] = test_sample_documents()
    
    # Summary
    print_header("FINAL SUMMARY")
    
    passed = 0
    failed = 0
    
    for test_name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    print(f"\n   Total: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed. Review the output above.")
        
        # Provide hints based on failures
        print("\n   TROUBLESHOOTING HINTS:")
        
        if not results.get('embedding_service'):
            print("   • Check EMBEDDING_SERVICE_URL in your .env file")
            print("   • Verify the embedding service is running on your droplet")
            print("   • Check firewall allows port 8001")
        
        if not results.get('location_filtering'):
            print("   • Location terms are detected but not filtering results")
            print("   • Check build_filter_string() is adding location filters")
            print("   • Verify documents have location_state/location_city populated")
        
        if not results.get('sample_documents'):
            print("   • Documents may not have location data indexed")
            print("   • Re-index documents with location fields populated")
    
    print("=" * 60)
    
    return all(results.values())


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)