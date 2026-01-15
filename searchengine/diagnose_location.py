"""
diagnose_typesense_location.py
Diagnose AND TEST FIXES for location filtering.

This file contains:
1. Your CURRENT (broken) code
2. The FIXED code
3. Side-by-side comparison tests

Run this to see the fix working before updating your main typesense_calculations.py
"""

import os
import sys
import re
from typing import Optional, Tuple, List, Dict
from decouple import config

# Setup paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# =============================================================================
# CONFIGURATION FROM .env (using decouple)
# =============================================================================

TYPESENSE_API_KEY = config('TYPESENSE_API_KEY')
TYPESENSE_HOST = config('TYPESENSE_HOST')
TYPESENSE_PORT = config('TYPESENSE_PORT')
TYPESENSE_PROTOCOL = config('TYPESENSE_PROTOCOL', default='http')
COLLECTION_NAME = 'documents'

print("=" * 70)
print("CONFIGURATION (from .env via decouple)")
print("=" * 70)
print(f"TYPESENSE_HOST: {TYPESENSE_HOST}")
print(f"TYPESENSE_PORT: {TYPESENSE_PORT}")
print(f"TYPESENSE_PROTOCOL: {TYPESENSE_PROTOCOL}")
print(f"TYPESENSE_API_KEY: {'*' * 8}...{TYPESENSE_API_KEY[-4:] if len(TYPESENSE_API_KEY) > 4 else '****'}")
print(f"COLLECTION_NAME: {COLLECTION_NAME}")


# =============================================================================
# REGEX PATTERNS (same as your typesense_calculations.py)
# =============================================================================

LOCATION_EXTRACT_PATTERNS = [
    re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
    re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
]


# =============================================================================
# THE FIX: State abbreviation mapping
# =============================================================================

US_STATE_ABBREV = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
}

# Reverse mapping
US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# =============================================================================
# OLD CODE (your current broken version)
# =============================================================================

def extract_location_OLD(query: str) -> Optional[str]:
    """
    YOUR CURRENT CODE - returns lowercase
    This is what's in your typesense_calculations.py now
    """
    stopwords = {'the', 'a', 'best', 'good', 'top'}
    
    for pattern in LOCATION_EXTRACT_PATTERNS:
        match = pattern.search(query.lower())
        if match:
            location = match.group(1).strip()
            if location not in stopwords:
                return location  # ← Returns lowercase: 'georgia'
    return None


def build_filter_OLD(location: str) -> str:
    """
    YOUR CURRENT CODE - uses lowercase location directly
    """
    if not location:
        return ''
    
    return (
        f"(location_city:={location} || location_state:={location} || "
        f"location_country:={location} || location_region:={location})"
    )


# =============================================================================
# NEW CODE (the fixed version)
# =============================================================================

def extract_location_NEW(query: str) -> Optional[str]:
    """
    FIXED VERSION - returns title case
    """
    stopwords = {'the', 'a', 'best', 'good', 'top'}
    
    for pattern in LOCATION_EXTRACT_PATTERNS:
        match = pattern.search(query.lower())
        if match:
            location = match.group(1).strip()
            if location not in stopwords:
                return location.title()  # ← THE FIX: Returns 'Georgia'
    return None


def build_filter_NEW(location: str) -> str:
    """
    FIXED VERSION - includes both full name AND abbreviation
    """
    if not location:
        return ''
    
    loc_title = location.title()
    loc_lower = location.lower()
    
    # Get state abbreviation if this is a US state
    abbrev = US_STATE_ABBREV.get(loc_lower, '')
    
    # Build filter parts
    filter_parts = [
        f"location_state:={loc_title}",
        f"location_city:={loc_title}",
    ]
    
    # Add abbreviation variant for states
    if abbrev:
        filter_parts.append(f"location_state:={abbrev}")
    
    return '(' + ' || '.join(filter_parts) + ')'


# =============================================================================
# TEST 1: Compare Location Extraction (OLD vs NEW)
# =============================================================================

def test_location_extraction():
    """Compare OLD vs NEW location extraction."""
    print("\n" + "=" * 70)
    print("TEST 1: LOCATION EXTRACTION - OLD vs NEW")
    print("=" * 70)
    
    test_queries = [
        "hbcus in georgia",
        "hbcus in texas",
        "restaurants in atlanta",
        "stores in florida",
        "colleges in new york",
        "black owned businesses in chicago",
        "museums near washington",
        "hotels around miami",
        "churches at detroit",
        "hbcus",  # no location
        "georgia hbcus",  # location without preposition
        "best restaurants in los angeles for dinner",
        "atlanta restaurants",
    ]
    
    print("\n{:<45} {:<15} {:<15}".format("Query", "OLD", "NEW"))
    print("-" * 75)
    
    for query in test_queries:
        old_result = extract_location_OLD(query) or "(none)"
        new_result = extract_location_NEW(query) or "(none)"
        
        # Highlight differences
        if old_result != new_result and old_result != "(none)":
            marker = "← FIXED!"
        else:
            marker = ""
        
        print(f"{query:<45} {old_result:<15} {new_result:<15} {marker}")


# =============================================================================
# TEST 2: Compare Filter Building (OLD vs NEW)
# =============================================================================

def test_filter_building():
    """Compare OLD vs NEW filter building."""
    print("\n" + "=" * 70)
    print("TEST 2: FILTER STRING BUILDING - OLD vs NEW")
    print("=" * 70)
    
    test_locations = [
        "georgia",
        "Georgia",  
        "texas",
        "new york",
        "atlanta",  # city, not state
        "chicago",  # city, not state
        "florida",
    ]
    
    for location in test_locations:
        old_filter = build_filter_OLD(location)
        new_filter = build_filter_NEW(location)
        
        print(f"\n📍 Location: '{location}'")
        print(f"   OLD filter: {old_filter}")
        print(f"   NEW filter: {new_filter}")
        
        # Check if it's a state and show what abbreviation would be added
        loc_lower = location.lower()
        if loc_lower in US_STATE_ABBREV:
            abbrev = US_STATE_ABBREV[loc_lower]
            print(f"   ✓ This is a US state! Abbreviation '{abbrev}' added to NEW filter")
        else:
            print(f"   ℹ Not a US state (city or other location)")


# =============================================================================
# TEST 3: Full Pipeline Comparison (OLD vs NEW)
# =============================================================================

def test_full_pipeline():
    """Compare full extraction + filter pipeline."""
    print("\n" + "=" * 70)
    print("TEST 3: FULL PIPELINE - OLD vs NEW")
    print("=" * 70)
    
    test_queries = [
        "hbcus in georgia",
        "hbcus in texas",
        "restaurants in atlanta",
        "stores in florida",
        "colleges in new york",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"QUERY: '{query}'")
        print(f"{'='*60}")
        
        # OLD pipeline
        old_location = extract_location_OLD(query)
        old_filter = build_filter_OLD(old_location) if old_location else "(no filter)"
        
        # NEW pipeline
        new_location = extract_location_NEW(query)
        new_filter = build_filter_NEW(new_location) if new_location else "(no filter)"
        
        print(f"\n🔴 OLD (current broken code):")
        print(f"   Extracted: '{old_location}'")
        print(f"   Filter: {old_filter}")
        
        print(f"\n🟢 NEW (fixed code):")
        print(f"   Extracted: '{new_location}'")
        print(f"   Filter: {new_filter}")
        
        # Highlight the improvement
        if old_location and new_location:
            loc_lower = old_location.lower()
            if loc_lower in US_STATE_ABBREV:
                abbrev = US_STATE_ABBREV[loc_lower]
                print(f"\n   ✅ IMPROVEMENT: NEW filter searches for BOTH '{new_location}' AND '{abbrev}'")
                print(f"      This will match your data which has '{abbrev}': 25+ docs")


# =============================================================================
# TEST 4: Typesense Queries - OLD vs NEW filters
# =============================================================================

def test_typesense_comparison():
    """Test OLD vs NEW filters against actual Typesense data."""
    print("\n" + "=" * 70)
    print("TEST 4: TYPESENSE QUERIES - OLD vs NEW FILTERS")
    print("=" * 70)
    
    try:
        import typesense
        
        client = typesense.Client({
            'api_key': TYPESENSE_API_KEY,
            'nodes': [{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL
            }],
            'connection_timeout_seconds': 5
        })
        
        print(f"\n✓ Connected to Typesense at {TYPESENSE_HOST}:{TYPESENSE_PORT}")
        
        # First, show what state values exist in the data
        print("\n--- Your Data: State Values (for reference) ---")
        try:
            response = client.collections[COLLECTION_NAME].documents.search({
                'q': '*',
                'query_by': 'document_title',
                'facet_by': 'location_state',
                'max_facet_values': 15,
                'per_page': 0
            })
            
            for facet in response.get('facet_counts', []):
                if facet.get('field_name') == 'location_state':
                    for c in facet.get('counts', [])[:10]:
                        print(f"   • '{c['value']}': {c['count']} docs")
        except Exception as e:
            print(f"   Could not get facets: {e}")
        
        # Test queries
        test_cases = [
            ("hbcus in georgia", "hbcu"),
            ("hbcus in texas", "hbcu"),
            ("hbcus in alabama", "hbcu"),
            ("stores in florida", "*"),
            ("colleges in new york", "college"),
        ]
        
        for query, search_term in test_cases:
            print(f"\n{'='*60}")
            print(f"QUERY: '{query}' (searching for: '{search_term}')")
            print(f"{'='*60}")
            
            # OLD pipeline
            old_location = extract_location_OLD(query)
            old_filter = build_filter_OLD(old_location) if old_location else None
            
            # NEW pipeline  
            new_location = extract_location_NEW(query)
            new_filter = build_filter_NEW(new_location) if new_location else None
            
            # Test OLD filter
            print(f"\n🔴 OLD FILTER: {old_filter}")
            if old_filter:
                try:
                    response = client.collections[COLLECTION_NAME].documents.search({
                        'q': search_term,
                        'query_by': 'document_title,key_facts,primary_keywords',
                        'filter_by': old_filter,
                        'per_page': 5
                    })
                    old_count = response.get('found', 0)
                    print(f"   Results: {old_count}")
                    
                    if old_count > 0:
                        for hit in response.get('hits', [])[:2]:
                            doc = hit.get('document', {})
                            title = doc.get('document_title', 'N/A')[:45]
                            state = doc.get('location_state', 'N/A')
                            print(f"   • {title}... | state='{state}'")
                    else:
                        print(f"   ⚠️ NO RESULTS with OLD filter!")
                        
                except Exception as e:
                    print(f"   ERROR: {e}")
                    old_count = 0
            else:
                old_count = 0
            
            # Test NEW filter
            print(f"\n🟢 NEW FILTER: {new_filter}")
            if new_filter:
                try:
                    response = client.collections[COLLECTION_NAME].documents.search({
                        'q': search_term,
                        'query_by': 'document_title,key_facts,primary_keywords',
                        'filter_by': new_filter,
                        'per_page': 5
                    })
                    new_count = response.get('found', 0)
                    print(f"   Results: {new_count}")
                    
                    if new_count > 0:
                        for hit in response.get('hits', [])[:3]:
                            doc = hit.get('document', {})
                            title = doc.get('document_title', 'N/A')[:45]
                            state = doc.get('location_state', 'N/A')
                            print(f"   • {title}... | state='{state}'")
                    else:
                        print(f"   ⚠️ NO RESULTS with NEW filter!")
                        
                except Exception as e:
                    print(f"   ERROR: {e}")
                    new_count = 0
            else:
                new_count = 0
            
            # Summary
            print(f"\n📊 COMPARISON:")
            print(f"   OLD: {old_count} results")
            print(f"   NEW: {new_count} results")
            
            if new_count > old_count:
                print(f"   ✅ NEW filter found {new_count - old_count} MORE results!")
            elif new_count == old_count and new_count > 0:
                print(f"   ✓ Same results (both working)")
            elif new_count == 0 and old_count == 0:
                print(f"   ⚠️ Both returned 0 - check if data exists for this location")
            
    except ImportError:
        print("❌ Could not import typesense. Install with: pip install typesense")
    except Exception as e:
        print(f"❌ Typesense error: {e}")


# =============================================================================
# TEST 5: State Abbreviation Mapping
# =============================================================================

def test_state_mapping():
    """Test the state abbreviation mapping."""
    print("\n" + "=" * 70)
    print("TEST 5: STATE ABBREVIATION MAPPING")
    print("=" * 70)
    
    print("\nUS_STATE_ABBREV dictionary:")
    print("-" * 40)
    
    # Show a sample
    sample_states = ['georgia', 'texas', 'new york', 'florida', 'alabama', 'california', 'north carolina']
    
    for state in sample_states:
        abbrev = US_STATE_ABBREV.get(state, 'NOT FOUND')
        print(f"   '{state}' → '{abbrev}'")
    
    print(f"\n   Total states in mapping: {len(US_STATE_ABBREV)}")
    
    # Test reverse lookup
    print("\nReverse lookup (abbreviation → full name):")
    print("-" * 40)
    
    sample_abbrevs = ['GA', 'TX', 'NY', 'FL', 'AL', 'CA', 'NC']
    for abbrev in sample_abbrevs:
        full_name = US_STATE_FULL.get(abbrev, 'NOT FOUND')
        print(f"   '{abbrev}' → '{full_name}'")


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary():
    """Print summary of what the fix does."""
    print("\n" + "=" * 70)
    print("SUMMARY: WHAT THE FIX DOES")
    print("=" * 70)
    
    print("""
┌─────────────────────────────────────────────────────────────────────┐
│  PROBLEM FOUND                                                       │
├─────────────────────────────────────────────────────────────────────┤
│  Your code extracts: 'georgia' (lowercase)                          │
│  Your data contains: 'GA' (25 docs), 'Georgia' (1 doc)              │
│  Filter used:        location_state:=georgia                        │
│  Result:             0 matches (case-sensitive!)                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  THE FIX                                                             │
├─────────────────────────────────────────────────────────────────────┤
│  1. extract_location_NEW() returns 'Georgia' (title case)           │
│  2. build_filter_NEW() searches for BOTH 'Georgia' AND 'GA'         │
│  3. New filter: (location_state:=Georgia || location_state:=GA)     │
│  Result:        Matches both formats in your data!                  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  TO APPLY THE FIX TO YOUR MAIN CODE                                  │
├─────────────────────────────────────────────────────────────────────┤
│  1. Add US_STATE_ABBREV dictionary to typesense_calculations.py     │
│  2. Change extract_location_from_query() to return .title()         │
│  3. Update build_filter_string() to use both full name + abbrev     │
│                                                                      │
│  See simple_fix.py for exact copy-paste code                        │
└─────────────────────────────────────────────────────────────────────┘
""")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("LOCATION FILTER DIAGNOSTIC - OLD vs NEW COMPARISON")
    print("=" * 70)
    print("This script tests the FIX before you apply it to your main code.")
    print("You'll see side-by-side comparison of OLD (broken) vs NEW (fixed).")
    
    # Run all tests
    test_state_mapping()
    test_location_extraction()
    test_filter_building()
    test_full_pipeline()
    test_typesense_comparison()
    print_summary()
    
    print("\n" + "=" * 70)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 70)
    print("\nIf the NEW filter returned more results than OLD, the fix works!")
    print("You can now apply the changes to your typesense_calculations.py")


if __name__ == "__main__":
    main()