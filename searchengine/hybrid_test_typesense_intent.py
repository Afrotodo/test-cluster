#!/usr/bin/env python
"""
hybrid_test_typesense_synonyms.py
==================================
Test Typesense SYNONYMS API approach.

Why synonyms? The V2 test proved:
- Individual terms work great (restaurant=9, cafe=2, tea house=1, food=13)
- Combining terms in the query BREAKS results (Typesense requires ALL tokens)
- Synonyms handle OR expansion SERVER-SIDE without breaking anything

This script will:
1. Show current results for "restaurants" (baseline)
2. Create synonyms in Typesense (reversible!)
3. Re-test the same queries to show improvement
4. Give you the option to KEEP or REMOVE the synonyms

Run: python hybrid_test_typesense_synonyms.py
"""

import os
import sys
import time
import json
from datetime import datetime
from typing import Dict, List, Any
from collections import defaultdict

# Add project paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')
import django
django.setup()


# =============================================================================
# TYPESENSE CONNECTION
# =============================================================================

print("=" * 70)
print("🔧 TYPESENSE SYNONYMS TEST")
print("=" * 70)

import typesense
from decouple import config

ts_client = typesense.Client({
    'api_key': config('TYPESENSE_API_KEY'),
    'nodes': [{
        'host': config('TYPESENSE_HOST'),
        'port': config('TYPESENSE_PORT'),
        'protocol': config('TYPESENSE_PROTOCOL')
    }],
    'connection_timeout_seconds': 5
})

COLLECTION_NAME = 'documents'

try:
    ts_client.collections[COLLECTION_NAME].retrieve()
    print("✅ Typesense connected")
except Exception as e:
    print(f"❌ Typesense not available: {e}")
    sys.exit(1)


# =============================================================================
# SYNONYMS TO CREATE
# =============================================================================

# One-way synonyms: searching "restaurants" ALSO finds these terms
# But searching "tea house" does NOT return all restaurants
SYNONYMS_TO_CREATE = [
    {
        'id': 'dining-places',
        'type': 'one_way',
        'root': 'restaurants',
        'synonyms': ['restaurant', 'cafe', 'tea house', 'bakery', 'bistro',
                      'diner', 'eatery', 'dining', 'soul food', 'coffee shop',
                      'tea company', 'brewery'],
    },
    {
        'id': 'dining-places-singular',
        'type': 'one_way',
        'root': 'restaurant',
        'synonyms': ['cafe', 'tea house', 'bakery', 'bistro', 'diner',
                      'eatery', 'dining', 'soul food', 'coffee shop',
                      'tea company', 'brewery'],
    },
    {
        'id': 'cafes-places',
        'type': 'one_way',
        'root': 'cafes',
        'synonyms': ['cafe', 'coffee shop', 'tea house', 'tea company', 'bakery'],
    },
    {
        'id': 'cafes-places-singular',
        'type': 'one_way',
        'root': 'cafe',
        'synonyms': ['coffee shop', 'tea house', 'tea company', 'bakery'],
    },
    {
        'id': 'hotels-lodging',
        'type': 'one_way',
        'root': 'hotels',
        'synonyms': ['hotel', 'motel', 'inn', 'resort', 'lodge', 'airbnb'],
    },
    {
        'id': 'salons-beauty',
        'type': 'one_way',
        'root': 'salons',
        'synonyms': ['salon', 'barbershop', 'spa', 'beauty', 'hair'],
    },
    {
        'id': 'salons-beauty-singular',
        'type': 'one_way',
        'root': 'salon',
        'synonyms': ['barbershop', 'spa', 'beauty', 'hair'],
    },
]


# =============================================================================
# UTILITY
# =============================================================================

class Timer:
    def __init__(self):
        self.start_time = None
        self.elapsed = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.start_time

    @property
    def ms(self):
        return (self.elapsed or 0) * 1000

    def __str__(self):
        return f"{self.ms:.1f}ms"


def ts_search(q, filter_by=None, per_page=50):
    """Search Typesense."""
    params = {
        'q': q,
        'query_by': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
        'query_by_weights': '10,8,6,4,3',
        'per_page': per_page,
        'page': 1,
        'num_typos': 1,
        'drop_tokens_threshold': 0,
        'include_fields': 'document_uuid,document_title,document_data_type,document_category,location_city',
    }
    if filter_by:
        params['filter_by'] = filter_by
    return ts_client.collections[COLLECTION_NAME].documents.search(params)


def extract_results(resp):
    """Extract results into a simple list."""
    results = []
    for hit in resp.get('hits', []):
        doc = hit.get('document', {})
        results.append({
            'id': doc.get('document_uuid', ''),
            'title': doc.get('document_title', 'Untitled'),
            'type': doc.get('document_data_type', ''),
            'category': doc.get('document_category', ''),
            'city': doc.get('location_city', ''),
        })
    return results


def print_results(results, max_show=20):
    """Print results list."""
    if not results:
        print(f"      (no results)")
        return
    for i, r in enumerate(results[:max_show], 1):
        print(f"      {i:>2}. [{r['type']}] {r['title'][:55]}")


def get_ids(results):
    """Get set of IDs."""
    return {r['id'] for r in results}


# =============================================================================
# STEP 1: BASELINE — Current results (before synonyms)
# =============================================================================

def step1_baseline():
    """Record current search results as baseline."""
    print(f"\n{'─' * 70}")
    print("STEP 1: BASELINE — Current results (before synonyms)")
    print('─' * 70)

    test_queries = [
        ('restaurants', 'location_city:=Atlanta'),
        ('restaurant', 'location_city:=Atlanta'),
        ('cafes', 'location_city:=Atlanta'),
        ('cafe', 'location_city:=Atlanta'),
        ('hotels', 'location_city:=Atlanta'),
        ('salons', 'location_city:=Atlanta'),
        ('best restaurants', 'location_city:=Atlanta'),
        ('black owned restaurants', 'location_city:=Atlanta'),
    ]

    baselines = {}

    for q, filter_by in test_queries:
        label = f"'{q}' + Atlanta"
        try:
            resp = ts_search(q, filter_by=filter_by)
            results = extract_results(resp)
            found = resp.get('found', 0)
            baselines[q] = {'results': results, 'found': found, 'ids': get_ids(results)}

            print(f"\n   🔍 {label}: {found} found")
            print_results(results, max_show=5)

        except Exception as e:
            print(f"\n   ❌ {label}: Error - {e}")
            baselines[q] = {'results': [], 'found': 0, 'ids': set()}

    return baselines


# =============================================================================
# STEP 2: CHECK EXISTING SYNONYMS
# =============================================================================

def step2_check_existing_synonyms():
    """Check what synonyms already exist."""
    print(f"\n{'─' * 70}")
    print("STEP 2: CHECK EXISTING SYNONYMS")
    print('─' * 70)

    try:
        existing = ts_client.collections[COLLECTION_NAME].synonyms.retrieve()
        synonyms_list = existing.get('synonyms', [])

        if synonyms_list:
            print(f"\n   Found {len(synonyms_list)} existing synonym(s):")
            for syn in synonyms_list:
                syn_id = syn.get('id', '?')
                root = syn.get('root', '')
                syns = syn.get('synonyms', [])
                print(f"      • {syn_id}: root='{root}' → {syns}")
        else:
            print("\n   No existing synonyms found. Clean slate.")

        return synonyms_list

    except Exception as e:
        print(f"\n   ❌ Error checking synonyms: {e}")
        return []


# =============================================================================
# STEP 3: CREATE SYNONYMS
# =============================================================================

def step3_create_synonyms():
    """Create the synonym rules in Typesense."""
    print(f"\n{'─' * 70}")
    print("STEP 3: CREATE SYNONYMS")
    print('─' * 70)

    created = []
    failed = []

    for syn_config in SYNONYMS_TO_CREATE:
        syn_id = syn_config['id']
        syn_type = syn_config.get('type', 'one_way')

        if syn_type == 'one_way':
            payload = {
                'root': syn_config['root'],
                'synonyms': syn_config['synonyms'],
            }
        else:
            payload = {
                'synonyms': syn_config['synonyms'],
            }

        try:
            result = ts_client.collections[COLLECTION_NAME].synonyms.upsert(
                syn_id, payload
            )
            created.append(syn_id)
            root = syn_config.get('root', 'multi-way')
            print(f"   ✅ Created '{syn_id}': '{root}' → {syn_config['synonyms'][:4]}...")

        except Exception as e:
            failed.append((syn_id, str(e)))
            print(f"   ❌ Failed '{syn_id}': {e}")

    print(f"\n   Created: {len(created)} | Failed: {len(failed)}")

    # Verify
    try:
        all_syns = ts_client.collections[COLLECTION_NAME].synonyms.retrieve()
        total = len(all_syns.get('synonyms', []))
        print(f"   Total synonyms now in Typesense: {total}")
    except:
        pass

    return created, failed


# =============================================================================
# STEP 4: TEST WITH SYNONYMS — Same queries, new results?
# =============================================================================

def step4_test_with_synonyms(baselines):
    """Re-run the same queries now that synonyms are active."""
    print(f"\n{'─' * 70}")
    print("STEP 4: TEST WITH SYNONYMS — Compare to baseline")
    print('─' * 70)

    # Small delay to let synonyms take effect
    time.sleep(0.5)

    test_queries = [
        ('restaurants', 'location_city:=Atlanta'),
        ('restaurant', 'location_city:=Atlanta'),
        ('cafes', 'location_city:=Atlanta'),
        ('cafe', 'location_city:=Atlanta'),
        ('hotels', 'location_city:=Atlanta'),
        ('salons', 'location_city:=Atlanta'),
        ('best restaurants', 'location_city:=Atlanta'),
        ('black owned restaurants', 'location_city:=Atlanta'),
    ]

    comparisons = []

    for q, filter_by in test_queries:
        baseline = baselines.get(q, {'results': [], 'found': 0, 'ids': set()})
        old_found = baseline['found']
        old_ids = baseline['ids']

        try:
            with Timer() as t:
                resp = ts_search(q, filter_by=filter_by)
            new_results = extract_results(resp)
            new_found = resp.get('found', 0)
            new_ids = get_ids(new_results)

            kept = old_ids & new_ids
            gained = new_ids - old_ids
            lost = old_ids - new_ids

            diff = new_found - old_found
            diff_str = f"+{diff}" if diff >= 0 else str(diff)
            status = "✅" if len(lost) == 0 and new_found >= old_found else "⚠️"

            print(f"\n   {status} '{q}' + Atlanta: {old_found} → {new_found} ({diff_str}) | {t}")

            if gained:
                print(f"      🆕 GAINED:")
                for r in new_results:
                    if r['id'] in gained:
                        print(f"         + [{r['type']}] {r['title'][:50]}")

            if lost:
                print(f"      ⚠️  LOST:")
                for r in baseline['results']:
                    if r['id'] in lost:
                        print(f"         - [{r['type']}] {r['title'][:50]}")

            if new_found > 5:
                print(f"      📄 All results ({len(new_results)}):")
                print_results(new_results, max_show=10)

            comparisons.append({
                'query': q,
                'old_found': old_found,
                'new_found': new_found,
                'kept': len(kept),
                'gained': len(gained),
                'lost': len(lost),
                'status': 'PASS' if len(lost) == 0 else 'FAIL',
            })

        except Exception as e:
            print(f"\n   ❌ '{q}': Error - {e}")
            comparisons.append({
                'query': q, 'old_found': old_found, 'new_found': 0,
                'kept': 0, 'gained': 0, 'lost': old_found, 'status': 'ERROR',
            })

    # Summary table
    print(f"\n\n   {'═' * 70}")
    print(f"   RESULTS COMPARISON: Before vs After Synonyms")
    print(f"   {'═' * 70}")
    print(f"\n   {'Query':<35} {'Before':<8} {'After':<8} {'Kept':<6} {'Gain':<6} {'Lost':<6} {'Status'}")
    print(f"   {'─' * 80}")

    all_pass = True
    total_gained = 0
    for c in comparisons:
        status_icon = "✅" if c['status'] == 'PASS' else "⚠️"
        print(f"   {c['query']:<35} {c['old_found']:<8} {c['new_found']:<8} "
              f"{c['kept']:<6} {c['gained']:<6} {c['lost']:<6} {status_icon} {c['status']}")
        if c['status'] != 'PASS':
            all_pass = False
        total_gained += c['gained']

    print(f"\n   Total new results gained: {total_gained}")

    return comparisons, all_pass


# =============================================================================
# STEP 5: DECISION — Keep or Remove?
# =============================================================================

def step5_decision(all_pass, comparisons):
    """Final decision and instructions."""
    print(f"\n{'─' * 70}")
    print("STEP 5: DECISION")
    print('─' * 70)

    if all_pass:
        print(f"""
   ✅ SYNONYMS ARE WORKING PERFECTLY

   - Zero original results lost
   - New results discovered
   - No code changes needed in your bridge/redis/word_discovery
   - Synonyms are one-way: "restaurants" finds tea houses,
     but "tea house" doesn't return all restaurants

   The synonyms are NOW LIVE in your Typesense collection.
   Your search will immediately benefit from this.

   To manage synonyms later:
   ─────────────────────────
   # List all synonyms
   ts_client.collections['{COLLECTION_NAME}'].synonyms.retrieve()

   # Delete a specific synonym
   ts_client.collections['{COLLECTION_NAME}'].synonyms['dining-places'].delete()

   # Add new synonym
   ts_client.collections['{COLLECTION_NAME}'].synonyms.upsert(
       'new-synonym-id',
       {{'root': 'stores', 'synonyms': ['shop', 'boutique', 'market']}}
   )
        """)
    else:
        print(f"""
   ⚠️  SOME RESULTS WERE LOST

   The synonyms may need tuning. You can:
   1. Keep them and adjust (some lost results may be low-relevance)
   2. Remove them and try a different approach

   To REMOVE all synonyms created by this test:
        """)
        print(f"   Run this to remove:")
        for syn in SYNONYMS_TO_CREATE:
            print(f"      ts_client.collections['{COLLECTION_NAME}'].synonyms['{syn['id']}'].delete()")

    # Always provide removal instructions
    print(f"\n   To REMOVE all test synonyms if needed:")
    print(f"   ────────────────────────────────────────")
    for syn in SYNONYMS_TO_CREATE:
        print(f"   ts_client.collections['{COLLECTION_NAME}'].synonyms['{syn['id']}'].delete()")


# =============================================================================
# CLEANUP FUNCTION (can be called separately)
# =============================================================================

def cleanup_synonyms():
    """Remove all synonyms created by this test."""
    print("\n🧹 Removing test synonyms...")
    for syn in SYNONYMS_TO_CREATE:
        try:
            ts_client.collections[COLLECTION_NAME].synonyms[syn['id']].delete()
            print(f"   ✅ Removed '{syn['id']}'")
        except Exception as e:
            print(f"   ⚠️  '{syn['id']}': {e}")
    print("   Done.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print(f"\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print("""
This script will:
  1. Record baseline search results (before synonyms)
  2. Check for existing synonyms
  3. Create one-way synonyms in Typesense
  4. Re-test and compare results
  5. Tell you if it's safe to keep them

⚠️  This CREATES synonyms in your Typesense collection.
    They can be removed at the end if results are bad.
    """)

    # Check for --cleanup flag
    if '--cleanup' in sys.argv:
        cleanup_synonyms()
        return

    overall_start = time.perf_counter()

    # Step 1: Baseline
    baselines = step1_baseline()

    # Step 2: Check existing
    existing = step2_check_existing_synonyms()

    # Step 3: Create synonyms
    created, failed = step3_create_synonyms()

    if not created:
        print("\n   ❌ No synonyms were created. Cannot proceed.")
        return

    # Step 4: Test with synonyms
    comparisons, all_pass = step4_test_with_synonyms(baselines)

    # Step 5: Decision
    step5_decision(all_pass, comparisons)

    overall_elapsed = time.perf_counter() - overall_start
    print("\n" + "=" * 70)
    print(f"Total test time: {overall_elapsed:.1f} seconds")
    print("=" * 70)

    # If not all pass, offer to cleanup
    if not all_pass:
        print("\n⚠️  To remove the synonyms, run:")
        print(f"   python {os.path.basename(__file__)} --cleanup")


if __name__ == "__main__":
    main()