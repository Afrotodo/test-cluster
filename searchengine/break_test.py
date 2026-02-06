"""
BREAK TEST: Diagnose why keyword path returns 0 results
========================================================
Run this from your Django project directory (where .env file is):

    cd /path/to/your/project
    source venv/bin/activate
    python break_test.py

Make sure:
    1. Your .env file is in the same directory
    2. Update TYPESENSE_COLLECTION below if it's not in your .env
       (or add TYPESENSE_COLLECTION=your_collection_name to .env)
"""

import sys
import os
import json
import time
from decouple import config

# =============================================================================
# CONFIG - loaded from .env file
# =============================================================================

# Redis
REDIS_HOST = config('REDIS_LOCATION')
REDIS_PORT = config('REDIS_PORT', cast=int)
REDIS_PASSWORD = config('REDIS_PASSWORD')
REDIS_USERNAME = config('REDIS_USERNAME', default='default')
REDIS_DB = config('REDIS_DB', default=0, cast=int)

# Typesense
TYPESENSE_HOST = config('TYPESENSE_HOST')
TYPESENSE_PORT = config('TYPESENSE_PORT')
TYPESENSE_PROTOCOL = config('TYPESENSE_PROTOCOL', default='http')
TYPESENSE_API_KEY = config('TYPESENSE_API_KEY')

# Collection name - UPDATE THIS to match your actual collection name
TYPESENSE_COLLECTION = config('TYPESENSE_COLLECTION', default='documents')

# Test query
TEST_QUERY = "black women"

# =============================================================================
# END CONFIG
# =============================================================================

print("=" * 70)
print("BREAK TEST - Diagnosing keyword path failure")
print("=" * 70)
print()

errors = []

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Redis Connection & Index
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 1: Redis Connection & Index Status")
print("═" * 70)

try:
    import redis
    from redis.commands.search.query import Query

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        username=REDIS_USERNAME,
        db=REDIS_DB,
        decode_responses=True
    )
    print(f"✅ Connected to Redis: {r.ping()}")

    # Index info
    info = r.ft("terms_idx").info()
    print(f"✅ Index 'terms_idx' exists")
    print(f"   Documents: {info['num_docs']}")
    print(f"   Indexing:  {info.get('indexing', 0)} (0 = complete)")

    # Show schema
    print(f"\n   Schema fields:")
    attributes = info.get('attributes', [])
    for attr in attributes:
        if isinstance(attr, list):
            name = attr[0] if len(attr) > 0 else '?'
            field_type = attr[2] if len(attr) > 2 else '?'
            print(f"     - {name}: {field_type}")
        elif isinstance(attr, dict):
            print(f"     - {attr.get('identifier', '?')}: {attr.get('type', '?')}")

except Exception as e:
    print(f"❌ Redis error: {e}")
    errors.append(f"Redis: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: Redis term lookups
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 2: Redis Term Lookups")
print("═" * 70)

try:
    test_terms = TEST_QUERY.split()

    for term in test_terms:
        print(f"\n  Testing term: '{term}'")

        # Check if hash key exists
        key = f"term:{term}"
        exists = r.exists(key)
        print(f"    Hash key '{key}': {'✅ EXISTS' if exists else '❌ MISSING'}")
        if exists:
            data = r.hgetall(key)
            print(f"    Data: {json.dumps(data, indent=6)}")

        # Search by exact match on term field
        try:
            escaped = term.replace("-", "\\-").replace("'", "\\'")
            result = r.ft("terms_idx").search(
                Query(f"@term:{{{escaped}}}").paging(0, 3)
            )
            print(f"    @term exact search: {result.total} results")
        except Exception as e:
            print(f"    @term exact search error: {e}")

        # Search by prefix
        try:
            result = r.ft("terms_idx").search(
                Query(f"@term:{term}*").paging(0, 3)
            )
            print(f"    @term prefix search: {result.total} results")
        except Exception as e:
            print(f"    @term prefix search error: {e}")

        # Wildcard text search (searches all TEXT fields)
        try:
            result = r.ft("terms_idx").search(
                Query(term).paging(0, 3)
            )
            print(f"    Full-text search: {result.total} results")
            if result.docs:
                doc = result.docs[0]
                print(f"    First match: term='{getattr(doc, 'term', '?')}', "
                      f"category='{getattr(doc, 'category', '?')}'")
        except Exception as e:
            print(f"    Full-text search error: {e}")

    # Combined term search
    print(f"\n  Testing combined: '{TEST_QUERY}'")
    try:
        result = r.ft("terms_idx").search(
            Query(TEST_QUERY).paging(0, 5)
        )
        print(f"    Full-text search '{TEST_QUERY}': {result.total} results")
    except Exception as e:
        print(f"    Full-text search error: {e}")

except Exception as e:
    print(f"❌ Redis lookup error: {e}")
    errors.append(f"Redis lookup: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: Typesense Connection
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 3: Typesense Connection & Collection")
print("═" * 70)

try:
    import typesense

    ts_client = typesense.Client({
        'nodes': [{
            'host': TYPESENSE_HOST,
            'port': TYPESENSE_PORT,
            'protocol': TYPESENSE_PROTOCOL,
        }],
        'api_key': TYPESENSE_API_KEY,
        'connection_timeout_seconds': 10,
    })

    # Check collection
    collection = ts_client.collections[TYPESENSE_COLLECTION].retrieve()
    print(f"✅ Collection '{TYPESENSE_COLLECTION}' exists")
    print(f"   Documents: {collection['num_documents']}")

    # Show fields
    print(f"\n   Fields:")
    field_names = []
    for field in collection.get('fields', []):
        field_names.append(field['name'])
        print(f"     - {field['name']}: {field['type']} "
              f"{'(index: true)' if field.get('index', True) else '(index: false)'}")

    # Check if keyword path fields exist
    keyword_fields = ['primary_keywords', 'entity_names', 'semantic_keywords',
                      'key_facts', 'document_title']
    print(f"\n   Keyword path field check:")
    for f in keyword_fields:
        exists = f in field_names
        print(f"     {f}: {'✅ EXISTS' if exists else '❌ MISSING'}")
        if not exists:
            errors.append(f"Typesense field '{f}' MISSING from collection")

except Exception as e:
    print(f"❌ Typesense connection error: {e}")
    errors.append(f"Typesense connection: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: Typesense Keyword Path Query (the exact query your app sends)
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 4: Typesense Keyword Path Query")
print("═" * 70)

try:
    # This is EXACTLY what your keyword path sends
    print(f"\n  Query: '{TEST_QUERY}'")
    print(f"  Fields: primary_keywords,entity_names,semantic_keywords,key_facts,document_title")
    print(f"  Weights: 10,8,6,4,3")

    result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
        'q': TEST_QUERY,
        'query_by': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
        'query_by_weights': '10,8,6,4,3',
        'per_page': 5,
        'num_typos': 1,
        'drop_tokens_threshold': 0,
        'sort_by': 'authority_score:desc,published_date:desc',
    })

    found = result.get('found', 0)
    hits = result.get('hits', [])
    print(f"\n  ✅ Found: {found}")
    print(f"  Hits returned: {len(hits)}")

    if hits:
        print(f"\n  First 3 results:")
        for i, hit in enumerate(hits[:3]):
            doc = hit.get('document', {})
            print(f"    [{i+1}] uuid: {doc.get('document_uuid', '?')}")
            print(f"        data_type: {doc.get('document_data_type', '?')}")
            print(f"        title: {doc.get('document_title', '?')[:80]}")
            print(f"        text_match: {hit.get('text_match', '?')}")
            print()
    else:
        print(f"\n  ❌ NO HITS - This is the problem!")
        errors.append("Typesense keyword query returned 0 results")

except Exception as e:
    print(f"❌ Typesense query error: {e}")
    errors.append(f"Typesense query: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: Typesense with different field combinations
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 5: Typesense Field-by-Field Test")
print("═" * 70)

try:
    test_fields = [
        'primary_keywords',
        'entity_names',
        'semantic_keywords',
        'key_facts',
        'document_title',
    ]

    for field in test_fields:
        try:
            result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
                'q': TEST_QUERY,
                'query_by': field,
                'per_page': 1,
                'num_typos': 1,
            })
            found = result.get('found', 0)
            status = "✅" if found > 0 else "⚠️"
            print(f"  {status} {field}: {found} results")
        except Exception as e:
            print(f"  ❌ {field}: ERROR - {e}")
            errors.append(f"Typesense field '{field}' query error: {e}")

except Exception as e:
    print(f"❌ Field test error: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: Typesense wildcard query (does collection have ANY data?)
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 6: Typesense Basic Sanity Check")
print("═" * 70)

try:
    # Wildcard - get any document
    result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
        'q': '*',
        'query_by': 'document_title',
        'per_page': 3,
    })
    print(f"  Wildcard search: {result.get('found', 0)} total documents")

    # Check a single word
    for word in TEST_QUERY.split():
        result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
            'q': word,
            'query_by': 'document_title',
            'per_page': 1,
            'num_typos': 1,
        })
        print(f"  '{word}' in document_title: {result.get('found', 0)} results")

except Exception as e:
    print(f"❌ Sanity check error: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Compare keyword path vs semantic path query
# ─────────────────────────────────────────────────────────────────────────────
print("═" * 70)
print("TEST 7: Keyword vs Semantic Path Comparison")
print("═" * 70)

try:
    # Keyword path query
    kw_result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
        'q': TEST_QUERY,
        'query_by': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
        'query_by_weights': '10,8,6,4,3',
        'per_page': 5,
        'num_typos': 1,
        'drop_tokens_threshold': 0,
        'sort_by': 'authority_score:desc,published_date:desc',
    })

    # Try broader field set (what semantic path might use)
    # Get all string fields from collection
    string_fields = [f['name'] for f in collection.get('fields', [])
                     if f.get('type') in ('string', 'string[]', 'auto')
                     and f.get('index', True)]

    print(f"  All indexed string fields: {string_fields}")

    if string_fields:
        broad_result = ts_client.collections[TYPESENSE_COLLECTION].documents.search({
            'q': TEST_QUERY,
            'query_by': ','.join(string_fields[:10]),  # first 10
            'per_page': 5,
            'num_typos': 1,
        })
        print(f"\n  Keyword path (5 fields): {kw_result.get('found', 0)} results")
        print(f"  Broad search ({len(string_fields[:10])} fields): {broad_result.get('found', 0)} results")

        if kw_result.get('found', 0) == 0 and broad_result.get('found', 0) > 0:
            print(f"\n  ⚠️ DIAGNOSIS: Data exists but keyword path fields don't match!")
            print(f"     The keyword path fields may not contain the search terms.")
            errors.append("Data exists in other fields but not in keyword path fields")
    else:
        print(f"  Keyword path: {kw_result.get('found', 0)} results")

except Exception as e:
    print(f"❌ Comparison error: {e}")

print()

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("SUMMARY")
print("=" * 70)

if errors:
    print(f"\n❌ {len(errors)} issue(s) found:\n")
    for i, err in enumerate(errors, 1):
        print(f"  {i}. {err}")
else:
    print(f"\n✅ All tests passed - the issue may be in filtering or display logic.")
    print(f"   Next steps:")
    print(f"   - Add debug logging to fetch_candidate_ids()")
    print(f"   - Check filter_cached_results()")
    print(f"   - Check fetch_full_documents()")

print()
print("=" * 70)