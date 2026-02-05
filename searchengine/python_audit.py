"""
audit.py - System Audit Script (Optimized)

This script audits your Redis vocabulary and Typesense schema using
efficient index queries instead of scanning all keys.

Uses:
- FT.INFO for index structure
- FT.AGGREGATE for category/entity_type/pos distributions
- Typesense collection schema

Run this script and share the output.

Usage:
    python audit.py
"""

import json
import os
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

try:
    from decouple import config
    REDIS_LOCATION = config('REDIS_LOCATION', default='localhost')
    REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)
    REDIS_DB = config('REDIS_DB', default=0, cast=int)
    REDIS_PASSWORD = config('REDIS_PASSWORD', default='')
    REDIS_USERNAME = config('REDIS_USERNAME', default='')
    
    TYPESENSE_HOST = config('TYPESENSE_HOST', default='localhost')
    TYPESENSE_PORT = config('TYPESENSE_PORT', default='8108')
    TYPESENSE_PROTOCOL = config('TYPESENSE_PROTOCOL', default='http')
    TYPESENSE_API_KEY = config('TYPESENSE_API_KEY', default='')
    TYPESENSE_COLLECTION = config('TYPESENSE_COLLECTION', default='documents')
except ImportError:
    print("WARNING: python-decouple not found, using environment variables")
    REDIS_LOCATION = os.getenv('REDIS_LOCATION', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '')
    REDIS_USERNAME = os.getenv('REDIS_USERNAME', '')
    
    TYPESENSE_HOST = os.getenv('TYPESENSE_HOST', 'localhost')
    TYPESENSE_PORT = os.getenv('TYPESENSE_PORT', '8108')
    TYPESENSE_PROTOCOL = os.getenv('TYPESENSE_PROTOCOL', 'http')
    TYPESENSE_API_KEY = os.getenv('TYPESENSE_API_KEY', '')
    TYPESENSE_COLLECTION = os.getenv('TYPESENSE_COLLECTION', 'documents')

INDEX_NAME = "terms_idx"


# =============================================================================
# REDIS AUDIT (Using RediSearch Aggregations)
# =============================================================================

def audit_redis():
    """Audit Redis using RediSearch index queries - fast and efficient."""
    
    print("\n" + "=" * 70)
    print("REDIS VOCABULARY AUDIT")
    print("=" * 70)
    
    try:
        import redis
    except ImportError:
        print("ERROR: redis package not installed. Run: pip install redis")
        return None
    
    # Connect
    try:
        redis_config = {
            'host': REDIS_LOCATION,
            'port': REDIS_PORT,
            'db': REDIS_DB,
            'decode_responses': True,
            'socket_connect_timeout': 10,
        }
        if REDIS_PASSWORD:
            redis_config['password'] = REDIS_PASSWORD
        if REDIS_USERNAME:
            redis_config['username'] = REDIS_USERNAME
        
        client = redis.Redis(**redis_config)
        client.ping()
        print(f"✓ Connected to Redis at {REDIS_LOCATION}:{REDIS_PORT}")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return None
    
    results = {
        'index_info': {},
        'hash_schema': {},
        'categories': {},
        'entity_types': {},
        'pos_values': {},
        'rank_stats': {},
        'sample_hashes': []
    }
    
    # -------------------------------------------------------------------------
    # 1. GET INDEX INFO (FT.INFO)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("INDEX STRUCTURE (FT.INFO)")
    print("-" * 50)
    
    try:
        info = client.ft(INDEX_NAME).info()
        
        results['index_info'] = {
            'index_name': info.get('index_name', INDEX_NAME),
            'num_docs': info.get('num_docs', 0),
            'num_terms': info.get('num_terms', 0),
            'num_records': info.get('num_records', 0),
        }
        
        print(f"  Index name: {results['index_info']['index_name']}")
        print(f"  Documents indexed: {results['index_info']['num_docs']}")
        print(f"  Terms indexed: {results['index_info']['num_terms']}")
        
        # Parse schema from index info
        print("\n  Index Schema (fields):")
        attributes = info.get('attributes', [])
        
        for attr in attributes:
            if isinstance(attr, list) and len(attr) >= 4:
                field_name = attr[1] if len(attr) > 1 else 'unknown'
                field_type = attr[3] if len(attr) > 3 else 'unknown'
                
                props = {}
                i = 4
                while i < len(attr) - 1:
                    props[attr[i]] = attr[i + 1]
                    i += 2
                
                results['hash_schema'][field_name] = {
                    'type': field_type,
                    'properties': props
                }
                
                print(f"    - {field_name}: {field_type} {props if props else ''}")
        
    except Exception as e:
        print(f"  Error getting index info: {e}")
    
    # -------------------------------------------------------------------------
    # 2. GET ALL CATEGORIES (FT.AGGREGATE)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("CATEGORIES (FT.AGGREGATE)")
    print("-" * 50)
    
    try:
        agg_result = client.execute_command(
            'FT.AGGREGATE', INDEX_NAME, '*',
            'GROUPBY', '1', '@category',
            'REDUCE', 'COUNT', '0', 'AS', 'count',
            'SORTBY', '2', '@count', 'DESC',
            'LIMIT', '0', '100'
        )
        
        if agg_result and len(agg_result) > 1:
            for item in agg_result[1:]:
                if isinstance(item, list) and len(item) >= 4:
                    cat_name = item[1]
                    cat_count = int(item[3])
                    results['categories'][cat_name] = {'count': cat_count}
                    print(f"  {cat_name}: {cat_count}")
        
    except Exception as e:
        print(f"  Error aggregating categories: {e}")
    
    # -------------------------------------------------------------------------
    # 3. GET ALL ENTITY TYPES (FT.AGGREGATE)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("ENTITY TYPES (FT.AGGREGATE)")
    print("-" * 50)
    
    try:
        agg_result = client.execute_command(
            'FT.AGGREGATE', INDEX_NAME, '*',
            'GROUPBY', '1', '@entity_type',
            'REDUCE', 'COUNT', '0', 'AS', 'count',
            'SORTBY', '2', '@count', 'DESC',
            'LIMIT', '0', '50'
        )
        
        if agg_result and len(agg_result) > 1:
            for item in agg_result[1:]:
                if isinstance(item, list) and len(item) >= 4:
                    etype = item[1]
                    ecount = int(item[3])
                    results['entity_types'][etype] = {'count': ecount}
                    print(f"  {etype}: {ecount}")
        
    except Exception as e:
        print(f"  Error aggregating entity types: {e}")
    
    # -------------------------------------------------------------------------
    # 4. GET ALL POS VALUES (FT.AGGREGATE)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("POS VALUES (FT.AGGREGATE)")
    print("-" * 50)
    
    try:
        agg_result = client.execute_command(
            'FT.AGGREGATE', INDEX_NAME, '*',
            'GROUPBY', '1', '@pos',
            'REDUCE', 'COUNT', '0', 'AS', 'count',
            'SORTBY', '2', '@count', 'DESC',
            'LIMIT', '0', '50'
        )
        
        if agg_result and len(agg_result) > 1:
            for item in agg_result[1:]:
                if isinstance(item, list) and len(item) >= 4:
                    pos = item[1]
                    pcount = int(item[3])
                    results['pos_values'][pos] = {'count': pcount}
                    print(f"  {pos}: {pcount}")
        
    except Exception as e:
        print(f"  Error aggregating POS values: {e}")
    
    # -------------------------------------------------------------------------
    # 5. GET RANK STATISTICS (FT.AGGREGATE)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("RANK STATISTICS (FT.AGGREGATE)")
    print("-" * 50)
    
    try:
        agg_result = client.execute_command(
            'FT.AGGREGATE', INDEX_NAME, '*',
            'GROUPBY', '0',
            'REDUCE', 'MIN', '1', '@rank', 'AS', 'min_rank',
            'REDUCE', 'MAX', '1', '@rank', 'AS', 'max_rank',
            'REDUCE', 'AVG', '1', '@rank', 'AS', 'avg_rank'
        )
        
        if agg_result and len(agg_result) > 1:
            stats = agg_result[1]
            if isinstance(stats, list):
                stats_dict = {}
                for i in range(0, len(stats) - 1, 2):
                    stats_dict[stats[i]] = stats[i + 1]
                
                results['rank_stats'] = {
                    'min': stats_dict.get('min_rank', 0),
                    'max': stats_dict.get('max_rank', 0),
                    'avg': stats_dict.get('avg_rank', 0)
                }
                
                print(f"  Min rank: {results['rank_stats']['min']}")
                print(f"  Max rank: {results['rank_stats']['max']}")
                print(f"  Avg rank: {results['rank_stats']['avg']}")
        
    except Exception as e:
        print(f"  Error getting rank stats: {e}")
    
    # -------------------------------------------------------------------------
    # 6. GET SAMPLE HASHES (show actual data structure)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("SAMPLE HASH STRUCTURES (3 samples)")
    print("-" * 50)
    
    try:
        from redis.commands.search.query import Query
        search_result = client.ft(INDEX_NAME).search(Query('*').paging(0, 3))
        
        for doc in search_result.docs:
            sample = {
                'id': doc.id,
                'fields': {}
            }
            
            for attr in dir(doc):
                if not attr.startswith('_') and attr not in ['id', 'payload']:
                    value = getattr(doc, attr, None)
                    if value is not None:
                        sample['fields'][attr] = value
            
            results['sample_hashes'].append(sample)
            
            print(f"\n  Hash: {doc.id}")
            for field, value in sample['fields'].items():
                display_value = str(value)[:60] + '...' if len(str(value)) > 60 else value
                print(f"    {field}: {display_value}")
        
    except Exception as e:
        print(f"  Error getting sample hashes: {e}")
    
    # -------------------------------------------------------------------------
    # 7. GET SAMPLES PER CATEGORY (2 each, first 15 categories)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("SAMPLES PER CATEGORY")
    print("-" * 50)
    
    for category in list(results['categories'].keys())[:15]:
        try:
            from redis.commands.search.query import Query
            escaped_cat = category.replace('-', '\\-').replace(' ', '\\ ')
            search_result = client.ft(INDEX_NAME).search(
                Query(f'@category:{{{escaped_cat}}}').paging(0, 2)
            )
            
            samples = []
            for doc in search_result.docs:
                term = getattr(doc, 'term', '')
                display = getattr(doc, 'display', '')
                rank = getattr(doc, 'rank', 0)
                samples.append({'term': term, 'display': display, 'rank': rank})
            
            results['categories'][category]['samples'] = samples
            
            sample_terms = [s['term'] for s in samples]
            print(f"  {category}: {sample_terms}")
            
        except Exception as e:
            print(f"  {category}: Error - {e}")
    
    return results


# =============================================================================
# TYPESENSE AUDIT
# =============================================================================

def audit_typesense():
    """Audit Typesense collection schema."""
    
    print("\n" + "=" * 70)
    print("TYPESENSE SCHEMA AUDIT")
    print("=" * 70)
    
    try:
        import typesense
    except ImportError:
        print("ERROR: typesense package not installed. Run: pip install typesense")
        return None
    
    # Connect
    try:
        client = typesense.Client({
            'api_key': TYPESENSE_API_KEY,
            'nodes': [{
                'host': TYPESENSE_HOST,
                'port': TYPESENSE_PORT,
                'protocol': TYPESENSE_PROTOCOL
            }],
            'connection_timeout_seconds': 10
        })
        
        health = client.operations.is_healthy()
        print(f"✓ Connected to Typesense at {TYPESENSE_HOST}:{TYPESENSE_PORT}")
    except Exception as e:
        print(f"✗ Failed to connect to Typesense: {e}")
        return None
    
    results = {
        'collection_name': TYPESENSE_COLLECTION,
        'num_documents': 0,
        'schema_fields': [],
        'field_categories': {
            'searchable_text': [],
            'filterable': [],
            'facetable': [],
            'sortable': [],
            'vector': []
        },
        'sample_document': {}
    }
    
    # -------------------------------------------------------------------------
    # 1. GET COLLECTION SCHEMA
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("COLLECTION SCHEMA")
    print("-" * 50)
    
    try:
        collection = client.collections[TYPESENSE_COLLECTION].retrieve()
        results['num_documents'] = collection.get('num_documents', 0)
        
        print(f"  Collection: {TYPESENSE_COLLECTION}")
        print(f"  Documents: {results['num_documents']}")
        print(f"\n  Fields:")
        
        for field in collection.get('fields', []):
            name = field.get('name', '')
            ftype = field.get('type', '')
            facet = field.get('facet', False)
            index = field.get('index', True)
            optional = field.get('optional', False)
            sort = field.get('sort', False)
            
            field_info = {
                'name': name,
                'type': ftype,
                'facet': facet,
                'index': index,
                'optional': optional,
                'sort': sort
            }
            results['schema_fields'].append(field_info)
            
            # Categorize
            if 'embedding' in name.lower() or ftype.startswith('float[]'):
                results['field_categories']['vector'].append(name)
            elif index and ftype in ['string', 'string[]', 'auto']:
                results['field_categories']['searchable_text'].append(name)
            
            if facet:
                results['field_categories']['facetable'].append(name)
            
            if ftype in ['string', 'string[]', 'int32', 'int64', 'float', 'bool'] or facet:
                results['field_categories']['filterable'].append(name)
            
            if sort or ftype in ['int32', 'int64', 'float']:
                results['field_categories']['sortable'].append(name)
            
            # Print
            attrs = []
            if facet: attrs.append('facet')
            if not index: attrs.append('no-index')
            if optional: attrs.append('optional')
            if sort: attrs.append('sort')
            
            attr_str = f" [{', '.join(attrs)}]" if attrs else ""
            print(f"    {name}: {ftype}{attr_str}")
        
    except Exception as e:
        print(f"  Error getting schema: {e}")
        return results
    
    # -------------------------------------------------------------------------
    # 2. FIELD CATEGORIES SUMMARY
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("FIELD CATEGORIES")
    print("-" * 50)
    
    print(f"\n  Searchable text fields ({len(results['field_categories']['searchable_text'])}):")
    for f in results['field_categories']['searchable_text']:
        print(f"    - {f}")
    
    print(f"\n  Filterable fields ({len(results['field_categories']['filterable'])}):")
    for f in results['field_categories']['filterable']:
        print(f"    - {f}")
    
    print(f"\n  Facetable fields ({len(results['field_categories']['facetable'])}):")
    for f in results['field_categories']['facetable']:
        print(f"    - {f}")
    
    print(f"\n  Sortable fields ({len(results['field_categories']['sortable'])}):")
    for f in results['field_categories']['sortable']:
        print(f"    - {f}")
    
    print(f"\n  Vector fields ({len(results['field_categories']['vector'])}):")
    for f in results['field_categories']['vector']:
        print(f"    - {f}")
    
    # -------------------------------------------------------------------------
    # 3. SAMPLE DOCUMENT (without embedding)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 50)
    print("SAMPLE DOCUMENT STRUCTURE")
    print("-" * 50)
    
    try:
        search_result = client.collections[TYPESENSE_COLLECTION].documents.search({
            'q': '*',
            'per_page': 1,
            'exclude_fields': 'embedding'
        })
        
        if search_result.get('hits'):
            doc = search_result['hits'][0].get('document', {})
            results['sample_document'] = {k: type(v).__name__ for k, v in doc.items()}
            
            print("\n  Fields with actual data:")
            for key, value in doc.items():
                if isinstance(value, list):
                    if value:
                        preview = value[:2]
                        print(f"    {key}: {type(value).__name__}[{len(value)}] = {preview}...")
                    else:
                        print(f"    {key}: {type(value).__name__}[0] = []")
                elif isinstance(value, str):
                    preview = value[:60] + '...' if len(value) > 60 else value
                    print(f"    {key}: \"{preview}\"")
                else:
                    print(f"    {key}: {value}")
        
    except Exception as e:
        print(f"  Error getting sample: {e}")
    
    return results


# =============================================================================
# SAVE REPORT
# =============================================================================

def save_report(redis_results, typesense_results, output_path='audit_report.json'):
    """Save audit report as JSON."""
    
    report = {
        'generated_at': datetime.now().isoformat(),
        'redis': redis_results,
        'typesense': typesense_results
    }
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n✓ Report saved to: {output_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run the audit."""
    
    print("\n" + "=" * 70)
    print("SEARCH SYSTEM AUDIT")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    redis_results = audit_redis()
    typesense_results = audit_typesense()
    
    save_report(redis_results, typesense_results)
    
    print("\n" + "=" * 70)
    print("AUDIT COMPLETE")
    print("=" * 70)
    print("\nShare the terminal output or audit_report.json to proceed.")


if __name__ == '__main__':
    main()