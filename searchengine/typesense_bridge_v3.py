"""
typesense_discovery_bridge.py (ASYNC)
=====================================
AfroToDo Search Bridge — Word Discovery v3 + Intent Detection + Typesense.

SCORING ALGORITHM (v5 — with subject-field hierarchy)
-----------------------------------------------------
final_score = (
    blend['text_match'] * text_score      +
    blend['semantic']   * semantic_score  +
    blend['authority']  * authority_score_n
)
final_score *= _domain_relevance(doc, signals)
final_score *= _content_intent_match(doc, query_mode)
final_score *= _pool_type_multiplier(doc, query_mode)

text_score now applies a subject-match multiplier:
  - primary_subject_name match + HIGH confidence  → ×1.60
  - primary_subject_name match + MEDIUM confidence → ×1.30
  - primary_subject_name match + LOW confidence    → ×1.10
  - secondary_subjects match only                  → ×1.15
  - query token in keywords but NOT any subject    → ×0.70 (penalty)

PIPELINE
--------
SEMANTIC:  1A+1B → 2 (rerank) → 3 (prune) → 4 (metadata) → 5 (score+count) → 6 (cache) → 7 (paginate+fetch)
KEYWORD:   1 (uuids+metadata) → 5 (count) → 6 (cache) → 7 (paginate+fetch)
QUESTION:  direct fetch → format → return
"""

import re
import json
import math
import time
import asyncio
import hashlib
import typesense
from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime
from decouple import config
import requests
import random


# ── Word Discovery v3 ────────────────────────────────────────────────────────

try:
    from .word_discovery_fulltest import WordDiscovery
    WORD_DISCOVERY_AVAILABLE = True
    print("✅ WordDiscovery imported from .word_discovery_fulltest")
except ImportError:
    try:
        from word_discovery_fulltest import WordDiscovery
        WORD_DISCOVERY_AVAILABLE = True
        print("✅ WordDiscovery imported from word_discovery_fulltest")
    except ImportError:
        WORD_DISCOVERY_AVAILABLE = False
        print("⚠️ WordDiscovery not available")


# ── Intent Detection ─────────────────────────────────────────────────────────

try:
    from .intent_detect import detect_intent, get_signals
    INTENT_DETECT_AVAILABLE = True
    print("✅ intent_detect imported")
except ImportError:
    try:
        from intent_detect import detect_intent, get_signals
        INTENT_DETECT_AVAILABLE = True
        print("✅ intent_detect imported (fallback)")
    except ImportError:
        INTENT_DETECT_AVAILABLE = False
        print("⚠️ intent_detect not available")


# ── Embedding Client ─────────────────────────────────────────────────────────

try:
    from .embedding_client import get_query_embedding
    print("✅ get_query_embedding imported from .embedding_client")
except ImportError:
    try:
        from embedding_client import get_query_embedding
        print("✅ get_query_embedding imported from embedding_client")
    except ImportError:
        def get_query_embedding(query: str) -> Optional[List[float]]:
            print("⚠️ embedding_client not available")
            return None


# ── Related Search Store ─────────────────────────────────────────────────────

try:
    from .cached_embedding_related_search import store_query_embedding
    print("✅ store_query_embedding imported")
except ImportError:
    try:
        from cached_embedding_related_search import store_query_embedding
        print("✅ store_query_embedding imported (fallback)")
    except ImportError:
        def store_query_embedding(*args, **kwargs):
            return False
        print("⚠️ store_query_embedding not available")


# ── Django Cache ─────────────────────────────────────────────────────────────

from django.core.cache import cache as django_cache


# ── Typesense Client ─────────────────────────────────────────────────────────

client = typesense.Client({
    'api_key':  config('TYPESENSE_API_KEY'),
    'nodes': [{
        'host':     config('TYPESENSE_HOST'),
        'port':     config('TYPESENSE_PORT'),
        'protocol': config('TYPESENSE_PROTOCOL'),
    }],
    'connection_timeout_seconds': 5,
})

COLLECTION_NAME = 'document'


# ── Cache Settings ───────────────────────────────────────────────────────────

CACHE_TTL_SECONDS  = 300
MAX_CACHED_RESULTS = 100


# ── UI Labels ────────────────────────────────────────────────────────────────

DATA_TYPE_LABELS = {
    'article':  'Articles',
    'person':   'People',
    'business': 'Businesses',
    'place':    'Places',
    'media':    'Media',
    'event':    'Events',
    'product':  'Products',
}

CATEGORY_LABELS = {
    'healthcare_medical': 'Healthcare',
    'fashion':            'Fashion',
    'beauty':             'Beauty',
    'food_recipes':       'Food & Recipes',
    'travel_tourism':     'Travel',
    'entertainment':      'Entertainment',
    'business':           'Business',
    'education':          'Education',
    'technology':         'Technology',
    'sports':             'Sports',
    'finance':            'Finance',
    'real_estate':        'Real Estate',
    'lifestyle':          'Lifestyle',
    'news':               'News',
    'culture':            'Culture',
    'general':            'General',
}

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


# ── Blend Ratios (base — _resolve_blend adjusts at runtime) ──────────────────

BLEND_RATIOS = {
    'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
    'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
    'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
    'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
    'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
    'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
}


# ── Pool Scoping per Query Mode ───────────────────────────────────────────────

POOL_SCOPE = {
    'local':   {'primary': 'business',  'allow': {'business', 'place'}},
    'shop':    {'primary': 'product',   'allow': {'product', 'business'}},
    'answer':  {'primary': None,        'allow': {'article', 'person', 'place'}},
    'browse':  {'primary': None,        'allow': {'article', 'business', 'product'}},
    'explore': {'primary': None,        'allow': {'article', 'person', 'media'}},
    'compare': {'primary': None,        'allow': {'article', 'person', 'business'}},
}


# ── Data Type Preferences ─────────────────────────────────────────────────────

DATA_TYPE_PREFERENCES = {
    'answer':  ['article', 'person', 'place'],
    'explore': ['article', 'person', 'media'],
    'browse':  ['article', 'business', 'product'],
    'local':   ['business', 'place', 'article'],
    'shop':    ['product', 'business', 'article'],
    'compare': ['article', 'person', 'business'],
}


# ── Domain → Document Category Alignment ─────────────────────────────────────

DOMAIN_CATEGORY_MAP = {
    'food':        {'food_recipes', 'dining', 'lifestyle'},
    'business':    {'business', 'finance', 'entrepreneurship'},
    'health':      {'healthcare_medical', 'wellness', 'fitness'},
    'music':       {'entertainment', 'music', 'culture'},
    'fashion':     {'fashion', 'beauty', 'lifestyle'},
    'education':   {'education', 'hbcu', 'scholarship'},
    'travel':      {'travel_tourism', 'lifestyle'},
    'real_estate': {'real_estate', 'business'},
    'sports':      {'sports', 'entertainment'},
    'technology':  {'technology', 'business'},
    'beauty':      {'beauty', 'lifestyle', 'fashion'},
    'culture':     {'culture', 'news', 'lifestyle'},
}


# ── Content Intent Alignment per Query Mode ───────────────────────────────────

INTENT_CONTENT_MAP = {
    'local':   {'transactional', 'navigational'},
    'shop':    {'transactional', 'commercial'},
    'answer':  {'informational', 'educational'},
    'browse':  {'informational', 'commercial', 'transactional'},
    'explore': {'informational', 'educational'},
    'compare': {'informational', 'commercial'},
}


# ── Question-Word → Subject-Type Alignment ───────────────────────────────────
# Used to boost docs whose primary_subject_type matches the question intent.

QUESTION_SUBJECT_TYPE_MAP = {
    'who':   {'PERSON'},
    'where': {'PLACE', 'LANDMARK'},
    'when':  {'EVENT'},
    'what':  {'CONCEPT', 'WORK', 'PRODUCT'},
    'which': {'CONCEPT', 'WORK', 'PRODUCT'},
}


# ── Scoring Thresholds ────────────────────────────────────────────────────────

SEMANTIC_DISTANCE_GATE          = 0.65
QUESTION_SEMANTIC_DISTANCE_GATE = 0.40
REVIEW_COUNT_SCALE_BIZ          = 500
REVIEW_COUNT_SCALE_RECIPE       = 200
BLACK_OWNED_BOOST               = 0.12
PREFERRED_TYPE_BOOST            = 0.08
SUPERLATIVE_SCORE_CAP           = 0.70


# ── Subject-Match Multipliers (NEW v5) ────────────────────────────────────────
# Applied multiplicatively in _compute_text_score AFTER weighted field overlap
# is computed. These are the core fix for the "mentioned but not about" bug.

_SUBJECT_BOOST_PRIMARY_HIGH    = 1.60   # query matches primary_subject_name + HIGH confidence
_SUBJECT_BOOST_PRIMARY_MEDIUM  = 1.30   # primary match + MEDIUM confidence
_SUBJECT_BOOST_PRIMARY_LOW     = 1.10   # primary match + LOW confidence
_SUBJECT_BOOST_SECONDARY       = 1.15   # query matches secondary_subjects only
_SUBJECT_PENALTY_NO_MATCH      = 0.70   # query keyword present but NOT a subject

# Subject-type boost when question word aligns with primary_subject_type.
_SUBJECT_TYPE_BOOST            = 1.20


# ── Utility Functions ─────────────────────────────────────────────────────────

def _parse_rank(rank_value: Any) -> int:
    """Safely convert any rank value to an integer."""
    if isinstance(rank_value, int):
        return rank_value
    try:
        return int(float(rank_value))
    except (ValueError, TypeError):
        return 0


def _has_real_images(item: Dict) -> bool:
    """Return True if the candidate has at least one non-empty image or logo URL."""
    image_urls = item.get('image_url', [])
    if isinstance(image_urls, str):
        image_urls = [image_urls]
    if any(u for u in image_urls if u):
        return True
    logo_urls = item.get('logo_url', [])
    if isinstance(logo_urls, str):
        logo_urls = [logo_urls]
    return any(u for u in logo_urls if u)


def _count_images_from_candidates(all_results: List[Dict]) -> int:
    """Count documents in the result set that have at least one real image."""
    return sum(1 for item in all_results if _has_real_images(item))


def _generate_stable_cache_key(session_id: str, query: str) -> str:
    """Build a deterministic MD5 cache key from session ID and normalized query."""
    normalized = query.strip().lower()
    key_string = f"final|{session_id or 'nosession'}|{normalized}"
    return hashlib.md5(key_string.encode()).hexdigest()


# ============================================================
# CACHE FUNCTIONS + WORD DISCOVERY RUNNERS
# ============================================================

async def _get_cached_results(cache_key: str):
    """
    Get the finished result package from Redis.
    Returns the cached dict or None on miss or error.
    """
    try:
        data = await asyncio.to_thread(django_cache.get, cache_key)
        if data is not None:
            print(f"🟢 Redis cache HIT: {cache_key[:12]}...")
            return data
        print(f"🔴 Redis cache MISS: {cache_key[:12]}...")
        return None
    except Exception as e:
        print(f"⚠️ Redis cache GET error: {e}")
        return None


async def _set_cached_results(cache_key: str, data: Dict) -> None:
    """
    Write the finished result package to Redis with TTL.
    Silently absorbs errors so a cache failure never breaks search.
    """
    try:
        await asyncio.to_thread(django_cache.set, cache_key, data, CACHE_TTL_SECONDS)
        print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
    except Exception as e:
        print(f"⚠️ Redis cache SET error: {e}")


async def clear_search_cache() -> None:
    """Clear all cached search results."""
    try:
        await asyncio.to_thread(django_cache.clear)
        print("🧹 Redis search cache cleared")
    except Exception as e:
        print(f"⚠️ Redis cache CLEAR error: {e}")


def _run_word_discovery_sync(query: str) -> Dict:
    """
    Run Word Discovery v3 on the query string (synchronous).
    Returns the full pre-classified profile dict.
    Falls back to a minimal safe structure if WD is unavailable.
    """
    if WORD_DISCOVERY_AVAILABLE:
        try:
            wd     = WordDiscovery(verbose=False)
            result = wd.process(query)
            return result
        except Exception as e:
            print(f"⚠️ WordDiscovery error: {e}")

    return {
        'query':                   query,
        'corrected_query':         query,
        'corrected_display_query': query,
        'search_terms':            [],
        'persons':                 [],
        'organizations':           [],
        'keywords':                [],
        'media':                   [],
        'cities':                  [],
        'states':                  [],
        'location_terms':          [],
        'primary_intent':          'general',
        'intent_scores':           {},
        'field_boosts':            {},
        'corrections':             [],
        'terms':                   [],
        'ngrams':                  [],
        'stats': {
            'total_words':     len(query.split()),
            'valid_words':     0,
            'corrected_words': 0,
            'unknown_words':   len(query.split()),
            'stopwords':       0,
            'ngram_count':     0,
        },
    }


async def _run_word_discovery(query: str) -> Dict:
    """Async wrapper — runs WordDiscovery in a thread."""
    try:
        return await asyncio.to_thread(_run_word_discovery_sync, query)
    except Exception as e:
        print(f"⚠️ _run_word_discovery async error: {e}")
        return _run_word_discovery_sync.__wrapped__(query) if hasattr(_run_word_discovery_sync, '__wrapped__') else {
            'query': query, 'corrected_query': query, 'corrected_display_query': query,
            'search_terms': [], 'persons': [], 'organizations': [], 'keywords': [],
            'media': [], 'cities': [], 'states': [], 'location_terms': [],
            'primary_intent': 'general', 'intent_scores': {}, 'field_boosts': {},
            'corrections': [], 'terms': [], 'ngrams': [],
            'stats': {'total_words': len(query.split()), 'valid_words': 0,
                      'corrected_words': 0, 'unknown_words': len(query.split()),
                      'stopwords': 0, 'ngram_count': 0},
        }


def _run_embedding_sync(query: str) -> Optional[List[float]]:
    """
    Call the embedding client and return the query vector (synchronous).
    Returns None if the client is unavailable.
    """
    return get_query_embedding(query)


async def _run_embedding(query: str) -> Optional[List[float]]:
    """Async wrapper — runs embedding generation in a thread."""
    try:
        return await asyncio.to_thread(_run_embedding_sync, query)
    except Exception as e:
        print(f"⚠️ _run_embedding async error: {e}")
        return None


async def run_parallel_prep(
    query: str,
    skip_embedding: bool = False
) -> Tuple[Dict, Optional[List[float]]]:
    """
    Run Word Discovery v3 and embedding generation in parallel using asyncio.gather.
    """
    if skip_embedding:
        discovery = await _run_word_discovery(query)
        for term in discovery.get('terms', []):
            if isinstance(term.get('context_flags'), (frozenset, set)):
                term['context_flags'] = sorted(list(term['context_flags']))
        return discovery, None

    # Run both in parallel
    discovery, embedding = await asyncio.gather(
        _run_word_discovery(query),
        _run_embedding(query),
    )

    # FIX — convert frozenset context_flags to sorted list
    for term in discovery.get('terms', []):
        if isinstance(term.get('context_flags'), (frozenset, set)):
            term['context_flags'] = sorted(list(term['context_flags']))

    corrected_query = discovery.get('corrected_query', query)

    if corrected_query.lower() != query.lower() and embedding is not None:
        corrections = discovery.get('corrections', [])

        UNSAFE_CATEGORIES = {
            'Food', 'US City', 'US State', 'Country', 'Location',
            'City', 'Place', 'Object', 'Animal', 'Color',
        }

        safe_corrections   = []
        unsafe_corrections = []

        for c in corrections:
            corrected_category = c.get('category', '')
            correction_type    = c.get('correction_type', '')

            if (correction_type == 'pos_mismatch' or
                    corrected_category in UNSAFE_CATEGORIES or
                    c.get('category', '') in ('Person', 'Organization', 'Brand')):
                unsafe_corrections.append(c)
            else:
                safe_corrections.append(c)

        if unsafe_corrections:
            print(f"⚠️ Skipping re-embed — unsafe corrections detected:")
            for c in unsafe_corrections:
                print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
                      f"(type={c.get('correction_type')}, category={c.get('category')})")
        elif safe_corrections:
            print(f"✅ Re-embedding with corrected query: '{corrected_query}'")
            embedding = await _run_embedding(corrected_query)

    return discovery, embedding


# ============================================================
# V3 PROFILE READER + TYPESENSE PARAMETER BUILDERS
# ============================================================

def _read_v3_profile(discovery: Dict, signals: Dict = None) -> Dict:
    """
    Read the pre-classified v3 profile directly.
    O(1) field reads with safe defaults — no re-classification.
    Adds preferred_data_types from a single dict lookup on query_mode.
    """
    query_mode = (signals or {}).get('query_mode', 'explore')

    return {
        'search_terms':      discovery.get('search_terms', []),
        'persons':           discovery.get('persons', []),
        'organizations':     discovery.get('organizations', []),
        'keywords':          discovery.get('keywords', []),
        'media':             discovery.get('media', []),
        'cities':            discovery.get('cities', []),
        'states':            discovery.get('states', []),
        'location_terms':    discovery.get('location_terms', []),
        'primary_intent':    discovery.get('primary_intent', 'general'),
        'intent_scores':     discovery.get('intent_scores', {}),
        'field_boosts':      discovery.get('field_boosts', {
            'document_title':   10,
            'entity_names':      2,
            'primary_keywords':  3,
            'key_facts':         3,
            'semantic_keywords': 2,
        }),
        'corrections':       discovery.get('corrections', []),
        'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
        'has_person':        bool(discovery.get('persons')),
        'has_organization':  bool(discovery.get('organizations')),
        'has_location':      bool(
            discovery.get('cities') or
            discovery.get('states') or
            discovery.get('location_terms')
        ),
        'has_keyword':       bool(discovery.get('keywords')),
        'has_media':         bool(discovery.get('media')),
    }


def build_typesense_params(
    profile: Dict,
    ui_filters: Dict = None,
    signals: Dict = None
) -> Dict:
    """
    Convert the v3 profile into Typesense search parameters.

    Field boost order (v5 — with subject hierarchy):
        primary_subject_name : 15  (highest — what the doc IS about)
        document_title       : 10
        entity_names         :  8
        primary_keywords     :  6
        secondary_subjects   :  5  (lesser subjects mentioned)
        key_facts            :  4
        semantic_keywords    :  3
    """
    signals    = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    params     = {}

    # ── Query string — deduplicate search_terms ───────────────────────────
    seen         = set()
    unique_terms = []
    for term in profile.get('search_terms', []):
        term_lower = term.lower()
        if term_lower not in seen:
            seen.add(term_lower)
            unique_terms.append(term)

    params['q'] = ' '.join(unique_terms) if unique_terms else '*'

    # ── Field boosts — v5 subject hierarchy ───────────────────────────────
    # Start from v3 profile boosts, then enforce the subject hierarchy.
    field_boosts = dict(profile.get('field_boosts', {}))

    # Subject hierarchy — these always take precedence over v3 boosts.
    field_boosts['primary_subject_name'] = 15
    field_boosts['document_title']       = max(field_boosts.get('document_title', 0), 10)
    field_boosts['entity_names']         = max(field_boosts.get('entity_names', 0), 8)
    field_boosts['primary_keywords']     = max(field_boosts.get('primary_keywords', 0), 6)
    field_boosts['secondary_subjects']   = 5
    field_boosts['key_facts']            = max(field_boosts.get('key_facts', 0), 4)
    field_boosts['semantic_keywords']    = max(field_boosts.get('semantic_keywords', 0), 3)

    if query_mode == 'local':
        field_boosts.setdefault('service_type',        12)
        field_boosts.setdefault('service_specialties', 10)

    sorted_fields              = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    params['query_by']         = ','.join(f[0] for f in sorted_fields)
    params['query_by_weights'] = ','.join(str(f[1]) for f in sorted_fields)

    # ── Typo / prefix / drop-token settings by mode ──────────────────────
    has_corrections = bool(profile.get('corrections'))
    term_count      = len(unique_terms)

    if query_mode == 'answer':
        params['num_typos']             = 0
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 0
    elif query_mode == 'explore':
        params['num_typos']             = 0 if has_corrections else 1
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
    elif query_mode == 'browse':
        params['num_typos']             = 1
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
    elif query_mode == 'local':
        params['num_typos']             = 1
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 1
    elif query_mode == 'compare':
        params['num_typos']             = 0
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 0
    elif query_mode == 'shop':
        params['num_typos']             = 1
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 1
    else:
        params['num_typos']             = 1
        params['prefix']                = 'no'
        params['drop_tokens_threshold'] = 1

    # ── Sort order ────────────────────────────────────────────────────────
    temporal_direction = signals.get('temporal_direction')
    price_direction    = signals.get('price_direction')
    has_superlative    = signals.get('has_superlative', False)
    has_rating         = signals.get('has_rating_signal', False)

    if temporal_direction == 'oldest':
        params['sort_by'] = 'time_period_start:asc,authority_score:desc'
    elif temporal_direction == 'newest':
        params['sort_by'] = 'published_date:desc,authority_score:desc'
    elif price_direction == 'cheap':
        params['sort_by'] = 'product_price:asc,authority_score:desc'
    elif price_direction == 'expensive':
        params['sort_by'] = 'product_price:desc,authority_score:desc'
    elif query_mode == 'local':
        params['sort_by'] = 'authority_score:desc'
    elif query_mode == 'browse' and has_superlative:
        params['sort_by'] = 'authority_score:desc'
    elif has_rating:
        params['sort_by'] = 'authority_score:desc'
    else:
        params['sort_by'] = '_text_match:desc,authority_score:desc'

    # ── filter_by — locations + pool scoping + UI filters ─────────────────
    filter_conditions = []

    is_location_subject = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )

    if not is_location_subject:
        cities = profile.get('cities', [])
        if cities:
            city_filters = [f"location_city:={c['name']}" for c in cities]
            filter_conditions.append(
                city_filters[0] if len(city_filters) == 1
                else '(' + ' || '.join(city_filters) + ')'
            )

        states = profile.get('states', [])
        if states:
            state_conditions = [
                f"location_state:={variant}"
                for state in states
                for variant in state.get('variants', [state['name']])
            ]
            filter_conditions.append(
                state_conditions[0] if len(state_conditions) == 1
                else '(' + ' || '.join(state_conditions) + ')'
            )

    # Pool scoping — read primary type from POOL_SCOPE
    scope = POOL_SCOPE.get(query_mode, {})
    primary_type = scope.get('primary')
    if primary_type and not (ui_filters and ui_filters.get('data_type')):
        filter_conditions.append(f'document_data_type:={primary_type}')

    if ui_filters:
        if ui_filters.get('data_type'):
            filter_conditions.append(f"document_data_type:={ui_filters['data_type']}")
        if ui_filters.get('category'):
            filter_conditions.append(f"document_category:={ui_filters['category']}")
        if ui_filters.get('schema'):
            filter_conditions.append(f"document_schema:={ui_filters['schema']}")

    if filter_conditions:
        params['filter_by'] = ' && '.join(filter_conditions)

    return params


def build_filter_string_without_data_type(
    profile: Dict,
    signals: Dict = None
) -> str:
    """
    Build the filter string used in Stage 1A.
    Includes location filters and pool scoping from POOL_SCOPE.
    """
    signals           = signals or {}
    filter_conditions = []
    query_mode        = signals.get('query_mode', 'explore')

    is_location_subject = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )

    if not is_location_subject:
        cities = profile.get('cities', [])
        if cities:
            city_filters = [f"location_city:={c['name']}" for c in cities]
            filter_conditions.append(
                city_filters[0] if len(city_filters) == 1
                else '(' + ' || '.join(city_filters) + ')'
            )

        states = profile.get('states', [])
        if states:
            state_conditions = [
                f"location_state:={variant}"
                for state in states
                for variant in state.get('variants', [state['name']])
            ]
            filter_conditions.append(
                state_conditions[0] if len(state_conditions) == 1
                else '(' + ' || '.join(state_conditions) + ')'
            )

    # Pool scoping — read primary type from POOL_SCOPE
    scope = POOL_SCOPE.get(query_mode, {})
    primary_type = scope.get('primary')
    if primary_type:
        filter_conditions.append(f'document_data_type:={primary_type}')

    return ' && '.join(filter_conditions) if filter_conditions else ''


# ============================================================
# SCORING FUNCTIONS (ALL SYNCHRONOUS — CPU ONLY)
# ============================================================

# ── Tunables (kept near the top for easy tweaking in prod) ────────────────────

# Field weights used by _compute_text_score to measure match quality.
# A hit in document_title is worth ~4x a hit in document_summary.
_FIELD_WEIGHTS = {
    'document_title':    1.00,
    'entity_names':      0.90,
    'primary_keywords':  0.75,
    'key_facts':         0.60,
    'topic_tags':        0.35,
    'semantic_keywords': 0.40,
    'document_summary':  0.25,
}
_MAX_FIELD_SCORE = sum(_FIELD_WEIGHTS.values())  # ~4.25

# Richness composite — weights sum to 1.0. All fields 100% populated.
_RICHNESS_WEIGHTS = {
    'content_depth_score':   0.35,
    'factual_density_score': 0.30,
    'concept_count':         0.20,
    'subtopic_richness':     0.10,
    'word_count':            0.05,
}

# Authority tier-2 composite — weights sum to 1.0. All fields 100% populated.
_AUTHORITY_COMPOSITE_WEIGHTS = {
    'authority_rank_score':  0.35,
    'factual_density_score': 0.25,
    'content_depth_score':   0.20,
    'subtopic_richness':     0.10,
    'evergreen_score':       0.10,
}

# Divisors used to normalize raw integer scores into [0..1].
_NORM_CEILINGS = {
    'authority_rank_score':   100.0,
    'factual_density_score':  100.0,
    'content_depth_score':    100.0,
    'evergreen_score':        100.0,
    'subtopic_richness':       50.0,
    'concept_count':           50.0,
    'word_count':            3000.0,
}

# Docs below this richness get their text_score capped at 0.5 so a stub
# cannot out-score a substantive doc on a single keyword hit.
_THIN_DOC_RICHNESS_CAP = 0.30

# Stopwords stripped before computing field overlap.
_TEXT_STOPWORDS = frozenset({
    'a', 'an', 'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for',
    'by', 'with', 'from', 'as', 'is', 'are', 'was', 'were', 'be', 'been',
    'it', 'its', 'that', 'this', 'these', 'those', 'not', 'no',
})


# ── Internal helpers ─────────────────────────────────────────────────────────

def _tokenize_for_match(text) -> set:
    """Lowercase, strip punctuation, drop stopwords and length<=2 tokens."""
    if not text:
        return set()
    if isinstance(text, list):
        text = ' '.join(str(v) for v in text if v)
    else:
        text = str(text)
    text = text.lower()
    text = re.sub(r"[^\w\s-]", " ", text)
    text = re.sub(r"\s*-\s*", " ", text)
    return {t for t in text.split() if len(t) > 2 and t not in _TEXT_STOPWORDS}


def _build_query_tokens_from_profile(profile: Dict) -> set:
    """
    Collect every meaningful token the query contributes, drawn from
    the v3 profile. Used by _compute_text_score to measure overlap.
    """
    tokens = set()
    for term in profile.get('search_terms', []):
        tokens |= _tokenize_for_match(term)
    for p in profile.get('persons', []):
        tokens |= _tokenize_for_match(p.get('phrase') or p.get('word', ''))
    for o in profile.get('organizations', []):
        tokens |= _tokenize_for_match(o.get('phrase') or o.get('word', ''))
    for k in profile.get('keywords', []):
        tokens |= _tokenize_for_match(k.get('phrase') or k.get('word', ''))
    for m in profile.get('media', []):
        tokens |= _tokenize_for_match(m.get('phrase') or m.get('word', ''))
    return tokens


def _build_query_phrases_from_profile(profile: Dict) -> set:
    """Multi-word phrases — used for exact-phrase title / entity bonuses."""
    phrases = set()
    for source in ('persons', 'organizations', 'keywords', 'media'):
        for entry in profile.get(source, []):
            phrase = (entry.get('phrase') or entry.get('word', '') or '').lower().strip()
            if phrase and ' ' in phrase:
                phrases.add(phrase)
    return phrases


def _compute_richness(doc: Dict) -> float:
    """
    Return [0..1] richness score derived from 100%-populated fields.
    Used by _compute_text_score to stop thin docs from winning.
    """
    score = 0.0
    for field, weight in _RICHNESS_WEIGHTS.items():
        raw = doc.get(field) or 0
        try:
            raw = float(raw)
        except (TypeError, ValueError):
            raw = 0.0
        ceiling = _NORM_CEILINGS.get(field, 100.0)
        norm = min(raw / ceiling, 1.0) if ceiling > 0 else 0.0
        score += weight * norm

    # small bump if the doc has multiple key_facts (indicates enrichment ran)
    if doc.get('key_facts') and len(doc.get('key_facts', [])) >= 3:
        score = min(score + 0.05, 1.0)

    return min(score, 1.0)


def _compute_subject_multiplier(
    doc: Dict,
    query_tokens: set,
    query_phrases: set,
    signals: Dict,
) -> Tuple[float, str]:
    """
    Compute the subject-match multiplier for one doc. (NEW v5)

    Returns (multiplier, reason) where:
        - reason is a short label for diagnostics ('primary_high', 'secondary',
          'no_match_penalty', 'neutral', etc.)

    Logic:
        1. Tokenize primary_subject_name and check overlap with query.
           If overlap exists → apply boost based on primary_subject_confidence.
        2. Else tokenize secondary_subjects and check overlap.
           If overlap exists → apply secondary boost.
        3. Else if the query overlaps the doc's keyword/title fields BUT
           does NOT overlap any subject → apply penalty (the "Africa
           mentioned but not the subject" case).
        4. Else neutral (1.0).

    Then optionally apply a question-word ↔ subject_type alignment boost.
    """
    if not query_tokens and not query_phrases:
        return 1.0, 'neutral_no_query'

    # ── Primary subject check ────────────────────────────────────────────
    primary_name   = doc.get('primary_subject_name') or ''
    primary_conf   = (doc.get('primary_subject_confidence') or '').upper()
    primary_tokens = _tokenize_for_match(primary_name)
    primary_lower  = primary_name.lower().strip()

    primary_match = False
    if primary_tokens and (primary_tokens & query_tokens):
        primary_match = True
    elif primary_lower:
        # Multi-word phrase match against primary subject
        for phrase in query_phrases:
            if phrase in primary_lower or primary_lower in phrase:
                primary_match = True
                break

    if primary_match:
        if primary_conf == 'HIGH':
            multiplier, reason = _SUBJECT_BOOST_PRIMARY_HIGH, 'primary_high'
        elif primary_conf == 'MEDIUM':
            multiplier, reason = _SUBJECT_BOOST_PRIMARY_MEDIUM, 'primary_medium'
        elif primary_conf == 'LOW':
            multiplier, reason = _SUBJECT_BOOST_PRIMARY_LOW, 'primary_low'
        else:
            # No confidence label — treat as MEDIUM
            multiplier, reason = _SUBJECT_BOOST_PRIMARY_MEDIUM, 'primary_unknown_conf'

        # Optional: subject-type ↔ question-word alignment boost
        question_word    = (signals.get('question_word') or '').lower()
        primary_subject_type = (doc.get('primary_subject_type') or '').upper()
        aligned_types    = QUESTION_SUBJECT_TYPE_MAP.get(question_word, set())
        if aligned_types and primary_subject_type in aligned_types:
            multiplier *= _SUBJECT_TYPE_BOOST
            reason += '+type_match'

        return multiplier, reason

    # ── Secondary subject check ──────────────────────────────────────────
    secondary_subjects = doc.get('secondary_subjects') or []
    if isinstance(secondary_subjects, str):
        secondary_subjects = [secondary_subjects]

    secondary_tokens   = set()
    secondary_phrases  = []
    for sub in secondary_subjects:
        if not sub:
            continue
        secondary_tokens |= _tokenize_for_match(sub)
        secondary_phrases.append(sub.lower().strip())

    secondary_match = False
    if secondary_tokens and (secondary_tokens & query_tokens):
        secondary_match = True
    else:
        for qphrase in query_phrases:
            for sphrase in secondary_phrases:
                if qphrase in sphrase or sphrase in qphrase:
                    secondary_match = True
                    break
            if secondary_match:
                break

    if secondary_match:
        return _SUBJECT_BOOST_SECONDARY, 'secondary'

    # ── No subject match — penalty if query touched keyword fields ──────
    # If the query has tokens that appear in primary_keywords / entity_names
    # / document_title but neither subject contains them, the doc merely
    # mentions the term. Penalize.
    keyword_field_tokens = set()
    for field in ('primary_keywords', 'entity_names', 'document_title',
                  'key_facts', 'semantic_keywords', 'topic_tags'):
        keyword_field_tokens |= _tokenize_for_match(doc.get(field))

    if query_tokens & keyword_field_tokens:
        return _SUBJECT_PENALTY_NO_MATCH, 'no_subject_match_penalty'

    return 1.0, 'neutral'


# ── PUBLIC API ────────────────────────────────────────────────────────────────

def _resolve_blend(
    query_mode: str,
    signals: Dict,
    candidates: List[Dict]
) -> Dict:
    """
    Build the final blend ratios for this query at runtime.
    Same signature and key names as the original ('text_match', 'semantic', 'authority').
    """
    blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

    sample = candidates[:20]
    if sample:
        avg_authority = sum(_extract_authority_score(c) for c in sample) / len(sample)
    else:
        avg_authority = 0.0
    has_live_authority = avg_authority >= 0.05

    if not has_live_authority and blend['authority'] > 0:
        print(f"   ⚠️ Authority weight dead (avg={avg_authority:.3f}) — redistributing to semantic")
        blend['semantic'] += blend['authority']
        blend['authority'] = 0.0

    if signals.get('has_unknown_terms', False):
        shift               = min(0.15, blend['text_match'])
        blend['text_match'] -= shift
        blend['semantic']   += shift
        print(f"   📊 Unknown term shift: text={blend['text_match']:.2f} sem={blend['semantic']:.2f}")

    if signals.get('has_superlative', False) and has_live_authority:
        shift              = min(0.10, blend['semantic'])
        blend['semantic']  -= shift
        blend['authority'] += shift
        print(f"   📊 Superlative shift: sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

    if signals.get('has_rating_signal', False) and has_live_authority:
        shift               = min(0.10, blend['text_match'])
        blend['text_match'] -= shift
        blend['authority']  += shift
        print(f"   📊 Rating shift: text={blend['text_match']:.2f} auth={blend['authority']:.2f}")

    if query_mode == 'answer' and signals.get('wants_single_result'):
        blend = {'text_match': 0.65, 'semantic': 0.25, 'authority': 0.10}

    print(f"   📊 Final blend ({query_mode}): "
          f"text={blend['text_match']:.2f} "
          f"sem={blend['semantic']:.2f} "
          f"auth={blend['authority']:.2f}")

    return blend


def _extract_authority_score(doc: Dict) -> float:
    """
    Return a normalized authority score [0.0 .. 1.0] appropriate
    for this document's data type.
    """
    data_type = (
        doc.get('data_type') or
        doc.get('document_data_type') or
        ''
    ).lower()

    # ── Tier 1: explicit rating fields (when present) ───────────────────
    if data_type == 'business':
        rating  = doc.get('service_rating') or 0.0
        reviews = doc.get('service_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)

    elif data_type == 'product':
        rating  = doc.get('product_rating') or 0.0
        reviews = doc.get('product_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)

    elif data_type == 'recipe':
        rating  = doc.get('recipe_rating') or 0.0
        reviews = doc.get('recipe_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_RECIPE),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)

    elif data_type == 'media':
        rating = doc.get('media_rating') or 0.0
        if rating > 0:
            return min(rating / 5.0, 1.0)

    # ── Tier 2: composite from 100%-populated enrichment fields ─────────
    score = 0.0
    for field, weight in _AUTHORITY_COMPOSITE_WEIGHTS.items():
        raw = doc.get(field) or 0
        try:
            raw = float(raw)
        except (TypeError, ValueError):
            raw = 0.0
        ceiling = _NORM_CEILINGS.get(field, 100.0)
        norm = min(raw / ceiling, 1.0) if ceiling > 0 else 0.0
        score += weight * norm

    return min(score, 1.0)


def _compute_text_score(
    keyword_rank: int,
    pool_size: int,
    item: Dict,
    profile: Dict,
    signals: Dict = None,
) -> float:
    """
    Score from the item's content match against the query.

    v5 changes:
      - signals param added (optional, defaults to None for backward compat)
      - subject-match multiplier applied AFTER existing field-overlap logic
      - subject_match_reason and subject_multiplier stashed on the item
        for diagnostics
    """
    signals = signals or {}

    query_tokens = _build_query_tokens_from_profile(profile)
    query_phrases = _build_query_phrases_from_profile(profile)

    if not query_tokens:
        # No meaningful query tokens — fall back to a mild positional signal
        base_score = 1.0 - (keyword_rank / max(pool_size, 1)) * 0.5
        # Still apply subject multiplier so docs about the topic win on '*' queries
        sub_mult, sub_reason = _compute_subject_multiplier(item, query_tokens, query_phrases, signals)
        item['subject_multiplier']     = round(sub_mult, 3)
        item['subject_match_reason']   = sub_reason
        return min(base_score * sub_mult, 1.0)

    # ── Weighted field overlap ──────────────────────────────────────────
    raw_score = 0.0
    for field, weight in _FIELD_WEIGHTS.items():
        field_tokens = _tokenize_for_match(item.get(field))
        if not field_tokens:
            continue
        overlap = query_tokens & field_tokens
        frac = len(overlap) / len(query_tokens)
        raw_score += weight * frac

    match_score = raw_score / _MAX_FIELD_SCORE if _MAX_FIELD_SCORE > 0 else 0.0

    # ── Exact multi-word phrase bonus ───────────────────────────────────
    if query_phrases:
        title = (item.get('document_title') or '').lower()
        entities = item.get('entity_names') or []
        entity_text = ' '.join(e.lower() for e in entities if e) if entities else ''
        phrase_hits = sum(
            1 for p in query_phrases
            if p in title or p in entity_text
        )
        if phrase_hits > 0:
            match_score = min(match_score + min(phrase_hits * 0.15, 0.30), 1.0)

    # ── Small Typesense-rank tiebreaker ─────────────────────────────────
    positional_bonus = (1.0 - (keyword_rank / max(pool_size, 1))) * 0.05
    match_score = min(match_score + positional_bonus, 1.0)

    # ── Primary-keyword exact-match bonus ───────────────────────────────
    doc_kws = set(k.lower() for k in (item.get('primary_keywords') or []))
    wd_kws  = set(
        k.get('phrase', '').lower()
        for k in profile.get('keywords', [])
        if k.get('phrase')
    )
    overlap = doc_kws & wd_kws
    if overlap:
        match_score = min(match_score + min(len(overlap) * 0.05, 0.15), 1.0)

    # ── Richness dampener — stops thin-doc wins ─────────────────────────
    richness = _compute_richness(item)
    if richness < _THIN_DOC_RICHNESS_CAP:
        match_score = min(match_score, 0.5)
    else:
        match_score *= (0.7 + 0.3 * richness)

    item['richness'] = round(richness, 4)

    # ── Subject-match multiplier (NEW v5) ───────────────────────────────
    # Applied AFTER all the existing field-overlap logic so the existing
    # behavior is preserved and the subject signal acts as an overlay.
    sub_mult, sub_reason = _compute_subject_multiplier(
        item, query_tokens, query_phrases, signals
    )
    match_score *= sub_mult

    item['subject_multiplier']   = round(sub_mult, 3)
    item['subject_match_reason'] = sub_reason

    return min(match_score, 1.0)


def _compute_semantic_score(vector_distance: float) -> float:
    """Convert vector distance to a score with a hard gate at 0.65."""
    if vector_distance is None or vector_distance >= SEMANTIC_DISTANCE_GATE:
        return 0.0
    return 1.0 - (vector_distance / SEMANTIC_DISTANCE_GATE)


def _domain_relevance(doc: Dict, signals: Dict) -> float:
    """Return a multiplier based on domain alignment."""
    primary_domain = signals.get('primary_domain')
    if not primary_domain:
        return 1.0

    doc_category  = (
        doc.get('document_category') or
        doc.get('category') or
        ''
    ).lower()

    doc_schema = (
        doc.get('document_schema') or
        doc.get('schema') or
        ''
    ).lower()

    service_types = [
        s.lower() for s in (doc.get('service_type') or [])
        if s
    ]

    aligned_categories = DOMAIN_CATEGORY_MAP.get(primary_domain, set())

    if not aligned_categories:
        return 1.0

    if doc_category in aligned_categories:
        return 1.15

    DOMAIN_SERVICE_MAP = {
        'food':        {
            'restaurant', 'cafe', 'bakery', 'catering', 'food',
            'dining', 'eatery', 'diner', 'buffet', 'bar', 'brewery',
            'winery', 'food truck', 'coffee',
        },
        'beauty':      {
            'salon', 'barbershop', 'spa', 'nail salon', 'hair salon',
            'nail tech', 'esthetician', 'lash studio', 'brow bar',
        },
        'health':      {
            'clinic', 'doctor', 'dentist', 'gym', 'fitness',
            'pharmacy', 'urgent care', 'therapist', 'chiropractor',
            'optometrist', 'mental health',
        },
        'education':   {
            'school', 'tutoring', 'daycare', 'academy',
            'preschool', 'childcare', 'learning center',
        },
        'real_estate': {
            'realty', 'realtor', 'property management',
            'real estate', 'mortgage', 'home inspection',
        },
        'technology':  {
            'software', 'it services', 'tech support',
            'web design', 'app development',
        },
        'business':    {
            'consulting', 'accounting', 'legal', 'staffing',
            'financial', 'insurance', 'marketing', 'advertising',
        },
        'culture':     {
            'museum', 'gallery', 'cultural center', 'community center',
            'church', 'nonprofit',
        },
        'music':       {
            'studio', 'recording studio', 'music venue', 'club',
            'lounge', 'concert venue',
        },
        'fashion':     {
            'boutique', 'clothing store', 'tailor', 'alterations',
            'fashion', 'apparel',
        },
        'sports':      {
            'gym', 'fitness center', 'sports facility', 'yoga',
            'martial arts', 'dance studio',
        },
    }

    aligned_services = DOMAIN_SERVICE_MAP.get(primary_domain, set())
    if aligned_services and any(s in aligned_services for s in service_types):
        return 1.15

    if primary_domain in doc_schema or primary_domain in doc_category:
        return 1.10

    return 0.75


def _content_intent_match(doc: Dict, query_mode: str) -> float:
    """Return a multiplier based on content_intent alignment."""
    doc_intent = (doc.get('content_intent') or '').lower()
    if not doc_intent:
        return 1.0

    preferred = INTENT_CONTENT_MAP.get(query_mode, set())
    if not preferred:
        return 1.0

    return 1.10 if doc_intent in preferred else 0.85


def _pool_type_multiplier(doc: Dict, query_mode: str) -> float:
    """Return a multiplier based on data type appropriateness."""
    data_type = (
        doc.get('data_type') or
        doc.get('document_data_type') or
        ''
    ).lower()

    scope         = POOL_SCOPE.get(query_mode, {})
    allowed_types = scope.get('allow', set())

    if not allowed_types:
        return 1.0

    return 1.0 if data_type in allowed_types else 0.5


def _score_document(
    idx: int,
    item: Dict,
    profile: Dict,
    signals: Dict,
    blend: Dict,
    pool_size: int,
    vector_data: Dict
) -> float:
    """
    Compute the final blended score for one document.
    v5: now passes signals through to _compute_text_score so the
    subject-match multiplier can read question_word.
    """
    query_mode = signals.get('query_mode', 'explore')
    item_id    = item.get('id', '')

    vd              = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
    vector_distance = vd.get('vector_distance', 1.0)
    semantic_rank   = vd.get('semantic_rank', 999999)

    item['vector_distance'] = vector_distance
    item['semantic_rank']   = semantic_rank

    text_score = _compute_text_score(idx, pool_size, item, profile, signals)
    sem_score  = _compute_semantic_score(vector_distance)
    auth_score = _extract_authority_score(item)

    blended = (
        blend['text_match'] * text_score +
        blend['semantic']   * sem_score  +
        blend['authority']  * auth_score
    )

    blended *= _domain_relevance(item, signals)
    blended *= _content_intent_match(item, query_mode)
    blended *= _pool_type_multiplier(item, query_mode)

    if item.get('data_type') in profile.get('preferred_data_types', []):
        blended = min(blended + PREFERRED_TYPE_BOOST, 1.0)

    if signals.get('has_black_owned') and item.get('black_owned') is True:
        blended = min(blended + BLACK_OWNED_BOOST, 1.0)

    if signals.get('has_superlative') and auth_score == 0.0:
        blended = min(blended, SUPERLATIVE_SCORE_CAP)

    item['blended_score'] = blended
    item['text_score']    = round(text_score, 4)
    item['sem_score']     = round(sem_score, 4)
    item['auth_score']    = round(auth_score, 4)

    return blended


# ============================================================
# CANDIDATE FETCHING (STAGES 1A, 1B, COMBINED)
# ============================================================

_MATCH_STOPWORDS = frozenset({
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
    'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
    'would', 'could', 'should', 'may', 'might', 'shall', 'can', 'need',
    'when', 'where', 'who', 'what', 'which', 'why', 'how', 'that', 'this',
    'these', 'those', 'it', 'its', 'he', 'she', 'they', 'we', 'you', 'i',
    'his', 'her', 'their', 'our', 'your', 'my', 'about', 'into', 'through',
    'during', 'before', 'after', 'above', 'below', 'between', 'each',
    'than', 'so', 'if', 'not', 'no', 'nor', 'yet', 'both', 'either',
    'just', 'also', 'then', 'than', 'such', 'more', 'most', 'other',
    'born', 'died', 'first', 'last', 'new', 'old', 'many', 'much',
    'long', 'little', 'own', 'right', 'big', 'high', 'great', 'small',
})


def _normalize_signal(text: str) -> set:
    """Normalize a signal string into a set of meaningful tokens."""
    if not text:
        return set()
    text = text.lower()
    text = re.sub(r"[^\w\s-]", " ", text)
    text = re.sub(r"\s*-\s*", " ", text)
    return {t for t in text.split() if len(t) > 2 and t not in _MATCH_STOPWORDS}


def _extract_query_signals(
    profile: Dict,
    discovery: Dict = None
) -> Tuple[set, list, Optional[set]]:
    """Extract and normalize all meaningful query signals from the v3 profile."""
    raw_signals    = []
    ranked_signals = []

    for p in profile.get('persons', []):
        phrase = p.get('phrase') or p.get('word', '')
        rank   = p.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    for o in profile.get('organizations', []):
        phrase = o.get('phrase') or o.get('word', '')
        rank   = o.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    for k in profile.get('keywords', []):
        phrase = k.get('phrase') or k.get('word', '')
        rank   = k.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    for term in profile.get('search_terms', []):
        if term:
            raw_signals.append(term)

    if discovery:
        for corr in discovery.get('corrections', []):
            if corr.get('correction_type') == 'suggestion' and corr.get('corrected'):
                corrected = corr['corrected']
                if corrected not in raw_signals:
                    raw_signals.append(corrected)
                    ranked_signals.append((100, corrected))

        for term in discovery.get('terms', []):
            if term.get('status') == 'unknown_suggest' and term.get('suggestion'):
                suggestion = term['suggestion']
                if suggestion not in raw_signals:
                    raw_signals.append(suggestion)
                    ranked_signals.append((100, suggestion))

    all_tokens   = set()
    full_phrases = []

    for phrase in raw_signals:
        all_tokens.update(_normalize_signal(phrase))
        phrase_lower = phrase.lower().strip()
        if phrase_lower:
            full_phrases.append(phrase_lower)

    primary_subject = None
    if ranked_signals:
        ranked_signals.sort(key=lambda x: -x[0])
        primary_subject = _normalize_signal(ranked_signals[0][1])

    return all_tokens, full_phrases, primary_subject


def _validate_question_hit(
    hit_doc: Dict,
    query_tokens: set,
    query_phrases: list,
    primary_subject: Optional[set],
    min_matches: int = 1,
) -> bool:
    """Validate a question hit against query signals using 4-level matching."""
    if not query_tokens:
        return True

    candidate_raw = (
        hit_doc.get('primary_keywords', []) +
        hit_doc.get('entities', []) +
        hit_doc.get('semantic_keywords', [])
    )

    if not candidate_raw:
        return False

    candidate_tokens  = set()
    candidate_phrases = []

    for val in candidate_raw:
        if not val:
            continue
        candidate_tokens.update(_normalize_signal(val))
        candidate_phrases.append(val.lower().strip())

    candidate_text = ' '.join(candidate_phrases)

    match_count         = 0
    primary_subject_hit = False

    exact_matches = query_tokens & candidate_tokens
    if exact_matches:
        match_count += len(exact_matches)
        if primary_subject and (primary_subject & exact_matches):
            primary_subject_hit = True

    for qt in query_tokens:
        if qt in exact_matches:
            continue
        for ct in candidate_tokens:
            if qt in ct or ct in qt:
                match_count += 1
                if primary_subject and qt in primary_subject:
                    primary_subject_hit = True
                break

    for qp in query_phrases:
        if len(qp) < 3:
            continue
        if qp in candidate_text:
            match_count += 1
            if primary_subject and _normalize_signal(qp) & primary_subject:
                primary_subject_hit = True
        else:
            for cp in candidate_phrases:
                if qp in cp or cp in qp:
                    match_count += 1
                    if primary_subject and _normalize_signal(qp) & primary_subject:
                        primary_subject_hit = True
                    break

    remaining_query = query_tokens - exact_matches
    token_overlap   = remaining_query & candidate_tokens
    if token_overlap:
        match_count += len(token_overlap)
        if primary_subject and (primary_subject & token_overlap):
            primary_subject_hit = True

    if match_count < min_matches:
        return False

    if primary_subject and len(query_tokens) >= 3:
        if not primary_subject_hit:
            return False

    return True


async def fetch_candidate_uuids(
    search_query: str,
    profile: Dict,
    signals: Dict = None,
    max_results: int = 100
) -> List[str]:
    """
    Stage 1A — keyword search against the document collection.
    Returns up to 100 document_uuid strings with no metadata.
    """
    signals    = signals or {}
    params     = build_typesense_params(profile, signals=signals)
    filter_str = build_filter_string_without_data_type(profile, signals=signals)
    query_mode = signals.get('query_mode', 'explore')

    print(f"🔍 Stage 1A (document): '{params.get('q', search_query)}'")
    print(f"   Mode: {query_mode} | Max: {max_results}")
    if filter_str:
        print(f"   Filters: {filter_str}")

    search_params = {
        'q':                     params.get('q', search_query),
        'query_by':              params.get('query_by',
            'primary_subject_name,document_title,entity_names,primary_keywords,secondary_subjects,key_facts,semantic_keywords'),
        'query_by_weights':      params.get('query_by_weights', '15,10,8,6,5,4,3'),
        'per_page':              max_results,
        'page':                  1,
        'include_fields':        'document_uuid',
        'num_typos':             params.get('num_typos', 0),
        'prefix':                params.get('prefix', 'no'),
        'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
        'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
    }

    if filter_str:
        search_params['filter_by'] = filter_str

    try:
        response = await asyncio.to_thread(
            client.collections[COLLECTION_NAME].documents.search,
            search_params
        )
        hits     = response.get('hits', [])
        uuids    = [
            hit['document']['document_uuid']
            for hit in hits
            if hit.get('document', {}).get('document_uuid')
        ]
        print(f"📊 Stage 1A: {len(uuids)} candidate UUIDs")
        return uuids
    except Exception as e:
        print(f"❌ Stage 1A error: {e}")
        return []


async def fetch_candidate_uuids_from_questions(
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
    max_results: int = 50,
    discovery: Dict = None,
) -> List[str]:
    """
    Stage 1B — vector search against the questions collection.
    """
    signals = signals or {}

    if not query_embedding:
        print("⚠️ Stage 1B (questions): no embedding — skipping")
        return []

    query_tokens, query_phrases, primary_subject = _extract_query_signals(
        profile, discovery=discovery
    )

    location_tokens = set()
    for c in profile.get('cities', []):
        location_tokens.update(_normalize_signal(c.get('name', '')))
    for s in profile.get('states', []):
        location_tokens.update(_normalize_signal(s.get('name', '')))
        for variant in s.get('variants', []):
            location_tokens.update(_normalize_signal(variant))

    print(f"🔍 Stage 1B validation signals:")
    print(f"   query_tokens    : {sorted(query_tokens)}")
    print(f"   query_phrases   : {query_phrases}")
    print(f"   primary_subject : {primary_subject}")
    if location_tokens:
        print(f"   location_tokens : {sorted(location_tokens)} (mandatory)")

    # ── Step A: Build facet filter ────────────────────────────────────────
    filter_parts = []

    primary_kws = [
        k.get('phrase') or k.get('word', '')
        for k in profile.get('keywords', [])
    ]
    primary_kws = [kw for kw in primary_kws if kw][:3]
    if primary_kws:
        kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
        filter_parts.append(f'primary_keywords:[{kw_values}]')

    entity_names = []
    for p in profile.get('persons', []):
        name = p.get('phrase') or p.get('word', '')
        rank = p.get('rank', 0)
        if name and (' ' in name or rank > 100):
            entity_names.append(name)
    for o in profile.get('organizations', []):
        name = o.get('phrase') or o.get('word', '')
        rank = o.get('rank', 0)
        if name and (' ' in name or rank > 100):
            entity_names.append(name)
    entity_names = [e for e in entity_names if e][:3]
    if entity_names:
        ent_values = ','.join([f'`{e}`' for e in entity_names])
        filter_parts.append(f'entities:[{ent_values}]')

    question_word     = signals.get('question_word') or ''
    question_type_map = {
        'when':  'TEMPORAL',
        'where': 'LOCATION',
        'who':   'PERSON',
        'what':  'FACTUAL',
        'which': 'FACTUAL',
        'why':   'REASON',
        'how':   'PROCESS',
    }
    question_type = question_type_map.get(question_word.lower(), '')
    if question_type:
        filter_parts.append(f'question_type:={question_type}')

    # ── Location filter ───────────────────────────────────────────────────
    location_filter_parts = []
    query_mode            = signals.get('query_mode', 'explore')
    is_location_subject   = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )

    if not is_location_subject:
        cities = profile.get('cities', [])
        if cities:
            city_filters = [f"location_city:=`{c['name']}`" for c in cities]
            location_filter_parts.append(
                city_filters[0] if len(city_filters) == 1
                else '(' + ' || '.join(city_filters) + ')'
            )

        states = profile.get('states', [])
        if states:
            state_conditions = [
                f"location_state:=`{variant}`"
                for state in states
                for variant in state.get('variants', [state['name']])
            ]
            location_filter_parts.append(
                state_conditions[0] if len(state_conditions) == 1
                else '(' + ' || '.join(state_conditions) + ')'
            )

    facet_filter    = ' || '.join(filter_parts) if filter_parts else ''
    location_filter = ' && '.join(location_filter_parts) if location_filter_parts else ''

    if facet_filter and location_filter:
        filter_str = f'({facet_filter}) && {location_filter}'
    elif location_filter:
        filter_str = location_filter
    else:
        filter_str = facet_filter

    print(f"   filter_by : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

    # ── Step B: Vector search ─────────────────────────────────────────────
    embedding_str = ','.join(str(x) for x in query_embedding)

    search_params = {
        'q':              '*',
        'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
        'per_page':       max_results * 2,
        'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
    }

    if filter_str:
        search_params['filter_by'] = filter_str

    try:
        search_requests = {'searches': [{'collection': 'questions', **search_params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        result          = response['results'][0]
        hits            = result.get('hits', [])

        if len(hits) < 5 and filter_str:
            fallback_filter = facet_filter if facet_filter else ''
            print(f"⚠️ Stage 1B: only {len(hits)} hits with location filter — "
                  f"retrying with facet filter only")

            sp_fallback = {**search_params}
            if fallback_filter:
                sp_fallback['filter_by'] = fallback_filter
            else:
                sp_fallback.pop('filter_by', None)

            r_fallback    = await asyncio.to_thread(
                client.multi_search.perform,
                {'searches': [{'collection': 'questions', **sp_fallback}]}, {}
            )
            fallback_hits = r_fallback['results'][0].get('hits', [])
            print(f"   Fallback returned {len(fallback_hits)} hits")

            if len(fallback_hits) < 5:
                print(f"⚠️ Stage 1B: retrying with no filter")
                sp_nofilter = {k: v for k, v in search_params.items() if k != 'filter_by'}
                r_nofilter  = await asyncio.to_thread(
                    client.multi_search.perform,
                    {'searches': [{'collection': 'questions', **sp_nofilter}]}, {}
                )
                hits = r_nofilter['results'][0].get('hits', [])
                print(f"   No-filter fallback returned {len(hits)} hits")
            else:
                hits = fallback_hits

        # ── Step C: Hard distance gate ────────────────────────────────────
        uuids    = []
        seen     = set()
        accepted = 0
        rejected = 0

        for hit in hits:
            doc           = hit.get('document', {})
            uuid          = doc.get('document_uuid')
            hit_distance  = hit.get('vector_distance', 1.0)

            if not uuid:
                continue

            if hit_distance >= QUESTION_SEMANTIC_DISTANCE_GATE:
                rejected += 1
                print(f"   🚫 Distance gate: '{doc.get('question', '')[:60]}' "
                      f"(distance={hit_distance:.4f} >= {QUESTION_SEMANTIC_DISTANCE_GATE})")
                continue

            is_valid = _validate_question_hit(
                hit_doc         = doc,
                query_tokens    = query_tokens,
                query_phrases   = query_phrases,
                primary_subject = primary_subject,
                min_matches     = 1,
            )

            if not is_valid:
                rejected += 1
                print(f"   ❌ Validation failed: '{doc.get('question', '')[:60]}' "
                      f"(distance={hit_distance:.4f})")
                continue

            if location_tokens:
                candidate_raw = (
                    doc.get('primary_keywords', []) +
                    doc.get('entities', []) +
                    doc.get('semantic_keywords', [])
                )
                candidate_tokens = set()
                for val in candidate_raw:
                    if val:
                        candidate_tokens.update(_normalize_signal(val))

                if not (location_tokens & candidate_tokens):
                    rejected += 1
                    print(f"   🚫 Location miss: '{doc.get('question', '')[:60]}' "
                          f"(need {sorted(location_tokens)}, not found)")
                    continue

            accepted += 1
            if uuid not in seen:
                seen.add(uuid)
                uuids.append(uuid)

            if len(uuids) >= max_results:
                break

        print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
              f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
        return uuids

    except Exception as e:
        print(f"❌ Stage 1B error: {e}")
        return []


async def fetch_all_candidate_uuids(
    search_query: str,
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
    discovery: Dict = None,
) -> List[str]:
    """
    Run Stage 1A (document) and Stage 1B (questions) in parallel.
    Merge order: overlap → document-only → question-only.
    """
    signals = signals or {}

    doc_uuids, q_uuids = await asyncio.gather(
        fetch_candidate_uuids(search_query, profile, signals, 100),
        fetch_candidate_uuids_from_questions(
            profile, query_embedding, signals, 50, discovery
        ),
    )

    doc_set = set(doc_uuids)
    q_set   = set(q_uuids)
    overlap  = doc_set & q_set

    merged = []
    seen   = set()

    for uuid in doc_uuids:
        if uuid in overlap and uuid not in seen:
            merged.append(uuid)
            seen.add(uuid)

    for uuid in doc_uuids:
        if uuid not in seen:
            merged.append(uuid)
            seen.add(uuid)

    for uuid in q_uuids:
        if uuid not in seen:
            merged.append(uuid)
            seen.add(uuid)

    print(f"📊 Stage 1 COMBINED:")
    print(f"   document pool  : {len(doc_uuids)}")
    print(f"   questions pool : {len(q_uuids)}")
    print(f"   overlap        : {len(overlap)}")
    print(f"   merged total   : {len(merged)}")

    return merged


# ============================================================
# METADATA FETCHING, RERANKING, COUNTING, FILTERING, PAGINATION
# ============================================================

# Shared metadata field list — used by both Stage 4 and the keyword path
# Stage 1 fetch. Includes the v5 subject fields plus all the existing
# fields the scorer needs.
_METADATA_INCLUDE_FIELDS = ','.join([
    # core identity
    'document_uuid', 'document_data_type', 'document_category',
    'document_schema', 'document_title', 'content_intent',
    # subject hierarchy (NEW v5)
    'primary_subject_name', 'primary_subject_type',
    'primary_subject_confidence', 'primary_subject_disambiguator',
    'secondary_subjects',
    # text-overlap fields needed by _compute_text_score
    'entity_names', 'primary_keywords', 'key_facts',
    'topic_tags', 'semantic_keywords', 'document_summary',
    # richness composite
    'content_depth_score', 'factual_density_score',
    'evergreen_score', 'subtopic_richness', 'concept_count',
    'authority_rank_score', 'word_count',
    # type-specific authority signals
    'authority_score',
    'service_rating', 'service_review_count', 'service_type',
    'product_rating', 'product_review_count',
    'recipe_rating', 'recipe_review_count',
    'media_rating',
    # filter + display
    'black_owned', 'image_url', 'logo_url',
])


def _doc_to_metadata(doc: Dict) -> Dict:
    """
    Map a Typesense document dict into the lightweight metadata record
    used by the scoring pipeline. Single source of truth so Stage 1 and
    Stage 4 can't drift.
    """
    return {
        # core identity
        'id':                          doc.get('document_uuid'),
        'data_type':                   doc.get('document_data_type', ''),
        'category':                    doc.get('document_category', ''),
        'schema':                      doc.get('document_schema', ''),
        'title':                       doc.get('document_title', ''),
        'content_intent':              doc.get('content_intent', ''),
        # subject hierarchy (NEW v5)
        'primary_subject_name':        doc.get('primary_subject_name', ''),
        'primary_subject_type':        doc.get('primary_subject_type', ''),
        'primary_subject_confidence':  doc.get('primary_subject_confidence', ''),
        'primary_subject_disambiguator': doc.get('primary_subject_disambiguator', ''),
        'secondary_subjects':          doc.get('secondary_subjects', []),
        # text-overlap fields
        'document_title':              doc.get('document_title', ''),
        'entity_names':                doc.get('entity_names', []),
        'primary_keywords':            doc.get('primary_keywords', []),
        'key_facts':                   doc.get('key_facts', []),
        'topic_tags':                  doc.get('topic_tags', []),
        'semantic_keywords':           doc.get('semantic_keywords', []),
        'document_summary':            doc.get('document_summary', ''),
        # richness composite
        'content_depth_score':         doc.get('content_depth_score', 0),
        'factual_density_score':       doc.get('factual_density_score', 0),
        'evergreen_score':             doc.get('evergreen_score', 0),
        'subtopic_richness':           doc.get('subtopic_richness', 0),
        'concept_count':               doc.get('concept_count', 0),
        'authority_rank_score':        doc.get('authority_rank_score', 0),
        'word_count':                  doc.get('word_count', 0),
        # type-specific authority signals
        'authority_score':             doc.get('authority_score', 0),
        'service_rating':              doc.get('service_rating', 0),
        'service_review_count':        doc.get('service_review_count', 0),
        'service_type':                doc.get('service_type', []),
        'product_rating':              doc.get('product_rating', 0),
        'product_review_count':        doc.get('product_review_count', 0),
        'recipe_rating':               doc.get('recipe_rating', 0),
        'recipe_review_count':         doc.get('recipe_review_count', 0),
        'media_rating':                doc.get('media_rating', 0),
        # filter + display
        'black_owned':                 doc.get('black_owned', False),
        'image_url':                   doc.get('image_url', []),
        'logo_url':                    doc.get('logo_url', []),
    }


async def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 250
) -> List[Dict]:
    """Stage 2 — pure vector ranking of the candidate pool."""
    if not candidate_ids or not query_embedding:
        return [
            {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
            for i, cid in enumerate(candidate_ids)
        ]

    ids_to_rerank = candidate_ids[:max_to_rerank]
    id_filter     = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
    embedding_str = ','.join(str(x) for x in query_embedding)

    params = {
        'q':              '*',
        'vector_query':   f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
        'filter_by':      f'document_uuid:[{id_filter}]',
        'per_page':       len(ids_to_rerank),
        'include_fields': 'document_uuid',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        hits            = response['results'][0].get('hits', [])

        reranked = [
            {
                'id':              hit['document'].get('document_uuid'),
                'vector_distance': hit.get('vector_distance', 1.0),
                'semantic_rank':   i,
            }
            for i, hit in enumerate(hits)
        ]

        reranked_ids = {r['id'] for r in reranked}
        for cid in ids_to_rerank:
            if cid not in reranked_ids:
                reranked.append({
                    'id':              cid,
                    'vector_distance': 1.0,
                    'semantic_rank':   len(reranked),
                })

        print(f"🎯 Stage 2: reranked {len(reranked)} candidates")
        return reranked

    except Exception as e:
        print(f"⚠️ Stage 2 error: {e}")
        return [
            {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
            for i, cid in enumerate(ids_to_rerank)
        ]


async def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
    """
    Stage 4 — fetch lightweight metadata for survivors. Batches in groups of 100.
    v5: include_fields now contains the 5 subject fields plus all
    fields the scorer needs (richness composite, etc.).
    """
    if not survivor_ids:
        return []

    BATCH_SIZE = 100
    doc_map    = {}

    async def _fetch_batch(batch_ids, batch_index):
        id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])
        params = {
            'q':              '*',
            'filter_by':      f'document_uuid:[{id_filter}]',
            'per_page':       len(batch_ids),
            'include_fields': _METADATA_INCLUDE_FIELDS,
        }
        try:
            search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
            response = await asyncio.to_thread(
                client.multi_search.perform,
                search_requests, {}
            )
            hits = response['results'][0].get('hits', [])
            batch_results = {}
            for hit in hits:
                doc  = hit.get('document', {})
                uuid = doc.get('document_uuid')
                if uuid:
                    batch_results[uuid] = _doc_to_metadata(doc)
            return batch_results
        except Exception as e:
            print(f"❌ Stage 4 metadata fetch error (batch {batch_index}): {e}")
            return {}

    batches = []
    for i in range(0, len(survivor_ids), BATCH_SIZE):
        batch_ids = survivor_ids[i:i + BATCH_SIZE]
        batches.append(_fetch_batch(batch_ids, i))

    batch_results = await asyncio.gather(*batches)

    for batch_map in batch_results:
        doc_map.update(batch_map)

    results = [doc_map[uuid] for uuid in survivor_ids if uuid in doc_map]
    print(f"📊 Stage 4: fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
    return results


async def fetch_candidates_with_metadata(
    search_query: str,
    profile: Dict,
    signals: Dict = None,
    max_results: int = MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    Keyword path only — fetch UUIDs and lightweight metadata together.
    v5: query_by includes primary_subject_name and secondary_subjects;
    include_fields includes all v5 subject fields.
    """
    signals    = signals or {}
    params     = build_typesense_params(profile, signals=signals)

    filter_str = params.get(
        'filter_by',
        build_filter_string_without_data_type(profile, signals=signals)
    )

    PAGE_SIZE    = 100
    all_results  = []
    current_page = 1
    max_pages    = (max_results // PAGE_SIZE) + 1
    query_mode   = signals.get('query_mode', 'explore')

    print(f"🔍 Stage 1 (keyword+metadata): '{params.get('q', search_query)}'")
    print(f"   Mode: {query_mode}")
    if filter_str:
        print(f"   Filters: {filter_str}")

    while len(all_results) < max_results and current_page <= max_pages:
        search_params = {
            'q':                     params.get('q', search_query),
            'query_by':              params.get('query_by',
                'primary_subject_name,document_title,entity_names,primary_keywords,secondary_subjects,key_facts,semantic_keywords'),
            'query_by_weights':      params.get('query_by_weights', '15,10,8,6,5,4,3'),
            'per_page':              PAGE_SIZE,
            'page':                  current_page,
            'include_fields':        _METADATA_INCLUDE_FIELDS,
            'num_typos':             params.get('num_typos', 0),
            'prefix':                params.get('prefix', 'no'),
            'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
            'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
        }

        if filter_str:
            search_params['filter_by'] = filter_str

        try:
            response = await asyncio.to_thread(
                client.collections[COLLECTION_NAME].documents.search,
                search_params
            )
            hits     = response.get('hits', [])
            found    = response.get('found', 0)

            if not hits:
                break

            for hit in hits:
                doc  = hit.get('document', {})
                meta = _doc_to_metadata(doc)
                meta['text_match'] = hit.get('text_match', 0)
                all_results.append(meta)

            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break

            current_page += 1

        except Exception as e:
            print(f"❌ Keyword fetch error (page {current_page}): {e}")
            break

    print(f"📊 Keyword path: {len(all_results)} candidates with metadata")
    return all_results[:max_results]


def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
    """Single pass counting by data_type, category, schema."""
    data_type_counts = {}
    category_counts  = {}
    schema_counts    = {}

    for item in cached_results:
        dt  = item.get('data_type', '')
        cat = item.get('category', '')
        sch = item.get('schema', '')
        if dt:
            data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
        if cat:
            category_counts[cat] = category_counts.get(cat, 0) + 1
        if sch:
            schema_counts[sch]   = schema_counts.get(sch, 0) + 1

    return {
        'data_type': [
            {'value': dt, 'count': c, 'label': DATA_TYPE_LABELS.get(dt, dt.title())}
            for dt, c in sorted(data_type_counts.items(), key=lambda x: -x[1])
        ],
        'category': [
            {'value': cat, 'count': c, 'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title())}
            for cat, c in sorted(category_counts.items(), key=lambda x: -x[1])
        ],
        'schema': [
            {'value': sch, 'count': c, 'label': sch}
            for sch, c in sorted(schema_counts.items(), key=lambda x: -x[1])
        ],
    }


def count_all(candidates: List[Dict]) -> Dict:
    """Stage 5 — single counting pass after all pruning and scoring is done."""
    facets      = count_facets_from_cache(candidates)
    image_count = _count_images_from_candidates(candidates)
    total       = len(candidates)

    print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
          f"data_types={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

    return {
        'facets':            facets,
        'facet_total':       total,
        'total_image_count': image_count,
    }


def filter_cached_results(
    cached_results: List[Dict],
    data_type: str = None,
    category: str  = None,
    schema: str    = None
) -> List[Dict]:
    """Filter the cached result set by UI-selected filters."""
    filtered = cached_results
    if data_type:
        filtered = [r for r in filtered if r.get('data_type') == data_type]
    if category:
        filtered = [r for r in filtered if r.get('category') == category]
    if schema:
        filtered = [r for r in filtered if r.get('schema') == schema]
    return filtered


def paginate_cached_results(
    cached_results: List[Dict],
    page: int,
    per_page: int
) -> Tuple[List[Dict], int]:
    """Slice the filtered result set to the requested page."""
    total = len(cached_results)
    start = (page - 1) * per_page
    end   = start + per_page
    if start >= total:
        return [], total
    return cached_results[start:end], total


# ============================================================
# DOCUMENT FETCHING, FORMATTING, AI OVERVIEW
# ============================================================

async def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
    """Fetch complete document records from Typesense for the current page only."""
    if not document_ids:
        return []

    id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])

    params = {
        'q':              '*',
        'filter_by':      f'document_uuid:[{id_filter}]',
        'per_page':       len(document_ids),
        'exclude_fields': 'embedding',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        hits            = response['results'][0].get('hits', [])

        doc_map = {
            hit['document']['document_uuid']: format_result(hit, query)
            for hit in hits
            if hit.get('document', {}).get('document_uuid')
        }

        return [doc_map[doc_id] for doc_id in document_ids if doc_id in doc_map]

    except Exception as e:
        print(f"❌ fetch_full_documents error: {e}")
        return []


async def fetch_documents_by_semantic_uuid(
    semantic_uuid: str,
    exclude_uuid: str = None,
    limit: int = 5
) -> List[Dict]:
    """Fetch documents that share the same semantic group."""
    if not semantic_uuid:
        return []

    filter_str = f'semantic_uuid:={semantic_uuid}'
    if exclude_uuid:
        filter_str += f' && document_uuid:!={exclude_uuid}'

    params = {
        'q':              '*',
        'filter_by':      filter_str,
        'per_page':       limit,
        'include_fields': 'document_uuid,document_title,document_url',
        'sort_by':        'authority_score:desc',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        hits            = response['results'][0].get('hits', [])

        related = [
            {
                'title': hit['document'].get('document_title', ''),
                'url':   hit['document'].get('document_url', ''),
                'id':    hit['document'].get('document_uuid', ''),
            }
            for hit in hits
            if hit.get('document', {}).get('document_uuid')
        ]

        print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
        return related

    except Exception as e:
        print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
        return []


async def fetch_questions_by_document_uuids(
    document_uuids: List[str],
    exclude_query: str = '',
    limit: int = 10
) -> List[Dict]:
    """Fetch questions from the questions collection by document UUIDs."""
    if not document_uuids:
        return []

    uuid_filter = ','.join([f'`{uuid}`' for uuid in document_uuids])

    params = {
        'q':              '*',
        'filter_by':      f'document_uuid:[{uuid_filter}]',
        'per_page':       limit,
        'include_fields': 'question,document_uuid,answer_type',
    }

    try:
        response = await asyncio.to_thread(
            client.collections['questions'].documents.search,
            params
        )
        hits = response.get('hits', [])

        exclude_lower = exclude_query.lower().strip()
        questions = [
            {
                'query':         hit['document'].get('question', ''),
                'document_uuid': hit['document'].get('document_uuid', ''),
                'answer_type':   hit['document'].get('answer_type', ''),
            }
            for hit in hits
            if hit.get('document', {}).get('question', '').lower().strip() != exclude_lower
        ]

        print(f"❓ Related questions: {len(questions)} for {len(document_uuids)} documents")
        return questions

    except Exception as e:
        print(f"❌ fetch_questions_by_document_uuids error: {e}")
        return []


async def fetch_documents_by_cluster_uuid(
    cluster_uuid: str,
    exclude_uuid: str = None,
    limit: int = 5
) -> List[Dict]:
    """Fetch documents that share the same cluster."""
    if not cluster_uuid:
        return []

    filter_str = f'cluster_uuid:={cluster_uuid}'
    if exclude_uuid:
        filter_str += f' && document_uuid:!={exclude_uuid}'

    params = {
        'q':              '*',
        'filter_by':      filter_str,
        'per_page':       limit,
        'include_fields': 'document_uuid,document_title,document_url',
        'sort_by':        'authority_score:desc',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response        = await asyncio.to_thread(
            client.multi_search.perform,
            search_requests, {}
        )
        hits            = response['results'][0].get('hits', [])

        related = [
            {
                'title': hit['document'].get('document_title', ''),
                'url':   hit['document'].get('document_url', ''),
                'id':    hit['document'].get('document_uuid', ''),
            }
            for hit in hits
            if hit.get('document', {}).get('document_uuid')
        ]

        print(f"🔗 Cluster docs: {len(related)} found for cluster_uuid={cluster_uuid[:12]}...")
        return related

    except Exception as e:
        print(f"❌ fetch_documents_by_cluster_uuid error: {e}")
        return []


def format_result(hit: Dict, query: str = '') -> Dict:
    """
    Transform a raw Typesense hit into the response format.
    v5: surfaces the 5 subject fields so the UI/debug tools can inspect them.
    """
    doc        = hit.get('document', {})
    highlights = hit.get('highlights', [])

    highlight_map = {
        h.get('field'): (
            h.get('value') or
            h.get('snippet') or
            (h.get('snippets') or [''])[0]
        )
        for h in highlights
    }

    vector_distance = hit.get('vector_distance')
    semantic_score  = round(1 - vector_distance, 3) if vector_distance is not None else None

    raw_date       = doc.get('published_date_string', '')
    formatted_date = ''
    if raw_date:
        try:
            if 'T' in raw_date:
                dt = datetime.strptime(raw_date.split('T')[0], '%Y-%m-%d')
            elif '-' in raw_date and len(raw_date) >= 10:
                dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
            else:
                dt = None
            formatted_date = dt.strftime('%b %d, %Y') if dt else raw_date
        except Exception:
            formatted_date = raw_date

    geopoint = (
        doc.get('location_geopoint') or
        doc.get('location_coordinates') or
        [None, None]
    )

    return {
        'id':                    doc.get('document_uuid'),
        'title':                 doc.get('document_title', 'Untitled'),
        'image_url':             doc.get('image_url') or [],
        'logo_url':              doc.get('logo_url') or [],
        'title_highlighted':     highlight_map.get('document_title', doc.get('document_title', '')),
        'summary':               doc.get('document_summary', ''),
        'summary_highlighted':   highlight_map.get('document_summary', doc.get('document_summary', '')),
        'url':                   doc.get('document_url', ''),
        'source':                doc.get('document_brand', 'unknown'),
        'site_name':             doc.get('document_brand', 'Website'),
        'image':                 (doc.get('image_url') or [None])[0],
        'category':              doc.get('document_category', ''),
        'data_type':             doc.get('document_data_type', ''),
        'schema':                doc.get('document_schema', ''),
        'date':                  formatted_date,
        'published_date':        formatted_date,
        'authority_score':       doc.get('authority_score', 0),
        'cluster_uuid':          doc.get('cluster_uuid'),
        'semantic_uuid':         doc.get('semantic_uuid'),
        'key_facts':             doc.get('key_facts', []),
        'humanized_summary':     '',
        'key_facts_highlighted': highlight_map.get('key_facts', ''),
        'semantic_score':        semantic_score,
        'black_owned':           doc.get('black_owned', False),
        'women_owned':           doc.get('women_owned', False),
        # ── v5 subject fields surfaced for diagnostics ────────────────
        'primary_subject_name':         doc.get('primary_subject_name', ''),
        'primary_subject_type':         doc.get('primary_subject_type', ''),
        'primary_subject_confidence':   doc.get('primary_subject_confidence', ''),
        'primary_subject_disambiguator': doc.get('primary_subject_disambiguator', ''),
        'secondary_subjects':           doc.get('secondary_subjects', []),
        # ── service / location / time period ──────────────────────────
        'service_rating':        doc.get('service_rating'),
        'service_review_count':  doc.get('service_review_count'),
        'service_type':          doc.get('service_type', []),
        'service_specialties':   doc.get('service_specialties', []),
        'service_price_range':   doc.get('service_price_range'),
        'service_phone':         doc.get('service_phone'),
        'service_hours':         doc.get('service_hours'),
        'location': {
            'city':    doc.get('location_city'),
            'state':   doc.get('location_state'),
            'country': doc.get('location_country'),
            'region':  doc.get('location_region'),
            'address': doc.get('location_address'),
            'geopoint': geopoint,
            'lat':     geopoint[0] if geopoint else None,
            'lng':     geopoint[1] if geopoint else None,
        },
        'time_period': {
            'start':   doc.get('time_period_start'),
            'end':     doc.get('time_period_end'),
            'context': doc.get('time_context'),
        },
        'score':           0.5,
        'related_sources': [],
    }


# ============================================================
# AI OVERVIEW
# ============================================================

def humanize_key_facts(
    key_facts: list,
    query: str = '',
    matched_keyword: str = '',
    question_word: str = None
) -> str:
    """Format key_facts into a readable AfroToDo AI Overview string."""
    if not key_facts:
        return ''

    facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
    if not facts:
        return ''

    if question_word:
        qw = question_word.lower()

        if qw == 'where':
            geo_words = {
                'located', 'bounded', 'continent', 'region', 'coast',
                'ocean', 'border', 'north', 'south', 'east', 'west',
                'latitude', 'longitude', 'hemisphere', 'capital',
                'city', 'state', 'country', 'area', 'lies', 'situated',
            }
            relevant_facts = [f for f in facts if any(gw in f.lower() for gw in geo_words)]

        elif qw == 'when':
            import re as _re
            temporal_words = {
                'founded', 'established', 'born', 'created', 'started',
                'opened', 'built', 'year', 'date', 'century', 'decade',
                'era', 'period',
            }
            relevant_facts = [
                f for f in facts
                if any(tw in f.lower() for tw in temporal_words)
                or _re.search(r'\b\d{4}\b', f)
            ]

        elif qw == 'who':
            who_words = {
                'first', 'president', 'founder', 'ceo', 'leader',
                'director', 'known', 'famous', 'awarded', 'pioneer',
                'invented', 'created', 'named', 'appointed', 'elected',
            }
            relevant_facts = [f for f in facts if any(ww in f.lower() for ww in who_words)]

        elif qw == 'what':
            what_words = {
                'is a', 'refers to', 'defined', 'known as',
                'type of', 'form of', 'means', 'represents',
            }
            relevant_facts = [f for f in facts if any(ww in f.lower() for ww in what_words)]

        else:
            relevant_facts = []

        if not relevant_facts and matched_keyword:
            keyword_lower  = matched_keyword.lower()
            relevant_facts = [f for f in facts if keyword_lower in f.lower()]

        if not relevant_facts:
            relevant_facts = [facts[0]]

    elif matched_keyword:
        keyword_lower  = matched_keyword.lower()
        relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        if not relevant_facts:
            relevant_facts = [facts[0]]
    else:
        relevant_facts = [facts[0]]

    relevant_facts = relevant_facts[:2]

    is_question = query and any(
        query.lower().startswith(w)
        for w in ['who', 'what', 'where', 'when', 'why', 'how',
                  'is', 'are', 'can', 'do', 'does']
    )

    if is_question:
        intros = [
            "Based on our sources,",
            "According to our data,",
            "From what we know,",
            "Our sources indicate that",
        ]
    else:
        intros = [
            "Here's what we know:",
            "From our sources:",
            "Based on our data:",
            "Our sources show that",
        ]

    intro = random.choice(intros)

    if len(relevant_facts) == 1:
        return f"{intro} {relevant_facts[0]}."
    else:
        return f"{intro} {relevant_facts[0]}. {relevant_facts[1]}."


def _should_trigger_ai_overview(
    signals: Dict,
    results: List[Dict],
    query: str
) -> bool:
    """Decide whether to show an AI Overview for this query."""
    if not results:
        return False

    query_mode = signals.get('query_mode', 'explore')

    if query_mode in ('browse', 'local', 'shop'):
        return False
    if query_mode in ('answer', 'compare'):
        return True

    if query_mode == 'explore':
        top_title = results[0].get('title', '').lower()
        top_facts = ' '.join(results[0].get('key_facts', [])).lower()
        stopwords  = {
            'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
            'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
            'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
        }
        query_words = [
            w for w in query.lower().split()
            if w not in stopwords and len(w) > 1
        ]
        if not query_words:
            return False
        matches = sum(1 for w in query_words if w in top_title or w in top_facts)
        return (matches / len(query_words)) >= 0.75

    return False


def _build_ai_overview(
    signals: Dict,
    results: List[Dict],
    query: str
) -> Optional[str]:
    """Build the AI Overview text using signal-driven key_fact selection."""
    if not results or not results[0].get('key_facts'):
        return None

    question_word = signals.get('question_word')
    stopwords     = {
        'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
        'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
        'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
    }
    query_words = [
        w for w in query.lower().split()
        if w not in stopwords and len(w) > 1
    ]

    matched_keyword = ''
    if query_words:
        top_title       = results[0].get('title', '').lower()
        top_facts       = ' '.join(results[0].get('key_facts', [])).lower()
        matched_keyword = max(
            query_words,
            key=lambda w: (w in top_title) + (w in top_facts)
        )

    return humanize_key_facts(
        results[0]['key_facts'],
        query,
        matched_keyword=matched_keyword,
        question_word=question_word,
    )


# ============================================================
# MAIN ENTRY POINT + COMPATIBILITY STUBS
# ============================================================

def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
    """Simple intent detection used only on the keyword path."""
    query_lower    = query.lower()
    location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
    if any(w in query_lower for w in location_words):
        return 'location'
    person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
    if any(w in query_lower for w in person_words):
        return 'person'
    return 'general'


async def execute_full_search(
    query: str,
    session_id: str = None,
    filters: Dict = None,
    page: int = 1,
    per_page: int = 20,
    user_location: Tuple[float, float] = None,
    pos_tags: List[Tuple] = None,
    safe_search: bool = True,
    alt_mode: str = 'y',
    answer: str = None,
    answer_type: str = None,
    skip_embedding: bool = False,
    document_uuid: str = None,
    search_source: str = None
) -> Dict:
    """
    Main entry point for search. Called by views.py.
    Now async — caller must use: result = await execute_full_search(...)
    """
    times = {}
    t0    = time.time()
    print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

    active_data_type = filters.get('data_type') if filters else None
    active_category  = filters.get('category')  if filters else None
    active_schema    = filters.get('schema')     if filters else None

    if filters:
        active_filters = {k: v for k, v in filters.items() if v}
        if active_filters:
            print(f"🎛️ Active UI filters: {active_filters}")

    # =========================================================================
    # QUESTION DIRECT PATH
    # =========================================================================
    if document_uuid and search_source == 'question':
        print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
        t_fetch = time.time()
        results = await fetch_full_documents([document_uuid], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        if results and results[0].get('cluster_uuid'):
            cluster_uuid = results[0]['cluster_uuid']
            try:
                cluster_docs = await fetch_documents_by_cluster_uuid(
                    cluster_uuid, exclude_uuid=document_uuid, limit=5
                )
                cluster_ids = [d['id'] for d in cluster_docs if d.get('id')]
                if cluster_ids:
                    cluster_results = await fetch_full_documents(cluster_ids, query)
                    results.extend(cluster_results)
                    print(f"   🔗 Cluster siblings: {len(cluster_results)} added "
                          f"from cluster={cluster_uuid[:12]}...")
            except Exception as e:
                print(f"⚠️ Cluster fetch error: {e}")

        ai_overview   = None
        question_word = None
        q_lower       = query.lower().strip()
        for word in ('who', 'what', 'where', 'when', 'why', 'how'):
            if q_lower.startswith(word):
                question_word = word
                break

        question_signals = {
            'query_mode':          'answer',
            'wants_single_result': True,
            'question_word':       question_word,
        }

        if results and results[0].get('key_facts'):
            ai_overview = _build_ai_overview(question_signals, results, query)
            if ai_overview:
                print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
                results[0]['humanized_summary'] = ai_overview

        all_doc_uuids = [document_uuid]
        for r in results[1:]:
            rid = r.get('id')
            if rid and rid != document_uuid:
                all_doc_uuids.append(rid)

        related_questions = await fetch_questions_by_document_uuids(
            all_doc_uuids, exclude_query=query, limit=10
        )

        times['total'] = round((time.time() - t0) * 1000, 2)

        return {
            'query':             query,
            'corrected_query':   query,
            'intent':            'answer',
            'query_mode':        'answer',
            'answer':            answer,
            'answer_type':       answer_type or 'UNKNOWN',
            'results':           results,
            'total':             len(results),
            'facet_total':       len(results),
            'total_image_count': 0,
            'page':              1,
            'per_page':          per_page,
            'search_time':       round(time.time() - t0, 3),
            'session_id':        session_id,
            'semantic_enabled':  False,
            'search_strategy':   'question_direct',
            'alt_mode':          alt_mode,
            'skip_embedding':    True,
            'search_source':     'question',
            'valid_terms':       query.split(),
            'unknown_terms':     [],
            'data_type_facets':  [],
            'category_facets':   [],
            'schema_facets':     [],
            'related_searches':  related_questions,
            'facets':            {},
            'word_discovery': {
                'valid_count':   len(query.split()),
                'unknown_count': 0,
                'corrections':   [],
                'filters':       [],
                'locations':     [],
                'sort':          None,
                'total_score':   0,
                'average_score': 0,
                'max_score':     0,
            },
            'timings':          times,
            'filters_applied': {
                'data_type':             None,
                'category':              None,
                'schema':                None,
                'is_local_search':       False,
                'local_search_strength': 'none',
            },
            'signals': question_signals,
            'profile': {},
        }

    # =========================================================================
    # FAST PATH — finished cache hit
    # =========================================================================
    stable_key = _generate_stable_cache_key(session_id, query)
    finished   = await _get_cached_results(stable_key)

    if finished is not None:
        print(f"⚡ FAST PATH: '{query}' | page={page} | "
              f"filter={active_data_type}/{active_category}/{active_schema}")

        all_results       = finished['all_results']
        all_facets        = finished['all_facets']
        facet_total       = finished['facet_total']
        ai_overview       = finished.get('ai_overview')
        total_image_count = finished.get('total_image_count', 0)
        metadata          = finished['metadata']
        times['cache']    = 'hit (fast path)'

        filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        t_fetch = time.time()
        results = await fetch_full_documents([item['id'] for item in page_items], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        if results and page == 1 and ai_overview:
            results[0]['humanized_summary'] = ai_overview

        times['total'] = round((time.time() - t0) * 1000, 2)
        signals        = metadata.get('signals', {})

        print(f"⏱️ FAST PATH TIMING: {times}")

        return {
            'query':             query,
            'corrected_query':   metadata.get('corrected_query', query),
            'intent':            metadata.get('intent', 'general'),
            'query_mode':        metadata.get('query_mode', 'keyword'),
            'results':           results,
            'total':             total_filtered,
            'facet_total':       facet_total,
            'total_image_count': total_image_count,
            'page':              page,
            'per_page':          per_page,
            'search_time':       round(time.time() - t0, 3),
            'session_id':        session_id,
            'semantic_enabled':  metadata.get('semantic_enabled', False),
            'search_strategy':   metadata.get('search_strategy', 'cached'),
            'alt_mode':          alt_mode,
            'skip_embedding':    skip_embedding,
            'search_source':     search_source,
            'valid_terms':       metadata.get('valid_terms', query.split()),
            'unknown_terms':     metadata.get('unknown_terms', []),
            'data_type_facets':  all_facets.get('data_type', []),
            'category_facets':   all_facets.get('category', []),
            'schema_facets':     all_facets.get('schema', []),
            'related_searches':  [],
            'facets':            all_facets,
            'word_discovery':    metadata.get('word_discovery', {
                'valid_count': len(query.split()), 'unknown_count': 0,
                'corrections': [], 'filters': [], 'locations': [],
                'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
            }),
            'timings':          times,
            'filters_applied':  metadata.get('filters_applied', {
                'data_type': active_data_type, 'category': active_category,
                'schema': active_schema, 'is_local_search': False,
                'local_search_strength': 'none',
            }),
            'signals': signals,
            'profile': metadata.get('profile', {}),
        }

    # =========================================================================
    # FULL PATH — no cache
    # =========================================================================
    print(f"🔬 FULL PATH: '{query}' (no cache for key={stable_key[:12]}...)")

    is_keyword_path = (
        alt_mode == 'n' or
        search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    )

    # =========================================================================
    # KEYWORD PATH — Stage 1 → 5 → 6 → 7
    # =========================================================================
    if is_keyword_path:
        print(f"⚡ KEYWORD PIPELINE: '{query}'")

        intent  = detect_query_intent(query, pos_tags)
        profile = {
            'search_terms':     query.split(),
            'cities':           [],
            'states':           [],
            'location_terms':   [],
            'primary_intent':   intent,
            'field_boosts': {
                'primary_subject_name': 15,
                'document_title':       10,
                'entity_names':          8,
                'primary_keywords':      6,
                'secondary_subjects':    5,
                'key_facts':             4,
                'semantic_keywords':     3,
            },
            'corrections':      [],
            'persons':          [],
            'organizations':    [],
            'keywords':         [],
            'media':            [],
            'preferred_data_types': ['article'],
        }

        t1          = time.time()
        all_results = await fetch_candidates_with_metadata(query, profile)
        times['stage1'] = round((time.time() - t1) * 1000, 2)

        counts = count_all(all_results)

        await _set_cached_results(stable_key, {
            'all_results':       all_results,
            'all_facets':        counts['facets'],
            'facet_total':       counts['facet_total'],
            'total_image_count': counts['total_image_count'],
            'ai_overview':       None,
            'metadata': {
                'corrected_query':  query,
                'intent':           intent,
                'query_mode':       'keyword',
                'semantic_enabled': False,
                'search_strategy':  'keyword_graph_filter',
                'valid_terms':      query.split(),
                'unknown_terms':    [],
                'signals':          {},
                'profile':          profile,
                'word_discovery': {
                    'valid_count': len(query.split()), 'unknown_count': 0,
                    'corrections': [], 'filters': [], 'locations': [],
                    'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
                },
                'filters_applied': {
                    'data_type': active_data_type, 'category': active_category,
                    'schema': active_schema, 'is_local_search': False,
                    'local_search_strength': 'none',
                },
            },
        })

        filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        t2      = time.time()
        results = await fetch_full_documents([item['id'] for item in page_items], query)
        times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
        times['total']      = round((time.time() - t0) * 1000, 2)

        print(f"⏱️ KEYWORD TIMING: {times}")

        return {
            'query':             query,
            'corrected_query':   query,
            'intent':            intent,
            'query_mode':        'keyword',
            'results':           results,
            'total':             total_filtered,
            'facet_total':       counts['facet_total'],
            'total_image_count': counts['total_image_count'],
            'page':              page,
            'per_page':          per_page,
            'search_time':       round(time.time() - t0, 3),
            'session_id':        session_id,
            'semantic_enabled':  False,
            'search_strategy':   'keyword_graph_filter',
            'alt_mode':          alt_mode,
            'skip_embedding':    True,
            'search_source':     search_source or 'dropdown',
            'valid_terms':       query.split(),
            'unknown_terms':     [],
            'data_type_facets':  counts['facets'].get('data_type', []),
            'category_facets':   counts['facets'].get('category', []),
            'schema_facets':     counts['facets'].get('schema', []),
            'related_searches':  [],
            'facets':            counts['facets'],
            'word_discovery': {
                'valid_count': len(query.split()), 'unknown_count': 0,
                'corrections': [], 'filters': [], 'locations': [],
                'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
            },
            'timings':          times,
            'filters_applied': {
                'data_type': active_data_type, 'category': active_category,
                'schema': active_schema, 'is_local_search': False,
                'local_search_strength': 'none',
            },
            'signals': {},
            'profile': profile,
        }

    # =========================================================================
    # SEMANTIC PATH — Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
    # =========================================================================
    print(f"🔬 SEMANTIC PIPELINE: '{query}'")

    # Stage 0 — Word Discovery + embedding in parallel
    t1 = time.time()
    discovery, query_embedding = await run_parallel_prep(query, skip_embedding=skip_embedding)
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

    # Intent detection
    signals = {}
    if INTENT_DETECT_AVAILABLE:
        try:
            discovery = await asyncio.to_thread(detect_intent, discovery)
            signals   = discovery.get('signals', {})
            print(f"   🎯 Intent: mode={signals.get('query_mode')} "
                  f"domain={signals.get('primary_domain')} "
                  f"local={signals.get('is_local_search')} "
                  f"black_owned={signals.get('has_black_owned')} "
                  f"superlative={signals.get('has_superlative')}")
        except Exception as e:
            print(f"   ⚠️ intent_detect error: {e}")

    corrected_query  = discovery.get('corrected_query', query)
    semantic_enabled = query_embedding is not None
    query_mode       = signals.get('query_mode', 'explore')

    # Read v3 profile
    t2      = time.time()
    profile = _read_v3_profile(discovery, signals=signals)
    times['read_profile'] = round((time.time() - t2) * 1000, 2)

    # Apply pos_mismatch corrections to search_terms
    corrections = discovery.get('corrections', [])
    if corrections:
        correction_map = {
            c['original'].lower(): c['corrected']
            for c in corrections
            if c.get('original') and c.get('corrected')
            and c.get('correction_type') == 'pos_mismatch'
        }
        if correction_map:
            original_terms          = profile['search_terms']
            profile['search_terms'] = [
                correction_map.get(t.lower(), t) for t in original_terms
            ]

    intent      = profile['primary_intent']
    city_names  = [c['name'] for c in profile['cities']]
    state_names = [s['name'] for s in profile['states']]

    print(f"   Intent: {intent} | Mode: {query_mode}")
    print(f"   Cities: {city_names} | States: {state_names}")
    print(f"   Search Terms: {profile['search_terms']}")

    # Stage 1 — candidate UUIDs
    t3 = time.time()

    UNSAFE_CATEGORIES = {
        'Food', 'US City', 'US State', 'Country', 'Location',
        'City', 'Place', 'Object', 'Animal', 'Color',
    }
    has_unsafe_corrections = any(
        c.get('correction_type') == 'pos_mismatch' or
        c.get('category', '') in UNSAFE_CATEGORIES
        for c in corrections
    )
    search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

    candidate_uuids = await fetch_all_candidate_uuids(
        search_query_for_stage1, profile, query_embedding,
        signals=signals, discovery=discovery,
    )
    times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)

    # Stage 2 + 3 — vector rerank + distance prune
    survivor_uuids = candidate_uuids
    vector_data    = {}

    if semantic_enabled and candidate_uuids:
        t4       = time.time()
        reranked = await semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
        times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

        vector_data = {
            item['id']: {
                'vector_distance': item.get('vector_distance', 1.0),
                'semantic_rank':   item.get('semantic_rank', 999999),
            }
            for item in reranked
        }

        DISTANCE_THRESHOLDS = {
            'answer':  0.60,
            'explore': 0.70,
            'compare': 0.65,
            'browse':  0.85,
            'local':   0.85,
            'shop':    0.80,
        }
        threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

        before_prune   = len(candidate_uuids)
        survivor_uuids = [
            uuid for uuid in candidate_uuids
            if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
        ]
        after_prune = len(survivor_uuids)

        if before_prune != after_prune:
            print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): "
                  f"{before_prune} → {after_prune} "
                  f"({before_prune - after_prune} removed)")
        times['stage3_prune'] = f"{before_prune} → {after_prune}"

    else:
        print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, "
              f"candidates={len(candidate_uuids)}")

    # Stage 4 — metadata for survivors
    t5          = time.time()
    all_results = await fetch_candidate_metadata(survivor_uuids)
    times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

    # Stage 5 — resolve blend once, score every document (CPU-bound, stays sync)
    if all_results:
        blend      = _resolve_blend(query_mode, signals, all_results)
        pool_size  = len(all_results)

        for idx, item in enumerate(all_results):
            _score_document(
                idx        = idx,
                item       = item,
                profile    = profile,
                signals    = signals,
                blend      = blend,
                pool_size  = pool_size,
                vector_data = vector_data,
            )

        all_results.sort(key=lambda x: -x.get('blended_score', 0))
        for i, item in enumerate(all_results):
            item['rank'] = i

    counts = count_all(all_results)

    # AI Overview preview
    ai_overview = None
    if all_results:
        preview_items, _ = paginate_cached_results(all_results, 1, per_page)
        preview_docs     = await fetch_full_documents([item['id'] for item in preview_items], query)
        if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
            ai_overview = _build_ai_overview(signals, preview_docs, query)
            if ai_overview:
                print(f"   💡 AI Overview: {ai_overview[:80]}...")

    valid_terms   = profile['search_terms']
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

    locations_block = (
        [
            {'field': 'location_city',  'values': city_names},
            {'field': 'location_state', 'values': state_names},
        ]
        if city_names or state_names else []
    )

    # Stage 6 — cache the finished package
    await _set_cached_results(stable_key, {
        'all_results':       all_results,
        'all_facets':        counts['facets'],
        'facet_total':       counts['facet_total'],
        'total_image_count': counts['total_image_count'],
        'ai_overview':       ai_overview,
        'metadata': {
            'corrected_query':  corrected_query,
            'intent':           intent,
            'query_mode':       query_mode,
            'semantic_enabled': semantic_enabled,
            'search_strategy':  'staged_semantic' if semantic_enabled else 'keyword_fallback',
            'valid_terms':      valid_terms,
            'unknown_terms':    unknown_terms,
            'signals':          signals,
            'city_names':       city_names,
            'state_names':      state_names,
            'profile':          profile,
            'word_discovery': {
                'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
                'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
                'corrections':   discovery.get('corrections', []),
                'filters':       [],
                'locations':     locations_block,
                'sort':          None,
                'total_score':   0,
                'average_score': 0,
                'max_score':     0,
            },
            'filters_applied': {
                'data_type':             active_data_type,
                'category':              active_category,
                'schema':                active_schema,
                'is_local_search':       signals.get('is_local_search', False),
                'local_search_strength': signals.get('local_search_strength', 'none'),
                'has_black_owned':       signals.get('has_black_owned', False),
                'graph_filters':         [],
                'graph_locations':       locations_block,
                'graph_sort':            None,
            },
        },
    })
    print(f"💾 Cached semantic package: {counts['facet_total']} results, "
          f"{counts['total_image_count']} image docs")

    # Stage 7 — filter → paginate → fetch full docs
    filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

    t6      = time.time()
    results = await fetch_full_documents([item['id'] for item in page_items], query)
    times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

    if results and page == 1 and ai_overview:
        results[0]['humanized_summary'] = ai_overview

    if query_embedding:
        try:
            await asyncio.to_thread(
                store_query_embedding,
                corrected_query, query_embedding,
                result_count=counts['facet_total']
            )
        except Exception as e:
            print(f"⚠️ store_query_embedding error: {e}")

    times['total'] = round((time.time() - t0) * 1000, 2)
    strategy       = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

    print(f"⏱️ SEMANTIC TIMING: {times}")
    print(f"🔍 {strategy.upper()} ({query_mode}) | "
          f"Total: {counts['facet_total']} | "
          f"Filtered: {total_filtered} | "
          f"Page: {len(results)} | "
          f"Images: {counts['total_image_count']}")

    return {
        'query':             query,
        'corrected_query':   corrected_query,
        'intent':            intent,
        'query_mode':        query_mode,
        'results':           results,
        'total':             total_filtered,
        'facet_total':       counts['facet_total'],
        'total_image_count': counts['total_image_count'],
        'page':              page,
        'per_page':          per_page,
        'search_time':       round(time.time() - t0, 3),
        'session_id':        session_id,
        'semantic_enabled':  semantic_enabled,
        'search_strategy':   strategy,
        'alt_mode':          alt_mode,
        'skip_embedding':    skip_embedding,
        'search_source':     search_source,
        'valid_terms':       valid_terms,
        'unknown_terms':     unknown_terms,
        'related_searches':  [],
        'data_type_facets':  counts['facets'].get('data_type', []),
        'category_facets':   counts['facets'].get('category', []),
        'schema_facets':     counts['facets'].get('schema', []),
        'facets':            counts['facets'],
        'word_discovery': {
            'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
            'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
            'corrections':   discovery.get('corrections', []),
            'filters':       [],
            'locations':     locations_block,
            'sort':          None,
            'total_score':   0,
            'average_score': 0,
            'max_score':     0,
        },
        'timings': times,
        'filters_applied': {
            'data_type':             active_data_type,
            'category':              active_category,
            'schema':                active_schema,
            'is_local_search':       signals.get('is_local_search', False),
            'local_search_strength': signals.get('local_search_strength', 'none'),
            'has_black_owned':       signals.get('has_black_owned', False),
            'graph_filters':         [],
            'graph_locations':       locations_block,
            'graph_sort':            None,
        },
        'signals': signals,
        'profile': profile,
    }


# ============================================================
# COMPATIBILITY STUBS — keep views.py imports working
# ============================================================

def get_facets(query: str) -> dict:
    """Returns empty dict. Kept for views.py import compatibility."""
    return {}


def get_related_searches(query: str, intent: str) -> list:
    """Returns empty list. Kept for views.py import compatibility."""
    return []


def get_featured_result(query: str, intent: str, results: list) -> dict:
    """Returns a featured snippet if the top result has high authority."""
    if not results:
        return None
    top = results[0]
    if top.get('authority_score', 0) >= 85:
        return {
            'type':      'featured_snippet',
            'title':     top.get('title'),
            'snippet':   top.get('summary', ''),
            'key_facts': top.get('key_facts', [])[:3],
            'source':    top.get('source'),
            'url':       top.get('url'),
            'image':     top.get('image'),
        }
    return None


def log_search_event(**kwargs):
    """No-op. Kept for views.py import compatibility."""
    pass


async def typesense_search(
    query: str = '*',
    filter_by: str = None,
    sort_by: str = 'authority_score:desc',
    per_page: int = 20,
    page: int = 1,
    facet_by: str = None,
    query_by: str = 'document_title,document_summary,keywords,primary_keywords',
    max_facet_values: int = 20,
) -> Dict:
    """Simple Typesense search wrapper for direct use outside the pipeline."""
    params = {
        'q':        query,
        'query_by': query_by,
        'per_page': per_page,
        'page':     page,
    }
    if filter_by:
        params['filter_by'] = filter_by
    if sort_by:
        params['sort_by'] = sort_by
    if facet_by:
        params['facet_by']         = facet_by
        params['max_facet_values'] = max_facet_values

    try:
        return await asyncio.to_thread(
            client.collections[COLLECTION_NAME].documents.search,
            params
        )
    except Exception as e:
        print(f"❌ typesense_search error: {e}")
        return {'hits': [], 'found': 0, 'error': str(e)}


# ============================================================
# END OF FILE
# ============================================================