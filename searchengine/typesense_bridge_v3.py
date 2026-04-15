# # ============================================================
# # FILE: typesense_discovery_bridge.py
# # ============================================================
# # HOW TO USE THIS FILE:
# #   Written in 8 parts. Paste all parts together in order
# #   into one single .py file.
# #   PART 1 → PART 2 → PART 3 → PART 4 → PART 5 →
# #   PART 6 → PART 7 → PART 8
# # ============================================================


# # ============================================================
# # PART 1 OF 8 — IMPORTS, CONSTANTS, UTILITY FUNCTIONS
# # ============================================================

# """
# typesense_discovery_bridge.py
# =============================
# AfroToDo Search Bridge — Word Discovery v3 + Intent Detection + Typesense.

# SCORING ALGORITHM (v4)
# ----------------------
# final_score = (
#     blend['text_match'] * text_score      +
#     blend['semantic']   * semantic_score  +
#     blend['authority']  * authority_score_n
# )
# final_score *= _domain_relevance(doc, signals)
# final_score *= _content_intent_match(doc, query_mode)
# final_score *= _pool_type_multiplier(doc, query_mode)

# PIPELINE
# --------
# SEMANTIC:  1A+1B → 2 (rerank) → 3 (prune) → 4 (metadata) → 5 (score+count) → 6 (cache) → 7 (paginate+fetch)
# KEYWORD:   1 (uuids+metadata) → 5 (count) → 6 (cache) → 7 (paginate+fetch)
# QUESTION:  direct fetch → format → return
# """

# import re
# import json
# import math
# import time
# import hashlib
# import threading
# import typesense
# from typing import Dict, List, Tuple, Optional, Any, Set
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# from decouple import config
# import requests
# import random


# # ── Word Discovery v3 ────────────────────────────────────────────────────────

# try:
#     from .word_discovery_fulltest import WordDiscovery
#     WORD_DISCOVERY_AVAILABLE = True
#     print("✅ WordDiscovery imported from .word_discovery_fulltest")
# except ImportError:
#     try:
#         from word_discovery_fulltest import WordDiscovery
#         WORD_DISCOVERY_AVAILABLE = True
#         print("✅ WordDiscovery imported from word_discovery_fulltest")
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ WordDiscovery not available")


# # ── Intent Detection ─────────────────────────────────────────────────────────

# try:
#     from .intent_detect import detect_intent, get_signals
#     INTENT_DETECT_AVAILABLE = True
#     print("✅ intent_detect imported")
# except ImportError:
#     try:
#         from intent_detect import detect_intent, get_signals
#         INTENT_DETECT_AVAILABLE = True
#         print("✅ intent_detect imported (fallback)")
#     except ImportError:
#         INTENT_DETECT_AVAILABLE = False
#         print("⚠️ intent_detect not available")


# # ── Embedding Client ─────────────────────────────────────────────────────────

# try:
#     from .embedding_client import get_query_embedding
#     print("✅ get_query_embedding imported from .embedding_client")
# except ImportError:
#     try:
#         from embedding_client import get_query_embedding
#         print("✅ get_query_embedding imported from embedding_client")
#     except ImportError:
#         def get_query_embedding(query: str) -> Optional[List[float]]:
#             print("⚠️ embedding_client not available")
#             return None


# # ── Related Search Store ─────────────────────────────────────────────────────

# try:
#     from .cached_embedding_related_search import store_query_embedding
#     print("✅ store_query_embedding imported")
# except ImportError:
#     try:
#         from cached_embedding_related_search import store_query_embedding
#         print("✅ store_query_embedding imported (fallback)")
#     except ImportError:
#         def store_query_embedding(*args, **kwargs):
#             return False
#         print("⚠️ store_query_embedding not available")


# # ── Django Cache ─────────────────────────────────────────────────────────────

# from django.core.cache import cache as django_cache


# # ── Thread Pool ──────────────────────────────────────────────────────────────

# _executor = ThreadPoolExecutor(max_workers=3)


# # ── Typesense Client ─────────────────────────────────────────────────────────

# client = typesense.Client({
#     'api_key':  config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host':     config('TYPESENSE_HOST'),
#         'port':     config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL'),
#     }],
#     'connection_timeout_seconds': 5,
# })

# COLLECTION_NAME = 'document'


# # ── Cache Settings ───────────────────────────────────────────────────────────

# CACHE_TTL_SECONDS  = 300
# MAX_CACHED_RESULTS = 100


# # ── UI Labels ────────────────────────────────────────────────────────────────

# DATA_TYPE_LABELS = {
#     'article':  'Articles',
#     'person':   'People',
#     'business': 'Businesses',
#     'place':    'Places',
#     'media':    'Media',
#     'event':    'Events',
#     'product':  'Products',
# }

# CATEGORY_LABELS = {
#     'healthcare_medical': 'Healthcare',
#     'fashion':            'Fashion',
#     'beauty':             'Beauty',
#     'food_recipes':       'Food & Recipes',
#     'travel_tourism':     'Travel',
#     'entertainment':      'Entertainment',
#     'business':           'Business',
#     'education':          'Education',
#     'technology':         'Technology',
#     'sports':             'Sports',
#     'finance':            'Finance',
#     'real_estate':        'Real Estate',
#     'lifestyle':          'Lifestyle',
#     'news':               'News',
#     'culture':            'Culture',
#     'general':            'General',
# }

# US_STATE_ABBREV = {
#     'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
#     'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
#     'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
#     'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
#     'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
#     'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
#     'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
#     'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
#     'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
#     'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
#     'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
#     'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
#     'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
# }


# # ── Blend Ratios (base — _resolve_blend adjusts at runtime) ──────────────────

# BLEND_RATIOS = {
#     'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
#     'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
#     'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
#     'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
# }


# # ── Pool Scoping per Query Mode ───────────────────────────────────────────────

# POOL_SCOPE = {
#     'local':   {'primary': 'business',  'allow': {'business', 'place'}},
#     'shop':    {'primary': 'product',   'allow': {'product', 'business'}},
#     'answer':  {'primary': None,        'allow': {'article', 'person', 'place'}},
#     'browse':  {'primary': None,        'allow': {'article', 'business', 'product'}},
#     'explore': {'primary': None,        'allow': {'article', 'person', 'media'}},
#     'compare': {'primary': None,        'allow': {'article', 'person', 'business'}},
# }


# # ── Data Type Preferences ─────────────────────────────────────────────────────

# DATA_TYPE_PREFERENCES = {
#     'answer':  ['article', 'person', 'place'],
#     'explore': ['article', 'person', 'media'],
#     'browse':  ['article', 'business', 'product'],
#     'local':   ['business', 'place', 'article'],
#     'shop':    ['product', 'business', 'article'],
#     'compare': ['article', 'person', 'business'],
# }


# # ── Domain → Document Category Alignment ─────────────────────────────────────

# DOMAIN_CATEGORY_MAP = {
#     'food':        {'food_recipes', 'dining', 'lifestyle'},
#     'business':    {'business', 'finance', 'entrepreneurship'},
#     'health':      {'healthcare_medical', 'wellness', 'fitness'},
#     'music':       {'entertainment', 'music', 'culture'},
#     'fashion':     {'fashion', 'beauty', 'lifestyle'},
#     'education':   {'education', 'hbcu', 'scholarship'},
#     'travel':      {'travel_tourism', 'lifestyle'},
#     'real_estate': {'real_estate', 'business'},
#     'sports':      {'sports', 'entertainment'},
#     'technology':  {'technology', 'business'},
#     'beauty':      {'beauty', 'lifestyle', 'fashion'},
#     'culture':     {'culture', 'news', 'lifestyle'},
# }


# # ── Content Intent Alignment per Query Mode ───────────────────────────────────

# INTENT_CONTENT_MAP = {
#     'local':   {'transactional', 'navigational'},
#     'shop':    {'transactional', 'commercial'},
#     'answer':  {'informational', 'educational'},
#     'browse':  {'informational', 'commercial', 'transactional'},
#     'explore': {'informational', 'educational'},
#     'compare': {'informational', 'commercial'},
# }


# # ── Scoring Thresholds ────────────────────────────────────────────────────────

# SEMANTIC_DISTANCE_GATE    = 0.65
# QUESTION_SEMANTIC_DISTANCE_GATE = 0.40  
# REVIEW_COUNT_SCALE_BIZ    = 500
# REVIEW_COUNT_SCALE_RECIPE = 200
# BLACK_OWNED_BOOST         = 0.12
# PREFERRED_TYPE_BOOST      = 0.08
# SUPERLATIVE_SCORE_CAP     = 0.70


# # ── Utility Functions ─────────────────────────────────────────────────────────

# def _parse_rank(rank_value: Any) -> int:
#     """Safely convert any rank value to an integer."""
#     if isinstance(rank_value, int):
#         return rank_value
#     try:
#         return int(float(rank_value))
#     except (ValueError, TypeError):
#         return 0


# def _has_real_images(item: Dict) -> bool:
#     """Return True if the candidate has at least one non-empty image or logo URL."""
#     image_urls = item.get('image_url', [])
#     if isinstance(image_urls, str):
#         image_urls = [image_urls]
#     if any(u for u in image_urls if u):
#         return True
#     logo_urls = item.get('logo_url', [])
#     if isinstance(logo_urls, str):
#         logo_urls = [logo_urls]
#     return any(u for u in logo_urls if u)


# def _count_images_from_candidates(all_results: List[Dict]) -> int:
#     """Count documents in the result set that have at least one real image."""
#     return sum(1 for item in all_results if _has_real_images(item))


# def _generate_stable_cache_key(session_id: str, query: str) -> str:
#     """Build a deterministic MD5 cache key from session ID and normalized query."""
#     normalized = query.strip().lower()
#     key_string = f"final|{session_id or 'nosession'}|{normalized}"
#     return hashlib.md5(key_string.encode()).hexdigest()


# # ============================================================
# # END OF PART 1
# # ============================================================


# # ============================================================
# # PART 2 OF 8 — CACHE FUNCTIONS + WORD DISCOVERY RUNNERS
# # ============================================================

# def _get_cached_results(cache_key: str):
#     """
#     Get the finished result package from Redis.
#     Returns the cached dict or None on miss or error.
#     """
#     try:
#         data = django_cache.get(cache_key)
#         if data is not None:
#             print(f"🟢 Redis cache HIT: {cache_key[:12]}...")
#             return data
#         print(f"🔴 Redis cache MISS: {cache_key[:12]}...")
#         return None
#     except Exception as e:
#         print(f"⚠️ Redis cache GET error: {e}")
#         return None


# def _set_cached_results(cache_key: str, data: Dict) -> None:
#     """
#     Write the finished result package to Redis with TTL.
#     Silently absorbs errors so a cache failure never breaks search.
#     """
#     try:
#         django_cache.set(cache_key, data, timeout=CACHE_TTL_SECONDS)
#         print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
#     except Exception as e:
#         print(f"⚠️ Redis cache SET error: {e}")


# def clear_search_cache() -> None:
#     """Clear all cached search results."""
#     try:
#         django_cache.clear()
#         print("🧹 Redis search cache cleared")
#     except Exception as e:
#         print(f"⚠️ Redis cache CLEAR error: {e}")


# def _run_word_discovery(query: str) -> Dict:
#     """
#     Run Word Discovery v3 on the query string.
#     Returns the full pre-classified profile dict.
#     Falls back to a minimal safe structure if WD is unavailable.
#     """
#     if WORD_DISCOVERY_AVAILABLE:
#         try:
#             wd     = WordDiscovery(verbose=False)
#             result = wd.process(query)
#             return result
#         except Exception as e:
#             print(f"⚠️ WordDiscovery error: {e}")

#     return {
#         'query':                   query,
#         'corrected_query':         query,
#         'corrected_display_query': query,
#         'search_terms':            [],
#         'persons':                 [],
#         'organizations':           [],
#         'keywords':                [],
#         'media':                   [],
#         'cities':                  [],
#         'states':                  [],
#         'location_terms':          [],
#         'primary_intent':          'general',
#         'intent_scores':           {},
#         'field_boosts':            {},
#         'corrections':             [],
#         'terms':                   [],
#         'ngrams':                  [],
#         'stats': {
#             'total_words':     len(query.split()),
#             'valid_words':     0,
#             'corrected_words': 0,
#             'unknown_words':   len(query.split()),
#             'stopwords':       0,
#             'ngram_count':     0,
#         },
#     }


# def _run_embedding(query: str) -> Optional[List[float]]:
#     """
#     Call the embedding client and return the query vector.
#     Returns None if the client is unavailable — pipeline falls back to keyword-only.
#     """
#     return get_query_embedding(query)


# def run_parallel_prep(
#     query: str,
#     skip_embedding: bool = False
# ) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run Word Discovery v3 and embedding generation in parallel.

#     FIX — frozenset serialization bug:
#         WD v3 writes context_flags as a frozenset on each term dict.
#         frozenset is not JSON-serializable and silently breaks Redis
#         caching, causing every request to bypass cache and run the
#         full pipeline. This function converts every context_flags
#         value to a sorted list before returning.

#     Embedding re-use logic:
#         Always embeds the original query first.
#         Only re-embeds with corrected_query when all corrections are
#         safe. Unsafe categories (Food, US City, US State, Country,
#         Location, City, Place, Object, Animal, Color) are never
#         re-embedded because replacing them changes semantic meaning.
#     """
#     if skip_embedding:
#         discovery = _run_word_discovery(query)
#         for term in discovery.get('terms', []):
#             if isinstance(term.get('context_flags'), (frozenset, set)):
#                 term['context_flags'] = sorted(list(term['context_flags']))
#         return discovery, None

#     discovery_future = _executor.submit(_run_word_discovery, query)
#     embedding_future = _executor.submit(_run_embedding, query)

#     discovery = discovery_future.result()
#     embedding = embedding_future.result()

#     # FIX — convert frozenset context_flags to sorted list
#     for term in discovery.get('terms', []):
#         if isinstance(term.get('context_flags'), (frozenset, set)):
#             term['context_flags'] = sorted(list(term['context_flags']))

#     corrected_query = discovery.get('corrected_query', query)

#     if corrected_query.lower() != query.lower() and embedding is not None:
#         corrections = discovery.get('corrections', [])

#         UNSAFE_CATEGORIES = {
#             'Food', 'US City', 'US State', 'Country', 'Location',
#             'City', 'Place', 'Object', 'Animal', 'Color',
#         }

#         safe_corrections   = []
#         unsafe_corrections = []

#         for c in corrections:
#             corrected_category = c.get('category', '')
#             correction_type    = c.get('correction_type', '')

#             if (correction_type == 'pos_mismatch' or
#                     corrected_category in UNSAFE_CATEGORIES or
#                     c.get('category', '') in ('Person', 'Organization', 'Brand')):
#                 unsafe_corrections.append(c)
#             else:
#                 safe_corrections.append(c)

#         if unsafe_corrections:
#             print(f"⚠️ Skipping re-embed — unsafe corrections detected:")
#             for c in unsafe_corrections:
#                 print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
#                       f"(type={c.get('correction_type')}, category={c.get('category')})")
#         elif safe_corrections:
#             print(f"✅ Re-embedding with corrected query: '{corrected_query}'")
#             embedding = get_query_embedding(corrected_query)

#     return discovery, embedding


# # ============================================================
# # END OF PART 2
# # ============================================================

# # ============================================================
# # PART 3 OF 8 — V3 PROFILE READER + TYPESENSE PARAMETER BUILDERS
# # ============================================================

# def _read_v3_profile(discovery: Dict, signals: Dict = None) -> Dict:
#     """
#     Read the pre-classified v3 profile directly.
#     O(1) field reads with safe defaults — no re-classification.
#     Adds preferred_data_types from a single dict lookup on query_mode.
#     """
#     query_mode = (signals or {}).get('query_mode', 'explore')

#     return {
#         'search_terms':      discovery.get('search_terms', []),
#         'persons':           discovery.get('persons', []),
#         'organizations':     discovery.get('organizations', []),
#         'keywords':          discovery.get('keywords', []),
#         'media':             discovery.get('media', []),
#         'cities':            discovery.get('cities', []),
#         'states':            discovery.get('states', []),
#         'location_terms':    discovery.get('location_terms', []),
#         'primary_intent':    discovery.get('primary_intent', 'general'),
#         'intent_scores':     discovery.get('intent_scores', {}),
#         'field_boosts':      discovery.get('field_boosts', {
#             'document_title':   10,
#             'entity_names':      2,
#             'primary_keywords':  3,
#             'key_facts':         3,
#             'semantic_keywords': 2,
#         }),
#         'corrections':       discovery.get('corrections', []),
#         'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
#         'has_person':        bool(discovery.get('persons')),
#         'has_organization':  bool(discovery.get('organizations')),
#         'has_location':      bool(
#             discovery.get('cities') or
#             discovery.get('states') or
#             discovery.get('location_terms')
#         ),
#         'has_keyword':       bool(discovery.get('keywords')),
#         'has_media':         bool(discovery.get('media')),
#     }


# def build_typesense_params(
#     profile: Dict,
#     ui_filters: Dict = None,
#     signals: Dict = None
# ) -> Dict:
#     """
#     Convert the v3 profile into Typesense search parameters.

#     Builds query_by, query_by_weights, filter_by, sort_by,
#     typo settings, and prefix settings from the profile and signals.

#     FIX — local mode pool scoping:
#         Adds document_data_type:=business to filter_by when
#         query_mode is local and no UI data_type filter overrides it.
#         This prevents restaurant queries competing against every
#         article in the index.
#     """
#     signals    = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
#     params     = {}

#     # ── Query string — deduplicate search_terms ───────────────────────────
#     seen         = set()
#     unique_terms = []
#     for term in profile.get('search_terms', []):
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)

#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'

#     # ── Field boosts — read from v3, add mode-specific fields ────────────
#     field_boosts = dict(profile.get('field_boosts', {}))

#     if query_mode == 'local':
#         field_boosts.setdefault('service_type',        12)
#         field_boosts.setdefault('service_specialties', 10)

#     sorted_fields              = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
#     params['query_by']         = ','.join(f[0] for f in sorted_fields)
#     params['query_by_weights'] = ','.join(str(f[1]) for f in sorted_fields)

#     # ── Typo / prefix / drop-token settings by mode ──────────────────────
#     has_corrections = bool(profile.get('corrections'))
#     term_count      = len(unique_terms)

#     if query_mode == 'answer':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'explore':
#         params['num_typos']             = 0 if has_corrections else 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
#     elif query_mode == 'browse':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
#     elif query_mode == 'local':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     elif query_mode == 'compare':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'shop':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     else:
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1

#     # ── Sort order ────────────────────────────────────────────────────────
#     temporal_direction = signals.get('temporal_direction')
#     price_direction    = signals.get('price_direction')
#     has_superlative    = signals.get('has_superlative', False)
#     has_rating         = signals.get('has_rating_signal', False)

#     if temporal_direction == 'oldest':
#         params['sort_by'] = 'time_period_start:asc,authority_score:desc'
#     elif temporal_direction == 'newest':
#         params['sort_by'] = 'published_date:desc,authority_score:desc'
#     elif price_direction == 'cheap':
#         params['sort_by'] = 'product_price:asc,authority_score:desc'
#     elif price_direction == 'expensive':
#         params['sort_by'] = 'product_price:desc,authority_score:desc'
#     elif query_mode == 'local':
#         params['sort_by'] = 'authority_score:desc'
#     elif query_mode == 'browse' and has_superlative:
#         params['sort_by'] = 'authority_score:desc'
#     elif has_rating:
#         params['sort_by'] = 'authority_score:desc'
#     else:
#         params['sort_by'] = '_text_match:desc,authority_score:desc'

#     # ── filter_by — locations + black_owned + local scope + UI filters ────
#     filter_conditions = []

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     # FIX — local mode: scope pool to business documents only
#     # Only applies when no UI data_type filter is already set
#     if query_mode == 'local' and not (ui_filters and ui_filters.get('data_type')):
#         filter_conditions.append('document_data_type:=business')

#     if ui_filters:
#         if ui_filters.get('data_type'):
#             filter_conditions.append(f"document_data_type:={ui_filters['data_type']}")
#         if ui_filters.get('category'):
#             filter_conditions.append(f"document_category:={ui_filters['category']}")
#         if ui_filters.get('schema'):
#             filter_conditions.append(f"document_schema:={ui_filters['schema']}")

#     if filter_conditions:
#         params['filter_by'] = ' && '.join(filter_conditions)

#     return params


# def build_filter_string_without_data_type(
#     profile: Dict,
#     signals: Dict = None
# ) -> str:
#     """
#     Build the location-only filter string used in Stage 1A.
#     No data_type included so facet counting stays accurate across all types.
#     black_owned is included because it is a hard filter, not a facet.
#     """
#     signals           = signals or {}
#     filter_conditions = []
#     query_mode        = signals.get('query_mode', 'explore')

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     return ' && '.join(filter_conditions) if filter_conditions else ''


# # ============================================================
# # END OF PART 3
# # ============================================================

# # ============================================================
# # PART 4 OF 8 — SCORING FUNCTIONS
# # ============================================================

# def _resolve_blend(
#     query_mode: str,
#     signals: Dict,
#     candidates: List[Dict]
# ) -> Dict:
#     """
#     Build the final blend ratios for this query at runtime.

#     Starts from BLEND_RATIOS[query_mode] then:
#     1. Samples up to 20 candidates to detect dead authority weight.
#        Business documents often have authority_score = 0.
#        If all sampled docs have zero authority, redistributes
#        that weight to semantic so it is not wasted.
#     2. Shifts text weight to semantic when unknown terms are present
#        because semantic handles unknown vocabulary better than keyword.
#     3. Shifts semantic weight to authority when a superlative is
#        present AND authority is live — "best" queries should reward
#        highly rated documents.
#     4. Overrides everything for single-answer mode.
#     """
#     blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

#     # Detect dead authority weight
#     sample             = candidates[:20]
#     has_live_authority = any(c.get('authority_score', 0) > 0 for c in sample)

#     if not has_live_authority and blend['authority'] > 0:
#         print(f"   ⚠️ Authority weight dead ({blend['authority']:.2f}) — redistributing to semantic")
#         blend['semantic'] += blend['authority']
#         blend['authority'] = 0.0

#     # Unknown terms shift — semantic handles unknown vocab better
#     if signals.get('has_unknown_terms', False):
#         shift               = min(0.15, blend['text_match'])
#         blend['text_match'] -= shift
#         blend['semantic']   += shift
#         print(f"   📊 Unknown term shift: text={blend['text_match']:.2f} sem={blend['semantic']:.2f}")

#     # Superlative shift — only when authority is live
#     if signals.get('has_superlative', False) and has_live_authority:
#         shift              = min(0.10, blend['semantic'])
#         blend['semantic']  -= shift
#         blend['authority'] += shift
#         print(f"   📊 Superlative shift: sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

#     # Single-answer override
#     if query_mode == 'answer' and signals.get('wants_single_result'):
#         blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

#     print(f"   📊 Final blend ({query_mode}): "
#           f"text={blend['text_match']:.2f} "
#           f"sem={blend['semantic']:.2f} "
#           f"auth={blend['authority']:.2f}")

#     return blend


# def _extract_authority_score(doc: Dict) -> float:
#     """
#     Return a normalized authority score [0.0 .. 1.0] appropriate
#     for this document's data type.

#     Branches on document_data_type OR data_type (metadata items
#     use 'data_type', full documents use 'document_data_type').

#     - business  : service_rating + service_review_count (log confidence)
#     - product   : product_rating + product_review_count (log confidence)
#     - recipe    : recipe_rating  + recipe_review_count  (log confidence)
#     - media     : media_rating   (direct normalize)
#     - article / person / place : authority_score field,
#                   falls back to factual_density_score + evergreen_score
#     - all others: 0.0

#     Log confidence formula:
#         confidence = log(1 + review_count) / log(1 + scale_anchor)
#         score      = (rating / 5.0) * confidence

#     Always returns a float — never None, never crashes the blend.
#     """
#     # Read both key names — metadata items use 'data_type',
#     # full Typesense documents use 'document_data_type'
#     data_type = (
#         doc.get('data_type') or
#         doc.get('document_data_type') or
#         ''
#     ).lower()

#     if data_type == 'business':
#         rating  = doc.get('service_rating') or 0.0
#         reviews = doc.get('service_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'product':
#         rating  = doc.get('product_rating') or 0.0
#         reviews = doc.get('product_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'recipe':
#         rating  = doc.get('recipe_rating') or 0.0
#         reviews = doc.get('recipe_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_RECIPE),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'media':
#         return min((doc.get('media_rating') or 0.0) / 5.0, 1.0)

#     # article, person, place — use authority_score field
#     raw = doc.get('authority_score') or 0.0
#     if raw > 0:
#         return min(raw / 100.0, 1.0)

#     # Fallback: article quality signals when authority_score is missing
#     depth     = doc.get('factual_density_score') or 0
#     evergreen = doc.get('evergreen_score') or 0
#     if depth > 0 or evergreen > 0:
#         return min((depth + evergreen) / 200.0, 0.5)

#     return 0.0


# def _compute_text_score(
#     keyword_rank: int,
#     pool_size: int,
#     item: Dict,
#     profile: Dict
# ) -> float:
#     """
#     Positional score from the item's rank in Stage 1A keyword results.

#     Base score is 1 - (rank / pool_size) so rank 0 scores 1.0
#     and the last document scores close to 0.0.

#     Adds a small bonus (up to 0.15) when the document's
#     primary_keywords overlap with WD-classified keywords from
#     the query profile. Each overlapping keyword adds 0.05.

#     Returns float [0.0 .. 1.0].
#     """
#     base = 1.0 - (keyword_rank / max(pool_size, 1))

#     # Keyword overlap bonus
#     doc_kws = set(k.lower() for k in (item.get('primary_keywords') or []))
#     wd_kws  = set(
#         k.get('phrase', '').lower()
#         for k in profile.get('keywords', [])
#         if k.get('phrase')
#     )
#     overlap = doc_kws & wd_kws
#     bonus   = min(len(overlap) * 0.05, 0.15)

#     return min(base + bonus, 1.0)


# def _compute_semantic_score(vector_distance: float) -> float:
#     """
#     Convert vector distance to a score with a hard gate at 0.65.

#     Any document with vector_distance >= SEMANTIC_DISTANCE_GATE
#     returns exactly 0.0 and cannot be rescued by blend weight.

#     Below the gate, score is linear relative to the gate:
#         score = 1.0 - (distance / gate)

#     Returns float [0.0 .. 1.0].
#     """
#     if vector_distance is None or vector_distance >= SEMANTIC_DISTANCE_GATE:
#         return 0.0
#     return 1.0 - (vector_distance / SEMANTIC_DISTANCE_GATE)


# def _domain_relevance(doc: Dict, signals: Dict) -> float:
#     """
#     Return a multiplier based on how well the document aligns
#     with the query's detected domain.

#     FIX — reads both key name variants:
#         metadata items  → 'category', 'schema', 'data_type'
#         full documents  → 'document_category', 'document_schema'

#     FIX — checks service_type when category does not match.
#     Many business documents have category='business' regardless
#     of cuisine/service. A restaurant with service_type=['restaurant']
#     should match the food domain even when category='business'.

#     Returns:
#         1.15  — domain match via category or service_type
#         1.10  — partial match via schema text
#         1.00  — neutral
#         0.75  — domain mismatch
#     """
#     primary_domain = signals.get('primary_domain')
#     if not primary_domain:
#         return 1.0

#     # Read both key name variants
#     doc_category  = (
#         doc.get('document_category') or
#         doc.get('category') or
#         ''
#     ).lower()

#     doc_schema = (
#         doc.get('document_schema') or
#         doc.get('schema') or
#         ''
#     ).lower()

#     # service_type is a list — normalize to lowercase strings
#     service_types = [
#         s.lower() for s in (doc.get('service_type') or [])
#         if s
#     ]

#     aligned_categories = DOMAIN_CATEGORY_MAP.get(primary_domain, set())

#     if not aligned_categories:
#         return 1.0

#     # Level 1 — direct category match
#     if doc_category in aligned_categories:
#         return 1.15

#     # Level 2 — service_type match
#     # Maps domain to the service_type values that belong to it.
#     # This handles business documents that have category='business'
#     # regardless of what kind of business they are.
#     DOMAIN_SERVICE_MAP = {
#         'food':        {
#             'restaurant', 'cafe', 'bakery', 'catering', 'food',
#             'dining', 'eatery', 'diner', 'buffet', 'bar', 'brewery',
#             'winery', 'food truck', 'coffee',
#         },
#         'beauty':      {
#             'salon', 'barbershop', 'spa', 'nail salon', 'hair salon',
#             'nail tech', 'esthetician', 'lash studio', 'brow bar',
#         },
#         'health':      {
#             'clinic', 'doctor', 'dentist', 'gym', 'fitness',
#             'pharmacy', 'urgent care', 'therapist', 'chiropractor',
#             'optometrist', 'mental health',
#         },
#         'education':   {
#             'school', 'tutoring', 'daycare', 'academy',
#             'preschool', 'childcare', 'learning center',
#         },
#         'real_estate': {
#             'realty', 'realtor', 'property management',
#             'real estate', 'mortgage', 'home inspection',
#         },
#         'technology':  {
#             'software', 'it services', 'tech support',
#             'web design', 'app development',
#         },
#         'business':    {
#             'consulting', 'accounting', 'legal', 'staffing',
#             'financial', 'insurance', 'marketing', 'advertising',
#         },
#         'culture':     {
#             'museum', 'gallery', 'cultural center', 'community center',
#             'church', 'nonprofit',
#         },
#         'music':       {
#             'studio', 'recording studio', 'music venue', 'club',
#             'lounge', 'concert venue',
#         },
#         'fashion':     {
#             'boutique', 'clothing store', 'tailor', 'alterations',
#             'fashion', 'apparel',
#         },
#         'sports':      {
#             'gym', 'fitness center', 'sports facility', 'yoga',
#             'martial arts', 'dance studio',
#         },
#     }

#     aligned_services = DOMAIN_SERVICE_MAP.get(primary_domain, set())
#     if aligned_services and any(s in aligned_services for s in service_types):
#         return 1.15

#     # Level 3 — partial match via schema or category text
#     if primary_domain in doc_schema or primary_domain in doc_category:
#         return 1.10

#     # No match — penalize
#     return 0.75


# def _content_intent_match(doc: Dict, query_mode: str) -> float:
#     """
#     Return a multiplier based on whether the document's content_intent
#     matches what the query mode expects.

#     local/shop     → want transactional or navigational
#     answer/explore → want informational or educational
#     browse         → want informational, commercial, or transactional
#     compare        → want informational or commercial

#     Returns:
#         1.10  — intent match
#         1.00  — neutral (no content_intent on doc)
#         0.85  — intent mismatch
#     """
#     doc_intent = (doc.get('content_intent') or '').lower()
#     if not doc_intent:
#         return 1.0

#     preferred = INTENT_CONTENT_MAP.get(query_mode, set())
#     if not preferred:
#         return 1.0

#     return 1.10 if doc_intent in preferred else 0.85


# def _pool_type_multiplier(doc: Dict, query_mode: str) -> float:
#     """
#     Return a multiplier based on whether the document's data type
#     is appropriate for this query mode.

#     FIX — reads both key name variants:
#         metadata items use 'data_type'
#         full Typesense documents use 'document_data_type'

#     Uses POOL_SCOPE to check allowed types per mode.
#     Documents outside the allowed set are penalized but never
#     hard-removed — edge cases exist where a wrong type is the
#     best answer.

#     Returns:
#         1.0  — correct type for this mode
#         0.5  — wrong type for this mode (soft penalty)
#     """
#     # FIX — read both key names
#     data_type = (
#         doc.get('data_type') or
#         doc.get('document_data_type') or
#         ''
#     ).lower()

#     scope         = POOL_SCOPE.get(query_mode, {})
#     allowed_types = scope.get('allow', set())

#     if not allowed_types:
#         return 1.0

#     return 1.0 if data_type in allowed_types else 0.5


# def _score_document(
#     idx: int,
#     item: Dict,
#     profile: Dict,
#     signals: Dict,
#     blend: Dict,
#     pool_size: int,
#     vector_data: Dict
# ) -> float:
#     """
#     Compute the final blended score for one document.

#     Orchestrates all scoring functions in sequence:
#     1. text_score           — positional + keyword overlap
#     2. semantic_score       — vector distance with hard gate
#     3. authority_score      — schema-aware rating/review signal
#     4. weighted blend       — text + semantic + authority
#     5. _domain_relevance    — multiplier
#     6. _content_intent_match — multiplier
#     7. _pool_type_multiplier — multiplier
#     8. post-blend adjustments:
#          - preferred data type boost
#          - black_owned boost
#          - superlative cap for zero-authority docs

#     Attaches intermediate values to the item dict for debugging.
#     Returns the final score float.
#     """
#     query_mode = signals.get('query_mode', 'explore')
#     item_id    = item.get('id', '')

#     # Pull vector data for this document
#     vd              = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
#     vector_distance = vd.get('vector_distance', 1.0)
#     semantic_rank   = vd.get('semantic_rank', 999999)

#     item['vector_distance'] = vector_distance
#     item['semantic_rank']   = semantic_rank

#     # Component scores
#     text_score = _compute_text_score(idx, pool_size, item, profile)
#     sem_score  = _compute_semantic_score(vector_distance)
#     auth_score = _extract_authority_score(item)

#     # Weighted blend
#     blended = (
#         blend['text_match'] * text_score +
#         blend['semantic']   * sem_score  +
#         blend['authority']  * auth_score
#     )

#     # Multipliers
#     blended *= _domain_relevance(item, signals)
#     blended *= _content_intent_match(item, query_mode)
#     blended *= _pool_type_multiplier(item, query_mode)

#     # Post-blend adjustments

#     # Preferred data type boost
#     if item.get('data_type') in profile.get('preferred_data_types', []):
#         blended = min(blended + PREFERRED_TYPE_BOOST, 1.0)

#     # Black-owned boost — only when signal fired and flag is set on doc
#     if signals.get('has_black_owned') and item.get('black_owned') is True:
#         blended = min(blended + BLACK_OWNED_BOOST, 1.0)

#     # Superlative cap — prevents zero-authority docs ranking top
#     # when the query demands the best (e.g. "best restaurants")
#     if signals.get('has_superlative') and auth_score == 0.0:
#         blended = min(blended, SUPERLATIVE_SCORE_CAP)

#     # Attach to item for debugging and cache
#     item['blended_score'] = blended
#     item['text_score']    = round(text_score, 4)
#     item['sem_score']     = round(sem_score, 4)
#     item['auth_score']    = round(auth_score, 4)

#     return blended


# # ============================================================
# # END OF PART 4
# # ============================================================

# # ============================================================
# # PART 5 OF 8 — CANDIDATE FETCHING (STAGES 1A, 1B, COMBINED)
# # ============================================================

# # ── Stopwords for question hit validation ─────────────────────────────────────

# _MATCH_STOPWORDS = frozenset({
#     'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
#     'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
#     'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
#     'would', 'could', 'should', 'may', 'might', 'shall', 'can', 'need',
#     'when', 'where', 'who', 'what', 'which', 'why', 'how', 'that', 'this',
#     'these', 'those', 'it', 'its', 'he', 'she', 'they', 'we', 'you', 'i',
#     'his', 'her', 'their', 'our', 'your', 'my', 'about', 'into', 'through',
#     'during', 'before', 'after', 'above', 'below', 'between', 'each',
#     'than', 'so', 'if', 'not', 'no', 'nor', 'yet', 'both', 'either',
#     'just', 'also', 'then', 'than', 'such', 'more', 'most', 'other',
#     'born', 'died', 'first', 'last', 'new', 'old', 'many', 'much',
#     'long', 'little', 'own', 'right', 'big', 'high', 'great', 'small',
# })


# def _normalize_signal(text: str) -> set:
#     """
#     Normalize a signal string into a set of meaningful tokens.
#     Strips punctuation, lowercases, removes stopwords and short tokens.
#     Used by question hit validation.
#     """
#     if not text:
#         return set()
#     text = text.lower()
#     text = re.sub(r"[^\w\s-]", " ", text)
#     text = re.sub(r"\s*-\s*", " ", text)
#     return {t for t in text.split() if len(t) > 2 and t not in _MATCH_STOPWORDS}


# def _extract_query_signals(
#     profile: Dict,
#     discovery: Dict = None
# ) -> Tuple[set, list, Optional[set]]:
#     """
#     Extract and normalize all meaningful query signals from the v3 profile.
#     Reads persons, organizations, keywords, and search_terms directly.
#     Also includes spelling suggestions from corrections.

#     Returns:
#         all_tokens      — set of all normalized tokens across all signals
#         full_phrases    — list of normalized phrase strings
#         primary_subject — normalized tokens of the highest-ranked entity
#     """
#     raw_signals    = []
#     ranked_signals = []

#     for p in profile.get('persons', []):
#         phrase = p.get('phrase') or p.get('word', '')
#         rank   = p.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for o in profile.get('organizations', []):
#         phrase = o.get('phrase') or o.get('word', '')
#         rank   = o.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for k in profile.get('keywords', []):
#         phrase = k.get('phrase') or k.get('word', '')
#         rank   = k.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for term in profile.get('search_terms', []):
#         if term:
#             raw_signals.append(term)

#     # Include spelling suggestions from corrections
#     if discovery:
#         for corr in discovery.get('corrections', []):
#             if corr.get('correction_type') == 'suggestion' and corr.get('corrected'):
#                 corrected = corr['corrected']
#                 if corrected not in raw_signals:
#                     raw_signals.append(corrected)
#                     ranked_signals.append((100, corrected))

#         for term in discovery.get('terms', []):
#             if term.get('status') == 'unknown_suggest' and term.get('suggestion'):
#                 suggestion = term['suggestion']
#                 if suggestion not in raw_signals:
#                     raw_signals.append(suggestion)
#                     ranked_signals.append((100, suggestion))

#     all_tokens   = set()
#     full_phrases = []

#     for phrase in raw_signals:
#         all_tokens.update(_normalize_signal(phrase))
#         phrase_lower = phrase.lower().strip()
#         if phrase_lower:
#             full_phrases.append(phrase_lower)

#     primary_subject = None
#     if ranked_signals:
#         ranked_signals.sort(key=lambda x: -x[0])
#         primary_subject = _normalize_signal(ranked_signals[0][1])

#     return all_tokens, full_phrases, primary_subject


# def _validate_question_hit(
#     hit_doc: Dict,
#     query_tokens: set,
#     query_phrases: list,
#     primary_subject: Optional[set],
#     min_matches: int = 1,
# ) -> bool:
#     """
#     Validate a question hit against query signals using 4-level matching.

#     Level 1 — Exact token match
#     Level 2 — Partial token match (substring of token)
#     Level 3 — Substring containment (phrase in candidate text)
#     Level 4 — Token overlap (remaining tokens after exact match)

#     Returns True if the hit passes validation.
#     If primary_subject is set and query has 3+ tokens, the primary
#     subject must appear in at least one match for the hit to pass.
#     """
#     if not query_tokens:
#         return True

#     candidate_raw = (
#         hit_doc.get('primary_keywords', []) +
#         hit_doc.get('entities', []) +
#         hit_doc.get('semantic_keywords', [])
#     )

#     if not candidate_raw:
#         return False

#     candidate_tokens  = set()
#     candidate_phrases = []

#     for val in candidate_raw:
#         if not val:
#             continue
#         candidate_tokens.update(_normalize_signal(val))
#         candidate_phrases.append(val.lower().strip())

#     candidate_text = ' '.join(candidate_phrases)

#     match_count         = 0
#     primary_subject_hit = False

#     # Level 1 — exact token match
#     exact_matches = query_tokens & candidate_tokens
#     if exact_matches:
#         match_count += len(exact_matches)
#         if primary_subject and (primary_subject & exact_matches):
#             primary_subject_hit = True

#     # Level 2 — partial token match
#     for qt in query_tokens:
#         if qt in exact_matches:
#             continue
#         for ct in candidate_tokens:
#             if qt in ct or ct in qt:
#                 match_count += 1
#                 if primary_subject and qt in primary_subject:
#                     primary_subject_hit = True
#                 break

#     # Level 3 — substring containment
#     for qp in query_phrases:
#         if len(qp) < 3:
#             continue
#         if qp in candidate_text:
#             match_count += 1
#             if primary_subject and _normalize_signal(qp) & primary_subject:
#                 primary_subject_hit = True
#         else:
#             for cp in candidate_phrases:
#                 if qp in cp or cp in qp:
#                     match_count += 1
#                     if primary_subject and _normalize_signal(qp) & primary_subject:
#                         primary_subject_hit = True
#                     break

#     # Level 4 — token overlap
#     remaining_query = query_tokens - exact_matches
#     token_overlap   = remaining_query & candidate_tokens
#     if token_overlap:
#         match_count += len(token_overlap)
#         if primary_subject and (primary_subject & token_overlap):
#             primary_subject_hit = True

#     if match_count < min_matches:
#         return False

#     if primary_subject and len(query_tokens) >= 3:
#         if not primary_subject_hit:
#             return False

#     return True


# def fetch_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = 100
# ) -> List[str]:
#     """
#     Stage 1A — keyword search against the document collection.
#     Returns up to 100 document_uuid strings with no metadata.

#     The local mode document_data_type filter is applied inside
#     build_typesense_params so this function does not need to
#     handle it separately.
#     """
#     signals    = signals or {}
#     params     = build_typesense_params(profile, signals=signals)
#     filter_str = build_filter_string_without_data_type(profile, signals=signals)
#     query_mode = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1A (document): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode} | Max: {max_results}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     search_params = {
#         'q':                     params.get('q', search_query),
#         'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#         'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
#         'per_page':              max_results,
#         'page':                  1,
#         'include_fields':        'document_uuid',
#         'num_typos':             params.get('num_typos', 0),
#         'prefix':                params.get('prefix', 'no'),
#         'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#         'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
#     }

#     if filter_str:
#         search_params['filter_by'] = filter_str

#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         hits     = response.get('hits', [])
#         uuids    = [
#             hit['document']['document_uuid']
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         ]
#         print(f"📊 Stage 1A: {len(uuids)} candidate UUIDs")
#         return uuids
#     except Exception as e:
#         print(f"❌ Stage 1A error: {e}")
#         return []


# def fetch_candidate_uuids_from_questions(
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     max_results: int = 50,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Stage 1B — vector search against the questions collection.

#     Step A — build facet filter from v3 profile metadata
#     Step B — run vector search within filtered subset
#     Step C — apply hard distance gate at 0.65 (rejects distant hits
#               before validation — fixes the 0.63–0.94 distance problem)
#     Step D — validate remaining hits with 4-level token matching

#     Location filter is AND'd onto the facet filter so question hits
#     are constrained to the detected geographic area.
#     """
#     signals = signals or {}

#     if not query_embedding:
#         print("⚠️ Stage 1B (questions): no embedding — skipping")
#         return []

#     query_tokens, query_phrases, primary_subject = _extract_query_signals(
#         profile, discovery=discovery
#     )

#     print(f"🔍 Stage 1B validation signals:")
#     print(f"   query_tokens    : {sorted(query_tokens)}")
#     print(f"   query_phrases   : {query_phrases}")
#     print(f"   primary_subject : {primary_subject}")

#     # ── Step A: Build facet filter ────────────────────────────────────────
#     filter_parts = []

#     primary_kws = [
#         k.get('phrase') or k.get('word', '')
#         for k in profile.get('keywords', [])
#     ]
#     primary_kws = [kw for kw in primary_kws if kw][:3]
#     if primary_kws:
#         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
#         filter_parts.append(f'primary_keywords:[{kw_values}]')

#     entity_names = []
#     for p in profile.get('persons', []):
#         name = p.get('phrase') or p.get('word', '')
#         rank = p.get('rank', 0)
#         if name and (' ' in name or rank > 100):
#             entity_names.append(name)
#     for o in profile.get('organizations', []):
#         name = o.get('phrase') or o.get('word', '')
#         rank = o.get('rank', 0)
#         if name and (' ' in name or rank > 100):
#             entity_names.append(name)
#     entity_names = [e for e in entity_names if e][:3]
#     if entity_names:
#         ent_values = ','.join([f'`{e}`' for e in entity_names])
#         filter_parts.append(f'entities:[{ent_values}]')

#     question_word     = signals.get('question_word') or ''
#     question_type_map = {
#         'when':  'TEMPORAL',
#         'where': 'LOCATION',
#         'who':   'PERSON',
#         'what':  'FACTUAL',
#         'which': 'FACTUAL',
#         'why':   'REASON',
#         'how':   'PROCESS',
#     }
#     question_type = question_type_map.get(question_word.lower(), '')
#     if question_type:
#         filter_parts.append(f'question_type:={question_type}')

#     # ── Location filter ───────────────────────────────────────────────────
#     location_filter_parts = []
#     query_mode            = signals.get('query_mode', 'explore')
#     is_location_subject   = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:=`{c['name']}`" for c in cities]
#             location_filter_parts.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:=`{variant}`"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             location_filter_parts.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     facet_filter    = ' || '.join(filter_parts) if filter_parts else ''
#     location_filter = ' && '.join(location_filter_parts) if location_filter_parts else ''

#     if facet_filter and location_filter:
#         filter_str = f'({facet_filter}) && {location_filter}'
#     elif location_filter:
#         filter_str = location_filter
#     else:
#         filter_str = facet_filter

#     print(f"   filter_by : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

#     # ── Step B: Vector search ─────────────────────────────────────────────
#     embedding_str = ','.join(str(x) for x in query_embedding)

#     search_params = {
#         'q':              '*',
#         'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#         'per_page':       max_results * 2,
#         'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#     }

#     if filter_str:
#         search_params['filter_by'] = filter_str

#     try:
#         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
#         response        = client.multi_search.perform(search_requests, {})
#         result          = response['results'][0]
#         hits            = result.get('hits', [])

#         # Fallback if too few hits with location filter
#         if len(hits) < 5 and filter_str:
#             fallback_filter = facet_filter if facet_filter else ''
#             print(f"⚠️ Stage 1B: only {len(hits)} hits with location filter — "
#                   f"retrying with facet filter only")

#             sp_fallback = {**search_params}
#             if fallback_filter:
#                 sp_fallback['filter_by'] = fallback_filter
#             else:
#                 sp_fallback.pop('filter_by', None)

#             r_fallback    = client.multi_search.perform(
#                 {'searches': [{'collection': 'questions', **sp_fallback}]}, {}
#             )
#             fallback_hits = r_fallback['results'][0].get('hits', [])
#             print(f"   Fallback returned {len(fallback_hits)} hits")

#             if len(fallback_hits) < 5:
#                 print(f"⚠️ Stage 1B: retrying with no filter")
#                 sp_nofilter = {k: v for k, v in search_params.items() if k != 'filter_by'}
#                 r_nofilter  = client.multi_search.perform(
#                     {'searches': [{'collection': 'questions', **sp_nofilter}]}, {}
#                 )
#                 hits = r_nofilter['results'][0].get('hits', [])
#                 print(f"   No-filter fallback returned {len(hits)} hits")
#             else:
#                 hits = fallback_hits

#         # ── Step C: Hard distance gate (FIX) ─────────────────────────────
#         # Any hit above SEMANTIC_DISTANCE_GATE is discarded immediately.
#         # This fixes the 0.63–0.94 distance problem in the questions pool.
#         uuids    = []
#         seen     = set()
#         accepted = 0
#         rejected = 0

#         for hit in hits:
#             doc           = hit.get('document', {})
#             uuid          = doc.get('document_uuid')
#             hit_distance  = hit.get('vector_distance', 1.0)

#             if not uuid:
#                 continue

#             # Hard gate — discard before validation
#             if hit_distance >= QUESTION_SEMANTIC_DISTANCE_GATE:
#                 rejected += 1
#                 print(f"   🚫 Distance gate: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit_distance:.4f} >= {QUESTION_SEMANTIC_DISTANCE_GATE})")
#                 continue

#             # ── Step D: Token validation ──────────────────────────────────
#             is_valid = _validate_question_hit(
#                 hit_doc         = doc,
#                 query_tokens    = query_tokens,
#                 query_phrases   = query_phrases,
#                 primary_subject = primary_subject,
#                 min_matches     = 1,
#             )

#             if is_valid:
#                 accepted += 1
#                 if uuid not in seen:
#                     seen.add(uuid)
#                     uuids.append(uuid)
#             else:
#                 rejected += 1
#                 print(f"   ❌ Validation failed: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit_distance:.4f})")

#             if len(uuids) >= max_results:
#                 break

#         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
#               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1B error: {e}")
#         return []


# def fetch_all_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Run Stage 1A (document) and Stage 1B (questions) in parallel.

#     Merge order — highest confidence first:
#     1. Overlap — found by both paths
#     2. Document-only hits
#     3. Question-only hits
#     """
#     signals = signals or {}

#     doc_future = _executor.submit(
#         fetch_candidate_uuids, search_query, profile, signals, 100
#     )
#     q_future = _executor.submit(
#         fetch_candidate_uuids_from_questions,
#         profile, query_embedding, signals, 50, discovery
#     )

#     doc_uuids = doc_future.result()
#     q_uuids   = q_future.result()

#     doc_set = set(doc_uuids)
#     q_set   = set(q_uuids)
#     overlap  = doc_set & q_set

#     merged = []
#     seen   = set()

#     # Overlap first
#     for uuid in doc_uuids:
#         if uuid in overlap and uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     # Document-only
#     for uuid in doc_uuids:
#         if uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     # Question-only
#     for uuid in q_uuids:
#         if uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     print(f"📊 Stage 1 COMBINED:")
#     print(f"   document pool  : {len(doc_uuids)}")
#     print(f"   questions pool : {len(q_uuids)}")
#     print(f"   overlap        : {len(overlap)}")
#     print(f"   merged total   : {len(merged)}")

#     return merged


# # ============================================================
# # END OF PART 5
# # ============================================================


# # ============================================================
# # PART 6 OF 8 — METADATA FETCHING, RERANKING, COUNTING,
# #               FILTERING, PAGINATION
# # ============================================================

# def semantic_rerank_candidates(
#     candidate_ids: List[str],
#     query_embedding: List[float],
#     max_to_rerank: int = 250
# ) -> List[Dict]:
#     """
#     Stage 2 — pure vector ranking of the candidate pool.

#     Takes the UUID list from Stage 1 and re-orders it by
#     vector similarity to the query embedding.

#     Returns a list of dicts with:
#         id              — document_uuid
#         vector_distance — raw distance from Typesense
#         semantic_rank   — position in the reranked list (0-indexed)

#     Documents not returned by Typesense (rare) are appended at
#     the end with distance=1.0 and rank=len(reranked).
#     """
#     if not candidate_ids or not query_embedding:
#         return [
#             {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#             for i, cid in enumerate(candidate_ids)
#         ]

#     ids_to_rerank = candidate_ids[:max_to_rerank]
#     id_filter     = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
#     embedding_str = ','.join(str(x) for x in query_embedding)

#     params = {
#         'q':              '*',
#         'vector_query':   f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
#         'filter_by':      f'document_uuid:[{id_filter}]',
#         'per_page':       len(ids_to_rerank),
#         'include_fields': 'document_uuid',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = client.multi_search.perform(search_requests, {})
#         hits            = response['results'][0].get('hits', [])

#         reranked = [
#             {
#                 'id':              hit['document'].get('document_uuid'),
#                 'vector_distance': hit.get('vector_distance', 1.0),
#                 'semantic_rank':   i,
#             }
#             for i, hit in enumerate(hits)
#         ]

#         # Append any ids that Typesense did not return
#         reranked_ids = {r['id'] for r in reranked}
#         for cid in ids_to_rerank:
#             if cid not in reranked_ids:
#                 reranked.append({
#                     'id':              cid,
#                     'vector_distance': 1.0,
#                     'semantic_rank':   len(reranked),
#                 })

#         print(f"🎯 Stage 2: reranked {len(reranked)} candidates")
#         return reranked

#     except Exception as e:
#         print(f"⚠️ Stage 2 error: {e}")
#         return [
#             {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#             for i, cid in enumerate(ids_to_rerank)
#         ]


# def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
#     """
#     Stage 4 — fetch lightweight metadata for documents that survived
#     vector pruning. Preserves the semantic rank order of survivor_ids.

#     Fetches all fields needed by the scoring functions in Part 4:
#         document_data_type, document_category, document_schema
#         authority_score, content_intent
#         service_rating, service_review_count
#         product_rating, product_review_count
#         recipe_rating, recipe_review_count
#         media_rating
#         factual_density_score, evergreen_score
#         primary_keywords, black_owned
#         document_title, service_type
#         image_url, logo_url

#     Batches in groups of 250 to stay within Typesense limits.
#     """
#     if not survivor_ids:
#         return []

#     BATCH_SIZE = 100
#     doc_map    = {}

#     for i in range(0, len(survivor_ids), BATCH_SIZE):
#         batch_ids = survivor_ids[i:i + BATCH_SIZE]
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])

#         params = {
#             'q':         '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page':  len(batch_ids),
#             'include_fields': ','.join([
#                 'document_uuid',
#                 'document_data_type',
#                 'document_category',
#                 'document_schema',
#                 'document_title',
#                 'content_intent',
#                 'authority_score',
#                 'service_rating',
#                 'service_review_count',
#                 'service_type',
#                 'product_rating',
#                 'product_review_count',
#                 'recipe_rating',
#                 'recipe_review_count',
#                 'media_rating',
#                 'factual_density_score',
#                 'evergreen_score',
#                 'primary_keywords',
#                 'black_owned',
#                 'image_url',
#                 'logo_url',
#             ]),
#         }

#         try:
#             search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#             response        = client.multi_search.perform(search_requests, {})
#             hits            = response['results'][0].get('hits', [])

#             for hit in hits:
#                 doc  = hit.get('document', {})
#                 uuid = doc.get('document_uuid')
#                 if uuid:
#                     doc_map[uuid] = {
#                         'id':                   uuid,
#                         'data_type':            doc.get('document_data_type', ''),
#                         'category':             doc.get('document_category', ''),
#                         'schema':               doc.get('document_schema', ''),
#                         'title':                doc.get('document_title', ''),
#                         'content_intent':       doc.get('content_intent', ''),
#                         'authority_score':      doc.get('authority_score', 0),
#                         'service_rating':       doc.get('service_rating', 0),
#                         'service_review_count': doc.get('service_review_count', 0),
#                         'service_type':         doc.get('service_type', []),
#                         'product_rating':       doc.get('product_rating', 0),
#                         'product_review_count': doc.get('product_review_count', 0),
#                         'recipe_rating':        doc.get('recipe_rating', 0),
#                         'recipe_review_count':  doc.get('recipe_review_count', 0),
#                         'media_rating':         doc.get('media_rating', 0),
#                         'factual_density_score': doc.get('factual_density_score', 0),
#                         'evergreen_score':      doc.get('evergreen_score', 0),
#                         'primary_keywords':     doc.get('primary_keywords', []),
#                         'black_owned':          doc.get('black_owned', False),
#                         'image_url':            doc.get('image_url', []),
#                         'logo_url':             doc.get('logo_url', []),
#                     }

#         except Exception as e:
#             print(f"❌ Stage 4 metadata fetch error (batch {i}): {e}")

#     results = [doc_map[uuid] for uuid in survivor_ids if uuid in doc_map]
#     print(f"📊 Stage 4: fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
#     return results


# def fetch_candidates_with_metadata(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[Dict]:
#     """
#     Keyword path only — fetch UUIDs and lightweight metadata together
#     in one Typesense call per page.

#     Used when alt_mode='n' or search_source is dropdown/keyword/
#     suggestion/autocomplete. Since the keyword path has no vector
#     pruning, all candidates survive so a separate metadata fetch
#     would be a wasted round trip.

#     FIX — uses the full filter from build_typesense_params so the
#     keyword path applies the same data type scoping as the semantic
#     path. Previously used build_filter_string_without_data_type which
#     stripped the document_data_type:=business filter for local mode,
#     causing Houston restaurants to return zero results on keyword path
#     even though the documents existed in the index.
#     """
#     signals    = signals or {}
#     params     = build_typesense_params(profile, signals=signals)

#     # FIX — use the full filter from build_typesense_params which
#     # includes document_data_type:=business for local mode.
#     # Fall back to location-only filter if full params has no filter.
#     filter_str = params.get(
#         'filter_by',
#         build_filter_string_without_data_type(profile, signals=signals)
#     )

#     PAGE_SIZE    = 100
#     all_results  = []
#     current_page = 1
#     max_pages    = (max_results // PAGE_SIZE) + 1
#     query_mode   = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1 (keyword+metadata): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     while len(all_results) < max_results and current_page <= max_pages:
#         search_params = {
#             'q':                     params.get('q', search_query),
#             'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#             'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
#             'per_page':              PAGE_SIZE,
#             'page':                  current_page,
#             'include_fields':        ','.join([
#                 'document_uuid',
#                 'document_data_type',
#                 'document_category',
#                 'document_schema',
#                 'document_title',
#                 'content_intent',
#                 'authority_score',
#                 'service_rating',
#                 'service_review_count',
#                 'service_type',
#                 'product_rating',
#                 'product_review_count',
#                 'recipe_rating',
#                 'recipe_review_count',
#                 'media_rating',
#                 'factual_density_score',
#                 'evergreen_score',
#                 'primary_keywords',
#                 'black_owned',
#                 'image_url',
#                 'logo_url',
#             ]),
#             'num_typos':             params.get('num_typos', 0),
#             'prefix':                params.get('prefix', 'no'),
#             'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#             'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
#         }

#         if filter_str:
#             search_params['filter_by'] = filter_str

#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(search_params)
#             hits     = response.get('hits', [])
#             found    = response.get('found', 0)

#             if not hits:
#                 break

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 all_results.append({
#                     'id':                   doc.get('document_uuid'),
#                     'data_type':            doc.get('document_data_type', ''),
#                     'category':             doc.get('document_category', ''),
#                     'schema':               doc.get('document_schema', ''),
#                     'title':                doc.get('document_title', ''),
#                     'content_intent':       doc.get('content_intent', ''),
#                     'authority_score':      doc.get('authority_score', 0),
#                     'service_rating':       doc.get('service_rating', 0),
#                     'service_review_count': doc.get('service_review_count', 0),
#                     'service_type':         doc.get('service_type', []),
#                     'product_rating':       doc.get('product_rating', 0),
#                     'product_review_count': doc.get('product_review_count', 0),
#                     'recipe_rating':        doc.get('recipe_rating', 0),
#                     'recipe_review_count':  doc.get('recipe_review_count', 0),
#                     'media_rating':         doc.get('media_rating', 0),
#                     'factual_density_score': doc.get('factual_density_score', 0),
#                     'evergreen_score':      doc.get('evergreen_score', 0),
#                     'primary_keywords':     doc.get('primary_keywords', []),
#                     'black_owned':          doc.get('black_owned', False),
#                     'image_url':            doc.get('image_url', []),
#                     'logo_url':             doc.get('logo_url', []),
#                     'text_match':           hit.get('text_match', 0),
#                 })

#             if len(all_results) >= found or len(hits) < PAGE_SIZE:
#                 break

#             current_page += 1

#         except Exception as e:
#             print(f"❌ Keyword fetch error (page {current_page}): {e}")
#             break

#     print(f"📊 Keyword path: {len(all_results)} candidates with metadata")
#     return all_results[:max_results]


# def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
#     """
#     Single pass through the full result set counting by
#     document_data_type, document_category, and document_schema.
#     Returns facets dict with value, count, and label per entry.
#     """
#     data_type_counts = {}
#     category_counts  = {}
#     schema_counts    = {}

#     for item in cached_results:
#         dt  = item.get('data_type', '')
#         cat = item.get('category', '')
#         sch = item.get('schema', '')
#         if dt:
#             data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
#         if cat:
#             category_counts[cat] = category_counts.get(cat, 0) + 1
#         if sch:
#             schema_counts[sch]   = schema_counts.get(sch, 0) + 1

#     return {
#         'data_type': [
#             {
#                 'value': dt,
#                 'count': c,
#                 'label': DATA_TYPE_LABELS.get(dt, dt.title()),
#             }
#             for dt, c in sorted(data_type_counts.items(), key=lambda x: -x[1])
#         ],
#         'category': [
#             {
#                 'value': cat,
#                 'count': c,
#                 'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title()),
#             }
#             for cat, c in sorted(category_counts.items(), key=lambda x: -x[1])
#         ],
#         'schema': [
#             {
#                 'value': sch,
#                 'count': c,
#                 'label': sch,
#             }
#             for sch, c in sorted(schema_counts.items(), key=lambda x: -x[1])
#         ],
#     }


# def count_all(candidates: List[Dict]) -> Dict:
#     """
#     Stage 5 — single counting pass after all pruning and scoring is done.
#     Single source of truth for facets, image count, and total.
#     Called once, never again for the same cached result set.
#     """
#     facets      = count_facets_from_cache(candidates)
#     image_count = _count_images_from_candidates(candidates)
#     total       = len(candidates)

#     print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
#           f"data_types={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

#     return {
#         'facets':            facets,
#         'facet_total':       total,
#         'total_image_count': image_count,
#     }


# def filter_cached_results(
#     cached_results: List[Dict],
#     data_type: str = None,
#     category: str  = None,
#     schema: str    = None
# ) -> List[Dict]:
#     """
#     Filter the cached result set by UI-selected filters.
#     All filters are optional — passing None skips that filter.
#     """
#     filtered = cached_results
#     if data_type:
#         filtered = [r for r in filtered if r.get('data_type') == data_type]
#     if category:
#         filtered = [r for r in filtered if r.get('category') == category]
#     if schema:
#         filtered = [r for r in filtered if r.get('schema') == schema]
#     return filtered


# def paginate_cached_results(
#     cached_results: List[Dict],
#     page: int,
#     per_page: int
# ) -> Tuple[List[Dict], int]:
#     """
#     Slice the filtered result set to the requested page.
#     Returns (page_items, total_count).
#     Returns empty list if page is beyond the total.
#     """
#     total = len(cached_results)
#     start = (page - 1) * per_page
#     end   = start + per_page
#     if start >= total:
#         return [], total
#     return cached_results[start:end], total


# # ============================================================
# # END OF PART 6
# # ============================================================
# # ============================================================
# # PART 7 OF 8 — DOCUMENT FETCHING, FORMATTING, AI OVERVIEW
# # ============================================================

# def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
#     """
#     Fetch complete document records from Typesense for the current
#     page only. Excludes the embedding field to keep response size small.
#     Preserves the order of the input document_ids list.
#     """
#     if not document_ids:
#         return []

#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])

#     params = {
#         'q':              '*',
#         'filter_by':      f'document_uuid:[{id_filter}]',
#         'per_page':       len(document_ids),
#         'exclude_fields': 'embedding',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = client.multi_search.perform(search_requests, {})
#         hits            = response['results'][0].get('hits', [])

#         doc_map = {
#             hit['document']['document_uuid']: format_result(hit, query)
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         }

#         return [doc_map[doc_id] for doc_id in document_ids if doc_id in doc_map]

#     except Exception as e:
#         print(f"❌ fetch_full_documents error: {e}")
#         return []


# def fetch_documents_by_semantic_uuid(
#     semantic_uuid: str,
#     exclude_uuid: str = None,
#     limit: int = 5
# ) -> List[Dict]:
#     """
#     Fetch documents that share the same semantic group.
#     Used for related searches on the question direct path.
#     Returns lightweight dicts with title, url, and id only.
#     """
#     if not semantic_uuid:
#         return []

#     filter_str = f'semantic_uuid:={semantic_uuid}'
#     if exclude_uuid:
#         filter_str += f' && document_uuid:!={exclude_uuid}'

#     params = {
#         'q':              '*',
#         'filter_by':      filter_str,
#         'per_page':       limit,
#         'include_fields': 'document_uuid,document_title,document_url',
#         'sort_by':        'authority_score:desc',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = client.multi_search.perform(search_requests, {})
#         hits            = response['results'][0].get('hits', [])

#         related = [
#             {
#                 'title': hit['document'].get('document_title', ''),
#                 'url':   hit['document'].get('document_url', ''),
#                 'id':    hit['document'].get('document_uuid', ''),
#             }
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         ]

#         print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
#         return related

#     except Exception as e:
#         print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
#         return []


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """
#     Transform a raw Typesense hit into the response format
#     that views.py expects.

#     Handles:
#         - highlight extraction from hit.highlights
#         - vector_distance to semantic_score conversion
#         - date string formatting to human-readable form
#         - geopoint field normalization
#         - location block construction
#         - time_period block construction
#     """
#     doc        = hit.get('document', {})
#     highlights = hit.get('highlights', [])

#     highlight_map = {
#         h.get('field'): (
#             h.get('value') or
#             h.get('snippet') or
#             (h.get('snippets') or [''])[0]
#         )
#         for h in highlights
#     }

#     vector_distance = hit.get('vector_distance')
#     semantic_score  = round(1 - vector_distance, 3) if vector_distance is not None else None

#     # Date formatting
#     raw_date       = doc.get('published_date_string', '')
#     formatted_date = ''
#     if raw_date:
#         try:
#             if 'T' in raw_date:
#                 dt = datetime.strptime(raw_date.split('T')[0], '%Y-%m-%d')
#             elif '-' in raw_date and len(raw_date) >= 10:
#                 dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
#             else:
#                 dt = None
#             formatted_date = dt.strftime('%b %d, %Y') if dt else raw_date
#         except Exception:
#             formatted_date = raw_date

#     geopoint = (
#         doc.get('location_geopoint') or
#         doc.get('location_coordinates') or
#         [None, None]
#     )

#     return {
#         'id':                    doc.get('document_uuid'),
#         'title':                 doc.get('document_title', 'Untitled'),
#         'image_url':             doc.get('image_url') or [],
#         'logo_url':              doc.get('logo_url') or [],
#         'title_highlighted':     highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary':               doc.get('document_summary', ''),
#         'summary_highlighted':   highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url':                   doc.get('document_url', ''),
#         'source':                doc.get('document_brand', 'unknown'),
#         'site_name':             doc.get('document_brand', 'Website'),
#         'image':                 (doc.get('image_url') or [None])[0],
#         'category':              doc.get('document_category', ''),
#         'data_type':             doc.get('document_data_type', ''),
#         'schema':                doc.get('document_schema', ''),
#         'date':                  formatted_date,
#         'published_date':        formatted_date,
#         'authority_score':       doc.get('authority_score', 0),
#         'cluster_uuid':          doc.get('cluster_uuid'),
#         'semantic_uuid':         doc.get('semantic_uuid'),
#         'key_facts':             doc.get('key_facts', []),
#         'humanized_summary':     '',
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score':        semantic_score,
#         'black_owned':           doc.get('black_owned', False),
#         'service_rating':        doc.get('service_rating'),
#         'service_review_count':  doc.get('service_review_count'),
#         'service_type':          doc.get('service_type', []),
#         'service_specialties':   doc.get('service_specialties', []),
#         'service_price_range':   doc.get('service_price_range'),
#         'service_phone':         doc.get('service_phone'),
#         'service_hours':         doc.get('service_hours'),
#         'location': {
#             'city':    doc.get('location_city'),
#             'state':   doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region':  doc.get('location_region'),
#             'address': doc.get('location_address'),
#             'geopoint': geopoint,
#             'lat':     geopoint[0] if geopoint else None,
#             'lng':     geopoint[1] if geopoint else None,
#         },
#         'time_period': {
#             'start':   doc.get('time_period_start'),
#             'end':     doc.get('time_period_end'),
#             'context': doc.get('time_context'),
#         },
#         'score':           0.5,
#         'related_sources': [],
#     }


# # ============================================================
# # AI OVERVIEW
# # ============================================================

# def humanize_key_facts(
#     key_facts: list,
#     query: str = '',
#     matched_keyword: str = '',
#     question_word: str = None
# ) -> str:
#     """
#     Format key_facts into a readable AfroToDo AI Overview string.

#     Filters facts by question_word type:
#         where → geographic facts
#         when  → temporal facts with years or founding dates
#         who   → facts about people, roles, achievements
#         what  → definition and classification facts

#     Falls back to keyword match, then to the first fact.
#     Returns at most two sentences.
#     """
#     if not key_facts:
#         return ''

#     facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
#     if not facts:
#         return ''

#     if question_word:
#         qw = question_word.lower()

#         if qw == 'where':
#             geo_words = {
#                 'located', 'bounded', 'continent', 'region', 'coast',
#                 'ocean', 'border', 'north', 'south', 'east', 'west',
#                 'latitude', 'longitude', 'hemisphere', 'capital',
#                 'city', 'state', 'country', 'area', 'lies', 'situated',
#             }
#             relevant_facts = [f for f in facts if any(gw in f.lower() for gw in geo_words)]

#         elif qw == 'when':
#             import re as _re
#             temporal_words = {
#                 'founded', 'established', 'born', 'created', 'started',
#                 'opened', 'built', 'year', 'date', 'century', 'decade',
#                 'era', 'period',
#             }
#             relevant_facts = [
#                 f for f in facts
#                 if any(tw in f.lower() for tw in temporal_words)
#                 or _re.search(r'\b\d{4}\b', f)
#             ]

#         elif qw == 'who':
#             who_words = {
#                 'first', 'president', 'founder', 'ceo', 'leader',
#                 'director', 'known', 'famous', 'awarded', 'pioneer',
#                 'invented', 'created', 'named', 'appointed', 'elected',
#             }
#             relevant_facts = [f for f in facts if any(ww in f.lower() for ww in who_words)]

#         elif qw == 'what':
#             what_words = {
#                 'is a', 'refers to', 'defined', 'known as',
#                 'type of', 'form of', 'means', 'represents',
#             }
#             relevant_facts = [f for f in facts if any(ww in f.lower() for ww in what_words)]

#         else:
#             relevant_facts = []

#         if not relevant_facts and matched_keyword:
#             keyword_lower  = matched_keyword.lower()
#             relevant_facts = [f for f in facts if keyword_lower in f.lower()]

#         if not relevant_facts:
#             relevant_facts = [facts[0]]

#     elif matched_keyword:
#         keyword_lower  = matched_keyword.lower()
#         relevant_facts = [f for f in facts if keyword_lower in f.lower()]
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     else:
#         relevant_facts = [facts[0]]

#     relevant_facts = relevant_facts[:2]

#     is_question = query and any(
#         query.lower().startswith(w)
#         for w in ['who', 'what', 'where', 'when', 'why', 'how',
#                   'is', 'are', 'can', 'do', 'does']
#     )

#     if is_question:
#         intros = [
#             "Based on our sources,",
#             "According to our data,",
#             "From what we know,",
#             "Our sources indicate that",
#         ]
#     else:
#         intros = [
#             "Here's what we know:",
#             "From our sources:",
#             "Based on our data:",
#             "Our sources show that",
#         ]

#     intro = random.choice(intros)

#     if len(relevant_facts) == 1:
#         return f"{intro} {relevant_facts[0]}."
#     else:
#         return f"{intro} {relevant_facts[0]}. {relevant_facts[1]}."


# def _should_trigger_ai_overview(
#     signals: Dict,
#     results: List[Dict],
#     query: str
# ) -> bool:
#     """
#     Decide whether to show an AI Overview for this query.

#     Rules:
#         - Never triggers for browse, local, shop modes
#         - Always triggers for answer and compare modes
#         - For explore mode: triggers only when top result's title
#           and key_facts match at least 75% of non-stopword query words
#     """
#     if not results:
#         return False

#     query_mode = signals.get('query_mode', 'explore')

#     if query_mode in ('browse', 'local', 'shop'):
#         return False
#     if query_mode in ('answer', 'compare'):
#         return True

#     if query_mode == 'explore':
#         top_title = results[0].get('title', '').lower()
#         top_facts = ' '.join(results[0].get('key_facts', [])).lower()
#         stopwords  = {
#             'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#             'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#             'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
#         }
#         query_words = [
#             w for w in query.lower().split()
#             if w not in stopwords and len(w) > 1
#         ]
#         if not query_words:
#             return False
#         matches = sum(1 for w in query_words if w in top_title or w in top_facts)
#         return (matches / len(query_words)) >= 0.75

#     return False


# def _build_ai_overview(
#     signals: Dict,
#     results: List[Dict],
#     query: str
# ) -> Optional[str]:
#     """
#     Build the AI Overview text using signal-driven key_fact selection.
#     Picks the matched keyword and question word from signals.
#     Returns None if no key_facts are available on the top result.
#     """
#     if not results or not results[0].get('key_facts'):
#         return None

#     question_word = signals.get('question_word')
#     stopwords     = {
#         'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#         'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#         'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
#     }
#     query_words = [
#         w for w in query.lower().split()
#         if w not in stopwords and len(w) > 1
#     ]

#     matched_keyword = ''
#     if query_words:
#         top_title       = results[0].get('title', '').lower()
#         top_facts       = ' '.join(results[0].get('key_facts', [])).lower()
#         matched_keyword = max(
#             query_words,
#             key=lambda w: (w in top_title) + (w in top_facts)
#         )

#     return humanize_key_facts(
#         results[0]['key_facts'],
#         query,
#         matched_keyword=matched_keyword,
#         question_word=question_word,
#     )


# # ============================================================
# # END OF PART 7
# # ============================================================

# # ============================================================
# # PART 8 OF 8 — MAIN ENTRY POINT + COMPATIBILITY STUBS
# # ============================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Simple intent detection used only on the keyword path where
#     Word Discovery v3 is not run. Returns a basic intent string.
#     """
#     query_lower    = query.lower()
#     location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
#     if any(w in query_lower for w in location_words):
#         return 'location'
#     person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
#     if any(w in query_lower for w in person_words):
#         return 'person'
#     return 'general'


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: str = 'y',
#     answer: str = None,
#     answer_type: str = None,
#     skip_embedding: bool = False,
#     document_uuid: str = None,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search. Called by views.py.

#     Runs one of four paths depending on request type:

#     QUESTION PATH
#         document_uuid + search_source='question' supplied.
#         Fetches that single document directly and returns it.
#         No pipeline. No cache. Fastest path.

#     FAST PATH (cache hit)
#         Finished result package found in Redis.
#         Applies UI filters, paginates, fetches full docs for page.
#         No Typesense search runs.

#     KEYWORD PATH (alt_mode='n' or dropdown source)
#         Stage 1 (keyword+metadata) → Stage 5 (count) →
#         Stage 6 (cache) → Stage 7 (paginate+fetch)
#         No embeddings. No vector search.

#     SEMANTIC PATH (default)
#         Stage 1A+1B (uuids) → Stage 2 (rerank) → Stage 3 (prune) →
#         Stage 4 (metadata) → Stage 5 (_resolve_blend + _score_document
#         per doc + count) → Stage 6 (cache) → Stage 7 (paginate+fetch)
#         Full algorithm runs here.
#     """
#     times = {}
#     t0    = time.time()
#     print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

#     active_data_type = filters.get('data_type') if filters else None
#     active_category  = filters.get('category')  if filters else None
#     active_schema    = filters.get('schema')     if filters else None

#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")

#     # =========================================================================
#     # QUESTION DIRECT PATH
#     # =========================================================================
#     if document_uuid and search_source == 'question':
#         print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
#         t_fetch = time.time()
#         results = fetch_full_documents([document_uuid], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         ai_overview   = None
#         question_word = None
#         q_lower       = query.lower().strip()
#         for word in ('who', 'what', 'where', 'when', 'why', 'how'):
#             if q_lower.startswith(word):
#                 question_word = word
#                 break

#         question_signals = {
#             'query_mode':          'answer',
#             'wants_single_result': True,
#             'question_word':       question_word,
#         }

#         if results and results[0].get('key_facts'):
#             ai_overview = _build_ai_overview(question_signals, results, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
#                 results[0]['humanized_summary'] = ai_overview

#         related_searches = []
#         if results:
#             semantic_uuid = results[0].get('semantic_uuid')
#             if semantic_uuid:
#                 try:
#                     related_docs     = fetch_documents_by_semantic_uuid(
#                         semantic_uuid, exclude_uuid=document_uuid, limit=5
#                     )
#                     related_searches = [
#                         {'query': doc.get('title', ''), 'url': doc.get('url', '')}
#                         for doc in related_docs if doc.get('title')
#                     ]
#                 except Exception as e:
#                     print(f"⚠️ Related searches error: {e}")

#         times['total'] = round((time.time() - t0) * 1000, 2)

#         return {
#             'query':             query,
#             'corrected_query':   query,
#             'intent':            'answer',
#             'query_mode':        'answer',
#             'answer':            answer,
#             'answer_type':       answer_type or 'UNKNOWN',
#             'results':           results,
#             'total':             len(results),
#             'facet_total':       len(results),
#             'total_image_count': 0,
#             'page':              1,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  False,
#             'search_strategy':   'question_direct',
#             'alt_mode':          alt_mode,
#             'skip_embedding':    True,
#             'search_source':     'question',
#             'valid_terms':       query.split(),
#             'unknown_terms':     [],
#             'data_type_facets':  [],
#             'category_facets':   [],
#             'schema_facets':     [],
#             'related_searches':  related_searches,
#             'facets':            {},
#             'word_discovery': {
#                 'valid_count':   len(query.split()),
#                 'unknown_count': 0,
#                 'corrections':   [],
#                 'filters':       [],
#                 'locations':     [],
#                 'sort':          None,
#                 'total_score':   0,
#                 'average_score': 0,
#                 'max_score':     0,
#             },
#             'timings':          times,
#             'filters_applied': {
#                 'data_type':             None,
#                 'category':              None,
#                 'schema':                None,
#                 'is_local_search':       False,
#                 'local_search_strength': 'none',
#             },
#             'signals': question_signals,
#             'profile': {},
#         }

#     # =========================================================================
#     # FAST PATH — finished cache hit
#     # =========================================================================
#     stable_key = _generate_stable_cache_key(session_id, query)
#     finished   = _get_cached_results(stable_key)

#     if finished is not None:
#         print(f"⚡ FAST PATH: '{query}' | page={page} | "
#               f"filter={active_data_type}/{active_category}/{active_schema}")

#         all_results       = finished['all_results']
#         all_facets        = finished['all_facets']
#         facet_total       = finished['facet_total']
#         ai_overview       = finished.get('ai_overview')
#         total_image_count = finished.get('total_image_count', 0)
#         metadata          = finished['metadata']
#         times['cache']    = 'hit (fast path)'

#         filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t_fetch = time.time()
#         results = fetch_full_documents([item['id'] for item in page_items], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         if results and page == 1 and ai_overview:
#             results[0]['humanized_summary'] = ai_overview

#         times['total'] = round((time.time() - t0) * 1000, 2)
#         signals        = metadata.get('signals', {})

#         print(f"⏱️ FAST PATH TIMING: {times}")

#         return {
#             'query':             query,
#             'corrected_query':   metadata.get('corrected_query', query),
#             'intent':            metadata.get('intent', 'general'),
#             'query_mode':        metadata.get('query_mode', 'keyword'),
#             'results':           results,
#             'total':             total_filtered,
#             'facet_total':       facet_total,
#             'total_image_count': total_image_count,
#             'page':              page,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  metadata.get('semantic_enabled', False),
#             'search_strategy':   metadata.get('search_strategy', 'cached'),
#             'alt_mode':          alt_mode,
#             'skip_embedding':    skip_embedding,
#             'search_source':     search_source,
#             'valid_terms':       metadata.get('valid_terms', query.split()),
#             'unknown_terms':     metadata.get('unknown_terms', []),
#             'data_type_facets':  all_facets.get('data_type', []),
#             'category_facets':   all_facets.get('category', []),
#             'schema_facets':     all_facets.get('schema', []),
#             'related_searches':  [],
#             'facets':            all_facets,
#             'word_discovery':    metadata.get('word_discovery', {
#                 'valid_count': len(query.split()), 'unknown_count': 0,
#                 'corrections': [], 'filters': [], 'locations': [],
#                 'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#             }),
#             'timings':          times,
#             'filters_applied':  metadata.get('filters_applied', {
#                 'data_type': active_data_type, 'category': active_category,
#                 'schema': active_schema, 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }),
#             'signals': signals,
#             'profile': metadata.get('profile', {}),
#         }

#     # =========================================================================
#     # FULL PATH — no cache
#     # =========================================================================
#     print(f"🔬 FULL PATH: '{query}' (no cache for key={stable_key[:12]}...)")

#     is_keyword_path = (
#         alt_mode == 'n' or
#         search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
#     )

#     # =========================================================================
#     # KEYWORD PATH — Stage 1 → 5 → 6 → 7
#     # =========================================================================
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PIPELINE: '{query}'")

#         intent  = detect_query_intent(query, pos_tags)
#         profile = {
#             'search_terms':     query.split(),
#             'cities':           [],
#             'states':           [],
#             'location_terms':   [],
#             'primary_intent':   intent,
#             'field_boosts': {
#                 'primary_keywords':  10,
#                 'entity_names':       8,
#                 'semantic_keywords':  6,
#                 'key_facts':          4,
#                 'document_title':     3,
#             },
#             'corrections':      [],
#             'persons':          [],
#             'organizations':    [],
#             'keywords':         [],
#             'media':            [],
#             'preferred_data_types': ['article'],
#         }

#         t1          = time.time()
#         all_results = fetch_candidates_with_metadata(query, profile)
#         times['stage1'] = round((time.time() - t1) * 1000, 2)

#         counts = count_all(all_results)

#         _set_cached_results(stable_key, {
#             'all_results':       all_results,
#             'all_facets':        counts['facets'],
#             'facet_total':       counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'ai_overview':       None,
#             'metadata': {
#                 'corrected_query':  query,
#                 'intent':           intent,
#                 'query_mode':       'keyword',
#                 'semantic_enabled': False,
#                 'search_strategy':  'keyword_graph_filter',
#                 'valid_terms':      query.split(),
#                 'unknown_terms':    [],
#                 'signals':          {},
#                 'profile':          profile,
#                 'word_discovery': {
#                     'valid_count': len(query.split()), 'unknown_count': 0,
#                     'corrections': [], 'filters': [], 'locations': [],
#                     'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#                 },
#                 'filters_applied': {
#                     'data_type': active_data_type, 'category': active_category,
#                     'schema': active_schema, 'is_local_search': False,
#                     'local_search_strength': 'none',
#                 },
#             },
#         })

#         filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t2      = time.time()
#         results = fetch_full_documents([item['id'] for item in page_items], query)
#         times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
#         times['total']      = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ KEYWORD TIMING: {times}")

#         return {
#             'query':             query,
#             'corrected_query':   query,
#             'intent':            intent,
#             'query_mode':        'keyword',
#             'results':           results,
#             'total':             total_filtered,
#             'facet_total':       counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'page':              page,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  False,
#             'search_strategy':   'keyword_graph_filter',
#             'alt_mode':          alt_mode,
#             'skip_embedding':    True,
#             'search_source':     search_source or 'dropdown',
#             'valid_terms':       query.split(),
#             'unknown_terms':     [],
#             'data_type_facets':  counts['facets'].get('data_type', []),
#             'category_facets':   counts['facets'].get('category', []),
#             'schema_facets':     counts['facets'].get('schema', []),
#             'related_searches':  [],
#             'facets':            counts['facets'],
#             'word_discovery': {
#                 'valid_count': len(query.split()), 'unknown_count': 0,
#                 'corrections': [], 'filters': [], 'locations': [],
#                 'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#             },
#             'timings':          times,
#             'filters_applied': {
#                 'data_type': active_data_type, 'category': active_category,
#                 'schema': active_schema, 'is_local_search': False,
#                 'local_search_strength': 'none',
#             },
#             'signals': {},
#             'profile': profile,
#         }

#     # =========================================================================
#     # SEMANTIC PATH — Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
#     # =========================================================================
#     print(f"🔬 SEMANTIC PIPELINE: '{query}'")

#     # Stage 0 — Word Discovery + embedding in parallel
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

#     # Intent detection
#     signals = {}
#     if INTENT_DETECT_AVAILABLE:
#         try:
#             discovery = detect_intent(discovery)
#             signals   = discovery.get('signals', {})
#             print(f"   🎯 Intent: mode={signals.get('query_mode')} "
#                   f"domain={signals.get('primary_domain')} "
#                   f"local={signals.get('is_local_search')} "
#                   f"black_owned={signals.get('has_black_owned')} "
#                   f"superlative={signals.get('has_superlative')}")
#         except Exception as e:
#             print(f"   ⚠️ intent_detect error: {e}")

#     corrected_query  = discovery.get('corrected_query', query)
#     semantic_enabled = query_embedding is not None
#     query_mode       = signals.get('query_mode', 'explore')

#     # Read v3 profile
#     t2      = time.time()
#     profile = _read_v3_profile(discovery, signals=signals)
#     times['read_profile'] = round((time.time() - t2) * 1000, 2)

#     # Apply pos_mismatch corrections to search_terms
#     corrections = discovery.get('corrections', [])
#     if corrections:
#         correction_map = {
#             c['original'].lower(): c['corrected']
#             for c in corrections
#             if c.get('original') and c.get('corrected')
#             and c.get('correction_type') == 'pos_mismatch'
#         }
#         if correction_map:
#             original_terms          = profile['search_terms']
#             profile['search_terms'] = [
#                 correction_map.get(t.lower(), t) for t in original_terms
#             ]

#     intent      = profile['primary_intent']
#     city_names  = [c['name'] for c in profile['cities']]
#     state_names = [s['name'] for s in profile['states']]

#     print(f"   Intent: {intent} | Mode: {query_mode}")
#     print(f"   Cities: {city_names} | States: {state_names}")
#     print(f"   Search Terms: {profile['search_terms']}")

#     # Stage 1 — candidate UUIDs
#     t3 = time.time()

#     UNSAFE_CATEGORIES = {
#         'Food', 'US City', 'US State', 'Country', 'Location',
#         'City', 'Place', 'Object', 'Animal', 'Color',
#     }
#     has_unsafe_corrections = any(
#         c.get('correction_type') == 'pos_mismatch' or
#         c.get('category', '') in UNSAFE_CATEGORIES
#         for c in corrections
#     )
#     search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

#     candidate_uuids = fetch_all_candidate_uuids(
#         search_query_for_stage1, profile, query_embedding,
#         signals=signals, discovery=discovery,
#     )
#     times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)

#     # Stage 2 + 3 — vector rerank + distance prune
#     survivor_uuids = candidate_uuids
#     vector_data    = {}

#     if semantic_enabled and candidate_uuids:
#         t4       = time.time()
#         reranked = semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
#         times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

#         vector_data = {
#             item['id']: {
#                 'vector_distance': item.get('vector_distance', 1.0),
#                 'semantic_rank':   item.get('semantic_rank', 999999),
#             }
#             for item in reranked
#         }

#         DISTANCE_THRESHOLDS = {
#             'answer':  0.60,
#             'explore': 0.70,
#             'compare': 0.65,
#             'browse':  0.85,
#             'local':   0.85,
#             'shop':    0.80,
#         }
#         threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

#         before_prune   = len(candidate_uuids)
#         survivor_uuids = [
#             uuid for uuid in candidate_uuids
#             if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
#         ]
#         after_prune = len(survivor_uuids)

#         if before_prune != after_prune:
#             print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): "
#                   f"{before_prune} → {after_prune} "
#                   f"({before_prune - after_prune} removed)")
#         times['stage3_prune'] = f"{before_prune} → {after_prune}"

#     else:
#         print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, "
#               f"candidates={len(candidate_uuids)}")

#     # Stage 4 — metadata for survivors
#     t5          = time.time()
#     all_results = fetch_candidate_metadata(survivor_uuids)
#     times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

#     # Stage 5 — resolve blend once, score every document
#     if all_results:
#         blend      = _resolve_blend(query_mode, signals, all_results)
#         pool_size  = len(all_results)

#         for idx, item in enumerate(all_results):
#             _score_document(
#                 idx        = idx,
#                 item       = item,
#                 profile    = profile,
#                 signals    = signals,
#                 blend      = blend,
#                 pool_size  = pool_size,
#                 vector_data = vector_data,
#             )

#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#     counts = count_all(all_results)

#     # AI Overview preview
#     ai_overview = None
#     if all_results:
#         preview_items, _ = paginate_cached_results(all_results, 1, per_page)
#         preview_docs     = fetch_full_documents([item['id'] for item in preview_items], query)
#         if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
#             ai_overview = _build_ai_overview(signals, preview_docs, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview: {ai_overview[:80]}...")

#     valid_terms   = profile['search_terms']
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

#     locations_block = (
#         [
#             {'field': 'location_city',  'values': city_names},
#             {'field': 'location_state', 'values': state_names},
#         ]
#         if city_names or state_names else []
#     )

#     # Stage 6 — cache the finished package
#     _set_cached_results(stable_key, {
#         'all_results':       all_results,
#         'all_facets':        counts['facets'],
#         'facet_total':       counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'ai_overview':       ai_overview,
#         'metadata': {
#             'corrected_query':  corrected_query,
#             'intent':           intent,
#             'query_mode':       query_mode,
#             'semantic_enabled': semantic_enabled,
#             'search_strategy':  'staged_semantic' if semantic_enabled else 'keyword_fallback',
#             'valid_terms':      valid_terms,
#             'unknown_terms':    unknown_terms,
#             'signals':          signals,
#             'city_names':       city_names,
#             'state_names':      state_names,
#             'profile':          profile,
#             'word_discovery': {
#                 'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
#                 'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#                 'corrections':   discovery.get('corrections', []),
#                 'filters':       [],
#                 'locations':     locations_block,
#                 'sort':          None,
#                 'total_score':   0,
#                 'average_score': 0,
#                 'max_score':     0,
#             },
#             'filters_applied': {
#                 'data_type':             active_data_type,
#                 'category':              active_category,
#                 'schema':                active_schema,
#                 'is_local_search':       signals.get('is_local_search', False),
#                 'local_search_strength': signals.get('local_search_strength', 'none'),
#                 'has_black_owned':       signals.get('has_black_owned', False),
#                 'graph_filters':         [],
#                 'graph_locations':       locations_block,
#                 'graph_sort':            None,
#             },
#         },
#     })
#     print(f"💾 Cached semantic package: {counts['facet_total']} results, "
#           f"{counts['total_image_count']} image docs")

#     # Stage 7 — filter → paginate → fetch full docs
#     filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#     t6      = time.time()
#     results = fetch_full_documents([item['id'] for item in page_items], query)
#     times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

#     if results and page == 1 and ai_overview:
#         results[0]['humanized_summary'] = ai_overview

#     if query_embedding:
#         try:
#             store_query_embedding(
#                 corrected_query, query_embedding,
#                 result_count=counts['facet_total']
#             )
#         except Exception as e:
#             print(f"⚠️ store_query_embedding error: {e}")

#     times['total'] = round((time.time() - t0) * 1000, 2)
#     strategy       = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

#     print(f"⏱️ SEMANTIC TIMING: {times}")
#     print(f"🔍 {strategy.upper()} ({query_mode}) | "
#           f"Total: {counts['facet_total']} | "
#           f"Filtered: {total_filtered} | "
#           f"Page: {len(results)} | "
#           f"Images: {counts['total_image_count']}")

#     return {
#         'query':             query,
#         'corrected_query':   corrected_query,
#         'intent':            intent,
#         'query_mode':        query_mode,
#         'results':           results,
#         'total':             total_filtered,
#         'facet_total':       counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'page':              page,
#         'per_page':          per_page,
#         'search_time':       round(time.time() - t0, 3),
#         'session_id':        session_id,
#         'semantic_enabled':  semantic_enabled,
#         'search_strategy':   strategy,
#         'alt_mode':          alt_mode,
#         'skip_embedding':    skip_embedding,
#         'search_source':     search_source,
#         'valid_terms':       valid_terms,
#         'unknown_terms':     unknown_terms,
#         'related_searches':  [],
#         'data_type_facets':  counts['facets'].get('data_type', []),
#         'category_facets':   counts['facets'].get('category', []),
#         'schema_facets':     counts['facets'].get('schema', []),
#         'facets':            counts['facets'],
#         'word_discovery': {
#             'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
#             'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#             'corrections':   discovery.get('corrections', []),
#             'filters':       [],
#             'locations':     locations_block,
#             'sort':          None,
#             'total_score':   0,
#             'average_score': 0,
#             'max_score':     0,
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type':             active_data_type,
#             'category':              active_category,
#             'schema':                active_schema,
#             'is_local_search':       signals.get('is_local_search', False),
#             'local_search_strength': signals.get('local_search_strength', 'none'),
#             'has_black_owned':       signals.get('has_black_owned', False),
#             'graph_filters':         [],
#             'graph_locations':       locations_block,
#             'graph_sort':            None,
#         },
#         'signals': signals,
#         'profile': profile,
#     }


# # ============================================================
# # COMPATIBILITY STUBS — keep views.py imports working
# # ============================================================

# def get_facets(query: str) -> dict:
#     """Returns empty dict. Kept for views.py import compatibility."""
#     return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """Returns empty list. Kept for views.py import compatibility."""
#     return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns a featured snippet if the top result has high authority."""
#     if not results:
#         return None
#     top = results[0]
#     if top.get('authority_score', 0) >= 85:
#         return {
#             'type':      'featured_snippet',
#             'title':     top.get('title'),
#             'snippet':   top.get('summary', ''),
#             'key_facts': top.get('key_facts', [])[:3],
#             'source':    top.get('source'),
#             'url':       top.get('url'),
#             'image':     top.get('image'),
#         }
#     return None


# def log_search_event(**kwargs):
#     """No-op. Kept for views.py import compatibility."""
#     pass


# def typesense_search(
#     query: str = '*',
#     filter_by: str = None,
#     sort_by: str = 'authority_score:desc',
#     per_page: int = 20,
#     page: int = 1,
#     facet_by: str = None,
#     query_by: str = 'document_title,document_summary,keywords,primary_keywords',
#     max_facet_values: int = 20,
# ) -> Dict:
#     """Simple Typesense search wrapper for direct use outside the pipeline."""
#     params = {
#         'q':        query,
#         'query_by': query_by,
#         'per_page': per_page,
#         'page':     page,
#     }
#     if filter_by:
#         params['filter_by'] = filter_by
#     if sort_by:
#         params['sort_by'] = sort_by
#     if facet_by:
#         params['facet_by']         = facet_by
#         params['max_facet_values'] = max_facet_values

#     try:
#         return client.collections[COLLECTION_NAME].documents.search(params)
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================
# # END OF PART 8 — END OF FILE
# # ============================================================


# ============================================================
# FILE: typesense_discovery_bridge.py
# ============================================================
# ASYNC VERSION — converted from synchronous to async/await.
#
# CHANGES FROM SYNC VERSION:
#   - ThreadPoolExecutor replaced with asyncio.gather()
#   - All Typesense calls wrapped in asyncio.to_thread()
#   - All Django cache calls wrapped in asyncio.to_thread()
#   - All external module calls (WordDiscovery, embedding,
#     intent_detect, store_query_embedding) wrapped in
#     asyncio.to_thread()
#   - Pure CPU functions (scoring, filtering, formatting,
#     counting, profile reading) remain synchronous
#   - Return formats are IDENTICAL to the sync version
#
# HOW TO USE:
#   In views.py, change:
#     result = execute_full_search(...)
#   To:
#     result = await execute_full_search(...)
#   And make your view function: async def your_view(request):
# ============================================================


# ============================================================
# PART 1 OF 8 — IMPORTS, CONSTANTS, UTILITY FUNCTIONS
# ============================================================

# """
# typesense_discovery_bridge.py (ASYNC)
# =====================================
# AfroToDo Search Bridge — Word Discovery v3 + Intent Detection + Typesense.

# SCORING ALGORITHM (v4)
# ----------------------
# final_score = (
#     blend['text_match'] * text_score      +
#     blend['semantic']   * semantic_score  +
#     blend['authority']  * authority_score_n
# )
# final_score *= _domain_relevance(doc, signals)
# final_score *= _content_intent_match(doc, query_mode)
# final_score *= _pool_type_multiplier(doc, query_mode)

# PIPELINE
# --------
# SEMANTIC:  1A+1B → 2 (rerank) → 3 (prune) → 4 (metadata) → 5 (score+count) → 6 (cache) → 7 (paginate+fetch)
# KEYWORD:   1 (uuids+metadata) → 5 (count) → 6 (cache) → 7 (paginate+fetch)
# QUESTION:  direct fetch → format → return
# """

# import re
# import json
# import math
# import time
# import asyncio
# import hashlib
# import typesense
# from typing import Dict, List, Tuple, Optional, Any, Set
# from datetime import datetime
# from decouple import config
# import requests
# import random


# # ── Word Discovery v3 ────────────────────────────────────────────────────────

# try:
#     from .word_discovery_fulltest import WordDiscovery
#     WORD_DISCOVERY_AVAILABLE = True
#     print("✅ WordDiscovery imported from .word_discovery_fulltest")
# except ImportError:
#     try:
#         from word_discovery_fulltest import WordDiscovery
#         WORD_DISCOVERY_AVAILABLE = True
#         print("✅ WordDiscovery imported from word_discovery_fulltest")
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ WordDiscovery not available")


# # ── Intent Detection ─────────────────────────────────────────────────────────

# try:
#     from .intent_detect import detect_intent, get_signals
#     INTENT_DETECT_AVAILABLE = True
#     print("✅ intent_detect imported")
# except ImportError:
#     try:
#         from intent_detect import detect_intent, get_signals
#         INTENT_DETECT_AVAILABLE = True
#         print("✅ intent_detect imported (fallback)")
#     except ImportError:
#         INTENT_DETECT_AVAILABLE = False
#         print("⚠️ intent_detect not available")


# # ── Embedding Client ─────────────────────────────────────────────────────────

# try:
#     from .embedding_client import get_query_embedding
#     print("✅ get_query_embedding imported from .embedding_client")
# except ImportError:
#     try:
#         from embedding_client import get_query_embedding
#         print("✅ get_query_embedding imported from embedding_client")
#     except ImportError:
#         def get_query_embedding(query: str) -> Optional[List[float]]:
#             print("⚠️ embedding_client not available")
#             return None


# # ── Related Search Store ─────────────────────────────────────────────────────

# try:
#     from .cached_embedding_related_search import store_query_embedding
#     print("✅ store_query_embedding imported")
# except ImportError:
#     try:
#         from cached_embedding_related_search import store_query_embedding
#         print("✅ store_query_embedding imported (fallback)")
#     except ImportError:
#         def store_query_embedding(*args, **kwargs):
#             return False
#         print("⚠️ store_query_embedding not available")


# # ── Django Cache ─────────────────────────────────────────────────────────────

# from django.core.cache import cache as django_cache


# # ── Typesense Client ─────────────────────────────────────────────────────────

# client = typesense.Client({
#     'api_key':  config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host':     config('TYPESENSE_HOST'),
#         'port':     config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL'),
#     }],
#     'connection_timeout_seconds': 5,
# })

# COLLECTION_NAME = 'document'


# # ── Cache Settings ───────────────────────────────────────────────────────────

# CACHE_TTL_SECONDS  = 300
# MAX_CACHED_RESULTS = 100


# # ── UI Labels ────────────────────────────────────────────────────────────────

# DATA_TYPE_LABELS = {
#     'article':  'Articles',
#     'person':   'People',
#     'business': 'Businesses',
#     'place':    'Places',
#     'media':    'Media',
#     'event':    'Events',
#     'product':  'Products',
# }

# CATEGORY_LABELS = {
#     'healthcare_medical': 'Healthcare',
#     'fashion':            'Fashion',
#     'beauty':             'Beauty',
#     'food_recipes':       'Food & Recipes',
#     'travel_tourism':     'Travel',
#     'entertainment':      'Entertainment',
#     'business':           'Business',
#     'education':          'Education',
#     'technology':         'Technology',
#     'sports':             'Sports',
#     'finance':            'Finance',
#     'real_estate':        'Real Estate',
#     'lifestyle':          'Lifestyle',
#     'news':               'News',
#     'culture':            'Culture',
#     'general':            'General',
# }

# US_STATE_ABBREV = {
#     'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
#     'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
#     'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
#     'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
#     'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
#     'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
#     'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
#     'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
#     'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
#     'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
#     'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
#     'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
#     'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
# }


# # ── Blend Ratios (base — _resolve_blend adjusts at runtime) ──────────────────

# BLEND_RATIOS = {
#     'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
#     'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
#     'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
#     'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
# }


# # ── Pool Scoping per Query Mode ───────────────────────────────────────────────

# POOL_SCOPE = {
#     'local':   {'primary': 'business',  'allow': {'business', 'place'}},
#     'shop':    {'primary': 'product',   'allow': {'product', 'business'}},
#     'answer':  {'primary': None,        'allow': {'article', 'person', 'place'}},
#     'browse':  {'primary': None,        'allow': {'article', 'business', 'product'}},
#     'explore': {'primary': None,        'allow': {'article', 'person', 'media'}},
#     'compare': {'primary': None,        'allow': {'article', 'person', 'business'}},
# }


# # ── Data Type Preferences ─────────────────────────────────────────────────────

# DATA_TYPE_PREFERENCES = {
#     'answer':  ['article', 'person', 'place'],
#     'explore': ['article', 'person', 'media'],
#     'browse':  ['article', 'business', 'product'],
#     'local':   ['business', 'place', 'article'],
#     'shop':    ['product', 'business', 'article'],
#     'compare': ['article', 'person', 'business'],
# }


# # ── Domain → Document Category Alignment ─────────────────────────────────────

# DOMAIN_CATEGORY_MAP = {
#     'food':        {'food_recipes', 'dining', 'lifestyle'},
#     'business':    {'business', 'finance', 'entrepreneurship'},
#     'health':      {'healthcare_medical', 'wellness', 'fitness'},
#     'music':       {'entertainment', 'music', 'culture'},
#     'fashion':     {'fashion', 'beauty', 'lifestyle'},
#     'education':   {'education', 'hbcu', 'scholarship'},
#     'travel':      {'travel_tourism', 'lifestyle'},
#     'real_estate': {'real_estate', 'business'},
#     'sports':      {'sports', 'entertainment'},
#     'technology':  {'technology', 'business'},
#     'beauty':      {'beauty', 'lifestyle', 'fashion'},
#     'culture':     {'culture', 'news', 'lifestyle'},
# }


# # ── Content Intent Alignment per Query Mode ───────────────────────────────────

# INTENT_CONTENT_MAP = {
#     'local':   {'transactional', 'navigational'},
#     'shop':    {'transactional', 'commercial'},
#     'answer':  {'informational', 'educational'},
#     'browse':  {'informational', 'commercial', 'transactional'},
#     'explore': {'informational', 'educational'},
#     'compare': {'informational', 'commercial'},
# }


# # ── Scoring Thresholds ────────────────────────────────────────────────────────

# SEMANTIC_DISTANCE_GATE    = 0.65
# QUESTION_SEMANTIC_DISTANCE_GATE = 0.40
# REVIEW_COUNT_SCALE_BIZ    = 500
# REVIEW_COUNT_SCALE_RECIPE = 200
# BLACK_OWNED_BOOST         = 0.12
# PREFERRED_TYPE_BOOST      = 0.08
# SUPERLATIVE_SCORE_CAP     = 0.70


# # ── Utility Functions ─────────────────────────────────────────────────────────

# def _parse_rank(rank_value: Any) -> int:
#     """Safely convert any rank value to an integer."""
#     if isinstance(rank_value, int):
#         return rank_value
#     try:
#         return int(float(rank_value))
#     except (ValueError, TypeError):
#         return 0


# def _has_real_images(item: Dict) -> bool:
#     """Return True if the candidate has at least one non-empty image or logo URL."""
#     image_urls = item.get('image_url', [])
#     if isinstance(image_urls, str):
#         image_urls = [image_urls]
#     if any(u for u in image_urls if u):
#         return True
#     logo_urls = item.get('logo_url', [])
#     if isinstance(logo_urls, str):
#         logo_urls = [logo_urls]
#     return any(u for u in logo_urls if u)


# def _count_images_from_candidates(all_results: List[Dict]) -> int:
#     """Count documents in the result set that have at least one real image."""
#     return sum(1 for item in all_results if _has_real_images(item))


# def _generate_stable_cache_key(session_id: str, query: str) -> str:
#     """Build a deterministic MD5 cache key from session ID and normalized query."""
#     normalized = query.strip().lower()
#     key_string = f"final|{session_id or 'nosession'}|{normalized}"
#     return hashlib.md5(key_string.encode()).hexdigest()


# # ============================================================
# # END OF PART 1
# # ============================================================


# # ============================================================
# # PART 2 OF 8 — CACHE FUNCTIONS + WORD DISCOVERY RUNNERS
# # ============================================================

# async def _get_cached_results(cache_key: str):
#     """
#     Get the finished result package from Redis.
#     Returns the cached dict or None on miss or error.
#     """
#     try:
#         data = await asyncio.to_thread(django_cache.get, cache_key)
#         if data is not None:
#             print(f"🟢 Redis cache HIT: {cache_key[:12]}...")
#             return data
#         print(f"🔴 Redis cache MISS: {cache_key[:12]}...")
#         return None
#     except Exception as e:
#         print(f"⚠️ Redis cache GET error: {e}")
#         return None


# async def _set_cached_results(cache_key: str, data: Dict) -> None:
#     """
#     Write the finished result package to Redis with TTL.
#     Silently absorbs errors so a cache failure never breaks search.
#     """
#     try:
#         await asyncio.to_thread(django_cache.set, cache_key, data, CACHE_TTL_SECONDS)
#         print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
#     except Exception as e:
#         print(f"⚠️ Redis cache SET error: {e}")


# async def clear_search_cache() -> None:
#     """Clear all cached search results."""
#     try:
#         await asyncio.to_thread(django_cache.clear)
#         print("🧹 Redis search cache cleared")
#     except Exception as e:
#         print(f"⚠️ Redis cache CLEAR error: {e}")


# def _run_word_discovery_sync(query: str) -> Dict:
#     """
#     Run Word Discovery v3 on the query string (synchronous).
#     Returns the full pre-classified profile dict.
#     Falls back to a minimal safe structure if WD is unavailable.
#     """
#     if WORD_DISCOVERY_AVAILABLE:
#         try:
#             wd     = WordDiscovery(verbose=False)
#             result = wd.process(query)
#             return result
#         except Exception as e:
#             print(f"⚠️ WordDiscovery error: {e}")

#     return {
#         'query':                   query,
#         'corrected_query':         query,
#         'corrected_display_query': query,
#         'search_terms':            [],
#         'persons':                 [],
#         'organizations':           [],
#         'keywords':                [],
#         'media':                   [],
#         'cities':                  [],
#         'states':                  [],
#         'location_terms':          [],
#         'primary_intent':          'general',
#         'intent_scores':           {},
#         'field_boosts':            {},
#         'corrections':             [],
#         'terms':                   [],
#         'ngrams':                  [],
#         'stats': {
#             'total_words':     len(query.split()),
#             'valid_words':     0,
#             'corrected_words': 0,
#             'unknown_words':   len(query.split()),
#             'stopwords':       0,
#             'ngram_count':     0,
#         },
#     }


# async def _run_word_discovery(query: str) -> Dict:
#     """Async wrapper — runs WordDiscovery in a thread."""
#     try:
#         return await asyncio.to_thread(_run_word_discovery_sync, query)
#     except Exception as e:
#         print(f"⚠️ _run_word_discovery async error: {e}")
#         return _run_word_discovery_sync.__wrapped__(query) if hasattr(_run_word_discovery_sync, '__wrapped__') else {
#             'query': query, 'corrected_query': query, 'corrected_display_query': query,
#             'search_terms': [], 'persons': [], 'organizations': [], 'keywords': [],
#             'media': [], 'cities': [], 'states': [], 'location_terms': [],
#             'primary_intent': 'general', 'intent_scores': {}, 'field_boosts': {},
#             'corrections': [], 'terms': [], 'ngrams': [],
#             'stats': {'total_words': len(query.split()), 'valid_words': 0,
#                       'corrected_words': 0, 'unknown_words': len(query.split()),
#                       'stopwords': 0, 'ngram_count': 0},
#         }


# def _run_embedding_sync(query: str) -> Optional[List[float]]:
#     """
#     Call the embedding client and return the query vector (synchronous).
#     Returns None if the client is unavailable.
#     """
#     return get_query_embedding(query)


# async def _run_embedding(query: str) -> Optional[List[float]]:
#     """Async wrapper — runs embedding generation in a thread."""
#     try:
#         return await asyncio.to_thread(_run_embedding_sync, query)
#     except Exception as e:
#         print(f"⚠️ _run_embedding async error: {e}")
#         return None


# async def run_parallel_prep(
#     query: str,
#     skip_embedding: bool = False
# ) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run Word Discovery v3 and embedding generation in parallel using asyncio.gather.

#     FIX — frozenset serialization bug:
#         WD v3 writes context_flags as a frozenset on each term dict.
#         frozenset is not JSON-serializable and silently breaks Redis
#         caching, causing every request to bypass cache and run the
#         full pipeline. This function converts every context_flags
#         value to a sorted list before returning.

#     Embedding re-use logic:
#         Always embeds the original query first.
#         Only re-embeds with corrected_query when all corrections are
#         safe. Unsafe categories (Food, US City, US State, Country,
#         Location, City, Place, Object, Animal, Color) are never
#         re-embedded because replacing them changes semantic meaning.
#     """
#     if skip_embedding:
#         discovery = await _run_word_discovery(query)
#         for term in discovery.get('terms', []):
#             if isinstance(term.get('context_flags'), (frozenset, set)):
#                 term['context_flags'] = sorted(list(term['context_flags']))
#         return discovery, None

#     # Run both in parallel
#     discovery, embedding = await asyncio.gather(
#         _run_word_discovery(query),
#         _run_embedding(query),
#     )

#     # FIX — convert frozenset context_flags to sorted list
#     for term in discovery.get('terms', []):
#         if isinstance(term.get('context_flags'), (frozenset, set)):
#             term['context_flags'] = sorted(list(term['context_flags']))

#     corrected_query = discovery.get('corrected_query', query)

#     if corrected_query.lower() != query.lower() and embedding is not None:
#         corrections = discovery.get('corrections', [])

#         UNSAFE_CATEGORIES = {
#             'Food', 'US City', 'US State', 'Country', 'Location',
#             'City', 'Place', 'Object', 'Animal', 'Color',
#         }

#         safe_corrections   = []
#         unsafe_corrections = []

#         for c in corrections:
#             corrected_category = c.get('category', '')
#             correction_type    = c.get('correction_type', '')

#             if (correction_type == 'pos_mismatch' or
#                     corrected_category in UNSAFE_CATEGORIES or
#                     c.get('category', '') in ('Person', 'Organization', 'Brand')):
#                 unsafe_corrections.append(c)
#             else:
#                 safe_corrections.append(c)

#         if unsafe_corrections:
#             print(f"⚠️ Skipping re-embed — unsafe corrections detected:")
#             for c in unsafe_corrections:
#                 print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
#                       f"(type={c.get('correction_type')}, category={c.get('category')})")
#         elif safe_corrections:
#             print(f"✅ Re-embedding with corrected query: '{corrected_query}'")
#             embedding = await _run_embedding(corrected_query)

#     return discovery, embedding


# # ============================================================
# # END OF PART 2
# # ============================================================

# # ============================================================
# # PART 3 OF 8 — V3 PROFILE READER + TYPESENSE PARAMETER BUILDERS
# # ============================================================

# def _read_v3_profile(discovery: Dict, signals: Dict = None) -> Dict:
#     """
#     Read the pre-classified v3 profile directly.
#     O(1) field reads with safe defaults — no re-classification.
#     Adds preferred_data_types from a single dict lookup on query_mode.
#     """
#     query_mode = (signals or {}).get('query_mode', 'explore')

#     return {
#         'search_terms':      discovery.get('search_terms', []),
#         'persons':           discovery.get('persons', []),
#         'organizations':     discovery.get('organizations', []),
#         'keywords':          discovery.get('keywords', []),
#         'media':             discovery.get('media', []),
#         'cities':            discovery.get('cities', []),
#         'states':            discovery.get('states', []),
#         'location_terms':    discovery.get('location_terms', []),
#         'primary_intent':    discovery.get('primary_intent', 'general'),
#         'intent_scores':     discovery.get('intent_scores', {}),
#         'field_boosts':      discovery.get('field_boosts', {
#             'document_title':   10,
#             'entity_names':      2,
#             'primary_keywords':  3,
#             'key_facts':         3,
#             'semantic_keywords': 2,
#         }),
#         'corrections':       discovery.get('corrections', []),
#         'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
#         'has_person':        bool(discovery.get('persons')),
#         'has_organization':  bool(discovery.get('organizations')),
#         'has_location':      bool(
#             discovery.get('cities') or
#             discovery.get('states') or
#             discovery.get('location_terms')
#         ),
#         'has_keyword':       bool(discovery.get('keywords')),
#         'has_media':         bool(discovery.get('media')),
#     }


# def build_typesense_params(
#     profile: Dict,
#     ui_filters: Dict = None,
#     signals: Dict = None
# ) -> Dict:
#     """
#     Convert the v3 profile into Typesense search parameters.

#     Builds query_by, query_by_weights, filter_by, sort_by,
#     typo settings, and prefix settings from the profile and signals.

#     FIX — local mode pool scoping:
#         Adds document_data_type:=business to filter_by when
#         query_mode is local and no UI data_type filter overrides it.
#         This prevents restaurant queries competing against every
#         article in the index.
#     """
#     signals    = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
#     params     = {}

#     # ── Query string — deduplicate search_terms ───────────────────────────
#     seen         = set()
#     unique_terms = []
#     for term in profile.get('search_terms', []):
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)

#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'

#     # ── Field boosts — read from v3, add mode-specific fields ────────────
#     field_boosts = dict(profile.get('field_boosts', {}))

#     if query_mode == 'local':
#         field_boosts.setdefault('service_type',        12)
#         field_boosts.setdefault('service_specialties', 10)

#     sorted_fields              = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
#     params['query_by']         = ','.join(f[0] for f in sorted_fields)
#     params['query_by_weights'] = ','.join(str(f[1]) for f in sorted_fields)

#     # ── Typo / prefix / drop-token settings by mode ──────────────────────
#     has_corrections = bool(profile.get('corrections'))
#     term_count      = len(unique_terms)

#     if query_mode == 'answer':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'explore':
#         params['num_typos']             = 0 if has_corrections else 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
#     elif query_mode == 'browse':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
#     elif query_mode == 'local':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     elif query_mode == 'compare':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'shop':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     else:
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1

#     # ── Sort order ────────────────────────────────────────────────────────
#     temporal_direction = signals.get('temporal_direction')
#     price_direction    = signals.get('price_direction')
#     has_superlative    = signals.get('has_superlative', False)
#     has_rating         = signals.get('has_rating_signal', False)

#     if temporal_direction == 'oldest':
#         params['sort_by'] = 'time_period_start:asc,authority_score:desc'
#     elif temporal_direction == 'newest':
#         params['sort_by'] = 'published_date:desc,authority_score:desc'
#     elif price_direction == 'cheap':
#         params['sort_by'] = 'product_price:asc,authority_score:desc'
#     elif price_direction == 'expensive':
#         params['sort_by'] = 'product_price:desc,authority_score:desc'
#     elif query_mode == 'local':
#         params['sort_by'] = 'authority_score:desc'
#     elif query_mode == 'browse' and has_superlative:
#         params['sort_by'] = 'authority_score:desc'
#     elif has_rating:
#         params['sort_by'] = 'authority_score:desc'
#     else:
#         params['sort_by'] = '_text_match:desc,authority_score:desc'

#     # ── filter_by — locations + black_owned + local scope + UI filters ────
#     filter_conditions = []

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     # FIX — local mode: scope pool to business documents only
#     if query_mode == 'local' and not (ui_filters and ui_filters.get('data_type')):
#         filter_conditions.append('document_data_type:=business')

#     if ui_filters:
#         if ui_filters.get('data_type'):
#             filter_conditions.append(f"document_data_type:={ui_filters['data_type']}")
#         if ui_filters.get('category'):
#             filter_conditions.append(f"document_category:={ui_filters['category']}")
#         if ui_filters.get('schema'):
#             filter_conditions.append(f"document_schema:={ui_filters['schema']}")

#     if filter_conditions:
#         params['filter_by'] = ' && '.join(filter_conditions)

#     return params


# def build_filter_string_without_data_type(
#     profile: Dict,
#     signals: Dict = None
# ) -> str:
#     """
#     Build the location-only filter string used in Stage 1A.
#     No data_type included so facet counting stays accurate across all types.
#     black_owned is included because it is a hard filter, not a facet.
#     """
#     signals           = signals or {}
#     filter_conditions = []
#     query_mode        = signals.get('query_mode', 'explore')

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     return ' && '.join(filter_conditions) if filter_conditions else ''


# # ============================================================
# # END OF PART 3
# # ============================================================

# # ============================================================
# # PART 4 OF 8 — SCORING FUNCTIONS (ALL SYNCHRONOUS — CPU ONLY)
# # ============================================================

# def _resolve_blend(
#     query_mode: str,
#     signals: Dict,
#     candidates: List[Dict]
# ) -> Dict:
#     """
#     Build the final blend ratios for this query at runtime.
#     """
#     blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

#     sample             = candidates[:20]
#     has_live_authority = any(c.get('authority_score', 0) > 0 for c in sample)

#     if not has_live_authority and blend['authority'] > 0:
#         print(f"   ⚠️ Authority weight dead ({blend['authority']:.2f}) — redistributing to semantic")
#         blend['semantic'] += blend['authority']
#         blend['authority'] = 0.0

#     if signals.get('has_unknown_terms', False):
#         shift               = min(0.15, blend['text_match'])
#         blend['text_match'] -= shift
#         blend['semantic']   += shift
#         print(f"   📊 Unknown term shift: text={blend['text_match']:.2f} sem={blend['semantic']:.2f}")

#     if signals.get('has_superlative', False) and has_live_authority:
#         shift              = min(0.10, blend['semantic'])
#         blend['semantic']  -= shift
#         blend['authority'] += shift
#         print(f"   📊 Superlative shift: sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

#     if query_mode == 'answer' and signals.get('wants_single_result'):
#         blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

#     print(f"   📊 Final blend ({query_mode}): "
#           f"text={blend['text_match']:.2f} "
#           f"sem={blend['semantic']:.2f} "
#           f"auth={blend['authority']:.2f}")

#     return blend


# def _extract_authority_score(doc: Dict) -> float:
#     """
#     Return a normalized authority score [0.0 .. 1.0] appropriate
#     for this document's data type.
#     """
#     data_type = (
#         doc.get('data_type') or
#         doc.get('document_data_type') or
#         ''
#     ).lower()

#     if data_type == 'business':
#         rating  = doc.get('service_rating') or 0.0
#         reviews = doc.get('service_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'product':
#         rating  = doc.get('product_rating') or 0.0
#         reviews = doc.get('product_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'recipe':
#         rating  = doc.get('recipe_rating') or 0.0
#         reviews = doc.get('recipe_review_count') or 0
#         if rating > 0 and reviews > 0:
#             confidence = min(
#                 math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_RECIPE),
#                 1.0
#             )
#             return min((rating / 5.0) * confidence, 1.0)
#         return 0.0

#     if data_type == 'media':
#         return min((doc.get('media_rating') or 0.0) / 5.0, 1.0)

#     raw = doc.get('authority_score') or 0.0
#     if raw > 0:
#         return min(raw / 100.0, 1.0)

#     depth     = doc.get('factual_density_score') or 0
#     evergreen = doc.get('evergreen_score') or 0
#     if depth > 0 or evergreen > 0:
#         return min((depth + evergreen) / 200.0, 0.5)

#     return 0.0


# def _compute_text_score(
#     keyword_rank: int,
#     pool_size: int,
#     item: Dict,
#     profile: Dict
# ) -> float:
#     """Positional score from the item's rank in Stage 1A keyword results."""
#     base = 1.0 - (keyword_rank / max(pool_size, 1))

#     doc_kws = set(k.lower() for k in (item.get('primary_keywords') or []))
#     wd_kws  = set(
#         k.get('phrase', '').lower()
#         for k in profile.get('keywords', [])
#         if k.get('phrase')
#     )
#     overlap = doc_kws & wd_kws
#     bonus   = min(len(overlap) * 0.05, 0.15)

#     return min(base + bonus, 1.0)


# def _compute_semantic_score(vector_distance: float) -> float:
#     """Convert vector distance to a score with a hard gate at 0.65."""
#     if vector_distance is None or vector_distance >= SEMANTIC_DISTANCE_GATE:
#         return 0.0
#     return 1.0 - (vector_distance / SEMANTIC_DISTANCE_GATE)


# def _domain_relevance(doc: Dict, signals: Dict) -> float:
#     """Return a multiplier based on domain alignment."""
#     primary_domain = signals.get('primary_domain')
#     if not primary_domain:
#         return 1.0

#     doc_category  = (
#         doc.get('document_category') or
#         doc.get('category') or
#         ''
#     ).lower()

#     doc_schema = (
#         doc.get('document_schema') or
#         doc.get('schema') or
#         ''
#     ).lower()

#     service_types = [
#         s.lower() for s in (doc.get('service_type') or [])
#         if s
#     ]

#     aligned_categories = DOMAIN_CATEGORY_MAP.get(primary_domain, set())

#     if not aligned_categories:
#         return 1.0

#     if doc_category in aligned_categories:
#         return 1.15

#     DOMAIN_SERVICE_MAP = {
#         'food':        {
#             'restaurant', 'cafe', 'bakery', 'catering', 'food',
#             'dining', 'eatery', 'diner', 'buffet', 'bar', 'brewery',
#             'winery', 'food truck', 'coffee',
#         },
#         'beauty':      {
#             'salon', 'barbershop', 'spa', 'nail salon', 'hair salon',
#             'nail tech', 'esthetician', 'lash studio', 'brow bar',
#         },
#         'health':      {
#             'clinic', 'doctor', 'dentist', 'gym', 'fitness',
#             'pharmacy', 'urgent care', 'therapist', 'chiropractor',
#             'optometrist', 'mental health',
#         },
#         'education':   {
#             'school', 'tutoring', 'daycare', 'academy',
#             'preschool', 'childcare', 'learning center',
#         },
#         'real_estate': {
#             'realty', 'realtor', 'property management',
#             'real estate', 'mortgage', 'home inspection',
#         },
#         'technology':  {
#             'software', 'it services', 'tech support',
#             'web design', 'app development',
#         },
#         'business':    {
#             'consulting', 'accounting', 'legal', 'staffing',
#             'financial', 'insurance', 'marketing', 'advertising',
#         },
#         'culture':     {
#             'museum', 'gallery', 'cultural center', 'community center',
#             'church', 'nonprofit',
#         },
#         'music':       {
#             'studio', 'recording studio', 'music venue', 'club',
#             'lounge', 'concert venue',
#         },
#         'fashion':     {
#             'boutique', 'clothing store', 'tailor', 'alterations',
#             'fashion', 'apparel',
#         },
#         'sports':      {
#             'gym', 'fitness center', 'sports facility', 'yoga',
#             'martial arts', 'dance studio',
#         },
#     }

#     aligned_services = DOMAIN_SERVICE_MAP.get(primary_domain, set())
#     if aligned_services and any(s in aligned_services for s in service_types):
#         return 1.15

#     if primary_domain in doc_schema or primary_domain in doc_category:
#         return 1.10

#     return 0.75


# def _content_intent_match(doc: Dict, query_mode: str) -> float:
#     """Return a multiplier based on content_intent alignment."""
#     doc_intent = (doc.get('content_intent') or '').lower()
#     if not doc_intent:
#         return 1.0

#     preferred = INTENT_CONTENT_MAP.get(query_mode, set())
#     if not preferred:
#         return 1.0

#     return 1.10 if doc_intent in preferred else 0.85


# def _pool_type_multiplier(doc: Dict, query_mode: str) -> float:
#     """Return a multiplier based on data type appropriateness."""
#     data_type = (
#         doc.get('data_type') or
#         doc.get('document_data_type') or
#         ''
#     ).lower()

#     scope         = POOL_SCOPE.get(query_mode, {})
#     allowed_types = scope.get('allow', set())

#     if not allowed_types:
#         return 1.0

#     return 1.0 if data_type in allowed_types else 0.5


# def _score_document(
#     idx: int,
#     item: Dict,
#     profile: Dict,
#     signals: Dict,
#     blend: Dict,
#     pool_size: int,
#     vector_data: Dict
# ) -> float:
#     """Compute the final blended score for one document."""
#     query_mode = signals.get('query_mode', 'explore')
#     item_id    = item.get('id', '')

#     vd              = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
#     vector_distance = vd.get('vector_distance', 1.0)
#     semantic_rank   = vd.get('semantic_rank', 999999)

#     item['vector_distance'] = vector_distance
#     item['semantic_rank']   = semantic_rank

#     text_score = _compute_text_score(idx, pool_size, item, profile)
#     sem_score  = _compute_semantic_score(vector_distance)
#     auth_score = _extract_authority_score(item)

#     blended = (
#         blend['text_match'] * text_score +
#         blend['semantic']   * sem_score  +
#         blend['authority']  * auth_score
#     )

#     blended *= _domain_relevance(item, signals)
#     blended *= _content_intent_match(item, query_mode)
#     blended *= _pool_type_multiplier(item, query_mode)

#     if item.get('data_type') in profile.get('preferred_data_types', []):
#         blended = min(blended + PREFERRED_TYPE_BOOST, 1.0)

#     if signals.get('has_black_owned') and item.get('black_owned') is True:
#         blended = min(blended + BLACK_OWNED_BOOST, 1.0)

#     if signals.get('has_superlative') and auth_score == 0.0:
#         blended = min(blended, SUPERLATIVE_SCORE_CAP)

#     item['blended_score'] = blended
#     item['text_score']    = round(text_score, 4)
#     item['sem_score']     = round(sem_score, 4)
#     item['auth_score']    = round(auth_score, 4)

#     return blended


# # ============================================================
# # END OF PART 4
# # ============================================================

# # ============================================================
# # PART 5 OF 8 — CANDIDATE FETCHING (STAGES 1A, 1B, COMBINED)
# # ============================================================

# _MATCH_STOPWORDS = frozenset({
#     'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
#     'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
#     'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
#     'would', 'could', 'should', 'may', 'might', 'shall', 'can', 'need',
#     'when', 'where', 'who', 'what', 'which', 'why', 'how', 'that', 'this',
#     'these', 'those', 'it', 'its', 'he', 'she', 'they', 'we', 'you', 'i',
#     'his', 'her', 'their', 'our', 'your', 'my', 'about', 'into', 'through',
#     'during', 'before', 'after', 'above', 'below', 'between', 'each',
#     'than', 'so', 'if', 'not', 'no', 'nor', 'yet', 'both', 'either',
#     'just', 'also', 'then', 'than', 'such', 'more', 'most', 'other',
#     'born', 'died', 'first', 'last', 'new', 'old', 'many', 'much',
#     'long', 'little', 'own', 'right', 'big', 'high', 'great', 'small',
# })


# def _normalize_signal(text: str) -> set:
#     """Normalize a signal string into a set of meaningful tokens."""
#     if not text:
#         return set()
#     text = text.lower()
#     text = re.sub(r"[^\w\s-]", " ", text)
#     text = re.sub(r"\s*-\s*", " ", text)
#     return {t for t in text.split() if len(t) > 2 and t not in _MATCH_STOPWORDS}


# def _extract_query_signals(
#     profile: Dict,
#     discovery: Dict = None
# ) -> Tuple[set, list, Optional[set]]:
#     """Extract and normalize all meaningful query signals from the v3 profile."""
#     raw_signals    = []
#     ranked_signals = []

#     for p in profile.get('persons', []):
#         phrase = p.get('phrase') or p.get('word', '')
#         rank   = p.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for o in profile.get('organizations', []):
#         phrase = o.get('phrase') or o.get('word', '')
#         rank   = o.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for k in profile.get('keywords', []):
#         phrase = k.get('phrase') or k.get('word', '')
#         rank   = k.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     for term in profile.get('search_terms', []):
#         if term:
#             raw_signals.append(term)

#     if discovery:
#         for corr in discovery.get('corrections', []):
#             if corr.get('correction_type') == 'suggestion' and corr.get('corrected'):
#                 corrected = corr['corrected']
#                 if corrected not in raw_signals:
#                     raw_signals.append(corrected)
#                     ranked_signals.append((100, corrected))

#         for term in discovery.get('terms', []):
#             if term.get('status') == 'unknown_suggest' and term.get('suggestion'):
#                 suggestion = term['suggestion']
#                 if suggestion not in raw_signals:
#                     raw_signals.append(suggestion)
#                     ranked_signals.append((100, suggestion))

#     all_tokens   = set()
#     full_phrases = []

#     for phrase in raw_signals:
#         all_tokens.update(_normalize_signal(phrase))
#         phrase_lower = phrase.lower().strip()
#         if phrase_lower:
#             full_phrases.append(phrase_lower)

#     primary_subject = None
#     if ranked_signals:
#         ranked_signals.sort(key=lambda x: -x[0])
#         primary_subject = _normalize_signal(ranked_signals[0][1])

#     return all_tokens, full_phrases, primary_subject


# def _validate_question_hit(
#     hit_doc: Dict,
#     query_tokens: set,
#     query_phrases: list,
#     primary_subject: Optional[set],
#     min_matches: int = 1,
# ) -> bool:
#     """Validate a question hit against query signals using 4-level matching."""
#     if not query_tokens:
#         return True

#     candidate_raw = (
#         hit_doc.get('primary_keywords', []) +
#         hit_doc.get('entities', []) +
#         hit_doc.get('semantic_keywords', [])
#     )

#     if not candidate_raw:
#         return False

#     candidate_tokens  = set()
#     candidate_phrases = []

#     for val in candidate_raw:
#         if not val:
#             continue
#         candidate_tokens.update(_normalize_signal(val))
#         candidate_phrases.append(val.lower().strip())

#     candidate_text = ' '.join(candidate_phrases)

#     match_count         = 0
#     primary_subject_hit = False

#     exact_matches = query_tokens & candidate_tokens
#     if exact_matches:
#         match_count += len(exact_matches)
#         if primary_subject and (primary_subject & exact_matches):
#             primary_subject_hit = True

#     for qt in query_tokens:
#         if qt in exact_matches:
#             continue
#         for ct in candidate_tokens:
#             if qt in ct or ct in qt:
#                 match_count += 1
#                 if primary_subject and qt in primary_subject:
#                     primary_subject_hit = True
#                 break

#     for qp in query_phrases:
#         if len(qp) < 3:
#             continue
#         if qp in candidate_text:
#             match_count += 1
#             if primary_subject and _normalize_signal(qp) & primary_subject:
#                 primary_subject_hit = True
#         else:
#             for cp in candidate_phrases:
#                 if qp in cp or cp in qp:
#                     match_count += 1
#                     if primary_subject and _normalize_signal(qp) & primary_subject:
#                         primary_subject_hit = True
#                     break

#     remaining_query = query_tokens - exact_matches
#     token_overlap   = remaining_query & candidate_tokens
#     if token_overlap:
#         match_count += len(token_overlap)
#         if primary_subject and (primary_subject & token_overlap):
#             primary_subject_hit = True

#     if match_count < min_matches:
#         return False

#     if primary_subject and len(query_tokens) >= 3:
#         if not primary_subject_hit:
#             return False

#     return True


# async def fetch_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = 100
# ) -> List[str]:
#     """
#     Stage 1A — keyword search against the document collection.
#     Returns up to 100 document_uuid strings with no metadata.
#     """
#     signals    = signals or {}
#     params     = build_typesense_params(profile, signals=signals)
#     filter_str = build_filter_string_without_data_type(profile, signals=signals)
#     query_mode = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1A (document): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode} | Max: {max_results}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     search_params = {
#         'q':                     params.get('q', search_query),
#         'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#         'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
#         'per_page':              max_results,
#         'page':                  1,
#         'include_fields':        'document_uuid',
#         'num_typos':             params.get('num_typos', 0),
#         'prefix':                params.get('prefix', 'no'),
#         'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#         'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
#     }

#     if filter_str:
#         search_params['filter_by'] = filter_str

#     try:
#         response = await asyncio.to_thread(
#             client.collections[COLLECTION_NAME].documents.search,
#             search_params
#         )
#         hits     = response.get('hits', [])
#         uuids    = [
#             hit['document']['document_uuid']
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         ]
#         print(f"📊 Stage 1A: {len(uuids)} candidate UUIDs")
#         return uuids
#     except Exception as e:
#         print(f"❌ Stage 1A error: {e}")
#         return []


# async def fetch_candidate_uuids_from_questions(
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     max_results: int = 50,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Stage 1B — vector search against the questions collection.
#     """
#     signals = signals or {}

#     if not query_embedding:
#         print("⚠️ Stage 1B (questions): no embedding — skipping")
#         return []

#     query_tokens, query_phrases, primary_subject = _extract_query_signals(
#         profile, discovery=discovery
#     )

#     print(f"🔍 Stage 1B validation signals:")
#     print(f"   query_tokens    : {sorted(query_tokens)}")
#     print(f"   query_phrases   : {query_phrases}")
#     print(f"   primary_subject : {primary_subject}")

#     # ── Step A: Build facet filter ────────────────────────────────────────
#     filter_parts = []

#     primary_kws = [
#         k.get('phrase') or k.get('word', '')
#         for k in profile.get('keywords', [])
#     ]
#     primary_kws = [kw for kw in primary_kws if kw][:3]
#     if primary_kws:
#         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
#         filter_parts.append(f'primary_keywords:[{kw_values}]')

#     entity_names = []
#     for p in profile.get('persons', []):
#         name = p.get('phrase') or p.get('word', '')
#         rank = p.get('rank', 0)
#         if name and (' ' in name or rank > 100):
#             entity_names.append(name)
#     for o in profile.get('organizations', []):
#         name = o.get('phrase') or o.get('word', '')
#         rank = o.get('rank', 0)
#         if name and (' ' in name or rank > 100):
#             entity_names.append(name)
#     entity_names = [e for e in entity_names if e][:3]
#     if entity_names:
#         ent_values = ','.join([f'`{e}`' for e in entity_names])
#         filter_parts.append(f'entities:[{ent_values}]')

#     question_word     = signals.get('question_word') or ''
#     question_type_map = {
#         'when':  'TEMPORAL',
#         'where': 'LOCATION',
#         'who':   'PERSON',
#         'what':  'FACTUAL',
#         'which': 'FACTUAL',
#         'why':   'REASON',
#         'how':   'PROCESS',
#     }
#     question_type = question_type_map.get(question_word.lower(), '')
#     if question_type:
#         filter_parts.append(f'question_type:={question_type}')

#     # ── Location filter ───────────────────────────────────────────────────
#     location_filter_parts = []
#     query_mode            = signals.get('query_mode', 'explore')
#     is_location_subject   = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:=`{c['name']}`" for c in cities]
#             location_filter_parts.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:=`{variant}`"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             location_filter_parts.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     facet_filter    = ' || '.join(filter_parts) if filter_parts else ''
#     location_filter = ' && '.join(location_filter_parts) if location_filter_parts else ''

#     if facet_filter and location_filter:
#         filter_str = f'({facet_filter}) && {location_filter}'
#     elif location_filter:
#         filter_str = location_filter
#     else:
#         filter_str = facet_filter

#     print(f"   filter_by : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

#     # ── Step B: Vector search ─────────────────────────────────────────────
#     embedding_str = ','.join(str(x) for x in query_embedding)

#     search_params = {
#         'q':              '*',
#         'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#         'per_page':       max_results * 2,
#         'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#     }

#     if filter_str:
#         search_params['filter_by'] = filter_str

#     try:
#         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
#         response        = await asyncio.to_thread(
#             client.multi_search.perform,
#             search_requests, {}
#         )
#         result          = response['results'][0]
#         hits            = result.get('hits', [])

#         # Fallback if too few hits with location filter
#         if len(hits) < 5 and filter_str:
#             fallback_filter = facet_filter if facet_filter else ''
#             print(f"⚠️ Stage 1B: only {len(hits)} hits with location filter — "
#                   f"retrying with facet filter only")

#             sp_fallback = {**search_params}
#             if fallback_filter:
#                 sp_fallback['filter_by'] = fallback_filter
#             else:
#                 sp_fallback.pop('filter_by', None)

#             r_fallback    = await asyncio.to_thread(
#                 client.multi_search.perform,
#                 {'searches': [{'collection': 'questions', **sp_fallback}]}, {}
#             )
#             fallback_hits = r_fallback['results'][0].get('hits', [])
#             print(f"   Fallback returned {len(fallback_hits)} hits")

#             if len(fallback_hits) < 5:
#                 print(f"⚠️ Stage 1B: retrying with no filter")
#                 sp_nofilter = {k: v for k, v in search_params.items() if k != 'filter_by'}
#                 r_nofilter  = await asyncio.to_thread(
#                     client.multi_search.perform,
#                     {'searches': [{'collection': 'questions', **sp_nofilter}]}, {}
#                 )
#                 hits = r_nofilter['results'][0].get('hits', [])
#                 print(f"   No-filter fallback returned {len(hits)} hits")
#             else:
#                 hits = fallback_hits

#         # ── Step C: Hard distance gate ────────────────────────────────────
#         uuids    = []
#         seen     = set()
#         accepted = 0
#         rejected = 0

#         for hit in hits:
#             doc           = hit.get('document', {})
#             uuid          = doc.get('document_uuid')
#             hit_distance  = hit.get('vector_distance', 1.0)

#             if not uuid:
#                 continue

#             if hit_distance >= QUESTION_SEMANTIC_DISTANCE_GATE:
#                 rejected += 1
#                 print(f"   🚫 Distance gate: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit_distance:.4f} >= {QUESTION_SEMANTIC_DISTANCE_GATE})")
#                 continue

#             is_valid = _validate_question_hit(
#                 hit_doc         = doc,
#                 query_tokens    = query_tokens,
#                 query_phrases   = query_phrases,
#                 primary_subject = primary_subject,
#                 min_matches     = 1,
#             )

#             if is_valid:
#                 accepted += 1
#                 if uuid not in seen:
#                     seen.add(uuid)
#                     uuids.append(uuid)
#             else:
#                 rejected += 1
#                 print(f"   ❌ Validation failed: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit_distance:.4f})")

#             if len(uuids) >= max_results:
#                 break

#         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
#               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1B error: {e}")
#         return []


# async def fetch_all_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Run Stage 1A (document) and Stage 1B (questions) in parallel.
#     Merge order: overlap → document-only → question-only.
#     """
#     signals = signals or {}

#     doc_uuids, q_uuids = await asyncio.gather(
#         fetch_candidate_uuids(search_query, profile, signals, 100),
#         fetch_candidate_uuids_from_questions(
#             profile, query_embedding, signals, 50, discovery
#         ),
#     )

#     doc_set = set(doc_uuids)
#     q_set   = set(q_uuids)
#     overlap  = doc_set & q_set

#     merged = []
#     seen   = set()

#     for uuid in doc_uuids:
#         if uuid in overlap and uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     for uuid in doc_uuids:
#         if uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     for uuid in q_uuids:
#         if uuid not in seen:
#             merged.append(uuid)
#             seen.add(uuid)

#     print(f"📊 Stage 1 COMBINED:")
#     print(f"   document pool  : {len(doc_uuids)}")
#     print(f"   questions pool : {len(q_uuids)}")
#     print(f"   overlap        : {len(overlap)}")
#     print(f"   merged total   : {len(merged)}")

#     return merged


# # ============================================================
# # END OF PART 5
# # ============================================================


# # ============================================================
# # PART 6 OF 8 — METADATA FETCHING, RERANKING, COUNTING,
# #               FILTERING, PAGINATION
# # ============================================================

# async def semantic_rerank_candidates(
#     candidate_ids: List[str],
#     query_embedding: List[float],
#     max_to_rerank: int = 250
# ) -> List[Dict]:
#     """Stage 2 — pure vector ranking of the candidate pool."""
#     if not candidate_ids or not query_embedding:
#         return [
#             {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#             for i, cid in enumerate(candidate_ids)
#         ]

#     ids_to_rerank = candidate_ids[:max_to_rerank]
#     id_filter     = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
#     embedding_str = ','.join(str(x) for x in query_embedding)

#     params = {
#         'q':              '*',
#         'vector_query':   f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
#         'filter_by':      f'document_uuid:[{id_filter}]',
#         'per_page':       len(ids_to_rerank),
#         'include_fields': 'document_uuid',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = await asyncio.to_thread(
#             client.multi_search.perform,
#             search_requests, {}
#         )
#         hits            = response['results'][0].get('hits', [])

#         reranked = [
#             {
#                 'id':              hit['document'].get('document_uuid'),
#                 'vector_distance': hit.get('vector_distance', 1.0),
#                 'semantic_rank':   i,
#             }
#             for i, hit in enumerate(hits)
#         ]

#         reranked_ids = {r['id'] for r in reranked}
#         for cid in ids_to_rerank:
#             if cid not in reranked_ids:
#                 reranked.append({
#                     'id':              cid,
#                     'vector_distance': 1.0,
#                     'semantic_rank':   len(reranked),
#                 })

#         print(f"🎯 Stage 2: reranked {len(reranked)} candidates")
#         return reranked

#     except Exception as e:
#         print(f"⚠️ Stage 2 error: {e}")
#         return [
#             {'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#             for i, cid in enumerate(ids_to_rerank)
#         ]


# async def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
#     """Stage 4 — fetch lightweight metadata for survivors. Batches in groups of 100."""
#     if not survivor_ids:
#         return []

#     BATCH_SIZE = 100
#     doc_map    = {}

#     # Build all batch coroutines
#     async def _fetch_batch(batch_ids, batch_index):
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])
#         params = {
#             'q':         '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page':  len(batch_ids),
#             'include_fields': ','.join([
#                 'document_uuid', 'document_data_type', 'document_category',
#                 'document_schema', 'document_title', 'content_intent',
#                 'authority_score', 'service_rating', 'service_review_count',
#                 'service_type', 'product_rating', 'product_review_count',
#                 'recipe_rating', 'recipe_review_count', 'media_rating',
#                 'factual_density_score', 'evergreen_score',
#                 'primary_keywords', 'black_owned', 'image_url', 'logo_url',
#             ]),
#         }
#         try:
#             search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#             response = await asyncio.to_thread(
#                 client.multi_search.perform,
#                 search_requests, {}
#             )
#             hits = response['results'][0].get('hits', [])
#             batch_results = {}
#             for hit in hits:
#                 doc  = hit.get('document', {})
#                 uuid = doc.get('document_uuid')
#                 if uuid:
#                     batch_results[uuid] = {
#                         'id':                   uuid,
#                         'data_type':            doc.get('document_data_type', ''),
#                         'category':             doc.get('document_category', ''),
#                         'schema':               doc.get('document_schema', ''),
#                         'title':                doc.get('document_title', ''),
#                         'content_intent':       doc.get('content_intent', ''),
#                         'authority_score':      doc.get('authority_score', 0),
#                         'service_rating':       doc.get('service_rating', 0),
#                         'service_review_count': doc.get('service_review_count', 0),
#                         'service_type':         doc.get('service_type', []),
#                         'product_rating':       doc.get('product_rating', 0),
#                         'product_review_count': doc.get('product_review_count', 0),
#                         'recipe_rating':        doc.get('recipe_rating', 0),
#                         'recipe_review_count':  doc.get('recipe_review_count', 0),
#                         'media_rating':         doc.get('media_rating', 0),
#                         'factual_density_score': doc.get('factual_density_score', 0),
#                         'evergreen_score':      doc.get('evergreen_score', 0),
#                         'primary_keywords':     doc.get('primary_keywords', []),
#                         'black_owned':          doc.get('black_owned', False),
#                         'image_url':            doc.get('image_url', []),
#                         'logo_url':             doc.get('logo_url', []),
#                     }
#             return batch_results
#         except Exception as e:
#             print(f"❌ Stage 4 metadata fetch error (batch {batch_index}): {e}")
#             return {}

#     # Run all batches concurrently
#     batches = []
#     for i in range(0, len(survivor_ids), BATCH_SIZE):
#         batch_ids = survivor_ids[i:i + BATCH_SIZE]
#         batches.append(_fetch_batch(batch_ids, i))

#     batch_results = await asyncio.gather(*batches)

#     for batch_map in batch_results:
#         doc_map.update(batch_map)

#     results = [doc_map[uuid] for uuid in survivor_ids if uuid in doc_map]
#     print(f"📊 Stage 4: fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
#     return results


# async def fetch_candidates_with_metadata(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[Dict]:
#     """
#     Keyword path only — fetch UUIDs and lightweight metadata together.
#     """
#     signals    = signals or {}
#     params     = build_typesense_params(profile, signals=signals)

#     filter_str = params.get(
#         'filter_by',
#         build_filter_string_without_data_type(profile, signals=signals)
#     )

#     PAGE_SIZE    = 100
#     all_results  = []
#     current_page = 1
#     max_pages    = (max_results // PAGE_SIZE) + 1
#     query_mode   = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1 (keyword+metadata): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     while len(all_results) < max_results and current_page <= max_pages:
#         search_params = {
#             'q':                     params.get('q', search_query),
#             'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#             'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
#             'per_page':              PAGE_SIZE,
#             'page':                  current_page,
#             'include_fields':        ','.join([
#                 'document_uuid', 'document_data_type', 'document_category',
#                 'document_schema', 'document_title', 'content_intent',
#                 'authority_score', 'service_rating', 'service_review_count',
#                 'service_type', 'product_rating', 'product_review_count',
#                 'recipe_rating', 'recipe_review_count', 'media_rating',
#                 'factual_density_score', 'evergreen_score',
#                 'primary_keywords', 'black_owned', 'image_url', 'logo_url',
#             ]),
#             'num_typos':             params.get('num_typos', 0),
#             'prefix':                params.get('prefix', 'no'),
#             'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#             'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
#         }

#         if filter_str:
#             search_params['filter_by'] = filter_str

#         try:
#             response = await asyncio.to_thread(
#                 client.collections[COLLECTION_NAME].documents.search,
#                 search_params
#             )
#             hits     = response.get('hits', [])
#             found    = response.get('found', 0)

#             if not hits:
#                 break

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 all_results.append({
#                     'id':                   doc.get('document_uuid'),
#                     'data_type':            doc.get('document_data_type', ''),
#                     'category':             doc.get('document_category', ''),
#                     'schema':               doc.get('document_schema', ''),
#                     'title':                doc.get('document_title', ''),
#                     'content_intent':       doc.get('content_intent', ''),
#                     'authority_score':      doc.get('authority_score', 0),
#                     'service_rating':       doc.get('service_rating', 0),
#                     'service_review_count': doc.get('service_review_count', 0),
#                     'service_type':         doc.get('service_type', []),
#                     'product_rating':       doc.get('product_rating', 0),
#                     'product_review_count': doc.get('product_review_count', 0),
#                     'recipe_rating':        doc.get('recipe_rating', 0),
#                     'recipe_review_count':  doc.get('recipe_review_count', 0),
#                     'media_rating':         doc.get('media_rating', 0),
#                     'factual_density_score': doc.get('factual_density_score', 0),
#                     'evergreen_score':      doc.get('evergreen_score', 0),
#                     'primary_keywords':     doc.get('primary_keywords', []),
#                     'black_owned':          doc.get('black_owned', False),
#                     'image_url':            doc.get('image_url', []),
#                     'logo_url':             doc.get('logo_url', []),
#                     'text_match':           hit.get('text_match', 0),
#                 })

#             if len(all_results) >= found or len(hits) < PAGE_SIZE:
#                 break

#             current_page += 1

#         except Exception as e:
#             print(f"❌ Keyword fetch error (page {current_page}): {e}")
#             break

#     print(f"📊 Keyword path: {len(all_results)} candidates with metadata")
#     return all_results[:max_results]


# def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
#     """Single pass counting by data_type, category, schema."""
#     data_type_counts = {}
#     category_counts  = {}
#     schema_counts    = {}

#     for item in cached_results:
#         dt  = item.get('data_type', '')
#         cat = item.get('category', '')
#         sch = item.get('schema', '')
#         if dt:
#             data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
#         if cat:
#             category_counts[cat] = category_counts.get(cat, 0) + 1
#         if sch:
#             schema_counts[sch]   = schema_counts.get(sch, 0) + 1

#     return {
#         'data_type': [
#             {'value': dt, 'count': c, 'label': DATA_TYPE_LABELS.get(dt, dt.title())}
#             for dt, c in sorted(data_type_counts.items(), key=lambda x: -x[1])
#         ],
#         'category': [
#             {'value': cat, 'count': c, 'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title())}
#             for cat, c in sorted(category_counts.items(), key=lambda x: -x[1])
#         ],
#         'schema': [
#             {'value': sch, 'count': c, 'label': sch}
#             for sch, c in sorted(schema_counts.items(), key=lambda x: -x[1])
#         ],
#     }


# def count_all(candidates: List[Dict]) -> Dict:
#     """Stage 5 — single counting pass after all pruning and scoring is done."""
#     facets      = count_facets_from_cache(candidates)
#     image_count = _count_images_from_candidates(candidates)
#     total       = len(candidates)

#     print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
#           f"data_types={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

#     return {
#         'facets':            facets,
#         'facet_total':       total,
#         'total_image_count': image_count,
#     }


# def filter_cached_results(
#     cached_results: List[Dict],
#     data_type: str = None,
#     category: str  = None,
#     schema: str    = None
# ) -> List[Dict]:
#     """Filter the cached result set by UI-selected filters."""
#     filtered = cached_results
#     if data_type:
#         filtered = [r for r in filtered if r.get('data_type') == data_type]
#     if category:
#         filtered = [r for r in filtered if r.get('category') == category]
#     if schema:
#         filtered = [r for r in filtered if r.get('schema') == schema]
#     return filtered


# def paginate_cached_results(
#     cached_results: List[Dict],
#     page: int,
#     per_page: int
# ) -> Tuple[List[Dict], int]:
#     """Slice the filtered result set to the requested page."""
#     total = len(cached_results)
#     start = (page - 1) * per_page
#     end   = start + per_page
#     if start >= total:
#         return [], total
#     return cached_results[start:end], total


# # ============================================================
# # END OF PART 6
# # ============================================================

# # ============================================================
# # PART 7 OF 8 — DOCUMENT FETCHING, FORMATTING, AI OVERVIEW
# # ============================================================

# async def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
#     """Fetch complete document records from Typesense for the current page only."""
#     if not document_ids:
#         return []

#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])

#     params = {
#         'q':              '*',
#         'filter_by':      f'document_uuid:[{id_filter}]',
#         'per_page':       len(document_ids),
#         'exclude_fields': 'embedding',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = await asyncio.to_thread(
#             client.multi_search.perform,
#             search_requests, {}
#         )
#         hits            = response['results'][0].get('hits', [])

#         doc_map = {
#             hit['document']['document_uuid']: format_result(hit, query)
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         }

#         return [doc_map[doc_id] for doc_id in document_ids if doc_id in doc_map]

#     except Exception as e:
#         print(f"❌ fetch_full_documents error: {e}")
#         return []


# async def fetch_documents_by_semantic_uuid(
#     semantic_uuid: str,
#     exclude_uuid: str = None,
#     limit: int = 5
# ) -> List[Dict]:
#     """Fetch documents that share the same semantic group."""
#     if not semantic_uuid:
#         return []

#     filter_str = f'semantic_uuid:={semantic_uuid}'
#     if exclude_uuid:
#         filter_str += f' && document_uuid:!={exclude_uuid}'

#     params = {
#         'q':              '*',
#         'filter_by':      filter_str,
#         'per_page':       limit,
#         'include_fields': 'document_uuid,document_title,document_url',
#         'sort_by':        'authority_score:desc',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response        = await asyncio.to_thread(
#             client.multi_search.perform,
#             search_requests, {}
#         )
#         hits            = response['results'][0].get('hits', [])

#         related = [
#             {
#                 'title': hit['document'].get('document_title', ''),
#                 'url':   hit['document'].get('document_url', ''),
#                 'id':    hit['document'].get('document_uuid', ''),
#             }
#             for hit in hits
#             if hit.get('document', {}).get('document_uuid')
#         ]

#         print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
#         return related

#     except Exception as e:
#         print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
#         return []


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transform a raw Typesense hit into the response format."""
#     doc        = hit.get('document', {})
#     highlights = hit.get('highlights', [])

#     highlight_map = {
#         h.get('field'): (
#             h.get('value') or
#             h.get('snippet') or
#             (h.get('snippets') or [''])[0]
#         )
#         for h in highlights
#     }

#     vector_distance = hit.get('vector_distance')
#     semantic_score  = round(1 - vector_distance, 3) if vector_distance is not None else None

#     raw_date       = doc.get('published_date_string', '')
#     formatted_date = ''
#     if raw_date:
#         try:
#             if 'T' in raw_date:
#                 dt = datetime.strptime(raw_date.split('T')[0], '%Y-%m-%d')
#             elif '-' in raw_date and len(raw_date) >= 10:
#                 dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
#             else:
#                 dt = None
#             formatted_date = dt.strftime('%b %d, %Y') if dt else raw_date
#         except Exception:
#             formatted_date = raw_date

#     geopoint = (
#         doc.get('location_geopoint') or
#         doc.get('location_coordinates') or
#         [None, None]
#     )

#     return {
#         'id':                    doc.get('document_uuid'),
#         'title':                 doc.get('document_title', 'Untitled'),
#         'image_url':             doc.get('image_url') or [],
#         'logo_url':              doc.get('logo_url') or [],
#         'title_highlighted':     highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary':               doc.get('document_summary', ''),
#         'summary_highlighted':   highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url':                   doc.get('document_url', ''),
#         'source':                doc.get('document_brand', 'unknown'),
#         'site_name':             doc.get('document_brand', 'Website'),
#         'image':                 (doc.get('image_url') or [None])[0],
#         'category':              doc.get('document_category', ''),
#         'data_type':             doc.get('document_data_type', ''),
#         'schema':                doc.get('document_schema', ''),
#         'date':                  formatted_date,
#         'published_date':        formatted_date,
#         'authority_score':       doc.get('authority_score', 0),
#         'cluster_uuid':          doc.get('cluster_uuid'),
#         'semantic_uuid':         doc.get('semantic_uuid'),
#         'key_facts':             doc.get('key_facts', []),
#         'humanized_summary':     '',
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score':        semantic_score,
#         'black_owned':           doc.get('black_owned', False),
#         'service_rating':        doc.get('service_rating'),
#         'service_review_count':  doc.get('service_review_count'),
#         'service_type':          doc.get('service_type', []),
#         'service_specialties':   doc.get('service_specialties', []),
#         'service_price_range':   doc.get('service_price_range'),
#         'service_phone':         doc.get('service_phone'),
#         'service_hours':         doc.get('service_hours'),
#         'location': {
#             'city':    doc.get('location_city'),
#             'state':   doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region':  doc.get('location_region'),
#             'address': doc.get('location_address'),
#             'geopoint': geopoint,
#             'lat':     geopoint[0] if geopoint else None,
#             'lng':     geopoint[1] if geopoint else None,
#         },
#         'time_period': {
#             'start':   doc.get('time_period_start'),
#             'end':     doc.get('time_period_end'),
#             'context': doc.get('time_context'),
#         },
#         'score':           0.5,
#         'related_sources': [],
#     }


# # ============================================================
# # AI OVERVIEW
# # ============================================================

# def humanize_key_facts(
#     key_facts: list,
#     query: str = '',
#     matched_keyword: str = '',
#     question_word: str = None
# ) -> str:
#     """Format key_facts into a readable AfroToDo AI Overview string."""
#     if not key_facts:
#         return ''

#     facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
#     if not facts:
#         return ''

#     if question_word:
#         qw = question_word.lower()

#         if qw == 'where':
#             geo_words = {
#                 'located', 'bounded', 'continent', 'region', 'coast',
#                 'ocean', 'border', 'north', 'south', 'east', 'west',
#                 'latitude', 'longitude', 'hemisphere', 'capital',
#                 'city', 'state', 'country', 'area', 'lies', 'situated',
#             }
#             relevant_facts = [f for f in facts if any(gw in f.lower() for gw in geo_words)]

#         elif qw == 'when':
#             import re as _re
#             temporal_words = {
#                 'founded', 'established', 'born', 'created', 'started',
#                 'opened', 'built', 'year', 'date', 'century', 'decade',
#                 'era', 'period',
#             }
#             relevant_facts = [
#                 f for f in facts
#                 if any(tw in f.lower() for tw in temporal_words)
#                 or _re.search(r'\b\d{4}\b', f)
#             ]

#         elif qw == 'who':
#             who_words = {
#                 'first', 'president', 'founder', 'ceo', 'leader',
#                 'director', 'known', 'famous', 'awarded', 'pioneer',
#                 'invented', 'created', 'named', 'appointed', 'elected',
#             }
#             relevant_facts = [f for f in facts if any(ww in f.lower() for ww in who_words)]

#         elif qw == 'what':
#             what_words = {
#                 'is a', 'refers to', 'defined', 'known as',
#                 'type of', 'form of', 'means', 'represents',
#             }
#             relevant_facts = [f for f in facts if any(ww in f.lower() for ww in what_words)]

#         else:
#             relevant_facts = []

#         if not relevant_facts and matched_keyword:
#             keyword_lower  = matched_keyword.lower()
#             relevant_facts = [f for f in facts if keyword_lower in f.lower()]

#         if not relevant_facts:
#             relevant_facts = [facts[0]]

#     elif matched_keyword:
#         keyword_lower  = matched_keyword.lower()
#         relevant_facts = [f for f in facts if keyword_lower in f.lower()]
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     else:
#         relevant_facts = [facts[0]]

#     relevant_facts = relevant_facts[:2]

#     is_question = query and any(
#         query.lower().startswith(w)
#         for w in ['who', 'what', 'where', 'when', 'why', 'how',
#                   'is', 'are', 'can', 'do', 'does']
#     )

#     if is_question:
#         intros = [
#             "Based on our sources,",
#             "According to our data,",
#             "From what we know,",
#             "Our sources indicate that",
#         ]
#     else:
#         intros = [
#             "Here's what we know:",
#             "From our sources:",
#             "Based on our data:",
#             "Our sources show that",
#         ]

#     intro = random.choice(intros)

#     if len(relevant_facts) == 1:
#         return f"{intro} {relevant_facts[0]}."
#     else:
#         return f"{intro} {relevant_facts[0]}. {relevant_facts[1]}."


# def _should_trigger_ai_overview(
#     signals: Dict,
#     results: List[Dict],
#     query: str
# ) -> bool:
#     """Decide whether to show an AI Overview for this query."""
#     if not results:
#         return False

#     query_mode = signals.get('query_mode', 'explore')

#     if query_mode in ('browse', 'local', 'shop'):
#         return False
#     if query_mode in ('answer', 'compare'):
#         return True

#     if query_mode == 'explore':
#         top_title = results[0].get('title', '').lower()
#         top_facts = ' '.join(results[0].get('key_facts', [])).lower()
#         stopwords  = {
#             'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#             'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#             'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
#         }
#         query_words = [
#             w for w in query.lower().split()
#             if w not in stopwords and len(w) > 1
#         ]
#         if not query_words:
#             return False
#         matches = sum(1 for w in query_words if w in top_title or w in top_facts)
#         return (matches / len(query_words)) >= 0.75

#     return False


# def _build_ai_overview(
#     signals: Dict,
#     results: List[Dict],
#     query: str
# ) -> Optional[str]:
#     """Build the AI Overview text using signal-driven key_fact selection."""
#     if not results or not results[0].get('key_facts'):
#         return None

#     question_word = signals.get('question_word')
#     stopwords     = {
#         'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#         'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#         'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that',
#     }
#     query_words = [
#         w for w in query.lower().split()
#         if w not in stopwords and len(w) > 1
#     ]

#     matched_keyword = ''
#     if query_words:
#         top_title       = results[0].get('title', '').lower()
#         top_facts       = ' '.join(results[0].get('key_facts', [])).lower()
#         matched_keyword = max(
#             query_words,
#             key=lambda w: (w in top_title) + (w in top_facts)
#         )

#     return humanize_key_facts(
#         results[0]['key_facts'],
#         query,
#         matched_keyword=matched_keyword,
#         question_word=question_word,
#     )


# # ============================================================
# # END OF PART 7
# # ============================================================

# # ============================================================
# # PART 8 OF 8 — MAIN ENTRY POINT + COMPATIBILITY STUBS
# # ============================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Simple intent detection used only on the keyword path."""
#     query_lower    = query.lower()
#     location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
#     if any(w in query_lower for w in location_words):
#         return 'location'
#     person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
#     if any(w in query_lower for w in person_words):
#         return 'person'
#     return 'general'


# async def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: str = 'y',
#     answer: str = None,
#     answer_type: str = None,
#     skip_embedding: bool = False,
#     document_uuid: str = None,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search. Called by views.py.
#     Now async — caller must use: result = await execute_full_search(...)

#     Runs one of four paths depending on request type:

#     QUESTION PATH
#         document_uuid + search_source='question' supplied.
#         Fetches that single document directly and returns it.

#     FAST PATH (cache hit)
#         Finished result package found in Redis.
#         Applies UI filters, paginates, fetches full docs for page.

#     KEYWORD PATH (alt_mode='n' or dropdown source)
#         Stage 1 (keyword+metadata) → Stage 5 (count) →
#         Stage 6 (cache) → Stage 7 (paginate+fetch)

#     SEMANTIC PATH (default)
#         Stage 1A+1B (uuids) → Stage 2 (rerank) → Stage 3 (prune) →
#         Stage 4 (metadata) → Stage 5 (score+count) → Stage 6 (cache) →
#         Stage 7 (paginate+fetch)
#     """
#     times = {}
#     t0    = time.time()
#     print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

#     active_data_type = filters.get('data_type') if filters else None
#     active_category  = filters.get('category')  if filters else None
#     active_schema    = filters.get('schema')     if filters else None

#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")

#     # =========================================================================
#     # QUESTION DIRECT PATH
#     # =========================================================================
#     if document_uuid and search_source == 'question':
#         print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
#         t_fetch = time.time()
#         results = await fetch_full_documents([document_uuid], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         ai_overview   = None
#         question_word = None
#         q_lower       = query.lower().strip()
#         for word in ('who', 'what', 'where', 'when', 'why', 'how'):
#             if q_lower.startswith(word):
#                 question_word = word
#                 break

#         question_signals = {
#             'query_mode':          'answer',
#             'wants_single_result': True,
#             'question_word':       question_word,
#         }

#         if results and results[0].get('key_facts'):
#             ai_overview = _build_ai_overview(question_signals, results, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
#                 results[0]['humanized_summary'] = ai_overview

#         related_searches = []
#         if results:
#             semantic_uuid = results[0].get('semantic_uuid')
#             if semantic_uuid:
#                 try:
#                     related_docs     = await fetch_documents_by_semantic_uuid(
#                         semantic_uuid, exclude_uuid=document_uuid, limit=5
#                     )
#                     related_searches = [
#                         {'query': doc.get('title', ''), 'url': doc.get('url', '')}
#                         for doc in related_docs if doc.get('title')
#                     ]
#                 except Exception as e:
#                     print(f"⚠️ Related searches error: {e}")

#         times['total'] = round((time.time() - t0) * 1000, 2)

#         return {
#             'query':             query,
#             'corrected_query':   query,
#             'intent':            'answer',
#             'query_mode':        'answer',
#             'answer':            answer,
#             'answer_type':       answer_type or 'UNKNOWN',
#             'results':           results,
#             'total':             len(results),
#             'facet_total':       len(results),
#             'total_image_count': 0,
#             'page':              1,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  False,
#             'search_strategy':   'question_direct',
#             'alt_mode':          alt_mode,
#             'skip_embedding':    True,
#             'search_source':     'question',
#             'valid_terms':       query.split(),
#             'unknown_terms':     [],
#             'data_type_facets':  [],
#             'category_facets':   [],
#             'schema_facets':     [],
#             'related_searches':  related_searches,
#             'facets':            {},
#             'word_discovery': {
#                 'valid_count':   len(query.split()),
#                 'unknown_count': 0,
#                 'corrections':   [],
#                 'filters':       [],
#                 'locations':     [],
#                 'sort':          None,
#                 'total_score':   0,
#                 'average_score': 0,
#                 'max_score':     0,
#             },
#             'timings':          times,
#             'filters_applied': {
#                 'data_type':             None,
#                 'category':              None,
#                 'schema':                None,
#                 'is_local_search':       False,
#                 'local_search_strength': 'none',
#             },
#             'signals': question_signals,
#             'profile': {},
#         }

#     # =========================================================================
#     # FAST PATH — finished cache hit
#     # =========================================================================
#     stable_key = _generate_stable_cache_key(session_id, query)
#     finished   = await _get_cached_results(stable_key)

#     if finished is not None:
#         print(f"⚡ FAST PATH: '{query}' | page={page} | "
#               f"filter={active_data_type}/{active_category}/{active_schema}")

#         all_results       = finished['all_results']
#         all_facets        = finished['all_facets']
#         facet_total       = finished['facet_total']
#         ai_overview       = finished.get('ai_overview')
#         total_image_count = finished.get('total_image_count', 0)
#         metadata          = finished['metadata']
#         times['cache']    = 'hit (fast path)'

#         filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t_fetch = time.time()
#         results = await fetch_full_documents([item['id'] for item in page_items], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         if results and page == 1 and ai_overview:
#             results[0]['humanized_summary'] = ai_overview

#         times['total'] = round((time.time() - t0) * 1000, 2)
#         signals        = metadata.get('signals', {})

#         print(f"⏱️ FAST PATH TIMING: {times}")

#         return {
#             'query':             query,
#             'corrected_query':   metadata.get('corrected_query', query),
#             'intent':            metadata.get('intent', 'general'),
#             'query_mode':        metadata.get('query_mode', 'keyword'),
#             'results':           results,
#             'total':             total_filtered,
#             'facet_total':       facet_total,
#             'total_image_count': total_image_count,
#             'page':              page,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  metadata.get('semantic_enabled', False),
#             'search_strategy':   metadata.get('search_strategy', 'cached'),
#             'alt_mode':          alt_mode,
#             'skip_embedding':    skip_embedding,
#             'search_source':     search_source,
#             'valid_terms':       metadata.get('valid_terms', query.split()),
#             'unknown_terms':     metadata.get('unknown_terms', []),
#             'data_type_facets':  all_facets.get('data_type', []),
#             'category_facets':   all_facets.get('category', []),
#             'schema_facets':     all_facets.get('schema', []),
#             'related_searches':  [],
#             'facets':            all_facets,
#             'word_discovery':    metadata.get('word_discovery', {
#                 'valid_count': len(query.split()), 'unknown_count': 0,
#                 'corrections': [], 'filters': [], 'locations': [],
#                 'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#             }),
#             'timings':          times,
#             'filters_applied':  metadata.get('filters_applied', {
#                 'data_type': active_data_type, 'category': active_category,
#                 'schema': active_schema, 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }),
#             'signals': signals,
#             'profile': metadata.get('profile', {}),
#         }

#     # =========================================================================
#     # FULL PATH — no cache
#     # =========================================================================
#     print(f"🔬 FULL PATH: '{query}' (no cache for key={stable_key[:12]}...)")

#     is_keyword_path = (
#         alt_mode == 'n' or
#         search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
#     )

#     # =========================================================================
#     # KEYWORD PATH — Stage 1 → 5 → 6 → 7
#     # =========================================================================
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PIPELINE: '{query}'")

#         intent  = detect_query_intent(query, pos_tags)
#         profile = {
#             'search_terms':     query.split(),
#             'cities':           [],
#             'states':           [],
#             'location_terms':   [],
#             'primary_intent':   intent,
#             'field_boosts': {
#                 'primary_keywords':  10,
#                 'entity_names':       8,
#                 'semantic_keywords':  6,
#                 'key_facts':          4,
#                 'document_title':     3,
#             },
#             'corrections':      [],
#             'persons':          [],
#             'organizations':    [],
#             'keywords':         [],
#             'media':            [],
#             'preferred_data_types': ['article'],
#         }

#         t1          = time.time()
#         all_results = await fetch_candidates_with_metadata(query, profile)
#         times['stage1'] = round((time.time() - t1) * 1000, 2)

#         counts = count_all(all_results)

#         await _set_cached_results(stable_key, {
#             'all_results':       all_results,
#             'all_facets':        counts['facets'],
#             'facet_total':       counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'ai_overview':       None,
#             'metadata': {
#                 'corrected_query':  query,
#                 'intent':           intent,
#                 'query_mode':       'keyword',
#                 'semantic_enabled': False,
#                 'search_strategy':  'keyword_graph_filter',
#                 'valid_terms':      query.split(),
#                 'unknown_terms':    [],
#                 'signals':          {},
#                 'profile':          profile,
#                 'word_discovery': {
#                     'valid_count': len(query.split()), 'unknown_count': 0,
#                     'corrections': [], 'filters': [], 'locations': [],
#                     'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#                 },
#                 'filters_applied': {
#                     'data_type': active_data_type, 'category': active_category,
#                     'schema': active_schema, 'is_local_search': False,
#                     'local_search_strength': 'none',
#                 },
#             },
#         })

#         filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t2      = time.time()
#         results = await fetch_full_documents([item['id'] for item in page_items], query)
#         times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
#         times['total']      = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ KEYWORD TIMING: {times}")

#         return {
#             'query':             query,
#             'corrected_query':   query,
#             'intent':            intent,
#             'query_mode':        'keyword',
#             'results':           results,
#             'total':             total_filtered,
#             'facet_total':       counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'page':              page,
#             'per_page':          per_page,
#             'search_time':       round(time.time() - t0, 3),
#             'session_id':        session_id,
#             'semantic_enabled':  False,
#             'search_strategy':   'keyword_graph_filter',
#             'alt_mode':          alt_mode,
#             'skip_embedding':    True,
#             'search_source':     search_source or 'dropdown',
#             'valid_terms':       query.split(),
#             'unknown_terms':     [],
#             'data_type_facets':  counts['facets'].get('data_type', []),
#             'category_facets':   counts['facets'].get('category', []),
#             'schema_facets':     counts['facets'].get('schema', []),
#             'related_searches':  [],
#             'facets':            counts['facets'],
#             'word_discovery': {
#                 'valid_count': len(query.split()), 'unknown_count': 0,
#                 'corrections': [], 'filters': [], 'locations': [],
#                 'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
#             },
#             'timings':          times,
#             'filters_applied': {
#                 'data_type': active_data_type, 'category': active_category,
#                 'schema': active_schema, 'is_local_search': False,
#                 'local_search_strength': 'none',
#             },
#             'signals': {},
#             'profile': profile,
#         }

#     # =========================================================================
#     # SEMANTIC PATH — Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
#     # =========================================================================
#     print(f"🔬 SEMANTIC PIPELINE: '{query}'")

#     # Stage 0 — Word Discovery + embedding in parallel
#     t1 = time.time()
#     discovery, query_embedding = await run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

#     # Intent detection
#     signals = {}
#     if INTENT_DETECT_AVAILABLE:
#         try:
#             discovery = await asyncio.to_thread(detect_intent, discovery)
#             signals   = discovery.get('signals', {})
#             print(f"   🎯 Intent: mode={signals.get('query_mode')} "
#                   f"domain={signals.get('primary_domain')} "
#                   f"local={signals.get('is_local_search')} "
#                   f"black_owned={signals.get('has_black_owned')} "
#                   f"superlative={signals.get('has_superlative')}")
#         except Exception as e:
#             print(f"   ⚠️ intent_detect error: {e}")

#     corrected_query  = discovery.get('corrected_query', query)
#     semantic_enabled = query_embedding is not None
#     query_mode       = signals.get('query_mode', 'explore')

#     # Read v3 profile
#     t2      = time.time()
#     profile = _read_v3_profile(discovery, signals=signals)
#     times['read_profile'] = round((time.time() - t2) * 1000, 2)

#     # Apply pos_mismatch corrections to search_terms
#     corrections = discovery.get('corrections', [])
#     if corrections:
#         correction_map = {
#             c['original'].lower(): c['corrected']
#             for c in corrections
#             if c.get('original') and c.get('corrected')
#             and c.get('correction_type') == 'pos_mismatch'
#         }
#         if correction_map:
#             original_terms          = profile['search_terms']
#             profile['search_terms'] = [
#                 correction_map.get(t.lower(), t) for t in original_terms
#             ]

#     intent      = profile['primary_intent']
#     city_names  = [c['name'] for c in profile['cities']]
#     state_names = [s['name'] for s in profile['states']]

#     print(f"   Intent: {intent} | Mode: {query_mode}")
#     print(f"   Cities: {city_names} | States: {state_names}")
#     print(f"   Search Terms: {profile['search_terms']}")

#     # Stage 1 — candidate UUIDs
#     t3 = time.time()

#     UNSAFE_CATEGORIES = {
#         'Food', 'US City', 'US State', 'Country', 'Location',
#         'City', 'Place', 'Object', 'Animal', 'Color',
#     }
#     has_unsafe_corrections = any(
#         c.get('correction_type') == 'pos_mismatch' or
#         c.get('category', '') in UNSAFE_CATEGORIES
#         for c in corrections
#     )
#     search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

#     candidate_uuids = await fetch_all_candidate_uuids(
#         search_query_for_stage1, profile, query_embedding,
#         signals=signals, discovery=discovery,
#     )
#     times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)

#     # Stage 2 + 3 — vector rerank + distance prune
#     survivor_uuids = candidate_uuids
#     vector_data    = {}

#     if semantic_enabled and candidate_uuids:
#         t4       = time.time()
#         reranked = await semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
#         times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

#         vector_data = {
#             item['id']: {
#                 'vector_distance': item.get('vector_distance', 1.0),
#                 'semantic_rank':   item.get('semantic_rank', 999999),
#             }
#             for item in reranked
#         }

#         DISTANCE_THRESHOLDS = {
#             'answer':  0.60,
#             'explore': 0.70,
#             'compare': 0.65,
#             'browse':  0.85,
#             'local':   0.85,
#             'shop':    0.80,
#         }
#         threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

#         before_prune   = len(candidate_uuids)
#         survivor_uuids = [
#             uuid for uuid in candidate_uuids
#             if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
#         ]
#         after_prune = len(survivor_uuids)

#         if before_prune != after_prune:
#             print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): "
#                   f"{before_prune} → {after_prune} "
#                   f"({before_prune - after_prune} removed)")
#         times['stage3_prune'] = f"{before_prune} → {after_prune}"

#     else:
#         print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, "
#               f"candidates={len(candidate_uuids)}")

#     # Stage 4 — metadata for survivors
#     t5          = time.time()
#     all_results = await fetch_candidate_metadata(survivor_uuids)
#     times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

#     # Stage 5 — resolve blend once, score every document (CPU-bound, stays sync)
#     if all_results:
#         blend      = _resolve_blend(query_mode, signals, all_results)
#         pool_size  = len(all_results)

#         for idx, item in enumerate(all_results):
#             _score_document(
#                 idx        = idx,
#                 item       = item,
#                 profile    = profile,
#                 signals    = signals,
#                 blend      = blend,
#                 pool_size  = pool_size,
#                 vector_data = vector_data,
#             )

#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#     counts = count_all(all_results)

#     # AI Overview preview
#     ai_overview = None
#     if all_results:
#         preview_items, _ = paginate_cached_results(all_results, 1, per_page)
#         preview_docs     = await fetch_full_documents([item['id'] for item in preview_items], query)
#         if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
#             ai_overview = _build_ai_overview(signals, preview_docs, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview: {ai_overview[:80]}...")

#     valid_terms   = profile['search_terms']
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

#     locations_block = (
#         [
#             {'field': 'location_city',  'values': city_names},
#             {'field': 'location_state', 'values': state_names},
#         ]
#         if city_names or state_names else []
#     )

#     # Stage 6 — cache the finished package
#     await _set_cached_results(stable_key, {
#         'all_results':       all_results,
#         'all_facets':        counts['facets'],
#         'facet_total':       counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'ai_overview':       ai_overview,
#         'metadata': {
#             'corrected_query':  corrected_query,
#             'intent':           intent,
#             'query_mode':       query_mode,
#             'semantic_enabled': semantic_enabled,
#             'search_strategy':  'staged_semantic' if semantic_enabled else 'keyword_fallback',
#             'valid_terms':      valid_terms,
#             'unknown_terms':    unknown_terms,
#             'signals':          signals,
#             'city_names':       city_names,
#             'state_names':      state_names,
#             'profile':          profile,
#             'word_discovery': {
#                 'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
#                 'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#                 'corrections':   discovery.get('corrections', []),
#                 'filters':       [],
#                 'locations':     locations_block,
#                 'sort':          None,
#                 'total_score':   0,
#                 'average_score': 0,
#                 'max_score':     0,
#             },
#             'filters_applied': {
#                 'data_type':             active_data_type,
#                 'category':              active_category,
#                 'schema':                active_schema,
#                 'is_local_search':       signals.get('is_local_search', False),
#                 'local_search_strength': signals.get('local_search_strength', 'none'),
#                 'has_black_owned':       signals.get('has_black_owned', False),
#                 'graph_filters':         [],
#                 'graph_locations':       locations_block,
#                 'graph_sort':            None,
#             },
#         },
#     })
#     print(f"💾 Cached semantic package: {counts['facet_total']} results, "
#           f"{counts['total_image_count']} image docs")

#     # Stage 7 — filter → paginate → fetch full docs
#     filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#     t6      = time.time()
#     results = await fetch_full_documents([item['id'] for item in page_items], query)
#     times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

#     if results and page == 1 and ai_overview:
#         results[0]['humanized_summary'] = ai_overview

#     if query_embedding:
#         try:
#             await asyncio.to_thread(
#                 store_query_embedding,
#                 corrected_query, query_embedding,
#                 result_count=counts['facet_total']
#             )
#         except Exception as e:
#             print(f"⚠️ store_query_embedding error: {e}")

#     times['total'] = round((time.time() - t0) * 1000, 2)
#     strategy       = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

#     print(f"⏱️ SEMANTIC TIMING: {times}")
#     print(f"🔍 {strategy.upper()} ({query_mode}) | "
#           f"Total: {counts['facet_total']} | "
#           f"Filtered: {total_filtered} | "
#           f"Page: {len(results)} | "
#           f"Images: {counts['total_image_count']}")

#     return {
#         'query':             query,
#         'corrected_query':   corrected_query,
#         'intent':            intent,
#         'query_mode':        query_mode,
#         'results':           results,
#         'total':             total_filtered,
#         'facet_total':       counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'page':              page,
#         'per_page':          per_page,
#         'search_time':       round(time.time() - t0, 3),
#         'session_id':        session_id,
#         'semantic_enabled':  semantic_enabled,
#         'search_strategy':   strategy,
#         'alt_mode':          alt_mode,
#         'skip_embedding':    skip_embedding,
#         'search_source':     search_source,
#         'valid_terms':       valid_terms,
#         'unknown_terms':     unknown_terms,
#         'related_searches':  [],
#         'data_type_facets':  counts['facets'].get('data_type', []),
#         'category_facets':   counts['facets'].get('category', []),
#         'schema_facets':     counts['facets'].get('schema', []),
#         'facets':            counts['facets'],
#         'word_discovery': {
#             'valid_count':   discovery.get('stats', {}).get('valid_words', 0),
#             'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#             'corrections':   discovery.get('corrections', []),
#             'filters':       [],
#             'locations':     locations_block,
#             'sort':          None,
#             'total_score':   0,
#             'average_score': 0,
#             'max_score':     0,
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type':             active_data_type,
#             'category':              active_category,
#             'schema':                active_schema,
#             'is_local_search':       signals.get('is_local_search', False),
#             'local_search_strength': signals.get('local_search_strength', 'none'),
#             'has_black_owned':       signals.get('has_black_owned', False),
#             'graph_filters':         [],
#             'graph_locations':       locations_block,
#             'graph_sort':            None,
#         },
#         'signals': signals,
#         'profile': profile,
#     }


# # ============================================================
# # COMPATIBILITY STUBS — keep views.py imports working
# # ============================================================

# def get_facets(query: str) -> dict:
#     """Returns empty dict. Kept for views.py import compatibility."""
#     return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """Returns empty list. Kept for views.py import compatibility."""
#     return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns a featured snippet if the top result has high authority."""
#     if not results:
#         return None
#     top = results[0]
#     if top.get('authority_score', 0) >= 85:
#         return {
#             'type':      'featured_snippet',
#             'title':     top.get('title'),
#             'snippet':   top.get('summary', ''),
#             'key_facts': top.get('key_facts', [])[:3],
#             'source':    top.get('source'),
#             'url':       top.get('url'),
#             'image':     top.get('image'),
#         }
#     return None


# def log_search_event(**kwargs):
#     """No-op. Kept for views.py import compatibility."""
#     pass


# async def typesense_search(
#     query: str = '*',
#     filter_by: str = None,
#     sort_by: str = 'authority_score:desc',
#     per_page: int = 20,
#     page: int = 1,
#     facet_by: str = None,
#     query_by: str = 'document_title,document_summary,keywords,primary_keywords',
#     max_facet_values: int = 20,
# ) -> Dict:
#     """Simple Typesense search wrapper for direct use outside the pipeline."""
#     params = {
#         'q':        query,
#         'query_by': query_by,
#         'per_page': per_page,
#         'page':     page,
#     }
#     if filter_by:
#         params['filter_by'] = filter_by
#     if sort_by:
#         params['sort_by'] = sort_by
#     if facet_by:
#         params['facet_by']         = facet_by
#         params['max_facet_values'] = max_facet_values

#     try:
#         return await asyncio.to_thread(
#             client.collections[COLLECTION_NAME].documents.search,
#             params
#         )
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================
# # END OF PART 8 — END OF FILE
# # ============================================================



# This code has the value for the questions to come in.# ============================================================
# FILE: typesense_discovery_bridge.py
# ============================================================
# ASYNC VERSION — converted from synchronous to async/await.
#
# CHANGES FROM SYNC VERSION:
#   - ThreadPoolExecutor replaced with asyncio.gather()
#   - All Typesense calls wrapped in asyncio.to_thread()
#   - All Django cache calls wrapped in asyncio.to_thread()
#   - All external module calls (WordDiscovery, embedding,
#     intent_detect, store_query_embedding) wrapped in
#     asyncio.to_thread()
#   - Pure CPU functions (scoring, filtering, formatting,
#     counting, profile reading) remain synchronous
#   - Return formats are IDENTICAL to the sync version
#
# HOW TO USE:
#   In views.py, change:
#     result = execute_full_search(...)
#   To:
#     result = await execute_full_search(...)
#   And make your view function: async def your_view(request):
# ============================================================


# ============================================================
# PART 1 OF 8 — IMPORTS, CONSTANTS, UTILITY FUNCTIONS
# ============================================================



"""
typesense_discovery_bridge.py (ASYNC)
=====================================
AfroToDo Search Bridge — Word Discovery v3 + Intent Detection + Typesense.

SCORING ALGORITHM (v4)
----------------------
final_score = (
    blend['text_match'] * text_score      +
    blend['semantic']   * semantic_score  +
    blend['authority']  * authority_score_n
)
final_score *= _domain_relevance(doc, signals)
final_score *= _content_intent_match(doc, query_mode)
final_score *= _pool_type_multiplier(doc, query_mode)

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


# ── Scoring Thresholds ────────────────────────────────────────────────────────

SEMANTIC_DISTANCE_GATE    = 0.65
QUESTION_SEMANTIC_DISTANCE_GATE = 0.40
REVIEW_COUNT_SCALE_BIZ    = 500
REVIEW_COUNT_SCALE_RECIPE = 200
BLACK_OWNED_BOOST         = 0.12
PREFERRED_TYPE_BOOST      = 0.08
SUPERLATIVE_SCORE_CAP     = 0.70


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
# END OF PART 1
# ============================================================


# ============================================================
# PART 2 OF 8 — CACHE FUNCTIONS + WORD DISCOVERY RUNNERS
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

    FIX — frozenset serialization bug:
        WD v3 writes context_flags as a frozenset on each term dict.
        frozenset is not JSON-serializable and silently breaks Redis
        caching, causing every request to bypass cache and run the
        full pipeline. This function converts every context_flags
        value to a sorted list before returning.

    Embedding re-use logic:
        Always embeds the original query first.
        Only re-embeds with corrected_query when all corrections are
        safe. Unsafe categories (Food, US City, US State, Country,
        Location, City, Place, Object, Animal, Color) are never
        re-embedded because replacing them changes semantic meaning.
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
# END OF PART 2
# ============================================================

# ============================================================
# PART 3 OF 8 — V3 PROFILE READER + TYPESENSE PARAMETER BUILDERS
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


# def build_typesense_params(
#     profile: Dict,
#     ui_filters: Dict = None,
#     signals: Dict = None
# ) -> Dict:
#     """
#     Convert the v3 profile into Typesense search parameters.

#     Builds query_by, query_by_weights, filter_by, sort_by,
#     typo settings, and prefix settings from the profile and signals.

#     FIX — local mode pool scoping:
#         Adds document_data_type:=business to filter_by when
#         query_mode is local and no UI data_type filter overrides it.
#         This prevents restaurant queries competing against every
#         article in the index.
#     """
#     signals    = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
#     params     = {}

#     # ── Query string — deduplicate search_terms ───────────────────────────
#     seen         = set()
#     unique_terms = []
#     for term in profile.get('search_terms', []):
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)

#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'

#     # ── Field boosts — read from v3, add mode-specific fields ────────────
#     field_boosts = dict(profile.get('field_boosts', {}))

#     if query_mode == 'local':
#         field_boosts.setdefault('service_type',        12)
#         field_boosts.setdefault('service_specialties', 10)

#     sorted_fields              = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
#     params['query_by']         = ','.join(f[0] for f in sorted_fields)
#     params['query_by_weights'] = ','.join(str(f[1]) for f in sorted_fields)

#     # ── Typo / prefix / drop-token settings by mode ──────────────────────
#     has_corrections = bool(profile.get('corrections'))
#     term_count      = len(unique_terms)

#     if query_mode == 'answer':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'explore':
#         params['num_typos']             = 0 if has_corrections else 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
#     elif query_mode == 'browse':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
#     elif query_mode == 'local':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     elif query_mode == 'compare':
#         params['num_typos']             = 0
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'shop':
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1
#     else:
#         params['num_typos']             = 1
#         params['prefix']                = 'no'
#         params['drop_tokens_threshold'] = 1

#     # ── Sort order ────────────────────────────────────────────────────────
#     temporal_direction = signals.get('temporal_direction')
#     price_direction    = signals.get('price_direction')
#     has_superlative    = signals.get('has_superlative', False)
#     has_rating         = signals.get('has_rating_signal', False)

#     if temporal_direction == 'oldest':
#         params['sort_by'] = 'time_period_start:asc,authority_score:desc'
#     elif temporal_direction == 'newest':
#         params['sort_by'] = 'published_date:desc,authority_score:desc'
#     elif price_direction == 'cheap':
#         params['sort_by'] = 'product_price:asc,authority_score:desc'
#     elif price_direction == 'expensive':
#         params['sort_by'] = 'product_price:desc,authority_score:desc'
#     elif query_mode == 'local':
#         params['sort_by'] = 'authority_score:desc'
#     elif query_mode == 'browse' and has_superlative:
#         params['sort_by'] = 'authority_score:desc'
#     elif has_rating:
#         params['sort_by'] = 'authority_score:desc'
#     else:
#         params['sort_by'] = '_text_match:desc,authority_score:desc'

#     # ── filter_by — locations + black_owned + local scope + UI filters ────
#     filter_conditions = []

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     # FIX — local mode: scope pool to business documents only
#     if query_mode == 'local' and not (ui_filters and ui_filters.get('data_type')):
#         filter_conditions.append('document_data_type:=business')

#     if ui_filters:
#         if ui_filters.get('data_type'):
#             filter_conditions.append(f"document_data_type:={ui_filters['data_type']}")
#         if ui_filters.get('category'):
#             filter_conditions.append(f"document_category:={ui_filters['category']}")
#         if ui_filters.get('schema'):
#             filter_conditions.append(f"document_schema:={ui_filters['schema']}")

#     if filter_conditions:
#         params['filter_by'] = ' && '.join(filter_conditions)

#     return params



# def build_filter_string_without_data_type(
#     profile: Dict,
#     signals: Dict = None
# ) -> str:
#     """
#     Build the location-only filter string used in Stage 1A.
#     No data_type included so facet counting stays accurate across all types.
#     black_owned is included because it is a hard filter, not a facet.
#     """
#     signals           = signals or {}
#     filter_conditions = []
#     query_mode        = signals.get('query_mode', 'explore')

#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             filter_conditions.append(
#                 city_filters[0] if len(city_filters) == 1
#                 else '(' + ' || '.join(city_filters) + ')'
#             )

#         states = profile.get('states', [])
#         if states:
#             state_conditions = [
#                 f"location_state:={variant}"
#                 for state in states
#                 for variant in state.get('variants', [state['name']])
#             ]
#             filter_conditions.append(
#                 state_conditions[0] if len(state_conditions) == 1
#                 else '(' + ' || '.join(state_conditions) + ')'
#             )

#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')

#     return ' && '.join(filter_conditions) if filter_conditions else ''



def build_typesense_params(
    profile: Dict,
    ui_filters: Dict = None,
    signals: Dict = None
) -> Dict:
    """
    Convert the v3 profile into Typesense search parameters.
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

    # ── Field boosts — read from v3, add mode-specific fields ────────────
    field_boosts = dict(profile.get('field_boosts', {}))

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
# END OF PART 3
# ============================================================

# ============================================================
# PART 4 OF 8 — SCORING FUNCTIONS (ALL SYNCHRONOUS — CPU ONLY)
# ============================================================

def _resolve_blend(
    query_mode: str,
    signals: Dict,
    candidates: List[Dict]
) -> Dict:
    """
    Build the final blend ratios for this query at runtime.
    """
    blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

    sample             = candidates[:20]
    has_live_authority = any(c.get('authority_score', 0) > 0 for c in sample)

    if not has_live_authority and blend['authority'] > 0:
        print(f"   ⚠️ Authority weight dead ({blend['authority']:.2f}) — redistributing to semantic")
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

    if query_mode == 'answer' and signals.get('wants_single_result'):
        blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

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

    if data_type == 'business':
        rating  = doc.get('service_rating') or 0.0
        reviews = doc.get('service_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)
        return 0.0

    if data_type == 'product':
        rating  = doc.get('product_rating') or 0.0
        reviews = doc.get('product_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)
        return 0.0

    if data_type == 'recipe':
        rating  = doc.get('recipe_rating') or 0.0
        reviews = doc.get('recipe_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_RECIPE),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)
        return 0.0

    if data_type == 'media':
        return min((doc.get('media_rating') or 0.0) / 5.0, 1.0)

    raw = doc.get('authority_score') or 0.0
    if raw > 0:
        return min(raw / 100.0, 1.0)

    depth     = doc.get('factual_density_score') or 0
    evergreen = doc.get('evergreen_score') or 0
    if depth > 0 or evergreen > 0:
        return min((depth + evergreen) / 200.0, 0.5)

    return 0.0


def _compute_text_score(
    keyword_rank: int,
    pool_size: int,
    item: Dict,
    profile: Dict
) -> float:
    """Positional score from the item's rank in Stage 1A keyword results."""
    base = 1.0 - (keyword_rank / max(pool_size, 1))

    doc_kws = set(k.lower() for k in (item.get('primary_keywords') or []))
    wd_kws  = set(
        k.get('phrase', '').lower()
        for k in profile.get('keywords', [])
        if k.get('phrase')
    )
    overlap = doc_kws & wd_kws
    bonus   = min(len(overlap) * 0.05, 0.15)

    return min(base + bonus, 1.0)


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
    """Compute the final blended score for one document."""
    query_mode = signals.get('query_mode', 'explore')
    item_id    = item.get('id', '')

    vd              = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
    vector_distance = vd.get('vector_distance', 1.0)
    semantic_rank   = vd.get('semantic_rank', 999999)

    item['vector_distance'] = vector_distance
    item['semantic_rank']   = semantic_rank

    text_score = _compute_text_score(idx, pool_size, item, profile)
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
# END OF PART 4
# ============================================================

# ============================================================
# PART 5 OF 8 — CANDIDATE FETCHING (STAGES 1A, 1B, COMBINED)
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
        'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
        'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
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

    # ── Build location tokens for mandatory location check ────────────────
    # If the query contains a detected city or state, any candidate that
    # passes the distance gate and token validation must ALSO contain
    # the location token. This prevents "first mayor of savannah" from
    # matching "first mayor of little rock".
    location_tokens = set()
    for c in profile.get('cities', []):
        location_tokens.update(_normalize_signal(c.get('name', '')))
    for s in profile.get('states', []):
        location_tokens.update(_normalize_signal(s.get('name', '')))
        # Also include state variants
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

        # Fallback if too few hits with location filter
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

            # ── Mandatory location check ──────────────────────────────────
            # If the query contains a detected city/state, the candidate
            # must contain that location token in its keywords/entities.
            # Without this, "first mayor of savannah" would match
            # "first mayor of little rock" because "first" and "mayor"
            # pass the generic token validation.
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
# END OF PART 5
# ============================================================


# ============================================================
# PART 6 OF 8 — METADATA FETCHING, RERANKING, COUNTING,
#               FILTERING, PAGINATION
# ============================================================

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
    """Stage 4 — fetch lightweight metadata for survivors. Batches in groups of 100."""
    if not survivor_ids:
        return []

    BATCH_SIZE = 100
    doc_map    = {}

    # Build all batch coroutines
    async def _fetch_batch(batch_ids, batch_index):
        id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])
        params = {
            'q':         '*',
            'filter_by': f'document_uuid:[{id_filter}]',
            'per_page':  len(batch_ids),
            'include_fields': ','.join([
                'document_uuid', 'document_data_type', 'document_category',
                'document_schema', 'document_title', 'content_intent',
                'authority_score', 'service_rating', 'service_review_count',
                'service_type', 'product_rating', 'product_review_count',
                'recipe_rating', 'recipe_review_count', 'media_rating',
                'factual_density_score', 'evergreen_score',
                'primary_keywords', 'black_owned', 'image_url', 'logo_url',
            ]),
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
                    batch_results[uuid] = {
                        'id':                   uuid,
                        'data_type':            doc.get('document_data_type', ''),
                        'category':             doc.get('document_category', ''),
                        'schema':               doc.get('document_schema', ''),
                        'title':                doc.get('document_title', ''),
                        'content_intent':       doc.get('content_intent', ''),
                        'authority_score':      doc.get('authority_score', 0),
                        'service_rating':       doc.get('service_rating', 0),
                        'service_review_count': doc.get('service_review_count', 0),
                        'service_type':         doc.get('service_type', []),
                        'product_rating':       doc.get('product_rating', 0),
                        'product_review_count': doc.get('product_review_count', 0),
                        'recipe_rating':        doc.get('recipe_rating', 0),
                        'recipe_review_count':  doc.get('recipe_review_count', 0),
                        'media_rating':         doc.get('media_rating', 0),
                        'factual_density_score': doc.get('factual_density_score', 0),
                        'evergreen_score':      doc.get('evergreen_score', 0),
                        'primary_keywords':     doc.get('primary_keywords', []),
                        'black_owned':          doc.get('black_owned', False),
                        'image_url':            doc.get('image_url', []),
                        'logo_url':             doc.get('logo_url', []),
                    }
            return batch_results
        except Exception as e:
            print(f"❌ Stage 4 metadata fetch error (batch {batch_index}): {e}")
            return {}

    # Run all batches concurrently
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
            'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
            'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
            'per_page':              PAGE_SIZE,
            'page':                  current_page,
            'include_fields':        ','.join([
                'document_uuid', 'document_data_type', 'document_category',
                'document_schema', 'document_title', 'content_intent',
                'authority_score', 'service_rating', 'service_review_count',
                'service_type', 'product_rating', 'product_review_count',
                'recipe_rating', 'recipe_review_count', 'media_rating',
                'factual_density_score', 'evergreen_score',
                'primary_keywords', 'black_owned', 'image_url', 'logo_url',
            ]),
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
                doc = hit.get('document', {})
                all_results.append({
                    'id':                   doc.get('document_uuid'),
                    'data_type':            doc.get('document_data_type', ''),
                    'category':             doc.get('document_category', ''),
                    'schema':               doc.get('document_schema', ''),
                    'title':                doc.get('document_title', ''),
                    'content_intent':       doc.get('content_intent', ''),
                    'authority_score':      doc.get('authority_score', 0),
                    'service_rating':       doc.get('service_rating', 0),
                    'service_review_count': doc.get('service_review_count', 0),
                    'service_type':         doc.get('service_type', []),
                    'product_rating':       doc.get('product_rating', 0),
                    'product_review_count': doc.get('product_review_count', 0),
                    'recipe_rating':        doc.get('recipe_rating', 0),
                    'recipe_review_count':  doc.get('recipe_review_count', 0),
                    'media_rating':         doc.get('media_rating', 0),
                    'factual_density_score': doc.get('factual_density_score', 0),
                    'evergreen_score':      doc.get('evergreen_score', 0),
                    'primary_keywords':     doc.get('primary_keywords', []),
                    'black_owned':          doc.get('black_owned', False),
                    'image_url':            doc.get('image_url', []),
                    'logo_url':             doc.get('logo_url', []),
                    'text_match':           hit.get('text_match', 0),
                })

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
# END OF PART 6
# ============================================================

# ============================================================
# PART 7 OF 8 — DOCUMENT FETCHING, FORMATTING, AI OVERVIEW
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
    """Transform a raw Typesense hit into the response format."""
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
# END OF PART 7
# ============================================================

# ============================================================
# PART 8 OF 8 — MAIN ENTRY POINT + COMPATIBILITY STUBS
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

    Runs one of four paths depending on request type:

    QUESTION PATH
        document_uuid + search_source='question' supplied.
        Fetches that single document directly and returns it.

    FAST PATH (cache hit)
        Finished result package found in Redis.
        Applies UI filters, paginates, fetches full docs for page.

    KEYWORD PATH (alt_mode='n' or dropdown source)
        Stage 1 (keyword+metadata) → Stage 5 (count) →
        Stage 6 (cache) → Stage 7 (paginate+fetch)

    SEMANTIC PATH (default)
        Stage 1A+1B (uuids) → Stage 2 (rerank) → Stage 3 (prune) →
        Stage 4 (metadata) → Stage 5 (score+count) → Stage 6 (cache) →
        Stage 7 (paginate+fetch)
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

    # # =========================================================================
    # # QUESTION DIRECT PATH
    # # =========================================================================
    # if document_uuid and search_source == 'question':
    #     print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
    #     t_fetch = time.time()
    #     results = await fetch_full_documents([document_uuid], query)
    #     times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

    #     ai_overview   = None
    #     question_word = None
    #     q_lower       = query.lower().strip()
    #     for word in ('who', 'what', 'where', 'when', 'why', 'how'):
    #         if q_lower.startswith(word):
    #             question_word = word
    #             break

    #     question_signals = {
    #         'query_mode':          'answer',
    #         'wants_single_result': True,
    #         'question_word':       question_word,
    #     }

    #     if results and results[0].get('key_facts'):
    #         ai_overview = _build_ai_overview(question_signals, results, query)
    #         if ai_overview:
    #             print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
    #             results[0]['humanized_summary'] = ai_overview

    #     related_searches = []
    #     if results:
    #         semantic_uuid = results[0].get('semantic_uuid')
    #         if semantic_uuid:
    #             try:
    #                 related_docs     = await fetch_documents_by_semantic_uuid(
    #                     semantic_uuid, exclude_uuid=document_uuid, limit=5
    #                 )
    #                 related_searches = [
    #                     {'query': doc.get('title', ''), 'url': doc.get('url', '')}
    #                     for doc in related_docs if doc.get('title')
    #                 ]
    #             except Exception as e:
    #                 print(f"⚠️ Related searches error: {e}")

    #     times['total'] = round((time.time() - t0) * 1000, 2)

    #     return {
    #         'query':             query,
    #         'corrected_query':   query,
    #         'intent':            'answer',
    #         'query_mode':        'answer',
    #         'answer':            answer,
    #         'answer_type':       answer_type or 'UNKNOWN',
    #         'results':           results,
    #         'total':             len(results),
    #         'facet_total':       len(results),
    #         'total_image_count': 0,
    #         'page':              1,
    #         'per_page':          per_page,
    #         'search_time':       round(time.time() - t0, 3),
    #         'session_id':        session_id,
    #         'semantic_enabled':  False,
    #         'search_strategy':   'question_direct',
    #         'alt_mode':          alt_mode,
    #         'skip_embedding':    True,
    #         'search_source':     'question',
    #         'valid_terms':       query.split(),
    #         'unknown_terms':     [],
    #         'data_type_facets':  [],
    #         'category_facets':   [],
    #         'schema_facets':     [],
    #         'related_searches':  related_searches,
    #         'facets':            {},
    #         'word_discovery': {
    #             'valid_count':   len(query.split()),
    #             'unknown_count': 0,
    #             'corrections':   [],
    #             'filters':       [],
    #             'locations':     [],
    #             'sort':          None,
    #             'total_score':   0,
    #             'average_score': 0,
    #             'max_score':     0,
    #         },
    #         'timings':          times,
    #         'filters_applied': {
    #             'data_type':             None,
    #             'category':              None,
    #             'schema':                None,
    #             'is_local_search':       False,
    #             'local_search_strength': 'none',
    #         },
    #         'signals': question_signals,
    #         'profile': {},
    #     }

    # =========================================================================
    # QUESTION DIRECT PATH
    # =========================================================================
    if document_uuid and search_source == 'question':
        print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
        t_fetch = time.time()
        results = await fetch_full_documents([document_uuid], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        # Fetch cluster siblings if the document belongs to a cluster
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

        related_searches = []
        if results:
            semantic_uuid = results[0].get('semantic_uuid')
            if semantic_uuid:
                try:
                    related_docs     = await fetch_documents_by_semantic_uuid(
                        semantic_uuid, exclude_uuid=document_uuid, limit=5
                    )
                    related_searches = [
                        {'query': doc.get('title', ''), 'url': doc.get('url', '')}
                        for doc in related_docs if doc.get('title')
                    ]
                except Exception as e:
                    print(f"⚠️ Related searches error: {e}")

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
            'related_searches':  related_searches,
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
                'primary_keywords':  10,
                'entity_names':       8,
                'semantic_keywords':  6,
                'key_facts':          4,
                'document_title':     3,
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
# END OF PART 8 — END OF FILE
# ============================================================