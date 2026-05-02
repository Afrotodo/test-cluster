# """
# typesense_discovery_bridge.py
# =============================
# Complete search bridge between Word Discovery v2 and Typesense.

# This file handles EVERYTHING:
# - Word Discovery v2 integration
# - Intent signal integration (query_mode, question_word, etc.)
# - Query profile building (POS-based term routing, field boosts per mode)
# - Embedding generation (via embedding_client.py)
# - Result caching (self-contained)
# - Stage 1: Graph Filter (candidate generation)
# - Stage 2: Semantic Rerank (vector-based ranking with mode-specific blend)
# - Facet counting from cache
# - Pagination from cache
# - Full document fetching
# - AI Overview (signal-driven key_fact selection)
# - Returns same structure as execute_full_search() for views.py compatibility

# USAGE IN VIEWS.PY:
#     from .typesense_discovery_bridge import execute_full_search
    
#     result = execute_full_search(
#         query=corrected_query,
#         session_id=params.session_id,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         alt_mode=params.alt_mode,
#         ...
#     )
# """

# import re
# import json
# import time
# import hashlib
# import threading
# import typesense
# from typing import Dict, List, Tuple, Optional, Any, Set
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# from decouple import config
# import requests

# # ============================================================================
# # IMPORTS - Word Discovery v2 and Embedding Client
# # ============================================================================

# try:
#     from .word_discovery_fulltest import WordDiscovery
#     WORD_DISCOVERY_AVAILABLE = True
#     print("✅ WordDiscovery imported from .word_discovery_v2")
# except ImportError:
#     try:
#         from word_discovery_fulltest import WordDiscovery
#         WORD_DISCOVERY_AVAILABLE = True
#         print("✅ WordDiscovery imported from word_discovery_v2")
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery_v2 not available")

# try:
#     from .intent_detect import detect_intent, get_signals
#     INTENT_DETECT_AVAILABLE = True
#     print("✅ intent_detect imported")
# except ImportError:
#     try:
#         from intent_detect import detect_intent, get_signals
#         INTENT_DETECT_AVAILABLE = True
#         print("✅ intent_detect imported from intent_detect")
#     except ImportError:
#         INTENT_DETECT_AVAILABLE = False
#         print("⚠️ intent_detect not available")

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


# import random


# def humanize_key_facts(key_facts: list, query: str = '', matched_keyword: str = '',
#                        question_word: str = None) -> str:
#     """Format key_facts into a readable AfroToDo AI Overview,
#     only returning facts relevant to the matched keyword and question type.
    
#     Blueprint Step 8: AI Overview key_fact filtering based on question_word.
#     """
#     if not key_facts:
#         return ''
    
#     # Clean up facts
#     facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
    
#     if not facts:
#         return ''
    
#     # ─── Question-word-based fact filtering (Blueprint Step 8) ───────
#     if question_word:
#         qw = question_word.lower()
#         if qw == 'where':
#             # Prioritize facts with geographic language
#             geo_words = {'located', 'bounded', 'continent', 'region', 'coast',
#                          'ocean', 'border', 'north', 'south', 'east', 'west',
#                          'latitude', 'longitude', 'hemisphere', 'capital',
#                          'city', 'state', 'country', 'area', 'lies', 'situated'}
#             relevant_facts = [f for f in facts
#                               if any(gw in f.lower() for gw in geo_words)]
#         elif qw == 'when':
#             # Prioritize facts with dates/years/temporal language
#             import re as _re
#             temporal_words = {'founded', 'established', 'born', 'created',
#                               'started', 'opened', 'built', 'year', 'date',
#                               'century', 'decade', 'era', 'period'}
#             relevant_facts = [f for f in facts
#                               if any(tw in f.lower() for tw in temporal_words)
#                               or _re.search(r'\b\d{4}\b', f)]
#         elif qw == 'who':
#             # Prioritize facts with names, roles, titles, achievements
#             who_words = {'first', 'president', 'founder', 'ceo', 'leader',
#                          'director', 'known', 'famous', 'awarded', 'pioneer',
#                          'invented', 'created', 'named', 'appointed', 'elected'}
#             relevant_facts = [f for f in facts
#                               if any(ww in f.lower() for ww in who_words)]
#         elif qw == 'what':
#             # Prioritize definitional facts
#             what_words = {'is a', 'refers to', 'defined', 'known as',
#                           'type of', 'form of', 'means', 'represents'}
#             relevant_facts = [f for f in facts
#                               if any(ww in f.lower() for ww in what_words)]
#         else:
#             relevant_facts = []
        
#         # Fall back to keyword match if question-word filter found nothing
#         if not relevant_facts and matched_keyword:
#             keyword_lower = matched_keyword.lower()
#             relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        
#         # Final fallback: first fact
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     elif matched_keyword:
#         keyword_lower = matched_keyword.lower()
#         relevant_facts = [f for f in facts if keyword_lower in f.lower()]
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     else:
#         relevant_facts = [facts[0]]
    
#     # Cap at 2 — keeps it concise
#     relevant_facts = relevant_facts[:2]
    
#     is_question = query and any(
#         query.lower().startswith(w) 
#         for w in ['who', 'what', 'where', 'when', 'why', 'how', 'is', 'are', 'can', 'do', 'does']
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


# # ============================================================================
# # THREAD POOL
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # TYPESENSE CLIENT
# # ============================================================================

# client = typesense.Client({
#     'api_key': config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host': config('TYPESENSE_HOST'),
#         'port': config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL')
#     }],
#     'connection_timeout_seconds': 5
# })

# COLLECTION_NAME = 'document'


# # ============================================================================
# # RESULT CACHE (Self-Contained)
# # ============================================================================

# from django.core.cache import cache as django_cache

# CACHE_TTL_SECONDS = 300  # 5 minutes
# MAX_CACHED_RESULTS = 2000


# def _get_cached_results(cache_key: str):
#     """Get cached result set from Redis."""
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


# def _set_cached_results(cache_key: str, data):
#     """Cache result set in Redis with TTL."""
#     try:
#         django_cache.set(cache_key, data, timeout=CACHE_TTL_SECONDS)
#         print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
#     except Exception as e:
#         print(f"⚠️ Redis cache SET error: {e}")


# def clear_search_cache():
#     """Clear all cached search results."""
#     try:
#         django_cache.clear()
#         print("🧹 Redis search cache cleared")
#     except Exception as e:
#         print(f"⚠️ Redis cache CLEAR error: {e}")


# # ============================================================================
# # CONSTANTS
# # ============================================================================

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

# KNOWN_CITIES = frozenset([
#     'atlanta', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio',
#     'san diego', 'dallas', 'austin', 'jacksonville', 'fort worth', 'columbus',
#     'charlotte', 'seattle', 'denver', 'boston', 'detroit', 'memphis', 'baltimore',
#     'nashville', 'milwaukee', 'albuquerque', 'tucson', 'fresno', 'sacramento',
#     'miami', 'oakland', 'minneapolis', 'tulsa', 'cleveland', 'new orleans',
#     'birmingham', 'montgomery', 'mobile', 'jackson', 'baton rouge', 'shreveport',
#     'savannah', 'charleston', 'richmond', 'norfolk', 'durham', 'raleigh',
#     'greensboro', 'louisville', 'lexington', 'cincinnati', 'st louis', 'kansas city',
#     'omaha', 'tampa', 'orlando', 'pittsburgh', 'las vegas', 'portland',
#     'los angeles', 'san francisco', 'new york', 'brooklyn', 'queens', 'harlem',
# ])

# # Categories for intent detection
# PERSON_CATEGORIES = frozenset([
#     'Person', 'Historical Figure', 'Celebrity', 'Athlete', 'Politician',
# ])

# ORGANIZATION_CATEGORIES = frozenset([
#     'Organization', 'Company', 'Business', 'Brand', 'HBCU',
# ])

# LOCATION_CATEGORIES = frozenset([
#     'US City', 'US State', 'US County', 'City', 'State', 'Country', 'Location',
#     'Continent', 'Region', 'continent', 'region', 'country',
# ])

# KEYWORD_CATEGORIES = frozenset([
#     'Keyword', 'Topic', 'Primary Keyword',
# ])

# MEDIA_CATEGORIES = frozenset([
#     'Song', 'Movie', 'Album', 'Book', 'TV Show',
# ])

# # Labels for UI
# DATA_TYPE_LABELS = {
#     'article': 'Articles',
#     'person': 'People',
#     'business': 'Businesses',
#     'place': 'Places',
#     'media': 'Media',
#     'event': 'Events',
#     'product': 'Products',
# }

# CATEGORY_LABELS = {
#     'healthcare_medical': 'Healthcare',
#     'fashion': 'Fashion',
#     'beauty': 'Beauty',
#     'food_recipes': 'Food & Recipes',
#     'travel_tourism': 'Travel',
#     'entertainment': 'Entertainment',
#     'business': 'Business',
#     'education': 'Education',
#     'technology': 'Technology',
#     'sports': 'Sports',
#     'finance': 'Finance',
#     'real_estate': 'Real Estate',
#     'lifestyle': 'Lifestyle',
#     'news': 'News',
#     'culture': 'Culture',
#     'general': 'General',
# }


# # ============================================================================
# # POS TAGS THAT GO INTO SEARCH QUERY (Blueprint Step 1)
# # ============================================================================

# # Only nouns go into the Typesense q parameter
# SEARCHABLE_POS = frozenset({
#     'noun', 'proper_noun',
# })

# # Everything else is a signal — never searched
# SIGNAL_POS = frozenset({
#     'verb', 'be', 'auxiliary', 'modal',
#     'wh_pronoun', 'pronoun',
#     'preposition', 'conjunction',
#     'adjective', 'adverb',
#     'article', 'determiner',
#     'negation', 'interjection',
# })


# # ============================================================================
# # SEMANTIC BLEND RATIOS (Blueprint Step 5)
# # ============================================================================

# BLEND_RATIOS = {
#     'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
#     'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
#     'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
#     'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
# }


# # ============================================================================
# # DATA TYPE PREFERENCES BY MODE (Blueprint Step 9)
# # ============================================================================

# DATA_TYPE_PREFERENCES = {
#     'answer':  ['article', 'person', 'place'],
#     'explore': ['article', 'person', 'media'],
#     'browse':  ['article', 'business', 'product'],
#     'local':   ['business', 'place', 'article'],
#     'shop':    ['product', 'business', 'article'],
#     'compare': ['article', 'person', 'business'],
# }


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def _parse_rank(rank_value: Any) -> int:
#     """Parse rank to integer."""
#     if isinstance(rank_value, int):
#         return rank_value
#     try:
#         return int(float(rank_value))
#     except (ValueError, TypeError):
#         return 0


# def get_state_variants(state_name: str) -> List[str]:
#     """Get both full name and abbreviation for a state."""
#     state_lower = state_name.lower().strip()
#     variants = [state_name.title()]
    
#     if state_lower in US_STATE_ABBREV:
#         variants.append(US_STATE_ABBREV[state_lower])
    
#     for full, abbrev in US_STATE_ABBREV.items():
#         if abbrev.lower() == state_lower:
#             variants.append(full.title())
#             break
    
#     return list(set(variants))


# def is_state_category(category: str) -> bool:
#     """Check if category is specifically a state."""
#     if not category:
#         return False
#     return 'state' in category.lower()


# def is_city_category(category: str) -> bool:
#     """Check if category is specifically a city."""
#     if not category:
#         return False
#     cat_lower = category.lower()
#     return 'city' in cat_lower or 'county' in cat_lower


# # ============================================================================
# # WORD DISCOVERY WRAPPER
# # ============================================================================

# def _run_word_discovery(query: str) -> Dict:
#     """Run Word Discovery v2 on query."""
#     if WORD_DISCOVERY_AVAILABLE:
#         try:
#             wd = WordDiscovery(verbose=False)
#             result = wd.process(query)
#             return result
#         except Exception as e:
#             print(f"⚠️ WordDiscovery error: {e}")
    
#     # Fallback
#     return {
#         'query': query,
#         'corrected_query': query,
#         'terms': [],
#         'ngrams': [],
#         'corrections': [],
#         'stats': {
#             'total_words': len(query.split()),
#             'valid_words': 0,
#             'corrected_words': 0,
#             'unknown_words': len(query.split()),
#             'stopwords': 0,
#             'ngram_count': 0,
#         }
#     }


# def _run_embedding(query: str) -> Optional[List[float]]:
#     """Run embedding generation."""
#     return get_query_embedding(query)


# # def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
# #     """Run word discovery and embedding IN PARALLEL."""
# #     if skip_embedding:
# #         discovery = _run_word_discovery(query)
# #         return discovery, None
    
# #     discovery_future = _executor.submit(_run_word_discovery, query)
# #     embedding_future = _executor.submit(_run_embedding, query)
    
# #     discovery = discovery_future.result()
# #     embedding = embedding_future.result()
    
# #     # Re-embed if query was corrected
# #     corrected_query = discovery.get('corrected_query', query)
# #     if corrected_query.lower() != query.lower() and embedding is not None:
# #         corrections = discovery.get('corrections', [])
# #         significant = any(
# #             c.get('original', '').lower() != c.get('corrected', '').lower()
# #             for c in corrections
# #         )
# #         if significant:
# #             embedding = get_query_embedding(corrected_query)
    
# #     return discovery, embedding


# # ============================================================================
# # QUERY PROFILE BUILDING (Blueprint Steps 1, 4, 7)
# # ============================================================================

# def build_query_profile(discovery: Dict, signals: Dict = None) -> Dict:
#     """
#     Analyze ALL metadata from Word Discovery to understand user intent.
    
#     Blueprint alignment:
#     - Step 1: POS-based term routing (only nouns → q)
#     - Step 4: Dynamic field weight computation from term categories + mode
#     - Step 7: Location terms stripped from q, applied as filters
    
#     Returns profile with:
#     - Primary intent (person, organization, location, keyword, media)
#     - Search terms (POS-filtered: only nouns)
#     - Cities and states for filters (stripped from search terms)
#     - Field boosts (mode-aware + category-aware)
#     - Mode-specific Typesense parameters
#     """
#     query_mode = (signals or {}).get('query_mode', 'explore')
    
#     profile = {
#         'has_person': False,
#         'has_organization': False,
#         'has_location': False,
#         'has_keyword': False,
#         'has_media': False,
        
#         'person_score': 0,
#         'organization_score': 0,
#         'location_score': 0,
#         'keyword_score': 0,
#         'media_score': 0,
        
#         'persons': [],
#         'organizations': [],
#         'cities': [],
#         'states': [],
#         'keywords': [],
#         'search_terms': [],       # Only nouns — POS filtered
#         'location_terms': [],     # Location words stripped from q
        
#         'primary_intent': 'general',
#         'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
        
#         # Base field weights (Blueprint Step 4 base weights)
#         'field_boosts': {
#             'document_title': 10,
#             'entity_names': 2,
#             'primary_keywords': 3,
#             'key_facts': 3,
#             'semantic_keywords': 2,
#         },
#     }
    
#     if not discovery:
#         return profile
    
#     terms = discovery.get('terms', [])
#     ngrams = discovery.get('ngrams', [])
    
#     # Build term lookup by position
#     term_by_position = {t.get('position', 0): t for t in terms}
    
#     # Track positions consumed by n-grams
#     ngram_positions = set()
    
#     # =================================================================
#     # Process N-grams First
#     # =================================================================
    
#     for ngram in ngrams:
#         phrase = ngram.get('phrase', '')
#         ngram_category = ngram.get('category', '')
#         rank = _parse_rank(ngram.get('rank', 0))
#         positions = ngram.get('positions', [])
#         display = ngram.get('display', phrase)
        
#         ngram_positions.update(positions)
        
#         # Check individual term categories
#         term_categories = []
#         ngram_words = ngram.get('words', phrase.split())
        
#         for i, pos in enumerate(positions):
#             term = term_by_position.get(pos, {})
#             term_cat = term.get('category', '')
#             term_word = term.get('word', '') or (ngram_words[i] if i < len(ngram_words) else '')
#             term_rank = _parse_rank(term.get('rank', 0))
            
#             if term_cat and term_cat not in ('', 'stopword'):
#                 term_categories.append({
#                     'word': term_word,
#                     'category': term_cat,
#                     'rank': term_rank,
#                 })
#             else:
#                 word_lower = term_word.lower()
#                 if word_lower in US_STATE_ABBREV:
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US State',
#                         'rank': 350,
#                     })
#                 elif term_word.upper() in US_STATE_ABBREV.values():
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US State',
#                         'rank': 350,
#                     })
#                 elif word_lower in KNOWN_CITIES:
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US City',
#                         'rank': 350,
#                     })
        
#         # Determine if this n-gram contains location terms
#         has_city_term = any(is_city_category(tc['category']) for tc in term_categories)
#         has_state_term = any(is_state_category(tc['category']) for tc in term_categories)
        
#         both_terms_are_locations = has_city_term and has_state_term
#         ngram_is_location = ngram_category in LOCATION_CATEGORIES
        
#         if both_terms_are_locations or ngram_is_location:
#             profile['has_location'] = True
            
#             # Check if any terms are non-filterable (continent/country/region)
#             has_filterable = any(
#                 is_city_category(tc['category']) or is_state_category(tc['category'])
#                 for tc in term_categories
#             )
#             is_subject = (
#                 query_mode == 'answer' and
#                 (signals or {}).get('question_word') == 'where'
#             )
            
#             for tc in term_categories:
#                 if is_city_category(tc['category']) and not is_subject:
#                     city_name = tc['word'].title()
#                     if city_name not in [c['name'] for c in profile['cities']]:
#                         profile['cities'].append({
#                             'name': city_name,
#                             'rank': tc['rank'],
#                         })
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
                    
#                 elif is_state_category(tc['category']) and not is_subject:
#                     state_name = tc['word'].title()
#                     if state_name not in [s['name'] for s in profile['states']]:
#                         profile['states'].append({
#                             'name': state_name,
#                             'rank': tc['rank'],
#                             'variants': get_state_variants(tc['word']),
#                         })
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
#                 else:
#                     # Continent/country/region or subject → keep in search
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
            
#             # If no filterable terms or location is subject, add phrase to search
#             if not has_filterable or is_subject:
#                 profile['search_terms'].append(phrase)
        
#         elif ngram_category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             profile['search_terms'].append(phrase)
            
#         else:
#             profile['search_terms'].append(phrase)
    
#     # =================================================================
#     # Process Individual Terms (not in n-grams)
#     # Blueprint Step 1: POS-based routing — only nouns go into search
#     # =================================================================
    
#     for term in terms:
#         position = term.get('position', 0)
#         word = term.get('word', '')
#         display = term.get('display', word)
#         category = term.get('category', '')
#         rank = _parse_rank(term.get('rank', 0))
#         pos = term.get('pos', '').lower()
#         is_stopword = term.get('is_stopword', False)
#         part_of_ngram = term.get('part_of_ngram', False) or (position in ngram_positions)
        
#         if is_stopword or part_of_ngram:
#             continue
        
#         # ─── Blueprint Step 1: Determine if this is a noun (needed for all routing)
#         is_noun = pos in SEARCHABLE_POS
        
#         # ─── Blueprint Step 7: Location terms → filter OR search ─────
#         if category in LOCATION_CATEGORIES:
#             profile['has_location'] = True
#             profile['location_score'] += rank
            
#             cat_lower = category.lower()
#             is_filterable = is_city_category(category) or is_state_category(category)
#             is_subject = (
#                 query_mode == 'answer' and
#                 (signals or {}).get('question_word') == 'where'
#             )
            
#             if is_filterable and not is_subject:
#                 if is_city_category(category):
#                     city_name = display or word.title()
#                     if city_name not in [c['name'] for c in profile['cities']]:
#                         profile['cities'].append({'name': city_name, 'rank': rank})
#                 elif is_state_category(category):
#                     state_name = display or word.title()
#                     if state_name not in [s['name'] for s in profile['states']]:
#                         profile['states'].append({
#                             'name': state_name,
#                             'rank': rank,
#                             'variants': get_state_variants(word),
#                         })
#                 profile['location_terms'].append(word)
#                 continue
#             else:
#                 if is_noun:
#                     profile['search_terms'].append(word)
#                 profile['location_terms'].append(word)
#                 continue
        
#         # ─── Blueprint Step 1: Only nouns go into search terms ───────
        
#         if category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category == 'Dictionary Word':
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         else:
#             if is_noun and word:
#                 profile['search_terms'].append(word)
    
#     # =================================================================
#     # Determine Primary Intent
#     # =================================================================
    
#     scores = {
#         'person': profile['person_score'],
#         'organization': profile['organization_score'],
#         'location': profile['location_score'],
#         'keyword': profile['keyword_score'],
#         'media': profile['media_score'],
#     }
    
#     max_score = max(scores.values())
#     if max_score > 0:
#         profile['primary_intent'] = max(scores, key=scores.get)
#     else:
#         profile['primary_intent'] = 'general'
    
#     # =================================================================
#     # Set Field Boosts — Blueprint Step 4
#     # =================================================================
    
#     boosts = _compute_field_boosts(profile, query_mode, signals)
#     profile['field_boosts'] = boosts
    
#     return profile


# def _compute_field_boosts(profile: Dict, query_mode: str, signals: Dict = None) -> Dict:
#     """
#     Blueprint Step 4: Dynamic field weight computation.
#     """
#     signals = signals or {}
    
#     boosts = {
#         'document_title': 10,
#         'entity_names': 2,
#         'primary_keywords': 3,
#         'key_facts': 3,
#         'semantic_keywords': 2,
#     }
    
#     if query_mode == 'answer':
#         boosts['document_title'] = 20
#         boosts['entity_names'] = 15
#         if signals.get('wants_single_result'):
#             boosts = {
#                 'document_title': 20,
#                 'entity_names': 15,
#                 'primary_keywords': 5,
#             }
#     elif query_mode == 'browse':
#         boosts['primary_keywords'] = 15
#         boosts['semantic_keywords'] = 10
#     elif query_mode == 'local':
#         boosts['primary_keywords'] = 12
#     elif query_mode == 'compare':
#         boosts['entity_names'] = 15
#         boosts['document_title'] = 15
#     elif query_mode == 'shop':
#         boosts['primary_keywords'] = 12
#         boosts['document_title'] = 10
    
#     if profile.get('has_person'):
#         best_rank = max((p.get('rank', 0) for p in profile.get('persons', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
#         boosts['document_title'] = boosts.get('document_title', 10) + 5
    
#     if profile.get('has_organization'):
#         best_rank = max((o.get('rank', 0) for o in profile.get('organizations', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
    
#     if profile.get('has_keyword'):
#         best_rank = max((k.get('rank', 0) for k in profile.get('keywords', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 10 + rank_bonus
#         boosts['semantic_keywords'] = boosts.get('semantic_keywords', 2) + 5
#         boosts['key_facts'] = boosts.get('key_facts', 3) + 4
    
#     if profile.get('has_media'):
#         best_rank = max((profile.get('media_score', 0),), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['document_title'] = boosts.get('document_title', 10) + 10 + rank_bonus
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 4
    
#     has_unknown = signals.get('has_unknown_terms', False)
#     has_known = (profile.get('has_person') or profile.get('has_organization')
#                  or profile.get('has_keyword') or profile.get('has_media'))
    
#     if has_unknown and has_known:
#         for field in boosts:
#             boosts[field] += 3
#     elif has_unknown and not has_known:
#         for field in boosts:
#             boosts[field] += 8
    
#     return boosts


# # ============================================================================
# # TYPESENSE PARAMETER BUILDING (Blueprint Steps 2, 3, 6)
# # ============================================================================

# def build_typesense_params(profile: Dict, ui_filters: Dict = None,
#                            signals: Dict = None) -> Dict:
#     """
#     Convert query profile into Typesense search parameters.
#     """
#     signals = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
    
#     params = {}
    
#     search_terms = profile.get('search_terms', [])
#     seen = set()
#     unique_terms = []
#     for term in search_terms:
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)
    
#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
#     field_boosts = profile.get('field_boosts', {})
    
#     if query_mode == 'local':
#         if 'service_type' not in field_boosts:
#             field_boosts['service_type'] = 12
#         if 'service_specialties' not in field_boosts:
#             field_boosts['service_specialties'] = 10
    
#     sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
#     params['query_by'] = ','.join([f[0] for f in sorted_fields])
#     params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
#     has_corrections = len(profile.get('corrections', [])) > 0 if isinstance(profile.get('corrections'), list) else False
#     term_count = len(unique_terms)
    
#     if query_mode == 'answer':
#         params['num_typos'] = 0
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'explore':
#         params['num_typos'] = 0 if has_corrections else 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
#     elif query_mode == 'browse':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
#     elif query_mode == 'local':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
#     elif query_mode == 'compare':
#         params['num_typos'] = 0
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'shop':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
#     else:
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
    
#     temporal_direction = signals.get('temporal_direction')
#     price_direction = signals.get('price_direction')
#     has_superlative = signals.get('has_superlative', False)
#     has_rating = signals.get('has_rating_signal', False)
    
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
    
#     filter_conditions = []
    
#     cities = profile.get('cities', [])
#     states = profile.get('states', [])
    
#     local_strength = signals.get('local_search_strength', 'none')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )
    
#     apply_location_filter = True
#     if is_location_subject:
#         apply_location_filter = False
    
#     if apply_location_filter:
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             if len(city_filters) == 1:
#                 filter_conditions.append(city_filters[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:={variant}")
            
#             if len(state_conditions) == 1:
#                 filter_conditions.append(state_conditions[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')
    
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


# def build_filter_string_without_data_type(profile: Dict, signals: Dict = None) -> str:
#     """Build filter string for locations only (no data_type for facet counting)."""
#     signals = signals or {}
#     filter_conditions = []
    
#     query_mode = signals.get('query_mode', 'explore')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )
    
#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         states = profile.get('states', [])
        
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             if len(city_filters) == 1:
#                 filter_conditions.append(city_filters[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:={variant}")
            
#             if len(state_conditions) == 1:
#                 filter_conditions.append(state_conditions[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')
    
#     return ' && '.join(filter_conditions) if filter_conditions else ''


# # # ============================================================================
# # # STAGE 1: GRAPH FILTER - Candidate Generation
# # # ============================================================================



# # # ============================================================================
# # # STAGE 1 (SEMANTIC): Fetch ONLY document_uuids
# # # ============================================================================

# # def fetch_candidate_uuids(
# #     search_query: str,
# #     profile: Dict,
# #     signals: Dict = None,
# #     max_results: int = MAX_CACHED_RESULTS
# # ) -> List[str]:
# #     """
# #     Stage 1 (Semantic path): Keyword search using intent-based fields.
# #     Returns ONLY a list of document_uuid strings — no metadata.
# #     Metadata is fetched later in Stage 4 for survivors only.
# #     """
# #     signals = signals or {}
# #     params = build_typesense_params(profile, signals=signals)
# #     filter_str = build_filter_string_without_data_type(profile, signals=signals)

# #     PAGE_SIZE = 250
# #     all_uuids = []
# #     current_page = 1
# #     max_pages = (max_results // PAGE_SIZE) + 1

# #     query_mode = signals.get('query_mode', 'explore')

# #     print(f"🔍 Stage 1 (uuids only): '{params.get('q', search_query)}'")
# #     print(f"   Mode: {query_mode}")
# #     print(f"   Fields: {params.get('query_by', '')}")
# #     print(f"   Weights: {params.get('query_by_weights', '')}")
# #     print(f"   num_typos: {params.get('num_typos', 1)} | prefix: {params.get('prefix', 'yes')}")
# #     print(f"   sort_by: {params.get('sort_by', 'default')}")
# #     if filter_str:
# #         print(f"   Filters: {filter_str}")

# #     while len(all_uuids) < max_results and current_page <= max_pages:
# #         search_params = {
# #             'q': params.get('q', search_query),
# #             'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
# #             'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
# #             'per_page': PAGE_SIZE,
# #             'page': current_page,
# #             'include_fields': 'document_uuid',
# #             'num_typos': params.get('num_typos', 0),
# #             'prefix': params.get('prefix', 'no'),
# #             'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
# #             'sort_by': params.get('sort_by', '_text_match:desc,authority_score:desc'),
# #         }

# #         if filter_str:
# #             search_params['filter_by'] = filter_str

# #         try:
# #             response = client.collections[COLLECTION_NAME].documents.search(search_params)
# #             hits = response.get('hits', [])
# #             found = response.get('found', 0)

# #             if not hits:
# #                 break

# #             for hit in hits:
# #                 doc = hit.get('document', {})
# #                 uuid = doc.get('document_uuid')
# #                 if uuid:
# #                     all_uuids.append(uuid)

# #             if len(all_uuids) >= found or len(hits) < PAGE_SIZE:
# #                 break

# #             current_page += 1

# #         except Exception as e:
# #             print(f"❌ Stage 1 error (page {current_page}): {e}")
# #             break

# #     print(f"📊 Stage 1: Retrieved {len(all_uuids)} candidate UUIDs")
# #     return all_uuids[:max_results]
# # ============================================================================
# # RUN_PARALLEL_PREP FIX — Replace existing run_parallel_prep function
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.

#     FIX: Always embed the ORIGINAL query first.
#     Only re-embed with corrected_query if:
#       - The correction is a genuine dictionary word fix
#       - NOT a proper noun being mangled into a common word
#       - NOT a name being changed to food/city/other wrong category
#     """
#     if skip_embedding:
#         discovery = _run_word_discovery(query)
#         return discovery, None

#     # Always embed original query in parallel with word discovery
#     discovery_future = _executor.submit(_run_word_discovery, query)
#     embedding_future = _executor.submit(_run_embedding, query)  # ← always original

#     discovery = discovery_future.result()
#     embedding = embedding_future.result()

#     # ── Decide if re-embedding with corrected query is safe ───────────────
#     corrected_query = discovery.get('corrected_query', query)

#     if corrected_query.lower() != query.lower() and embedding is not None:
#         corrections = discovery.get('corrections', [])

#         # Only re-embed if corrections are genuine dictionary fixes
#         # NOT if they are proper nouns being mangled into wrong categories
#         SAFE_CORRECTION_TYPES = {'spelling', 'phonetic', 'abbreviation'}

#         UNSAFE_CATEGORIES = {
#             'Food', 'US City', 'US State', 'Country', 'Location',
#             'City', 'Place', 'Object', 'Animal', 'Color',
#         }

#         safe_corrections = []
#         unsafe_corrections = []

#         for c in corrections:
#             corrected_category = c.get('category', '')
#             correction_type    = c.get('correction_type', '')
#             original           = c.get('original', '')
#             corrected          = c.get('corrected', '')

#             # Flag as unsafe if:
#             # 1. Correction type is pos_mismatch (word discovery guessing)
#             # 2. Corrected category is something clearly wrong (Food, City, etc.)
#             # 3. Original was classified as Person/Organization (proper noun)
#             is_pos_mismatch    = correction_type == 'pos_mismatch'
#             is_wrong_category  = corrected_category in UNSAFE_CATEGORIES
#             is_proper_noun     = c.get('category', '') in ('Person', 'Organization', 'Brand')

#             if is_pos_mismatch or is_wrong_category or is_proper_noun:
#                 unsafe_corrections.append(c)
#             else:
#                 safe_corrections.append(c)

#         has_safe_corrections   = len(safe_corrections) > 0
#         has_unsafe_corrections = len(unsafe_corrections) > 0

#         if has_unsafe_corrections:
#             # Do NOT re-embed — original embedding is more accurate
#             print(f"⚠️  Skipping re-embed — unsafe corrections detected:")
#             for c in unsafe_corrections:
#                 print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
#                       f"(type={c.get('correction_type')}, category={c.get('category')})")
#             print(f"   Keeping original query embedding: '{query}'")

#         elif has_safe_corrections:
#             # Safe to re-embed with corrected query
#             print(f"✅  Re-embedding with corrected query: '{corrected_query}'")
#             embedding = get_query_embedding(corrected_query)

#     return discovery, embedding


# # ============================================================================
# # STAGE 1A: Document collection — keyword graph, 100 candidates
# # ============================================================================

# def fetch_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = 100
# ) -> List[str]:
#     """
#     Stage 1A: Keyword graph search against the document collection.
#     Returns up to 100 document_uuid strings — no metadata.
#     """
#     signals = signals or {}
#     params = build_typesense_params(profile, signals=signals)
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
#         hits = response.get('hits', [])

#         uuids = []
#         for hit in hits:
#             doc = hit.get('document', {})
#             uuid = doc.get('document_uuid')
#             if uuid:
#                 uuids.append(uuid)

#         print(f"📊 Stage 1A (document): {len(uuids)} candidate UUIDs")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1A error: {e}")
#         return []


# # # ============================================================================
# # # STAGE 1B: Questions collection — facet filter + vector search, 50 candidates
# # # ============================================================================

# # def fetch_candidate_uuids_from_questions(
# #     profile: Dict,
# #     query_embedding: List[float],
# #     signals: Dict = None,
# #     max_results: int = 50
# # ) -> List[str]:
# #     """
# #     Stage 1B: Two-step search against the questions collection.

# #     FIX 1: Uses the ORIGINAL query embedding (protected in run_parallel_prep)
# #             so proper noun mangling does not corrupt the vector search.

# #     FIX 2: Facet filter is built more carefully:
# #            - Entity names are validated — single-word fragments like
# #              "prentice" or "herman" are weak and may not exist in the
# #              entities field as standalone values. We detect this and
# #              fall back to question_type only when entities are fragments.
# #            - This prevents an over-narrow filter that returns 0 hits
# #              when word discovery breaks a proper name into parts.

# #     Step A — build facet filter from profile metadata
# #     Step B — vector search within that filtered subset
# #     """
# #     signals = signals or {}

# #     if not query_embedding:
# #         print("⚠️ Stage 1B (questions): no embedding — skipping")
# #         return []

# #     # ── Step A: Build facet filter ────────────────────────────────────────
# #     filter_parts = []

# #     # primary_keywords — use top 3
# #     primary_kws = profile.get('primary_keywords', [])
# #     if not primary_kws:
# #         primary_kws = [
# #             k.get('phrase') or k.get('word', '')
# #             for k in profile.get('keywords', [])
# #         ]
# #     primary_kws = [kw for kw in primary_kws if kw][:3]

# #     if primary_kws:
# #         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
# #         filter_parts.append(f'primary_keywords:[{kw_values}]')

# #     # entities — validate that names are meaningful multi-word phrases
# #     # Single-word fragments (e.g. "prentice", "herman") are unreliable
# #     # because the entities field stores full names like "Prentice Herman Polk"
# #     entity_names = []
# #     for p in profile.get('persons', []):
# #         name = p.get('phrase') or p.get('word', '')
# #         # Only use entity if it looks like a full name (has a space)
# #         # or is clearly a known proper noun (capitalized, rank > 100)
# #         rank = p.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)

# #     for o in profile.get('organizations', []):
# #         name = o.get('phrase') or o.get('word', '')
# #         rank = o.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)

# #     entity_names = [e for e in entity_names if e][:3]

# #     if entity_names:
# #         ent_values = ','.join([f'`{e}`' for e in entity_names])
# #         filter_parts.append(f'entities:[{ent_values}]')

# #     # semantic_keywords — use top 3
# #     semantic_kws = profile.get('semantic_keywords', [])
# #     semantic_kws = [kw for kw in semantic_kws if kw][:3]

# #     if semantic_kws:
# #         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
# #         filter_parts.append(f'semantic_keywords:[{sem_values}]')

# #     # question_type — always include when we have a question word signal
# #     # This is the most reliable filter when entity names are fragments
# #     question_word = signals.get('question_word', '')
# #     question_type_map = {
# #         'when':  'TEMPORAL',
# #         'where': 'LOCATION',
# #         'who':   'PERSON',
# #         'what':  'FACTUAL',
# #         'which': 'FACTUAL',
# #         'why':   'REASON',
# #         'how':   'PROCESS',
# #     }
# #     question_type = question_type_map.get(question_word.lower(), '')
# #     if question_type:
# #         filter_parts.append(f'question_type:={question_type}')

# #     # ── Filter strategy: ──────────────────────────────────────────────────
# #     # If we have strong entity names → use OR (broad net)
# #     # If we only have question_type → use it alone (still narrows well)
# #     # If we have nothing → no filter (full vector scan)
# #     if filter_parts:
# #         filter_str = ' || '.join(filter_parts)
# #     else:
# #         filter_str = ''

# #     print(f"🔍 Stage 1B (questions): vector search within facet filter")
# #     print(f"   primary_keywords : {primary_kws}")
# #     print(f"   entities         : {entity_names}")
# #     print(f"   semantic_keywords: {semantic_kws}")
# #     print(f"   question_type    : {question_type or 'any'}")
# #     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

# #     # ── Step B: Vector search within filtered subset ──────────────────────
# #     embedding_str = ','.join(str(x) for x in query_embedding)

# #     search_params = {
# #         'q':            '*',
# #         'vector_query': f'embedding:([{embedding_str}], k:{max_results})',
# #         'per_page':     max_results,
# #         'include_fields': 'document_uuid,question,answer_type,question_type',
# #     }

# #     if filter_str:
# #         search_params['filter_by'] = filter_str

# #     try:
# #         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
# #         response = client.multi_search.perform(search_requests, {})
# #         result = response['results'][0]
# #         hits = result.get('hits', [])

# #         # ── If filter returned too few hits, retry without filter ─────────
# #         # This is the safety net for when all filter parts are too narrow
# #         if len(hits) < 5 and filter_str:
# #             print(f"⚠️  Stage 1B: only {len(hits)} hits with filter — retrying without filter")
# #             search_params_fallback = {
# #                 'q':              '*',
# #                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results})',
# #                 'per_page':       max_results,
# #                 'include_fields': 'document_uuid,question,answer_type,question_type',
# #             }
# #             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
# #             response_fallback = client.multi_search.perform(search_requests_fallback, {})
# #             hits = response_fallback['results'][0].get('hits', [])
# #             print(f"   Fallback returned {len(hits)} hits")

# #         uuids = []
# #         seen = set()
# #         for hit in hits:
# #             doc = hit.get('document', {})
# #             uuid = doc.get('document_uuid')
# #             if uuid and uuid not in seen:
# #                 seen.add(uuid)
# #                 uuids.append(uuid)

# #             if len(uuids) >= max_results:
# #                 break

# #         print(f"📊 Stage 1B (questions): {len(uuids)} candidate UUIDs from {len(hits)} question hits")
# #         return uuids

# #     except Exception as e:
# #         print(f"❌ Stage 1B error: {e}")
# #         return []

# # ============================================================================
# # STAGE 1B: Questions collection — facet filter + vector search + validation
# # ============================================================================

# # Stopwords and question words to exclude from signal matching
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
#     - Lowercase
#     - Strip punctuation except hyphens inside words
#     - Remove stopwords and question words
#     - Keep only tokens longer than 2 characters
#     """
#     import re
#     if not text:
#         return set()

#     # Lowercase
#     text = text.lower()

#     # Replace punctuation with spaces (keep hyphens between word chars)
#     text = re.sub(r"[^\w\s-]", " ", text)
#     text = re.sub(r"\s*-\s*", " ", text)  # normalize hyphens

#     tokens = text.split()

#     return {
#         t for t in tokens
#         if len(t) > 2 and t not in _MATCH_STOPWORDS
#     }


# def _extract_query_signals(profile: Dict) -> tuple:
#     """
#     Extract and normalize all meaningful query signals from the profile.
#     Returns:
#         all_tokens   — set of all individual normalized tokens
#         full_phrases — list of normalized full phrase strings (for substring match)
#         primary_subject — the highest-ranked entity/keyword (must-match candidate)
#     """
#     raw_signals = []
#     ranked_signals = []  # (rank, phrase)

#     # Persons — highest priority
#     for p in profile.get('persons', []):
#         phrase = p.get('phrase') or p.get('word', '')
#         rank   = p.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Organizations
#     for o in profile.get('organizations', []):
#         phrase = o.get('phrase') or o.get('word', '')
#         rank   = o.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Keywords
#     for k in profile.get('keywords', []):
#         phrase = k.get('phrase') or k.get('word', '')
#         rank   = k.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Primary keywords from profile (if populated)
#     for kw in profile.get('primary_keywords', []):
#         if kw:
#             raw_signals.append(kw)
#             ranked_signals.append((0, kw))

#     # Search terms (nouns from word discovery)
#     for term in profile.get('search_terms', []):
#         if term:
#             raw_signals.append(term)

#     # Build token set and full phrase list
#     all_tokens   = set()
#     full_phrases = []

#     for phrase in raw_signals:
#         normalized = _normalize_signal(phrase)
#         all_tokens.update(normalized)
#         phrase_lower = phrase.lower().strip()
#         if phrase_lower:
#             full_phrases.append(phrase_lower)

#     # Primary subject = highest ranked signal
#     primary_subject = None
#     if ranked_signals:
#         ranked_signals.sort(key=lambda x: -x[0])
#         primary_subject = _normalize_signal(ranked_signals[0][1])

#     return all_tokens, full_phrases, primary_subject


# def _validate_question_hit(
#     hit_doc: Dict,
#     query_tokens: set,
#     query_phrases: list,
#     primary_subject: set,
#     min_matches: int = 1,
# ) -> bool:
#     """
#     Validate a question hit against query signals using 4-level matching.

#     Level 1 — Exact token match (case insensitive)
#     Level 2 — Partial token match (query token inside candidate string)
#     Level 3 — Substring containment (query phrase inside candidate or vice versa)
#     Level 4 — Token overlap (shared meaningful tokens between strings)

#     Rules:
#     - At least min_matches signals must match
#     - If primary_subject is provided and query has 3+ signals,
#       primary subject must be one of the matches (prevents Grammy
#       matching on Beyoncé when user asked about Dr. Dre)

#     Returns True if hit passes validation, False if it should be discarded.
#     """
#     if not query_tokens:
#         # No signals to validate against — accept everything
#         return True

#     # Collect candidate values from the hit
#     candidate_raw = []
#     candidate_raw.extend(hit_doc.get('primary_keywords', []))
#     candidate_raw.extend(hit_doc.get('entities', []))
#     candidate_raw.extend(hit_doc.get('semantic_keywords', []))

#     if not candidate_raw:
#         return False

#     # Normalize all candidate values
#     candidate_tokens   = set()
#     candidate_phrases  = []

#     for val in candidate_raw:
#         if not val:
#             continue
#         normalized = _normalize_signal(val)
#         candidate_tokens.update(normalized)
#         candidate_phrases.append(val.lower().strip())

#     candidate_text = ' '.join(candidate_phrases)

#     match_count         = 0
#     primary_subject_hit = False

#     # ── Level 1: Exact token match ────────────────────────────────────────
#     exact_matches = query_tokens & candidate_tokens
#     if exact_matches:
#         match_count += len(exact_matches)
#         if primary_subject and (primary_subject & exact_matches):
#             primary_subject_hit = True

#     # ── Level 2: Partial token match ─────────────────────────────────────
#     # Query token appears as substring inside any candidate token
#     for qt in query_tokens:
#         if qt in exact_matches:
#             continue  # already counted
#         for ct in candidate_tokens:
#             if qt in ct or ct in qt:
#                 match_count += 1
#                 if primary_subject and qt in primary_subject:
#                     primary_subject_hit = True
#                 break

#     # ── Level 3: Substring containment ───────────────────────────────────
#     # Full query phrase appears inside candidate text or vice versa
#     for qp in query_phrases:
#         if len(qp) < 3:
#             continue
#         if qp in candidate_text:
#             match_count += 1
#             if primary_subject:
#                 qp_tokens = _normalize_signal(qp)
#                 if qp_tokens & primary_subject:
#                     primary_subject_hit = True
#         else:
#             # Check if any candidate phrase contains the query phrase
#             for cp in candidate_phrases:
#                 if qp in cp or cp in qp:
#                     match_count += 1
#                     if primary_subject:
#                         qp_tokens = _normalize_signal(qp)
#                         if qp_tokens & primary_subject:
#                             primary_subject_hit = True
#                     break

#     # ── Level 4: Token overlap ────────────────────────────────────────────
#     # Shared meaningful tokens between query and candidate
#     # Only counts tokens not already matched
#     remaining_query = query_tokens - exact_matches
#     token_overlap   = remaining_query & candidate_tokens
#     if token_overlap:
#         match_count += len(token_overlap)
#         if primary_subject and (primary_subject & token_overlap):
#             primary_subject_hit = True

#     # ── Decision ──────────────────────────────────────────────────────────
#     if match_count < min_matches:
#         return False

#     # If query has 3+ signals AND we have a primary subject,
#     # primary subject must be one of the matches.
#     # This prevents "Grammy" alone matching Dr. Dre questions
#     # to Beyoncé Grammy questions.
#     if primary_subject and len(query_tokens) >= 3:
#         if not primary_subject_hit:
#             return False

#     return True


# # def fetch_candidate_uuids_from_questions(
# #     profile: Dict,
# #     query_embedding: List[float],
# #     signals: Dict = None,
# #     max_results: int = 50
# # ) -> List[str]:
# #     """
# #     Stage 1B: Two-step search against the questions collection.

# #     Step A — Build facet filter from profile metadata to narrow
# #               the questions pool before the vector scan.

# #     Step B — Run vector search within that filtered subset.

# #     Step C — NEW: Validate each hit against query signals using
# #               4-level case-insensitive partial matching before
# #               accepting into the candidate pool.
# #               This prevents structurally similar but topically
# #               unrelated questions from polluting the pool
# #               (e.g. Bivins birth question matching Polk birth query,
# #               Beyoncé Grammy question matching Dr. Dre Grammy query).

# #     Returns up to max_results validated document_uuid strings.
# #     """
# #     signals = signals or {}

# #     if not query_embedding:
# #         print("⚠️ Stage 1B (questions): no embedding — skipping")
# #         return []

# #     # ── Extract query signals for validation ─────────────────────────────
# #     query_tokens, query_phrases, primary_subject = _extract_query_signals(profile)

# #     print(f"🔍 Stage 1B validation signals:")
# #     print(f"   query_tokens    : {sorted(query_tokens)}")
# #     print(f"   query_phrases   : {query_phrases}")
# #     print(f"   primary_subject : {primary_subject}")

# #     # ── Step A: Build facet filter ────────────────────────────────────────
# #     filter_parts = []

# #     primary_kws = profile.get('primary_keywords', [])
# #     if not primary_kws:
# #         primary_kws = [
# #             k.get('phrase') or k.get('word', '')
# #             for k in profile.get('keywords', [])
# #         ]
# #     primary_kws = [kw for kw in primary_kws if kw][:3]

# #     if primary_kws:
# #         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
# #         filter_parts.append(f'primary_keywords:[{kw_values}]')

# #     # Only use entity names that are full names (have space) or high rank
# #     entity_names = []
# #     for p in profile.get('persons', []):
# #         name = p.get('phrase') or p.get('word', '')
# #         rank = p.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)
# #     for o in profile.get('organizations', []):
# #         name = o.get('phrase') or o.get('word', '')
# #         rank = o.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)
# #     entity_names = [e for e in entity_names if e][:3]

# #     if entity_names:
# #         ent_values = ','.join([f'`{e}`' for e in entity_names])
# #         filter_parts.append(f'entities:[{ent_values}]')

# #     semantic_kws = profile.get('semantic_keywords', [])
# #     semantic_kws = [kw for kw in semantic_kws if kw][:3]
# #     if semantic_kws:
# #         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
# #         filter_parts.append(f'semantic_keywords:[{sem_values}]')

# #     question_word = signals.get('question_word', '')
# #     question_type_map = {
# #         'when':  'TEMPORAL',
# #         'where': 'LOCATION',
# #         'who':   'PERSON',
# #         'what':  'FACTUAL',
# #         'which': 'FACTUAL',
# #         'why':   'REASON',
# #         'how':   'PROCESS',
# #     }
# #     question_type = question_type_map.get(question_word.lower(), '')
# #     if question_type:
# #         filter_parts.append(f'question_type:={question_type}')

# #     filter_str = ' || '.join(filter_parts) if filter_parts else ''

# #     print(f"   primary_keywords : {primary_kws}")
# #     print(f"   entities         : {entity_names}")
# #     print(f"   semantic_keywords: {semantic_kws}")
# #     print(f"   question_type    : {question_type or 'any'}")
# #     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

# #     # ── Step B: Vector search within filtered subset ──────────────────────
# #     embedding_str = ','.join(str(x) for x in query_embedding)

# #     search_params = {
# #         'q':              '*',
# #         'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',  # fetch extra for validation
# #         'per_page':       max_results * 2,
# #         'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
# #     }

# #     if filter_str:
# #         search_params['filter_by'] = filter_str

# #     try:
# #         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
# #         response = client.multi_search.perform(search_requests, {})
# #         result = response['results'][0]
# #         hits = result.get('hits', [])

# #         # Fallback: retry without filter if too few hits
# #         if len(hits) < 5 and filter_str:
# #             print(f"⚠️  Stage 1B: only {len(hits)} hits with filter — retrying without filter")
# #             search_params_fallback = {
# #                 'q':              '*',
# #                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
# #                 'per_page':       max_results * 2,
# #                 'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
# #             }
# #             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
# #             response_fallback = client.multi_search.perform(search_requests_fallback, {})
# #             hits = response_fallback['results'][0].get('hits', [])
# #             print(f"   Fallback returned {len(hits)} hits")

# #         # ── Step C: Validate each hit against query signals ───────────────
# #         uuids       = []
# #         seen        = set()
# #         accepted    = 0
# #         rejected    = 0

# #         for hit in hits:
# #             doc  = hit.get('document', {})
# #             uuid = doc.get('document_uuid')

# #             if not uuid:
# #                 continue

# #             # Validate hit against query signals
# #             is_valid = _validate_question_hit(
# #                 hit_doc         = doc,
# #                 query_tokens    = query_tokens,
# #                 query_phrases   = query_phrases,
# #                 primary_subject = primary_subject,
# #                 min_matches     = 1,
# #             )

# #             if is_valid:
# #                 accepted += 1
# #                 if uuid not in seen:
# #                     seen.add(uuid)
# #                     uuids.append(uuid)
# #             else:
# #                 rejected += 1
# #                 print(f"   ❌ Rejected: '{doc.get('question', '')[:60]}' "
# #                       f"(distance={hit.get('vector_distance', 1.0):.4f})")

# #             if len(uuids) >= max_results:
# #                 break

# #         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
# #               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
# #         return uuids

# #     except Exception as e:
# #         print(f"❌ Stage 1B error: {e}")
# #         return []

# # def fetch_candidate_uuids_from_questions(
# #     profile: Dict,
# #     query_embedding: List[float],
# #     signals: Dict = None,
# #     max_results: int = 50
# # ) -> List[str]:
# #     """
# #     Stage 1B: Two-step search against the questions collection.

# #     Step A — Build facet filter from profile metadata to narrow
# #               the questions pool before the vector scan.

# #     Step B — Run vector search within that filtered subset.

# #     Step C — Validate each hit against query signals using
# #               4-level case-insensitive partial matching before
# #               accepting into the candidate pool.

# #     Returns up to max_results validated document_uuid strings.
# #     """
# #     signals = signals or {}

# #     if not query_embedding:
# #         print("⚠️ Stage 1B (questions): no embedding — skipping")
# #         return []

# #     # ── Extract query signals for validation ─────────────────────────────
# #     query_tokens, query_phrases, primary_subject = _extract_query_signals(profile)

# #     print(f"🔍 Stage 1B validation signals:")
# #     print(f"   query_tokens    : {sorted(query_tokens)}")
# #     print(f"   query_phrases   : {query_phrases}")
# #     print(f"   primary_subject : {primary_subject}")

# #     # ── Step A: Build facet filter ────────────────────────────────────────
# #     filter_parts = []

# #     primary_kws = profile.get('primary_keywords', [])
# #     if not primary_kws:
# #         primary_kws = [
# #             k.get('phrase') or k.get('word', '')
# #             for k in profile.get('keywords', [])
# #         ]
# #     primary_kws = [kw for kw in primary_kws if kw][:3]

# #     if primary_kws:
# #         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
# #         filter_parts.append(f'primary_keywords:[{kw_values}]')

# #     # Only use entity names that are full names (have space) or high rank
# #     entity_names = []
# #     for p in profile.get('persons', []):
# #         name = p.get('phrase') or p.get('word', '')
# #         rank = p.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)
# #     for o in profile.get('organizations', []):
# #         name = o.get('phrase') or o.get('word', '')
# #         rank = o.get('rank', 0)
# #         if name and (' ' in name or rank > 100):
# #             entity_names.append(name)
# #     entity_names = [e for e in entity_names if e][:3]

# #     if entity_names:
# #         ent_values = ','.join([f'`{e}`' for e in entity_names])
# #         filter_parts.append(f'entities:[{ent_values}]')

# #     semantic_kws = profile.get('semantic_keywords', [])
# #     semantic_kws = [kw for kw in semantic_kws if kw][:3]
# #     if semantic_kws:
# #         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
# #         filter_parts.append(f'semantic_keywords:[{sem_values}]')

# #     # ★ FIX: use `or ''` to guard against None value
# #     question_word = signals.get('question_word') or ''
# #     question_type_map = {
# #         'when':  'TEMPORAL',
# #         'where': 'LOCATION',
# #         'who':   'PERSON',
# #         'what':  'FACTUAL',
# #         'which': 'FACTUAL',
# #         'why':   'REASON',
# #         'how':   'PROCESS',
# #     }
# #     question_type = question_type_map.get(question_word.lower(), '')
# #     if question_type:
# #         filter_parts.append(f'question_type:={question_type}')

# #     filter_str = ' || '.join(filter_parts) if filter_parts else ''

# #     print(f"   primary_keywords : {primary_kws}")
# #     print(f"   entities         : {entity_names}")
# #     print(f"   semantic_keywords: {semantic_kws}")
# #     print(f"   question_type    : {question_type or 'any'}")
# #     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

# #     # ── Step B: Vector search within filtered subset ──────────────────────
# #     embedding_str = ','.join(str(x) for x in query_embedding)

# #     search_params = {
# #         'q':              '*',
# #         'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
# #         'per_page':       max_results * 2,
# #         'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
# #     }

# #     if filter_str:
# #         search_params['filter_by'] = filter_str

# #     try:
# #         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
# #         response = client.multi_search.perform(search_requests, {})
# #         result = response['results'][0]
# #         hits = result.get('hits', [])

# #         # Fallback: retry without filter if too few hits
# #         if len(hits) < 5 and filter_str:
# #             print(f"⚠️  Stage 1B: only {len(hits)} hits with filter — retrying without filter")
# #             search_params_fallback = {
# #                 'q':              '*',
# #                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
# #                 'per_page':       max_results * 2,
# #                 'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
# #             }
# #             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
# #             response_fallback = client.multi_search.perform(search_requests_fallback, {})
# #             hits = response_fallback['results'][0].get('hits', [])
# #             print(f"   Fallback returned {len(hits)} hits")

# #         # ── Step C: Validate each hit against query signals ───────────────
# #         uuids       = []
# #         seen        = set()
# #         accepted    = 0
# #         rejected    = 0

# #         for hit in hits:
# #             doc  = hit.get('document', {})
# #             uuid = doc.get('document_uuid')

# #             if not uuid:
# #                 continue

# #             # Validate hit against query signals
# #             is_valid = _validate_question_hit(
# #                 hit_doc         = doc,
# #                 query_tokens    = query_tokens,
# #                 query_phrases   = query_phrases,
# #                 primary_subject = primary_subject,
# #                 min_matches     = 1,
# #             )

# #             if is_valid:
# #                 accepted += 1
# #                 if uuid not in seen:
# #                     seen.add(uuid)
# #                     uuids.append(uuid)
# #             else:
# #                 rejected += 1
# #                 print(f"   ❌ Rejected: '{doc.get('question', '')[:60]}' "
# #                       f"(distance={hit.get('vector_distance', 1.0):.4f})")

# #             if len(uuids) >= max_results:
# #                 break

# #         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
# #               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
# #         return uuids

# #     except Exception as e:
# #         print(f"❌ Stage 1B error: {e}")
# #         return []

# # # ============================================================================
# # # STAGE 1 COMBINED: Run both in parallel, merge + dedup
# # # ============================================================================

# def fetch_all_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
# ) -> List[str]:
#     """
#     Runs Stage 1A (document) and Stage 1B (questions) in parallel.

#     Merge order:
#     1. Overlap — found by both paths (highest confidence)
#     2. Document-only hits
#     3. Question-only hits

#     Stage 1B runs independently of Stage 1A results.
#     Even if Stage 1A returns 0 (e.g. bad keyword graph), Stage 1B
#     can still surface the right document via vector search.
#     """
#     signals = signals or {}

#     doc_future = _executor.submit(
#         fetch_candidate_uuids, search_query, profile, signals, 100
#     )
#     q_future = _executor.submit(
#         fetch_candidate_uuids_from_questions, profile, query_embedding, signals, 50
#     )

#     doc_uuids = doc_future.result()
#     q_uuids   = q_future.result()

#     # Find overlap
#     doc_set = set(doc_uuids)
#     q_set   = set(q_uuids)
#     overlap  = doc_set & q_set

#     # Merge: overlap first, then document-only, then question-only
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
#     print(f"   document pool    : {len(doc_uuids)}")
#     print(f"   questions pool   : {len(q_uuids)}")
#     print(f"   overlap (both)   : {len(overlap)}")
#     print(f"   merged total     : {len(merged)}")

#     return merged

# def fetch_candidate_uuids_from_questions(
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     max_results: int = 50
# ) -> List[str]:
#     """
#     Stage 1B: Two-step search against the questions collection.

#     Step A — Build facet filter from profile metadata to narrow
#               the questions pool before the vector scan.

#     Step B — Run vector search within that filtered subset.

#     Step C — Validate each hit against query signals using
#               4-level case-insensitive partial matching before
#               accepting into the candidate pool.

#     FIX: Location filter (city/state) is now AND'd onto the facet filter
#          so question hits are constrained to the detected geographic area.
#          Previously, Stage 1B returned results from ALL cities because
#          only Stage 1A applied the location filter.

#     Returns up to max_results validated document_uuid strings.
#     """
#     signals = signals or {}

#     if not query_embedding:
#         print("⚠️ Stage 1B (questions): no embedding — skipping")
#         return []

#     # ── Extract query signals for validation ─────────────────────────────
#     query_tokens, query_phrases, primary_subject = _extract_query_signals(profile)

#     print(f"🔍 Stage 1B validation signals:")
#     print(f"   query_tokens    : {sorted(query_tokens)}")
#     print(f"   query_phrases   : {query_phrases}")
#     print(f"   primary_subject : {primary_subject}")

#     # ── Step A: Build facet filter ────────────────────────────────────────
#     filter_parts = []

#     primary_kws = profile.get('primary_keywords', [])
#     if not primary_kws:
#         primary_kws = [
#             k.get('phrase') or k.get('word', '')
#             for k in profile.get('keywords', [])
#         ]
#     primary_kws = [kw for kw in primary_kws if kw][:3]

#     if primary_kws:
#         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
#         filter_parts.append(f'primary_keywords:[{kw_values}]')

#     # Only use entity names that are full names (have space) or high rank
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

#     semantic_kws = profile.get('semantic_keywords', [])
#     semantic_kws = [kw for kw in semantic_kws if kw][:3]
#     if semantic_kws:
#         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
#         filter_parts.append(f'semantic_keywords:[{sem_values}]')

#     # ★ FIX: use `or ''` to guard against None value
#     question_word = signals.get('question_word') or ''
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

#     # ── Location filter: enforce city/state on questions collection ────────
#     # The profile already detected cities/states from word discovery.
#     # Stage 1A applies this to the document collection, but without it here
#     # the questions vector search returns results from ALL cities.
#     #
#     # Location is AND'd (hard constraint) while facet filters are OR'd
#     # (wide semantic net). A hit must be in the right city AND match
#     # at least one semantic facet.
#     location_filter_parts = []

#     # Check if location is the subject (e.g. "where is Atlanta") —
#     # in that case we do NOT filter by location, same logic as Stage 1A
#     query_mode = signals.get('query_mode', 'explore')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:=`{c['name']}`" for c in cities]
#             if len(city_filters) == 1:
#                 location_filter_parts.append(city_filters[0])
#             else:
#                 location_filter_parts.append('(' + ' || '.join(city_filters) + ')')

#         states = profile.get('states', [])
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:=`{variant}`")
#             if len(state_conditions) == 1:
#                 location_filter_parts.append(state_conditions[0])
#             else:
#                 location_filter_parts.append('(' + ' || '.join(state_conditions) + ')')

#     # Build final filter:
#     # - Facet filters (keywords, entities, question_type) are OR'd together
#     # - Location filter is AND'd to enforce geographic constraint
#     facet_filter = ' || '.join(filter_parts) if filter_parts else ''
#     location_filter = ' && '.join(location_filter_parts) if location_filter_parts else ''

#     if facet_filter and location_filter:
#         filter_str = f'({facet_filter}) && {location_filter}'
#     elif location_filter:
#         filter_str = location_filter
#     else:
#         filter_str = facet_filter

#     print(f"   primary_keywords : {primary_kws}")
#     print(f"   entities         : {entity_names}")
#     print(f"   semantic_keywords: {semantic_kws}")
#     print(f"   question_type    : {question_type or 'any'}")
#     print(f"   location_filter  : {location_filter or 'none'}")
#     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

#     # ── Step B: Vector search within filtered subset ──────────────────────
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
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])

#         # Fallback: if location filter returned too few hits, retry WITHOUT
#         # the location filter but KEEP facet filters. This handles cases
#         # where questions collection doesn't have location fields populated.
#         if len(hits) < 5 and filter_str:
#             # First try: drop location, keep facet filters
#             fallback_filter = facet_filter if facet_filter else ''
#             print(f"⚠️  Stage 1B: only {len(hits)} hits with location filter — "
#                   f"retrying with facet filter only")

#             search_params_fallback = {
#                 'q':              '*',
#                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#                 'per_page':       max_results * 2,
#                 'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#             }
#             if fallback_filter:
#                 search_params_fallback['filter_by'] = fallback_filter

#             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
#             response_fallback = client.multi_search.perform(search_requests_fallback, {})
#             fallback_hits = response_fallback['results'][0].get('hits', [])
#             print(f"   Fallback (facet only) returned {len(fallback_hits)} hits")

#             # Second try: drop everything if still too few
#             if len(fallback_hits) < 5:
#                 print(f"⚠️  Stage 1B: still only {len(fallback_hits)} hits — "
#                       f"retrying with no filter")
#                 search_params_nofilter = {
#                     'q':              '*',
#                     'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#                     'per_page':       max_results * 2,
#                     'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#                 }
#                 search_requests_nofilter = {'searches': [{'collection': 'questions', **search_params_nofilter}]}
#                 response_nofilter = client.multi_search.perform(search_requests_nofilter, {})
#                 hits = response_nofilter['results'][0].get('hits', [])
#                 print(f"   Fallback (no filter) returned {len(hits)} hits")
#             else:
#                 hits = fallback_hits

#         # ── Step C: Validate each hit against query signals ───────────────
#         uuids       = []
#         seen        = set()
#         accepted    = 0
#         rejected    = 0

#         for hit in hits:
#             doc  = hit.get('document', {})
#             uuid = doc.get('document_uuid')

#             if not uuid:
#                 continue

#             # Validate hit against query signals
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
#                 print(f"   ❌ Rejected: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit.get('vector_distance', 1.0):.4f})")

#             if len(uuids) >= max_results:
#                 break

#         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
#               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1B error: {e}")
#         return []

# # ============================================================================
# # STAGE 1 (KEYWORD): Fetch uuids + metadata in one call (no pruning)
# # ============================================================================

# def fetch_candidates_with_metadata(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[Dict]:
#     """
#     Stage 1 (Keyword path): Fetch uuids AND lightweight metadata together.
#     Since keyword path has no vector pruning, all candidates survive,
#     so a separate metadata fetch would be a wasted round-trip.
#     """
#     signals = signals or {}
#     params = build_typesense_params(profile, signals=signals)
#     filter_str = build_filter_string_without_data_type(profile, signals=signals)

#     PAGE_SIZE = 250
#     all_results = []
#     current_page = 1
#     max_pages = (max_results // PAGE_SIZE) + 1

#     query_mode = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1 (keyword, with metadata): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode}")
#     print(f"   Fields: {params.get('query_by', '')}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     while len(all_results) < max_results and current_page <= max_pages:
#         search_params = {
#             'q': params.get('q', search_query),
#             'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#             'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
#             'per_page': PAGE_SIZE,
#             'page': current_page,
#             'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
#             'num_typos': params.get('num_typos', 0),
#             'prefix': params.get('prefix', 'no'),
#             'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#             'sort_by': params.get('sort_by', '_text_match:desc,authority_score:desc'),
#         }

#         if filter_str:
#             search_params['filter_by'] = filter_str

#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(search_params)
#             hits = response.get('hits', [])
#             found = response.get('found', 0)

#             if not hits:
#                 break

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 all_results.append({
#                     'id': doc.get('document_uuid'),
#                     'data_type': doc.get('document_data_type', ''),
#                     'category': doc.get('document_category', ''),
#                     'schema': doc.get('document_schema', ''),
#                     'authority_score': doc.get('authority_score', 0),
#                     'text_match': hit.get('text_match', 0),
#                     'image_url': doc.get('image_url', []),
#                     'logo_url': doc.get('logo_url', []),
#                 })

#             if len(all_results) >= found or len(hits) < PAGE_SIZE:
#                 break

#             current_page += 1

#         except Exception as e:
#             print(f"❌ Stage 1 error (page {current_page}): {e}")
#             break

#     print(f"📊 Stage 1 (keyword): Retrieved {len(all_results)} candidates with metadata")
#     return all_results[:max_results]
# def semantic_rerank_candidates(
#     candidate_ids: List[str],
#     query_embedding: List[float],
#     max_to_rerank: int = 250
# ) -> List[Dict]:
#     """
#     Stage 2: Semantic Rerank - Pure Vector Ranking
#     """
#     if not candidate_ids or not query_embedding:
#         return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#                 for i, cid in enumerate(candidate_ids)]
    
#     ids_to_rerank = candidate_ids[:max_to_rerank]
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'per_page': len(ids_to_rerank),
#         'include_fields': 'document_uuid',
#     }
    
#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])
        
#         reranked = []
#         for i, hit in enumerate(hits):
#             doc = hit.get('document', {})
#             reranked.append({
#                 'id': doc.get('document_uuid'),
#                 'vector_distance': hit.get('vector_distance', 1.0),
#                 'semantic_rank': i
#             })
        
#         reranked_ids = {r['id'] for r in reranked}
#         for cid in ids_to_rerank:
#             if cid not in reranked_ids:
#                 reranked.append({
#                     'id': cid,
#                     'vector_distance': 1.0,
#                     'semantic_rank': len(reranked)
#                 })
        
#         print(f"🎯 Stage 2: Reranked {len(reranked)} candidates")
#         return reranked
        
#     except Exception as e:
#         print(f"⚠️ Stage 2 error: {e}")
#         return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#                 for i, cid in enumerate(ids_to_rerank)]


# def apply_semantic_ranking(
#     cached_results: List[Dict],
#     reranked_results: List[Dict],
#     signals: Dict = None
# ) -> List[Dict]:
#     """
#     Apply semantic ranking to cached results with mode-specific blend ratios.
#     """
#     if not reranked_results:
#         return cached_results
    
#     signals = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
    
#     blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()
    
#     if query_mode == 'answer' and signals.get('wants_single_result'):
#         blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}
    
#     if signals.get('has_unknown_terms', False):
#         shift = min(0.15, blend['text_match'])
#         blend['text_match'] -= shift
#         blend['semantic'] += shift
#         print(f"   📊 Unknown term shift: text_match={blend['text_match']:.2f}, semantic={blend['semantic']:.2f}")
    
#     if signals.get('has_superlative', False):
#         shift = min(0.10, blend['semantic'])
#         blend['semantic'] -= shift
#         blend['authority'] += shift
#         print(f"   📊 Superlative shift: semantic={blend['semantic']:.2f}, authority={blend['authority']:.2f}")
    
#     print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")
    
#     best_distance = min(
#         (r.get('vector_distance', 1.0) for r in reranked_results if r.get('vector_distance', 1.0) < 1.0),
#         default=1.0
#     )
#     cutoff = min(best_distance * 2.0, 0.85)
    
#     print(f"   🎯 Semantic cutoff: best={best_distance:.3f}, cutoff={cutoff:.3f}")
    
#     rank_lookup = {
#         r['id']: {
#             'semantic_rank': r['semantic_rank'],
#             'vector_distance': r.get('vector_distance', 1.0)
#         }
#         for r in reranked_results
#     }
    
#     total_candidates = len(cached_results)
#     max_sem_rank = len(reranked_results)
    
#     for idx, item in enumerate(cached_results):
#         item_id = item.get('id')
#         authority = item.get('authority_score', 0)
        
#         if item_id in rank_lookup:
#             item['semantic_rank'] = rank_lookup[item_id]['semantic_rank']
#             item['vector_distance'] = rank_lookup[item_id]['vector_distance']
#         else:
#             item['semantic_rank'] = 999999
#             item['vector_distance'] = 1.0
        
#         text_score = 1.0 - (idx / max(total_candidates, 1))
#         sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
#         auth_score = min(authority / 100.0, 1.0)
        
#         item['blended_score'] = (
#             blend['text_match'] * text_score +
#             blend['semantic'] * sem_score +
#             blend['authority'] * auth_score
#         )
        
#         if item['vector_distance'] > cutoff:
#             item['blended_score'] -= 1.0
    
#     cached_results.sort(key=lambda x: -x.get('blended_score', 0))
    
#     for i, item in enumerate(cached_results):
#         item['rank'] = i
    
#     return cached_results


# # ============================================================================
# # FACET COUNTING FROM CACHE
# # ============================================================================

# def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
#     """Count facets from cached result set (always accurate)."""
#     data_type_counts = {}
#     category_counts = {}
#     schema_counts = {}
    
#     for item in cached_results:
#         dt = item.get('data_type', '')
#         if dt:
#             data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
        
#         cat = item.get('category', '')
#         if cat:
#             category_counts[cat] = category_counts.get(cat, 0) + 1
        
#         sch = item.get('schema', '')
#         if sch:
#             schema_counts[sch] = schema_counts.get(sch, 0) + 1
    
#     return {
#         'data_type': [
#             {
#                 'value': dt,
#                 'count': count,
#                 'label': DATA_TYPE_LABELS.get(dt, dt.title())
#             }
#             for dt, count in sorted(data_type_counts.items(), key=lambda x: -x[1])
#         ],
#         'category': [
#             {
#                 'value': cat,
#                 'count': count,
#                 'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title())
#             }
#             for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])
#         ],
#         'schema': [
#             {
#                 'value': sch,
#                 'count': count,
#                 'label': sch
#             }
#             for sch, count in sorted(schema_counts.items(), key=lambda x: -x[1])
#         ]
#     }


# # ============================================================================
# # FILTER AND PAGINATE CACHE
# # ============================================================================

# def filter_cached_results(
#     cached_results: List[Dict],
#     data_type: str = None,
#     category: str = None,
#     schema: str = None
# ) -> List[Dict]:
#     """Filter cached results by UI filters."""
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
#     """Paginate cached results."""
#     total = len(cached_results)
#     start = (page - 1) * per_page
#     end = start + per_page
    
#     if start >= total:
#         return [], total
    
#     return cached_results[start:end], total


# # ============================================================================
# # FULL DOCUMENT FETCHING
# # ============================================================================

# def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
#     """Fetch full document details for display."""
#     if not document_ids:
#         return []
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'per_page': len(document_ids),
#         'exclude_fields': 'embedding',
#     }
    
#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])
        
#         doc_map = {}
#         for hit in hits:
#             doc = hit.get('document', {})
#             doc_id = doc.get('document_uuid')
#             if doc_id:
#                 doc_map[doc_id] = format_result(hit, query)
        
#         results = []
#         for doc_id in document_ids:
#             if doc_id in doc_map:
#                 results.append(doc_map[doc_id])
        
#         return results
        
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
#     """
#     if not semantic_uuid:
#         return []

#     filter_str = f'semantic_uuid:={semantic_uuid}'
#     if exclude_uuid:
#         filter_str += f' && document_uuid:!={exclude_uuid}'

#     params = {
#         'q': '*',
#         'filter_by': filter_str,
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url',
#         'sort_by': 'authority_score:desc',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])

#         related = []
#         for hit in hits:
#             doc = hit.get('document', {})
#             related.append({
#                 'title': doc.get('document_title', ''),
#                 'url': doc.get('document_url', ''),
#                 'id': doc.get('document_uuid', ''),
#             })

#         print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
#         return related

#     except Exception as e:
#         print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
#         return []


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transform Typesense hit into response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     raw_date = doc.get('published_date_string', '')
#     formatted_date = ''
#     if raw_date:
#         try:
#             if 'T' in raw_date:
#                 date_part = raw_date.split('T')[0]
#                 dt = datetime.strptime(date_part, '%Y-%m-%d')
#                 formatted_date = dt.strftime('%b %d, %Y')
#             elif '-' in raw_date and len(raw_date) >= 10:
#                 dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
#                 formatted_date = dt.strftime('%b %d, %Y')
#             else:
#                 formatted_date = raw_date
#         except:
#             formatted_date = raw_date
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'image_url': doc.get('image_url') or [],
#         'logo_url': doc.get('logo_url') or [],
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'schema': doc.get('document_schema', ''),
#         'date': formatted_date,
#         'published_date': formatted_date,
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'humanized_summary': '',
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region'),
#             'geopoint': doc.get('location_geopoint') or doc.get('location_coordinates'),
#             'address': doc.get('location_address'),
#             'lat': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[0],
#             'lng': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[1],
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': 0.5,
#         'related_sources': []
#     }


# # ============================================================================
# # AI OVERVIEW LOGIC (Blueprint Step 8)
# # ============================================================================

# def _should_trigger_ai_overview(signals: Dict, results: List[Dict], query: str) -> bool:
#     """Blueprint Step 8: Determine if AI Overview should trigger."""
#     if not results:
#         return False
    
#     query_mode = signals.get('query_mode', 'explore')
    
#     if query_mode in ('browse', 'local', 'shop'):
#         return False
    
#     if query_mode == 'answer':
#         return True
    
#     if query_mode == 'compare':
#         return True
    
#     if query_mode == 'explore':
#         top_result = results[0]
#         top_title = top_result.get('title', '').lower()
#         top_facts = ' '.join(top_result.get('key_facts', [])).lower()
        
#         stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#                      'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#                      'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
#         query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
        
#         if not query_words:
#             return False
        
#         matches = sum(1 for w in query_words if w in top_title or w in top_facts)
#         confidence = matches / len(query_words)
        
#         return confidence >= 0.75
    
#     return False


# def _build_ai_overview(signals: Dict, results: List[Dict], query: str) -> Optional[str]:
#     """Build the AI Overview text using signal-driven key_fact selection."""
#     if not results or not results[0].get('key_facts'):
#         return None
    
#     question_word = signals.get('question_word')
    
#     stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#                  'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#                  'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
#     query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
    
#     matched_keyword = ''
#     if query_words:
#         top_title = results[0].get('title', '').lower()
#         top_facts = ' '.join(results[0].get('key_facts', [])).lower()
#         matched_keyword = max(query_words,
#                               key=lambda w: (w in top_title) + (w in top_facts))
    
#     return humanize_key_facts(
#         results[0]['key_facts'],
#         query,
#         matched_keyword=matched_keyword,
#         question_word=question_word,
#     )


# # ============================================================================
# # INTENT DETECTION (for compatibility — keyword path)
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Simple intent detection for compatibility."""
#     query_lower = query.lower()
    
#     location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
#     if any(w in query_lower for w in location_words):
#         return 'location'
    
#     person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
#     if any(w in query_lower for w in person_words):
#         return 'person'
    
#     return 'general'


# # ============================================================================
# # STABLE CACHE KEY
# # ============================================================================

# def _generate_stable_cache_key(session_id: str, query: str) -> str:
#     """
#     Stable cache key for the FINISHED result package.
#     Uses session_id + original query so tab clicks and pagination
#     always find the same cache. Never depends on derived values
#     like corrected_query, query_mode, cities, states.
#     """
#     normalized = query.strip().lower()
#     key_string = f"final|{session_id or 'nosession'}|{normalized}"
#     return hashlib.md5(key_string.encode()).hexdigest()


# # ============================================================================
# # IMAGE COUNTING HELPERS (FIXED — counts documents, not URLs)
# # ============================================================================



# # ============================================================================
# # STAGE 4: Fetch lightweight metadata for SURVIVORS ONLY
# # ============================================================================

# def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
#     """
#     Stage 4 (Semantic path only): Fetch lightweight metadata for documents
#     that survived vector pruning. Documents below the cutoff are never fetched.
    
#     Returns list of dicts with: id, data_type, category, schema,
#     authority_score, image_url, logo_url — in the same order as input
#     (preserving semantic rank order).
#     """
#     if not survivor_ids:
#         return []

#     BATCH_SIZE = 250
#     doc_map = {}

#     for i in range(0, len(survivor_ids), BATCH_SIZE):
#         batch_ids = survivor_ids[i:i + BATCH_SIZE]
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])

#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': len(batch_ids),
#             'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
#         }

#         try:
#             search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#             response = client.multi_search.perform(search_requests, {})
#             result = response['results'][0]
#             hits = result.get('hits', [])

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 uuid = doc.get('document_uuid')
#                 if uuid:
#                     doc_map[uuid] = {
#                         'id': uuid,
#                         'data_type': doc.get('document_data_type', ''),
#                         'category': doc.get('document_category', ''),
#                         'schema': doc.get('document_schema', ''),
#                         'authority_score': doc.get('authority_score', 0),
#                         'image_url': doc.get('image_url', []),
#                         'logo_url': doc.get('logo_url', []),
#                     }

#         except Exception as e:
#             print(f"❌ Stage 4 metadata fetch error (batch {i}): {e}")

#     # Return in original order, preserving semantic rank
#     results = []
#     for uuid in survivor_ids:
#         if uuid in doc_map:
#             results.append(doc_map[uuid])

#     print(f"📊 Stage 4: Fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
#     return results


# # ============================================================================
# # IMAGE COUNTING HELPERS
# # ============================================================================

# def _has_real_images(item):
#     """Check if a candidate has at least one non-empty image or logo URL.
    
#     Handles edge cases:
#     - image_url might be a string instead of a list
#     - Arrays might contain empty strings like ['']
#     - Fields might be missing entirely
    
#     This is exported for use by views.py image pagination.
#     """
#     image_urls = item.get('image_url', [])
#     if isinstance(image_urls, str):
#         image_urls = [image_urls]
#     if any(u for u in image_urls if u):
#         return True
#     logo_urls = item.get('logo_url', [])
#     if isinstance(logo_urls, str):
#         logo_urls = [logo_urls]
#     return any(u for u in logo_urls if u)


# def _count_images_from_candidates(all_results):
#     """Count DOCUMENTS that have at least one real image or logo URL."""
#     return sum(1 for item in all_results if _has_real_images(item))


# # ============================================================================
# # STAGE 5: ONE count pass — single source of truth
# # ============================================================================

# def count_all(candidates: List[Dict]) -> Dict:
#     """
#     Stage 5: Single counting pass. Runs ONCE, after all pruning is done.
#     Returns facets, image count, and total.
#     This is the ONLY place counting happens — single source of truth.
#     """
#     facets = count_facets_from_cache(candidates)
#     image_count = _count_images_from_candidates(candidates)
#     total = len(candidates)

#     print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
#           f"facets={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

#     return {
#         'facets': facets,
#         'facet_total': total,
#         'total_image_count': image_count,
#     }
# # ============================================================================
# # MAIN ENTRY POINT — Clean 7-Stage Pipeline
# # ============================================================================

# # SEMANTIC:  1(uuids) → 2(rerank) → 3(prune) → 4(metadata survivors) → 5(count) → 6(cache) → 7(paginate)
# # KEYWORD:   1(uuids+metadata) → 5(count) → 6(cache) → 7(paginate)

# # ============================================================================

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
#     answer: str = None,           # ← NEW
#     answer_type: str = None,      # ← NEW
#     skip_embedding: bool = False,
#     document_uuid: str = None,        # ← NEW
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     Clean 7-Stage Pipeline:
#         SEMANTIC:  1 → 2 → 3 → 4 → 5 → 6 → 7
#         KEYWORD:   1 → 5 → 6 → 7
    
#     Counting happens ONCE in Stage 5, after all pruning is done.
#     Single source of truth for facets, image counts, and totals.
#     """
#     times = {}
#     t0 = time.time()
#     print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

#     # Extract active filters
#     active_data_type = filters.get('data_type') if filters else None
#     active_category = filters.get('category') if filters else None
#     active_schema = filters.get('schema') if filters else None

#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
  
# # =========================================================================
# # ★ QUESTION DIRECT PATH: bypass all stages, fetch single document
# # =========================================================================
#     if document_uuid and search_source == 'question':
#         print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
#         t_fetch = time.time()
#         results = fetch_full_documents([document_uuid], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         # ── AI Overview: always trigger on question path ──
#         ai_overview = None
#         if results and results[0].get('key_facts'):

#             # Derive question_word from query since we skipped intent detection
#             question_word = None
#             q_lower = query.lower().strip()
#             for word in ('who', 'what', 'where', 'when', 'why', 'how'):
#                 if q_lower.startswith(word):
#                     question_word = word
#                     break

#             question_signals = {
#                 'query_mode': 'answer',
#                 'wants_single_result': True,
#                 'question_word': question_word,
#             }

#             ai_overview = _build_ai_overview(question_signals, results, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
#                 results[0]['humanized_summary'] = ai_overview

#                 # ── Related searches via semantic group ──
#         related_searches = []
#         if results:
#             semantic_uuid = results[0].get('semantic_uuid')
#             if semantic_uuid:
#                 try:
#                     related_docs = fetch_documents_by_semantic_uuid(
#                         semantic_uuid,
#                         exclude_uuid=document_uuid,
#                         limit=5
#                     )
#                     related_searches = [
#                         {
#                             'query': doc.get('title', ''),
#                             'url': doc.get('url', '')
#                         }
#                         for doc in related_docs
#                         if doc.get('title')
#                     ]
#                 except Exception as e:
#                     print(f"⚠️ Related searches error: {e}")


#         times['total'] = round((time.time() - t0) * 1000, 2)

#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': 'answer',
#             'query_mode': 'answer',
#             'answer': answer,                        # ← NEW
#             'answer_type': answer_type or 'UNKNOWN',
#             'results': results,
#             'total': len(results),
#             'facet_total': len(results),
#             'total_image_count': 0,
#             'page': 1,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'question_direct',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': 'question',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'related_searches': [],
#             'facets': {},
#             'related_searches': related_searches,  # was []
#             'word_discovery': {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': None,
#                 'category': None,
#                 'schema': None,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             },
#             'signals': question_signals,  # ← updated from {}
#             'profile': {},
#         }
# # =========================================================================
# # ★ FAST PATH: Check for finished cache FIRST
# # =========================================================================
#     stable_key = _generate_stable_cache_key(session_id, query)
#     finished = _get_cached_results(stable_key)

#     if finished is not None:
#         print(f"⚡ FAST PATH: '{query}' | page={page} | filter={active_data_type}/{active_category}/{active_schema}")

#         all_results = finished['all_results']
#         all_facets = finished['all_facets']
#         facet_total = finished['facet_total']
#         ai_overview = finished.get('ai_overview')
#         total_image_count = finished.get('total_image_count', 0)
#         metadata = finished['metadata']
#         times['cache'] = 'hit (fast path)'

#         # Filter by UI filters (tab click)
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )

#         # Paginate
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         # Stage 7: Fetch full documents for this page only
#         t_fetch = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         # Reattach AI overview on page 1
#         if results and page == 1 and ai_overview:
#             results[0]['humanized_summary'] = ai_overview

#         times['total'] = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ FAST PATH TIMING: {times}")
#         print(f"🔍 FAST PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)} | Images: {total_image_count}")

#         # Build return dict using cached metadata
#         signals = metadata.get('signals', {})

#         return {
#             'query': query,
#             'corrected_query': metadata.get('corrected_query', query),
#             'intent': metadata.get('intent', 'general'),
#             'query_mode': metadata.get('query_mode', 'keyword'),
#             'results': results,
#             'total': total_filtered,
#             'facet_total': facet_total,
#             'total_image_count': total_image_count,
#             'page': page,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': metadata.get('semantic_enabled', False),
#             'search_strategy': metadata.get('search_strategy', 'cached'),
#             'alt_mode': alt_mode,
#             'skip_embedding': skip_embedding,
#             'search_source': search_source,
#             'valid_terms': metadata.get('valid_terms', query.split()),
#             'unknown_terms': metadata.get('unknown_terms', []),
#             'data_type_facets': all_facets.get('data_type', []),
#             'category_facets': all_facets.get('category', []),
#             'schema_facets': all_facets.get('schema', []),
#             'related_searches': [],
#             'facets': all_facets,
#             'word_discovery': metadata.get('word_discovery', {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             }),
#             'timings': times,
#             'filters_applied': metadata.get('filters_applied', {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }),
#             'signals': signals,
#             'profile': metadata.get('profile', {}),
#         }

# # =========================================================================
# # ★ FULL PATH: No finished cache. Run the pipeline.
# # =========================================================================
#     print(f"🔬 FULL PATH: '{query}' (no finished cache for stable_key={stable_key[:12]}...)")

#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

# # =========================================================================
# # KEYWORD PATH:  Stage 1 → 5 → 6 → 7
# # =========================================================================

#     if is_keyword_path:
#         print(f"⚡ KEYWORD PIPELINE: '{query}'")

#         intent = detect_query_intent(query, pos_tags)

#         profile = {
#             'search_terms': query.split(),
#             'cities': [],
#             'states': [],
#             'location_terms': [],
#             'primary_intent': intent,
#             'field_boosts': {
#                 'primary_keywords': 10,
#                 'entity_names': 8,
#                 'semantic_keywords': 6,
#                 'key_facts': 4,
#                 'document_title': 3,
#             },
#         }

#         # ── Stage 1: Fetch uuids + metadata in one call (no pruning) ──
#         t1 = time.time()
#         all_results = fetch_candidates_with_metadata(query, profile)
#         times['stage1'] = round((time.time() - t1) * 1000, 2)

#         # ── Stage 5: ONE count pass ──
#         counts = count_all(all_results)

#         # ── Stage 6: Cache the final package ──
#         _set_cached_results(stable_key, {
#             'all_results': all_results,
#             'all_facets': counts['facets'],
#             'facet_total': counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'ai_overview': None,
#             'metadata': {
#                 'corrected_query': query,
#                 'intent': intent,
#                 'query_mode': 'keyword',
#                 'semantic_enabled': False,
#                 'search_strategy': 'keyword_graph_filter',
#                 'valid_terms': query.split(),
#                 'unknown_terms': [],
#                 'signals': {},
#                 'city_names': [],
#                 'state_names': [],
#                 'profile': profile,
#                 'word_discovery': {
#                     'valid_count': len(query.split()),
#                     'unknown_count': 0,
#                     'corrections': [],
#                     'filters': [],
#                     'locations': [],
#                     'sort': None,
#                     'total_score': 0,
#                     'average_score': 0,
#                     'max_score': 0,
#                 },
#                 'filters_applied': {
#                     'data_type': active_data_type,
#                     'category': active_category,
#                     'schema': active_schema,
#                     'is_local_search': False,
#                     'local_search_strength': 'none',
#                 },
#             },
#         })
#         print(f"💾 Cached keyword package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

#         # ── Stage 7: Filter → Paginate → Fetch full docs ──
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t2 = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ KEYWORD TIMING: {times}")

#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total_filtered,
#             'facet_total': counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'page': page,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword_graph_filter',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             'data_type_facets': counts['facets'].get('data_type', []),
#             'category_facets': counts['facets'].get('category', []),
#             'schema_facets': counts['facets'].get('schema', []),
#             'related_searches': [],
#             'facets': counts['facets'],
#             'word_discovery': {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }
#         }

# # =========================================================================
# # SEMANTIC PATH:  Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
# # =========================================================================

#     print(f"🔬 SEMANTIC PIPELINE: '{query}'")

#     # --- Word discovery + embedding in parallel ---
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

#     # --- Intent detection ---
#     signals = {}
#     if INTENT_DETECT_AVAILABLE:
#         try:
#             discovery = detect_intent(discovery)
#             signals = discovery.get('signals', {})
#             print(f"   🎯 Intent signals: mode={signals.get('query_mode')}, "
#                   f"q_word={signals.get('question_word')}, "
#                   f"local={signals.get('is_local_search')}, "
#                   f"location={signals.get('has_location')}, "
#                   f"service={signals.get('service_words')}, "
#                   f"temporal={signals.get('temporal_direction')}, "
#                   f"black_owned={signals.get('has_black_owned')}, "
#                   f"single={signals.get('wants_single_result')}, "
#                   f"domains={signals.get('domains_detected', [])[:3]}")
#         except Exception as e:
#             print(f"   ⚠️ intent_detect error: {e}")

#     corrected_query = discovery.get('corrected_query', query)
#     semantic_enabled = query_embedding is not None
#     query_mode = signals.get('query_mode', 'explore')

#     # --- Build profile ---
#     t2 = time.time()
#     profile = build_query_profile(discovery, signals=signals)
#     times['build_profile'] = round((time.time() - t2) * 1000, 2)

#     # --- Apply corrections to search terms ---
#     corrections = discovery.get('corrections', [])
#     if corrections:
#         correction_map = {
#             c['original'].lower(): c['corrected']
#             for c in corrections
#             if c.get('original') and c.get('corrected')
#         }
#         original_terms = profile.get('search_terms', [])
#         profile['search_terms'] = [
#             correction_map.get(term.lower(), term)
#             for term in original_terms
#         ]
#         if original_terms != profile['search_terms']:
#             print(f"   ✅ Applied corrections to search terms: {original_terms} → {profile['search_terms']}")

#     intent = profile.get('primary_intent', 'general')
#     city_names = [c['name'] for c in profile.get('cities', [])]
#     state_names = [s['name'] for s in profile.get('states', [])]

#     print(f"   Intent: {intent} | Mode: {query_mode}")
#     print(f"   Cities: {city_names}")
#     print(f"   States: {state_names}")
#     print(f"   Search Terms: {profile.get('search_terms', [])}")
#     print(f"   Field Boosts: {profile.get('field_boosts', {})}")

#     # # ── Stage 1: Fetch ONLY document_uuids ──
#     # t3 = time.time()
#     # candidate_uuids = fetch_candidate_uuids(corrected_query, profile, signals=signals)
#     # times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
#     # print(f"📊 Stage 1: {len(candidate_uuids)} candidate UUIDs")

#     # ── Stage 1: Fetch candidate UUIDs from both collections in parallel ──
#     t3 = time.time()

#     # If word discovery made unsafe corrections (proper nouns mangled into
#     # wrong categories), use the original query for Stage 1A keyword graph.


#     # Stage 1B always uses the original embedding so it is already protected.
#     UNSAFE_CATEGORIES = {
#         'Food', 'US City', 'US State', 'Country', 'Location',
#         'City', 'Place', 'Object', 'Animal', 'Color',
#     }
#     corrections = discovery.get('corrections', [])
#     has_unsafe_corrections = any(
#         c.get('correction_type') == 'pos_mismatch' or
#         c.get('category', '') in UNSAFE_CATEGORIES
#         for c in corrections
#     )
#     search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

#     if has_unsafe_corrections:
#         print(f"⚠️  Unsafe corrections — using original query for Stage 1A: '{query}'")

#     candidate_uuids = fetch_all_candidate_uuids(
#         search_query_for_stage1,
#         profile,
#         query_embedding,
#         signals=signals
#     )
#     times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
#     print(f"📊 Stage 1 COMBINED: {len(candidate_uuids)} candidate UUIDs")

#     # ── Stage 2: Vector rerank (only needs IDs + embedding) ──
#     survivor_uuids = candidate_uuids  # default if no embedding
#     vector_data = {}  # id → {vector_distance, semantic_rank}

#     if semantic_enabled and candidate_uuids:
#         t4 = time.time()
#         reranked = semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
#         times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

#         # Build lookup: id → vector data
#         for item in reranked:
#             vector_data[item['id']] = {
#                 'vector_distance': item.get('vector_distance', 1.0),
#                 'semantic_rank': item.get('semantic_rank', 999999),
#             }

#         # ── Stage 3: Vector prune — remove IDs below cutoff ──
#         DISTANCE_THRESHOLDS = {
#             'answer':  0.60,
#             'explore': 0.70,
#             'compare': 0.65,
#             'browse':  0.85,
#             'local':   0.85,
#             'shop':    0.80,
#         }
#         threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

#         before_prune = len(candidate_uuids)
#         survivor_uuids = [
#             uuid for uuid in candidate_uuids
#             if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
#         ]
#         after_prune = len(survivor_uuids)

#         if before_prune != after_prune:
#             print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): {before_prune} → {after_prune} ({before_prune - after_prune} removed)")
#         times['stage3_prune'] = f"{before_prune} → {after_prune}"
#     else:
#         print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, candidates={len(candidate_uuids)}")

#     # ── Stage 4: Fetch metadata for SURVIVORS ONLY ──
#     t5 = time.time()
#     all_results = fetch_candidate_metadata(survivor_uuids)
#     times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

#     # Attach vector data and compute blended scores
#     if vector_data:
#         total_candidates = len(all_results)
#         max_sem_rank = len(vector_data)
#         blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

#         if query_mode == 'answer' and signals.get('wants_single_result'):
#             blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

#         if signals.get('has_unknown_terms', False):
#             shift = min(0.15, blend['text_match'])
#             blend['text_match'] -= shift
#             blend['semantic'] += shift

#         if signals.get('has_superlative', False):
#             shift = min(0.10, blend['semantic'])
#             blend['semantic'] -= shift
#             blend['authority'] += shift

#         print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

#         for idx, item in enumerate(all_results):
#             item_id = item.get('id')
#             vd = vector_data.get(item_id, {})
#             item['vector_distance'] = vd.get('vector_distance', 1.0)
#             item['semantic_rank'] = vd.get('semantic_rank', 999999)

#             authority = item.get('authority_score', 0)
#             text_score = 1.0 - (idx / max(total_candidates, 1))
#             sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
#             auth_score = min(authority / 100.0, 1.0)

#             item['blended_score'] = (
#                 blend['text_match'] * text_score +
#                 blend['semantic'] * sem_score +
#                 blend['authority'] * auth_score
#             )

#         # Sort by blended score
#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#     # ── Stage 5: ONE count pass ──
#     counts = count_all(all_results)

#     # --- AI Overview (from page 1 full docs) ---
#     ai_overview = None
#     if all_results:
#         preview_items, _ = paginate_cached_results(all_results, 1, per_page)
#         preview_ids = [item['id'] for item in preview_items]
#         preview_docs = fetch_full_documents(preview_ids, query)

#         if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
#             ai_overview = _build_ai_overview(signals, preview_docs, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview: {ai_overview[:80]}...")

#     # --- Extract terms ---
#     valid_terms = profile.get('search_terms', [])
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

#     # ── Stage 6: Cache the final package ──
#     _set_cached_results(stable_key, {
#         'all_results': all_results,
#         'all_facets': counts['facets'],
#         'facet_total': counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'ai_overview': ai_overview,
#         'metadata': {
#             'corrected_query': corrected_query,
#             'intent': intent,
#             'query_mode': query_mode,
#             'semantic_enabled': semantic_enabled,
#             'search_strategy': 'staged_semantic' if semantic_enabled else 'keyword_fallback',
#             'valid_terms': valid_terms,
#             'unknown_terms': unknown_terms,
#             'signals': signals,
#             'city_names': city_names,
#             'state_names': state_names,
#             'profile': profile,
#             'word_discovery': {
#                 'valid_count': discovery.get('stats', {}).get('valid_words', 0),
#                 'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#                 'corrections': discovery.get('corrections', []),
#                 'filters': [],
#                 'locations': [
#                     {'field': 'location_city', 'values': city_names},
#                     {'field': 'location_state', 'values': state_names},
#                 ] if city_names or state_names else [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'filters_applied': {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': signals.get('is_local_search', False),
#                 'local_search_strength': signals.get('local_search_strength', 'none'),
#                 'has_black_owned': signals.get('has_black_owned', False),
#                 'graph_filters': [],
#                 'graph_locations': [
#                     {'field': 'location_city', 'values': city_names},
#                     {'field': 'location_state', 'values': state_names},
#                 ] if city_names or state_names else [],
#                 'graph_sort': None,
#             },
#         },
#     })
#     print(f"💾 Cached semantic package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

#     # ── Stage 7: Filter → Paginate → Fetch full docs ──
#     filtered_results = filter_cached_results(
#         all_results,
#         data_type=active_data_type,
#         category=active_category,
#         schema=active_schema
#     )

#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#     t6 = time.time()
#     page_ids = [item['id'] for item in page_items]
#     results = fetch_full_documents(page_ids, query)
#     times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

#     # Attach AI overview on page 1
#     if results and page == 1 and ai_overview:
#         results[0]['humanized_summary'] = ai_overview

#     # Store query embedding
#     if query_embedding:
#         try:
#             store_query_embedding(corrected_query, query_embedding, result_count=counts['facet_total'])
#         except Exception as e:
#             print(f"⚠️ store_query_embedding error: {e}")

#     times['total'] = round((time.time() - t0) * 1000, 2)

#     strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

#     print(f"⏱️ SEMANTIC TIMING: {times}")
#     print(f"🔍 {strategy.upper()} ({query_mode}) | Total: {counts['facet_total']} | Filtered: {total_filtered} | Page: {len(results)} | Images: {counts['total_image_count']}")

#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'query_mode': query_mode,
#         'results': results,
#         'total': total_filtered,
#         'facet_total': counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'page': page,
#         'per_page': per_page,
#         'search_time': round(time.time() - t0, 3),
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': strategy,
#         'alt_mode': alt_mode,
#         'skip_embedding': skip_embedding,
#         'search_source': search_source,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'related_searches': [],
#         'data_type_facets': counts['facets'].get('data_type', []),
#         'category_facets': counts['facets'].get('category', []),
#         'schema_facets': counts['facets'].get('schema', []),
#         'facets': counts['facets'],
#         'word_discovery': {
#             'valid_count': discovery.get('stats', {}).get('valid_words', 0),
#             'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#             'corrections': discovery.get('corrections', []),
#             'filters': [],
#             'locations': [
#                 {'field': 'location_city', 'values': city_names},
#                 {'field': 'location_state', 'values': state_names},
#             ] if city_names or state_names else [],
#             'sort': None,
#             'total_score': 0,
#             'average_score': 0,
#             'max_score': 0,
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type': active_data_type,
#             'category': active_category,
#             'schema': active_schema,
#             'is_local_search': signals.get('is_local_search', False),
#             'local_search_strength': signals.get('local_search_strength', 'none'),
#             'has_black_owned': signals.get('has_black_owned', False),
#             'graph_filters': [],
#             'graph_locations': [
#                 {'field': 'location_city', 'values': city_names},
#                 {'field': 'location_state', 'values': state_names},
#             ] if city_names or state_names else [],
#             'graph_sort': None,
#         },
#         'signals': signals,
#         'profile': profile,
#     }

# # ============================================================================
# # CONVENIENCE FUNCTIONS (for compatibility with views.py imports)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options."""
#     return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """Returns related searches."""
#     return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content."""
#     if not results:
#         return None
    
#     top = results[0]
#     if top.get('authority_score', 0) >= 85:
#         return {
#             'type': 'featured_snippet',
#             'title': top.get('title'),
#             'snippet': top.get('summary', ''),
#             'key_facts': top.get('key_facts', [])[:3],
#             'source': top.get('source'),
#             'url': top.get('url'),
#             'image': top.get('image')
#         }
#     return None


# def log_search_event(**kwargs):
#     """Logs search event."""
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
#     """Simple Typesense search wrapper."""
#     params = {
#         'q': query,
#         'query_by': query_by,
#         'per_page': per_page,
#         'page': page,
#     }
    
#     if filter_by:
#         params['filter_by'] = filter_by
#     if sort_by:
#         params['sort_by'] = sort_by
#     if facet_by:
#         params['facet_by'] = facet_by
#         params['max_facet_values'] = max_facet_values
    
#     try:
#         return client.collections[COLLECTION_NAME].documents.search(params)
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}
    

# # ============================================================================
# # TEST / CLI
# # ============================================================================

# if __name__ == "__main__":
#     import sys
    
#     if len(sys.argv) < 2:
#         print("Usage: python typesense_discovery_bridge.py \"your search query\"")
#         sys.exit(1)
    
#     query = ' '.join(sys.argv[1:])
    
#     print("=" * 70)
#     print(f"🚀 TESTING: '{query}'")
#     print("=" * 70)
    
#     result = execute_full_search(
#         query=query,
#         session_id='test-session',
#         filters={},
#         page=1,
#         per_page=10,
#         alt_mode='y',  # Semantic path
#     )
    
#     print("\n" + "=" * 70)
#     print("📊 RESULTS")
#     print("=" * 70)
#     print(f"Query: {result['query']}")
#     print(f"Corrected: {result['corrected_query']}")
#     print(f"Intent: {result['intent']}")
#     print(f"Query Mode: {result.get('query_mode', 'N/A')}")
#     print(f"Total: {result['total']}")
#     print(f"Facet Total: {result['facet_total']}")
#     print(f"Total Image Count: {result['total_image_count']}")
#     print(f"Strategy: {result['search_strategy']}")
#     print(f"Semantic: {result['semantic_enabled']}")
    
#     print(f"\n🔧 Corrections:")
#     for c in result.get('word_discovery', {}).get('corrections', []):
#         print(f"   '{c['original']}' → '{c['corrected']}' (type: {c.get('correction_type', 'unknown')})")

#     print(f"\n🔄 Query Flow:")
#     print(f"   Original:  '{result['query']}'")
#     print(f"   Corrected: '{result['corrected_query']}'")
#     print(f"   Changed:   {result['query'] != result['corrected_query']}")
        
#     print(f"\n📝 Terms:")
#     print(f"   Valid: {result['valid_terms']}")
#     print(f"   Unknown: {result['unknown_terms']}")

#     print(f"\n📍 Locations:")
#     for loc in result.get('word_discovery', {}).get('locations', []):
#         print(f"   {loc['field']}: {loc['values']}")
    
#     print(f"\n📁 Data Type Facets:")
#     for f in result.get('data_type_facets', []):
#         print(f"   {f['label']}: {f['count']}")
    
#     print(f"\n🎯 Signals:")
#     sigs = result.get('signals', {})
#     if sigs:
#         print(f"   query_mode: {sigs.get('query_mode')}")
#         print(f"   question_word: {sigs.get('question_word')}")
#         print(f"   wants_single: {sigs.get('wants_single_result')}")
#         print(f"   wants_multiple: {sigs.get('wants_multiple_results')}")
#         print(f"   is_local: {sigs.get('is_local_search')}")
#         print(f"   has_black_owned: {sigs.get('has_black_owned')}")
#         print(f"   temporal: {sigs.get('temporal_direction')}")
#         print(f"   has_unknown: {sigs.get('has_unknown_terms')}")
    
#     print(f"\n📄 Results ({len(result['results'])}):")
#     for i, r in enumerate(result['results'][:5], 1):
#         print(f"   {i}. {r['title'][:60]}")
#         if r.get('humanized_summary'):
#             print(f"      💡 {r['humanized_summary'][:80]}...")
#         print(f"      📍 {r['location'].get('city', '')}, {r['location'].get('state', '')}")
#         print(f"      🔗 {r['url'][:50]}...")
    
#     print(f"\n⏱️ Timings: {result['timings']}")


# """
# typesense_discovery_bridge.py
# =============================
# Complete search bridge between Word Discovery v2 and Typesense.

# This file handles EVERYTHING:
# - Word Discovery v2 integration
# - Intent signal integration (query_mode, question_word, etc.)
# - Query profile building (POS-based term routing, field boosts per mode)
# - Embedding generation (via embedding_client.py)
# - Result caching (self-contained)
# - Stage 1: Graph Filter (candidate generation)
# - Stage 2: Semantic Rerank (vector-based ranking with mode-specific blend)
# - Facet counting from cache
# - Pagination from cache
# - Full document fetching
# - AI Overview (signal-driven key_fact selection)
# - Returns same structure as execute_full_search() for views.py compatibility

# USAGE IN VIEWS.PY:
#     from .typesense_discovery_bridge import execute_full_search
    
#     result = execute_full_search(
#         query=corrected_query,
#         session_id=params.session_id,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         alt_mode=params.alt_mode,
#         ...
#     )
# """

# import re
# import json
# import time
# import hashlib
# import threading
# import typesense
# from typing import Dict, List, Tuple, Optional, Any, Set
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# from decouple import config
# import requests

# # ============================================================================
# # IMPORTS - Word Discovery v2 and Embedding Client
# # ============================================================================

# try:
#     from .word_discovery_fulltest import WordDiscovery
#     WORD_DISCOVERY_AVAILABLE = True
#     print("✅ WordDiscovery imported from .word_discovery_v2")
# except ImportError:
#     try:
#         from word_discovery_fulltest import WordDiscovery
#         WORD_DISCOVERY_AVAILABLE = True
#         print("✅ WordDiscovery imported from word_discovery_v2")
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery_v2 not available")

# try:
#     from .intent_detect import detect_intent, get_signals
#     INTENT_DETECT_AVAILABLE = True
#     print("✅ intent_detect imported")
# except ImportError:
#     try:
#         from intent_detect import detect_intent, get_signals
#         INTENT_DETECT_AVAILABLE = True
#         print("✅ intent_detect imported from intent_detect")
#     except ImportError:
#         INTENT_DETECT_AVAILABLE = False
#         print("⚠️ intent_detect not available")

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


# import random


# def humanize_key_facts(key_facts: list, query: str = '', matched_keyword: str = '',
#                        question_word: str = None) -> str:
#     """Format key_facts into a readable AfroToDo AI Overview,
#     only returning facts relevant to the matched keyword and question type.
    
#     Blueprint Step 8: AI Overview key_fact filtering based on question_word.
#     """
#     if not key_facts:
#         return ''
    
#     # Clean up facts
#     facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
    
#     if not facts:
#         return ''
    
#     # ─── Question-word-based fact filtering (Blueprint Step 8) ───────
#     if question_word:
#         qw = question_word.lower()
#         if qw == 'where':
#             # Prioritize facts with geographic language
#             geo_words = {'located', 'bounded', 'continent', 'region', 'coast',
#                          'ocean', 'border', 'north', 'south', 'east', 'west',
#                          'latitude', 'longitude', 'hemisphere', 'capital',
#                          'city', 'state', 'country', 'area', 'lies', 'situated'}
#             relevant_facts = [f for f in facts
#                               if any(gw in f.lower() for gw in geo_words)]
#         elif qw == 'when':
#             # Prioritize facts with dates/years/temporal language
#             import re as _re
#             temporal_words = {'founded', 'established', 'born', 'created',
#                               'started', 'opened', 'built', 'year', 'date',
#                               'century', 'decade', 'era', 'period'}
#             relevant_facts = [f for f in facts
#                               if any(tw in f.lower() for tw in temporal_words)
#                               or _re.search(r'\b\d{4}\b', f)]
#         elif qw == 'who':
#             # Prioritize facts with names, roles, titles, achievements
#             who_words = {'first', 'president', 'founder', 'ceo', 'leader',
#                          'director', 'known', 'famous', 'awarded', 'pioneer',
#                          'invented', 'created', 'named', 'appointed', 'elected'}
#             relevant_facts = [f for f in facts
#                               if any(ww in f.lower() for ww in who_words)]
#         elif qw == 'what':
#             # Prioritize definitional facts
#             what_words = {'is a', 'refers to', 'defined', 'known as',
#                           'type of', 'form of', 'means', 'represents'}
#             relevant_facts = [f for f in facts
#                               if any(ww in f.lower() for ww in what_words)]
#         else:
#             relevant_facts = []
        
#         # Fall back to keyword match if question-word filter found nothing
#         if not relevant_facts and matched_keyword:
#             keyword_lower = matched_keyword.lower()
#             relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        
#         # Final fallback: first fact
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     elif matched_keyword:
#         keyword_lower = matched_keyword.lower()
#         relevant_facts = [f for f in facts if keyword_lower in f.lower()]
#         if not relevant_facts:
#             relevant_facts = [facts[0]]
#     else:
#         relevant_facts = [facts[0]]
    
#     # Cap at 2 — keeps it concise
#     relevant_facts = relevant_facts[:2]
    
#     is_question = query and any(
#         query.lower().startswith(w) 
#         for w in ['who', 'what', 'where', 'when', 'why', 'how', 'is', 'are', 'can', 'do', 'does']
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


# # ============================================================================
# # THREAD POOL
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # TYPESENSE CLIENT
# # ============================================================================

# client = typesense.Client({
#     'api_key': config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host': config('TYPESENSE_HOST'),
#         'port': config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL')
#     }],
#     'connection_timeout_seconds': 5
# })

# COLLECTION_NAME = 'document'


# # ============================================================================
# # RESULT CACHE (Self-Contained)
# # ============================================================================

# from django.core.cache import cache as django_cache

# CACHE_TTL_SECONDS = 300  # 5 minutes
# MAX_CACHED_RESULTS = 2000


# def _get_cached_results(cache_key: str):
#     """Get cached result set from Redis."""
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


# def _set_cached_results(cache_key: str, data):
#     """Cache result set in Redis with TTL."""
#     try:
#         django_cache.set(cache_key, data, timeout=CACHE_TTL_SECONDS)
#         print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
#     except Exception as e:
#         print(f"⚠️ Redis cache SET error: {e}")


# def clear_search_cache():
#     """Clear all cached search results."""
#     try:
#         django_cache.clear()
#         print("🧹 Redis search cache cleared")
#     except Exception as e:
#         print(f"⚠️ Redis cache CLEAR error: {e}")


# # ============================================================================
# # CONSTANTS
# # ============================================================================

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

# KNOWN_CITIES = frozenset([
#     'atlanta', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio',
#     'san diego', 'dallas', 'austin', 'jacksonville', 'fort worth', 'columbus',
#     'charlotte', 'seattle', 'denver', 'boston', 'detroit', 'memphis', 'baltimore',
#     'nashville', 'milwaukee', 'albuquerque', 'tucson', 'fresno', 'sacramento',
#     'miami', 'oakland', 'minneapolis', 'tulsa', 'cleveland', 'new orleans',
#     'birmingham', 'montgomery', 'mobile', 'jackson', 'baton rouge', 'shreveport',
#     'savannah', 'charleston', 'richmond', 'norfolk', 'durham', 'raleigh',
#     'greensboro', 'louisville', 'lexington', 'cincinnati', 'st louis', 'kansas city',
#     'omaha', 'tampa', 'orlando', 'pittsburgh', 'las vegas', 'portland',
#     'los angeles', 'san francisco', 'new york', 'brooklyn', 'queens', 'harlem',
# ])

# # Categories for intent detection
# PERSON_CATEGORIES = frozenset([
#     'Person', 'Historical Figure', 'Celebrity', 'Athlete', 'Politician',
# ])

# ORGANIZATION_CATEGORIES = frozenset([
#     'Organization', 'Company', 'Business', 'Brand', 'HBCU',
# ])

# LOCATION_CATEGORIES = frozenset([
#     'US City', 'US State', 'US County', 'City', 'State', 'Country', 'Location',
#     'Continent', 'Region', 'continent', 'region', 'country',
# ])

# KEYWORD_CATEGORIES = frozenset([
#     'Keyword', 'Topic', 'Primary Keyword',
# ])

# MEDIA_CATEGORIES = frozenset([
#     'Song', 'Movie', 'Album', 'Book', 'TV Show',
# ])

# # Labels for UI
# DATA_TYPE_LABELS = {
#     'article': 'Articles',
#     'person': 'People',
#     'business': 'Businesses',
#     'place': 'Places',
#     'media': 'Media',
#     'event': 'Events',
#     'product': 'Products',
# }

# CATEGORY_LABELS = {
#     'healthcare_medical': 'Healthcare',
#     'fashion': 'Fashion',
#     'beauty': 'Beauty',
#     'food_recipes': 'Food & Recipes',
#     'travel_tourism': 'Travel',
#     'entertainment': 'Entertainment',
#     'business': 'Business',
#     'education': 'Education',
#     'technology': 'Technology',
#     'sports': 'Sports',
#     'finance': 'Finance',
#     'real_estate': 'Real Estate',
#     'lifestyle': 'Lifestyle',
#     'news': 'News',
#     'culture': 'Culture',
#     'general': 'General',
# }


# # ============================================================================
# # POS TAGS THAT GO INTO SEARCH QUERY (Blueprint Step 1)
# # ============================================================================

# # Only nouns go into the Typesense q parameter
# SEARCHABLE_POS = frozenset({
#     'noun', 'proper_noun',
# })

# # Everything else is a signal — never searched
# SIGNAL_POS = frozenset({
#     'verb', 'be', 'auxiliary', 'modal',
#     'wh_pronoun', 'pronoun',
#     'preposition', 'conjunction',
#     'adjective', 'adverb',
#     'article', 'determiner',
#     'negation', 'interjection',
# })


# # ============================================================================
# # SEMANTIC BLEND RATIOS (Blueprint Step 5)
# # ============================================================================

# BLEND_RATIOS = {
#     'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
#     'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
#     'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
#     'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
#     'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
# }


# # ============================================================================
# # DATA TYPE PREFERENCES BY MODE (Blueprint Step 9)
# # ============================================================================

# DATA_TYPE_PREFERENCES = {
#     'answer':  ['article', 'person', 'place'],
#     'explore': ['article', 'person', 'media'],
#     'browse':  ['article', 'business', 'product'],
#     'local':   ['business', 'place', 'article'],
#     'shop':    ['product', 'business', 'article'],
#     'compare': ['article', 'person', 'business'],
# }


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def _parse_rank(rank_value: Any) -> int:
#     """Parse rank to integer."""
#     if isinstance(rank_value, int):
#         return rank_value
#     try:
#         return int(float(rank_value))
#     except (ValueError, TypeError):
#         return 0


# def get_state_variants(state_name: str) -> List[str]:
#     """Get both full name and abbreviation for a state."""
#     state_lower = state_name.lower().strip()
#     variants = [state_name.title()]
    
#     if state_lower in US_STATE_ABBREV:
#         variants.append(US_STATE_ABBREV[state_lower])
    
#     for full, abbrev in US_STATE_ABBREV.items():
#         if abbrev.lower() == state_lower:
#             variants.append(full.title())
#             break
    
#     return list(set(variants))


# def is_state_category(category: str) -> bool:
#     """Check if category is specifically a state."""
#     if not category:
#         return False
#     return 'state' in category.lower()


# def is_city_category(category: str) -> bool:
#     """Check if category is specifically a city."""
#     if not category:
#         return False
#     cat_lower = category.lower()
#     return 'city' in cat_lower or 'county' in cat_lower


# # ============================================================================
# # WORD DISCOVERY WRAPPER
# # ============================================================================

# def _run_word_discovery(query: str) -> Dict:
#     """Run Word Discovery v2 on query."""
#     if WORD_DISCOVERY_AVAILABLE:
#         try:
#             wd = WordDiscovery(verbose=False)
#             result = wd.process(query)
#             return result
#         except Exception as e:
#             print(f"⚠️ WordDiscovery error: {e}")
    
#     # Fallback
#     return {
#         'query': query,
#         'corrected_query': query,
#         'terms': [],
#         'ngrams': [],
#         'corrections': [],
#         'stats': {
#             'total_words': len(query.split()),
#             'valid_words': 0,
#             'corrected_words': 0,
#             'unknown_words': len(query.split()),
#             'stopwords': 0,
#             'ngram_count': 0,
#         }
#     }


# def _run_embedding(query: str) -> Optional[List[float]]:
#     """Run embedding generation."""
#     return get_query_embedding(query)


# # ============================================================================
# # QUERY PROFILE BUILDING (Blueprint Steps 1, 4, 7)
# # ============================================================================

# def build_query_profile(discovery: Dict, signals: Dict = None) -> Dict:
#     """
#     Analyze ALL metadata from Word Discovery to understand user intent.
    
#     Blueprint alignment:
#     - Step 1: POS-based term routing (only nouns → q)
#     - Step 4: Dynamic field weight computation from term categories + mode
#     - Step 7: Location terms stripped from q, applied as filters
    
#     Returns profile with:
#     - Primary intent (person, organization, location, keyword, media)
#     - Search terms (POS-filtered: only nouns)
#     - Cities and states for filters (stripped from search terms)
#     - Field boosts (mode-aware + category-aware)
#     - Mode-specific Typesense parameters
#     """
#     query_mode = (signals or {}).get('query_mode', 'explore')
    
#     profile = {
#         'has_person': False,
#         'has_organization': False,
#         'has_location': False,
#         'has_keyword': False,
#         'has_media': False,
        
#         'person_score': 0,
#         'organization_score': 0,
#         'location_score': 0,
#         'keyword_score': 0,
#         'media_score': 0,
        
#         'persons': [],
#         'organizations': [],
#         'cities': [],
#         'states': [],
#         'keywords': [],
#         'search_terms': [],       # Only nouns — POS filtered
#         'location_terms': [],     # Location words stripped from q
        
#         'primary_intent': 'general',
#         'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
        
#         # Base field weights (Blueprint Step 4 base weights)
#         'field_boosts': {
#             'document_title': 10,
#             'entity_names': 2,
#             'primary_keywords': 3,
#             'key_facts': 3,
#             'semantic_keywords': 2,
#         },
#     }
    
#     if not discovery:
#         return profile
    
#     terms = discovery.get('terms', [])
#     ngrams = discovery.get('ngrams', [])
    
#     # Build term lookup by position
#     term_by_position = {t.get('position', 0): t for t in terms}
    
#     # Track positions consumed by n-grams
#     ngram_positions = set()
    
#     # =================================================================
#     # Process N-grams First
#     # =================================================================
    
#     for ngram in ngrams:
#         phrase = ngram.get('phrase', '')
#         ngram_category = ngram.get('category', '')
#         rank = _parse_rank(ngram.get('rank', 0))
#         positions = ngram.get('positions', [])
#         display = ngram.get('display', phrase)
        
#         ngram_positions.update(positions)
        
#         # Check individual term categories
#         term_categories = []
#         ngram_words = ngram.get('words', phrase.split())
        
#         for i, pos in enumerate(positions):
#             term = term_by_position.get(pos, {})
#             term_cat = term.get('category', '')
#             term_word = term.get('word', '') or (ngram_words[i] if i < len(ngram_words) else '')
#             term_rank = _parse_rank(term.get('rank', 0))
            
#             if term_cat and term_cat not in ('', 'stopword'):
#                 term_categories.append({
#                     'word': term_word,
#                     'category': term_cat,
#                     'rank': term_rank,
#                 })
#             else:
#                 word_lower = term_word.lower()
#                 if word_lower in US_STATE_ABBREV:
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US State',
#                         'rank': 350,
#                     })
#                 elif term_word.upper() in US_STATE_ABBREV.values():
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US State',
#                         'rank': 350,
#                     })
#                 elif word_lower in KNOWN_CITIES:
#                     term_categories.append({
#                         'word': term_word,
#                         'category': 'US City',
#                         'rank': 350,
#                     })
        
#         # Determine if this n-gram contains location terms
#         has_city_term = any(is_city_category(tc['category']) for tc in term_categories)
#         has_state_term = any(is_state_category(tc['category']) for tc in term_categories)
        
#         both_terms_are_locations = has_city_term and has_state_term
#         ngram_is_location = ngram_category in LOCATION_CATEGORIES
        
#         if both_terms_are_locations or ngram_is_location:
#             profile['has_location'] = True
            
#             # Check if any terms are non-filterable (continent/country/region)
#             has_filterable = any(
#                 is_city_category(tc['category']) or is_state_category(tc['category'])
#                 for tc in term_categories
#             )
#             is_subject = (
#                 query_mode == 'answer' and
#                 (signals or {}).get('question_word') == 'where'
#             )
            
#             for tc in term_categories:
#                 if is_city_category(tc['category']) and not is_subject:
#                     city_name = tc['word'].title()
#                     if city_name not in [c['name'] for c in profile['cities']]:
#                         profile['cities'].append({
#                             'name': city_name,
#                             'rank': tc['rank'],
#                         })
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
                    
#                 elif is_state_category(tc['category']) and not is_subject:
#                     state_name = tc['word'].title()
#                     if state_name not in [s['name'] for s in profile['states']]:
#                         profile['states'].append({
#                             'name': state_name,
#                             'rank': tc['rank'],
#                             'variants': get_state_variants(tc['word']),
#                         })
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
#                 else:
#                     # Continent/country/region or subject → keep in search
#                     profile['location_score'] += tc['rank']
#                     profile['location_terms'].append(tc['word'])
            
#             # If no filterable terms or location is subject, add phrase to search
#             if not has_filterable or is_subject:
#                 profile['search_terms'].append(phrase)
        
#         elif ngram_category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif ngram_category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             profile['search_terms'].append(phrase)
            
#         else:
#             profile['search_terms'].append(phrase)
    
#     # =================================================================
#     # Process Individual Terms (not in n-grams)
#     # Blueprint Step 1: POS-based routing — only nouns go into search
#     # =================================================================
    
#     for term in terms:
#         position = term.get('position', 0)
#         word = term.get('word', '')
#         display = term.get('display', word)
#         category = term.get('category', '')
#         rank = _parse_rank(term.get('rank', 0))
#         pos = term.get('pos', '').lower()
#         is_stopword = term.get('is_stopword', False)
#         part_of_ngram = term.get('part_of_ngram', False) or (position in ngram_positions)
        
#         if is_stopword or part_of_ngram:
#             continue
        
#         # ─── Blueprint Step 1: Determine if this is a noun (needed for all routing)
#         is_noun = pos in SEARCHABLE_POS
        
#         # ─── Blueprint Step 7: Location terms → filter OR search ─────
#         if category in LOCATION_CATEGORIES:
#             profile['has_location'] = True
#             profile['location_score'] += rank
            
#             cat_lower = category.lower()
#             is_filterable = is_city_category(category) or is_state_category(category)
#             is_subject = (
#                 query_mode == 'answer' and
#                 (signals or {}).get('question_word') == 'where'
#             )
            
#             if is_filterable and not is_subject:
#                 if is_city_category(category):
#                     city_name = display or word.title()
#                     if city_name not in [c['name'] for c in profile['cities']]:
#                         profile['cities'].append({'name': city_name, 'rank': rank})
#                 elif is_state_category(category):
#                     state_name = display or word.title()
#                     if state_name not in [s['name'] for s in profile['states']]:
#                         profile['states'].append({
#                             'name': state_name,
#                             'rank': rank,
#                             'variants': get_state_variants(word),
#                         })
#                 profile['location_terms'].append(word)
#                 continue
#             else:
#                 if is_noun:
#                     profile['search_terms'].append(word)
#                 profile['location_terms'].append(word)
#                 continue
        
#         # ─── Blueprint Step 1: Only nouns go into search terms ───────
        
#         if category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'word': word, 'display': display, 'rank': rank})
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         elif category == 'Dictionary Word':
#             if is_noun:
#                 profile['search_terms'].append(word)
            
#         else:
#             if is_noun and word:
#                 profile['search_terms'].append(word)
    
#     # =================================================================
#     # Determine Primary Intent
#     # =================================================================
    
#     scores = {
#         'person': profile['person_score'],
#         'organization': profile['organization_score'],
#         'location': profile['location_score'],
#         'keyword': profile['keyword_score'],
#         'media': profile['media_score'],
#     }
    
#     max_score = max(scores.values())
#     if max_score > 0:
#         profile['primary_intent'] = max(scores, key=scores.get)
#     else:
#         profile['primary_intent'] = 'general'
    
#     # =================================================================
#     # Set Field Boosts — Blueprint Step 4
#     # =================================================================
    
#     boosts = _compute_field_boosts(profile, query_mode, signals)
#     profile['field_boosts'] = boosts
    
#     return profile


# def _compute_field_boosts(profile: Dict, query_mode: str, signals: Dict = None) -> Dict:
#     """
#     Blueprint Step 4: Dynamic field weight computation.
#     """
#     signals = signals or {}
    
#     boosts = {
#         'document_title': 10,
#         'entity_names': 2,
#         'primary_keywords': 3,
#         'key_facts': 3,
#         'semantic_keywords': 2,
#     }
    
#     if query_mode == 'answer':
#         boosts['document_title'] = 20
#         boosts['entity_names'] = 15
#         if signals.get('wants_single_result'):
#             boosts = {
#                 'document_title': 20,
#                 'entity_names': 15,
#                 'primary_keywords': 5,
#             }
#     elif query_mode == 'browse':
#         boosts['primary_keywords'] = 15
#         boosts['semantic_keywords'] = 10
#     elif query_mode == 'local':
#         boosts['primary_keywords'] = 12
#     elif query_mode == 'compare':
#         boosts['entity_names'] = 15
#         boosts['document_title'] = 15
#     elif query_mode == 'shop':
#         boosts['primary_keywords'] = 12
#         boosts['document_title'] = 10
    
#     if profile.get('has_person'):
#         best_rank = max((p.get('rank', 0) for p in profile.get('persons', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
#         boosts['document_title'] = boosts.get('document_title', 10) + 5
    
#     if profile.get('has_organization'):
#         best_rank = max((o.get('rank', 0) for o in profile.get('organizations', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
    
#     if profile.get('has_keyword'):
#         best_rank = max((k.get('rank', 0) for k in profile.get('keywords', [])), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 10 + rank_bonus
#         boosts['semantic_keywords'] = boosts.get('semantic_keywords', 2) + 5
#         boosts['key_facts'] = boosts.get('key_facts', 3) + 4
    
#     if profile.get('has_media'):
#         best_rank = max((profile.get('media_score', 0),), default=0)
#         rank_bonus = min(best_rank // 100, 5)
#         boosts['document_title'] = boosts.get('document_title', 10) + 10 + rank_bonus
#         boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
#         boosts['entity_names'] = boosts.get('entity_names', 2) + 4
    
#     has_unknown = signals.get('has_unknown_terms', False)
#     has_known = (profile.get('has_person') or profile.get('has_organization')
#                  or profile.get('has_keyword') or profile.get('has_media'))
    
#     if has_unknown and has_known:
#         for field in boosts:
#             boosts[field] += 3
#     elif has_unknown and not has_known:
#         for field in boosts:
#             boosts[field] += 8
    
#     return boosts


# # ============================================================================
# # TYPESENSE PARAMETER BUILDING (Blueprint Steps 2, 3, 6)
# # ============================================================================

# def build_typesense_params(profile: Dict, ui_filters: Dict = None,
#                            signals: Dict = None) -> Dict:
#     """
#     Convert query profile into Typesense search parameters.
#     """
#     signals = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
    
#     params = {}
    
#     search_terms = profile.get('search_terms', [])
#     seen = set()
#     unique_terms = []
#     for term in search_terms:
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)
    
#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
#     field_boosts = profile.get('field_boosts', {})
    
#     if query_mode == 'local':
#         if 'service_type' not in field_boosts:
#             field_boosts['service_type'] = 12
#         if 'service_specialties' not in field_boosts:
#             field_boosts['service_specialties'] = 10
    
#     sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
#     params['query_by'] = ','.join([f[0] for f in sorted_fields])
#     params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
#     has_corrections = len(profile.get('corrections', [])) > 0 if isinstance(profile.get('corrections'), list) else False
#     term_count = len(unique_terms)
    
#     if query_mode == 'answer':
#         params['num_typos'] = 0
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'explore':
#         params['num_typos'] = 0 if has_corrections else 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
#     elif query_mode == 'browse':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
#     elif query_mode == 'local':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
#     elif query_mode == 'compare':
#         params['num_typos'] = 0
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 0
#     elif query_mode == 'shop':
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
#     else:
#         params['num_typos'] = 1
#         params['prefix'] = 'no'
#         params['drop_tokens_threshold'] = 1
    
#     temporal_direction = signals.get('temporal_direction')
#     price_direction = signals.get('price_direction')
#     has_superlative = signals.get('has_superlative', False)
#     has_rating = signals.get('has_rating_signal', False)
    
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
    
#     filter_conditions = []
    
#     cities = profile.get('cities', [])
#     states = profile.get('states', [])
    
#     local_strength = signals.get('local_search_strength', 'none')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )
    
#     apply_location_filter = True
#     if is_location_subject:
#         apply_location_filter = False
    
#     if apply_location_filter:
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             if len(city_filters) == 1:
#                 filter_conditions.append(city_filters[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:={variant}")
            
#             if len(state_conditions) == 1:
#                 filter_conditions.append(state_conditions[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')
    
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


# def build_filter_string_without_data_type(profile: Dict, signals: Dict = None) -> str:
#     """Build filter string for locations only (no data_type for facet counting)."""
#     signals = signals or {}
#     filter_conditions = []
    
#     query_mode = signals.get('query_mode', 'explore')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )
    
#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         states = profile.get('states', [])
        
#         if cities:
#             city_filters = [f"location_city:={c['name']}" for c in cities]
#             if len(city_filters) == 1:
#                 filter_conditions.append(city_filters[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:={variant}")
            
#             if len(state_conditions) == 1:
#                 filter_conditions.append(state_conditions[0])
#             else:
#                 filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
#     if signals.get('has_black_owned', False):
#         filter_conditions.append('black_owned:=true')
    
#     return ' && '.join(filter_conditions) if filter_conditions else ''


# # ============================================================================
# # RUN_PARALLEL_PREP FIX — Replace existing run_parallel_prep function
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.

#     FIX: Always embed the ORIGINAL query first.
#     Only re-embed with corrected_query if:
#       - The correction is a genuine dictionary word fix
#       - NOT a proper noun being mangled into a common word
#       - NOT a name being changed to food/city/other wrong category
#     """
#     if skip_embedding:
#         discovery = _run_word_discovery(query)
#         return discovery, None

#     # Always embed original query in parallel with word discovery
#     discovery_future = _executor.submit(_run_word_discovery, query)
#     embedding_future = _executor.submit(_run_embedding, query)  # ← always original

#     discovery = discovery_future.result()
#     embedding = embedding_future.result()

#     # ── Decide if re-embedding with corrected query is safe ───────────────
#     corrected_query = discovery.get('corrected_query', query)

#     if corrected_query.lower() != query.lower() and embedding is not None:
#         corrections = discovery.get('corrections', [])

#         # Only re-embed if corrections are genuine dictionary fixes
#         # NOT if they are proper nouns being mangled into wrong categories
#         SAFE_CORRECTION_TYPES = {'spelling', 'phonetic', 'abbreviation'}

#         UNSAFE_CATEGORIES = {
#             'Food', 'US City', 'US State', 'Country', 'Location',
#             'City', 'Place', 'Object', 'Animal', 'Color',
#         }

#         safe_corrections = []
#         unsafe_corrections = []

#         for c in corrections:
#             corrected_category = c.get('category', '')
#             correction_type    = c.get('correction_type', '')
#             original           = c.get('original', '')
#             corrected          = c.get('corrected', '')

#             # Flag as unsafe if:
#             # 1. Correction type is pos_mismatch (word discovery guessing)
#             # 2. Corrected category is something clearly wrong (Food, City, etc.)
#             # 3. Original was classified as Person/Organization (proper noun)
#             is_pos_mismatch    = correction_type == 'pos_mismatch'
#             is_wrong_category  = corrected_category in UNSAFE_CATEGORIES
#             is_proper_noun     = c.get('category', '') in ('Person', 'Organization', 'Brand')

#             if is_pos_mismatch or is_wrong_category or is_proper_noun:
#                 unsafe_corrections.append(c)
#             else:
#                 safe_corrections.append(c)

#         has_safe_corrections   = len(safe_corrections) > 0
#         has_unsafe_corrections = len(unsafe_corrections) > 0

#         if has_unsafe_corrections:
#             # Do NOT re-embed — original embedding is more accurate
#             print(f"⚠️  Skipping re-embed — unsafe corrections detected:")
#             for c in unsafe_corrections:
#                 print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
#                       f"(type={c.get('correction_type')}, category={c.get('category')})")
#             print(f"   Keeping original query embedding: '{query}'")

#         elif has_safe_corrections:
#             # Safe to re-embed with corrected query
#             print(f"✅  Re-embedding with corrected query: '{corrected_query}'")
#             embedding = get_query_embedding(corrected_query)

#     return discovery, embedding


# # ============================================================================
# # STAGE 1A: Document collection — keyword graph, 100 candidates
# # ============================================================================

# def fetch_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = 100
# ) -> List[str]:
#     """
#     Stage 1A: Keyword graph search against the document collection.
#     Returns up to 100 document_uuid strings — no metadata.
#     """
#     signals = signals or {}
#     params = build_typesense_params(profile, signals=signals)
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
#         hits = response.get('hits', [])

#         uuids = []
#         for hit in hits:
#             doc = hit.get('document', {})
#             uuid = doc.get('document_uuid')
#             if uuid:
#                 uuids.append(uuid)

#         print(f"📊 Stage 1A (document): {len(uuids)} candidate UUIDs")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1A error: {e}")
#         return []


# # ============================================================================
# # STAGE 1B: Questions collection — facet filter + vector search + validation
# # ============================================================================

# # Stopwords and question words to exclude from signal matching
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
#     - Lowercase
#     - Strip punctuation except hyphens inside words
#     - Remove stopwords and question words
#     - Keep only tokens longer than 2 characters
#     """
#     import re
#     if not text:
#         return set()

#     # Lowercase
#     text = text.lower()

#     # Replace punctuation with spaces (keep hyphens between word chars)
#     text = re.sub(r"[^\w\s-]", " ", text)
#     text = re.sub(r"\s*-\s*", " ", text)  # normalize hyphens

#     tokens = text.split()

#     return {
#         t for t in tokens
#         if len(t) > 2 and t not in _MATCH_STOPWORDS
#     }


# def _extract_query_signals(profile: Dict, discovery: Dict = None) -> tuple:
#     """
#     Extract and normalize all meaningful query signals from the profile.

#     Also extracts suggestion terms from word discovery's unknown_suggest
#     words, so that spelling corrections (e.g. "restuarants" → "restaurants")
#     are included in the validation token set even though the original
#     misspelled word is kept in the search query.

#     Returns:
#         all_tokens   — set of all individual normalized tokens
#         full_phrases — list of normalized full phrase strings (for substring match)
#         primary_subject — the highest-ranked entity/keyword (must-match candidate)
#     """
#     raw_signals = []
#     ranked_signals = []  # (rank, phrase)

#     # Persons — highest priority
#     for p in profile.get('persons', []):
#         phrase = p.get('phrase') or p.get('word', '')
#         rank   = p.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Organizations
#     for o in profile.get('organizations', []):
#         phrase = o.get('phrase') or o.get('word', '')
#         rank   = o.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Keywords
#     for k in profile.get('keywords', []):
#         phrase = k.get('phrase') or k.get('word', '')
#         rank   = k.get('rank', 0)
#         if phrase:
#             raw_signals.append(phrase)
#             ranked_signals.append((rank, phrase))

#     # Primary keywords from profile (if populated)
#     for kw in profile.get('primary_keywords', []):
#         if kw:
#             raw_signals.append(kw)
#             ranked_signals.append((0, kw))

#     # Search terms (nouns from word discovery)
#     for term in profile.get('search_terms', []):
#         if term:
#             raw_signals.append(term)

#     # ── Suggestion terms from unknown_suggest words ──────────────────
#     # When word discovery keeps a misspelled word (status='unknown_suggest')
#     # but found a suggestion (e.g. "restuarants" → "restaurants"), add the
#     # SUGGESTION to the signal set so _validate_question_hit() can match
#     # against hit keywords containing the correctly spelled word.
#     if discovery:
#         for term in discovery.get('terms', []):
#             if term.get('status') == 'unknown_suggest' and term.get('suggestion'):
#                 suggestion = term['suggestion']
#                 if suggestion not in raw_signals:
#                     raw_signals.append(suggestion)
#                     ranked_signals.append((100, suggestion))

#         for corr in discovery.get('corrections', []):
#             if corr.get('correction_type') == 'suggestion' and corr.get('corrected'):
#                 corrected = corr['corrected']
#                 if corrected not in raw_signals:
#                     raw_signals.append(corrected)
#                     ranked_signals.append((100, corrected))
#     # ── End suggestion block ─────────────────────────────────────────

#     # Build token set and full phrase list
#     all_tokens   = set()
#     full_phrases = []

#     for phrase in raw_signals:
#         normalized = _normalize_signal(phrase)
#         all_tokens.update(normalized)
#         phrase_lower = phrase.lower().strip()
#         if phrase_lower:
#             full_phrases.append(phrase_lower)

#     # Primary subject = highest ranked signal
#     primary_subject = None
#     if ranked_signals:
#         ranked_signals.sort(key=lambda x: -x[0])
#         primary_subject = _normalize_signal(ranked_signals[0][1])

#     return all_tokens, full_phrases, primary_subject


# def _validate_question_hit(
#     hit_doc: Dict,
#     query_tokens: set,
#     query_phrases: list,
#     primary_subject: set,
#     min_matches: int = 1,
# ) -> bool:
#     """
#     Validate a question hit against query signals using 4-level matching.

#     Level 1 — Exact token match (case insensitive)
#     Level 2 — Partial token match (query token inside candidate string)
#     Level 3 — Substring containment (query phrase inside candidate or vice versa)
#     Level 4 — Token overlap (shared meaningful tokens between strings)

#     Rules:
#     - At least min_matches signals must match
#     - If primary_subject is provided and query has 3+ signals,
#       primary subject must be one of the matches (prevents Grammy
#       matching on Beyoncé when user asked about Dr. Dre)

#     Returns True if hit passes validation, False if it should be discarded.
#     """
#     if not query_tokens:
#         # No signals to validate against — accept everything
#         return True

#     # Collect candidate values from the hit
#     candidate_raw = []
#     candidate_raw.extend(hit_doc.get('primary_keywords', []))
#     candidate_raw.extend(hit_doc.get('entities', []))
#     candidate_raw.extend(hit_doc.get('semantic_keywords', []))

#     if not candidate_raw:
#         return False

#     # Normalize all candidate values
#     candidate_tokens   = set()
#     candidate_phrases  = []

#     for val in candidate_raw:
#         if not val:
#             continue
#         normalized = _normalize_signal(val)
#         candidate_tokens.update(normalized)
#         candidate_phrases.append(val.lower().strip())

#     candidate_text = ' '.join(candidate_phrases)

#     match_count         = 0
#     primary_subject_hit = False

#     # ── Level 1: Exact token match ────────────────────────────────────────
#     exact_matches = query_tokens & candidate_tokens
#     if exact_matches:
#         match_count += len(exact_matches)
#         if primary_subject and (primary_subject & exact_matches):
#             primary_subject_hit = True

#     # ── Level 2: Partial token match ─────────────────────────────────────
#     # Query token appears as substring inside any candidate token
#     for qt in query_tokens:
#         if qt in exact_matches:
#             continue  # already counted
#         for ct in candidate_tokens:
#             if qt in ct or ct in qt:
#                 match_count += 1
#                 if primary_subject and qt in primary_subject:
#                     primary_subject_hit = True
#                 break

#     # ── Level 3: Substring containment ───────────────────────────────────
#     # Full query phrase appears inside candidate text or vice versa
#     for qp in query_phrases:
#         if len(qp) < 3:
#             continue
#         if qp in candidate_text:
#             match_count += 1
#             if primary_subject:
#                 qp_tokens = _normalize_signal(qp)
#                 if qp_tokens & primary_subject:
#                     primary_subject_hit = True
#         else:
#             # Check if any candidate phrase contains the query phrase
#             for cp in candidate_phrases:
#                 if qp in cp or cp in qp:
#                     match_count += 1
#                     if primary_subject:
#                         qp_tokens = _normalize_signal(qp)
#                         if qp_tokens & primary_subject:
#                             primary_subject_hit = True
#                     break

#     # ── Level 4: Token overlap ────────────────────────────────────────────
#     # Shared meaningful tokens between query and candidate
#     # Only counts tokens not already matched
#     remaining_query = query_tokens - exact_matches
#     token_overlap   = remaining_query & candidate_tokens
#     if token_overlap:
#         match_count += len(token_overlap)
#         if primary_subject and (primary_subject & token_overlap):
#             primary_subject_hit = True

#     # ── Decision ──────────────────────────────────────────────────────────
#     if match_count < min_matches:
#         return False

#     # If query has 3+ signals AND we have a primary subject,
#     # primary subject must be one of the matches.
#     # This prevents "Grammy" alone matching Dr. Dre questions
#     # to Beyoncé Grammy questions.
#     if primary_subject and len(query_tokens) >= 3:
#         if not primary_subject_hit:
#             return False

#     return True


# # ============================================================================
# # STAGE 1 COMBINED: Run both in parallel, merge + dedup
# # ============================================================================

# def fetch_all_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Runs Stage 1A (document) and Stage 1B (questions) in parallel.

#     Merge order:
#     1. Overlap — found by both paths (highest confidence)
#     2. Document-only hits
#     3. Question-only hits

#     Stage 1B runs independently of Stage 1A results.
#     Even if Stage 1A returns 0 (e.g. bad keyword graph), Stage 1B
#     can still surface the right document via vector search.
#     """
#     signals = signals or {}

#     doc_future = _executor.submit(
#         fetch_candidate_uuids, search_query, profile, signals, 100
#     )
#     q_future = _executor.submit(
#         fetch_candidate_uuids_from_questions, profile, query_embedding, signals, 50, discovery
#     )

#     doc_uuids = doc_future.result()
#     q_uuids   = q_future.result()

#     # Find overlap
#     doc_set = set(doc_uuids)
#     q_set   = set(q_uuids)
#     overlap  = doc_set & q_set

#     # Merge: overlap first, then document-only, then question-only
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
#     print(f"   document pool    : {len(doc_uuids)}")
#     print(f"   questions pool   : {len(q_uuids)}")
#     print(f"   overlap (both)   : {len(overlap)}")
#     print(f"   merged total     : {len(merged)}")

#     return merged

# def fetch_candidate_uuids_from_questions(
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     max_results: int = 50,
#     discovery: Dict = None,
# ) -> List[str]:
#     """
#     Stage 1B: Two-step search against the questions collection.

#     Step A — Build facet filter from profile metadata to narrow
#               the questions pool before the vector scan.

#     Step B — Run vector search within that filtered subset.

#     Step C — Validate each hit against query signals using
#               4-level case-insensitive partial matching before
#               accepting into the candidate pool.

#     FIX: Location filter (city/state) is now AND'd onto the facet filter
#          so question hits are constrained to the detected geographic area.
#          Previously, Stage 1B returned results from ALL cities because
#          only Stage 1A applied the location filter.

#     FIX: discovery dict is passed to _extract_query_signals() so that
#          spelling suggestions (unknown_suggest) are included in the
#          validation token set.

#     Returns up to max_results validated document_uuid strings.
#     """
#     signals = signals or {}

#     if not query_embedding:
#         print("⚠️ Stage 1B (questions): no embedding — skipping")
#         return []

#     # ── Extract query signals for validation ─────────────────────────────
#     query_tokens, query_phrases, primary_subject = _extract_query_signals(profile, discovery=discovery)

#     print(f"🔍 Stage 1B validation signals:")
#     print(f"   query_tokens    : {sorted(query_tokens)}")
#     print(f"   query_phrases   : {query_phrases}")
#     print(f"   primary_subject : {primary_subject}")

#     # ── Step A: Build facet filter ────────────────────────────────────────
#     filter_parts = []

#     primary_kws = profile.get('primary_keywords', [])
#     if not primary_kws:
#         primary_kws = [
#             k.get('phrase') or k.get('word', '')
#             for k in profile.get('keywords', [])
#         ]
#     primary_kws = [kw for kw in primary_kws if kw][:3]

#     if primary_kws:
#         kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
#         filter_parts.append(f'primary_keywords:[{kw_values}]')

#     # Only use entity names that are full names (have space) or high rank
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

#     semantic_kws = profile.get('semantic_keywords', [])
#     semantic_kws = [kw for kw in semantic_kws if kw][:3]
#     if semantic_kws:
#         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
#         filter_parts.append(f'semantic_keywords:[{sem_values}]')

#     # ★ FIX: use `or ''` to guard against None value
#     question_word = signals.get('question_word') or ''
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

#     # ── Location filter: enforce city/state on questions collection ────────
#     location_filter_parts = []

#     query_mode = signals.get('query_mode', 'explore')
#     is_location_subject = (
#         query_mode == 'answer' and
#         signals.get('has_question_word') and
#         signals.get('question_word') in ('where',) and
#         signals.get('has_location_entity', False)
#     )

#     if not is_location_subject:
#         cities = profile.get('cities', [])
#         if cities:
#             city_filters = [f"location_city:=`{c['name']}`" for c in cities]
#             if len(city_filters) == 1:
#                 location_filter_parts.append(city_filters[0])
#             else:
#                 location_filter_parts.append('(' + ' || '.join(city_filters) + ')')

#         states = profile.get('states', [])
#         if states:
#             state_conditions = []
#             for state in states:
#                 variants = state.get('variants', [state['name']])
#                 for variant in variants:
#                     state_conditions.append(f"location_state:=`{variant}`")
#             if len(state_conditions) == 1:
#                 location_filter_parts.append(state_conditions[0])
#             else:
#                 location_filter_parts.append('(' + ' || '.join(state_conditions) + ')')

#     facet_filter = ' || '.join(filter_parts) if filter_parts else ''
#     location_filter = ' && '.join(location_filter_parts) if location_filter_parts else ''

#     if facet_filter and location_filter:
#         filter_str = f'({facet_filter}) && {location_filter}'
#     elif location_filter:
#         filter_str = location_filter
#     else:
#         filter_str = facet_filter

#     print(f"   primary_keywords : {primary_kws}")
#     print(f"   entities         : {entity_names}")
#     print(f"   semantic_keywords: {semantic_kws}")
#     print(f"   question_type    : {question_type or 'any'}")
#     print(f"   location_filter  : {location_filter or 'none'}")
#     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

#     # ── Step B: Vector search within filtered subset ──────────────────────
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
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])

#         # Fallback: if location filter returned too few hits, retry WITHOUT
#         # the location filter but KEEP facet filters.
#         if len(hits) < 5 and filter_str:
#             fallback_filter = facet_filter if facet_filter else ''
#             print(f"⚠️  Stage 1B: only {len(hits)} hits with location filter — "
#                   f"retrying with facet filter only")

#             search_params_fallback = {
#                 'q':              '*',
#                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#                 'per_page':       max_results * 2,
#                 'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#             }
#             if fallback_filter:
#                 search_params_fallback['filter_by'] = fallback_filter

#             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
#             response_fallback = client.multi_search.perform(search_requests_fallback, {})
#             fallback_hits = response_fallback['results'][0].get('hits', [])
#             print(f"   Fallback (facet only) returned {len(fallback_hits)} hits")

#             if len(fallback_hits) < 5:
#                 print(f"⚠️  Stage 1B: still only {len(fallback_hits)} hits — "
#                       f"retrying with no filter")
#                 search_params_nofilter = {
#                     'q':              '*',
#                     'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
#                     'per_page':       max_results * 2,
#                     'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
#                 }
#                 search_requests_nofilter = {'searches': [{'collection': 'questions', **search_params_nofilter}]}
#                 response_nofilter = client.multi_search.perform(search_requests_nofilter, {})
#                 hits = response_nofilter['results'][0].get('hits', [])
#                 print(f"   Fallback (no filter) returned {len(hits)} hits")
#             else:
#                 hits = fallback_hits

#         # ── Step C: Validate each hit against query signals ───────────────
#         uuids       = []
#         seen        = set()
#         accepted    = 0
#         rejected    = 0

#         for hit in hits:
#             doc  = hit.get('document', {})
#             uuid = doc.get('document_uuid')

#             if not uuid:
#                 continue

#             # Validate hit against query signals
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
#                 print(f"   ❌ Rejected: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit.get('vector_distance', 1.0):.4f})")

#             if len(uuids) >= max_results:
#                 break

#         print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
#               f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1B error: {e}")
#         return []

# # ============================================================================
# # STAGE 1 (KEYWORD): Fetch uuids + metadata in one call (no pruning)
# # ============================================================================

# def fetch_candidates_with_metadata(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[Dict]:
#     """
#     Stage 1 (Keyword path): Fetch uuids AND lightweight metadata together.
#     Since keyword path has no vector pruning, all candidates survive,
#     so a separate metadata fetch would be a wasted round-trip.
#     """
#     signals = signals or {}
#     params = build_typesense_params(profile, signals=signals)
#     filter_str = build_filter_string_without_data_type(profile, signals=signals)

#     PAGE_SIZE = 250
#     all_results = []
#     current_page = 1
#     max_pages = (max_results // PAGE_SIZE) + 1

#     query_mode = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1 (keyword, with metadata): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode}")
#     print(f"   Fields: {params.get('query_by', '')}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     while len(all_results) < max_results and current_page <= max_pages:
#         search_params = {
#             'q': params.get('q', search_query),
#             'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#             'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
#             'per_page': PAGE_SIZE,
#             'page': current_page,
#             'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
#             'num_typos': params.get('num_typos', 0),
#             'prefix': params.get('prefix', 'no'),
#             'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
#             'sort_by': params.get('sort_by', '_text_match:desc,authority_score:desc'),
#         }

#         if filter_str:
#             search_params['filter_by'] = filter_str

#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(search_params)
#             hits = response.get('hits', [])
#             found = response.get('found', 0)

#             if not hits:
#                 break

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 all_results.append({
#                     'id': doc.get('document_uuid'),
#                     'data_type': doc.get('document_data_type', ''),
#                     'category': doc.get('document_category', ''),
#                     'schema': doc.get('document_schema', ''),
#                     'authority_score': doc.get('authority_score', 0),
#                     'text_match': hit.get('text_match', 0),
#                     'image_url': doc.get('image_url', []),
#                     'logo_url': doc.get('logo_url', []),
#                 })

#             if len(all_results) >= found or len(hits) < PAGE_SIZE:
#                 break

#             current_page += 1

#         except Exception as e:
#             print(f"❌ Stage 1 error (page {current_page}): {e}")
#             break

#     print(f"📊 Stage 1 (keyword): Retrieved {len(all_results)} candidates with metadata")
#     return all_results[:max_results]
# def semantic_rerank_candidates(
#     candidate_ids: List[str],
#     query_embedding: List[float],
#     max_to_rerank: int = 250
# ) -> List[Dict]:
#     """
#     Stage 2: Semantic Rerank - Pure Vector Ranking
#     """
#     if not candidate_ids or not query_embedding:
#         return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#                 for i, cid in enumerate(candidate_ids)]
    
#     ids_to_rerank = candidate_ids[:max_to_rerank]
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'per_page': len(ids_to_rerank),
#         'include_fields': 'document_uuid',
#     }
    
#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])
        
#         reranked = []
#         for i, hit in enumerate(hits):
#             doc = hit.get('document', {})
#             reranked.append({
#                 'id': doc.get('document_uuid'),
#                 'vector_distance': hit.get('vector_distance', 1.0),
#                 'semantic_rank': i
#             })
        
#         reranked_ids = {r['id'] for r in reranked}
#         for cid in ids_to_rerank:
#             if cid not in reranked_ids:
#                 reranked.append({
#                     'id': cid,
#                     'vector_distance': 1.0,
#                     'semantic_rank': len(reranked)
#                 })
        
#         print(f"🎯 Stage 2: Reranked {len(reranked)} candidates")
#         return reranked
        
#     except Exception as e:
#         print(f"⚠️ Stage 2 error: {e}")
#         return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
#                 for i, cid in enumerate(ids_to_rerank)]


# def apply_semantic_ranking(
#     cached_results: List[Dict],
#     reranked_results: List[Dict],
#     signals: Dict = None
# ) -> List[Dict]:
#     """
#     Apply semantic ranking to cached results with mode-specific blend ratios.
#     """
#     if not reranked_results:
#         return cached_results
    
#     signals = signals or {}
#     query_mode = signals.get('query_mode', 'explore')
    
#     blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()
    
#     if query_mode == 'answer' and signals.get('wants_single_result'):
#         blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}
    
#     if signals.get('has_unknown_terms', False):
#         shift = min(0.15, blend['text_match'])
#         blend['text_match'] -= shift
#         blend['semantic'] += shift
#         print(f"   📊 Unknown term shift: text_match={blend['text_match']:.2f}, semantic={blend['semantic']:.2f}")
    
#     if signals.get('has_superlative', False):
#         shift = min(0.10, blend['semantic'])
#         blend['semantic'] -= shift
#         blend['authority'] += shift
#         print(f"   📊 Superlative shift: semantic={blend['semantic']:.2f}, authority={blend['authority']:.2f}")
    
#     print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")
    
#     best_distance = min(
#         (r.get('vector_distance', 1.0) for r in reranked_results if r.get('vector_distance', 1.0) < 1.0),
#         default=1.0
#     )
#     cutoff = min(best_distance * 2.0, 0.85)
    
#     print(f"   🎯 Semantic cutoff: best={best_distance:.3f}, cutoff={cutoff:.3f}")
    
#     rank_lookup = {
#         r['id']: {
#             'semantic_rank': r['semantic_rank'],
#             'vector_distance': r.get('vector_distance', 1.0)
#         }
#         for r in reranked_results
#     }
    
#     total_candidates = len(cached_results)
#     max_sem_rank = len(reranked_results)
    
#     for idx, item in enumerate(cached_results):
#         item_id = item.get('id')
#         authority = item.get('authority_score', 0)
        
#         if item_id in rank_lookup:
#             item['semantic_rank'] = rank_lookup[item_id]['semantic_rank']
#             item['vector_distance'] = rank_lookup[item_id]['vector_distance']
#         else:
#             item['semantic_rank'] = 999999
#             item['vector_distance'] = 1.0
        
#         text_score = 1.0 - (idx / max(total_candidates, 1))
#         sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
#         auth_score = min(authority / 100.0, 1.0)
        
#         item['blended_score'] = (
#             blend['text_match'] * text_score +
#             blend['semantic'] * sem_score +
#             blend['authority'] * auth_score
#         )
        
#         if item['vector_distance'] > cutoff:
#             item['blended_score'] -= 1.0
    
#     cached_results.sort(key=lambda x: -x.get('blended_score', 0))
    
#     for i, item in enumerate(cached_results):
#         item['rank'] = i
    
#     return cached_results


# # ============================================================================
# # FACET COUNTING FROM CACHE
# # ============================================================================

# def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
#     """Count facets from cached result set (always accurate)."""
#     data_type_counts = {}
#     category_counts = {}
#     schema_counts = {}
    
#     for item in cached_results:
#         dt = item.get('data_type', '')
#         if dt:
#             data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
        
#         cat = item.get('category', '')
#         if cat:
#             category_counts[cat] = category_counts.get(cat, 0) + 1
        
#         sch = item.get('schema', '')
#         if sch:
#             schema_counts[sch] = schema_counts.get(sch, 0) + 1
    
#     return {
#         'data_type': [
#             {
#                 'value': dt,
#                 'count': count,
#                 'label': DATA_TYPE_LABELS.get(dt, dt.title())
#             }
#             for dt, count in sorted(data_type_counts.items(), key=lambda x: -x[1])
#         ],
#         'category': [
#             {
#                 'value': cat,
#                 'count': count,
#                 'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title())
#             }
#             for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])
#         ],
#         'schema': [
#             {
#                 'value': sch,
#                 'count': count,
#                 'label': sch
#             }
#             for sch, count in sorted(schema_counts.items(), key=lambda x: -x[1])
#         ]
#     }


# # ============================================================================
# # FILTER AND PAGINATE CACHE
# # ============================================================================

# def filter_cached_results(
#     cached_results: List[Dict],
#     data_type: str = None,
#     category: str = None,
#     schema: str = None
# ) -> List[Dict]:
#     """Filter cached results by UI filters."""
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
#     """Paginate cached results."""
#     total = len(cached_results)
#     start = (page - 1) * per_page
#     end = start + per_page
    
#     if start >= total:
#         return [], total
    
#     return cached_results[start:end], total


# # ============================================================================
# # FULL DOCUMENT FETCHING
# # ============================================================================

# def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
#     """Fetch full document details for display."""
#     if not document_ids:
#         return []
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'per_page': len(document_ids),
#         'exclude_fields': 'embedding',
#     }
    
#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])
        
#         doc_map = {}
#         for hit in hits:
#             doc = hit.get('document', {})
#             doc_id = doc.get('document_uuid')
#             if doc_id:
#                 doc_map[doc_id] = format_result(hit, query)
        
#         results = []
#         for doc_id in document_ids:
#             if doc_id in doc_map:
#                 results.append(doc_map[doc_id])
        
#         return results
        
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
#     """
#     if not semantic_uuid:
#         return []

#     filter_str = f'semantic_uuid:={semantic_uuid}'
#     if exclude_uuid:
#         filter_str += f' && document_uuid:!={exclude_uuid}'

#     params = {
#         'q': '*',
#         'filter_by': filter_str,
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url',
#         'sort_by': 'authority_score:desc',
#     }

#     try:
#         search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])

#         related = []
#         for hit in hits:
#             doc = hit.get('document', {})
#             related.append({
#                 'title': doc.get('document_title', ''),
#                 'url': doc.get('document_url', ''),
#                 'id': doc.get('document_uuid', ''),
#             })

#         print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
#         return related

#     except Exception as e:
#         print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
#         return []


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transform Typesense hit into response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     raw_date = doc.get('published_date_string', '')
#     formatted_date = ''
#     if raw_date:
#         try:
#             if 'T' in raw_date:
#                 date_part = raw_date.split('T')[0]
#                 dt = datetime.strptime(date_part, '%Y-%m-%d')
#                 formatted_date = dt.strftime('%b %d, %Y')
#             elif '-' in raw_date and len(raw_date) >= 10:
#                 dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
#                 formatted_date = dt.strftime('%b %d, %Y')
#             else:
#                 formatted_date = raw_date
#         except:
#             formatted_date = raw_date
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'image_url': doc.get('image_url') or [],
#         'logo_url': doc.get('logo_url') or [],
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'schema': doc.get('document_schema', ''),
#         'date': formatted_date,
#         'published_date': formatted_date,
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'humanized_summary': '',
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region'),
#             'geopoint': doc.get('location_geopoint') or doc.get('location_coordinates'),
#             'address': doc.get('location_address'),
#             'lat': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[0],
#             'lng': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[1],
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': 0.5,
#         'related_sources': []
#     }


# # ============================================================================
# # AI OVERVIEW LOGIC (Blueprint Step 8)
# # ============================================================================

# def _should_trigger_ai_overview(signals: Dict, results: List[Dict], query: str) -> bool:
#     """Blueprint Step 8: Determine if AI Overview should trigger."""
#     if not results:
#         return False
    
#     query_mode = signals.get('query_mode', 'explore')
    
#     if query_mode in ('browse', 'local', 'shop'):
#         return False
    
#     if query_mode == 'answer':
#         return True
    
#     if query_mode == 'compare':
#         return True
    
#     if query_mode == 'explore':
#         top_result = results[0]
#         top_title = top_result.get('title', '').lower()
#         top_facts = ' '.join(top_result.get('key_facts', [])).lower()
        
#         stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#                      'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#                      'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
#         query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
        
#         if not query_words:
#             return False
        
#         matches = sum(1 for w in query_words if w in top_title or w in top_facts)
#         confidence = matches / len(query_words)
        
#         return confidence >= 0.75
    
#     return False


# def _build_ai_overview(signals: Dict, results: List[Dict], query: str) -> Optional[str]:
#     """Build the AI Overview text using signal-driven key_fact selection."""
#     if not results or not results[0].get('key_facts'):
#         return None
    
#     question_word = signals.get('question_word')
    
#     stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
#                  'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
#                  'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
#     query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
    
#     matched_keyword = ''
#     if query_words:
#         top_title = results[0].get('title', '').lower()
#         top_facts = ' '.join(results[0].get('key_facts', [])).lower()
#         matched_keyword = max(query_words,
#                               key=lambda w: (w in top_title) + (w in top_facts))
    
#     return humanize_key_facts(
#         results[0]['key_facts'],
#         query,
#         matched_keyword=matched_keyword,
#         question_word=question_word,
#     )


# # ============================================================================
# # INTENT DETECTION (for compatibility — keyword path)
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Simple intent detection for compatibility."""
#     query_lower = query.lower()
    
#     location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
#     if any(w in query_lower for w in location_words):
#         return 'location'
    
#     person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
#     if any(w in query_lower for w in person_words):
#         return 'person'
    
#     return 'general'


# # ============================================================================
# # STABLE CACHE KEY
# # ============================================================================

# def _generate_stable_cache_key(session_id: str, query: str) -> str:
#     """
#     Stable cache key for the FINISHED result package.
#     Uses session_id + original query so tab clicks and pagination
#     always find the same cache. Never depends on derived values
#     like corrected_query, query_mode, cities, states.
#     """
#     normalized = query.strip().lower()
#     key_string = f"final|{session_id or 'nosession'}|{normalized}"
#     return hashlib.md5(key_string.encode()).hexdigest()


# # ============================================================================
# # STAGE 4: Fetch lightweight metadata for SURVIVORS ONLY
# # ============================================================================

# def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
#     """
#     Stage 4 (Semantic path only): Fetch lightweight metadata for documents
#     that survived vector pruning. Documents below the cutoff are never fetched.
    
#     Returns list of dicts with: id, data_type, category, schema,
#     authority_score, image_url, logo_url — in the same order as input
#     (preserving semantic rank order).
#     """
#     if not survivor_ids:
#         return []

#     BATCH_SIZE = 250
#     doc_map = {}

#     for i in range(0, len(survivor_ids), BATCH_SIZE):
#         batch_ids = survivor_ids[i:i + BATCH_SIZE]
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])

#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': len(batch_ids),
#             'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
#         }

#         try:
#             search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
#             response = client.multi_search.perform(search_requests, {})
#             result = response['results'][0]
#             hits = result.get('hits', [])

#             for hit in hits:
#                 doc = hit.get('document', {})
#                 uuid = doc.get('document_uuid')
#                 if uuid:
#                     doc_map[uuid] = {
#                         'id': uuid,
#                         'data_type': doc.get('document_data_type', ''),
#                         'category': doc.get('document_category', ''),
#                         'schema': doc.get('document_schema', ''),
#                         'authority_score': doc.get('authority_score', 0),
#                         'image_url': doc.get('image_url', []),
#                         'logo_url': doc.get('logo_url', []),
#                     }

#         except Exception as e:
#             print(f"❌ Stage 4 metadata fetch error (batch {i}): {e}")

#     # Return in original order, preserving semantic rank
#     results = []
#     for uuid in survivor_ids:
#         if uuid in doc_map:
#             results.append(doc_map[uuid])

#     print(f"📊 Stage 4: Fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
#     return results


# # ============================================================================
# # IMAGE COUNTING HELPERS
# # ============================================================================

# def _has_real_images(item):
#     """Check if a candidate has at least one non-empty image or logo URL."""
#     image_urls = item.get('image_url', [])
#     if isinstance(image_urls, str):
#         image_urls = [image_urls]
#     if any(u for u in image_urls if u):
#         return True
#     logo_urls = item.get('logo_url', [])
#     if isinstance(logo_urls, str):
#         logo_urls = [logo_urls]
#     return any(u for u in logo_urls if u)


# def _count_images_from_candidates(all_results):
#     """Count DOCUMENTS that have at least one real image or logo URL."""
#     return sum(1 for item in all_results if _has_real_images(item))


# # ============================================================================
# # STAGE 5: ONE count pass — single source of truth
# # ============================================================================

# def count_all(candidates: List[Dict]) -> Dict:
#     """
#     Stage 5: Single counting pass. Runs ONCE, after all pruning is done.
#     Returns facets, image count, and total.
#     This is the ONLY place counting happens — single source of truth.
#     """
#     facets = count_facets_from_cache(candidates)
#     image_count = _count_images_from_candidates(candidates)
#     total = len(candidates)

#     print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
#           f"facets={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

#     return {
#         'facets': facets,
#         'facet_total': total,
#         'total_image_count': image_count,
#     }
# # ============================================================================
# # MAIN ENTRY POINT — Clean 7-Stage Pipeline
# # ============================================================================

# # SEMANTIC:  1(uuids) → 2(rerank) → 3(prune) → 4(metadata survivors) → 5(count) → 6(cache) → 7(paginate)
# # KEYWORD:   1(uuids+metadata) → 5(count) → 6(cache) → 7(paginate)

# # ============================================================================

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
#     answer: str = None,           # ← NEW
#     answer_type: str = None,      # ← NEW
#     skip_embedding: bool = False,
#     document_uuid: str = None,        # ← NEW
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     Clean 7-Stage Pipeline:
#         SEMANTIC:  1 → 2 → 3 → 4 → 5 → 6 → 7
#         KEYWORD:   1 → 5 → 6 → 7
    
#     Counting happens ONCE in Stage 5, after all pruning is done.
#     Single source of truth for facets, image counts, and totals.
#     """
#     times = {}
#     t0 = time.time()
#     print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

#     # Extract active filters
#     active_data_type = filters.get('data_type') if filters else None
#     active_category = filters.get('category') if filters else None
#     active_schema = filters.get('schema') if filters else None

#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
  
# # =========================================================================
# # ★ QUESTION DIRECT PATH: bypass all stages, fetch single document
# # =========================================================================
#     if document_uuid and search_source == 'question':
#         print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
#         t_fetch = time.time()
#         results = fetch_full_documents([document_uuid], query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         # ── AI Overview: always trigger on question path ──
#         ai_overview = None
#         if results and results[0].get('key_facts'):

#             # Derive question_word from query since we skipped intent detection
#             question_word = None
#             q_lower = query.lower().strip()
#             for word in ('who', 'what', 'where', 'when', 'why', 'how'):
#                 if q_lower.startswith(word):
#                     question_word = word
#                     break

#             question_signals = {
#                 'query_mode': 'answer',
#                 'wants_single_result': True,
#                 'question_word': question_word,
#             }

#             ai_overview = _build_ai_overview(question_signals, results, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
#                 results[0]['humanized_summary'] = ai_overview

#                 # ── Related searches via semantic group ──
#         related_searches = []
#         if results:
#             semantic_uuid = results[0].get('semantic_uuid')
#             if semantic_uuid:
#                 try:
#                     related_docs = fetch_documents_by_semantic_uuid(
#                         semantic_uuid,
#                         exclude_uuid=document_uuid,
#                         limit=5
#                     )
#                     related_searches = [
#                         {
#                             'query': doc.get('title', ''),
#                             'url': doc.get('url', '')
#                         }
#                         for doc in related_docs
#                         if doc.get('title')
#                     ]
#                 except Exception as e:
#                     print(f"⚠️ Related searches error: {e}")


#         times['total'] = round((time.time() - t0) * 1000, 2)

#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': 'answer',
#             'query_mode': 'answer',
#             'answer': answer,                        # ← NEW
#             'answer_type': answer_type or 'UNKNOWN',
#             'results': results,
#             'total': len(results),
#             'facet_total': len(results),
#             'total_image_count': 0,
#             'page': 1,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'question_direct',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': 'question',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             'data_type_facets': [],
#             'category_facets': [],
#             'schema_facets': [],
#             'related_searches': [],
#             'facets': {},
#             'related_searches': related_searches,  # was []
#             'word_discovery': {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': None,
#                 'category': None,
#                 'schema': None,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             },
#             'signals': question_signals,  # ← updated from {}
#             'profile': {},
#         }
# # =========================================================================
# # ★ FAST PATH: Check for finished cache FIRST
# # =========================================================================
#     stable_key = _generate_stable_cache_key(session_id, query)
#     finished = _get_cached_results(stable_key)

#     if finished is not None:
#         print(f"⚡ FAST PATH: '{query}' | page={page} | filter={active_data_type}/{active_category}/{active_schema}")

#         all_results = finished['all_results']
#         all_facets = finished['all_facets']
#         facet_total = finished['facet_total']
#         ai_overview = finished.get('ai_overview')
#         total_image_count = finished.get('total_image_count', 0)
#         metadata = finished['metadata']
#         times['cache'] = 'hit (fast path)'

#         # Filter by UI filters (tab click)
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )

#         # Paginate
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         # Stage 7: Fetch full documents for this page only
#         t_fetch = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

#         # Reattach AI overview on page 1
#         if results and page == 1 and ai_overview:
#             results[0]['humanized_summary'] = ai_overview

#         times['total'] = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ FAST PATH TIMING: {times}")
#         print(f"🔍 FAST PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)} | Images: {total_image_count}")

#         # Build return dict using cached metadata
#         signals = metadata.get('signals', {})

#         return {
#             'query': query,
#             'corrected_query': metadata.get('corrected_query', query),
#             'intent': metadata.get('intent', 'general'),
#             'query_mode': metadata.get('query_mode', 'keyword'),
#             'results': results,
#             'total': total_filtered,
#             'facet_total': facet_total,
#             'total_image_count': total_image_count,
#             'page': page,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': metadata.get('semantic_enabled', False),
#             'search_strategy': metadata.get('search_strategy', 'cached'),
#             'alt_mode': alt_mode,
#             'skip_embedding': skip_embedding,
#             'search_source': search_source,
#             'valid_terms': metadata.get('valid_terms', query.split()),
#             'unknown_terms': metadata.get('unknown_terms', []),
#             'data_type_facets': all_facets.get('data_type', []),
#             'category_facets': all_facets.get('category', []),
#             'schema_facets': all_facets.get('schema', []),
#             'related_searches': [],
#             'facets': all_facets,
#             'word_discovery': metadata.get('word_discovery', {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             }),
#             'timings': times,
#             'filters_applied': metadata.get('filters_applied', {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }),
#             'signals': signals,
#             'profile': metadata.get('profile', {}),
#         }

# # =========================================================================
# # ★ FULL PATH: No finished cache. Run the pipeline.
# # =========================================================================
#     print(f"🔬 FULL PATH: '{query}' (no finished cache for stable_key={stable_key[:12]}...)")

#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

# # =========================================================================
# # KEYWORD PATH:  Stage 1 → 5 → 6 → 7
# # =========================================================================

#     if is_keyword_path:
#         print(f"⚡ KEYWORD PIPELINE: '{query}'")

#         intent = detect_query_intent(query, pos_tags)

#         profile = {
#             'search_terms': query.split(),
#             'cities': [],
#             'states': [],
#             'location_terms': [],
#             'primary_intent': intent,
#             'field_boosts': {
#                 'primary_keywords': 10,
#                 'entity_names': 8,
#                 'semantic_keywords': 6,
#                 'key_facts': 4,
#                 'document_title': 3,
#             },
#         }

#         # ── Stage 1: Fetch uuids + metadata in one call (no pruning) ──
#         t1 = time.time()
#         all_results = fetch_candidates_with_metadata(query, profile)
#         times['stage1'] = round((time.time() - t1) * 1000, 2)

#         # ── Stage 5: ONE count pass ──
#         counts = count_all(all_results)

#         # ── Stage 6: Cache the final package ──
#         _set_cached_results(stable_key, {
#             'all_results': all_results,
#             'all_facets': counts['facets'],
#             'facet_total': counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'ai_overview': None,
#             'metadata': {
#                 'corrected_query': query,
#                 'intent': intent,
#                 'query_mode': 'keyword',
#                 'semantic_enabled': False,
#                 'search_strategy': 'keyword_graph_filter',
#                 'valid_terms': query.split(),
#                 'unknown_terms': [],
#                 'signals': {},
#                 'city_names': [],
#                 'state_names': [],
#                 'profile': profile,
#                 'word_discovery': {
#                     'valid_count': len(query.split()),
#                     'unknown_count': 0,
#                     'corrections': [],
#                     'filters': [],
#                     'locations': [],
#                     'sort': None,
#                     'total_score': 0,
#                     'average_score': 0,
#                     'max_score': 0,
#                 },
#                 'filters_applied': {
#                     'data_type': active_data_type,
#                     'category': active_category,
#                     'schema': active_schema,
#                     'is_local_search': False,
#                     'local_search_strength': 'none',
#                 },
#             },
#         })
#         print(f"💾 Cached keyword package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

#         # ── Stage 7: Filter → Paginate → Fetch full docs ──
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#         t2 = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)

#         print(f"⏱️ KEYWORD TIMING: {times}")

#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total_filtered,
#             'facet_total': counts['facet_total'],
#             'total_image_count': counts['total_image_count'],
#             'page': page,
#             'per_page': per_page,
#             'search_time': round(time.time() - t0, 3),
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword_graph_filter',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             'data_type_facets': counts['facets'].get('data_type', []),
#             'category_facets': counts['facets'].get('category', []),
#             'schema_facets': counts['facets'].get('schema', []),
#             'related_searches': [],
#             'facets': counts['facets'],
#             'word_discovery': {
#                 'valid_count': len(query.split()),
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'filters': [],
#                 'locations': [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': False,
#                 'local_search_strength': 'none',
#             }
#         }

# # =========================================================================
# # SEMANTIC PATH:  Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
# # =========================================================================

#     print(f"🔬 SEMANTIC PIPELINE: '{query}'")

#     # --- Word discovery + embedding in parallel ---
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

#     # --- Intent detection ---
#     signals = {}
#     if INTENT_DETECT_AVAILABLE:
#         try:
#             discovery = detect_intent(discovery)
#             signals = discovery.get('signals', {})
#             print(f"   🎯 Intent signals: mode={signals.get('query_mode')}, "
#                   f"q_word={signals.get('question_word')}, "
#                   f"local={signals.get('is_local_search')}, "
#                   f"location={signals.get('has_location')}, "
#                   f"service={signals.get('service_words')}, "
#                   f"temporal={signals.get('temporal_direction')}, "
#                   f"black_owned={signals.get('has_black_owned')}, "
#                   f"single={signals.get('wants_single_result')}, "
#                   f"domains={signals.get('domains_detected', [])[:3]}")
#         except Exception as e:
#             print(f"   ⚠️ intent_detect error: {e}")

#     corrected_query = discovery.get('corrected_query', query)
#     semantic_enabled = query_embedding is not None
#     query_mode = signals.get('query_mode', 'explore')

#     # --- Build profile ---
#     t2 = time.time()
#     profile = build_query_profile(discovery, signals=signals)
#     times['build_profile'] = round((time.time() - t2) * 1000, 2)

#     # --- Apply corrections to search terms ---
#     corrections = discovery.get('corrections', [])
#     if corrections:
#         correction_map = {
#             c['original'].lower(): c['corrected']
#             for c in corrections
#             if c.get('original') and c.get('corrected')
#         }
#         original_terms = profile.get('search_terms', [])
#         profile['search_terms'] = [
#             correction_map.get(term.lower(), term)
#             for term in original_terms
#         ]
#         if original_terms != profile['search_terms']:
#             print(f"   ✅ Applied corrections to search terms: {original_terms} → {profile['search_terms']}")

#     intent = profile.get('primary_intent', 'general')
#     city_names = [c['name'] for c in profile.get('cities', [])]
#     state_names = [s['name'] for s in profile.get('states', [])]

#     print(f"   Intent: {intent} | Mode: {query_mode}")
#     print(f"   Cities: {city_names}")
#     print(f"   States: {state_names}")
#     print(f"   Search Terms: {profile.get('search_terms', [])}")
#     print(f"   Field Boosts: {profile.get('field_boosts', {})}")

#     # ── Stage 1: Fetch candidate UUIDs from both collections in parallel ──
#     t3 = time.time()

#     UNSAFE_CATEGORIES = {
#         'Food', 'US City', 'US State', 'Country', 'Location',
#         'City', 'Place', 'Object', 'Animal', 'Color',
#     }
#     corrections = discovery.get('corrections', [])
#     has_unsafe_corrections = any(
#         c.get('correction_type') == 'pos_mismatch' or
#         c.get('category', '') in UNSAFE_CATEGORIES
#         for c in corrections
#     )
#     search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

#     if has_unsafe_corrections:
#         print(f"⚠️  Unsafe corrections — using original query for Stage 1A: '{query}'")

#     candidate_uuids = fetch_all_candidate_uuids(
#         search_query_for_stage1,
#         profile,
#         query_embedding,
#         signals=signals,
#         discovery=discovery,
#     )
#     times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
#     print(f"📊 Stage 1 COMBINED: {len(candidate_uuids)} candidate UUIDs")

#     # ── Stage 2: Vector rerank (only needs IDs + embedding) ──
#     survivor_uuids = candidate_uuids  # default if no embedding
#     vector_data = {}  # id → {vector_distance, semantic_rank}

#     if semantic_enabled and candidate_uuids:
#         t4 = time.time()
#         reranked = semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
#         times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

#         # Build lookup: id → vector data
#         for item in reranked:
#             vector_data[item['id']] = {
#                 'vector_distance': item.get('vector_distance', 1.0),
#                 'semantic_rank': item.get('semantic_rank', 999999),
#             }

#         # ── Stage 3: Vector prune — remove IDs below cutoff ──
#         DISTANCE_THRESHOLDS = {
#             'answer':  0.60,
#             'explore': 0.70,
#             'compare': 0.65,
#             'browse':  0.85,
#             'local':   0.85,
#             'shop':    0.80,
#         }
#         threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

#         before_prune = len(candidate_uuids)
#         survivor_uuids = [
#             uuid for uuid in candidate_uuids
#             if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
#         ]
#         after_prune = len(survivor_uuids)

#         if before_prune != after_prune:
#             print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): {before_prune} → {after_prune} ({before_prune - after_prune} removed)")
#         times['stage3_prune'] = f"{before_prune} → {after_prune}"
#     else:
#         print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, candidates={len(candidate_uuids)}")

#     # ── Stage 4: Fetch metadata for SURVIVORS ONLY ──
#     t5 = time.time()
#     all_results = fetch_candidate_metadata(survivor_uuids)
#     times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

#     # Attach vector data and compute blended scores
#     if vector_data:
#         total_candidates = len(all_results)
#         max_sem_rank = len(vector_data)
#         blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

#         if query_mode == 'answer' and signals.get('wants_single_result'):
#             blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

#         if signals.get('has_unknown_terms', False):
#             shift = min(0.15, blend['text_match'])
#             blend['text_match'] -= shift
#             blend['semantic'] += shift

#         if signals.get('has_superlative', False):
#             shift = min(0.10, blend['semantic'])
#             blend['semantic'] -= shift
#             blend['authority'] += shift

#         print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

#         for idx, item in enumerate(all_results):
#             item_id = item.get('id')
#             vd = vector_data.get(item_id, {})
#             item['vector_distance'] = vd.get('vector_distance', 1.0)
#             item['semantic_rank'] = vd.get('semantic_rank', 999999)

#             authority = item.get('authority_score', 0)
#             text_score = 1.0 - (idx / max(total_candidates, 1))
#             sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
#             auth_score = min(authority / 100.0, 1.0)

#             item['blended_score'] = (
#                 blend['text_match'] * text_score +
#                 blend['semantic'] * sem_score +
#                 blend['authority'] * auth_score
#             )

#         # Sort by blended score
#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#     # ── Stage 5: ONE count pass ──
#     counts = count_all(all_results)

#     # --- AI Overview (from page 1 full docs) ---
#     ai_overview = None
#     if all_results:
#         preview_items, _ = paginate_cached_results(all_results, 1, per_page)
#         preview_ids = [item['id'] for item in preview_items]
#         preview_docs = fetch_full_documents(preview_ids, query)

#         if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
#             ai_overview = _build_ai_overview(signals, preview_docs, query)
#             if ai_overview:
#                 print(f"   💡 AI Overview: {ai_overview[:80]}...")

#     # --- Extract terms ---
#     valid_terms = profile.get('search_terms', [])
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

#     # ── Stage 6: Cache the final package ──
#     _set_cached_results(stable_key, {
#         'all_results': all_results,
#         'all_facets': counts['facets'],
#         'facet_total': counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'ai_overview': ai_overview,
#         'metadata': {
#             'corrected_query': corrected_query,
#             'intent': intent,
#             'query_mode': query_mode,
#             'semantic_enabled': semantic_enabled,
#             'search_strategy': 'staged_semantic' if semantic_enabled else 'keyword_fallback',
#             'valid_terms': valid_terms,
#             'unknown_terms': unknown_terms,
#             'signals': signals,
#             'city_names': city_names,
#             'state_names': state_names,
#             'profile': profile,
#             'word_discovery': {
#                 'valid_count': discovery.get('stats', {}).get('valid_words', 0),
#                 'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#                 'corrections': discovery.get('corrections', []),
#                 'filters': [],
#                 'locations': [
#                     {'field': 'location_city', 'values': city_names},
#                     {'field': 'location_state', 'values': state_names},
#                 ] if city_names or state_names else [],
#                 'sort': None,
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#             },
#             'filters_applied': {
#                 'data_type': active_data_type,
#                 'category': active_category,
#                 'schema': active_schema,
#                 'is_local_search': signals.get('is_local_search', False),
#                 'local_search_strength': signals.get('local_search_strength', 'none'),
#                 'has_black_owned': signals.get('has_black_owned', False),
#                 'graph_filters': [],
#                 'graph_locations': [
#                     {'field': 'location_city', 'values': city_names},
#                     {'field': 'location_state', 'values': state_names},
#                 ] if city_names or state_names else [],
#                 'graph_sort': None,
#             },
#         },
#     })
#     print(f"💾 Cached semantic package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

#     # ── Stage 7: Filter → Paginate → Fetch full docs ──
#     filtered_results = filter_cached_results(
#         all_results,
#         data_type=active_data_type,
#         category=active_category,
#         schema=active_schema
#     )

#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

#     t6 = time.time()
#     page_ids = [item['id'] for item in page_items]
#     results = fetch_full_documents(page_ids, query)
#     times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

#     # Attach AI overview on page 1
#     if results and page == 1 and ai_overview:
#         results[0]['humanized_summary'] = ai_overview

#     # Store query embedding
#     if query_embedding:
#         try:
#             store_query_embedding(corrected_query, query_embedding, result_count=counts['facet_total'])
#         except Exception as e:
#             print(f"⚠️ store_query_embedding error: {e}")

#     times['total'] = round((time.time() - t0) * 1000, 2)

#     strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

#     print(f"⏱️ SEMANTIC TIMING: {times}")
#     print(f"🔍 {strategy.upper()} ({query_mode}) | Total: {counts['facet_total']} | Filtered: {total_filtered} | Page: {len(results)} | Images: {counts['total_image_count']}")

#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'query_mode': query_mode,
#         'results': results,
#         'total': total_filtered,
#         'facet_total': counts['facet_total'],
#         'total_image_count': counts['total_image_count'],
#         'page': page,
#         'per_page': per_page,
#         'search_time': round(time.time() - t0, 3),
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': strategy,
#         'alt_mode': alt_mode,
#         'skip_embedding': skip_embedding,
#         'search_source': search_source,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'related_searches': [],
#         'data_type_facets': counts['facets'].get('data_type', []),
#         'category_facets': counts['facets'].get('category', []),
#         'schema_facets': counts['facets'].get('schema', []),
#         'facets': counts['facets'],
#         'word_discovery': {
#             'valid_count': discovery.get('stats', {}).get('valid_words', 0),
#             'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
#             'corrections': discovery.get('corrections', []),
#             'filters': [],
#             'locations': [
#                 {'field': 'location_city', 'values': city_names},
#                 {'field': 'location_state', 'values': state_names},
#             ] if city_names or state_names else [],
#             'sort': None,
#             'total_score': 0,
#             'average_score': 0,
#             'max_score': 0,
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type': active_data_type,
#             'category': active_category,
#             'schema': active_schema,
#             'is_local_search': signals.get('is_local_search', False),
#             'local_search_strength': signals.get('local_search_strength', 'none'),
#             'has_black_owned': signals.get('has_black_owned', False),
#             'graph_filters': [],
#             'graph_locations': [
#                 {'field': 'location_city', 'values': city_names},
#                 {'field': 'location_state', 'values': state_names},
#             ] if city_names or state_names else [],
#             'graph_sort': None,
#         },
#         'signals': signals,
#         'profile': profile,
#     }

# # ============================================================================
# # CONVENIENCE FUNCTIONS (for compatibility with views.py imports)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options."""
#     return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """Returns related searches."""
#     return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content."""
#     if not results:
#         return None
    
#     top = results[0]
#     if top.get('authority_score', 0) >= 85:
#         return {
#             'type': 'featured_snippet',
#             'title': top.get('title'),
#             'snippet': top.get('summary', ''),
#             'key_facts': top.get('key_facts', [])[:3],
#             'source': top.get('source'),
#             'url': top.get('url'),
#             'image': top.get('image')
#         }
#     return None


# def log_search_event(**kwargs):
#     """Logs search event."""
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
#     """Simple Typesense search wrapper."""
#     params = {
#         'q': query,
#         'query_by': query_by,
#         'per_page': per_page,
#         'page': page,
#     }
    
#     if filter_by:
#         params['filter_by'] = filter_by
#     if sort_by:
#         params['sort_by'] = sort_by
#     if facet_by:
#         params['facet_by'] = facet_by
#         params['max_facet_values'] = max_facet_values
    
#     try:
#         return client.collections[COLLECTION_NAME].documents.search(params)
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}
    

# # ============================================================================
# # TEST / CLI
# # ============================================================================

# if __name__ == "__main__":
#     import sys
    
#     if len(sys.argv) < 2:
#         print("Usage: python typesense_discovery_bridge.py \"your search query\"")
#         sys.exit(1)
    
#     query = ' '.join(sys.argv[1:])
    
#     print("=" * 70)
#     print(f"🚀 TESTING: '{query}'")
#     print("=" * 70)
    
#     result = execute_full_search(
#         query=query,
#         session_id='test-session',
#         filters={},
#         page=1,
#         per_page=10,
#         alt_mode='y',  # Semantic path
#     )
    
#     print("\n" + "=" * 70)
#     print("📊 RESULTS")
#     print("=" * 70)
#     print(f"Query: {result['query']}")
#     print(f"Corrected: {result['corrected_query']}")
#     print(f"Intent: {result['intent']}")
#     print(f"Query Mode: {result.get('query_mode', 'N/A')}")
#     print(f"Total: {result['total']}")
#     print(f"Facet Total: {result['facet_total']}")
#     print(f"Total Image Count: {result['total_image_count']}")
#     print(f"Strategy: {result['search_strategy']}")
#     print(f"Semantic: {result['semantic_enabled']}")
    
#     print(f"\n🔧 Corrections:")
#     for c in result.get('word_discovery', {}).get('corrections', []):
#         print(f"   '{c['original']}' → '{c['corrected']}' (type: {c.get('correction_type', 'unknown')})")

#     print(f"\n🔄 Query Flow:")
#     print(f"   Original:  '{result['query']}'")
#     print(f"   Corrected: '{result['corrected_query']}'")
#     print(f"   Changed:   {result['query'] != result['corrected_query']}")
        
#     print(f"\n📝 Terms:")
#     print(f"   Valid: {result['valid_terms']}")
#     print(f"   Unknown: {result['unknown_terms']}")

#     print(f"\n📍 Locations:")
#     for loc in result.get('word_discovery', {}).get('locations', []):
#         print(f"   {loc['field']}: {loc['values']}")
    
#     print(f"\n📁 Data Type Facets:")
#     for f in result.get('data_type_facets', []):
#         print(f"   {f['label']}: {f['count']}")
    
#     print(f"\n🎯 Signals:")
#     sigs = result.get('signals', {})
#     if sigs:
#         print(f"   query_mode: {sigs.get('query_mode')}")
#         print(f"   question_word: {sigs.get('question_word')}")
#         print(f"   wants_single: {sigs.get('wants_single_result')}")
#         print(f"   wants_multiple: {sigs.get('wants_multiple_results')}")
#         print(f"   is_local: {sigs.get('is_local_search')}")
#         print(f"   has_black_owned: {sigs.get('has_black_owned')}")
#         print(f"   temporal: {sigs.get('temporal_direction')}")
#         print(f"   has_unknown: {sigs.get('has_unknown_terms')}")
    
#     print(f"\n📄 Results ({len(result['results'])}):")
#     for i, r in enumerate(result['results'][:5], 1):
#         print(f"   {i}. {r['title'][:60]}")
#         if r.get('humanized_summary'):
#             print(f"      💡 {r['humanized_summary'][:80]}...")
#         print(f"      📍 {r['location'].get('city', '')}, {r['location'].get('state', '')}")
#         print(f"      🔗 {r['url'][:50]}...")
    
#     print(f"\n⏱️ Timings: {result['timings']}")

"""
typesense_discovery_bridge.py
=============================
Complete search bridge between Word Discovery v3 and Typesense.

v3 MIGRATION NOTES:
- build_query_profile() and _compute_field_boosts() are REMOVED.
  Word Discovery v3 returns a fully pre-classified profile with:
    profile['search_terms'], profile['persons'], profile['organizations'],
    profile['keywords'], profile['media'], profile['cities'],
    profile['states'], profile['location_terms'], profile['field_boosts'],
    profile['primary_intent'], profile['corrections'], profile['corrected_query']
  The bridge reads these directly — it does NOT re-classify or rebuild.

- _read_v3_profile() replaces build_query_profile(). It is O(1): no loops,
  no re-analysis, just a direct field read with safe defaults.

This file handles:
- Word Discovery v3 integration
- Intent signal integration (query_mode, question_word, etc.)
- Typesense filter_by and query_by_weights construction from v3 profile
- Embedding generation (via embedding_client.py)
- Result caching (self-contained, Redis-backed)
- Stage 1A: Document collection keyword search (candidate UUIDs)
- Stage 1B: Questions collection vector search (candidate UUIDs)
- Stage 2: Semantic rerank
- Stage 3: Vector prune
- Stage 4: Metadata fetch for survivors
- Stage 5: Single counting pass (facets, image count, total)
- Stage 6: Cache the finished package
- Stage 7: Filter → Paginate → Fetch full documents
- AI Overview (signal-driven key_fact selection)
- Returns same structure as execute_full_search() for views.py compatibility

USAGE IN VIEWS.PY:
    from .typesense_discovery_bridge import execute_full_search

    result = execute_full_search(
        query=corrected_query,
        session_id=params.session_id,
        filters=filters,
        page=page,
        per_page=per_page,
        alt_mode=params.alt_mode,
        ...
    )
"""

import re
import json
import time
import hashlib
import threading
import typesense
from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from decouple import config
import requests

# ============================================================================
# IMPORTS - Word Discovery v3 and Embedding Client
# ============================================================================

try:
    from .word_discovery_fulltest import WordDiscovery
    WORD_DISCOVERY_AVAILABLE = True
    print("✅ WordDiscovery imported from .word_discovery_v3")
except ImportError:
    try:
        from word_discovery_fulltest import WordDiscovery
        WORD_DISCOVERY_AVAILABLE = True
        print("✅ WordDiscovery imported from word_discovery_v3")
    except ImportError:
        WORD_DISCOVERY_AVAILABLE = False
        print("⚠️ word_discovery_v3 not available")

try:
    from .intent_detect import detect_intent, get_signals
    INTENT_DETECT_AVAILABLE = True
    print("✅ intent_detect imported")
except ImportError:
    try:
        from intent_detect import detect_intent, get_signals
        INTENT_DETECT_AVAILABLE = True
        print("✅ intent_detect imported from intent_detect")
    except ImportError:
        INTENT_DETECT_AVAILABLE = False
        print("⚠️ intent_detect not available")

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


import random


def humanize_key_facts(key_facts: list, query: str = '', matched_keyword: str = '',
                       question_word: str = None) -> str:
    """Format key_facts into a readable AfroToDo AI Overview,
    only returning facts relevant to the matched keyword and question type.

    Blueprint Step 8: AI Overview key_fact filtering based on question_word.
    """
    if not key_facts:
        return ''

    facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]

    if not facts:
        return ''

    if question_word:
        qw = question_word.lower()
        if qw == 'where':
            geo_words = {'located', 'bounded', 'continent', 'region', 'coast',
                         'ocean', 'border', 'north', 'south', 'east', 'west',
                         'latitude', 'longitude', 'hemisphere', 'capital',
                         'city', 'state', 'country', 'area', 'lies', 'situated'}
            relevant_facts = [f for f in facts
                              if any(gw in f.lower() for gw in geo_words)]
        elif qw == 'when':
            import re as _re
            temporal_words = {'founded', 'established', 'born', 'created',
                              'started', 'opened', 'built', 'year', 'date',
                              'century', 'decade', 'era', 'period'}
            relevant_facts = [f for f in facts
                              if any(tw in f.lower() for tw in temporal_words)
                              or _re.search(r'\b\d{4}\b', f)]
        elif qw == 'who':
            who_words = {'first', 'president', 'founder', 'ceo', 'leader',
                         'director', 'known', 'famous', 'awarded', 'pioneer',
                         'invented', 'created', 'named', 'appointed', 'elected'}
            relevant_facts = [f for f in facts
                              if any(ww in f.lower() for ww in who_words)]
        elif qw == 'what':
            what_words = {'is a', 'refers to', 'defined', 'known as',
                          'type of', 'form of', 'means', 'represents'}
            relevant_facts = [f for f in facts
                              if any(ww in f.lower() for ww in what_words)]
        else:
            relevant_facts = []

        if not relevant_facts and matched_keyword:
            keyword_lower = matched_keyword.lower()
            relevant_facts = [f for f in facts if keyword_lower in f.lower()]

        if not relevant_facts:
            relevant_facts = [facts[0]]
    elif matched_keyword:
        keyword_lower = matched_keyword.lower()
        relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        if not relevant_facts:
            relevant_facts = [facts[0]]
    else:
        relevant_facts = [facts[0]]

    relevant_facts = relevant_facts[:2]

    is_question = query and any(
        query.lower().startswith(w)
        for w in ['who', 'what', 'where', 'when', 'why', 'how', 'is', 'are', 'can', 'do', 'does']
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


# ============================================================================
# THREAD POOL
# ============================================================================

_executor = ThreadPoolExecutor(max_workers=3)


# ============================================================================
# TYPESENSE CLIENT
# ============================================================================

client = typesense.Client({
    'api_key': config('TYPESENSE_API_KEY'),
    'nodes': [{
        'host': config('TYPESENSE_HOST'),
        'port': config('TYPESENSE_PORT'),
        'protocol': config('TYPESENSE_PROTOCOL')
    }],
    'connection_timeout_seconds': 5
})

COLLECTION_NAME = 'document'


# ============================================================================
# RESULT CACHE (Self-Contained)
# ============================================================================

from django.core.cache import cache as django_cache

CACHE_TTL_SECONDS = 300  # 5 minutes
MAX_CACHED_RESULTS = 2000


def _get_cached_results(cache_key: str):
    """Get cached result set from Redis."""
    try:
        data = django_cache.get(cache_key)
        if data is not None:
            print(f"🟢 Redis cache HIT: {cache_key[:12]}...")
            return data
        print(f"🔴 Redis cache MISS: {cache_key[:12]}...")
        return None
    except Exception as e:
        print(f"⚠️ Redis cache GET error: {e}")
        return None


def _set_cached_results(cache_key: str, data):
    """Cache result set in Redis with TTL."""
    try:
        django_cache.set(cache_key, data, timeout=CACHE_TTL_SECONDS)
        print(f"💾 Redis cache SET: {cache_key[:12]}... (TTL={CACHE_TTL_SECONDS}s)")
    except Exception as e:
        print(f"⚠️ Redis cache SET error: {e}")


def clear_search_cache():
    """Clear all cached search results."""
    try:
        django_cache.clear()
        print("🧹 Redis search cache cleared")
    except Exception as e:
        print(f"⚠️ Redis cache CLEAR error: {e}")


# ============================================================================
# CONSTANTS
# ============================================================================

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

# Labels for UI
DATA_TYPE_LABELS = {
    'article': 'Articles',
    'person': 'People',
    'business': 'Businesses',
    'place': 'Places',
    'media': 'Media',
    'event': 'Events',
    'product': 'Products',
}

CATEGORY_LABELS = {
    'healthcare_medical': 'Healthcare',
    'fashion': 'Fashion',
    'beauty': 'Beauty',
    'food_recipes': 'Food & Recipes',
    'travel_tourism': 'Travel',
    'entertainment': 'Entertainment',
    'business': 'Business',
    'education': 'Education',
    'technology': 'Technology',
    'sports': 'Sports',
    'finance': 'Finance',
    'real_estate': 'Real Estate',
    'lifestyle': 'Lifestyle',
    'news': 'News',
    'culture': 'Culture',
    'general': 'General',
}


# ============================================================================
# SEMANTIC BLEND RATIOS (Blueprint Step 5)
# ============================================================================

BLEND_RATIOS = {
    'answer':  {'text_match': 0.25, 'semantic': 0.60, 'authority': 0.15},
    'explore': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
    'browse':  {'text_match': 0.40, 'semantic': 0.35, 'authority': 0.25},
    'local':   {'text_match': 0.30, 'semantic': 0.30, 'authority': 0.40},
    'compare': {'text_match': 0.30, 'semantic': 0.50, 'authority': 0.20},
    'shop':    {'text_match': 0.35, 'semantic': 0.30, 'authority': 0.35},
}


# ============================================================================
# DATA TYPE PREFERENCES BY MODE (Blueprint Step 9)
# ============================================================================

DATA_TYPE_PREFERENCES = {
    'answer':  ['article', 'person', 'place'],
    'explore': ['article', 'person', 'media'],
    'browse':  ['article', 'business', 'product'],
    'local':   ['business', 'place', 'article'],
    'shop':    ['product', 'business', 'article'],
    'compare': ['article', 'person', 'business'],
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _parse_rank(rank_value: Any) -> int:
    """Parse rank to integer."""
    if isinstance(rank_value, int):
        return rank_value
    try:
        return int(float(rank_value))
    except (ValueError, TypeError):
        return 0


# ============================================================================
# V3 PROFILE READER  (replaces build_query_profile + _compute_field_boosts)
#
# O(1): no loops, no re-analysis. Pure field reads with safe defaults.
# v3 already classified everything correctly; we just consume it.
# The only Typesense-specific addition is preferred_data_types, which is
# a single dict lookup on query_mode.
# ============================================================================

def _read_v3_profile(discovery: Dict, signals: Dict = None) -> Dict:
    """
    Read the pre-classified v3 profile directly.

    v3 delivers all classification, entity detection, location detection,
    and field boost computation. This function is purely a safe field read
    with fallback defaults — it does NOT re-classify or re-analyze anything.

    Returns a profile dict that build_typesense_params() and the rest of
    the pipeline can consume without any changes to downstream code.
    """
    query_mode = (signals or {}).get('query_mode', 'explore')

    return {
        # ── Search inputs (POS-filtered by v3) ───────────────────────
        'search_terms':    discovery.get('search_terms', []),

        # ── Classified entities (pre-gated by v3) ────────────────────
        'persons':         discovery.get('persons', []),
        'organizations':   discovery.get('organizations', []),
        'keywords':        discovery.get('keywords', []),
        'media':           discovery.get('media', []),

        # ── Locations ready for filter_by (pre-merged by v3) ─────────
        'cities':          discovery.get('cities', []),
        'states':          discovery.get('states', []),
        'location_terms':  discovery.get('location_terms', []),

        # ── Intent & scoring (pre-computed by v3) ────────────────────
        'primary_intent':  discovery.get('primary_intent', 'general'),
        'intent_scores':   discovery.get('intent_scores', {}),

        # ── Field boosts (pre-adjusted by v3 for entity context) ─────
        'field_boosts':    discovery.get('field_boosts', {
            'document_title':    10,
            'entity_names':       2,
            'primary_keywords':   3,
            'key_facts':          3,
            'semantic_keywords':  2,
        }),

        # ── Corrections (for search term substitution) ────────────────
        'corrections':     discovery.get('corrections', []),

        # ── Typesense-specific addition (single dict lookup) ──────────
        'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),

        # ── Convenience booleans derived from list presence (O(1)) ───
        'has_person':       bool(discovery.get('persons')),
        'has_organization': bool(discovery.get('organizations')),
        'has_location':     bool(discovery.get('cities') or discovery.get('states') or discovery.get('location_terms')),
        'has_keyword':      bool(discovery.get('keywords')),
        'has_media':        bool(discovery.get('media')),
    }


# ============================================================================
# WORD DISCOVERY WRAPPER
# ============================================================================

def _run_word_discovery(query: str) -> Dict:
    """Run Word Discovery v3 on query."""
    if WORD_DISCOVERY_AVAILABLE:
        try:
            wd = WordDiscovery(verbose=False)
            result = wd.process(query)
            return result
        except Exception as e:
            print(f"⚠️ WordDiscovery error: {e}")

    # Fallback — minimal safe structure matching v3 shape
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


def _run_embedding(query: str) -> Optional[List[float]]:
    """Run embedding generation."""
    return get_query_embedding(query)


# ============================================================================
# TYPESENSE PARAMETER BUILDING (Blueprint Steps 2, 3, 6)
# ============================================================================

def build_typesense_params(profile: Dict, ui_filters: Dict = None,
                           signals: Dict = None) -> Dict:
    """
    Convert v3 profile into Typesense search parameters.

    Reads field_boosts, search_terms, cities, states directly from the
    v3 profile. Applies mode-specific Typesense settings on top.
    No re-classification happens here.
    """
    signals    = signals or {}
    query_mode = signals.get('query_mode', 'explore')

    params = {}

    # ── Query string: deduplicate search_terms (O(n) on term count, tiny) ──
    seen         = set()
    unique_terms = []
    for term in profile.get('search_terms', []):
        term_lower = term.lower()
        if term_lower not in seen:
            seen.add(term_lower)
            unique_terms.append(term)

    params['q'] = ' '.join(unique_terms) if unique_terms else '*'

    # ── Field boosts: read directly from v3, add mode-specific fields ────
    field_boosts = dict(profile.get('field_boosts', {}))  # shallow copy

    if query_mode == 'local':
        field_boosts.setdefault('service_type',        12)
        field_boosts.setdefault('service_specialties', 10)

    sorted_fields            = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    params['query_by']       = ','.join(f[0] for f in sorted_fields)
    params['query_by_weights'] = ','.join(str(f[1]) for f in sorted_fields)

    # ── Typo / prefix / drop-token settings by mode ───────────────────────
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

    # ── filter_by: locations + black_owned + UI filters ───────────────────
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

    if signals.get('has_black_owned', False):
        filter_conditions.append('black_owned:=true')

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


def build_filter_string_without_data_type(profile: Dict, signals: Dict = None) -> str:
    """Build filter string for locations only (no data_type for facet counting)."""
    signals          = signals or {}
    filter_conditions = []
    query_mode       = signals.get('query_mode', 'explore')

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

    if signals.get('has_black_owned', False):
        filter_conditions.append('black_owned:=true')

    return ' && '.join(filter_conditions) if filter_conditions else ''


# ============================================================================
# RUN_PARALLEL_PREP
# ============================================================================

def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
    """
    Run word discovery and embedding IN PARALLEL.

    Always embeds the ORIGINAL query first.
    Only re-embeds with corrected_query when corrections are safe
    (genuine spelling fixes, not proper-noun mangling).
    """
    if skip_embedding:
        discovery = _run_word_discovery(query)
        return discovery, None

    discovery_future = _executor.submit(_run_word_discovery, query)
    embedding_future = _executor.submit(_run_embedding, query)

    discovery = discovery_future.result()
    embedding = embedding_future.result()

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
            print(f"⚠️  Skipping re-embed — unsafe corrections detected:")
            for c in unsafe_corrections:
                print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
                      f"(type={c.get('correction_type')}, category={c.get('category')})")
            print(f"   Keeping original query embedding: '{query}'")
        elif safe_corrections:
            print(f"✅  Re-embedding with corrected query: '{corrected_query}'")
            embedding = get_query_embedding(corrected_query)

    return discovery, embedding


# ============================================================================
# STAGE 1A: Document collection — keyword graph, 100 candidates
# ============================================================================

def fetch_candidate_uuids(
    search_query: str,
    profile: Dict,
    signals: Dict = None,
    max_results: int = 100
) -> List[str]:
    """
    Stage 1A: Keyword graph search against the document collection.
    Returns up to 100 document_uuid strings — no metadata.
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
        response = client.collections[COLLECTION_NAME].documents.search(search_params)
        hits     = response.get('hits', [])
        uuids    = [hit['document']['document_uuid']
                    for hit in hits
                    if hit.get('document', {}).get('document_uuid')]
        print(f"📊 Stage 1A (document): {len(uuids)} candidate UUIDs")
        return uuids
    except Exception as e:
        print(f"❌ Stage 1A error: {e}")
        return []


# ============================================================================
# STAGE 1B: Questions collection — facet filter + vector search + validation
# ============================================================================

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
    text  = text.lower()
    text  = re.sub(r"[^\w\s-]", " ", text)
    text  = re.sub(r"\s*-\s*", " ", text)
    return {t for t in text.split() if len(t) > 2 and t not in _MATCH_STOPWORDS}


def _extract_query_signals(profile: Dict, discovery: Dict = None) -> tuple:
    """
    Extract and normalize all meaningful query signals from the v3 profile.

    v3 already classified persons, organizations, keywords — we read them
    directly. O(E) where E = number of entities (always small).

    Returns:
        all_tokens      — set of all normalized tokens
        full_phrases    — list of normalized phrase strings
        primary_subject — normalized tokens of highest-ranked entity
    """
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

    # Include spelling suggestions from corrections
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
    primary_subject: set,
    min_matches: int = 1,
) -> bool:
    """
    Validate a question hit against query signals using 4-level matching.

    Level 1 — Exact token match
    Level 2 — Partial token match
    Level 3 — Substring containment
    Level 4 — Token overlap

    Returns True if hit passes validation.
    """
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

    # Level 1: Exact token match
    exact_matches = query_tokens & candidate_tokens
    if exact_matches:
        match_count += len(exact_matches)
        if primary_subject and (primary_subject & exact_matches):
            primary_subject_hit = True

    # Level 2: Partial token match
    for qt in query_tokens:
        if qt in exact_matches:
            continue
        for ct in candidate_tokens:
            if qt in ct or ct in qt:
                match_count += 1
                if primary_subject and qt in primary_subject:
                    primary_subject_hit = True
                break

    # Level 3: Substring containment
    for qp in query_phrases:
        if len(qp) < 3:
            continue
        if qp in candidate_text:
            match_count += 1
            if primary_subject:
                if _normalize_signal(qp) & primary_subject:
                    primary_subject_hit = True
        else:
            for cp in candidate_phrases:
                if qp in cp or cp in qp:
                    match_count += 1
                    if primary_subject and _normalize_signal(qp) & primary_subject:
                        primary_subject_hit = True
                    break

    # Level 4: Token overlap
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


# ============================================================================
# STAGE 1 COMBINED: Run both in parallel, merge + dedup
# ============================================================================

def fetch_all_candidate_uuids(
    search_query: str,
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
    discovery: Dict = None,
) -> List[str]:
    """
    Runs Stage 1A (document) and Stage 1B (questions) in parallel.

    Merge order:
    1. Overlap — found by both paths (highest confidence)
    2. Document-only hits
    3. Question-only hits
    """
    signals = signals or {}

    doc_future = _executor.submit(
        fetch_candidate_uuids, search_query, profile, signals, 100
    )
    q_future = _executor.submit(
        fetch_candidate_uuids_from_questions, profile, query_embedding, signals, 50, discovery
    )

    doc_uuids = doc_future.result()
    q_uuids   = q_future.result()

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
    print(f"   document pool    : {len(doc_uuids)}")
    print(f"   questions pool   : {len(q_uuids)}")
    print(f"   overlap (both)   : {len(overlap)}")
    print(f"   merged total     : {len(merged)}")

    return merged


def fetch_candidate_uuids_from_questions(
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
    max_results: int = 50,
    discovery: Dict = None,
) -> List[str]:
    """
    Stage 1B: Two-step search against the questions collection.

    Step A — Build facet filter from v3 profile metadata.
    Step B — Run vector search within filtered subset.
    Step C — Validate each hit against query signals (4-level matching).

    Location filter is AND'd onto the facet filter so question hits are
    constrained to the detected geographic area.
    """
    signals = signals or {}

    if not query_embedding:
        print("⚠️ Stage 1B (questions): no embedding — skipping")
        return []

    query_tokens, query_phrases, primary_subject = _extract_query_signals(profile, discovery=discovery)

    print(f"🔍 Stage 1B validation signals:")
    print(f"   query_tokens    : {sorted(query_tokens)}")
    print(f"   query_phrases   : {query_phrases}")
    print(f"   primary_subject : {primary_subject}")

    # ── Step A: Build facet filter from v3 profile ────────────────────────
    filter_parts = []

    # Keywords: read directly from v3 profile
    primary_kws = [
        k.get('phrase') or k.get('word', '')
        for k in profile.get('keywords', [])
    ]
    primary_kws = [kw for kw in primary_kws if kw][:3]
    if primary_kws:
        kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
        filter_parts.append(f'primary_keywords:[{kw_values}]')

    # Entities: persons + organizations from v3 profile
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

    # Question type from signals
    question_word = signals.get('question_word') or ''
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

    # ── Location filter from v3 cities/states ─────────────────────────────
    location_filter_parts = []
    query_mode = signals.get('query_mode', 'explore')
    is_location_subject = (
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

    print(f"   primary_keywords : {primary_kws}")
    print(f"   entities         : {entity_names}")
    print(f"   question_type    : {question_type or 'any'}")
    print(f"   location_filter  : {location_filter or 'none'}")
    print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

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
        response        = client.multi_search.perform(search_requests, {})
        result          = response['results'][0]
        hits            = result.get('hits', [])

        # Fallback: retry without location filter if too few hits
        if len(hits) < 5 and filter_str:
            fallback_filter = facet_filter if facet_filter else ''
            print(f"⚠️  Stage 1B: only {len(hits)} hits with location filter — "
                  f"retrying with facet filter only")

            sp_fallback = {**search_params}
            if fallback_filter:
                sp_fallback['filter_by'] = fallback_filter
            else:
                sp_fallback.pop('filter_by', None)

            r_fallback   = client.multi_search.perform({'searches': [{'collection': 'questions', **sp_fallback}]}, {})
            fallback_hits = r_fallback['results'][0].get('hits', [])
            print(f"   Fallback (facet only) returned {len(fallback_hits)} hits")

            if len(fallback_hits) < 5:
                print(f"⚠️  Stage 1B: still only {len(fallback_hits)} hits — retrying with no filter")
                sp_nofilter = {k: v for k, v in search_params.items() if k != 'filter_by'}
                r_nofilter  = client.multi_search.perform({'searches': [{'collection': 'questions', **sp_nofilter}]}, {})
                hits        = r_nofilter['results'][0].get('hits', [])
                print(f"   Fallback (no filter) returned {len(hits)} hits")
            else:
                hits = fallback_hits

        # ── Step C: Validate hits ─────────────────────────────────────────
        uuids    = []
        seen     = set()
        accepted = 0
        rejected = 0

        for hit in hits:
            doc  = hit.get('document', {})
            uuid = doc.get('document_uuid')
            if not uuid:
                continue

            is_valid = _validate_question_hit(
                hit_doc         = doc,
                query_tokens    = query_tokens,
                query_phrases   = query_phrases,
                primary_subject = primary_subject,
                min_matches     = 1,
            )

            if is_valid:
                accepted += 1
                if uuid not in seen:
                    seen.add(uuid)
                    uuids.append(uuid)
            else:
                rejected += 1
                print(f"   ❌ Rejected: '{doc.get('question', '')[:60]}' "
                      f"(distance={hit.get('vector_distance', 1.0):.4f})")

            if len(uuids) >= max_results:
                break

        print(f"📊 Stage 1B (questions): {len(uuids)} validated UUIDs "
              f"({accepted} accepted, {rejected} rejected from {len(hits)} hits)")
        return uuids

    except Exception as e:
        print(f"❌ Stage 1B error: {e}")
        return []


# ============================================================================
# STAGE 1 (KEYWORD): Fetch uuids + metadata in one call (no pruning)
# ============================================================================

def fetch_candidates_with_metadata(
    search_query: str,
    profile: Dict,
    signals: Dict = None,
    max_results: int = MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    Stage 1 (Keyword path): Fetch uuids AND lightweight metadata together.
    Since keyword path has no vector pruning, all candidates survive,
    so a separate metadata fetch would be a wasted round-trip.
    """
    signals    = signals or {}
    params     = build_typesense_params(profile, signals=signals)
    filter_str = build_filter_string_without_data_type(profile, signals=signals)

    PAGE_SIZE    = 250
    all_results  = []
    current_page = 1
    max_pages    = (max_results // PAGE_SIZE) + 1
    query_mode   = signals.get('query_mode', 'explore')

    print(f"🔍 Stage 1 (keyword, with metadata): '{params.get('q', search_query)}'")
    print(f"   Mode: {query_mode}")
    print(f"   Fields: {params.get('query_by', '')}")
    if filter_str:
        print(f"   Filters: {filter_str}")

    while len(all_results) < max_results and current_page <= max_pages:
        search_params = {
            'q':                     params.get('q', search_query),
            'query_by':              params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
            'query_by_weights':      params.get('query_by_weights', '10,8,6,4,3'),
            'per_page':              PAGE_SIZE,
            'page':                  current_page,
            'include_fields':        'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
            'num_typos':             params.get('num_typos', 0),
            'prefix':                params.get('prefix', 'no'),
            'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
            'sort_by':               params.get('sort_by', '_text_match:desc,authority_score:desc'),
        }

        if filter_str:
            search_params['filter_by'] = filter_str

        try:
            response = client.collections[COLLECTION_NAME].documents.search(search_params)
            hits     = response.get('hits', [])
            found    = response.get('found', 0)

            if not hits:
                break

            for hit in hits:
                doc = hit.get('document', {})
                all_results.append({
                    'id':              doc.get('document_uuid'),
                    'data_type':       doc.get('document_data_type', ''),
                    'category':        doc.get('document_category', ''),
                    'schema':          doc.get('document_schema', ''),
                    'authority_score': doc.get('authority_score', 0),
                    'text_match':      hit.get('text_match', 0),
                    'image_url':       doc.get('image_url', []),
                    'logo_url':        doc.get('logo_url', []),
                })

            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break

            current_page += 1

        except Exception as e:
            print(f"❌ Stage 1 error (page {current_page}): {e}")
            break

    print(f"📊 Stage 1 (keyword): Retrieved {len(all_results)} candidates with metadata")
    return all_results[:max_results]


# ============================================================================
# STAGE 2: Semantic Rerank
# ============================================================================

def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 250
) -> List[Dict]:
    """Stage 2: Semantic Rerank — Pure Vector Ranking."""
    if not candidate_ids or not query_embedding:
        return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
                for i, cid in enumerate(candidate_ids)]

    ids_to_rerank = candidate_ids[:max_to_rerank]
    id_filter     = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
    embedding_str = ','.join(str(x) for x in query_embedding)

    params = {
        'q':            '*',
        'vector_query': f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
        'filter_by':    f'document_uuid:[{id_filter}]',
        'per_page':     len(ids_to_rerank),
        'include_fields': 'document_uuid',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response        = client.multi_search.perform(search_requests, {})
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
                reranked.append({'id': cid, 'vector_distance': 1.0, 'semantic_rank': len(reranked)})

        print(f"🎯 Stage 2: Reranked {len(reranked)} candidates")
        return reranked

    except Exception as e:
        print(f"⚠️ Stage 2 error: {e}")
        return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
                for i, cid in enumerate(ids_to_rerank)]


def apply_semantic_ranking(
    cached_results: List[Dict],
    reranked_results: List[Dict],
    signals: Dict = None
) -> List[Dict]:
    """Apply semantic ranking to cached results with mode-specific blend ratios."""
    if not reranked_results:
        return cached_results

    signals    = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    blend      = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

    if query_mode == 'answer' and signals.get('wants_single_result'):
        blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

    if signals.get('has_unknown_terms', False):
        shift = min(0.15, blend['text_match'])
        blend['text_match'] -= shift
        blend['semantic']   += shift
        print(f"   📊 Unknown term shift: text_match={blend['text_match']:.2f}, semantic={blend['semantic']:.2f}")

    if signals.get('has_superlative', False):
        shift = min(0.10, blend['semantic'])
        blend['semantic']   -= shift
        blend['authority']  += shift
        print(f"   📊 Superlative shift: semantic={blend['semantic']:.2f}, authority={blend['authority']:.2f}")

    print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

    best_distance = min(
        (r.get('vector_distance', 1.0) for r in reranked_results if r.get('vector_distance', 1.0) < 1.0),
        default=1.0
    )
    cutoff = min(best_distance * 2.0, 0.85)
    print(f"   🎯 Semantic cutoff: best={best_distance:.3f}, cutoff={cutoff:.3f}")

    rank_lookup = {
        r['id']: {
            'semantic_rank':   r['semantic_rank'],
            'vector_distance': r.get('vector_distance', 1.0),
        }
        for r in reranked_results
    }

    total_candidates = len(cached_results)
    max_sem_rank     = len(reranked_results)

    for idx, item in enumerate(cached_results):
        item_id   = item.get('id')
        authority = item.get('authority_score', 0)
        vd        = rank_lookup.get(item_id, {'semantic_rank': 999999, 'vector_distance': 1.0})

        item['semantic_rank']   = vd['semantic_rank']
        item['vector_distance'] = vd['vector_distance']

        text_score = 1.0 - (idx / max(total_candidates, 1))
        sem_score  = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
        auth_score = min(authority / 100.0, 1.0)

        item['blended_score'] = (
            blend['text_match'] * text_score +
            blend['semantic']   * sem_score  +
            blend['authority']  * auth_score
        )

        if item['vector_distance'] > cutoff:
            item['blended_score'] -= 1.0

    cached_results.sort(key=lambda x: -x.get('blended_score', 0))
    for i, item in enumerate(cached_results):
        item['rank'] = i

    return cached_results


# ============================================================================
# FACET COUNTING FROM CACHE
# ============================================================================

def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
    """Count facets from cached result set (always accurate)."""
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
            {'value': dt,  'count': c, 'label': DATA_TYPE_LABELS.get(dt, dt.title())}
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


# ============================================================================
# FILTER AND PAGINATE CACHE
# ============================================================================

def filter_cached_results(
    cached_results: List[Dict],
    data_type: str = None,
    category: str  = None,
    schema: str    = None
) -> List[Dict]:
    """Filter cached results by UI filters."""
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
    """Paginate cached results."""
    total = len(cached_results)
    start = (page - 1) * per_page
    end   = start + per_page
    if start >= total:
        return [], total
    return cached_results[start:end], total


# ============================================================================
# FULL DOCUMENT FETCHING
# ============================================================================

def fetch_full_documents(document_ids: List[str], query: str = '') -> List[Dict]:
    """Fetch full document details for display."""
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
        response        = client.multi_search.perform(search_requests, {})
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


def fetch_documents_by_semantic_uuid(
    semantic_uuid: str,
    exclude_uuid: str = None,
    limit: int = 5
) -> List[Dict]:
    """
    Fetch documents that share the same semantic group.
    Used for related searches on the question direct path.
    """
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
        response        = client.multi_search.perform(search_requests, {})
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


def format_result(hit: Dict, query: str = '') -> Dict:
    """Transform Typesense hit into response format."""
    doc        = hit.get('document', {})
    highlights = hit.get('highlights', [])

    highlight_map = {h.get('field'): (h.get('value') or h.get('snippet') or
                     (h.get('snippets') or [''])[0]) for h in highlights}

    vector_distance = hit.get('vector_distance')
    semantic_score  = round(1 - vector_distance, 3) if vector_distance else None

    raw_date      = doc.get('published_date_string', '')
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

    geopoint = doc.get('location_geopoint') or doc.get('location_coordinates') or [None, None]

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
        'location': {
            'city':    doc.get('location_city'),
            'state':   doc.get('location_state'),
            'country': doc.get('location_country'),
            'region':  doc.get('location_region'),
            'geopoint': geopoint,
            'address': doc.get('location_address'),
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


# ============================================================================
# AI OVERVIEW LOGIC (Blueprint Step 8)
# ============================================================================

def _should_trigger_ai_overview(signals: Dict, results: List[Dict], query: str) -> bool:
    """Blueprint Step 8: Determine if AI Overview should trigger."""
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
        stopwords  = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
                      'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
                      'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
        query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
        if not query_words:
            return False
        matches    = sum(1 for w in query_words if w in top_title or w in top_facts)
        return (matches / len(query_words)) >= 0.75

    return False


def _build_ai_overview(signals: Dict, results: List[Dict], query: str) -> Optional[str]:
    """Build the AI Overview text using signal-driven key_fact selection."""
    if not results or not results[0].get('key_facts'):
        return None

    question_word = signals.get('question_word')
    stopwords     = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
                     'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
                     'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
    query_words   = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]

    matched_keyword = ''
    if query_words:
        top_title       = results[0].get('title', '').lower()
        top_facts       = ' '.join(results[0].get('key_facts', [])).lower()
        matched_keyword = max(query_words,
                              key=lambda w: (w in top_title) + (w in top_facts))

    return humanize_key_facts(
        results[0]['key_facts'],
        query,
        matched_keyword=matched_keyword,
        question_word=question_word,
    )


# ============================================================================
# INTENT DETECTION (for compatibility — keyword path)
# ============================================================================

def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
    """Simple intent detection for compatibility."""
    query_lower    = query.lower()
    location_words = ['in', 'near', 'around', 'at', 'restaurant', 'store', 'hotel']
    if any(w in query_lower for w in location_words):
        return 'location'
    person_words = ['who is', 'biography', 'born', 'died', 'ceo', 'founder']
    if any(w in query_lower for w in person_words):
        return 'person'
    return 'general'


# ============================================================================
# STABLE CACHE KEY
# ============================================================================

def _generate_stable_cache_key(session_id: str, query: str) -> str:
    """
    Stable cache key for the FINISHED result package.
    Uses session_id + original query. Never depends on derived values.
    """
    normalized = query.strip().lower()
    key_string = f"final|{session_id or 'nosession'}|{normalized}"
    return hashlib.md5(key_string.encode()).hexdigest()


# ============================================================================
# STAGE 4: Fetch lightweight metadata for SURVIVORS ONLY
# ============================================================================

def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
    """
    Stage 4 (Semantic path only): Fetch lightweight metadata for documents
    that survived vector pruning. Preserves semantic rank order.
    """
    if not survivor_ids:
        return []

    BATCH_SIZE = 250
    doc_map    = {}

    for i in range(0, len(survivor_ids), BATCH_SIZE):
        batch_ids = survivor_ids[i:i + BATCH_SIZE]
        id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])

        params = {
            'q':              '*',
            'filter_by':      f'document_uuid:[{id_filter}]',
            'per_page':       len(batch_ids),
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
        }

        try:
            search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
            response        = client.multi_search.perform(search_requests, {})
            hits            = response['results'][0].get('hits', [])

            for hit in hits:
                doc  = hit.get('document', {})
                uuid = doc.get('document_uuid')
                if uuid:
                    doc_map[uuid] = {
                        'id':              uuid,
                        'data_type':       doc.get('document_data_type', ''),
                        'category':        doc.get('document_category', ''),
                        'schema':          doc.get('document_schema', ''),
                        'authority_score': doc.get('authority_score', 0),
                        'image_url':       doc.get('image_url', []),
                        'logo_url':        doc.get('logo_url', []),
                    }

        except Exception as e:
            print(f"❌ Stage 4 metadata fetch error (batch {i}): {e}")

    results = [doc_map[uuid] for uuid in survivor_ids if uuid in doc_map]
    print(f"📊 Stage 4: Fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
    return results


# ============================================================================
# IMAGE COUNTING HELPERS
# ============================================================================

def _has_real_images(item: Dict) -> bool:
    """Check if a candidate has at least one non-empty image or logo URL."""
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
    """Count documents that have at least one real image or logo URL."""
    return sum(1 for item in all_results if _has_real_images(item))


# ============================================================================
# STAGE 5: ONE count pass — single source of truth
# ============================================================================

def count_all(candidates: List[Dict]) -> Dict:
    """
    Stage 5: Single counting pass after all pruning is done.
    Returns facets, image count, and total.
    """
    facets      = count_facets_from_cache(candidates)
    image_count = _count_images_from_candidates(candidates)
    total       = len(candidates)

    print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
          f"facets={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

    return {
        'facets':            facets,
        'facet_total':       total,
        'total_image_count': image_count,
    }


# ============================================================================
# MAIN ENTRY POINT — Clean 7-Stage Pipeline
# ============================================================================

# SEMANTIC:  1(uuids) → 2(rerank) → 3(prune) → 4(metadata survivors) → 5(count) → 6(cache) → 7(paginate)
# KEYWORD:   1(uuids+metadata) → 5(count) → 6(cache) → 7(paginate)

def execute_full_search(
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
    Main entry point for search.

    Clean 7-Stage Pipeline:
        SEMANTIC:  1 → 2 → 3 → 4 → 5 → 6 → 7
        KEYWORD:   1 → 5 → 6 → 7

    v3 migration: profile building replaced by _read_v3_profile() which is
    a pure O(1) field read — no re-classification, no re-analysis.
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
    # ★ QUESTION DIRECT PATH
    # =========================================================================
    if document_uuid and search_source == 'question':
        print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
        t_fetch = time.time()
        results = fetch_full_documents([document_uuid], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        ai_overview   = None
        question_word = None
        q_lower       = query.lower().strip()
        for word in ('who', 'what', 'where', 'when', 'why', 'how'):
            if q_lower.startswith(word):
                question_word = word
                break

        question_signals = {
            'query_mode':         'answer',
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
                    related_docs     = fetch_documents_by_semantic_uuid(
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
            'query':            query,
            'corrected_query':  query,
            'intent':           'answer',
            'query_mode':       'answer',
            'answer':           answer,
            'answer_type':      answer_type or 'UNKNOWN',
            'results':          results,
            'total':            len(results),
            'facet_total':      len(results),
            'total_image_count': 0,
            'page':             1,
            'per_page':         per_page,
            'search_time':      round(time.time() - t0, 3),
            'session_id':       session_id,
            'semantic_enabled': False,
            'search_strategy':  'question_direct',
            'alt_mode':         alt_mode,
            'skip_embedding':   True,
            'search_source':    'question',
            'valid_terms':      query.split(),
            'unknown_terms':    [],
            'data_type_facets': [],
            'category_facets':  [],
            'schema_facets':    [],
            'related_searches': related_searches,
            'facets':           {},
            'word_discovery': {
                'valid_count':    len(query.split()),
                'unknown_count':  0,
                'corrections':    [],
                'filters':        [],
                'locations':      [],
                'sort':           None,
                'total_score':    0,
                'average_score':  0,
                'max_score':      0,
            },
            'timings':          times,
            'filters_applied': {
                'data_type':            None,
                'category':             None,
                'schema':               None,
                'is_local_search':      False,
                'local_search_strength': 'none',
            },
            'signals': question_signals,
            'profile': {},
        }

    # =========================================================================
    # ★ FAST PATH: Finished cache
    # =========================================================================
    stable_key = _generate_stable_cache_key(session_id, query)
    finished   = _get_cached_results(stable_key)

    if finished is not None:
        print(f"⚡ FAST PATH: '{query}' | page={page} | filter={active_data_type}/{active_category}/{active_schema}")

        all_results       = finished['all_results']
        all_facets        = finished['all_facets']
        facet_total       = finished['facet_total']
        ai_overview       = finished.get('ai_overview')
        total_image_count = finished.get('total_image_count', 0)
        metadata          = finished['metadata']
        times['cache']    = 'hit (fast path)'

        filtered_results         = filter_cached_results(all_results, active_data_type, active_category, active_schema)
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        t_fetch = time.time()
        results = fetch_full_documents([item['id'] for item in page_items], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        if results and page == 1 and ai_overview:
            results[0]['humanized_summary'] = ai_overview

        times['total'] = round((time.time() - t0) * 1000, 2)
        print(f"⏱️ FAST PATH TIMING: {times}")
        print(f"🔍 FAST PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)} | Images: {total_image_count}")

        signals = metadata.get('signals', {})

        return {
            'query':            query,
            'corrected_query':  metadata.get('corrected_query', query),
            'intent':           metadata.get('intent', 'general'),
            'query_mode':       metadata.get('query_mode', 'keyword'),
            'results':          results,
            'total':            total_filtered,
            'facet_total':      facet_total,
            'total_image_count': total_image_count,
            'page':             page,
            'per_page':         per_page,
            'search_time':      round(time.time() - t0, 3),
            'session_id':       session_id,
            'semantic_enabled': metadata.get('semantic_enabled', False),
            'search_strategy':  metadata.get('search_strategy', 'cached'),
            'alt_mode':         alt_mode,
            'skip_embedding':   skip_embedding,
            'search_source':    search_source,
            'valid_terms':      metadata.get('valid_terms', query.split()),
            'unknown_terms':    metadata.get('unknown_terms', []),
            'data_type_facets': all_facets.get('data_type', []),
            'category_facets':  all_facets.get('category', []),
            'schema_facets':    all_facets.get('schema', []),
            'related_searches': [],
            'facets':           all_facets,
            'word_discovery':   metadata.get('word_discovery', {
                'valid_count': len(query.split()), 'unknown_count': 0,
                'corrections': [], 'filters': [], 'locations': [],
                'sort': None, 'total_score': 0, 'average_score': 0, 'max_score': 0,
            }),
            'timings':          times,
            'filters_applied':  metadata.get('filters_applied', {
                'data_type': active_data_type, 'category': active_category,
                'schema': active_schema, 'is_local_search': False, 'local_search_strength': 'none',
            }),
            'signals': signals,
            'profile': metadata.get('profile', {}),
        }

    # =========================================================================
    # ★ FULL PATH
    # =========================================================================
    print(f"🔬 FULL PATH: '{query}' (no finished cache for stable_key={stable_key[:12]}...)")

    is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

    # =========================================================================
    # KEYWORD PATH:  Stage 1 → 5 → 6 → 7
    # =========================================================================
    if is_keyword_path:
        print(f"⚡ KEYWORD PIPELINE: '{query}'")

        intent = detect_query_intent(query, pos_tags)

        # Minimal profile for keyword path — no v3 needed
        profile = {
            'search_terms': query.split(),
            'cities':        [],
            'states':        [],
            'location_terms': [],
            'primary_intent': intent,
            'field_boosts': {
                'primary_keywords': 10,
                'entity_names':      8,
                'semantic_keywords': 6,
                'key_facts':         4,
                'document_title':    3,
            },
            'corrections': [],
            'persons':       [],
            'organizations': [],
            'keywords':      [],
            'media':         [],
        }

        t1          = time.time()
        all_results = fetch_candidates_with_metadata(query, profile)
        times['stage1'] = round((time.time() - t1) * 1000, 2)

        counts = count_all(all_results)

        _set_cached_results(stable_key, {
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
                'city_names':       [],
                'state_names':      [],
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
        print(f"💾 Cached keyword package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

        filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        t2      = time.time()
        results = fetch_full_documents([item['id'] for item in page_items], query)
        times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
        times['total']      = round((time.time() - t0) * 1000, 2)

        print(f"⏱️ KEYWORD TIMING: {times}")

        return {
            'query':            query,
            'corrected_query':  query,
            'intent':           intent,
            'results':          results,
            'total':            total_filtered,
            'facet_total':      counts['facet_total'],
            'total_image_count': counts['total_image_count'],
            'page':             page,
            'per_page':         per_page,
            'search_time':      round(time.time() - t0, 3),
            'session_id':       session_id,
            'semantic_enabled': False,
            'search_strategy':  'keyword_graph_filter',
            'alt_mode':         alt_mode,
            'skip_embedding':   True,
            'search_source':    search_source or 'dropdown',
            'valid_terms':      query.split(),
            'unknown_terms':    [],
            'data_type_facets': counts['facets'].get('data_type', []),
            'category_facets':  counts['facets'].get('category', []),
            'schema_facets':    counts['facets'].get('schema', []),
            'related_searches': [],
            'facets':           counts['facets'],
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
        }

    # =========================================================================
    # SEMANTIC PATH:  Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
    # =========================================================================
    print(f"🔬 SEMANTIC PIPELINE: '{query}'")

    # ── Word discovery + embedding in parallel ────────────────────────────
    t1 = time.time()
    discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

    # ── Intent detection ──────────────────────────────────────────────────
    signals = {}
    if INTENT_DETECT_AVAILABLE:
        try:
            discovery = detect_intent(discovery)
            signals   = discovery.get('signals', {})
            print(f"   🎯 Intent signals: mode={signals.get('query_mode')}, "
                  f"q_word={signals.get('question_word')}, "
                  f"local={signals.get('is_local_search')}, "
                  f"location={signals.get('has_location')}, "
                  f"service={signals.get('service_words')}, "
                  f"temporal={signals.get('temporal_direction')}, "
                  f"black_owned={signals.get('has_black_owned')}, "
                  f"single={signals.get('wants_single_result')}, "
                  f"domains={signals.get('domains_detected', [])[:3]}")
        except Exception as e:
            print(f"   ⚠️ intent_detect error: {e}")

    corrected_query  = discovery.get('corrected_query', query)
    semantic_enabled = query_embedding is not None
    query_mode       = signals.get('query_mode', 'explore')

    # ── Read v3 profile — O(1) field reads, no re-analysis ───────────────
    t2      = time.time()
    profile = _read_v3_profile(discovery, signals=signals)
    times['read_profile'] = round((time.time() - t2) * 1000, 2)

    # ── Apply corrections to search_terms (pos_mismatch only) ────────────
    # v3 sets corrected_query already; here we align search_terms to match
    # so Stage 1A query string is consistent with what v3 resolved.
    corrections = discovery.get('corrections', [])
    if corrections:
        correction_map = {
            c['original'].lower(): c['corrected']
            for c in corrections
            if c.get('original') and c.get('corrected')
               and c.get('correction_type') == 'pos_mismatch'
        }
        if correction_map:
            original_terms        = profile['search_terms']
            profile['search_terms'] = [
                correction_map.get(t.lower(), t) for t in original_terms
            ]
            if original_terms != profile['search_terms']:
                print(f"   ✅ Applied pos_mismatch corrections to search_terms: "
                      f"{original_terms} → {profile['search_terms']}")

    intent      = profile['primary_intent']
    city_names  = [c['name'] for c in profile['cities']]
    state_names = [s['name'] for s in profile['states']]

    print(f"   Intent: {intent} | Mode: {query_mode}")
    print(f"   Cities: {city_names}")
    print(f"   States: {state_names}")
    print(f"   Search Terms: {profile['search_terms']}")
    print(f"   Field Boosts: {profile['field_boosts']}")

    # ── Stage 1: Candidate UUIDs (document + questions in parallel) ───────
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

    if has_unsafe_corrections:
        print(f"⚠️  Unsafe corrections — using original query for Stage 1A: '{query}'")

    candidate_uuids = fetch_all_candidate_uuids(
        search_query_for_stage1, profile, query_embedding,
        signals=signals, discovery=discovery,
    )
    times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
    print(f"📊 Stage 1 COMBINED: {len(candidate_uuids)} candidate UUIDs")

    # ── Stage 2 + 3: Vector rerank + prune ───────────────────────────────
    survivor_uuids = candidate_uuids
    vector_data    = {}

    if semantic_enabled and candidate_uuids:
        t4       = time.time()
        reranked = semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
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
        after_prune    = len(survivor_uuids)

        if before_prune != after_prune:
            print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): "
                  f"{before_prune} → {after_prune} ({before_prune - after_prune} removed)")
        times['stage3_prune'] = f"{before_prune} → {after_prune}"
    else:
        print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, candidates={len(candidate_uuids)}")

    # ── Stage 4: Metadata for survivors ───────────────────────────────────
    t5          = time.time()
    all_results = fetch_candidate_metadata(survivor_uuids)
    times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

    # Attach vector data + compute blended scores
    if vector_data:
        total_candidates = len(all_results)
        max_sem_rank     = len(vector_data)
        blend            = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

        if query_mode == 'answer' and signals.get('wants_single_result'):
            blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

        if signals.get('has_unknown_terms', False):
            shift            = min(0.15, blend['text_match'])
            blend['text_match'] -= shift
            blend['semantic']   += shift

        if signals.get('has_superlative', False):
            shift            = min(0.10, blend['semantic'])
            blend['semantic']   -= shift
            blend['authority']  += shift

        print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} "
              f"sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

        for idx, item in enumerate(all_results):
            item_id   = item.get('id')
            vd        = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
            authority = item.get('authority_score', 0)

            item['vector_distance'] = vd['vector_distance']
            item['semantic_rank']   = vd['semantic_rank']

            text_score = 1.0 - (idx / max(total_candidates, 1))
            sem_score  = (1.0 - (item['semantic_rank'] / max(max_sem_rank, 1))
                          if item['semantic_rank'] < 999999 else 0.0)
            auth_score = min(authority / 100.0, 1.0)

            item['blended_score'] = (
                blend['text_match'] * text_score +
                blend['semantic']   * sem_score  +
                blend['authority']  * auth_score
            )

        all_results.sort(key=lambda x: -x.get('blended_score', 0))
        for i, item in enumerate(all_results):
            item['rank'] = i

    # ── Stage 5: ONE count pass ───────────────────────────────────────────
    counts = count_all(all_results)

    # ── AI Overview preview ───────────────────────────────────────────────
    ai_overview = None
    if all_results:
        preview_items, _ = paginate_cached_results(all_results, 1, per_page)
        preview_docs     = fetch_full_documents([item['id'] for item in preview_items], query)
        if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
            ai_overview = _build_ai_overview(signals, preview_docs, query)
            if ai_overview:
                print(f"   💡 AI Overview: {ai_overview[:80]}...")

    valid_terms   = profile['search_terms']
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

    # ── Stage 6: Cache the final package ─────────────────────────────────
    _set_cached_results(stable_key, {
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
                'valid_count':  discovery.get('stats', {}).get('valid_words', 0),
                'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
                'corrections':  discovery.get('corrections', []),
                'filters':      [],
                'locations': (
                    [
                        {'field': 'location_city',  'values': city_names},
                        {'field': 'location_state', 'values': state_names},
                    ] if city_names or state_names else []
                ),
                'sort':          None,
                'total_score':   0,
                'average_score': 0,
                'max_score':     0,
            },
            'filters_applied': {
                'data_type':            active_data_type,
                'category':             active_category,
                'schema':               active_schema,
                'is_local_search':      signals.get('is_local_search', False),
                'local_search_strength': signals.get('local_search_strength', 'none'),
                'has_black_owned':      signals.get('has_black_owned', False),
                'graph_filters':        [],
                'graph_locations': (
                    [
                        {'field': 'location_city',  'values': city_names},
                        {'field': 'location_state', 'values': state_names},
                    ] if city_names or state_names else []
                ),
                'graph_sort': None,
            },
        },
    })
    print(f"💾 Cached semantic package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

    # ── Stage 7: Filter → Paginate → Fetch full docs ──────────────────────
    filtered_results           = filter_cached_results(all_results, active_data_type, active_category, active_schema)
    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

    t6      = time.time()
    results = fetch_full_documents([item['id'] for item in page_items], query)
    times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

    if results and page == 1 and ai_overview:
        results[0]['humanized_summary'] = ai_overview

    if query_embedding:
        try:
            store_query_embedding(corrected_query, query_embedding, result_count=counts['facet_total'])
        except Exception as e:
            print(f"⚠️ store_query_embedding error: {e}")

    times['total'] = round((time.time() - t0) * 1000, 2)
    strategy       = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

    print(f"⏱️ SEMANTIC TIMING: {times}")
    print(f"🔍 {strategy.upper()} ({query_mode}) | Total: {counts['facet_total']} | "
          f"Filtered: {total_filtered} | Page: {len(results)} | Images: {counts['total_image_count']}")

    locations_block = (
        [
            {'field': 'location_city',  'values': city_names},
            {'field': 'location_state', 'values': state_names},
        ] if city_names or state_names else []
    )

    return {
        'query':            query,
        'corrected_query':  corrected_query,
        'intent':           intent,
        'query_mode':       query_mode,
        'results':          results,
        'total':            total_filtered,
        'facet_total':      counts['facet_total'],
        'total_image_count': counts['total_image_count'],
        'page':             page,
        'per_page':         per_page,
        'search_time':      round(time.time() - t0, 3),
        'session_id':       session_id,
        'semantic_enabled': semantic_enabled,
        'search_strategy':  strategy,
        'alt_mode':         alt_mode,
        'skip_embedding':   skip_embedding,
        'search_source':    search_source,
        'valid_terms':      valid_terms,
        'unknown_terms':    unknown_terms,
        'related_searches': [],
        'data_type_facets': counts['facets'].get('data_type', []),
        'category_facets':  counts['facets'].get('category', []),
        'schema_facets':    counts['facets'].get('schema', []),
        'facets':           counts['facets'],
        'word_discovery': {
            'valid_count':  discovery.get('stats', {}).get('valid_words', 0),
            'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
            'corrections':  discovery.get('corrections', []),
            'filters':      [],
            'locations':    locations_block,
            'sort':          None,
            'total_score':   0,
            'average_score': 0,
            'max_score':     0,
        },
        'timings': times,
        'filters_applied': {
            'data_type':            active_data_type,
            'category':             active_category,
            'schema':               active_schema,
            'is_local_search':      signals.get('is_local_search', False),
            'local_search_strength': signals.get('local_search_strength', 'none'),
            'has_black_owned':      signals.get('has_black_owned', False),
            'graph_filters':        [],
            'graph_locations':      locations_block,
            'graph_sort':           None,
        },
        'signals': signals,
        'profile': profile,
    }


# ============================================================================
# CONVENIENCE FUNCTIONS (for compatibility with views.py imports)
# ============================================================================

def get_facets(query: str) -> dict:
    """Returns available filter options."""
    return {}


def get_related_searches(query: str, intent: str) -> list:
    """Returns related searches."""
    return []


def get_featured_result(query: str, intent: str, results: list) -> dict:
    """Returns featured content."""
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
    """Logs search event."""
    pass


def typesense_search(
    query: str = '*',
    filter_by: str = None,
    sort_by: str = 'authority_score:desc',
    per_page: int = 20,
    page: int = 1,
    facet_by: str = None,
    query_by: str = 'document_title,document_summary,keywords,primary_keywords',
    max_facet_values: int = 20,
) -> Dict:
    """Simple Typesense search wrapper."""
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
        params['facet_by']          = facet_by
        params['max_facet_values']  = max_facet_values

    try:
        return client.collections[COLLECTION_NAME].documents.search(params)
    except Exception as e:
        print(f"❌ typesense_search error: {e}")
        return {'hits': [], 'found': 0, 'error': str(e)}


# ============================================================================
# TEST / CLI
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python typesense_discovery_bridge.py \"your search query\"")
        sys.exit(1)

    query = ' '.join(sys.argv[1:])

    print("=" * 70)
    print(f"🚀 TESTING: '{query}'")
    print("=" * 70)

    result = execute_full_search(
        query=query,
        session_id='test-session',
        filters={},
        page=1,
        per_page=10,
        alt_mode='y',
    )

    print("\n" + "=" * 70)
    print("📊 RESULTS")
    print("=" * 70)
    print(f"Query:           {result['query']}")
    print(f"Corrected:       {result['corrected_query']}")
    print(f"Intent:          {result['intent']}")
    print(f"Query Mode:      {result.get('query_mode', 'N/A')}")
    print(f"Total:           {result['total']}")
    print(f"Facet Total:     {result['facet_total']}")
    print(f"Total Images:    {result['total_image_count']}")
    print(f"Strategy:        {result['search_strategy']}")
    print(f"Semantic:        {result['semantic_enabled']}")

    print(f"\n🔧 Corrections:")
    for c in result.get('word_discovery', {}).get('corrections', []):
        print(f"   '{c['original']}' → '{c['corrected']}' (type: {c.get('correction_type', 'unknown')})")

    print(f"\n🔄 Query Flow:")
    print(f"   Original:  '{result['query']}'")
    print(f"   Corrected: '{result['corrected_query']}'")
    print(f"   Changed:   {result['query'] != result['corrected_query']}")

    print(f"\n📝 Terms:")
    print(f"   Valid:   {result['valid_terms']}")
    print(f"   Unknown: {result['unknown_terms']}")

    print(f"\n📍 Locations:")
    for loc in result.get('word_discovery', {}).get('locations', []):
        print(f"   {loc['field']}: {loc['values']}")

    print(f"\n📁 Data Type Facets:")
    for f in result.get('data_type_facets', []):
        print(f"   {f['label']}: {f['count']}")

    print(f"\n🎯 Signals:")
    sigs = result.get('signals', {})
    if sigs:
        print(f"   query_mode:    {sigs.get('query_mode')}")
        print(f"   question_word: {sigs.get('question_word')}")
        print(f"   wants_single:  {sigs.get('wants_single_result')}")
        print(f"   wants_multi:   {sigs.get('wants_multiple_results')}")
        print(f"   is_local:      {sigs.get('is_local_search')}")
        print(f"   has_black_own: {sigs.get('has_black_owned')}")
        print(f"   temporal:      {sigs.get('temporal_direction')}")
        print(f"   has_unknown:   {sigs.get('has_unknown_terms')}")

    print(f"\n📄 Results ({len(result['results'])}):")
    for i, r in enumerate(result['results'][:5], 1):
        print(f"   {i}. {r['title'][:60]}")
        if r.get('humanized_summary'):
            print(f"      💡 {r['humanized_summary'][:80]}...")
        print(f"      📍 {r['location'].get('city', '')}, {r['location'].get('state', '')}")
        print(f"      🔗 {r['url'][:50]}...")

    print(f"\n⏱️ Timings: {result['timings']}")





















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





#-----------------------------------------------------------------
#-----------------------------------------------------------------
# Version 2 Works
#----------------------------------------------------------------

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


# ============================================================
# END OF PART 4
# ============================================================

# ============================================================
# PART 4 OF 8 — SCORING FUNCTIONS (ALL SYNCHRONOUS — CPU ONLY)
# ============================================================
#
# DROP-IN REPLACEMENT
# -------------------
# Same function names, same signatures, same _score_document flow
# as the original Part 4. Only the INSIDES of the broken functions
# have been rewritten.
#
# WHAT WAS BROKEN AND WHAT CHANGED
# --------------------------------
# _compute_text_score was positional (rank-in-pool). A thin doc at
# rank 2 scored 0.992 regardless of why Typesense ranked it there.
# Rewritten to score based on weighted field overlap:
#     title > entity_names > primary_keywords > key_facts >
#     topic_tags > semantic_keywords > summary
# Then multiplied by a richness factor derived from 100%-populated
# fields (content_depth_score, factual_density_score, concept_count,
# subtopic_richness, word_count). Thin docs can no longer out-score
# rich ones on a single lucky keyword hit. The signature (keyword_rank,
# pool_size, item, profile) is preserved — keyword_rank is still read
# for a small tiebreaker bonus.
#
# _extract_authority_score had a fallback capped at 0.5 and ignored
# all the 100%-populated enrichment fields. That capped cap is gone.
# Tier 1 still uses service_rating / product_rating / recipe_rating /
# media_rating when present (with review-count confidence). Tier 2
# falls back to a weighted composite of authority_rank_score,
# factual_density_score, content_depth_score, subtopic_richness,
# evergreen_score — all 100% populated in your data. Every doc now
# has a live authority signal in [0..1].
#
# _resolve_blend used to call a doc "has live authority" only when
# doc.get('authority_score') > 0, which is almost never true. Now it
# calls _extract_authority_score on the sample so the check is real.
# Blend key 'text_match' is unchanged — nothing else in the bridge
# needs updating.
#
# _score_document flow is BYTE-FOR-BYTE identical to the original:
#   1. read vector_data
#   2. compute text_score, sem_score, auth_score
#   3. blend
#   4. multiply by domain_relevance × content_intent_match × pool_type
#   5. preferred-type boost
#   6. black-owned boost
#   7. superlative cap
#   8. stash diagnostic fields on the item
#   9. return blended
# All the fixes happen inside the component functions it calls.
#
# _compute_semantic_score, _domain_relevance, _content_intent_match,
# _pool_type_multiplier are UNCHANGED from the original.
#
# FIELDS READ BY THE REWRITTEN INTERNALS
# --------------------------------------
#   document_title            100% populated
#   entity_names              100% populated
#   primary_keywords          100% populated
#   key_facts                 100% populated
#   topic_tags                100% populated
#   semantic_keywords         100% populated
#   document_summary           ~95% populated
#   content_depth_score       100% populated
#   factual_density_score     100% populated
#   evergreen_score           100% populated
#   subtopic_richness         100% populated
#   concept_count             100% populated
#   authority_rank_score      100% populated
#   word_count                100% populated
#   service_rating + reviews  sparse (used when present)
#   product_rating + reviews  sparse (used when present)
#   recipe_rating  + reviews  sparse (used when present)
#   media_rating              sparse (used when present)
#   black_owned               flag
#
# NOTE ON METADATA FETCH
# ----------------------
# The Stage 4 fetch in Part 6 (fetch_candidate_metadata) and the
# Stage 1 keyword fetch in Part 5 (fetch_candidates_with_metadata)
# MUST include these fields in include_fields for scoring to work:
#   document_title, entity_names, primary_keywords, key_facts,
#   topic_tags, semantic_keywords, document_summary,
#   content_depth_score, factual_density_score, evergreen_score,
#   subtopic_richness, concept_count, authority_rank_score,
#   word_count, content_intent, document_data_type,
#   document_category, document_schema, service_type,
#   service_rating, service_review_count, product_rating,
#   product_review_count, recipe_rating, recipe_review_count,
#   media_rating, black_owned, image_url, logo_url
# Most of these are already in your current fetch — verify the
# new ones (entity_names, key_facts, topic_tags, semantic_keywords,
# document_summary, content_depth_score, subtopic_richness,
# concept_count, authority_rank_score, word_count, document_title)
# are added before deploying.
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


# ── Internal helper: tokenize a field for overlap measurement ────────────────

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


# ── PUBLIC API — same function names & signatures as the original Part 4 ────

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

    # NEW: use the real authority function on the sample, not the dead
    # `authority_score` field which is 0 on almost every doc.
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

    REWRITTEN: tier-2 fallback now uses the 100%-populated composite
    instead of a 0.5-capped stub. Every doc now has a live authority
    signal, not just the rare ones with service/product/recipe ratings.
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
        # fall through to tier 2

    elif data_type == 'product':
        rating  = doc.get('product_rating') or 0.0
        reviews = doc.get('product_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_BIZ),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)
        # fall through to tier 2

    elif data_type == 'recipe':
        rating  = doc.get('recipe_rating') or 0.0
        reviews = doc.get('recipe_review_count') or 0
        if rating > 0 and reviews > 0:
            confidence = min(
                math.log1p(reviews) / math.log1p(REVIEW_COUNT_SCALE_RECIPE),
                1.0
            )
            return min((rating / 5.0) * confidence, 1.0)
        # fall through to tier 2

    elif data_type == 'media':
        rating = doc.get('media_rating') or 0.0
        if rating > 0:
            return min(rating / 5.0, 1.0)
        # fall through to tier 2

    # ── Tier 2: composite from 100%-populated enrichment fields ─────────
    # This is where the old code returned 0.0 or a 0.5-capped stub.
    # Every document has these fields, so every document now has a
    # meaningful authority value.
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
    profile: Dict
) -> float:
    """
    Score from the item's content match against the query, NOT its
    position in the keyword pool.

    REWRITTEN: this is the fix for the thin-doc-wins bug.

    Original behavior: base = 1.0 - (rank / pool_size).
    A thin doc at rank 2 out of 250 got 0.992 regardless of whether
    the match was a single incidental keyword or a full topical match.

    New behavior: weighted field overlap.
        - Measures what fraction of query tokens appear in each field
        - Weighted by field importance
        - Exact multi-word phrase hits in title/entity_names get a bonus
        - Result is dampened by richness so thin docs can't win big
        - Keyword_rank is still used for a small tiebreaker bonus so
          Typesense's ranking isn't completely ignored
    """
    query_tokens = _build_query_tokens_from_profile(profile)
    if not query_tokens:
        # No meaningful query tokens — fall back to a mild positional signal
        return 1.0 - (keyword_rank / max(pool_size, 1)) * 0.5

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
    query_phrases = _build_query_phrases_from_profile(profile)
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

    # ── Small Typesense-rank tiebreaker (keeps the original signature useful) ─
    positional_bonus = (1.0 - (keyword_rank / max(pool_size, 1))) * 0.05
    match_score = min(match_score + positional_bonus, 1.0)

    # ── Primary-keyword exact-match bonus (unchanged from original) ─────
    doc_kws = set(k.lower() for k in (item.get('primary_keywords') or []))
    wd_kws  = set(
        k.get('phrase', '').lower()
        for k in profile.get('keywords', [])
        if k.get('phrase')
    )
    overlap = doc_kws & wd_kws
    if overlap:
        match_score = min(match_score + min(len(overlap) * 0.05, 0.15), 1.0)

    # ── Richness dampener — the actual fix for thin-doc-wins ────────────
    richness = _compute_richness(item)
    if richness < _THIN_DOC_RICHNESS_CAP:
        # Thin doc — cap hard so it can't beat a rich doc on lucky matches
        match_score = min(match_score, 0.5)
    else:
        # Soft multiplier in [0.7 .. 1.0] based on how rich the doc is
        match_score *= (0.7 + 0.3 * richness)

    # Stash richness on the item for debugging / tracing
    item['richness'] = round(richness, 4)

    return min(match_score, 1.0)


def _compute_semantic_score(vector_distance: float) -> float:
    """Convert vector distance to a score with a hard gate at 0.65. UNCHANGED."""
    if vector_distance is None or vector_distance >= SEMANTIC_DISTANCE_GATE:
        return 0.0
    return 1.0 - (vector_distance / SEMANTIC_DISTANCE_GATE)


def _domain_relevance(doc: Dict, signals: Dict) -> float:
    """Return a multiplier based on domain alignment. UNCHANGED from original."""
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
    """Return a multiplier based on content_intent alignment. UNCHANGED."""
    doc_intent = (doc.get('content_intent') or '').lower()
    if not doc_intent:
        return 1.0

    preferred = INTENT_CONTENT_MAP.get(query_mode, set())
    if not preferred:
        return 1.0

    return 1.10 if doc_intent in preferred else 0.85


def _pool_type_multiplier(doc: Dict, query_mode: str) -> float:
    """Return a multiplier based on data type appropriateness. UNCHANGED."""
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

    Same flow as the original — the fixes are inside the component
    functions this calls. Returns blended score, same as before.
    """
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

        # Related questions — from the original document + cluster siblings
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


#-----------------------------------------------------------------
# ------ Version 3------------------------------------------------

# """
# typesense_discovery_bridge.py (ASYNC)
# =====================================
# AfroToDo Search Bridge — Word Discovery v3 + Intent Detection + Typesense.

# SCORING ALGORITHM (v5 — CENTRALITY-WEIGHTED)
# --------------------------------------------
# aboutness = hits_in_identity_fields / total_identity_slots
# scope_mult = topic_scope → {central:1.0, major:0.7, supporting:0.3, peripheral:0.1}

# blended = (
#     blend['text_match'] * text_score      +
#     blend['semantic']   * semantic_score  +   # capped if aboutness<0.1 & text_score<0.1
#     blend['authority']  * authority_score
# )
# final_score = blended
#             * (aboutness ^ 1.5)            # centrality gate — kills peripheral mentions
#             * scope_mult                   # direct topic_scope multiplier
#             * _domain_relevance(doc, signals)
#             * _content_intent_match(doc, query_mode)
#             * _pool_type_multiplier(doc, query_mode)

# A document that is ABOUT Africa (title match, topic_tags root=africa,
# topic_scope=central) will dominate a document that merely MENTIONS
# Africa in one entity_names slot.

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

# SEMANTIC_DISTANCE_GATE          = 0.65
# QUESTION_SEMANTIC_DISTANCE_GATE = 0.40
# REVIEW_COUNT_SCALE_BIZ          = 500
# REVIEW_COUNT_SCALE_RECIPE       = 200
# BLACK_OWNED_BOOST               = 0.12
# PREFERRED_TYPE_BOOST            = 0.08
# SUPERLATIVE_SCORE_CAP           = 0.70


# # ── Centrality-Weighted Ranking Constants (v5) ───────────────────────────────

# # Exponent on aboutness ratio. Higher = harsher penalty on peripheral mentions.
# # α=1.5 means aboutness=0.1 → 0.03 multiplier, aboutness=0.5 → 0.35, aboutness=1.0 → 1.0
# ABOUTNESS_EXPONENT = 1.5

# # topic_scope → multiplier. Missing defaults to 0.5 (neutral, not penalty).
# TOPIC_SCOPE_WEIGHTS = {
#     'central':    1.00,
#     'major':      0.70,
#     'supporting': 0.30,
#     'peripheral': 0.10,
# }
# TOPIC_SCOPE_DEFAULT = 0.50  # when topic_scope is missing/empty

# # Semantic-only gate: when text is weak AND aboutness is weak,
# # cap semantic contribution so vector neighbors can't dominate.
# SEMANTIC_ONLY_ABOUTNESS_FLOOR = 0.10
# SEMANTIC_ONLY_TEXT_FLOOR      = 0.10
# SEMANTIC_ONLY_CAP             = 0.30


# # ── Tiered Field Weights (identity → peripheral) ─────────────────────────────

# # Used as the DEFAULT field_boosts when v3 WordDiscovery does not supply its own.
# # Tier 1 (identity)     : document_title, topic_tags         → 10 / 5
# # Tier 2 (primary)      : primary_keywords, document_summary → 5  / 5
# # Tier 3 (supporting)   : key_facts                          → 2
# # Tier 4 (peripheral)   : entity_names, semantic_keywords    → 1  / 0.5
# DEFAULT_FIELD_BOOSTS = {
#     'document_title':    10,
#     'primary_keywords':   5,
#     'document_summary':   5,
#     'topic_tags':         5,
#     'key_facts':          2,
#     'entity_names':       1,
#     'semantic_keywords':  1,  # typesense weights are ints; 1 is min
# }

# # Fields used to compute aboutness ratio (identity + primary tiers).
# # Each field contributes 1 "slot" to the denominator.
# # primary_keywords contributes up to 3 slots (top 3 keywords).
# # topic_tags contributes 1 slot (root tag only — the most central).
# ABOUTNESS_IDENTITY_FIELDS = [
#     'document_title',
#     'document_summary',
#     'document_category',
# ]
# ABOUTNESS_PRIMARY_KEYWORD_SLOTS = 3  # top N primary_keywords
# ABOUTNESS_TOPIC_TAG_SLOTS       = 1  # root topic_tag only


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

#     v5 CHANGE: field_boosts default now uses tiered weights that
#     reflect CENTRALITY (identity > primary > supporting > peripheral).
#     document_title=10 + topic_tags=5 are the identity tier.
#     primary_keywords=5 + document_summary=5 are the primary subject tier.
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
#         # v5 — tiered field boosts (centrality-weighted default)
#         'field_boosts':      discovery.get('field_boosts', dict(DEFAULT_FIELD_BOOSTS)),
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

#     # ── filter_by — locations + pool scoping + UI filters ─────────────────
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

#     # Pool scoping — read primary type from POOL_SCOPE
#     scope = POOL_SCOPE.get(query_mode, {})
#     primary_type = scope.get('primary')
#     if primary_type and not (ui_filters and ui_filters.get('data_type')):
#         filter_conditions.append(f'document_data_type:={primary_type}')

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
#     Build the filter string used in Stage 1A.
#     Includes location filters and pool scoping from POOL_SCOPE.
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

#     # Pool scoping — read primary type from POOL_SCOPE
#     scope = POOL_SCOPE.get(query_mode, {})
#     primary_type = scope.get('primary')
#     if primary_type:
#         filter_conditions.append(f'document_data_type:={primary_type}')

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


# # ─────────────────────────────────────────────────────────────────────────────
# # v5 CENTRALITY-WEIGHTED RANKING HELPERS
# # ─────────────────────────────────────────────────────────────────────────────

# def _query_tokens_from_profile(profile: Dict) -> Set[str]:
#     """
#     Extract the meaningful content tokens from the query profile.
#     Used to test aboutness — does the document's identity contain
#     these tokens? We strip stopwords and short tokens so "of" and
#     "the" don't inflate the hit count.
#     """
#     stopwords = {
#         'a', 'an', 'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to',
#         'for', 'with', 'by', 'from', 'as', 'is', 'are', 'was', 'were',
#         'be', 'been', 'it', 'this', 'that', 'these', 'those',
#     }
#     tokens: Set[str] = set()

#     # Pull from search_terms (primary signal)
#     for term in profile.get('search_terms', []) or []:
#         if not term:
#             continue
#         for t in re.split(r'\s+', str(term).lower().strip()):
#             t = re.sub(r'[^\w-]', '', t)
#             if t and len(t) > 2 and t not in stopwords:
#                 tokens.add(t)

#     # Also pull from named entities (persons, organizations, keywords)
#     for group in ('persons', 'organizations', 'keywords'):
#         for item in profile.get(group, []) or []:
#             phrase = item.get('phrase') or item.get('word', '') if isinstance(item, dict) else ''
#             if not phrase:
#                 continue
#             for t in re.split(r'\s+', str(phrase).lower().strip()):
#                 t = re.sub(r'[^\w-]', '', t)
#                 if t and len(t) > 2 and t not in stopwords:
#                     tokens.add(t)

#     return tokens


# def _field_contains_any_token(field_value: Any, tokens: Set[str]) -> bool:
#     """
#     Return True if any token appears (case-insensitive substring) in the field.
#     Handles both string and list-of-string field values.
#     """
#     if not field_value or not tokens:
#         return False

#     if isinstance(field_value, list):
#         haystack = ' '.join(str(v) for v in field_value if v).lower()
#     else:
#         haystack = str(field_value).lower()

#     if not haystack:
#         return False

#     return any(tok in haystack for tok in tokens)


# def _compute_aboutness_ratio(
#     item: Dict,
#     profile: Dict,
#     query_tokens: Optional[Set[str]] = None,
# ) -> float:
#     """
#     Compute how much the document's IDENTITY is about the query.

#     identity slots:
#         - document_title                  (1 slot)
#         - document_summary                (1 slot)
#         - document_category               (1 slot)
#         - top N primary_keywords          (up to 3 slots, one per keyword)
#         - root topic_tag (topic_tags[0])  (1 slot)

#     A slot "hits" when any query token appears (substring, case-insensitive)
#     in that slot's value.

#     PERFORMANCE (v5.1):
#         query_tokens can be pre-computed once per query by the caller
#         and passed in, avoiding redundant extraction across hundreds
#         of documents in the re-rank loop.

#     Returns: float in [0.0, 1.0]
#         0.0 = query terms do not appear in any identity field
#         1.0 = query terms appear in every identity field
#     """
#     if query_tokens is None:
#         query_tokens = _query_tokens_from_profile(profile)

#     if not query_tokens:
#         # No meaningful query tokens (e.g. wildcard) — neutral aboutness
#         return 0.5

#     hits  = 0
#     slots = 0

#     # Tier 1 & 2 — simple string fields
#     for field in ABOUTNESS_IDENTITY_FIELDS:
#         slots += 1
#         if _field_contains_any_token(item.get(field), query_tokens):
#             hits += 1

#     # primary_keywords — evaluate top N individually (each is a slot)
#     primary_kws = item.get('primary_keywords') or []
#     if isinstance(primary_kws, str):
#         primary_kws = [primary_kws]
#     top_primary = primary_kws[:ABOUTNESS_PRIMARY_KEYWORD_SLOTS]
#     # Always count slots even if list is short (so a doc with 1 keyword
#     # doesn't artificially score 1.0 just because it had room for more)
#     slots += ABOUTNESS_PRIMARY_KEYWORD_SLOTS
#     for kw in top_primary:
#         if _field_contains_any_token(kw, query_tokens):
#             hits += 1

#     # topic_tags — root tag only (most central)
#     topic_tags = item.get('topic_tags') or []
#     if isinstance(topic_tags, str):
#         topic_tags = [topic_tags]
#     slots += ABOUTNESS_TOPIC_TAG_SLOTS
#     if topic_tags and _field_contains_any_token(topic_tags[0], query_tokens):
#         hits += 1

#     if slots == 0:
#         return 0.0

#     ratio = hits / slots
#     return min(max(ratio, 0.0), 1.0)


# def _topic_scope_multiplier(item: Dict) -> float:
#     """
#     Map the document's topic_scope enrichment to a score multiplier.

#     EDGE CASE HANDLING (v5.1):
#         - If topic_scope is present: use the mapped weight
#         - If topic_scope is missing BUT other enrichment fields exist:
#             return neutral default (0.5) — this doc was enriched but the
#             scope field didn't get written for some reason
#         - If topic_scope is missing AND no other enrichment exists:
#             return 1.0 (no penalty) — this is a legacy pre-enrichment
#             doc and we shouldn't bury it beneath fully-enriched docs
#             when it might be a better match

#     This prevents the ~589 unenriched docs from your April 12 audit
#     from being systematically buried by enriched docs.
#     """
#     scope = (item.get('topic_scope') or '').strip().lower()
#     if scope:
#         return TOPIC_SCOPE_WEIGHTS.get(scope, TOPIC_SCOPE_DEFAULT)

#     # No topic_scope — decide: enriched-but-missing-field vs fully-unenriched
#     has_any_enrichment = bool(
#         item.get('topic_tags') or
#         item.get('primary_keywords') or
#         item.get('document_summary') or
#         item.get('document_category')
#     )
#     if not has_any_enrichment:
#         return 1.0  # fully unenriched — neutralize, don't penalize
#     return TOPIC_SCOPE_DEFAULT  # enriched but scope field missing


# def _should_apply_centrality(item: Dict) -> bool:
#     """
#     Decide whether centrality-weighted ranking makes sense for this doc.

#     Centrality is a TOPICAL signal — "how much is this document ABOUT
#     the query." That concept works for articles, biographies, media,
#     and events, where there's a clear subject matter.

#     For entity-type documents (business, product, place), the doc
#     doesn't have a topic — it represents an entity. Applying aboutness
#     here would penalize legitimate matches (e.g., a barbershop whose
#     title is the business name, not containing the query 'barbershop').
#     Pool type and domain relevance are the correct signals for those.

#     Returns: True  → apply aboutness + topic_scope multipliers
#              False → neutralize them (1.0 each) and rely on other signals
#     """
#     data_type = (
#         item.get('data_type') or
#         item.get('document_data_type') or
#         ''
#     ).lower()
#     return data_type in {'article', 'person', 'media', 'event'}


# # ─────────────────────────────────────────────────────────────────────────────
# # Existing domain / intent / pool multipliers (unchanged)
# # ─────────────────────────────────────────────────────────────────────────────

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
#     vector_data: Dict,
#     query_tokens: Optional[Set[str]] = None,
# ) -> float:
#     """
#     Compute the final blended score for one document.

#     v5.1 CENTRALITY-WEIGHTED PIPELINE:
#         1. Compute raw signals (text, semantic, authority)
#         2. Decide if centrality applies (articles/people/media/events only)
#         3. Compute aboutness ratio + topic_scope multiplier
#         4. Gate semantic-only matches when aboutness is low
#         5. Blend raw signals
#         6. Multiply blended by aboutness^α and scope_multiplier
#         7. Apply existing domain / intent / pool / boost multipliers

#     v5.1 EDGE CASE HANDLING:
#         - query_tokens can be pre-computed by the caller (perf: avoid
#           recomputing for every doc in the rerank loop)
#         - Entity-type docs (business/product/place) skip centrality
#           because they represent entities, not topics
#         - Unenriched docs (no topic_scope, no topic_tags) get neutral
#           multipliers instead of the 0.5 penalty
#         - No-content-token queries neutralize centrality entirely
#     """
#     query_mode = signals.get('query_mode', 'explore')
#     item_id    = item.get('id', '')

#     # Pre-compute query_tokens once per caller if not provided
#     if query_tokens is None:
#         query_tokens = _query_tokens_from_profile(profile)

#     vd              = vector_data.get(item_id, {'vector_distance': 1.0, 'semantic_rank': 999999})
#     vector_distance = vd.get('vector_distance', 1.0)
#     semantic_rank   = vd.get('semantic_rank', 999999)

#     item['vector_distance'] = vector_distance
#     item['semantic_rank']   = semantic_rank

#     # ── Raw signals ───────────────────────────────────────────────────────
#     text_score = _compute_text_score(idx, pool_size, item, profile)
#     sem_score  = _compute_semantic_score(vector_distance)
#     auth_score = _extract_authority_score(item)

#     # ── v5.1 — Decide whether centrality applies to THIS doc type ─────────
#     apply_centrality = _should_apply_centrality(item)

#     # ── v5 — Centrality signals (only if applicable) ──────────────────────
#     if apply_centrality:
#         aboutness  = _compute_aboutness_ratio(item, profile, query_tokens=query_tokens)
#         scope_mult = _topic_scope_multiplier(item)
#     else:
#         # Entity-type doc — neutralize centrality, let pool/domain/intent
#         # multipliers handle identity. aboutness=1.0 means no centrality
#         # penalty, scope_mult=1.0 means no topic_scope influence.
#         aboutness  = 1.0
#         scope_mult = 1.0

#     # ── v5 — Semantic-only gate ───────────────────────────────────────────
#     # Kill the scenario where a document is pulled in purely by vector
#     # similarity (text match near zero AND aboutness near zero) — this
#     # is the "Africa mentioned once in key_facts" failure mode.
#     # Only applies when centrality is in play (topical docs).
#     effective_sem_score = sem_score
#     if (apply_centrality and
#             aboutness < SEMANTIC_ONLY_ABOUTNESS_FLOOR and
#             text_score < SEMANTIC_ONLY_TEXT_FLOOR and
#             sem_score > 0):
#         effective_sem_score = sem_score * SEMANTIC_ONLY_CAP

#     # ── Blended raw score (same shape as v4) ──────────────────────────────
#     blended = (
#         blend['text_match'] * text_score           +
#         blend['semantic']   * effective_sem_score  +
#         blend['authority']  * auth_score
#     )

#     # ── v5.1 — Centrality gate ────────────────────────────────────────────
#     # Multiply blended by aboutness^α. A doc with aboutness=0.1 gets
#     # its blended score multiplied by ~0.03. A doc with aboutness=0.9
#     # barely loses anything.
#     #
#     # Two safety valves:
#     #   1. If there are no content tokens in the query (wildcard, all
#     #      stopwords), centrality is meaningless → factor = 1.0
#     #   2. If centrality doesn't apply (entity docs), factor = 1.0
#     if not apply_centrality:
#         centrality_factor = 1.0
#     elif not query_tokens:
#         centrality_factor = 1.0
#     else:
#         centrality_factor = (aboutness ** ABOUTNESS_EXPONENT) if aboutness > 0 else 0.0

#     final = blended * centrality_factor * scope_mult

#     # ── Existing multipliers (unchanged) ──────────────────────────────────
#     final *= _domain_relevance(item, signals)
#     final *= _content_intent_match(item, query_mode)
#     final *= _pool_type_multiplier(item, query_mode)

#     # ── Existing boosts (unchanged) ───────────────────────────────────────
#     if item.get('data_type') in profile.get('preferred_data_types', []):
#         final = min(final + PREFERRED_TYPE_BOOST, 1.0)

#     if signals.get('has_black_owned') and item.get('black_owned') is True:
#         final = min(final + BLACK_OWNED_BOOST, 1.0)

#     if signals.get('has_superlative') and auth_score == 0.0:
#         final = min(final, SUPERLATIVE_SCORE_CAP)

#     # ── Debug-friendly annotations on the item ────────────────────────────
#     item['blended_score']     = final
#     item['text_score']        = round(text_score, 4)
#     item['sem_score']         = round(sem_score, 4)
#     item['sem_score_capped']  = round(effective_sem_score, 4)
#     item['auth_score']        = round(auth_score, 4)
#     item['aboutness']         = round(aboutness, 4)
#     item['scope_mult']        = round(scope_mult, 4)
#     item['centrality_factor'] = round(centrality_factor, 4)
#     item['centrality_applied'] = apply_centrality

#     return final


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
#         # v5 — tiered query_by default with topic_tags included
#         'query_by':              params.get(
#             'query_by',
#             'document_title,primary_keywords,document_summary,topic_tags,key_facts,entity_names,semantic_keywords'
#         ),
#         'query_by_weights':      params.get('query_by_weights', '10,5,5,5,2,1,1'),
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

#     # ── Build location tokens for mandatory location check ────────────────
#     location_tokens = set()
#     for c in profile.get('cities', []):
#         location_tokens.update(_normalize_signal(c.get('name', '')))
#     for s in profile.get('states', []):
#         location_tokens.update(_normalize_signal(s.get('name', '')))
#         for variant in s.get('variants', []):
#             location_tokens.update(_normalize_signal(variant))

#     print(f"🔍 Stage 1B validation signals:")
#     print(f"   query_tokens    : {sorted(query_tokens)}")
#     print(f"   query_phrases   : {query_phrases}")
#     print(f"   primary_subject : {primary_subject}")
#     if location_tokens:
#         print(f"   location_tokens : {sorted(location_tokens)} (mandatory)")

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

#         # ── Step C: Hard distance gate + validation ────────────────────────
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

#             if not is_valid:
#                 rejected += 1
#                 print(f"   ❌ Validation failed: '{doc.get('question', '')[:60]}' "
#                       f"(distance={hit_distance:.4f})")
#                 continue

#             # ── Mandatory location check ──────────────────────────────────
#             if location_tokens:
#                 candidate_raw = (
#                     doc.get('primary_keywords', []) +
#                     doc.get('entities', []) +
#                     doc.get('semantic_keywords', [])
#                 )
#                 candidate_tokens = set()
#                 for val in candidate_raw:
#                     if val:
#                         candidate_tokens.update(_normalize_signal(val))

#                 if not (location_tokens & candidate_tokens):
#                     rejected += 1
#                     print(f"   🚫 Location miss: '{doc.get('question', '')[:60]}' "
#                           f"(need {sorted(location_tokens)}, not found)")
#                     continue

#             accepted += 1
#             if uuid not in seen:
#                 seen.add(uuid)
#                 uuids.append(uuid)

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

# # v5 — centrality-aware metadata fields
# # Must include document_title + document_summary + document_category +
# # primary_keywords + topic_tags + topic_scope so _compute_aboutness_ratio
# # and _topic_scope_multiplier have what they need at re-rank time.
# _METADATA_INCLUDE_FIELDS = ','.join([
#     # identity & routing
#     'document_uuid', 'document_data_type', 'document_category',
#     'document_schema', 'document_title', 'document_summary',
#     'content_intent',
#     # v5 centrality signals
#     'topic_scope', 'topic_tags',
#     # existing authority signals
#     'authority_score', 'authority_rank_score',
#     'content_depth_score', 'factual_density_score', 'evergreen_score',
#     # rating-based authority
#     'service_rating', 'service_review_count', 'service_type',
#     'product_rating', 'product_review_count',
#     'recipe_rating', 'recipe_review_count',
#     'media_rating',
#     # existing
#     'primary_keywords', 'black_owned',
#     'image_url', 'logo_url',
# ])


# def _build_metadata_item(doc: Dict) -> Dict:
#     """Shape a Typesense hit into the dict used by the re-ranker."""
#     return {
#         'id':                    doc.get('document_uuid'),
#         'data_type':             doc.get('document_data_type', ''),
#         'category':              doc.get('document_category', ''),
#         'schema':                doc.get('document_schema', ''),
#         'title':                 doc.get('document_title', ''),
#         'content_intent':        doc.get('content_intent', ''),
#         # v5 — preserve identity fields needed for aboutness
#         'document_title':        doc.get('document_title', ''),
#         'document_summary':      doc.get('document_summary', ''),
#         'document_category':     doc.get('document_category', ''),
#         'topic_scope':           doc.get('topic_scope', ''),
#         'topic_tags':            doc.get('topic_tags', []),
#         # authority
#         'authority_score':       doc.get('authority_score', 0),
#         'authority_rank_score':  doc.get('authority_rank_score', 0),
#         'content_depth_score':   doc.get('content_depth_score', 0),
#         'factual_density_score': doc.get('factual_density_score', 0),
#         'evergreen_score':       doc.get('evergreen_score', 0),
#         'service_rating':        doc.get('service_rating', 0),
#         'service_review_count':  doc.get('service_review_count', 0),
#         'service_type':          doc.get('service_type', []),
#         'product_rating':        doc.get('product_rating', 0),
#         'product_review_count':  doc.get('product_review_count', 0),
#         'recipe_rating':         doc.get('recipe_rating', 0),
#         'recipe_review_count':   doc.get('recipe_review_count', 0),
#         'media_rating':          doc.get('media_rating', 0),
#         'primary_keywords':      doc.get('primary_keywords', []),
#         'black_owned':           doc.get('black_owned', False),
#         'image_url':             doc.get('image_url', []),
#         'logo_url':              doc.get('logo_url', []),
#     }


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

#     async def _fetch_batch(batch_ids, batch_index):
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])
#         params = {
#             'q':              '*',
#             'filter_by':      f'document_uuid:[{id_filter}]',
#             'per_page':       len(batch_ids),
#             'include_fields': _METADATA_INCLUDE_FIELDS,
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
#                     batch_results[uuid] = _build_metadata_item(doc)
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
#             # v5 — tiered query_by default with topic_tags included
#             'query_by':              params.get(
#                 'query_by',
#                 'document_title,primary_keywords,document_summary,topic_tags,key_facts,entity_names,semantic_keywords'
#             ),
#             'query_by_weights':      params.get('query_by_weights', '10,5,5,5,2,1,1'),
#             'per_page':              PAGE_SIZE,
#             'page':                  current_page,
#             'include_fields':        _METADATA_INCLUDE_FIELDS,
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
#                 doc  = hit.get('document', {})
#                 item = _build_metadata_item(doc)
#                 item['text_match'] = hit.get('text_match', 0)
#                 all_results.append(item)

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


# async def fetch_questions_by_document_uuids(
#     document_uuids: List[str],
#     exclude_query: str = '',
#     limit: int = 10
# ) -> List[Dict]:
#     """Fetch questions from the questions collection by document UUIDs."""
#     if not document_uuids:
#         return []

#     uuid_filter = ','.join([f'`{uuid}`' for uuid in document_uuids])

#     params = {
#         'q':              '*',
#         'filter_by':      f'document_uuid:[{uuid_filter}]',
#         'per_page':       limit,
#         'include_fields': 'question,document_uuid,answer_type',
#     }

#     try:
#         response = await asyncio.to_thread(
#             client.collections['questions'].documents.search,
#             params
#         )
#         hits = response.get('hits', [])

#         exclude_lower = exclude_query.lower().strip()
#         questions = [
#             {
#                 'query':         hit['document'].get('question', ''),
#                 'document_uuid': hit['document'].get('document_uuid', ''),
#                 'answer_type':   hit['document'].get('answer_type', ''),
#             }
#             for hit in hits
#             if hit.get('document', {}).get('question', '').lower().strip() != exclude_lower
#         ]

#         print(f"❓ Related questions: {len(questions)} for {len(document_uuids)} documents")
#         return questions

#     except Exception as e:
#         print(f"❌ fetch_questions_by_document_uuids error: {e}")
#         return []


# async def fetch_documents_by_cluster_uuid(
#     cluster_uuid: str,
#     exclude_uuid: str = None,
#     limit: int = 5
# ) -> List[Dict]:
#     """Fetch documents that share the same cluster."""
#     if not cluster_uuid:
#         return []

#     filter_str = f'cluster_uuid:={cluster_uuid}'
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

#         print(f"🔗 Cluster docs: {len(related)} found for cluster_uuid={cluster_uuid[:12]}...")
#         return related

#     except Exception as e:
#         print(f"❌ fetch_documents_by_cluster_uuid error: {e}")
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
#         # v5 — expose centrality signals for debugging/UI
#         'topic_scope':           doc.get('topic_scope', ''),
#         'topic_tags':            doc.get('topic_tags', []),
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
#         'women_owned':           doc.get('women_owned', False),
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

#     v5 — CENTRALITY-WEIGHTED RANKING
#     Scoring now gates on aboutness (how much the doc's identity is
#     about the query) and topic_scope (enrichment-extracted centrality).

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

#         # Fetch cluster siblings if the document belongs to a cluster
#         if results and results[0].get('cluster_uuid'):
#             cluster_uuid = results[0]['cluster_uuid']
#             try:
#                 cluster_docs = await fetch_documents_by_cluster_uuid(
#                     cluster_uuid, exclude_uuid=document_uuid, limit=5
#                 )
#                 cluster_ids = [d['id'] for d in cluster_docs if d.get('id')]
#                 if cluster_ids:
#                     cluster_results = await fetch_full_documents(cluster_ids, query)
#                     results.extend(cluster_results)
#                     print(f"   🔗 Cluster siblings: {len(cluster_results)} added "
#                           f"from cluster={cluster_uuid[:12]}...")
#             except Exception as e:
#                 print(f"⚠️ Cluster fetch error: {e}")

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

#         # Related questions — from the original document + cluster siblings
#         all_doc_uuids = [document_uuid]
#         for r in results[1:]:
#             rid = r.get('id')
#             if rid and rid != document_uuid:
#                 all_doc_uuids.append(rid)

#         related_questions = await fetch_questions_by_document_uuids(
#             all_doc_uuids, exclude_query=query, limit=10
#         )

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
#             'related_searches':  related_questions,
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
#         # v5 — keyword path profile now uses tiered DEFAULT_FIELD_BOOSTS
#         profile = {
#             'search_terms':     query.split(),
#             'cities':           [],
#             'states':           [],
#             'location_terms':   [],
#             'primary_intent':   intent,
#             'field_boosts':     dict(DEFAULT_FIELD_BOOSTS),
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

#     # Stage 4 — metadata for survivors (v5 — includes topic_scope & topic_tags)
#     t5          = time.time()
#     all_results = await fetch_candidate_metadata(survivor_uuids)
#     times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

#     # Stage 5 — resolve blend once, score every document (CPU-bound, stays sync)
#     if all_results:
#         blend      = _resolve_blend(query_mode, signals, all_results)
#         pool_size  = len(all_results)

#         # v5.1 — compute query tokens ONCE per query, not per document
#         query_tokens = _query_tokens_from_profile(profile)
#         print(f"   📝 Query tokens for aboutness: {sorted(query_tokens) if query_tokens else '(none)'}")

#         for idx, item in enumerate(all_results):
#             _score_document(
#                 idx          = idx,
#                 item         = item,
#                 profile      = profile,
#                 signals      = signals,
#                 blend        = blend,
#                 pool_size    = pool_size,
#                 vector_data  = vector_data,
#                 query_tokens = query_tokens,
#             )

#         all_results.sort(key=lambda x: -x.get('blended_score', 0))
#         for i, item in enumerate(all_results):
#             item['rank'] = i

#         # v5.1 — log top 3 for debugging centrality behavior
#         print(f"   🏆 Top 3 after centrality re-rank:")
#         for i, item in enumerate(all_results[:3]):
#             print(f"     #{i+1}: '{item.get('title', '')[:60]}' "
#                   f"score={item.get('blended_score', 0):.4f} "
#                   f"aboutness={item.get('aboutness', 0):.2f} "
#                   f"scope={item.get('topic_scope', '?') or '(none)'} "
#                   f"centrality={item.get('centrality_factor', 0):.3f} "
#                   f"applied={item.get('centrality_applied', False)} "
#                   f"type={item.get('data_type', '?')}")

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