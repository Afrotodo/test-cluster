"""
typesense_discovery_bridge.py
=============================
Complete search bridge between Word Discovery v2 and Typesense.

This file handles EVERYTHING:
- Word Discovery v2 integration
- Intent signal integration (query_mode, question_word, etc.)
- Query profile building (POS-based term routing, field boosts per mode)
- Embedding generation (via embedding_client.py)
- Result caching (self-contained)
- Stage 1: Graph Filter (candidate generation)
- Stage 2: Semantic Rerank (vector-based ranking with mode-specific blend)
- Facet counting from cache
- Pagination from cache
- Full document fetching
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
# IMPORTS - Word Discovery v2 and Embedding Client
# ============================================================================

try:
    from .word_discovery_fulltest import WordDiscovery
    WORD_DISCOVERY_AVAILABLE = True
    print("✅ WordDiscovery imported from .word_discovery_v2")
except ImportError:
    try:
        from word_discovery_fulltest import WordDiscovery
        WORD_DISCOVERY_AVAILABLE = True
        print("✅ WordDiscovery imported from word_discovery_v2")
    except ImportError:
        WORD_DISCOVERY_AVAILABLE = False
        print("⚠️ word_discovery_v2 not available")

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
    
    # Clean up facts
    facts = [f.rstrip('.').strip() for f in key_facts if f.strip()]
    
    if not facts:
        return ''
    
    # ─── Question-word-based fact filtering (Blueprint Step 8) ───────
    if question_word:
        qw = question_word.lower()
        if qw == 'where':
            # Prioritize facts with geographic language
            geo_words = {'located', 'bounded', 'continent', 'region', 'coast',
                         'ocean', 'border', 'north', 'south', 'east', 'west',
                         'latitude', 'longitude', 'hemisphere', 'capital',
                         'city', 'state', 'country', 'area', 'lies', 'situated'}
            relevant_facts = [f for f in facts
                              if any(gw in f.lower() for gw in geo_words)]
        elif qw == 'when':
            # Prioritize facts with dates/years/temporal language
            import re as _re
            temporal_words = {'founded', 'established', 'born', 'created',
                              'started', 'opened', 'built', 'year', 'date',
                              'century', 'decade', 'era', 'period'}
            relevant_facts = [f for f in facts
                              if any(tw in f.lower() for tw in temporal_words)
                              or _re.search(r'\b\d{4}\b', f)]
        elif qw == 'who':
            # Prioritize facts with names, roles, titles, achievements
            who_words = {'first', 'president', 'founder', 'ceo', 'leader',
                         'director', 'known', 'famous', 'awarded', 'pioneer',
                         'invented', 'created', 'named', 'appointed', 'elected'}
            relevant_facts = [f for f in facts
                              if any(ww in f.lower() for ww in who_words)]
        elif qw == 'what':
            # Prioritize definitional facts
            what_words = {'is a', 'refers to', 'defined', 'known as',
                          'type of', 'form of', 'means', 'represents'}
            relevant_facts = [f for f in facts
                              if any(ww in f.lower() for ww in what_words)]
        else:
            relevant_facts = []
        
        # Fall back to keyword match if question-word filter found nothing
        if not relevant_facts and matched_keyword:
            keyword_lower = matched_keyword.lower()
            relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        
        # Final fallback: first fact
        if not relevant_facts:
            relevant_facts = [facts[0]]
    elif matched_keyword:
        keyword_lower = matched_keyword.lower()
        relevant_facts = [f for f in facts if keyword_lower in f.lower()]
        if not relevant_facts:
            relevant_facts = [facts[0]]
    else:
        relevant_facts = [facts[0]]
    
    # Cap at 2 — keeps it concise
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

KNOWN_CITIES = frozenset([
    'atlanta', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio',
    'san diego', 'dallas', 'austin', 'jacksonville', 'fort worth', 'columbus',
    'charlotte', 'seattle', 'denver', 'boston', 'detroit', 'memphis', 'baltimore',
    'nashville', 'milwaukee', 'albuquerque', 'tucson', 'fresno', 'sacramento',
    'miami', 'oakland', 'minneapolis', 'tulsa', 'cleveland', 'new orleans',
    'birmingham', 'montgomery', 'mobile', 'jackson', 'baton rouge', 'shreveport',
    'savannah', 'charleston', 'richmond', 'norfolk', 'durham', 'raleigh',
    'greensboro', 'louisville', 'lexington', 'cincinnati', 'st louis', 'kansas city',
    'omaha', 'tampa', 'orlando', 'pittsburgh', 'las vegas', 'portland',
    'los angeles', 'san francisco', 'new york', 'brooklyn', 'queens', 'harlem',
])

# Categories for intent detection
PERSON_CATEGORIES = frozenset([
    'Person', 'Historical Figure', 'Celebrity', 'Athlete', 'Politician',
])

ORGANIZATION_CATEGORIES = frozenset([
    'Organization', 'Company', 'Business', 'Brand', 'HBCU',
])

LOCATION_CATEGORIES = frozenset([
    'US City', 'US State', 'US County', 'City', 'State', 'Country', 'Location',
    'Continent', 'Region', 'continent', 'region', 'country',
])

KEYWORD_CATEGORIES = frozenset([
    'Keyword', 'Topic', 'Primary Keyword',
])

MEDIA_CATEGORIES = frozenset([
    'Song', 'Movie', 'Album', 'Book', 'TV Show',
])

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
# POS TAGS THAT GO INTO SEARCH QUERY (Blueprint Step 1)
# ============================================================================

# Only nouns go into the Typesense q parameter
SEARCHABLE_POS = frozenset({
    'noun', 'proper_noun',
})

# Everything else is a signal — never searched
SIGNAL_POS = frozenset({
    'verb', 'be', 'auxiliary', 'modal',
    'wh_pronoun', 'pronoun',
    'preposition', 'conjunction',
    'adjective', 'adverb',
    'article', 'determiner',
    'negation', 'interjection',
})


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


def get_state_variants(state_name: str) -> List[str]:
    """Get both full name and abbreviation for a state."""
    state_lower = state_name.lower().strip()
    variants = [state_name.title()]
    
    if state_lower in US_STATE_ABBREV:
        variants.append(US_STATE_ABBREV[state_lower])
    
    for full, abbrev in US_STATE_ABBREV.items():
        if abbrev.lower() == state_lower:
            variants.append(full.title())
            break
    
    return list(set(variants))


def is_state_category(category: str) -> bool:
    """Check if category is specifically a state."""
    if not category:
        return False
    return 'state' in category.lower()


def is_city_category(category: str) -> bool:
    """Check if category is specifically a city."""
    if not category:
        return False
    cat_lower = category.lower()
    return 'city' in cat_lower or 'county' in cat_lower


# ============================================================================
# WORD DISCOVERY WRAPPER
# ============================================================================

def _run_word_discovery(query: str) -> Dict:
    """Run Word Discovery v2 on query."""
    if WORD_DISCOVERY_AVAILABLE:
        try:
            wd = WordDiscovery(verbose=False)
            result = wd.process(query)
            return result
        except Exception as e:
            print(f"⚠️ WordDiscovery error: {e}")
    
    # Fallback
    return {
        'query': query,
        'corrected_query': query,
        'terms': [],
        'ngrams': [],
        'corrections': [],
        'stats': {
            'total_words': len(query.split()),
            'valid_words': 0,
            'corrected_words': 0,
            'unknown_words': len(query.split()),
            'stopwords': 0,
            'ngram_count': 0,
        }
    }


def _run_embedding(query: str) -> Optional[List[float]]:
    """Run embedding generation."""
    return get_query_embedding(query)


# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """Run word discovery and embedding IN PARALLEL."""
#     if skip_embedding:
#         discovery = _run_word_discovery(query)
#         return discovery, None
    
#     discovery_future = _executor.submit(_run_word_discovery, query)
#     embedding_future = _executor.submit(_run_embedding, query)
    
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Re-embed if query was corrected
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower() and embedding is not None:
#         corrections = discovery.get('corrections', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant:
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# ============================================================================
# QUERY PROFILE BUILDING (Blueprint Steps 1, 4, 7)
# ============================================================================

def build_query_profile(discovery: Dict, signals: Dict = None) -> Dict:
    """
    Analyze ALL metadata from Word Discovery to understand user intent.
    
    Blueprint alignment:
    - Step 1: POS-based term routing (only nouns → q)
    - Step 4: Dynamic field weight computation from term categories + mode
    - Step 7: Location terms stripped from q, applied as filters
    
    Returns profile with:
    - Primary intent (person, organization, location, keyword, media)
    - Search terms (POS-filtered: only nouns)
    - Cities and states for filters (stripped from search terms)
    - Field boosts (mode-aware + category-aware)
    - Mode-specific Typesense parameters
    """
    query_mode = (signals or {}).get('query_mode', 'explore')
    
    profile = {
        'has_person': False,
        'has_organization': False,
        'has_location': False,
        'has_keyword': False,
        'has_media': False,
        
        'person_score': 0,
        'organization_score': 0,
        'location_score': 0,
        'keyword_score': 0,
        'media_score': 0,
        
        'persons': [],
        'organizations': [],
        'cities': [],
        'states': [],
        'keywords': [],
        'search_terms': [],       # Only nouns — POS filtered
        'location_terms': [],     # Location words stripped from q
        
        'primary_intent': 'general',
        'preferred_data_types': DATA_TYPE_PREFERENCES.get(query_mode, ['article']),
        
        # Base field weights (Blueprint Step 4 base weights)
        'field_boosts': {
            'document_title': 10,
            'entity_names': 2,
            'primary_keywords': 3,
            'key_facts': 3,
            'semantic_keywords': 2,
        },
    }
    
    if not discovery:
        return profile
    
    terms = discovery.get('terms', [])
    ngrams = discovery.get('ngrams', [])
    
    # Build term lookup by position
    term_by_position = {t.get('position', 0): t for t in terms}
    
    # Track positions consumed by n-grams
    ngram_positions = set()
    
    # =================================================================
    # Process N-grams First
    # =================================================================
    
    for ngram in ngrams:
        phrase = ngram.get('phrase', '')
        ngram_category = ngram.get('category', '')
        rank = _parse_rank(ngram.get('rank', 0))
        positions = ngram.get('positions', [])
        display = ngram.get('display', phrase)
        
        ngram_positions.update(positions)
        
        # Check individual term categories
        term_categories = []
        ngram_words = ngram.get('words', phrase.split())
        
        for i, pos in enumerate(positions):
            term = term_by_position.get(pos, {})
            term_cat = term.get('category', '')
            term_word = term.get('word', '') or (ngram_words[i] if i < len(ngram_words) else '')
            term_rank = _parse_rank(term.get('rank', 0))
            
            if term_cat and term_cat not in ('', 'stopword'):
                term_categories.append({
                    'word': term_word,
                    'category': term_cat,
                    'rank': term_rank,
                })
            else:
                word_lower = term_word.lower()
                if word_lower in US_STATE_ABBREV:
                    term_categories.append({
                        'word': term_word,
                        'category': 'US State',
                        'rank': 350,
                    })
                elif term_word.upper() in US_STATE_ABBREV.values():
                    term_categories.append({
                        'word': term_word,
                        'category': 'US State',
                        'rank': 350,
                    })
                elif word_lower in KNOWN_CITIES:
                    term_categories.append({
                        'word': term_word,
                        'category': 'US City',
                        'rank': 350,
                    })
        
        # Determine if this n-gram contains location terms
        has_city_term = any(is_city_category(tc['category']) for tc in term_categories)
        has_state_term = any(is_state_category(tc['category']) for tc in term_categories)
        
        both_terms_are_locations = has_city_term and has_state_term
        ngram_is_location = ngram_category in LOCATION_CATEGORIES
        
        if both_terms_are_locations or ngram_is_location:
            profile['has_location'] = True
            
            # Check if any terms are non-filterable (continent/country/region)
            has_filterable = any(
                is_city_category(tc['category']) or is_state_category(tc['category'])
                for tc in term_categories
            )
            is_subject = (
                query_mode == 'answer' and
                (signals or {}).get('question_word') == 'where'
            )
            
            for tc in term_categories:
                if is_city_category(tc['category']) and not is_subject:
                    city_name = tc['word'].title()
                    if city_name not in [c['name'] for c in profile['cities']]:
                        profile['cities'].append({
                            'name': city_name,
                            'rank': tc['rank'],
                        })
                    profile['location_score'] += tc['rank']
                    profile['location_terms'].append(tc['word'])
                    
                elif is_state_category(tc['category']) and not is_subject:
                    state_name = tc['word'].title()
                    if state_name not in [s['name'] for s in profile['states']]:
                        profile['states'].append({
                            'name': state_name,
                            'rank': tc['rank'],
                            'variants': get_state_variants(tc['word']),
                        })
                    profile['location_score'] += tc['rank']
                    profile['location_terms'].append(tc['word'])
                else:
                    # Continent/country/region or subject → keep in search
                    profile['location_score'] += tc['rank']
                    profile['location_terms'].append(tc['word'])
            
            # If no filterable terms or location is subject, add phrase to search
            if not has_filterable or is_subject:
                profile['search_terms'].append(phrase)
        
        elif ngram_category in PERSON_CATEGORIES:
            profile['has_person'] = True
            profile['person_score'] += rank
            profile['persons'].append({'phrase': phrase, 'display': display, 'rank': rank})
            profile['search_terms'].append(phrase)
            
        elif ngram_category in ORGANIZATION_CATEGORIES:
            profile['has_organization'] = True
            profile['organization_score'] += rank
            profile['organizations'].append({'phrase': phrase, 'display': display, 'rank': rank})
            profile['search_terms'].append(phrase)
            
        elif ngram_category in KEYWORD_CATEGORIES:
            profile['has_keyword'] = True
            profile['keyword_score'] += rank
            profile['keywords'].append({'phrase': phrase, 'display': display, 'rank': rank})
            profile['search_terms'].append(phrase)
            
        elif ngram_category in MEDIA_CATEGORIES:
            profile['has_media'] = True
            profile['media_score'] += rank
            profile['search_terms'].append(phrase)
            
        else:
            profile['search_terms'].append(phrase)
    
    # =================================================================
    # Process Individual Terms (not in n-grams)
    # Blueprint Step 1: POS-based routing — only nouns go into search
    # =================================================================
    
    for term in terms:
        position = term.get('position', 0)
        word = term.get('word', '')
        display = term.get('display', word)
        category = term.get('category', '')
        rank = _parse_rank(term.get('rank', 0))
        pos = term.get('pos', '').lower()
        is_stopword = term.get('is_stopword', False)
        part_of_ngram = term.get('part_of_ngram', False) or (position in ngram_positions)
        
        if is_stopword or part_of_ngram:
            continue
        
        # ─── Blueprint Step 1: Determine if this is a noun (needed for all routing)
        is_noun = pos in SEARCHABLE_POS
        
        # ─── Blueprint Step 7: Location terms → filter OR search ─────
        if category in LOCATION_CATEGORIES:
            profile['has_location'] = True
            profile['location_score'] += rank
            
            cat_lower = category.lower()
            is_filterable = is_city_category(category) or is_state_category(category)
            is_subject = (
                query_mode == 'answer' and
                (signals or {}).get('question_word') == 'where'
            )
            
            if is_filterable and not is_subject:
                if is_city_category(category):
                    city_name = display or word.title()
                    if city_name not in [c['name'] for c in profile['cities']]:
                        profile['cities'].append({'name': city_name, 'rank': rank})
                elif is_state_category(category):
                    state_name = display or word.title()
                    if state_name not in [s['name'] for s in profile['states']]:
                        profile['states'].append({
                            'name': state_name,
                            'rank': rank,
                            'variants': get_state_variants(word),
                        })
                profile['location_terms'].append(word)
                continue
            else:
                if is_noun:
                    profile['search_terms'].append(word)
                profile['location_terms'].append(word)
                continue
        
        # ─── Blueprint Step 1: Only nouns go into search terms ───────
        
        if category in PERSON_CATEGORIES:
            profile['has_person'] = True
            profile['person_score'] += rank
            profile['persons'].append({'word': word, 'display': display, 'rank': rank})
            if is_noun:
                profile['search_terms'].append(word)
            
        elif category in ORGANIZATION_CATEGORIES:
            profile['has_organization'] = True
            profile['organization_score'] += rank
            profile['organizations'].append({'word': word, 'display': display, 'rank': rank})
            if is_noun:
                profile['search_terms'].append(word)
            
        elif category in KEYWORD_CATEGORIES:
            profile['has_keyword'] = True
            profile['keyword_score'] += rank
            profile['keywords'].append({'word': word, 'display': display, 'rank': rank})
            if is_noun:
                profile['search_terms'].append(word)
            
        elif category in MEDIA_CATEGORIES:
            profile['has_media'] = True
            profile['media_score'] += rank
            if is_noun:
                profile['search_terms'].append(word)
            
        elif category == 'Dictionary Word':
            if is_noun:
                profile['search_terms'].append(word)
            
        else:
            if is_noun and word:
                profile['search_terms'].append(word)
    
    # =================================================================
    # Determine Primary Intent
    # =================================================================
    
    scores = {
        'person': profile['person_score'],
        'organization': profile['organization_score'],
        'location': profile['location_score'],
        'keyword': profile['keyword_score'],
        'media': profile['media_score'],
    }
    
    max_score = max(scores.values())
    if max_score > 0:
        profile['primary_intent'] = max(scores, key=scores.get)
    else:
        profile['primary_intent'] = 'general'
    
    # =================================================================
    # Set Field Boosts — Blueprint Step 4
    # =================================================================
    
    boosts = _compute_field_boosts(profile, query_mode, signals)
    profile['field_boosts'] = boosts
    
    return profile


def _compute_field_boosts(profile: Dict, query_mode: str, signals: Dict = None) -> Dict:
    """
    Blueprint Step 4: Dynamic field weight computation.
    """
    signals = signals or {}
    
    boosts = {
        'document_title': 10,
        'entity_names': 2,
        'primary_keywords': 3,
        'key_facts': 3,
        'semantic_keywords': 2,
    }
    
    if query_mode == 'answer':
        boosts['document_title'] = 20
        boosts['entity_names'] = 15
        if signals.get('wants_single_result'):
            boosts = {
                'document_title': 20,
                'entity_names': 15,
                'primary_keywords': 5,
            }
    elif query_mode == 'browse':
        boosts['primary_keywords'] = 15
        boosts['semantic_keywords'] = 10
    elif query_mode == 'local':
        boosts['primary_keywords'] = 12
    elif query_mode == 'compare':
        boosts['entity_names'] = 15
        boosts['document_title'] = 15
    elif query_mode == 'shop':
        boosts['primary_keywords'] = 12
        boosts['document_title'] = 10
    
    if profile.get('has_person'):
        best_rank = max((p.get('rank', 0) for p in profile.get('persons', [])), default=0)
        rank_bonus = min(best_rank // 100, 5)
        boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
        boosts['document_title'] = boosts.get('document_title', 10) + 5
    
    if profile.get('has_organization'):
        best_rank = max((o.get('rank', 0) for o in profile.get('organizations', [])), default=0)
        rank_bonus = min(best_rank // 100, 5)
        boosts['entity_names'] = boosts.get('entity_names', 2) + 10 + rank_bonus
        boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
    
    if profile.get('has_keyword'):
        best_rank = max((k.get('rank', 0) for k in profile.get('keywords', [])), default=0)
        rank_bonus = min(best_rank // 100, 5)
        boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 10 + rank_bonus
        boosts['semantic_keywords'] = boosts.get('semantic_keywords', 2) + 5
        boosts['key_facts'] = boosts.get('key_facts', 3) + 4
    
    if profile.get('has_media'):
        best_rank = max((profile.get('media_score', 0),), default=0)
        rank_bonus = min(best_rank // 100, 5)
        boosts['document_title'] = boosts.get('document_title', 10) + 10 + rank_bonus
        boosts['primary_keywords'] = boosts.get('primary_keywords', 3) + 5
        boosts['entity_names'] = boosts.get('entity_names', 2) + 4
    
    has_unknown = signals.get('has_unknown_terms', False)
    has_known = (profile.get('has_person') or profile.get('has_organization')
                 or profile.get('has_keyword') or profile.get('has_media'))
    
    if has_unknown and has_known:
        for field in boosts:
            boosts[field] += 3
    elif has_unknown and not has_known:
        for field in boosts:
            boosts[field] += 8
    
    return boosts


# ============================================================================
# TYPESENSE PARAMETER BUILDING (Blueprint Steps 2, 3, 6)
# ============================================================================

def build_typesense_params(profile: Dict, ui_filters: Dict = None,
                           signals: Dict = None) -> Dict:
    """
    Convert query profile into Typesense search parameters.
    """
    signals = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    
    params = {}
    
    search_terms = profile.get('search_terms', [])
    seen = set()
    unique_terms = []
    for term in search_terms:
        term_lower = term.lower()
        if term_lower not in seen:
            seen.add(term_lower)
            unique_terms.append(term)
    
    params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
    field_boosts = profile.get('field_boosts', {})
    
    if query_mode == 'local':
        if 'service_type' not in field_boosts:
            field_boosts['service_type'] = 12
        if 'service_specialties' not in field_boosts:
            field_boosts['service_specialties'] = 10
    
    sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    params['query_by'] = ','.join([f[0] for f in sorted_fields])
    params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
    has_corrections = len(profile.get('corrections', [])) > 0 if isinstance(profile.get('corrections'), list) else False
    term_count = len(unique_terms)
    
    if query_mode == 'answer':
        params['num_typos'] = 0
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 0
    elif query_mode == 'explore':
        params['num_typos'] = 0 if has_corrections else 1
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 0 if term_count <= 2 else 1
    elif query_mode == 'browse':
        params['num_typos'] = 1
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 1 if term_count <= 3 else 2
    elif query_mode == 'local':
        params['num_typos'] = 1
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 1
    elif query_mode == 'compare':
        params['num_typos'] = 0
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 0
    elif query_mode == 'shop':
        params['num_typos'] = 1
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 1
    else:
        params['num_typos'] = 1
        params['prefix'] = 'no'
        params['drop_tokens_threshold'] = 1
    
    temporal_direction = signals.get('temporal_direction')
    price_direction = signals.get('price_direction')
    has_superlative = signals.get('has_superlative', False)
    has_rating = signals.get('has_rating_signal', False)
    
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
    
    filter_conditions = []
    
    cities = profile.get('cities', [])
    states = profile.get('states', [])
    
    local_strength = signals.get('local_search_strength', 'none')
    is_location_subject = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )
    
    apply_location_filter = True
    if is_location_subject:
        apply_location_filter = False
    
    if apply_location_filter:
        if cities:
            city_filters = [f"location_city:={c['name']}" for c in cities]
            if len(city_filters) == 1:
                filter_conditions.append(city_filters[0])
            else:
                filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
        if states:
            state_conditions = []
            for state in states:
                variants = state.get('variants', [state['name']])
                for variant in variants:
                    state_conditions.append(f"location_state:={variant}")
            
            if len(state_conditions) == 1:
                filter_conditions.append(state_conditions[0])
            else:
                filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
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
    signals = signals or {}
    filter_conditions = []
    
    query_mode = signals.get('query_mode', 'explore')
    is_location_subject = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )
    
    if not is_location_subject:
        cities = profile.get('cities', [])
        states = profile.get('states', [])
        
        if cities:
            city_filters = [f"location_city:={c['name']}" for c in cities]
            if len(city_filters) == 1:
                filter_conditions.append(city_filters[0])
            else:
                filter_conditions.append('(' + ' || '.join(city_filters) + ')')
        
        if states:
            state_conditions = []
            for state in states:
                variants = state.get('variants', [state['name']])
                for variant in variants:
                    state_conditions.append(f"location_state:={variant}")
            
            if len(state_conditions) == 1:
                filter_conditions.append(state_conditions[0])
            else:
                filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
    if signals.get('has_black_owned', False):
        filter_conditions.append('black_owned:=true')
    
    return ' && '.join(filter_conditions) if filter_conditions else ''


# # ============================================================================
# # STAGE 1: GRAPH FILTER - Candidate Generation
# # ============================================================================



# # ============================================================================
# # STAGE 1 (SEMANTIC): Fetch ONLY document_uuids
# # ============================================================================

# def fetch_candidate_uuids(
#     search_query: str,
#     profile: Dict,
#     signals: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[str]:
#     """
#     Stage 1 (Semantic path): Keyword search using intent-based fields.
#     Returns ONLY a list of document_uuid strings — no metadata.
#     Metadata is fetched later in Stage 4 for survivors only.
#     """
#     signals = signals or {}
#     params = build_typesense_params(profile, signals=signals)
#     filter_str = build_filter_string_without_data_type(profile, signals=signals)

#     PAGE_SIZE = 250
#     all_uuids = []
#     current_page = 1
#     max_pages = (max_results // PAGE_SIZE) + 1

#     query_mode = signals.get('query_mode', 'explore')

#     print(f"🔍 Stage 1 (uuids only): '{params.get('q', search_query)}'")
#     print(f"   Mode: {query_mode}")
#     print(f"   Fields: {params.get('query_by', '')}")
#     print(f"   Weights: {params.get('query_by_weights', '')}")
#     print(f"   num_typos: {params.get('num_typos', 1)} | prefix: {params.get('prefix', 'yes')}")
#     print(f"   sort_by: {params.get('sort_by', 'default')}")
#     if filter_str:
#         print(f"   Filters: {filter_str}")

#     while len(all_uuids) < max_results and current_page <= max_pages:
#         search_params = {
#             'q': params.get('q', search_query),
#             'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
#             'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
#             'per_page': PAGE_SIZE,
#             'page': current_page,
#             'include_fields': 'document_uuid',
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
#                 uuid = doc.get('document_uuid')
#                 if uuid:
#                     all_uuids.append(uuid)

#             if len(all_uuids) >= found or len(hits) < PAGE_SIZE:
#                 break

#             current_page += 1

#         except Exception as e:
#             print(f"❌ Stage 1 error (page {current_page}): {e}")
#             break

#     print(f"📊 Stage 1: Retrieved {len(all_uuids)} candidate UUIDs")
#     return all_uuids[:max_results]
# ============================================================================
# RUN_PARALLEL_PREP FIX — Replace existing run_parallel_prep function
# ============================================================================

def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
    """
    Run word discovery and embedding IN PARALLEL.

    FIX: Always embed the ORIGINAL query first.
    Only re-embed with corrected_query if:
      - The correction is a genuine dictionary word fix
      - NOT a proper noun being mangled into a common word
      - NOT a name being changed to food/city/other wrong category
    """
    if skip_embedding:
        discovery = _run_word_discovery(query)
        return discovery, None

    # Always embed original query in parallel with word discovery
    discovery_future = _executor.submit(_run_word_discovery, query)
    embedding_future = _executor.submit(_run_embedding, query)  # ← always original

    discovery = discovery_future.result()
    embedding = embedding_future.result()

    # ── Decide if re-embedding with corrected query is safe ───────────────
    corrected_query = discovery.get('corrected_query', query)

    if corrected_query.lower() != query.lower() and embedding is not None:
        corrections = discovery.get('corrections', [])

        # Only re-embed if corrections are genuine dictionary fixes
        # NOT if they are proper nouns being mangled into wrong categories
        SAFE_CORRECTION_TYPES = {'spelling', 'phonetic', 'abbreviation'}

        UNSAFE_CATEGORIES = {
            'Food', 'US City', 'US State', 'Country', 'Location',
            'City', 'Place', 'Object', 'Animal', 'Color',
        }

        safe_corrections = []
        unsafe_corrections = []

        for c in corrections:
            corrected_category = c.get('category', '')
            correction_type    = c.get('correction_type', '')
            original           = c.get('original', '')
            corrected          = c.get('corrected', '')

            # Flag as unsafe if:
            # 1. Correction type is pos_mismatch (word discovery guessing)
            # 2. Corrected category is something clearly wrong (Food, City, etc.)
            # 3. Original was classified as Person/Organization (proper noun)
            is_pos_mismatch    = correction_type == 'pos_mismatch'
            is_wrong_category  = corrected_category in UNSAFE_CATEGORIES
            is_proper_noun     = c.get('category', '') in ('Person', 'Organization', 'Brand')

            if is_pos_mismatch or is_wrong_category or is_proper_noun:
                unsafe_corrections.append(c)
            else:
                safe_corrections.append(c)

        has_safe_corrections   = len(safe_corrections) > 0
        has_unsafe_corrections = len(unsafe_corrections) > 0

        if has_unsafe_corrections:
            # Do NOT re-embed — original embedding is more accurate
            print(f"⚠️  Skipping re-embed — unsafe corrections detected:")
            for c in unsafe_corrections:
                print(f"     '{c.get('original')}' → '{c.get('corrected')}' "
                      f"(type={c.get('correction_type')}, category={c.get('category')})")
            print(f"   Keeping original query embedding: '{query}'")

        elif has_safe_corrections:
            # Safe to re-embed with corrected query
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
    signals = signals or {}
    params = build_typesense_params(profile, signals=signals)
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
        hits = response.get('hits', [])

        uuids = []
        for hit in hits:
            doc = hit.get('document', {})
            uuid = doc.get('document_uuid')
            if uuid:
                uuids.append(uuid)

        print(f"📊 Stage 1A (document): {len(uuids)} candidate UUIDs")
        return uuids

    except Exception as e:
        print(f"❌ Stage 1A error: {e}")
        return []


# # ============================================================================
# # STAGE 1B: Questions collection — facet filter + vector search, 50 candidates
# # ============================================================================

# def fetch_candidate_uuids_from_questions(
#     profile: Dict,
#     query_embedding: List[float],
#     signals: Dict = None,
#     max_results: int = 50
# ) -> List[str]:
#     """
#     Stage 1B: Two-step search against the questions collection.

#     FIX 1: Uses the ORIGINAL query embedding (protected in run_parallel_prep)
#             so proper noun mangling does not corrupt the vector search.

#     FIX 2: Facet filter is built more carefully:
#            - Entity names are validated — single-word fragments like
#              "prentice" or "herman" are weak and may not exist in the
#              entities field as standalone values. We detect this and
#              fall back to question_type only when entities are fragments.
#            - This prevents an over-narrow filter that returns 0 hits
#              when word discovery breaks a proper name into parts.

#     Step A — build facet filter from profile metadata
#     Step B — vector search within that filtered subset
#     """
#     signals = signals or {}

#     if not query_embedding:
#         print("⚠️ Stage 1B (questions): no embedding — skipping")
#         return []

#     # ── Step A: Build facet filter ────────────────────────────────────────
#     filter_parts = []

#     # primary_keywords — use top 3
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

#     # entities — validate that names are meaningful multi-word phrases
#     # Single-word fragments (e.g. "prentice", "herman") are unreliable
#     # because the entities field stores full names like "Prentice Herman Polk"
#     entity_names = []
#     for p in profile.get('persons', []):
#         name = p.get('phrase') or p.get('word', '')
#         # Only use entity if it looks like a full name (has a space)
#         # or is clearly a known proper noun (capitalized, rank > 100)
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

#     # semantic_keywords — use top 3
#     semantic_kws = profile.get('semantic_keywords', [])
#     semantic_kws = [kw for kw in semantic_kws if kw][:3]

#     if semantic_kws:
#         sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
#         filter_parts.append(f'semantic_keywords:[{sem_values}]')

#     # question_type — always include when we have a question word signal
#     # This is the most reliable filter when entity names are fragments
#     question_word = signals.get('question_word', '')
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

#     # ── Filter strategy: ──────────────────────────────────────────────────
#     # If we have strong entity names → use OR (broad net)
#     # If we only have question_type → use it alone (still narrows well)
#     # If we have nothing → no filter (full vector scan)
#     if filter_parts:
#         filter_str = ' || '.join(filter_parts)
#     else:
#         filter_str = ''

#     print(f"🔍 Stage 1B (questions): vector search within facet filter")
#     print(f"   primary_keywords : {primary_kws}")
#     print(f"   entities         : {entity_names}")
#     print(f"   semantic_keywords: {semantic_kws}")
#     print(f"   question_type    : {question_type or 'any'}")
#     print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

#     # ── Step B: Vector search within filtered subset ──────────────────────
#     embedding_str = ','.join(str(x) for x in query_embedding)

#     search_params = {
#         'q':            '*',
#         'vector_query': f'embedding:([{embedding_str}], k:{max_results})',
#         'per_page':     max_results,
#         'include_fields': 'document_uuid,question,answer_type,question_type',
#     }

#     if filter_str:
#         search_params['filter_by'] = filter_str

#     try:
#         search_requests = {'searches': [{'collection': 'questions', **search_params}]}
#         response = client.multi_search.perform(search_requests, {})
#         result = response['results'][0]
#         hits = result.get('hits', [])

#         # ── If filter returned too few hits, retry without filter ─────────
#         # This is the safety net for when all filter parts are too narrow
#         if len(hits) < 5 and filter_str:
#             print(f"⚠️  Stage 1B: only {len(hits)} hits with filter — retrying without filter")
#             search_params_fallback = {
#                 'q':              '*',
#                 'vector_query':   f'embedding:([{embedding_str}], k:{max_results})',
#                 'per_page':       max_results,
#                 'include_fields': 'document_uuid,question,answer_type,question_type',
#             }
#             search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
#             response_fallback = client.multi_search.perform(search_requests_fallback, {})
#             hits = response_fallback['results'][0].get('hits', [])
#             print(f"   Fallback returned {len(hits)} hits")

#         uuids = []
#         seen = set()
#         for hit in hits:
#             doc = hit.get('document', {})
#             uuid = doc.get('document_uuid')
#             if uuid and uuid not in seen:
#                 seen.add(uuid)
#                 uuids.append(uuid)

#             if len(uuids) >= max_results:
#                 break

#         print(f"📊 Stage 1B (questions): {len(uuids)} candidate UUIDs from {len(hits)} question hits")
#         return uuids

#     except Exception as e:
#         print(f"❌ Stage 1B error: {e}")
#         return []

# ============================================================================
# STAGE 1B: Questions collection — facet filter + vector search + validation
# ============================================================================

# Stopwords and question words to exclude from signal matching
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
    """
    Normalize a signal string into a set of meaningful tokens.
    - Lowercase
    - Strip punctuation except hyphens inside words
    - Remove stopwords and question words
    - Keep only tokens longer than 2 characters
    """
    import re
    if not text:
        return set()

    # Lowercase
    text = text.lower()

    # Replace punctuation with spaces (keep hyphens between word chars)
    text = re.sub(r"[^\w\s-]", " ", text)
    text = re.sub(r"\s*-\s*", " ", text)  # normalize hyphens

    tokens = text.split()

    return {
        t for t in tokens
        if len(t) > 2 and t not in _MATCH_STOPWORDS
    }


def _extract_query_signals(profile: Dict) -> tuple:
    """
    Extract and normalize all meaningful query signals from the profile.
    Returns:
        all_tokens   — set of all individual normalized tokens
        full_phrases — list of normalized full phrase strings (for substring match)
        primary_subject — the highest-ranked entity/keyword (must-match candidate)
    """
    raw_signals = []
    ranked_signals = []  # (rank, phrase)

    # Persons — highest priority
    for p in profile.get('persons', []):
        phrase = p.get('phrase') or p.get('word', '')
        rank   = p.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    # Organizations
    for o in profile.get('organizations', []):
        phrase = o.get('phrase') or o.get('word', '')
        rank   = o.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    # Keywords
    for k in profile.get('keywords', []):
        phrase = k.get('phrase') or k.get('word', '')
        rank   = k.get('rank', 0)
        if phrase:
            raw_signals.append(phrase)
            ranked_signals.append((rank, phrase))

    # Primary keywords from profile (if populated)
    for kw in profile.get('primary_keywords', []):
        if kw:
            raw_signals.append(kw)
            ranked_signals.append((0, kw))

    # Search terms (nouns from word discovery)
    for term in profile.get('search_terms', []):
        if term:
            raw_signals.append(term)

    # Build token set and full phrase list
    all_tokens   = set()
    full_phrases = []

    for phrase in raw_signals:
        normalized = _normalize_signal(phrase)
        all_tokens.update(normalized)
        phrase_lower = phrase.lower().strip()
        if phrase_lower:
            full_phrases.append(phrase_lower)

    # Primary subject = highest ranked signal
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

    Level 1 — Exact token match (case insensitive)
    Level 2 — Partial token match (query token inside candidate string)
    Level 3 — Substring containment (query phrase inside candidate or vice versa)
    Level 4 — Token overlap (shared meaningful tokens between strings)

    Rules:
    - At least min_matches signals must match
    - If primary_subject is provided and query has 3+ signals,
      primary subject must be one of the matches (prevents Grammy
      matching on Beyoncé when user asked about Dr. Dre)

    Returns True if hit passes validation, False if it should be discarded.
    """
    if not query_tokens:
        # No signals to validate against — accept everything
        return True

    # Collect candidate values from the hit
    candidate_raw = []
    candidate_raw.extend(hit_doc.get('primary_keywords', []))
    candidate_raw.extend(hit_doc.get('entities', []))
    candidate_raw.extend(hit_doc.get('semantic_keywords', []))

    if not candidate_raw:
        return False

    # Normalize all candidate values
    candidate_tokens   = set()
    candidate_phrases  = []

    for val in candidate_raw:
        if not val:
            continue
        normalized = _normalize_signal(val)
        candidate_tokens.update(normalized)
        candidate_phrases.append(val.lower().strip())

    candidate_text = ' '.join(candidate_phrases)

    match_count         = 0
    primary_subject_hit = False

    # ── Level 1: Exact token match ────────────────────────────────────────
    exact_matches = query_tokens & candidate_tokens
    if exact_matches:
        match_count += len(exact_matches)
        if primary_subject and (primary_subject & exact_matches):
            primary_subject_hit = True

    # ── Level 2: Partial token match ─────────────────────────────────────
    # Query token appears as substring inside any candidate token
    for qt in query_tokens:
        if qt in exact_matches:
            continue  # already counted
        for ct in candidate_tokens:
            if qt in ct or ct in qt:
                match_count += 1
                if primary_subject and qt in primary_subject:
                    primary_subject_hit = True
                break

    # ── Level 3: Substring containment ───────────────────────────────────
    # Full query phrase appears inside candidate text or vice versa
    for qp in query_phrases:
        if len(qp) < 3:
            continue
        if qp in candidate_text:
            match_count += 1
            if primary_subject:
                qp_tokens = _normalize_signal(qp)
                if qp_tokens & primary_subject:
                    primary_subject_hit = True
        else:
            # Check if any candidate phrase contains the query phrase
            for cp in candidate_phrases:
                if qp in cp or cp in qp:
                    match_count += 1
                    if primary_subject:
                        qp_tokens = _normalize_signal(qp)
                        if qp_tokens & primary_subject:
                            primary_subject_hit = True
                    break

    # ── Level 4: Token overlap ────────────────────────────────────────────
    # Shared meaningful tokens between query and candidate
    # Only counts tokens not already matched
    remaining_query = query_tokens - exact_matches
    token_overlap   = remaining_query & candidate_tokens
    if token_overlap:
        match_count += len(token_overlap)
        if primary_subject and (primary_subject & token_overlap):
            primary_subject_hit = True

    # ── Decision ──────────────────────────────────────────────────────────
    if match_count < min_matches:
        return False

    # If query has 3+ signals AND we have a primary subject,
    # primary subject must be one of the matches.
    # This prevents "Grammy" alone matching Dr. Dre questions
    # to Beyoncé Grammy questions.
    if primary_subject and len(query_tokens) >= 3:
        if not primary_subject_hit:
            return False

    return True


def fetch_candidate_uuids_from_questions(
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
    max_results: int = 50
) -> List[str]:
    """
    Stage 1B: Two-step search against the questions collection.

    Step A — Build facet filter from profile metadata to narrow
              the questions pool before the vector scan.

    Step B — Run vector search within that filtered subset.

    Step C — NEW: Validate each hit against query signals using
              4-level case-insensitive partial matching before
              accepting into the candidate pool.
              This prevents structurally similar but topically
              unrelated questions from polluting the pool
              (e.g. Bivins birth question matching Polk birth query,
              Beyoncé Grammy question matching Dr. Dre Grammy query).

    Returns up to max_results validated document_uuid strings.
    """
    signals = signals or {}

    if not query_embedding:
        print("⚠️ Stage 1B (questions): no embedding — skipping")
        return []

    # ── Extract query signals for validation ─────────────────────────────
    query_tokens, query_phrases, primary_subject = _extract_query_signals(profile)

    print(f"🔍 Stage 1B validation signals:")
    print(f"   query_tokens    : {sorted(query_tokens)}")
    print(f"   query_phrases   : {query_phrases}")
    print(f"   primary_subject : {primary_subject}")

    # ── Step A: Build facet filter ────────────────────────────────────────
    filter_parts = []

    primary_kws = profile.get('primary_keywords', [])
    if not primary_kws:
        primary_kws = [
            k.get('phrase') or k.get('word', '')
            for k in profile.get('keywords', [])
        ]
    primary_kws = [kw for kw in primary_kws if kw][:3]

    if primary_kws:
        kw_values = ','.join([f'`{kw}`' for kw in primary_kws])
        filter_parts.append(f'primary_keywords:[{kw_values}]')

    # Only use entity names that are full names (have space) or high rank
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

    semantic_kws = profile.get('semantic_keywords', [])
    semantic_kws = [kw for kw in semantic_kws if kw][:3]
    if semantic_kws:
        sem_values = ','.join([f'`{kw}`' for kw in semantic_kws])
        filter_parts.append(f'semantic_keywords:[{sem_values}]')

    question_word = signals.get('question_word', '')
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

    filter_str = ' || '.join(filter_parts) if filter_parts else ''

    print(f"   primary_keywords : {primary_kws}")
    print(f"   entities         : {entity_names}")
    print(f"   semantic_keywords: {semantic_kws}")
    print(f"   question_type    : {question_type or 'any'}")
    print(f"   filter_by        : {filter_str[:120] if filter_str else 'none (full vector scan)'}")

    # ── Step B: Vector search within filtered subset ──────────────────────
    embedding_str = ','.join(str(x) for x in query_embedding)

    search_params = {
        'q':              '*',
        'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',  # fetch extra for validation
        'per_page':       max_results * 2,
        'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
    }

    if filter_str:
        search_params['filter_by'] = filter_str

    try:
        search_requests = {'searches': [{'collection': 'questions', **search_params}]}
        response = client.multi_search.perform(search_requests, {})
        result = response['results'][0]
        hits = result.get('hits', [])

        # Fallback: retry without filter if too few hits
        if len(hits) < 5 and filter_str:
            print(f"⚠️  Stage 1B: only {len(hits)} hits with filter — retrying without filter")
            search_params_fallback = {
                'q':              '*',
                'vector_query':   f'embedding:([{embedding_str}], k:{max_results * 2})',
                'per_page':       max_results * 2,
                'include_fields': 'document_uuid,question,answer_type,question_type,primary_keywords,entities,semantic_keywords',
            }
            search_requests_fallback = {'searches': [{'collection': 'questions', **search_params_fallback}]}
            response_fallback = client.multi_search.perform(search_requests_fallback, {})
            hits = response_fallback['results'][0].get('hits', [])
            print(f"   Fallback returned {len(hits)} hits")

        # ── Step C: Validate each hit against query signals ───────────────
        uuids       = []
        seen        = set()
        accepted    = 0
        rejected    = 0

        for hit in hits:
            doc  = hit.get('document', {})
            uuid = doc.get('document_uuid')

            if not uuid:
                continue

            # Validate hit against query signals
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
# STAGE 1 COMBINED: Run both in parallel, merge + dedup
# ============================================================================

def fetch_all_candidate_uuids(
    search_query: str,
    profile: Dict,
    query_embedding: List[float],
    signals: Dict = None,
) -> List[str]:
    """
    Runs Stage 1A (document) and Stage 1B (questions) in parallel.

    Merge order:
    1. Overlap — found by both paths (highest confidence)
    2. Document-only hits
    3. Question-only hits

    Stage 1B runs independently of Stage 1A results.
    Even if Stage 1A returns 0 (e.g. bad keyword graph), Stage 1B
    can still surface the right document via vector search.
    """
    signals = signals or {}

    doc_future = _executor.submit(
        fetch_candidate_uuids, search_query, profile, signals, 100
    )
    q_future = _executor.submit(
        fetch_candidate_uuids_from_questions, profile, query_embedding, signals, 50
    )

    doc_uuids = doc_future.result()
    q_uuids   = q_future.result()

    # Find overlap
    doc_set = set(doc_uuids)
    q_set   = set(q_uuids)
    overlap  = doc_set & q_set

    # Merge: overlap first, then document-only, then question-only
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
    signals = signals or {}
    params = build_typesense_params(profile, signals=signals)
    filter_str = build_filter_string_without_data_type(profile, signals=signals)

    PAGE_SIZE = 250
    all_results = []
    current_page = 1
    max_pages = (max_results // PAGE_SIZE) + 1

    query_mode = signals.get('query_mode', 'explore')

    print(f"🔍 Stage 1 (keyword, with metadata): '{params.get('q', search_query)}'")
    print(f"   Mode: {query_mode}")
    print(f"   Fields: {params.get('query_by', '')}")
    if filter_str:
        print(f"   Filters: {filter_str}")

    while len(all_results) < max_results and current_page <= max_pages:
        search_params = {
            'q': params.get('q', search_query),
            'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
            'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
            'per_page': PAGE_SIZE,
            'page': current_page,
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
            'num_typos': params.get('num_typos', 0),
            'prefix': params.get('prefix', 'no'),
            'drop_tokens_threshold': params.get('drop_tokens_threshold', 0),
            'sort_by': params.get('sort_by', '_text_match:desc,authority_score:desc'),
        }

        if filter_str:
            search_params['filter_by'] = filter_str

        try:
            response = client.collections[COLLECTION_NAME].documents.search(search_params)
            hits = response.get('hits', [])
            found = response.get('found', 0)

            if not hits:
                break

            for hit in hits:
                doc = hit.get('document', {})
                all_results.append({
                    'id': doc.get('document_uuid'),
                    'data_type': doc.get('document_data_type', ''),
                    'category': doc.get('document_category', ''),
                    'schema': doc.get('document_schema', ''),
                    'authority_score': doc.get('authority_score', 0),
                    'text_match': hit.get('text_match', 0),
                    'image_url': doc.get('image_url', []),
                    'logo_url': doc.get('logo_url', []),
                })

            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break

            current_page += 1

        except Exception as e:
            print(f"❌ Stage 1 error (page {current_page}): {e}")
            break

    print(f"📊 Stage 1 (keyword): Retrieved {len(all_results)} candidates with metadata")
    return all_results[:max_results]
def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 250
) -> List[Dict]:
    """
    Stage 2: Semantic Rerank - Pure Vector Ranking
    """
    if not candidate_ids or not query_embedding:
        return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i}
                for i, cid in enumerate(candidate_ids)]
    
    ids_to_rerank = candidate_ids[:max_to_rerank]
    id_filter = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
    embedding_str = ','.join(str(x) for x in query_embedding)
    
    params = {
        'q': '*',
        'vector_query': f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",
        'filter_by': f'document_uuid:[{id_filter}]',
        'per_page': len(ids_to_rerank),
        'include_fields': 'document_uuid',
    }
    
    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response = client.multi_search.perform(search_requests, {})
        result = response['results'][0]
        hits = result.get('hits', [])
        
        reranked = []
        for i, hit in enumerate(hits):
            doc = hit.get('document', {})
            reranked.append({
                'id': doc.get('document_uuid'),
                'vector_distance': hit.get('vector_distance', 1.0),
                'semantic_rank': i
            })
        
        reranked_ids = {r['id'] for r in reranked}
        for cid in ids_to_rerank:
            if cid not in reranked_ids:
                reranked.append({
                    'id': cid,
                    'vector_distance': 1.0,
                    'semantic_rank': len(reranked)
                })
        
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
    """
    Apply semantic ranking to cached results with mode-specific blend ratios.
    """
    if not reranked_results:
        return cached_results
    
    signals = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    
    blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()
    
    if query_mode == 'answer' and signals.get('wants_single_result'):
        blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}
    
    if signals.get('has_unknown_terms', False):
        shift = min(0.15, blend['text_match'])
        blend['text_match'] -= shift
        blend['semantic'] += shift
        print(f"   📊 Unknown term shift: text_match={blend['text_match']:.2f}, semantic={blend['semantic']:.2f}")
    
    if signals.get('has_superlative', False):
        shift = min(0.10, blend['semantic'])
        blend['semantic'] -= shift
        blend['authority'] += shift
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
            'semantic_rank': r['semantic_rank'],
            'vector_distance': r.get('vector_distance', 1.0)
        }
        for r in reranked_results
    }
    
    total_candidates = len(cached_results)
    max_sem_rank = len(reranked_results)
    
    for idx, item in enumerate(cached_results):
        item_id = item.get('id')
        authority = item.get('authority_score', 0)
        
        if item_id in rank_lookup:
            item['semantic_rank'] = rank_lookup[item_id]['semantic_rank']
            item['vector_distance'] = rank_lookup[item_id]['vector_distance']
        else:
            item['semantic_rank'] = 999999
            item['vector_distance'] = 1.0
        
        text_score = 1.0 - (idx / max(total_candidates, 1))
        sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
        auth_score = min(authority / 100.0, 1.0)
        
        item['blended_score'] = (
            blend['text_match'] * text_score +
            blend['semantic'] * sem_score +
            blend['authority'] * auth_score
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
    category_counts = {}
    schema_counts = {}
    
    for item in cached_results:
        dt = item.get('data_type', '')
        if dt:
            data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
        
        cat = item.get('category', '')
        if cat:
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        sch = item.get('schema', '')
        if sch:
            schema_counts[sch] = schema_counts.get(sch, 0) + 1
    
    return {
        'data_type': [
            {
                'value': dt,
                'count': count,
                'label': DATA_TYPE_LABELS.get(dt, dt.title())
            }
            for dt, count in sorted(data_type_counts.items(), key=lambda x: -x[1])
        ],
        'category': [
            {
                'value': cat,
                'count': count,
                'label': CATEGORY_LABELS.get(cat, cat.replace('_', ' ').title())
            }
            for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])
        ],
        'schema': [
            {
                'value': sch,
                'count': count,
                'label': sch
            }
            for sch, count in sorted(schema_counts.items(), key=lambda x: -x[1])
        ]
    }


# ============================================================================
# FILTER AND PAGINATE CACHE
# ============================================================================

def filter_cached_results(
    cached_results: List[Dict],
    data_type: str = None,
    category: str = None,
    schema: str = None
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
    end = start + per_page
    
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
        'q': '*',
        'filter_by': f'document_uuid:[{id_filter}]',
        'per_page': len(document_ids),
        'exclude_fields': 'embedding',
    }
    
    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response = client.multi_search.perform(search_requests, {})
        result = response['results'][0]
        hits = result.get('hits', [])
        
        doc_map = {}
        for hit in hits:
            doc = hit.get('document', {})
            doc_id = doc.get('document_uuid')
            if doc_id:
                doc_map[doc_id] = format_result(hit, query)
        
        results = []
        for doc_id in document_ids:
            if doc_id in doc_map:
                results.append(doc_map[doc_id])
        
        return results
        
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
        'q': '*',
        'filter_by': filter_str,
        'per_page': limit,
        'include_fields': 'document_uuid,document_title,document_url',
        'sort_by': 'authority_score:desc',
    }

    try:
        search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
        response = client.multi_search.perform(search_requests, {})
        result = response['results'][0]
        hits = result.get('hits', [])

        related = []
        for hit in hits:
            doc = hit.get('document', {})
            related.append({
                'title': doc.get('document_title', ''),
                'url': doc.get('document_url', ''),
                'id': doc.get('document_uuid', ''),
            })

        print(f"🔗 Related searches: {len(related)} found for semantic_uuid={semantic_uuid[:12]}...")
        return related

    except Exception as e:
        print(f"❌ fetch_documents_by_semantic_uuid error: {e}")
        return []


def format_result(hit: Dict, query: str = '') -> Dict:
    """Transform Typesense hit into response format."""
    doc = hit.get('document', {})
    highlights = hit.get('highlights', [])
    
    highlight_map = {}
    for h in highlights:
        field = h.get('field')
        snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
        highlight_map[field] = snippet
    
    vector_distance = hit.get('vector_distance')
    semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
    raw_date = doc.get('published_date_string', '')
    formatted_date = ''
    if raw_date:
        try:
            if 'T' in raw_date:
                date_part = raw_date.split('T')[0]
                dt = datetime.strptime(date_part, '%Y-%m-%d')
                formatted_date = dt.strftime('%b %d, %Y')
            elif '-' in raw_date and len(raw_date) >= 10:
                dt = datetime.strptime(raw_date[:10], '%Y-%m-%d')
                formatted_date = dt.strftime('%b %d, %Y')
            else:
                formatted_date = raw_date
        except:
            formatted_date = raw_date
    
    return {
        'id': doc.get('document_uuid'),
        'title': doc.get('document_title', 'Untitled'),
        'image_url': doc.get('image_url') or [],
        'logo_url': doc.get('logo_url') or [],
        'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
        'summary': doc.get('document_summary', ''),
        'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
        'url': doc.get('document_url', ''),
        'source': doc.get('document_brand', 'unknown'),
        'site_name': doc.get('document_brand', 'Website'),
        'image': (doc.get('image_url') or [None])[0],
        'category': doc.get('document_category', ''),
        'data_type': doc.get('document_data_type', ''),
        'schema': doc.get('document_schema', ''),
        'date': formatted_date,
        'published_date': formatted_date,
        'authority_score': doc.get('authority_score', 0),
        'cluster_uuid': doc.get('cluster_uuid'),
        'semantic_uuid': doc.get('semantic_uuid'),
        'key_facts': doc.get('key_facts', []),
        'humanized_summary': '',
        'key_facts_highlighted': highlight_map.get('key_facts', ''),
        'semantic_score': semantic_score,
        'location': {
            'city': doc.get('location_city'),
            'state': doc.get('location_state'),
            'country': doc.get('location_country'),
            'region': doc.get('location_region'),
            'geopoint': doc.get('location_geopoint') or doc.get('location_coordinates'),
            'address': doc.get('location_address'),
            'lat': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[0],
            'lng': (doc.get('location_geopoint') or doc.get('location_coordinates', [None, None]) or [None, None])[1],
        },
        'time_period': {
            'start': doc.get('time_period_start'),
            'end': doc.get('time_period_end'),
            'context': doc.get('time_context')
        },
        'score': 0.5,
        'related_sources': []
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
    
    if query_mode == 'answer':
        return True
    
    if query_mode == 'compare':
        return True
    
    if query_mode == 'explore':
        top_result = results[0]
        top_title = top_result.get('title', '').lower()
        top_facts = ' '.join(top_result.get('key_facts', [])).lower()
        
        stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
                     'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
                     'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
        query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
        
        if not query_words:
            return False
        
        matches = sum(1 for w in query_words if w in top_title or w in top_facts)
        confidence = matches / len(query_words)
        
        return confidence >= 0.75
    
    return False


def _build_ai_overview(signals: Dict, results: List[Dict], query: str) -> Optional[str]:
    """Build the AI Overview text using signal-driven key_fact selection."""
    if not results or not results[0].get('key_facts'):
        return None
    
    question_word = signals.get('question_word')
    
    stopwords = {'who', 'what', 'where', 'when', 'why', 'how', 'is', 'are',
                 'the', 'a', 'an', 'in', 'of', 'for', 'to', 'do', 'does',
                 'can', 'was', 'were', 'be', 'been', 'it', 'its', 'this', 'that'}
    query_words = [w for w in query.lower().split() if w not in stopwords and len(w) > 1]
    
    matched_keyword = ''
    if query_words:
        top_title = results[0].get('title', '').lower()
        top_facts = ' '.join(results[0].get('key_facts', [])).lower()
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
    query_lower = query.lower()
    
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
    Uses session_id + original query so tab clicks and pagination
    always find the same cache. Never depends on derived values
    like corrected_query, query_mode, cities, states.
    """
    normalized = query.strip().lower()
    key_string = f"final|{session_id or 'nosession'}|{normalized}"
    return hashlib.md5(key_string.encode()).hexdigest()


# ============================================================================
# IMAGE COUNTING HELPERS (FIXED — counts documents, not URLs)
# ============================================================================



# ============================================================================
# STAGE 4: Fetch lightweight metadata for SURVIVORS ONLY
# ============================================================================

def fetch_candidate_metadata(survivor_ids: List[str]) -> List[Dict]:
    """
    Stage 4 (Semantic path only): Fetch lightweight metadata for documents
    that survived vector pruning. Documents below the cutoff are never fetched.
    
    Returns list of dicts with: id, data_type, category, schema,
    authority_score, image_url, logo_url — in the same order as input
    (preserving semantic rank order).
    """
    if not survivor_ids:
        return []

    BATCH_SIZE = 250
    doc_map = {}

    for i in range(0, len(survivor_ids), BATCH_SIZE):
        batch_ids = survivor_ids[i:i + BATCH_SIZE]
        id_filter = ','.join([f'`{doc_id}`' for doc_id in batch_ids])

        params = {
            'q': '*',
            'filter_by': f'document_uuid:[{id_filter}]',
            'per_page': len(batch_ids),
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score,image_url,logo_url',
        }

        try:
            search_requests = {'searches': [{'collection': COLLECTION_NAME, **params}]}
            response = client.multi_search.perform(search_requests, {})
            result = response['results'][0]
            hits = result.get('hits', [])

            for hit in hits:
                doc = hit.get('document', {})
                uuid = doc.get('document_uuid')
                if uuid:
                    doc_map[uuid] = {
                        'id': uuid,
                        'data_type': doc.get('document_data_type', ''),
                        'category': doc.get('document_category', ''),
                        'schema': doc.get('document_schema', ''),
                        'authority_score': doc.get('authority_score', 0),
                        'image_url': doc.get('image_url', []),
                        'logo_url': doc.get('logo_url', []),
                    }

        except Exception as e:
            print(f"❌ Stage 4 metadata fetch error (batch {i}): {e}")

    # Return in original order, preserving semantic rank
    results = []
    for uuid in survivor_ids:
        if uuid in doc_map:
            results.append(doc_map[uuid])

    print(f"📊 Stage 4: Fetched metadata for {len(results)}/{len(survivor_ids)} survivors")
    return results


# ============================================================================
# IMAGE COUNTING HELPERS
# ============================================================================

def _has_real_images(item):
    """Check if a candidate has at least one non-empty image or logo URL.
    
    Handles edge cases:
    - image_url might be a string instead of a list
    - Arrays might contain empty strings like ['']
    - Fields might be missing entirely
    
    This is exported for use by views.py image pagination.
    """
    image_urls = item.get('image_url', [])
    if isinstance(image_urls, str):
        image_urls = [image_urls]
    if any(u for u in image_urls if u):
        return True
    logo_urls = item.get('logo_url', [])
    if isinstance(logo_urls, str):
        logo_urls = [logo_urls]
    return any(u for u in logo_urls if u)


def _count_images_from_candidates(all_results):
    """Count DOCUMENTS that have at least one real image or logo URL."""
    return sum(1 for item in all_results if _has_real_images(item))


# ============================================================================
# STAGE 5: ONE count pass — single source of truth
# ============================================================================

def count_all(candidates: List[Dict]) -> Dict:
    """
    Stage 5: Single counting pass. Runs ONCE, after all pruning is done.
    Returns facets, image count, and total.
    This is the ONLY place counting happens — single source of truth.
    """
    facets = count_facets_from_cache(candidates)
    image_count = _count_images_from_candidates(candidates)
    total = len(candidates)

    print(f"📊 Stage 5 (final counts): total={total}, images={image_count}, "
          f"facets={[(f['value'], f['count']) for f in facets.get('data_type', [])]}")

    return {
        'facets': facets,
        'facet_total': total,
        'total_image_count': image_count,
    }
# ============================================================================
# MAIN ENTRY POINT — Clean 7-Stage Pipeline
# ============================================================================
#
# SEMANTIC:  1(uuids) → 2(rerank) → 3(prune) → 4(metadata survivors) → 5(count) → 6(cache) → 7(paginate)
# KEYWORD:   1(uuids+metadata) → 5(count) → 6(cache) → 7(paginate)
#
# ============================================================================

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
    answer: str = None,           # ← NEW
    answer_type: str = None,      # ← NEW
    skip_embedding: bool = False,
    document_uuid: str = None,        # ← NEW
    search_source: str = None
) -> Dict:
    """
    Main entry point for search.
    
    Clean 7-Stage Pipeline:
        SEMANTIC:  1 → 2 → 3 → 4 → 5 → 6 → 7
        KEYWORD:   1 → 5 → 6 → 7
    
    Counting happens ONCE in Stage 5, after all pruning is done.
    Single source of truth for facets, image counts, and totals.
    """
    times = {}
    t0 = time.time()
    print(f"DEBUG execute answer={answer!r} answer_type={answer_type!r}")

    # Extract active filters
    active_data_type = filters.get('data_type') if filters else None
    active_category = filters.get('category') if filters else None
    active_schema = filters.get('schema') if filters else None

    if filters:
        active_filters = {k: v for k, v in filters.items() if v}
        if active_filters:
            print(f"🎛️ Active UI filters: {active_filters}")
    
  
# =========================================================================
# ★ QUESTION DIRECT PATH: bypass all stages, fetch single document
# =========================================================================
    if document_uuid and search_source == 'question':
        print(f"❓ QUESTION PATH: document_uuid={document_uuid} query='{query}'")
        t_fetch = time.time()
        results = fetch_full_documents([document_uuid], query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        # ── AI Overview: always trigger on question path ──
        ai_overview = None
        if results and results[0].get('key_facts'):

            # Derive question_word from query since we skipped intent detection
            question_word = None
            q_lower = query.lower().strip()
            for word in ('who', 'what', 'where', 'when', 'why', 'how'):
                if q_lower.startswith(word):
                    question_word = word
                    break

            question_signals = {
                'query_mode': 'answer',
                'wants_single_result': True,
                'question_word': question_word,
            }

            ai_overview = _build_ai_overview(question_signals, results, query)
            if ai_overview:
                print(f"   💡 AI Overview (question path): {ai_overview[:80]}...")
                results[0]['humanized_summary'] = ai_overview

                # ── Related searches via semantic group ──
        related_searches = []
        if results:
            semantic_uuid = results[0].get('semantic_uuid')
            if semantic_uuid:
                try:
                    related_docs = fetch_documents_by_semantic_uuid(
                        semantic_uuid,
                        exclude_uuid=document_uuid,
                        limit=5
                    )
                    related_searches = [
                        {
                            'query': doc.get('title', ''),
                            'url': doc.get('url', '')
                        }
                        for doc in related_docs
                        if doc.get('title')
                    ]
                except Exception as e:
                    print(f"⚠️ Related searches error: {e}")


        times['total'] = round((time.time() - t0) * 1000, 2)

        return {
            'query': query,
            'corrected_query': query,
            'intent': 'answer',
            'query_mode': 'answer',
            'answer': answer,                        # ← NEW
            'answer_type': answer_type or 'UNKNOWN',
            'results': results,
            'total': len(results),
            'facet_total': len(results),
            'total_image_count': 0,
            'page': 1,
            'per_page': per_page,
            'search_time': round(time.time() - t0, 3),
            'session_id': session_id,
            'semantic_enabled': False,
            'search_strategy': 'question_direct',
            'alt_mode': alt_mode,
            'skip_embedding': True,
            'search_source': 'question',
            'valid_terms': query.split(),
            'unknown_terms': [],
            'data_type_facets': [],
            'category_facets': [],
            'schema_facets': [],
            'related_searches': [],
            'facets': {},
            'related_searches': related_searches,  # was []
            'word_discovery': {
                'valid_count': len(query.split()),
                'unknown_count': 0,
                'corrections': [],
                'filters': [],
                'locations': [],
                'sort': None,
                'total_score': 0,
                'average_score': 0,
                'max_score': 0,
            },
            'timings': times,
            'filters_applied': {
                'data_type': None,
                'category': None,
                'schema': None,
                'is_local_search': False,
                'local_search_strength': 'none',
            },
            'signals': question_signals,  # ← updated from {}
            'profile': {},
        }
# =========================================================================
# ★ FAST PATH: Check for finished cache FIRST
# =========================================================================
    stable_key = _generate_stable_cache_key(session_id, query)
    finished = _get_cached_results(stable_key)

    if finished is not None:
        print(f"⚡ FAST PATH: '{query}' | page={page} | filter={active_data_type}/{active_category}/{active_schema}")

        all_results = finished['all_results']
        all_facets = finished['all_facets']
        facet_total = finished['facet_total']
        ai_overview = finished.get('ai_overview')
        total_image_count = finished.get('total_image_count', 0)
        metadata = finished['metadata']
        times['cache'] = 'hit (fast path)'

        # Filter by UI filters (tab click)
        filtered_results = filter_cached_results(
            all_results,
            data_type=active_data_type,
            category=active_category,
            schema=active_schema
        )

        # Paginate
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        # Stage 7: Fetch full documents for this page only
        t_fetch = time.time()
        page_ids = [item['id'] for item in page_items]
        results = fetch_full_documents(page_ids, query)
        times['fetch_docs'] = round((time.time() - t_fetch) * 1000, 2)

        # Reattach AI overview on page 1
        if results and page == 1 and ai_overview:
            results[0]['humanized_summary'] = ai_overview

        times['total'] = round((time.time() - t0) * 1000, 2)

        print(f"⏱️ FAST PATH TIMING: {times}")
        print(f"🔍 FAST PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)} | Images: {total_image_count}")

        # Build return dict using cached metadata
        signals = metadata.get('signals', {})

        return {
            'query': query,
            'corrected_query': metadata.get('corrected_query', query),
            'intent': metadata.get('intent', 'general'),
            'query_mode': metadata.get('query_mode', 'keyword'),
            'results': results,
            'total': total_filtered,
            'facet_total': facet_total,
            'total_image_count': total_image_count,
            'page': page,
            'per_page': per_page,
            'search_time': round(time.time() - t0, 3),
            'session_id': session_id,
            'semantic_enabled': metadata.get('semantic_enabled', False),
            'search_strategy': metadata.get('search_strategy', 'cached'),
            'alt_mode': alt_mode,
            'skip_embedding': skip_embedding,
            'search_source': search_source,
            'valid_terms': metadata.get('valid_terms', query.split()),
            'unknown_terms': metadata.get('unknown_terms', []),
            'data_type_facets': all_facets.get('data_type', []),
            'category_facets': all_facets.get('category', []),
            'schema_facets': all_facets.get('schema', []),
            'related_searches': [],
            'facets': all_facets,
            'word_discovery': metadata.get('word_discovery', {
                'valid_count': len(query.split()),
                'unknown_count': 0,
                'corrections': [],
                'filters': [],
                'locations': [],
                'sort': None,
                'total_score': 0,
                'average_score': 0,
                'max_score': 0,
            }),
            'timings': times,
            'filters_applied': metadata.get('filters_applied', {
                'data_type': active_data_type,
                'category': active_category,
                'schema': active_schema,
                'is_local_search': False,
                'local_search_strength': 'none',
            }),
            'signals': signals,
            'profile': metadata.get('profile', {}),
        }

# =========================================================================
# ★ FULL PATH: No finished cache. Run the pipeline.
# =========================================================================
    print(f"🔬 FULL PATH: '{query}' (no finished cache for stable_key={stable_key[:12]}...)")

    is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

# =========================================================================
# KEYWORD PATH:  Stage 1 → 5 → 6 → 7
# =========================================================================

    if is_keyword_path:
        print(f"⚡ KEYWORD PIPELINE: '{query}'")

        intent = detect_query_intent(query, pos_tags)

        profile = {
            'search_terms': query.split(),
            'cities': [],
            'states': [],
            'location_terms': [],
            'primary_intent': intent,
            'field_boosts': {
                'primary_keywords': 10,
                'entity_names': 8,
                'semantic_keywords': 6,
                'key_facts': 4,
                'document_title': 3,
            },
        }

        # ── Stage 1: Fetch uuids + metadata in one call (no pruning) ──
        t1 = time.time()
        all_results = fetch_candidates_with_metadata(query, profile)
        times['stage1'] = round((time.time() - t1) * 1000, 2)

        # ── Stage 5: ONE count pass ──
        counts = count_all(all_results)

        # ── Stage 6: Cache the final package ──
        _set_cached_results(stable_key, {
            'all_results': all_results,
            'all_facets': counts['facets'],
            'facet_total': counts['facet_total'],
            'total_image_count': counts['total_image_count'],
            'ai_overview': None,
            'metadata': {
                'corrected_query': query,
                'intent': intent,
                'query_mode': 'keyword',
                'semantic_enabled': False,
                'search_strategy': 'keyword_graph_filter',
                'valid_terms': query.split(),
                'unknown_terms': [],
                'signals': {},
                'city_names': [],
                'state_names': [],
                'profile': profile,
                'word_discovery': {
                    'valid_count': len(query.split()),
                    'unknown_count': 0,
                    'corrections': [],
                    'filters': [],
                    'locations': [],
                    'sort': None,
                    'total_score': 0,
                    'average_score': 0,
                    'max_score': 0,
                },
                'filters_applied': {
                    'data_type': active_data_type,
                    'category': active_category,
                    'schema': active_schema,
                    'is_local_search': False,
                    'local_search_strength': 'none',
                },
            },
        })
        print(f"💾 Cached keyword package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

        # ── Stage 7: Filter → Paginate → Fetch full docs ──
        filtered_results = filter_cached_results(
            all_results,
            data_type=active_data_type,
            category=active_category,
            schema=active_schema
        )
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

        t2 = time.time()
        page_ids = [item['id'] for item in page_items]
        results = fetch_full_documents(page_ids, query)
        times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
        times['total'] = round((time.time() - t0) * 1000, 2)

        print(f"⏱️ KEYWORD TIMING: {times}")

        return {
            'query': query,
            'corrected_query': query,
            'intent': intent,
            'results': results,
            'total': total_filtered,
            'facet_total': counts['facet_total'],
            'total_image_count': counts['total_image_count'],
            'page': page,
            'per_page': per_page,
            'search_time': round(time.time() - t0, 3),
            'session_id': session_id,
            'semantic_enabled': False,
            'search_strategy': 'keyword_graph_filter',
            'alt_mode': alt_mode,
            'skip_embedding': True,
            'search_source': search_source or 'dropdown',
            'valid_terms': query.split(),
            'unknown_terms': [],
            'data_type_facets': counts['facets'].get('data_type', []),
            'category_facets': counts['facets'].get('category', []),
            'schema_facets': counts['facets'].get('schema', []),
            'related_searches': [],
            'facets': counts['facets'],
            'word_discovery': {
                'valid_count': len(query.split()),
                'unknown_count': 0,
                'corrections': [],
                'filters': [],
                'locations': [],
                'sort': None,
                'total_score': 0,
                'average_score': 0,
                'max_score': 0,
            },
            'timings': times,
            'filters_applied': {
                'data_type': active_data_type,
                'category': active_category,
                'schema': active_schema,
                'is_local_search': False,
                'local_search_strength': 'none',
            }
        }

# =========================================================================
# SEMANTIC PATH:  Stage 1 → 2 → 3 → 4 → 5 → 6 → 7
# =========================================================================

    print(f"🔬 SEMANTIC PIPELINE: '{query}'")

    # --- Word discovery + embedding in parallel ---
    t1 = time.time()
    discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)

    # --- Intent detection ---
    signals = {}
    if INTENT_DETECT_AVAILABLE:
        try:
            discovery = detect_intent(discovery)
            signals = discovery.get('signals', {})
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

    corrected_query = discovery.get('corrected_query', query)
    semantic_enabled = query_embedding is not None
    query_mode = signals.get('query_mode', 'explore')

    # --- Build profile ---
    t2 = time.time()
    profile = build_query_profile(discovery, signals=signals)
    times['build_profile'] = round((time.time() - t2) * 1000, 2)

    # --- Apply corrections to search terms ---
    corrections = discovery.get('corrections', [])
    if corrections:
        correction_map = {
            c['original'].lower(): c['corrected']
            for c in corrections
            if c.get('original') and c.get('corrected')
        }
        original_terms = profile.get('search_terms', [])
        profile['search_terms'] = [
            correction_map.get(term.lower(), term)
            for term in original_terms
        ]
        if original_terms != profile['search_terms']:
            print(f"   ✅ Applied corrections to search terms: {original_terms} → {profile['search_terms']}")

    intent = profile.get('primary_intent', 'general')
    city_names = [c['name'] for c in profile.get('cities', [])]
    state_names = [s['name'] for s in profile.get('states', [])]

    print(f"   Intent: {intent} | Mode: {query_mode}")
    print(f"   Cities: {city_names}")
    print(f"   States: {state_names}")
    print(f"   Search Terms: {profile.get('search_terms', [])}")
    print(f"   Field Boosts: {profile.get('field_boosts', {})}")

    # # ── Stage 1: Fetch ONLY document_uuids ──
    # t3 = time.time()
    # candidate_uuids = fetch_candidate_uuids(corrected_query, profile, signals=signals)
    # times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
    # print(f"📊 Stage 1: {len(candidate_uuids)} candidate UUIDs")

    # ── Stage 1: Fetch candidate UUIDs from both collections in parallel ──
    t3 = time.time()

    # If word discovery made unsafe corrections (proper nouns mangled into
    # wrong categories), use the original query for Stage 1A keyword graph.


    # Stage 1B always uses the original embedding so it is already protected.
    UNSAFE_CATEGORIES = {
        'Food', 'US City', 'US State', 'Country', 'Location',
        'City', 'Place', 'Object', 'Animal', 'Color',
    }
    corrections = discovery.get('corrections', [])
    has_unsafe_corrections = any(
        c.get('correction_type') == 'pos_mismatch' or
        c.get('category', '') in UNSAFE_CATEGORIES
        for c in corrections
    )
    search_query_for_stage1 = query if has_unsafe_corrections else corrected_query

    if has_unsafe_corrections:
        print(f"⚠️  Unsafe corrections — using original query for Stage 1A: '{query}'")

    candidate_uuids = fetch_all_candidate_uuids(
        search_query_for_stage1,
        profile,
        query_embedding,
        signals=signals
    )
    times['stage1_uuids'] = round((time.time() - t3) * 1000, 2)
    print(f"📊 Stage 1 COMBINED: {len(candidate_uuids)} candidate UUIDs")

    # ── Stage 2: Vector rerank (only needs IDs + embedding) ──
    survivor_uuids = candidate_uuids  # default if no embedding
    vector_data = {}  # id → {vector_distance, semantic_rank}

    if semantic_enabled and candidate_uuids:
        t4 = time.time()
        reranked = semantic_rerank_candidates(candidate_uuids, query_embedding, max_to_rerank=500)
        times['stage2_rerank'] = round((time.time() - t4) * 1000, 2)

        # Build lookup: id → vector data
        for item in reranked:
            vector_data[item['id']] = {
                'vector_distance': item.get('vector_distance', 1.0),
                'semantic_rank': item.get('semantic_rank', 999999),
            }

        # ── Stage 3: Vector prune — remove IDs below cutoff ──
        DISTANCE_THRESHOLDS = {
            'answer':  0.60,
            'explore': 0.70,
            'compare': 0.65,
            'browse':  0.85,
            'local':   0.85,
            'shop':    0.80,
        }
        threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)

        before_prune = len(candidate_uuids)
        survivor_uuids = [
            uuid for uuid in candidate_uuids
            if vector_data.get(uuid, {}).get('vector_distance', 1.0) <= threshold
        ]
        after_prune = len(survivor_uuids)

        if before_prune != after_prune:
            print(f"   🔪 Stage 3 prune ({query_mode}, threshold={threshold}): {before_prune} → {after_prune} ({before_prune - after_prune} removed)")
        times['stage3_prune'] = f"{before_prune} → {after_prune}"
    else:
        print(f"⚠️ Skipping Stages 2-3: semantic={semantic_enabled}, candidates={len(candidate_uuids)}")

    # ── Stage 4: Fetch metadata for SURVIVORS ONLY ──
    t5 = time.time()
    all_results = fetch_candidate_metadata(survivor_uuids)
    times['stage4_metadata'] = round((time.time() - t5) * 1000, 2)

    # Attach vector data and compute blended scores
    if vector_data:
        total_candidates = len(all_results)
        max_sem_rank = len(vector_data)
        blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()

        if query_mode == 'answer' and signals.get('wants_single_result'):
            blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}

        if signals.get('has_unknown_terms', False):
            shift = min(0.15, blend['text_match'])
            blend['text_match'] -= shift
            blend['semantic'] += shift

        if signals.get('has_superlative', False):
            shift = min(0.10, blend['semantic'])
            blend['semantic'] -= shift
            blend['authority'] += shift

        print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")

        for idx, item in enumerate(all_results):
            item_id = item.get('id')
            vd = vector_data.get(item_id, {})
            item['vector_distance'] = vd.get('vector_distance', 1.0)
            item['semantic_rank'] = vd.get('semantic_rank', 999999)

            authority = item.get('authority_score', 0)
            text_score = 1.0 - (idx / max(total_candidates, 1))
            sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
            auth_score = min(authority / 100.0, 1.0)

            item['blended_score'] = (
                blend['text_match'] * text_score +
                blend['semantic'] * sem_score +
                blend['authority'] * auth_score
            )

        # Sort by blended score
        all_results.sort(key=lambda x: -x.get('blended_score', 0))
        for i, item in enumerate(all_results):
            item['rank'] = i

    # ── Stage 5: ONE count pass ──
    counts = count_all(all_results)

    # --- AI Overview (from page 1 full docs) ---
    ai_overview = None
    if all_results:
        preview_items, _ = paginate_cached_results(all_results, 1, per_page)
        preview_ids = [item['id'] for item in preview_items]
        preview_docs = fetch_full_documents(preview_ids, query)

        if preview_docs and _should_trigger_ai_overview(signals, preview_docs, query):
            ai_overview = _build_ai_overview(signals, preview_docs, query)
            if ai_overview:
                print(f"   💡 AI Overview: {ai_overview[:80]}...")

    # --- Extract terms ---
    valid_terms = profile.get('search_terms', [])
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']

    # ── Stage 6: Cache the final package ──
    _set_cached_results(stable_key, {
        'all_results': all_results,
        'all_facets': counts['facets'],
        'facet_total': counts['facet_total'],
        'total_image_count': counts['total_image_count'],
        'ai_overview': ai_overview,
        'metadata': {
            'corrected_query': corrected_query,
            'intent': intent,
            'query_mode': query_mode,
            'semantic_enabled': semantic_enabled,
            'search_strategy': 'staged_semantic' if semantic_enabled else 'keyword_fallback',
            'valid_terms': valid_terms,
            'unknown_terms': unknown_terms,
            'signals': signals,
            'city_names': city_names,
            'state_names': state_names,
            'profile': profile,
            'word_discovery': {
                'valid_count': discovery.get('stats', {}).get('valid_words', 0),
                'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
                'corrections': discovery.get('corrections', []),
                'filters': [],
                'locations': [
                    {'field': 'location_city', 'values': city_names},
                    {'field': 'location_state', 'values': state_names},
                ] if city_names or state_names else [],
                'sort': None,
                'total_score': 0,
                'average_score': 0,
                'max_score': 0,
            },
            'filters_applied': {
                'data_type': active_data_type,
                'category': active_category,
                'schema': active_schema,
                'is_local_search': signals.get('is_local_search', False),
                'local_search_strength': signals.get('local_search_strength', 'none'),
                'has_black_owned': signals.get('has_black_owned', False),
                'graph_filters': [],
                'graph_locations': [
                    {'field': 'location_city', 'values': city_names},
                    {'field': 'location_state', 'values': state_names},
                ] if city_names or state_names else [],
                'graph_sort': None,
            },
        },
    })
    print(f"💾 Cached semantic package: {counts['facet_total']} results, {counts['total_image_count']} image docs")

    # ── Stage 7: Filter → Paginate → Fetch full docs ──
    filtered_results = filter_cached_results(
        all_results,
        data_type=active_data_type,
        category=active_category,
        schema=active_schema
    )

    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)

    t6 = time.time()
    page_ids = [item['id'] for item in page_items]
    results = fetch_full_documents(page_ids, query)
    times['fetch_docs'] = round((time.time() - t6) * 1000, 2)

    # Attach AI overview on page 1
    if results and page == 1 and ai_overview:
        results[0]['humanized_summary'] = ai_overview

    # Store query embedding
    if query_embedding:
        try:
            store_query_embedding(corrected_query, query_embedding, result_count=counts['facet_total'])
        except Exception as e:
            print(f"⚠️ store_query_embedding error: {e}")

    times['total'] = round((time.time() - t0) * 1000, 2)

    strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'

    print(f"⏱️ SEMANTIC TIMING: {times}")
    print(f"🔍 {strategy.upper()} ({query_mode}) | Total: {counts['facet_total']} | Filtered: {total_filtered} | Page: {len(results)} | Images: {counts['total_image_count']}")

    return {
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
        'query_mode': query_mode,
        'results': results,
        'total': total_filtered,
        'facet_total': counts['facet_total'],
        'total_image_count': counts['total_image_count'],
        'page': page,
        'per_page': per_page,
        'search_time': round(time.time() - t0, 3),
        'session_id': session_id,
        'semantic_enabled': semantic_enabled,
        'search_strategy': strategy,
        'alt_mode': alt_mode,
        'skip_embedding': skip_embedding,
        'search_source': search_source,
        'valid_terms': valid_terms,
        'unknown_terms': unknown_terms,
        'related_searches': [],
        'data_type_facets': counts['facets'].get('data_type', []),
        'category_facets': counts['facets'].get('category', []),
        'schema_facets': counts['facets'].get('schema', []),
        'facets': counts['facets'],
        'word_discovery': {
            'valid_count': discovery.get('stats', {}).get('valid_words', 0),
            'unknown_count': discovery.get('stats', {}).get('unknown_words', 0),
            'corrections': discovery.get('corrections', []),
            'filters': [],
            'locations': [
                {'field': 'location_city', 'values': city_names},
                {'field': 'location_state', 'values': state_names},
            ] if city_names or state_names else [],
            'sort': None,
            'total_score': 0,
            'average_score': 0,
            'max_score': 0,
        },
        'timings': times,
        'filters_applied': {
            'data_type': active_data_type,
            'category': active_category,
            'schema': active_schema,
            'is_local_search': signals.get('is_local_search', False),
            'local_search_strength': signals.get('local_search_strength', 'none'),
            'has_black_owned': signals.get('has_black_owned', False),
            'graph_filters': [],
            'graph_locations': [
                {'field': 'location_city', 'values': city_names},
                {'field': 'location_state', 'values': state_names},
            ] if city_names or state_names else [],
            'graph_sort': None,
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
            'type': 'featured_snippet',
            'title': top.get('title'),
            'snippet': top.get('summary', ''),
            'key_facts': top.get('key_facts', [])[:3],
            'source': top.get('source'),
            'url': top.get('url'),
            'image': top.get('image')
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
        'q': query,
        'query_by': query_by,
        'per_page': per_page,
        'page': page,
    }
    
    if filter_by:
        params['filter_by'] = filter_by
    if sort_by:
        params['sort_by'] = sort_by
    if facet_by:
        params['facet_by'] = facet_by
        params['max_facet_values'] = max_facet_values
    
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
        alt_mode='y',  # Semantic path
    )
    
    print("\n" + "=" * 70)
    print("📊 RESULTS")
    print("=" * 70)
    print(f"Query: {result['query']}")
    print(f"Corrected: {result['corrected_query']}")
    print(f"Intent: {result['intent']}")
    print(f"Query Mode: {result.get('query_mode', 'N/A')}")
    print(f"Total: {result['total']}")
    print(f"Facet Total: {result['facet_total']}")
    print(f"Total Image Count: {result['total_image_count']}")
    print(f"Strategy: {result['search_strategy']}")
    print(f"Semantic: {result['semantic_enabled']}")
    
    print(f"\n🔧 Corrections:")
    for c in result.get('word_discovery', {}).get('corrections', []):
        print(f"   '{c['original']}' → '{c['corrected']}' (type: {c.get('correction_type', 'unknown')})")

    print(f"\n🔄 Query Flow:")
    print(f"   Original:  '{result['query']}'")
    print(f"   Corrected: '{result['corrected_query']}'")
    print(f"   Changed:   {result['query'] != result['corrected_query']}")
        
    print(f"\n📝 Terms:")
    print(f"   Valid: {result['valid_terms']}")
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
        print(f"   query_mode: {sigs.get('query_mode')}")
        print(f"   question_word: {sigs.get('question_word')}")
        print(f"   wants_single: {sigs.get('wants_single_result')}")
        print(f"   wants_multiple: {sigs.get('wants_multiple_results')}")
        print(f"   is_local: {sigs.get('is_local_search')}")
        print(f"   has_black_owned: {sigs.get('has_black_owned')}")
        print(f"   temporal: {sigs.get('temporal_direction')}")
        print(f"   has_unknown: {sigs.get('has_unknown_terms')}")
    
    print(f"\n📄 Results ({len(result['results'])}):")
    for i, r in enumerate(result['results'][:5], 1):
        print(f"   {i}. {r['title'][:60]}")
        if r.get('humanized_summary'):
            print(f"      💡 {r['humanized_summary'][:80]}...")
        print(f"      📍 {r['location'].get('city', '')}, {r['location'].get('state', '')}")
        print(f"      🔗 {r['url'][:50]}...")
    
    print(f"\n⏱️ Timings: {result['timings']}")