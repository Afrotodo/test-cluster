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

COLLECTION_NAME = 'documents'


# ============================================================================
# RESULT CACHE (Self-Contained)
# ============================================================================

_result_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 300  # 5 minutes
MAX_CACHED_RESULTS = 2000
MAX_CACHE_ENTRIES = 100


def _generate_cache_key(query: str, mode: str, cities: List = None, states: List = None) -> str:
    """Generate unique cache key for a search."""
    key_parts = [
        query.lower().strip(),
        mode,
        json.dumps(cities or [], sort_keys=True),
        json.dumps(states or [], sort_keys=True),
    ]
    key_string = '|'.join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()


def _get_cached_results(cache_key: str) -> Optional[List[Dict]]:
    """Get cached result set if not expired."""
    with _cache_lock:
        if cache_key in _result_cache:
            entry = _result_cache[cache_key]
            age = (datetime.now() - entry['timestamp']).total_seconds()
            if age < CACHE_TTL_SECONDS:
                return entry['data']
            else:
                del _result_cache[cache_key]
    return None


def _set_cached_results(cache_key: str, data: List[Dict]):
    """Cache result set with timestamp."""
    with _cache_lock:
        if len(_result_cache) >= MAX_CACHE_ENTRIES:
            oldest_key = min(_result_cache.keys(),
                           key=lambda k: _result_cache[k]['timestamp'])
            del _result_cache[oldest_key]
        
        _result_cache[cache_key] = {
            'timestamp': datetime.now(),
            'data': data
        }


def clear_search_cache():
    """Clear all cached search results."""
    global _result_cache
    with _cache_lock:
        _result_cache = {}
    print("🧹 Search cache cleared")


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


def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
    """Run word discovery and embedding IN PARALLEL."""
    if skip_embedding:
        discovery = _run_word_discovery(query)
        return discovery, None
    
    discovery_future = _executor.submit(_run_word_discovery, query)
    embedding_future = _executor.submit(_run_embedding, query)
    
    discovery = discovery_future.result()
    embedding = embedding_future.result()
    
    # Re-embed if query was corrected
    corrected_query = discovery.get('corrected_query', query)
    if corrected_query.lower() != query.lower() and embedding is not None:
        corrections = discovery.get('corrections', [])
        significant = any(
            c.get('original', '').lower() != c.get('corrected', '').lower()
            for c in corrections
        )
        if significant:
            embedding = get_query_embedding(corrected_query)
    
    return discovery, embedding


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
        # Cities/states → filter_by (strip from q)
        # Continents/countries/regions → keep in search_terms (not filterable)
        # Answer mode "where is X" → location IS the subject, keep in search
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
                # City/state → filter, strip from search
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
                # Continent/country/region OR location-as-subject → SEARCH it
                # These aren't filterable fields, so they go into q
                if is_noun:
                    profile['search_terms'].append(word)
                profile['location_terms'].append(word)
                continue
        
        # ─── Blueprint Step 1: Only nouns go into search terms ───────
        # Verbs, adjectives, prepositions, wh-pronouns are SIGNALS not SEARCH
        
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
            # Unknown/other category — only add if it's a noun
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
    # Mode override FIRST, then additive boosts from term categories
    # =================================================================
    
    boosts = _compute_field_boosts(profile, query_mode, signals)
    profile['field_boosts'] = boosts
    
    return profile


def _compute_field_boosts(profile: Dict, query_mode: str, signals: Dict = None) -> Dict:
    """
    Blueprint Step 4: Dynamic field weight computation.
    
    1. Start with base weights
    2. Apply query mode overrides
    3. Add category-based boosts from detected terms
    """
    signals = signals or {}
    
    # ─── Base weights ────────────────────────────────────────────────
    boosts = {
        'document_title': 10,
        'entity_names': 2,
        'primary_keywords': 3,
        'key_facts': 3,
        'semantic_keywords': 2,
    }
    
    # ─── Query mode overrides (applied BEFORE additive boosts) ───────
    if query_mode == 'answer':
        boosts['document_title'] = 20
        boosts['entity_names'] = 15
        # For single-result answers, restrict to title-focused fields only
        # Skip key_facts and semantic_keywords — they're noisy and match too broadly
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
        # service_type and service_specialties added to query_by in params builder
    elif query_mode == 'compare':
        boosts['entity_names'] = 15
        boosts['document_title'] = 15
    elif query_mode == 'shop':
        boosts['primary_keywords'] = 12
        boosts['document_title'] = 10
    # explore uses base weights
    
    # ─── Additive boosts from term categories ────────────────────────
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
    
    # ─── Unknown term handling (Blueprint Step 4) ────────────────────
    has_unknown = signals.get('has_unknown_terms', False)
    has_known = (profile.get('has_person') or profile.get('has_organization')
                 or profile.get('has_keyword') or profile.get('has_media'))
    
    if has_unknown and has_known:
        # Mixed: boost all fields +3
        for field in boosts:
            boosts[field] += 3
    elif has_unknown and not has_known:
        # All unknown: widest net
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
    
    Blueprint alignment:
    - Step 2: Exact match enforcement (num_typos, prefix)
    - Step 3: Query mode → strictness params
    - Step 6: Sort order from signals
    - Step 7: Location as filter_by (already stripped from q in profile)
    
    Returns:
        Dict with q, query_by, query_by_weights, filter_by, num_typos,
        prefix, drop_tokens_threshold, sort_by
    """
    signals = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    
    params = {}
    
    # ─── Build query string (POS-filtered in profile) ────────────────
    search_terms = profile.get('search_terms', [])
    seen = set()
    unique_terms = []
    for term in search_terms:
        term_lower = term.lower()
        if term_lower not in seen:
            seen.add(term_lower)
            unique_terms.append(term)
    
    params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
    # ─── Build query_by and weights ──────────────────────────────────
    field_boosts = profile.get('field_boosts', {})
    
    # Add service fields for local mode (Blueprint Step 3)
    if query_mode == 'local':
        if 'service_type' not in field_boosts:
            field_boosts['service_type'] = 12
        if 'service_specialties' not in field_boosts:
            field_boosts['service_specialties'] = 10
    
    sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    params['query_by'] = ','.join([f[0] for f in sorted_fields])
    params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
    # ─── Strictness params per mode (Blueprint Steps 2 & 3) ─────────
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
    
    # ─── Sort order (Blueprint Step 6) ───────────────────────────────
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
    
    # ─── Build filter string ─────────────────────────────────────────
    filter_conditions = []
    
    # Location filters (Blueprint Step 7 — locations are FILTERS not search text)
    cities = profile.get('cities', [])
    states = profile.get('states', [])
    
    # Determine location filter strictness (Blueprint Step 7)
    local_strength = signals.get('local_search_strength', 'none')
    is_location_subject = (
        query_mode == 'answer' and
        signals.get('has_question_word') and
        signals.get('question_word') in ('where',) and
        signals.get('has_location_entity', False)
    )
    
    # For answer mode where location IS the subject (e.g., "where is Atlanta"),
    # do NOT filter by location — search for it instead
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
    
    # Black-owned filter (Blueprint: add filter_by: black_owned:=true)
    if signals.get('has_black_owned', False):
        filter_conditions.append('black_owned:=true')
    
    # UI filters
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
    
    # Check if location is the subject (don't filter)
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
    
    # Black-owned filter
    if signals.get('has_black_owned', False):
        filter_conditions.append('black_owned:=true')
    
    return ' && '.join(filter_conditions) if filter_conditions else ''


# ============================================================================
# STAGE 1: GRAPH FILTER - Candidate Generation
# ============================================================================

def fetch_candidate_ids(
    search_query: str,
    profile: Dict,
    signals: Dict = None,
    max_results: int = MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    Stage 1: Graph Filter - Candidate Generation
    
    Uses keyword fields for FILTERING (fast inverted index lookup).
    Does NOT filter by data_type so we get ALL types for tab counts.
    Now signal-aware: uses mode-specific params.
    """
    signals = signals or {}
    params = build_typesense_params(profile, signals=signals)
    
    # Override filter to exclude data_type (for accurate facet counts)
    filter_str = build_filter_string_without_data_type(profile, signals=signals)
    
    PAGE_SIZE = 250
    all_results = []
    current_page = 1
    max_pages = (max_results // PAGE_SIZE) + 1
    
    query_mode = signals.get('query_mode', 'explore')
    
    print(f"🔍 Stage 1 Query: '{params.get('q', search_query)}'")
    print(f"   Mode: {query_mode}")
    print(f"   Fields: {params.get('query_by', '')}")
    print(f"   Weights: {params.get('query_by_weights', '')}")
    print(f"   num_typos: {params.get('num_typos', 1)} | prefix: {params.get('prefix', 'yes')}")
    print(f"   sort_by: {params.get('sort_by', 'default')}")
    if filter_str:
        print(f"   Filters: {filter_str}")
    
    while len(all_results) < max_results and current_page <= max_pages:
        search_params = {
            'q': params.get('q', search_query),
            'query_by': params.get('query_by', 'document_title,primary_keywords,entity_names,key_facts,semantic_keywords'),
            'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
            'per_page': PAGE_SIZE,
            'page': current_page,
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score',
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
                    'service_phone': doc.get('service_phone'),
                    'text_match': hit.get('text_match', 0),
                })
            
            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break
            
            current_page += 1
            
        except Exception as e:
            print(f"❌ Stage 1 error (page {current_page}): {e}")
            break
    
    print(f"📊 Stage 1: Retrieved {len(all_results)} candidates")
    return all_results[:max_results]


# ============================================================================
# STAGE 2: SEMANTIC RERANK - Vector-Based Ranking (Blueprint Step 5)
# ============================================================================

def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 250
) -> List[Dict]:
    """
    Stage 2: Semantic Rerank - Pure Vector Ranking
    
    Takes candidate IDs and reranks by vector similarity ONLY.
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
        
        # Add any missing IDs
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
    
    Blueprint Step 5: Blend ratio between text_match, semantic, and authority
    depends on query_mode AND whether unknown terms are present.
    """
    if not reranked_results:
        return cached_results
    
    signals = signals or {}
    query_mode = signals.get('query_mode', 'explore')
    
    # Get blend ratios for this mode
    blend = BLEND_RATIOS.get(query_mode, BLEND_RATIOS['explore']).copy()
    
    # Single-result answer mode: text_match dominance
    # For factual questions, authority of the source doesn't determine correctness
    if query_mode == 'answer' and signals.get('wants_single_result'):
        blend = {'text_match': 0.70, 'semantic': 0.30, 'authority': 0.00}
    
    # Unknown term adjustment: shift +0.15 from text_match to semantic
    if signals.get('has_unknown_terms', False):
        shift = min(0.15, blend['text_match'])
        blend['text_match'] -= shift
        blend['semantic'] += shift
        print(f"   📊 Unknown term shift: text_match={blend['text_match']:.2f}, semantic={blend['semantic']:.2f}")
    
    # Superlative adjustment: shift +0.10 from semantic to authority
    if signals.get('has_superlative', False):
        shift = min(0.10, blend['semantic'])
        blend['semantic'] -= shift
        blend['authority'] += shift
        print(f"   📊 Superlative shift: semantic={blend['semantic']:.2f}, authority={blend['authority']:.2f}")
    
    print(f"   📊 Blend ratios ({query_mode}): text={blend['text_match']:.2f} sem={blend['semantic']:.2f} auth={blend['authority']:.2f}")
    
    # Find best distance for relative cutoff
    best_distance = min(
        (r.get('vector_distance', 1.0) for r in reranked_results if r.get('vector_distance', 1.0) < 1.0),
        default=1.0
    )
    cutoff = min(best_distance * 2.0, 0.85)
    
    print(f"   🎯 Semantic cutoff: best={best_distance:.3f}, cutoff={cutoff:.3f}")
    
    # Build lookup
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
        
        # Compute blended score
        # text_score: based on POSITION in Stage 1 results (idx), not raw Typesense score
        # Position 0 = best = score 1.0, last position = score 0.0
        text_score = 1.0 - (idx / max(total_candidates, 1))
        sem_score = 1.0 - (item['semantic_rank'] / max(max_sem_rank, 1)) if item['semantic_rank'] < 999999 else 0.0
        auth_score = min(authority / 100.0, 1.0)
        
        item['blended_score'] = (
            blend['text_match'] * text_score +
            blend['semantic'] * sem_score +
            blend['authority'] * auth_score
        )
        
        # Apply cutoff — demote documents above threshold
        if item['vector_distance'] > cutoff:
            item['blended_score'] -= 1.0
    
    # Sort by blended score (highest first)
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
        
        # Return in original order
        results = []
        for doc_id in document_ids:
            if doc_id in doc_map:
                results.append(doc_map[doc_id])
        
        return results
        
    except Exception as e:
        print(f"❌ fetch_full_documents error: {e}")
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
    
    # Format date
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
    """
    Blueprint Step 8: Determine if AI Overview should trigger.
    
    | Query Mode | Trigger Condition |
    |------------|-------------------|
    | answer     | ALWAYS if top result title matches primary noun |
    | explore    | If top result has ≥75% query word match confidence |
    | browse     | DO NOT trigger |
    | local      | DO NOT trigger |
    | shop       | DO NOT trigger |
    | compare    | Trigger with facts from both items |
    """
    if not results:
        return False
    
    query_mode = signals.get('query_mode', 'explore')
    
    if query_mode in ('browse', 'local', 'shop'):
        return False
    
    if query_mode == 'answer':
        # Always trigger if top result title matches
        return True
    
    if query_mode == 'compare':
        return True
    
    if query_mode == 'explore':
        # Check ≥75% word match confidence
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
    """
    Build the AI Overview text using signal-driven key_fact selection.
    Blueprint Step 8.
    """
    if not results or not results[0].get('key_facts'):
        return None
    
    question_word = signals.get('question_word')
    
    # Get meaningful words from query for keyword matching
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
# MAIN ENTRY POINT
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
    skip_embedding: bool = False,
    search_source: str = None
) -> Dict:
    """
    Main entry point for search.
    
    Returns same structure as old execute_full_search() for views.py compatibility.
    
    alt_mode:
        'n' = KEYWORD PATH - no semantic reranking
        'y' = SEMANTIC PATH - full staged retrieval with signal-driven behavior
    """
    times = {}
    t0 = time.time()
    
    # Extract active filters
    active_data_type = filters.get('data_type') if filters else None
    active_category = filters.get('category') if filters else None
    active_schema = filters.get('schema') if filters else None
    
    if filters:
        active_filters = {k: v for k, v in filters.items() if v}
        if active_filters:
            print(f"🎛️ Active UI filters: {active_filters}")
    
    # =========================================================================
    # KEYWORD PATH (alt_mode='n')
    # =========================================================================
    
    is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
    if is_keyword_path:
        print(f"⚡ KEYWORD PATH: '{query}'")
        
        intent = detect_query_intent(query, pos_tags)
        
        # Simple profile for keyword path
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
        
        cache_key = _generate_cache_key(query, 'keyword', [], [])
        cached_data = _get_cached_results(cache_key)
        
        if cached_data:
            print(f"✅ Cache HIT: {len(cached_data)} candidates")
            all_results = cached_data
            times['cache'] = 'hit'
        else:
            print(f"❌ Cache MISS: Running Stage 1...")
            t1 = time.time()
            all_results = fetch_candidate_ids(query, profile)
            times['stage1'] = round((time.time() - t1) * 1000, 2)
            
            if all_results:
                _set_cached_results(cache_key, all_results)
            times['cache'] = 'miss'
        
        # Facets from cache
        all_facets = count_facets_from_cache(all_results)
        facet_total = len(all_results)
        
        # Filter by UI filters
        filtered_results = filter_cached_results(
            all_results,
            data_type=active_data_type,
            category=active_category,
            schema=active_schema
        )
        
        # Paginate
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
        
        # Fetch full documents
        t2 = time.time()
        page_ids = [item['id'] for item in page_items]
        results = fetch_full_documents(page_ids, query)
        times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
        times['total'] = round((time.time() - t0) * 1000, 2)
        
        print(f"⏱️ TIMING: {times}")
        print(f"🔍 KEYWORD PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
        
        return {
            'query': query,
            'corrected_query': query,
            'intent': intent,
            'results': results,
            'total': total_filtered,
            'facet_total': facet_total,
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
            'data_type_facets': all_facets.get('data_type', []),
            'category_facets': all_facets.get('category', []),
            'schema_facets': all_facets.get('schema', []),
            'related_searches': [],
            'facets': all_facets,
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
    # SEMANTIC PATH (alt_mode='y')
    # =========================================================================
    
    print(f"🔬 SEMANTIC PATH: '{query}'")
    
    # Run word discovery and embedding in parallel
    t1 = time.time()
    discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
    # Run intent detection
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
    
    # Build profile from discovery (now signal-aware)
    t2 = time.time()
    profile = build_query_profile(discovery, signals=signals)
    times['build_profile'] = round((time.time() - t2) * 1000, 2)
    
    # =========================================================================
    # Apply corrections to search terms
    # =========================================================================
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
    
    print(f"   Intent: {intent} | Mode: {query_mode}")
    print(f"   Cities: {[c['name'] for c in profile.get('cities', [])]}")
    print(f"   States: {[s['name'] for s in profile.get('states', [])]}")
    print(f"   Search Terms (POS-filtered): {profile.get('search_terms', [])}")
    print(f"   Location Terms (stripped): {profile.get('location_terms', [])}")
    print(f"   Field Boosts: {profile.get('field_boosts', {})}")
    
    # Generate cache key
    city_names = [c['name'] for c in profile.get('cities', [])]
    state_names = [s['name'] for s in profile.get('states', [])]
    cache_key = _generate_cache_key(corrected_query, query_mode, city_names, state_names)
    
    # Check cache
    cached_data = _get_cached_results(cache_key)
    
    if cached_data:
        print(f"✅ Cache HIT: {len(cached_data)} candidates")
        all_results = cached_data
        times['cache'] = 'hit'
    else:
        print(f"❌ Cache MISS: Running Stage 1...")
        t3 = time.time()
        all_results = fetch_candidate_ids(corrected_query, profile, signals=signals)
        times['stage1'] = round((time.time() - t3) * 1000, 2)
        
        if all_results:
            _set_cached_results(cache_key, all_results)
        times['cache'] = 'miss'
    
    # Facets from cache
    all_facets = count_facets_from_cache(all_results)
    facet_total = len(all_results)
    
    print(f"📊 Facets: {[(f['value'], f['count']) for f in all_facets.get('data_type', [])]}")
    
    # Filter by UI filters
    filtered_results = filter_cached_results(
        all_results,
        data_type=active_data_type,
        category=active_category,
        schema=active_schema
    )
    
    # Stage 2: Semantic Rerank (with mode-specific blend ratios)
    if semantic_enabled and filtered_results:
        t4 = time.time()
        candidate_ids = [item['id'] for item in filtered_results]
        reranked = semantic_rerank_candidates(candidate_ids, query_embedding, max_to_rerank=500)
        filtered_results = apply_semantic_ranking(filtered_results, reranked, signals=signals)
        times['stage2'] = round((time.time() - t4) * 1000, 2)
        
        # ─── Vector distance hard filter ─────────────────────────────
        # Remove results that are too far from the query embedding.
        # Thresholds vary by mode: answer is strictest, browse is most lenient.
        DISTANCE_THRESHOLDS = {
            'answer':  0.60,
            'explore': 0.70,
            'compare': 0.65,
            'browse':  0.85,
            'local':   0.85,
            'shop':    0.80,
        }
        threshold = DISTANCE_THRESHOLDS.get(query_mode, 0.75)
        
        before_filter = len(filtered_results)
        filtered_results = [
            r for r in filtered_results
            if r.get('vector_distance', 1.0) <= threshold
        ]
        after_filter = len(filtered_results)
        
        if before_filter != after_filter:
            print(f"   🔪 Vector filter ({query_mode}, threshold={threshold}): {before_filter} → {after_filter} ({before_filter - after_filter} removed)")
        
        # Update total for display
        total_filtered = len(filtered_results)
    else:
        print(f"⚠️ Skipping Stage 2: semantic={semantic_enabled}, filtered={len(filtered_results)}")
    
    # Paginate
    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
    # Fetch full documents
    t5 = time.time()
    page_ids = [item['id'] for item in page_items]
    results = fetch_full_documents(page_ids, query)
    times['fetch_docs'] = round((time.time() - t5) * 1000, 2)
    
    # ─── AI Overview (Blueprint Step 8 — signal-driven) ─────────────
    if results and page == 1:
        if _should_trigger_ai_overview(signals, results, query):
            overview = _build_ai_overview(signals, results, query)
            if overview:
                results[0]['humanized_summary'] = overview
    
    # Store query embedding for popular queries
    if query_embedding:
        try:
            store_query_embedding(corrected_query, query_embedding, result_count=facet_total)
        except Exception as e:
            print(f"⚠️ store_query_embedding error: {e}")
    
    times['total'] = round((time.time() - t0) * 1000, 2)
    
    strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
    print(f"⏱️ TIMING: {times}")
    print(f"🔍 {strategy.upper()} ({query_mode}) | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
    # Extract valid/unknown terms
    valid_terms = profile.get('search_terms', [])
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
    return {
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
        'query_mode': query_mode,
        'results': results,
        'total': total_filtered,
        'facet_total': facet_total,
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
        'data_type_facets': all_facets.get('data_type', []),
        'category_facets': all_facets.get('category', []),
        'schema_facets': all_facets.get('schema', []),
        'facets': all_facets,
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