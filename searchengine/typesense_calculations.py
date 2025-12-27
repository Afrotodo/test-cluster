"""
typesense_calculations.py

Handles all Typesense search logic:
- Query building
- Intent detection
- Weighting & ranking
- Result processing
"""


import typesense
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime

import re

# === PRE-COMPILED REGEX PATTERNS ===
# Compiled once at module load, reused on every search

# Location patterns
LOCATION_PATTERNS = [
    re.compile(r'\b(in|near|around|at)\s+\w+'),
    re.compile(r'\b(city|state|country|region)\b'),
    re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
]

# Historical patterns
HISTORICAL_PATTERNS = [
    re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
    re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
    re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
    re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
]

# Product patterns
PRODUCT_PATTERNS = [
    re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
    re.compile(r'\b(product|item|purchase|order|shipping)\b'),
    re.compile(r'\$[0-9]+'),
]

# Person patterns
PERSON_PATTERNS = [
    re.compile(r'\b(who is|biography|born|died|life of)\b'),
    re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
]

# Media patterns
MEDIA_PATTERNS = [
    re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
    re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
]

# Extraction patterns
LOCATION_EXTRACT_PATTERNS = [
    re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
    re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
]

DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# HELPER FUNCTIONS PART 0

# Add these to your typesense_calculations.py

def get_facets(query: str) -> dict:
    """
    Returns available filter options based on result set.
    
    Why: Shows user what filters are available (like "Videos (23)", "Articles (156)")
    """
    search_params = {
        'q': query,
        'query_by': ','.join(SEARCH_FIELDS),
        'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
        'max_facet_values': 10,
        'per_page': 0  # We only want facets, not results
    }
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(search_params)
        facets = {}
        
        for facet in response.get('facet_counts', []):
            field = facet['field_name']
            facets[field] = [
                {'value': count['value'], 'count': count['count']}
                for count in facet['counts']
            ]
        
        return facets
    except:
        return {}


def get_related_searches(query: str, intent: str) -> list:
    """
    Returns "People also search for" suggestions.
    
    Why: Helps users explore related topics, increases engagement.
    """
    # Option 1: Query-based suggestions from your keyword data
    # Option 2: Use semantic similarity
    # Option 3: Use search logs (what did others search after this?)
    
    # Simple implementation: search for documents with similar keywords
    # and extract their primary_keywords
    
    search_params = {
        'q': query,
        'query_by': 'primary_keywords,keywords',
        'per_page': 10,
        'include_fields': 'primary_keywords,keywords'
    }
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
        # Collect all keywords from results
        all_keywords = set()
        query_words = set(query.lower().split())
        
        for hit in response.get('hits', []):
            doc = hit.get('document', {})
            for kw in doc.get('primary_keywords', []):
                if kw.lower() not in query_words:
                    all_keywords.add(kw)
            for kw in doc.get('keywords', [])[:5]:
                if kw.lower() not in query_words:
                    all_keywords.add(kw)
        
        # Return top 6 as related searches
        related = list(all_keywords)[:6]
        return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
    except:
        return []


def get_featured_result(query: str, intent: str, results: list) -> dict:
    """
    Returns featured content: knowledge panel, direct answer, or top result highlight.
    
    Why: Like Google's featured snippets - gives quick answer without clicking.
    """
    if not results:
        return None
    
    top_result = results[0]
    
    # High authority + high relevance = feature it
    if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
        # Person intent: show person card
        if intent == 'person' and top_result.get('data_type') == 'person':
            return {
                'type': 'person_card',
                'data': top_result
            }
        
        # Location intent: show place card
        if intent == 'location':
            return {
                'type': 'place_card',
                'data': top_result
            }
        
        # General: show featured snippet
        return {
            'type': 'featured_snippet',
            'title': top_result.get('title'),
            'snippet': top_result.get('summary', ''),
            'source': top_result.get('source'),
            'url': top_result.get('url'),
            'image': top_result.get('image')
        }
    
    return None


def log_search_event(
    query: str,
    corrected_query: str,
    session_id: str,
    intent: str,
    total_results: int,
    filters: dict,
    page: int
):
    """
    Logs search event for analytics and personalization.
    
    Why: Track what users search, improve results over time, personalization.
    """
    # Implement based on your logging system
    # Options: database, Redis, analytics service, log file
    
    event = {
        'timestamp': datetime.now().isoformat(),
        'session_id': session_id,
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
        'total_results': total_results,
        'filters': filters,
        'page': page,
        'zero_results': total_results == 0
    }
    
    # Example: save to database or send to analytics
    # SearchLog.objects.create(**event)
    # analytics.track('search', event)
    
    pass  # Replace with your implementation



# === CLIENT SETUP  PART 1  ===
client = typesense.Client({
    'api_key': 'm07ecySfbgRSS6gF6p45K4jliMMwVoE7',
    'nodes': [{
        'host': 'gb890veru46kj7yfp-1.a1.typesense.net',
        'port': '443',
        'protocol': 'https'
    }],
    'connection_timeout_seconds': 5
})

COLLECTION_NAME = 'documents'


# === FIELD CONFIGURATION  PART 2 ===
# These define which fields to search and their relative importance

SEARCH_FIELDS = [
    'document_title',
    'primary_keywords', 
    'keywords',
    'semantic_keywords',
    'document_summary',
    'key_passages',
    'entity_names'
]

# Default weights when no specific intent detected
DEFAULT_WEIGHTS = [5, 4, 3, 2, 2, 1, 2]

# Intent-specific weight profiles
INTENT_WEIGHTS = {
    'general': [5, 4, 3, 2, 2, 1, 2],
    'location': [4, 3, 3, 2, 2, 1, 4],      # boost entity_names for places
    'historical': [4, 4, 4, 3, 2, 1, 3],    # boost keywords for era terms
    'product': [5, 4, 3, 2, 2, 1, 2],       # title-heavy for products
    'person': [5, 3, 3, 2, 2, 1, 5],        # boost entity_names for people
    'media': [5, 4, 3, 3, 2, 1, 3],         # boost semantic for genre matching
}

# Authority scores by source (used for boosting)
SOURCE_AUTHORITY = {
    'britannica': 95,
    'wikipedia': 85,
    'government': 90,
    'academic': 88,
    'news': 70,
    'blog': 50,
    'social': 40,
    'default': 60
}


#  === INTENT DETECTION PART 3 ===
# Determines what type of search the user wants

def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
    """
    Analyzes query to determine user intent.
    Uses pre-compiled patterns for performance.
    """
    query_lower = query.lower()
    
    # Location check
    for pattern in LOCATION_PATTERNS:
        if pattern.search(query_lower):
            return 'location'
    
    # Historical check
    for pattern in HISTORICAL_PATTERNS:
        if pattern.search(query_lower):
            return 'historical'
    
    # Product check
    for pattern in PRODUCT_PATTERNS:
        if pattern.search(query_lower):
            return 'product'
    
    # Person check
    for pattern in PERSON_PATTERNS:
        if pattern.search(query_lower):
            return 'person'
    
    # Media check
    for pattern in MEDIA_PATTERNS:
        if pattern.search(query_lower):
            return 'media'
    
    return 'general'


def extract_location_from_query(query: str) -> Optional[str]:
    """Uses pre-compiled patterns."""
    stopwords = {'the', 'a', 'best', 'good', 'top'}
    
    for pattern in LOCATION_EXTRACT_PATTERNS:
        match = pattern.search(query.lower())
        if match:
            location = match.group(1).strip()
            if location not in stopwords:
                return location
    
    return None


def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
    """Uses pre-compiled patterns."""
    query_lower = query.lower()
    
    # Specific decade
    match = DECADE_PATTERN.search(query_lower)
    if match:
        decade = int(match.group(1))
        return (decade, decade + 99)
    
    # Specific century
    match = CENTURY_PATTERN.search(query_lower)
    if match:
        century = int(match.group(1))
        start = (century - 1) * 100
        return (start, start + 99)
    
    # Era keywords (no regex needed - simple dict lookup)
    era_ranges = {
        'ancient': (-3000, 500),
        'medieval': (500, 1500),
        'colonial': (1500, 1900),
        'modern': (1900, 2024),
        'contemporary': (1990, 2024)
    }
    
    for era, (start, end) in era_ranges.items():
        if era in query_lower:
            return (start, end)
    
    return (None, None)


def extract_content_type_from_query(query: str) -> Optional[str]:
    """
    Detects if user wants specific content type.
    "africa documentary" -> "video"
    
    Why: Allows filtering to specific document_data_type without explicit filter.
    """
    query_lower = query.lower()
    
    type_indicators = {
        'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
        'article': ['article', 'read', 'blog', 'post', 'news'],
        'product': ['buy', 'purchase', 'price', 'shop', 'store'],
        'service': ['hire', 'book', 'appointment', 'service'],
        'person': ['who is', 'biography', 'profile']
    }
    
    for content_type, indicators in type_indicators.items():
        for indicator in indicators:
            if indicator in query_lower:
                return content_type
    
    return None


# === QUERY BUILDING  PART 4 ===
# Constructs the Typesense search parameters

def build_query_weights(intent: str) -> str:
    """
    Returns comma-separated weights string for query_by_weights.
    
    Why: Typesense needs weights as a string like "5,4,3,2,2,1,2"
    """
    weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    return ','.join(str(w) for w in weights)


def build_filter_string(
    filters: Dict = None,
    intent: str = None,
    time_start: int = None,
    time_end: int = None,
    location: str = None,
    content_type: str = None
) -> str:
    """
    Builds Typesense filter_by string.
    
    Why: Combines explicit user filters with auto-detected filters from intent.
    Returns string like: "document_category:=geography && time_period_start:>=1800"
    """
    conditions = []
    
    # User-provided explicit filters
    if filters:
        if filters.get('category'):
            conditions.append(f"document_category:={filters['category']}")
        if filters.get('source'):
            conditions.append(f"document_brand:={filters['source']}")
        if filters.get('data_type'):
            conditions.append(f"document_data_type:={filters['data_type']}")
    
    # Auto-detected time period filter
    if time_start is not None:
        conditions.append(f"time_period_start:>={time_start}")
    if time_end is not None:
        conditions.append(f"time_period_end:<={time_end}")
    
    # Auto-detected location filter
    if location:
        conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
    # Auto-detected content type filter
    if content_type:
        conditions.append(f"document_data_type:={content_type}")
    
    # Always exclude inactive documents
    conditions.append("status:=active")
    
    return ' && '.join(conditions) if conditions else ''


def build_sort_string(
    intent: str,
    user_location: Tuple[float, float] = None
) -> str:
    """
    Builds Typesense sort_by string.
    
    Why: Different intents need different sort priorities.
    - Location queries: geo distance first
    - General: text relevance + authority
    - Historical: time relevance
    """
    if intent == 'location' and user_location:
        lat, lng = user_location
        return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
    if intent == 'product':
        return "_text_match:desc,product_rating:desc,authority_score:desc"
    
    if intent == 'media':
        return "_text_match:desc,media_rating:desc,published_date:desc"
    
    # Default: relevance + authority + freshness
    return "_text_match:desc,authority_score:desc,published_date:desc"


def build_search_params(
    query: str,
    intent: str,
    filters: Dict = None,
    page: int = 1,
    per_page: int = 20,
    user_location: Tuple[float, float] = None,
    safe_search: bool = True  # ← Add here too
) -> Dict:
    """
    Constructs complete Typesense search parameters.
    
    Why: Single function that assembles all search options.
    This is what gets passed to client.collections['documents'].documents.search()
    """
    # Auto-extract filters from query
    location = extract_location_from_query(query)
    time_start, time_end = extract_time_period_from_query(query)
    content_type = extract_content_type_from_query(query)
    
    params = {
        'q': query,
        'query_by': ','.join(SEARCH_FIELDS),
        'query_by_weights': build_query_weights(intent),
        'filter_by': build_filter_string(
            filters=filters,
            intent=intent,
            time_start=time_start,
            time_end=time_end,
            location=location,
            content_type=content_type
        ),
        'sort_by': build_sort_string(intent, user_location),
        'page': page,
        'per_page': per_page,
        'highlight_full_fields': 'document_title,document_summary',
        'highlight_start_tag': '<mark>',
        'highlight_end_tag': '</mark>',
        'snippet_threshold': 500,
    }
    
    # Remove empty filter_by
    if not params['filter_by']:
        del params['filter_by']
    
    return params

######################################=== RESULT PROCESSING  PART 5 ===###################################################################

# Transforms raw Typesense hits into clean response format

def calculate_final_score(hit: Dict) -> float:
    """
    Combines Typesense text_match score with authority and freshness.
    
    Why: Typesense's default scoring doesn't consider your custom authority_score.
    This re-ranks results to favor trusted sources.
    """
    # Get Typesense's relevance score (0-1 normalized)
    text_score = hit.get('text_match', 0) / 100000000  # Typesense uses large numbers
    
    # Get authority score (0-100, normalize to 0-1)
    doc = hit.get('document', {})
    authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
    # Get freshness score based on published_date
    freshness = 0.5  # Default middle score
    if doc.get('published_date'):
        days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
        if days_old < 30:
            freshness = 1.0
        elif days_old < 180:
            freshness = 0.8
        elif days_old < 365:
            freshness = 0.6
        else:
            freshness = 0.4
    
    # Weighted combination
    final_score = (text_score * 0.5) + (authority * 0.35) + (freshness * 0.15)
    
    return round(final_score, 4)


def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
    """
    Removes near-duplicate documents using cluster_uuid.
    Keeps highest-scoring document per cluster.
    
    Why: Wikipedia and Britannica articles on "Africa" shouldn't both appear
    at positions 1 and 2. Keep the best one, group others.
    """
    seen_clusters = {}
    deduplicated = []
    duplicates_grouped = {}
    
    for result in results:
        cluster = result.get('cluster_uuid')
        
        if cluster and cluster in seen_clusters:
            # Add to duplicates group
            if cluster not in duplicates_grouped:
                duplicates_grouped[cluster] = []
            duplicates_grouped[cluster].append(result)
        else:
            if cluster:
                seen_clusters[cluster] = len(deduplicated)
            deduplicated.append(result)
    
    # Attach duplicates to their primary result
    for i, result in enumerate(deduplicated):
        cluster = result.get('cluster_uuid')
        if cluster in duplicates_grouped:
            deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
    return deduplicated


def format_result(hit: Dict) -> Dict:
    """
    Transforms a single Typesense hit into clean response format.
    
    Why: Decouples your API response format from Typesense's internal format.
    Frontend only sees clean, consistent data structure.
    """
    doc = hit.get('document', {})
    highlights = hit.get('highlights', [])
    
    # Build highlight map
    highlight_map = {}
    for h in highlights:
        field = h.get('field')
        snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
        highlight_map[field] = snippet
    
    return {
        'id': doc.get('document_uuid'),
        'title': doc.get('document_title', 'Untitled'),
        'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
        'summary': doc.get('document_summary', ''),
        'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
        'url': doc.get('document_url', ''),
        'source': doc.get('document_brand', 'unknown'),
        'image': (doc.get('image_url') or [None])[0],
        'category': doc.get('document_category', ''),
        'data_type': doc.get('document_data_type', ''),
        'published_date': doc.get('published_date_string', ''),
        'authority_score': doc.get('authority_score', 0),
        'cluster_uuid': doc.get('cluster_uuid'),
        'location': {
            'city': doc.get('location_city'),
            'state': doc.get('location_state'),
            'country': doc.get('location_country'),
            'region': doc.get('location_region')
        },
        'time_period': {
            'start': doc.get('time_period_start'),
            'end': doc.get('time_period_end'),
            'context': doc.get('time_context')
        },
        'score': calculate_final_score(hit),
        'related_sources': []  # Populated by deduplicate_by_cluster
    }


def process_results(raw_response: Dict) -> List[Dict]:
    """
    Processes full Typesense response into clean result list.
    
    Why: Handles the full pipeline: format -> score -> sort -> dedupe
    """
    hits = raw_response.get('hits', [])
    
    # Format each hit
    results = [format_result(hit) for hit in hits]
    
    # Re-sort by final score (includes authority)
    results.sort(key=lambda x: x['score'], reverse=True)
    
    # Deduplicate by cluster
    # results = deduplicate_by_cluster(results)
    
    return results


####################################################################  === MAIN ORCHESTRATOR PART 6 ===
# Single entry point for views.py

def execute_search(query: str, options: Dict = None) -> Dict:
    """
    Low-level search execution. Calls Typesense directly.
    
    Why: Separated from full_search for cases where you need raw results.
    """
    options = options or {}
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(options)
        return response
    except Exception as e:
        return {'hits': [], 'found': 0, 'error': str(e)}


def execute_full_search(
    query: str,
    session_id: str = None,
    filters: Dict = None,
    page: int = 1,
    per_page: int = 20,
    user_location: Tuple[float, float] = None,
    pos_tags: List[Tuple] = None,
    safe_search: bool = True  # ← Add this line
) -> Dict:
    """
    Main entry point for search. Call this from views.py.
    
    Orchestrates:
    1. Intent detection
    2. Parameter building
    3. Typesense search
    4. Result processing
    5. Response formatting
    
    Why: Views.py stays clean. All search logic lives here.
    """
    import time
    start_time = time.time()
    
    # Step 1: Detect intent
    intent = detect_query_intent(query, pos_tags)
    
    # Step 2: Build search parameters
    search_params = build_search_params(
    query=query,
    intent=intent,
    filters=filters,
    page=page,
    per_page=per_page,
    user_location=user_location,
    safe_search=safe_search  # ← Pass it through
)
    
    # Step 3: Execute search
    raw_response = execute_search(query, search_params)
    
    # Step 4: Process results
    results = process_results(raw_response)
    
    # Step 5: Build final response
    search_time = round(time.time() - start_time, 3)
    
    return {
        'query': query,
        'intent': intent,
        'results': results,
        'total': raw_response.get('found', 0),
        'page': page,
        'per_page': per_page,
        'search_time': search_time,
        'session_id': session_id,
        'filters_applied': {
            'time_period': extract_time_period_from_query(query),
            'location': extract_location_from_query(query),
            'content_type': extract_content_type_from_query(query)
        }
    }


