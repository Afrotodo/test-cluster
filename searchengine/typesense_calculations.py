

"""
typesense_calculations.py (v9.0 - Staged Retrieval Strategy)

STAGED RETRIEVAL ARCHITECTURE:
1. STAGE 1 - GRAPH FILTER (Candidate Generation):
   - Use keyword fields (primary_keywords, entity_names, etc.) for FILTERING
   - Inverted index lookup = FAST
   - Millions → Thousands of candidates
   - CACHE these candidate IDs with metadata

2. STAGE 2 - SEMANTIC RERANK (Precision Ordering):
   - Generate embedding for query
   - Run PURE vector search constrained to cached candidate IDs
   - Rank by vector_distance ONLY (no keyword noise)
   - Reorder the cached list before pagination

PRESERVED FROM v8.0 (The Numbers Fix):
- Tab counts ALWAYS come from cache (accurate)
- Filtering = slice cache (not re-query)
- Pagination = slice cache (not re-query)
- Facet counts always match actual results

This ensures:
- Graph structure does the filtering (fast, leverages knowledge graph)
- Vectors do the ranking (pure semantic meaning)
- Numbers are always consistent
- Performance is optimal (vector math on thousands, not millions)
"""

import typesense
from typing import Dict, List, Tuple, Optional, Any
import re
import json
import hashlib
from decouple import config
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .cached_embedding_related_search import store_query_embedding, get_related_searches as get_semantic_related_searches
from .cached_embedding_related_search import store_query_embedding, get_related_searches as get_semantic_related_searches, debug_related_searches


# ============================================================================
# THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# ============================================================================

_executor = ThreadPoolExecutor(max_workers=3)


# ============================================================================
# CONNECTION POOLING FOR EMBEDDING API
# ============================================================================

_http_session = None
_session_lock = threading.Lock()


def _get_http_session():
    """Reusable HTTP session with connection pooling."""
    global _http_session
    if _http_session is None:
        with _session_lock:
            if _http_session is None:
                _http_session = requests.Session()
                adapter = HTTPAdapter(
                    pool_connections=10,
                    pool_maxsize=10,
                    max_retries=Retry(total=2, backoff_factor=0.1)
                )
                _http_session.mount('http://', adapter)
                _http_session.mount('https://', adapter)
    return _http_session


# ============================================================================
# RESULT SET CACHE (In-Memory with TTL)
# ============================================================================

_result_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 300  # 5 minutes
MAX_CACHED_RESULTS = 2000  # Cap to prevent memory issues
MAX_CACHE_ENTRIES = 100  # Max number of different queries to cache


def _generate_cache_key(query: str, alt_mode: str, discovery_filters: List = None, 
                        discovery_locations: List = None) -> str:
    """Generate a unique cache key for a search query."""
    key_parts = [
        query.lower().strip(),
        alt_mode,
        json.dumps(discovery_filters or [], sort_keys=True),
        json.dumps(discovery_locations or [], sort_keys=True),
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
                # Expired, remove it
                del _result_cache[cache_key]
    return None


def _set_cached_results(cache_key: str, data: List[Dict]):
    """Cache result set with timestamp."""
    with _cache_lock:
        # Evict old entries if cache is full
        if len(_result_cache) >= MAX_CACHE_ENTRIES:
            # Remove oldest entry
            oldest_key = min(_result_cache.keys(), 
                           key=lambda k: _result_cache[k]['timestamp'])
            del _result_cache[oldest_key]
        
        _result_cache[cache_key] = {
            'timestamp': datetime.now(),
            'data': data
        }


def _clear_expired_cache():
    """Remove expired cache entries (call periodically)."""
    with _cache_lock:
        now = datetime.now()
        expired_keys = [
            k for k, v in _result_cache.items()
            if (now - v['timestamp']).total_seconds() >= CACHE_TTL_SECONDS
        ]
        for k in expired_keys:
            del _result_cache[k]


# ============================================================================
# US STATE ABBREVIATION MAPPING
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

US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# ============================================================================
# QUERY PREPROCESSING
# ============================================================================

FILLER_PATTERNS = [
    re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
    re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
    re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
    re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
    re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
    re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
    re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
    re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
]


def truncate_for_embedding(query: str, max_words: int = 40) -> str:
    """Strip filler words and truncate for faster embedding."""
    cleaned = query
    for pattern in FILLER_PATTERNS:
        cleaned = pattern.sub(' ', cleaned)
    cleaned = ' '.join(cleaned.split())
    words = cleaned.split()
    if len(words) > max_words:
        cleaned = ' '.join(words[:max_words])
    return cleaned.strip() or query[:200]


# ============================================================================
# EMBEDDING CLIENT
# ============================================================================

EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


def get_query_embedding(query: str) -> Optional[List[float]]:
    """Get embedding with query truncation and connection pooling."""
    clean_query = truncate_for_embedding(query, max_words=40)
    try:
        session = _get_http_session()
        response = session.post(
            EMBEDDING_SERVICE_URL,
            json={"text": clean_query},
            timeout=2
        )
        response.raise_for_status()
        return response.json().get("embedding")
    except Exception as e:
        print(f"⚠️ Embedding error: {e}")
        return None


# ============================================================================
# WORD DISCOVERY
# ============================================================================

# try:
#     from .word_discovery import process_query_optimized
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     try:
#         from word_discovery import process_query_optimized
#         WORD_DISCOVERY_AVAILABLE = True
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery not available, using basic search")
try:
    from .word_discovery import process_query_optimized
    WORD_DISCOVERY_AVAILABLE = True
    print("✅ WORD_DISCOVERY IMPORTED SUCCESSFULLY")  # ADD THIS
except ImportError:
    try:
        from word_discovery import process_query_optimized
        WORD_DISCOVERY_AVAILABLE = True
        print("✅ WORD_DISCOVERY IMPORTED (fallback)")  # ADD THIS
    except ImportError:
        WORD_DISCOVERY_AVAILABLE = False
        print("⚠️ word_discovery not available, using basic search")


def _do_word_discovery(query: str) -> Dict:
    """Wrapper for thread pool."""
    if WORD_DISCOVERY_AVAILABLE:
        from .word_discovery import process_query_optimized
        WORD_DISCOVERY_AVAILABLE = True
        print(f"✅ WORD_DISCOVERY LOADED FROM: {process_query_optimized.__module__}")  # ADD THIS
        return process_query_optimized(query, verbose=False)
      
    return {
        'query': query,
        'corrected_query': query,
        'valid_count': 0,
        'unknown_count': len(query.split()),
        'corrections': [],
        'filters': [],
        'locations': [],
        'sort': None,
        'ngrams': [],
        'terms': [],
        'total_score': 0,
        'average_score': 0,
        'max_score': 0,
        'processing_time_ms': 0
    }


def _do_embedding(query: str) -> Optional[List[float]]:
    """Wrapper for thread pool."""
    return get_query_embedding(query)


# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """Run word discovery and embedding IN PARALLEL."""
#     if skip_embedding:
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrections', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding

def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
    """
    Run word discovery and embedding IN PARALLEL.
    
    ENHANCED: Adds 'signals' to discovery output for easier consumption.
    
    Args:
        query: The search query
        skip_embedding: If True, don't generate embedding
    
    Returns:
        Tuple of (discovery_dict_with_signals, embedding_or_none)
    """
    # Import executor and functions (adjust paths as needed)
    # from concurrent.futures import ThreadPoolExecutor
    # _executor = ThreadPoolExecutor(max_workers=3)
    
    if skip_embedding:
        discovery = _do_word_discovery(query)
        # Add signals to discovery
        discovery['signals'] = extract_search_signals(discovery)
        return discovery, None
    
    discovery_future = _executor.submit(_do_word_discovery, query)
    embedding_future = _executor.submit(_do_embedding, query)
    
    discovery = discovery_future.result()
    embedding = embedding_future.result()
    
    # Add signals to discovery
    discovery['signals'] = extract_search_signals(discovery)
    
    # If query was corrected significantly, re-embed the corrected version
    corrected_query = discovery.get('corrected_query', query)
    if corrected_query.lower() != query.lower():
        corrections = discovery.get('corrections', [])
        significant = any(
            c.get('original', '').lower() != c.get('corrected', '').lower()
            for c in corrections
        )
        if significant and embedding is not None:
            embedding = get_query_embedding(corrected_query)
    
    return discovery, embedding


# ============================================================================
# REGEX PATTERNS
# ============================================================================

LOCATION_PATTERNS = [
    re.compile(r'\b(in|near|around|at)\s+\w+'),
    re.compile(r'\b(city|state|country|region)\b'),
    re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
]

HISTORICAL_PATTERNS = [
    re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
    re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
    re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
    re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
]

PRODUCT_PATTERNS = [
    re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
    re.compile(r'\b(product|item|purchase|order|shipping)\b'),
    re.compile(r'\$[0-9]+'),
]

PERSON_PATTERNS = [
    re.compile(r'\b(who is|biography|born|died|life of)\b'),
    re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
    re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
]

MEDIA_PATTERNS = [
    re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
    re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
]

LOCATION_EXTRACT_PATTERNS = [
    re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
    re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
]

DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# ============================================================================
# CLIENT SETUP
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
# FIELD CONFIGURATION
# ============================================================================

SEARCH_FIELDS = [
    'key_facts',
    'document_title',
    'primary_keywords',
    'entity_names'
]

DEFAULT_WEIGHTS = [10, 5, 3, 2]

INTENT_WEIGHTS = {
    'general':    [10, 5, 3, 2],
    'location':   [8, 5, 3, 4],
    'historical': [10, 4, 4, 3],
    'product':    [8, 6, 4, 2],
    'person':     [10, 5, 3, 5],
    'media':      [9, 5, 4, 3],
}

MIN_SCORE_THRESHOLD = 0.5

SOURCE_AUTHORITY = {
    'britannica': 95,
    'wikipedia': 90,
    'government': 90,
    'academic': 88,
    'news': 70,
    'blog': 50,
    'social': 40,
    'default': 60
}


# ============================================================================
# VALID FILTER VALUES
# ============================================================================

VALID_DATA_TYPES = frozenset([
    'article', 'person', 'business', 'place', 'media', 'event', 'product'
])

VALID_SCHEMAS = frozenset([
    'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
    'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
    'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
    'AudioObject', 'Book', 'Movie', 'MusicRecording'
])

VALID_CATEGORIES = frozenset([
    'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
    'travel_tourism', 'entertainment', 'business', 'education',
    'technology', 'sports', 'finance', 'real_estate', 'automotive',
    'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
])

# Labels for UI display
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
# INTENT DETECTION
# ============================================================================

def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
    """Analyzes query to determine user intent."""
    query_lower = query.lower()
    
    for pattern in LOCATION_PATTERNS:
        if pattern.search(query_lower):
            return 'location'
    for pattern in HISTORICAL_PATTERNS:
        if pattern.search(query_lower):
            return 'historical'
    for pattern in PRODUCT_PATTERNS:
        if pattern.search(query_lower):
            return 'product'
    for pattern in PERSON_PATTERNS:
        if pattern.search(query_lower):
            return 'person'
    for pattern in MEDIA_PATTERNS:
        if pattern.search(query_lower):
            return 'media'
    return 'general'


# ============================================================================
# FILTER BUILDING (For Stage 1 Graph Filter)
# ============================================================================

# def build_filter_string_from_discovery(
#     discovery: Dict,
#     filters: Dict = None,
#     exclude_data_type: bool = False
# ) -> str:
#     """Builds Typesense filter_by string from word_discovery output."""
#     conditions = []
    
#     # 1. GRAPH-BASED FILTERS (from word_discovery)
#     for filter_item in discovery.get('filters', []):
#         field = filter_item.get('field')
#         value = filter_item.get('value')
#         if field and value:
#             safe_value = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(value))
#             if safe_value:
#                 conditions.append(f"{field}:={safe_value}")
    
#     # Location filters from discovery
#     for loc_item in discovery.get('locations', []):
#         field = loc_item.get('field', 'location_state')
#         values = loc_item.get('values', [])
#         if values:
#             loc_parts = []
#             for val in values:
#                 safe_val = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(val))
#                 if safe_val:
#                     loc_parts.append(f"{field}:={safe_val}")
#             if loc_parts:
#                 if len(loc_parts) == 1:
#                     conditions.append(loc_parts[0])
#                 else:
#                     conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     # 2. UI FILTERS (skip data_type if exclude_data_type=True)
#     if filters:
#         if not exclude_data_type:
#             data_type = filters.get('data_type')
#             if data_type and data_type in VALID_DATA_TYPES:
#                 conditions.append(f"document_data_type:={data_type}")
        
#         category = filters.get('category')
#         if category:
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         schema = filters.get('schema')
#         if schema and schema in VALID_SCHEMAS:
#             conditions.append(f"document_schema:={schema}")
    
#     return ' && '.join(conditions) if conditions else ''
def build_filter_string_from_discovery(
    discovery: Dict,
    filters: Dict = None,
    exclude_data_type: bool = False
) -> str:
    """
    Builds Typesense filter_by string from word_discovery output.
    
    ENHANCED: Better handling of locations and filters from discovery.
    
    Args:
        discovery: Output from word_discovery.process_query_optimized()
        filters: UI filters (data_type, category, schema)
        exclude_data_type: If True, don't filter by data_type (for facet counting)
    
    Returns:
        Typesense filter_by string
    """
    conditions = []
    
    if not discovery:
        discovery = {}
    
    # 1. GRAPH-BASED FILTERS (from word_discovery)
    for filter_item in discovery.get('filters', []):
        field = filter_item.get('field')
        value = filter_item.get('value')
        if field and value:
            # Sanitize value for Typesense
            safe_value = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(value))
            if safe_value:
                # Use exact match for filter fields
                conditions.append(f"{field}:={safe_value}")
    
    # 2. LOCATION FILTERS (from word_discovery)
    for loc_item in discovery.get('locations', []):
        field = loc_item.get('field', 'location_state')
        values = loc_item.get('values', [])
        
        if values:
            loc_parts = []
            for val in values:
                safe_val = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(val))
                if safe_val:
                    loc_parts.append(f"{field}:={safe_val}")
            
            if loc_parts:
                if len(loc_parts) == 1:
                    conditions.append(loc_parts[0])
                else:
                    # OR together multiple location variants (e.g., "Georgia" OR "GA")
                    conditions.append('(' + ' || '.join(loc_parts) + ')')
    
    # 3. UI FILTERS (from frontend)
    if filters:
        # Data type filter (skip if exclude_data_type=True for facet counting)
        if not exclude_data_type:
            data_type = filters.get('data_type')
            if data_type:
                # Validate against allowed values
                safe_dt = re.sub(r'[^a-zA-Z0-9_]', '', data_type)
                if safe_dt:
                    conditions.append(f"document_data_type:={safe_dt}")
        
        # Category filter
        category = filters.get('category')
        if category:
            safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
            if safe_category:
                conditions.append(f"document_category:={safe_category}")
        
        # Schema filter
        schema = filters.get('schema')
        if schema:
            safe_schema = re.sub(r'[^a-zA-Z0-9]', '', schema)
            if safe_schema:
                conditions.append(f"document_schema:={safe_schema}")
    
    return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, discovery: Dict = None) -> str:
#     """Builds Typesense sort_by string."""
#     if discovery:
#         sort_instruction = discovery.get('sort')
#         if sort_instruction:
#             field = sort_instruction.get('field')
#             order = sort_instruction.get('order', 'asc')
#             if field:
#                 return f"{field}:{order},authority_score:desc"
#     return "authority_score:desc,published_date:desc"

def build_sort_string(intent: str, discovery: Dict = None) -> str:
    """
    Builds Typesense sort_by string.
    
    Uses sort instruction from word_discovery if present (e.g., "first", "latest").
    
    Args:
        intent: Query intent (historical, person, etc.)
        discovery: Output from word_discovery.process_query_optimized()
    
    Returns:
        Typesense sort_by string
    """
    # Check if word_discovery detected a sort instruction
    if discovery:
        sort_instruction = discovery.get('sort')
        if sort_instruction:
            field = sort_instruction.get('field')
            order = sort_instruction.get('order', 'asc')
            if field:
                # Primary sort by discovered field, secondary by authority
                return f"{field}:{order},authority_score:desc"
    
    # Default: authority score (most authoritative first), then recency
    return "authority_score:desc,published_date:desc"

# ============================================================================
# DISCOVERY SIGNAL EXTRACTION (NEW SECTION - ADD HERE)
# ============================================================================

def get_dynamic_weights(category_summary: Dict) -> str:
    """
    Adjust field weights based on what word_discovery found.
    
    This makes the search adapt to the content types in the query:
    - Person queries boost entity_names
    - Location queries boost location fields
    - Media queries boost semantic_keywords
    
    Args:
        category_summary: Dict from word_discovery with has_person, has_location, etc.
    
    Returns:
        Comma-separated weight string for query_by_weights
    """
    # Base weights aligned with get_query_fields() default order:
    # primary_keywords, entity_names, semantic_keywords, key_facts, document_title
    weights = {
        'primary_keywords': 10,
        'entity_names': 8,
        'semantic_keywords': 6,
        'key_facts': 4,
        'document_title': 3,
        'location_city': 8,
        'location_state': 8,
    }
    
    if not category_summary:
        return '10,8,6,4,3'
    
    # Boost entity_names for person queries
    if category_summary.get('has_person'):
        weights['entity_names'] = 15
        weights['key_facts'] = 6  # People often mentioned in facts
    
    # Boost for location queries
    if category_summary.get('has_location'):
        weights['location_city'] = 12
        weights['location_state'] = 12
        weights['primary_keywords'] = 8  # Slightly reduce keyword dominance
    
    # Boost for media/song queries
    if category_summary.get('has_song_title') or category_summary.get('has_media'):
        weights['semantic_keywords'] = 12
        weights['primary_keywords'] = 12
        weights['document_title'] = 8  # Song/media titles matter
    
    # Boost for topic/culture queries
    if category_summary.get('has_topic') or category_summary.get('has_culture'):
        weights['primary_keywords'] = 12
        weights['semantic_keywords'] = 10
    
    # Boost for business/entity queries
    if category_summary.get('has_business') or category_summary.get('has_entity'):
        weights['entity_names'] = 14
        weights['primary_keywords'] = 10
    
    # Boost for food queries
    if category_summary.get('has_food'):
        weights['primary_keywords'] = 12
        weights['semantic_keywords'] = 10
    
    return weights


def get_query_fields(category_summary: Dict) -> Tuple[str, str]:
    """
    Select which fields to search based on content types found.
    
    Args:
        category_summary: Dict from word_discovery with has_person, has_location, etc.
    
    Returns:
        Tuple of (query_by string, query_by_weights string)
    """
    # Base field order
    fields = ['primary_keywords', 'entity_names', 'semantic_keywords', 'key_facts', 'document_title']
    
    weights = get_dynamic_weights(category_summary)
    
    # Add location fields if location detected
    if category_summary and category_summary.get('has_location'):
        # Insert location fields near the front
        fields = ['location_city', 'location_state'] + fields
    
    # Build weight string in same order as fields
    weight_values = []
    for field in fields:
        weight_values.append(str(weights.get(field, 5)))
    
    return ','.join(fields), ','.join(weight_values)


def build_boosted_query(discovery: Dict, original_query: str) -> str:
    """
    Build a smarter query using term-level metadata from word_discovery.
    
    High-rank terms and ngrams are prioritized. Stopwords are excluded.
    Ngrams (phrases) are quoted for exact matching when high-rank.
    
    Args:
        discovery: Full output from word_discovery.process_query_optimized()
        original_query: The original query string (fallback)
    
    Returns:
        Optimized query string for Typesense
    """
    if not discovery:
        return original_query
    
    terms = discovery.get('terms', [])
    ngrams = discovery.get('ngrams', [])
    
    # If no enriched data, use corrected query
    if not terms and not ngrams:
        return discovery.get('corrected_query', original_query)
    
    boosted_parts = []
    
    # Track positions covered by ngrams (don't duplicate)
    ngram_positions = set()
    for ngram in ngrams:
        ngram_positions.update(ngram.get('positions', []))
    
    # Process ngrams first (higher priority - they're recognized phrases)
    for ngram in ngrams:
        phrase = ngram.get('ngram', '')
        rank = ngram.get('rank', 0)
        category = ngram.get('category', '')
        
        if not phrase:
            continue
        
        # Convert rank to int if string
        if isinstance(rank, str):
            try:
                rank = int(float(rank))
            except (ValueError, TypeError):
                rank = 0
        
        # High-rank ngrams (known entities/phrases) get quoted for exact match
        # This tells Typesense to match the phrase together
        if rank > 30000:
            boosted_parts.append(f'"{phrase}"')
        else:
            boosted_parts.append(phrase)
    
    # Process individual terms not covered by ngrams
    for term in terms:
        position = term.get('position', 0)
        
        # Skip if this position is part of an ngram
        if position in ngram_positions:
            continue
        
        # Skip stopwords
        if term.get('category') == 'stopword':
            continue
        
        # Skip unknown/uncorrected terms (low confidence)
        status = term.get('status', '')
        if status == 'unknown':
            # Still include but don't prioritize
            word = term.get('word', '')
            if word:
                boosted_parts.append(word)
            continue
        
        # Get the search word (corrected if available)
        word = term.get('search_word', term.get('word', ''))
        if not word:
            continue
        
        rank = term.get('rank', term.get('score', 0))
        if isinstance(rank, str):
            try:
                rank = int(float(rank))
            except (ValueError, TypeError):
                rank = 0
        
        category = term.get('category', '')
        
        # High-rank individual terms are important entities
        # Don't quote single words, but they'll be included
        boosted_parts.append(word)
    
    # If we built something, use it; otherwise fall back
    if boosted_parts:
        return ' '.join(boosted_parts)
    
    return discovery.get('corrected_query', original_query)


def extract_search_signals(discovery: Dict) -> Dict[str, Any]:
    """
    Extract actionable search signals from word_discovery output.
    
    This consolidates all the intelligence from word_discovery into
    a single structure that can be used by the search functions.
    
    Args:
        discovery: Full output from word_discovery.process_query_optimized()
    
    Returns:
        Dict with:
            - boosted_query: Optimized query string
            - query_fields: Fields to search
            - query_weights: Weights for those fields
            - filter_string: Pre-built filter string
            - sort_string: Sort instruction if any
            - high_value_terms: List of high-rank terms for potential boosting
            - detected_entities: Recognized entities with their types
    """
    if not discovery:
        return {
            'boosted_query': '',
            'query_fields': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
            'query_weights': '10,8,6,4,3',
            'filter_string': '',
            'sort_string': 'authority_score:desc',
            'high_value_terms': [],
            'detected_entities': [],
        }
    
    category_summary = discovery.get('category_summary', {})
    
    # Get dynamic fields and weights
    query_fields, query_weights = get_query_fields(category_summary)
    
    # Build optimized query
    boosted_query = build_boosted_query(discovery, discovery.get('corrected_query', ''))
    
    # Extract high-value terms (rank > 50000)
    high_value_terms = []
    for term in discovery.get('terms', []):
        rank = term.get('rank', 0)
        if isinstance(rank, str):
            try:
                rank = int(float(rank))
            except:
                rank = 0
        if rank > 50000 and term.get('category') != 'stopword':
            high_value_terms.append({
                'term': term.get('search_word', term.get('word')),
                'rank': rank,
                'category': term.get('category', ''),
                'pos': term.get('pos', '')
            })
    
    # Extract detected entities from ngrams
    detected_entities = []
    for ngram in discovery.get('ngrams', []):
        category = ngram.get('category', '')
        if category and category not in ('stopword', 'unknown', ''):
            detected_entities.append({
                'phrase': ngram.get('ngram'),
                'category': category,
                'rank': ngram.get('rank', 0),
                'type': ngram.get('type', 'bigram')
            })
    
    return {
        'boosted_query': boosted_query,
        'query_fields': query_fields,
        'query_weights': query_weights,
        'high_value_terms': high_value_terms,
        'detected_entities': detected_entities,
        'category_summary': category_summary,
    }


# ============================================================================
# SEARCH EXECUTION
# ============================================================================

def execute_search_multi(search_params: Dict) -> Dict:
    """Execute search using multi_search endpoint."""
    search_requests = {
        'searches': [{
            'collection': COLLECTION_NAME,
            **search_params
        }]
    }
    
    try:
        response = client.multi_search.perform(search_requests, {})
        return response['results'][0]
    except Exception as e:
        print(f"TYPESENSE ERROR: {e}")
        return {'hits': [], 'found': 0, 'error': str(e)}


# ============================================================================
# STAGE 1: GRAPH FILTER - Candidate Generation (KEYWORD-BASED FILTERING)
# ============================================================================

# def fetch_candidate_ids_graph_filter(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     max_results: int = MAX_CACHED_RESULTS
# ) -> List[Dict]:
#     """
#     STAGE 1: Graph Filter - Candidate Generation
    
#     Uses inverted index fields (primary_keywords, entity_names, etc.) for FILTERING.
#     This leverages your knowledge graph structure to narrow down candidates FAST.
    
#     Returns list of {id, data_type, category, schema, authority_score} for caching.
    
#     IMPORTANT: Does NOT rank semantically - just filters candidates.
#     IMPORTANT: Does NOT filter by data_type so we get ALL types for tab counts.
#     """
#     # Build filter WITHOUT data_type (we want ALL types for tabs)
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters,
#         exclude_data_type=True  # Get all types for accurate facet counts
#     )
    
#     PAGE_SIZE = 250  # Typesense max
#     all_results = []
#     current_page = 1
#     max_pages = (max_results // PAGE_SIZE) + 1  # Safety limit
    
#     while len(all_results) < max_results and current_page <= max_pages:
#         # STAGE 1: Pure keyword/graph filtering - NO vector search here
#         # This uses the inverted index for fast candidate retrieval
#         params = {
#             'q': query,
#             'query_by': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
#             'query_by_weights': '10,8,6,4,3',  # Weight graph fields heavily
#             'per_page': PAGE_SIZE,
#             'page': current_page,
#             'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score',
#             'num_typos': 1,
#             # 'drop_tokens_threshold': 2,
#              'drop_tokens_threshold': 0, 
#             'sort_by': 'authority_score:desc',  # Sort by authority for now (reranked later)
#         }
        
#         if filter_str:
#             params['filter_by'] = filter_str
        
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             hits = response.get('hits', [])
#             found = response.get('found', 0)
            
#             if not hits:
#                 break  # No more results
            
#             for hit in hits:
#                 doc = hit.get('document', {})
#                 all_results.append({
#                     'id': doc.get('document_uuid'),
#                     'data_type': doc.get('document_data_type', ''),
#                     'category': doc.get('document_category', ''),
#                     'schema': doc.get('document_schema', ''),
#                     'authority_score': doc.get('authority_score', 0),
#                     'text_match': hit.get('text_match', 0),
#                     # No rank yet - will be set by semantic reranking
#                 })
            
#             # Check if we've fetched all available results
#             if len(all_results) >= found or len(hits) < PAGE_SIZE:
#                 break
            
#             current_page += 1
            
#         except Exception as e:
#             print(f"❌ fetch_candidate_ids_graph_filter error (page {current_page}): {e}")
#             break
    
#     print(f"📊 STAGE 1 (Graph Filter): Retrieved {len(all_results)} candidates")
#     return all_results[:max_results]

def fetch_candidate_ids_graph_filter(
    query: str,
    discovery: Dict = None,
    filters: Dict = None,
    max_results: int = 2000  # MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    STAGE 1: Graph Filter - Candidate Generation
    
    Uses inverted index fields (primary_keywords, entity_names, etc.) for FILTERING.
    This leverages your knowledge graph structure to narrow down candidates FAST.
    
    ENHANCED: Now properly uses word_discovery metadata:
    - Dynamic field weights based on category_summary
    - Boosted query using term ranks
    - Smart field selection based on content types
    
    Args:
        query: The search query (used as fallback)
        discovery: Output from word_discovery.process_query_optimized()
        filters: UI filters (data_type, category, schema)
        max_results: Maximum candidates to retrieve
    
    Returns:
        List of {id, data_type, category, schema, authority_score, text_match}
    """
    # Import client here to avoid circular imports (adjust path as needed)
    # from . import client, COLLECTION_NAME
    # For this file, assume client and COLLECTION_NAME are available globally
    
    # Extract search signals from discovery
    signals = extract_search_signals(discovery) if discovery else {}
    
    # Use boosted query if available, otherwise use corrected query or original
    if signals.get('boosted_query'):
        search_query = signals['boosted_query']
    elif discovery and discovery.get('corrected_query'):
        search_query = discovery['corrected_query']
    else:
        search_query = query
    
    # Get dynamic fields and weights based on what was discovered
    query_fields = signals.get('query_fields', 
        'primary_keywords,entity_names,semantic_keywords,key_facts,document_title')
    query_weights = signals.get('query_weights', '10,8,6,4,3')
    
    # Build filter WITHOUT data_type (we want ALL types for tabs)
    filter_str = build_filter_string_from_discovery(
        discovery=discovery or {},
        filters=filters,
        exclude_data_type=True  # Get all types for accurate facet counts
    )
    
    # Build sort string
    sort_str = build_sort_string('general', discovery)
    
    PAGE_SIZE = 250  # Typesense max
    all_results = []
    current_page = 1
    max_pages = (max_results // PAGE_SIZE) + 1  # Safety limit
    
    # Log what we're searching with
    print(f"🔍 Graph Filter Query: '{search_query}'")
    print(f"   Fields: {query_fields}")
    print(f"   Weights: {query_weights}")
    if filter_str:
        print(f"   Filters: {filter_str}")
    if signals.get('high_value_terms'):
        print(f"   High-value terms: {[t['term'] for t in signals['high_value_terms']]}")
    if signals.get('detected_entities'):
        print(f"   Detected entities: {[e['phrase'] for e in signals['detected_entities']]}")
    
    while len(all_results) < max_results and current_page <= max_pages:
        # STAGE 1: Graph filtering with discovery-enhanced parameters
        params = {
            'q': search_query,
            'query_by': query_fields,
            'query_by_weights': query_weights,
            'per_page': PAGE_SIZE,
            'page': current_page,
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score',
            'num_typos': 1,
            'drop_tokens_threshold': 0,  # Don't drop tokens - we've curated the query
            'sort_by': sort_str,
        }
        
        if filter_str:
            params['filter_by'] = filter_str
        
        try:
            # Execute search (uses your existing client)
            response = client.collections[COLLECTION_NAME].documents.search(params)
            hits = response.get('hits', [])
            found = response.get('found', 0)
            
            if not hits:
                break  # No more results
            
            for hit in hits:
                doc = hit.get('document', {})
                all_results.append({
                    'id': doc.get('document_uuid'),
                    'data_type': doc.get('document_data_type', ''),
                    'category': doc.get('document_category', ''),
                    'schema': doc.get('document_schema', ''),
                    'authority_score': doc.get('authority_score', 0),
                    'text_match': hit.get('text_match', 0),
                    # No semantic rank yet - will be set by Stage 2
                })
            
            # Check if we've fetched all available results
            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break
            
            current_page += 1
            
        except Exception as e:
            print(f"❌ fetch_candidate_ids_graph_filter error (page {current_page}): {e}")
            break
    
    print(f"📊 STAGE 1 (Graph Filter): Retrieved {len(all_results)} candidates")
    
    # Store signals in a way that can be passed along (optional)
    # This could be returned as part of the response if needed
    
    return all_results[:max_results]

# ============================================================================
# STAGE 2: SEMANTIC RERANK - Precision Ordering (VECTOR-BASED RANKING)
# ============================================================================

def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 500
) -> List[Dict]:
    """
    STAGE 2: Semantic Rerank - Precision Ordering
    
    Takes candidate IDs from Stage 1 and reranks them using PURE vector similarity.
    No keyword influence on ranking - meaning only.
    
    Args:
        candidate_ids: List of document UUIDs from Stage 1 (graph filter)
        query_embedding: The query embedding vector
        max_to_rerank: Max candidates to rerank (for performance)
    
    Returns:
        List of {id, vector_distance, semantic_rank} ordered by vector_distance
    """
    if not candidate_ids or not query_embedding:
        # Return original order if no embedding
        return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i} 
                for i, cid in enumerate(candidate_ids)]
    
    # Limit candidates for performance
    ids_to_rerank = candidate_ids[:max_to_rerank]
    
    # Build ID filter for constrained vector search
    id_filter = ','.join([f'`{doc_id}`' for doc_id in ids_to_rerank])
    
    embedding_str = ','.join(str(x) for x in query_embedding)
    
    # PURE vector search - no keyword fields, no hybrid blending
    # This ranks ONLY by semantic similarity (vector_distance)
    params = {
        'q': '*',  # No text query - pure vector
        'vector_query': f"embedding:([{embedding_str}], k:{len(ids_to_rerank)}, alpha:1.0)",  # alpha:1.0 = 100% vector
        'filter_by': f'document_uuid:[{id_filter}]',
        'per_page': len(ids_to_rerank),
        'include_fields': 'document_uuid',
    }
    
    try:
        response = execute_search_multi(params)
        hits = response.get('hits', [])
        
        reranked = []
        for i, hit in enumerate(hits):
            doc = hit.get('document', {})
            reranked.append({
                'id': doc.get('document_uuid'),
                'vector_distance': hit.get('vector_distance', 1.0),
                'semantic_rank': i
            })
        
        # Add any IDs that weren't returned (shouldn't happen, but safety)
        reranked_ids = {r['id'] for r in reranked}
        for cid in ids_to_rerank:
            if cid not in reranked_ids:
                reranked.append({
                    'id': cid,
                    'vector_distance': 1.0,
                    'semantic_rank': len(reranked)
                })
        
        print(f"🎯 STAGE 2 (Semantic Rerank): Reranked {len(reranked)} candidates by vector similarity")
        return reranked
        
    except Exception as e:
        print(f"⚠️ semantic_rerank_candidates error: {e}")
        # Return original order on error
        return [{'id': cid, 'vector_distance': 1.0, 'semantic_rank': i} 
                for i, cid in enumerate(ids_to_rerank)]


def apply_semantic_ranking_to_cache(
    cached_results: List[Dict],
    reranked_results: List[Dict]
) -> List[Dict]:
    """
    Apply semantic ranking to cached results.
    
    Takes the cached results (with metadata) and reorders them based on 
    semantic reranking results (vector_distance order).
    
    Preserves all metadata (data_type, category, schema, authority_score).
    """
    if not reranked_results:
        return cached_results
    
    # Create lookup: id -> semantic rank and vector_distance
    rank_lookup = {
        r['id']: {
            'semantic_rank': r['semantic_rank'],
            'vector_distance': r.get('vector_distance', 1.0)
        }
        for r in reranked_results
    }
    
    # Add semantic info to cached results
    for item in cached_results:
        item_id = item.get('id')
        if item_id in rank_lookup:
            item['semantic_rank'] = rank_lookup[item_id]['semantic_rank']
            item['vector_distance'] = rank_lookup[item_id]['vector_distance']
        else:
            # Items not in rerank set get pushed to end
            item['semantic_rank'] = 999999
            item['vector_distance'] = 1.0
    
    # Sort by semantic rank (pure vector similarity order)
    cached_results.sort(key=lambda x: x.get('semantic_rank', 999999))
    
    # Update rank field to reflect new order
    for i, item in enumerate(cached_results):
        item['rank'] = i
    
    return cached_results


# ============================================================================
# FACET COUNTING FROM CACHE (PRESERVED FROM v8.0 - THE FIX)
# ============================================================================

def count_facets_from_cache(cached_results: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Count facets from cached result set.
    This ensures tab counts ALWAYS match actual results.
    
    PRESERVED FROM v8.0 - This is the fix for accurate numbers.
    """
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
    
    facets = {
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
    
    return facets


def filter_cached_results(
    cached_results: List[Dict],
    data_type: str = None,
    category: str = None,
    schema: str = None
) -> List[Dict]:
    """
    Filter cached results by data_type, category, or schema.
    
    PRESERVED FROM v8.0 - This is the fix for accurate filtering.
    """
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
    """
    Paginate cached results. Returns (page_items, total_count).
    
    PRESERVED FROM v8.0 - This is the fix for accurate pagination.
    """
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
    """
    Fetch full document details for a list of IDs.
    Used after pagination to get complete data for display.
    Preserves the order of document_ids.
    """
    if not document_ids:
        return []
    
    # Build ID filter
    id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
    params = {
        'q': '*',
        'filter_by': f'document_uuid:[{id_filter}]',
        'per_page': len(document_ids),
        'exclude_fields': 'embedding',
    }
    
    try:
        response = execute_search_multi(params)
        hits = response.get('hits', [])
        
        # Create a map of id -> full doc
        doc_map = {}
        for hit in hits:
            doc = hit.get('document', {})
            doc_id = doc.get('document_uuid')
            if doc_id:
                doc_map[doc_id] = format_result({'document': doc, 'highlights': hit.get('highlights', [])}, query)
        
        # Return in original order (preserves ranking)
        results = []
        for doc_id in document_ids:
            if doc_id in doc_map:
                results.append(doc_map[doc_id])
        
        return results
    
    except Exception as e:
        print(f"❌ fetch_full_documents error: {e}")
        return []


# ============================================================================
# RESULT FORMATTING
# ============================================================================

def calculate_final_score(hit: Dict, query: str = '') -> float:
    """Combines vector_distance and authority scores."""
    vector_distance = hit.get('vector_distance', 1.0)
    vector_similarity = max(0, 1 - vector_distance)
    
    doc = hit.get('document', {})
    authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
    text_score = hit.get('text_match', 0) / 100000000
    
    if text_score > 0:
        final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
    else:
        final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
    return round(min(1.0, final_score), 4)


def format_result(hit: Dict, query: str = '') -> Dict:
    """Transforms a Typesense hit into clean response format."""
    doc = hit.get('document', {})
    highlights = hit.get('highlights', [])
    
    highlight_map = {}
    for h in highlights:
        field = h.get('field')
        snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
        highlight_map[field] = snippet
    
    vector_distance = hit.get('vector_distance')
    semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
    # Format date without time (e.g., "Sep 30, 2023")
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
        'image_url': doc.get('image_url') or [],  # ADD THIS - full array
        'logo_url': doc.get('logo_url') or [],    # ADD THIS - full array
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
        'key_facts_highlighted': highlight_map.get('key_facts', ''),
        'semantic_score': semantic_score,
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
        'score': calculate_final_score(hit, query),
        'related_sources': []
    }


def get_filter_terms_from_discovery(discovery: Dict) -> List[str]:
    """Extract searchable terms from discovery result."""
    terms = []
    
    for f in discovery.get('filters', []):
        term = f.get('term', f.get('value', ''))
        if term:
            terms.append(term)
    
    for loc in discovery.get('locations', []):
        term = loc.get('term', '')
        if term:
            terms.append(term)
    
    for t in discovery.get('terms', []):
        if t.get('status') in ('valid', 'corrected'):
            if t.get('category') != 'stopword':
                word = t.get('search_word', t.get('word', ''))
                if word and word not in terms:
                    terms.append(word)
    
    return terms


# ============================================================================
# MAIN SEARCH FUNCTION (v9.0 - Staged Retrieval Strategy)
# ============================================================================

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
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - v9.0 Staged Retrieval Strategy.
    
#     STAGED RETRIEVAL ARCHITECTURE:
    
#     1. Check cache for Stage 1 candidates (graph-filtered set)
#     2. If cache miss: Run Stage 1 (Graph Filter) - keyword-based candidate generation
#     3. Cache the candidates (with metadata for accurate facets)
#     4. Count facets FROM CACHE (always accurate - YOUR FIX)
#     5. Filter cache by data_type/category/schema if UI filters active
#     6. Run Stage 2 (Semantic Rerank) - pure vector ranking on filtered set
#     7. Paginate from reranked results
#     8. Fetch full docs only for current page
    
#     This ensures:
#     - Graph structure does filtering (fast, leverages knowledge graph)
#     - Vectors do ranking (pure semantic meaning)
#     - Numbers are always consistent (facets from cache)
#     - Performance is optimal (vector math on thousands, not millions)
    
#     alt_mode:
#         'n' = KEYWORD PATH (dropdown click) - no semantic reranking
#         'y' = SEMANTIC PATH (typed freely) - full staged retrieval
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # Extract active filters
#     active_data_type = filters.get('data_type') if filters else None
#     active_category = filters.get('category') if filters else None
#     active_schema = filters.get('schema') if filters else None
    
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
#     # =========================================================================
#     # KEYWORD PATH (alt_mode='n') - No semantic reranking
#     # =========================================================================
    
#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PATH (Stage 1 Only): '{query}' (alt_mode={alt_mode})")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Generate cache key
#         cache_key = _generate_cache_key(query, 'keyword', None, None)
        
#         # Check cache for Stage 1 candidates
#         cached_data = _get_cached_results(cache_key)
        
#         if cached_data:
#             print(f"✅ Cache HIT: {len(cached_data)} candidates")
#             all_results = cached_data
#             times['cache'] = 'hit'
#         else:
#             print(f"❌ Cache MISS: Running Stage 1 (Graph Filter)...")
#             t2 = time.time()
            
#             # STAGE 1: Graph Filter - Candidate Generation
#             all_results = fetch_candidate_ids_graph_filter(
#                 query=query,
#                 discovery={},
#                 filters=filters,
#                 max_results=MAX_CACHED_RESULTS
#             )
#             times['stage1_graph_filter'] = round((time.time() - t2) * 1000, 2)
            
#             # Cache the candidates (with metadata)
#             if all_results:
#                 _set_cached_results(cache_key, all_results)
#             print(f"📦 Cached {len(all_results)} candidates")
#             times['cache'] = 'miss'
        
#         # Count facets FROM CACHE (always accurate - YOUR FIX!)
#         t3 = time.time()
#         all_facets = count_facets_from_cache(all_results)
#         times['count_facets'] = round((time.time() - t3) * 1000, 2)
        
#         data_type_facets = all_facets.get('data_type', [])
#         category_facets = all_facets.get('category', [])
#         schema_facets = all_facets.get('schema', [])
#         facet_total = len(all_results)
        
#         print(f"📊 Facets from cache: {[(f['value'], f['count']) for f in data_type_facets]}")
        
#         # Filter cache by active UI filters (YOUR FIX!)
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )
        
#         # NO Stage 2 for keyword path - use authority_score ordering from Stage 1
        
#         # Paginate (YOUR FIX!)
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
        
#         # # Fetch full documents for this page
#         # t4 = time.time()
#         # page_ids = [item['id'] for item in page_items]
#         # results = fetch_full_documents(page_ids, query)
#         # times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
        
#         # times['total'] = round((time.time() - t0) * 1000, 2)
        
#         # print(f"⏱️ TIMING: {times}")
#         # print(f"🔍 Strategy: KEYWORD (Stage 1 Only) | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
        
#         # search_time = round(time.time() - t0, 3)
#         # Fetch full documents for this page
#         t4 = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
        
#         # =========================================================================
#         # RELATED SEARCHES (keyword-based fallback for KEYWORD PATH)
#         # =========================================================================
        
#         related_searches = []
        
#         try:
#             # Use existing keyword-based related searches (no embedding in KEYWORD PATH)
#             related_searches = get_related_searches(query, intent)
#             if related_searches:
#                 print(f"🔗 Related Searches (keyword): {[r.get('label', '')[:20] for r in related_searches]}")
#         except Exception as e:
#             print(f"⚠️ Related searches error: {e}")
#             related_searches = []
        
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: KEYWORD (Stage 1 Only) | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
        
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total_filtered,
#             'facet_total': facet_total,
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword_graph_filter',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             'data_type_facets': data_type_facets,
#             'category_facets': category_facets,
#             'schema_facets': schema_facets,
#             'related_searches': related_searches,
#             'facets': all_facets,
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
#             }
#         }
    
#     # # =========================================================================
#     # # SEMANTIC PATH (alt_mode='y') - Full Staged Retrieval
#     # # =========================================================================
    
#     # print(f"🔬 SEMANTIC PATH (Staged Retrieval): '{query}' (alt_mode={alt_mode})")
    
#     # # Run word discovery and embedding generation IN PARALLEL
#     # t1 = time.time()
#     # discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     # times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # corrected_query = discovery.get('corrected_query', query)
#     # valid_terms = get_filter_terms_from_discovery(discovery)
#     # unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     # semantic_enabled = query_embedding is not None
#     # intent = detect_query_intent(query, pos_tags)
    
#     # # Generate cache key (includes discovery filters/locations)
#     # cache_key = _generate_cache_key(
#     #     corrected_query, 
#     #     'semantic',
#     #     discovery.get('filters', []),
#     #     discovery.get('locations', [])
#     # )
    
#     # # Check cache for Stage 1 candidates
#     # cached_data = _get_cached_results(cache_key)
    
#     # if cached_data:
#     #     print(f"✅ Cache HIT: {len(cached_data)} candidates")
#     #     all_results = cached_data
#     #     times['cache'] = 'hit'
#     # else:
#     #     print(f"❌ Cache MISS: Running Stage 1 (Graph Filter)...")
#     #     t2 = time.time()
        
#     #     # STAGE 1: Graph Filter - Candidate Generation
#     #     # Uses keyword/entity fields for FILTERING only (not ranking)
#     #     all_results = fetch_candidate_ids_graph_filter(
#     #         query=corrected_query,
#     #         discovery=discovery,
#     #         filters=filters,
#     #         max_results=MAX_CACHED_RESULTS
#     #     )
#     #     times['stage1_graph_filter'] = round((time.time() - t2) * 1000, 2)
        
#     #     # Cache the candidates (with metadata for facets)
#     #     if all_results:
#     #         _set_cached_results(cache_key, all_results)
#     #     print(f"📦 Cached {len(all_results)} candidates")
#     #     times['cache'] = 'miss'
    
#     # # Count facets FROM CACHE (always accurate - YOUR FIX!)
#     # t3 = time.time()
#     # all_facets = count_facets_from_cache(all_results)
#     # times['count_facets'] = round((time.time() - t3) * 1000, 2)
    
#     # data_type_facets = all_facets.get('data_type', [])
#     # category_facets = all_facets.get('category', [])
#     # schema_facets = all_facets.get('schema', [])
#     # facet_total = len(all_results)
    
#     # print(f"📊 Facets from cache: {[(f['value'], f['count']) for f in data_type_facets]}")
    
#     # # Filter cache by active UI filters (YOUR FIX!)
#     # filtered_results = filter_cached_results(
#     #     all_results,
#     #     data_type=active_data_type,
#     #     category=active_category,
#     #     schema=active_schema
#     # )
    
#     # # =========================================================================
#     # # STAGE 2: Semantic Rerank - Pure Vector Ranking
#     # # =========================================================================
    
#     # if semantic_enabled and filtered_results:
#     #     t_rerank = time.time()
        
#     #     # Get IDs to rerank
#     #     candidate_ids = [item['id'] for item in filtered_results]
        
#     #     # Run PURE vector search on candidates only
#     #     reranked = semantic_rerank_candidates(
#     #         candidate_ids=candidate_ids,
#     #         query_embedding=query_embedding,
#     #         max_to_rerank=500  # Rerank top 500 for performance
#     #     )
        
#     #     # Apply semantic ranking to the filtered results
#     #     filtered_results = apply_semantic_ranking_to_cache(filtered_results, reranked)
        
#     #     times['stage2_semantic_rerank'] = round((time.time() - t_rerank) * 1000, 2)
#     # else:
#     #     print(f"⚠️ Skipping Stage 2: semantic_enabled={semantic_enabled}, filtered_count={len(filtered_results)}")
    
#     # # Paginate from (now semantically-ranked) filtered results (YOUR FIX!)
#     # page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
#     # # Fetch full documents for this page
#     # t4 = time.time()
#     # page_ids = [item['id'] for item in page_items]
#     # results = fetch_full_documents(page_ids, query)
#     # times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
    
#     # times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # actual_strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
#     # print(f"⏱️ TIMING: {times}")
#     # print(f"🔍 Strategy: {actual_strategy.upper()} | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
#     # search_time = round(time.time() - t0, 3)
    
#     # return {
#     #     'query': query,
#     #     'corrected_query': corrected_query,
#     #     'intent': intent,
#     #     'results': results,
#     #     'total': total_filtered,
#     #     'facet_total': facet_total,
#     #     'page': page,
#     #     'per_page': per_page,
#     #     'search_time': search_time,
#     #     'session_id': session_id,
#     #     'semantic_enabled': semantic_enabled,
#     #     'search_strategy': actual_strategy,
#     #     'alt_mode': alt_mode,
#     #     'skip_embedding': skip_embedding,
#     #     'search_source': search_source,
#     #     'valid_terms': valid_terms,
#     #     'related_searches': related_searches,
#     #     'unknown_terms': unknown_terms,
#     #     'data_type_facets': data_type_facets,
#     #     'category_facets': category_facets,
#     #     'schema_facets': schema_facets,
#     #     'facets': all_facets,
#     #     'word_discovery': {
#     #         'valid_count': discovery.get('valid_count', 0),
#     #         'unknown_count': discovery.get('unknown_count', 0),
#     #         'corrections': discovery.get('corrections', []),
#     #         'filters': discovery.get('filters', []),
#     #         'locations': discovery.get('locations', []),
#     #         'sort': discovery.get('sort'),
#     #         'total_score': discovery.get('total_score', 0),
#     #         'average_score': discovery.get('average_score', 0),
#     #         'max_score': discovery.get('max_score', 0),
#     #     },
#     #     'timings': times,
#     #     'filters_applied': {
#     #         'data_type': active_data_type,
#     #         'category': active_category,
#     #         'schema': active_schema,
#     #         'graph_filters': discovery.get('filters', []),
#     #         'graph_locations': discovery.get('locations', []),
#     #         'graph_sort': discovery.get('sort'),
#     #     }
#     # }



# # =========================================================================
#     # SEMANTIC PATH (alt_mode='y') - Full Staged Retrieval
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH (Staged Retrieval): '{query}' (alt_mode={alt_mode})")
    
#     # Run word discovery and embedding generation IN PARALLEL
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     valid_terms = get_filter_terms_from_discovery(discovery)
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     semantic_enabled = query_embedding is not None
#     intent = detect_query_intent(query, pos_tags)
    
#     # Generate cache key (includes discovery filters/locations)
#     cache_key = _generate_cache_key(
#         corrected_query, 
#         'semantic',
#         discovery.get('filters', []),
#         discovery.get('locations', [])
#     )
    
#     # Check cache for Stage 1 candidates
#     cached_data = _get_cached_results(cache_key)
    
#     if cached_data:
#         print(f"✅ Cache HIT: {len(cached_data)} candidates")
#         all_results = cached_data
#         times['cache'] = 'hit'
#     else:
#         print(f"❌ Cache MISS: Running Stage 1 (Graph Filter)...")
#         t2 = time.time()
        
#         # STAGE 1: Graph Filter - Candidate Generation
#         # Uses keyword/entity fields for FILTERING only (not ranking)
#         all_results = fetch_candidate_ids_graph_filter(
#             query=corrected_query,
#             discovery=discovery,
#             filters=filters,
#             max_results=MAX_CACHED_RESULTS
#         )
#         times['stage1_graph_filter'] = round((time.time() - t2) * 1000, 2)
        
#         # Cache the candidates (with metadata for facets)
#         if all_results:
#             _set_cached_results(cache_key, all_results)
#         print(f"📦 Cached {len(all_results)} candidates")
#         times['cache'] = 'miss'
    
#     # Count facets FROM CACHE (always accurate - YOUR FIX!)
#     t3 = time.time()
#     all_facets = count_facets_from_cache(all_results)
#     times['count_facets'] = round((time.time() - t3) * 1000, 2)
    
#     data_type_facets = all_facets.get('data_type', [])
#     category_facets = all_facets.get('category', [])
#     schema_facets = all_facets.get('schema', [])
#     facet_total = len(all_results)
    
#     print(f"📊 Facets from cache: {[(f['value'], f['count']) for f in data_type_facets]}")
    
#     # Filter cache by active UI filters (YOUR FIX!)
#     filtered_results = filter_cached_results(
#         all_results,
#         data_type=active_data_type,
#         category=active_category,
#         schema=active_schema
#     )
    
#     # =========================================================================
#     # STAGE 2: Semantic Rerank - Pure Vector Ranking
#     # =========================================================================
    
#     if semantic_enabled and filtered_results:
#         t_rerank = time.time()
        
#         # Get IDs to rerank
#         candidate_ids = [item['id'] for item in filtered_results]
        
#         # Run PURE vector search on candidates only
#         reranked = semantic_rerank_candidates(
#             candidate_ids=candidate_ids,
#             query_embedding=query_embedding,
#             max_to_rerank=500  # Rerank top 500 for performance
#         )
        
#         # Apply semantic ranking to the filtered results
#         filtered_results = apply_semantic_ranking_to_cache(filtered_results, reranked)
        
#         times['stage2_semantic_rerank'] = round((time.time() - t_rerank) * 1000, 2)
#     else:
#         print(f"⚠️ Skipping Stage 2: semantic_enabled={semantic_enabled}, filtered_count={len(filtered_results)}")
    
#     # Paginate from (now semantically-ranked) filtered results (YOUR FIX!)
#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
#     # Fetch full documents for this page
#     t4 = time.time()
#     page_ids = [item['id'] for item in page_items]
#     results = fetch_full_documents(page_ids, query)
#     times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
    
#     # =========================================================================
#     # RELATED SEARCHES (from stored query embeddings)
#     # =========================================================================
    
#     related_searches = []
    
#     if semantic_enabled and query_embedding:
#         t_related = time.time()
        
#         try:
#             # Store this query's embedding for future related searches
#             store_query_embedding(corrected_query, query_embedding)
#             debug_related_searches(query_embedding, exclude_query=corrected_query)
            
#             # Get related searches from stored embeddings
#             related_searches = get_semantic_related_searches(
#                 embedding=query_embedding,
#                 limit=5,
#                 exclude_query=corrected_query
#             )
            
#             times['related_searches'] = round((time.time() - t_related) * 1000, 2)
            
#             if related_searches:
#                 print(f"🔗 Related Searches: {[r['query'][:30] for r in related_searches]}")
        
#         except Exception as e:
#             print(f"⚠️ Related searches error: {e}")
#             related_searches = []
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     actual_strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': total_filtered,
#         'facet_total': facet_total,
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': actual_strategy,
#         'alt_mode': alt_mode,
#         'skip_embedding': skip_embedding,
#         'search_source': search_source,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'related_searches': related_searches,
#         'data_type_facets': data_type_facets,
#         'category_facets': category_facets,
#         'schema_facets': schema_facets,
#         'facets': all_facets,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrections', []),
#             'filters': discovery.get('filters', []),
#             'locations': discovery.get('locations', []),
#             'sort': discovery.get('sort'),
#             'total_score': discovery.get('total_score', 0),
#             'average_score': discovery.get('average_score', 0),
#             'max_score': discovery.get('max_score', 0),
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type': active_data_type,
#             'category': active_category,
#             'schema': active_schema,
#             'graph_filters': discovery.get('filters', []),
#             'graph_locations': discovery.get('locations', []),
#             'graph_sort': discovery.get('sort'),
#         }
#     }


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
    Main entry point for search - v9.0 Staged Retrieval Strategy.
    
    STAGED RETRIEVAL ARCHITECTURE:
    
    1. Check cache for Stage 1 candidates (graph-filtered set)
    2. If cache miss: Run Stage 1 (Graph Filter) - keyword-based candidate generation
    3. Cache the candidates (with metadata for accurate facets)
    4. Count facets FROM CACHE (always accurate - YOUR FIX)
    5. Filter cache by data_type/category/schema if UI filters active
    6. Run Stage 2 (Semantic Rerank) - pure vector ranking on filtered set
    7. Paginate from reranked results
    8. Fetch full docs only for current page
    
    This ensures:
    - Graph structure does filtering (fast, leverages knowledge graph)
    - Vectors do ranking (pure semantic meaning)
    - Numbers are always consistent (facets from cache)
    - Performance is optimal (vector math on thousands, not millions)
    
    alt_mode:
        'n' = KEYWORD PATH (dropdown click) - no semantic reranking
        'y' = SEMANTIC PATH (typed freely) - full staged retrieval
    """
    import time
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
    # KEYWORD PATH (alt_mode='n') - No semantic reranking
    # =========================================================================
    
    is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
    if is_keyword_path:
        print(f"⚡ KEYWORD PATH (Stage 1 Only): '{query}' (alt_mode={alt_mode})")
        
        t1 = time.time()
        intent = detect_query_intent(query, pos_tags)
        
        # Generate cache key
        cache_key = _generate_cache_key(query, 'keyword', None, None)
        
        # Check cache for Stage 1 candidates
        cached_data = _get_cached_results(cache_key)
        
        if cached_data:
            print(f"✅ Cache HIT: {len(cached_data)} candidates")
            all_results = cached_data
            times['cache'] = 'hit'
        else:
            print(f"❌ Cache MISS: Running Stage 1 (Graph Filter)...")
            t2 = time.time()
            
            # STAGE 1: Graph Filter - Candidate Generation
            all_results = fetch_candidate_ids_graph_filter(
                query=query,
                discovery={},
                filters=filters,
                max_results=MAX_CACHED_RESULTS
            )
            times['stage1_graph_filter'] = round((time.time() - t2) * 1000, 2)
            
            # Cache the candidates (with metadata)
            if all_results:
                _set_cached_results(cache_key, all_results)
            print(f"📦 Cached {len(all_results)} candidates")
            times['cache'] = 'miss'
        
        # Count facets FROM CACHE (always accurate - YOUR FIX!)
        t3 = time.time()
        all_facets = count_facets_from_cache(all_results)
        times['count_facets'] = round((time.time() - t3) * 1000, 2)
        
        data_type_facets = all_facets.get('data_type', [])
        category_facets = all_facets.get('category', [])
        schema_facets = all_facets.get('schema', [])
        facet_total = len(all_results)
        
        print(f"📊 Facets from cache: {[(f['value'], f['count']) for f in data_type_facets]}")
        
        # Filter cache by active UI filters (YOUR FIX!)
        filtered_results = filter_cached_results(
            all_results,
            data_type=active_data_type,
            category=active_category,
            schema=active_schema
        )
        
        # NO Stage 2 for keyword path - use authority_score ordering from Stage 1
        
        # Paginate (YOUR FIX!)
        page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
        
        # Fetch full documents for this page
        t4 = time.time()
        page_ids = [item['id'] for item in page_items]
        results = fetch_full_documents(page_ids, query)
        times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
        
        # =========================================================================
        # RELATED SEARCHES (semantic lookup for KEYWORD PATH)
        # =========================================================================
        
        related_searches = []
        
        try:
            # Generate embedding just for related searches lookup
            kw_embedding = get_query_embedding(query)
            if kw_embedding:
                # Store this query for future related searches
                store_query_embedding(query, kw_embedding, result_count=total_filtered)
                
                # Get semantically similar queries that have results
                related_searches = get_related_searches(query, intent, embedding=kw_embedding)
                if related_searches:
                    print(f"🔗 Related Searches (keyword path): {[r.get('label', '')[:20] for r in related_searches]}")
        except Exception as e:
            print(f"⚠️ Related searches error: {e}")
            related_searches = []
        
        times['total'] = round((time.time() - t0) * 1000, 2)
        
        print(f"⏱️ TIMING: {times}")
        print(f"🔍 Strategy: KEYWORD (Stage 1 Only) | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
        
        search_time = round(time.time() - t0, 3)
        
        return {
            'query': query,
            'corrected_query': query,
            'intent': intent,
            'results': results,
            'total': total_filtered,
            'facet_total': facet_total,
            'page': page,
            'per_page': per_page,
            'search_time': search_time,
            'session_id': session_id,
            'semantic_enabled': False,
            'search_strategy': 'keyword_graph_filter',
            'alt_mode': alt_mode,
            'skip_embedding': True,
            'search_source': search_source or 'dropdown',
            'valid_terms': query.split(),
            'unknown_terms': [],
            'data_type_facets': data_type_facets,
            'category_facets': category_facets,
            'schema_facets': schema_facets,
            'related_searches': related_searches,
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
            }
        }
    
    # =========================================================================
    # SEMANTIC PATH (alt_mode='y') - Full Staged Retrieval
    # =========================================================================
    
    print(f"🔬 SEMANTIC PATH (Staged Retrieval): '{query}' (alt_mode={alt_mode})")
    
    # Run word discovery and embedding generation IN PARALLEL
    t1 = time.time()
    discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    print(f"🎯 DISCOVERY KEYS: {discovery.keys()}")  # ADD THIS
    print(f"🎯 HAS SIGNALS: {'signals' in discovery}")  # ADD THIS
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    print(f"🎯 SIGNALS: {discovery.get('signals', 'NOT FOUND')}")
    
    corrected_query = discovery.get('corrected_query', query)
    valid_terms = get_filter_terms_from_discovery(discovery)
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
    semantic_enabled = query_embedding is not None
    intent = detect_query_intent(query, pos_tags)
    
    # Generate cache key (includes discovery filters/locations)
    cache_key = _generate_cache_key(
        corrected_query, 
        'semantic',
        discovery.get('filters', []),
        discovery.get('locations', [])
    )
    
    # Check cache for Stage 1 candidates
    cached_data = _get_cached_results(cache_key)
    
    if cached_data:
        print(f"✅ Cache HIT: {len(cached_data)} candidates")
        all_results = cached_data
        times['cache'] = 'hit'
    else:
        print(f"❌ Cache MISS: Running Stage 1 (Graph Filter)...")
        t2 = time.time()
        
        # STAGE 1: Graph Filter - Candidate Generation
        # Uses keyword/entity fields for FILTERING only (not ranking)
        all_results = fetch_candidate_ids_graph_filter(
            query=corrected_query,
            discovery=discovery,
            filters=filters,
            max_results=MAX_CACHED_RESULTS
        )
        times['stage1_graph_filter'] = round((time.time() - t2) * 1000, 2)
        
        # Cache the candidates (with metadata for facets)
        if all_results:
            _set_cached_results(cache_key, all_results)
        print(f"📦 Cached {len(all_results)} candidates")
        times['cache'] = 'miss'
    
    # Count facets FROM CACHE (always accurate - YOUR FIX!)
    t3 = time.time()
    all_facets = count_facets_from_cache(all_results)
    times['count_facets'] = round((time.time() - t3) * 1000, 2)
    
    data_type_facets = all_facets.get('data_type', [])
    category_facets = all_facets.get('category', [])
    schema_facets = all_facets.get('schema', [])
    facet_total = len(all_results)
    
    print(f"📊 Facets from cache: {[(f['value'], f['count']) for f in data_type_facets]}")
    
    # Filter cache by active UI filters (YOUR FIX!)
    filtered_results = filter_cached_results(
        all_results,
        data_type=active_data_type,
        category=active_category,
        schema=active_schema
    )
    
    # =========================================================================
    # STAGE 2: Semantic Rerank - Pure Vector Ranking
    # =========================================================================
    
    if semantic_enabled and filtered_results:
        t_rerank = time.time()
        
        # Get IDs to rerank
        candidate_ids = [item['id'] for item in filtered_results]
        
        # Run PURE vector search on candidates only
        reranked = semantic_rerank_candidates(
            candidate_ids=candidate_ids,
            query_embedding=query_embedding,
            max_to_rerank=500  # Rerank top 500 for performance
        )
        
        # Apply semantic ranking to the filtered results
        filtered_results = apply_semantic_ranking_to_cache(filtered_results, reranked)
        
        times['stage2_semantic_rerank'] = round((time.time() - t_rerank) * 1000, 2)
    else:
        print(f"⚠️ Skipping Stage 2: semantic_enabled={semantic_enabled}, filtered_count={len(filtered_results)}")
    
    # Paginate from (now semantically-ranked) filtered results (YOUR FIX!)
    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
    # Fetch full documents for this page
    t4 = time.time()
    page_ids = [item['id'] for item in page_items]
    results = fetch_full_documents(page_ids, query)
    times['fetch_docs'] = round((time.time() - t4) * 1000, 2)
    
    # =========================================================================
    # RELATED SEARCHES (from stored query embeddings)
    # =========================================================================
    
    related_searches = []
    
    if semantic_enabled and query_embedding:
        t_related = time.time()
        
        try:
            # Store this query's embedding for future related searches (WITH RESULT COUNT)
            store_query_embedding(corrected_query, query_embedding, result_count=total_filtered)
            debug_related_searches(query_embedding, exclude_query=corrected_query)
            
            # Get related searches from stored embeddings
            related_searches = get_related_searches(corrected_query, intent, embedding=query_embedding)
            
            times['related_searches'] = round((time.time() - t_related) * 1000, 2)
            
            if related_searches:
                print(f"🔗 Related Searches: {[r['query'][:30] for r in related_searches]}")
        
        except Exception as e:
            print(f"⚠️ Related searches error: {e}")
            related_searches = []
    
    times['total'] = round((time.time() - t0) * 1000, 2)
    
    actual_strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
    print(f"⏱️ TIMING: {times}")
    print(f"🔍 Strategy: {actual_strategy.upper()} | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
    search_time = round(time.time() - t0, 3)
    
    return {
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
        'results': results,
        'total': total_filtered,
        'facet_total': facet_total,
        'page': page,
        'per_page': per_page,
        'search_time': search_time,
        'session_id': session_id,
        'semantic_enabled': semantic_enabled,
        'search_strategy': actual_strategy,
        'alt_mode': alt_mode,
        'skip_embedding': skip_embedding,
        'search_source': search_source,
        'valid_terms': valid_terms,
        'unknown_terms': unknown_terms,
        'related_searches': related_searches,
        'data_type_facets': data_type_facets,
        'category_facets': category_facets,
        'schema_facets': schema_facets,
        'facets': all_facets,
        'word_discovery': {
            'valid_count': discovery.get('valid_count', 0),
            'unknown_count': discovery.get('unknown_count', 0),
            'corrections': discovery.get('corrections', []),
            'filters': discovery.get('filters', []),
            'locations': discovery.get('locations', []),
            'sort': discovery.get('sort'),
            'total_score': discovery.get('total_score', 0),
            'average_score': discovery.get('average_score', 0),
            'max_score': discovery.get('max_score', 0),
        },
        'timings': times,
        'filters_applied': {
            'data_type': active_data_type,
            'category': active_category,
            'schema': active_schema,
            'graph_filters': discovery.get('filters', []),
            'graph_locations': discovery.get('locations', []),
            'graph_sort': discovery.get('sort'),
        }
    }
# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def quick_search(query: str, limit: int = 10) -> List[Dict]:
    """Quick semantic search for autocomplete."""
    query_embedding = get_query_embedding(query)
    
    if not query_embedding:
        params = {
            'q': query,
            'query_by': 'document_title,key_facts',
            'per_page': limit,
            'include_fields': 'document_uuid,document_title,document_url,key_facts'
        }
        try:
            response = client.collections[COLLECTION_NAME].documents.search(params)
            return [hit['document'] for hit in response.get('hits', [])]
        except:
            return []
    
    embedding_str = ','.join(str(x) for x in query_embedding)
    params = {
        'q': '*',
        'vector_query': f"embedding:([{embedding_str}], k:{limit}, alpha:1.0)",
        'per_page': limit,
        'exclude_fields': 'embedding',
        'include_fields': 'document_uuid,document_title,document_url,key_facts'
    }
    
    response = execute_search_multi(params)
    return [hit['document'] for hit in response.get('hits', [])]


def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
    """Find documents similar to a given document."""
    try:
        doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
        results = []
        semantic_uuid = doc.get('semantic_uuid')
        
        if semantic_uuid and '-' in semantic_uuid:
            parts = semantic_uuid.split('-')
            if len(parts) >= 2:
                prefix = parts[0]
                cluster = parts[1]
                
                if cluster != '00':
                    try:
                        params = {
                            'q': '*',
                            'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
                            'per_page': limit,
                            'exclude_fields': 'embedding'
                        }
                        
                        response = execute_search_multi(params)
                        results = [hit['document'] for hit in response.get('hits', [])]
                        
                        if len(results) >= limit:
                            return results[:limit]
                    except Exception as e:
                        print(f"⚠️ Cluster search failed: {e}")
        
        embedding = doc.get('embedding')
        
        if embedding and len(results) < limit:
            remaining = limit - len(results)
            existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
            exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
            exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
            embedding_str = ','.join(str(x) for x in embedding)
            params = {
                'q': '*',
                'vector_query': f"embedding:([{embedding_str}], k:{remaining + 1})",
                'per_page': remaining,
                'exclude_fields': 'embedding',
            }
            
            if exclude_filter:
                params['filter_by'] = exclude_filter
            
            response = execute_search_multi(params)
            vector_results = [hit['document'] for hit in response.get('hits', [])]
            results.extend(vector_results)
        
        return results[:limit]
    
    except Exception as e:
        print(f"Error finding similar documents: {e}")
        return []


def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
    """Find all documents in the same semantic cluster."""
    if not semantic_uuid or '-' not in semantic_uuid:
        return []
    
    parts = semantic_uuid.split('-')
    if len(parts) < 2:
        return []
    
    prefix = parts[0]
    cluster = parts[1]
    
    try:
        params = {
            'q': '*',
            'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
            'per_page': limit,
            'exclude_fields': 'embedding',
            'sort_by': 'semantic_uuid:asc'
        }
        
        response = execute_search_multi(params)
        return [hit['document'] for hit in response.get('hits', [])]
    
    except Exception as e:
        print(f"Error finding cluster documents: {e}")
        return []


# ============================================================================
# HELPER FUNCTIONS (Required by views.py)
# ============================================================================

def get_facets(query: str) -> dict:
    """Returns available filter options based on result set."""
    search_params = {
        'q': query,
        'query_by': ','.join(SEARCH_FIELDS),
        'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
        'max_facet_values': 20,
        'per_page': 0
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


def get_tab_facets(query: str) -> dict:
    """DEPRECATED: Use facets returned by execute_full_search instead."""
    search_params = {
        'q': query if query else '*',
        'query_by': ','.join(SEARCH_FIELDS),
        'facet_by': 'document_data_type,document_category,document_schema',
        'max_facet_values': 20,
        'per_page': 0
    }
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(search_params)
        facets = {'data_type': [], 'category': [], 'schema': []}
        
        for facet in response.get('facet_counts', []):
            field = facet.get('field_name', '')
            counts = facet.get('counts', [])
            
            if field == 'document_data_type':
                facets['data_type'] = [
                    {'value': c.get('value', ''), 'count': c.get('count', 0), 
                     'label': DATA_TYPE_LABELS.get(c.get('value', ''), c.get('value', '').title())}
                    for c in counts if c.get('value') and c.get('count', 0) > 0
                ]
            elif field == 'document_category':
                facets['category'] = [
                    {'value': c.get('value', ''), 'count': c.get('count', 0),
                     'label': CATEGORY_LABELS.get(c.get('value', ''), c.get('value', '').replace('_', ' ').title())}
                    for c in counts if c.get('value') and c.get('count', 0) > 0
                ]
            elif field == 'document_schema':
                facets['schema'] = [
                    {'value': c.get('value', ''), 'count': c.get('count', 0), 'label': c.get('value', '')}
                    for c in counts if c.get('value') and c.get('count', 0) > 0
                ]
        
        return facets
    except Exception as e:
        print(f"Error getting tab facets: {e}")
        return {'data_type': [], 'category': [], 'schema': []}


# def get_related_searches(query: str, intent: str) -> list:
#     """Returns 'People also search for' suggestions."""
#     search_params = {
#         'q': query,
#         'query_by': 'primary_keywords,keywords,key_facts',
#         'per_page': 10,
#         'include_fields': 'primary_keywords,keywords,key_facts'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         all_keywords = set()
#         query_words = set(query.lower().split())
        
#         for hit in response.get('hits', []):
#             doc = hit.get('document', {})
#             for kw in doc.get('primary_keywords', []):
#                 if kw.lower() not in query_words:
#                     all_keywords.add(kw)
#             for kw in doc.get('keywords', [])[:5]:
#                 if kw.lower() not in query_words:
#                     all_keywords.add(kw)
#             for fact in doc.get('key_facts', [])[:3]:
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w.lower() not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []

def get_related_searches(query: str, intent: str = None, embedding: List[float] = None) -> list:
    """
    Returns related searches from real user queries.
    
    Uses semantic similarity from stored query embeddings.
    Only returns queries that have verified results.
    """
    from .cached_embedding_related_search import get_related_searches as get_semantic_related
    
    # If no embedding provided, generate one
    if not embedding:
        try:
            embedding = get_query_embedding(query)  # Your embedding function
        except Exception:
            return []
    
    if not embedding:
        return []
    
    try:
        # Get semantically similar queries that have results
        related = get_semantic_related(
            embedding=embedding,
            limit=6,
            exclude_query=query,
            min_results=1  # Only queries with verified results
        )
        
        # Format for template
        return [{'query': r['query'], 'label': r['query']} for r in related]
    
    except Exception:
        return []


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
    """Simple Typesense search wrapper for views."""
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
        response = client.collections[COLLECTION_NAME].documents.search(params)
        return response
    except Exception as e:
        print(f"❌ typesense_search error: {e}")
        return {'hits': [], 'found': 0, 'error': str(e)}


def get_featured_result(query: str, intent: str, results: list) -> dict:
    """Returns featured content: knowledge panel or featured snippet."""
    if not results:
        return None
    
    top_result = results[0]
    
    if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        if intent == 'person' and top_result.get('data_type') == 'person':
            return {'type': 'person_card', 'data': top_result}
        if intent == 'location':
            return {'type': 'place_card', 'data': top_result}
        return {
            'type': 'featured_snippet',
            'title': top_result.get('title'),
            'snippet': top_result.get('summary', ''),
            'key_facts': top_result.get('key_facts', [])[:3],
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
    page: int,
    semantic_enabled: bool = False,
    semantic_boost: float = 0.0,
    alt_mode: str = 'n'
):
    """Logs search event for analytics."""
    pass


# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

def clear_search_cache():
    """Clear all cached search results."""
    global _result_cache
    with _cache_lock:
        _result_cache = {}
    print("🧹 Search cache cleared")


def get_cache_stats() -> Dict:
    """Get cache statistics."""
    with _cache_lock:
        now = datetime.now()
        stats = {
            'total_entries': len(_result_cache),
            'max_entries': MAX_CACHE_ENTRIES,
            'ttl_seconds': CACHE_TTL_SECONDS,
            'entries': []
        }
        
        for key, entry in _result_cache.items():
            age = (now - entry['timestamp']).total_seconds()
            stats['entries'].append({
                'key': key[:16] + '...',
                'count': len(entry['data']),
                'age_seconds': round(age, 1),
                'expires_in': round(CACHE_TTL_SECONDS - age, 1)
            })
        
        return stats