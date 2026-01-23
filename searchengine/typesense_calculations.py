# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# """


# import typesense
# from typing import Dict, List, Tuple, Optional
# import re
# from datetime import datetime
# from decouple import config

# import re

# # === PRE-COMPILED REGEX PATTERNS ===
# # Compiled once at module load, reused on every search

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # HELPER FUNCTIONS PART 0

# # Add these to your typesense_calculations.py

# def get_facets(query: str) -> dict:
#     """
#     Returns available filter options based on result set.
    
#     Why: Shows user what filters are available (like "Videos (23)", "Articles (156)")
#     """
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0  # We only want facets, not results
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """
#     Returns "People also search for" suggestions.
    
#     Why: Helps users explore related topics, increases engagement.
#     """
#     # Option 1: Query-based suggestions from your keyword data
#     # Option 2: Use semantic similarity
#     # Option 3: Use search logs (what did others search after this?)
    
#     # Simple implementation: search for documents with similar keywords
#     # and extract their primary_keywords
    
#     search_params = {
#         'q': query,
#         'query_by': 'primary_keywords,keywords',
#         'per_page': 10,
#         'include_fields': 'primary_keywords,keywords'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         # Collect all keywords from results
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
        
#         # Return top 6 as related searches
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """
#     Returns featured content: knowledge panel, direct answer, or top result highlight.
    
#     Why: Like Google's featured snippets - gives quick answer without clicking.
#     """
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     # High authority + high relevance = feature it
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         # Person intent: show person card
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         # Location intent: show place card
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         # General: show featured snippet
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int
# ):
#     """
#     Logs search event for analytics and personalization.
    
#     Why: Track what users search, improve results over time, personalization.
#     """
#     # Implement based on your logging system
#     # Options: database, Redis, analytics service, log file
    
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0
#     }
    
#     # Example: save to database or send to analytics
#     # SearchLog.objects.create(**event)
#     # analytics.track('search', event)
    
#     pass  # Replace with your implementation





# # === CLIENT SETUP  PART 1  ===
# client = typesense.Client({
#     'api_key':config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host': config('TYPESENSE_HOST'),
#         'port': config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL')
#     }],
#     'connection_timeout_seconds': 5
# })

# COLLECTION_NAME = 'documents'


# # === FIELD CONFIGURATION  PART 2 ===
# # These define which fields to search and their relative importance

# SEARCH_FIELDS = [
#     'document_title',
#     'primary_keywords', 
#     'keywords',
#     'semantic_keywords',
#     'document_summary',
#     'key_passages',
#     'entity_names'
# ]

# # Default weights when no specific intent detected
# DEFAULT_WEIGHTS = [5, 4, 3, 2, 2, 1, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general': [5, 4, 3, 2, 2, 1, 2],
#     'location': [4, 3, 3, 2, 2, 1, 4],      # boost entity_names for places
#     'historical': [4, 4, 4, 3, 2, 1, 3],    # boost keywords for era terms
#     'product': [5, 4, 3, 2, 2, 1, 2],       # title-heavy for products
#     'person': [5, 3, 3, 2, 2, 1, 5],        # boost entity_names for people
#     'media': [5, 4, 3, 3, 2, 1, 3],         # boost semantic for genre matching
# }

# # Authority scores by source (used for boosting)
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 85,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# #  === INTENT DETECTION PART 3 ===
# # Determines what type of search the user wants

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     Uses pre-compiled patterns for performance.
#     """
#     query_lower = query.lower()
    
#     # Location check
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     # Historical check
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     # Product check
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     # Person check
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     # Media check
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Uses pre-compiled patterns."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Uses pre-compiled patterns."""
#     query_lower = query.lower()
    
#     # Specific decade
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     # Specific century
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     # Era keywords (no regex needed - simple dict lookup)
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """
#     Detects if user wants specific content type.
#     "africa documentary" -> "video"
    
#     Why: Allows filtering to specific document_data_type without explicit filter.
#     """
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # === QUERY BUILDING  PART 4 ===
# # Constructs the Typesense search parameters

# def build_query_weights(intent: str) -> str:
#     """
#     Returns comma-separated weights string for query_by_weights.
    
#     Why: Typesense needs weights as a string like "5,4,3,2,2,1,2"
#     """
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string.
    
#     Why: Combines explicit user filters with auto-detected filters from intent.
#     Returns string like: "document_category:=geography && time_period_start:>=1800"
#     """
#     conditions = []
    
#     # User-provided explicit filters
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     # Auto-detected time period filter
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     # Auto-detected location filter
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     # Auto-detected content type filter
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     # Always exclude inactive documents
#     conditions.append("status:=active")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """
#     Builds Typesense sort_by string.
    
#     Why: Different intents need different sort priorities.
#     - Location queries: geo distance first
#     - General: text relevance + authority
#     - Historical: time relevance
#     """
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True  # ← Add here too
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
    
#     Why: Single function that assembles all search options.
#     This is what gets passed to client.collections['documents'].documents.search()
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#     }
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params

# ######################################=== RESULT PROCESSING  PART 5 ===###################################################################

# # Transforms raw Typesense hits into clean response format

# def calculate_final_score(hit: Dict) -> float:
#     """
#     Combines Typesense text_match score with authority and freshness.
    
#     Why: Typesense's default scoring doesn't consider your custom authority_score.
#     This re-ranks results to favor trusted sources.
#     """
#     # Get Typesense's relevance score (0-1 normalized)
#     text_score = hit.get('text_match', 0) / 100000000  # Typesense uses large numbers
    
#     # Get authority score (0-100, normalize to 0-1)
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Get freshness score based on published_date
#     freshness = 0.5  # Default middle score
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination
#     final_score = (text_score * 0.5) + (authority * 0.35) + (freshness * 0.15)
    
#     return round(final_score, 4)


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """
#     Removes near-duplicate documents using cluster_uuid.
#     Keeps highest-scoring document per cluster.
    
#     Why: Wikipedia and Britannica articles on "Africa" shouldn't both appear
#     at positions 1 and 2. Keep the best one, group others.
#     """
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             # Add to duplicates group
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     # Attach duplicates to their primary result
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict) -> Dict:
#     """
#     Transforms a single Typesense hit into clean response format.
    
#     Why: Decouples your API response format from Typesense's internal format.
#     Frontend only sees clean, consistent data structure.
#     """
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit),
#         'related_sources': []  # Populated by deduplicate_by_cluster
#     }


# def process_results(raw_response: Dict) -> List[Dict]:
#     """
#     Processes full Typesense response into clean result list.
    
#     Why: Handles the full pipeline: format -> score -> sort -> dedupe
#     """
#     hits = raw_response.get('hits', [])
    
#     # Format each hit
#     results = [format_result(hit) for hit in hits]
    
#     # Re-sort by final score (includes authority)
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     # Deduplicate by cluster
#     # results = deduplicate_by_cluster(results)
    
#     return results


# ####################################################################  === MAIN ORCHESTRATOR PART 6 ===
# # Single entry point for views.py

# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution. Calls Typesense directly.
    
#     Why: Separated from full_search for cases where you need raw results.
#     """
#     options = options or {}
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True  # ← Add this line
# ) -> Dict:
#     """
#     Main entry point for search. Call this from views.py.
    
#     Orchestrates:
#     1. Intent detection
#     2. Parameter building
#     3. Typesense search
#     4. Result processing
#     5. Response formatting
    
#     Why: Views.py stays clean. All search logic lives here.
#     """
#     import time
#     start_time = time.time()
    
#     # Step 1: Detect intent
#     intent = detect_query_intent(query, pos_tags)
    
#     # Step 2: Build search parameters
#     search_params = build_search_params(
#     query=query,
#     intent=intent,
#     filters=filters,
#     page=page,
#     per_page=per_page,
#     user_location=user_location,
#     safe_search=safe_search  # ← Pass it through
# )
    
#     # Step 3: Execute search
#     raw_response = execute_search(query, search_params)
    
#     # Step 4: Process results
#     results = process_results(raw_response)
    
#     # Step 5: Build final response
#     search_time = round(time.time() - start_time, 3)
    
#     return {
#         'query': query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# """


# import typesense
# from typing import Dict, List, Tuple, Optional
# import re
# from datetime import datetime
# from decouple import config

# import re

# # === PRE-COMPILED REGEX PATTERNS ===
# # Compiled once at module load, reused on every search

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # HELPER FUNCTIONS PART 0

# # Add these to your typesense_calculations.py

# def get_facets(query: str) -> dict:
#     """
#     Returns available filter options based on result set.
    
#     Why: Shows user what filters are available (like "Videos (23)", "Articles (156)")
#     """
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0  # We only want facets, not results
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_related_searches(query: str, intent: str) -> list:
#     """
#     Returns "People also search for" suggestions.
    
#     Why: Helps users explore related topics, increases engagement.
#     """
#     # Option 1: Query-based suggestions from your keyword data
#     # Option 2: Use semantic similarity
#     # Option 3: Use search logs (what did others search after this?)
    
#     # Simple implementation: search for documents with similar keywords
#     # and extract their primary_keywords
    
#     search_params = {
#         'q': query,
#         'query_by': 'primary_keywords,keywords',
#         'per_page': 10,
#         'include_fields': 'primary_keywords,keywords'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         # Collect all keywords from results
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
        
#         # Return top 6 as related searches
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """
#     Returns featured content: knowledge panel, direct answer, or top result highlight.
    
#     Why: Like Google's featured snippets - gives quick answer without clicking.
#     """
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     # High authority + high relevance = feature it
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         # Person intent: show person card
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         # Location intent: show place card
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         # General: show featured snippet
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int
# ):
#     """
#     Logs search event for analytics and personalization.
    
#     Why: Track what users search, improve results over time, personalization.
#     """
#     # Implement based on your logging system
#     # Options: database, Redis, analytics service, log file
    
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0
#     }
    
#     # Example: save to database or send to analytics
#     # SearchLog.objects.create(**event)
#     # analytics.track('search', event)
    
#     pass  # Replace with your implementation





# # === CLIENT SETUP  PART 1  ===
# client = typesense.Client({
#     'api_key':config('TYPESENSE_API_KEY'),
#     'nodes': [{
#         'host': config('TYPESENSE_HOST'),
#         'port': config('TYPESENSE_PORT'),
#         'protocol': config('TYPESENSE_PROTOCOL')
#     }],
#     'connection_timeout_seconds': 5
# })

# COLLECTION_NAME = 'documents'


# # === FIELD CONFIGURATION  PART 2 ===
# # These define which fields to search and their relative importance

# SEARCH_FIELDS = [
#     'document_title',
#     'primary_keywords', 
#     'keywords',
#     'semantic_keywords',
#     'document_summary',
#     'key_passages',
#     'entity_names'
# ]

# # Default weights when no specific intent detected
# DEFAULT_WEIGHTS = [5, 4, 3, 2, 2, 1, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general': [5, 4, 3, 2, 2, 1, 2],
#     'location': [4, 3, 3, 2, 2, 1, 4],      # boost entity_names for places
#     'historical': [4, 4, 4, 3, 2, 1, 3],    # boost keywords for era terms
#     'product': [5, 4, 3, 2, 2, 1, 2],       # title-heavy for products
#     'person': [5, 3, 3, 2, 2, 1, 5],        # boost entity_names for people
#     'media': [5, 4, 3, 3, 2, 1, 3],         # boost semantic for genre matching
# }

# # Authority scores by source (used for boosting)
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 85,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# #  === INTENT DETECTION PART 3 ===
# # Determines what type of search the user wants

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     Uses pre-compiled patterns for performance.
#     """
#     query_lower = query.lower()
    
#     # Location check
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     # Historical check
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     # Product check
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     # Person check
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     # Media check
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Uses pre-compiled patterns."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Uses pre-compiled patterns."""
#     query_lower = query.lower()
    
#     # Specific decade
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     # Specific century
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     # Era keywords (no regex needed - simple dict lookup)
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """
#     Detects if user wants specific content type.
#     "africa documentary" -> "video"
    
#     Why: Allows filtering to specific document_data_type without explicit filter.
#     """
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # === QUERY BUILDING  PART 4 ===
# # Constructs the Typesense search parameters

# def build_query_weights(intent: str) -> str:
#     """
#     Returns comma-separated weights string for query_by_weights.
    
#     Why: Typesense needs weights as a string like "5,4,3,2,2,1,2"
#     """
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string.
    
#     Why: Combines explicit user filters with auto-detected filters from intent.
#     Returns string like: "document_category:=geography && time_period_start:>=1800"
#     """
#     conditions = []
    
#     # User-provided explicit filters
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     # Auto-detected time period filter
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     # Auto-detected location filter
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     # Auto-detected content type filter
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     # REMOVED: status:=active filter that was causing zero results
#     # If you need this filter, make sure your documents have status="active"
#     # conditions.append("status:=active")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """
#     Builds Typesense sort_by string.
    
#     Why: Different intents need different sort priorities.
#     - Location queries: geo distance first
#     - General: text relevance + authority
#     - Historical: time relevance
#     """
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
    
#     Why: Single function that assembles all search options.
#     This is what gets passed to client.collections['documents'].documents.search()
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#     }
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params

# ######################################=== RESULT PROCESSING  PART 5 ===###################################################################

# # Transforms raw Typesense hits into clean response format

# def calculate_final_score(hit: Dict) -> float:
#     """
#     Combines Typesense text_match score with authority and freshness.
    
#     Why: Typesense's default scoring doesn't consider your custom authority_score.
#     This re-ranks results to favor trusted sources.
#     """
#     # Get Typesense's relevance score (0-1 normalized)
#     text_score = hit.get('text_match', 0) / 100000000  # Typesense uses large numbers
    
#     # Get authority score (0-100, normalize to 0-1)
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Get freshness score based on published_date
#     freshness = 0.5  # Default middle score
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination
#     final_score = (text_score * 0.5) + (authority * 0.35) + (freshness * 0.15)
    
#     return round(final_score, 4)


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """
#     Removes near-duplicate documents using cluster_uuid.
#     Keeps highest-scoring document per cluster.
    
#     Why: Wikipedia and Britannica articles on "Africa" shouldn't both appear
#     at positions 1 and 2. Keep the best one, group others.
#     """
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             # Add to duplicates group
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     # Attach duplicates to their primary result
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict) -> Dict:
#     """
#     Transforms a single Typesense hit into clean response format.
    
#     Why: Decouples your API response format from Typesense's internal format.
#     Frontend only sees clean, consistent data structure.
#     """
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),  # Added for template compatibility
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),  # Changed from published_date to date for template
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit),
#         'related_sources': []  # Populated by deduplicate_by_cluster
#     }


# def process_results(raw_response: Dict) -> List[Dict]:
#     """
#     Processes full Typesense response into clean result list.
    
#     Why: Handles the full pipeline: format -> score -> sort -> dedupe
#     """
#     hits = raw_response.get('hits', [])
    
#     # Format each hit
#     results = [format_result(hit) for hit in hits]
    
#     # Re-sort by final score (includes authority)
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     # Deduplicate by cluster
#     # results = deduplicate_by_cluster(results)
    
#     return results


# ####################################################################  === MAIN ORCHESTRATOR PART 6 ===
# # Single entry point for views.py

# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution. Calls Typesense directly.
    
#     Why: Separated from full_search for cases where you need raw results.
#     """
#     options = options or {}
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True
# ) -> Dict:
#     """
#     Main entry point for search. Call this from views.py.
    
#     Orchestrates:
#     1. Intent detection
#     2. Parameter building
#     3. Typesense search
#     4. Result processing
#     5. Response formatting
    
#     Why: Views.py stays clean. All search logic lives here.
#     """
#     import time
#     start_time = time.time()
    
#     # Step 1: Detect intent
#     intent = detect_query_intent(query, pos_tags)
    
#     # Step 2: Build search parameters
#     search_params = build_search_params(
#         query=query,
#         intent=intent,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         safe_search=safe_search
#     )
    
#     # DEBUG LOGGING - remove in production
#     print("=" * 60)
#     print("TYPESENSE DEBUG")
#     print("=" * 60)
#     print(f"Query: '{query}'")
#     print(f"Intent: {intent}")
#     print(f"Search params: {search_params}")
#     print("=" * 60)
    
#     # Step 3: Execute search
#     raw_response = execute_search(query, search_params)
    
#     # DEBUG LOGGING - remove in production
#     print(f"Typesense found: {raw_response.get('found', 0)} documents")
#     print(f"Hits returned: {len(raw_response.get('hits', []))}")
#     if raw_response.get('hits'):
#         first_doc = raw_response['hits'][0].get('document', {})
#         print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
#         print(f"First hit URL: {first_doc.get('document_url', 'NO URL')}")
#     if raw_response.get('error'):
#         print(f"ERROR: {raw_response.get('error')}")
#     print("=" * 60)
    
#     # Step 4: Process results
#     results = process_results(raw_response)
    
#     # DEBUG LOGGING - remove in production
#     print(f"Processed results count: {len(results)}")
#     if results:
#         print(f"First processed result keys: {list(results[0].keys())}")
#         print(f"First result title: {results[0].get('title')}")
#         print(f"First result summary: {results[0].get('summary', '')[:100]}...")
#     print("=" * 60)
    
#     # Step 5: Build final response
#     search_time = round(time.time() - start_time, 3)
    
#     return {
#         'query': query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# - HYBRID SEARCH (text + semantic/vector)

# Updated: Added semantic vector search with smart weighting
# """

# import typesense
# from typing import Dict, List, Tuple, Optional
# import re
# from datetime import datetime
# from decouple import config

# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded for semantic search
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False  # Track if model failed to load

# def get_embedding_model():
#     """
#     Lazy loads the embedding model on first use.
    
#     Why lazy loading:
#     - Model uses ~200MB RAM
#     - Only loads when actually needed
#     - App starts faster
#     - If it fails, we gracefully fall back to text-only search
#     """
#     global _embedding_model, _embedding_model_failed
    
#     # Don't retry if it already failed
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Generates embedding vector for a search query.
    
#     Returns:
#         List of 384 floats, or None if model not available
#     """
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # SEMANTIC BOOST CALCULATION
# # ============================================================================

# def calculate_semantic_boost(
#     query: str,
#     intent: str,
#     alt_mode: bool = False
# ) -> float:
#     """
#     Determines how much to weight semantic vs text search.
    
#     Returns:
#         Float between 0.0 and 1.0
#         - 0.0 = text only
#         - 0.5 = balanced
#         - 1.0 = semantic heavy
    
#     Strategy:
#         - Long queries benefit from semantic (understand context)
#         - Short queries need text matching (exact terms)
#         - Person/historical intents need semantic (context matters)
#         - Product/location intents need text (exact names)
#         - alt_mode forces high semantic for exploration
#     """
    
#     # Alt mode = user wants semantic exploration
#     if alt_mode:
#         return 0.85
    
#     # Base boost by intent
#     intent_boosts = {
#         'general': 0.5,      # Balanced
#         'person': 0.7,       # People queries benefit from semantic
#         'historical': 0.7,   # Historical context needs semantic
#         'media': 0.6,        # Genre matching benefits from semantic
#         'location': 0.3,     # Place names need exact matching
#         'product': 0.2,      # Product names need exact matching
#     }
    
#     base_boost = intent_boosts.get(intent, 0.5)
    
#     # Adjust by query length
#     word_count = len(query.split())
    
#     if word_count >= 6:
#         # Long query: boost semantic understanding
#         length_modifier = 0.2
#     elif word_count >= 4:
#         # Medium query: slight boost
#         length_modifier = 0.1
#     elif word_count <= 2:
#         # Very short: reduce semantic (need exact matches)
#         length_modifier = -0.15
#     else:
#         length_modifier = 0
    
#     # Adjust by query specificity
#     # Queries with specific terms (names, places) need less semantic
#     specific_indicators = [
#         r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Proper names like "Martin Luther"
#         r'\b\d{4}\b',                        # Years like 1965
#         r'\b(Dr\.|Mr\.|Mrs\.|Rev\.)\b',     # Titles
#     ]
    
#     specificity_modifier = 0
#     for pattern in specific_indicators:
#         if re.search(pattern, query):
#             specificity_modifier -= 0.1
    
#     # Calculate final boost (clamp between 0 and 1)
#     final_boost = base_boost + length_modifier + specificity_modifier
#     return max(0.0, min(1.0, final_boost))


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (unchanged from original)
# # ============================================================================

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# # Search fields - added key_facts for semantic search
# SEARCH_FIELDS = [
#     'key_facts',          # NEW: Primary for semantic matching
#     'document_title',
#     'primary_keywords', 
#     'keywords',
#     'semantic_keywords',
#     'document_summary',
#     'key_passages',
#     'entity_names'
# ]

# # Default weights - key_facts highest for semantic relevance
# DEFAULT_WEIGHTS = [6, 5, 4, 3, 2, 2, 1, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general':    [6, 5, 4, 3, 2, 2, 1, 2],  # Balanced
#     'location':   [5, 4, 3, 3, 2, 2, 1, 4],  # Boost entity_names for places
#     'historical': [6, 4, 4, 4, 3, 2, 1, 3],  # Boost keywords for era terms
#     'product':    [4, 5, 4, 3, 2, 2, 1, 2],  # Title-heavy for products
#     'person':     [7, 5, 3, 3, 2, 2, 1, 5],  # Key facts crucial for people
#     'media':      [5, 5, 4, 3, 3, 2, 1, 3],  # Boost semantic for genre matching
# }

# # Authority scores by source
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     """
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # ============================================================================
# # QUERY BUILDING
# # ============================================================================

# def build_query_weights(intent: str) -> str:
#     """Returns comma-separated weights string."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     semantic_boost: float = 0.5
# ) -> str:
#     """
#     Builds the vector_query string for Typesense.
    
#     Args:
#         query_embedding: The query vector (384 floats)
#         k: Number of nearest neighbors to find
#         semantic_boost: How much to weight vector vs text (0-1)
    
#     Returns:
#         String like: "embedding:([0.1, -0.2, ...], k:20, alpha:0.5)"
#     """
#     # Convert embedding to string
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     # Alpha controls text vs vector balance:
#     # alpha=0 means pure text, alpha=1 means pure vector
#     # We use semantic_boost directly as alpha
#     alpha = round(semantic_boost, 2)
    
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True,
#     query_embedding: List[float] = None,
#     semantic_boost: float = 0.5
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
#     Now includes vector_query for hybrid search.
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary,key_facts',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#         'exclude_fields': 'embedding',  # Don't return the large embedding array
#     }
    
#     # Add vector query if embedding available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,  # Get more candidates for re-ranking
#             semantic_boost=semantic_boost
#         )
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, semantic_boost: float = 0.5) -> float:
#     """
#     Combines text_match, vector_distance, and authority scores.
    
#     Updated to include vector similarity in scoring.
#     """
#     # Get Typesense's relevance score
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Get vector distance (lower is better, convert to similarity)
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)  # Convert distance to similarity
    
#     # Get authority score
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Get freshness score
#     freshness = 0.5
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination - now includes vector similarity
#     # Adjust weights based on semantic_boost
#     text_weight = 0.4 * (1 - semantic_boost * 0.5)
#     vector_weight = 0.3 * (0.5 + semantic_boost * 0.5)
#     authority_weight = 0.25
#     freshness_weight = 0.05
    
#     final_score = (
#         text_score * text_weight +
#         vector_similarity * vector_weight +
#         authority * authority_weight +
#         freshness * freshness_weight
#     )
    
#     return round(final_score, 4)


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """Removes near-duplicate documents using cluster_uuid."""
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict, semantic_boost: float = 0.5) -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     # Get vector distance for display (if available)
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),  # NEW: Include key facts
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),  # NEW
#         'semantic_score': semantic_score,  # NEW: Vector similarity score
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, semantic_boost),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, semantic_boost: float = 0.5) -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     results = [format_result(hit, semantic_boost) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
#     # results = deduplicate_by_cluster(results)
    
#     return results


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],  # NEW
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# # ============================================================================
# # MAIN SEARCH FUNCTIONS
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """
#     Execute search using multi_search endpoint.
    
#     Why: Regular search has URL length limits.
#     Vector queries are large, so we use multi_search with POST body.
#     """
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution.
#     Routes to multi_search if vector query present.
#     """
#     options = options or {}
    
#     # Use multi_search if vector query is present (to avoid URL length issues)
#     if 'vector_query' in options:
#         return execute_search_multi(options)
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: bool = False  # NEW: Enable semantic-heavy search
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     NEW PARAMETER:
#         alt_mode: bool - When True, uses heavy semantic weighting.
#                         Good for exploratory searches or when text
#                         search isn't finding what user wants.
    
#     HYBRID SEARCH BEHAVIOR:
#         1. Always attempts to use semantic search
#         2. Falls back to text-only if embedding model unavailable
#         3. Semantic weight adjusted based on:
#            - Intent (person/historical = more semantic)
#            - Query length (longer = more semantic)
#            - alt_mode flag (forced high semantic)
#     """
#     import time
#     start_time = time.time()
    
#     # Step 1: Detect intent
#     intent = detect_query_intent(query, pos_tags)
    
#     # Step 2: Calculate semantic boost
#     semantic_boost = calculate_semantic_boost(query, intent, alt_mode)
    
#     # Step 3: Generate query embedding (may return None if model unavailable)
#     query_embedding = get_query_embedding(query)
#     semantic_enabled = query_embedding is not None
    
#     # Step 4: Build search parameters
#     search_params = build_search_params(
#         query=query,
#         intent=intent,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         safe_search=safe_search,
#         query_embedding=query_embedding,
#         semantic_boost=semantic_boost
#     )
    
#     # DEBUG LOGGING
#     print("=" * 60)
#     print("TYPESENSE HYBRID SEARCH DEBUG")
#     print("=" * 60)
#     print(f"Query: '{query}'")
#     print(f"Intent: {intent}")
#     print(f"Semantic enabled: {semantic_enabled}")
#     print(f"Semantic boost: {semantic_boost}")
#     print(f"Alt mode: {alt_mode}")
#     if 'vector_query' in search_params:
#         print(f"Vector query: YES (alpha={semantic_boost})")
#     else:
#         print("Vector query: NO (text-only search)")
#     print("=" * 60)
    
#     # Step 5: Execute search
#     raw_response = execute_search(query, search_params)
    
#     # DEBUG LOGGING
#     print(f"Typesense found: {raw_response.get('found', 0)} documents")
#     print(f"Hits returned: {len(raw_response.get('hits', []))}")
#     if raw_response.get('hits'):
#         first_hit = raw_response['hits'][0]
#         first_doc = first_hit.get('document', {})
#         print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
#         print(f"First hit key_facts: {first_doc.get('key_facts', [])[:2]}")
#         if first_hit.get('vector_distance'):
#             print(f"First hit vector_distance: {first_hit.get('vector_distance')}")
#     if raw_response.get('error'):
#         print(f"ERROR: {raw_response.get('error')}")
#     print("=" * 60)
    
#     # Step 6: Process results
#     results = process_results(raw_response, semantic_boost)
    
#     # Step 7: Build final response
#     search_time = round(time.time() - start_time, 3)
    
#     return {
#         'query': query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,  # NEW
#         'semantic_boost': semantic_boost,      # NEW
#         'alt_mode': alt_mode,                  # NEW
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Simple search for autocomplete or quick lookups.
#     Text-only, no semantic.
#     """
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts,primary_keywords',
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return [hit['document'] for hit in response.get('hits', [])]
#     except:
#         return []


# def semantic_search_only(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Pure semantic search - useful for "find similar" features.
#     """
#     query_embedding = get_query_embedding(query)
#     if not query_embedding:
#         return []
    
#     params = {
#         'q': '*',  # Match all, rely only on vector
#         'vector_query': build_vector_query(query_embedding, k=limit, semantic_boost=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
#     Uses the document's embedding to find neighbors.
    
#     Useful for "Related articles" feature.
#     """
#     # First, get the document's embedding
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         # Search for similar
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,  # +1 to exclude self
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'  # Exclude the source document
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []

# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# - HYBRID SEARCH (text + semantic/vector)

# Updated: Added semantic vector search with smart weighting
# """

# import typesense
# from typing import Dict, List, Tuple, Optional
# import re
# from datetime import datetime
# from decouple import config

# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded for semantic search
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False  # Track if model failed to load

# def get_embedding_model():
#     """
#     Lazy loads the embedding model on first use.
    
#     Why lazy loading:
#     - Model uses ~200MB RAM
#     - Only loads when actually needed
#     - App starts faster
#     - If it fails, we gracefully fall back to text-only search
#     """
#     global _embedding_model, _embedding_model_failed
    
#     # Don't retry if it already failed
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Generates embedding vector for a search query.
    
#     Returns:
#         List of 384 floats, or None if model not available
#     """
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # SEMANTIC BOOST CALCULATION
# # ============================================================================

# def calculate_semantic_boost(
#     query: str,
#     intent: str,
#     alt_mode: str = 'n'
# ) -> float:
#     """
#     Determines how much to weight semantic vs text search.
    
#     Returns:
#         Float between 0.0 and 1.0
#         - 0.0 = text only
#         - 0.5 = balanced
#         - 1.0 = semantic heavy
    
#     Strategy:
#         - Long queries benefit from semantic (understand context)
#         - Short queries need text matching (exact terms)
#         - Person/historical intents need semantic (context matters)
#         - Product/location intents need text (exact names)
#         - alt_mode='y' forces high semantic for exploration
    
#     Args:
#         alt_mode: 'y' = semantic heavy, 'n' = normal weighting
#     """
    
#     # Alt mode = user wants semantic exploration
#     if alt_mode == 'y':
#         return 0.85
    
#     # Base boost by intent
#     intent_boosts = {
#         'general': 0.5,      # Balanced
#         'person': 0.7,       # People queries benefit from semantic
#         'historical': 0.7,   # Historical context needs semantic
#         'media': 0.6,        # Genre matching benefits from semantic
#         'location': 0.3,     # Place names need exact matching
#         'product': 0.2,      # Product names need exact matching
#     }
    
#     base_boost = intent_boosts.get(intent, 0.5)
    
#     # Adjust by query length
#     word_count = len(query.split())
    
#     if word_count >= 6:
#         # Long query: boost semantic understanding
#         length_modifier = 0.2
#     elif word_count >= 4:
#         # Medium query: slight boost
#         length_modifier = 0.1
#     elif word_count <= 2:
#         # Very short: reduce semantic (need exact matches)
#         length_modifier = -0.15
#     else:
#         length_modifier = 0
    
#     # Adjust by query specificity
#     # Queries with specific terms (names, places) need less semantic
#     specific_indicators = [
#         r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Proper names like "Martin Luther"
#         r'\b\d{4}\b',                        # Years like 1965
#         r'\b(Dr\.|Mr\.|Mrs\.|Rev\.)\b',     # Titles
#     ]
    
#     specificity_modifier = 0
#     for pattern in specific_indicators:
#         if re.search(pattern, query):
#             specificity_modifier -= 0.1
    
#     # Calculate final boost (clamp between 0 and 1)
#     final_boost = base_boost + length_modifier + specificity_modifier
#     return max(0.0, min(1.0, final_boost))


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (unchanged from original)
# # ============================================================================

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# # Search fields - added key_facts for semantic search
# SEARCH_FIELDS = [
#     'key_facts',          # NEW: Primary for semantic matching
#     'document_title',
#     'primary_keywords', 
#     'keywords',
#     'semantic_keywords',
#     'document_summary',
#     'key_passages',
#     'entity_names'
# ]

# # Default weights - key_facts highest for semantic relevance
# DEFAULT_WEIGHTS = [6, 5, 4, 3, 2, 2, 1, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general':    [6, 5, 4, 3, 2, 2, 1, 2],  # Balanced
#     'location':   [5, 4, 3, 3, 2, 2, 1, 4],  # Boost entity_names for places
#     'historical': [6, 4, 4, 4, 3, 2, 1, 3],  # Boost keywords for era terms
#     'product':    [4, 5, 4, 3, 2, 2, 1, 2],  # Title-heavy for products
#     'person':     [7, 5, 3, 3, 2, 2, 1, 5],  # Key facts crucial for people
#     'media':      [5, 5, 4, 3, 3, 2, 1, 3],  # Boost semantic for genre matching
# }

# # Authority scores by source
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     """
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # ============================================================================
# # QUERY BUILDING
# # ============================================================================

# def build_query_weights(intent: str) -> str:
#     """Returns comma-separated weights string."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     semantic_boost: float = 0.5
# ) -> str:
#     """
#     Builds the vector_query string for Typesense.
    
#     Args:
#         query_embedding: The query vector (384 floats)
#         k: Number of nearest neighbors to find
#         semantic_boost: How much to weight vector vs text (0-1)
    
#     Returns:
#         String like: "embedding:([0.1, -0.2, ...], k:20, alpha:0.5)"
#     """
#     # Convert embedding to string
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     # Alpha controls text vs vector balance:
#     # alpha=0 means pure text, alpha=1 means pure vector
#     # We use semantic_boost directly as alpha
#     alpha = round(semantic_boost, 2)
    
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True,
#     query_embedding: List[float] = None,
#     semantic_boost: float = 0.5
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
#     Now includes vector_query for hybrid search.
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary,key_facts',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#         'exclude_fields': 'embedding',  # Don't return the large embedding array
#     }
    
#     # Add vector query if embedding available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,  # Get more candidates for re-ranking
#             semantic_boost=semantic_boost
#         )
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, semantic_boost: float = 0.5) -> float:
#     """
#     Combines text_match, vector_distance, and authority scores.
    
#     Updated to include vector similarity in scoring.
#     """
#     # Get Typesense's relevance score
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Get vector distance (lower is better, convert to similarity)
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)  # Convert distance to similarity
    
#     # Get authority score
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Get freshness score
#     freshness = 0.5
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination - now includes vector similarity
#     # Adjust weights based on semantic_boost
#     text_weight = 0.4 * (1 - semantic_boost * 0.5)
#     vector_weight = 0.3 * (0.5 + semantic_boost * 0.5)
#     authority_weight = 0.25
#     freshness_weight = 0.05
    
#     final_score = (
#         text_score * text_weight +
#         vector_similarity * vector_weight +
#         authority * authority_weight +
#         freshness * freshness_weight
#     )
    
#     return round(final_score, 4)


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """Removes near-duplicate documents using cluster_uuid."""
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict, semantic_boost: float = 0.5) -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     # Get vector distance for display (if available)
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),  # NEW: Include key facts
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),  # NEW
#         'semantic_score': semantic_score,  # NEW: Vector similarity score
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, semantic_boost),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, semantic_boost: float = 0.5) -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     results = [format_result(hit, semantic_boost) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
#     # results = deduplicate_by_cluster(results)
    
#     return results


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],  # NEW
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# # ============================================================================
# # MAIN SEARCH FUNCTIONS
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """
#     Execute search using multi_search endpoint.
    
#     Why: Regular search has URL length limits.
#     Vector queries are large, so we use multi_search with POST body.
#     """
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution.
#     Routes to multi_search if vector query present.
#     """
#     options = options or {}
    
#     # Use multi_search if vector query is present (to avoid URL length issues)
#     if 'vector_query' in options:
#         return execute_search_multi(options)
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: str = 'n'  # 'y' = semantic heavy, 'n' = normal
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     PARAMETER:
#         alt_mode: str - 'y' for semantic-heavy search, 'n' for normal.
#                        Use 'y' when user types custom query not found
#                        in dropdown suggestions.
    
#     HYBRID SEARCH BEHAVIOR:
#         1. Always attempts to use semantic search
#         2. Falls back to text-only if embedding model unavailable
#         3. Semantic weight adjusted based on:
#            - Intent (person/historical = more semantic)
#            - Query length (longer = more semantic)
#            - alt_mode='y' (forced high semantic)
#     """
#     import time
#     start_time = time.time()
    
#     # Step 1: Detect intent
#     intent = detect_query_intent(query, pos_tags)
    
#     # Step 2: Calculate semantic boost
#     semantic_boost = calculate_semantic_boost(query, intent, alt_mode)
    
#     # Step 3: Generate query embedding (may return None if model unavailable)
#     query_embedding = get_query_embedding(query)
#     semantic_enabled = query_embedding is not None
    
#     # Step 4: Build search parameters
#     search_params = build_search_params(
#         query=query,
#         intent=intent,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         safe_search=safe_search,
#         query_embedding=query_embedding,
#         semantic_boost=semantic_boost
#     )
    
#     # DEBUG LOGGING
#     print("=" * 60)
#     print("TYPESENSE HYBRID SEARCH DEBUG")
#     print("=" * 60)
#     print(f"Query: '{query}'")
#     print(f"Intent: {intent}")
#     print(f"Semantic enabled: {semantic_enabled}")
#     print(f"Semantic boost: {semantic_boost}")
#     print(f"Alt mode: {alt_mode}")
#     if 'vector_query' in search_params:
#         print(f"Vector query: YES (alpha={semantic_boost})")
#     else:
#         print("Vector query: NO (text-only search)")
#     print("=" * 60)
    
#     # Step 5: Execute search
#     raw_response = execute_search(query, search_params)
    
#     # DEBUG LOGGING
#     print(f"Typesense found: {raw_response.get('found', 0)} documents")
#     print(f"Hits returned: {len(raw_response.get('hits', []))}")
#     if raw_response.get('hits'):
#         first_hit = raw_response['hits'][0]
#         first_doc = first_hit.get('document', {})
#         print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
#         print(f"First hit key_facts: {first_doc.get('key_facts', [])[:2]}")
#         if first_hit.get('vector_distance'):
#             print(f"First hit vector_distance: {first_hit.get('vector_distance')}")
#     if raw_response.get('error'):
#         print(f"ERROR: {raw_response.get('error')}")
#     print("=" * 60)
    
#     # Step 6: Process results
#     results = process_results(raw_response, semantic_boost)
    
#     # Step 7: Build final response
#     search_time = round(time.time() - start_time, 3)
    
#     return {
#         'query': query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,  # NEW
#         'semantic_boost': semantic_boost,      # NEW
#         'alt_mode': alt_mode,                  # NEW
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Simple search for autocomplete or quick lookups.
#     Text-only, no semantic.
#     """
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts,primary_keywords',
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return [hit['document'] for hit in response.get('hits', [])]
#     except:
#         return []


# def semantic_search_only(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Pure semantic search - useful for "find similar" features.
#     """
#     query_embedding = get_query_embedding(query)
#     if not query_embedding:
#         return []
    
#     params = {
#         'q': '*',  # Match all, rely only on vector
#         'vector_query': build_vector_query(query_embedding, k=limit, semantic_boost=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
#     Uses the document's embedding to find neighbors.
    
#     Useful for "Related articles" feature.
#     """
#     # First, get the document's embedding
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         # Search for similar
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,  # +1 to exclude self
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'  # Exclude the source document
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []

# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# - HYBRID SEARCH (text + semantic/vector)

# Updated: Added semantic vector search with smart weighting
# """

# import typesense
# from typing import Dict, List, Tuple, Optional
# import re
# from datetime import datetime
# from decouple import config

# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded for semantic search
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False  # Track if model failed to load


# def get_embedding_model():
#     """
#     Lazy loads the embedding model on first use.
    
#     Why lazy loading:
#     - Model uses ~200MB RAM
#     - Only loads when actually needed
#     - App starts faster
#     - If it fails, we gracefully fall back to text-only search
#     """
#     global _embedding_model, _embedding_model_failed
    
#     # Don't retry if it already failed
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Generates embedding vector for a search query.
    
#     Returns:
#         List of 384 floats, or None if model not available
#     """
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # SEMANTIC BOOST CALCULATION
# # ============================================================================

# def calculate_semantic_boost(
#     query: str,
#     intent: str,
#     alt_mode: str = 'n'
# ) -> float:
#     """
#     Determines how much to weight semantic vs text search.
    
#     Returns:
#         Float between 0.0 and 1.0
#         - 0.0 = text only
#         - 0.5 = balanced
#         - 1.0 = semantic heavy
    
#     Strategy:
#         - Long queries benefit from semantic (understand context)
#         - Short queries need text matching (exact terms)
#         - Person/historical intents need semantic (context matters)
#         - Product/location intents need text (exact names)
#         - alt_mode='y' forces high semantic for exploration
    
#     Args:
#         alt_mode: 'y' = semantic heavy, 'n' = normal weighting
#     """
    
#     # Alt mode = user wants semantic exploration
#     if alt_mode == 'y':
#         return 0.85
    
#     # Base boost by intent
#     intent_boosts = {
#         'general': 0.5,      # Balanced
#         'person': 0.7,       # People queries benefit from semantic
#         'historical': 0.7,   # Historical context needs semantic
#         'media': 0.6,        # Genre matching benefits from semantic
#         'location': 0.3,     # Place names need exact matching
#         'product': 0.2,      # Product names need exact matching
#     }
    
#     base_boost = intent_boosts.get(intent, 0.5)
    
#     # Adjust by query length
#     word_count = len(query.split())
    
#     if word_count >= 6:
#         # Long query: boost semantic understanding
#         length_modifier = 0.2
#     elif word_count >= 4:
#         # Medium query: slight boost
#         length_modifier = 0.1
#     elif word_count <= 2:
#         # Very short: reduce semantic (need exact matches)
#         length_modifier = -0.15
#     else:
#         length_modifier = 0
    
#     # Adjust by query specificity
#     # Queries with specific terms (names, places) need less semantic
#     specific_indicators = [
#         r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Proper names like "Martin Luther"
#         r'\b\d{4}\b',                        # Years like 1965
#         r'\b(Dr\.|Mr\.|Mrs\.|Rev\.)\b',     # Titles
#     ]
    
#     specificity_modifier = 0
#     for pattern in specific_indicators:
#         if re.search(pattern, query):
#             specificity_modifier -= 0.1
    
#     # Calculate final boost (clamp between 0 and 1)
#     final_boost = base_boost + length_modifier + specificity_modifier
#     return max(0.0, min(1.0, final_boost))


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (unchanged from original)
# # ============================================================================

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# # Search fields - key_facts is primary for accurate matching
# SEARCH_FIELDS = [
#     'key_facts',          # Primary - specific facts about the document
#     'document_title',     # High - exact title matches
#     'primary_keywords',   # Medium - curated keywords
#     'keywords',           # Lower - broader keywords
#     'semantic_keywords',  # Lower - related concepts
#     'document_summary',   # Low - too many generic words
#     'key_passages',       # Low - can match anything
#     'entity_names'        # Medium - names of people/places
# ]

# # Default weights - key_facts dominates, summary very low
# # Format: [key_facts, title, primary_kw, keywords, semantic_kw, summary, passages, entities]
# DEFAULT_WEIGHTS = [10, 5, 3, 2, 1, 1, 1, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2, 1, 1, 1, 2],  # Key facts dominate
#     'location':   [8, 5, 3, 2, 1, 1, 1, 4],   # Boost entity_names for places
#     'historical': [10, 4, 4, 3, 2, 1, 1, 3],  # Boost keywords for era terms
#     'product':    [8, 6, 4, 3, 1, 1, 1, 2],   # Title important for products
#     'person':     [10, 5, 3, 2, 1, 1, 1, 5],  # Key facts + entity names for people
#     'media':      [9, 5, 4, 3, 2, 1, 1, 3],   # Balanced for media
# }

# # Minimum score threshold - filter out weak matches
# MIN_SCORE_THRESHOLD = 0.15

# # Context-aware stop words
# # These are ONLY removed for history/document queries, NOT for product queries
# HISTORY_STOP_WORDS = {
#     'african', 'american', 'african american', 'afro',
#     'history', 'historic', 'historical',
#     'united', 'states', 'usa',
# }

# # General stop words - always safe to remove
# GENERAL_STOP_WORDS = {
#     'the', 'a', 'an', 'of', 'in', 'at', 'on', 'for', 'to', 'and', 'or', 'is', 'are'
# }

# # Product/service indicators - if query contains these, keep all words
# PRODUCT_INDICATORS = {
#     'buy', 'shop', 'price', 'cheap', 'expensive', 'sale', 'deal',
#     'store', 'order', 'shipping', 'delivery',
#     'shoes', 'shirt', 'pants', 'jacket', 'dress', 'clothes', 'clothing',
#     'car', 'truck', 'vehicle', 'auto',
#     'food', 'restaurant', 'eat', 'menu',
#     'service', 'hire', 'book', 'appointment',
#     'salon', 'barber', 'hair', 'beauty', 'spa',
#     'lawyer', 'doctor', 'dentist', 'plumber', 'contractor',
#     'business', 'company', 'brand', 'owned'
# }

# # History/document indicators - if query contains these, can remove generic terms
# HISTORY_INDICATORS = {
#     'history', 'historic', 'historical', 'museum', 'monument', 'memorial',
#     'civil rights', 'slavery', 'enslaved', 'freedom', 'liberation',
#     'first', 'mayor', 'governor', 'senator', 'president', 'politician',
#     'church', 'baptist', 'ame', 'methodist',
#     'war', 'battle', 'military', 'soldier', 'regiment',
#     'underground railroad', 'abolition', 'emancipation',
#     'segregation', 'integration', 'jim crow',
#     'founded', 'established', 'landmark', 'site'
# }

# # Authority scores by source
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     """
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def clean_query_for_search(query: str) -> str:
#     """
#     Context-aware query cleaning.
    
#     - Product queries: Keep ALL words ("black shoes" → "black shoes")
#     - History queries: Remove generic terms ("black history museum" → "museum")
#     - Mixed/unclear: Keep all words (safer)
    
#     Why: "black" means color for products but is generic for history content.
#     """
#     query_lower = query.lower()
#     words = query_lower.split()
    
#     # Check if this is a product/service query
#     is_product_query = any(indicator in query_lower for indicator in PRODUCT_INDICATORS)
    
#     # Check if this is a history/document query
#     is_history_query = any(indicator in query_lower for indicator in HISTORY_INDICATORS)
    
#     # Decision logic:
#     # - Product query → Keep all words
#     # - History query (and NOT product) → Remove generic history terms
#     # - Neither or both → Keep all words (safer default)
    
#     if is_product_query:
#         # Product search: keep everything, only remove basic stop words
#         # "black shoes on sale" → "black shoes sale"
#         cleaned_words = [w for w in words if w not in GENERAL_STOP_WORDS]
#         return ' '.join(cleaned_words) if cleaned_words else query
    
#     elif is_history_query and not is_product_query:
#         # History search: remove generic history terms
#         # "african american history museum" → "museum"
#         # "first black mayor" → "first black mayor" (keep "black" with "first")
        
#         # Special case: keep "black" if paired with achievement words
#         achievement_words = {'first', 'only', 'oldest', 'youngest', 'largest'}
#         has_achievement = any(w in words for w in achievement_words)
        
#         if has_achievement:
#             # Keep "black" for "first black mayor" type queries
#             stop_words = GENERAL_STOP_WORDS | (HISTORY_STOP_WORDS - {'black'})
#         else:
#             # Remove all history stop words
#             stop_words = GENERAL_STOP_WORDS | HISTORY_STOP_WORDS
        
#         cleaned_words = [w for w in words if w not in stop_words]
        
#         # Handle multi-word stop phrases
#         cleaned_query = ' '.join(cleaned_words)
#         cleaned_query = cleaned_query.replace('african american', '').strip()
        
#         return cleaned_query if cleaned_query and len(cleaned_query) > 1 else query
    
#     else:
#         # Unclear context: only remove basic stop words, keep everything else
#         cleaned_words = [w for w in words if w not in GENERAL_STOP_WORDS]
#         return ' '.join(cleaned_words) if cleaned_words else query


# def get_specific_terms(query: str) -> List[str]:
#     """
#     Extracts the most specific/unique terms from query.
#     These terms should be required to match for good results.
    
#     Example:
#         "tuskegee airmen pilots" -> ["tuskegee", "airmen", "pilots"]
#         "black shoes" -> ["black", "shoes"]
#         "first black mayor atlanta" -> ["first", "black", "mayor", "atlanta"]
#     """
#     query_lower = query.lower()
#     words = query_lower.split()
    
#     # Only remove basic stop words for term extraction
#     specific = [w for w in words if w not in GENERAL_STOP_WORDS and len(w) > 2]
    
#     return specific


# def detect_query_type(query: str) -> str:
#     """
#     Detects if query is for products, services, or documents/history.
    
#     Returns: 'product', 'service', 'history', or 'general'
#     """
#     query_lower = query.lower()
    
#     # Product indicators
#     product_words = {'buy', 'shop', 'price', 'shoes', 'shirt', 'jacket', 'dress', 
#                      'car', 'clothes', 'clothing', 'pants', 'hat', 'bag'}
#     if any(word in query_lower for word in product_words):
#         return 'product'
    
#     # Service indicators
#     service_words = {'salon', 'barber', 'restaurant', 'lawyer', 'doctor', 
#                      'dentist', 'hire', 'book', 'appointment', 'service'}
#     if any(word in query_lower for word in service_words):
#         return 'service'
    
#     # History indicators
#     history_words = {'history', 'museum', 'monument', 'civil rights', 'slavery',
#                      'first', 'church', 'landmark', 'historic'}
#     if any(word in query_lower for word in history_words):
#         return 'history'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # ============================================================================
# # QUERY BUILDING
# # ============================================================================

# def build_query_weights(intent: str) -> str:
#     """Returns comma-separated weights string."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     semantic_boost: float = 0.5
# ) -> str:
#     """
#     Builds the vector_query string for Typesense.
    
#     Args:
#         query_embedding: The query vector (384 floats)
#         k: Number of nearest neighbors to find
#         semantic_boost: How much to weight vector vs text (0-1)
    
#     Returns:
#         String like: "embedding:([0.1, -0.2, ...], k:20, alpha:0.5)"
#     """
#     # Convert embedding to string
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     # Alpha controls text vs vector balance:
#     # alpha=0 means pure text, alpha=1 means pure vector
#     # We use semantic_boost directly as alpha
#     alpha = round(semantic_boost, 2)
    
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True,
#     query_embedding: List[float] = None,
#     semantic_boost: float = 0.5
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
#     Now includes vector_query for hybrid search.
#     Uses cleaned query to remove generic terms.
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # Clean query to remove generic terms that match everything
#     cleaned_query = clean_query_for_search(query)
    
#     params = {
#         'q': cleaned_query,  # Use cleaned query for search
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary,key_facts',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#         'exclude_fields': 'embedding',  # Don't return the large embedding array
        
#         # Stricter matching options
#         'drop_tokens_threshold': 5,     # Only drop tokens if > 5 results (stricter)
#         'typo_tokens_threshold': 3,     # Fewer typo corrections
#         'num_typos': 1,                 # Allow only 1 typo (stricter)
#     }
    
#     # Add vector query if embedding available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,  # Get more candidates for re-ranking
#             semantic_boost=semantic_boost
#         )
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, semantic_boost: float = 0.5, query: str = '') -> float:
#     """
#     Combines text_match, vector_distance, and authority scores.
#     Gives bonus for key_facts matches.
    
#     Updated to be stricter and favor specific matches over generic ones.
#     """
#     # Get Typesense's relevance score
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Get vector distance (lower is better, convert to similarity)
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)  # Convert distance to similarity
    
#     # Get document data
#     doc = hit.get('document', {})
    
#     # Get authority score
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Check for key_facts match - give bonus if query terms appear in key_facts
#     key_facts_bonus = 0.0
#     key_facts = doc.get('key_facts', [])
#     if key_facts and query:
#         query_terms = get_specific_terms(query.lower())
#         key_facts_text = ' '.join(key_facts).lower()
        
#         matches = sum(1 for term in query_terms if term in key_facts_text)
#         if matches > 0:
#             # Significant bonus for key_facts matches (0.1 to 0.3)
#             key_facts_bonus = min(0.3, matches * 0.1)
    
#     # Check for title match - bonus if query terms in title
#     title_bonus = 0.0
#     title = doc.get('document_title', '').lower()
#     if title and query:
#         query_terms = get_specific_terms(query.lower())
#         matches = sum(1 for term in query_terms if term in title)
#         if matches > 0:
#             title_bonus = min(0.2, matches * 0.1)
    
#     # Get freshness score (less important for historical content)
#     freshness = 0.5
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination
#     # Adjust weights based on semantic_boost
#     text_weight = 0.35 * (1 - semantic_boost * 0.5)
#     vector_weight = 0.25 * (0.5 + semantic_boost * 0.5)
#     authority_weight = 0.20
#     freshness_weight = 0.05
    
#     # Base score
#     base_score = (
#         text_score * text_weight +
#         vector_similarity * vector_weight +
#         authority * authority_weight +
#         freshness * freshness_weight
#     )
    
#     # Add bonuses for specific matches
#     final_score = base_score + key_facts_bonus + title_bonus
    
#     return round(min(1.0, final_score), 4)  # Cap at 1.0


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """Removes near-duplicate documents using cluster_uuid."""
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict, semantic_boost: float = 0.5, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     # Get vector distance for display (if available)
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),  # Include key facts
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,  # Vector similarity score
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, semantic_boost, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, semantic_boost: float = 0.5, query: str = '') -> List[Dict]:
#     """
#     Processes Typesense response into clean result list.
#     Filters out weak matches below MIN_SCORE_THRESHOLD.
#     """
#     hits = raw_response.get('hits', [])
    
#     results = [format_result(hit, semantic_boost, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     # Filter out weak matches
#     filtered_results = [r for r in results if r['score'] >= MIN_SCORE_THRESHOLD]
    
#     # If all results filtered out, keep top 3 anyway (better than empty)
#     if not filtered_results and results:
#         filtered_results = results[:3]
    
#     # results = deduplicate_by_cluster(filtered_results)
    
#     return filtered_results


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],  # NEW
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# # ============================================================================
# # MAIN SEARCH FUNCTIONS
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """
#     Execute search using multi_search endpoint.
    
#     Why: Regular search has URL length limits.
#     Vector queries are large, so we use multi_search with POST body.
#     """
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution.
#     Routes to multi_search if vector query present.
#     """
#     options = options or {}
    
#     # Use multi_search if vector query is present (to avoid URL length issues)
#     if 'vector_query' in options:
#         return execute_search_multi(options)
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: str = 'n'  # 'y' = semantic heavy, 'n' = normal
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     PARAMETER:
#         alt_mode: str - 'y' for semantic-heavy search, 'n' for normal.
#                        Use 'y' when user types custom query not found
#                        in dropdown suggestions.
    
#     HYBRID SEARCH BEHAVIOR:
#         1. Always attempts to use semantic search
#         2. Falls back to text-only if embedding model unavailable
#         3. Semantic weight adjusted based on:
#            - Intent (person/historical = more semantic)
#            - Query length (longer = more semantic)
#            - alt_mode='y' (forced high semantic)
#     """
#     import time
#     start_time = time.time()
    
#     # Step 1: Detect intent
#     intent = detect_query_intent(query, pos_tags)
    
#     # Step 2: Calculate semantic boost
#     semantic_boost = calculate_semantic_boost(query, intent, alt_mode)
    
#     # Step 3: Generate query embedding (may return None if model unavailable)
#     query_embedding = get_query_embedding(query)
#     semantic_enabled = query_embedding is not None
    
#     # Step 4: Build search parameters
#     search_params = build_search_params(
#         query=query,
#         intent=intent,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         safe_search=safe_search,
#         query_embedding=query_embedding,
#         semantic_boost=semantic_boost
#     )
    
#     # DEBUG LOGGING
#     print("=" * 60)
#     print("TYPESENSE HYBRID SEARCH DEBUG")
#     print("=" * 60)
#     print(f"Query: '{query}'")
#     print(f"Intent: {intent}")
#     print(f"Semantic enabled: {semantic_enabled}")
#     print(f"Semantic boost: {semantic_boost}")
#     print(f"Alt mode: {alt_mode}")
#     if 'vector_query' in search_params:
#         print(f"Vector query: YES (alpha={semantic_boost})")
#     else:
#         print("Vector query: NO (text-only search)")
#     print("=" * 60)
    
#     # Step 5: Execute search
#     raw_response = execute_search(query, search_params)
    
#     # DEBUG LOGGING
#     print(f"Typesense found: {raw_response.get('found', 0)} documents")
#     print(f"Hits returned: {len(raw_response.get('hits', []))}")
#     if raw_response.get('hits'):
#         first_hit = raw_response['hits'][0]
#         first_doc = first_hit.get('document', {})
#         print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
#         print(f"First hit key_facts: {first_doc.get('key_facts', [])[:2]}")
#         if first_hit.get('vector_distance'):
#             print(f"First hit vector_distance: {first_hit.get('vector_distance')}")
#     if raw_response.get('error'):
#         print(f"ERROR: {raw_response.get('error')}")
#     print("=" * 60)
    
#     # Step 6: Process results
#     results = process_results(raw_response, semantic_boost, query)
    
#     # Step 7: Build final response
#     search_time = round(time.time() - start_time, 3)
    
#     # Get cleaned query for display
#     cleaned_query = clean_query_for_search(query)
    
#     return {
#         'query': query,
#         'cleaned_query': cleaned_query,  # Show what was actually searched
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Simple search for autocomplete or quick lookups.
#     Text-only, no semantic.
#     """
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts,primary_keywords',
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return [hit['document'] for hit in response.get('hits', [])]
#     except:
#         return []


# def semantic_search_only(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Pure semantic search - useful for "find similar" features.
#     """
#     query_embedding = get_query_embedding(query)
#     if not query_embedding:
#         return []
    
#     params = {
#         'q': '*',  # Match all, rely only on vector
#         'vector_query': build_vector_query(query_embedding, k=limit, semantic_boost=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
#     Uses the document's embedding to find neighbors.
    
#     Useful for "Related articles" feature.
#     """
#     # First, get the document's embedding
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         # Search for similar
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,  # +1 to exclude self
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'  # Exclude the source document
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# """
# typesense_calculations.py

# Handles all Typesense search logic:
# - Query building
# - Intent detection
# - Weighting & ranking
# - Result processing
# - HYBRID SEARCH (text + semantic/vector)
# - WORD DISCOVERY INTEGRATION (keyword filter first, then embedding)

# Updated: Two-stage search - keyword filter FIRST, embedding SECOND
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from datetime import datetime
# from decouple import config

# # Import word discovery for search strategy
# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     WORD_DISCOVERY_AVAILABLE = False
#     print("⚠️ word_discovery_optimized not available, using basic search")


# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded for semantic search
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False  # Track if model failed to load


# def get_embedding_model():
#     """
#     Lazy loads the embedding model on first use.
    
#     Why lazy loading:
#     - Model uses ~200MB RAM
#     - Only loads when actually needed
#     - App starts faster
#     - If it fails, we gracefully fall back to text-only search
#     """
#     global _embedding_model, _embedding_model_failed
    
#     # Don't retry if it already failed
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Generates embedding vector for a search query.
    
#     Returns:
#         List of 384 floats, or None if model not available
#     """
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # SEMANTIC BOOST CALCULATION
# # ============================================================================

# def calculate_semantic_boost(
#     query: str,
#     intent: str,
#     alt_mode: str = 'n'
# ) -> float:
#     """
#     Determines how much to weight semantic vs text search.
    
#     Returns:
#         Float between 0.0 and 1.0
#         - 0.0 = text only
#         - 0.5 = balanced
#         - 1.0 = semantic heavy
    
#     Strategy:
#         - Long queries benefit from semantic (understand context)
#         - Short queries need text matching (exact terms)
#         - Person/historical intents need semantic (context matters)
#         - Product/location intents need text (exact names)
#         - alt_mode='y' forces high semantic for exploration
    
#     Args:
#         alt_mode: 'y' = semantic heavy, 'n' = normal weighting
#     """
    
#     # Alt mode = user wants semantic exploration
#     if alt_mode == 'y':
#         return 0.85
    
#     # Base boost by intent
#     intent_boosts = {
#         'general': 0.5,      # Balanced
#         'person': 0.7,       # People queries benefit from semantic
#         'historical': 0.7,   # Historical context needs semantic
#         'media': 0.6,        # Genre matching benefits from semantic
#         'location': 0.3,     # Place names need exact matching
#         'product': 0.2,      # Product names need exact matching
#     }
    
#     base_boost = intent_boosts.get(intent, 0.5)
    
#     # Adjust by query length
#     word_count = len(query.split())
    
#     if word_count >= 6:
#         # Long query: boost semantic understanding
#         length_modifier = 0.2
#     elif word_count >= 4:
#         # Medium query: slight boost
#         length_modifier = 0.1
#     elif word_count <= 2:
#         # Very short: reduce semantic (need exact matches)
#         length_modifier = -0.15
#     else:
#         length_modifier = 0
    
#     # Adjust by query specificity
#     # Queries with specific terms (names, places) need less semantic
#     specific_indicators = [
#         r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Proper names like "Martin Luther"
#         r'\b\d{4}\b',                        # Years like 1965
#         r'\b(Dr\.|Mr\.|Mrs\.|Rev\.)\b',     # Titles
#     ]
    
#     specificity_modifier = 0
#     for pattern in specific_indicators:
#         if re.search(pattern, query):
#             specificity_modifier -= 0.1
    
#     # Calculate final boost (clamp between 0 and 1)
#     final_boost = base_boost + length_modifier + specificity_modifier
#     return max(0.0, min(1.0, final_boost))


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (unchanged from original)
# # ============================================================================

# # Location patterns
# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# # Historical patterns
# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# # Product patterns
# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# # Person patterns
# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# # Media patterns
# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# # Extraction patterns
# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# # Search fields - ONLY the most specific fields
# # REMOVED: keywords, semantic_keywords, document_summary, key_passages
# # These were matching generic words like "soldiers", "military", "war"
# SEARCH_FIELDS = [
#     'key_facts',          # Primary - specific facts about the document
#     'document_title',     # High - exact title matches
#     'primary_keywords',   # Main topic keywords
#     'entity_names'        # Names of people/places
# ]

# # Default weights - key_facts dominates
# # Format: [key_facts, title, primary_kw, entities]
# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# # Intent-specific weight profiles
# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],  # Key facts dominate
#     'location':   [8, 5, 3, 4],   # Boost entity_names for places
#     'historical': [10, 4, 4, 3],  # Key facts for history
#     'product':    [8, 6, 4, 2],   # Title important for products
#     'person':     [10, 5, 3, 5],  # Key facts + entity names for people
#     'media':      [9, 5, 4, 3],   # Balanced for media
# }

# # Minimum score threshold - filter out weak matches (increased)
# MIN_SCORE_THRESHOLD = 0.25

# # Context-aware stop words
# # These are ONLY removed for history/document queries, NOT for product queries
# HISTORY_STOP_WORDS = {
#     'african', 'american', 'african american', 'afro',
#     'history', 'historic', 'historical',
#     'united', 'states', 'usa',
# }

# # General stop words - always safe to remove
# GENERAL_STOP_WORDS = {
#     'the', 'a', 'an', 'of', 'in', 'at', 'on', 'for', 'to', 'and', 'or', 'is', 'are'
# }

# # Product/service indicators - if query contains these, keep all words
# PRODUCT_INDICATORS = {
#     'buy', 'shop', 'price', 'cheap', 'expensive', 'sale', 'deal',
#     'store', 'order', 'shipping', 'delivery',
#     'shoes', 'shirt', 'pants', 'jacket', 'dress', 'clothes', 'clothing',
#     'car', 'truck', 'vehicle', 'auto',
#     'food', 'restaurant', 'eat', 'menu',
#     'service', 'hire', 'book', 'appointment',
#     'salon', 'barber', 'hair', 'beauty', 'spa',
#     'lawyer', 'doctor', 'dentist', 'plumber', 'contractor',
#     'business', 'company', 'brand', 'owned'
# }

# # History/document indicators - if query contains these, can remove generic terms
# HISTORY_INDICATORS = {
#     'history', 'historic', 'historical', 'museum', 'monument', 'memorial',
#     'civil rights', 'slavery', 'enslaved', 'freedom', 'liberation',
#     'first', 'mayor', 'governor', 'senator', 'president', 'politician',
#     'church', 'baptist', 'ame', 'methodist',
#     'war', 'battle', 'military', 'soldier', 'regiment',
#     'underground railroad', 'abolition', 'emancipation',
#     'segregation', 'integration', 'jim crow',
#     'founded', 'established', 'landmark', 'site'
# }

# # Authority scores by source
# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """
#     Analyzes query to determine user intent.
#     """
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def clean_query_for_search(query: str) -> str:
#     """
#     Context-aware query cleaning.
    
#     - Product queries: Keep ALL words ("black shoes" → "black shoes")
#     - History queries: Remove generic terms ("black history museum" → "museum")
#     - Mixed/unclear: Keep all words (safer)
    
#     Why: "black" means color for products but is generic for history content.
#     """
#     query_lower = query.lower()
#     words = query_lower.split()
    
#     # Check if this is a product/service query
#     is_product_query = any(indicator in query_lower for indicator in PRODUCT_INDICATORS)
    
#     # Check if this is a history/document query
#     is_history_query = any(indicator in query_lower for indicator in HISTORY_INDICATORS)
    
#     # Decision logic:
#     # - Product query → Keep all words
#     # - History query (and NOT product) → Remove generic history terms
#     # - Neither or both → Keep all words (safer default)
    
#     if is_product_query:
#         # Product search: keep everything, only remove basic stop words
#         # "black shoes on sale" → "black shoes sale"
#         cleaned_words = [w for w in words if w not in GENERAL_STOP_WORDS]
#         return ' '.join(cleaned_words) if cleaned_words else query
    
#     elif is_history_query and not is_product_query:
#         # History search: remove generic history terms
#         # "african american history museum" → "museum"
#         # "first black mayor" → "first black mayor" (keep "black" with "first")
        
#         # Special case: keep "black" if paired with achievement words
#         achievement_words = {'first', 'only', 'oldest', 'youngest', 'largest'}
#         has_achievement = any(w in words for w in achievement_words)
        
#         if has_achievement:
#             # Keep "black" for "first black mayor" type queries
#             stop_words = GENERAL_STOP_WORDS | (HISTORY_STOP_WORDS - {'black'})
#         else:
#             # Remove all history stop words
#             stop_words = GENERAL_STOP_WORDS | HISTORY_STOP_WORDS
        
#         cleaned_words = [w for w in words if w not in stop_words]
        
#         # Handle multi-word stop phrases
#         cleaned_query = ' '.join(cleaned_words)
#         cleaned_query = cleaned_query.replace('african american', '').strip()
        
#         return cleaned_query if cleaned_query and len(cleaned_query) > 1 else query
    
#     else:
#         # Unclear context: only remove basic stop words, keep everything else
#         cleaned_words = [w for w in words if w not in GENERAL_STOP_WORDS]
#         return ' '.join(cleaned_words) if cleaned_words else query


# def get_specific_terms(query: str) -> List[str]:
#     """
#     Extracts the most specific/unique terms from query.
#     These terms should be required to match for good results.
    
#     Example:
#         "tuskegee airmen pilots" -> ["tuskegee", "airmen", "pilots"]
#         "black shoes" -> ["black", "shoes"]
#         "first black mayor atlanta" -> ["first", "black", "mayor", "atlanta"]
#     """
#     query_lower = query.lower()
#     words = query_lower.split()
    
#     # Only remove basic stop words for term extraction
#     specific = [w for w in words if w not in GENERAL_STOP_WORDS and len(w) > 2]
    
#     return specific


# def detect_query_type(query: str) -> str:
#     """
#     Detects if query is for products, services, or documents/history.
    
#     Returns: 'product', 'service', 'history', or 'general'
#     """
#     query_lower = query.lower()
    
#     # Product indicators
#     product_words = {'buy', 'shop', 'price', 'shoes', 'shirt', 'jacket', 'dress', 
#                      'car', 'clothes', 'clothing', 'pants', 'hat', 'bag'}
#     if any(word in query_lower for word in product_words):
#         return 'product'
    
#     # Service indicators
#     service_words = {'salon', 'barber', 'restaurant', 'lawyer', 'doctor', 
#                      'dentist', 'hire', 'book', 'appointment', 'service'}
#     if any(word in query_lower for word in service_words):
#         return 'service'
    
#     # History indicators
#     history_words = {'history', 'museum', 'monument', 'civil rights', 'slavery',
#                      'first', 'church', 'landmark', 'historic'}
#     if any(word in query_lower for word in history_words):
#         return 'history'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
    
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
    
#     return None


# # ============================================================================
# # QUERY BUILDING
# # ============================================================================

# def build_query_weights(intent: str) -> str:
#     """Returns comma-separated weights string."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
#     return ','.join(str(w) for w in weights)


# def build_filter_string(
#     filters: Dict = None,
#     intent: str = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(
#     intent: str,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
#     if intent == 'product':
#         return "_text_match:desc,product_rating:desc,authority_score:desc"
    
#     if intent == 'media':
#         return "_text_match:desc,media_rating:desc,published_date:desc"
    
#     # Default: relevance + authority + freshness
#     return "_text_match:desc,authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     semantic_boost: float = 0.5
# ) -> str:
#     """
#     Builds the vector_query string for Typesense.
    
#     Args:
#         query_embedding: The query vector (384 floats)
#         k: Number of nearest neighbors to find
#         semantic_boost: How much to weight vector vs text (0-1)
    
#     Returns:
#         String like: "embedding:([0.1, -0.2, ...], k:20, alpha:0.5)"
#     """
#     # Convert embedding to string
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     # Alpha controls text vs vector balance:
#     # alpha=0 means pure text, alpha=1 means pure vector
#     # We use semantic_boost directly as alpha
#     alpha = round(semantic_boost, 2)
    
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# def build_search_params(
#     query: str,
#     intent: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True,
#     query_embedding: List[float] = None,
#     semantic_boost: float = 0.5
# ) -> Dict:
#     """
#     Constructs complete Typesense search parameters.
#     Now includes vector_query for hybrid search.
#     Uses cleaned query to remove generic terms.
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # Clean query to remove generic terms that match everything
#     cleaned_query = clean_query_for_search(query)
    
#     params = {
#         'q': cleaned_query,  # Use cleaned query for search
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary,key_facts',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#         'exclude_fields': 'embedding',  # Don't return the large embedding array
        
#         # Stricter matching options
#         'drop_tokens_threshold': 5,     # Only drop tokens if > 5 results (stricter)
#         'typo_tokens_threshold': 3,     # Fewer typo corrections
#         'num_typos': 1,                 # Allow only 1 typo (stricter)
#     }
    
#     # Add vector query if embedding available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,  # Get more candidates for re-ranking
#             semantic_boost=semantic_boost
#         )
    
#     # Remove empty filter_by
#     if not params['filter_by']:
#         del params['filter_by']
    
#     return params


# def build_search_params_with_strategy(
#     query: str,
#     corrected_query: str,
#     intent: str,
#     search_strategy: str,
#     valid_terms: List[str],
#     unknown_terms: List[str],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     safe_search: bool = True,
#     query_embedding: List[float] = None,
#     semantic_boost: float = 0.5
# ) -> Dict:
#     """
#     Builds search parameters based on word discovery strategy.
    
#     STRATEGIES:
#         'strict': Use valid terms as keyword filter, require matches
#         'mixed': One valid term filter + full query search
#         'semantic': Pure embedding search (no keyword filter)
    
#     This implements the two-stage search:
#         1. Keyword filter (valid terms) → Narrow down results
#         2. Embedding rerank (semantic) → Sort filtered results
#     """
#     # Auto-extract filters from query
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # =========================================================================
#     # BUILD QUERY STRING BASED ON STRATEGY
#     # =========================================================================
    
#     if search_strategy == 'strict':
#         # STRICT: Search only for valid terms (they exist in our dictionary)
#         # This eliminates generic word matching
#         search_query = ' '.join(valid_terms)
        
#     elif search_strategy == 'mixed':
#         # MIXED: Use corrected query (includes both valid and unknown)
#         search_query = corrected_query
        
#     else:  # semantic
#         # SEMANTIC: Use full corrected query, rely on embedding
#         search_query = corrected_query
    
#     # =========================================================================
#     # BUILD BASE PARAMS
#     # =========================================================================
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': build_query_weights(intent),
#         'filter_by': build_filter_string(
#             filters=filters,
#             intent=intent,
#             time_start=time_start,
#             time_end=time_end,
#             location=location,
#             content_type=content_type
#         ),
#         'sort_by': build_sort_string(intent, user_location),
#         'page': page,
#         'per_page': per_page,
#         'highlight_full_fields': 'document_title,document_summary,key_facts',
#         'highlight_start_tag': '<mark>',
#         'highlight_end_tag': '</mark>',
#         'snippet_threshold': 500,
#         'exclude_fields': 'embedding',
#     }
    
#     # =========================================================================
#     # STRATEGY-SPECIFIC SETTINGS
#     # =========================================================================
    
#     if search_strategy == 'strict':
#         # STRICT: Require all terms to match, very few typos
#         params['drop_tokens_threshold'] = 1      # Almost no token dropping
#         params['typo_tokens_threshold'] = 2      # Very strict typos
#         params['num_typos'] = 0                  # No typos allowed
        
#         # For strict mode, we want exact phrase matching if possible
#         if len(valid_terms) >= 2:
#             # Check if this might be a known phrase (bigram)
#             phrase = ' '.join(valid_terms)
#             # Use infix search for better phrase matching
#             params['infix'] = 'fallback'
        
#     elif search_strategy == 'mixed':
#         # MIXED: Moderate strictness
#         params['drop_tokens_threshold'] = 3
#         params['typo_tokens_threshold'] = 3
#         params['num_typos'] = 1
        
#     else:  # semantic
#         # SEMANTIC: Looser text matching, rely on embedding
#         params['drop_tokens_threshold'] = 5
#         params['typo_tokens_threshold'] = 5
#         params['num_typos'] = 2
    
#     # =========================================================================
#     # ADD VECTOR QUERY (for reranking)
#     # =========================================================================
    
#     if query_embedding:
#         # Adjust k based on strategy
#         if search_strategy == 'strict':
#             k = per_page  # Smaller k, we already filtered
#         else:
#             k = per_page * 2  # Larger k for more candidates
        
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=k,
#             semantic_boost=semantic_boost
#         )
    
#     # =========================================================================
#     # CLEANUP
#     # =========================================================================
    
#     # Remove empty filter_by
#     if not params.get('filter_by'):
#         params.pop('filter_by', None)
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, semantic_boost: float = 0.5, query: str = '') -> float:
#     """
#     Combines text_match, vector_distance, and authority scores.
#     Gives bonus for key_facts matches.
    
#     Updated to be stricter and favor specific matches over generic ones.
#     """
#     # Get Typesense's relevance score
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Get vector distance (lower is better, convert to similarity)
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)  # Convert distance to similarity
    
#     # Get document data
#     doc = hit.get('document', {})
    
#     # Get authority score
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Check for key_facts match - give bonus if query terms appear in key_facts
#     key_facts_bonus = 0.0
#     key_facts = doc.get('key_facts', [])
#     if key_facts and query:
#         query_terms = get_specific_terms(query.lower())
#         key_facts_text = ' '.join(key_facts).lower()
        
#         matches = sum(1 for term in query_terms if term in key_facts_text)
#         if matches > 0:
#             # Significant bonus for key_facts matches (0.1 to 0.3)
#             key_facts_bonus = min(0.3, matches * 0.1)
    
#     # Check for title match - bonus if query terms in title
#     title_bonus = 0.0
#     title = doc.get('document_title', '').lower()
#     if title and query:
#         query_terms = get_specific_terms(query.lower())
#         matches = sum(1 for term in query_terms if term in title)
#         if matches > 0:
#             title_bonus = min(0.2, matches * 0.1)
    
#     # Get freshness score (less important for historical content)
#     freshness = 0.5
#     if doc.get('published_date'):
#         days_old = (datetime.now().timestamp() - doc['published_date']) / 86400
#         if days_old < 30:
#             freshness = 1.0
#         elif days_old < 180:
#             freshness = 0.8
#         elif days_old < 365:
#             freshness = 0.6
#         else:
#             freshness = 0.4
    
#     # Weighted combination
#     # Adjust weights based on semantic_boost
#     text_weight = 0.35 * (1 - semantic_boost * 0.5)
#     vector_weight = 0.25 * (0.5 + semantic_boost * 0.5)
#     authority_weight = 0.20
#     freshness_weight = 0.05
    
#     # Base score
#     base_score = (
#         text_score * text_weight +
#         vector_similarity * vector_weight +
#         authority * authority_weight +
#         freshness * freshness_weight
#     )
    
#     # Add bonuses for specific matches
#     final_score = base_score + key_facts_bonus + title_bonus
    
#     return round(min(1.0, final_score), 4)  # Cap at 1.0


# def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
#     """Removes near-duplicate documents using cluster_uuid."""
#     seen_clusters = {}
#     deduplicated = []
#     duplicates_grouped = {}
    
#     for result in results:
#         cluster = result.get('cluster_uuid')
        
#         if cluster and cluster in seen_clusters:
#             if cluster not in duplicates_grouped:
#                 duplicates_grouped[cluster] = []
#             duplicates_grouped[cluster].append(result)
#         else:
#             if cluster:
#                 seen_clusters[cluster] = len(deduplicated)
#             deduplicated.append(result)
    
#     for i, result in enumerate(deduplicated):
#         cluster = result.get('cluster_uuid')
#         if cluster in duplicates_grouped:
#             deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
#     return deduplicated


# def format_result(hit: Dict, semantic_boost: float = 0.5, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     # Build highlight map
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     # Get vector distance for display (if available)
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),  # Include key facts
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,  # Vector similarity score
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, semantic_boost, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, semantic_boost: float = 0.5, query: str = '') -> List[Dict]:
#     """
#     Processes Typesense response into clean result list.
#     Filters out weak matches below MIN_SCORE_THRESHOLD.
#     """
#     hits = raw_response.get('hits', [])
    
#     results = [format_result(hit, semantic_boost, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     # Filter out weak matches
#     filtered_results = [r for r in results if r['score'] >= MIN_SCORE_THRESHOLD]
    
#     # If all results filtered out, keep top 3 anyway (better than empty)
#     if not filtered_results and results:
#         filtered_results = results[:3]
    
#     # results = deduplicate_by_cluster(filtered_results)
    
#     return filtered_results


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],  # NEW
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# # ============================================================================
# # MAIN SEARCH FUNCTIONS
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """
#     Execute search using multi_search endpoint.
    
#     Why: Regular search has URL length limits.
#     Vector queries are large, so we use multi_search with POST body.
#     """
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def execute_search(query: str, options: Dict = None) -> Dict:
#     """
#     Low-level search execution.
#     Routes to multi_search if vector query present.
#     """
#     options = options or {}
    
#     # Use multi_search if vector query is present (to avoid URL length issues)
#     if 'vector_query' in options:
#         return execute_search_multi(options)
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(options)
#         return response
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # def execute_full_search(
# #     query: str,
# #     session_id: str = None,
# #     filters: Dict = None,
# #     page: int = 1,
# #     per_page: int = 20,
# #     user_location: Tuple[float, float] = None,
# #     pos_tags: List[Tuple] = None,
# #     safe_search: bool = True,
# #     alt_mode: str = 'n'  # 'y' = semantic heavy, 'n' = normal
# # ) -> Dict:
# #     """
# #     Main entry point for search.
    
# #     NEW TWO-STAGE SEARCH:
# #         1. Word Discovery: Validate query terms against dictionary
# #         2. Keyword Filter: Use valid terms to filter Typesense results
# #         3. Embedding Rerank: Use semantic search to rank filtered results
    
# #     SEARCH STRATEGIES (based on word discovery):
# #         - 'strict': 2+ valid terms → Filter on keywords, then rerank
# #         - 'mixed': 1 valid term → Loose filter + embedding
# #         - 'semantic': 0 valid terms → Pure embedding search
    
# #     PARAMETER:
# #         alt_mode: str - 'y' for semantic-heavy search, 'n' for normal.
# #                        Use 'y' when user types custom query not found
# #                        in dropdown suggestions.
# #     """
# #     import time
# #     start_time = time.time()
    
# #     # =========================================================================
# #     # STEP 1: Word Discovery - Validate terms and determine strategy
# #     # =========================================================================
# #     if WORD_DISCOVERY_AVAILABLE:
# #         discovery = process_query_optimized(query, verbose=False)
        
# #         search_strategy = discovery.get('search_strategy', 'semantic')
# #         valid_terms = get_filter_terms(discovery)
# #         unknown_terms = get_loose_terms(discovery)
# #         corrected_query = discovery.get('corrected_query', query)
# #         search_terms = get_all_search_terms(discovery)
# #     else:
# #         # Fallback: No word discovery, use semantic search
# #         discovery = {
# #             'valid_count': 0,
# #             'unknown_count': len(query.split()),
# #             'corrected_terms': []
# #         }
# #         search_strategy = 'semantic'
# #         valid_terms = []
# #         unknown_terms = query.split()
# #         corrected_query = query
# #         search_terms = query.split()
    
# #     # Override to semantic if alt_mode='y' (user typed custom query)
# #     if alt_mode == 'y':
# #         search_strategy = 'semantic'
    
# #     # =========================================================================
# #     # STEP 2: Detect intent (for weighting)
# #     # =========================================================================
# #     intent = detect_query_intent(query, pos_tags)
    
# #     # =========================================================================
# #     # STEP 3: Calculate semantic boost based on strategy
# #     # =========================================================================
# #     if search_strategy == 'strict':
# #         # Keyword filter is strong, less semantic needed
# #         semantic_boost = 0.3
# #     elif search_strategy == 'mixed':
# #         # One valid term, need more semantic help
# #         semantic_boost = 0.5
# #     else:  # semantic
# #         # All unknown or alt_mode, heavy semantic
# #         semantic_boost = 0.8
    
# #     # Adjust for intent
# #     if intent in ('person', 'historical'):
# #         semantic_boost = min(1.0, semantic_boost + 0.1)
    
# #     # =========================================================================
# #     # STEP 4: Generate query embedding (for reranking)
# #     # =========================================================================
# #     # Only generate embedding if needed for strategy
# #     if search_strategy == 'semantic' or alt_mode == 'y':
# #         query_embedding = get_query_embedding(corrected_query)
# #     elif search_strategy == 'mixed':
# #         query_embedding = get_query_embedding(corrected_query)
# #     else:
# #         # Strict mode - embedding optional, only for reranking
# #         query_embedding = get_query_embedding(corrected_query) if per_page > 10 else None
    
# #     semantic_enabled = query_embedding is not None
    
# #     # =========================================================================
# #     # STEP 5: Build search parameters based on strategy
# #     # =========================================================================
# #     search_params = build_search_params_with_strategy(
# #         query=query,
# #         corrected_query=corrected_query,
# #         intent=intent,
# #         search_strategy=search_strategy,
# #         valid_terms=valid_terms,
# #         unknown_terms=unknown_terms,
# #         filters=filters,
# #         page=page,
# #         per_page=per_page,
# #         user_location=user_location,
# #         safe_search=safe_search,
# #         query_embedding=query_embedding,
# #         semantic_boost=semantic_boost
# #     )
    
# #     # =========================================================================
# #     # DEBUG LOGGING
# #     # =========================================================================
# #     print("=" * 60)
# #     print("TYPESENSE TWO-STAGE SEARCH DEBUG")
# #     print("=" * 60)
# #     print(f"Query: '{query}'")
# #     print(f"Corrected: '{corrected_query}'")
# #     print(f"Valid terms: {valid_terms}")
# #     print(f"Unknown terms: {unknown_terms}")
# #     print(f"Search strategy: {search_strategy.upper()}")
# #     print(f"Intent: {intent}")
# #     print(f"Semantic enabled: {semantic_enabled}")
# #     print(f"Semantic boost: {semantic_boost}")
# #     print(f"Alt mode: {alt_mode}")
# #     if 'vector_query' in search_params:
# #         print(f"Vector query: YES (alpha={semantic_boost})")
# #     else:
# #         print("Vector query: NO (text-only search)")
# #     print("=" * 60)
    
# #     # =========================================================================
# #     # STEP 6: Execute search
# #     # =========================================================================
# #     raw_response = execute_search(query, search_params)
    
# #     # DEBUG LOGGING
# #     print(f"Typesense found: {raw_response.get('found', 0)} documents")
# #     print(f"Hits returned: {len(raw_response.get('hits', []))}")
# #     if raw_response.get('hits'):
# #         first_hit = raw_response['hits'][0]
# #         first_doc = first_hit.get('document', {})
# #         print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
# #         print(f"First hit key_facts: {first_doc.get('key_facts', [])[:2]}")
# #         if first_hit.get('vector_distance'):
# #             print(f"First hit vector_distance: {first_hit.get('vector_distance')}")
# #     if raw_response.get('error'):
# #         print(f"ERROR: {raw_response.get('error')}")
# #     print("=" * 60)
    
# #     # =========================================================================
# #     # STEP 7: Process results
# #     # =========================================================================
# #     results = process_results(raw_response, semantic_boost, query)
    
# #     # =========================================================================
# #     # STEP 8: Build final response
# #     # =========================================================================
# #     search_time = round(time.time() - start_time, 3)
    
# #     return {
# #         'query': query,
# #         'corrected_query': corrected_query,
# #         'intent': intent,
# #         'results': results,
# #         'total': raw_response.get('found', 0),
# #         'page': page,
# #         'per_page': per_page,
# #         'search_time': search_time,
# #         'session_id': session_id,
# #         'semantic_enabled': semantic_enabled,
# #         'semantic_boost': semantic_boost,
# #         'alt_mode': alt_mode,
# #         # NEW: Word discovery info
# #         'search_strategy': search_strategy,
# #         'valid_terms': valid_terms,
# #         'unknown_terms': unknown_terms,
# #         'word_discovery': {
# #             'valid_count': discovery.get('valid_count', 0),
# #             'unknown_count': discovery.get('unknown_count', 0),
# #             'corrections': discovery.get('corrected_terms', [])
# #         },
# #         'filters_applied': {
# #             'time_period': extract_time_period_from_query(query),
# #             'location': extract_location_from_query(query),
# #             'content_type': extract_content_type_from_query(query)
# #         }
# #     }

# def execute_full_search(
#     query: str,
#     session_id: str = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     user_location: Tuple[float, float] = None,
#     pos_tags: List[Tuple] = None,
#     safe_search: bool = True,
#     alt_mode: str = 'n'
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     TWO-STAGE SEARCH:
#         1. Word Discovery: Validate query terms against dictionary
#         2. Keyword Filter: Use valid terms to filter Typesense results
#         3. Embedding Rerank: Use semantic search to rank filtered results
    
#     SEARCH STRATEGIES (based on word discovery):
#         - 'strict': 2+ valid terms → Filter on keywords, then rerank
#         - 'mixed': 1 valid term → Loose filter + embedding
#         - 'semantic': 0 valid terms → Pure embedding search
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # STEP 1: Word Discovery - Validate terms and determine strategy
#     # =========================================================================
#     t1 = time.time()
#     if WORD_DISCOVERY_AVAILABLE:
#         discovery = process_query_optimized(query, verbose=False)
        
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#         corrected_query = discovery.get('corrected_query', query)
#         search_terms = get_all_search_terms(discovery)
#     else:
#         discovery = {
#             'valid_count': 0,
#             'unknown_count': len(query.split()),
#             'corrected_terms': []
#         }
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
#         corrected_query = query
#         search_terms = query.split()
    
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     times['word_discovery'] = round((time.time() - t1) * 1000, 2)
    
#     # =========================================================================
#     # STEP 2: Detect intent (for weighting)
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 3: Calculate semantic boost based on strategy
#     # =========================================================================
#     if search_strategy == 'strict':
#         semantic_boost = 0.3
#     elif search_strategy == 'mixed':
#         semantic_boost = 0.5
#     else:
#         semantic_boost = 0.8
    
#     if intent in ('person', 'historical'):
#         semantic_boost = min(1.0, semantic_boost + 0.1)
    
#     # =========================================================================
#     # STEP 4: Generate query embedding (for reranking)
#     # =========================================================================
#     t2 = time.time()
#     if search_strategy == 'semantic' or alt_mode == 'y':
#         query_embedding = get_query_embedding(corrected_query)
#     elif search_strategy == 'mixed':
#         query_embedding = get_query_embedding(corrected_query)
#     else:
#         query_embedding = get_query_embedding(corrected_query) if per_page > 10 else None
    
#     semantic_enabled = query_embedding is not None
#     times['embedding'] = round((time.time() - t2) * 1000, 2)
    
#     # =========================================================================
#     # STEP 5: Build search parameters based on strategy
#     # =========================================================================
#     search_params = build_search_params_with_strategy(
#         query=query,
#         corrected_query=corrected_query,
#         intent=intent,
#         search_strategy=search_strategy,
#         valid_terms=valid_terms,
#         unknown_terms=unknown_terms,
#         filters=filters,
#         page=page,
#         per_page=per_page,
#         user_location=user_location,
#         safe_search=safe_search,
#         query_embedding=query_embedding,
#         semantic_boost=semantic_boost
#     )
    
#     # =========================================================================
#     # STEP 6: Execute search
#     # =========================================================================
#     t3 = time.time()
#     raw_response = execute_search(query, search_params)
#     times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # TIMING OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
    
#     # =========================================================================
#     # STEP 7: Process results
#     # =========================================================================
#     results = process_results(raw_response, semantic_boost, query)
    
#     # =========================================================================
#     # STEP 8: Build final response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode,
#         'search_strategy': search_strategy,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', [])
#         },
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }
# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Simple search for autocomplete or quick lookups.
#     Text-only, no semantic.
#     """
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts,primary_keywords',
#         'per_page': limit,
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return [hit['document'] for hit in response.get('hits', [])]
#     except:
#         return []


# def semantic_search_only(query: str, limit: int = 10) -> List[Dict]:
#     """
#     Pure semantic search - useful for "find similar" features.
#     """
#     query_embedding = get_query_embedding(query)
#     if not query_embedding:
#         return []
    
#     params = {
#         'q': '*',  # Match all, rely only on vector
#         'vector_query': build_vector_query(query_embedding, k=limit, semantic_boost=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
#     Uses the document's embedding to find neighbors.
    
#     Useful for "Related articles" feature.
#     """
#     # First, get the document's embedding
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         # Search for similar
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,  # +1 to exclude self
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'  # Exclude the source document
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# """
# typesense_calculations.py (REFACTORED v2)

# CHANGES FROM ORIGINAL:
# - KEEPS word discovery (it's fast with batch Redis)
# - Uses q:'*' for semantic mode (skips slow full-text matching)
# - Full-text only used in 'strict' mode when we have valid terms
# - Embedding uses corrected_query from word discovery

# PERFORMANCE:
# - Word discovery: ~10-30ms (batch Redis - KEEP)
# - Embedding: ~50-100ms (unavoidable - KEEP)
# - Full-text search: ~200-500ms (REMOVED in semantic mode)
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from datetime import datetime
# from decouple import config

# # Import word discovery for search strategy
# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     WORD_DISCOVERY_AVAILABLE = False
#     print("⚠️ word_discovery not available, using basic search")


# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False


# def get_embedding_model():
#     """Lazy loads the embedding model on first use."""
#     global _embedding_model, _embedding_model_failed
    
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """Generates embedding vector for a search query."""
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.25

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     # Default: authority + freshness (vector distance handled by Typesense)
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """
#     SEMANTIC MODE: Pure vector search, NO full-text matching.
    
#     Uses q:'*' which tells Typesense to match all documents
#     and rely entirely on vector similarity. This is FAST.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',  # KEY: Match all, skip text search
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0  # Pure vector
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     MIXED MODE: Light text matching + vector.
    
#     Uses reduced fields and high alpha (vector-dominant).
#     Only searches title and key_facts for text.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',  # Reduced fields
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8  # Vector-heavy
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     STRICT MODE: Text search on valid terms + optional vector rerank.
    
#     Used when word discovery found 2+ valid terms.
#     Searches only for known terms (higher precision).
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # Search for valid terms only
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,  # Strict
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,  # No typos
#     }
    
#     # Add vector for reranking if available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3  # Text-heavy for strict mode
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     FALLBACK: Full text search when embedding fails.
#     This is the SLOW path - only used when necessary.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Text match score (if available)
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Weighted combination
#     if text_score > 0:
#         # Hybrid mode: blend all three
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         # Pure semantic: vector + authority
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     filtered_results = [r for r in results if r['score'] >= MIN_SCORE_THRESHOLD]
    
#     if not filtered_results and results:
#         filtered_results = results[:3]
    
#     return filtered_results


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # MAIN SEARCH FUNCTION
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
#     alt_mode: str = 'n'
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     SEARCH FLOW:
#         1. Word Discovery (batch Redis) → spelling correction, strategy
#         2. Generate embedding (on corrected query)
#         3. Execute search based on strategy:
#            - 'semantic': q='*' + vector (FAST)
#            - 'mixed': light text + vector (MEDIUM)
#            - 'strict': text on valid terms + vector rerank (PRECISE)
#         4. Fallback to text-only if embedding fails
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # STEP 1: Word Discovery - spelling correction & strategy
#     # =========================================================================
#     t1 = time.time()
    
#     if WORD_DISCOVERY_AVAILABLE:
#         discovery = process_query_optimized(query, verbose=False)
        
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#         corrected_query = discovery.get('corrected_query', query)
#     else:
#         discovery = {'valid_count': 0, 'unknown_count': len(query.split())}
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
#         corrected_query = query
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     times['word_discovery'] = round((time.time() - t1) * 1000, 2)
    
#     # =========================================================================
#     # STEP 2: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 3: Generate embedding (on CORRECTED query)
#     # =========================================================================
#     t2 = time.time()
#     query_embedding = get_query_embedding(corrected_query)
#     semantic_enabled = query_embedding is not None
#     times['embedding'] = round((time.time() - t2) * 1000, 2)
    
#     # =========================================================================
#     # STEP 4: Build search params based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         # Fallback: No embedding, use text search
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     elif search_strategy == 'strict' and valid_terms:
#         # STRICT: Text search on valid terms + vector rerank
#         actual_strategy = 'strict'
#         search_params = build_strict_params(
#             query=query,
#             valid_terms=valid_terms,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     elif search_strategy == 'mixed':
#         # MIXED: Light text + vector
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     else:
#         # SEMANTIC: Pure vector search (DEFAULT, FASTEST)
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
    
#     # =========================================================================
#     # STEP 5: Execute search
#     # =========================================================================
#     t3 = time.time()
#     raw_response = execute_search_multi(search_params)
#     times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 6: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     # =========================================================================
#     # STEP 7: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': actual_strategy,
#         'alt_mode': alt_mode,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', [])
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         # Fallback to simple text
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w.lower() not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass






# ###################Works 

# """
# typesense_calculations.py (REFACTORED v2)

# CHANGES FROM ORIGINAL:
# - KEEPS word discovery (it's fast with batch Redis)
# - Uses q:'*' for semantic mode (skips slow full-text matching)
# - Full-text only used in 'strict' mode when we have valid terms
# - Embedding uses corrected_query from word discovery

# PERFORMANCE:
# - Word discovery: ~10-30ms (batch Redis - KEEP)
# - Embedding: ~50-100ms (unavoidable - KEEP)
# - Full-text search: ~200-500ms (REMOVED in semantic mode)
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from datetime import datetime
# from decouple import config

# # Import word discovery for search strategy
# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     WORD_DISCOVERY_AVAILABLE = False
#     print("⚠️ word_discovery not available, using basic search")


# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded
# # ============================================================================

# _embedding_model = None
# _embedding_model_failed = False


# def get_embedding_model():
#     """Lazy loads the embedding model on first use."""
#     global _embedding_model, _embedding_model_failed
    
#     if _embedding_model_failed:
#         return None
    
#     if _embedding_model is None:
#         try:
#             from sentence_transformers import SentenceTransformer
#             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
#             print("✅ Embedding model loaded successfully")
#         except Exception as e:
#             print(f"⚠️ Could not load embedding model: {e}")
#             print("   Falling back to text-only search")
#             _embedding_model_failed = True
#             return None
    
#     return _embedding_model


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """Generates embedding vector for a search query."""
#     model = get_embedding_model()
#     if model is None:
#         return None
    
#     try:
#         embedding = model.encode(query)
#         return embedding.tolist()
#     except Exception as e:
#         print(f"⚠️ Embedding generation failed: {e}")
#         return None


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5  # Raised from 0.25 - filters weak semantic matches

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     # Default: authority + freshness (vector distance handled by Typesense)
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """
#     SEMANTIC MODE: Pure vector search, NO full-text matching.
    
#     Uses q:'*' which tells Typesense to match all documents
#     and rely entirely on vector similarity. This is FAST.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',  # KEY: Match all, skip text search
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0  # Pure vector
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     MIXED MODE: Light text matching + vector.
    
#     Uses reduced fields and high alpha (vector-dominant).
#     Only searches title and key_facts for text.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',  # Reduced fields
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8  # Vector-heavy
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     STRICT MODE: Text search on valid terms + optional vector rerank.
    
#     Used when word discovery found 2+ valid terms.
#     Searches only for known terms (higher precision).
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # Search for valid terms only
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,  # Strict
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,  # No typos
#     }
    
#     # Add vector for reranking if available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3  # Text-heavy for strict mode
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     FALLBACK: Full text search when embedding fails.
#     This is the SLOW path - only used when necessary.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Text match score (if available)
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Weighted combination
#     if text_score > 0:
#         # Hybrid mode: blend all three
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         # Pure semantic: vector + authority
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """
#     Processes Typesense response into clean result list.
    
#     Filtering:
#     1. Minimum threshold (0.5) - removes weak matches
#     2. Relative cutoff (70% of top score) - removes results far below best match
#     """
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     # Get top score as reference
#     top_score = results[0]['score']
    
#     # Calculate relative cutoff (70% of top score)
#     relative_cutoff = top_score * 0.7
    
#     # Use the higher of: minimum threshold OR relative cutoff
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     # Filter results
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     # If all results filtered out, keep top result only
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     # Cap at 20 results max
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # MAIN SEARCH FUNCTION
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
#     alt_mode: str = 'n'
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     SEARCH FLOW:
#         1. Word Discovery (batch Redis) → spelling correction, strategy
#         2. Generate embedding (on corrected query)
#         3. Execute search based on strategy:
#            - 'semantic': q='*' + vector (FAST)
#            - 'mixed': light text + vector (MEDIUM)
#            - 'strict': text on valid terms + vector rerank (PRECISE)
#         4. Fallback to text-only if embedding fails
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # STEP 1: Word Discovery - spelling correction & strategy
#     # =========================================================================
#     t1 = time.time()
    
#     if WORD_DISCOVERY_AVAILABLE:
#         discovery = process_query_optimized(query, verbose=False)
        
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#         corrected_query = discovery.get('corrected_query', query)
#     else:
#         discovery = {'valid_count': 0, 'unknown_count': len(query.split())}
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
#         corrected_query = query
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     times['word_discovery'] = round((time.time() - t1) * 1000, 2)
    
#     # =========================================================================
#     # STEP 2: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 3: Generate embedding (on CORRECTED query)
#     # =========================================================================
#     t2 = time.time()
#     query_embedding = get_query_embedding(corrected_query)
#     semantic_enabled = query_embedding is not None
#     times['embedding'] = round((time.time() - t2) * 1000, 2)
    
#     # =========================================================================
#     # STEP 4: Build search params based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         # Fallback: No embedding, use text search
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     elif search_strategy == 'strict' and valid_terms:
#         # STRICT: Text search on valid terms + vector rerank
#         actual_strategy = 'strict'
#         search_params = build_strict_params(
#             query=query,
#             valid_terms=valid_terms,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     elif search_strategy == 'mixed':
#         # MIXED: Light text + vector
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#     else:
#         # SEMANTIC: Pure vector search (DEFAULT, FASTEST)
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
    
#     # =========================================================================
#     # STEP 5: Execute search
#     # =========================================================================
#     t3 = time.time()
#     raw_response = execute_search_multi(search_params)
#     times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 6: Process results (with filtering)
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     # Debug: Show filtering effect
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 7: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': actual_strategy,
#         'alt_mode': alt_mode,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', [])
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         # Fallback to simple text
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w.lower() not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass

















# """
# typesense_calculations.py (REFACTORED v2)

# CHANGES FROM ORIGINAL:
# - KEEPS word discovery (it's fast with batch Redis)
# - Uses q:'*' for semantic mode (skips slow full-text matching)
# - Full-text only used in 'strict' mode when we have valid terms
# - Embedding uses corrected_query from word discovery

# PERFORMANCE:
# - Word discovery: ~10-30ms (batch Redis - KEEP)
# - Embedding: ~50-100ms (unavoidable - KEEP)
# - Full-text search: ~200-500ms (REMOVED in semantic mode)
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from .embedding_client import get_query_embedding



# # Import word discovery for search strategy
# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     WORD_DISCOVERY_AVAILABLE = False
#     print("⚠️ word_discovery not available, using basic search")


# # ============================================================================
# # EMBEDDING MODEL - Lazy loaded
# # ============================================================================

# # _embedding_model = None
# # _embedding_model_failed = False


# # def get_embedding_model():
# #     """Lazy loads the embedding model on first use."""
# #     global _embedding_model, _embedding_model_failed
    
# #     if _embedding_model_failed:
# #         return None
    
# #     if _embedding_model is None:
# #         try:
# #             from sentence_transformers import SentenceTransformer
# #             _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
# #             print("✅ Embedding model loaded successfully")
# #         except Exception as e:
# #             print(f"⚠️ Could not load embedding model: {e}")
# #             print("   Falling back to text-only search")
# #             _embedding_model_failed = True
# #             return None
    
# #     return _embedding_model


# # def get_query_embedding(query: str) -> Optional[List[float]]:
# #     """Generates embedding vector for a search query."""
# #     model = get_embedding_model()
# #     if model is None:
# #         return None
    
# #     try:
# #         embedding = model.encode(query)
# #         return embedding.tolist()
# #     except Exception as e:
# #         print(f"⚠️ Embedding generation failed: {e}")
# #         return None


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5  # Raised from 0.25 - filters weak semantic matches

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     # Default: authority + freshness (vector distance handled by Typesense)
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """
#     SEMANTIC MODE: Pure vector search, NO full-text matching.
    
#     Uses q:'*' which tells Typesense to match all documents
#     and rely entirely on vector similarity. This is FAST.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',  # KEY: Match all, skip text search
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0  # Pure vector
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     MIXED MODE: Light text matching + vector.
    
#     Uses reduced fields and high alpha (vector-dominant).
#     Only searches title and key_facts for text.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',  # Reduced fields
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8  # Vector-heavy
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     STRICT MODE: Text search on valid terms + optional vector rerank.
    
#     Used when word discovery found 2+ valid terms.
#     Searches only for known terms (higher precision).
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     # Search for valid terms only
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,  # Strict
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,  # No typos
#     }
    
#     # Add vector for reranking if available
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3  # Text-heavy for strict mode
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     FALLBACK: Full text search when embedding fails.
#     This is the SLOW path - only used when necessary.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     # Text match score (if available)
#     text_score = hit.get('text_match', 0) / 100000000
    
#     # Weighted combination
#     if text_score > 0:
#         # Hybrid mode: blend all three
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         # Pure semantic: vector + authority
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """
#     Processes Typesense response into clean result list.
    
#     Filtering:
#     1. Minimum threshold (0.5) - removes weak matches
#     2. Relative cutoff (70% of top score) - removes results far below best match
#     """
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     # Get top score as reference
#     top_score = results[0]['score']
    
#     # Calculate relative cutoff (70% of top score)
#     relative_cutoff = top_score * 0.7
    
#     # Use the higher of: minimum threshold OR relative cutoff
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     # Filter results
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     # If all results filtered out, keep top result only
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     # Cap at 20 results max
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """
#     STAGE 1: Text search to find candidate documents.
    
#     Returns list of document_uuids that match the keywords.
#     No vector search - pure text matching.
#     """
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',  # Only get IDs
#         'drop_tokens_threshold': 1,  # Strict matching
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     # Add filters
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         # Extract document UUIDs
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:  # Show top 5
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """
#     STAGE 2: Vector search filtered to specific documents.
    
#     Reranks the candidate documents by semantic relevance.
#     """
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         # No embedding - just return the documents without reranking
#         # Fetch them by ID
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     # Build filter for document IDs
#     # Typesense array filter syntax: field:[value1, value2, ...]
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """
#     Two-stage search: Keyword filter THEN vector rerank.
    
#     Stage 1: Text search finds candidate documents (max 50)
#     Stage 2: Vector search reranks only those candidates
#     """
#     # Stage 1: Get candidate document IDs via keyword matching
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         # Fallback to pure semantic if no keyword matches
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     # Stage 2: Rerank candidates by vector similarity
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION
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
#     alt_mode: str = 'n'
# ) -> Dict:
#     """
#     Main entry point for search.
    
#     SEARCH FLOW:
#         1. Word Discovery (batch Redis) → spelling correction, strategy
#         2. Generate embedding (on corrected query)
#         3. Execute search based on strategy:
#            - 'semantic': q='*' + vector (FAST)
#            - 'mixed': light text + vector (MEDIUM)
#            - 'strict': text on valid terms + vector rerank (PRECISE)
#         4. Fallback to text-only if embedding fails
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # STEP 1: Word Discovery - spelling correction & strategy
#     # =========================================================================
#     t1 = time.time()
    
#     if WORD_DISCOVERY_AVAILABLE:
#         discovery = process_query_optimized(query, verbose=False)
        
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#         corrected_query = discovery.get('corrected_query', query)
#     else:
#         discovery = {'valid_count': 0, 'unknown_count': len(query.split())}
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
#         corrected_query = query
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     times['word_discovery'] = round((time.time() - t1) * 1000, 2)
    
#     # =========================================================================
#     # STEP 2: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 3: Generate embedding (on CORRECTED query)
#     # =========================================================================
#     t2 = time.time()
#     query_embedding = get_query_embedding(corrected_query)
#     semantic_enabled = query_embedding is not None
#     times['embedding'] = round((time.time() - t2) * 1000, 2)
    
#     # =========================================================================
#     # STEP 4: Build search params based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         # Fallback: No embedding, use text search only
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         # TWO-STAGE SEARCH: Keyword filter THEN vector rerank
#         actual_strategy = f'two_stage_{search_strategy}'
        
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         # SEMANTIC: Pure vector search (no valid terms to filter on)
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
        
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 6: Process results (with filtering)
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     # Debug: Show filtering effect
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 7: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': actual_strategy,
#         'alt_mode': alt_mode,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', [])
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         # Fallback to simple text
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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
#             # Include key_facts as suggestions
#             for fact in doc.get('key_facts', [])[:3]:
#                 # Extract key terms from facts
#                 fact_words = [w for w in fact.split() if len(w) > 4 and w.lower() not in query_words]
#                 all_keywords.update(fact_words[:2])
        
#         related = list(all_keywords)[:6]
#         return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
#     except:
#         return []


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass



#                                           Part 2  - This works 

# """
# typesense_calculations.py (OPTIMIZED v3)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# EXPECTED PERFORMANCE GAINS:
# - Short queries (3 words): ~130ms → ~100ms
# - Medium queries (15 words): ~180ms → ~120ms
# - Long queries (50+ words): ~300ms → ~130ms
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
    
#     Example:
#     'I'm looking for a really good Italian restaurant in Buenos Aires 
#      that has outdoor seating and serves authentic pasta with wine'
    
#     Becomes:
#     'Italian restaurant Buenos Aires outdoor seating authentic pasta wine'
    
#     This reduces embedding time from ~200ms to ~30ms for long queries.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     # Collapse whitespace
#     cleaned = ' '.join(cleaned.split())
    
#     # Truncate to max words
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     # Truncate BEFORE sending to model
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=5
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     WORD_DISCOVERY_AVAILABLE = False
#     print("⚠️ word_discovery not available, using basic search")


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'search_strategy': 'semantic',
#         'corrected_query': query,
#         'corrected_terms': []
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
    
#     Saves ~30ms by not waiting for word discovery before embedding.
#     """
#     # Submit both tasks
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     # Wait for both
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrected_terms', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             # Re-embed with corrected query
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search, NO full-text matching."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Text search to find candidate documents."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Keyword filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (OPTIMIZED)
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
#     alt_mode: str = 'n'
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION.
    
#     OPTIMIZATIONS:
#     1. Query truncation before embedding (biggest win for long queries)
#     2. Parallel word discovery + embedding
#     3. Connection pooling for HTTP requests
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # STEP 1 + 2: PARALLEL - Word Discovery + Embedding
#     # =========================================================================
#     t1 = time.time()
    
#     discovery, query_embedding = run_parallel_prep(query)
    
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # Extract discovery results
#     if WORD_DISCOVERY_AVAILABLE:
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#     else:
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
    
#     corrected_query = discovery.get('corrected_query', query)
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # STEP 3: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 4: Execute search based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         actual_strategy = f'two_stage_{search_strategy}'
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 5: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 6: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
#         'page': page,
#         'per_page': per_page,
#         'search_time': search_time,
#         'session_id': session_id,
#         'semantic_enabled': semantic_enabled,
#         'search_strategy': actual_strategy,
#         'alt_mode': alt_mode,
#         'valid_terms': valid_terms,
#         'unknown_terms': unknown_terms,
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', [])
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
#         embedding = doc.get('embedding')
        
#         if not embedding:
#             return []
        
#         params = {
#             'q': '*',
#             'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
#             'per_page': limit + 1,
#             'exclude_fields': 'embedding',
#             'filter_by': f'document_uuid:!={document_uuid}'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])][:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass





# """
# typesense_calculations.py (OPTIMIZED v4.1)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# NEW IN v4:
# 5. Score-based strategy selection (uses Redis rank field)
# 6. Semantic UUID support for related content
# 7. Enhanced debug output with term scores

# NEW IN v4.1:
# 8. skip_embedding parameter for dropdown/keyword selections
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
    
#     Example:
#     'I'm looking for a really good Italian restaurant in Buenos Aires 
#      that has outdoor seating and serves authentic pasta with wine'
    
#     Becomes:
#     'Italian restaurant Buenos Aires outdoor seating authentic pasta wine'
    
#     This reduces embedding time from ~200ms to ~30ms for long queries.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     # Collapse whitespace
#     cleaned = ' '.join(cleaned.split())
    
#     # Truncate to max words
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     # Truncate BEFORE sending to model
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2  # Reduced from 5 to fail faster
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms,
#         get_term_scores,
#         get_high_score_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     try:
#         from word_discovery import (
#             process_query_optimized,
#             get_search_strategy,
#             get_filter_terms,
#             get_loose_terms,
#             get_all_search_terms,
#             get_term_scores,
#             get_high_score_terms
#         )
#         WORD_DISCOVERY_AVAILABLE = True
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery not available, using basic search")
        
#         # Fallback implementations
#         def get_term_scores(result):
#             return []
        
#         def get_high_score_terms(result, min_score=500):
#             return []


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'search_strategy': 'semantic',
#         'corrected_query': query,
#         'corrected_terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'terms': []
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
    
#     Saves ~30ms by not waiting for word discovery before embedding.
    
#     Args:
#         query: Search query string
#         skip_embedding: If True, skip embedding call entirely (for dropdown/keyword selections)
#     """
#     if skip_embedding:
#         # Dropdown/keyword selection - skip embedding entirely
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     # Submit both tasks
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     # Wait for both
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrected_terms', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             # Re-embed with corrected query
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search, NO full-text matching."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),  # Include semantic_uuid
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Text search to find candidate documents."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Keyword filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (OPTIMIZED)
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
#     alt_mode: str = 'n',
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION v4.1.
    
#     OPTIMIZATIONS:
#     1. Query truncation before embedding (biggest win for long queries)
#     2. Parallel word discovery + embedding
#     3. Connection pooling for HTTP requests
#     4. Score-based strategy selection
#     5. skip_embedding for dropdown/keyword selections (NEW in v4.1)
    
#     Args:
#         query: Search query string
#         session_id: User session identifier
#         filters: Dict of filter conditions
#         page: Page number for pagination
#         per_page: Results per page
#         user_location: (lat, lng) tuple for location-based sorting
#         pos_tags: Pre-computed POS tags (optional)
#         safe_search: Enable safe search filtering
#         alt_mode: 'y' to force semantic search, 'n' for normal
#         skip_embedding: True to skip embedding (for dropdown/keyword selections)
#         search_source: Source of search ('dropdown', 'keyboard', 'home', etc.)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # AUTO-DETECT: Skip embedding for dropdown/keyword selections
#     # =========================================================================
#     if search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete'):
#         skip_embedding = True
   

# # alt_mode='n' indicates dropdown/keyword selection in your system
#     if alt_mode == 'n':
#         skip_embedding = True
    
#     # Also skip if alt_mode='n' suggests this is a direct keyword click
#     # (You can customize this logic based on your frontend behavior)
    
#     # =========================================================================
#     # STEP 1 + 2: Word Discovery + Embedding (parallel or skip)
#     # =========================================================================
#     t1 = time.time()
    
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # Extract discovery results
#     if WORD_DISCOVERY_AVAILABLE:
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#     else:
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
    
#     corrected_query = discovery.get('corrected_query', query)
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     # If we skipped embedding and strategy wants semantic, force to strict/text
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # STEP 3: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 4: Execute search based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         actual_strategy = f'two_stage_{search_strategy}'
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT (Enhanced with scores)
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#         # Show individual term scores
#         for term in discovery.get('terms', []):
#             if term.get('rank_score', 0) > 0:
#                 print(f"      • {term['search_word']}: {term['rank_score']} pts")
    
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 5: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 6: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': raw_response.get('found', 0),
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
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', []),
#             # Score metrics
#             'total_score': discovery.get('total_score', 0),
#             'average_score': discovery.get('average_score', 0),
#             'max_score': discovery.get('max_score', 0),
#             'term_scores': [
#                 {'term': t['search_word'], 'score': t.get('rank_score', 0)}
#                 for t in discovery.get('terms', [])
#                 if t.get('rank_score', 0) > 0
#             ]
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
    
#     Strategy:
#     1. If document has semantic_uuid, find others in same cluster first
#     2. Fall back to vector similarity if no semantic_uuid or need more results
#     """
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         # Strategy 1: Use semantic_uuid cluster
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]  # e.g., "DOC"
#                 cluster = parts[1]  # e.g., "11"
                
#                 # Find documents in same cluster (excluding self)
#                 # Only use cluster-based search if cluster != 00 (00 = no semantic clustering)
#                 if cluster != '00':
#                     try:
#                         # Search for documents with same prefix-cluster pattern
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
#                         # Fall through to vector search
        
#         # Strategy 2: Vector similarity (for remaining slots or fallback)
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             # Build exclusion filter
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """
#     Find all documents in the same semantic cluster.
    
#     Args:
#         semantic_uuid: e.g., "DOC-11-0042"
#         limit: Maximum documents to return
        
#     Returns:
#         List of documents in the same cluster
#     """
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]  # e.g., "DOC"
#     cluster = parts[1]  # e.g., "11"
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# """
# typesense_calculations.py (OPTIMIZED v4.2)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# NEW IN v4:
# 5. Score-based strategy selection (uses Redis rank field)
# 6. Semantic UUID support for related content
# 7. Enhanced debug output with term scores

# NEW IN v4.1:
# 8. skip_embedding parameter for dropdown/keyword selections

# NEW IN v4.2:
# 9. DROPDOWN FAST PATH - skip word discovery entirely for dropdown selections
#    When user selects from dropdown, search the exact phrase directly
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
    
#     Example:
#     'I'm looking for a really good Italian restaurant in Buenos Aires 
#      that has outdoor seating and serves authentic pasta with wine'
    
#     Becomes:
#     'Italian restaurant Buenos Aires outdoor seating authentic pasta wine'
    
#     This reduces embedding time from ~200ms to ~30ms for long queries.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     # Collapse whitespace
#     cleaned = ' '.join(cleaned.split())
    
#     # Truncate to max words
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     # Truncate BEFORE sending to model
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2  # Reduced from 5 to fail faster
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms,
#         get_term_scores,
#         get_high_score_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     try:
#         from word_discovery import (
#             process_query_optimized,
#             get_search_strategy,
#             get_filter_terms,
#             get_loose_terms,
#             get_all_search_terms,
#             get_term_scores,
#             get_high_score_terms
#         )
#         WORD_DISCOVERY_AVAILABLE = True
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery not available, using basic search")
        
#         # Fallback implementations
#         def get_term_scores(result):
#             return []
        
#         def get_high_score_terms(result, min_score=500):
#             return []


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'search_strategy': 'semantic',
#         'corrected_query': query,
#         'corrected_terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'terms': []
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
    
#     Saves ~30ms by not waiting for word discovery before embedding.
    
#     Args:
#         query: Search query string
#         skip_embedding: If True, skip embedding call entirely (for dropdown/keyword selections)
#     """
#     if skip_embedding:
#         # Dropdown/keyword selection - skip embedding entirely
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     # Submit both tasks
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     # Wait for both
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrected_terms', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             # Re-embed with corrected query
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """Builds Typesense filter_by string."""
#     conditions = []
    
#     if filters:
#         if filters.get('category'):
#             conditions.append(f"document_category:={filters['category']}")
#         if filters.get('source'):
#             conditions.append(f"document_brand:={filters['source']}")
#         if filters.get('data_type'):
#             conditions.append(f"document_data_type:={filters['data_type']}")
    
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     if location:
#         conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
#     if content_type:
#         conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search, NO full-text matching."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_exact_phrase_params(
#     phrase: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     EXACT PHRASE MODE: For dropdown selections.
#     Searches for the exact phrase without splitting or processing.
#     """
#     location = extract_location_from_query(phrase)
#     time_start, time_end = extract_time_period_from_query(phrase)
#     content_type = extract_content_type_from_query(phrase)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': f'"{phrase}"',  # Quote for exact phrase matching
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'num_typos': 0,  # No typos for exact match
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),  # Include semantic_uuid
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Text search to find candidate documents."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Keyword filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (OPTIMIZED)
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
#     alt_mode: str = 'n',
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION v4.2.
    
#     OPTIMIZATIONS:
#     1. Query truncation before embedding (biggest win for long queries)
#     2. Parallel word discovery + embedding
#     3. Connection pooling for HTTP requests
#     4. Score-based strategy selection
#     5. skip_embedding for dropdown/keyword selections
#     6. DROPDOWN FAST PATH - skip word discovery entirely (NEW in v4.2)
    
#     Args:
#         query: Search query string
#         session_id: User session identifier
#         filters: Dict of filter conditions
#         page: Page number for pagination
#         per_page: Results per page
#         user_location: (lat, lng) tuple for location-based sorting
#         pos_tags: Pre-computed POS tags (optional)
#         safe_search: Enable safe search filtering
#         alt_mode: 'y' to force semantic search, 'n' for normal
#         skip_embedding: True to skip embedding (for dropdown/keyword selections)
#         search_source: Source of search ('dropdown', 'keyboard', 'home', etc.)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # =========================================================================
#     # DROPDOWN FAST PATH - Skip everything, just search the exact phrase
#     # =========================================================================
#     is_dropdown = search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

# # alt_mode='y' forces full semantic processing, even for dropdowns
#     if alt_mode == 'y':
#         is_dropdown = False


#     if is_dropdown:
#         print(f"⚡ DROPDOWN FAST PATH: '{query}'")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build exact phrase search params
#         search_params = build_exact_phrase_params(
#             phrase=query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         # Debug output
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: EXACT_PHRASE (dropdown) | Found: {raw_response.get('found', 0)}")
#         print(f"   Search phrase: \"{query}\"")
        
#         # Process results
#         results = process_results(raw_response, query)
        
#         raw_count = len(raw_response.get('hits', []))
#         filtered_count = len(results)
#         if raw_count > 0:
#             top_score = results[0]['score'] if results else 0
#             print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#             print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
        
#         # Build response
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,  # No correction for dropdown
#             'intent': intent,
#             'results': results,
#             'total': len(results), 
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'exact_phrase',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': [query],  # Whole phrase is the term
#             'unknown_terms': [],
#             'word_discovery': {
#                 'valid_count': 1,
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#                 'term_scores': []
#             },
#             'timings': times,
#             'filters_applied': {
#                 'time_period': extract_time_period_from_query(query),
#                 'location': extract_location_from_query(query),
#                 'content_type': extract_content_type_from_query(query)
#             }
#         }
    
#     # =========================================================================
#     # NORMAL PATH - Full word discovery + embedding
#     # =========================================================================
    
#     # STEP 1 + 2: Word Discovery + Embedding (parallel or skip)
#     t1 = time.time()
    
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # Extract discovery results
#     if WORD_DISCOVERY_AVAILABLE:
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#     else:
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
    
#     corrected_query = discovery.get('corrected_query', query)
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     # If we skipped embedding and strategy wants semantic, force to strict/text
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # STEP 3: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 4: Execute search based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         actual_strategy = f'two_stage_{search_strategy}'
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT (Enhanced with scores)
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#         # Show individual term scores
#         for term in discovery.get('terms', []):
#             if term.get('rank_score', 0) > 0:
#                 print(f"      • {term['search_word']}: {term['rank_score']} pts")
    
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 5: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 6: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': len(results), 
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
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', []),
#             # Score metrics
#             'total_score': discovery.get('total_score', 0),
#             'average_score': discovery.get('average_score', 0),
#             'max_score': discovery.get('max_score', 0),
#             'term_scores': [
#                 {'term': t['search_word'], 'score': t.get('rank_score', 0)}
#                 for t in discovery.get('terms', [])
#                 if t.get('rank_score', 0) > 0
#             ]
#         },
#         'timings': times,
#         'filters_applied': {
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
    
#     Strategy:
#     1. If document has semantic_uuid, find others in same cluster first
#     2. Fall back to vector similarity if no semantic_uuid or need more results
#     """
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         # Strategy 1: Use semantic_uuid cluster
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]  # e.g., "DOC"
#                 cluster = parts[1]  # e.g., "11"
                
#                 # Find documents in same cluster (excluding self)
#                 # Only use cluster-based search if cluster != 00 (00 = no semantic clustering)
#                 if cluster != '00':
#                     try:
#                         # Search for documents with same prefix-cluster pattern
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
#                         # Fall through to vector search
        
#         # Strategy 2: Vector similarity (for remaining slots or fallback)
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             # Build exclusion filter
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """
#     Find all documents in the same semantic cluster.
    
#     Args:
#         semantic_uuid: e.g., "DOC-11-0042"
#         limit: Maximum documents to return
        
#     Returns:
#         List of documents in the same cluster
#     """
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]  # e.g., "DOC"
#     cluster = parts[1]  # e.g., "11"
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 10,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


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


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass



















# """
# typesense_calculations.py (OPTIMIZED v4.3)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# NEW IN v4:
# 5. Score-based strategy selection (uses Redis rank field)
# 6. Semantic UUID support for related content
# 7. Enhanced debug output with term scores

# NEW IN v4.1:
# 8. skip_embedding parameter for dropdown/keyword selections

# NEW IN v4.2:
# 9. DROPDOWN FAST PATH - skip word discovery entirely for dropdown selections
#    When user selects from dropdown, search the exact phrase directly

# NEW IN v4.3:
# 10. Dynamic tab filter support (data_type, category, schema filters)
# 11. Enhanced facet support for tabs
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
    
#     Example:
#     'I'm looking for a really good Italian restaurant in Buenos Aires 
#      that has outdoor seating and serves authentic pasta with wine'
    
#     Becomes:
#     'Italian restaurant Buenos Aires outdoor seating authentic pasta wine'
    
#     This reduces embedding time from ~200ms to ~30ms for long queries.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     # Collapse whitespace
#     cleaned = ' '.join(cleaned.split())
    
#     # Truncate to max words
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     # Truncate BEFORE sending to model
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2  # Reduced from 5 to fail faster
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms,
#         get_term_scores,
#         get_high_score_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     try:
#         from word_discovery import (
#             process_query_optimized,
#             get_search_strategy,
#             get_filter_terms,
#             get_loose_terms,
#             get_all_search_terms,
#             get_term_scores,
#             get_high_score_terms
#         )
#         WORD_DISCOVERY_AVAILABLE = True
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery not available, using basic search")
        
#         # Fallback implementations
#         def get_term_scores(result):
#             return []
        
#         def get_high_score_terms(result, min_score=500):
#             return []


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'search_strategy': 'semantic',
#         'corrected_query': query,
#         'corrected_terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'terms': []
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
    
#     Saves ~30ms by not waiting for word discovery before embedding.
    
#     Args:
#         query: Search query string
#         skip_embedding: If True, skip embedding call entirely (for dropdown/keyword selections)
#     """
#     if skip_embedding:
#         # Dropdown/keyword selection - skip embedding entirely
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     # Submit both tasks
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     # Wait for both
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrected_terms', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             # Re-embed with corrected query
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES (for validation)
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """Extracts location from query."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER & SORT BUILDING (UPDATED FOR DYNAMIC TABS)
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string.
    
#     Supports dynamic tab filters:
#     - data_type: document_data_type (content, service, product, person, media, location)
#     - category: document_category (healthcare_medical, fashion, beauty, etc.)
#     - schema: document_schema (Article, BlogPosting, Product, Service, etc.)
#     - source: document_brand
    
#     Also supports:
#     - time_start/time_end: time_period_start/time_period_end
#     - location: location_city, location_state, location_country, location_region
#     - content_type: document_data_type (from query extraction)
#     """
#     conditions = []
    
#     if filters:
#         # Dynamic tab filter: data_type → document_data_type
#         data_type = filters.get('data_type')
#         if data_type:
#             # Validate against allowed values
#             if data_type in VALID_DATA_TYPES:
#                 conditions.append(f"document_data_type:={data_type}")
#             else:
#                 print(f"⚠️ Invalid data_type filter: {data_type}")
        
#         # Dynamic tab filter: category → document_category
#         category = filters.get('category')
#         if category:
#             # Allow any category (might have custom ones)
#             # Sanitize to prevent injection
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         # Dynamic tab filter: schema → document_schema
#         schema = filters.get('schema')
#         if schema:
#             if schema in VALID_SCHEMAS:
#                 conditions.append(f"document_schema:={schema}")
#             else:
#                 print(f"⚠️ Invalid schema filter: {schema}")
        
#         # Source filter → document_brand
#         source = filters.get('source')
#         if source:
#             # Sanitize
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         # Time range filter
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             if time_conditions:
#                 conditions.extend(time_conditions)
        
#         # Location filter from filters dict
#         loc_filter = filters.get('location')
#         if loc_filter:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', loc_filter)
#             if safe_loc:
#                 conditions.append(
#                     f"(location_city:={safe_loc} || location_state:={safe_loc} || "
#                     f"location_country:={safe_loc} || location_region:={safe_loc})"
#                 )
    
#     # Time period from query extraction
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     # Location from query extraction (if not already set by filters)
#     if location and not (filters and filters.get('location')):
#         conditions.append(
#             f"(location_city:={location} || location_state:={location} || "
#             f"location_country:={location} || location_region:={location})"
#         )
    
#     # Content type from query extraction (if not already set by data_type filter)
#     if content_type and not (filters and filters.get('data_type')):
#         if content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
    
#     now = datetime.now()
    
#     if time_range == 'day':
#         # Last 24 hours
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         # Last 7 days
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         # Last 30 days
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         # Last 365 days
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search, NO full-text matching."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_exact_phrase_params(
#     phrase: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     EXACT PHRASE MODE: For dropdown selections.
#     Searches for the exact phrase without splitting or processing.
#     """
#     location = extract_location_from_query(phrase)
#     time_start, time_end = extract_time_period_from_query(phrase)
#     content_type = extract_content_type_from_query(phrase)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': f'"{phrase}"',  # Quote for exact phrase matching
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'num_typos': 0,  # No typos for exact match
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'schema': doc.get('document_schema', ''),  # Include schema for filtering
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Text search to find candidate documents."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Keyword filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (OPTIMIZED)
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
#     alt_mode: str = 'n',
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION v4.3.
    
#     OPTIMIZATIONS:
#     1. Query truncation before embedding (biggest win for long queries)
#     2. Parallel word discovery + embedding
#     3. Connection pooling for HTTP requests
#     4. Score-based strategy selection
#     5. skip_embedding for dropdown/keyword selections
#     6. DROPDOWN FAST PATH - skip word discovery entirely (v4.2)
#     7. Dynamic tab filter support (v4.3)
    
#     Args:
#         query: Search query string
#         session_id: User session identifier
#         filters: Dict of filter conditions including:
#             - data_type: Filter by document_data_type (content, service, product, etc.)
#             - category: Filter by document_category
#             - schema: Filter by document_schema (Article, Product, etc.)
#             - source: Filter by document_brand
#             - time_range: Filter by time (day, week, month, year)
#             - location: Filter by location
#         page: Page number for pagination
#         per_page: Results per page
#         user_location: (lat, lng) tuple for location-based sorting
#         pos_tags: Pre-computed POS tags (optional)
#         safe_search: Enable safe search filtering
#         alt_mode: 'y' to force semantic search, 'n' for normal
#         skip_embedding: True to skip embedding (for dropdown/keyword selections)
#         search_source: Source of search ('dropdown', 'keyboard', 'home', etc.)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # Log active filters for debugging
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active filters: {active_filters}")
    
#     # =========================================================================
#     # DROPDOWN FAST PATH - Skip everything, just search the exact phrase
#     # =========================================================================
#     is_dropdown = search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

#     # alt_mode='y' forces full semantic processing, even for dropdowns
#     if alt_mode == 'y':
#         is_dropdown = False

#     if is_dropdown:
#         print(f"⚡ DROPDOWN FAST PATH: '{query}'")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build exact phrase search params
#         search_params = build_exact_phrase_params(
#             phrase=query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         # Debug output
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: EXACT_PHRASE (dropdown) | Found: {raw_response.get('found', 0)}")
#         print(f"   Search phrase: \"{query}\"")
        
#         # Process results
#         results = process_results(raw_response, query)
        
#         raw_count = len(raw_response.get('hits', []))
#         filtered_count = len(results)
#         if raw_count > 0:
#             top_score = results[0]['score'] if results else 0
#             print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#             print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
        
#         # Build response
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,  # No correction for dropdown
#             'intent': intent,
#             'results': results,
#             'total': len(results), 
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'exact_phrase',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': [query],  # Whole phrase is the term
#             'unknown_terms': [],
#             'word_discovery': {
#                 'valid_count': 1,
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#                 'term_scores': []
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#                 'time_period': extract_time_period_from_query(query),
#                 'location': extract_location_from_query(query),
#                 'content_type': extract_content_type_from_query(query)
#             }
#         }
    
#     # =========================================================================
#     # NORMAL PATH - Full word discovery + embedding
#     # =========================================================================
    
#     # STEP 1 + 2: Word Discovery + Embedding (parallel or skip)
#     t1 = time.time()
    
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # Extract discovery results
#     if WORD_DISCOVERY_AVAILABLE:
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#     else:
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
    
#     corrected_query = discovery.get('corrected_query', query)
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     # If we skipped embedding and strategy wants semantic, force to strict/text
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # STEP 3: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 4: Execute search based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         actual_strategy = f'two_stage_{search_strategy}'
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT (Enhanced with scores)
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#         # Show individual term scores
#         for term in discovery.get('terms', []):
#             if term.get('rank_score', 0) > 0:
#                 print(f"      • {term['search_word']}: {term['rank_score']} pts")
    
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # =========================================================================
#     # STEP 5: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 6: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': len(results), 
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
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', []),
#             # Score metrics
#             'total_score': discovery.get('total_score', 0),
#             'average_score': discovery.get('average_score', 0),
#             'max_score': discovery.get('max_score', 0),
#             'term_scores': [
#                 {'term': t['search_word'], 'score': t.get('rank_score', 0)}
#                 for t in discovery.get('terms', [])
#                 if t.get('rank_score', 0) > 0
#             ]
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
    
#     Strategy:
#     1. If document has semantic_uuid, find others in same cluster first
#     2. Fall back to vector similarity if no semantic_uuid or need more results
#     """
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         # Strategy 1: Use semantic_uuid cluster
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]  # e.g., "DOC"
#                 cluster = parts[1]  # e.g., "11"
                
#                 # Find documents in same cluster (excluding self)
#                 # Only use cluster-based search if cluster != 00 (00 = no semantic clustering)
#                 if cluster != '00':
#                     try:
#                         # Search for documents with same prefix-cluster pattern
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
#                         # Fall through to vector search
        
#         # Strategy 2: Vector similarity (for remaining slots or fallback)
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             # Build exclusion filter
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """
#     Find all documents in the same semantic cluster.
    
#     Args:
#         semantic_uuid: e.g., "DOC-11-0042"
#         limit: Maximum documents to return
        
#     Returns:
#         List of documents in the same cluster
#     """
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]  # e.g., "DOC"
#     cluster = parts[1]  # e.g., "11"
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """
#     Get facet counts specifically for dynamic tabs.
    
#     Returns facets for:
#     - document_data_type (main tabs)
#     - document_category (secondary filter)
#     - document_schema (tertiary filter)
    
#     This is called WITHOUT any filters to get total counts.
#     """
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0  # Only need facets, not results
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         facets = {
#             'data_type': [],
#             'category': [],
#             'schema': []
#         }
        
#         # Label mappings
#         data_type_labels = {
#                     'article': 'Articles',
#                     'person': 'People',
#                     'business': 'Businesses',
#                     'place': 'Places',
#                     'media': 'Media',
#                     'event': 'Events',
#                     'product': 'Products',
#                 }
        
#         category_labels = {
#             'healthcare_medical': 'Healthcare',
#             'fashion': 'Fashion',
#             'beauty': 'Beauty',
#             'food_recipes': 'Food & Recipes',
#             'travel_tourism': 'Travel',
#             'entertainment': 'Entertainment',
#             'business': 'Business',
#             'education': 'Education',
#             'technology': 'Technology',
#             'sports': 'Sports',
#             'finance': 'Finance',
#             'real_estate': 'Real Estate',
#             'lifestyle': 'Lifestyle',
#             'news': 'News',
#             'culture': 'Culture',
#             'general': 'General',
#         }
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             counts = facet['counts']
            
#             if field == 'document_data_type':
#                 facets['data_type'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': data_type_labels.get(c['value'], c['value'].title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_category':
#                 facets['category'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': category_labels.get(c['value'], c['value'].replace('_', ' ').title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_schema':
#                 facets['schema'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': c['value']
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
        
#         return facets
        
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass



# """
# typesense_calculations.py (OPTIMIZED v4.4)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# NEW IN v4:
# 5. Score-based strategy selection (uses Redis rank field)
# 6. Semantic UUID support for related content
# 7. Enhanced debug output with term scores

# NEW IN v4.1:
# 8. skip_embedding parameter for dropdown/keyword selections

# NEW IN v4.2:
# 9. DROPDOWN FAST PATH - skip word discovery entirely for dropdown selections
#    When user selects from dropdown, search the exact phrase directly

# NEW IN v4.3:
# 10. Dynamic tab filter support (data_type, category, schema filters)
# 11. Enhanced facet support for tabs

# NEW IN v4.4:
# 12. FIXED: Location filtering now handles case sensitivity and state abbreviations
#     - Extracts location as title case ('Georgia' not 'georgia')
#     - Searches for BOTH full state name AND abbreviation ('Georgia' OR 'GA')
#     - Properly matches data regardless of format
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # US STATE ABBREVIATION MAPPING (NEW in v4.4)
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

# # Reverse mapping: abbreviation → full name
# US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
    
#     Example:
#     'I'm looking for a really good Italian restaurant in Buenos Aires 
#      that has outdoor seating and serves authentic pasta with wine'
    
#     Becomes:
#     'Italian restaurant Buenos Aires outdoor seating authentic pasta wine'
    
#     This reduces embedding time from ~200ms to ~30ms for long queries.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     # Collapse whitespace
#     cleaned = ' '.join(cleaned.split())
    
#     # Truncate to max words
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     # Truncate BEFORE sending to model
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2  # Reduced from 5 to fail faster
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

# try:
#     from .word_discovery import (
#         process_query_optimized,
#         get_search_strategy,
#         get_filter_terms,
#         get_loose_terms,
#         get_all_search_terms,
#         get_term_scores,
#         get_high_score_terms
#     )
#     WORD_DISCOVERY_AVAILABLE = True
# except ImportError:
#     try:
#         from word_discovery import (
#             process_query_optimized,
#             get_search_strategy,
#             get_filter_terms,
#             get_loose_terms,
#             get_all_search_terms,
#             get_term_scores,
#             get_high_score_terms
#         )
#         WORD_DISCOVERY_AVAILABLE = True
#     except ImportError:
#         WORD_DISCOVERY_AVAILABLE = False
#         print("⚠️ word_discovery not available, using basic search")
        
#         # Fallback implementations
#         def get_term_scores(result):
#             return []
        
#         def get_high_score_terms(result, min_score=500):
#             return []


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'search_strategy': 'semantic',
#         'corrected_query': query,
#         'corrected_terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'terms': []
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
    
#     Saves ~30ms by not waiting for word discovery before embedding.
    
#     Args:
#         query: Search query string
#         skip_embedding: If True, skip embedding call entirely (for dropdown/keyword selections)
#     """
#     if skip_embedding:
#         # Dropdown/keyword selection - skip embedding entirely
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     # Submit both tasks
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     # Wait for both
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
#     corrected_query = discovery.get('corrected_query', query)
#     if corrected_query.lower() != query.lower():
#         corrections = discovery.get('corrected_terms', [])
#         significant = any(
#             c.get('original', '').lower() != c.get('corrected', '').lower()
#             for c in corrections
#         )
#         if significant and embedding is not None:
#             # Re-embed with corrected query
#             embedding = get_query_embedding(corrected_query)
    
#     return discovery, embedding


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES (for validation)
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# def extract_location_from_query(query: str) -> Optional[str]:
#     """
#     Extracts location from query.
    
#     FIXED in v4.4: Returns title case for proper Typesense matching.
#     'georgia' -> 'Georgia'
#     'new york' -> 'New York'
#     """
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location.title()  # FIXED: Return title case
#     return None


# def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """Extracts time period from query."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # LOCATION FILTER HELPER (NEW in v4.4)
# # ============================================================================

# def _build_location_filter(location: str) -> str:
#     """
#     Build a location filter that handles:
#     1. Title case for proper matching ('Georgia' not 'georgia')
#     2. State abbreviations ('GA' for Georgia, 'FL' for Florida)
#     3. Both city and state fields
    
#     Returns a filter string or empty string.
    
#     Example:
#         'georgia' -> (location_state:=Georgia || location_city:=Georgia || location_state:=GA)
#         'atlanta' -> (location_state:=Atlanta || location_city:=Atlanta)
#     """
#     if not location:
#         return ''
    
#     loc_title = location.title()
#     loc_lower = location.lower()
    
#     # Get state abbreviation if this is a US state
#     abbrev = US_STATE_ABBREV.get(loc_lower, '')
    
#     # Build filter parts
#     filter_parts = [
#         f"location_state:={loc_title}",
#         f"location_city:={loc_title}",
#     ]
    
#     # Add abbreviation variant for states
#     if abbrev:
#         filter_parts.append(f"location_state:={abbrev}")
    
#     return '(' + ' || '.join(filter_parts) + ')'


# # ============================================================================
# # FILTER & SORT BUILDING (UPDATED FOR DYNAMIC TABS + LOCATION FIX)
# # ============================================================================

# def build_filter_string(
#     filters: Dict = None,
#     time_start: int = None,
#     time_end: int = None,
#     location: str = None,
#     content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string.
    
#     Supports dynamic tab filters:
#     - data_type: document_data_type (content, service, product, person, media, location)
#     - category: document_category (healthcare_medical, fashion, beauty, etc.)
#     - schema: document_schema (Article, BlogPosting, Product, Service, etc.)
#     - source: document_brand
    
#     Also supports:
#     - time_start/time_end: time_period_start/time_period_end
#     - location: location_city, location_state, location_country, location_region
#     - content_type: document_data_type (from query extraction)
    
#     FIXED in v4.4: Location filter now handles case sensitivity and state abbreviations.
#     """
#     conditions = []
    
#     if filters:
#         # Dynamic tab filter: data_type → document_data_type
#         data_type = filters.get('data_type')
#         if data_type:
#             # Validate against allowed values
#             if data_type in VALID_DATA_TYPES:
#                 conditions.append(f"document_data_type:={data_type}")
#             else:
#                 print(f"⚠️ Invalid data_type filter: {data_type}")
        
#         # Dynamic tab filter: category → document_category
#         category = filters.get('category')
#         if category:
#             # Allow any category (might have custom ones)
#             # Sanitize to prevent injection
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         # Dynamic tab filter: schema → document_schema
#         schema = filters.get('schema')
#         if schema:
#             if schema in VALID_SCHEMAS:
#                 conditions.append(f"document_schema:={schema}")
#             else:
#                 print(f"⚠️ Invalid schema filter: {schema}")
        
#         # Source filter → document_brand
#         source = filters.get('source')
#         if source:
#             # Sanitize
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         # Time range filter
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             if time_conditions:
#                 conditions.extend(time_conditions)
        
#         # Location filter from filters dict (FIXED in v4.4)
#         loc_filter = filters.get('location')
#         if loc_filter:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', loc_filter)
#             if safe_loc:
#                 location_condition = _build_location_filter(safe_loc)
#                 if location_condition:
#                     conditions.append(location_condition)
    
#     # Time period from query extraction
#     if time_start is not None:
#         conditions.append(f"time_period_start:>={time_start}")
#     if time_end is not None:
#         conditions.append(f"time_period_end:<={time_end}")
    
#     # Location from query extraction (FIXED in v4.4)
#     if location and not (filters and filters.get('location')):
#         location_condition = _build_location_filter(location)
#         if location_condition:
#             conditions.append(location_condition)
    
#     # Content type from query extraction (if not already set by data_type filter)
#     if content_type and not (filters and filters.get('data_type')):
#         if content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
    
#     now = datetime.now()
    
#     if time_range == 'day':
#         # Last 24 hours
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         # Last 7 days
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         # Last 30 days
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         # Last 365 days
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(intent: str, user_location: Tuple[float, float] = None) -> str:
#     """Builds Typesense sort_by string."""
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search, NO full-text matching."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_exact_phrase_params(
#     phrase: str,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """
#     EXACT PHRASE MODE: For dropdown selections.
#     Searches for the exact phrase without splitting or processing.
#     """
#     location = extract_location_from_query(phrase)
#     time_start, time_end = extract_time_period_from_query(phrase)
#     content_type = extract_content_type_from_query(phrase)
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': f'"{phrase}"',  # Quote for exact phrase matching
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'num_typos': 0,  # No typos for exact match
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
#         'title_highlighted': highlight_map.get('document_title', doc.get('document_title', '')),
#         'summary': doc.get('document_summary', ''),
#         'summary_highlighted': highlight_map.get('document_summary', doc.get('document_summary', '')),
#         'url': doc.get('document_url', ''),
#         'source': doc.get('document_brand', 'unknown'),
#         'site_name': doc.get('document_brand', 'Website'),
#         'image': (doc.get('image_url') or [None])[0],
#         'category': doc.get('document_category', ''),
#         'data_type': doc.get('document_data_type', ''),
#         'schema': doc.get('document_schema', ''),  # Include schema for filtering
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Keyword Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Text search to find candidate documents."""
#     location = extract_location_from_query(query)
#     time_start, time_end = extract_time_period_from_query(query)
#     content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string(
#         filters=filters,
#         time_start=time_start,
#         time_end=time_end,
#         location=location,
#         content_type=content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (keyword filter): Found {len(doc_ids)} candidates")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
    
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Keyword filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (OPTIMIZED)
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
#     alt_mode: str = 'n',
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION v4.4.
    
#     OPTIMIZATIONS:
#     1. Query truncation before embedding (biggest win for long queries)
#     2. Parallel word discovery + embedding
#     3. Connection pooling for HTTP requests
#     4. Score-based strategy selection
#     5. skip_embedding for dropdown/keyword selections
#     6. DROPDOWN FAST PATH - skip word discovery entirely (v4.2)
#     7. Dynamic tab filter support (v4.3)
#     8. FIXED: Location filtering handles case + abbreviations (v4.4)
    
#     Args:
#         query: Search query string
#         session_id: User session identifier
#         filters: Dict of filter conditions including:
#             - data_type: Filter by document_data_type (content, service, product, etc.)
#             - category: Filter by document_category
#             - schema: Filter by document_schema (Article, Product, etc.)
#             - source: Filter by document_brand
#             - time_range: Filter by time (day, week, month, year)
#             - location: Filter by location
#         page: Page number for pagination
#         per_page: Results per page
#         user_location: (lat, lng) tuple for location-based sorting
#         pos_tags: Pre-computed POS tags (optional)
#         safe_search: Enable safe search filtering
#         alt_mode: 'y' to force semantic search, 'n' for normal
#         skip_embedding: True to skip embedding (for dropdown/keyword selections)
#         search_source: Source of search ('dropdown', 'keyboard', 'home', etc.)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     # Log active filters for debugging
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active filters: {active_filters}")
    
#     # =========================================================================
#     # DROPDOWN FAST PATH - Skip everything, just search the exact phrase
#     # =========================================================================
#     is_dropdown = search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

#     # alt_mode='y' forces full semantic processing, even for dropdowns
#     if alt_mode == 'y':
#         is_dropdown = False

#     if is_dropdown:
#         print(f"⚡ DROPDOWN FAST PATH: '{query}'")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build exact phrase search params
#         search_params = build_exact_phrase_params(
#             phrase=query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         # Debug output
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: EXACT_PHRASE (dropdown) | Found: {raw_response.get('found', 0)}")
#         print(f"   Search phrase: \"{query}\"")
        
#         # Process results
#         results = process_results(raw_response, query)
        
#         raw_count = len(raw_response.get('hits', []))
#         filtered_count = len(results)
#         if raw_count > 0:
#             top_score = results[0]['score'] if results else 0
#             print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#             print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
        
#         # Build response
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,  # No correction for dropdown
#             'intent': intent,
#             'results': results,
#             'total': len(results), 
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'exact_phrase',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': [query],  # Whole phrase is the term
#             'unknown_terms': [],
#             'word_discovery': {
#                 'valid_count': 1,
#                 'unknown_count': 0,
#                 'corrections': [],
#                 'total_score': 0,
#                 'average_score': 0,
#                 'max_score': 0,
#                 'term_scores': []
#             },
#             'timings': times,
#             'filters_applied': {
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#                 'time_period': extract_time_period_from_query(query),
#                 'location': extract_location_from_query(query),
#                 'content_type': extract_content_type_from_query(query)
#             }
#         }
    
#     # =========================================================================
#     # NORMAL PATH - Full word discovery + embedding
#     # =========================================================================
    
#     # STEP 1 + 2: Word Discovery + Embedding (parallel or skip)
#     t1 = time.time()
    
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
    
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     # Extract discovery results
#     if WORD_DISCOVERY_AVAILABLE:
#         search_strategy = discovery.get('search_strategy', 'semantic')
#         valid_terms = get_filter_terms(discovery)
#         unknown_terms = get_loose_terms(discovery)
#     else:
#         search_strategy = 'semantic'
#         valid_terms = []
#         unknown_terms = query.split()
    
#     corrected_query = discovery.get('corrected_query', query)
    
#     # Override to semantic if alt_mode='y'
#     if alt_mode == 'y':
#         search_strategy = 'semantic'
    
#     # If we skipped embedding and strategy wants semantic, force to strict/text
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # STEP 3: Detect intent
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # STEP 4: Execute search based on strategy
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy in ('strict', 'mixed') and valid_terms:
#         actual_strategy = f'two_stage_{search_strategy}'
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT (Enhanced with scores)
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if valid_terms:
#         print(f"   Valid terms: {valid_terms}")
#         # Show individual term scores
#         for term in discovery.get('terms', []):
#             if term.get('rank_score', 0) > 0:
#                 print(f"      • {term['search_word']}: {term['rank_score']} pts")
    
#     if discovery.get('corrected_terms'):
#         print(f"   Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrected_terms', [])]}")
    
#     # Log location filter for debugging (NEW in v4.4)
#     location = extract_location_from_query(query)
#     if location:
#         abbrev = US_STATE_ABBREV.get(location.lower(), '')
#         if abbrev:
#             print(f"📍 Location filter: {location} (+ {abbrev})")
#         else:
#             print(f"📍 Location filter: {location}")
    
#     # =========================================================================
#     # STEP 5: Process results
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # STEP 6: Build response
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': len(results), 
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
#         'word_discovery': {
#             'valid_count': discovery.get('valid_count', 0),
#             'unknown_count': discovery.get('unknown_count', 0),
#             'corrections': discovery.get('corrected_terms', []),
#             # Score metrics
#             'total_score': discovery.get('total_score', 0),
#             'average_score': discovery.get('average_score', 0),
#             'max_score': discovery.get('max_score', 0),
#             'term_scores': [
#                 {'term': t['search_word'], 'score': t.get('rank_score', 0)}
#                 for t in discovery.get('terms', [])
#                 if t.get('rank_score', 0) > 0
#             ]
#         },
#         'timings': times,
#         'filters_applied': {
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'time_period': extract_time_period_from_query(query),
#             'location': extract_location_from_query(query),
#             'content_type': extract_content_type_from_query(query)
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """
#     Find documents similar to a given document.
    
#     Strategy:
#     1. If document has semantic_uuid, find others in same cluster first
#     2. Fall back to vector similarity if no semantic_uuid or need more results
#     """
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         # Strategy 1: Use semantic_uuid cluster
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]  # e.g., "DOC"
#                 cluster = parts[1]  # e.g., "11"
                
#                 # Find documents in same cluster (excluding self)
#                 # Only use cluster-based search if cluster != 00 (00 = no semantic clustering)
#                 if cluster != '00':
#                     try:
#                         # Search for documents with same prefix-cluster pattern
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
#                         # Fall through to vector search
        
#         # Strategy 2: Vector similarity (for remaining slots or fallback)
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             # Build exclusion filter
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """
#     Find all documents in the same semantic cluster.
    
#     Args:
#         semantic_uuid: e.g., "DOC-11-0042"
#         limit: Maximum documents to return
        
#     Returns:
#         List of documents in the same cluster
#     """
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]  # e.g., "DOC"
#     cluster = parts[1]  # e.g., "11"
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """
#     Get facet counts specifically for dynamic tabs.
    
#     Returns facets for:
#     - document_data_type (main tabs)
#     - document_category (secondary filter)
#     - document_schema (tertiary filter)
    
#     This is called WITHOUT any filters to get total counts.
#     """
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0  # Only need facets, not results
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         facets = {
#             'data_type': [],
#             'category': [],
#             'schema': []
#         }
        
#         # Label mappings
#         data_type_labels = {
#                     'article': 'Articles',
#                     'person': 'People',
#                     'business': 'Businesses',
#                     'place': 'Places',
#                     'media': 'Media',
#                     'event': 'Events',
#                     'product': 'Products',
#                 }
        
#         category_labels = {
#             'healthcare_medical': 'Healthcare',
#             'fashion': 'Fashion',
#             'beauty': 'Beauty',
#             'food_recipes': 'Food & Recipes',
#             'travel_tourism': 'Travel',
#             'entertainment': 'Entertainment',
#             'business': 'Business',
#             'education': 'Education',
#             'technology': 'Technology',
#             'sports': 'Sports',
#             'finance': 'Finance',
#             'real_estate': 'Real Estate',
#             'lifestyle': 'Lifestyle',
#             'news': 'News',
#             'culture': 'Culture',
#             'general': 'General',
#         }
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             counts = facet['counts']
            
#             if field == 'document_data_type':
#                 facets['data_type'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': data_type_labels.get(c['value'], c['value'].title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_category':
#                 facets['category'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': category_labels.get(c['value'], c['value'].replace('_', ' ').title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_schema':
#                 facets['schema'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': c['value']
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
        
#         return facets
        
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
    
#     # Replace with your implementation
#     # SearchLog.objects.create(**event)
#     pass


# """
# typesense_calculations.py (OPTIMIZED v5.0)

# OPTIMIZATIONS:
# 1. Truncate long queries BEFORE embedding (biggest win for long sentences)
# 2. Parallel execution: Word Discovery + Embedding run simultaneously  
# 3. Connection pooling for FastAPI embedding calls
# 4. Reuse Typesense client (already done)

# NEW IN v5.0:
# - GRAPH-BASED FILTERING: Uses word_discovery's extracted filters, locations, sort
# - Word discovery now provides:
#   - filters: [{field: 'primary_keywords', value: 'hbcu'}, ...]
#   - locations: [{field: 'location_state', values: ['Georgia', 'GA']}, ...]
#   - sort: {field: 'time_period_start', order: 'asc'}
# - Removed duplicate regex extraction (word_discovery handles it)
# - Simplified filter building (assembles from word_discovery output)

# PREVIOUS VERSIONS:
# v4.4: Location filtering handles case + abbreviations
# v4.3: Dynamic tab filter support
# v4.2: Dropdown fast path
# v4.1: skip_embedding parameter
# v4.0: Score-based strategy selection
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # US STATE ABBREVIATION MAPPING (Fallback when word_discovery unavailable)
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

# US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     cleaned = ' '.join(cleaned.split())
    
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY (v5.0 - Simplified imports)
# # ============================================================================

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


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
    
#     # Fallback when word_discovery not available
#     return {
#         'query': query,
#         'corrected_query': query,
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'corrections': [],
#         'filters': [],
#         'locations': [],
#         'sort': None,
#         'ngrams': [],
#         'terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'processing_time_ms': 0
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
#     """
#     if skip_embedding:
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
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


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (Fallback when word_discovery unavailable)
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES (for validation)
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# # ============================================================================
# # FALLBACK EXTRACTION (Used only when word_discovery unavailable)
# # ============================================================================

# def extract_location_from_query_fallback(query: str) -> Optional[str]:
#     """
#     FALLBACK: Extracts location from query using regex.
#     Only used when word_discovery is not available.
#     """
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location.title()
#     return None


# def extract_time_period_from_query_fallback(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """
#     FALLBACK: Extracts time period from query using regex.
#     Only used when word_discovery is not available.
#     """
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER BUILDING (v5.0 - Uses word_discovery output)
# # ============================================================================

# def build_filter_string_from_discovery(
#     discovery: Dict,
#     filters: Dict = None,
#     fallback_time_start: int = None,
#     fallback_time_end: int = None,
#     fallback_location: str = None,
#     fallback_content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string from word_discovery output.
    
#     v5.0: Primary source is word_discovery's extracted filters/locations.
#     Fallback to regex extraction only if word_discovery doesn't provide values.
#     """
#     conditions = []
    
#     # =========================================================================
#     # 1. GRAPH-BASED FILTERS (from word_discovery)
#     # =========================================================================
    
#     # Keyword filters (primary_keywords, entity_names, etc.)
#     for filter_item in discovery.get('filters', []):
#         field = filter_item.get('field')
#         value = filter_item.get('value')
        
#         if field and value:
#             safe_value = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(value))
#             if safe_value:
#                 conditions.append(f"{field}:={safe_value}")
#                 print(f"   📌 Filter from vocabulary: {field}:={safe_value}")
    
#     # Location filters (with variants like Georgia, GA)
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
#                 print(f"   📍 Location from vocabulary: {values}")
    
#     # =========================================================================
#     # 2. UI FILTERS (from filters dict - tabs, dropdowns, etc.)
#     # =========================================================================
    
#     if filters:
#         data_type = filters.get('data_type')
#         if data_type and data_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={data_type}")
        
#         category = filters.get('category')
#         if category:
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         schema = filters.get('schema')
#         if schema and schema in VALID_SCHEMAS:
#             conditions.append(f"document_schema:={schema}")
        
#         source = filters.get('source')
#         if source:
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             conditions.extend(time_conditions)
        
#         ui_location = filters.get('location')
#         if ui_location:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', ui_location)
#             if safe_loc:
#                 loc_title = safe_loc.title()
#                 abbrev = US_STATE_ABBREV.get(safe_loc.lower(), '')
                
#                 loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#                 if abbrev:
#                     loc_parts.append(f"location_state:={abbrev}")
                
#                 conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     # =========================================================================
#     # 3. FALLBACK EXTRACTION (only if word_discovery didn't provide values)
#     # =========================================================================
    
#     if not discovery.get('locations') and fallback_location:
#         if not (filters and filters.get('location')):
#             loc_title = fallback_location.title()
#             abbrev = US_STATE_ABBREV.get(fallback_location.lower(), '')
            
#             loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#             if abbrev:
#                 loc_parts.append(f"location_state:={abbrev}")
            
#             conditions.append('(' + ' || '.join(loc_parts) + ')')
#             print(f"   📍 Location from fallback: {loc_title}")
    
#     if fallback_time_start is not None:
#         conditions.append(f"time_period_start:>={fallback_time_start}")
#     if fallback_time_end is not None:
#         conditions.append(f"time_period_end:<={fallback_time_end}")
    
#     if fallback_content_type and not (filters and filters.get('data_type')):
#         if fallback_content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={fallback_content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
#     now = datetime.now()
    
#     if time_range == 'day':
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(
#     intent: str,
#     discovery: Dict = None,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """
#     Builds Typesense sort_by string.
    
#     v5.0: Uses word_discovery's sort instruction for temporal queries.
#     """
#     if discovery:
#         sort_instruction = discovery.get('sort')
#         if sort_instruction:
#             field = sort_instruction.get('field')
#             order = sort_instruction.get('order', 'asc')
#             if field:
#                 print(f"   🔢 Sort from vocabulary: {field}:{order}")
#                 return f"{field}:{order},authority_score:desc"
    
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH STRATEGY SELECTION (v5.0)
# # ============================================================================

# def determine_search_strategy(discovery: Dict) -> str:
#     """
#     Determine search strategy based on word_discovery output.
#     """
#     filters = discovery.get('filters', [])
#     locations = discovery.get('locations', [])
#     valid_count = discovery.get('valid_count', 0)
#     unknown_count = discovery.get('unknown_count', 0)
#     total_score = discovery.get('total_score', 0)
    
#     if filters or locations:
#         if total_score >= 200:
#             return 'two_stage_strict'
#         else:
#             return 'two_stage_mixed'
    
#     if valid_count > 0 and unknown_count <= valid_count:
#         return 'mixed'
    
#     return 'semantic'


# def get_filter_terms_from_discovery(discovery: Dict) -> List[str]:
#     """
#     Extract searchable terms from discovery result.
#     """
#     terms = []
    
#     for f in discovery.get('filters', []):
#         term = f.get('term', f.get('value', ''))
#         if term:
#             terms.append(term)
    
#     for loc in discovery.get('locations', []):
#         term = loc.get('term', '')
#         if term:
#             terms.append(term)
    
#     for t in discovery.get('terms', []):
#         if t.get('status') in ('valid', 'corrected'):
#             if t.get('category') != 'stopword':
#                 word = t.get('search_word', t.get('word', ''))
#                 if word and word not in terms:
#                     terms.append(word)
    
#     return terms


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search with graph filters."""
    
#     fallback_location = None
#     fallback_time_start, fallback_time_end = None, None
#     fallback_content_type = None
    
#     if not discovery or not discovery.get('locations'):
#         fallback_location = extract_location_from_query_fallback(query)
#     if not discovery:
#         fallback_time_start, fallback_time_end = extract_time_period_from_query_fallback(query)
#         fallback_content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters,
#         fallback_time_start=fallback_time_start,
#         fallback_time_end=fallback_time_end,
#         fallback_location=fallback_location,
#         fallback_content_type=fallback_content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string('general', discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector + graph filters."""
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_exact_phrase_params(
#     phrase: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """EXACT PHRASE MODE: For dropdown selections."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': f'"{phrase}"',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'num_typos': 0,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
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
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """Processes Typesense response into clean result list."""
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return []
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return []
    
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     return filtered_results[:20]


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Graph Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Use graph filters to find candidate documents."""
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (graph filter): Found {len(doc_ids)} candidates")
#         if filter_str:
#             print(f"   Filter: {filter_str}")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Graph filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         discovery=discovery,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 discovery=discovery,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (v5.0)
# # ============================================================================

# # def execute_full_search(
# #     query: str,
# #     session_id: str = None,
# #     filters: Dict = None,
# #     page: int = 1,
# #     per_page: int = 20,
# #     user_location: Tuple[float, float] = None,
# #     pos_tags: List[Tuple] = None,
# #     safe_search: bool = True,
# #     alt_mode: str = 'n',
# #     skip_embedding: bool = False,
# #     search_source: str = None
# # ) -> Dict:
# #     """
# #     Main entry point for search - OPTIMIZED VERSION v5.0.
# #     """
# #     import time
# #     times = {}
# #     t0 = time.time()
    
# #     if filters:
# #         active_filters = {k: v for k, v in filters.items() if v}
# #         if active_filters:
# #             print(f"🎛️ Active UI filters: {active_filters}")
    
# #     # =========================================================================
# #     # DROPDOWN FAST PATH
# #     # =========================================================================
# #     is_dropdown = search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')

# #     if alt_mode == 'y':
# #         is_dropdown = False

# #     if is_dropdown:
# #         print(f"⚡ DROPDOWN FAST PATH: '{query}'")
        
# #         t1 = time.time()
# #         intent = detect_query_intent(query, pos_tags)
        
# #         search_params = build_exact_phrase_params(
# #             phrase=query,
# #             filters=filters,
# #             page=page,
# #             per_page=per_page,
# #             intent=intent
# #         )
        
# #         t2 = time.time()
# #         raw_response = execute_search_multi(search_params)
# #         times['typesense'] = round((time.time() - t2) * 1000, 2)
# #         times['total'] = round((time.time() - t0) * 1000, 2)
        
# #         print(f"⏱️ TIMING: {times}")
# #         print(f"🔍 Strategy: EXACT_PHRASE (dropdown) | Found: {raw_response.get('found', 0)}")
        
# #         results = process_results(raw_response, query)
# #         search_time = round(time.time() - t0, 3)
        
# #         return {
# #             'query': query,
# #             'corrected_query': query,
# #             'intent': intent,
# #             'results': results,
# #             'total': len(results), 
# #             'page': page,
# #             'per_page': per_page,
# #             'search_time': search_time,
# #             'session_id': session_id,
# #             'semantic_enabled': False,
# #             'search_strategy': 'exact_phrase',
# #             'alt_mode': alt_mode,
# #             'skip_embedding': True,
# #             'search_source': search_source or 'dropdown',
# #             'valid_terms': [query],
# #             'unknown_terms': [],
# #             'word_discovery': {
# #                 'valid_count': 1,
# #                 'unknown_count': 0,
# #                 'corrections': [],
# #                 'filters': [],
# #                 'locations': [],
# #                 'sort': None,
# #                 'total_score': 0,
# #                 'average_score': 0,
# #                 'max_score': 0,
# #             },
# #             'timings': times,
# #             'filters_applied': {
# #                 'data_type': filters.get('data_type') if filters else None,
# #                 'category': filters.get('category') if filters else None,
# #                 'schema': filters.get('schema') if filters else None,
# #             }
# #         }
    
# #     # =========================================================================
# #     # NORMAL PATH - Full word discovery + embedding
# #     # =========================================================================
    
# #     t1 = time.time()
# #     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
# #     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
# #     corrected_query = discovery.get('corrected_query', query)
# #     valid_terms = get_filter_terms_from_discovery(discovery)
# #     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
# #     # =========================================================================
# #     # STRATEGY SELECTION (v5.0 - based on graph constraints)
# #     # =========================================================================
    
# #     search_strategy = determine_search_strategy(discovery)
    
# #     if alt_mode == 'y':
# #         search_strategy = 'semantic'
    
# #     if skip_embedding and search_strategy == 'semantic':
# #         if valid_terms:
# #             search_strategy = 'two_stage_strict'
# #         else:
# #             search_strategy = 'text_fallback'
    
# #     semantic_enabled = query_embedding is not None
    
# #     # =========================================================================
# #     # DETECT INTENT
# #     # =========================================================================
# #     intent = detect_query_intent(query, pos_tags)
    
# #     # =========================================================================
# #     # EXECUTE SEARCH
# #     # =========================================================================
    
# #     if not semantic_enabled:
# #         actual_strategy = 'text_fallback'
# #         search_params = build_fallback_text_params(
# #             query=corrected_query,
# #             discovery=discovery,
# #             filters=filters,
# #             page=page,
# #             per_page=per_page,
# #             intent=intent
# #         )
# #         t3 = time.time()
# #         raw_response = execute_search_multi(search_params)
# #         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
# #     elif search_strategy.startswith('two_stage'):
# #         actual_strategy = search_strategy
# #         t3 = time.time()
# #         raw_response = execute_two_stage_search(
# #             query=corrected_query,
# #             query_embedding=query_embedding,
# #             discovery=discovery,
# #             filters=filters,
# #             page=page,
# #             per_page=per_page
# #         )
# #         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
# #     elif search_strategy == 'mixed':
# #         actual_strategy = 'mixed'
# #         search_params = build_mixed_params(
# #             query=corrected_query,
# #             query_embedding=query_embedding,
# #             discovery=discovery,
# #             filters=filters,
# #             page=page,
# #             per_page=per_page,
# #             intent=intent
# #         )
# #         t3 = time.time()
# #         raw_response = execute_search_multi(search_params)
# #         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
# #     else:
# #         actual_strategy = 'semantic'
# #         search_params = build_semantic_params(
# #             query_embedding=query_embedding,
# #             discovery=discovery,
# #             filters=filters,
# #             page=page,
# #             per_page=per_page,
# #             query=query
# #         )
# #         t3 = time.time()
# #         raw_response = execute_search_multi(search_params)
# #         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
# #     times['total'] = round((time.time() - t0) * 1000, 2)
    
# #     # =========================================================================
# #     # DEBUG OUTPUT
# #     # =========================================================================
# #     print(f"⏱️ TIMING: {times}")
# #     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
# #     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
# #     if skip_embedding:
# #         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
# #     if discovery.get('filters'):
# #         print(f"   📌 Filters: {[f['term'] for f in discovery.get('filters', [])]}")
# #     if discovery.get('locations'):
# #         print(f"   📍 Locations: {[l['term'] for l in discovery.get('locations', [])]}")
# #     if discovery.get('sort'):
# #         print(f"   🔢 Sort: {discovery.get('sort')}")
    
# #     if discovery.get('corrections'):
# #         print(f"   ✏️ Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrections', [])]}")
    
# #     # =========================================================================
# #     # PROCESS RESULTS
# #     # =========================================================================
# #     results = process_results(raw_response, query)
    
# #     raw_count = len(raw_response.get('hits', []))
# #     filtered_count = len(results)
# #     if raw_count > 0:
# #         top_score = results[0]['score'] if results else 0
# #         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
# #         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
# #     # =========================================================================
# #     # BUILD RESPONSE
# #     # =========================================================================
# #     search_time = round(time.time() - t0, 3)
    
# #     return {
# #         'query': query,
# #         'corrected_query': corrected_query,
# #         'intent': intent,
# #         'results': results,
# #         'total': len(results), 
# #         'page': page,
# #         'per_page': per_page,
# #         'search_time': search_time,
# #         'session_id': session_id,
# #         'semantic_enabled': semantic_enabled,
# #         'search_strategy': actual_strategy,
# #         'alt_mode': alt_mode,
# #         'skip_embedding': skip_embedding,
# #         'search_source': search_source,
# #         'valid_terms': valid_terms,
# #         'unknown_terms': unknown_terms,
# #         'word_discovery': {
# #             'valid_count': discovery.get('valid_count', 0),
# #             'unknown_count': discovery.get('unknown_count', 0),
# #             'corrections': discovery.get('corrections', []),
# #             'filters': discovery.get('filters', []),
# #             'locations': discovery.get('locations', []),
# #             'sort': discovery.get('sort'),
# #             'total_score': discovery.get('total_score', 0),
# #             'average_score': discovery.get('average_score', 0),
# #             'max_score': discovery.get('max_score', 0),
# #         },
# #         'timings': times,
# #         'filters_applied': {
# #             'data_type': filters.get('data_type') if filters else None,
# #             'category': filters.get('category') if filters else None,
# #             'schema': filters.get('schema') if filters else None,
# #             'graph_filters': discovery.get('filters', []),
# #             'graph_locations': discovery.get('locations', []),
# #             'graph_sort': discovery.get('sort'),
# #         }
# #     }
# # ============================================================================
# # MAIN SEARCH FUNCTION (v5.1 - Fixed alt_mode handling)
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
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - OPTIMIZED VERSION v5.2.
    
#     alt_mode:
#         'n' = User clicked dropdown item (skip word discovery, direct search)
#         'y' = User typed freely (run word discovery + semantic search)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
#     # =========================================================================
#     # DROPDOWN FAST PATH (alt_mode='n' means user clicked a dropdown item)
#     # Word is from the hash - already spelled correctly, skip word discovery
#     # =========================================================================
    
#     is_dropdown = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_dropdown:
#         print(f"⚡ DROPDOWN FAST PATH: '{query}' (alt_mode={alt_mode})")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build filter string from UI filters only (no word discovery)
#         filter_conditions = []
#         if filters:
#             if filters.get('data_type') and filters.get('data_type') in VALID_DATA_TYPES:
#                 filter_conditions.append(f"document_data_type:={filters['data_type']}")
#             if filters.get('category'):
#                 safe_cat = re.sub(r'[^a-zA-Z0-9_]', '', filters['category'])
#                 if safe_cat:
#                     filter_conditions.append(f"document_category:={safe_cat}")
#             if filters.get('schema') and filters.get('schema') in VALID_SCHEMAS:
#                 filter_conditions.append(f"document_schema:={filters['schema']}")
        
#         filter_by = ' && '.join(filter_conditions) if filter_conditions else ''
        
#         # Use flexible text search (NOT exact phrase)
#         # The dropdown term is correct, but we want to find documents ABOUT it
#         search_params = {
#             'q': query,
#             'query_by': 'key_facts,document_title,primary_keywords,entity_names,document_summary',
#             'query_by_weights': '10,8,5,4,2',
#             'page': page,
#             'per_page': per_page,
#             'exclude_fields': 'embedding',
#             'num_typos': 1,
#             'drop_tokens_threshold': 2,
#         }
        
#         if filter_by:
#             search_params['filter_by'] = filter_by
        
#         print(f"   Search params: q='{query}', filter_by='{filter_by}'")
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: DROPDOWN_DIRECT | Found: {raw_response.get('found', 0)}")
        
#         results = process_results(raw_response, query)
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             # 'total': len(results), 
#             'total': raw_response.get('found', 0),
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'dropdown_direct',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
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
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#             }
#         }
    
#     # =========================================================================
#     # SEMANTIC PATH (alt_mode='y' means user typed freely)
#     # Run full word discovery + embedding
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH: '{query}' (alt_mode={alt_mode})")
    
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     valid_terms = get_filter_terms_from_discovery(discovery)
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     # =========================================================================
#     # STRATEGY SELECTION (based on word discovery results)
#     # =========================================================================
    
#     search_strategy = determine_search_strategy(discovery)
    
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'two_stage_strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # DETECT INTENT
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # EXECUTE SEARCH
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy.startswith('two_stage'):
#         actual_strategy = search_strategy
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy == 'mixed':
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if discovery.get('filters'):
#         print(f"   📌 Filters: {[f['term'] for f in discovery.get('filters', [])]}")
#     if discovery.get('locations'):
#         print(f"   📍 Locations: {[l['term'] for l in discovery.get('locations', [])]}")
#     if discovery.get('sort'):
#         print(f"   🔢 Sort: {discovery.get('sort')}")
    
#     if discovery.get('corrections'):
#         print(f"   ✏️ Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrections', [])]}")
    
#     # =========================================================================
#     # PROCESS RESULTS
#     # =========================================================================
#     results = process_results(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     filtered_count = len(results)
#     if raw_count > 0:
#         top_score = results[0]['score'] if results else 0
#         print(f"📊 Filtering: {raw_count} → {filtered_count} results")
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # BUILD RESPONSE
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         # 'total': len(results), 
#         'total': raw_response.get('found', 0),
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
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'graph_filters': discovery.get('filters', []),
#             'graph_locations': discovery.get('locations', []),
#             'graph_sort': discovery.get('sort'),
#         }
#     }

# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]
#                 cluster = parts[1]
                
#                 if cluster != '00':
#                     try:
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
        
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """Find all documents in the same semantic cluster."""
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]
#     cluster = parts[1]
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """Get facet counts specifically for dynamic tabs."""
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         facets = {
#             'data_type': [],
#             'category': [],
#             'schema': []
#         }
        
#         data_type_labels = {
#             'article': 'Articles',
#             'person': 'People',
#             'business': 'Businesses',
#             'place': 'Places',
#             'media': 'Media',
#             'event': 'Events',
#             'product': 'Products',
#         }
        
#         category_labels = {
#             'healthcare_medical': 'Healthcare',
#             'fashion': 'Fashion',
#             'beauty': 'Beauty',
#             'food_recipes': 'Food & Recipes',
#             'travel_tourism': 'Travel',
#             'entertainment': 'Entertainment',
#             'business': 'Business',
#             'education': 'Education',
#             'technology': 'Technology',
#             'sports': 'Sports',
#             'finance': 'Finance',
#             'real_estate': 'Real Estate',
#             'lifestyle': 'Lifestyle',
#             'news': 'News',
#             'culture': 'Culture',
#             'general': 'General',
#         }
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             counts = facet['counts']
            
#             if field == 'document_data_type':
#                 facets['data_type'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': data_type_labels.get(c['value'], c['value'].title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_category':
#                 facets['category'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': category_labels.get(c['value'], c['value'].replace('_', ' ').title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_schema':
#                 facets['schema'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': c['value']
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
        
#         return facets
        
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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


# # ============================================================================
# # SIMPLE TYPESENSE SEARCH (Used by views for basic queries)
# # ============================================================================

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
#     """
#     Simple Typesense search wrapper for views.
#     Returns raw Typesense response.
#     """
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
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return response
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}

# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
#     pass


















# """
# typesense_calculations.py (FIXED v5.3)

# FIX: Pagination now matches actual results
# - Keyword path: No score filtering, total = Typesense's found count
# - Semantic path: Score filtering applied, total = filtered count

# The key insight: 
# - Typesense's 'found' count is for ALL matching docs (before score filtering)
# - If we filter by score client-side, we can't use 'found' for pagination
# - Solution: Different processing for each path
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # US STATE ABBREVIATION MAPPING (Fallback when word_discovery unavailable)
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

# US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# # ============================================================================
# # QUERY PREPROCESSING - THE BIGGEST WIN FOR LONG QUERIES
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """
#     Strip filler words and truncate for faster embedding.
#     """
#     cleaned = query
    
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
    
#     cleaned = ' '.join(cleaned.split())
    
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
    
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT (with pooling + truncation)
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """
#     Get embedding with:
#     1. Query truncation (faster inference)
#     2. Connection pooling (faster HTTP)
#     """
#     clean_query = truncate_for_embedding(query, max_words=40)
    
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY (v5.0 - Simplified imports)
# # ============================================================================

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


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
    
#     # Fallback when word_discovery not available
#     return {
#         'query': query,
#         'corrected_query': query,
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'corrections': [],
#         'filters': [],
#         'locations': [],
#         'sort': None,
#         'ngrams': [],
#         'terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'processing_time_ms': 0
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


# # ============================================================================
# # PARALLEL EXECUTION HELPER
# # ============================================================================

# def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
#     """
#     Run word discovery and embedding IN PARALLEL.
#     """
#     if skip_embedding:
#         discovery = _do_word_discovery(query)
#         return discovery, None
    
#     discovery_future = _executor.submit(_do_word_discovery, query)
#     embedding_future = _executor.submit(_do_embedding, query)
    
#     discovery = discovery_future.result()
#     embedding = embedding_future.result()
    
#     # Check if we need to re-embed due to significant correction
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


# # ============================================================================
# # PRE-COMPILED REGEX PATTERNS (Fallback when word_discovery unavailable)
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES (for validation)
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])


# # ============================================================================
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
    
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
    
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
    
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
    
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
    
#     return 'general'


# # ============================================================================
# # FALLBACK EXTRACTION (Used only when word_discovery unavailable)
# # ============================================================================

# def extract_location_from_query_fallback(query: str) -> Optional[str]:
#     """
#     FALLBACK: Extracts location from query using regex.
#     Only used when word_discovery is not available.
#     """
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
    
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location.title()
#     return None


# def extract_time_period_from_query_fallback(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """
#     FALLBACK: Extracts time period from query using regex.
#     Only used when word_discovery is not available.
#     """
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
    
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
    
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
    
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
    
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER BUILDING (v5.0 - Uses word_discovery output)
# # ============================================================================

# def build_filter_string_from_discovery(
#     discovery: Dict,
#     filters: Dict = None,
#     fallback_time_start: int = None,
#     fallback_time_end: int = None,
#     fallback_location: str = None,
#     fallback_content_type: str = None
# ) -> str:
#     """
#     Builds Typesense filter_by string from word_discovery output.
    
#     v5.0: Primary source is word_discovery's extracted filters/locations.
#     Fallback to regex extraction only if word_discovery doesn't provide values.
#     """
#     conditions = []
    
#     # =========================================================================
#     # 1. GRAPH-BASED FILTERS (from word_discovery)
#     # =========================================================================
    
#     # Keyword filters (primary_keywords, entity_names, etc.)
#     for filter_item in discovery.get('filters', []):
#         field = filter_item.get('field')
#         value = filter_item.get('value')
        
#         if field and value:
#             safe_value = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(value))
#             if safe_value:
#                 conditions.append(f"{field}:={safe_value}")
#                 print(f"   📌 Filter from vocabulary: {field}:={safe_value}")
    
#     # Location filters (with variants like Georgia, GA)
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
#                 print(f"   📍 Location from vocabulary: {values}")
    
#     # =========================================================================
#     # 2. UI FILTERS (from filters dict - tabs, dropdowns, etc.)
#     # =========================================================================
    
#     if filters:
#         data_type = filters.get('data_type')
#         if data_type and data_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={data_type}")
        
#         category = filters.get('category')
#         if category:
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         schema = filters.get('schema')
#         if schema and schema in VALID_SCHEMAS:
#             conditions.append(f"document_schema:={schema}")
        
#         source = filters.get('source')
#         if source:
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             conditions.extend(time_conditions)
        
#         ui_location = filters.get('location')
#         if ui_location:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', ui_location)
#             if safe_loc:
#                 loc_title = safe_loc.title()
#                 abbrev = US_STATE_ABBREV.get(safe_loc.lower(), '')
                
#                 loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#                 if abbrev:
#                     loc_parts.append(f"location_state:={abbrev}")
                
#                 conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     # =========================================================================
#     # 3. FALLBACK EXTRACTION (only if word_discovery didn't provide values)
#     # =========================================================================
    
#     if not discovery.get('locations') and fallback_location:
#         if not (filters and filters.get('location')):
#             loc_title = fallback_location.title()
#             abbrev = US_STATE_ABBREV.get(fallback_location.lower(), '')
            
#             loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#             if abbrev:
#                 loc_parts.append(f"location_state:={abbrev}")
            
#             conditions.append('(' + ' || '.join(loc_parts) + ')')
#             print(f"   📍 Location from fallback: {loc_title}")
    
#     if fallback_time_start is not None:
#         conditions.append(f"time_period_start:>={fallback_time_start}")
#     if fallback_time_end is not None:
#         conditions.append(f"time_period_end:<={fallback_time_end}")
    
#     if fallback_content_type and not (filters and filters.get('data_type')):
#         if fallback_content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={fallback_content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
#     now = datetime.now()
    
#     if time_range == 'day':
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(
#     intent: str,
#     discovery: Dict = None,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """
#     Builds Typesense sort_by string.
    
#     v5.0: Uses word_discovery's sort instruction for temporal queries.
#     """
#     if discovery:
#         sort_instruction = discovery.get('sort')
#         if sort_instruction:
#             field = sort_instruction.get('field')
#             order = sort_instruction.get('order', 'asc')
#             if field:
#                 print(f"   🔢 Sort from vocabulary: {field}:{order}")
#                 return f"{field}:{order},authority_score:desc"
    
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH STRATEGY SELECTION (v5.0)
# # ============================================================================

# def determine_search_strategy(discovery: Dict) -> str:
#     """
#     Determine search strategy based on word_discovery output.
#     """
#     filters = discovery.get('filters', [])
#     locations = discovery.get('locations', [])
#     valid_count = discovery.get('valid_count', 0)
#     unknown_count = discovery.get('unknown_count', 0)
#     total_score = discovery.get('total_score', 0)
    
#     if filters or locations:
#         if total_score >= 200:
#             return 'two_stage_strict'
#         else:
#             return 'two_stage_mixed'
    
#     if valid_count > 0 and unknown_count <= valid_count:
#         return 'mixed'
    
#     return 'semantic'


# def get_filter_terms_from_discovery(discovery: Dict) -> List[str]:
#     """
#     Extract searchable terms from discovery result.
#     """
#     terms = []
    
#     for f in discovery.get('filters', []):
#         term = f.get('term', f.get('value', ''))
#         if term:
#             terms.append(term)
    
#     for loc in discovery.get('locations', []):
#         term = loc.get('term', '')
#         if term:
#             terms.append(term)
    
#     for t in discovery.get('terms', []):
#         if t.get('status') in ('valid', 'corrected'):
#             if t.get('category') != 'stopword':
#                 word = t.get('search_word', t.get('word', ''))
#                 if word and word not in terms:
#                     terms.append(word)
    
#     return terms


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = ''
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search with graph filters."""
    
#     fallback_location = None
#     fallback_time_start, fallback_time_end = None, None
#     fallback_content_type = None
    
#     if not discovery or not discovery.get('locations'):
#         fallback_location = extract_location_from_query_fallback(query)
#     if not discovery:
#         fallback_time_start, fallback_time_end = extract_time_period_from_query_fallback(query)
#         fallback_content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters,
#         fallback_time_start=fallback_time_start,
#         fallback_time_end=fallback_time_end,
#         fallback_location=fallback_location,
#         fallback_content_type=fallback_content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string('general', discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector + graph filters."""
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_strict_params(
#     query: str,
#     valid_terms: List[str],
#     query_embedding: Optional[List[float]] = None,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """STRICT MODE: Text search on valid terms + optional vector rerank."""
    
#     search_query = ' '.join(valid_terms)
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': search_query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 2,
#         'num_typos': 0,
#     }
    
#     if query_embedding:
#         params['vector_query'] = build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page,
#             alpha=0.3
#         )
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# def build_exact_phrase_params(
#     phrase: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general'
# ) -> Dict:
#     """EXACT PHRASE MODE: For dropdown selections."""
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': f'"{phrase}"',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'num_typos': 0,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # RESULT PROCESSING (v5.3 - FIXED)
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
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
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results_keyword(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int]:
#     """
#     KEYWORD PATH: Format results WITHOUT score filtering.
#     Returns (results, total) where total = Typesense's found count.
    
#     Trust Typesense's text relevance ranking - no client-side filtering.
#     """
#     hits = raw_response.get('hits', [])
#     found = raw_response.get('found', 0)
    
#     if not hits:
#         return [], 0
    
#     # Just format, don't filter
#     results = [format_result(hit, query) for hit in hits]
    
#     # Sort by Typesense's implicit ranking (they're already sorted)
#     # But we recalculate scores for display purposes
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     return results, found


# def process_results_semantic(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int]:
#     """
#     SEMANTIC PATH: Format results WITH score filtering.
#     Returns (results, total) where total = count AFTER filtering.
    
#     Since we filter client-side, we can't use Typesense's 'found' for pagination.
#     The total reflects what we actually return.
#     """
#     hits = raw_response.get('hits', [])
    
#     if not hits:
#         return [], 0
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return [], 0
    
#     # Apply score filtering
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     # Always return at least the top result
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     filtered_results = filtered_results[:20]
    
#     # CRITICAL: total = filtered count, NOT Typesense's found
#     return filtered_results, len(filtered_results)


# # Keep the old function for backward compatibility, but it now just calls the keyword version
# def process_results(raw_response: Dict, query: str = '') -> List[Dict]:
#     """
#     DEPRECATED: Use process_results_keyword or process_results_semantic instead.
#     This now defaults to keyword behavior (no filtering).
#     """
#     results, _ = process_results_keyword(raw_response, query)
#     return results


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH: Graph Filter -> Vector Rerank
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> List[str]:
#     """STAGE 1: Use graph filters to find candidate documents."""
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (graph filter): Found {len(doc_ids)} candidates")
#         if filter_str:
#             print(f"   Filter: {filter_str}")
#         for hit in hits[:5]:
#             print(f"   - {hit['document'].get('document_title', 'NO TITLE')}")
        
#         return doc_ids
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return []


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     print(f"🔍 Stage 2 (vector rerank): Reranking {len(document_ids)} documents")
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Dict:
#     """Two-stage search: Graph filter THEN vector rerank."""
#     candidate_ids = stage1_keyword_filter(
#         query=query,
#         discovery=discovery,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 discovery=discovery,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query
#             )
#             return execute_search_multi(params)
#         else:
#             return {'hits': [], 'found': 0}
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result


# # ============================================================================
# # MAIN SEARCH FUNCTION (v5.3 - FIXED PAGINATION)
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
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - FIXED VERSION v5.3.
    
#     KEY FIX: Pagination now matches actual results.
    
#     alt_mode:
#         'n' = User clicked dropdown item (KEYWORD PATH - no score filtering)
#         'y' = User typed freely (SEMANTIC PATH - score filtering applied)
    
#     KEYWORD PATH:
#         - No word discovery
#         - No embedding
#         - No score filtering
#         - total = Typesense's 'found' count (accurate pagination)
    
#     SEMANTIC PATH:
#         - Full word discovery + embedding
#         - Score filtering applied
#         - total = filtered count (pagination limited to current page)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
#     # =========================================================================
#     # KEYWORD PATH (alt_mode='n' means user clicked a dropdown item)
#     # No score filtering - trust Typesense's relevance ranking
#     # =========================================================================
    
#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PATH: '{query}' (alt_mode={alt_mode})")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build filter string from UI filters only (no word discovery)
#         filter_conditions = []
#         if filters:
#             if filters.get('data_type') and filters.get('data_type') in VALID_DATA_TYPES:
#                 filter_conditions.append(f"document_data_type:={filters['data_type']}")
#             if filters.get('category'):
#                 safe_cat = re.sub(r'[^a-zA-Z0-9_]', '', filters['category'])
#                 if safe_cat:
#                     filter_conditions.append(f"document_category:={safe_cat}")
#             if filters.get('schema') and filters.get('schema') in VALID_SCHEMAS:
#                 filter_conditions.append(f"document_schema:={filters['schema']}")
        
#         filter_by = ' && '.join(filter_conditions) if filter_conditions else ''
        
#         # Flexible text search - trust Typesense's ranking
#         search_params = {
#             'q': query,
#             'query_by': 'key_facts,document_title,primary_keywords,entity_names,document_summary',
#             'query_by_weights': '10,8,5,4,2',
#             'page': page,
#             'per_page': per_page,
#             'exclude_fields': 'embedding',
#             'num_typos': 1,
#             'drop_tokens_threshold': 2,
#         }
        
#         if filter_by:
#             search_params['filter_by'] = filter_by
        
#         print(f"   Search params: q='{query}', filter_by='{filter_by}'")
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         # KEYWORD PATH: Use process_results_keyword (no score filtering)
#         results, total = process_results_keyword(raw_response, query)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: KEYWORD | Found: {total} | Returned: {len(results)}")
        
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total,  # FIXED: Uses Typesense's found count
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
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
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#             }
#         }
    
#     # =========================================================================
#     # SEMANTIC PATH (alt_mode='y' means user typed freely)
#     # Score filtering applied - total reflects filtered count
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH: '{query}' (alt_mode={alt_mode})")
    
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     valid_terms = get_filter_terms_from_discovery(discovery)
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     # =========================================================================
#     # STRATEGY SELECTION (based on word discovery results)
#     # =========================================================================
    
#     search_strategy = determine_search_strategy(discovery)
    
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'two_stage_strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
    
#     # =========================================================================
#     # DETECT INTENT
#     # =========================================================================
#     intent = detect_query_intent(query, pos_tags)
    
#     # =========================================================================
#     # EXECUTE SEARCH
#     # =========================================================================
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy.startswith('two_stage'):
#         actual_strategy = search_strategy
#         t3 = time.time()
#         raw_response = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy == 'mixed':
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # =========================================================================
#     # DEBUG OUTPUT
#     # =========================================================================
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Typesense found: {raw_response.get('found', 0)}")
#     print(f"📊 Scores: avg={discovery.get('average_score', 0)}, total={discovery.get('total_score', 0)}, max={discovery.get('max_score', 0)}")
    
#     if skip_embedding:
#         print(f"⚡ Embedding skipped (source: {search_source or 'skip_embedding=True'})")
    
#     if discovery.get('filters'):
#         print(f"   📌 Filters: {[f['term'] for f in discovery.get('filters', [])]}")
#     if discovery.get('locations'):
#         print(f"   📍 Locations: {[l['term'] for l in discovery.get('locations', [])]}")
#     if discovery.get('sort'):
#         print(f"   🔢 Sort: {discovery.get('sort')}")
    
#     if discovery.get('corrections'):
#         print(f"   ✏️ Corrections: {[(c.get('original'), c.get('corrected')) for c in discovery.get('corrections', [])]}")
    
#     # =========================================================================
#     # PROCESS RESULTS - SEMANTIC PATH uses score filtering
#     # =========================================================================
#     results, total = process_results_semantic(raw_response, query)
    
#     raw_count = len(raw_response.get('hits', []))
#     print(f"📊 Filtering: {raw_count} hits → {len(results)} results (total for pagination: {total})")
#     if results:
#         top_score = results[0]['score']
#         print(f"   Top score: {top_score:.4f} | Cutoff: {max(MIN_SCORE_THRESHOLD, top_score * 0.7):.4f}")
    
#     # =========================================================================
#     # BUILD RESPONSE
#     # =========================================================================
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': total,  # FIXED: Uses filtered count for semantic path
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
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'graph_filters': discovery.get('filters', []),
#             'graph_locations': discovery.get('locations', []),
#             'graph_sort': discovery.get('sort'),
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]
#                 cluster = parts[1]
                
#                 if cluster != '00':
#                     try:
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         print(f"🔗 Found {len(results)} related docs in cluster {prefix}-{cluster}")
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
        
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
            
#             print(f"🔍 Found {len(vector_results)} similar docs via vector search")
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """Find all documents in the same semantic cluster."""
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]
#     cluster = parts[1]
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """Get facet counts specifically for dynamic tabs."""
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
#         facets = {
#             'data_type': [],
#             'category': [],
#             'schema': []
#         }
        
#         data_type_labels = {
#             'article': 'Articles',
#             'person': 'People',
#             'business': 'Businesses',
#             'place': 'Places',
#             'media': 'Media',
#             'event': 'Events',
#             'product': 'Products',
#         }
        
#         category_labels = {
#             'healthcare_medical': 'Healthcare',
#             'fashion': 'Fashion',
#             'beauty': 'Beauty',
#             'food_recipes': 'Food & Recipes',
#             'travel_tourism': 'Travel',
#             'entertainment': 'Entertainment',
#             'business': 'Business',
#             'education': 'Education',
#             'technology': 'Technology',
#             'sports': 'Sports',
#             'finance': 'Finance',
#             'real_estate': 'Real Estate',
#             'lifestyle': 'Lifestyle',
#             'news': 'News',
#             'culture': 'Culture',
#             'general': 'General',
#         }
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             counts = facet['counts']
            
#             if field == 'document_data_type':
#                 facets['data_type'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': data_type_labels.get(c['value'], c['value'].title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_category':
#                 facets['category'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': category_labels.get(c['value'], c['value'].replace('_', ' ').title())
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
            
#             elif field == 'document_schema':
#                 facets['schema'] = [
#                     {
#                         'value': c['value'],
#                         'count': c['count'],
#                         'label': c['value']
#                     }
#                     for c in counts if c['count'] > 0
#                 ]
        
#         return facets
        
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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


# # ============================================================================
# # SIMPLE TYPESENSE SEARCH (Used by views for basic queries)
# # ============================================================================

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
#     """
#     Simple Typesense search wrapper for views.
#     Returns raw Typesense response.
#     """
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
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return response
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {
#                 'type': 'person_card',
#                 'data': top_result
#             }
        
#         if intent == 'location':
#             return {
#                 'type': 'place_card',
#                 'data': top_result
#             }
        
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     event = {
#         'timestamp': datetime.now().isoformat(),
#         'session_id': session_id,
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'total_results': total_results,
#         'filters': filters,
#         'page': page,
#         'zero_results': total_results == 0,
#         'semantic_enabled': semantic_enabled,
#         'semantic_boost': semantic_boost,
#         'alt_mode': alt_mode
#     }
#     pass



# """
# typesense_calculations.py (FIXED v6.0)

# KEY FIXES:
# 1. Keyword path: No score filtering, total = Typesense found count
# 2. Semantic path: Score filtering, total = filtered count  
# 3. BOTH paths now return facets from the SAME search query
# 4. Facets are consistent with results (no separate facet query needed)

# This ensures:
# - Tab counts match actual filterable results
# - Pagination is accurate
# - No mismatch between facets and results
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # US STATE ABBREVIATION MAPPING
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

# US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# # ============================================================================
# # QUERY PREPROCESSING
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """Strip filler words and truncate for faster embedding."""
#     cleaned = query
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
#     cleaned = ' '.join(cleaned.split())
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """Get embedding with query truncation and connection pooling."""
#     clean_query = truncate_for_embedding(query, max_words=40)
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

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


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'query': query,
#         'corrected_query': query,
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'corrections': [],
#         'filters': [],
#         'locations': [],
#         'sort': None,
#         'ngrams': [],
#         'terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'processing_time_ms': 0
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


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


# # ============================================================================
# # REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])

# # Labels for UI display
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
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
#     return 'general'


# # ============================================================================
# # FALLBACK EXTRACTION
# # ============================================================================

# def extract_location_from_query_fallback(query: str) -> Optional[str]:
#     """FALLBACK: Extracts location from query using regex."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location.title()
#     return None


# def extract_time_period_from_query_fallback(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """FALLBACK: Extracts time period from query using regex."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER BUILDING
# # ============================================================================

# def build_filter_string_from_discovery(
#     discovery: Dict,
#     filters: Dict = None,
#     fallback_time_start: int = None,
#     fallback_time_end: int = None,
#     fallback_location: str = None,
#     fallback_content_type: str = None
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
#                 print(f"   📌 Filter from vocabulary: {field}:={safe_value}")
    
#     # Location filters
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
#                 print(f"   📍 Location from vocabulary: {values}")
    
#     # 2. UI FILTERS
#     if filters:
#         data_type = filters.get('data_type')
#         if data_type and data_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={data_type}")
        
#         category = filters.get('category')
#         if category:
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         schema = filters.get('schema')
#         if schema and schema in VALID_SCHEMAS:
#             conditions.append(f"document_schema:={schema}")
        
#         source = filters.get('source')
#         if source:
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             conditions.extend(time_conditions)
        
#         ui_location = filters.get('location')
#         if ui_location:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', ui_location)
#             if safe_loc:
#                 loc_title = safe_loc.title()
#                 abbrev = US_STATE_ABBREV.get(safe_loc.lower(), '')
#                 loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#                 if abbrev:
#                     loc_parts.append(f"location_state:={abbrev}")
#                 conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     # 3. FALLBACK EXTRACTION
#     if not discovery.get('locations') and fallback_location:
#         if not (filters and filters.get('location')):
#             loc_title = fallback_location.title()
#             abbrev = US_STATE_ABBREV.get(fallback_location.lower(), '')
#             loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#             if abbrev:
#                 loc_parts.append(f"location_state:={abbrev}")
#             conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     if fallback_time_start is not None:
#         conditions.append(f"time_period_start:>={fallback_time_start}")
#     if fallback_time_end is not None:
#         conditions.append(f"time_period_end:<={fallback_time_end}")
    
#     if fallback_content_type and not (filters and filters.get('data_type')):
#         if fallback_content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={fallback_content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
#     now = datetime.now()
    
#     if time_range == 'day':
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(
#     intent: str,
#     discovery: Dict = None,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if discovery:
#         sort_instruction = discovery.get('sort')
#         if sort_instruction:
#             field = sort_instruction.get('field')
#             order = sort_instruction.get('order', 'asc')
#             if field:
#                 return f"{field}:{order},authority_score:desc"
    
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH STRATEGY SELECTION
# # ============================================================================

# def determine_search_strategy(discovery: Dict) -> str:
#     """Determine search strategy based on word_discovery output."""
#     filters = discovery.get('filters', [])
#     locations = discovery.get('locations', [])
#     valid_count = discovery.get('valid_count', 0)
#     unknown_count = discovery.get('unknown_count', 0)
#     total_score = discovery.get('total_score', 0)
    
#     if filters or locations:
#         if total_score >= 200:
#             return 'two_stage_strict'
#         else:
#             return 'two_stage_mixed'
    
#     if valid_count > 0 and unknown_count <= valid_count:
#         return 'mixed'
    
#     return 'semantic'


# def get_filter_terms_from_discovery(discovery: Dict) -> List[str]:
#     """Extract searchable terms from discovery result."""
#     terms = []
    
#     for f in discovery.get('filters', []):
#         term = f.get('term', f.get('value', ''))
#         if term:
#             terms.append(term)
    
#     for loc in discovery.get('locations', []):
#         term = loc.get('term', '')
#         if term:
#             terms.append(term)
    
#     for t in discovery.get('terms', []):
#         if t.get('status') in ('valid', 'corrected'):
#             if t.get('category') != 'stopword':
#                 word = t.get('search_word', t.get('word', ''))
#                 if word and word not in terms:
#                     terms.append(word)
    
#     return terms


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = '',
#     include_facets: bool = True
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search with graph filters."""
    
#     fallback_location = None
#     fallback_time_start, fallback_time_end = None, None
#     fallback_content_type = None
    
#     if not discovery or not discovery.get('locations'):
#         fallback_location = extract_location_from_query_fallback(query)
#     if not discovery:
#         fallback_time_start, fallback_time_end = extract_time_period_from_query_fallback(query)
#         fallback_content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     # Add facets to get counts from SAME query
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters,
#         fallback_time_start=fallback_time_start,
#         fallback_time_end=fallback_time_end,
#         fallback_location=fallback_location,
#         fallback_content_type=fallback_content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string('general', discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general',
#     include_facets: bool = True
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector + graph filters."""
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general',
#     include_facets: bool = True
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # FACET PARSING
# # ============================================================================

# def parse_facets_from_response(raw_response: Dict) -> Dict[str, List[Dict]]:
#     """
#     Parse facet counts from Typesense response.
#     Returns dict with data_type, category, schema facets.
#     """
#     facets = {
#         'data_type': [],
#         'category': [],
#         'schema': []
#     }
    
#     for facet in raw_response.get('facet_counts', []):
#         field = facet.get('field_name', '')
#         counts = facet.get('counts', [])
        
#         if field == 'document_data_type':
#             facets['data_type'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': DATA_TYPE_LABELS.get(c.get('value', ''), c.get('value', '').title())
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_category':
#             facets['category'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': CATEGORY_LABELS.get(c.get('value', ''), c.get('value', '').replace('_', ' ').title())
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_schema':
#             facets['schema'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': c.get('value', '')
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
    
#     return facets


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
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
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results_keyword(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int, Dict]:
#     """
#     KEYWORD PATH: Format results WITHOUT score filtering.
#     Returns (results, total, facets) where total = Typesense's found count.
#     """
#     hits = raw_response.get('hits', [])
#     found = raw_response.get('found', 0)
#     facets = parse_facets_from_response(raw_response)
    
#     if not hits:
#         return [], 0, facets
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     return results, found, facets


# def process_results_semantic(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int, Dict]:
#     """
#     SEMANTIC PATH: Format results WITH score filtering.
#     Returns (results, total, facets) where total = count AFTER filtering.
    
#     IMPORTANT: Facets are from the same query, so they represent
#     what's available BEFORE score filtering. For accurate tab counts
#     in semantic mode, we adjust the facets based on filtered results.
#     """
#     hits = raw_response.get('hits', [])
#     raw_facets = parse_facets_from_response(raw_response)
    
#     if not hits:
#         return [], 0, raw_facets
    
#     results = [format_result(hit, query) for hit in hits]
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     if not results:
#         return [], 0, raw_facets
    
#     # Apply score filtering
#     top_score = results[0]['score']
#     relative_cutoff = top_score * 0.7
#     effective_cutoff = max(MIN_SCORE_THRESHOLD, relative_cutoff)
    
#     filtered_results = [r for r in results if r['score'] >= effective_cutoff]
    
#     if not filtered_results and results:
#         filtered_results = [results[0]]
    
#     filtered_results = filtered_results[:20]
    
#     # Recalculate facets from filtered results for accuracy
#     filtered_facets = {
#         'data_type': {},
#         'category': {},
#         'schema': {}
#     }
    
#     for r in filtered_results:
#         dt = r.get('data_type', '')
#         cat = r.get('category', '')
#         sch = r.get('schema', '')
        
#         if dt:
#             filtered_facets['data_type'][dt] = filtered_facets['data_type'].get(dt, 0) + 1
#         if cat:
#             filtered_facets['category'][cat] = filtered_facets['category'].get(cat, 0) + 1
#         if sch:
#             filtered_facets['schema'][sch] = filtered_facets['schema'].get(sch, 0) + 1
    
#     # Convert to list format with labels
#     final_facets = {
#         'data_type': [
#             {
#                 'value': k,
#                 'count': v,
#                 'label': DATA_TYPE_LABELS.get(k, k.title())
#             }
#             for k, v in sorted(filtered_facets['data_type'].items(), key=lambda x: -x[1])
#         ],
#         'category': [
#             {
#                 'value': k,
#                 'count': v,
#                 'label': CATEGORY_LABELS.get(k, k.replace('_', ' ').title())
#             }
#             for k, v in sorted(filtered_facets['category'].items(), key=lambda x: -x[1])
#         ],
#         'schema': [
#             {
#                 'value': k,
#                 'count': v,
#                 'label': k
#             }
#             for k, v in sorted(filtered_facets['schema'].items(), key=lambda x: -x[1])
#         ]
#     }
    
#     return filtered_results, len(filtered_results), final_facets


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> Tuple[List[str], Dict]:
#     """STAGE 1: Use graph filters to find candidate documents."""
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
#         facets = parse_facets_from_response(response)
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (graph filter): Found {len(doc_ids)} candidates")
        
#         return doc_ids, facets
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return [], {'data_type': [], 'category': [], 'schema': []}


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Tuple[Dict, Dict]:
#     """Two-stage search: Graph filter THEN vector rerank. Returns (response, facets)."""
#     candidate_ids, facets = stage1_keyword_filter(
#         query=query,
#         discovery=discovery,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 discovery=discovery,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query,
#                 include_facets=True
#             )
#             response = execute_search_multi(params)
#             facets = parse_facets_from_response(response)
#             return response, facets
#         else:
#             return {'hits': [], 'found': 0}, facets
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result, facets


# # ============================================================================
# # MAIN SEARCH FUNCTION (v6.0)
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
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - FIXED VERSION v6.0.
    
#     KEY FEATURES:
#     - Returns facets from the SAME search query (not separate)
#     - Keyword path: No score filtering, accurate pagination
#     - Semantic path: Score filtering, facets reflect filtered results
    
#     alt_mode:
#         'n' = KEYWORD PATH (dropdown click)
#         'y' = SEMANTIC PATH (typed freely)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
#     # =========================================================================
#     # KEYWORD PATH (alt_mode='n')
#     # =========================================================================
    
#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PATH: '{query}' (alt_mode={alt_mode})")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build filter string from UI filters only
#         filter_conditions = []
#         if filters:
#             if filters.get('data_type') and filters.get('data_type') in VALID_DATA_TYPES:
#                 filter_conditions.append(f"document_data_type:={filters['data_type']}")
#             if filters.get('category'):
#                 safe_cat = re.sub(r'[^a-zA-Z0-9_]', '', filters['category'])
#                 if safe_cat:
#                     filter_conditions.append(f"document_category:={safe_cat}")
#             if filters.get('schema') and filters.get('schema') in VALID_SCHEMAS:
#                 filter_conditions.append(f"document_schema:={filters['schema']}")
        
#         filter_by = ' && '.join(filter_conditions) if filter_conditions else ''
        
#         # Text search with facets
#         search_params = {
#             'q': query,
#             'query_by': 'key_facts,document_title,primary_keywords,entity_names,document_summary',
#             'query_by_weights': '10,8,5,4,2',
#             'page': page,
#             'per_page': per_page,
#             'exclude_fields': 'embedding',
#             'num_typos': 1,
#             'drop_tokens_threshold': 2,
#             'facet_by': 'document_data_type,document_category,document_schema',
#             'max_facet_values': 20,
#         }
        
#         if filter_by:
#             search_params['filter_by'] = filter_by
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         results, total, facets = process_results_keyword(raw_response, query)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: KEYWORD | Found: {total} | Returned: {len(results)}")
        
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total,
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             # NEW: Facets from same query
#             'facets': facets,
#             'data_type_facets': facets.get('data_type', []),
#             'category_facets': facets.get('category', []),
#             'schema_facets': facets.get('schema', []),
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
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#             }
#         }
    
#     # =========================================================================
#     # SEMANTIC PATH (alt_mode='y')
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH: '{query}' (alt_mode={alt_mode})")
    
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     valid_terms = get_filter_terms_from_discovery(discovery)
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     search_strategy = determine_search_strategy(discovery)
    
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'two_stage_strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
#     intent = detect_query_intent(query, pos_tags)
    
#     # Execute search based on strategy
#     facets = {'data_type': [], 'category': [], 'schema': []}
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy.startswith('two_stage'):
#         actual_strategy = search_strategy
#         t3 = time.time()
#         raw_response, facets = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy == 'mixed':
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # Debug output
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Typesense found: {raw_response.get('found', 0)}")
    
#     # Process results with score filtering
#     results, total, facets = process_results_semantic(raw_response, query)
    
#     print(f"📊 Filtering: {len(raw_response.get('hits', []))} hits → {len(results)} results")
    
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': total,
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
#         # NEW: Facets from same query (recalculated for filtered results)
#         'facets': facets,
#         'data_type_facets': facets.get('data_type', []),
#         'category_facets': facets.get('category', []),
#         'schema_facets': facets.get('schema', []),
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
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'graph_filters': discovery.get('filters', []),
#             'graph_locations': discovery.get('locations', []),
#             'graph_sort': discovery.get('sort'),
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS (unchanged)
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]
#                 cluster = parts[1]
                
#                 if cluster != '00':
#                     try:
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
        
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """Find all documents in the same semantic cluster."""
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]
#     cluster = parts[1]
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """
#     DEPRECATED: Use facets returned by execute_full_search instead.
#     This is kept for backward compatibility.
#     """
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         return parse_facets_from_response(response)
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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
#     """Simple Typesense search wrapper for views."""
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
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return response
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {'type': 'person_card', 'data': top_result}
#         if intent == 'location':
#             return {'type': 'place_card', 'data': top_result}
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     pass


# """
# typesense_calculations.py (FIXED v6.0)

# KEY FIXES:
# 1. Keyword path: No score filtering, total = Typesense found count
# 2. Semantic path: Score filtering, total = filtered count  
# 3. BOTH paths now return facets from the SAME search query
# 4. Facets are consistent with results (no separate facet query needed)

# This ensures:
# - Tab counts match actual filterable results
# - Pagination is accurate
# - No mismatch between facets and results
# """

# import typesense
# from typing import Dict, List, Tuple, Optional, Any
# import re
# from decouple import config
# from datetime import datetime
# from concurrent.futures import ThreadPoolExecutor
# import threading
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry


# # ============================================================================
# # THREAD POOL - Reused across requests (3 workers for I/O-bound tasks)
# # ============================================================================

# _executor = ThreadPoolExecutor(max_workers=3)


# # ============================================================================
# # CONNECTION POOLING FOR EMBEDDING API
# # ============================================================================

# _http_session = None
# _session_lock = threading.Lock()


# def _get_http_session():
#     """Reusable HTTP session with connection pooling."""
#     global _http_session
#     if _http_session is None:
#         with _session_lock:
#             if _http_session is None:
#                 _http_session = requests.Session()
#                 adapter = HTTPAdapter(
#                     pool_connections=10,
#                     pool_maxsize=10,
#                     max_retries=Retry(total=2, backoff_factor=0.1)
#                 )
#                 _http_session.mount('http://', adapter)
#                 _http_session.mount('https://', adapter)
#     return _http_session


# # ============================================================================
# # US STATE ABBREVIATION MAPPING
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

# US_STATE_FULL = {v: k.title() for k, v in US_STATE_ABBREV.items()}


# # ============================================================================
# # QUERY PREPROCESSING
# # ============================================================================

# FILLER_PATTERNS = [
#     re.compile(r'\b(i am |i\'m |i want to |i need to |can you |please |help me )\b', re.I),
#     re.compile(r'\b(looking for |find me |search for |show me |tell me about )\b', re.I),
#     re.compile(r'\b(what is |who is |where is |how to |how do i |how can i )\b', re.I),
#     re.compile(r'\b(could you |would you |i would like |do you know |do you have )\b', re.I),
#     re.compile(r'\b(the best |a good |some good |any good |really good |very good )\b', re.I),
#     re.compile(r'\b(basically |actually |just |really |very |quite |pretty )\b', re.I),
#     re.compile(r'\b(i think |i believe |i guess |maybe |perhaps |probably )\b', re.I),
#     re.compile(r'\b(kind of |sort of |type of |lots of |a lot of )\b', re.I),
# ]


# def truncate_for_embedding(query: str, max_words: int = 40) -> str:
#     """Strip filler words and truncate for faster embedding."""
#     cleaned = query
#     for pattern in FILLER_PATTERNS:
#         cleaned = pattern.sub(' ', cleaned)
#     cleaned = ' '.join(cleaned.split())
#     words = cleaned.split()
#     if len(words) > max_words:
#         cleaned = ' '.join(words[:max_words])
#     return cleaned.strip() or query[:200]


# # ============================================================================
# # EMBEDDING CLIENT
# # ============================================================================

# EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')


# def get_query_embedding(query: str) -> Optional[List[float]]:
#     """Get embedding with query truncation and connection pooling."""
#     clean_query = truncate_for_embedding(query, max_words=40)
#     try:
#         session = _get_http_session()
#         response = session.post(
#             EMBEDDING_SERVICE_URL,
#             json={"text": clean_query},
#             timeout=2
#         )
#         response.raise_for_status()
#         return response.json().get("embedding")
#     except Exception as e:
#         print(f"⚠️ Embedding error: {e}")
#         return None


# # ============================================================================
# # WORD DISCOVERY
# # ============================================================================

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


# def _do_word_discovery(query: str) -> Dict:
#     """Wrapper for thread pool."""
#     if WORD_DISCOVERY_AVAILABLE:
#         return process_query_optimized(query, verbose=False)
#     return {
#         'query': query,
#         'corrected_query': query,
#         'valid_count': 0,
#         'unknown_count': len(query.split()),
#         'corrections': [],
#         'filters': [],
#         'locations': [],
#         'sort': None,
#         'ngrams': [],
#         'terms': [],
#         'total_score': 0,
#         'average_score': 0,
#         'max_score': 0,
#         'processing_time_ms': 0
#     }


# def _do_embedding(query: str) -> Optional[List[float]]:
#     """Wrapper for thread pool."""
#     return get_query_embedding(query)


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


# # ============================================================================
# # REGEX PATTERNS
# # ============================================================================

# LOCATION_PATTERNS = [
#     re.compile(r'\b(in|near|around|at)\s+\w+'),
#     re.compile(r'\b(city|state|country|region)\b'),
#     re.compile(r'\b(restaurant|store|shop|hotel|near me)\b'),
# ]

# HISTORICAL_PATTERNS = [
#     re.compile(r'\b(history|historical|ancient|medieval|colonial)\b'),
#     re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b'),
#     re.compile(r'\b([0-9]{2}th|[0-9]{2}st)\s+century\b'),
#     re.compile(r'\b(war|empire|kingdom|dynasty|era|period)\b'),
# ]

# PRODUCT_PATTERNS = [
#     re.compile(r'\b(buy|price|cheap|expensive|review|best)\b'),
#     re.compile(r'\b(product|item|purchase|order|shipping)\b'),
#     re.compile(r'\$[0-9]+'),
# ]

# PERSON_PATTERNS = [
#     re.compile(r'\b(who is|biography|born|died|life of)\b'),
#     re.compile(r'\b(ceo|president|founder|actor|artist|author)\b'),
#     re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
# ]

# MEDIA_PATTERNS = [
#     re.compile(r'\b(movie|film|song|album|video|watch|listen)\b'),
#     re.compile(r'\b(trailer|episode|season|soundtrack)\b'),
# ]

# LOCATION_EXTRACT_PATTERNS = [
#     re.compile(r'\b(?:in|near|around|at)\s+([a-zA-Z\s]+?)(?:\s+(?:for|with|and)|$)'),
#     re.compile(r'\b([a-zA-Z\s]+?)\s+(?:restaurants?|stores?|shops?|hotels?)\b'),
# ]

# DECADE_PATTERN = re.compile(r'\b(\d{4})s\b')
# CENTURY_PATTERN = re.compile(r'\b(\d{1,2})(?:st|nd|rd|th)\s+century\b')


# # ============================================================================
# # CLIENT SETUP
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

# COLLECTION_NAME = 'documents'


# # ============================================================================
# # FIELD CONFIGURATION
# # ============================================================================

# SEARCH_FIELDS = [
#     'key_facts',
#     'document_title',
#     'primary_keywords',
#     'entity_names'
# ]

# DEFAULT_WEIGHTS = [10, 5, 3, 2]

# INTENT_WEIGHTS = {
#     'general':    [10, 5, 3, 2],
#     'location':   [8, 5, 3, 4],
#     'historical': [10, 4, 4, 3],
#     'product':    [8, 6, 4, 2],
#     'person':     [10, 5, 3, 5],
#     'media':      [9, 5, 4, 3],
# }

# MIN_SCORE_THRESHOLD = 0.5

# SOURCE_AUTHORITY = {
#     'britannica': 95,
#     'wikipedia': 90,
#     'government': 90,
#     'academic': 88,
#     'news': 70,
#     'blog': 50,
#     'social': 40,
#     'default': 60
# }


# # ============================================================================
# # VALID FILTER VALUES
# # ============================================================================

# VALID_DATA_TYPES = frozenset([
#     'article', 'person', 'business', 'place', 'media', 'event', 'product'
# ])

# VALID_SCHEMAS = frozenset([
#     'Article', 'BlogPosting', 'NewsArticle', 'HowTo', 'Recipe',
#     'WebPage', 'FAQPage', 'Product', 'Service', 'LocalBusiness',
#     'Person', 'Organization', 'Event', 'VideoObject', 'ImageGallery',
#     'AudioObject', 'Book', 'Movie', 'MusicRecording'
# ])

# VALID_CATEGORIES = frozenset([
#     'healthcare_medical', 'fashion', 'beauty', 'food_recipes',
#     'travel_tourism', 'entertainment', 'business', 'education',
#     'technology', 'sports', 'finance', 'real_estate', 'automotive',
#     'lifestyle', 'news', 'culture', 'politics', 'science', 'general'
# ])

# # Labels for UI display
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
# # INTENT DETECTION
# # ============================================================================

# def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
#     """Analyzes query to determine user intent."""
#     query_lower = query.lower()
    
#     for pattern in LOCATION_PATTERNS:
#         if pattern.search(query_lower):
#             return 'location'
#     for pattern in HISTORICAL_PATTERNS:
#         if pattern.search(query_lower):
#             return 'historical'
#     for pattern in PRODUCT_PATTERNS:
#         if pattern.search(query_lower):
#             return 'product'
#     for pattern in PERSON_PATTERNS:
#         if pattern.search(query_lower):
#             return 'person'
#     for pattern in MEDIA_PATTERNS:
#         if pattern.search(query_lower):
#             return 'media'
#     return 'general'


# # ============================================================================
# # FALLBACK EXTRACTION
# # ============================================================================

# def extract_location_from_query_fallback(query: str) -> Optional[str]:
#     """FALLBACK: Extracts location from query using regex."""
#     stopwords = {'the', 'a', 'best', 'good', 'top'}
#     for pattern in LOCATION_EXTRACT_PATTERNS:
#         match = pattern.search(query.lower())
#         if match:
#             location = match.group(1).strip()
#             if location not in stopwords:
#                 return location.title()
#     return None


# def extract_time_period_from_query_fallback(query: str) -> Tuple[Optional[int], Optional[int]]:
#     """FALLBACK: Extracts time period from query using regex."""
#     query_lower = query.lower()
    
#     match = DECADE_PATTERN.search(query_lower)
#     if match:
#         decade = int(match.group(1))
#         return (decade, decade + 99)
    
#     match = CENTURY_PATTERN.search(query_lower)
#     if match:
#         century = int(match.group(1))
#         start = (century - 1) * 100
#         return (start, start + 99)
    
#     era_ranges = {
#         'ancient': (-3000, 500),
#         'medieval': (500, 1500),
#         'colonial': (1500, 1900),
#         'modern': (1900, 2024),
#         'contemporary': (1990, 2024)
#     }
#     for era, (start, end) in era_ranges.items():
#         if era in query_lower:
#             return (start, end)
#     return (None, None)


# def extract_content_type_from_query(query: str) -> Optional[str]:
#     """Detects if user wants specific content type."""
#     query_lower = query.lower()
#     type_indicators = {
#         'video': ['video', 'watch', 'documentary', 'film', 'movie', 'youtube', 'tiktok'],
#         'article': ['article', 'read', 'blog', 'post', 'news'],
#         'product': ['buy', 'purchase', 'price', 'shop', 'store'],
#         'service': ['hire', 'book', 'appointment', 'service'],
#         'person': ['who is', 'biography', 'profile']
#     }
#     for content_type, indicators in type_indicators.items():
#         for indicator in indicators:
#             if indicator in query_lower:
#                 return content_type
#     return None


# # ============================================================================
# # FILTER BUILDING
# # ============================================================================

# def build_filter_string_from_discovery(
#     discovery: Dict,
#     filters: Dict = None,
#     fallback_time_start: int = None,
#     fallback_time_end: int = None,
#     fallback_location: str = None,
#     fallback_content_type: str = None
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
#                 print(f"   📌 Filter from vocabulary: {field}:={safe_value}")
    
#     # Location filters
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
#                 print(f"   📍 Location from vocabulary: {values}")
    
#     # 2. UI FILTERS
#     if filters:
#         data_type = filters.get('data_type')
#         if data_type and data_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={data_type}")
        
#         category = filters.get('category')
#         if category:
#             safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
#             if safe_category:
#                 conditions.append(f"document_category:={safe_category}")
        
#         schema = filters.get('schema')
#         if schema and schema in VALID_SCHEMAS:
#             conditions.append(f"document_schema:={schema}")
        
#         source = filters.get('source')
#         if source:
#             safe_source = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', source)
#             if safe_source:
#                 conditions.append(f"document_brand:={safe_source}")
        
#         time_range = filters.get('time_range')
#         if time_range:
#             time_conditions = _parse_time_range_filter(time_range)
#             conditions.extend(time_conditions)
        
#         ui_location = filters.get('location')
#         if ui_location:
#             safe_loc = re.sub(r'[&|!=<>:;\[\]{}()\'"\\]', '', ui_location)
#             if safe_loc:
#                 loc_title = safe_loc.title()
#                 abbrev = US_STATE_ABBREV.get(safe_loc.lower(), '')
#                 loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#                 if abbrev:
#                     loc_parts.append(f"location_state:={abbrev}")
#                 conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     # 3. FALLBACK EXTRACTION
#     if not discovery.get('locations') and fallback_location:
#         if not (filters and filters.get('location')):
#             loc_title = fallback_location.title()
#             abbrev = US_STATE_ABBREV.get(fallback_location.lower(), '')
#             loc_parts = [f"location_state:={loc_title}", f"location_city:={loc_title}"]
#             if abbrev:
#                 loc_parts.append(f"location_state:={abbrev}")
#             conditions.append('(' + ' || '.join(loc_parts) + ')')
    
#     if fallback_time_start is not None:
#         conditions.append(f"time_period_start:>={fallback_time_start}")
#     if fallback_time_end is not None:
#         conditions.append(f"time_period_end:<={fallback_time_end}")
    
#     if fallback_content_type and not (filters and filters.get('data_type')):
#         if fallback_content_type in VALID_DATA_TYPES:
#             conditions.append(f"document_data_type:={fallback_content_type}")
    
#     return ' && '.join(conditions) if conditions else ''


# def _parse_time_range_filter(time_range: str) -> List[str]:
#     """Parse time_range filter value into Typesense conditions."""
#     conditions = []
#     now = datetime.now()
    
#     if time_range == 'day':
#         timestamp = int((now.timestamp() - 86400) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'week':
#         timestamp = int((now.timestamp() - 604800) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'month':
#         timestamp = int((now.timestamp() - 2592000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
#     elif time_range == 'year':
#         timestamp = int((now.timestamp() - 31536000) * 1000)
#         conditions.append(f"created_at:>={timestamp}")
    
#     return conditions


# def build_sort_string(
#     intent: str,
#     discovery: Dict = None,
#     user_location: Tuple[float, float] = None
# ) -> str:
#     """Builds Typesense sort_by string."""
#     if discovery:
#         sort_instruction = discovery.get('sort')
#         if sort_instruction:
#             field = sort_instruction.get('field')
#             order = sort_instruction.get('order', 'asc')
#             if field:
#                 return f"{field}:{order},authority_score:desc"
    
#     if intent == 'location' and user_location:
#         lat, lng = user_location
#         return f"location_geopoint({lat},{lng}):asc,authority_score:desc"
    
#     return "authority_score:desc,published_date:desc"


# def build_vector_query(
#     query_embedding: List[float],
#     k: int = 20,
#     alpha: float = 1.0
# ) -> str:
#     """Builds the vector_query string for Typesense."""
#     embedding_str = ','.join(str(x) for x in query_embedding)
#     return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


# # ============================================================================
# # SEARCH STRATEGY SELECTION
# # ============================================================================

# def determine_search_strategy(discovery: Dict) -> str:
#     """Determine search strategy based on word_discovery output."""
#     filters = discovery.get('filters', [])
#     locations = discovery.get('locations', [])
#     valid_count = discovery.get('valid_count', 0)
#     unknown_count = discovery.get('unknown_count', 0)
#     total_score = discovery.get('total_score', 0)
    
#     if filters or locations:
#         if total_score >= 200:
#             return 'two_stage_strict'
#         else:
#             return 'two_stage_mixed'
    
#     if valid_count > 0 and unknown_count <= valid_count:
#         return 'mixed'
    
#     return 'semantic'


# def get_filter_terms_from_discovery(discovery: Dict) -> List[str]:
#     """Extract searchable terms from discovery result."""
#     terms = []
    
#     for f in discovery.get('filters', []):
#         term = f.get('term', f.get('value', ''))
#         if term:
#             terms.append(term)
    
#     for loc in discovery.get('locations', []):
#         term = loc.get('term', '')
#         if term:
#             terms.append(term)
    
#     for t in discovery.get('terms', []):
#         if t.get('status') in ('valid', 'corrected'):
#             if t.get('category') != 'stopword':
#                 word = t.get('search_word', t.get('word', ''))
#                 if word and word not in terms:
#                     terms.append(word)
    
#     return terms


# # ============================================================================
# # SEARCH PARAMETER BUILDERS
# # ============================================================================

# def build_semantic_params(
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     query: str = '',
#     include_facets: bool = True
# ) -> Dict:
#     """SEMANTIC MODE: Pure vector search with graph filters."""
    
#     fallback_location = None
#     fallback_time_start, fallback_time_end = None, None
#     fallback_content_type = None
    
#     if not discovery or not discovery.get('locations'):
#         fallback_location = extract_location_from_query_fallback(query)
#     if not discovery:
#         fallback_time_start, fallback_time_end = extract_time_period_from_query_fallback(query)
#         fallback_content_type = extract_content_type_from_query(query)
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=1.0
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     # Add facets to get counts from SAME query
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters,
#         fallback_time_start=fallback_time_start,
#         fallback_time_end=fallback_time_end,
#         fallback_location=fallback_location,
#         fallback_content_type=fallback_content_type
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string('general', discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_mixed_params(
#     query: str,
#     query_embedding: List[float],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general',
#     include_facets: bool = True
# ) -> Dict:
#     """MIXED MODE: Light text matching + vector + graph filters."""
    
#     params = {
#         'q': query,
#         'query_by': 'document_title,key_facts',
#         'query_by_weights': '5,3',
#         'vector_query': build_vector_query(
#             query_embedding=query_embedding,
#             k=per_page * 2,
#             alpha=0.8
#         ),
#         'page': page,
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#         'drop_tokens_threshold': 10,
#         'typo_tokens_threshold': 10,
#         'num_typos': 2,
#     }
    
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     if discovery:
#         sort_str = build_sort_string(intent, discovery)
#         if sort_str:
#             params['sort_by'] = sort_str
    
#     return params


# def build_fallback_text_params(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     intent: str = 'general',
#     include_facets: bool = True
# ) -> Dict:
#     """FALLBACK: Full text search when embedding fails."""
    
#     weights = INTENT_WEIGHTS.get(intent, DEFAULT_WEIGHTS)
    
#     params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'query_by_weights': ','.join(str(w) for w in weights),
#         'page': page,
#         'per_page': per_page,
#         'drop_tokens_threshold': 5,
#         'typo_tokens_threshold': 3,
#         'num_typos': 1,
#     }
    
#     if include_facets:
#         params['facet_by'] = 'document_data_type,document_category,document_schema'
#         params['max_facet_values'] = 20
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     return params


# # ============================================================================
# # FACET PARSING
# # ============================================================================

# def parse_facets_from_response(raw_response: Dict) -> Dict[str, List[Dict]]:
#     """
#     Parse facet counts from Typesense response.
#     Returns dict with data_type, category, schema facets.
#     """
#     facets = {
#         'data_type': [],
#         'category': [],
#         'schema': []
#     }
    
#     for facet in raw_response.get('facet_counts', []):
#         field = facet.get('field_name', '')
#         counts = facet.get('counts', [])
        
#         if field == 'document_data_type':
#             facets['data_type'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': DATA_TYPE_LABELS.get(c.get('value', ''), c.get('value', '').title())
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_category':
#             facets['category'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': CATEGORY_LABELS.get(c.get('value', ''), c.get('value', '').replace('_', ' ').title())
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
        
#         elif field == 'document_schema':
#             facets['schema'] = [
#                 {
#                     'value': c.get('value', ''),
#                     'count': c.get('count', 0),
#                     'label': c.get('value', '')
#                 }
#                 for c in counts if c.get('value') and c.get('count', 0) > 0
#             ]
    
#     return facets


# # ============================================================================
# # RESULT PROCESSING
# # ============================================================================

# def calculate_final_score(hit: Dict, query: str = '') -> float:
#     """Combines vector_distance and authority scores."""
#     vector_distance = hit.get('vector_distance', 1.0)
#     vector_similarity = max(0, 1 - vector_distance)
    
#     doc = hit.get('document', {})
#     authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
#     text_score = hit.get('text_match', 0) / 100000000
    
#     if text_score > 0:
#         final_score = (vector_similarity * 0.5) + (text_score * 0.3) + (authority * 0.2)
#     else:
#         final_score = (vector_similarity * 0.7) + (authority * 0.3)
    
#     return round(min(1.0, final_score), 4)


# def format_result(hit: Dict, query: str = '') -> Dict:
#     """Transforms a Typesense hit into clean response format."""
#     doc = hit.get('document', {})
#     highlights = hit.get('highlights', [])
    
#     highlight_map = {}
#     for h in highlights:
#         field = h.get('field')
#         snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
#         highlight_map[field] = snippet
    
#     vector_distance = hit.get('vector_distance')
#     semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
#     return {
#         'id': doc.get('document_uuid'),
#         'title': doc.get('document_title', 'Untitled'),
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
#         'date': doc.get('published_date_string', ''),
#         'published_date': doc.get('published_date_string', ''),
#         'authority_score': doc.get('authority_score', 0),
#         'cluster_uuid': doc.get('cluster_uuid'),
#         'semantic_uuid': doc.get('semantic_uuid'),
#         'key_facts': doc.get('key_facts', []),
#         'key_facts_highlighted': highlight_map.get('key_facts', ''),
#         'semantic_score': semantic_score,
#         'location': {
#             'city': doc.get('location_city'),
#             'state': doc.get('location_state'),
#             'country': doc.get('location_country'),
#             'region': doc.get('location_region')
#         },
#         'time_period': {
#             'start': doc.get('time_period_start'),
#             'end': doc.get('time_period_end'),
#             'context': doc.get('time_context')
#         },
#         'score': calculate_final_score(hit, query),
#         'related_sources': []
#     }


# def process_results_keyword(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int, Dict]:
#     """
#     KEYWORD PATH: Format results, trust Typesense's ranking.
#     Returns (results, total, facets) where total = Typesense's found count.
#     """
#     hits = raw_response.get('hits', [])
#     found = raw_response.get('found', 0)
#     facets = parse_facets_from_response(raw_response)
    
#     if not hits:
#         return [], 0, facets
    
#     results = [format_result(hit, query) for hit in hits]
#     # Keep Typesense's ranking order, just add our score for display
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     return results, found, facets


# def process_results_semantic(raw_response: Dict, query: str = '') -> Tuple[List[Dict], int, Dict]:
#     """
#     SEMANTIC PATH: Format results, trust Typesense's vector ranking.
#     Returns (results, total, facets) where total = Typesense's found count.
    
#     NO CLIENT-SIDE SCORE FILTERING - this was causing:
#     - Facet counts to be wrong (9 shown, 2 returned)
#     - Pagination to break (12 pages shown, only 1 page of results)
    
#     Typesense's vector search already ranks by semantic similarity.
#     We trust that ranking instead of applying arbitrary score cutoffs.
#     """
#     hits = raw_response.get('hits', [])
#     found = raw_response.get('found', 0)
#     facets = parse_facets_from_response(raw_response)
    
#     if not hits:
#         return [], 0, facets
    
#     results = [format_result(hit, query) for hit in hits]
#     # Sort by our calculated score (combines vector similarity + authority)
#     results.sort(key=lambda x: x['score'], reverse=True)
    
#     # NO SCORE FILTERING - trust Typesense's ranking
#     # The vector search already returns the most relevant results
    
#     return results, found, facets


# # ============================================================================
# # SEARCH EXECUTION
# # ============================================================================

# def execute_search_multi(search_params: Dict) -> Dict:
#     """Execute search using multi_search endpoint."""
#     search_requests = {
#         'searches': [{
#             'collection': COLLECTION_NAME,
#             **search_params
#         }]
#     }
    
#     try:
#         response = client.multi_search.perform(search_requests, {})
#         return response['results'][0]
#     except Exception as e:
#         print(f"TYPESENSE ERROR: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# # ============================================================================
# # TWO-STAGE SEARCH
# # ============================================================================

# def stage1_keyword_filter(
#     query: str,
#     discovery: Dict = None,
#     filters: Dict = None,
#     max_candidates: int = 50
# ) -> Tuple[List[str], Dict]:
#     """STAGE 1: Use graph filters to find candidate documents."""
#     params = {
#         'q': query,
#         'query_by': 'key_facts,document_title,primary_keywords,entity_names',
#         'query_by_weights': '10,8,5,3',
#         'per_page': max_candidates,
#         'include_fields': 'document_uuid,document_title',
#         'drop_tokens_threshold': 1,
#         'typo_tokens_threshold': 1,
#         'num_typos': 1,
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#     }
    
#     filter_str = build_filter_string_from_discovery(
#         discovery=discovery or {},
#         filters=filters
#     )
#     if filter_str:
#         params['filter_by'] = filter_str
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         hits = response.get('hits', [])
#         facets = parse_facets_from_response(response)
        
#         doc_ids = [hit['document']['document_uuid'] for hit in hits if hit.get('document', {}).get('document_uuid')]
        
#         print(f"📝 Stage 1 (graph filter): Found {len(doc_ids)} candidates")
        
#         return doc_ids, facets
        
#     except Exception as e:
#         print(f"❌ Stage 1 error: {e}")
#         return [], {'data_type': [], 'category': [], 'schema': []}


# def stage2_vector_rerank(
#     query_embedding: List[float],
#     document_ids: List[str],
#     per_page: int = 20
# ) -> Dict:
#     """STAGE 2: Vector search filtered to specific documents."""
#     if not document_ids:
#         return {'hits': [], 'found': 0}
    
#     if not query_embedding:
#         id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids[:per_page]])
#         params = {
#             'q': '*',
#             'filter_by': f'document_uuid:[{id_filter}]',
#             'per_page': per_page,
#         }
#         return execute_search_multi(params)
    
#     id_filter = ','.join([f'`{doc_id}`' for doc_id in document_ids])
#     embedding_str = ','.join(str(x) for x in query_embedding)
    
#     params = {
#         'q': '*',
#         'filter_by': f'document_uuid:[{id_filter}]',
#         'vector_query': f"embedding:([{embedding_str}], k:{len(document_ids)}, alpha:1.0)",
#         'per_page': per_page,
#         'exclude_fields': 'embedding',
#     }
    
#     return execute_search_multi(params)


# def execute_two_stage_search(
#     query: str,
#     query_embedding: Optional[List[float]],
#     discovery: Dict = None,
#     filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20
# ) -> Tuple[Dict, Dict]:
#     """Two-stage search: Graph filter THEN vector rerank. Returns (response, facets)."""
#     candidate_ids, facets = stage1_keyword_filter(
#         query=query,
#         discovery=discovery,
#         filters=filters,
#         max_candidates=50
#     )
    
#     if not candidate_ids:
#         print("⚠️ Stage 1 found no candidates, falling back to semantic search")
#         if query_embedding:
#             params = build_semantic_params(
#                 query_embedding=query_embedding,
#                 discovery=discovery,
#                 filters=filters,
#                 page=page,
#                 per_page=per_page,
#                 query=query,
#                 include_facets=True
#             )
#             response = execute_search_multi(params)
#             facets = parse_facets_from_response(response)
#             return response, facets
#         else:
#             return {'hits': [], 'found': 0}, facets
    
#     result = stage2_vector_rerank(
#         query_embedding=query_embedding,
#         document_ids=candidate_ids,
#         per_page=per_page
#     )
    
#     return result, facets


# # ============================================================================
# # MAIN SEARCH FUNCTION (v6.0)
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
#     skip_embedding: bool = False,
#     search_source: str = None
# ) -> Dict:
#     """
#     Main entry point for search - FIXED VERSION v6.0.
    
#     KEY FEATURES:
#     - Returns facets from the SAME search query (not separate)
#     - Keyword path: No score filtering, accurate pagination
#     - Semantic path: Score filtering, facets reflect filtered results
    
#     alt_mode:
#         'n' = KEYWORD PATH (dropdown click)
#         'y' = SEMANTIC PATH (typed freely)
#     """
#     import time
#     times = {}
#     t0 = time.time()
    
#     if filters:
#         active_filters = {k: v for k, v in filters.items() if v}
#         if active_filters:
#             print(f"🎛️ Active UI filters: {active_filters}")
    
#     # =========================================================================
#     # KEYWORD PATH (alt_mode='n')
#     # =========================================================================
    
#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PATH: '{query}' (alt_mode={alt_mode})")
        
#         t1 = time.time()
#         intent = detect_query_intent(query, pos_tags)
        
#         # Build filter string from UI filters only
#         filter_conditions = []
#         if filters:
#             if filters.get('data_type') and filters.get('data_type') in VALID_DATA_TYPES:
#                 filter_conditions.append(f"document_data_type:={filters['data_type']}")
#             if filters.get('category'):
#                 safe_cat = re.sub(r'[^a-zA-Z0-9_]', '', filters['category'])
#                 if safe_cat:
#                     filter_conditions.append(f"document_category:={safe_cat}")
#             if filters.get('schema') and filters.get('schema') in VALID_SCHEMAS:
#                 filter_conditions.append(f"document_schema:={filters['schema']}")
        
#         filter_by = ' && '.join(filter_conditions) if filter_conditions else ''
        
#         # Text search with facets
#         search_params = {
#             'q': query,
#             'query_by': 'key_facts,document_title,primary_keywords,entity_names,document_summary',
#             'query_by_weights': '10,8,5,4,2',
#             'page': page,
#             'per_page': per_page,
#             'exclude_fields': 'embedding',
#             'num_typos': 1,
#             'drop_tokens_threshold': 2,
#             'facet_by': 'document_data_type,document_category,document_schema',
#             'max_facet_values': 20,
#         }
        
#         if filter_by:
#             search_params['filter_by'] = filter_by
        
#         t2 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t2) * 1000, 2)
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         results, total, facets = process_results_keyword(raw_response, query)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 Strategy: KEYWORD | Found: {total} | Returned: {len(results)}")
        
#         search_time = round(time.time() - t0, 3)
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total,
#             'page': page,
#             'per_page': per_page,
#             'search_time': search_time,
#             'session_id': session_id,
#             'semantic_enabled': False,
#             'search_strategy': 'keyword',
#             'alt_mode': alt_mode,
#             'skip_embedding': True,
#             'search_source': search_source or 'dropdown',
#             'valid_terms': query.split(),
#             'unknown_terms': [],
#             # NEW: Facets from same query
#             'facets': facets,
#             'data_type_facets': facets.get('data_type', []),
#             'category_facets': facets.get('category', []),
#             'schema_facets': facets.get('schema', []),
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
#                 'data_type': filters.get('data_type') if filters else None,
#                 'category': filters.get('category') if filters else None,
#                 'schema': filters.get('schema') if filters else None,
#             }
#         }
    
#     # =========================================================================
#     # SEMANTIC PATH (alt_mode='y')
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH: '{query}' (alt_mode={alt_mode})")
    
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     valid_terms = get_filter_terms_from_discovery(discovery)
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     search_strategy = determine_search_strategy(discovery)
    
#     if skip_embedding and search_strategy == 'semantic':
#         if valid_terms:
#             search_strategy = 'two_stage_strict'
#         else:
#             search_strategy = 'text_fallback'
    
#     semantic_enabled = query_embedding is not None
#     intent = detect_query_intent(query, pos_tags)
    
#     # Execute search based on strategy
#     facets = {'data_type': [], 'category': [], 'schema': []}
    
#     if not semantic_enabled:
#         actual_strategy = 'text_fallback'
#         search_params = build_fallback_text_params(
#             query=corrected_query,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy.startswith('two_stage'):
#         actual_strategy = search_strategy
#         t3 = time.time()
#         raw_response, facets = execute_two_stage_search(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page
#         )
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     elif search_strategy == 'mixed':
#         actual_strategy = 'mixed'
#         search_params = build_mixed_params(
#             query=corrected_query,
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             intent=intent,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
        
#     else:
#         actual_strategy = 'semantic'
#         search_params = build_semantic_params(
#             query_embedding=query_embedding,
#             discovery=discovery,
#             filters=filters,
#             page=page,
#             per_page=per_page,
#             query=query,
#             include_facets=True
#         )
#         t3 = time.time()
#         raw_response = execute_search_multi(search_params)
#         times['typesense'] = round((time.time() - t3) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     # Debug output
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 Strategy: {actual_strategy.upper()} | Found: {raw_response.get('found', 0)}")
    
#     # Process results - NO score filtering, trust Typesense ranking
#     results, total, facets = process_results_semantic(raw_response, query)
    
#     print(f"📊 Results: {len(results)} on this page, {total} total")
    
#     search_time = round(time.time() - t0, 3)
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': total,
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
#         # NEW: Facets from same query (recalculated for filtered results)
#         'facets': facets,
#         'data_type_facets': facets.get('data_type', []),
#         'category_facets': facets.get('category', []),
#         'schema_facets': facets.get('schema', []),
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
#             'data_type': filters.get('data_type') if filters else None,
#             'category': filters.get('category') if filters else None,
#             'schema': filters.get('schema') if filters else None,
#             'graph_filters': discovery.get('filters', []),
#             'graph_locations': discovery.get('locations', []),
#             'graph_sort': discovery.get('sort'),
#         }
#     }


# # ============================================================================
# # CONVENIENCE FUNCTIONS (unchanged)
# # ============================================================================

# def quick_search(query: str, limit: int = 10) -> List[Dict]:
#     """Quick semantic search for autocomplete."""
#     query_embedding = get_query_embedding(query)
    
#     if not query_embedding:
#         params = {
#             'q': query,
#             'query_by': 'document_title,key_facts',
#             'per_page': limit,
#             'include_fields': 'document_uuid,document_title,document_url,key_facts'
#         }
#         try:
#             response = client.collections[COLLECTION_NAME].documents.search(params)
#             return [hit['document'] for hit in response.get('hits', [])]
#         except:
#             return []
    
#     params = {
#         'q': '*',
#         'vector_query': build_vector_query(query_embedding, k=limit, alpha=1.0),
#         'per_page': limit,
#         'exclude_fields': 'embedding',
#         'include_fields': 'document_uuid,document_title,document_url,key_facts'
#     }
    
#     response = execute_search_multi(params)
#     return [hit['document'] for hit in response.get('hits', [])]


# def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
#     """Find documents similar to a given document."""
#     try:
#         doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        
#         results = []
#         semantic_uuid = doc.get('semantic_uuid')
        
#         if semantic_uuid and '-' in semantic_uuid:
#             parts = semantic_uuid.split('-')
#             if len(parts) >= 2:
#                 prefix = parts[0]
#                 cluster = parts[1]
                
#                 if cluster != '00':
#                     try:
#                         params = {
#                             'q': '*',
#                             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999 && document_uuid:!={document_uuid}',
#                             'per_page': limit,
#                             'exclude_fields': 'embedding'
#                         }
                        
#                         response = execute_search_multi(params)
#                         results = [hit['document'] for hit in response.get('hits', [])]
                        
#                         if len(results) >= limit:
#                             return results[:limit]
#                     except Exception as e:
#                         print(f"⚠️ Cluster search failed: {e}")
        
#         embedding = doc.get('embedding')
        
#         if embedding and len(results) < limit:
#             remaining = limit - len(results)
#             existing_ids = [r.get('document_uuid') for r in results] + [document_uuid]
            
#             exclude_parts = [f'document_uuid:!={uid}' for uid in existing_ids if uid]
#             exclude_filter = ' && '.join(exclude_parts) if exclude_parts else ''
            
#             params = {
#                 'q': '*',
#                 'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{remaining + 1})",
#                 'per_page': remaining,
#                 'exclude_fields': 'embedding',
#             }
            
#             if exclude_filter:
#                 params['filter_by'] = exclude_filter
            
#             response = execute_search_multi(params)
#             vector_results = [hit['document'] for hit in response.get('hits', [])]
#             results.extend(vector_results)
        
#         return results[:limit]
    
#     except Exception as e:
#         print(f"Error finding similar documents: {e}")
#         return []


# def find_documents_in_cluster(semantic_uuid: str, limit: int = 10) -> List[Dict]:
#     """Find all documents in the same semantic cluster."""
#     if not semantic_uuid or '-' not in semantic_uuid:
#         return []
    
#     parts = semantic_uuid.split('-')
#     if len(parts) < 2:
#         return []
    
#     prefix = parts[0]
#     cluster = parts[1]
    
#     try:
#         params = {
#             'q': '*',
#             'filter_by': f'semantic_uuid:>={prefix}-{cluster}-0000 && semantic_uuid:<={prefix}-{cluster}-9999',
#             'per_page': limit,
#             'exclude_fields': 'embedding',
#             'sort_by': 'semantic_uuid:asc'
#         }
        
#         response = execute_search_multi(params)
#         return [hit['document'] for hit in response.get('hits', [])]
    
#     except Exception as e:
#         print(f"Error finding cluster documents: {e}")
#         return []


# # ============================================================================
# # HELPER FUNCTIONS (Required by views.py)
# # ============================================================================

# def get_facets(query: str) -> dict:
#     """Returns available filter options based on result set."""
#     search_params = {
#         'q': query,
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_category,document_data_type,document_schema,document_brand,location_country,temporal_relevance',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         facets = {}
        
#         for facet in response.get('facet_counts', []):
#             field = facet['field_name']
#             facets[field] = [
#                 {'value': count['value'], 'count': count['count']}
#                 for count in facet['counts']
#             ]
        
#         return facets
#     except:
#         return {}


# def get_tab_facets(query: str) -> dict:
#     """
#     DEPRECATED: Use facets returned by execute_full_search instead.
#     This is kept for backward compatibility.
#     """
#     search_params = {
#         'q': query if query else '*',
#         'query_by': ','.join(SEARCH_FIELDS),
#         'facet_by': 'document_data_type,document_category,document_schema',
#         'max_facet_values': 20,
#         'per_page': 0
#     }
    
#     try:
#         response = client.collections[COLLECTION_NAME].documents.search(search_params)
#         return parse_facets_from_response(response)
#     except Exception as e:
#         print(f"Error getting tab facets: {e}")
#         return {'data_type': [], 'category': [], 'schema': []}


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
#     """Simple Typesense search wrapper for views."""
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
#         response = client.collections[COLLECTION_NAME].documents.search(params)
#         return response
#     except Exception as e:
#         print(f"❌ typesense_search error: {e}")
#         return {'hits': [], 'found': 0, 'error': str(e)}


# def get_featured_result(query: str, intent: str, results: list) -> dict:
#     """Returns featured content: knowledge panel or featured snippet."""
#     if not results:
#         return None
    
#     top_result = results[0]
    
#     if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
#         if intent == 'person' and top_result.get('data_type') == 'person':
#             return {'type': 'person_card', 'data': top_result}
#         if intent == 'location':
#             return {'type': 'place_card', 'data': top_result}
#         return {
#             'type': 'featured_snippet',
#             'title': top_result.get('title'),
#             'snippet': top_result.get('summary', ''),
#             'key_facts': top_result.get('key_facts', [])[:3],
#             'source': top_result.get('source'),
#             'url': top_result.get('url'),
#             'image': top_result.get('image')
#         }
    
#     return None


# def log_search_event(
#     query: str,
#     corrected_query: str,
#     session_id: str,
#     intent: str,
#     total_results: int,
#     filters: dict,
#     page: int,
#     semantic_enabled: bool = False,
#     semantic_boost: float = 0.0,
#     alt_mode: str = 'n'
# ):
#     """Logs search event for analytics."""
#     pass
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

try:
    from .word_discovery import process_query_optimized
    WORD_DISCOVERY_AVAILABLE = True
except ImportError:
    try:
        from word_discovery import process_query_optimized
        WORD_DISCOVERY_AVAILABLE = True
    except ImportError:
        WORD_DISCOVERY_AVAILABLE = False
        print("⚠️ word_discovery not available, using basic search")


def _do_word_discovery(query: str) -> Dict:
    """Wrapper for thread pool."""
    if WORD_DISCOVERY_AVAILABLE:
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


def run_parallel_prep(query: str, skip_embedding: bool = False) -> Tuple[Dict, Optional[List[float]]]:
    """Run word discovery and embedding IN PARALLEL."""
    if skip_embedding:
        discovery = _do_word_discovery(query)
        return discovery, None
    
    discovery_future = _executor.submit(_do_word_discovery, query)
    embedding_future = _executor.submit(_do_embedding, query)
    
    discovery = discovery_future.result()
    embedding = embedding_future.result()
    
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

def build_filter_string_from_discovery(
    discovery: Dict,
    filters: Dict = None,
    exclude_data_type: bool = False
) -> str:
    """Builds Typesense filter_by string from word_discovery output."""
    conditions = []
    
    # 1. GRAPH-BASED FILTERS (from word_discovery)
    for filter_item in discovery.get('filters', []):
        field = filter_item.get('field')
        value = filter_item.get('value')
        if field and value:
            safe_value = re.sub(r'[&|!=<>;\[\]{}()\'"\\]', '', str(value))
            if safe_value:
                conditions.append(f"{field}:={safe_value}")
    
    # Location filters from discovery
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
                    conditions.append('(' + ' || '.join(loc_parts) + ')')
    
    # 2. UI FILTERS (skip data_type if exclude_data_type=True)
    if filters:
        if not exclude_data_type:
            data_type = filters.get('data_type')
            if data_type and data_type in VALID_DATA_TYPES:
                conditions.append(f"document_data_type:={data_type}")
        
        category = filters.get('category')
        if category:
            safe_category = re.sub(r'[^a-zA-Z0-9_]', '', category)
            if safe_category:
                conditions.append(f"document_category:={safe_category}")
        
        schema = filters.get('schema')
        if schema and schema in VALID_SCHEMAS:
            conditions.append(f"document_schema:={schema}")
    
    return ' && '.join(conditions) if conditions else ''


def build_sort_string(intent: str, discovery: Dict = None) -> str:
    """Builds Typesense sort_by string."""
    if discovery:
        sort_instruction = discovery.get('sort')
        if sort_instruction:
            field = sort_instruction.get('field')
            order = sort_instruction.get('order', 'asc')
            if field:
                return f"{field}:{order},authority_score:desc"
    return "authority_score:desc,published_date:desc"


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

def fetch_candidate_ids_graph_filter(
    query: str,
    discovery: Dict = None,
    filters: Dict = None,
    max_results: int = MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    STAGE 1: Graph Filter - Candidate Generation
    
    Uses inverted index fields (primary_keywords, entity_names, etc.) for FILTERING.
    This leverages your knowledge graph structure to narrow down candidates FAST.
    
    Returns list of {id, data_type, category, schema, authority_score} for caching.
    
    IMPORTANT: Does NOT rank semantically - just filters candidates.
    IMPORTANT: Does NOT filter by data_type so we get ALL types for tab counts.
    """
    # Build filter WITHOUT data_type (we want ALL types for tabs)
    filter_str = build_filter_string_from_discovery(
        discovery=discovery or {},
        filters=filters,
        exclude_data_type=True  # Get all types for accurate facet counts
    )
    
    PAGE_SIZE = 250  # Typesense max
    all_results = []
    current_page = 1
    max_pages = (max_results // PAGE_SIZE) + 1  # Safety limit
    
    while len(all_results) < max_results and current_page <= max_pages:
        # STAGE 1: Pure keyword/graph filtering - NO vector search here
        # This uses the inverted index for fast candidate retrieval
        params = {
            'q': query,
            'query_by': 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title',
            'query_by_weights': '10,8,6,4,3',  # Weight graph fields heavily
            'per_page': PAGE_SIZE,
            'page': current_page,
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score',
            'num_typos': 1,
            'drop_tokens_threshold': 2,
            'sort_by': 'authority_score:desc',  # Sort by authority for now (reranked later)
        }
        
        if filter_str:
            params['filter_by'] = filter_str
        
        try:
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
                    # No rank yet - will be set by semantic reranking
                })
            
            # Check if we've fetched all available results
            if len(all_results) >= found or len(hits) < PAGE_SIZE:
                break
            
            current_page += 1
            
        except Exception as e:
            print(f"❌ fetch_candidate_ids_graph_filter error (page {current_page}): {e}")
            break
    
    print(f"📊 STAGE 1 (Graph Filter): Retrieved {len(all_results)} candidates")
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
    times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
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


def get_related_searches(query: str, intent: str) -> list:
    """Returns 'People also search for' suggestions."""
    search_params = {
        'q': query,
        'query_by': 'primary_keywords,keywords,key_facts',
        'per_page': 10,
        'include_fields': 'primary_keywords,keywords,key_facts'
    }
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(search_params)
        
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
            for fact in doc.get('key_facts', [])[:3]:
                fact_words = [w for w in fact.split() if len(w) > 4 and w.lower() not in query_words]
                all_keywords.update(fact_words[:2])
        
        related = list(all_keywords)[:6]
        return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
    except:
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