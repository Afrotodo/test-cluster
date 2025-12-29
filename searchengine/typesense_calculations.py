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

"""
typesense_calculations.py

Handles all Typesense search logic:
- Query building
- Intent detection
- Weighting & ranking
- Result processing
- HYBRID SEARCH (text + semantic/vector)

Updated: Added semantic vector search with smart weighting
"""

import typesense
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime
from decouple import config

# ============================================================================
# EMBEDDING MODEL - Lazy loaded for semantic search
# ============================================================================

_embedding_model = None
_embedding_model_failed = False  # Track if model failed to load

def get_embedding_model():
    """
    Lazy loads the embedding model on first use.
    
    Why lazy loading:
    - Model uses ~200MB RAM
    - Only loads when actually needed
    - App starts faster
    - If it fails, we gracefully fall back to text-only search
    """
    global _embedding_model, _embedding_model_failed
    
    # Don't retry if it already failed
    if _embedding_model_failed:
        return None
    
    if _embedding_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            _embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Embedding model loaded successfully")
        except Exception as e:
            print(f"⚠️ Could not load embedding model: {e}")
            print("   Falling back to text-only search")
            _embedding_model_failed = True
            return None
    
    return _embedding_model


def get_query_embedding(query: str) -> Optional[List[float]]:
    """
    Generates embedding vector for a search query.
    
    Returns:
        List of 384 floats, or None if model not available
    """
    model = get_embedding_model()
    if model is None:
        return None
    
    try:
        embedding = model.encode(query)
        return embedding.tolist()
    except Exception as e:
        print(f"⚠️ Embedding generation failed: {e}")
        return None


# ============================================================================
# SEMANTIC BOOST CALCULATION
# ============================================================================

def calculate_semantic_boost(
    query: str,
    intent: str,
    alt_mode: str = 'n'
) -> float:
    """
    Determines how much to weight semantic vs text search.
    
    Returns:
        Float between 0.0 and 1.0
        - 0.0 = text only
        - 0.5 = balanced
        - 1.0 = semantic heavy
    
    Strategy:
        - Long queries benefit from semantic (understand context)
        - Short queries need text matching (exact terms)
        - Person/historical intents need semantic (context matters)
        - Product/location intents need text (exact names)
        - alt_mode='y' forces high semantic for exploration
    
    Args:
        alt_mode: 'y' = semantic heavy, 'n' = normal weighting
    """
    
    # Alt mode = user wants semantic exploration
    if alt_mode == 'y':
        return 0.85
    
    # Base boost by intent
    intent_boosts = {
        'general': 0.5,      # Balanced
        'person': 0.7,       # People queries benefit from semantic
        'historical': 0.7,   # Historical context needs semantic
        'media': 0.6,        # Genre matching benefits from semantic
        'location': 0.3,     # Place names need exact matching
        'product': 0.2,      # Product names need exact matching
    }
    
    base_boost = intent_boosts.get(intent, 0.5)
    
    # Adjust by query length
    word_count = len(query.split())
    
    if word_count >= 6:
        # Long query: boost semantic understanding
        length_modifier = 0.2
    elif word_count >= 4:
        # Medium query: slight boost
        length_modifier = 0.1
    elif word_count <= 2:
        # Very short: reduce semantic (need exact matches)
        length_modifier = -0.15
    else:
        length_modifier = 0
    
    # Adjust by query specificity
    # Queries with specific terms (names, places) need less semantic
    specific_indicators = [
        r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Proper names like "Martin Luther"
        r'\b\d{4}\b',                        # Years like 1965
        r'\b(Dr\.|Mr\.|Mrs\.|Rev\.)\b',     # Titles
    ]
    
    specificity_modifier = 0
    for pattern in specific_indicators:
        if re.search(pattern, query):
            specificity_modifier -= 0.1
    
    # Calculate final boost (clamp between 0 and 1)
    final_boost = base_boost + length_modifier + specificity_modifier
    return max(0.0, min(1.0, final_boost))


# ============================================================================
# PRE-COMPILED REGEX PATTERNS (unchanged from original)
# ============================================================================

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
    re.compile(r'\b(first\s+(?:black|african american|woman|female))\b'),
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

# Search fields - added key_facts for semantic search
SEARCH_FIELDS = [
    'key_facts',          # NEW: Primary for semantic matching
    'document_title',
    'primary_keywords', 
    'keywords',
    'semantic_keywords',
    'document_summary',
    'key_passages',
    'entity_names'
]

# Default weights - key_facts highest for semantic relevance
DEFAULT_WEIGHTS = [6, 5, 4, 3, 2, 2, 1, 2]

# Intent-specific weight profiles
INTENT_WEIGHTS = {
    'general':    [6, 5, 4, 3, 2, 2, 1, 2],  # Balanced
    'location':   [5, 4, 3, 3, 2, 2, 1, 4],  # Boost entity_names for places
    'historical': [6, 4, 4, 4, 3, 2, 1, 3],  # Boost keywords for era terms
    'product':    [4, 5, 4, 3, 2, 2, 1, 2],  # Title-heavy for products
    'person':     [7, 5, 3, 3, 2, 2, 1, 5],  # Key facts crucial for people
    'media':      [5, 5, 4, 3, 3, 2, 1, 3],  # Boost semantic for genre matching
}

# Authority scores by source
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
# INTENT DETECTION
# ============================================================================

def detect_query_intent(query: str, pos_tags: List[Tuple] = None) -> str:
    """
    Analyzes query to determine user intent.
    """
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


def extract_location_from_query(query: str) -> Optional[str]:
    """Extracts location from query."""
    stopwords = {'the', 'a', 'best', 'good', 'top'}
    
    for pattern in LOCATION_EXTRACT_PATTERNS:
        match = pattern.search(query.lower())
        if match:
            location = match.group(1).strip()
            if location not in stopwords:
                return location
    
    return None


def extract_time_period_from_query(query: str) -> Tuple[Optional[int], Optional[int]]:
    """Extracts time period from query."""
    query_lower = query.lower()
    
    match = DECADE_PATTERN.search(query_lower)
    if match:
        decade = int(match.group(1))
        return (decade, decade + 99)
    
    match = CENTURY_PATTERN.search(query_lower)
    if match:
        century = int(match.group(1))
        start = (century - 1) * 100
        return (start, start + 99)
    
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
    """Detects if user wants specific content type."""
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


# ============================================================================
# QUERY BUILDING
# ============================================================================

def build_query_weights(intent: str) -> str:
    """Returns comma-separated weights string."""
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
    """Builds Typesense filter_by string."""
    conditions = []
    
    if filters:
        if filters.get('category'):
            conditions.append(f"document_category:={filters['category']}")
        if filters.get('source'):
            conditions.append(f"document_brand:={filters['source']}")
        if filters.get('data_type'):
            conditions.append(f"document_data_type:={filters['data_type']}")
    
    if time_start is not None:
        conditions.append(f"time_period_start:>={time_start}")
    if time_end is not None:
        conditions.append(f"time_period_end:<={time_end}")
    
    if location:
        conditions.append(f"(location_city:={location} || location_state:={location} || location_country:={location} || location_region:={location})")
    
    if content_type:
        conditions.append(f"document_data_type:={content_type}")
    
    return ' && '.join(conditions) if conditions else ''


def build_sort_string(
    intent: str,
    user_location: Tuple[float, float] = None
) -> str:
    """Builds Typesense sort_by string."""
    if intent == 'location' and user_location:
        lat, lng = user_location
        return f"location_geopoint({lat},{lng}):asc,_text_match:desc,authority_score:desc"
    
    if intent == 'product':
        return "_text_match:desc,product_rating:desc,authority_score:desc"
    
    if intent == 'media':
        return "_text_match:desc,media_rating:desc,published_date:desc"
    
    # Default: relevance + authority + freshness
    return "_text_match:desc,authority_score:desc,published_date:desc"


def build_vector_query(
    query_embedding: List[float],
    k: int = 20,
    semantic_boost: float = 0.5
) -> str:
    """
    Builds the vector_query string for Typesense.
    
    Args:
        query_embedding: The query vector (384 floats)
        k: Number of nearest neighbors to find
        semantic_boost: How much to weight vector vs text (0-1)
    
    Returns:
        String like: "embedding:([0.1, -0.2, ...], k:20, alpha:0.5)"
    """
    # Convert embedding to string
    embedding_str = ','.join(str(x) for x in query_embedding)
    
    # Alpha controls text vs vector balance:
    # alpha=0 means pure text, alpha=1 means pure vector
    # We use semantic_boost directly as alpha
    alpha = round(semantic_boost, 2)
    
    return f"embedding:([{embedding_str}], k:{k}, alpha:{alpha})"


def build_search_params(
    query: str,
    intent: str,
    filters: Dict = None,
    page: int = 1,
    per_page: int = 20,
    user_location: Tuple[float, float] = None,
    safe_search: bool = True,
    query_embedding: List[float] = None,
    semantic_boost: float = 0.5
) -> Dict:
    """
    Constructs complete Typesense search parameters.
    Now includes vector_query for hybrid search.
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
        'highlight_full_fields': 'document_title,document_summary,key_facts',
        'highlight_start_tag': '<mark>',
        'highlight_end_tag': '</mark>',
        'snippet_threshold': 500,
        'exclude_fields': 'embedding',  # Don't return the large embedding array
    }
    
    # Add vector query if embedding available
    if query_embedding:
        params['vector_query'] = build_vector_query(
            query_embedding=query_embedding,
            k=per_page * 2,  # Get more candidates for re-ranking
            semantic_boost=semantic_boost
        )
    
    # Remove empty filter_by
    if not params['filter_by']:
        del params['filter_by']
    
    return params


# ============================================================================
# RESULT PROCESSING
# ============================================================================

def calculate_final_score(hit: Dict, semantic_boost: float = 0.5) -> float:
    """
    Combines text_match, vector_distance, and authority scores.
    
    Updated to include vector similarity in scoring.
    """
    # Get Typesense's relevance score
    text_score = hit.get('text_match', 0) / 100000000
    
    # Get vector distance (lower is better, convert to similarity)
    vector_distance = hit.get('vector_distance', 1.0)
    vector_similarity = max(0, 1 - vector_distance)  # Convert distance to similarity
    
    # Get authority score
    doc = hit.get('document', {})
    authority = doc.get('authority_score', SOURCE_AUTHORITY['default']) / 100
    
    # Get freshness score
    freshness = 0.5
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
    
    # Weighted combination - now includes vector similarity
    # Adjust weights based on semantic_boost
    text_weight = 0.4 * (1 - semantic_boost * 0.5)
    vector_weight = 0.3 * (0.5 + semantic_boost * 0.5)
    authority_weight = 0.25
    freshness_weight = 0.05
    
    final_score = (
        text_score * text_weight +
        vector_similarity * vector_weight +
        authority * authority_weight +
        freshness * freshness_weight
    )
    
    return round(final_score, 4)


def deduplicate_by_cluster(results: List[Dict]) -> List[Dict]:
    """Removes near-duplicate documents using cluster_uuid."""
    seen_clusters = {}
    deduplicated = []
    duplicates_grouped = {}
    
    for result in results:
        cluster = result.get('cluster_uuid')
        
        if cluster and cluster in seen_clusters:
            if cluster not in duplicates_grouped:
                duplicates_grouped[cluster] = []
            duplicates_grouped[cluster].append(result)
        else:
            if cluster:
                seen_clusters[cluster] = len(deduplicated)
            deduplicated.append(result)
    
    for i, result in enumerate(deduplicated):
        cluster = result.get('cluster_uuid')
        if cluster in duplicates_grouped:
            deduplicated[i]['related_sources'] = duplicates_grouped[cluster]
    
    return deduplicated


def format_result(hit: Dict, semantic_boost: float = 0.5) -> Dict:
    """Transforms a Typesense hit into clean response format."""
    doc = hit.get('document', {})
    highlights = hit.get('highlights', [])
    
    # Build highlight map
    highlight_map = {}
    for h in highlights:
        field = h.get('field')
        snippet = h.get('value') or h.get('snippet') or h.get('snippets', [''])[0]
        highlight_map[field] = snippet
    
    # Get vector distance for display (if available)
    vector_distance = hit.get('vector_distance')
    semantic_score = round(1 - vector_distance, 3) if vector_distance else None
    
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
        'date': doc.get('published_date_string', ''),
        'published_date': doc.get('published_date_string', ''),
        'authority_score': doc.get('authority_score', 0),
        'cluster_uuid': doc.get('cluster_uuid'),
        'key_facts': doc.get('key_facts', []),  # NEW: Include key facts
        'key_facts_highlighted': highlight_map.get('key_facts', ''),  # NEW
        'semantic_score': semantic_score,  # NEW: Vector similarity score
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
        'score': calculate_final_score(hit, semantic_boost),
        'related_sources': []
    }


def process_results(raw_response: Dict, semantic_boost: float = 0.5) -> List[Dict]:
    """Processes Typesense response into clean result list."""
    hits = raw_response.get('hits', [])
    
    results = [format_result(hit, semantic_boost) for hit in hits]
    results.sort(key=lambda x: x['score'], reverse=True)
    # results = deduplicate_by_cluster(results)
    
    return results


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_facets(query: str) -> dict:
    """Returns available filter options based on result set."""
    search_params = {
        'q': query,
        'query_by': ','.join(SEARCH_FIELDS),
        'facet_by': 'document_category,document_data_type,document_brand,location_country,temporal_relevance',
        'max_facet_values': 10,
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
            # Include key_facts as suggestions
            for fact in doc.get('key_facts', [])[:3]:
                # Extract key terms from facts
                fact_words = [w for w in fact.split() if len(w) > 4 and w not in query_words]
                all_keywords.update(fact_words[:2])
        
        related = list(all_keywords)[:6]
        return [{'query': f"{query} {kw}", 'label': kw} for kw in related]
    
    except:
        return []


def get_featured_result(query: str, intent: str, results: list) -> dict:
    """Returns featured content: knowledge panel or featured snippet."""
    if not results:
        return None
    
    top_result = results[0]
    
    if top_result.get('authority_score', 0) >= 85 and top_result.get('score', 0) >= 0.7:
        
        if intent == 'person' and top_result.get('data_type') == 'person':
            return {
                'type': 'person_card',
                'data': top_result
            }
        
        if intent == 'location':
            return {
                'type': 'place_card',
                'data': top_result
            }
        
        return {
            'type': 'featured_snippet',
            'title': top_result.get('title'),
            'snippet': top_result.get('summary', ''),
            'key_facts': top_result.get('key_facts', [])[:3],  # NEW
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
    event = {
        'timestamp': datetime.now().isoformat(),
        'session_id': session_id,
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
        'total_results': total_results,
        'filters': filters,
        'page': page,
        'zero_results': total_results == 0,
        'semantic_enabled': semantic_enabled,
        'semantic_boost': semantic_boost,
        'alt_mode': alt_mode
    }
    
    # Replace with your implementation
    # SearchLog.objects.create(**event)
    pass


# ============================================================================
# MAIN SEARCH FUNCTIONS
# ============================================================================

def execute_search_multi(search_params: Dict) -> Dict:
    """
    Execute search using multi_search endpoint.
    
    Why: Regular search has URL length limits.
    Vector queries are large, so we use multi_search with POST body.
    """
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


def execute_search(query: str, options: Dict = None) -> Dict:
    """
    Low-level search execution.
    Routes to multi_search if vector query present.
    """
    options = options or {}
    
    # Use multi_search if vector query is present (to avoid URL length issues)
    if 'vector_query' in options:
        return execute_search_multi(options)
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(options)
        return response
    except Exception as e:
        print(f"TYPESENSE ERROR: {e}")
        return {'hits': [], 'found': 0, 'error': str(e)}


def execute_full_search(
    query: str,
    session_id: str = None,
    filters: Dict = None,
    page: int = 1,
    per_page: int = 20,
    user_location: Tuple[float, float] = None,
    pos_tags: List[Tuple] = None,
    safe_search: bool = True,
    alt_mode: str = 'n'  # 'y' = semantic heavy, 'n' = normal
) -> Dict:
    """
    Main entry point for search.
    
    PARAMETER:
        alt_mode: str - 'y' for semantic-heavy search, 'n' for normal.
                       Use 'y' when user types custom query not found
                       in dropdown suggestions.
    
    HYBRID SEARCH BEHAVIOR:
        1. Always attempts to use semantic search
        2. Falls back to text-only if embedding model unavailable
        3. Semantic weight adjusted based on:
           - Intent (person/historical = more semantic)
           - Query length (longer = more semantic)
           - alt_mode='y' (forced high semantic)
    """
    import time
    start_time = time.time()
    
    # Step 1: Detect intent
    intent = detect_query_intent(query, pos_tags)
    
    # Step 2: Calculate semantic boost
    semantic_boost = calculate_semantic_boost(query, intent, alt_mode)
    
    # Step 3: Generate query embedding (may return None if model unavailable)
    query_embedding = get_query_embedding(query)
    semantic_enabled = query_embedding is not None
    
    # Step 4: Build search parameters
    search_params = build_search_params(
        query=query,
        intent=intent,
        filters=filters,
        page=page,
        per_page=per_page,
        user_location=user_location,
        safe_search=safe_search,
        query_embedding=query_embedding,
        semantic_boost=semantic_boost
    )
    
    # DEBUG LOGGING
    print("=" * 60)
    print("TYPESENSE HYBRID SEARCH DEBUG")
    print("=" * 60)
    print(f"Query: '{query}'")
    print(f"Intent: {intent}")
    print(f"Semantic enabled: {semantic_enabled}")
    print(f"Semantic boost: {semantic_boost}")
    print(f"Alt mode: {alt_mode}")
    if 'vector_query' in search_params:
        print(f"Vector query: YES (alpha={semantic_boost})")
    else:
        print("Vector query: NO (text-only search)")
    print("=" * 60)
    
    # Step 5: Execute search
    raw_response = execute_search(query, search_params)
    
    # DEBUG LOGGING
    print(f"Typesense found: {raw_response.get('found', 0)} documents")
    print(f"Hits returned: {len(raw_response.get('hits', []))}")
    if raw_response.get('hits'):
        first_hit = raw_response['hits'][0]
        first_doc = first_hit.get('document', {})
        print(f"First hit title: {first_doc.get('document_title', 'NO TITLE')}")
        print(f"First hit key_facts: {first_doc.get('key_facts', [])[:2]}")
        if first_hit.get('vector_distance'):
            print(f"First hit vector_distance: {first_hit.get('vector_distance')}")
    if raw_response.get('error'):
        print(f"ERROR: {raw_response.get('error')}")
    print("=" * 60)
    
    # Step 6: Process results
    results = process_results(raw_response, semantic_boost)
    
    # Step 7: Build final response
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
        'semantic_enabled': semantic_enabled,  # NEW
        'semantic_boost': semantic_boost,      # NEW
        'alt_mode': alt_mode,                  # NEW
        'filters_applied': {
            'time_period': extract_time_period_from_query(query),
            'location': extract_location_from_query(query),
            'content_type': extract_content_type_from_query(query)
        }
    }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def quick_search(query: str, limit: int = 10) -> List[Dict]:
    """
    Simple search for autocomplete or quick lookups.
    Text-only, no semantic.
    """
    params = {
        'q': query,
        'query_by': 'document_title,key_facts,primary_keywords',
        'per_page': limit,
        'include_fields': 'document_uuid,document_title,document_url,key_facts'
    }
    
    try:
        response = client.collections[COLLECTION_NAME].documents.search(params)
        return [hit['document'] for hit in response.get('hits', [])]
    except:
        return []


def semantic_search_only(query: str, limit: int = 10) -> List[Dict]:
    """
    Pure semantic search - useful for "find similar" features.
    """
    query_embedding = get_query_embedding(query)
    if not query_embedding:
        return []
    
    params = {
        'q': '*',  # Match all, rely only on vector
        'vector_query': build_vector_query(query_embedding, k=limit, semantic_boost=1.0),
        'per_page': limit,
        'exclude_fields': 'embedding'
    }
    
    response = execute_search_multi(params)
    return [hit['document'] for hit in response.get('hits', [])]


def find_similar_documents(document_uuid: str, limit: int = 5) -> List[Dict]:
    """
    Find documents similar to a given document.
    Uses the document's embedding to find neighbors.
    
    Useful for "Related articles" feature.
    """
    # First, get the document's embedding
    try:
        doc = client.collections[COLLECTION_NAME].documents[document_uuid].retrieve()
        embedding = doc.get('embedding')
        
        if not embedding:
            return []
        
        # Search for similar
        params = {
            'q': '*',
            'vector_query': f"embedding:([{','.join(str(x) for x in embedding)}], k:{limit + 1})",
            'per_page': limit + 1,  # +1 to exclude self
            'exclude_fields': 'embedding',
            'filter_by': f'document_uuid:!={document_uuid}'  # Exclude the source document
        }
        
        response = execute_search_multi(params)
        return [hit['document'] for hit in response.get('hits', [])][:limit]
    
    except Exception as e:
        print(f"Error finding similar documents: {e}")
        return []