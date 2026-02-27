# """
# typesense_discovery_bridge.py
# =============================
# Bridge between Word Discovery v2 and Typesense Search.

# PHILOSOPHY:
#     - Word Discovery provides RICH metadata (category, rank, pos, etc.)
#     - The bridge uses ALL metadata to paint the picture
#     - The picture determines: filters, field priorities, document types
#     - ONE query to Typesense with graph (filters) + semantic (embedding)

# FLOW:
#     1. RECEIVE: Full Word Discovery output with all metadata
#     2. PROFILE: Analyze metadata to understand query intent
#     3. MAP: Convert categories to Typesense fields and filters
#     4. BUILD: One query with filters + field weights + vector search
#     5. EXECUTE: Typesense handles graph + semantic fusion
#     6. RETURN: Ranked results

# QUERY PROFILE:
#     - Scores from Word Discovery determine PRIMARY INTENT
#     - Person query (MLK speech) vs Organization query (MLK Foundation)
#     - Intent determines: field_boosts, preferred_data_types, filters
# """

# import re
# import json
# import typesense
# from typing import Dict, List, Tuple, Optional, Any, Set
# from decouple import config


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# # Category to Typesense field mapping
# CATEGORY_TO_FIELD = {
#     # Locations → filter fields
#     'US City': 'location_city',
#     'US State': 'location_state',
#     'US County': 'location_city',
#     'City': 'location_city',
#     'State': 'location_state',
#     'Country': 'location_country',
#     'Location': 'location_city',
    
#     # Entities → search in entity_names
#     'Person': 'entity_names',
#     'Historical Figure': 'entity_names',
#     'Celebrity': 'entity_names',
#     'Athlete': 'entity_names',
#     'Politician': 'entity_names',
#     'Organization': 'entity_names',
#     'Company': 'entity_names',
#     'Business': 'entity_names',
#     'Brand': 'entity_names',
#     'HBCU': 'entity_names',
#     'entity': 'entity_names',
    
#     # Keywords → search in primary_keywords
#     'Keyword': 'primary_keywords',
#     'Topic': 'primary_keywords',
#     'Primary Keyword': 'primary_keywords',
    
#     # Dictionary words → search in key_facts, document_title
#     'Dictionary Word': 'key_facts',
    
#     # Media → search in semantic_keywords
#     'Song': 'semantic_keywords',
#     'Movie': 'semantic_keywords',
#     'Album': 'semantic_keywords',
#     'Book': 'semantic_keywords',
    
#     # Food → search in primary_keywords
#     'Food': 'primary_keywords',
#     'Cuisine': 'primary_keywords',
#     'Recipe': 'primary_keywords',
# }

# # Categories that should become FILTERS (not search terms)
# FILTER_CATEGORIES = frozenset([
#     'US City', 'US State', 'US County', 'City', 'State', 'Country', 'Location',
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
# ])

# KEYWORD_CATEGORIES = frozenset([
#     'Keyword', 'Topic', 'Primary Keyword',
# ])

# MEDIA_CATEGORIES = frozenset([
#     'Song', 'Movie', 'Album', 'Book', 'TV Show',
# ])

# # US State abbreviations
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


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

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


# def is_location_category(category: str) -> bool:
#     """Check if category is a location type."""
#     return category in LOCATION_CATEGORIES


# def is_state_category(category: str) -> bool:
#     """Check if category is specifically a state."""
#     if not category:
#         return False
#     cat_lower = category.lower()
#     return 'state' in cat_lower


# def is_city_category(category: str) -> bool:
#     """Check if category is specifically a city."""
#     if not category:
#         return False
#     cat_lower = category.lower()
#     return 'city' in cat_lower or 'county' in cat_lower


# # =============================================================================
# # STEP 1: RECEIVE - Parse Word Discovery Output
# # =============================================================================

# def parse_discovery_output(discovery: Dict) -> Dict:
#     """
#     Parse and normalize Word Discovery output.
#     Extracts all terms and ngrams with their full metadata.
#     """
#     parsed = {
#         'query': discovery.get('query', ''),
#         'corrected_query': discovery.get('corrected_query', ''),
#         'terms': [],
#         'ngrams': [],
#         'stats': discovery.get('stats', {}),
#     }
    
#     # Parse terms with full metadata
#     for term in discovery.get('terms', []):
#         parsed['terms'].append({
#             'position': term.get('position', 0),
#             'word': term.get('word', ''),
#             'display': term.get('display', term.get('word', '')),
#             'status': term.get('status', 'unknown'),
#             'is_stopword': term.get('is_stopword', False),
#             'part_of_ngram': term.get('part_of_ngram', False),
#             'category': term.get('category', ''),
#             'pos': term.get('pos', ''),
#             'rank': _parse_rank(term.get('rank', 0)),
#             'entity_type': term.get('entity_type', 'unigram'),
#             'description': term.get('description', ''),
#             'corrected': term.get('corrected'),
#             'match_count': term.get('match_count', 0),
#         })
    
#     # Parse ngrams with full metadata
#     for ngram in discovery.get('ngrams', []):
#         parsed['ngrams'].append({
#             'type': ngram.get('type', 'bigram'),
#             'phrase': ngram.get('phrase', ''),
#             'display': ngram.get('display', ngram.get('phrase', '')),
#             'words': ngram.get('words', []),
#             'positions': ngram.get('positions', []),
#             'category': ngram.get('category', ''),
#             'pos': ngram.get('pos', ''),
#             'rank': _parse_rank(ngram.get('rank', 0)),
#             'description': ngram.get('description', ''),
#         })
    
#     return parsed


# # =============================================================================
# # STEP 2: PROFILE - Analyze Metadata to Understand Intent
# # =============================================================================

# def build_query_profile(parsed: Dict) -> Dict:
#     """
#     Analyze ALL metadata to understand what the user wants.
#     This paints the picture.
    
#     Uses:
#     - Categories to identify entity types
#     - Ranks to weight importance
#     - POS to understand structure
#     - N-grams vs terms to understand phrases
#     """
#     profile = {
#         # Entity flags
#         'has_person': False,
#         'has_organization': False,
#         'has_location': False,
#         'has_keyword': False,
#         'has_media': False,
#         'has_dictionary_word': False,
        
#         # Cumulative scores by type (from ranks)
#         'person_score': 0,
#         'organization_score': 0,
#         'location_score': 0,
#         'keyword_score': 0,
#         'media_score': 0,
        
#         # Collected items by type
#         'persons': [],
#         'organizations': [],
#         'locations': [],
#         'keywords': [],
#         'media_items': [],
#         'search_terms': [],
        
#         # Location details for filters
#         'cities': [],
#         'states': [],
        
#         # Primary intent (determined by highest score)
#         'primary_intent': 'general',
        
#         # Preferred document types based on intent
#         'preferred_data_types': [],
        
#         # Field boosts for Typesense query_by_weights
#         'field_boosts': {
#             'primary_keywords': 10,
#             'entity_names': 8,
#             'semantic_keywords': 6,
#             'key_facts': 4,
#             'document_title': 3,
#         },
        
#         # All metadata for debugging
#         'all_terms': [],
#         'all_ngrams': [],
#     }
    
#     # Build term lookup by position (for n-gram cross-reference)
#     term_by_position = {t['position']: t for t in parsed['terms']}
    
#     # DEBUG: Show the mapping
#     print(f"\n   DEBUG term_by_position keys: {list(term_by_position.keys())}")
#     for pos, t in term_by_position.items():
#         print(f"   DEBUG pos {pos}: word='{t['word']}', category='{t['category']}', rank={t['rank']}")
    
#     # Track positions consumed by n-grams
#     ngram_positions = set()
    
#     # =================================================================
#     # Process N-grams First
#     # =================================================================
    
#     for ngram in parsed['ngrams']:
#         phrase = ngram['phrase']
#         category = ngram['category']
#         rank = ngram['rank']
#         positions = ngram['positions']
#         display = ngram['display']
        
#         # DEBUG: Show ngram positions
#         print(f"\n   DEBUG ngram '{phrase}': positions={positions}, category='{category}'")
        
#         ngram_positions.update(positions)
#         profile['all_ngrams'].append(ngram)
        
#         # Check individual term categories (they may be more accurate)
#         term_categories = []
#         for pos in positions:
#             term = term_by_position.get(pos, {})
#             if term.get('category'):
#                 term_categories.append({
#                     'word': term.get('word'),
#                     'category': term.get('category'),
#                     'rank': term.get('rank', 0),
#                 })
        
#         # DEBUG: Print what we found
#         print(f"   DEBUG n-gram '{phrase}': term_categories = {term_categories}")
        
#         # Determine if this n-gram contains location terms
#         has_city_term = any(is_city_category(tc['category']) for tc in term_categories)
#         has_state_term = any(is_state_category(tc['category']) for tc in term_categories)
        
#         print(f"   DEBUG: has_city_term={has_city_term}, has_state_term={has_state_term}")
        
#         # If individual terms indicate location, use that info
#         if has_city_term or has_state_term:
#             profile['has_location'] = True
            
#             for tc in term_categories:
#                 if is_city_category(tc['category']):
#                     city_name = tc['word'].title()
#                     if city_name not in [c['name'] for c in profile['cities']]:
#                         profile['cities'].append({
#                             'name': city_name,
#                             'rank': tc['rank'],
#                         })
#                     profile['location_score'] += tc['rank']
                    
#                 elif is_state_category(tc['category']):
#                     state_name = tc['word'].title()
#                     if state_name not in [s['name'] for s in profile['states']]:
#                         profile['states'].append({
#                             'name': state_name,
#                             'rank': tc['rank'],
#                             'variants': get_state_variants(tc['word']),
#                         })
#                     profile['location_score'] += tc['rank']
        
#         # Process based on n-gram's own category (if not location)
#         elif category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             profile['media_items'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             profile['search_terms'].append(phrase)
            
#         elif category in LOCATION_CATEGORIES:
#             profile['has_location'] = True
#             profile['location_score'] += rank
#             profile['locations'].append({'phrase': phrase, 'display': display, 'rank': rank})
#             # Don't add to search_terms - will be filter
            
#         else:
#             # Unknown category n-gram - add to search terms
#             profile['search_terms'].append(phrase)
    
#     # =================================================================
#     # Process Individual Terms (not in n-grams)
#     # =================================================================
    
#     for term in parsed['terms']:
#         position = term['position']
#         word = term['word']
#         display = term['display']
#         category = term['category']
#         rank = term['rank']
#         is_stopword = term['is_stopword']
#         part_of_ngram = term['part_of_ngram'] or (position in ngram_positions)
        
#         profile['all_terms'].append(term)
        
#         # Skip stopwords and terms already in n-grams
#         if is_stopword:
#             continue
            
#         if part_of_ngram:
#             continue
        
#         # Classify by category
#         if category in PERSON_CATEGORIES:
#             profile['has_person'] = True
#             profile['person_score'] += rank
#             profile['persons'].append({'word': word, 'display': display, 'rank': rank})
#             profile['search_terms'].append(word)
            
#         elif category in ORGANIZATION_CATEGORIES:
#             profile['has_organization'] = True
#             profile['organization_score'] += rank
#             profile['organizations'].append({'word': word, 'display': display, 'rank': rank})
#             profile['search_terms'].append(word)
            
#         elif category in KEYWORD_CATEGORIES:
#             profile['has_keyword'] = True
#             profile['keyword_score'] += rank
#             profile['keywords'].append({'word': word, 'display': display, 'rank': rank})
#             profile['search_terms'].append(word)
            
#         elif category in MEDIA_CATEGORIES:
#             profile['has_media'] = True
#             profile['media_score'] += rank
#             profile['media_items'].append({'word': word, 'display': display, 'rank': rank})
#             profile['search_terms'].append(word)
            
#         elif is_city_category(category):
#             profile['has_location'] = True
#             profile['location_score'] += rank
#             city_name = display or word.title()
#             if city_name not in [c['name'] for c in profile['cities']]:
#                 profile['cities'].append({'name': city_name, 'rank': rank})
            
#         elif is_state_category(category):
#             profile['has_location'] = True
#             profile['location_score'] += rank
#             state_name = display or word.title()
#             if state_name not in [s['name'] for s in profile['states']]:
#                 profile['states'].append({
#                     'name': state_name,
#                     'rank': rank,
#                     'variants': get_state_variants(word),
#                 })
            
#         elif category == 'Dictionary Word':
#             profile['has_dictionary_word'] = True
#             profile['search_terms'].append(word)
            
#         else:
#             # Unknown category - add to search terms
#             if word:
#                 profile['search_terms'].append(word)
    
#     # =================================================================
#     # Determine Primary Intent from Scores
#     # =================================================================
    
#     scores = {
#         'person': profile['person_score'],
#         'organization': profile['organization_score'],
#         'location': profile['location_score'],
#         'keyword': profile['keyword_score'],
#         'media': profile['media_score'],
#     }
    
#     # Find highest non-zero score
#     max_score = max(scores.values())
#     if max_score > 0:
#         profile['primary_intent'] = max(scores, key=scores.get)
#     else:
#         profile['primary_intent'] = 'general'
    
#     # =================================================================
#     # Set Preferred Document Types Based on Intent
#     # =================================================================
    
#     if profile['primary_intent'] == 'person':
#         profile['preferred_data_types'] = ['article', 'person', 'media']
#         profile['field_boosts'] = {
#             'entity_names': 15,
#             'key_facts': 10,
#             'primary_keywords': 8,
#             'semantic_keywords': 6,
#             'document_title': 5,
#         }
        
#     elif profile['primary_intent'] == 'organization':
#         profile['preferred_data_types'] = ['business', 'article', 'organization']
#         profile['field_boosts'] = {
#             'entity_names': 15,
#             'primary_keywords': 12,
#             'key_facts': 8,
#             'semantic_keywords': 6,
#             'document_title': 5,
#         }
        
#     elif profile['primary_intent'] == 'keyword':
#         profile['preferred_data_types'] = ['article', 'media']
#         profile['field_boosts'] = {
#             'primary_keywords': 15,
#             'key_facts': 10,
#             'entity_names': 8,
#             'semantic_keywords': 8,
#             'document_title': 5,
#         }
        
#     elif profile['primary_intent'] == 'media':
#         profile['preferred_data_types'] = ['media', 'article']
#         profile['field_boosts'] = {
#             'semantic_keywords': 15,
#             'primary_keywords': 12,
#             'document_title': 10,
#             'entity_names': 8,
#             'key_facts': 5,
#         }
        
#     elif profile['primary_intent'] == 'location':
#         profile['preferred_data_types'] = ['place', 'business', 'article']
#         profile['field_boosts'] = {
#             'primary_keywords': 12,
#             'entity_names': 10,
#             'key_facts': 8,
#             'semantic_keywords': 6,
#             'document_title': 5,
#         }
    
#     else:
#         # General intent - balanced weights
#         profile['preferred_data_types'] = ['article']
#         profile['field_boosts'] = {
#             'primary_keywords': 10,
#             'entity_names': 8,
#             'key_facts': 6,
#             'semantic_keywords': 6,
#             'document_title': 4,
#         }
    
#     return profile


# # =============================================================================
# # STEP 3: MAP - Convert Profile to Typesense Parameters
# # =============================================================================

# def build_typesense_params(profile: Dict, ui_filters: Dict = None, page: int = 1, per_page: int = 20) -> Dict:
#     """
#     Convert the query profile into Typesense search parameters.
    
#     Builds:
#     - filter_by: from locations
#     - query_by: from field_boosts
#     - query_by_weights: from field_boosts values
#     - q: from search_terms
#     """
#     params = {}
    
#     # =================================================================
#     # Build Query String (q)
#     # =================================================================
    
#     search_terms = profile.get('search_terms', [])
    
#     # Remove duplicates while preserving order
#     seen = set()
#     unique_terms = []
#     for term in search_terms:
#         term_lower = term.lower()
#         if term_lower not in seen:
#             seen.add(term_lower)
#             unique_terms.append(term)
    
#     params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
#     # =================================================================
#     # Build Query By Fields and Weights
#     # =================================================================
    
#     field_boosts = profile.get('field_boosts', {})
    
#     # Sort by weight descending
#     sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    
#     params['query_by'] = ','.join([f[0] for f in sorted_fields])
#     params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
#     # =================================================================
#     # Build Filter String
#     # =================================================================
    
#     filter_conditions = []
    
#     # Location filters
#     cities = profile.get('cities', [])
#     states = profile.get('states', [])
    
#     if cities:
#         city_names = [c['name'] for c in cities]
#         # Use OR for multiple cities
#         city_filters = [f"location_city:={name}" for name in city_names]
#         if len(city_filters) == 1:
#             filter_conditions.append(city_filters[0])
#         else:
#             filter_conditions.append('(' + ' || '.join(city_filters) + ')')
    
#     if states:
#         state_conditions = []
#         for state in states:
#             variants = state.get('variants', [state['name']])
#             for variant in variants:
#                 state_conditions.append(f"location_state:={variant}")
        
#         if len(state_conditions) == 1:
#             filter_conditions.append(state_conditions[0])
#         else:
#             filter_conditions.append('(' + ' || '.join(state_conditions) + ')')
    
#     # UI filters (data_type, category, schema)
#     if ui_filters:
#         if ui_filters.get('data_type'):
#             filter_conditions.append(f"document_data_type:={ui_filters['data_type']}")
#         if ui_filters.get('category'):
#             filter_conditions.append(f"document_category:={ui_filters['category']}")
#         if ui_filters.get('schema'):
#             filter_conditions.append(f"document_schema:={ui_filters['schema']}")
    
#     if filter_conditions:
#         params['filter_by'] = ' && '.join(filter_conditions)
    
#     # =================================================================
#     # Other Parameters
#     # =================================================================
    
#     params['page'] = page
#     params['per_page'] = per_page
#     params['sort_by'] = 'authority_score:desc,published_date:desc'
#     params['num_typos'] = 1
#     params['drop_tokens_threshold'] = 1  # Allow some flexibility
    
#     return params


# # =============================================================================
# # STEP 4 & 5: BUILD AND EXECUTE - Run the Typesense Query
# # =============================================================================

# def execute_search(
#     discovery: Dict,
#     ui_filters: Dict = None,
#     page: int = 1,
#     per_page: int = 20,
#     client = None,
#     collection_name: str = 'documents',
#     embedding: List[float] = None,
#     verbose: bool = False,
# ) -> Dict:
#     """
#     Execute a complete search using Word Discovery output.
    
#     Steps:
#     1. Parse discovery output
#     2. Build query profile (analyze all metadata)
#     3. Map profile to Typesense parameters
#     4. Execute search (with optional vector query)
#     5. Return results with profile info
#     """
    
#     # Step 1: Parse
#     parsed = parse_discovery_output(discovery)
    
#     # Step 2: Profile
#     profile = build_query_profile(parsed)
    
#     # Step 3: Map to Typesense params
#     params = build_typesense_params(profile, ui_filters, page, per_page)
    
#     # Add vector query if embedding provided
#     if embedding:
#         embedding_str = ','.join(str(x) for x in embedding)
#         params['vector_query'] = f"embedding:([{embedding_str}], k:{per_page * 5}, alpha:0.5)"
    
#     # Verbose output
#     if verbose:
#         print_profile(profile)
#         print_params(params)
    
#     # Build response
#     response = {
#         'query': parsed['query'],
#         'corrected_query': parsed['corrected_query'],
#         'profile': profile,
#         'params': params,
#         'hits': [],
#         'found': 0,
#     }
    
#     # Step 4 & 5: Execute if client provided
#     if client:
#         try:
#             result = client.collections[collection_name].documents.search(params)
#             response['hits'] = result.get('hits', [])
#             response['found'] = result.get('found', 0)
#             response['typesense_response'] = result
#         except Exception as e:
#             response['error'] = str(e)
#             print(f"❌ Typesense error: {e}")
    
#     return response


# # =============================================================================
# # PRETTY PRINT HELPERS
# # =============================================================================

# def print_profile(profile: Dict) -> None:
#     """Pretty print the query profile."""
#     print("\n" + "=" * 70)
#     print("📊 QUERY PROFILE")
#     print("=" * 70)
    
#     print(f"\n🎯 Primary Intent: {profile['primary_intent'].upper()}")
    
#     print(f"\n📈 Scores:")
#     print(f"   Person: {profile['person_score']}")
#     print(f"   Organization: {profile['organization_score']}")
#     print(f"   Location: {profile['location_score']}")
#     print(f"   Keyword: {profile['keyword_score']}")
#     print(f"   Media: {profile['media_score']}")
    
#     if profile['persons']:
#         print(f"\n👤 Persons:")
#         for p in profile['persons']:
#             print(f"   • {p.get('phrase') or p.get('word')} (rank: {p['rank']})")
    
#     if profile['organizations']:
#         print(f"\n🏢 Organizations:")
#         for o in profile['organizations']:
#             print(f"   • {o.get('phrase') or o.get('word')} (rank: {o['rank']})")
    
#     if profile['cities'] or profile['states']:
#         print(f"\n📍 Locations:")
#         for c in profile['cities']:
#             print(f"   • City: {c['name']} (rank: {c['rank']})")
#         for s in profile['states']:
#             print(f"   • State: {s['name']} → {s['variants']} (rank: {s['rank']})")
    
#     if profile['keywords']:
#         print(f"\n🏷️ Keywords:")
#         for k in profile['keywords']:
#             print(f"   • {k.get('phrase') or k.get('word')} (rank: {k['rank']})")
    
#     if profile['search_terms']:
#         print(f"\n🔍 Search Terms: {profile['search_terms']}")
    
#     print(f"\n📁 Preferred Doc Types: {profile['preferred_data_types']}")
    
#     print(f"\n⚖️ Field Boosts:")
#     for field, weight in sorted(profile['field_boosts'].items(), key=lambda x: -x[1]):
#         print(f"   • {field}: {weight}")


# def print_params(params: Dict) -> None:
#     """Pretty print Typesense params."""
#     print("\n" + "=" * 70)
#     print("🔎 TYPESENSE PARAMETERS")
#     print("=" * 70)
#     print(json.dumps(params, indent=2))


# def print_results(response: Dict) -> None:
#     """Pretty print search results."""
#     print("\n" + "=" * 70)
#     print("📄 SEARCH RESULTS")
#     print("=" * 70)
    
#     found = response.get('found', 0)
#     hits = response.get('hits', [])
    
#     print(f"\n🎯 Found: {found} documents")
#     print(f"📑 Showing: {len(hits)} results\n")
    
#     for i, hit in enumerate(hits, 1):
#         doc = hit.get('document', {})
        
#         title = doc.get('document_title', 'No title')[:60]
#         url = doc.get('document_url', '')[:50]
#         category = doc.get('document_category', 'unknown')
#         data_type = doc.get('document_data_type', 'unknown')
#         authority = doc.get('authority_score', 0)
        
#         vector_dist = hit.get('vector_distance')
#         text_match = hit.get('text_match', 0)
        
#         print(f"  {i}. {title}")
#         print(f"     📁 {data_type} | {category}")
#         print(f"     🔗 {url}...")
#         print(f"     📊 Authority: {authority} | Text Match: {text_match}", end="")
#         if vector_dist is not None:
#             print(f" | Vector: {vector_dist:.4f}")
#         else:
#             print()
        
#         city = doc.get('location_city')
#         state = doc.get('location_state')
#         if city or state:
#             loc_str = ', '.join(filter(None, [city, state]))
#             print(f"     📍 {loc_str}")
        
#         print()


# # =============================================================================
# # LIVE TEST FUNCTION
# # =============================================================================

# def run_live_test(query: str, verbose: bool = True):
#     """
#     Run a LIVE test:
#     1. Call word_discovery_v2.py
#     2. Build profile from metadata
#     3. Execute Typesense search
#     4. Display results
#     """
#     print("\n" + "=" * 70)
#     print(f"🚀 LIVE TEST: '{query}'")
#     print("=" * 70)
    
#     # Step 1: Run Word Discovery
#     print("\n📖 STEP 1: Running Word Discovery v2...")
    
#     try:
#         from word_discovery_fulltest import WordDiscovery
#         wd = WordDiscovery(verbose=verbose)
#         discovery = wd.process(query)
#         print(f"   ✅ Word Discovery complete: {discovery['stats']}")
#     except ImportError as e:
#         print(f"   ❌ Could not import word_discovery_v2: {e}")
#         return None
#     except Exception as e:
#         print(f"   ❌ Word Discovery error: {e}")
#         import traceback
#         traceback.print_exc()
#         return None
    
#     # Step 2: Connect to Typesense
#     print("\n🔌 STEP 2: Connecting to Typesense...")
    
#     try:
#         client = typesense.Client({
#             'api_key': config('TYPESENSE_API_KEY'),
#             'nodes': [{
#                 'host': config('TYPESENSE_HOST'),
#                 'port': config('TYPESENSE_PORT'),
#                 'protocol': config('TYPESENSE_PROTOCOL')
#             }],
#             'connection_timeout_seconds': 5
#         })
#         client.collections['documents'].retrieve()
#         print("   ✅ Connected to Typesense")
#     except Exception as e:
#         print(f"   ❌ Typesense connection error: {e}")
#         return None
    
#     # Step 3: Execute Search
#     print("\n🔍 STEP 3: Executing search...")
    
#     response = execute_search(
#         discovery=discovery,
#         ui_filters=None,
#         page=1,
#         per_page=10,
#         client=client,
#         collection_name='documents',
#         embedding=None,  # Add embedding support later
#         verbose=True,
#     )
    
#     # Step 4: Display Results
#     print_results(response)
    
#     # Summary
#     print("=" * 70)
#     print("📊 SUMMARY")
#     print("=" * 70)
#     profile = response.get('profile', {})
#     print(f"  Query: '{query}'")
#     print(f"  Primary Intent: {profile.get('primary_intent', 'unknown')}")
#     print(f"  Locations: {len(profile.get('cities', []))} cities, {len(profile.get('states', []))} states")
#     print(f"  Filter Applied: {response.get('params', {}).get('filter_by', 'none')}")
#     print(f"  Total Found: {response.get('found', 0)}")
    
#     return response


# # =============================================================================
# # MAIN ENTRY POINT
# # =============================================================================

# if __name__ == "__main__":
#     import sys
    
#     if len(sys.argv) > 1:
#         query = ' '.join(sys.argv[1:])
#         run_live_test(query, verbose=True)
#     else:
#         print("\n" + "=" * 70)
#         print("TYPESENSE DISCOVERY BRIDGE - Test Runner")
#         print("=" * 70)
#         print("\nUsage:")
#         print("  python typesense_discovery_bridge.py \"your search query\"")
#         print("\nExamples:")
#         print("  python typesense_discovery_bridge.py \"black owned restaurants in atlanta georgia\"")
#         print("  python typesense_discovery_bridge.py \"martin luther king speech\"")
#         print("  python typesense_discovery_bridge.py \"martin luther king foundation atlanta\"")
#         print("  python typesense_discovery_bridge.py \"hbcu football teams in alabama\"")
#         print("\nThis will:")
#         print("  1. Call word_discovery_v2.py to process your query")
#         print("  2. Build a PROFILE from all metadata (scores, categories, etc.)")
#         print("  3. Map profile to Typesense parameters")
#         print("  4. Execute search and display ranked results")


"""
typesense_discovery_bridge.py
=============================
Complete search bridge between Word Discovery v2 and Typesense.

This file handles EVERYTHING:
- Word Discovery v2 integration
- Query profile building (intent, locations, field boosts)
- Embedding generation (via embedding_client.py)
- Result caching (self-contained)
- Stage 1: Graph Filter (candidate generation)
- Stage 2: Semantic Rerank (vector-based ranking)
- Facet counting from cache
- Pagination from cache
- Full document fetching
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
            return wd.process(query)
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
# QUERY PROFILE BUILDING
# ============================================================================

def build_query_profile(discovery: Dict) -> Dict:
    """
    Analyze ALL metadata from Word Discovery to understand user intent.
    
    Returns profile with:
    - Primary intent (person, organization, location, keyword, media)
    - Scores for each intent type
    - Cities and states for filters
    - Search terms for query
    - Field boosts for query_by_weights
    """
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
        'search_terms': [],
        
        'primary_intent': 'general',
        'preferred_data_types': [],
        
        'field_boosts': {
            'primary_keywords': 10,
            'entity_names': 8,
            'semantic_keywords': 6,
            'key_facts': 4,
            'document_title': 3,
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
                # Fallback: check if word is a known location
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
        
        # Rule: If BOTH terms are locations → trust term categories
        #       Otherwise → trust n-gram category
        both_terms_are_locations = has_city_term and has_state_term
        ngram_is_location = ngram_category in LOCATION_CATEGORIES
        
        if both_terms_are_locations or ngram_is_location:
            profile['has_location'] = True
            
            for tc in term_categories:
                if is_city_category(tc['category']):
                    city_name = tc['word'].title()
                    if city_name not in [c['name'] for c in profile['cities']]:
                        profile['cities'].append({
                            'name': city_name,
                            'rank': tc['rank'],
                        })
                    profile['location_score'] += tc['rank']
                    
                elif is_state_category(tc['category']):
                    state_name = tc['word'].title()
                    if state_name not in [s['name'] for s in profile['states']]:
                        profile['states'].append({
                            'name': state_name,
                            'rank': tc['rank'],
                            'variants': get_state_variants(tc['word']),
                        })
                    profile['location_score'] += tc['rank']
        
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
            # Unknown category - add to search terms
            profile['search_terms'].append(phrase)
    
    # =================================================================
    # Process Individual Terms (not in n-grams)
    # =================================================================
    
    for term in terms:
        position = term.get('position', 0)
        word = term.get('word', '')
        display = term.get('display', word)
        category = term.get('category', '')
        rank = _parse_rank(term.get('rank', 0))
        is_stopword = term.get('is_stopword', False)
        part_of_ngram = term.get('part_of_ngram', False) or (position in ngram_positions)
        
        if is_stopword or part_of_ngram:
            continue
        
        if category in PERSON_CATEGORIES:
            profile['has_person'] = True
            profile['person_score'] += rank
            profile['persons'].append({'word': word, 'display': display, 'rank': rank})
            profile['search_terms'].append(word)
            
        elif category in ORGANIZATION_CATEGORIES:
            profile['has_organization'] = True
            profile['organization_score'] += rank
            profile['organizations'].append({'word': word, 'display': display, 'rank': rank})
            profile['search_terms'].append(word)
            
        elif category in KEYWORD_CATEGORIES:
            profile['has_keyword'] = True
            profile['keyword_score'] += rank
            profile['keywords'].append({'word': word, 'display': display, 'rank': rank})
            profile['search_terms'].append(word)
            
        elif category in MEDIA_CATEGORIES:
            profile['has_media'] = True
            profile['media_score'] += rank
            profile['search_terms'].append(word)
            
        elif is_city_category(category):
            profile['has_location'] = True
            profile['location_score'] += rank
            city_name = display or word.title()
            if city_name not in [c['name'] for c in profile['cities']]:
                profile['cities'].append({'name': city_name, 'rank': rank})
            
        elif is_state_category(category):
            profile['has_location'] = True
            profile['location_score'] += rank
            state_name = display or word.title()
            if state_name not in [s['name'] for s in profile['states']]:
                profile['states'].append({
                    'name': state_name,
                    'rank': rank,
                    'variants': get_state_variants(word),
                })
            
        elif category == 'Dictionary Word':
            profile['search_terms'].append(word)
            
        else:
            if word:
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
    # Set Field Boosts Based on Intent
    # =================================================================
    
    if profile['primary_intent'] == 'person':
        profile['preferred_data_types'] = ['article', 'person', 'media']
        profile['field_boosts'] = {
            'entity_names': 15,
            'key_facts': 10,
            'primary_keywords': 8,
            'semantic_keywords': 6,
            'document_title': 5,
        }
        
    elif profile['primary_intent'] == 'organization':
        profile['preferred_data_types'] = ['business', 'article', 'organization']
        profile['field_boosts'] = {
            'entity_names': 15,
            'primary_keywords': 12,
            'key_facts': 8,
            'semantic_keywords': 6,
            'document_title': 5,
        }
        
    elif profile['primary_intent'] == 'keyword':
        profile['preferred_data_types'] = ['article', 'media']
        profile['field_boosts'] = {
            'primary_keywords': 15,
            'key_facts': 10,
            'entity_names': 8,
            'semantic_keywords': 8,
            'document_title': 5,
        }
        
    elif profile['primary_intent'] == 'media':
        profile['preferred_data_types'] = ['media', 'article']
        profile['field_boosts'] = {
            'semantic_keywords': 15,
            'primary_keywords': 12,
            'document_title': 10,
            'entity_names': 8,
            'key_facts': 5,
        }
        
    elif profile['primary_intent'] == 'location':
        profile['preferred_data_types'] = ['place', 'business', 'article']
        profile['field_boosts'] = {
            'primary_keywords': 12,
            'entity_names': 10,
            'key_facts': 8,
            'semantic_keywords': 6,
            'document_title': 5,
        }
    
    return profile


# ============================================================================
# TYPESENSE PARAMETER BUILDING
# ============================================================================

def build_typesense_params(profile: Dict, ui_filters: Dict = None) -> Dict:
    """
    Convert query profile into Typesense search parameters.
    
    Returns:
        Dict with q, query_by, query_by_weights, filter_by
    """
    params = {}
    
    # Build query string
    search_terms = profile.get('search_terms', [])
    seen = set()
    unique_terms = []
    for term in search_terms:
        term_lower = term.lower()
        if term_lower not in seen:
            seen.add(term_lower)
            unique_terms.append(term)
    
    params['q'] = ' '.join(unique_terms) if unique_terms else '*'
    
    # Build query_by and weights
    field_boosts = profile.get('field_boosts', {})
    sorted_fields = sorted(field_boosts.items(), key=lambda x: x[1], reverse=True)
    params['query_by'] = ','.join([f[0] for f in sorted_fields])
    params['query_by_weights'] = ','.join([str(f[1]) for f in sorted_fields])
    
    # Build filter string
    filter_conditions = []
    
    # Location filters
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


def build_filter_string_without_data_type(profile: Dict) -> str:
    """Build filter string for locations only (no data_type for facet counting)."""
    filter_conditions = []
    
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
    
    return ' && '.join(filter_conditions) if filter_conditions else ''


# ============================================================================
# STAGE 1: GRAPH FILTER - Candidate Generation
# ============================================================================

def fetch_candidate_ids(
    search_query: str,
    profile: Dict,
    max_results: int = MAX_CACHED_RESULTS
) -> List[Dict]:
    """
    Stage 1: Graph Filter - Candidate Generation
    
    Uses keyword fields for FILTERING (fast inverted index lookup).
    Does NOT filter by data_type so we get ALL types for tab counts.
    """
    params = build_typesense_params(profile)
    
    # Override filter to exclude data_type (for accurate facet counts)
    filter_str = build_filter_string_without_data_type(profile)
    
    PAGE_SIZE = 250
    all_results = []
    current_page = 1
    max_pages = (max_results // PAGE_SIZE) + 1
    
    print(f"🔍 Stage 1 Query: '{params.get('q', search_query)}'")
    print(f"   Fields: {params.get('query_by', '')}")
    print(f"   Weights: {params.get('query_by_weights', '')}")
    if filter_str:
        print(f"   Location Filters: {filter_str}")
    
    while len(all_results) < max_results and current_page <= max_pages:
        search_params = {
            'q': params.get('q', search_query),
            'query_by': params.get('query_by', 'primary_keywords,entity_names,semantic_keywords,key_facts,document_title'),
            'query_by_weights': params.get('query_by_weights', '10,8,6,4,3'),
            'per_page': PAGE_SIZE,
            'page': current_page,
            'include_fields': 'document_uuid,document_data_type,document_category,document_schema,authority_score',
            'num_typos': 1,
            'drop_tokens_threshold': 0,
            'sort_by': 'authority_score:desc,published_date:desc',
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
# STAGE 2: SEMANTIC RERANK - Vector-Based Ranking
# ============================================================================

def semantic_rerank_candidates(
    candidate_ids: List[str],
    query_embedding: List[float],
    max_to_rerank: int = 500
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
    reranked_results: List[Dict]
) -> List[Dict]:
    """Apply semantic ranking to cached results."""
    if not reranked_results:
        return cached_results
    
    rank_lookup = {
        r['id']: {
            'semantic_rank': r['semantic_rank'],
            'vector_distance': r.get('vector_distance', 1.0)
        }
        for r in reranked_results
    }
    
    for item in cached_results:
        item_id = item.get('id')
        if item_id in rank_lookup:
            item['semantic_rank'] = rank_lookup[item_id]['semantic_rank']
            item['vector_distance'] = rank_lookup[item_id]['vector_distance']
        else:
            item['semantic_rank'] = 999999
            item['vector_distance'] = 1.0
    
    cached_results.sort(key=lambda x: x.get('semantic_rank', 999999))
    
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
    print(f"📍 RAW: '{doc.get('document_title', '')[:35]}' geopoint={doc.get('location_geopoint')} coords={doc.get('location_coordinates')} addr={doc.get('location_address', '')[:30]}")
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
        'score': 0.5,  # Placeholder
        'related_sources': []
    }


# ============================================================================
# INTENT DETECTION (for compatibility)
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
# MAIN ENTRY POINT: execute_full_search
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
#     Main entry point for search.
    
#     Returns same structure as old execute_full_search() for views.py compatibility.
    
#     alt_mode:
#         'n' = KEYWORD PATH - no semantic reranking
#         'y' = SEMANTIC PATH - full staged retrieval
#     """
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
#     # KEYWORD PATH (alt_mode='n')
#     # =========================================================================
    
#     is_keyword_path = (alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')
    
#     if is_keyword_path:
#         print(f"⚡ KEYWORD PATH: '{query}'")
        
#         intent = detect_query_intent(query, pos_tags)
        
#         # Simple profile for keyword path
#         profile = {
#             'search_terms': query.split(),
#             'cities': [],
#             'states': [],
#             'primary_intent': intent,
#             'field_boosts': {
#                 'primary_keywords': 10,
#                 'entity_names': 8,
#                 'semantic_keywords': 6,
#                 'key_facts': 4,
#                 'document_title': 3,
#             },
#         }
        
#         cache_key = _generate_cache_key(query, 'keyword', [], [])
#         cached_data = _get_cached_results(cache_key)
        
#         if cached_data:
#             print(f"✅ Cache HIT: {len(cached_data)} candidates")
#             all_results = cached_data
#             times['cache'] = 'hit'
#         else:
#             print(f"❌ Cache MISS: Running Stage 1...")
#             t1 = time.time()
#             all_results = fetch_candidate_ids(query, profile)
#             times['stage1'] = round((time.time() - t1) * 1000, 2)
            
#             if all_results:
#                 _set_cached_results(cache_key, all_results)
#             times['cache'] = 'miss'
        
#         # Facets from cache
#         all_facets = count_facets_from_cache(all_results)
#         facet_total = len(all_results)
        
#         # Filter by UI filters
#         filtered_results = filter_cached_results(
#             all_results,
#             data_type=active_data_type,
#             category=active_category,
#             schema=active_schema
#         )
        
#         # Paginate
#         page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
        
#         # Fetch full documents
#         t2 = time.time()
#         page_ids = [item['id'] for item in page_items]
#         results = fetch_full_documents(page_ids, query)
#         times['fetch_docs'] = round((time.time() - t2) * 1000, 2)
        
#         times['total'] = round((time.time() - t0) * 1000, 2)
        
#         print(f"⏱️ TIMING: {times}")
#         print(f"🔍 KEYWORD PATH | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
        
#         return {
#             'query': query,
#             'corrected_query': query,
#             'intent': intent,
#             'results': results,
#             'total': total_filtered,
#             'facet_total': facet_total,
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
#             'data_type_facets': all_facets.get('data_type', []),
#             'category_facets': all_facets.get('category', []),
#             'schema_facets': all_facets.get('schema', []),
#             'related_searches': [],
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
    
#     # =========================================================================
#     # SEMANTIC PATH (alt_mode='y')
#     # =========================================================================
    
#     print(f"🔬 SEMANTIC PATH: '{query}'")
    
#     # Run word discovery and embedding in parallel
#     t1 = time.time()
#     discovery, query_embedding = run_parallel_prep(query, skip_embedding=skip_embedding)
#     times['parallel_prep'] = round((time.time() - t1) * 1000, 2)
    
#     corrected_query = discovery.get('corrected_query', query)
#     semantic_enabled = query_embedding is not None
    
#     # Build profile from discovery
#     t2 = time.time()
#     profile = build_query_profile(discovery)
#     times['build_profile'] = round((time.time() - t2) * 1000, 2)
    
#     intent = profile.get('primary_intent', 'general')
    
#     print(f"   Intent: {intent}")
#     print(f"   Cities: {[c['name'] for c in profile.get('cities', [])]}")
#     print(f"   States: {[s['name'] for s in profile.get('states', [])]}")
#     print(f"   Search Terms: {profile.get('search_terms', [])}")
    
#     # Generate cache key
#     city_names = [c['name'] for c in profile.get('cities', [])]
#     state_names = [s['name'] for s in profile.get('states', [])]
#     cache_key = _generate_cache_key(corrected_query, 'semantic', city_names, state_names)
    
#     # Check cache
#     cached_data = _get_cached_results(cache_key)
    
#     if cached_data:
#         print(f"✅ Cache HIT: {len(cached_data)} candidates")
#         all_results = cached_data
#         times['cache'] = 'hit'
#     else:
#         print(f"❌ Cache MISS: Running Stage 1...")
#         t3 = time.time()
#         all_results = fetch_candidate_ids(corrected_query, profile)
#         times['stage1'] = round((time.time() - t3) * 1000, 2)
        
#         if all_results:
#             _set_cached_results(cache_key, all_results)
#         times['cache'] = 'miss'
    
#     # Facets from cache
#     all_facets = count_facets_from_cache(all_results)
#     facet_total = len(all_results)
    
#     print(f"📊 Facets: {[(f['value'], f['count']) for f in all_facets.get('data_type', [])]}")
    
#     # Filter by UI filters
#     filtered_results = filter_cached_results(
#         all_results,
#         data_type=active_data_type,
#         category=active_category,
#         schema=active_schema
#     )
    
#     # Stage 2: Semantic Rerank
#     if semantic_enabled and filtered_results:
#         t4 = time.time()
#         candidate_ids = [item['id'] for item in filtered_results]
#         reranked = semantic_rerank_candidates(candidate_ids, query_embedding, max_to_rerank=500)
#         filtered_results = apply_semantic_ranking(filtered_results, reranked)
#         times['stage2'] = round((time.time() - t4) * 1000, 2)
#     else:
#         print(f"⚠️ Skipping Stage 2: semantic={semantic_enabled}, filtered={len(filtered_results)}")
    
#     # Paginate
#     page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
#     # Fetch full documents
#     t5 = time.time()
#     page_ids = [item['id'] for item in page_items]
#     results = fetch_full_documents(page_ids, query)
#     times['fetch_docs'] = round((time.time() - t5) * 1000, 2)
    
#     times['total'] = round((time.time() - t0) * 1000, 2)
    
#     strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
#     print(f"⏱️ TIMING: {times}")
#     print(f"🔍 {strategy.upper()} | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
#     # Extract valid/unknown terms
#     valid_terms = profile.get('search_terms', [])
#     unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
#     return {
#         'query': query,
#         'corrected_query': corrected_query,
#         'intent': intent,
#         'results': results,
#         'total': total_filtered,
#         'facet_total': facet_total,
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
#         'data_type_facets': all_facets.get('data_type', []),
#         'category_facets': all_facets.get('category', []),
#         'schema_facets': all_facets.get('schema', []),
#         'facets': all_facets,
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
#             'graph_filters': [],
#             'graph_locations': [
#                 {'field': 'location_city', 'values': city_names},
#                 {'field': 'location_state', 'values': state_names},
#             ] if city_names or state_names else [],
#             'graph_sort': None,
#         },
#         'profile': profile,  # Include profile for debugging
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
    Main entry point for search.
    
    Returns same structure as old execute_full_search() for views.py compatibility.
    
    alt_mode:
        'n' = KEYWORD PATH - no semantic reranking
        'y' = SEMANTIC PATH - full staged retrieval
    """
    times = {}
    t0 = time.time()
    print(f"🔴 DEBUG: alt_mode='{alt_mode}' type={type(alt_mode)} search_source='{search_source}'")
    print(f"🔴 DEBUG: is_keyword_path would be: {(alt_mode == 'n') or search_source in ('dropdown', 'keyword', 'suggestion', 'autocomplete')}")
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
                # Added this for the location based search and mapping
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
    
        # Run intent detection (logging only - no behavior changes)
    if INTENT_DETECT_AVAILABLE:
        try:
            discovery = detect_intent(discovery)
            signals = discovery.get('signals', {})
            print(f"   🎯 Intent signals: mode={signals.get('query_mode')}, "
                  f"local={signals.get('is_local_search')}, "
                  f"location={signals.get('has_location')}, "
                  f"service={signals.get('service_words')}, "
                  f"question={signals.get('has_question_word')}, "
                  f"temporal={signals.get('temporal_direction')}, "
                  f"domains={signals.get('domains_detected', [])[:3]}")
        except Exception as e:
            print(f"   ⚠️ intent_detect error: {e}")

    corrected_query = discovery.get('corrected_query', query)
    semantic_enabled = query_embedding is not None
    
    # Build profile from discovery
    t2 = time.time()
    profile = build_query_profile(discovery)
    times['build_profile'] = round((time.time() - t2) * 1000, 2)
    
    # =========================================================================
    # FIX: Apply corrections to search terms so Typesense gets corrected words
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
    # =========================================================================
    
    intent = profile.get('primary_intent', 'general')
    
    print(f"   Intent: {intent}")
    print(f"   Cities: {[c['name'] for c in profile.get('cities', [])]}")
    print(f"   States: {[s['name'] for s in profile.get('states', [])]}")
    print(f"   Search Terms: {profile.get('search_terms', [])}")
    
    # Generate cache key
    city_names = [c['name'] for c in profile.get('cities', [])]
    state_names = [s['name'] for s in profile.get('states', [])]
    cache_key = _generate_cache_key(corrected_query, 'semantic', city_names, state_names)
    
    # Check cache
    cached_data = _get_cached_results(cache_key)
    
    if cached_data:
        print(f"✅ Cache HIT: {len(cached_data)} candidates")
        all_results = cached_data
        times['cache'] = 'hit'
    else:
        print(f"❌ Cache MISS: Running Stage 1...")
        t3 = time.time()
        all_results = fetch_candidate_ids(corrected_query, profile)
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
    
    # Stage 2: Semantic Rerank
    if semantic_enabled and filtered_results:
        t4 = time.time()
        candidate_ids = [item['id'] for item in filtered_results]
        reranked = semantic_rerank_candidates(candidate_ids, query_embedding, max_to_rerank=500)
        filtered_results = apply_semantic_ranking(filtered_results, reranked)
        times['stage2'] = round((time.time() - t4) * 1000, 2)
    else:
        print(f"⚠️ Skipping Stage 2: semantic={semantic_enabled}, filtered={len(filtered_results)}")
    
    # Paginate
    page_items, total_filtered = paginate_cached_results(filtered_results, page, per_page)
    
    # Fetch full documents
    t5 = time.time()
    page_ids = [item['id'] for item in page_items]
    results = fetch_full_documents(page_ids, query)
    times['fetch_docs'] = round((time.time() - t5) * 1000, 2)

    # Store query embedding for popular queries
    if query_embedding:
        try:
            store_query_embedding(corrected_query, query_embedding, result_count=facet_total)
        except Exception as e:
            print(f"⚠️ store_query_embedding error: {e}")
    
    times['total'] = round((time.time() - t0) * 1000, 2)
    
    strategy = 'staged_semantic' if semantic_enabled else 'keyword_fallback'
    
    print(f"⏱️ TIMING: {times}")
    print(f"🔍 {strategy.upper()} | Total: {facet_total} | Filtered: {total_filtered} | Page: {len(results)}")
    
    # Extract valid/unknown terms
    valid_terms = profile.get('search_terms', [])
    unknown_terms = [t['word'] for t in discovery.get('terms', []) if t.get('status') == 'unknown']
    
    return {
        'query': query,
        'corrected_query': corrected_query,
        'intent': intent,
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

             # Added this for the location based search and mapping

            'is_local_search': discovery.get('signals', {}).get('is_local_search', False),
            'local_search_strength': discovery.get('signals', {}).get('local_search_strength', 'none'),

            'graph_filters': [],
            'graph_locations': [
                {'field': 'location_city', 'values': city_names},
                {'field': 'location_state', 'values': state_names},
            ] if city_names or state_names else [],
            'graph_sort': None,
        },
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
    
    print(f"\n📄 Results ({len(result['results'])}):")
    for i, r in enumerate(result['results'][:5], 1):
        print(f"   {i}. {r['title'][:60]}")
        print(f"      📍 {r['location'].get('city', '')}, {r['location'].get('state', '')}")
        print(f"      🔗 {r['url'][:50]}...")
    
    print(f"\n⏱️ Timings: {result['timings']}")