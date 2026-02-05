"""
intent_detect.py
Pure fact extraction from word discovery output.

PURPOSE:
    Extract and report ALL linguistic signals from the query.
    Does NOT make decisions - only presents facts for Typesense to interpret.

SIGNALS EXTRACTED:
    - Question signals (who, what, when, where, why, how)
    - Temporal signals (first, last, oldest, newest)
    - Proximity signals (near, in, at, around)
    - Superlative signals (best, top, most, biggest)
    - Quantity signals (plural nouns, list requests)
    - Entity signals (person, place, organization detected)
    - Service signals (restaurants, designers, doctors)
    - Action signals (find, show, get, search)
    - Comparison signals (vs, versus, compare, difference)
    - Definition signals (what is, define, meaning)

PERFORMANCE:
    All lookups are O(1) using frozensets and dicts.
    No loops over external data.
    Single pass through terms list.

USAGE:
    from intent_detect import detect_intent
    
    result = process_query_optimized(query)
    result = detect_intent(result)
    
    # Access signals
    signals = result['signals']
    if signals['has_question_word']:
        # Query contains who/what/when/where/why/how
    if signals['has_proximity']:
        # Query contains near/in/around
"""

from typing import Dict, Any, List, Optional, Set, Tuple


# =============================================================================
# SIGNAL WORD SETS (O(1) lookup)
# =============================================================================

# Question words - signal user is asking a question
QUESTION_WORDS = frozenset({
    'who', "who's", 'whom', 'whose',
    'what', "what's", 'whatever',
    'when', "when's",
    'where', "where's", 'wherever',
    'why', "why's",
    'how', "how's",
    'which', 'whichever',
})

# Question starters - verbs that start questions
QUESTION_STARTERS = frozenset({
    'is', 'are', 'was', 'were',
    'do', 'does', 'did',
    'can', 'could',
    'will', 'would',
    'should', 'shall',
    'has', 'have', 'had',
    'may', 'might', 'must',
})

# Temporal words - signal time-based filtering/sorting
TEMPORAL_OLDEST = frozenset({
    'first', 'oldest', 'earliest', 'original', 'initial',
    'founding', 'pioneer', 'pioneering', 'inaugural',
})

TEMPORAL_NEWEST = frozenset({
    'last', 'latest', 'newest', 'recent', 'current',
    'modern', 'contemporary', 'today', 'now',
})

TEMPORAL_ALL = TEMPORAL_OLDEST | TEMPORAL_NEWEST

# Proximity words - signal location-based search
PROXIMITY_WORDS = frozenset({
    'near', 'nearby', 'around', 'close',
    'in', 'at', 'on', 'within',
    'local', 'locally',
})

# Location prepositions - words before location names
LOCATION_PREPOSITIONS = frozenset({
    'in', 'at', 'near', 'around', 'from', 'to',
    'within', 'outside', 'inside',
})

# Superlative words - signal ranking/best-of queries
SUPERLATIVE_WORDS = frozenset({
    'best', 'top', 'most', 'greatest', 'finest',
    'worst', 'least',
    'biggest', 'largest', 'smallest',
    'highest', 'lowest',
    'richest', 'poorest',
    'fastest', 'slowest',
    'strongest', 'weakest',
    'famous', 'popular', 'renowned',
})

# Quantity/list words - signal multiple results wanted
QUANTITY_WORDS = frozenset({
    'list', 'lists',
    'all', 'every', 'any', 'some',
    'many', 'few', 'several', 'multiple',
    'examples', 'options', 'choices',
    'types', 'kinds', 'varieties',
    'recommendations', 'suggestions',
})

# Action words - signal user intent
ACTION_FIND = frozenset({
    'find', 'search', 'look', 'looking', 'seek', 'seeking',
    'locate', 'discover',
})

ACTION_SHOW = frozenset({
    'show', 'display', 'give', 'tell', 'list',
    'get', 'fetch', 'bring',
})

ACTION_LEARN = frozenset({
    'learn', 'understand', 'explain', 'know',
    'teach', 'study',
})

ACTION_ALL = ACTION_FIND | ACTION_SHOW | ACTION_LEARN

# Definition signals - signal user wants explanation
DEFINITION_PATTERNS = frozenset({
    'define', 'definition', 'meaning',
    'means', 'mean',
})

# Comparison words - signal comparison query
COMPARISON_WORDS = frozenset({
    'vs', 'vs.', 'versus', 'against',
    'compare', 'compared', 'comparing', 'comparison',
    'difference', 'differences', 'different',
    'between', 'or', 'better',
})

# Service/business types - signal local search
SERVICE_WORDS = frozenset({
    # Food & Dining
    'restaurant', 'restaurants', 'cafe', 'cafes', 'coffee',
    'bar', 'bars', 'club', 'clubs', 'lounge', 'lounges',
    'bakery', 'bakeries', 'diner', 'diners',
    'pizzeria', 'pizzerias', 'steakhouse', 'steakhouses',
    'food', 'foods', 'dining', 'eatery', 'eateries',
    
    # Professional Services
    'lawyer', 'lawyers', 'attorney', 'attorneys',
    'doctor', 'doctors', 'physician', 'physicians',
    'dentist', 'dentists', 'therapist', 'therapists',
    'accountant', 'accountants', 'consultant', 'consultants',
    'designer', 'designers', 'developer', 'developers',
    'architect', 'architects', 'engineer', 'engineers',
    'photographer', 'photographers', 'videographer', 'videographers',
    'plumber', 'plumbers', 'electrician', 'electricians',
    'mechanic', 'mechanics', 'contractor', 'contractors',
    
    # Retail & Shopping
    'store', 'stores', 'shop', 'shops', 'boutique', 'boutiques',
    'market', 'markets', 'mall', 'malls',
    'salon', 'salons', 'spa', 'spas', 'barbershop', 'barbershops',
    
    # Health & Fitness
    'gym', 'gyms', 'fitness', 'yoga', 'studio', 'studios',
    'clinic', 'clinics', 'hospital', 'hospitals',
    'pharmacy', 'pharmacies',
    
    # Education
    'school', 'schools', 'college', 'colleges',
    'university', 'universities', 'tutor', 'tutors',
    
    # Entertainment
    'theater', 'theaters', 'theatre', 'theatres',
    'cinema', 'cinemas', 'museum', 'museums',
    'gallery', 'galleries', 'venue', 'venues',
    
    # Lodging
    'hotel', 'hotels', 'motel', 'motels',
    'inn', 'inns', 'resort', 'resorts',
    'airbnb', 'hostel', 'hostels',
    
    # Other Services
    'bank', 'banks', 'atm', 'atms',
    'gas', 'station', 'stations',
    'parking', 'garage', 'garages',
    'church', 'churches', 'mosque', 'mosques', 'temple', 'temples',
})

# Role/title words - signal looking for a specific person by role
ROLE_WORDS = frozenset({
    # Leadership
    'president', 'vice-president', 'vp',
    'ceo', 'cfo', 'cto', 'coo', 'cmo',
    'chairman', 'chairwoman', 'chairperson', 'chair',
    'director', 'directors',
    'manager', 'managers',
    'leader', 'leaders',
    'head', 'chief',
    'boss', 'executive', 'executives',
    
    # Founders & Creators
    'founder', 'founders', 'co-founder',
    'creator', 'creators',
    'inventor', 'inventors',
    'pioneer', 'pioneers',
    'originator',
    
    # Government
    'mayor', 'governor', 'senator', 'representative',
    'congressman', 'congresswoman',
    'president', 'minister', 'ambassador',
    'judge', 'justice',
    
    # Education
    'principal', 'dean', 'provost', 'chancellor',
    'professor', 'teacher', 'instructor',
    'superintendent',
    
    # Religion
    'pastor', 'priest', 'minister', 'rabbi', 'imam',
    'bishop', 'pope', 'reverend',
    
    # Arts & Entertainment
    'author', 'writer', 'poet', 'playwright',
    'singer', 'musician', 'artist', 'performer',
    'actor', 'actress', 'director',
    'producer', 'composer', 'conductor',
    'chef', 'photographer',
    
    # Sports
    'coach', 'captain', 'quarterback', 'pitcher',
    'player', 'athlete', 'champion',
    
    # Business
    'owner', 'proprietor',
    'partner', 'associate',
    'employee', 'worker',
})

# Plural noun endings - signal multiple results expected
PLURAL_ENDINGS = ('s', 'es', 'ies')

# "Me" signals - signal personalized/location-based
ME_WORDS = frozenset({
    'me', 'my', 'i', 'mine',
})

# Category words from your domain
CULTURE_WORDS = frozenset({
    'black', 'african', 'american', 'african-american',
    'culture', 'cultural', 'heritage', 'history', 'historical',
    'tradition', 'traditional', 'community',
})

FOOD_WORDS = frozenset({
    'food', 'foods', 'recipe', 'recipes', 'cooking',
    'cuisine', 'dish', 'dishes', 'meal', 'meals',
    'soul', 'southern', 'comfort',
})

MUSIC_WORDS = frozenset({
    'music', 'song', 'songs', 'album', 'albums',
    'jazz', 'blues', 'gospel', 'soul', 'hip-hop', 'rap',
    'r&b', 'funk', 'motown',
    'artist', 'artists', 'singer', 'singers',
    'musician', 'musicians', 'band', 'bands',
})


# =============================================================================
# PATTERN DETECTION (position-aware)
# =============================================================================

def _check_definition_pattern(terms: List[Dict]) -> bool:
    """Check for 'what is X' or 'define X' patterns."""
    if len(terms) < 2:
        return False
    
    first = terms[0].get('word', '').lower()
    second = terms[1].get('word', '').lower() if len(terms) > 1 else ''
    
    # "what is X" pattern
    if first == 'what' and second == 'is':
        return True
    
    # "define X" pattern
    if first in DEFINITION_PATTERNS:
        return True
    
    return False


def _check_proximity_pattern(terms: List[Dict]) -> Tuple[bool, Optional[str]]:
    """
    Check for 'X near Y' or 'X in Y' patterns.
    Returns (has_pattern, proximity_word).
    """
    for i, term in enumerate(terms):
        word = term.get('word', '').lower()
        
        if word in PROXIMITY_WORDS:
            # Check if there's something after the proximity word
            if i < len(terms) - 1:
                return True, word
    
    return False, None


def _check_me_pattern(terms: List[Dict]) -> bool:
    """Check for 'near me', 'for me', 'my area' patterns."""
    words = [t.get('word', '').lower() for t in terms]
    
    # Direct "me" reference
    if 'me' in words:
        return True
    
    # "my" + location word
    for i, word in enumerate(words):
        if word == 'my' and i < len(words) - 1:
            next_word = words[i + 1]
            if next_word in {'area', 'location', 'city', 'neighborhood', 'town'}:
                return True
    
    return False


def _check_comparison_pattern(terms: List[Dict]) -> Tuple[bool, List[str]]:
    """
    Check for comparison patterns like 'X vs Y' or 'X or Y'.
    Returns (has_comparison, items_being_compared).
    """
    words = [t.get('word', '').lower() for t in terms]
    items = []
    
    for i, word in enumerate(words):
        if word in {'vs', 'vs.', 'versus', 'or', 'and'}:
            # Get words before and after
            if i > 0:
                items.append(words[i - 1])
            if i < len(words) - 1:
                items.append(words[i + 1])
    
    return len(items) >= 2, items


# =============================================================================
# MAIN SIGNAL EXTRACTION
# =============================================================================

def detect_intent(discovery_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all linguistic signals from word discovery output.
    
    Does NOT make decisions - only reports facts.
    Typesense uses these signals to decide how to search.
    
    Args:
        discovery_result: Dict from process_query_optimized()
    
    Returns:
        Same dict with 'signals' field added
    """
    # Get data from discovery result
    query = discovery_result.get('query', '').lower().strip()
    terms = discovery_result.get('terms', [])
    ngrams = discovery_result.get('ngrams', [])
    category_summary = discovery_result.get('category_summary', {})
    sort_info = discovery_result.get('sort')
    
    # Initialize signals dict
    signals = {
        # Question signals
        'has_question_word': False,
        'question_word': None,
        'question_position': None,  # 'start', 'middle', 'end'
        'has_question_starter': False,
        'question_starter': None,
        
        # Temporal signals
        'has_temporal': False,
        'temporal_direction': None,  # 'oldest', 'newest'
        'temporal_word': None,
        
        # Proximity/location signals
        'has_proximity': False,
        'proximity_word': None,
        'has_me_reference': False,
        'has_location_preposition': False,
        'location_preposition': None,
        
        # Superlative signals
        'has_superlative': False,
        'superlative_word': None,
        
        # Quantity signals
        'has_quantity_word': False,
        'quantity_word': None,
        'has_plural_noun': False,
        'plural_nouns': [],
        
        # Action signals
        'has_action_word': False,
        'action_type': None,  # 'find', 'show', 'learn'
        'action_word': None,
        
        # Pattern signals
        'is_definition_query': False,
        'is_comparison_query': False,
        'comparison_items': [],
        
        # Service/local signals
        'has_service_word': False,
        'service_words': [],
        'is_local_search': False,
        
        # Role signals
        'has_role_word': False,
        'role_word': None,
        
        # Domain signals (from your content)
        'has_culture_word': False,
        'has_food_word': False,
        'has_music_word': False,
        
        # Entity signals (from category_summary)
        'has_person': category_summary.get('has_person', False),
        'has_location': category_summary.get('has_location', False),
        'has_organization': category_summary.get('has_business', False),
        'has_media': category_summary.get('has_media', False),
        'has_song_title': category_summary.get('has_song_title', False),
        
        # Query structure signals
        'word_count': len(terms),
        'all_nouns': False,
        'starts_with_noun': False,
        'ends_with_noun': False,
        'has_adjective': False,
        'has_verb': False,
        
        # Detected entities (from ngrams and high-rank terms)
        'detected_entities': [],
        
        # Sort signal (from word_discovery)
        'has_sort_signal': sort_info is not None,
        'sort_field': sort_info.get('field') if sort_info else None,
        'sort_order': sort_info.get('order') if sort_info else None,
    }
    
    # Track for aggregate analysis
    noun_count = 0
    service_words_found = []
    plural_nouns_found = []
    entities_found = []
    
    # =================================================================
    # SINGLE PASS THROUGH TERMS
    # =================================================================
    for i, term in enumerate(terms):
        word = term.get('word', '').lower()
        search_word = term.get('search_word', word).lower()
        pos = term.get('pos', '').lower()
        category = term.get('category', '').lower()
        rank = term.get('rank', 0)
        
        position = 'start' if i == 0 else ('end' if i == len(terms) - 1 else 'middle')
        
        # --- Question word check ---
        if word in QUESTION_WORDS:
            signals['has_question_word'] = True
            signals['question_word'] = word
            signals['question_position'] = position
        
        # --- Question starter check (only at start) ---
        if i == 0 and word in QUESTION_STARTERS:
            signals['has_question_starter'] = True
            signals['question_starter'] = word
        
        # --- Temporal check ---
        if word in TEMPORAL_OLDEST:
            signals['has_temporal'] = True
            signals['temporal_direction'] = 'oldest'
            signals['temporal_word'] = word
        elif word in TEMPORAL_NEWEST:
            signals['has_temporal'] = True
            signals['temporal_direction'] = 'newest'
            signals['temporal_word'] = word
        
        # --- Proximity check ---
        if word in PROXIMITY_WORDS:
            signals['has_proximity'] = True
            signals['proximity_word'] = word
        
        if word in LOCATION_PREPOSITIONS:
            signals['has_location_preposition'] = True
            signals['location_preposition'] = word
        
        # --- Me reference check ---
        if word in ME_WORDS:
            signals['has_me_reference'] = True
        
        # --- Superlative check ---
        if word in SUPERLATIVE_WORDS:
            signals['has_superlative'] = True
            signals['superlative_word'] = word
        
        # --- Quantity check ---
        if word in QUANTITY_WORDS:
            signals['has_quantity_word'] = True
            signals['quantity_word'] = word
        
        # --- Action word check ---
        if word in ACTION_FIND:
            signals['has_action_word'] = True
            signals['action_type'] = 'find'
            signals['action_word'] = word
        elif word in ACTION_SHOW:
            signals['has_action_word'] = True
            signals['action_type'] = 'show'
            signals['action_word'] = word
        elif word in ACTION_LEARN:
            signals['has_action_word'] = True
            signals['action_type'] = 'learn'
            signals['action_word'] = word
        
        # --- Service word check ---
        if word in SERVICE_WORDS or search_word in SERVICE_WORDS:
            signals['has_service_word'] = True
            service_words_found.append(word)
        
        # --- Role word check ---
        if word in ROLE_WORDS or search_word in ROLE_WORDS:
            signals['has_role_word'] = True
            signals['role_word'] = word
        
        # --- Domain word checks ---
        if word in CULTURE_WORDS:
            signals['has_culture_word'] = True
        if word in FOOD_WORDS:
            signals['has_food_word'] = True
        if word in MUSIC_WORDS:
            signals['has_music_word'] = True
        
        # --- Comparison check ---
        if word in COMPARISON_WORDS:
            signals['is_comparison_query'] = True
        
        # --- POS analysis ---
        if pos in {'noun', 'proper_noun'}:
            noun_count += 1
            if i == 0:
                signals['starts_with_noun'] = True
            if i == len(terms) - 1:
                signals['ends_with_noun'] = True
            
            # Check for plural
            if word.endswith(PLURAL_ENDINGS) and len(word) > 2:
                signals['has_plural_noun'] = True
                plural_nouns_found.append(word)
        
        if pos == 'adjective':
            signals['has_adjective'] = True
        
        if pos in {'verb', 'be', 'auxiliary', 'modal'}:
            signals['has_verb'] = True
        
        # --- Entity detection (high-rank proper nouns) ---
        if pos == 'proper_noun' and rank > 100 and category != 'stopword':
            entities_found.append({
                'word': search_word,
                'category': category,
                'rank': rank
            })
    
    # =================================================================
    # POST-LOOP ANALYSIS
    # =================================================================
    
    # All nouns check
    non_stopword_terms = [t for t in terms if t.get('category') != 'stopword']
    if non_stopword_terms and noun_count == len(non_stopword_terms):
        signals['all_nouns'] = True
    
    # Service words list
    signals['service_words'] = service_words_found
    
    # Plural nouns list
    signals['plural_nouns'] = plural_nouns_found
    
    # Definition pattern check
    signals['is_definition_query'] = _check_definition_pattern(terms)
    
    # Proximity pattern check
    has_prox, prox_word = _check_proximity_pattern(terms)
    if has_prox:
        signals['has_proximity'] = True
        signals['proximity_word'] = prox_word
    
    # Me pattern check
    if _check_me_pattern(terms):
        signals['has_me_reference'] = True
    
    # Comparison pattern check
    has_comp, comp_items = _check_comparison_pattern(terms)
    if has_comp:
        signals['is_comparison_query'] = True
        signals['comparison_items'] = comp_items
    
    # Local search detection
    signals['is_local_search'] = (
        signals['has_service_word'] and 
        (signals['has_proximity'] or signals['has_me_reference'] or signals['has_location'])
    )
    
    # =================================================================
    # NGRAM ENTITY EXTRACTION
    # =================================================================
    for ngram in ngrams:
        ngram_text = ngram.get('ngram', '')
        category = ngram.get('category', '').lower()
        rank = ngram.get('rank', 0)
        
        if category and ngram_text:
            entities_found.append({
                'phrase': ngram_text,
                'category': category,
                'rank': rank,
                'is_ngram': True
            })
    
    # Sort entities by rank (highest first)
    entities_found.sort(key=lambda x: -x.get('rank', 0))
    signals['detected_entities'] = entities_found
    
    # =================================================================
    # ADD SIGNALS TO RESULT
    # =================================================================
    discovery_result['signals'] = signals
    
    return discovery_result


# =============================================================================
# DEBUG PRINT FUNCTION
# =============================================================================

def print_intent_debug(discovery_result: Dict[str, Any]) -> None:
    """Print all detected signals for debugging."""
    signals = discovery_result.get('signals', {})
    
    print("\n" + "=" * 70)
    print("🎯 SIGNAL DETECTION DEBUG")
    print("=" * 70)
    
    print(f"\n📝 Query: '{discovery_result.get('query', '')}'")
    print(f"📊 Word Count: {signals.get('word_count', 0)}")
    
    # Question signals
    print("\n" + "-" * 70)
    print("❓ QUESTION SIGNALS")
    print("-" * 70)
    print(f"  has_question_word: {signals.get('has_question_word')} → {signals.get('question_word')} ({signals.get('question_position')})")
    print(f"  has_question_starter: {signals.get('has_question_starter')} → {signals.get('question_starter')}")
    print(f"  is_definition_query: {signals.get('is_definition_query')}")
    
    # Temporal signals
    print("\n" + "-" * 70)
    print("⏰ TEMPORAL SIGNALS")
    print("-" * 70)
    print(f"  has_temporal: {signals.get('has_temporal')} → {signals.get('temporal_word')}")
    print(f"  temporal_direction: {signals.get('temporal_direction')}")
    print(f"  has_sort_signal: {signals.get('has_sort_signal')} → {signals.get('sort_field')} {signals.get('sort_order')}")
    
    # Location/Proximity signals
    print("\n" + "-" * 70)
    print("📍 LOCATION/PROXIMITY SIGNALS")
    print("-" * 70)
    print(f"  has_proximity: {signals.get('has_proximity')} → {signals.get('proximity_word')}")
    print(f"  has_me_reference: {signals.get('has_me_reference')}")
    print(f"  has_location_preposition: {signals.get('has_location_preposition')} → {signals.get('location_preposition')}")
    print(f"  has_location (entity): {signals.get('has_location')}")
    print(f"  is_local_search: {signals.get('is_local_search')}")
    
    # Service signals
    print("\n" + "-" * 70)
    print("🏪 SERVICE SIGNALS")
    print("-" * 70)
    print(f"  has_service_word: {signals.get('has_service_word')}")
    print(f"  service_words: {signals.get('service_words')}")
    
    # Superlative/Quantity signals
    print("\n" + "-" * 70)
    print("📈 SUPERLATIVE/QUANTITY SIGNALS")
    print("-" * 70)
    print(f"  has_superlative: {signals.get('has_superlative')} → {signals.get('superlative_word')}")
    print(f"  has_quantity_word: {signals.get('has_quantity_word')} → {signals.get('quantity_word')}")
    print(f"  has_plural_noun: {signals.get('has_plural_noun')}")
    print(f"  plural_nouns: {signals.get('plural_nouns')}")
    
    # Action signals
    print("\n" + "-" * 70)
    print("🎬 ACTION SIGNALS")
    print("-" * 70)
    print(f"  has_action_word: {signals.get('has_action_word')} → {signals.get('action_word')}")
    print(f"  action_type: {signals.get('action_type')}")
    
    # Role signals
    print("\n" + "-" * 70)
    print("👔 ROLE SIGNALS")
    print("-" * 70)
    print(f"  has_role_word: {signals.get('has_role_word')} → {signals.get('role_word')}")
    
    # Comparison signals
    print("\n" + "-" * 70)
    print("⚖️ COMPARISON SIGNALS")
    print("-" * 70)
    print(f"  is_comparison_query: {signals.get('is_comparison_query')}")
    print(f"  comparison_items: {signals.get('comparison_items')}")
    
    # Domain signals
    print("\n" + "-" * 70)
    print("🎨 DOMAIN SIGNALS")
    print("-" * 70)
    print(f"  has_culture_word: {signals.get('has_culture_word')}")
    print(f"  has_food_word: {signals.get('has_food_word')}")
    print(f"  has_music_word: {signals.get('has_music_word')}")
    
    # Entity signals
    print("\n" + "-" * 70)
    print("🏷️ ENTITY SIGNALS")
    print("-" * 70)
    print(f"  has_person: {signals.get('has_person')}")
    print(f"  has_organization: {signals.get('has_organization')}")
    print(f"  has_media: {signals.get('has_media')}")
    print(f"  has_song_title: {signals.get('has_song_title')}")
    
    # Structure signals
    print("\n" + "-" * 70)
    print("🔤 STRUCTURE SIGNALS")
    print("-" * 70)
    print(f"  all_nouns: {signals.get('all_nouns')}")
    print(f"  starts_with_noun: {signals.get('starts_with_noun')}")
    print(f"  ends_with_noun: {signals.get('ends_with_noun')}")
    print(f"  has_adjective: {signals.get('has_adjective')}")
    print(f"  has_verb: {signals.get('has_verb')}")
    
    # Detected entities
    print("\n" + "-" * 70)
    print("🎯 DETECTED ENTITIES")
    print("-" * 70)
    entities = signals.get('detected_entities', [])
    if entities:
        for ent in entities[:5]:  # Show top 5
            if ent.get('is_ngram'):
                print(f"  📎 '{ent.get('phrase')}' → {ent.get('category')} (rank: {ent.get('rank')})")
            else:
                print(f"  📌 '{ent.get('word')}' → {ent.get('category')} (rank: {ent.get('rank')})")
    else:
        print("  (none detected)")
    
    print("\n" + "=" * 70)


# =============================================================================
# CONVENIENCE FUNCTIONS (for Typesense to use)
# =============================================================================

def get_signals(discovery_result: Dict[str, Any]) -> Dict[str, Any]:
    """Get the signals dict from discovery result."""
    return discovery_result.get('signals', {})


def is_question_query(discovery_result: Dict[str, Any]) -> bool:
    """Check if query has question signals."""
    signals = get_signals(discovery_result)
    return signals.get('has_question_word', False) or signals.get('has_question_starter', False)


def is_local_search(discovery_result: Dict[str, Any]) -> bool:
    """Check if query is a local/proximity search."""
    signals = get_signals(discovery_result)
    return signals.get('is_local_search', False)


def wants_single_result(discovery_result: Dict[str, Any]) -> bool:
    """
    Heuristic: Does this query likely want a single/specific result?
    
    Signals that suggest single result:
    - Question word (who, what, when, where)
    - Temporal superlative (first, last)
    - Role word + organization
    - Specific entity detected
    """
    signals = get_signals(discovery_result)
    
    # Strong single-result signals
    if signals.get('has_question_word') and signals.get('question_word') in {'who', 'what', 'when', 'where'}:
        if signals.get('has_temporal') or signals.get('has_role_word'):
            return True
    
    # Temporal + role is strong signal
    if signals.get('has_temporal') and signals.get('has_role_word'):
        return True
    
    return False


def wants_multiple_results(discovery_result: Dict[str, Any]) -> bool:
    """
    Heuristic: Does this query likely want multiple results?
    
    Signals that suggest multiple results:
    - Plural nouns
    - Quantity words (list, all, many)
    - Service words (restaurants, designers)
    - Superlatives (best, top)
    - No question word, all nouns
    """
    signals = get_signals(discovery_result)
    
    if signals.get('has_plural_noun'):
        return True
    if signals.get('has_quantity_word'):
        return True
    if signals.get('has_service_word'):
        return True
    if signals.get('has_superlative'):
        return True
    if signals.get('all_nouns') and not signals.get('has_question_word'):
        return True
    
    return False


def get_detected_entities(discovery_result: Dict[str, Any]) -> List[Dict]:
    """Get list of detected entities, sorted by rank."""
    signals = get_signals(discovery_result)
    return signals.get('detected_entities', [])


def get_temporal_direction(discovery_result: Dict[str, Any]) -> Optional[str]:
    """Get temporal direction if present ('oldest' or 'newest')."""
    signals = get_signals(discovery_result)
    return signals.get('temporal_direction')


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    # Test with mock data
    
    print("=" * 70)
    print("TESTING SIGNAL DETECTION")
    print("=" * 70)
    
    # Test 1: Question query
    test1 = {
        'query': 'who was the first president of morehouse',
        'terms': [
            {'word': 'who', 'search_word': 'who', 'pos': 'wh_pronoun', 'category': 'stopword', 'rank': 0},
            {'word': 'was', 'search_word': 'was', 'pos': 'verb', 'category': 'stopword', 'rank': 0},
            {'word': 'the', 'search_word': 'the', 'pos': 'article', 'category': 'stopword', 'rank': 0},
            {'word': 'first', 'search_word': 'first', 'pos': 'adjective', 'category': 'keyword', 'rank': 500},
            {'word': 'president', 'search_word': 'president', 'pos': 'noun', 'category': 'keyword', 'rank': 600},
            {'word': 'of', 'search_word': 'of', 'pos': 'preposition', 'category': 'stopword', 'rank': 0},
            {'word': 'morehouse', 'search_word': 'morehouse', 'pos': 'proper_noun', 'category': 'organization', 'rank': 800},
        ],
        'ngrams': [],
        'category_summary': {'has_person': False, 'has_location': False, 'has_business': True},
        'sort': {'field': 'time_period_start', 'order': 'asc'}
    }
    
    result1 = detect_intent(test1)
    print_intent_debug(result1)
    
    # Test 2: Local search
    test2 = {
        'query': 'restaurants near me',
        'terms': [
            {'word': 'restaurants', 'search_word': 'restaurants', 'pos': 'noun', 'category': 'service', 'rank': 400},
            {'word': 'near', 'search_word': 'near', 'pos': 'preposition', 'category': 'stopword', 'rank': 0},
            {'word': 'me', 'search_word': 'me', 'pos': 'pronoun', 'category': 'stopword', 'rank': 0},
        ],
        'ngrams': [],
        'category_summary': {'has_person': False, 'has_location': False},
        'sort': None
    }
    
    result2 = detect_intent(test2)
    print_intent_debug(result2)
    
    # Test 3: Browse query
    test3 = {
        'query': 'black women leadership',
        'terms': [
            {'word': 'black', 'search_word': 'black', 'pos': 'adjective', 'category': 'culture', 'rank': 850},
            {'word': 'women', 'search_word': 'women', 'pos': 'noun', 'category': 'keyword', 'rank': 400},
            {'word': 'leadership', 'search_word': 'leadership', 'pos': 'noun', 'category': 'keyword', 'rank': 600},
        ],
        'ngrams': [],
        'category_summary': {'has_person': False, 'has_location': False, 'has_culture': True},
        'sort': None
    }
    
    result3 = detect_intent(test3)
    print_intent_debug(result3)
    
    # Test 4: Entity lookup
    test4 = {
        'query': 'billie holiday',
        'terms': [
            {'word': 'billie', 'search_word': 'billie', 'pos': 'proper_noun', 'category': 'person', 'rank': 700},
            {'word': 'holiday', 'search_word': 'holiday', 'pos': 'proper_noun', 'category': 'person', 'rank': 500},
        ],
        'ngrams': [
            {'ngram': 'billie holiday', 'category': 'person', 'rank': 900}
        ],
        'category_summary': {'has_person': True, 'has_location': False},
        'sort': None
    }
    
    result4 = detect_intent(test4)
    print_intent_debug(result4)
    
    # Test 5: Superlative query
    test5 = {
        'query': 'best soul food in atlanta',
        'terms': [
            {'word': 'best', 'search_word': 'best', 'pos': 'adjective', 'category': 'keyword', 'rank': 300},
            {'word': 'soul', 'search_word': 'soul', 'pos': 'noun', 'category': 'music', 'rank': 600},
            {'word': 'food', 'search_word': 'food', 'pos': 'noun', 'category': 'food', 'rank': 700},
            {'word': 'in', 'search_word': 'in', 'pos': 'preposition', 'category': 'stopword', 'rank': 0},
            {'word': 'atlanta', 'search_word': 'atlanta', 'pos': 'proper_noun', 'category': 'us city', 'rank': 850},
        ],
        'ngrams': [
            {'ngram': 'soul food', 'category': 'food', 'rank': 920}
        ],
        'category_summary': {'has_person': False, 'has_location': True, 'has_food': True},
        'sort': None
    }
    
    result5 = detect_intent(test5)
    print_intent_debug(result5)
    
    print("\n✓ All tests completed")