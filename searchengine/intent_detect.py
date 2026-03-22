"""
intent_detect.py
Pure fact extraction from word discovery output.

PURPOSE:
    Extract and report ALL linguistic signals from the query.
    Does NOT make decisions - only presents facts for the search bridge to interpret.

DESIGN PRINCIPLES:
    1. DETECT, DON'T DECIDE - Extract signals, let the bridge act on them
    2. O(1) LOOKUPS - All word checks use frozensets and dicts
    3. SINGLE PASS - One loop through terms, no redundant iteration
    4. VERTICAL AGNOSTIC - Works for food, fashion, real estate, travel, etc.
    5. COMPOSABLE - Signals can be combined by the bridge for complex intents

SIGNALS EXTRACTED:
    - Question signals (who, what, when, where, why, how)
    - Temporal signals (first, last, oldest, newest, historical, upcoming)
    - Proximity/location signals (near, in, at, around + term-level city/state detection)
    - Superlative signals (best, top, most, biggest, cheapest)
    - Quantity signals (plural nouns, list requests)
    - Entity signals (person, place, organization from terms AND category_summary)
    - Service signals (restaurants, designers, doctors, realtors, etc.)
    - Product signals (shoes, phones, laptops, clothing, etc.)
    - Action signals (find, show, get, buy, book, compare)
    - Comparison signals (vs, versus, compare, difference, between)
    - Definition signals (what is, define, meaning)
    - Negation signals (not, without, no, except, exclude)
    - Price signals (cheap, expensive, under $X, affordable, luxury)
    - Rating signals (best, top rated, highest rated, 5 star)
    - Recency signals (new, latest, 2024, this year, upcoming)
    - Black-owned signals (black owned, black-owned)
    - Query mode (browse, answer, explore, compare, shop, local)
    - Intent complexity (how many signals are active)
    - wants_single_result / wants_multiple_results (heuristics for the bridge)
    - has_unknown_terms (from WD stats, for semantic blend adjustment)

USAGE:
    from intent_detect import detect_intent

    result = process_query_optimized(query)
    result = detect_intent(result)

    signals = result['signals']
    if signals['is_local_search']:
        # Apply location filters
    if signals['query_mode'] == 'answer':
        # Boost featured result
    if signals['has_price_signal']:
        # Apply price sorting
"""

from typing import Dict, Any, List, Optional, Tuple


# =============================================================================
# SIGNAL WORD SETS (O(1) lookup)
# =============================================================================

# ─── QUESTION WORDS ─────────────────────────────────────────────────────────
QUESTION_WORDS = frozenset({
    'who', "who's", 'whom', 'whose',
    'what', "what's", 'whatever',
    'when', "when's",
    'where', "where's", 'wherever',
    'why', "why's",
    'how', "how's",
    'which', 'whichever',
})

QUESTION_STARTERS = frozenset({
    'is', 'are', 'was', 'were',
    'do', 'does', 'did',
    'can', 'could',
    'will', 'would',
    'should', 'shall',
    'has', 'have', 'had',
    'may', 'might', 'must',
})

# ─── TEMPORAL WORDS ─────────────────────────────────────────────────────────
TEMPORAL_OLDEST = frozenset({
    'first', 'oldest', 'earliest', 'original', 'initial',
    'founding', 'pioneer', 'pioneering', 'inaugural',
    'historic', 'historical', 'ancient', 'classic', 'vintage',
})

TEMPORAL_NEWEST = frozenset({
    'last', 'latest', 'newest', 'recent', 'current',
    'modern', 'contemporary', 'today', 'now',
    'upcoming', 'trending', 'emerging', 'new',
})

TEMPORAL_ALL = TEMPORAL_OLDEST | TEMPORAL_NEWEST

# ─── PROXIMITY / LOCATION WORDS ─────────────────────────────────────────────
PROXIMITY_WORDS = frozenset({
    'near', 'nearby', 'around', 'close',
    'in', 'at', 'on', 'within',
    'local', 'locally',
})

LOCATION_PREPOSITIONS = frozenset({
    'in', 'at', 'near', 'around', 'from', 'to',
    'within', 'outside', 'inside', 'across',
    'throughout', 'between',
})

# ─── SUPERLATIVE WORDS ──────────────────────────────────────────────────────
SUPERLATIVE_WORDS = frozenset({
    'best', 'top', 'most', 'greatest', 'finest',
    'worst', 'least',
    'biggest', 'largest', 'smallest',
    'highest', 'lowest',
    'richest', 'poorest',
    'fastest', 'slowest',
    'strongest', 'weakest',
    'famous', 'popular', 'renowned', 'leading',
    'favorite', 'favourite', 'premier', 'premier',
    'number one', 'no 1',
})

# ─── QUANTITY / LIST WORDS ──────────────────────────────────────────────────
QUANTITY_WORDS = frozenset({
    'list', 'lists',
    'all', 'every', 'any', 'some',
    'many', 'few', 'several', 'multiple',
    'examples', 'options', 'choices', 'alternatives',
    'types', 'kinds', 'varieties', 'categories',
    'recommendations', 'suggestions', 'ideas',
    'guide', 'directory', 'collection',
})

# ─── ACTION WORDS ───────────────────────────────────────────────────────────
ACTION_FIND = frozenset({
    'find', 'search', 'look', 'looking', 'seek', 'seeking',
    'locate', 'discover', 'explore',
})

ACTION_SHOW = frozenset({
    'show', 'display', 'give', 'tell', 'list',
    'get', 'fetch', 'bring', 'see',
})

ACTION_LEARN = frozenset({
    'learn', 'understand', 'explain', 'know',
    'teach', 'study', 'read',
})

ACTION_BUY = frozenset({
    'buy', 'purchase', 'order', 'shop', 'shopping',
    'get', 'rent', 'lease', 'hire', 'book', 'reserve',
})

ACTION_CREATE = frozenset({
    'make', 'create', 'build', 'design', 'cook',
    'prepare', 'craft', 'diy',
})

ACTION_ALL = ACTION_FIND | ACTION_SHOW | ACTION_LEARN | ACTION_BUY | ACTION_CREATE

# ─── DEFINITION SIGNALS ─────────────────────────────────────────────────────
DEFINITION_PATTERNS = frozenset({
    'define', 'definition', 'meaning',
    'means', 'mean',
})

# ─── COMPARISON WORDS ───────────────────────────────────────────────────────
COMPARISON_WORDS = frozenset({
    'vs', 'vs.', 'versus', 'against',
    'compare', 'compared', 'comparing', 'comparison',
    'difference', 'differences', 'different',
    'between', 'better', 'alternative', 'alternatives',
})

# ─── NEGATION WORDS ─────────────────────────────────────────────────────────
NEGATION_WORDS = frozenset({
    'not', 'no', 'without', 'except', 'exclude', 'excluding',
    'none', 'never', 'neither', 'nor', 'avoid',
    'non', 'free',  # as in "gluten-free", "sugar-free"
})

# ─── PRICE / VALUE SIGNALS ──────────────────────────────────────────────────
PRICE_CHEAP = frozenset({
    'cheap', 'affordable', 'budget', 'inexpensive',
    'free', 'discount', 'discounted', 'deal', 'deals',
    'bargain', 'value', 'economical', 'low-cost',
    'under', 'less',
})

PRICE_EXPENSIVE = frozenset({
    'expensive', 'luxury', 'luxurious', 'premium', 'high-end',
    'upscale', 'exclusive', 'designer', 'boutique',
    'fine', 'gourmet', 'deluxe', 'elite',
})

PRICE_ALL = PRICE_CHEAP | PRICE_EXPENSIVE

# ─── RATING SIGNALS ─────────────────────────────────────────────────────────
RATING_WORDS = frozenset({
    'rated', 'rating', 'ratings', 'review', 'reviews', 'reviewed',
    'star', 'stars', 'recommended', 'award', 'award-winning',
    'certified', 'accredited', 'verified', 'trusted',
})

# ─── SERVICE / BUSINESS TYPES (comprehensive for all verticals) ─────────────
SERVICE_WORDS = frozenset({
    # Food & Dining
    'restaurant', 'restaurants', 'cafe', 'cafes', 'coffee',
    'bar', 'bars', 'club', 'clubs', 'lounge', 'lounges',
    'bakery', 'bakeries', 'diner', 'diners',
    'pizzeria', 'pizzerias', 'steakhouse', 'steakhouses',
    'food', 'foods', 'dining', 'eatery', 'eateries',
    'catering', 'caterer', 'caterers',
    'brewery', 'breweries', 'winery', 'wineries', 'distillery',

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
    'realtor', 'realtors', 'agent', 'agents', 'broker', 'brokers',
    'advisor', 'advisors', 'planner', 'planners',
    'veterinarian', 'vet', 'vets',

    # Retail & Shopping
    'store', 'stores', 'shop', 'shops', 'boutique', 'boutiques',
    'market', 'markets', 'mall', 'malls', 'outlet', 'outlets',
    'salon', 'salons', 'spa', 'spas', 'barbershop', 'barbershops',
    'dealership', 'dealerships', 'showroom', 'showrooms',

    # Health & Fitness
    'gym', 'gyms', 'fitness', 'yoga', 'studio', 'studios',
    'clinic', 'clinics', 'hospital', 'hospitals',
    'pharmacy', 'pharmacies', 'urgent care',
    'chiropractor', 'chiropractors', 'optometrist',

    # Education
    'school', 'schools', 'college', 'colleges',
    'university', 'universities', 'tutor', 'tutors',
    'academy', 'academies', 'institute', 'institutes',
    'preschool', 'daycare',

    # Entertainment
    'theater', 'theaters', 'theatre', 'theatres',
    'cinema', 'cinemas', 'museum', 'museums',
    'gallery', 'galleries', 'venue', 'venues',
    'arena', 'arenas', 'stadium', 'stadiums',
    'park', 'parks', 'zoo', 'aquarium',

    # Lodging & Travel
    'hotel', 'hotels', 'motel', 'motels',
    'inn', 'inns', 'resort', 'resorts',
    'airbnb', 'hostel', 'hostels', 'lodge', 'lodges',
    'vacation rental', 'bed and breakfast',

    # Real Estate
    'apartment', 'apartments', 'condo', 'condos', 'condo',
    'townhouse', 'townhomes', 'house', 'houses', 'home', 'homes',
    'property', 'properties', 'listing', 'listings',
    'rental', 'rentals',

    # Financial
    'bank', 'banks', 'credit union', 'atm', 'atms',
    'insurance', 'mortgage', 'lender', 'lenders',

    # Auto
    'gas', 'station', 'stations',
    'parking', 'garage', 'garages',
    'car wash', 'auto shop', 'tire',

    # Religious
    'church', 'churches', 'mosque', 'mosques',
    'temple', 'temples', 'synagogue', 'synagogues',

    # Other
    'laundromat', 'dry cleaner', 'cleaners',
    'florist', 'florists', 'tailor', 'tailors',
    'moving company', 'storage',
})

# ─── PRODUCT WORDS ──────────────────────────────────────────────────────────
PRODUCT_WORDS = frozenset({
    # Fashion & Apparel
    'shoes', 'shoe', 'sneakers', 'sneaker', 'boots', 'heels',
    'dress', 'dresses', 'shirt', 'shirts', 'pants', 'jeans',
    'jacket', 'jackets', 'coat', 'coats', 'suit', 'suits',
    'hat', 'hats', 'bag', 'bags', 'purse', 'purses',
    'jewelry', 'jewellery', 'watch', 'watches', 'sunglasses',
    'clothing', 'clothes', 'apparel', 'outfit', 'outfits',
    'hoodie', 'hoodies', 'sweater', 'sweaters',

    # Tech & Electronics
    'phone', 'phones', 'laptop', 'laptops', 'computer', 'computers',
    'tablet', 'tablets', 'headphones', 'earbuds', 'speaker', 'speakers',
    'camera', 'cameras', 'tv', 'television', 'monitor', 'monitors',
    'keyboard', 'mouse', 'printer', 'printers',
    'software', 'app', 'apps',

    # Home & Living
    'furniture', 'sofa', 'couch', 'table', 'chair', 'chairs',
    'bed', 'beds', 'mattress', 'mattresses', 'pillow', 'pillows',
    'lamp', 'lamps', 'rug', 'rugs', 'curtains', 'blinds',
    'appliance', 'appliances',

    # Beauty & Personal Care
    'skincare', 'makeup', 'cosmetics', 'perfume', 'cologne',
    'shampoo', 'conditioner', 'lotion', 'serum', 'moisturizer',
    'lipstick', 'foundation', 'mascara', 'hair products',

    # Automotive
    'car', 'cars', 'truck', 'trucks', 'suv', 'suvs',
    'motorcycle', 'motorcycles', 'vehicle', 'vehicles',
    'tires', 'parts',

    # Books & Media
    'book', 'books', 'novel', 'novels', 'textbook', 'textbooks',
    'album', 'albums', 'vinyl', 'game', 'games',

    # Food Products
    'recipe', 'recipes', 'ingredient', 'ingredients',
    'supplement', 'supplements', 'vitamin', 'vitamins',
    'snack', 'snacks', 'drink', 'drinks', 'beverage', 'beverages',

    # Fitness & Sports
    'equipment', 'gear', 'weights', 'treadmill',
    'bike', 'bikes', 'bicycle', 'bicycles',

    # Kids & Baby
    'toy', 'toys', 'stroller', 'crib', 'diaper', 'diapers',

    # Garden & Outdoor
    'plant', 'plants', 'seeds', 'tools', 'grill', 'grills',

    # Pet
    'pet food', 'dog food', 'cat food', 'pet supplies',
})

# ─── ROLE / TITLE WORDS ─────────────────────────────────────────────────────
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
    'minister', 'ambassador',
    'judge', 'justice', 'secretary',
    'commissioner', 'councilman', 'councilwoman',

    # Education
    'principal', 'dean', 'provost', 'chancellor',
    'professor', 'teacher', 'instructor',
    'superintendent',

    # Religion
    'pastor', 'priest', 'rabbi', 'imam',
    'bishop', 'pope', 'reverend', 'deacon',

    # Arts & Entertainment
    'author', 'writer', 'poet', 'playwright',
    'singer', 'musician', 'artist', 'performer',
    'actor', 'actress',
    'producer', 'composer', 'conductor',
    'chef', 'influencer', 'blogger',

    # Sports
    'coach', 'captain', 'quarterback', 'pitcher',
    'player', 'athlete', 'champion', 'mvp',

    # Business
    'owner', 'proprietor',
    'partner', 'associate',
    'entrepreneur', 'mogul',
})

# ─── LOCATION CATEGORY KEYWORDS (from Word Discovery category field) ────────
LOCATION_CATEGORIES = frozenset({
    'us city', 'us_city', 'city',
    'us state', 'us_state', 'state',
    'us county', 'us_county', 'county',
    'country', 'continent',
    'location', 'region', 'neighborhood',
    'district', 'borough', 'parish',
})

# ─── PERSON CATEGORY KEYWORDS ───────────────────────────────────────────────
PERSON_CATEGORIES = frozenset({
    'person', 'historical figure', 'celebrity',
    'athlete', 'politician', 'artist', 'musician',
    'author', 'leader', 'activist', 'entrepreneur',
})

# ─── ORGANIZATION CATEGORY KEYWORDS ─────────────────────────────────────────
ORGANIZATION_CATEGORIES = frozenset({
    'organization', 'company', 'business', 'brand',
    'hbcu', 'university', 'school', 'nonprofit',
    'agency', 'foundation', 'corporation',
})

# ─── MEDIA CATEGORY KEYWORDS ────────────────────────────────────────────────
MEDIA_CATEGORIES = frozenset({
    'song', 'movie', 'album', 'book', 'tv show',
    'podcast', 'documentary', 'film', 'series',
    'magazine', 'newspaper', 'publication',
})

# ─── "ME" SIGNALS ───────────────────────────────────────────────────────────
ME_WORDS = frozenset({
    'me', 'my', 'i', 'mine', 'myself',
})

# ─── BLACK-OWNED SIGNALS ────────────────────────────────────────────────────
BLACK_OWNED_PHRASES = frozenset({
    'black owned', 'black-owned', 'blackowned',
    'minority owned', 'minority-owned',
})

# ─── DOMAIN / VERTICAL WORDS ────────────────────────────────────────────────
# These are broad topic indicators, NOT for disambiguation
# The bridge uses these to understand which vertical the query touches

DOMAIN_CULTURE = frozenset({
    'black', 'african', 'american', 'african-american',
    'culture', 'cultural', 'heritage', 'history', 'historical',
    'tradition', 'traditional', 'community', 'diaspora',
    'indigenous', 'native', 'ethnic', 'multicultural',
})

DOMAIN_FOOD = frozenset({
    'food', 'foods', 'recipe', 'recipes', 'cooking',
    'cuisine', 'dish', 'dishes', 'meal', 'meals',
    'soul food', 'southern', 'comfort food',
    'vegan', 'vegetarian', 'organic', 'gluten-free',
    'breakfast', 'lunch', 'dinner', 'brunch',
    'dessert', 'appetizer', 'entree',
})

DOMAIN_MUSIC = frozenset({
    'music', 'song', 'songs', 'album', 'albums',
    'jazz', 'blues', 'gospel', 'soul music', 'hip-hop', 'rap',
    'r&b', 'funk', 'motown', 'reggae', 'afrobeat',
    'concert', 'concerts', 'tour', 'playlist',
    'artist', 'artists', 'singer', 'singers',
    'musician', 'musicians', 'band', 'bands',
})

DOMAIN_FASHION = frozenset({
    'fashion', 'style', 'outfit', 'outfits',
    'clothing', 'clothes', 'apparel', 'wardrobe',
    'streetwear', 'couture', 'runway', 'collection',
    'trend', 'trends', 'trending',
})

DOMAIN_HEALTH = frozenset({
    'health', 'healthy', 'wellness', 'fitness',
    'medical', 'medicine', 'healthcare',
    'mental health', 'therapy', 'nutrition',
    'exercise', 'workout', 'diet',
    'disease', 'condition', 'treatment', 'symptom', 'symptoms',
})

DOMAIN_TRAVEL = frozenset({
    'travel', 'traveling', 'travelling', 'trip', 'trips',
    'vacation', 'holiday', 'holidays', 'tourism', 'tourist',
    'flight', 'flights', 'airline', 'airlines',
    'destination', 'destinations', 'itinerary',
    'beach', 'island', 'resort', 'cruise',
    'passport', 'visa', 'abroad',
})

DOMAIN_REALESTATE = frozenset({
    'real estate', 'realestate', 'property', 'properties',
    'house', 'houses', 'home', 'homes', 'housing',
    'apartment', 'apartments', 'condo', 'condos',
    'rent', 'rental', 'lease', 'mortgage',
    'bedroom', 'bathroom', 'sqft', 'square feet',
    'foreclosure', 'investment property',
})

DOMAIN_EDUCATION = frozenset({
    'education', 'school', 'schools', 'college', 'colleges',
    'university', 'universities', 'degree', 'degrees',
    'scholarship', 'scholarships', 'tuition',
    'student', 'students', 'graduate', 'undergraduate',
    'hbcu', 'hbcus', 'campus', 'enrollment',
    'major', 'program', 'programs', 'course', 'courses',
})

DOMAIN_SPORTS = frozenset({
    'sports', 'sport', 'athletic', 'athletics',
    'football', 'basketball', 'baseball', 'soccer',
    'tennis', 'golf', 'boxing', 'track',
    'nfl', 'nba', 'mlb', 'nhl', 'ncaa',
    'team', 'teams', 'league', 'season',
    'game', 'games', 'match', 'tournament',
    'championship', 'playoffs', 'draft',
})

DOMAIN_BUSINESS = frozenset({
    'business', 'businesses', 'company', 'companies',
    'startup', 'startups', 'entrepreneur', 'entrepreneurship',
    'corporation', 'corporate', 'industry',
    'revenue', 'profit', 'investment', 'investor',
    'franchise', 'brand', 'brands',
    'marketing', 'advertising', 'sales',
    'black-owned', 'black owned', 'minority-owned', 'woman-owned',
})

DOMAIN_TECHNOLOGY = frozenset({
    'tech', 'technology', 'digital', 'software',
    'ai', 'artificial intelligence', 'machine learning',
    'app', 'apps', 'website', 'platform',
    'startup', 'innovation', 'cyber', 'blockchain',
    'coding', 'programming', 'developer',
})

DOMAIN_BEAUTY = frozenset({
    'beauty', 'skincare', 'makeup', 'cosmetics',
    'hair', 'hairstyle', 'hairstyles', 'haircare',
    'nails', 'fragrance', 'grooming',
    'natural hair', 'braids', 'locs', 'twists',
})

# Map domain name to its frozenset for iteration
ALL_DOMAINS = {
    'culture': DOMAIN_CULTURE,
    'food': DOMAIN_FOOD,
    'music': DOMAIN_MUSIC,
    'fashion': DOMAIN_FASHION,
    'health': DOMAIN_HEALTH,
    'travel': DOMAIN_TRAVEL,
    'real_estate': DOMAIN_REALESTATE,
    'education': DOMAIN_EDUCATION,
    'sports': DOMAIN_SPORTS,
    'business': DOMAIN_BUSINESS,
    'technology': DOMAIN_TECHNOLOGY,
    'beauty': DOMAIN_BEAUTY,
}

# Plural endings for detection
PLURAL_ENDINGS = ('s', 'es', 'ies')

# Words that are ambiguous across domains — NOT used for domain signals
# These need surrounding context to disambiguate
AMBIGUOUS_WORDS = frozenset({
    'soul',       # music OR food ("soul food" vs "soul music")
    'club',       # nightclub OR sports club OR book club
    'studio',     # art, music, fitness, hair
    'bar',        # drinking OR legal bar OR gym bar
    'court',      # legal OR sports
    'plant',      # factory OR botanical
    'stock',      # financial OR inventory
    'pitch',      # sales OR sports
    'coach',      # sports OR life coach
    'draft',      # sports OR beer OR writing
    'foundation', # cosmetics OR nonprofit
    'press',      # media OR weightlifting
    'record',     # music OR achievement
    'set',        # tennis OR collection
    'match',      # sports OR dating
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

    if first == 'what' and second in ('is', 'are', 'was', 'were'):
        return True
    if first == 'who' and second in ('is', 'was'):
        return True
    if first in DEFINITION_PATTERNS:
        return True

    return False


def _check_proximity_pattern(terms: List[Dict]) -> Tuple[bool, Optional[str]]:
    """Check for 'X near Y' or 'X in Y' patterns."""
    for i, term in enumerate(terms):
        word = term.get('word', '').lower()
        if word in PROXIMITY_WORDS and i < len(terms) - 1:
            return True, word
    return False, None


def _check_me_pattern(terms: List[Dict]) -> bool:
    """Check for 'near me', 'for me', 'my area' patterns."""
    words = [t.get('word', '').lower() for t in terms]

    if 'me' in words:
        return True

    for i, word in enumerate(words):
        if word == 'my' and i < len(words) - 1:
            next_word = words[i + 1]
            if next_word in {'area', 'location', 'city', 'neighborhood',
                             'town', 'zip', 'zipcode', 'region'}:
                return True

    return False


def _check_comparison_pattern(terms: List[Dict], ngrams: List[Dict]) -> Tuple[bool, List[str]]:
    """
    Check for comparison patterns like 'X vs Y' or 'X or Y'.
    Uses ngrams for multi-word entity awareness.
    """
    words = [t.get('word', '').lower() for t in terms]
    comparison_markers = {'vs', 'vs.', 'versus', 'or', 'compared'}

    # Build ngram phrases for better entity extraction
    ngram_phrases = {}
    for ng in ngrams:
        positions = ng.get('positions', [])
        phrase = ng.get('ngram', ng.get('phrase', ''))
        if positions and phrase:
            for pos in positions:
                ngram_phrases[pos] = phrase

    items = []
    for i, word in enumerate(words):
        if word in comparison_markers:
            # Look backward for entity
            before_phrase = ngram_phrases.get(i - 1)
            if before_phrase:
                items.append(before_phrase)
            elif i > 0:
                items.append(words[i - 1])

            # Look forward for entity
            after_phrase = ngram_phrases.get(i + 1)
            if after_phrase:
                items.append(after_phrase)
            elif i < len(words) - 1:
                items.append(words[i + 1])

    return len(items) >= 2, items


def _check_negation_context(terms: List[Dict]) -> Tuple[bool, List[str], List[str]]:
    """
    Check for negation patterns and what is being negated.
    Returns: (has_negation, negation_words_found, negated_terms)

    Examples:
        "restaurants not fast food" → True, ['not'], ['fast food']
        "vegan without gluten" → True, ['without'], ['gluten']
    """
    words = [t.get('word', '').lower() for t in terms]
    negation_found = []
    negated_terms = []

    for i, word in enumerate(words):
        if word in NEGATION_WORDS:
            negation_found.append(word)
            # The word(s) after the negation are what's being excluded
            if i < len(words) - 1:
                # Take up to 2 words after negation as the negated concept
                remaining = words[i + 1:i + 3]
                # Filter out stopwords from negated terms
                negated = [w for w in remaining
                          if w not in {'a', 'an', 'the', 'and', 'or'}]
                if negated:
                    negated_terms.append(' '.join(negated))

    return len(negation_found) > 0, negation_found, negated_terms


def _detect_price_signal(terms: List[Dict]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Detect price-related signals.
    Returns: (has_price, direction ('cheap'|'expensive'|None), price_word)
    """
    for term in terms:
        word = term.get('word', '').lower()
        if word in PRICE_CHEAP:
            return True, 'cheap', word
        if word in PRICE_EXPENSIVE:
            return True, 'expensive', word

    return False, None, None


def _detect_black_owned(query: str, terms: List[Dict]) -> bool:
    """
    Detect 'black owned' / 'black-owned' in query.
    Uses raw query string for phrase matching since WD may split the words.
    """
    query_lower = query.lower()
    for phrase in BLACK_OWNED_PHRASES:
        if phrase in query_lower:
            return True

    # Also check consecutive terms: "black" followed by "owned"
    words = [t.get('word', '').lower() for t in terms]
    for i in range(len(words) - 1):
        if words[i] == 'black' and words[i + 1] == 'owned':
            return True

    return False


def _disambiguate_domain(word: str, terms: List[Dict], word_index: int) -> Optional[str]:
    """
    Attempt to disambiguate an ambiguous word using surrounding context.
    Returns the likely domain or None if still ambiguous.

    Example: "soul" next to "food" → 'food', next to "music" → 'music'
    """
    if word not in AMBIGUOUS_WORDS:
        return None

    words = [t.get('word', '').lower() for t in terms]
    # Check 2 words before and after for context
    context_start = max(0, word_index - 2)
    context_end = min(len(words), word_index + 3)
    context_words = set(words[context_start:context_end]) - {word}

    # Score each domain by context overlap
    best_domain = None
    best_overlap = 0

    for domain_name, domain_set in ALL_DOMAINS.items():
        overlap = len(context_words & domain_set)
        if overlap > best_overlap:
            best_overlap = overlap
            best_domain = domain_name

    return best_domain if best_overlap > 0 else None


# =============================================================================
# QUERY MODE CLASSIFICATION
# =============================================================================

def _classify_query_mode(signals: Dict) -> str:
    """
    Classify the overall query mode based on detected signals.

    Modes (per bridge strategy blueprint):
        'answer'  - User wants a specific fact (question_word + entity)
        'browse'  - User wants a list of options (service, product, plural nouns, superlatives)
        'shop'    - User wants to buy something (product + buy action or price)
        'compare' - User wants to compare options (vs, compare, difference)
        'explore' - User wants to learn about a topic (general, no specific intent)
        'local'   - User wants nearby services/places (service + location)
    """
    # ── Compare mode (highest priority — explicit comparison intent)
    if signals.get('is_comparison_query'):
        return 'compare'

    # ── Shop mode (product + buy action or price signal)
    if signals.get('has_product_word') and (
        signals.get('action_type') == 'buy' or signals.get('has_price_signal')
    ):
        return 'shop'

    # ── Local mode (service + location/proximity signal)
    if signals.get('is_local_search'):
        return 'local'

    # ── Answer mode — question word present
    # Blueprint: "User wants ONE correct result. Question word + entity."
    # All question words (who, what, when, where, why, how) → answer mode
    # UNLESS the query also has browse signals (plural nouns, superlatives, quantity)
    if signals.get('has_question_word'):
        qw = signals.get('question_word', '')

        # Strong answer signals: who/when/where always → answer
        if qw in ('who', 'when', 'where'):
            return 'answer'

        # what + definition pattern → answer
        if qw == 'what' and signals.get('is_definition_query'):
            return 'answer'

        # what/how/why → answer IF we have entity or role signals
        # (indicates a specific factual question, not browsing)
        if qw in ('what', 'how', 'why'):
            has_entity = (
                signals.get('has_person') or
                signals.get('has_organization') or
                signals.get('has_role_word')
            )
            if has_entity:
                return 'answer'

            # what/how/why without entity but also without browse signals → answer
            has_browse_signals = (
                signals.get('has_plural_noun') or
                signals.get('has_superlative') or
                signals.get('has_quantity_word') or
                signals.get('has_service_word')
            )
            if not has_browse_signals:
                return 'answer'

        # which → answer if temporal + role
        if qw == 'which':
            if signals.get('has_temporal') and signals.get('has_role_word'):
                return 'answer'

        # Temporal + role without explicit question word covered below

    # ── Answer mode — temporal + role (even without question word)
    # "first president of morehouse" without "who" is still answer mode
    if signals.get('has_temporal') and signals.get('has_role_word'):
        return 'answer'

    # ── Browse mode — looking for lists/options
    if signals.get('has_service_word') or signals.get('has_plural_noun'):
        return 'browse'
    if signals.get('has_quantity_word') or signals.get('has_superlative'):
        return 'browse'

    # ── Default to explore
    return 'explore'


# =============================================================================
# MAIN SIGNAL EXTRACTION
# =============================================================================

def detect_intent(discovery_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all linguistic signals from word discovery output.

    Does NOT make decisions - only reports facts.
    The search bridge uses these signals to decide how to search.

    Args:
        discovery_result: Dict from process_query_optimized() or WordDiscovery.process()

    Returns:
        Same dict with 'signals' field added
    """
    query = discovery_result.get('query', '').lower().strip()
    terms = discovery_result.get('terms', [])
    ngrams = discovery_result.get('ngrams', [])
    category_summary = discovery_result.get('category_summary', {})
    sort_info = discovery_result.get('sort')
    stats = discovery_result.get('stats', {})

    # ─── Initialize signals dict ────────────────────────────────────────
    signals = {
        # Question signals
        'has_question_word': False,
        'question_word': None,
        'question_position': None,
        'has_question_starter': False,
        'question_starter': None,

        # Temporal signals
        'has_temporal': False,
        'temporal_direction': None,  # 'oldest' | 'newest'
        'temporal_word': None,

        # Proximity / Location signals
        'has_proximity': False,
        'proximity_word': None,
        'has_me_reference': False,
        'has_location_preposition': False,
        'location_preposition': None,
        'has_location': False,
        'location_terms': [],       # actual city/state terms found

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
        'action_type': None,  # 'find' | 'show' | 'learn' | 'buy' | 'create'
        'action_word': None,

        # Pattern signals
        'is_definition_query': False,
        'is_comparison_query': False,
        'comparison_items': [],

        # Negation signals
        'has_negation': False,
        'negation_words': [],
        'negated_terms': [],

        # Price signals
        'has_price_signal': False,
        'price_direction': None,  # 'cheap' | 'expensive'
        'price_word': None,

        # Rating signals
        'has_rating_signal': False,
        'rating_word': None,

        # Service signals
        'has_service_word': False,
        'service_words': [],
        'is_local_search': False,

        # Product signals
        'has_product_word': False,
        'product_words': [],

        # Role signals
        'has_role_word': False,
        'role_word': None,

        # Black-owned signal
        'has_black_owned': False,

        # Domain signals (which verticals are touched)
        'domains_detected': [],
        'primary_domain': None,

        # Entity signals (from terms AND category_summary)
        'has_person': category_summary.get('has_person', False),
        'has_location_entity': category_summary.get('has_location', False),
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

        # Unknown terms signal (from WD stats — used by bridge for semantic blend)
        'has_unknown_terms': stats.get('unknown_words', 0) > 0,
        'unknown_term_count': stats.get('unknown_words', 0),

        # Detected entities
        'detected_entities': [],

        # Sort signal (from word_discovery)
        'has_sort_signal': sort_info is not None,
        'sort_field': sort_info.get('field') if sort_info else None,
        'sort_order': sort_info.get('order') if sort_info else None,

        # Query mode (set after all signals are detected)
        'query_mode': 'explore',

        # Result expectation heuristics (set after all signals)
        'wants_single_result': False,
        'wants_multiple_results': False,

        # Intent complexity (count of active signals)
        'signal_count': 0,

        # Local search strength
        'local_search_strength': 'none',  # 'none' | 'weak' | 'strong'
    }

    # ─── Tracking vars ──────────────────────────────────────────────────
    noun_count = 0
    service_words_found = []
    product_words_found = []
    plural_nouns_found = []
    entities_found = []
    location_terms_found = []
    domain_scores = {name: 0 for name in ALL_DOMAINS}

    # =================================================================
    # SINGLE PASS THROUGH TERMS
    # =================================================================
    for i, term in enumerate(terms):
        word = term.get('word', '').lower()
        search_word = term.get('search_word', word).lower()
        pos = term.get('pos', '').lower()
        category = term.get('category', '').lower()
        rank = term.get('rank', 0)
        is_stopword = term.get('is_stopword', False) or category == 'stopword'

        position = 'start' if i == 0 else ('end' if i == len(terms) - 1 else 'middle')

        # ─── Location detection from term category ──────────────────
        if category in LOCATION_CATEGORIES:
            signals['has_location'] = True
            signals['has_location_entity'] = True
            location_terms_found.append({
                'word': search_word or word,
                'category': category,
                'rank': rank,
                'position': i,
            })

        # ─── Question word check ────────────────────────────────────
        if word in QUESTION_WORDS:
            signals['has_question_word'] = True
            signals['question_word'] = word
            signals['question_position'] = position

        # ─── Question starter check (only at start) ─────────────────
        if i == 0 and word in QUESTION_STARTERS:
            signals['has_question_starter'] = True
            signals['question_starter'] = word

        # ─── Temporal check ─────────────────────────────────────────
        if word in TEMPORAL_OLDEST:
            signals['has_temporal'] = True
            signals['temporal_direction'] = 'oldest'
            signals['temporal_word'] = word
        elif word in TEMPORAL_NEWEST:
            signals['has_temporal'] = True
            signals['temporal_direction'] = 'newest'
            signals['temporal_word'] = word

        # ─── Proximity check ────────────────────────────────────────
        if word in PROXIMITY_WORDS:
            signals['has_proximity'] = True
            signals['proximity_word'] = word

        if word in LOCATION_PREPOSITIONS:
            signals['has_location_preposition'] = True
            signals['location_preposition'] = word

        # ─── Me reference check ─────────────────────────────────────
        if word in ME_WORDS:
            signals['has_me_reference'] = True

        # ─── Superlative check ──────────────────────────────────────
        if word in SUPERLATIVE_WORDS:
            signals['has_superlative'] = True
            signals['superlative_word'] = word

        # ─── Quantity check ─────────────────────────────────────────
        if word in QUANTITY_WORDS:
            signals['has_quantity_word'] = True
            signals['quantity_word'] = word

        # ─── Action word check ──────────────────────────────────────
        if not signals['has_action_word']:
            if word in ACTION_FIND:
                signals['has_action_word'] = True
                signals['action_type'] = 'find'
                signals['action_word'] = word
            elif word in ACTION_BUY:
                signals['has_action_word'] = True
                signals['action_type'] = 'buy'
                signals['action_word'] = word
            elif word in ACTION_SHOW:
                signals['has_action_word'] = True
                signals['action_type'] = 'show'
                signals['action_word'] = word
            elif word in ACTION_LEARN:
                signals['has_action_word'] = True
                signals['action_type'] = 'learn'
                signals['action_word'] = word
            elif word in ACTION_CREATE:
                signals['has_action_word'] = True
                signals['action_type'] = 'create'
                signals['action_word'] = word

        # ─── Service word check ─────────────────────────────────────
        if word in SERVICE_WORDS or search_word in SERVICE_WORDS:
            signals['has_service_word'] = True
            service_words_found.append(word)

        # ─── Product word check ─────────────────────────────────────
        if word in PRODUCT_WORDS or search_word in PRODUCT_WORDS:
            signals['has_product_word'] = True
            product_words_found.append(word)

        # ─── Role word check ────────────────────────────────────────
        if word in ROLE_WORDS or search_word in ROLE_WORDS:
            signals['has_role_word'] = True
            if not signals['role_word']:
                signals['role_word'] = word

        # ─── Rating signal check ────────────────────────────────────
        if word in RATING_WORDS:
            signals['has_rating_signal'] = True
            signals['rating_word'] = word

        # ─── Comparison check ───────────────────────────────────────
        if word in COMPARISON_WORDS:
            signals['is_comparison_query'] = True

        # ─── Domain scoring ─────────────────────────────────────────
        # Skip ambiguous words — they need context
        if word in AMBIGUOUS_WORDS:
            resolved_domain = _disambiguate_domain(word, terms, i)
            if resolved_domain:
                domain_scores[resolved_domain] += 1
        elif not is_stopword:
            for domain_name, domain_set in ALL_DOMAINS.items():
                if word in domain_set:
                    domain_scores[domain_name] += 1

        # ─── POS analysis ───────────────────────────────────────────
        if pos in {'noun', 'proper_noun'}:
            noun_count += 1
            if i == 0:
                signals['starts_with_noun'] = True
            if i == len(terms) - 1:
                signals['ends_with_noun'] = True

            if word.endswith(PLURAL_ENDINGS) and len(word) > 3:
                signals['has_plural_noun'] = True
                plural_nouns_found.append(word)

        if pos == 'adjective':
            signals['has_adjective'] = True

        if pos in {'verb', 'be', 'auxiliary', 'modal'}:
            signals['has_verb'] = True

        # ─── Entity detection (high-rank proper nouns) ──────────────
        if pos == 'proper_noun' and rank > 100 and category != 'stopword':
            entities_found.append({
                'word': search_word or word,
                'category': category,
                'rank': rank,
            })

    # =================================================================
    # POST-LOOP ANALYSIS
    # =================================================================

    # Store collected lists
    signals['service_words'] = service_words_found
    signals['product_words'] = product_words_found
    signals['plural_nouns'] = plural_nouns_found
    signals['location_terms'] = location_terms_found

    # All nouns check
    non_stopword_terms = [t for t in terms if t.get('category', '').lower() != 'stopword']
    if non_stopword_terms and noun_count == len(non_stopword_terms):
        signals['all_nouns'] = True

    # ─── Definition pattern ─────────────────────────────────────────
    signals['is_definition_query'] = _check_definition_pattern(terms)

    # ─── Proximity pattern ──────────────────────────────────────────
    has_prox, prox_word = _check_proximity_pattern(terms)
    if has_prox:
        signals['has_proximity'] = True
        signals['proximity_word'] = prox_word

    # ─── Me pattern ─────────────────────────────────────────────────
    if _check_me_pattern(terms):
        signals['has_me_reference'] = True

    # ─── Comparison pattern (with ngram awareness) ──────────────────
    has_comp, comp_items = _check_comparison_pattern(terms, ngrams)
    if has_comp:
        signals['is_comparison_query'] = True
        signals['comparison_items'] = comp_items

    # ─── Negation detection ─────────────────────────────────────────
    has_neg, neg_words, negated = _check_negation_context(terms)
    signals['has_negation'] = has_neg
    signals['negation_words'] = neg_words
    signals['negated_terms'] = negated

    # ─── Price signal detection ─────────────────────────────────────
    has_price, price_dir, price_word = _detect_price_signal(terms)
    signals['has_price_signal'] = has_price
    signals['price_direction'] = price_dir
    signals['price_word'] = price_word

    # ─── Black-owned detection ──────────────────────────────────────
    signals['has_black_owned'] = _detect_black_owned(query, terms)

    # ─── Local search detection ─────────────────────────────────────
    # Blueprint: service_word + location signal = local
    # Location signal = has_location OR has_proximity OR has_me_reference
    has_location_signal = (
        signals['has_location'] or
        signals['has_location_entity'] or
        signals['has_me_reference']
    )
    has_proximity_signal = (
        signals['has_proximity'] or
        signals['has_me_reference'] or
        has_location_signal
    )

    signals['is_local_search'] = (
        signals['has_service_word'] and has_proximity_signal
    )

    # Local search strength
    if signals['is_local_search']:
        strength_score = 0
        if signals['has_service_word']:
            strength_score += 1
        if signals['has_location']:
            strength_score += 1
        if signals['has_proximity']:
            strength_score += 1
        if signals['has_me_reference']:
            strength_score += 1

        signals['local_search_strength'] = 'strong' if strength_score >= 3 else 'weak'

    # ─── Domain detection ───────────────────────────────────────────
    active_domains = [
        (name, score) for name, score in domain_scores.items() if score > 0
    ]
    active_domains.sort(key=lambda x: -x[1])
    signals['domains_detected'] = [d[0] for d in active_domains]
    signals['primary_domain'] = active_domains[0][0] if active_domains else None

    # ─── Ngram entity extraction ────────────────────────────────────
    for ngram in ngrams:
        ngram_text = ngram.get('ngram', ngram.get('phrase', ''))
        ng_category = ngram.get('category', '').lower()
        ng_rank = ngram.get('rank', 0)

        if ng_category and ngram_text:
            entities_found.append({
                'phrase': ngram_text,
                'category': ng_category,
                'rank': ng_rank,
                'is_ngram': True,
            })

            # Check if ngram has location category
            if ng_category in LOCATION_CATEGORIES:
                signals['has_location'] = True
                signals['has_location_entity'] = True

    # Sort entities by rank
    entities_found.sort(key=lambda x: -x.get('rank', 0))
    signals['detected_entities'] = entities_found

    # ─── Signal count (complexity) ──────────────────────────────────
    signal_keys = [
        'has_question_word', 'has_temporal', 'has_proximity',
        'has_superlative', 'has_quantity_word', 'has_action_word',
        'has_service_word', 'has_product_word', 'has_role_word',
        'has_negation', 'has_price_signal', 'has_rating_signal',
        'has_location', 'has_me_reference', 'is_comparison_query',
        'has_plural_noun', 'has_adjective', 'has_black_owned',
    ]
    signals['signal_count'] = sum(1 for k in signal_keys if signals.get(k))

    # ─── Query mode classification ──────────────────────────────────
    signals['query_mode'] = _classify_query_mode(signals)

    # ─── Result expectation heuristics ──────────────────────────────
    # wants_single_result: user likely wants ONE specific answer
    signals['wants_single_result'] = _compute_wants_single(signals)
    # wants_multiple_results: user likely wants a list
    signals['wants_multiple_results'] = _compute_wants_multiple(signals)

    # ─── Add to result ──────────────────────────────────────────────
    discovery_result['signals'] = signals

    return discovery_result


def _compute_wants_single(signals: Dict) -> bool:
    """
    Heuristic: Does this query likely want a single/specific result?

    Per blueprint: answer mode queries want single results.
    Also: definition queries, temporal+role combos.
    """
    if signals.get('query_mode') == 'answer':
        return True

    if signals.get('is_definition_query'):
        return True

    if signals.get('has_question_word') and signals.get('question_word') in {'who', 'what', 'when', 'where'}:
        if signals.get('has_temporal') or signals.get('has_role_word'):
            return True

    if signals.get('has_temporal') and signals.get('has_role_word'):
        return True

    return False


def _compute_wants_multiple(signals: Dict) -> bool:
    """
    Heuristic: Does this query likely want multiple results?

    Per blueprint: browse/local/shop modes want multiple results.
    Also: plural nouns, quantity words, service words, superlatives.
    """
    if signals.get('query_mode') in ('browse', 'local', 'shop'):
        return True

    if signals.get('has_plural_noun'):
        return True
    if signals.get('has_quantity_word'):
        return True
    if signals.get('has_service_word'):
        return True
    if signals.get('has_product_word'):
        return True
    if signals.get('has_superlative'):
        return True
    if signals.get('all_nouns') and not signals.get('has_question_word'):
        return True

    return False


# =============================================================================
# CONVENIENCE FUNCTIONS (for the bridge to use)
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
    """Check if query wants a single/specific result."""
    signals = get_signals(discovery_result)
    return signals.get('wants_single_result', False)


def wants_multiple_results(discovery_result: Dict[str, Any]) -> bool:
    """Check if query wants multiple results."""
    signals = get_signals(discovery_result)
    return signals.get('wants_multiple_results', False)


def get_query_mode(discovery_result: Dict[str, Any]) -> str:
    """Get the classified query mode."""
    signals = get_signals(discovery_result)
    return signals.get('query_mode', 'explore')


def get_detected_entities(discovery_result: Dict[str, Any]) -> List[Dict]:
    """Get list of detected entities, sorted by rank."""
    signals = get_signals(discovery_result)
    return signals.get('detected_entities', [])


def get_temporal_direction(discovery_result: Dict[str, Any]) -> Optional[str]:
    """Get temporal direction if present ('oldest' or 'newest')."""
    signals = get_signals(discovery_result)
    return signals.get('temporal_direction')


def get_location_terms(discovery_result: Dict[str, Any]) -> List[Dict]:
    """Get location terms detected from term categories."""
    signals = get_signals(discovery_result)
    return signals.get('location_terms', [])


def get_active_domains(discovery_result: Dict[str, Any]) -> List[str]:
    """Get list of active domain verticals."""
    signals = get_signals(discovery_result)
    return signals.get('domains_detected', [])


def get_primary_domain(discovery_result: Dict[str, Any]) -> Optional[str]:
    """Get the primary domain vertical."""
    signals = get_signals(discovery_result)
    return signals.get('primary_domain')


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
    print(f"🎮 Query Mode: {signals.get('query_mode', 'unknown')}")
    print(f"🔢 Signal Count: {signals.get('signal_count', 0)}")
    print(f"🎯 Wants Single: {signals.get('wants_single_result')} | Wants Multiple: {signals.get('wants_multiple_results')}")

    print("\n" + "─" * 70)
    print("❓ QUESTION SIGNALS")
    print("─" * 70)
    print(f"  has_question_word: {signals.get('has_question_word')} → {signals.get('question_word')} ({signals.get('question_position')})")
    print(f"  has_question_starter: {signals.get('has_question_starter')} → {signals.get('question_starter')}")
    print(f"  is_definition_query: {signals.get('is_definition_query')}")

    print("\n" + "─" * 70)
    print("⏰ TEMPORAL SIGNALS")
    print("─" * 70)
    print(f"  has_temporal: {signals.get('has_temporal')} → {signals.get('temporal_word')}")
    print(f"  temporal_direction: {signals.get('temporal_direction')}")
    print(f"  has_sort_signal: {signals.get('has_sort_signal')} → {signals.get('sort_field')} {signals.get('sort_order')}")

    print("\n" + "─" * 70)
    print("📍 LOCATION SIGNALS")
    print("─" * 70)
    print(f"  has_location: {signals.get('has_location')}")
    print(f"  has_location_entity: {signals.get('has_location_entity')}")
    print(f"  location_terms: {signals.get('location_terms')}")
    print(f"  has_proximity: {signals.get('has_proximity')} → {signals.get('proximity_word')}")
    print(f"  has_me_reference: {signals.get('has_me_reference')}")
    print(f"  is_local_search: {signals.get('is_local_search')}")
    print(f"  local_search_strength: {signals.get('local_search_strength')}")

    print("\n" + "─" * 70)
    print("🏪 SERVICE / PRODUCT SIGNALS")
    print("─" * 70)
    print(f"  has_service_word: {signals.get('has_service_word')} → {signals.get('service_words')}")
    print(f"  has_product_word: {signals.get('has_product_word')} → {signals.get('product_words')}")
    print(f"  has_black_owned: {signals.get('has_black_owned')}")

    print("\n" + "─" * 70)
    print("📈 SUPERLATIVE / QUANTITY / RATING")
    print("─" * 70)
    print(f"  has_superlative: {signals.get('has_superlative')} → {signals.get('superlative_word')}")
    print(f"  has_quantity_word: {signals.get('has_quantity_word')} → {signals.get('quantity_word')}")
    print(f"  has_plural_noun: {signals.get('has_plural_noun')} → {signals.get('plural_nouns')}")
    print(f"  has_rating_signal: {signals.get('has_rating_signal')} → {signals.get('rating_word')}")

    print("\n" + "─" * 70)
    print("💰 PRICE SIGNALS")
    print("─" * 70)
    print(f"  has_price_signal: {signals.get('has_price_signal')}")
    print(f"  price_direction: {signals.get('price_direction')} → {signals.get('price_word')}")

    print("\n" + "─" * 70)
    print("🚫 NEGATION SIGNALS")
    print("─" * 70)
    print(f"  has_negation: {signals.get('has_negation')}")
    print(f"  negation_words: {signals.get('negation_words')}")
    print(f"  negated_terms: {signals.get('negated_terms')}")

    print("\n" + "─" * 70)
    print("🎬 ACTION SIGNALS")
    print("─" * 70)
    print(f"  has_action_word: {signals.get('has_action_word')} → {signals.get('action_word')}")
    print(f"  action_type: {signals.get('action_type')}")

    print("\n" + "─" * 70)
    print("👔 ROLE SIGNALS")
    print("─" * 70)
    print(f"  has_role_word: {signals.get('has_role_word')} → {signals.get('role_word')}")

    print("\n" + "─" * 70)
    print("⚖️ COMPARISON SIGNALS")
    print("─" * 70)
    print(f"  is_comparison_query: {signals.get('is_comparison_query')}")
    print(f"  comparison_items: {signals.get('comparison_items')}")

    print("\n" + "─" * 70)
    print("🌐 DOMAIN SIGNALS")
    print("─" * 70)
    print(f"  primary_domain: {signals.get('primary_domain')}")
    print(f"  domains_detected: {signals.get('domains_detected')}")

    print("\n" + "─" * 70)
    print("🏷️ ENTITY SIGNALS")
    print("─" * 70)
    print(f"  has_person: {signals.get('has_person')}")
    print(f"  has_organization: {signals.get('has_organization')}")
    print(f"  has_location_entity: {signals.get('has_location_entity')}")
    print(f"  has_media: {signals.get('has_media')}")

    print("\n" + "─" * 70)
    print("🔤 STRUCTURE SIGNALS")
    print("─" * 70)
    print(f"  all_nouns: {signals.get('all_nouns')}")
    print(f"  starts_with_noun: {signals.get('starts_with_noun')}")
    print(f"  ends_with_noun: {signals.get('ends_with_noun')}")
    print(f"  has_adjective: {signals.get('has_adjective')}")
    print(f"  has_verb: {signals.get('has_verb')}")
    print(f"  has_unknown_terms: {signals.get('has_unknown_terms')} ({signals.get('unknown_term_count')})")

    print("\n" + "─" * 70)
    print("🎯 DETECTED ENTITIES")
    print("─" * 70)
    entities = signals.get('detected_entities', [])
    if entities:
        for ent in entities[:8]:
            label = ent.get('phrase', ent.get('word', '?'))
            is_ng = "📎" if ent.get('is_ngram') else "📌"
            print(f"  {is_ng} '{label}' → {ent.get('category')} (rank: {ent.get('rank')})")
    else:
        print("  (none detected)")

    print("\n" + "=" * 70)


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TESTING SIGNAL DETECTION (All Verticals)")
    print("=" * 70)

    test_cases = [
        # Local service search
        {
            'name': 'Local restaurant search',
            'query': 'restaurants in atlanta',
            'terms': [
                {'word': 'restaurants', 'search_word': 'restaurants', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'in', 'search_word': 'in', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'atlanta', 'search_word': 'atlanta', 'pos': 'proper_noun', 'category': 'US City', 'rank': 850, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'is_local_search': True,
                'has_service_word': True,
                'has_location': True,
                'query_mode': 'local',
                'wants_multiple_results': True,
            },
        },
        # Product search
        {
            'name': 'Product shopping',
            'query': 'best running shoes under 100',
            'terms': [
                {'word': 'best', 'search_word': 'best', 'pos': 'adjective', 'category': 'Keyword', 'rank': 300, 'is_stopword': False},
                {'word': 'running', 'search_word': 'running', 'pos': 'adjective', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
                {'word': 'shoes', 'search_word': 'shoes', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'under', 'search_word': 'under', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': '100', 'search_word': '100', 'pos': 'noun', 'category': 'Keyword', 'rank': 0, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_product_word': True,
                'has_superlative': True,
                'has_price_signal': True,
                'query_mode': 'shop',  # Blueprint: product + price → shop
            },
        },
        # Question / answer
        {
            'name': 'Factual question (who)',
            'query': 'who was the first president of morehouse',
            'terms': [
                {'word': 'who', 'search_word': 'who', 'pos': 'wh_pronoun', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'was', 'search_word': 'was', 'pos': 'verb', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'the', 'search_word': 'the', 'pos': 'article', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'first', 'search_word': 'first', 'pos': 'adjective', 'category': 'Keyword', 'rank': 500, 'is_stopword': False},
                {'word': 'president', 'search_word': 'president', 'pos': 'noun', 'category': 'Keyword', 'rank': 600, 'is_stopword': False},
                {'word': 'of', 'search_word': 'of', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'morehouse', 'search_word': 'morehouse', 'pos': 'proper_noun', 'category': 'Organization', 'rank': 800, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {'has_business': True},
            'sort': {'field': 'time_period_start', 'order': 'asc'},
            'stats': {'unknown_words': 0},
            'expected': {
                'has_question_word': True,
                'has_temporal': True,
                'has_role_word': True,
                'query_mode': 'answer',
                'wants_single_result': True,
            },
        },
        # Where question
        {
            'name': 'Where question',
            'query': 'where is africa',
            'terms': [
                {'word': 'where', 'search_word': 'where', 'pos': 'wh_pronoun', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'is', 'search_word': 'is', 'pos': 'verb', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'africa', 'search_word': 'africa', 'pos': 'proper_noun', 'category': 'continent', 'rank': 900, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {'has_location': True},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_question_word': True,
                'question_word': 'where',
                'query_mode': 'answer',
                'wants_single_result': True,
            },
        },
        # What question (with entity → answer)
        {
            'name': 'What question with entity',
            'query': 'what is the largest hbcu',
            'terms': [
                {'word': 'what', 'search_word': 'what', 'pos': 'wh_pronoun', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'is', 'search_word': 'is', 'pos': 'verb', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'the', 'search_word': 'the', 'pos': 'article', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'largest', 'search_word': 'largest', 'pos': 'adjective', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
                {'word': 'hbcu', 'search_word': 'hbcu', 'pos': 'proper_noun', 'category': 'Organization', 'rank': 700, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {'has_business': True},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_question_word': True,
                'question_word': 'what',
                'query_mode': 'answer',
                'wants_single_result': True,
            },
        },
        # How question (general → answer)
        {
            'name': 'How question general',
            'query': 'how was jazz created',
            'terms': [
                {'word': 'how', 'search_word': 'how', 'pos': 'wh_pronoun', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'was', 'search_word': 'was', 'pos': 'verb', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'jazz', 'search_word': 'jazz', 'pos': 'noun', 'category': 'Keyword', 'rank': 600, 'is_stopword': False},
                {'word': 'created', 'search_word': 'created', 'pos': 'verb', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_question_word': True,
                'question_word': 'how',
                'query_mode': 'answer',
            },
        },
        # Comparison query
        {
            'name': 'Comparison',
            'query': 'spelman vs morehouse',
            'terms': [
                {'word': 'spelman', 'search_word': 'spelman', 'pos': 'proper_noun', 'category': 'Organization', 'rank': 700, 'is_stopword': False},
                {'word': 'vs', 'search_word': 'vs', 'pos': 'noun', 'category': 'Keyword', 'rank': 0, 'is_stopword': False},
                {'word': 'morehouse', 'search_word': 'morehouse', 'pos': 'proper_noun', 'category': 'Organization', 'rank': 800, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {'has_business': True},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'is_comparison_query': True,
                'query_mode': 'compare',
            },
        },
        # Real estate search
        {
            'name': 'Real estate',
            'query': 'affordable apartments in atlanta',
            'terms': [
                {'word': 'affordable', 'search_word': 'affordable', 'pos': 'adjective', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
                {'word': 'apartments', 'search_word': 'apartments', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'in', 'search_word': 'in', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'atlanta', 'search_word': 'atlanta', 'pos': 'proper_noun', 'category': 'US City', 'rank': 850, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'is_local_search': True,
                'has_price_signal': True,
                'price_direction': 'cheap',
                'query_mode': 'local',
            },
        },
        # Negation query
        {
            'name': 'Negation',
            'query': 'vegan restaurants not fast food',
            'terms': [
                {'word': 'vegan', 'search_word': 'vegan', 'pos': 'adjective', 'category': 'Keyword', 'rank': 300, 'is_stopword': False},
                {'word': 'restaurants', 'search_word': 'restaurants', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'not', 'search_word': 'not', 'pos': 'negation', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'fast', 'search_word': 'fast', 'pos': 'adjective', 'category': 'Keyword', 'rank': 100, 'is_stopword': False},
                {'word': 'food', 'search_word': 'food', 'pos': 'noun', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_negation': True,
                'has_service_word': True,
            },
        },
        # Travel query
        {
            'name': 'Travel browse',
            'query': 'best beaches to visit',
            'terms': [
                {'word': 'best', 'search_word': 'best', 'pos': 'adjective', 'category': 'Keyword', 'rank': 300, 'is_stopword': False},
                {'word': 'beaches', 'search_word': 'beaches', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'to', 'search_word': 'to', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'visit', 'search_word': 'visit', 'pos': 'verb', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_superlative': True,
                'has_plural_noun': True,
                'query_mode': 'browse',
            },
        },
        # Entity lookup
        {
            'name': 'Entity lookup',
            'query': 'billie holiday',
            'terms': [
                {'word': 'billie', 'search_word': 'billie', 'pos': 'proper_noun', 'category': 'Person', 'rank': 700, 'is_stopword': False},
                {'word': 'holiday', 'search_word': 'holiday', 'pos': 'proper_noun', 'category': 'Person', 'rank': 500, 'is_stopword': False},
            ],
            'ngrams': [
                {'ngram': 'billie holiday', 'category': 'Person', 'rank': 900}
            ],
            'category_summary': {'has_person': True},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_person': True,
                'query_mode': 'explore',
            },
        },
        # Black-owned query
        {
            'name': 'Black-owned search',
            'query': 'black owned restaurants in houston',
            'terms': [
                {'word': 'black', 'search_word': 'black', 'pos': 'adjective', 'category': 'Keyword', 'rank': 300, 'is_stopword': False},
                {'word': 'owned', 'search_word': 'owned', 'pos': 'verb', 'category': 'Keyword', 'rank': 100, 'is_stopword': False},
                {'word': 'restaurants', 'search_word': 'restaurants', 'pos': 'noun', 'category': 'Keyword', 'rank': 400, 'is_stopword': False},
                {'word': 'in', 'search_word': 'in', 'pos': 'preposition', 'category': 'stopword', 'rank': 0, 'is_stopword': True},
                {'word': 'houston', 'search_word': 'houston', 'pos': 'proper_noun', 'category': 'US City', 'rank': 850, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 0},
            'expected': {
                'has_black_owned': True,
                'is_local_search': True,
                'has_service_word': True,
                'query_mode': 'local',
            },
        },
        # Unknown terms query
        {
            'name': 'Unknown terms',
            'query': 'xylophone makers',
            'terms': [
                {'word': 'xylophone', 'search_word': 'xylophone', 'pos': 'noun', 'category': 'Keyword', 'rank': 100, 'is_stopword': False},
                {'word': 'makers', 'search_word': 'makers', 'pos': 'noun', 'category': 'Keyword', 'rank': 200, 'is_stopword': False},
            ],
            'ngrams': [],
            'category_summary': {},
            'sort': None,
            'stats': {'unknown_words': 2},
            'expected': {
                'has_unknown_terms': True,
                'unknown_term_count': 2,
            },
        },
    ]

    passed = 0
    failed = 0

    for case in test_cases:
        print(f"\n{'─' * 70}")
        print(f"TEST: {case['name']}")
        print(f"Query: '{case['query']}'")
        print('─' * 70)

        result = detect_intent(case)
        signals = result.get('signals', {})

        # Check expected values
        all_match = True
        for key, expected_value in case.get('expected', {}).items():
            actual = signals.get(key)
            match = actual == expected_value
            icon = "✅" if match else "❌"
            print(f"  {icon} {key}: expected={expected_value}, got={actual}")
            if not match:
                all_match = False

        if all_match:
            passed += 1
            print(f"  ✅ PASSED")
        else:
            failed += 1
            print(f"  ❌ FAILED")

        # Print key signals
        print(f"  Mode: {signals.get('query_mode')} | Signals: {signals.get('signal_count')}")
        print(f"  Domains: {signals.get('domains_detected', [])}")
        print(f"  Wants Single: {signals.get('wants_single_result')} | Wants Multiple: {signals.get('wants_multiple_results')}")

    print(f"\n{'═' * 70}")
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print(f"{'═' * 70}")