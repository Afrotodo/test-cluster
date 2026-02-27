# """
# searchengine/address_maps.py
# ==============================
# Data-driven address map feature for AfroTodo search.

# Shows map cards ONLY when search results contain business location data
# (latitude/longitude fields from your Redis hash OR location_geopoint from
# Typesense). No address guessing, no geocoding, no external API calls.

# INTEGRATION (2 lines in views.py):

#     from .address_maps import process_address_maps

#     # After section 14, before section 15:
#     map_data = process_address_maps(request, results)

#     # In context dict:
#     context = { ..., **map_data, ... }

# REMOVAL:
#     Delete this file + those 2 lines. Done.

# DEPENDENCIES:
#     None. No geopy, no external APIs.
# """

# import urllib.parse
# import logging

# logger = logging.getLogger(__name__)


# # ═══════════════════════════════════════════════════════════════════
# # PUBLIC API — the only function your view calls
# # ═══════════════════════════════════════════════════════════════════

# def process_address_maps(request, results=None):
#     """
#     Scan search results for location data. Build map context if found.

#     Args:
#         request: Django HttpRequest
#         results: list of search result dicts from your pipeline

#     Returns:
#         dict with keys (always safe to ** unpack):
#             address_map     - inline card data for first located result, or None
#             map_view_data   - full map page data (when ?view=map), or None
#             show_map_view   - True when ?view=map
#     """
#     view_mode = request.GET.get('view', '').strip()
#     lat_param = request.GET.get('lat')
#     lng_param = request.GET.get('lng')
#     query = request.GET.get('query', '').strip()

#     output = {
#         'address_map': None,
#         'map_view_data': None,
#         'show_map_view': False,
#     }

#     # ── FULL MAP PAGE (?view=map) ────────────────────────────────
#     if view_mode == 'map':
#         output['show_map_view'] = True

#         if lat_param and lng_param:
#             try:
#                 lat_f = float(lat_param)
#                 lng_f = float(lng_param)
#                 nearme = request.GET.get('nearme', '')
#                 title = request.GET.get('title', '') or query or 'Location'
#                 address = request.GET.get('address', '')
#                 city = request.GET.get('city', '')
#                 state = request.GET.get('state', '')
#                 phone = request.GET.get('phone', '')
#                 hours = request.GET.get('hours', '')
#                 website = request.GET.get('website', '')
#                 category_sub = request.GET.get('category_sub', '')

#                 location_line = ', '.join(filter(None, [city, state]))

#                 output['map_view_data'] = {
#                     'lat': lat_f,
#                     'lng': lng_f,
#                     'short_address': title,
#                     'address': address,
#                     'location_line': location_line or address,
#                     'city': city,
#                     'state': state,
#                     'phone': phone,
#                     'hours': hours,
#                     'website': website,
#                     'category_sub': category_sub,
#                     'is_nearme': bool(nearme),
#                     **_build_urls(lat_f, lng_f, query),
#                 }
#             except (ValueError, TypeError):
#                 output['map_view_data'] = {
#                     'error': True,
#                     'error_message': 'Invalid coordinates. Could not display map.',
#                 }
#         else:
#             output['map_view_data'] = {
#                 'error': True,
#                 'error_message': 'No location data provided.',
#             }
#         return output

#     # ── INLINE MAP CARD (from search results) ────────────────────
#     if results:
#         # DEBUG: see what fields results actually have
#         print(f"🗺️ MAP DEBUG: {len(results)} results")
#         print(f"🗺️ MAP DEBUG: First result keys: {list(results[0].keys())}")
#         print(f"🗺️ MAP DEBUG: location dict = {results[0].get('location')}")
#         print(f"🗺️ MAP DEBUG: lat = {results[0].get('lat')}, latitude = {results[0].get('latitude')}")
#         print(f"🗺️ MAP DEBUG: location_geopoint = {results[0].get('location_geopoint')}")
#         print(f"🗺️ MAP DEBUG: location_address = {results[0].get('location_address')}")
#         print(f"🗺️ MAP DEBUG: document_title = {results[0].get('document_title')}")

#         # Enrich all results with map links
#         _enrich_results(results)

#         # Find the FIRST result with coordinates → build inline card
#         for item in results:
#             location = _extract_location(item)
#             if location:
#                 output['address_map'] = {
#                 **location,
#                 'id': item.get('id'),  # ← ADD THIS
#                 **_build_urls(location['lat'], location['lng'], query),
#                 }
#                 break  # Only show one inline card

#     return output


# # ═══════════════════════════════════════════════════════════════════
# # PRIVATE IMPLEMENTATION
# # ═══════════════════════════════════════════════════════════════════

# def _extract_coords(item):
#     """
#     Extract (lat, lng) floats from a result dict.

#     Supports:
#     - Redis hash fields: latitude/lat, longitude/lng
#     - Typesense geopoint (top-level): location_geopoint [lat, lng]
#     - Nested location dict from format_result: location.geopoint

#     Returns (lat_f, lng_f) or (None, None).
#     """
#     lat = item.get('latitude') or item.get('lat')
#     lng = item.get('longitude') or item.get('lng')

#     # Typesense geopoint support (top-level — legacy/Redis path)
#     if not lat or not lng:
#         geopoint = item.get('location_geopoint')
#         if geopoint and isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
#             lat, lng = geopoint[0], geopoint[1]

#     # Nested location dict from format_result (Typesense bridge path)
#     if not lat or not lng:
#         loc = item.get('location', {})
#         if isinstance(loc, dict):
#             geopoint = loc.get('geopoint')
#             if geopoint:
#                 if isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
#                     lat, lng = geopoint[0], geopoint[1]
#                 elif isinstance(geopoint, str) and ',' in geopoint:
#                     parts = geopoint.split(',')
#                     if len(parts) == 2:
#                         lat, lng = parts[0].strip(), parts[1].strip()

#     if not lat or not lng:
#         return None, None

#     try:
#         lat_f = float(lat)
#         lng_f = float(lng)
#     except (ValueError, TypeError):
#         return None, None

#     if not (-90 <= lat_f <= 90 and -180 <= lng_f <= 180):
#         return None, None

#     return lat_f, lng_f


# def _extract_location(item):
#     """
#     Extract location data from a single search result dict.
#     Returns a location dict if coordinates found, else None.

#     Supports both Redis hash fields, Typesense document fields,
#     and the nested location dict from format_result.
#     """
#     lat_f, lng_f = _extract_coords(item)
#     if lat_f is None:
#         return None

#     # Nested location dict from format_result
#     loc = item.get('location', {}) if isinstance(item.get('location'), dict) else {}

#     # Support Redis fields, top-level Typesense fields, AND nested location dict
#     display = (item.get('display')
#                or item.get('document_title')
#                or item.get('title')
#                or item.get('term', ''))

#     address = (item.get('address')
#                or item.get('location_address', '')
#                or loc.get('address', ''))

#     city = (item.get('city')
#             or item.get('location_city', '')
#             or loc.get('city', ''))

#     state = (item.get('state')
#              or item.get('location_state', '')
#              or loc.get('state', ''))

#     zip_code = (item.get('zip')
#                 or item.get('location_zip', '')
#                 or loc.get('zip', ''))

#     phone = (item.get('phone')
#              or item.get('service_phone', '')
#              or item.get('service_phone', ''))

#     hours = (item.get('hours')
#              or item.get('service_hours', ''))

#     website = (item.get('website')
#                or item.get('service_website', ''))

#     category_sub = (item.get('category_sub')
#                     or item.get('document_category', '')
#                     or loc.get('category', ''))

#     description = (item.get('description')
#                    or item.get('document_description', ''))

#     location_line = address or ', '.join(filter(None, [city, state, zip_code]))

#     return {
#         'lat': lat_f,
#         'lng': lng_f,
#         'short_address': display,
#         'address': address,
#         'location_line': location_line,
#         'city': city,
#         'state': state,
#         'zip': zip_code,
#         'phone': phone,
#         'hours': hours,
#         'website': website,
#         'category_sub': category_sub,
#         'description': description,
#     }


# def _enrich_results(results):
#     """Add map_url and has_map to results that have location data. Mutates in place."""
#     for item in results:
#         lat_f, lng_f = _extract_coords(item)
#         if lat_f is None:
#             continue

#         # Nested location dict from format_result
#         loc = item.get('location', {}) if isinstance(item.get('location'), dict) else {}

#         params = {
#             'view': 'map',
#             'lat': str(lat_f),
#             'lng': str(lng_f),
#             'title': (item.get('display')
#                       or item.get('document_title')
#                       or item.get('title', '')),
#             'address': (item.get('address')
#                         or item.get('location_address', '')
#                         or loc.get('address', '')),
#             'city': (item.get('city')
#                      or item.get('location_city', '')
#                      or loc.get('city', '')),
#             'state': (item.get('state')
#                       or item.get('location_state', '')
#                       or loc.get('state', '')),
#             'phone': (item.get('phone')
#                       or item.get('service_phone', '')),
#             'hours': (item.get('hours')
#                       or item.get('service_hours', '')),
#             'website': (item.get('website')
#                         or item.get('service_website', '')),
#             'category_sub': (item.get('category_sub')
#                              or item.get('document_category', '')),
#         }
#         # Remove empty params to keep URL clean
#         params = {k: v for k, v in params.items() if v}
#         item['map_url'] = f"/search/?{urllib.parse.urlencode(params)}"
#         item['has_map'] = True


# def _build_urls(lat, lng, query=''):
#     """Build Google Maps external URLs + internal expand URL."""
#     params = {'view': 'map', 'lat': str(lat), 'lng': str(lng)}
#     if query:
#         params['query'] = query
#     return {
#         'directions_url': f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}",
#         'share_url': f"https://www.google.com/maps/search/?api=1&query={lat},{lng}",
#         'streetview_url': f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lng}",
#         'full_map_url': f"/search/?{urllib.parse.urlencode(params)}",
#     }

"""
searchengine/address_maps.py
==============================
Data-driven address map feature for AfroTodo search.

Shows map cards ONLY when search results contain business location data
(latitude/longitude fields from your Redis hash OR location_geopoint from
Typesense). No address guessing, no geocoding, no external API calls.

INTEGRATION (2 lines in views.py):

    from .address_maps import process_address_maps

    # After section 14, before section 15:
    map_data = process_address_maps(request, results)

    # In context dict:
    context = { ..., **map_data, ... }

REMOVAL:
    Delete this file + those 2 lines. Done.

DEPENDENCIES:
    None. No geopy, no external APIs.

TEMPLATE VARIABLES:
    address_map        - First located result (inline card), or None
    map_locations      - List of ALL results with geopoints (for multi-pin map)
    map_location_count - Number of results with geopoints
    map_view_data      - Full map page data (when ?view=map), or None
    show_map_view      - True when ?view=map
"""

import urllib.parse
import logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# PUBLIC API — the only function your view calls
# ═══════════════════════════════════════════════════════════════════

def process_address_maps(request, results=None):
    """
    Scan search results for location data. Build map context if found.

    Args:
        request: Django HttpRequest
        results: list of search result dicts from your pipeline

    Returns:
        dict with keys (always safe to ** unpack):
            address_map        - inline card data for first located result, or None
            map_locations      - list of ALL results with geopoints
            map_location_count - count of results with geopoints
            map_view_data      - full map page data (when ?view=map), or None
            show_map_view      - True when ?view=map
    """
    view_mode = request.GET.get('view', '').strip()
    lat_param = request.GET.get('lat')
    lng_param = request.GET.get('lng')
    query = request.GET.get('query', '').strip()

    output = {
        'address_map': None,
        'map_locations': [],
        'map_location_count': 0,
        'map_view_data': None,
        'show_map_view': False,
    }

    # ── FULL MAP PAGE (?view=map) ────────────────────────────────
    if view_mode == 'map':
        output['show_map_view'] = True

        if lat_param and lng_param:
            try:
                lat_f = float(lat_param)
                lng_f = float(lng_param)
                nearme = request.GET.get('nearme', '')
                title = request.GET.get('title', '') or query or 'Location'
                address = request.GET.get('address', '')
                city = request.GET.get('city', '')
                state = request.GET.get('state', '')
                phone = request.GET.get('phone', '')
                hours = request.GET.get('hours', '')
                website = request.GET.get('website', '')
                category_sub = request.GET.get('category_sub', '')

                location_line = ', '.join(filter(None, [city, state]))

                output['map_view_data'] = {
                    'lat': lat_f,
                    'lng': lng_f,
                    'short_address': title,
                    'address': address,
                    'location_line': location_line or address,
                    'city': city,
                    'state': state,
                    'phone': phone,
                    'hours': hours,
                    'website': website,
                    'category_sub': category_sub,
                    'is_nearme': bool(nearme),
                    **_build_urls(lat_f, lng_f, query),
                }
            except (ValueError, TypeError):
                output['map_view_data'] = {
                    'error': True,
                    'error_message': 'Invalid coordinates. Could not display map.',
                }
        else:
            output['map_view_data'] = {
                'error': True,
                'error_message': 'No location data provided.',
            }
        return output

    # ── INLINE MAP CARD + MULTI-PIN MAP (from search results) ───
    if results:
        # Enrich all results with map links
        _enrich_results(results)

        # Collect ALL results with coordinates
        map_locations = []
        for item in results:
            location = _extract_location(item)
            if location:
                map_locations.append({
                    **location,
                    'id': item.get('id'),
                    'title': item.get('title', location.get('short_address', '')),
                    'image': item.get('image'),
                    'data_type': item.get('data_type', ''),
                    'schema': item.get('schema', ''),
                    **_build_urls(location['lat'], location['lng'], query),
                })

        # First location → inline card (backward compatible)
        if map_locations:
            output['address_map'] = map_locations[0]

        # ALL locations → multi-pin map
        output['map_locations'] = map_locations
        output['map_location_count'] = len(map_locations)

    return output


# ═══════════════════════════════════════════════════════════════════
# PRIVATE IMPLEMENTATION
# ═══════════════════════════════════════════════════════════════════

def _extract_coords(item):
    """
    Extract (lat, lng) floats from a result dict.

    Supports:
    - Redis hash fields: latitude/lat, longitude/lng
    - Typesense geopoint (top-level): location_geopoint [lat, lng]
    - Nested location dict from format_result: location.geopoint

    Returns (lat_f, lng_f) or (None, None).
    """
    lat = item.get('latitude') or item.get('lat')
    lng = item.get('longitude') or item.get('lng')

    # Typesense geopoint support (top-level — legacy/Redis path)
    if not lat or not lng:
        geopoint = item.get('location_geopoint')
        if geopoint and isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
            lat, lng = geopoint[0], geopoint[1]

    # Nested location dict from format_result (Typesense bridge path)
    if not lat or not lng:
        loc = item.get('location', {})
        if isinstance(loc, dict):
            # Try lat/lng directly first
            loc_lat = loc.get('lat')
            loc_lng = loc.get('lng')
            if loc_lat and loc_lng:
                lat, lng = loc_lat, loc_lng
            else:
                # Try geopoint array
                geopoint = loc.get('geopoint')
                if geopoint:
                    if isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
                        lat, lng = geopoint[0], geopoint[1]
                    elif isinstance(geopoint, str) and ',' in geopoint:
                        parts = geopoint.split(',')
                        if len(parts) == 2:
                            lat, lng = parts[0].strip(), parts[1].strip()

    if not lat or not lng:
        return None, None

    try:
        lat_f = float(lat)
        lng_f = float(lng)
    except (ValueError, TypeError):
        return None, None

    if not (-90 <= lat_f <= 90 and -180 <= lng_f <= 180):
        return None, None

    return lat_f, lng_f


def _extract_location(item):
    """
    Extract location data from a single search result dict.
    Returns a location dict if coordinates found, else None.

    Supports both Redis hash fields, Typesense document fields,
    and the nested location dict from format_result.
    """
    lat_f, lng_f = _extract_coords(item)
    if lat_f is None:
        return None

    # Nested location dict from format_result
    loc = item.get('location', {}) if isinstance(item.get('location'), dict) else {}

    # Support Redis fields, top-level Typesense fields, AND nested location dict
    display = (item.get('display')
               or item.get('document_title')
               or item.get('title')
               or item.get('term', ''))

    address = (item.get('address')
               or item.get('location_address', '')
               or loc.get('address', ''))

    city = (item.get('city')
            or item.get('location_city', '')
            or loc.get('city', ''))

    state = (item.get('state')
             or item.get('location_state', '')
             or loc.get('state', ''))

    zip_code = (item.get('zip')
                or item.get('location_zip', '')
                or loc.get('zip', ''))

    phone = (item.get('phone')
             or item.get('service_phone', '')
             or item.get('service_phone', ''))

    hours = (item.get('hours')
             or item.get('service_hours', ''))

    website = (item.get('website')
               or item.get('service_website', ''))

    category_sub = (item.get('category_sub')
                    or item.get('document_category', '')
                    or loc.get('category', ''))

    description = (item.get('description')
                   or item.get('document_description', ''))

    location_line = address or ', '.join(filter(None, [city, state, zip_code]))

    return {
        'lat': lat_f,
        'lng': lng_f,
        'short_address': display,
        'address': address,
        'location_line': location_line,
        'city': city,
        'state': state,
        'zip': zip_code,
        'phone': phone,
        'hours': hours,
        'website': website,
        'category_sub': category_sub,
        'description': description,
    }


def _enrich_results(results):
    """Add map_url and has_map to results that have location data. Mutates in place."""
    for item in results:
        lat_f, lng_f = _extract_coords(item)
        if lat_f is None:
            continue

        # Nested location dict from format_result
        loc = item.get('location', {}) if isinstance(item.get('location'), dict) else {}

        params = {
            'view': 'map',
            'lat': str(lat_f),
            'lng': str(lng_f),
            'title': (item.get('display')
                      or item.get('document_title')
                      or item.get('title', '')),
            'address': (item.get('address')
                        or item.get('location_address', '')
                        or loc.get('address', '')),
            'city': (item.get('city')
                     or item.get('location_city', '')
                     or loc.get('city', '')),
            'state': (item.get('state')
                      or item.get('location_state', '')
                      or loc.get('state', '')),
            'phone': (item.get('phone')
                      or item.get('service_phone', '')),
            'hours': (item.get('hours')
                      or item.get('service_hours', '')),
            'website': (item.get('website')
                        or item.get('service_website', '')),
            'category_sub': (item.get('category_sub')
                             or item.get('document_category', '')),
        }
        # Remove empty params to keep URL clean
        params = {k: v for k, v in params.items() if v}
        item['map_url'] = f"/search/?{urllib.parse.urlencode(params)}"
        item['has_map'] = True


def _build_urls(lat, lng, query=''):
    """Build Google Maps external URLs + internal expand URL."""
    params = {'view': 'map', 'lat': str(lat), 'lng': str(lng)}
    if query:
        params['query'] = query
    return {
        'directions_url': f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}",
        'share_url': f"https://www.google.com/maps/search/?api=1&query={lat},{lng}",
        'streetview_url': f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lng}",
        'full_map_url': f"/search/?{urllib.parse.urlencode(params)}",
    }