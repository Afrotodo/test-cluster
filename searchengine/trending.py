# """
# Trending Results Cache System
# =============================
# Caches top Typesense results for high-scoring queries in the analytics Redis instance,
# keyed by city. Serves rich trending cards on the home page without
# hitting Typesense repeatedly.

# Uses the same Redis connection as redis_analytics.py (REDIS_ANALYTICS_URL).

# Redis key structure (lives alongside existing analytics keys):
#   - trending:index:{city}          → Sorted set (ZSET) of query hashes scored by popularity
#   - trending:data:{query_hash}     → Hash with result data (title, summary, image, url, etc.)

# Usage:
#   # When a search happens (in your search view):
#   from .trending import cache_trending_result
#   cache_trending_result(query, top_result_doc, city)

#   # On home page (in your home view):
#   from .trending import get_trending_results
#   trending_items = get_trending_results(city, limit=6)
# """

# import hashlib
# import logging

# from redis.exceptions import RedisError

# # Import the existing Redis client from your analytics module
# from .redis_analytics import get_redis_client

# logger = logging.getLogger(__name__)

# # Configuration
# TRENDING_TTL = 60 * 60 * 24  # 24 hours
# TRENDING_MAX_ITEMS = 20       # Max trending items per city
# DEFAULT_CITY = "general"


# def _query_hash(query: str) -> str:
#     """Create a stable hash for a query string (normalized)."""
#     normalized = query.strip().lower()
#     return hashlib.md5(normalized.encode()).hexdigest()[:12]


# def _extract_result_data(document: dict) -> dict:
#     """
#     Extract the fields we need from a formatted search result.
    
#     Handles both formats:
#     - Raw Typesense documents (document_title, document_url, etc.)
#     - Formatted results from format_result() (title, url, source, image, etc.)
#     """
#     # Get image — format_result() provides 'image' as first image already extracted
#     image_url = document.get("image", "")

#     # Fallback: try image_url array
#     if not image_url and document.get("image_url"):
#         images = document["image_url"]
#         if isinstance(images, list) and len(images) > 0:
#             image_url = images[0]
#         elif isinstance(images, str):
#             image_url = images

#     # Fallback: try logo_url array
#     if not image_url and document.get("logo_url"):
#         logos = document["logo_url"]
#         if isinstance(logos, list) and len(logos) > 0:
#             image_url = logos[0]
#         elif isinstance(logos, str):
#             image_url = logos

#     return {
#         "title": document.get("title", document.get("document_title", "")),
#         "summary": document.get("summary", document.get("document_summary", "")),
#         "url": document.get("url", document.get("document_url", "")),
#         "image_url": image_url or "",
#         "brand": document.get("source", document.get("document_brand", "")),
#         "category": document.get("category", document.get("document_category", "")),
#         "schema": document.get("schema", document.get("document_schema", "")),
#         "document_uuid": document.get("id", document.get("document_uuid", "")),
#     }


# def cache_trending_result(query: str, top_result: dict, city: str = None):
#     """
#     Cache a search result for trending display.

#     Call this in your search view after getting results from Typesense.
#     Only caches if the result has the minimum required fields.

#     Args:
#         query: The search query string
#         top_result: The top Typesense document (raw dict)
#         city: User's city (falls back to DEFAULT_CITY)
#     """
#     if not query or not top_result:
#         return

#     r = get_redis_client()
#     if r is None:
#         return

#     city = (city or DEFAULT_CITY).lower().strip()
#     qhash = _query_hash(query)
#     result_data = _extract_result_data(top_result)

#     # Skip if we don't have minimum viable data
#     if not result_data["title"] or not result_data["url"]:
#         return

#     result_data["query"] = query.strip()
#     result_data["city"] = city

#     try:
#         pipe = r.pipeline()

#         # Store the result data as a Redis hash
#         data_key = f"trending:data:{qhash}"
#         pipe.hset(data_key, mapping=result_data)
#         pipe.expire(data_key, TRENDING_TTL)

#         # Increment score in the city's sorted set
#         index_key = f"trending:index:{city}"
#         pipe.zincrby(index_key, 1, qhash)
#         pipe.expire(index_key, TRENDING_TTL)

#         # Also increment in the general index (fallback for cities with no data)
#         if city != DEFAULT_CITY:
#             general_key = f"trending:index:{DEFAULT_CITY}"
#             pipe.zincrby(general_key, 1, qhash)
#             pipe.expire(general_key, TRENDING_TTL)

#         # Trim to max items (remove lowest scored if over limit)
#         pipe.zremrangebyrank(index_key, 0, -(TRENDING_MAX_ITEMS + 1))

#         pipe.execute()

#         logger.debug(f"Cached trending result: city={city}, query='{query}', hash={qhash}")

#     except RedisError as e:
#         logger.warning(f"Failed to cache trending result: {e}")
#     except Exception as e:
#         logger.warning(f"Unexpected error caching trending result: {e}")


# def get_trending_results(city: str = None, limit: int = 6) -> list:
#     """
#     Get trending results for a city, sorted by score (highest first).

#     Returns a list of dicts with: title, summary, url, image_url, brand,
#     category, query, score.

#     Args:
#         city: User's city (falls back to DEFAULT_CITY)
#         limit: Max number of results to return

#     Returns:
#         List of trending result dicts, sorted by popularity
#     """
#     r = get_redis_client()
#     if r is None:
#         return []

#     city = (city or DEFAULT_CITY).lower().strip()

#     try:
#         index_key = f"trending:index:{city}"

#         # Get top query hashes by score (highest first)
#         top_hashes = r.zrevrange(index_key, 0, limit - 1, withscores=True)

#         # Fall back to general if city has no data
#         if not top_hashes and city != DEFAULT_CITY:
#             index_key = f"trending:index:{DEFAULT_CITY}"
#             top_hashes = r.zrevrange(index_key, 0, limit - 1, withscores=True)

#         if not top_hashes:
#             return []

#         # Batch fetch all result data
#         pipe = r.pipeline()
#         for qhash, score in top_hashes:
#             pipe.hgetall(f"trending:data:{qhash}")

#         data_list = pipe.execute()

#         results = []
#         for (qhash, score), data in zip(top_hashes, data_list):
#             if data and data.get("title"):
#                 data["score"] = int(score)
#                 results.append(data)

#         return results

#     except RedisError as e:
#         logger.warning(f"Failed to get trending results: {e}")
#         return []
#     except Exception as e:
#         logger.warning(f"Unexpected error getting trending results: {e}")
#         return []


# def clear_trending(city: str = None):
#     """
#     Clear all trending data for a city.
#     If city is None, clears the general/default index.
#     """
#     r = get_redis_client()
#     if r is None:
#         return

#     city = (city or DEFAULT_CITY).lower().strip()

#     try:
#         index_key = f"trending:index:{city}"

#         # Get all hashes in this city's index
#         all_hashes = r.zrange(index_key, 0, -1)

#         if all_hashes:
#             pipe = r.pipeline()
#             for qhash in all_hashes:
#                 pipe.delete(f"trending:data:{qhash}")
#             pipe.delete(index_key)
#             pipe.execute()

#         logger.info(f"Cleared trending data for city={city}, removed {len(all_hashes)} items")

#     except RedisError as e:
#         logger.warning(f"Failed to clear trending: {e}")



# """
# Trending Results Cache System
# =============================
# Caches top Typesense results for high-scoring queries in the analytics Redis instance,
# keyed by city. Serves rich trending cards on the home page without
# hitting Typesense repeatedly.

# Uses the same Redis connection as redis_analytics.py (REDIS_ANALYTICS_URL).

# Redis key structure (lives alongside existing analytics keys):
#   - trending:index:{city}          → Sorted set (ZSET) of query hashes scored by popularity
#   - trending:data:{query_hash}     → Hash with result data (title, summary, image, url, etc.)

# Usage:
#   # When a search happens (in your search view):
#   from .trending import cache_trending_result
#   cache_trending_result(query, top_result_doc, city)

#   # On home page (in your home view):
#   from .trending import get_trending_results
#   trending_items = get_trending_results(city, limit=6)
# """

# import hashlib
# import logging

# from redis.exceptions import RedisError

# # Import the existing Redis client from your analytics module
# from .redis_analytics import get_redis_client

# logger = logging.getLogger(__name__)

# # Configuration
# TRENDING_TTL = 60 * 60 * 24  # 24 hours
# TRENDING_MAX_ITEMS = 20       # Max trending items per city
# DEFAULT_CITY = "general"


# def _query_hash(query: str) -> str:
#     """Create a stable hash for a query string (normalized)."""
#     normalized = query.strip().lower()
#     return hashlib.md5(normalized.encode()).hexdigest()[:12]


# def _extract_result_data(document: dict) -> dict:
#     """
#     Extract the fields we need from a formatted search result.
    
#     Handles both formats:
#     - Raw Typesense documents (document_title, document_url, etc.)
#     - Formatted results from format_result() (title, url, source, image, etc.)
#     """
#     # Get image — format_result() provides 'image' as first image already extracted
#     image_url = document.get("image", "")

#     # Fallback: try image_url array
#     if not image_url and document.get("image_url"):
#         images = document["image_url"]
#         if isinstance(images, list) and len(images) > 0:
#             image_url = images[0]
#         elif isinstance(images, str):
#             image_url = images

#     # Fallback: try logo_url array
#     if not image_url and document.get("logo_url"):
#         logos = document["logo_url"]
#         if isinstance(logos, list) and len(logos) > 0:
#             image_url = logos[0]
#         elif isinstance(logos, str):
#             image_url = logos

#     return {
#         "title": document.get("title", document.get("document_title", "")),
#         "summary": document.get("summary", document.get("document_summary", "")),
#         "url": document.get("url", document.get("document_url", "")),
#         "image_url": image_url or "",
#         "brand": document.get("source", document.get("document_brand", "")),
#         "category": document.get("category", document.get("document_category", "")),
#         "schema": document.get("schema", document.get("document_schema", "")),
#         "document_uuid": document.get("id", document.get("document_uuid", "")),
#     }


# def cache_trending_result(query: str, top_result: dict, city: str = None):
#     """
#     Cache a search result for trending display.

#     Call this in your search view after getting results from Typesense.
#     Only caches if the result has the minimum required fields.

#     Args:
#         query: The search query string
#         top_result: The top Typesense document (raw dict)
#         city: User's city (falls back to DEFAULT_CITY)
#     """
#     if not query or not top_result:
#         return

#     r = get_redis_client()
#     if r is None:
#         return

#     city = (city or DEFAULT_CITY).lower().strip()
#     qhash = _query_hash(query)
#     result_data = _extract_result_data(top_result)

#     # Skip if we don't have minimum viable data
#     if not result_data["title"] or not result_data["url"]:
#         return

#     result_data["query"] = query.strip()
#     result_data["city"] = city

#     try:
#         pipe = r.pipeline()

#         # Store the result data as a Redis hash
#         data_key = f"trending:data:{qhash}"
#         pipe.hset(data_key, mapping=result_data)
#         pipe.expire(data_key, TRENDING_TTL)

#         # Increment score in the city's sorted set
#         index_key = f"trending:index:{city}"
#         pipe.zincrby(index_key, 1, qhash)
#         pipe.expire(index_key, TRENDING_TTL)

#         # Also increment in the general index (fallback for cities with no data)
#         if city != DEFAULT_CITY:
#             general_key = f"trending:index:{DEFAULT_CITY}"
#             pipe.zincrby(general_key, 1, qhash)
#             pipe.expire(general_key, TRENDING_TTL)

#         # Trim to max items (remove lowest scored if over limit)
#         pipe.zremrangebyrank(index_key, 0, -(TRENDING_MAX_ITEMS + 1))

#         pipe.execute()

#         logger.debug(f"Cached trending result: city={city}, query='{query}', hash={qhash}")

#     except RedisError as e:
#         logger.warning(f"Failed to cache trending result: {e}")
#     except Exception as e:
#         logger.warning(f"Unexpected error caching trending result: {e}")


# def get_trending_results(city: str = None, limit: int = 6) -> list:
#     """
#     Get trending results for a city, sorted by score (highest first).

#     Returns a list of dicts with: title, summary, url, image_url, brand,
#     category, query, score.

#     Args:
#         city: User's city (falls back to DEFAULT_CITY)
#         limit: Max number of results to return

#     Returns:
#         List of trending result dicts, sorted by popularity
#     """
#     r = get_redis_client()
#     if r is None:
#         return []

#     city = (city or DEFAULT_CITY).lower().strip()

#     try:
#         index_key = f"trending:index:{city}"

#         # Fetch more than `limit` so we still have enough after deduplication
#         fetch_limit = limit * 3
#         top_hashes = r.zrevrange(index_key, 0, fetch_limit - 1, withscores=True)

#         # Fall back to general if city has no data
#         if not top_hashes and city != DEFAULT_CITY:
#             index_key = f"trending:index:{DEFAULT_CITY}"
#             top_hashes = r.zrevrange(index_key, 0, fetch_limit - 1, withscores=True)

#         if not top_hashes:
#             return []

#         # Batch fetch all result data
#         pipe = r.pipeline()
#         for qhash, score in top_hashes:
#             pipe.hgetall(f"trending:data:{qhash}")

#         data_list = pipe.execute()

#         results = []
#         seen_docs = set()
#         for (qhash, score), data in zip(top_hashes, data_list):
#             if not data or not data.get("title"):
#                 continue

#             # Deduplicate by document_uuid, falling back to url
#             dedup_key = data.get("document_uuid") or data.get("url")
#             if dedup_key and dedup_key in seen_docs:
#                 continue
#             if dedup_key:
#                 seen_docs.add(dedup_key)

#             data["score"] = int(score)
#             results.append(data)

#             if len(results) >= limit:
#                 break

#         return results

#     except RedisError as e:
#         logger.warning(f"Failed to get trending results: {e}")
#         return []
#     except Exception as e:
#         logger.warning(f"Unexpected error getting trending results: {e}")
#         return []


# def clear_trending(city: str = None):
#     """
#     Clear all trending data for a city.
#     If city is None, clears the general/default index.
#     """
#     r = get_redis_client()
#     if r is None:
#         return

#     city = (city or DEFAULT_CITY).lower().strip()

#     try:
#         index_key = f"trending:index:{city}"

#         # Get all hashes in this city's index
#         all_hashes = r.zrange(index_key, 0, -1)

#         if all_hashes:
#             pipe = r.pipeline()
#             for qhash in all_hashes:
#                 pipe.delete(f"trending:data:{qhash}")
#             pipe.delete(index_key)
#             pipe.execute()

#         logger.info(f"Cleared trending data for city={city}, removed {len(all_hashes)} items")

#     except RedisError as e:
#         logger.warning(f"Failed to clear trending: {e}")



"""
Trending Results Cache System
=============================
Caches clicked results for trending display, keyed by city.
Only actual user clicks feed into trending — not raw search results.

Deduplication: each (session, query, document) combination is counted
only once, preventing users from inflating scores by repeated clicks.

Redis key structure:
  - trending:index:{city}          → Sorted set of query hashes scored by click popularity
  - trending:data:{query_hash}     → Hash with result data (title, summary, image, url, etc.)
  - trending:seen:{session_id}     → Set of already-counted (query+doc) pairs per session

Usage:
  # In your click tracking endpoint (click_redirect or track_click API):
  from .trending import cache_trending_result
  cache_trending_result(query, clicked_result, city, session_id)

  # On home page:
  from .trending import get_trending_results
  trending_items = get_trending_results(city, limit=6)
"""

import hashlib
import logging

from redis.exceptions import RedisError

from .redis_analytics import get_redis_client

logger = logging.getLogger(__name__)

# Configuration
TRENDING_TTL = 60 * 60 * 24  # 24 hours
TRENDING_MAX_ITEMS = 20       # Max trending items per city
DEFAULT_CITY = "general"


def _query_hash(query: str) -> str:
    """Create a stable hash for a query string (normalized)."""
    normalized = query.strip().lower()
    return hashlib.md5(normalized.encode()).hexdigest()[:12]


def _extract_result_data(document: dict) -> dict:
    """
    Extract the fields we need from a formatted search result.

    Handles both formats:
    - Raw Typesense documents (document_title, document_url, etc.)
    - Formatted results from format_result() (title, url, source, image, etc.)
    - Minimal dicts from click tracking (title, url, result_id, source)
    """
    image_url = document.get("image", "")

    if not image_url and document.get("image_url"):
        images = document["image_url"]
        if isinstance(images, list) and len(images) > 0:
            image_url = images[0]
        elif isinstance(images, str):
            image_url = images

    if not image_url and document.get("logo_url"):
        logos = document["logo_url"]
        if isinstance(logos, list) and len(logos) > 0:
            image_url = logos[0]
        elif isinstance(logos, str):
            image_url = logos

    return {
        "title": document.get("title", document.get("document_title", "")),
        "summary": document.get("summary", document.get("document_summary", "")),
        "url": document.get("url", document.get("document_url", "")),
        "image_url": image_url or "",
        "brand": document.get("source", document.get("document_brand", "")),
        "category": document.get("category", document.get("document_category", "")),
        "schema": document.get("schema", document.get("document_schema", "")),
        "document_uuid": document.get("id", document.get("document_uuid", document.get("result_id", ""))),
    }


def cache_trending_result(query: str, top_result: dict, city: str = None, session_id: str = None):
    """
    Cache a clicked result for trending display.

    Call this from your click tracking endpoint after a user clicks a result.
    Only counts once per document per session to prevent gaming.

    Args:
        query: The search query string
        top_result: The clicked result document (dict with title, url, etc.)
        city: User's city (falls back to DEFAULT_CITY)
        session_id: Session ID for deduplication (required — skips if missing)
    """
    if not query or not top_result:
        return

    if not session_id:
        logger.debug("cache_trending_result called without session_id, skipping")
        return

    r = get_redis_client()
    if r is None:
        return

    city = (city or DEFAULT_CITY).lower().strip()
    qhash = _query_hash(query)
    result_data = _extract_result_data(top_result)

    # Skip if we don't have minimum viable data
    if not result_data["title"] or not result_data["url"]:
        return

    # --- Session-based deduplication ---
    # Build a unique key from the document (prefer document_uuid, fall back to url)
    doc_id = result_data.get("document_uuid") or result_data.get("url", "")
    if not doc_id:
        return

    dedup_key = f"trending:seen:{session_id}"
    dedup_member = f"{qhash}:{hashlib.md5(doc_id.encode()).hexdigest()[:12]}"

    try:
        # SADD returns 0 if the member already exists — this session already
        # clicked this document for this query, so skip the score bump.
        added = r.sadd(dedup_key, dedup_member)
        if not added:
            logger.debug(
                f"Duplicate trending click ignored: session={session_id}, "
                f"query='{query}', doc={doc_id[:60]}"
            )
            return
        # Expire the dedup set along with other trending data
        r.expire(dedup_key, TRENDING_TTL)
    except RedisError as e:
        logger.warning(f"Trending dedup check failed: {e}")
        # On failure, fall through and count the click rather than
        # silently dropping a legitimate one

    result_data["query"] = query.strip()
    result_data["city"] = city

    try:
        pipe = r.pipeline()

        # Store the result data as a Redis hash
        data_key = f"trending:data:{qhash}"
        pipe.hset(data_key, mapping=result_data)
        pipe.expire(data_key, TRENDING_TTL)

        # Increment score in the city's sorted set
        index_key = f"trending:index:{city}"
        pipe.zincrby(index_key, 1, qhash)
        pipe.expire(index_key, TRENDING_TTL)

        # Also increment in the general index (fallback for cities with no data)
        if city != DEFAULT_CITY:
            general_key = f"trending:index:{DEFAULT_CITY}"
            pipe.zincrby(general_key, 1, qhash)
            pipe.expire(general_key, TRENDING_TTL)

        # Trim to max items (remove lowest scored if over limit)
        pipe.zremrangebyrank(index_key, 0, -(TRENDING_MAX_ITEMS + 1))

        pipe.execute()

        logger.debug(f"Cached trending result: city={city}, query='{query}', hash={qhash}")

    except RedisError as e:
        logger.warning(f"Failed to cache trending result: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error caching trending result: {e}")


def get_trending_results(city: str = None, limit: int = 6) -> list:
    """
    Get trending results for a city, sorted by score (highest first).

    Returns a list of dicts with: title, summary, url, image_url, brand,
    category, query, score.
    """
    r = get_redis_client()
    if r is None:
        return []

    city = (city or DEFAULT_CITY).lower().strip()

    try:
        index_key = f"trending:index:{city}"

        fetch_limit = limit * 3
        top_hashes = r.zrevrange(index_key, 0, fetch_limit - 1, withscores=True)

        if not top_hashes and city != DEFAULT_CITY:
            index_key = f"trending:index:{DEFAULT_CITY}"
            top_hashes = r.zrevrange(index_key, 0, fetch_limit - 1, withscores=True)

        if not top_hashes:
            return []

        pipe = r.pipeline()
        for qhash, score in top_hashes:
            pipe.hgetall(f"trending:data:{qhash}")

        data_list = pipe.execute()

        results = []
        seen_docs = set()
        for (qhash, score), data in zip(top_hashes, data_list):
            if not data or not data.get("title"):
                continue

            dedup_key = data.get("document_uuid") or data.get("url")
            if dedup_key and dedup_key in seen_docs:
                continue
            if dedup_key:
                seen_docs.add(dedup_key)

            data["score"] = int(score)
            results.append(data)

            if len(results) >= limit:
                break

        return results

    except RedisError as e:
        logger.warning(f"Failed to get trending results: {e}")
        return []
    except Exception as e:
        logger.warning(f"Unexpected error getting trending results: {e}")
        return []


def clear_trending(city: str = None):
    """Clear all trending data for a city."""
    r = get_redis_client()
    if r is None:
        return

    city = (city or DEFAULT_CITY).lower().strip()

    try:
        index_key = f"trending:index:{city}"
        all_hashes = r.zrange(index_key, 0, -1)

        if all_hashes:
            pipe = r.pipeline()
            for qhash in all_hashes:
                pipe.delete(f"trending:data:{qhash}")
            pipe.delete(index_key)
            pipe.execute()

        logger.info(f"Cleared trending data for city={city}, removed {len(all_hashes)} items")

    except RedisError as e:
        logger.warning(f"Failed to clear trending: {e}")