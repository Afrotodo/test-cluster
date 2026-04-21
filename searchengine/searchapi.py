
# """
# searchapi.py - Redis-based search preprocessing for Django

# This module provides Redis hash and sorted set lookups for:
# - Query term validation
# - Spelling correction
# - Autocomplete suggestions (terms + questions)
# - Query caching
# - Index introspection utilities
# """

# from __future__ import annotations

# import json
# import logging
# import string
# from typing import Any, Dict, List, Optional, Set, Tuple

# import redis
# from decouple import config
# from redis.commands.search.query import Query

# try:
#     from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_dl_distance
#     _USE_FAST_DL = True
# except ImportError:  # pragma: no cover
#     _USE_FAST_DL = False

# logger = logging.getLogger(__name__)


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# REDIS_LOCATION: str = config("REDIS_LOCATION")
# REDIS_PORT: int = config("REDIS_PORT", cast=int)
# REDIS_DB: int = config("REDIS_DB", default=0, cast=int)
# REDIS_PASSWORD: str = config("REDIS_PASSWORD", default="")
# REDIS_USERNAME: str = config("REDIS_USERNAME", default="")

# INDEX_NAME = "terms_idx"

# _CATEGORY_KEYWORDS: frozenset[str] = frozenset({
#     "city", "country", "state", "us_city", "us_state",
#     "continent", "word", "culture", "business", "education",
#     "fashion", "food", "health", "music", "sport", "tech",
#     "dictionary_word",
# })


# # =============================================================================
# # REDIS CONNECTION
# # =============================================================================

# class RedisLookupTable:
#     """Redis-based lookup table using RediSearch."""

#     _client: Optional[redis.Redis] = None

#     @classmethod
#     def get_client(cls) -> Optional[redis.Redis]:
#         """Return a live Redis client, reconnecting if the previous one dropped."""
#         if cls._client is not None:
#             try:
#                 cls._client.ping()
#                 return cls._client
#             except (redis.ConnectionError, redis.TimeoutError):
#                 cls._client = None

#         if not REDIS_LOCATION:
#             logger.error("REDIS_LOCATION is empty or not set")
#             return None

#         try:
#             redis_config: Dict[str, Any] = {
#                 "host": REDIS_LOCATION,
#                 "port": REDIS_PORT,
#                 "db": REDIS_DB,
#                 "decode_responses": True,
#                 "socket_connect_timeout": 5,
#                 "socket_timeout": 5,
#             }
#             if REDIS_PASSWORD:
#                 redis_config["password"] = REDIS_PASSWORD
#             if REDIS_USERNAME:
#                 redis_config["username"] = REDIS_USERNAME

#             cls._client = redis.Redis(**redis_config)
#             cls._client.ping()
#             return cls._client

#         except Exception:
#             logger.exception("Redis connection error")
#             return None


# # =============================================================================
# # QUERY ESCAPING HELPERS
# # =============================================================================

# _QUERY_SPECIAL = frozenset('@!{}()|\\-=><[]"\' ~*:.,/&^$#;')
# _TAG_SPECIAL   = frozenset(',./<>{}[]"\':;!@#$%^&*()-+=~|\\/\'')


# def escape_query(text: str) -> str:
#     """Escape special characters for a RediSearch full-text query."""
#     if not text:
#         return ""
#     return "".join(f"\\{c}" if c in _QUERY_SPECIAL else c for c in text)


# def escape_tag(text: str) -> str:
#     """Escape special characters for a RediSearch TAG field value."""
#     if not text:
#         return ""
#     return "".join(f"\\{c}" if c in _TAG_SPECIAL else c for c in text)


# # =============================================================================
# # INDEX MANAGEMENT
# # =============================================================================

# def create_index() -> bool:
#     """Create the RediSearch index (STOPWORDS 0 — index every token)."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False

#     try:
#         try:
#             client.ft(INDEX_NAME).info()
#             logger.info("Index '%s' already exists", INDEX_NAME)
#             return True
#         except Exception:
#             pass  # index doesn't exist yet — create it

#         client.execute_command(
#             "FT.CREATE", INDEX_NAME,
#             "ON", "HASH",
#             "PREFIX", "1", "term:",
#             "STOPWORDS", "0",
#             "SCHEMA",
#             "term",        "TEXT",    "WEIGHT", "5.0",
#             "display",     "TEXT",    "WEIGHT", "3.0",
#             "category",    "TAG",     "SORTABLE",
#             "description", "TEXT",    "WEIGHT", "1.0",
#             "pos",         "TAG",
#             "entity_type", "TAG",
#             "rank",        "NUMERIC", "SORTABLE",
#         )
#         logger.info("Index '%s' created successfully (STOPWORDS 0)", INDEX_NAME)
#         return True

#     except Exception:
#         logger.exception("Error creating index '%s'", INDEX_NAME)
#         return False


# def drop_index() -> bool:
#     """Drop the index while keeping the underlying data."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False

#     try:
#         client.ft(INDEX_NAME).dropindex(delete_documents=False)
#         logger.info("Index '%s' dropped", INDEX_NAME)
#         return True
#     except Exception:
#         logger.exception("Error dropping index '%s'", INDEX_NAME)
#         return False


# # =============================================================================
# # INDEX INTROSPECTION UTILITIES
# # =============================================================================

# def get_index_info(client: Optional[redis.Redis] = None) -> Dict[str, Any]:
#     """
#     Return RediSearch index metadata as a flat key/value dict.
#     Returns an empty dict if the index does not exist or the call fails.
#     """
#     client = client or RedisLookupTable.get_client()
#     if not client:
#         return {}

#     try:
#         raw = client.ft(INDEX_NAME).info()
#         items = list(raw)
#         return {items[i]: items[i + 1] for i in range(0, len(items) - 1, 2)}
#     except redis.ResponseError:
#         return {}
#     except Exception:
#         logger.warning("Could not read index info for '%s'", INDEX_NAME, exc_info=True)
#         return {}


# def index_has_field(field_name: str, client: Optional[redis.Redis] = None) -> bool:
#     """Return True if *field_name* is a registered attribute on INDEX_NAME."""
#     client = client or RedisLookupTable.get_client()
#     if not client:
#         return False

#     info = get_index_info(client)
#     attributes = info.get("attributes", [])
#     field_lower = field_name.lower()

#     for attr in attributes:
#         flat = list(attr)
#         attr_dict = {
#             str(flat[i]).lower(): flat[i + 1]
#             for i in range(0, len(flat) - 1, 2)
#         }
#         if attr_dict.get("identifier", "").lower() == field_lower:
#             return True
#     return False


# def get_index_status() -> Dict[str, Any]:
#     """
#     Return a structured summary of the current index and key counts.

#     Intended for health-check endpoints or management commands.
#     Does NOT print — callers decide how to surface the data.
#     """
#     client = RedisLookupTable.get_client()
#     if not client:
#         return {"index_exists": False, "error": "Redis connection failed"}

#     info = get_index_info(client)

#     if not info:
#         return {
#             "index_exists": False,
#             "index_name": INDEX_NAME,
#             "message": "Index not found — run create_index() to create it",
#         }

#     index_def_str = str(info.get("index_definition", ""))
#     term_count     = sum(1 for _ in client.scan_iter("term:*",      count=100))
#     question_count = sum(1 for _ in client.scan_iter("questions:*", count=100))

#     return {
#         "index_exists": True,
#         "index_name": INDEX_NAME,
#         "num_docs": info.get("num_docs", "unknown"),
#         "indexing_failures": info.get("hash_indexing_failures", 0),
#         "fields": {
#             "term_exact":        index_has_field("term_exact",  client),
#             "entity_type":       index_has_field("entity_type", client),
#             "rank":              index_has_field("rank",        client),
#             "questions_prefix":  "questions:" in index_def_str,
#         },
#         "key_counts": {
#             "term_keys":     term_count,
#             "question_keys": question_count,
#         },
#     }


# # =============================================================================
# # HELPER FUNCTIONS
# # =============================================================================

# def extract_base_term(member: str) -> str:
#     """Extract the base term from a document ID (``term:xxx:category`` → ``xxx``)."""
#     if not member:
#         return ""

#     if member.startswith("term:"):
#         member = member[5:]

#     parts = member.rsplit(":", 1)
#     if len(parts) == 2:
#         potential_category = parts[1].lower()
#         if potential_category in _CATEGORY_KEYWORDS or len(potential_category) < 15:
#             return parts[0]

#     return member


# def _safe_int(value: Any, default: int = 0) -> int:
#     """Safely coerce *value* to int, returning *default* on failure."""
#     try:
#         return int(float(value))
#     except (TypeError, ValueError):
#         return default


# def parse_search_doc(doc: Any) -> Dict[str, Any]:
#     """Parse a RediSearch document into a standard dict format."""
#     try:
#         term = getattr(doc, "term", "")
#         return {
#             "id":          doc.id,
#             "member":      doc.id,
#             "term":        term,
#             "display":     getattr(doc, "display", term),
#             "description": getattr(doc, "description", ""),
#             "category":    getattr(doc, "category", ""),
#             "entity_type": getattr(doc, "entity_type", ""),
#             "pos":         getattr(doc, "pos", ""),
#             "rank":        _safe_int(getattr(doc, "rank", 0)),
#             "exists":      True,
#         }
#     except Exception:
#         logger.exception("Error parsing RediSearch doc")
#         return {}


# # =============================================================================
# # CORE SEARCH FUNCTIONS
# # =============================================================================

# def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
#     """Get metadata for a single term via a direct HGETALL lookup."""
#     client = RedisLookupTable.get_client()
#     if not client or not member:
#         return None

#     try:
#         hash_key = member if member.startswith("term:") else f"term:{member}"
#         metadata = client.hgetall(hash_key)

#         if not metadata:
#             return None

#         base_term = extract_base_term(member)
#         return {
#             "member":      member,
#             "term":        metadata.get("term", base_term),
#             "exists":      True,
#             "display":     metadata.get("display", base_term),
#             "pos":         metadata.get("pos", "unknown"),
#             "category":    metadata.get("category", ""),
#             "description": metadata.get("description", ""),
#             "entity_type": metadata.get("entity_type", ""),
#             "rank":        _safe_int(metadata.get("rank", 0)),
#         }

#     except Exception:
#         logger.exception("Error getting term metadata for '%s'", member)
#         return None


# def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
#     """
#     Find all Redis hashes whose key matches ``term:{word}:*``.

#     Uses direct key scan — not RediSearch full-text search — so stop-words
#     and tokenisation rules do not affect results.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []

#     term_lower = term.lower().strip()
#     if not term_lower:
#         return []

#     # Multi-word terms use underscores in the key
#     term_key = term_lower.replace(" ", "_")

#     try:
#         keys = client.keys(f"term:{term_key}:*")
#         if not keys:
#             return []

#         matches: List[Dict[str, Any]] = []
#         for key in keys:
#             metadata = client.hgetall(key)
#             if not metadata:
#                 continue

#             # matches.append({
#             #     "id":            key,
#             #     "member":        key,
#             #     "term":          metadata.get("question", ""),
#             #     "display":       metadata.get("question", ""),
#             #     "description":   "",
#             #     "category":      "question",
#             #     "entity_type":   "question",
#             #     "pos":           "question",
#             #     "rank":          _safe_int(metadata.get("score", 0)),
#             #     "exists":        True,
#             #     "document_uuid": metadata.get("document_uuid", ""),
#             #     "answer":        metadata.get("answer", ""),
#             #     "answer_type":   metadata.get("answer_type", "UNKNOWN"),
#             # })

#             matches.append({
#             "id":            key,
#             "member":        key,
#             "term":          metadata.get("term", ""),      # ← correct
#             "display":       metadata.get("display", ""),   # ← correct
#             "description":   metadata.get("description", ""),
#             "category":      metadata.get("category", ""),
#             "entity_type":   metadata.get("entity_type", ""),
#             "pos":           metadata.get("pos", ""),
#             "rank":          _safe_int(metadata.get("rank", 0)),  # ← correct
#             "exists":        True,
#             })

#         matches.sort(key=lambda x: x.get("rank", 0), reverse=True)
#         return matches

#     except Exception:
#         logger.exception("Exact match error for term '%s'", term)
#         return []



# # =============================================================================
# # QUESTION LOOKUP — DIRECT KEY SCAN
# # =============================================================================

# def get_question_matches(prefix: str, limit: int = 5) -> List[Dict[str, Any]]:
#     """
#     Return question hashes whose key starts with the given prefix.

#     Key pattern: ``questions:{slugified-question}``
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []

#     prefix_lower = prefix.lower().strip()
#     if len(prefix_lower) < 2:
#         return []

#     slug_prefix = prefix_lower.replace(" ", "_")
#     slug_prefix = "".join(c for c in slug_prefix if c.isalnum() or c == "_")[:120]

#     try:
#         keys = client.keys(f"questions:{slug_prefix}*")
#         if not keys:
#             return []

#         matches: List[Dict[str, Any]] = []
#         for key in keys[: limit * 2]:
             
#             metadata = client.hgetall(key)
          
#             if not metadata:
#                 continue

#             matches.append({
#                 "id":            key,
#                 "member":        key,
#                 "term":          metadata.get("question", ""),
#                 "display":       metadata.get("question", ""),
#                 "description":   "",
#                 "category":      "question",
#                 "entity_type":   "question",
#                 "pos":           "question",
#                 "rank":          _safe_int(metadata.get("score", 0)),
#                 "exists":        True,
#                 "document_uuid": metadata.get("document_uuid", ""),
#                 "answer":        metadata.get("answer", ""),
#                 "answer_type":   metadata.get("answer_type", "UNKNOWN"),
#             })

#         matches.sort(key=lambda x: x.get("rank", 0), reverse=True)
#         return matches[:limit]

#     except Exception:
#         logger.exception("Question match error for prefix '%s'", prefix)
#         return []


# def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """Return terms that start with *prefix* using a RediSearch prefix query."""
#     client = RedisLookupTable.get_client()
#     if not client or not prefix:
#         return []

#     prefix_lower = prefix.lower().strip()
#     if not prefix_lower:
#         return []

#     try:
#         query_str = f"{escape_query(prefix_lower)}*"
#         query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)

#         return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

#     except Exception:
#         logger.exception("Prefix match error for prefix '%s'", prefix)
#         return []


# # kept for backwards compatibility
# def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     return get_prefix_matches(prefix, limit=limit)


# # =============================================================================
# # DAMERAU-LEVENSHTEIN DISTANCE
# # =============================================================================

# def damerau_levenshtein_distance(s1: str, s2: str) -> int:
#     """
#     Damerau-Levenshtein distance between two strings.

#     Uses the fast C extension when available, falls back to a pure-Python
#     implementation so the module works in all environments.
#     """
#     if _USE_FAST_DL:
#         return _fast_dl_distance(s1, s2)  # type: ignore[return-value]

#     len1, len2 = len(s1), len(s2)
#     d = [[0] * (len2 + 1) for _ in range(len1 + 1)]

#     for i in range(len1 + 1):
#         d[i][0] = i
#     for j in range(len2 + 1):
#         d[0][j] = j

#     for i in range(1, len1 + 1):
#         for j in range(1, len2 + 1):
#             cost = 0 if s1[i - 1] == s2[j - 1] else 1
#             d[i][j] = min(
#                 d[i - 1][j] + 1,
#                 d[i][j - 1] + 1,
#                 d[i - 1][j - 1] + cost,
#             )
#             if i > 1 and j > 1 and s1[i - 1] == s2[j - 2] and s1[i - 2] == s2[j - 1]:
#                 d[i][j] = min(d[i][j], d[i - 2][j - 2] + cost)

#     return d[len1][len2]


# def calculate_score(distance: int, rank: int, max_rank: int = 10_000_000) -> float:
#     """Combined ranking score: lower distance + higher rank = lower (better) score."""
#     rank_bonus = min(rank, max_rank) / max_rank
#     return distance - rank_bonus


# # =============================================================================
# # FUZZY SEARCH (SPELL CORRECTION)
# # =============================================================================

# def _run_fuzzy_query(
#     client: redis.Redis,
#     query_str: str,
#     input_lower: str,
#     limit: int,
# ) -> List[Dict[str, Any]]:
#     """Execute a single fuzzy RediSearch query and return enriched results."""
#     query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
#     result = client.ft(INDEX_NAME).search(query)

#     matches: List[Dict[str, Any]] = []
#     for doc in result.docs:
#         parsed = parse_search_doc(doc)
#         if parsed:
#             parsed["distance"] = damerau_levenshtein_distance(
#                 input_lower, parsed["term"].lower()
#             )
#             matches.append(parsed)

#     matches.sort(key=lambda x: (x["distance"], -x.get("rank", 0)))
#     return matches


# def get_fuzzy_matches(
#     term: str,
#     limit: int = 10,
#     max_distance: int = 2,
# ) -> List[Dict[str, Any]]:
#     """
#     Return fuzzy matches using RediSearch Levenshtein operators.

#     - ``%term%``  = 1 edit distance
#     - ``%%term%%`` = 2 edit distance

#     For multi-word queries only the *last* word is fuzzified.
#     """
#     client = RedisLookupTable.get_client()
#     if not client or not term:
#         return []

#     term_lower = term.lower().strip()
#     if len(term_lower) < 3:
#         return []

#     words = term_lower.split()

#     try:
#         if len(words) > 1:
#             prefix_part = escape_query(" ".join(words[:-1]))
#             last_escaped = escape_query(words[-1])

#             if len(words[-1]) < 3:
#                 return []

#             for distance, wraps in ((1, ("%", "%")), (2, ("%%", "%%"))):
#                 if max_distance < distance:
#                     continue
#                 query_str = f"{prefix_part} {wraps[0]}{last_escaped}{wraps[1]}"
#                 try:
#                     matches = _run_fuzzy_query(client, query_str, term_lower, limit)
#                     if matches:
#                         return matches[:limit]
#                 except Exception:
#                     logger.debug("Fuzzy query failed (%d-edit, multi-word): %s", distance, query_str)

#             return []

#         # Single word
#         escaped = escape_query(term_lower)
#         for distance, wraps in ((1, ("%", "%")), (2, ("%%", "%%"))):
#             if max_distance < distance:
#                 continue
#             query_str = f"{wraps[0]}{escaped}{wraps[1]}"
#             try:
#                 matches = _run_fuzzy_query(client, query_str, term_lower, limit)
#                 if matches:
#                     return matches[:limit]
#             except Exception:
#                 logger.debug("Fuzzy query failed (%d-edit, single-word): %s", distance, query_str)

#         return []

#     except Exception:
#         logger.exception("Fuzzy match error for term '%s'", term)
#         return []


# # =============================================================================
# # BATCH OPERATIONS
# # =============================================================================

# def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
#     """Batch-fetch metadata for multiple terms using a Redis pipeline."""
#     client = RedisLookupTable.get_client()
#     if not client or not members:
#         return {}

#     try:
#         pipeline = client.pipeline()
#         for member in members:
#             hash_key = member if member.startswith("term:") else f"term:{member}"
#             pipeline.hgetall(hash_key)

#         results = pipeline.execute()
#         metadata_dict: Dict[str, Dict[str, Any]] = {}

#         for member, metadata in zip(members, results):
#             if not metadata:
#                 continue
#             base_term = extract_base_term(member)
#             metadata_dict[member] = {
#                 "member":      member,
#                 "term":        metadata.get("term", base_term),
#                 "exists":      True,
#                 "display":     metadata.get("display", base_term),
#                 "pos":         metadata.get("pos", "unknown"),
#                 "category":    metadata.get("category", ""),
#                 "description": metadata.get("description", ""),
#                 "entity_type": metadata.get("entity_type", ""),
#                 "rank":        _safe_int(metadata.get("rank", 0)),
#             }

#         return metadata_dict

#     except Exception:
#         logger.exception("Batch metadata error")
#         return {}


# def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#     Validate a list of words against Redis.

#     Returns a dict mapping ``word_lower`` → validation result with metadata.
#     """
#     if not words:
#         return {}

#     client = RedisLookupTable.get_client()
#     if not client:
#         return {}

#     results: Dict[str, Dict[str, Any]] = {}

#     try:
#         for word in words:
#             word_lower = word.lower().strip()
#             if not word_lower:
#                 continue

#             matches = get_exact_term_matches(word_lower)
#             if matches:
#                 results[word_lower] = {
#                     "is_valid": True,
#                     "word":     word_lower,
#                     "member":   matches[0].get("id", ""),
#                     "matches":  matches,
#                     "metadata": matches[0],
#                 }
#             else:
#                 results[word_lower] = {
#                     "is_valid": False,
#                     "word":     word_lower,
#                     "member":   None,
#                     "matches":  [],
#                     "metadata": {},
#                 }

#         return results

#     except Exception:
#         logger.exception("Batch validation error")
#         return {}


# # =============================================================================
# # VALIDATE WORD (SPELL CHECK)
# # =============================================================================

# def validate_word(
#     word: str,
#     _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None,
# ) -> Dict[str, Any]:
#     """Validate a single word and return spelling correction metadata if needed."""
#     word_lower = word.lower().strip()

#     # Fast path: already validated upstream
#     if _pre_validated is not None:
#         pre = _pre_validated.get(word_lower)
#         if pre and pre.get("is_valid"):
#             meta = pre.get("metadata", {})
#             return {
#                 "word":       word,
#                 "is_valid":   True,
#                 "suggestion": None,
#                 "metadata": {
#                     "display":  meta.get("display", word_lower),
#                     "pos":      meta.get("pos", "unknown"),
#                     "category": meta.get("category", ""),
#                     "rank":     meta.get("rank", 0),
#                 },
#             }

#     exact_matches = get_exact_term_matches(word_lower)
#     if exact_matches:
#         meta = exact_matches[0]
#         return {
#             "word":       word,
#             "is_valid":   True,
#             "suggestion": None,
#             "metadata": {
#                 "display":  meta.get("display", word_lower),
#                 "pos":      meta.get("pos", "unknown"),
#                 "category": meta.get("category", ""),
#                 "rank":     meta.get("rank", 0),
#             },
#         }

#     result = get_suggestions(word_lower, limit=1, max_distance=2)
#     if result["suggestions"]:
#         best = result["suggestions"][0]
#         return {
#             "word":       word,
#             "is_valid":   False,
#             "suggestion": best["term"],
#             "distance":   best.get("distance", 0),
#             "score":      best.get("score", 0),
#             "tier_used":  result["tier_used"],
#             "metadata": {
#                 "display":  best.get("display", ""),
#                 "pos":      best.get("pos", "unknown"),
#                 "category": best.get("category", ""),
#                 "rank":     best.get("rank", 0),
#             },
#         }

#     return {"word": word, "is_valid": False, "suggestion": None}


# # =============================================================================
# # GET SUGGESTIONS (UNIFIED SEARCH)
# # =============================================================================

# def get_suggestions(
#     input_text: str,
#     limit: int = 10,
#     max_distance: int = 2,
#     category: Optional[str] = None,
# ) -> Dict[str, Any]:
#     """
#     Unified suggestion function using RediSearch.

#     Returns combined results from Exact → Prefix → Fuzzy tiers,
#     sorted by rank descending then edit-distance ascending.

#     Questions are intentionally excluded here; they are prepended by
#     ``get_autocomplete()`` on the keyword path only.
#     """
#     response: Dict[str, Any] = {
#         "success":     True,
#         "input":       input_text,
#         "suggestions": [],
#         "exact_match": False,
#         "tier_used":   None,
#         "error":       None,
#     }

#     client = RedisLookupTable.get_client()
#     if not client:
#         response.update(success=False, error="Redis connection failed")
#         return response

#     if not input_text or not input_text.strip():
#         response.update(success=False, error="Empty input")
#         return response

#     input_lower = input_text.lower().strip()

#     try:
#         all_results: List[Dict[str, Any]] = []
#         seen_terms:  Set[str] = set()
#         tiers_used:  List[str] = []

#         # --- Tier 1: exact match ---
#         for match in get_exact_term_matches(input_lower):
#             term_lower = match.get("term", "").lower()
#             if term_lower not in seen_terms:
#                 match.update(distance=0, score=-match.get("rank", 0))
#                 all_results.append(match)
#                 seen_terms.add(term_lower)

#         if all_results:
#             response["exact_match"] = True
#             tiers_used.append("exact")

#         # --- Tier 2: prefix match (always run) ---
#         for item in get_prefix_matches(input_lower, limit=limit * 3):
#             term_lower = item.get("term", "").lower()
#             if term_lower not in seen_terms:
#                 distance = damerau_levenshtein_distance(input_lower, term_lower)
#                 item.update(distance=distance, score=calculate_score(distance, item.get("rank", 0)))
#                 all_results.append(item)
#                 seen_terms.add(term_lower)
#                 tiers_used.append("prefix") if "prefix" not in tiers_used else None

#         # --- Tier 3: fuzzy match (only when results are sparse) ---
#         if len(all_results) < limit:
#             for item in get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance):
#                 term_lower = item.get("term", "").lower()
#                 if term_lower not in seen_terms and item.get("distance", 99) <= max_distance:
#                     item["score"] = calculate_score(item["distance"], item.get("rank", 0))
#                     all_results.append(item)
#                     seen_terms.add(term_lower)
#                     tiers_used.append("fuzzy") if "fuzzy" not in tiers_used else None

#         all_results.sort(key=lambda x: (-x.get("rank", 0), x.get("distance", 99)))

#         response["suggestions"] = all_results[:limit]
#         response["tier_used"]   = "+".join(tiers_used) if tiers_used else "none"
#         return response

#     except Exception:
#         logger.exception("get_suggestions error for '%s'", input_text)
#         response.update(success=False, error="Internal search error")
#         return response


# # =============================================================================
# # AUTOCOMPLETE
# # =============================================================================

# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Return autocomplete suggestions: questions first, then terms.

#     - Up to 3 question results sourced from ``get_question_matches()``
#     - Remaining slots filled with term suggestions from ``get_suggestions()``
#     - Frontend can split on ``entity_type == 'question'`` to render separately
#     """
#     if not prefix or len(prefix.strip()) < 2:
#         return []

#     prefix_clean = prefix.strip()

#     question_results = get_question_matches(prefix_clean, limit=3)
#     term_results = get_suggestions(prefix_clean, limit=limit - len(question_results))

#     return question_results + term_results.get("suggestions", [])


# # =============================================================================
# # TOP WORDS BY RANK
# # =============================================================================

# def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
#     """Return the top *limit* words sorted by rank descending."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return []

#     try:
#         query = Query("*").sort_by("rank", asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
#         return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

#     except Exception:
#         logger.exception("Error getting top words by rank")
#         return []


# # =============================================================================
# # FILTERED SEARCH
# # =============================================================================

# def search_by_category(
#     query_text: str,
#     category: str,
#     limit: int = 20,
# ) -> List[Dict[str, Any]]:
#     """Search within a specific category."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not category:
#         return []

#     try:
#         query_str = (
#             f"{escape_query(query_text.lower().strip())}*"
#             f" @category:{{{escape_tag(category)}}}"
#         )
#         query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
#         return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

#     except Exception:
#         logger.exception("Category search error for '%s' / '%s'", query_text, category)
#         return []


# def search_by_entity_type(
#     query_text: str,
#     entity_type: str,
#     limit: int = 20,
# ) -> List[Dict[str, Any]]:
#     """Search by entity type (unigram, bigram, trigram, …)."""
#     client = RedisLookupTable.get_client()
#     if not client or not query_text or not entity_type:
#         return []

#     try:
#         query_str = (
#             f"{escape_query(query_text.lower().strip())}*"
#             f" @entity_type:{{{escape_tag(entity_type)}}}"
#         )
#         query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
#         result = client.ft(INDEX_NAME).search(query)
#         return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

#     except Exception:
#         logger.exception("Entity-type search error for '%s' / '%s'", query_text, entity_type)
#         return []


# # =============================================================================
# # CACHE FUNCTIONS
# # =============================================================================

# def check_cache(query: str) -> Optional[Dict[str, Any]]:
#     """Return cached results for *query*, or ``None`` on a cache miss."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return None

#     try:
#         cache_key = f"query_cache:{query.lower().strip()}"
#         cached = client.get(cache_key)
#         return json.loads(cached) if cached else None

#     except Exception:
#         logger.exception("Cache check error for query '%s'", query)
#         return None


# def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
#     """Persist *results* in Redis with a *ttl*-second expiry."""
#     client = RedisLookupTable.get_client()
#     if not client:
#         return False

#     try:
#         cache_key = f"query_cache:{query.lower().strip()}"
#         client.setex(cache_key, ttl, json.dumps(results))
#         return True

#     except Exception:
#         logger.exception("Cache save error for query '%s'", query)
#         return False


# # =============================================================================
# # MAIN API FUNCTION
# # =============================================================================

# def lookup_table(
#     query: str,
#     check_cache_first: bool = True,
#     include_suggestions: bool = True,
#     autocomplete_prefix: Optional[str] = None,
#     autocomplete_limit: int = 10,
#     return_validation_cache: bool = False,
# ) -> Dict[str, Any]:
#     """
#     Main API entry point for Redis-based search preprocessing.

#     Handles autocomplete, cache, per-word validation, and spell correction
#     in a single call.
#     """
#     response: Dict[str, Any] = {
#         "success":          True,
#         "query":            query,
#         "normalized_query": "",
#         "terms":            [],
#         "cache_hit":        False,
#         "autocomplete":     [],
#         "error":            None,
#     }

#     try:
#         if autocomplete_prefix:
#             response["autocomplete"] = get_autocomplete(
#                 autocomplete_prefix, limit=autocomplete_limit
#             )
#             return response

#         if not query or not query.strip():
#             response.update(success=False, error="Empty query")
#             return response

#         if check_cache_first:
#             cached = check_cache(query)
#             if cached:
#                 response.update(
#                     cache_hit=True,
#                     terms=cached.get("terms", []),
#                     normalized_query=cached.get("normalized_query", ""),
#                 )
#                 return response

#         words = query.lower().split()
#         validation_cache = batch_validate_words_redis(words)

#         terms: List[Dict[str, Any]] = []
#         normalized_words: List[str] = []

#         for i, word in enumerate(words):
#             word = word.strip()
#             if not word:
#                 continue

#             validation = validate_word(word, _pre_validated=validation_cache)

#             if validation["is_valid"]:
#                 terms.append({
#                     "position": i + 1,
#                     "word":     word,
#                     "exists":   True,
#                     "display":  validation["metadata"]["display"],
#                     "pos":      validation["metadata"]["pos"],
#                     "category": validation["metadata"]["category"],
#                     "rank":     validation["metadata"]["rank"],
#                     "metadata": validation["metadata"],
#                 })
#                 normalized_words.append(word)
#             else:
#                 terms.append({
#                     "position":  i + 1,
#                     "word":      word,
#                     "exists":    False,
#                     "suggestion": validation.get("suggestion"),
#                     "distance":  validation.get("distance"),
#                     "score":     validation.get("score"),
#                     "tier_used": validation.get("tier_used"),
#                     "metadata":  validation.get("metadata", {}),
#                 })
#                 if include_suggestions and validation.get("suggestion"):
#                     normalized_words.append(validation["suggestion"])

#         response["terms"]            = terms
#         response["normalized_query"] = " ".join(normalized_words)

#         if return_validation_cache:
#             response["_validation_cache"] = validation_cache

#         save_to_cache(query, {"terms": terms, "normalized_query": response["normalized_query"]})
#         return response

#     except Exception:
#         logger.exception("lookup_table error for query '%s'", query)
#         response.update(success=False, error="Internal error")
#         return response


# # =============================================================================
# # LEGACY / BACKWARDS-COMPATIBILITY FUNCTIONS
# # =============================================================================

# def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
#     """
#     Generate spelling candidates from common typo patterns.

#     Kept for backwards compatibility — ``get_fuzzy_matches()`` is preferred.
#     """
#     candidates: Set[str] = set()
#     word_lower = word.lower()
#     length = len(word_lower)

#     if length < 2:
#         return candidates

#     alphabet = string.ascii_lowercase
#     vowels   = "aeiou"

#     keyboard_proximity: Dict[str, str] = {
#         "q": "wa",    "w": "qeas",  "e": "wrds",  "r": "etdf",  "t": "ryfg",
#         "y": "tugh",  "u": "yihj",  "i": "uojk",  "o": "ipkl",  "p": "ol",
#         "a": "qwsz",  "s": "awedxz","d": "serfcx","f": "drtgvc","g": "ftyhbv",
#         "h": "gyujnb","j": "huikmn","k": "jiolm", "l": "kop",
#         "z": "asx",   "x": "zsdc",  "c": "xdfv",  "v": "cfgb",  "b": "vghn",
#         "n": "bhjm",  "m": "njk",
#     }

#     for i in range(length):
#         char = word_lower[i]
#         for nearby in keyboard_proximity.get(char, ""):
#             candidates.add(word_lower[:i] + nearby + word_lower[i + 1:])

#     for i in range(length - 1):
#         candidates.add(word_lower[:i] + word_lower[i + 1] + word_lower[i] + word_lower[i + 2:])

#     for i in range(length):
#         candidate = word_lower[:i] + word_lower[i + 1:]
#         if candidate:
#             candidates.add(candidate)

#     for i in range(length):
#         if word_lower[i] in vowels:
#             for v in vowels:
#                 if v != word_lower[i]:
#                     candidates.add(word_lower[:i] + v + word_lower[i + 1:])

#     if len(candidates) < max_candidates // 2:
#         for i in range(length + 1):
#             for char in alphabet:
#                 candidates.add(word_lower[:i] + char + word_lower[i:])
#                 if len(candidates) >= max_candidates:
#                     break
#             if len(candidates) >= max_candidates:
#                 break

#     return set(list(candidates)[:max_candidates])


# def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
#     """Check whether candidates exist in Redis. Kept for backwards compatibility."""
#     found: List[Dict[str, Any]] = []
#     for candidate in list(candidates)[:50]:
#         matches = get_exact_term_matches(candidate)
#         if matches:
#             found.append(matches[0])
#     return found


# def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
#     """Check multiple bigrams. Kept for backwards compatibility."""
#     results: Dict[str, Dict[str, Any]] = {}
#     for w1, w2 in word_pairs:
#         bigram = f"{w1.lower()} {w2.lower()}"
#         matches = get_exact_term_matches(bigram)
#         if matches:
#             results[bigram] = matches[0]
#     return results


# # =============================================================================
# # CONVENIENCE SHORTHANDS
# # =============================================================================

# def lookup(query: str) -> Dict[str, Any]:
#     """Shorthand for ``lookup_table`` with default settings."""
#     return lookup_table(query)


# def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """Shorthand for ``get_autocomplete``."""
#     return get_autocomplete(prefix, limit)


# def spell_check(word: str) -> Dict[str, Any]:
#     """Shorthand for ``validate_word``."""
#     return validate_word(word)




# ---------------------------------  Version 2 -------------------------
"""
searchapi.py - Redis-based search preprocessing for Django

This module provides Redis hash and sorted set lookups for:
- Query term validation
- Spelling correction
- Autocomplete suggestions (terms + questions)
- Query caching
- Index introspection utilities
"""

from __future__ import annotations

import json
import logging
import string
from typing import Any, Dict, List, Optional, Set, Tuple

import redis
from decouple import config
from redis.commands.search.query import Query

try:
    from pyxdameraulevenshtein import damerau_levenshtein_distance as _fast_dl_distance
    _USE_FAST_DL = True
except ImportError:  # pragma: no cover
    _USE_FAST_DL = False

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_LOCATION: str = config("REDIS_LOCATION")
REDIS_PORT: int = config("REDIS_PORT", cast=int)
REDIS_DB: int = config("REDIS_DB", default=0, cast=int)
REDIS_PASSWORD: str = config("REDIS_PASSWORD", default="")
REDIS_USERNAME: str = config("REDIS_USERNAME", default="")

INDEX_NAME = "terms_idx"
QUESTIONS_INDEX_NAME = "questions_idx"

_CATEGORY_KEYWORDS: frozenset[str] = frozenset({
    "city", "country", "state", "us_city", "us_state",
    "continent", "word", "culture", "business", "education",
    "fashion", "food", "health", "music", "sport", "tech",
    "dictionary_word",
})


# =============================================================================
# REDIS CONNECTION
# =============================================================================

class RedisLookupTable:
    """Redis-based lookup table using RediSearch."""

    _client: Optional[redis.Redis] = None

    @classmethod
    def get_client(cls) -> Optional[redis.Redis]:
        """Return a live Redis client, reconnecting if the previous one dropped."""
        if cls._client is not None:
            try:
                cls._client.ping()
                return cls._client
            except (redis.ConnectionError, redis.TimeoutError):
                cls._client = None

        if not REDIS_LOCATION:
            logger.error("REDIS_LOCATION is empty or not set")
            return None

        try:
            redis_config: Dict[str, Any] = {
                "host": REDIS_LOCATION,
                "port": REDIS_PORT,
                "db": REDIS_DB,
                "decode_responses": True,
                "socket_connect_timeout": 5,
                "socket_timeout": 5,
            }
            if REDIS_PASSWORD:
                redis_config["password"] = REDIS_PASSWORD
            if REDIS_USERNAME:
                redis_config["username"] = REDIS_USERNAME

            cls._client = redis.Redis(**redis_config)
            cls._client.ping()
            return cls._client

        except Exception:
            logger.exception("Redis connection error")
            return None


# =============================================================================
# QUERY ESCAPING HELPERS
# =============================================================================

_QUERY_SPECIAL = frozenset('@!{}()|\\-=><[]"\' ~*:.,/&^$#;')
_TAG_SPECIAL   = frozenset(',./<>{}[]"\':;!@#$%^&*()-+=~|\\/\'')


def escape_query(text: str) -> str:
    """Escape special characters for a RediSearch full-text query."""
    if not text:
        return ""
    return "".join(f"\\{c}" if c in _QUERY_SPECIAL else c for c in text)


def escape_tag(text: str) -> str:
    """Escape special characters for a RediSearch TAG field value."""
    if not text:
        return ""
    return "".join(f"\\{c}" if c in _TAG_SPECIAL else c for c in text)


# =============================================================================
# INDEX MANAGEMENT
# =============================================================================

def create_index() -> bool:
    """Create the RediSearch index (STOPWORDS 0 — index every token)."""
    client = RedisLookupTable.get_client()
    if not client:
        return False

    try:
        try:
            client.ft(INDEX_NAME).info()
            logger.info("Index '%s' already exists", INDEX_NAME)
            return True
        except Exception:
            pass  # index doesn't exist yet — create it

        client.execute_command(
            "FT.CREATE", INDEX_NAME,
            "ON", "HASH",
            "PREFIX", "1", "term:",
            "STOPWORDS", "0",
            "SCHEMA",
            "term",        "TEXT",    "WEIGHT", "5.0",
            "display",     "TEXT",    "WEIGHT", "3.0",
            "category",    "TAG",     "SORTABLE",
            "description", "TEXT",    "WEIGHT", "1.0",
            "pos",         "TAG",
            "entity_type", "TAG",
            "rank",        "NUMERIC", "SORTABLE",
        )
        logger.info("Index '%s' created successfully (STOPWORDS 0)", INDEX_NAME)
        return True

    except Exception:
        logger.exception("Error creating index '%s'", INDEX_NAME)
        return False


def drop_index() -> bool:
    """Drop the index while keeping the underlying data."""
    client = RedisLookupTable.get_client()
    if not client:
        return False

    try:
        client.ft(INDEX_NAME).dropindex(delete_documents=False)
        logger.info("Index '%s' dropped", INDEX_NAME)
        return True
    except Exception:
        logger.exception("Error dropping index '%s'", INDEX_NAME)
        return False


# =============================================================================
# INDEX INTROSPECTION UTILITIES
# =============================================================================

def get_index_info(client: Optional[redis.Redis] = None) -> Dict[str, Any]:
    """
    Return RediSearch index metadata as a flat key/value dict.
    Returns an empty dict if the index does not exist or the call fails.
    """
    client = client or RedisLookupTable.get_client()
    if not client:
        return {}

    try:
        raw = client.ft(INDEX_NAME).info()
        items = list(raw)
        return {items[i]: items[i + 1] for i in range(0, len(items) - 1, 2)}
    except redis.ResponseError:
        return {}
    except Exception:
        logger.warning("Could not read index info for '%s'", INDEX_NAME, exc_info=True)
        return {}


def index_has_field(field_name: str, client: Optional[redis.Redis] = None) -> bool:
    """Return True if *field_name* is a registered attribute on INDEX_NAME."""
    client = client or RedisLookupTable.get_client()
    if not client:
        return False

    info = get_index_info(client)
    attributes = info.get("attributes", [])
    field_lower = field_name.lower()

    for attr in attributes:
        flat = list(attr)
        attr_dict = {
            str(flat[i]).lower(): flat[i + 1]
            for i in range(0, len(flat) - 1, 2)
        }
        if attr_dict.get("identifier", "").lower() == field_lower:
            return True
    return False


def get_index_status() -> Dict[str, Any]:
    """
    Return a structured summary of the current index and key counts.

    Intended for health-check endpoints or management commands.
    Does NOT print — callers decide how to surface the data.
    """
    client = RedisLookupTable.get_client()
    if not client:
        return {"index_exists": False, "error": "Redis connection failed"}

    info = get_index_info(client)

    if not info:
        return {
            "index_exists": False,
            "index_name": INDEX_NAME,
            "message": "Index not found — run create_index() to create it",
        }

    index_def_str = str(info.get("index_definition", ""))
    term_count     = sum(1 for _ in client.scan_iter("term:*",      count=100))
    question_count = sum(1 for _ in client.scan_iter("questions:*", count=100))

    return {
        "index_exists": True,
        "index_name": INDEX_NAME,
        "num_docs": info.get("num_docs", "unknown"),
        "indexing_failures": info.get("hash_indexing_failures", 0),
        "fields": {
            "term_exact":        index_has_field("term_exact",  client),
            "entity_type":       index_has_field("entity_type", client),
            "rank":              index_has_field("rank",        client),
            "questions_prefix":  "questions:" in index_def_str,
        },
        "key_counts": {
            "term_keys":     term_count,
            "question_keys": question_count,
        },
    }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_base_term(member: str) -> str:
    """Extract the base term from a document ID (``term:xxx:category`` → ``xxx``)."""
    if not member:
        return ""

    if member.startswith("term:"):
        member = member[5:]

    parts = member.rsplit(":", 1)
    if len(parts) == 2:
        potential_category = parts[1].lower()
        if potential_category in _CATEGORY_KEYWORDS or len(potential_category) < 15:
            return parts[0]

    return member


def _safe_int(value: Any, default: int = 0) -> int:
    """Safely coerce *value* to int, returning *default* on failure."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def parse_search_doc(doc: Any) -> Dict[str, Any]:
    """Parse a RediSearch document into a standard dict format."""
    try:
        term = getattr(doc, "term", "")
        return {
            "id":          doc.id,
            "member":      doc.id,
            "term":        term,
            "display":     getattr(doc, "display", term),
            "description": getattr(doc, "description", ""),
            "category":    getattr(doc, "category", ""),
            "entity_type": getattr(doc, "entity_type", ""),
            "pos":         getattr(doc, "pos", ""),
            "rank":        _safe_int(getattr(doc, "rank", 0)),
            "exists":      True,
        }
    except Exception:
        logger.exception("Error parsing RediSearch doc")
        return {}


# =============================================================================
# CORE SEARCH FUNCTIONS
# =============================================================================

def get_term_metadata(member: str) -> Optional[Dict[str, Any]]:
    """Get metadata for a single term via a direct HGETALL lookup."""
    client = RedisLookupTable.get_client()
    if not client or not member:
        return None

    try:
        hash_key = member if member.startswith("term:") else f"term:{member}"
        metadata = client.hgetall(hash_key)

        if not metadata:
            return None

        base_term = extract_base_term(member)
        return {
            "member":      member,
            "term":        metadata.get("term", base_term),
            "exists":      True,
            "display":     metadata.get("display", base_term),
            "pos":         metadata.get("pos", "unknown"),
            "category":    metadata.get("category", ""),
            "description": metadata.get("description", ""),
            "entity_type": metadata.get("entity_type", ""),
            "rank":        _safe_int(metadata.get("rank", 0)),
        }

    except Exception:
        logger.exception("Error getting term metadata for '%s'", member)
        return None


def get_exact_term_matches(term: str) -> List[Dict[str, Any]]:
    """
    Find all Redis hashes whose key matches ``term:{word}:*``.

    Uses direct key scan — not RediSearch full-text search — so stop-words
    and tokenisation rules do not affect results.
    """
    client = RedisLookupTable.get_client()
    if not client or not term:
        return []

    term_lower = term.lower().strip()
    if not term_lower:
        return []

    # Multi-word terms use underscores in the key
    term_key = term_lower.replace(" ", "_")

    try:
        keys = client.keys(f"term:{term_key}:*")
        if not keys:
            return []

        matches: List[Dict[str, Any]] = []
        for key in keys:
            metadata = client.hgetall(key)
            if not metadata:
                continue

            # matches.append({
            #     "id":            key,
            #     "member":        key,
            #     "term":          metadata.get("question", ""),
            #     "display":       metadata.get("question", ""),
            #     "description":   "",
            #     "category":      "question",
            #     "entity_type":   "question",
            #     "pos":           "question",
            #     "rank":          _safe_int(metadata.get("score", 0)),
            #     "exists":        True,
            #     "document_uuid": metadata.get("document_uuid", ""),
            #     "answer":        metadata.get("answer", ""),
            #     "answer_type":   metadata.get("answer_type", "UNKNOWN"),
            # })

            matches.append({
            "id":            key,
            "member":        key,
            "term":          metadata.get("term", ""),      # ← correct
            "display":       metadata.get("display", ""),   # ← correct
            "description":   metadata.get("description", ""),
            "category":      metadata.get("category", ""),
            "entity_type":   metadata.get("entity_type", ""),
            "pos":           metadata.get("pos", ""),
            "rank":          _safe_int(metadata.get("rank", 0)),  # ← correct
            "exists":        True,
            })

        matches.sort(key=lambda x: x.get("rank", 0), reverse=True)
        return matches

    except Exception:
        logger.exception("Exact match error for term '%s'", term)
        return []



# =============================================================================
# QUESTION LOOKUP — REDISEARCH (questions_idx)
# =============================================================================

def get_question_matches(prefix: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Return question hashes matching the prefix via RediSearch questions_idx.

    Splits the input on spaces, escapes each word individually, and appends
    the wildcard ``*`` to the last word only — so spaces act as RediSearch
    token separators and multi-word prefixes like ``"who was the"`` work.
    """
    client = RedisLookupTable.get_client()
    if not client or not prefix:
        return []

    prefix_lower = prefix.lower().strip()
    if len(prefix_lower) < 2:
        return []

    try:
        words = prefix_lower.split()
        if not words:
            return []

        escaped_words = [escape_query(w) for w in words]
        escaped_words[-1] = escaped_words[-1] + "*"
        query_str = " ".join(escaped_words)

        query = Query(query_str).sort_by("score", asc=False).paging(0, limit)
        result = client.ft(QUESTIONS_INDEX_NAME).search(query)

        matches: List[Dict[str, Any]] = []
        for doc in result.docs:
            matches.append({
                "id":            doc.id,
                "member":        doc.id,
                "term":          getattr(doc, "question", ""),
                "display":       getattr(doc, "question", ""),
                "description":   "",
                "category":      "question",
                "entity_type":   "question",
                "pos":           "question",
                "rank":          _safe_int(getattr(doc, "score", 0)),
                "exists":        True,
                "document_uuid": getattr(doc, "document_uuid", ""),
                "answer":        getattr(doc, "answer", ""),
                "answer_type":   getattr(doc, "answer_type", "UNKNOWN"),
            })
        return matches

    except Exception:
        logger.exception("Question match error for prefix '%s'", prefix)
        return []


def get_prefix_matches(prefix: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return terms that start with *prefix* using a RediSearch prefix query."""
    client = RedisLookupTable.get_client()
    if not client or not prefix:
        return []

    prefix_lower = prefix.lower().strip()
    if not prefix_lower:
        return []

    try:
        query_str = f"{escape_query(prefix_lower)}*"
        query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)

        return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

    except Exception:
        logger.exception("Prefix match error for prefix '%s'", prefix)
        return []


# kept for backwards compatibility
def get_prefix_matches_with_rank(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    return get_prefix_matches(prefix, limit=limit)


# =============================================================================
# DAMERAU-LEVENSHTEIN DISTANCE
# =============================================================================

def damerau_levenshtein_distance(s1: str, s2: str) -> int:
    """
    Damerau-Levenshtein distance between two strings.

    Uses the fast C extension when available, falls back to a pure-Python
    implementation so the module works in all environments.
    """
    if _USE_FAST_DL:
        return _fast_dl_distance(s1, s2)  # type: ignore[return-value]

    len1, len2 = len(s1), len(s2)
    d = [[0] * (len2 + 1) for _ in range(len1 + 1)]

    for i in range(len1 + 1):
        d[i][0] = i
    for j in range(len2 + 1):
        d[0][j] = j

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            d[i][j] = min(
                d[i - 1][j] + 1,
                d[i][j - 1] + 1,
                d[i - 1][j - 1] + cost,
            )
            if i > 1 and j > 1 and s1[i - 1] == s2[j - 2] and s1[i - 2] == s2[j - 1]:
                d[i][j] = min(d[i][j], d[i - 2][j - 2] + cost)

    return d[len1][len2]


def calculate_score(distance: int, rank: int, max_rank: int = 10_000_000) -> float:
    """Combined ranking score: lower distance + higher rank = lower (better) score."""
    rank_bonus = min(rank, max_rank) / max_rank
    return distance - rank_bonus


# =============================================================================
# FUZZY SEARCH (SPELL CORRECTION)
# =============================================================================

def _run_fuzzy_query(
    client: redis.Redis,
    query_str: str,
    input_lower: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """Execute a single fuzzy RediSearch query and return enriched results."""
    query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
    result = client.ft(INDEX_NAME).search(query)

    matches: List[Dict[str, Any]] = []
    for doc in result.docs:
        parsed = parse_search_doc(doc)
        if parsed:
            parsed["distance"] = damerau_levenshtein_distance(
                input_lower, parsed["term"].lower()
            )
            matches.append(parsed)

    matches.sort(key=lambda x: (x["distance"], -x.get("rank", 0)))
    return matches


def get_fuzzy_matches(
    term: str,
    limit: int = 10,
    max_distance: int = 2,
) -> List[Dict[str, Any]]:
    """
    Return fuzzy matches using RediSearch Levenshtein operators.

    - ``%term%``  = 1 edit distance
    - ``%%term%%`` = 2 edit distance

    For multi-word queries only the *last* word is fuzzified.
    """
    client = RedisLookupTable.get_client()
    if not client or not term:
        return []

    term_lower = term.lower().strip()
    if len(term_lower) < 3:
        return []

    words = term_lower.split()

    try:
        if len(words) > 1:
            prefix_part = escape_query(" ".join(words[:-1]))
            last_escaped = escape_query(words[-1])

            if len(words[-1]) < 3:
                return []

            for distance, wraps in ((1, ("%", "%")), (2, ("%%", "%%"))):
                if max_distance < distance:
                    continue
                query_str = f"{prefix_part} {wraps[0]}{last_escaped}{wraps[1]}"
                try:
                    matches = _run_fuzzy_query(client, query_str, term_lower, limit)
                    if matches:
                        return matches[:limit]
                except Exception:
                    logger.debug("Fuzzy query failed (%d-edit, multi-word): %s", distance, query_str)

            return []

        # Single word
        escaped = escape_query(term_lower)
        for distance, wraps in ((1, ("%", "%")), (2, ("%%", "%%"))):
            if max_distance < distance:
                continue
            query_str = f"{wraps[0]}{escaped}{wraps[1]}"
            try:
                matches = _run_fuzzy_query(client, query_str, term_lower, limit)
                if matches:
                    return matches[:limit]
            except Exception:
                logger.debug("Fuzzy query failed (%d-edit, single-word): %s", distance, query_str)

        return []

    except Exception:
        logger.exception("Fuzzy match error for term '%s'", term)
        return []


# =============================================================================
# BATCH OPERATIONS
# =============================================================================

def batch_get_term_metadata(members: List[str]) -> Dict[str, Dict[str, Any]]:
    """Batch-fetch metadata for multiple terms using a Redis pipeline."""
    client = RedisLookupTable.get_client()
    if not client or not members:
        return {}

    try:
        pipeline = client.pipeline()
        for member in members:
            hash_key = member if member.startswith("term:") else f"term:{member}"
            pipeline.hgetall(hash_key)

        results = pipeline.execute()
        metadata_dict: Dict[str, Dict[str, Any]] = {}

        for member, metadata in zip(members, results):
            if not metadata:
                continue
            base_term = extract_base_term(member)
            metadata_dict[member] = {
                "member":      member,
                "term":        metadata.get("term", base_term),
                "exists":      True,
                "display":     metadata.get("display", base_term),
                "pos":         metadata.get("pos", "unknown"),
                "category":    metadata.get("category", ""),
                "description": metadata.get("description", ""),
                "entity_type": metadata.get("entity_type", ""),
                "rank":        _safe_int(metadata.get("rank", 0)),
            }

        return metadata_dict

    except Exception:
        logger.exception("Batch metadata error")
        return {}


def batch_validate_words_redis(words: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Validate a list of words against Redis.

    Returns a dict mapping ``word_lower`` → validation result with metadata.
    """
    if not words:
        return {}

    client = RedisLookupTable.get_client()
    if not client:
        return {}

    results: Dict[str, Dict[str, Any]] = {}

    try:
        for word in words:
            word_lower = word.lower().strip()
            if not word_lower:
                continue

            matches = get_exact_term_matches(word_lower)
            if matches:
                results[word_lower] = {
                    "is_valid": True,
                    "word":     word_lower,
                    "member":   matches[0].get("id", ""),
                    "matches":  matches,
                    "metadata": matches[0],
                }
            else:
                results[word_lower] = {
                    "is_valid": False,
                    "word":     word_lower,
                    "member":   None,
                    "matches":  [],
                    "metadata": {},
                }

        return results

    except Exception:
        logger.exception("Batch validation error")
        return {}


# =============================================================================
# VALIDATE WORD (SPELL CHECK)
# =============================================================================

def validate_word(
    word: str,
    _pre_validated: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Validate a single word and return spelling correction metadata if needed."""
    word_lower = word.lower().strip()

    # Fast path: already validated upstream
    if _pre_validated is not None:
        pre = _pre_validated.get(word_lower)
        if pre and pre.get("is_valid"):
            meta = pre.get("metadata", {})
            return {
                "word":       word,
                "is_valid":   True,
                "suggestion": None,
                "metadata": {
                    "display":  meta.get("display", word_lower),
                    "pos":      meta.get("pos", "unknown"),
                    "category": meta.get("category", ""),
                    "rank":     meta.get("rank", 0),
                },
            }

    exact_matches = get_exact_term_matches(word_lower)
    if exact_matches:
        meta = exact_matches[0]
        return {
            "word":       word,
            "is_valid":   True,
            "suggestion": None,
            "metadata": {
                "display":  meta.get("display", word_lower),
                "pos":      meta.get("pos", "unknown"),
                "category": meta.get("category", ""),
                "rank":     meta.get("rank", 0),
            },
        }

    result = get_suggestions(word_lower, limit=1, max_distance=2)
    if result["suggestions"]:
        best = result["suggestions"][0]
        return {
            "word":       word,
            "is_valid":   False,
            "suggestion": best["term"],
            "distance":   best.get("distance", 0),
            "score":      best.get("score", 0),
            "tier_used":  result["tier_used"],
            "metadata": {
                "display":  best.get("display", ""),
                "pos":      best.get("pos", "unknown"),
                "category": best.get("category", ""),
                "rank":     best.get("rank", 0),
            },
        }

    return {"word": word, "is_valid": False, "suggestion": None}


# =============================================================================
# GET SUGGESTIONS (UNIFIED SEARCH)
# =============================================================================

def get_suggestions(
    input_text: str,
    limit: int = 10,
    max_distance: int = 2,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified suggestion function using RediSearch.

    Returns combined results from Exact → Prefix → Fuzzy tiers,
    sorted by rank descending then edit-distance ascending.

    Questions are intentionally excluded here; they are prepended by
    ``get_autocomplete()`` on the keyword path only.
    """
    response: Dict[str, Any] = {
        "success":     True,
        "input":       input_text,
        "suggestions": [],
        "exact_match": False,
        "tier_used":   None,
        "error":       None,
    }

    client = RedisLookupTable.get_client()
    if not client:
        response.update(success=False, error="Redis connection failed")
        return response

    if not input_text or not input_text.strip():
        response.update(success=False, error="Empty input")
        return response

    input_lower = input_text.lower().strip()

    try:
        all_results: List[Dict[str, Any]] = []
        seen_terms:  Set[str] = set()
        tiers_used:  List[str] = []

        # --- Tier 1: exact match ---
        for match in get_exact_term_matches(input_lower):
            term_lower = match.get("term", "").lower()
            if term_lower not in seen_terms:
                match.update(distance=0, score=-match.get("rank", 0))
                all_results.append(match)
                seen_terms.add(term_lower)

        if all_results:
            response["exact_match"] = True
            tiers_used.append("exact")

        # --- Tier 2: prefix match (always run) ---
        for item in get_prefix_matches(input_lower, limit=limit * 3):
            term_lower = item.get("term", "").lower()
            if term_lower not in seen_terms:
                distance = damerau_levenshtein_distance(input_lower, term_lower)
                item.update(distance=distance, score=calculate_score(distance, item.get("rank", 0)))
                all_results.append(item)
                seen_terms.add(term_lower)
                tiers_used.append("prefix") if "prefix" not in tiers_used else None

        # --- Tier 3: fuzzy match (only when results are sparse) ---
        if len(all_results) < limit:
            for item in get_fuzzy_matches(input_lower, limit=limit * 2, max_distance=max_distance):
                term_lower = item.get("term", "").lower()
                if term_lower not in seen_terms and item.get("distance", 99) <= max_distance:
                    item["score"] = calculate_score(item["distance"], item.get("rank", 0))
                    all_results.append(item)
                    seen_terms.add(term_lower)
                    tiers_used.append("fuzzy") if "fuzzy" not in tiers_used else None

        all_results.sort(key=lambda x: (-x.get("rank", 0), x.get("distance", 99)))

        response["suggestions"] = all_results[:limit]
        response["tier_used"]   = "+".join(tiers_used) if tiers_used else "none"
        return response

    except Exception:
        logger.exception("get_suggestions error for '%s'", input_text)
        response.update(success=False, error="Internal search error")
        return response


# =============================================================================
# AUTOCOMPLETE
# =============================================================================

# def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
#     """
#     Return autocomplete suggestions: questions first, then terms.

#     - Up to 3 question results sourced from ``get_question_matches()``
#     - Remaining slots filled with term suggestions from ``get_suggestions()``
#     - Frontend can split on ``entity_type == 'question'`` to render separately
#     """
#     if not prefix or len(prefix.strip()) < 2:
#         return []

#     prefix_clean = prefix.strip()

#     question_results = get_question_matches(prefix_clean, limit=3)
#     term_results = get_suggestions(prefix_clean, limit=limit - len(question_results))

#     return question_results + term_results.get("suggestions", [])

# Interrogative words that signal the user is typing a question
_INTERROGATIVES: frozenset[str] = frozenset({
    "who", "what", "when", "where", "why", "which", "whose", "how",
    "is", "are", "was", "were", "do", "does", "did",
    "can", "could", "should", "will", "would",
})


def get_autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Return autocomplete suggestions: terms first, then questions.

    Questions are only fetched when the input starts with an interrogative
    word (who, what, when, where, why, how, etc.) — typing ``jeff`` returns
    only term suggestions, while ``who was jeff`` returns questions too.

    - Terms come from ``get_suggestions()`` (exact → prefix → fuzzy tiers)
    - Questions (if triggered) come from ``get_question_matches()``,
      appended after terms
    - Frontend can split on ``entity_type == 'question'`` to render separately
    """
    if not prefix or len(prefix.strip()) < 2:
        return []

    prefix_clean = prefix.strip()
    first_word = prefix_clean.lower().split()[0] if prefix_clean else ""
    is_question_intent = first_word in _INTERROGATIVES

    if is_question_intent:
        # Reserve a few slots for questions, fill the rest with terms
        term_results = get_suggestions(prefix_clean, limit=limit - 3)
        question_results = get_question_matches(prefix_clean, limit=3)
        return term_results.get("suggestions", []) + question_results

    # Keyword search — terms only
    term_results = get_suggestions(prefix_clean, limit=limit)
    return term_results.get("suggestions", [])


# =============================================================================
# TOP WORDS BY RANK
# =============================================================================

def get_top_words_by_rank(limit: int = 200) -> List[Dict[str, Any]]:
    """Return the top *limit* words sorted by rank descending."""
    client = RedisLookupTable.get_client()
    if not client:
        return []

    try:
        query = Query("*").sort_by("rank", asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)
        return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

    except Exception:
        logger.exception("Error getting top words by rank")
        return []


# =============================================================================
# FILTERED SEARCH
# =============================================================================

def search_by_category(
    query_text: str,
    category: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Search within a specific category."""
    client = RedisLookupTable.get_client()
    if not client or not query_text or not category:
        return []

    try:
        query_str = (
            f"{escape_query(query_text.lower().strip())}*"
            f" @category:{{{escape_tag(category)}}}"
        )
        query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)
        return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

    except Exception:
        logger.exception("Category search error for '%s' / '%s'", query_text, category)
        return []


def search_by_entity_type(
    query_text: str,
    entity_type: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Search by entity type (unigram, bigram, trigram, …)."""
    client = RedisLookupTable.get_client()
    if not client or not query_text or not entity_type:
        return []

    try:
        query_str = (
            f"{escape_query(query_text.lower().strip())}*"
            f" @entity_type:{{{escape_tag(entity_type)}}}"
        )
        query = Query(query_str).sort_by("rank", asc=False).paging(0, limit)
        result = client.ft(INDEX_NAME).search(query)
        return [doc for doc in (parse_search_doc(d) for d in result.docs) if doc]

    except Exception:
        logger.exception("Entity-type search error for '%s' / '%s'", query_text, entity_type)
        return []


# =============================================================================
# CACHE FUNCTIONS
# =============================================================================

def check_cache(query: str) -> Optional[Dict[str, Any]]:
    """Return cached results for *query*, or ``None`` on a cache miss."""
    client = RedisLookupTable.get_client()
    if not client:
        return None

    try:
        cache_key = f"query_cache:{query.lower().strip()}"
        cached = client.get(cache_key)
        return json.loads(cached) if cached else None

    except Exception:
        logger.exception("Cache check error for query '%s'", query)
        return None


def save_to_cache(query: str, results: Dict[str, Any], ttl: int = 3600) -> bool:
    """Persist *results* in Redis with a *ttl*-second expiry."""
    client = RedisLookupTable.get_client()
    if not client:
        return False

    try:
        cache_key = f"query_cache:{query.lower().strip()}"
        client.setex(cache_key, ttl, json.dumps(results))
        return True

    except Exception:
        logger.exception("Cache save error for query '%s'", query)
        return False


# =============================================================================
# MAIN API FUNCTION
# =============================================================================

def lookup_table(
    query: str,
    check_cache_first: bool = True,
    include_suggestions: bool = True,
    autocomplete_prefix: Optional[str] = None,
    autocomplete_limit: int = 10,
    return_validation_cache: bool = False,
) -> Dict[str, Any]:
    """
    Main API entry point for Redis-based search preprocessing.

    Handles autocomplete, cache, per-word validation, and spell correction
    in a single call.
    """
    response: Dict[str, Any] = {
        "success":          True,
        "query":            query,
        "normalized_query": "",
        "terms":            [],
        "cache_hit":        False,
        "autocomplete":     [],
        "error":            None,
    }

    try:
        if autocomplete_prefix:
            response["autocomplete"] = get_autocomplete(
                autocomplete_prefix, limit=autocomplete_limit
            )
            return response

        if not query or not query.strip():
            response.update(success=False, error="Empty query")
            return response

        if check_cache_first:
            cached = check_cache(query)
            if cached:
                response.update(
                    cache_hit=True,
                    terms=cached.get("terms", []),
                    normalized_query=cached.get("normalized_query", ""),
                )
                return response

        words = query.lower().split()
        validation_cache = batch_validate_words_redis(words)

        terms: List[Dict[str, Any]] = []
        normalized_words: List[str] = []

        for i, word in enumerate(words):
            word = word.strip()
            if not word:
                continue

            validation = validate_word(word, _pre_validated=validation_cache)

            if validation["is_valid"]:
                terms.append({
                    "position": i + 1,
                    "word":     word,
                    "exists":   True,
                    "display":  validation["metadata"]["display"],
                    "pos":      validation["metadata"]["pos"],
                    "category": validation["metadata"]["category"],
                    "rank":     validation["metadata"]["rank"],
                    "metadata": validation["metadata"],
                })
                normalized_words.append(word)
            else:
                terms.append({
                    "position":  i + 1,
                    "word":      word,
                    "exists":    False,
                    "suggestion": validation.get("suggestion"),
                    "distance":  validation.get("distance"),
                    "score":     validation.get("score"),
                    "tier_used": validation.get("tier_used"),
                    "metadata":  validation.get("metadata", {}),
                })
                if include_suggestions and validation.get("suggestion"):
                    normalized_words.append(validation["suggestion"])

        response["terms"]            = terms
        response["normalized_query"] = " ".join(normalized_words)

        if return_validation_cache:
            response["_validation_cache"] = validation_cache

        save_to_cache(query, {"terms": terms, "normalized_query": response["normalized_query"]})
        return response

    except Exception:
        logger.exception("lookup_table error for query '%s'", query)
        response.update(success=False, error="Internal error")
        return response


# =============================================================================
# LEGACY / BACKWARDS-COMPATIBILITY FUNCTIONS
# =============================================================================

def generate_candidates_smart(word: str, max_candidates: int = 100) -> Set[str]:
    """
    Generate spelling candidates from common typo patterns.

    Kept for backwards compatibility — ``get_fuzzy_matches()`` is preferred.
    """
    candidates: Set[str] = set()
    word_lower = word.lower()
    length = len(word_lower)

    if length < 2:
        return candidates

    alphabet = string.ascii_lowercase
    vowels   = "aeiou"

    keyboard_proximity: Dict[str, str] = {
        "q": "wa",    "w": "qeas",  "e": "wrds",  "r": "etdf",  "t": "ryfg",
        "y": "tugh",  "u": "yihj",  "i": "uojk",  "o": "ipkl",  "p": "ol",
        "a": "qwsz",  "s": "awedxz","d": "serfcx","f": "drtgvc","g": "ftyhbv",
        "h": "gyujnb","j": "huikmn","k": "jiolm", "l": "kop",
        "z": "asx",   "x": "zsdc",  "c": "xdfv",  "v": "cfgb",  "b": "vghn",
        "n": "bhjm",  "m": "njk",
    }

    for i in range(length):
        char = word_lower[i]
        for nearby in keyboard_proximity.get(char, ""):
            candidates.add(word_lower[:i] + nearby + word_lower[i + 1:])

    for i in range(length - 1):
        candidates.add(word_lower[:i] + word_lower[i + 1] + word_lower[i] + word_lower[i + 2:])

    for i in range(length):
        candidate = word_lower[:i] + word_lower[i + 1:]
        if candidate:
            candidates.add(candidate)

    for i in range(length):
        if word_lower[i] in vowels:
            for v in vowels:
                if v != word_lower[i]:
                    candidates.add(word_lower[:i] + v + word_lower[i + 1:])

    if len(candidates) < max_candidates // 2:
        for i in range(length + 1):
            for char in alphabet:
                candidates.add(word_lower[:i] + char + word_lower[i:])
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= max_candidates:
                break

    return set(list(candidates)[:max_candidates])


def batch_check_candidates(candidates: Set[str]) -> List[Dict[str, Any]]:
    """Check whether candidates exist in Redis. Kept for backwards compatibility."""
    found: List[Dict[str, Any]] = []
    for candidate in list(candidates)[:50]:
        matches = get_exact_term_matches(candidate)
        if matches:
            found.append(matches[0])
    return found


def batch_check_bigrams(word_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
    """Check multiple bigrams. Kept for backwards compatibility."""
    results: Dict[str, Dict[str, Any]] = {}
    for w1, w2 in word_pairs:
        bigram = f"{w1.lower()} {w2.lower()}"
        matches = get_exact_term_matches(bigram)
        if matches:
            results[bigram] = matches[0]
    return results


# =============================================================================
# CONVENIENCE SHORTHANDS
# =============================================================================

def lookup(query: str) -> Dict[str, Any]:
    """Shorthand for ``lookup_table`` with default settings."""
    return lookup_table(query)


def autocomplete(prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Shorthand for ``get_autocomplete``."""
    return get_autocomplete(prefix, limit)


def spell_check(word: str) -> Dict[str, Any]:
    """Shorthand for ``validate_word``."""
    return validate_word(word)