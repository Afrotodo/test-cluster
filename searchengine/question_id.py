"""
question_id.py

Fire-and-forget score increment for question hashes.

Called from the bridge whenever a user clicks a question in the dropdown.
Runs in a background thread so the search response is never blocked.

The bridge passes in the question text (query) and the question_id (UUID).
The helper rebuilds the Redis key from the query by lowercasing it,
stripping punctuation, and replacing spaces with underscores —
matching how the keys were originally written.

Each bump:
- Increments the `score` field on the question hash by 1
- Updates `last_clicked_at` (unix timestamp) so decay can be applied later
"""

import logging
import re
import threading
import time

import redis
from decouple import config

logger = logging.getLogger(__name__)

QUESTION_KEY_PREFIX = "questions:"

_client: redis.Redis | None = None
_client_lock = threading.Lock()


def _get_client() -> redis.Redis | None:
    """Lazily build a Redis client from env vars. Cached for reuse."""
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:
            return _client

        host = config("REDIS_LOCATION")
        port = config("REDIS_PORT")
        password = config("REDIS_PASSWORD", default="")
        username = config("REDIS_USERNAME", default="")
        db = config("REDIS_DB", default="0")

        if not host:
            logger.warning("REDIS_LOCATION not set; cannot build Redis client")
            return None

        try:
            _client = redis.Redis(
                host=host,
                port=int(port),
                db=int(db),
                username=username or None,
                password=password or None,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        except Exception:
            logger.exception("Failed to construct Redis client")
            return None

        return _client


def _query_to_key(query: str) -> str:
    """
    Rebuild the questions:<slug> Redis key from the question text.

    Example:
        "Who was the assistant pianist who led the Fisk Jubilee Singers?"
        → "questions:who_was_the_assistant_pianist_who_led_the_fisk_jubilee_singers"
    """
    slug = query.lower()
    slug = re.sub(r"[^a-z0-9\s]", "", slug)    # strip punctuation
    slug = re.sub(r"\s+", "_", slug.strip())   # collapse whitespace into underscores
    return f"{QUESTION_KEY_PREFIX}{slug}"


def bump_question_score(query: str, question_id: str = "") -> None:
    """
    Fire-and-forget. Returns immediately. The bump runs in a background thread.

    Args:
        query:       The question text, e.g. "Who was the assistant pianist..."
        question_id: The question's UUID (logged for traceability; not required
                     for the write itself).
    """
    print(f"🎯 bump_question_score called  query={query!r}  question_id={question_id!r}")

    if not query:
        print("❌ Empty query; skipping bump")
        return

    thread = threading.Thread(
        target=_do_bump,
        args=(query, question_id),
        daemon=True,
    )
    thread.start()


def _do_bump(query: str, question_id: str) -> None:
    """Background worker — does the actual Redis writes."""
    try:
        client = _get_client()
        if client is None:
            print("❌ No Redis client")
            return

        key = _query_to_key(query)
        print(f"🔑 Computed key: {key!r}")

        if not client.exists(key):
            print(f"⚠️  Key not found in Redis: {key!r}  (question_id={question_id!r})")
            return

        new_score = client.hincrby(key, "score", 100)
        client.hset(key, "last_clicked_at", int(time.time()))

        print(f"✅ Bumped {key} → score={new_score}")
        logger.debug("Bumped %s → score=%d (question_id=%s)", key, new_score, question_id)

    except Exception:
        logger.exception("Failed to bump score for query=%r question_id=%r", query, question_id)
        print(f"❌ Exception bumping query={query!r}")