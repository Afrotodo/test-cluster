"""
question_id.py

Fire-and-forget score increment for question hashes.

Called from the bridge whenever a user clicks a question in the dropdown.
Runs in a background thread so the search response is never blocked.

Each (session_id, question_id) pair can bump the score only once,
enforced via a short-lived Redis marker key.
"""

import logging
import re
import threading
import time

import redis
from decouple import config

logger = logging.getLogger(__name__)

QUESTION_KEY_PREFIX = "questions:"
SESSION_CLICK_PREFIX = "session_click:"
SESSION_CLICK_TTL_SECONDS = 24 * 60 * 60   # 24h — matches a typical session lifetime

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
        password = config("REDIS_PASSWORD")
        username = config("REDIS_USERNAME")
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
    """Rebuild the questions:<slug> Redis key from the question text."""
    slug = query.lower()
    slug = re.sub(r"[^a-z0-9\s]", "", slug)
    slug = re.sub(r"\s+", "_", slug.strip())
    return f"{QUESTION_KEY_PREFIX}{slug}"


def bump_question_score(
    query: str,
    question_id: str = "",
    session_id: str = "",
) -> None:
    """
    Fire-and-forget. Returns immediately. Bump runs in a background thread.

    A (session_id, question_id) pair can only bump once within
    SESSION_CLICK_TTL_SECONDS.
    """
    if not query:
        return

    thread = threading.Thread(
        target=_do_bump,
        args=(query, question_id, session_id),
        daemon=True,
    )
    thread.start()


def _do_bump(query: str, question_id: str, session_id: str) -> None:
    """Background worker — claims the session marker, then writes."""
    try:
        client = _get_client()
        if client is None:
            return

        # Atomically claim the (session, question) marker.
        # If it already exists, this session already bumped this question — skip.
        if session_id and question_id:
            marker_key = f"{SESSION_CLICK_PREFIX}{session_id}:{question_id}"
            claimed = client.set(
                marker_key,
                "1",
                nx=True,
                ex=SESSION_CLICK_TTL_SECONDS,
            )
            if not claimed:
                logger.debug(
                    "Session %s already bumped question %s; skipping",
                    session_id, question_id
                )
                return

        key = _query_to_key(query)

        if not client.exists(key):
            logger.info("Key not found in Redis: %r", key)
            return

        new_score = client.hincrby(key, "score", 100)
        client.hset(key, "last_clicked_at", int(time.time()))

        logger.debug(
            "Bumped %s → score=%d (question_id=%s session=%s)",
            key, new_score, question_id, session_id,
        )

    except Exception:
        logger.exception(
            "Failed to bump score for query=%r question_id=%r session=%r",
            query, question_id, session_id,
        )