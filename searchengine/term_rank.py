"""
term_rank.py

Fire-and-forget rank increment for term hashes.

Called from the bridge when a user clicks a term suggestion in the dropdown.
Runs in a background thread so the search response is never blocked.

Each (session_id, term_key) pair can bump only once within
SESSION_CLICK_TTL_SECONDS, preventing a single user from inflating
a term's rank by clicking the same suggestion repeatedly.

Each bump:
- Increments the `rank` field on the term hash by RANK_BUMP
- Updates `last_clicked_at` (unix timestamp) so decay can be applied later
"""

import logging
import threading
import time

import redis
from decouple import config

logger = logging.getLogger(__name__)

TERM_KEY_PREFIX = "term:"
SESSION_CLICK_PREFIX = "session_term_click:"
SESSION_CLICK_TTL_SECONDS = 24 * 60 * 60   # 24h
RANK_BUMP = 10000                            # tune to taste

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


def bump_term_rank(term_key: str, session_id: str = "") -> None:
    """
    Fire-and-forget. Returns immediately. Bump runs in a background thread.

    Args:
        term_key:   Full Redis key, e.g. "term:13th_cavalry:organization"
        session_id: Browser session ID — used to dedupe one click per session.
    """
    if not term_key:
        return

    # Defensive: only allow keys under the term: namespace
    if not term_key.startswith(TERM_KEY_PREFIX):
        logger.warning("Refusing to bump non-term key: %r", term_key)
        return

    thread = threading.Thread(
        target=_do_bump,
        args=(term_key, session_id),
        daemon=True,
    )
    thread.start()


def _do_bump(term_key: str, session_id: str) -> None:
    """Background worker — claims the session marker, then writes."""
    try:
        client = _get_client()
        if client is None:
            return

        # Atomically claim the (session, term) marker.
        # If it already exists, this session has already bumped this term.
        if session_id:
            marker_key = f"{SESSION_CLICK_PREFIX}{session_id}:{term_key}"
            claimed = client.set(
                marker_key,
                "1",
                nx=True,
                ex=SESSION_CLICK_TTL_SECONDS,
            )
            if not claimed:
                logger.debug(
                    "Session %s already bumped term %s; skipping",
                    session_id, term_key,
                )
                return

        # Guard against typos / stale keys creating empty hashes
        if not client.exists(term_key):
            logger.info("Term key not found in Redis: %r", term_key)
            return

        new_rank = client.hincrby(term_key, "rank", RANK_BUMP)
        client.hset(term_key, "last_clicked_at", int(time.time()))

        logger.debug(
            "Bumped %s → rank=%d (session=%s)",
            term_key, new_rank, session_id,
        )

    except Exception:
        logger.exception(
            "Failed to bump rank for term_key=%r session=%r",
            term_key, session_id,
        )