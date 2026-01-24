"""
Related Searches via Semantic Query Storage

This module stores query embeddings to power the "Related Searches" sidebar feature.
It does NOT cache search results or skip searches - it only enables finding
semantically similar queries that other users have searched.

Usage in typesense_calculations.py:

    from cached_embedding_related_search import (
        store_query_embedding,
        get_related_searches
    )
    
    # After search completes, store the query + embedding
    store_query_embedding(query, query_embedding)
    
    # Get related searches for sidebar
    related = get_related_searches(query_embedding, limit=5, exclude_query=query)

Redis Storage:
    qcache:meta:{hash}  - Query text, timestamp, search count
    qcache:emb:{hash}   - Embedding vector (384 floats)
    qcache:index        - Sorted set of all query hashes
"""

import logging
import json
import time
import hashlib
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any
from functools import wraps
import numpy as np

import redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    RedisError,
)
from decouple import config


# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Redis
REDIS_CACHE_URL = config('REDIS_ANALYTICS_URL')

# Embedding
EMBEDDING_DIMENSION = 384

# Cache settings
CACHE_KEY_PREFIX = 'qcache'
CACHE_MAX_QUERIES = config('CACHE_MAX_QUERIES', default=5000, cast=int)
CACHE_TTL_HOURS = config('CACHE_TTL_HOURS', default=72, cast=int)
CACHE_MAX_TTL_HOURS = config('CACHE_MAX_TTL_HOURS', default=168, cast=int)  # 7 days

# Similarity thresholds for Related Searches
SIMILARITY_THRESHOLD_RELATED_MIN = config('SIMILARITY_THRESHOLD_RELATED_MIN', default=0.70, cast=float)
SIMILARITY_THRESHOLD_RELATED_MAX = config('SIMILARITY_THRESHOLD_RELATED_MAX', default=0.92, cast=float)

# Query validation
MIN_QUERY_LENGTH = 3
MIN_QUERY_WORDS = 2

# Retry settings
MAX_RETRIES = 2
RETRY_DELAY = 0.1


# ============================================================================
# REDIS CLIENT (Singleton)
# ============================================================================

_redis_client = None
_redis_lock = threading.Lock()


def _get_redis_client() -> Optional[redis.Redis]:
    """Get or create Redis client."""
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        
        try:
            _redis_client = redis.from_url(
                REDIS_CACHE_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            _redis_client.ping()
            logger.info("Related searches Redis connection established")
            return _redis_client
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            _redis_client = None
            return None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_query(query: str) -> str:
    """Normalize query for consistent storage."""
    if not query:
        return ''
    
    # Lowercase and strip
    normalized = query.lower().strip()
    
    # Remove punctuation (keep alphanumeric and spaces)
    normalized = ''.join(c if c.isalnum() or c.isspace() else ' ' for c in normalized)
    
    # Collapse multiple spaces
    normalized = ' '.join(normalized.split())
    
    return normalized[:500]


def _hash_query(query: str) -> str:
    """Generate hash for normalized query."""
    normalized = _normalize_query(query)
    return hashlib.md5(normalized.encode()).hexdigest()[:16]


def _get_key(key_type: str, query_hash: str) -> str:
    """Generate Redis key."""
    return f"{CACHE_KEY_PREFIX}:{key_type}:{query_hash}"


def _should_store(query: str) -> bool:
    """Determine if query should be stored."""
    if not query:
        return False
    
    normalized = _normalize_query(query)
    
    # Too short
    if len(normalized) < MIN_QUERY_LENGTH:
        return False
    
    # Too few words
    words = normalized.split()
    if len(words) < MIN_QUERY_WORDS:
        return False
    
    # Time-sensitive terms (don't store - not useful for related searches)
    time_sensitive = ['today', 'tomorrow', 'yesterday', 'this week', 'tonight',
                     'now', 'current', 'latest', '2026', '2025']
    if any(term in normalized for term in time_sensitive):
        return False
    
    return True


def _compute_similarity(embedding_a: List[float], embedding_b: List[float]) -> float:
    """Compute cosine similarity between two embeddings."""
    if not embedding_a or not embedding_b:
        return 0.0
    
    try:
        a = np.array(embedding_a)
        b = np.array(embedding_b)
        
        dot_product = np.dot(a, b)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        
        if norm_a == 0 or norm_b == 0:
            return 0.0
        
        similarity = dot_product / (norm_a * norm_b)
        return float(max(0.0, min(1.0, similarity)))
        
    except Exception as e:
        logger.error(f"Similarity computation error: {e}")
        return 0.0


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def store_query_embedding(
    query: str,
    embedding: List[float],
    search_count: int = 1
) -> bool:
    """
    Store a query and its embedding for related searches.
    
    Call this after each successful search in typesense_calculations.py.
    Does NOT store document UUIDs - only the query and embedding.
    
    Args:
        query: The search query
        embedding: The embedding vector (from run_parallel_prep)
        search_count: How many times this query has been searched (optional)
        
    Returns:
        True on success, False on failure
    """
    # Validate inputs
    if not query or not embedding:
        return False
    
    if not _should_store(query):
        return False
    
    if len(embedding) != EMBEDDING_DIMENSION:
        logger.warning(f"Invalid embedding dimension: {len(embedding)}, expected {EMBEDDING_DIMENSION}")
        return False
    
    redis_client = _get_redis_client()
    if not redis_client:
        return False
    
    normalized = _normalize_query(query)
    query_hash = _hash_query(query)
    
    now = time.time()
    now_iso = datetime.utcnow().isoformat()
    ttl_seconds = CACHE_TTL_HOURS * 3600
    
    try:
        # Check if already exists - if so, just increment count
        meta_key = _get_key('meta', query_hash)
        existing = redis_client.exists(meta_key)
        
        if existing:
            # Increment search count and update timestamp
            pipe = redis_client.pipeline()
            pipe.hincrby(meta_key, 'search_count', 1)
            pipe.hset(meta_key, 'last_searched_at', now_iso)
            pipe.hset(meta_key, 'last_searched_ts', now)
            
            # Extend TTL
            current_ttl = redis_client.ttl(meta_key)
            new_ttl = min(current_ttl + (24 * 3600), CACHE_MAX_TTL_HOURS * 3600)
            pipe.expire(meta_key, new_ttl)
            
            emb_key = _get_key('emb', query_hash)
            pipe.expire(emb_key, new_ttl)
            
            pipe.execute()
            logger.debug(f"Updated existing query: {normalized[:50]}...")
            return True
        
        # New query - store it
        pipe = redis_client.pipeline()
        
        # Store metadata
        pipe.hset(meta_key, mapping={
            'query': query[:500],
            'query_normalized': normalized,
            'created_at': now_iso,
            'created_ts': now,
            'last_searched_at': now_iso,
            'last_searched_ts': now,
            'search_count': search_count,
        })
        pipe.expire(meta_key, ttl_seconds)
        
        # Store embedding
        emb_key = _get_key('emb', query_hash)
        pipe.set(emb_key, json.dumps(embedding))
        pipe.expire(emb_key, ttl_seconds)
        
        # Add to index
        index_key = f"{CACHE_KEY_PREFIX}:index"
        pipe.zadd(index_key, {query_hash: now})
        
        pipe.execute()
        
        # Enforce max cache size (do this separately to not block the main operation)
        try:
            cache_size = redis_client.zcard(index_key)
            if cache_size and cache_size > CACHE_MAX_QUERIES:
                # Remove oldest entries
                to_remove = cache_size - CACHE_MAX_QUERIES + 100
                oldest = redis_client.zrange(index_key, 0, to_remove - 1)
                for old_hash in oldest:
                    _delete_query(old_hash)
        except Exception:
            pass  # Non-critical
        
        logger.debug(f"Stored new query: {normalized[:50]}...")
        return True
        
    except RedisError as e:
        logger.error(f"Failed to store query embedding: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error storing query: {e}")
        return False


def get_related_searches(
    embedding: List[float],
    limit: int = 5,
    exclude_query: Optional[str] = None,
    min_similarity: float = SIMILARITY_THRESHOLD_RELATED_MIN,
    max_similarity: float = SIMILARITY_THRESHOLD_RELATED_MAX
) -> List[Dict]:
    """
    Find related searches for the template sidebar.
    
    Returns queries that are similar (but not too similar) to the current query.
    Similarity range 0.70-0.92 means "related but different" queries.
    
    Args:
        embedding: The current query's embedding vector
        limit: Maximum number of related searches to return
        exclude_query: The current query (to exclude from results)
        min_similarity: Minimum similarity (below = too different)
        max_similarity: Maximum similarity (above = too similar)
        
    Returns:
        List of dicts: [{'query': str, 'similarity': float, 'search_count': int}, ...]
    """
    if not embedding:
        return []
    
    redis_client = _get_redis_client()
    if not redis_client:
        return []
    
    try:
        # Get all stored embeddings
        index_key = f"{CACHE_KEY_PREFIX}:index"
        all_hashes = redis_client.zrange(index_key, 0, -1)
        
        if not all_hashes:
            return []
        
        exclude_hash = _hash_query(exclude_query) if exclude_query else None
        
        # Calculate similarities
        similarities = []
        
        for query_hash in all_hashes:
            # Skip the current query
            if query_hash == exclude_hash:
                continue
            
            # Get stored embedding
            emb_key = _get_key('emb', query_hash)
            emb_json = redis_client.get(emb_key)
            
            if not emb_json:
                continue
            
            try:
                stored_embedding = json.loads(emb_json)
            except json.JSONDecodeError:
                continue
            
            # Calculate similarity
            similarity = _compute_similarity(embedding, stored_embedding)

            
            
            # Check if in the "related but different" range
            if min_similarity <= similarity <= max_similarity:
                similarities.append((query_hash, similarity))
        
        if not similarities:
            return []
        
        # Sort by similarity (highest first)
        similarities.sort(key=lambda x: x[1], reverse=True)
        
        # Get metadata for top matches
        related = []
        for query_hash, similarity in similarities[:limit]:
            meta_key = _get_key('meta', query_hash)
            meta = redis_client.hgetall(meta_key)
            
            if meta:
                related.append({
                    'query': meta.get('query', ''),
                    'similarity': round(similarity, 3),
                    'search_count': int(meta.get('search_count', 0)),
                })
        
        return related
        
    except RedisError as e:
        logger.error(f"Failed to get related searches: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error getting related searches: {e}")
        return []


def _delete_query(query_hash: str) -> None:
    """Delete a stored query by hash."""
    redis_client = _get_redis_client()
    if not redis_client:
        return
    
    try:
        pipe = redis_client.pipeline()
        pipe.delete(_get_key('meta', query_hash))
        pipe.delete(_get_key('emb', query_hash))
        pipe.zrem(f"{CACHE_KEY_PREFIX}:index", query_hash)
        pipe.execute()
    except RedisError:
        pass


# ============================================================================
# ADMIN / MAINTENANCE FUNCTIONS
# ============================================================================

def get_stats() -> Dict[str, Any]:
    """Get storage statistics for monitoring."""
    redis_client = _get_redis_client()
    if not redis_client:
        return {'error': 'Redis not available'}
    
    try:
        index_key = f"{CACHE_KEY_PREFIX}:index"
        all_hashes = redis_client.zrange(index_key, 0, -1)
        
        total_searches = 0
        queries_data = []
        
        for query_hash in all_hashes:
            meta_key = _get_key('meta', query_hash)
            meta = redis_client.hgetall(meta_key)
            if meta:
                search_count = int(meta.get('search_count', 0))
                total_searches += search_count
                queries_data.append({
                    'query': meta.get('query', ''),
                    'search_count': search_count,
                    'created_at': meta.get('created_at', '')
                })
        
        # Sort by search count
        queries_data.sort(key=lambda x: x['search_count'], reverse=True)
        
        return {
            'total_queries_stored': len(all_hashes),
            'max_queries': CACHE_MAX_QUERIES,
            'total_searches': total_searches,
            'ttl_hours': CACHE_TTL_HOURS,
            'similarity_range': f"{SIMILARITY_THRESHOLD_RELATED_MIN}-{SIMILARITY_THRESHOLD_RELATED_MAX}",
            'top_queries': queries_data[:20],
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except RedisError as e:
        logger.error(f"Failed to get stats: {e}")
        return {'error': str(e)}


def cleanup_expired(max_age_hours: int = None) -> int:
    """Remove old entries. Returns count deleted."""
    redis_client = _get_redis_client()
    if not redis_client:
        return 0
    
    max_age = max_age_hours or CACHE_MAX_TTL_HOURS
    cutoff_ts = time.time() - (max_age * 3600)
    
    try:
        index_key = f"{CACHE_KEY_PREFIX}:index"
        old_entries = redis_client.zrangebyscore(index_key, 0, cutoff_ts)
        
        deleted = 0
        for query_hash in old_entries:
            _delete_query(query_hash)
            deleted += 1
        
        if deleted:
            logger.info(f"Cleanup: deleted {deleted} expired query embeddings")
        return deleted
        
    except RedisError as e:
        logger.error(f"Cleanup failed: {e}")
        return 0


def clear_all() -> bool:
    """Delete all stored queries. Use with caution."""
    redis_client = _get_redis_client()
    if not redis_client:
        return False
    
    try:
        index_key = f"{CACHE_KEY_PREFIX}:index"
        all_hashes = redis_client.zrange(index_key, 0, -1)
        
        for query_hash in all_hashes:
            _delete_query(query_hash)
        
        logger.info(f"Cleared all {len(all_hashes)} stored queries")
        return True
        
    except RedisError as e:
        logger.error(f"Failed to clear: {e}")
        return False


def health_check() -> Dict[str, Any]:
    """Check if the system is healthy."""
    result = {
        'healthy': False,
        'redis_ok': False,
        'redis_latency_ms': None,
        'queries_stored': 0,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    redis_client = _get_redis_client()
    if redis_client:
        try:
            start = time.time()
            redis_client.ping()
            result['redis_latency_ms'] = round((time.time() - start) * 1000, 2)
            result['redis_ok'] = True
            result['queries_stored'] = redis_client.zcard(f"{CACHE_KEY_PREFIX}:index") or 0
            result['healthy'] = True
        except Exception as e:
            result['redis_error'] = str(e)
    
    return result


# ============================================================================
# BOOTSTRAP (Optional - seed from analytics)
# ============================================================================

def bootstrap_from_analytics(
    embedding_func,
    limit: int = 1000
) -> Dict[str, int]:
    """
    Seed the related searches from popular queries in analytics.
    
    Args:
        embedding_func: Function to get embedding (e.g., get_query_embedding from typesense_calculations)
        limit: Maximum queries to bootstrap
        
    Returns:
        Dict with 'stored', 'failed', 'skipped' counts
        
    Usage:
        from typesense_calculations import get_query_embedding
        from cached_embedding_related_search import bootstrap_from_analytics
        
        stats = bootstrap_from_analytics(get_query_embedding, limit=500)
    """
    redis_client = _get_redis_client()
    if not redis_client:
        return {'stored': 0, 'failed': 0, 'skipped': 0, 'error': 'Redis not available'}
    
    stats = {'stored': 0, 'failed': 0, 'skipped': 0}
    
    try:
        # Get popular queries from analytics
        popular = redis_client.zrevrange('analytics:popular:queries', 0, limit - 1, withscores=True)
        
        if not popular:
            logger.warning("No popular queries found in analytics")
            return stats
        
        logger.info(f"Bootstrapping {len(popular)} queries...")
        
        for query, count in popular:
            # Skip if shouldn't store
            if not _should_store(query):
                stats['skipped'] += 1
                continue
            
            # Skip if already exists
            query_hash = _hash_query(query)
            if redis_client.exists(_get_key('meta', query_hash)):
                stats['skipped'] += 1
                continue
            
            # Get embedding
            try:
                embedding = embedding_func(query)
                if not embedding:
                    stats['failed'] += 1
                    continue
                
                # Store it
                if store_query_embedding(query, embedding, search_count=int(count)):
                    stats['stored'] += 1
                else:
                    stats['failed'] += 1
                
                # Rate limit
                time.sleep(0.05)
                
            except Exception as e:
                logger.warning(f"Failed to bootstrap '{query}': {e}")
                stats['failed'] += 1
        
        logger.info(f"Bootstrap complete: {stats}")
        return stats
        
    except RedisError as e:
        logger.error(f"Bootstrap failed: {e}")
        return {**stats, 'error': str(e)}
    

    # ============================================================================
# DEBUG FUNCTION - Remove after testing
# ============================================================================

def debug_related_searches(
    embedding: List[float],
    exclude_query: Optional[str] = None,
    min_similarity: float = SIMILARITY_THRESHOLD_RELATED_MIN,
    max_similarity: float = SIMILARITY_THRESHOLD_RELATED_MAX
) -> None:
    """
    Debug function to print similarity calculations.
    Call this before get_related_searches() to see what's happening.
    
    REMOVE THIS FUNCTION AFTER TESTING.
    """
    if not embedding:
        print("🔍 DEBUG: No embedding provided")
        return
    
    redis_client = _get_redis_client()
    if not redis_client:
        print("🔍 DEBUG: Redis not available")
        return
    
    try:
        index_key = f"{CACHE_KEY_PREFIX}:index"
        all_hashes = redis_client.zrange(index_key, 0, -1)
        
        if not all_hashes:
            print("🔍 DEBUG: No stored queries in Redis")
            return
        
        print(f"\n{'='*60}")
        print(f"🔍 DEBUG: Related Searches Analysis")
        print(f"{'='*60}")
        print(f"   Current query: '{exclude_query}'")
        print(f"   Stored queries: {len(all_hashes)}")
        print(f"   Similarity range: {min_similarity} - {max_similarity}")
        print(f"{'-'*60}")
        
        exclude_hash = _hash_query(exclude_query) if exclude_query else None
        
        included = []
        excluded_same = []
        excluded_too_similar = []
        excluded_too_different = []
        
        for query_hash in all_hashes:
            meta_key = _get_key('meta', query_hash)
            meta = redis_client.hgetall(meta_key)
            stored_query = meta.get('query', 'unknown') if meta else 'unknown'
            
            if query_hash == exclude_hash:
                excluded_same.append(stored_query)
                continue
            
            emb_key = _get_key('emb', query_hash)
            emb_json = redis_client.get(emb_key)
            
            if not emb_json:
                continue
            
            try:
                stored_embedding = json.loads(emb_json)
            except json.JSONDecodeError:
                continue
            
            similarity = _compute_similarity(embedding, stored_embedding)
            
            if similarity >= min_similarity and similarity <= max_similarity:
                included.append((stored_query, similarity))
            elif similarity > max_similarity:
                excluded_too_similar.append((stored_query, similarity))
            else:
                excluded_too_different.append((stored_query, similarity))
        
        # Print results
        if excluded_same:
            print(f"\n⏭️  SKIPPED (same query):")
            for q in excluded_same:
                print(f"      • {q[:50]}")
        
        if excluded_too_similar:
            print(f"\n⚠️  TOO SIMILAR (>{max_similarity}):")
            for q, sim in sorted(excluded_too_similar, key=lambda x: -x[1]):
                print(f"      • {q[:50]} → {sim:.3f}")
        
        if excluded_too_different:
            print(f"\n❌ TOO DIFFERENT (<{min_similarity}):")
            for q, sim in sorted(excluded_too_different, key=lambda x: -x[1]):
                print(f"      • {q[:50]} → {sim:.3f}")
        
        if included:
            print(f"\n✅ INCLUDED ({min_similarity}-{max_similarity}):")
            for q, sim in sorted(included, key=lambda x: -x[1]):
                print(f"      • {q[:50]} → {sim:.3f}")
        else:
            print(f"\n✅ INCLUDED: None")
        
        print(f"\n{'='*60}")
        print(f"   Summary: {len(included)} related searches found")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"🔍 DEBUG ERROR: {e}")