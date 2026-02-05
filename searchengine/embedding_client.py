"""
embedding_client.py

Django client for the FastAPI embedding service.
Place this in your Django app and import get_query_embedding from here.

Replaces the local model loading with HTTP calls to the FastAPI service.
"""

import requests
from typing import List, Optional
import logging
import os
from decouple import config

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

EMBEDDING_SERVICE_URL = config('EMBEDDING_SERVICE_URL')
EMBEDDING_TIMEOUT = config('EMBEDDING_TIMEOUT', default=2, cast=int)



# ============================================================================
# SINGLE EMBEDDING
# ============================================================================

def get_query_embedding(query: str) -> Optional[List[float]]:
    """
    Get embedding for a single query by calling the FastAPI service.
    
    This replaces the old local model loading approach.
    
    Args:
        query: The text to embed
        
    Returns:
        List of 384 floats, or None if service unavailable
    """
    if not query or not query.strip():
        return None
    
    try:
        response = requests.post(
            f"{EMBEDDING_SERVICE_URL}/embed",
            json={"text": query.strip()},
            timeout=EMBEDDING_TIMEOUT
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("embedding")
        else:
            logger.error(f"Embedding service error: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        logger.error("Embedding service not available (connection refused)")
        return None
    except requests.exceptions.Timeout:
        logger.error("Embedding service timeout")
        return None
    except Exception as e:
        logger.error(f"Embedding service error: {e}")
        return None


# ============================================================================
# BATCH EMBEDDING (for future use)
# ============================================================================

def get_embeddings_batch(texts: List[str]) -> Optional[List[List[float]]]:
    """
    Get embeddings for multiple texts in one call (more efficient).
    
    Args:
        texts: List of texts to embed
        
    Returns:
        List of embeddings, or None if service unavailable
    """
    if not texts:
        return None
    
    # Filter empty texts
    texts = [t.strip() for t in texts if t and t.strip()]
    
    if not texts:
        return None
    
    try:
        response = requests.post(
            f"{EMBEDDING_SERVICE_URL}/embed/batch",
            json={"texts": texts},
            timeout=EMBEDDING_TIMEOUT + (len(texts) * 0.1)  # Extra time for batches
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("embeddings")
        else:
            logger.error(f"Embedding batch error: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        logger.error("Embedding service not available (connection refused)")
        return None
    except requests.exceptions.Timeout:
        logger.error("Embedding batch timeout")
        return None
    except Exception as e:
        logger.error(f"Embedding batch error: {e}")
        return None


# ============================================================================
# HEALTH CHECK
# ============================================================================

def check_embedding_service() -> bool:
    """
    Check if the embedding service is running and model is loaded.
    
    Returns:
        True if service is healthy, False otherwise
    """
    try:
        response = requests.get(
            f"{EMBEDDING_SERVICE_URL}/health",
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get("model_loaded", False)
        return False
        
    except:
        return False


# ============================================================================
# OPTIONAL: Lazy model as fallback (if you want local fallback)
# ============================================================================

_fallback_model = None
_fallback_failed = False

def get_query_embedding_with_fallback(query: str) -> Optional[List[float]]:
    """
    Try FastAPI service first, fall back to local model if unavailable.
    
    Use this if you want redundancy, but it means keeping 
    sentence-transformers in Django requirements.
    """
    global _fallback_model, _fallback_failed
    
    # Try service first
    embedding = get_query_embedding(query)
    if embedding:
        return embedding
    
    # Fallback to local model
    if _fallback_failed:
        return None
    
    if _fallback_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            logger.warning("Embedding service unavailable, loading local model...")
            _fallback_model = SentenceTransformer('all-MiniLM-L6-v2')
        except Exception as e:
            logger.error(f"Fallback model failed: {e}")
            _fallback_failed = True
            return None
    
    try:
        embedding = _fallback_model.encode(query)
        return embedding.tolist()
    except Exception as e:
        logger.error(f"Fallback embedding failed: {e}")
        return None