"""
Typesense service for document ingestion.
Extends functionality for write operations.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from decouple import config

try:
    import typesense
    from typesense.exceptions import (
        ObjectAlreadyExists,
        ObjectNotFound,
        RequestMalformed,
        ServerError,
        ServiceUnavailable,
        TypesenseClientError,
    )
    TYPESENSE_AVAILABLE = True
except ImportError:
    TYPESENSE_AVAILABLE = False

logger = logging.getLogger(__name__)


class TypesenseService:
    """
    Service class for Typesense document operations.
    Handles upsert, delete, and bulk operations.
    """
    
    COLLECTION_NAME = 'documents'
    
    def __init__(self):
        self._client: Optional[typesense.Client] = None
        self._available: bool = False
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize Typesense client."""
        if not TYPESENSE_AVAILABLE:
            logger.error("Typesense library not installed")
            return
        
        try:
            self._client = typesense.Client({
                'api_key': config('TYPESENSE_API_KEY'),
                'nodes': [{
                    'host': config('TYPESENSE_HOST'),
                    'port': config('TYPESENSE_PORT', cast=int),
                    'protocol': config('TYPESENSE_PROTOCOL', default='http')
                }],
                'connection_timeout_seconds': 5,
                'num_retries': 3,
            })
            # Test connection
            self._client.collections.retrieve()
            self._available = True
            logger.info("TypesenseService: Connection established")
        except Exception as e:
            logger.error(f"TypesenseService: Initialization failed - {e}")
            self._client = None
            self._available = False
    
    @property
    def available(self) -> bool:
        return self._available and self._client is not None
    
    def upsert_document(
        self,
        document: Dict[str, Any],
        collection: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Upsert a single document to Typesense.
        
        Args:
            document: Document data with 'id' field
            collection: Collection name (defaults to COLLECTION_NAME)
        
        Returns:
            Tuple of (success, document_or_none, error_message_or_none)
        """
        if not self.available:
            return False, None, "Typesense client not available"
        
        collection = collection or self.COLLECTION_NAME
        
        try:
            result = self._client.collections[collection].documents.upsert(document)
            logger.info(f"Document upserted: {document.get('id')}")
            return True, result, None
        except RequestMalformed as e:
            error_msg = f"Malformed request: {e}"
            logger.error(error_msg)
            return False, None, error_msg
        except ObjectNotFound as e:
            error_msg = f"Collection not found: {e}"
            logger.error(error_msg)
            return False, None, error_msg
        except ServiceUnavailable as e:
            error_msg = f"Service unavailable: {e}"
            logger.error(error_msg)
            self._available = False
            return False, None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(error_msg)
            return False, None, error_msg
    
    def bulk_upsert(
        self,
        documents: List[Dict[str, Any]],
        collection: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Bulk upsert multiple documents to Typesense.
        
        Args:
            documents: List of document dicts, each with 'id' field
            collection: Collection name (defaults to COLLECTION_NAME)
        
        Returns:
            Dict with 'success_count', 'error_count', 'errors' list
        """
        if not self.available:
            return {
                'success_count': 0,
                'error_count': len(documents),
                'errors': [{'error': 'Typesense client not available'}]
            }
        
        collection = collection or self.COLLECTION_NAME
        
        results = {
            'success_count': 0,
            'error_count': 0,
            'errors': [],
            'imported_ids': []
        }
        
        try:
            # Use import with upsert action for bulk operations
            import_results = self._client.collections[collection].documents.import_(
                documents,
                {'action': 'upsert'}
            )
            
            # Process results - import_ returns a list of result objects
            for i, result in enumerate(import_results):
                if isinstance(result, dict):
                    if result.get('success', False):
                        results['success_count'] += 1
                        results['imported_ids'].append(documents[i].get('id'))
                    else:
                        results['error_count'] += 1
                        results['errors'].append({
                            'document_id': documents[i].get('id'),
                            'error': result.get('error', 'Unknown error')
                        })
                elif isinstance(result, str):
                    # Sometimes returns JSONL strings
                    import json
                    try:
                        parsed = json.loads(result)
                        if parsed.get('success', False):
                            results['success_count'] += 1
                            results['imported_ids'].append(documents[i].get('id'))
                        else:
                            results['error_count'] += 1
                            results['errors'].append({
                                'document_id': documents[i].get('id'),
                                'error': parsed.get('error', 'Unknown error')
                            })
                    except json.JSONDecodeError:
                        results['success_count'] += 1
                        results['imported_ids'].append(documents[i].get('id'))
            
            logger.info(
                f"Bulk upsert completed: {results['success_count']} success, "
                f"{results['error_count']} errors"
            )
            
        except RequestMalformed as e:
            error_msg = f"Malformed request: {e}"
            logger.error(error_msg)
            results['error_count'] = len(documents)
            results['errors'].append({'error': error_msg})
        except ServiceUnavailable as e:
            error_msg = f"Service unavailable: {e}"
            logger.error(error_msg)
            self._available = False
            results['error_count'] = len(documents)
            results['errors'].append({'error': error_msg})
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(error_msg)
            results['error_count'] = len(documents)
            results['errors'].append({'error': error_msg})
        
        return results
    
    def delete_document(
        self,
        document_id: str,
        collection: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Delete a document from Typesense.
        
        Args:
            document_id: The document ID to delete
            collection: Collection name (defaults to COLLECTION_NAME)
        
        Returns:
            Tuple of (success, error_message_or_none)
        """
        if not self.available:
            return False, "Typesense client not available"
        
        collection = collection or self.COLLECTION_NAME
        
        try:
            self._client.collections[collection].documents[document_id].delete()
            logger.info(f"Document deleted: {document_id}")
            return True, None
        except ObjectNotFound:
            return False, f"Document not found: {document_id}"
        except Exception as e:
            error_msg = f"Delete error: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def get_document(
        self,
        document_id: str,
        collection: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Retrieve a document from Typesense.
        
        Args:
            document_id: The document ID to retrieve
            collection: Collection name (defaults to COLLECTION_NAME)
        
        Returns:
            Tuple of (success, document_or_none, error_message_or_none)
        """
        if not self.available:
            return False, None, "Typesense client not available"
        
        collection = collection or self.COLLECTION_NAME
        
        try:
            doc = self._client.collections[collection].documents[document_id].retrieve()
            return True, doc, None
        except ObjectNotFound:
            return False, None, f"Document not found: {document_id}"
        except Exception as e:
            error_msg = f"Retrieve error: {e}"
            logger.error(error_msg)
            return False, None, error_msg


# Singleton instance
_typesense_service: Optional[TypesenseService] = None


def get_typesense_service() -> TypesenseService:
    """Get or create the TypesenseService singleton."""
    global _typesense_service
    if _typesense_service is None:
        _typesense_service = TypesenseService()
    return _typesense_service