"""
API Views for n8n automation endpoints.
Handles batch document imports to Typesense.
"""
import logging
from typing import Any, Dict, Type

from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .authentication import APIKeyAuthentication
from .serializers import (
    BaseDocumentSerializer,
    BatchDocumentSerializer,
    BusinessDocumentSerializer,
    CommunityDocumentSerializer,
    CultureDocumentSerializer,
    HealthDocumentSerializer,
    NewsDocumentSerializer,
)
from .services import get_typesense_service

logger = logging.getLogger(__name__)


class BaseBatchImportView(APIView):
    """
    Base view for batch document imports.
    Subclasses define the document serializer to use.
    """
    
    authentication_classes = [APIKeyAuthentication]
    permission_classes = [AllowAny]  # Auth handled by APIKeyAuthentication
    
    # Override in subclasses
    document_serializer_class: Type[BaseDocumentSerializer] = BaseDocumentSerializer
    schema_name: str = 'document'
    
    def post(self, request: Request) -> Response:
        """
        Handle batch document import.
        
        Expected payload:
        {
            "documents": [
                {"document_title": "...", "document_summary": "...", ...},
                {"document_title": "...", "document_summary": "...", ...}
            ]
        }
        """
        # Validate batch wrapper
        batch_serializer = BatchDocumentSerializer(data=request.data)
        if not batch_serializer.is_valid():
            logger.warning(f"Invalid batch format: {batch_serializer.errors}")
            return Response(
                {
                    'success': False,
                    'error': 'Invalid request format',
                    'details': batch_serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        documents_data = batch_serializer.validated_data['documents']
        
        # Validate each document
        validated_documents = []
        validation_errors = []
        
        for i, doc_data in enumerate(documents_data):
            serializer = self.document_serializer_class(data=doc_data)
            if serializer.is_valid():
                validated_documents.append(serializer.to_typesense())
            else:
                validation_errors.append({
                    'index': i,
                    'document_title': doc_data.get('document_title', 'Unknown'),
                    'errors': serializer.errors
                })
        
        # If all documents failed validation, return error
        if not validated_documents:
            logger.warning(f"All {len(documents_data)} documents failed validation")
            return Response(
                {
                    'success': False,
                    'error': 'All documents failed validation',
                    'validation_errors': validation_errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Import to Typesense
        typesense_service = get_typesense_service()
        
        if not typesense_service.available:
            logger.error("Typesense service unavailable")
            return Response(
                {
                    'success': False,
                    'error': 'Search service temporarily unavailable'
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )
        
        results = typesense_service.bulk_upsert(validated_documents)
        
        # Build response
        response_data = {
            'success': results['error_count'] == 0,
            'schema': self.schema_name,
            'summary': {
                'total_received': len(documents_data),
                'validated': len(validated_documents),
                'imported': results['success_count'],
                'failed': results['error_count'] + len(validation_errors),
            },
            'imported_ids': results['imported_ids'],
        }
        
        # Include errors if any
        if validation_errors:
            response_data['validation_errors'] = validation_errors
        if results['errors']:
            response_data['import_errors'] = results['errors']
        
        # Determine HTTP status
        if results['error_count'] == 0 and not validation_errors:
            http_status = status.HTTP_201_CREATED
        elif results['success_count'] > 0:
            http_status = status.HTTP_207_MULTI_STATUS  # Partial success
        else:
            http_status = status.HTTP_400_BAD_REQUEST
        
        logger.info(
            f"{self.schema_name} import: {results['success_count']}/{len(documents_data)} successful"
        )
        
        return Response(response_data, status=http_status)


class NewsImportView(BaseBatchImportView):
    """
    Batch import endpoint for news articles.
    
    POST /api/v1/news/import/
    
    Headers:
        X-API-Key: your-api-key
        Content-Type: application/json
    
    Body:
    {
        "documents": [
            {
                "document_title": "Breaking News Title",
                "document_summary": "Article summary...",
                "document_category": "politics",
                "document_url": "https://example.com/article",
                "document_brand": "News Source",
                "document_author": "John Doe",
                "primary_keywords": ["politics", "election"],
                "keywords": ["congress", "senate"],
                "semantic_keywords": ["government", "democracy"],
                "image_url": ["https://example.com/image.jpg"],
                "authority_score": 75.5
            }
        ]
    }
    """
    
    document_serializer_class = NewsDocumentSerializer
    schema_name = 'news'


class BusinessImportView(BaseBatchImportView):
    """
    Batch import endpoint for business directory listings.
    
    POST /api/v1/business/import/
    """
    
    document_serializer_class = BusinessDocumentSerializer
    schema_name = 'business'


class CommunityImportView(BaseBatchImportView):
    """
    Batch import endpoint for community content.
    
    POST /api/v1/community/import/
    """
    
    document_serializer_class = CommunityDocumentSerializer
    schema_name = 'community'


class HealthImportView(BaseBatchImportView):
    """
    Batch import endpoint for health content.
    
    POST /api/v1/health/import/
    """
    
    document_serializer_class = HealthDocumentSerializer
    schema_name = 'health'


class CultureImportView(BaseBatchImportView):
    """
    Batch import endpoint for culture content.
    
    POST /api/v1/culture/import/
    """
    
    document_serializer_class = CultureDocumentSerializer
    schema_name = 'culture'


class HealthCheckView(APIView):
    """
    Health check endpoint for monitoring.
    No authentication required.
    
    GET /api/v1/health-check/
    """
    
    authentication_classes = []
    permission_classes = [AllowAny]
    
    def get(self, request: Request) -> Response:
        typesense_service = get_typesense_service()
        
        return Response({
            'status': 'healthy',
            'typesense_available': typesense_service.available,
        })


class DocumentDeleteView(APIView):
    """
    Delete a document by ID.
    
    DELETE /api/v1/documents/<document_id>/
    """
    
    authentication_classes = [APIKeyAuthentication]
    permission_classes = [AllowAny]
    
    def delete(self, request: Request, document_id: str) -> Response:
        typesense_service = get_typesense_service()
        
        if not typesense_service.available:
            return Response(
                {'success': False, 'error': 'Search service unavailable'},
                status=status.HTTP_503_SERVICE_UNAVAILABLE
            )
        
        success, error = typesense_service.delete_document(document_id)
        
        if success:
            return Response(
                {'success': True, 'deleted_id': document_id},
                status=status.HTTP_200_OK
            )
        else:
            return Response(
                {'success': False, 'error': error},
                status=status.HTTP_404_NOT_FOUND if 'not found' in error.lower() else status.HTTP_400_BAD_REQUEST
            )