"""
Serializers for validating documents before Typesense ingestion.
No Django models - these are pure validation serializers.
"""
import time
import uuid
from typing import Any, Dict, List

from rest_framework import serializers


class BaseDocumentSerializer(serializers.Serializer):
    """
    Base serializer with common fields for all document types.
    """
    
    # === CORE IDENTIFICATION ===
    document_uuid = serializers.CharField(max_length=255, required=False)
    semantic_uuid = serializers.CharField(max_length=255, required=False)
    cluster_uuid = serializers.CharField(max_length=255, required=False)
    
    # === DOCUMENT CORE (Required) ===
    document_title = serializers.CharField(max_length=500)
    document_summary = serializers.CharField(max_length=10000)
    document_category = serializers.CharField(max_length=100)
    document_data_type = serializers.CharField(max_length=50)
    document_schema = serializers.CharField(max_length=50)
    document_url = serializers.URLField(max_length=2000)
    
    # === DOCUMENT CORE (Optional) ===
    document_author = serializers.CharField(max_length=255, required=False, allow_blank=True)
    document_brand = serializers.CharField(max_length=255, required=False, allow_blank=True)
    
    # === KEYWORDS (Required) ===
    primary_keywords = serializers.ListField(
        child=serializers.CharField(max_length=100),
        min_length=1,
        max_length=10
    )
    keywords = serializers.ListField(
        child=serializers.CharField(max_length=100),
        required=False,
        default=list
    )
    semantic_keywords = serializers.ListField(
        child=serializers.CharField(max_length=100),
        required=False,
        default=list
    )
    key_passages = serializers.ListField(
        child=serializers.CharField(max_length=2000),
        required=False,
        default=list
    )
    
    # === DATES ===
    published_date = serializers.IntegerField(required=False, allow_null=True)
    published_date_string = serializers.CharField(max_length=50, required=False, allow_blank=True)
    created_at = serializers.IntegerField(required=False)
    
    # === MEDIA ASSETS ===
    image_url = serializers.ListField(
        child=serializers.URLField(max_length=2000),
        required=False,
        default=list
    )
    video_url = serializers.ListField(
        child=serializers.URLField(max_length=2000),
        required=False,
        default=list
    )
    logo_url = serializers.ListField(
        child=serializers.URLField(max_length=2000),
        required=False,
        default=list
    )
    social_media = serializers.ListField(
        child=serializers.URLField(max_length=2000),
        required=False,
        default=list
    )
    
    # === LOCATION ===
    location_city = serializers.CharField(max_length=100, required=False, allow_blank=True)
    location_state = serializers.CharField(max_length=100, required=False, allow_blank=True)
    location_country = serializers.CharField(max_length=100, required=False, allow_blank=True)
    location_region = serializers.CharField(max_length=100, required=False, allow_blank=True)
    location_address = serializers.CharField(max_length=500, required=False, allow_blank=True)
    location_coordinates = serializers.ListField(
        child=serializers.FloatField(),
        min_length=2,
        max_length=2,
        required=False,
        help_text="[latitude, longitude]"
    )
    location_geopoint = serializers.ListField(
        child=serializers.FloatField(),
        min_length=2,
        max_length=2,
        required=False,
        help_text="[latitude, longitude]"
    )
    
    # === TEMPORAL ===
    time_context = serializers.CharField(max_length=100, required=False, allow_blank=True)
    temporal_relevance = serializers.ChoiceField(
        choices=['current', 'historical', 'timeless'],
        required=False
    )
    time_period_start = serializers.IntegerField(required=False, allow_null=True)
    time_period_end = serializers.IntegerField(required=False, allow_null=True)
    
    # === ENTITIES ===
    entity_names = serializers.ListField(
        child=serializers.CharField(max_length=255),
        required=False,
        default=list
    )
    
    # === RANKING SIGNALS ===
    authority_score = serializers.FloatField(
        min_value=0,
        max_value=100,
        required=False,
        allow_null=True
    )
    content_depth = serializers.IntegerField(required=False, allow_null=True)
    
    # === STATUS & FLAGS ===
    status = serializers.CharField(max_length=50, required=False, allow_blank=True)
    black_owned = serializers.BooleanField(required=False, default=False)
    rating = serializers.FloatField(required=False, allow_null=True)
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add auto-generated fields."""
        # Generate UUID if not provided
        if not data.get('document_uuid'):
            data['document_uuid'] = str(uuid.uuid4())
        
        # Set created_at timestamp if not provided
        if not data.get('created_at'):
            data['created_at'] = int(time.time())
        
        # Ensure id field for Typesense (uses document_uuid)
        data['id'] = data['document_uuid']
        
        return data
    
    def to_typesense(self) -> Dict[str, Any]:
        """
        Convert validated data to Typesense document format.
        Removes empty/None values to keep documents clean.
        """
        data = self.validated_data.copy()
        
        # Remove None values and empty strings
        cleaned = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, str) and value == '':
                continue
            if isinstance(value, list) and len(value) == 0:
                continue
            cleaned[key] = value
        
        return cleaned


class NewsDocumentSerializer(BaseDocumentSerializer):
    """
    Serializer for news articles.
    Enforces document_schema = 'news' and validates news-specific fields.
    """
    
    # Override with news-specific choices
    document_category = serializers.ChoiceField(
        choices=[
            'national',
            'politics', 
            'business',
            'sports',
            'entertainment',
            'opinion',
            'local',
            'technology',
            'health',
            'science',
            'world',
        ]
    )
    
    document_data_type = serializers.ChoiceField(
        choices=['article', 'video', 'podcast', 'opinion', 'analysis'],
        default='article'
    )
    
    def validate_document_schema(self, value: str) -> str:
        """Ensure schema is 'news'."""
        if value != 'news':
            raise serializers.ValidationError("document_schema must be 'news' for news documents.")
        return value
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Set defaults for news documents."""
        # Force schema to news
        data['document_schema'] = 'news'
        
        # Set default data type
        if not data.get('document_data_type'):
            data['document_data_type'] = 'article'
        
        return super().validate(data)


class BusinessDocumentSerializer(BaseDocumentSerializer):
    """
    Serializer for business directory listings.
    """
    
    # === SERVICE FIELDS ===
    service_type = serializers.ListField(
        child=serializers.CharField(max_length=100),
        required=False,
        default=list
    )
    service_specialties = serializers.ListField(
        child=serializers.CharField(max_length=100),
        required=False,
        default=list
    )
    service_phone = serializers.CharField(max_length=50, required=False, allow_blank=True)
    service_rating = serializers.FloatField(
        min_value=0,
        max_value=5,
        required=False,
        allow_null=True
    )
    
    def validate_document_schema(self, value: str) -> str:
        if value != 'business':
            raise serializers.ValidationError("document_schema must be 'business'.")
        return value
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data['document_schema'] = 'business'
        return super().validate(data)


class CommunityDocumentSerializer(BaseDocumentSerializer):
    """
    Serializer for community content.
    """
    
    def validate_document_schema(self, value: str) -> str:
        if value != 'community':
            raise serializers.ValidationError("document_schema must be 'community'.")
        return value
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data['document_schema'] = 'community'
        return super().validate(data)


class HealthDocumentSerializer(BaseDocumentSerializer):
    """
    Serializer for health content.
    """
    
    def validate_document_schema(self, value: str) -> str:
        if value != 'health':
            raise serializers.ValidationError("document_schema must be 'health'.")
        return value
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data['document_schema'] = 'health'
        return super().validate(data)


class CultureDocumentSerializer(BaseDocumentSerializer):
    """
    Serializer for culture content.
    """
    
    # === MEDIA FIELDS (often relevant for culture) ===
    media_author_creator = serializers.CharField(max_length=255, required=False, allow_blank=True)
    media_genre = serializers.ListField(
        child=serializers.CharField(max_length=100),
        required=False,
        default=list
    )
    media_duration = serializers.CharField(max_length=50, required=False, allow_blank=True)
    media_release_date = serializers.CharField(max_length=50, required=False, allow_blank=True)
    media_language = serializers.CharField(max_length=50, required=False, allow_blank=True)
    media_rating = serializers.FloatField(
        min_value=0,
        max_value=10,
        required=False,
        allow_null=True
    )
    
    def validate_document_schema(self, value: str) -> str:
        if value != 'culture':
            raise serializers.ValidationError("document_schema must be 'culture'.")
        return value
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data['document_schema'] = 'culture'
        return super().validate(data)


class BatchDocumentSerializer(serializers.Serializer):
    """
    Wrapper serializer for batch imports.
    
    Expected payload:
    {
        "documents": [
            {"document_title": "...", ...},
            {"document_title": "...", ...}
        ]
    }
    """
    documents = serializers.ListField(
        child=serializers.DictField(),
        min_length=1,
        max_length=100,  # Limit batch size
        help_text="List of documents to import (max 100 per request)"
    )