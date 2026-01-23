"""
URL routing for automation API endpoints.
"""
from django.urls import path

from .views import (
    BusinessImportView,
    CommunityImportView,
    CultureImportView,
    DocumentDeleteView,
    HealthCheckView,
    HealthImportView,
    NewsImportView,
)

app_name = 'automations'

urlpatterns = [
    # Health check (no auth required)
    path('health-check/', HealthCheckView.as_view(), name='health-check'),
    
    # Import endpoints (API key required)
    path('news/import/', NewsImportView.as_view(), name='news-import'),
    path('business/import/', BusinessImportView.as_view(), name='business-import'),
    path('community/import/', CommunityImportView.as_view(), name='community-import'),
    path('health/import/', HealthImportView.as_view(), name='health-import'),
    path('culture/import/', CultureImportView.as_view(), name='culture-import'),
    
    # Document management (API key required)
    path('documents/<str:document_id>/', DocumentDeleteView.as_view(), name='document-delete'),
]