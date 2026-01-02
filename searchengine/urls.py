from django.urls import path
from . import views
from . import searchapi

app_name="searchengine"





# urlpatterns = [

# path('', views.home, name='home'),
# path('api/search/', views.search_suggestions, name='search_suggestions'),
# path('api/submit/', views.form_submit, name='form_submit'),
# path('search', views.search, name='search'),
# path('category/<str:category_slug>/', views.category_view, name='category'),
# path('business/', views.category_view, {'category_slug': 'business'}, name='business'),
# path('culture/', views.category_view, {'category_slug': 'culture'}, name='culture'),
# path('health/', views.category_view, {'category_slug': 'health'}, name='health'),
# path('news/', views.category_view, {'category_slug': 'news'}, name='news'),
# path('community/', views.category_view, {'category_slug': 'community'}, name='community'),
# ]


urlpatterns = [
    # ==================== HOME & SEARCH ====================
    path('', views.home, name='home'),
    path('search/', views.search, name='search'),
    
    # ==================== API ENDPOINTS ====================
    path('api/search/', views.search_suggestions, name='search_suggestions'),
    path('api/submit/', views.form_submit, name='form_submit'),
    
    # Business API
    path('api/business/search/', views.business_search_api, name='business_search_api'),
    
    # Community API
    path('api/community/search/', views.community_search_api, name='community_search_api'),
    
    # ==================== CATEGORY PAGES ====================
    # Generic category router (handles all categories)
    path('category/<str:category_slug>/', views.category_view, name='category'),
    
    # Direct category shortcuts (optional, for cleaner URLs)
    path('business/', views.business_category, name='business'),
    path('culture/', views.culture_category, name='culture'),
    path('health/', views.health_category, name='health'),
    path('news/', views.news_category, name='news'),
    path('community/', views.community_category, name='community'),
    
    # ==================== DETAIL PAGES ====================
    # Business detail
    path('business/<str:business_id>/', views.business_detail, name='business_detail'),
    
    # ==================== CITY-SPECIFIC ROUTES (OPTIONAL) ====================
    # Community by city (alternative URL pattern)
    path('community/<str:city_slug>/', views.community_by_city, name='community_by_city'),
]

    # path('api/search', views.search_api, name='search_api'),
    # # Keep old endpoint for backwards compatibility if needed
    # path('api/submit/', views.search_api, name='form_submit'),

