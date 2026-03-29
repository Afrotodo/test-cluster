from django.urls import path
from . import views
# from .word_discovery_edge_cases import debug_word_discovery

from .views import track_click, click_redirect
from . import searchapi
from . import cache_views  # ← Add this

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



    # Cache API endpoints
    path('api/cache/reload/', cache_views.reload_cache_view, name='cache_reload'),
    path('api/cache/reload-from-file/', cache_views.reload_cache_from_file_view, name='cache_reload_file'),
    path('api/cache/status/', cache_views.cache_status_view, name='cache_status'),
    path('api/cache/test/', cache_views.cache_test_view, name='cache_test'),
    path('api/cache/add/', cache_views.add_to_cache_view, name='cache_add'),


    # path('api/cache/reload-nosave/', cache_views.reload_cache_nosave_view, name='cache_reload_nosave'),
    # path('api/cache/add-nosave/', cache_views.add_to_cache_nosave_view, name='cache_add_nosave'),
    # path('api/cache/save/', cache_views.save_cache_view, name='cache_save'),

    path('api/cache/reload-nosave/', cache_views.reload_cache_nosave_view, name='cache_reload_nosave'),
    path('api/cache/add-nosave/', cache_views.add_to_cache_nosave_view, name='cache_add_nosave'),
    path('api/cache/save/', cache_views.save_cache_view, name='cache_save'),

    path('api/cache/upload-chunk/', cache_views.upload_chunk_view, name='cache_upload_chunk'),
    path('api/cache/finalize/', cache_views.finalize_upload_view, name='cache_finalize'),
    
    # ==================== CATEGORY PAGES ====================
    # Generic category router (handles all categories)
    path('category/<str:category_slug>/', views.category_view, name='category'),
    
    # Direct category shortcuts (optional, for cleaner URLs)
    path('business/', views.business_category, name='business'),
    path('culture/', views.culture_category, name='culture'),
    path('health/', views.health_category, name='health'),
    path('news/', views.news_category, name='news'),
    path('community/', views.community_category, name='community'),
    path('api/track-click/', track_click, name='track_click'),
    path('api/facets/', views.facets_api, name='facets_api'),
    path('about',views.about,name="about"),
    path('privacy',views.privacy,name="privacy"),
    path('terms',views.term,name="term"),
    path('contact-us',views.contact,name="contact"),
    path('search/images-load/', views.load_images, name='load_images'),


 
    # path('api/update-session-activity/', views.update_session_activity, name='update_session_activity'),  # ← Missing?
    path('click/', click_redirect, name='click_redirect'),
    
    # ==================== DETAIL PAGES ====================
    # Business detail
    path('business/<str:business_id>/', views.business_detail, name='business_detail'),
    
    # ==================== CITY-SPECIFIC ROUTES (OPTIONAL) ====================
    # Community by city (alternative URL pattern)
    path('community/<str:city_slug>/', views.community_by_city, name='community_by_city'),


    #  Testing endpoints 
    # path('debug/search/', views.debug_search_view, name='debug_search'),
    path('debug/search/', views.debug_search_view, name='debug_search'),
    # path('debug/business/', views.debug_business_search, name='debug_business'),
    # path('debug/schema/', views.debug_schema, name='debug_schema'),
    # path('debug/stage2/', views.debug_stage2_view, name='debug_stage2'),
    # path('debug/batch/', views.debug_batch_view, name='debug_batch'),

    # path('debug/word-discovery/', debug_word_discovery, name='debug_word_discovery')
    # If you want to test the fixed view:
    # path('business-test/', views.business_category_fixed, name='business_test'),

]

    # path('api/search', views.search_api, name='search_api'),
    # # Keep old endpoint for backwards compatibility if needed
    # path('api/submit/', views.search_api, name='form_submit'),

