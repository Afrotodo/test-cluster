from django.urls import path
from . import views
from . import searchapi

app_name="searchengine"





urlpatterns = [

path('', views.home, name='home'),
path('api/search/', views.search_suggestions, name='search_suggestions'),
path('api/submit/', views.form_submit, name='form_submit'),
path('search', views.search, name='search'),
path('category/<str:category_slug>/', views.category_view, name='category'),
path('business/', views.category_view, {'category_slug': 'business'}, name='business'),
path('culture/', views.category_view, {'category_slug': 'culture'}, name='culture'),
path('health/', views.category_view, {'category_slug': 'health'}, name='health'),
path('news/', views.category_view, {'category_slug': 'news'}, name='news'),
path('community/', views.category_view, {'category_slug': 'community'}, name='community'),
]

    # path('api/search', views.search_api, name='search_api'),
    # # Keep old endpoint for backwards compatibility if needed
    # path('api/submit/', views.search_api, name='form_submit'),

