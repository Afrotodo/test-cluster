from django.urls import path
from . import views
from . import searchapi

app_name="searchengine"





urlpatterns = [

path('', views.home, name='home'),
path('api/search/', views.search_suggestions, name='search_suggestions'),
path('api/submit/', views.form_submit, name='form_submit'),
path('search', views.search, name='search'),

    # path('api/search', views.search_api, name='search_api'),
    # # Keep old endpoint for backwards compatibility if needed
    # path('api/submit/', views.search_api, name='form_submit'),

]