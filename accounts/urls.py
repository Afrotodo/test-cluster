from django.urls import path
from . import views

app_name = 'accounts'



urlpatterns = [
    path('login', views.login, name='login'),
    path('logout', views.logout, name='logout'),
    path('register', views.register, name='register'),
    path('register/business/', views.business, name='business'),
    path('register/user/', views.user, name='user'),
    path('register/username_check/', views.check_username, name='check-username'),
    path('register/email_check/', views.check_email, name='check-email'),
    path('password_check/', views.check_password, name='check-password'),
    path('login/', views.login, name='login'),
    path('logout/', views.logout, name='logout'),
    path('register/', views.register, name='register'),
    path('register/social_media_check/', views.validate_url_and_social, name='validate_url_and_social'),
    path('register/user/thank-you/', views.thankyoupage, name='thankyoupage'),
    path('register/business/thank-you/', views.thankyoupage, name='thankyoupage'),
    
  
]