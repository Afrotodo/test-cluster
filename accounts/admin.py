from django.contrib import admin

# Register your models here.
from django.contrib import admin
from .models import BusinessProfile, UserProfile, UserSession, SearchPreferences

@admin.register(BusinessProfile)
class BusinessProfileAdmin(admin.ModelAdmin):
    list_display = ('business_name', 'user', 'industry', 'city', 'country', 'created_at')
    list_filter = ('industry', 'country', 'state')
    search_fields = ('business_name', 'user__username', 'business_address')
   
   

@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'gender', 'city', 'country', 'created_at')
    list_filter = ('gender', 'country')
    search_fields = ('user__username', 'user__email', 'city')
  

@admin.register(UserSession)
class UserSessionAdmin(admin.ModelAdmin):
    list_display = ('user', 'ip_address', 'device_type', 'os', 'timestamp')
    list_filter = ('device_type', 'os')
    search_fields = ('user__username', 'ip_address')
    readonly_fields = ('timestamp',)
    date_hierarchy = 'timestamp'

@admin.register(SearchPreferences)
class SearchPreferencesAdmin(admin.ModelAdmin):
    list_display = ('user', 'default_language', 'safe_search', 'results_per_page')
    list_filter = ('safe_search', 'default_language')
    search_fields = ('user__username',)
