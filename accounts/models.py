from django.db import models

# Create your models here.
from django.db import models
from django.contrib.auth.models import AbstractUser, User

# Create your models here.

class BusinessProfile(models.Model):  # Better name than BusinessSignup
    user = models.OneToOneField(User, on_delete=models.CASCADE)  # One-to-one relationship
    business_name = models.CharField(max_length=255)
    business_address = models.CharField(max_length=255)
    industry = models.CharField(max_length=100)
    website = models.URLField(unique=True)
    business_start_date = models.TextField()  # Changed from TextField
    submission_group=models.CharField(max_length=20, null=True, blank=True)
    store_type=models.CharField(max_length=8)
    gender = models.CharField(max_length=6, null=True, blank=True)

    # Location fields
    latitude = models.DecimalField(max_digits=9, decimal_places=6, null=True)  # More precise
    longitude = models.DecimalField(max_digits=9, decimal_places=6, null=True)
    country = models.CharField(max_length=4)  # Use ISO country codes
    state = models.CharField(max_length=50)
    city = models.CharField(max_length=100)
    postal_code = models.CharField(max_length=10)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Business Profile"
        verbose_name_plural = "Business Profiles"

class UserProfile(models.Model):  # Better name than UserSignup
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    gender = models.CharField(max_length=6, null=True, blank=True)
    birthdate = models.TextField()
    submission_group=models.CharField(max_length=10, null=True, blank=True)
    
    # Location fields
    country = models.CharField(max_length=4)  # Use ISO country codes
    state = models.CharField(max_length=50, blank=True)
    city = models.CharField(max_length=100)
    postal_code = models.CharField(max_length=10)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class UserSession(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    ip_address = models.GenericIPAddressField()
    browser = models.CharField(max_length=200)
    device_type = models.CharField(max_length=50)
    os = models.CharField(max_length=50)
    timestamp = models.DateTimeField(auto_now_add=True)


class SearchPreferences(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    default_language = models.CharField(max_length=2)  # ISO language code
    safe_search = models.BooleanField(default=True)
    results_per_page = models.IntegerField(default=10)
    preferred_categories = models.JSONField(null=True, blank=True)