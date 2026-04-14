import re
import time
import logging

from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.contrib.auth.models import User
from django.contrib.auth import get_user_model
from django.contrib import messages
from django.contrib.auth import authenticate, login as auth_login, logout as auth_logout
from django.contrib.auth import authenticate

from .models import UserProfile, UserSession, BusinessProfile
from searchengine.geolocation import (
    get_location_from_request,
    get_client_ip,
    get_device_info,
)

logger = logging.getLogger(__name__)


# =============================================================================
# AUTH VIEWS
# =============================================================================

def login(request):
    """Handle user login."""
    if request.method != 'POST':
        return render(request, 'login.html')

    email = request.POST.get('email', '').strip()
    password = request.POST.get('password', '')

    try:
        username = User.objects.get(email=email).username
        user = authenticate(username=username, password=password)
    except User.DoesNotExist:
        user = None

    if user is not None:
        auth_login(request, user)
        messages.success(request, 'Successfully logged in!')
        return redirect('/')
    else:
        messages.error(request, 'Invalid email or password.')
        return redirect('accounts:login')


def logout(request):
    """Handle user logout."""
    auth_logout(request)
    return redirect('/')


# =============================================================================
# REGISTRATION VIEWS
# =============================================================================

def register(request):
    """
    Render the registration page with location data pre-populated.
    Location comes from server-side IP geolocation (cached in session).
    """
    location = get_location_from_request(request) or {}
    return render(request, 'register.html', {'user_location': location})


def user(request):
    """Handle user registration form submission."""
    if request.method != 'POST':
        return redirect('accounts:register')

    # Get location early so we can pass it back on validation errors
    location = get_location_from_request(request) or {}

    try:
        ip_address = get_client_ip(request)
        device = get_device_info(request.META.get('HTTP_USER_AGENT', ''))

        # Get form data
        username = request.POST.get('username', '').strip()
        email = request.POST.get('email', '').strip()
        password = request.POST.get('password', '')
        gender = request.POST.get('Gender', '')
        birthdate_month = request.POST.get('Birthdate_One', '')
        birthdate_day = request.POST.get('Birthdate_Two', '')
        birthdate_year = request.POST.get('Birthdate_Three', '')
        birthdate = f"{birthdate_month}-{birthdate_day.zfill(2)}-{birthdate_year}"

        # Validate required fields
        if not username or not email or not password:
            messages.error(request, "Please fill in all required fields.")
            return render(request, 'register.html', {'user_location': location})

        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already exists. Please choose a different username.")
            return render(request, 'register.html', {'user_location': location})

        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already exists. Please choose a different email.")
            return render(request, 'register.html', {'user_location': location})

        # Create user
        user_obj = User.objects.create_user(
            username=username,
            email=email,
            password=password,
        )

        # Create profile
        UserProfile.objects.create(
            user=user_obj,
            gender=gender,
            birthdate=birthdate,
            submission_group='UserGroup',
            country=location.get('country_code') or '',
            state=location.get('region_code') or location.get('region') or '',
            city=location.get('city') or '',
            postal_code=location.get('postal') or '',
        )

        # Create session record
        UserSession.objects.create(
            user=user_obj,
            ip_address=ip_address,
            browser=f"{device['browser']} {device.get('browser_version', '')}".strip(),
            device_type=device['device_type'],
            os=f"{device['os']} {device.get('os_version', '')}".strip(),
        )

        messages.success(request, "Registration successful!")
        return redirect('accounts:thankyoupage')

    except Exception as e:
        logger.error(f"User registration error: {e}")
        messages.error(request, "An error occurred during registration. Please try again.")
        return redirect('accounts:register')


def business(request):
    """Handle business registration form submission."""
    if request.method != 'POST':
        location = get_location_from_request(request) or {}
        return render(request, 'register.html', {'user_location': location})

    # Get location early so we can pass it back on validation errors
    location = get_location_from_request(request) or {}

    try:
        ip_address = get_client_ip(request)
        device = get_device_info(request.META.get('HTTP_USER_AGENT', ''))

        # Get form data
        username = request.POST.get('username', '').strip()
        email = request.POST.get('email', '').strip()
        password = request.POST.get('password', '')
        business_name = request.POST.get('business_name', '').strip()
        business_website = request.POST.get('business_website', '').strip()
        industry = request.POST.get('industry', '')
        store_type = request.POST.get('store_type', '')
        gender = request.POST.get('Gender', '')
        birthdate_month = request.POST.get('Birthdate_One', '')
        birthdate_day = request.POST.get('Birthdate_Two', '')
        birthdate_year = request.POST.get('Birthdate_Three', '')
        business_start_date = f"{birthdate_month}-{birthdate_day.zfill(2)}-{birthdate_year}"

        # Validate required fields
        if not username or not email or not password or not business_name:
            messages.error(request, "Please fill in all required fields.")
            return render(request, 'register.html', {'user_location': location})

        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already exists. Please choose a different username.")
            return render(request, 'register.html', {'user_location': location})

        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already exists. Please choose a different email.")
            return render(request, 'register.html', {'user_location': location})

        # Create user
        user_obj = User.objects.create_user(
            username=username,
            email=email,
            password=password,
        )

        time.sleep(1)

        # Create business profile
        BusinessProfile.objects.create(
            user=user_obj,
            business_name=business_name,
            business_address='None',
            industry=industry,
            website=business_website,
            gender=gender,
            business_start_date=business_start_date,
            store_type=store_type,
            submission_group='BusinessGroup',
            latitude=location.get('lat') or None,
            longitude=location.get('lng') or None,
            country=location.get('country_code') or '',
            state=location.get('region_code') or location.get('region') or '',
            city=location.get('city') or '',
            postal_code=location.get('postal') or '',
        )

        # Create session record
        UserSession.objects.create(
            user=user_obj,
            ip_address=ip_address,
            browser=f"{device['browser']} {device.get('browser_version', '')}".strip(),
            device_type=device['device_type'],
            os=f"{device['os']} {device.get('os_version', '')}".strip(),
        )

        messages.success(request, "Registration successful!")
        return redirect('accounts:thankyoupage')

    except Exception as e:
        logger.error(f"Business registration error: {e}")
        messages.error(request, "An error occurred during registration. Please try again.")
        return redirect('accounts:register')


def thankyoupage(request):
    """Thank you page after registration."""
    return render(request, 'thankyou.html')


# =============================================================================
# HTMX VALIDATION ENDPOINTS
# =============================================================================

def check_username(request):
    """HTMX: Check if username is available."""
    username = request.POST.get('username', '').strip()
    if not username:
        return HttpResponse("")
    if get_user_model().objects.filter(username=username).exists():
        return HttpResponse("This username already exists")
    return HttpResponse("This username is available")


def check_email(request):
    """HTMX: Check if email is valid and available."""
    email = request.POST.get('email', '').strip()
    if not email:
        return HttpResponse("")

    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    if not re.match(email_pattern, email):
        return HttpResponse("Awaiting correct email format.")

    if get_user_model().objects.filter(email=email).exists():
        return HttpResponse("This email already exists.")
    return HttpResponse("This email is available.")


def validate_url_and_social(request):
    """HTMX: Validate business website or social media handle."""
    if request.method != 'POST':
        return HttpResponse("")

    input_text = request.POST.get('business_website', '').strip().lower()
    if not input_text:
        return HttpResponse("")

    url_patterns = [
        r'^(https?:\/\/)?(www\.)([\w\-]+\.)+[\w\-]+(\/[\w\-\?\=\&\%\#\.]*)*\/?$',
        r'^@[\w\._\-]+$',
        r'^(https?:\/\/)?(www\.)?(facebook|fb|twitter|instagram|linkedin|youtube|tiktok|pinterest|reddit|snapchat|fiverr)\.com\/?',
        r'^(https?:\/\/)?([\w\-]+\.)?(shopify|myshopify)\.com\/?',
    ]

    for pattern in url_patterns:
        if re.match(pattern, input_text):
            return HttpResponse("Matches url structure")

    return HttpResponse("Awaiting match")


def check_password(request):
    """HTMX: Check if passwords match."""
    if request.method != 'POST':
        return HttpResponse("Invalid request", status=400)

    password_one = request.POST.get('password_one', '')
    password_two = request.POST.get('password_two', '')

    if not password_one or not password_two:
        return HttpResponse("Please fill both password fields.")
    if password_one == password_two:
        return HttpResponse("Passwords match.")
    return HttpResponse("Passwords do not match.")