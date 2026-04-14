# from django.shortcuts import render
# from django.http import HttpResponse 
# from django.contrib.auth.models import User, auth
# from .models import UserProfile, UserSession, BusinessProfile
# from django.contrib.auth import get_user_model
# import time
# import logging
# from django.contrib import messages
# from django.shortcuts import render, redirect
# import requests
# from django.contrib.auth import authenticate, login as auth_login
# from django.core.exceptions import ValidationError
# from django.core.validators import EmailValidator
# from user_agents import parse
# import re
# from django.conf import settings
# from django.http import Http404, HttpResponseBadRequest, JsonResponse
# import json
# from searchengine.geolocation import (
#     get_location_from_request,
#     get_client_ip,
#     get_device_info,
# )
# from decouple import config
# api_key = config('ABSTRACTAPI_GEOLOCATION_KEY_V2')
# logger = logging.getLogger(__name__)

# logging.basicConfig(
#     level=logging.ERROR,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     filename='app.log'
# )
# # views.py
# import json

# def get_location(request):
#     try:
#         x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
#         ip_address = x_forwarded_for.split(',')[0] if x_forwarded_for else request.META.get('REMOTE_ADDR')
        
#         response = requests.get(
#             f"https://ip-intelligence.abstractapi.com/v1/?api_key={api_key}&ip_address={ip_address}"
#         )
#         data = response.json()
        
#         return JsonResponse({
#             'ip_address': ip_address,
#             'city': data.get('location', {}).get('city', ''),
#             'region': data.get('location', {}).get('region_iso_code', ''),
#             'country': data.get('location', {}).get('country_code', ''),
#             'postal_code': data.get('location', {}).get('postal_code', ''),
#             'latitude': data.get('location', {}).get('latitude', ''),
#             'longitude': data.get('location', {}).get('longitude', ''),
#             'timezone': data.get('timezone', {}).get('name', '')
#         })
#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)

# def login(request):
#     if request.method == 'POST':
#         email = request.POST.get('email')
#         password = request.POST.get('password')

#         # Try to get user by email first
#         try:
#             username = User.objects.get(email=email).username
#             user = auth.authenticate(username=username, password=password)
#         except User.DoesNotExist:
#             user = None

#         if user is not None:
#             auth.login(request, user)
#             messages.success(request, 'Successfully logged in!')
#             return redirect('/')  # or wherever you want to redirect after login
#         else:
#             messages.error(request, 'Invalid email or password.')
#             return redirect('accounts:login') 
    
#     return render(request, 'login.html')

# def register(request):
#   return render(request, 'register.html')


# def logout(request):
#   auth.logout(request)
#   return redirect('/')

  
# def validate_url_and_social(request):

#     if request.method == 'POST':
           
#         input_text=request.POST.get('business_website')
#         input_text = input_text.strip().lower()
        
#         # URL patterns
#         url_patterns = [
#         # General URL pattern (requiring www)
#         r'^(https?:\/\/)?(www\.)([\w\-]+\.)+[\w\-]+(\/[\w\-\?\=\&\%\#\.]*)*\/?$',
        
#         # Social media handles pattern
#         r'^@[\w\._\-]+$',
        
#         # Common social media and platform patterns (these don't require www)
#         r'^(https?:\/\/)?(www\.)?(facebook|fb|twitter|instagram|linkedin|youtube|tiktok|pinterest|reddit|snapchat|fiverr)\.com\/?',
        
#         # Shopify store pattern
#         r'^(https?:\/\/)?([\w\-]+\.)?(shopify|myshopify)\.com\/?'
#     ]
        
#         # Check if input matches any pattern
#         for pattern in url_patterns:
#             if re.match(pattern, input_text):
#                 return HttpResponse("Matches url structure")
                
#         return HttpResponse("Awaiting match")
    


# def check_username(request):
   
#     username= request.POST.get('username')
#     if get_user_model().objects.filter(username=username).exists():
#         return HttpResponse("This username already exists")
#     else:
#        return HttpResponse("This username is available") 
    



# """
# Updated registration views using server-side geolocation.

# Replace your existing register, business, and user views with these.
# Location is resolved server-side via get_location_from_request() which:
#   1. Checks Django session first (free, instant)
#   2. Only hits AbstractAPI if no session data exists (once per session)

# No more JS-based location fetching. No more /accounts/get-location/ endpoint needed.
# """

# import time
# import logging
# from django.shortcuts import render, redirect
# from django.contrib.auth.models import User
# from django.contrib import messages

# from searchengine.geolocation import (
#     get_location_from_request,
#     get_client_ip,
#     get_device_info,
# )

# logger = logging.getLogger(__name__)


# # ==================== REGISTER PAGE (GET) ====================

# def register(request):
#     """
#     Render the registration page with location data pre-populated.
#     Location comes from server-side IP geolocation (cached in session).
#     """
#     location = get_location_from_request(request) or {}

#     context = {
#         'user_location': location,
#     }
#     return render(request, 'register.html', context)


# # ==================== USER REGISTRATION (POST) ====================

# def user(request):
#     """Handle user registration form submission."""
#     if request.method != 'POST':
#         return redirect('accounts:register')

#     try:
#         # Get IP and device info
#         ip_address = get_client_ip(request)
#         device = get_device_info(request.META.get('HTTP_USER_AGENT', ''))

#         # Get location — session first, API only if needed
#         location = get_location_from_request(request) or {}

#         print(f"[USER REGISTER] Location: {location}")

#         # Get form data
#         username = request.POST.get('username', '').strip()
#         email = request.POST.get('email', '').strip()
#         password = request.POST.get('password', '')
#         gender = request.POST.get('Gender', '')
#         birthdate_month = request.POST.get('Birthdate_One', '')
#         birthdate_day = request.POST.get('Birthdate_Two', '')
#         birthdate_year = request.POST.get('Birthdate_Three', '')
#         birthdate = f"{birthdate_month}-{birthdate_day.zfill(2)}-{birthdate_year}"

#         # Validate
#         if not username or not email or not password:
#             messages.error(request, "Please fill in all required fields.")
#             return render(request, 'register.html', {'user_location': location})

#         if User.objects.filter(username=username).exists():
#             messages.error(request, "Username already exists. Please choose a different username.")
#             return render(request, 'register.html', {'user_location': location})

#         if User.objects.filter(email=email).exists():
#             messages.error(request, "Email already exists. Please choose a different email.")
#             return render(request, 'register.html', {'user_location': location})

#         # Create user
#         user_obj = User.objects.create_user(
#             username=username,
#             email=email,
#             password=password,
#         )

#         # Create profile
#         from .models import UserProfile, UserSession

#         UserProfile.objects.create(
#             user=user_obj,
#             gender=gender,
#             birthdate=birthdate,
#             submission_group='UserGroup',
#             country=location.get('country_code') or '',
#             state=location.get('region_code') or location.get('region') or '',
#             city=location.get('city') or '',
#             postal_code=location.get('postal') or '',
#         )

#         # Create session record
#         UserSession.objects.create(
#             user=user_obj,
#             ip_address=ip_address,
#             browser=f"{device['browser']} {device.get('browser_version', '')}".strip(),
#             device_type=device['device_type'],
#             os=f"{device['os']} {device.get('os_version', '')}".strip(),
#         )

#         print(f"[USER REGISTER] Success: {username} from {location.get('city', 'Unknown')}")
#         messages.success(request, "Registration successful!")
#         return redirect('accounts:thankyoupage')

#     except Exception as e:
#         logger.error(f"User registration error: {str(e)}")
#         print(f"[USER REGISTER] Error: {type(e).__name__}: {str(e)}")
#         messages.error(request, "An error occurred during registration. Please try again.")
#         return redirect('accounts:register')


# # ==================== BUSINESS REGISTRATION (POST) ====================

# def business(request):
#     """Handle business registration form submission."""
#     if request.method != 'POST':
#         return redirect('accounts:register')

#     try:
#         # Get IP and device info
#         ip_address = get_client_ip(request)
#         device = get_device_info(request.META.get('HTTP_USER_AGENT', ''))

#         # Get location — session first, API only if needed
#         location = get_location_from_request(request) or {}

#         print(f"[BUSINESS REGISTER] Location: {location}")

#         # Get form data
#         username = request.POST.get('username', '').strip()
#         email = request.POST.get('email', '').strip()
#         password = request.POST.get('password', '')
#         business_name = request.POST.get('business_name', '').strip()
#         business_website = request.POST.get('business_website', '').strip()
#         industry = request.POST.get('industry', '')
#         store_type = request.POST.get('store_type', '')
#         gender = request.POST.get('Gender', '')
#         birthdate_month = request.POST.get('Birthdate_One', '')
#         birthdate_day = request.POST.get('Birthdate_Two', '')
#         birthdate_year = request.POST.get('Birthdate_Three', '')
#         business_start_date = f"{birthdate_month}-{birthdate_day.zfill(2)}-{birthdate_year}"

#         # Validate
#         if not username or not email or not password or not business_name:
#             messages.error(request, "Please fill in all required fields.")
#             return render(request, 'register.html', {'user_location': location})

#         if User.objects.filter(username=username).exists():
#             messages.error(request, "Username already exists. Please choose a different username.")
#             return render(request, 'register.html', {'user_location': location})

#         if User.objects.filter(email=email).exists():
#             messages.error(request, "Email already exists. Please choose a different email.")
#             return render(request, 'register.html', {'user_location': location})

#         # Create user
#         user_obj = User.objects.create_user(
#             username=username,
#             email=email,
#             password=password,
#         )

#         time.sleep(1)

#         # Create business profile
#         from .models import BusinessProfile, UserSession

#         BusinessProfile.objects.create(
#             user=user_obj,
#             business_name=business_name,
#             business_address='None',
#             industry=industry,
#             website=business_website,
#             gender=gender,
#             business_start_date=business_start_date,
#             store_type=store_type,
#             submission_group='BusinessGroup',

#             # Location from server-side geolocation
#             latitude=location.get('lat') or None,
#             longitude=location.get('lng') or None,
#             country=location.get('country_code') or '',
#             state=location.get('region_code') or location.get('region') or '',
#             city=location.get('city') or '',
#             postal_code=location.get('postal') or '',
#         )

#         # Create session record
#         UserSession.objects.create(
#             user=user_obj,
#             ip_address=ip_address,
#             browser=f"{device['browser']} {device.get('browser_version', '')}".strip(),
#             device_type=device['device_type'],
#             os=f"{device['os']} {device.get('os_version', '')}".strip(),
#         )

#         print(f"[BUSINESS REGISTER] Success: {business_name} ({username}) from {location.get('city', 'Unknown')}")
#         messages.success(request, "Registration successful!")
#         return redirect('accounts:thankyoupage')

#     except Exception as e:
#         logger.error(f"Business registration error: {str(e)}")
#         print(f"[BUSINESS REGISTER] Error: {type(e).__name__}: {str(e)}")
#         messages.error(request, "An error occurred during registration. Please try again.")
#         return redirect('accounts:register')
    
# # def business(request):
# #     location_values = {}
           
# #     if request.method != 'POST':
# #         return render(request, 'register.html')
    
# #     try:
# #         # Get user's IP address from request headers
# #         x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
# #         ip_address = x_forwarded_for.split(',')[0] if x_forwarded_for else request.META.get('REMOTE_ADDR')

# #         # Get device information from user agent
# #         ua_string = request.META.get('HTTP_USER_AGENT', '')
# #         user_agent = parse(ua_string)
# #         device_info = {
# #             'browser': f"{user_agent.browser.family} {user_agent.browser.version_string}",
# #             'device_type': 'Mobile' if user_agent.is_mobile else ('Tablet' if user_agent.is_tablet else 'Desktop'),
# #             'operating_system': f"{user_agent.os.family} {user_agent.os.version_string}"
# #         }

# #         # Priority 1: Try to get location data from frontend form submission
# #         frontend_ip = request.POST.get('user_ip', '').strip()
# #         frontend_city = request.POST.get('user_city', '').strip()
# #         frontend_region = request.POST.get('user_region', '').strip()
# #         frontend_country = request.POST.get('user_country', '').strip()
# #         frontend_postal = request.POST.get('user_postal', '').strip()
# #         frontend_lat = request.POST.get('user_latitude', '').strip()
# #         frontend_lng = request.POST.get('user_longitude', '').strip()
# #         frontend_timezone = request.POST.get('user_timezone', '').strip()

# #         print("Frontend location data received:")
# #         print(f"IP: {frontend_ip}")
# #         print(f"City: {frontend_city}")
# #         print(f"Region: {frontend_region}")
# #         print(f"Country: {frontend_country}")
# #         print(f"Postal: {frontend_postal}")
# #         print(f"Lat: {frontend_lat}")
# #         print(f"Lng: {frontend_lng}")
# #         print(f"Timezone: {frontend_timezone}")

# #         # Use frontend data if available, otherwise fallback to API call
# #         if frontend_ip and (frontend_city or frontend_region or frontend_country):
# #             # Use frontend location data
# #             location_info = {
# #                 'ip_address': frontend_ip,
# #                 'city': frontend_city,
# #                 'region': frontend_region,
# #                 'country': frontend_country,
# #                 'postal_code': frontend_postal,
# #                 'latitude': frontend_lat,
# #                 'longitude': frontend_lng,
# #                 'timezone': frontend_timezone
# #             }
# #             print("✅ Using frontend location data")
# #         else:
# #             # Priority 2: Fallback to API call with actual user IP
# #             print("❌ Frontend data incomplete, falling back to API call")
# #             try:
# #                 # Use the actual user's IP address instead of hardcoded one
 
# #                 api_url = f"https://ip-intelligence.abstractapi.com/v1/?api_key={api_key}&ip_address={ip_address}"
# #                 print(f"Making API call to: {api_url}")
# #                 response = requests.get(api_url)
                
# #                 if response.status_code == 200:
# #                     data = response.json()
# #                     location_info = {
# #                         'ip_address': ip_address,
# #                         'city': data.get('location', {}).get('city', ''),
# #                         'region': data.get('location', {}).get('region_iso_code', ''),
# #                         'country': data.get('location', {}).get('country_code', ''),
# #                         'postal_code': data.get('location', {}).get('postal_code', ''),
# #                         'latitude': data.get('location', {}).get('latitude', ''),
# #                         'longitude': data.get('location', {}).get('longitude', ''),
# #                         'timezone': data.get('timezone', {}).get('name', '')
# #                     }
# #                     print("✅ Using API location data")
# #                 else:
# #                     raise Exception(f"API request failed: {response.status_code}")
# #             except Exception as e:
# #                 print(f"❌ API call failed: {str(e)}")
# #                 logger.error(f"Error getting location data: {str(e)}")
# #                 location_info = {
# #                     'ip_address': ip_address,
# #                     'city': '',
# #                     'region': '',
# #                     'country': '',
# #                     'postal_code': '',
# #                     'latitude': '',
# #                     'longitude': '',
# #                     'timezone': ''
# #                 }
# #                 print("⚠️ Using default empty location data")

# #         # Format location_values for backward compatibility
# #         for key, value in location_info.items():
# #             formatted_key = key.replace('_', ' ').title()
# #             location_values[formatted_key] = value

# #         # Get user registration data
# #         username = request.POST['username']
# #         email = request.POST['email']
# #         business_name = request.POST['business_name']
# #         business_website = request.POST['business_website']
# #         industry = request.POST['industry']
# #         store_type = request.POST['store_type']
# #         password = request.POST['password']
# #         gender = request.POST['Gender']
# #         Birthdate_One = request.POST['Birthdate_One']
# #         Birthdate_Two = request.POST['Birthdate_Two']
# #         Birthdate_Three = request.POST['Birthdate_Three']
# #         business_start_date = f"{Birthdate_One}-{Birthdate_Two.zfill(2)}-{Birthdate_Three.zfill(2)}"

# #         # Check existing users
# #         if User.objects.filter(username=username).exists():
# #             messages.error(request, "Username already exists. Please choose a different username.")
# #             return render(request, 'register.html')
            
# #         if User.objects.filter(email=email).exists():
# #             messages.error(request, "Email already exists. Please choose a different email.")
# #             return render(request, 'register.html')

# #         # Create User instance
# #         user = User.objects.create_user(
# #             username=username,
# #             email=email,
# #             password=password
# #         )

# #         time.sleep(1)

# #         print("Before creating BusinessProfile")
# #         print(f"User data: {user}")
# #         print(f"Gender: {gender}")
# #         print(f"Business start date: {business_start_date}")
# #         print(f"Final location data: {location_info}")
# #         print(f"Formatted location values: {location_values}")

# #         BusinessProfile.objects.create(
# #             user=user,
# #             business_name=business_name,
# #             business_address='None',  
# #             industry=industry,
# #             website=business_website,
# #             gender=gender,
# #             business_start_date=business_start_date,
# #             store_type=store_type,
# #             submission_group='BusinessGroup',
            
# #             # Location fields - using location_info directly for accuracy
# #             latitude=location_info.get('latitude', ''),
# #             longitude=location_info.get('longitude', ''),
# #             country=location_info.get('country', ''),
# #             state=location_info.get('region', ''),
# #             city=location_info.get('city', ''),
# #             postal_code=location_info.get('postal_code', ''),
# #         )

# #         # Create UserSession
# #         UserSession.objects.create(
# #             user=user,
# #             ip_address=location_info.get('ip_address', ip_address),
# #             browser=device_info['browser'],
# #             device_type=device_info['device_type'],
# #             os=device_info['operating_system']
# #         )

# #         print("✅ Business registration completed successfully")
# #         messages.success(request, "Registration successful!")
# #         return redirect('accounts:thankyoupage')

# #     except Exception as e:
# #         logger.error(f"Registration error: {str(e)}")
# #         print(f"❌ Error type: {type(e)}")
# #         print(f"❌ Error message: {str(e)}")
# #         messages.error(request, "An error occurred during registration. Please try again.")
# #         return redirect('accounts:register')




# # def user(request):
# #     location_values = {}
           
# #     if request.method != 'POST':
# #         return render(request, 'register.html')
    
# #     try:
# #         # Get user's IP address from request headers
# #         x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
# #         ip_address = x_forwarded_for.split(',')[0] if x_forwarded_for else request.META.get('REMOTE_ADDR')

# #         # Get device information from user agent
# #         ua_string = request.META.get('HTTP_USER_AGENT', '')
# #         user_agent = parse(ua_string)
# #         device_info = {
# #             'browser': f"{user_agent.browser.family} {user_agent.browser.version_string}",
# #             'device_type': 'Mobile' if user_agent.is_mobile else ('Tablet' if user_agent.is_tablet else 'Desktop'),
# #             'operating_system': f"{user_agent.os.family} {user_agent.os.version_string}"
# #         }

# #         # Priority 1: Try to get location data from frontend form submission
# #         frontend_ip = request.POST.get('user_ip', '').strip()
# #         frontend_city = request.POST.get('user_city', '').strip()
# #         frontend_region = request.POST.get('user_region', '').strip()
# #         frontend_country = request.POST.get('user_country', '').strip()
# #         frontend_postal = request.POST.get('user_postal', '').strip()
# #         frontend_lat = request.POST.get('user_latitude', '').strip()
# #         frontend_lng = request.POST.get('user_longitude', '').strip()
# #         frontend_timezone = request.POST.get('user_timezone', '').strip()

# #         print("Frontend location data received:")
# #         print(f"IP: {frontend_ip}")
# #         print(f"City: {frontend_city}")
# #         print(f"Region: {frontend_region}")
# #         print(f"Country: {frontend_country}")
# #         print(f"Postal: {frontend_postal}")
# #         print(f"Lat: {frontend_lat}")
# #         print(f"Lng: {frontend_lng}")
# #         print(f"Timezone: {frontend_timezone}")

# #         # Use frontend data if available, otherwise fallback to API call
# #         if frontend_ip and (frontend_city or frontend_region or frontend_country):
# #             # Use frontend location data
# #             location_info = {
# #                 'ip_address': frontend_ip,
# #                 'city': frontend_city,
# #                 'region': frontend_region,
# #                 'country': frontend_country,
# #                 'postal_code': frontend_postal,
# #                 'latitude': frontend_lat,
# #                 'longitude': frontend_lng,
# #                 'timezone': frontend_timezone
# #             }
# #             print("✅ Using frontend location data")
# #         else:
# #             # Priority 2: Fallback to API call with actual user IP
# #             print("❌ Frontend data incomplete, falling back to API call")
# #             try:
# #                 # Use the actual user's IP address instead of hardcoded one
# #                 api_url = f"https://ip-intelligence.abstractapi.com/v1/?api_key={api_key}&ip_address={ip_address}"
# #                 print(f"Making API call to: {api_url}")
# #                 response = requests.get(api_url)
                
# #                 if response.status_code == 200:
# #                     data = response.json()
# #                     location_info = {
# #                         'ip_address': ip_address,
# #                         'city': data.get('location', {}).get('city', ''),
# #                         'region': data.get('location', {}).get('region_iso_code', ''),
# #                         'country': data.get('location', {}).get('country_code', ''),
# #                         'postal_code': data.get('location', {}).get('postal_code', ''),
# #                         'latitude': data.get('location', {}).get('latitude', ''),
# #                         'longitude': data.get('location', {}).get('longitude', ''),
# #                         'timezone': data.get('timezone', {}).get('name', '')
# #                     }
# #                     print("✅ Using API location data")
# #                 else:
# #                     raise Exception(f"API request failed: {response.status_code}")
# #             except Exception as e:
# #                 print(f"❌ API call failed: {str(e)}")
# #                 logger.error(f"Error getting location data: {str(e)}")
# #                 location_info = {
# #                     'ip_address': ip_address,
# #                     'city': '',
# #                     'region': '',
# #                     'country': '',
# #                     'postal_code': '',
# #                     'latitude': '',
# #                     'longitude': '',
# #                     'timezone': ''
# #                 }
# #                 print("⚠️ Using default empty location data")

# #         # Format location_values for backward compatibility
# #         for key, value in location_info.items():
# #             formatted_key = key.replace('_', ' ').title()
# #             location_values[formatted_key] = value

# #         # Get user registration data
# #         username = request.POST['username']
# #         email = request.POST['email']
# #         password = request.POST['password']
# #         gender = request.POST['Gender']
# #         Birthdate_One = request.POST['Birthdate_One']
# #         Birthdate_Two = request.POST['Birthdate_Two']
# #         Birthdate_Three = request.POST['Birthdate_Three']
# #         birthdate = f"{Birthdate_One}-{Birthdate_Two.zfill(2)}-{Birthdate_Three.zfill(2)}"

# #         # Validate existing users
# #         if User.objects.filter(username=username).exists():
# #             messages.error(request, "Username already exists. Please choose a different username.")
# #             return render(request, 'register.html')
            
# #         if User.objects.filter(email=email).exists():
# #             messages.error(request, "Email already exists. Please choose a different email.")
# #             return render(request, 'register.html')

# #         # Create user
# #         user = User.objects.create_user(
# #             username=username,
# #             email=email,
# #             password=password
# #         )

# #         time.sleep(1)

# #         print("Before creating UserProfile")
# #         print(f"User data: {user}")
# #         print(f"Gender: {gender}")
# #         print(f"Birthdate: {birthdate}")
# #         print(f"Final location data: {location_info}")
# #         print(f"Formatted location values: {location_values}")

# #         # Create UserProfile with location data
# #         UserProfile.objects.create(
# #             user=user,
# #             gender=gender,
# #             birthdate=birthdate,
# #             # Using location_info directly for accuracy
# #             country=location_info.get('country', ''),
# #             state=location_info.get('region', ''),
# #             city=location_info.get('city', ''),
# #             postal_code=location_info.get('postal_code', ''),
# #             submission_group='UserGroup'
# #         )

# #         # Create user session
# #         UserSession.objects.create(
# #             user=user,
# #             ip_address=location_info.get('ip_address', ip_address),
# #             browser=device_info['browser'],
# #             device_type=device_info['device_type'],
# #             os=device_info['operating_system']
# #         )

# #         print("✅ User registration completed successfully")
# #         messages.success(request, "Registration successful!")
# #         return redirect('accounts:thankyoupage')

# #     except Exception as e:
# #         logger.error(f"Registration error: {str(e)}")
# #         print(f"❌ Error type: {type(e)}")
# #         print(f"❌ Error message: {str(e)}")
# #         messages.error(request, "An error occurred during registration. Please try again.")
# #         return redirect('accounts:register')
    
# def thankyoupage(request):
#     return render(request, 'thankyou.html')




# def check_email(request):
   

#     email = request.POST.get('email')
    
#     # Regex pattern for validating an email
#     email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    
#     # Check if the email format is valid
#     if not re.match(email_pattern, email):
#         return HttpResponse("Awaiting correct email format.")
    
#     # Check if the email already exists in the database
#     if get_user_model().objects.filter(email=email).exists():
#         return HttpResponse("This email already exists.")
#     else:
#         return HttpResponse("This email is available.") 
    



# def check_password(request):
#     if request.method == 'POST':
#             password_one = request.POST.get('password_one')
#             password_two= request.POST.get('password_two')

#             if password_one and password_two:
#                 if password_one == password_two:
#                     return HttpResponse("Passwords match.")
#                 else:
#                     return HttpResponse("Passwords do not match.")
#             else:
#                 return HttpResponse("Please fill both password fields.")
#     return HttpResponse("Invalid request", status=400)

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