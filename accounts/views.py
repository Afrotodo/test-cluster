from django.shortcuts import render
from django.http import HttpResponse 
from django.contrib.auth.models import User, auth
from .models import UserProfile, UserSession, BusinessProfile
from django.contrib.auth import get_user_model
import time
import logging
from django.contrib import messages
from django.shortcuts import render, redirect
import requests
from django.contrib.auth import authenticate, login as auth_login
from django.core.exceptions import ValidationError
from django.core.validators import EmailValidator
from user_agents import parse
import re


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='app.log'
)

def login(request):
    if request.method == 'POST':
        email = request.POST.get('email')
        password = request.POST.get('password')

        # Try to get user by email first
        try:
            username = User.objects.get(email=email).username
            user = auth.authenticate(username=username, password=password)
        except User.DoesNotExist:
            user = None

        if user is not None:
            auth.login(request, user)
            messages.success(request, 'Successfully logged in!')
            return redirect('/')  # or wherever you want to redirect after login
        else:
            messages.error(request, 'Invalid email or password.')
            return redirect('accounts:login') 
    
    return render(request, 'login.html')




def register(request):
  return render(request, 'register.html')



def logout(request):
  auth.logout(request)
  return redirect('/')

  
def validate_url_and_social(request):

    if request.method == 'POST':
           
        input_text=request.POST.get('business_website')
        input_text = input_text.strip().lower()
        
        # URL patterns
        url_patterns = [
        # General URL pattern (requiring www)
        r'^(https?:\/\/)?(www\.)([\w\-]+\.)+[\w\-]+(\/[\w\-\?\=\&\%\#\.]*)*\/?$',
        
        # Social media handles pattern
        r'^@[\w\._\-]+$',
        
        # Common social media and platform patterns (these don't require www)
        r'^(https?:\/\/)?(www\.)?(facebook|fb|twitter|instagram|linkedin|youtube|tiktok|pinterest|reddit|snapchat|fiverr)\.com\/?',
        
        # Shopify store pattern
        r'^(https?:\/\/)?([\w\-]+\.)?(shopify|myshopify)\.com\/?'
    ]
        
        # Check if input matches any pattern
        for pattern in url_patterns:
            if re.match(pattern, input_text):
                return HttpResponse("Matches url structure")
                
        return HttpResponse("Awaiting match")
    


def check_username(request):
   
    username= request.POST.get('username')
    if get_user_model().objects.filter(username=username).exists():
        return HttpResponse("This username already exists")
    else:
       return HttpResponse("This username is available") 
    


def business(request):
    location_values = {}
           
    if request.method != 'POST':
        return render(request, 'register.html')
    
    try:
        # Get user's IP address from request headers
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        ip_address = x_forwarded_for.split(',')[0] if x_forwarded_for else request.META.get('REMOTE_ADDR')

        # Get device information from user agent
        ua_string = request.META.get('HTTP_USER_AGENT', '')
        user_agent = parse(ua_string)
        device_info = {
            'browser': f"{user_agent.browser.family} {user_agent.browser.version_string}",
            'device_type': 'Mobile' if user_agent.is_mobile else ('Tablet' if user_agent.is_tablet else 'Desktop'),
            'operating_system': f"{user_agent.os.family} {user_agent.os.version_string}"
        }

        # Priority 1: Try to get location data from frontend form submission
        frontend_ip = request.POST.get('user_ip', '').strip()
        frontend_city = request.POST.get('user_city', '').strip()
        frontend_region = request.POST.get('user_region', '').strip()
        frontend_country = request.POST.get('user_country', '').strip()
        frontend_postal = request.POST.get('user_postal', '').strip()
        frontend_lat = request.POST.get('user_latitude', '').strip()
        frontend_lng = request.POST.get('user_longitude', '').strip()
        frontend_timezone = request.POST.get('user_timezone', '').strip()

        print("Frontend location data received:")
        print(f"IP: {frontend_ip}")
        print(f"City: {frontend_city}")
        print(f"Region: {frontend_region}")
        print(f"Country: {frontend_country}")
        print(f"Postal: {frontend_postal}")
        print(f"Lat: {frontend_lat}")
        print(f"Lng: {frontend_lng}")
        print(f"Timezone: {frontend_timezone}")

        # Use frontend data if available, otherwise fallback to API call
        if frontend_ip and (frontend_city or frontend_region or frontend_country):
            # Use frontend location data
            location_info = {
                'ip_address': frontend_ip,
                'city': frontend_city,
                'region': frontend_region,
                'country': frontend_country,
                'postal_code': frontend_postal,
                'latitude': frontend_lat,
                'longitude': frontend_lng,
                'timezone': frontend_timezone
            }
            print("✅ Using frontend location data")
        else:
            # Priority 2: Fallback to API call with actual user IP
            print("❌ Frontend data incomplete, falling back to API call")
            try:
                # Use the actual user's IP address instead of hardcoded one
                api_url = f"https://ipgeolocation.abstractapi.com/v1/?api_key=69f8a774e279408e8fa7f7d0ed6937d6&ip_address={ip_address}"
                print(f"Making API call to: {api_url}")
                response = requests.get(api_url)
                
                if response.status_code == 200:
                    data = response.json()
                    location_info = {
                        'ip_address': ip_address,
                        'city': data.get('city', ''),
                        'region': data.get('region_iso_code', ''),
                        'country': data.get('country_code', ''),
                        'postal_code': data.get('postal_code', ''),
                        'latitude': data.get('latitude', ''),
                        'longitude': data.get('longitude', ''),
                        'timezone': data.get('timezone', {}).get('name', '')
                    }
                    print("✅ Using API location data")
                else:
                    raise Exception(f"API request failed: {response.status_code}")
            except Exception as e:
                print(f"❌ API call failed: {str(e)}")
                logger.error(f"Error getting location data: {str(e)}")
                location_info = {
                    'ip_address': ip_address,
                    'city': '',
                    'region': '',
                    'country': '',
                    'postal_code': '',
                    'latitude': '',
                    'longitude': '',
                    'timezone': ''
                }
                print("⚠️ Using default empty location data")

        # Format location_values for backward compatibility
        for key, value in location_info.items():
            formatted_key = key.replace('_', ' ').title()
            location_values[formatted_key] = value

        # Get user registration data
        username = request.POST['username']
        email = request.POST['email']
        business_name = request.POST['business_name']
        business_website = request.POST['business_website']
        industry = request.POST['industry']
        store_type = request.POST['store_type']
        password = request.POST['password']
        gender = request.POST['Gender']
        Birthdate_One = request.POST['Birthdate_One']
        Birthdate_Two = request.POST['Birthdate_Two']
        Birthdate_Three = request.POST['Birthdate_Three']
        business_start_date = f"{Birthdate_One}-{Birthdate_Two.zfill(2)}-{Birthdate_Three.zfill(2)}"

        # Check existing users
        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already exists. Please choose a different username.")
            return render(request, 'register.html')
            
        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already exists. Please choose a different email.")
            return render(request, 'register.html')

        # Create User instance
        user = User.objects.create_user(
            username=username,
            email=email,
            password=password
        )

        time.sleep(1)

        print("Before creating BusinessProfile")
        print(f"User data: {user}")
        print(f"Gender: {gender}")
        print(f"Business start date: {business_start_date}")
        print(f"Final location data: {location_info}")
        print(f"Formatted location values: {location_values}")

        BusinessProfile.objects.create(
            user=user,
            business_name=business_name,
            business_address='None',  
            industry=industry,
            website=business_website,
            gender=gender,
            business_start_date=business_start_date,
            store_type=store_type,
            submission_group='BusinessGroup',
            
            # Location fields - using location_info directly for accuracy
            latitude=location_info.get('latitude', ''),
            longitude=location_info.get('longitude', ''),
            country=location_info.get('country', ''),
            state=location_info.get('region', ''),
            city=location_info.get('city', ''),
            postal_code=location_info.get('postal_code', ''),
        )

        # Create UserSession
        UserSession.objects.create(
            user=user,
            ip_address=location_info.get('ip_address', ip_address),
            browser=device_info['browser'],
            device_type=device_info['device_type'],
            os=device_info['operating_system']
        )

        print("✅ Business registration completed successfully")
        messages.success(request, "Registration successful!")
        return redirect('accounts:thankyoupage')

    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        print(f"❌ Error type: {type(e)}")
        print(f"❌ Error message: {str(e)}")
        messages.error(request, "An error occurred during registration. Please try again.")
        return redirect('accounts:register')




def user(request):
    location_values = {}
           
    if request.method != 'POST':
        return render(request, 'register.html')
    
    try:
        # Get user's IP address from request headers
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        ip_address = x_forwarded_for.split(',')[0] if x_forwarded_for else request.META.get('REMOTE_ADDR')

        # Get device information from user agent
        ua_string = request.META.get('HTTP_USER_AGENT', '')
        user_agent = parse(ua_string)
        device_info = {
            'browser': f"{user_agent.browser.family} {user_agent.browser.version_string}",
            'device_type': 'Mobile' if user_agent.is_mobile else ('Tablet' if user_agent.is_tablet else 'Desktop'),
            'operating_system': f"{user_agent.os.family} {user_agent.os.version_string}"
        }

        # Priority 1: Try to get location data from frontend form submission
        frontend_ip = request.POST.get('user_ip', '').strip()
        frontend_city = request.POST.get('user_city', '').strip()
        frontend_region = request.POST.get('user_region', '').strip()
        frontend_country = request.POST.get('user_country', '').strip()
        frontend_postal = request.POST.get('user_postal', '').strip()
        frontend_lat = request.POST.get('user_latitude', '').strip()
        frontend_lng = request.POST.get('user_longitude', '').strip()
        frontend_timezone = request.POST.get('user_timezone', '').strip()

        print("Frontend location data received:")
        print(f"IP: {frontend_ip}")
        print(f"City: {frontend_city}")
        print(f"Region: {frontend_region}")
        print(f"Country: {frontend_country}")
        print(f"Postal: {frontend_postal}")
        print(f"Lat: {frontend_lat}")
        print(f"Lng: {frontend_lng}")
        print(f"Timezone: {frontend_timezone}")

        # Use frontend data if available, otherwise fallback to API call
        if frontend_ip and (frontend_city or frontend_region or frontend_country):
            # Use frontend location data
            location_info = {
                'ip_address': frontend_ip,
                'city': frontend_city,
                'region': frontend_region,
                'country': frontend_country,
                'postal_code': frontend_postal,
                'latitude': frontend_lat,
                'longitude': frontend_lng,
                'timezone': frontend_timezone
            }
            print("✅ Using frontend location data")
        else:
            # Priority 2: Fallback to API call with actual user IP
            print("❌ Frontend data incomplete, falling back to API call")
            try:
                # Use the actual user's IP address instead of hardcoded one
                api_url = f"https://ipgeolocation.abstractapi.com/v1/?api_key=69f8a774e279408e8fa7f7d0ed6937d6&ip_address={ip_address}"
                print(f"Making API call to: {api_url}")
                response = requests.get(api_url)
                
                if response.status_code == 200:
                    data = response.json()
                    location_info = {
                        'ip_address': ip_address,
                        'city': data.get('city', ''),
                        'region': data.get('region_iso_code', ''),
                        'country': data.get('country_code', ''),
                        'postal_code': data.get('postal_code', ''),
                        'latitude': data.get('latitude', ''),
                        'longitude': data.get('longitude', ''),
                        'timezone': data.get('timezone', {}).get('name', '')
                    }
                    print("✅ Using API location data")
                else:
                    raise Exception(f"API request failed: {response.status_code}")
            except Exception as e:
                print(f"❌ API call failed: {str(e)}")
                logger.error(f"Error getting location data: {str(e)}")
                location_info = {
                    'ip_address': ip_address,
                    'city': '',
                    'region': '',
                    'country': '',
                    'postal_code': '',
                    'latitude': '',
                    'longitude': '',
                    'timezone': ''
                }
                print("⚠️ Using default empty location data")

        # Format location_values for backward compatibility
        for key, value in location_info.items():
            formatted_key = key.replace('_', ' ').title()
            location_values[formatted_key] = value

        # Get user registration data
        username = request.POST['username']
        email = request.POST['email']
        password = request.POST['password']
        gender = request.POST['Gender']
        Birthdate_One = request.POST['Birthdate_One']
        Birthdate_Two = request.POST['Birthdate_Two']
        Birthdate_Three = request.POST['Birthdate_Three']
        birthdate = f"{Birthdate_One}-{Birthdate_Two.zfill(2)}-{Birthdate_Three.zfill(2)}"

        # Validate existing users
        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already exists. Please choose a different username.")
            return render(request, 'register.html')
            
        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already exists. Please choose a different email.")
            return render(request, 'register.html')

        # Create user
        user = User.objects.create_user(
            username=username,
            email=email,
            password=password
        )

        time.sleep(1)

        print("Before creating UserProfile")
        print(f"User data: {user}")
        print(f"Gender: {gender}")
        print(f"Birthdate: {birthdate}")
        print(f"Final location data: {location_info}")
        print(f"Formatted location values: {location_values}")

        # Create UserProfile with location data
        UserProfile.objects.create(
            user=user,
            gender=gender,
            birthdate=birthdate,
            # Using location_info directly for accuracy
            country=location_info.get('country', ''),
            state=location_info.get('region', ''),
            city=location_info.get('city', ''),
            postal_code=location_info.get('postal_code', ''),
            submission_group='UserGroup'
        )

        # Create user session
        UserSession.objects.create(
            user=user,
            ip_address=location_info.get('ip_address', ip_address),
            browser=device_info['browser'],
            device_type=device_info['device_type'],
            os=device_info['operating_system']
        )

        print("✅ User registration completed successfully")
        messages.success(request, "Registration successful!")
        return redirect('accounts:thankyoupage')

    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        print(f"❌ Error type: {type(e)}")
        print(f"❌ Error message: {str(e)}")
        messages.error(request, "An error occurred during registration. Please try again.")
        return redirect('accounts:register')
    
def thankyoupage(request):
    return render(request, 'thankyou.html')




def check_email(request):
   

    email = request.POST.get('email')
    
    # Regex pattern for validating an email
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    
    # Check if the email format is valid
    if not re.match(email_pattern, email):
        return HttpResponse("Awaiting correct email format.")
    
    # Check if the email already exists in the database
    if get_user_model().objects.filter(email=email).exists():
        return HttpResponse("This email already exists.")
    else:
        return HttpResponse("This email is available.") 
    



def check_password(request):
    if request.method == 'POST':
            password_one = request.POST.get('password_one')
            password_two= request.POST.get('password_two')

            if password_one and password_two:
                if password_one == password_two:
                    return HttpResponse("Passwords match.")
                else:
                    return HttpResponse("Passwords do not match.")
            else:
                return HttpResponse("Please fill both password fields.")
    return HttpResponse("Invalid request", status=400)




