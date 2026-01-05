"""
Analytics Test Script
Run with: python searchengine/tests.py
"""

import os
import sys
from decouple import config


# # Add project root to path
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, project_root)

# # Set Django settings - CHANGE THIS TO YOUR ACTUAL SETTINGS MODULE
# # Common options: 'config.settings', 'core.settings', 'afrotodosearch.settings', 'searchengine.settings'
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')  # ← CHANGE THIS

# import django
# django.setup()

# # Now run the test
# from datetime import datetime
# from searchengine.redis_analytics import SearchAnalytics

# def test_analytics():
#     print("=" * 60)
#     print("ANALYTICS TEST")
#     print("=" * 60)
    
#     # Create analytics instance
#     print("\n1. Creating SearchAnalytics instance...")
#     try:
#         analytics = SearchAnalytics()
#         print("   SUCCESS: SearchAnalytics created")
#     except Exception as e:
#         print(f"   FAILED: {e}")
#         return
    
#     # Test Redis connection
#     print("\n2. Testing Redis connection...")
#     try:
#         ping = analytics.redis.ping()
#         print(f"   SUCCESS: Redis ping = {ping}")
#     except Exception as e:
#         print(f"   FAILED: {e}")
#         return
    
#     # Test track_search
#     print("\n3. Testing track_search...")
#     test_session = f"test-{datetime.now().strftime('%H%M%S')}"
#     try:
#         result = analytics.track_search(
#             session_id=test_session,
#             query="test query from script",
#             results_count=25,
#             alt_mode='n'
#         )
#         print(f"   SUCCESS: Tracked query '{result.get('query')}'")
#     except Exception as e:
#         print(f"   FAILED: {e}")
#         import traceback
#         traceback.print_exc()
#         return
    
#     # Verify search saved
#     print("\n4. Verifying search was saved...")
#     searches = analytics.redis.lrange(f"session:{test_session}:searches", 0, -1)
#     print(f"   Searches in session: {len(searches)}")
#     if searches:
#         print("   SUCCESS: Search saved to Redis!")
#     else:
#         print("   FAILED: Search NOT saved!")
    
#     # Check popular queries
#     popular = analytics.redis.zrevrange("analytics:popular:queries", 0, 5, withscores=True)
#     print(f"   Popular queries: {popular[:3]}...")
    
#     # Test track_click
#     print("\n5. Testing track_click...")
#     try:
#         analytics.track_click(
#             session_id=test_session,
#             query="test query from script",
#             clicked_url="https://example.com/test-article",
#             clicked_position=1
#         )
#         print("   SUCCESS: Click tracked")
#     except Exception as e:
#         print(f"   FAILED: {e}")
#         import traceback
#         traceback.print_exc()
#         return
    
#     # Verify click saved
#     print("\n6. Verifying click was saved...")
#     clicks = analytics.redis.lrange(f"session:{test_session}:clicks", 0, -1)
#     print(f"   Clicks in session: {len(clicks)}")
#     if clicks:
#         print("   SUCCESS: Click saved to Redis!")
#     else:
#         print("   FAILED: Click NOT saved!")
    
#     # Summary
#     print("\n" + "=" * 60)
#     print("SUMMARY")
#     print("=" * 60)
#     if searches and clicks:
#         print("SUCCESS: Analytics is working!")
#         print(f"Test session: {test_session}")
#     else:
#         print("FAILED: Check the errors above")
#     print("=" * 60)


# if __name__ == "__main__":
#     test_analytics()

# Add project root to path




### Testing the full system ### This works below


# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

import django
django.setup()

from datetime import datetime
from searchengine.redis_analytics import SearchAnalytics


def test_analytics_direct():
    """Test analytics module directly."""
    print("=" * 60)
    print("TEST 1: ANALYTICS MODULE (DIRECT)")
    print("=" * 60)
    
    print("\n1. Creating SearchAnalytics instance...")
    try:
        analytics = SearchAnalytics()
        print("   ✓ SUCCESS: SearchAnalytics created")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        return False
    
    print("\n2. Testing Redis connection...")
    try:
        ping = analytics.redis.ping()
        print(f"   ✓ SUCCESS: Redis ping = {ping}")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        return False
    
    print("\n3. Testing track_search...")
    test_session = f"test-direct-{datetime.now().strftime('%H%M%S')}"
    try:
        result = analytics.track_search(
            session_id=test_session,
            query="direct test query",
            results_count=25,
            alt_mode='n'
        )
        print(f"   ✓ SUCCESS: Tracked query '{result.get('query')}'")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    searches = analytics.redis.lrange(f"session:{test_session}:searches", 0, -1)
    print(f"   Searches saved: {len(searches)}")
    
    print("\n4. Testing track_click...")
    try:
        analytics.track_click(
            session_id=test_session,
            query="direct test query",
            clicked_url="https://example.com/test",
            clicked_position=1
        )
        print("   ✓ SUCCESS: Click tracked")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        return False
    
    clicks = analytics.redis.lrange(f"session:{test_session}:clicks", 0, -1)
    print(f"   Clicks saved: {len(clicks)}")
    
    return len(searches) > 0 and len(clicks) > 0


def test_views_analytics():
    """Test analytics as called from views.py."""
    print("\n" + "=" * 60)
    print("TEST 2: VIEWS.PY ANALYTICS FUNCTIONS")
    print("=" * 60)
    
    print("\n1. Checking ANALYTICS_AVAILABLE...")
    try:
        from searchengine.views import ANALYTICS_AVAILABLE
        print(f"   ANALYTICS_AVAILABLE = {ANALYTICS_AVAILABLE}")
        if not ANALYTICS_AVAILABLE:
            print("   ✗ PROBLEM: Analytics is disabled!")
            return False
        print("   ✓ SUCCESS: Analytics is enabled")
    except ImportError as e:
        print(f"   ✗ FAILED to import: {e}")
        return False
    
    print("\n2. Checking get_analytics function...")
    try:
        from searchengine.views import get_analytics
        print(f"   ✓ SUCCESS: get_analytics function found")
    except ImportError as e:
        print(f"   ✗ FAILED: get_analytics not found in views.py!")
        print(f"   Error: {e}")
        return False
    
    print("\n3. Calling get_analytics()...")
    try:
        analytics = get_analytics()
        print(f"   get_analytics() returned: {analytics}")
        print(f"   Type: {type(analytics)}")
        
        if analytics is None:
            print("   ✗ PROBLEM: get_analytics() returned None!")
            return False
        else:
            print("   ✓ SUCCESS: get_analytics() returned an instance")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n4. Testing track_search through views' get_analytics()...")
    test_session = f"test-views-{datetime.now().strftime('%H%M%S')}"
    try:
        analytics.track_search(
            session_id=test_session,
            query="views test query",
            results_count=15,
            alt_mode='n'
        )
        print("   ✓ SUCCESS: track_search completed")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    searches = analytics.redis.lrange(f"session:{test_session}:searches", 0, -1)
    print(f"   Searches saved: {len(searches)}")
    
    if len(searches) > 0:
        print("   ✓ SUCCESS: Search tracked through views!")
        return True
    else:
        print("   ✗ FAILED: Search NOT saved!")
        return False


def test_views_track_click():
    """Test the track_click view function."""
    print("\n" + "=" * 60)
    print("TEST 3: VIEWS.PY track_click ENDPOINT")
    print("=" * 60)
    
    print("\n1. Checking track_click function...")
    try:
        from searchengine.views import track_click
        print(f"   ✓ SUCCESS: track_click function found")
        
        if hasattr(track_click, 'csrf_exempt'):
            print("   ✓ Has csrf_exempt attribute")
        else:
            print(f"   Function: {track_click}")
            print(f"   Name: {track_click.__name__ if hasattr(track_click, '__name__') else 'N/A'}")
    except ImportError as e:
        print(f"   ✗ FAILED: {e}")
        return False
    
    print("\n2. Simulating track_click POST request...")
    from django.test import RequestFactory
    import json
    
    factory = RequestFactory()
    
    test_data = {
        'session_id': f'test-endpoint-{datetime.now().strftime("%H%M%S")}',
        'query': 'endpoint test query',
        'url': 'https://example.com/clicked',
        'position': 2,
        'result_id': 'test-doc-123',
        'title': 'Test Result Title',
        'results_count': 10,
        'was_corrected': 'false'
    }
    
    request = factory.post(
        '/api/track-click/',
        data=json.dumps(test_data),
        content_type='application/json'
    )
    
    try:
        response = track_click(request)
        print(f"   Response status: {response.status_code}")
        print(f"   Response content: {response.content.decode()}")
        
        if response.status_code == 200:
            print("   ✓ SUCCESS: track_click endpoint works!")
            
            from searchengine.views import get_analytics
            analytics = get_analytics()
            if analytics:
                clicks = analytics.redis.lrange(f"session:{test_data['session_id']}:clicks", 0, -1)
                print(f"   Clicks saved: {len(clicks)}")
            return True
        else:
            print(f"   ✗ FAILED: Status code {response.status_code}")
            return False
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_actual_search_view():
    """Simulate an actual search request to the search view."""
    print("\n" + "=" * 60)
    print("TEST 4: SIMULATE ACTUAL SEARCH REQUEST")
    print("=" * 60)
    
    from django.test import RequestFactory, Client
    from searchengine.views import search, get_analytics
    
    # Create a unique session ID for this test
    test_session = f"test-search-view-{datetime.now().strftime('%H%M%S')}"
    
    print(f"\n1. Creating test request with session_id: {test_session}")
    
    factory = RequestFactory()
    request = factory.get('/search/', {
        'query': 'test search view query',
        'session_id': test_session,
        'alt_mode': 'n',
        'device_type': 'desktop'
    })
    
    # Add session support
    from django.contrib.sessions.middleware import SessionMiddleware
    from django.contrib.auth.models import AnonymousUser
    
    middleware = SessionMiddleware(lambda x: None)
    middleware.process_request(request)
    request.session.save()
    request.user = AnonymousUser()
    
    print("\n2. Calling search view...")
    try:
        response = search(request)
        print(f"   Response status: {response.status_code}")
        print("   ✓ Search view executed")
    except Exception as e:
        print(f"   ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n3. Checking if search was tracked in Redis...")
    analytics = get_analytics()
    if analytics:
        # Check session searches
        searches = analytics.redis.lrange(f"session:{test_session}:searches", 0, -1)
        print(f"   Searches in session '{test_session}': {len(searches)}")
        
        if len(searches) > 0:
            print("   ✓ SUCCESS: Search was tracked!")
            import json
            search_data = json.loads(searches[0])
            print(f"   Query tracked: {search_data.get('query')}")
            return True
        else:
            print("   ✗ FAILED: Search was NOT tracked!")
            print("\n   This means track_search() is not being called in the search view.")
            print("   Check that the analytics tracking code is in the search() function.")
            return False
    else:
        print("   ✗ FAILED: get_analytics() returned None")
        return False


def check_duplicate_functions():
    """Check for duplicate function definitions in views.py."""
    print("\n" + "=" * 60)
    print("TEST 5: CHECK FOR DUPLICATE FUNCTIONS")
    print("=" * 60)
    
    import inspect
    from searchengine import views
    
    views_file = inspect.getfile(views)
    print(f"\nViews file: {views_file}")
    
    with open(views_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    functions_to_check = ['def track_click', 'def click_redirect', 'def get_analytics']
    
    for func in functions_to_check:
        count = content.count(func)
        if count > 1:
            print(f"   ⚠ WARNING: '{func}' defined {count} times!")
        elif count == 1:
            print(f"   ✓ '{func}' defined once")
        else:
            print(f"   ✗ '{func}' NOT FOUND!")
    
    if '@csrf_exempt' in content:
        csrf_before_track = content[max(0, content.find('def track_click')-200):content.find('def track_click')]
        if '@csrf_exempt' in csrf_before_track:
            print("   ✓ @csrf_exempt appears before track_click")
        else:
            print("   ⚠ WARNING: @csrf_exempt may not be applied to track_click!")
    else:
        print("   ✗ @csrf_exempt NOT FOUND in views.py!")


def check_search_view_analytics_code():
    """Check if analytics tracking code exists in search view."""
    print("\n" + "=" * 60)
    print("TEST 6: CHECK SEARCH VIEW FOR ANALYTICS CODE")
    print("=" * 60)
    
    import inspect
    from searchengine import views
    
    views_file = inspect.getfile(views)
    
    with open(views_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the search function
    search_start = content.find('def search(request):')
    if search_start == -1:
        print("   ✗ Could not find 'def search(request):' in views.py!")
        return False
    
    # Find the next function definition (end of search function)
    next_def = content.find('\ndef ', search_start + 1)
    if next_def == -1:
        next_def = len(content)
    
    search_function = content[search_start:next_def]
    
    print(f"\n   Search function found ({len(search_function)} characters)")
    
    # Check for key analytics code
    checks = [
        ('analytics = get_analytics()', 'Get analytics instance'),
        ('analytics.track_search(', 'Call track_search'),
        ('analytics.start_session(', 'Call start_session'),
    ]
    
    all_found = True
    for code, description in checks:
        if code in search_function:
            print(f"   ✓ Found: {description}")
        else:
            print(f"   ✗ MISSING: {description}")
            all_found = False
    
    # Check if track_search is inside an if block
    if 'if analytics:' in search_function:
        print("   ✓ Found: 'if analytics:' check")
        
        # Check what's inside the if block
        if_start = search_function.find('if analytics:')
        if_block = search_function[if_start:if_start+500]
        
        if 'track_search' in if_block:
            print("   ✓ track_search is inside 'if analytics:' block")
        else:
            print("   ⚠ track_search may not be inside 'if analytics:' block")
    
    # Check for try/except that might hide errors
    if 'except Exception as e:' in search_function:
        print("   ⚠ Found 'except Exception' - errors might be silently caught")
    
    return all_found


def main():
    print("\n" + "=" * 60)
    print("COMPREHENSIVE ANALYTICS TEST")
    print("=" * 60)
    
    results = {}
    
    # Test 1: Direct analytics
    results['direct'] = test_analytics_direct()
    
    # Test 2: Views analytics
    results['views'] = test_views_analytics()
    
    # Test 3: Track click endpoint
    results['endpoint'] = test_views_track_click()
    
    # Test 4: Actual search view simulation
    results['search_view'] = test_actual_search_view()
    
    # Test 5: Check for duplicates
    check_duplicate_functions()
    
    # Test 6: Check search view code
    check_search_view_analytics_code()
    
    # Summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {test_name}: {status}")
    
    if all(results.values()):
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed. Check the output above for details.")
    
    print("=" * 60)


if __name__ == "__main__":
    main()


# import os
# import sys

# project_root = os.path.dirname(os.path.abspath(__file__))
# sys.path.insert(0, project_root)
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

# # # Add project root to path
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, project_root)

# # # Set Django settings
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')

# # import django
# # django.setup()

# # from datetime import datetime

# import django
# django.setup()

# from searchengine.geolocation import get_location_from_ip

# # Test with known public IPs
# test_ips = [
#     ("8.8.8.8", "Google DNS - Mountain View, CA"),
#     ("1.1.1.1", "Cloudflare - San Francisco, CA"),
#     ("208.67.222.222", "OpenDNS - San Francisco, CA"),
# ]

# for ip, description in test_ips:
#     print(f"\nTesting {ip} ({description}):")
#     location = get_location_from_ip(ip)
#     print(f"  City: {location.get('city')}")
#     print(f"  Country: {location.get('country')}")