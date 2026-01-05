
"""
redis_analytics.py

Comprehensive analytics tracking for search behavior, sessions, and user actions.

TRACKS:
- Search queries (with alt_mode comparison: dropdown vs semantic)
- User sessions (duration, activity, engagement)
- Click events (what users click after searching)
- Zero-result searches (content gaps)
- Location data (geographic insights)
- Logged-in vs anonymous behavior
- Query patterns and trends

REDIS STRUCTURE:
- session:{session_id}           → Hash: session metadata
- session:{session_id}:searches  → List: all searches in session
- session:{session_id}:clicks    → List: all clicks in session
- session:{session_id}:events    → List: all events timeline
- analytics:searches:daily:{date} → Sorted Set: query frequencies
- analytics:searches:zero_results → Sorted Set: failed queries
- analytics:alt_mode:y           → Sorted Set: dropdown searches
- analytics:alt_mode:n           → Sorted Set: semantic searches
- analytics:locations            → Sorted Set: search by location
- analytics:users:{user_id}      → Hash: user-level stats
- analytics:popular:queries      → Sorted Set: all-time popular
- analytics:popular:clicks       → Sorted Set: most clicked results

USAGE:
    from redis_analytics import SearchAnalytics
    
    analytics = SearchAnalytics()
    
    # Track a search
    analytics.track_search(
        session_id="abc123",
        query="black hair care",
        results_count=25,
        alt_mode="n",
        user_id=None,  # or user ID if logged in
        location={"city": "Atlanta", "country": "US"},
        device_type="mobile",
        search_time_ms=145
    )
    
    # Track a click
    analytics.track_click(
        session_id="abc123",
        query="black hair care",
        clicked_url="https://example.com/article",
        clicked_position=2,
        result_id="doc_uuid_here"
    )
    
    # Get insights
    analytics.get_popular_queries(days=7, limit=100)
    analytics.get_zero_result_queries(limit=50)
    analytics.compare_alt_modes()
"""

# import redis
# import json
# import time
# from datetime import datetime, timedelta
# from typing import Dict, List, Optional, Any, Tuple
# from decouple import config
# import hashlib


# # ============================================================================
# # CONFIGURATION
# # ============================================================================

# REDIS_ANALYTICS_URL = config('REDIS_ANALYTICS_URL', default='redis://localhost:6379/2')
# SESSION_EXPIRE_HOURS = config('SESSION_EXPIRE_HOURS', default=24, cast=int)
# ANALYTICS_RETENTION_DAYS = config('ANALYTICS_RETENTION_DAYS', default=90, cast=int)


# # ============================================================================
# # REDIS CONNECTION
# # ============================================================================

# _redis_client = None


# def get_redis_client():
#     """Get or create Redis connection for analytics."""
#     global _redis_client
#     if _redis_client is None:
#         _redis_client = redis.from_url(
#             REDIS_ANALYTICS_URL,
#             decode_responses=True,
#             socket_connect_timeout=5,
#             socket_timeout=5
#         )
#     return _redis_client


# # ============================================================================
# # MAIN ANALYTICS CLASS
# # ============================================================================

# class SearchAnalytics:
#     """
#     Comprehensive search analytics tracking.
    
#     Tracks user sessions, searches, clicks, and generates insights
#     for understanding user behavior and improving search quality.
#     """
    
#     def __init__(self, redis_client=None):
#         """Initialize with optional custom Redis client."""
#         self.redis = redis_client or get_redis_client()
#         self.session_expire_seconds = SESSION_EXPIRE_HOURS * 3600

#     def _get_time_bucket(self, time_ms: int) -> str:
#         """
#         Convert time-to-click milliseconds into a bucket for aggregation.
        
#         Buckets:
#         - instant: < 2 seconds
#         - fast: 2-5 seconds
#         - normal: 5-15 seconds
#         - slow: 15-30 seconds
#         - very_slow: > 30 seconds
#         """
#         if time_ms < 2000:
#             return 'instant'
#         elif time_ms < 5000:
#             return 'fast'
#         elif time_ms < 15000:
#             return 'normal'
#         elif time_ms < 30000:
#             return 'slow'
#         else:
#             return 'very_slow'
        
#     # ========================================================================
#     # SESSION MANAGEMENT
#     # ========================================================================
#     def start_session(
#         self,
#         session_id: str,
#         user_id: Optional[str] = None,
#         device_type: Optional[str] = None,
#         user_agent: Optional[str] = None,
#         ip_address: Optional[str] = None,
#         location: Optional[Dict] = None,
#         referrer: Optional[str] = None,
#         # NEW: Device/browser fields
#         browser: Optional[str] = None,
#         browser_version: Optional[str] = None,
#         os_name: Optional[str] = None,
#         os_version: Optional[str] = None,
#         is_mobile: bool = False,
#         is_bot: bool = False
#         ) -> Dict:
#         """
#         Initialize or update a user session.
        
#         Call this when a user first visits or on each page load to keep session alive.
#         """
#         session_key = f"session:{session_id}"
#         now = time.time()
#         now_iso = datetime.utcnow().isoformat()
        
#         # Check if session exists
#         existing = self.redis.hgetall(session_key)
        
#         if existing:
#             # Update existing session
#             self.redis.hset(session_key, mapping={
#                 'last_active': now_iso,
#                 'last_active_ts': now,
#                 'page_views': int(existing.get('page_views', 0)) + 1
#             })
            
#             # Update user_id if they just logged in
#             if user_id and not existing.get('user_id'):
#                 self.redis.hset(session_key, 'user_id', user_id)
#                 self._link_session_to_user(session_id, user_id)
#         else:
#             # Create new session
#             session_data = {
#                 'session_id': session_id,
#                 'created_at': now_iso,
#                 'created_ts': now,
#                 'last_active': now_iso,
#                 'last_active_ts': now,
#                 'user_id': user_id or '',
#                 'device_type': device_type or 'unknown',
#                 'user_agent': user_agent or '',
#                 'ip_address': ip_address or '',
#                 # Location fields
#                 'location_city': location.get('city', '') if location else '',
#                 'location_region': location.get('region', '') if location else '',
#                 'location_country': location.get('country', '') if location else '',
#                 'location_country_code': location.get('country_code', '') if location else '',
#                 'location_lat': location.get('lat', '') if location else '',
#                 'location_lng': location.get('lng', '') if location else '',
#                 'location_timezone': location.get('timezone', '') if location else '',
#                 # NEW: Browser/Device fields
#                 'browser': browser or 'Unknown',
#                 'browser_version': browser_version or '',
#                 'os_name': os_name or 'Unknown',
#                 'os_version': os_version or '',
#                 'is_mobile': '1' if is_mobile else '0',
#                 'is_bot': '1' if is_bot else '0',
#                 # Other fields
#                 'referrer': referrer or '',
#                 'page_views': 1,
#                 'search_count': 0,
#                 'click_count': 0,
#                 'is_logged_in': '1' if user_id else '0'
#             }
            
#             self.redis.hset(session_key, mapping=session_data)
            
#             # Track daily active session
#             date_key = datetime.utcnow().strftime('%Y-%m-%d')
#             self.redis.sadd(f"analytics:sessions:daily:{date_key}", session_id)
            
#             # Track by location
#             if location and location.get('country'):
#                 self.redis.zincrby("analytics:locations:countries", 1, location['country'])
#             if location and location.get('city'):
#                 city_key = f"{location.get('city')}, {location.get('country_code', location.get('country', ''))}"
#                 self.redis.zincrby("analytics:locations:cities", 1, city_key)
            
#             # Track by device type
#             if device_type:
#                 self.redis.zincrby(f"analytics:devices:{date_key}", 1, device_type)
#                 self.redis.expire(f"analytics:devices:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # Track by browser
#             if browser:
#                 self.redis.zincrby(f"analytics:browsers:{date_key}", 1, browser)
#                 self.redis.expire(f"analytics:browsers:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # Track by OS
#             if os_name:
#                 self.redis.zincrby(f"analytics:os:{date_key}", 1, os_name)
#                 self.redis.expire(f"analytics:os:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # Track bot vs human
#             if is_bot:
#                 self.redis.hincrby(f"analytics:daily:{date_key}", 'bot_sessions', 1)
            
#             # Link to user if logged in
#             if user_id:
#                 self._link_session_to_user(session_id, user_id)
        
#         # Set/refresh expiration
#         self.redis.expire(session_key, self.session_expire_seconds)
        
#         return self.get_session(session_id)
    
#     def get_session(self, session_id: str) -> Optional[Dict]:
#         """Get session data."""
#         session_key = f"session:{session_id}"
#         data = self.redis.hgetall(session_key)
        
#         if not data:
#             return None
        
#         # Calculate session duration
#         created_ts = float(data.get('created_ts', 0))
#         last_active_ts = float(data.get('last_active_ts', 0))
        
#         data['duration_seconds'] = int(last_active_ts - created_ts)
#         data['duration_minutes'] = round((last_active_ts - created_ts) / 60, 1)
        
#         return data
    
#     def end_session(self, session_id: str) -> Optional[Dict]:
#         """
#         Explicitly end a session (e.g., user logs out).
        
#         Calculates final stats and archives session data.
#         """
#         session = self.get_session(session_id)
#         if not session:
#             return None
        
#         # Mark as ended
#         session_key = f"session:{session_id}"
#         self.redis.hset(session_key, mapping={
#             'ended_at': datetime.utcnow().isoformat(),
#             'ended_ts': time.time(),
#             'status': 'ended'
#         })
        
#         # Archive session stats
#         self._archive_session_stats(session_id, session)
        
#         return self.get_session(session_id)
    
#     def _link_session_to_user(self, session_id: str, user_id: str):
#         """Link session to user for cross-session analysis."""
#         user_sessions_key = f"analytics:user:{user_id}:sessions"
#         self.redis.lpush(user_sessions_key, session_id)
#         self.redis.ltrim(user_sessions_key, 0, 99)  # Keep last 100 sessions
#         self.redis.expire(user_sessions_key, ANALYTICS_RETENTION_DAYS * 86400)
    
#     def _archive_session_stats(self, session_id: str, session: Dict):
#         """Archive session statistics for long-term analysis."""
#         date_key = datetime.utcnow().strftime('%Y-%m-%d')
        
#         # Aggregate daily stats
#         pipe = self.redis.pipeline()
        
#         duration = int(session.get('duration_seconds', 0))
#         searches = int(session.get('search_count', 0))
#         clicks = int(session.get('click_count', 0))
        
#         pipe.hincrby(f"analytics:daily:{date_key}", 'total_sessions', 1)
#         pipe.hincrby(f"analytics:daily:{date_key}", 'total_duration_seconds', duration)
#         pipe.hincrby(f"analytics:daily:{date_key}", 'total_searches', searches)
#         pipe.hincrby(f"analytics:daily:{date_key}", 'total_clicks', clicks)
        
#         if session.get('is_logged_in') == '1':
#             pipe.hincrby(f"analytics:daily:{date_key}", 'logged_in_sessions', 1)
#         else:
#             pipe.hincrby(f"analytics:daily:{date_key}", 'anonymous_sessions', 1)
        
#         pipe.expire(f"analytics:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
#         pipe.execute()
    
#     # ========================================================================
#     # SEARCH TRACKING
#     # ========================================================================
#     def track_search(
#         self,
#         session_id: str,
#         query: str,
#         results_count: int,
#         alt_mode: str = 'n',
#         user_id: Optional[str] = None,
#         location: Optional[Dict] = None,
#         device_type: Optional[str] = None,
#         search_time_ms: Optional[float] = None,
#         search_strategy: Optional[str] = None,
#         corrected_query: Optional[str] = None,
#         filters_applied: Optional[Dict] = None,
#         page: int = 1,
#         intent: Optional[str] = None,
#         request_id: Optional[str] = None,
#         # NEW: Device/browser fields
#         browser: Optional[str] = None,
#         browser_version: Optional[str] = None,
#         os_name: Optional[str] = None,
#         os_version: Optional[str] = None,
#         is_mobile: bool = False,
#         is_bot: bool = False
#     ) -> Dict:
#         """
#         Track a search event with full context.
        
#         Args:
#             session_id: Browser session ID
#             query: The search query
#             results_count: Number of results returned
#             alt_mode: 'y' for dropdown/strict, 'n' for semantic
#             user_id: User ID if logged in
#             location: {city, region, country, lat, lng}
#             device_type: desktop/mobile/tablet
#             search_time_ms: How long the search took
#             search_strategy: semantic/strict/mixed/text_fallback
#             corrected_query: Spell-corrected version
#             filters_applied: Any filters used
#             page: Results page number
#             intent: Detected intent (location, person, etc.)
#             request_id: Unique request ID for this search
#             browser: Browser name (Chrome, Firefox, etc.)
#             browser_version: Browser version
#             os_name: Operating system name
#             os_version: OS version
#             is_mobile: Whether device is mobile
#             is_bot: Whether request is from a bot
            
#         Returns:
#             Search event data
#         """
#         now = time.time()
#         now_iso = datetime.utcnow().isoformat()
#         date_key = datetime.utcnow().strftime('%Y-%m-%d')
#         hour_key = datetime.utcnow().strftime('%Y-%m-%d:%H')
        
#         # Normalize query for aggregation
#         query_normalized = query.lower().strip()
#         query_hash = hashlib.md5(query_normalized.encode()).hexdigest()[:12]
        
#         # Build search event
#         search_event = {
#             'event_type': 'search',
#             'timestamp': now_iso,
#             'timestamp_ts': now,
#             'session_id': session_id,
#             'request_id': request_id or f"{session_id}:{now}",
#             'query': query,
#             'query_normalized': query_normalized,
#             'query_hash': query_hash,
#             'query_length': len(query),
#             'query_word_count': len(query.split()),
#             'results_count': results_count,
#             'has_results': results_count > 0,
#             'alt_mode': alt_mode,
#             'user_id': user_id or '',
#             'is_logged_in': bool(user_id),
#             'device_type': device_type or 'unknown',
#             'browser': browser or 'Unknown',
#             'browser_version': browser_version or '',
#             'os_name': os_name or 'Unknown',
#             'os_version': os_version or '',
#             'is_mobile': is_mobile,
#             'is_bot': is_bot,
#             'search_time_ms': search_time_ms or 0,
#             'search_strategy': search_strategy or '',
#             'corrected_query': corrected_query or '',
#             'was_corrected': corrected_query and corrected_query.lower() != query.lower(),
#             'filters_applied': json.dumps(filters_applied) if filters_applied else '{}',
#             'page': page,
#             'intent': intent or 'general',
#             'location_city': location.get('city', '') if location else '',
#             'location_country': location.get('country', '') if location else '',
#             'location_country_code': location.get('country_code', '') if location else ''
#         }
        
#         # Execute all Redis operations in pipeline
#         pipe = self.redis.pipeline()
        
#         # 1. Add to session's search history
#         session_searches_key = f"session:{session_id}:searches"
#         pipe.lpush(session_searches_key, json.dumps(search_event))
#         pipe.ltrim(session_searches_key, 0, 499)  # Keep last 500 searches per session
#         pipe.expire(session_searches_key, self.session_expire_seconds)
        
#         # 2. Add to session's event timeline
#         session_events_key = f"session:{session_id}:events"
#         pipe.lpush(session_events_key, json.dumps(search_event))
#         pipe.ltrim(session_events_key, 0, 999)
#         pipe.expire(session_events_key, self.session_expire_seconds)
        
#         # 3. Update session search count
#         session_key = f"session:{session_id}"
#         pipe.hincrby(session_key, 'search_count', 1)
#         pipe.hset(session_key, 'last_search', now_iso)
#         pipe.hset(session_key, 'last_query', query)
        
#         # 4. Track query popularity (daily)
#         pipe.zincrby(f"analytics:searches:daily:{date_key}", 1, query_normalized)
#         pipe.expire(f"analytics:searches:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 5. Track query popularity (all-time, with decay)
#         pipe.zincrby("analytics:popular:queries", 1, query_normalized)
        
#         # 6. Track by alt_mode for comparison
#         alt_mode_key = f"analytics:alt_mode:{alt_mode}:{date_key}"
#         pipe.zincrby(alt_mode_key, 1, query_normalized)
#         pipe.expire(alt_mode_key, ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 7. Track alt_mode stats
#         pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_count", 1)
#         pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_results", results_count)
#         if search_time_ms:
#             pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_time_ms", int(search_time_ms))
#         pipe.expire(f"analytics:alt_mode_stats:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 8. Track zero-result searches (content gaps!)
#         if results_count == 0:
#             pipe.zincrby("analytics:searches:zero_results", 1, query_normalized)
#             pipe.zincrby(f"analytics:searches:zero_results:{date_key}", 1, query_normalized)
#             pipe.expire(f"analytics:searches:zero_results:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 9. Track by location
#         if location:
#             if location.get('country'):
#                 pipe.zincrby(f"analytics:searches:by_country:{date_key}", 1, location['country'])
#                 pipe.expire(f"analytics:searches:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
#                 pipe.zincrby("analytics:locations:countries", 1, location['country'])
#             if location.get('city'):
#                 city_key = f"{location.get('city')}, {location.get('country_code', location.get('country', ''))}"
#                 pipe.zincrby(f"analytics:searches:by_city:{date_key}", 1, city_key)
#                 pipe.expire(f"analytics:searches:by_city:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
#                 pipe.zincrby("analytics:locations:cities", 1, city_key)
        
#         # 10. Track by intent
#         if intent:
#             pipe.zincrby(f"analytics:intent:{date_key}", 1, intent)
#             pipe.expire(f"analytics:intent:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 11. Track hourly volume (for traffic patterns)
#         pipe.hincrby(f"analytics:hourly:{hour_key}", 'searches', 1)
#         pipe.expire(f"analytics:hourly:{hour_key}", 7 * 86400)  # Keep 7 days of hourly
        
#         # 12. Track search strategy effectiveness
#         if search_strategy:
#             pipe.hincrby(f"analytics:strategy:{date_key}", search_strategy, 1)
#             pipe.hincrby(f"analytics:strategy:{date_key}", f"{search_strategy}_results", results_count)
#             pipe.expire(f"analytics:strategy:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 13. Track logged-in vs anonymous
#         user_type = 'logged_in' if user_id else 'anonymous'
#         pipe.hincrby(f"analytics:user_type:{date_key}", user_type, 1)
#         pipe.expire(f"analytics:user_type:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # 14. If logged in, track user's search history
#         if user_id:
#             user_searches_key = f"analytics:user:{user_id}:searches"
#             pipe.lpush(user_searches_key, json.dumps({
#                 'query': query,
#                 'timestamp': now_iso,
#                 'results_count': results_count
#             }))
#             pipe.ltrim(user_searches_key, 0, 999)  # Keep last 1000 searches per user
#             pipe.expire(user_searches_key, ANALYTICS_RETENTION_DAYS * 86400)
        
#         pipe.execute()
        
#         return search_event
        
#         # ========================================================================
#         # CLICK TRACKING
#         # ========================================================================
        
#     def track_click(
#             self,
#             session_id: str,
#             query: str,
#             clicked_url: str,
#             clicked_position: int,
#             result_id: Optional[str] = None,
#             result_title: Optional[str] = None,
#             result_source: Optional[str] = None,
#             user_id: Optional[str] = None,
#             time_to_click_ms: Optional[int] = None,
#             location: Optional[Dict] = None,
#             search_request_id: Optional[str] = None,
#             results_count: Optional[int] = None,
#             was_corrected: bool = False,
#             corrected_query: Optional[str] = None
#         ) -> Dict:
#             # """
#             # Track when a user clicks on a search result.
            
#             # Args:
#             #     session_id: Browser session ID
#             #     query: The search query that led to this click
#             #     clicked_url: URL that was clicked
#             #     clicked_position: Position in results (1-indexed)
#             #     result_id: Document UUID
#             #     result_title: Title of clicked result
#             #     result_source: Source/brand of result
#             #     user_id: User ID if logged in
#             #     time_to_click_ms: Time from search to click
#             #     location: {city, region, country, lat, lng}
#             #     search_request_id: Request ID from the original search (links click to search)
#             #     results_count: Number of results from the original search
#             #     was_corrected: Whether the query was spell-corrected
#             #     corrected_query: The corrected query if applicable
                
#             # Returns:
#             #     Click event data
#             # """
#             now = time.time()
#             now_iso = datetime.utcnow().isoformat()
#             date_key = datetime.utcnow().strftime('%Y-%m-%d')
            
#             query_normalized = query.lower().strip()
            
#             click_event = {
#                 'event_type': 'click',
#                 'timestamp': now_iso,
#                 'timestamp_ts': now,
#                 'session_id': session_id,
#                 'query': query,
#                 'query_normalized': query_normalized,
#                 'clicked_url': clicked_url,
#                 'clicked_position': clicked_position,
#                 'result_id': result_id or '',
#                 'result_title': result_title or '',
#                 'result_source': result_source or '',
#                 'user_id': user_id or '',
#                 'time_to_click_ms': time_to_click_ms or 0,
#                 'location_city': location.get('city', '') if location else '',
#                 'location_region': location.get('region', '') if location else '',
#                 'location_country': location.get('country', '') if location else '',
#                 'search_request_id': search_request_id or '',
#                 'results_count': results_count or 0,
#                 'was_corrected': was_corrected,
#                 'corrected_query': corrected_query or ''
#             }
            
#             pipe = self.redis.pipeline()
            
#             # 1. Add to session's click history
#             session_clicks_key = f"session:{session_id}:clicks"
#             pipe.lpush(session_clicks_key, json.dumps(click_event))
#             pipe.ltrim(session_clicks_key, 0, 499)
#             pipe.expire(session_clicks_key, self.session_expire_seconds)
            
#             # 2. Add to session's event timeline
#             session_events_key = f"session:{session_id}:events"
#             pipe.lpush(session_events_key, json.dumps(click_event))
#             pipe.ltrim(session_events_key, 0, 999)
            
#             # 3. Update session click count
#             session_key = f"session:{session_id}"
#             pipe.hincrby(session_key, 'click_count', 1)
#             pipe.hset(session_key, 'last_click', now_iso)
            
#             # 4. Track popular clicked URLs
#             pipe.zincrby("analytics:popular:clicks", 1, clicked_url)
#             pipe.zincrby(f"analytics:clicks:daily:{date_key}", 1, clicked_url)
#             pipe.expire(f"analytics:clicks:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 5. Track click position distribution (are users clicking top results?)
#             pipe.hincrby(f"analytics:click_positions:{date_key}", f"position_{clicked_position}", 1)
#             pipe.expire(f"analytics:click_positions:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 6. Track query → click relationship
#             pipe.zincrby(f"analytics:query_clicks:{query_normalized[:50]}", 1, clicked_url)
            
#             # 7. Track click-through rate data
#             pipe.hincrby(f"analytics:ctr:{date_key}", 'total_clicks', 1)
#             pipe.expire(f"analytics:ctr:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 8. Track source/brand clicks
#             if result_source:
#                 pipe.zincrby(f"analytics:source_clicks:{date_key}", 1, result_source)
#                 pipe.expire(f"analytics:source_clicks:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 9. Track time-to-click distribution
#             if time_to_click_ms:
#                 bucket = self._get_time_bucket(time_to_click_ms)
#                 pipe.hincrby(f"analytics:time_to_click:{date_key}", bucket, 1)
#                 pipe.expire(f"analytics:time_to_click:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 10. If logged in, track user clicks
#             if user_id:
#                 user_clicks_key = f"analytics:user:{user_id}:clicks"
#                 pipe.lpush(user_clicks_key, json.dumps({
#                     'query': query,
#                     'url': clicked_url,
#                     'position': clicked_position,
#                     'timestamp': now_iso,
#                     'results_count': results_count or 0,
#                     'was_corrected': was_corrected
#                 }))
#                 pipe.ltrim(user_clicks_key, 0, 499)
#                 pipe.expire(user_clicks_key, ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 11. Track clicks by location
#             if location:
#                 if location.get('country'):
#                     pipe.zincrby(f"analytics:clicks:by_country:{date_key}", 1, location['country'])
#                     pipe.expire(f"analytics:clicks:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
#                 if location.get('city'):
#                     city_key = f"{location.get('city')}, {location.get('country', '')}"
#                     pipe.zincrby(f"analytics:clicks:by_city:{date_key}", 1, city_key)
#                     pipe.expire(f"analytics:clicks:by_city:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 12. Track clicks on corrected vs non-corrected queries
#             correction_status = 'corrected' if was_corrected else 'original'
#             pipe.hincrby(f"analytics:clicks:correction_status:{date_key}", correction_status, 1)
#             pipe.expire(f"analytics:clicks:correction_status:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
#             # 13. Track result_id clicks (which documents get clicked most)
#             if result_id:
#                 pipe.zincrby(f"analytics:document_clicks:{date_key}", 1, result_id)
#                 pipe.expire(f"analytics:document_clicks:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
#                 pipe.zincrby("analytics:popular:documents", 1, result_id)
            
#             pipe.execute()
            
#             return click_event
    
#     # ========================================================================
#     # EVENT TRACKING (Generic)
#     # ========================================================================
        
#     def track_event(
#         self,
#         session_id: str,
#         event_type: str,
#         event_data: Dict,
#         user_id: Optional[str] = None,
#         location: Optional[Dict] = None
#     ) -> Dict:
#         """
#         Track any custom event.
        
#         Use for: page views, filter usage, result expansion, etc.
        
#         Args:
#             session_id: Browser session ID
#             event_type: Type of event (e.g., 'filter_applied', 'result_expanded')
#             event_data: Event-specific data
#             user_id: User ID if logged in
#             location: {city, region, country, lat, lng}
            
#         Returns:
#             Event data with timestamp
#         """
#         now = time.time()
#         now_iso = datetime.utcnow().isoformat()
#         date_key = datetime.utcnow().strftime('%Y-%m-%d')
        
#         event = {
#             'event_type': event_type,
#             'timestamp': now_iso,
#             'timestamp_ts': now,
#             'session_id': session_id,
#             'user_id': user_id or '',
#             'location_city': location.get('city', '') if location else '',
#             'location_region': location.get('region', '') if location else '',
#             'location_country': location.get('country', '') if location else '',
#             **event_data
#         }
        
#         pipe = self.redis.pipeline()
        
#         # Add to session timeline
#         session_events_key = f"session:{session_id}:events"
#         pipe.lpush(session_events_key, json.dumps(event))
#         pipe.ltrim(session_events_key, 0, 999)
#         pipe.expire(session_events_key, self.session_expire_seconds)
        
#         # Track event type counts
#         pipe.hincrby(f"analytics:events:{date_key}", event_type, 1)
#         pipe.expire(f"analytics:events:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # Track events by location
#         if location and location.get('country'):
#             pipe.hincrby(f"analytics:events:by_country:{date_key}", location['country'], 1)
#             pipe.expire(f"analytics:events:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
        
#         # If logged in, track user events
#         if user_id:
#             user_events_key = f"analytics:user:{user_id}:events"
#             pipe.lpush(user_events_key, json.dumps({
#                 'event_type': event_type,
#                 'timestamp': now_iso,
#                 **event_data
#             }))
#             pipe.ltrim(user_events_key, 0, 499)
#             pipe.expire(user_events_key, ANALYTICS_RETENTION_DAYS * 86400)
        
#         pipe.execute()
        
#         return event
    
#     # ========================================================================
#     # ANALYTICS QUERIES - INSIGHTS
#     # ========================================================================
    
#     def get_popular_queries(
#         self,
#         days: int = 7,
#         limit: int = 100
#     ) -> List[Tuple[str, int]]:
#         """
#         Get most popular search queries.
        
#         Args:
#             days: Look back period
#             limit: Number of results
            
#         Returns:
#             List of (query, count) tuples
#         """
#         if days == 0:
#             # All-time
#             results = self.redis.zrevrange("analytics:popular:queries", 0, limit - 1, withscores=True)
#             return [(q, int(s)) for q, s in results]
        
#         # Aggregate from daily keys
#         keys = []
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             keys.append(f"analytics:searches:daily:{date}")
        
#         # Use zunionstore to combine
#         temp_key = f"temp:popular_queries:{time.time()}"
#         if keys:
#             existing_keys = [k for k in keys if self.redis.exists(k)]
#             if existing_keys:
#                 self.redis.zunionstore(temp_key, existing_keys)
#                 results = self.redis.zrevrange(temp_key, 0, limit - 1, withscores=True)
#                 self.redis.delete(temp_key)
#                 return [(q, int(s)) for q, s in results]
        
#         return []
    
#     def get_zero_result_queries(
#         self,
#         days: int = 7,
#         limit: int = 100
#     ) -> List[Tuple[str, int]]:
#         """
#         Get queries that returned zero results (content gaps).
        
#         This is GOLD for content strategy.
        
#         Args:
#             days: Look back period (0 for all-time)
#             limit: Number of results
            
#         Returns:
#             List of (query, count) tuples
#         """
#         if days == 0:
#             results = self.redis.zrevrange("analytics:searches:zero_results", 0, limit - 1, withscores=True)
#             return [(q, int(s)) for q, s in results]
        
#         keys = []
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             keys.append(f"analytics:searches:zero_results:{date}")
        
#         temp_key = f"temp:zero_results:{time.time()}"
#         existing_keys = [k for k in keys if self.redis.exists(k)]
#         if existing_keys:
#             self.redis.zunionstore(temp_key, existing_keys)
#             results = self.redis.zrevrange(temp_key, 0, limit - 1, withscores=True)
#             self.redis.delete(temp_key)
#             return [(q, int(s)) for q, s in results]
        
#         return []
    
#     def compare_alt_modes(self, days: int = 7) -> Dict:
#         """
#         Compare dropdown (alt_mode=y) vs semantic (alt_mode=n) performance.
        
#         Returns:
#             Comparison stats for both modes
#         """
#         stats = {
#             'dropdown': {'searches': 0, 'total_results': 0, 'total_time_ms': 0, 'avg_results': 0, 'avg_time_ms': 0},
#             'semantic': {'searches': 0, 'total_results': 0, 'total_time_ms': 0, 'avg_results': 0, 'avg_time_ms': 0}
#         }
        
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             day_stats = self.redis.hgetall(f"analytics:alt_mode_stats:{date}")
            
#             if day_stats:
#                 # Dropdown (alt_mode=y)
#                 stats['dropdown']['searches'] += int(day_stats.get('mode_y_count', 0))
#                 stats['dropdown']['total_results'] += int(day_stats.get('mode_y_results', 0))
#                 stats['dropdown']['total_time_ms'] += int(day_stats.get('mode_y_time_ms', 0))
                
#                 # Semantic (alt_mode=n)
#                 stats['semantic']['searches'] += int(day_stats.get('mode_n_count', 0))
#                 stats['semantic']['total_results'] += int(day_stats.get('mode_n_results', 0))
#                 stats['semantic']['total_time_ms'] += int(day_stats.get('mode_n_time_ms', 0))
        
#         # Calculate averages
#         for mode in ['dropdown', 'semantic']:
#             if stats[mode]['searches'] > 0:
#                 stats[mode]['avg_results'] = round(stats[mode]['total_results'] / stats[mode]['searches'], 2)
#                 stats[mode]['avg_time_ms'] = round(stats[mode]['total_time_ms'] / stats[mode]['searches'], 2)
        
#         return stats
    
#     def get_session_stats(self, session_id: str) -> Dict:
#         """
#         Get comprehensive stats for a session.
        
#         Returns:
#             Session data with searches, clicks, duration, etc.
#         """
#         session = self.get_session(session_id)
#         if not session:
#             return {}
        
#         # Get searches
#         searches_raw = self.redis.lrange(f"session:{session_id}:searches", 0, -1)
#         searches = [json.loads(s) for s in searches_raw]
        
#         # Get clicks
#         clicks_raw = self.redis.lrange(f"session:{session_id}:clicks", 0, -1)
#         clicks = [json.loads(c) for c in clicks_raw]
        
#         # Get all events
#         events_raw = self.redis.lrange(f"session:{session_id}:events", 0, -1)
#         events = [json.loads(e) for e in events_raw]
        
#         # Calculate click-through rate
#         ctr = len(clicks) / len(searches) if searches else 0
        
#         return {
#             'session': session,
#             'searches': searches,
#             'clicks': clicks,
#             'events': events,
#             'search_count': len(searches),
#             'click_count': len(clicks),
#             'click_through_rate': round(ctr, 3),
#             'duration_minutes': session.get('duration_minutes', 0),
#             'unique_queries': len(set(s.get('query_normalized', '') for s in searches))
#         }
    
#     def get_location_stats(self, days: int = 7) -> Dict:
#         """
#         Get search volume by location.
        
#         Returns:
#             Location breakdown with countries and cities
#         """
#         countries = {}
#         cities = {}
        
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            
#             # Countries
#             country_data = self.redis.zrevrange(f"analytics:searches:by_country:{date}", 0, 49, withscores=True)
#             for country, count in country_data:
#                 countries[country] = countries.get(country, 0) + int(count)
            
#             # Cities
#             city_data = self.redis.zrevrange(f"analytics:searches:by_city:{date}", 0, 49, withscores=True)
#             for city, count in city_data:
#                 cities[city] = cities.get(city, 0) + int(count)
        
#         return {
#             'countries': sorted(countries.items(), key=lambda x: x[1], reverse=True)[:20],
#             'cities': sorted(cities.items(), key=lambda x: x[1], reverse=True)[:50]
#         }
    
#     def get_daily_stats(self, days: int = 30) -> List[Dict]:
#         """
#         Get daily aggregated statistics.
        
#         Returns:
#             List of daily stat dicts
#         """
#         daily_stats = []
        
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             stats = self.redis.hgetall(f"analytics:daily:{date}")
            
#             if stats:
#                 total_sessions = int(stats.get('total_sessions', 0))
#                 total_duration = int(stats.get('total_duration_seconds', 0))
#                 total_searches = int(stats.get('total_searches', 0))
#                 total_clicks = int(stats.get('total_clicks', 0))
                
#                 daily_stats.append({
#                     'date': date,
#                     'sessions': total_sessions,
#                     'searches': total_searches,
#                     'clicks': total_clicks,
#                     'avg_session_duration_seconds': round(total_duration / total_sessions, 1) if total_sessions else 0,
#                     'searches_per_session': round(total_searches / total_sessions, 2) if total_sessions else 0,
#                     'click_through_rate': round(total_clicks / total_searches, 3) if total_searches else 0,
#                     'logged_in_sessions': int(stats.get('logged_in_sessions', 0)),
#                     'anonymous_sessions': int(stats.get('anonymous_sessions', 0))
#                 })
        
#         return daily_stats
    
#     def get_click_position_distribution(self, days: int = 7) -> Dict[str, int]:
#         """
#         Get distribution of click positions.
        
#         Shows if users are clicking top results or scrolling down.
#         """
#         distribution = {}
        
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             day_data = self.redis.hgetall(f"analytics:click_positions:{date}")
            
#             for position, count in day_data.items():
#                 distribution[position] = distribution.get(position, 0) + int(count)
        
#         return dict(sorted(distribution.items(), key=lambda x: int(x[0].split('_')[1])))
    
#     def get_user_stats(self, user_id: str) -> Dict:
#         """
#         Get stats for a specific logged-in user.
        
#         Returns:
#             User's search history, preferences, and behavior
#         """
#         # Get user's sessions
#         sessions = self.redis.lrange(f"analytics:user:{user_id}:sessions", 0, 99)
        
#         # Get user's searches
#         searches_raw = self.redis.lrange(f"analytics:user:{user_id}:searches", 0, 999)
#         searches = [json.loads(s) for s in searches_raw]
        
#         # Get user's clicks
#         clicks_raw = self.redis.lrange(f"analytics:user:{user_id}:clicks", 0, 499)
#         clicks = [json.loads(c) for c in clicks_raw]
        
#         # Analyze search patterns
#         query_counts = {}
#         for search in searches:
#             q = search.get('query', '').lower()
#             query_counts[q] = query_counts.get(q, 0) + 1
        
#         top_queries = sorted(query_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        
#         return {
#             'user_id': user_id,
#             'total_sessions': len(sessions),
#             'total_searches': len(searches),
#             'total_clicks': len(clicks),
#             'top_queries': top_queries,
#             'recent_searches': searches[:20],
#             'recent_clicks': clicks[:20],
#             'click_through_rate': round(len(clicks) / len(searches), 3) if searches else 0
#         }
    
#     def get_search_strategy_stats(self, days: int = 7) -> Dict:
#         """
#         Get stats by search strategy (semantic, strict, mixed, fallback).
        
#         Helps understand which strategy performs best.
#         """
#         strategies = {}
        
#         for i in range(days):
#             date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
#             day_data = self.redis.hgetall(f"analytics:strategy:{date}")
            
#             for key, value in day_data.items():
#                 if '_results' not in key:
#                     strategy = key
#                     count = int(value)
#                     results = int(day_data.get(f"{strategy}_results", 0))
                    
#                     if strategy not in strategies:
#                         strategies[strategy] = {'count': 0, 'total_results': 0}
                    
#                     strategies[strategy]['count'] += count
#                     strategies[strategy]['total_results'] += results
        
#         # Calculate averages
#         for strategy in strategies:
#             if strategies[strategy]['count'] > 0:
#                 strategies[strategy]['avg_results'] = round(
#                     strategies[strategy]['total_results'] / strategies[strategy]['count'], 2
#                 )
        
#         return strategies
    
#     # ========================================================================
#     # CLEANUP & MAINTENANCE
#     # ========================================================================
    
#     def cleanup_old_data(self, days_to_keep: int = None):
#         """
#         Remove analytics data older than retention period.
        
#         Run this periodically (e.g., daily cron job).
#         """
#         days_to_keep = days_to_keep or ANALYTICS_RETENTION_DAYS
#         cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
#         # Find and delete old daily keys
#         patterns = [
#             "analytics:searches:daily:*",
#             "analytics:searches:zero_results:*",
#             "analytics:alt_mode:*",
#             "analytics:alt_mode_stats:*",
#             "analytics:daily:*",
#             "analytics:clicks:daily:*",
#             "analytics:click_positions:*",
#             "analytics:ctr:*",
#             "analytics:events:*",
#             "analytics:hourly:*",
#             "analytics:intent:*",
#             "analytics:searches:by_country:*",
#             "analytics:searches:by_city:*",
#             "analytics:source_clicks:*",
#             "analytics:strategy:*",
#             "analytics:time_to_click:*",
#             "analytics:user_type:*",
#             "analytics:sessions:daily:*"
#         ]
        
#         deleted_count = 0
#         for pattern in patterns:
#             for key in self.redis.scan_iter(pattern):
#                 # Extract date from key
#                 parts = key.split(':')
#                 for part in parts:
#                     if len(part) == 10 and part.count('-') == 2:  # Looks like a date
#                         try:
#                             key_date = datetime.strptime(part, '%Y-%m-%d')
#                             if key_date < cutoff_date:
#                                 self.redis.delete(key)
#                                 deleted_count += 1
#                         except ValueError:
#                             pass
        
#         return {'deleted_keys': deleted_count}


# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def get_analytics() -> SearchAnalytics:
#     """Get a SearchAnalytics instance."""
#     return SearchAnalytics()


# # ============================================================================
# # USAGE EXAMPLES (for reference)
# # ============================================================================

# """
# # In your Django view:

# from redis_analytics import SearchAnalytics

# analytics = SearchAnalytics()

# # On page load
# analytics.start_session(
#     session_id=request.session.session_key,
#     user_id=request.user.id if request.user.is_authenticated else None,
#     device_type=detect_device(request),
#     user_agent=request.META.get('HTTP_USER_AGENT'),
#     ip_address=get_client_ip(request),
#     location=get_location_from_ip(get_client_ip(request)),
#     referrer=request.META.get('HTTP_REFERER')
# )

# # On search
# analytics.track_search(
#     session_id=request.session.session_key,
#     query=query,
#     results_count=len(results),
#     alt_mode=request.GET.get('alt_mode', 'n'),
#     user_id=request.user.id if request.user.is_authenticated else None,
#     location={'city': 'Atlanta', 'country': 'US'},
#     device_type='mobile',
#     search_time_ms=search_response['search_time'] * 1000,
#     search_strategy=search_response['search_strategy'],
#     corrected_query=search_response.get('corrected_query'),
#     intent=search_response.get('intent')
# )

# # On result click
# analytics.track_click(
#     session_id=request.session.session_key,
#     query=request.GET.get('q'),
#     clicked_url=request.GET.get('url'),
#     clicked_position=int(request.GET.get('position', 0)),
#     result_id=request.GET.get('id'),
#     user_id=request.user.id if request.user.is_authenticated else None
# )

# # Get insights
# popular = analytics.get_popular_queries(days=7, limit=100)
# gaps = analytics.get_zero_result_queries(days=7, limit=50)
# alt_comparison = analytics.compare_alt_modes(days=7)
# """

"""
redis_analytics.py

Comprehensive analytics tracking for search behavior, sessions, and user actions.

PRODUCTION FEATURES:
- Connection using REDIS_ANALYTICS_URL (supports multiple Redis databases)
- Circuit breaker pattern for Redis outages
- Retry logic with exponential backoff
- Graceful degradation on failures
- Health check endpoint
- Proper logging (no print statements)

TRACKS:
- Search queries (with alt_mode comparison: dropdown vs semantic)
- User sessions (duration, activity, engagement)
- Click events (what users click after searching)
- Zero-result searches (content gaps)
- Location data (geographic insights)
- Logged-in vs anonymous behavior
- Query patterns and trends

REDIS STRUCTURE:
- session:{session_id}           → Hash: session metadata
- session:{session_id}:searches  → List: all searches in session
- session:{session_id}:clicks    → List: all clicks in session
- session:{session_id}:events    → List: all events timeline
- analytics:searches:daily:{date} → Sorted Set: query frequencies
- analytics:searches:zero_results → Sorted Set: failed queries
- analytics:alt_mode:y           → Sorted Set: dropdown searches
- analytics:alt_mode:n           → Sorted Set: semantic searches
- analytics:locations            → Sorted Set: search by location
- analytics:users:{user_id}      → Hash: user-level stats
- analytics:popular:queries      → Sorted Set: all-time popular
- analytics:popular:clicks       → Sorted Set: most clicked results

USAGE:
    from redis_analytics import SearchAnalytics
    
    analytics = SearchAnalytics()
    
    # Check health before using
    if analytics.is_healthy():
        analytics.track_search(...)
    
    # Track a search
    analytics.track_search(
        session_id="abc123",
        query="black hair care",
        results_count=25,
        alt_mode="n",
        user_id=None,  # or user ID if logged in
        location={"city": "Atlanta", "country": "US"},
        device_type="mobile",
        search_time_ms=145
    )
    
    # Track a click
    analytics.track_click(
        session_id="abc123",
        query="black hair care",
        clicked_url="https://example.com/article",
        clicked_position=2,
        result_id="doc_uuid_here"
    )
    
    # Get insights
    analytics.get_popular_queries(days=7, limit=100)
    analytics.get_zero_result_queries(limit=50)
    analytics.compare_alt_modes()
"""

import logging
import redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    RedisError,
)
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Callable
from functools import wraps
from decouple import config
import hashlib
import threading


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Redis URL - supports multiple databases via URL path (e.g., redis://localhost:6379/2)
REDIS_ANALYTICS_URL = config('REDIS_ANALYTICS_URL', default='redis://localhost:6379/2')

SESSION_EXPIRE_HOURS = config('SESSION_EXPIRE_HOURS', default=24, cast=int)
ANALYTICS_RETENTION_DAYS = config('ANALYTICS_RETENTION_DAYS', default=90, cast=int)

# Connection settings
REDIS_SOCKET_TIMEOUT = config('REDIS_SOCKET_TIMEOUT', default=5, cast=int)
REDIS_SOCKET_CONNECT_TIMEOUT = config('REDIS_SOCKET_CONNECT_TIMEOUT', default=5, cast=int)
REDIS_RETRY_ON_TIMEOUT = config('REDIS_RETRY_ON_TIMEOUT', default=True, cast=bool)

# Retry settings
REDIS_MAX_RETRIES = config('REDIS_MAX_RETRIES', default=3, cast=int)
REDIS_RETRY_DELAY = config('REDIS_RETRY_DELAY', default=0.1, cast=float)  # Base delay in seconds
REDIS_RETRY_BACKOFF = config('REDIS_RETRY_BACKOFF', default=2.0, cast=float)  # Exponential backoff multiplier

# Circuit breaker settings
CIRCUIT_BREAKER_FAILURE_THRESHOLD = config('CIRCUIT_BREAKER_FAILURE_THRESHOLD', default=5, cast=int)
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = config('CIRCUIT_BREAKER_RECOVERY_TIMEOUT', default=30, cast=int)  # seconds
CIRCUIT_BREAKER_HALF_OPEN_REQUESTS = config('CIRCUIT_BREAKER_HALF_OPEN_REQUESTS', default=3, cast=int)


# ============================================================================
# CIRCUIT BREAKER IMPLEMENTATION
# ============================================================================

class CircuitBreakerState:
    """Enum-like class for circuit breaker states."""
    CLOSED = 'closed'      # Normal operation
    OPEN = 'open'          # Failing, reject requests
    HALF_OPEN = 'half_open'  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for Redis connections.
    
    Prevents cascading failures by stopping requests to a failing service
    and allowing it time to recover.
    """
    
    def __init__(
        self,
        failure_threshold: int = CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout: int = CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
        half_open_requests: int = CIRCUIT_BREAKER_HALF_OPEN_REQUESTS
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_requests = half_open_requests
        
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_successes = 0
        self._lock = threading.Lock()
    
    @property
    def state(self) -> str:
        """Get current circuit breaker state."""
        with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                # Check if recovery timeout has passed
                if self._last_failure_time and \
                   time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_successes = 0
                    logger.info("Circuit breaker transitioning to HALF_OPEN state")
            return self._state
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == CircuitBreakerState.CLOSED
    
    @property
    def is_open(self) -> bool:
        """Check if circuit is open (rejecting requests)."""
        return self.state == CircuitBreakerState.OPEN
    
    @property
    def allows_request(self) -> bool:
        """Check if a request should be allowed."""
        state = self.state
        return state in (CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN)
    
    def record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_requests:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    self._last_failure_time = None
                    logger.info("Circuit breaker CLOSED after successful recovery")
            elif self._state == CircuitBreakerState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed request."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitBreakerState.HALF_OPEN:
                # Any failure in half-open state opens the circuit
                self._state = CircuitBreakerState.OPEN
                logger.warning("Circuit breaker OPEN after failure in half-open state")
            elif self._state == CircuitBreakerState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(
                        f"Circuit breaker OPEN after {self._failure_count} consecutive failures"
                    )
    
    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None
            self._half_open_successes = 0
            logger.info("Circuit breaker manually reset to CLOSED state")
    
    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status for monitoring."""
        with self._lock:
            return {
                'state': self._state,
                'failure_count': self._failure_count,
                'last_failure_time': self._last_failure_time,
                'half_open_successes': self._half_open_successes,
                'failure_threshold': self.failure_threshold,
                'recovery_timeout': self.recovery_timeout
            }


# ============================================================================
# RETRY DECORATOR
# ============================================================================

def with_retry(
    max_retries: int = REDIS_MAX_RETRIES,
    base_delay: float = REDIS_RETRY_DELAY,
    backoff: float = REDIS_RETRY_BACKOFF,
    exceptions: tuple = (RedisConnectionError, RedisTimeoutError)
):
    """
    Decorator that adds retry logic with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        backoff: Multiplier for exponential backoff
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                # Check circuit breaker
                if hasattr(self, 'circuit_breaker') and not self.circuit_breaker.allows_request:
                    logger.warning(f"Circuit breaker OPEN, skipping {func.__name__}")
                    return self._get_default_return_value(func.__name__)
                
                try:
                    result = func(self, *args, **kwargs)
                    # Record success
                    if hasattr(self, 'circuit_breaker'):
                        self.circuit_breaker.record_success()
                    return result
                    
                except exceptions as e:
                    last_exception = e
                    
                    # Record failure
                    if hasattr(self, 'circuit_breaker'):
                        self.circuit_breaker.record_failure()
                    
                    if attempt < max_retries:
                        delay = base_delay * (backoff ** attempt)
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        
                except RedisError as e:
                    # Non-retryable Redis errors
                    logger.error(f"{func.__name__} failed with non-retryable error: {e}")
                    if hasattr(self, 'circuit_breaker'):
                        self.circuit_breaker.record_failure()
                    return self._get_default_return_value(func.__name__)
            
            # All retries exhausted
            return self._get_default_return_value(func.__name__)
        
        return wrapper
    return decorator


# ============================================================================
# REDIS CONNECTION
# ============================================================================

_redis_client = None


def get_redis_client() -> Optional[redis.Redis]:
    """Get or create Redis connection for analytics."""
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    try:
        _redis_client = redis.from_url(
            REDIS_ANALYTICS_URL,
            decode_responses=True,
            socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
        )
        # Test connection
        _redis_client.ping()
        logger.info(f"Redis analytics connection established: {REDIS_ANALYTICS_URL}")
        return _redis_client
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Failed to connect to Redis analytics: {e}")
        _redis_client = None
        return None
    except Exception as e:
        logger.error(f"Unexpected error connecting to Redis analytics: {e}")
        _redis_client = None
        return None


# ============================================================================
# MAIN ANALYTICS CLASS
# ============================================================================

class SearchAnalytics:
    """
    Comprehensive search analytics tracking with production-grade reliability.
    
    Features:
    - Circuit breaker for Redis outages
    - Retry logic with exponential backoff
    - Graceful degradation on failures
    - Health check endpoint
    
    Tracks user sessions, searches, clicks, and generates insights
    for understanding user behavior and improving search quality.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        Initialize with optional custom Redis client.
        
        Args:
            redis_client: Custom Redis client (uses default connection if None)
        """
        self.redis = redis_client or get_redis_client()
        self.session_expire_seconds = SESSION_EXPIRE_HOURS * 3600
        self.circuit_breaker = CircuitBreaker()
        
        if self.redis is None:
            logger.warning("SearchAnalytics initialized without Redis connection")
    
    def _get_default_return_value(self, method_name: str) -> Any:
        """
        Get default return value for methods when Redis is unavailable.
        
        Provides graceful degradation by returning appropriate empty values.
        """
        # Methods that return Dict
        dict_methods = {
            'start_session', 'get_session', 'end_session', 'track_search',
            'track_click', 'track_event', 'get_session_stats', 'compare_alt_modes',
            'get_location_stats', 'get_user_stats', 'get_search_strategy_stats',
            'cleanup_old_data', 'get_click_position_distribution', 'health_check'
        }
        
        # Methods that return List
        list_methods = {
            'get_popular_queries', 'get_zero_result_queries', 'get_daily_stats'
        }
        
        if method_name in dict_methods:
            return {}
        elif method_name in list_methods:
            return []
        else:
            return None
    
    def _get_time_bucket(self, time_ms: int) -> str:
        """
        Convert time-to-click milliseconds into a bucket for aggregation.
        
        Buckets:
        - instant: < 2 seconds
        - fast: 2-5 seconds
        - normal: 5-15 seconds
        - slow: 15-30 seconds
        - very_slow: > 30 seconds
        """
        if time_ms < 2000:
            return 'instant'
        elif time_ms < 5000:
            return 'fast'
        elif time_ms < 15000:
            return 'normal'
        elif time_ms < 30000:
            return 'slow'
        else:
            return 'very_slow'
    
    # ========================================================================
    # HEALTH CHECK
    # ========================================================================
    
    def health_check(self) -> Dict[str, Any]:
        """
        Comprehensive health check for the analytics system.
        
        Returns:
            Dict with health status, connection info, and circuit breaker state
        """
        result = {
            'healthy': False,
            'redis_connected': False,
            'redis_url': REDIS_ANALYTICS_URL,
            'circuit_breaker': self.circuit_breaker.get_status(),
            'timestamp': datetime.utcnow().isoformat(),
            'error': None
        }
        
        if self.redis is None:
            result['error'] = 'Redis client not initialized'
            return result
        
        try:
            # Test Redis connection
            start_time = time.time()
            self.redis.ping()
            latency_ms = (time.time() - start_time) * 1000
            
            result['redis_connected'] = True
            result['latency_ms'] = round(latency_ms, 2)
            result['healthy'] = self.circuit_breaker.is_closed or self.circuit_breaker.state == CircuitBreakerState.HALF_OPEN
            
        except RedisConnectionError as e:
            result['error'] = f'Connection error: {e}'
            logger.error(f"Health check failed: {e}")
        except RedisTimeoutError as e:
            result['error'] = f'Timeout error: {e}'
            logger.error(f"Health check timeout: {e}")
        except Exception as e:
            result['error'] = f'Unexpected error: {e}'
            logger.error(f"Health check unexpected error: {e}")
        
        return result
    
    def is_healthy(self) -> bool:
        """
        Quick health check returning boolean.
        
        Returns:
            True if analytics system is healthy and ready
        """
        if self.redis is None:
            return False
        
        if not self.circuit_breaker.allows_request:
            return False
        
        try:
            self.redis.ping()
            return True
        except Exception:
            return False
    
    def reset_circuit_breaker(self) -> None:
        """Manually reset circuit breaker to closed state."""
        self.circuit_breaker.reset()
    
    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================
    
    @with_retry()
    def start_session(
        self,
        session_id: str,
        user_id: Optional[str] = None,
        device_type: Optional[str] = None,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
        location: Optional[Dict] = None,
        referrer: Optional[str] = None,
        browser: Optional[str] = None,
        browser_version: Optional[str] = None,
        os_name: Optional[str] = None,
        os_version: Optional[str] = None,
        is_mobile: bool = False,
        is_bot: bool = False
    ) -> Dict:
        """
        Initialize or update a user session.
        
        Call this when a user first visits or on each page load to keep session alive.
        
        Args:
            session_id: Unique session identifier
            user_id: User ID if logged in
            device_type: desktop/mobile/tablet
            user_agent: Full user agent string
            ip_address: Client IP address
            location: Location dict with city, region, country, etc.
            referrer: HTTP referrer
            browser: Browser name
            browser_version: Browser version
            os_name: Operating system name
            os_version: OS version
            is_mobile: Whether device is mobile
            is_bot: Whether request is from a bot
            
        Returns:
            Session data dict
        """
        if self.redis is None:
            logger.warning("Cannot start session: Redis not available")
            return {}
        
        session_key = f"session:{session_id}"
        now = time.time()
        now_iso = datetime.utcnow().isoformat()
        
        # Check if session exists
        existing = self.redis.hgetall(session_key)
        
        if existing:
            # Update existing session
            self.redis.hset(session_key, mapping={
                'last_active': now_iso,
                'last_active_ts': now,
                'page_views': int(existing.get('page_views', 0)) + 1
            })
            
            # Update user_id if they just logged in
            if user_id and not existing.get('user_id'):
                self.redis.hset(session_key, 'user_id', user_id)
                self._link_session_to_user(session_id, user_id)
        else:
            # Create new session
            session_data = {
                'session_id': session_id,
                'created_at': now_iso,
                'created_ts': now,
                'last_active': now_iso,
                'last_active_ts': now,
                'user_id': user_id or '',
                'device_type': device_type or 'unknown',
                'user_agent': (user_agent or '')[:500],  # Limit user agent length
                'ip_address': ip_address or '',
                # Location fields
                'location_city': location.get('city', '') if location else '',
                'location_region': location.get('region', '') if location else '',
                'location_country': location.get('country', '') if location else '',
                'location_country_code': location.get('country_code', '') if location else '',
                'location_lat': location.get('lat', '') if location else '',
                'location_lng': location.get('lng', '') if location else '',
                'location_timezone': location.get('timezone', '') if location else '',
                # Browser/Device fields
                'browser': browser or 'Unknown',
                'browser_version': browser_version or '',
                'os_name': os_name or 'Unknown',
                'os_version': os_version or '',
                'is_mobile': '1' if is_mobile else '0',
                'is_bot': '1' if is_bot else '0',
                # Other fields
                'referrer': (referrer or '')[:500],  # Limit referrer length
                'page_views': 1,
                'search_count': 0,
                'click_count': 0,
                'is_logged_in': '1' if user_id else '0'
            }
            
            self.redis.hset(session_key, mapping=session_data)
            
            # Use pipeline for batch operations
            pipe = self.redis.pipeline()
            
            # Track daily active session
            date_key = datetime.utcnow().strftime('%Y-%m-%d')
            pipe.sadd(f"analytics:sessions:daily:{date_key}", session_id)
            pipe.expire(f"analytics:sessions:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # Track by location
            if location and location.get('country'):
                pipe.zincrby("analytics:locations:countries", 1, location['country'])
            if location and location.get('city'):
                city_key = f"{location.get('city')}, {location.get('country_code', location.get('country', ''))}"
                pipe.zincrby("analytics:locations:cities", 1, city_key)
            
            # Track by device type
            if device_type:
                pipe.zincrby(f"analytics:devices:{date_key}", 1, device_type)
                pipe.expire(f"analytics:devices:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # Track by browser
            if browser:
                pipe.zincrby(f"analytics:browsers:{date_key}", 1, browser)
                pipe.expire(f"analytics:browsers:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # Track by OS
            if os_name:
                pipe.zincrby(f"analytics:os:{date_key}", 1, os_name)
                pipe.expire(f"analytics:os:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # Track bot vs human
            if is_bot:
                pipe.hincrby(f"analytics:daily:{date_key}", 'bot_sessions', 1)
            
            # Execute pipeline
            try:
                pipe.execute()
            except RedisError as e:
                logger.warning(f"Pipeline execution failed in start_session: {e}")
                # Session was created, just tracking failed - continue
            
            # Link to user if logged in
            if user_id:
                self._link_session_to_user(session_id, user_id)
        
        # Set/refresh expiration
        self.redis.expire(session_key, self.session_expire_seconds)
        
        return self.get_session(session_id)
    
    @with_retry()
    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get session data."""
        if self.redis is None:
            return None
        
        session_key = f"session:{session_id}"
        data = self.redis.hgetall(session_key)
        
        if not data:
            return None
        
        # Calculate session duration
        try:
            created_ts = float(data.get('created_ts', 0))
            last_active_ts = float(data.get('last_active_ts', 0))
            
            data['duration_seconds'] = int(last_active_ts - created_ts)
            data['duration_minutes'] = round((last_active_ts - created_ts) / 60, 1)
        except (TypeError, ValueError):
            data['duration_seconds'] = 0
            data['duration_minutes'] = 0
        
        return data
    
    @with_retry()
    def end_session(self, session_id: str) -> Optional[Dict]:
        """
        Explicitly end a session (e.g., user logs out).
        
        Calculates final stats and archives session data.
        """
        if self.redis is None:
            return None
        
        session = self.get_session(session_id)
        if not session:
            return None
        
        # Mark as ended
        session_key = f"session:{session_id}"
        self.redis.hset(session_key, mapping={
            'ended_at': datetime.utcnow().isoformat(),
            'ended_ts': time.time(),
            'status': 'ended'
        })
        
        # Archive session stats
        self._archive_session_stats(session_id, session)
        
        return self.get_session(session_id)
    
    def _link_session_to_user(self, session_id: str, user_id: str) -> None:
        """Link session to user for cross-session analysis."""
        if self.redis is None:
            return
        
        try:
            user_sessions_key = f"analytics:user:{user_id}:sessions"
            self.redis.lpush(user_sessions_key, session_id)
            self.redis.ltrim(user_sessions_key, 0, 99)  # Keep last 100 sessions
            self.redis.expire(user_sessions_key, ANALYTICS_RETENTION_DAYS * 86400)
        except RedisError as e:
            logger.warning(f"Failed to link session to user: {e}")
    
    def _archive_session_stats(self, session_id: str, session: Dict) -> None:
        """Archive session statistics for long-term analysis."""
        if self.redis is None:
            return
        
        date_key = datetime.utcnow().strftime('%Y-%m-%d')
        
        try:
            # Aggregate daily stats
            pipe = self.redis.pipeline()
            
            duration = int(session.get('duration_seconds', 0))
            searches = int(session.get('search_count', 0))
            clicks = int(session.get('click_count', 0))
            
            pipe.hincrby(f"analytics:daily:{date_key}", 'total_sessions', 1)
            pipe.hincrby(f"analytics:daily:{date_key}", 'total_duration_seconds', duration)
            pipe.hincrby(f"analytics:daily:{date_key}", 'total_searches', searches)
            pipe.hincrby(f"analytics:daily:{date_key}", 'total_clicks', clicks)
            
            if session.get('is_logged_in') == '1':
                pipe.hincrby(f"analytics:daily:{date_key}", 'logged_in_sessions', 1)
            else:
                pipe.hincrby(f"analytics:daily:{date_key}", 'anonymous_sessions', 1)
            
            pipe.expire(f"analytics:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            pipe.execute()
        except RedisError as e:
            logger.warning(f"Failed to archive session stats: {e}")
    
    # ========================================================================
    # SEARCH TRACKING
    # ========================================================================
    
    @with_retry()
    def track_search(
        self,
        session_id: str,
        query: str,
        results_count: int,
        alt_mode: str = 'n',
        user_id: Optional[str] = None,
        location: Optional[Dict] = None,
        device_type: Optional[str] = None,
        search_time_ms: Optional[float] = None,
        search_strategy: Optional[str] = None,
        corrected_query: Optional[str] = None,
        filters_applied: Optional[Dict] = None,
        page: int = 1,
        intent: Optional[str] = None,
        request_id: Optional[str] = None,
        browser: Optional[str] = None,
        browser_version: Optional[str] = None,
        os_name: Optional[str] = None,
        os_version: Optional[str] = None,
        is_mobile: bool = False,
        is_bot: bool = False
    ) -> Dict:
        """
        Track a search event with full context.
        
        Args:
            session_id: Browser session ID
            query: The search query
            results_count: Number of results returned
            alt_mode: 'y' for dropdown/strict, 'n' for semantic
            user_id: User ID if logged in
            location: {city, region, country, lat, lng}
            device_type: desktop/mobile/tablet
            search_time_ms: How long the search took
            search_strategy: semantic/strict/mixed/text_fallback
            corrected_query: Spell-corrected version
            filters_applied: Any filters used
            page: Results page number
            intent: Detected intent (location, person, etc.)
            request_id: Unique request ID for this search
            browser: Browser name (Chrome, Firefox, etc.)
            browser_version: Browser version
            os_name: Operating system name
            os_version: OS version
            is_mobile: Whether device is mobile
            is_bot: Whether request is from a bot
            
        Returns:
            Search event data
        """
        if self.redis is None:
            logger.warning("Cannot track search: Redis not available")
            return {}
        
        now = time.time()
        now_iso = datetime.utcnow().isoformat()
        date_key = datetime.utcnow().strftime('%Y-%m-%d')
        hour_key = datetime.utcnow().strftime('%Y-%m-%d:%H')
        
        # Normalize query for aggregation
        query_normalized = query.lower().strip()[:500]  # Limit query length
        query_hash = hashlib.md5(query_normalized.encode()).hexdigest()[:12]
        
        # Build search event
        search_event = {
            'event_type': 'search',
            'timestamp': now_iso,
            'timestamp_ts': now,
            'session_id': session_id,
            'request_id': request_id or f"{session_id}:{now}",
            'query': query[:500],  # Limit query length
            'query_normalized': query_normalized,
            'query_hash': query_hash,
            'query_length': len(query),
            'query_word_count': len(query.split()),
            'results_count': results_count,
            'has_results': results_count > 0,
            'alt_mode': alt_mode,
            'user_id': user_id or '',
            'is_logged_in': bool(user_id),
            'device_type': device_type or 'unknown',
            'browser': browser or 'Unknown',
            'browser_version': browser_version or '',
            'os_name': os_name or 'Unknown',
            'os_version': os_version or '',
            'is_mobile': is_mobile,
            'is_bot': is_bot,
            'search_time_ms': search_time_ms or 0,
            'search_strategy': search_strategy or '',
            'corrected_query': (corrected_query or '')[:500],
            'was_corrected': corrected_query and corrected_query.lower() != query.lower(),
            'filters_applied': json.dumps(filters_applied) if filters_applied else '{}',
            'page': page,
            'intent': intent or 'general',
            'location_city': location.get('city', '') if location else '',
            'location_country': location.get('country', '') if location else '',
            'location_country_code': location.get('country_code', '') if location else ''
        }
        
        # Execute all Redis operations in pipeline
        pipe = self.redis.pipeline()
        
        try:
            # 1. Add to session's search history
            session_searches_key = f"session:{session_id}:searches"
            pipe.lpush(session_searches_key, json.dumps(search_event))
            pipe.ltrim(session_searches_key, 0, 499)  # Keep last 500 searches per session
            pipe.expire(session_searches_key, self.session_expire_seconds)
            
            # 2. Add to session's event timeline
            session_events_key = f"session:{session_id}:events"
            pipe.lpush(session_events_key, json.dumps(search_event))
            pipe.ltrim(session_events_key, 0, 999)
            pipe.expire(session_events_key, self.session_expire_seconds)
            
            # 3. Update session search count
            session_key = f"session:{session_id}"
            pipe.hincrby(session_key, 'search_count', 1)
            pipe.hset(session_key, 'last_search', now_iso)
            pipe.hset(session_key, 'last_query', query[:200])
            
            # 4. Track query popularity (daily)
            pipe.zincrby(f"analytics:searches:daily:{date_key}", 1, query_normalized[:100])
            pipe.expire(f"analytics:searches:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 5. Track query popularity (all-time, with decay)
            pipe.zincrby("analytics:popular:queries", 1, query_normalized[:100])
            
            # 6. Track by alt_mode for comparison
            alt_mode_key = f"analytics:alt_mode:{alt_mode}:{date_key}"
            pipe.zincrby(alt_mode_key, 1, query_normalized[:100])
            pipe.expire(alt_mode_key, ANALYTICS_RETENTION_DAYS * 86400)
            
            # 7. Track alt_mode stats
            pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_count", 1)
            pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_results", results_count)
            if search_time_ms:
                pipe.hincrby(f"analytics:alt_mode_stats:{date_key}", f"mode_{alt_mode}_time_ms", int(search_time_ms))
            pipe.expire(f"analytics:alt_mode_stats:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 8. Track zero-result searches (content gaps!)
            if results_count == 0:
                pipe.zincrby("analytics:searches:zero_results", 1, query_normalized[:100])
                pipe.zincrby(f"analytics:searches:zero_results:{date_key}", 1, query_normalized[:100])
                pipe.expire(f"analytics:searches:zero_results:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 9. Track by location
            if location:
                if location.get('country'):
                    pipe.zincrby(f"analytics:searches:by_country:{date_key}", 1, location['country'])
                    pipe.expire(f"analytics:searches:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
                    pipe.zincrby("analytics:locations:countries", 1, location['country'])
                if location.get('city'):
                    city_key = f"{location.get('city')}, {location.get('country_code', location.get('country', ''))}"
                    pipe.zincrby(f"analytics:searches:by_city:{date_key}", 1, city_key[:100])
                    pipe.expire(f"analytics:searches:by_city:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
                    pipe.zincrby("analytics:locations:cities", 1, city_key[:100])
            
            # 10. Track by intent
            if intent:
                pipe.zincrby(f"analytics:intent:{date_key}", 1, intent)
                pipe.expire(f"analytics:intent:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 11. Track hourly volume (for traffic patterns)
            pipe.hincrby(f"analytics:hourly:{hour_key}", 'searches', 1)
            pipe.expire(f"analytics:hourly:{hour_key}", 7 * 86400)  # Keep 7 days of hourly
            
            # 12. Track search strategy effectiveness
            if search_strategy:
                pipe.hincrby(f"analytics:strategy:{date_key}", search_strategy, 1)
                pipe.hincrby(f"analytics:strategy:{date_key}", f"{search_strategy}_results", results_count)
                pipe.expire(f"analytics:strategy:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 13. Track logged-in vs anonymous
            user_type = 'logged_in' if user_id else 'anonymous'
            pipe.hincrby(f"analytics:user_type:{date_key}", user_type, 1)
            pipe.expire(f"analytics:user_type:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 14. If logged in, track user's search history
            if user_id:
                user_searches_key = f"analytics:user:{user_id}:searches"
                pipe.lpush(user_searches_key, json.dumps({
                    'query': query[:200],
                    'timestamp': now_iso,
                    'results_count': results_count
                }))
                pipe.ltrim(user_searches_key, 0, 999)  # Keep last 1000 searches per user
                pipe.expire(user_searches_key, ANALYTICS_RETENTION_DAYS * 86400)
            
            pipe.execute()
            
        except RedisError as e:
            logger.error(f"Pipeline execution failed in track_search: {e}")
            # Re-raise to trigger retry
            raise
        
        return search_event
    
    # ========================================================================
    # CLICK TRACKING
    # ========================================================================
    
    @with_retry()
    def track_click(
        self,
        session_id: str,
        query: str,
        clicked_url: str,
        clicked_position: int,
        result_id: Optional[str] = None,
        result_title: Optional[str] = None,
        result_source: Optional[str] = None,
        user_id: Optional[str] = None,
        time_to_click_ms: Optional[int] = None,
        location: Optional[Dict] = None,
        search_request_id: Optional[str] = None,
        results_count: Optional[int] = None,
        was_corrected: bool = False,
        corrected_query: Optional[str] = None
    ) -> Dict:
        """
        Track when a user clicks on a search result.
        
        Args:
            session_id: Browser session ID
            query: The search query that led to this click
            clicked_url: URL that was clicked
            clicked_position: Position in results (1-indexed)
            result_id: Document UUID
            result_title: Title of clicked result
            result_source: Source/brand of result
            user_id: User ID if logged in
            time_to_click_ms: Time from search to click
            location: {city, region, country, lat, lng}
            search_request_id: Request ID from the original search (links click to search)
            results_count: Number of results from the original search
            was_corrected: Whether the query was spell-corrected
            corrected_query: The corrected query if applicable
            
        Returns:
            Click event data
        """
        if self.redis is None:
            logger.warning("Cannot track click: Redis not available")
            return {}
        
        now = time.time()
        now_iso = datetime.utcnow().isoformat()
        date_key = datetime.utcnow().strftime('%Y-%m-%d')
        
        query_normalized = query.lower().strip()[:500]
        
        click_event = {
            'event_type': 'click',
            'timestamp': now_iso,
            'timestamp_ts': now,
            'session_id': session_id,
            'query': query[:500],
            'query_normalized': query_normalized,
            'clicked_url': clicked_url[:2000],
            'clicked_position': clicked_position,
            'result_id': result_id or '',
            'result_title': (result_title or '')[:500],
            'result_source': (result_source or '')[:200],
            'user_id': user_id or '',
            'time_to_click_ms': time_to_click_ms or 0,
            'location_city': location.get('city', '') if location else '',
            'location_region': location.get('region', '') if location else '',
            'location_country': location.get('country', '') if location else '',
            'search_request_id': search_request_id or '',
            'results_count': results_count or 0,
            'was_corrected': was_corrected,
            'corrected_query': (corrected_query or '')[:500]
        }
        
        pipe = self.redis.pipeline()
        
        try:
            # 1. Add to session's click history
            session_clicks_key = f"session:{session_id}:clicks"
            pipe.lpush(session_clicks_key, json.dumps(click_event))
            pipe.ltrim(session_clicks_key, 0, 499)
            pipe.expire(session_clicks_key, self.session_expire_seconds)
            
            # 2. Add to session's event timeline
            session_events_key = f"session:{session_id}:events"
            pipe.lpush(session_events_key, json.dumps(click_event))
            pipe.ltrim(session_events_key, 0, 999)
            
            # 3. Update session click count
            session_key = f"session:{session_id}"
            pipe.hincrby(session_key, 'click_count', 1)
            pipe.hset(session_key, 'last_click', now_iso)
            
            # 4. Track popular clicked URLs
            pipe.zincrby("analytics:popular:clicks", 1, clicked_url[:500])
            pipe.zincrby(f"analytics:clicks:daily:{date_key}", 1, clicked_url[:500])
            pipe.expire(f"analytics:clicks:daily:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 5. Track click position distribution (are users clicking top results?)
            position_key = f"position_{min(clicked_position, 100)}"  # Cap position at 100
            pipe.hincrby(f"analytics:click_positions:{date_key}", position_key, 1)
            pipe.expire(f"analytics:click_positions:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 6. Track query → click relationship
            pipe.zincrby(f"analytics:query_clicks:{query_normalized[:50]}", 1, clicked_url[:500])
            
            # 7. Track click-through rate data
            pipe.hincrby(f"analytics:ctr:{date_key}", 'total_clicks', 1)
            pipe.expire(f"analytics:ctr:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 8. Track source/brand clicks
            if result_source:
                pipe.zincrby(f"analytics:source_clicks:{date_key}", 1, result_source[:100])
                pipe.expire(f"analytics:source_clicks:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 9. Track time-to-click distribution
            if time_to_click_ms:
                bucket = self._get_time_bucket(time_to_click_ms)
                pipe.hincrby(f"analytics:time_to_click:{date_key}", bucket, 1)
                pipe.expire(f"analytics:time_to_click:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 10. If logged in, track user clicks
            if user_id:
                user_clicks_key = f"analytics:user:{user_id}:clicks"
                pipe.lpush(user_clicks_key, json.dumps({
                    'query': query[:200],
                    'url': clicked_url[:500],
                    'position': clicked_position,
                    'timestamp': now_iso,
                    'results_count': results_count or 0,
                    'was_corrected': was_corrected
                }))
                pipe.ltrim(user_clicks_key, 0, 499)
                pipe.expire(user_clicks_key, ANALYTICS_RETENTION_DAYS * 86400)
            
            # 11. Track clicks by location
            if location:
                if location.get('country'):
                    pipe.zincrby(f"analytics:clicks:by_country:{date_key}", 1, location['country'])
                    pipe.expire(f"analytics:clicks:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
                if location.get('city'):
                    city_key = f"{location.get('city')}, {location.get('country', '')}"
                    pipe.zincrby(f"analytics:clicks:by_city:{date_key}", 1, city_key[:100])
                    pipe.expire(f"analytics:clicks:by_city:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 12. Track clicks on corrected vs non-corrected queries
            correction_status = 'corrected' if was_corrected else 'original'
            pipe.hincrby(f"analytics:clicks:correction_status:{date_key}", correction_status, 1)
            pipe.expire(f"analytics:clicks:correction_status:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # 13. Track result_id clicks (which documents get clicked most)
            if result_id:
                pipe.zincrby(f"analytics:document_clicks:{date_key}", 1, result_id[:100])
                pipe.expire(f"analytics:document_clicks:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
                pipe.zincrby("analytics:popular:documents", 1, result_id[:100])
            
            pipe.execute()
            
        except RedisError as e:
            logger.error(f"Pipeline execution failed in track_click: {e}")
            raise
        
        return click_event
    
    # ========================================================================
    # EVENT TRACKING (Generic)
    # ========================================================================
    
    @with_retry()
    def track_event(
        self,
        session_id: str,
        event_type: str,
        event_data: Dict,
        user_id: Optional[str] = None,
        location: Optional[Dict] = None
    ) -> Dict:
        """
        Track any custom event.
        
        Use for: page views, filter usage, result expansion, etc.
        
        Args:
            session_id: Browser session ID
            event_type: Type of event (e.g., 'filter_applied', 'result_expanded')
            event_data: Event-specific data
            user_id: User ID if logged in
            location: {city, region, country, lat, lng}
            
        Returns:
            Event data with timestamp
        """
        if self.redis is None:
            logger.warning("Cannot track event: Redis not available")
            return {}
        
        now = time.time()
        now_iso = datetime.utcnow().isoformat()
        date_key = datetime.utcnow().strftime('%Y-%m-%d')
        
        # Sanitize event_data - limit size of values
        sanitized_event_data = {}
        for key, value in list(event_data.items())[:50]:  # Max 50 keys
            if isinstance(value, str):
                sanitized_event_data[key[:100]] = value[:1000]
            elif isinstance(value, (int, float, bool)):
                sanitized_event_data[key[:100]] = value
            elif isinstance(value, dict):
                sanitized_event_data[key[:100]] = {
                    k[:50]: str(v)[:200] for k, v in list(value.items())[:20]
                }
            else:
                sanitized_event_data[key[:100]] = str(value)[:500]
        
        event = {
            'event_type': event_type[:100],
            'timestamp': now_iso,
            'timestamp_ts': now,
            'session_id': session_id,
            'user_id': user_id or '',
            'location_city': location.get('city', '') if location else '',
            'location_region': location.get('region', '') if location else '',
            'location_country': location.get('country', '') if location else '',
            **sanitized_event_data
        }
        
        pipe = self.redis.pipeline()
        
        try:
            # Add to session timeline
            session_events_key = f"session:{session_id}:events"
            pipe.lpush(session_events_key, json.dumps(event))
            pipe.ltrim(session_events_key, 0, 999)
            pipe.expire(session_events_key, self.session_expire_seconds)
            
            # Track event type counts
            pipe.hincrby(f"analytics:events:{date_key}", event_type[:100], 1)
            pipe.expire(f"analytics:events:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # Track events by location
            if location and location.get('country'):
                pipe.hincrby(f"analytics:events:by_country:{date_key}", location['country'], 1)
                pipe.expire(f"analytics:events:by_country:{date_key}", ANALYTICS_RETENTION_DAYS * 86400)
            
            # If logged in, track user events
            if user_id:
                user_events_key = f"analytics:user:{user_id}:events"
                pipe.lpush(user_events_key, json.dumps({
                    'event_type': event_type[:100],
                    'timestamp': now_iso,
                    **sanitized_event_data
                }))
                pipe.ltrim(user_events_key, 0, 499)
                pipe.expire(user_events_key, ANALYTICS_RETENTION_DAYS * 86400)
            
            pipe.execute()
            
        except RedisError as e:
            logger.error(f"Pipeline execution failed in track_event: {e}")
            raise
        
        return event
    
    # ========================================================================
    # ANALYTICS QUERIES - INSIGHTS
    # ========================================================================
    
    @with_retry()
    def get_popular_queries(
        self,
        days: int = 7,
        limit: int = 100
    ) -> List[Tuple[str, int]]:
        """
        Get most popular search queries.
        
        Args:
            days: Look back period
            limit: Number of results
            
        Returns:
            List of (query, count) tuples
        """
        if self.redis is None:
            return []
        
        if days == 0:
            # All-time
            results = self.redis.zrevrange("analytics:popular:queries", 0, limit - 1, withscores=True)
            return [(q, int(s)) for q, s in results]
        
        # Aggregate from daily keys
        keys = []
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            keys.append(f"analytics:searches:daily:{date}")
        
        # Use zunionstore to combine
        temp_key = f"temp:popular_queries:{time.time()}"
        if keys:
            existing_keys = [k for k in keys if self.redis.exists(k)]
            if existing_keys:
                self.redis.zunionstore(temp_key, existing_keys)
                results = self.redis.zrevrange(temp_key, 0, limit - 1, withscores=True)
                self.redis.delete(temp_key)
                return [(q, int(s)) for q, s in results]
        
        return []
    
    @with_retry()
    def get_zero_result_queries(
        self,
        days: int = 7,
        limit: int = 100
    ) -> List[Tuple[str, int]]:
        """
        Get queries that returned zero results (content gaps).
        
        This is GOLD for content strategy.
        
        Args:
            days: Look back period (0 for all-time)
            limit: Number of results
            
        Returns:
            List of (query, count) tuples
        """
        if self.redis is None:
            return []
        
        if days == 0:
            results = self.redis.zrevrange("analytics:searches:zero_results", 0, limit - 1, withscores=True)
            return [(q, int(s)) for q, s in results]
        
        keys = []
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            keys.append(f"analytics:searches:zero_results:{date}")
        
        temp_key = f"temp:zero_results:{time.time()}"
        existing_keys = [k for k in keys if self.redis.exists(k)]
        if existing_keys:
            self.redis.zunionstore(temp_key, existing_keys)
            results = self.redis.zrevrange(temp_key, 0, limit - 1, withscores=True)
            self.redis.delete(temp_key)
            return [(q, int(s)) for q, s in results]
        
        return []
    
    @with_retry()
    def compare_alt_modes(self, days: int = 7) -> Dict:
        """
        Compare dropdown (alt_mode=y) vs semantic (alt_mode=n) performance.
        
        Returns:
            Comparison stats for both modes
        """
        if self.redis is None:
            return {}
        
        stats = {
            'dropdown': {'searches': 0, 'total_results': 0, 'total_time_ms': 0, 'avg_results': 0, 'avg_time_ms': 0},
            'semantic': {'searches': 0, 'total_results': 0, 'total_time_ms': 0, 'avg_results': 0, 'avg_time_ms': 0}
        }
        
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            day_stats = self.redis.hgetall(f"analytics:alt_mode_stats:{date}")
            
            if day_stats:
                # Dropdown (alt_mode=y)
                stats['dropdown']['searches'] += int(day_stats.get('mode_y_count', 0))
                stats['dropdown']['total_results'] += int(day_stats.get('mode_y_results', 0))
                stats['dropdown']['total_time_ms'] += int(day_stats.get('mode_y_time_ms', 0))
                
                # Semantic (alt_mode=n)
                stats['semantic']['searches'] += int(day_stats.get('mode_n_count', 0))
                stats['semantic']['total_results'] += int(day_stats.get('mode_n_results', 0))
                stats['semantic']['total_time_ms'] += int(day_stats.get('mode_n_time_ms', 0))
        
        # Calculate averages
        for mode in ['dropdown', 'semantic']:
            if stats[mode]['searches'] > 0:
                stats[mode]['avg_results'] = round(stats[mode]['total_results'] / stats[mode]['searches'], 2)
                stats[mode]['avg_time_ms'] = round(stats[mode]['total_time_ms'] / stats[mode]['searches'], 2)
        
        return stats
    
    @with_retry()
    def get_session_stats(self, session_id: str) -> Dict:
        """
        Get comprehensive stats for a session.
        
        Returns:
            Session data with searches, clicks, duration, etc.
        """
        if self.redis is None:
            return {}
        
        session = self.get_session(session_id)
        if not session:
            return {}
        
        # Get searches
        searches_raw = self.redis.lrange(f"session:{session_id}:searches", 0, -1)
        searches = []
        for s in searches_raw:
            try:
                searches.append(json.loads(s))
            except json.JSONDecodeError:
                pass
        
        # Get clicks
        clicks_raw = self.redis.lrange(f"session:{session_id}:clicks", 0, -1)
        clicks = []
        for c in clicks_raw:
            try:
                clicks.append(json.loads(c))
            except json.JSONDecodeError:
                pass
        
        # Get all events
        events_raw = self.redis.lrange(f"session:{session_id}:events", 0, -1)
        events = []
        for e in events_raw:
            try:
                events.append(json.loads(e))
            except json.JSONDecodeError:
                pass
        
        # Calculate click-through rate
        ctr = len(clicks) / len(searches) if searches else 0
        
        return {
            'session': session,
            'searches': searches,
            'clicks': clicks,
            'events': events,
            'search_count': len(searches),
            'click_count': len(clicks),
            'click_through_rate': round(ctr, 3),
            'duration_minutes': session.get('duration_minutes', 0),
            'unique_queries': len(set(s.get('query_normalized', '') for s in searches))
        }
    
    @with_retry()
    def get_location_stats(self, days: int = 7) -> Dict:
        """
        Get search volume by location.
        
        Returns:
            Location breakdown with countries and cities
        """
        if self.redis is None:
            return {}
        
        countries = {}
        cities = {}
        
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            
            # Countries
            country_data = self.redis.zrevrange(f"analytics:searches:by_country:{date}", 0, 49, withscores=True)
            for country, count in country_data:
                countries[country] = countries.get(country, 0) + int(count)
            
            # Cities
            city_data = self.redis.zrevrange(f"analytics:searches:by_city:{date}", 0, 49, withscores=True)
            for city, count in city_data:
                cities[city] = cities.get(city, 0) + int(count)
        
        return {
            'countries': sorted(countries.items(), key=lambda x: x[1], reverse=True)[:20],
            'cities': sorted(cities.items(), key=lambda x: x[1], reverse=True)[:50]
        }
    
    @with_retry()
    def get_daily_stats(self, days: int = 30) -> List[Dict]:
        """
        Get daily aggregated statistics.
        
        Returns:
            List of daily stat dicts
        """
        if self.redis is None:
            return []
        
        daily_stats = []
        
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            stats = self.redis.hgetall(f"analytics:daily:{date}")
            
            if stats:
                try:
                    total_sessions = int(stats.get('total_sessions', 0))
                    total_duration = int(stats.get('total_duration_seconds', 0))
                    total_searches = int(stats.get('total_searches', 0))
                    total_clicks = int(stats.get('total_clicks', 0))
                    
                    daily_stats.append({
                        'date': date,
                        'sessions': total_sessions,
                        'searches': total_searches,
                        'clicks': total_clicks,
                        'avg_session_duration_seconds': round(total_duration / total_sessions, 1) if total_sessions else 0,
                        'searches_per_session': round(total_searches / total_sessions, 2) if total_sessions else 0,
                        'click_through_rate': round(total_clicks / total_searches, 3) if total_searches else 0,
                        'logged_in_sessions': int(stats.get('logged_in_sessions', 0)),
                        'anonymous_sessions': int(stats.get('anonymous_sessions', 0))
                    })
                except (TypeError, ValueError) as e:
                    logger.warning(f"Error parsing daily stats for {date}: {e}")
        
        return daily_stats
    
    @with_retry()
    def get_click_position_distribution(self, days: int = 7) -> Dict[str, int]:
        """
        Get distribution of click positions.
        
        Shows if users are clicking top results or scrolling down.
        """
        if self.redis is None:
            return {}
        
        distribution = {}
        
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            day_data = self.redis.hgetall(f"analytics:click_positions:{date}")
            
            for position, count in day_data.items():
                try:
                    distribution[position] = distribution.get(position, 0) + int(count)
                except ValueError:
                    pass
        
        # Sort by position number
        def sort_key(item):
            try:
                return int(item[0].split('_')[1])
            except (IndexError, ValueError):
                return 999
        
        return dict(sorted(distribution.items(), key=sort_key))
    
    @with_retry()
    def get_user_stats(self, user_id: str) -> Dict:
        """
        Get stats for a specific logged-in user.
        
        Returns:
            User's search history, preferences, and behavior
        """
        if self.redis is None:
            return {}
        
        # Get user's sessions
        sessions = self.redis.lrange(f"analytics:user:{user_id}:sessions", 0, 99)
        
        # Get user's searches
        searches_raw = self.redis.lrange(f"analytics:user:{user_id}:searches", 0, 999)
        searches = []
        for s in searches_raw:
            try:
                searches.append(json.loads(s))
            except json.JSONDecodeError:
                pass
        
        # Get user's clicks
        clicks_raw = self.redis.lrange(f"analytics:user:{user_id}:clicks", 0, 499)
        clicks = []
        for c in clicks_raw:
            try:
                clicks.append(json.loads(c))
            except json.JSONDecodeError:
                pass
        
        # Analyze search patterns
        query_counts = {}
        for search in searches:
            q = search.get('query', '').lower()
            query_counts[q] = query_counts.get(q, 0) + 1
        
        top_queries = sorted(query_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        
        return {
            'user_id': user_id,
            'total_sessions': len(sessions),
            'total_searches': len(searches),
            'total_clicks': len(clicks),
            'top_queries': top_queries,
            'recent_searches': searches[:20],
            'recent_clicks': clicks[:20],
            'click_through_rate': round(len(clicks) / len(searches), 3) if searches else 0
        }
    
    @with_retry()
    def get_search_strategy_stats(self, days: int = 7) -> Dict:
        """
        Get stats by search strategy (semantic, strict, mixed, fallback).
        
        Helps understand which strategy performs best.
        """
        if self.redis is None:
            return {}
        
        strategies = {}
        
        for i in range(days):
            date = (datetime.utcnow() - timedelta(days=i)).strftime('%Y-%m-%d')
            day_data = self.redis.hgetall(f"analytics:strategy:{date}")
            
            for key, value in day_data.items():
                if '_results' not in key:
                    strategy = key
                    try:
                        count = int(value)
                        results = int(day_data.get(f"{strategy}_results", 0))
                        
                        if strategy not in strategies:
                            strategies[strategy] = {'count': 0, 'total_results': 0}
                        
                        strategies[strategy]['count'] += count
                        strategies[strategy]['total_results'] += results
                    except ValueError:
                        pass
        
        # Calculate averages
        for strategy in strategies:
            if strategies[strategy]['count'] > 0:
                strategies[strategy]['avg_results'] = round(
                    strategies[strategy]['total_results'] / strategies[strategy]['count'], 2
                )
        
        return strategies
    
    # ========================================================================
    # CLEANUP & MAINTENANCE
    # ========================================================================
    
    @with_retry()
    def cleanup_old_data(self, days_to_keep: int = None) -> Dict:
        """
        Remove analytics data older than retention period.
        
        Run this periodically (e.g., daily cron job).
        
        Args:
            days_to_keep: Override default retention period
            
        Returns:
            Dict with count of deleted keys
        """
        if self.redis is None:
            return {'deleted_keys': 0, 'error': 'Redis not available'}
        
        days_to_keep = days_to_keep or ANALYTICS_RETENTION_DAYS
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Find and delete old daily keys
        patterns = [
            "analytics:searches:daily:*",
            "analytics:searches:zero_results:*",
            "analytics:alt_mode:*",
            "analytics:alt_mode_stats:*",
            "analytics:daily:*",
            "analytics:clicks:daily:*",
            "analytics:click_positions:*",
            "analytics:ctr:*",
            "analytics:events:*",
            "analytics:hourly:*",
            "analytics:intent:*",
            "analytics:searches:by_country:*",
            "analytics:searches:by_city:*",
            "analytics:source_clicks:*",
            "analytics:strategy:*",
            "analytics:time_to_click:*",
            "analytics:user_type:*",
            "analytics:sessions:daily:*",
            "analytics:devices:*",
            "analytics:browsers:*",
            "analytics:os:*"
        ]
        
        deleted_count = 0
        for pattern in patterns:
            try:
                for key in self.redis.scan_iter(pattern):
                    # Extract date from key
                    parts = key.split(':')
                    for part in parts:
                        if len(part) == 10 and part.count('-') == 2:  # Looks like a date
                            try:
                                key_date = datetime.strptime(part, '%Y-%m-%d')
                                if key_date < cutoff_date:
                                    self.redis.delete(key)
                                    deleted_count += 1
                            except ValueError:
                                pass
            except RedisError as e:
                logger.warning(f"Error during cleanup for pattern {pattern}: {e}")
        
        logger.info(f"Cleanup complete: deleted {deleted_count} old analytics keys")
        return {'deleted_keys': deleted_count}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_analytics() -> SearchAnalytics:
    """Get a SearchAnalytics instance."""
    return SearchAnalytics()


# ============================================================================
# USAGE EXAMPLES (for reference)
# ============================================================================

"""
# In your Django view:

from redis_analytics import SearchAnalytics

analytics = SearchAnalytics()

# Check health first
if not analytics.is_healthy():
    logger.warning("Analytics system unhealthy")

# On page load
analytics.start_session(
    session_id=request.session.session_key,
    user_id=request.user.id if request.user.is_authenticated else None,
    device_type=detect_device(request),
    user_agent=request.META.get('HTTP_USER_AGENT'),
    ip_address=get_client_ip(request),
    location=get_location_from_ip(get_client_ip(request)),
    referrer=request.META.get('HTTP_REFERER')
)

# On search
analytics.track_search(
    session_id=request.session.session_key,
    query=query,
    results_count=len(results),
    alt_mode=request.GET.get('alt_mode', 'n'),
    user_id=request.user.id if request.user.is_authenticated else None,
    location={'city': 'Atlanta', 'country': 'US'},
    device_type='mobile',
    search_time_ms=search_response['search_time'] * 1000,
    search_strategy=search_response['search_strategy'],
    corrected_query=search_response.get('corrected_query'),
    intent=search_response.get('intent')
)

# On result click
analytics.track_click(
    session_id=request.session.session_key,
    query=request.GET.get('q'),
    clicked_url=request.GET.get('url'),
    clicked_position=int(request.GET.get('position', 0)),
    result_id=request.GET.get('id'),
    user_id=request.user.id if request.user.is_authenticated else None
)

# Get insights
popular = analytics.get_popular_queries(days=7, limit=100)
gaps = analytics.get_zero_result_queries(days=7, limit=50)
alt_comparison = analytics.compare_alt_modes(days=7)

# Health check endpoint
health = analytics.health_check()
"""