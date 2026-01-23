"""
API Key Authentication for n8n automations.
"""
import hashlib
import hmac
from typing import Optional, Tuple

from decouple import config
from rest_framework import authentication, exceptions
from rest_framework.request import Request


class APIKeyAuthentication(authentication.BaseAuthentication):
    """
    Custom authentication using API key in header.
    
    Usage in n8n:
        Header: X-API-Key: your-secret-key
    """
    
    HEADER_NAME = 'HTTP_X_API_KEY'
    
    def authenticate(self, request: Request) -> Optional[Tuple[None, str]]:
        """
        Authenticate the request and return a tuple of (None, api_key) on success.
        Returns None if no API key provided (allows other auth methods).
        Raises AuthenticationFailed if key is invalid.
        """
        api_key = request.META.get(self.HEADER_NAME)
        
        if not api_key:
            # No API key provided - let other authentication methods handle it
            # Or raise if you want to require API key
            return None
        
        if not self._validate_key(api_key):
            raise exceptions.AuthenticationFailed('Invalid API key.')
        
        # Return (user, auth) tuple - user is None since this is service-to-service
        return (None, api_key)
    
    def _validate_key(self, provided_key: str) -> bool:
        """
        Securely compare the provided key against the stored key.
        Uses constant-time comparison to prevent timing attacks.
        """
        stored_key = config('N8N_API_KEY', default='')
        
        if not stored_key:
            # No key configured - reject all requests
            return False
        
        # Constant-time comparison
        return hmac.compare_digest(
            provided_key.encode('utf-8'),
            stored_key.encode('utf-8')
        )
    
    def authenticate_header(self, request: Request) -> str:
        """
        Return a string to be used as the value of the WWW-Authenticate
        header in a 401 Unauthenticated response.
        """
        return 'API-Key'


def require_api_key(view_func):
    """
    Decorator for function-based views that require API key.
    
    Usage:
        @require_api_key
        def my_view(request):
            ...
    """
    from functools import wraps
    from django.http import JsonResponse
    
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        api_key = request.META.get('HTTP_X_API_KEY')
        stored_key = config('N8N_API_KEY', default='')
        
        if not api_key or not stored_key:
            return JsonResponse(
                {'error': 'API key required'},
                status=401
            )
        
        if not hmac.compare_digest(api_key.encode(), stored_key.encode()):
            return JsonResponse(
                {'error': 'Invalid API key'},
                status=401
            )
        
        return view_func(request, *args, **kwargs)
    
    return wrapper