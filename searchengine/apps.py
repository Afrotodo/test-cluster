# from django.apps import AppConfig


# class SearchengineConfig(AppConfig):
#     name = 'searchengine'


from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class SearchengineConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'searchengine'
    
    def ready(self):
        import sys
        import os
        if 'runserver' in sys.argv and os.environ.get('RUN_MAIN') != 'true':
            return
        
        try:
            from .vocabulary_cache import ensure_loaded, get_cache_status
            
            success = ensure_loaded()
            
            if success:
                status = get_cache_status()
                logger.info(f"Cache loaded: {status['term_count']:,} terms")
            else:
                logger.warning("Cache not loaded - waiting for Colab data")
        
        except Exception as e:
            logger.error(f"Cache load error: {e}")