# import os
# import sys
# import django
# import json
# import typesense
# from datetime import datetime

# # Setup Django environment
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'afrotodosearch.settings')
# django.setup()

# # Initialize Typesense client with your cloud credentials


# documents = {
#     "name": "documents",
    
#     "fields": [
      
#         # === CORE IDENTIFICATION ===
#         {"name": "document_uuid", "type": "string"},
#         {"name": "semantic_uuid", "type": "string"},
#         {"name": "cluster_uuid", "type": "string"},

#         # === MEDIA ASSETS ===
#         {"name": "image_url", "type": "string[]", "optional": True},
#         {"name": "video_url", "type": "string[]", "optional": True},
#         {"name": "logo_url", "type": "string[]", "optional": True},
#         {"name": "social_media", "type": "string[]", "optional": True},

#         # === TEMPORAL (Labels + Numeric Range) ===
#         {"name": "time_context", "type": "string", "facet": True, "optional": True},           # "1990s", "2020s contemporary"
#         {"name": "temporal_relevance", "type": "string", "facet": True, "optional": True},     # current, historical, timeless
#         {"name": "time_period_start", "type": "int32", "optional": True},                      # -3000 for BCE, 1850 for CE
#         {"name": "time_period_end", "type": "int32", "optional": True},                        # enables range queries

#         # === LOCATION ===
#         {"name": "location_city", "type": "string", "facet": True, "optional": True},
#         {"name": "location_state", "type": "string", "facet": True, "optional": True},
#         {"name": "location_country", "type": "string", "facet": True, "optional": True},
#         {"name": "location_region", "type": "string", "facet": True, "optional": True},
#         {"name": "location_coordinates", "type": "geopoint", "optional": True},                # changed from string
#         {"name": "location_address", "type": "string", "optional": True},
#         {"name": "location_geopoint", "type": "geopoint", "optional": True},

#         # === DOCUMENT CORE ===
#         {"name": "document_title", "type": "string", "facet": True},
#         {"name": "document_summary", "type": "string"},
#         {"name": "document_category", "type": "string", "facet": True},
#         {"name": "document_data_type", "type": "string", "facet": True},                       # article, video, product, service, person, media
#         {"name": "document_schema", "type": "string", "facet": True},
#         {"name": "document_author", "type": "string", "facet": True, "optional": True},
#         {"name": "document_url", "type": "string"},
#         {"name": "document_brand", "type": "string", "facet": True, "optional": True},         # source: wikipedia, youtube, instagram, etc.

#         # === KEYWORDS (Tiered) ===
#         {"name": "primary_keywords", "type": "string[]", "facet": True},                       # 3-5 core terms, highest weight
#         {"name": "keywords", "type": "string[]", "facet": True},                               # secondary/entity keywords
#         {"name": "semantic_keywords", "type": "string[]", "facet": True},                      # conceptual themes
#         {"name": "key_passages", "type": "string[]", "optional": True},                        # searchable excerpts

#         # === DATES ===
#         {"name": "published_date", "type": "int64", "sort": True, "optional": True},
#         {"name": "published_date_string", "type": "string", "index": False, "optional": True},
#         {"name": "created_at", "type": "int64"},

#         # === ENTITIES ===
#         {"name": "entity_names", "type": "string[]", "facet": True, "optional": True},         # changed to array

#         # === RANKING SIGNALS ===
#         {"name": "authority_score", "type": "float", "optional": True},                        # 0-100, for boosting
#         {"name": "content_depth", "type": "int32", "optional": True},                          # word count or depth indicator

#         # === PRODUCT ===
#         {"name": "product_category", "type": "string", "facet": True, "optional": True},
#         {"name": "product_price", "type": "float", "optional": True},
#         {"name": "product_currency", "type": "string", "facet": True, "optional": True},
#         {"name": "product_rating", "type": "float", "optional": True},                         # fixed: was inconsistent
#         {"name": "product_available", "type": "bool", "facet": True, "optional": True},
#         {"name": "product_features", "type": "string[]", "facet": True, "optional": True},

#         # === PERSON ===
#         {"name": "person_full_name", "type": "string", "optional": True},
#         {"name": "person_birth_date", "type": "string", "optional": True},
#         {"name": "person_gender", "type": "string", "facet": True, "optional": True},
#         {"name": "person_race", "type": "string", "facet": True, "optional": True},
#         {"name": "person_positions", "type": "string[]", "facet": True, "optional": True},
#         {"name": "person_education", "type": "string[]", "optional": True},
#         {"name": "person_achievements", "type": "string[]", "optional": True},

#         # === MEDIA (movies, music, videos) ===
#         {"name": "media_author_creator", "type": "string", "facet": True, "optional": True},
#         {"name": "media_genre", "type": "string[]", "facet": True, "optional": True},
#         {"name": "media_duration", "type": "string", "optional": True},
#         {"name": "media_release_date", "type": "string", "facet": True, "optional": True},
#         {"name": "media_language", "type": "string", "facet": True, "optional": True},
#         {"name": "media_rating", "type": "float", "optional": True},                           # fixed: float

#         # === SERVICE ===
#         {"name": "service_type", "type": "string[]", "facet": True, "optional": True},
#         {"name": "service_specialties", "type": "string[]", "facet": True, "optional": True},
#         {"name": "service_phone", "type": "string", "optional": True},
#         {"name": "service_rating", "type": "float", "optional": True},                         # fixed: float

#         # === STATUS & FLAGS ===
#         {"name": "status", "type": "string", "facet": True, "optional": True},
#         {"name": "black_owned", "type": "bool", "facet": True, "optional": True},              # changed to bool
#         {"name": "rating", "type": "float", "optional": True},                                 # fixed: float
#     ]
# }

# # Function to check if collection exists and create if it doesn't
# def create_collection_if_not_exists(schema):
#     try:
#         # Check if collection already exists
#         collections = client.collections.retrieve()
#         collection_names = [c['name'] for c in collections]
        
#         if schema['name'] in collection_names:
#             print(f"Collection '{schema['name']}' already exists. Skipping creation.")
#             return False
        
#         # Create the collection
#         response = client.collections.create(schema)
#         print(f"Collection '{schema['name']}' created successfully.")
#         return True
#     except Exception as e:
#         print(f"Error with collection '{schema['name']}': {e}")
#         return False

# # Create all collections only if they don't exist
# print("\nAttempting to create collections...")


# schemas = [
#     documents 
# ]


# for schema in schemas:
#     create_collection_if_not_exists(schema)

# # Verify collections
# try:
#     collections = client.collections.retrieve()
#     print("\nAvailable collections:")
#     for collection in collections:
#         print(f" - {collection['name']} ({collection.get('num_documents', 0)} documents)")
# except Exception as e:
#     print(f"Error retrieving collections: {e}")

# print("\nSchema update completed.")