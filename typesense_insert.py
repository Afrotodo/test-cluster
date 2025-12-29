# import typesense


# documents = [
#     # {
#     #     "document_uuid": "wiki_africa_001",
#     #     "semantic_uuid": "sem_africa_001",
#     #     "cluster_uuid": "cluster_africa",
        
#     #     "image_url": ["https://upload.wikimedia.org/wikipedia/commons/thumb/8/86/Africa_%28orthographic_projection%29.svg/550px-Africa_%28orthographic_projection%29.svg.png"],
#     #     "video_url": [],
#     #     "logo_url": [],
#     #     "social_media": [],
        
#     #     "time_context": "prehistoric to 2020s",
#     #     "temporal_relevance": "timeless",
#     #     "time_period_start": -3000,
#     #     "time_period_end": 2024,
        
#     #     "location_region": "Africa",
#     #     "location_geopoint": [1.6508, 17.7707],
        
#     #     "document_title": "Africa",
#     #     "document_summary": "Africa is the world's second-largest and second-most populous continent after Asia. At about 30.3 million km2 including adjacent islands, it covers 20% of Earth's land area and 6% of its total surface area. With nearly 1.4 billion people as of 2021, it accounts for about 18% of the world's human population.",
#     #     "document_category": "geography",
#     #     "document_data_type": "article",
#     #     "document_schema": "encyclopedia",
#     #     "document_author": "Wikipedia Contributors",
#     #     "document_url": "https://en.wikipedia.org/wiki/Africa",
#     #     "document_brand": "wikipedia",
        
#     #     "primary_keywords": ["africa", "continent", "african"],
#     #     "keywords": ["egypt", "nigeria", "algeria", "ethiopia", "kenya", "sahara", "nile", "mediterranean", "atlantic", "indian ocean", "african union", "colonialism", "decolonization"],
#     #     "semantic_keywords": ["geography", "history", "population", "culture", "wildlife", "colonization", "independence", "economy", "climate", "biodiversity"],
#     #     "key_passages": [
#     #         "Africa is the world's second-largest and second-most populous continent after Asia",
#     #         "With nearly 1.4 billion people as of 2021, it accounts for about 18% of the world's human population",
#     #         "Africa is considered by most paleoanthropologists to be the oldest inhabited territory on Earth",
#     #         "The continent includes 54 fully recognised sovereign states",
#     #         "Algeria is Africa's largest country by area, and Nigeria is its largest by population"
#     #     ],
        
#     #     "published_date": 1734652800,
#     #     "published_date_string": "2024-12-20",
#     #     "created_at": 1734652800,
        
#     #     "entity_names": ["Sahara", "Nile", "African Union", "Egypt", "Nigeria", "Algeria", "Ethiopia", "Kenya", "Mali Empire", "Songhai Empire"],
        
#     #     "authority_score": 85.0,
#     #     "content_depth": 15000,
        
#     #     "status": "active",
#     #     "black_owned": False
#     # },
#     # {
#     #     "document_uuid": "brit_africa_001",
#     #     "semantic_uuid": "sem_africa_002",
#     #     "cluster_uuid": "cluster_africa",
        
#     #     "image_url": ["https://cdn.britannica.com/57/5957-050-B132B7C6/Africa.jpg"],
#     #     "video_url": [],
#     #     "logo_url": [],
#     #     "social_media": [],
        
#     #     "time_context": "prehistoric to 2020s",
#     #     "temporal_relevance": "timeless",
#     #     "time_period_start": -3000,
#     #     "time_period_end": 2024,
        
#     #     "location_region": "Africa",
#     #     "location_geopoint": [1.6508, 17.7707],
        
#     #     "document_title": "Africa",
#     #     "document_summary": "Africa, the second largest continent, covering about one-fifth of the total land surface of Earth. The continent is bounded on the west by the Atlantic Ocean, on the north by the Mediterranean Sea, on the east by the Red Sea and the Indian Ocean, and on the south by the mingling waters of the Atlantic and Indian oceans.",
#     #     "document_category": "geography",
#     #     "document_data_type": "article",
#     #     "document_schema": "encyclopedia",
#     #     "document_author": "Britannica Editors",
#     #     "document_url": "https://www.britannica.com/place/Africa",
#     #     "document_brand": "britannica",
        
#     #     "primary_keywords": ["africa", "continent", "african"],
#     #     "keywords": ["egypt", "nigeria", "kenya", "sahara", "nile", "colonialism", "wildlife", "safari"],
#     #     "semantic_keywords": ["geography", "history", "culture", "wildlife", "tourism", "economy", "people", "land", "environment"],
#     #     "key_passages": [
#     #         "Africa is the second largest continent covering about one-fifth of the total land surface of Earth",
#     #         "The continent is bounded on the west by the Atlantic Ocean",
#     #         "Africa contains 54 independent countries"
#     #     ],
        
#     #     "published_date": 1718409600,
#     #     "published_date_string": "2024-06-15",
#     #     "created_at": 1718409600,
        
#     #     "entity_names": ["Sahara", "Nile", "Great Rift Valley", "Victoria Falls", "Serengeti"],
        
#     #     "authority_score": 95.0,
#     #     "content_depth": 5000,
        
#     #     "status": "active",
#     #     "black_owned": False
#     # }

# #     {
# #     "document_uuid": "32527c7f-6e9e-4456-b359-4ec2d8f2fa0e",
# #     "semantic_uuid": "8f082119-f682-4090-b17d-a66180237b7b",
# #     "cluster_uuid": "58e7fe0f-38c0-46b8-975b-e4fd0feb650e",

# #     "image_url": [],
# #     "video_url": [],
# #     "logo_url": ["https://www.trustblackwomen.org/files/images/stories/tbw300smaller.png"],
# #     "social_media": ["https://www.facebook.com/trustblackwomen", "https://twitter.com/#!/NotOnOurWatch"],

# #     "time_context": "2010s to 2020s",
# #     "temporal_relevance": "current",
# #     "time_period_start": 2010,
# #     "time_period_end": 2025,

# #     "location_city": None,
# #     "location_state": None,
# #     "location_country": "United States",
# #     "location_region": "North America",

# #     "document_title": "Trust Black Women",
# #     "document_summary": "Trust Black Women (TBW) is a partnership formed in response to political campaigns that undermine the dignity and reproductive rights of Black women. Established in 2010, TBW unites women from diverse backgrounds to advocate for reproductive justice, emphasizing the right to make personal health decisions. The organization challenges narratives that seek to control Black women's reproductive choices, highlighting the historical context of oppression and the need for self-determination. TBW aims to support Black women in accessing reproductive health services while combating misinformation and societal pressures. The partnership calls for community support to counteract the financial and media resources of opponents who perpetuate harmful stereotypes and agendas against Black women.",
# #     "document_category": "organization",
# #     "document_data_type": "article",
# #     "document_schema": "firecrawl",
# #     "document_author": None,
# #     "document_url": "https://www.trustblackwomen.org/",
# #     "document_brand": "trustblackwomen",

# #     "primary_keywords": ["trust black women", "reproductive justice", "black women"],
# #     "keywords": ["human rights", "abortion", "advocacy", "partnership", "reproductive rights", "self-determination"],
# #     "semantic_keywords": ["reproductive health", "social justice", "community support", "advocacy", "women's rights", "activism"],
# #     "key_passages": [
# #         "Trust Black Women is a partnership formed in response to political campaigns that undermine the dignity and reproductive rights of Black women",
# #         "Established in 2010, TBW unites women from diverse backgrounds to advocate for reproductive justice",
# #         "The organization challenges narratives that seek to control Black women's reproductive choices"
# #     ],

# #     "published_date": 1730699730,
# #     "published_date_string": "2025-11-04",
# #     "created_at": 1730699730,

# #     "entity_names": ["Trust Black Women", "TBW"],

# #     "authority_score": 70.0,
# #     "content_depth": 500,

# #     "service_type": ["advocacy", "reproductive health"],
# #     "service_specialties": ["reproductive justice", "community organizing", "education"],

# #     "status": "active",
# #     "black_owned": True
# # }



# # Document 1: Arnold Bennett Donawa
# {
#     "document_uuid": "56974227-f6f7-4cb7-be16-91c2525d8c97",
#     "semantic_uuid": "0e257f30-93be-4211-8cee-db58895732c2",
#     "cluster_uuid": "219d3319-b8b9-4710-a1d9-266e68ee8465",

#     "image_url": ["https://blackpast.org/wp-content/uploads/2024/08/Albin_Ragner_and_Dr_Arnold_Donowa_1938.jpg"],
#     "video_url": [],
#     "logo_url": [],
#     "social_media": [],

#     "time_context": "20th century",
#     "temporal_relevance": "historical",
#     "time_period_start": 1895,
#     "time_period_end": 1966,

#     "location_city": "Washington",
#     "location_state": "D.C.",
#     "location_country": "United States",
#     "location_region": "North America",

#     "document_title": "Arnold Bennett Donawa (1895-1966)",
#     "document_summary": "Arnold Bennett Donawa (1895-1966) was a Trinidad-born dental surgeon and Spanish Civil War veteran. He earned his D.D.S. from Howard University in 1922 and worked in various dental institutions before becoming the first dean of Howard's College of Dentistry. Donawa joined the Communist Party in 1934 and coordinated medical aid for Ethiopia before volunteering in the Spanish Civil War. He was wounded in Spain but became a prominent figure in the American press. After returning to the U.S., he campaigned for the Spanish Republic and protested against anti-Semitic quotas in dental schools. He retired in the late 1950s and returned to Trinidad, where he died in 1964. Donawa was a significant leader among African American dentists and a symbol against fascism.",
#     "document_category": "biography",
#     "document_data_type": "article",
#     "document_schema": "History & Facts",
#     "document_author": "Edward Mikkelsen Jr.",
#     "document_url": "https://blackpast.org/african-american-history/donawa-arnold-bennett-1896-196/",
#     "document_brand": "blackpast",

#     "primary_keywords": ["arnold bennett donawa", "howard university", "spanish civil war"],
#     "keywords": ["communist party", "ethiopia", "oral surgery", "african american history", "trinidad", "dentistry"],
#     "semantic_keywords": ["african american dentists", "fascism", "medical aid", "political activism", "civil rights"],
#     "key_passages": [
#         "Arnold Bennett Donawa was a Trinidad-born dental surgeon and Spanish Civil War veteran",
#         "He became the first dean of Howard's College of Dentistry",
#         "Donawa was a significant leader among African American dentists and a symbol against fascism"
#     ],

#     "published_date": 1168992000,
#     "published_date_string": "2007-01-17",
#     "created_at": 1730958276,

#     "entity_names": ["Arnold Donawa", "Francisco Franco", "Albin Ragner", "Howard University"],

#     "authority_score": 80.0,
#     "content_depth": 800,

#     "person_full_name": "Arnold Bennett Donawa",
#     "person_birth_date": "1895",
#     "person_gender": "male",
#     "person_race": "black",
#     "person_positions": ["dental surgeon", "first dean of Howard's College of Dentistry", "Head of Oral Surgery"],
#     "person_education": ["Howard University D.D.S. 1922"],
#     "person_achievements": [
#         "First dean of Howard University's reorganized College of Dentistry",
#         "Coordinated medical aid for Ethiopia",
#         "Served as Head of Oral Surgery in the Spanish Civil War",
#         "Led protests against anti-Semitic dental school quotas"
#     ],

#     "status": "active",
#     "black_owned": False
# },

# # Document 2: Andrew Bryan
# {
#     "document_uuid": "b02c6fba-b545-4946-966a-876fd4593b43",
#     "semantic_uuid": "8532cef3-4035-4e3b-8054-a3199700a666",
#     "cluster_uuid": "d3b89211-c5e4-4e90-8847-fb25b73185dd",

#     "image_url": ["https://blackpast.org/wp-content/uploads/2024/08/Andrew_Bryan.jpg"],
#     "video_url": [],
#     "logo_url": [],
#     "social_media": [],

#     "time_context": "18th century",
#     "temporal_relevance": "historical",
#     "time_period_start": 1737,
#     "time_period_end": 1812,

#     "location_city": "Savannah",
#     "location_state": "Georgia",
#     "location_country": "United States",
#     "location_region": "North America",

#     "document_title": "Andrew Bryan (1737-1812)",
#     "document_summary": "Andrew Bryan (1737-1812) was a significant figure in the history of the First African Baptist Church in Savannah, Georgia, which is recognized as the oldest African American church in the U.S. He was baptized by George Liele in 1782 and became an ordained minister in 1788. Bryan led the church after Liele's evacuation during the Revolutionary War and remained its pastor until his death in 1812. His leadership contributed to the church's certification by the Georgia Baptist Association in 1790, five years before the establishment of the white Baptist Church in Savannah.",
#     "document_category": "biography",
#     "document_data_type": "article",
#     "document_schema": "History & Facts",
#     "document_author": "Turkiya Lowe",
#     "document_url": "https://blackpast.org/african-american-history/bryan-andrew-1737-1812/",
#     "document_brand": "blackpast",

#     "primary_keywords": ["andrew bryan", "first african baptist church", "savannah"],
#     "keywords": ["georgia", "george liele", "ordained minister", "african american history", "baptist"],
#     "semantic_keywords": ["african american church", "baptist church", "spiritual leadership", "evangelism", "religion"],
#     "key_passages": [
#         "Andrew Bryan was a significant figure in the history of the First African Baptist Church in Savannah",
#         "The First African Baptist Church is recognized as the oldest African American church in the U.S.",
#         "His leadership contributed to the church's certification by the Georgia Baptist Association in 1790"
#     ],

#     "published_date": 1168992000,
#     "published_date_string": "2007-01-17",
#     "created_at": 1730958276,

#     "entity_names": ["Andrew Bryan", "George Liele", "Jonathan Bryan", "First African Baptist Church", "Georgia Baptist Association"],

#     "authority_score": 80.0,
#     "content_depth": 600,

#     "person_full_name": "Andrew Bryan",
#     "person_birth_date": "1737",
#     "person_gender": "male",
#     "person_race": "black",
#     "person_positions": ["pastor", "ordained minister"],
#     "person_education": [],
#     "person_achievements": [
#         "First Colored Baptist Church established",
#         "Ordained minister in 1788",
#         "Led the church until his death in 1812"
#     ],

#     "status": "active",
#     "black_owned": False
# }
# ]

# # Insert documents
# for doc in documents:
#     try:
#         result = client.collections['documents'].documents.create(doc)
#         print(f"Inserted: {doc['document_uuid']}")
#     except Exception as e:
#         print(f"Error inserting {doc['document_uuid']}: {e}")

# print("Done.")

# # python typesense_insert.py