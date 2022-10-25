from gutenberg.query import get_etexts
from gutenberg.acquire import set_metadata_cache
from gutenberg.acquire.metadata import PostgresMetadataCache


connection_string = "postgresql://gutenberg_user:gutenberg_password@localhost:5434/gutenberg_db"
cache = PostgresMetadataCache("test_setmetadatacache", connection_string)
if not cache.exists:
    raise Exception("Expected database to already be populated but was empty.")
set_metadata_cache(cache)
print(get_etexts("author", "Melville, Hermann"))
