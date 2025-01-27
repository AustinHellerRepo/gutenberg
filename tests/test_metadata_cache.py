# pylint: disable=C0111
# pylint: disable=R0904


import os
import sys
import tempfile
import unittest
from urllib.request import pathname2url
import psycopg2

from gutenberg.acquire.metadata import CacheAlreadyExistsException
from gutenberg.acquire.metadata import InvalidCacheException
from gutenberg.acquire.metadata import FusekiMetadataCache
from gutenberg.acquire.metadata import PostgresMetadataCache
from gutenberg.acquire.metadata import SqliteMetadataCache
from gutenberg.acquire.metadata import set_metadata_cache
from gutenberg.query import get_metadata
from tests._util import always_throw


# noinspection PyPep8Naming,PyAttributeOutsideInit
class MetadataCache:
    def test_read_unpopulated_cache(self):
        set_metadata_cache(self.cache)
        try:
            get_metadata('title', 50405)
        except InvalidCacheException:
            pass

    def test_initialize(self):
        # Simply creating the cache shouldn't create on-disk structures
        self.assertFalse(self.exists)

    def test_populate(self):
        self.cache.populate()
        set_metadata_cache(self.cache)
        title = get_metadata('title', 30929)
        self.assertIn('Het loterijbriefje', title)

    def test_repopulate(self):
        self.cache.populate()
        set_metadata_cache(self.cache)
        self.cache.delete()
        self.cache.populate()
        title = get_metadata('title', 30929)
        self.assertIn('Het loterijbriefje', title)

    def test_refresh(self):
        self.cache.populate()
        set_metadata_cache(self.cache)
        title = get_metadata('title', 30929)
        self.assertIn('Het loterijbriefje', title)

        self.cache.refresh()
        title = get_metadata('title', 30929)
        self.assertIn('Het loterijbriefje', title)

    def test_repopulate_without_delete(self):
        # Trying to populate an existing cache should raise an exception
        self.cache.populate()
        try:
            self.cache.populate()
        except CacheAlreadyExistsException:
            pass

    def test_delete(self):
        self.assertFalse(self.exists)
        self.cache.populate()
        self.assertTrue(self.exists)
        self.cache.delete()
        self.assertFalse(self.exists)

    def test_read_deleted_cache(self):
        self.cache.populate()
        set_metadata_cache(self.cache)
        self.cache.delete()
        try:
            get_metadata('title', 50405)
        except InvalidCacheException:
            pass

    def tearDown(self):
        set_metadata_cache(None)
        if self.cache.is_open:
            self.cache.delete()
        self.cache = None


class TestFuseki(MetadataCache, unittest.TestCase):
    def setUp(self):
        cache_url = os.getenv('UNIT_TEST_GUTENBERG_FUSEKI_URL')
        if not cache_url:
            raise unittest.SkipTest('Fuseki URL not set')

        self.local_storage = "%s.url" % tempfile.mktemp()
        self.cache = FusekiMetadataCache(self.local_storage, cache_url)
        self.cache.catalog_source = _sample_metadata_catalog_source()

    @property
    def exists(self):
        return self.cache.exists


class TestPostgresMetadataCache(MetadataCache, unittest.TestCase):
    def setUp(self):
        self.connection_string = "postgresql://gutenberg_user:gutenberg_password@localhost:5434/gutenberg_db"

        # remove database structures if they happen to exist from previous runs failing
        connection = psycopg2.connect(self.connection_string)
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS gutenberg.cache;")
            cursor.execute("DROP SCHEMA IF EXISTS gutenberg;")
            connection.commit()

        self.cache = PostgresMetadataCache(TestPostgresMetadataCache.__name__, self.connection_string)
        self.cache.catalog_source = _sample_metadata_catalog_source()

    @property
    def exists(self):
        return self.cache.exists


class TestSqlite(MetadataCache, unittest.TestCase):
    def setUp(self):
        raise unittest.SkipTest('TODO remove SkipTest exception in %s class %s function' % (TestSqlite.__name__, TestSqlite.setUp.__name__))

        self.local_storage = "%s.sqlite" % tempfile.mktemp()
        self.cache = SqliteMetadataCache(self.local_storage)
        self.cache.catalog_source = _sample_metadata_catalog_source()

    @property
    def exists(self):
        return self.cache.exists

    def test_add_does_not_swallow_exceptions(self):
        original_add = self.cache.graph.add
        self.cache.graph.add = always_throw(IOError)
        try:
            with self.assertRaises(IOError):
                self.test_populate()
        finally:
            self.cache.graph.add = original_add


def _sample_metadata_catalog_source():
    module = os.path.dirname(sys.modules['tests'].__file__)
    path = os.path.join(module, 'data', 'sample-rdf-files.tar.bz2')
    return 'file://%s' % pathname2url(path)


if __name__ == '__main__':
    unittest.main()
