"""Module to deal with metadata acquisition."""
# pylint:disable=W0603


import abc
import codecs
import logging
import os
import re
import shutil
import tarfile
import tempfile
from contextlib import closing
from contextlib import contextmanager
from urllib.request import urlopen
from typing import Dict, Tuple, Optional, Union, TextIO, BinaryIO, IO, List
import pathlib
import hashlib


from rdflib import plugin
from rdflib.graph import Graph, create_input_source
from rdflib.query import ResultException
from rdflib.store import Store, VALID_STORE
from rdflib.term import BNode
from rdflib.term import URIRef
from rdflib_sqlalchemy import registerplugins
from rdflib.parser import InputSource, Parser
from rdflib.exceptions import ParserError
from rdflib.paths import Path

from gutenberg._domain_model.exceptions import CacheAlreadyExistsException
from gutenberg._domain_model.exceptions import InvalidCacheException
from gutenberg._domain_model.persistence import local_path
from gutenberg._domain_model.vocabulary import DCTERMS
from gutenberg._domain_model.vocabulary import PGTERMS
from gutenberg._util.logging import disable_logging
from gutenberg._util.os import makedirs
from gutenberg._util.os import remove

import psycopg2
import psycopg2.errors
import psycopg2.extensions

_GUTENBERG_CATALOG_URL = \
    r'http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2'
_DB_IDENTIFIER = 'urn:gutenberg:metadata'
_DB_PATH = local_path(os.path.join('metadata', 'metadata.db'))


class MetadataCache(metaclass=abc.ABCMeta):
    """Super-class for all metadata cache implementations.

    """
    def __init__(self, store, cache_uri):
        self.store = store
        self.cache_uri = cache_uri
        if store is not None:
            self.graph = Graph(store=self.store, identifier=_DB_IDENTIFIER)
        self.is_open = False
        self.catalog_source = _GUTENBERG_CATALOG_URL

    @property
    def exists(self):
        """Detect if the cache exists.

        """
        return os.path.exists(self._local_storage_path)

    def open(self):
        """Opens an existing cache.

        """
        try:
            self.graph.open(self.cache_uri, create=False)
            self._add_namespaces(self.graph)
            self.is_open = True
        except Exception:
            raise InvalidCacheException('The cache is invalid or not created')

    def close(self):
        """Closes an opened cache.

        """
        self.graph.close()
        self.is_open = False

    def delete(self):
        """Delete the cache.

        """
        self.close()
        remove(self._local_storage_path)

    def populate(self):
        """Populates a new cache.

        """
        if self.exists:
            raise CacheAlreadyExistsException('location: %s' % self.cache_uri)

        self._populate_setup()

        with closing(self.graph):
            with self._download_metadata_archive() as metadata_archive:
                for fact in self._iter_metadata_triples(metadata_archive):
                    self._add_to_graph(fact)

    def _add_to_graph(self, fact):
        """Adds a (subject, predicate, object) RDF triple to the graph.

        """
        self.graph.add(fact)

    def _populate_setup(self):
        """Executes operations necessary before the cache can be populated.

        """
        pass

    def refresh(self):
        """Refresh the cache by deleting the old one and creating a new one.

        """
        if self.exists:
            self.delete()
        self.populate()
        self.open()

    @property
    def _local_storage_path(self):
        """Returns a path to the on-disk structure of the cache.

        """
        return self.cache_uri

    @staticmethod
    def _add_namespaces(graph):
        """Function to ensure that the graph always has some specific namespace
        aliases set.

        """
        graph.bind('pgterms', PGTERMS)
        graph.bind('dcterms', DCTERMS)

    @contextmanager
    def _download_metadata_archive(self):
        """Makes a remote call to the Project Gutenberg servers and downloads
        the entire Project Gutenberg meta-data catalog. The catalog describes
        the texts on Project Gutenberg in RDF. The function returns a
        file-pointer to the catalog.

        """
        with tempfile.NamedTemporaryFile(delete=False) as metadata_archive:
            shutil.copyfileobj(urlopen(self.catalog_source), metadata_archive)
        yield metadata_archive.name
        remove(metadata_archive.name)

    @classmethod
    def _metadata_is_invalid(cls, fact):
        """Determines if the fact is not well formed.

        """
        return any(isinstance(token, URIRef) and ' ' in token
                   for token in fact)

    @classmethod
    def _iter_metadata_triples(cls, metadata_archive_path):
        """Yields all meta-data of Project Gutenberg texts contained in the
        catalog dump.

        """
        pg_rdf_regex = re.compile(r'pg\d+.rdf$')
        with closing(tarfile.open(metadata_archive_path)) as metadata_archive:
            for item in metadata_archive:
                if pg_rdf_regex.search(item.name):
                    with disable_logging():
                        extracted = metadata_archive.extractfile(item)
                        graph = Graph().parse(extracted)
                    for fact in graph:
                        if cls._metadata_is_invalid(fact):
                            logging.info('skipping invalid triple %s', fact)
                        else:
                            yield fact


class PostgresSingle:

    def __init__(self, value: str):
        self.__value = value

    def toPython(self):
        return self.__value


class PostgresTuple:

    def __init__(self, tuple: Tuple[str, str, str]):
        self.__tuple = tuple

    def toPython(self):
        return self.__tuple


class PostgresTripleCollection(Store):

    def __init__(self, connection_string: str):
        self.__connection_string = connection_string
        self.__populate_connection = None
        self.is_closed = True
        super().__init__(connection_string)

    @staticmethod
    def __verify_row(row, query_parameters) -> bool:
        for index in range(3):
            if query_parameters[index] is not None:
                if query_parameters[index].toPython() != row[index]:
                    return False
        return True

    @staticmethod
    def __hash(value: str) -> int:
        return int.from_bytes(hashlib.sha256(str(value).encode()).digest()[:7], 'little')

    def triples(self, triple_pattern, context = None):
        connection = psycopg2.connect(self.__connection_string)
        try:
            s, p, o = triple_pattern
            # TODO optimize by using stored procedures per combination
            columns_detail_tuple_per_condition_name = {
                "subject": (0, s),
                "predicate": (1, p),
                "object": (2, o)
            }
            remaining_indexes = [0, 1, 2]
            query_parameters = []
            conditions_total = 0
            for condition_name, detail_tuple in columns_detail_tuple_per_condition_name.items():
                if detail_tuple[1] is not None:
                    condition = detail_tuple[1].toPython()
                    remaining_indexes.remove(detail_tuple[0])
                    hashed_condition = PostgresTripleCollection.__hash(condition)
                    query_parameters.append(hashed_condition)
                    conditions_total += 1
                else:
                    query_parameters.append(None)

            with connection.cursor("fetch") as cursor:
                cursor.itersize = 10000
                try:
                    cursor.execute(
                        "SELECT subject, predicate, object FROM gutenberg.cache WHERE COALESCE(%s, subject_hash) = subject_hash AND COALESCE(%s, predicate_hash) = predicate_hash AND COALESCE(%s, object_hash) = object_hash;",
                        query_parameters)
                except psycopg2.errors.UndefinedTable as ex:
                    if 'relation "gutenberg.cache" does not exist' in str(ex):
                        raise InvalidCacheException()
                    raise
                rows = cursor.fetchall()
                for row in rows:
                    yield (PostgresSingle(row[0]), PostgresSingle(row[1]), PostgresSingle(row[2])), None
        finally:
            connection.close()

    def open(self, configuration: str, create: bool = False) -> Optional[int]:
        # NOTE: not useable to change or set graph data source
        if self.__populate_connection is not None:
            raise Exception("Already opened connection")
        if not self.is_closed:
            raise Exception("Must first be closed before opening again")
        self.__populate_connection = psycopg2.connect(self.__connection_string)
        self.is_closed = False
        return VALID_STORE

    def close(self, commit_pending_transaction: bool = False):
        self.__populate_connection.close()
        self.__populate_connection = None
        self.is_closed = True

    def destroy(self, configuration):
        # TODO consider if anything should be done
        raise NotImplementedError()

    def add(self, triple, context, quoted: bool = False):
        # insert the data into the database
        subject, predicate, object = triple
        subject_hash = PostgresTripleCollection.__hash(subject.toPython())
        predicate_hash = PostgresTripleCollection.__hash(predicate.toPython())
        object_hash = PostgresTripleCollection.__hash(object.toPython())
        with self.__populate_connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO gutenberg.cache (subject, predicate, object, subject_hash, predicate_hash, object_hash) VALUES (%s, %s, %s, %s, %s, %s)",
                (subject, predicate, object, subject_hash, predicate_hash, object_hash)
            )
            self.__populate_connection.commit()

    def remove(self, triple, context = None):
        # remove the data into the database
        subject, predicate, object = triple
        if subject is None or predicate is None or object is None:
            raise Exception("Need to determine how to react to null triple values.")
        subject_hash = PostgresTripleCollection.__hash(subject.toPython())
        predicate_hash = PostgresTripleCollection.__hash(predicate.toPython())
        object_hash = PostgresTripleCollection.__hash(object.toPython())
        with self.__populate_connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gutenberg.cache WHERE subject_hash = %s AND predicate_hash = %s AND object_hash = %s);",
                (subject_hash, predicate_hash, object_hash)
            )
            self.__populate_connection.commit()

    def query(self, query, initNs, initBindings, queryGraph, **kwargs):
        raise NotImplementedError()

    def update(self, update, initNs, initBindings, queryGraph, **kwargs):
        raise NotImplementedError()


class PostgresMetadataCache(MetadataCache):

    def __init__(self, name: str, connection_string: str = None):
        MetadataCache.__init__(self, None, name)
        self.__connection_string = connection_string or os.getenv('GUTENBERG_POSTGRES_CONNECTIONSTRING')
        self.__graph = None  # type: Graph
        self.is_open = False

    @property
    def graph(self):
        return self.__graph

    @property
    def exists(self):
        connection = psycopg2.connect(self.__connection_string)
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'gutenberg';")
            connection.commit()
            tables_in_schema_total = cursor.fetchone()[0]
            return tables_in_schema_total != 0

    def open(self):
        if not self.exists:
            raise InvalidCacheException()
        # the graph may not be initialized yet, but it is intended for self.__graph to never be None
        if self.__graph is None:
            # NOTE: passing the connection string automatically opens PostgresTripleCollection
            self.__graph = Graph(PostgresTripleCollection(self.__connection_string))
        else:
            self.__graph.open(None)
        self.is_open = True

    def close(self):
        if self.__graph is None:
            raise Exception("Must first be opened before closing.")
        self.__graph.close()
        self.is_open = False

    def delete(self):
        connection = psycopg2.connect(self.__connection_string)
        try:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE gutenberg.cache;")
                cursor.execute("DROP SCHEMA gutenberg;")
                connection.commit()
        finally:
            connection.close()
        if self.is_open:
            self.close()

    # NOTE: populate does not need to be overridden

    def _add_to_graph(self, fact):
        self.__graph.store.add(fact, None)

    def _populate_setup(self):
        connection = psycopg2.connect(self.__connection_string)
        try:
            # TODO create necessary stored procedures
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS gutenberg;")
                cursor.execute("CREATE TABLE gutenberg.cache (subject TEXT, predicate TEXT, object TEXT, subject_hash BIGINT, predicate_hash BIGINT, object_hash BIGINT);")
                cursor.execute("CREATE INDEX IX_subject_predicate_object ON gutenberg.cache (subject_hash, predicate_hash, object_hash);")
                connection.commit()
        finally:
            connection.close()

        # set the internal "graph" object to be used externally as if it was an RDF graph
        self.__graph = Graph(PostgresTripleCollection(self.__connection_string))

    @property
    def _local_storage_path(self):
        raise NotImplementedError("This exception should never be encountered unless the overridden functions are not actually being called.")

    @staticmethod
    def _add_namespaces(graph):
        raise NotImplementedError("This exception should never be encountered unless the overridden functions are not actually being called.")

    # NOTE: _download_metadata_archive does not need to be overridden

    # NOTE: _metadata_is_invalid does not need to be overridden

    # NOTE: _iter_metadata_triples does not need to be overridden


class FusekiMetadataCache(MetadataCache):
    _CACHE_URL_PREFIXES = ('http://', 'https://')

    def __init__(self, cache_location, cache_url, user=None, password=None):
        self._check_can_be_instantiated(cache_url)
        store = 'SPARQLUpdateStore'
        MetadataCache.__init__(self, store, cache_url)
        user = user or os.getenv('GUTENBERG_FUSEKI_USER')
        password = password or os.getenv('GUTENBERG_FUSEKI_PASSWORD')
        self.graph.store.setCredentials(user, password)
        self._cache_marker = cache_location

    def _populate_setup(self):
        """Just create a local marker file since the actual database should
        already be created on the Fuseki server.

        """
        makedirs(os.path.dirname(self._cache_marker))
        with codecs.open(self._cache_marker, 'w', encoding='utf-8') as fobj:
            fobj.write(self.cache_uri)
        self.graph.open(self.cache_uri)

    def delete(self):
        """Deletes the local marker file and also any data in the Fuseki
        server.

        """
        MetadataCache.delete(self)
        try:
            self.graph.query('DELETE WHERE { ?s ?p ?o . }')
        except ResultException:
            # this is often just a false positive since Jena Fuseki does not
            # return tuples for a deletion query, so swallowing the exception
            # here is fine
            logging.exception('error when deleting graph')

    @property
    def _local_storage_path(self):
        """Returns the path to the local marker file that gets written when
        the cache was created.

        """
        return self._cache_marker

    @classmethod
    def _check_can_be_instantiated(cls, cache_location):
        """Pre-conditions: the cache location is the URL to a Fuseki server
        and the SPARQLWrapper library exists (transitive dependency of
        RDFlib's sparqlstore).

        """
        if not any(cache_location.startswith(prefix)
                   for prefix in cls._CACHE_URL_PREFIXES):
            raise InvalidCacheException('cache location is not a Fuseki url')

        try:
            from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
        except ImportError:
            raise InvalidCacheException('unable to import sparql store')
        del SPARQLUpdateStore

    @classmethod
    def _metadata_is_invalid(cls, fact):
        """Filters out blank nodes since the SPARQLUpdateStore does not
        support them.

        """
        return (MetadataCache._metadata_is_invalid(fact)
                or any(isinstance(token, BNode) for token in fact))


class SqliteMetadataCache(MetadataCache):
    """Cache manager based on SQLite and the RDFlib plugin for SQLAlchemy.
    Quite slow.

    """
    _CACHE_URI_PREFIX = 'sqlite:///'

    def __init__(self, cache_location):
        cache_uri = self._CACHE_URI_PREFIX + cache_location
        store = plugin.get('SQLAlchemy', Store)(identifier=_DB_IDENTIFIER)
        MetadataCache.__init__(self, store, cache_uri)

    def _populate_setup(self):
        self.graph.open(self.cache_uri, create=True)

    @property
    def _local_storage_path(self):
        return self.cache_uri[len(self._CACHE_URI_PREFIX):]

    def _add_to_graph(self, fact):
        try:
            self.graph.add(fact)
        except Exception as ex:
            self.graph.rollback()
            if not self._is_graph_add_exception_acceptable(ex):
                raise ex
        else:
            self.graph.commit()

    @classmethod
    def _is_graph_add_exception_acceptable(cls, ex):
        """Checks if a graph-add exception can be safely ignored.

        """
        # integrity errors due to violating unique constraints should be safe
        # to ignore since the only unique constraints in rdflib-sqlalchemy are
        # on index columns
        return 'UNIQUE constraint failed' in str(ex)


_METADATA_CACHE = None

registerplugins()


def set_metadata_cache(cache):
    """Sets the metadata cache object to use.

    """
    global _METADATA_CACHE

    if _METADATA_CACHE and _METADATA_CACHE.is_open:
        _METADATA_CACHE.close()

    _METADATA_CACHE = cache


def get_metadata_cache():
    """Returns the current metadata cache object.

    """
    global _METADATA_CACHE

    if _METADATA_CACHE is None:
        _METADATA_CACHE = _create_metadata_cache(_DB_PATH)

    return _METADATA_CACHE


def _create_metadata_cache(cache_location):
    """Creates a new metadata cache instance appropriate for this platform.

    """
    cache_url = os.getenv('GUTENBERG_FUSEKI_URL')
    if cache_url:
        logging.info('Utilizing FusekiMetadataCache')
        return FusekiMetadataCache(cache_location, cache_url)
    postgres_name = os.getenv('GUTENBERG_POSTGRES_NAME')
    if postgres_name:
        logging.info('Utilizing PostgresMetadataCache')
        return PostgresMetadataCache(postgres_name)

    logging.warning('Unable to create cache based on FusekiMetadataCache or PostgresMetadataCache. '
                    'Falling back to SQLite backend. '
                    'Performance may be degraded significantly.')

    return SqliteMetadataCache(cache_location)


def load_metadata(refresh_cache=False):
    """Returns a graph representing meta-data for all Project Gutenberg texts.
    Pertinent information about texts or about how texts relate to each other
    (e.g. shared authors, shared subjects) can be extracted using standard RDF
    processing techniques (e.g. SPARQL queries). After making an initial remote
    call to Project Gutenberg's servers, the meta-data is persisted locally.

    """
    cache = get_metadata_cache()

    if refresh_cache:
        cache.refresh()

    if not cache.is_open:
        cache.open()

    return cache.graph
