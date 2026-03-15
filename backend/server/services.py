from index.indexer import MetadataIndexer
from search.engine import SearchEngine
from server.config import INDEX_DB
from server.source_registry import build_default_registry


registry = build_default_registry()
indexer = MetadataIndexer(index_db=INDEX_DB)
engine = SearchEngine(index_db=INDEX_DB)
