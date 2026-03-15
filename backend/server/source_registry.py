"""
Source registry — holds all configured connectors.

All sources (seeded at startup or uploaded by the user at runtime) are treated
identically inside the system.  Dynamic sources (those registered after the
initial startup, e.g. user uploads) are persisted in a JSON manifest so they
survive container restarts.
"""

import json
import logging
import os
from pathlib import Path

from connectors.abstraction.base import BaseConnector, IndexingRules, SourceInfo
from connectors.csv_connector import CSVConnector  # noqa: F401 — re-exported for api_app
from connectors.sqlite_connector import SQLiteConnector  # noqa: F401 — re-exported for api_app

logger = logging.getLogger(__name__)

UPLOADS_DIR = os.getenv("UPLOADS_DIR") or "/data/uploads"
SOURCES_MANIFEST = os.getenv("SOURCES_MANIFEST") or "/data/sources.json"


# ---------------------------------------------------------------------------
# IndexingRules (de)serialisation helpers
# ---------------------------------------------------------------------------

def _indexing_rules_from_dict(d: dict) -> IndexingRules:
    """Deserialise an IndexingRules from a plain JSON-compatible dict."""

    def _set(v):
        return set(v) if v is not None else None

    def _dict_of_sets(v):
        return {k: set(vals) for k, vals in v.items()} if v is not None else None

    return IndexingRules(
        include_tables=_set(d.get("include_tables")),
        exclude_tables=_set(d.get("exclude_tables")),
        include_columns=_dict_of_sets(d.get("include_columns")),
        exclude_columns=_dict_of_sets(d.get("exclude_columns")),
        row_value_tables=_set(d.get("row_value_tables")),
        row_value_columns=_dict_of_sets(d.get("row_value_columns")),
    )


def _indexing_rules_to_dict(rules: IndexingRules) -> dict:
    """Serialise IndexingRules to a JSON-compatible dict."""

    def _ser_set(s):
        return list(s) if s is not None else None

    def _ser_dict_of_sets(d):
        return {k: list(v) for k, v in d.items()} if d is not None else None

    return {
        "include_tables": _ser_set(rules.include_tables),
        "exclude_tables": _ser_set(rules.exclude_tables),
        "include_columns": _ser_dict_of_sets(rules.include_columns),
        "exclude_columns": _ser_dict_of_sets(rules.exclude_columns),
        "row_value_tables": _ser_set(rules.row_value_tables),
        "row_value_columns": _ser_dict_of_sets(rules.row_value_columns),
    }


# ---------------------------------------------------------------------------
# Unified registry
# ---------------------------------------------------------------------------

class SourceRegistry:
    """
    Holds all connectors in a single in-memory dict.

    Sources registered at startup (seeded) and sources added later (user
    uploads) share the same registry with no internal distinction.
    Dynamic sources — those registered via `register_dynamic()` — are
    persisted in a JSON manifest so they survive container restarts.
    """

    def __init__(self, manifest_path: str | Path | None = None) -> None:
        self._connectors: dict[str, BaseConnector] = {}
        self._dynamic_ids: set[str] = set()
        self._manifest_path = Path(manifest_path) if manifest_path else None

    # ------------------------------------------------------------------
    # Core CRUD
    # ------------------------------------------------------------------

    def register(self, connector: BaseConnector) -> None:
        """Register a connector (startup / seed sources use this path)."""
        info = connector.get_source_info()
        self._connectors[info.source_id] = connector

    def register_dynamic(self, connector: BaseConnector) -> None:
        """Register a user-added source and persist it to the manifest."""
        self.register(connector)
        info = connector.get_source_info()
        self._dynamic_ids.add(info.source_id)
        self._save_manifest()

    def get(self, source_id: str) -> BaseConnector | None:
        return self._connectors.get(source_id)

    def remove(self, source_id: str) -> bool:
        if source_id in self._connectors:
            del self._connectors[source_id]
            return True
        return False

    def remove_dynamic(self, source_id: str) -> bool:
        removed = self.remove(source_id)
        if removed:
            self._dynamic_ids.discard(source_id)
            self._save_manifest()
        return removed

    def all(self) -> list[BaseConnector]:
        return list(self._connectors.values())

    def list_sources(self) -> list[SourceInfo]:
        return [c.get_source_info() for c in self._connectors.values()]

    def is_dynamic(self, source_id: str) -> bool:
        return source_id in self._dynamic_ids

    # ------------------------------------------------------------------
    # Manifest persistence (dynamic sources only)
    # ------------------------------------------------------------------

    def load_manifest(self) -> None:
        """Reload dynamic sources persisted in the JSON manifest."""
        if self._manifest_path is None or not self._manifest_path.exists():
            return
        try:
            entries = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not load sources manifest: %s", exc)
            return

        for entry in entries:
            try:
                self._restore_entry(entry)
                self._dynamic_ids.add(entry["source_id"])
            except Exception as exc:
                logger.warning("Skipping manifest entry %s: %s", entry, exc)

    def _restore_entry(self, entry: dict) -> None:
        source_type = entry["source_type"]
        rules = _indexing_rules_from_dict(entry.get("indexing_rules") or {})
        if source_type == "csv":
            path = Path(entry["location"])
            if not path.exists():
                logger.warning("Manifest CSV file missing, skipping: %s", path)
                return
            connector = CSVConnector(
                source_id=entry["source_id"],
                file_path=str(path),
                description=entry.get("description", ""),
                indexing_rules=rules,
            )
        elif source_type == "sqlite":
            path = Path(entry["location"])
            if not path.exists():
                logger.warning("Manifest SQLite file missing, skipping: %s", path)
                return
            connector = SQLiteConnector(
                source_id=entry["source_id"],
                db_path=str(path),
                description=entry.get("description", ""),
                indexing_rules=rules,
            )
        else:
            logger.warning("Unknown source_type in manifest: %s", source_type)
            return
        self.register(connector)

    def _save_manifest(self) -> None:
        """Persist dynamic sources to JSON."""
        if self._manifest_path is None:
            return
        self._manifest_path.parent.mkdir(parents=True, exist_ok=True)
        entries = []
        for connector in self._connectors.values():
            info = connector.get_source_info()
            if info.source_id not in self._dynamic_ids:
                continue
            entry: dict = {
                "source_id": info.source_id,
                "source_type": info.source_type,
                "description": info.description,
                "location": info.location,
                "indexing_rules": _indexing_rules_to_dict(connector.indexing_rules),
            }
            entries.append(entry)
        try:
            self._manifest_path.write_text(
                json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as exc:
            logger.error("Could not save sources manifest: %s", exc)


# ---------------------------------------------------------------------------
# Default registry factory
# ---------------------------------------------------------------------------

def build_default_registry() -> SourceRegistry:
    """
    Build the registry that is used at server startup.

    Seed sources (SQLite + per-file CSV connectors for every CSV in
    UPLOADS_DIR that was placed there by seed_data.py) are registered via
    `register()`.  Previously user-added sources are reloaded from the
    manifest via `load_manifest()`.
    """
    reg = SourceRegistry(manifest_path=SOURCES_MANIFEST)

    # Reload user-added sources that survived from a previous run
    reg.load_manifest()

    return reg
