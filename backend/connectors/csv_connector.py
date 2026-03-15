"""
CSV data source connectors.

CSVConnector   — scans a directory for CSV files (used internally).
CSVFileConnector — wraps a single CSV file as its own named source.
"""

import csv
import logging
from pathlib import Path
from typing import Any

from connectors.abstraction.base import (
    BaseConnector,
    ColumnMeta,
    IndexingRules,
    SourceInfo,
    TableMeta,
)


logger = logging.getLogger(__name__)


class CSVConnector(BaseConnector):
    def __init__(
        self,
        source_id: str,
        directory: str,
        description: str = "",
        indexing_rules: IndexingRules | None = None,
    ):
        super().__init__(indexing_rules=indexing_rules)
        self.source_id = source_id
        self.directory = Path(directory)
        self.description = description or f"CSV files in: {self.directory}"

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            source_id=self.source_id,
            source_type="csv",
            description=self.description,
            location=str(self.directory),
        )

    def _csv_files(self) -> list[Path]:
        if not self.directory.exists():
            return []
        return sorted(self.directory.glob("*.csv"))

    def list_tables(self) -> list[TableMeta]:
        tables = []
        for csv_file in self._csv_files():
            if not self.should_index_table(csv_file.stem):
                continue
            meta = self._build_table_meta(csv_file)
            if meta:
                tables.append(meta)
        return tables

    def get_schema(self, path: str) -> TableMeta | None:
        # path is the filename stem or full filename
        target = self.directory / path
        if not target.exists():
            target = self.directory / (path + ".csv")
        if not target.exists():
            return None
        if not self.should_index_table(target.stem):
            return None
        return self._build_table_meta(target)

    def get_sample(self, path: str, limit: int = 5) -> list[dict[str, Any]]:
        target = self.directory / path
        if not target.exists():
            target = self.directory / (path + ".csv")
        if not target.exists():
            return []
        rows = []
        with open(target, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                rows.append(dict(row))
        return rows

    def _build_table_meta(self, csv_file: Path) -> TableMeta | None:
        try:
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                field_names = reader.fieldnames or []
                sample_rows: list[dict] = []
                row_count = 0
                for row in reader:
                    row_count += 1
                    if len(sample_rows) < 3:
                        sample_rows.append(dict(row))

            columns = []
            for col_name in field_names:
                if not self.should_index_column(csv_file.stem, col_name):
                    continue
                # Infer type from sample values
                sample_values = [row[col_name] for row in sample_rows if col_name in row]
                data_type = self._infer_type(sample_values)
                columns.append(
                    ColumnMeta(
                        name=col_name,
                        data_type=data_type,
                        sample_values=sample_values[:3],
                    )
                )

            return TableMeta(
                name=csv_file.stem,
                source_id=self.source_id,
                columns=columns,
                row_count=row_count,
                path=csv_file.name,
            )
        except Exception as exc:
            logger.warning("Failed to read CSV file '%s': %s", csv_file, exc)
            return None

    @staticmethod
    def _infer_type(values: list[str]) -> str:
        if not values:
            return "TEXT"
        numeric = 0
        for v in values:
            try:
                float(v)
                numeric += 1
            except (ValueError, TypeError):
                pass
        if numeric == len(values):
            # Check if all integers
            try:
                for v in values:
                    int(v)
                return "INTEGER"
            except (ValueError, TypeError):
                return "REAL"
        return "TEXT"


# ---------------------------------------------------------------------------
# Per-file CSV connector (one connector = one CSV file)
# ---------------------------------------------------------------------------

class CSVFileConnector(CSVConnector):
    """
    A CSVConnector that wraps a single CSV file instead of a directory.

    source_type is reported as "csv_file" so the UI can distinguish it.
    The file is placed in a temporary single-file directory so the parent
    CSVConnector directory-scanning logic works unchanged.
    """

    def __init__(
        self,
        source_id: str,
        file_path: str,
        description: str = "",
        indexing_rules: IndexingRules | None = None,
    ):
        self._file_path = Path(file_path)
        # Use the file's parent directory but only expose this one file.
        super().__init__(
            source_id=source_id,
            directory=str(self._file_path.parent),
            description=description or f"CSV file: {self._file_path.name}",
            indexing_rules=indexing_rules,
        )
        self._single_file = self._file_path.name

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            source_id=self.source_id,
            source_type="csv_file",
            description=self.description,
            location=str(self._file_path),
        )

    def _csv_files(self) -> list[Path]:
        """Override: yield only the single file this connector owns."""
        p = self._file_path
        if p.exists():
            return [p]
        return []
