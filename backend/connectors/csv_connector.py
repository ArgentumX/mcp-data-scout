"""CSV data source connector."""

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
        file_path: str,
        description: str = "",
        indexing_rules: IndexingRules | None = None,
    ):
        super().__init__(indexing_rules=indexing_rules)
        self.source_id = source_id
        self.file_path = Path(file_path)
        self.description = description or f"CSV file: {self.file_path.name}"

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            source_id=self.source_id,
            source_type="csv",
            description=self.description,
            location=str(self.file_path),
        )

    def list_tables(self) -> list[TableMeta]:
        if not self.file_path.exists():
            return []
        if not self.should_index_table(self.file_path.stem):
            return []
        meta = self._build_table_meta(self.file_path)
        return [meta] if meta else []

    def get_schema(self, path: str) -> TableMeta | None:
        target = self._resolve_requested_path(path)
        if target is None:
            return None
        if not self.should_index_table(target.stem):
            return None
        return self._build_table_meta(target)

    def get_sample(self, path: str, limit: int = 5) -> list[dict[str, Any]]:
        target = self._resolve_requested_path(path)
        if target is None:
            return []
        rows = []
        with open(target, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                rows.append(dict(row))
        return rows

    def _resolve_requested_path(self, path: str) -> Path | None:
        if not self.file_path.exists():
            return None

        normalized = path.strip()
        accepted = {
            self.file_path.name,
            self.file_path.stem,
            str(self.file_path),
        }
        if normalized in accepted:
            return self.file_path
        return None

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
            try:
                for v in values:
                    int(v)
                return "INTEGER"
            except (ValueError, TypeError):
                return "REAL"
        return "TEXT"
