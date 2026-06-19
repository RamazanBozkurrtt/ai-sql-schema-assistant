import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


GENERIC_COLUMN_NAMES = {
    "recversion",
    "createdbyid",
    "modifiedbyid",
}


def normalize_identifier(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold()) if value else ""


def is_generic_column(column_name: str) -> bool:
    return normalize_identifier(column_name) in GENERIC_COLUMN_NAMES


@dataclass
class SchemaOptimizationStats:
    tables_seen: int = 0
    tables_written: int = 0
    duplicate_tables_skipped: int = 0
    columns_seen: int = 0
    columns_written: int = 0
    generic_columns_filtered: int = 0
    duplicate_columns_skipped: int = 0
    relations_seen: int = 0
    relations_written: int = 0
    generic_relations_filtered: int = 0
    duplicate_relations_skipped: int = 0

    def as_log_message(self) -> str:
        return (
            "tables_seen=%s tables_written=%s duplicate_tables_skipped=%s "
            "columns_seen=%s columns_written=%s generic_columns_filtered=%s duplicate_columns_skipped=%s "
            "relations_seen=%s relations_written=%s generic_relations_filtered=%s duplicate_relations_skipped=%s"
        ) % (
            self.tables_seen,
            self.tables_written,
            self.duplicate_tables_skipped,
            self.columns_seen,
            self.columns_written,
            self.generic_columns_filtered,
            self.duplicate_columns_skipped,
            self.relations_seen,
            self.relations_written,
            self.generic_relations_filtered,
            self.duplicate_relations_skipped,
        )


def _iter_raw_columns(raw_columns) -> Iterator[str]:
    for column in raw_columns or []:
        if isinstance(column, dict):
            column_name = column.get("column") or column.get("name")
        elif isinstance(column, str):
            column_name = column
        else:
            continue

        if isinstance(column_name, str) and column_name:
            yield column_name


def _iter_raw_relations(raw_relations) -> Iterator[tuple[str, str]]:
    for relation in raw_relations or []:
        if not isinstance(relation, dict):
            continue

        column_name = relation.get("column")
        ref_table = relation.get("ref_table")
        if isinstance(column_name, str) and column_name and isinstance(ref_table, str) and ref_table:
            yield column_name, ref_table


def optimize_table_payload(table_name: str, raw_table_data: dict, stats: SchemaOptimizationStats | None = None) -> dict:
    columns = []
    relations = []
    seen_columns = set()
    seen_relations = set()

    for column_name in _iter_raw_columns(raw_table_data.get("columns", [])):
        if stats:
            stats.columns_seen += 1

        normalized_column = normalize_identifier(column_name)
        if not normalized_column:
            continue

        if is_generic_column(column_name):
            if stats:
                stats.generic_columns_filtered += 1
            continue

        if normalized_column in seen_columns:
            if stats:
                stats.duplicate_columns_skipped += 1
            continue

        seen_columns.add(normalized_column)
        columns.append(column_name)
        if stats:
            stats.columns_written += 1

    for column_name, ref_table in _iter_raw_relations(raw_table_data.get("relations", [])):
        if stats:
            stats.relations_seen += 1

        if is_generic_column(column_name):
            if stats:
                stats.generic_relations_filtered += 1
            continue

        relation_key = (normalize_identifier(column_name), ref_table.casefold())
        if relation_key in seen_relations:
            if stats:
                stats.duplicate_relations_skipped += 1
            continue

        seen_relations.add(relation_key)
        relations.append(
            {
                "column": column_name,
                "ref_table": ref_table,
            }
        )
        if stats:
            stats.relations_written += 1

    optimized_table = {
        "name": table_name,
        "columns": columns,
    }
    if relations:
        optimized_table["relations"] = relations

    return optimized_table


def _update_column_index(column_index_entries: dict, table_name: str, columns: Iterable[str]) -> None:
    for column_name in columns:
        normalized_column = normalize_identifier(column_name)
        if not normalized_column:
            continue

        entry = column_index_entries.setdefault(
            normalized_column,
            {
                "name": column_name,
                "tables": [],
                "seen_tables": set(),
            },
        )

        if table_name in entry["seen_tables"]:
            continue

        entry["seen_tables"].add(table_name)
        entry["tables"].append(table_name)


def _build_column_index(column_index_entries: dict) -> dict[str, list[str]]:
    column_index = {}

    for entry in sorted(column_index_entries.values(), key=lambda item: item["name"].casefold()):
        column_index[entry["name"]] = entry["tables"]

    return column_index


def build_optimized_schema(table_entries: Iterable[tuple[str, dict]], logger: logging.Logger | None = None) -> tuple[dict, SchemaOptimizationStats]:
    stats = SchemaOptimizationStats()
    tables = []
    seen_tables = set()
    column_index_entries = {}

    for table_name, raw_table_data in table_entries:
        stats.tables_seen += 1
        normalized_table = table_name.casefold()
        if normalized_table in seen_tables:
            stats.duplicate_tables_skipped += 1
            continue

        seen_tables.add(normalized_table)
        optimized_table = optimize_table_payload(table_name, raw_table_data, stats=stats)
        tables.append(optimized_table)
        stats.tables_written += 1
        _update_column_index(column_index_entries, table_name, optimized_table["columns"])

    optimized_schema = {
        "tables": tables,
        "column_index": _build_column_index(column_index_entries),
    }

    if logger:
        logger.info("Optimized schema built: %s", stats.as_log_message())

    return optimized_schema, stats


class _StreamingJSONReader:
    def __init__(self, path: Path, chunk_size: int = 262_144):
        self.path = Path(path)
        self.chunk_size = chunk_size
        self.decoder = json.JSONDecoder()
        self.file = None
        self.buffer = ""
        self.position = 0
        self.eof = False

    def __enter__(self):
        self.file = self.path.open("r", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.file:
            self.file.close()

    def _read_more(self) -> bool:
        chunk = self.file.read(self.chunk_size)
        if not chunk:
            self.eof = True
            return False

        self.buffer = self.buffer[self.position:] + chunk
        self.position = 0
        return True

    def _ensure_available(self) -> bool:
        while self.position >= len(self.buffer):
            if not self._read_more():
                return False
        return True

    def _compact(self) -> None:
        if self.position >= self.chunk_size:
            self.buffer = self.buffer[self.position:]
            self.position = 0

    def skip_whitespace(self) -> None:
        while True:
            if not self._ensure_available():
                return

            if not self.buffer[self.position].isspace():
                return

            self.position += 1
            self._compact()

    def peek_non_whitespace(self) -> str:
        self.skip_whitespace()
        if not self._ensure_available():
            raise ValueError("Unexpected end of JSON stream.")
        return self.buffer[self.position]

    def expect_char(self, expected: str) -> None:
        actual = self.peek_non_whitespace()
        if actual != expected:
            raise ValueError(f"Expected {expected!r}, found {actual!r}.")

        self.position += 1
        self._compact()

    def consume_delimiter(self, allowed: set[str]) -> str:
        actual = self.peek_non_whitespace()
        if actual not in allowed:
            raise ValueError(f"Expected one of {sorted(allowed)!r}, found {actual!r}.")

        self.position += 1
        self._compact()
        return actual

    def parse_value(self):
        self.skip_whitespace()

        while True:
            try:
                value, next_position = self.decoder.raw_decode(self.buffer, self.position)
                self.position = next_position
                self._compact()
                return value
            except json.JSONDecodeError:
                if not self._read_more():
                    raise

    def parse_string(self) -> str:
        value = self.parse_value()
        if not isinstance(value, str):
            raise ValueError(f"Expected JSON string, found {type(value).__name__}.")
        return value


def _iter_object_items(reader: _StreamingJSONReader) -> Iterator[tuple[str, dict]]:
    reader.expect_char("{")

    while True:
        if reader.peek_non_whitespace() == "}":
            reader.expect_char("}")
            return

        key = reader.parse_string()
        reader.expect_char(":")
        value = reader.parse_value()
        yield key, value

        delimiter = reader.consume_delimiter({",", "}"})
        if delimiter == "}":
            return


def iter_legacy_schema_tables(input_path: str | Path, chunk_size: int = 262_144) -> Iterator[tuple[str, dict]]:
    with _StreamingJSONReader(Path(input_path), chunk_size=chunk_size) as reader:
        reader.expect_char("{")

        while True:
            if reader.peek_non_whitespace() == "}":
                reader.expect_char("}")
                return

            key = reader.parse_string()
            reader.expect_char(":")

            if key == "schema":
                yield from _iter_object_items(reader)
            else:
                reader.parse_value()

            delimiter = reader.consume_delimiter({",", "}"})
            if delimiter == "}":
                return


def optimize_schema_file(
    input_path: str | Path,
    output_path: str | Path,
    chunk_size: int = 262_144,
    logger: logging.Logger | None = None,
) -> SchemaOptimizationStats:
    logger = logger or logging.getLogger(__name__)
    stats = SchemaOptimizationStats()
    seen_tables = set()
    column_index_entries = {}
    output_path = Path(output_path)
    temp_output_path = output_path.with_suffix(f"{output_path.suffix}.tmp")

    with temp_output_path.open("w", encoding="utf-8") as output_file:
        output_file.write('{"tables":[')
        first_table = True

        for table_name, raw_table_data in iter_legacy_schema_tables(input_path, chunk_size=chunk_size):
            stats.tables_seen += 1
            normalized_table = table_name.casefold()
            if normalized_table in seen_tables:
                stats.duplicate_tables_skipped += 1
                continue

            seen_tables.add(normalized_table)
            optimized_table = optimize_table_payload(table_name, raw_table_data, stats=stats)
            if not first_table:
                output_file.write(",")

            json.dump(optimized_table, output_file, ensure_ascii=False, separators=(",", ":"))
            first_table = False
            stats.tables_written += 1
            _update_column_index(column_index_entries, table_name, optimized_table["columns"])

        output_file.write('],"column_index":')
        json.dump(_build_column_index(column_index_entries), output_file, ensure_ascii=False, separators=(",", ":"))
        output_file.write("}")

    temp_output_path.replace(output_path)
    logger.info("Optimized schema file created at %s: %s", output_path, stats.as_log_message())
    return stats
