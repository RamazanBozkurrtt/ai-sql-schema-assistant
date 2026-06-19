import pyodbc
import json
import logging
import os
import re
import requests
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from schema_optimizer import build_optimized_schema, normalize_identifier


PROJECT_ROOT = Path(__file__).resolve().parent


def _load_local_env_file() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def build_connection_string() -> str:
    explicit_connection_string = os.getenv("MSSQL_CONNECTION_STRING", "").strip()
    if explicit_connection_string:
        return explicit_connection_string

    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server").strip()
    server = os.getenv("MSSQL_SERVER", "").strip()
    database = os.getenv("MSSQL_DATABASE", "").strip()
    username = os.getenv("MSSQL_UID", "").strip()
    password = os.getenv("MSSQL_PWD", "").strip()
    trust_server_certificate = os.getenv("MSSQL_TRUST_SERVER_CERTIFICATE", "yes").strip()
    trusted_connection = os.getenv("MSSQL_TRUSTED_CONNECTION", "").strip().lower()

    if not server or not database:
        raise RuntimeError("MSSQL_SERVER and MSSQL_DATABASE must be configured in .env or environment variables.")

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"TrustServerCertificate={trust_server_certificate}",
    ]

    if trusted_connection in {"1", "true", "yes"}:
        parts.append("Trusted_Connection=yes")
    else:
        if not username or not password:
            raise RuntimeError("MSSQL_UID and MSSQL_PWD must be configured, or set MSSQL_TRUSTED_CONNECTION=yes.")
        parts.extend([f"UID={username}", f"PWD={password}"])

    return ";".join(parts) + ";"


_load_local_env_file()

_UNSAFE_SQL_PATTERN = re.compile(
    r"\b(?:DROP|DELETE|UPDATE|ALTER|TRUNCATE)\b",
    re.IGNORECASE,
)
_JSON_BLOCK_PATTERN = re.compile(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", re.IGNORECASE | re.DOTALL)
_SQL_LIKE_RESPONSE_PATTERN = re.compile(
    r"(?is)^\s*(?:```sql\s*)?(?:WITH|SELECT|CREATE|ALTER|INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE|EXEC)\b"
)
_QUESTION_TERM_PATTERN = re.compile(r"\w+", re.UNICODE)
RULES_FILE = PROJECT_ROOT / "rules.json"
SCHEMA_CACHE_FILE = PROJECT_ROOT / "schema_cache.json"
SCHEMA_CACHE_META_FILE = PROJECT_ROOT / "schema_cache.meta.json"
SCHEMA_AI_CACHE_FILE = PROJECT_ROOT / "schema_cache.ai.json"
SCHEMA_SEARCH_INDEX_FILE = PROJECT_ROOT / "schema_search_index.json"
SCHEMA_CACHE_VERSION = 5
LOGGER = logging.getLogger(__name__)
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:14b")
ANALYSIS_SYSTEM_PROMPT = """
You are a Turkish-speaking SQL Server schema assistant for a legacy ERP codebase.

Guidelines:
- You are a database analyst.
- Never guess from table names alone.
- Help the user understand schema complexity, duplicate-looking columns, likely canonical columns, legacy naming, and table relationships.
- Never generate SQL queries, stored procedures, DDL, DML, or code blocks containing SQL.
- If the user asks for SQL, explain which tables, columns, and relationships are relevant instead.
- Before answering, identify relevant tables and rely on live data checks whenever available.
- For each relevant table, prefer evidence such as row count, null ratio, whether the relevant columns actually contain data, and sample rows.
- When the user asks which column or table should be preferred, reason from schema, foreign keys, names, repository usage patterns, and live data evidence.
- If certainty is limited, say "emin degilim".
- Do not answer "X nerede tutuluyor?" using name similarity alone; verify whether the data is actually stored there or whether the table is only an aggregate or movement table.
- Keep answers practical and concise.
""".strip()
_IMPORTANT_COLUMN_HINTS = (
    "id",
    "kod",
    "adi",
    "ad",
    "no",
    "tarih",
    "date",
    "miktar",
    "durum",
    "aktif",
    "isdeleted",
    "siparis",
    "cari",
    "stok",
    "urun",
    "ref",
)
_GENERIC_QUESTION_TERMS = {
    "adin",
    "adi",
    "ad",
    "aktif",
    "ayni",
    "benzer",
    "canonical",
    "canonik",
    "fark",
    "alan",
    "getir",
    "gerekli",
    "gerekiyor",
    "goster",
    "gore",
    "hangi",
    "hangiisi",
    "hangisi",
    "ihtiyac",
    "icin",
    "ile",
    "kanonik",
    "kolon",
    "kolonu",
    "kullaniliyor",
    "kullanilan",
    "kullanilmayan",
    "lazim",
    "list",
    "liste",
    "listesi",
    "mi",
    "olan",
    "sonra",
    "sonrasi",
    "sutun",
    "sutunu",
    "tablo",
    "tabloda",
    "tarih",
    "tercih",
    "tum",
    "ve",
    "ver",
    "var",
    "yok",
}
_QUESTION_TABLE_HINTS = {
    "musteri": ["Cr_Cari", "Cr_CariAdres"],
    "cari": ["Cr_Cari", "Cr_CariAdres"],
    "siparis": ["Sip_Siparis", "Sip_SiparisDetay"],
    "siparisdetay": ["Sip_SiparisDetay", "Sip_Siparis"],
    "uretim": ["Sip_UretimSiparis", "Sip_SiparisDetay"],
    "urun": ["Gnl_StokUrunMaster", "Urt_Urun"],
    "stok": ["Gnl_StokUrunMaster", "DepoHareketleri"],
    "irsaliye": ["Irs_Irsaliye", "Irs_IrsaliyeDetay"],
    "fatura": ["Fns_FaturaFisler", "Fns_FaturaFislerDetay"],
}
_READ_ONLY_ANALYSIS_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE)
_BLOCKED_ANALYSIS_SQL_PATTERN = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|ALTER|DROP|TRUNCATE|CREATE|EXEC|EXECUTE|GRANT|REVOKE|DENY)\b",
    re.IGNORECASE,
)


def get_connection():
    return pyodbc.connect(build_connection_string())


def get_database_identity() -> dict:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME AS ServerName, DB_NAME() AS DatabaseName")
        row = cursor.fetchone()

    return {
        "server": row[0],
        "database": row[1],
    }


def get_tables():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.tables ORDER BY name")
        return [row[0] for row in cursor.fetchall()]


def get_columns():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                t.name AS TableName,
                c.name AS ColumnName,
                ty.name AS DataType
            FROM sys.tables t
            JOIN sys.columns c ON t.object_id = c.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            ORDER BY t.name, c.column_id
        """)

        result = {}

        for table, column, dtype in cursor.fetchall():
            if table not in result:
                result[table] = []

            result[table].append({
                "column": column,
                "type": dtype
            })

        return result


def get_foreign_keys():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                OBJECT_NAME(fk.parent_object_id) AS TableName,
                COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
                OBJECT_NAME(fk.referenced_object_id) AS RefTable
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fc 
            ON fk.object_id = fc.constraint_object_id
            ORDER BY OBJECT_NAME(fk.parent_object_id), COL_NAME(fc.parent_object_id, fc.parent_column_id)
        """)

        fks = []

        for row in cursor.fetchall():
            fks.append({
                "table": row[0],
                "column": row[1],
                "ref_table": row[2]
            })

        return fks


def find_similar_columns(columns_dict):
    column_map = {}

    for table, cols in columns_dict.items():
        for col in cols:
            col_name = col["column"].lower()

            # sadece ID gibi önemli kolonları al
            if not col_name.endswith("id"):
                continue

            if col_name not in column_map:
                column_map[col_name] = []

            column_map[col_name].append(table)

    return {
        col: tables
        for col, tables in column_map.items()
        if len(tables) > 1
    }


def is_meaningful_key(column_name: str) -> bool:
    if not column_name:
        return False

    normalized_name = re.sub(r"[^a-z0-9]", "", column_name.lower())
    if normalized_name in {
        "createdby",
        "updatedby",
        "deletedby",
        "createddate",
        "updateddate",
        "deleteddate",
    }:
        return False

    if not normalized_name.endswith("id"):
        return False

    tokenized_name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", column_name)
    tokens = [
        token.lower()
        for token in re.split(r"[^A-Za-z0-9]+|\s+", tokenized_name)
        if token
    ]

    excluded_names = {
        "createdbyid",
        "updatedbyid",
        "deletedbyid",
        "createduserid",
        "updateduserid",
        "deleteduserid",
        "createdat",
        "updatedat",
        "deletedat",
        "createdon",
        "updatedon",
        "deletedon",
        "createdtime",
        "updatedtime",
        "deletedtime",
        "modifiedbyid",
        "modifieduserid",
        "modifieddate",
        "modifiedat",
        "modifiedon",
        "modifiedtime",
        "insertedbyid",
        "inserteduserid",
        "inserteddate",
        "insertedat",
        "insertedon",
        "insertedtime",
    }
    if normalized_name in excluded_names:
        return False

    audit_actions = {"created", "updated", "deleted", "modified", "inserted"}
    audit_targets = {"by", "user", "userid", "date", "time", "at", "on"}
    if tokens and tokens[0] in audit_actions and any(token in audit_targets for token in tokens[1:]):
        return False

    if any(token in {"audit", "log", "history", "trace"} for token in tokens):
        return False

    return True


def find_similar_columns(columns_dict, foreign_keys=None):
    fk_pairs = {
        (fk["table"], fk["column"].lower())
        for fk in (foreign_keys or [])
        if fk.get("table") and fk.get("column")
    }
    column_map = {}

    for table, cols in columns_dict.items():
        for col in cols:
            raw_name = col["column"]
            if not is_meaningful_key(raw_name):
                continue

            col_name = raw_name.lower()
            if col_name not in column_map:
                column_map[col_name] = []

            column_map[col_name].append({
                "table": table,
                "is_foreign_key": (table, col_name) in fk_pairs,
            })

    similar_columns = {}

    for col_name, entries in column_map.items():
        ordered_entries = sorted(entries, key=lambda entry: not entry["is_foreign_key"])
        related_tables = []
        seen_tables = set()

        for entry in ordered_entries:
            table_name = entry["table"]
            if table_name in seen_tables:
                continue

            seen_tables.add(table_name)
            related_tables.append(table_name)

        if len(related_tables) > 1:
            similar_columns[col_name] = related_tables

    return similar_columns


def extract_schema():
    tables = get_tables()
    columns = get_columns()
    relations_by_table = {table_name: [] for table_name in tables}

    for fk in get_foreign_keys():
        relations_by_table.setdefault(fk["table"], []).append(
            {
                "column": fk["column"],
                "ref_table": fk["ref_table"],
            }
        )

    table_entries = (
        (
            table_name,
            {
                "columns": columns.get(table_name, []),
                "relations": relations_by_table.get(table_name, []),
            },
        )
        for table_name in tables
    )
    optimized_schema, stats = build_optimized_schema(table_entries, logger=LOGGER)
    LOGGER.info("Database schema extracted in optimized form: %s", stats.as_log_message())
    return optimized_schema

def build_ai_input(schema):
    result = ""

    for table, data in schema.items():
        result += f"\n=== TABLE: {table} ===\n"


        result += "Columns:\n"
        for col in data["columns"]:
            result += f"- {col['column']} ({col['type']})\n"

        if data["relations"]:
            result += "\nStrong Relations:\n"
            for rel in data["relations"]:
                result += f"- {rel['column']} → {rel['ref_table']}\n"

        if data["possible_links"]:
            result += "\nPossible Joins (ID based):\n"
            for link in data["possible_links"]:
                result += f"- {link['column']} → {', '.join(link['related_tables'])}\n"

        result += "\n" + "-"*50 + "\n"

    return result


def refresh_schema_cache() -> dict:
    schema = extract_schema()
    identity = get_database_identity()
    schema_cache = {
        "schema": schema,
        "database_identity": identity,
        "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    with open(SCHEMA_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(schema_cache, f, ensure_ascii=False, indent=2)

    return schema_cache


def load_schema_cache() -> dict:
    if not SCHEMA_CACHE_FILE.exists():
        return refresh_schema_cache()

    try:
        with open(SCHEMA_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return refresh_schema_cache()

    if "schema" not in data or not isinstance(data["schema"], dict):
        return refresh_schema_cache()

    cached_identity = data.get("database_identity")
    if not isinstance(cached_identity, dict):
        return refresh_schema_cache()

    current_identity = get_database_identity()
    if (
        cached_identity.get("server") != current_identity.get("server")
        or cached_identity.get("database") != current_identity.get("database")
    ):
        return refresh_schema_cache()

    return data


def get_schema_text(force_refresh=False):
    schema_cache = refresh_schema_cache() if force_refresh else load_schema_cache()
    return build_ai_input(schema_cache["schema"])


def get_schema_data(force_refresh=False) -> dict:
    schema_cache = refresh_schema_cache() if force_refresh else load_schema_cache()
    return schema_cache["schema"]


def build_schema_browser_data(force_refresh=False) -> dict:
    schema = get_schema_data(force_refresh=force_refresh)
    table_items = []

    for table_name, table_data in schema.items():
        column_names = [column["column"] for column in table_data.get("columns", [])]
        table_items.append(
            {
                "name": table_name,
                "columns": column_names,
            }
        )

    return {"tables": table_items}


def search_columns_local(column_name: str) -> list[dict]:
    needle = column_name.strip().lower()
    if not needle:
        return []

    schema = get_schema_data()
    exact_matches = {}
    similar_matches = {}

    for table_name, table_data in schema.items():
        for column in table_data.get("columns", []):
            current_name = column["column"]
            current_lower = current_name.lower()

            if current_lower == needle:
                exact_matches.setdefault(current_name, []).append(table_name)
            elif needle in current_lower or current_lower.endswith(needle):
                similar_matches.setdefault(current_name, []).append(table_name)

    if exact_matches:
        return [
            {"column": name, "tables": tables}
            for name, tables in sorted(exact_matches.items())
        ]

    return [
        {"column": name, "tables": tables}
        for name, tables in sorted(similar_matches.items())
    ]


def _extract_json_payload(response_text: str) -> str:
    if not response_text:
        return ""

    fenced_match = _JSON_BLOCK_PATTERN.search(response_text)
    if fenced_match:
        return fenced_match.group(1).strip()

    object_start = response_text.find("{")
    object_end = response_text.rfind("}")
    if object_start != -1 and object_end != -1 and object_end > object_start:
        return response_text[object_start:object_end + 1].strip()

    array_start = response_text.find("[")
    array_end = response_text.rfind("]")
    if array_start != -1 and array_end != -1 and array_end > array_start:
        return response_text[array_start:array_end + 1].strip()

    return response_text.strip()


def search_columns_with_ai(column_name: str) -> list[dict]:
    prompt = f"""
    Find which tables might contain this column based on schema.

    Target column:
    {column_name}

    Schema:
    {get_ai_schema_text()}

    Return ONLY JSON in this format:
    {{
    "matches": [
        {{
        "column": "{column_name}",
        "tables": ["Table1", "Table2"]
        }}
    ]
    }}
    """

    response_text = ask_ai(prompt)
    payload = _extract_json_payload(response_text)

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return []

    matches = parsed.get("matches", []) if isinstance(parsed, dict) else []
    normalized_matches = []

    for match in matches:
        if not isinstance(match, dict):
            continue

        tables = [
            table_name
            for table_name in match.get("tables", [])
            if isinstance(table_name, str) and table_name
        ]

        if not tables:
            continue

        normalized_matches.append(
            {
                "column": match.get("column") or column_name,
                "tables": tables,
            }
        )

    return normalized_matches


def search_columns(column_name: str) -> list[dict]:
    local_matches = search_columns_local(column_name)
    if local_matches:
        return local_matches

    return search_columns_with_ai(column_name)


def search_tables_local(table_name: str) -> list[dict]:
    needle = table_name.strip().lower()
    if not needle:
        return []

    schema = get_schema_data()
    exact_matches = []
    similar_matches = []

    for current_table, table_data in sorted(schema.items()):
        entry = {
            "table": current_table,
            "columns": [column["column"] for column in table_data.get("columns", [])],
        }

        current_lower = current_table.lower()
        if current_lower == needle:
            exact_matches.append(entry)
        elif needle in current_lower or current_lower.endswith(needle):
            similar_matches.append(entry)

    return exact_matches or similar_matches


def search_tables(table_name: str) -> list[dict]:
    return search_tables_local(table_name)


def build_ai_input(schema):
    blocks = []

    for table in schema.get("tables", []):
        block_lines = [f"=== TABLE: {table['name']} ==="]
        columns = table.get("columns", [])
        block_lines.append(f"Columns: {', '.join(columns) if columns else '(none)'}")

        relations = table.get("relations", [])
        if relations:
            block_lines.append("Relations:")
            for rel in relations:
                block_lines.append(f"- {rel['column']} -> {rel['ref_table']}")

        blocks.append("\n".join(block_lines))

    return "\n\n".join(blocks)


def _build_schema_search_index(schema: dict) -> dict:
    column_index = schema.get("column_index")
    if not isinstance(column_index, dict):
        column_index = _build_local_column_index(schema.get("tables", []))

    return {
        "column_index": dict(sorted(column_index.items(), key=lambda item: item[0].casefold())),
    }


def refresh_schema_cache() -> dict:
    extracted_schema = extract_schema()
    search_index = _build_schema_search_index(extracted_schema)
    schema = {
        key: value
        for key, value in extracted_schema.items()
        if key != "column_index"
    }
    ai_schema = build_ai_schema_cache(schema)
    identity = get_database_identity()

    with open(SCHEMA_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)

    with open(SCHEMA_AI_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(ai_schema, f, ensure_ascii=False, indent=2)

    with open(SCHEMA_SEARCH_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(search_index, f, ensure_ascii=False, indent=2)

    with open(SCHEMA_CACHE_META_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "schema_version": SCHEMA_CACHE_VERSION,
                "database_identity": identity,
                "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
                "table_count": len(schema.get("tables", [])),
                "ai_table_count": len(ai_schema.get("tables", [])),
                "ai_char_count": len(build_ai_input(ai_schema)),
                "search_column_count": len(search_index.get("column_index", {})),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    LOGGER.info("Optimized schema cache refreshed: %s", SCHEMA_CACHE_FILE)
    return schema


def load_schema_cache() -> dict:
    if (
        not SCHEMA_CACHE_FILE.exists()
        or not SCHEMA_CACHE_META_FILE.exists()
        or not SCHEMA_AI_CACHE_FILE.exists()
        or not SCHEMA_SEARCH_INDEX_FILE.exists()
    ):
        return refresh_schema_cache()

    try:
        with open(SCHEMA_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with open(SCHEMA_CACHE_META_FILE, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (json.JSONDecodeError, OSError):
        return refresh_schema_cache()

    if not isinstance(data, dict):
        return refresh_schema_cache()

    if "tables" not in data or not isinstance(data["tables"], list):
        return refresh_schema_cache()

    if meta.get("schema_version") != SCHEMA_CACHE_VERSION:
        return refresh_schema_cache()

    cached_identity = meta.get("database_identity") if isinstance(meta, dict) else None
    if not isinstance(cached_identity, dict):
        return refresh_schema_cache()

    current_identity = get_database_identity()
    if (
        cached_identity.get("server") != current_identity.get("server")
        or cached_identity.get("database") != current_identity.get("database")
    ):
        return refresh_schema_cache()

    return data


def get_schema_text(force_refresh=False):
    schema_cache = refresh_schema_cache() if force_refresh else load_schema_cache()
    return build_ai_input(schema_cache)


def get_schema_data(force_refresh=False) -> dict:
    return refresh_schema_cache() if force_refresh else load_schema_cache()


def load_schema_search_index(force_refresh: bool = False) -> dict:
    if force_refresh:
        refresh_schema_cache()
    else:
        load_schema_cache()

    if not SCHEMA_SEARCH_INDEX_FILE.exists():
        refresh_schema_cache()

    try:
        with open(SCHEMA_SEARCH_INDEX_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        refresh_schema_cache()
        with open(SCHEMA_SEARCH_INDEX_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    if not isinstance(data, dict) or "column_index" not in data or not isinstance(data["column_index"], dict):
        refresh_schema_cache()
        with open(SCHEMA_SEARCH_INDEX_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    return data


def get_schema_search_index(force_refresh: bool = False) -> dict:
    return load_schema_search_index(force_refresh=force_refresh)


def load_ai_schema_cache(force_refresh: bool = False) -> dict:
    if force_refresh:
        refresh_schema_cache()

    if not SCHEMA_AI_CACHE_FILE.exists():
        refresh_schema_cache()

    try:
        with open(SCHEMA_AI_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        refresh_schema_cache()
        with open(SCHEMA_AI_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    if not isinstance(data, dict) or "tables" not in data or not isinstance(data["tables"], list):
        refresh_schema_cache()
        with open(SCHEMA_AI_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    return data


def get_ai_schema_data(force_refresh: bool = False) -> dict:
    return load_ai_schema_cache(force_refresh=force_refresh)


def get_ai_schema_text(force_refresh: bool = False) -> str:
    return build_ai_input(get_ai_schema_data(force_refresh=force_refresh))


def build_schema_browser_data(force_refresh=False) -> dict:
    schema = get_schema_data(force_refresh=force_refresh)
    return {"tables": schema.get("tables", [])}


def search_columns_local(column_name: str, allow_similar: bool = True) -> list[dict]:
    needle = column_name.strip()
    normalized_needle = normalize_identifier(needle)
    if not normalized_needle:
        return []

    search_index = get_schema_search_index()
    exact_matches = []
    similar_matches = []

    for current_name, tables in sorted(search_index.get("column_index", {}).items()):
        normalized_name = normalize_identifier(current_name)
        entry = {
            "column": current_name,
            "tables": tables,
        }

        if normalized_name == normalized_needle:
            exact_matches.append(entry)
        elif normalized_needle in normalized_name or normalized_name.endswith(normalized_needle):
            similar_matches.append(entry)

    return exact_matches or (similar_matches if allow_similar else [])


def search_tables_local(table_name: str, allow_similar: bool = True) -> list[dict]:
    needle = table_name.strip().lower()
    if not needle:
        return []

    schema = get_schema_data()
    exact_matches = []
    similar_matches = []

    for table_data in schema.get("tables", []):
        current_table = table_data.get("name", "")
        entry = {
            "table": current_table,
            "columns": table_data.get("columns", []),
        }

        current_lower = current_table.lower()
        if current_lower == needle:
            exact_matches.append(entry)
        elif needle in current_lower or current_lower.endswith(needle):
            similar_matches.append(entry)

    return exact_matches or (similar_matches if allow_similar else [])


def _quote_identifier(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def _get_schema_table_map() -> dict[str, dict]:
    schema = get_schema_data()
    return {
        normalize_identifier(table_data.get("name", "")): table_data
        for table_data in schema.get("tables", [])
        if table_data.get("name")
    }


def _split_identifier_tokens(name: str) -> list[str]:
    tokenized_name = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name or "")
    return [
        normalize_identifier(token)
        for token in re.split(r"[^A-Za-z0-9]+|\s+", tokenized_name)
        if normalize_identifier(token)
    ]


def _column_family_key(column_name: str) -> str:
    ignore_tokens = {
        "id",
        "kod",
        "kodu",
        "ad",
        "adi",
        "adlari",
        "no",
        "numara",
        "tarih",
        "tarihi",
        "date",
        "miktar",
        "tip",
        "tipi",
        "durum",
        "aciklama",
        "aciklamaek",
        "oran",
        "tutar",
    }
    tokens = [token for token in _split_identifier_tokens(column_name) if token not in ignore_tokens]
    if not tokens:
        return ""
    return "".join(tokens[:2])


def _extract_analysis_terms(user_question: str) -> list[str]:
    return [
        term
        for term in _extract_question_terms(user_question)
        if len(term) >= 4 and term not in _GENERIC_QUESTION_TERMS
    ]


def _add_analysis_table_candidate(
    selected: list[dict],
    seen: set[str],
    schema_table_map: dict[str, dict],
    table_name: str,
    max_tables: int,
) -> bool:
    normalized = normalize_identifier(table_name)
    if not table_name or normalized in seen:
        return False

    actual = schema_table_map.get(normalized)
    if actual is None:
        return False

    selected.append(actual)
    seen.add(normalized)
    return len(selected) >= max_tables


def _resolve_analysis_tables(user_question: str, max_tables: int = 3) -> list[dict]:
    schema_table_map = _get_schema_table_map()
    selected = []
    seen = set()
    analysis_terms = _extract_analysis_terms(user_question)

    for term in analysis_terms:
        for table_data in search_tables_local(term, allow_similar=False):
            if _add_analysis_table_candidate(
                selected,
                seen,
                schema_table_map,
                table_data.get("table", ""),
                max_tables,
            ):
                return selected

    for term in analysis_terms:
        for hinted_table in _QUESTION_TABLE_HINTS.get(term, []):
            if _add_analysis_table_candidate(selected, seen, schema_table_map, hinted_table, max_tables):
                return selected

    for term in analysis_terms:
        for column_match in search_columns_local(term, allow_similar=False):
            for table_name in column_match.get("tables", []):
                if _add_analysis_table_candidate(selected, seen, schema_table_map, table_name, max_tables):
                    return selected

    for term in analysis_terms:
        for table_data in search_tables_local(term, allow_similar=True):
            if _add_analysis_table_candidate(
                selected,
                seen,
                schema_table_map,
                table_data.get("table", ""),
                max_tables,
            ):
                return selected

    for term in analysis_terms:
        for column_match in search_columns_local(term, allow_similar=True):
            for table_name in column_match.get("tables", []):
                if _add_analysis_table_candidate(selected, seen, schema_table_map, table_name, max_tables):
                    return selected

    return selected


def _resolve_analysis_columns(table_data: dict, user_question: str, max_columns: int = 6) -> list[str]:
    table_columns = table_data.get("columns", [])
    selected = []
    seen = set()
    analysis_terms = _extract_analysis_terms(user_question)

    for allow_similar in (False, True):
        for term in analysis_terms:
            for match in search_columns_local(term, allow_similar=allow_similar):
                column_name = match.get("column", "")
                if column_name not in table_columns or column_name in seen:
                    continue
                selected.append(column_name)
                seen.add(column_name)
                if len(selected) >= max_columns:
                    return selected

        if selected:
            return selected

    return selected


def _safe_analysis_query(query: str) -> list[dict]:
    normalized_query = " ".join((query or "").strip().split())
    if not normalized_query or not _READ_ONLY_ANALYSIS_PATTERN.match(normalized_query):
        raise ValueError("Only read-only SELECT analysis queries are allowed.")
    if _BLOCKED_ANALYSIS_SQL_PATTERN.search(normalized_query) or _UNSAFE_SQL_PATTERN.search(normalized_query):
        raise ValueError("Unsafe analysis query was blocked.")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.timeout = 8
        try:
            cursor.execute("SET LOCK_TIMEOUT 3000")
        except Exception:
            pass
        try:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        except Exception:
            pass
        cursor.execute(query)
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = cursor.fetchall() if cursor.description else []
        return [dict(zip(columns, row)) for row in rows]


def _get_table_row_count(table_name: str) -> int | None:
    try:
        result = _safe_analysis_query(f"SELECT COUNT_BIG(1) AS RowCount FROM {_quote_identifier(table_name)}")
    except Exception:
        return None

    if not result:
        return None
    return int(result[0].get("RowCount") or 0)


def _profile_column_live(table_name: str, column_name: str) -> dict | None:
    query = f"""
SELECT
    COUNT_BIG(1) AS RowCount,
    SUM(CASE WHEN {_quote_identifier(column_name)} IS NULL THEN 1 ELSE 0 END) AS NullCount,
    SUM(CASE WHEN TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)}) = N'' THEN 1 ELSE 0 END) AS EmptyCount,
    COUNT_BIG(DISTINCT TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)})) AS DistinctCount
FROM {_quote_identifier(table_name)}
""".strip()
    try:
        result = _safe_analysis_query(query)
    except Exception:
        return None

    if not result:
        return None

    try:
        sample_rows = _safe_analysis_query(
            f"""
SELECT TOP 5
    TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)}) AS SampleValue,
    COUNT_BIG(1) AS HitCount
FROM {_quote_identifier(table_name)}
WHERE {_quote_identifier(column_name)} IS NOT NULL
GROUP BY TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)})
ORDER BY COUNT_BIG(1) DESC, TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)})
""".strip()
        )
    except Exception:
        sample_rows = []

    profile = result[0]
    profile["samples"] = [
        {
            "value": row.get("SampleValue"),
            "count": int(row.get("HitCount") or 0),
        }
        for row in sample_rows
    ]
    return profile


def _sample_rows_live(table_name: str, column_names: list[str], limit: int = 10) -> list[dict]:
    selected_columns = []
    seen = set()

    for column_name in column_names:
        if not column_name or column_name in seen:
            continue
        seen.add(column_name)
        selected_columns.append(column_name)
        if len(selected_columns) >= 6:
            break

    if not selected_columns:
        return []

    select_list = ",\n    ".join(
        f"TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(column_name)}) AS {_quote_identifier(column_name)}"
        for column_name in selected_columns
    )
    query = f"""
SELECT TOP {int(limit)}
    {select_list}
FROM {_quote_identifier(table_name)}
WHERE {" OR ".join(f"{_quote_identifier(column_name)} IS NOT NULL" for column_name in selected_columns)}
""".strip()
    try:
        return _safe_analysis_query(query)
    except Exception:
        return []


def _compare_columns_live(table_name: str, left_column: str, right_column: str) -> dict | None:
    query = f"""
SELECT
    COUNT_BIG(1) AS RowCount,
    SUM(CASE WHEN {_quote_identifier(left_column)} IS NULL AND {_quote_identifier(right_column)} IS NULL THEN 1 ELSE 0 END) AS BothNullCount,
    SUM(CASE WHEN TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(left_column)}) = TRY_CONVERT(NVARCHAR(4000), {_quote_identifier(right_column)}) AND {_quote_identifier(left_column)} IS NOT NULL AND {_quote_identifier(right_column)} IS NOT NULL THEN 1 ELSE 0 END) AS EqualNonNullCount,
    SUM(CASE WHEN {_quote_identifier(left_column)} IS NOT NULL AND {_quote_identifier(right_column)} IS NULL THEN 1 ELSE 0 END) AS LeftOnlyCount,
    SUM(CASE WHEN {_quote_identifier(left_column)} IS NULL AND {_quote_identifier(right_column)} IS NOT NULL THEN 1 ELSE 0 END) AS RightOnlyCount
FROM {_quote_identifier(table_name)}
""".strip()
    try:
        result = _safe_analysis_query(query)
    except Exception:
        return None

    return result[0] if result else None


def _build_similar_column_groups(table_data: dict, max_groups: int = 6) -> list[list[str]]:
    groups = {}
    for column_name in table_data.get("columns", []):
        family_key = _column_family_key(column_name)
        if not family_key:
            continue
        groups.setdefault(family_key, []).append(column_name)

    selected_groups = []
    for columns in groups.values():
        if len(columns) < 2:
            continue
        selected_groups.append(sorted(columns))

    selected_groups.sort(key=lambda group: (-len(group), group[0].casefold()))
    return selected_groups[:max_groups]


def _build_unused_column_candidates(table_data: dict, max_columns: int = 10) -> list[str]:
    candidates = []
    for column_name in table_data.get("columns", []):
        normalized = normalize_identifier(column_name)
        if normalized == "id" or normalized.endswith("id") or normalized in {"createddate", "modifieddate", "isdeleted", "aktif", "dbtableid"}:
            continue
        candidates.append(column_name)
        if len(candidates) >= max_columns:
            break
    return candidates


def build_live_analysis_context(user_question: str) -> str:
    analysis_tables = _resolve_analysis_tables(user_question, max_tables=2)
    if not analysis_tables:
        return "No live database context was collected."

    normalized_question = normalize_identifier(user_question)
    wants_unused_analysis = any(
        keyword in normalized_question
        for keyword in ("kullanilmayan", "gereksiz", "bos", "unused", "deprecated", "eski")
    )
    wants_compare_analysis = any(
        keyword in normalized_question
        for keyword in ("ayni", "benzer", "fark", "hangisi", "tercih", "kanonik", "duplicate")
    )
    wants_storage_analysis = any(
        keyword in normalized_question
        for keyword in ("neredetutuluyor", "neredesaklaniyor", "hangitabloda", "hangikolon", "whereisstored", "stored")
    )

    blocks = []
    for table_data in analysis_tables:
        table_name = table_data.get("name", "")
        if not table_name:
            continue

        row_count = _get_table_row_count(table_name)
        block_lines = [f"Table {table_name}"]
        block_lines.append(f"RowCount: {row_count if row_count is not None else 'unknown'}")

        target_columns = _resolve_analysis_columns(table_data, user_question, max_columns=6)
        if not target_columns and wants_unused_analysis:
            target_columns = _build_unused_column_candidates(table_data, max_columns=8)

        if target_columns:
            profile_lines = []
            for column_name in target_columns[:8]:
                profile = _profile_column_live(table_name, column_name)
                if profile is None:
                    continue
                sample_values = ", ".join(
                    [
                        f"{sample.get('value')} ({sample.get('count')})"
                        for sample in profile.get("samples", [])[:3]
                    ]
                ) or "none"
                profile_lines.append(
                    "- %s: null=%s empty=%s distinct=%s null_ratio=%s samples=%s"
                    % (
                        column_name,
                        int(profile.get("NullCount") or 0),
                        int(profile.get("EmptyCount") or 0),
                        int(profile.get("DistinctCount") or 0),
                        (
                            "%0.2f%%"
                            % (
                                (
                                    float(int(profile.get("NullCount") or 0))
                                    / float(int(profile.get("RowCount") or 1))
                                )
                                * 100.0
                            )
                        ),
                        sample_values,
                    )
                )
            if profile_lines:
                block_lines.append("ColumnProfiles:")
                block_lines.extend(profile_lines)
                if wants_storage_analysis:
                    sample_rows = _sample_rows_live(table_name, target_columns[:6], limit=10)
                    if sample_rows:
                        block_lines.append("SampleRowsTop10:")
                        for row in sample_rows[:10]:
                            compact_values = ", ".join(
                                f"{key}={value}"
                                for key, value in row.items()
                            )
                            block_lines.append("- " + compact_values)

        similar_groups = _build_similar_column_groups(table_data, max_groups=4)
        if similar_groups:
            block_lines.append("SimilarColumnFamilies:")
            for group in similar_groups:
                block_lines.append("- " + ", ".join(group))

        if wants_compare_analysis:
            comparisons_done = 0
            if len(target_columns) >= 2:
                comparison_lines = []
                for left_column in target_columns:
                    for right_column in target_columns:
                        if left_column >= right_column:
                            continue
                        comparison = _compare_columns_live(table_name, left_column, right_column)
                        if comparison is None:
                            continue
                        comparison_lines.append(
                            "- %s vs %s: equal_non_null=%s left_only=%s right_only=%s both_null=%s"
                            % (
                                left_column,
                                right_column,
                                int(comparison.get("EqualNonNullCount") or 0),
                                int(comparison.get("LeftOnlyCount") or 0),
                                int(comparison.get("RightOnlyCount") or 0),
                                int(comparison.get("BothNullCount") or 0),
                            )
                        )
                        comparisons_done += 1
                        if comparisons_done >= 4:
                            break
                    if comparisons_done >= 4:
                        break
                if comparison_lines:
                    block_lines.append("ColumnComparisons:")
                    block_lines.extend(comparison_lines)

        blocks.append("\n".join(block_lines))

    return "\n\n".join(blocks) or "No live database context was collected."


def _load_json_document(path: Path, default_payload: dict) -> dict:
    payload = deepcopy(default_payload)

    if not path.exists():
        return payload

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return payload

    if not isinstance(loaded, dict):
        return payload

    payload.update(loaded)
    return payload


def _normalize_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []

    normalized = []
    for value in values:
        if not isinstance(value, str):
            continue

        cleaned = value.strip()
        if cleaned:
            normalized.append(cleaned)

    return normalized


def load_rules():
    rules_data = _load_json_document(RULES_FILE, {"rules": []})

    return {
        "rules": _normalize_string_list(rules_data.get("rules", [])),
    }


def build_request_profile(user_question: str) -> dict:
    return {
        "system_prompt": ANALYSIS_SYSTEM_PROMPT,
        "question_terms": _extract_question_terms(user_question),
    }


def _score_text_for_question(text: str, question_terms: list[str]) -> int:
    normalized_text = normalize_identifier(text)
    if not normalized_text:
        return 0

    score = 0
    for term in question_terms:
        if term == normalized_text:
            score += 50
        elif term in normalized_text or normalized_text in term:
            score += 12

    return score


def select_relevant_rules(
    rules_data: dict,
    user_question: str,
    max_rules: int = 14,
) -> dict:
    question_terms = _extract_question_terms(user_question)
    scored_rules = []
    for rule in rules_data.get("rules", []):
        score = _score_text_for_question(rule, question_terms)
        bonus = 3 if any(keyword in normalize_identifier(rule) for keyword in ("siparis", "cari", "stok", "urun", "aktif")) else 0
        scored_rules.append((score + bonus, rule))

    selected_rules = [
        rule
        for score, rule in sorted(scored_rules, key=lambda item: (-item[0], item[1]))
        if score > 0
    ][:max_rules]
    if not selected_rules:
        selected_rules = rules_data.get("rules", [])[:max_rules]

    return {
        "rules": selected_rules,
    }


def _extract_question_terms(question: str) -> list[str]:
    seen = set()
    terms = []

    for token in _QUESTION_TERM_PATTERN.findall(question or ""):
        normalized = normalize_identifier(token)
        if len(normalized) < 2 or normalized in seen:
            continue

        seen.add(normalized)
        terms.append(normalized)

        if "musteri" in normalized and "cari" not in seen:
            seen.add("cari")
            terms.append("cari")
        if "siparis" in normalized and "siparisdetay" not in seen:
            seen.add("siparisdetay")
            terms.append("siparisdetay")
        if "urun" in normalized:
            for synonym in ("stok", "stokurunmaster"):
                if synonym not in seen:
                    seen.add(synonym)
                    terms.append(synonym)

    return terms


def _schema_table_lookup(schema: dict) -> dict[str, dict]:
    return {
        normalize_identifier(table.get("name", "")): table
        for table in schema.get("tables", [])
        if table.get("name")
    }


def _select_table_columns(table_data: dict, question_terms: list[str], max_columns: int = 18) -> list[str]:
    columns = table_data.get("columns", [])
    if len(columns) <= max_columns:
        return columns

    scored_columns = []
    for index, column_name in enumerate(columns):
        normalized_column = normalize_identifier(column_name)
        score = 0

        if normalized_column == "id":
            score += 1000
        if normalized_column.endswith("id"):
            score += 150
        if any(hint in normalized_column for hint in _IMPORTANT_COLUMN_HINTS):
            score += 35
        if normalized_column in question_terms:
            score += 120
        elif any(len(term) >= 3 and term in normalized_column for term in question_terms):
            score += 40
        score += max(0, 20 - index)

        scored_columns.append((score, index, column_name))

    scored_columns.sort(key=lambda item: (-item[0], item[1], item[2]))
    selected = [column_name for _, _, column_name in scored_columns[:max_columns]]
    if "Id" in columns and "Id" not in selected:
        selected[-1] = "Id"

    selected_lookup = set(selected)
    ordered_selected = [column_name for column_name in columns if column_name in selected_lookup]
    return ordered_selected


def _select_ai_table_columns(table_data: dict, max_columns: int = 12) -> list[str]:
    columns = table_data.get("columns", [])
    if len(columns) <= max_columns:
        return columns

    scored_columns = []
    for index, column_name in enumerate(columns):
        normalized_column = normalize_identifier(column_name)
        score = 0

        if normalized_column == "id":
            score += 2000
        if normalized_column.endswith("id"):
            score += 300
        if "date" in normalized_column or "tarih" in normalized_column:
            score += 220
        if any(hint in normalized_column for hint in ("kod", "adi", "ad", "ref", "miktar", "durum", "aktif", "no")):
            score += 90
        if normalized_column in {"createddate", "modifieddate", "isdeleted", "aktif"}:
            score += 160
        score += max(0, 25 - index)

        scored_columns.append((score, index, column_name))

    scored_columns.sort(key=lambda item: (-item[0], item[1], item[2]))
    selected = [column_name for _, _, column_name in scored_columns[:max_columns]]
    if "Id" in columns and "Id" not in selected:
        selected[-1] = "Id"

    selected_lookup = set(selected)
    return [column_name for column_name in columns if column_name in selected_lookup]


def _build_local_column_index(schema_tables: list[dict]) -> dict[str, list[str]]:
    column_index = {}

    for table_data in schema_tables:
        table_name = table_data.get("name", "")
        if not table_name:
            continue

        for column_name in table_data.get("columns", []):
            column_index.setdefault(column_name, [])
            if table_name not in column_index[column_name]:
                column_index[column_name].append(table_name)

    return dict(sorted(column_index.items(), key=lambda item: item[0].casefold()))


def build_ai_schema_cache(schema: dict, max_columns_per_table: int = 12, max_relations_per_table: int = 6) -> dict:
    ai_tables = []

    for table_data in schema.get("tables", []):
        table_name = table_data.get("name", "")
        if not table_name:
            continue

        selected_columns = _select_ai_table_columns(table_data, max_columns=max_columns_per_table)
        selected_relations = table_data.get("relations", [])[:max_relations_per_table]
        date_columns = [
            column_name
            for column_name in table_data.get("columns", [])
            if "date" in normalize_identifier(column_name) or "tarih" in normalize_identifier(column_name)
        ][:4]

        ai_table = {
            "name": table_name,
            "columns": selected_columns,
            "all_column_count": len(table_data.get("columns", [])),
        }
        if date_columns:
            ai_table["date_columns"] = date_columns
        if selected_relations:
            ai_table["relations"] = selected_relations

        ai_tables.append(ai_table)

    ai_schema = {
        "tables": ai_tables,
    }
    return ai_schema


def build_compact_schema_text(schema: dict, user_question: str, max_columns_per_table: int = 18) -> str:
    question_terms = _extract_question_terms(user_question)
    blocks = []

    for table in schema.get("tables", []):
        table_name = table.get("name", "")
        columns = _select_table_columns(table, question_terms, max_columns=max_columns_per_table)
        hidden_column_count = max(0, table.get("all_column_count", len(table.get("columns", []))) - len(columns))
        relations = table.get("relations", [])
        date_columns = table.get("date_columns", [])

        block_lines = [f"TABLE {table_name}"]
        block_lines.append(f"Columns: {', '.join(columns)}")
        if hidden_column_count:
            block_lines.append(f"MoreColumnsOmitted: {hidden_column_count}")
        if date_columns:
            block_lines.append(f"DateColumns: {', '.join(date_columns)}")

        if relations:
            relation_parts = [
                f"{relation['column']} -> {relation['ref_table']}.Id"
                for relation in relations[:8]
            ]
            block_lines.append(f"Joins: {'; '.join(relation_parts)}")

        blocks.append("\n".join(block_lines))

    return "\n\n".join(blocks)


def _score_table_for_question(table_data: dict, question_blob: str, question_terms: list[str]) -> int:
    table_name = table_data.get("name", "")
    normalized_table = normalize_identifier(table_name)
    if not normalized_table:
        return 0

    score = 0
    matched_table_terms = 0

    if normalized_table in question_blob:
        score += 120

    for term in question_terms:
        if term == normalized_table:
            score += 140
            matched_table_terms += 1
        elif term in normalized_table or normalized_table in term:
            score += 80
            matched_table_terms += 1

    if matched_table_terms:
        score += matched_table_terms * 45

    for column_name in table_data.get("columns", []):
        normalized_column = normalize_identifier(column_name)
        if not normalized_column:
            continue

        if normalized_column in question_terms and normalized_column not in _GENERIC_QUESTION_TERMS:
            score += 8
        elif any(
            len(term) >= 4 and term not in _GENERIC_QUESTION_TERMS and term in normalized_column
            for term in question_terms
        ):
            score += 2

    for relation in table_data.get("relations", []):
        normalized_relation_column = normalize_identifier(relation.get("column", ""))
        normalized_ref_table = normalize_identifier(relation.get("ref_table", ""))

        if normalized_relation_column in question_terms and normalized_relation_column not in _GENERIC_QUESTION_TERMS:
            score += 4
        if normalized_ref_table and normalized_ref_table in question_blob:
            score += 12

    return score


def select_relevant_schema(schema: dict, user_question: str, max_tables: int = 18) -> dict:
    tables = schema.get("tables", [])
    if len(tables) <= max_tables:
        return schema

    question_terms = _extract_question_terms(user_question)
    question_blob = "".join(question_terms)
    table_by_name = {
        table_data.get("name", ""): table_data
        for table_data in tables
        if table_data.get("name")
    }
    scored_tables = []

    for table_data in tables:
        score = _score_table_for_question(table_data, question_blob, question_terms)
        if score > 0:
            scored_tables.append((score, table_data.get("name", ""), table_data))

    if not scored_tables:
        return {
            "tables": tables[:max_tables],
            "column_index": schema.get("column_index", {}),
        }

    scored_tables.sort(key=lambda item: (-item[0], item[1].casefold()))
    selected_names = []
    selected_lookup = {}

    for term in question_terms:
        for hinted_table in _QUESTION_TABLE_HINTS.get(term, []):
            table_data = table_by_name.get(hinted_table)
            if table_data is None or hinted_table in selected_lookup:
                continue

            selected_lookup[hinted_table] = table_data
            selected_names.append(hinted_table)
            if len(selected_lookup) >= max(6, max_tables // 2):
                break

        if len(selected_lookup) >= max(6, max_tables // 2):
            break

    for _, table_name, table_data in scored_tables:
        if table_name in selected_lookup:
            continue

        selected_lookup[table_name] = table_data
        selected_names.append(table_name)
        if len(selected_lookup) >= max(6, max_tables // 2):
            break

    adjacency = {}
    for table_data in tables:
        table_name = table_data.get("name")
        if not table_name:
            continue

        adjacency.setdefault(table_name, set())
        for relation in table_data.get("relations", []):
            ref_table = relation.get("ref_table")
            if not ref_table:
                continue

            adjacency[table_name].add(ref_table)
            adjacency.setdefault(ref_table, set()).add(table_name)

    for table_name in list(selected_names):
        for related_table in sorted(adjacency.get(table_name, set())):
            if related_table in selected_lookup:
                continue

            related_table_data = next(
                (item for item in tables if item.get("name") == related_table),
                None,
            )
            if related_table_data is None:
                continue

            selected_lookup[related_table] = related_table_data
            if len(selected_lookup) >= max_tables:
                break

        if len(selected_lookup) >= max_tables:
            break

    selected_tables = [
        selected_lookup[table_name]
        for table_name in selected_lookup
    ]
    selected_column_names = {
        column_name
        for table_data in selected_tables
        for column_name in table_data.get("columns", [])
    }
    selected_column_index = {
        column_name: related_tables
        for column_name, related_tables in schema.get("column_index", {}).items()
        if column_name in selected_column_names
    }

    return {
        "tables": selected_tables,
        "column_index": selected_column_index,
    }


def build_rule_prompt_sections(
    rules_data: dict,
    user_question: str,
    request_profile: dict | None = None,
) -> dict:
    request_profile = request_profile or build_request_profile(user_question)
    relevant_rules_data = select_relevant_rules(
        rules_data,
        user_question,
        max_rules=14,
    )

    domain_rules = "\n".join(f"- {rule}" for rule in relevant_rules_data.get("rules", [])) or "- No domain rules loaded."

    guidance_text = f"""
Domain rules:
{domain_rules}
""".strip()

    return {
        "selected": relevant_rules_data,
        "domain_rules": domain_rules,
        "guidance_text": guidance_text,
    }


def _extract_repository_context_terms(user_question: str) -> list[str]:
    terms = []
    seen = set()

    for raw_token in _QUESTION_TERM_PATTERN.findall(user_question or ""):
        token = raw_token.strip()
        normalized = normalize_identifier(token)
        if len(normalized) < 3:
            continue

        if normalized in seen:
            continue

        seen.add(normalized)
        terms.append(token)

    for match in search_tables_local(user_question.strip()):
        table_name = match.get("table")
        normalized = normalize_identifier(table_name or "")
        if table_name and normalized and normalized not in seen:
            seen.add(normalized)
            terms.append(table_name)

    for match in search_columns_local(user_question.strip()):
        column_name = match.get("column")
        normalized = normalize_identifier(column_name or "")
        if column_name and normalized and normalized not in seen:
            seen.add(normalized)
            terms.append(column_name)

    return terms[:8]


def build_repository_usage_context(user_question: str, max_matches: int = 24) -> str:
    sql_dir = PROJECT_ROOT / "sql_procedures"
    if not sql_dir.exists():
        return ""

    terms = _extract_repository_context_terms(user_question)
    if not terms:
        return ""

    lowered_terms = [term.casefold() for term in terms]
    matches = []

    for file_path in sorted(sql_dir.glob("*.sql")):
        try:
            lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue

        for line_no, line in enumerate(lines, start=1):
            lowered_line = line.casefold()
            if not any(term in lowered_line for term in lowered_terms):
                continue

            compact_line = " ".join(line.strip().split())
            if not compact_line:
                continue

            matches.append(f"{file_path.name}:{line_no}: {compact_line[:220]}")
            if len(matches) >= max_matches:
                return "\n".join(matches)

    return "\n".join(matches)


def build_analysis_prompt(
    schema_text: str,
    rules_data: dict,
    user_question: str,
    include_repository_usage: bool = True,
    include_live_database: bool = True,
) -> str:
    request_profile = build_request_profile(user_question)
    rule_sections = build_rule_prompt_sections(rules_data, user_question, request_profile=request_profile)
    repository_usage = "Repository usage skipped for this pass."
    live_database_context = "Live database context skipped for this pass."

    if include_repository_usage:
        repository_usage = build_repository_usage_context(user_question, max_matches=12) or "No direct repository usage snippets found."

    if include_live_database:
        try:
            live_database_context = build_live_analysis_context(user_question)
        except Exception as error:
            live_database_context = f"Live database context could not be collected: {_format_db_error(error)}"

    return f"""
You are helping a developer understand a real ERP schema and legacy database design.

Important notes:
- Answer in Turkish.
- Do not output SQL, stored procedures, query snippets, or SQL code blocks.
- If the user asks for SQL, answer by explaining the relevant tables, columns, filters, and relationships instead.
- You are a database analyst.
- Guessing from table names is forbidden.
- Do not answer before using live data evidence when it is available.
- Focus on interpretation, tradeoffs, likely canonical columns, duplicate-looking fields, table intent, and repository evidence.
- If the user asks which column should be preferred, make the best evidence-based recommendation.
- Separate what is certain from what is only likely.
- If evidence is insufficient, explicitly say "emin degilim".
- For "X nerede tutuluyor?" questions, test whether the table really stores the value, whether the relevant columns actually contain data, and whether the table is a movement or aggregate table.

Domain rules:
{rule_sections["domain_rules"]}

Repository usage clues:
{repository_usage}

Live database clues:
{live_database_context}

Schema:
{schema_text}

User:
{user_question}

Return a practical assistant answer, not SQL.
Use this exact response shape:
1. Incelenen tablolar
2. Yapilan kontroller
3. Bulgular
4. SONUC
""".strip()


def ask_ai(prompt, system_prompt: str | None = None, temperature: float = 0):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": temperature,
        },
    }
    if system_prompt:
        payload["system"] = system_prompt

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json=payload,
            timeout=240,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        response = getattr(error, "response", None)
        response_text = ""
        if response is not None:
            try:
                response_text = response.text.strip()
            except Exception:
                response_text = ""
        detail = response_text[:600] if response_text else str(error)
        raise RuntimeError(f"Ollama request failed: {detail}") from error

    return response.json()["response"]


def _format_db_error(error):
    if getattr(error, "args", None):
        parts = [str(part) for part in error.args if part]
        if parts:
            return " ".join(parts)
    return str(error)


def _looks_like_sql_response(answer: str) -> bool:
    return bool(_SQL_LIKE_RESPONSE_PATTERN.match(answer or ""))


def enforce_assistant_output(invalid_response: str, schema_text: str, user_question: str) -> str:
    prompt = f"""
Your previous answer incorrectly returned SQL or SQL-like output.

Schema:
{schema_text}

User:
{user_question}

Invalid previous answer:
{invalid_response}

Return a Turkish assistant answer.
Do not return SQL.
Do not include query snippets, stored procedures, DDL, DML, or SQL code fences.
Explain the schema, table/column reasoning, and relationships directly.
""".strip()
    return ask_ai(prompt, system_prompt=ANALYSIS_SYSTEM_PROMPT, temperature=0)


def generate_response(user_question: str) -> str:
    rules_data = load_rules()
    ai_schema = get_ai_schema_data()
    schema_variants = []
    seen_variants = set()

    for max_tables in (10, 16):
        schema_variant = select_relevant_schema(ai_schema, user_question, max_tables=max_tables)
        variant_key = tuple(sorted(table.get("name", "") for table in schema_variant.get("tables", [])))
        if variant_key in seen_variants:
            continue
        seen_variants.add(variant_key)
        schema_variants.append(schema_variant)

    if not schema_variants:
        schema_variants.append(ai_schema)

    last_answer = ""
    last_error = ""
    for schema_variant in schema_variants:
        for max_columns_per_table, include_repository_usage, include_live_database in (
            (20, True, True),
            (14, True, False),
            (10, False, False),
        ):
            schema_text = build_compact_schema_text(
                schema_variant,
                user_question,
                max_columns_per_table=max_columns_per_table,
            )
            prompt = build_analysis_prompt(
                schema_text,
                rules_data,
                user_question,
                include_repository_usage=include_repository_usage,
                include_live_database=include_live_database,
            )

            try:
                answer = ask_ai(prompt, system_prompt=ANALYSIS_SYSTEM_PROMPT, temperature=0)
            except Exception as error:
                last_error = _format_db_error(error)
                continue

            last_answer = answer or last_answer

            if _looks_like_sql_response(answer):
                answer = enforce_assistant_output(answer, schema_text, user_question)
                last_answer = answer or last_answer

            if answer and not _looks_like_sql_response(answer):
                return answer.strip()

    if last_answer:
        return last_answer.strip()
    if last_error:
        return f"Yerel modelden yanit alinamadi. Detay: {last_error}"
    return "Yanit uretilemedi."


if __name__ == "__main__":
    schema_text = get_schema_text()
    rules_data = load_rules()
    question = "Sip_Siparis ile Sip_SiparisDetay arasındaki fark nedir?"
    prompt = build_analysis_prompt(schema_text, rules_data, question)
    sql = ask_ai(prompt, system_prompt=ANALYSIS_SYSTEM_PROMPT, temperature=0)

    print("\n ÜRETİLEN YANIT:\n")
    print(sql)
