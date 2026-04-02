import pyodbc
import json
import logging
import re
import requests
from datetime import datetime, timezone
from pathlib import Path

from schema_optimizer import build_optimized_schema, normalize_identifier


CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=WinPremiumCanpak;"
    "UID=sa;"
    "PWD=YourStrong!Pass123;"
    "TrustServerCertificate=yes;"
)

_UNSAFE_SQL_PATTERN = re.compile(
    r"\b(?:DROP|DELETE|UPDATE|ALTER|TRUNCATE)\b",
    re.IGNORECASE,
)
_CODE_FENCE_PATTERN = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
_JSON_BLOCK_PATTERN = re.compile(r"```(?:json)?\s*(\{.*\}|\[.*\])\s*```", re.IGNORECASE | re.DOTALL)
_ROW_LIMIT_PATTERN = re.compile(
    r"\b(?:LIMIT\s+\d+|TOP\s*\(?\s*\d+\s*\)?|FETCH\s+NEXT\s+\d+\s+ROWS\s+ONLY)\b",
    re.IGNORECASE,
)
_SELECT_DISTINCT_PATTERN = re.compile(r"^\s*SELECT\s+DISTINCT\b", re.IGNORECASE)
_SELECT_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE)
_SQL_TABLE_REF_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+((?:\[[^\]]+\]|[#@A-Za-z_][\w$#@]*)(?:\.(?:\[[^\]]+\]|[#@A-Za-z_][\w$#@]*))?)"
    r"(?:\s+(?:AS\s+)?(\[[^\]]+\]|[#@A-Za-z_][\w$#@]*))?",
    re.IGNORECASE,
)
_SQL_QUALIFIED_COLUMN_PATTERN = re.compile(
    r"(\[[^\]]+\]|[#@A-Za-z_][\w$#@]*)\s*\.\s*(\[[^\]]+\]|[#@A-Za-z_][\w$#@]*|\*)",
    re.IGNORECASE,
)
_SQL_CTE_PATTERN = re.compile(
    r"(?:\bWITH\b|,)\s*(\[[^\]]+\]|[#@A-Za-z_][\w$#@]*)\s+AS\s*\(",
    re.IGNORECASE,
)
_STAR_SELECT_PATTERN = re.compile(r"(?i)\bSELECT\s+(?:TOP\s*\(?\s*\d+\s*\)?\s+)?(?:DISTINCT\s+)?\*")
_ALIASED_STAR_SELECT_PATTERN = re.compile(r"(?i)\b[#@A-Za-z_][\w$#@]*\s*\.\s*\*")
_PROCEDURAL_SQL_PATTERN = re.compile(r"\b(?:CREATE\s+PROCEDURE|ALTER\s+PROCEDURE|BEGIN\b|END\b)\b", re.IGNORECASE)
_QUESTION_TERM_PATTERN = re.compile(r"\w+", re.UNICODE)
RULES_FILE = Path(__file__).resolve().parent / "rules.json"
LEARNING_FILE = Path(__file__).resolve().parent / "learning.json"
SCHEMA_CACHE_FILE = Path(__file__).resolve().parent / "schema_cache.json"
SCHEMA_CACHE_META_FILE = Path(__file__).resolve().parent / "schema_cache.meta.json"
SCHEMA_AI_CACHE_FILE = Path(__file__).resolve().parent / "schema_cache.ai.json"
SCHEMA_CACHE_VERSION = 4
LOGGER = logging.getLogger(__name__)
OLLAMA_MODEL = "qwen2.5-coder:14b"
SQL_SYSTEM_PROMPT = """
You are a Microsoft SQL Server assistant.

Guidelines:
- Prefer using the exact table and column names that appear in the provided schema.
- Do not invent generic demo tables like Orders, Customers, Products when the real schema gives better names.
- When the user asks for only SQL, return only SQL.
- If part of the request is ambiguous, make the best schema-grounded choice instead of explaining at length.
""".strip()
PROCEDURE_SYSTEM_PROMPT = """
You are a Microsoft SQL Server stored procedure assistant.

Guidelines:
- Generate valid T-SQL for the user's requested stored procedure.
- Prefer using the exact table and column names that appear in the provided schema.
- Keep the procedure read-only unless the user explicitly asks for write operations.
- When the user asks for only SQL, return only SQL.
""".strip()
_SQL_KEYWORDS = {
    "and",
    "as",
    "by",
    "cross",
    "desc",
    "except",
    "fetch",
    "from",
    "full",
    "group",
    "having",
    "inner",
    "intersect",
    "join",
    "left",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "right",
    "union",
    "where",
}
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
    "alan",
    "getir",
    "goster",
    "gore",
    "icin",
    "ile",
    "list",
    "liste",
    "listesi",
    "olan",
    "sonra",
    "sonrasi",
    "tarih",
    "tum",
    "ve",
    "ver",
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
_PROCEDURE_REQUEST_TERMS = ("storedprocedure", "procedure", "prosedur", "proc", "sp")


def get_connection():
    return pyodbc.connect(CONNECTION_STRING)


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


def refresh_schema_cache() -> dict:
    schema = extract_schema()
    ai_schema = build_ai_schema_cache(schema)
    identity = get_database_identity()

    with open(SCHEMA_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)

    with open(SCHEMA_AI_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(ai_schema, f, ensure_ascii=False, indent=2)

    with open(SCHEMA_CACHE_META_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "schema_version": SCHEMA_CACHE_VERSION,
                "database_identity": identity,
                "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
                "table_count": len(schema.get("tables", [])),
                "ai_table_count": len(ai_schema.get("tables", [])),
                "ai_char_count": len(build_ai_input(ai_schema)),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    LOGGER.info("Optimized schema cache refreshed: %s", SCHEMA_CACHE_FILE)
    return schema


def load_schema_cache() -> dict:
    if not SCHEMA_CACHE_FILE.exists() or not SCHEMA_CACHE_META_FILE.exists() or not SCHEMA_AI_CACHE_FILE.exists():
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

    if "column_index" not in data or not isinstance(data["column_index"], dict):
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


def search_columns_local(column_name: str) -> list[dict]:
    needle = column_name.strip()
    normalized_needle = normalize_identifier(needle)
    if not normalized_needle:
        return []

    schema = get_schema_data()
    exact_matches = []
    similar_matches = []

    for current_name, tables in sorted(schema.get("column_index", {}).items()):
        normalized_name = normalize_identifier(current_name)
        entry = {
            "column": current_name,
            "tables": tables,
        }

        if normalized_name == normalized_needle:
            exact_matches.append(entry)
        elif normalized_needle in normalized_name or normalized_name.endswith(normalized_needle):
            similar_matches.append(entry)

    return exact_matches or similar_matches


def search_tables_local(table_name: str) -> list[dict]:
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

    return exact_matches or similar_matches


def load_rules():
    with open(RULES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_learning() -> dict:
    if not LEARNING_FILE.exists():
        LEARNING_FILE.write_text(
            json.dumps({"examples": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    with open(LEARNING_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "examples" not in data or not isinstance(data["examples"], list):
        data["examples"] = []

    return data


def save_learning(question: str, wrong_sql: str, correct_sql: str, reason: str = "") -> None:
    learning_data = load_learning()
    learning_data["examples"].append(
        {
            "question": question,
            "wrong_sql": wrong_sql,
            "correct_sql": correct_sql,
            "reason": reason,
        }
    )

    with open(LEARNING_FILE, "w", encoding="utf-8") as f:
        json.dump(learning_data, f, ensure_ascii=False, indent=2)


def build_learning_section(learning_data: dict) -> str:
    examples = learning_data.get("examples", [])[-5:]
    blocks = []

    for example in examples:
        blocks.append(
            "\n".join(
                [
                    "Previous mistake:",
                    f"User: {example.get('question', '')}",
                    f"Wrong SQL: {example.get('wrong_sql', '')}",
                    f"Correct SQL: {example.get('correct_sql', '')}",
                    f"Reason: {example.get('reason', '')}",
                    "-----------",
                ]
            )
        )

    return "\n".join(blocks)


def build_request_profile(user_question: str) -> dict:
    normalized_question = normalize_identifier(user_question)
    wants_procedure = any(term in normalized_question for term in _PROCEDURE_REQUEST_TERMS)
    return {
        "wants_procedure": wants_procedure,
        "system_prompt": PROCEDURE_SYSTEM_PROMPT if wants_procedure else SQL_SYSTEM_PROMPT,
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


def select_relevant_rules(rules_data: dict, user_question: str, max_rules: int = 14, max_examples: int = 5) -> dict:
    question_terms = _extract_question_terms(user_question)

    scored_rules = []
    for rule in rules_data.get("rules", []):
        score = _score_text_for_question(rule, question_terms)
        bonus = 3 if any(keyword in normalize_identifier(rule) for keyword in ("siparis", "cari", "stok", "urun", "aktif")) else 0
        scored_rules.append((score + bonus, rule))

    scored_examples = []
    for example in rules_data.get("examples", []):
        example_text = " ".join([example.get("question", ""), example.get("sql", "")])
        score = _score_text_for_question(example_text, question_terms)
        scored_examples.append((score, example))

    selected_rules = [
        rule
        for score, rule in sorted(scored_rules, key=lambda item: (-item[0], item[1]))
        if score > 0
    ][:max_rules]
    if not selected_rules:
        selected_rules = rules_data.get("rules", [])[:max_rules]

    selected_examples = [
        example
        for score, example in sorted(
            scored_examples,
            key=lambda item: (-item[0], item[1].get("question", "")),
        )
        if score > 0
    ][:max_examples]
    if not selected_examples:
        selected_examples = rules_data.get("examples", [])[:max_examples]

    return {
        "rules": selected_rules,
        "examples": selected_examples,
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
        "column_index": _build_local_column_index(ai_tables),
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


def _clean_sql_identifier(identifier: str) -> str:
    if not identifier:
        return ""

    cleaned = identifier.strip().strip(",;")
    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]

    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]

    return cleaned


def _extract_cte_names(sql: str) -> set[str]:
    return {
        normalize_identifier(_clean_sql_identifier(name))
        for name in _SQL_CTE_PATTERN.findall(sql)
        if name
    }


def _extract_table_references(sql: str) -> tuple[list[str], dict[str, str]]:
    table_names = []
    alias_map = {}

    for raw_table, raw_alias in _SQL_TABLE_REF_PATTERN.findall(sql):
        table_name = _clean_sql_identifier(raw_table)
        if not table_name:
            continue

        table_names.append(table_name)
        alias_map[normalize_identifier(table_name)] = table_name

        alias_name = _clean_sql_identifier(raw_alias)
        if alias_name and alias_name.casefold() not in _SQL_KEYWORDS:
            alias_map[normalize_identifier(alias_name)] = table_name

    return table_names, alias_map


def validate_sql_grounding(sql: str, schema: dict, allow_procedural: bool = False) -> list[str]:
    normalized_sql = _normalize_sql(sql, allow_procedural=allow_procedural)
    issues = []

    if not normalized_sql:
        expected = "stored procedure" if allow_procedural else "SELECT/WITH query"
        return [f"Model did not return a valid {expected}."]

    if _PROCEDURAL_SQL_PATTERN.search(sql) and not allow_procedural:
        issues.append("Procedural SQL wrappers are not allowed; return a single query only.")

    schema_lookup = _schema_table_lookup(schema)
    if not schema_lookup:
        return issues

    known_ctes = _extract_cte_names(normalized_sql)
    table_names, alias_map = _extract_table_references(normalized_sql)
    if not table_names:
        issues.append("Query must reference at least one table from the schema.")
        return issues

    table_columns = {
        normalize_identifier(table.get("name", "")): {
            normalize_identifier(column_name)
            for column_name in table.get("columns", [])
        }
        for table in schema.get("tables", [])
        if table.get("name")
    }

    referenced_schema_tables = 0
    for table_name in table_names:
        normalized_table = normalize_identifier(table_name)
        if normalized_table in known_ctes:
            continue

        if normalized_table not in schema_lookup:
            issues.append(f"Unknown table `{table_name}`. Use only schema tables.")
            continue

        referenced_schema_tables += 1

    if referenced_schema_tables == 0:
        issues.append("Query did not use any validated schema table.")

    for alias_name, column_name in _SQL_QUALIFIED_COLUMN_PATTERN.findall(normalized_sql):
        if column_name == "*":
            continue

        normalized_alias = normalize_identifier(_clean_sql_identifier(alias_name))
        resolved_table = alias_map.get(normalized_alias)
        if not resolved_table:
            continue

        normalized_table = normalize_identifier(resolved_table)
        if normalized_table not in table_columns:
            continue

        normalized_column = normalize_identifier(_clean_sql_identifier(column_name))
        if normalized_column and normalized_column not in table_columns[normalized_table]:
            issues.append(f"Unknown column `{column_name}` on table `{resolved_table}`.")

    deduped_issues = []
    seen = set()
    for issue in issues:
        if issue in seen:
            continue

        seen.add(issue)
        deduped_issues.append(issue)

    return deduped_issues


def validate_sql_syntax(sql: str, allow_procedural: bool = False) -> tuple[bool, str | None]:
    try:
        with get_connection() as conn:
            return validate_sql(conn, sql, allow_procedural=allow_procedural)
    except Exception as error:
        LOGGER.warning("SQL syntax validation skipped: %s", _format_db_error(error))
        return True, None


def build_prompt(schema_text, rules_data, user_question, only_sql=False, request_profile: dict | None = None):
    request_profile = request_profile or build_request_profile(user_question)
    relevant_rules_data = select_relevant_rules(rules_data, user_question)
    rules = "\n".join(f"- {rule}" for rule in relevant_rules_data.get("rules", []))
    learning_data = load_learning()
    learning_section = build_learning_section(learning_data) or "No previous corrections yet."
    pattern_examples = "\n".join(
        [
            "\n".join(
                [
                    f"User intent: {example.get('question', '')}",
                    f"Preferred pattern: {example.get('sql', '')}",
                    "---",
                ]
            )
            for example in relevant_rules_data.get("examples", [])
        ]
    )
    if request_profile.get("wants_procedure"):
        extra = "Return ONLY the stored procedure SQL code."
    else:
        extra = "Return ONLY SQL query." if only_sql else "Return SQL first."
    procedure_pattern = """
Procedure pattern hint:
- Prefer `CREATE OR ALTER PROCEDURE [dbo].[Get_Siparisler]`.
- Use SQL Server parameter syntax like `@PageNo INT`, `@PageSize INT`, `@ChangedAfter DATETIME`.
- Never use MySQL-style `IN` / `OUT` parameter modifiers.
- Join `Sip_SiparisDetay.SiparisId = Sip_Siparis.Id`.
- Join `Sip_Siparis.CariId = Cr_Cari.Id`.
- Join `Sip_SiparisDetay.StokUrunMasterId = Gnl_StokUrunMaster.Id`.
- For "changed after" filtering, prefer real schema dates such as `ISNULL(Sip_Siparis.ModifiedDate, Sip_Siparis.CreatedDate)` or the same pattern on joined tables when those columns exist.
- If change-tracking dates are unavailable in the selected tables, use the nearest real date column from schema like `Sip_Siparis.SiparisTarihi` or `Sip_SiparisDetay.OnayTarih`; never invent names like `DegisiklikTarihi`.
- For pagination prefer `ROW_NUMBER()` or `OFFSET ... FETCH`.
""".strip() if request_profile.get("wants_procedure") else ""
    output_contract = (
        "- Output one read-only stored procedure."
        if request_profile.get("wants_procedure")
        else "- Output one read-only query."
    )

    prompt = f"""
You are a senior SQL engineer working against a real production schema.

Important notes:
- Use the schema section below as the primary source of truth for table and column names.
- Avoid inventing generic fallback names when real schema names exist.
- {"Stored procedure output is expected for this request." if request_profile.get("wants_procedure") else "Return a query unless the user explicitly asks for a stored procedure."}
- Foreign-key joins usually follow `<Table>.<ForeignKeyColumn> = <ReferencedTable>.Id` unless schema suggests otherwise.
{output_contract}

Rules:
{rules}

Domain patterns:
{pattern_examples}

Learn from previous corrections:
{learning_section}

{procedure_pattern}

Schema:
{schema_text}

User:
{user_question}

{extra}
"""
    return prompt


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

    response = requests.post(
        "http://localhost:11434/api/generate",
        json=payload,
        timeout=240,
    )
    response.raise_for_status()

    return response.json()["response"]


def _format_db_error(error):
    if getattr(error, "args", None):
        parts = [str(part) for part in error.args if part]
        if parts:
            return " ".join(parts)
    return str(error)


def _looks_like_sql(sql: str, allow_procedural: bool = False) -> bool:
    normalized_sql = _normalize_sql(sql, allow_procedural=allow_procedural).lstrip()
    if not normalized_sql:
        return False

    if allow_procedural:
        return bool(re.match(r"(?is)^(CREATE|ALTER|CREATE\s+OR\s+ALTER|WITH|SELECT)\b", normalized_sql))

    return bool(re.match(r"(?is)^(WITH|SELECT)\b", normalized_sql))


def _normalize_sql(sql, allow_procedural: bool = False):
    if not sql:
        return ""

    fenced_sql = _CODE_FENCE_PATTERN.search(sql)
    if fenced_sql:
        sql = fenced_sql.group(1)

    if not allow_procedural:
        statement_start = re.search(r"(?im)^\s*(SELECT|WITH)\b", sql)
        if statement_start:
            sql = sql[statement_start.start():]

    return sql.strip()


def _ensure_top_100(sql, allow_procedural: bool = False):
    normalized_sql = _normalize_sql(sql, allow_procedural=allow_procedural)
    return normalized_sql


def is_safe_query(sql: str) -> bool:
    normalized_sql = _normalize_sql(sql)
    return not bool(_UNSAFE_SQL_PATTERN.search(normalized_sql))


def validate_sql(conn, sql: str, allow_procedural: bool = False) -> tuple[bool, str | None]:
    normalized_sql = _normalize_sql(sql, allow_procedural=allow_procedural)
    parse_cursor = conn.cursor()

    try:
        parse_cursor.execute("SET PARSEONLY ON")
        parse_cursor.execute(normalized_sql)
        return True, None
    except Exception as error:
        return False, _format_db_error(error)
    finally:
        try:
            parse_cursor.execute("SET PARSEONLY OFF")
        except Exception:
            pass

        try:
            parse_cursor.close()
        except Exception:
            pass


def fix_sql(schema_text: str, bad_sql: str, error: str, request_profile: dict | None = None) -> str:
    request_profile = request_profile or {"wants_procedure": False, "system_prompt": SQL_SYSTEM_PROMPT}
    prompt = f"""
You are fixing a Microsoft SQL Server query.

Schema:
{schema_text}

Broken SQL:
{bad_sql}

Validation Error:
{error}

Return ONLY the corrected SQL{" stored procedure" if request_profile.get("wants_procedure") else " query"}.
Keep the SQL read-only.
Use only tables and columns that exist in the schema.
Keep the style close to the user's request.
{"Stored procedure output is allowed." if request_profile.get("wants_procedure") else ""}
"""
    fixed_sql = ask_ai(prompt, system_prompt=request_profile.get("system_prompt"), temperature=0)
    return _ensure_top_100(fixed_sql, allow_procedural=request_profile.get("wants_procedure", False))


def enforce_sql_output(
    invalid_response: str,
    schema_text: str,
    user_question: str,
    request_profile: dict,
) -> str:
    prompt = f"""
Your previous answer violated the output contract.

User request:
{user_question}

Schema:
{schema_text}

Invalid previous answer:
{invalid_response}

Return ONLY valid {"stored procedure" if request_profile.get("wants_procedure") else "query"} SQL.
No explanation.
No markdown.
Use only schema tables and columns.
"""
    repaired = ask_ai(prompt, system_prompt=request_profile.get("system_prompt"), temperature=0)
    return _ensure_top_100(repaired, allow_procedural=request_profile.get("wants_procedure", False))


def execute_sql(conn, sql: str) -> list[dict]:
    normalized_sql = _normalize_sql(sql)
    query_cursor = conn.cursor()

    try:
        query_cursor.execute(normalized_sql)

        if query_cursor.description is None:
            return []

        columns = [column[0] for column in query_cursor.description]
        rows = query_cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        try:
            query_cursor.close()
        except Exception:
            pass


def generate_sql_query(user_question: str, only_sql: bool = True) -> str:
    request_profile = build_request_profile(user_question)
    rules_data = load_rules()
    full_schema = get_schema_data()
    ai_schema = get_ai_schema_data()
    schema_variants = []
    seen_variants = set()

    for max_tables in (8, 12):
        schema_variant = select_relevant_schema(ai_schema, user_question, max_tables=max_tables)
        variant_key = tuple(sorted(table.get("name", "") for table in schema_variant.get("tables", [])))
        if variant_key in seen_variants:
            continue

        seen_variants.add(variant_key)
        schema_variants.append(schema_variant)

    if not schema_variants:
        schema_variants.append(ai_schema)

    last_candidate = ""
    last_error = "Schema-grounded SQL generation failed."

    for schema_variant in schema_variants:
        schema_text = build_compact_schema_text(schema_variant, user_question)
        prompt = build_prompt(schema_text, rules_data, user_question, only_sql=only_sql, request_profile=request_profile)
        candidate_sql = _ensure_top_100(
            ask_ai(prompt, system_prompt=request_profile.get("system_prompt"), temperature=0),
            allow_procedural=request_profile.get("wants_procedure", False),
        )
        if not _looks_like_sql(candidate_sql, allow_procedural=request_profile.get("wants_procedure", False)):
            candidate_sql = enforce_sql_output(candidate_sql, schema_text, user_question, request_profile)
        last_candidate = candidate_sql or last_candidate

        issues = validate_sql_grounding(
            candidate_sql,
            full_schema,
            allow_procedural=request_profile.get("wants_procedure", False),
        )
        syntax_ok, syntax_error = validate_sql_syntax(
            candidate_sql,
            allow_procedural=request_profile.get("wants_procedure", False),
        )
        if syntax_error:
            issues.append(syntax_error)

        if not issues and syntax_ok:
            return candidate_sql

        try:
            fixed_sql = _ensure_top_100(
                fix_sql(schema_text, candidate_sql, " | ".join(issues), request_profile=request_profile),
                allow_procedural=request_profile.get("wants_procedure", False),
            )
            if not _looks_like_sql(fixed_sql, allow_procedural=request_profile.get("wants_procedure", False)):
                fixed_sql = enforce_sql_output(fixed_sql, schema_text, user_question, request_profile)
        except Exception as error:
            last_error = _format_db_error(error)
            continue

        last_candidate = fixed_sql or last_candidate
        fixed_issues = validate_sql_grounding(
            fixed_sql,
            full_schema,
            allow_procedural=request_profile.get("wants_procedure", False),
        )
        fixed_syntax_ok, fixed_syntax_error = validate_sql_syntax(
            fixed_sql,
            allow_procedural=request_profile.get("wants_procedure", False),
        )
        if fixed_syntax_error:
            fixed_issues.append(fixed_syntax_error)

        if not fixed_issues and fixed_syntax_ok:
            return fixed_sql

        if fixed_issues:
            last_error = " | ".join(fixed_issues)

    LOGGER.warning("Returning last candidate despite validation issues. Last candidate=%s Reason: %s", last_candidate, last_error)
    return last_candidate or f"-- SQL_GENERATION_FAILED: {last_error}"


def process_query(conn, schema_text: str, prompt: str) -> dict:
    generated_sql = _ensure_top_100(ask_ai(prompt, system_prompt=SQL_SYSTEM_PROMPT, temperature=0))

    if not is_safe_query(generated_sql):
        return {
            "sql": generated_sql,
            "result": [],
            "error": "Unsafe SQL query blocked.",
        }

    is_valid, validation_error = validate_sql(conn, generated_sql)
    final_sql = generated_sql

    if not is_valid:
        try:
            final_sql = _ensure_top_100(fix_sql(schema_text, generated_sql, validation_error))
        except Exception as error:
            return {
                "sql": generated_sql,
                "result": [],
                "error": f"Auto-fix failed: {_format_db_error(error)}",
            }

        if not is_safe_query(final_sql):
            return {
                "sql": final_sql,
                "result": [],
                "error": "Unsafe SQL query blocked.",
            }

        is_valid, validation_error = validate_sql(conn, final_sql)
        if not is_valid:
            return {
                "sql": final_sql,
                "result": [],
                "error": validation_error,
            }

    try:
        result = execute_sql(conn, final_sql)
    except Exception as error:
        return {
            "sql": final_sql,
            "result": [],
            "error": _format_db_error(error),
        }

    return {
        "sql": final_sql,
        "result": result,
    }


if __name__ == "__main__":
    schema_text = get_schema_text()
    rules_data = load_rules()
    question = "müşterilerin siparişlerini getir"
    prompt = build_prompt(schema_text, rules_data, question, only_sql=True)
    sql = ask_ai(prompt, system_prompt=SQL_SYSTEM_PROMPT, temperature=0)

    print("\n ÜRETİLEN SQL:\n")
    print(sql)
