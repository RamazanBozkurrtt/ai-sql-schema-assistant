import pyodbc
import json
import requests


conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=Northwind;"
    "UID=sa;"
    "PWD=YourStrong!Pass123"
)

cursor = conn.cursor()



def get_tables():
    cursor.execute("SELECT name FROM sys.tables")
    return [row[0] for row in cursor.fetchall()]


def get_columns():
    cursor.execute("""
        SELECT 
            t.name AS TableName,
            c.name AS ColumnName,
            ty.name AS DataType
        FROM sys.tables t
        JOIN sys.columns c ON t.object_id = c.object_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
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
    cursor.execute("""
        SELECT 
            OBJECT_NAME(fk.parent_object_id) AS TableName,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
            OBJECT_NAME(fk.referenced_object_id) AS RefTable
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fc 
        ON fk.object_id = fc.constraint_object_id
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



def extract_schema():
    tables = get_tables()
    columns = get_columns()
    fks = get_foreign_keys()
    similar_columns = find_similar_columns(columns)

    schema = {}

    for table in tables:
        schema[table] = {
            "columns": columns.get(table, []),
            "relations": [],
            "possible_links": []
        }


    for fk in fks:
        schema[fk["table"]]["relations"].append({
            "column": fk["column"],
            "ref_table": fk["ref_table"]
        })


    for col, tables_list in similar_columns.items():
        for table in tables_list:
            others = [t for t in tables_list if t != table]

            schema[table]["possible_links"].append({
                "column": col,
                "related_tables": others
            })

    return schema



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

def load_rules():
    with open("rules.json", "r", encoding="utf-8") as f:
        return json.load(f)
    
def build_prompt(schema_text, rules_data, user_question):
    rules = "\n".join(f"- {r}" for r in rules_data["rules"])

    examples = ""
    for ex in rules_data["examples"]:
        examples += f"""
    User: {ex['question']}
    SQL:
    {ex['sql']}
    ---
    """

    prompt = f"""
    You are a senior SQL engineer.

    Rules:
    {rules}

    Examples:
    {examples}

    Schema:
    {schema_text}

    User:
    {user_question}

    Return ONLY SQL query.
    """

    return prompt

def ask_ai(prompt):
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "deepseek-coder:6.7b",
            "prompt": prompt,
            "stream": False
        }
    )

    return response.json()["response"]


schema = extract_schema()
schema_text = build_ai_input(schema)

rules_data = load_rules()

question = "müşterilerin siparişlerini getir"

prompt = build_prompt(schema_text, rules_data, question)

sql = ask_ai(prompt)

print("\n ÜRETİLEN SQL:\n")
print(sql)

