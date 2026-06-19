# AI SQL Schema Assistant

A local AI-powered assistant for complex Microsoft SQL Server schemas. It extracts schema metadata, builds local caches, and helps explain legacy ERP tables, columns, and relationships.

The project is designed to run locally. Database credentials, schema caches, company rules, and procedure dumps are ignored by Git.

## Features

* Extracts SQL Server tables, columns, and foreign-key relations
* Builds lightweight local schema/search caches
* Explains table intent, similar columns, likely joins, and legacy relationships
* Uses optional, manually maintained local rules for better schema answers
* Refuses SQL generation and answers with schema/relationship explanations instead
* Uses Ollama locally for LLM responses

## Requirements

* Python 3.10+
* Microsoft SQL Server access
* Microsoft ODBC Driver 17 or 18 for SQL Server
* Ollama installed and running

## Setup

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Install and start Ollama:

```bash
ollama pull qwen2.5-coder:14b
ollama serve
```

If Ollama is already running, `ollama serve` may say the port is already in use. That is fine.

3. Create your local environment file:

```bash
copy .env.example .env
```

Edit `.env` with your SQL Server settings:

```env
MSSQL_SERVER=localhost,1433
MSSQL_DATABASE=YourDatabaseName
MSSQL_UID=your_user
MSSQL_PWD=your_password
MSSQL_TRUST_SERVER_CERTIFICATE=yes
OLLAMA_MODEL=qwen2.5-coder:14b
```

For Windows authentication, set `MSSQL_TRUSTED_CONNECTION=yes` and remove or ignore `MSSQL_UID` / `MSSQL_PWD`.

4. Optionally create local domain rules:

```bash
copy rules.example.json rules.json
```

This file is optional and starts empty:

```json
{
  "rules": []
}
```

The app does not auto-generate or auto-save these rules. Add only reviewed, generalized schema or relationship guidance when the database schema alone is not enough.

## Run

Start the web app:

```bash
python app.py
```

Open:

```text
http://127.0.0.1:5000
```

On first schema access, the app connects to SQL Server and creates local cache files:

* `schema_cache.json`
* `schema_cache.ai.json`
* `schema_cache.meta.json`
* `schema_search_index.json`

Use the refresh schema action in the UI after database schema changes.

## Usage

Ask schema questions in Turkish or English, for example:

```text
Sip_Siparis ile Sip_SiparisDetay arasındaki fark nedir?
```

The assistant explains schema intent, table relationships, candidate columns, and legacy naming tradeoffs. It does not generate SQL queries or stored procedures.

The `/schema`, `/search-column`, and `/search-table` endpoints depend on the local schema cache. If no cache exists yet, they need a working SQL Server connection to build it.

`rules.json` is only a manual hint layer. The app works without it, and an empty `rules.json` is valid.

## Private Data Safety

The following local files are ignored by Git and should not be committed:

* `.env` and other local environment files
* generated schema caches such as `schema_cache*.json` and `schema_search_index.json`
* local prompt assets: `rules.json`
* procedure dumps under `sql_procedures/`
* Python runtime files under `__pycache__/`

Keep only the `*.example.json` files generic enough to share publicly.

## Troubleshooting

If you see `MSSQL_SERVER and MSSQL_DATABASE must be configured`, create `.env` or set the matching environment variables.

If `pyodbc` cannot connect, confirm that the SQL Server ODBC driver is installed and that the driver name in `.env` matches your machine, for example `ODBC Driver 17 for SQL Server` or `ODBC Driver 18 for SQL Server`.

If the app cannot reach Ollama, make sure Ollama is running and that the model in `OLLAMA_MODEL` has been pulled.

If answers are too generic, add reviewed schema/domain rules to `rules.json`, or add local `.sql` procedure files under `sql_procedures/` as relationship evidence. Do not commit those private files.
