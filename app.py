from flask import Flask, jsonify, render_template, request

import main

app = Flask(__name__)


@app.get("/schema")
def schema():
    refresh_requested = request.args.get("refresh", "").lower() in {"1", "true", "yes"}
    return jsonify(main.build_schema_browser_data(force_refresh=refresh_requested))


@app.post("/search-column")
def search_column():
    payload = request.get_json(silent=True) or {}
    column_name = str(payload.get("column", "")).strip()
    matches = main.search_columns(column_name)
    return jsonify({"matches": matches})


@app.post("/search-table")
def search_table():
    payload = request.get_json(silent=True) or {}
    table_name = str(payload.get("table", "")).strip()
    matches = main.search_tables(table_name)
    return jsonify({"matches": matches})


@app.route("/", methods=["GET", "POST"])
def index():
    result = ""
    learning_saved = False
    schema_refreshed = False

    if request.method == "POST":
        action = request.form.get("action", "generate")
        question = request.form.get("question", "").strip()
        only_sql = "only_sql" in request.form
        correct_sql = request.form.get("correct_sql", "").strip()
        reason = request.form.get("reason", "").strip()
        wrong_sql = request.form.get("wrong_sql", "").strip()

        if action == "refresh_schema":
            main.get_schema_text(force_refresh=True)
            schema_refreshed = True
        elif correct_sql and wrong_sql and question:
            main.save_learning(question, wrong_sql, correct_sql, reason)
            result = wrong_sql
            learning_saved = True
        elif question:
            result = main.generate_sql_query(question, only_sql=only_sql)

    return render_template(
        "index.html",
        result=result,
        learning_saved=learning_saved,
        schema_refreshed=schema_refreshed,
    )


if __name__ == "__main__":
    app.run(debug=True)
