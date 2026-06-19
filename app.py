from flask import Flask, jsonify, render_template, request

import main

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


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
    schema_refreshed = False

    if request.method == "POST":
        action = request.form.get("action", "generate")
        question = request.form.get("question", "").strip()
        if action == "refresh_schema":
            main.get_schema_text(force_refresh=True)
            schema_refreshed = True
        elif question:
            try:
                result = main.generate_response(question)
            except Exception as error:
                result = f"Bir hata olustu: {error}"

    return render_template(
        "index.html",
        result=result,
        schema_refreshed=schema_refreshed,
    )


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers.pop("ETag", None)
    return response


if __name__ == "__main__":
    app.run(debug=True)
