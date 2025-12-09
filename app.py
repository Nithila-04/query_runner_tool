from flask import Flask, render_template, request, send_file, redirect, url_for
import psycopg2
import re
import sqlite3
import csv
import io
from datetime import datetime
import os

app = Flask(__name__)

PG_CONFIG = {
    "host": "ps-prod-v2.ctiasiwwcicd.ap-south-1.rds.amazonaws.com",
    "user": "support_profitstory_ai",
    "password": "Welc0me@P$",
    "database": "postgres",
    "port": 5432
}

ORG_CLIENT_MAP = {
    "AGAV": "Agavai16","ARIR": "Ariro","ASHT": "Ashtangam","BABY": "Baby Eli",
    "CHEFS": "ChefSpray","FARMT": "Farm Theory","FLO": "FiloMilo","FUTUR": "Futurewagon",
    "Grac": "Graciss","HABERL": "Haber Living","HERBO": "Herbomil","LAKSH": "LakshmiKrishna",
    "MELBI": "melbify","NAILS": "nailsnmore","NEUBA": "NeuBaby","NSURE": "Nsure",
    "PALM": "Palm Era","PRINK": "Prink","RBW": "Threadmill","RELI": "RELISH",
    "ROBAG": "ROBAGO","SOLA": "Solaris","THEJ": "THE JOURNAL LAB","TOMTO": "TomTommy",
    "TRYB": "TRYB","TRYS": "Tryst","VILV": "Vilvah","WELL": "WELLBI","WEST": "WestBrooke"
}

SAVED_DB = "saved_queries.db"

def init_saved_db():
    if not os.path.exists(SAVED_DB):
        conn = sqlite3.connect(SAVED_DB)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE saved_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                query_text TEXT NOT NULL,
                orgs TEXT,
                created_at TEXT
            );
        """)
        conn.commit()
        cur.close()
        conn.close()

def save_query_to_db(name, query_text, orgs_list):
    conn = sqlite3.connect(SAVED_DB)
    cur = conn.cursor()
    orgs_csv = ",".join(orgs_list) if orgs_list else ""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT OR REPLACE INTO saved_queries (name, query_text, orgs, created_at)
        VALUES (?, ?, ?, ?)
    """, (name, query_text, orgs_csv, now))
    conn.commit()
    cur.close()
    conn.close()

def load_all_saved():
    conn = sqlite3.connect(SAVED_DB)
    cur = conn.cursor()
    cur.execute("SELECT id, name, query_text, orgs, created_at FROM saved_queries ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    saved = []
    for r in rows:
        saved.append({
            "id": r[0],
            "name": r[1],
            "query": r[2],
            "orgs": r[3].split(",") if r[3] else [],
            "created_at": r[4]
        })
    return saved

def load_saved_by_name(name):
    conn = sqlite3.connect(SAVED_DB)
    cur = conn.cursor()
    cur.execute("SELECT id, name, query_text, orgs, created_at FROM saved_queries WHERE name = ?", (name,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    if r:
        return {"id": r[0], "name": r[1], "query": r[2], "orgs": r[3].split(",") if r[3] else [], "created_at": r[4]}
    return None

def format_result(rows, cursor):
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    return {"columns": columns, "rows": rows}

def run_postgres_query(query):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        if query.strip().lower().startswith("select"):
            rows = cur.fetchall()
            return format_result(rows, cur)
        else:
            conn.commit()
            return "Query executed successfully."
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if cur: cur.close()
        if conn: conn.close()

def build_query_for_org(original_query, org):
    safe_org = org.replace("'", "''")
    if "{org}" in original_query:
        return original_query.replace("{org}", safe_org)
    pattern = re.compile(r"(org_code\s*=\s*)'([^']*)'", flags=re.IGNORECASE)
    if pattern.search(original_query):
        return pattern.sub(lambda m: f"{m.group(1)}'{safe_org}'", original_query)
    if re.search(r"\bwhere\b", original_query, flags=re.IGNORECASE):
        return original_query + f" AND org_code ILIKE '{safe_org}'"
    else:
        if original_query.rstrip().endswith(";"):
            return original_query.rstrip()[:-1] + f" WHERE org_code ILIKE '{safe_org}';"
        else:
            return original_query + f" WHERE org_code ILIKE '{safe_org}'"

@app.route("/", methods=["GET", "POST"])
def index():
    init_saved_db()
    saved = load_all_saved()
    results = {}
    error = None
    query_text = ""
    selected_orgs = []
    run_all_flag = False

    if request.method == "POST":
        query_text = request.form.get("query", "").strip()
        db_choice = request.form.get("db", "postgres").strip()
        selected_orgs = request.form.getlist("org_codes")
        save_name = request.form.get("save_name", "").strip()
        load_name = request.form.get("load_saved", "").strip()
        run_all_flag = True if request.form.get("run_all") == "on" else False

        if load_name:
            saved_item = load_saved_by_name(load_name)
            if saved_item:
                query_text = saved_item["query"]
                selected_orgs = saved_item["orgs"]

        if not query_text:
            error = "Please enter a SQL query."
            return render_template("index.html", org_map=ORG_CLIENT_MAP, saved=saved, results=results, query=query_text, selected_orgs=selected_orgs, error=error)

        if save_name:
            try:
                save_query_to_db(save_name, query_text, selected_orgs)
                saved = load_all_saved()
            except Exception as e:
                error = f"Failed to save query: {e}"
                return render_template("index.html", org_map=ORG_CLIENT_MAP, saved=saved, results=results, query=query_text, selected_orgs=selected_orgs, error=error)

        try:
            if run_all_flag or not selected_orgs:
                out = run_postgres_query(query_text)
                results["ALL"] = {"client_name": "All / No org filter", "query_used": query_text, "output": out}
            else:
                for org in selected_orgs:
                    client_name = ORG_CLIENT_MAP.get(org, "Unknown")
                    q = build_query_for_org(query_text, org)
                    out = run_postgres_query(q)
                    results[org] = {"client_name": client_name, "query_used": q, "output": out}
        except Exception as e:
            error = f"Error running query: {e}"
            return render_template("index.html", org_map=ORG_CLIENT_MAP, saved=saved, results=results, query=query_text, selected_orgs=selected_orgs, error=error)

    return render_template("index.html", org_map=ORG_CLIENT_MAP, saved=saved, results=results, query=query_text, selected_orgs=selected_orgs, error=error)

@app.route("/download_csv", methods=["POST"])
def download_csv():
    query = request.form.get("query", "").strip()
    if not query:
        return "No query provided", 400
    output = run_postgres_query(query)
    if isinstance(output, str):
        return f"Cannot download: {output}", 400
    if not output.get("rows"):
        return "No data to download", 400

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(output["columns"])
    cw.writerows(output["rows"])
    mem = io.BytesIO()
    mem.write(si.getvalue().encode("utf-8"))
    mem.seek(0)
    filename = f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=filename)

if __name__ == "__main__":
    init_saved_db()
    app.run(debug=True)