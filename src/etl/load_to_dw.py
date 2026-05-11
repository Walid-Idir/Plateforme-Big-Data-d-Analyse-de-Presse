"""
Enhanced ETL Loader — Gold JSON → PostgreSQL Data Warehouse

Loads:
  1. Individual articles into articles_gold
  2. Aggregated fact tables from gold/*.json analytical tables
  3. DQ audit log (if report provided)
"""
import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def to_ts(s):
    if not s:
        return None
    try:
        from dateutil import parser as dp
        return dp.parse(s)
    except Exception:
        return None


def get_conn(db_url: str):
    return psycopg2.connect(db_url)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_articles(conn, gold_articles_dir: Path):
    """Load individual gold articles into articles_gold table."""
    files = list(gold_articles_dir.glob("*.json"))
    if not files:
        print("[ETL] No gold article files found.")
        return 0

    rows = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
            rows.append((
                obj.get("title"),
                obj.get("author"),
                to_ts(obj.get("date") or obj.get("published_at") or obj.get("collected_at")),
                obj.get("category"),
                obj.get("content_clean") or obj.get("content"),
                obj.get("source"),
                obj.get("url"),
                obj.get("language"),
                obj.get("word_count"),
                to_ts(obj.get("collected_at")),
            ))
        except Exception as e:
            print(f"[ETL] Error reading {f.name}: {e}")

    if not rows:
        return 0

    sql = """
        INSERT INTO articles_gold
            (title, author, published_at, category, content, source, url, language, word_count, collected_at)
        VALUES %s
        ON CONFLICT (url) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=200)
    conn.commit()
    print(f"[ETL] Loaded {len(rows)} articles into articles_gold")
    return len(rows)


def load_fact_table(conn, json_path: Path, table: str, col_map: dict):
    """Generic fact table loader with upsert."""
    if not json_path.exists():
        print(f"[ETL] {json_path.name} not found, skipping {table}")
        return 0
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cols = list(col_map.keys())
    db_cols = list(col_map.values())
    unique_col = db_cols[0]

    rows = []
    for item in data:
        rows.append(tuple(item.get(c) for c in cols))

    if not rows:
        return 0

    sql = f"""
        INSERT INTO {table} ({", ".join(db_cols)})
        VALUES %s
        ON CONFLICT ({unique_col}) DO UPDATE SET
            {db_cols[1]} = EXCLUDED.{db_cols[1]},
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    print(f"[ETL] Upserted {len(rows)} rows into {table}")
    return len(rows)


def load_dq_audit(conn, report_path: Path, layer: str):
    """Insert a DQ report into the audit log."""
    if not report_path.exists():
        return
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)
    sql = """
        INSERT INTO dq_audit_log
            (layer, total_files, files_with_issues, pass_rate, dimension_failures, report_json)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            layer,
            report.get("total_articles", 0),
            report.get("articles_with_issues", 0),
            report.get("pass_rate", 0),
            json.dumps(report.get("dimension_failure_counts", {})),
            json.dumps(report),
        ))
    conn.commit()
    print(f"[ETL] DQ audit log inserted for layer={layer}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ETL: Load gold data into Data Warehouse")
    parser.add_argument("--gold", required=True, help="Path to gold directory")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--dq-report", default=None, help="Path to DQ report JSON (optional)")
    args = parser.parse_args()

    if not args.db_url:
        raise RuntimeError("DATABASE_URL not provided. Use --db-url or set env var.")

    gold_dir = Path(args.gold)
    conn = get_conn(args.db_url)

    # 1. Load individual articles
    gold_articles = gold_dir / "articles"
    if gold_articles.exists():
        load_articles(conn, gold_articles)

    # 2. Load fact tables
    load_fact_table(conn, gold_dir / "articles_per_source.json",
                    "fact_articles_per_source",
                    {"source": "source", "count": "article_count"})

    load_fact_table(conn, gold_dir / "articles_per_category.json",
                    "fact_articles_per_category",
                    {"category": "category", "count": "article_count"})

    load_fact_table(conn, gold_dir / "articles_per_language.json",
                    "fact_articles_per_language",
                    {"language": "language", "count": "article_count"})

    # 3. Top keywords
    kw_path = gold_dir / "top_keywords.json"
    if kw_path.exists():
        with open(kw_path) as f:
            kws = json.load(f)
        rows = [(item["keyword"], item["count"]) for item in kws]
        sql = """
            INSERT INTO fact_top_keywords (keyword, frequency)
            VALUES %s
            ON CONFLICT (keyword) DO UPDATE SET frequency = EXCLUDED.frequency, updated_at = NOW()
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
        print(f"[ETL] Upserted {len(rows)} keywords into fact_top_keywords")

    # 4. DQ audit
    if args.dq_report:
        load_dq_audit(conn, Path(args.dq_report), "gold")

    conn.close()
    print("[ETL] Done.")


if __name__ == "__main__":
    main()
