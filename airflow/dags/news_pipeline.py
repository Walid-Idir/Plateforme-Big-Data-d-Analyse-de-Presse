"""
Master Airflow DAG — Batch News Pipeline
Schedule: every hour

Tasks:
  1. scrape_news         — Run scraper, write to Bronze
  2. dq_bronze           — Run DQ checks on Bronze
  3. bronze_to_silver    — Medallion transform: Bronze → Silver
  4. dq_silver           — Run DQ checks on Silver
  5. silver_to_gold      — Medallion aggregate: Silver → Gold
  6. load_dw             — ETL: Gold → PostgreSQL DW
  7. update_catalog      — Refresh data catalog
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

PYTHON = "/app/.venv/bin/python"   # override if using system python
BASE   = "/app"

with DAG(
    dag_id="news_batch_pipeline",
    default_args=default_args,
    description="Hourly batch pipeline: scrape → medallion → DW",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["news", "batch", "medallion"],
) as dag:

    # ---- 1. Scrape -------------------------------------------------------
    scrape = BashOperator(
        task_id="scrape_news",
        bash_command=(
            f"python {BASE}/src/scraper/scraper.py "
            f"--config {BASE}/src/scraper/config.yaml "
            f"--out {BASE}/data/bronze"
        ),
    )

    # ---- 2. DQ on Bronze -------------------------------------------------
    dq_bronze = BashOperator(
        task_id="dq_bronze",
        bash_command=(
            f"python {BASE}/src/quality/dq_report.py "
            f"--input {BASE}/data/bronze "
            f"--output {BASE}/data/reports/dq_bronze.json"
        ),
        trigger_rule=TriggerRule.ALL_DONE,   # run even if scrape partially failed
    )

    # ---- 3. Bronze → Silver ----------------------------------------------
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"python {BASE}/src/transform/medallion.py silver "
            f"--bronze {BASE}/data/bronze "
            f"--silver {BASE}/data/silver"
        ),
    )

    # ---- 4. DQ on Silver -------------------------------------------------
    dq_silver = BashOperator(
        task_id="dq_silver",
        bash_command=(
            f"python {BASE}/src/quality/dq_report.py "
            f"--input {BASE}/data/silver "
            f"--output {BASE}/data/reports/dq_silver.json"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ---- 5. Silver → Gold ------------------------------------------------
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"python {BASE}/src/transform/medallion.py gold "
            f"--silver {BASE}/data/silver "
            f"--gold {BASE}/data/gold"
        ),
    )

    # ---- 6. Load DW ------------------------------------------------------
    load_dw = BashOperator(
        task_id="load_dw",
        bash_command=(
            f"python {BASE}/src/etl/load_to_dw.py "
            f"--gold {BASE}/data/gold "
            f"--dq-report {BASE}/data/reports/dq_silver.json"
        ),
        env={"DATABASE_URL": "postgresql://scrap_user:scrap_pass@postgres:5432/scrap_dw"},
    )

    # ---- 7. Update catalog -----------------------------------------------
    update_catalog = BashOperator(
        task_id="update_catalog",
        bash_command=(
            f"python {BASE}/src/governance/catalog.py "
            f"--data-dir {BASE}/data "
            f"--output {BASE}/data/catalog.json "
            f"--markdown {BASE}/data/catalog.md"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ---- Dependencies ----------------------------------------------------
    scrape >> dq_bronze >> bronze_to_silver >> dq_silver >> silver_to_gold >> load_dw >> update_catalog
