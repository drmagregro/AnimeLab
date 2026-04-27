"""
DAG d'introduction AniData Lab.
Livrable séance 4 : premier DAG Airflow simple avec PythonOperator.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_anidata(**context):
    print("🎌 Bienvenue dans AniData Lab")
    print("Pipeline cible : Extract → Transform → Load → Monitoring")
    print(f"Run id : {context['run_id']}")


def check_context(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    print("Configuration reçue :", conf)
    return {"status": "ok", "project": "AniData Lab"}


with DAG(
    dag_id="hello_anidata",
    description="Premier DAG AniData Lab avec logs et XCom simple.",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["anidata", "demo", "airflow"],
    default_args={
        "owner": "sakura_analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    start = PythonOperator(
        task_id="hello_project",
        python_callable=hello_anidata,
    )

    context_check = PythonOperator(
        task_id="check_airflow_context",
        python_callable=check_context,
    )

    start >> context_check
