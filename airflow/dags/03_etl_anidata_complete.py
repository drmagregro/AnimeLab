"""
DAG ETL complet AniData Lab.
Objectifs couverts : Extract → Transform → Load, indexation Elasticsearch optionnelle,
monitoring par rapports JSON, pipeline déclenchable en un clic.
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

BASE_DIR = Path(os.environ.get("ANIDATA_BASE_DIR", "/opt/airflow"))
PROJECT_DIR = Path(os.environ.get("ANIDATA_PROJECT_DIR", BASE_DIR))
DATA_DIR = Path(os.environ.get("ANIDATA_DATA_DIR", PROJECT_DIR / "data"))
OUTPUT_DIR = Path(os.environ.get("ANIDATA_OUTPUT_DIR", PROJECT_DIR / "output"))
SCRIPTS_DIR = Path(os.environ.get("ANIDATA_SCRIPTS_DIR", PROJECT_DIR / "include"))
ELASTIC_URL = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ELASTIC_INDEX = os.environ.get("ELASTICSEARCH_INDEX", "anidata_gold")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_RAW_FILES = ["anime.csv", "rating_complete.csv", "anime_with_synopsis.csv"]


def check_raw_files():
    missing = [name for name in REQUIRED_RAW_FILES if not (DATA_DIR / name).exists()]
    if missing:
        raise AirflowFailException(f"Fichiers bruts manquants dans {DATA_DIR}: {missing}")
    return {"status": "PASS", "data_dir": str(DATA_DIR)}


def audit_raw_data():
    for script in ["01_audit_complet.py", "02_audit_visuel.py"]:
        script_path = SCRIPTS_DIR / script
        result = subprocess.run(["python", str(script_path)], cwd=str(PROJECT_DIR), text=True, capture_output=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise AirflowFailException(f"Échec audit {script}")
    return {"status": "PASS"}


def run_transform_chain():
    for script in ["03_nettoyage.py", "04_feature_engineering.py", "05_validation.py"]:
        script_path = SCRIPTS_DIR / script
        result = subprocess.run(["python", str(script_path)], cwd=str(PROJECT_DIR), text=True, capture_output=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise AirflowFailException(f"Échec transformation {script}")
    return {"status": "PASS"}


def validate_gold_for_load():
    gold = OUTPUT_DIR / "anime_gold_validated.csv"
    ndjson = OUTPUT_DIR / "anime_gold.json"
    if not gold.exists() or not ndjson.exists():
        raise AirflowFailException("Exports gold absents : CSV validé ou JSON Elasticsearch manquant")
    df = pd.read_csv(gold)
    checks = {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "nan_pct": float(df.isna().sum().sum() / (df.shape[0] * df.shape[1]) * 100),
        "json_path": str(ndjson),
    }
    if checks["rows"] < 10_000:
        raise AirflowFailException(f"Dataset trop petit pour la démo : {checks['rows']} lignes")
    print(checks)
    return checks


def load_to_elasticsearch(**context):
    """Indexation bulk optionnelle. Si elasticsearch-py n'est pas installé, le DAG génère un rapport skip propre."""
    ndjson = OUTPUT_DIR / "anime_gold.json"
    report_path = OUTPUT_DIR / "load_elasticsearch_report.json"
    try:
        from elasticsearch import Elasticsearch, helpers
    except Exception as exc:
        report = {"status": "SKIPPED", "reason": f"elasticsearch-py indisponible: {exc}", "index": ELASTIC_INDEX}
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(report)
        return report

    es = Elasticsearch(ELASTIC_URL)
    if not es.ping():
        raise AirflowFailException(f"Elasticsearch inaccessible : {ELASTIC_URL}")

    def actions():
        with ndjson.open("r", encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                doc_id = doc.get("mal_id") or doc.get("anime_id") or doc.get("id")
                action = {"_index": ELASTIC_INDEX, "_source": doc}
                if doc_id is not None:
                    action["_id"] = str(doc_id)
                yield action

    success, errors = helpers.bulk(es, actions(), raise_on_error=False)
    report = {"status": "PASS", "indexed": success, "errors": len(errors), "index": ELASTIC_INDEX}
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if errors:
        raise AirflowFailException(f"Erreurs bulk Elasticsearch : {len(errors)}")
    return report


def write_monitoring_summary(**context):
    ti = context["ti"]
    summary = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "raw_check": ti.xcom_pull(task_ids="check_raw_files"),
        "gold_validation": ti.xcom_pull(task_ids="validate_gold_for_load"),
        "load": ti.xcom_pull(task_ids="load_to_elasticsearch"),
    }
    path = OUTPUT_DIR / "etl_monitoring_summary.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Résumé monitoring : {path}")
    return summary


with DAG(
    dag_id="anidata_etl_complete",
    description="Pipeline complet Extract → Transform → Load pour AniData Lab.",
    start_date=datetime(2026, 3, 26),
    schedule=None,
    catchup=False,
    tags=["anidata", "etl", "elasticsearch", "grafana"],
    default_args={
        "owner": "sakura_analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    check = PythonOperator(task_id="check_raw_files", python_callable=check_raw_files)
    audit = PythonOperator(task_id="audit_raw_data", python_callable=audit_raw_data)
    transform = PythonOperator(task_id="clean_feature_validate", python_callable=run_transform_chain)
    validate = PythonOperator(task_id="validate_gold_for_load", python_callable=validate_gold_for_load)
    load = PythonOperator(task_id="load_to_elasticsearch", python_callable=load_to_elasticsearch)
    monitoring = PythonOperator(task_id="write_monitoring_summary", python_callable=write_monitoring_summary)

    check >> audit >> transform >> validate >> load >> monitoring
