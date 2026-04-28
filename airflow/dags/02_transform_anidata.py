"""
DAG Transform AniData Lab.
Objectifs couverts :
- exécution du nettoyage,
- BranchPythonOperator pour décider si le feature engineering peut continuer,
- versioning gold_vYYYYMMDD_HHMMSS,
- validation finale CSV + JSON.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

BASE_DIR = Path(os.environ.get("ANIDATA_BASE_DIR", "/opt/airflow"))
PROJECT_DIR = Path(os.environ.get("ANIDATA_PROJECT_DIR", BASE_DIR))
OUTPUT_DIR = Path(os.environ.get("ANIDATA_OUTPUT_DIR", PROJECT_DIR / "output"))
SCRIPTS_DIR = Path(os.environ.get("ANIDATA_SCRIPTS_DIR", PROJECT_DIR / "include"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_script(script_name: str):
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        raise AirflowFailException(f"Script introuvable : {script_path}")
    result = subprocess.run(
        ["python", str(script_path)],
        cwd=str(PROJECT_DIR),
        text=True,
        capture_output=True,
        check=False,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise AirflowFailException(f"Échec script {script_name}, code={result.returncode}")
    return {"script": script_name, "status": "success"}


def quality_gate_cleaned(**context):
    cleaned = OUTPUT_DIR / "anime_cleaned.csv"
    if not cleaned.exists():
        raise AirflowFailException(f"Dataset nettoyé absent : {cleaned}")
    df = pd.read_csv(cleaned)
    nan_pct = float(df.isna().sum().sum() / (df.shape[0] * df.shape[1]) * 100)
    print(f"Lignes={len(df)}, colonnes={df.shape[1]}, NaN={nan_pct:.2f}%")
    if len(df) >= 10_000 and nan_pct < 40:
        return "run_feature_engineering"
    return "stop_transform_quality_failed"


def version_gold_dataset(**context):
    validated = OUTPUT_DIR / "anime_gold_validated.csv"
    json_file = OUTPUT_DIR / "anime_gold.json"
    if not validated.exists():
        raise AirflowFailException(f"Fichier validé absent : {validated}")

    version = context["ts_nodash"]
    version_dir = OUTPUT_DIR / "versions" / f"gold_{version}"
    version_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(validated, version_dir / "anime_gold_validated.csv")
    if json_file.exists():
        shutil.copy2(json_file, version_dir / "anime_gold.json")

    latest_file = OUTPUT_DIR / "versions" / "LATEST.txt"
    latest_file.write_text(str(version_dir), encoding="utf-8")
    return {"version_dir": str(version_dir)}


with DAG(
    dag_id="transform_anidata",
    description="Nettoyage, feature engineering, validation et versioning du dataset gold.",
    start_date=datetime(2026, 3, 26),
    schedule=None,
    catchup=False,
    tags=["anidata", "transform", "branch", "versioning"],
    default_args={
        "owner": "sakura_analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    run_cleaning = PythonOperator(
        task_id="run_cleaning",
        python_callable=run_script,
        op_kwargs={"script_name": "03_nettoyage.py"},
    )

    decide = BranchPythonOperator(
        task_id="quality_gate_cleaned_dataset",
        python_callable=quality_gate_cleaned,
    )

    run_features = PythonOperator(
        task_id="run_feature_engineering",
        python_callable=run_script,
        op_kwargs={"script_name": "04_feature_engineering.py"},
    )

    stop_failed = EmptyOperator(task_id="stop_transform_quality_failed")

    validate_export = PythonOperator(
        task_id="validate_and_export_gold",
        python_callable=run_script,
        op_kwargs={"script_name": "05_validation.py"},
    )

    version_gold = PythonOperator(task_id="version_gold_dataset", python_callable=version_gold_dataset)

    run_cleaning >> decide >> [run_features, stop_failed]
    run_features >> validate_export >> version_gold
