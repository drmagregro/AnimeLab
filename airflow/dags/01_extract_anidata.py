"""
DAG Extract AniData Lab.
Objectifs couverts :
- lecture des 3 CSV bruts,
- validation de schéma,
- XComs avec métriques de fichiers,
- retries et erreurs explicites.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

BASE_DIR = Path(os.environ.get("ANIDATA_BASE_DIR", "/opt/airflow"))
DATA_DIR = Path(os.environ.get("ANIDATA_DATA_DIR", BASE_DIR / "data"))
OUTPUT_DIR = Path(os.environ.get("ANIDATA_OUTPUT_DIR", BASE_DIR / "output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "anime": {
        "filename": "anime.csv",
        "required_columns": ["MAL_ID", "Name", "Score", "Genres", "Type", "Episodes", "Members"],
        "sample_rows": None,
    },
    "ratings": {
        "filename": "rating_complete.csv",
        "required_columns": ["user_id", "anime_id", "rating"],
        "sample_rows": 500_000,
    },
    "synopsis": {
        "filename": "anime_with_synopsis.csv",
        "required_columns": ["MAL_ID"],
        "sample_rows": None,
    },
}


def extract_csv(dataset_name: str, **context):
    config = FILES[dataset_name]
    path = DATA_DIR / config["filename"]
    if not path.exists():
        raise AirflowFailException(f"Fichier manquant : {path}")

    nrows = config.get("sample_rows")
    df = pd.read_csv(path, nrows=nrows)
    metrics = {
        "dataset": dataset_name,
        "path": str(path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "n_columns": int(df.shape[1]),
        "missing_total": int(df.isna().sum().sum()),
    }
    print(metrics)
    return metrics


def validate_schema(**context):
    ti = context["ti"]
    errors = []
    report = []

    for dataset_name, config in FILES.items():
        metrics = ti.xcom_pull(task_ids=f"extract_{dataset_name}")
        if not metrics:
            errors.append(f"Aucune métrique XCom pour {dataset_name}")
            continue
        columns = set(metrics["columns"])
        required = set(config["required_columns"])
        missing = sorted(required - columns)
        report.append({
            "dataset": dataset_name,
            "rows": metrics["rows"],
            "missing_required_columns": missing,
        })
        if missing:
            errors.append(f"{dataset_name}: colonnes manquantes {missing}")
        if metrics["rows"] == 0:
            errors.append(f"{dataset_name}: fichier vide")

    report_path = OUTPUT_DIR / "extract_schema_report.json"
    pd.DataFrame(report).to_json(report_path, orient="records", force_ascii=False, indent=2)
    print(f"Rapport écrit : {report_path}")

    if errors:
        raise AirflowFailException("Validation extract échouée : " + " | ".join(errors))
    return {"status": "PASS", "report_path": str(report_path)}


with DAG(
    dag_id="extract_anidata",
    description="Extraction multi-fichiers MyAnimeList avec validation de schéma.",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["anidata", "extract", "validation"],
    default_args={
        "owner": "sakura_analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:
    extract_anime = PythonOperator(task_id="extract_anime", python_callable=extract_csv, op_kwargs={"dataset_name": "anime"})
    extract_ratings = PythonOperator(task_id="extract_ratings", python_callable=extract_csv, op_kwargs={"dataset_name": "ratings"})
    extract_synopsis = PythonOperator(task_id="extract_synopsis", python_callable=extract_csv, op_kwargs={"dataset_name": "synopsis"})

    validate = PythonOperator(task_id="validate_raw_schema", python_callable=validate_schema)

    [extract_anime, extract_ratings, extract_synopsis] >> validate
