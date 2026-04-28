"""
DAG de détection d'anomalies AniData Lab.
Objectifs couverts : règles métier, scheduling/backfill, atomicité, rapport exploitable.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BASE_DIR = Path(os.environ.get("ANIDATA_BASE_DIR", "/opt/airflow"))
PROJECT_DIR = Path(os.environ.get("ANIDATA_PROJECT_DIR", BASE_DIR))
DATA_DIR = Path(os.environ.get("ANIDATA_DATA_DIR", PROJECT_DIR / "data"))
OUTPUT_DIR = Path(os.environ.get("ANIDATA_OUTPUT_DIR", PROJECT_DIR / "output"))
ANOMALY_DIR = OUTPUT_DIR / "anomalies"
ANOMALY_DIR.mkdir(parents=True, exist_ok=True)


def detect_anime_gold_anomalies(**context):
    gold = OUTPUT_DIR / "anime_gold_validated.csv"
    if not gold.exists():
        raise AirflowFailException(f"Dataset gold absent : {gold}")
    df = pd.read_csv(gold)
    anomalies = []

    def add(rule, frame):
        if len(frame) > 0:
            sample = frame.head(50).to_dict(orient="records")
            anomalies.append({"rule": rule, "count": int(len(frame)), "sample": sample})

    if "score" in df.columns:
        score = pd.to_numeric(df["score"], errors="coerce")
        add("score_hors_1_10", df[(score.notna()) & ((score < 1) | (score > 10))])
        add("score_zero_nan_deguise", df[score == 0])

    if "episodes" in df.columns:
        episodes = pd.to_numeric(df["episodes"], errors="coerce")
        add("episodes_negatifs_ou_zero", df[(episodes.notna()) & (episodes <= 0)])

    if "members" in df.columns:
        members = pd.to_numeric(df["members"], errors="coerce")
        add("members_negatifs", df[(members.notna()) & (members < 0)])

    if "drop_ratio" in df.columns:
        drop_ratio = pd.to_numeric(df["drop_ratio"], errors="coerce")
        add("drop_ratio_hors_0_1", df[(drop_ratio.notna()) & ((drop_ratio < 0) | (drop_ratio > 1))])
        add("drop_ratio_tres_eleve", df[(drop_ratio.notna()) & (drop_ratio > 0.80)])

    report = {
        "run_id": context["run_id"],
        "dataset": str(gold),
        "rows": int(len(df)),
        "total_rules_with_anomalies": len(anomalies),
        "anomalies": anomalies,
    }
    tmp = ANOMALY_DIR / f"anime_gold_anomalies_{context['ts_nodash']}.tmp"
    final = ANOMALY_DIR / f"anime_gold_anomalies_{context['ts_nodash']}.json"
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(final)
    print(f"Rapport anomalies anime : {final}")
    return {"report_path": str(final), "rules_with_anomalies": len(anomalies)}


def detect_rating_spam_sample(**context):
    ratings = DATA_DIR / "rating_complete.csv"
    if not ratings.exists():
        raise AirflowFailException(f"Fichier ratings absent : {ratings}")

    df = pd.read_csv(ratings, nrows=1_000_000)
    required = {"user_id", "anime_id", "rating"}
    missing = required - set(df.columns)
    if missing:
        raise AirflowFailException(f"Colonnes ratings manquantes : {missing}")

    user_stats = df.groupby("user_id").agg(
        n_ratings=("anime_id", "count"),
        n_anime_unique=("anime_id", "nunique"),
        rating_std=("rating", "std"),
        rating_mean=("rating", "mean"),
    ).reset_index()
    suspicious = user_stats[
        (user_stats["n_ratings"] >= 300)
        & ((user_stats["rating_std"].fillna(0) == 0) | (user_stats["rating_mean"].isin([1, 10])))
    ]

    report = {
        "run_id": context["run_id"],
        "sample_rows": int(len(df)),
        "suspicious_users": int(len(suspicious)),
        "rules": [">=300 ratings dans l'échantillon", "écart-type nul ou moyenne extrême 1/10"],
        "sample": suspicious.head(100).to_dict(orient="records"),
    }
    tmp = ANOMALY_DIR / f"rating_spam_sample_{context['ts_nodash']}.tmp"
    final = ANOMALY_DIR / f"rating_spam_sample_{context['ts_nodash']}.json"
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(final)
    print(f"Rapport anomalies ratings : {final}")
    return {"report_path": str(final), "suspicious_users": int(len(suspicious))}


with DAG(
    dag_id="anidata_anomaly_detector",
    description="Détection d'anomalies sur dataset gold et ratings MyAnimeList.",
    start_date=datetime(2026, 3, 27),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["anidata", "anomaly", "quality", "production"],
    default_args={
        "owner": "sakura_analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    gold_anomalies = PythonOperator(task_id="detect_anime_gold_anomalies", python_callable=detect_anime_gold_anomalies)
    rating_spam = PythonOperator(task_id="detect_rating_spam_sample", python_callable=detect_rating_spam_sample)

    trigger_etl_refresh = TriggerDagRunOperator(
        task_id="optional_trigger_etl_refresh_after_anomaly_report",
        trigger_dag_id="anidata_etl_complete",
        wait_for_completion=False,
        reset_dag_run=False,
        conf={"source": "anomaly_detector"},
    )

    [gold_anomalies, rating_spam] >> trigger_etl_refresh
