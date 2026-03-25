from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    "owner": "anidata-lab",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def extraire_anime(**kwargs):
    """Tâche d'extraction du fichier anime.csv"""
    df = pd.read_csv("/opt/airflow/data/anime.csv")
    print(f"✅ {len(df)} animes chargés")
    return len(df) # Retourné via XCom

# Créer la tâche
extract_task = PythonOperator(
    task_id="extraire_anime",
    python_callable=extraire_anime,
    provide_context=True, # Donne accès à **kwargs
)

with DAG(
    dag_id="extract_anime_dag",
    default_args=default_args,
    schedule_interval=None, # Manuel
    start_date=datetime(2026, 3, 25),
    catchup=False,
) as dag:
    extract_task