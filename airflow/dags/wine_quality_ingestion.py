"""
Wine Quality Data Pipeline DAG

Downloads wine quality data from Kaggle, uploads to GCS,
creates BigQuery external tables, and runs dbt transformations.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET_RAW = "wine_quality_raw"
KAGGLE_DATASET = "joebeachcapital/wine-quality"
LOCAL_PATH = "/opt/airflow/data"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def download_from_kaggle(**kwargs):
    """Download dataset from Kaggle API to local staging directory."""
    from kaggle.api.kaggle_api_extended import KaggleApi

    os.makedirs(LOCAL_PATH, exist_ok=True)

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=LOCAL_PATH, unzip=True)

    # Verify files exist
    red_path = os.path.join(LOCAL_PATH, "winequality-red.csv")
    white_path = os.path.join(LOCAL_PATH, "winequality-white.csv")

    if not os.path.exists(red_path):
        raise FileNotFoundError(f"Red wine file not found at {red_path}")
    if not os.path.exists(white_path):
        raise FileNotFoundError(f"White wine file not found at {white_path}")

    print(f"Downloaded red wine: {os.path.getsize(red_path)} bytes")
    print(f"Downloaded white wine: {os.path.getsize(white_path)} bytes")


with DAG(
    dag_id="wine_quality_ingestion",
    description="Ingest wine quality data from Kaggle to GCS and BigQuery",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["wine-quality"],
) as dag:
    download_task = PythonOperator(
        task_id="download_from_kaggle",
        python_callable=download_from_kaggle,
    )

    upload_red_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_red_wine_to_gcs",
        src=f"{LOCAL_PATH}/winequality-red.csv",
        dst="raw/red/winequality-red.csv",
        bucket=BUCKET,
    )

    upload_white_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_white_wine_to_gcs",
        src=f"{LOCAL_PATH}/winequality-white.csv",
        dst="raw/white/winequality-white.csv",
        bucket=BUCKET,
    )

    create_red_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_red_wine_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_RAW,
                "tableId": "external_red_wine",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/raw/red/winequality-red.csv"],
                "csvOptions": {
                    "skipLeadingRows": "1",
                    "fieldDelimiter": ";",
                },
            },
        },
    )

    create_white_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_white_wine_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_RAW,
                "tableId": "external_white_wine",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/raw/white/winequality-white.csv"],
                "csvOptions": {
                    "skipLeadingRows": "1",
                    "fieldDelimiter": ";",
                },
            },
        },
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt/wine_quality && dbt run --profiles-dir .",
    )

    # Task dependencies
    download_task >> [upload_red_to_gcs, upload_white_to_gcs]
    upload_red_to_gcs >> create_red_external_table
    upload_white_to_gcs >> create_white_external_table
    [create_red_external_table, create_white_external_table] >> run_dbt
