"""
CDC Hospital Admissions Data Pipeline - Cloud Composer DAG

This DAG orchestrates the CDC data processing pipeline:
1. Triggers Cloud Run Job to ingest data from CDC API
2. Submits PySpark job to Dataproc cluster for data processing
3. PySpark job processes raw JSON and loads into BigQuery

Pipeline Flow:
    CDC API → Cloud Run Job → GCS Raw → Dataproc (PySpark ETL) → BigQuery
"""

import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.bash import BashOperator

PROJECT_ID = "neeraja-data-pipeline"
REGION = "us-central1"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.datetime(2025, 11, 23),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    "cdc_refresh_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */12 * * *",
    catchup=False,
    tags=["cdc", "cloudrun", "dataproc", "bigquery"],
) as dag:

    # ----------------------------------------------------
    # TASK 1: Trigger Cloud Run Job (CDC ingestion)
    # ----------------------------------------------------
    # Executes the Cloud Run job to fetch CDC data from API and upload to GCS
    # Uses BashOperator to run gcloud command for better compatibility
    ingest_cdc_data = BashOperator(
        task_id="run_cdc_ingestion",
        bash_command=f"gcloud run jobs execute cdc-ingest-job --region={REGION} --project={PROJECT_ID} --wait",
    )

    # ----------------------------------------------------
    # TASK 2: Submit Dataproc PySpark Job
    # ----------------------------------------------------
    # Submits a PySpark job to the Dataproc cluster that:
    # - Reads raw JSON files from GCS
    # - Performs data cleaning and transformations
    # - Writes processed data to BigQuery
    # The BigQuery connector JAR is included for Spark-to-BigQuery integration
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_cdc_dataproc_job",
        project_id=PROJECT_ID,
        region=REGION,
        gcp_conn_id="google_cloud_default",
        job={
            "placement": {"cluster_name": "cdc-processing-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://cdc-health-ingestion-bucket/data-proc/main.py",
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest.jar"],
                "properties": {
                    "spark.sql.execution.arrow.pyspark.enabled": "true"
                }
            }
        }
    )

    ingest_cdc_data >> run_dataproc_job
