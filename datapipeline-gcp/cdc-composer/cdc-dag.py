"""
CDC Hospital Admissions Pipeline

On-Demand Dataproc Cluster Version

This DAG orchestrates the CDC data processing pipeline with cost-optimized on-demand clusters:
1. Triggers Cloud Run Job to ingest data from CDC API
2. Creates Dataproc cluster on-demand
3. Submits PySpark job to process data
4. Deletes cluster after job completion (always, even on failure)

Pipeline Flow:
    CDC API → Cloud Run Job → GCS Raw → Dataproc (PySpark ETL) → BigQuery
"""

import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "neeraja-data-pipeline"
REGION = "us-central1"
CLUSTER_NAME = "cdc-processing-cluster-temp"

PYSPARK_URI = "gs://cdc-health-ingestion-bucket/data-proc/main.py"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.datetime(2025, 11, 23),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    "cdc_refresh_pipeline",
    default_args=DEFAULT_ARGS,
    description="CDC Hospital Admissions Pipeline (On-Demand Dataproc)",
    schedule_interval="0 */12 * * *",
    catchup=False,
    tags=["cdc", "dataproc", "bigquery"],
) as dag:

    # ---------------------------------------------------------
    # 1️⃣ TRIGGER CLOUD RUN INGESTION JOB
    # ---------------------------------------------------------
    trigger_ingestion = CloudRunExecuteJobOperator(
        task_id="trigger_ingestion_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="cdc-ingest-job",       # your Cloud Run job name
        gcp_conn_id="google_cloud_default",
    )

    # ---------------------------------------------------------
    # 2️⃣ CREATE DATAPROC CLUSTER
    # ---------------------------------------------------------
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id="google_cloud_default",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
            },
            "software_config": {
                "image_version": "2.1-debian11",
            },
        },
    )

    # ---------------------------------------------------------
    # 3️⃣ RUN PYSPARK JOB
    # ---------------------------------------------------------
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dataproc_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest.jar"],
                "properties": {
                    "spark.sql.execution.arrow.pyspark.enabled": "true"
                }
            },
        },
    )

    # ---------------------------------------------------------
    # 4️⃣ ALWAYS DELETE CLUSTER (EVEN IF JOB FAILS)
    # ---------------------------------------------------------
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,     # ensures cleanup ALWAYS happens
    )

    # ---------------------------------------------------------
    # DEPENDENCIES
    # ---------------------------------------------------------
    trigger_ingestion >> create_cluster >> run_dataproc_job >> delete_cluster
