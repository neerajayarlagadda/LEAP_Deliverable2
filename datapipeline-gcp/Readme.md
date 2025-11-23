# CDC Hospital Admissions — Data Pipeline (GCP)

**Project**: CDC Hospital Admissions Pipeline
**Owner**: Neeraja Y. (project: `neeraja-data-pipeline`)
**Last updated**: 2025-11-23

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [GCP Resources & Naming](#gcp-resources--naming)
4. [Data Flow (End-to-end)](#data-flow-end-to-end)
5. [Files in Repo (what to include)](#files-in-repo-what-to-include)
6. [Detailed Step-by-step Setup & Commands](#detailed-step-by-step-setup--commands)

   * A. Create / verify GCS buckets
   * B. BigQuery: dataset & table DDL
   * C. Dataproc PySpark job (`main.py`) — upload & run
   * D. Cloud Composer DAG — upload & enable
   * E. Streamlit dashboard — local test & Cloud Run deploy
7. [Testing & Validation](#testing--validation)
8. [Common Errors & Troubleshooting](#common-errors--troubleshooting)
9. [Security, IAM & Permissions](#security-iam--permissions)
10. [Cost Considerations](#cost-considerations)
11. [Next Improvements & Ideas](#next-improvements--ideas)
12. [Appendix — Full Code Snippets](#appendix---full-code-snippets)

---

## Project Overview

This repository documents an ETL pipeline that ingests CDC hospital admissions data (CDC API) and processes it in GCP to produce a cleaned BigQuery table used by a Streamlit dashboard.

High-level goals:

* Periodically ingest CDC JSON data into GCS
* Process / clean JSON via Dataproc (PySpark)
* Store cleaned data in BigQuery table `cdc_hospital_admissions`
* Display results interactively with Streamlit (deployed to Cloud Run)
* Orchestrate the pipeline with Cloud Composer (Airflow)

This README explains all components, commands, and troubleshooting steps you need to reproduce and present the project.

---

## Architecture

Textual architecture diagram:

```
[CDC API] 
    |
    v
[Cloud Composer DAG] (cdc-composer/cdc-dag.py)
    | Scheduled (every 12 hours) or manual trigger
    |
    ├─> TASK 1: BashOperator triggers [Cloud Run Job] (cdc-ingest-job)
    |   | Executes cdc-ingestion/main.py container
    |   | Fetches CDC API data and uploads to GCS
    |   v
    |   [GCS Raw Bucket] gs://cdc-health-ingestion-bucket/raw/*.json
    |   |
    |   v
    └─> TASK 2: DataprocSubmitJobOperator submits [Dataproc Job]
        | PySpark Job (cdc-dataproc/main.py)
        | Reads raw JSON from GCS
        | Performs data cleaning and transformations
        v
        ├─> [GCS Processed] gs://.../processed/cdc_clean/ (Parquet)
        |
        v
        [BigQuery Table] neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions
            |
            v
        [Streamlit Dashboard] (cdc-streamlit/main.py on Cloud Run)
            | Queries BigQuery on page load
            v
        [Interactive UI] Charts, filters, visualizations
```

**Component Notes**:

* **Cloud Composer DAG**: Orchestrates the entire pipeline with two sequential tasks
* **Cloud Run Job** (Task 1): Executes ingestion container to fetch CDC API data and upload to GCS raw bucket
* **Dataproc** (Task 2): Processes heavy-lifting transformations (data cleaning, type casting, date conversion)
* **BigQuery**: Single source of truth for analytics-ready data
* **Streamlit**: Queries BigQuery on every page load (dashboard shows latest data automatically)
* **Composer**: Manages scheduling, retries, and workflow orchestration

---

## GCP Resources & Naming

Use these exact names (used in this project):

* GCP Project: `neeraja-data-pipeline`
* GCS ingestion bucket: `gs://cdc-health-ingestion-bucket`

  * folders: `raw/`, `processed/`, `data-proc/` (script), `data-proc/errors/`
* Dataproc cluster: `cdc-processing-cluster` (region `us-central1`)
* Dataproc staging bucket: `gs://cdc-dataproc-staging` (create if not existing)
* BigQuery dataset: `cdc_health_data`
* BigQuery table: `cdc_hospital_admissions`

  * Fully-qualified: `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`
* Composer environment: `cdc-health-composer` (region `us-central1`)
* Streamlit image: `gcr.io/<YOUR_PROJECT>/cdc-streamlit` (replace `<YOUR_PROJECT>`)
* Streamlit Cloud Run service name: `cdc-streamlit`

> Replace `YOUR_PROJECT` in commands where required. The code samples in the Appendix use these names exactly — change only if you intentionally want different names.

---

## Data Flow (End-to-end)

1. **Orchestration**: Cloud Composer DAG (`cdc-composer/cdc-dag.py`) runs on a schedule (every 12 hours by default) or can be triggered manually. The DAG contains two sequential tasks:
   - **Task 1 (Ingestion)**: BashOperator executes Cloud Run Job `cdc-ingest-job` which runs the ingestion container (`cdc-ingestion/main.py`). This fetches data from CDC API `https://data.cdc.gov/resource/akn2-qxic.json` and writes the JSON response to `gs://cdc-health-ingestion-bucket/raw/cdc_data_<timestamp>.json`. The CDC API returns JSON arrays, e.g., `[{...}, {...}]`.
   - **Task 2 (Processing)**: DataprocSubmitJobOperator submits a PySpark job to Dataproc cluster after ingestion completes.
2. **Transform**: Dataproc PySpark job (`cdc-dataproc/main.py`) reads JSON using `multiLine=True`, performs data cleaning (type casting, trimming, date conversion), writes cleaned Parquet to `gs://cdc-health-ingestion-bucket/processed/cdc_clean/` and loads directly into BigQuery.
3. **Serve**: Streamlit dashboard (`cdc-streamlit/main.py`) reads BigQuery table on page load and visualizes results. Deploy Streamlit to Cloud Run for public access.
4. **Monitoring**: Composer & Dataproc logs are visible through Cloud Logging and the Dataproc job console.

---

## Files in Repo (what to include)

Actual project structure:

```
/
├─ Readme.md                    <-- this document
├─ cdc-dataproc/
│   └─ main.py                  <-- PySpark ETL script (upload to GCS)
├─ cdc-composer/
│   └─ cdc-dag.py               <-- Airflow DAG (upload to Composer dags/)
├─ cdc-streamlit/
│   ├─ main.py                  <-- Streamlit dashboard application
│   ├─ requirements.txt         <-- Python dependencies
│   └─ Dockerfile               <-- Container image for Cloud Run
└─ cdc-ingestion/
    ├─ main.py                  <-- Cloud Run Job container for CDC API ingestion
    ├─ Dockerfile               <-- Container image definition
    └─ requirements.txt         <-- Python dependencies
```


---

## Detailed Step-by-step Setup & Commands

### A. Deploy Data Ingestion Component

You have two options for deploying the ingestion component:

#### Option 1: Cloud Run Job (Recommended - matches current code structure)

**Build and push the Docker image** (PowerShell - use backtick `` ` `` for line continuation):

```powershell
cd cdc-ingestion
gcloud builds submit --tag gcr.io/neeraja-data-pipeline/cdc-ingest-job
```

**Create Cloud Run Job** (PowerShell):

```powershell
gcloud run jobs create cdc-ingest-job `
  --image gcr.io/neeraja-data-pipeline/cdc-ingest-job `
  --region us-central1 `
  --max-retries 3 `
  --task-timeout 300
```

**Execute the job manually**:

```powershell
gcloud run jobs execute cdc-ingest-job --region us-central1
```

**Schedule with Cloud Scheduler** (PowerShell):

```powershell
# First, get your project number
$PROJECT_NUMBER = (gcloud projects describe neeraja-data-pipeline --format="value(projectNumber)")

# Create scheduler job
gcloud scheduler jobs create http cdc-ingestion-schedule `
  --schedule="0 */6 * * *" `
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_NUMBER/jobs/cdc-ingest-job:run" `
  --http-method=POST `
  --oauth-service-account-email=$PROJECT_NUMBER-compute@developer.gserviceaccount.com `
  --location=us-central1
```

#### Option 2: Cloud Function (Alternative - requires code modification)

**Note**: Current `main.py` uses `run()` function. For Cloud Function, change to `ingest_cdc_data(request)`.

**Deploy as Cloud Function** (PowerShell):

```powershell
gcloud functions deploy ingest_cdc_data `
  --runtime python311 `
  --trigger-http `
  --allow-unauthenticated `
  --source cdc-ingestion `
  --entry-point ingest_cdc_data `
  --region us-central1 `
  --timeout 60s `
  --memory 256MB
```

**Test the function**:

```powershell
Invoke-WebRequest -Uri "https://us-central1-neeraja-data-pipeline.cloudfunctions.net/ingest_cdc_data"
```

**Verify ingestion** (both options):

```powershell
gsutil ls gs://cdc-health-ingestion-bucket/raw/
```

**Verify ingestion**:

```bash
gsutil ls gs://cdc-health-ingestion-bucket/raw/
```

---

### B. Create / verify GCS buckets

1. **Create main bucket** (if not created already)

```bash
gsutil mb -l US gs://cdc-health-ingestion-bucket
```

2. **Create folders** (just for organization; optional)

```bash
gsutil ls gs://cdc-health-ingestion-bucket || true
gsutil cp -r local_placeholder gs://cdc-health-ingestion-bucket/raw/
# or create directories by creating placeholder objects:
echo "{}" | gsutil cp - gs://cdc-health-ingestion-bucket/raw/.placeholder
echo "{}" | gsutil cp - gs://cdc-health-ingestion-bucket/processed/.placeholder
gsutil mb -l US gs://cdc-dataproc-staging
```

**Important**: Ensure the Dataproc cluster service account and Composer service account have read/write access to these buckets (see IAM section).

---

### C. BigQuery: dataset & table DDL

Run this in BigQuery console (SQL workspace) or via `bq`:

```sql
-- create dataset if not exists
CREATE SCHEMA IF NOT EXISTS `neeraja-data-pipeline.cdc_health_data`;

-- drop + create final table
DROP TABLE IF EXISTS `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`;

CREATE TABLE `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions` (
  state STRING,
  county STRING,
  fips_code STRING,
  county_population INT64,
  health_sa_name STRING,
  health_sa_number INT64,
  health_sa_population INT64,
  report_date TIMESTAMP,
  week_end_date TIMESTAMP,
  mmwr_report_week INT64,
  mmwr_report_year INT64,
  total_adm_all_covid_confirmed INT64,
  total_adm_all_covid_confirmed_per_100k FLOAT64,
  total_adm_all_covid_confirmed_level STRING,
  admissions_covid_confirmed FLOAT64,
  admissions_covid_confirmed_level STRING,
  avg_percent_inpatient_beds FLOAT64,
  avg_percent_inpatient_beds_level STRING,
  abs_chg_avg_percent_inpatient FLOAT64,
  avg_percent_staff_icu_beds FLOAT64,
  avg_percent_staff_icu_beds_level STRING,
  abs_chg_avg_percent_staff FLOAT64
);
```

---

### D. Dataproc PySpark job (main.py)

**Upload the finalized PySpark script** to GCS path:
`gs://cdc-health-ingestion-bucket/data-proc/main.py`

**Upload command**:
```bash
gsutil cp cdc-dataproc/main.py gs://cdc-health-ingestion-bucket/data-proc/main.py
```

**Key points used in the script**:

* Uses `spark.read.json(..., multiLine=True)` because CDC JSON is an array format.
* Trims whitespace from string fields to ensure data quality.
* Casts numeric fields to float for consistency (handles nulls gracefully).
* Converts date strings to timestamps using `to_timestamp()` for BigQuery compatibility.
* Writes cleaned Parquet to GCS (`processed/cdc_clean/`) for backup/archival.
* Loads directly into BigQuery using `df.write.format("bigquery")` with `temporaryGcsBucket` option.
* Uses indirect write method for better reliability (writes to GCS staging first, then loads to BigQuery).

**Quick run (manual)**:

```bash
gcloud dataproc jobs submit pyspark gs://cdc-health-ingestion-bucket/data-proc/main.py \
  --cluster=cdc-processing-cluster \
  --region=us-central1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar
```

If you prefer to avoid the connector, the script can write Parquet and then you load Parquet to BigQuery using CLI or BigQuery job.

---

### E. Cloud Composer DAG

**Location**: Upload `cdc-composer/cdc-dag.py` into Composer environment `dags/` folder.

**Upload command**:
```bash
gsutil cp cdc-composer/cdc-dag.py gs://<COMPOSER_BUCKET>/dags/cdc-dag.py
```

**Key DAG elements**:

* DAG ID: `cdc_refresh_pipeline`
* **Task 1**: `BashOperator` (task_id: `run_cdc_ingestion`) - Executes Cloud Run Job `cdc-ingest-job` to fetch CDC data and upload to GCS
* **Task 2**: `DataprocSubmitJobOperator` (task_id: `run_cdc_dataproc_job`) - Submits PySpark job to Dataproc cluster `cdc-processing-cluster` (region `us-central1`) with the main python file `gs://cdc-health-ingestion-bucket/data-proc/main.py`
* Task dependencies: Task 1 must complete before Task 2 runs (`ingest_cdc_data >> run_dataproc_job`)
* Schedule: `0 */12 * * *` (every 12 hours)
* DAG has `catchup=False`, `retries=1`
* Includes BigQuery connector JAR: `gs://spark-lib/bigquery/spark-bigquery-latest.jar`

**Trigger manually**:

* Composer UI → DAGs → `cdc_refresh_pipeline` → Trigger DAG

**Airflow quick check**:

* If job fails, check Airflow task logs and Dataproc job page for driver output links.

---

### F. Streamlit Dashboard — Local test & Cloud Run deploy

**Local test**:

1. `cd cdc-streamlit/`
2. `pip install -r requirements.txt`
3. `streamlit run main.py`
4. Verify charts at `http://localhost:8501`
5. **Important**: Update the BigQuery query in `main.py` with your actual project and dataset:
   ```python
   FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`
   ```

**requirements.txt** (example)

```
streamlit
pandas
google-cloud-bigquery[pandas]
db-dtypes
plotly
```

**Dockerfile** (example):

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8080
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]
```

**Build & Push**:

```bash
gcloud builds submit --tag gcr.io/neeraja-data-pipeline/cdc-streamlit ./cdc-streamlit
```

**Deploy to Cloud Run**:

```bash
gcloud run deploy cdc-streamlit \
  --image gcr.io/neeraja-data-pipeline/cdc-streamlit \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

> The Streamlit app queries BigQuery on page load so it will show the latest data as soon as Dataproc writes to BigQuery. Optionally add `st_autorefresh` or a refresh button.

---

## Testing & Validation

1. **Validate raw ingestion**

   * Check files in GCS:
     `gsutil ls gs://cdc-health-ingestion-bucket/raw/`
   * Confirm first few lines:
     `gsutil cat gs://.../raw/cdc_raw_<date>.json | head -n 50`

2. **Run Dataproc job manually**

   * Submit pyspark job and watch logs
   * Confirm Parquet in `gs://cdc-health-ingestion-bucket/processed/cdc_clean/`

3. **Validate BigQuery data**

   ```sql
   SELECT state, county, report_date, total_adm_all_covid_confirmed
   FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`
   ORDER BY report_date DESC
   LIMIT 50;
   ```

4. **Validate Streamlit**

   * Verify charts reflect the same top rows

---

## Common Errors & Troubleshooting

### 1. `_corrupt_record` only appears in schema

**Cause**: Spark couldn't parse the JSON file (likely a JSON array vs NDJSON).
**Fix**: Use `spark.read.json(path, multiLine=True)` for JSON arrays or ensure files are newline-delimited JSON.

### 2. `PATH_ALREADY_EXISTS` on write to GCS

**Cause**: Output path already exists and mode defaults to `error`.
**Fix**: Use `.mode("overwrite")` or write to a timestamped folder.

### 3. Dataproc Job "File not found: gs://.../main.py"

**Cause**: The file path provided to the Dataproc job does not exist in GCS.
**Fix**: Upload `main.py` to that exact path.

### 4. Composer failing with `NotFound` cluster

**Cause**: DAG references wrong cluster name or cluster was deleted.
**Fix**: Update `cluster_name` in DAG to the actual cluster or recreate cluster.

### 5. BigQuery connector errors / `ClassNotFoundException`

**Cause**: Spark BigQuery connector not available to the job.
**Fix**: Submit the job with `--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar` or include connector during cluster creation.

### 6. `Please install 'db-dtypes'` when Streamlit loads BigQuery result

**Cause**: `google-cloud-bigquery[pandas]` needs `db-dtypes` to convert to pandas.
**Fix**: Add `db-dtypes` to `requirements.txt` and redeploy.

### 7. Permission errors (403)

**Cause**: Dataproc or Composer service account lacks Storage/BigQuery permissions.
**Fix**: Grant appropriate IAM roles (see IAM section).

---

## Security, IAM & Permissions (minimum)

Ensure the following service accounts/roles exist:

* **Dataproc cluster service account** (usually compute default or custom):

  * `roles/storage.objectViewer` (read raw from GCS)
  * `roles/storage.objectAdmin` (write processed files)
  * `roles/bigquery.dataEditor` (write to BigQuery)
* **Composer service account** (Airflow workers):

  * `roles/dataproc.jobRunner` (submit Dataproc jobs)
  * `roles/storage.objectViewer` / `roles/storage.objectAdmin` (GCS access)
  * `roles/bigquery.user` (run BQ queries)
* **Cloud Run service account** (for Streamlit if accessing BigQuery):

  * `roles/bigquery.dataViewer` (read-only access to query data)

* **Cloud Run Job service account** (for ingestion):

  * `roles/storage.objectAdmin` (write to GCS raw bucket)

Grant minimum required privileges; avoid giving overly broad roles like Owner.

---

## Cost Considerations

* **Dataproc**: charged per cluster VM runtime. Use single-node cluster or set autoscaling / idle timeout.
* **BigQuery**: storage + query cost. Avoid repeated full-table scans; partition the table by `report_date` in future versions.
* **Composer**: environment cost; choose small environment for experiments.
* **Cloud Run**: pay-per-use; small for low traffic.

---

## Next Improvements & Ideas

* Partition BigQuery table by `report_date` to optimize query costs.
* Use Delta Lake / Hive metastore if you need time-travel.
* Add unit tests for ETL logic (PySpark local testing using small ndjson files).
* Add more visualizations and filters in Streamlit (e.g., heatmap choropleth).
* Implement alerting (Slack/email) on Composer task failures.
* Add retries, SLA sensors, and monitoring dashboards.

---

## Appendix — Full Code Snippets

(Place these in `dataproc/main.py`, `composer/dags/cdc_refresh_dag.py`, and `streamlit/app.py` respectively.)

### `cdc-dataproc/main.py` (final recommended)

```python
# PySpark ETL script for processing CDC hospital admissions data
# - Uses multiLine=True to read JSON arrays from GCS
# - Trims whitespace from string fields
# - Casts numeric fields to float for consistency
# - Converts report_date, week_end_date to TIMESTAMP using to_timestamp()
# - Writes cleaned Parquet to GCS (processed/cdc_clean/)
# - Loads directly into BigQuery using Spark BigQuery connector
# - Uses temporaryGcsBucket for BigQuery staging
```

### `cdc-composer/cdc-dag.py`

```python
# Cloud Composer DAG for orchestrating the CDC data pipeline
# - DAG ID: cdc_refresh_pipeline
# - Schedule: Every 12 hours (0 */12 * * *)
# - Task 1: BashOperator executes Cloud Run Job (cdc-ingest-job) for data ingestion
# - Task 2: DataprocSubmitJobOperator submits PySpark job to Dataproc cluster cdc-processing-cluster
# - Task dependencies: Task 1 completes before Task 2 starts
# - References main script: gs://cdc-health-ingestion-bucket/data-proc/main.py
# - Includes BigQuery connector JAR for Spark-to-BigQuery integration
```

### `cdc-streamlit/main.py`

```python
# Streamlit dashboard application
# Creates a BigQuery client and runs:
# df = client.query("SELECT ... FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`").to_dataframe()
# Then builds interactive charts and filters using Plotly.
# Remember to update the query with your actual project and dataset names.
```

### `cdc-ingestion/main.py`

```python
# Cloud Run Job container for CDC API data ingestion
# Fetches data from CDC API and uploads to GCS raw bucket
# Deployed as Cloud Run Job (cdc-ingest-job) and triggered by Composer DAG
# Can also be executed manually via gcloud run jobs execute command
# Handles errors gracefully with proper logging
```

(Full code blocks are intentionally referenced here to keep README concise — include the exact validated scripts in `cdc-dataproc/main.py`, `cdc-composer/cdc-dag.py`, `cdc-streamlit/main.py`, and `cdc-ingestion/main.py` in your repo.)

---

## Final notes

* Keep all environment/resource names consistent across your code and DAG.
* Monitor the Dataproc staging bucket (`cdc-dataproc-staging`) — if it gets deleted, jobs fail silently without driver logs.
* Ensure the BigQuery query in Streamlit (`cdc-streamlit/main.py`) is updated with your actual project and dataset names.
* The ingestion component is deployed as a Cloud Run Job (`cdc-ingest-job`) and is triggered by the Composer DAG as the first task. It can also be executed manually via `gcloud run jobs execute` command.
* When presenting: show one successful run (Composer DAG → Cloud Run Job → CDC API → GCS raw file → Dataproc job → BigQuery rows → Streamlit UI) and highlight logs + the DAG run history.
* All code files include comprehensive comments explaining each major section/function for better maintainability.


