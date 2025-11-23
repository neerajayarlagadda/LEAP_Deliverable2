# CDC Hospital Admissions Data Pipeline - GCP Implementation

**Project**: CDC Hospital Admissions Pipeline  
**Owner**: Neeraja Y.  
**GCP Project**: `neeraja-data-pipeline`  
**Last Updated**: 2025-11-23

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Implementation Flow](#implementation-flow)
3. [Architecture](#architecture)
4. [Step-by-Step Implementation](#step-by-step-implementation)
5. [Testing & Validation](#testing--validation)
6. [GCP Resources](#gcp-resources)
7. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project implements an end-to-end data pipeline on Google Cloud Platform (GCP) that:

- **Ingests** CDC hospital admissions data from a public API
- **Processes** raw JSON data using Dataproc (PySpark)
- **Stores** cleaned data in BigQuery for analytics
- **Visualizes** data through an interactive Streamlit dashboard
- **Orchestrates** the entire pipeline using Cloud Composer (Airflow)

The pipeline follows a modern data engineering approach with containerized services, serverless processing, and automated orchestration.

---

## Implementation Flow

This README documents the implementation flow that was followed:

1. ✅ **Ingestion using Cloud Run** - Deployed containerized ingestion service
2. ✅ **Testing** - Verified ingestion service functionality
3. ✅ **BigQuery Dataset Creation** - Set up dataset and table schema
4. ✅ **Dataproc Cluster & Data Validation** - Created cluster and verified data processing
5. ✅ **Streamlit Dashboard on Cloud Run** - Deployed and tested interactive dashboard
6. ✅ **Cloud Composer DAG** - Created and tested complete end-to-end flow

---

## Architecture

```
┌─────────────────┐
│   CDC API       │
│  (data.cdc.gov) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Cloud Composer (Airflow DAG)       │
│  - Orchestrates entire pipeline     │
│  - Scheduled: Every 12 hours        │
└────────┬────────────────────────────┘
         │
         ├─── Task 1: Cloud Run Job (Ingestion)
         │    │
         │    ▼
         │    ┌─────────────────────────────┐
         │    │  Cloud Run Job              │
         │    │  (cdc-ingest-job)           │
         │    │  - Fetches CDC API data     │
         │    │  - Uploads to GCS           │
         │    └──────────┬──────────────────┘
         │               │
         │               ▼
         │    ┌─────────────────────────────┐
         │    │  GCS Raw Bucket             │
         │    │  gs://.../raw/*.json        │
         │    └──────────┬──────────────────┘
         │               │
         └─── Task 2: Dataproc Job (Processing)
              │
              ▼
         ┌─────────────────────────────┐
         │  Dataproc Cluster           │
         │  (cdc-processing-cluster)   │
         │  - PySpark ETL processing   │
         │  - Data cleaning & transform│
         └──────────┬──────────────────┘
                    │
                    ├───► GCS Processed (Parquet)
                    │     gs://.../processed/cdc_clean/
                    │
                    ▼
         ┌─────────────────────────────┐
         │  BigQuery Table             │
         │  cdc_hospital_admissions    │
         └──────────┬──────────────────┘
                    │
                    ▼
         ┌─────────────────────────────┐
         │  Streamlit Dashboard        │
         │  (Cloud Run Service)        │
         │  - Interactive visualizations│
         │  - Real-time data queries   │
         └─────────────────────────────┘
```

---

## Step-by-Step Implementation

### Step 1: Ingestion using Cloud Run

**Objective**: Create a containerized service to fetch CDC API data and store it in GCS.

**Implementation Details**:
- Created `cdc-ingestion/main.py` - Python script that fetches data from CDC API
- Created `cdc-ingestion/Dockerfile` - Containerizes the ingestion service
- Deployed as Cloud Run Job for serverless execution

**Files**:
- `cdc-ingestion/main.py` - Ingestion logic
- `cdc-ingestion/Dockerfile` - Container definition
- `cdc-ingestion/requirements.txt` - Dependencies

**Deployment Commands**:

```powershell
# Build and push Docker image
cd cdc-ingestion
gcloud builds submit --tag gcr.io/neeraja-data-pipeline/cdc-ingest-job

# Create Cloud Run Job
gcloud run jobs create cdc-ingest-job `
  --image gcr.io/neeraja-data-pipeline/cdc-ingest-job `
  --region us-central1 `
  --max-retries 3 `
  --task-timeout 300
```

**What it does**:
- Fetches data from CDC API: `https://data.cdc.gov/resource/akn2-qxic.json`
- Uploads JSON response to GCS bucket: `gs://cdc-health-ingestion-bucket/raw/cdc_data_<timestamp>.json`
- Handles errors gracefully with proper logging

---

### Step 2: Testing the Ingestion Service

**Objective**: Verify that the Cloud Run Job successfully fetches and stores data.

**Testing Steps**:

1. **Execute the job manually**:
```powershell
gcloud run jobs execute cdc-ingest-job --region us-central1
```

2. **Verify data in GCS**:
```powershell
gsutil ls gs://cdc-health-ingestion-bucket/raw/
```

3. **Check file contents**:
```powershell
gsutil cat gs://cdc-health-ingestion-bucket/raw/cdc_data_*.json | head -n 50
```

4. **Review Cloud Run logs**:
```powershell
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=cdc-ingest-job" --limit 50
```

**Expected Results**:
- ✅ Job executes successfully
- ✅ JSON file appears in GCS raw bucket
- ✅ File contains valid CDC hospital admissions data
- ✅ Logs show successful completion

---

### Step 3: Create BigQuery Dataset

**Objective**: Set up BigQuery dataset and table schema for storing processed data.

**Implementation**:

1. **Create BigQuery Dataset**:
```sql
CREATE SCHEMA IF NOT EXISTS `neeraja-data-pipeline.cdc_health_data`;
```

2. **Create Table with Schema**:
```sql
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

**Verification**:
```sql
SELECT COUNT(*) as total_records 
FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`;
```

---

### Step 4: Create Dataproc Cluster and Check Data in BigQuery Table

**Objective**: Set up Dataproc cluster, process raw data, and verify data appears in BigQuery.

**Implementation Details**:

1. **Create GCS Buckets** (if not exists):
```powershell
# Main ingestion bucket
gsutil mb -l US gs://cdc-health-ingestion-bucket

# Dataproc staging bucket
gsutil mb -l US gs://cdc-dataproc-staging
```

2. **Upload PySpark Script to GCS**:
```powershell
gsutil cp cdc-dataproc/main.py gs://cdc-health-ingestion-bucket/data-proc/main.py
```

3. **Create Dataproc Cluster**:
```powershell
gcloud dataproc clusters create cdc-processing-cluster `
  --region=us-central1 `
  --zone=us-central1-a `
  --single-node `
  --master-machine-type=n1-standard-4 `
  --image-version=2.1-debian11
```

4. **Submit PySpark Job**:
```powershell
gcloud dataproc jobs submit pyspark gs://cdc-health-ingestion-bucket/data-proc/main.py `
  --cluster=cdc-processing-cluster `
  --region=us-central1 `
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar
```

**What the PySpark Job Does** (`cdc-dataproc/main.py`):
- Reads raw JSON files from GCS using `multiLine=True` (handles JSON arrays)
- Cleans data: trims whitespace, casts numeric fields, converts dates
- Writes processed Parquet to `gs://cdc-health-ingestion-bucket/processed/cdc_clean/`
- Loads cleaned data directly into BigQuery table

**Verification Steps**:

1. **Check Dataproc Job Status**:
```powershell
gcloud dataproc jobs list --region=us-central1
```

2. **Verify Processed Data in GCS**:
```powershell
gsutil ls gs://cdc-health-ingestion-bucket/processed/cdc_clean/
```

3. **Query BigQuery Table**:
```sql
SELECT 
  state, 
  county, 
  report_date, 
  total_adm_all_covid_confirmed,
  avg_percent_inpatient_beds
FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`
ORDER BY report_date DESC
LIMIT 50;
```

**Expected Results**:
- ✅ Dataproc job completes successfully
- ✅ Parquet files appear in processed GCS path
- ✅ BigQuery table contains cleaned data
- ✅ Data types are correct (timestamps, numeric fields)
- ✅ No null or corrupted records

---

### Step 5: Create Streamlit Dashboard using Cloud Run and Test

**Objective**: Deploy an interactive Streamlit dashboard to visualize BigQuery data.

**Implementation Details**:

1. **Update BigQuery Query in Streamlit**:
   - Edit `cdc-streamlit/main.py`
   - Update query to use correct project/dataset:
   ```python
   FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`
   ```

2. **Build Docker Image**:
```powershell
cd cdc-streamlit
gcloud builds submit --tag gcr.io/neeraja-data-pipeline/cdc-streamlit
```

3. **Deploy to Cloud Run**:
```powershell
gcloud run deploy cdc-streamlit `
  --image gcr.io/neeraja-data-pipeline/cdc-streamlit `
  --platform managed `
  --region us-central1 `
  --allow-unauthenticated `
  --port 8080
```

**Dashboard Features** (`cdc-streamlit/main.py`):
- State filter dropdown
- Time series chart showing COVID-19 admission trends
- Bar chart displaying inpatient bed utilization by county
- Data table with admission level indicators
- Real-time queries from BigQuery on page load

**Testing Steps**:

1. **Local Testing** (optional):
```powershell
cd cdc-streamlit
pip install -r requirements.txt
streamlit run main.py
# Access at http://localhost:8501
```

2. **Access Cloud Run Service**:
   - Get service URL:
   ```powershell
   gcloud run services describe cdc-streamlit --region us-central1 --format="value(status.url)"
   ```
   - Open URL in browser
   - Verify dashboard loads and displays data

3. **Test Dashboard Features**:
   - ✅ Select different states from dropdown
   - ✅ Verify charts update correctly
   - ✅ Check data table displays correctly
   - ✅ Confirm data matches BigQuery table

**Expected Results**:
- ✅ Dashboard loads successfully
- ✅ Charts render with data
- ✅ State filter works correctly
- ✅ Data matches BigQuery table contents

---

### Step 6: Create DAG and Test Complete Flow

**Objective**: Orchestrate the entire pipeline using Cloud Composer (Airflow) and test end-to-end.

**Implementation Details**:

1. **Create Cloud Composer Environment** (if not exists):
```powershell
gcloud composer environments create cdc-health-composer `
  --location us-central1 `
  --image-version composer-2.7.0-airflow-2.7.3
```

2. **Get Composer Bucket Name**:
```powershell
gcloud composer environments describe cdc-health-composer `
  --location us-central1 `
  --format="value(config.dagGcsPrefix)"
```

3. **Upload DAG to Composer**:
```powershell
gsutil cp cdc-composer/cdc-dag.py gs://<COMPOSER_BUCKET>/dags/cdc-dag.py
```

**DAG Structure** (`cdc-composer/cdc-dag.py`):

- **DAG ID**: `cdc_refresh_pipeline`
- **Schedule**: Every 12 hours (`0 */12 * * *`)
- **Task 1**: `run_cdc_ingestion` (BashOperator)
  - Executes Cloud Run Job `cdc-ingest-job`
  - Fetches CDC API data and uploads to GCS
- **Task 2**: `run_cdc_dataproc_job` (DataprocSubmitJobOperator)
  - Submits PySpark job to Dataproc cluster
  - Processes raw data and loads into BigQuery
- **Dependencies**: Task 1 → Task 2 (sequential execution)

**Testing Steps**:

1. **Verify DAG Appears in Airflow UI**:
   - Access Airflow UI:
   ```powershell
   gcloud composer environments describe cdc-health-composer `
     --location us-central1 `
     --format="value(config.airflowUri)"
   ```
   - Navigate to DAGs page
   - Confirm `cdc_refresh_pipeline` appears

2. **Trigger DAG Manually**:
   - In Airflow UI: DAGs → `cdc_refresh_pipeline` → Trigger DAG
   - Or via command:
   ```powershell
   gcloud composer environments run cdc-health-composer `
     --location us-central1 `
     dags trigger cdc_refresh_pipeline
   ```

3. **Monitor DAG Execution**:
   - Watch task execution in Airflow UI
   - Check task logs for each step
   - Verify task dependencies (Task 1 completes before Task 2)

4. **End-to-End Validation**:
   - ✅ Task 1 (Ingestion) completes successfully
   - ✅ New JSON file appears in GCS raw bucket
   - ✅ Task 2 (Dataproc) starts after Task 1
   - ✅ Dataproc job completes successfully
   - ✅ BigQuery table is updated with new data
   - ✅ Streamlit dashboard shows updated data

5. **Verify Scheduled Runs**:
   - Wait for scheduled execution (every 12 hours)
   - Confirm DAG runs automatically
   - Check historical runs in Airflow UI

**Expected Results**:
- ✅ DAG appears in Airflow UI
- ✅ Manual trigger executes successfully
- ✅ Both tasks complete without errors
- ✅ Data flows: CDC API → GCS → Dataproc → BigQuery
- ✅ Scheduled runs execute automatically
- ✅ Complete pipeline works end-to-end

---

## Testing & Validation

### Complete Pipeline Validation Checklist

- [ ] **Ingestion**: Cloud Run Job fetches data and stores in GCS
- [ ] **Raw Data**: JSON files are valid and contain expected fields
- [ ] **Dataproc Processing**: PySpark job processes data correctly
- [ ] **BigQuery**: Table contains cleaned, properly typed data
- [ ] **Streamlit**: Dashboard displays data and visualizations work
- [ ] **DAG Orchestration**: Both tasks execute in correct order
- [ ] **Scheduled Runs**: DAG runs automatically on schedule
- [ ] **Error Handling**: Pipeline handles failures gracefully

### Validation Queries

**Check Raw Data Count**:
```powershell
gsutil ls gs://cdc-health-ingestion-bucket/raw/ | Measure-Object -Line
```

**Check BigQuery Record Count**:
```sql
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT state) as unique_states,
       MIN(report_date) as earliest_date,
       MAX(report_date) as latest_date
FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`;
```

**Check Data Quality**:
```sql
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT state) as states,
  COUNT(DISTINCT county) as counties,
  SUM(CASE WHEN total_adm_all_covid_confirmed IS NULL THEN 1 ELSE 0 END) as null_admissions
FROM `neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions`;
```

---

## GCP Resources

### Resource Naming Convention

| Resource Type | Name | Region | Purpose |
|--------------|------|--------|---------|
| GCS Bucket | `cdc-health-ingestion-bucket` | US | Raw and processed data storage |
| GCS Bucket | `cdc-dataproc-staging` | US | Dataproc staging and temp files |
| Cloud Run Job | `cdc-ingest-job` | us-central1 | Data ingestion service |
| Cloud Run Service | `cdc-streamlit` | us-central1 | Streamlit dashboard |
| Dataproc Cluster | `cdc-processing-cluster` | us-central1 | PySpark data processing |
| BigQuery Dataset | `cdc_health_data` | US | Analytics dataset |
| BigQuery Table | `cdc_hospital_admissions` | US | Processed data table |
| Composer Environment | `cdc-health-composer` | us-central1 | Airflow orchestration |

### Required IAM Permissions

**Dataproc Service Account**:
- `roles/storage.objectViewer` (read from GCS)
- `roles/storage.objectAdmin` (write to GCS)
- `roles/bigquery.dataEditor` (write to BigQuery)

**Composer Service Account**:
- `roles/dataproc.jobRunner` (submit Dataproc jobs)
- `roles/storage.objectViewer` (read GCS)
- `roles/run.invoker` (invoke Cloud Run jobs)

**Cloud Run Service Account** (Streamlit):
- `roles/bigquery.dataViewer` (read from BigQuery)

**Cloud Run Job Service Account** (Ingestion):
- `roles/storage.objectAdmin` (write to GCS)

---

## Troubleshooting

### Common Issues and Solutions

**1. Cloud Run Job Fails**
- **Issue**: Job execution fails with timeout or API error
- **Solution**: Check CDC API availability, increase timeout, verify GCS permissions

**2. Dataproc Job Fails**
- **Issue**: "File not found" or "ClassNotFoundException"
- **Solution**: Verify PySpark script is uploaded to GCS, include BigQuery connector JAR

**3. BigQuery Write Errors**
- **Issue**: Schema mismatch or permission errors
- **Solution**: Verify table schema matches DataFrame, check service account permissions

**4. Streamlit Dashboard Shows No Data**
- **Issue**: Dashboard loads but shows empty charts
- **Solution**: Verify BigQuery query uses correct project/dataset, check service account has BigQuery read permissions

**5. DAG Tasks Fail**
- **Issue**: Tasks fail in Airflow
- **Solution**: Check task logs, verify resource names match, ensure dependencies are correct

**6. Scheduled DAG Not Running**
- **Issue**: DAG doesn't trigger on schedule
- **Solution**: Verify DAG is enabled, check schedule_interval syntax, confirm Composer environment is running

---

## Project Structure

```
datapipeline-gcp/
├── Readme.md                    # This file
├── cdc-ingestion/               # Step 1: Cloud Run Ingestion
│   ├── main.py                  # Ingestion logic
│   ├── Dockerfile               # Container definition
│   └── requirements.txt         # Python dependencies
├── cdc-dataproc/                # Step 4: Dataproc Processing
│   └── main.py                  # PySpark ETL script
├── cdc-streamlit/               # Step 5: Streamlit Dashboard
│   ├── main.py                  # Dashboard application
│   ├── Dockerfile               # Container definition
│   └── requirements.txt         # Python dependencies
└── cdc-composer/                # Step 6: Airflow DAG
    └── cdc-dag.py               # Orchestration DAG
```

---

## Summary

This pipeline successfully implements a complete data engineering workflow on GCP:

1. ✅ **Ingestion**: Cloud Run Job fetches CDC data and stores in GCS
2. ✅ **Processing**: Dataproc PySpark job cleans and transforms data
3. ✅ **Storage**: BigQuery stores analytics-ready data
4. ✅ **Visualization**: Streamlit dashboard provides interactive insights
5. ✅ **Orchestration**: Cloud Composer DAG automates the entire pipeline

The pipeline is production-ready with error handling, logging, and scheduled execution. All components have been tested and validated end-to-end.

---

**For questions or issues, refer to the detailed code comments in each component or check the GCP service logs for debugging.**
