"""
CDC Hospital Admissions Data Processing - Dataproc PySpark Job

This script processes raw CDC hospital admissions JSON data from GCS:
1. Reads raw JSON files from GCS bucket
2. Cleans and transforms data (type casting, trimming, date conversion)
3. Writes processed data to GCS as Parquet files
4. Loads cleaned data into BigQuery for analytics

Expected Input: JSON files in gs://cdc-health-ingestion-bucket/raw/*.json
Output: BigQuery table neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp


def main():
    """
    Main function orchestrating the CDC data processing pipeline.
    Initializes Spark session and executes the ETL workflow.
    """
    # Initialize Spark session for distributed data processing
    spark = SparkSession.builder \
        .appName("CDC Admissions Processing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    print("🚀 Dataproc Job Started: CDC Admissions Processing")

    # -------------------------------------------------------
    # STEP 1: Read Raw JSON Data from GCS
    # -------------------------------------------------------
    # Reads all JSON files from the raw data bucket using wildcard pattern
    # multiLine=True is required for JSON arrays or multi-line JSON objects
    raw_path = "gs://cdc-health-ingestion-bucket/raw/*.json"
    print(f"📥 Reading raw data from: {raw_path}")

    df = spark.read.json(raw_path, multiLine=True)

    print("📊 Raw Data Schema:")
    df.printSchema()

    # -------------------------------------------------------
    # STEP 2: Data Cleaning and Type Transformations
    # -------------------------------------------------------
    # Performs data quality transformations:
    # - Trims whitespace from string fields
    # - Converts numeric fields to appropriate data types
    # - Converts date strings to timestamp format for BigQuery compatibility
    numeric_cols = [
        "county_population",
        "health_sa_population",
        "mmwr_report_week",
        "mmwr_report_year",
        "total_adm_all_covid_confirmed",
        "total_adm_all_covid_confirmed_per_100k",
        "admissions_covid_confirmed",
        "avg_percent_inpatient_beds",
        "abs_chg_avg_percent_inpatient",
        "avg_percent_staff_icu_beds",
        "abs_chg_avg_percent_staff"
    ]

    df_clean = df

    # Trim text fields
    for c in ["state", "county", "fips_code", "health_sa_name",
              "total_adm_all_covid_confirmed_level",
              "admissions_covid_confirmed_level",
              "avg_percent_inpatient_beds_level",
              "avg_percent_staff_icu_beds_level"]:
        if c in df.columns:
            df_clean = df_clean.withColumn(c, trim(col(c)))

    # Convert numeric fields
    for c in numeric_cols:
        if c in df.columns:
            df_clean = df_clean.withColumn(c, col(c).cast("float"))

    # Convert date fields
    for d in ["report_date", "week_end_date"]:
        if d in df.columns:
            df_clean = df_clean.withColumn(d, to_timestamp(col(d)))

    print("✅ Data Cleaning Complete")

    # -------------------------------------------------------
    # STEP 3: Write Processed Data to GCS as Parquet
    # -------------------------------------------------------
    # Saves cleaned data in Parquet format for efficient storage and future processing
    # Parquet format provides better compression and schema preservation than JSON
    processed_path = "gs://cdc-health-ingestion-bucket/processed/cdc_clean"
    print(f"📤 Writing cleaned Parquet to: {processed_path}")

    df_clean.write.mode("overwrite").parquet(processed_path)

    print("📁 Cleaned Parquet written to GCS")

    # -------------------------------------------------------
    # STEP 4: Load Processed Data into BigQuery
    # -------------------------------------------------------
    # Writes the cleaned DataFrame directly to BigQuery using the Spark BigQuery connector
    # temporaryGcsBucket is required for staging intermediate data during the write operation
    # Overwrite mode replaces existing data in the table with fresh processed data
    print("📥 Loading cleaned data into BigQuery: neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions")

    df_clean.write.format("bigquery") \
        .option("table", "neeraja-data-pipeline.cdc_health_data.cdc_hospital_admissions") \
        .option("temporaryGcsBucket", "cdc-dataproc-staging") \
        .mode("overwrite") \
        .save()

    print("✅ BigQuery Load Completed Successfully")

    spark.stop()
    print("🎉 Dataproc Job Finished Successfully")

if __name__ == "__main__":
    main()
