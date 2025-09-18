## NYC Yellow Taxi – Minimal Terraform + Glue PySpark

This project provisions a minimal AWS Glue job via Terraform and runs a compact PySpark ETL that writes RAW and GOLD Iceberg tables for NYC Yellow Taxi data.

### Clean S3 structure (only two items)
- Bucket: `rowdynyc`
- Existing prefix: `nyctaxi/` (you already have your parquet here)
- New prefix created: `nyctaxi/warehouse/` (Iceberg warehouse for RAW and GOLD)

Input file you already have:
- `s3://rowdynyc/nyctaxi/yellow_tripdata_2025-07.parquet`

### What the ETL does
1. Reads parquet(s) from `s3://rowdynyc/nyctaxi/yellow_tripdata_*.parquet`.
2. Cleans and casts columns, removes duplicates, fills missing values.
3. Adds `source_filename` and `processing_ts` to each row.
4. Writes RAW Iceberg table `glue_catalog.nyc_raw.yellow_trips` partitioned by `pickup_year`, `pickup_month` into `s3://rowdynyc/nyctaxi/warehouse/`.
5. Builds GOLD monthly metrics for 2025 and writes Iceberg tables in `nyc_gold` database:
   - `monthly_metrics`
   - `monthly_payment_distribution`
   - `monthly_top_locations`
   - `monthly_total_amount_per_vendor`

### Prerequisites
- AWS account and credentials configured locally (Administrator or equivalent for Glue/S3/Logs/IAM).
- Terraform >= 1.6, AWS provider >= 5.0.

### Deploy
```bash
terraform init
terraform apply -auto-approve
```
Outputs include the Glue job name.

### Run the job (Terraform-only)
Terraform creates a `SCHEDULED` Glue trigger with a far-future cron and `start_on_creation = true`. Creation of the trigger immediately fires one job run without recurring runs. To re-run without any CLI except Terraform:

1) First run:
```bash
terraform apply -auto-approve -var="aws_region=eu-north-1" -var="aws_access_key_id=..." -var="aws_secret_access_key=..."
```
2) Re-run later by changing the token (forces a new trigger):
```bash
terraform apply -auto-approve -var="aws_region=eu-north-1" -var="run_token=run2" -var="aws_access_key_id=..." -var="aws_secret_access_key=..."
```
Each unique `run_token` creates a new trigger whose creation immediately starts a new job run.

### Querying results (Athena v3)
1. In Athena, set the data source to Glue Data Catalog.
2. Create catalogs if needed and ensure Iceberg engine is enabled.
3. Example queries:
```sql
SELECT * FROM nyc_gold.monthly_metrics ORDER BY pickup_month;
SELECT * FROM nyc_gold.monthly_payment_distribution ORDER BY pickup_month;
SELECT * FROM nyc_gold.monthly_top_locations ORDER BY pickup_month, trip_count DESC;
SELECT * FROM nyc_gold.monthly_total_amount_per_vendor ORDER BY pickup_month, vendorid;
```

### Local development (VS Code)
Open the folder and edit `etl_job.py` or `main.tf`. Commit and push to GitHub as needed.

### Notes
- The pipeline is incremental by design: each run appends new data to the Iceberg RAW table; GOLD tables are rebuilt from RAW for 2025.
- Keep dropping more monthly parquet files into `s3://rowdynyc/nyctaxi/` to extend coverage.


