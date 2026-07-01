# ✈️ Flights Booking Project — AWS Edition

GCP → AWS conversion of the Flights Booking data pipeline.

| GCP Service | AWS Equivalent |
|---|---|
| Cloud Composer (Airflow) | Amazon MWAA (Managed Workflows for Apache Airflow) |
| Dataproc Serverless (PySpark) | EMR Serverless |
| Google Cloud Storage (GCS) | Amazon S3 |
| BigQuery | AWS Glue Data Catalog + S3 Parquet (queryable via Athena) |
| GCS File Sensor | S3KeySensor (Apache Airflow) |

---

## Architecture

```
GitHub (dev/main branch push)
        │
        ▼
GitHub Actions CI/CD
        │
   ┌────┴──────────┐
   │               │
   ▼               ▼
MWAA DEV        MWAA PROD
(Airflow)       (Airflow)
   │               │
   ▼               ▼
S3KeySensor     S3KeySensor
(wait for CSV)  (wait for CSV)
   │               │
   ▼               ▼
EMR Serverless  EMR Serverless
(PySpark Job)   (PySpark Job)
   │               │
   ▼               ▼
S3 Output       S3 Output
+ Glue Catalog  + Glue Catalog
(queryable via Athena)
```

## Repository structure

```
.
├── airflow_job/
│   └── airflow_job.py          # MWAA DAG
├── spark_job/
│   └── spark_transformation_job.py   # EMR Serverless PySpark job
├── variables/
│   ├── dev/variables.json
│   └── prod/variables.json
└── .github/workflows/ci-cd.yaml
```
