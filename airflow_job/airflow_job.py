from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}

# Define the DAG
with DAG(
    dag_id="flight_booking_emr_glue_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # Fetch environment variables from Airflow Variables
    env = Variable.get("env", default_var="dev")
    s3_bucket = Variable.get("s3_bucket", default_var="flights-booking-project-dev")
    aws_account_id = Variable.get("aws_account_id", default_var="123456789012")
    glue_database = Variable.get("glue_database", default_var=f"flight_data_{env}")
    aws_region = Variable.get("aws_region", default_var="us-east-1")
    emr_application_id = Variable.get("emr_application_id", default_var="")

    tables = Variable.get("tables", deserialize_json=True)

    # Extract table names
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique job run name
    job_run_name = f"flight-booking-job-{env}-{str(uuid.uuid4())[:8]}"

    # Task 1: S3 File Sensor — waits for source CSV to arrive
    file_sensor = S3KeySensor(
        task_id="check_file_arrival",
        bucket_name=s3_bucket,
        bucket_key=f"flight-booking-analysis/source-{env}/flight_booking.csv",
        aws_conn_id="aws_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 2: Submit EMR Serverless job via AWS CLI
    spark_job = BashOperator(
        task_id="run_spark_job_on_emr_serverless",
        bash_command="""
        aws emr-serverless start-job-run \
            --application-id {{ var.value.emr_application_id }} \
            --execution-role-arn arn:aws:iam::{{ var.value.aws_account_id }}:role/EMRServerlessExecutionRole \
            --region {{ var.value.aws_region }} \
            --name {{ run_id }} \
            --job-driver '{
                "sparkSubmit": {
                    "entryPoint": "s3://{{ var.value.s3_bucket }}/flight-booking-analysis/spark-job/spark_transformation_job.py",
                    "entryPointArguments": [
                        "--env={{ var.value.env }}",
                        "--s3_bucket={{ var.value.s3_bucket }}",
                        "--glue_database={{ var.value.glue_database }}",
                        "--transformed_table=transformed_flight_data_{{ var.value.env }}",
                        "--route_insights_table=route_insights_{{ var.value.env }}",
                        "--origin_insights_table=origin_insights_{{ var.value.env }}"
                    ],
                    "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalogImplementation=hive"
                }
            }' \
            --configuration-overrides '{
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": "s3://{{ var.value.s3_bucket }}/flight-booking-analysis/emr-logs/"
                    }
                }
            }'
        """,
    )

    # Task Dependencies
    file_sensor >> spark_job