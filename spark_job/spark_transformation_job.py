import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def job_process(env, s3_bucket, glue_database, transformed_table, route_insights_table, origin_insights_table):
    try:
        # Initialize SparkSession with AWS Glue Data Catalog as Hive metastore
        spark = SparkSession.builder \
            .appName("FlightBookingAnalysis") \
            .config(
                "spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            ) \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()

        logger.info("Spark session initialized.")

        # Resolve S3 input path based on the environment
        input_path = f"s3://{s3_bucket}/flight-booking-analysis/source-{env}/"
        output_base = f"s3://{s3_bucket}/flight-booking-analysis/output-{env}/"
        logger.info(f"Input path resolved: {input_path}")

        # Read the data from S3
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from S3.")

        # ── Data Transformations ──────────────────────────────────────────────
        logger.info("Starting data transformations.")

        transformed_data = data.withColumn(
            "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category",
            when(col("purchase_lead") < 7, lit("Last-Minute"))
            .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 30), lit("Short-Term"))
            .otherwise(lit("Long-Term"))
        ).withColumn(
            "booking_success_rate", expr("booking_complete / num_passengers")
        )

        # Aggregations
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )

        logger.info("Data transformations completed.")

        # ── Ensure Glue Database Exists ───────────────────────────────────────
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{glue_database}`")
        logger.info(f"Glue database ensured: {glue_database}")

        # ── Write Transformed Data to S3 + Register in Glue Catalog ──────────
        transformed_path = f"{output_base}{transformed_table}/"
        logger.info(f"Writing transformed data to: {transformed_path}")
        transformed_data.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", transformed_path) \
            .saveAsTable(f"`{glue_database}`.`{transformed_table}`")

        # ── Write Route Insights ──────────────────────────────────────────────
        route_path = f"{output_base}{route_insights_table}/"
        logger.info(f"Writing route insights to: {route_path}")
        route_insights.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", route_path) \
            .saveAsTable(f"`{glue_database}`.`{route_insights_table}`")

        # ── Write Booking Origin Insights ─────────────────────────────────────
        origin_path = f"{output_base}{origin_insights_table}/"
        logger.info(f"Writing booking origin insights to: {origin_path}")
        booking_origin_insights.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", origin_path) \
            .saveAsTable(f"`{glue_database}`.`{origin_insights_table}`")

        logger.info("All data written to S3 and registered in Glue Catalog successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process flight booking data and write to S3 / Glue Catalog.")
    parser.add_argument("--env",                   required=True, help="Environment (dev or prod)")
    parser.add_argument("--s3_bucket",             required=True, help="S3 bucket name")
    parser.add_argument("--glue_database",         required=True, help="AWS Glue database name")
    parser.add_argument("--transformed_table",     required=True, help="Glue table for transformed data")
    parser.add_argument("--route_insights_table",  required=True, help="Glue table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="Glue table for booking origin insights")

    args = parser.parse_args()

    job_process(
        env=args.env,
        s3_bucket=args.s3_bucket,
        glue_database=args.glue_database,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table,
    )
