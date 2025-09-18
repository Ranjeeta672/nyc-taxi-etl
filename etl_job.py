import sys
from datetime import datetime

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(warehouse_path: str) -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-etl")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path)
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.fanout.enabled", "true")
        .getOrCreate()
    )


def cleanse_and_cast(df):
    # Standardize schema and types
    schema_casts = {
        "VendorID": T.IntegerType(),
        "tpep_pickup_datetime": T.TimestampType(),
        "tpep_dropoff_datetime": T.TimestampType(),
        "passenger_count": T.IntegerType(),
        "trip_distance": T.DoubleType(),
        "RatecodeID": T.IntegerType(),
        "store_and_fwd_flag": T.StringType(),
        "PULocationID": T.IntegerType(),
        "DOLocationID": T.IntegerType(),
        "payment_type": T.IntegerType(),
        "fare_amount": T.DoubleType(),
        "extra": T.DoubleType(),
        "mta_tax": T.DoubleType(),
        "tip_amount": T.DoubleType(),
        "tolls_amount": T.DoubleType(),
        "improvement_surcharge": T.DoubleType(),
        "total_amount": T.DoubleType(),
        "congestion_surcharge": T.DoubleType(),
    }

    for col, dtype in schema_casts.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(dtype))

    # Default missing values for numerics and strings commonly present
    numeric_cols = [
        c
        for c, t in df.dtypes
        if t in ("double", "float", "bigint", "int", "smallint", "tinyint", "long")
    ]
    string_cols = [c for c, t in df.dtypes if t == "string"]

    if numeric_cols:
        df = df.fillna(0, subset=numeric_cols)
    if string_cols:
        df = df.fillna("", subset=string_cols)

    # Remove exact duplicates (all columns)
    df = df.dropDuplicates()

    return df


def derive_partition_columns(df):
    # Partition by pickup year and month for typical query patterns
    return (
        df.withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    )


def write_raw_iceberg(df, raw_db: str, table_name: str = "yellow_trips"):
    full_name = f"glue_catalog.{raw_db}.{table_name}"
    # Create table if not exists with Iceberg using Spark SQL
    df.writeTo(full_name).option("fanout-enabled", "true").partitionedBy("pickup_year", "pickup_month").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()


def compute_and_write_gold(spark: SparkSession, raw_db: str, gold_db: str):
    raw_table = f"glue_catalog.{raw_db}.yellow_trips"

    df = spark.read.table(raw_table)
    df_2025 = df.filter(F.col("pickup_year") == 2025)

    # Monthly core metrics
    monthly_metrics = (
        df_2025.groupBy("pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.sum("tip_amount").alias("total_tips"),
            F.avg("tip_amount").alias("avg_tips_per_trip"),
            F.avg("passenger_count").alias("avg_passenger_count"),
        )
        .orderBy("pickup_month")
    )

    monthly_metrics.writeTo(f"glue_catalog.{gold_db}.monthly_metrics").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()

    # Payment type distribution per month (pivot)
    payment_dist = (
        df_2025.groupBy("pickup_month", "payment_type")
        .agg(F.count("*").alias("trip_count"))
        .groupBy("pickup_month")
        .pivot("payment_type")
        .agg(F.first("trip_count"))
        .na.fill(0)
        .orderBy("pickup_month")
    )

    payment_dist.writeTo(
        f"glue_catalog.{gold_db}.monthly_payment_distribution"
    ).tableProperty("write.format.default", "parquet").createOrReplace()

    # Top pickup and dropoff locations per month
    window_rank = (
        df_2025.groupBy("pickup_month", "PULocationID", "DOLocationID")
        .agg(F.count("*").alias("trip_count"))
    )

    # Keep top 10 per month
    from pyspark.sql.window import Window

    w = Window.partitionBy("pickup_month").orderBy(F.desc("trip_count"))
    top_locations = (
        window_rank.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
        .drop("rank")
        .orderBy("pickup_month", F.desc("trip_count"))
    )

    top_locations.writeTo(
        f"glue_catalog.{gold_db}.monthly_top_locations"
    ).tableProperty("write.format.default", "parquet").createOrReplace()

    # Average total amount per VendorID per month
    vendor_totals = (
        df_2025.groupBy("pickup_month", "VendorID")
        .agg(F.avg("total_amount").alias("avg_total_amount"))
        .orderBy("pickup_month", "VendorID")
    )

    vendor_totals.writeTo(
        f"glue_catalog.{gold_db}.monthly_total_amount_per_vendor"
    ).tableProperty("write.format.default", "parquet").createOrReplace()


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "INPUT_PATH",
            "WAREHOUSE_PATH",
            "RAW_DB",
            "GOLD_DB",
        ],
    )

    input_path = args["INPUT_PATH"]
    warehouse_path = args["WAREHOUSE_PATH"]
    raw_db = args["RAW_DB"]
    gold_db = args["GOLD_DB"]

    spark = build_spark(warehouse_path)

    # Ensure Glue databases exist (defensive against timing/region/catalog issues)
    glue = boto3.client("glue")
    for db in [raw_db, gold_db]:
        try:
            glue.get_database(Name=db)
        except glue.exceptions.EntityNotFoundException:
            glue.create_database(DatabaseInput={"Name": db})
        except Exception:
            # Fallback via Iceberg namespace create (works when catalog is loaded)
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")

    # Read input parquet(s)
    df = spark.read.parquet(input_path)

    # Track lineage columns
    df = df.withColumn("source_filename", F.input_file_name())
    df = df.withColumn("processing_ts", F.current_timestamp())

    # Clean and standardize
    df = cleanse_and_cast(df)

    # Derive partitions
    df = derive_partition_columns(df)

    # Ensure clustering by partition columns to satisfy Iceberg writer assumptions
    df = df.repartition("pickup_year", "pickup_month").sortWithinPartitions("pickup_year", "pickup_month")

    # Write to Iceberg raw table incrementally (append semantics, replace table definition if needed)
    write_raw_iceberg(df, raw_db)

    # Derive gold tables for 2025 monthly aggregations
    compute_and_write_gold(spark, raw_db, gold_db)

    spark.stop()


if __name__ == "__main__":
    main()


