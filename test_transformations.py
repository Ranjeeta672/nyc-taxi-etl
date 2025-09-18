from pyspark.sql import Row
from pyspark.sql import functions as F
import pytest

from etl_job import cleanse_and_cast, derive_partition_columns

def test_cleanse_and_cast_types_and_defaults(spark):
    data = [
        Row(VendorID="1", passenger_count=None, fare_amount="12.5", store_and_fwd_flag=None),
        Row(VendorID="2", passenger_count=2, fare_amount=None, store_and_fwd_flag="Y"),
    ]
    df = spark.createDataFrame(data)

    cleaned = cleanse_and_cast(df).collect()

    # All VendorIDs should be integers
    assert all(isinstance(r.VendorID, int) for r in cleaned)

    # All fare_amount should be floats
    assert all(isinstance(r.fare_amount, float) for r in cleaned)

    # Missing numeric and string values should be defaulted
    row1 = [r for r in cleaned if r.VendorID == 1][0]
    assert row1.passenger_count == 0
    assert row1.store_and_fwd_flag == ""


def test_cleanse_and_cast_removes_duplicates(spark):
    data = [Row(VendorID=1, fare_amount=10.0), Row(VendorID=1, fare_amount=10.0)]
    df = spark.createDataFrame(data)

    cleaned = cleanse_and_cast(df)

    assert cleaned.count() == 1


def test_derive_partition_columns(spark):
    data = [Row(tpep_pickup_datetime="2025-07-15 08:00:00")]
    df = spark.createDataFrame(data).withColumn(
        "tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime")
    )

    df_out = derive_partition_columns(df).collect()[0]

    assert df_out.pickup_year == 2025
    assert df_out.pickup_month == 7
