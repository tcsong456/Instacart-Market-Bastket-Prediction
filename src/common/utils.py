import logging
from pathlib import Path
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    ByteType,
    ShortType,
    IntegerType,
    StringType,
    DoubleType,
)


ORDER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", ShortType(), False),
        StructField("reordered", ByteType(), False),
    ]
)


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("eval_set", StringType(), False),
        StructField("order_number", ByteType(), False),
        StructField("order_dow", ByteType(), False),
        StructField("order_hour_of_day", ByteType(), False),
        StructField("days_since_prior_order", DoubleType(), True),
    ]
)


PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("aisle_id", ShortType(), False),
        StructField("department_id", ByteType(), False),
    ]
)


AISLES_SCHEMA = StructType(
    [
        StructField("aisle_id", IntegerType(), False),
        StructField("aisle", StringType(), False),
    ]
)

DEPARTMENTS_SCHEMA = StructType(
    [
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), False),
    ]
)


def gcs_join(base_path: str | Path, filename: str) -> str:
    return str(base_path).rstrip("/") + "/" + filename


def assert_spark_df_equal(
    actual_df: DataFrame, expected_df: DataFrame, order_by_col: list
) -> None:
    actual_df = actual_df.select(expected_df.columns)
    assert (
        actual_df.orderBy(*order_by_col).collect()
        == expected_df.orderBy(*order_by_col).collect()
    )
    assert actual_df.schema == expected_df.schema


def partition_logging(logger: logging.Logger, df: DataFrame) -> None:
    sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    logger.info("Partitions: %s", sizes)
    logger.info("Number of partitions: %s", len(sizes))
    logger.info("Max partition size: %s", max(sizes))
    logger.info("Min partition size: %s", min(sizes))


def partition_distribution(df: DataFrame) -> None:
    (
        df.withColumn("partition_id", F.spark_partition_id())
        .groupBy("partition_id")
        .count()
        .orderBy("partition_id")
        .show(100, truncate=False)
    )
