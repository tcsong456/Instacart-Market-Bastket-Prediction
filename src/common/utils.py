import logging
from pathlib import Path
from pyspark.sql import DataFrame, functions as F, Column
from pyspark.sql.types import (
    StructField,
    StructType,
    ByteType,
    ShortType,
    IntegerType,
    StringType,
    DoubleType,
    ArrayType,
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


def pad_array(
    array_column: F.Column,
    max_length: int,
) -> tuple[F.Column, F.Column]:
    """
    Truncate or pad an integer array to a fixed length.

    Args:
        array_column: Spark array column containing integer values.
        max_length: Desired output array length.

    Returns:
        A tuple containing:
            - padded_array: Array column of exactly ``max_length`` elements.
            - seq_length: Integer column representing the original sequence
              length after truncation (i.e. ``min(original_length, max_length)``).
    """

    truncated_array = F.slice(array_column, 1, max_length)
    seq_length = F.size(truncated_array).cast("int")
    padding_len = max_length - seq_length
    padded_array = F.concat(
        truncated_array, F.array_repeat(F.lit(0).cast("int"), padding_len)
    )

    return padded_array, seq_length


def parse_string_sequence(column: Column, pattern: str = r"\s+") -> F.Column:
    """
    Parse a whitespace-delimited string column into an integer array.
    Null values and empty strings are converted to empty arrays. Otherwise,
    the string is trimmed, split on whitespace, and each token is cast to an
    integer.

    Args:
        column_name: A column containing whitespace-delimited
            integer values.

    Returns:
        A Spark array column of integers.

    Examples:
        "1 2 3" -> [1, 2, 3]
        " 4  5 " -> [4, 5]
        "" -> []
        None -> []
    """

    empty_array = F.array().cast(ArrayType(IntegerType()))

    return F.when(
        column.isNull() | (F.trim(column) == ""),
        empty_array,
    ).otherwise(F.transform(F.split(F.trim(column), pattern), lambda x: x.cast("int")))
