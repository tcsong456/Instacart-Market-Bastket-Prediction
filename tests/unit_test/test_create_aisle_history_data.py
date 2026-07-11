from pyspark.sql import Row
from src.common.io import read_parquet
from tests.helper import assert_spark_df_equal
from src.ingestion.create_aisle_history_data import parse_seq, build_aisle_history_data
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    ArrayType,
)


def test_parse_seq(fake_user_data, spark):
    COLUMNS = [
        "user_id",
        "aisle_raw",
        "aisle_prev",
        "aisle_next",
        "aisle_all",
        "aisle_set",
        "next_aisle_set",
    ]

    user_data = read_parquet(fake_user_data / "user_data", spark)
    actual_df = parse_seq(user_data)
    actual_df = actual_df.select(COLUMNS)

    expected_schema = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("aisle_raw", ArrayType(StringType(), containsNull=False), True),
            StructField(
                "aisle_prev", ArrayType(StringType(), containsNull=False), True
            ),
            StructField("aisle_next", StringType(), True),
            StructField(
                "aisle_all",
                ArrayType(
                    ArrayType(IntegerType(), containsNull=True), containsNull=False
                ),
                True,
            ),
            StructField("aisle_set", ArrayType(IntegerType(), containsNull=True), True),
            StructField(
                "next_aisle_set", ArrayType(IntegerType(), containsNull=True), True
            ),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=10,
                aisle_raw=["5_10_15", "10_20", "5_10_5_15", "5"],
                aisle_prev=["5_10_15", "10_20", "5_10_5_15"],
                aisle_next="5",
                aisle_all=[[5, 10, 15], [10, 20], [5, 10, 5, 15]],
                aisle_set=[5, 10, 15, 20],
                next_aisle_set=[5],
            ),
            Row(
                user_id=20,
                aisle_raw=["5_5", "15", "15_20_20_15_5", "20_5_25"],
                aisle_prev=["5_5", "15", "15_20_20_15_5"],
                aisle_next="20_5_25",
                aisle_all=[[5, 5], [15], [15, 20, 20, 15, 5]],
                aisle_set=[5, 15, 20],
                next_aisle_set=[5, 20, 25],
            ),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(
        actual_df, expected_df, ["user_id"], ["aisle_set", "next_aisle_set"]
    )


def test_build_aisle_history_data(fake_parse_seq_data, spark):
    COLUMNS = [
        "user_id",
        "aisle_id",
        "is_ordered_history",
        "position_in_order",
        "num_products_from_aisle",
        "aisle_history_size",
        "order_dows",
        "order_hours",
        "days_since_prior_orders",
        "order_numbers",
        "eval_set",
    ]
    actual_df = build_aisle_history_data(fake_parse_seq_data)
    actual_df = actual_df.select(COLUMNS)

    common_cols_10 = dict(
        aisle_history_size="3 2 3",
        order_dows="1 2 3 4",
        order_hours="8 13 8 0",
        days_since_prior_orders="10 19 22 6",
        order_numbers="3 4 5 6",
        eval_set="train",
    )
    common_cols_20 = dict(
        aisle_history_size="2 2 3",
        order_dows="1 2 3 4",
        order_hours="8 13 8 0",
        days_since_prior_orders="10 19 22 6",
        order_numbers="3 4 5 6",
        eval_set="test",
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=10,
                aisle_id=5,
                is_ordered_history="1 0 1",
                position_in_order="1 0 1",
                num_products_from_aisle="1 0 2",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=10,
                is_ordered_history="1 1 1",
                position_in_order="2 1 2",
                num_products_from_aisle="1 1 1",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=15,
                is_ordered_history="1 0 1",
                position_in_order="3 0 4",
                num_products_from_aisle="1 0 1",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=20,
                is_ordered_history="0 1 0",
                position_in_order="0 2 0",
                num_products_from_aisle="0 1 0",
                **common_cols_10,
            ),
            Row(
                user_id=20,
                aisle_id=5,
                is_ordered_history="1 0 1",
                position_in_order="1 0 5",
                num_products_from_aisle="2 0 1",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=5,
                is_ordered_history="1 0 1",
                position_in_order="1 0 5",
                num_products_from_aisle="2 0 1",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=15,
                is_ordered_history="0 1 1",
                position_in_order="0 1 1",
                num_products_from_aisle="0 1 2",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=20,
                is_ordered_history="0 0 1",
                position_in_order="0 0 2",
                num_products_from_aisle="0 0 2",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=5,
                is_ordered_history="1 0 1",
                position_in_order="1 0 5",
                num_products_from_aisle="2 0 1",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=25,
                is_ordered_history="1 1 0",
                position_in_order="3 2 0",
                num_products_from_aisle="1 1 0",
                **common_cols_20,
            ),
        ]
    )
    actual_df.printSchema()
    assert_spark_df_equal(actual_df, expected_df, ["user_id", "aisle_id"], [])
