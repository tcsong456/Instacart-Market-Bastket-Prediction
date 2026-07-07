# import pandas as pd
from pyspark.sql import Row
from src.common.io import read_csv, read_parquet
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from src.common.utils import assert_spark_df_equal
from src.ingestion.create_product_history_data import (
    parse_seq,
    filtered_orders,
    build_each_product_in_order_history,
    SELECTED_COLUMNS,
    build_each_reorder_history,
)


def test_parse_seq_with_set(spark, fake_user_data):
    df = read_parquet(fake_user_data / "user_data", spark)
    df = parse_seq(
        df=df, input_col="product_ids", prefix="products", calculate_set=True
    )

    row = df.filter("user_id==10").first()
    assert row["products_raw"] == ["1_2_3", "4_10", "5_323_1_12", "5"]
    assert row["products_prev"] == ["1_2_3", "4_10", "5_323_1_12"]
    assert row["products_all"] == [[1, 2, 3], [4, 10], [5, 323, 1, 12]]
    assert row["next_products"] == "5"
    assert row["next_products_int"] == [5]
    assert sorted(row["products_set"]) == [1, 2, 3, 4, 5, 10, 12, 323]
    assert sorted(row["next_products_set"]) == [5]


def test_parse_seq_without_set(spark, fake_user_data):
    df = read_parquet(fake_user_data / "user_data", spark)
    df = parse_seq(df=df, input_col="reorders", prefix="reorders", calculate_set=False)

    row = df.filter("user_id==20").first()
    assert row["reorders_raw"] == ["0_0", "0", "0_1_0_1_1", "1_1_0"]
    assert row["reorders_prev"] == ["0_0", "0", "0_1_0_1_1"]
    assert row["reorders_all"] == [[0, 0], [0], [0, 1, 0, 1, 1]]
    assert row["next_reorders"] == "1_1_0"
    assert row["next_reorders_int"] == [1, 1, 0]
    assert "reorders_set" not in df.columns
    assert "next_reorders_set" not in df.columns


def test_filtered_orders(spark, fake_orders):
    df = filtered_orders(fake_orders, spark)

    assert df.count() == 2
    assert set(df.columns) == {"user_id", "target_eval_set"}

    rows = {row["user_id"]: row["target_eval_set"] for row in df.collect()}

    assert rows == {10: "train", 20: "test"}


def test_build_each_product_in_order_history(
    spark, fake_parse_seq_data, fake_filtered_orders, fake_products_data
):
    df = fake_parse_seq_data
    orders = read_csv(fake_filtered_orders / "filtered_orders.csv", spark)

    actual_df = build_each_product_in_order_history(
        path=fake_products_data, df=df, orders=orders, spark=spark
    )

    common_cols_1 = dict(
        user_id=10,
        order_dows="5 0 5 1",
        order_hours="23 1 17 16",
        days_since_prior_orders="-1 30 60 25",
        order_numbers="1 2 3 4",
        history_order_size="3 2 4",
        history_reorder_size="0 0 1",
        eval_set="train",
    )
    common_cols_2 = dict(
        user_id=20,
        order_dows="1 2 3 4",
        order_hours="8 13 8 0",
        days_since_prior_orders="10 19 22 6",
        order_numbers="3 4 5 6",
        history_order_size="3 2 5",
        history_reorder_size="0 1 2",
        eval_set="test",
    )
    expected_schema = StructType(
        [
            StructField("product_id", LongType(), True),
            StructField("label", IntegerType(), True),
            StructField("is_ordered_history", StringType(), True),
            StructField("position_in_order_history", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("aisle_id", IntegerType(), True),
            StructField("department_id", IntegerType(), True),
            StructField("user_id", LongType(), True),
            StructField("order_dows", StringType(), True),
            StructField("order_hours", StringType(), True),
            StructField("days_since_prior_orders", StringType(), True),
            StructField("order_numbers", StringType(), True),
            StructField("history_order_size", StringType(), True),
            StructField("history_reorder_size", StringType(), False),
            StructField("eval_set", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                product_id=1,
                label=0,
                is_ordered_history="1 0 1",
                position_in_order_history="1 0 3",
                product_name="b",
                aisle_id=5,
                department_id=10,
                **common_cols_1,
            ),
            Row(
                product_id=2,
                label=0,
                is_ordered_history="1 0 0",
                position_in_order_history="2 0 0",
                product_name="c",
                aisle_id=10,
                department_id=10,
                **common_cols_1,
            ),
            Row(
                product_id=3,
                label=0,
                is_ordered_history="1 0 0",
                position_in_order_history="3 0 0",
                product_name="d",
                aisle_id=15,
                department_id=30,
                **common_cols_1,
            ),
            Row(
                product_id=4,
                label=0,
                is_ordered_history="0 1 0",
                position_in_order_history="0 1 0",
                product_name="e",
                aisle_id=10,
                department_id=20,
                **common_cols_1,
            ),
            Row(
                product_id=5,
                label=1,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 1",
                product_name="f",
                aisle_id=5,
                department_id=10,
                **common_cols_1,
            ),
            Row(
                product_id=10,
                label=0,
                is_ordered_history="0 1 0",
                position_in_order_history="0 2 0",
                product_name="g",
                aisle_id=20,
                department_id=40,
                **common_cols_1,
            ),
            Row(
                product_id=12,
                label=0,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 4",
                product_name="i",
                aisle_id=15,
                department_id=20,
                **common_cols_1,
            ),
            Row(
                product_id=323,
                label=0,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 2",
                product_name="n",
                aisle_id=10,
                department_id=20,
                **common_cols_1,
            ),
            Row(
                product_id=5,
                label=-1,
                is_ordered_history="1 0 0",
                position_in_order_history="2 0 0",
                product_name="f",
                aisle_id=5,
                department_id=10,
                **common_cols_2,
            ),
            Row(
                product_id=11,
                label=-1,
                is_ordered_history="1 0 1",
                position_in_order_history="1 0 5",
                product_name="h",
                aisle_id=5,
                department_id=30,
                **common_cols_2,
            ),
            Row(
                product_id=12,
                label=-1,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 1",
                product_name="i",
                aisle_id=15,
                department_id=20,
                **common_cols_2,
            ),
            Row(
                product_id=20,
                label=-1,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 2",
                product_name="l",
                aisle_id=20,
                department_id=30,
                **common_cols_2,
            ),
            Row(
                product_id=23,
                label=-1,
                is_ordered_history="1 1 0",
                position_in_order_history="3 2 0",
                product_name="z",
                aisle_id=25,
                department_id=50,
                **common_cols_2,
            ),
            Row(
                product_id=300,
                label=-1,
                is_ordered_history="0 1 1",
                position_in_order_history="0 1 4",
                product_name="o",
                aisle_id=15,
                department_id=40,
                **common_cols_2,
            ),
            Row(
                product_id=1000,
                label=-1,
                is_ordered_history="0 0 1",
                position_in_order_history="0 0 3",
                product_name="m",
                aisle_id=20,
                department_id=40,
                **common_cols_2,
            ),
        ],
        schema=expected_schema,
    )
    expected_df = expected_df.select(SELECTED_COLUMNS)

    assert_spark_df_equal(actual_df, expected_df, ["user_id", "product_id"])


def test_build_each_reorder_history(spark, fake_parse_seq_data, fake_filtered_orders):
    df = fake_parse_seq_data

    orders = read_csv(fake_filtered_orders / "filtered_orders.csv", spark)
    actual_reorders = build_each_reorder_history(df, orders)

    common_cols_1 = dict(
        user_id=10,
        order_dows="5 0 5 1",
        order_hours="23 1 17 16",
        days_since_prior_orders="-1 30 60 25",
        order_numbers="1 2 3 4",
        eval_set="train",
    )
    common_cols_2 = dict(
        user_id=20,
        order_dows="1 2 3 4",
        order_hours="8 13 8 0",
        days_since_prior_orders="10 19 22 6",
        order_numbers="3 4 5 6",
        eval_set="test",
    )
    common_cols_3 = dict(
        product_id=0,
        aisle_id=0,
        department_id=0,
        product_name="",
        position_in_order_history="0 0 0",
    )
    expected_schema = StructType(
        [
            StructField("label", IntegerType(), True),
            StructField("is_ordered_history", StringType(), True),
            StructField("history_order_size", StringType(), True),
            StructField("history_reorder_size", StringType(), True),
            StructField("user_id", LongType(), True),
            StructField("order_dows", StringType(), True),
            StructField("order_hours", StringType(), True),
            StructField("days_since_prior_orders", StringType(), True),
            StructField("order_numbers", StringType(), True),
            StructField("eval_set", StringType(), True),
            StructField("product_id", IntegerType(), False),
            StructField("aisle_id", IntegerType(), False),
            StructField("department_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("position_in_order_history", StringType(), True),
        ]
    )
    expected_reorders = spark.createDataFrame(
        [
            Row(
                label=0,
                is_ordered_history="1 1 0",
                history_order_size="3 2 4",
                history_reorder_size="0 0 1",
                **common_cols_1,
                **common_cols_3,
            ),
            Row(
                label=-1,
                is_ordered_history="0 0 0",
                history_order_size="3 2 5",
                history_reorder_size="1 1 3",
                **common_cols_2,
                **common_cols_3,
            ),
        ],
        schema=expected_schema,
    )
    expected_reorders = expected_reorders.select(SELECTED_COLUMNS)

    assert_spark_df_equal(actual_reorders, expected_reorders, ["user_id"])
