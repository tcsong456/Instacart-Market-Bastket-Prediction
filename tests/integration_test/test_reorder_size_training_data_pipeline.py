from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.common.io import read_parquet, write_parquet
from src.ingestion.prepare_reorder_size_training_data import build_reorder_size_data
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def test_build_reorder_size_data(spark, tmp_path):
    user_data = spark.createDataFrame(
        [
            Row(
                user_id=1,
                reorders="0_1_0_0_1_1_1",
                order_dows="5",
                order_hours="20",
                days_since_prior_orders="33",
                order_numbers="4",
                eval_set="train",
            ),
            Row(
                user_id=2,
                reorders="",
                order_dows="1 3 0 4",
                order_hours="11 7 2 21",
                days_since_prior_orders="28 19 18 7",
                order_numbers="2 3 4 5",
                eval_set="train",
            ),
            Row(
                user_id=3,
                reorders=None,
                order_dows="2 5 1 2 3 5 1",
                order_hours="22 19 14 22 5 1 19",
                days_since_prior_orders="16 10 21 24 39 9 38",
                order_numbers="2 3 4 5 6 7 8",
                eval_set="test",
            ),
            Row(
                user_id=4,
                reorders="1",
                order_dows="0 5 0 2 1 0 5 3 4 5",
                order_hours="4 21 21 5 12 6 11 22 15 10",
                days_since_prior_orders="34 20 35 9 31 37 31 10 2 13",
                order_numbers="2 3 4 5 6 7 8 9 10 11",
                eval_set="test",
            ),
            Row(
                user_id=5,
                reorders="1_1_0_1 0_0_0_0_0 1_0_1",
                order_dows="0 6 1",
                order_hours="11 22 23",
                days_since_prior_orders="-1 14 34",
                order_numbers="1 2 3",
                eval_set="train",
            ),
            Row(
                user_id=6,
                reorders="0 1_1_0",
                order_dows="2 4",
                order_hours="20 10",
                days_since_prior_orders="-1 18",
                order_numbers="1 2",
                eval_set="prior",
            ),
        ]
    )
    user_data_path = tmp_path / "user_data"
    write_parquet(user_data_path, user_data)

    build_reorder_size_data(spark, tmp_path, tmp_path, 7)
    actual_df = read_parquet(tmp_path / "reorder_size_training_data", spark)

    expected_schema = StructType(
        [
            StructField("user_id", LongType(), nullable=True),
            StructField(
                "order_dows",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "order_hours",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "days_since_prior_orders",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "order_numbers",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField("eval_set", StringType(), nullable=True),
            StructField(
                "order_sizes",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "reorder_sizes",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField("label", IntegerType(), nullable=True),
            StructField("history_length", IntegerType(), nullable=True),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                order_dows=[5, 0, 0, 0, 0, 0, 0],
                order_hours=[20, 0, 0, 0, 0, 0, 0],
                days_since_prior_orders=[33, 0, 0, 0, 0, 0, 0],
                order_numbers=[4, 0, 0, 0, 0, 0, 0],
                eval_set="train",
                order_sizes=[7, 0, 0, 0, 0, 0, 0],
                reorder_sizes=[4, 0, 0, 0, 0, 0, 0],
                label=0,
                history_length=1,
            ),
            Row(
                user_id=2,
                order_dows=[1, 3, 0, 4, 0, 0, 0],
                order_hours=[11, 7, 2, 21, 0, 0, 0],
                days_since_prior_orders=[28, 19, 18, 7, 0, 0, 0],
                order_numbers=[2, 3, 4, 5, 0, 0, 0],
                eval_set="train",
                order_sizes=[0, 0, 0, 0, 0, 0, 0],
                reorder_sizes=[0, 0, 0, 0, 0, 0, 0],
                label=0,
                history_length=0,
            ),
            Row(
                user_id=3,
                order_dows=[2, 5, 1, 2, 3, 5, 1],
                order_hours=[22, 19, 14, 22, 5, 1, 19],
                days_since_prior_orders=[16, 10, 21, 24, 39, 9, 38],
                order_numbers=[2, 3, 4, 5, 6, 7, 8],
                eval_set="test",
                order_sizes=[0, 0, 0, 0, 0, 0, 0],
                reorder_sizes=[0, 0, 0, 0, 0, 0, 0],
                label=0,
                history_length=0,
            ),
            Row(
                user_id=4,
                order_dows=[0, 5, 0, 2, 1, 0, 5],
                order_hours=[4, 21, 21, 5, 12, 6, 11],
                days_since_prior_orders=[34, 20, 35, 9, 31, 37, 31],
                order_numbers=[2, 3, 4, 5, 6, 7, 8],
                eval_set="test",
                order_sizes=[1, 0, 0, 0, 0, 0, 0],
                reorder_sizes=[1, 0, 0, 0, 0, 0, 0],
                label=0,
                history_length=1,
            ),
            Row(
                user_id=5,
                order_dows=[0, 6, 1, 0, 0, 0, 0],
                order_hours=[11, 22, 23, 0, 0, 0, 0],
                days_since_prior_orders=[-1, 14, 34, 0, 0, 0, 0],
                order_numbers=[1, 2, 3, 0, 0, 0, 0],
                eval_set="train",
                order_sizes=[4, 5, 0, 0, 0, 0, 0],
                reorder_sizes=[3, 0, 0, 0, 0, 0, 0],
                label=2,
                history_length=2,
            ),
            Row(
                user_id=6,
                order_dows=[2, 4, 0, 0, 0, 0, 0],
                order_hours=[20, 10, 0, 0, 0, 0, 0],
                days_since_prior_orders=[-1, 18, 0, 0, 0, 0, 0],
                order_numbers=[1, 2, 0, 0, 0, 0, 0],
                eval_set="prior",
                order_sizes=[1, 0, 0, 0, 0, 0, 0],
                reorder_sizes=[0, 0, 0, 0, 0, 0, 0],
                label=2,
                history_length=1,
            ),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(actual_df, expected_df, ["user_id"])
