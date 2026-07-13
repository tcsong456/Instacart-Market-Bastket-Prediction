from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.ingestion.prepare_reorder_size_training_data import (
    transform_reorder_size_training_data,
    pad_column_arrays,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)


def test_transform_reorder_size_training_data(spark):
    user_data = spark.createDataFrame(
        [
            Row(user_id=1, reorders="0_1_0_0_1_1_1"),
            Row(user_id=2, reorders=""),
            Row(user_id=3, reorders=None),
            Row(user_id=4, reorders="1"),
            Row(user_id=5, reorders="1_1_0_1 0_0_0_0_0 1_0_1"),
            Row(user_id=6, reorders="0 1_1_0"),
        ]
    )

    actual_df = transform_reorder_size_training_data(user_data)
    actual_df = actual_df.select(
        "user_id",
        "reorders_prev",
        "reorders_next",
        "order_sizes",
        "reorder_sizes",
        "label",
    )

    expected_schema = StructType(
        [
            StructField("user_id", LongType(), nullable=True),
            StructField(
                "reorders_prev",
                ArrayType(
                    ArrayType(
                        IntegerType(),
                        containsNull=True,
                    ),
                    containsNull=True,
                ),
                nullable=True,
            ),
            StructField(
                "reorders_next",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "order_sizes",
                ArrayType(IntegerType(), containsNull=False),
                nullable=True,
            ),
            StructField(
                "reorder_sizes",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField("label", IntegerType(), nullable=True),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                reorders_prev=[[0, 1, 0, 0, 1, 1, 1]],
                reorders_next=[],
                order_sizes=[7],
                reorder_sizes=[4],
                label=0,
            ),
            Row(
                user_id=2,
                reorders_prev=[],
                reorders_next=[],
                order_sizes=[],
                reorder_sizes=[],
                label=0,
            ),
            Row(
                user_id=3,
                reorders_prev=[],
                reorders_next=[],
                order_sizes=[],
                reorder_sizes=[],
                label=0,
            ),
            Row(
                user_id=4,
                reorders_prev=[[1]],
                reorders_next=[],
                order_sizes=[1],
                reorder_sizes=[1],
                label=0,
            ),
            Row(
                user_id=5,
                reorders_prev=[[1, 1, 0, 1], [0, 0, 0, 0, 0]],
                reorders_next=[1, 0, 1],
                order_sizes=[4, 5],
                reorder_sizes=[3, 0],
                label=2,
            ),
            Row(
                user_id=6,
                reorders_prev=[[0]],
                reorders_next=[1, 1, 0],
                order_sizes=[1],
                reorder_sizes=[0],
                label=2,
            ),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(actual_df, expected_df, ["user_id"])


def test_pad_column_arrays(spark):
    df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                order_numbers="1 2 3 4 5",
                order_dows="",
                order_hours="10 20 5 0 9",
                days_since_prior_orders="-1 10 5 20 30",
                order_sizes=[5, 10, 21, 13],
                reorder_sizes=[3, 4, 9, 5],
            ),
            Row(
                user_id=2,
                order_numbers="1 2 3 4 5 6 7",
                order_dows="6 6 3 2 2 5 0",
                order_hours=None,
                days_since_prior_orders="-1 30 31 20 7 11 18",
                order_sizes=[4, 3, 7, 10, 1, 9],
                reorder_sizes=[0, 3, 1, 5, 1, 6],
            ),
            Row(
                user_id=3,
                order_numbers="1 2 3 4 5 6 7 8 9",
                order_dows="5 1 2 3 0 2 1 6 4",
                order_hours="23 1 17 14 20 20 8 19 12",
                days_since_prior_orders="-1 14 15 22 13 28 10 16 25",
                order_sizes=[20, 23, 24, 23, 14, 9, 11, 8],
                reorder_sizes=[],
            ),
        ]
    )

    actual_df = pad_column_arrays(df, 7)
    actual_df = actual_df.select(
        "user_id",
        "order_dows",
        "order_hours",
        "days_since_prior_orders",
        "order_numbers",
        "order_sizes",
        "reorder_sizes",
        "history_length",
    )

    expected_schema = StructType(
        [
            StructField("user_id", LongType(), nullable=True),
            StructField(
                "order_numbers",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "order_dows", ArrayType(IntegerType(), containsNull=True), nullable=True
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
                "order_sizes", ArrayType(LongType(), containsNull=True), nullable=True
            ),
            StructField(
                "reorder_sizes", ArrayType(LongType(), containsNull=True), nullable=True
            ),
            StructField("history_length", IntegerType(), nullable=False),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                order_numbers=[1, 2, 3, 4, 5, 0, 0],
                order_dows=[0, 0, 0, 0, 0, 0, 0],
                order_hours=[10, 20, 5, 0, 9, 0, 0],
                days_since_prior_orders=[-1, 10, 5, 20, 30, 0, 0],
                order_sizes=[5, 10, 21, 13, 0, 0, 0],
                reorder_sizes=[3, 4, 9, 5, 0, 0, 0],
                history_length=4,
            ),
            Row(
                user_id=2,
                order_numbers=[1, 2, 3, 4, 5, 6, 7],
                order_dows=[6, 6, 3, 2, 2, 5, 0],
                order_hours=[0, 0, 0, 0, 0, 0, 0],
                days_since_prior_orders=[-1, 30, 31, 20, 7, 11, 18],
                order_sizes=[4, 3, 7, 10, 1, 9, 0],
                reorder_sizes=[0, 3, 1, 5, 1, 6, 0],
                history_length=6,
            ),
            Row(
                user_id=3,
                order_numbers=[1, 2, 3, 4, 5, 6, 7],
                order_dows=[5, 1, 2, 3, 0, 2, 1],
                order_hours=[23, 1, 17, 14, 20, 20, 8],
                days_since_prior_orders=[-1, 14, 15, 22, 13, 28, 10],
                order_sizes=[20, 23, 24, 23, 14, 9, 11],
                reorder_sizes=[0, 0, 0, 0, 0, 0, 0],
                history_length=0,
            ),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(actual_df, expected_df, ["user_id"])
