from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.ingestion.prepare_aisle_training_data import (
    parse_aisle_seq_data,
    PARSE_COLUMNS,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)


def test_parse_aisle_seq_data(spark):
    df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                is_ordered_history="1 0 0 1 1 0 0",
                position_in_order="2 1 3 7 2 1 4",
                num_products_from_aisle="1 1 2 1 3 2 3",
                aisle_history_size="8 4 8 12 10 7 6",
                order_dows="3 5 5 2 4 6 1",
                order_hours="16 10 21 7 4 3 2",
                days_since_prior_orders="26 34 32 34 16 12 7",
                order_numbers="3 4 5 6 7 8 9",
            ),
            Row(
                user_id=2,
                is_ordered_history="0 1 1",
                position_in_order="0 1 4",
                num_products_from_aisle="0 1 2",
                aisle_history_size="3 5 9",
                order_dows="1 0 6",
                order_hours="10 16 4",
                days_since_prior_orders="-1 30 20",
                order_numbers="1 2 3",
            ),
            Row(
                user_id=3,
                is_ordered_history="0 0 1 0 1",
                position_in_order="0 0 2 0 6",
                num_products_from_aisle="0 0 2 0 1",
                aisle_history_size="5 5 9 3 11",
                order_dows="5 2 2 0 4",
                order_hours="20 0 23 19 8",
                days_since_prior_orders="-1 28 0 19 29",
                order_numbers="1 2 3 4 5",
            ),
            Row(
                user_id=4,
                is_ordered_history="   ",
                position_in_order="2",
                num_products_from_aisle="",
                aisle_history_size="10",
                order_dows=None,
                order_hours="14",
                days_since_prior_orders="-1",
                order_numbers="1",
            ),
        ]
    )

    wanted_columns = PARSE_COLUMNS + ["user_id", "history_length"]
    actual_df = parse_aisle_seq_data(df, 5)
    actual_df = actual_df.select(wanted_columns)

    expected_schema = StructType(
        [
            StructField("user_id", LongType(), nullable=True),
            StructField(
                "is_ordered_history",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "position_in_order",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "num_products_from_aisle",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
            StructField(
                "aisle_history_size",
                ArrayType(IntegerType(), containsNull=True),
                nullable=True,
            ),
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
            StructField("history_length", IntegerType(), nullable=False),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                is_ordered_history=[1, 0, 0, 1, 1],
                position_in_order=[2, 1, 3, 7, 2],
                num_products_from_aisle=[1, 1, 2, 1, 3],
                aisle_history_size=[8, 4, 8, 12, 10],
                order_dows=[3, 5, 5, 2, 4],
                order_hours=[16, 10, 21, 7, 4],
                days_since_prior_orders=[26, 34, 32, 34, 16],
                order_numbers=[3, 4, 5, 6, 7],
                history_length=5,
            ),
            Row(
                user_id=2,
                is_ordered_history=[0, 1, 1, 0, 0],
                position_in_order=[0, 1, 4, 0, 0],
                num_products_from_aisle=[0, 1, 2, 0, 0],
                aisle_history_size=[3, 5, 9, 0, 0],
                order_dows=[1, 0, 6, 0, 0],
                order_hours=[10, 16, 4, 0, 0],
                days_since_prior_orders=[-1, 30, 20, 0, 0],
                order_numbers=[1, 2, 3, 0, 0],
                history_length=3,
            ),
            Row(
                user_id=3,
                is_ordered_history=[0, 0, 1, 0, 1],
                position_in_order=[0, 0, 2, 0, 6],
                num_products_from_aisle=[0, 0, 2, 0, 1],
                aisle_history_size=[5, 5, 9, 3, 11],
                order_dows=[5, 2, 2, 0, 4],
                order_hours=[20, 0, 23, 19, 8],
                days_since_prior_orders=[-1, 28, 0, 19, 29],
                order_numbers=[1, 2, 3, 4, 5],
                history_length=5,
            ),
            Row(
                user_id=4,
                is_ordered_history=[0, 0, 0, 0, 0],
                position_in_order=[2, 0, 0, 0, 0],
                num_products_from_aisle=[0, 0, 0, 0, 0],
                aisle_history_size=[10, 0, 0, 0, 0],
                order_dows=[0, 0, 0, 0, 0],
                order_hours=[14, 0, 0, 0, 0],
                days_since_prior_orders=[-1, 0, 0, 0, 0],
                order_numbers=[1, 0, 0, 0, 0],
                history_length=0,
            ),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(actual_df, expected_df, ["user_id"])
