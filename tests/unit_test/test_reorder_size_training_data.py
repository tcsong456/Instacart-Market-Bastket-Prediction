from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.ingestion.prepare_reorder_size_training_data import (
    transform_reorder_size_training_data,
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
                order_sizes=[0],
                reorder_sizes=[0],
                label=0,
            ),
            Row(
                user_id=3,
                reorders_prev=[],
                reorders_next=[],
                order_sizes=[0],
                reorder_sizes=[0],
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
        ]
    )
    print(actual_df.printSchema())
    assert_spark_df_equal(actual_df, expected_df, ["user_id"])
