from pyspark.sql import Row
from src.common.io import read_parquet
from src.common.utils import gcs_join
from tests.helper import assert_spark_df_equal
from src.ingestion.create_aisle_history_data import build_create_aisle_history_data
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType


def test_aisle_history_data_pipeline(
    spark, fake_user_data, fake_products_data, tmp_path
):
    output_dir = tmp_path / "curated"
    output_dir.mkdir(parents=True, exist_ok=True)
    build_create_aisle_history_data(
        spark=spark,
        input_dir=fake_user_data,
        raw_dir=fake_products_data,
        output_dir=str(output_dir),
    )
    actual_df = read_parquet(gcs_join(output_dir, "aisle_history_data"), spark)

    common_cols_10 = dict(
        aisle_history_size="3 2 3",
        order_dows="5 0 5 1",
        order_hours="23 1 17 16",
        days_since_prior_orders="-1 30 60 25",
        order_numbers="1 2 3 4",
        eval_set="train",
    )
    common_cols_20 = dict(
        aisle_history_size="1 1 3",
        order_dows="1 2 3 4",
        order_hours="8 13 8 0",
        days_since_prior_orders="10 19 22 6",
        order_numbers="3 4 5 6",
        eval_set="test",
    )
    expected_schmae = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("aisle_id", IntegerType(), True),
            StructField("department_id", LongType(), True),
            StructField("is_ordered_history", StringType(), True),
            StructField("position_in_order", StringType(), True),
            StructField("num_products_from_aisle", StringType(), True),
            StructField("aisle_history_size", StringType(), True),
            StructField("order_dows", StringType(), True),
            StructField("order_hours", StringType(), True),
            StructField("days_since_prior_orders", StringType(), True),
            StructField("order_numbers", StringType(), True),
            StructField("eval_set", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=10,
                aisle_id=5,
                department_id=10,
                is_ordered_history="1 0 1",
                position_in_order="1 0 1",
                num_products_from_aisle="1 0 2",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=10,
                department_id=20,
                is_ordered_history="1 1 1",
                position_in_order="2 1 2",
                num_products_from_aisle="1 1 1",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=15,
                department_id=30,
                is_ordered_history="1 0 1",
                position_in_order="3 0 3",
                num_products_from_aisle="1 0 1",
                **common_cols_10,
            ),
            Row(
                user_id=10,
                aisle_id=20,
                department_id=40,
                is_ordered_history="0 1 0",
                position_in_order="0 2 0",
                num_products_from_aisle="0 1 0",
                **common_cols_10,
            ),
            Row(
                user_id=20,
                aisle_id=5,
                department_id=10,
                is_ordered_history="1 0 1",
                position_in_order="1 0 3",
                num_products_from_aisle="2 0 1",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=15,
                department_id=30,
                is_ordered_history="0 1 1",
                position_in_order="0 1 1",
                num_products_from_aisle="0 1 2",
                **common_cols_20,
            ),
            Row(
                user_id=20,
                aisle_id=20,
                department_id=40,
                is_ordered_history="0 0 1",
                position_in_order="0 0 2",
                num_products_from_aisle="0 0 2",
                **common_cols_20,
            ),
        ],
        schema=expected_schmae,
    )

    assert_spark_df_equal(actual_df, expected_df, ["user_id", "aisle_id"])
