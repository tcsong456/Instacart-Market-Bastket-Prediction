from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.common.io import write_parquet, read_parquet
from src.ingestion.prepare_product_training_data import build_product_seq_data


def test_product_seq_data_pipeline(spark, tmp_path):
    products = spark.createDataFrame(
        [
            Row(product_id=7, product_name="Chicken BREST"),
            Row(product_id=19, product_name="chicken liver"),
            Row(
                product_id=15,
                product_name="banana milk & Organic cage LARGE brown eggs",
            ),
            Row(product_id=30, product_name="ORGANIC BANANA with Olive Oil"),
            Row(product_id=22, product_name="Banana yogurt & Organic Whole Milk"),
        ]
    )
    product_path = tmp_path / "products"
    write_parquet(product_path, products)

    product_history_data = spark.createDataFrame(
        [
            Row(
                user_id=1,
                product_id=15,
                is_ordered_history="1 1 0 0 1 0 0 0 0 1",
                position_in_order_history="1 5 0 0 8 0 0 0 0 4",
                history_order_size="3 3 10 7 13 21 5 5 10 9",
                history_reorder_size="2 3 4 5 10 11 1 2 3 8",
                product_name_encoded="1 5 3 2 8 10 7 9",
            ),
            Row(
                user_id=1,
                product_id=22,
                is_ordered_history="0 0 1 1 1 0",
                position_in_order_history="0 0 2 4 1 0",
                history_order_size="3 3 10 7 13 21",
                history_reorder_size="2 3 4 5 10 11",
            ),
            Row(
                user_id=1,
                product_id=31,
                is_ordered_history="0 0 0 0 0 1",
                position_in_order_history="0 0 0 0 0 10",
                history_order_size="3 3 10 7 13 21",
                history_reorder_size="0 3 4 5 10 11",
            ),
            Row(
                user_id=2,
                product_id=19,
                is_ordered_history="1 0 1 0",
                position_in_order_history="2 0 3 0",
                history_order_size="9 5 18 3",
                history_reorder_size="0 3 10 1",
            ),
            Row(
                user_id=2,
                product_id=7,
                is_ordered_history=None,
                position_in_order_history="    ",
                history_order_size="9 5 18 3",
                history_reorder_size="0 3 10 1",
            ),
            Row(
                user_id=3,
                product_id=30,
                is_ordered_history="1 0 0 1 0 0 0 0 0 1 1 0",
                position_in_order_history="1 0 0 2 0 0 0 0 0 11 1 0",
                history_order_size="",
                history_reorder_size=None,
            ),
        ]
    )
    write_parquet(tmp_path / "product_history_data", product_history_data)

    build_product_seq_data(
        spark=spark,
        raw_dir=tmp_path,
        input_dir=tmp_path,
        output_dir=tmp_path,
        min_word_freq=0,
        product_name_length=6,
        encode_length=10,
    )
    actual_df = read_parquet(tmp_path / "product_training_data", spark)
    actual_df = actual_df.select(
        "user_id",
        "product_id",
        "is_ordered_history",
        "position_in_order_history",
        "history_order_size",
        "history_reorder_size",
        "product_name_encoded",
        "history_length",
        "product_name_length",
    )

    expected_df = spark.createDataFrame(
        [
            Row(
                user_id=1,
                product_id=15,
                is_ordered_history=[1, 1, 0, 0, 1, 0, 0, 0, 0, 1],
                position_in_order_history=[1, 5, 0, 0, 8, 0, 0, 0, 0, 4],
                history_order_size=[3, 3, 10, 7, 13, 21, 5, 5, 10, 9],
                history_reorder_size=[2, 3, 4, 5, 10, 11, 1, 2, 3, 8],
                product_name_encoded=[1, 5, 3, 2, 8, 10],
                history_length=10,
                product_name_length=6,
            ),
            Row(
                user_id=1,
                product_id=22,
                is_ordered_history=[0, 0, 1, 1, 1, 0, 0, 0, 0, 0],
                position_in_order_history=[0, 0, 2, 4, 1, 0, 0, 0, 0, 0],
                history_order_size=[3, 3, 10, 7, 13, 21, 0, 0, 0, 0],
                history_reorder_size=[2, 3, 4, 5, 10, 11, 0, 0, 0, 0],
                product_name_encoded=[1, 16, 3, 2, 14, 5],
                history_length=6,
                product_name_length=6,
            ),
            Row(
                user_id=1,
                product_id=31,
                is_ordered_history=[0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
                position_in_order_history=[0, 0, 0, 0, 0, 10, 0, 0, 0, 0],
                history_order_size=[3, 3, 10, 7, 13, 21, 0, 0, 0, 0],
                history_reorder_size=[0, 3, 4, 5, 10, 11, 0, 0, 0, 0],
                product_name_encoded=[0, 0, 0, 0, 0, 0],
                history_length=6,
                product_name_length=0,
            ),
            Row(
                user_id=2,
                product_id=19,
                is_ordered_history=[1, 0, 1, 0, 0, 0, 0, 0, 0, 0],
                position_in_order_history=[2, 0, 3, 0, 0, 0, 0, 0, 0, 0],
                history_order_size=[9, 5, 18, 3, 0, 0, 0, 0, 0, 0],
                history_reorder_size=[0, 3, 10, 1, 0, 0, 0, 0, 0, 0],
                product_name_encoded=[4, 11, 0, 0, 0, 0],
                history_length=4,
                product_name_length=2,
            ),
            Row(
                user_id=2,
                product_id=7,
                is_ordered_history=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                position_in_order_history=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                history_order_size=[9, 5, 18, 3, 0, 0, 0, 0, 0, 0],
                history_reorder_size=[0, 3, 10, 1, 0, 0, 0, 0, 0, 0],
                product_name_encoded=[4, 6, 0, 0, 0, 0],
                history_length=0,
                product_name_length=2,
            ),
            Row(
                user_id=3,
                product_id=30,
                is_ordered_history=[1, 0, 0, 1, 0, 0, 0, 0, 0, 1],
                position_in_order_history=[1, 0, 0, 2, 0, 0, 0, 0, 0, 11],
                history_order_size=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                history_reorder_size=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                product_name_encoded=[2, 1, 15, 13, 12, 0],
                history_length=10,
                product_name_length=5,
            ),
        ]
    )

    actual_df.printSchema()
    assert_spark_df_equal(actual_df, expected_df, ["user_id", "product_id"])
