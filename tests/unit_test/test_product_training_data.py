from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.ingestion.prepare_product_training_data import build_word_idx
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


def test_build_word_idx(spark):
    products = spark.createDataFrame(
        [
            Row(product_name="Garlic bread"),
            Row(product_name="garlic Cheeze"),
            Row(product_name="Strawberry cheeze"),
            Row(product_name=None),
            Row(product_name="    "),
            Row(product_name="  GARLIC "),
            Row(product_name=""),
        ]
    )

    actual_df = build_word_idx(products, 2)

    expected_schma = StructType(
        [
            StructField("word", StringType(), False),
            StructField("word_idx", IntegerType(), False),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(word="bread", word_idx=0),
            Row(word="strawberry", word_idx=0),
            Row(word="cheeze", word_idx=1),
            Row(word="garlic", word_idx=2),
        ],
        schema=expected_schma,
    )

    assert_spark_df_equal(actual_df, expected_df, ["word_idx", "word"])
