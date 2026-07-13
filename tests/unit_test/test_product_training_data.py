from pyspark.sql import Row
from tests.helper import assert_spark_df_equal
from src.ingestion.prepare_product_training_data import (
    build_word_idx,
    encode_product_names,
)
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
)


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
            Row(word="cheeze", word_idx=2),
            Row(word="garlic", word_idx=1),
        ],
        schema=expected_schma,
    )

    assert_spark_df_equal(actual_df, expected_df, ["word_idx", "word"])


def test_encode_product_names(spark):
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
            Row(product_id=28, product_name=None),
            Row(product_id=35, product_name=""),
        ]
    )

    word_index = spark.createDataFrame(
        [
            Row(word="banana", word_idx=1),
            Row(word="organic", word_idx=2),
            Row(word="&", word_idx=3),
            Row(word="chicken", word_idx=4),
            Row(word="milk", word_idx=5),
            Row(word="brest", word_idx=6),
            Row(word="brown", word_idx=7),
            Row(word="cage", word_idx=8),
            Row(word="eggs", word_idx=9),
            Row(word="large", word_idx=10),
            Row(word="liver", word_idx=11),
            Row(word="oil", word_idx=12),
            Row(word="olive", word_idx=13),
            Row(word="whole", word_idx=14),
            Row(word="with", word_idx=15),
            Row(word="yogurt", word_idx=16),
        ]
    )

    actual_df = encode_product_names(products, word_index)

    expected_schema = StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("product_name_encoded", StringType(), False),
        ]
    )
    expected_df = spark.createDataFrame(
        [
            Row(product_id=7, product_name_encoded="4 6"),
            Row(product_id=19, product_name_encoded="4 11"),
            Row(product_id=15, product_name_encoded="1 5 3 2 8 10 7 9"),
            Row(product_id=30, product_name_encoded="2 1 15 13 12"),
            Row(product_id=22, product_name_encoded="1 16 3 2 14 5"),
            Row(product_id=28, product_name_encoded="0"),
            Row(product_id=35, product_name_encoded="0"),
        ],
        schema=expected_schema,
    )

    assert_spark_df_equal(actual_df, expected_df, ["product_id"])
