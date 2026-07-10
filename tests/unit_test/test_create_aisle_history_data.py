from pyspark.sql import Row
from src.common.io import read_parquet
from tests.helper import assert_spark_df_equal
from src.ingestion.create_aisle_history_data import parse_seq
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
        "aislle_raw",
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


# def test_build_aisle_history_data(fake_parse_seq_data):
#     df = fake_parse_seq_data
#     df = build_aisle_history_data(df)

#     row = df.filter('user_id==10').first()
#     assert row['aisle_history_size'] == "3 2 3"
