import pytest
from src.common.utils import parse_string_sequence, pad_array
from src.common.io import read_csv, read_parquet, write_parquet
from src.common.cleaning import fillna_and_cast
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    ArrayType,
)


def test_read_csv(spark, tmp_path):
    csv_path = tmp_path / "test.csv"

    csv_path.write_text("id,name\n5,Alice\n11,Bob\n29,Tony\n")

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
        ]
    )

    df = read_csv(path=csv_path, spark=spark, schema=schema)

    assert df.count() == 3
    assert df.columns == ["id", "name"]
    assert dict(df.dtypes)["id"] == "int" and dict(df.dtypes)["name"] == "string"


def test_write_parquet(spark, tmp_path):
    df = spark.createDataFrame(
        [(5, "Alice"), (11, "Bob"), (29, "Tony")],
        ["id", "name"],
    )

    output_path = tmp_path / "parquet"

    write_parquet(output_path, df)

    loaded_df = spark.read.parquet(str(output_path))

    assert loaded_df.count() == 3
    assert loaded_df.orderBy("id").collect() == df.orderBy("id").collect()


def test_read_parquet(spark, tmp_path):
    df = spark.createDataFrame(
        [(5, "Alice"), (11, "Bob"), (29, "Tony")],
        ["id", "name"],
    )

    output_path = tmp_path / "parquet"

    df.write.parquet(str(output_path))

    readed_df = read_parquet(output_path, spark)

    assert readed_df.count() == 3
    assert readed_df.orderBy("id").collect() == df.orderBy("id").collect()


def test_read_missing_file(spark, tmp_path):
    schema = StructType([StructField("id", IntegerType(), False)])

    path = tmp_path / "missing_df.csv"

    with pytest.raises(Exception):
        read_csv(path, spark, schema).count()


def test_fill_nan_and_cast(spark):
    df = spark.createDataFrame(
        [(5, "10", "Alice"), (11, None, "Bob"), (29, "20", None)],
        schema=StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", StringType(), True),
                StructField("name", StringType(), True),
            ]
        ),
    )

    for c, v, s in zip(["value", "name"], ["100", "Unknown"], ["int", "string"]):
        df = fillna_and_cast(df=df, column=c, fillvalue=v, dtype=s)

    rows = df.orderBy("id").collect()

    assert rows[0]["id"] == 5
    assert rows[1]["value"] == 100
    assert rows[2]["name"] == "Unknown"
    assert dict(df.dtypes)["value"] == "int" and dict(df.dtypes)["name"] == "string"


@pytest.mark.parametrize(
    ("input_array", "max_length", "expected_array", "expected_length"),
    [
        ([1, 2, 3], 5, [1, 2, 3, 0, 0], 3),
        ([1, 2, 3, 4, 5], 5, [1, 2, 3, 4, 5], 5),
        ([1, 2, 3, 4, 5, 6, 7], 5, [1, 2, 3, 4, 5], 5),
        ([], 5, [0, 0, 0, 0, 0], 0),
    ],
)
def test_pad_array(spark, input_array, max_length, expected_array, expected_length):
    schema = StructType([StructField("array", ArrayType(IntegerType(), False), False)])

    df = spark.createDataFrame([(input_array,)], schema=schema)

    padded_array, seq_len = pad_array(F.col("array"), max_length)
    actual_df = df.select(
        padded_array.alias("padded_array"), seq_len.alias("seq_len")
    ).first()

    assert actual_df["padded_array"] == expected_array
    assert actual_df["seq_len"] == expected_length


@pytest.mark.parametrize(
    ("input_string", "expected_array"),
    [
        ("1 2 3", [1, 2, 3]),
        ("10", [10]),
        ("  4   5  6 ", [4, 5, 6]),
        (None, []),
        ("", []),
        ("       ", []),
    ],
)
def test_parse_string_sequence(spark, input_string, expected_array):
    schema = StructType([StructField("sequence", StringType(), True)])

    df = spark.createDataFrame([(input_string,)], schema=schema)

    seq = parse_string_sequence(F.col("sequence")).alias("parsed_seq")
    actual_df = df.select(seq).first()

    assert actual_df["parsed_seq"] == expected_array
