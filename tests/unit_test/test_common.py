import pytest
from src.common.io import read_csv, read_parquet, write_parquet
from src.common.cleaning import fillna_and_cast
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType
)


def test_read_csv(spark, tmp_path):
    csv_path = tmp_path / 'test.csv'

    csv_path.write_text(
        'id,name\n'
        '5,Alice\n'
        '11,Bob\n'
        '29,Tony\n'
    )

    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('name', StringType(), False)
    ])

    df = read_csv(
        path=csv_path,
        spark=spark,
        schema=schema
    )

    assert df.count() == 3
    assert df.columns == ['id', 'name']
    assert dict(df.dtypes)['id'] == 'int' and dict(df.dtypes)['name'] == 'string'


def test_write_parquet(spark, tmp_path):
    df = spark.createDataFrame(
        [
            (5, 'Alice'),
            (11, 'Bob'),
            (29, 'Tony')
        ],
        ["id", "name"],
    )

    output_path = tmp_path / 'parquet'

    write_parquet(output_path, df)

    loaded_df = spark.read.parquet(str(output_path))

    assert loaded_df.count() == 3
    assert loaded_df.orderBy('id').collect() == df.orderBy('id').collect()


def test_read_parquet(spark, tmp_path):
    df = spark.createDataFrame(
        [
            (5, 'Alice'),
            (11, 'Bob'),
            (29, 'Tony')
        ],
        ['id', 'name'],
    )

    output_path = tmp_path / 'parquet'

    df.write.parquet(str(output_path))

    readed_df = read_parquet(output_path, spark)

    assert readed_df.count() == 3
    assert readed_df.orderBy('id').collect() == df.orderBy('id').collect()


def test_read_missing_file(spark, tmp_path):
    schema = StructType([
        StructField('id', IntegerType(), False)
    ])

    path = tmp_path / 'missing_df.csv'

    with pytest.raises(Exception):
        read_csv(path, spark, schema).count()


def test_fill_nan_and_cast(spark):
    df = spark.createDataFrame(
        [
            (5, '10', 'Alice'),
            (11, None, 'Bob'),
            (29, '20', None)
        ],
        schema=StructType([
            StructField('id', IntegerType(), False),
            StructField('value', StringType(), True),
            StructField('name', StringType(), True)
        ])
    )

    for c, v, s in zip(['value', 'name'], ['100', 'Unknown'], ['int', 'string']):
        df = fillna_and_cast(
            df=df,
            column=c,
            fillvalue=v,
            dtype=s
        )

    rows = df.orderBy('id').collect()

    assert rows[0]['id'] == 5
    assert rows[1]['value'] == 100
    assert rows[2]['name'] == 'Unknown'
    assert dict(df.dtypes)['value'] == 'int' and dict(df.dtypes)['name'] == 'string'
