import pytest
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)
from src.ingestion.create_order_products import (
    fill_nans,
    load_csv_data,
    validate_join_counts,
    ORDER_PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA
)
from tests.helper import assert_schema_matches


def test_validate_join_counts_passes_when_match(spark):
    df1 = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ['order_id']
    )
    df2 = spark.createDataFrame(
        [(10,), (20,), (30,)],
        ['product_id']
    )

    validate_join_counts(df1, df2)


def test_validate_join_counts_fails_when_mismatch(spark):
    df1 = spark.createDataFrame(
        [(1,), (2,)],
        ['order_id']
    )
    df2 = spark.createDataFrame(
        [(10,), (20,), (30,)],
        ['product_id']
    )

    with pytest.raises(ValueError, match='Joined row count mismatch'):
        validate_join_counts(df1, df2)


def test_load_csv_data(spark, tiny_fake_testset):
    orders, products, order_prior, order_train = load_csv_data(
        path_dir=tiny_fake_testset,
        spark=spark
    )

    assert orders.count() == 6
    assert products.count() == 5
    assert order_prior.count() == 3
    assert order_train.count() == 2

    assert_schema_matches(orders, ORDERS_SCHEMA)
    assert_schema_matches(products, PRODUCTS_SCHEMA)
    assert_schema_matches(order_prior, ORDER_PRODUCTS_SCHEMA)
    assert_schema_matches(order_train, ORDER_PRODUCTS_SCHEMA)


def test_fill_nans(spark):
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('a', StringType(), True),
        StructField('b', IntegerType(), True)
    ])

    df = spark.createDataFrame(
        [
            (None, None, None)
        ],
        schema=schema
    )

    result = fill_nans(df, ['a', 'b'])

    row = result.first()

    assert row['id'] is None
    assert row['a'] == -1
    assert row['b'] == -1
    assert dict(df.dtypes)['a'] == 'string'
    assert dict(result.dtypes)['a'] == 'int' and dict(result.dtypes)['b'] == 'int'
