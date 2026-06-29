from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    StructType
)
from src.ingestion.create_order_products import (
    fill_nans,
    load_csv_data,
    ORDER_PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA
)


def test_load_csv_data(spark, tiny_fake_testset):
    orders, products, order_prior, order_train = load_csv_data(
        path_dir=tiny_fake_testset,
        spark=spark
    )

    assert orders.count() == 6
    assert products.count() == 5
    assert order_prior.count() == 3
    assert order_train.count() == 2

    assert orders.schema == ORDERS_SCHEMA
    assert products.schema == PRODUCTS_SCHEMA
    assert order_prior.schema == ORDER_PRODUCTS_SCHEMA
    assert order_train.schema == ORDER_PRODUCTS_SCHEMA


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
