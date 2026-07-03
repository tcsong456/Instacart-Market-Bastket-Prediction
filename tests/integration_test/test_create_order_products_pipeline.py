from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.ingestion.create_order_products import (
    load_csv_data,
    orders_prior_train_join,
    full_join,
    fill_nans,
    validate_join_counts,
    NULL_COLS,
)


def test_create_order_products(spark: SparkSession, tiny_fake_testset: Path) -> None:
    orders, products, order_prior, order_train = load_csv_data(
        spark=spark, path_dir=tiny_fake_testset
    )

    order_products = orders_prior_train_join(order_prior, order_train)
    df = full_join(order_products, orders, products)
    validate_join_counts(df, order_products)

    df = fill_nans(df, NULL_COLS)

    all_columns = [
        "order_id",
        "product_id",
        "add_to_cart_order",
        "reordered",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "product_name",
        "aisle_id",
        "department_id",
    ]

    row = df.filter(col("order_id") == 1).first()
    assert df.count() == 5
    assert df.filter(col("days_since_prior_order").isNull()).count() == 0
    assert dict(df.dtypes)["days_since_prior_order"] == "int"
    assert set(df.columns) == set(all_columns)
    assert row["days_since_prior_order"] == -1
    assert row["product_id"] == 101
    assert row["product_name"] == "a"
    assert row["aisle_id"] == 1
    assert row["department_id"] == 10
    assert row["user_id"] == 10
