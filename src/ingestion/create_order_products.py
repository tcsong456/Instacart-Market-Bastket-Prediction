import logging
import argparse
from pyspark.sql import SparkSession, DataFrame
from src.common.spark import create_spark_session
from src.common.io import read_csv, write_parquet
from src.common.cleaning import fillna_and_cast
from src.common.utils import gcs_join
from pyspark.sql.types import (
    StructField,
    StructType,
    ByteType,
    ShortType,
    IntegerType,
    StringType,
    DoubleType,
)


logger = logging.getLogger(__name__)


ORDER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", ShortType(), False),
        StructField("reordered", ByteType(), False),
    ]
)


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("eval_set", StringType(), False),
        StructField("order_number", ByteType(), False),
        StructField("order_dow", ByteType(), False),
        StructField("order_hour_of_day", ByteType(), False),
        StructField("days_since_prior_order", DoubleType(), True),
    ]
)


PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("aisle_id", ShortType(), False),
        StructField("department_id", ByteType(), False),
    ]
)


NULL_COLS = ["days_since_prior_order"]


def load_csv_data(
    path_dir: str, spark: SparkSession
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    orders = read_csv(
        path=gcs_join(path_dir, "orders.csv"), spark=spark, schema=ORDERS_SCHEMA
    )

    products = read_csv(
        path=gcs_join(path_dir, "products.csv"), spark=spark, schema=PRODUCTS_SCHEMA
    )

    orders_prior = read_csv(
        path=gcs_join(path_dir, "order_products__prior.csv"),
        spark=spark,
        schema=ORDER_PRODUCTS_SCHEMA,
    )

    orders_train = read_csv(
        path=gcs_join(path_dir, "order_products__train.csv"),
        spark=spark,
        schema=ORDER_PRODUCTS_SCHEMA,
    )
    return orders, products, orders_prior, orders_train


def log_info(
    order_products: DataFrame,
    order_prior: DataFrame,
    order_train: DataFrame,
    df: DataFrame,
) -> None:
    logger.info("order_products count is: %s", order_products.count())
    logger.info("order_prior count is %s", order_prior.count())
    logger.info("order_train count is %s", order_train.count())
    logger.info("df count is %s", df.count())
    logger.info("df partition is %s", df.rdd.getNumPartitions())


def orders_prior_train_join(
    orders_prior: DataFrame, orders_train: DataFrame
) -> DataFrame:
    return orders_prior.unionByName(orders_train)


def full_join(
    order_products: DataFrame, orders: DataFrame, products: DataFrame
) -> DataFrame:
    return order_products.join(orders, how="inner", on="order_id").join(
        products, how="inner", on="product_id"
    )


def fill_nans(df: DataFrame, null_columns: list[str]) -> DataFrame:
    for c in null_columns:
        df = fillna_and_cast(df=df, column=c, fillvalue=-1, dtype="int")
    return df


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def validate_join_counts(joined_df: DataFrame, order_products: DataFrame) -> None:
    if joined_df.count() != order_products.count():
        raise ValueError(
            (
                f"Joined row count mismatch with original row count: "
                f"joined_count={joined_df.count()}, "
                f"raw_count={order_products.count()}"
            )
        )


def build_order_products(spark: SparkSession, path: str, debug: bool = False) -> None:
    logging.basicConfig(level=logging.INFO)

    orders, products, order_prior, order_train = load_csv_data(
        spark=spark, path_dir=path
    )

    order_products = orders_prior_train_join(order_prior, order_train)
    df = full_join(order_products, orders, products)
    validate_join_counts(df, order_products)

    if debug:
        log_info(order_products, order_prior, order_train, df)

    df = fill_nans(df, NULL_COLS)

    write_parquet(path=gcs_join(path, "order_products"), df=df)


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("instacart-basket")
    build_order_products(spark=spark, path=args.path, debug=args.debug)
