import logging
import argparse
from pyspark.sql import SparkSession, DataFrame
from src.common.spark import create_spark_session
from src.common.io import read_parquet, write_parquet
from src.common.cleaning import fillna_and_cast
from src.common.utils import gcs_join, partition_logging


logger = logging.getLogger(__name__)


NULL_COLS = ["days_since_prior_order"]


def load_parquet_data(
    path_dir: str, spark: SparkSession
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    orders = read_parquet(path=gcs_join(path_dir, "orders"), spark=spark)

    products = read_parquet(path=gcs_join(path_dir, "products"), spark=spark)

    orders_prior = read_parquet(
        path=gcs_join(path_dir, "order_products__prior"), spark=spark
    )

    orders_train = read_parquet(
        path=gcs_join(path_dir, "order_products__train"), spark=spark
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
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--log-partition", action="store_true")
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


def build_order_products(
    spark: SparkSession,
    input_dir: str,
    output_dir: str,
    debug: bool = False,
    log_partition: bool = False,
) -> None:
    """
    Args:
        spark: Active Spark session
        path: The directory to download raw instacart datasets
        debug: Whether to display metadata about the contructed datasets
        log_partition: Whether to display spark partition infomation
    Returns:
        A dataframe that contains full information for per user-order including
        features such as all products inside each order well their purchase order,
        day and hour of purchase, etc
    """

    logging.basicConfig(level=logging.INFO)

    orders, products, order_prior, order_train = load_parquet_data(
        spark=spark, path_dir=input_dir
    )

    order_products = orders_prior_train_join(order_prior, order_train)
    df = full_join(order_products, orders, products)
    if debug:
        validate_join_counts(df, order_products)
        log_info(order_products, order_prior, order_train, df)

    df = fill_nans(df, NULL_COLS)

    if log_partition:
        partition_logging(logger, df)

    write_parquet(path=gcs_join(output_dir, "order_products"), df=df)


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("instacart-basket")
    build_order_products(
        spark=spark,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        debug=args.debug,
        log_partition=args.log_partition,
    )
    spark.stop()
