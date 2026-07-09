import logging
import argparse
from pyspark.sql import SparkSession
from src.common.utils import (
    gcs_join,
    ORDER_PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    AISLES_SCHEMA,
    DEPARTMENTS_SCHEMA,
)
from src.common.io import read_csv, write_parquet
from src.common.spark import create_spark_session


logger = logging.getLogger(__name__)


SCHEMAS = {
    "orders.csv": ORDERS_SCHEMA,
    "products.csv": PRODUCTS_SCHEMA,
    "order_products__prior.csv": ORDER_PRODUCTS_SCHEMA,
    "order_products__train.csv": ORDER_PRODUCTS_SCHEMA,
    "aisles.csv": AISLES_SCHEMA,
    "departments.csv": DEPARTMENTS_SCHEMA,
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def build_bronze_datasets(spark: SparkSession, input_dir: str, output_dir: str) -> None:
    logging.basicConfig(level=logging.INFO)

    for filename, schema in SCHEMAS.items():
        csv_path = gcs_join(input_dir, filename)
        csv_file = read_csv(csv_path, spark, schema)
        table_name = filename.removesuffix(".csv")
        logger.info(f"{table_name} count is {csv_file.count()}")
        parquet_path = gcs_join(output_dir, table_name)
        write_parquet(parquet_path, csv_file)


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("bronze-dataset")
    build_bronze_datasets(
        spark=spark, input_dir=args.input_dir, output_dir=args.output_dir
    )
    spark.stop()
