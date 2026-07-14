import argparse
from pyspark.sql import DataFrame, functions as F, SparkSession
from src.common.spark import create_spark_session
from src.common.io import read_parquet, write_parquet
from src.common.utils import parse_string_sequence, pad_array, gcs_join


PARSE_COLUMNS = [
    "is_ordered_history",
    "position_in_order",
    "num_products_from_aisle",
    "aisle_history_size",
    "order_dows",
    "order_hours",
    "days_since_prior_orders",
    "order_numbers",
]


def parse_aisle_seq_data(df: DataFrame, max_padded_length: int = 30) -> DataFrame:
    for colname in PARSE_COLUMNS:
        df = df.withColumn(colname, parse_string_sequence(F.trim(F.col(colname))))

        padded_array, padded_length = pad_array(F.col(colname), max_padded_length)

        if colname == "is_ordered_history":
            df = df.withColumn("history_length", padded_length)

        df = df.withColumn(colname, padded_array)

    return df


def build_aisle_seq_data(
    spark: SparkSession, input_dir: str, output_dir: str, max_padded_length: int = 30
) -> None:
    aisle_history_data = read_parquet(gcs_join(input_dir, "aisle_historyy_data"), spark)
    df = parse_aisle_seq_data(aisle_history_data, max_padded_length)
    write_parquet(gcs_join(output_dir, "aisle_training_data"), df)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-padded-length", type=int, default=30)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("aisle_seq_data")
    build_aisle_seq_data(
        spark=spark,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        max_padded_length=args.max_padded_length,
    )
    spark.stop()
