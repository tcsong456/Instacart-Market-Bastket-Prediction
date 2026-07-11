import argparse
from src.common.utils import gcs_join
from src.common.io import read_parquet
from src.common.spark import create_spark_session
from pyspark.sql import functions as F, DataFrame, Window


def build_word_idx(df: DataFrame, min_word_freq: int = 5):
    word_counts = (
        df.select(
            F.explode(F.split(F.trim(F.lower(F.col("product_name"))), r"\s+")).alias(
                "word"
            )
        )
        .filter(F.col("word") != "")
        .groupBy("word")
        .agg(F.count("*").alias("word_count"))
    )

    frquent_window = Window.orderBy("word_count")
    frquent_words = (
        word_counts.filter(F.col("word_count") >= min_word_freq)
        .withColumn("word_idx", F.row_number().over(frquent_window).cast("int"))
        .select("word", "word_idx")
    )

    rare_words = (
        word_counts.filter(F.col("word_count") < min_word_freq)
        .withColumn("word_idx", F.lit(0).cast("int").alias("word_idx"))
        .select("word", "word_idx")
    )
    words = frquent_words.unionByName(rare_words)

    return words


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-word-freq", type=int, default=5)
    parser.add_argument("--input-dir", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("product_training_data")
    products = read_parquet(gcs_join(args.input_dir, "products"), spark)
    df = build_word_idx(products, args.min_word_freq)
    df.show(20, truncate=False)
