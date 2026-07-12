import argparse
from src.common.utils import gcs_join
from src.common.io import read_parquet
from src.common.spark import create_spark_session
from pyspark.sql import functions as F, DataFrame, Window


def build_word_idx(products: DataFrame, min_word_freq: int = 5) -> DataFrame:
    """
    Build a word-to-index lookup table from product names.
    Words appearing fewer than `min_word_frequency` times receive index 0.
    Frequent words receive IDs starting from 1.

    Returns:
        DataFrame with columns:
            word: string
            word_idx: integer
    """

    word_counts = (
        products.select(
            F.explode(F.split(F.trim(F.lower(F.col("product_name"))), r"\s+")).alias(
                "word"
            )
        )
        .filter(F.col("word") != "")
        .groupBy("word")
        .agg(F.count("*").alias("word_count"))
    )

    frquent_window = Window.orderBy(F.col("word_count").desc(), F.col("word").asc())
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
    word_index = frquent_words.unionByName(rare_words)
    # print(word_index.count())

    return word_index


def encode_product_names(products: DataFrame, word_index: DataFrame) -> DataFrame:
    """
    Encode each product name while preserving the original word order.

    Returns:
        DataFrame with columns:
            product_id
            product_name_encoded: array<int>
    """

    clean_products = products.select(
        F.col("product_id").cast("int"),
        F.trim(F.lower(F.col("product_name"))).alias("cleaned_product_name"),
    )

    product_tokens = (
        clean_products.select(
            "product_id",
            F.posexplode(F.split("cleaned_product_name", r"\s+")).alias(
                "word_pos", "word"
            ),
        )
        .filter(F.col("word") != "")
        .join(word_index, how="left", on="word")
        .groupby("product_id")
        .agg(
            F.sort_array(F.collect_list(F.struct("word_pos", "word_idx"))).alias(
                "encoded_words"
            )
        )
        .select(
            "product_id",
            F.expr(
                """
                    array_join(
                        transform(
                            encoded_words,
                            x -> x.word_idx
                        ),
                    ' '
                    )
                """
            ).alias("product_name_encoded"),
        )
    )

    result = (
        clean_products.select("product_id")
        .join(product_tokens, how="left", on="product_id")
        .withColumn(
            "product_name_encoded",
            F.coalesce(F.col("product_name_encoded"), F.lit("0")),
        )
    )

    return result


def pad_array(
    array_column: F.Column,
    max_length: int,
) -> tuple[F.Column, F.Column]:
    """
    Truncate and right-pad an integer array with zeros.

    Returns:
        padded_array
        original_length_after_truncation
    """

    truncated_array = F.slice(array_column, 1, max_length)
    seq_length = F.size(truncated_array).cast("int")
    padding_len = max_length - seq_length
    padded_array = F.concat(
        truncated_array, F.array_repeat(F.lit(0).cast("int"), padding_len)
    )

    return padded_array, seq_length


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-word-freq", type=int, default=5)
    parser.add_argument("--input-dir", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("product_training_data")
    products = read_parquet(gcs_join(args.input_dir, "products"), spark)
    word_index = build_word_idx(products, args.min_word_freq)
    df = encode_product_names(products, word_index)
    df.show(20, truncate=False)
