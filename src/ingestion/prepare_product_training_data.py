import argparse
from src.common.utils import gcs_join, pad_array, parse_string_sequence
from src.common.io import read_parquet, write_parquet
from src.common.spark import create_spark_session
from pyspark.sql import functions as F, DataFrame, Window, SparkSession


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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-word-freq", type=int, default=5)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--product-name-length", type=int, default=50)
    parser.add_argument("--encode-length", type=int, default=50)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--raw-dir", required=True)

    return parser.parse_args()


def build_product_seq_data(
    spark: SparkSession,
    raw_dir: str,
    input_dir: str,
    output_dir: str,
    min_word_freq: int = 5,
    product_name_length: int = 50,
    encode_length: int = 50,
) -> None:
    """
    Build the model-ready product sequence dataset.
    Loads product metadata and product history features, encodes product names
    as sequences of word indices, pads all variable-length sequence features to
    fixed lengths, and writes the resulting dataset to Parquet.

    Args:
        spark: Active spark session
        raw_dir: Path to the products metadata
        input_dir: Path to product history data
        output_dir: Path to write the built dataframe to
        min_word_freq: Minimum number of word count to be given a word_index greater than zero
        product_name_length: Maximum number of product words
        encode_length: Maximum length of a concated string
    """

    products = read_parquet(gcs_join(raw_dir, "products"), spark)
    product_history_data = read_parquet(
        gcs_join(input_dir, "product_history_data"), spark
    )

    HISTORY_COLUMNS = [
        "position_in_order_history",
        "order_dows",
        "order_hours",
        "days_since_prior_orders",
        "history_order_size",
        "history_reorder_size",
        "order_numbers",
    ]

    word_index = build_word_idx(products, min_word_freq)
    encoded_product_name = encode_product_names(products, word_index)

    df = (
        product_history_data.join(encoded_product_name, how="left", on="product_id")
        .withColumn(
            "product_name_encoded", F.coalesce("product_name_encoded", F.lit(""))
        )
        .withColumn(
            "_parsed_product_name", parse_string_sequence(F.col("product_name_encoded"))
        )
    )

    padded_product_name, prod_name_length = pad_array(
        F.col("_parsed_product_name"), product_name_length
    )
    df = (
        df.withColumn("product_name_encoded", padded_product_name)
        .withColumn("product_name_length", prod_name_length.cast("int"))
        .drop("_parsed_product_name")
    )

    df = df.withColumn(
        "_parsed_is_ordered_history", parse_string_sequence(F.col("is_ordered_history"))
    )
    padded_history_order, history_order_length = pad_array(
        F.col("_parsed_is_ordered_history"), encode_length
    )
    df = df.withColumn("is_ordered_history", padded_history_order).withColumn(
        "history_length", history_order_length.cast("int")
    )

    for colname in HISTORY_COLUMNS:
        parsed_col = parse_string_sequence(F.col(colname))
        name, _ = pad_array(parsed_col, encode_length)
        df = df.withColumn(colname, name)

    df = df.select(
        "user_id",
        "product_id",
        "eval_set",
        "label",
        "product_name_encoded",
        "is_ordered_history",
        "position_in_order_history",
        "history_order_size",
        "history_reorder_size",
        "order_dows",
        "order_hours",
        "days_since_prior_orders",
        "order_numbers",
        "history_length",
        "product_name_length",
    )

    write_parquet(gcs_join(output_dir, "product_training_data"), df)


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("product_training_data")
    build_product_seq_data(
        raw_dir=args.raw_dir,
        spark=spark,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        min_word_freq=args.min_word_freq,
        product_name_length=args.product_name_length,
        encode_length=args.encode_length,
    )
    spark.stop()
