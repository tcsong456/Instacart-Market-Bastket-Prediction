import argparse
from src.common.io import read_parquet, write_parquet
from src.common.spark import create_spark_session
from pyspark.sql import DataFrame, functions as F, SparkSession
from src.common.utils import gcs_join


SELECTED_COLUMNS = [
    "user_id",
    "aisle_id",
    "department_id",
    "eval_set",
    "is_ordered_history",
    "position_in_order",
    "num_products_from_aisle",
    "aisle_history_size",
    "order_dows",
    "order_hours",
    "days_since_prior_orders",
    "order_numbers",
]


def parse_seq(df: DataFrame) -> DataFrame:
    """
    Parse sequential aisle history strings into structured array features.
    The input ``aisle_ids`` column contains a space-separated sequence of
    orders, where each order is represented by underscore-separated aisle
    IDs. This function converts the raw string representation into array-based
    features for downstream feature engineering.

    Args:
        df: Input DataFrame containing an ``aisle_ids`` column.

    Returns:
        DataFrame with the parsed aisle history features appended.
    """
    df = (
        df.withColumn("aisle_raw", F.split("aisle_ids", " "))
        .withColumn("aisle_prev", F.slice("aisle_raw", 1, F.size("aisle_raw") - 1))
        .withColumn("aisle_next", F.element_at("aisle_raw", -1))
        .withColumn(
            "aisle_all",
            F.expr(
                """
                    transform(
                        aisle_prev,
                        x -> transform(
                            split(x, '_'), y -> cast(y as int)
                        )
                    )
                """
            ),
        )
        .withColumn("aisle_set", F.array_distinct(F.flatten(F.col("aisle_all"))))
        .withColumn(
            "next_aisle_set",
            F.expr(
                """
                    array_distinct(
                        transform(
                            split(aisle_next, '_'),
                            x -> cast(x as int)
                        )
                    )
                """
            ),
        )
    )

    return df


def build_aisle_history_data(df: DataFrame):
    """
    Generate per-aisle historical features from parsed aisle histories.
    Expands each user's historical aisle set into one row per aisle and
    constructs sequential features describing the user's interaction with
    that aisle across previous orders.

    Args:
        df: Input DataFrame produced by ``parse_seq()``, containing the
            parsed aisle history columns (``aisle_all``, ``aisle_set``,
            etc.).

    Returns:
        DataFrame with one row per user-aisle pair and the corresponding
        historical aisle features appended.
    """

    df = (
        df.withColumn(
            "aisle_history_size",
            F.expr(
                """
                    array_join(
                        transform(
                            aisle_all,
                            x -> size(array_distinct(x))
                        ),
                    ' '
                    )
                """
            ),
        )
        .withColumn("aisle_id", F.explode("aisle_set"))
        .withColumn(
            "is_ordered_history",
            F.expr(
                """
                    array_join(
                        transform(
                            aisle_all,
                            x -> cast(array_contains(x, aisle_id) as int)
                        ),
                    ' '
                    )
                """
            ),
        )
        .withColumn(
            "position_in_order",
            F.expr(
                """
                    array_join(
                        transform(
                            aisle_all,
                            x -> 
                            CASE
                            WHEN array_contains(x, aisle_id)
                            THEN array_position(array_distinct(x), aisle_id)
                            ELSE 0
                            END
                        ),
                    ' '
                    )
                """
            ),
        )
        .withColumn(
            "num_products_from_aisle",
            F.expr(
                """
                    array_join(
                        transform(
                            aisle_all,
                            x -> cast(size(filter(x, y -> y = aisle_id)) as string)
                        ),
                    ' '
                    )
                """
            ),
        )
    )

    return df


def parse_pargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("-display-partition", action="store_true")
    return parser.parse_args()


def build_create_aisle_history_data(
    spark: SparkSession, input_dir: str, output_dir: str, raw_dir: str
) -> None:
    user_data = read_parquet(gcs_join(input_dir, "user_data"), spark)
    df = parse_seq(user_data)
    df = build_aisle_history_data(df)
    products = read_parquet(gcs_join(raw_dir, "products"), spark)
    df = df.join(
        products.select("aisle_id", "department_id").distinct(),
        how="left",
        on="aisle_id",
    )
    df = df.select(SELECTED_COLUMNS)
    write_parquet(gcs_join(output_dir, "aisle_history_data"), df)


if __name__ == "__main__":
    spark = create_spark_session("aisle_data")
    args = parse_pargs()
    build_create_aisle_history_data(
        spark=spark,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        raw_dir=args.raw_dir,
    )
