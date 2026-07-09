import argparse
from src.common.utils import gcs_join
from src.common.spark import create_spark_session
from src.common.io import read_parquet, write_parquet
from pyspark.sql import DataFrame, functions as F, SparkSession


SELECTED_COLUMNS = [
    "user_id",
    "product_id",
    "label",
    "aisle_id",
    "department_id",
    "product_name",
    "eval_set",
    "is_ordered_history",
    "position_in_order_history",
    "history_order_size",
    "history_reorder_size",
    "order_dows",
    "order_hours",
    "days_since_prior_orders",
    "order_numbers",
]


def parse_seq(
    df: DataFrame, input_col: str, prefix: str, calculate_set: bool = False
) -> DataFrame:
    """
    Args:"
        df: A user dataframe with historical sequential features from each of
            its past order concated with '_' as delimiter
        input_col: The name of the column to build features upon
        prefix: The prefix for the name of generated columns
        calculate_set: Whether to get all the unique products for both the past
        and next order
    Returns:
        A dataframe with its sequential features parsed for downstream data
        processing jobs
    """

    df = (
        df.withColumn(f"{prefix}_raw", F.split(F.col(input_col), " "))
        .withColumn(
            f"{prefix}_prev", F.expr(f"slice({prefix}_raw, 1, size({prefix}_raw) - 1)")
        )
        .withColumn(
            f"{prefix}_all",
            F.expr(
                f"""
                    transform(
                        {prefix}_prev,
                        x -> transform(
                            split(x, '_'), y -> cast(y as int)
                        )
                    )
                    """
            ),
        )
        .withColumn(f"next_{prefix}", F.element_at(f"{prefix}_raw", -1))
        .withColumn(
            f"next_{prefix}_int",
            F.expr(
                f"""
                    transform(
                        split(next_{prefix}, '_'),
                        x -> cast(x as int)
                    )
                """
            ),
        )
    )

    if calculate_set:
        df = df.withColumn(
            f"{prefix}_set", F.array_distinct(F.flatten(F.col(f"{prefix}_all")))
        ).withColumn(
            f"next_{prefix}_set", F.array_distinct(F.col(f"next_{prefix}_int"))
        )
    return df


def filtered_orders(path: str, spark: SparkSession) -> DataFrame:
    """
     Args:
        path: Directory that contains all instacart data
        spark: Active Spark session
    returns:
        A orders dataframe with eval_set in each raw equals to
        either 'train' or test
    """

    orders_path = gcs_join(path, "orders")
    orders = read_parquet(path=orders_path, spark=spark)
    orders = orders.filter(F.col("eval_set").isin("train", "test")).select(
        "user_id", F.col("eval_set").alias("target_eval_set")
    )
    return orders


def build_each_product_in_order_history(
    path: str, df: DataFrame, orders: DataFrame, spark: SparkSession
) -> DataFrame:
    """
    Generate per-product history features for each user

    Collapse each user's purchase history into one row per user-product and
    and construct sequential product history features.

    Args:
        df: Parsed user product sequential dataframe
        path: Directory that contains all instacart csv files
        orders: Dataframe contains order dataset filtered by eval_set
        spark: Active Spark session
    Returns:
        Dataframe contains one row per user-product with sequential
        order history features
    """

    product_path = gcs_join(path, "products")
    products = read_parquet(path=product_path, spark=spark)

    df = df.join(orders, how="left", on="user_id")

    df = (
        df.withColumn(
            "history_order_size",
            F.expr("""
                   array_join(
                    transform(products_all, x -> size(x)),
                   ' '
                )
        """),
        )
        .withColumn(
            "history_reorder_size",
            F.expr(
                """
                array_join(
                    transform(
                        sequence(1, size(products_all)),
                        i ->
                        CASE
                        WHEN i = 1 THEN 0 ELSE
                        size(
                            array_intersect(
                                array_distinct(flatten(slice(products_all, 1, i - 1))),
                                element_at(products_all, i)
                            )
                        )
                        END
                    ),
                    ' '
                )
                """
            ),
        )
        .withColumn("product_id", F.explode("products_set"))
        .withColumn(
            "label",
            F.when(
                F.col("target_eval_set") == "train",
                F.array_contains(F.col("next_products_set"), F.col("product_id")).cast(
                    "int"
                ),
            ).otherwise(F.lit(-1)),
        )
        .withColumn(
            "is_ordered_history",
            F.expr(
                """
                array_join(
                    transform(
                        products_all, x -> cast(array_contains(x, product_id) as int)
                    ),
                    ' '
                )
                """
            ),
        )
        .withColumn(
            "position_in_order_history",
            F.expr(
                """
                array_join(
                    transform(
                        products_all, x -> 
                            CASE
                            WHEN array_contains(x, product_id)
                            THEN array_position(x, product_id)
                            ELSE 0
                        END
                    ),
                    ' '
                )
                """
            ),
        )
    )

    df = df.join(products, how="left", on="product_id")
    return df.select(SELECTED_COLUMNS)


def build_each_reorder_history(df: DataFrame, orders: DataFrame) -> DataFrame:
    """
    Args:
        df: A dataframe with parsed sequential features
        orders: A dataframe with metadata about orders provided by instacart but
                only include rows with eval_set in ['train', 'test']
    Returns:
        A dataframe that aggregates its features to construct the history recorde
        for the dummpy 'None' product.
    """

    df = df.join(orders, how="left", on="user_id")
    df = (
        df.withColumn("product_id", F.lit(0))
        .withColumn("aisle_id", F.lit(0))
        .withColumn("department_id", F.lit(0))
        .withColumn("product_name", F.lit(""))
        .withColumn(
            "label",
            F.when(
                F.col("target_eval_set") == "train",
                (F.array_max("next_reorders_int") == 0).cast("int"),
            ).otherwise(F.lit(-1)),
        )
        .withColumn(
            "is_ordered_history",
            F.expr(
                """
                    array_join(
                        transform(
                            reorders_all,
                            x -> cast(array_max(x) == 0 as int)
                        ),
                        ' '
                    )
                """
            ),
        )
        .withColumn(
            "position_in_order_history",
            F.expr(
                """
                    array_join(
                        transform(
                            reorders_all,
                            x -> '0'
                    ),
                    ' '
                )
                """
            ),
        )
        .withColumn(
            "history_order_size",
            F.expr(
                """
                    array_join(
                        transform(
                            reorders_all,
                            x -> cast(size(x) as string)
                        ),
                        ' '
                    )
                """
            ),
        )
        .withColumn(
            "history_reorder_size",
            F.expr(
                """
                    array_join(
                        transform(
                            reorders_all,
                            x -> cast(
                                aggregate(
                                    x, cast(0 as bigint), (acc, v) -> acc + v
                                ) as string
                            )
                        ),
                    ' '
                    )
                """
            ),
        )
    )
    return df.select(SELECTED_COLUMNS)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def build_product_history_data(
    spark: SparkSession,
    input_dir: str,
    raw_dir: str,
    output_dir: str,
) -> None:
    """
    Args:
        None
    Returns:
        A dataframe that contains aggregated historical features for
        each  user-product pair ready downstream model training
    """
    orders = filtered_orders(raw_dir, spark)
    user_data_path = gcs_join(input_dir, "user_data")
    df = read_parquet(user_data_path, spark)
    products = parse_seq(df, "product_ids", "products", True)
    reorders = parse_seq(df, "reorders", "reorders")
    product_history = build_each_product_in_order_history(
        df=products, path=raw_dir, orders=orders, spark=spark
    )
    reorder_history = build_each_reorder_history(reorders, orders)
    product_include_none_history = product_history.unionByName(reorder_history)
    write_parquet(output_dir, product_include_none_history)


if __name__ == "__main__":
    args = parse_args()
    spark = create_spark_session("instacart-product-history")
    build_product_history_data(
        spark=spark,
        input_dir=args.input_dir,
        raw_dir=args.raw_dir,
        output_dir=args.output_dir,
    )
    spark.stop()
