from pathlib import Path
from pyspark.sql import DataFrame


def gcs_join(base_path: str | Path, filename: str) -> str:
    return str(base_path).rstrip("/") + "/" + filename


def assert_spark_df_equal(
    actual_df: DataFrame, expected_df: DataFrame, order_by_col: list
) -> None:
    actual_df = actual_df.select(expected_df.columns)
    assert (
        actual_df.orderBy(*order_by_col).collect()
        == expected_df.orderBy(*order_by_col).collect()
    )
    assert actual_df.schema == expected_df.schema
