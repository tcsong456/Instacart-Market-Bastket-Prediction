from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


def assert_schema_matches(
    df: DataFrame, expected_schema: StructType, check_nullable: bool = False
) -> None:
    actual_fields = df.schema.fields
    expected_fields = expected_schema.fields

    assert [f.name for f in actual_fields] == [f.name for f in expected_fields]

    if check_nullable:
        assert [f.nullable for f in actual_fields] == [
            f.nullable for f in expected_fields
        ]


def assert_spark_df_equal(
    actual_df: DataFrame,
    expected_df: DataFrame,
    order_by_col: list[str],
    sort_array_columns: list[str] = [],
) -> None:
    actual_df = actual_df.select(expected_df.columns)
    if len(sort_array_columns) > 0:
        for column_name in sort_array_columns:
            actual_df = actual_df.withColumn(
                column_name, F.array_sort(F.col(column_name))
            )
            expected_df = expected_df.withColumn(
                column_name, F.array_sort(F.col(column_name))
            )

    assert (
        actual_df.orderBy(*order_by_col).collect()
        == expected_df.orderBy(*order_by_col).collect()
    )
    assert actual_df.schema == expected_df.schema
