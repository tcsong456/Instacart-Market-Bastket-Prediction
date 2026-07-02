from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def fillna_and_cast(
    df: DataFrame,
    column: str,
    fillvalue,
    dtype: str,
) -> DataFrame:
    return (
        df.fillna({column: fillvalue})
        .withColumn(column, col(column).cast(dtype))
    )
