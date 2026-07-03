from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame


def read_csv(path: str | Path, spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.read.schema(schema).option("header", True).csv(str(path))


def read_parquet(path: str | Path, spark: SparkSession) -> DataFrame:
    return spark.read.parquet(str(path))


def write_parquet(path: str | Path, df: DataFrame) -> None:
    (df.write.mode("overwrite").parquet(str(path)))
