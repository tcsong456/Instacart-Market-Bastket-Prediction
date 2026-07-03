from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def assert_schema_matches(
    df: DataFrame, expected_schema: StructType, check_nullable: bool = False
) -> None:
    actual_fields = df.schema.fields
    expected_fields = expected_schema.fields

    assert [f.name for f in actual_fields] == [f.name for f in expected_fields]
    assert [f.dataType for f in actual_fields] == [f.dataType for f in expected_fields]

    if check_nullable:
        assert [f.nullable for f in actual_fields] == [
            f.nullable for f in expected_fields
        ]
