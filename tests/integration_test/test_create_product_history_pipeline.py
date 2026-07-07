from src.common.io import read_parquet
from src.ingestion.create_product_history_data import build_product_history_data


def test_build_product_history_data_pipeline(
    spark,
    fake_user_data,
    fake_filtered_orders,
    fake_products_data,
    fake_orders,
    raw_dir,
    tmp_path,
):
    output_dir = tmp_path / "product_data"
    build_product_history_data(
        input_dir=fake_user_data, raw_dir=raw_dir, output_dir=output_dir, spark=spark
    )

    result = read_parquet(output_dir, spark)
    result.show(truncate=False)

    x = 10
    assert x == 5
