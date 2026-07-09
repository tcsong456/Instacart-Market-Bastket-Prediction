from src.ingestion.create_bronze_datasets import build_bronze_datasets
from src.common.io import read_parquet


def test_create_bronze_datasets_pipeline(spark, tiny_fake_testset_csv):
    build_bronze_datasets(
        spark=spark, input_dir=tiny_fake_testset_csv, output_dir=tiny_fake_testset_csv
    )

    orders = read_parquet(tiny_fake_testset_csv / "orders", spark)
    rows = orders.orderBy("order_id").collect()
    assert orders.count() == 6
    assert [r["order_id"] for r in rows] == [1, 2, 3, 4, 5, 6]
    assert [r["order_number"] for r in rows] == [1, 2, 1, 2, 1, 2]
    assert [r["days_since_prior_order"] for r in rows] == [None, 7, None, 5, None, 3]
    assert [r["user_id"] for r in rows] == [10, 10, 20, 20, 30, 30]
    assert [r["eval_set"] for r in rows] == [
        "prior",
        "train",
        "prior",
        "train",
        "prior",
        "test",
    ]
    assert [r["order_dow"] for r in rows] == [1, 2, 1, 2, 1, 2]
    assert [r["order_hour_of_day"] for r in rows] == [10, 11, 10, 11, 10, 11]

    prior = read_parquet(tiny_fake_testset_csv / "order_products__prior", spark)
    rows = prior.orderBy("order_id").collect()
    assert prior.count() == 3
    assert [r["order_id"] for r in rows] == [1, 3, 5]
    assert [r["product_id"] for r in rows] == [101, 201, 301]
    assert [r["add_to_cart_order"] for r in rows] == [1, 1, 1]
    assert [r["reordered"] for r in rows] == [0, 0, 0]

    train = read_parquet(tiny_fake_testset_csv / "order_products__train", spark)
    rows = train.orderBy("order_id").collect()
    assert train.count() == 2
    assert [r["order_id"] for r in rows] == [2, 4]
    assert [r["product_id"] for r in rows] == [102, 202]
    assert [r["add_to_cart_order"] for r in rows] == [1, 1]
    assert [r["reordered"] for r in rows] == [1, 1]

    products = read_parquet(tiny_fake_testset_csv / "products", spark)
    rows = products.orderBy("product_id").collect()
    assert products.count() == 5
    assert [r["product_id"] for r in rows] == [101, 102, 201, 202, 301]
    assert [r["product_name"] for r in rows] == ["a", "b", "c", "d", "e"]
    assert [r["aisle_id"] for r in rows] == [1, 1, 2, 2, 3]
    assert [r["department_id"] for r in rows] == [10, 10, 20, 20, 30]

    aisles = read_parquet(tiny_fake_testset_csv / "aisles", spark)
    rows = aisles.orderBy("aisle_id").collect()
    assert aisles.count() == 3
    assert [r["aisle_id"] for r in rows] == [1, 2, 3]
    assert [r["aisle"] for r in rows] == ["fresh", "dairy", "snacks"]

    departments = read_parquet(tiny_fake_testset_csv / "departments", spark)
    rows = departments.orderBy("department_id").collect()
    assert departments.count() == 3
    assert [r["department_id"] for r in rows] == [10, 20, 30]
    assert [r["department"] for r in rows] == ["produce", "frozen", "pantry"]
