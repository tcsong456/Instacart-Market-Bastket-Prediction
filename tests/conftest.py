import sys
import pytest
import pandas as pd
from pathlib import Path
from src.common.io import write_parquet
from src.common.spark import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def raw_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


@pytest.fixture
def tiny_fake_testset_csv(raw_dir):
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "user_id": [10, 10, 20, 20, 30, 30],
            "eval_set": ["prior", "train", "prior", "train", "prior", "test"],
            "order_number": [1, 2, 1, 2, 1, 2],
            "order_dow": [1, 2, 1, 2, 1, 2],
            "order_hour_of_day": [10, 11, 10, 11, 10, 11],
            "days_since_prior_order": [None, 7, None, 5, None, 3],
        }
    )

    prior = pd.DataFrame(
        {
            "order_id": [1, 3, 5],
            "product_id": [101, 201, 301],
            "add_to_cart_order": [1, 1, 1],
            "reordered": [0, 0, 0],
        }
    )

    train = pd.DataFrame(
        {
            "order_id": [2, 4],
            "product_id": [102, 202],
            "add_to_cart_order": [1, 1],
            "reordered": [1, 1],
        }
    )

    products = pd.DataFrame(
        {
            "product_id": [101, 102, 201, 202, 301],
            "product_name": ["a", "b", "c", "d", "e"],
            "aisle_id": [1, 1, 2, 2, 3],
            "department_id": [10, 10, 20, 20, 30],
        }
    )

    aisles = pd.DataFrame(
        {
            "aisle_id": [1, 2, 3],
            "aisle": ["fresh", "dairy", "snacks"],
        }
    )

    departments = pd.DataFrame(
        {
            "department_id": [10, 20, 30],
            "department": ["produce", "frozen", "pantry"],
        }
    )

    orders.to_csv(raw_dir / "orders.csv", index=False)
    prior.to_csv(raw_dir / "order_products__prior.csv", index=False)
    train.to_csv(raw_dir / "order_products__train.csv", index=False)
    products.to_csv(raw_dir / "products.csv", index=False)
    aisles.to_csv(raw_dir / "aisles.csv", index=False)
    departments.to_csv(raw_dir / "departments.csv", index=False)

    return raw_dir


@pytest.fixture
def tiny_fake_testset_parquet(spark, raw_dir):
    orders = spark.createDataFrame(
        [
            (1, 10, "prior", 1, 1, 10, None),
            (2, 10, "train", 2, 2, 11, 7),
            (3, 20, "prior", 1, 1, 10, None),
            (4, 20, "train", 2, 2, 11, 5),
            (5, 30, "prior", 1, 1, 10, None),
            (6, 30, "test", 2, 2, 11, 3),
        ],
        [
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ],
    )

    prior = spark.createDataFrame(
        [
            (1, 101, 1, 0),
            (3, 201, 1, 0),
            (5, 301, 1, 0),
        ],
        [
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
        ],
    )

    train = spark.createDataFrame(
        [
            (2, 102, 1, 1),
            (4, 202, 1, 1),
        ],
        ["order_id", "product_id", "add_to_cart_order", "reordered"],
    )

    products = spark.createDataFrame(
        [
            (101, "a", 1, 10),
            (102, "b", 1, 10),
            (201, "c", 2, 20),
            (202, "d", 2, 20),
            (301, "e", 3, 30),
        ],
        ["product_id", "product_name", "aisle_id", "department_id"],
    )

    aisles = spark.createDataFrame(
        [
            (1, "fresh"),
            (2, "dairy"),
            (3, "snacks"),
        ],
        ["aisle_id", "aisle"],
    )

    departments = spark.createDataFrame(
        [
            (10, "produce"),
            (20, "frozen"),
            (30, "pantry"),
        ],
        ["department_id", "department"],
    )

    write_parquet(raw_dir / "orders", orders)
    write_parquet(raw_dir / "order_products__prior", prior)
    write_parquet(raw_dir / "order_products__train", train)
    write_parquet(raw_dir / "products", products)
    write_parquet(raw_dir / "aisles", aisles)
    write_parquet(raw_dir / "departments", departments)

    return raw_dir


@pytest.fixture
def tiny_fake_testset_v1(spark, tmp_path):
    raw_dir = tmp_path / "raw_v1"
    raw_dir.mkdir()

    orders = spark.createDataFrame(
        [
            (1, 10, "prior", 1, 1, 10, None),
            (2, 10, "train", 2, 2, 11, 7),
            (3, 20, "prior", 1, 1, 10, None),
            (4, 20, "train", 2, 2, 11, 5),
            (5, 30, "prior", 1, 1, 10, None),
            (6, 30, "test", 2, 2, 11, 3),
        ],
        [
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ],
    )

    prior = spark.createDataFrame(
        [
            (1, 101, 2, 0),
            (1, 100, 3, 1),
            (1, 110, 1, 0),
            (3, 301, 2, 1),
            (3, 201, 1, 0),
            (5, 400, 1, 1),
        ],
        [
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
        ],
    )

    train = spark.createDataFrame(
        [
            (2, 102, 1, 1),
            (2, 202, 3, 1),
            (2, 99, 4, 0),
            (2, 57, 2, 1),
            (4, 20, 1, 1),
            (4, 200, 2, 0),
            (6, 501, 1, 0),
        ],
        [
            "order_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
        ],
    )

    products = spark.createDataFrame(
        [
            (20, "a", 1, 10),
            (57, "b", 1, 10),
            (99, "c", 2, 20),
            (100, "d", 2, 20),
            (101, "e", 3, 30),
            (102, "f", 1, 10),
            (110, "g", 3, 30),
            (200, "h", 4, 40),
            (201, "i", 4, 40),
            (202, "j", 2, 20),
            (301, "k", 1, 10),
            (400, "l", 5, 50),
            (501, "m", 2, 20),
        ],
        [
            "product_id",
            "product_name",
            "aisle_id",
            "department_id",
        ],
    )

    write_parquet(raw_dir / "orders", orders)
    write_parquet(raw_dir / "order_products__prior", prior)
    write_parquet(raw_dir / "order_products__train", train)
    write_parquet(raw_dir / "products", products)

    return raw_dir


@pytest.fixture
def fake_filtered_orders(spark, raw_dir):
    filtered_orders = spark.createDataFrame(
        [
            {"user_id": 10, "target_eval_set": "train"},
            {"user_id": 20, "target_eval_set": "test"},
        ]
    )

    order_path = raw_dir / "filtered_orders.csv"
    filtered_orders.toPandas().to_csv(order_path, index=False)
    return raw_dir


@pytest.fixture
def fake_orders(spark, raw_dir):
    orders = spark.createDataFrame(
        [
            (10, 1, "train", 3, 23),
            (20, 2, "test", 0, 11),
            (30, 3, "prior", 1, 7),
            (40, 4, "prior", 5, 1),
        ],
        [
            "user_id",
            "order_id",
            "eval_set",
            "order_dow",
            "order_hour_of_day",
        ],
    )
    order_path = raw_dir / "orders"
    write_parquet(order_path, orders)
    return raw_dir


@pytest.fixture
def fake_products_data(spark, raw_dir):
    products = spark.createDataFrame(
        [
            (1, "b", 5, 10),
            (2, "c", 10, 10),
            (3, "d", 15, 30),
            (4, "e", 10, 20),
            (5, "f", 5, 10),
            (10, "g", 20, 40),
            (11, "h", 5, 30),
            (12, "i", 15, 20),
            (15, "j", 30, 20),
            (20, "l", 20, 30),
            (1000, "m", 20, 40),
            (323, "n", 10, 20),
            (300, "o", 15, 40),
            (23, "z", 25, 50),
            (75, "k", 25, 40),
        ],
        [
            "product_id",
            "product_name",
            "aisle_id",
            "department_id",
        ],
    )
    product_path = raw_dir / "products"
    write_parquet(product_path, products)
    return raw_dir


@pytest.fixture
def fake_user_data(spark, tmp_path):
    curated_dir = tmp_path / "curated"
    curated_dir.mkdir(parents=True, exist_ok=True)

    user_data = spark.createDataFrame(
        [
            (
                10,
                "1_2_3 4_10 5_323_1_12 5",
                "0_0_0 1_1 1_0_1_0 1",
                "5_10_15 10_20 5_10_5_15 5",
                "10_10_30 20_40 10_20_10_20 10",
                "train",
                "5 0 5 1",
                "23 1 17 16",
                "-1 30 60 25",
                "1 2 3 4",
            ),
            (
                20,
                "11_5 300 12_20_1000_300_11 20_11_75",
                "0_0 0 0_1_0_1_1 1_1_0",
                "5_5 15 15_20_20_15_5 20_5_25",
                "30_10 40 20_30_40_40_30 30_10_40",
                "test",
                "1 2 3 4",
                "8 13 8 0",
                "10 19 22 6",
                "3 4 5 6",
            ),
        ],
        schema=[
            "user_id",
            "product_ids",
            "reorders",
            "aisle_ids",
            "department_ids",
            "eval_set",
            "order_dows",
            "order_hours",
            "days_since_prior_orders",
            "order_numbers",
        ],
    )

    write_parquet(curated_dir / "user_data", user_data)
    return curated_dir


@pytest.fixture
def fake_parse_seq_data(spark):
    df = spark.createDataFrame(
        [
            {
                "user_id": 10,
                "products_set": [1, 2, 3, 4, 5, 10, 12, 323],
                "products_all": [[1, 2, 3], [4, 10], [5, 323, 1, 12]],
                "reorders_all": [[0, 0, 0], [0, 0], [0, 0, 1, 0]],
                "aisle_set": [5, 10, 15, 10, 5, 20, 15, 10],
                "aisle_all": [[5, 10, 15], [10, 20], [5, 10, 5, 15]],
                "next_products_set": [5],
                "next_reorders_int": [1],
                "order_dows": "5 0 5 1",
                "order_hours": "23 1 17 16",
                "days_since_prior_orders": "-1 30 60 25",
                "order_numbers": "1 2 3 4",
                "eval_set": "train",
            },
            {
                "user_id": 20,
                "products_set": [11, 5, 23, 300, 12, 20, 1000],
                "products_all": [[11, 5, 23], [300, 23], [12, 20, 1000, 300, 11]],
                "reorders_all": [[0, 1, 0], [0, 1], [1, 0, 0, 1, 1]],
                "aisle_set": [5, 5, 25, 15, 15, 20, 20],
                "aisle_all": [[5, 5, 25], [15, 25], [15, 20, 20, 15, 5]],
                "next_products_set": [],
                "next_reorders_int": [],
                "order_dows": "1 2 3 4",
                "order_hours": "8 13 8 0",
                "days_since_prior_orders": "10 19 22 6",
                "order_numbers": "3 4 5 6",
                "eval_set": "test",
            },
        ]
    )
    return df


@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session("unit-test")
    yield spark
    spark.stop()
