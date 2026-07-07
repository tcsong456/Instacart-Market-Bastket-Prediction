import sys
import pytest
import pandas as pd
from pathlib import Path
from src.common.spark import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def tiny_fake_testset(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

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
def tiny_fake_testset_v1(tmp_path):
    raw_dir = tmp_path / "raw_v1"
    raw_dir.mkdir()

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
            "order_id": [1, 1, 1, 3, 3, 5],
            "product_id": [101, 100, 110, 301, 201, 400],
            "add_to_cart_order": [2, 3, 1, 2, 1, 1],
            "reordered": [0, 1, 0, 1, 0, 1],
        }
    )

    train = pd.DataFrame(
        {
            "order_id": [2, 2, 2, 2, 4, 4, 6],
            "product_id": [102, 202, 99, 57, 20, 200, 501],
            "add_to_cart_order": [1, 3, 4, 2, 1, 2, 1],
            "reordered": [1, 1, 0, 1, 1, 0, 0],
        }
    )

    products = pd.DataFrame(
        {
            "product_id": [
                20,
                57,
                99,
                100,
                101,
                102,
                110,
                200,
                201,
                202,
                301,
                400,
                501,
            ],
            "product_name": [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
                "k",
                "l",
                "m",
            ],
            "aisle_id": [1, 1, 2, 2, 3, 1, 3, 4, 4, 2, 1, 5, 2],
            "department_id": [10, 10, 20, 20, 30, 10, 30, 40, 40, 20, 10, 50, 20],
        }
    )

    orders.to_csv(raw_dir / "orders.csv", index=False)
    prior.to_csv(raw_dir / "order_products__prior.csv", index=False)
    train.to_csv(raw_dir / "order_products__train.csv", index=False)
    products.to_csv(raw_dir / "products.csv", index=False)

    return raw_dir


@pytest.fixture
def fake_filtered_orders(spark):
    return spark.createDataFrame(
        [
            {"user_id": 10, "target_eval_set": "train"},
            {"user_id": 20, "target_eval_set": "test"},
        ]
    )


@pytest.fixture
def fake_user_data(spark):
    return spark.createDataFrame(
        [
            (
                10,
                "1_2_3 0_10 5_323_1_12 5",
                "0_0_0 1_1 1_0_1_0 1",
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
            "eval_set",
            "order_dows",
            "order_hours",
            "days_since_prior_orders",
            "order_numbers",
        ],
    )


@pytest.fixture
def fake_parse_seq_data(spark):
    df = spark.createDataFrame(
        [
            {
                "user_id": 10,
                "products_set": [0, 1, 2, 3, 5, 10, 12, 323],
                "products_all": [[1, 2, 3], [0, 10], [5, 323, 1, 12]],
                "reorders_all": [[0, 0, 0], [0, 0], [0, 0, 1, 0]],
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
