import pandas as pd
from src.ingestion.create_product_history_data import parse_seq, filtered_orders


def test_parse_seq_with_set(tiny_fake_testset_v2):
    df = tiny_fake_testset_v2
    df = parse_seq(
        df=df, input_col="product_ids", prefix="products", calculate_set=True
    )

    row = df.filter("user_id==10").first()
    assert row["products_raw"] == ["1_2_3", "0_10", "5_323_1_12", "5"]
    assert row["products_prev"] == ["1_2_3", "0_10", "5_323_1_12"]
    assert row["products_all"] == [[1, 2, 3], [0, 10], [5, 323, 1, 12]]
    assert row["next_products"] == "5"
    assert row["next_products_int"] == [5]
    assert sorted(row["products_set"]) == [0, 1, 2, 3, 5, 10, 12, 323]
    assert sorted(row["next_products_set"]) == [5]


def test_parse_seq_without_set(tiny_fake_testset_v2):
    df = tiny_fake_testset_v2
    df = parse_seq(df=df, input_col="reorders", prefix="reorders", calculate_set=False)

    row = df.filter("user_id==20").first()
    assert row["reorders_raw"] == ["0_0", "0", "0_1_0_1_1", "1_1_0"]
    assert row["reorders_prev"] == ["0_0", "0", "0_1_0_1_1"]
    assert row["reorders_all"] == [[0, 0], [0], [0, 1, 0, 1, 1]]
    assert row["next_reorders"] == "1_1_0"
    assert row["next_reorders_int"] == [1, 1, 0]
    assert "reorders_set" not in df.columns
    assert "next_reorders_set" not in df.columns


def test_filtered_orders(spark, tmp_path):
    df = pd.DataFrame(
        [
            (10, 1, "train", 3, 23),
            (20, 2, "prior", 0, 11),
            (30, 3, "test", 1, 7),
            (40, 4, "prior", 5, 1),
        ],
        columns=["user_id", "order_id", "eval_set", "order_dow", "order_hour_of_day"],
    )
    order_path = tmp_path / "orders.csv"
    df.to_csv(order_path, index=False)

    df = filtered_orders(tmp_path, spark)

    assert df.count() == 2
    assert set(df.columns) == {"user_id", "target_eval_set"}

    rows = {row["user_id"]: row["target_eval_set"] for row in df.collect()}

    assert rows == {10: "train", 30: "test"}
