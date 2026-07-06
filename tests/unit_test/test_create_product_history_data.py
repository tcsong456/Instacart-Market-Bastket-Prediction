from src.ingestion.create_product_history_data import parse_seq


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
