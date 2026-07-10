from src.common.io import read_parquet
from src.ingestion.create_aisle_history_data import parse_seq


def test_parse_seq(fake_user_data, spark):
    user_data = read_parquet(fake_user_data / "user_data", spark)
    df = parse_seq(user_data)

    row = df.filter("user_id==10").first()
    assert row["aisle_raw"] == ["5_10_15", "10_20", "5_10_5_15", "5"]
    assert row["aisle_next"] == "5"
    assert sorted(row["aisle_set"]) == [5, 10, 15, 20]

    row = df.filter("user_id==20").first()
    assert row["aisle_prev"] == ["5_5", "15", "15_20_20_15_5"]
    assert row["aisle_all"] == [[5, 5], [15], [15, 20, 20, 15, 5]]
    assert sorted(row["next_aisle_set"]) == [5, 20, 25]
