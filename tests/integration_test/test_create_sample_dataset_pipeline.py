import pandas as pd
from src.ingestion.create_sample_dataset import main


def test_create_sample_dataset_pipeline(tiny_fake_testset_csv, tmp_path):
    sample_dir = tmp_path / "sample"

    main(
        raw_dir=tiny_fake_testset_csv,
        sample_dir=sample_dir,
        chunk_size=2,
        seed=7827,
        sample_n=2,
    )

    orders = pd.read_csv(sample_dir / "orders.csv")
    orders_train = pd.read_csv(sample_dir / "order_products__train.csv")
    orders_prior = pd.read_csv(sample_dir / "order_products__prior.csv")
    order_products = pd.concat([orders_prior, orders_train])
    assert set(order_products["order_id"]).issubset(set(orders["order_id"]))
