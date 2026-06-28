import pandas as pd
from src.ingestion.create_sample_dataset import (
    sample_users,
    filter_orders_by_users,
    write_filtered_order_products
)


def test_same_seed_same_users(tiny_fake_testset):
    orders = pd.read_csv(tiny_fake_testset / 'orders.csv')

    user1 = sample_users(orders, 2, 19810)
    user2 = sample_users(orders, 2, 19810)

    return user1.tolist() == user2.tolist()


def test_sample_users_are_unique(tiny_fake_testset):
    orders = pd.read_csv(tiny_fake_testset / 'orders.csv')

    sampled_users = sample_users(orders, 3, 19810)

    return sampled_users.is_unique


def test_sampled_users_count(tiny_fake_testset):
    orders = pd.read_csv(tiny_fake_testset / 'orders.csv')

    sampled_users = sample_users(orders, 3, 19810)

    assert sampled_users.nunique() == 3


def test_filter_orders_contain_only_sampled_users(tiny_fake_testset):
    orders = pd.read_csv(tiny_fake_testset / 'orders.csv')
    sampled_users = pd.Series([10, 30])
    filtered_order_users = filter_orders_by_users(orders, sampled_users)

    assert set(filtered_order_users['user_id']) == {10, 30}
    assert filtered_order_users['order_id'].tolist() == [1, 2, 5, 6]


def test_filter_orders_contain_orders_correctly(tiny_fake_testset, tmp_path):
    order_ids = {1, 5}
    sample_dir = tmp_path / 'sample'
    sample_dir.mkdir()

    write_filtered_order_products(
        raw_dir=tiny_fake_testset,
        sample_dir=sample_dir,
        chunksize=1,
        filename='order_products__prior.csv',
        order_ids=order_ids
    )
    filtered_order_products = pd.read_csv(sample_dir / 'order_products__prior.csv')

    assert set(filtered_order_products['order_id']) == order_ids


def test_filtered_orders_header_only_once(tiny_fake_testset, tmp_path):
    sample_dir = tmp_path / 'sample'
    sample_dir.mkdir()

    write_filtered_order_products(
        raw_dir=tiny_fake_testset,
        sample_dir=sample_dir,
        chunksize=1,
        filename='order_products__prior.csv',
        order_ids={1, 3, 5}
    )

    header = "order_id,product_id,add_to_cart_order,reordered\n"

    with open(sample_dir / 'order_products__prior.csv', encoding='utf-8') as f:
        lines = f.readlines()

    assert lines.count(header) == 1


def test_filtered_orders_produce_no_mathcing_results(tiny_fake_testset, tmp_path):
    sample_dir = tmp_path / 'sample'
    sample_dir.mkdir()

    write_filtered_order_products(
        raw_dir=tiny_fake_testset,
        sample_dir=sample_dir,
        chunksize=1,
        filename='order_products__prior.csv',
        order_ids={100000}
    )

    output_dir = sample_dir / 'order_products__prior.csv'
    assert not output_dir.exists()
    if output_dir.exists():
        result = pd.read_csv(output_dir)
        assert result.empty
