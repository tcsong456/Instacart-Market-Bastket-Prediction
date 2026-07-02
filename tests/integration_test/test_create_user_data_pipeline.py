from src.ingestion.create_user_data import build_user_data
from src.ingestion.create_order_products import build_order_products
from src.common.io import read_parquet


def test_create_user_data_pipeline(spark, tiny_fake_testset_v1):
    build_order_products(spark, tiny_fake_testset_v1, True)
    tmp_order_products_path = tiny_fake_testset_v1 / 'order_products_sample'
    tmp_user_data_path = tiny_fake_testset_v1 / 'user_data_sample'
    build_user_data(spark, tmp_order_products_path, tmp_user_data_path)
    user_data = read_parquet(tmp_user_data_path, spark)

    assert user_data.count() == 3
    assert set(user_data.columns) == {
        'user_id',
        'order_ids',
        'order_numbers',
        'order_dows',
        'order_hours',
        'days_since_prior_orders',
        'product_ids',
        'reorders',
        'department_ids',
        'aisle_ids',
        'eval_set'
    }

    row = user_data.filter('user_id==10').first()
    assert row['product_ids'] == '110_101_100 102_57_202_99'
    assert row['reorders'] == '0_0_1 1_1_1_0'
    assert row['eval_set'] == 'train'
    assert row['department_ids'] == '30_30_20 10_10_20_20'
    assert row['aisle_ids'] == '3_3_2 1_1_2_2'

    row = user_data.filter('user_id==30').first()
    assert row['product_ids'] == '400 501'
    assert row['reorders'] == '1 0'
    assert row['eval_set'] == 'test'
    assert row['department_ids'] == '50 20'
    assert row['aisle_ids'] == '5 2'
