from pyspark.sql import Row
from src.ingestion.create_user_data import build_order_level_data, build_user_level_data


def test_build_order_level_data(spark):
    df = spark.createDataFrame(
        [
            Row(
                user_id=10,
                order_id=1,
                add_to_cart_order=2,
                product_id=101,
                reordered=1,
                department_id=10,
                aisle_id=200,
                order_number=1,
                order_dow=3,
                order_hour_of_day=11,
                days_since_prior_order=-1,
                eval_set="prior",
            ),
            Row(
                user_id=10,
                order_id=1,
                add_to_cart_order=1,
                product_id=102,
                reordered=0,
                department_id=20,
                aisle_id=100,
                order_number=1,
                order_dow=3,
                order_hour_of_day=11,
                days_since_prior_order=-1,
                eval_set="prior",
            ),
            Row(
                user_id=10,
                order_id=2,
                add_to_cart_order=1,
                product_id=201,
                reordered=0,
                department_id=30,
                aisle_id=300,
                order_number=2,
                order_dow=4,
                order_hour_of_day=12,
                days_since_prior_order=7,
                eval_set="train",
            ),
        ]
    )

    result = build_order_level_data(df)
    assert result.count() == 2

    row1 = result.filter('order_id==1').first()
    assert row1['products'] == '102_101'
    assert row1['reorders'] == '0_1'
    assert row1['departments'] == '20_10'
    assert row1['aisles'] == '100_200'
    assert row1['order_number'] == 1
    assert row1['order_dow'] == 3
    assert row1['order_hour'] == 11
    assert row1['days_since_prior_order'] == -1
    assert row1['eval_set'] == 'prior'

    row2 = result.filter('order_id==2').first()
    assert row2["products"] == "201"
    assert row2["reorders"] == "0"
    assert row2["departments"] == "30"
    assert row2["aisles"] == "300"


def test_build_user_level_data(spark):
    df = spark.createDataFrame([
        Row(
            user_id=10,
            order_id=2,
            order_number=2,
            order_dow="4",
            order_hour="12",
            days_since_prior_order="-1",
            products="201_202",
            reorders="1_0",
            departments="5_6",
            aisles="300_301",
            eval_set="train",
        ),
        Row(
            user_id=10,
            order_id=1,
            order_number=1,
            order_dow="6",
            order_hour="11",
            days_since_prior_order="7",
            products="102_101",
            reorders="0_1",
            departments="20_10",
            aisles="100_200",
            eval_set="prior",
        ),
    ])

    result = build_user_level_data(df)
    assert result.count() == 1

    row = result.first()
    assert row['user_id'] == 10
    assert row['order_ids'] == '1 2'
    assert row['order_numbers'] == '1 2'
    assert row['order_dows'] == '6 4'
    assert row['order_hours'] == '11 12'
    assert row['days_since_prior_orders'] == '7 -1'
    assert row['product_ids'] == '102_101 201_202'
    assert row['reorders'] == '0_1 1_0'
    assert row['department_ids'] == '20_10 5_6'
    assert row['aisle_ids'] == '100_200 300_301'
    assert row['eval_set'] == 'train'
