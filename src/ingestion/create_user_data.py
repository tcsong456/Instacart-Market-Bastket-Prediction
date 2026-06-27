import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark():
    spark = (
        SparkSession.builder
        .appName('instacart-etl')
        .getOrCreate()
    )
    return spark


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-dir',
        required=True
    )
    parser.add_argument(
        '--output-dir',
        required=True
    )
    return parser.parse_args()


def build_user_data(
        args,
        spark
        ):
    df = spark.read.parquet(args.input_dir)
    item_struct = F.struct(
        F.col("add_to_cart_order"),
        F.col('product_id').cast('string').alias('product_id'),
        F.col('reordered').cast('string').alias('reordered'),
        F.col('department_id').cast('string').alias('department_id'),
        F.col('aisle_id').cast('string').alias('aisle_id')
    )

    df = (
        df
        .withColumn('item', item_struct)
        .groupby('user_id', 'order_id')
        .agg(
            F.sort_array(F.collect_list('item')).alias('items'),
            F.first('order_number').alias('order_number'),
            F.first('order_dow').alias('order_dow'),
            F.first('order_hour_of_day').alias('order_hour'),
            F.first('days_since_prior_order').alias('days_since_prior_order'),
            F.first("eval_set").alias("eval_set"),
        )
        .withColumn(
            'products',
            F.concat_ws(
                '_',
                F.expr(
                    'transform(items, x -> x.product_id)'
                )
            )
        )
        .withColumn(
            'reorders',
            F.concat_ws(
                '_',
                F.expr(
                    'transform(items, x -> x.reordered)'
                )
            )
        )
        .withColumn(
            'departments',
            F.concat_ws(
                '_',
                F.expr(
                    'transform(items, x -> x.department_id)'
                )
            )
        )
        .withColumn(
            'aisles',
            F.concat_ws(
                '_',
                F.expr(
                    'transform(items, x -> x.aisle_id)'
                )
            )
        )
        .drop('items')
    )

    order_struct = F.struct(
        F.col('order_number').cast('string').alias('order_number'),
        F.col('order_id').cast('string').alias('order_id'),
        F.col('order_dow').cast('string').alias('order_dow'),
        F.col('order_hour').cast('string').alias('order_hour'),
        (
            F.col('days_since_prior_order')
            .cast('string')
            .alias('days_since_prior_order')
        ),
        F.col('products'),
        F.col('reorders'),
        F.col('departments'),
        F.col('aisles'),
        F.col('eval_set'),
    )

    user_data = (
        df
        .withColumn('order', order_struct)
        .groupby('user_id')
        .agg(
            F.sort_array(F.collect_list('order')).alias('orders')
        )
        .withColumn(
            'order_ids',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.order_id)'
                )
            )
        )
        .withColumn(
            'order_numbers',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.order_number)'
                )
            )
        )
        .withColumn(
            'order_dows',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.order_dow)'
                )
            )
        )
        .withColumn(
            'order_hours',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.order_hour)'
                )
            )
        )
        .withColumn(
            'days_since_prior_orders',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.days_since_prior_order)'
                )
            )
        )
        .withColumn(
            'product_ids',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.products)'
                )
            )
        )
        .withColumn(
            'reorders',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.reorders)'
                )
            )
        )
        .withColumn(
            'department_ids',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.departments)'
                )
            )
        )
        .withColumn(
            'aisle_ids',
            F.concat_ws(
                ' ',
                F.expr(
                    'transform(orders, x -> x.aisles)'
                )
            )
        )
        .withColumn(
            "eval_set",
            F.expr(
                "element_at(transform(orders, x -> x.eval_set), -1)"
            )
        )
        .drop('orders')
    )
    return user_data


if __name__ == '__main__':
    args = parse_args()
    spark = create_spark()
    user_data = build_user_data(spark=spark, args=args)
    user_data.write.mode("overwrite").parquet(args.output_dir)
    spark.stop()
