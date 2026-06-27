import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def create_spark():
    return (SparkSession.builder
            .appName('instacart-etl')
            .getOrCreate())

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample-dir', required=True, type=str)
    return parser.parse_args()

def build_df(spark,
             args):
    sample_dir = args.sample_dir

    prior_products = (
        spark.read
        .option('header', True)
        .option('inferSchema', True)
        .csv(f'{sample_dir}/order_products__prior.csv')
    )

    train_products = (
        spark.read
        .option('header', True)
        .option('inferSchema', True)
        .csv(f'{sample_dir}/order_products__train.csv')
    )

    orders = (
        spark.read
        .option('header', True)
        .option('inferSchema', True)
        .csv(f'{sample_dir}/orders.csv')
    )

    products = (
        spark.read
        .option('header', True)
        .option('inferSchema', True)
        .csv(f'{sample_dir}/products.csv')
    )

    order_products = prior_products.unionByName(train_products)
    df = order_products.join(orders, how='inner', on='order_id')
    df = df.join(products, how='inner', on='product_id')
    print(f'prior_products count: {prior_products.count()}')
    print(f'train_products count: {train_products.count()}')
    print(f'order_products count: {order_products.count()}')
    print(f'df count: {df.count()}')
    print(df.rdd.getNumPartitions())

    null_cols = [
        "product_id",
        "aisle_id",
        "department_id",
        "add_to_cart_order",
        "reordered",
        "days_since_prior_order"
    ]
    for c in null_cols:
        df = (
            df.fillna({c: 0})
            .withColumn(
                c,
                col(c).cast('int')
            )
        )
    return df

if __name__ == '__main__':
    args = parse_args()
    spark = create_spark()
    df = build_df(spark=spark,
                  args=args)
    output_dir = Path(f'{args.sample_dir}/df_sample')
    df.write.mode('overwrite').parquet(str(output_dir))
    spark.stop()
