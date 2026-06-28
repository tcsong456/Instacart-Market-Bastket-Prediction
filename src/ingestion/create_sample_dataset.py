import shutil
import argparse
import pandas as pd
from pathlib import Path


LOOKUP_FILES = ["aisles.csv", "departments.csv", "products.csv"]
ORDER_PRODUCT_FILES = ["order_products__prior.csv", "order_products__train.csv"]


def sample_users(order: pd.DataFrame, sample_n: int, seed: int) -> pd.Series:
    return order['user_id'].drop_duplicates().sample(n=sample_n, random_state=seed)


def filter_orders_by_users(orders: pd.DataFrame, sample_users: pd.Series) -> pd.DataFrame:
    return orders[orders['user_id'].isin(sample_users)]


def write_filtered_order_products(
        raw_dir: Path,
        sample_dir: Path,
        chunksize: int,
        filename: str,
        order_ids: set[int]
        ) -> None:
    first_chunk = True
    for chunk in pd.read_csv(raw_dir / filename, chunksize=chunksize):
        filtered_chunk = chunk[chunk['order_id'].isin(order_ids)]
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(
                sample_dir / filename,
                index=False,
                header=first_chunk,
                mode='w' if first_chunk else 'a'
            )
            first_chunk = False


def copy_lookup_files(raw_dir: Path, sample_dir: Path) -> None:
    for filename in LOOKUP_FILES:
        shutil.copy2(raw_dir / filename, sample_dir / filename)


def arg_parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--raw-dir',
        required=True,
        type=Path
    )
    parser.add_argument(
        '--sample-dir',
        required=True,
        type=Path
    )
    parser.add_argument(
        '--chunk-size',
        default=1000,
        type=int
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42
    )
    parser.add_argument(
        '--sample-n',
        default=5,
        type=int
    )
    return parser.parse_args()


def main(
    raw_dir: Path,
    sample_dir: Path,
    chunk_size: int,
    seed: int,
    sample_n: int
) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(raw_dir / 'orders.csv')
    sample_user_ids = sample_users(orders, sample_n, seed)
    filtered_orders = filter_orders_by_users(orders, sample_user_ids)
    filtered_order_ids = set(filtered_orders['order_id'])
    filtered_orders.to_csv(sample_dir / 'orders.csv', index=False)

    for filename in ORDER_PRODUCT_FILES:
        write_filtered_order_products(
            raw_dir=raw_dir,
            sample_dir=sample_dir,
            chunksize=chunk_size,
            filename=filename,
            order_ids=filtered_order_ids
        )

    copy_lookup_files(raw_dir, sample_dir)


if __name__ == '__main__':
    args = arg_parse()
    main(raw_dir=args.raw_dir,
         sample_dir=args.sample_dir,
         chunk_size=args.chunk_size,
         seed=args.seed,
         sample_n=args.sample_n)
