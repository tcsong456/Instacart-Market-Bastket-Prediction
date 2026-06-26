import pandas as pd
from pathlib import Path

RAW_DIR = Path('data')
SAMPLE_DIR = Path('data/sample')
SAMPLE_N = 500

SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

orders = pd.read_csv(RAW_DIR / 'orders.csv')
sample_users = (
    orders['user_id']
    .drop_duplicates()
    .sample(n=SAMPLE_N, random_state=19209)
)

orders_sample = orders[orders['user_id'].isin(sample_users)]
sample_order_ids = set(orders_sample['order_id'])
orders_sample.to_csv(SAMPLE_DIR / 'orders.csv', index=False)

for filename in ['order_products__prior.csv', 'order_products__train.csv']:
    first_chunk = True
    for chunk in pd.read_csv(RAW_DIR / filename, chunksize=1e6):
        filtered_chunk = chunk[chunk['order_id'].isin(sample_order_ids)]
        filtered_chunk.to_csv(
            SAMPLE_DIR / filename,
            index=False,
            mode='w' if first_chunk else 'a',
            header=first_chunk
        )
        first_chunk=False

for filename in ['aisles.csv', 'departments.csv', 'products.csv']:
    df = pd.read_csv(RAW_DIR / filename)
    df.to_csv(SAMPLE_DIR / filename, index=False)