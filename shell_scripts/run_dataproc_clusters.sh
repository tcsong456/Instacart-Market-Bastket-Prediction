# python -m zipfile -c src.zip src
# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_order_products.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --path=gs://instacart-data-8f061ed7/raw \
#     --debug

# python -m zipfile -c src.zip src
# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_user_data.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --input-dir=gs://instacart-data-8f061ed7/raw/order_products \
#     --output-dir=gs://instacart-curated-data-8f061ed7/user_data

python -m zipfile -c src.zip src
gcloud dataproc jobs submit pyspark \
    src/ingestion/create_product_history_data.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-curated-data-8f061ed7 \
    --raw-dir=gs://instacart-data-8f061ed7/raw \
    --output-dir=gs://instacart-curated-data-8f061ed7/product_history_data