gcloud dataproc jobs submit pyspark \
    src/ingestion/create_order_products.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --path=gs://instacart-data-8f061ed7/raw \
    --debug

gcloud dataproc jobs submit pyspark \
    src/ingestion/create_user_data.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-data-8f061ed7/raw/order_products \
    --output-dir=gs://instacart-curated-data-8f061ed7/user_data