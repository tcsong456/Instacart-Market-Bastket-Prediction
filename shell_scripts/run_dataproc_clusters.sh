gcloud storage rsync -r data/raw gs://instacart-raw-8f061ed7/raw
gcloud storage ls gs://instacart-raw-8f061ed7/raw

python -m pytest \
    tests/unit_test/test_create_sample_dataset.py \
    tests/integration_test/test_create_sample_dataset_pipeline.py
python -m src.ingestion.create_sample_dataset \
    --raw-dir data/raw \
    --sample-dir data/sample \
    --chunk-size 1000000 \
    --seed 7872 \
    --sample-n 500

gcloud storage rsync -r data/sample gs://instacart-raw-8f061ed7/sample
gcloud storage ls gs://instacart-raw-8f061ed7/sample

python -m pytest tests/integration_test/test_create_bronze_datasets_pipeline.py
python -m zipfile -c src.zip src
gcloud dataproc jobs submit pyspark \
    src/ingestion/create_bronze_datasets.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir gs://instacart-raw-8f061ed7/raw \
    --output-dir gs://instacart-bronze-8f061ed7/raw

gcloud dataproc jobs submit pyspark \
    src/ingestion/create_order_products.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-bronze-8f061ed7/raw \
    --output-dir=gs://instacart-silver-8f061ed7/raw

gcloud dataproc jobs submit pyspark \
    src/ingestion/create_user_data.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-silver-8f061ed7/raw/order_products \
    --output-dir=gs://instacart-silver-8f061ed7/raw/user_data

gcloud dataproc jobs submit pyspark \
    src/ingestion/create_product_history_data.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-silver-8f061ed7/raw \
    --raw-dir=gs://instacart-bronze-8f061ed7/raw \
    --output-dir=gs://instacart-silver-8f061ed7/raw/product_history_data