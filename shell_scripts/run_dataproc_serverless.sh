gcloud storage rsync -r data/raw gs://instacart-data-8f061ed7/raw
gcloud storage ls gs://instacart-data-8f061ed7/raw

python -m pytest tests/unit_test/test_create_sample_dataset.py
python -m src.ingestion.create_sample_dataset \
    --raw-dir data/raw \
    --sample-dir data/sample \
    --chunk-size 1000000 \
    --seed 7872 \
    --sample-n 500

gcloud storage rsync -r data/sample gs://instacart-data-8f061ed7/sample
gcloud storage ls gs://instacart-data-8f061ed7/sample

python -m zipfile -c src.zip src
gcloud storage cp src.zip gs://instacart-dataproc-staging-8f061ed7/packages/src.zip
gcloud storage cp src/ingestion/create_order_products.py gs://instacart-dataproc-staging-8f061ed7/jobs/

gcloud dataproc batches submit pyspark \
    gs://instacart-dataproc-staging-8f061ed7/jobs/create_order_products.py \
    --region=europe-west1 \
    --service-account=dataproc-etl-sa@instacart-basket.iam.gserviceaccount.com \
    --py-files=gs://instacart-dataproc-staging-8f061ed7/packages/src.zip \
    -- \
    --path=gs://instacart-data-8f061ed7/raw \
    --debug

gcloud storage cp src/ingestion/create_user_data.py gs://instacart-dataproc-staging-8f061ed7/jobs/
gcloud dataproc batches submit pyspark \
    gs://instacart-dataproc-staging-8f061ed7/jobs/create_user_data.py \
    --region=europe-west1 \
    --service-account=dataproc-etl-sa@instacart-basket.iam.gserviceaccount.com \
    --py-files=gs://instacart-dataproc-staging-8f061ed7/packages/src.zip \
    -- \
    --input-dir=gs://instacart-data-8f061ed7/raw/order_products \
    --output-dir=gs://instacart-curated-data-8f061ed7/user_data