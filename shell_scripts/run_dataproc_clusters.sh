# gcloud storage rsync -r data/raw gs://instacart-raw-8f061ed7/raw
# gcloud storage ls gs://instacart-raw-8f061ed7/raw

file=$1
if [[ "$file" == "curated" ]]; then
    bronze_input_level="raw"
else
    bronze_input_level="sample"
fi

# python -m src.ingestion.create_sample_dataset \
#     --raw-dir data/raw \
#     --sample-dir data/sample \
#     --chunk-size 1000000 \
#     --seed 7872 \
#     --sample-n 500

# gcloud storage rsync -r data/sample gs://instacart-raw-8f061ed7/sample
# gcloud storage ls gs://instacart-raw-8f061ed7/sample

python -m zipfile -c src.zip src
# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_bronze_datasets.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --input-dir gs://instacart-raw-8f061ed7/$bronze_input_level \
#     --output-dir gs://instacart-bronze-8f061ed7/$file

# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_order_products.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --input-dir=gs://instacart-bronze-8f061ed7/$file \
#     --output-dir=gs://instacart-silver-8f061ed7/$file

# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_user_data.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --input-dir=gs://instacart-silver-8f061ed7/$file/order_products \
#     --output-dir=gs://instacart-silver-8f061ed7/$file/user_data

# gcloud dataproc jobs submit pyspark \
#     src/ingestion/create_product_history_data.py \
#     --cluster=instacart-dataproc-cluster-8f061ed7 \
#     --region=europe-west1 \
#     --py-files=src.zip \
#     -- \
#     --input-dir=gs://instacart-silver-8f061ed7/$file \
#     --raw-dir=gs://instacart-bronze-8f061ed7/$file \
#     --output-dir=gs://instacart-silver-8f061ed7/$file/product_history_data

gcloud dataproc jobs submit pyspark \
    src/ingestion/create_aisle_history_data.py \
    --cluster=instacart-dataproc-cluster-8f061ed7 \
    --region=europe-west1 \
    --py-files=src.zip \
    -- \
    --input-dir=gs://instacart-silver-8f061ed7/$file \
    --raw-dir=gs://instacart-bronze-8f061ed7/$file \
    --output-dir=gs://instacart-silver-8f061ed7/$file