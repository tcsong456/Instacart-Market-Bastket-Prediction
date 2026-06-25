import sys
import argparse
from pathlib import Path
from google.cloud import storage

ALLOWED_FILES = {
    "aisles.csv",
    "departments.csv",
    "orders.csv",
    "products.csv",
    "order_products__prior.csv",
    "order_products__train.csv",
}

def upload_to_gcs(
    project_id: str,
    bucket_name: str,
    local_data_dir: Path,
    overwrite: bool = False
):
    if not local_data_dir.exists():
        raise FileNotFoundError(f'local data dir does not exist: {local_data_dir}')
    
    upload_count = 0
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    for file in local_data_dir.glob('*.csv'):
        if file.name not in ALLOWED_FILES:
            print(f"Skipping non-ETL file: {file.name}")
            continue
        blob_path = f"{file.name}"
        blob = bucket.blob(blob_path)
        if blob.exists() and not overwrite:
            print(f"Skipping existing file: gs://{bucket_name}/{blob_path}")
            continue

        print(f"Uploading {blob_path}")
        blob.upload_from_filename(file)
        upload_count += 1
    
    if upload_count == 0:
        print('No files uploaded because no file change detected or no allowed files existed')
        
    print(f"Upload completed. Uploaded {upload_count} file(s).")

def parse_args():
    parser = argparse.ArgumentParser(
        description='Upload Instacart ETL CSV files to gcs raw bucket'
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project id"
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help='the name of the gcs bucket'
    )
    parser.add_argument(
        "--local-data-dir",
        required=True,
        help="the path for where the data is stored locally"
    )
    parser.add_argument(
        "--overwrite",
        action='store_true',
        help="whether the data already on the bucket can be replaced"
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    try:
        upload_to_gcs(
            project_id=args.project_id,
            bucket_name=args.bucket_name,
            local_data_dir=Path(args.local_data_dir),
            overwrite=args.overwrite
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)