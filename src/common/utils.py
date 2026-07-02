def gcs_join(base_path: str, filename: str) -> str:
    return base_path.rstrip("/") + "/" + filename
