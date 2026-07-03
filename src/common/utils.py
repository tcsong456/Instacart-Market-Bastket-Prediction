from pathlib import Path

def gcs_join(base_path: str | Path, filename: str) -> str:
    return str(base_path).rstrip("/") + "/" + filename
