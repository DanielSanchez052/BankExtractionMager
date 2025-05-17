import os
import shutil
from pandas import DataFrame


def post_load(log: DataFrame, processed_dir: str, logger) -> DataFrame:
    """Move processed files to specified directory"""
    if "identifier" in log.columns and "message" in log.columns and processed_dir:
        processed_files = log[log["identifier"] == "file_processed"]["message"].tolist()
        os.makedirs(processed_dir, exist_ok=True)
        for file_path in processed_files:
            if os.path.isfile(file_path):
                try:
                    shutil.move(file_path, os.path.join(processed_dir, os.path.basename(file_path)))
                except Exception as e:
                    logger.error(f"Error moving {file_path}: {e}")
    return log
