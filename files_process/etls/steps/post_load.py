import os
import shutil
from pandas import DataFrame
import datetime

from files_process.etls.utils import insert_row


def move_file(log: DataFrame, processed_dir: str, logger, *args, **kwargs) -> DataFrame:
    """Move processed files to specified directory"""
    if "identifier" in log.columns and "message" in log.columns and processed_dir:
        processed_files = log[log["identifier"] == "file_processed"]["message"].tolist()
        os.makedirs(processed_dir, exist_ok=True)
        for file_path in processed_files:
            if os.path.isfile(file_path):
                try:
                    prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    new_file_name = f"{prefix}_{os.path.basename(file_path)}"
                    shutil.move(file_path, os.path.join(processed_dir, os.path.basename(new_file_name)))
                except Exception as e:
                    logger.error(f"Error moving {file_path}: {e}")
    return log


def delete_file(log: DataFrame, logger, *args, **kwargs) -> DataFrame:
    if "identifier" in log.columns and "message" in log.columns:
        processed_files = log[log["identifier"] == "file_processed"]["message"].tolist()

        for file_path in processed_files:
            if os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"Successfully deleted {file_path}")
                    insert_row(log, ["error ", "Successfully file deleted"])
                except Exception as e:
                    error_message = f"Error deleting {file_path}: {e}"
                    logger.error(error_message)
                    insert_row(log, ["error ", error_message])
    return log
