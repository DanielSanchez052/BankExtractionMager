import os
import glob
import pandas as pd
from files_process.etls.utils import insert_row
from files_process.extractors.pdf_extractor import PDFExtractor


def extract(log: pd.DataFrame, logger, *args, **kwargs) -> tuple:
    """Extracts data from PDF files and returns a DataFrame."""
    settings = kwargs
    files_to_process = []

    if "column_mapping" not in settings or not isinstance(settings["column_mapping"], list):
        logger.error("Missing required parameter 'column_mapping' or it is not a list.")
        insert_row(log, ["error", "Missing required parameter 'column_mapping' or it is not a list."])
        return pd.DataFrame(), log

    if "filepath" in settings and os.path.isfile(settings["filepath"]):
        files_to_process.append(settings["filepath"])

    if "process_dir" in settings:
        process_dir = settings["process_dir"]
        if not os.path.exists(process_dir):
            os.mkdir(process_dir)

        all_files = glob.glob(os.path.join(process_dir, "*.pdf"))
        if not all_files:
            logger.warning(f"No PDF files found in directory {process_dir}.")
            insert_row(log, ["warning", f"No PDF files found in directory {process_dir}."])

        files_to_process.extend(all_files)

    extractor = PDFExtractor()
    extracted_data = []

    for file in files_to_process:
        try:
            tables = extractor.extract_tables(file, settings.get("password"), flavor="network")
            filtered = [table for table in tables if table is not None and len(table.columns) == len(settings["column_mapping"])]
            logger.info(f"Processed file {file}")
            extracted_data.extend(filtered)
        except Exception as e:
            logger.warning(f"Error extracting tables from file {file}: {str(e)}")
            insert_row(log, ["error", f"Error extracting tables from file {file}: {str(e)}"])

    df = pd.concat(extracted_data, ignore_index=True) if extracted_data else pd.DataFrame(columns=settings["column_mapping"])
    if df.empty:
        logger.error("No valid tables found in PDF files.")
        insert_row(log, ["error", "No valid tables found in PDF files."])
    else:
        df.columns = settings["column_mapping"]

    return df, log
