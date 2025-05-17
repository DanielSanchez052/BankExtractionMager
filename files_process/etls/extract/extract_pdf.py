import os
import glob
import pandas as pd
from files_process.etls.utils import insert_row
from files_process.extractors.pdf_extractor import PDFExtractor


def extract(log: pd.DataFrame, *args, **kwargs) -> tuple:
    """
    Extracts data from PDF files and returns a DataFrame.

    Args:
        log: A DataFrame to log the process.
        *args: Any additional positional arguments.
        **kwargs: Any additional keyword arguments.

    Keyword Args:
        filepath: The path to the PDF file to process.
        process_dir: The directory path to process all PDF files within.
        password: The password to decrypt the PDF file.
        column_mapping: A list of column names to map the extracted data to.

    Returns:
        A DataFrame with the extracted data, or an empty DataFrame if no data was extracted.
    """
    settings = kwargs
    files_to_process = []
    if "column_mapping" not in settings or not isinstance(settings["column_mapping"], list):
        log.error("Falta el parámetro obligatorio 'column_mapping' o no es una lista.")
        insert_row(log, ["error", "Falta el parámetro obligatorio 'column_mapping' o no es una lista."])
        return pd.DataFrame(), log

    if "filepath" in settings and os.path.isfile(settings["filepath"]):
        files_to_process.append(settings["filepath"])
    else:

        if "process_dir" not in settings:
            insert_row(log, ["error", "Falta el parámetro 'process_dir' en settings."])
            return pd.DataFrame(), log
        if not os.path.exists(settings["process_dir"]):
            os.mkdir(settings["process_dir"])

        all_files = glob.glob(os.path.join(settings["process_dir"], "*.pdf"))
        if not all_files:
            insert_row(log, ["warning", f"No se encontraron archivos PDF en el directorio {settings['process_dir']}."])
        files_to_process.extend(all_files)

    extractor = PDFExtractor()
    extracted_data = []

    def extract_and_filter(file):
        try:
            tables = extractor.extract_tables(file, settings.get("password"), flavor="network")
            filtered = [table for table in tables if table is not None and len(table.columns) == len(settings["column_mapping"])]
            insert_row(log, ["file_processed", file])
            return filtered
        except Exception as e:
            insert_row(log, ["warning", f"Error al extraer tablas del archivo {file}: {str(e)}"])
            return []

    for file in files_to_process:
        extracted_data.extend(extract_and_filter(file))

    df = pd.concat(extracted_data, ignore_index=True) if extracted_data else pd.DataFrame(columns=settings["column_mapping"])
    if df.empty:
        insert_row(log, ["error", "No se encontraron tablas válidas en los archivos PDF."])
    else:
        df.columns = settings["column_mapping"]

    return df, log
