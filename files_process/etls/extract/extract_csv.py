import os
import pandas as pd
import glob

from files_process.etls.utils import insert_row


def extract(log: pd.DataFrame, *args, **kwargs) -> tuple:
    """Extracts data from CSV files and returns a DataFrame."""

    settings = kwargs

    if not os.path.exists(settings["process_dir"]):
        os.mkdir(settings["process_dir"])

    all_files = glob.glob(os.path.join(settings["process_dir"], "*.csv"))

    if not all_files:
        log = insert_row(log, ["warning", f"No se encontraron archivos CSV en el directorio {settings['process_dir']}."])
        return pd.DataFrame(), log

    dfs = []

    for file in all_files:
        try:
            df = pd.read_csv(file, sep=settings["separator"])
            dfs.append(df)
        except Exception as e:
            log = insert_row(log, ["error", f"Error al procesar {file}: {e}"])

    if not dfs:
        return pd.DataFrame(), log

    data = pd.concat(dfs, ignore_index=True)

    return data, log
