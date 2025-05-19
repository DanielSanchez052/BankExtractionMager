import pandas as pd
from pandas import DataFrame

from files_process.etls.utils import insert_row


def clean_data(data: DataFrame, log: DataFrame, logger, *args, **kwargs) -> DataFrame:
    """Transforms the dataset into the desired structure and filters.

    DataFrame columns:
    - concept
    - nro
    - value
    - concept1
    - value1
    - mes
    """
    try:
        logger.info("Transformando el dataframe de resumen de movimientos")

        # Remove duplicates based on 'concept'
        data.drop_duplicates(subset=["concept"], inplace=True)

        # Replace empty strings with NaN
        data.replace(r"^\s*$", pd.NA, regex=True, inplace=True)

        # Replace NaN values with 0 in 'value' and 'value1' columns
        data["value"] = data["value"].fillna(0)
        data["value1"] = data["value1"].fillna(0)

        # Remove 'Nro' column
        data = data.drop(columns=["nro"])

        # Remove , from value and value1 columns
        data["value"] = data["value"].str.replace(",", "", regex=False)
        data["value1"] = data["value1"].str.replace(",", "", regex=False)

        # Convert 'value' and 'value1' columns to numeric
        data["value"] = pd.to_numeric(data["value"], errors="coerce")
        data["value1"] = pd.to_numeric(data["value1"], errors="coerce")

    except Exception as e:
        log = insert_row(log, ["error", f"Error al procesar {data}: {e}"])
        logger.error(f"Error al procesar {data}: {e}")
        data = pd.DataFrame(columns=data.columns)

    return data, log


def transform_data(data_frame: DataFrame, log: DataFrame, logger, *args, **kwargs) -> DataFrame:
    """Transform Dataset and compact in columns.

    DataFrame columns: concept, nro, value
    """
    try:
        new_rows = data_frame[['concept1', 'value1']].copy()
        new_rows.rename(columns={'concept1': 'concept', 'value1': 'value'}, inplace=True)

        data_frame = data_frame.drop(columns=['concept1', 'value1'])
        data_frame = pd.concat([data_frame, new_rows], ignore_index=True)

        # get month from kwargs and add it to the dataframe
        month = kwargs.get("month")
        year = kwargs.get("year")
        data_frame["month"] = f"{month}/{year}"

    except Exception as e:
        log = insert_row(log, ["error", f"Error al transformar {data_frame}: {e}"])
        logger.error(f"Error al transformar {data_frame}: {e}")
        data_frame = pd.DataFrame(columns=data_frame.columns)

    return data_frame, log
