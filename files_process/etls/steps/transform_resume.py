import pandas as pd
from pandas import DataFrame

from files_process.etls.utils import insert_row


def clean_data(data: DataFrame, log: DataFrame, logger, *args, **kwargs) -> DataFrame:
    """Transforms the dataset into the desired structure and filters.

    DataFrame columns:
    - Resumen Movimientos
    - Nro
    - Valor
    - Descripcion
    - Valor1
    - mes
    """
    try:
        logger.info("Transformando el dataframe de resumen de movimientos")

        # Remove duplicates based on 'Resumen Movimientos'
        data.drop_duplicates(subset=["Resumen Movimientos"], inplace=True)

        # Replace empty strings with NaN
        data.replace(r"^\s*$", pd.NA, regex=True, inplace=True)

        # Replace NaN values with 0 in 'Valor' and 'Valor1' columns
        data["Valor"] = data["Valor"].fillna(0)
        data["Valor1"] = data["Valor1"].fillna(0)

        # Remove 'Nro' column
        data = data.drop(columns=["Nro"])

        # Remove , from Valor and Valor1 columns
        data["Valor"] = data["Valor"].str.replace(",", "", regex=False)
        data["Valor1"] = data["Valor1"].str.replace(",", "", regex=False)

        # Convert 'Valor' and 'Valor1' columns to numeric
        data["Valor"] = pd.to_numeric(data["Valor"], errors="coerce")
        data["Valor1"] = pd.to_numeric(data["Valor1"], errors="coerce")

    except Exception as e:
        log = insert_row(log, ["error", f"Error al procesar {data}: {e}"])
        logger.error(f"Error al procesar {data}: {e}")
        data = pd.DataFrame(columns=data.columns)

    return data, log


def transform_data(data_frame: DataFrame, log: DataFrame, logger, *args, **kwargs) -> DataFrame:
    """Transform Dataset and compact in columns.

    DataFrame columns: Resumen Movimientos, Nro, Valor
    """
    try:
        new_rows = data_frame[['Descripcion', 'Valor1']].copy()
        new_rows.rename(columns={'Descripcion': 'Resumen Movimientos', 'Valor1': 'Valor'}, inplace=True)

        data_frame = data_frame.drop(columns=['Descripcion', 'Valor1'])
        data_frame = pd.concat([data_frame, new_rows], ignore_index=True)

        # get month from kwargs and add it to the dataframe
        month = kwargs.get("month")
        year = kwargs.get("year")
        data_frame["mes"] = f"{month}/{year}"

    except Exception as e:
        log = insert_row(log, ["error", f"Error al transformar {data_frame}: {e}"])
        logger.error(f"Error al transformar {data_frame}: {e}")
        data_frame = pd.DataFrame(columns=data_frame.columns)

    return data_frame, log
