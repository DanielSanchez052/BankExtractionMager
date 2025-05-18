import pandas as pd

from files_process.etls.utils import insert_row


def clean_data(dataframe: pd.DataFrame, log: pd.DataFrame, logger, *args, **kwargs) -> pd.DataFrame:
    """Cleans and transforms the dataset into the desired structure and filters

        DataFrame columns:
          Movimiento, Fecha, Operacion, Fecha, Valor, Concepto, Cargos, Abonos, Saldo
    """

    try:
        logger.info("Transformando el dataframe de transacciones")
        # Remove duplicates
        dataframe.drop_duplicates(subset=["Movimiento"], inplace=True)
        # Fill empty strings with NaN
        dataframe.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
        # Replace NaN values with 0 in numeric columns
        numeric_columns = ["Cargos", "Abonos", "Saldo"]
        for column in numeric_columns:
            dataframe[column] = dataframe[column].fillna(0)
        # # Convert columns to numeric
        # for column in numeric_columns:
        #     dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    except Exception as e:
        log = insert_row(log, ["error", f"Error al procesar {dataframe}: {e}"])
        logger.error(f"Error al procesar {dataframe}: {e}")
        dataframe = pd.DataFrame(columns=dataframe.columns)

    return dataframe, log
