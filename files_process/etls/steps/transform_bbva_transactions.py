import pandas as pd

from files_process.etls.utils import insert_row


def clean_data(dataframe: pd.DataFrame, log: pd.DataFrame, logger, *args, **kwargs) -> pd.DataFrame:
    """Cleans and transforms the dataset into the desired structure and filters

        DataFrame columns:
          operation_date, concept, charges, credits, balance, month
    """

    try:
        logger.info("Transformando el dataframe de transacciones")
        # get month from kwargs and add it to the dataframe
        month = kwargs.get("month")
        year = kwargs.get("year")
        dataframe["month"] = f"{month}/{year}"

        # Remove duplicates
        dataframe.drop_duplicates(subset=["movement"], inplace=True)

        dataframe["extra_data"] = dataframe["movement"]
        dataframe.drop(columns=["movement", "value_date"], inplace=True)
        dataframe["bank"] = "BBVA"
        # Fill empty strings with NaN
        dataframe.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
        # Replace NaN values with 0 in numeric columns
        numeric_columns = ["charges", "credits", "balance"]
        for column in numeric_columns:
            dataframe[column] = dataframe[column].str.replace(",", "", regex=False)
            dataframe[column] = dataframe[column].fillna(0)
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    except Exception as e:
        log = insert_row(log, ["error", f"Error al procesar {dataframe}: {e}"])
        logger.error(f"Error al procesar {dataframe}: {e}")
        dataframe = pd.DataFrame(columns=dataframe.columns)

    return dataframe, log
