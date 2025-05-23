import pandas as pd
from files_process.etls.utils import insert_row


def clean_data(dataframe: pd.DataFrame, log: pd.DataFrame, logger, *args, **kwargs) -> pd.DataFrame:
    """
    Cleans and transforms the dataset into the desired structure and filters

    DataFrame columns:
      operation_date, description, value, balance, month
    """

    try:
        # Get month from kwargs and add it to the dataframe
        try:
            dataframe["month"] = f"{kwargs.get('month')}/{kwargs.get('year')}"
        except Exception as e:
            log = insert_row(log, ["error", f"Error adding month to dataframe: {e}"])
            logger.error(f"Error adding month to dataframe: {e}")
            dataframe["month"] = ""

        # Fill empty strings with NaN
        try:
            dataframe = dataframe.replace(r"^\s*$", pd.NA, regex=True)
        except Exception as e:
            log = insert_row(log, ["error", f"Error replacing empty strings with NaN: {e}"])
            logger.error(f"Error replacing empty strings with NaN: {e}")

        # Replace NaN values with 0 in numeric columns
        numeric_columns = ["value", "balance"]
        for column in numeric_columns:
            try:
                dataframe[column] = dataframe[column].str.replace(",", "", regex=False)
                dataframe[column] = dataframe[column].str.replace("$", "", regex=False)
                dataframe[column] = dataframe[column].fillna(0)
                dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
                dataframe = dataframe[pd.notna(dataframe[column])]
            except Exception as e:
                log = insert_row(log, ["error", f"Error processing numeric column {column}: {e}"])
                logger.error(f"Error processing numeric column {column}: {e}")

        # Convert description and month to text
        try:
            dataframe["description"] = dataframe["description"].astype(str)
            dataframe["month"] = dataframe["month"].astype(str)
        except Exception as e:
            log = insert_row(log, ["error", f"Error converting description and month to text: {e}"])
            logger.error(f"Error converting description and month to text: {e}")

    except Exception as e:
        log = insert_row(log, ["error", f"Error al procesar {dataframe}: {e}"])
        logger.error(f"Error al procesar {dataframe}: {e}")
        dataframe = pd.DataFrame(columns=dataframe.columns)

    return dataframe, log


def transform_transactions(dataframe: pd.DataFrame, log: pd.DataFrame, logger, *args, **kwargs) -> pd.DataFrame:
    """
        input DataFrame columns:
          operation_date, description, value, balance, month
        output DataFrame columns:
         operation_date, concept, charges, credits, balance, bank, extra_data, month
    """
    try:

        dataframe["concept"] = dataframe["description"]
        dataframe["charges"] = dataframe["value"].apply(lambda x: x if x <= 0 else 0)
        dataframe["credits"] = dataframe["value"].apply(lambda x: x if x > 0 else 0)
        dataframe["balance"] = dataframe["balance"]
        dataframe["bank"] = "Nequi"
        dataframe["extra_data"] = ""
        dataframe = dataframe.drop(columns=["description", "value"])
        dataframe["charges"] = dataframe["charges"].apply(lambda x: abs(x) if x < 0 else x)
    except Exception as e:
        log = insert_row(log, ["error", f"Error al transformar {dataframe}: {e}"])
        logger.error(f"Error al transformar {dataframe}: {e}")
        dataframe = pd.DataFrame(columns=dataframe.columns)
    return dataframe, log
