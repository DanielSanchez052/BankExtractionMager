import os
from pandas import DataFrame
from sqlalchemy import create_engine

from files_process.etls.utils import insert_row


def save_to_sql(dataframe: DataFrame, log: DataFrame, logger, *args, **kwargs):
    """Load data into a Mysql database."""

    if "destination" not in kwargs:
        error_message = "The 'destination' argument is required."
        logger.error(error_message)
        log = insert_row(log, ["error", error_message])
        return None, log

    destination = kwargs["destination"]
    operation = kwargs.get("operation", "replace")
    connection_string_env_var = kwargs.get("connection_string_env_var")

    if connection_string_env_var:
        connection_string = os.environ.get(connection_string_env_var)
        if not connection_string:
            error_message = f"Environment variable '{connection_string_env_var}' not set."
            logger.error(error_message)
            log = insert_row(log, ["error", error_message])
            return None, log
    elif "connection_string" in kwargs:
        connection_string = kwargs["connection_string"]
    else:
        error_message = "Either 'connection_string_env_var' or 'connection_string' argument is required."
        logger.error(error_message)
        log = insert_row(log, ["error", error_message])
        return None, log

    try:
        engine = create_engine(connection_string)
        logger.info(f"Saving {len(dataframe)} rows to {destination}")
        dataframe.to_sql(destination, engine, if_exists=operation, index=False)
        insert_row(log, ["success", f"Saved {len(dataframe)} rows to {destination}"])
    except Exception as e:
        error_message = f"Error saving to SQL: {e}"
        logger.error(error_message)
        log = log.append({"identifier": "error", "message": error_message}, ignore_index=True)

    return None, log
