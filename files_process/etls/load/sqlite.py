from pandas import DataFrame
from sqlalchemy import create_engine


def save_to_sqlite(dataframe: DataFrame, log: DataFrame, logger, *args, **kwargs):
    """Load data into a SQLite database."""

    if "destination" not in kwargs:
        raise ValueError("The 'destination' argument is required.")
    if "connection_string" not in kwargs:
        raise ValueError("The 'connection_string' argument is required.")

    destination = kwargs["destination"]
    connection_string = kwargs["connection_string"]
    operation = kwargs.get("operation", "replace")

    engine = create_engine(connection_string)

    try:
        logger.info(f"Saving {len(dataframe)} rows to {destination}")
        dataframe.to_sql(destination, engine, if_exists=operation)
    except Exception as e:
        error_message = f"Error saving to SQLite: {e}"
        logger.error(error_message)
        log = log.append({"identifier": "error", "message": error_message}, ignore_index=True)

    return log
