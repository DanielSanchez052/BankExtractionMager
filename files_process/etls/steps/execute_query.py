
from files_process.etls.utils import insert_row


def execute_store_query(log, logger, *args, **kwargs):
    """Execute a stored query in a MySQL database."""
    if "connection_string" not in kwargs:
        raise ValueError("The 'connection_string' argument is required.")
    if "stored_query" not in kwargs:
        raise ValueError("The 'stored_query' argument is required.")

    connection_string = kwargs["connection_string"]
    stored_query = kwargs["stored_query"]

    try:
        from sqlalchemy import create_engine

        engine = create_engine(connection_string, encoding="utf-8")
        connection = engine.raw_connection()

        cursor = connection.cursor()
        cursor.execute(stored_query)
        cursor.close()
        connection.commit()

        logger.info(f"Executed stored query {stored_query}")
    except Exception as e:
        error_message = f"Error executing stored query: {e}"
        log = insert_row(log, ["error", error_message])
        logger.error(error_message)
    finally:
        try:
            connection.close()
        except Exception as e:
            error_message = f"Error closing connection: {e}"
            log = insert_row(log, ["error", error_message])
            logger.error(error_message)

    return log
