
import os
from files_process.etls.utils import insert_row


def execute_store_procedure(log, logger, *args, **kwargs):
    """Execute a stored procedure in a MySQL database."""
    if 'validate_error' in kwargs and kwargs['validate_error'] and log[log["identifier"] == "error"].shape[0] > 0:
        logger.error("Error in previous step.")
        return log

    if "stored_procedure" not in kwargs:
        error_message = "The 'stored_procedure' argument is required."
        logger.error(error_message)
        log = insert_row(log, ["error", error_message])
        return log

    stored_procedure = kwargs["stored_procedure"]
    connection_string_env_var = kwargs.get("connection_string_env_var")

    if connection_string_env_var:
        connection_string = os.environ.get(connection_string_env_var)
        if not connection_string:
            error_message = f"Environment variable '{connection_string_env_var}' not set."
            logger.error(error_message)
            log = insert_row(log, ["error", error_message])
            return log
    elif "connection_string" in kwargs:
        connection_string = kwargs["connection_string"]
    else:
        error_message = "Either 'connection_string_env_var' or 'connection_string' argument is required."
        logger.error(error_message)
        log = insert_row(log, ["error", error_message])
        return log

    procedure_params = []
    if 'procedure_params' in kwargs and isinstance(kwargs['procedure_params'], list):
        procedure_params.extend(kwargs['procedure_params'])

    try:
        from sqlalchemy import create_engine

        engine = create_engine(connection_string)
        connection = engine.raw_connection()

        cursor = connection.cursor()
        cursor.callproc(stored_procedure, procedure_params)
        result = list(cursor.fetchall())
        cursor.close()
        connection.commit()
        [insert_row(log, row) for row in result]
        logger.info(f"Executed stored procedure {stored_procedure}")
    except Exception as e:
        error_message = f"Error executing stored procedure: {e}"
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
