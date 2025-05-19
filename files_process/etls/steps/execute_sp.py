
from files_process.etls.utils import insert_row


def execute_store_procedure(log, logger, *args, **kwargs):
    """Execute a stored procedure in a MySQL database."""
    if 'validate_error' in kwargs and kwargs['validate_error'] and log[log["identifier"] == "error"].shape[0] > 0:
        logger.error("Error in previous step.")
        return log

    if "connection_string" not in kwargs:
        raise ValueError("The 'connection_string' argument is required.")
    if "stored_procedure" not in kwargs:
        raise ValueError("The 'stored_procedure' argument is required.")

    connection_string = kwargs["connection_string"]
    stored_procedure = kwargs["stored_procedure"]

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
