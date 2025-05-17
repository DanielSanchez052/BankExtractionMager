from pandas import DataFrame
from sqlalchemy import create_engine


def save_to_sqlite(df: DataFrame, log: DataFrame, destination: str, connection_string: str, operation: str = "replace", *args, **kwargs):
    """ Loads data into a sqllite database"""
    try:
        print(f"Saving {len(df)} rows to {destination}")
        disk_engine = create_engine(connection_string)
        df.to_sql(destination, disk_engine, if_exists=operation)

    except Exception as e:
        print(f"Error al guardar en SQLite: {e}")
        log = log.append({"identifier": "error", "message": str(e)}, ignore_index=True)

    return log
