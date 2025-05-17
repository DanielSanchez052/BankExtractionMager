from pandas import DataFrame
from sqlalchemy import create_engine


def save_to_sqlite(df: DataFrame, log: DataFrame, destination: str, connection_string: str, operation: str = "replace"):
    """ Loads data into a sqllite database"""
    if len(log) <= 0:
        print(f"Saving {len(df)} rows to {destination}")
        disk_engine = create_engine(connection_string)
        df.to_sql(destination, disk_engine, if_exists=operation)
