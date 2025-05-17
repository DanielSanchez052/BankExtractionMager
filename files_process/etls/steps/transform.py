from pandas import DataFrame
from pipeline import insert_row


def step_1(data: DataFrame, log: DataFrame, *args, **kwargs) -> DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = data[data.gender.isin(["Female"])]
    return df, log
