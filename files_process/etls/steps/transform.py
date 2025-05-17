from pandas import DataFrame


def step_1(data: DataFrame, log: DataFrame, *args, **kwargs) -> DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = data[data.gender.isin(["F"])]
    return df, log
