from typing import List
import pandas as pd


def insert_row(df: pd.DataFrame, row: List):
    insert_loc = df.index.max()

    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

    return df
