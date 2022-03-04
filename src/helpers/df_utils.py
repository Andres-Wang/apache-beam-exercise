import pandas as pd


def get_index_list(path):
    df = pd.read_csv(path)
    return df.columns.to_list()

