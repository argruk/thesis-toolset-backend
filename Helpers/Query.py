from pandas import DataFrame
import pandas as pd
from typing import Union


# Custom query may be added by extending this class with a custom query
class Query:
    def __init__(self, dataset: DataFrame):
        self.dataset: DataFrame = dataset

    def aggregate_by(self, columns: list, agg_type: list):
        self.dataset = self.dataset.agg(agg_type, columns)

    def filter_by(self, column: str, filter_param: Union[str, list]):
        if type(filter_param) == str: self.dataset = self.dataset[self.dataset[f"{column}"] == filter_param]
        if type(filter_param) == list: self.dataset = self.dataset[self.dataset[f"{column}"].isin(filter_param)]

    def select_columns(self, columns: list):
        all_columns = list(self.dataset.columns)
        self.dataset = self.dataset.drop([i for i in all_columns if i not in columns], axis=1)

    def select_time_window_for_column(self, window: str, measurement_type: Union[str, list, None] = None,
                                      group_type: Union[str, None] = None):
        if measurement_type is not None: self.filter_by("fragment.series", measurement_type)

        self.select_columns(["time", "fragment.series", "value"])
        self.dataset.set_index(["time", "fragment.series"], inplace=True, drop=True)
        grouping_in_progress = self.dataset.groupby(
            [pd.Grouper(freq=window, level="time"), pd.Grouper(level="fragment.series")])
        if group_type == "mean":
            self.dataset = grouping_in_progress.mean()
        elif group_type == "sum":
            self.dataset = grouping_in_progress.sum()
        elif group_type == "min":
            self.dataset = grouping_in_progress.min()
        elif group_type == "max":
            self.dataset = grouping_in_progress.max()
        elif group_type is None:
            self.dataset = grouping_in_progress.count()

        self.dataset.reset_index(inplace=True)

    def group_by(self, column_name: str, group_type: Union[str, None]):
        if group_type == "mean":
            self.dataset = self.dataset.groupby(column_name).mean()
        elif group_type == "sum":
            self.dataset = self.dataset.groupby(column_name).sum()
        elif group_type == "min":
            self.dataset = self.dataset.groupby(column_name).min()
        elif group_type == "max":
            self.dataset = self.dataset.groupby(column_name).max()
        elif group_type is None:
            self.dataset = self.dataset.groupby(column_name)[column_name].count()
