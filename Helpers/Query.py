from pandas import DataFrame
import pandas as pd
from typing import Union

# Custom query may be added by extending this class with a custom query
from Helpers.DatasetLoader import DatasetLoader


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
        pd.to_datetime(self.dataset['time'])

        testdf_dates = pd.date_range(start=self.dataset.head(1)['time'].values[0],
                                     end=self.dataset.tail(1)['time'].values[0], freq=window).tz_localize('UTC').floor(window)
        testdf_dates = testdf_dates.append(pd.DatetimeIndex([pd.Timestamp(testdf_dates[len(testdf_dates)-1]) + pd.Timedelta(window)]))
        index = pd.MultiIndex.from_product([list(testdf_dates), measurement_type], names=['time', 'fragment.series'])
        testdf = DataFrame(columns=['value'], index=index)

        self.dataset.set_index(["time", "fragment.series"], inplace=True, drop=True)
        grouping_in_progress = self.dataset.groupby(
            [pd.Grouper(freq=window, level="time", dropna=False), "fragment.series"], dropna=False)
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

        testdf.loc[self.dataset.index.to_native_types(), ['value']] = self.dataset.values
        DatasetLoader.save_current_dataset_state('test1', testdf, add_index=True)

        testdf.reset_index(inplace=True)

        self.dataset = testdf

    def group_by(self, column_name: str, group_type: Union[str, None]):
        if group_type == "mean":
            self.dataset = self.dataset.groupby(column_name).mean()
        elif group_type == "sum":
            self.dataset = self.dataset.groupby(column_name).sum()
        elif group_type == "min":
            self.dataset = self.dataset.groupby(column_name).min()
        elif group_type == "max":
            self.dataset = self.dataset.groupby(column_name).max()
        elif group_type == "mt" and column_name == 'device_name':
            self.dataset = self.__group_by_mt_ext()
        elif group_type == "mt" and column_name == 'fragment.series':
            self.dataset = self.__group_by_mt_ext_reverse()
        elif group_type is None:
            self.dataset = self.dataset.groupby(column_name)[column_name].count()

    def __group_by_mt_ext(self):
        grouped_dataset = DataFrame(self.dataset.groupby(['source', 'device_name'])['fragment.series'].apply(lambda y: ', '.join(y.unique())))
        grouped_dataset.reset_index(inplace=True)
        grouped_dataset['device'] = grouped_dataset['device_name'] + '(' + grouped_dataset['source'].astype(str) + ')'
        grouped_dataset = grouped_dataset[['device', 'fragment.series']]
        grouped_dataset.set_index('device', inplace=True, drop=True)
        return grouped_dataset['fragment.series']

    @staticmethod
    def __y_ext_1(y):
        y['device'] = y['device_name'] + '(' + y['source'].astype(str) + ')'
        return ', '.join(y[['fragment.series', 'device']]['device'].unique())

    def __group_by_mt_ext_reverse(self):
        grouped_dataset = DataFrame(self.dataset.groupby('fragment.series')['fragment.series', 'source', 'device_name'].apply(lambda y: self.__y_ext_1(y)), columns=['device_name'])
        return grouped_dataset['device_name']
