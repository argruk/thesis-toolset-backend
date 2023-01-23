from Algorithms.SystemClasses.DatasetParam import DatasetParam


class ReplaceValueParam:
    value: int


class ColumnNameParam:
    value: str


# Class name has to match the name of the file
class PandasImputer:
    def __init__(self, dataset: DatasetParam, replace_value: int, column_name: str):
        self.dataset = dataset.data
        self.replace_value = replace_value.value
        self.column_name = column_name.value

    # Main function should be named Run
    def Run(self):
        return self.dataset[self.column_name].replace(self.replace_value, self.dataset[self.column_name].mean())
