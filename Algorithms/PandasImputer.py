from Algorithms.SystemClasses.DatasetParam import DatasetParam

# Class name has to match the name of the file
class PandasImputer:
    def __init__(self, dataset: DatasetParam, replace_value: int, column_name: str):
        self.dataset = dataset.data
        self.replace_value = replace_value
        self.column_name = column_name

    # Main function should be named Run
    def Run(self):
        return self.dataset.replace(self.replace_value, self.dataset[self.column_name].mean())
