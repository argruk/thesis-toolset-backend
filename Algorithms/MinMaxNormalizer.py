from Algorithms.SystemClasses.DatasetParam import DatasetParam

class MinMaxNormalizer:
    def __init__(self, dataset: DatasetParam, column_name: str):
        self.dataset = dataset.data
        self.column_name = column_name

    def Run(self):
        d = self.dataset
        x = self.column_name
        self.dataset[self.column_name] = (d[x] - d[x].min())/(d[x].max() - d[x].min())
        return self.dataset

