import numpy as np
from sklearn.impute import SimpleImputer
from enum import Enum

from Algorithms.SystemClasses.DatasetParam import DatasetParam


class MissingValuesParam(Enum):
    INT = 0
    FLOAT = 1
    STR = 2
    NP_NAN = 3
    NONE = 4
    PD_NA = 5


class StrategyParam(Enum):
    MEAN = 0
    MEDIAN = 1
    MOST_FREQUENT = 2
    CONSTANT = 3

# Class name has to match the name of the file
class ScikitImputer:
    def __init__(self, missing_values: MissingValuesParam, strategy: StrategyParam, dataset: DatasetParam):
        self.missing_values = MissingValuesParam(missing_values).name.lower()
        self.strategy = StrategyParam(strategy).name.lower()
        self.dataset = dataset.data

    # Main function should be named Run
    def Run(self):
        imp = SimpleImputer(missing_values=MissingValuesParam[self.missing_values.upper()].value, strategy=self.strategy)
        fitted_dataset = imp.fit(self.dataset)
        return fitted_dataset.transform(self.dataset)

    def ParseKeyValuePair(self, key, value):
        if key is "int":
            return value
        elif key is "float":
            return value
        elif key is "string":
            return value
        elif key is "np_nan":
            return np.nan
        elif key is "none":
            return None
        elif key is "object":
            return value
