from enum import Enum
import numpy as np

from Algorithms.SystemClasses.DatasetParam import DatasetParam

class LogType(Enum):
    BINARY = 'BINARY'
    DECIMAL = 'DECIMAL'
    NATURAL = 'NATURAL'

# Requires: numpy
class LogarithmicNormalizer:
    def __init__(self, dataset: DatasetParam, log_type: LogType, column_name: str):
        self.dataset = dataset.data
        self.log_type = LogType[log_type]
        self.column_name = column_name

    def Run(self):
        print(LogType.DECIMAL, self.log_type)
        if self.log_type == LogType.BINARY:
            self.dataset[self.column_name] = np.log2(self.dataset[self.column_name])
        elif self.log_type == LogType.DECIMAL:
            self.dataset[self.column_name] = np.log10(self.dataset[self.column_name])
        elif self.log_type == LogType.NATURAL:
            self.dataset[self.column_name] = np.log(self.dataset[self.column_name])
        else:
            pass
        return self.dataset