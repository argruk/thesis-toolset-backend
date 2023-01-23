import inspect
import sys
from enum import Enum
import glob
import importlib

from Algorithms.SystemClasses.DatasetParam import DatasetParam
from Helpers.DatasetLoader import DatasetLoader


class AlgorithmHelper:
    @staticmethod
    def GetEnumValues(enum: Enum):
        return [e.name for e in enum]

    def GetAvailableAlgorithms(self):
        res = glob.glob("Algorithms/*.py")
        algorithms = [f.split("\\")[1] for f in res]
        return [self.TrimFileExtension(alg) for alg in algorithms]

    def GetAlgorithmAsModule(self, filename, body):
        module = importlib.import_module(f"Algorithms.{self.TrimFileExtension(filename)}")
        members = inspect.getmembers(sys.modules[module.__name__], inspect.isclass)
        found_class = list(filter(lambda x: x[0] == self.TrimFileExtension(filename), members))[0][1]

        dataset = DatasetLoader.load_dataset_by_name("dataset1")

        print(dataset)
        parameters = {"dataset": DatasetParam(dataset), **body.parameters}
        instance = found_class(**parameters)

        return instance.Run()

    def GetAlgorithmParameters(self, filename):
        module = importlib.import_module(f"Algorithms.{self.TrimFileExtension(filename)}")
        members = inspect.getmembers(sys.modules[module.__name__], inspect.isclass)
        instance = list(filter(lambda x: x[0] == self.TrimFileExtension(filename), members))[0][1]
        parameters = inspect.signature(instance.__init__).parameters

        dict_of_parameters = dict()

        for param in parameters.values():
            if param.name == "self":
                pass
            elif issubclass(parameters[param.name].annotation, Enum):
                dict_of_parameters[param.name] = self.GetEnumValues(parameters[param.name].annotation)
            else:
                dict_of_parameters[param.name] = f"{self.ExtractParameterClass(parameters[param.name].__str__())}"

        return dict_of_parameters

    def TrimFileExtension(self, filename):
        return filename.split('.')[0]

    def ExtractParameterClass(self, param):
        return param.split(" ")[1]
