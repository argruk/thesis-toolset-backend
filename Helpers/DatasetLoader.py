import glob
import pandas as pd
from pathlib import Path

class DatasetLoader:
    def __init__(self, folder_path):
        self.dataset = pd.DataFrame()
        self.data_path = f"{folder_path}/*.csv"

    def load_data(self):
        files = glob.glob(self.data_path)
        datasets = []

        for f in files:
            read_file = pd.read_csv(f)
            datasets.append(read_file)

        df = pd.concat(datasets, axis=0, ignore_index=True)
        return df

    def load_dataset_list(self):
        files = glob.glob(self.data_path)
        datasets = []

        for f in files:
            datasets.append(Path(f).stem)
        return datasets

    def load_dataset_sample(self, dataset_name: str, num_of_rows=1_000):
        dataset = self.load_dataset_by_name(dataset_name)
        return dataset[:num_of_rows].to_json()

    @staticmethod
    def load_dataset_by_name(dataset_name):
        df = pd.DataFrame()
        with open(f"SavedDatasets/{dataset_name}.csv", "r") as f:
            df = pd.read_csv(f)

        return df

    @staticmethod
    def save_current_dataset_state(dataset_name, dataset: pd.DataFrame, add_index=False, as_json=False):
        if as_json:
            dataset.to_json(f"SavedDatasets/{dataset_name}.json", index=add_index)
        else:
            dataset.to_csv(f"SavedDatasets/{dataset_name}.csv", index=add_index)
