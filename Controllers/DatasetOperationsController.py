from fastapi import APIRouter, Query

from Helpers.Query import Query as QueryDataset
from Helpers.DatasetLoader import DatasetLoader
from typing import Union, List
from pydantic import BaseModel
import json

router = APIRouter()

class SelectColumnsObject(BaseModel):
    columns: list

@router.get("/dataset/transform/group")
async def group_dataset(dataset_name: str, column_name: str, group_type: Union[str, None] = None):
    loader = DatasetLoader("./SavedDatasets")
    query = QueryDataset(loader.load_dataset_by_name(dataset_name))
    if query.dataset.empty:
        return json.loads("[]")
    query.group_by(column_name, group_type)
    return json.loads(query.dataset.to_json())


@router.get("/dataset/transform/aggregate")
async def aggregate_dataset(dataset_name: str, column_name: str, group_type: Union[str, None] = None):
    loader = DatasetLoader("./SavedDatasets")
    query = QueryDataset(loader.load_dataset_by_name(dataset_name))
    if query.dataset.empty:
        return json.loads("[]")
    query.aggregate_by(column_name, group_type)
    return json.loads(query.dataset.to_json())


@router.get("/dataset/transform/select/columns")
async def select_columns(dataset_name: str, body: SelectColumnsObject):
    loader = DatasetLoader("./SavedDatasets")
    query = QueryDataset(loader.load_dataset_by_name(dataset_name))
    if query.dataset.empty:
        return json.loads("[]")
    query.select_columns(body.columns)
    return json.loads(query.dataset.to_json())


@router.get("/dataset/transform/window")
async def window_dataset(dataset_name: str, window: str, measurement_type: Union[str, None] = None, group_type: Union[str, None] = None):
    loader = DatasetLoader("./SavedDatasets")
    query = QueryDataset(loader.load_dataset_by_name(dataset_name))
    if query.dataset.empty:
        return json.loads("[]")
    parsed_measurement_type = None
    if measurement_type is not None:
        parsed_measurement_type = json.loads(measurement_type)

    query.select_time_window_for_column(window, parsed_measurement_type, group_type)

    return json.loads(query.dataset.to_json())


@router.get("/dataset/transform/filter")
async def filter_dataset(dataset_name: str, column_name: str, filter_param: Union[str, list]):
    loader = DatasetLoader("./SavedDatasets")
    query = QueryDataset(loader.load_dataset_by_name(dataset_name))
    if query.dataset.empty:
        return json.loads("[]")
    query.filter_by(column_name, filter_param)
    return json.loads(query.dataset.to_json())
