from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import uvicorn
import json
from pydantic import BaseModel

from typing import Union

from Controllers import DatasetOperationsController
from Helpers.AlgorithmRunner import AlgorithmHelper
from Helpers.CumulocityFetcher import CumulocityFetcher
from Helpers.DatasetLoader import DatasetLoader
from Helpers.NotificationFetcher import NotificationFetcher
from Helpers.RabbitMQClient import RabbitMQClient


class Body(BaseModel):
    parameters: dict


app = FastAPI(debug=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(DatasetOperationsController.router)

f = open('credentials.json')
credentials = json.load(f)
f.close()

username = credentials['USERNAME']
password = credentials['PASSWORD']
tenant_url = credentials['TENANT_URL']


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.websocket_route("/ws")
async def measurement_stream(websocket: WebSocket):
    conn = NotificationFetcher(platform=tenant_url, username=username, password=password)
    token = conn.create_token().get('token')
    session = aiohttp.ClientSession()
    async with session.ws_connect(f"wss://{tenant_url}/notification2/consumer/?token={token}") as ws:
        await websocket.accept()
        async for msg in ws:
            await websocket.send_text(msg.data)
    await websocket.close()
    await ws.close()
    await session.close()


@app.post("/notification/subscribe")
async def new_subscription(deviceId: int, type: str):
    conn = NotificationFetcher(platform=tenant_url, username=username, password=password)
    return conn.add_subscription(deviceId, type)


@app.get("/devices")
async def get_devices():
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_all_devices()


@app.get("/search/devices")
async def get_devices(type: str, id: str = "", name: str = ""):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    if type == 'name':
        return conn.get_device_by_name(name)
    elif type == 'id':
        return conn.get_device_by_id(id)
    else:
        return []


@app.get("/search/devices/wrong")
async def get_wrong_devices(type: str, id: str = "", name: str = ""):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    if type == 'name':
        return conn.get_device_by_name_wrong(name)
    elif type == 'id':
        return conn.get_device_by_id(id)
    else:
        return []


@app.get("/devices/{device_id}")
async def get_devices(device_id: str):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_single_device(device_id)


@app.get("/devices/{device_id}/measurements")
async def get_device_meaurements(device_id: str, type: str = "", pageSize: int = 10):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_measurements_for_device(device_id, type, pageSize)


@app.get("/devices/{device_id}/measurements/types")
async def get_device_meaurement_types(device_id: str):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_measurement_types_for_device(device_id)


@app.get("/devices/{device_id}/children")
async def get_device_children(device_id: str):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    # return conn.get_device_children(device_id)
    return RecursiveDeviceTree(device_id, "device_name", conn)


@app.get("/measurements/ranged")
async def get_measurements_ranged(date_from, date_to):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_all_measurements_ranged(date_from, date_to)


@app.get("/measurements/series")
async def get_measurements_series(date_from, date_to):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_measurement_series(date_from, date_to)


@app.get("/algorithms")
async def get_all_algorithms():
    runner = AlgorithmHelper()
    return runner.GetAvailableAlgorithms()


@app.post("/algorithms/run")
async def run_algorithm(algorithm_name: str, dataset_name: str, save_as: str, body: Body):
    runner = AlgorithmHelper()
    print(body)
    DatasetLoader.save_current_dataset_state(save_as, runner.RunAlgorithm(algorithm_name, dataset_name, body))
    return "completed"


@app.get("/algorithms/parameters")
async def get_all_algorithm_parameters(algorithm_name: str):
    runner = AlgorithmHelper()
    return runner.GetAlgorithmParameters(algorithm_name)


@app.get("/dataset/current")
async def save_current_dataset():
    loader = DatasetLoader("../Masters Thesis/dashboard-data-function/data")
    DatasetLoader.save_current_dataset_state("new_name", loader.load_data())
    return "File created."


@app.get("/dataset/all")
async def get_all_present_datasets():
    loader = DatasetLoader("./SavedDatasets")
    return loader.load_dataset_list()

@app.get("/dataset/sample")
async def get_dataset_sample(dataset_name: str):
    loader = DatasetLoader("./SavedDatasets")
    return loader.load_dataset_sample(dataset_name)


class NewDatasetType(BaseModel):
    filename: str
    date_from: str
    date_to: str
    mt: Union[str, None]

@app.post("/jobs/new/dataset")
async def create_new_dataset_job(body: NewDatasetType):
    mq_client = RabbitMQClient("new/dataset")
    if body.mt is not None:
        mq_client.send_message(RabbitMQClient.prepare_new_dataset_obj_with_mt(body.filename, body.date_from, body.date_to, body.mt))
    else:
        mq_client.send_message(RabbitMQClient.prepare_new_dataset_obj(body.filename, body.date_from, body.date_to))

def RecursiveDeviceTree(device_id: str, device_name: str, conn: CumulocityFetcher):
    children = conn.get_device_children(device_id)['references']
    if len(children) > 0:
        sub_children = []
        for child in children:
            sub_children.append({
                'id': child['managedObject']['id'],
                'name': child['managedObject']['name'],
                'children': RecursiveDeviceTree(child['managedObject']['id'], child['managedObject']['name'], conn)
            })
        return sub_children
    else:
        return {
            'id': device_id,
            'name': device_name
        }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
