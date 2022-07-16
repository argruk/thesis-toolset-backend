from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import uvicorn
import json

from Helpers.CumulocityFetcher import CumulocityFetcher
from Helpers.NotificationFetcher import NotificationFetcher

app = FastAPI(debug=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.get("/devices/{device_id}")
async def get_devices(device_id: str):
    conn = CumulocityFetcher(platform=tenant_url, username=username, password=password)
    return conn.get_single_device(device_id)

@app.get("/devices/{device_id}/measurements")
async def get_device_meaurements(device_id: str, type:str = "", pageSize:int = 10):
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
