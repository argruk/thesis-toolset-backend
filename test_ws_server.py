from fastapi import FastAPI, WebSocket
import time

app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while websocket.state:
        time.sleep(2)
        await websocket.send_text(f"Random value")
