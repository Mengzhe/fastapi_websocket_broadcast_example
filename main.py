import asyncio
import random

import websockets
import json

from typing import *
# import yfinance
from fastapi import FastAPI, Request, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
# from starlette.websockets import WebSocket, WebSocketDisconnect

from pydantic import BaseModel
import datetime
import time

class CustomMessage(BaseModel):
    value: Union[int, str]
    timestamp: str


templates = Jinja2Templates(directory="templates")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

value = 0

class WS_Manager:

    def __init__(self):
        self.connections: Dict[int, WebSocket] = {}
        self.map_ws_to_idx: Dict[int, int] = {}
        self.idx_counter: int = 0
        self.message_queues: Dict[int, asyncio.Queue] = {}
        self.DEFAULT_MAX_QUEUE_SIZE = 5

    async def accept(self, websocket: WebSocket):
        await websocket.accept()
        time_now = datetime.datetime.now()
        unixtime = round(time.mktime(time_now.timetuple()), 2)
        ws_idx = self.idx_counter
        self.connections.update({ws_idx: websocket})
        self.map_ws_to_idx.update({id(websocket): ws_idx})
        self.message_queues.update({ws_idx: asyncio.Queue(maxsize=self.DEFAULT_MAX_QUEUE_SIZE)})
        message = CustomMessage(value=f"{ws_idx} joins.",
                                timestamp=str(time_now))
        self.idx_counter += 1
        await self.broadcast(message)
        await self.broadcast_to_queues(message)
        return ws_idx, self.message_queues[ws_idx]

    async def remove(self, websocket: WebSocket):
        time_now = datetime.datetime.now()
        if id(websocket) not in self.map_ws_to_idx:
            return

        ws_idx = self.map_ws_to_idx.get(id(websocket))
        message = CustomMessage(value=f"ws_idx: {ws_idx} leaves.",
                                timestamp=str(time_now))

        if ws_idx in self.connections:
            del self.connections[ws_idx]

        if id(websocket) in self.map_ws_to_idx:
            del self.map_ws_to_idx[id(websocket)]

        if ws_idx in self.message_queues:
            del self.message_queues[ws_idx]

        await self.broadcast(message)
        await self.broadcast_to_queues(message)
        print("message", message)

        try:
            await websocket.close()

        except Exception as e:
            print(e)


    async def broadcast(self, message: CustomMessage):
        if len(self.connections)==0:
            return

        expired_connections = []
        for wsidx, ws in self.connections.items():
            try:
                await ws.send_json(message.dict())
            except:
                expired_connections.append((wsidx, ws))


        if len(expired_connections)>0:
            print("expired_connections", expired_connections)


    async def broadcast_to_queues(self, message: CustomMessage):
        # expired_connections = set()
        for wsidx, ws in self.connections.items():
            queue = self.message_queues[wsidx]
            try:
                await asyncio.wait_for(queue.put(message),
                                       timeout=0.1)
            except asyncio.TimeoutError:
                print(f"{wsidx} timeout")
                # expired_connections.add(wsidx)

        for key, value in self.message_queues.items():
            print("key", key, "value", value)

        # for wsidx in expired_connections:
        #     await self.remove(self.connections[wsidx])
        # print("living connections", self.connections)

manager = WS_Manager()

@app.get('/')
async def index(request: Request):
    return templates.TemplateResponse("index.html",
                                      context={"request": request})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    ws_idx, queue = await manager.accept(websocket)

    while True:
        message = CustomMessage(value=ws_idx,
                                timestamp=str(datetime.datetime.now()))
        # TODO: get message from message queue
        try:
            await websocket.send_json(message.dict())
            await asyncio.sleep(2)

        # except (websockets.exceptions.ConnectionClosedOK,
        #         websockets.exceptions.ConnectionClosedError,
        #         WebSocketDisconnect) as e:
        #     print("WebSocketDisconnect", e)
        #     await manager.remove(websocket)
        #     break

        except:
            await manager.remove(websocket)
            break






