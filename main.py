import asyncio
import random

import websockets
import json

from typing import *
from fastapi import FastAPI, Request, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from pydantic import BaseModel
import datetime
import time

from collections import defaultdict


class CustomMessage(BaseModel):
    value: Union[int, str]
    timestamp: str
    type: str


templates = Jinja2Templates(directory="templates")

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


class WS_Manager:

    def __init__(self):
        self.connections: Dict[int, WebSocket] = {}
        self.map_ws_to_idx: Dict[int, int] = {}
        self.idx_counter: int = 0
        self.message_queues: Dict[int, asyncio.Queue] = {}
        self.tasks = set()

        self.DEFAULT_MAX_QUEUE_SIZE = 5
        self.TASKS_LIMIT = 5 # every client has at most TASKS_LIMIT number of ongoing resend tasks

    async def accept(self, websocket: WebSocket):
        await websocket.accept()
        time_now = datetime.datetime.now()
        unixtime = round(time.mktime(time_now.timetuple()), 2)
        ws_idx = self.idx_counter
        self.connections.update({ws_idx: websocket})
        self.map_ws_to_idx.update({id(websocket): ws_idx})
        self.message_queues.update({ws_idx: asyncio.Queue(maxsize=self.DEFAULT_MAX_QUEUE_SIZE)})
        # self.message_queues.update({ws_idx: asyncio.Queue()})
        message = CustomMessage(value=f"{ws_idx} joins.",
                                timestamp=str(time_now),
                                type="message")
        self.idx_counter += 1
        # await self.broadcast(message)
        await self.broadcast_to_queues(message)
        # self.broadcast_to_queues(message)
        return ws_idx, self.message_queues[ws_idx]

    async def remove(self, websocket: WebSocket):
        time_now = datetime.datetime.now()
        if id(websocket) not in self.map_ws_to_idx:
            return

        ws_idx = self.map_ws_to_idx.get(id(websocket))
        message = CustomMessage(value=f"ws_idx: {ws_idx} leaves.",
                                timestamp=str(time_now),
                                type="message")

        if ws_idx in self.connections:
            del self.connections[ws_idx]

        if id(websocket) in self.map_ws_to_idx:
            del self.map_ws_to_idx[id(websocket)]

        if ws_idx in self.message_queues:
            del self.message_queues[ws_idx]

        try:
            await websocket.close()
            # await self.broadcast(message)
            await self.broadcast_to_queues(message)
            # self.broadcast_to_queues(message)
            # print("message", message)

        except Exception as e:
            print(e)

    async def broadcast(self, message: CustomMessage):
        if len(self.connections) == 0:
            return

        expired_connections = []
        for ws_idx, ws in self.connections.items():
            try:
                await ws.send_json(message.dict())
            except:
                expired_connections.append((ws_idx, ws))

        # if len(expired_connections)>0:
        #     print("expired_connections", expired_connections)

    async def broadcast_to_queues(self, message: CustomMessage):
        # expired_connections = set()
        for ws_idx, ws in self.connections.items():
            queue = self.message_queues[ws_idx]
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:

                # # when the queue is full, drop the old data and append the new one
                # dropping_message: CustomMessage = queue.get_nowait()
                # print(f"ws_idx: {ws_idx} queue is full. Dropping data: " +
                #       f"{dropping_message.value}, {dropping_message.timestamp}")
                # queue.task_done()

                # when the queue is full, remove the old data and create a task to resend it later
                message_to_resend: CustomMessage = queue.get_nowait()
                message_to_resend.type = "resent message"
                queue.task_done()
                queue.put_nowait(message)

                # every client has at most TASKS_LIMIT number of ongoing resend tasks
                # when there are already enough tasks
                # we need to wait for one to be finished
                if len(self.tasks)>self.TASKS_LIMIT:
                    done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
                    # print("done", done)
                    # print("pending", pending)
                    self.tasks = pending
                # create a new resend task for this client
                print(f"ws_idx: {ws_idx} queue is full. Task created: Resend message " +
                      f"{message_to_resend.value}, {message_to_resend.timestamp}")
                task = asyncio.create_task(self.resend(ws_idx=ws_idx,
                                                       message=message_to_resend))
                self.tasks.add(task)

        # for key, value in self.message_queues.items():
        #     print("key", key, "value", value)

    async def resend(self, ws_idx: int, message: CustomMessage, counter: int = 5):
        if ws_idx not in self.message_queues:
            return

        queue = self.message_queues[ws_idx]

        # while True:
        for c in range(counter):
            try:
                # print(f"ws_idx: {ws_idx}, queue.qsize: {queue.qsize()}")
                # queue.put_nowait(message)
                # print(f"ws_idx: {ws_idx}, message: {message} resend success. counter: {c}")
                # print(f"ws_idx: {ws_idx}, message: {message} resend success. ")
                await asyncio.wait_for(queue.put(message),
                                       timeout=0.1)
                return
            # except asyncio.QueueFull:
            except asyncio.TimeoutError:
                print(f"ws_idx: {ws_idx} queue is full. Resend later message: {message}.")
                await asyncio.sleep(2)

        print(f"ws_idx: {ws_idx} queue is full, message: {message} is dropped.")
        return


manager = WS_Manager()

async def producer(manager: WS_Manager):
    value = 0
    print("producer starts...")
    while True:
        message = CustomMessage(value=value,
                                timestamp=str(datetime.datetime.now()),
                                type="message")
        # print("message", message)
        await manager.broadcast_to_queues(message)
        # manager.broadcast_to_queues(message)
        value += 1
        await asyncio.sleep(1)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(producer(manager))


@app.get('/')
async def index(request: Request):
    return templates.TemplateResponse("index.html",
                                      context={"request": request})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    ws_idx, queue = await manager.accept(websocket)
    while True:
        # fetch message from message queues
        try:
            message = await asyncio.wait_for(queue.get(), timeout=0.2)
            queue.task_done()
            try:
                await websocket.send_json(message.dict())
                # print("message", message)
                # print("queue", queue)
                # await asyncio.sleep(2)
                await asyncio.sleep(random.randrange(5))
            except (WebSocketDisconnect, websockets.exceptions.ConnectionClosedOK):
                await manager.remove(websocket)
                return
        # if there is no message in the queue
        except asyncio.TimeoutError:
            # send a ping to check the connection
            try:
                await websocket.send_json({'type': 'ping'})
                # print("ping sent")
                recv = await asyncio.wait_for(websocket.receive_json(),
                                              timeout=0.2)
                # print("recv", recv)
                type = recv.get('type')
                if type != 'pong':
                    await manager.remove(websocket)
                    return

            except (asyncio.TimeoutError, WebSocketDisconnect, websockets.exceptions.ConnectionClosedOK) as e:
                await manager.remove(websocket)
                return

            await asyncio.sleep(2)
