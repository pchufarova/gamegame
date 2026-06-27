from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from sqlalchemy.orm import Session
from pydantic import BaseModel

import random

from database import SessionLocal, Base, engine, Room, Player
from game_manager import manager, GameLogic


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

game = GameLogic(SessionLocal)

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return RedirectResponse("/static/game.html")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class CreateRoom(BaseModel):
    host_nickname: str


class JoinRoom(BaseModel):
    room_code: str
    nickname: str


@app.post("/api/rooms/create")
def create_room(data: CreateRoom, db: Session = Depends(get_db)):
    room = Room(
        code=str(random.randint(100000, 999999)),
        host_player_id=None,
        is_active=True
    )

    db.add(room)
    db.commit()
    db.refresh(room)

    player = Player(
        nickname=data.host_nickname,
        room_id=room.id,
        avatar_color="#4A90D9"
    )

    db.add(player)
    db.commit()
    db.refresh(player)

    room.host_player_id = player.id
    db.commit()

    return {
        "room_code": room.code,
        "player_id": player.id
    }


@app.post("/api/rooms/join")
def join_room(data: JoinRoom, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == data.room_code).first()

    if not room:
        return {"error": "Room not found"}

    player = Player(
        nickname=data.nickname,
        room_id=room.id,
        avatar_color="#FF6B6B"
    )

    db.add(player)
    db.commit()
    db.refresh(player)

    return {"player_id": player.id}


@app.get("/api/rooms/{room_code}")
def get_room(room_code: str, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == room_code).first()

    if not room:
        return {"error": "Room not found"}

    players = db.query(Player).filter(Player.room_id == room.id).all()

    return {
        "room_code": room.code,
        "host_player_id": room.host_player_id,
        "players": [
            {
                "id": p.id,
                "nickname": p.nickname,
                "score": p.score,
                "avatar_color": p.avatar_color
            }
            for p in players
        ]
    }


# =========================
# WS
# =========================
@app.websocket("/ws/{room_code}/{player_id}")
async def websocket_endpoint(websocket: WebSocket, room_code: str, player_id: int):
    await manager.connect(room_code, player_id, websocket)

    try:
        await manager.broadcast_to_room(room_code, {
            "type": "player_joined",
            "data": {"player_id": player_id}
        })

        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            payload = data.get("data", {})

            if msg_type == "start_game":
                await game.start_game(room_code)

            elif msg_type == "submit_phrase":
                text = payload.get("text")

                if text:
                    # 🔥 ВАЖНО: теперь не БД, а память GameLogic
                    game.register_phrase(room_code, player_id, text)

            elif msg_type == "vote":
                await game.handle_vote(
                    room_code,
                    voter_id=player_id,
                    voted_player_id=payload.get("voted_player_id")
                )

    except WebSocketDisconnect:
        manager.disconnect(room_code, player_id)

        await manager.broadcast_to_room(room_code, {
            "type": "player_left",
            "data": {"player_id": player_id}
        })