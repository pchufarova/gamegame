from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from sqlalchemy.orm import Session

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


# =========================
# CREATE ROOM
# =========================
@app.post("/api/rooms/create")
def create_room(data: dict, db: Session = Depends(get_db)):
    room = Room(
        code=str(random.randint(100000, 999999)),
        host_player_id=None,
        is_active=True
    )

    db.add(room)
    db.commit()
    db.refresh(room)

    player = Player(
        nickname=data["host_nickname"],
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


# =========================
# JOIN ROOM
# =========================
@app.post("/api/rooms/join")
def join_room(data: dict, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == data["room_code"]).first()

    if not room:
        return {"error": "Room not found"}

    player = Player(
        nickname=data["nickname"],
        room_id=room.id,
        avatar_color="#FF6B6B"
    )

    db.add(player)
    db.commit()
    db.refresh(player)

    return {"player_id": player.id}


# =========================
# ROOM INFO
# =========================
@app.get("/api/rooms/{room_code}")
def get_room(room_code: str, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == room_code).first()

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
# WEBSOCKET
# =========================
@app.websocket("/ws/{room_code}/{player_id}")
async def websocket_endpoint(websocket: WebSocket, room_code: str, player_id: int):
    await manager.connect(room_code, player_id, websocket)

    #  функция синка игроков
    async def send_players_state():
        db = SessionLocal()
        try:
            room = db.query(Room).filter(Room.code == room_code).first()
            players = db.query(Player).filter(Player.room_id == room.id).all()

            await manager.broadcast_to_room(room_code, {
                "type": "players_update",
                "data": {
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
            })
        finally:
            db.close()

    try:
        #  сразу синхронизируем всех
        await send_players_state()

        while True:
            data = await websocket.receive_json()

            msg_type = data.get("type")
            payload = data.get("data", {})

            if msg_type == "start_game":
                if room_code in game.room_phrases:
                    continue
                await game.start_game(room_code)

            elif msg_type == "vote":
                await game.handle_vote(
                    room_code,
                    player_id,
                    payload.get("voted_player_id")
                )

            #  после любого действия обновляем список игроков
            await send_players_state()

    except WebSocketDisconnect:
        manager.disconnect(room_code, player_id)
        await send_players_state()