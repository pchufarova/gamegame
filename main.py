from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from database import SessionLocal, Base, engine, Room, Player
from game_manager import manager, GameLogic

import json

# =========================
# APP INIT
# =========================
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


# =========================
# DB DEPENDENCY
# =========================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================
# API: GET ROOM INFO
# =========================
@app.get("/api/rooms/{room_code}")
def get_room(room_code: str, db: Session = Depends(get_db)):
    room = db.query(Room).filter(Room.code == room_code).first()
    if not room:
        return {"error": "Room not found"}

    players = db.query(Player).filter(Player.room_id == room.id).all()

    return {
        "room_code": room.code,
        "host_player_id": room.host_player_id,
        "category": getattr(room, "category", "Mixed"),
        "max_rounds": getattr(room, "max_rounds", 10),
        "timer_seconds": getattr(room, "timer_seconds", 60),
        "players": [
            {
                "id": p.id,
                "nickname": p.nickname,
                "score": p.score,
                "avatar_color": p.avatar_color,
                "is_explaining": getattr(p, "is_explaining", False)
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

    try:
        await manager.broadcast_to_room(room_code, {
            "type": "player_joined",
            "data": {"player_id": player_id}
        })

        while True:
            data = await websocket.receive_text()

            try:
                msg = json.loads(data)
            except:
                continue

            msg_type = msg.get("type")
            data = msg.get("data", {})

            # =========================
            # START GAME
            # =========================
            if msg_type == "start_game":
                await game.start_game(room_code)

            # =========================
            # CHAT
            # =========================
            elif msg_type == "chat":
                await manager.broadcast_to_room(room_code, {
                    "type": "chat_message",
                    "data": {
                        "player_id": player_id,
                        "message": data.get("message", "")
                    }
                })

            # =========================
            # VOTE
            # =========================
            elif msg_type == "vote":
                voted_player_id = data.get("voted_player_id")

                if voted_player_id is not None:
                    await game.handle_vote(
                        room_code,
                        voter_id=player_id,
                        voted_player_id=voted_player_id
                    )

    except WebSocketDisconnect:
        manager.disconnect(room_code, player_id)

        await manager.broadcast_to_room(room_code, {
            "type": "player_left",
            "data": {"player_id": player_id}
        })