from pydantic import BaseModel
from typing import List
from enum import Enum


# =========================
# Создание комнаты
# =========================
class RoomCreate(BaseModel):
    host_nickname: str


# =========================
# Вход в комнату
# =========================
class JoinRoom(BaseModel):
    room_code: str
    nickname: str


# =========================
# Игрок
# =========================
class PlayerResponse(BaseModel):
    id: int
    nickname: str
    score: int
    avatar_color: str


# =========================
# Информация о комнате
# =========================
class RoomResponse(BaseModel):
    id: int
    code: str
    host_player_id: int
    is_active: bool
    players: List[PlayerResponse]


# =========================
# Отправка фразы
# =========================
class SubmitPhrase(BaseModel):
    text: str


# =========================
# Голосование
# =========================
class VoteAction(BaseModel):
    voted_player_id: int


# =========================
# Универсальное сообщение
# =========================
class GameMessage(BaseModel):
    type: str
    data: dict


# =========================
# Типы сообщений WebSocket
# =========================
class MessageType(str, Enum):

    PLAYER_JOINED = "player_joined"
    PLAYER_LEFT = "player_left"

    GAME_STARTING = "game_starting"

    START_COLLECTING = "start_collecting"

    PHRASE_SUBMITTED = "phrase_submitted"

    START_VOTING = "start_voting"

    PLAYER_VOTED = "player_voted"

    REVEAL_AUTHOR = "reveal_author"

    SCORE_UPDATE = "score_update"

    NEXT_PHRASE = "next_phrase"

    TIMER_UPDATE = "timer_update"

    GAME_END = "game_end"