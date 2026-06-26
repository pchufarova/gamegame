from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    JSON
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = "sqlite:///./game.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


# =========================
# Комната
# =========================
class Room(Base):
    __tablename__ = "rooms"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(6), unique=True, index=True)

    host_player_id = Column(Integer, nullable=True)

    is_active = Column(Boolean, default=True)

    current_phrase_index = Column(Integer, default=0)

    created_at = Column(DateTime, default=datetime.utcnow)

    players = relationship(
        "Player",
        back_populates="room"
    )

    phrases = relationship(
        "Phrase",
        back_populates="room"
    )


# =========================
# Игрок
# =========================
class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)

    nickname = Column(String(50))

    room_id = Column(
        Integer,
        ForeignKey("rooms.id"),
        nullable=False
    )

    score = Column(Integer, default=0)

    joined_at = Column(
        DateTime,
        default=datetime.utcnow
    )

    avatar_color = Column(
        String(7),
        default="#4A90D9"
    )

    room = relationship(
        "Room",
        back_populates="players"
    )


# =========================
# Фразы игроков
# =========================
class Phrase(Base):
    __tablename__ = "phrases"

    id = Column(Integer, primary_key=True, index=True)

    text = Column(String(500))

    room_id = Column(
        Integer,
        ForeignKey("rooms.id"),
        nullable=False
    )

    author_id = Column(
        Integer,
        ForeignKey("players.id"),
        nullable=False
    )

    is_shown = Column(Boolean, default=False)

    room = relationship(
        "Room",
        back_populates="phrases"
    )


# =========================
# Голоса
# =========================
class Vote(Base):
    __tablename__ = "votes"

    id = Column(Integer, primary_key=True, index=True)

    phrase_id = Column(
        Integer,
        ForeignKey("phrases.id"),
        nullable=False
    )

    voter_id = Column(
        Integer,
        ForeignKey("players.id"),
        nullable=False
    )

    voted_player_id = Column(
        Integer,
        ForeignKey("players.id"),
        nullable=False
    )


# =========================
# Состояние игры
# =========================
class GameState(Base):
    __tablename__ = "game_states"

    id = Column(Integer, primary_key=True, index=True)

    room_id = Column(
        Integer,
        ForeignKey("rooms.id"),
        unique=True
    )

    phase = Column(
        String(50),
        default="lobby"
    )
    # lobby
    # collecting
    # voting
    # reveal
    # finished

    current_phrase_id = Column(
        Integer,
        nullable=True
    )

    timer_seconds = Column(
        Integer,
        default=60
    )

    shuffled_phrase_order = Column(
        JSON,
        default=[]
    )


# =========================
# Создание БД
# =========================
if os.path.exists("game.db"):
    os.remove("game.db")
    print("🗑️ Старая база удалена")

Base.metadata.create_all(bind=engine)
print("✅ Новая база создана")


def get_db():
    return SessionLocal()


def get_db_depends():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()