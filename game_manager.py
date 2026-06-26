from typing import Dict, List
from fastapi import WebSocket
import random
import asyncio


# =========================
# CONNECTION MANAGER
# =========================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Dict[int, WebSocket]] = {}

    async def connect(self, room_code: str, player_id: int, websocket: WebSocket):
        await websocket.accept()

        if room_code not in self.active_connections:
            self.active_connections[room_code] = {}

        self.active_connections[room_code][player_id] = websocket

    def disconnect(self, room_code: str, player_id: int):
        if room_code in self.active_connections:
            self.active_connections[room_code].pop(player_id, None)

            if not self.active_connections[room_code]:
                del self.active_connections[room_code]

    async def broadcast_to_room(self, room_code: str, message: dict, exclude_player: int = None):
        if room_code not in self.active_connections:
            return

        for pid, ws in list(self.active_connections[room_code].items()):
            if pid == exclude_player:
                continue
            try:
                await ws.send_json(message)
            except:
                pass

    async def send_to_player(self, room_code: str, player_id: int, message: dict):
        if room_code in self.active_connections:
            ws = self.active_connections[room_code].get(player_id)
            if ws:
                try:
                    await ws.send_json(message)
                except:
                    pass


manager = ConnectionManager()


# =========================
# GAME LOGIC
# =========================
class GameLogic:
    """
    State machine:
    collecting → voting → reveal → next phrase → end
    """

    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory

        # runtime state (in-memory)
        self.room_phrases: Dict[str, List] = {}
        self.current_index: Dict[str, int] = {}
        self.votes: Dict[str, Dict[int, int]] = {}  # room -> voter_id -> voted_player_id
        self.current_phrase_author: Dict[str, int] = {}

    # =========================
    # START GAME
    # =========================
    async def start_game(self, room_code: str):
        db = self.db_session_factory()
        try:
            from database import Room, Player, Phrase

            room = db.query(Room).filter(Room.code == room_code).first()
            if not room:
                return

            players = db.query(Player).filter(Player.room_id == room.id).all()
            if len(players) < 2:
                return

            phrases = db.query(Phrase).filter(Phrase.room_id == room.id).all()
            if not phrases:
                return

            random.shuffle(phrases)

            self.room_phrases[room_code] = phrases
            self.current_index[room_code] = 0
            self.votes[room_code] = {}

            await self._start_phrase(room_code)

        finally:
            db.close()

    # =========================
    # PHRASE FLOW
    # =========================
    async def _start_phrase(self, room_code: str):
        db = self.db_session_factory()
        try:
            from database import Phrase, Player

            idx = self.current_index[room_code]
            phrases = self.room_phrases[room_code]

            if idx >= len(phrases):
                await self._end_game(room_code)
                return

            phrase = phrases[idx]

            author = db.query(Player).filter(Player.id == phrase.author_id).first()

            self.current_phrase_author[room_code] = author.id
            self.votes[room_code] = {}

            # send phrase to room (all players see it)
            await manager.broadcast_to_room(room_code, {
                "type": "start_voting",
                "data": {
                    "phrase": phrase.text,
                    "phrase_id": phrase.id,
                    "round": idx + 1
                }
            })

            asyncio.create_task(self._voting_timer(room_code, phrase.id))

        finally:
            db.close()

    # =========================
    # VOTING TIMER
    # =========================
    async def _voting_timer(self, room_code: str, phrase_id: int):
        db = self.db_session_factory()
        try:
            from database import Room

            room = db.query(Room).filter(Room.code == room_code).first()
            seconds = room.timer_seconds if room else 60

            for i in range(seconds, -1, -1):
                await manager.broadcast_to_room(room_code, {
                    "type": "timer_update",
                    "data": {"seconds": i}
                })
                await asyncio.sleep(1)

                # early stop if everyone voted
                if self._all_voted(room_code):
                    break

            await self._reveal(room_code)

        finally:
            db.close()

    # =========================
    # HANDLE VOTE
    # =========================
    async def handle_vote(self, room_code: str, voter_id: int, voted_player_id: int):
        if room_code not in self.votes:
            self.votes[room_code] = {}

        self.votes[room_code][voter_id] = voted_player_id

        # optional: auto finish early
        if self._all_voted(room_code):
            await self._reveal(room_code)

    def _all_voted(self, room_code: str) -> bool:
        db = self.db_session_factory()
        try:
            from database import Player

            players = db.query(Player).filter(Player.room_id == db.query(Player.room_id)).all()

            if room_code not in self.votes:
                return False

            return len(self.votes[room_code]) >= len(players)

        finally:
            db.close()

    # =========================
    # REVEAL + SCORING
    # =========================
    async def _reveal(self, room_code: str):
        db = self.db_session_factory()
        try:
            from database import Player, Phrase

            idx = self.current_index[room_code]
            phrase = self.room_phrases[room_code][idx]

            author_id = phrase.author_id
            votes = self.votes.get(room_code, {})

            # scoring
            players = db.query(Player).filter(Player.room_id == phrase.room_id).all()

            for voter_id, voted_id in votes.items():
                if voter_id != author_id and voted_id == author_id:
                    voter = next((p for p in players if p.id == voter_id), None)
                    if voter:
                        voter.score += 2

                    author = next((p for p in players if p.id == author_id), None)
                    if author:
                        author.score += 1

            db.commit()

            author = db.query(Player).filter(Player.id == author_id).first()

            await manager.broadcast_to_room(room_code, {
                "type": "reveal_author",
                "data": {
                    "phrase": phrase.text,
                    "author": author.nickname
                }
            })

            await asyncio.sleep(3)

            self.current_index[room_code] += 1
            await self._start_phrase(room_code)

        finally:
            db.close()

    # =========================
    # END GAME
    # =========================
    async def _end_game(self, room_code: str):
        db = self.db_session_factory()
        try:
            from database import Player, Room

            room = db.query(Room).filter(Room.code == room_code).first()
            players = db.query(Player).filter(Player.room_id == room.id).all()

            winner = max(players, key=lambda p: p.score)

            await manager.broadcast_to_room(room_code, {
                "type": "game_end",
                "data": {
                    "winner": winner.nickname,
                    "final_scores": {p.nickname: p.score for p in players}
                }
            })

            room.is_active = False
            db.commit()

        finally:
            db.close()