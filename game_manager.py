from typing import Dict, List
from fastapi import WebSocket
import random
import asyncio


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

    async def broadcast_to_room(self, room_code: str, message: dict):
        if room_code not in self.active_connections:
            return

        for ws in list(self.active_connections[room_code].values()):
            try:
                await ws.send_json(message)
            except:
                pass


manager = ConnectionManager()


class GameLogic:
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory

        self.collecting: Dict[str, bool] = {}
        self.room_phrases: Dict[str, List] = {}
        self.current_index: Dict[str, int] = {}
        self.votes: Dict[str, Dict[int, int]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running: Dict[str, bool] = {}

    # ================= START GAME =================
    async def start_game(self, room_code: str):
        if self._running.get(room_code):
            return

        self._running[room_code] = True

        db = self.db_session_factory()
        try:
            from database import Room, Player, Phrase

            room = db.query(Room).filter(Room.code == room_code).first()
            if not room:
                self._running[room_code] = False
                return

            players = db.query(Player).filter(Player.room_id == room.id).all()
            if len(players) < 2:
                self._running[room_code] = False
                return

            # COLLECTION PHASE
            self.collecting[room_code] = True

            await manager.broadcast_to_room(room_code, {
                "type": "start_collecting",
                "data": {"seconds": 30}
            })

            await asyncio.sleep(30)

            self.collecting[room_code] = False

            phrases = db.query(Phrase).filter(Phrase.room_id == room.id).all()

            if not phrases or len(phrases) < 1:
                await manager.broadcast_to_room(room_code, {
                    "type": "game_end",
                    "data": {"error": "No phrases"}
                })
                self._running[room_code] = False
                return

            random.shuffle(phrases)

            self.room_phrases[room_code] = phrases
            self.current_index[room_code] = 0
            self.votes[room_code] = {}

            await self._start_phrase(room_code)

        finally:
            db.close()

    # ================= PHRASE ROUND =================
    async def _start_phrase(self, room_code: str):
        db = self.db_session_factory()

        try:
            from database import Player

            idx = self.current_index.get(room_code, 0)
            phrases = self.room_phrases.get(room_code, [])

            if idx >= len(phrases):
                await self._end_game(room_code)
                return

            phrase = phrases[idx]

            self.votes[room_code] = {}

            await manager.broadcast_to_room(room_code, {
                "type": "start_voting",
                "data": {
                    "phrase": phrase.text,
                    "author_id": phrase.author_id,
                    "round": idx + 1
                }
            })

            if room_code in self._tasks:
                self._tasks[room_code].cancel()

            self._tasks[room_code] = asyncio.create_task(
                self._voting_timer(room_code)
            )

        finally:
            db.close()

    # ================= TIMER =================
    async def _voting_timer(self, room_code: str):
        try:
            for i in range(60, -1, -1):
                await manager.broadcast_to_room(room_code, {
                    "type": "timer_update",
                    "data": {"seconds": i}
                })

                await asyncio.sleep(1)

                if self._all_voted(room_code):
                    break

            await self._reveal(room_code)

        except asyncio.CancelledError:
            return

    # ================= VOTES =================
    async def handle_vote(self, room_code: str, voter_id: int, voted_player_id: int):

        # ❌ запрет голосовать за себя ИЛИ за автора фразы
        idx = self.current_index.get(room_code, 0)
        phrase = self.room_phrases[room_code][idx]

        if voter_id == phrase.author_id:
            return

        self.votes.setdefault(room_code, {})
        self.votes[room_code][voter_id] = voted_player_id

        if self._all_voted(room_code):
            await self._reveal(room_code)

    def _all_voted(self, room_code: str) -> bool:
        db = self.db_session_factory()
        try:
            from database import Room, Player

            room = db.query(Room).filter(Room.code == room_code).first()
            if not room:
                return False

            players = db.query(Player).filter(Player.room_id == room.id).all()

            return len(self.votes.get(room_code, {})) >= len(players)

        finally:
            db.close()

    # ================= REVEAL =================
    async def _reveal(self, room_code: str):
        db = self.db_session_factory()

        try:
            from database import Player

            idx = self.current_index[room_code]
            phrase = self.room_phrases[room_code][idx]

            votes = self.votes.get(room_code, {})

            players = db.query(Player).filter(Player.room_id == phrase.room_id).all()

            correct_voters = []

            for voter_id, voted_id in votes.items():
                if voted_id == phrase.author_id:
                    correct_voters.append(voter_id)

                    voter = next((p for p in players if p.id == voter_id), None)
                    if voter:
                        voter.score += 2

            db.commit()

            author = db.query(Player).filter(Player.id == phrase.author_id).first()

            await manager.broadcast_to_room(room_code, {
                "type": "reveal_author",
                "data": {
                    "phrase": phrase.text,
                    "author": author.nickname,
                    "correctVoters": correct_voters
                }
            })

            await asyncio.sleep(3)

            self.current_index[room_code] += 1
            await self._start_phrase(room_code)

        finally:
            db.close()

    # ================= END GAME =================
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

        finally:
            db.close()

        self._running[room_code] = False

    # ================= ACCESS =================
    def can_submit(self, room_code: str) -> bool:
        return self.collecting.get(room_code, False)