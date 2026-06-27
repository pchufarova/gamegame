from typing import Dict, List
import asyncio
import random
from fastapi import WebSocket


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

        self.collecting = {}
        self.submitted = {}

        self.room_phrases: Dict[str, List[str]] = {}
        self.players_cache: Dict[str, List] = {}

        self.current_index = {}
        self.votes = {}
        self._tasks = {}
        self._running = {}
        self.revealing = {}

    # ================= START =================
    async def start_game(self, room_code: str):
        if self._running.get(room_code):
            return

        db = self.db_session_factory()

        try:
            from database import Room, Player

            room = db.query(Room).filter(Room.code == room_code).first()
            if not room:
                return

            players = db.query(Player).filter(Player.room_id == room.id).all()

            if len(players) < 2:
                return

            self._running[room_code] = True
            self.collecting[room_code] = True

            self.players_cache[room_code] = players
            self.submitted[room_code] = []

            await manager.broadcast_to_room(room_code, {
                "type": "start_collecting",
                "data": {"seconds": 30}
            })

            await asyncio.sleep(30)

            self.collecting[room_code] = False

            phrases = self.submitted.get(room_code, [])

            # 🔥 ДОБИВКА чтобы не было -1 раунда
            while len(phrases) < len(players):
                phrases.append("...")

            random.shuffle(phrases)

            self.room_phrases[room_code] = phrases
            self.current_index[room_code] = 0
            self.votes[room_code] = {}
            self.revealing[room_code] = False

            await self._start_phrase(room_code)

        finally:
            db.close()

    # ================= REGISTER =================
    def register_phrase(self, room_code: str, player_id: int, text: str):
        if room_code not in self.submitted:
            self.submitted[room_code] = []

        self.submitted[room_code].append(text)

    # ================= PHRASE =================
    async def _start_phrase(self, room_code: str):
        idx = self.current_index.get(room_code, 0)
        phrases = self.room_phrases.get(room_code, [])

        if idx >= len(phrases):
            await self._end_game(room_code)
            return

        phrase = phrases[idx]

        self.votes[room_code] = {}
        self.revealing[room_code] = False

        await manager.broadcast_to_room(room_code, {
            "type": "start_voting",
            "data": {
                "phrase": phrase,
                "author_id": -1
            }
        })

        if room_code in self._tasks:
            self._tasks[room_code].cancel()

        self._tasks[room_code] = asyncio.create_task(
            self._voting_timer(room_code)
        )

    # ================= TIMER =================
    async def _voting_timer(self, room_code: str):
        try:
            for i in range(60, -1, -1):

                if self._all_voted(room_code):
                    break

                await manager.broadcast_to_room(room_code, {
                    "type": "timer_update",
                    "data": {"seconds": i}
                })

                await asyncio.sleep(1)

            await self._reveal(room_code)

        except asyncio.CancelledError:
            return

    # ================= VOTE =================
    async def handle_vote(self, room_code: str, voter_id: int, voted_player_id: int):
        if room_code not in self.votes:
            self.votes[room_code] = {}

        self.votes[room_code][voter_id] = voted_player_id

        if self._all_voted(room_code):
            if room_code in self._tasks:
                self._tasks[room_code].cancel()

            await self._reveal(room_code)

    def _all_voted(self, room_code: str):
        players = self.players_cache.get(room_code, [])
        return len(self.votes.get(room_code, {})) >= len(players) - 1

    # ================= REVEAL =================
    async def _reveal(self, room_code: str):
        if self.revealing.get(room_code):
            return

        self.revealing[room_code] = True

        idx = self.current_index[room_code]
        phrase = self.room_phrases[room_code][idx]

        await manager.broadcast_to_room(room_code, {
            "type": "reveal_author",
            "data": {
                "phrase": phrase,
                "author": "unknown",
                "correctVoters": []
            }
        })

        await asyncio.sleep(2)

        self.current_index[room_code] += 1
        await self._start_phrase(room_code)

    # ================= END =================
    async def _end_game(self, room_code: str):
        players = self.players_cache.get(room_code, [])

        if not players:
            return

        winner = players[0]

        await manager.broadcast_to_room(room_code, {
            "type": "game_end",
            "data": {
                "winner": winner.nickname,
                "final_scores": {p.nickname: 0 for p in players}
            }
        })

        self._running[room_code] = False