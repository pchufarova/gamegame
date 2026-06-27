from typing import Dict
from fastapi import WebSocket
import asyncio
import random


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
        self.room_phrases = {}
        self.current_index = {}
        self.votes = {}
        self._tasks = {}
        self._running = {}
        self.revealing = {}
        self._locks = {}

        # 🔥 НОВОЕ: контроль сбора фраз
        self.submitted = {}
        self.collect_done = {}

    # ================= START GAME =================
    async def start_game(self, room_code: str):
        if self._running.get(room_code):
            return

        db = self.db_session_factory()

        try:
            from database import Room, Player, Phrase

            room = db.query(Room).filter(Room.code == room_code).first()
            if not room:
                return

            players = db.query(Player).filter(Player.room_id == room.id).all()
            if len(players) < 2:
                return

            self._running[room_code] = True
            self.collecting[room_code] = True

            # init collect state
            self.submitted[room_code] = set()
            self.collect_done[room_code] = asyncio.Event()

            await manager.broadcast_to_room(room_code, {
                "type": "start_collecting",
                "data": {"seconds": 30}
            })

            await self.collect_done[room_code].wait()

            self.collecting[room_code] = False

            await asyncio.sleep(0.3)

            db.expire_all()

            phrases = db.query(Phrase).filter(
                Phrase.room_id == room.id
            ).all()
            if len(phrases) != len(players):
                print("❌ NOT ALL PHRASES COLLECTED")
                print("Players:", len(players))
                print("Phrases:", len(phrases))

                self._running[room_code] = False
                return

            print("PLAYERS:", len(players))
            print("PHRASES:", len(phrases))

            for p in phrases:
                print(">", p.text)

            if len(phrases) == 0:
                self._running[room_code] = False
                return

            random.shuffle(phrases)

            self.room_phrases[room_code] = phrases
            self.current_index[room_code] = 0
            self.votes[room_code] = {}
            self.revealing[room_code] = False
            self._locks[room_code] = asyncio.Lock()

            await self._start_phrase(room_code)

        finally:
            db.close()

    # ================= PHRASE =================
    async def _start_phrase(self, room_code: str):
        self.revealing[room_code] = False

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
                "author_id": phrase.author_id
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

    # ================= SUBMIT PHRASE =================
    def register_phrase(self, room_code: str, player_id: int):
        if room_code not in self.submitted:
            self.submitted[room_code] = set()

        self.submitted[room_code].add(player_id)

        if self.collect_done.get(room_code):
            from database import SessionLocal, Room, Player

            db = SessionLocal()
            try:
                room = db.query(Room).filter(Room.code == room_code).first()
                players = db.query(Player).filter(Player.room_id == room.id).all()

                if len(self.submitted[room_code]) >= len(players):
                    self.collect_done[room_code].set()
            finally:
                db.close()

    # ================= VOTES =================
    async def handle_vote(self, room_code: str, voter_id: int, voted_player_id: int):

        if room_code not in self.votes:
            self.votes[room_code] = {}

        self.votes[room_code][voter_id] = voted_player_id

        if self._all_voted(room_code):
            if room_code in self._tasks:
                self._tasks[room_code].cancel()

            await self._reveal(room_code)

    def _all_voted(self, room_code):
        db = self.db_session_factory()

        try:
            from database import Player, Room

            room = db.query(Room).filter(Room.code == room_code).first()
            players = db.query(Player).filter(Player.room_id == room.id).all()

            return len(self.votes.get(room_code, {})) >= len(players) - 1

        finally:
            db.close()

    # ================= REVEAL =================
    async def _reveal(self, room_code: str):

        if self.revealing.get(room_code):
            return

        self.revealing[room_code] = True

        async with self._locks[room_code]:

            db = self.db_session_factory()

            try:
                from database import Player

                idx = self.current_index[room_code]
                phrase = self.room_phrases[room_code][idx]

                votes = self.votes.get(room_code, {})

                players = db.query(Player).filter(
                    Player.room_id == phrase.room_id
                ).all()

                correct_voters = []

                for voter_id, voted_id in votes.items():
                    if voted_id == phrase.author_id:
                        correct_voters.append(voter_id)

                        voter = next((p for p in players if p.id == voter_id), None)
                        if voter:
                            voter.score += 2

                db.commit()

                author = db.query(Player).filter(
                    Player.id == phrase.author_id
                ).first()

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
                self.revealing[room_code] = False

                await self._start_phrase(room_code)

            finally:
                db.close()

    # ================= END =================
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

        # cleanup
        self.room_phrases.pop(room_code, None)
        self.current_index.pop(room_code, None)
        self.votes.pop(room_code, None)
        self.revealing.pop(room_code, None)
        self._tasks.pop(room_code, None)
        self._locks.pop(room_code, None)
        self.submitted.pop(room_code, None)
        self.collect_done.pop(room_code, None)