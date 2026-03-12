from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import AFKEntry, PanelRecord

UTC = timezone.utc


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self._lock:
            conn = self._connect()
            try:
                conn.executescript(
                    """
                    PRAGMA journal_mode = WAL;

                    CREATE TABLE IF NOT EXISTS panels (
                        guild_id   INTEGER PRIMARY KEY,
                        channel_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        created_by INTEGER NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS afk_entries (
                        guild_id     INTEGER NOT NULL,
                        user_id      INTEGER NOT NULL,
                        display_name TEXT NOT NULL,
                        reason       TEXT NOT NULL,
                        eta          TEXT NOT NULL,
                        started_at   TEXT NOT NULL,
                        PRIMARY KEY (guild_id, user_id)
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

    async def set_panel(
        self,
        guild_id: int,
        channel_id: int,
        message_id: int,
        created_by: int,
    ) -> None:
        updated_at = _utcnow().isoformat()
        async with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO panels (guild_id, channel_id, message_id, created_by, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        channel_id = excluded.channel_id,
                        message_id = excluded.message_id,
                        created_by = excluded.created_by,
                        updated_at = excluded.updated_at
                    """,
                    (guild_id, channel_id, message_id, created_by, updated_at),
                )
                conn.commit()
            finally:
                conn.close()

    async def get_panel(self, guild_id: int) -> PanelRecord | None:
        async with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT guild_id, channel_id, message_id, created_by, updated_at FROM panels WHERE guild_id = ?",
                    (guild_id,),
                ).fetchone()
            finally:
                conn.close()

        if row is None:
            return None

        return PanelRecord(
            guild_id=row["guild_id"],
            channel_id=row["channel_id"],
            message_id=row["message_id"],
            created_by=row["created_by"],
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    async def clear_panel(self, guild_id: int) -> None:
        async with self._lock:
            conn = self._connect()
            try:
                conn.execute("DELETE FROM panels WHERE guild_id = ?", (guild_id,))
                conn.commit()
            finally:
                conn.close()

    async def upsert_afk(
        self,
        guild_id: int,
        user_id: int,
        display_name: str,
        reason: str,
        eta: str,
    ) -> None:
        started_at = _utcnow().isoformat()
        async with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO afk_entries (guild_id, user_id, display_name, reason, eta, started_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id) DO UPDATE SET
                        display_name = excluded.display_name,
                        reason = excluded.reason,
                        eta = excluded.eta,
                        started_at = excluded.started_at
                    """,
                    (guild_id, user_id, display_name, reason, eta, started_at),
                )
                conn.commit()
            finally:
                conn.close()

    async def remove_afk(self, guild_id: int, user_id: int) -> bool:
        async with self._lock:
            conn = self._connect()
            try:
                cursor = conn.execute(
                    "DELETE FROM afk_entries WHERE guild_id = ? AND user_id = ?",
                    (guild_id, user_id),
                )
                conn.commit()
                deleted = cursor.rowcount > 0
            finally:
                conn.close()
        return deleted

    async def get_afk(self, guild_id: int, user_id: int) -> AFKEntry | None:
        async with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    """
                    SELECT guild_id, user_id, display_name, reason, eta, started_at
                    FROM afk_entries
                    WHERE guild_id = ? AND user_id = ?
                    """,
                    (guild_id, user_id),
                ).fetchone()
            finally:
                conn.close()

        return _row_to_entry(row) if row else None

    async def list_afk(self, guild_id: int) -> list[AFKEntry]:
        async with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                    SELECT guild_id, user_id, display_name, reason, eta, started_at
                    FROM afk_entries
                    WHERE guild_id = ?
                    ORDER BY started_at ASC, user_id ASC
                    """,
                    (guild_id,),
                ).fetchall()
            finally:
                conn.close()

        return [_row_to_entry(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn



def _row_to_entry(row: sqlite3.Row) -> AFKEntry:
    return AFKEntry(
        guild_id=row["guild_id"],
        user_id=row["user_id"],
        display_name=row["display_name"],
        reason=row["reason"],
        eta=row["eta"],
        started_at=datetime.fromisoformat(row["started_at"]),
    )



def _utcnow() -> datetime:
    return datetime.now(tz=UTC)
