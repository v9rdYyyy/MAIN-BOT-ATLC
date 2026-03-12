from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class AFKEntry:
    guild_id: int
    user_id: int
    display_name: str
    reason: str
    eta: str
    started_at: datetime


@dataclass(slots=True)
class PanelRecord:
    guild_id: int
    channel_id: int
    message_id: int
    created_by: int
    updated_at: datetime
