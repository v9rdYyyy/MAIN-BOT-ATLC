from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from types import MethodType
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

from afk_bot.bot import AFKBot, AFKCog, AFKPanelView
from afk_bot.config import Settings as AFKSettings
from afk_bot.storage import Storage as AFKStorage
from sbornik_bot.bot import GatherCog, GatherView, SbornikBot
from sbornik_bot.config import Settings as SbornikSettings
from sbornik_bot.storage import Storage as SbornikStorage

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG = logging.getLogger("unified_bot")


@dataclass(slots=True)
class UnifiedSettings:
    token: str
    fallback_token: str
    guild_id: int | None
    afk_db_path: Path
    sbornik_db_path: Path
    timezone: str



BOT_TOKEN = ""

BOT_OWNER_IDS: set[int] = {
    504936984326832128,
}

DEFAULT_RESULTS_CHANNEL_ID = 1481031147717918863
DEFAULT_INTERVIEW_VOICE_CHANNEL_ID = 1444268474686902338
DEFAULT_REVIEW_ROLE_ID = 1444268473592053868
DEFAULT_APPLICATIONS_CATEGORY_ID = 1480994826546581525
DEFAULT_ARCHIVE_CATEGORY_ID = 1481057104025485323
DEFAULT_SERVER_NAME = "A T L E T I C O"

COMMAND_GUILD_ID = 1444268473256513569


def load_settings() -> UnifiedSettings:
    token = (
        os.getenv("DISCORD_BOT_TOKEN", "").strip()
        or os.getenv("DISCORD_TOKEN", "").strip()
        or os.getenv("TOKEN", "").strip()
        or BOT_TOKEN.strip()
    )
    token = token.replace("Bot ", "").strip()

    guild_raw = os.getenv("DISCORD_GUILD_ID", "").strip()
    guild_id = int(guild_raw) if guild_raw else COMMAND_GUILD_ID

    afk_db_path = Path(os.getenv("AFK_BOT_DB_PATH", "").strip() or (DATA_DIR / "afk_bot.sqlite3"))
    sbornik_db_path = Path(os.getenv("SBORNIK_BOT_DB_PATH", "").strip() or (DATA_DIR / "sbornik_bot.sqlite3"))
    timezone = os.getenv("BOT_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"

    afk_db_path.parent.mkdir(parents=True, exist_ok=True)
    sbornik_db_path.parent.mkdir(parents=True, exist_ok=True)

    return UnifiedSettings(
        token=token,
        fallback_token=BOT_TOKEN.strip(),
        guild_id=guild_id,
        afk_db_path=afk_db_path,
        sbornik_db_path=sbornik_db_path,
        timezone=timezone,
    )


class ClientBackedAdapter:
    def __init__(self, client: commands.Bot) -> None:
        self.client = client

    def __getattr__(self, item):
        return getattr(self.client, item)


class AFKAdapter(ClientBackedAdapter):
    def __init__(self, client: commands.Bot, settings: AFKSettings) -> None:
        super().__init__(client)
        self.settings = settings
        self.storage = AFKStorage(settings.database_path)
        self._ticker_task: asyncio.Task[None] | None = None


class SbornikAdapter(ClientBackedAdapter):
    def __init__(self, client: commands.Bot, settings: SbornikSettings) -> None:
        super().__init__(client)
        self.settings = settings
        self.storage = SbornikStorage(settings.database_path)
        self.tzinfo = ZoneInfo(settings.timezone)


for _name in (
    "replace_panel",
    "refresh_panel",
    "_fetch_message",
    "_panel_ticker_loop",
    "_process_guild_afk",
):
    setattr(AFKAdapter, _name, getattr(AFKBot, _name))

for _name, _member in inspect.getmembers(SbornikBot, predicate=inspect.iscoroutinefunction):
    if _name not in {"__init__", "setup_hook", "on_ready"}:
        setattr(SbornikAdapter, _name, _member)
for _name, _member in inspect.getmembers(SbornikBot, predicate=inspect.isfunction):
    if _name not in {"__init__", "setup_hook", "on_ready"}:
        setattr(SbornikAdapter, _name, _member)


class UnifiedBot(commands.Bot):
    def __init__(self, settings: UnifiedSettings) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(
            command_prefix=commands.when_mentioned,
            intents=intents,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self.settings = settings
        self.afk = AFKAdapter(
            self,
            AFKSettings(
                token=settings.token,
                guild_id=settings.guild_id,
                database_path=settings.afk_db_path,
            ),
        )
        self.sbornik = SbornikAdapter(
            self,
            SbornikSettings(
                token=settings.token,
                guild_id=settings.guild_id,
                database_path=settings.sbornik_db_path,
                timezone=settings.timezone,
            ),
        )
        self.family = None
        self._family_on_ready = None
        self._family_on_guild_join = None

    def _load_family_module(self):
        if self.settings.token:
            os.environ.setdefault("DISCORD_TOKEN", self.settings.token)
        module = importlib.import_module("family_bot_module")

        module.FALLBACK_TOKEN = self.settings.fallback_token
        module.BOT_OWNER_IDS = set(BOT_OWNER_IDS)
        module.DEFAULT_RESULTS_CHANNEL_ID = DEFAULT_RESULTS_CHANNEL_ID
        module.DEFAULT_INTERVIEW_VOICE_CHANNEL_ID = DEFAULT_INTERVIEW_VOICE_CHANNEL_ID
        module.DEFAULT_REVIEW_ROLE_ID = DEFAULT_REVIEW_ROLE_ID
        module.DEFAULT_APPLICATIONS_CATEGORY_ID = DEFAULT_APPLICATIONS_CATEGORY_ID
        module.DEFAULT_ARCHIVE_CATEGORY_ID = DEFAULT_ARCHIVE_CATEGORY_ID
        module.DEFAULT_SERVER_NAME = DEFAULT_SERVER_NAME
        module.COMMAND_GUILD_ID = COMMAND_GUILD_ID
        module.bot = self

        async def _sync_tree_for_guild(guild: discord.Guild | None) -> int:
            if getattr(module, "COMMAND_GUILD_ID", None) and guild:
                target = discord.Object(id=module.COMMAND_GUILD_ID)
                self.tree.copy_global_to(guild=target)
                synced = await self.tree.sync(guild=target)
                return len(synced)
            synced = await self.tree.sync()
            return len(synced)

        module.sync_tree_for_guild = _sync_tree_for_guild
        self.family = module
        self._family_on_ready = getattr(module, "on_ready", None)
        self._family_on_guild_join = getattr(module, "on_guild_join", None)
        for command in (
            module.family_setup,
            module.family_panel,
            module.family_panel_image,
            module.family_recruitment,
            module.family_cooldown,
            module.family_config,
            module.family_sync,
            module.family_retry,
            module.family_archive_find,
        ):
            try:
                self.tree.add_command(command)
            except Exception:
                pass
        self.add_view(module.PanelView())
        self.add_view(module.StartFormView())
        self.add_view(module.ReviewView())
        self.add_view(module.InterviewView())

    async def setup_hook(self) -> None:
        self._load_family_module()

        await self.afk.storage.initialize()
        await self.add_cog(AFKCog(self.afk))
        self.add_view(AFKPanelView(self.afk))
        self.afk._ticker_task = asyncio.create_task(self.afk._panel_ticker_loop())

        await self.sbornik.storage.initialize()
        await self.add_cog(GatherCog(self.sbornik))
        gathers = await self.sbornik.storage.list_open_gathers()
        for gather in gathers:
            self.add_view(GatherView(self.sbornik, gather.gather_id))

        sync_guild_id = getattr(self.family, "COMMAND_GUILD_ID", None) or self.settings.guild_id
        if sync_guild_id:
            guild = discord.Object(id=sync_guild_id)
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            LOG.info("Команды синхронизированы с сервером %s: %s шт.", sync_guild_id, len(synced))
        else:
            synced = await self.tree.sync()
            LOG.info("Глобальные команды синхронизированы: %s шт.", len(synced))

    async def on_ready(self) -> None:
        if self.user is not None:
            LOG.info("Unified bot online as %s (%s)", self.user, self.user.id)
        if self.family is not None:
            for guild in self.guilds:
                await self.family.ensure_defaults_saved(guild)

    async def on_guild_join(self, guild: discord.Guild) -> None:
        if self.family is not None:
            await self.family.ensure_defaults_saved(guild)

    async def close(self) -> None:
        task = self.afk._ticker_task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await super().close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")
    settings = load_settings()
    if not settings.token:
        raise RuntimeError(
            "Не найден токен. Хостинг должен передать TOKEN / DISCORD_TOKEN / DISCORD_BOT_TOKEN, "
            "или временно укажи BOT_TOKEN в main.py."
        )
    bot = UnifiedBot(settings)
    bot.run(settings.token)


if __name__ == "__main__":
    main()
