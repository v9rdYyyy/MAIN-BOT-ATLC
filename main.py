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

# =========================
# ЕДИНЫЙ КОНФИГ БОТА
# Заполняй всё здесь, без .env
# =========================
BOT_TOKEN = "PASTE_YOUR_BOT_TOKEN_HERE"

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

BOT_TIMEZONE = "Europe/Moscow"
AFK_DB_FILENAME = "afk_bot.sqlite3"
SBORNIK_DB_FILENAME = "sbornik_bot.sqlite3"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG = logging.getLogger("unified_bot")


@dataclass(slots=True)
class UnifiedSettings:
    token: str
    guild_id: int | None
    afk_db_path: Path
    sbornik_db_path: Path
    timezone: str
    bot_owner_ids: set[int]
    default_results_channel_id: int
    default_interview_voice_channel_id: int
    default_review_role_id: int
    default_applications_category_id: int
    default_archive_category_id: int
    default_server_name: str


def load_settings() -> UnifiedSettings:
    token = BOT_TOKEN.replace("Bot ", "").strip()
    if not token or token == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Вставь токен бота в переменную BOT_TOKEN внутри main.py")

    afk_db_path = DATA_DIR / AFK_DB_FILENAME
    sbornik_db_path = DATA_DIR / SBORNIK_DB_FILENAME
    afk_db_path.parent.mkdir(parents=True, exist_ok=True)
    sbornik_db_path.parent.mkdir(parents=True, exist_ok=True)

    return UnifiedSettings(
        token=token,
        guild_id=COMMAND_GUILD_ID,
        afk_db_path=afk_db_path,
        sbornik_db_path=sbornik_db_path,
        timezone=BOT_TIMEZONE,
        bot_owner_ids=set(BOT_OWNER_IDS),
        default_results_channel_id=DEFAULT_RESULTS_CHANNEL_ID,
        default_interview_voice_channel_id=DEFAULT_INTERVIEW_VOICE_CHANNEL_ID,
        default_review_role_id=DEFAULT_REVIEW_ROLE_ID,
        default_applications_category_id=DEFAULT_APPLICATIONS_CATEGORY_ID,
        default_archive_category_id=DEFAULT_ARCHIVE_CATEGORY_ID,
        default_server_name=DEFAULT_SERVER_NAME,
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

    def _load_family_module(self):
        os.environ["DISCORD_TOKEN"] = self.settings.token
        module = importlib.import_module("family_bot_module")
        module.bot = self

        # Централизуем все важные настройки заявок через main.py
        module.FALLBACK_TOKEN = self.settings.token
        module.TOKEN = self.settings.token
        module.BOT_OWNER_IDS = set(self.settings.bot_owner_ids)
        module.DEFAULT_RESULTS_CHANNEL_ID = self.settings.default_results_channel_id
        module.DEFAULT_INTERVIEW_VOICE_CHANNEL_ID = self.settings.default_interview_voice_channel_id
        module.DEFAULT_REVIEW_ROLE_ID = self.settings.default_review_role_id
        module.DEFAULT_APPLICATIONS_CATEGORY_ID = self.settings.default_applications_category_id
        module.DEFAULT_ARCHIVE_CATEGORY_ID = self.settings.default_archive_category_id
        module.DEFAULT_SERVER_NAME = self.settings.default_server_name
        module.COMMAND_GUILD_ID = self.settings.guild_id

        if hasattr(module, "db") and hasattr(module.db, "get_config"):
            original_get_config = module.db.get_config

            def patched_get_config(_db_self, guild_id: int):
                config = original_get_config(guild_id)
                if getattr(config, "result_channel_id", 0) == 0:
                    config.result_channel_id = self.settings.default_results_channel_id
                if getattr(config, "voice_channel_id", 0) == 0:
                    config.voice_channel_id = self.settings.default_interview_voice_channel_id
                if getattr(config, "review_role_id", 0) == 0:
                    config.review_role_id = self.settings.default_review_role_id
                if getattr(config, "applications_category_id", 0) == 0:
                    config.applications_category_id = self.settings.default_applications_category_id
                if getattr(config, "archive_category_id", 0) == 0:
                    config.archive_category_id = self.settings.default_archive_category_id
                if not getattr(config, "server_name", ""):
                    config.server_name = self.settings.default_server_name
                return config

            module.db.get_config = MethodType(patched_get_config, module.db)

        async def _sync_tree_for_guild(guild: discord.Guild | None) -> int:
            if self.settings.guild_id and guild:
                target = discord.Object(id=self.settings.guild_id)
                self.tree.copy_global_to(guild=target)
                synced = await self.tree.sync(guild=target)
                return len(synced)
            synced = await self.tree.sync()
            return len(synced)

        module.sync_tree_for_guild = _sync_tree_for_guild
        self.family = module
        for command in (
            module.family_setup,
            module.family_panel,
            module.family_panel_image,
            module.family_recruitment,
            module.family_cooldown,
            module.family_config,
            module.family_sync,
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

        if self.settings.guild_id:
            guild = discord.Object(id=self.settings.guild_id)
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            LOG.info("Команды синхронизированы с сервером %s: %s шт.", self.settings.guild_id, len(synced))
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
    bot = UnifiedBot(settings)
    bot.run(settings.token)


if __name__ == "__main__":
    main()
