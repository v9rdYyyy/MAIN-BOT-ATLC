from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from .config import Settings
from .panel import EmbedFactory
from .storage import Storage

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("afk_bot")

MSK = timezone(timedelta(hours=3), name="MSK")
ETA_META_PREFIX = "afkmeta:"
MINUTES_RE = re.compile(r"^\d+$")
TIME_RE = re.compile(r"^(\d{2}):(\d{2})$")
DATETIME_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2})$")

ALWAYS_ADMIN_IDS = {
    504936984326832128,
}


@dataclass(slots=True)
class AFKMeta:
    raw_input: str
    start_ts: int
    end_ts: int
    display_eta: str


class AFKModal(discord.ui.Modal, title="Встать в АФК"):
    reason = discord.ui.TextInput(
        label="Причина",
        placeholder="Укажите причину вставания в АФК",
        max_length=200,
        required=True,
    )
    eta = discord.ui.TextInput(
        label="Примерное время выхода из АФК",
        placeholder="Например: 30 / 60 / 18:35 / 15.03.2026 18:00",
        max_length=120,
        required=True,
    )

    def __init__(self, bot: "AFKBot") -> None:
        super().__init__(timeout=300)
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.guild_id is None or interaction.user is None:
            await interaction.response.send_message(
                "Эта форма работает только на сервере.", ephemeral=True
            )
            return

        reason = str(self.reason).strip()
        eta_text = str(self.eta).strip()
        display_name = _display_name(interaction.user)

        try:
            meta = _parse_user_eta_input(eta_text)
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        await self.bot.storage.upsert_afk(
            guild_id=interaction.guild_id,
            user_id=interaction.user.id,
            display_name=display_name,
            reason=reason,
            eta=_encode_afk_meta(meta),
        )
        await self.bot.refresh_panel(interaction.guild_id)

        await interaction.followup.send(
            f"Ты успешно встал(а) в АФК до {meta.display_eta}. Панель обновлена.",
            ephemeral=True,
        )


class ManageAFKSelect(discord.ui.Select["ManageAFKView"]):
    def __init__(self, bot: "AFKBot", guild_id: int, entries: list[Any]) -> None:
        self.bot = bot
        self.guild_id = guild_id
        self._entry_names = {entry.user_id: entry.display_name for entry in entries}

        options = []
        for entry in entries[:25]:
            options.append(
                discord.SelectOption(
                    label=_truncate(entry.display_name, 100),
                    value=str(entry.user_id),
                    description=_truncate(f"Причина: {entry.reason}", 100),
                )
            )

        super().__init__(
            placeholder="Выбери человека, которого нужно снять с АФК",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.guild_id is None:
            await interaction.response.send_message(
                "Эта функция доступна только на сервере.", ephemeral=True
            )
            return

        if not _interaction_is_admin(interaction):
            await interaction.response.send_message(
                "Только администратор может снимать людей с АФК.", ephemeral=True
            )
            return

        user_id = int(self.values[0])
        removed = await self.bot.storage.remove_afk(interaction.guild_id, user_id)
        if removed:
            await self.bot.refresh_panel(interaction.guild_id)
            name = self._entry_names.get(user_id, str(user_id))
            await interaction.response.edit_message(
                content=f"Пользователь **{discord.utils.escape_markdown(name)}** снят с АФК.",
                view=None,
            )
        else:
            await interaction.response.edit_message(
                content="Пользователь уже не находится в АФК.",
                view=None,
            )


class ManageAFKView(discord.ui.View):
    def __init__(self, bot: "AFKBot", guild_id: int, entries: list[Any]) -> None:
        super().__init__(timeout=180)
        self.add_item(ManageAFKSelect(bot, guild_id, entries))


class AFKPanelView(discord.ui.View):
    def __init__(self, bot: "AFKBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Встать АФК",
        style=discord.ButtonStyle.green,
        custom_id="afk:join",
        row=0,
    )
    async def join_afk(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button["AFKPanelView"],
    ) -> None:
        del button
        await interaction.response.send_modal(AFKModal(self.bot))

    @discord.ui.button(
        label="Выйти с АФК",
        style=discord.ButtonStyle.danger,
        custom_id="afk:leave",
        row=0,
    )
    async def leave_afk(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button["AFKPanelView"],
    ) -> None:
        del button
        if interaction.guild_id is None or interaction.user is None:
            await interaction.response.send_message(
                "Эта кнопка работает только на сервере.", ephemeral=True
            )
            return

        removed = await self.bot.storage.remove_afk(
            interaction.guild_id, interaction.user.id
        )
        if not removed:
            await interaction.response.send_message(
                "Ты сейчас не стоишь в АФК.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        await self.bot.refresh_panel(interaction.guild_id)
        await interaction.followup.send(
            "Ты вышел(а) из АФК. Панель обновлена.", ephemeral=True
        )

    @discord.ui.button(
        label="Обновить",
        style=discord.ButtonStyle.secondary,
        custom_id="afk:refresh",
        row=0,
    )
    async def refresh_panel_button(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button["AFKPanelView"],
    ) -> None:
        del button
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message(
                "Эта кнопка работает только на сервере.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)
        await self.bot.replace_panel(interaction)
        await interaction.followup.send(
            "AFK-панель переотправлена и обновлена.", ephemeral=True
        )

    @discord.ui.button(
        label="Управление",
        style=discord.ButtonStyle.blurple,
        custom_id="afk:manage",
        row=0,
    )
    async def manage_afk(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button["AFKPanelView"],
    ) -> None:
        del button
        if interaction.guild_id is None:
            await interaction.response.send_message(
                "Эта кнопка работает только на сервере.", ephemeral=True
            )
            return

        if not _interaction_is_admin(interaction):
            await interaction.response.send_message(
                "Эта кнопка доступна только администраторам сервера.",
                ephemeral=True,
            )
            return

        entries = await self.bot.storage.list_afk(interaction.guild_id)
        if not entries:
            await interaction.response.send_message(
                "Сейчас никто не стоит в АФК.", ephemeral=True
            )
            return

        view = ManageAFKView(self.bot, interaction.guild_id, entries)
        note = "Кого хочешь кикнуть с АФК?"
        if len(entries) > 25:
            note += "\nПоказаны только первые 25 человек из-за ограничения Discord для select-меню."
        await interaction.response.send_message(note, view=view, ephemeral=True)


class AFKCog(commands.Cog):
    def __init__(self, bot: "AFKBot") -> None:
        self.bot = bot

    @app_commands.command(name="hello", description="Публикует AFK-панель в текущем канале")
    @app_commands.guild_only()
    async def hello(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None or interaction.channel is None:
            await interaction.response.send_message(
                "Эта команда работает только на сервере.", ephemeral=True
            )
            return

        if not _interaction_is_admin(interaction):
            await interaction.response.send_message(
                f"Нет доступа. Твой ID: {interaction.user.id}",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        await self.bot.replace_panel(interaction)
        await interaction.followup.send(
            "AFK-панель опубликована или обновлена в этом канале.", ephemeral=True
        )

    @hello.error
    async def hello_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        logger.exception("Ошибка в /hello", exc_info=error)
        message = "Не удалось выполнить команду. Проверь логи бота."

        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)


class AFKBot(commands.Bot):
    def __init__(self, settings: Settings) -> None:
        intents = discord.Intents.none()
        intents.guilds = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self.settings = settings
        self.storage = Storage(settings.database_path)
        self._ticker_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        await self.storage.initialize()
        await self.add_cog(AFKCog(self))
        self.add_view(AFKPanelView(self))
        self._ticker_task = asyncio.create_task(self._panel_ticker_loop())

        if self.settings.guild_id:
            guild = discord.Object(id=self.settings.guild_id)
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            logger.info(
                "Команды синхронизированы с тестовым сервером %s: %s шт.",
                self.settings.guild_id,
                len(synced),
            )
        else:
            synced = await self.tree.sync()
            logger.info("Глобальные команды синхронизированы: %s шт.", len(synced))

    async def close(self) -> None:
        if self._ticker_task is not None:
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass
        await super().close()

    async def on_ready(self) -> None:
        if self.user is not None:
            logger.info("Бот вошёл как %s (%s)", self.user, self.user.id)

    async def replace_panel(self, interaction: discord.Interaction) -> None:
        assert interaction.guild is not None
        assert interaction.channel is not None

        existing = await self.storage.get_panel(interaction.guild.id)
        if existing:
            old_message = await self._fetch_message(existing.channel_id, existing.message_id)
            if old_message is not None:
                try:
                    await old_message.delete()
                except discord.HTTPException:
                    logger.warning(
                        "Не удалось удалить старую AFK-панель %s в гильдии %s",
                        existing.message_id,
                        interaction.guild.id,
                    )

        entries = await self.storage.list_afk(interaction.guild.id)
        message = await interaction.channel.send(
            embed=EmbedFactory.build_panel(entries),
            view=AFKPanelView(self),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await self.storage.set_panel(
            guild_id=interaction.guild.id,
            channel_id=message.channel.id,
            message_id=message.id,
            created_by=interaction.user.id,
        )

    async def refresh_panel(self, guild_id: int) -> None:
        panel = await self.storage.get_panel(guild_id)
        if panel is None:
            return

        message = await self._fetch_message(panel.channel_id, panel.message_id)
        if message is None:
            logger.warning(
                "Панель для guild_id=%s не найдена, запись будет очищена.", guild_id
            )
            await self.storage.clear_panel(guild_id)
            return

        entries = await self.storage.list_afk(guild_id)
        try:
            await message.edit(
                embed=EmbedFactory.build_panel(entries),
                view=AFKPanelView(self),
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.HTTPException:
            logger.exception("Не удалось обновить AFK-панель для guild_id=%s", guild_id)

    async def _fetch_message(
        self,
        channel_id: int,
        message_id: int,
    ) -> discord.Message | None:
        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return None

        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return None

        try:
            return await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None

    async def _panel_ticker_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                for guild in list(self.guilds):
                    should_refresh = await self._process_guild_afk(guild.id)
                    if should_refresh:
                        await self.refresh_panel(guild.id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ошибка в цикле обновления AFK-панели")

            await asyncio.sleep(1)

    async def _process_guild_afk(self, guild_id: int) -> bool:
        entries = await self.storage.list_afk(guild_id)
        if not entries:
            return False

        now_ts = int(_now_msk().timestamp())
        changed = False
        active_entries: list[Any] = []

        for entry in entries:
            meta = _resolve_entry_meta(entry)
            if meta is not None and meta.end_ts <= now_ts:
                removed = await self.storage.remove_afk(guild_id, entry.user_id)
                changed = changed or removed
                continue
            active_entries.append(entry)

        return bool(active_entries) or changed



def _display_name(user: discord.abc.User) -> str:
    if isinstance(user, discord.Member):
        return user.display_name
    return getattr(user, "global_name", None) or user.name



def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return text[: limit - 1] + "…"



def _is_admin(user: discord.abc.User) -> bool:
    return user.id in ALWAYS_ADMIN_IDS or (
        isinstance(user, discord.Member) and user.guild_permissions.administrator
    )



def _interaction_is_admin(interaction: discord.Interaction) -> bool:
    return interaction.user.id in ALWAYS_ADMIN_IDS or interaction.permissions.administrator



def _now_msk() -> datetime:
    return datetime.now(MSK)



def _parse_user_eta_input(value: str) -> AFKMeta:
    text = value.strip()
    now = _now_msk()
    start_ts = int(now.timestamp())

    if MINUTES_RE.fullmatch(text):
        minutes = int(text)
        if minutes <= 0:
            raise ValueError("Минуты должны быть больше нуля.")
        end_dt = now + timedelta(minutes=minutes)
        display = end_dt.strftime("%d.%m.%Y %H:%M MSK")
        return AFKMeta(raw_input=text, start_ts=start_ts, end_ts=int(end_dt.timestamp()), display_eta=display)

    time_match = TIME_RE.fullmatch(text)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        if hour > 23 or minute > 59:
            raise ValueError("Время должно быть в формате ЧЧ:ММ по Москве.")
        end_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if end_dt <= now:
            raise ValueError("Нельзя поставить выход из АФК на прошедшее время.")
        display = end_dt.strftime("%d.%m.%Y %H:%M MSK")
        return AFKMeta(raw_input=text, start_ts=start_ts, end_ts=int(end_dt.timestamp()), display_eta=display)

    dt_match = DATETIME_RE.fullmatch(text)
    if dt_match:
        day = int(dt_match.group(1))
        month = int(dt_match.group(2))
        year = int(dt_match.group(3))
        hour = int(dt_match.group(4))
        minute = int(dt_match.group(5))
        try:
            end_dt = datetime(year, month, day, hour, minute, tzinfo=MSK)
        except ValueError as exc:
            raise ValueError("Дата должна быть реальной и в формате ДД.ММ.ГГГГ ЧЧ:ММ.") from exc
        if end_dt <= now:
            raise ValueError("Нельзя поставить выход из АФК на прошедшую дату или время.")
        display = end_dt.strftime("%d.%m.%Y %H:%M MSK")
        return AFKMeta(raw_input=text, start_ts=start_ts, end_ts=int(end_dt.timestamp()), display_eta=display)

    raise ValueError(
        "Разрешены только форматы: 30 / 60 / 18:35 / 15.03.2026 18:00. Другие символы и текст нельзя."
    )



def _encode_afk_meta(meta: AFKMeta) -> str:
    payload = {
        "v": 1,
        "raw": meta.raw_input,
        "start_ts": meta.start_ts,
        "end_ts": meta.end_ts,
        "display_eta": meta.display_eta,
    }
    return ETA_META_PREFIX + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))



def _decode_afk_meta(value: str) -> AFKMeta | None:
    if not value.startswith(ETA_META_PREFIX):
        return None
    try:
        payload = json.loads(value[len(ETA_META_PREFIX):])
    except json.JSONDecodeError:
        return None

    try:
        raw_input = str(payload["raw"])
        start_ts = int(payload["start_ts"])
        end_ts = int(payload["end_ts"])
        display_eta = str(payload["display_eta"])
    except (KeyError, TypeError, ValueError):
        return None

    return AFKMeta(
        raw_input=raw_input,
        start_ts=start_ts,
        end_ts=end_ts,
        display_eta=display_eta,
    )



def _resolve_entry_meta(entry: Any) -> AFKMeta | None:
    eta_value = str(getattr(entry, "eta", "") or "")
    meta = _decode_afk_meta(eta_value)
    if meta is not None:
        return meta

    start_ts = _extract_start_ts(entry)
    if start_ts is None:
        return None

    raw_input = eta_value.strip()
    if not raw_input:
        return None

    start_dt = datetime.fromtimestamp(start_ts, tz=MSK)
    end_dt = _derive_end_datetime_from_raw(raw_input, start_dt)
    if end_dt is None:
        return None

    return AFKMeta(
        raw_input=raw_input,
        start_ts=start_ts,
        end_ts=int(end_dt.timestamp()),
        display_eta=end_dt.strftime("%d.%m.%Y %H:%M MSK"),
    )



def _extract_start_ts(entry: Any) -> int | None:
    for attr_name in ("started_at", "created_at", "afk_since", "entered_at", "timestamp"):
        value = getattr(entry, attr_name, None)
        parsed = _coerce_timestamp(value)
        if parsed is not None:
            return parsed
    return None



def _coerce_timestamp(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=MSK)
        return int(value.astimezone(MSK).timestamp())
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return int(stripped)
        try:
            parsed = datetime.fromisoformat(stripped)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=MSK)
        return int(parsed.astimezone(MSK).timestamp())
    return None



def _derive_end_datetime_from_raw(raw_input: str, start_dt: datetime) -> datetime | None:
    if MINUTES_RE.fullmatch(raw_input):
        return start_dt + timedelta(minutes=int(raw_input))

    time_match = TIME_RE.fullmatch(raw_input)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        if hour > 23 or minute > 59:
            return None
        return start_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)

    dt_match = DATETIME_RE.fullmatch(raw_input)
    if dt_match:
        day = int(dt_match.group(1))
        month = int(dt_match.group(2))
        year = int(dt_match.group(3))
        hour = int(dt_match.group(4))
        minute = int(dt_match.group(5))
        try:
            return datetime(year, month, day, hour, minute, tzinfo=MSK)
        except ValueError:
            return None

    return None
