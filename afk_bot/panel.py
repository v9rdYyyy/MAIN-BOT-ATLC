from __future__ import annotations

import ast
import json
from datetime import datetime
from typing import Any

import discord


class EmbedFactory:
    FIELD_LIMIT = 1024
    TOTAL_LIMIT = 6000
    MAX_FIELDS = 25

    @staticmethod
    def _safe_text(value: Any, default: str) -> str:
        if value is None:
            return default
        text = str(value).strip()
        if not text:
            return default
        return discord.utils.escape_markdown(text)

    @staticmethod
    def _dt_to_ts(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value > 0 else None
        if isinstance(value, float):
            ivalue = int(value)
            return ivalue if ivalue > 0 else None
        if isinstance(value, datetime):
            return int(value.timestamp())
        text = str(value).strip()
        if not text:
            return None
        try:
            if text.isdigit():
                ivalue = int(text)
                return ivalue if ivalue > 0 else None
            return int(float(text))
        except (TypeError, ValueError):
            return None

    @classmethod
    def _parse_meta_string(cls, raw: str) -> dict[str, Any] | None:
        text = raw.strip()
        if not text:
            return None
        if text.startswith("afkmeta:"):
            text = text[len("afkmeta:") :].strip()

        for loader in (json.loads, ast.literal_eval):
            try:
                value = loader(text)
            except Exception:
                continue
            if isinstance(value, dict):
                return value
        return None

    @classmethod
    def _resolve_meta(cls, entry: Any) -> dict[str, Any] | None:
        for attr in ("meta", "afk_meta", "metadata"):
            value = getattr(entry, attr, None)
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                parsed = cls._parse_meta_string(value)
                if parsed:
                    return parsed

        eta_value = getattr(entry, "eta", None)
        if isinstance(eta_value, str):
            parsed = cls._parse_meta_string(eta_value)
            if parsed:
                return parsed

        return None

    @classmethod
    def _get_start_ts(cls, entry: Any, meta: dict[str, Any] | None) -> int | None:
        if meta:
            for key in ("start_ts", "started_ts", "since_ts", "created_ts"):
                ts = cls._dt_to_ts(meta.get(key))
                if ts:
                    return ts

        for attr in (
            "start_ts",
            "started_ts",
            "since_ts",
            "created_ts",
            "started_at",
            "created_at",
            "afk_since",
            "entered_at",
            "timestamp",
        ):
            ts = cls._dt_to_ts(getattr(entry, attr, None))
            if ts:
                return ts

        return None

    @classmethod
    def _get_end_ts(cls, entry: Any, meta: dict[str, Any] | None) -> int | None:
        if meta:
            for key in ("end_ts", "until_ts", "eta_ts"):
                ts = cls._dt_to_ts(meta.get(key))
                if ts:
                    return ts

        for attr in ("end_ts", "until_ts", "eta_ts"):
            ts = cls._dt_to_ts(getattr(entry, attr, None))
            if ts:
                return ts

        return None

    @classmethod
    def _format_eta(cls, entry: Any, meta: dict[str, Any] | None) -> str:
        end_ts = cls._get_end_ts(entry, meta)
        if end_ts:
            return f"<t:{end_ts}:f> (<t:{end_ts}:R>)"

        if meta:
            display_eta = cls._safe_text(meta.get("display_eta"), "")
            if display_eta:
                return display_eta
            raw_meta = cls._safe_text(meta.get("raw"), "")
            if raw_meta:
                return raw_meta

        raw_eta = getattr(entry, "eta", None)
        if isinstance(raw_eta, str) and raw_eta.strip().startswith("afkmeta:"):
            return "Не указано"

        return cls._safe_text(raw_eta, "Не указано")

    @classmethod
    def _entry_line(cls, index: int, entry: Any) -> str:
        user_id = getattr(entry, "user_id", None)
        mention_or_name = f"<@{user_id}>" if user_id else cls._safe_text(
            getattr(entry, "display_name", None), "Неизвестно"
        )
        reason = cls._safe_text(getattr(entry, "reason", None), "Не указана")
        meta = cls._resolve_meta(entry)
        start_ts = cls._get_start_ts(entry, meta)
        eta_text = cls._format_eta(entry, meta)

        lines = [
            f"**{index}.** {mention_or_name}",
            f"Причина: {reason}",
        ]

        if start_ts:
            lines.append(f"Встал в АФК: <t:{start_ts}:f>")
            lines.append(f"Прошло: <t:{start_ts}:R>")

        lines.append(f"Когда вернётся: {eta_text}")
        return "\n".join(lines)

    @classmethod
    def build_panel(cls, entries) -> discord.Embed:
        embed = discord.Embed(
            title="AFK-панель",
            description="Нажми кнопку ниже, чтобы встать в АФК или выйти из него.",
            color=discord.Color.blurple(),
        )

        if not entries:
            embed.add_field(
                name="Сейчас в АФК",
                value="Никто не стоит в АФК.",
                inline=False,
            )
            return embed

        chunks: list[str] = []
        current = ""
        shown = 0

        for index, entry in enumerate(entries, start=1):
            line = cls._entry_line(index, entry)
            candidate = line if not current else f"{current}\n\n{line}"

            if len(candidate) <= cls.FIELD_LIMIT:
                current = candidate
                shown = index
                continue

            if current:
                chunks.append(current)
                if len(chunks) >= cls.MAX_FIELDS:
                    break
                current = line if len(line) <= cls.FIELD_LIMIT else line[: cls.FIELD_LIMIT - 1] + "…"
                shown = index
                continue

            chunks.append(line[: cls.FIELD_LIMIT - 1] + "…")
            shown = index
            if len(chunks) >= cls.MAX_FIELDS:
                break
            current = ""

        if current and len(chunks) < cls.MAX_FIELDS:
            chunks.append(current)

        total_chars = len(embed.title or "") + len(embed.description or "")
        used_chunks: list[str] = []

        for chunk in chunks:
            extra = len("Сейчас в АФК") + len(chunk)
            if total_chars + extra > cls.TOTAL_LIMIT:
                break
            used_chunks.append(chunk)
            total_chars += extra

        hidden_count = max(0, len(entries) - shown)
        if hidden_count and used_chunks:
            suffix = f"\n\n*И ещё {hidden_count} чел.*"
            last = used_chunks[-1]
            if len(last) + len(suffix) <= cls.FIELD_LIMIT:
                used_chunks[-1] = last + suffix
            else:
                overflow_text = f"И ещё {hidden_count} чел."
                if (
                    len(used_chunks) < cls.MAX_FIELDS
                    and total_chars + len("Сейчас в АФК (ещё)") + len(overflow_text)
                    <= cls.TOTAL_LIMIT
                ):
                    used_chunks.append(overflow_text)

        for idx, chunk in enumerate(used_chunks, start=1):
            field_name = "Сейчас в АФК" if idx == 1 else f"Сейчас в АФК (часть {idx})"
            embed.add_field(name=field_name, value=chunk, inline=False)

        embed.set_footer(text=f"Всего в АФК: {len(entries)}")
        return embed
