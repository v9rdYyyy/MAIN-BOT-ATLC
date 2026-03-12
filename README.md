# Unified Discord Bot

Один бот, объединяющий функции из трёх проектов:
- заявки в семью;
- AFK-панель;
- сборы.

## Что внутри
- `main.py` — единая точка запуска
- `family_bot_module.py` — логика заявок
- `afk_bot/` — модуль AFK
- `sbornik_bot/` — модуль сборов

## Запуск
```bash
pip install -r requirements.txt
cp main.py main.py
python main.py
```

## Важно
- используется **один токен** для всех функций;
- базы AFK и сборов разнесены по отдельным SQLite-файлам;
- семейный модуль использует свой файл `family_bot.sqlite3` рядом с проектом, как и в исходнике.

## Команды
### Семья / заявки
- `/family_setup`
- `/family_panel`
- `/family_panel_image`
- `/family_recruitment`
- `/family_cooldown`
- `/family_config`
- `/family_sync`
- `/family_archive_find`

### AFK
- `/hello`

### Сборы
- `/сбор`
- `/логи`
- `/стоплоги`
