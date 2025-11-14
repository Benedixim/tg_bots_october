# main.py
import os
import time
import threading
import datetime as dt
from collections import defaultdict

import sqlite3
import matplotlib.pyplot as plt
from flask import Flask
import asyncio
from aiohttp import ClientSession

import telebot
from telebot import types

from db_sql import (
    get_banks,
    get_latest_categories_by_bank,
    get_partners_latest_by_bank_category,
    search_partners_latest,
    get_partner_counts_by_bank,
    get_bank_name,  
    backup_database,   # <— НОВОЕ
    remember_user, 
    get_all_chat_ids, 
    get_today_partner_changes,
    ensure_tg_users_table,
    fetch_partners_scrape_config
)

from updates import update_all_banks_categories


# ---------- Telegram Bot ----------
TOKEN = os.getenv("TELEGRAM_TOKEN", "8176791165:AAFeivYr8ipnSI0m0yZ8IlLrkCuYHPMbZ0k")
bot = telebot.TeleBot(TOKEN)


# ---------- Plot ----------
def plot_partners_by_bank(bank_id: int) -> str:
    bank_name = get_bank_name(bank_id)
    data = get_partner_counts_by_bank(bank_id)
    categories = [row[0] for row in data]
    counts = [row[1] for row in data]

    plt.figure(figsize=(12, 6))
    bars = plt.bar(categories, counts)
    plt.xlabel("Категории")
    plt.ylabel("Количество партнёров")
    plt.title(f"Партнёры по категориям — {bank_name}")  # ← название банка в заголовке
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.tight_layout()

    # подписи над столбцами
    for bar, value in zip(bars, counts):
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, h, f"{int(value)}", ha="center", va="bottom", fontsize=9)

    # уникальное имя файла
    safe_name = "".join(ch for ch in bank_name if ch.isalnum() or ch in ("_", "-")).strip("_-")
    out = f"partners_chart_{bank_id}_{safe_name}.png"
    plt.savefig(out)
    plt.close()
    return out


# ---------- Bot Handlers ----------
@bot.message_handler(commands=['start'])
def start_message(message):
    remember_user(message.chat.id) # запоминаем
    banks = get_banks()
    if not banks:
        bot.send_message(message.chat.id, "Банки не найдены.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"bank_{bank_id}"))
    bot.send_message(message.chat.id, "Выберите банк:", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('bank_'))
def callback_bank(call):
    bank_id = int(call.data[5:])
    banks = get_banks()
    selected = next((b for b in banks if b[0] == bank_id), None)
    if selected:
        name, loyalty_url = selected[1], selected[2]
        if loyalty_url:
            bot.send_message(call.message.chat.id, f"Ссылка на программу лояльности: {loyalty_url}")

    categories = get_latest_categories_by_bank(bank_id)
    if not categories:
        bot.send_message(call.message.chat.id, "Нет категорий у данного банка.")
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    for cat_id, cat_name, cat_url in categories:
        markup.add(types.InlineKeyboardButton(cat_name, callback_data=f"cat_{bank_id}_{cat_id}"))
    bot.send_message(call.message.chat.id, "Выберите категорию партнёра:", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('cat_'))
def callback_category(call):
    _, bank_id, cat_id = call.data.split('_', 2)
    partners = get_partners_latest_by_bank_category(int(bank_id), int(cat_id))
    if not partners:
        bot.send_message(call.message.chat.id, "Нет партнёров для этой категории.")
        return

     # ← НОВОЕ: берём bonus_unit через db_sql, а не через sqlite напрямую
    cfg = fetch_partners_scrape_config(bank_id)
    bonus_unit = cfg.get("bonus_unit", "") or ""

    reply = "Партнёры данной категории:\n\n"
    for name, bonus, link in partners:
        shown_link = link or "#"

        # ✅ как ты хотел: выводим бонус только если он есть, в формате как в /search
        bonus_display = f" — {bonus} {bonus_unit}".strip() if bonus else ""

        reply += f"- [{name}]({shown_link}){bonus_display}\n"
    # reply = "Партнёры данной категории:\n\n"
    # for name, bonus, link in partners:
    #     if link and not link.startswith("http"):
    #         # если понадобится, можно хранить домен в banks и добавлять его тут
    #         pass
    #     bonus_display = bonus if bonus else "—"
    #     shown_link = link if link else "#"
    #     reply += f"- [{name}]({shown_link}) — бонус: {bonus_display}\n"


        #не знает
        #bonus_disp = f" — {bonus} {bonus_unit}".strip() if bonus else ""
        #lines.append(f"- [{name}]({shown_link}){bonus_disp}")

    bot.send_message(call.message.chat.id, reply, parse_mode='Markdown', disable_web_page_preview=True)


@bot.message_handler(commands=['graph'])
def graph_start(message):
    remember_user(message.chat.id) # запоминаем
    banks = get_banks()
    if not banks:
        bot.send_message(message.chat.id, "Банки не найдены.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"graphbank_{bank_id}"))
    bot.send_message(message.chat.id, "Выберите банк для графика:", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('graphbank_'))
def callback_graphbank(call):
    bank_id = int(call.data.split('_')[1])
    bank_name = get_bank_name(bank_id)
    file_path = plot_partners_by_bank(bank_id)
    with open(file_path, "rb") as photo:
        bot.send_photo(
            call.message.chat.id,
            photo,
            caption=f"График партнёров по категориям — {bank_name}"  # ← подпись с названием банка
        )


@bot.message_handler(commands=['search'])
def search_command(message):
    remember_user(message.chat.id) # запоминаем
    msg = bot.send_message(message.chat.id, "Введите имя партнёра для поиска:")
    bot.register_next_step_handler(msg, perform_search)


def perform_search(message):
    query = message.text.strip()
    if not query:
        bot.send_message(message.chat.id, "Пустой запрос. Введите имя снова командой /search.")
        return

    results = search_partners_latest(query)
    if not results:
        bot.send_message(message.chat.id, f"Ничего не найдено по запросу «{query}».")
        return

    # grouped[bank][category] = list(partners)
    grouped = defaultdict(lambda: defaultdict(list))
    for bank_name, category_name, partner_name, bonus, bonus_unit, link in results:
        grouped[bank_name][category_name].append({
            "name": partner_name,
            "bonus": bonus,
            "bonus_unit": bonus_unit,
            "link": link or "#",
        })

    lines = [f"🔎 Найдено совпадений: {len(results)}"]
    for bank, cats in grouped.items():
        lines.append(f"\n🏦 *{bank}*")
        for category, partners in cats.items():
            lines.append(f"  → _{category}_")
            for p in partners:
                bonus_disp = f" — {p['bonus']} {p['bonus_unit']}".strip() if p['bonus'] else ""
                lines.append(f"    [{p['name']}]({p['link']}) {bonus_disp}")

    bot.send_message(message.chat.id, "\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)


# ---------- Nightly Scheduler (01:00) ----------
def _seconds_until_next_1am(now: dt.datetime | None = None) -> int:
    now = now or dt.datetime.now()
    target_date = now.date()
    if now.hour >= 1:
        target_date = target_date + dt.timedelta(days=1)
    target_dt = dt.datetime.combine(target_date, dt.time(1, 0, 0))
    return max(1, int((target_dt - now).total_seconds()))


def nightly_scrape_loop():
    while True:
        wait_s = _seconds_until_next_1am()
        time.sleep(wait_s)
        try:
            print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] ▶️ Nightly categories update")
            update_all_banks_categories()
            print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] ✅ Nightly update done")
        except Exception as e:
            print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Nightly update error: {e}")

# секрет можно переопределить через переменную окружения UPDATE_SECRET
UPDATE_SECRET = os.getenv("UPDATE_SECRET", "qwerty11")
_update_lock = threading.Lock()
_update_running = False

def _run_manual_update_with_progress(chat_id: int):
    global _update_running
    try:
        # 1) Отправляем стартовое сообщение
        msg = bot.send_message(chat_id, "🔄 Запускаю ручное обновление…")

        # 2) Локальная функция для обновления прогресса
        def tg_progress(done: int, total: int, note: str):
            # защита от деления на ноль
            total = max(1, total)
            pct = int(done * 100 / total)
            width = 20  # ширина «полосы»
            filled = int(width * pct / 100)
            bar = "▓" * filled + "░" * (width - filled)
            text = (
                f"🔄 Обновление категорий и партнёров\n"
                f"[{bar}] {pct}% ({done}/{total})\n"
                f"{note}"
            )
            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    text=text
                )
            except Exception:
                # редактирование может падать при частых апдейтах — игнорируем
                pass

        # 3) Запуск обновления с прогрессом
        tg_progress(0, 1, "Подготовка…")
        update_all_banks_categories(progress=tg_progress)

        # 4) Финальный штрих
        tg_progress(1, 1, "Готово ✅")
        bot.send_message(chat_id, "✅ Ручное обновление завершено.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при ручном обновлении: {e}")
    finally:
        _update_running = False
        try:
            _update_lock.release()
        except RuntimeError:
            pass

@bot.message_handler(commands=['update'])
def update_command(message):
    global _update_running
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip() != UPDATE_SECRET:
        bot.send_message(message.chat.id, "⛔️ Неверный секрет. Формат: /update <secret>")
        return

    if _update_running:
        bot.send_message(message.chat.id, "⏳ Обновление уже выполняется. Дождитесь завершения.")
        return

    if not _update_lock.acquire(blocking=False):
        bot.send_message(message.chat.id, "⏳ Обновление уже выполняется. Дождитесь завершения.")
        return

    _update_running = True
    threading.Thread(
        target=_run_manual_update_with_progress,
        args=(message.chat.id,),
        daemon=True
    ).start()


#------------- Скачивание БД --------------

# --- Secure DB download (/db, /dump, /downloaddb) ---
DB_DOWNLOAD_SECRET = os.getenv("DB_DOWNLOAD_SECRET", "qwerty11")

def _send_db_backup(chat_id: int):
    try:
        bot.send_message(chat_id, "📦 Готовлю резервную копию базы…")
        backup_path = backup_database(dest_dir=".")
        caption = f"Резервная копия базы данных: {os.path.basename(backup_path)}"
        with open(backup_path, "rb") as f:
            bot.send_document(chat_id, f, caption=caption)
        # при желании можно удалять временный файл:
        # os.remove(backup_path)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при подготовке бэкапа: {e}")

@bot.message_handler(commands=['db', 'dump', 'downloaddb'])
def download_db_command(message):
    # ожидаем формат: "/db <secret>"
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip() != DB_DOWNLOAD_SECRET:
        bot.send_message(message.chat.id, "⛔️ Неверный секрет. Формат: /db <secret>")
        return

    # делаем бэкап и отправляем его в отдельном потоке, чтобы не блокировать бота
    threading.Thread(target=_send_db_backup, args=(message.chat.id,), daemon=True).start()



# ---------- Morning ---------------------
from collections import defaultdict

def format_changes_message(changes: list[dict]) -> str:
    """
    Формирует красивый Markdown-вывод в стиле /search:
    сгруппировано по банкам и категориям.
    Ожидает элементы:
    {
        "bank_name": str,
        "category_name": str,
        "partner_name": str,
        "partner_bonus": str | None,
        "change_type": "new" | "updated",
        "checked_at": "YYYY-MM-DD HH:MM:SS",
    }
    """
    if not changes:
        return ""

    grouped = defaultdict(lambda: defaultdict(list))
    total_new = 0
    total_updated = 0

    for ch in changes:
        bank = ch["bank_name"]
        cat = ch["category_name"]
        grouped[bank][cat].append(ch)
        if ch["change_type"] == "new":
            total_new += 1
        else:
            total_updated += 1

    total = total_new + total_updated

    lines: list[str] = []
    # шапка
    lines.append(
        f"🔔 Обновления программы лояльности за сегодня:\n"
        f"• всего: *{total}* партнёров "
        f"(_{total_new} новых_, _{total_updated} обновлено_)\n"
    )

    # как в /search: банк → категория → партнёры
    for bank, cats in grouped.items():
        lines.append(f"\n🏦 *{bank}*")
        for category, partners in cats.items():
            lines.append(f"  → _{category}_")
            for p in partners:
                bonus_disp = f" — {p['partner_bonus']}%" if p["partner_bonus"] else ""
                emoji = "🆕" if p["change_type"] == "new" else "🔁"
                # здесь ссылок нет, поэтому без [name](link)
                lines.append(f"    {emoji} {p['partner_name']}{bonus_disp}")

    return "\n".join(lines).strip()



def _seconds_until_next_7am(now: dt.datetime | None = None) -> int:
    now = now or dt.datetime.now()
    target_date = now.date()
    if now.hour >= 7:
        target_date = target_date + dt.timedelta(days=1)
    target_dt = dt.datetime.combine(target_date, dt.time(7, 0, 0))
    return max(1, int((target_dt - now).total_seconds()))

def morning_digest_loop():
    from db_sql import get_today_partner_changes  # если в отдельном модуле
    ensure_tg_users_table()  # на всякий случай

    while True:
        wait_s = _seconds_until_next_7am()
        time.sleep(wait_s)

        try:
            now = dt.datetime.now()
            print(f"[{now:%Y-%m-%d %H:%M:%S}] ▶️ Morning digest start")

            changes = get_today_partner_changes()
            if not changes:
                print(f"[{now:%Y-%m-%d %H:%M:%S}] ℹ️ Morning digest: изменений нет")
                continue

            text = format_changes_message(changes)
            if not text:
                print(f"[{now:%Y-%m-%d %H:%M:%S}] ℹ️ Morning digest: нечего отправлять")
                continue

            chat_ids = get_all_chat_ids()
            print(f"[{now:%Y-%m-%d %H:%M:%S}] ▶️ Отправляем дайджест {len(chat_ids)} пользователям")

            for chat_id in chat_ids:
                try:
                    # на всякий случай режем по 4000 символов
                    chunk = 3500
                    for i in range(0, len(text), chunk):
                        bot.send_message(chat_id, text[i:i+chunk])
                except Exception as e:
                    print(f"[{now:%Y-%m-%d %H:%M:%S}] ⚠️ Ошибка отправки дайджеста chat_id={chat_id}: {e}")

            print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] ✅ Morning digest done")
        except Exception as e:
            print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] ❌ Morning digest error: {e}")

# ----------------- ручной morning --------------------------- 

# ---------- Ручной запуск утренней рассылки (/morning <secret>) ----------

_morning_lock = threading.Lock()
_morning_running = False


def _run_manual_morning_digest(chat_id: int):
    """
    Однократная отправка утреннего дайджеста ТОЛЬКО пользователю,
    который вызвал команду /morning <secret>.
    """
    global _morning_running
    try:
        msg = bot.send_message(chat_id, "📨 Формирую утренний дайджест…")

        # 1. Берём изменения за сегодня
        changes = get_today_partner_changes()
        if not changes:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ За сегодня нет новых или изменённых партнёров. Дайджест не требуется."
            )
            return

        # 2. Формируем текст
        text = format_changes_message(changes)
        if not text:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ Не удалось сформировать текст дайджеста."
            )
            return

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg.message_id,
            text="📨 Отправляю дайджест…"
        )

        # 3. Отправляем ТОЛЬКО этому пользователю
        chunk = 3500  # чтобы не упереться в лимит Telegram
        for i in range(0, len(text), chunk):
            bot.send_message(chat_id, text[i:i + chunk])

        bot.send_message(chat_id, "✅ Утренний дайджест отправлен.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при ручном запуске дайджеста: {e}")
    finally:
        _morning_running = False
        try:
            _morning_lock.release()
        except RuntimeError:
            pass

@bot.message_handler(commands=['morning'])
def morning_command(message):
    """
    Ручной запуск утренней рассылки только для отправителя.
    Формат: /morning <secret> (секрет тот же, что и UPDATE_SECRET).
    """
    global _morning_running

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2 or parts[1].strip() != UPDATE_SECRET:
        bot.send_message(message.chat.id, "⛔️ Неверный секрет. Формат: /morning <secret>")
        return

    if _morning_running:
        bot.send_message(message.chat.id, "⏳ Утренняя рассылка уже выполняется. Дождитесь завершения.")
        return

    if not _morning_lock.acquire(blocking=False):
        bot.send_message(message.chat.id, "⏳ Утренняя рассылка уже выполняется. Дождитесь завершения.")
        return

    _morning_running = True
    threading.Thread(
        target=_run_manual_morning_digest,
        args=(message.chat.id,),
        daemon=True
    ).start()


# ---------- KeepAlive + Flask ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"


async def keep_alive():
    url = os.getenv("KEEPALIVE_URL", "https://tg-bots-october.onrender.com/")
    while True:
        try:
            async with ClientSession() as session:
                async with session.get(url) as resp:
                    print(f"[KeepAlive] Ping {url} → {resp.status}")
        except Exception as e:
            print(f"[KeepAlive] Error: {e}")
        await asyncio.sleep(300)


def start_keep_alive():
    asyncio.run(keep_alive())


def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))


def run_bot():
    bot.polling(none_stop=True)


if __name__ == "__main__":
    # Flask
    threading.Thread(target=run_flask, daemon=True).start()
    # KeepAlive
    threading.Thread(target=start_keep_alive, daemon=True).start()
    # Nightly scraper
    threading.Thread(target=nightly_scrape_loop, daemon=True).start()
    # Morning scraper
    threading.Thread(target=morning_digest_loop, daemon=True).start()
    # Bot
    run_bot()
