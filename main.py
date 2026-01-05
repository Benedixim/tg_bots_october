# main.py
import os
from dotenv import load_dotenv
import time
import threading
import datetime as dt
from collections import defaultdict

import sqlite3
import matplotlib.pyplot as plt
from flask import Flask
import asyncio
from aiohttp import ClientSession
from update_nw import fetch_categories_for_bank

import db_sql
import telebot
from telebot import types
from db_sql import (
    get_banks,
    fix_status_problems,
    get_latest_categories_by_bank,
    get_partners_latest_by_bank_category,
    search_partners_latest,
    get_partner_counts_by_bank,
    search_partners,
    get_bank_name,  
    backup_database,   # <— НОВОЕ
    remember_user, 
    get_all_chat_ids, 
    get_today_partner_changes,
    ensure_tg_users_table,
    fetch_partners_scrape_config,
    get_categories,
    get_banks_name,
    get_test_digest_data,
    ensure_status_columns,
    prepare_statuses_for_update,
    finalize_statuses_after_update,
    get_status_report,
    get_today_changes_with_status,
    get_special_banks,
    DB_PATH
)

from update_nw import update_all_banks_categories

# ---------- Load .env ----------
load_dotenv()


# ---------- Telegram Bot -----------
TOKEN = os.getenv('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)


# ---------- Plot ----------
# Замените эту функцию в main.py

def plot_partners_by_bank(bank_id: int) -> str:

    bank_name = get_bank_name(bank_id)
    data = get_partner_counts_by_bank(bank_id)
    

    data = [(cat, count) for cat, count in data if cat and cat.strip() and count > 0]
    
    seen = {}
    unique_data = []
    for cat, count in data:
        cat_lower = cat.lower().strip()
        if cat_lower not in seen:
            seen[cat_lower] = (cat, count)
            unique_data.append((cat, count))
    
    data = unique_data
    
    if not data:
        print(f"Нет данных для графика банка {bank_name}")
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.png') as f:
            return f.name
    
    categories = [row[0] for row in data]
    counts = [row[1] for row in data]

    print(f"   Создаю график для {bank_name}")
    print(f"   Категорий: {len(categories)}")
    print(f"   Данные: {list(zip(categories, counts))}")

    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(categories)), counts, color='#1f77b4')
    
    plt.xlabel("Категории", fontsize=12)
    plt.ylabel("Количество партнёров", fontsize=12)
    plt.title(f"Партнёры по категориям — {bank_name}", fontsize=14, fontweight='bold')
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    

    plt.xticks(range(len(categories)), categories, rotation=45, ha='right', fontsize=10)
    plt.tight_layout()


    for i, (bar, value) in enumerate(zip(bars, counts)):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height, f"{int(value)}", 
                ha="center", va="bottom", fontsize=10, fontweight='bold')


    safe_name = "".join(ch for ch in bank_name if ch.isalnum() or ch in ("_", "-")).strip("_-")
    out = f"partners_chart_{bank_id}_{safe_name}.png"
    
    print(f"   Сохраняю в: {out}")
    plt.savefig(out, dpi=100, bbox_inches='tight')
    plt.close()
    
    return out


# ---------- Bot Handlers ----------
@bot.message_handler(func=lambda message: message.text == "🏦 Выбрать банк")
def start_message(message):
    remember_user(message.chat.id) # запоминаем
    banks = get_banks()
    if not banks:
        bot.send_message(message.chat.id, "Банки не найдены.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        print(f'{bank_id}, {name}')
        if bank_id != 13 and bank_id != 6 and bank_id != 1:
            name += " - С" 
        markup.add(types.InlineKeyboardButton(name, callback_data=f"bank_{bank_id}"))
    bot.send_message(message.chat.id, "Выберите банк:", reply_markup=markup)

def send_main_menu(bot, chat_id):
    """
    Отправляет главное меню с кнопками
    """
    # Создаем объект клавиатуры
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    # Создаем кнопки
    btn1 = types.KeyboardButton("🏦 Выбрать банк")
    btn2 = types.KeyboardButton("🔍 Найти партнёра")
    btn3 = types.KeyboardButton("📊 Построить график")
    
    # Добавляем кнопки в клавиатуру
    # Можно добавлять по одной или списком
    markup.add(btn1, btn2, btn3)
    # Или построчно:
    # markup.row(btn1)
    # markup.row(btn2, btn3)
    
    # Отправляем сообщение с клавиатурой
    bot.send_message(
        chat_id, 
        "Выберите действие:", 
        reply_markup=markup
    )

#Add Buttons to All Users (/addbuttons <secret>) 

@bot.message_handler(commands=['addbuttons'])
def add_buttons_to_all_users(message):
    parts = message.text.strip().split()
    if len(parts) < 2 or parts[1] != 'qwerty11':
        return  
    

    bot.send_message(message.chat.id, "Начинаю добавлять кнопки всем пользователям...")
    

    all_users = get_all_chat_ids()
    
    if not all_users:
        bot.send_message(message.chat.id, "Нет пользователей в базе")
        return
    
    bot.send_message(message.chat.id, f"Найдено {len(all_users)} пользователей")
    
    success = 0
    failed = 0
    
    for user_id in all_users:
        try:

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            btn1 = types.KeyboardButton("🏦 Выбрать банк")
            btn2 = types.KeyboardButton("🔍 Найти партнёра")
            btn3 = types.KeyboardButton("📊 Построить график")
            markup.add(btn1, btn2, btn3)
            

            bot.send_message(
                user_id, 
                "🎉 Бот обновлен! Доступны новые функции.", 
                reply_markup=markup
            )
            success += 1
            

            time.sleep(0.1)
            
        except Exception as e:
            failed += 1
            print(f"Ошибка для пользователя {user_id}: {e}")
    

    report = f"""
    Обновление завершено!

    Успешно: {success}
    Не удалось: {failed}
    Всего: {len(all_users)}
    """
    bot.send_message(message.chat.id, report)
    
from update_bnb import fetch_categories_simple_bank
from belkart import fetch_promotions, save_belkart_items

BANKS = [
    {"id": 1, "name": "BNB", "func": fetch_categories_simple_bank},
    {"id": 2, "name": "Belkart", "func": fetch_promotions},
]

@bot.message_handler(commands=['parse_banks'])
def parse_banks_command(message):
    bot.send_message(message.chat.id, "🚀 Запуск парсеров банков...")

    # Последовательный запуск
    for bank in BANKS:
        bot.send_message(message.chat.id, f"🔹 Парсим банк {bank['name']} ({bank['id']})")
        
        if bank['name'] == "BNB":
            bank['func'](bank_id=bank['id'])
        elif bank['name'] == "Belkart":
            items = bank['func'](bank_id=bank['id'])
            save_belkart_items(bank['id'], items)

    bot.send_message(message.chat.id, "✅ Парсинг завершён!")



@bot.message_handler(commands=['start', 'menu'])
def handle_start(message):
    send_main_menu(bot, message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('bank_'))
def callback_bank(call):
    bank_id = int(call.data[5:])

    banks = get_banks()
    selected = next((b for b in banks if b[0] == bank_id), None)
    if selected:
        name, loyalty_url = selected[1], selected[2]
        if loyalty_url:
            bot.send_message(call.message.chat.id, f"Ссылка на программу лояльности: {loyalty_url}")

    if bank_id == 2:
        partners = get_partners_latest_by_bank_category(bank_id, 0)  

        if not partners:
            bot.send_message(call.message.chat.id, "У Белкарт нет партнёров.")
            return

        cfg = fetch_partners_scrape_config(bank_id)
        bonus_unit = cfg.get("bonus_unit", "") or ""


        lines = ["🏦 *Белкарт — партнёры:*"]
        for name, bonus, link in partners:
            shown_link = link or "#"
            bonus_display = f" — {bonus} {bonus_unit}".strip() if bonus else ""
            lines.append(f"• [{name}]({shown_link}){bonus_display}")

        reply = "\n".join(lines)
        
        bot.send_message(
            call.message.chat.id,
            reply,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        return

    # Для остальных банков — показываем категории
    categories = get_latest_categories_by_bank(bank_id)
    if not categories:
        bot.send_message(call.message.chat.id, "Нет категорий для этого банка.")
        return

    markup = types.InlineKeyboardMarkup(row_width=1)
    for cat_id, cat_name, cat_url in categories:
        markup.add(types.InlineKeyboardButton(cat_name, callback_data=f"cat_{bank_id}_{cat_id}"))

    bot.send_message(call.message.chat.id, "Выберите категорию:", reply_markup=markup)



# Исправление в main.py - функция callback_category

@bot.callback_query_handler(func=lambda call: call.data.startswith('cat_'))
def callback_category(call):
    parts = call.data.split('_', 2)
    if len(parts) != 3:
        bot.send_message(call.message.chat.id, "❌ Ошибка обработки категории")
        return
    
    _, bank_id_str, cat_id_str = parts
    try:
        bank_id = int(bank_id_str)
        cat_id = int(cat_id_str)
    except ValueError:
        bot.send_message(call.message.chat.id, "❌ Неверный формат данных")
        return

    # Получаем партнёров
    partners = get_partners_latest_by_bank_category(bank_id, cat_id)
    
    if not partners:
        bot.send_message(call.message.chat.id, "⚠️ Нет партнёров для этой категории")
        return

    # Получаем конфиг бонусов и информацию о категории
    try:
        cfg = fetch_partners_scrape_config(bank_id)
        bonus_unit = cfg.get("bonus_unit", "") or ""
    except Exception:
        bonus_unit = ""

    try:
        cat_name, cat_link = get_categories(cat_id)
    except Exception:
        cat_name = "Категория"
        cat_link = "#"

    try:
        bank_name = get_banks_name(bank_id)
    except Exception:
        bank_name = f"Банк {bank_id}"

    # Формируем сообщение
    reply = f'Партнёры категории [{cat_name}]({cat_link}), {bank_name}\n\n'
    
    for name, bonus, link in partners:
        shown_link = link or "#"
        
        # Бонус отображаем только если есть
        if bonus and bonus.strip():
            bonus_display = f" – {bonus} {bonus_unit}".strip()
        else:
            bonus_display = ""

        reply += f"- [{name}]({shown_link}){bonus_display}\n"

    # Отправляем сообщение
    try:
        bot.send_message(
            call.message.chat.id,
            reply,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    except Exception as e:
        print(f"⚠️ Ошибка отправки сообщения: {e}")
        # Отправляем упрощённую версию без markdown
        simple_reply = f"Партнёры {cat_name} ({bank_name}):\n\n"
        for name, bonus, link in partners:
            bonus_display = f" – {bonus}" if bonus else ""
            simple_reply += f"• {name}{bonus_display}\n"
        bot.send_message(call.message.chat.id, simple_reply)


async def update_all_banks_with_status(progress_callback=None):
    """
    Обёртка над существующим update_all_banks_categories с системой статусов
    """
    try:
        ensure_status_columns()
        
        # ШАГ 1: Подготовка статусов перед обновлением
        prepared = prepare_statuses_for_update()
        print(f"✓ Подготовлено статусов: {prepared}")
        
        if progress_callback:
            progress_callback(0, 100, "Подготовка статусов...")

        
        original_save_partners = db_sql.save_partners
        

        db_sql.save_partners = db_sql.save_partners_with_status_logic
        
        try:
            print("Запускаем существующий update_all_banks_categories...")
            await update_all_banks_categories(progress_callback)
            

            finalized = finalize_statuses_after_update()
            print(f"✓ Финализировано статусов: {finalized}")
            
            
            report = get_status_report()
            
            print(f"\n✅ Обновление с системой статусов завершено!")
            print(f"Статистика: {report['stats']}")
            
            return report
            
        finally:
            db_sql.save_partners = original_save_partners
            
    except Exception as e:
        print(f"❌ Ошибка в обновлении со статусами: {e}")
        raise

def run_update_with_status_wrapper(progress_callback=None):
    return asyncio.run(update_all_banks_with_status(progress_callback))


@bot.message_handler(commands=['digest_with_status'])
def digest_with_status_command(message):
    """
    Дайджест с текущими статусами партнёров из БД
    """
    try:
        from db_sql import get_today_changes_with_status
        changes = get_today_changes_with_status()
        
        if not changes:
            bot.send_message(message.chat.id, "ℹ️ Сегодняшних изменений нет")
            return
        
        # Проверяем, есть ли статусы в данных
        has_status = any('status' in change for change in changes)
        
        if not has_status:
            bot.send_message(
                message.chat.id,
                "⚠️ Колонка 'status' не найдена в базе данных.\n"
                "Для работы системы статусов выполните:\n"
                "`/init_status qwerty11`\n\n"
                "Показываю обычный дайджест без статусов..."
            )
        
        # Формируем дайджест
        text = format_changes_message(changes)
        
        # Показываем
        header = "📋 ДАЙДЖЕСТ СО СТАТУСАМИ (сегодня):\n" if has_status else "📋 ОБЫЧНЫЙ ДАЙДЖЕСТ (сегодня):\n"
        header += f"• Партнёров: {len(changes)}\n"
        
        if not has_status:
            header += "• ⚠️ Статусы недоступны (требуется инициализация)\n"
        
        bot.send_message(message.chat.id, header)
        
        send_markdown_long(message.chat.id, text)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(commands=['check_db'])
def check_db_command(message):
    """Проверяет структуру базы данных"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Проверяем таблицу partners
        cur.execute("PRAGMA table_info(partners);")
        partners_cols = cur.fetchall()
        
        # Проверяем таблицу status_log
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='status_log';")
        has_status_log = cur.fetchone() is not None
        
        response = "🔍 ПРОВЕРКА СТРУКТУРЫ БАЗЫ:\n\n"
        response += "📋 Таблица partners:\n"
        for col in partners_cols:
            col_id, name, type_, notnull, default, pk = col
            response += f"• {name} ({type_})"
            if default:
                response += f" DEFAULT={default}"
            response += "\n"
        
        response += f"\n📋 Таблица status_log: {'✅ есть' if has_status_log else '❌ отсутствует'}\n"
        
        # Проверяем, есть ли данные со статусами
        if 'status' in [col[1] for col in partners_cols]:
            cur.execute("SELECT COUNT(*) FROM partners WHERE status IS NOT NULL AND status != ''")
            count_with_status = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM partners")
            total = cur.fetchone()[0]
            response += f"\n📊 Данные со статусами: {count_with_status}/{total} записей\n"
        
        conn.close()
        
        bot.send_message(message.chat.id, response)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(commands=['init_status'])
def init_status_command(message):
    """Инициализирует систему статусов"""
    try:
        parts = message.text.strip().split()
        if len(parts) < 2 or parts[1] != 'qwerty11':
            bot.send_message(message.chat.id, "⛔️ Неверный секрет")
            return
        
        bot.send_message(message.chat.id, "🔧 Инициализация системы статусов...")
        
        # Проверяем и создаем колонку
        from db_sql import ensure_status_columns
        ensure_status_columns()
        
        # Проверяем структуру
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(partners);")
        columns = [col[1] for col in cur.fetchall()]
        conn.close()
        
        response = "✅ Система статусов инициализирована\n\n"
        response += "Структура таблицы partners:\n"
        for col in columns:
            response += f"• {col}\n"
        
        bot.send_message(message.chat.id, response)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")


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


@bot.message_handler(func=lambda message: message.text == "📊 Построить график")
def graph_start(message):

    remember_user(message.chat.id) # запоминаем
    banks = get_banks()
    if not banks:
        bot.send_message(message.chat.id, "Банки не найдены.")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        if bank_id != 13 and bank_id != 6:
            name += " - С"         
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


@bot.message_handler(func=lambda message: message.text == "🔍 Найти партнёра")
def search_command(message):
    remember_user(message.chat.id) # запоминаем
    msg = bot.send_message(message.chat.id, "Введите имя партнёра для поиска:")
    bot.register_next_step_handler(msg, perform_search)



def perform_search(message):
    query = message.text.strip()
    
    print(f"DEBUG: perform_search query = '{query}'")  # Логируем входной запрос
    
    if not query:
        bot.send_message(message.chat.id, "Пустой запрос. Введите имя снова командой /search.")
        return

    results = search_partners(query)
    
    print(f"DEBUG: results = {results}")  # Логируем результаты
    
    if not results:
        bot.send_message(message.chat.id, f"❌ Ничего не найдено по запросу «{query}».")
        return

    from collections import defaultdict
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

    bot.send_message(
        message.chat.id, 
        "\n".join(lines), 
        parse_mode="Markdown", 
        disable_web_page_preview=True
    )
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
            _send_db_backup(1784338004)
            run_update_with_status_wrapper()
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
        run_update_with_status_wrapper(progress_callback=tg_progress)

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
    total_deleted = 0

    for ch in changes:
        bank = ch["bank_name"]
        cat = ch["category_name"]
        grouped[bank][cat].append(ch)

        status = ch.get("status", "")
        if status == "new":
            total_new += 1
            ch["change_type"] = "new"
        elif status == "new_delete":
            total_deleted += 1
            ch["change_type"] = "deleted"
        else:
            total_updated += 1
            ch["change_type"] = "updated"

    total = total_new + total_updated + total_deleted

    lines: list[str] = []
    # шапка
    lines.append(
        f"🔔 Обновления программы лояльности за сегодня:\n"
        f"• всего: *{total}* партнёров "
        f"(_{total_new} новых_, _{total_updated} обновлено_, _{total_deleted} удалено_)\n"
    )

    # # как в /search: банк → категория → партнёры
    # for bank, cats in grouped.items():
    #     lines.append(f"\n🏦 *{bank}*")
    #     for category, partners in cats.items():
    #         lines.append(f"  → _{category}_")
    #         for p in partners:
    #             bonus_disp = f" — {p['partner_bonus']}%" if p["partner_bonus"] else ""
    #             emoji = "🆕" if p["change_type"] == "new" else "🔁"
    #             link = p.get("partner_link") or "#"   # 👈 на всякий случай заглушка
    #             lines.append(
    #                 f"    {emoji} [{p['partner_name']}]({link}){bonus_disp}"
    #             )  # 👈 имя как Markdown-ссылка
    #             # здесь ссылок нет, поэтому без [name](link)
    #             #lines.append(f"    {emoji} {p['partner_name']}{bonus_disp}")
     # дальше — как в /search
    for bank, cats in grouped.items():
        lines.append(f"\n🏦 *{bank}*")

        for category, partners in cats.items():
            lines.append(f"  → _{category}_")

            for p in partners:
                # Определяем эмодзи по типу изменения
                change_type = p.get("change_type", "updated")
                if change_type == "new":
                    emoji = "✅"
                elif change_type == "deleted":
                    emoji = "❌"
                else:
                    emoji = "🔁"
               
                bonus = p.get("partner_bonus", "")
                bonus_unit = p.get("bonus_unit", "")
                
                if bonus and bonus.strip():
                    if bank == "Паритетбанк":
                        bonus_disp = ""
                    else:
                        bonus_disp = f" — {bonus}{bonus_unit}".strip()
                else:
                    bonus_disp = ""
                
                link = p.get("partner_link", "#")
                if change_type == "deleted":
                    link = "#"
                    bonus_disp = ""


                lines.append(f"-   {emoji} [{p['partner_name']}]({link}) {bonus_disp}")
            #bot.send_message("1784338004", "\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)
        


    return "\n".join(lines).strip()



# Команда для бота
@bot.message_handler(commands=['fix_status'])
def fix_status_command(message):
    """Исправляет проблемы со статусами в БД"""
    parts = message.text.strip().split()
    if len(parts) < 2 or parts[1] != 'qwerty11':
        bot.send_message(message.chat.id, "⛔️ Неверный секрет")
        return
    
    try:
        bot.send_message(message.chat.id, "🔧 Исправляю проблемы со статусами...")
        result = fix_status_problems()
        
        report = f"""
        ✅ Исправление завершено:
        
        • Исправлено партнеров: {result['fixed_partners']}
        • Удалено дубликатов: {result['deleted_duplicates']}
        
        Теперь партнеры со статусом 'delete' не будут создавать новых записей при повторном появлении.
        """
        
        bot.send_message(message.chat.id, report)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(commands=['db_digest'])
def db_digest_command(message):
    """
    Статичный дайджест из реальных данных БД
    """
    try:
        from db_sql import get_test_digest_data
        changes = get_test_digest_data()
        #changes = get_today_partner_changes()
        
        if not changes:
            bot.send_message(message.chat.id, "ℹ️ В базе нет данных для дайджеста.")
            return
        
        text = format_changes_message(changes)
        
        bot.send_message(
            message.chat.id,
            "🗄️ СТАТИЧНЫЙ ДАЙДЖЕСТ ИЗ БД:\n"
            f"• Записей: {len(changes)}\n"
            f"• Данные взяты из базы\n"
            f"• Будет показывать одни и те же данные\n"
        )
        
        # Сохраняем текст при первом вызове
        if not hasattr(db_digest_command, 'cached_text'):
            db_digest_command.cached_text = text
        
        send_markdown_long(message.chat.id, db_digest_command.cached_text)
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

def _seconds_until_next_7am(now: dt.datetime | None = None) -> int:
    now = now or dt.datetime.now()
    target_date = now.date()
    if now.hour >= 7:
        target_date = target_date + dt.timedelta(days=1)
    target_dt = dt.datetime.combine(target_date, dt.time(7, 0, 0))
    return max(1, int((target_dt - now).total_seconds()))

def send_markdown_long(chat_id: int, text: str, chunk_size: int = 3500):
    """
    Безопасное разбиение — никогда не ломает Markdown теги.
    Режет только по логическим блокам:
    блок начинается с строки '🏦 *Банк*'
    """
    lines = text.split("\n")
    
    blocks = []
    current_block = []

    # 1) Разбираем на блоки вида:
    #   🏦 *Банк*
    #     → Категория
    #       - партнёр...
    for line in lines:
        if line.startswith("🏦 "):  # начало нового банка
            if current_block:
                blocks.append("\n".join(current_block))
            current_block = [line]
        else:
            current_block.append(line)

    if current_block:
        blocks.append("\n".join(current_block))

    # 2) Склеиваем блоки в чанки не превышающие chunk_size
    buf = ""
    for block in blocks:
        # +1 за перевод строки между блоками
        add_len = len(block) + (1 if buf else 0)

        if len(buf) + add_len > chunk_size:
            # отправляем текущий буфер
            bot.send_message(
                chat_id,
                buf,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            buf = block
        else:
            buf = block if not buf else f"{buf}\n{block}"

    # 3) Отправляем гипотетично последний
    if buf:
        bot.send_message(
            chat_id,
            buf,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )



def morning_digest_loop():
    # from db_sql import get_today_partner_changes  # если в отдельном модуле
    from db_sql import get_today_changes_with_status
    ensure_tg_users_table()  # на всякий случай

    while True:
        wait_s = _seconds_until_next_7am()
        time.sleep(wait_s)

        try:
            now = dt.datetime.now()
            print(f"[{now:%Y-%m-%d %H:%M:%S}] ▶️ Morning digest start")

            # changes = get_today_partner_changes()
            changes = get_today_changes_with_status()
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
                    send_markdown_long(chat_id, text)
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
    Одноразовая отправка утреннего дайджеста пользователю,
    который вызвал команду /morning <secret>.
    """
    global _morning_running
    try:
        msg = bot.send_message(chat_id, "📨 Формирую утренний дайджест…")

        # 1. Берём изменения за сегодня
        changes = get_today_changes_with_status()
        
        if not changes:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ За сегодня нет новых или изменённых партнёров. Дайджест не требуется."
            )
            return

        # 2. Форматируем текст
        text = format_changes_message(changes)
        
        # ✅ Проверяем, не пуста ли строка
        if not text or not text.strip():
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="⚠️ Не удалось сформировать дайджест (данные пусты). Попробуйте позже."
            )
            print(f"⚠️ Дайджест пуст после форматирования")
            return

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg.message_id,
            text="📨 Отправляю дайджест…"
        )

        # 3. Отправляем длинный текст (может быть разбит на несколько сообщений)
        send_markdown_long(chat_id, text)

        bot.send_message(chat_id, "✅ Утренний дайджест отправлен.")
        
    except Exception as e:
        print(f"❌ Ошибка при ручном запуске дайджеста: {e}")
        import traceback
        traceback.print_exc()
        try:
            bot.send_message(chat_id, f"❌ Ошибка при отправке дайджеста: {str(e)[:100]}")
        except:
            pass
    finally:
        _morning_running = False
        try:
            _morning_lock.release()
        except RuntimeError:
            pass


def _run_manual_morning_digest_all(chat_id: int):
    """
    Массовая отправка утреннего дайджеста всем пользователям.
    """
    global _morning_running
    try:
        msg = bot.send_message(chat_id, "📨 Формирую утренний дайджест для всех…")

        # 1. Берём изменения за сегодня
        changes = get_today_changes_with_status()

        if not changes:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ За сегодня нет новых или изменённых партнёров. Отправка отменена."
            )
            return

        # 2. Форматируем текст
        text = format_changes_message(changes)
        
        # ✅ Проверяем, не пуста ли строка
        if not text or not text.strip():
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="⚠️ Не удалось сформировать дайджест (данные пусты)."
            )
            print(f"⚠️ Дайджест пуст после форматирования")
            return

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg.message_id,
            text="📨 Отправляю дайджест всем пользователям…"
        )

        # 3. Получаем всех пользователей
        all_chat_ids = get_all_chat_ids()

        if not all_chat_ids:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ Нет пользователей в базе для отправки."
            )
            return

        sent_count = 0
        failed_count = 0

        # 4. Отправляем дайджест каждому пользователю
        for user_chat_id in all_chat_ids:
            try:
                if user_chat_id == chat_id:
                    continue
                    
                send_markdown_long(user_chat_id, text)
                sent_count += 1
                print(f"✅ Отправлена дайджест пользователю {user_chat_id}")
                
                time.sleep(0.1)
                
            except Exception as user_e:
                failed_count += 1
                print(f"⚠️ Ошибка отправки дайджеста пользователю {user_chat_id}: {user_e}")

        # 5. Отправляем себе
        try:
            send_markdown_long(chat_id, text)
            sent_count += 1
        except Exception as e:
            failed_count += 1
            print(f"⚠️ Ошибка отправки дайджеста себе: {e}")
        
        # 6. Отправляем отчёт
        report = f"✅ Дайджест отправлен:\n• Успешно: {sent_count}\n• Ошибок: {failed_count}"
        bot.send_message(chat_id, report)
        print(report)
        
    except Exception as e:
        print(f"❌ Ошибка при массовой отправке дайджеста: {e}")
        import traceback
        traceback.print_exc()
        try:
            bot.send_message(chat_id, f"❌ Ошибка при отправке: {str(e)[:100]}")
        except:
            pass
    finally:
        _morning_running = False
        try:
            _morning_lock.release()
        except RuntimeError:
            pass


def _run_manual_morning_digest_all(chat_id: int):
    """
    Однократная отправка утреннего дайджеста всем пользователям,
    """
    global _morning_running
    try:
        msg = bot.send_message(chat_id, "📨 Формирую утренний дайджест…")

        # 1. Берём изменения за сегодня
        changes = get_today_changes_with_status()

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
        all_chat_ids = get_all_chat_ids()

        if not all_chat_ids:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg.message_id,
                text="ℹ️ Нет пользователей в базе для отправки."
            )
            return

        sent_count = 0
        failed_count = 0

        for user_chat_id in all_chat_ids:
            try:
                if user_chat_id == chat_id:
                    continue
                    
                send_markdown_long(user_chat_id, text)
                sent_count += 1
                print(f"Удачная отправка дайджеста пользователю {user_chat_id}")
                
                time.sleep(0.1)
                
            except Exception as user_e:
                failed_count += 1
                print(f"⚠️ Ошибка отправки дайджеста пользователю {user_chat_id}: {user_e}")

        try:
            send_markdown_long(chat_id, text)
            sent_count += 1
        except Exception as e:
            failed_count += 1
            print(f"⚠️ Ошибка отправки дайджеста себе: {e}")
        
        
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при массовой рассылке дайджеста: {e}")
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


@bot.message_handler(commands=['morning_all'])
def morning_command_all(message):
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
        target=_run_manual_morning_digest_all,
        args=(message.chat.id,),
        daemon=True
    ).start()

# ---------- KeepAlive + Flask ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"


async def keep_alive():
    url = os.getenv("KEEPALIVE_URL", "https://partners-bot.onrender.com/")
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
    #bot.polling(none_stop=True)
    while True:
        try:
            bot.polling(none_stop=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            print("Ошибка polling:", e)
            time.sleep(3)


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
