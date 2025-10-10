import telebot
from telebot import types
import sqlite3
import matplotlib.pyplot as plt

from flask import Flask
import os
import threading

TOKEN = "8176791165:AAFeivYr8ipnSI0m0yZ8IlLrkCuYHPMbZ0k"
bot = telebot.TeleBot(TOKEN)

def get_banks_from_db():
    conn = sqlite3.connect("banks.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, loyalty_url FROM banks ORDER BY name;")
    banks = cursor.fetchall()
    conn.close()
    return banks  # [(id, name, loyalty_url), ...]

def get_categories_by_bank(bank_id):
    conn = sqlite3.connect("banks.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.id, c.name, c.url
        FROM categories c
        INNER JOIN (
            SELECT name, MAX(checked_at) as max_checked
            FROM categories
            WHERE bank_id = ?
            GROUP BY name
        ) sub ON c.name = sub.name AND c.checked_at = sub.max_checked
        WHERE c.bank_id = ?
        ORDER BY c.name;
    """, (bank_id, bank_id))
    categories = cursor.fetchall()
    conn.close()
    return categories  # [(id, name, url), ...]

def get_partners_by_bank_category(bank_id, category_id):
    conn = sqlite3.connect("banks.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT partner_name, partner_bonus, partner_link
        FROM partners
        WHERE bank_id = ? AND category_id = ?
        AND checked_at = (SELECT MAX(checked_at) FROM partners p2 WHERE p2.bank_id=? AND p2.category_id=?)
        ORDER BY partner_name;
    """, (bank_id, category_id, bank_id, category_id))
    partners = cursor.fetchall()
    conn.close()
    return partners

def plot_partners_by_bank(bank_id):
    conn = sqlite3.connect("banks.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.name, COUNT(p.partner_name)
        FROM categories c
        LEFT JOIN partners p ON c.id = p.category_id AND p.bank_id = ?
        WHERE c.bank_id = ?
        GROUP BY c.name
        ORDER BY c.name;
    """, (bank_id, bank_id))
    data = cursor.fetchall()
    conn.close()
    categories = [row[0] for row in data]
    counts = [row[1] for row in data]
    
    plt.figure(figsize=(12, 6))  # увеличьте ширину графика
    plt.bar(categories, counts, color="skyblue")
    plt.xlabel("Категории")
    plt.ylabel("Количество партнеров")
    plt.title("Партнеры по категориям")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.xticks(rotation=45, ha='right', fontsize=10)  # Повернуть и уменьшить подписи!
    plt.tight_layout()  # чтобы подписи не выходили за пределы холста
    plt.savefig("partners_chart.png")
    plt.close()

    return "partners_chart.png"

@bot.message_handler(commands=['start'])
def start_message(message):
    banks = get_banks_from_db()
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"bank_{bank_id}"))
    bot.send_message(message.chat.id, "Выберите банк:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('bank_'))
def callback_bank(call):
    bank_id = int(call.data[5:])
    banks = get_banks_from_db()
    selected_bank = next((b for b in banks if b[0] == bank_id), None)
    if selected_bank:
        name, loyalty_url = selected_bank[1], selected_bank[2]
        bot.send_message(call.message.chat.id, f"Ссылка на программу лояльности: {loyalty_url}")

    categories = get_categories_by_bank(bank_id)
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
    partners = get_partners_by_bank_category(int(bank_id), int(cat_id))
    if not partners:
        bot.send_message(call.message.chat.id, "Нет партнёров для этой категории.")
        return
    #reply = "Партнёры данной категории:\n"
    #for name, bonus, link in partners:
    #    reply += f"- {name} | Бонус: {bonus if bonus else '—'} | Ссылка: {link}\n"
    #bot.send_message(call.message.chat.id, reply)
    
    reply = "Партнёры данной категории:\n\n"
    for name, bonus, link in partners:
        # Если ссылка не полная, добавляем базовую часть
        if not link.startswith("http"):
            link = "https://www.alfabank.by" + link
        # Формируем строку с markdown-ссылкой
        reply += f"- [{name}]({link}) — бонус: {bonus if bonus else '—'}\n"
    
    bot.send_message(call.message.chat.id, reply, parse_mode='Markdown')

# === ДОБАВЛЯЕМ КОМАНДУ ДЛЯ ОТКРЫТИЯ MINI APP с графиками ===
@bot.message_handler(commands=['graph'])
def graph_start(message):
    banks = get_banks_from_db()
    markup = types.InlineKeyboardMarkup(row_width=1)
    for bank_id, name, loyalty_url in banks:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"graphbank_{bank_id}"))
    bot.send_message(message.chat.id, "Выберите банк для графика:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('graphbank_'))
def callback_graphbank(call):
    bank_id = int(call.data.split('_')[1])
    file_path = plot_partners_by_bank(bank_id)
    with open(file_path, "rb") as photo:
        bot.send_photo(call.message.chat.id, photo, caption="График партнеров по категориям для выбранного банка")

# Добавьте Flask app для порта
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    app.run(host='0.0.0.0', port=5000)

def run_bot():
    bot.polling(none_stop=True)

if __name__ == '__main__':
    # Запускаем Flask в отдельном потоке
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Запускаем бота
    run_bot()
