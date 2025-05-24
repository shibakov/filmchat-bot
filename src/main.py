# Updated: Sat May 24 12:00:48 +04 2025
# This file was updated on Sat May 24 11:55:47 +04 2025
import os
import psycopg2
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

load_dotenv()

# Подключение к БД (однократно при старте)
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

# Создание таблицы, если её нет
cur.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    message_id BIGINT,
    chat_id BIGINT,
    chat_title TEXT,
    user_id BIGINT,
    username TEXT,
    text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    msg = update.message
    user = msg.from_user
    chat = msg.chat

    cur.execute(
        """
        INSERT INTO messages (message_id, chat_id, chat_title, user_id, username, text)
        VALUES (, , , , , )
        """,
        (msg.message_id, chat.id, chat.title, user.id, user.username, msg.text)
    )
    conn.commit()

    print(f"[{chat.title}] @{user.username}: {msg.text}")

app = ApplicationBuilder().token(os.getenv("BOT_TOKEN")).build()
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

app.run_polling()