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
    username TEXT,
    user_id BIGINT,
    text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Обработка сообщений
async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text

    # Сохраняем сообщение
    cur.execute(
        "INSERT INTO messages (username, user_id, text) VALUES (%s, %s, %s)",
        (user.username, user.id, text)
    )
    conn.commit()

    await update.message.reply_text(f"Принято: {text}")

app = ApplicationBuilder().token(os.getenv("BOT_TOKEN")).build()
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

app.run_polling()
