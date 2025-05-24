import os
import json
import openai
import psycopg2
import asyncio
import logging
import sys
import signal
from pathlib import Path
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# .env (если есть)
env_path = Path('.env')
if env_path.exists():
    load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not all([openai.api_key, BOT_TOKEN, DATABASE_URL]):
    raise ValueError("❌ Не найдены переменные окружения")

# Глобальная БД
conn = None
cur = None

async def setup_database():
    global conn, cur
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies_info (
        id SERIAL PRIMARY KEY,
        original_message TEXT,
        title TEXT,
        year INT,
        genres TEXT,
        actors TEXT,
        kinopoisk_rating FLOAT,
        kinopoisk_link TEXT,
        imdb_rating FLOAT,
        imdb_link TEXT,
        poster_url TEXT,
        added_by TEXT,
        chat_id BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    logger.info("✅ Таблица создана")

async def cleanup_database():
    global conn, cur
    if cur: cur.close()
    if conn: conn.close()
    logger.info("✅ Соединение с БД закрыто")

async def analyze_film_text(text):
    prompt = f"""
Проанализируй это сообщение как предложение фильма и выдай результат в JSON. Если в тексте упоминается несколько фильмов, выбери первый:

"{text}"

Формат:
{{
"title": "Название фильма",
"year": 1994,
"genres": ["жанр1", "жанр2"],
"actors": ["Актёр 1", "Актёр 2", "Актёр 3"],
"kinopoisk_rating": 8.7,
"kinopoisk_link": "https://www.kinopoisk.ru/film/12345/",
"imdb_rating": 8.9,
"imdb_link": "https://www.imdb.com/title/tt1234567/",
"poster_url": "https://somecdn.com/poster.jpg"
}}

Если фильм не распознан — ответь {{ "error": "not recognized" }}
Обязательно найди ссылки на IMDb и Кинопоиск.
"""
    try:
        response = await openai.ChatCompletion.acreate(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        return json.loads(response.choices[0].message.content.strip())
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        return {"error": "gpt_fail"}

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text
    chat = update.effective_chat
    user = update.effective_user
    logger.info(f"📥 {chat.title or chat.id} / @{user.username}: {text}")

    if not any(k in text.lower() for k in ["фильм", "кино", "movie", "film", "предлагаю", "рекомендую"]):
        return

    status = await update.message.reply_text("🎬 Анализирую фильм...", quote=True)
    result = await analyze_film_text(text)

    if "error" in result:
        await status.edit_text("❌ GPT не смог распознать фильм")
        return

    cur.execute("""
        INSERT INTO movies_info (
            original_message, title, year, genres, actors,
            kinopoisk_rating, kinopoisk_link,
            imdb_rating, imdb_link, poster_url,
            added_by, chat_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        text,
        result["title"],
        result["year"],
        ", ".join(result["genres"]),
        ", ".join(result["actors"]),
        result["kinopoisk_rating"],
        result["kinopoisk_link"],
        result["imdb_rating"],
        result["imdb_link"],
        result["poster_url"],
        user.username,
        chat.id
    ))
    conn.commit()

    caption = f"""🎬 *{result["title"]}* ({result["year"]})

👤 *Актёры:* {", ".join(result["actors"])}
🎭 *Жанры:* {", ".join(result["genres"])}
⭐️ КиноПоиск: {result["kinopoisk_rating"]} — [ссылка]({result["kinopoisk_link"]})
⭐️ IMDb: {result["imdb_rating"]} — [ссылка]({result["imdb_link"]})

📎 Предложил: @{user.username}
"""
    try:
        await update.message.reply_photo(
            photo=result["poster_url"],
            caption=caption,
            parse_mode="Markdown"
        )
        await status.delete()
    except:
        await status.edit_text(caption + "\n\n⚠️ Постер не удалось загрузить", parse_mode="Markdown")

async def run_bot():
    await setup_database()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    logger.info("🚀 Бот запущен")
    await app.run_polling()
    await cleanup_database()

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.error(f"FATAL: {e}") 