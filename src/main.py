import os
import json
import openai
import psycopg2
import asyncio
import logging
import sys
import signal
import traceback
from pathlib import Path
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
from datetime import datetime

# Импорт TelegramLogHandler
TELEGRAM_LOG_CHANNEL_ID = os.getenv("TELEGRAM_LOG_CHANNEL_ID")
if TELEGRAM_LOG_CHANNEL_ID:
    from src.telegram_log_handler import TelegramLogHandler

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

# Добавляем Telegram handler если указан ID канала
if TELEGRAM_LOG_CHANNEL_ID:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    telegram_handler = TelegramLogHandler(BOT_TOKEN, TELEGRAM_LOG_CHANNEL_ID)
    telegram_handler.setLevel(logging.INFO)
    telegram_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s\n%(message)s'))
    logger.addHandler(telegram_handler)
    logger.info("✅ Логирование в Telegram канал активировано")

# Загрузка .env
env_path = Path('.env')
if env_path.exists():
    load_dotenv()
    logger.info("✅ Конфигурация загружена")

# Проверка и загрузка переменных окружения
REQUIRED_ENV_VARS = {
    "OPENAI_API_KEY": "OpenAI API ключ",
    "BOT_TOKEN": "Telegram бот токен",
    "DATABASE_URL": "URL базы данных"
}

missing_vars = [name for name in REQUIRED_ENV_VARS.keys() if not os.getenv(name)]
if missing_vars:
    missing_list = "\n".join(f"- {name}: {REQUIRED_ENV_VARS[name]}" for name in missing_vars)
    logger.error(f"❌ Отсутствуют обязательные переменные окружения:\n{missing_list}")
    raise ValueError("Отсутствуют обязательные переменные окружения")

openai.api_key = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Глобальные переменные для БД
conn = None
cur = None

async def setup_database():
    global conn, cur
    try:
        logger.info("🔄 Подключение к базе данных...")
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
            chat_id BIGINT,
            added_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        logger.info("✅ База данных инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        logger.debug(traceback.format_exc())
        raise

async def cleanup_database():
    global conn, cur
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("✅ Соединение с БД закрыто")
    except Exception as e:
        logger.error(f"❌ Ошибка закрытия БД: {e}")
        logger.debug(traceback.format_exc())

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
        logger.info("🤖 Отправка запроса к GPT...")
        response = await openai.ChatCompletion.acreate(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        result = response.choices[0].message.content.strip()
        logger.info("✅ Получен ответ от GPT")
        return json.loads(result)
    except Exception as e:
        logger.error(f"❌ Ошибка GPT: {e}")
        logger.debug(traceback.format_exc())
        return {"error": "gpt_fail"}

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"📥 Сообщение от {user.username}")

    if not any(k in text.lower() for k in ["фильм", "кино", "movie", "film", "предлагаю", "рекомендую"]):
        return

    try:
        status = await update.message.reply_text("🎬 Анализирую фильм...", quote=True)
        
        result = await analyze_film_text(text)
        
        if "error" in result:
            error_msg = "❌ Не удалось распознать фильм в сообщении"
            logger.warning(error_msg)
            await status.edit_text(error_msg)
            return

        logger.info(f"✅ Распознан фильм: {result['title']} ({result['year']})")
        
        try:
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
            logger.info("✅ Информация сохранена в БД")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в БД: {e}")
            logger.debug(traceback.format_exc())
            await status.edit_text("❌ Ошибка сохранения информации")
            return

        caption = f"""🎬 *{result["title"]}* ({result["year"]})

👤 *Актёры:* {", ".join(result["actors"])}
🎭 *Жанры:* {", ".join(result["genres"])}
⭐️ КиноПоиск: {result["kinopoisk_rating"]} — [ссылка]({result["kinopoisk_link"]})
⭐️ IMDb: {result["imdb_rating"]} — [ссылка]({result["imdb_link"]})

📎 Предложил: @{user.username}"""

        try:
            await update.message.reply_photo(
                photo=result["poster_url"],
                caption=caption,
                parse_mode="Markdown"
            )
            await status.delete()
            logger.info("✅ Ответ отправлен")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки ответа: {e}")
            logger.debug(traceback.format_exc())
            await status.edit_text(caption + "\n\n⚠️ Не удалось загрузить постер", parse_mode="Markdown")
            
    except Exception as e:
        error_msg = f"❌ Ошибка обработки сообщения: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        await status.edit_text(error_msg)

async def run_bot():
    try:
        await setup_database()
        
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
        
        # Добавляем обработчик сигналов для graceful shutdown
        def signal_handler(signum, frame):
            logger.info("🛑 Получен сигнал остановки")
            asyncio.create_task(cleanup_database())
            app.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("🚀 Бот запущен")
        await app.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.debug(traceback.format_exc())
    finally:
        await cleanup_database()

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"FATAL: {e}")
        logger.debug(traceback.format_exc()) 