import os
import json
import openai
import psycopg2
import asyncio
import logging
import sys
import signal
import traceback
import nest_asyncio
from pathlib import Path
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
from logging.handlers import QueueHandler
from queue import Queue
import asyncio
from functools import partial
import threading
from datetime import datetime

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token, channel_id):
        super().__init__()
        self.bot = Bot(bot_token)
        self.channel_id = channel_id
        self.queue = Queue()
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
        
    def _worker(self):
        while self.running:
            try:
                if not self.queue.empty():
                    msg = self.queue.get()
                    # Create new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    # Send message
                    loop.run_until_complete(
                        self.bot.send_message(
                            chat_id=self.channel_id,
                            text=f"🤖 Log [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]:\n{msg}"
                        )
                    )
                    loop.close()
            except Exception as e:
                print(f"Error in log worker: {e}")
            finally:
                # Small sleep to prevent CPU overuse
                threading.Event().wait(0.1)
                
    def emit(self, record):
        try:
            msg = self.format(record)
            self.queue.put(msg)
        except Exception:
            self.handleError(record)
            
    def close(self):
        self.running = False
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=2.0)
        super().close()

# Настройка логирования
TELEGRAM_LOG_CHANNEL_ID = os.getenv("TELEGRAM_LOG_CHANNEL_ID")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log')
    ]
)

logger = logging.getLogger(__name__)

# .env (если есть)
env_path = Path('.env')
if env_path.exists():
    load_dotenv()
    logger.info("✅ Файл .env загружен")
else:
    logger.warning("⚠️ Файл .env не найден")

# Проверка и загрузка переменных окружения
openai.api_key = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Добавляем Telegram handler если указан ID канала
if TELEGRAM_LOG_CHANNEL_ID:
    telegram_handler = TelegramLogHandler(BOT_TOKEN, TELEGRAM_LOG_CHANNEL_ID)
    telegram_handler.setLevel(logging.INFO)
    telegram_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s\n%(message)s'
    ))
    logger.addHandler(telegram_handler)
    logger.info("✅ Логирование в Telegram канал активировано")
else:
    logger.warning("⚠️ TELEGRAM_LOG_CHANNEL_ID не указан, логи не будут отправляться в Telegram")

logger.info("🔑 Проверка переменных окружения:")
logger.info(f"- OPENAI_API_KEY: {'✅ установлен' if openai.api_key else '❌ отсутствует'}")
logger.info(f"- BOT_TOKEN: {'✅ установлен' if BOT_TOKEN else '❌ отсутствует'}")
logger.info(f"- DATABASE_URL: {'✅ установлен' if DATABASE_URL else '❌ отсутствует'}")
logger.info(f"- TELEGRAM_LOG_CHANNEL_ID: {'✅ установлен' if TELEGRAM_LOG_CHANNEL_ID else '❌ отсутствует'}")

if not all([openai.api_key, BOT_TOKEN, DATABASE_URL]):
    logger.error("❌ Не найдены необходимые переменные окружения")
    raise ValueError("❌ Не найдены необходимые переменные окружения")

# Глобальная БД
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
            added_by TEXT,
            chat_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        logger.info("✅ База данных успешно инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации БД: {e}")
        logger.error(traceback.format_exc())
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
        logger.error(f"❌ Ошибка при закрытии соединения с БД: {e}")
        logger.error(traceback.format_exc())

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
        logger.debug(f"Текст запроса: {text}")
        
        response = await openai.ChatCompletion.acreate(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        
        result = response.choices[0].message.content.strip()
        logger.info("✅ Получен ответ от GPT")
        logger.debug(f"Ответ GPT: {result}")
        
        return json.loads(result)
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке GPT запроса: {e}")
        logger.error(traceback.format_exc())
        return {"error": "gpt_fail"}

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text = update.message.text
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"📥 Сообщение от {chat.title or chat.id} / @{user.username}")
    logger.debug(f"Текст сообщения: {text}")

    if not any(k in text.lower() for k in ["фильм", "кино", "movie", "film", "предлагаю", "рекомендую"]):
        return

    try:
        status = await update.message.reply_text("🎬 Анализирую фильм...", quote=True)
        logger.info("🔄 Начало анализа фильма")
        
        result = await analyze_film_text(text)
        
        if "error" in result:
            error_msg = "❌ GPT не смог распознать фильм"
            logger.warning(error_msg)
            await status.edit_text(error_msg)
            return

        logger.info(f"✅ Фильм распознан: {result['title']} ({result['year']})")
        
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
            logger.error(f"❌ Ошибка при сохранении в БД: {e}")
            logger.error(traceback.format_exc())

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
            logger.info("✅ Ответ с постером отправлен")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке постера: {e}")
            logger.error(traceback.format_exc())
            await status.edit_text(caption + "\n\n⚠️ Постер не удалось загрузить", parse_mode="Markdown")
            
    except Exception as e:
        error_msg = f"❌ Произошла ошибка при обработке сообщения: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        await status.edit_text(error_msg)

async def run_bot():
    try:
        await setup_database()
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
        
        logger.info("🚀 Бот запущен")
        await app.run_polling()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.error(traceback.format_exc())
    finally:
        await cleanup_database()

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.error(f"FATAL: {e}")
        logger.error(traceback.format_exc()) 