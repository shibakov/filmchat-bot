import os
import json
import openai
import psycopg2
import asyncio
import signal
import logging
import sys
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загружаем .env только если файл существует
env_path = Path('.env')
if env_path.exists():
    load_dotenv()

# Проверяем и получаем переменные окружения
openai.api_key = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not all([openai.api_key, BOT_TOKEN, DATABASE_URL]):
    raise ValueError("Необходимые переменные окружения не установлены")

# Глобальные переменные для БД
conn = None
cur = None

async def setup_database():
    """Инициализация подключения к базе данных"""
    global conn, cur
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Таблица для фильмов
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
        logger.info("Database connection established and schema created")
    except Exception as e:
        logger.error(f"Database setup error: {e}")
        raise

async def cleanup_database():
    """Закрытие соединения с базой данных"""
    global conn, cur
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

async def analyze_film_text(text: str) -> dict:
    """Анализ текста сообщения через GPT"""
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
    except openai.error.OpenAIError as e:
        logger.error(f"OpenAI API error: {e}")
        return {"error": "openai_error"}
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
        return {"error": "invalid_json"}
    except Exception as e:
        logger.error(f"Unexpected error in analyze_film_text: {e}")
        return {"error": "unknown_error"}

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик входящих сообщений"""
    if not update.message or not update.message.text:
        return

    text = update.message.text
    chat = update.effective_chat
    user = update.effective_user

    # Проверяем ключевые слова
    if not any(k in text.lower() for k in ["фильм", "кино", "посмотреть", "рекомендую", "предлагаю", "советую", "movie", "film"]):
        return

    # Отправляем статус
    status_message = await update.message.reply_text(
        "🎬 Анализирую информацию о фильме...",
        quote=True
    )

    try:
        # Анализируем текст
        result = await analyze_film_text(text)

        if "error" in result:
            error_messages = {
                "not_recognized": "❌ Не удалось распознать фильм в сообщении",
                "openai_error": "❌ Ошибка при обращении к GPT. Попробуйте позже",
                "invalid_json": "❌ Ошибка обработки данных",
                "unknown_error": "❌ Произошла неизвестная ошибка"
            }
            await status_message.edit_text(error_messages.get(result["error"], "❌ Произошла ошибка"))
            return

        # Сохраняем в БД
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

        # Формируем ответ
        caption = f"""🎬 *{result["title"]}* ({result["year"]})

👥 *В ролях:*
{", ".join(result["actors"])}

🎭 *Жанры:*
{", ".join(result["genres"])}

⭐️ *Рейтинги:*
• КиноПоиск: {result["kinopoisk_rating"]} — [открыть]({result["kinopoisk_link"]})
• IMDb: {result["imdb_rating"]} — [открыть]({result["imdb_link"]})

🎯 Предложил: @{user.username}"""

        try:
            await update.message.reply_photo(
                photo=result["poster_url"],
                caption=caption,
                parse_mode="Markdown"
            )
            await status_message.delete()
        except Exception as e:
            logger.error(f"Error sending photo: {e}")
            await status_message.edit_text(
                f"{caption}\n\n❗️ Не удалось загрузить постер фильма",
                parse_mode="Markdown"
            )

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await status_message.edit_text("❌ Произошла ошибка при обработке сообщения")

async def run_bot():
    """Основная функция запуска бота"""
    application = None
    try:
        # Подключаемся к БД
        await setup_database()
        
        # Создаем приложение
        application = ApplicationBuilder().token(BOT_TOKEN).build()
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
        
        logger.info("Starting bot...")
        await application.initialize()
        await application.start()
        logger.info("Bot is running...")
        
        # Запускаем бота
        await application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"Error in run_bot: {e}")
        raise
    finally:
        try:
            if application and application.running:
                logger.info("Stopping bot...")
                await application.stop()
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")
        finally:
            await cleanup_database()

def main():
    """Точка входа"""
    try:
        # Настраиваем обработку сигналов
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda s, f: sys.exit(0))
            
        # Запускаем бота
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except SystemExit:
        logger.info("Bot stopped by system signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main() 