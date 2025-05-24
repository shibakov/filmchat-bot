import os
import json
import openai
import psycopg2
import asyncio
import signal
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
from pathlib import Path

# Загружаем .env только если файл существует
env_path = Path('.env')
if env_path.exists():
    load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not all([openai.api_key, BOT_TOKEN, DATABASE_URL]):
    raise ValueError("Необходимые переменные окружения не установлены")

# Подключение к БД
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

# GPT-запрос
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
    except openai.error.OpenAIError as e:
        print(f"OpenAI API error: {e}")
        return {"error": "openai_error"}
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return {"error": "invalid_json"}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"error": "unknown_error"}

# Обработка сообщений
async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = update.message.text
    chat = update.effective_chat
    user = update.effective_user

    # Проверяем, содержит ли сообщение ключевые слова о фильмах
    if not any(k in text.lower() for k in ["фильм", "кино", "посмотреть", "рекомендую", "предлагаю", "советую", "movie", "film"]):
        return

    # Отправляем сообщение о начале обработки
    status_message = await update.message.reply_text(
        "🎬 Анализирую информацию о фильме...",
        quote=True
    )

    try:
        # Получаем информацию о фильме через GPT
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

        # Сохраняем информацию в базу
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

        # Формируем красивое сообщение с информацией о фильме
        caption = f"""🎬 *{result["title"]}* ({result["year"]})

👥 *В ролях:*
{", ".join(result["actors"])}

🎭 *Жанры:*
{", ".join(result["genres"])}

⭐️ *Рейтинги:*
• КиноПоиск: {result["kinopoisk_rating"]} — [открыть]({result["kinopoisk_link"]})
• IMDb: {result["imdb_rating"]} — [открыть]({result["imdb_link"]})

🎯 Предложил: @{user.username}"""

        # Отправляем фото с описанием
        try:
            await update.message.reply_photo(
                photo=result["poster_url"],
                caption=caption,
                parse_mode="Markdown"
            )
            # Удаляем статус-сообщение после успешной отправки
            await status_message.delete()
        except Exception as e:
            print(f"Error sending photo: {e}")
            # Если не удалось отправить фото, отправляем только текст
            await status_message.edit_text(
                f"{caption}\n\n❗️ Не удалось загрузить постер фильма",
                parse_mode="Markdown"
            )

    except Exception as e:
        print(f"Error processing message: {e}")
        await status_message.edit_text("❌ Произошла ошибка при обработке сообщения")

async def shutdown(app):
    """Корректное завершение работы бота"""
    print("Stopping bot...")
    try:
        if hasattr(app, 'running') and app.running:
            await app.stop()
    except Exception as e:
        print(f"Error during shutdown: {e}")
    finally:
        try:
            if 'cur' in globals() and cur:
                cur.close()
            if 'conn' in globals() and conn:
                conn.close()
        except Exception as e:
            print(f"Error closing database connections: {e}")
        print("Bot stopped successfully")

async def main():
    """Основная функция запуска бота"""
    app = None
    try:
        # Создаем приложение
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        
        # Добавляем обработчик сообщений
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
        
        # Настраиваем корректное завершение работы
        loop = asyncio.get_event_loop()
        for signal_type in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signal_type,
                lambda: asyncio.create_task(shutdown(app))
            )
        
        print("Starting bot...")
        await app.initialize()
        await app.start()
        print("Bot is running...")
        await app.run_polling(stop_signals=None)
    except Exception as e:
        print(f"Error starting bot: {e}")
        if app:
            await shutdown(app)
    finally:
        if app:
            await shutdown(app)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}") 