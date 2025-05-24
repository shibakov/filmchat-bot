# Filmchat Telegram Bot

Telegram бот для обработки сообщений о фильмах. Анализирует сообщения, извлекает информацию о фильмах и сохраняет её в базу данных.

## Функциональность

- Мониторинг сообщений на предмет упоминаний фильмов
- Анализ текста с помощью GPT-4
- Извлечение информации о фильмах (название, год, актеры, жанры)
- Получение рейтингов и ссылок с IMDb и Кинопоиск
- Сохранение информации в PostgreSQL
- Отправка форматированных ответов с постерами фильмов

## Требования

- Python 3.8+
- PostgreSQL
- OpenAI API ключ
- Telegram Bot токен

## Переменные окружения

```env
BOT_TOKEN=your_telegram_bot_token
OPENAI_API_KEY=your_openai_api_key
DATABASE_URL=postgresql://user:password@host:5432/dbname
```

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/shibakov/filmchat-bot.git
cd filmchat-bot
```

2. Создайте виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# или
.\venv\Scripts\activate  # Windows
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Настройте переменные окружения (создайте файл .env или установите в системе)

5. Запустите бота:
```bash
python3 src/main.py
```

## Развертывание

Бот готов к развертыванию на Heroku или другой платформе. Используется Procfile для определения процесса worker.

## Структура проекта

```
filmchat-bot/
├── src/
│   └── main.py          # Основной код бота
├── .env.example         # Пример файла с переменными окружения
├── Procfile            # Конфигурация для Heroku
├── README.md           # Документация
└── requirements.txt    # Зависимости проекта
```
