import logging
import asyncio
import threading
from telegram import Bot
from datetime import datetime

class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token, channel_id):
        super().__init__()
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.queue = asyncio.Queue()
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._worker())

    async def _worker(self):
        bot = Bot(self.bot_token)
        while True:
            msg = await self.queue.get()
            try:
                await bot.send_message(
                    chat_id=self.channel_id,
                    text=f"🤖 Log [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]:\n{msg}"
                )
            except Exception as e:
                print(f"Error sending log to Telegram: {e}")

    def emit(self, record):
        msg = self.format(record)
        try:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)
        except Exception:
            self.handleError(record) 