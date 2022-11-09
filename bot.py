import time

from aiogram import Bot, Dispatcher, types, executor
import asyncio
import aioschedule
import logging
import config
from db_manager import Database

bot_token = config.token
channel_id = config.channel_id
bot = Bot(token=bot_token)
logging.basicConfig(level=logging.INFO)
dp = Dispatcher(bot)
db = Database()


def test_job():  # TEST ONLY
    bot.send_message(channel_id, f'test {time.time()}')
    print('i work')


async def daily_job():
    pass


async def scheduler():
    aioschedule.every(5).seconds.do(test_job)  # TEST ONLY
    # aioschedule.every(24).hours.do(daily_job())
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def on_startup(_):
    if config.first_run:
        config.first_run = False
        db.create_wine_table()
    asyncio.create_task(scheduler())


def start_bot():
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply("Hi!")


@dp.message_handler(commands=['ch'])
async def channel_message(message: types.Message):
    await bot.send_message(channel_id, 'test')


@dp.message_handler(commands=['wines'])
async def channel_message(message: types.Message):
    wines = db.get_wines()
    for wine in wines:
        await bot.send_message(channel_id, wine)


@dp.message_handler()
async def echo(message: types.Message):
    await message.answer(message.text)

