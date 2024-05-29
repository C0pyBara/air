import os
from aiogram import Bot, Dispatcher, types

# Ваш токен, полученный от BotFather
BOT_TOKEN = '7141646480:AAETrVWTjNZxnaRiD10AXUKmU1rV0w6Ergg'
CHAT_ID = '534239907'

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply("Hello! I am your weather report bot. Use /help to see available commands.")

@dp.message_handler(commands=['help'])
async def send_help(message: types.Message):
    await message.reply("Available commands:\n/start - Start the bot\n/help - Show this help message")

async def send_files():
    files = ['/tmp/report.txt', '/tmp/temperature_plot.png', '/tmp/pressure_plot.png']
    for file in files:
        if os.path.exists(file):
            await bot.send_document(CHAT_ID, open(file, 'rb'), caption='Weather Report and Graphs')
        else:
            await bot.send_message(CHAT_ID, f"File {file} does not exist")

if __name__ == '__main__':
 result = dp.start_polling(dp, skip_updates=True) 

