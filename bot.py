import json

from aiogram import Bot, Dispatcher, types, executor
from aiogram.types import Message

from src.main import aggregate_data
from src.db import get_collection
from src.conf import BOT_TOKEN


# Create a bot instance
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)


# Handler function to process incoming messages
@dp.message_handler(commands=["start"])
async def process_start_command(message: Message):
    await message.reply("Please send me a JSON input.")


# Handler function to process JSON input
@dp.message_handler(content_types=["text"])
async def process_json_input(message: Message):
    try:
        input_json = json.loads(message.text)
        data_collection = get_collection()

        output_data = aggregate_data(
            input_json["dt_from"],
            input_json["dt_upto"],
            input_json["group_type"],
            data_collection,
        )  # main function

        # Send the output JSON back to the user
        await message.reply(json.dumps(output_data))

    except ValueError:
        await message.reply("Invalid JSON input. Please try again.")


if __name__ == "__main__":
    # Start the bot
    executor.start_polling(dp, skip_updates=True)
