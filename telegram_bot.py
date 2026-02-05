import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from env import TG_API_TOKEN
from llm_process import ask_mistral, ask_db




BOT_TOKEN = TG_API_TOKEN

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message()
async def echo_message(message: types.Message):
    user_text = message.text
    print(user_text)

    prompt = f"""
        Ты эксперт по базе данных.
        Есть две таблицы: videos и video_snaphots

        Стуктура video:
        id: str | video_created_at: str | views_count: int | likes_count: int | reports_count: int | comments_count: int | creator_id: str | created_at: str | updated_at: str

        Структура video_snapshots:
         id: str | video_id: str | views_count: int | likes_count: int | reports_count: int | comments_count: int | delta_views_count: int | delta_likes_count: int | delta_reports_count: int | delta_comments_count: int | created_at: str | updated_at: str

        Вопрос пользователя: {user_text}

        Сгенерируй SQL запрос для ответа на вопрос.
        Верни ТОЛЬКО SQL без пояснений.
        БЕЗ '''sql''', ТОЛЬКО ЗАПРОС!
    """

    sql = await ask_mistral(prompt)
    print(sql)

    answer = await ask_db(sql)
    print(answer)

    await message.answer(str(answer))


async def main():
    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())