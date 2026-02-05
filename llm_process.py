import asyncio
from env import MISTRAL_API_KEY
from mistralai import Mistral
import asyncpg
import concurrent.futures
from env import DB_CONFIG
import logging

api_key = MISTRAL_API_KEY
client = Mistral(api_key=api_key)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


question = "Сколько всего видео есть в системе?"
prompt_ = f"""
    Ты эксперт по базе данных.
    Есть две таблицы: videos и video_snaphots
    
    Стуктура video:
    id: str | video_created_at: str | views_count: int | likes_count: int | reports_count: int | comments_count: int | creator_id: str | created_at: str | updated_at: str
    
    Структура video_snapshots:
     id: str | video_id: str | views_count: int | likes_count: int | reports_count: int | comments_count: int | delta_views_count: int | delta_likes_count: int | delta_reports_count: int | delta_comments_count: int | created_at: str | updated_at: str
     
    Вопрос пользователя: {question}
    
    Сгенерируй SQL запрос для ответа на вопрос.
    Верни ТОЛЬКО SQL без пояснений.
    БЕЗ '''sql''', ТОЛЬКО ЗАПРОС!
"""
async def ask_mistral(prompt: str):
    loop = asyncio.get_event_loop()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        chat_response = await loop.run_in_executor(
            pool,
            lambda: client.chat.complete(
                model="mistral-large-latest",
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
        )

    sql_query = chat_response.choices[0].message.content
    return sql_query

async def ask_db(sql: str):
    try:
        conn = await asyncpg.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"]
        )

        async with conn.transaction():
            res = await conn.fetchval(sql)
            # logger.info("Запрос успешно отправлен")

    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
        raise

    return res


async def main():
    sql = await ask_mistral(prompt_)
    print("Запрос: \n" + sql + "\n")

    answer = await ask_db(sql)
    print(answer)


if __name__ == "__main__":
    asyncio.run(main())