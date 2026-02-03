import json
import asyncio
import logging
import asyncpg
from db_config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JsonParser:
    def __init__(self, path):
        self.path = path

    async def pars(self):
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                context = json.load(file)
                context = list(context.items())[0][1] #list
        except JsonParser:
            Exception("Json wrong")

        iterator = iter(context)
        while True:
            try:
                el = next(iterator)

                id = el["id"]
                video_created_at = el["video_created_at"]
                views_count = el["views_count"]
                likes_count = el["likes_count"]
                reports_count = el["reports_count"]
                comments_count = el["comments_count"]
                creator_id = el["creator_id"]
                created_at = el["created_at"]
                updated_at = el["updated_at"]


                insert_into_sql = f"""
                INSERT INTO videos (
                    id,
                    video_created_at,
                    views_count,
                    likes_count,
                    reports_count,
                    comments_count,
                    creator_id,
                    created_at,
                    updated_at
                )
                VALUES (
                    '{id}',
                    '{video_created_at}',
                    {views_count},
                    {likes_count},
                    {reports_count},
                    {comments_count},
                    '{creator_id}',
                    '{created_at}',
                    '{updated_at}'
                    );
                """

                try:
                    conn = await asyncpg.connect(
                        user=DB_CONFIG["user"],
                        password=DB_CONFIG["password"],
                        host=DB_CONFIG["host"],
                        port=DB_CONFIG["port"],
                        database=DB_CONFIG["database"]
                    )

                    async with conn.transaction():
                        await conn.execute(insert_into_sql)
                        logger.info("Данные добавились в БД")
                except Exception as e:
                    logger.error(f"Ошибка добавления данных {e}")

            except StopIteration:
                break

        return context


async def main():
    parser = JsonParser('data/videos.json')
    await parser.pars()

if __name__ == "__main__":
    asyncio.run(main())