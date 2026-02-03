import asyncio
import asyncpg
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncDataBaseManager:
    def __init__(self,
                 user: str = "postgres",
                 password: str = "admin",
                 host: str = "localhost",
                 port: str = "5432"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    async def create_database(self, db_name: str):
        conn = await asyncpg.connect(
            user = self.user,
            password = self.password,
            host = self.host,
            port = self.port,
            database = 'postgres'
        )

        try:
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", db_name
            )

            if not exists:
                await conn.execute(f'CREATE DATABASE "{db_name}"')
                logger.info("База создана")
            else:
                logger.info("База уже существует")

        except Exception as e:
            logger.error(f"Ошибка создания БД: {e}")
            raise
        finally:
            await conn.close()

    async def create_tables(self, db_name):

        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS videos(
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(100) UNIQUE NOT NULL,
            video_created_at TIMESTAMP NOT NULL,
            views_count INTEGER,
            likes_count INTEGER,
            reports_count INTEGER,
            comments_count INTEGER,
            creator_id VARCHAR(100) NOT NULL,
            created_at VARCHAR(100) NOT NULL,
            updated_at VARCHAR(100) NOT NULL
        );
        """

        try:
            conn = await asyncpg.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=db_name
            )

            async with conn.transaction():
                await conn.execute(create_tables_sql)
                logger.info("Таблица создана")
        except Exception as e:
            logger.error("Ошибка при создании таблицы")
            raise

async def setup_database():
    DB_CONFIG = {
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432",
        "database": "videos_db"
    }

    db_manager = AsyncDataBaseManager(
        user = DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
    )

    try:
        await db_manager.create_database(DB_CONFIG["database"])

        await db_manager.create_tables(DB_CONFIG["database"])

        logger.info("БД успешно настроена")

        return db_manager
    except Exception as e:
        logger.error(f'Ошибка настройки БД: {e}')
        raise

if __name__ == '__main__':
    asyncio.run(setup_database())