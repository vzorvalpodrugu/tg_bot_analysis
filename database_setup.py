import asyncio
import asyncpg
from typing import Optional
import logging
from db_config import DB_CONFIG, DB_CONFIG_video_snapshots

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncDataBaseManager:
    def __init__(self,
                 user: str = DB_CONFIG["user"],
                 password: str = DB_CONFIG["password"],
                 host: str = DB_CONFIG["host"],
                 port: str = DB_CONFIG["port"]
                 ):
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
                "SELECT 1 FROM pg_database WHERE datname = $1",
                db_name
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
            id VARCHAR(100) UNIQUE NOT NULL,
            video_created_at TIMESTAMP NOT NULL,
            views_count INTEGER,
            likes_count INTEGER,
            reports_count INTEGER,
            comments_count INTEGER,
            creator_id VARCHAR(100) NOT NULL,
            created_at VARCHAR(100) NOT NULL,
            updated_at VARCHAR(100) NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS video_snapshots(
             id VARCHAR(100) UNIQUE NOT NULL,
             video_id VARCHAR(100) NOT NULL,
             views_count INTEGER,
             likes_count INTEGER,
             reports_count INTEGER,
             comments_count INTEGER,
             delta_views_count VARCHAR(100),
             delta_likes_count VARCHAR(100),
             delta_reports_count VARCHAR(100),
             delta_comments_count VARCHAR(100),
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
                logger.info("Таблица либо уже существовала, либо сейчас создана.")
        except Exception as e:
            logger.error("Ошибка при создании таблицы")
            raise

async def setup_database():

    db_manager = AsyncDataBaseManager(
        user = DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
    )

    try:
        await db_manager.create_database("tg_analysis")

        await db_manager.create_tables("tg_analysis")

        logger.info("БД успешно настроена")

        return db_manager
    except Exception as e:
        logger.error(f'Ошибка настройки БД: {e}')
        raise

if __name__ == '__main__':
    asyncio.run(setup_database())