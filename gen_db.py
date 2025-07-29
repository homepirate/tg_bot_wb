import asyncio
from config import config
from models import *

async def create_all_models():
    async with config.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Все таблицы успешно созданы.")

if __name__ == "__main__":
    asyncio.run(create_all_models())
