from .base import DatabaseDownloader
import asyncpg
from fastapi import HTTPException, status


class PostgresDownloader(DatabaseDownloader):

    def __init__(self, connection_data:str, query:str, *args, **kwargs) -> None:
        super().__init__(connection_data, query, *args, **kwargs)

    async def get_data(self):
        """
        Получение данных из Postgresql
        """
        try:
            conn = await asyncpg.connect(self._connection_str)
            result = await conn.fetch(self.query)

            return [dict(row) for row in result]
        except Exception:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f'Не удалось получить данные')