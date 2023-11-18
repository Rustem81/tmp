import pyodbc
from .base import DatabaseDownloader
import asyncio
from fastapi import HTTPException, status

class MSSqlDownloader(DatabaseDownloader):

    def __init__(self, connection_data: str, query: str, *args, **kwargs) -> None:
        super().__init__(connection_data, query, *args, **kwargs)

    async def get_data(self):
        """
        Получение данных из MSSQL Server
        """
        try:
            loop = asyncio.get_running_loop()
            with pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'+self._connection_str) as connection:
                with connection.cursor() as cursor:
                    await loop.run_in_executor(self._pool, cursor.execute, self.query)
                    columns = [column[0] for column in cursor.description]
                    result = await loop.run_in_executor(self._pool, cursor.fetchall)
                    return [dict(zip(columns, row)) for row in result]
        except:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='Не удалось получить данные')