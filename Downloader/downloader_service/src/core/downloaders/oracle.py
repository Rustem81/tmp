import asyncio
import datetime
import cx_Oracle
from .base import DatabaseDownloader
from config import settings
from fastapi import HTTPException, status


class OracleDownloader(DatabaseDownloader):
    
    def __init__(self, connection_data: dict, query: str, *args, **kwargs) -> None:
        self.__user = connection_data.username
        self.__password = connection_data.password
        self.__connection_str = connection_data.conn_string
        super().__init__(connection_data, query, *args, **kwargs)

    @staticmethod
    def type_handler(cursor, name, default_type, size, precision, scale):
        if default_type == cx_Oracle.DATETIME:
            return cursor.var(datetime.datetime, arraysize=cursor.arraysize)
        if default_type == cx_Oracle.DB_TYPE_CLOB:
            return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
        if default_type == cx_Oracle.DB_TYPE_BLOB:
            return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)

    async def get_data(self):
        """
        Получение данных из Oracle
        """
        try:
            loop = asyncio.get_running_loop()
            with cx_Oracle.connect(user=self.__user, password=self.__password, dsn=self.__connection_str, threaded=True, encoding="UTF-8") as conn:
                with conn.cursor() as cursor:
                    conn.outputtypehandler = self.type_handler
                    cursor.prefetchrows = settings.prefetch_rows

                    await loop.run_in_executor(self._pool, cursor.execute, self.query)
                    columns = [column[0] for column in cursor.description]
                    cursor.rowfactory = lambda *args: dict(zip(columns, args))
                    return await loop.run_in_executor(self._pool, cursor.fetchall)
        except:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail='Не удалось получтиь данные')