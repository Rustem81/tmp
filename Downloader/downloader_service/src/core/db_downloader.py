import uuid
from enum import Enum
from typing import  List
from .s3.minio_uploader import upload_data
from .downloaders.base import DatabaseDownloader 
from .downloaders.oracle import OracleDownloader
from .downloaders.postgres import PostgresDownloader
from .downloaders.mssql import MSSqlDownloader
from .models import ConnectionModel, SQLQueryModel


class DBMSType(int, Enum):
    oracle = 1
    postgresql = 2
    mssql = 3


async def downloader_builder(database_type: int, connection_data: ConnectionModel, query: str):
    """
    Фабрика загрузчиков

    args:
        database_type - тип РСУБД
        connection_data - данные подключения
        query - запрос к БД

    return:
        Downloader объект
    """
    match database_type:
        case DBMSType.oracle:
            return OracleDownloader(connection_data=connection_data, query=query)
        case DBMSType.postgresql:
            return PostgresDownloader(connection_data=connection_data.conn_string, query=query)
        case DBMSType.mssql:
            return MSSqlDownloader(connection_data=connection_data.conn_string, query=query)
        case _:
            raise ValueError("Unsupported database type")


async def archive_data(data: List[dict]):
    data_worker = DatabaseDownloader()
    return await data_worker.save_to_zip(data)


async def execute_query(data: SQLQueryModel):
    """
    Выполнить запрос
    """
    db_handler = await downloader_builder(data.db_type, data.connection, query=data.query)
    return await db_handler.get_data()


async def upload_to_minio(file):
    """
    Положить данные в S3
    """
    contents = await file.read()
    return await upload_data(contents, file.filename)