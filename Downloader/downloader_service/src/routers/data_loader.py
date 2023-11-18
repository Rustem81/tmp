import io
import uuid
from typing import List
from fastapi import APIRouter, Body, UploadFile, File
from fastapi import status
from fastapi.responses import StreamingResponse
from src.core.models import MissingError, SQLQueryModel, MinioUploadResponse
from src.core.db_downloader import execute_query, upload_to_minio, archive_data


examples_request = {
    "Oracle": {
        "summary": "Oracle",
        "value": {
            "db_type": 1,
            "connection": {
                "username": "user",
                "password": "top_secret",
                "conn_string": "host.url.ru/SV"
            },
            "query": "SELECT * FROM ALL_USERS"
        },
    },
    "PostgreSQL": {
        "summary": "PostgreSQL",
        "value": {
            "db_type": 2,
            "connection": {
                "username": "not required",
                "password": "not required",
                "conn_string": "postgresql://user:pwd@localhost/testdb"
            },
            "query": "SELECT * FROM testable"
        },
    },
    "MSSQL": {
        "summary": "MSSQL",
        "value": {
            "db_type": 3,
            "connection": {
                "username": "not required",
                "password": "not required",
                "conn_string": "Server=host.url.ru;Database=testDatabase;Uid=user;Pwd=password;TrustServerCertificate=yes;"
            },
            "query": "SELECT GETDATE();"
        },
    },
}


router = APIRouter(prefix='/data_loader', tags=['Data'])

@router.post(
    "/fetch-data/",
    responses={
        status.HTTP_200_OK: {
            'model': List[dict], 'description': 'List of data'
        },
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            'model': MissingError, 'description': 'Not valid connection string'
        }
    },
)
async def fetch_data(request: SQLQueryModel = Body(...,examples = examples_request)):
    response = await execute_query(request)
    return response


@router.post(
    "/zip_data",
)
async def zip_data(request: List[dict]):
    response = await archive_data(request)
    return StreamingResponse(response, media_type='application/zip', headers={'Content-Disposition': 'attachment; filename="data.zip"'})


@router.post(
    "/get_to_minio/",
    responses={
        status.HTTP_200_OK: {
            'model': MinioUploadResponse, 'description':'Access url'
        },
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            'model': MissingError, 'description': 'Failed to get/upload data'
        }
    },
)
async def get_to_minio(file: UploadFile = File(...)):
    response = await upload_to_minio(file)
    return {'url': response}