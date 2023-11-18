import csv
import io
import zipfile

from minio import Minio
from config import settings
from datetime import timedelta

class DataUploader:
    """
    Класс Minio uploader-а
    """
    def __init__(
            self,
            minio_host=settings.minio_host,
            minio_access_key=settings.minio_access_key,
            minio_secret_key=settings.minio_secret_key,
            minio_bucket_name = settings.minio_bucket_name
        ):
        self._minio_client = Minio(minio_host, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
        self._bucket_name = minio_bucket_name

    async def simple_upload(self, data: bytes, object_name: str):
        self._minio_client.put_object(
            self._bucket_name, object_name, io.BytesIO(data), length=len(data),
        )
        return self._minio_client.get_presigned_url("GET", self._bucket_name, object_name, expires = timedelta(days=settings.DATA_ACCESS_EXPIRATION_DAYS))

    async def multipart_upload(self, data: bytes, object_name: str):
        self._minio_client.put_object(
            self._bucket_name, object_name, data, length=-1, part_size=10*1024*1024
        )
        return self._minio_client.get_presigned_url("GET", self._bucket_name, object_name, expires = timedelta(days=settings.DATA_ACCESS_EXPIRATION_DAYS))


MinioClient = DataUploader()

async def upload_data(data: bytes, object_name: str):
    try:
        data_size = len(data)
        if data_size < 5 * 1024 * 1024:
            return await MinioClient.simple_upload(data=data, object_name=object_name)
        else:
            return await MinioClient.multipart_upload(data=data, object_name=object_name)
    except S3Error as e:
        raise f"Failed to upload data to Minio: {e}"
