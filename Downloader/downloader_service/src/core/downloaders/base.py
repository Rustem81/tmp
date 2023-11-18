from concurrent.futures.thread import ThreadPoolExecutor
import csv
import io
import zipfile
from src.core.s3.minio_uploader import DataUploader
from minio.error import S3Error


class DatabaseDownloader:
    """
    Базовый класс работы с БД и данными
    """
    def __init__(self, connection_data: str=None, query: str=None, *args, **kwargs) -> None:
        self._connection_str = connection_data
        self.query = query
        self._pool = ThreadPoolExecutor()
        self.s3_client = DataUploader()

    async def get_data(self):
        raise NotImplementedError("Subclasses should implement this method")

    async def save_to_zip(self, data):
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)
        csv_buffer.seek(0)

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('data.csv', csv_buffer.getvalue())

        zip_buffer.seek(0)
        return zip_buffer