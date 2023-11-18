from pydantic import BaseSettings, Field

class Settings(BaseSettings):

    client_path: str = Field(title="Oracle client path", default='/usr/lib/oracle/19.20/client64/lib')
    prefetch_rows: int = Field(title="Num of prefetch rows", default=1000)
    minio_host:str = Field(title="Minio host")
    minio_access_key:str = Field(title="Access key", default="minioadmin")
    minio_secret_key:str = Field(title="Secret key", default="minioadmin")
    minio_bucket_name:str = Field(title="Minio bucket name", default="datawarehouse")
    DATA_ACCESS_EXPIRATION_DAYS:int = Field(title="Data access expiration days", default=7)

    class Config:
        env_file = ".env"

settings = Settings()