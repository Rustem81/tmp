"""
Универсальный сервис для получения данных из различных РСУБД 
"""
from fastapi import FastAPI
from fastapi_versioning import VersionedFastAPI
import cx_Oracle
from src.routers import data_loader
from config import settings

app = FastAPI(
    title="DBMS Downloader",
    description="Service for download data from different DBMS",
    openapi_url="/openapi.json",
    docs_url="/doc"
)

app.include_router(
    data_loader.router,
)

app = VersionedFastAPI(app, prefix_format="/api"+'/v{major}_{minor}', enable_latest=True)

@app.on_event('startup')
async def startup_event():
    cx_Oracle.init_oracle_client(lib_dir=settings.client_path)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8087, log_level="debug", use_colors=False)