from pydantic import AnyHttpUrl, BaseModel, Field, validator

class ConnectionModel(BaseModel):
    username: str = Field(None,title='Имя пользователя')
    password: str = Field(None,title='Пароль')
    conn_string: str = Field(..., title='Строка подключения')

class SQLQueryModel(BaseModel):

    db_type: int = Field(..., title='Тип СУБД', description="1 - Oracle, 2 - PostgreSQL, 3 - MSSQL", gt=0, le=3)
    connection: ConnectionModel
    query: str = Field(..., title='SQL запрос на выполнение')

    @validator('connection', pre=True)
    def username_password_required(cls, connection, values):
        db_type = values.get('db_type')
        if db_type == 1:
            if connection.get('username') is None or connection.get('password') is None:
                raise MissingError('Username and password are required for Oracle!')
        return connection

class MinioUploadResponse(BaseModel):
    url:AnyHttpUrl

class BaseMessage(BaseModel):
    detail: str = Field(...)

class MissingError(BaseMessage):
    pass