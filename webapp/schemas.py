from datetime import datetime
from typing import List, Optional


from pydantic import BaseModel

from webapp.models import FileStatus


class FileBase(BaseModel):
    filename: str
    filesize: Optional[int] = None
    status: FileStatus


class FileDetails(FileBase):
    id: int
    server_id: int
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ServerBase(BaseModel):
    host: str
    port: int = 22
    username: str
    directory: str
    is_active: bool = True


class ServerCreate(ServerBase):
    credentials: str


class ServerUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    credentials: Optional[str] = None
    directory: Optional[str] = None
    is_active: Optional[bool] = None


class ServerDetails(ServerBase):
    id: int

    class Config:
        from_attributes = True


class ServerWithFiles(ServerDetails):
    files: List[FileDetails] = []
