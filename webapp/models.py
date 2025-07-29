import enum
from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    ForeignKey, BigInteger, Enum as PgEnum, Text, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class FileStatus(enum.Enum):
    DISCOVERED = "discovered"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    UPLOADING_TO_MINIO = "uploading_to_minio"
    UPLOADED_TO_MINIO = "uploaded_to_minio"
    NOTIFYING = "notifying"
    COMPLETED = "completed"
    FAILED = "failed"


class SFTPServer(Base):
    id = Column(Integer, primary_key=True)
    host = Column(String(255), nullable=False, unique=True)
    port = Column(Integer, default=22, nullable=False)
    username = Column(String(100), nullable=False)
    credentials = Column(String(512), nullable=False)
    directory = Column(String(512), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    files = relationship("ImportedFile", back_populates="server", cascade="all, delete-orphan")

    __tablename__ = 'sftp_servers'


class ImportedFile(Base):
    id = Column(Integer, primary_key=True)
    server_id = Column(Integer, ForeignKey('sftp_servers.id'), nullable=False)
    filename = Column(String(512), nullable=False)
    filesize = Column(BigInteger, nullable=True)
    status = Column(PgEnum(FileStatus), nullable=False, default=FileStatus.DISCOVERED)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    server = relationship("SFTPServer", back_populates="files")

    __tablename__ = 'imported_files'
    __table_args__ = (
        UniqueConstraint('server_id', 'filename', name='_server_filename_uc'),
    )
