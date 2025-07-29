from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from webapp import schemas, models
from webapp.database import get_db

router = APIRouter()


@router.post("/servers/", response_model=schemas.ServerDetails, status_code=status.HTTP_201_CREATED)
def create_server(server: schemas.ServerCreate, db: Session = Depends(get_db)):
    """Создать новый SFTP сервер для мониторинга. """
    db_server = models.SFTPServer(**server.model_dump())
    db.add(db_server)
    try:
        db.commit()
        db.refresh(db_server)
    except Exception:
        db.rollback()
        raise HTTPException(status_code=400, detail="Server with this host already exists.")
    return db_server


@router.get("/servers/", response_model=List[schemas.ServerDetails])
def read_servers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Получить список всех сконфигурированных серверов. """
    servers = db.query(models.SFTPServer).offset(skip).limit(limit).all()
    return servers


@router.get("/servers/{server_id}", response_model=schemas.ServerDetails)
def read_server(server_id: int, db: Session = Depends(get_db)):
    """Получить детальную информацию о конкретном сервере. """
    db_server = db.query(models.SFTPServer).filter(models.SFTPServer.id == server_id).first()
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    return db_server


@router.patch("/servers/{server_id}", response_model=schemas.ServerDetails)
def update_server(server_id: int, server_update: schemas.ServerUpdate, db: Session = Depends(get_db)):
    """Обновить данные сервера (например, деактивировать). """
    db_server = db.query(models.SFTPServer).filter(models.SFTPServer.id == server_id).first()
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")

    update_data = server_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_server, key, value)

    db.add(db_server)
    db.commit()
    db.refresh(db_server)
    return db_server


@router.delete("/servers/{server_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_server(server_id: int, db: Session = Depends(get_db)):
    """Удалить сервер и все связанные с ним файлы из мониторинга. """
    db_server = db.query(models.SFTPServer).filter(models.SFTPServer.id == server_id).first()
    if db_server is None:
        raise HTTPException(status_code=404, detail="Server not found")
    db.delete(db_server)
    db.commit()
    return


@router.get("/files/", response_model=List[schemas.FileDetails])
def read_files(
        status: Optional[models.FileStatus] = Query(None, description="Фильтр по статусу обработки"),
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """ 📂 Получить список импортированных файлов с возможностью фильтрации. """
    query = db.query(models.ImportedFile)
    if status:
        query = query.filter(models.ImportedFile.status == status)
    files = query.order_by(models.ImportedFile.updated_at.desc()).offset(skip).limit(limit).all()
    return files


@router.get("/files/{file_id}", response_model=schemas.FileDetails)
def read_file(file_id: int, db: Session = Depends(get_db)):
    """Получить детальную информацию о конкретном файле. """
    db_file = db.query(models.ImportedFile).filter(models.ImportedFile.id == file_id).first()
    if db_file is None:
        raise HTTPException(status_code=404, detail="File not found")
    return db_file
