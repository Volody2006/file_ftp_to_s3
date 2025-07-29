import logging
import os
from datetime import datetime, timedelta

import pysftp
from celery import Celery
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "files")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

celery_app = Celery(
    "tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['webapp.celery_worker']
)

from webapp.models import SFTPServer, ImportedFile, FileStatus
from webapp.database import SessionLocal
from webapp.connectors import (amqp_publisher, MinioClient,
                               MINIO_ENDPOINT, MINIO_ACCESS_KEY,
                               MINIO_SECRET_KEY, MINIO_SECURE)

logger = logging.getLogger(__name__)


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Настраивает периодический запуск задач через Celery Beat."""

    sender.add_periodic_task(30.0, discover_all_servers.s(), name='discover all servers every 30s')
    sender.add_periodic_task(15 * 60.0, reconcile_stuck_files.s(), name='reconcile stuck files every 15m')


@celery_app.task(name="webapp.discover_all_servers")
def discover_all_servers():
    """Находит все активные серверы и создает задачи обнаружения для них."""

    db: Session = SessionLocal()
    try:
        active_servers = db.query(SFTPServer).filter(SFTPServer.is_active == True).all()
        for server in active_servers:
            discover_files_on_server.delay(server.id)
    finally:
        db.close()


@celery_app.task(name="webapp.discover_files_on_server")
def discover_files_on_server(server_id: int):
    """Обнаруживает новые файлы и запускает для каждого задачу скачивания."""

    db: Session = SessionLocal()
    try:
        server = db.query(SFTPServer).filter_by(id=server_id).first()
        if not server: return

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(
                host=server.host, username=server.username,
                password=server.credentials, port=server.port, cnopts=cnopts
        ) as sftp:
            remote_files_attrs = {attr.filename: attr for attr in sftp.listdir_attr(server.directory)}
            existing_db_files = {
                f.filename for f in db.query(ImportedFile.filename).filter(
                    ImportedFile.server_id == server_id,
                    ImportedFile.filename.in_(remote_files_attrs.keys())
                ).all()
            }

            for filename, attr in remote_files_attrs.items():
                full_path = f"{server.directory}/{filename}"
                if not sftp.isfile(full_path) or filename.endswith('.tmp'): continue

                if filename not in existing_db_files:
                    new_file = ImportedFile(
                        server_id=server.id, filename=filename, filesize=attr.st_size, status=FileStatus.DISCOVERED
                    )
                    db.add(new_file)
                    try:
                        db.commit()
                        download_file.delay(new_file.id)
                        logger.debug(f"Обнаружен новый файл: {filename}, запуск скачивания.")
                    except IntegrityError:
                        db.rollback()
                        logger.warning(f"Файл {filename} уже был обнаружен. Пропускаем.")
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Не удалось запустить обработку для {filename}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обнаружении файлов на сервере {server_id}: {e}", exc_info=True)
    finally:
        db.close()


@celery_app.task(bind=True, name="webapp.download_file", autoretry_for=(Exception,),
                 retry_kwargs={'max_retries': 3, 'countdown': 60})
def download_file(self, file_id: int):
    """Скачивает файл с SFTP в общую временную директорию."""

    db: Session = SessionLocal()
    try:
        file_record = db.query(ImportedFile).options(joinedload(ImportedFile.server)).get(file_id)
        if not file_record or file_record.status != FileStatus.DISCOVERED:
            logger.warning(f"Задача скачивания для файла {file_id} отменена (неверный статус или не найден).")
            return

        server = file_record.server
        local_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.dat")

        file_record.status = FileStatus.DOWNLOADING
        db.commit()

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(
                host=server.host, username=server.username, password=server.credentials, port=server.port, cnopts=cnopts
        ) as sftp:
            sftp.get(f"{server.directory}/{file_record.filename}", local_path)

        file_record.status = FileStatus.DOWNLOADED
        db.commit()
        logger.info(f"Файл {file_id} успешно скачан в {local_path}.")

        upload_to_minio.delay(file_id)

    except Exception as e:
        handle_task_failure(db, file_id, e)
        raise self.retry(exc=e)
    finally:
        db.close()


@celery_app.task(bind=True, name="webapp.upload_to_minio", autoretry_for=(Exception,),
                 retry_kwargs={'max_retries': 3, 'countdown': 60})
def upload_to_minio(self, file_id: int):
    """Загружает скачанный файл из временной директории в MinIO."""

    db: Session = SessionLocal()
    local_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.dat")
    try:
        file_record = db.query(ImportedFile).options(joinedload(ImportedFile.server)).get(file_id)
        if not file_record or file_record.status != FileStatus.DOWNLOADED:
            logger.warning(f"Задача загрузки в MinIO для файла {file_id} отменена (неверный статус или не найден).")
            return

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Локальный файл {local_path} для загрузки в MinIO не найден.")

        file_record.status = FileStatus.UPLOADING_TO_MINIO
        db.commit()

        minio_client = MinioClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)
        object_name = f"{file_record.server.host}/{file_record.filename}"

        with open(local_path, 'rb') as file_data:
            minio_client.upload_fileobj(file_data, MINIO_BUCKET_NAME, object_name)

        file_record.status = FileStatus.UPLOADED_TO_MINIO
        db.commit()
        logger.info(f"Файл {file_id} успешно загружен в MinIO как {object_name}.")

        notify_and_cleanup.delay(file_id)

    except Exception as e:
        handle_task_failure(db, file_id, e)
        raise self.retry(exc=e)
    finally:
        db.close()


@celery_app.task(bind=True, name="webapp.notify_and_cleanup", autoretry_for=(Exception,),
                 retry_kwargs={'max_retries': 3, 'countdown': 60})
def notify_and_cleanup(self, file_id: int):
    """Отправляет уведомление и удаляет временный файл."""

    db: Session = SessionLocal()
    local_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.dat")
    try:
        file_record = db.query(ImportedFile).options(joinedload(ImportedFile.server)).get(file_id)
        if not file_record or file_record.status != FileStatus.UPLOADED_TO_MINIO:
            logger.warning(f"Задача уведомления для файла {file_id} отменена (неверный статус или не найден).")
            return

        file_record.status = FileStatus.NOTIFYING
        db.commit()

        amqp_publisher.send_notification(
            message={
                "file_id": file_record.id,
                "minio_path": f"{file_record.server.host}/{file_record.filename}"
            },
            routing_key="file.processed",
            queue_name="file_processed_queue"

        )

        file_record.status = FileStatus.COMPLETED
        file_record.error_message = None
        db.commit()
        logger.info(f"Файл {file_id} полностью обработан.")

    except Exception as e:
        handle_task_failure(db, file_id, e)
        raise self.retry(exc=e)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.debug(f"Временный файл {local_path} удален.")
        db.close()


@celery_app.task(name="webapp.reconcile_stuck_files")
def reconcile_stuck_files():
    """Находит файлы, застрявшие на промежуточных этапах, и перезапускает их обработку."""
    db: Session = SessionLocal()
    try:
        # Ищем файлы, которые находятся в промежуточном состоянии дольше 1 часа
        stuck_time = datetime.utcnow() - timedelta(hours=1)
        stuck_files = db.query(ImportedFile).filter(
            ImportedFile.updated_at < stuck_time,
            or_(
                ImportedFile.status == FileStatus.DISCOVERED,
                ImportedFile.status == FileStatus.DOWNLOADING,
                ImportedFile.status == FileStatus.DOWNLOADED,
                ImportedFile.status == FileStatus.UPLOADING_TO_MINIO,
                ImportedFile.status == FileStatus.UPLOADED_TO_MINIO,
                ImportedFile.status == FileStatus.NOTIFYING
            )
        ).all()

        if not stuck_files:
            logger.info("Проверка зависших файлов: не найдено.")
            return

        logger.warning(f"Обнаружено {len(stuck_files)} зависших файлов. Запуск восстановления.")
        for file in stuck_files:
            if file.status in [FileStatus.DISCOVERED, FileStatus.DOWNLOADING]:
                logger.info(f"Перезапуск скачивания для зависшего файла {file.id} (статус: {file.status.value})")
                file.status = FileStatus.DISCOVERED
                download_file.delay(file.id)
            elif file.status in [FileStatus.DOWNLOADED, FileStatus.UPLOADING_TO_MINIO]:
                logger.info(f"Перезапуск загрузки в MinIO для зависшего файла {file.id} (статус: {file.status.value})")
                file.status = FileStatus.DOWNLOADED
                upload_to_minio.delay(file.id)
            elif file.status in [FileStatus.UPLOADED_TO_MINIO, FileStatus.NOTIFYING]:
                logger.info(f"Перезапуск уведомления для зависшего файла {file.id} (статус: {file.status.value})")
                file.status = FileStatus.UPLOADED_TO_MINIO
                notify_and_cleanup.delay(file.id)
        db.commit()

    except Exception as e:
        logger.error(f"Ошибка в задаче-санитаре: {e}", exc_info=True)
    finally:
        db.close()


def handle_task_failure(db: Session, file_id: int, error: Exception):
    """Централизованно обновляет статус файла на FAILED."""
    logger.error(f"Ошибка при обработке файла ID={file_id}: {error}", exc_info=True)
    db.rollback()
    try:
        failed_file = db.query(ImportedFile).get(file_id)
        if failed_file:
            failed_file.status = FileStatus.FAILED
            failed_file.error_message = str(error)
            db.commit()
    except Exception as db_exc:
        logger.error(f"Критическая ошибка: не удалось даже обновить статус на FAILED для файла {file_id}: {db_exc}")
        db.rollback()
