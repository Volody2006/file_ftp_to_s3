import json
import logging
import os

import pika
from minio import Minio, S3Error

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", False)
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@localhost:5672/")


class MinioClient:
    def __init__(self, endpoint, access_key, secret_key, secure=MINIO_SECURE):
        """
        Инициализирует клиент MinIO.

        :param endpoint: Адрес конечной точки MinIO (например, "localhost:9000").
        :param access_key: Ключ доступа MinIO.
        :param secret_key: Секретный ключ Minio.
        :param secure: Использовать HTTPS (True) или HTTP (False).
        """
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            logger.info("MinioClient успешно инициализирован.")
        except Exception as e:
            logger.error(f"Ошибка инициализации MinioClient: {e}")
            self.client = None

    def upload_fileobj(self, file_obj, bucket_name, object_name):
        """
        Загружает объект файла в MinIO.

        :param file_obj: Открытый файловый объект (например, io.BytesIO, SpooledTemporaryFile).
        :param bucket_name: Имя целевого бакета MinIO.
        :param object_name: Имя объекта, под которым файл будет сохранен в бакете.
        :return: True в случае успешной загрузки, False в противном случае.
        """
        if not self.client:
            logger.error("MinioClient не инициализирован. Невозможно выполнить загрузку.")
            return False

        logger.info(f"Начало загрузки в MinIO: бакет='{bucket_name}', объект='{object_name}'")

        try:
            file_obj.seek(0)

            current_pos = file_obj.tell()
            file_obj.seek(0, 2)
            file_size = file_obj.tell()
            file_obj.seek(current_pos)

            if file_size == 0:
                logger.warning(
                    f"Попытка загрузить пустой файл: бакет='{bucket_name}', объект='{object_name}'. Пропускаем.")
                return False

            logger.info(f"Размер файла для загрузки: {file_size} байт")

            found = self.client.bucket_exists(bucket_name)
            if not found:
                try:
                    self.client.make_bucket(bucket_name)
                    logger.info(f"Бакет '{bucket_name}' создан успешно.")
                except S3Error as e:
                    logger.error(f"Ошибка при создании бакета '{bucket_name}': {e}")
                    return False
            else:
                logger.info(f"Бакет '{bucket_name}' уже существует.")

            self.client.put_object(
                bucket_name,
                object_name,
                file_obj,
                file_size,
                content_type="application/octet-stream"
            )
            logger.info(f"Файл успешно загружен в MinIO: бакет='{bucket_name}', объект='{object_name}'")
            return True
        except S3Error as e:
            logger.error(f"Ошибка MinIO при загрузке файла '{object_name}' в бакет '{bucket_name}': {e}")
            return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при загрузке файла '{object_name}' в MinIO: {e}", exc_info=True)
            return False


class AmqpPublisher:
    def __init__(self, broker_url: str, exchange_name: str = 'file_events', exchange_type: str = 'topic'):
        """
        Инициализирует паблишер.
        :param broker_url: URL для подключения к RabbitMQ (например, 'amqp://guest:guest@localhost:5672/').
        :param exchange_name: Имя обменника для отправки сообщений.
        :param exchange_type: Тип обменника (topic, direct, fanout).
        """
        self.broker_url = broker_url
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.connection = None
        self.channel = None

    def _connect(self, queue_name: str = None, routing_key: str = None):
        """
        Устанавливает соединение, создает канал и объявляет обменник,
        а также опционально объявляет и привязывает очередь.

        :param queue_name: Имя очереди для объявления и привязки. Если None, очередь не объявляется.
        :param routing_key: Маршрутный ключ для привязки очереди. Требуется, если указан queue_name.
        """
        self.connection = pika.BlockingConnection(pika.URLParameters(self.broker_url))
        self.channel = self.connection.channel()

        self.channel.confirm_delivery()

        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=True
        )
        logger.debug(f"Обменник '{self.exchange_name}' типа '{self.exchange_type}' объявлен.")


        if queue_name and routing_key:
            self.channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
            logger.debug(f"Очередь '{queue_name}' объявлена.")

            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key
            )
            logger.debug(f"Очередь '{queue_name}' привязана к обменнику '{self.exchange_name}' с ключом '{routing_key}'.")

    def _disconnect(self):
        """Закрывает канал и соединение."""
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
                logger.debug("Канал AMQP закрыт.")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии канала AMQP: {e}")
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
                logger.debug("Соединение AMQP закрыто.")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии соединения AMQP: {e}")

    def send_notification(self, message: dict, routing_key: str, queue_name: str = None):
        """
        Отправляет сообщение в RabbitMQ с подтверждением доставки.
        :param message: Словарь с данными сообщения.
        :param routing_key: Маршрутный ключ для сообщения.
        :param queue_name: Опциональное имя очереди, которая будет объявлена и привязана.
        """
        try:
            self._connect(queue_name=queue_name, routing_key=routing_key)
            message_body = json.dumps(message)

            was_published = self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
                mandatory=True # Если сообщение не может быть маршрутизировано, оно будет возвращено
            )

            if was_published:
                logger.info(f"Подтверждена отправка уведомления с ключом '{routing_key}'.")
                return True
            else:
                # Это произойдет, если брокер отклонил сообщение (nack)
                logger.error(f"Отправка уведомления с ключом '{routing_key}' НЕ подтверждена брокером.")
                return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при отправке AMQP уведомления: {e}", exc_info=True)
            return False
        finally:
            self._disconnect()

minio_client = MinioClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE)
amqp_publisher = AmqpPublisher(broker_url=BROKER_URL)
