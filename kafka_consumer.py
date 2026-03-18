import logging
from typing import Dict, Any, Optional, Union
from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

class KafkaMessageReader:
    def __init__(self, config: Dict[str, Any], partition: Optional[int] = None):
        self.config = config
        self.target_projections = self.config.get('projections',[])

        self.partition = partition

        self.logger = logging.getLogger(__name__)
        self.consumer: Optional[Consumer] = None
        self.is_connected = False
        self.running = True
        self.connect()

    def connect(self):
        try:
            self.logger.info(f"⏳ Подключение к Kafka для партиции {self.partition}...")

            consumer_config = self.config.get('consumer_config').copy()
            topic = self.config.get('topic')

            self.consumer = Consumer(consumer_config)

            # Если указана конкретная партиция - назначаем её
            if self.partition is not None:
                self.consumer.assign([TopicPartition(topic, self.partition)])
            else:
                self.consumer.subscribe([topic])

            self.is_connected = True
            self.logger.info(f"✅ Успешное подключение к Kafka. Топик: {topic}, Партиция: {self.partition}")

        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при подключении к Kafka: {e}")
            raise ValueError(f"❌ Ошибка подключения к кафке!")

    def read_message(self) -> Union[Message, Dict]:
        if not self.consumer or not self.is_connected:
            self.logger.error("❗ Consumer не подключен")
            return {}

        try:
            message = self.consumer.poll(timeout=1.0)

            if message is None:
                return {}

            # Добавляем обработку EOF
            if message and message.error() and message.error().code() == KafkaError._PARTITION_EOF:
                return {}  # Просто пропускаем EOF

            if message.error():
                self.logger.error(f"❗ Ошибка чтения сообщения: {message.error()}")
                return {}

            return message

        except KafkaError as e:
            self.logger.error(f"❌ Ошибка чтения из Kafka: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при чтении: {e}")
            return {}

    def close(self):
        if self.consumer:
            try:
                self.consumer.close()
                self.is_connected = False
                self.logger.info(f"✅ Соединение с Kafka закрыто для партиции {self.partition}")
            except Exception as e:
                self.logger.error(f"❌ Ошибка при закрытии соединения: {e}")