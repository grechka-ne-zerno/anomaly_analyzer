import logging
from queue import Queue
from typing import Dict, Any, List, Optional
from threading import Thread
from daily_statistic import DailyStats
from kafka_consumer import KafkaMessageReader
from message_converter import MessageConverter
from time import time

class PartitionManager:
    def __init__(self, config: Dict[str, Any], daily_statistic: DailyStats):
        self.config = config
        self.num_partitions = self.config.get('num_partitions', 1)
        self.daily_statistic = daily_statistic
        self.converters: List[MessageConverter] = []
        self.readers: List[KafkaMessageReader] = []
        self.threads: List[Thread] = []

        #Статистика
        self.start_time = time()
        self.last_msg_seconds = [None] * self.num_partitions
        self.last_log_count = 0
        self.processed_messages = 0
        self.filtered_messages = 0
        self.partition_start_hour = {}   # время первого сообщения для каждой партиции
        self.ready_partitions = set()    # партиции, набравшие час

        self.logger = logging.getLogger(__name__)

    def start_all(self, message_queue):
        for partition in range(self.num_partitions):

            converter = MessageConverter(partition)
            self.converters.append(converter)

            reader = KafkaMessageReader(self.config, partition)
            self.readers.append(reader)

            thread = Thread(target=self._read_loop, args=(reader, partition, converter, message_queue))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

        self.logger.info(f"✅ Запущено {self.num_partitions} потоков для чтения партиций")

    def _update_partition_time(self, partition: int, timestamp_ms: int):
        current_hour = timestamp_ms // 3600000

        # Первое сообщение
        if partition not in self.partition_start_hour:
            self.partition_start_hour[partition] = current_hour
            return

        old_hour = self.partition_start_hour[partition]

        if current_hour > old_hour:
            if partition not in self.ready_partitions:
                self.ready_partitions.add(partition)
                self.logger.debug(f"✅ Партиция {partition} завершила час {old_hour}")

            self.partition_start_hour[partition] = current_hour

    def check_all_ready(self) -> bool:
        if len(self.ready_partitions) != self.num_partitions:
            return False

        # Сбрасываем для нового часа
        self.ready_partitions.clear()
        self.logger.info(f"✅ Вычитал час по всем партициям.")
        return True

    def log_statistics(self, queue_size: int, delta: int):
            instant_speed = (self.processed_messages - self.last_log_count) / delta
            self.last_log_count = self.processed_messages
            self.logger.info(
                f"📊 Статистика: "
                f"Обработано: {self.processed_messages}, "
                f"Отфильтровано: {self.filtered_messages}, "
                f"Скорость: {instant_speed:.1f} msg/sec, "
                f"Размер очереди: {queue_size}"
            )

    def _decode_key(self, message_key) -> Optional[str]:
        if not message_key:
            self.logger.warning("⚠️ Ключ отсутствует!")
            return None

            # Декодируем ключ
        if isinstance(message_key, bytes):
            msg_key = message_key.decode('utf-8').strip()
        else:
            msg_key = str(message_key)

        return msg_key

    def _read_loop(self, reader: KafkaMessageReader, partition: int, converter: MessageConverter, message_queue: Queue):
        while reader.running:
            try:
                message = reader.read_message()

                if message:
                    self.processed_messages += 1
                    if message.timestamp():
                        ts_ms = message.timestamp()[1]
                        self.last_msg_seconds[partition] = ts_ms
                        self._update_partition_time(partition, ts_ms)

                    msg_key = self._decode_key(message.key())
                    if msg_key:
                        if any(msg_key.startswith(prefix) for prefix in self.config.get('projections')):
                            self.filtered_messages += 1
                            anomalies = converter.add_message(message, msg_key)
                            self.logger.debug(f"✅ Сообщение с ключом {msg_key} передано в обработку.")
                            if anomalies:
                                self.daily_statistic.update_stats(anomalies)
                                message_queue.put((partition, anomalies))
                                self.logger.debug(f"✅ Аномалий обработано: {len(anomalies)}.")

                    timeout_anomalies = converter.check_timeout()
                    if timeout_anomalies:
                        self.daily_statistic.update_stats(timeout_anomalies)
                        message_queue.put((partition, timeout_anomalies))
                        self.logger.debug(f"✅ Аномалий обработано по таймауту: {len(timeout_anomalies)}.")

            except Exception as e:
                if reader.running:
                    self.logger.error(f"Ошибка в потоке {partition}: {e}")

    def stop_all(self):
        self.logger.info("Начинаю остановку вычитывателей.")
        for reader in self.readers:
            reader.running = False
            reader.close()
        self.logger.info("Начинаю остановку преобразователей.")
        for converter in self.converters:
            converter.shutdown()