import logging
import orjson
from collections import defaultdict
from time import time
from typing import Dict, List, Optional
from confluent_kafka import Message
from partition_storage import PartitionStorage

class MessageConverter:
    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self.logger = logging.getLogger(__name__)

        # Хранилище необработанных данных при остановке приложения
        self.storage = PartitionStorage(partition_id)

        # Буфер для текущей секунды
        self.current_second_data: Dict[str, List[Dict]] = defaultdict(list)
        self.current_second: Optional[int] = None

        # Буфер для прошлой секунды
        self.past_second_data: Dict[str, List[Dict]] = defaultdict(list)
        self.past_second: Optional[int] = None

        self.last_message_time = None
        self.process_timeout = 10

        self._load_saved_data()

    def _load_saved_data(self):
        current_second, current_second_data, past_second, past_second_data = self.storage.load()

        if current_second and current_second_data:
            self.current_second = current_second
            self.current_second_data.update(current_second_data)

        if past_second and past_second_data:
            self.past_second = past_second
            self.past_second_data.update(past_second_data)

        if current_second or past_second:
            total = (len(current_second_data) if current_second_data else 0) + (len(past_second_data) if past_second_data else 0)
            self.logger.info(f"🔄 Загружено {total} сообщений для партиции {self.partition_id}")

    def _decode_and_parse_message(self, message: Message) -> Dict:
        try:
            offset = message.offset() if message.offset() else "Оффсет не найден"
            message_value = message.value()
            if isinstance(message_value, bytes):
                msg_value = message_value.decode('utf-8')
            else:
                msg_value = str(message_value)

            action = msg_value[:2]
            body = msg_value[2:]
            try:
                if body and body.strip():
                    json_body = orjson.loads(body.encode())
                else:
                    json_body = {}
            except orjson.JSONDecodeError:
                json_body = {'raw_body': body}

            return {'offset': offset, 'action': action, 'body': json_body}
        except Exception as e:
            self.logger.error(f"❗Ошибка декодирования: {e}")
            return {}

    @staticmethod
    def _process_second(second: int, second_data: Dict[str, List[Dict]]) -> List[Dict]:
        anomalies = []
        for msg_key, messages in second_data.items():
            anomalies.append({
                'timestamp': second,
                'key': msg_key,
                'count': len(messages),
                'messages': messages
            })
        return anomalies

    def _handle_future(self, tm_sec: int, msg_key: str, parsed_message: dict) -> List[Dict]:
        anomalies = []
        if self.past_second is not None:
            anomalies.extend(self._process_second(self.past_second, self.past_second_data))
            self.past_second = None
            self.past_second_data.clear()

        anomalies.extend(self._process_second(self.current_second, self.current_second_data))

        # Начинаем новую секунду
        self.current_second = tm_sec
        self.current_second_data.clear()
        self.current_second_data[msg_key].append(parsed_message)

        return anomalies

    def _handle_past(self, tm_sec: int, key: str, parsed_message: dict) -> Optional[List[Dict]]:
        if self.current_second - tm_sec == 1:
            if self.past_second is None:
                self.past_second = tm_sec
            self.past_second_data[key].append(parsed_message)
            return None

        self.logger.warning(f"⚠️ Старое сообщение: {tm_sec} < {self.current_second}")
        return None

    def add_message(self, message: Message, msg_key: str) -> Optional[List[Dict]]:
        self.last_message_time = time()

        parsed_message = self._decode_and_parse_message(message)

        if not message.timestamp():
            return None

        tm_sec = message.timestamp()[1] // 1000

        # Первое сообщение
        if self.current_second is None:
            self.current_second = tm_sec
            self.current_second_data[msg_key].append(parsed_message)
            return None

        # Сообщение из будущего
        if tm_sec > self.current_second:
            return self._handle_future(tm_sec, msg_key, parsed_message)

        # Сообщение из прошлого
        if tm_sec < self.current_second:
            return self._handle_past(tm_sec, msg_key, parsed_message)

        # Текущая секунда
        self.current_second_data[msg_key].append(parsed_message)
        return None

    def check_timeout(self) -> Optional[List[Dict]]:
        if self.current_second is None or self.last_message_time is None:
            return None

        time_since_last = time() - self.last_message_time

        if time_since_last >= self.process_timeout:
            anomalies = []

            # Отправляем past секунду если есть
            if self.past_second:
                past_anomalies = self._process_second(self.past_second, self.past_second_data)
                anomalies.extend(past_anomalies)
                self.past_second = None
                self.past_second_data.clear()

            # Отправляем current секунду
            if self.current_second:
                current_anomalies = self._process_second(self.current_second, self.current_second_data)
                anomalies.extend(current_anomalies)
                self.current_second = None
                self.current_second_data.clear()

            self.logger.info(f"⏱️ Принудительная отправка по таймауту для партиции {self.partition_id}")
            return anomalies

        return None

    def shutdown(self):
        if self.last_message_time is not None:
            self.storage.save(
                self.current_second, self.current_second_data,
                self.past_second, self.past_second_data if self.past_second_data else None
            )
            total = len(self.current_second_data) + len(self.past_second_data)
            self.logger.info(f"💾 Сохранено {total} сообщений для партиции {self.partition_id}")
        else:
            self.logger.info(f"⏹️ Нет данных для сохранения в партиции {self.partition_id}")