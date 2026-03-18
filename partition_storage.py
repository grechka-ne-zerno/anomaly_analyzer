import orjson
import logging
import os
from datetime import datetime

class PartitionStorage:
    def __init__(self, partition_id: int):
        self.logger = logging.getLogger(__name__)
        self.partition_id = partition_id
        self.storage_dir = "partition_storage"
        os.makedirs(self.storage_dir, exist_ok=True)
        self.storage_file = os.path.join(self.storage_dir, f"partition_{partition_id}.json")

    def save(self, current_second: int, current_data: dict, past_second: int = None, past_data: dict = None):
        try:
            data = {
                'partition_id': self.partition_id,
                'current_second': current_second,
                'current_data': current_data,
                'past_second': past_second,
                'past_data': past_data,
                'saved_at': datetime.now().isoformat()
            }

            with open(self.storage_file, 'wb') as f:
                f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_NUMPY))

            total = (len(current_data) if current_data else 0) + (len(past_data) if past_data else 0)
            self.logger.info(f"💾 Сохранено {total} сообщений для партиции {self.partition_id}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения: {e}")

    def load(self):
        if not os.path.exists(self.storage_file):
            return None, None, None, None

        try:
            with open(self.storage_file, 'rb') as f:  # бинарный режим
                data = orjson.loads(f.read())

            if data.get('partition_id') != self.partition_id:
                self.logger.warning(f"⚠️ Несоответствие partition_id")
                return None, None, None, None

            os.remove(self.storage_file)

            return (data.get('current_second'), data.get('current_data'),
                    data.get('past_second'), data.get('past_data'))

        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки: {e}")
            return None, None, None, None