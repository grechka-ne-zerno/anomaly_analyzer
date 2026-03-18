import logging

import yaml
from typing import Dict, Any
from logging.handlers import TimedRotatingFileHandler

class ConfigParser:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.data: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)

        self.logger.info(f"🚀Стартуем! Настройка анализатора..")
        self._load_config()

    def _load_config(self) -> None:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.data = yaml.safe_load(file)
            self.logger.info(f"✅ Конфигурация загружена из {self.config_path}")
        except FileNotFoundError:
            self.logger.error(f"❌ Конфигурационный файл {self.config_path} не найден")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"❌ Ошибка парсинга YAML: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при загрузке конфига: {e}")
            raise

    def get_save_total_statistic(self):
        return self.data.get('save_total_statistic', False)

    def get_notifaer_config(self) -> Dict[str, Any]:
        return self.data.get('notifaer', {})

    def get_kafka_config(self) -> Dict[str, Any]:
        return self.data.get('kafka', {})

    def get_logging_config(self) -> Dict[str, Any]:
        return self.data.get('logging', {})

    def validate_config(self) -> bool:
        required_sections = ['notifaer', 'kafka', 'logging']

        for section in required_sections:
            if section not in self.data:
                self.logger.error(f"❌ Отсутствует обязательная секция: {section}")
                return False

        tg_config = self.get_notifaer_config().get('telegram')
        if not tg_config:
            self.logger.error("❌ Отсутствует поле 'telegram'!")
            return False

        required_tg_fields = ['token', 'chat.id', 'is_active']
        for field in required_tg_fields:
            if tg_config.get(field) is None:
                self.logger.error(f"❌ Отсутствует обязательное поле для Telegram: {field}")
                return False

        yu_config = self.get_notifaer_config().get('yuchat')
        if not yu_config:
            self.logger.error("❌ Отсутствует поле 'yuchat'!")
            return False

        required_yuchat_fields = ['token', 'chat.id', 'workspace.id', 'is_active']
        for field in required_yuchat_fields:
            if yu_config.get(field) is None:
                self.logger.error(f"❌ Отсутствует обязательное поле для YuChat: {field}")
                return False

        # Проверка обязательных полей Kafka
        kafka_config = self.get_kafka_config()
        required_kafka_fields = ['consumer_config', 'topic', 'num_partitions', 'projections']

        for field in required_kafka_fields:
            if kafka_config.get(field) is None:
                self.logger.error(f"❌ Отсутствует обязательное поле для Kafka: {field}")
                return False

        if kafka_config.get('num_partitions', 0) <= 0:
            self.logger.error(f"❌ Укажите количество партиций в топике!")
            return False

        if len(kafka_config.get('projections', [])) == 0:
            self.logger.error(f"❌ Укажите искомые проекции!")
            return False

        consumer_config = kafka_config.get('consumer_config', {})
        required_consumer_config_field = ['bootstrap.servers', 'group.id', 'auto.offset.reset', 'enable.auto.commit']
        for field in required_consumer_config_field:
            if not consumer_config.get(field):
                self.logger.error(f"❌ Отсутствует обязательное поле для consumer_config: {field}")
                return False

        self.logger.debug("✅ Конфигурация валидна")
        return True

def setup_logging_from_config(config: ConfigParser):
    logging_config = config.get_logging_config()
    logging.basicConfig(
        level = getattr(logging, logging_config['level'].upper()),
        format = logging_config['format'],
        datefmt = logging_config['datefmt'],
        force = True  # Перезаписываем существующие настройки
    )
    formatter = logging.Formatter(
        fmt=logging_config['format'],
        datefmt=logging_config['datefmt']
    )
    handler = TimedRotatingFileHandler("logs/application.log", when="midnight", backupCount=10, encoding="utf-8")
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)