import logging
from typing import Dict, Any
import requests
import json

class AnomalyNotifier:
    def __init__(self, config: Dict[str, Any]):
        self.tg_config = config.get('telegram')
        self.tgbase_url = f"https://api.telegram.org/bot{self.tg_config.get('token')}"

        self.yu_config = config.get('yuchat')
        self.ybase_url = f"https://" #Ваш бот апи для ючата

        self.logger = logging.getLogger(__name__)
        self.logger.info("✅ Уведомления инициализированы")

    def notify_chat(self, message: str) -> None:
        if self.tg_config.get('is_active'):
            self.send_tg_message(message)
        if self.yu_config.get('is_active'):
            self.send_yu_message(message)

    def send_tg_message(self, message: str, retry: int = 0) -> None:
        url = f"{self.tgbase_url}/sendMessage"
        payload = {
            "chat_id": self.tg_config.get('chat.id'),
            "text": message,
            "parse_mode": "HTML"
        }

        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            self.logger.info("✅ Сообщение отправлено в телеграм")
        except requests.exceptions.Timeout:
            self.logger.error("❌ Таймаут при отправке сообщения в телеграм: сервер не ответил вовремя")
            if retry <= 3:
                retry +=1
                self.send_tg_message(message, retry)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Ошибка отправки сообщения: {e}")
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка: {e}")

    def send_yu_message(self, message: str) -> None:
        url = f"{self.ybase_url}/public/v1/chat.message.send"
        payload = json.dumps({
            "workspaceId": self.yu_config.get('workspace.id'),
            "chatId": self.yu_config.get('chat.id'),
            "markdown": message
        })
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.yu_config.get("token")}'
        }

        try:
            self.logger.debug(f"Отправка запроса на {url}")
            response = requests.post(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            self.logger.info("✅ Сообщение отправлено в ючат")
        except requests.exceptions.Timeout:
            self.logger.error("❌ Таймаут при отправке сообщения в ючат: сервер не ответил вовремя")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Ошибка отправки сообщения: {e}")
        except Exception as e:
            self.logger.error(f"❌ Неожиданная ошибка: {e}")