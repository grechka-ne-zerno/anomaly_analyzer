from collections import defaultdict
from datetime import datetime
from anomaly_notifaer import AnomalyNotifier
from threading import Lock
import logging

class DailyStats:
    def __init__(self, notifaer: AnomalyNotifier):
        self.notifaer = notifaer
        # Храним статистику по дням
        self.daily_stats = defaultdict(lambda: {"per_hour": 0, "per_day": 0, "last_day": 0})
        self.current_day = None
        self.max_message_length = 4000
        self._stats_lock = Lock()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"✅ Сбор статистики инициализирован")

    def create_stats_message(self, current_time):
        message = "🌅 СТАТИСТИКА АНОМАЛИЙ 🌅\n\n"
        message += "<pre>\n"
        message += f" Дата: {current_time}\n"
        message += "┌──────────────────────────────────────────────────────┬─────────┬─────────┬─────────┐\n"
        message += "│                       Проекция                       │ За час  │ Сегодня │  Вчера  │\n"
        message += "├──────────────────────────────────────────────────────┼─────────┼─────────┼─────────┤\n"

        rows = []
        for i, (key, value) in enumerate(list(self.daily_stats.items())):
            row = ""
            if value['per_hour'] != 0 or value['per_day'] != 0 or value['last_day'] != 0:
                if i > 0:
                    row += "├──────────────────────────────────────────────────────┼─────────┼─────────┼─────────┤\n"
                row += f"│{key:^54}│{value['per_hour']:^9}│{value['per_day']:^9}│{value['last_day']:^9}│\n"
                rows.append(row)

        # Если сообщение слишком длинное, разбиваем на части
        full_message = message + "".join(rows) + "└──────────────────────────────────────────────────────┴─────────┴─────────┴─────────┘\n</pre>"

        if len(full_message) <= self.max_message_length:
            return [full_message]

        # Разбиваем на несколько сообщений
        messages = []
        current_message = message
        current_rows = []

        for row in rows:
            test_message = current_message + "".join(current_rows) + row
            if len(test_message) + 100 < self.max_message_length:  # +100 на закрывающие теги
                current_rows.append(row)
            else:
                # Отправляем текущую часть
                messages.append(current_message + "".join(current_rows) +
                                "└──────────────────────────────────────────────────────┴─────────┴─────────┴─────────┘\n</pre>")
                # Начинаем новую часть
                current_message = "🌅 СТАТИСТИКА АНОМАЛИЙ (продолжение) 🌅\n\n<pre>\n"
                current_message += f" Дата: {current_time}\n"
                current_message += "┌──────────────────────────────────────────────────────┬─────────┬─────────┬─────────┐\n"
                current_message += "│                       Проекция                       │  За час │ Сегодня │  Вчера  │\n"
                current_rows = [row]

        # Добавляем последнюю часть
        if current_rows:
            messages.append(current_message + "".join(current_rows) +
                            "└──────────────────────────────────────────────────────┴─────────┴─────────┴─────────┘\n</pre>")

        return messages

    def update_stats(self, anomalies: list):
        with self._stats_lock:
            self.logger.debug(f"Добавлено {len(anomalies)} аномалий в статистику")
            for anomaly in anomalies:
                projection_name = anomaly['key'].split(":")[0]
                stats = self.daily_stats[projection_name]
                stats["per_hour"] += 1
                stats["per_day"] += 1

    def send_report(self, current_time):
            has_data = any(
                stats["per_hour"] > 0 or stats["per_day"] > 0
                for stats in self.daily_stats.values()
            )
            if has_data:
                current_day = datetime.fromtimestamp(current_time).date()
                if self.current_day is None:
                    self.current_day = current_day

                # Сбрасываем per_day
                if self.current_day != current_day:
                    for value in self.daily_stats.values():
                        self.current_day = current_day
                        value["last_day"] = value["per_day"]
                        value["per_day"] = 0

                messages = self.create_stats_message(datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M'))
                for msg in messages:
                    self.logger.debug(msg)
                    self.notifaer.notify_chat(msg)

                # Сбрасываем per_hour
                for value in self.daily_stats.values():
                    value["per_hour"] = 0


