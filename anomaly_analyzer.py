import logging
import os
import signal
import csv
from collections import defaultdict
from datetime import datetime
from time import time
from typing import Dict
from config_parser import ConfigParser, setup_logging_from_config
from anomaly_notifaer import AnomalyNotifier
from daily_statistic import DailyStats
from partition_manager import PartitionManager
from queue import Queue, Empty

class ProjectionAnomalyAnalyzer:
    def __init__(self, config_path: str = "config.yaml"):
        # Загрузка конфигурации
        config = ConfigParser(config_path)
        if not config.validate_config():
            raise ValueError("❌ Невалидная конфигурация")

        # Настройка логирования
        setup_logging_from_config(config)
        self.logger = logging.getLogger(__name__)

        self.save_total_statistic = config.get_save_total_statistic()

        self.notifaer = AnomalyNotifier(config.get_notifaer_config())

        self.daily_stats = DailyStats(self.notifaer)

        self.partition_manager = PartitionManager(config.get_kafka_config(), self.daily_stats)

        self.message_queue = Queue(maxsize=50000)

        self.analyze_is_running = False

        self.anomaly_statistic: Dict[str, int] = defaultdict(int)

        self.logger.info(f"✅ Анализатор инициализирован.")

    def _handle_signal(self, signum, frame):
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGHUP"
        self.logger.debug(f"📢 Получен сигнал {signal_name}, запуск shutdown.")
        self.logger.info(f"📢 Остановка сервиса")
        self.analyze_is_running = False

    def start_analysis_loop(self):
        self.analyze_is_running = True

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGHUP, self._handle_signal)

        self.partition_manager.start_all(self.message_queue)

        last_log_time = time()
        #last_check_time = time()
        while self.analyze_is_running:
            try:
                queue_size = self.message_queue.qsize()

                if self.partition_manager.check_all_ready():
                    min_sec = min([ts for ts in self.partition_manager.last_msg_seconds if ts is not None])
                    self.daily_stats.send_report(min_sec // 1000)

                current_time = time()
                delta = current_time - last_log_time
                if delta >= 20:
                    last_log_time = current_time
                    self.partition_manager.log_statistics(queue_size, delta)

                #if current_time - last_check_time >= 3600:
                    #last_check_time = current_time
                    #start_date = datetime.fromtimestamp(self.partition_manager.start_time).date()
                    #current_date = datetime.today().date()
                    #if start_date != current_date:
                        #self.partition_manager.start_time = time()
                        #self.partition_manager.processed_messages = 0
                        #self.partition_manager.filtered_messages = 0
                    #self.logger.info(f"📅 Смена дня: {current_date}")

                partition, anomalies = self.message_queue.get_nowait()

                if anomalies:
                    for anomaly in anomalies:
                        if self.save_total_statistic:
                            key = anomaly['key'].split(':')[0]
                            self.anomaly_statistic[key] += 1
                    #self.save_keys_to_csv(anomalies)
            except Empty:
                if not self.analyze_is_running:  # Проверка флага
                    break
                continue
            except Exception as e:
                self.logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
                break

        self.shutdown()

    def _save_keys_to_csv(self, anomalies: list):
        try:
            # Единый файл для всех партиций
            filename = f"anomaly_keys_infoservice_v3.csv"
            filepath = os.path.join("keys_storage", filename)

            # Создаем директорию если нет
            os.makedirs("keys_storage", exist_ok=True)

            # Проверяем существует ли файл
            file_exists = os.path.isfile(filepath)

            # Подготавливаем данные
            rows = []
            for anomaly in anomalies:
                key = anomaly.get('key').split(':')[1]
                timestamp_sec = anomaly.get('timestamp')
                info_service = anomaly['messages'][0]['body']['InfoService']
                code = info_service.get('dicSocialMeasureClassifierCode')
                ismigration = info_service.get('ismigrationprocesssign')

                if timestamp_sec:
                    dt = datetime.fromtimestamp(timestamp_sec)
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_time = ''

                rows.append([key, formatted_time, code, ismigration])

            # Записываем
            with open(filepath, 'a', encoding='utf-8', newline='',  buffering=65536) as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['key', 'timestamp', 'code', 'ismigration'])  # Заголовок
                writer.writerows(rows)  # Все строки за раз

            self.logger.debug(f"💾 Сохранено {len(rows)} ключей в {filepath}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения ключей: {e}")

    def _save_stats(self):
        filename = f"anomaly_stats_v1.csv"
        filepath = os.path.join("stats_storage", filename)
        file_exists = os.path.exists(filepath)

        sorted_stats = sorted(self.anomaly_statistic.items(),
                              key=lambda x: x[1],
                              reverse=True)
        # Создаем директорию если нет
        os.makedirs("stats_storage", exist_ok=True)

        # Проверяем существует ли файл
        file_exists = os.path.isfile(filepath)

        # Перезаписываем файл
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['Проекция', 'Количество'])
            writer.writerows(sorted_stats)

    def _try_end_queue(self):
        self.logger.info(f"Обрабатываю остатки очереди. Размер очереди: {self.message_queue.qsize()}")
        # Обрабатываем оставшиеся сообщения
        while not self.message_queue.empty():
            try:
                partition, anomalies = self.message_queue.get_nowait()
                if anomalies:
                    self.daily_stats.update_stats(anomalies)
                    #self._save_keys_to_csv(anomalies)
                    for anomaly in anomalies:
                        if self.save_total_statistic:
                            key = anomaly['key'].split(':')[0]
                            self.anomaly_statistic[key] += 1
            except Empty:
                break
        self.logger.info(f"Очередь обработана.")

    def shutdown(self):
        self.logger.info("🛑 Завершение работы")
        self._try_end_queue()
        self.analyze_is_running = False
        try:
            self.partition_manager.stop_all()

            self.logger.info("📊 Отправка финального отчета")
            min_sec = min([ts for ts in self.partition_manager.last_msg_seconds if ts is not None])
            self.daily_stats.send_report(min_sec // 1000)

            self.logger.info("Сохранение финальной статистики")
            if self.save_total_statistic:
                self._save_stats()
        except Exception as e:
            self.logger.error(f"Ошибка при остановке потоков: {e}")
        self.logger.info("⛔ Анализатор остановлен")