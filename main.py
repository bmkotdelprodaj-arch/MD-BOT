# main.py — ФИНАЛЬНЫЙ, 100% РАБОЧИЙ, С ЛОГАМИ С ПЕРВОЙ СЕКУНДЫ

import logging
import sys
import os
from datetime import datetime, timedelta
import time
import schedule
import asyncio
import pandas as pd

# ═══════════════════════════════════════════════════════════════
# ВАЖНО: НАСТРОЙКА ЛОГИРОВАНИЯ ДО ВСЕХ ИМПОРТОВ!
# ═══════════════════════════════════════════════════════════════
os.makedirs("logs", exist_ok=True)  # создаём папку для логов

logging.basicConfig(
    level=logging.INFO,  # меняй на DEBUG, если хочешь ВСЁ
    format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler("logs/degustation_analyzer.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)  # ← ГАРАНТИРОВАННО в консоль
    ]
)

# Принудительно включаем логи для всех наших модулей
for name in logging.root.manager.loggerDict.keys():
    if name.startswith(('data_processor', 'telegram_bot', 'google_sheets', 'config')):
        logging.getLogger(name).setLevel(logging.INFO)

# Тест — ДОЛЖЕН появиться сразу при запуске!
logging.info("=" * 70)
logging.info("ЛОГИРОВАНИЕ УСПЕШНО ЗАПУЩЕНО — ЭТА СТРОКА ДОЛЖНА БЫТЬ ПЕРВОЙ!")
logging.info("Если ты видишь это — значит логи работают на 100%")
logging.info("=" * 70)

# ═══════════════════════════════════════════════════════════════
# ТЕПЕРЬ можно импортировать всё остальное
# ═══════════════════════════════════════════════════════════════
from config import Config
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor
from telegram_bot import UltimateTelegramBot

logger = logging.getLogger('DegustationAnalyzer')


class DegustationAnalyzer:
    def __init__(self):
        logger.info("Инициализация DegustationAnalyzer...")
        self.config = Config()
        self.sheets_service = GoogleSheetsService()
        self.data_processor = DataProcessor()
        self.telegram_bot = UltimateTelegramBot()
        self.last_check_time = datetime.now() - timedelta(days=1)
        self.daily_reports = []  # сюда собираем отчёты за день

    def check_for_new_reports(self):
        try:
            logger.info("Запуск проверки новых отчётов...")
            morning_df = self.sheets_service.get_new_records(
                self.config.MORNING_SHEET_ID, self.last_check_time
            )
            evening_df = self.sheets_service.get_new_records(
                self.config.EVENING_SHEET_ID, self.last_check_time
            )

            if morning_df.empty and evening_df.empty:
                logger.info("Новых записей не найдено")
                self.last_check_time = datetime.now()
                return

            logger.info(f"Найдено новых: утро={len(morning_df)}, вечер={len(evening_df)}")

            # Берём ВСЁ утро (для сопоставления) + новые вечерние записи
            full_morning_df = self.sheets_service.get_sheet_data(
                self.config.MORNING_SHEET_ID,
                self.config.MORNING_SHEET_NAME
            )
            new_reports = self.data_processor.process_daily_reports(full_morning_df, evening_df)

            logger.info(f"Успешно сопоставлено новых пар: {len(new_reports)}")

            for report in new_reports:
                # Проверяем, не отправляли ли уже этот отчёт
                report_key = f"{report['date']}_{report['city']}_{report['employee']}_{report['normalized_address']}"
                if any(r.get('key') == report_key for r in self.daily_reports):
                    continue

                report['key'] = report_key
                self.daily_reports.append(report)

                msg = self.telegram_bot.format_detailed_report(report)
                success = asyncio.run(self.telegram_bot.send_message(msg))
                if success:
                    logger.info(f"Отчёт отправлен: {report['city']} | {report['employee']} | {report['total_sales']} шт.")
                else:
                    logger.error(f"НЕ УДАЛОСЬ отправить отчёт: {report['city']} | {report['employee']}")

            self.last_check_time = datetime.now()

        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в check_for_new_reports: {e}", exc_info=True)

    def generate_daily_summary(self):
        try:
            today = datetime.now().date()
            logger.info(f"Генерация сводного отчёта за {today}")

            full_morning = self.sheets_service.get_sheet_data(
                self.config.MORNING_SHEET_ID,
                self.config.MORNING_SHEET_NAME
            )
            expected = len(full_morning[
                pd.to_datetime(full_morning[self.config.MORNING_COLUMNS['date']]).dt.date == today
            ])
            actual = len(self.daily_reports)

            if expected == 0:
                logger.info("Ожидаемых отчётов нет — пропускаем сводку")
                return

            summary = self.data_processor.generate_summary_report(self.daily_reports, expected, actual)
            if summary:
                msg = self.telegram_bot.format_summary_report(summary)
                asyncio.run(self.telegram_bot.send_message(msg))
                logger.info(f"Сводный отчёт отправлен: {actual}/{expected} магазинов")
            else:
                logger.warning("Сводка пустая — не отправляем")

            # Очищаем на следующий день
            self.daily_reports = []

        except Exception as e:
            logger.error(f"ОШИБКА при генерации сводного отчёта: {e}", exc_info=True)

    def run_scheduler(self):
        schedule.every(self.config.CHECK_INTERVAL).minutes.do(self.check_for_new_reports)
        schedule.every().day.at(self.config.END_OF_DAY_TIME).do(self.generate_daily_summary)

        logger.info("СИСТЕМА ЗАПУЩЕНА!")
        logger.info(f"Проверка новых отчётов: каждые {self.config.CHECK_INTERVAL} минут")
        logger.info(f"Сводный отчёт: ежедневно в {self.config.END_OF_DAY_TIME}")

        while True:
            schedule.run_pending()
            time.sleep(60)


if __name__ == "__main__":
    analyzer = DegustationAnalyzer()
    analyzer.run_scheduler()