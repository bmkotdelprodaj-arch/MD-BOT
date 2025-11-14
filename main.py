import time
import logging
from datetime import datetime, timedelta
import schedule
from config import Config
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor
from telegram_bot import TelegramBot

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('degustation_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DegustationAnalyzer')

class DegustationAnalyzer:
    def __init__(self):
        self.config = Config()
        self.sheets_service = GoogleSheetsService()
        self.data_processor = DataProcessor()
        self.telegram_bot = TelegramBot()
        self.last_check_time = datetime.now() - timedelta(days=1)
        self.daily_reports = []  # Хранение отчетов за день
    
    def check_for_new_reports(self):
        """Проверяет наличие новых отчетов и обрабатывает их"""
        try:
            logger.info("Проверка новых отчетов...")
            
            # Получаем новые записи
            morning_df = self.sheets_service.get_new_records(
                self.config.MORNING_SHEET_ID, self.last_check_time
            )
            
            evening_df = self.sheets_service.get_new_records(
                self.config.EVENING_SHEET_ID, self.last_check_time
            )
            
            if not morning_df.empty or not evening_df.empty:
                logger.info(f"Найдено новых записей: утро={len(morning_df)}, вечер={len(evening_df)}")

                # Получаем полные данные для сопоставления
                full_morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)

                # Обрабатываем только новые вечерние отчеты
                new_reports = self.data_processor.process_daily_reports(
                    full_morning_df, evening_df  # evening_df - только новые
                )
                
                # Отправляем детальные отчеты
                for report in new_reports:
                    self.daily_reports.append(report)
                    message = self.telegram_bot.format_detailed_report(report)
                    success = self.telegram_bot.send_message(message)
                    
                    if success:
                        logger.info(f"Отправлен отчет: {report['city']} - {report['employee']}")
                    else:
                        logger.error(f"Ошибка отправки отчета: {report['city']} - {report['employee']}")
            
            # Обновляем время последней проверки
            self.last_check_time = datetime.now()
            
        except Exception as e:
            logger.error(f"Ошибка при проверке отчетов: {e}")
    
    def generate_daily_summary(self):
        """Генерирует и отправляет сводный отчет за день"""
        try:
            # Получаем данные за вчерашний день
            yesterday = (datetime.now() - timedelta(days=1)).date()

            # Получаем полные данные за вчера
            full_morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            full_evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            # Вычисляем ожидаемое количество отчетов (из утренних анкет)
            expected_reports = self.data_processor.get_expected_reports_for_day(
                full_morning_df, full_evening_df, yesterday
            )

            actual_reports = len(self.daily_reports)

            if expected_reports == 0:
                logger.info("Нет утренних отчетов за вчерашний день")
                return

            logger.info(f"Ожидаемые отчеты: {expected_reports}, Получено: {actual_reports}")

            summary = self.data_processor.generate_summary_report(
                self.daily_reports, expected_reports, actual_reports
            )

            if summary:
                message = self.telegram_bot.format_summary_report(summary)
                success = self.telegram_bot.send_message(message)

                if success:
                    logger.info("Сводный отчет успешно отправлен")
                    # Очищаем данные за день
                    self.daily_reports = []
                    self.data_processor.processed_pairs = set()
                else:
                    logger.error("Ошибка отправки сводного отчета")
            else:
                logger.info("Нет данных для сводного отчета")

        except Exception as e:
            logger.error(f"Ошибка при генерации сводного отчета: {e}")
    
    def run_scheduler(self):
        """Запускает планировщик задач"""
        # Проверка новых отчетов каждые N минут
        schedule.every(self.config.CHECK_INTERVAL).minutes.do(self.check_for_new_reports)
        
        # Сводный отчет в конце дня
        schedule.every().day.at(self.config.END_OF_DAY_TIME).do(self.generate_daily_summary)
        
        logger.info(f"Система запущена. Проверка каждые {self.config.CHECK_INTERVAL} минут. Сводный отчет в {self.config.END_OF_DAY_TIME}")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту

if __name__ == "__main__":
    # Для локального тестирования - запускаем планировщик
    # Для продакшена на Render - используем webhook
    import os

    if os.getenv('RENDER'):  # Если запущено на Render
        from telegram_webhook import app
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
    else:  # Локальный запуск с планировщиком
        analyzer = DegustationAnalyzer()
        analyzer.run_scheduler()
