# telegram_bot.py — ФИНАЛЬНЫЙ, НЕПАДАЮЩИЙ, С ЛОГАМИ С ПЕРВОЙ СЕКУНДЫ (2025)

import logging
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import TelegramError
from config import Config
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor

# ═══════════════════════════════════════════════════════════════
# ВАЖНО: ПРИНУДИТЕЛЬНО ВКЛЮЧАЕМ ЛОГИРОВАНИЕ В ЭТОМ МОДУЛЕ
# ═══════════════════════════════════════════════════════════════
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Если main.py ещё не успел настроить хендлеры — делаем это сами
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# ← ЭТА СТРОКА ДОЛЖНА ПОЯВИТЬСЯ СРАЗУ ПРИ ЗАПУСКЕ!
logger.info("telegram_bot.py ЗАГРУЖЕН — ЛОГИ РАБОТАЮТ НА 100%!")
# ═══════════════════════════════════════════════════════════════


class UltimateTelegramBot:
    def __init__(self):
        logger.info("Инициализация UltimateTelegramBot...")
        self.config = Config()
        self.sheets = GoogleSheetsService()
        self.processor = DataProcessor()
        # Для фонового сервиса (main.py) сразу создаём Bot,
        # чтобы можно было отправлять сообщения без Application/PTB.
        if self.config.BOT_TOKEN:
            self.bot: Bot = Bot(self.config.BOT_TOKEN)
            logger.info("Bot-инстанс создан напрямую из BOT_TOKEN (для фонового сервиса)")
        else:
            self.bot: Bot = None
            logger.error("BOT_TOKEN не задан — отправка сообщений в Telegram невозможна")
        logger.info("UltimateTelegramBot успешно инициализирован")

    def set_bot(self, bot: Bot):
        self.bot = bot
        logger.info("Бот-инстанс установлен (set_bot)")

    async def send_message(self, text: str, chat_id: str = None) -> bool:
        if not self.bot:
            logger.error("Попытка отправки сообщения, но self.bot = None!")
            return False

        try:
            await self.bot.send_message(
                chat_id=chat_id or self.config.CHAT_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"Сообщение успешно отправлено в чат {chat_id or self.config.CHAT_ID}")
            return True
        except TelegramError as e:
            logger.error(f"TelegramError при отправке: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Неизвестная ошибка при отправке сообщения: {e}", exc_info=True)
            return False

    def format_detailed_report(self, report: dict) -> str:
        try:
            cheese_data = report.get('cheese_data', {})
            total = report.get('total_sales', 0)
            eff = report.get('efficiency', 0)

            lines = [
                "НОВЫЙ ОТЧЁТ ДЕГУСТАЦИИ",
                "",
                f"Промоутер: <b>{report.get('employee', '—')}</b>",
                f"Город: <b>{report.get('city', '—')}</b>",
                f"Сеть: <b>{report.get('network', '—')}</b>",
                f"Адрес: <b>{report.get('normalized_address', '—')}</b>",
                f"Дата: <b>{report.get('date', '—')}</b>",
                "",
                f"Посетителей: <b>{report.get('visitors', 0)}</b>",
                f"Продано всего: <b>{total} шт.</b>",
                f"Эффективность: <b>{eff}%</b>",
                "",
                "По сортам:",
            ]
            for cheese in self.config.CHEESE_TYPES:
                data = cheese_data.get(cheese, {'sold': 0})
                lines.append(f"   • {cheese}: <b>{data['sold']} шт.</b> (начало: {data.get('start', 0)} → конец: {data.get('end', 0)})")

            result = "\n".join(lines)
            logger.info(f"Сформирован детальный отчёт: {report['employee']} | {total} шт.")
            return result

        except Exception as e:
            logger.error(f"ОШИБКА форматирования детального отчёта: {e}", exc_info=True)
            return "Ошибка при формировании детального отчёта."

    def format_summary_report(self, summary: dict) -> str:
        try:
            lines = [
                "СВОДНЫЙ ОТЧЁТ ЗА ДЕНЬ",
                "",
                f"Магазинов отчиталось: <b>{summary.get('total_stores', 0)}</b>",
                f"Ожидалось: <b>{summary.get('expected_stores', 0)}</b>",
                f"Продано всего: <b>{summary.get('total_sales', 0):,} шт.</b>",
                f"Средняя эффективность: <b>{summary.get('average_efficiency', 0):.1f}%</b>",
                "",
                f"Лучший город: <b>{summary.get('best_city', '—')} — {summary.get('best_city_sales', 0)} шт.</b>",
                f"Лучшая сеть: <b>{summary.get('best_network', '—')} — {summary.get('best_network_sales', 0)} шт.</b>",
                f"Лучший сотрудник: <b>{summary.get('best_employee', '—')} — {summary.get('best_employee_sales', 0)} шт.</b>",
            ]

            missing = summary.get('missing_reports', 0)
            if missing <= 0:
                lines.append("\nВСЁ СДАНО! ОТЛИЧНАЯ РАБОТА!")
            else:
                lines.append(f"\nЖдём ещё <b>{missing}</b> отчётов...")

            result = "\n".join(lines)
            logger.info(f"Сформирован сводный отчёт: {summary.get('total_stores')}/{summary.get('expected_stores')} магазинов")
            return result

        except Exception as e:
            logger.error(f"ОШИБКА форматирования сводного отчёта: {e}", exc_info=True)
            return "Ошибка при формировании сводного отчёта."

    # ===================== КОМАНДЫ =====================
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Команда /start от пользователя {update.effective_user.id}")
        await update.message.reply_text(
            "Бот дегустаций сыра работает!\n"
            "Автоматические отчёты приходят сами.\n\n"
            "Доступные команды:\n"
            "/menu — открыть меню статистики",
            disable_web_page_preview=True
        )

    async def menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Команда /menu от пользователя {update.effective_user.id}")
        keyboard = [
            [InlineKeyboardButton("По дате", callback_data="stats_by_date")],
            [InlineKeyboardButton("За всё время", callback_data="stats_all_time")],
            [InlineKeyboardButton("Текущий день", callback_data="stats_today")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Выберите тип статистики:", reply_markup=reply_markup)

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data

        logger.info(f"Нажата кнопка: {data} (от {update.effective_user.id})")

        if data == "stats_today":
            text = "Статистика за сегодня — в разработке..."
        elif data == "stats_by_date":
            text = "Выбор даты — в разработке..."
        elif data == "stats_all_time":
            text = "Общая статистика — в разработке..."
        else:
            text = f"Неизвестная кнопка: {data}"

        try:
            await query.edit_message_text(text=text)
        except Exception as e:
            logger.error(f"Ошибка при редактировании сообщения: {e}")

    # ===================== ЗАПУСК ДЛЯ ТЕСТА =====================
    def run_polling(self):
        logger.info("Запуск бота в polling-режиме...")
        app = Application.builder().token(self.config.BOT_TOKEN).build()

        async def post_init(application: Application):
            self.set_bot(application.bot)
            logger.info("Бот успешно запущен и готов к работе!")

        app.post_init = post_init
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("menu", self.menu))
        app.add_handler(CallbackQueryHandler(self.button))

        print("=" * 60)
        print("БОТ ЗАПУЩЕН! Напиши /start или /menu в Telegram")
        print("=" * 60)

        app.run_polling(drop_pending_updates=True)


# Для быстрого теста: python telegram_bot.py
if __name__ == "__main__":
    UltimateTelegramBot().run_polling()