# telegram_ptb_bot.py — ФИНАЛЬНАЯ ВЕРСИЯ С ЛОГАМИ, БЕЗ БАГОВ, 2025

import logging
import asyncio
from datetime import datetime, date, timedelta
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from config import Config
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor

# ═══════════════════════════════════════════════════════════════
# ПРИНУДИТЕЛЬНО ВКЛЮЧАЕМ ЛОГИ С САМОЙ ПЕРВОЙ СТРОКИ!
# ═══════════════════════════════════════════════════════════════
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Если main.py ещё не успел — включаем сами
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.info("telegram_ptb_bot.py ЗАГРУЖЕН — ЛОГИ РАБОТАЮТ НА 100%!")
# ═══════════════════════════════════════════════════════════════


class TelegramPTBBot:
    def __init__(self):
        logger.info("Инициализация TelegramPTBBot...")
        self.config = Config()
        self.sheets_service = GoogleSheetsService()
        self.data_processor = DataProcessor()
        logger.info("TelegramPTBBot успешно инициализирован")

    # ===================== УДАЛЕНИЕ СТАРЫХ UI =====================
    async def cleanup_previous_ui(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Раньше удаляли старые UI-сообщения, сейчас навигация сделана через edit_message_text,
        поэтому здесь ничего не делаем, чтобы не трогать историю чата.
        Оставлено для обратной совместимости.
        """
        return

    # ===================== ОТПРАВКА СООБЩЕНИЙ =====================
    async def send_result_message(self, chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE,
                                reply_markup=None):
        msg = await context.bot.send_message(
            chat_id=chat_id, text=text, parse_mode="HTML",
            reply_markup=reply_markup, disable_web_page_preview=True
        )
        context.chat_data.setdefault('keep_messages', set()).add(msg.message_id)
        logger.info(f"Отправлено РЕЗУЛЬТАТИВНОЕ сообщение {msg.message_id}")
        return msg

    async def send_ui_message(self, chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE,
                            reply_markup=None):
        msg = await context.bot.send_message(
            chat_id=chat_id, text=text, parse_mode="HTML",
            reply_markup=reply_markup, disable_web_page_preview=True
        )
        context.chat_data.setdefault('last_ui_message_ids', []).append(msg.message_id)
        logger.info(f"Отправлено UI-сообщение {msg.message_id}")
        return msg

    # ===================== /start =====================
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        logger.info(f"Команда /start от {update.effective_user.full_name} ({update.effective_user.id})")

        keyboard = [
            [InlineKeyboardButton("История по магазину", callback_data="history_store")],
            [InlineKeyboardButton("История по городу", callback_data="history_city")],
            [InlineKeyboardButton("История по дате", callback_data="history_date")],
            [InlineKeyboardButton("История по сети", callback_data="history_network")],
            [InlineKeyboardButton("Статистика за всё время", callback_data="all_time_menu")],
        ]

        text = (
            "<b>Бот для анализа дегустаций сыра</b>\n\n"
            "Выберите нужный тип отчёта:"
        )
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    # ===================== CALLBACK =====================
    async def callback_query_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        chat_id = query.message.chat_id
        data = query.data

        logger.info(f"Callback: {data} от пользователя {update.effective_user.id}")

        # Сохраняем тип запроса
        if data in [
            "history_store",
            "history_city",
            "history_date",
            "history_network",
            "all_time_city",
            "all_time_network",
            "all_time_overall",
        ]:
            context.user_data["request_type"] = data

        if data == "history_store":
            await self.start_store_flow(chat_id, context, query)
        elif data == "history_city":
            await self.start_city_flow(chat_id, context, query)
        elif data == "history_date":
            await self.start_date_flow(chat_id, context, query)
        elif data == "history_network":
            await self.start_network_flow(chat_id, context, query)
        elif data == "all_time_menu":
            await self.start_all_time_menu(chat_id, context)
        elif data == "all_time_city":
            await self.show_all_time_city_stats(chat_id, context)
        elif data == "all_time_network":
            await self.show_all_time_network_stats(chat_id, context)
        elif data == "all_time_overall":
            await self.show_all_time_overall_stats(chat_id, context)
        elif data.startswith("date_"):
            await self.handle_date_selection(chat_id, data[5:], context, query)
        elif data.startswith("city_"):
            await self.handle_city_selection(chat_id, data[5:], context, query)
        elif data.startswith("network_"):
            await self.handle_network_selection(chat_id, data[8:], context, query)
        elif data.startswith("address_"):
            await self.handle_address_selection(chat_id, data[8:], context)

    # ===================== ПОТОКИ =====================
    async def start_store_flow(self, chat_id, context, query=None):
        dates = await self.get_available_dates()
        if not dates:
            return await self.send_result_message(chat_id, "Нет данных", context)

        keyboard = [[InlineKeyboardButton(d.strftime("%d.%m.%Y"), callback_data=f"date_{d.strftime('%Y-%m-%d')}")] 
                   for d in dates[-10:]]
        text = "Выберите дату для поиска по магазину:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_city_flow(self, chat_id, context, query=None):
        dates = await self.get_available_dates()
        if not dates:
            return await self.send_result_message(chat_id, "Нет данных", context)

        keyboard = [[InlineKeyboardButton(d.strftime("%d.%m.%Y"), callback_data=f"date_{d.strftime('%Y-%m-%d')}")] 
                   for d in dates[-10:]]
        text = "Выберите дату для статистики по городам:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_date_flow(self, chat_id, context, query=None):
        keyboard = []
        for i in range(10):
            d = (datetime.now().date() - timedelta(days=i))
            keyboard.append([InlineKeyboardButton(d.strftime("%d.%m.%Y"), callback_data=f"date_{d.strftime('%Y-%m-%d')}")])
        text = "Выберите дату для общей статистики:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_network_flow(self, chat_id, context, query=None):
        dates = await self.get_available_dates()
        if not dates:
            return await self.send_result_message(chat_id, "Нет данных", context)

        keyboard = [[InlineKeyboardButton(d.strftime("%d.%m.%Y"), callback_data=f"date_{d.strftime('%Y-%m-%d')}")] 
                   for d in dates[-10:]]
        text = "Выберите дату для статистики по сетям:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_all_time_menu(self, chat_id, context, query=None):
        keyboard = [
            [InlineKeyboardButton("По городам (за всё время)", callback_data="all_time_city")],
            [InlineKeyboardButton("По сетям (за всё время)", callback_data="all_time_network")],
            [InlineKeyboardButton("Общая статистика (за всё время)", callback_data="all_time_overall")],
        ]
        text = "Выберите тип статистики за всё время:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(
                chat_id,
                text,
                context,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )

    # ===================== ОБРАБОТКА ВЫБОРА =====================
    async def handle_date_selection(self, chat_id, date_str, context, query=None):
        selected_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        context.user_data["selected_date"] = selected_date
        request_type = context.user_data.get("request_type")

        if request_type == "history_store":
            context.user_data["flow"] = "store"
            await self.show_city_selection(chat_id, selected_date, context, query)
        elif request_type == "history_city":
            await self.show_city_stats(chat_id, selected_date, context)
        elif request_type == "history_date":
            await self.show_general_date_stats(chat_id, selected_date, context)
        elif request_type == "history_network":
            await self.show_network_selection(chat_id, selected_date, context, query)

    async def show_city_selection(self, chat_id, date_obj, context, query=None):
        cities = await self.get_available_cities(date_obj)
        if not cities:
            return await self.send_result_message(chat_id, f"Нет данных за {date_obj.strftime('%d.%m.%Y')}", context)

        keyboard = [[InlineKeyboardButton(city, callback_data=f"city_{city}")] for city in sorted(cities)]
        text = f"Выберите город за {date_obj.strftime('%d.%m.%Y')}:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def show_city_stats(self, chat_id, date_obj, context):
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_f = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
        evening_f = evening_df[pd.to_datetime(evening_df[self.config.EVENING_COLUMNS['date']]).dt.date == date_obj]

        reports = self.data_processor.process_daily_reports(morning_f, evening_f)
        if not reports:
            return await self.send_result_message(chat_id, f"Нет завершённых отчётов за {date_obj.strftime('%d.%m.%Y')}", context)

        city_stats = {}
        for r in reports:
            city = r['city']
            city_stats.setdefault(city, {'stores': 0, 'sales': 0, 'eff': []})
            city_stats[city]['stores'] += 1
            city_stats[city]['sales'] += r['total_sales']
            city_stats[city]['eff'].append(r['efficiency'])

        text = f"<b>Статистика по городам за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
        for city, s in sorted(city_stats.items(), key=lambda x: x[1]['sales'], reverse=True):
            avg_eff = sum(s['eff']) / len(s['eff']) if s['eff'] else 0
            text += f"<b>{city}</b>\n"
            text += f"Магазинов: {s['stores']} | Продано: {s['sales']} шт. | Эфф.: {avg_eff:.1f}%\n\n"

        await self.send_result_message(chat_id, text, context)

    async def show_general_date_stats(self, chat_id, date_obj, context):
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_f = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
        evening_f = evening_df[pd.to_datetime(evening_df[self.config.EVENING_COLUMNS['date']]).dt.date == date_obj]

        reports = self.data_processor.process_daily_reports(morning_f, evening_f)

        if not reports:
            expected = len(morning_f)
            text = f"<b>Статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
            text += f"Ожидалось отчётов: {expected}\nПолучено: 0\n"
            text += "Пока нет завершённых пар."
        else:
            total_sales = sum(r['total_sales'] for r in reports)
            avg_eff = sum(r['efficiency'] for r in reports) / len(reports)
            total_visitors = sum(r['visitors'] for r in reports)

            text = f"<b>Общая статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
            text += f"Магазинов: {len(reports)}\n"
            text += f"Продано: {total_sales} шт.\n"
            text += f"Посетителей: {total_visitors}\n"
            text += f"Средняя эффективность: {avg_eff:.1f}%\n"

        await self.send_result_message(chat_id, text, context)

    async def show_network_selection(self, chat_id, date_obj, context, query=None):
        networks = await self.get_available_networks(date_obj)
        if not networks:
            return await self.send_result_message(chat_id, "Нет данных за эту дату", context)

        keyboard = [[InlineKeyboardButton(net, callback_data=f"network_{net}")] for net in sorted(networks)]
        text = f"Выберите сеть за {date_obj.strftime('%d.%m.%Y')}:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_city_selection(self, chat_id, city, context, query=None):
        date_obj = context.user_data.get("selected_date")
        if not date_obj:
            return await self.send_result_message(chat_id, "Ошибка: дата не выбрана", context)

        context.user_data["selected_city"] = city
        networks = await self.get_available_networks_in_city(date_obj, city)

        if not networks:
            return await self.send_result_message(chat_id, f"Нет данных по городу {city}", context)

        keyboard = [[InlineKeyboardButton(net, callback_data=f"network_{net}")] for net in sorted(networks)]
        text = f"Выберите сеть в {city}:"
        if query:
            await query.edit_message_text(
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            await self.send_ui_message(chat_id, text, context,
                                       reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_network_selection(self, chat_id, network, context, query=None):
        date_obj = context.user_data.get("selected_date")
        city = context.user_data.get("selected_city")

        if city:  # По магазину
            addresses = await self.get_available_addresses(date_obj, city, network)
            if not addresses:
                return await self.send_result_message(chat_id, "Нет адресов", context)

            keyboard = [[InlineKeyboardButton(a[:40] + ("..." if len(a)>40 else ""), 
                                            callback_data=f"address_{a}")] for a in addresses]
            text = f"Выберите магазин ({network}):"
            if query:
                await query.edit_message_text(
                    text=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            else:
                await self.send_ui_message(chat_id, text, context,
                                           reply_markup=InlineKeyboardMarkup(keyboard))
        else:  # По сети в целом
            await self.show_network_stats(chat_id, date_obj, network, context)

    async def handle_address_selection(self, chat_id, address, context):
        date_obj = context.user_data.get("selected_date")
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_f = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
        evening_f = evening_df[pd.to_datetime(evening_df[self.config.EVENING_COLUMNS['date']]).dt.date == date_obj]

        report = None
        for r in self.data_processor.process_daily_reports(morning_f, evening_f):
            if self.data_processor.normalizer.normalize(address) in self.data_processor.normalizer.normalize(r.get('normalized_address', '')):
                report = r
                break

        if not report:
            return await self.send_result_message(chat_id, "Отчёт не найден", context)

        text = self.format_detailed_report(report)
        await self.send_result_message(chat_id, text, context)

    async def show_network_stats(self, chat_id, date_obj, network, context):
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_f = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
        evening_f = evening_df[pd.to_datetime(evening_df[self.config.EVENING_COLUMNS['date']]).dt.date == date_obj]

        reports = [r for r in self.data_processor.process_daily_reports(morning_f, evening_f) if r['network'] == network]
        if not reports:
            return await self.send_result_message(chat_id, f"Нет данных по сети {network}", context)

        total_sales = sum(r['total_sales'] for r in reports)
        avg_eff = sum(r['efficiency'] for r in reports) / len(reports)
        total_vis = sum(r['visitors'] for r in reports)

        cheese_totals = {}
        for r in reports:
            for ch, data in r['cheese_data'].items():
                cheese_totals[ch] = cheese_totals.get(ch, 0) + data['sold']

        text = f"<b>Сеть «{network}» за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
        text += f"Магазинов: {len(reports)}\n"
        text += f"Продано: {total_sales} шт.\n"
        text += f"Посетителей: {total_vis}\n"
        text += f"Эффективность: {avg_eff:.1f}%\n\n"
        text += "<b>По сырам:</b>\n"
        for ch, sold in cheese_totals.items():
            text += f"• {ch}: {sold} шт.\n"

        await self.send_result_message(chat_id, text, context)

    # ===================== СТАТИСТИКА ЗА ВСЁ ВРЕМЯ =====================
    async def _load_all_reports(self):
        """Загружает все пары утро+вечер за всё время одним вызовом."""
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)
        reports = self.data_processor.process_daily_reports(morning_df, evening_df)
        logger.info(f"Всего сопоставленных отчётов за всё время: {len(reports)}")
        return reports

    async def show_all_time_city_stats(self, chat_id, context):
        reports = await self._load_all_reports()
        if not reports:
            return await self.send_result_message(chat_id, "Нет завершённых отчётов за всё время", context)

        city_stats = {}
        for r in reports:
            city = r['city']
            city_stats.setdefault(city, {'stores': 0, 'sales': 0, 'eff': []})
            city_stats[city]['stores'] += 1
            city_stats[city]['sales'] += r['total_sales']
            city_stats[city]['eff'].append(r['efficiency'])

        text = "<b>Статистика по городам за всё время</b>\n\n"
        for city, s in sorted(city_stats.items(), key=lambda x: x[1]['sales'], reverse=True):
            avg_eff = sum(s['eff']) / len(s['eff']) if s['eff'] else 0
            text += f"<b>{city}</b>\n"
            text += f"Магазинов: {s['stores']} | Продано: {s['sales']} шт. | Эфф.: {avg_eff:.1f}%\n\n"

        await self.send_result_message(chat_id, text, context)

    async def show_all_time_network_stats(self, chat_id, context):
        reports = await self._load_all_reports()
        if not reports:
            return await self.send_result_message(chat_id, "Нет завершённых отчётов за всё время", context)

        net_stats = {}
        cheese_totals = {}
        total_vis = 0
        for r in reports:
            net = r['network']
            net_stats.setdefault(net, {'stores': 0, 'sales': 0, 'eff': []})
            net_stats[net]['stores'] += 1
            net_stats[net]['sales'] += r['total_sales']
            net_stats[net]['eff'].append(r['efficiency'])
            total_vis += r['visitors']

            for ch, data in r['cheese_data'].items():
                cheese_totals[ch] = cheese_totals.get(ch, 0) + data['sold']

        text = "<b>Статистика по сетям за всё время</b>\n\n"
        for net, s in sorted(net_stats.items(), key=lambda x: x[1]['sales'], reverse=True):
            avg_eff = sum(s['eff']) / len(s['eff']) if s['eff'] else 0
            text += f"<b>{net}</b>\n"
            text += f"Магазинов: {s['stores']} | Продано: {s['sales']} шт. | Эфф.: {avg_eff:.1f}%\n\n"

        text += "<b>По сырам за всё время:</b>\n"
        for ch, sold in cheese_totals.items():
            text += f"• {ch}: {sold} шт.\n"

        text += f"\nВсего посетителей по всем дегустациям: {total_vis}\n"

        await self.send_result_message(chat_id, text, context)

    async def show_all_time_overall_stats(self, chat_id, context):
        reports = await self._load_all_reports()
        if not reports:
            return await self.send_result_message(chat_id, "Нет завершённых отчётов за всё время", context)

        total_stores = len(reports)
        total_sales = sum(r['total_sales'] for r in reports)
        total_visitors = sum(r['visitors'] for r in reports)
        avg_eff = sum(r['efficiency'] for r in reports) / len(reports)

        text = "<b>Общая статистика за всё время</b>\n\n"
        text += f"Магазинов (дней-дегустаций): {total_stores}\n"
        text += f"Продано всего: {total_sales} шт.\n"
        text += f"Посетителей всего: {total_visitors}\n"
        text += f"Средняя эффективность: {avg_eff:.1f}%\n"

        await self.send_result_message(chat_id, text, context)

    # ===================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =====================
    async def get_available_dates(self):
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            dates = set()
            if not morning_df.empty:
                dates.update(pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date.unique())
            return sorted(dates, reverse=True)
        except Exception as e:
            logger.error(f"Ошибка получения дат: {e}", exc_info=True)
            return []

    async def get_available_cities(self, date_obj):
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            filtered = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
            return sorted(filtered[self.config.MORNING_COLUMNS['city']].dropna().unique())
        except Exception as e:
            logger.error(f"Ошибка получения городов: {e}")
            return []

    async def get_available_networks(self, date_obj):
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            filtered = morning_df[pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj]
            return sorted(filtered[self.config.MORNING_COLUMNS['network_name']].dropna().unique())
        except Exception as e:
            logger.error(f"Ошибка получения сетей: {e}")
            return []

    async def get_available_networks_in_city(self, date_obj, city):
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            filtered = morning_df[
                (pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj) &
                (morning_df[self.config.MORNING_COLUMNS['city']] == city)
            ]
            return sorted(filtered[self.config.MORNING_COLUMNS['network_name']].dropna().unique())
        except Exception as e:
            logger.error(f"Ошибка получения сетей в городе: {e}")
            return []

    async def get_available_addresses(self, date_obj, city, network):
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            filtered = morning_df[
                (pd.to_datetime(morning_df[self.config.MORNING_COLUMNS['date']]).dt.date == date_obj) &
                (morning_df[self.config.MORNING_COLUMNS['city']] == city) &
                (morning_df[self.config.MORNING_COLUMNS['network_name']] == network)
            ]
            return list(filtered[self.config.MORNING_COLUMNS['address']].dropna().unique())
        except Exception as e:
            logger.error(f"Ошибка получения адресов: {e}")
            return []

    def format_detailed_report(self, report):
        lines = [
            "<b>ДЕТАЛЬНЫЙ ОТЧЁТ</b>",
            f"<b>Дата:</b> {report['date']}",
            f"<b>Город:</b> {report['city']}",
            f"<b>Сеть:</b> {report['network']}",
            f"<b>Сотрудник:</b> {report['employee']}",
            f"<b>Адрес:</b> {report.get('normalized_address', '—')}",
            "",
            f"<b>Посетителей:</b> {report['visitors']}",
            f"<b>Всего продано:</b> {report['total_sales']} шт.",
            f"<b>Эффективность:</b> {report['efficiency']}%",
            "",
            "<b>По сортам:</b>"
        ]
        for cheese, data in report['cheese_data'].items():
            lines.append(f"• <b>{cheese}</b>: {data['start']} → {data['end']} (продано {data['sold']})")

        return "\n".join(lines)


# ===================== ЗАПУСК =====================
def create_application():
    bot = TelegramPTBBot()
    app = Application.builder().token(bot.config.BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", bot.start_command))
    app.add_handler(CallbackQueryHandler(bot.callback_query_handler))
    return app


if __name__ == "__main__":
    app = create_application()
    logger.info("Запуск TelegramPTBBot в polling-режиме...")
    app.run_polling(drop_pending_updates=True)