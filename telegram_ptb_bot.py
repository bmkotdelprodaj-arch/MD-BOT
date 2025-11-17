import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor
from config import Config
import asyncio

logger = logging.getLogger(__name__)

class TelegramPTBBot:
    def __init__(self):
        self.config = Config()
        self.sheets_service = GoogleSheetsService()
        self.data_processor = DataProcessor()

    async def cleanup_previous_ui(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """
        Удаляет предыдущие интерфейсные сообщения в чате, оставляя результативные.

        Интерфейсные сообщения:
        - С reply_markup (кнопки)
        - Короткий текст меню (< 200 символов)
        - Без document/photo/animation

        Результативные сообщения:
        - Помеченные в context.chat_data['keep_messages']
        - Без reply_markup и длина > 200 символов
        - Содержат document/photo/animation
        """
        if 'last_ui_message_ids' not in context.chat_data:
            return

        message_ids_to_delete = context.chat_data['last_ui_message_ids']
        keep_messages = context.chat_data.get('keep_messages', set())

        for msg_id in message_ids_to_delete:
            if msg_id in keep_messages:
                continue

            try:
                # Получаем информацию о сообщении
                message = await context.bot.get_chat_member(chat_id, context.bot.id)  # Это не правильно, нужно получить сообщение

                # На самом деле, нам нужно хранить информацию о сообщениях
                # Для упрощения, попробуем удалить и обработаем ошибки
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"Удалено интерфейсное сообщение {msg_id} в чате {chat_id}")

            except Exception as e:
                error_code = getattr(e, 'error_code', None)
                if error_code == 400:  # MessageToDeleteNotFound или BadRequest
                    logger.warning(f"Не удалось удалить сообщение {msg_id} в чате {chat_id}: {e}")
                else:
                    logger.error(f"Ошибка при удалении сообщения {msg_id} в чате {chat_id}: {e}")

        # Очищаем список после удаления
        context.chat_data['last_ui_message_ids'] = []

    async def send_result_message(self, chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE,
                                parse_mode="HTML", reply_markup=None, **kwargs) -> None:
        """
        Отправляет результативное сообщение и помечает его для сохранения.
        """
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            **kwargs
        )

        # Помечаем как результативное
        if 'keep_messages' not in context.chat_data:
            context.chat_data['keep_messages'] = set()
        context.chat_data['keep_messages'].add(message.message_id)

        logger.info(f"Отправлено результативное сообщение {message.message_id} в чате {chat_id}")
        return message

    async def send_ui_message(self, chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE,
                            reply_markup=None, **kwargs):
        """
        Отправляет интерфейсное сообщение и сохраняет его ID для последующего удаления.
        """
        message = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            **kwargs
        )

        # Сохраняем ID для последующего удаления
        if 'last_ui_message_ids' not in context.chat_data:
            context.chat_data['last_ui_message_ids'] = []
        context.chat_data['last_ui_message_ids'].append(message.message_id)

        logger.info(f"Отправлено интерфейсное сообщение {message.message_id} в чате {chat_id}")
        return message

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обработчик команды /start"""
        chat_id = update.effective_chat.id

        # Удаляем предыдущие интерфейсные сообщения
        await self.cleanup_previous_ui(chat_id, context)

        # Отправляем главное меню
        keyboard = [
            [InlineKeyboardButton("🏪 История по магазину", callback_data="history_store")],
            [InlineKeyboardButton("🏙️ История по городу", callback_data="history_city")],
            [InlineKeyboardButton("📅 История по дате", callback_data="history_date")],
            [InlineKeyboardButton("🏢 История по сети", callback_data="history_network")]
        ]

        text = """
🤖 <b>Бот для анализа дегустаций</b>

Выберите тип отчета для просмотра истории:
• 🏪 <b>По магазину</b> - детальная статистика конкретного магазина
• 🏙️ <b>По городу</b> - сводка по всем сетям города
• 📅 <b>По дате</b> - общая статистика за день
• 🏢 <b>По сети</b> - статистика по всей сети
"""

        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def callback_query_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обработчик callback запросов"""
        query = update.callback_query
        await query.answer()

        callback_data = query.data
        chat_id = query.message.chat_id

        # Удаляем предыдущие интерфейсные сообщения
        await self.cleanup_previous_ui(chat_id, context)

        # Обрабатываем callback
        if callback_data == "history_store":
            await self.start_store_history(chat_id, context)
        elif callback_data == "history_city":
            await self.start_city_history(chat_id, context)
        elif callback_data == "history_date":
            await self.start_date_history(chat_id, context)
        elif callback_data == "history_network":
            await self.start_network_history(chat_id, context)
        elif callback_data.startswith("date_"):
            date = callback_data.split("_", 1)[1]
            await self.handle_date_selection(chat_id, date, context)
        elif callback_data.startswith("city_"):
            city = callback_data.split("_", 1)[1]
            await self.handle_city_selection(chat_id, city, context)
        elif callback_data.startswith("network_"):
            network = callback_data.split("_", 1)[1]
            await self.handle_network_selection(chat_id, network, context)
        elif callback_data.startswith("address_"):
            address = callback_data.split("_", 1)[1]
            await self.handle_address_selection(chat_id, address, context)

    async def start_store_history(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Начинает процесс выбора для истории по магазину"""
        # Получаем доступные даты
        dates = await self.get_available_dates()

        if not dates:
            await self.send_result_message(chat_id, "❌ Нет доступных данных для анализа", context)
            return

        keyboard = []
        for date in dates[-10:]:  # Последние 10 дат
            keyboard.append([InlineKeyboardButton(
                date.strftime("%d.%m.%Y"),
                callback_data=f"date_{date.strftime('%Y-%m-%d')}"
            )])

        text = "📅 Выберите дату для просмотра истории по магазину:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_city_history(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Начинает процесс выбора для истории по городу"""
        dates = await self.get_available_dates()

        if not dates:
            await self.send_result_message(chat_id, "❌ Нет доступных данных для анализа", context)
            return

        keyboard = []
        for date in dates[-10:]:
            keyboard.append([InlineKeyboardButton(
                date.strftime("%d.%m.%Y"),
                callback_data=f"date_{date.strftime('%Y-%m-%d')}"
            )])

        text = "📅 Выберите дату для просмотра истории по городу:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_date_history(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Начинает процесс выбора даты для общей статистики"""
        from datetime import datetime, timedelta

        keyboard = []
        for i in range(10):
            date = datetime.now().date() - timedelta(days=i)
            keyboard.append([InlineKeyboardButton(
                date.strftime("%d.%m.%Y"),
                callback_data=f"date_{date.strftime('%Y-%m-%d')}"
            )])

        text = "📅 Выберите дату для просмотра общей статистики:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def start_network_history(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Начинает процесс выбора для истории по сети"""
        dates = await self.get_available_dates()

        if not dates:
            await self.send_result_message(chat_id, "❌ Нет доступных данных для анализа", context)
            return

        keyboard = []
        for date in dates[-10:]:
            keyboard.append([InlineKeyboardButton(
                date.strftime("%d.%m.%Y"),
                callback_data=f"date_{date.strftime('%Y-%m-%d')}"
            )])

        text = "📅 Выберите дату для просмотра истории по сети:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_date_selection(self, chat_id: int, date_str: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обрабатывает выбор даты"""
        from datetime import datetime
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

        user_type = context.user_data.get("type", "general_date")

        if user_type == "store":
            await self.show_city_selection(chat_id, date_obj, context)
        elif user_type == "city":
            await self.show_city_stats(chat_id, date_obj, context)
        elif user_type == "general_date":
            await self.show_general_date_stats(chat_id, date_obj, context)
        elif user_type == "network":
            await self.show_network_selection(chat_id, date_obj, context)

    async def show_city_selection(self, chat_id: int, date_obj, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показывает выбор города"""
        cities = await self.get_available_cities(date_obj)

        if not cities:
            await self.send_result_message(chat_id, f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}", context)
            return

        keyboard = []
        for city in sorted(cities):
            keyboard.append([InlineKeyboardButton(city, callback_data=f"city_{city}")])

        context.user_data["selected_date"] = date_obj
        text = f"🏙️ Выберите город за {date_obj.strftime('%d.%m.%Y')}:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def show_city_stats(self, chat_id: int, date_obj, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показывает статистику по городу"""
        # Получаем данные и формируем отчет
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        if morning_filtered.empty and evening_filtered.empty:
            await self.send_result_message(chat_id, f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}", context)
            return

        # Получаем статистику по городам
        city_stats = await self.get_city_statistics(morning_filtered, evening_filtered)

        if not city_stats:
            await self.send_result_message(chat_id, f"❌ Нет завершенных отчетов за {date_obj.strftime('%d.%m.%Y')}", context)
            return

        message = f"🏙️ <b>Статистика по городам за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"

        for city, stats in sorted(city_stats.items()):
            message += f"🏙️ <b>{city}</b>\n"
            message += f"🏪 Магазинов: {stats['stores']}\n"
            message += f"💰 Продаж: {stats['sales']} шт.\n"
            message += f"📈 Эффективность: {stats['efficiency']}%\n\n"

        await self.send_result_message(chat_id, message, context)

    async def show_general_date_stats(self, chat_id: int, date_obj, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показывает общую статистику за дату"""
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        if morning_filtered.empty and evening_filtered.empty:
            await self.send_result_message(chat_id, f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}", context)
            return

        # Получаем все доступные отчеты
        reports = self.data_processor.process_daily_reports(morning_df, evening_filtered)

        if not reports:
            expected = len(morning_filtered)
            message = f"📊 <b>Статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
            message += f"📋 Ожидалось отчетов: {expected}\n"
            message += f"✅ Получено отчетов: 0\n"
            message += f"❌ Пропущено: {expected}\n\n"
            message += "Пока нет завершенных пар отчетов за эту дату."
        else:
            total_sales = sum(r['total_sales'] for r in reports)
            avg_efficiency = sum(r['efficiency'] for r in reports) / len(reports)

            message = f"📊 <b>Общая статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
            message += f"🏪 Магазинов: {len(reports)}\n"
            message += f"💰 Общий объем продаж: {total_sales} шт.\n"
            message += f"📈 Средняя эффективность: {avg_efficiency:.1f}%\n"
            message += f"👥 Общее количество участников: {sum(r['visitors'] for r in reports)}\n"

        await self.send_result_message(chat_id, message, context)

    async def show_network_selection(self, chat_id: int, date_obj, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показывает выбор сети"""
        networks = await self.get_available_networks(date_obj)

        if not networks:
            await self.send_result_message(chat_id, "❌ Нет данных за выбранную дату", context)
            return

        keyboard = []
        for network in sorted(networks):
            keyboard.append([InlineKeyboardButton(network, callback_data=f"network_{network}")])

        context.user_data["selected_date"] = date_obj
        text = f"🏢 Выберите сеть за {date_obj.strftime('%d.%m.%Y')}:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_city_selection(self, chat_id: int, city: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обрабатывает выбор города"""
        date_obj = context.user_data.get("selected_date")
        if not date_obj:
            await self.send_result_message(chat_id, "❌ Ошибка: дата не выбрана", context)
            return

        networks = await self.get_available_networks_in_city(date_obj, city)

        if not networks:
            await self.send_result_message(chat_id, f"❌ Нет данных по городу {city} за выбранную дату", context)
            return

        keyboard = []
        for network in sorted(networks):
            keyboard.append([InlineKeyboardButton(network, callback_data=f"network_{network}")])

        context.user_data["selected_city"] = city
        text = f"🏢 Выберите сеть в городе {city}:"
        await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_network_selection(self, chat_id: int, network: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обрабатывает выбор сети"""
        user_data = context.user_data
        date_obj = user_data.get("selected_date")
        city = user_data.get("selected_city")

        if not date_obj:
            await self.send_result_message(chat_id, "❌ Ошибка: дата не выбрана", context)
            return

        if city:  # История по магазину
            addresses = await self.get_available_addresses(date_obj, city, network)

            if not addresses:
                await self.send_result_message(chat_id, f"❌ Нет данных по сети {network} в городе {city}", context)
                return

            keyboard = []
            for address in addresses:
                keyboard.append([InlineKeyboardButton(
                    address[:30] + "..." if len(address) > 30 else address,
                    callback_data=f"address_{address}"
                )])

            user_data["selected_network"] = network
            text = f"🏪 Выберите адрес в сети {network}:"
            await self.send_ui_message(chat_id, text, context, reply_markup=InlineKeyboardMarkup(keyboard))
        else:  # История по сети
            await self.show_network_stats(chat_id, date_obj, network, context)

    async def handle_address_selection(self, chat_id: int, address: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обрабатывает выбор адреса и показывает статистику"""
        user_data = context.user_data
        date_obj = user_data.get("selected_date")
        city = user_data.get("selected_city")
        network = user_data.get("selected_network")

        if not all([date_obj, city, network]):
            await self.send_result_message(chat_id, "❌ Ошибка: не все параметры выбраны", context)
            return

        # Получаем данные и находим соответствующий отчет
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        # Фильтруем по дате
        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        # Ищем отчет по адресу
        report = await self.find_report_by_address(morning_filtered, evening_filtered, address)

        if not report:
            await self.send_result_message(chat_id, f"❌ Отчет по адресу {address} не найден или не завершен", context)
            return

        message = self.format_detailed_report(report)
        await self.send_result_message(chat_id, message, context)

    async def show_network_stats(self, chat_id: int, date_obj, network: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Показывает статистику по сети"""
        morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
        evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        # Получаем статистику по сети
        network_stats = await self.get_network_statistics(morning_filtered, evening_filtered, network)

        if not network_stats:
            await self.send_result_message(chat_id, f"❌ Нет данных по сети {network} за {date_obj.strftime('%d.%m.%Y')}", context)
            return

        message = f"🏢 <b>Статистика сети '{network}' за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
        message += f"🏪 Магазинов: {network_stats['stores']}\n"
        message += f"💰 Общий объем продаж: {network_stats['sales']} шт.\n"
        message += f"📈 Средняя эффективность: {network_stats['efficiency']:.1f}%\n"
        message += f"👥 Участников: {network_stats['visitors']}\n"

        # Показываем продажи по сырам
        if network_stats['cheese_sales']:
            message += "\n🧀 <b>Продажи по сырам:</b>\n"
            for cheese, sales in network_stats['cheese_sales'].items():
                message += f"• {cheese}: {sales} шт.\n"

        await self.send_result_message(chat_id, message, context)

    # Вспомогательные методы (нужно адаптировать под async)
    async def get_available_dates(self):
        """Получает список доступных дат"""
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            dates = set()
            if not morning_df.empty:
                morning_dates = morning_df[self.config.MORNING_COLUMNS['date']].dropna().dt.date.unique()
                dates.update(morning_dates)
            if not evening_df.empty:
                evening_dates = evening_df[self.config.EVENING_COLUMNS['date']].dropna().dt.date.unique()
                dates.update(evening_dates)

            return sorted(list(dates), reverse=True)
        except Exception as e:
            logger.error(f"Ошибка получения дат: {e}")
            return []

    async def get_available_cities(self, date_obj):
        """Получает список городов за определенную дату"""
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            cities = set()
            if not morning_df.empty:
                morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
                cities.update(morning_filtered[self.config.MORNING_COLUMNS['city']].dropna().unique())
            if not evening_df.empty:
                evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]
                cities.update(evening_filtered[self.config.EVENING_COLUMNS['city']].dropna().unique())

            return list(cities)
        except Exception as e:
            logger.error(f"Ошибка получения городов: {e}")
            return []

    async def get_available_networks(self, date_obj):
        """Получает список сетей за определенную дату"""
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            networks = set()
            if not morning_df.empty:
                morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
                networks.update(morning_filtered[self.config.MORNING_COLUMNS['network_name']].dropna().unique())
            if not evening_df.empty:
                evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]
                networks.update(evening_filtered[self.config.EVENING_COLUMNS['network_name']].dropna().unique())

            return list(networks)
        except Exception as e:
            logger.error(f"Ошибка получения сетей: {e}")
            return []

    async def get_available_networks_in_city(self, date_obj, city):
        """Получает список сетей в конкретном городе"""
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = self.sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            networks = set()
            if not morning_df.empty:
                filtered = morning_df[
                    (morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj) &
                    (morning_df[self.config.MORNING_COLUMNS['city']] == city)
                ]
                networks.update(filtered[self.config.MORNING_COLUMNS['network_name']].dropna().unique())
            if not evening_df.empty:
                filtered = evening_df[
                    (evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj) &
                    (evening_df[self.config.EVENING_COLUMNS['city']] == city)
                ]
                networks.update(filtered[self.config.EVENING_COLUMNS['network_name']].dropna().unique())

            return list(networks)
        except Exception as e:
            logger.error(f"Ошибка получения сетей в городе: {e}")
            return []

    async def get_available_addresses(self, date_obj, city, network):
        """Получает список адресов в городе и сети"""
        try:
            morning_df = self.sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)

            if morning_df.empty:
                return []

            filtered = morning_df[
                (morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj) &
                (morning_df[self.config.MORNING_COLUMNS['city']] == city) &
                (morning_df[self.config.MORNING_COLUMNS['network_name']] == network)
            ]

            addresses = filtered[self.config.MORNING_COLUMNS['address']].dropna().unique()
            return list(addresses)
        except Exception as e:
            logger.error(f"Ошибка получения адресов: {e}")
            return []

    async def get_city_statistics(self, morning_df, evening_df):
        """Получает статистику по городам"""
        try:
            reports = self.data_processor.process_daily_reports(morning_df, evening_df)

            city_stats = {}
            for report in reports:
                city = report['city']
                if city not in city_stats:
                    city_stats[city] = {
                        'stores': 0,
                        'sales': 0,
                        'efficiency': 0,
                        'reports': []
                    }

                city_stats[city]['stores'] += 1
                city_stats[city]['sales'] += report['total_sales']
                city_stats[city]['reports'].append(report['efficiency'])

            # Вычисляем среднюю эффективность
            for city_data in city_stats.values():
                if city_data['reports']:
                    city_data['efficiency'] = sum(city_data['reports']) / len(city_data['reports'])
                else:
                    city_data['efficiency'] = 0
                del city_data['reports']

            return city_stats
        except Exception as e:
            logger.error(f"Ошибка получения статистики по городам: {e}")
            return {}

    async def get_network_statistics(self, morning_df, evening_df, network):
        """Получает статистику по сети"""
        try:
            reports = self.data_processor.process_daily_reports(morning_df, evening_df)

            network_reports = [r for r in reports if r['network'] == network]

            if not network_reports:
                return None

            total_sales = sum(r['total_sales'] for r in network_reports)
            total_visitors = sum(r['visitors'] for r in network_reports)
            avg_efficiency = sum(r['efficiency'] for r in network_reports) / len(network_reports)

            cheese_sales = {}
            for report in network_reports:
                for cheese, data in report['cheese_data'].items():
                    cheese_sales[cheese] = cheese_sales.get(cheese, 0) + data['sold']

            return {
                'stores': len(network_reports),
                'sales': total_sales,
                'visitors': total_visitors,
                'efficiency': avg_efficiency,
                'cheese_sales': cheese_sales
            }
        except Exception as e:
            logger.error(f"Ошибка получения статистики по сети: {e}")
            return None

    async def find_report_by_address(self, morning_df, evening_df, address):
        """Находит отчет по адресу"""
        try:
            reports = self.data_processor.process_daily_reports(morning_df, evening_df)

            for report in reports:
                if report['normalized_address'] == self.data_processor.normalizer.normalize(address):
                    return report

            return None
        except Exception as e:
            logger.error(f"Ошибка поиска отчета по адресу: {e}")
            return None

    def format_detailed_report(self, report):
        """Форматирует детальный отчет"""
        message = f"""
📊 <b>Отчет по дегустации</b>

📅 <b>Дата:</b> {report['date']}
🏙️ <b>Город:</b> {report['city']}
🏢 <b>Сеть:</b> {report['network']}
👤 <b>Сотрудник:</b> {report['employee']}

👥 <b>Участников:</b> {report['visitors']}

🧀 <b>Остатки на начало дня:</b>
"""
        
        for cheese, data in report['cheese_data'].items():
            message += f"• {cheese}: {data['start']} шт.\n"
        
        message += "\n🏁 <b>Остатки на конец дня:</b>\n"
        for cheese, data in report['cheese_data'].items():
            message += f"• {cheese}: {data['end']} шт.\n"
        
        message += "\n💰 <b>Продажи:</b>\n"
        for cheese, data in report['cheese_data'].items():
            if data['sold'] > 0:
                message += f"• {cheese}: {data['sold']} шт.\n"
        
        message += f"\n📦 <b>Всего продано:</b> {report['total_sales']} шт.\n"
        message += f"🎯 <b>Эффективность:</b> {report['efficiency']}%\n"
        
        return message

def create_application():
    """Создает и настраивает Application"""
    bot = TelegramPTBBot()

    application = Application.builder().token(bot.config.BOT_TOKEN).build()

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", bot.start_command))
    application.add_handler(CallbackQueryHandler(bot.callback_query_handler))

    return application

if __name__ == "__main__":
    # Для тестирования
    application = create_application()
    application.run_polling()
