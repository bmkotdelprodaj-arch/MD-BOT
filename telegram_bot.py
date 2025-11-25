import requests
from config import Config
import logging
from datetime import datetime, timedelta
import json
import pandas as pd
import traceback  # <-- ДОБАВЛЕНО

logger = logging.getLogger(__name__)  # <-- Лучше использовать именованный логгер

class TelegramBot:
    def __init__(self):
        self.bot_token = Config.BOT_TOKEN
        self.chat_id = Config.CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

        # Хранение состояний пользователей для интерактивного режима
        self.user_states = {}
        # user_data[user_id] = {
        #   'selected_date': ...,
        #   'selected_network': ...,
        #   'last_menu_message_id': ...  <-- добавим
        # }
        self.user_data = {}

    def send_message(self, text, parse_mode="HTML", reply_markup=None, chat_id=None, message_id=None):
        """Отправляет или редактирует сообщение в Telegram"""
        target_chat_id = chat_id or self.chat_id
        if message_id:
            # Редактируем сообщение
            url = f"{self.base_url}/editMessageText"
            payload = {
                'chat_id': target_chat_id,
                'message_id': message_id,
                'text': text,
                'parse_mode': parse_mode
            }
            if reply_markup:
                payload['reply_markup'] = json.dumps(reply_markup)
        else:
            # Отправляем новое сообщение
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': target_chat_id,
                'text': text,
                'parse_mode': parse_mode,
                'disable_web_page_preview': True
            }
            if reply_markup:
                payload['reply_markup'] = json.dumps(reply_markup)

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            action = "edited" if message_id else "sent"
            logger.info(f"send_message: Message {action} successfully to chat_id {target_chat_id}, message_id {message_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки/редактирования в Telegram: {e}")
            logger.error(f"Payload: {payload}")
            return False

    def delete_message(self, chat_id, message_id):
        """Удаляет сообщение"""
        url = f"{self.base_url}/deleteMessage"
        payload = {
            'chat_id': chat_id,
            'message_id': message_id
        }

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            logger.info(f"delete_message: Message {message_id} deleted from chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления сообщения {message_id} в чате {chat_id}: {e}")
            return False

    def send_start_menu(self, chat_id=None, message_id=None):
        """Отправляет или редактирует стартовое меню с инлайн кнопками"""
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "🏪 История по магазину", "callback_data": "history_store"},
                    {"text": "🏙️ История по городу", "callback_data": "history_city"}
                ],
                [
                    {"text": "📅 История по дате", "callback_data": "history_date"},
                    {"text": "🏢 История по сети", "callback_data": "history_network"}
                ]
            ]
        }

        text = """
🤖 <b>Бот для анализа дегустаций</b>

Выберите тип отчета для просмотра истории:
• 🏪 <b>По магазину</b> - детальная статистика конкретного магазина
• 🏙️ <b>По городу</b> - сводка по всем сетям города
• 📅 <b>По дате</b> - общая статистика за день
• 🏢 <b>По сети</b> - статистика по всей сети
"""

        return self.send_message(text, reply_markup=keyboard, chat_id=chat_id, message_id=message_id)

    def handle_callback(self, callback_data, user_id, google_sheets_service, data_processor, message_id, chat_id):
        """Обрабатывает нажатия инлайн кнопок и редактирует/отправляет новые сообщения"""
        try:
            logger.info(f"handle_callback: Processing callback_data '{callback_data}' for user {user_id} in chat {chat_id}, message {message_id}")
            # Обновляем последнее сообщение меню у пользователя
            if user_id not in self.user_data:
                self.user_data[user_id] = {}
            self.user_data[user_id]['last_menu_message_id'] = message_id

            if callback_data == "start_menu":
                # Отправляем стартовое меню (редактируем сообщение)
                return self.send_start_menu(chat_id=chat_id, message_id=message_id)

            elif callback_data == "history_store":
                logger.info(f"handle_callback: history_store selected by user {user_id}")
                self.user_states[user_id] = "waiting_date_store"
                self.user_data[user_id] = {"type": "store", "last_menu_message_id": message_id}
                return self.start_store_history(user_id, google_sheets_service, chat_id)
            elif callback_data == "history_city":
                logger.info(f"handle_callback: history_city selected by user {user_id}")
                self.user_states[user_id] = "waiting_date_city"
                self.user_data[user_id] = {"type": "city", "last_menu_message_id": message_id}
                return self.start_city_history(user_id, google_sheets_service, chat_id)
            elif callback_data == "history_date":
                logger.info(f"handle_callback: history_date selected by user {user_id}")
                self.user_states[user_id] = "waiting_date_general"
                self.user_data[user_id] = {"type": "general_date", "last_menu_message_id": message_id}
                return self.start_date_history(user_id, chat_id)
            elif callback_data == "history_network":
                logger.info(f"handle_callback: history_network selected by user {user_id}")
                self.user_states[user_id] = "waiting_date_network"
                self.user_data[user_id] = {"type": "network", "last_menu_message_id": message_id}
                return self.start_network_history(user_id, google_sheets_service, chat_id)
            elif callback_data.startswith("date_"):
                date = callback_data.split("_", 1)[1]
                logger.info(f"handle_callback: date '{date}' selected by user {user_id}")
                return self.handle_date_selection(user_id, date, google_sheets_service, data_processor, chat_id)
            elif callback_data.startswith("city_"):
                city = callback_data.split("_", 1)[1]
                logger.info(f"handle_callback: city '{city}' selected by user {user_id}")
                return self.handle_city_selection(user_id, city, google_sheets_service, data_processor, chat_id)
            elif callback_data.startswith("network_"):
                network = callback_data.split("_", 1)[1]
                logger.info(f"handle_callback: network '{network}' selected by user {user_id}")
                return self.handle_network_selection(user_id, network, google_sheets_service, data_processor, chat_id)
            elif callback_data.startswith("address_"):
                address = callback_data.split("_", 1)[1]
                logger.info(f"handle_callback: address '{address}' selected by user {user_id}")
                return self.handle_address_selection(user_id, address, google_sheets_service, data_processor, chat_id)
            else:
                logger.warning(f"handle_callback: Unknown callback_data '{callback_data}' from user {user_id}")
                # Отправляем пользователю сообщение об ошибке В ЛИЧКУ
                self.send_message(
                    text="❌ Неизвестная команда.",
                    chat_id=user_id  # важно: отправляем пользователю, а не в общий чат
                )
        except Exception as e:
            logger.error(f"handle_callback: Error processing callback '{callback_data}' for user {user_id}: {e}")
            logger.error(traceback.format_exc())  # <-- ВАЖНО: теперь видно стек вызовов
            # Отправляем пользователю сообщение об ошибке В ЛИЧКУ
            self.send_message(
                text="❌ Произошла ошибка при обработке запроса. Пожалуйста, повторите позже.",
                chat_id=user_id
            )

    def get_available_dates(self, google_sheets_service):
        """Получает список доступных дат из Google Sheets"""
        logger.info("get_available_dates: Start fetching dates from Google Sheets")

        morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
        logger.info(f"get_available_dates: morning_df loaded with {len(morning_df)} rows")

        evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)
        logger.info(f"get_available_dates: evening_df loaded with {len(evening_df)} rows")

        # convert date columns to datetime to avoid .dt accessor error
        morning_df[Config.MORNING_COLUMNS['date']] = pd.to_datetime(morning_df[Config.MORNING_COLUMNS['date']])
        evening_df[Config.EVENING_COLUMNS['date']] = pd.to_datetime(evening_df[Config.EVENING_COLUMNS['date']])

        dates_morning = set(morning_df[Config.MORNING_COLUMNS['date']].dt.date.unique())
        dates_evening = set(evening_df[Config.EVENING_COLUMNS['date']].dt.date.unique())

        available_dates = sorted(list(dates_morning.union(dates_evening)))
        logger.info(f"get_available_dates: available_dates found {available_dates}")

        return available_dates

    def start_store_history(self, user_id, google_sheets_service, chat_id):
        """Начинает процесс выбора для истории по магазину"""
        logger.info(f"start_store_history: User {user_id} initiated")
        self.user_states[user_id] = "waiting_date_store"
        self.user_data[user_id] = {"type": "store"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            self.send_message("❌ Нет доступных данных для анализа", chat_id=chat_id)
            return

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по магазину:"
        self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

    def start_city_history(self, user_id, google_sheets_service, chat_id):
        """Начинает процесс выбора для истории по городу"""
        logger.info(f"start_city_history: User {user_id} initiated")
        self.user_states[user_id] = "waiting_date_city"
        self.user_data[user_id] = {"type": "city"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            self.send_message("❌ Нет доступных данных для анализа", chat_id=chat_id)
            return

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по городу:"
        self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

    def start_date_history(self, user_id, chat_id):
        """Начинает процесс выбора даты для общей статистики"""
        logger.info(f"start_date_history: User {user_id} initiated")
        self.user_states[user_id] = "waiting_date_general"
        self.user_data[user_id] = {"type": "general_date"}

        keyboard = {"inline_keyboard": []}
        for i in range(10):
            date = datetime.now().date() - timedelta(days=i)
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра общей статистики:"
        self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

    def start_network_history(self, user_id, google_sheets_service, chat_id):
        """Начинает процесс выбора для истории по сети"""
        logger.info(f"start_network_history: User {user_id} initiated")
        self.user_states[user_id] = "waiting_date_network"
        self.user_data[user_id] = {"type": "network"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            self.send_message("❌ Нет доступных данных для анализа", chat_id=chat_id)
            return

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по сети:"
        self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

    def handle_date_selection(self, user_id, date_str, google_sheets_service, data_processor, chat_id):
        """Обрабатывает выбор даты"""
        logger.info(f"handle_date_selection: Processing date_str '{date_str}' for user {user_id}")
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            logger.info(f"handle_date_selection: Parsed date_obj: {date_obj}")
        except ValueError as e:
            logger.error(f"handle_date_selection: Invalid date format '{date_str}': {e}")
            self.send_message("❌ Неверный формат даты.", chat_id=chat_id)
            return

        user_type = self.user_data.get(user_id, {}).get("type")
        logger.info(f"handle_date_selection: user_type is '{user_type}'")

        if user_type == "store":
            return self.show_network_selection(user_id, date_obj, google_sheets_service, chat_id)
        elif user_type == "city":
            return self.show_city_selection(user_id, date_obj, google_sheets_service, chat_id)
        elif user_type == "general_date":
            return self.show_general_date_stats(user_id, date_obj, google_sheets_service, data_processor, chat_id)
        elif user_type == "network":
            return self.show_network_selection(user_id, date_obj, google_sheets_service, chat_id)
        else:
            logger.warning(f"handle_date_selection: Unknown user_type '{user_type}' for user_id")
            self.send_message("❌ Неизвестный тип запроса.", chat_id=chat_id)

    def show_network_selection(self, user_id, date_obj, google_sheets_service, chat_id):
        """Показывает выбор сети для выбранной даты (для веток 'магазин' и 'сеть')"""
        logger.info(f"show_network_selection: Called for user {user_id}, date {date_obj}")
        try:
            # Получаем все отчеты за дату
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            # Объединяем уникальные значения сетей из обеих таблиц
            all_networks = set()
            if not morning_filtered.empty:
                all_networks.update(morning_filtered[Config.MORNING_COLUMNS['network_name']].dropna().unique())
            if not evening_filtered.empty:
                all_networks.update(evening_filtered[Config.EVENING_COLUMNS['network_name']].dropna().unique())

            if not all_networks:
                self.send_message(f"❌ Нет данных по сетям за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                return

            # Сохраняем дату в user_data
            self.user_data[user_id]['selected_date'] = date_obj

            keyboard = {"inline_keyboard": []}
            for network in sorted(all_networks):
                keyboard["inline_keyboard"].append([
                    {"text": network, "callback_data": f"network_{network}"}
                ])

            text = f"🏢 Выберите сеть за {date_obj.strftime('%d.%m.%Y')}:"
            self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

        except Exception as e:
            logger.error(f"show_network_selection failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании списка сетей.", chat_id=chat_id)

    def show_city_selection(self, user_id, date_obj, google_sheets_service, chat_id):
        """Показывает выбор города для выбранной даты"""
        logger.info(f"show_city_selection: Called for user {user_id}, date {date_obj}")
        try:
            # Получаем все отчеты за дату
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            # Объединяем уникальные значения городов
            all_cities = set()
            if not morning_filtered.empty:
                all_cities.update(morning_filtered[Config.MORNING_COLUMNS['city']].dropna().unique())
            if not evening_filtered.empty:
                all_cities.update(evening_filtered[Config.EVENING_COLUMNS['city']].dropna().unique())

            if not all_cities:
                self.send_message(f"❌ Нет данных по городам за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                return

            # Сохраняем дату в user_data
            self.user_data[user_id]['selected_date'] = date_obj

            keyboard = {"inline_keyboard": []}
            for city in sorted(all_cities):
                keyboard["inline_keyboard"].append([
                    {"text": city, "callback_data": f"city_{city}"}
                ])

            text = f"🏙️ Выберите город за {date_obj.strftime('%d.%m.%Y')}:"
            self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

        except Exception as e:
            logger.error(f"show_city_selection failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании списка городов.", chat_id=chat_id)

    def handle_city_selection(self, user_id, city, google_sheets_service, data_processor, chat_id):
        """Обрабатывает выбор города — показывает статистику по городу"""
        logger.info(f"handle_city_selection: Called for user {user_id}, city {city}")
        date_obj = self.user_data.get(user_id, {}).get('selected_date')
        if not date_obj:
            self.send_message("❌ Ошибка: дата не выбрана.", chat_id=chat_id)
            return

        try:
            # Получаем все отчеты за дату
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            # Обрабатываем отчеты
            reports = data_processor.process_daily_reports(morning_filtered, evening_filtered)

            # Фильтруем по городу
            city_reports = [r for r in reports if r['city'] == city]

            if not city_reports:
                self.send_message(f"❌ Нет данных по городу {city} за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                return

            # Агрегируем отчеты
            summary = self.aggregate_reports(city_reports)

            # Форматируем сообщение
            message = f"🏙️ <b>Статистика по {city} за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
            message += self.format_report_summary(summary)

            self.send_message(message, chat_id=chat_id)

        except Exception as e:
            logger.error(f"handle_city_selection failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании отчета по городу.", chat_id=chat_id)

    def handle_network_selection(self, user_id, network, google_sheets_service, data_processor, chat_id):
        """Обрабатывает выбор сети — показывает статистику по сети или города"""
        logger.info(f"handle_network_selection: Called for user {user_id}, network {network}")
        date_obj = self.user_data.get(user_id, {}).get('selected_date')
        user_type = self.user_data.get(user_id, {}).get('type')
        if not date_obj:
            self.send_message("❌ Ошибка: дата не выбрана.", chat_id=chat_id)
            return

        try:
            # Получаем все отчеты за дату
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            # Обрабатываем отчеты
            reports = data_processor.process_daily_reports(morning_filtered, evening_df)

            # Если это ветка "по магазину" — показываем города в этой сети
            if user_type == "store":
                # Фильтруем по сети
                network_reports = [r for r in reports if r['network'] == network]
                if not network_reports:
                    self.send_message(f"❌ Нет данных по сети {network} за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                    return

                # Получаем уникальные города
                cities = set(r['city'] for r in network_reports)

                if not cities:
                    self.send_message(f"❌ Нет данных по городам в сети {network} за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                    return

                # Сохраняем выбранную сеть
                self.user_data[user_id]['selected_network'] = network

                keyboard = {"inline_keyboard": []}
                for city in sorted(cities):
                    keyboard["inline_keyboard"].append([
                        {"text": city, "callback_data": f"city_{city}"}
                    ])

                text = f"🏙️ Выберите город в сети {network}:"
                self.send_message(text, reply_markup=keyboard, chat_id=chat_id)

            # Если это ветка "по сети" — показываем статистику по сети
            elif user_type == "network":
                network_reports = [r for r in reports if r['network'] == network]

                if not network_reports:
                    self.send_message(f"❌ Нет данных по сети {network} за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                    return

                # Агрегируем отчеты
                summary = self.aggregate_reports(network_reports)

                # Форматируем сообщение
                message = f"🏢 <b>Статистика сети '{network}' за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
                message += self.format_report_summary(summary)

                self.send_message(message, chat_id=chat_id)

            else:
                self.send_message("❌ Неизвестный тип запроса.", chat_id=chat_id)

        except Exception as e:
            logger.error(f"handle_network_selection failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании отчета по сети.", chat_id=chat_id)

    def handle_address_selection(self, user_id, address, google_sheets_service, data_processor, chat_id):
        """Обрабатывает выбор адреса — показывает детальный отчет по магазину"""
        logger.info(f"handle_address_selection: Called for user {user_id}, address {address}")
        date_obj = self.user_data.get(user_id, {}).get('selected_date')
        network = self.user_data.get(user_id, {}).get('selected_network')
        city = self.user_data.get(user_id, {}).get('selected_city')

        if not all([date_obj, network, city]):
            self.send_message("❌ Ошибка: не все параметры выбраны.", chat_id=chat_id)
            return

        try:
            # Получаем все отчеты за дату
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            # Обрабатываем отчеты
            reports = data_processor.process_daily_reports(morning_filtered, evening_filtered)

            # Нормализуем адрес
            normalized_address = data_processor.normalizer.normalize(address)

            # Ищем отчет по адресу
            found_report = None
            for report in reports:
                if report['normalized_address'] == normalized_address:
                    found_report = report
                    break

            if not found_report:
                self.send_message(f"❌ Отчет по адресу '{address}' не найден.", chat_id=chat_id)
                return

            # Форматируем детальный отчет
            message = self.format_detailed_report(found_report)
            self.send_message(message, chat_id=chat_id)

        except Exception as e:
            logger.error(f"handle_address_selection failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании отчета по магазину.", chat_id=chat_id)

    def show_general_date_stats(self, user_id, date_obj, google_sheets_service, data_processor, chat_id):
        """Показывает общую статистику за дату"""
        logger.info(f"show_general_date_stats: Called for user {user_id}, date {date_obj}")
        try:
            # Получаем данные из Google Sheets
            morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
            evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

            # Убедимся, что колонки дат — datetime
            date_col_morning = Config.MORNING_COLUMNS['date']
            date_col_evening = Config.EVENING_COLUMNS['date']

            if not pd.api.types.is_datetime64_any_dtype(morning_df[date_col_morning]):
                morning_df[date_col_morning] = pd.to_datetime(morning_df[date_col_morning])
            if not pd.api.types.is_datetime64_any_dtype(evening_df[date_col_evening]):
                evening_df[date_col_evening] = pd.to_datetime(evening_df[date_col_evening])

            # Фильтрация по дате
            morning_filtered = morning_df[morning_df[date_col_morning].dt.date == date_obj]
            evening_filtered = evening_df[evening_df[date_col_evening].dt.date == date_obj]

            if morning_filtered.empty and evening_filtered.empty:
                self.send_message(f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}", chat_id=chat_id)
                return

            # Обработка отчетов через data_processor
            reports = data_processor.process_daily_reports(morning_filtered, evening_filtered)

            # Вычисляем ожидаемое количество отчетов
            expected_reports = data_processor.get_expected_reports_for_day(morning_df, evening_df, date_obj)

            actual_reports = len(reports)

            # Если нет пар — формируем сообщение о пропущенных
            if not reports:
                message = f"📊 <b>Общая статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
                message += f"📋 Ожидалось отчетов: {expected_reports}\n"
                message += f"✅ Получено отчетов: 0\n"
                message += f"❌ Пропущено: {expected_reports}\n\n"
                message += "Пока нет завершенных пар отчетов за эту дату."
            else:
                # Агрегируем отчеты
                summary = self.aggregate_reports(reports)

                # Форматируем сообщение
                message = f"📊 <b>Общая статистика за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"
                message += self.format_report_summary(summary)

            self.send_message(message, chat_id=chat_id)

        except Exception as e:
            logger.error(f"show_general_date_stats failed: {e}")
            logger.error(traceback.format_exc())
            self.send_message("❌ Ошибка при формировании отчета.", chat_id=chat_id)

    def aggregate_reports(self, reports):
        """Агрегирует список отчетов в один общий"""
        if not reports:
            return {
                'cheese_start': {cheese: 0 for cheese in Config.CHEESE_TYPES},
                'cheese_end': {cheese: 0 for cheese in Config.CHEESE_TYPES},
                'cheese_sold': {cheese: 0 for cheese in Config.CHEESE_TYPES},
                'total_sales': 0,
                'total_visitors': 0,
                'efficiency': 0.0,
                'stores': 0
            }

        total = {
            'cheese_start': {cheese: 0 for cheese in Config.CHEESE_TYPES},
            'cheese_end': {cheese: 0 for cheese in Config.CHEESE_TYPES},
            'cheese_sold': {cheese: 0 for cheese in Config.CHEESE_TYPES},
            'total_sales': 0,
            'total_visitors': 0,
            'efficiency': 0.0,
            'stores': len(reports)
        }

        for report in reports:
            total['total_sales'] += report['total_sales']
            total['total_visitors'] += report.get('visitors', 0)
            for cheese in Config.CHEESE_TYPES:
                data = report['cheese_data'].get(cheese, {'start': 0, 'end': 0, 'sold': 0})
                total['cheese_start'][cheese] += data['start']
                total['cheese_end'][cheese] += data['end']
                total['cheese_sold'][cheese] += data['sold']

        # Средняя эффективность
        if total['stores'] > 0:
            total['efficiency'] = sum(r['efficiency'] for r in reports) / total['stores']

        return total

    def format_report_summary(self, summary):
        """Форматирует агрегированный отчет в строку"""
        message = f"🏪 Магазинов: {summary['stores']}\n"
        message += f"👥 Участников: {summary['total_visitors']}\n\n"

        for cheese in Config.CHEESE_TYPES:
            message += f"🧀 {cheese}: {summary['cheese_start'][cheese]} начальный остаток\n"
        message += "\n"
        for cheese in Config.CHEESE_TYPES:
            message += f"🧀 {cheese}: {summary['cheese_end'][cheese]} конечный остаток\n"
        message += "\n"
        for cheese in Config.CHEESE_TYPES:
            message += f"🧀 {cheese}: {summary['cheese_sold'][cheese]} продано\n"
        message += f"\n📦 <b>Всего продано:</b> {summary['total_sales']} шт.\n"
        message += f"🎯 <b>Эффективность:</b> {summary['efficiency']:.1f}%\n"

        return message

    def format_detailed_report(self, report):
        """Форматирует детальный отчет по магазину"""
        message = f"""
📊 <b>Отчет по дегустации</b>

📅 <b>Дата:</b> {report['date']}
🏙️ <b>Город:</b> {report['city']}
🏢 <b>Сеть:</b> {report['network']}
🏪 <b>Адрес:</b> {report['normalized_address']}
👤 <b>Сотрудник:</b> {report['employee']}

👥 <b>Участников:</b> {report['visitors']}

"""

        # Выводим остатки на начало дня
        for cheese in Config.CHEESE_TYPES:
            data = report['cheese_data'].get(cheese, {'start': 0})
            message += f" cheeses_start = report['cheese_data']\n"
        message += "\n"
        for cheese in Config.CHEESE_TYPES:
            data = report['cheese_data'].get(cheese, {'end': 0})
            message += f" cheeses_end = report['cheese_data']\n"
        message += "\n"
        for cheese in Config.CHEESE_TYPES:
            data = report['cheese_data'].get(cheese, {'sold': 0})
            message += f" cheese_sold = report['cheese_data']\n"
        message += f"\n📦 <b>Всего продано:</b> {report['total_sales']} шт.\n"
        message += f"🎯 <b>Эффективность:</b> {report['efficiency']}%\n"

        return message
