import requests
from config import Config
import logging
from datetime import datetime, timedelta
import json

class TelegramBot:
    def __init__(self):
        self.bot_token = Config.BOT_TOKEN
        self.chat_id = Config.CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

        # Хранение состояний пользователей для интерактивного режима
        self.user_states = {}
        self.user_data = {}

    def send_message(self, text, parse_mode="HTML", reply_markup=None):
        """Отправляет сообщение в Telegram"""
        url = f"{self.base_url}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': parse_mode,
            'disable_web_page_preview': True
        }

        if reply_markup:
            payload['reply_markup'] = json.dumps(reply_markup)

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            logging.error(f"Ошибка отправки в Telegram: {e}")
            return False

    def send_start_menu(self):
        """Отправляет стартовое меню с инлайн кнопками"""
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

        return self.send_message(text, reply_markup=keyboard)

    def handle_callback(self, callback_data, user_id, google_sheets_service, data_processor):
        """Обрабатывает нажатия инлайн кнопок"""
        if callback_data == "history_store":
            return self.start_store_history(user_id, google_sheets_service)
        elif callback_data == "history_city":
            return self.start_city_history(user_id, google_sheets_service)
        elif callback_data == "history_date":
            return self.start_date_history(user_id)
        elif callback_data == "history_network":
            return self.start_network_history(user_id, google_sheets_service)
        elif callback_data.startswith("date_"):
            date = callback_data.split("_", 1)[1]
            return self.handle_date_selection(user_id, date, google_sheets_service, data_processor)
        elif callback_data.startswith("city_"):
            city = callback_data.split("_", 1)[1]
            return self.handle_city_selection(user_id, city, google_sheets_service, data_processor)
        elif callback_data.startswith("network_"):
            network = callback_data.split("_", 1)[1]
            return self.handle_network_selection(user_id, network, google_sheets_service, data_processor)
        elif callback_data.startswith("address_"):
            address = callback_data.split("_", 1)[1]
            return self.handle_address_selection(user_id, address, google_sheets_service, data_processor)

    def get_available_dates(self, google_sheets_service):
        """Получает список доступных дат из Google Sheets"""
        morning_df = google_sheets_service.get_sheet_data(Config.MORNING_SHEET_ID, Config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(Config.EVENING_SHEET_ID, Config.EVENING_SHEET_NAME)

        dates_morning = set(morning_df[Config.MORNING_COLUMNS['date']].dt.date.unique())
        dates_evening = set(evening_df[Config.EVENING_COLUMNS['date']].dt.date.unique())

        available_dates = sorted(list(dates_morning.union(dates_evening)))

        return available_dates

    def start_store_history(self, user_id, google_sheets_service):
        """Начинает процесс выбора для истории по магазину"""
        self.user_states[user_id] = "waiting_date_store"
        self.user_data[user_id] = {"type": "store"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            return self.send_message("❌ Нет доступных данных для анализа")

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по магазину:"
        return self.send_message(text, reply_markup=keyboard)

    def start_city_history(self, user_id, google_sheets_service):
        """Начинает процесс выбора для истории по городу"""
        self.user_states[user_id] = "waiting_date_city"
        self.user_data[user_id] = {"type": "city"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            return self.send_message("❌ Нет доступных данных для анализа")

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по городу:"
        return self.send_message(text, reply_markup=keyboard)

    def start_date_history(self, user_id):
        """Начинает процесс выбора даты для общей статистики"""
        self.user_states[user_id] = "waiting_date_general"
        self.user_data[user_id] = {"type": "general_date"}

        keyboard = {"inline_keyboard": []}
        for i in range(10):
            date = datetime.now().date() - timedelta(days=i)
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра общей статистики:"
        return self.send_message(text, reply_markup=keyboard)

    def start_network_history(self, user_id, google_sheets_service):
        """Начинает процесс выбора для истории по сети"""
        self.user_states[user_id] = "waiting_date_network"
        self.user_data[user_id] = {"type": "network"}

        dates = self.get_available_dates(google_sheets_service)
        if not dates:
            return self.send_message("❌ Нет доступных данных для анализа")

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:
            keyboard["inline_keyboard"].append([
                {"text": date.strftime("%d.%m.%Y"), "callback_data": f"date_{date.strftime('%Y-%m-%d')}"}
            ])

        text = "📅 Выберите дату для просмотра истории по сети:"
        return self.send_message(text, reply_markup=keyboard)

    def handle_date_selection(self, user_id, date_str, google_sheets_service, data_processor):
        """Обрабатывает выбор даты"""
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        user_type = self.user_data.get(user_id, {}).get("type")

        if user_type == "store":
            return self.show_city_selection(user_id, date_obj, google_sheets_service)
        elif user_type == "city":
            return self.show_city_stats(user_id, date_obj, google_sheets_service, data_processor)
        elif user_type == "general_date":
            return self.show_general_date_stats(user_id, date_obj, google_sheets_service, data_processor)
        elif user_type == "network":
            return self.show_network_selection(user_id, date_obj, google_sheets_service)
