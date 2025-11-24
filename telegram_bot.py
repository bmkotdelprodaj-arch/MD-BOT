import requests
from config import Config
import logging
from datetime import datetime, timedelta
import json

class TelegramBot:
    def __init__(self):
        self.config = Config()
        self.bot_token = self.config.BOT_TOKEN
        self.chat_id = self.config.CHAT_ID
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

    def start_store_history(self, user_id, google_sheets_service):
        """Начинает процесс выбора для истории по магазину"""
        self.user_states[user_id] = "waiting_date_store"
        self.user_data[user_id] = {"type": "store"}

        # Получаем доступные даты из таблиц
        dates = self.get_available_dates(google_sheets_service)

        if not dates:
            return self.send_message("❌ Нет доступных данных для анализа")

        keyboard = {"inline_keyboard": []}
        for date in dates[-10:]:  # Показываем последние 10 дат
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

        # Создаем клавиатуру с последними 10 днями
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

    def show_city_selection(self, user_id, date_obj, google_sheets_service):
        """Показывает выбор города для истории по магазину"""
        cities = self.get_available_cities(google_sheets_service, date_obj)

        if not cities:
            return self.send_message("❌ Нет данных за выбранную дату")

        keyboard = {"inline_keyboard": []}
        for city in sorted(cities):
            keyboard["inline_keyboard"].append([
                {"text": city, "callback_data": f"city_{city}"}
            ])

        self.user_data[user_id]["selected_date"] = date_obj
        text = f"🏙️ Выберите город за {date_obj.strftime('%d.%m.%Y')}:"
        return self.send_message(text, reply_markup=keyboard)

    def show_city_stats(self, user_id, date_obj, google_sheets_service, data_processor):
        """Показывает статистику по городу"""
        # Получаем все отчеты за дату
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

        # Фильтруем по дате
        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        if morning_filtered.empty and evening_filtered.empty:
            return self.send_message(f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}")

        # Получаем статистику по городам
        city_stats = self.get_city_statistics(morning_filtered, evening_filtered, data_processor)

        if not city_stats:
            return self.send_message(f"❌ Нет завершенных отчетов за {date_obj.strftime('%d.%m.%Y')}")

        message = f"🏙️ <b>Статистика по городам за {date_obj.strftime('%d.%m.%Y')}</b>\n\n"

        for city, stats in sorted(city_stats.items()):
            message += f"🏙️ <b>{city}</b>\n"
            message += f"🏪 Магазинов: {stats['stores']}\n"
            message += f"💰 Продаж: {stats['sales']} шт.\n"
            message += f"📈 Эффективность: {stats['efficiency']}%\n\n"

        return self.send_message(message)

    def show_general_date_stats(self, user_id, date_obj, google_sheets_service, data_processor):
        """Показывает общую статистику за дату"""
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        if morning_filtered.empty and evening_filtered.empty:
            return self.send_message(f"❌ Нет данных за {date_obj.strftime('%d.%m.%Y')}")

        # Получаем все доступные отчеты
        reports = data_processor.process_daily_reports(morning_df, evening_filtered)

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

        return self.send_message(message)

    def show_network_selection(self, user_id, date_obj, google_sheets_service):
        """Показывает выбор сети"""
        networks = self.get_available_networks(google_sheets_service, date_obj)

        if not networks:
            return self.send_message("❌ Нет данных за выбранную дату")

        keyboard = {"inline_keyboard": []}
        for network in sorted(networks):
            keyboard["inline_keyboard"].append([
                {"text": network, "callback_data": f"network_{network}"}
            ])

        self.user_data[user_id]["selected_date"] = date_obj
        text = f"🏢 Выберите сеть за {date_obj.strftime('%d.%m.%Y')}:"
        return self.send_message(text, reply_markup=keyboard)

    def handle_city_selection(self, user_id, city, google_sheets_service, data_processor):
        """Обрабатывает выбор города"""
        date_obj = self.user_data.get(user_id, {}).get("selected_date")
        if not date_obj:
            return self.send_message("❌ Ошибка: дата не выбрана")

        networks = self.get_available_networks_in_city(google_sheets_service, date_obj, city)

        if not networks:
            return self.send_message(f"❌ Нет данных по городу {city} за выбранную дату")

        keyboard = {"inline_keyboard": []}
        for network in sorted(networks):
            keyboard["inline_keyboard"].append([
                {"text": network, "callback_data": f"network_{network}"}
            ])

        self.user_data[user_id]["selected_city"] = city
        text = f"🏢 Выберите сеть в городе {city}:"
        return self.send_message(text, reply_markup=keyboard)

    def handle_network_selection(self, user_id, network, google_sheets_service, data_processor):
        """Обрабатывает выбор сети"""
        user_data = self.user_data.get(user_id, {})
        date_obj = user_data.get("selected_date")
        city = user_data.get("selected_city")

        if not date_obj:
            return self.send_message("❌ Ошибка: дата не выбрана")

        if city:  # История по магазину
            addresses = self.get_available_addresses(google_sheets_service, date_obj, city, network)

            if not addresses:
                return self.send_message(f"❌ Нет данных по сети {network} в городе {city}")

            keyboard = {"inline_keyboard": []}
            for address in addresses:
                keyboard["inline_keyboard"].append([
                    {"text": address[:30] + "..." if len(address) > 30 else address,
                     "callback_data": f"address_{address}"}
                ])

            self.user_data[user_id]["selected_network"] = network
            text = f"🏪 Выберите адрес в сети {network}:"
            return self.send_message(text, reply_markup=keyboard)
        else:  # История по сети
            return self.show_network_stats(user_id, date_obj, network, google_sheets_service, data_processor)

    def handle_address_selection(self, user_id, address, google_sheets_service, data_processor):
        """Обрабатывает выбор адреса и показывает статистику"""
        user_data = self.user_data.get(user_id, {})
        date_obj = user_data.get("selected_date")
        city = user_data.get("selected_city")
        network = user_data.get("selected_network")

        if not all([date_obj, city, network]):
            return self.send_message("❌ Ошибка: не все параметры выбраны")

        # Получаем данные и находим соответствующий отчет
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

        # Фильтруем по дате
        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        # Ищем отчет по адресу
        report = self.find_report_by_address(morning_filtered, evening_filtered, address, data_processor)

        if not report:
            return self.send_message(f"❌ Отчет по адресу {address} не найден или не завершен")

        message = self.format_detailed_report(report)
        return self.send_message(message)

    def show_network_stats(self, user_id, date_obj, network, google_sheets_service, data_processor):
        """Показывает статистику по сети"""
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

        morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
        evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]

        # Получаем статистику по сети
        network_stats = self.get_network_statistics(morning_filtered, evening_filtered, network, data_processor)

        if not network_stats:
            return self.send_message(f"❌ Нет данных по сети {network} за {date_obj.strftime('%d.%m.%Y')}")

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

        return self.send_message(message)

    # Вспомогательные методы для получения данных
    def get_available_dates(self, google_sheets_service):
        """Получает список доступных дат из таблиц"""
        try:
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

            dates = set()
            if not morning_df.empty:
                morning_dates = morning_df[self.config.MORNING_COLUMNS['date']].dropna().dt.date.unique()
                dates.update(morning_dates)
            if not evening_df.empty:
                evening_dates = evening_df[self.config.EVENING_COLUMNS['date']].dropna().dt.date.unique()
                dates.update(evening_dates)

            return sorted(list(dates), reverse=True)
        except Exception as e:
            logging.error(f"Ошибка получения дат: {e}")
            return []

    def get_available_cities(self, google_sheets_service, date_obj):
        """Получает список городов за определенную дату"""
        try:
        morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID, self.config.MORNING_SHEET_NAME)
        evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID, self.config.EVENING_SHEET_NAME)

            cities = set()
            if not morning_df.empty:
                morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
                cities.update(morning_filtered[self.config.MORNING_COLUMNS['city']].dropna().unique())
            if not evening_df.empty:
                evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]
                cities.update(evening_filtered[self.config.EVENING_COLUMNS['city']].dropna().unique())

            return list(cities)
        except Exception as e:
            logging.error(f"Ошибка получения городов: {e}")
            return []

    def get_available_networks(self, google_sheets_service, date_obj):
        """Получает список сетей за определенную дату"""
        try:
            morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

            networks = set()
            if not morning_df.empty:
                morning_filtered = morning_df[morning_df[self.config.MORNING_COLUMNS['date']].dt.date == date_obj]
                networks.update(morning_filtered[self.config.MORNING_COLUMNS['network_name']].dropna().unique())
            if not evening_df.empty:
                evening_filtered = evening_df[evening_df[self.config.EVENING_COLUMNS['date']].dt.date == date_obj]
                networks.update(evening_filtered[self.config.EVENING_COLUMNS['network_name']].dropna().unique())

            return list(networks)
        except Exception as e:
            logging.error(f"Ошибка получения сетей: {e}")
            return []

    def get_available_networks_in_city(self, google_sheets_service, date_obj, city):
        """Получает список сетей в конкретном городе"""
        try:
            morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)
            evening_df = google_sheets_service.get_sheet_data(self.config.EVENING_SHEET_ID)

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
            logging.error(f"Ошибка получения сетей в городе: {e}")
            return []

    def get_available_addresses(self, google_sheets_service, date_obj, city, network):
        """Получает список адресов в городе и сети"""
        try:
            morning_df = google_sheets_service.get_sheet_data(self.config.MORNING_SHEET_ID)

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
            logging.error(f"Ошибка получения адресов: {e}")
            return []

    def get_city_statistics(self, morning_df, evening_df, data_processor):
        """Получает статистику по городам"""
        try:
            reports = data_processor.process_daily_reports(morning_df, evening_df)

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
            logging.error(f"Ошибка получения статистики по городам: {e}")
            return {}

    def get_network_statistics(self, morning_df, evening_df, network, data_processor):
        """Получает статистику по сети"""
        try:
            reports = data_processor.process_daily_reports(morning_df, evening_df)

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
            logging.error(f"Ошибка получения статистики по сети: {e}")
            return None

    def find_report_by_address(self, morning_df, evening_df, address, data_processor):
        """Находит отчет по адресу"""
        try:
            reports = data_processor.process_daily_reports(morning_df, evening_df)

            for report in reports:
                if report['normalized_address'] == data_processor.normalizer.normalize(address):
                    return report

            return None
        except Exception as e:
            logging.error(f"Ошибка поиска отчета по адресу: {e}")
            return None
    
    def format_detailed_report(self, report):
        """Форматирует детальный отчет для Telegram"""
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
    
    def format_summary_report(self, summary):
        """Форматирует сводный отчет для Telegram"""
        message = f"""
🏆 <b>СВОДНЫЙ ОТЧЕТ ЗА ДЕНЬ</b>

🏪 <b>Всего магазинов:</b> {summary['total_stores']} из {summary['expected_stores']}
💰 <b>Общий объем продаж:</b> {summary['total_sales']} шт.
📈 <b>Средняя эффективность:</b> {summary['average_efficiency']}%

⭐ <b>Лучшие показатели:</b>
🏙️ <b>Лучший город:</b> {summary['best_city']} ({summary['best_city_sales']} шт.)
🏢 <b>Лучшая сеть:</b> {summary['best_network']} ({summary['best_network_sales']} шт.)
👤 <b>Лучший сотрудник:</b> {summary['best_employee']} ({summary['best_employee_sales']} шт.)
🧀 <b>Лучшее SKU:</b> {summary['best_cheese']} ({summary['best_cheese_sales']} шт.)
"""

        # Добавляем уведомление о недостающих отчетах
        if summary['missing_reports'] > 0:
            message += f"\n⚠️ <b>Внимание:</b> Не получено {summary['missing_reports']} вечерних отчетов!"

        return message
