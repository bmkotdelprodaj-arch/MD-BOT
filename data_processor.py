import pandas as pd
from datetime import datetime, date
from config import Config
from address_normalizer import AddressNormalizer
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.config = Config()
        self.normalizer = AddressNormalizer()
        self.processed_pairs = set()  # Для отслеживания обработанных пар
        
    def process_daily_reports(self, morning_df, evening_df):
        """
        Обрабатывает пары отчетов и возвращает готовые к отправке отчеты
        """
        reports = []

        # Нормализуем адреса в обоих датафреймах
        morning_df['normalized_address'] = morning_df[self.config.MORNING_COLUMNS['address']].apply(
            lambda x: self.normalizer.normalize(str(x))
        )
        evening_df['normalized_address'] = evening_df[self.config.EVENING_COLUMNS['address']].apply(
            lambda x: self.normalizer.normalize(str(x))
        )

        # Нормализуем ФИО сотрудников
        morning_df['normalized_employee'] = morning_df[self.config.MORNING_COLUMNS['employee_name']].apply(
            lambda x: self._normalize_employee_name(str(x))
        )
        evening_df['normalized_employee'] = evening_df[self.config.EVENING_COLUMNS['employee_name']].apply(
            lambda x: self._normalize_employee_name(str(x))
        )

        # Группируем по дате и адресу
        for _, evening_row in evening_df.iterrows():
            try:
                evening_date = pd.to_datetime(evening_row[self.config.EVENING_COLUMNS['date']]).date()
                evening_address = evening_row['normalized_address']
                evening_employee = evening_row['normalized_employee']

                # Ищем соответствующий утренний отчет
                morning_match = None

                for _, morning_row in morning_df.iterrows():
                    morning_date = pd.to_datetime(morning_row[self.config.MORNING_COLUMNS['date']]).date()
                    morning_address = morning_row['normalized_address']
                    morning_employee = morning_row['normalized_employee']

                    # Проверяем совпадение по дате, сотруднику и адресу
                    if (morning_date == evening_date and
                        morning_employee == evening_employee and
                        self.normalizer.match_addresses(morning_address, evening_address)):

                        morning_match = morning_row
                        break

                if morning_match is not None:
                    # Создаем уникальный ключ для отслеживания
                    pair_key = f"{evening_date}_{evening_address}_{evening_employee}"

                    if pair_key not in self.processed_pairs:
                        report = self._generate_detailed_report(morning_match, evening_row)
                        if report:  # Проверяем, что отчет сгенерирован успешно
                            reports.append(report)
                            self.processed_pairs.add(pair_key)

            except Exception as e:
                logger.error(f"Ошибка обработки вечернего отчета: {e}")
                continue

        return reports
    
    def _generate_detailed_report(self, morning_row, evening_row):
        """Генерирует детальный отчет по одному магазину"""
        try:
            # Вычисляем продажи по каждому сыру
            sales_data = {}
            total_sales = 0

            for cheese in self.config.CHEESE_TYPES:
                start_col = self.config.MORNING_COLUMNS['cheese_start'][cheese]
                end_col = self.config.EVENING_COLUMNS['cheese_end'][cheese]

                # Безопасное преобразование значений с обработкой NaN и текстовых значений
                start_qty = self._safe_int_convert(morning_row.get(start_col, 0))
                end_qty = self._safe_int_convert(evening_row.get(end_col, 0))
                sold = max(0, start_qty - end_qty)  # Защита от отрицательных значений

                sales_data[cheese] = {
                    'start': start_qty,
                    'end': end_qty,
                    'sold': sold
                }
                total_sales += sold

            # Вычисляем эффективность
            visitors = self._safe_int_convert(evening_row.get(self.config.EVENING_COLUMNS['visitors'], 0))
            conversion = total_sales / visitors if visitors > 0 else 0

            # Максимально возможные продажи (ограничены остатками)
            max_possible_sales = sum(sales_data[cheese]['start'] for cheese in self.config.CHEESE_TYPES)

            # Улучшенная формула эффективности
            # Базовая конверсия: продажи / посетители
            base_conversion = total_sales / visitors if visitors > 0 else 0

            # Корректировка на основе остатков: если остатков было мало, эффективность выше
            stock_factor = min(1.0, max_possible_sales / (visitors * 2)) if visitors > 0 else 1.0  # Предполагаем 2 сыра на посетителя

            # Итоговая эффективность: (базовая конверсия / целевая) * 100 * stock_factor
            if base_conversion > 0:
                efficiency = (base_conversion / self.config.TARGET_CONVERSION) * 100 * stock_factor
            else:
                efficiency = 0

            return {
                'date': pd.to_datetime(evening_row[self.config.EVENING_COLUMNS['date']]).strftime('%d.%m.%Y'),
                'city': evening_row[self.config.EVENING_COLUMNS['city']],
                'network': evening_row[self.config.EVENING_COLUMNS['network_name']],
                'employee': evening_row[self.config.EVENING_COLUMNS['employee_name']],
                'visitors': visitors,
                'cheese_data': sales_data,
                'total_sales': total_sales,
                'efficiency': round(efficiency, 1),
                'normalized_address': evening_row['normalized_address']
            }
        except Exception as e:
            logger.error(f"Ошибка генерации детального отчета: {e}")
            return None

    def _safe_int_convert(self, value):
        """Безопасное преобразование значения в int с обработкой NaN и текстовых значений"""
        try:
            if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'нет', 'нету', 'не знаю', 'не указано']:
                return 0
            # Если это строка, пытаемся извлечь числа
            if isinstance(value, str):
                import re
                numbers = re.findall(r'\d+', value)
                return int(numbers[0]) if numbers else 0
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def _normalize_employee_name(self, name):
        """Нормализует ФИО сотрудника для сопоставления"""
        if not name or pd.isna(name):
            return ""

        # Приводим к нижнему регистру и убираем лишние пробелы
        normalized = str(name).lower().strip()

        # Убираем лишние пробелы между словами
        import re
        normalized = re.sub(r'\s+', ' ', normalized)

        # Убираем знаки препинания, кроме дефиса в фамилиях
        normalized = re.sub(r'[^\w\s-]', '', normalized)

        return normalized.strip()
    
    def get_expected_reports_for_day(self, morning_df, evening_df, target_date):
        """Получает ожидаемое количество отчетов за день из утренних анкет"""
        if morning_df.empty:
            return 0

        # Приводим колонку даты к datetime64 для корректного использования .dt
        morning_df['date'] = pd.to_datetime(morning_df['date'])

        # Если target_date - datetime.date, конвертируем к pd.Timestamp для сравнения с datetime64
        if isinstance(target_date, (str, datetime.date)):
            target_date = pd.to_datetime(target_date)

        expected = len(morning_df[morning_df['date'].dt.date == target_date.date()])
        return expected

    def generate_summary_report(self, all_reports, expected_reports, actual_reports):
        """Генерирует сводный отчет за день"""
        if not all_reports:
            return None

        # Собираем данные для анализа
        city_sales = {}
        network_sales = {}
        employee_sales = {}
        cheese_sales = {cheese: 0 for cheese in self.config.CHEESE_TYPES}

        for report in all_reports:
            # По городам
            city = report['city']
            city_sales[city] = city_sales.get(city, 0) + report['total_sales']

            # По сетям
            network = report['network']
            network_sales[network] = network_sales.get(network, 0) + report['total_sales']

            # По сотрудникам
            employee = report['employee']
            employee_sales[employee] = employee_sales.get(employee, 0) + report['total_sales']

            # По сырам
            for cheese, data in report['cheese_data'].items():
                cheese_sales[cheese] += data['sold']

        # Находим лучших
        best_city = max(city_sales.items(), key=lambda x: x[1]) if city_sales else ("Нет данных", 0)
        best_network = max(network_sales.items(), key=lambda x: x[1]) if network_sales else ("Нет данных", 0)
        best_employee = max(employee_sales.items(), key=lambda x: x[1]) if employee_sales else ("Нет данных", 0)
        best_cheese = max(cheese_sales.items(), key=lambda x: x[1]) if cheese_sales else ("Нет данных", 0)

        # Проверяем недостающие отчеты
        missing_reports = expected_reports - actual_reports

        return {
            'best_city': best_city[0],
            'best_city_sales': best_city[1],
            'best_network': best_network[0],
            'best_network_sales': best_network[1],
            'best_employee': best_employee[0],
            'best_employee_sales': best_employee[1],
            'best_cheese': best_cheese[0],
            'best_cheese_sales': best_cheese[1],
            'total_stores': len(all_reports),
            'expected_stores': expected_reports,
            'missing_reports': missing_reports,
            'total_sales': sum(r['total_sales'] for r in all_reports),
            'average_efficiency': round(sum(r['efficiency'] for r in all_reports) / len(all_reports), 1) if all_reports else 0
        }
