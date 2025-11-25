import pandas as pd
from datetime import datetime, date
from config import Config
from address_normalizer import AddressNormalizer
import logging
import re  # <-- Добавим регулярные выражения

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

        # --- ВАЖНО: Сделаем копии, чтобы избежать SettingWithCopyWarning ---
        morning_df = morning_df.copy()
        evening_df = evening_df.copy()

        logger.info(f"process_daily_reports: Starting with morning_df shape: {morning_df.shape}, evening_df shape: {evening_df.shape}")

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

        logger.info(f"process_daily_reports: morning_df count = {len(morning_df)}, evening_df count = {len(evening_df)}")

        # Проходим по вечерним отчётам и ищем утренние
        for idx_e, evening_row in evening_df.iterrows():
            try:
                # Конвертируем дату в date объект
                evening_date_raw = evening_row[self.config.EVENING_COLUMNS['date']]
                # Если уже datetime, просто извлекаем date
                if pd.api.types.is_datetime64_any_dtype(evening_df[self.config.EVENING_COLUMNS['date']]):
                    evening_date = pd.to_datetime(evening_date_raw).date()
                else:
                    # Если строка, парсим
                    try:
                        parsed_date = datetime.strptime(str(evening_date_raw), '%m/%d/%Y').date()
                    except ValueError:
                        try:
                            parsed_date = pd.to_datetime(evening_date_raw).date()
                        except:
                            logger.warning(f"process_daily_reports: Cannot parse evening date '{evening_date_raw}' for row {idx_e}, skipping.")
                            continue
                    evening_date = parsed_date

                evening_address = evening_row['normalized_address']
                evening_employee = evening_row['normalized_employee']
                evening_original_address = evening_row[self.config.EVENING_COLUMNS['address']]
                evening_original_employee = evening_row[self.config.EVENING_COLUMNS['employee_name']]

                logger.debug(f"process_daily_reports: Evening row {idx_e} for {evening_date}, '{evening_original_employee}' -> '{evening_employee}', '{evening_original_address}' -> '{evening_address}'")

                # Ищем соответствующий утренний отчет
                morning_match = None

                for idx_m, morning_row in morning_df.iterrows():
                    # Конвертируем дату утреннего отчёта
                    morning_date_raw = morning_row[self.config.MORNING_COLUMNS['date']]
                    if pd.api.types.is_datetime64_any_dtype(morning_df[self.config.MORNING_COLUMNS['date']]):
                        morning_date = pd.to_datetime(morning_date_raw).date()
                    else:
                        try:
                            parsed_date = datetime.strptime(str(morning_date_raw), '%m/%d/%Y').date()
                        except ValueError:
                            try:
                                parsed_date = pd.to_datetime(morning_date_raw).date()
                            except:
                                logger.warning(f"process_daily_reports: Cannot parse morning date '{morning_date_raw}' for row {idx_m}, skipping.")
                                continue
                        morning_date = parsed_date

                    morning_address = morning_row['normalized_address']
                    morning_employee = morning_row['normalized_employee']
                    morning_original_address = morning_row[self.config.MORNING_COLUMNS['address']]
                    morning_original_employee = morning_row[self.config.MORNING_COLUMNS['employee_name']]

                    logger.debug(f"process_daily_reports:     vs Morning row {idx_m} for {morning_date}, '{morning_original_employee}' -> '{morning_employee}', '{morning_original_address}' -> '{morning_address}'")

                    # Проверяем совпадение по дате, сотруднику и адресу
                    date_match = (morning_date == evening_date)
                    employee_match = (morning_employee == evening_employee)
                    address_match = self.normalizer.match_addresses(morning_address, evening_address)

                    logger.debug(f"process_daily_reports:         Date match: {date_match}, Employee match: {employee_match}, Address match: {address_match}")

                    if date_match and employee_match and address_match:
                        logger.debug(f"process_daily_reports: Match found for {idx_m} -> {idx_e} on {morning_date}, {morning_employee}, {morning_address}")
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
                            logger.info(f"process_daily_reports: Report generated for {report['employee']}, sales: {report['total_sales']}, visitors: {report['visitors']}, city: {report['city']}, network: {report['network']}")
                        else:
                            logger.warning(f"process_daily_reports: Report generation failed for {pair_key} (returned None)")
                    else:
                        logger.debug(f"process_daily_reports: Pair {pair_key} already processed, skipping.")

            except Exception as e:
                logger.error(f"process_daily_reports: Ошибка обработки вечернего отчета (index {idx_e}): {e}", exc_info=True)
                continue

        logger.info(f"process_daily_reports: Total reports generated = {len(reports)}")
        # Логируем суммарные данные
        if reports:
            total_sales_all = sum(r['total_sales'] for r in reports)
            total_visitors_all = sum(r['visitors'] for r in reports)
            logger.info(f"process_daily_reports: Summary - Total Sales: {total_sales_all}, Total Visitors: {total_visitors_all}")
        else:
            logger.info(f"process_daily_reports: Summary - Total Sales: 0, Total Visitors: 0 (no reports generated)")

        return reports

    def _generate_detailed_report(self, morning_row, evening_row):
        """Генерирует детальный отчет по одному магазину"""
        try:
            # Вычисляем продажи по каждому сыру
            sales_data = {}
            total_sales = 0

            logger.debug(f"_generate_detailed_report: Processing morning_row and evening_row...")
            logger.debug(f"_generate_detailed_report: Morning original data: {morning_row[self.config.MORNING_COLUMNS['employee_name']]}, {morning_row[self.config.MORNING_COLUMNS['address']]}")
            logger.debug(f"_generate_detailed_report: Evening original data: {evening_row[self.config.EVENING_COLUMNS['employee_name']]}, {evening_row[self.config.EVENING_COLUMNS['address']]}")

            for cheese in self.config.CHEESE_TYPES:
                start_col = self.config.MORNING_COLUMNS['cheese_start'][cheese]
                end_col = self.config.EVENING_COLUMNS['cheese_end'][cheese]

                # Безопасное преобразование значений с обработкой NaN и текстовых значений
                start_qty_raw = morning_row.get(start_col, 0)
                end_qty_raw = evening_row.get(end_col, 0)

                logger.debug(f"_generate_detailed_report: Cheese {cheese} - Start raw: '{start_qty_raw}', End raw: '{end_qty_raw}'")

                start_qty = self._safe_int_convert(start_qty_raw)
                end_qty = self._safe_int_convert(end_qty_raw)

                logger.debug(f"_generate_detailed_report: Cheese {cheese} - Start converted: {start_qty}, End converted: {end_qty}")

                if start_qty is None or end_qty is None:
                    logger.warning(f"_generate_detailed_report: Start or End quantity is None for {cheese}, skipping report generation.")
                    return None

                sold = max(0, start_qty - end_qty)  # Защита от отрицательных значений

                logger.debug(f"_generate_detailed_report: Cheese {cheese} - Sold calculated: {sold}")

                sales_data[cheese] = {
                    'start': start_qty,
                    'end': end_qty,
                    'sold': sold
                }
                total_sales += sold

            # Вычисляем эффективность
            visitors_raw = evening_row.get(self.config.EVENING_COLUMNS['visitors'], 0)
            logger.debug(f"_generate_detailed_report: Visitors raw: '{visitors_raw}'")
            visitors = self._safe_int_convert(visitors_raw)
            logger.debug(f"_generate_detailed_report: Visitors converted: {visitors}")

            if visitors is None:
                logger.warning(f"_generate_detailed_report: Visitors is None, skipping report generation.")
                return None

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

            # Проверяем, что все значения числовые и не None
            if any(v is None for v in [total_sales, visitors, efficiency]):
                logger.warning(f"_generate_detailed_report: Invalid numeric value found: total_sales={total_sales}, visitors={visitors}, efficiency={efficiency}")
                return None

            report = {
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

            logger.debug(f"_generate_detailed_report: Final report - Sales: {total_sales}, Visitors: {visitors}, City: {report['city']}, Network: {report['network']}")
            return report
        except Exception as e:
            logger.error(f"_generate_detailed_report: Ошибка генерации детального отчета: {e}", exc_info=True)
            return None

    def _safe_int_convert(self, value):
        """Безопасное преобразование значения в int с обработкой NaN, текстовых значений, диапазонов и приближённых чисел"""
        try:
            if pd.isna(value) or value == '' or str(value).lower() in ['nan', 'нет', 'нету', 'не знаю', 'не указано', 'не было этого сыра', 'не было голландского сыра в магазине', 'отсутствуют', 'не было на продаже', 'не было на продажу']:
                logger.debug(f"_safe_int_convert: Returning 0 for NaN/empty/not_found: '{value}'")
                return 0
            # Если это строка, пытаемся извлечь числа
            if isinstance(value, str):
                # Приводим к нижнему регистру для проверки
                lower_val = value.lower().strip()
                # Проверяем на "все продано", "все ушло" и т.п.
                if 'все' in lower_val and ('продано' in lower_val or 'ушло' in lower_val or 'разобрали' in lower_val):
                    # Пытаемся найти начальный остаток из строки типа "было 10, все продано"
                    numbers_in_str = re.findall(r'\d+', value)
                    if numbers_in_str:
                        initial_stock = int(numbers_in_str[0]) # Берём первое найденное число как начальный остаток
                        logger.debug(f"_safe_int_convert: 'Все продано' detected, returning initial stock: {initial_stock} from '{value}'")
                        return initial_stock
                    else:
                        logger.debug(f"_safe_int_convert: 'Все продано' detected but no initial stock found, returning 0 from '{value}'")
                        return 0
                # Проверяем на "не было", "отсутствует" и т.п.
                if 'не ' in lower_val and ('было' in lower_val or 'оказалось' in lower_val):
                    logger.debug(f"_safe_int_convert: 'Не было' detected, returning 0 from '{value}'")
                    return 0
                # Ищем приближённые числа (≈60, примерно 30, ~45)
                approx_match = re.search(r'[≈~∼~∼~]\s*(\d+)|примерно\s+(\d+)|около\s+(\d+)|порядка\s+(\d+)', value, re.IGNORECASE)
                if approx_match:
                    found_num = next(filter(None, approx_match.groups()), None)
                    if found_num:
                        result = int(found_num)
                        logger.debug(f"_safe_int_convert: Approximate number '{value}' -> {result}")
                        return result
                # Ищем диапазоны (20-30, 15 -- 25, 10 до 20)
                range_match = re.search(r'(\d+)\s*[-–—−]\s*(\d+)|(\d+)\s+до\s+(\d+)', value, re.IGNORECASE)
                if range_match:
                    g = range_match.groups()
                    if g[0] is not None and g[1] is not None:
                        start, end = int(g[0]), int(g[1])
                    elif g[2] is not None and g[3] is not None:
                        start, end = int(g[2]), int(g[3])
                    else:
                        logger.warning(f"_safe_int_convert: Range regex matched but groups are invalid in '{value}'")
                        return 0
                    result = int((start + end) / 2)
                    logger.debug(f"_safe_int_convert: Range '{value}' -> {start}, {end} -> {result}")
                    return result
                # Ищем все числа в строке (для случаев "Много", "20-30 человек", "≈50")
                numbers = re.findall(r'\d+', value)
                if numbers:
                    # Берём первое найденное число как самое вероятное
                    result = int(numbers[0])
                    logger.debug(f"_safe_int_convert: String '{value}' -> first number {result}")
                    return result
                # Если не нашли чисел, но строка содержит "много", "много людей", "более 20" и т.п.
                if 'много' in lower_val:
                    # Можно вернуть условное "много", например 100, или 50, или None -> 0
                    # Пока вернём 50 как среднее
                    logger.debug(f"_safe_int_convert: String '{value}' -> 'много' -> 50")
                    return 50
                if 'более' in lower_val or 'больше' in lower_val:
                    more_match = re.search(r'(\d+)', lower_val)
                    if more_match:
                        base_num = int(more_match.group(1))
                        # "Более 20" -> вернём 20 + 10 (или какую-то эвристику)
                        result = base_num + 10
                        logger.debug(f"_safe_int_convert: String '{value}' -> 'более {base_num}' -> {result}")
                        return result
                if 'менее' in lower_val or 'меньше' in lower_val:
                    less_match = re.search(r'(\d+)', lower_val)
                    if less_match:
                        base_num = int(less_match.group(1))
                        result = max(0, base_num - 10)
                        logger.debug(f"_safe_int_convert: String '{value}' -> 'менее {base_num}' -> {result}")
                        return result
                # Если строка не содержит известных паттернов, возвращаем 0
                logger.debug(f"_safe_int_convert: String '{value}' does not match any known pattern, returning 0")
                return 0

            # Если не строка и не NaN, пытаемся преобразовать как обычно
            result = int(float(value))
            logger.debug(f"_safe_int_convert: Value '{value}' -> number {result}")
            return result
        except (ValueError, TypeError) as e:
            logger.warning(f"_safe_int_convert: Failed to convert value '{value}' to int, returning 0. Error: {e}")
            return 0

    def _normalize_employee_name(self, name):
        """Нормализует ФИО сотрудника для сопоставления"""
        if not name or pd.isna(name):
            return ""

        # Приводим к нижнему регистру и убираем лишние пробелы
        normalized = str(name).lower().strip()

        # Убираем лишние пробелы между словами
        normalized = re.sub(r'\s+', ' ', normalized)

        # Убираем знаки препинания, кроме дефиса в фамилиях
        normalized = re.sub(r'[^\w\s-]', '', normalized)

        return normalized.strip()

    def get_expected_reports_for_day(self, morning_df, evening_df, target_date):
        """Получает ожидаемое количество отчетов за день из утренних анкет"""
        if morning_df.empty:
            return 0

        # Приводим колонку даты к datetime64 для корректного использования .dt
        morning_df_local = morning_df.copy()
        date_col = self.config.MORNING_COLUMNS['date']
        if not pd.api.types.is_datetime64_any_dtype(morning_df_local[date_col]):
            # Если строка, парсим
            try:
                morning_df_local[date_col] = pd.to_datetime(morning_df_local[date_col], format='%m/%d/%Y', errors='coerce')
            except:
                morning_df_local[date_col] = pd.to_datetime(morning_df_local[date_col], errors='coerce')

        # Если target_date - datetime.date, конвертируем к pd.Timestamp для сравнения с datetime64
        if isinstance(target_date, (str, datetime.date)):
            target_date = pd.to_datetime(target_date)

        expected = len(morning_df_local[morning_df_local[date_col].dt.date == target_date.date()])
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