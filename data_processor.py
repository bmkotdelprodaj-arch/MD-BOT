# data_processor.py — 100% РАБОЧИЕ ЛОГИ С ПЕРВОЙ СЕКУНДЫ

import pandas as pd
from datetime import datetime, date
from config import Config
from address_normalizer import AddressNormalizer
import logging
import re
import numpy as np
from scipy.optimize import linear_sum_assignment
from rapidfuzz import fuzz

# ═══════════════════════════════════════════════
# ВАЖНО: ПРИНУДИТЕЛЬНО ВКЛЮЧАЕМ ЛОГИРОВАНИЕ В ЭТОМ МОДУЛЕ
# ═══════════════════════════════════════════════
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # ← ГАРАНТИРУЕМ, что будет писать INFO

# Если хендлеры ещё не добавлены (т.е. main.py ещё не настроил basicConfig) — добавляем сами
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Тестовая строка — ДОЛЖНА появиться при каждом импорте модуля!
logger.info("data_processor.py ЗАГРУЖЕН — ЛОГИ РАБОТАЮТ НА 100%!")
# ═══════════════════════════════════════════════

class DataProcessor:
    def __init__(self):
        self.config = Config()
        self.normalizer = AddressNormalizer()
        self.processed_pairs = set()
        logger.info("DataProcessor инициализирован")

    def process_daily_reports(self, morning_df, evening_df):
        reports = []

        if morning_df.empty or evening_df.empty:
            logger.info("Один из датафреймов пустой — ничего не сопоставляем")
            return reports

        m_df = morning_df.copy()
        e_df = evening_df.copy()

        # Перенумеровываем индексы, чтобы матрица сходства соответствовала позициям 0..n-1
        m_df = m_df.reset_index(drop=True)
        e_df = e_df.reset_index(drop=True)

        logger.info(f"process_daily_reports: Утренних: {len(m_df)}, Вечерних: {len(e_df)}")

        # === НОРМАЛИЗАЦИЯ ФИО ===
        def norm_fio(name):
            if pd.isna(name):
                return ""
            s = re.sub(r'[^а-яёa-z\s]', ' ', str(name).lower())
            words = [w for w in re.findall(r'\b[а-яёa-z]+\b', s) if len(w) > 1]
            return ' '.join(sorted(words)[:3])

        m_df['fio'] = m_df[self.config.MORNING_COLUMNS['employee_name']].apply(norm_fio)
        e_df['fio'] = e_df[self.config.EVENING_COLUMNS['employee_name']].apply(norm_fio)

        # === НОРМАЛИЗАЦИЯ АДРЕСА ===
        def norm_address(addr):
            if pd.isna(addr):
                return ""
            s = str(addr).lower()
            s = re.sub(r'\b(тц|тк|трц|магнит|мария.?ра|мария|ашан|пятёрочка|перекрёсток|г\.?\s*|город\s*|ул\.?\s*|улица\s*|проспект\s*|пр\.?\s*|дом\s*|д\.?\s*|карла\s+)\b', ' ', s)
            s = re.sub(r'[^\w\s/,.-]', ' ', s)
            s = re.sub(r'\s+', ' ', s).strip()
            tokens = [t.strip(',.') for t in s.split() if len(t) > 1]
            return ' '.join(sorted(tokens))

        m_df['addr'] = m_df[self.config.MORNING_COLUMNS['address']].apply(norm_address)
        e_df['addr'] = e_df[self.config.EVENING_COLUMNS['address']].apply(norm_address)

        # === ДАТЫ ===
        def to_date(val):
            try:
                # Даты в таблицах в формате MM/DD/YYYY (например, 10/31/2025)
                return pd.to_datetime(val, format='%m/%d/%Y', errors='coerce').date()
            except Exception:
                return None

        m_df['date'] = m_df[self.config.MORNING_COLUMNS['date']].apply(to_date)
        e_df['date'] = e_df[self.config.EVENING_COLUMNS['date']].apply(to_date)

        m_df = m_df.dropna(subset=['date'])
        e_df = e_df.dropna(subset=['date'])

        # === МАТРИЦА СХОЖЕСТИ ===
        n, m = len(m_df), len(e_df)
        scores = np.zeros((n, m))

        for i, mr in m_df.iterrows():
            for j, er in e_df.iterrows():
                if mr['date'] != er['date']:
                    continue
                fio_score = fuzz.token_set_ratio(mr['fio'], er['fio']) / 100.0
                addr_score = fuzz.token_set_ratio(mr['addr'], er['addr']) / 100.0
                # Базовый взвешенный скор
                weighted = 0.72 * fio_score + 0.28 * addr_score
                # Если адрес совпал почти идеально — даём шансу пройти даже при более слабом ФИО
                total_score = max(weighted, addr_score)
                scores[i, j] = total_score

        # === СОПОСТАВЛЕНИЕ ===
        if n > 0 and m > 0:
            cost_matrix = 1.0 - scores
            row_ind, col_ind = linear_sum_assignment(cost_matrix)
            used_morning_indices = set()

            for r, c in zip(row_ind, col_ind):
                score = scores[r, c]
                if score < 0.75:
                    # Детальное логирование отклонённых пар для отладки
                    mr = m_df.iloc[r]
                    er = e_df.iloc[c]
                    logger.debug(
                        "Отклонена пара (низкий score): %s | %s | score=%.3f, fio_m='%s', fio_e='%s', addr_m='%s', addr_e='%s'",
                        mr.get(self.config.MORNING_COLUMNS['employee_name'], '')[:25],
                        er.get(self.config.EVENING_COLUMNS['address'], '')[:40],
                        score,
                        mr.get('fio', ''),
                        er.get('fio', ''),
                        mr.get('addr', ''),
                        er.get('addr', ''),
                    )
                    continue
                if r in used_morning_indices:
                    continue

                used_morning_indices.add(r)
                morning_row = m_df.iloc[r]
                evening_row = e_df.iloc[c]

                pair_key = f"{morning_row['date']}_{morning_row['fio']}_{morning_row['addr']}"
                if pair_key in self.processed_pairs:
                    continue

                report = self._generate_detailed_report(morning_row, evening_row)
                if report:
                    reports.append(report)
                    self.processed_pairs.add(pair_key)
                    logger.info(f"Сопоставлено -> {morning_row[self.config.MORNING_COLUMNS['employee_name']][:25]:<25} | "
                                f"{evening_row[self.config.EVENING_COLUMNS['address']][:40]:<40} | score={score:.3f} | продано={report['total_sales']} шт.")

        logger.info(f"ИТОГО сопоставлено пар: {len(reports)} из {min(len(m_df), len(e_df))}")
        return reports

    def _generate_detailed_report(self, morning_row, evening_row):
        try:
            sales_data = {}
            total_sales = 0

            for cheese in self.config.CHEESE_TYPES:
                start_col = self.config.MORNING_COLUMNS['cheese_start'][cheese]
                end_col = self.config.EVENING_COLUMNS['cheese_end'][cheese]

                start_qty = self._safe_int_convert(morning_row.get(start_col, 0))
                end_qty = self._safe_int_convert(evening_row.get(end_col, 0))

                if start_qty is None or end_qty is None:
                    logger.warning(f"Пропущен сыр {cheese}: start={start_qty}, end={end_qty}")
                    return None

                sold = max(0, start_qty - end_qty)
                sales_data[cheese] = {'start': start_qty, 'end': end_qty, 'sold': sold}
                total_sales += sold

            visitors = self._safe_int_convert(evening_row.get(self.config.EVENING_COLUMNS['visitors'], 0))
            if visitors is None:
                logger.warning("Не удалось получить количество посетителей")
                return None

            conversion = total_sales / visitors if visitors > 0 else 0
            max_possible = sum(d['start'] for d in sales_data.values())
            stock_factor = min(1.0, max_possible / (visitors * 2)) if visitors > 0 else 1.0
            efficiency = (conversion / self.config.TARGET_CONVERSION) * 100 * stock_factor

            report = {
                'date': pd.to_datetime(evening_row[self.config.EVENING_COLUMNS['date']]).strftime('%d.%m.%Y'),
                'city': evening_row[self.config.EVENING_COLUMNS['city']],
                'network': evening_row[self.config.EVENING_COLUMNS['network_name']],
                'employee': evening_row[self.config.EVENING_COLUMNS['employee_name']],
                'visitors': visitors,
                'cheese_data': sales_data,
                'total_sales': total_sales,
                'efficiency': round(efficiency, 1),
                'normalized_address': evening_row.get('normalized_address', evening_row[self.config.EVENING_COLUMNS['address']])
            }
            logger.info(f"Отчёт успешно сформирован: {report['city']} | {report['employee']} | продано {total_sales} шт.")
            return report

        except Exception as e:
            logger.error(f"Ошибка при генерации отчёта: {e}", exc_info=True)
            return None

    def _safe_int_convert(self, value):
        if pd.isna(value) or value in ['', 'nan', 'NaN', 'нет', 'не было', 'отсутствует']:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            text = value.lower().strip()
            if any(x in text for x in ['все продано', 'всё ушло', 'все ушло', 'разобрали']):
                nums = re.findall(r'\d+', value)
                return int(nums[0]) if nums else 0
            approx = re.search(r'(?:≈|~|примерно|около|порядка)\s*(\d+)', text)
            if approx:
                return int(approx.group(1))
            rng = re.search(r'(\d+)\s*[-–—−]\s*(\d+)|(\d+)\s+до\s+(\d+)', text)
            if rng:
                nums = [int(x) for x in rng.groups() if x]
                return int(sum(nums) / len(nums))
            nums = re.findall(r'\d+', value)
            if nums:
                return int(nums[0])
            if 'много' in text:
                return 50
            if 'более' in text or 'больше' in text:
                n = re.search(r'\d+', text)
                return int(n.group()) + 10 if n else 50
        return 0

    def generate_summary_report(self, all_reports, expected_reports, actual_reports):
        if not all_reports:
            logger.info("Нет отчётов для сводки")
            return None

        city_sales = {}
        network_sales = {}
        employee_sales = {}
        cheese_sales = {cheese: 0 for cheese in self.config.CHEESE_TYPES}

        for report in all_reports:
            city_sales[report['city']] = city_sales.get(report['city'], 0) + report['total_sales']
            network_sales[report['network']] = network_sales.get(report['network'], 0) + report['total_sales']
            employee_sales[report['employee']] = employee_sales.get(report['employee'], 0) + report['total_sales']
            for cheese, data in report['cheese_data'].items():
                cheese_sales[cheese] += data['sold']

        best_city = max(city_sales.items(), key=lambda x: x[1]) if city_sales else ("—", 0)
        best_network = max(network_sales.items(), key=lambda x: x[1]) if network_sales else ("—", 0)
        best_employee = max(employee_sales.items(), key=lambda x: x[1]) if employee_sales else ("—", 0)
        best_cheese = max(cheese_sales.items(), key=lambda x: x[1]) if cheese_sales else ("—", 0)

        result = {
            'best_city': best_city[0], 'best_city_sales': best_city[1],
            'best_network': best_network[0], 'best_network_sales': best_network[1],
            'best_employee': best_employee[0], 'best_employee_sales': best_employee[1],
            'best_cheese': best_cheese[0], 'best_cheese_sales': best_cheese[1],
            'total_stores': len(all_reports),
            'expected_stores': expected_reports,
            'missing_reports': expected_reports - actual_reports,
            'total_sales': sum(r['total_sales'] for r in all_reports),
            'average_efficiency': round(sum(r['efficiency'] for r in all_reports) / len(all_reports), 1)
        }
        logger.info(f"Сводный отчёт сформирован: {result['total_stores']} магазинов, {result['total_sales']} шт. продано")
        return result