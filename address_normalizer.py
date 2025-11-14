import re
from rapidfuzz import fuzz, process

class AddressNormalizer:
    def __init__(self):
        # Канонические названия магазинов (нужно заполнить реальными адресами)
        self.canonical_addresses = [
            "г. Москва, ул. Ленина, 10",
            "г. Санкт-Петербург, Невский пр., 15",
            "г. Екатеринбург, ул. Вайнера, 12",
            "г. Новосибирск, Красный пр., 25",
            "г. Казань, ул. Баумана, 5",
            "г. Нижний Новгород, ул. Большая Покровская, 8",
            "г. Челябинск, ул. Кирова, 15",
            "г. Омск, ул. Ленина, 20",
            "г. Самара, ул. Куйбышева, 12",
            "г. Ростов-на-Дону, ул. Большая Садовая, 18",
            # Добавить больше адресов по мере необходимости
        ]
        
        # Словарь замен для нормализации
        self.replacement_rules = {
            r'ул\.|улица': 'ул.',
            r'пр\.|проспект': 'пр.',
            r'г\.|город': 'г.',
            r'тц|торговый центр': 'ТЦ',
            r'\s+': ' ',  # Убираем лишние пробелы
        }
    
    def normalize(self, raw_address):
        """Нормализует адрес к стандартному формату"""
        if not raw_address:
            return ""
        
        # Приводим к нижнему регистру
        address = raw_address.lower().strip()
        
        # Применяем правила замены
        for pattern, replacement in self.replacement_rules.items():
            address = re.sub(pattern, replacement, address)
        
        # Убираем лишние символы
        address = re.sub(r'[^\w\s\.,-]', '', address)
        
        # Находим наиболее похожий канонический адрес
        if self.canonical_addresses:
            result = process.extractOne(address, self.canonical_addresses, 
                                      scorer=fuzz.token_sort_ratio)
            if result and result[1] > 80:  # 80% схожести
                return result[0]
        
        return address.strip()
    
    def match_addresses(self, address1, address2):
        """Проверяет, являются ли два адреса одним и тем же"""
        norm1 = self.normalize(address1)
        norm2 = self.normalize(address2)
        similarity = fuzz.token_sort_ratio(norm1, norm2)
        return similarity > 85  # Порог схожести 85%