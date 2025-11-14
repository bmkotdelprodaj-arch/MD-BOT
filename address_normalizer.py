import re
from rapidfuzz import fuzz, process

class AddressNormalizer:
    def __init__(self):
        # Канонические названия магазинов (нужно заполнить)
        self.canonical_addresses = [
            "г. Москва, ул. Ленина, 10",
            "г. Санкт-Петербург, Невский пр., 15",
            # ... другие адреса
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