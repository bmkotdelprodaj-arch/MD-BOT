import os
import json
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Google Sheets
    # Если GOOGLE_CREDENTIALS_JSON задана, используем её, иначе файл
    if os.getenv('GOOGLE_CREDENTIALS_JSON'):
        import base64
        CREDENTIALS_JSON = json.loads(base64.b64decode(os.getenv('GOOGLE_CREDENTIALS_JSON')).decode('utf-8'))
    else:
        CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
        # Загружаем credentials из файла, если переменная окружения не задана
        try:
            with open(CREDENTIALS_PATH, 'r') as f:
                CREDENTIALS_JSON = json.load(f)
        except FileNotFoundError:
            CREDENTIALS_JSON = None

    MORNING_SHEET_ID = os.getenv('MORNING_SHEET_ID')
    EVENING_SHEET_ID = os.getenv('EVENING_SHEET_ID')
    
    # Telegram
    BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')  # ID группы
    
    # Cheese configuration
    CHEESE_TYPES = [
        "Лёгкий", "Голландский", "Тильзитер", "Манжерок"
    ]

    # Column mappings
    MORNING_COLUMNS = {
        'timestamp': 'Timestamp',
        'employee_name': 'ФИО промоутера',
        'city': 'Город',
        'network_name': 'Название сети',
        'date': 'Дата заполнения',
        'address': 'Адрес проведения дегустации',
        'cheese_start': {
            "Лёгкий": 'Укажите остатки на полке магазина сыра "Лёгкий"',
            "Голландский": 'Укажите остатки на полке магазина сыра "Голландский"',
            "Тильзитер": 'Укажите остатки на полке магазина сыра "Тильзитер"',
            "Манжерок": 'Укажите остатки на полке магазина сыра "Манжерок"'
        }
    }

    EVENING_COLUMNS = {
        'timestamp': 'Timestamp',
        'employee_name': 'ФИО промоутера',
        'date': 'Дата заполнения',
        'city': 'Город',
        'network_name': 'Сеть',
        'address': 'Адрес магазина',
        'visitors': 'Сколько человек поучаствовало в дегустации?',
        'cheese_end': {
            "Лёгкий": 'Остатки сыра "Лёгкий" на полке после дегустации (только цифры)',
            "Голландский": 'Остатки сыра "Голландский" на полке после дегустации (только цифры)',
            "Тильзитер": 'Остатки сыра "Тильзитер" на полке после дегустации (только цифры)',
            "Манжерок": 'Остатки сыра "Манжерок" на полке после дегустации(только цифры)'
        }
    }
    
    # Target conversion rate
    TARGET_CONVERSION = 0.5
    
    # Check interval (minutes)
    CHECK_INTERVAL = 5
    
    # End of day report time
    END_OF_DAY_TIME = "22:00"