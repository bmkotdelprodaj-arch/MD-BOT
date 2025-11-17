import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class Config:
    # Google Sheets: загрузка учётных данных
    CREDENTIALS_JSON = None
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

    # Сначала пробуем GOOGLE_CREDENTIALS_JSON (base64-encoded JSON)
    google_creds_b64 = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if google_creds_b64:
        try:
            decoded = base64.b64decode(google_creds_b64).decode('utf-8')
            CREDENTIALS_JSON = json.loads(decoded)
            logger.info("Loaded credentials from GOOGLE_CREDENTIALS_JSON")
        except Exception as e:
            logger.error(f"Failed to decode GOOGLE_CREDENTIALS_JSON: {e}")
    else:
        # Иначе — из файла
        try:
            with open(CREDENTIALS_PATH, 'r') as f:
                CREDENTIALS_JSON = json.load(f)
            logger.info(f"Loaded credentials from {CREDENTIALS_PATH}")
        except FileNotFoundError:
            logger.error(f"credentials file not found: {CREDENTIALS_PATH}")
        except Exception as e:
            logger.error(f"Error loading credentials from file: {e}")

    # 🔥 КРИТИЧЕСКАЯ НОРМАЛИЗАЦИЯ: всегда заменяем \\n → \n, даже если их нет
    if CREDENTIALS_JSON and "private_key" in CREDENTIALS_JSON:
        original = CREDENTIALS_JSON["private_key"]
        normalized = original.replace("\\n", "\n")
        if normalized != original:
            logger.debug("Normalized private_key: replaced \\n with actual newlines")
        CREDENTIALS_JSON["private_key"] = normalized

    # Проверка: если всё ещё нет credentials — критическая ошибка
    if not CREDENTIALS_JSON:
        logger.critical("❌ NO GOOGLE CREDENTIALS FOUND! Set GOOGLE_CREDENTIALS_JSON or provide credentials.json")
        raise RuntimeError("Google credentials are missing")

    # --- Остальные параметры ---
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
