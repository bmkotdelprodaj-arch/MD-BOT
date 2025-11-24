import os
import json
import base64
import logging
from dotenv import load_dotenv

# 🔑 КРИТИЧЕСКИ: загружаем .env ДО определения класса Config
# На Render .env нет, но load_dotenv() безопасен — просто пропустит.
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    # === 1. Загрузка учётных данных ===
    CREDENTIALS_JSON = None
    CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

    # --- Попытка 1: из GOOGLE_CREDENTIALS_JSON (base64-encoded JSON) ---
    google_creds_b64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if google_creds_b64:
        try:
            decoded_bytes = base64.b64decode(google_creds_b64)
            decoded_str = decoded_bytes.decode("utf-8")
            CREDENTIALS_JSON = json.loads(decoded_str)
            logger.info("✅ Loaded credentials from GOOGLE_CREDENTIALS_JSON")
        except Exception as e:
            logger.error(f"❌ Failed to decode/parse GOOGLE_CREDENTIALS_JSON: {e}")
            logger.debug(f"Raw base64 length: {len(google_creds_b64)} chars")

    # --- Попытка 2: из файла (только если GOOGLE_CREDENTIALS_JSON не задан или сломан) ---
    if not CREDENTIALS_JSON:
        try:
            with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
                CREDENTIALS_JSON = json.load(f)
            logger.info(f"✅ Loaded credentials from file: {CREDENTIALS_PATH}")
        except FileNotFoundError:
            logger.error(f"❌ File not found: {CREDENTIALS_PATH}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {CREDENTIALS_PATH}: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error loading file: {e}")

    # === 2. Нормализация private_key (обязательно!) ===
    if CREDENTIALS_JSON and "private_key" in CREDENTIALS_JSON:
        orig_key = CREDENTIALS_JSON["private_key"]
        # Заменяем ВСЕ \\n → \n (даже если их нет — безопасно)
        norm_key = orig_key.replace("\\n", "\n")
        CREDENTIALS_JSON["private_key"] = norm_key

        # Диагностика
        logger.info(f"🔑 private_key length: {len(norm_key)} chars")
        preview = repr(norm_key[:50])
        logger.debug(f"🔑 private_key preview: {preview}")
        if "\\n" in norm_key:
            logger.warning("⚠️  private_key still contains literal '\\\\n' — normalization may have failed!")
        else:
            logger.info("✅ private_key normalized (no literal \\n left)")

    # === 3. Критическая проверка: credentials должны быть! ===
    if not CREDENTIALS_JSON:
        logger.critical("💥 CRITICAL: No Google credentials available!")
        logger.critical("→ Set GOOGLE_CREDENTIALS_JSON (base64) in Render Env")
        logger.critical("→ OR provide valid 'credentials.json' in repo (not recommended)")
        raise RuntimeError("Google credentials missing — see logs above")

    # === 4. Остальные настройки ===
    MORNING_SHEET_ID = os.getenv("MORNING_SHEET_ID")
    EVENING_SHEET_ID = os.getenv("EVENING_SHEET_ID")
    MORNING_SHEET_NAME = "Form Responses 1"
    EVENING_SHEET_NAME = "Form Responses 1"

    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # ID группы

    CHEESE_TYPES = ["Лёгкий", "Голландский", "Тильзитер", "Манжерок"]

    MORNING_COLUMNS = {
        "timestamp": "Timestamp",
        "employee_name": "ФИО промоутера",
        "city": "Город",
        "network_name": "Название сети",
        "date": "Дата заполнения",
        "address": "Адрес проведения дегустации",
        "cheese_start": {
            "Лёгкий": 'Укажите остатки на полке магазина сыра "Лёгкий"',
            "Голландский": 'Укажите остатки на полке магазина сыра "Голландский"',
            "Тильзитер": 'Укажите остатки на полке магазина сыра "Тильзитер"',
            "Манжерок": 'Укажите остатки на полке магазина сыра "Манжерок"',
        },
    }

    EVENING_COLUMNS = {
        "timestamp": "Timestamp",
        "employee_name": "ФИО промоутера",
        "date": "Дата заполнения",
        "city": "Город",
        "network_name": "Сеть",
        "address": "Адрес магазина",
        "visitors": "Сколько человек поучаствовало в дегустации?",
        "cheese_end": {
            "Лёгкий": 'Остатки сыра "Лёгкий" на полке после дегустации (только цифры)',
            "Голландский": 'Остатки сыра "Голландский" на полке после дегустации (только цифры)',
            "Тильзитер": 'Остатки сыра "Тильзитер" на полке после дегустации (только цифры)',
            "Манжерок": 'Остатки сыра "Манжерок" на полке после дегустации(только цифры)',
        },
    }

    TARGET_CONVERSION = 0.5
    CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))
    END_OF_DAY_TIME = os.getenv("END_OF_DAY_TIME", "22:00")

    def __init__(self):
        for key, value in self.__class__.__dict__.items():
            if not key.startswith("__") and not callable(value):
                setattr(self, key, value)
