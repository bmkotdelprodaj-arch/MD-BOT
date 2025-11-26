# config.py — ФИНАЛЬНЫЙ, РАБОЧИЙ СЕЙЧАС ЖЕ!
import os
import json
import base64
from dotenv import load_dotenv

load_dotenv()

# === GOOGLE CREDENTIALS — РАБОТАЕТ И НА RENDER, И ЛОКАЛЬНО ===
raw_creds = os.getenv("GOOGLE_CREDENTIALS_JSON")

if raw_creds:
    # На Render / в окружении — берём из переменной (base64 → JSON)
    CREDENTIALS_JSON = json.loads(base64.b64decode(raw_creds).decode("utf-8"))
else:
    # ЛОКАЛЬНО — используем переданный JSON сервисного аккаунта
    CREDENTIALS_JSON = {
        "type": "service_account",
        "project_id": "degustation-bot",
        "private_key_id": "6de644277ecd879db5693a5e710f022c030c7771",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDOG9MwULeFadyR\n43YDSANExvXS/RnMzjhsy74reFwsKKptjz0oEnB3TDWsC4hsfoSDdxfqoekdX+he\nO0ke4etICKrFT0WAoVQ5LjVnsIbSPmq/S2Bq5MQbFKpl+DTudP1tUwY0OM701/n2\nbccuRQt1eIYk84lMO3tcSTCN7uJE4fNeI78SXpvG1kAL82OAexG7c//jx30Hy/0P\nvYPfjvW7TYLd0FgT0J9mLKJWa45eWgy9cZb+f+O8agdOzh60N0HAxORHYJ1KV9rD\nRBALvkLY7vmAvdX/7NnS9thQZU9AtnpnjZnruwBbx0f3nCJXpnDu/Fx8J8Lf8nNG\nlyCbdc8bAgMBAAECggEAPciWgpW3pb0Jmwvp3Orx2+SeXPx1BvFMdcSPgZ1nV+9q\nw0Pca843O3OzZRrh7s/wLi59k16ssBsHX3P7I9BKRMIgWtw78+aohimFZctDZvPB\n8Q0J3pEr/ggwWjfQuiiErYhlRDNBSqERc58NxZ9542FZKOt+HUV5Yb0o2mshFc9x\n91gWnCgqV2jJEKGyfXCYWKHLF56YhiWJOTqpTWYHXVb+hkULQIrIPVEqBbDgX4SY\nZfETlOpxHj25JB55THL8FEiScfdcDq5QwplXhI+PnhyxiRfRtEZhuevsLb1jPERV\ntiIgHxqZm1G+SQLPUoJNUjFuqa61NU/QsqBwrf2qlQKBgQDpFcl66XNp221wK/dg\nhDAhYfK7P7KUiUen1IzSk9MglttMYv4W57nKP3nKVBZ/VWDPwAl5ll1gEBTMIyFy\nP0Z5y5ZpQNxpoHImsZsD0tNRL7QRcS0aMXVC6XDA4zc0kVBEFjJWmVoSf3lOB75v\nmuVOxb/xedTrMiOVe/sCCcZuRQKBgQDiXxqd91NlryO3MgLP71USv5nPHaApbKTn\n0dfBeRcZHReNNDf2BypcxQ1g6hbmcDzqayq7bvnMcuNQ3oXrrXVt9LDg6FotNnoi\nKzZ3AYP6OFu3cEHQbULwFfHHNlNJ15yn5xdNmalkQtPmmiqkSdAnYvW9aLCqvB9O\nc5Snuj1N3wKBgAI2ULk36f6BNKKx6CEkYAiRE2qvihRa0TFIlSWSfZh7lg09i0fQ\nXzUKMS+4aJZAyzBtlClxQeWdgXUGiFS7QQH66qyJDw55lVvEp8H7iEHnAACCYHk1\nKvW9nXJXNbjsACUFSOkFf5NZXl9dN3N7tt1uhwnVrC2nRisvCAzW5uk5AoGAYrNy\n6JNxMnHl0belkEJbRpxyFONm9Qjg8g8F0t7YWmPb4/5zs/s11lM43RvUNk+Tz+ZA\nMI9XrXQedyE/JR5xiCrbgITu5K3PHNzYl5T6lXxDST1SBO3O9N2smN6v/A9dx7uz\n5mrsd8HHoNFxbEae0r2MRjTCav8xa0Rr+aHznuMCgYBTD3ea88z9/ZRsbZwLuZNE\n9EJjHqoxEcPDcdF4SfevQaHD3B5GIl22x2cgO6MUvE7nzyZRCtUdTzJtc/aaGZZd\n+vItCPoTsMgsigVONxLo4XAODeytVwdZx4aPE0xVc+9T0/PXgTTeAPZAarDe5IrS\nvJH7uByBlm/uxXUK8DKLww==\n-----END PRIVATE KEY-----\n",
        "client_email": "degustation-bot@degustation-bot.iam.gserviceaccount.com",
        "client_id": "103091089365384606415",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/degustation-bot%40degustation-bot.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
    }

# Фиксим переносы в private_key (если они экранированы как `\\n`)
if "\\n" in CREDENTIALS_JSON["private_key"]:
    CREDENTIALS_JSON["private_key"] = CREDENTIALS_JSON["private_key"].replace("\\n", "\n")

# === КЛАСС С ВСЕМИ НАСТРОЙКАМИ ===
class Config:
    GOOGLE_CREDENTIALS = CREDENTIALS_JSON

    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    MORNING_SHEET_ID = "1Kmwgh4bH6O8ghyK_FOGMAqoHcWRbIA5DVLBC_6YhX8U"
    EVENING_SHEET_ID = "1cLaeC_pWTIMUZuy5FGoOrQi7xsNX_Wsvxs8jUTWZCBo"
    MORNING_SHEET_NAME = "Form Responses 1"
    EVENING_SHEET_NAME = "Form Responses 1"

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