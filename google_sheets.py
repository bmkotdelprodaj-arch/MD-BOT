import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
import os
import base64
import json
import logging
from gspread.exceptions import WorksheetNotFound

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    def __init__(self):
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self._client_cache = None
        self._client_timestamp = None
        self.CLIENT_CACHE_TTL = 300  # 5 минут (меньше срока жизни access_token ~1h)

    def _get_fresh_client(self) -> gspread.Client:
        """Создаёт новый клиент с корректно загруженными credentials."""
        try:
            # 1. Получаем base64-строку из переменной окружения
            encoded = os.getenv("GOOGLE_CREDENTIALS_JSON")
            if not encoded:
                raise ValueError(
                    "GOOGLE_CREDENTIALS_JSON environment variable is not set. "
                    "Please add it in Render dashboard."
                )

            # Лог текущего времени UTC для диагностики
            logger.info(f"Current UTC time for auth check: {datetime.utcnow().isoformat()}")

            # 2. Декодируем base64 → bytes → JSON → dict
            try:
                decoded_bytes = base64.b64decode(encoded)
                creds_dict = json.loads(decoded_bytes)
                logger.debug(f"Decoded credentials JSON keys: {list(creds_dict.keys())}")
            except (ValueError, json.JSONDecodeError) as e:
                logger.critical("❌ Failed to decode or parse GOOGLE_CREDENTIALS_JSON. "
                               "Check that it's valid base64-encoded JSON.", exc_info=True)
                raise ValueError("Invalid GOOGLE_CREDENTIALS_JSON format") from e

            # 3. Проверяем обязательные поля (защита от пустого/битого JSON)
            required_keys = ["type", "project_id", "private_key", "client_email", "client_id"]
            missing = [k for k in required_keys if k not in creds_dict]
            if missing:
                raise ValueError(f"Missing required keys in credentials JSON: {missing}")

            # 4. Логируем для отладки (можно закомментировать после проверки)
            pk_preview = creds_dict["private_key"][:40].replace("\n", "\\n")
            logger.info(f"✅ Credentials loaded. private_key preview: {pk_preview}...")

            # 5. Создаём credentials
            creds = Credentials.from_service_account_info(creds_dict, scopes=self.scope)

            # 6. Опционально: принудительно обновляем access token (не обязательно — gspread сам сделает при первом запросе)
            try:
                creds.refresh(Request())
                logger.info("🔑 Access token refreshed successfully")
            except Exception as refresh_err:
                logger.error(f"⚠️ Token refresh failed with exception: {refresh_err}", exc_info=True)

            # 7. Авторизуем gspread
            client = gspread.authorize(creds)
            logger.info("✅ New Google Sheets client created successfully")
            return client

        except Exception as e:
            logger.error(f"❌ Auth failed during client creation: {e}", exc_info=True)
            raise

    def _get_client(self) -> gspread.Client:
        """Возвращает кэшированный клиент или создаёт новый при истечении TTL."""
        now = datetime.now()
        cache_expired = (
            self._client_cache is None or
            self._client_timestamp is None or
            (now - self._client_timestamp).total_seconds() > self.CLIENT_CACHE_TTL
        )

        if cache_expired:
            logger.info("🔄 Creating new Google Sheets client (cache expired or first call)")
            self._client_cache = self._get_fresh_client()
            self._client_timestamp = now

        return self._client_cache

    def get_sheet_data(self, sheet_id: str, sheet_name: str) -> pd.DataFrame:
        """Получает данные из Google Sheets в виде pandas DataFrame."""
        logger.info(f"Запрос данных с листа '{sheet_name}' в таблице {sheet_id}")
        try:
            client = self._get_client()
            try:
                sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            except WorksheetNotFound:
                available_sheets = [ws.title for ws in client.open_by_key(sheet_id).worksheets()]
                error_msg = f"Лист '{sheet_name}' не найден в таблице {sheet_id}. Доступные листы: {available_sheets}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            records = sheet.get_all_records()
            df = pd.DataFrame(records)
            logger.debug(f"📥 Loaded {len(df)} rows from sheet '{sheet_name}' ({sheet_id})")
            return df
        except Exception as e:
            # Не делаем retry при Invalid JWT — это ошибка конфигурации, а не временная
            if "invalid_grant" in str(e) or "Invalid JWT" in str(e):
                logger.critical("🔴 Persistent auth error — check GOOGLE_CREDENTIALS_JSON!")
            logger.error(f"❌ Error fetching sheet data: {e}", exc_info=True)
            raise

    def get_new_records(self, sheet_id: str, last_check_time: datetime, sheet_name: str) -> pd.DataFrame:
        """Возвращает только новые записи, добавленные после last_check_time."""
        df = self.get_sheet_data(sheet_id, sheet_name)
        if df.empty:
            return df

        # Предполагается, что в таблице есть колонка 'timestamp' в формате ISO или 'dd.mm.yyyy HH:MM'
        if 'timestamp' not in df.columns:
            logger.warning("⚠️ Column 'timestamp' not found — returning all records")
            return df

        # Преобразуем в datetime (гибко: поддерживаем разные форматы)
        try:
            # Сначала пробуем ISO
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            # Если много NaT — попробуем ручной формат (часто используется вручную)
            if df['timestamp'].isna().sum() > len(df) * 0.5:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d.%m.%Y %H:%M', errors='coerce')
        except Exception as parse_err:
            logger.error(f"⚠️ Failed to parse 'timestamp' column: {parse_err}")
            return df

        # Фильтруем по времени
        mask = df['timestamp'] > last_check_time
        new_df = df[mask].copy()
        logger.info(f"🆕 Found {len(new_df)} new records (out of {len(df)}) since {last_check_time}")
        return new_df
