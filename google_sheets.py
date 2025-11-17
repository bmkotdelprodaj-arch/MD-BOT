import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
from config import Config
import logging

class GoogleSheetsService:
    def __init__(self):
        self.config = Config()
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self._client_cache = None
        self._client_timestamp = None
        self.CLIENT_CACHE_TTL = 300  # 5 минут — меньше, чем JWT expiry (1h)

    def _get_fresh_client(self) -> gspread.Client:
        """Создаёт клиент с обновлёнными credentials (с принудительным refresh)"""
        try:
            # Создаём credentials
            if hasattr(self.config, 'CREDENTIALS_JSON') and self.config.CREDENTIALS_JSON:
                creds = Credentials.from_service_account_info(
                    self.config.CREDENTIALS_JSON,
                    scopes=self.scope
                )
            else:
                creds = Credentials.from_service_account_file(
                    self.config.CREDENTIALS_PATH,
                    scopes=self.scope
                )

            # 🔥 КРИТИЧЕСКИ: обновляем токен с учётом точного времени Google
            creds.refresh(Request())

            return gspread.authorize(creds)

        except Exception as e:
            logging.error(f"Auth failed during client creation: {e}", exc_info=True)
            raise

    def _get_client(self) -> gspread.Client:
        """Кэшированный клиент с TTL (для производительности, но безопасно)"""
        now = datetime.now()
        if (self._client_cache is None or
            self._client_timestamp is None or
            (now - self._client_timestamp).total_seconds() > self.CLIENT_CACHE_TTL):
            
            logging.info("🔄 Creating new Google Sheets client (cache expired or first call)")
            self._client_cache = self._get_fresh_client()
            self._client_timestamp = now

        return self._client_cache

    def get_sheet_data(self, sheet_id: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
        """Получает данные — использует кэшированный, но обновляемый клиент"""
        try:
            client = self._get_client()
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            data = sheet.get_all_records()
            return pd.DataFrame(data)
        except Exception as e:
            # При ошибке аутентификации — сбрасываем кэш и повторяем
            if "invalid_grant" in str(e) or "Invalid JWT" in str(e):
                logging.warning("⚠️ Invalid token detected — clearing cache and retrying...")
                self._client_cache = None
                self._client_timestamp = None
                # Повторная попытка
                client = self._get_client()
                sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
                data = sheet.get_all_records()
                return pd.DataFrame(data)
            else:
                raise

    def get_new_records(self, sheet_id: str, last_check_time: datetime) -> pd.DataFrame:
        df = self.get_sheet_data(sheet_id)
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df[df['timestamp'] > last_check_time]
        return df
