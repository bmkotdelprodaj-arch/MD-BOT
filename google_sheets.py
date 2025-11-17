import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from config import Config
import logging

class GoogleSheetsService:
    def __init__(self):
        self.config = Config()
        self.scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']

    def _get_client(self) -> gspread.Client:
        """
        Создает и возвращает авторизованный клиент gspread с использованием service account credentials.

        Returns:
            gspread.Client: Авторизованный клиент для работы с Google Sheets.

        Raises:
            ValueError: Если не удалось создать credentials или авторизовать клиента.
        """
        try:
            # Используем credentials из config
            if hasattr(self.config, 'CREDENTIALS_JSON') and self.config.CREDENTIALS_JSON:
                creds = Credentials.from_service_account_info(self.config.CREDENTIALS_JSON, scopes=self.scope)
            else:
                creds = Credentials.from_service_account_file(self.config.CREDENTIALS_PATH, scopes=self.scope)
            return gspread.authorize(creds)
        except Exception as e:
            logging.error(f"Failed to create Google Sheets client: {e}")
            raise ValueError(f"Authentication failed: {e}")
    
    def get_sheet_data(self, sheet_id: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
        """
        Получает данные из Google Sheet и возвращает их в виде DataFrame.

        Args:
            sheet_id (str): ID Google Sheet.
            sheet_name (str, optional): Название листа. По умолчанию 'Sheet1'.

        Returns:
            pd.DataFrame: Данные из листа.
        """
        client = self._get_client()
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_records()
        return pd.DataFrame(data)

    def get_new_records(self, sheet_id: str, last_check_time: datetime) -> pd.DataFrame:
        """
        Получает только новые записи с момента последней проверки.

        Args:
            sheet_id (str): ID Google Sheet.
            last_check_time (datetime): Время последней проверки.

        Returns:
            pd.DataFrame: Новые записи.
        """
        df = self.get_sheet_data(sheet_id)
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df[df['timestamp'] > last_check_time]
        return df
