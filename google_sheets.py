import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from config import Config
import logging

class GoogleSheetsService:
    def __init__(self):
        self.config = Config()
        # ✅ Используем современный и минимально необходимый scope
        self.scope = ['https://www.googleapis.com/auth/spreadsheets']

    def _get_client(self) -> gspread.Client:
        """
        Создает и возвращает авторизованный клиент gspread с использованием service account credentials.

        Returns:
            gspread.Client: Авторизованный клиент для работы с Google Sheets.

        Raises:
            ValueError: Если не удалось создать credentials или авторизовать клиента.
        """
        try:
            creds_json = {
                "type": "service_account",
                "project_id": "degustation-bot",
                "private_key_id": "1341752cd5b808416b7efc79343a3f48514642ab",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDUD0BGlUE4eIVd\nfoJHMfb+J+Ong5hSgRoA0lzkKILHte+/nVHTEjpYZpHxnGuNGRQq1wCslwacMUf9\nDZ7nzf1g34orRiiWi6yrzzmmveg7vlHz7A6uy7z+Talcy+RndpC6cMeIOn875eZt\nHcHBlNjq/+IAP/lRAvM0sVqL0i89IMQP1Y2rZcM/E1XESSuUhAv6+nIdpdRmDQK2\nG/Ndunp6nQq8Q3n4gnA9Qm1A5dG6OXFOR/dshyDG1/PKwUK+UHJClnMq5iaiO+Tx\nBnKEwqAHN/h2h1r+JWM4VO2t6ZU/7e5aAhu5sDQyCWRoZ/D/jSSC6jUIpW9ULLqb\n1XCvTNFLAgMBAAECggEAC2QwVzpQURjETUPs+qcX9gothAOhLJlrzKyAOe9lKxY+\nZUQtr5F0megUoe59pow41Z5MFnlcbQqGq/TN2vTpTglkEjQIHSIuqeINXkQoVAFC\nxMAi/8fJBL4mQWpggCBR4PoK9enyiMSuTqFBFcWJq4IIYWwOWAg5CVUXoJgvWnCt\nlhATYXHSWXVLmA4Ebw8bdwg1LIlHW55MtHu6EIQeqROL4Jn77AXOnV5H32U/7vk8\ns+lvA1nYx5hLIkGHO4qGp8jczJ8UUg1u1gAXIRng/tLcdEOziR6goFGOJaW15zY9\nJiEBKEygcZOAPjkh0srULTZO6bkbg/5kYTz1YsfOCQKBgQD1z1Jq3W++8p6/BYI0\nzaijRMra7QAV3iXhArh9ZMV67GIju/yY9VuQFWumksYiWhtJjrU/1MEqzHV+m47l\np7Hc8h3zfV4Iv7u1O0ttLbeFwIAXhmQIUudNlLyIQsh1rBWEuwteXAJNftJYqlYO\n9n/xy936BaUGcXGOBh9b2KHM8wKBgQDc2cBeI9yCFCApIqVVTaKK6jPLDkrIdtbb\nG74Gu/hwgdEaQXpJwrU+0gEUcCj4CjC4vC6OFk+awkc1aHMVIUDyJrrYyywOWAg5\nCVUXoJgvWnCtlhATYXHSWXVLmA4Ebw8bdwg1LIlHW55MtHu6EIQeqROL4Jn77AXOn\nV5H32U/7vk8s+lvA1nYx5hLIkGHO4qGp8jczJ8UUg1u1gAXIRng/tLcdEOziR6go\nFGOJaW15zY9JiEBKEygcZOAPjkh0srULTZO6bkbg/5kYTz1YsfOCQKBgQCAAfxonHK\nCHPUggyzgdK06gSumwL9HpA1T1UfgPSc+MXWWKGKOAVAyg4UxFMRLxWLqdn/bAt+\npexpA2Z+b5y2P4yJH1+StZ7PH2F4aODGXXL0YAyt6zjmYRDm+OBEOfpQiMAnmlMG\n6O64s8F+qoSHV+JXbiy76YP+Ct5uDCBiSkwKBgAvsLXhnkzXqhDn0RtyuDkJVlmo\n14U+lKsA1ZTLT3vuj5nf2ZPWqb0Ju7+P8khmh4zN1\n5UWCatW9+z4k32Gao/juolOl1EMfMqo7KgNpFbRj2Pe0Wwa5w7S/RE0rntula2fN\nyLpvjg2Ba7SWUY9xjC5bm2TrC0WCSioAfkFdDyMRAoGBAMmaer95pf/TVG2ZGuVO\nQhw5PTilRS8kRNFtdF9jnMvBnpbr0xGlQzoNPO5bpUSfd+SOHTDkAxGAs5tVeQB1\nKXCAuOeGjBQRhuej9uyOL/JNifsz+x7qd4chAv/E4yi33Vo6CeDbiuQ93efB3rRO\nng/jhd8J78frLEbPoLkqSq4a\n-----END PRIVATE KEY-----\n",
                "client_email": "degustation-bot@degustation-bot.iam.gserviceaccount.com",
                "client_id": "101050435170142780195",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/degustation-bot%40degustation-bot.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
            }
            creds = Credentials.from_service_account_info(creds_json, scopes=self.scope)
            return gspread.authorize(creds)
        except Exception as e:
            logging.error(f"Failed to create Google Sheets client: {e}", exc_info=True)
            raise ValueError(f"Authentication failed: {e}") from e

    def get_sheet_data(self, sheet_id: str, sheet_name: str = 'Sheet1') -> pd.DataFrame:
        """
        Получает данные из Google Sheet и возвращает их в виде DataFrame.

        Args:
            sheet_id (str): ID Google Sheet.
            sheet_name (str, optional): Название листа. По умолчанию 'Sheet1'.

        Returns:
            pd.DataFrame: Данные из листа.
        """
        try:
            client = self._get_client()
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            data = sheet.get_all_records()
            return pd.DataFrame(data)
        except Exception as e:
            logging.error(f"Failed to fetch data from sheet {sheet_id}/{sheet_name}: {e}", exc_info=True)
            raise

    def get_new_records(self, sheet_id: str, last_check_time: datetime) -> pd.DataFrame:
        """
        Получает только новые записи с момента последней проверки.

        Args:
            sheet_id (str): ID Google Sheet.
            last_check_time (datetime): Время последней проверки.

        Returns:
            pd.DataFrame: Новые записи.
        """
        try:
            df = self.get_sheet_data(sheet_id)
            if not df.empty and 'timestamp' in df.columns:
                # Приводим к datetime — с обработкой ошибок и явным указанием UTC при необходимости
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                # Удаляем строки, где timestamp не распарсился
                df = df.dropna(subset=['timestamp'])
                return df[df['timestamp'] > last_check_time]
            return pd.DataFrame()  # пустой DataFrame, если колонки 'timestamp' нет
        except Exception as e:
            logging.error(f"Failed to filter new records: {e}", exc_info=True)
            raise
