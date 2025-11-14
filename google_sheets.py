import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from config import Config

class GoogleSheetsService:
    def __init__(self):
        self.config = Config()
        self.scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
        
    def _get_client(self):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.config.CREDENTIALS_PATH, self.scope)
        return gspread.authorize(creds)
    
    def get_sheet_data(self, sheet_id, sheet_name='Sheet1'):
        """Получает данные из Google Sheet"""
        client = self._get_client()
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    
    def get_new_records(self, sheet_id, last_check_time):
        """Получает только новые записи с последней проверки"""
        df = self.get_sheet_data(sheet_id)
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df[df['timestamp'] > last_check_time]
        return df