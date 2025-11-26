import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
import logging
from gspread.exceptions import WorksheetNotFound
from config import Config

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
        self.config = Config()

    def _get_fresh_client(self) -> gspread.Client:
        """Создаёт новый клиент, используя credentials из Config (локально и на Render)."""
        try:
            creds_dict = self.config.GOOGLE_CREDENTIALS

            required_keys = ["type", "project_id", "private_key", "client_email", "client_id"]
            missing = [k for k in required_keys if k not in creds_dict]
            if missing:
                raise ValueError(f"Missing required keys in credentials JSON: {missing}")

            pk_preview = creds_dict.get("private_key", "")[:40].replace("\n", "\\n")
            logger.info(f"Credentials loaded from Config. private_key preview: {pk_preview}...")

            creds = Credentials.from_service_account_info(creds_dict, scopes=self.scope)

            try:
                creds.refresh(Request())
                logger.info("Access token refreshed successfully")
            except Exception as refresh_err:
                logger.error(f"Token refresh failed with exception: {refresh_err}", exc_info=True)

            client = gspread.authorize(creds)
            logger.info("New Google Sheets client created successfully")
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
            logger.info("Creating new Google Sheets client (cache expired or first call)")
            self._client_cache = self._get_fresh_client()
            self._client_timestamp = now

        return self._client_cache

    def _resolve_sheet_name(self, sheet_id: str, sheet_name: str | None) -> str:
        """Определяет имя листа, если оно не передано явно."""
        if sheet_name:
            return sheet_name

        if sheet_id == self.config.MORNING_SHEET_ID:
            return self.config.MORNING_SHEET_NAME
        if sheet_id == self.config.EVENING_SHEET_ID:
            return self.config.EVENING_SHEET_NAME

        # Запасной вариант — стандартное имя формы
        logger.warning(
            f"Имя листа не передано для таблицы {sheet_id}, используем дефолтное 'Form Responses 1'"
        )
        return "Form Responses 1"

    def get_sheet_data(self, sheet_id: str, sheet_name: str | None = None) -> pd.DataFrame:
        """Получает данные из Google Sheets в виде pandas DataFrame."""
        if not sheet_id or not isinstance(sheet_id, str) or sheet_id.strip() == "":
            error_msg = "Ошибка: sheet_id пуст или некорректен."
            logger.error(error_msg)
            raise ValueError(error_msg)

        sheet_name = self._resolve_sheet_name(sheet_id, sheet_name)
        logger.info(f"Запрос данных с листа '{sheet_name}' в таблице с ключом: {sheet_id}")
        try:
            client = self._get_client()
            try:
                sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            except WorksheetNotFound:
                available_sheets = [ws.title for ws in client.open_by_key(sheet_id).worksheets()]
                error_msg = (
                    f"Лист '{sheet_name}' не найден в таблице с ключом {sheet_id}. "
                    f"Доступные листы: {available_sheets}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            except gspread.exceptions.APIError as api_error:
                if api_error.response.status_code == 404:
                    logger.error(
                        f"Ошибка 404: таблица с ключом {sheet_id} не найдена или отсутствуют права доступа."
                    )
                    raise ValueError(
                        f"Ошибка 404: таблица с ключом {sheet_id} не найдена или отсутствуют права доступа."
                    )
                else:
                    raise
            records = sheet.get_all_records()
            df = pd.DataFrame(records)
            logger.info(
                f"Загружено {len(df)} строк с листа '{sheet_name}' (ключ: {sheet_id}). "
                f"Колонки: {list(df.columns)}"
            )
            return df
        except Exception as e:
            if "invalid_grant" in str(e) or "Invalid JWT" in str(e):
                logger.critical("Постоянная ошибка аутентификации — проверьте GOOGLE_CREDENTIALS_JSON/Config!")
            logger.error(f"Ошибка при получении данных листа: {e}", exc_info=True)
            raise

    def get_new_records(
        self,
        sheet_id: str,
        last_check_time: datetime,
        sheet_name: str | None = None
    ) -> pd.DataFrame:
        """Возвращает только новые записи, добавленные после last_check_time."""
        df = self.get_sheet_data(sheet_id, sheet_name)
        if df.empty:
            logger.info("Лист пустой — новых записей нет")
            return df

        # Поддерживаем разные варианты названий колонки времени
        possible_timestamp_cols = [
            "timestamp",
            "Timestamp",
            self.config.MORNING_COLUMNS.get("timestamp"),
            self.config.EVENING_COLUMNS.get("timestamp"),
        ]
        possible_timestamp_cols = [c for c in possible_timestamp_cols if c]

        timestamp_col = next((c for c in possible_timestamp_cols if c in df.columns), None)

        if not timestamp_col:
            logger.warning(
                f"⚠️ Колонка времени не найдена ни под одним из имён {possible_timestamp_cols} — возвращаем все записи"
            )
            return df

            logger.info(f"Используем колонку времени '{timestamp_col}' для фильтрации новых записей")

        # Преобразуем в datetime (гибко: поддерживаем разные форматы)
        try:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
            # Если много NaT — пробуем формат dd.mm.yyyy HH:MM
            if df[timestamp_col].isna().sum() > len(df) * 0.5:
                df[timestamp_col] = pd.to_datetime(
                    df[timestamp_col],
                    format='%d.%m.%Y %H:%M',
                    errors='coerce'
                )
        except Exception as parse_err:
            logger.error(f"⚠️ Failed to parse '{timestamp_col}' column: {parse_err}")
            return df

        before = len(df)
        mask = df[timestamp_col] > last_check_time
        new_df = df[mask].copy()
        logger.info(
            f"Найдено {len(new_df)} новых записей (из {before}) после {last_check_time} "
            f"по колонке '{timestamp_col}'"
        )
        return new_df
