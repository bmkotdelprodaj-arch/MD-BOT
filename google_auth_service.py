import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleAuthService:
    """
    Надёжный сервис для аутентификации через сервисный аккаунт Google.
    Обрабатывает проблемы с экранированием ключей, валидирует приватный ключ,
    проверяет системное время и выполняет тестовый запрос к API.
    """

    def __init__(self, creds_path: str, scopes: Optional[list] = None):
        """
        Инициализация сервиса аутентификации.

        Args:
            creds_path: Путь к JSON-файлу с credentials сервисного аккаунта
            scopes: Список scopes для доступа (по умолчанию readonly для Sheets)
        """
        self.creds_path = creds_path
        self.scopes = scopes or ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self._credentials = None
        self._service = None

    def _check_system_time(self) -> bool:
        """
        Проверяет синхронизацию системного времени с NTP.
        Возвращает True если разница менее 5 минут.

        Returns:
            bool: True если время синхронизировано
        """
        try:
            # Получаем время с NTP сервера
            response = requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC', timeout=5)
            response.raise_for_status()
            ntp_time = datetime.fromisoformat(response.json()['datetime'].replace('Z', '+00:00'))

            # Текущее системное время
            system_time = datetime.now(timezone.utc)

            # Разница в минутах
            time_diff = abs((system_time - ntp_time).total_seconds()) / 60

            if time_diff > 5:
                logger.warning(f"Системное время отличается от NTP на {time_diff:.1f} минут. "
                             "Это может вызвать ошибки аутентификации JWT.")
                return False

            logger.info("Системное время синхронизировано с NTP.")
            return True

        except Exception as e:
            logger.warning(f"Не удалось проверить системное время: {e}")
            return True  # Не блокируем аутентификацию из-за ошибки проверки

    def _load_credentials_json(self) -> Dict[str, Any]:
        """
        Загружает и обрабатывает JSON с credentials.

        Returns:
            Dict с данными credentials

        Raises:
            ValueError: При ошибках загрузки или парсинга
        """
        try:
            with open(self.creds_path, 'r', encoding='utf-8') as f:
                creds_data = json.load(f)

            # Проверяем наличие обязательных полей
            required_fields = ['type', 'project_id', 'private_key_id', 'private_key',
                             'client_email', 'token_uri']
            missing_fields = [field for field in required_fields if field not in creds_data]

            if missing_fields:
                raise ValueError(f"Отсутствуют обязательные поля в credentials: {missing_fields}")

            # Обрабатываем экранирование переносов строк в private_key
            if '\\n' in creds_data['private_key']:
                creds_data['private_key'] = creds_data['private_key'].replace('\\n', '\n')
                logger.info("Обработано экранирование переносов строк в private_key")

            return creds_data

        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка парсинга JSON файла {self.creds_path}: {e}")
        except FileNotFoundError:
            raise ValueError(f"Файл credentials не найден: {self.creds_path}")
        except Exception as e:
            raise ValueError(f"Ошибка загрузки credentials: {e}")

    def _validate_private_key(self, private_key_pem: str) -> bool:
        """
        Валидирует приватный ключ с помощью cryptography.

        Args:
            private_key_pem: PEM строка приватного ключа

        Returns:
            bool: True если ключ валиден

        Raises:
            ValueError: При ошибках валидации
        """
        try:
            # Загружаем ключ
            private_key = serialization.load_pem_private_key(
                private_key_pem.encode('utf-8'),
                password=None
            )

            # Проверяем что это RSA ключ
            if not isinstance(private_key, rsa.RSAPrivateKey):
                raise ValueError("Приватный ключ должен быть RSA ключом")

            # Проверяем размер ключа (должен быть не менее 2048 бит)
            key_size = private_key.key_size
            if key_size < 2048:
                raise ValueError(f"Размер ключа {key_size} бит слишком мал (минимум 2048)")

            logger.info(f"Приватный ключ валиден (RSA, {key_size} бит)")
            return True

        except Exception as e:
            raise ValueError(f"Ошибка валидации приватного ключа: {e}")

    def create_credentials(self) -> service_account.Credentials:
        """
        Создает и возвращает credentials для сервисного аккаунта.

        Returns:
            service_account.Credentials: Готовые credentials

        Raises:
            ValueError: При ошибках создания credentials
        """
        # Проверяем системное время
        self._check_system_time()

        # Загружаем данные credentials
        creds_data = self._load_credentials_json()

        # Валидируем приватный ключ
        self._validate_private_key(creds_data['private_key'])

        # Создаем credentials
        try:
            self._credentials = service_account.Credentials.from_service_account_info(
                creds_data,
                scopes=self.scopes
            )
            logger.info("Credentials успешно созданы")
            return self._credentials

        except Exception as e:
            raise ValueError(f"Ошибка создания credentials: {e}")

    def create_sheets_service(self) -> Any:
        """
        Создает клиент для Google Sheets API.

        Returns:
            Google Sheets API service object

        Raises:
            ValueError: При ошибках создания сервиса
        """
        if not self._credentials:
            self.create_credentials()

        try:
            self._service = build('sheets', 'v4', credentials=self._credentials)
            logger.info("Google Sheets API клиент успешно создан")
            return self._service

        except Exception as e:
            raise ValueError(f"Ошибка создания Sheets API клиента: {e}")

    def test_api_call(self, spreadsheet_id: Optional[str] = None) -> bool:
        """
        Выполняет тестовый запрос к Google Sheets API.

        Args:
            spreadsheet_id: ID тестовой таблицы (опционально)

        Returns:
            bool: True если запрос успешен

        Raises:
            ValueError: При ошибках API запроса
        """
        if not self._service:
            self.create_sheets_service()

        # Если spreadsheet_id не указан, используем тестовый
        if not spreadsheet_id:
            # Можно использовать публичную тестовую таблицу или просто проверить доступ
            spreadsheet_id = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'  # Пример публичной таблицы

        try:
            # Выполняем простой запрос на получение метаданных таблицы
            request = self._service.spreadsheets().get(spreadsheetId=spreadsheet_id)
            response = request.execute()

            logger.info(f"Тестовый API запрос успешен. Таблица: {response.get('properties', {}).get('title', 'Unknown')}")
            return True

        except HttpError as e:
            error_details = json.loads(e.content.decode('utf-8'))
            error_code = error_details.get('error', {}).get('code')
            error_message = error_details.get('error', {}).get('message', '')

            if 'invalid_grant' in error_message and 'Invalid JWT Signature' in error_message:
                raise ValueError("Ошибка JWT подписи. Возможные причины:\n"
                               "- Приватный ключ поврежден или не соответствует сервисному аккаунту\n"
                               "- Сервисный аккаунт отключен или удален в Google Cloud\n"
                               "- Системное время не синхронизировано\n"
                               "- Неправильный project_id или client_email")

            elif error_code == 403:
                raise ValueError("Доступ запрещен. Проверьте:\n"
                               "- Scopes доступа\n"
                               "- Разрешения сервисного аккаунта для таблицы\n"
                               "- Активность сервисного аккаунта")

            elif error_code == 404:
                raise ValueError("Таблица не найдена. Проверьте spreadsheet_id")

            else:
                raise ValueError(f"Ошибка API запроса ({error_code}): {error_message}")

        except Exception as e:
            raise ValueError(f"Неожиданная ошибка при тестовом запросе: {e}")


def authenticate_google_sheets(creds_path: str, scopes: Optional[list] = None,
                              test_spreadsheet_id: Optional[str] = None) -> Any:
    """
    Удобная функция для полной аутентификации и тестирования.

    Args:
        creds_path: Путь к JSON файлу credentials
        scopes: Список scopes
        test_spreadsheet_id: ID таблицы для теста

    Returns:
        Google Sheets API service object

    Raises:
        ValueError: При любых ошибках аутентификации
    """
    auth_service = GoogleAuthService(creds_path, scopes)
    auth_service.create_credentials()
    auth_service.create_sheets_service()
    auth_service.test_api_call(test_spreadsheet_id)
    logger.info("Аутентификация и тестирование завершены успешно")
    return auth_service._service
