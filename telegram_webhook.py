import requests
from flask import Flask, request, jsonify
from telegram_bot import TelegramBot
from google_sheets import GoogleSheetsService
from data_processor import DataProcessor
from main import DegustationAnalyzer
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация сервисов
telegram_bot = TelegramBot()
google_sheets_service = GoogleSheetsService()
data_processor = DataProcessor()

@app.route('/', methods=['GET'])
def root():
    """Корневой маршрут для избежания 404"""
    return jsonify({'status': 'ok', 'service': 'MD-BOT'})

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обрабатывает входящие обновления от Telegram"""
    try:
        update = request.get_json()

        if not update:
            return jsonify({'status': 'error', 'message': 'No update data'}), 400

        logger.info(f"Received update: {update}")

        # Обрабатываем callback query (нажатия инлайн кнопок)
        if 'callback_query' in update:
            callback_query = update['callback_query']
            user_id = callback_query['from']['id']
            callback_data = callback_query['data']

            logger.info(f"Callback from user {user_id}: {callback_data}")

            # Отвечаем на callback query
            requests.post(
                f"https://api.telegram.org/bot{telegram_bot.bot_token}/answerCallbackQuery",
                json={'callback_query_id': callback_query['id']}
            )

            # Обрабатываем callback
            telegram_bot.handle_callback(callback_data, user_id, google_sheets_service, data_processor)

        # Обрабатываем текстовые сообщения
        elif 'message' in update:
            message = update['message']
            user_id = message['from']['id']
            text = message.get('text', '')

            logger.info(f"Message from user {user_id}: {text}")

            # Обрабатываем команду /start
            if text == '/start':
                telegram_bot.send_start_menu()

        return jsonify({'status': 'ok'}), 200

    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Проверка здоровья сервиса"""
    return jsonify({
        'status': 'healthy',
        'services': {
            'telegram_bot': 'initialized',
            'google_sheets': 'initialized',
            'data_processor': 'initialized'
        }
    })

@app.route('/trigger-check', methods=['POST'])
def trigger_check():
    """Ручной запуск проверки отчетов"""
    try:
        logger.info("Manual trigger check initiated")

        # Создаем экземпляр анализатора
        analyzer = DegustationAnalyzer()

        # Запускаем проверку
        analyzer.check_for_new_reports()

        logger.info("Manual trigger check completed successfully")
        return jsonify({'triggered': True}), 200

    except Exception as e:
        logger.error(f"Error in manual trigger check: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
