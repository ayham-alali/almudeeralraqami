"""Al-Mudeer Services Package"""

from .email_service import EmailService, EMAIL_PROVIDERS
from .telegram_service import TelegramService, TelegramBotManager, TELEGRAM_SETUP_GUIDE
from .gmail_oauth_service import GmailOAuthService
from .gmail_api_service import GmailAPIService
from .telegram_phone_service import TelegramPhoneService

__all__ = [
    'EmailService',
    'EMAIL_PROVIDERS',
    'TelegramService', 
    'TelegramBotManager',
    'TELEGRAM_SETUP_GUIDE',
    'GmailOAuthService',
    'GmailAPIService',
    'TelegramPhoneService',
]

