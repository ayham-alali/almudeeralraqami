"""
Al-Mudeer - Database Models Package
Re-exports all model functions for backward compatibility
"""

# Base utilities and initialization
from .base import (
    DB_TYPE,
    DATABASE_PATH,
    DATABASE_URL,
    POSTGRES_AVAILABLE,
    ID_PK,
    TIMESTAMP_NOW,
    init_enhanced_tables,
    init_customers_and_analytics,
    simple_encrypt,
    simple_decrypt,
    init_models,
)

# Email configuration
from .email_config import (
    save_email_config,
    get_email_config,
    get_email_oauth_tokens,
    update_email_config_settings,
    get_email_password,
)

# Telegram configuration
from .telegram_config import (
    save_telegram_config,
    get_telegram_config,
    get_telegram_bot_token,
    update_telegram_config_settings,
    save_telegram_phone_session,
    get_telegram_phone_session,
    get_telegram_phone_session_data,
    deactivate_telegram_phone_session,
    update_telegram_phone_session_sync_time,
    update_telegram_phone_session_settings,
    get_whatsapp_config,
    update_whatsapp_config_settings,
)

# Inbox/Outbox
from .inbox import (
    save_inbox_message,
    update_inbox_analysis,
    get_inbox_messages,
    get_inbox_messages_count,
    get_inbox_conversations,
    get_inbox_conversations_count,
    get_inbox_status_counts,
    get_conversation_messages,
    get_conversation_messages_cursor,
    get_full_chat_history,
    update_inbox_status,
    ignore_chat,
    approve_chat_messages,
    create_outbox_message,
    approve_outbox_message,
    mark_outbox_sent,
    get_pending_outbox,
    search_messages,
)

# Customers, Analytics, Preferences, Notifications
from .customers import (
    # Customer profiles
    get_or_create_customer,
    get_customers,
    get_customer,
    update_customer,
    get_recent_conversation,
    get_customer_for_message,
    increment_customer_messages,
    # Lead scoring
    calculate_lead_score,
    determine_segment,
    update_customer_lead_score,
    # Analytics
    update_daily_analytics,
    get_ai_usage_today,
    get_analytics_summary,
    # Analytics
    update_daily_analytics,
    get_ai_usage_today,
    get_analytics_summary,
    # Notifications
    create_notification,
    get_notifications,
    get_unread_count,
    mark_notification_read,
    mark_all_notifications_read,
    delete_old_notifications,
    create_smart_notification,
)

from .preferences import (
    get_preferences,
    update_preferences,
    delete_preferences,
)

# Re-export aiosqlite for backward compatibility
try:
    from .base import aiosqlite
except ImportError:
    aiosqlite = None

__all__ = [
    # Base
    "DB_TYPE",
    "DATABASE_PATH",
    "DATABASE_URL",
    "POSTGRES_AVAILABLE",
    "ID_PK",
    "TIMESTAMP_NOW",
    "init_enhanced_tables",
    "init_customers_and_analytics",
    "simple_encrypt",
    "simple_decrypt",
    "init_models",
    "aiosqlite",
    # Email
    "save_email_config",
    "get_email_config",
    "get_email_oauth_tokens",
    "update_email_config_settings",
    "get_email_password",
    # Telegram
    "save_telegram_config",
    "get_telegram_config",
    "get_telegram_bot_token",
    "update_telegram_config_settings",
    "save_telegram_phone_session",
    "get_telegram_phone_session",
    "get_telegram_phone_session_data",
    "deactivate_telegram_phone_session",
    "update_telegram_phone_session_sync_time",
    "update_telegram_phone_session_settings",
    "get_whatsapp_config",
    "update_whatsapp_config_settings",
    # Inbox
    "save_inbox_message",
    "update_inbox_analysis",
    "get_inbox_messages",
    "get_inbox_messages_count",
    "get_inbox_conversations",
    "get_inbox_conversations_count",
    "get_inbox_status_counts",
    "get_conversation_messages",
    "get_conversation_messages_cursor",
    "update_inbox_status",
    "ignore_chat",
    "approve_chat_messages",
    "create_outbox_message",
    "approve_outbox_message",
    "mark_outbox_sent",
    "mark_outbox_sent",
    "get_pending_outbox",
    "search_messages",
    # Customers
    "get_or_create_customer",
    "get_customers",
    "get_customer",
    "update_customer",
    "get_recent_conversation",
    "get_customer_for_message",
    "increment_customer_messages",
    "calculate_lead_score",
    "determine_segment",
    "update_customer_lead_score",
    # Analytics
    "update_daily_analytics",
    "get_ai_usage_today",
    "get_analytics_summary",
    # Preferences
    "get_preferences",
    "update_preferences",
    "delete_preferences",
    # Notifications
    "create_notification",
    "get_notifications",
    "get_unread_count",
    "mark_notification_read",
    "mark_all_notifications_read",
    "delete_old_notifications",
    "create_smart_notification",
]
