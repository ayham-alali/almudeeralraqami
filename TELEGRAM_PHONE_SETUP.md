# Telegram Phone Number Integration - Setup Guide

## Overview

This integration allows businesses to connect their **existing Telegram phone number** (the one customers already know) instead of requiring them to create a bot. Messages are received via MTProto client (Telethon library).

## Important Environment Variables Needed

Before using this feature, you **must** set these environment variables in Railway:

### 1. Telegram API Credentials

Get these from: https://my.telegram.org/apps

- **TELEGRAM_API_ID**: Your Telegram API ID (number)
- **TELEGRAM_API_HASH**: Your Telegram API Hash (string)

**How to get them:**
1. Go to https://my.telegram.org/apps
2. Log in with your phone number
3. Fill in the form (App title, Short name, Platform)
4. Copy the `api_id` and `api_hash`
5. Add them to Railway environment variables

**Note**: These are the same credentials for all users - they identify your application to Telegram, not individual users.

## Database Migration

The following table will be created automatically when `init_enhanced_tables()` is called:

```sql
telegram_phone_sessions (
    id, license_key_id, phone_number, 
    session_data_encrypted, user_id, user_first_name, 
    user_last_name, user_username, is_active, 
    last_synced_at, created_at, updated_at
)
```

## API Endpoints

### 1. Start Login (Send Code)
**POST** `/api/integrations/telegram-phone/start`

Request:
```json
{
  "phone_number": "+963912345678"
}
```

Response:
```json
{
  "success": true,
  "message": "تم إرسال كود التحقق إلى Telegram الخاص برقم +963912345678",
  "session_id": "temp_+963912345678_1234567890.123",
  "phone_number": "+963912345678"
}
```

### 2. Verify Code
**POST** `/api/integrations/telegram-phone/verify`

Request:
```json
{
  "phone_number": "+963912345678",
  "code": "12345",
  "session_id": "temp_+963912345678_1234567890.123"
}
```

Response:
```json
{
  "success": true,
  "message": "تم ربط رقم Telegram بنجاح",
  "user": {
    "id": 123456789,
    "phone": "+963912345678",
    "first_name": "John",
    "last_name": "Doe",
    "username": "johndoe"
  },
  "config_id": 1
}
```

### 3. Test Connection
**POST** `/api/integrations/telegram-phone/test`

Response:
```json
{
  "success": true,
  "message": "الاتصال ناجح",
  "user": { ... }
}
```

### 4. Get Config
**GET** `/api/integrations/telegram-phone/config`

Response:
```json
{
  "config": {
    "id": 1,
    "phone_number": "+963***678",
    "phone_number_masked": "+963***678",
    "user_first_name": "John",
    "user_last_name": "Doe",
    "user_username": "johndoe",
    "is_active": true,
    "last_synced_at": "2024-01-01T12:00:00",
    ...
  }
}
```

### 5. Disconnect
**POST** `/api/integrations/telegram-phone/disconnect`

## Current Status

✅ **Phase 1 Complete**:
- Backend service (`TelegramPhoneService`)
- Database table and CRUD functions
- API endpoints for login flow
- Session encryption

⏳ **Phase 2 Pending**:
- Worker polling for messages (will be added next)
- Frontend UI for phone number integration

## Limitations & Notes

1. **2FA Not Supported**: Accounts with 2FA enabled will show an error. Users need to temporarily disable 2FA or use Telegram Bot instead.

2. **Session Management**: Sessions are stored encrypted in the database. If a session expires (rare), users will need to re-authenticate.

3. **Polling Required**: Unlike Telegram Bot API which uses webhooks, phone number integration requires continuous polling (will be implemented in worker).

4. **One Session Per License**: Each license can only have one active Telegram phone session.

5. **Private Messages Only**: Currently designed for private 1-on-1 conversations, not groups/channels.

## Next Steps

1. Set `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` in Railway
2. Test the login flow using the API endpoints
3. Wait for Phase 2 (worker polling) to receive messages
4. Add frontend UI for phone number integration

