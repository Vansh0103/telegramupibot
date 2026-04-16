from datetime import datetime, timedelta

# ╔══════════════════════════════════════════════════════════════╗
# ║                    CONFIGURATION                             ║
# ╚══════════════════════════════════════════════════════════════╝

from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.photos import (
    UploadProfilePhotoRequest,
    DeletePhotosRequest,
)
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

# ╔══════════════════════════════════════════════════════════════╗
# ║                    CONFIGURATION                             ║
# ╚══════════════════════════════════════════════════════════════╝

BOT_TOKEN = "8367562923:AAE0vnjRXBHPj0cja_hoc_oQLSjAPU9wHww"
API_ID = 30995906
API_HASH = "07d2a5d554438de69001bda68e5cb7bd"
ADMIN_ID = 8624480309
SUPPORT_USERNAME = "@itsukiarai"
SUPPORT_URL = "https://t.me/itsukiarai"
BOT_NAME = "SkullAutomation"
BOT_VERSION = "3.0.0"
DB_FILE = "skull_automation.db"
BACKUP_DIR = "backups"
MEDIA_DIR = "media_cache"
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024

# Plan Limits Configuration
PLAN_CONFIG = {
    "free": {
        "name": "🆓 Free",
        "max_keywords": 10,
        "max_filters": 5,
        "max_forwards": 2,
        "max_scheduled": 5,
        "max_blocked_words": 15,
        "max_whitelist": 10,
        "max_templates": 3,
        "media_in_replies": False,
        "auto_react": False,
        "working_hours": False,
        "recurring_schedule": False,
        "regex_keywords": False,
        "multi_media": False,
        "custom_commands": False,
        "priority_support": False,
        "backup_export": False,
        "advanced_stats": False,
        "broadcast_receive": True,
    },
    "premium": {
        "name": "⭐ Premium",
        "max_keywords": 50,
        "max_filters": 25,
        "max_forwards": 10,
        "max_scheduled": 25,
        "max_blocked_words": 50,
        "max_whitelist": 50,
        "max_templates": 15,
        "media_in_replies": True,
        "auto_react": True,
        "working_hours": True,
        "recurring_schedule": True,
        "regex_keywords": True,
        "multi_media": False,
        "custom_commands": True,
        "priority_support": True,
        "backup_export": True,
        "advanced_stats": True,
        "broadcast_receive": True,
    },
    "vip": {
        "name": "👑 VIP",
        "max_keywords": 200,
        "max_filters": 100,
        "max_forwards": 30,
        "max_scheduled": 100,
        "max_blocked_words": 200,
        "max_whitelist": 200,
        "max_templates": 50,
        "media_in_replies": True,
        "auto_react": True,
        "working_hours": True,
        "recurring_schedule": True,
        "regex_keywords": True,
        "multi_media": True,
        "custom_commands": True,
        "priority_support": True,
        "backup_export": True,
        "advanced_stats": True,
        "broadcast_receive": True,
    },
}

# Reaction Emoji Options
REACTION_EMOJIS = [
    "👍", "❤️", "🔥", "🥰", "👏", "😁", "🤔", "🤯",
    "😱", "🎉", "⚡", "🏆", "💯", "😍", "🤗", "🫡",
    "👎", "😢", "💔", "🤮", "💩", "🤡", "👀", "🦴",
]

# Days of week for working hours
DAYS_OF_WEEK = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]

# ╔══════════════════════════════════════════════════════════════╗
# ║                  CONVERSATION STATES                         ║
# ╚══════════════════════════════════════════════════════════════╝

(
    ST_PHONE, ST_OTP, ST_2FA,
    ST_WELCOME_MSG, ST_WELCOME_MEDIA,
    ST_AWAY_MSG, ST_AWAY_MEDIA,
    ST_KW_TRIGGER, ST_KW_RESPONSE, ST_KW_MEDIA,
    ST_FILTER_NAME, ST_FILTER_RESP, ST_FILTER_MEDIA,
    ST_BIO, ST_NAME, ST_USERNAME,
    ST_PROFILE_PIC,
    ST_SCHED_TARGET, ST_SCHED_MSG, ST_SCHED_TIME, ST_SCHED_MEDIA,
    ST_FWD_SOURCE, ST_FWD_DEST,
    ST_BLOCK_WORD, ST_WHITELIST,
    ST_PM_MSG, ST_PM_MEDIA,
    ST_SPAM_LIMIT, ST_SPAM_MSG,
    ST_ADMIN_BROADCAST, ST_ADMIN_BROADCAST_MEDIA,
    ST_ADMIN_SEARCH,
    ST_ADMIN_UPLOAD_DB,
    ST_TEMPLATE_NAME, ST_TEMPLATE_CONTENT, ST_TEMPLATE_MEDIA,
    ST_WORKING_HOURS,
    ST_REACT_EMOJI,
    ST_ADMIN_SET_PLAN, ST_ADMIN_SET_PLAN_DAYS,
    ST_ADMIN_BAN_REASON,
    ST_EXPORT_CONFIRM,
    ST_IMPORT_FILE,
    ST_CUSTOM_CMD_NAME, ST_CUSTOM_CMD_RESP, ST_CUSTOM_CMD_MEDIA,
    ST_ADMIN_ANNOUNCE,
    ST_FEEDBACK,
    ST_NOTE_TITLE, ST_NOTE_CONTENT, ST_NOTE_MEDIA,
    ST_AUTO_REPLY_DELAY,
    ST_ADMIN_FORCEJOIN_ADD, ST_ADMIN_FORCEJOIN_REMOVE,
) = range(54)
