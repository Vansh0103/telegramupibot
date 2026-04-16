import os

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from config import *
from app_logger import logger
from database import db
from bot_handlers import (
    cmd_start,
    cmd_help,
    cmd_cancel,
    cmd_status,
    cmd_plan,
    cmd_stats,
    cmd_export,
    cmd_feedback,
    on_callback,
    on_media,
    on_text,
    on_callback_media,
)
from workers import post_init

# ╔══════════════════════════════════════════════════════════════╗
# ║                    APPLICATION BUILDER                       ║
# ╚══════════════════════════════════════════════════════════════╝

def build_application() -> Application:
    """Build and configure the bot application."""
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .build()
    )

    # Command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("plan", cmd_plan))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("feedback", cmd_feedback))

    # Callback handlers
    app.add_handler(CallbackQueryHandler(
        on_callback_media,
        pattern=r"^(kw_add_media_|flt_add_media_)",
    ))
    app.add_handler(CallbackQueryHandler(on_callback))

    # Media handlers (must be before text handler)
    media_filter = (
        filters.PHOTO
        | filters.VIDEO
        | filters.ANIMATION
        | filters.Document.ALL
        | filters.VOICE
        | filters.AUDIO
        | filters.VIDEO_NOTE
        | filters.Sticker.ALL
    )
    app.add_handler(MessageHandler(media_filter, on_media))

    # Text handler (last)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, on_text
    ))

    return app


# ╔══════════════════════════════════════════════════════════════╗
# ║                       MAIN                                   ║
# ╚══════════════════════════════════════════════════════════════╝


def main():
    """Main entry point."""
    # Validate configuration
    errors = []
    if not BOT_TOKEN:
        errors.append("BOT_TOKEN is not set")
    if not API_ID:
        errors.append("API_ID is not set")
    if not API_HASH:
        errors.append("API_HASH is not set")
    if not ADMIN_ID:
        errors.append("ADMIN_ID is not set")

    if errors:
        for err in errors:
            logger.error("❌ Configuration: %s", err)
        raise RuntimeError(
            "Missing required configuration. "
            "Set BOT_TOKEN, API_ID, API_HASH, and ADMIN_ID."
        )

    # Create directories
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)

    # Build and run
    app = build_application()

    class BotPollingAdapter:
        def __init__(self, application):
            self.application = application

        def infinity_polling(self):
            self.application.run_polling(
                drop_pending_updates=False,
                allowed_updates=Update.ALL_TYPES,
            )

    bot = BotPollingAdapter(app)

    logger.info("=" * 50)
    logger.info("%s v%s", BOT_NAME, BOT_VERSION)
    logger.info("Admin: %s", ADMIN_ID)
    logger.info("Support: %s", SUPPORT_USERNAME)
    logger.info("Database: %s (%s)", DB_FILE, db.db_size())
    logger.info("Users: %s", db.total_users())
    logger.info("Sessions: %s", db.active_sessions_count())
    logger.info("=" * 50)
    logger.info("Starting polling...")

    bot.infinity_polling()


if __name__ == "__main__":
    main()
