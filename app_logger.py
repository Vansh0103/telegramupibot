import os
import logging

from config import BACKUP_DIR, MEDIA_DIR, BOT_NAME

# ╔══════════════════════════════════════════════════════════════╗
# ║                      LOGGING                                ║
# ╚══════════════════════════════════════════════════════════════╝

os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{BOT_NAME.lower()}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(BOT_NAME)
