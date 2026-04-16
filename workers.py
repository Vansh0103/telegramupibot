import os
import io
import json
import logging
import asyncio
import re
import sqlite3
import shutil
import tempfile
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict

from telegram.ext import Application

from config import *
from app_logger import logger
from database import db
from client_runtime import *

async def scheduled_worker(app: Application):
    """Background task to send scheduled messages."""
    logger.info("Scheduled worker started")
    await asyncio.sleep(5)

    while True:
        try:
            pending = db.pending_scheduled()
            for item in pending:
                uid = item["user_id"]
                sess = item["session_str"]
                if not sess:
                    continue

                temp_client = TelegramClient(
                    StringSession(sess),
                    API_ID,
                    API_HASH,
                    device_model=BOT_NAME,
                    system_version="2.0",
                    app_version=BOT_VERSION,
                )
                try:
                    await temp_client.connect()

                    if not await temp_client.is_user_authorized():
                        db.log(
                            uid, "sched_auth_fail",
                            f"id={item['id']}", "schedule",
                        )
                        continue

                    target = item["target"]
                    try:
                        target = int(target)
                    except (ValueError, TypeError):
                        pass

                    msg = item["message"] or ""
                    mid = item["media_file_id"]
                    mtype = item["media_type"]

                    if mid and mtype:
                        try:
                            await temp_client.send_file(
                                target, mid, caption=msg
                            )
                        except Exception:
                            await temp_client.send_message(
                                target, msg
                            )
                    else:
                        await temp_client.send_message(target, msg)

                    db.mark_sent(
                        item["id"],
                        bool(item["recurring"]),
                        int(item["interval_hr"] or 0),
                        int(item["max_repeats"] or 0),
                        int(item["repeat_count"] or 0),
                    )
                    db.inc_stat(uid, "scheduled_sent")
                    db.log(
                        uid, "scheduled_sent",
                        f"id={item['id']} to={item['target']}",
                        "schedule",
                    )

                    # Notify user
                    try:
                        await app.bot.send_message(
                            chat_id=uid,
                            text=(
                                f"✅ Scheduled message sent!\n"
                                f"To: `{item['target']}`\n"
                                f"Msg: {truncate(msg, 50)}"
                            ),
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass

                except Exception as exc:
                    db.log(
                        uid, "scheduled_error",
                        f"id={item['id']} err={str(exc)[:100]}",
                        "schedule",
                    )
                    logger.error(
                        "Scheduled send error uid=%s: %s",
                        uid, exc,
                    )
                finally:
                    try:
                        await temp_client.disconnect()
                    except Exception:
                        pass

        except Exception as exc:
            logger.exception("scheduled_worker error: %s", exc)

        await asyncio.sleep(15)


async def plan_expiry_checker(app: Application):
    """Check and handle expiring plans."""
    logger.info("Plan expiry checker started")
    await asyncio.sleep(30)

    while True:
        try:
            # Check for expired plans
            now = datetime.now().isoformat()
            with db.conn() as cx:
                expired = cx.execute(
                    """SELECT * FROM users
                       WHERE plan != 'free'
                       AND plan_until IS NOT NULL
                       AND plan_until < ?""",
                    (now,),
                ).fetchall()

            for user in expired:
                uid = user["user_id"]
                old_plan = user["plan"]
                db.set_plan(uid, "free", admin_id=0, auto=True)
                logger.info(
                    "Plan expired for uid=%s (%s → free)",
                    uid, old_plan,
                )

                # Notify user
                try:
                    await app.bot.send_message(
                        chat_id=uid,
                        text=(
                            f"⚠️ *Plan Expired*\n\n"
                            f"Your {fmt_plan(old_plan)} plan has expired.\n"
                            f"You've been moved to the 🆓 Free plan.\n\n"
                            f"Contact {SUPPORT_USERNAME} to renew!"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            # Notify users with plans expiring in 3 days
            expiring = db.expiring_plans(3)
            for user in expiring:
                uid = user["user_id"]
                try:
                    exp = datetime.fromisoformat(user["plan_until"])
                    days_left = (exp - datetime.now()).days
                    if days_left in (3, 1):
                        notif_key = f"expiry_notified_{days_left}d"
                        if db.get_stat(uid, notif_key) == 0:
                            await app.bot.send_message(
                                chat_id=uid,
                                text=(
                                    f"⏳ *Plan Expiring Soon!*\n\n"
                                    f"Your {fmt_plan(user['plan'])} plan "
                                    f"expires in *{days_left}* day(s).\n\n"
                                    f"Renew now! Contact {SUPPORT_USERNAME}"
                                ),
                                parse_mode="Markdown",
                            )
                            db.inc_stat(uid, notif_key)
                except Exception:
                    pass

        except Exception as exc:
            logger.exception("plan_expiry_checker error: %s", exc)

        await asyncio.sleep(3600)


async def cleanup_worker():
    """Periodic database cleanup."""
    logger.info("Cleanup worker started")
    await asyncio.sleep(60)

    while True:
        try:
            db.cleanup()
            logger.info("Database cleanup completed")
        except Exception as exc:
            logger.exception("cleanup_worker error: %s", exc)
        await asyncio.sleep(3600 * 6)


async def health_check_worker():
    """Check active client connections periodically."""
    logger.info("Health check worker started")
    await asyncio.sleep(120)

    while True:
        try:
            disconnected = []
            for uid, client in list(active_clients.items()):
                try:
                    if not client.is_connected():
                        disconnected.append(uid)
                except Exception:
                    disconnected.append(uid)

            for uid in disconnected:
                logger.info("Reconnecting uid=%s", uid)
                try:
                    await start_client(uid)
                except Exception as exc:
                    logger.error(
                        "Health reconnect failed uid=%s: %s",
                        uid, exc,
                    )

        except Exception as exc:
            logger.exception("health_check error: %s", exc)

        await asyncio.sleep(300)


async def reconnect_saved_clients():
    """Reconnect all saved sessions on startup."""
    logger.info("Reconnecting saved clients...")
    users = db.users_with_sessions()
    connected = 0
    failed = 0

    for user in users:
        uid = user["user_id"]
        try:
            client = await start_client(uid)
            if client:
                connected += 1
            else:
                failed += 1
        except Exception as exc:
            logger.error(
                "Reconnect failed uid=%s: %s", uid, exc
            )
            failed += 1
        await asyncio.sleep(1)

    logger.info(
        "Reconnect complete: %d connected, %d failed",
        connected, failed,
    )


async def post_init(app: Application):
    """Initialize background tasks after bot starts."""
    logger.info("%s v%s initializing...", BOT_NAME, BOT_VERSION)

    asyncio.create_task(reconnect_saved_clients())
    asyncio.create_task(scheduled_worker(app))
    asyncio.create_task(plan_expiry_checker(app))
    asyncio.create_task(cleanup_worker())
    asyncio.create_task(health_check_worker())

    logger.info("All background tasks started")

    # Notify admin
    try:
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🟢 *{BOT_NAME} v{BOT_VERSION} Started*\n\n"
                f"Users: {db.total_users()}\n"
                f"Sessions: {db.active_sessions_count()}\n"
                f"DB Size: {db.db_size()}"
            ),
            parse_mode="Markdown",
        )
    except Exception:
        pass
