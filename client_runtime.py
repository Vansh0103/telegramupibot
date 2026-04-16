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

from telegram import Update
from telegram.ext import ContextTypes
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession

from config import *
from app_logger import logger
from database import db
from shared import active_clients, client_locks
from helpers import *

# ╔══════════════════════════════════════════════════════════════╗
# ║              TELETHON CLIENT MANAGEMENT                      ║
# ╚══════════════════════════════════════════════════════════════╝

def register_handlers(client: TelegramClient, uid: int):
    """Register all event handlers for a user's client."""

    @client.on(events.NewMessage(
        incoming=True, func=lambda e: e.is_private
    ))
    async def on_pm(event):
        try:
            await handle_pm(event, uid, client)
        except Exception as exc:
            logger.exception(
                "PM handler error uid=%s: %s", uid, exc
            )

    @client.on(events.NewMessage(
        incoming=True, func=lambda e: not e.is_private
    ))
    async def on_group(event):
        try:
            await handle_group(event, uid, client)
        except Exception as exc:
            logger.exception(
                "Group handler error uid=%s: %s", uid, exc
            )


async def start_client(uid: int) -> Optional[TelegramClient]:
    """Start or restart a user's Telethon client."""
    sess = db.get_session(uid)
    if not sess:
        return None

    async with client_locks[uid]:
        old = active_clients.pop(uid, None)
        if old:
            try:
                await old.disconnect()
            except Exception:
                pass

        try:
            client = TelegramClient(
                StringSession(sess),
                API_ID,
                API_HASH,
                device_model=BOT_NAME,
                system_version="2.0",
                app_version=BOT_VERSION,
                flood_sleep_threshold=60,
            )
            await client.connect()

            if not await client.is_user_authorized():
                db.remove_session(uid)
                db.log(uid, "auth_fail", "Session expired", "system")
                return None

            register_handlers(client, uid)
            active_clients[uid] = client
            db.log(uid, "client_start", "Userbot connected", "system")
            db.touch_user(uid)
            return client

        except (AuthKeyUnregisteredError, UserDeactivatedBanError):
            db.remove_session(uid)
            db.log(uid, "session_invalid", "Session revoked", "system")
            return None
        except Exception as exc:
            logger.exception(
                "start_client uid=%s failed: %s", uid, exc
            )
            db.log(uid, "client_error", str(exc)[:200], "system")
            return None


async def stop_client(uid: int):
    """Stop a user's Telethon client."""
    async with client_locks[uid]:
        client = active_clients.pop(uid, None)
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
    db.log(uid, "client_stop", "Userbot disconnected", "system")


def get_client(uid: int) -> Optional[TelegramClient]:
    """Get active client for a user."""
    client = active_clients.get(uid)
    if client and client.is_connected():
        return client
    return None


# ╔══════════════════════════════════════════════════════════════╗
# ║            TELETHON RUNTIME HANDLERS                         ║
# ╚══════════════════════════════════════════════════════════════╝


async def reply_with_media(event, text, media_file_id, media_type,
                           client):
    """Reply to a telethon event with optional media."""
    if media_file_id and media_type:
        try:
            await client.send_file(
                event.chat_id,
                media_file_id,
                caption=text or "",
                reply_to=event.id,
            )
            return
        except Exception as exc:
            logger.debug("Media reply fallback: %s", exc)

    if text:
        await event.reply(text)


async def handle_pm(event, uid: int, client: TelegramClient):
    """Handle incoming private messages for a user."""
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return
    if sender.id == uid:
        return

    sid = sender.id
    text = event.raw_text or ""
    db.inc_stat(uid, "messages_received")
    db.touch_user(uid)

    # Check if within working hours
    if not db.is_working_hours(uid):
        wh_msg = db.get_setting(
            uid, "wh_message",
            "🕐 I'm currently outside working hours. "
            "I'll respond when I'm back!"
        )
        wh_once = db.get_setting(uid, "wh_notify_once", "true")
        if wh_once == "true":
            k = f"wh_notified_{sid}"
            if db.get_stat(uid, k) == 0:
                await event.reply(substitute_vars(wh_msg, sender))
                db.inc_stat(uid, k)
        else:
            await event.reply(substitute_vars(wh_msg, sender))
        return

    # Check whitelist
    s_uname = getattr(sender, "username", None)
    in_wl = db.is_whitelisted(uid, sid, s_uname)

    # Anti-spam check
    if (
        not in_wl
        and db.get_setting(uid, "anti_spam", "false") == "true"
    ):
        try:
            limit = int(db.get_setting(uid, "spam_limit", "5"))
        except ValueError:
            limit = 5

        if db.is_spam_blocked(uid, sid):
            return

        if db.check_spam(uid, sid, limit):
            spam_msg = db.get_setting(
                uid, "spam_msg",
                "⚠️ Spam detected. Your messages are being ignored."
            )
            spam_media = db.get_setting(uid, "spam_media_id")
            spam_media_type = db.get_setting(uid, "spam_media_type")
            await reply_with_media(
                event, spam_msg, spam_media, spam_media_type, client
            )
            db.log(uid, "spam_block", f"sender={sid}", "spam")
            db.inc_stat(uid, "spam_blocked")
            return

    # Blocked words check
    for bw in db.get_blocked(uid):
        if bw["word"] in text.lower() and not in_wl:
            action = bw["action"] or "warn"
            if action == "delete":
                try:
                    await event.delete()
                except Exception:
                    await event.reply(
                        "⚠️ Your message contained a blocked word."
                    )
            elif action == "mute":
                await event.reply(
                    "🔇 You've been muted for using a blocked word."
                )
            else:
                await event.reply(
                    "⚠️ Your message contained a blocked word."
                )
            db.log(
                uid, "blocked_word",
                f"word={bw['word']} sender={sid}",
                "moderation",
            )
            db.inc_stat(uid, "blocked_word_triggered")
            return

    # PM Permit check
    if (
        not in_wl
        and db.get_setting(uid, "pm_permit", "false") == "true"
    ):
        if not db.is_pm_approved(uid, sid):
            pm_msg = db.get_setting(
                uid, "pm_msg",
                "⚠️ You are not approved to PM me. Please wait."
            )
            pm_media = db.get_setting(uid, "pm_media_id")
            pm_media_type = db.get_setting(uid, "pm_media_type")

            try:
                limit = int(db.get_setting(uid, "pm_limit", "3"))
            except ValueError:
                limit = 3

            stat_k = f"pm_warn_{sid}"
            warns = db.get_stat(uid, stat_k)
            if warns >= limit:
                pm_block_msg = db.get_setting(
                    uid, "pm_block_msg",
                    "🚫 You have reached the PM permit limit."
                )
                await event.reply(pm_block_msg)
                db.log(uid, "pm_block", f"sender={sid}", "pm")
                db.inc_stat(uid, "pm_blocked")
                return

            db.inc_stat(uid, stat_k)
            remaining = max(limit - warns - 1, 0)
            msg = (
                f"{substitute_vars(pm_msg, sender)}\n\n"
                f"⚠️ {remaining} warning(s) left."
            )
            await reply_with_media(
                event, msg, pm_media, pm_media_type, client
            )
            db.inc_stat(uid, "pm_warnings_sent")
            return

    # Check custom commands (if user typed /something)
    if text.startswith("/"):
        cmd_text = text[1:].split()[0].lower()
        for cmd in db.get_custom_cmds(uid):
            if cmd["command"] == cmd_text:
                resp = substitute_vars(cmd["response"], sender)
                await reply_with_media(
                    event, resp,
                    cmd["media_file_id"], cmd["media_type"],
                    client,
                )
                db.custom_cmd_inc(cmd["id"])
                db.inc_stat(uid, "custom_cmd_used")
                return

    # Keyword matching
    for kw in db.get_keywords(uid):
        matched = False
        trigger = kw["trigger_text"]
        mt = kw["match_type"]
        tl = text.lower()

        if mt == "exact" and tl == trigger:
            matched = True
        elif mt == "contains" and trigger in tl:
            matched = True
        elif mt == "startswith" and tl.startswith(trigger):
            matched = True
        elif mt == "endswith" and tl.endswith(trigger):
            matched = True
        elif mt == "regex":
            if db.plan_check(uid, "regex_keywords"):
                try:
                    matched = bool(re.search(trigger, text, re.I))
                except re.error:
                    matched = False

        if matched:
            delay = kw["reply_delay"] or 0
            if delay > 0:
                await asyncio.sleep(delay)

            resp = substitute_vars(kw["response"], sender)
            await reply_with_media(
                event, resp,
                kw["media_file_id"], kw["media_type"],
                client,
            )
            db.kw_inc(kw["id"])
            db.inc_stat(uid, "keyword_replies")
            db.log(
                uid, "kw_reply",
                f"trigger={trigger}", "keyword",
            )
            return

    # Filter matching
    for filt in db.get_filters(uid):
        if filt["name"] in text.lower():
            resp = substitute_vars(filt["response"], sender)
            await reply_with_media(
                event, resp,
                filt["media_file_id"], filt["media_type"],
                client,
            )
            db.filter_inc(filt["id"])
            db.inc_stat(uid, "filter_replies")
            db.log(
                uid, "filter_reply",
                f"name={filt['name']}", "filter",
            )
            return

    # Welcome message
    if db.get_setting(uid, "welcome", "false") == "true":
        w_msg = db.get_setting(
            uid, "welcome_msg",
            "👋 Hi {name}! Thanks for messaging me."
        )
        w_media = db.get_setting(uid, "welcome_media_id")
        w_media_type = db.get_setting(uid, "welcome_media_type")
        w_mode = db.get_setting(uid, "welcome_mode", "first_time")
        msg = substitute_vars(w_msg, sender)

        if w_mode == "always":
            await reply_with_media(
                event, msg, w_media, w_media_type, client
            )
            db.inc_stat(uid, "welcome_sent")
        else:
            k = f"welcomed_{sid}"
            if db.get_stat(uid, k) == 0:
                await reply_with_media(
                    event, msg, w_media, w_media_type, client
                )
                db.inc_stat(uid, k)
                db.inc_stat(uid, "welcome_sent")

    # Away message
    if db.get_setting(uid, "away", "false") == "true":
        a_msg = db.get_setting(
            uid, "away_msg",
            "🌙 I'm currently away. I'll reply when I'm back!"
        )
        a_media = db.get_setting(uid, "away_media_id")
        a_media_type = db.get_setting(uid, "away_media_type")
        msg = substitute_vars(a_msg, sender)
        await reply_with_media(
            event, msg, a_media, a_media_type, client
        )
        db.inc_stat(uid, "away_sent")

    # Auto-react
    if (
        db.plan_check(uid, "auto_react")
        and db.get_setting(uid, "auto_react", "false") == "true"
    ):
        emoji = db.get_setting(uid, "react_emoji", "👍")
        try:
            await client(SendReactionRequest(
                peer=event.chat_id,
                msg_id=event.id,
                reaction=[ReactionEmoji(emoticon=emoji)],
            ))
            db.inc_stat(uid, "auto_reacted")
        except Exception:
            pass


async def handle_group(event, uid: int, client: TelegramClient):
    """Handle incoming group messages for auto-forwarding."""
    for rule in db.get_forwards(uid):
        try:
            chat = await event.get_chat()
            cid_str = str(chat.id)
            uname = getattr(chat, "username", "") or ""
            src = str(rule["source"])

            match = src in (
                cid_str, uname, f"@{uname}",
                str(-100) + cid_str,
            )
            if not match and uname:
                match = src.lower().lstrip("@") == uname.lower()

            if match:
                # Apply text filter if set
                ft = rule["filter_text"]
                if ft and ft.lower() not in (
                    event.raw_text or ""
                ).lower():
                    continue

                dest = rule["dest"]
                try:
                    dest = int(dest)
                except (ValueError, TypeError):
                    pass

                if rule["forward_media"] or not event.media:
                    await client.forward_messages(
                        dest, event.message
                    )
                else:
                    if event.raw_text:
                        await client.send_message(
                            dest, event.raw_text
                        )

                db.inc_stat(uid, "messages_forwarded")

        except Exception as exc:
            logger.debug(
                "Forward error uid=%s: %s", uid, exc
            )

# ══════════════════════════════════════════════════════════════
# PART 2 OF 3 — Bot Commands, Menus, Callback Router
# Place this code directly after Part 1 in the same file
# ══════════════════════════════════════════════════════════════


# ╔══════════════════════════════════════════════════════════════╗
# ║                    BOT COMMANDS                              ║
# ╚══════════════════════════════════════════════════════════════╝

