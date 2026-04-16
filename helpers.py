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

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAnimation,
)
from telegram.ext import ContextTypes
from telegram.constants import ParseMode, ChatAction

from config import *
from app_logger import logger
from database import db

# ╔══════════════════════════════════════════════════════════════╗
# ║                     HELPERS                                  ║
# ╚══════════════════════════════════════════════════════════════╝

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def admin_only(fn):
    @wraps(fn)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                      *args, **kwargs):
        user = update.effective_user
        if not user or not is_admin(user.id):
            target = update.effective_message
            if target:
                await target.reply_text("⛔ Admin only.")
            return
        return await fn(update, ctx, *args, **kwargs)
    return wrapper


def fmt_bool(val) -> str:
    return "🟢" if val in ("true", True, 1, "1") else "🔴"


def fmt_plan(plan: str) -> str:
    config = PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])
    return config["name"]


def back_btn(cb="main_menu"):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("◀️ Back", callback_data=cb)]]
    )


def confirm_btns(yes_cb, no_cb="main_menu", yes_text="✅ Yes",
                  no_text="❌ Cancel"):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(yes_text, callback_data=yes_cb),
        InlineKeyboardButton(no_text, callback_data=no_cb),
    ]])


def substitute_vars(text: str, sender=None) -> str:
    if not text:
        return ""
    now = datetime.now()
    replacements = {
        "{time}": now.strftime("%H:%M:%S"),
        "{date}": now.strftime("%Y-%m-%d"),
        "{day}": now.strftime("%A"),
        "{bot}": BOT_NAME,
    }
    if sender:
        username = getattr(sender, "username", None)
        replacements.update({
            "{name}": getattr(sender, "first_name", "") or "",
            "{lastname}": getattr(sender, "last_name", "") or "",
            "{fullname}": (
                f"{getattr(sender, 'first_name', '') or ''} "
                f"{getattr(sender, 'last_name', '') or ''}"
            ).strip(),
            "{username}": f"@{username}" if username else "",
            "{id}": str(getattr(sender, "id", "")),
            "{mention}": (
                f"[{getattr(sender, 'first_name', 'User')}]"
                f"(tg://user?id={getattr(sender, 'id', 0)})"
            ),
        })
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text


def parse_bool(v, default=False):
    if v is None:
        return default
    return str(v).lower() in {"1", "true", "yes", "on"}


def truncate(text: str, max_len: int = 50) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + "..."


def get_media_info(message) -> Tuple[Optional[str], Optional[str]]:
    """Extract media file_id and type from a telegram message."""
    if not message:
        return None, None

    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.video:
        return message.video.file_id, "video"
    if message.animation:
        return message.animation.file_id, "animation"
    if message.document:
        return message.document.file_id, "document"
    if message.voice:
        return message.voice.file_id, "voice"
    if message.audio:
        return message.audio.file_id, "audio"
    if message.video_note:
        return message.video_note.file_id, "video_note"
    if message.sticker:
        return message.sticker.file_id, "sticker"
    return None, None


async def send_media_message(bot_or_client, chat_id, text=None,
                             media_file_id=None, media_type=None,
                             reply_to=None, parse_mode="Markdown",
                             is_telethon=False):
    """Universal media sender for both bot and telethon client."""
    if is_telethon:
        return await send_media_telethon(
            bot_or_client, chat_id, text,
            media_file_id, media_type, reply_to,
        )
    return await send_media_bot(
        bot_or_client, chat_id, text,
        media_file_id, media_type, reply_to, parse_mode,
    )


async def send_media_bot(bot, chat_id, text=None,
                         media_file_id=None, media_type=None,
                         reply_to=None, parse_mode="Markdown"):
    """Send media via telegram bot API."""
    try:
        if not media_file_id or not media_type:
            return await bot.send_message(
                chat_id=chat_id,
                text=text or "​",
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )

        caption = text[:MAX_CAPTION_LENGTH] if text else None
        methods = {
            "photo": bot.send_photo,
            "video": bot.send_video,
            "animation": bot.send_animation,
            "document": bot.send_document,
            "voice": bot.send_voice,
            "audio": bot.send_audio,
            "video_note": bot.send_video_note,
            "sticker": bot.send_sticker,
        }

        method = methods.get(media_type)
        if not method:
            return await bot.send_message(
                chat_id=chat_id,
                text=text or "​",
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )

        kwargs = {
            "chat_id": chat_id,
            media_type: media_file_id,
            "reply_to_message_id": reply_to,
        }

        if media_type not in ("sticker", "video_note"):
            kwargs["caption"] = caption
            kwargs["parse_mode"] = parse_mode

        return await method(**kwargs)
    except Exception as exc:
        logger.error("send_media_bot error: %s", exc)
        if text:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to,
            )


async def send_media_telethon(client, chat_id, text=None,
                              media_file_id=None, media_type=None,
                              reply_to=None):
    """Send media via telethon client."""
    try:
        if media_file_id and media_type:
            try:
                return await client.send_file(
                    chat_id,
                    media_file_id,
                    caption=text or "",
                    reply_to=reply_to,
                )
            except Exception:
                pass

        if text:
            return await client.send_message(
                chat_id,
                text,
                reply_to=reply_to,
            )
    except Exception as exc:
        logger.error("send_media_telethon error: %s", exc)


def system_setting(key: str, default=None):
    return db.get_setting(0, key, default)


def system_setting_bool(key: str, default=False) -> bool:
    return parse_bool(system_setting(key, str(default).lower()), default)


def parse_force_join_input(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    raw = (text or "").strip()
    if not raw:
        return "", None, None

    parts = [p.strip() for p in raw.split("|")]
    ident = parts[0] if parts else raw
    join_url = parts[1] if len(parts) > 1 and parts[1] else None
    title = parts[2] if len(parts) > 2 and parts[2] else None

    if ident.startswith("https://t.me/") or ident.startswith("http://t.me/"):
        slug = ident.rstrip("/").split("/")[-1]
        ident = f"@{slug}"
        join_url = join_url or raw.strip()
        title = title or f"@{slug}"
    elif ident.startswith("t.me/"):
        slug = ident.rstrip("/").split("/")[-1]
        ident = f"@{slug}"
        join_url = join_url or f"https://t.me/{slug}"
        title = title or f"@{slug}"
    elif ident.startswith("@"):
        slug = ident[1:]
        join_url = join_url or f"https://t.me/{slug}"
        title = title or ident
    else:
        ident = ident.replace(" ", "")
        title = title or ident

    if join_url and not (join_url.startswith("https://") or join_url.startswith("http://")):
        join_url = f"https://{join_url.lstrip('/')}"

    return ident, join_url, title


def format_force_join_channel(row) -> str:
    title = row["title"] or row["chat_ref"]
    link = f"\n   🔗 {row['join_url']}" if row["join_url"] else ""
    return f"• #{row['id']} — {title} (`{row['chat_ref']}`){link}"


async def get_missing_force_join_channels(bot, uid: int):
    if is_admin(uid):
        return []
    if not system_setting_bool("force_join_enabled", True):
        return []

    rows = db.get_force_join_channels(active_only=True)
    if not rows:
        return []

    missing = []
    for row in rows:
        try:
            member = await bot.get_chat_member(row["chat_ref"], uid)
            status = getattr(member, "status", "")
            if status in ("creator", "administrator", "member"):
                continue
            if status == "restricted" and getattr(member, "is_member", False):
                continue
            missing.append(row)
        except Exception:
            missing.append(row)
    return missing


def force_join_kb(missing_rows) -> InlineKeyboardMarkup:
    rows = []
    for row in missing_rows[:10]:
        url = row["join_url"]
        if not url and str(row["chat_ref"]).startswith("@"):
            url = f"https://t.me/{str(row['chat_ref'])[1:]}"
        if url:
            rows.append([InlineKeyboardButton(
                f"📢 Join {truncate(row['title'] or row['chat_ref'], 24)}",
                url=url,
            )])
    rows.append([InlineKeyboardButton("🔄 I Joined, Check Again", callback_data="forcejoin_refresh")])
    rows.append([InlineKeyboardButton("◀️ Main Menu", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


async def enforce_access(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                         allow_force_refresh: bool = False) -> bool:
    user = update.effective_user
    if not user:
        return False
    uid = user.id
    if is_admin(uid):
        return True

    if system_setting_bool("maintenance_mode", False):
        msg = (
            "🛠️ *Bot is under maintenance.*\n\n"
            "Please try again later or contact "
            f"{SUPPORT_USERNAME}."
        )
        if update.callback_query:
            await update.callback_query.answer("Bot is under maintenance.", show_alert=True)
            await update.callback_query.message.reply_text(msg, parse_mode="Markdown")
        elif update.effective_message:
            await update.effective_message.reply_text(msg, parse_mode="Markdown")
        return False

    if update.callback_query and allow_force_refresh and update.callback_query.data == "forcejoin_refresh":
        return True

    missing = await get_missing_force_join_channels(ctx.bot, uid)
    if not missing:
        return True

    lines = [
        "🔒 *Force Join Required*",
        "",
        "Join all required channels before using the bot:",
        "",
    ]
    for row in missing[:20]:
        lines.append(f"• {row['title'] or row['chat_ref']}")
    text = "\n".join(lines)
    kb = force_join_kb(missing)

    if update.callback_query:
        await update.callback_query.answer("Join required channels first.", show_alert=True)
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    elif update.effective_message:
        await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    return False


def plan_limit_text(uid: int, feature: str, current: int) -> str:
    """Get formatted limit info."""
    limit = db.plan_limit(uid, feature)
    plan = db.get_plan(uid)
    if current >= limit:
        return (
            f"❌ You've reached the limit ({current}/{limit}) "
            f"for your {fmt_plan(plan)} plan.\n"
            f"Upgrade to unlock more! Contact {SUPPORT_USERNAME}"
        )
    return ""


# ╔══════════════════════════════════════════════════════════════╗
# ║              KEYBOARD BUILDERS                               ║
# ╚══════════════════════════════════════════════════════════════╝


def main_kb(uid: int) -> InlineKeyboardMarkup:
    logged_in = bool(db.get_session(uid))
    plan = db.get_plan(uid)
    plan_icon = {"free": "🆓", "premium": "⭐", "vip": "👑"}.get(
        plan, "🆓"
    )

    rows = []
    if logged_in:
        rows = [
            [
                InlineKeyboardButton(
                    "📱 My Account", callback_data="account"
                ),
                InlineKeyboardButton(
                    f"⚙️ Settings {plan_icon}",
                    callback_data="settings_menu",
                ),
            ],
            [
                InlineKeyboardButton(
                    "💬 Welcome", callback_data="welcome_menu"
                ),
                InlineKeyboardButton(
                    "🔑 Keywords", callback_data="kw_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🤖 Away Mode", callback_data="away_menu"
                ),
                InlineKeyboardButton(
                    "📝 Filters", callback_data="filter_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🛡️ PM Permit", callback_data="pm_menu"
                ),
                InlineKeyboardButton(
                    "🔇 Anti-Spam", callback_data="spam_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🚫 Blocked Words", callback_data="bw_menu"
                ),
                InlineKeyboardButton(
                    "📋 Whitelist", callback_data="wl_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "⏰ Scheduled", callback_data="sched_menu"
                ),
                InlineKeyboardButton(
                    "↗️ Auto-Forward", callback_data="fwd_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "👤 Profile", callback_data="profile_menu"
                ),
                InlineKeyboardButton(
                    "📑 Templates", callback_data="tmpl_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "⏰ Work Hours", callback_data="wh_menu"
                ),
                InlineKeyboardButton(
                    "😍 Auto-React", callback_data="react_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📒 Notes", callback_data="notes_menu"
                ),
                InlineKeyboardButton(
                    "🤖 Custom Cmds", callback_data="ccmd_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📊 Stats", callback_data="stats_menu"
                ),
                InlineKeyboardButton(
                    "📜 Logs", callback_data="logs_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💎 My Plan", callback_data="plan_menu"
                ),
                InlineKeyboardButton(
                    "💬 Feedback", callback_data="feedback_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "📥 Backup", callback_data="backup_menu"
                ),
                InlineKeyboardButton(
                    "❓ Help", callback_data="help_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔄 Reconnect", callback_data="reconnect"
                ),
                InlineKeyboardButton(
                    "🚪 Logout", callback_data="logout_ask"
                ),
            ],
        ]
    else:
        rows = [
            [
                InlineKeyboardButton(
                    "🔐 Login", callback_data="login_start"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💎 Plans", callback_data="plan_info"
                ),
                InlineKeyboardButton(
                    "❓ Help", callback_data="help_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "💬 Support", url=SUPPORT_URL
                ),
            ],
        ]
    if is_admin(uid):
        rows.append([
            InlineKeyboardButton(
                "👑 Admin Panel", callback_data="admin_home"
            ),
        ])
    return InlineKeyboardMarkup(rows)


async def show_main(target, uid: int):
    plan = db.get_plan(uid)
    plan_text = fmt_plan(plan)
    await target.edit_message_text(
        f"🦴 *{BOT_NAME}* — Main Menu\n\n"
        f"Your Plan: {plan_text}",
        reply_markup=main_kb(uid),
        parse_mode="Markdown",
    )


async def ask_state(q, ctx, state, prompt, extra_kb=None):
    ctx.user_data["state"] = state
    kb = extra_kb
    if not kb:
        cancel_row = [
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_state")
        ]
        kb = InlineKeyboardMarkup([cancel_row])
    await q.edit_message_text(
        f"{prompt}\n\n_Send /cancel or tap Cancel to abort._",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def ask_state_msg(message, ctx, state, prompt):
    ctx.user_data["state"] = state
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_state")
    ]])
    await message.reply_text(
        f"{prompt}\n\n_Send /cancel to abort._",
        parse_mode="Markdown",
        reply_markup=kb,
    )
