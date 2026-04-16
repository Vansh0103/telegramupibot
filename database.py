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

from telethon import TelegramClient

from config import DB_FILE
from app_logger import logger

# ╔══════════════════════════════════════════════════════════════╗
# ║                      DATABASE                               ║
# ╚══════════════════════════════════════════════════════════════╝

class Database:
    """SQLite database manager with full feature support."""

    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self._init()

    def conn(self):
        c = sqlite3.connect(self.db_file, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        return c

    def _init(self):
        with self.conn() as cx:
            cx.executescript("""
            -- Users table
            CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER PRIMARY KEY,
                username      TEXT,
                first_name    TEXT,
                last_name     TEXT,
                phone         TEXT,
                session_str   TEXT,
                joined_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                last_active   TEXT DEFAULT CURRENT_TIMESTAMP,
                is_banned     INTEGER DEFAULT 0,
                ban_reason    TEXT,
                plan          TEXT DEFAULT 'free',
                plan_until    TEXT,
                referral_code TEXT,
                referred_by   INTEGER,
                language      TEXT DEFAULT 'en',
                timezone      TEXT DEFAULT 'UTC'
            );

            -- Settings table
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER,
                key     TEXT,
                value   TEXT,
                PRIMARY KEY (user_id, key)
            );

            -- Keywords with full media support
            CREATE TABLE IF NOT EXISTS keywords (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                trigger_text TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                match_type  TEXT DEFAULT 'contains',
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                reply_delay INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Filters with media
            CREATE TABLE IF NOT EXISTS filters (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                name        TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Blocked words
            CREATE TABLE IF NOT EXISTS blocked_words (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                word    TEXT,
                action  TEXT DEFAULT 'warn',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Whitelist
            CREATE TABLE IF NOT EXISTS whitelist (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                target_user TEXT,
                target_name TEXT,
                added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Scheduled messages with media
            CREATE TABLE IF NOT EXISTS scheduled (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                target       TEXT,
                message      TEXT,
                media_file_id TEXT,
                media_type   TEXT,
                send_at      TEXT,
                is_sent      INTEGER DEFAULT 0,
                sent_at      TEXT,
                recurring    INTEGER DEFAULT 0,
                interval_hr  INTEGER DEFAULT 0,
                max_repeats  INTEGER DEFAULT 0,
                repeat_count INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Auto-forward rules
            CREATE TABLE IF NOT EXISTS auto_forward (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                source   TEXT,
                dest     TEXT,
                active   INTEGER DEFAULT 1,
                filter_text TEXT,
                forward_media INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- PM permit
            CREATE TABLE IF NOT EXISTS pm_permit (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER,
                approved     INTEGER,
                approved_name TEXT,
                approved_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                auto_approved INTEGER DEFAULT 0,
                UNIQUE(user_id, approved)
            );

            -- Stats
            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER,
                key     TEXT,
                value   INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, key)
            );

            -- Daily stats for analytics
            CREATE TABLE IF NOT EXISTS daily_stats (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date    TEXT,
                key     TEXT,
                value   INTEGER DEFAULT 0,
                UNIQUE(user_id, date, key)
            );

            -- Logs
            CREATE TABLE IF NOT EXISTS logs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                action    TEXT,
                detail    TEXT,
                category  TEXT DEFAULT 'general',
                ts        TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Spam tracking
            CREATE TABLE IF NOT EXISTS spam_track (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                sender   INTEGER,
                count    INTEGER DEFAULT 0,
                last_ts  TEXT,
                blocked  INTEGER DEFAULT 0,
                UNIQUE(user_id, sender)
            );

            -- Templates
            CREATE TABLE IF NOT EXISTS templates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                name        TEXT,
                content     TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                category    TEXT DEFAULT 'general',
                is_global   INTEGER DEFAULT 0,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Working hours
            CREATE TABLE IF NOT EXISTS working_hours (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER,
                day      INTEGER,
                start_hr INTEGER DEFAULT 9,
                start_min INTEGER DEFAULT 0,
                end_hr   INTEGER DEFAULT 17,
                end_min  INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                UNIQUE(user_id, day)
            );

            -- Custom commands
            CREATE TABLE IF NOT EXISTS custom_commands (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                command     TEXT,
                response    TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_active   INTEGER DEFAULT 1,
                used_count  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Notes / saved messages
            CREATE TABLE IF NOT EXISTS notes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                title       TEXT,
                content     TEXT,
                media_file_id TEXT,
                media_type  TEXT,
                is_pinned   INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            );

            -- Media attachments (for multi-media support)
            CREATE TABLE IF NOT EXISTS media_attachments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                parent_type TEXT,
                parent_id   INTEGER,
                file_id     TEXT,
                media_type  TEXT,
                caption     TEXT,
                position    INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Feedback
            CREATE TABLE IF NOT EXISTS feedback (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                message   TEXT,
                status    TEXT DEFAULT 'pending',
                admin_reply TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                replied_at TEXT
            );

            -- Plan history
            CREATE TABLE IF NOT EXISTS plan_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER,
                old_plan  TEXT,
                new_plan  TEXT,
                days      INTEGER,
                changed_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Announcements
            CREATE TABLE IF NOT EXISTS announcements (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                title     TEXT,
                content   TEXT,
                media_file_id TEXT,
                media_type TEXT,
                target    TEXT DEFAULT 'all',
                created_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );


            -- Force join channels
            CREATE TABLE IF NOT EXISTS force_join_channels (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_ref   TEXT UNIQUE,
                join_url   TEXT,
                title      TEXT,
                is_active  INTEGER DEFAULT 1,
                added_by   INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_keywords_user ON keywords(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_filters_user ON filters(user_id, is_active);
            CREATE INDEX IF NOT EXISTS idx_scheduled_pending ON scheduled(is_sent, send_at);
            CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id, ts);
            CREATE INDEX IF NOT EXISTS idx_spam_track ON spam_track(user_id, sender);
            CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(user_id, date);
            CREATE INDEX IF NOT EXISTS idx_force_join_active ON force_join_channels(is_active, chat_ref);
            """)

    # ═══════════════════ USER METHODS ═══════════════════

    def add_user(self, uid, username=None, first_name=None, last_name=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO users(user_id, username, first_name, last_name, joined_at, last_active)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       username=COALESCE(excluded.username, users.username),
                       first_name=COALESCE(excluded.first_name, users.first_name),
                       last_name=COALESCE(excluded.last_name, users.last_name),
                       last_active=excluded.last_active""",
                (uid, username, first_name, last_name,
                 datetime.now().isoformat(), datetime.now().isoformat()),
            )

    def get_user(self, uid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE user_id=?", (uid,)
            ).fetchone()

    def all_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users ORDER BY last_active DESC"
            ).fetchall()

    def users_with_sessions(self):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM users
                   WHERE session_str IS NOT NULL
                   AND session_str != ''
                   AND is_banned=0"""
            ).fetchall()

    def users_by_plan(self, plan: str):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan=? ORDER BY last_active DESC",
                (plan,),
            ).fetchall()

    def active_users(self, days: int = 7):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE last_active > ? ORDER BY last_active DESC",
                (cutoff,),
            ).fetchall()

    def update_user(self, uid, **kw):
        if not kw:
            return
        allowed = {
            "username", "first_name", "last_name", "phone",
            "session_str", "is_banned", "ban_reason", "plan",
            "plan_until", "referral_code", "referred_by",
            "language", "timezone", "last_active",
        }
        sets = []
        vals = []
        for k, v in kw.items():
            if k in allowed:
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        vals.append(uid)
        with self.conn() as cx:
            cx.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE user_id=?",
                vals,
            )

    def touch_user(self, uid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE users SET last_active=? WHERE user_id=?",
                (datetime.now().isoformat(), uid),
            )

    def delete_user_data(self, uid):
        tables = [
            "settings", "keywords", "filters", "blocked_words",
            "whitelist", "scheduled", "auto_forward", "pm_permit",
            "stats", "daily_stats", "logs", "spam_track", "templates",
            "working_hours", "custom_commands", "notes",
            "media_attachments", "feedback",
        ]
        with self.conn() as cx:
            for t in tables:
                cx.execute(f"DELETE FROM {t} WHERE user_id=?", (uid,))

    def full_delete_user(self, uid):
        self.delete_user_data(uid)
        with self.conn() as cx:
            cx.execute("DELETE FROM users WHERE user_id=?", (uid,))

    # ═══════════════════ BAN METHODS ═══════════════════

    def ban_user(self, uid, reason=""):
        self.update_user(uid, is_banned=1, ban_reason=reason)
        self.log(uid, "banned", reason, "admin")

    def unban_user(self, uid):
        self.update_user(uid, is_banned=0, ban_reason=None)
        self.log(uid, "unbanned", "", "admin")

    def is_banned(self, uid):
        u = self.get_user(uid)
        return bool(u and u["is_banned"])

    def banned_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE is_banned=1"
            ).fetchall()

    # ═══════════════════ PLAN METHODS ═══════════════════

    def get_plan(self, uid) -> str:
        u = self.get_user(uid)
        if not u:
            return "free"
        plan = u["plan"] or "free"
        plan_until = u["plan_until"]
        if plan != "free" and plan_until:
            try:
                if datetime.fromisoformat(plan_until) < datetime.now():
                    self.set_plan(uid, "free", admin_id=0, auto=True)
                    return "free"
            except (ValueError, TypeError):
                self.set_plan(uid, "free", admin_id=0, auto=True)
                return "free"
        return plan

    def get_plan_config(self, uid) -> dict:
        plan = self.get_plan(uid)
        return PLAN_CONFIG.get(plan, PLAN_CONFIG["free"])

    def get_plan_expiry(self, uid) -> Optional[str]:
        u = self.get_user(uid)
        if u and u["plan_until"]:
            return u["plan_until"]
        return None

    def set_plan(self, uid, plan: str, days: int = 0,
                 admin_id: int = 0, auto: bool = False):
        old_plan = self.get_plan(uid)
        expiry = None
        if plan != "free" and days > 0:
            expiry = (datetime.now() + timedelta(days=days)).isoformat()

        self.update_user(uid, plan=plan, plan_until=expiry)

        with self.conn() as cx:
            cx.execute(
                """INSERT INTO plan_history
                   (user_id, old_plan, new_plan, days, changed_by)
                   VALUES (?,?,?,?,?)""",
                (uid, old_plan, plan, days, admin_id),
            )

        if not auto:
            self.log(
                uid, "plan_change",
                f"{old_plan} → {plan} ({days}d) by {admin_id}",
                "plan",
            )

    def plan_check(self, uid, feature: str) -> bool:
        config = self.get_plan_config(uid)
        return config.get(feature, False)

    def plan_limit(self, uid, feature: str) -> int:
        config = self.get_plan_config(uid)
        return config.get(feature, 0)

    def premium_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan IN ('premium', 'vip')"
            ).fetchall()

    def vip_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM users WHERE plan='vip'"
            ).fetchall()

    def expiring_plans(self, days: int = 3):
        cutoff = (datetime.now() + timedelta(days=days)).isoformat()
        now = datetime.now().isoformat()
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM users
                   WHERE plan != 'free'
                   AND plan_until IS NOT NULL
                   AND plan_until BETWEEN ? AND ?""",
                (now, cutoff),
            ).fetchall()

    def plan_history(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM plan_history
                   WHERE user_id=?
                   ORDER BY created_at DESC LIMIT 20""",
                (uid,),
            ).fetchall()

    # ═══════════════════ SESSION METHODS ═══════════════════

    def save_session(self, uid, phone, session_str):
        with self.conn() as cx:
            cx.execute(
                "UPDATE users SET phone=?, session_str=? WHERE user_id=?",
                (phone, session_str, uid),
            )

    def get_session(self, uid):
        u = self.get_user(uid)
        return u["session_str"] if u else None

    def remove_session(self, uid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE users
                   SET session_str=NULL, phone=NULL
                   WHERE user_id=?""",
                (uid,),
            )

    # ═══════════════════ SETTINGS METHODS ═══════════════════

    def set_setting(self, uid, key, value):
        with self.conn() as cx:
            cx.execute(
                """INSERT OR REPLACE INTO settings(user_id, key, value)
                   VALUES(?,?,?)""",
                (uid, key, str(value)),
            )

    def get_setting(self, uid, key, default=None):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT value FROM settings WHERE user_id=? AND key=?",
                (uid, key),
            ).fetchone()
        return r["value"] if r else default

    def all_settings(self, uid):
        with self.conn() as cx:
            rows = cx.execute(
                "SELECT key, value FROM settings WHERE user_id=?",
                (uid,),
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def del_setting(self, uid, key):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM settings WHERE user_id=? AND key=?",
                (uid, key),
            )

    def bulk_set_settings(self, uid, settings_dict: dict):
        with self.conn() as cx:
            for key, value in settings_dict.items():
                cx.execute(
                    """INSERT OR REPLACE INTO settings(user_id, key, value)
                       VALUES(?,?,?)""",
                    (uid, key, str(value)),
                )

    # ═══════════════════ FORCE JOIN METHODS ═══════════════════

    def add_force_join_channel(self, chat_ref: str, join_url: Optional[str] = None,
                               title: Optional[str] = None, added_by: int = 0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO force_join_channels(chat_ref, join_url, title, is_active, added_by)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(chat_ref) DO UPDATE SET
                       join_url=excluded.join_url,
                       title=COALESCE(excluded.title, force_join_channels.title),
                       is_active=1,
                       added_by=excluded.added_by""",
                (str(chat_ref).strip(), join_url, title, 1, added_by),
            )

    def get_force_join_channels(self, active_only: bool = True):
        query = "SELECT * FROM force_join_channels"
        if active_only:
            query += " WHERE is_active=1"
        query += " ORDER BY id ASC"
        with self.conn() as cx:
            return cx.execute(query).fetchall()

    def force_join_count(self, active_only: bool = True) -> int:
        with self.conn() as cx:
            if active_only:
                row = cx.execute(
                    "SELECT COUNT(*) AS c FROM force_join_channels WHERE is_active=1"
                ).fetchone()
            else:
                row = cx.execute(
                    "SELECT COUNT(*) AS c FROM force_join_channels"
                ).fetchone()
        return int(row["c"] if row else 0)

    def remove_force_join_channel(self, identifier: str) -> int:
        ident = str(identifier).strip()
        with self.conn() as cx:
            if ident.isdigit():
                cur = cx.execute(
                    "DELETE FROM force_join_channels WHERE id=?",
                    (int(ident),),
                )
                if cur.rowcount:
                    return cur.rowcount
            cur = cx.execute(
                "DELETE FROM force_join_channels WHERE chat_ref=? OR title=? OR join_url=?",
                (ident, ident, ident),
            )
            return cur.rowcount

    # ═══════════════════ KEYWORD METHODS ═══════════════════

    def add_keyword(self, uid, trigger, response, match_type="contains",
                    media_file_id=None, media_type=None, reply_delay=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO keywords
                   (user_id, trigger_text, response, match_type,
                    media_file_id, media_type, reply_delay)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, trigger.lower().strip(), response, match_type,
                 media_file_id, media_type, reply_delay),
            )
            return cx.execute("SELECT last_insert_rowid()").fetchone()[0]

    def update_keyword(self, uid, kid, **kw):
        allowed = {
            "trigger_text", "response", "match_type",
            "media_file_id", "media_type", "is_active",
            "reply_delay",
        }
        sets = []
        vals = []
        for k, v in kw.items():
            if k in allowed:
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        sets.append("updated_at=?")
        vals.append(datetime.now().isoformat())
        vals.extend([kid, uid])
        with self.conn() as cx:
            cx.execute(
                f"UPDATE keywords SET {', '.join(sets)} WHERE id=? AND user_id=?",
                vals,
            )

    def get_keywords(self, uid, active_only=True):
        q = "SELECT * FROM keywords WHERE user_id=?"
        if active_only:
            q += " AND is_active=1"
        q += " ORDER BY id DESC"
        with self.conn() as cx:
            return cx.execute(q, (uid,)).fetchall()

    def get_keyword(self, uid, kid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            ).fetchone()

    def del_keyword(self, uid, kid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            )

    def toggle_keyword(self, uid, kid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT is_active FROM keywords WHERE id=? AND user_id=?",
                (kid, uid),
            ).fetchone()
            if r:
                new_val = 0 if r["is_active"] else 1
                cx.execute(
                    """UPDATE keywords SET is_active=?, updated_at=?
                       WHERE id=? AND user_id=?""",
                    (new_val, datetime.now().isoformat(), kid, uid),
                )

    def clear_keywords(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM keywords WHERE user_id=?", (uid,))

    def kw_inc(self, kid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE keywords SET used_count=used_count+1 WHERE id=?",
                (kid,),
            )

    def keyword_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM keywords WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ FILTER METHODS ═══════════════════

    def add_filter(self, uid, name, response,
                   media_file_id=None, media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO filters
                   (user_id, name, response, media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, name.lower().strip(), response,
                 media_file_id, media_type),
            )
            return cx.execute("SELECT last_insert_rowid()").fetchone()[0]

    def get_filters(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM filters
                   WHERE user_id=? AND is_active=1
                   ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def get_filter(self, uid, fid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM filters WHERE id=? AND user_id=?",
                (fid, uid),
            ).fetchone()

    def del_filter(self, uid, fid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM filters WHERE id=? AND user_id=?",
                (fid, uid),
            )

    def clear_filters(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM filters WHERE user_id=?", (uid,))

    def filter_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM filters WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def filter_inc(self, fid):
        with self.conn() as cx:
            cx.execute(
                "UPDATE filters SET used_count=used_count+1 WHERE id=?",
                (fid,),
            )

    # ═══════════════════ BLOCKED WORDS ═══════════════════

    def add_blocked(self, uid, word, action="warn"):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO blocked_words(user_id, word, action)
                   VALUES(?,?,?)""",
                (uid, word.lower().strip(), action),
            )

    def get_blocked(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM blocked_words
                   WHERE user_id=? ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_blocked(self, uid, bid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM blocked_words WHERE id=? AND user_id=?",
                (bid, uid),
            )

    def clear_blocked(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM blocked_words WHERE user_id=?", (uid,))

    def blocked_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM blocked_words WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ WHITELIST ═══════════════════

    def add_whitelist(self, uid, target, target_name=""):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO whitelist(user_id, target_user, target_name)
                   VALUES(?,?,?)""",
                (uid, target, target_name),
            )

    def get_whitelist(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM whitelist
                   WHERE user_id=? ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_whitelist(self, uid, wid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM whitelist WHERE id=? AND user_id=?",
                (wid, uid),
            )

    def clear_whitelist(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM whitelist WHERE user_id=?", (uid,))

    def whitelist_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM whitelist WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def is_whitelisted(self, uid, sender_id, sender_username=None):
        wl = self.get_whitelist(uid)
        wl_ids = set()
        for w in wl:
            wl_ids.add(str(w["target_user"]))
        if str(sender_id) in wl_ids:
            return True
        if sender_username:
            clean = sender_username.lower().lstrip("@")
            for w in wl:
                t = str(w["target_user"]).lower().lstrip("@")
                if t == clean:
                    return True
        return False

    # ═══════════════════ SCHEDULED ═══════════════════

    def add_scheduled(self, uid, target, message, send_at,
                      media_file_id=None, media_type=None,
                      recurring=False, interval_hr=0, max_repeats=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO scheduled
                   (user_id, target, message, send_at, media_file_id,
                    media_type, recurring, interval_hr, max_repeats)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (uid, target, message, send_at, media_file_id,
                 media_type, int(recurring), interval_hr, max_repeats),
            )

    def pending_scheduled(self):
        now = datetime.now().isoformat()
        with self.conn() as cx:
            return cx.execute(
                """SELECT s.*, u.session_str FROM scheduled s
                   JOIN users u ON s.user_id=u.user_id
                   WHERE s.is_sent=0
                   AND s.send_at<=?
                   AND u.session_str IS NOT NULL
                   AND u.session_str != ''
                   AND u.is_banned=0""",
                (now,),
            ).fetchall()

    def mark_sent(self, sid, recurring=False, interval_hr=0,
                  max_repeats=0, repeat_count=0):
        with self.conn() as cx:
            if recurring and interval_hr > 0:
                new_count = repeat_count + 1
                if max_repeats > 0 and new_count >= max_repeats:
                    cx.execute(
                        """UPDATE scheduled
                           SET is_sent=1, sent_at=?, repeat_count=?
                           WHERE id=?""",
                        (datetime.now().isoformat(), new_count, sid),
                    )
                else:
                    new_at = (
                        datetime.now() + timedelta(hours=interval_hr)
                    ).isoformat()
                    cx.execute(
                        """UPDATE scheduled
                           SET send_at=?, repeat_count=?
                           WHERE id=?""",
                        (new_at, new_count, sid),
                    )
            else:
                cx.execute(
                    """UPDATE scheduled
                       SET is_sent=1, sent_at=?
                       WHERE id=?""",
                    (datetime.now().isoformat(), sid),
                )

    def user_scheduled(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM scheduled
                   WHERE user_id=? AND is_sent=0
                   ORDER BY send_at ASC""",
                (uid,),
            ).fetchall()

    def del_scheduled(self, uid, sid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM scheduled WHERE id=? AND user_id=?",
                (sid, uid),
            )

    def scheduled_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT COUNT(*) FROM scheduled
                   WHERE user_id=? AND is_sent=0""",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ AUTO-FORWARD ═══════════════════

    def add_forward(self, uid, source, dest, filter_text="",
                    forward_media=True):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO auto_forward
                   (user_id, source, dest, filter_text, forward_media)
                   VALUES(?,?,?,?,?)""",
                (uid, source, dest, filter_text, int(forward_media)),
            )

    def get_forwards(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM auto_forward
                   WHERE user_id=? AND active=1
                   ORDER BY id DESC""",
                (uid,),
            ).fetchall()

    def del_forward(self, uid, fid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM auto_forward WHERE id=? AND user_id=?",
                (fid, uid),
            )

    def clear_forwards(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM auto_forward WHERE user_id=?", (uid,)
            )

    def forward_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT COUNT(*) FROM auto_forward
                   WHERE user_id=? AND active=1""",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ PM PERMIT ═══════════════════

    def approve_pm(self, uid, sender, sender_name="", auto=False):
        with self.conn() as cx:
            cx.execute(
                """INSERT OR IGNORE INTO pm_permit
                   (user_id, approved, approved_name, auto_approved)
                   VALUES(?,?,?,?)""",
                (uid, sender, sender_name, int(auto)),
            )

    def is_pm_approved(self, uid, sender):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT 1 FROM pm_permit WHERE user_id=? AND approved=?",
                (uid, sender),
            ).fetchone()
        return r is not None

    def get_approved(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM pm_permit
                   WHERE user_id=?
                   ORDER BY approved_at DESC""",
                (uid,),
            ).fetchall()

    def revoke_pm(self, uid, sender):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM pm_permit WHERE user_id=? AND approved=?",
                (uid, sender),
            )

    def approved_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM pm_permit WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    # ═══════════════════ STATS ═══════════════════

    def inc_stat(self, uid, key, n=1):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO stats(user_id, key, value)
                   VALUES(?,?,?)
                   ON CONFLICT(user_id, key)
                   DO UPDATE SET value=value+?""",
                (uid, key, n, n),
            )
        today = datetime.now().strftime("%Y-%m-%d")
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO daily_stats(user_id, date, key, value)
                   VALUES(?,?,?,?)
                   ON CONFLICT(user_id, date, key)
                   DO UPDATE SET value=value+?""",
                (uid, today, key, n, n),
            )

    def get_stat(self, uid, key):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT value FROM stats WHERE user_id=? AND key=?",
                (uid, key),
            ).fetchone()
        return r["value"] if r else 0

    def all_stats(self, uid):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT key, value FROM stats
                   WHERE user_id=? ORDER BY key ASC""",
                (uid,),
            ).fetchall()
        return {r["key"]: r["value"] for r in rows}

    def daily_stats(self, uid, days=7):
        cutoff = (
            datetime.now() - timedelta(days=days)
        ).strftime("%Y-%m-%d")
        with self.conn() as cx:
            return cx.execute(
                """SELECT date, key, value FROM daily_stats
                   WHERE user_id=? AND date>=?
                   ORDER BY date DESC, key ASC""",
                (uid, cutoff),
            ).fetchall()

    def reset_stats(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM stats WHERE user_id=?", (uid,))
            cx.execute(
                "DELETE FROM daily_stats WHERE user_id=?", (uid,)
            )

    def global_stats(self):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT key, SUM(value) as total
                   FROM stats GROUP BY key ORDER BY total DESC"""
            ).fetchall()
        return {r["key"]: r["total"] for r in rows}

    # ═══════════════════ LOGS ═══════════════════

    def log(self, uid, action, detail="", category="general"):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO logs(user_id, action, detail, category)
                   VALUES(?,?,?,?)""",
                (uid, action, detail[:500], category),
            )

    def get_logs(self, uid, limit=30, category=None):
        q = "SELECT * FROM logs WHERE user_id=?"
        params = [uid]
        if category:
            q += " AND category=?"
            params.append(category)
        q += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def all_logs(self, limit=50, category=None):
        q = "SELECT * FROM logs"
        params = []
        if category:
            q += " WHERE category=?"
            params.append(category)
        q += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def clear_logs(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM logs WHERE user_id=?", (uid,))

    def clear_all_logs(self):
        with self.conn() as cx:
            cx.execute("DELETE FROM logs")

    # ═══════════════════ SPAM TRACKING ═══════════════════

    def check_spam(self, uid, sender, limit=5):
        now = datetime.now()
        min_ago = (now - timedelta(minutes=1)).isoformat()
        with self.conn() as cx:
            r = cx.execute(
                """SELECT * FROM spam_track
                   WHERE user_id=? AND sender=? AND last_ts>?""",
                (uid, sender, min_ago),
            ).fetchone()
            if r:
                new_cnt = r["count"] + 1
                if new_cnt >= limit:
                    cx.execute(
                        """UPDATE spam_track
                           SET count=?, blocked=1, last_ts=?
                           WHERE id=?""",
                        (new_cnt, now.isoformat(), r["id"]),
                    )
                    return True
                cx.execute(
                    "UPDATE spam_track SET count=?, last_ts=? WHERE id=?",
                    (new_cnt, now.isoformat(), r["id"]),
                )
            else:
                cx.execute(
                    """INSERT INTO spam_track
                       (user_id, sender, count, last_ts)
                       VALUES(?,?,1,?)""",
                    (uid, sender, now.isoformat()),
                )
        return False

    def is_spam_blocked(self, uid, sender):
        with self.conn() as cx:
            r = cx.execute(
                """SELECT blocked FROM spam_track
                   WHERE user_id=? AND sender=?""",
                (uid, sender),
            ).fetchone()
        return bool(r and r["blocked"])

    def unblock_spam(self, uid, sender):
        with self.conn() as cx:
            cx.execute(
                """UPDATE spam_track
                   SET blocked=0, count=0
                   WHERE user_id=? AND sender=?""",
                (uid, sender),
            )

    # ═══════════════════ TEMPLATES ═══════════════════

    def add_template(self, uid, name, content, category="general",
                     media_file_id=None, media_type=None,
                     is_global=False):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO templates
                   (user_id, name, content, category,
                    media_file_id, media_type, is_global)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, name, content, category,
                 media_file_id, media_type, int(is_global)),
            )

    def get_templates(self, uid, include_global=True):
        with self.conn() as cx:
            if include_global:
                return cx.execute(
                    """SELECT * FROM templates
                       WHERE user_id=? OR is_global=1
                       ORDER BY name ASC""",
                    (uid,),
                ).fetchall()
            return cx.execute(
                """SELECT * FROM templates
                   WHERE user_id=?
                   ORDER BY name ASC""",
                (uid,),
            ).fetchall()

    def get_template(self, uid, tid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM templates
                   WHERE id=? AND (user_id=? OR is_global=1)""",
                (tid, uid),
            ).fetchone()

    def del_template(self, uid, tid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM templates WHERE id=? AND user_id=?",
                (tid, uid),
            )

    def clear_templates(self, uid):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM templates
                   WHERE user_id=? AND is_global=0""",
                (uid,),
            )

    def template_count(self, uid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT COUNT(*) FROM templates WHERE user_id=?",
                (uid,),
            ).fetchone()
            return r[0] if r else 0

    def template_inc(self, tid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE templates
                   SET used_count=used_count+1
                   WHERE id=?""",
                (tid,),
            )

    # ═══════════════════ WORKING HOURS ═══════════════════

    def set_working_hours(self, uid, day, start_hr, start_min,
                          end_hr, end_min, is_active=True):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO working_hours
                   (user_id, day, start_hr, start_min,
                    end_hr, end_min, is_active)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(user_id, day)
                   DO UPDATE SET
                       start_hr=excluded.start_hr,
                       start_min=excluded.start_min,
                       end_hr=excluded.end_hr,
                       end_min=excluded.end_min,
                       is_active=excluded.is_active""",
                (uid, day, start_hr, start_min,
                 end_hr, end_min, int(is_active)),
            )

    def get_working_hours(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM working_hours
                   WHERE user_id=?
                   ORDER BY day ASC""",
                (uid,),
            ).fetchall()

    def is_working_hours(self, uid):
        if self.get_setting(uid, "working_hours", "false") != "true":
            return True

        now = datetime.now()
        day = now.weekday()
        with self.conn() as cx:
            r = cx.execute(
                """SELECT * FROM working_hours
                   WHERE user_id=? AND day=? AND is_active=1""",
                (uid, day),
            ).fetchone()

        if not r:
            return True

        current_mins = now.hour * 60 + now.minute
        start_mins = r["start_hr"] * 60 + r["start_min"]
        end_mins = r["end_hr"] * 60 + r["end_min"]
        return start_mins <= current_mins <= end_mins

    def clear_working_hours(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM working_hours WHERE user_id=?", (uid,)
            )

    # ═══════════════════ CUSTOM COMMANDS ═══════════════════

    def add_custom_cmd(self, uid, command, response,
                       media_file_id=None, media_type=None):
        cmd = command.lower().strip().lstrip("/")
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO custom_commands
                   (user_id, command, response,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, cmd, response, media_file_id, media_type),
            )

    def get_custom_cmds(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM custom_commands
                   WHERE user_id=? AND is_active=1
                   ORDER BY command ASC""",
                (uid,),
            ).fetchall()

    def del_custom_cmd(self, uid, cid):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM custom_commands
                   WHERE id=? AND user_id=?""",
                (cid, uid),
            )

    def clear_custom_cmds(self, uid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM custom_commands WHERE user_id=?", (uid,)
            )

    def custom_cmd_inc(self, cid):
        with self.conn() as cx:
            cx.execute(
                """UPDATE custom_commands
                   SET used_count=used_count+1
                   WHERE id=?""",
                (cid,),
            )

    # ═══════════════════ NOTES ═══════════════════

    def add_note(self, uid, title, content,
                 media_file_id=None, media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO notes
                   (user_id, title, content,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?)""",
                (uid, title, content, media_file_id, media_type),
            )

    def get_notes(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM notes
                   WHERE user_id=?
                   ORDER BY is_pinned DESC, updated_at DESC""",
                (uid,),
            ).fetchall()

    def get_note(self, uid, nid):
        with self.conn() as cx:
            return cx.execute(
                "SELECT * FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            ).fetchone()

    def del_note(self, uid, nid):
        with self.conn() as cx:
            cx.execute(
                "DELETE FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            )

    def toggle_pin_note(self, uid, nid):
        with self.conn() as cx:
            r = cx.execute(
                "SELECT is_pinned FROM notes WHERE id=? AND user_id=?",
                (nid, uid),
            ).fetchone()
            if r:
                cx.execute(
                    """UPDATE notes SET is_pinned=?
                       WHERE id=? AND user_id=?""",
                    (0 if r["is_pinned"] else 1, nid, uid),
                )

    def clear_notes(self, uid):
        with self.conn() as cx:
            cx.execute("DELETE FROM notes WHERE user_id=?", (uid,))

    # ═══════════════════ MEDIA ATTACHMENTS ═══════════════════

    def add_media_attachment(self, uid, parent_type, parent_id,
                             file_id, media_type, caption="",
                             position=0):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO media_attachments
                   (user_id, parent_type, parent_id,
                    file_id, media_type, caption, position)
                   VALUES(?,?,?,?,?,?,?)""",
                (uid, parent_type, parent_id,
                 file_id, media_type, caption, position),
            )

    def get_media_attachments(self, uid, parent_type, parent_id):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM media_attachments
                   WHERE user_id=? AND parent_type=? AND parent_id=?
                   ORDER BY position ASC""",
                (uid, parent_type, parent_id),
            ).fetchall()

    def del_media_attachments(self, uid, parent_type, parent_id):
        with self.conn() as cx:
            cx.execute(
                """DELETE FROM media_attachments
                   WHERE user_id=? AND parent_type=? AND parent_id=?""",
                (uid, parent_type, parent_id),
            )

    # ═══════════════════ FEEDBACK ═══════════════════

    def add_feedback(self, uid, message):
        with self.conn() as cx:
            cx.execute(
                "INSERT INTO feedback(user_id, message) VALUES(?,?)",
                (uid, message),
            )

    def get_all_feedback(self, status=None):
        q = "SELECT f.*, u.username, u.first_name FROM feedback f LEFT JOIN users u ON f.user_id=u.user_id"
        params = []
        if status:
            q += " WHERE f.status=?"
            params.append(status)
        q += " ORDER BY f.created_at DESC"
        with self.conn() as cx:
            return cx.execute(q, params).fetchall()

    def reply_feedback(self, fid, reply):
        with self.conn() as cx:
            cx.execute(
                """UPDATE feedback
                   SET status='replied', admin_reply=?, replied_at=?
                   WHERE id=?""",
                (reply, datetime.now().isoformat(), fid),
            )

    def user_feedback(self, uid):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM feedback
                   WHERE user_id=?
                   ORDER BY created_at DESC""",
                (uid,),
            ).fetchall()

    # ═══════════════════ ANNOUNCEMENTS ═══════════════════

    def add_announcement(self, title, content, created_by,
                         target="all", media_file_id=None,
                         media_type=None):
        with self.conn() as cx:
            cx.execute(
                """INSERT INTO announcements
                   (title, content, created_by, target,
                    media_file_id, media_type)
                   VALUES(?,?,?,?,?,?)""",
                (title, content, created_by, target,
                 media_file_id, media_type),
            )

    def get_announcements(self, limit=10):
        with self.conn() as cx:
            return cx.execute(
                """SELECT * FROM announcements
                   ORDER BY created_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()

    # ═══════════════════ ADMIN HELPERS ═══════════════════

    def total_users(self):
        with self.conn() as cx:
            return cx.execute(
                "SELECT COUNT(*) FROM users"
            ).fetchone()[0]

    def active_sessions_count(self):
        with self.conn() as cx:
            return cx.execute(
                """SELECT COUNT(*) FROM users
                   WHERE session_str IS NOT NULL
                   AND session_str != ''"""
            ).fetchone()[0]

    def users_by_plan_count(self):
        with self.conn() as cx:
            rows = cx.execute(
                """SELECT plan, COUNT(*) as cnt
                   FROM users GROUP BY plan"""
            ).fetchall()
        return {r["plan"]: r["cnt"] for r in rows}

    def db_size(self):
        if not os.path.exists(self.db_file):
            return "0 KB"
        size = os.path.getsize(self.db_file)
        if size < 1024:
            return f"{size} B"
        if size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        return f"{size / (1024 * 1024):.1f} MB"

    def cleanup(self):
        week_ago = (
            datetime.now() - timedelta(days=7)
        ).isoformat()
        day_ago = (
            datetime.now() - timedelta(days=1)
        ).isoformat()
        month_ago = (
            datetime.now() - timedelta(days=30)
        ).isoformat()
        with self.conn() as cx:
            cx.execute("DELETE FROM logs WHERE ts<?", (week_ago,))
            cx.execute(
                "DELETE FROM spam_track WHERE last_ts<?", (day_ago,)
            )
            cx.execute("DELETE FROM scheduled WHERE is_sent=1")
            cx.execute(
                "DELETE FROM daily_stats WHERE date<?",
                (month_ago[:10],),
            )
            cx.execute("VACUUM")

    # ═══════════════════ EXPORT / IMPORT ═══════════════════

    def export_user_data(self, uid) -> dict:
        data = {
            "version": BOT_VERSION,
            "exported_at": datetime.now().isoformat(),
            "user_id": uid,
            "settings": self.all_settings(uid),
            "keywords": [],
            "filters": [],
            "blocked_words": [],
            "whitelist": [],
            "templates": [],
            "custom_commands": [],
            "notes": [],
            "working_hours": [],
        }

        for kw in self.get_keywords(uid, active_only=False):
            data["keywords"].append({
                "trigger": kw["trigger_text"],
                "response": kw["response"],
                "match_type": kw["match_type"],
                "media_file_id": kw["media_file_id"],
                "media_type": kw["media_type"],
                "is_active": kw["is_active"],
                "reply_delay": kw["reply_delay"],
            })

        for f in self.get_filters(uid):
            data["filters"].append({
                "name": f["name"],
                "response": f["response"],
                "media_file_id": f["media_file_id"],
                "media_type": f["media_type"],
            })

        for bw in self.get_blocked(uid):
            data["blocked_words"].append({
                "word": bw["word"],
                "action": bw["action"],
            })

        for w in self.get_whitelist(uid):
            data["whitelist"].append({
                "target": w["target_user"],
                "name": w["target_name"],
            })

        for t in self.get_templates(uid, include_global=False):
            data["templates"].append({
                "name": t["name"],
                "content": t["content"],
                "category": t["category"],
                "media_file_id": t["media_file_id"],
                "media_type": t["media_type"],
            })

        for cmd in self.get_custom_cmds(uid):
            data["custom_commands"].append({
                "command": cmd["command"],
                "response": cmd["response"],
                "media_file_id": cmd["media_file_id"],
                "media_type": cmd["media_type"],
            })

        for n in self.get_notes(uid):
            data["notes"].append({
                "title": n["title"],
                "content": n["content"],
                "media_file_id": n["media_file_id"],
                "media_type": n["media_type"],
                "is_pinned": n["is_pinned"],
            })

        for wh in self.get_working_hours(uid):
            data["working_hours"].append({
                "day": wh["day"],
                "start_hr": wh["start_hr"],
                "start_min": wh["start_min"],
                "end_hr": wh["end_hr"],
                "end_min": wh["end_min"],
                "is_active": wh["is_active"],
            })

        return data

    def import_user_data(self, uid, data: dict):
        if "settings" in data:
            self.bulk_set_settings(uid, data["settings"])

        if "keywords" in data:
            for kw in data["keywords"]:
                self.add_keyword(
                    uid,
                    kw["trigger"],
                    kw["response"],
                    kw.get("match_type", "contains"),
                    kw.get("media_file_id"),
                    kw.get("media_type"),
                    kw.get("reply_delay", 0),
                )

        if "filters" in data:
            for f in data["filters"]:
                self.add_filter(
                    uid,
                    f["name"],
                    f["response"],
                    f.get("media_file_id"),
                    f.get("media_type"),
                )

        if "blocked_words" in data:
            for bw in data["blocked_words"]:
                self.add_blocked(
                    uid,
                    bw["word"],
                    bw.get("action", "warn"),
                )

        if "whitelist" in data:
            for w in data["whitelist"]:
                self.add_whitelist(
                    uid,
                    w["target"],
                    w.get("name", ""),
                )

        if "templates" in data:
            for t in data["templates"]:
                self.add_template(
                    uid,
                    t["name"],
                    t["content"],
                    t.get("category", "general"),
                    t.get("media_file_id"),
                    t.get("media_type"),
                )

        if "custom_commands" in data:
            for cmd in data["custom_commands"]:
                self.add_custom_cmd(
                    uid,
                    cmd["command"],
                    cmd["response"],
                    cmd.get("media_file_id"),
                    cmd.get("media_type"),
                )

        if "notes" in data:
            for n in data["notes"]:
                self.add_note(
                    uid,
                    n["title"],
                    n["content"],
                    n.get("media_file_id"),
                    n.get("media_type"),
                )

        if "working_hours" in data:
            for wh in data["working_hours"]:
                self.set_working_hours(
                    uid,
                    wh["day"],
                    wh["start_hr"],
                    wh["start_min"],
                    wh["end_hr"],
                    wh["end_min"],
                    wh.get("is_active", True),
                )

        self.log(uid, "data_import", "Settings imported", "system")

# Initialize database
db = Database()
