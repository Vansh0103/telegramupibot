import os
import time
import sqlite3
import hashlib
import re
from flask import Flask, request, jsonify, render_template
from core import get_setting, db_execute, play_mine_game
import json
import random
from datetime import datetime, timedelta

app = Flask(__name__, template_folder="templates")

DB_PATH = os.environ.get("DB_PATH", "/data/bot_database.db")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "realupilootbot")
SECRET_SALT = os.environ.get("SECRET_SALT", "change_me_in_production")

MAX_ATTEMPTS = 5
RATE_WINDOW = 3600


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            balance REAL DEFAULT 0,
            total_earned REAL DEFAULT 0,
            total_withdrawn REAL DEFAULT 0,
            referral_count INTEGER DEFAULT 0,
            referred_by INTEGER DEFAULT 0,
            upi_id TEXT DEFAULT '',
            banned INTEGER DEFAULT 0,
            joined_at TEXT DEFAULT '',
            last_daily TEXT DEFAULT '',
            is_premium INTEGER DEFAULT 0,
            referral_paid INTEGER DEFAULT 0,
            ip_address TEXT DEFAULT '',
            ip_verified INTEGER DEFAULT 0,
            verify_attempts INTEGER DEFAULT 0,
            last_attempt_at REAL DEFAULT 0,
            verified_at REAL DEFAULT 0,
            session_hash TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            device_type TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS verify_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            ip TEXT,
            result TEXT,
            reason TEXT,
            user_agent TEXT,
            ts REAL,
            session_hash TEXT DEFAULT ''
        )
    """)

    extra_columns = [
        ("referral_paid", "INTEGER DEFAULT 0"),
        ("ip_address", "TEXT DEFAULT ''"),
        ("ip_verified", "INTEGER DEFAULT 0"),
        ("verify_attempts", "INTEGER DEFAULT 0"),
        ("last_attempt_at", "REAL DEFAULT 0"),
        ("verified_at", "REAL DEFAULT 0"),
        ("session_hash", "TEXT DEFAULT ''"),
        ("user_agent", "TEXT DEFAULT ''"),
        ("device_type", "TEXT DEFAULT ''"),
    ]

    for col_name, col_type in extra_columns:
        try:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()


def get_real_ip():
    headers_to_check = ["CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For"]
    for header in headers_to_check:
        value = request.headers.get(header, "")
        if value:
            return value.split(",")[0].strip()
    return request.remote_addr or ""


def detect_device(user_agent: str) -> str:
    ua = user_agent or ""
    if re.search(r"iPad|Tablet", ua, re.IGNORECASE):
        return "Tablet"
    if re.search(r"Mobi|Android|iPhone|iPod", ua, re.IGNORECASE):
        return "Mobile"
    return "Desktop"


def make_session_hash(user_id: int, ip: str, user_agent: str) -> str:
    raw = f"{user_id}|{ip}|{user_agent}|{SECRET_SALT}|{time.time()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def ip_taken_by_other_account(ip: str, user_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM users WHERE ip_address = ? AND user_id != ? LIMIT 1",
        (ip, user_id)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def format_ts(ts_value):
    try:
        ts_value = float(ts_value or 0)
        if ts_value <= 0:
            return "—"
        return time.strftime("%d %b %Y • %I:%M %p", time.localtime(ts_value))
    except Exception:
        return "—"


def log_verification(cur, user_id, ip, result, reason, user_agent, session_hash=""):
    cur.execute("""
        INSERT INTO verify_log (user_id, ip, result, reason, user_agent, ts, session_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, ip, result, reason, user_agent, time.time(), session_hash))


def verify_user(user_id: int, ip: str, user_agent: str):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cur.fetchone()

    if not user:
        log_verification(cur, user_id, ip, "fail", "user_not_found", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": "User not found. Please start the bot first.",
            "code": "ERR_USER_404"
        }

    if int(user["banned"] or 0) == 1:
        log_verification(cur, user_id, ip, "fail", "account_banned", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": "Your account is banned.",
            "code": "ERR_ACCT_BAN"
        }

    now = time.time()
    attempts = int(user["verify_attempts"] or 0)
    last_attempt_at = float(user["last_attempt_at"] or 0)

    if now - last_attempt_at >= RATE_WINDOW:
        attempts = 0

    if attempts >= MAX_ATTEMPTS:
        remaining = int(max(60, RATE_WINDOW - (now - last_attempt_at)))
        mins = max(1, remaining // 60)
        log_verification(cur, user_id, ip, "fail", "rate_limited", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": f"Too many attempts. Try again in {mins} minute(s).",
            "code": "ERR_RATE_LIMIT"
        }

    if not ip:
        log_verification(cur, user_id, ip, "fail", "ip_missing", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": "Could not detect your IP address.",
            "code": "ERR_IP_DETECT"
        }

    if int(user["ip_verified"] or 0) == 1:
        conn.close()
        return True, {
            "message": "Already verified.",
            "status": "already_verified",
            "user_id": user_id,
            "session_hash": user["session_hash"] or "",
            "verified_at": format_ts(user["verified_at"]),
            "device_type": user["device_type"] or detect_device(user_agent),
            "bot_username": BOT_USERNAME
        }

    if ip_taken_by_other_account(ip, user_id):
        cur.execute("""
            UPDATE users
            SET verify_attempts = ?, last_attempt_at = ?
            WHERE user_id = ?
        """, (attempts + 1, now, user_id))
        log_verification(cur, user_id, ip, "fail", "ip_conflict", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": "This IP is already linked to another account.",
            "code": "ERR_IP_CONFLICT"
        }

    device_type = detect_device(user_agent)
    session_hash = make_session_hash(user_id, ip, user_agent)

    cur.execute("""
        UPDATE users
        SET
            ip_address = ?,
            ip_verified = 1,
            verify_attempts = ?,
            last_attempt_at = ?,
            verified_at = ?,
            session_hash = ?,
            user_agent = ?,
            device_type = ?
        WHERE user_id = ?
    """, (
        ip,
        attempts + 1,
        now,
        now,
        session_hash,
        user_agent,
        device_type,
        user_id
    ))

    log_verification(cur, user_id, ip, "success", "verified", user_agent, session_hash)
    conn.commit()
    conn.close()

    return True, {
        "message": "Verification successful.",
        "status": "verified",
        "user_id": user_id,
        "session_hash": session_hash,
        "verified_at": format_ts(now),
        "device_type": device_type,
        "bot_username": BOT_USERNAME
    }



def get_user_row(user_id: int):
    return db_execute("SELECT * FROM users WHERE user_id=?", (int(user_id),), fetchone=True)

def get_game_history(user_id: int, limit: int = 20):
    return db_execute(
        "SELECT game_key as game_type, bet_amount as amount, reward_amount as reward, result, created_at FROM game_sessions WHERE user_id=? ORDER BY id DESC LIMIT ?",
        (int(user_id), int(limit)),
        fetch=True
    ) or []

def _games_payload(user_id: int):
    user = get_user_row(user_id)
    if not user:
        return None
    rows = [dict(r) for r in get_game_history(user_id, 20)]
    return {
        "status": "ok",
        "user_id": user_id,
        "balance": float(user["balance"] or 0),
        "games_enabled": bool(get_setting("games_enabled", True)),
        "mine_game_enabled": bool(get_setting("mine_game_enabled", True)),
        "mine_game_name": get_setting("mine_game_name", "Mine Game"),
        "min_bet": float(get_setting("mine_game_min_bet", 1) or 1),
        "max_bet": float(get_setting("mine_game_max_bet", 50) or 50),
        "win_ratio": float(get_setting("mine_game_win_ratio", 35) or 35),
        "reward_multiplier": float(get_setting("mine_game_reward_multiplier", 2.0) or 2.0),
        "daily_limit": int(get_setting("mine_game_daily_limit", 50) or 50),
        "cooldown_seconds": int(get_setting("mine_game_cooldown_seconds", 30) or 30),
        "history": rows,
        "note": "Future games can be added here."
    }

@app.route("/games")
def games_home():
    uid = request.args.get("uid", "").strip()
    if not uid.isdigit():
        return "Invalid or missing uid", 400
    user_id = int(uid)
    payload = _games_payload(user_id)
    if not payload:
        return "User not found", 404

    wants_json = request.args.get("format") == "json" or "application/json" in (request.headers.get("Accept", ""))
    if wants_json:
        return jsonify(payload)

    try:
        return render_template(
            "mine_game.html",
            user_id=user_id,
            mine_game_name=payload["mine_game_name"],
            balance=payload["balance"],
            min_bet=payload["min_bet"],
            max_bet=payload["max_bet"],
            reward_multiplier=payload["reward_multiplier"],
            cooldown_seconds=payload["cooldown_seconds"],
        )
    except Exception as e:
        return jsonify({"error": f"Game page render failed: {str(e)}"}), 500

@app.route("/games/play", methods=["POST"])
def games_play():
    payload = request.get_json(silent=True) or request.form or {}
    uid = str(payload.get("uid", "")).strip()
    bet = payload.get("bet", 0)
    if not uid.isdigit():
        return jsonify({"error": "Invalid user id"}), 400
    try:
        bet_amount = float(bet)
    except Exception:
        return jsonify({"error": "Invalid bet amount"}), 400
    ok, data, code = play_mine_game(int(uid), bet_amount)
    return jsonify(data), code

@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "service": "web_verify",
        "version": "5.0"
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "timestamp": int(time.time())
    })


@app.route("/ip-verify")
def ip_verify():
    uid = request.args.get("uid", "").strip()

    if not uid or not uid.isdigit():
        return render_template(
            "verify.html",
            page_state="error",
            title="Verification Failed",
            message="Invalid or missing user ID. Use the correct link from the bot.",
            error_code="ERR_INVALID_UID",
            user_id="—",
            session_hash="—",
            verified_at="—",
            device_type="—",
            bot_username=BOT_USERNAME,
        ), 400

    user_id = int(uid)
    ip = get_real_ip()
    user_agent = request.headers.get("User-Agent", "")

    ok, data = verify_user(user_id, ip, user_agent)

    if not ok:
        return render_template(
            "verify.html",
            page_state="error",
            title="Verification Failed",
            message=data["message"],
            error_code=data["code"],
            user_id=user_id,
            session_hash="—",
            verified_at="—",
            device_type=detect_device(user_agent),
            bot_username=BOT_USERNAME,
        ), 400

    return render_template(
        "verify.html",
        page_state="success",
        title="Verified Successfully",
        message=data["message"],
        error_code="—",
        user_id=data["user_id"],
        session_hash=data["session_hash"],
        verified_at=data["verified_at"],
        device_type=data["device_type"],
        bot_username=data["bot_username"],
    )


@app.route("/api/verify-status/<int:user_id>")
def verify_status(user_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT ip_verified, ip_address, verified_at, device_type, session_hash
        FROM users
        WHERE user_id = ?
    """, (user_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({
            "verified": False,
            "error": "user_not_found"
        }), 404

    return jsonify({
        "verified": bool(int(row["ip_verified"] or 0)),
        "ip_address": row["ip_address"] or "",
        "verified_at": row["verified_at"] or 0,
        "device_type": row["device_type"] or "",
        "session_hash": row["session_hash"] or ""
    })


@app.route("/api/verify-log/<int:user_id>")
def verify_log(user_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT result, reason, ts, ip
        FROM verify_log
        WHERE user_id = ?
        ORDER BY ts DESC
        LIMIT 20
    """, (user_id,))
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    return jsonify({
        "user_id": user_id,
        "logs": rows
    })


@app.route("/api/stats")
def stats():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS total FROM users")
    total_users = cur.fetchone()["total"]

    cur.execute("SELECT COUNT(*) AS total FROM users WHERE ip_verified = 1")
    total_verified = cur.fetchone()["total"]

    cur.execute("SELECT COUNT(*) AS total FROM verify_log WHERE result = 'fail'")
    total_failed = cur.fetchone()["total"]

    conn.close()

    return jsonify({
        "total_users": total_users,
        "total_verified": total_verified,
        "total_failed_attempts": total_failed
    })


ensure_schema()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
