import telebot
from telebot import types
import sqlite3
import threading
import time
import random
import string
import json
from datetime import datetime
import os
import csv
import io
from telebot.types import WebAppInfo
from anticheat import AntiCheatSystem
from broadcast import BroadcastSystem
from getoldb import DatabaseImportSystem
from withdrawlimit import WithdrawLimitSystem
from adminhelp import AdminHelpSystem
# ======================== CONFIGURATION ========================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = 7353041224
HELP_USERNAME = "@itsukiarai"
MESSAGE_EFFECT_ID = "5104841245755180586"
FORCE_JOIN_CHANNELS = ["@skullmodder","@botsarefather","@upilootpay"]
REQUEST_CHANNEL = "https://t.me/+kOrz7X6VUygyYjk1"
NOTIFICATION_CHANNEL = "@upilootpay"

WELCOME_IMAGE = "https://image2url.com/r2/default/images/1775843670811-7e698bcc-a37c-46f9-a0bd-6a5cabe5f6ec.png"
WITHDRAWAL_IMAGE = "https://image2url.com/r2/default/images/1775843858548-29ae7a16-81b2-4c75-aded-cfb3093df954.png"

DEFAULT_SETTINGS = {
    "per_refer": 2,
    "min_withdraw": 5,
    "welcome_bonus": 0.5,
    "daily_bonus": 0.5,
    "max_withdraw_per_day": 100,
    "withdraw_enabled": True,
    "refer_enabled": True,
    "gift_enabled": True,
    "bot_maintenance": False,
    "welcome_image": WELCOME_IMAGE,
    "withdraw_image": WITHDRAWAL_IMAGE,
    "withdraw_time_start": 0,
    "withdraw_time_end": 23,
    "max_gift_create": 100,
    "min_gift_amount": 3,
    "tasks_enabled": True,
    "redeem_withdraw_enabled": True,
    "redeem_min_withdraw": 10,
    "redeem_multiple_of": 5,
    "redeem_gst_cut": 3,
}

PE = {
    "eyes": "5210956306952758910","smile": "5461117441612462242","zap": "5456140674028019486",
    "comet": "5224607267797606837","bag": "5229064374403998351","no_entry": "5260293700088511294",
    "prohibited": "5240241223632954241","excl": "5274099962655816924","double_excl": "5440660757194744323",
    "question_excl": "5314504236132747481","question": "5436113877181941026","warning": "5447644880824181073",
    "warning2": "5420323339723881652","globe": "5447410659077661506","speech": "5443038326535759644",
    "thought": "5467538555158943525","question2": "5452069934089641166","chart": "5231200819986047254",
    "up": "5449683594425410231","down": "5447183459602669338","candle": "5451882707875276247",
    "chart_up": "5244837092042750681","chart_down": "5246762912428603768","check": "5206607081334906820",
    "cross": "5210952531676504517","cool": "5222079954421818267","bell": "5458603043203327669",
    "disguise": "5391112412445288650","clown": "5269531045165816230","lips": "5395444514028529554",
    "pin": "5397782960512444700","money": "5409048419211682843","fly_money": "5233326571099534068",
    "fly_money2": "5231449120635370684","fly_money3": "5278751923338490157","fly_money4": "5290017777174722330",
    "fly_money5": "5231005931550030290","exchange": "5402186569006210455","play": "5264919878082509254",
    "red": "5411225014148014586","green": "5416081784641168838","arrow": "5416117059207572332",
    "fire": "5424972470023104089","boom": "5276032951342088188","mic": "5294339927318739359",
    "mic2": "5224736245665511429","megaphone": "5424818078833715060","shush": "5431609822288033666",
    "thumbs_down": "5449875686837726134","speaking": "5460795800101594035","search": "5231012545799666522",
    "shield": "5251203410396458957","link": "5271604874419647061","pc": "5282843764451195532",
    "copyright": "5323442290708985472","info": "5334544901428229844","thumbs_up": "5337080053119336309",
    "play2": "5348125953090403204","pause": "5359543311897998264","hundred": "5341498088408234504",
    "refresh": "5375338737028841420","top": "5415655814079723871","new_tag": "5382357040008021292",
    "soon": "5440621591387980068","location": "5391032818111363540","plus": "5397916757333654639",
    "diamond": "5427168083074628963","star": "5438496463044752972","sparkle": "5325547803936572038",
    "crown": "5217822164362739968","trash": "5445267414562389170","bookmark": "5222444124698853913",
    "envelope": "5253742260054409879","lock": "5296369303661067030","surprised": "5303479226882603449",
    "paperclip": "5305265301917549162","gear": "5341715473882955310","game": "5361741454685256344",
    "speaker": "5388632425314140043","hourglass": "5386367538735104399","down_arrow": "5406745015365943482",
    "sun": "5402477260982731644","rain": "5399913388845322366","moon": "5449569374065152798",
    "snow": "5449449325434266744","rainbow": "5409109841538994759","drop": "5393512611968995988",
    "calendar": "5413879192267805083","bulb": "5422439311196834318","gold": "5440539497383087970",
    "silver": "5447203607294265305","bronze": "5453902265922376865","music": "5463107823946717464",
    "free": "5406756500108501710","pencil": "5395444784611480792","siren": "5395695537687123235",
    "shopping": "5406683434124859552","home": "5416041192905265756","flag": "5460755126761312667",
    "party": "5461151367559141950",
    "target": "5411225014148014586","rocket": "5424972470023104089","trophy": "5440539497383087970",
    "medal": "5447203607294265305","task": "5334544901428229844","done": "5206607081334906820",
    "pending2": "5386367538735104399","reject": "5210952531676504517","new": "5382357040008021292",
    "coins": "5409048419211682843","wallet": "5233326571099534068","verify": "5251203410396458957",
    "submit": "5397916757333654639","active": "5416081784641168838","inactive": "5411225014148014586",
    "tag": "5382357040008021292","key": "5296369303661067030","people": "5337080053119336309",
    "admin": "5217822164362739968","database": "5282843764451195532","add": "5397916757333654639",
    "edit": "5395444784611480792","delete": "5445267414562389170","export": "5406756500108501710",
    "import": "5406756500108501710","stats": "5231200819986047254","list": "5334544901428229844",
}

def pe(name):
    eid = PE.get(name, "")
    if eid:
        return f'<tg-emoji emoji-id="{eid}">⭐</tg-emoji>'
    return "⭐"

# ======================== BOT INIT ========================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
# ======================== DATABASE ========================
DB_PATH = os.environ.get("DB_PATH", "/data/bot_database.db")
DB_LOCK = threading.Lock()
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
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
            ip_verified INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            upi_id TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT '',
            processed_at TEXT DEFAULT '',
            admin_note TEXT DEFAULT '',
            txn_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS gift_codes (
            code TEXT PRIMARY KEY,
            amount REAL,
            created_by INTEGER,
            claimed_by INTEGER DEFAULT 0,
            created_at TEXT DEFAULT '',
            claimed_at TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            gift_type TEXT DEFAULT 'user',
            max_claims INTEGER DEFAULT 1,
            total_claims INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS gift_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            user_id INTEGER,
            claimed_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS bonus_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            bonus_type TEXT,
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT DEFAULT '',
            description TEXT DEFAULT '',
            reward REAL DEFAULT 0,
            task_type TEXT DEFAULT 'channel',
            task_url TEXT DEFAULT '',
            task_channel TEXT DEFAULT '',
            required_action TEXT DEFAULT 'join',
            status TEXT DEFAULT 'active',
            created_by INTEGER DEFAULT 0,
            created_at TEXT DEFAULT '',
            updated_at TEXT DEFAULT '',
            max_completions INTEGER DEFAULT 0,
            total_completions INTEGER DEFAULT 0,
            image_url TEXT DEFAULT '',
            order_num INTEGER DEFAULT 0,
            is_repeatable INTEGER DEFAULT 0,
            category TEXT DEFAULT 'general'
        );
        CREATE TABLE IF NOT EXISTS task_submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            user_id INTEGER,
            status TEXT DEFAULT 'pending',
            submitted_at TEXT DEFAULT '',
            reviewed_at TEXT DEFAULT '',
            proof_text TEXT DEFAULT '',
            proof_file_id TEXT DEFAULT '',
            admin_note TEXT DEFAULT '',
            reward_paid REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS task_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            user_id INTEGER,
            completed_at TEXT DEFAULT '',
            reward_paid REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            added_by INTEGER DEFAULT 0,
            added_at TEXT DEFAULT '',
            permissions TEXT DEFAULT 'all',
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action TEXT DEFAULT '',
            details TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS redeem_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT DEFAULT '',
            code TEXT UNIQUE,
            amount REAL DEFAULT 0,
            gst_cut REAL DEFAULT 5,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER DEFAULT 0,
            created_at TEXT DEFAULT '',
            assigned_to INTEGER DEFAULT 0,
            assigned_at TEXT DEFAULT '',
            note TEXT DEFAULT ''
        );
    """)

    try:
        c.execute("ALTER TABLE users ADD COLUMN referral_paid INTEGER DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN ip_address TEXT DEFAULT ''")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN ip_verified INTEGER DEFAULT 0")
    except:
        pass
    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN method TEXT DEFAULT 'upi'")
    except:
        pass

    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN redeem_code_id INTEGER DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN redeem_product TEXT DEFAULT ''")
    except:
        pass

    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN gst_amount REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN net_amount REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE withdrawals ADD COLUMN payout_code TEXT DEFAULT ''")
    except:
        pass

    for key, value in DEFAULT_SETTINGS.items():
        c.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
            (key, json.dumps(value))
        )

    # Force redeem-code withdrawal rules from code so old DB values do not keep overriding them
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("redeem_min_withdraw", json.dumps(DEFAULT_SETTINGS["redeem_min_withdraw"])))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("redeem_multiple_of", json.dumps(DEFAULT_SETTINGS["redeem_multiple_of"])))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("redeem_gst_cut", json.dumps(DEFAULT_SETTINGS["redeem_gst_cut"])))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute(
        "INSERT OR IGNORE INTO admins (user_id, username, first_name, added_by, added_at, permissions, is_active) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ADMIN_ID, "main_admin", "Main Admin", 0, now, "all", 1)
    )

    conn.commit()
    conn.close()
init_db()

def db_execute(query, params=(), fetch=False, fetchone=False):
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute(query, params)
            result = None
            if fetchone:
                result = c.fetchone()
            elif fetch:
                result = c.fetchall()
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            print(f"DB Error: {e} | Query: {query}")
            return None
        finally:
            conn.close()

def db_lastrowid(query, params=()):
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute(query, params)
            last_id = c.lastrowid
            conn.commit()
            return last_id
        except Exception as e:
            conn.rollback()
            print(f"DB Error: {e}")
            return None
        finally:
            conn.close()

def get_setting(key):
    row = db_execute("SELECT value FROM settings WHERE key=?", (key,), fetchone=True)
    if row:
        try:
            return json.loads(row["value"])
        except:
            return row["value"]
    return DEFAULT_SETTINGS.get(key)

def set_setting(key, value):
    db_execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, json.dumps(value))
    )

def get_user(user_id):
    return db_execute("SELECT * FROM users WHERE user_id=?", (user_id,), fetchone=True)

def get_all_users():
    return db_execute("SELECT * FROM users", fetch=True) or []

def get_user_count():
    row = db_execute("SELECT COUNT(*) as cnt FROM users", fetchone=True)
    return row["cnt"] if row else 0

def get_total_withdrawn():
    row = db_execute(
        "SELECT SUM(amount) as total FROM withdrawals WHERE status='approved'",
        fetchone=True
    )
    return (row["total"] or 0) if row else 0

def get_total_pending():
    row = db_execute(
        "SELECT COUNT(*) as cnt FROM withdrawals WHERE status='pending'",
        fetchone=True
    )
    return row["cnt"] if row else 0

def get_total_referrals():
    row = db_execute("SELECT SUM(referral_count) as total FROM users", fetchone=True)
    return (row["total"] or 0) if row else 0

def get_redeem_min_withdraw():
    try:
        value = float(get_setting("redeem_min_withdraw") or DEFAULT_SETTINGS["redeem_min_withdraw"])
    except Exception:
        value = float(DEFAULT_SETTINGS["redeem_min_withdraw"])
    return max(1, value)

def get_redeem_multiple_of():
    try:
        value = int(get_setting("redeem_multiple_of") or DEFAULT_SETTINGS["redeem_multiple_of"])
    except Exception:
        value = int(DEFAULT_SETTINGS["redeem_multiple_of"])
    return max(1, value)

def get_redeem_gst_cut():
    try:
        value = float(get_setting("redeem_gst_cut") or DEFAULT_SETTINGS["redeem_gst_cut"])
    except Exception:
        value = float(DEFAULT_SETTINGS["redeem_gst_cut"])
    return max(0, value)

def get_active_redeem_codes(limit=None):
    query = (
        "SELECT * FROM redeem_codes WHERE is_active=1 AND assigned_to=0 "
        "ORDER BY amount ASC, platform ASC, id ASC"
    )
    if limit:
        query += f" LIMIT {int(limit)}"
    return db_execute(query, fetch=True) or []

def get_redeem_code_by_id(code_id):
    return db_execute("SELECT * FROM redeem_codes WHERE id=?", (code_id,), fetchone=True)

def get_redeem_inventory_summary():
    return db_execute(
        "SELECT platform, amount, COUNT(*) as cnt FROM redeem_codes "
        "WHERE is_active=1 AND assigned_to=0 GROUP BY platform, amount "
        "ORDER BY amount ASC, platform ASC",
        fetch=True
    ) or []

def assign_redeem_code_atomic(code_id, user_id):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with DB_LOCK:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute(
                "SELECT * FROM redeem_codes WHERE id=? AND is_active=1 AND assigned_to=0",
                (code_id,)
            )
            row = c.fetchone()
            if not row:
                conn.rollback()
                return None
            c.execute(
                "UPDATE redeem_codes SET is_active=0, assigned_to=?, assigned_at=? WHERE id=? AND is_active=1 AND assigned_to=0",
                (user_id, now, code_id)
            )
            if c.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return dict(row)
        except Exception as e:
            conn.rollback()
            print(f"Redeem assign error: {e}")
            return None
        finally:
            conn.close()

def show_upi_withdraw(chat_id, user_id):
    user = get_user(user_id)
    if not user:
        safe_send(chat_id, "Please send /start first.")
        return

    limit_result = withdraw_limit.check_and_send_limit_message(chat_id, user_id)
    if not limit_result["allowed"]:
        return

    today_withdraws = limit_result["used_today"]
    daily_limit = limit_result["daily_limit"]
    min_withdraw = get_setting("min_withdraw")

    if user["balance"] < min_withdraw:
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("👥 Refer & Earn More", callback_data="open_refer"))
        safe_send(
            chat_id,
            f"{pe('warning')} <b>Insufficient Balance!</b>\n\n"
            f"{pe('fly_money')} Balance: ₹{user['balance']:.2f}\n"
            f"{pe('down_arrow')} Minimum: ₹{min_withdraw}\n"
            f"{pe('calendar')} <b>Daily Limit:</b> {daily_limit} withdrawals per day\n"
            f"{pe('calendar')} <b>Today's Withdrawals:</b> {today_withdraws}/{daily_limit}\n"
            f"{pe('excl')} Need ₹{max(0, min_withdraw - user['balance']):.2f} more\n\n"
            f"{pe('arrow')} Refer friends to earn more!",
            reply_markup=markup
        )
        return

    if user["upi_id"]:
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton(f"✅ Use: {user['upi_id']}", callback_data="use_saved_upi"),
            types.InlineKeyboardButton("✏️ Use Different UPI ID", callback_data="enter_new_upi"),
            types.InlineKeyboardButton("🔙 Back", callback_data="open_withdraw")
        )
        withdraw_image = get_setting("withdraw_image")
        caption = (
            f"{pe('fly_money')} <b>UPI Withdraw Funds</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{pe('money')} <b>Balance:</b> ₹{user['balance']:.2f}\n"
            f"{pe('calendar')} <b>Daily Limit:</b> {daily_limit} withdrawals per day\n"
            f"{pe('calendar')} <b>Today's Withdrawals:</b> {today_withdraws}/{daily_limit}\n"
            f"{pe('down_arrow')} <b>Min:</b> ₹{min_withdraw}\n"
            f"{pe('link')} <b>Saved UPI:</b> {user['upi_id']}\n\n"
            f"{pe('question2')} Choose an option:\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        try:
            bot.send_photo(chat_id, withdraw_image, caption=caption, parse_mode="HTML", reply_markup=markup)
        except:
            safe_send(chat_id, caption, reply_markup=markup)
    else:
        set_state(user_id, "enter_upi")
        safe_send(
            chat_id,
            f"{pe('pencil')} <b>Enter Your UPI ID</b>\n\n"
            f"{pe('calendar')} <b>Daily Limit:</b> {daily_limit} withdrawals per day\n"
            f"{pe('calendar')} <b>Today's Withdrawals:</b> {today_withdraws}/{daily_limit}\n\n"
            f"{pe('info')} Valid formats:\n"
            f"  <code>name@paytm</code>\n"
            f"  <code>9876543210@okaxis</code>\n"
            f"  <code>name@ybl</code>\n\n"
            f"{pe('warning')} Double-check your UPI ID!"
        )


def show_redeem_withdraw(chat_id, user_id):
    user = get_user(user_id)
    if not user:
        safe_send(chat_id, "Please send /start first.")
        return

    if not get_setting("redeem_withdraw_enabled"):
        safe_send(chat_id, f"{pe('no_entry')} <b>Redeem code withdrawals are disabled right now.</b>")
        return

    redeem_min = get_redeem_min_withdraw()
    gst_cut = get_redeem_gst_cut()
    summary = get_redeem_inventory_summary()
    if not summary:
        safe_send(chat_id, f"{pe('warning')} <b>No redeem codes are available right now.</b>")
        return

    available_lines = []
    active_codes = get_active_redeem_codes(limit=20)
    for row in summary[:20]:
        available_lines.append(f"• {row['platform']} — ₹{row['amount']:.0f} ({row['cnt']} available)")

    markup = types.InlineKeyboardMarkup(row_width=2)
    for row in active_codes[:20]:
        label = f"{row['platform'][:14]} ₹{row['amount']:.0f}"
        markup.add(types.InlineKeyboardButton(label, callback_data=f"rwsel|{row['id']}"))
    markup.add(types.InlineKeyboardButton("🔙 Back", callback_data="open_withdraw"))

    safe_send(
        chat_id,
        f"{pe('tag')} <b>Redeem Code Withdraw</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{pe('money')} <b>Your Balance:</b> ₹{user['balance']:.2f}\n"
        f"{pe('down_arrow')} <b>Minimum Code Value:</b> ₹{redeem_min:.0f}\n"
        f"{pe('info')} <b>GST / Fee:</b> ₹{gst_cut:.0f} extra per redemption\n"
        f"{pe('arrow')} <b>Allowed amounts:</b> multiples of ₹{get_redeem_multiple_of():.0f} only\n\n"
        f"{pe('list')} <b>Available Codes:</b>\n" + "\n".join(available_lines) + "\n\n"
        f"{pe('warning')} You will be charged <b>Code Amount + ₹{gst_cut:.0f}</b> from your balance.",
        reply_markup=markup
    )

def create_user(user_id, username, first_name, referred_by=0):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    welcome_bonus = get_setting("welcome_bonus") or 0
    existing = get_user(user_id)
    if existing:
        return False

    db_execute(
        "INSERT OR IGNORE INTO users "
        "(user_id, username, first_name, balance, total_earned, referred_by, joined_at, referral_paid, ip_address, ip_verified) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (user_id, username or "", first_name or "User", 0, welcome_bonus, referred_by, now, 0, "", 0)
    )

    if welcome_bonus > 0:
        add_user_balance(user_id, welcome_bonus, "bonus_balance", "welcome_bonus")

    if referred_by and referred_by != user_id:
        referer = get_user(referred_by)
        if referer:
            try:
                safe_send(
                    referred_by,
                    f"{pe('bell')} <b>New Referral Joined!</b>\n\n"
                    f"{pe('info')} A user joined using your link.\n"
                    f"{pe('hourglass')} Waiting for channel join and IP verification.\n\n"
                    f"{pe('sparkle')} Reward will be added after verification!"
                )
            except:
                pass

    return True


# 👇 YAHAN YE NAYA FUNCTION ADD KARO
def process_referral_bonus(user_id):
    user = get_user(user_id)
    if not user:
        return False

    referred_by = user["referred_by"] or 0
    referral_paid = user["referral_paid"] or 0
    ip_verified = user["ip_verified"] or 0

    if int(ip_verified) != 1:
        return False

    if not referred_by:
        return False

    if int(referred_by) == int(user_id):
        return False

    if int(referral_paid) == 1:
        return False

    referer = get_user(referred_by)
    if not referer:
        return False

    per_refer = get_setting("per_refer") or 0

    db_execute(
        "UPDATE users SET balance=balance+?, total_earned=total_earned+?, referral_count=referral_count+1 WHERE user_id=?",
        (per_refer, per_refer, referred_by)
    )

    db_execute(
        "UPDATE users SET referral_paid=1 WHERE user_id=?",
        (user_id,)
    )

    try:
        safe_send(
            referred_by,
            f"{pe('party')} <b>Referral Bonus Claimed!</b>\n\n"
            f"{pe('check')} Your referred user completed channel join and IP verification.\n"
            f"{pe('money')} You earned <b>₹{per_refer}</b>\n\n"
            f"{pe('fire')} Keep sharing to earn more!"
        )
    except:
        pass

    return True

# 👇 FIR YE SAME REHNE DO
def update_user(user_id, **kwargs):
    if not kwargs:
        return
    sets = ", ".join([f"{k}=?" for k in kwargs])
    vals = list(kwargs.values()) + [user_id]
    db_execute(f"UPDATE users SET {sets} WHERE user_id=?", tuple(vals))

def generate_code(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def generate_txn_id():
    return "TXN" + ''.join(random.choices(string.digits, k=10))
#=================ip verify================
def send_ip_verify_message(chat_id, user_id):
    anticheat.send_ip_verify_message(chat_id, user_id)

# ======================== ADMIN MANAGEMENT ========================
def is_admin(user_id):
    if int(user_id) == int(ADMIN_ID):
        return True
    row = db_execute(
        "SELECT * FROM admins WHERE user_id=? AND is_active=1",
        (int(user_id),), fetchone=True
    )
    return row is not None

def is_super_admin(user_id):
    return int(user_id) == int(ADMIN_ID)

def get_all_admins():
    return db_execute("SELECT * FROM admins WHERE is_active=1", fetch=True) or []

def add_admin(user_id, username, first_name, added_by, permissions="all"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_execute(
        "INSERT OR REPLACE INTO admins (user_id, username, first_name, added_by, added_at, permissions, is_active) "
        "VALUES (?,?,?,?,?,?,?)",
        (int(user_id), username or "", first_name or "", int(added_by), now, permissions, 1)
    )

def remove_admin(user_id):
    db_execute("UPDATE admins SET is_active=0 WHERE user_id=?", (int(user_id),))

def log_admin_action(admin_id, action, details=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_execute(
        "INSERT INTO admin_logs (admin_id, action, details, created_at) VALUES (?,?,?,?)",
        (admin_id, action, details, now)
    )

def get_admin_logs(limit=50):
    return db_execute(
        "SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT ?",
        (limit,), fetch=True
    ) or []

# ======================== SAFE SEND / EDIT ========================
def safe_send(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, parse_mode="HTML", **kwargs)
    except Exception as e:
        print(f"safe_send error to {chat_id}: {e}")
        return None

def safe_edit(chat_id, message_id, text, **kwargs):
    try:
        return bot.edit_message_text(
            text, chat_id=chat_id, message_id=message_id,
            parse_mode="HTML", **kwargs
        )
    except Exception as e:
        print(f"safe_edit error: {e}")
        return None

def safe_answer(call, text="", alert=False):
    try:
        bot.answer_callback_query(call.id, text, show_alert=alert)
    except:
        pass


# ======================== SYSTEMS INIT ========================

anticheat = AntiCheatSystem(
    bot=bot,
    db_path=DB_PATH,
    db_execute=db_execute,
    get_user=get_user,
    update_user=update_user,
    get_setting=get_setting,
    set_setting=set_setting,
    safe_send=safe_send,
    safe_answer=safe_answer,
    is_admin=is_admin,
    pe=pe,
    process_referral_bonus=process_referral_bonus,
)
anticheat.init_schema()
anticheat.register_bot_handlers()

broadcaster = BroadcastSystem(
    bot=bot,
    is_admin=is_admin,
    get_all_users=get_all_users,
    safe_send=safe_send,
    log_admin_action=log_admin_action,
)
broadcaster.register_handlers()

db_importer = DatabaseImportSystem(
    bot=bot,
    is_admin=is_admin,
    safe_send=safe_send,
    db_path=DB_PATH,
    get_db=get_db,
    db_execute=db_execute,
    log_admin_action=log_admin_action,
)
db_importer.register_handlers()

withdraw_limit = WithdrawLimitSystem(
    db_execute=db_execute,
    get_setting=get_setting,
    set_setting=set_setting,
    safe_send=safe_send,
    pe=pe,
)

withdraw_limit.ensure_settings()
admin_help = AdminHelpSystem(
    bot=bot,
    is_admin=is_admin,
    safe_send=safe_send,
    pe=pe,
)

admin_help.register_handlers()
user_states = {}
states_lock = threading.Lock()

# ======================== DB GET (Admin) ========================
def set_state(user_id, state, data=None):
    with states_lock:
        user_states[int(user_id)] = {"state": state, "data": data or {}}

def get_state(user_id):
    with states_lock:
        return user_states.get(int(user_id), {}).get("state")

def get_state_data(user_id):
    with states_lock:
        return user_states.get(int(user_id), {}).get("data", {})

def clear_state(user_id):
    with states_lock:
        user_states.pop(int(user_id), None)

# ======================== KEYBOARDS ========================
def get_main_keyboard(user_id=None):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("💰 Balance"),
        types.KeyboardButton("👥 Refer"),
    )
    markup.add(
        types.KeyboardButton("🏧 Withdraw"),
        types.KeyboardButton("🎁 Bonus"),
    )
    markup.add(
        types.KeyboardButton("📋 Tasks"),
    )
    if user_id and is_admin(user_id):
        markup.add(types.KeyboardButton("👑 Admin Panel"))
    return markup

def get_admin_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("📊 Dashboard"),
        types.KeyboardButton("👥 All Users"),
    )
    markup.add(
        types.KeyboardButton("💳 Withdrawals"),
        types.KeyboardButton("⚙️ Settings"),
    )
    markup.add(
        types.KeyboardButton("📢 Broadcast"),
        types.KeyboardButton("🎁 Gift Manager"),
    )
    markup.add(
        types.KeyboardButton("🎟 Redeem Codes"),
    )
    markup.add(
        types.KeyboardButton("📋 Task Manager"),
        types.KeyboardButton("🗄 DB Manager"),
    )
    markup.add(
        types.KeyboardButton("👮 Admin Manager"),
        types.KeyboardButton("🔙 User Panel"),
    )
    return markup

# ======================== FORCE JOIN ========================
def check_force_join(user_id):
    for channel in FORCE_JOIN_CHANNELS:
        try:
            member = bot.get_chat_member(channel, user_id)
            if member.status in ["left", "kicked"]:
                return False
        except Exception as e:
            print(f"Force join check error for {channel}: {e}")
            return False
    return True

def send_join_message(chat_id):
    join_image = "https://advisory-brown-r63twvnsdu.edgeone.app/c693132c-cd1f-4a81-9b5e-8b8f042e490b.png"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton("🔏 Join", url=REQUEST_CHANNEL))
    channel_buttons = [
        types.InlineKeyboardButton("🔒 Join", url="https://t.me/skullmodder"),
        types.InlineKeyboardButton("🔒 Join", url="https://t.me/botsarefather"),
        types.InlineKeyboardButton("🔒 Join", url="https://t.me/upilootpay"),
        types.InlineKeyboardButton("🔒 Join", url="https://tinyurl.com/UpiLootpay"),
        ]
    markup.add(*channel_buttons[:2])
    markup.add(*channel_buttons[2:])
    markup.add(types.InlineKeyboardButton("🔐Joined - Verify", callback_data="verify_join"))
    caption = (
        f"{pe('warning')} <b>Join Required</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{pe('arrow')} Please join all channels below first.\n"
        f"{pe('info')} After joining, tap <b>🔐Joined - Verify</b>.\n\n"
        f"{pe('excl')} <b>Note:</b> Force join works only for public channels where the bot is admin.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    try:
        bot.send_photo(chat_id, join_image, caption=caption, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        print(f"send_join_message photo error: {e}")
        bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=markup)

# ======================== NOTIFICATIONS ========================
def send_public_withdrawal_notification(user_id, amount, upi_id, status, txn_id=""):
    try:
        user = get_user(user_id)
        name = user["first_name"] if user else "User"
        masked = (upi_id[:3] + "****" + upi_id[-4:]) if len(upi_id) > 7 else "****"
        bot_username = bot.get_me().username
        WD_IMAGE = "https://image2url.com/r2/default/images/1775843858548-29ae7a16-81b2-4c75-aded-cfb3093df954.png"
        if status == "approved":
            text = (
                f"<b>╔══════════════════════╗</b>\n"
                f"<b>      💸 PAYMENT SENT! ✅      </b>\n"
                f"<b>╚══════════════════════╝</b>\n\n"
                f"🎉 <b>{name}</b> just got paid!\n\n"
                f"┌─────────────────────\n"
                f"│ 💰 <b>Amount</b>  →  <b>₹{amount}</b>\n"
                f"│ 🏦 <b>UPI</b>     →  <code>{masked}</code>\n"
                f"│ 🔖 <b>TXN ID</b>  →  <code>{txn_id}</code>\n"
                f"│ ✅ <b>Status</b>  →  Approved\n"
                f"└─────────────────────\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🚀 <b>You can earn too!</b>\n"
                f"👉 Join → @{bot_username}\n"
                f"💎 Refer friends & earn <b>₹{get_setting('per_refer')}</b> each!\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("💰 Start Earning Now", url=f"https://t.me/{bot_username}"))
            bot.send_photo(NOTIFICATION_CHANNEL, photo=WD_IMAGE, caption=text, parse_mode="HTML", reply_markup=markup)
        else:
            text = (
                f"<b>╔══════════════════════╗</b>\n"
                f"<b>      ❌ WITHDRAWAL REJECTED      </b>\n"
                f"<b>╚══════════════════════╝</b>\n\n"
                f"👤 <b>User:</b> {name}\n"
                f"💸 <b>Amount:</b> ₹{amount}\n\n"
                f"📩 For help → {HELP_USERNAME}"
            )
            bot.send_message(NOTIFICATION_CHANNEL, text, parse_mode="HTML")
    except Exception as e:
        print(f"Notification error: {e}")

# ======================== TASK HELPERS ========================
def get_task(task_id):
    return db_execute("SELECT * FROM tasks WHERE id=?", (task_id,), fetchone=True)

def get_active_tasks():
    return db_execute(
        "SELECT * FROM tasks WHERE status='active' ORDER BY order_num ASC, id DESC",
        fetch=True
    ) or []

def get_all_tasks():
    return db_execute(
        "SELECT * FROM tasks ORDER BY order_num ASC, id DESC",
        fetch=True
    ) or []

def get_task_completion(task_id, user_id):
    return db_execute(
        "SELECT * FROM task_completions WHERE task_id=? AND user_id=?",
        (task_id, user_id), fetchone=True
    )

def get_task_submission(task_id, user_id):
    return db_execute(
        "SELECT * FROM task_submissions WHERE task_id=? AND user_id=? ORDER BY id DESC",
        (task_id, user_id), fetchone=True
    )

def get_pending_task_submissions():
    return db_execute(
        "SELECT ts.*, t.title as task_title, t.reward as task_reward "
        "FROM task_submissions ts "
        "JOIN tasks t ON ts.task_id = t.id "
        "WHERE ts.status='pending' ORDER BY ts.submitted_at DESC",
        fetch=True
    ) or []

def get_task_submission_by_id(sub_id):
    return db_execute(
        "SELECT ts.*, t.title as task_title, t.reward as task_reward, t.task_type "
        "FROM task_submissions ts "
        "JOIN tasks t ON ts.task_id = t.id "
        "WHERE ts.id=?",
        (sub_id,), fetchone=True
    )

def get_user_completed_tasks(user_id):
    return db_execute(
        "SELECT tc.*, t.title as task_title FROM task_completions tc "
        "JOIN tasks t ON tc.task_id = t.id WHERE tc.user_id=?",
        (user_id,), fetch=True
    ) or []

def get_task_stats(task_id):
    total = db_execute(
        "SELECT COUNT(*) as c FROM task_submissions WHERE task_id=?",
        (task_id,), fetchone=True
    )
    pending = db_execute(
        "SELECT COUNT(*) as c FROM task_submissions WHERE task_id=? AND status='pending'",
        (task_id,), fetchone=True
    )
    approved = db_execute(
        "SELECT COUNT(*) as c FROM task_submissions WHERE task_id=? AND status='approved'",
        (task_id,), fetchone=True
    )
    rejected = db_execute(
        "SELECT COUNT(*) as c FROM task_submissions WHERE task_id=? AND status='rejected'",
        (task_id,), fetchone=True
    )
    return {
        "total": total["c"] if total else 0,
        "pending": pending["c"] if pending else 0,
        "approved": approved["c"] if approved else 0,
        "rejected": rejected["c"] if rejected else 0,
    }

TASK_TYPE_EMOJI = {
    "channel": "📢","youtube": "▶️","instagram": "📸","twitter": "🐦",
    "facebook": "📘","website": "🌐","app": "📱","survey": "📋",
    "referral": "👥","custom": "⚡","video": "🎬","follow": "➕",
}

def get_task_type_emoji(task_type):
    return TASK_TYPE_EMOJI.get(task_type, "⚡")


# ======================== ADVANCED FEATURE EXTENSIONS ========================
ADVANCED_DEFAULT_SETTINGS = {
    "bonus_section_title": "Bonus",
    "multi_referral_enabled": True,
    "referral_level_1_type": "fixed",
    "referral_level_1_value": 2.0,
    "referral_level_2_type": "fixed",
    "referral_level_2_value": 1.0,
    "referral_level_3_type": "fixed",
    "referral_level_3_value": 0.5,
    "referral_trigger": "verification",
    "daily_bonus_random_enabled": False,
    "daily_bonus_random_min": 0.2,
    "daily_bonus_random_max": 1.0,
    "daily_bonus_min_referrals": 1,
    "claim_code_min_referrals": 2,
    "activity_deduction_enabled": True,
    "activity_deduction_percent": 10.0,
    "activity_inactivity_hours": 24,
    "activity_min_balance_floor": 0.01,
    "activity_require_referral": True,
    "activity_require_any_action": True,
    "bonus_withdraw_tax_enabled": True,
    "bonus_withdraw_tax_percent": 70.0,
    "bonus_withdraw_taxable_wallets": ["bonus_balance"],
    "upi_withdraw_gst_enabled": False,
    "upi_withdraw_gst_percent": 0.0,
    "gift_code_gst_enabled": False,
    "gift_code_gst_percent": 0.0,
    "game_hub_enabled": True,
    "game_mines_enabled": True,
    "game_mines_min_bet": 1.0,
    "game_mines_max_bet": 25.0,
    "game_mines_win_ratio": 1.8,
    "game_mines_daily_limit": 25,
    "game_mines_cooldown_seconds": 10,
    "game_mines_visible": True,
    "game_webapp_title": "Games",
    "game_webapp_base_url": "",
}
for _k, _v in ADVANCED_DEFAULT_SETTINGS.items():
    DEFAULT_SETTINGS.setdefault(_k, _v)
    if get_setting(_k) is None:
        set_setting(_k, _v)

def ensure_advanced_schema():
    conn = get_db()
    c = conn.cursor()
    for stmt in [
        "ALTER TABLE users ADD COLUMN bonus_balance REAL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN task_balance REAL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN game_balance REAL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN locked_balance REAL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN last_active_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN last_referral_at TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN last_deduction_at TEXT DEFAULT ''",
        "ALTER TABLE withdrawals ADD COLUMN tax_percent REAL DEFAULT 0",
        "ALTER TABLE withdrawals ADD COLUMN tax_amount REAL DEFAULT 0",
        "ALTER TABLE withdrawals ADD COLUMN source_wallets TEXT DEFAULT ''",
        "ALTER TABLE withdrawals ADD COLUMN source_breakdown TEXT DEFAULT ''",
    ]:
        try:
            c.execute(stmt)
        except Exception:
            pass
    c.executescript("""
        CREATE TABLE IF NOT EXISTS referral_earnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_user_id INTEGER DEFAULT 0,
            beneficiary_user_id INTEGER DEFAULT 0,
            level_no INTEGER DEFAULT 0,
            reward_type TEXT DEFAULT 'fixed',
            reward_value REAL DEFAULT 0,
            amount REAL DEFAULT 0,
            trigger_type TEXT DEFAULT 'verification',
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS user_activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER DEFAULT 0,
            activity_type TEXT DEFAULT '',
            meta TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS game_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER DEFAULT 0,
            game_name TEXT DEFAULT '',
            bet_amount REAL DEFAULT 0,
            reward_amount REAL DEFAULT 0,
            result TEXT DEFAULT '',
            meta TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
        );
    """)
    conn.commit(); conn.close()
ensure_advanced_schema()

def touch_user_activity(user_id, activity_type="activity", meta=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_execute("UPDATE users SET last_active_at=? WHERE user_id=?", (now, user_id))
    db_execute("INSERT INTO user_activity_log (user_id, activity_type, meta, created_at) VALUES (?,?,?,?)", (user_id, activity_type, str(meta)[:500], now))

def sync_total_balance(user_id):
    user = get_user(user_id)
    if not user:
        return
    total = round(float(user["bonus_balance"] or 0) + float(user["referral_balance"] or 0) + float(user["task_balance"] or 0) + float(user["game_balance"] or 0), 2)
    db_execute("UPDATE users SET balance=? WHERE user_id=?", (max(0, total), user_id))

def add_user_balance(user_id, amount, wallet_type="bonus_balance", bonus_type="system"):
    amount = round(float(amount or 0), 2)
    if amount <= 0:
        return False
    if wallet_type not in ["bonus_balance", "referral_balance", "task_balance", "game_balance"]:
        wallet_type = "bonus_balance"
    db_execute(f"UPDATE users SET {wallet_type}=COALESCE({wallet_type},0)+?, total_earned=COALESCE(total_earned,0)+? WHERE user_id=?", (amount, amount, user_id))
    sync_total_balance(user_id)
    db_execute("INSERT INTO bonus_history (user_id, amount, bonus_type, created_at) VALUES (?,?,?,?)", (user_id, amount, bonus_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    touch_user_activity(user_id, bonus_type, f"+{amount} -> {wallet_type}")
    return True

def deduct_user_balance(user_id, amount, preferred_wallets=None):
    amount = round(float(amount or 0), 2)
    user = get_user(user_id)
    if not user:
        return {"ok": False, "reason": "user_not_found", "breakdown": {}}
    order = preferred_wallets or ["task_balance", "referral_balance", "game_balance", "bonus_balance"]
    remaining = amount
    breakdown = {}
    for wallet in order:
        avail = round(float(user[wallet] or 0), 2) if wallet in user.keys() else 0
        if avail <= 0:
            continue
        take = min(avail, remaining)
        if take > 0:
            breakdown[wallet] = take
            remaining = round(remaining - take, 2)
            if remaining <= 0:
                break
    if remaining > 0:
        return {"ok": False, "reason": "insufficient_balance", "breakdown": breakdown}
    for wallet, val in breakdown.items():
        db_execute(f"UPDATE users SET {wallet}=MAX(0, COALESCE({wallet},0)-?) WHERE user_id=?", (val, user_id))
    sync_total_balance(user_id)
    touch_user_activity(user_id, "balance_deducted", json.dumps(breakdown))
    return {"ok": True, "deducted": amount, "breakdown": breakdown}

def get_referral_levels():
    return [{"level": i, "type": get_setting(f"referral_level_{i}_type") or "fixed", "value": float(get_setting(f"referral_level_{i}_value") or 0)} for i in range(1,4)]

def get_upline_chain(user_id, max_levels=3):
    chain=[]; seen={int(user_id)}; cur=get_user(user_id)
    while cur and len(chain)<max_levels:
        pid=int(cur["referred_by"] or 0)
        if not pid or pid in seen:
            break
        parent=get_user(pid)
        if not parent:
            break
        chain.append(parent); seen.add(pid); cur=parent
    return chain

def calculate_referral_reward(base_amount, level_cfg):
    if str(level_cfg.get("type","fixed")) == "percent":
        return round(float(base_amount or 0) * float(level_cfg.get("value",0))/100.0, 2)
    return round(float(level_cfg.get("value",0) or 0), 2)

def process_referral_bonus(user_id, base_amount=None):
    user = get_user(user_id)
    if not user or not get_setting("refer_enabled") or not get_setting("multi_referral_enabled"):
        return False
    if int(user["ip_verified"] or 0) != 1 or int(user["referral_paid"] or 0) == 1:
        return False
    base = float(base_amount if base_amount is not None else get_setting("per_refer") or 0)
    chain = get_upline_chain(user_id, 3)
    if not chain:
        return False
    levels = get_referral_levels(); now=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    paid=False
    for idx,parent in enumerate(chain, start=1):
        cfg=levels[idx-1]; reward=calculate_referral_reward(base,cfg)
        if reward<=0: continue
        add_user_balance(parent["user_id"], reward, "referral_balance", f"referral_l{idx}")
        db_execute("UPDATE users SET referral_count=COALESCE(referral_count,0)+1, last_referral_at=? WHERE user_id=?", (now, parent["user_id"]))
        db_execute("INSERT INTO referral_earnings (source_user_id, beneficiary_user_id, level_no, reward_type, reward_value, amount, trigger_type, created_at) VALUES (?,?,?,?,?,?,?,?)", (user_id, parent["user_id"], idx, cfg["type"], float(cfg["value"]), reward, get_setting("referral_trigger") or "verification", now))
        paid=True
    if paid:
        db_execute("UPDATE users SET referral_paid=1 WHERE user_id=?", (user_id,))
    return paid

def get_referral_leaderboard(limit=10):
    return db_execute("SELECT user_id, first_name, username, referral_count, referral_balance, total_earned FROM users ORDER BY referral_count DESC, referral_balance DESC LIMIT ?", (limit,), fetch=True) or []

def get_user_referral_breakdown(user_id):
    rows = db_execute("SELECT level_no, SUM(amount) AS total, COUNT(*) AS cnt FROM referral_earnings WHERE beneficiary_user_id=? GROUP BY level_no ORDER BY level_no ASC", (user_id,), fetch=True) or []
    return {int(r['level_no']): {"total": float(r['total'] or 0), "count": int(r['cnt'] or 0)} for r in rows}

def can_claim_daily_bonus(user_id):
    user=get_user(user_id); needed=int(get_setting("daily_bonus_min_referrals") or 0)
    if not user: return False, "User not found"
    if int(user["referral_count"] or 0) < needed: return False, f"Need at least {needed} referrals"
    return True, "ok"

def can_claim_code(user_id):
    user=get_user(user_id); needed=int(get_setting("claim_code_min_referrals") or 0)
    if not user: return False, "User not found"
    if int(user["referral_count"] or 0) < needed: return False, f"Need at least {needed} referrals"
    return True, "ok"

def get_daily_bonus_amount():
    if get_setting("daily_bonus_random_enabled"):
        mn=float(get_setting("daily_bonus_random_min") or 0); mx=float(get_setting("daily_bonus_random_max") or mn)
        if mx < mn: mx = mn
        return round(random.uniform(mn, mx), 2)
    return round(float(get_setting("daily_bonus") or 0), 2)

def compute_withdraw_tax(user_id, amount, method="upi"):
    user=get_user(user_id)
    if not user:
        return {"tax_percent":0.0,"tax_amount":0.0,"taxable":False}
    amount=float(amount or 0)
    tax_percent=0.0
    taxable_wallets=get_setting("bonus_withdraw_taxable_wallets") or ["bonus_balance"]
    taxable_total=sum(float(user[w] or 0) for w in taxable_wallets if w in user.keys())
    if get_setting("bonus_withdraw_tax_enabled") and amount <= taxable_total and amount > 0:
        tax_percent += float(get_setting("bonus_withdraw_tax_percent") or 0)
    if method == "upi" and get_setting("upi_withdraw_gst_enabled"):
        tax_percent += float(get_setting("upi_withdraw_gst_percent") or 0)
    if method == "redeem_code" and get_setting("gift_code_gst_enabled"):
        tax_percent += float(get_setting("gift_code_gst_percent") or 0)
    tax_amount=round(amount * tax_percent / 100.0, 2)
    return {"tax_percent": round(tax_percent,2), "tax_amount": tax_amount, "taxable": tax_amount>0}

def apply_inactivity_deductions():
    if not get_setting("activity_deduction_enabled"):
        return 0
    hours=max(1,int(get_setting("activity_inactivity_hours") or 24)); pct=max(0.0,float(get_setting("activity_deduction_percent") or 0)); floor=max(0.0,float(get_setting("activity_min_balance_floor") or 0.01)); now=datetime.now(); affected=0
    for user in get_all_users():
        balance=float(user['balance'] or 0)
        if balance <= floor:
            continue
        last_active=str(user['last_active_at'] or user['joined_at'] or '')
        try: last_dt=datetime.strptime(last_active, "%Y-%m-%d %H:%M:%S") if last_active else now
        except Exception: last_dt=now
        inactive=(now-last_dt).total_seconds() >= hours*3600
        no_ref_today=bool(get_setting("activity_require_referral")) and str(user['last_referral_at'] or '')[:10] != now.strftime("%Y-%m-%d")
        no_action=bool(get_setting("activity_require_any_action")) and inactive
        if not (no_ref_today or no_action):
            continue
        if str(user['last_deduction_at'] or '')[:10] == now.strftime("%Y-%m-%d"):
            continue
        deduct=round(balance * pct / 100.0, 2)
        if balance - deduct < floor:
            deduct=round(max(0.0, balance-floor), 2)
        if deduct <= 0:
            continue
        res=deduct_user_balance(user['user_id'], deduct)
        if not res.get('ok'):
            continue
        db_execute("UPDATE users SET last_deduction_at=? WHERE user_id=?", (now.strftime("%Y-%m-%d %H:%M:%S"), user['user_id']))
        affected += 1
    return affected

def _background_maintenance_loop():
    while True:
        try: apply_inactivity_deductions()
        except Exception as e: print(f"background maintenance error: {e}")
        time.sleep(3600)

def start_background_maintenance():
    t = threading.Thread(target=_background_maintenance_loop, daemon=True)
    t.start(); return t
start_background_maintenance()
try:
    anticheat.process_referral_bonus = process_referral_bonus
except Exception:
    pass
