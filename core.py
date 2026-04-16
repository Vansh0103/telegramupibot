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
    "daily_bonus_random_enabled": False,
    "daily_bonus_random_min": 0.2,
    "daily_bonus_random_max": 2.0,
    "daily_bonus_min_referrals": 1,
    "gift_code_min_referrals": 2,
    "max_withdraw_per_day": 100,
    "withdraw_enabled": True,
    "refer_enabled": True,
    "gift_enabled": True,
    "games_enabled": True,
    "game_label": "Bonus",
    "bonus_label": "Bonus",
    "gift_label": "Gift",
    "games_section_title": "Games",
    "gift_section_title": "Gift",
    "bot_maintenance": False,
    "welcome_image": WELCOME_IMAGE,
    "withdraw_image": WITHDRAWAL_IMAGE,
    "withdraw_time_start": 0,
    "withdraw_time_end": 23,
    "max_gift_create": 100,
    "min_gift_amount": 3,
    "tasks_enabled": True,
    "redeem_withdraw_enabled": True,
    "redeem_min_withdraw": 1,
    "redeem_multiple_of": 1,
    "redeem_gst_cut": 3,
    "upi_gst_enabled": True,
    "upi_gst_cut": 0,
    "withdraw_bonus_tax_enabled": True,
    "withdraw_bonus_tax_percent": 70,
    "withdraw_bonus_tax_balance_types": ["daily_bonus", "welcome_bonus", "gift_claim", "gift_admin", "game_reward"],
    "referral_system_enabled": True,
    "referral_levels_enabled": True,
    "referral_bonus_type": "fixed",
    "referral_fixed_levels": {"level1": 2, "level2": 1, "level3": 0.5},
    "referral_percent_levels": {"level1": 0, "level2": 0, "level3": 0},
    "referral_condition_require_verified": True,
    "referral_condition_min_user_balance": 0,
    "referral_leaderboard_enabled": True,
    "activity_deduction_enabled": True,
    "activity_deduction_percent": 10,
    "activity_deduction_mode": "no_activity_or_referral",
    "activity_deduction_inactivity_days": 1,
    "activity_deduction_min_balance": 0.1,
    "activity_deduction_run_on_actions": True,
    "ip_verification_enabled": True,
    "game_style": "web",
    "mine_game_enabled": True,
    "mine_game_web_enabled": True,
    "mine_game_normal_enabled": True,
    "mine_game_visible": True,
    "mine_game_win_ratio": 0.55,
    "mine_game_min_bet": 1,
    "mine_game_max_bet": 100,
    "mine_game_reward_multiplier": 1.8,
    "mine_game_loss_multiplier": 1.0,
    "mine_game_daily_limit": 25,
    "mine_game_cooldown_seconds": 15,
    "mine_game_result_display": True,
    "site_name": "UPI Loot Pay",
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
        CREATE TABLE IF NOT EXISTS balance_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL DEFAULT 0,
            balance_type TEXT DEFAULT 'main',
            direction TEXT DEFAULT 'credit',
            source TEXT DEFAULT '',
            note TEXT DEFAULT '',
            reference_id TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS game_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            game_key TEXT DEFAULT 'mine',
            bet_amount REAL DEFAULT 0,
            result TEXT DEFAULT 'pending',
            reward_amount REAL DEFAULT 0,
            payload TEXT DEFAULT '',
            created_at TEXT DEFAULT ''
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

    try:
        c.execute("ALTER TABLE users ADD COLUMN bonus_balance REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN gift_balance REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN game_balance REAL DEFAULT 0")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN last_active_at TEXT DEFAULT ''")
    except:
        pass

    try:
        c.execute("ALTER TABLE users ADD COLUMN last_referral_at TEXT DEFAULT ''")
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


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def get_float_setting(key, default=0.0):
    try:
        return float(get_setting(key))
    except Exception:
        return float(default)


def get_int_setting(key, default=0):
    try:
        return int(float(get_setting(key)))
    except Exception:
        return int(default)


def get_balance_field_for_type(balance_type):
    mapping = {
        "referral": "referral_balance",
        "referral_bonus": "referral_balance",
        "daily_bonus": "bonus_balance",
        "welcome_bonus": "bonus_balance",
        "gift_claim": "gift_balance",
        "gift_admin": "gift_balance",
        "game_reward": "game_balance",
        "game_loss": "game_balance",
    }
    return mapping.get(balance_type)


def add_ledger_entry(user_id, amount, balance_type="main", direction="credit", source="", note="", reference_id=""):
    db_execute(
        "INSERT INTO balance_ledger (user_id, amount, balance_type, direction, source, note, reference_id, created_at) VALUES (?,?,?,?,?,?,?,?)",
        (user_id, amount, balance_type, direction, source, note, reference_id, now_str())
    )


def credit_user_balance(user_id, amount, balance_type="main", source="", note="", increment_total_earned=True):
    user = get_user(user_id)
    if not user:
        return False
    field = get_balance_field_for_type(balance_type)
    kwargs = {"balance": float(user["balance"] or 0) + float(amount), "last_active_at": now_str()}
    if increment_total_earned:
        kwargs["total_earned"] = float(user["total_earned"] or 0) + float(amount)
    if field:
        kwargs[field] = float(user[field] or 0) + float(amount)
    update_user(user_id, **kwargs)
    add_ledger_entry(user_id, amount, balance_type, "credit", source, note)
    return True


def debit_user_balance(user_id, amount, balance_type="main", source="", note=""):
    user = get_user(user_id)
    if not user:
        return False
    kwargs = {"balance": max(0.0, float(user["balance"] or 0) - float(amount)), "last_active_at": now_str()}
    field = get_balance_field_for_type(balance_type)
    if field:
        kwargs[field] = max(0.0, float(user[field] or 0) - float(amount))
    update_user(user_id, **kwargs)
    add_ledger_entry(user_id, amount, balance_type, "debit", source, note)
    return True


def get_user_referral_leaderboard(limit=10):
    return db_execute(
        "SELECT user_id, first_name, username, referral_count, referral_balance, total_earned FROM users ORDER BY referral_count DESC, referral_balance DESC, total_earned DESC LIMIT ?",
        (limit,), fetch=True
    ) or []


def count_direct_referrals_since(user_id, since_date):
    row = db_execute(
        "SELECT COUNT(*) as cnt FROM users WHERE referred_by=? AND joined_at>=?",
        (user_id, since_date), fetchone=True
    )
    return int(row["cnt"] or 0) if row else 0


def can_user_claim_daily_bonus(user_id):
    user = get_user(user_id)
    if not user:
        return False, "User not found"
    needed = get_int_setting("daily_bonus_min_referrals", DEFAULT_SETTINGS["daily_bonus_min_referrals"])
    if needed <= 0:
        return True, "ok"
    recent = count_direct_referrals_since(user_id, today_str() + " 00:00:00")
    if recent < needed:
        return False, f"You need at least {needed} referral(s) today to claim daily bonus."
    return True, "ok"


def can_user_claim_gift_code(user_id):
    user = get_user(user_id)
    if not user:
        return False, "User not found"
    needed = get_int_setting("gift_code_min_referrals", DEFAULT_SETTINGS["gift_code_min_referrals"])
    if needed <= 0:
        return True, "ok"
    recent = count_direct_referrals_since(user_id, today_str() + " 00:00:00")
    if recent < needed:
        return False, f"You need at least {needed} referral(s) today to claim code/gift."
    return True, "ok"


def maybe_apply_inactivity_deduction(user_id):
    if not get_setting("activity_deduction_enabled"):
        return False, 0.0
    user = get_user(user_id)
    if not user:
        return False, 0.0
    days = max(1, get_int_setting("activity_deduction_inactivity_days", 1))
    pct = max(0.0, min(99.0, get_float_setting("activity_deduction_percent", 10)))
    min_keep = max(0.0, get_float_setting("activity_deduction_min_balance", 0.1))
    mode = get_setting("activity_deduction_mode") or "no_activity_or_referral"
    last_active = user["last_active_at"] or user["joined_at"] or now_str()
    try:
        inactive_days = (datetime.now() - datetime.strptime(last_active, "%Y-%m-%d %H:%M:%S")).total_seconds() / 86400.0
    except Exception:
        inactive_days = 0
    no_recent_ref = count_direct_referrals_since(user_id, (datetime.now() - __import__('datetime').timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")) == 0
    should_deduct = False
    if mode == "no_referral":
        should_deduct = no_recent_ref
    elif mode == "no_activity":
        should_deduct = inactive_days >= days
    else:
        should_deduct = inactive_days >= days or no_recent_ref
    if not should_deduct:
        return False, 0.0
    bal = float(user["balance"] or 0)
    if bal <= min_keep:
        return False, 0.0
    deduct = round(min(bal - min_keep, bal * pct / 100.0), 2)
    if deduct <= 0:
        return False, 0.0
    update_user(user_id, balance=round(bal - deduct, 2), last_active_at=now_str())
    add_ledger_entry(user_id, deduct, "main", "debit", "activity_deduction", f"Auto deduction {pct}% after inactivity")
    return True, deduct


def calculate_withdrawal_taxes(user_id, amount, method="upi"):
    user = get_user(user_id)
    if not user:
        return {"gross": amount, "tax": 0.0, "gst": 0.0, "net": amount, "bonus_tax": 0.0}
    amount = float(amount)
    gst = 0.0
    if method == "upi" and get_setting("upi_gst_enabled"):
        gst = max(0.0, get_float_setting("upi_gst_cut", 0))
    bonus_tax = 0.0
    if get_setting("withdraw_bonus_tax_enabled"):
        taxable_types = get_setting("withdraw_bonus_tax_balance_types") or []
        pools = []
        for t in taxable_types:
            field = get_balance_field_for_type(t)
            if field and field not in pools:
                pools.append(field)
        taxable_balance = sum(float(user[f] or 0) for f in pools)
        if taxable_balance >= amount:
            bonus_tax = round(amount * max(0.0, min(100.0, get_float_setting("withdraw_bonus_tax_percent", 70))) / 100.0, 2)
    net = max(0.0, round(amount - bonus_tax - gst, 2))
    return {"gross": amount, "tax": round(bonus_tax + gst, 2), "gst": gst, "net": net, "bonus_tax": bonus_tax}


def get_recent_game_history(user_id, limit=10):
    return db_execute("SELECT * FROM game_sessions WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, limit), fetch=True) or []


def get_game_stats_today(user_id, game_key="mine"):
    row = db_execute("SELECT COUNT(*) as cnt FROM game_sessions WHERE user_id=? AND game_key=? AND created_at LIKE ?", (user_id, game_key, today_str()+"%"), fetchone=True)
    return int(row["cnt"] or 0) if row else 0


def play_mine_game(user_id, bet_amount, choice=None, mode="normal"):
    if not get_setting("games_enabled") or not get_setting("mine_game_enabled"):
        return {"ok": False, "message": "Game is disabled by admin."}
    if mode == "web" and not get_setting("mine_game_web_enabled"):
        return {"ok": False, "message": "Web game style is disabled by admin."}
    if mode == "normal" and not get_setting("mine_game_normal_enabled"):
        return {"ok": False, "message": "Normal game style is disabled by admin."}
    user = get_user(user_id)
    if not user:
        return {"ok": False, "message": "User not found."}
    bet_amount = float(bet_amount)
    min_bet = get_float_setting("mine_game_min_bet", 1)
    max_bet = get_float_setting("mine_game_max_bet", 100)
    if bet_amount < min_bet or bet_amount > max_bet:
        return {"ok": False, "message": f"Bet must be between ₹{min_bet} and ₹{max_bet}."}
    if float(user["balance"] or 0) < bet_amount:
        return {"ok": False, "message": "Insufficient balance."}
    today_count = get_game_stats_today(user_id, "mine")
    limit = get_int_setting("mine_game_daily_limit", 25)
    if today_count >= limit:
        return {"ok": False, "message": "Daily game limit reached."}
    last = db_execute("SELECT created_at FROM game_sessions WHERE user_id=? AND game_key='mine' ORDER BY id DESC LIMIT 1", (user_id,), fetchone=True)
    cooldown = get_int_setting("mine_game_cooldown_seconds", 15)
    if last and last["created_at"]:
        try:
            secs = (datetime.now() - datetime.strptime(last["created_at"], "%Y-%m-%d %H:%M:%S")).total_seconds()
            if secs < cooldown:
                return {"ok": False, "message": f"Wait {int(cooldown-secs)}s before playing again."}
        except Exception:
            pass
    debit_user_balance(user_id, bet_amount, "main", "mine_game_bet", "Mine game bet")
    win_ratio = max(0.0, min(1.0, float(get_setting("mine_game_win_ratio") or 0.55)))
    win = random.random() < win_ratio
    multiplier = get_float_setting("mine_game_reward_multiplier", 1.8)
    reward = round(bet_amount * multiplier, 2) if win else 0.0
    if reward > 0:
        credit_user_balance(user_id, reward, "game_reward", "mine_game", "Mine game reward")
    payload = json.dumps({"choice": choice, "mode": mode, "win_ratio": win_ratio})
    db_execute("INSERT INTO game_sessions (user_id, game_key, bet_amount, result, reward_amount, payload, created_at) VALUES (?,?,?,?,?,?,?)", (user_id, 'mine', bet_amount, 'win' if win else 'lose', reward, payload, now_str()))
    return {"ok": True, "win": win, "bet": bet_amount, "reward": reward, "net": round(reward - bet_amount, 2), "message": "You won!" if win else "You lost!"}

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
        "(user_id, username, first_name, balance, total_earned, referred_by, joined_at, referral_paid, ip_address, ip_verified, bonus_balance, last_active_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (user_id, username or "", first_name or "User", welcome_bonus, welcome_bonus, referred_by, now, 0, "", 0, welcome_bonus, now)
    )
    add_ledger_entry(user_id, welcome_bonus, "welcome_bonus", "credit", "signup", "Welcome bonus")

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
    if not get_setting("referral_system_enabled") or not get_setting("refer_enabled"):
        return False
    user = get_user(user_id)
    if not user:
        return False
    referred_by = int(user["referred_by"] or 0)
    referral_paid = int(user["referral_paid"] or 0)
    ip_verified = int(user["ip_verified"] or 0)
    if get_setting("ip_verification_enabled") and ip_verified != 1:
        return False
    if not referred_by or referred_by == int(user_id) or referral_paid == 1:
        return False

    levels = []
    cur = referred_by
    for _ in range(3):
        if not cur:
            break
        parent = get_user(cur)
        if not parent:
            break
        levels.append(parent)
        cur = int(parent["referred_by"] or 0)

    bonus_type = (get_setting("referral_bonus_type") or "fixed").lower()
    fixed_levels = get_setting("referral_fixed_levels") or {"level1": 2, "level2": 1, "level3": 0.5}
    percent_levels = get_setting("referral_percent_levels") or {"level1": 0, "level2": 0, "level3": 0}
    base_amount = get_float_setting("per_refer", 0)
    paid_any = False

    for idx, parent in enumerate(levels, start=1):
        key = f"level{idx}"
        reward = float(fixed_levels.get(key, 0) or 0)
        if bonus_type == "percent":
            reward = round(base_amount * float(percent_levels.get(key, 0) or 0) / 100.0, 2)
        if reward <= 0:
            continue
        credit_user_balance(parent["user_id"], reward, "referral", f"referral_level_{idx}", f"Referral level {idx} reward from user {user_id}")
        if idx == 1:
            update_user(parent["user_id"], referral_count=int(parent["referral_count"] or 0) + 1, last_referral_at=now_str())
        try:
            safe_send(
                parent["user_id"],
                f"{pe('party')} <b>Referral Bonus Claimed!</b>\n\n"
                f"{pe('check')} Level {idx} referral completed verification.\n"
                f"{pe('money')} You earned <b>₹{reward:.2f}</b>"
            )
        except Exception:
            pass
        paid_any = True

    if paid_any:
        update_user(user_id, referral_paid=1)
    return paid_any
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
        types.KeyboardButton("🎮 Game Control"),
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
