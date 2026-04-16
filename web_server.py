import os
import logging
from anticheat import create_verification_app
from flask import render_template, request, jsonify
import sqlite3, json, random, datetime

# ================== CONFIG ==================

PORT = int(os.environ.get("PORT", 8000))
DB_PATH = os.environ.get("DB_PATH", "/data/bot_database.db")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "NeturalPredictorbot")

# ================== LOGGING ==================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logging.info("🚀 Starting IP Verification Server...")
logging.info(f"📂 DB_PATH: {DB_PATH}")
logging.info(f"🤖 BOT_USERNAME: {BOT_USERNAME}")

# ================== CREATE APP ==================

app = create_verification_app(
    DB_PATH=DB_PATH,
    BOT_USERNAME=BOT_USERNAME
)

# ================== EXTRA ROUTES ==================

@app.route("/debug")
def debug_info():
    return {
        "status": "running",
        "db_path": DB_PATH,
        "bot": BOT_USERNAME,
        "env_vars": list(os.environ.keys())
    }

@app.route("/ping")
def ping():
    return "pong"

# ================== ERROR HANDLING ==================

@app.errorhandler(404)
def not_found(e):
    return {
        "error": "Not Found",
        "message": "Invalid route"
    }, 404

@app.errorhandler(500)
def server_error(e):
    return {
        "error": "Server Error",
        "message": "Something went wrong"
    }, 500

# ================== START ==================

if __name__ == "__main__":
    logging.info(f"🌐 Running on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)



def ws_get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ws_get_setting(key, default=None):
    conn = ws_get_db(); cur = conn.cursor(); cur.execute("SELECT value FROM settings WHERE key=?", (key,)); row = cur.fetchone(); conn.close()
    if not row:
        return default
    try:
        return json.loads(row['value'])
    except Exception:
        return row['value']


def ws_get_user(user_id):
    conn = ws_get_db(); cur = conn.cursor(); cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,)); row = cur.fetchone(); conn.close(); return row


def ws_update_user_balance(user_id, delta, reward_type='game_reward'):
    conn = ws_get_db(); cur = conn.cursor(); user = ws_get_user(user_id)
    if not user:
        return False
    new_balance = max(0.0, float(user['balance'] or 0) + float(delta))
    game_balance = float(user['game_balance'] or 0) + (float(delta) if delta > 0 else 0)
    total_earned = float(user['total_earned'] or 0) + (float(delta) if delta > 0 else 0)
    cur.execute("UPDATE users SET balance=?, game_balance=?, total_earned=?, last_active_at=? WHERE user_id=?", (new_balance, game_balance, total_earned, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
    conn.commit(); conn.close(); return True


@app.route('/games/mine')
def mine_game_page():
    return render_template('mine.html', site_name=ws_get_setting('site_name', 'UPI Loot Pay'), user_id=request.args.get('user_id', ''))


@app.route('/api/games/mine/play', methods=['POST'])
def mine_game_play_api():
    data = request.get_json(silent=True) or {}
    user_id = int(data.get('user_id') or 0)
    bet = float(data.get('bet') or 0)
    if not ws_get_setting('games_enabled', True) or not ws_get_setting('mine_game_enabled', True):
        return jsonify({'ok': False, 'message': 'Game disabled by admin'})
    user = ws_get_user(user_id)
    if not user:
        return jsonify({'ok': False, 'message': 'User not found'})
    if float(user['balance'] or 0) < bet:
        return jsonify({'ok': False, 'message': 'Insufficient balance'})
    min_bet = float(ws_get_setting('mine_game_min_bet', 1) or 1)
    max_bet = float(ws_get_setting('mine_game_max_bet', 100) or 100)
    if bet < min_bet or bet > max_bet:
        return jsonify({'ok': False, 'message': f'Bet must be between ₹{min_bet} and ₹{max_bet}'})
    conn = ws_get_db(); cur = conn.cursor()
    cur.execute("UPDATE users SET balance=?, last_active_at=? WHERE user_id=?", (float(user['balance'] or 0)-bet, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
    win = random.random() < float(ws_get_setting('mine_game_win_ratio', 0.55) or 0.55)
    reward = round(bet * float(ws_get_setting('mine_game_reward_multiplier', 1.8) or 1.8), 2) if win else 0.0
    if reward > 0:
        user2 = ws_get_user(user_id)
        cur.execute("UPDATE users SET balance=?, game_balance=?, total_earned=?, last_active_at=? WHERE user_id=?", (float(user2['balance'] or 0)+reward, float(user2['game_balance'] or 0)+reward, float(user2['total_earned'] or 0)+reward, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
    cur.execute("INSERT INTO game_sessions (user_id, game_key, bet_amount, result, reward_amount, payload, created_at) VALUES (?,?,?,?,?,?,?)", (user_id, 'mine', bet, 'win' if win else 'lose', reward, json.dumps({'source':'web'}), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit(); conn.close()
    fresh = ws_get_user(user_id)
    return jsonify({'ok': True, 'win': win, 'bet': bet, 'reward': reward, 'balance': float(fresh['balance'] or 0)})
