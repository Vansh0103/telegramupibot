import os
import logging
from anticheat import create_verification_app

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


import sqlite3
import random
from flask import request, jsonify, render_template_string

def _ws_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def _ws_setting(key, default=None):
    conn = _ws_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return default
    try:
        import json as _json
        return _json.loads(row[0])
    except Exception:
        return row[0]

@app.route('/games/mines')
def mines_page():
    uid = request.args.get('uid', '0')
    title = _ws_setting('game_webapp_title', 'Games')
    return render_template_string("""
    <!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>Mines Game</title><style>body{font-family:Arial;background:#0f172a;color:#fff;padding:16px}button,input{padding:10px;border-radius:10px;border:none;margin:6px 0} .card{max-width:420px;margin:auto;background:#1e293b;padding:18px;border-radius:16px}</style></head>
    <body><div class='card'><h2>💣 {{title}}</h2><p>Mines Game</p><p>User: {{uid}}</p>
    <input id='bet' type='number' placeholder='Bet amount'/><br>
    <button onclick='play()'>Play Mines</button><pre id='out'></pre>
    <script>
    async function play(){
      const bet=document.getElementById('bet').value;
      const res=await fetch('/games/api/mines/play',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:'{{uid}}',bet:bet})});
      document.getElementById('out').textContent=JSON.stringify(await res.json(),null,2);
    }
    </script></div></body></html>""", uid=uid, title=title)

@app.route('/games/api/mines/play', methods=['POST'])
def play_mines_api():
    data = request.get_json(silent=True) or {}
    uid = int(data.get('uid') or 0)
    bet = float(data.get('bet') or 0)
    min_bet = float(_ws_setting('game_mines_min_bet', 1.0) or 1.0)
    max_bet = float(_ws_setting('game_mines_max_bet', 25.0) or 25.0)
    ratio = float(_ws_setting('game_mines_win_ratio', 1.8) or 1.8)
    enabled = bool(_ws_setting('game_mines_enabled', True))
    if not enabled:
        return jsonify({'ok': False, 'message': 'Game disabled by admin'}), 400
    if uid <= 0 or bet < min_bet or bet > max_bet:
        return jsonify({'ok': False, 'message': 'Invalid bet'}), 400
    conn = _ws_db(); cur = conn.cursor()
    cur.execute('SELECT balance, game_balance, bonus_balance FROM users WHERE user_id=?', (uid,))
    user = cur.fetchone()
    if not user or float(user['balance'] or 0) < bet:
        conn.close(); return jsonify({'ok': False, 'message': 'Insufficient balance'}), 400
    # debit from bonus balance first to avoid UI changes
    cur.execute('UPDATE users SET bonus_balance=MAX(0, COALESCE(bonus_balance,0)-?) WHERE user_id=?', (min(float(user['bonus_balance'] or 0), bet), uid))
    win = random.random() < 0.5
    reward = round(bet * ratio, 2) if win else 0.0
    if win:
        cur.execute('UPDATE users SET game_balance=COALESCE(game_balance,0)+?, total_earned=COALESCE(total_earned,0)+? WHERE user_id=?', (reward, reward, uid))
    cur.execute("INSERT INTO game_results (user_id, game_name, bet_amount, reward_amount, result, meta, created_at) VALUES (?,?,?,?,?,?,datetime('now'))", (uid, 'mines', bet, reward, 'win' if win else 'lose', '{}'))
    cur.execute('UPDATE users SET balance=COALESCE(bonus_balance,0)+COALESCE(referral_balance,0)+COALESCE(task_balance,0)+COALESCE(game_balance,0) WHERE user_id=?', (uid,))
    conn.commit(); conn.close()
    return jsonify({'ok': True, 'result': 'win' if win else 'lose', 'reward': reward, 'bet': bet})
