import os
import logging
from anticheat import create_verification_app
from flask import render_template, request, jsonify
import sqlite3, json, random

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


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/games")
def games_home():
    return render_template("mine.html")

@app.route("/games/mine")
def mine_page():
    return render_template("mine.html")

@app.route("/api/games/history")
def game_history_api():
    uid = int(request.args.get("uid", "0") or 0)
    conn = get_db_conn(); cur = conn.cursor()
    cur.execute("SELECT created_at, result, bet_amount, net_change FROM game_results WHERE user_id=? ORDER BY id DESC LIMIT 20", (uid,))
    items = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"items": items})

@app.route("/api/games/mines/play", methods=["POST"])
def mines_play():
    data = request.get_json(force=True, silent=True) or {}
    uid = int(data.get("uid", 0) or 0)
    bet = round(float(data.get("bet", 0) or 0), 2)
    if uid <= 0:
        return jsonify({"ok": False, "error": "Invalid user"}), 400
    conn = get_db_conn(); cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key='games_config'")
    row = cur.fetchone()
    cfg = {"mines_enabled": True, "mines_min_bet": 1, "mines_max_bet": 25, "mines_win_ratio": 45, "mines_reward_multiplier": 1.8}
    if row and row[0]:
        try:
            cfg.update(json.loads(row[0]))
        except Exception:
            pass
    if not cfg.get("mines_enabled", True):
        conn.close(); return jsonify({"ok": False, "error": "Game disabled by admin"}), 403
    if bet < float(cfg.get("mines_min_bet", 1)) or bet > float(cfg.get("mines_max_bet", 25)):
        conn.close(); return jsonify({"ok": False, "error": "Bet outside allowed limits"}), 400
    cur.execute("SELECT balance FROM users WHERE user_id=?", (uid,))
    user = cur.fetchone()
    if not user or float(user[0] or 0) < bet:
        conn.close(); return jsonify({"ok": False, "error": "Insufficient balance"}), 400
    cur.execute("UPDATE users SET balance=balance-?, last_activity_at=datetime('now','localtime') WHERE user_id=?", (bet, uid))
    cur.execute("INSERT INTO game_results (user_id, game_key, bet_amount, reward_amount, net_change, result, metadata, created_at) VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))", (uid, 'mines', bet, 0, -bet, 'pending', json.dumps({"state":"started"})))
    game_id = cur.lastrowid
    conn.commit(); conn.close()
    return jsonify({"ok": True, "game_id": game_id, "message": "Round started. Pick a tile."})

@app.route("/api/games/mines/reveal", methods=["POST"])
def mines_reveal():
    data = request.get_json(force=True, silent=True) or {}
    uid = int(data.get("uid", 0) or 0)
    idx = int(data.get("index", 0) or 0)
    conn = get_db_conn(); cur = conn.cursor()
    cur.execute("SELECT id, bet_amount, result FROM game_results WHERE user_id=? AND game_key='mines' ORDER BY id DESC LIMIT 1", (uid,))
    row = cur.fetchone()
    if not row or row[2] != 'pending':
        conn.close(); return jsonify({"ok": False, "error": "No active round"}), 400
    cur.execute("SELECT value FROM settings WHERE key='games_config'")
    cfg_row = cur.fetchone(); cfg = {"mines_win_ratio": 45, "mines_reward_multiplier": 1.8}
    if cfg_row and cfg_row[0]:
        try: cfg.update(json.loads(cfg_row[0]))
        except Exception: pass
    win = random.randint(1,100) <= int(cfg.get("mines_win_ratio",45))
    bet = float(row[1] or 0)
    reward = round(bet * float(cfg.get("mines_reward_multiplier", 1.8)), 2) if win else 0
    net = round(reward if win else -bet, 2)
    result = 'win' if win else 'loss'
    cur.execute("UPDATE game_results SET reward_amount=?, net_change=?, result=?, metadata=? WHERE id=?", (reward, net, result, json.dumps({"pick":idx,"win":win}), row[0]))
    if win:
        cur.execute("UPDATE users SET balance=balance+?, total_earned=total_earned+?, last_activity_at=datetime('now','localtime') WHERE user_id=?", (reward, reward, uid))
    conn.commit(); conn.close()
    return jsonify({"ok": True, "finished": True, "result": result, "message": f"You {'won' if win else 'lost'}! {'Reward ₹'+str(reward) if win else 'Better luck next round.'}"})


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
