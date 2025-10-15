from flask import Flask, jsonify, request
import os
from buildDailyPerformance import build_daily_performance  # ← توجه: با اسم فایل تو دقیقا همین باشه

app = Flask(__name__)

def _authorized():
    expected = os.getenv("RUN_TOKEN")
    if not expected:
        return True
    token = request.headers.get("X-Run-Token") or request.args.get("token")
    return token == expected

@app.get("/")
def home():
    return jsonify(status="ok", service="daily_performance")

@app.route("/run-daily-performance", methods=["GET", "POST"])
def run_daily():
    if not _authorized():
        return jsonify(ok=False, error="unauthorized"), 401
    try:
        build_daily_performance()
        return jsonify(ok=True, message="Daily_Performance built successfully.")
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
