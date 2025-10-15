from flask import Flask, jsonify, request
import os

from build_daily_performance import build_daily_performance  # ← همین فایل تو

app = Flask(__name__)

def _authorized():
    expected = os.getenv("RUN_TOKEN")
    if not expected:
        return True
    # هدر یا querystring
    token = request.headers.get("X-Run-Token") or request.args.get("token")
    return token == expected

@app.get("/")
def health():
    return jsonify(status="ok", service="daily_performance")

@app.route("/run-daily-performance", methods=["GET","POST"])
def run_daily():
    if not _authorized():
        return jsonify(ok=False, error="unauthorized"), 401
    try:
        build_daily_performance()
        return jsonify(ok=True, message="Daily_Performance built.")
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
