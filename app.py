import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def health():
    return "CommCare Cloud Run service is alive"

@app.route("/run")
def run():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
