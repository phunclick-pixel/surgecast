import os
import re

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()

app = Flask(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]


def is_valid_email(email):
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/subscribe", methods=["POST"])
def subscribe():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    city  = (data.get("city")  or "").strip().title()
    state = (data.get("state") or "").strip().upper()

    if not email or not is_valid_email(email):
        return jsonify({"error": "Please enter a valid email address."}), 400
    if not city:
        return jsonify({"error": "Please enter your city."}), 400
    if not state or len(state) != 2:
        return jsonify({"error": "Please select your state."}), 400

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=ignore-duplicates",
        },
        json={"email": email, "city": city, "state": state, "active": True},
    )

    if resp.status_code in (200, 201):
        return jsonify({"success": True})
    elif resp.status_code == 409:
        return jsonify({"error": "This email is already subscribed."}), 409
    else:
        return jsonify({"error": "Something went wrong. Please try again."}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
