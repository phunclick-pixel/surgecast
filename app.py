import os
import re

import requests
from dotenv import load_dotenv
from flask import (Flask, jsonify, redirect, render_template,
                   request, session, url_for)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change-me-in-production")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "surgecast-admin")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_valid_email(email):
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def get_city_threshold(city, state):
    """
    Returns an alert threshold based on city size.
    Uses Nominatim's importance score (0–1) as a proxy for population.
    Falls back to 70 if the lookup fails.
    """
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{city}, {state}, US", "format": "json", "limit": 1},
            headers={"User-Agent": "Surgecast/1.0"},
            timeout=10,
        )
        results = resp.json()
        if results:
            importance = float(results[0].get("importance", 0.45))
            if importance >= 0.70:
                return 85   # major city  (Charlotte, Atlanta, Nashville)
            elif importance >= 0.55:
                return 70   # large city  (Asheville, Boulder, Santa Fe)
            elif importance >= 0.40:
                return 55   # small city  (Hendersonville, Brevard)
            else:
                return 35   # small town  (Freeport, ME; Black Mountain)
    except Exception:
        pass
    return 70  # safe default


def admin_required(f):
    """Decorator that redirects to login if not authenticated."""
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Public routes
# ---------------------------------------------------------------------------

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

    threshold = get_city_threshold(city, state)
    print(f"Signup: {email} / {city}, {state} → threshold {threshold}")

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates"},
        json={"email": email, "city": city, "state": state,
              "active": True, "alert_threshold": threshold},
    )

    if resp.status_code in (200, 201):
        return jsonify({"success": True})
    elif resp.status_code == 409:
        return jsonify({"error": "This email is already subscribed."}), 409
    else:
        return jsonify({"error": "Something went wrong. Please try again."}), 500


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == ADMIN_PASSWORD:
            session["admin"] = True
            return redirect(url_for("admin_dashboard"))
        error = "Wrong password."
    return render_template("admin_login.html", error=error)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.route("/admin")
@admin_required
def admin_dashboard():
    subs = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"select": "id,email,city,state,alert_threshold,active,created_at",
                "order": "created_at.desc", "limit": "500"},
    ).json()

    # City breakdown
    cities = {}
    for s in subs:
        key = f"{s['city']}, {s['state']}"
        cities[key] = cities.get(key, 0) + 1

    return render_template("admin.html", subscribers=subs, cities=cities)


@app.route("/admin/toggle/<sub_id>", methods=["POST"])
@admin_required
def admin_toggle(sub_id):
    # Get current state
    rows = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"id": f"eq.{sub_id}", "select": "active"},
    ).json()
    if not rows:
        return jsonify({"error": "Not found"}), 404

    new_state = not rows[0]["active"]
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"id": f"eq.{sub_id}"},
        json={"active": new_state},
    )
    return jsonify({"active": new_state})


@app.route("/admin/threshold/<sub_id>", methods=["POST"])
@admin_required
def admin_threshold(sub_id):
    try:
        value = int(request.get_json().get("threshold", 70))
        value = max(0, min(100, value))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid value"}), 400

    requests.patch(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"id": f"eq.{sub_id}"},
        json={"alert_threshold": value},
    )
    return jsonify({"threshold": value})


@app.route("/admin/delete/<sub_id>", methods=["POST"])
@admin_required
def admin_delete(sub_id):
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"id": f"eq.{sub_id}"},
    )
    return jsonify({"deleted": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
