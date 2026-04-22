import os
import random
import re

import requests
import resend
from dotenv import load_dotenv
from flask import (Flask, jsonify, redirect, render_template,
                   request, session, url_for)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change-me-in-production")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "surgecast-admin")
ALERT_FROM = "Surgecast <onboarding@resend.dev>"

PLAN_LIMITS = {
    "starter": {"max_cities": 1,  "label": "Starter",  "price": "$29/mo"},
    "growth":  {"max_cities": 3,  "label": "Growth",   "price": "$79/mo"},
    "pro":     {"max_cities": 10, "label": "Pro",       "price": "$149/mo"},
}

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
                return 85
            elif importance >= 0.55:
                return 70
            elif importance >= 0.40:
                return 55
            else:
                return 35
    except Exception:
        pass
    return 70


def admin_required(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


def customer_required(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("customer_email"):
            return redirect(url_for("dashboard_login"))
        return f(*args, **kwargs)
    return wrapper


def get_subscriber_by_email(email):
    rows = requests.get(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={
            "select": "id,email,plan,active,subscriber_cities(id,city,state,alert_threshold)",
            "email": f"eq.{email}",
            "limit": "1",
        },
    ).json()
    return rows[0] if rows else None


def send_login_code(email, code):
    resend.api_key = os.environ["RESEND_API_KEY"]
    resend.Emails.send({
        "from": ALERT_FROM,
        "to": [email],
        "subject": "Your Surgecast login code",
        "html": f"""
        <html><body style='font-family:monospace;padding:2rem;'>
        <h2 style='color:#6366f1;'>Surgecast</h2>
        <p>Your login code is:</p>
        <h1 style='letter-spacing:8px;color:#fff;background:#0f0f24;
                   padding:1rem 2rem;border-radius:8px;display:inline-block;'>{code}</h1>
        <p style='color:#888;margin-top:1rem;'>Expires in 15 minutes. Do not share this code.</p>
        </body></html>
        """,
    })


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

    # Create subscriber
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers={**SB_HEADERS, "Prefer": "return=representation,resolution=ignore-duplicates"},
        json={"email": email, "active": True, "plan": "starter"},
    )

    if resp.status_code not in (200, 201):
        return jsonify({"error": "Something went wrong. Please try again."}), 500

    sub = resp.json()
    sub_id = sub[0]["id"] if isinstance(sub, list) and sub else None

    # Add city to subscriber_cities if we got a subscriber id
    if sub_id:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/subscriber_cities",
            headers={**SB_HEADERS, "Prefer": "resolution=ignore-duplicates"},
            json={"subscriber_id": sub_id, "city": city,
                  "state": state, "alert_threshold": threshold},
        )

    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Customer dashboard
# ---------------------------------------------------------------------------

@app.route("/dashboard/login", methods=["GET", "POST"])
def dashboard_login():
    if request.method == "GET":
        return render_template("dashboard_login.html")

    email = (request.form.get("email") or "").strip().lower()
    if not email or not is_valid_email(email):
        return render_template("dashboard_login.html", error="Please enter a valid email.")

    sub = get_subscriber_by_email(email)
    if not sub:
        # Don't reveal whether email exists
        return render_template("dashboard_login.html",
                               sent=True, email=email)

    code = str(random.randint(100000, 999999))
    expires = (
        requests.utils.default_headers()  # just used for the import side-effect
    )
    import datetime
    expires_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=15)).isoformat()

    requests.post(
        f"{SUPABASE_URL}/rest/v1/login_tokens",
        headers=SB_HEADERS,
        json={"email": email, "code": code, "expires_at": expires_at},
    )

    try:
        send_login_code(email, code)
    except Exception as e:
        print(f"Login email error: {e}")

    return render_template("dashboard_login.html", sent=True, email=email)


@app.route("/dashboard/verify", methods=["POST"])
def dashboard_verify():
    email = (request.form.get("email") or "").strip().lower()
    code  = (request.form.get("code")  or "").strip()
    import datetime
    now = datetime.datetime.utcnow().isoformat()

    tokens = requests.get(
        f"{SUPABASE_URL}/rest/v1/login_tokens",
        headers=SB_HEADERS,
        params={
            "email": f"eq.{email}",
            "code": f"eq.{code}",
            "used": "eq.false",
            "expires_at": f"gt.{now}",
            "order": "created_at.desc",
            "limit": "1",
        },
    ).json()

    if not tokens:
        return render_template("dashboard_login.html",
                               sent=True, email=email,
                               error="Invalid or expired code. Try again.")

    # Mark token used
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/login_tokens",
        headers=SB_HEADERS,
        params={"id": f"eq.{tokens[0]['id']}"},
        json={"used": True},
    )

    session["customer_email"] = email
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@customer_required
def dashboard():
    email = session["customer_email"]
    sub = get_subscriber_by_email(email)
    if not sub:
        session.clear()
        return redirect(url_for("dashboard_login"))

    plan = sub.get("plan", "starter")
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])
    cities = sub.get("subscriber_cities", [])

    # Fetch upcoming high-impact events for all their cities
    import datetime
    today = datetime.date.today().isoformat()
    all_events = []
    for c in cities:
        events = requests.get(
            f"{SUPABASE_URL}/rest/v1/events",
            headers=SB_HEADERS,
            params={
                "select": "title,venue_name,start_date,impact_score,source",
                "city": f"eq.{c['city']}",
                "start_date": f"gte.{today}",
                "impact_score": f"gt.{c.get('alert_threshold', 70)}",
                "order": "start_date.asc",
                "limit": "5",
            },
        ).json()
        for e in (events if isinstance(events, list) else []):
            e["city"] = c["city"]
            all_events.append(e)

    all_events.sort(key=lambda x: x.get("start_date", ""))

    return render_template("dashboard.html",
                           sub=sub, plan=plan, limits=limits,
                           cities=cities, events=all_events,
                           plan_info=PLAN_LIMITS)


@app.route("/dashboard/add-city", methods=["POST"])
@customer_required
def dashboard_add_city():
    email = session["customer_email"]
    sub = get_subscriber_by_email(email)
    if not sub:
        return jsonify({"error": "Account not found."}), 404

    plan = sub.get("plan", "starter")
    max_cities = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])["max_cities"]
    current = len(sub.get("subscriber_cities", []))

    if current >= max_cities:
        return jsonify({
            "error": f"Your {plan.title()} plan supports up to {max_cities} city. "
                     f"Upgrade to add more."
        }), 403

    data  = request.get_json()
    city  = (data.get("city")  or "").strip().title()
    state = (data.get("state") or "").strip().upper()

    if not city or not state:
        return jsonify({"error": "City and state are required."}), 400

    threshold = get_city_threshold(city, state)

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/subscriber_cities",
        headers={**SB_HEADERS, "Prefer": "return=representation"},
        json={"subscriber_id": sub["id"], "city": city,
              "state": state, "alert_threshold": threshold},
    )

    if resp.status_code in (200, 201):
        new_city = resp.json()[0] if isinstance(resp.json(), list) else resp.json()
        return jsonify({"success": True, "city": new_city})
    return jsonify({"error": "Could not add city."}), 500


@app.route("/dashboard/remove-city/<city_id>", methods=["POST"])
@customer_required
def dashboard_remove_city(city_id):
    email = session["customer_email"]
    sub = get_subscriber_by_email(email)
    if not sub:
        return jsonify({"error": "Account not found."}), 404

    # Verify this city belongs to this subscriber
    owned = [c for c in sub.get("subscriber_cities", []) if str(c["id"]) == city_id]
    if not owned:
        return jsonify({"error": "Not found."}), 404

    requests.delete(
        f"{SUPABASE_URL}/rest/v1/subscriber_cities",
        headers=SB_HEADERS,
        params={"id": f"eq.{city_id}"},
    )
    return jsonify({"deleted": True})


@app.route("/dashboard/logout")
def dashboard_logout():
    session.pop("customer_email", None)
    return redirect(url_for("dashboard_login"))


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
        params={
            "select": "id,email,plan,active,created_at,subscriber_cities(city,state)",
            "order": "created_at.desc",
            "limit": "500",
        },
    ).json()

    cities = {}
    for s in subs:
        for c in s.get("subscriber_cities", []):
            key = f"{c['city']}, {c['state']}"
            cities[key] = cities.get(key, 0) + 1

    return render_template("admin.html", subscribers=subs,
                           cities=cities, plan_info=PLAN_LIMITS)


@app.route("/admin/toggle/<sub_id>", methods=["POST"])
@admin_required
def admin_toggle(sub_id):
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


@app.route("/admin/set-plan/<sub_id>", methods=["POST"])
@admin_required
def admin_set_plan(sub_id):
    plan = (request.get_json().get("plan") or "starter").lower()
    if plan not in PLAN_LIMITS:
        return jsonify({"error": "Invalid plan"}), 400
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/subscribers",
        headers=SB_HEADERS,
        params={"id": f"eq.{sub_id}"},
        json={"plan": plan},
    )
    return jsonify({"plan": plan})


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
