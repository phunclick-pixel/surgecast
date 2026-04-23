import datetime
import hashlib
import hmac as _hmac
import os
import re
import time

import resend
import requests
import schedule
from dotenv import load_dotenv

load_dotenv()

TICKETMASTER_KEY = os.environ["TICKETMASTER_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
PREDICTHQ_KEY = os.environ["PREDICTHQ_KEY"]

VENUE_SCORE_OVERRIDES = {
    "asheville yards": 85,
    "mccormick field": 70,
    "the orange peel": 55,
}


def calculate_score(title, category, venue_name):
    venue_lower = (venue_name or "").lower()

    if venue_lower in VENUE_SCORE_OVERRIDES:
        return VENUE_SCORE_OVERRIDES[venue_lower]

    score = 0
    title_lower = (title or "").lower()
    category_lower = (category or "").lower()

    # Category scoring
    if any(x in category_lower for x in ["music", "concert", "sports"]):
        score += 40
    elif any(x in category_lower for x in ["arts", "theatre", "comedy"]):
        score += 25
    elif any(x in category_lower for x in ["miscellaneous", "family"]):
        score += 15
    elif "city_permit" in category_lower:
        score += 10
        if any(x in title_lower for x in ["festival", "parade", "marathon", "race", "5k", "10k", "half marathon"]):
            score += 15
        if any(x in title_lower for x in ["concert", "music", "performance", "show"]):
            score += 10
        if any(x in title_lower for x in ["market", "fair", "carnival", "block party"]):
            score += 8

    # Venue size scoring
    if any(x in venue_lower for x in ["stadium", "arena", "coliseum"]):
        score += 40
    elif any(x in venue_lower for x in ["amphitheatre", "amphitheater", "center"]):
        score += 30
    elif any(x in venue_lower for x in ["theater", "theatre", "hall"]):
        score += 20
    elif any(x in venue_lower for x in ["club", "lounge", "bar", "room"]):
        score += 10

    # Title keyword boosts
    if any(x in title_lower for x in ["sold out", "championship", "festival", "playoff"]):
        score += 15
    if any(x in title_lower for x in ["marathon", "parade", "graduation", "commencement"]):
        score += 12
    if any(x in title_lower for x in ["tour", "live", "concert"]):
        score += 5

    return min(score, 100)


def save_event(event):
    url = f"{SUPABASE_URL}/rest/v1/events"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates",
    }
    response = requests.post(url, headers=headers, json=event)
    return response.status_code


# ---------------------------------------------------------------------------
# Plan limits
# ---------------------------------------------------------------------------

PLAN_LIMITS = {
    "starter": {"max_cities": 1,  "alerts_per_day": 1},
    "growth":  {"max_cities": 3,  "alerts_per_day": 1},
    "pro":     {"max_cities": 10, "alerts_per_day": 2},
}

# Alert email thresholds (adjustable)
ALERT_THRESHOLD_HIGH = 70   # At or above → HIGH PRIORITY section
ALERT_THRESHOLD_LOW  = 40   # Below this  → silently skipped


# ---------------------------------------------------------------------------
# Subscribers
# ---------------------------------------------------------------------------

def _trial_expired(sub):
    """Return True if this subscriber's trial ended and they never upgraded."""
    trial_ends_at = sub.get("trial_ends_at")
    plan = sub.get("plan", "starter")
    if not trial_ends_at:
        return False  # No trial set — treat as active (legacy / admin-created)
    try:
        ends = datetime.date.fromisoformat(trial_ends_at[:10])
        return ends < datetime.date.today() and plan == "starter"
    except (ValueError, TypeError):
        return False


def get_subscribers(pro_only=False):
    """Return active subscribers with their cities from subscriber_cities.

    Excludes subscribers whose free trial has expired and who have not upgraded.
    """
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {
        "select": "id,email,plan,trial_ends_at,subscriber_cities(id,city,state,alert_threshold)",
        "active": "eq.true",
        "limit": "500",
    }
    if pro_only:
        params["plan"] = "eq.pro"

    resp = requests.get(f"{SUPABASE_URL}/rest/v1/subscribers", headers=headers, params=params)
    rows = resp.json()
    if not isinstance(rows, list):
        print(f"subscribers table error ({resp.status_code}): {rows}")
        return []

    valid = []
    for s in rows:
        if not s.get("subscriber_cities"):
            continue
        if _trial_expired(s):
            print(f"  Skipping {s['email']} — trial expired, no active plan")
            continue
        valid.append(s)
    return valid


# ---------------------------------------------------------------------------
# Geocoding (used by PredictHQ to build the radius search)
# ---------------------------------------------------------------------------

_geocode_cache = {}


def geocode_city(city, state):
    """Return (lat, lon, importance) for a city via Nominatim. Cached per run."""
    key = (city.lower(), state.upper())
    if key in _geocode_cache:
        return _geocode_cache[key]
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{city}, {state}, US", "format": "json", "limit": 1},
            headers={"User-Agent": "Surgecast/1.0"},
            timeout=10,
        )
        results = resp.json()
        if results:
            lat        = float(results[0]["lat"])
            lon        = float(results[0]["lon"])
            importance = float(results[0].get("importance", 0.45))
            _geocode_cache[key] = (lat, lon, importance)
            return lat, lon, importance
    except Exception as e:
        print(f"Geocode error for {city}, {state}: {e}")
    _geocode_cache[key] = (None, None, 0.45)
    return None, None, 0.45


def _search_radius(importance):
    """Return PredictHQ search radius in miles based on city size."""
    if importance >= 0.70:
        return 30   # Major city  (Nashville, Atlanta …)
    elif importance >= 0.55:
        return 20   # Mid-size    (Asheville, Savannah …)
    elif importance >= 0.40:
        return 15   # Small city  (Freeport ME, Staunton VA …)
    else:
        return 10   # Very small town


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def get_existing_keys(city):
    """Return a set of (title_lower, start_date) for every event in the DB for this city."""
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    rows = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=headers,
        params={"select": "title,start_date", "city": f"eq.{city}", "limit": "2000"},
    ).json()
    return {
        (r["title"].lower().strip(), r["start_date"])
        for r in rows
        if r.get("title") and r.get("start_date")
    }


def is_duplicate(title, start_date, existing_keys):
    return (title.lower().strip(), start_date) in existing_keys


# ---------------------------------------------------------------------------
# Ticketmaster
# ---------------------------------------------------------------------------

def scrape_ticketmaster(city, state):
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": TICKETMASTER_KEY,
        "city": city,
        "stateCode": state,
        "countryCode": "US",
        "size": 20,
        "sort": "date,asc",
    }
    response = requests.get(url, params=params)
    data = response.json()
    events = data.get("_embedded", {}).get("events", [])
    print(f"Found {len(events)} Ticketmaster events in {city}")

    saved = 0
    for e in events:
        venue = e.get("_embedded", {}).get("venues", [{}])[0]
        venue_name = venue.get("name")
        title = e["name"]
        category = e.get("classifications", [{}])[0].get("segment", {}).get("name")
        score = calculate_score(title, category, venue_name)

        event = {
            "source": "ticketmaster",
            "external_id": e["id"],
            "title": title,
            "venue_name": venue_name,
            "city": city,
            "start_date": e["dates"]["start"].get("localDate"),
            "category": category,
            "impact_score": score,
        }

        status = save_event(event)
        if status in [200, 201]:
            saved += 1
            print(f"  [{score:3d}] {title}")
        else:
            print(f"  [skip] {title}")

    print(f"Ticketmaster: {saved} new event(s) added\n")


# ---------------------------------------------------------------------------
# PredictHQ
# NOTE: run this SQL once in Supabase before using this source:
#   ALTER TABLE events ADD COLUMN IF NOT EXISTS phq_attendance integer;
# ---------------------------------------------------------------------------

def scrape_predicthq(city, existing_keys, lat, lon, radius=30):
    """Pulls upcoming events within `radius` miles of (lat, lon) from PredictHQ."""
    url = "https://api.predicthq.com/v1/events/"
    headers = {
        "Authorization": f"Bearer {PREDICTHQ_KEY}",
        "Accept": "application/json",
    }
    params = {
        "within": f"{radius}mi@{lat},{lon}",
        "active.gte": datetime.date.today().isoformat(),
        "limit": 200,
    }

    print(f"-- PredictHQ ({radius}-mile radius) --")
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"PredictHQ error: {e}")
        return 0

    results = data.get("results", [])
    print(f"Found {len(results)} events near {city} ({radius}-mile radius)")

    saved = 0
    for e in results:
        title = (e.get("title") or "").strip()
        start_date = (e.get("start") or "")[:10]
        if not title or not start_date:
            continue

        if is_duplicate(title, start_date, existing_keys):
            continue

        venue_name = None
        for entity in e.get("entities", []):
            if entity.get("type") == "venue":
                venue_name = entity.get("name")
                break

        category = e.get("category", "")
        phq_attendance = e.get("phq_attendance")
        score = calculate_score(title, category, venue_name)

        event = {
            "source": "predicthq",
            "external_id": e.get("id"),
            "title": title,
            "venue_name": venue_name,
            "city": city,
            "start_date": start_date,
            "category": category,
            "impact_score": score,
            "phq_attendance": phq_attendance,
        }

        status = save_event(event)
        if status in [200, 201]:
            saved += 1
            existing_keys.add((title.lower().strip(), start_date))
            att_str = f"{phq_attendance:,}" if phq_attendance else "N/A"
            print(f"  [{score:3d}] {title[:50]:<50}  att: {att_str}")

    print(f"PredictHQ: {saved} new event(s) added\n")
    return saved


# ---------------------------------------------------------------------------
# Asheville City Permits  (SimpliCity GraphQL API — Asheville-only)
# ---------------------------------------------------------------------------

_DATE_PATTERNS = [
    (r"\b([A-Za-z]+ \d{1,2},? \d{4})\b", "%B %d, %Y"),  # April 15, 2026
    (r"\b(\d{1,2}/\d{1,2}/\d{4})\b",      "%m/%d/%Y"),   # 04/15/2026
    (r"\b(\d{4}-\d{2}-\d{2})\b",          "%Y-%m-%d"),   # 2026-04-15
]

_PERMITS_QUERY = """
query getPermitsQuery {
  permits(
    date_field: "applied_date",
    after: "%s",
    before: "%s"
  ) {
    permit_number
    permit_type
    permit_description
    application_name
    address
    applied_date
    status_date
  }
}
"""


def _parse_permit_date(text):
    """Return the first future date found in text as 'YYYY-MM-DD', or None."""
    today = datetime.date.today()
    text_clean = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text)
    for pattern, fmt in _DATE_PATTERNS:
        for m in re.finditer(pattern, text_clean, re.IGNORECASE):
            raw = m.group(1).strip().rstrip(",")
            try:
                d = datetime.datetime.strptime(raw, fmt).date()
                if d >= today:
                    return d.isoformat()
            except ValueError:
                continue
    return None


def scrape_city_permits(existing_keys):
    """Pulls Event-Temporary Use permits from Asheville's SimpliCity GraphQL API."""
    print("── Asheville City Permits ─────────────────────────────")

    today = datetime.date.today()
    after  = (today - datetime.timedelta(days=180)).isoformat()
    before = (today + datetime.timedelta(days=30)).isoformat()

    try:
        resp = requests.post(
            "https://data-api1.ashevillenc.gov/graphql",
            headers={
                "Content-Type": "application/json",
                "x-apollo-operation-name": "getPermitsQuery",
            },
            json={
                "operationName": "getPermitsQuery",
                "query": _PERMITS_QUERY % (after, before),
            },
            timeout=15,
        )
        resp.raise_for_status()
        permits = resp.json().get("data", {}).get("permits", [])
    except Exception as e:
        print(f"City permits API error: {e}")
        return 0

    event_permits = [
        p for p in permits
        if "event" in (p.get("permit_type") or "").lower()
        or "temporary use" in (p.get("permit_type") or "").lower()
    ]
    print(f"Found {len(event_permits)} event permit(s) from SimpliCity")

    saved = 0
    seen_in_run = set()

    for p in event_permits:
        title = (p.get("permit_description") or p.get("application_name") or "").strip()
        if not title:
            title = f"Event Permit {p.get('permit_number', '')}".strip()
        title = title[:80]

        start_date = _parse_permit_date(title)

        if not start_date:
            for field in ("status_date", "applied_date"):
                raw = (p.get(field) or "")[:10]
                if raw:
                    try:
                        d = datetime.date.fromisoformat(raw)
                        if d >= today:
                            start_date = d.isoformat()
                            break
                    except ValueError:
                        continue

        if not start_date:
            continue

        venue_name = (p.get("address") or "City of Asheville").strip()

        dedup_key = (title.lower().strip(), start_date)
        if dedup_key in seen_in_run or is_duplicate(title, start_date, existing_keys):
            continue
        seen_in_run.add(dedup_key)

        ext_id = hashlib.md5(
            f"citypermit:{p.get('permit_number', title)}:{start_date}".encode()
        ).hexdigest()[:16]
        score = calculate_score(title, "city_permit", venue_name)

        event = {
            "source": "city_permits",
            "external_id": ext_id,
            "title": title,
            "venue_name": venue_name,
            "city": "Asheville",
            "start_date": start_date,
            "category": "city_permit",
            "impact_score": score,
        }

        status = save_event(event)
        if status in [200, 201]:
            saved += 1
            existing_keys.add(dedup_key)
            print(f"  [{score:3d}] {title}")

    print(f"City permits: {saved} new event(s) added\n")
    return saved


# ---------------------------------------------------------------------------
# UNCW  (Wilmington, NC)
# Sources: general events JSON feed + sports iCal
# ---------------------------------------------------------------------------

def _score_uncw_event(title, is_sports=False):
    t = title.lower()
    if any(x in t for x in ["graduation", "commencement"]):
        return 90
    if "homecoming" in t:
        return 85
    if any(x in t for x in ["move-in", "move in", "moving in", "move day"]):
        return 80
    if any(x in t for x in ["football", "basketball"]):
        return 65
    if is_sports:
        return 50   # Any other sports event
    return 35       # General campus event


def _scrape_uncw_general(existing_keys):
    """Pull UNCW general university events from the JSON feed."""
    url = "https://www.uncw.edu/events/_data/current.json"
    today = datetime.date.today().isoformat()
    try:
        resp = requests.get(url, timeout=15,
                            headers={"User-Agent": "Surgecast/1.0"})
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        print(f"UNCW general events error: {e}")
        return 0

    items = raw if isinstance(raw, list) else raw.get("events", [])
    saved = 0
    for item in items:
        title = (item.get("title") or "").strip()
        start_raw = (item.get("startDate") or "")[:10]
        if not title or not start_raw or start_raw < today:
            continue
        if is_duplicate(title, start_raw, existing_keys):
            continue

        location = ""
        for d in (item.get("additionDetails") or []):
            location = (d.get("text") or "").strip()
            if location:
                break

        score  = _score_uncw_event(title, is_sports=False)
        ext_id = hashlib.md5(f"uncw:{title}:{start_raw}".encode()).hexdigest()[:16]
        event  = {
            "source": "uncw",
            "external_id": ext_id,
            "title": title,
            "venue_name": location or "UNCW Campus",
            "city": "Wilmington",
            "start_date": start_raw,
            "category": "university",
            "impact_score": score,
        }
        status = save_event(event)
        if status in (200, 201):
            saved += 1
            existing_keys.add((title.lower().strip(), start_raw))
            print(f"  [{score:3d}] {title[:60]}")
    return saved


def _scrape_uncw_sports(existing_keys):
    """Pull UNCW sports events from the Sidearm iCal feed."""
    url = "https://uncwsports.com/calendar.ics"
    today = datetime.date.today().isoformat()
    try:
        resp = requests.get(url, timeout=15,
                            headers={"User-Agent": "Surgecast/1.0"})
        resp.raise_for_status()
        text = resp.text
    except Exception as e:
        print(f"UNCW sports iCal error: {e}")
        return 0

    # Unfold continued lines (iCal spec: lines continued with leading space/tab)
    text = re.sub(r"\r?\n[ \t]", "", text)
    saved = 0

    for block in re.split(r"BEGIN:VEVENT", text)[1:]:
        block = block.split("END:VEVENT")[0]

        def field(name):
            m = re.search(rf"^{name}[^:\r\n]*:(.+)", block, re.MULTILINE)
            return m.group(1).strip() if m else ""

        title    = field("SUMMARY")
        dtstart  = field("DTSTART")
        location = field("LOCATION")

        if not title:
            continue

        dm = re.search(r"(\d{8})", dtstart)
        if not dm:
            continue
        ds = dm.group(1)
        start_date = f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"

        if start_date < today:
            continue
        if is_duplicate(title, start_date, existing_keys):
            continue

        score  = _score_uncw_event(title, is_sports=True)
        ext_id = hashlib.md5(
            f"uncw_sports:{title}:{start_date}".encode()
        ).hexdigest()[:16]
        event  = {
            "source": "uncw",
            "external_id": ext_id,
            "title": title,
            "venue_name": location or "UNCW",
            "city": "Wilmington",
            "start_date": start_date,
            "category": "sports",
            "impact_score": score,
        }
        status = save_event(event)
        if status in (200, 201):
            saved += 1
            existing_keys.add((title.lower().strip(), start_date))
            print(f"  [{score:3d}] {title[:60]}")
    return saved


def scrape_uncw(existing_keys):
    print("-- UNCW General Events ---")
    g = _scrape_uncw_general(existing_keys)
    print(f"UNCW general: {g} new event(s)\n")

    print("-- UNCW Sports (iCal) ---")
    s = _scrape_uncw_sports(existing_keys)
    print(f"UNCW sports: {s} new event(s)\n")
    return g + s


# ---------------------------------------------------------------------------
# Dedup + summary
# ---------------------------------------------------------------------------

def remove_duplicates(city):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    rows = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=headers,
        params={"select": "id,title,start_date", "city": f"eq.{city}", "order": "id.asc", "limit": "1000"},
    ).json()

    seen = {}
    to_delete = []
    for r in rows:
        key = (r["title"], r["start_date"])
        if key in seen:
            to_delete.append(r["id"])
        else:
            seen[key] = r["id"]

    if not to_delete:
        print("No duplicates found")
        return

    del_headers = {**headers, "Prefer": "return=representation"}
    for rid in to_delete:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/events",
            headers=del_headers,
            params={"id": f"eq.{rid}"},
        )
    print(f"Removed {len(to_delete)} duplicate(s)")


def print_summary(city):
    today = datetime.date.today().isoformat()
    base_headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

    all_events = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=base_headers,
        params={"select": "impact_score", "city": f"eq.{city}", "limit": "1000"},
    ).json()

    total = len(all_events)
    above_70 = sum(1 for e in all_events if e["impact_score"] > 70)
    above_50 = sum(1 for e in all_events if e["impact_score"] > 50)

    upcoming = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=base_headers,
        params={
            "select": "title,venue_name,start_date,impact_score",
            "city": f"eq.{city}",
            "start_date": f"gte.{today}",
            "impact_score": "gt.50",
            "order": "start_date.asc",
            "limit": "3",
        },
    ).json()

    print("\n" + "=" * 55)
    print(f"  SUMMARY — {city}")
    print("=" * 55)
    print(f"  Total events in database : {total}")
    print(f"  Scoring above 70         : {above_70}")
    print(f"  Scoring above 50         : {above_50}")
    print("\n  Next 3 high-score upcoming events:")
    for e in upcoming:
        print(f"    [{e['impact_score']:3d}] {e['start_date']}  {e['title'][:35]:<35}  @ {e['venue_name']}")
    print("=" * 55 + "\n")


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

ALERT_FROM   = "Surgecast <alerts@surgecast.io>"
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-me-in-production")


def _unsub_link(email):
    token = _hmac.new(
        FLASK_SECRET.encode(),
        email.strip().lower().encode(),
        hashlib.sha256,
    ).hexdigest()[:32]
    return f"https://surgecast.io/unsubscribe?email={email}&token={token}"



def _event_card_html(e):
    score = e["impact_score"]
    try:
        fmt_date = datetime.datetime.strptime(
            e["start_date"], "%Y-%m-%d"
        ).strftime("%B %d, %Y")
    except ValueError:
        fmt_date = e["start_date"]
    bar    = "\u2588" * int(score / 10) + "\u2591" * (10 - int(score / 10))
    venue  = e.get("venue_name") or "Venue TBD"
    return (
        "<div style='background:#0f0f24;border:1px solid #1e1e3a;"
        "border-radius:10px;padding:14px 18px;margin-bottom:10px;'>"
        f"<div style='font-size:15px;font-weight:700;color:#fff;"
        f"margin-bottom:4px;'>{e['title']}</div>"
        f"<div style='font-size:13px;color:#8888a8;'>{fmt_date} &middot; {venue}</div>"
        f"<div style='font-size:12px;color:#6366f1;margin-top:6px;"
        f"font-family:monospace;'>{bar} {score}/100</div>"
        "</div>"
    )


def send_alert_email(high_events, medium_events, city, to_email):
    date_str = datetime.date.today().strftime("%B %d, %Y")

    # Subject line
    if high_events and medium_events:
        subject = (f"Surgecast {city}: {len(high_events)} High-Priority + "
                   f"{len(medium_events)} to Monitor")
    elif high_events:
        subject = f"Surgecast {city}: {len(high_events)} High-Priority Event(s) This Week"
    else:
        subject = f"Surgecast {city}: {len(medium_events)} Event(s) Worth Monitoring"

    # Build sections
    sections_html = ""

    if high_events:
        cards = "".join(_event_card_html(e) for e in high_events)
        sections_html += (
            "<div style='margin-bottom:28px;'>"
            "<div style='color:#f59e0b;font-weight:700;font-size:11px;"
            "text-transform:uppercase;letter-spacing:0.08em;"
            "margin-bottom:6px;'>HIGH PRIORITY</div>"
            "<div style='color:#e2e2f0;font-size:13px;margin-bottom:14px;"
            "border-left:3px solid #f59e0b;padding-left:10px;'>"
            "Action recommended before these dates</div>"
            + cards +
            "</div>"
        )

    if medium_events:
        cards = "".join(_event_card_html(e) for e in medium_events)
        sections_html += (
            "<div style='margin-bottom:28px;'>"
            "<div style='color:#6366f1;font-weight:700;font-size:11px;"
            "text-transform:uppercase;letter-spacing:0.08em;"
            "margin-bottom:6px;'>WORTH MONITORING</div>"
            "<div style='color:#e2e2f0;font-size:13px;margin-bottom:14px;"
            "border-left:3px solid #6366f1;padding-left:10px;'>"
            "Worth monitoring</div>"
            + cards +
            "</div>"
        )

    html_body = f"""
<html>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
             background:#07070f;color:#e2e2f0;padding:32px;max-width:600px;margin:0 auto;">
  <div style="margin-bottom:24px;">
    <div style="font-size:20px;font-weight:800;color:#fff;">
      Surge<span style="color:#6366f1;">cast</span>
    </div>
    <div style="font-size:13px;color:#6666a0;margin-top:4px;">
      {city} Alert &mdash; {date_str}
    </div>
  </div>
  {sections_html}
  <div style="text-align:center;margin:28px 0;">
    <a href="https://surgecast.io/dashboard"
       style="background:#6366f1;color:#fff;padding:12px 28px;
              border-radius:8px;text-decoration:none;font-weight:700;
              font-size:14px;display:inline-block;">
      View your dashboard &rarr;
    </a>
  </div>
  <hr style="border:none;border-top:1px solid #1e1e3a;margin:24px 0;">
  <div style="font-size:11px;color:#444466;text-align:center;">
    You&rsquo;re receiving this because you signed up at surgecast.io &middot;
    <a href="https://surgecast.io/dashboard" style="color:#6366f1;">Manage subscription</a>
    &middot;
    <a href="{_unsub_link(to_email)}" style="color:#444466;">Unsubscribe</a>
  </div>
</body>
</html>"""

    resend.api_key = os.environ["RESEND_API_KEY"]
    resend.Emails.send({
        "from": ALERT_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    })
    print(f"  Alert sent -> {to_email}")


def send_advance_email(events, city, to_email):
    """Weekly heads-up email — HIGH events 8-30 days out."""
    date_str  = datetime.date.today().strftime("%B %d, %Y")
    lookahead = (datetime.date.today() + datetime.timedelta(days=30)).strftime("%B %d")
    subject   = (f"Surgecast {city}: {len(events)} high-impact event(s) "
                 f"coming in the next 30 days")

    cards = "".join(_event_card_html(e) for e in events)

    html_body = f"""
<html>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
             background:#07070f;color:#e2e2f0;padding:32px;max-width:600px;margin:0 auto;">
  <div style="margin-bottom:24px;">
    <div style="font-size:20px;font-weight:800;color:#fff;">
      Surge<span style="color:#6366f1;">cast</span>
    </div>
    <div style="font-size:13px;color:#6666a0;margin-top:4px;">
      {city} &mdash; 30-Day Advance Notice &mdash; {date_str}
    </div>
  </div>

  <div style="background:rgba(99,102,241,0.08);border:1px solid rgba(99,102,241,0.25);
              border-radius:10px;padding:14px 18px;margin-bottom:24px;">
    <div style="font-size:13px;color:#a5b4fc;">
      <strong>Plan ahead.</strong> These high-impact events are coming before {lookahead}.
      Stock up, staff up, and get ahead of your competitors.
    </div>
  </div>

  <div style="margin-bottom:8px;">
    <div style="color:#f59e0b;font-weight:700;font-size:11px;
                text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px;">
      HIGH PRIORITY &mdash; {len(events)} upcoming event(s)
    </div>
    {cards}
  </div>

  <div style="text-align:center;margin:28px 0;">
    <a href="https://surgecast.io/dashboard"
       style="background:#6366f1;color:#fff;padding:12px 28px;
              border-radius:8px;text-decoration:none;font-weight:700;
              font-size:14px;display:inline-block;">
      View your dashboard &rarr;
    </a>
  </div>
  <hr style="border:none;border-top:1px solid #1e1e3a;margin:24px 0;">
  <div style="font-size:11px;color:#444466;text-align:center;">
    Weekly advance notice from surgecast.io &middot;
    You&rsquo;ll also get a 7-day reminder as each event approaches &middot;
    <a href="https://surgecast.io/dashboard" style="color:#6366f1;">Manage subscription</a>
    &middot;
    <a href="{_unsub_link(to_email)}" style="color:#444466;">Unsubscribe</a>
  </div>
</body>
</html>"""

    resend.api_key = os.environ["RESEND_API_KEY"]
    resend.Emails.send({
        "from": ALERT_FROM,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    })
    print(f"  Advance alert sent -> {to_email}")


def check_and_alert_advance(city, subscriber):
    """Send advance alert for HIGH events 8-30 days from now."""
    in_8_days  = (datetime.date.today() + datetime.timedelta(days=8)).isoformat()
    in_30_days = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    sub_threshold = subscriber.get("alert_threshold") or ALERT_THRESHOLD_HIGH
    to_email = subscriber["email"]

    events = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        params=[
            ("select",      "title,venue_name,start_date,impact_score"),
            ("city",        f"eq.{city}"),
            ("start_date",  f"gte.{in_8_days}"),
            ("start_date",  f"lte.{in_30_days}"),
            ("impact_score", f"gte.{sub_threshold}"),
            ("order",       "start_date.asc"),
        ],
    ).json()

    if not isinstance(events, list):
        events = []

    if events:
        print(f"  {len(events)} HIGH event(s) in 8-30 day window -> advance alert to {to_email}")
        send_advance_email(events, city, to_email)
    else:
        print(f"  No HIGH events in 8-30 day window for {to_email} — skipped")


def check_and_alert(city, subscriber):
    today     = datetime.date.today().isoformat()
    in_7_days = (datetime.date.today() + datetime.timedelta(days=7)).isoformat()
    # Subscriber's threshold defines the HIGH/MEDIUM split for their city
    sub_threshold = subscriber.get("alert_threshold") or ALERT_THRESHOLD_HIGH
    to_email  = subscriber["email"]

    # Fetch everything above the global floor (ALERT_THRESHOLD_LOW)
    all_events = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        params=[
            ("select", "title,venue_name,start_date,impact_score"),
            ("city",        f"eq.{city}"),
            ("start_date",  f"gte.{today}"),
            ("start_date",  f"lte.{in_7_days}"),
            ("impact_score", f"gte.{ALERT_THRESHOLD_LOW}"),
            ("order",       "impact_score.desc"),
        ],
    ).json()

    if not isinstance(all_events, list):
        all_events = []

    # Split into HIGH (at or above subscriber threshold) and MEDIUM (floor → threshold-1)
    high   = [e for e in all_events if e["impact_score"] >= sub_threshold]
    medium = [e for e in all_events
              if ALERT_THRESHOLD_LOW <= e["impact_score"] < sub_threshold]

    if high or medium:
        print(f"  {len(high)} HIGH + {len(medium)} MEDIUM → alert to {to_email}")
        send_alert_email(high, medium, city, to_email)
    else:
        print(f"  No events above floor ({ALERT_THRESHOLD_LOW}) for {to_email} — skipped")


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------

def run_job(afternoon=False):
    label = "afternoon (Pro)" if afternoon else "morning"
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"[{now}] Starting {label} scrape...\n")

    subscribers = get_subscribers(pro_only=afternoon)
    if not subscribers:
        print(f"No {'Pro ' if afternoon else ''}active subscribers — nothing to do.")
        return

    # Build city → [subscriber alert dicts] from subscriber_cities rows
    cities = {}
    for sub in subscribers:
        for city_row in sub.get("subscriber_cities", []):
            key = (city_row["city"], city_row["state"])
            cities.setdefault(key, []).append({
                "email": sub["email"],
                "plan": sub.get("plan", "starter"),
                "alert_threshold": city_row.get("alert_threshold") or 70,
            })

    print(f"{len(subscribers)} subscriber(s) across {len(cities)} city/cities\n")

    for (city, state), city_subs in cities.items():
        print(f"\n{'='*55}")
        print(f"  {city}, {state}  ({len(city_subs)} subscriber(s))")
        print(f"{'='*55}\n")

        # Only scrape fresh data on the morning run; afternoon is alerts-only
        if not afternoon:
            scrape_ticketmaster(city, state)

            existing_keys = get_existing_keys(city)
            print(f"Loaded {len(existing_keys)} existing key(s) for cross-source dedup\n")

            lat, lon, importance = geocode_city(city, state)
            if lat and lon:
                radius = _search_radius(importance)
                print(f"City size score: {importance:.3f} → {radius}-mile search radius")
                scrape_predicthq(city, existing_keys, lat, lon, radius=radius)
            else:
                print(f"PredictHQ: could not geocode {city}, {state} — skipping\n")

            if city.lower() == "asheville" and state.upper() == "NC":
                scrape_city_permits(existing_keys)

            if city.lower() == "wilmington" and state.upper() == "NC":
                scrape_uncw(existing_keys)

            remove_duplicates(city)
            print_summary(city)

        print(f"Sending {'afternoon ' if afternoon else ''}alerts for {city}...")
        for sub in city_subs:
            check_and_alert(city, sub)


def run_advance_job():
    """Every Monday: send HIGH-event advance alerts for the 8-30 day window."""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"[{now}] Starting weekly advance alert job...\n")

    subscribers = get_subscribers(pro_only=False)
    if not subscribers:
        print("No active subscribers — nothing to do.")
        return

    cities = {}
    for sub in subscribers:
        for city_row in sub.get("subscriber_cities", []):
            key = (city_row["city"], city_row["state"])
            cities.setdefault(key, []).append({
                "email": sub["email"],
                "plan": sub.get("plan", "starter"),
                "alert_threshold": city_row.get("alert_threshold") or 70,
            })

    print(f"{len(subscribers)} subscriber(s) across {len(cities)} city/cities\n")

    for (city, state), city_subs in cities.items():
        print(f"[Advance] {city}, {state}")
        for sub in city_subs:
            check_and_alert_advance(city, sub)


if __name__ == "__main__":
    schedule.every().day.at("08:00").do(lambda: run_job(afternoon=False))
    schedule.every().day.at("16:00").do(lambda: run_job(afternoon=True))
    schedule.every().monday.at("08:30").do(run_advance_job)

    print("Scheduler active:")
    print("  08:00 daily   - morning scrape + 7-day alerts")
    print("  16:00 daily   - Pro afternoon alerts")
    print("  08:30 Mondays - 30-day advance alerts")
    print("\nRunning initial morning scrape now...\n")
    run_job(afternoon=False)
    while True:
        schedule.run_pending()
        time.sleep(60)
