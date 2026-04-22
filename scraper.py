import datetime
import os
import time

import resend
import requests
import schedule

TICKETMASTER_KEY = os.environ["TICKETMASTER_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

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
        "Prefer": "resolution=ignore-duplicates"
    }
    response = requests.post(url, headers=headers, json=event)
    return response.status_code

def scrape_ticketmaster(city, state):
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": TICKETMASTER_KEY,
        "city": city,
        "stateCode": state,
        "countryCode": "US",
        "size": 20,
        "sort": "date,asc"
    }
    response = requests.get(url, params=params)
    data = response.json()
    events = data.get("_embedded", {}).get("events", [])
    print(f"Found {len(events)} events in {city}\n")

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
            "impact_score": score
        }

        status = save_event(event)
        if status in [200, 201]:
            saved += 1
            print(f"[{score:3d}] {title}")
        else:
            print(f"[skip] {title}")

    print(f"\nDone - {saved} new events saved")


def print_summary():
    today = datetime.date.today().isoformat()
    base_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    all_events = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=base_headers,
        params={"select": "impact_score", "limit": "1000"},
    ).json()

    total = len(all_events)
    above_70 = sum(1 for e in all_events if e["impact_score"] > 70)
    above_50 = sum(1 for e in all_events if e["impact_score"] > 50)

    upcoming = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers=base_headers,
        params={
            "select": "title,venue_name,start_date,impact_score",
            "start_date": f"gte.{today}",
            "impact_score": "gt.50",
            "order": "start_date.asc",
            "limit": "3",
        },
    ).json()

    print("\n" + "=" * 55)
    print("  POST-SCRAPE SUMMARY")
    print("=" * 55)
    print(f"  Total events in database : {total}")
    print(f"  Scoring above 70         : {above_70}")
    print(f"  Scoring above 50         : {above_50}")
    print("\n  Next 3 high-score upcoming events:")
    for e in upcoming:
        print(f"    [{e['impact_score']:3d}] {e['start_date']}  {e['title'][:35]:<35}  @ {e['venue_name']}")
    print("=" * 55 + "\n")


ALERT_TO = "phunclick@gmail.com"
ALERT_FROM = "Surgecast <onboarding@resend.dev>"

def send_alert_email(events):
    date_str = datetime.date.today().strftime("%B %d, %Y")
    lines = [
        f"Surgecast Alert - {date_str}",
        "=" * 40,
        f"{len(events)} high-impact event(s) in Asheville in the next 7 days:\n",
    ]
    for e in events:
        lines.append(f"[{e['impact_score']}] {e['start_date']}  {e['title']}")
        lines.append(f"      @ {e['venue_name']}")
        lines.append("")
    lines += ["--", "Surgecast - Asheville event intelligence"]

    resend.api_key = os.environ["RESEND_API_KEY"]
    resend.Emails.send({
        "from": ALERT_FROM,
        "to": [ALERT_TO],
        "subject": f"Surgecast: {len(events)} High-Impact Event(s) This Week",
        "text": "\n".join(lines),
    })
    print(f"Alert sent to {ALERT_TO}")


def check_and_alert():
    today = datetime.date.today().isoformat()
    in_7_days = (datetime.date.today() + datetime.timedelta(days=7)).isoformat()

    events = requests.get(
        f"{SUPABASE_URL}/rest/v1/events",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        params=[
            ("select", "title,venue_name,start_date,impact_score"),
            ("start_date", f"gte.{today}"),
            ("start_date", f"lte.{in_7_days}"),
            ("impact_score", "gt.70"),
            ("order", "start_date.asc"),
        ],
    ).json()

    if events:
        print(f"Found {len(events)} high-score event(s) in the next 7 days - sending alert...")
        send_alert_email(events)
    else:
        print("No events scoring above 70 in the next 7 days - no alert sent")


def run_job():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"[{now}] Starting scheduled scrape...")
    scrape_ticketmaster("Asheville", "NC")
    print_summary()
    check_and_alert()


if __name__ == "__main__":
    schedule.every().day.at("08:00").do(run_job)
    print("Scheduler active - runs daily at 08:00. Press Ctrl+C to stop.")
    print("Running initial scrape now...\n")
    run_job()
    while True:
        schedule.run_pending()
        time.sleep(60)