import datetime
import os
import time

import requests
import schedule
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

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


def send_alert_email(events):
    api_key = os.environ.get("SENDGRID_API_KEY")
    from_email = os.environ.get("ALERT_FROM_EMAIL")
    recipients = [e.strip() for e in os.environ.get("ALERT_EMAILS", "").split(",") if e.strip()]

    if not api_key or not from_email or not recipients:
        print("SendGrid not configured - skipping email alert")
        return

    rows = "".join(
        f"""<tr>
              <td style="padding:10px;border-bottom:1px solid #eee;">{e['start_date']}</td>
              <td style="padding:10px;border-bottom:1px solid #eee;">{e['title']}</td>
              <td style="padding:10px;border-bottom:1px solid #eee;">{e['venue_name']}</td>
              <td style="padding:10px;border-bottom:1px solid #eee;text-align:center;
                         font-weight:bold;color:#2563eb;">{e['impact_score']}</td>
            </tr>"""
        for e in events
    )

    html = f"""
    <html><body style="font-family:Arial,sans-serif;background:#f4f4f4;padding:20px;">
      <div style="max-width:620px;margin:0 auto;background:#fff;border-radius:8px;padding:32px;">
        <h2 style="margin-top:0;color:#1a1a1a;">Surgecast Weekly Alert</h2>
        <p style="color:#555;">{len(events)} high-impact event(s) in Asheville in the next 7 days:</p>
        <table style="width:100%;border-collapse:collapse;margin-top:16px;">
          <thead>
            <tr style="background:#f0f0f0;">
              <th style="padding:10px;text-align:left;border-bottom:2px solid #ddd;">Date</th>
              <th style="padding:10px;text-align:left;border-bottom:2px solid #ddd;">Event</th>
              <th style="padding:10px;text-align:left;border-bottom:2px solid #ddd;">Venue</th>
              <th style="padding:10px;text-align:center;border-bottom:2px solid #ddd;">Score</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        <p style="color:#999;font-size:12px;margin-top:24px;">Surgecast &mdash; Asheville event intelligence</p>
      </div>
    </body></html>
    """

    message = Mail(
        from_email=from_email,
        to_emails=recipients,
        subject=f"Surgecast: {len(events)} High-Impact Event(s) Coming This Week",
        html_content=html,
    )
    response = SendGridAPIClient(api_key).send(message)
    print(f"Alert sent to {len(recipients)} subscriber(s) (HTTP {response.status_code})")


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