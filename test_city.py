"""
Quick dry-run test — shows what each source finds for a city WITHOUT saving to DB.
Usage:  python test_city.py
"""

import datetime
import os
import requests
from dotenv import load_dotenv

load_dotenv()

TICKETMASTER_KEY = os.environ["TICKETMASTER_KEY"]
PREDICTHQ_KEY    = os.environ["PREDICTHQ_KEY"]

CITY  = "Freeport"
STATE = "ME"

# ── Geocode ──────────────────────────────────────────────────────────────────
print(f"\n{'*'*55}")
print(f"  Testing: {CITY}, {STATE}")
print(f"{'*'*55}\n")

resp = requests.get(
    "https://nominatim.openstreetmap.org/search",
    params={"q": f"{CITY}, {STATE}, US", "format": "json", "limit": 1},
    headers={"User-Agent": "Surgecast/1.0"},
    timeout=10,
)
results = resp.json()
if not results:
    print("Could not geocode — check city/state spelling")
    exit(1)

lat = float(results[0]["lat"])
lon = float(results[0]["lon"])
importance = float(results[0].get("importance", 0))
print(f"Geocode: {lat}, {lon}  (importance: {importance:.3f})")

if importance >= 0.70:   threshold, radius = 85, 30
elif importance >= 0.55: threshold, radius = 70, 20
elif importance >= 0.40: threshold, radius = 55, 15
else:                    threshold, radius = 35, 10
print(f"Auto threshold would be: {threshold}")
print(f"Search radius would be:  {radius} miles\n")

# -- Ticketmaster -----------------------------------------------──────────────────────
print("-- Ticketmaster -----------------------------------------------")
tm = requests.get(
    "https://app.ticketmaster.com/discovery/v2/events.json",
    params={
        "apikey": TICKETMASTER_KEY,
        "city": CITY,
        "stateCode": STATE,
        "countryCode": "US",
        "size": 20,
        "sort": "date,asc",
    },
).json()

events = tm.get("_embedded", {}).get("events", [])
print(f"Found {len(events)} events\n")
for e in events:
    venue = e.get("_embedded", {}).get("venues", [{}])[0].get("name", "?")
    date  = e["dates"]["start"].get("localDate", "?")
    print(f"  {date}  {e['name'][:50]:<50}  @ {venue}")

# ── PredictHQ ────────────────────────────────────────────────────────────────
print(f"\n-- PredictHQ ({radius}-mile radius from {CITY}) --------------------")
today = datetime.date.today().isoformat()
phq = requests.get(
    "https://api.predicthq.com/v1/events/",
    headers={"Authorization": f"Bearer {PREDICTHQ_KEY}", "Accept": "application/json"},
    params={
        "within": f"{radius}mi@{lat},{lon}",
        "active.gte": today,
        "limit": 50,
        "sort": "start",
    },
    timeout=15,
).json()

results = phq.get("results", [])
print(f"Found {len(results)} events within {radius} miles\n")
for e in results:
    date  = (e.get("start") or "")[:10]
    title = e.get("title", "")[:50]
    cat   = e.get("category", "")
    att   = e.get("phq_attendance")
    att_s = f"  att:{att:,}" if att else ""
    print(f"  {date}  {title:<50}  [{cat}]{att_s}")

print(f"\n{'*'*55}")
print(f"  Summary: {len(events)} Ticketmaster + {len(results)} PredictHQ")
print(f"  City permits: not available for {CITY} (Asheville only)")
print(f"  Search radius used: {radius} miles")
print(f"{'*'*55}\n")
