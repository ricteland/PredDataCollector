import os
import sys
import json
import requests
import datetime

# ── Allow running directly as `python weather/fetch_weather_tokens.py` ───────
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

GAMMA_URL = "https://gamma-api.polymarket.com/events"
CITIES = ["london", "seoul", "nyc", "toronto", "wellington", "atlanta", "chicago", "seattle", "buenos-aires", "miami"]

def generate_weather_slugs(city, target_date):
    slugs = []
    month_full = target_date.strftime("%B").lower()
    month_short = target_date.strftime("%b").lower()[:3]
    day = str(target_date.day)
    year = target_date.strftime("%Y")
    prev_year = str(int(year) - 1)
    
    city_variants = [city]
    if city == "nyc":
        city_variants.append("new-york-city")
    if city == "buenos-aires":
        city_variants.append("buenos-aires-ar")
        
    for cv in city_variants:
        for m in [month_full, month_short]:
            for y in [year, prev_year, ""]:
                suffix = f"-{y}" if y else ""
                slugs.append(f"highest-temperature-in-{cv}-on-{m}-{day}{suffix}")
            
    return list(dict.fromkeys(slugs))

def fetch_events():
    all_events = []
    today = datetime.datetime.now(datetime.timezone.utc).date()
    yesterday = today - datetime.timedelta(days=1)
    tomorrow = today + datetime.timedelta(days=1)
    
    for city in CITIES:
        for target_date in [yesterday, today, tomorrow]:
            slugs_to_try = generate_weather_slugs(city, target_date)
            
            event = None
            for slug in slugs_to_try:
                try:
                    r = requests.get(f"{GAMMA_URL}?slug={slug}", timeout=10)
                    if r.status_code == 200:
                        data = r.json()
                        if data and isinstance(data, list) and len(data) > 0:
                            event = data[0]
                            break
                except Exception:
                    pass
            
            if not event:
                continue
                
            active = event.get('active', False)
            closed = event.get('closed', False)
            if closed or not active:
                continue
                
            markets = event.get("markets", [])
            for m in markets:
                if not m.get("active", False):
                    continue
                condition_id = m.get("conditionId") or m.get("condition_id")
                question = m.get("question", "")
                market_slug = m.get("slug", "")
                end_date = m.get('endDate') or m.get('end_date')
                
                try:
                    outcomes = json.loads(m.get("outcomes", "[]"))
                    clobTokenIds = json.loads(m.get("clobTokenIds", "[]"))
                except Exception:
                    continue
                
                tokens_dict = {}
                for i, outcome in enumerate(outcomes):
                    if i < len(clobTokenIds):
                        key = outcome.lower()
                        tokens_dict[key] = {
                            "token_id": clobTokenIds[i],
                            "outcome": outcome
                        }
                if condition_id and tokens_dict:
                    all_events.append({
                        "city": city,
                        "date": target_date.strftime("%Y-%m-%d"),
                        "event_slug": slug,
                        "market_slug": market_slug,
                        "condition_id": condition_id,
                        "question": question,
                        "end_date": end_date,
                        "tokens": tokens_dict
                    })

    output_data = {
        "discovered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "events": all_events
    }
    
    # Always write to the weather/ folder alongside this script
    out_path = os.path.join(_HERE, 'weather_data_fetched.json')
    with open(out_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    print(f"Weather tokens fetched: {len(all_events)} individual buckets.")

if __name__ == "__main__":
    fetch_events()
